---
title: "Per-caller rate limiting for gRPC services"
status: implemented
created: 2026-05-07
updated: 2026-05-13
tags: [adr, architecture, security, rate-limiting, networking]
---

# A-0005 — Per-Caller Rate Limiting

## Context

rr's gRPC services (AdminService, ClientService) are protected by a per-service
token bucket implemented in `crates/rr-network/src/rate_limit.rs`. A
`RateLimitInterceptor` is configured with a shared `RateLimiter` (backed by
`tokio::sync::Semaphore`) and runs **after** TLS handshake and bearer-token
authentication.

The current model is:

```
caller A ─┐
caller B ─┤── [one shared bucket: N rps] ─→ ClientService
caller C ─┘
```

All authenticated callers to a service draw from the same token pool. A single
misbehaving or runaway client can exhaust the entire quota, starving legitimate
callers ("noisy neighbour" problem).

Wave 6.1 (2026-04-26) added `caller_token_fp` — a siphash-1-3 fingerprint of the
bearer token — to audit-log rejection events. This fingerprint is computed
on-the-fly in `BearerInterceptor` and does **not** persist caller state. It
provides a correlation handle in logs but is not used for rate limiting.

The `authorization` metadata header (from which the token is extracted) persists
through the interceptor chain and is readable in `RateLimitInterceptor::call()`.
This means per-caller identity is available without any changes to the auth layer.

Key invariants from the existing design that must be preserved:

- **RaftService is never rate limited** — throttling Raft RPCs causes election
  timeouts. This constraint is structural and must not be relaxed.
- Rate limiting runs **after auth** — unauthenticated/invalid callers fail before
  consuming any quota.
- No external dependencies were introduced for rate limiting (tokio Semaphore,
  no `governor`, no `tower-governor`).

## Problem

Design a **per-caller rate limiting** extension that:

1. Isolates quota per authenticated caller so one caller cannot starve others.
2. Reuses `caller_token_fp` (computed from the bearer token) as the caller key —
   no additional auth changes required.
3. Introduces no new mandatory dependencies (ideally `std` only).
4. Is backward compatible — existing `RateLimitConfig` callers see no breakage.
5. Does not add complexity when the feature is disabled.
6. Handles caller churn (new callers over time) without unbounded memory growth.

## Options

### Option 1 — Additive (global + per-caller both enforced)

Requests must pass **both** the global service-level bucket and the per-caller
bucket. A request is rejected if either is exhausted.

**Pros:**
- Hard cap on total service throughput even if all per-caller quotas pass.
- Simple mental model: "N rps per service, M rps per caller".

**Cons:**
- Double-checking adds latency (two semaphore acquires).
- Confusing when `per_caller_rps > service_rps` (per-caller limit is meaningless).
- Config surface is larger — operators must reason about both simultaneously.

### Option 2 — Per-caller exclusive (replaces global for authenticated callers)

When `per_caller_rps` is set, each authenticated caller gets its own bucket.
The global `client_rps` / `admin_rps` serves as a fallback **only** when caller
identity is unavailable (unauthenticated services, pre-auth failures).

```
caller A ─── [bucket_A: M rps] ─┐
caller B ─── [bucket_B: M rps] ─┤── ClientService
caller C ─── [bucket_C: M rps] ─┘
```

**Pros:**
- Clean isolation: one caller's traffic does not affect others.
- Single limit to reason about per caller.
- Simpler code: one code path, no double-check.

**Cons:**
- No global cap on total throughput. 1000 callers each at 100 rps = 100 000 rps
  total — server must be sized accordingly.
- Operators lose the "total ceiling" knob (can be added via `max_total_rps` later
  if needed).

### Option 3 — Configurable mode (operator chooses additive or exclusive)

Add a `RateLimitMode` enum (`Additive | PerCallerExclusive`) and apply the
chosen mode at runtime.

**Pros:**
- Maximum flexibility.

**Cons:**
- Config surface doubles.
- Two modes = two code paths to maintain and test.
- Premature: no known use case for Additive mode exists today.

## Decision

**Option 2 — Per-caller exclusive**.

Rationale:
- Matches the primary use-case (noisy neighbour isolation).
- Single, clear mental model.
- Simpler implementation.
- If a global cap is ever needed it can be added as `max_total_rps` (a separate
  concept) without changing per-caller semantics.

### Implementation plan

**`RateLimitConfig` additions** (no breaking changes to existing fields):

```rust
pub struct RateLimitConfig {
    // existing fields unchanged
    pub admin_rps: Option<u32>,
    pub client_rps: Option<u32>,
    pub burst: Option<u32>,

    // new opt-in fields
    /// Per-caller rps limit (exclusive mode). When set, each unique
    /// `caller_token_fp` gets its own bucket of this size.
    /// The global `admin_rps`/`client_rps` is ignored for services where
    /// per-caller limiting is active.
    pub per_caller_admin_rps: Option<u32>,
    pub per_caller_client_rps: Option<u32>,
    /// Per-caller burst. Defaults to `max(per_caller_rps, 10)` if unset.
    pub per_caller_burst: Option<u32>,
}
```

**`PerCallerLimiterMap`** — shared state for the per-caller buckets:

```rust
struct CallerEntry {
    limiter: RateLimiter,
    last_seen: std::time::Instant,
}

pub struct PerCallerLimiterMap {
    entries: Arc<std::sync::RwLock<HashMap<String, CallerEntry>>>,
    per_caller_rps: u32,
    per_caller_burst: u32,
    ttl: Duration,  // default: 5 minutes idle
}
```

`RwLock<HashMap>` chosen over `DashMap` to avoid a new dependency. The map is
not a hot read path (one short-lived read per request), so `std::sync::RwLock`
contention is acceptable. If profiling shows contention, upgrading to `DashMap`
is a one-line change.

**Caller identity** — extracted inline in `RateLimitInterceptor::call()`:

```rust
fn caller_fp(request: &Request<()>) -> Option<String> {
    request
        .metadata()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(crate::auth::token_fingerprint)
}
```

This re-reads the `authorization` header that `BearerInterceptor` already
validated. No API change to `BearerInterceptor` is needed.

**TTL eviction** — lazy strategy: on each map write-lock acquisition to insert a
new caller, evict entries whose `last_seen` age exceeds `ttl` (5 minutes by
default). This avoids a background task and keeps the implementation in `std`.

**Audit log** — per-caller rejections emit `caller_token_fp` in the `rate_limit`
audit event (extends existing pattern from auth rejections).

## Consequences

### Positive
- Strong noisy-neighbour isolation: one caller's traffic does not affect others.
- Zero new mandatory crate dependencies.
- Backward compatible: `per_caller_*` fields are opt-in; deployments without
  them see no behaviour change.
- `caller_token_fp` surface already established (Wave 6.1) — consistent story
  for operators correlating logs.

### Negative
- No global throughput cap when per-caller mode is active. Operators who need a
  ceiling must size the server accordingly or pair with an upstream proxy.
- `RwLock<HashMap>` under high cardinality caller sets will see write contention
  on new-caller insertion. Acceptable for typical cluster usage patterns
  (dozens of callers, not millions).

### Neutral
- `RateLimiter` struct (and its `tokio::spawn` refill loop) is reused unchanged —
  one `RateLimiter` instance per active caller.
- The existing `disabled()` / `with_limiter()` constructors are preserved.
- Unauthenticated services (no bearer token) continue to use the global bucket
  unchanged.

## Changelog

| Date | Author | Change |
|------|--------|--------|
| 2026-05-07 | session | Initial draft — design from analysis + user discussion |
| 2026-05-13 | session | Status → `implemented`. `PerCallerLimiterMap`, `RateLimitConfig::per_caller_*` fields, `RateLimitInterceptor::with_per_caller`, and 13 unit tests already shipped in `crates/rr-network/src/rate_limit.rs`. Final wire-up adds `--per-caller-admin-rps` / `--per-caller-client-rps` / `--per-caller-burst` CLI flags + matching `RR_PER_CALLER_*` env vars + `[rate_limit].per_caller_*` TOML fields, plus the `ResolvedRateLimit` → `RateLimitConfig` plumbing in `rr-server`. |
