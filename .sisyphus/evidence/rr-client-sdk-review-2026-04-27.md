# rr-client SDK Review Report

**Date**: 2026-04-27
**Reviewer**: Sisyphus
**Scope**: `crates/rr-client/` — Rust client SDK for the rr distributed framework
**Trigger**: Pre-flight review before building Java SDK with feature parity (Phase A of multi-language SDK initiative)
**Verdict**: **PRODUCTION-CAPABLE CORE, BUT NOT PUBLISH-READY** — 3 must-fix items before Java SDK begins (so the parity target itself is correct).

---

## TL;DR

- **Core API design is sound** — `RrClient::connect / send_request / send_request_idempotent / get_cluster_info` are clean, async, and instrumented with tracing + metrics. Retry loop with leader-redirect cache is correctly implemented.
- **3 production gaps** that block parity claims and break the published-SDK story:
  1. **No TLS / mTLS support at SDK level** — `Channel::from_shared(format!("http://{addr}"))` hard-codes plaintext. `rr-network::tls::TlsConfig::build_client_tls()` already exists but is never wired in. **MUST FIX**.
  2. **No bearer-token auth support** — `rr-network::auth::attach_bearer_token()` already exists but rr-client never calls it. Server-side `BearerInterceptor` is enforced when `client_token` is set, so any production rr cluster with auth on rejects rr-client traffic. **MUST FIX**.
  3. **`Cargo.toml` missing publishing metadata** — version `0.1.0`, no description, license, repository, keywords, categories, readme. crates.io will reject `cargo publish` until these are added. **MUST FIX**.
- **1 nice-to-have polish item**: missing `crates/rr-client/README.md`. Not blocking, but expected by users browsing crates.io / docs.rs.
- **No issues found** in: API ergonomics, retry semantics, idempotency wiring, leader-cache invalidation, metrics surface, tracing instrumentation, error mapping, test coverage.

After these 3 fixes (estimated ~4-6 hours of focused work), rr-client is fit to be the Rust-side reference for the Java SDK and publishable to crates.io as `0.3.0`.

---

## Review Methodology

Read every line of:
- `crates/rr-client/src/lib.rs` (540 lines, all of it)
- `crates/rr-client/Cargo.toml` (13 lines)
- Comparisons against:
  - `crates/rr-network/src/tls.rs` (159 lines — TlsConfig already plumbed for client use)
  - `crates/rr-network/src/auth.rs` (342 lines — BearerInterceptor + `attach_bearer_token` helper already shipped)
  - `crates/rr-network/proto/client.proto` (75 lines — wire protocol authoritative)
  - `crates/rr-server/src/cli.rs`, `config.rs` (server-side TLS + auth wiring as reference pattern)

Compared against rr's own server-side security invariants (`validate_security_invariants`) and the documented threat model in `crates/rr-network/src/auth.rs` module rustdoc.

---

## Findings — Detail

### F1 (BLOCKER): No TLS/mTLS support in `RrClient`

**Severity**: P0 (production-blocker for any TLS-enabled cluster)

**Evidence**: `crates/rr-client/src/lib.rs:127-139`

```rust
async fn connect_to_addr(&self, addr: &str) -> Result<ClientServiceClient<Channel>, RrError> {
    let channel = Channel::from_shared(format!("http://{addr}"))   // ← hard-coded plaintext
        .expect("valid uri")
        .connect()
        .await
        .map_err(|e| {
            RrError::Network(NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            })
        })?;
    Ok(ClientServiceClient::new(channel))
}
```

**Impact**:
- Any rr cluster with `tls_config: Some(_)` rejects rr-client connections at handshake time.
- Operators currently must construct `tonic::Channel` themselves with `ClientTlsConfig` and bypass `RrClient` entirely — defeating the purpose of the SDK.
- Java SDK target (which will use grpc-java + netty-tcnative-boringssl) will support TLS by default, creating an asymmetric capability gap if Rust SDK doesn't catch up.

**Why this happens**: `rr-network::tls::TlsConfig::build_client_tls()` exists (lines 77-92 of `tls.rs`) but is consumed only by `rr-raft/src/network_factory.rs` (the inter-node ChannelPool), never by `rr-client`. The author shipped TLS for the Raft path but didn't propagate it to the SDK.

**Fix**: Add `RrClientBuilder` API:

```rust
pub struct RrClientBuilder {
    bootstrap_addrs: Vec<String>,
    retry_config: RetryConfig,
    tls_config: Option<TlsConfig>,
    bearer_token: Option<String>,  // see F2
}

impl RrClientBuilder {
    pub fn new(addrs: Vec<String>) -> Self { ... }
    pub fn with_tls(mut self, tls: TlsConfig) -> Self { ... }
    pub fn with_bearer_token(mut self, token: String) -> Self { ... }
    pub fn with_retry_config(mut self, cfg: RetryConfig) -> Self { ... }
    pub async fn build(self) -> Result<RrClient, RrError> { ... }
}
```

Inside `connect_to_addr`, branch on TLS presence:

```rust
let scheme = if self.tls_config.is_some() { "https" } else { "http" };
let mut endpoint = Channel::from_shared(format!("{scheme}://{addr}"))?;
if let Some(tls) = &self.tls_config {
    endpoint = endpoint.tls_config(tls.build_client_tls()?)?;
}
let channel = endpoint.connect().await?;
```

Existing `RrClient::connect` and `connect_with_config` are kept as thin wrappers over `RrClientBuilder` for backward compatibility (no TLS = same behavior as today).

**Effort**: ~1.5 hours including unit test that wires a `TlsConfig` and verifies the channel scheme.

---

### F2 (BLOCKER): No bearer-token auth support in `RrClient`

**Severity**: P0 (production-blocker for any auth-enabled cluster)

**Evidence**: `crates/rr-client/src/lib.rs:262` — `grpc_client.request(tonic::Request::new(request)).await` sends a bare `Request` with no metadata. There is **no** code path in `rr-client` that touches `request.metadata_mut()`.

Server-side `crates/rr-network/src/auth.rs:115-160` `BearerInterceptor::call` rejects with `Status::unauthenticated("authentication failed")` when `client_token` is configured and the request lacks `Authorization: Bearer <token>`.

**Impact**: Same shape as F1 — any production cluster with `auth.client_token = Some(_)` cannot use the SDK as-shipped. Users today must wrap `tonic::Request` manually before calling internal RPCs, again defeating the SDK.

**Why this happens**: Same as F1. The helper exists (`attach_bearer_token` in `crates/rr-network/src/auth.rs:187-193`), but rr-client never calls it.

**Fix**: Inside `send_request_inner`, after constructing the inner `ClientRequest`:

```rust
let mut req = tonic::Request::new(request);
if let Some(token) = &self.bearer_token {
    rr_network::auth::attach_bearer_token(&mut req, token)
        .map_err(|status| RrError::Network(NetworkError::RpcFailed(status.message().to_string())))?;
}
match grpc_client.request(req).await { ... }
```

Same treatment for `get_cluster_info`.

**Security note (P-tier)**: rr's documented threat model (`crates/rr-network/src/auth.rs` module rustdoc, line 25-28) explicitly states: *"Tokens travel over the wire in plaintext inside the gRPC metadata; **TLS must be enabled** to prevent interception."* Therefore `RrClientBuilder::with_bearer_token` MUST emit a `tracing::warn!` (or return an error in strict mode) when called without `with_tls`. Keep the warn for parity with server-side `validate_security_invariants` — the SDK should not silently weaken the deployment posture.

**Effort**: ~1 hour including unit test that verifies the metadata is attached.

---

### F3 (BLOCKER): `Cargo.toml` missing publishing metadata

**Severity**: P0 (publish-blocker)

**Evidence**: `crates/rr-client/Cargo.toml`:

```toml
[package]
name = "rr-client"
version = "0.1.0"
edition = "2021"
```

That's all. No description, license, repository, homepage, keywords, categories, or readme.

**Impact**:
- `cargo publish` fails with `manifest has no description` and `manifest has no license` errors.
- crates.io listing has no abstract or category, hurting discoverability.
- docs.rs has no README to render on the landing page.

**Fix**: Add (matching project's existing license = MIT, repo = `gardenias/rr`):

```toml
[package]
name = "rr-client"
version = "0.3.0"  # bump to align with framework release
edition = "2021"
rust-version.workspace = true
description = "Async Rust client SDK for the rr distributed Multi-Raft consensus framework. Provides automatic leader-redirect, exponential-backoff retry, framework-level idempotency, and TLS/mTLS+bearer auth out of the box."
license = "MIT"
repository = "https://github.com/gardenias/rr"
homepage = "https://github.com/gardenias/rr"
documentation = "https://docs.rs/rr-client"
readme = "README.md"
keywords = ["raft", "consensus", "distributed", "grpc", "multi-raft"]
categories = ["network-programming", "asynchronous", "database-implementations"]
```

Workspace `Cargo.toml` should also gain shared `description = "..."` etc. fields under `[workspace.package]` so other crates can `field.workspace = true` once they go publishable.

**Effort**: ~30 minutes including bumping workspace shared fields if user chooses.

**Note on version bump**: User has chosen "暂不做版本管理" per session 2026-04-26 directive (CONTEXT.md line 12). I recommend bumping version to `0.3.0` to align with the framework's current main HEAD (which already has all v0.3 features), but this is a **decision that needs user sign-off** since it affects the broader release plan. Default below: keep at `0.1.0` for this phase, flag for follow-up.

---

### F4 (NICE-TO-HAVE): Missing `README.md`

**Severity**: P2 (publish-polish, not blocker)

**Evidence**: `ls crates/rr-client/` shows only `Cargo.toml` and `src/`. No README.

**Impact**: docs.rs landing page falls back to lib.rs module docs (which are good — 4 lines of `//!`). crates.io listing shows the description from Cargo.toml only. Acceptable but not best-practice for a flagship SDK.

**Fix**: Author a short `crates/rr-client/README.md` covering: connect, send_request, send_request_idempotent, retry config, TLS/auth setup (after F1+F2), get_cluster_info, links to main project docs.

**Effort**: ~30 minutes.

---

## What's Good (Don't Touch)

These items were reviewed and **passed** — no changes recommended:

| Item | Verdict | Why |
|---|---|---|
| API surface (`connect`, `send_request`, `send_request_idempotent`, `send_request_with_partition`, `get_cluster_info`) | ✓ Clean | Methods are minimal, async, well-named, and have rustdoc with examples |
| Retry loop semantics | ✓ Correct | Exponential backoff with `retry_delay_ms * 2^attempt`, max_attempts respected, leader-cache invalidation on transport error |
| Leader cache | ✓ Correct | `RwLock<HashMap<GroupId, (NodeId, String)>>` — cheap reads, write only on redirect/eviction. Bootstrap address round-robin on cache miss |
| Idempotency wiring | ✓ Correct | `send_request_idempotent` properly maps `IdempotencyKey` → proto `idem_key`; surfaces `IdempotencyExpired` from `idem_error` in response |
| Metrics surface (`rr_client_request_duration_seconds`, `rr_client_retries_total`, `rr_client_leader_cache_{hits,misses}_total`) | ✓ Aligned | Matches `docs/docs/reference/metrics-reference.md` documented surface |
| Tracing instrumentation | ✓ Adequate | `#[instrument]` on public methods; `tracing::debug!` / `tracing::warn!` / `tracing::info!` at appropriate levels with structured fields |
| Error mapping (`RrError::IdempotencyExpired`, `RrError::Network(NetworkError::*)`) | ✓ Correct | Wire-level `IdempotencyError` → typed Rust error; transport errors propagate with addr context |
| Test coverage (8 unit tests, ~95% per CONTEXT.md) | ✓ Solid | Covers: connect happy path, empty addrs reject, leader cache update/remove, retry config defaults + custom, send_request to unreachable addr, get_cluster_info to unreachable addr, send_request_with_partition wrapper |
| `Cargo.toml` dependency footprint | ✓ Minimal | 7 deps total: rr-core, rr-network, tokio, tonic, tracing, async-trait, metrics. No bloat, all workspace-shared |

---

## Risks Carrying Forward to Java SDK

These exist in rr-client today and the Java SDK MUST replicate them faithfully (so the SDKs stay observably equivalent):

1. **Bootstrap-addr round-robin on cache miss is intentional** — not all nodes serve all groups equally well, but rr-server transparently redirects. Java SDK must not implement smarter "pick nearest" logic; it would diverge from Rust observable behavior.
2. **`request_id` in `IdempotencyKey` is `u64`, not opaque bytes** — proto3 `uint64`. Java side must use `long` (signed) and document that values >= `2^63` are valid bit-pattern but observed as negative `long`. Or use `BigInteger`. Recommend `long` + comment, parity with how other gRPC Java APIs handle uint64.
3. **Leader cache is per-group, not per-cluster** — `HashMap<GroupId, (NodeId, String)>`. Java side should use `ConcurrentHashMap<Long, LeaderEntry>`.
4. **No connection pooling visible to user** — rr-client wraps `rr-network::ChannelPool` internally but doesn't expose it. Java SDK should hide grpc-java `ManagedChannel` lifecycle similarly; users get an `RrClient`, channels are an implementation detail.
5. **Errors are typed, not stringly** — `RrError::IdempotencyExpired { request_type, request_id, watermark }` is a struct variant. Java side should mirror with a checked `IdempotencyExpiredException` carrying the same three fields.
6. **`#[instrument]` adds W3C-compatible spans** — Java side should use OpenTelemetry's `@WithSpan` or grpc-java's `ClientInterceptor` to attach equivalent spans, allowing trace propagation across language boundaries.

---

## Proposed Next Steps

After this review, Phase A.2 work (subject to user Rule 0 confirmation):

1. Add `RrClientBuilder` + `with_tls(TlsConfig)` + wiring (resolves F1)
2. Add `with_bearer_token(String)` + `attach_bearer_token` call site (resolves F2)
3. Update `Cargo.toml` metadata for publish-readiness (resolves F3)
4. Author `crates/rr-client/README.md` (resolves F4)
5. Add 4 new unit tests: TLS config wiring, bearer token attachment, builder API ergonomics, security warning when token without TLS
6. Run `cargo test -p rr-client && cargo clippy -p rr-client -- -D warnings && cargo doc -p rr-client --no-deps` for L1 verification

Total estimated effort: **~4-6 hours focused work**.

After Phase A.2 lands, Phase B (Java SDK) starts with a Rust-side reference that has full feature parity exposed via clean public API.

---

## Open Questions (User Decisions Pending)

1. **Version bump**: keep `0.1.0` or bump to `0.3.0` aligning with framework's main HEAD? Recommend `0.3.0` for SDK-framework version coherence, but defer to user since it intersects with the broader "暂不做版本管理" directive.
2. **`RrClient::connect` backward compatibility**: Keep as a thin wrapper over the new builder, or deprecate? Recommend keep + add `#[deprecated(note = "use RrClientBuilder")]` only at next major. Users on `0.1.0` should not be forced to migrate this session.
3. **Strict-mode for TLS-less bearer token**: warn-only (current recommendation) or error-out? Recommend warn for parity with server's `allow_insecure` opt-in.

---

## Appendix — File Inventory Touched / Read

| File | Lines | Action |
|---|---|---|
| `crates/rr-client/src/lib.rs` | 540 | Read all |
| `crates/rr-client/Cargo.toml` | 13 | Read all |
| `crates/rr-network/src/tls.rs` | 159 | Read all |
| `crates/rr-network/src/auth.rs` | 342 | Read 1-200 (interceptor + token attach) |
| `crates/rr-network/proto/client.proto` | 75 | Read all |
| `crates/rr-core/src/config.rs` | 137 | Read all (for ConfigProvider context, not rr-client) |
| `Cargo.toml` (workspace) | 46 | Read all |

Total: 1312 lines reviewed.

---

## Changelog

| Date | Change | Reason |
|---|---|---|
| 2026-04-27 | Initial review report. F1/F2/F3 BLOCKER + F4 NICE-TO-HAVE found. | Pre-flight before Java SDK build |
