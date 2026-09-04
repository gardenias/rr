---
title: "Framework-level idempotency via (request_type, request_id)"
status: implemented
created: 2026-04-24
updated: 2026-09-04
tags: [adr, architecture, idempotency, correctness]
---

# A-0003 — Framework Idempotency Layer

## Context

Investigation of rr's request flow found the following:

- **Synchronous RPC only**: The `ClientService.Request` gRPC call blocks until
  the write entry is committed AND applied. The response is returned inline
  on the gRPC stream.
- **Correlation is implicit**: No `ProposalId`, no `oneshot::channel`, no
  request ID. Request-response pairing relies entirely on gRPC's stream
  pairing.
- **Disconnect loses response, not effect**: If the client disconnects after
  proposal but before response delivery, the Raft entry is still committed
  and applied. The response bytes are computed but discarded (stream closed).
- **No framework idempotency**: `ClientRequest` has no request-id field. No
  duplicate detection. Retrying the same request after disconnect causes
  **re-apply** — user `Service::handle()` sees the same payload twice.

Quoting from the client flow investigation:

> **Idempotency / Dedup Story**
> **No built-in idempotency or deduplication in rr:**
> - No `request_id` field in `ClientRequest` protobuf
> - No `client_id + sequence number` tracking
> - No duplicate detection in the framework
>
> Responsibility is on the application.

Every business using rr today reimplements idempotency in their `Service`, and
each implementation must also manually:

1. Serialise idempotency state in `Service::snapshot()`
2. Restore it in `Service::restore()`
3. Bound memory growth
4. Handle concurrent apply() correctly

These are all framework-level concerns that don't belong in business code.
A-0001 (Kafka input) amplifies the problem: Kafka's at-least-once delivery
means duplicates are **routine**, not exceptional.

## Problem

Design a **framework-level idempotency mechanism** that:

1. Is **opt-in** — users who prefer business-layer idempotency (e.g. using
   complex multi-field composite keys) can skip it completely
2. Integrates with existing snapshot/restore automatically
3. Has bounded memory
4. Handles distributed-origin requests correctly (multiple clients submitting
   same request type)
5. Does not require users to register clients or manage connection state

## Options

### Option A — Per-`(client_id, request_type)` monotonic request_id

Each client maintains its own per-type monotonic counter. Framework tracks
high-watermark per `(client_id, request_type)` pair.

Pros:
- Low coordination cost — clients don't share ID space
- Each client independently maintains monotonicity

Cons:
- Memory scales with `O(N_clients × M_types)`
- Requires a `client_id` registration / lease story to garbage-collect stale
  clients' watermarks
- Clients without stable identity (e.g. edge pods churning) overwhelm the
  watermark map

### Option B — Per-`(request_type)` globally monotonic request_id

Framework tracks ONE watermark per request_type across the entire cluster.
Monotonicity is a **global** invariant per type, enforced by the id generation
source.

Pros:
- Minimal state: `O(M_types)` watermarks (typically <20)
- No client registration needed
- Simpler mental model

Cons:
- Requires a **single monotonic id source** per type across the entire system
- Unsuitable for edge-generated UUIDs
- Cross-client id collisions cause silent request drops

### Option C — Content-addressable dedup (hash-based)

Frame each request with a `content_hash = sha256(payload)`. Framework
deduplicates on hash.

Pros:
- No id generation story at all — content IS the key
- Natural fit for truly-idempotent business ops

Cons:
- Non-idempotent-intended retries collide (e.g. two independent "transfer 100
  from A to B" operations must be distinct business events)
- Hash cache memory unbounded in proportion to request volume
- No room for "client retry of same request" vs "two independent requests
  with same content"

### Option D — No framework support (status quo)

Pros: zero framework work

Cons:
- Every user reimplements, often incorrectly
- Snapshot integration repeatedly forgotten
- Framework users solve the same problem N times

## Decision

**Option B is adopted**, with one refinement confirmed in design discussion:

- Idempotency key structure: `(request_type, request_id, client_id)`
- **Dedup scope**: `(request_type, request_id)` — `client_id` does **NOT**
  participate in dedup
- **`client_id` retained as diagnostic field** — logged and included in
  traces for debugging; used to detect cross-client id collisions
- **Opt-in per request** — `IdempotencyKey` is optional in `ClientRequest`;
  requests without it flow exactly as today (no state tracking, no overhead)

### Protocol

```protobuf
// client.proto

message ClientRequest {
  uint64 partition_id = 1;
  bytes payload = 2;

  // Optional. When set, framework deduplicates on (request_type, request_id).
  // When unset, request flows normally (useful for business-layer composite-key
  // idempotency, one-shot operations, or requests intentionally non-idempotent).
  optional IdempotencyKey idem_key = 3;
}

message IdempotencyKey {
  // Business-defined type, e.g. "transfer" / "create_order" / "put_kv".
  // Watermark and response cache are scoped per request_type.
  string request_type = 1;

  // Monotonically increasing within request_type across the entire cluster.
  // See "Monotonicity requirement" below.
  uint64 request_id = 2;

  // Diagnostic-only. Typical values: "appid+podip" / "hostname+pid" / UUID.
  // Logged and traced; NOT part of the dedup key.
  string client_id = 3;
}
```

### State machine integration

Framework wraps user `Service` in a state machine that intercepts apply():

```rust
struct IdempotencyLayer {
    // request_type → highest request_id successfully applied
    watermarks: HashMap<String, u64>,

    // (request_type, request_id) → cached response + metadata
    // Bounded LRU to prevent unbounded memory growth
    response_cache: LruCache<(String, u64), CachedResponse>,
}

struct CachedResponse {
    payload: Vec<u8>,
    client_id: String,       // for collision detection / warn-log
    applied_at: SystemTime,  // for operational visibility
}

enum IdempotencyDecision {
    New,                    // proceed to Service::handle()
    Replay(Vec<u8>),        // return cached response immediately
    Expired,                // return IdempotencyExpired error
    Collision {             // warn log + still apply (business decides)
        cached_client: String,
        incoming_client: String,
    },
}

impl IdempotencyLayer {
    fn decide(&self, key: &IdempotencyKey) -> IdempotencyDecision {
        let cache_key = (key.request_type.clone(), key.request_id);

        if let Some(cached) = self.response_cache.get(&cache_key) {
            // Exact match in cache
            if cached.client_id != key.client_id {
                return IdempotencyDecision::Collision {
                    cached_client: cached.client_id.clone(),
                    incoming_client: key.client_id.clone(),
                };
            }
            return IdempotencyDecision::Replay(cached.payload.clone());
        }

        // Not in cache; check watermark
        let wm = self.watermarks.get(&key.request_type).copied().unwrap_or(0);

        if key.request_id <= wm {
            // Already processed but evicted from cache
            IdempotencyDecision::Expired
        } else {
            IdempotencyDecision::New
        }
    }

    fn record(&mut self, key: &IdempotencyKey, response: Vec<u8>) {
        let wm = self.watermarks.entry(key.request_type.clone()).or_insert(0);
        *wm = (*wm).max(key.request_id);

        self.response_cache.put(
            (key.request_type.clone(), key.request_id),
            CachedResponse {
                payload: response,
                client_id: key.client_id.clone(),
                applied_at: SystemTime::now(),
            },
        );
    }
}
```

### Framed snapshot format

User `Service::snapshot()` continues to produce raw user state. Framework
wraps it:

```rust
#[derive(Serialize, Deserialize)]
struct FramedSnapshot {
    version: u8,  // = 1 initially
    framework_state: FrameworkState,
    user_snapshot: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct FrameworkState {
    // A-0003 state
    idempotency_watermarks: HashMap<String, u64>,
    idempotency_cache: Vec<(String, u64, CachedResponse)>,  // LRU serialised as ordered Vec

    // A-0001 state (if enabled)
    last_applied_offset: Option<Vec<u8>>,  // opaque bytes, per ingestor type
}
```

On restore, framework deserialises `FramedSnapshot`, replays `framework_state`
into `IdempotencyLayer` internals, then calls `Service::restore(user_snapshot)`.

### Expiration error

Rather than silently returning an empty response or treating the request as
new, the framework returns a distinct error:

```rust
// In ClientResponse:
message ClientResponse {
  bool success = 1;
  bytes payload = 2;
  optional LeaderInfo leader_info = 3;
  optional IdempotencyError idem_error = 4;  // new
}

message IdempotencyError {
  enum Kind {
    EXPIRED = 0;   // request_id <= watermark AND not in cache
  }
  Kind kind = 1;
  uint64 current_watermark = 2;
  string message = 3;  // "request_id=42 was processed but response evicted from cache (watermark=100). Use a newer request_id."
}
```

Client SDK raises this as a dedicated error type:

```rust
pub enum RrClientError {
    // ...existing variants...
    IdempotencyExpired {
        request_type: String,
        request_id: u64,
        watermark: u64,
    },
}
```

Clients can choose to investigate externally (DB query: "did this transfer
complete?") or to treat as unknown and alert.

### Collision detection

When `IdempotencyKey` arrives with same `(request_type, request_id)` but
different `client_id` than cached:

```rust
warn!(
    target: "rr::idempotency",
    request_type = %key.request_type,
    request_id = key.request_id,
    cached_client = %cached.client_id,
    incoming_client = %key.client_id,
    "idempotency key collision across clients — check request_id generation uniqueness"
);
```

The framework still returns the cached response (same rules as
`Replay`) — but the warn log surfaces the bug. Detection is best-effort: if
the cached entry was evicted, collision is invisible.

## Consequences

### Positive

- **Massive user-burden reduction**: Every rr user who needed idempotency
  before now gets it for free. Zero business code for the common case.
- **Snapshot-integrated by construction**: Framework owns the state; user
  can't forget to serialize it.
- **Traceable**: `client_id` + `request_type` + `request_id` in every log /
  trace = easy debugging of retry-related bugs.
- **Composable with A-0001**: Kafka at-least-once + framework dedup =
  exactly-once apply semantics at the `Service::handle()` level.
- **Opt-out preserves current users**: Existing code works unchanged; no
  forced migration.

### Negative

- **Monotonicity responsibility externalised**: Users MUST use a globally
  monotonic id source per `request_type`. Documented acceptable sources:
  - Snowflake (each producer gets distinct worker_id)
  - DB sequence / auto-increment
  - Kafka message offset (from A-0001)
  - Redis `INCR`
  - **Unacceptable**: UUID (not monotonic), `Instant::now()` nanos (clock
    skew possible)
- **Cross-client collisions are silent when cache evicts**: Warning only
  fires if the colliding entry is still in the LRU cache. Mitigation: size
  the cache generously; alert on warn-log volume.
- **Memory overhead per group**: `HashMap<String, u64>` watermarks (~100 bytes
  per distinct type) + LRU cache (configurable, default 10000 × avg response
  size). Document tuning knobs.
- **New protocol fields**: Breaking protobuf change? No — `optional` fields
  are backward compatible in protobuf3.
- **`IdempotencyExpired` is a new error surface**: Client SDKs must handle
  it; existing code paths that blindly retry will fail closed.

### Neutral

- **Opt-in design means zero cost for non-users**: Users who skip
  `IdempotencyKey` pay no memory / no CPU / no state-machine overhead.
- **Framed snapshot format**: one-time breaking change (coordinates with
  A-0001 and A-0002 implementation — all three should ship together to avoid
  multiple snapshot migrations).

## Open questions for implementation

1. **Default LRU cache size**: Per group? Per node? Proposal: per group,
   default 10,000 entries, configurable via `NodeConfig::idempotency_cache_size`.
   Rationale: 10K × 1KB avg = 10MB per group, manageable.

2. **TTL on cached responses?** Currently design is LRU-only. Should we also
   expire by age (e.g. cache for max 1 hour)? Proposal: **no TTL initially**
   — monotonic `request_id` means clients stop hitting old ids naturally.
   Add TTL only if operational data shows cache churn is a problem.

3. **Watermark garbage collection**: `request_type` strings accumulate forever
   if a type is added then removed. Proposal: **no GC initially** — types
   are fixed by business schema, typically small set. Document the caveat.

4. **Concurrent request with same key during apply**: Raft log already
   serialises; impossible within one group.

5. **Cross-group requests with same key**: Each group has its own
   `IdempotencyLayer`. If business genuinely has the same request_type across
   groups, de-dup only happens within a group. Document this.

6. **Response cache eviction strategy for `Expired` responses**: When cache
   evicts the response for request_id=N, and then request_id=N is re-sent,
   it's classified as `Expired`. Is this acceptable? **Yes** — clients with
   at-least-once semantics should be using NEW request_ids for new attempts.
   Re-sending N after its response was cleaned up means the client
   mis-implemented retry.

7. **Metrics**: Expose `rr_idempotency_replays_total`,
   `rr_idempotency_collisions_total`, `rr_idempotency_expired_total`,
   `rr_idempotency_cache_size` gauge. Wire into Prometheus endpoint.

## Usage patterns

### Pattern 1: Client-side retry with Snowflake

```rust
let mut client = RrClient::connect(vec!["10.0.0.1:51001".into()]).await?;
let snowflake = SnowflakeGen::new(worker_id=42);

loop {
    let request_id = snowflake.next_id();

    let key = IdempotencyKey {
        request_type: "transfer".into(),
        request_id,
        client_id: format!("{}+{}", app_id, pod_ip),
    };

    match client.send_request_idempotent(group_id, payload.clone(), key).await {
        Ok(resp) => return Ok(resp),
        Err(RrClientError::Transport(_)) => {
            // Retry with SAME request_id — framework deduplicates
            continue;
        }
        Err(RrClientError::IdempotencyExpired { watermark, .. }) => {
            // Response was cached but evicted. Unknown whether apply happened.
            // Check business state externally.
            return Err(UnknownOutcome);
        }
        Err(e) => return Err(e),
    }
}
```

### Pattern 2: Business-layer composite-key idempotency (skip framework)

For requests where framework's `(type, id)` model doesn't fit (e.g. dedup on
`(tenant_id, order_id, sku)`), omit `IdempotencyKey` entirely:

```rust
// Framework bypasses its idempotency layer; user Service::handle() does
// composite-key dedup internally.
client.send_request(group_id, payload).await?;
```

### Pattern 3: Kafka offset as natural monotonic id (A-0001 + A-0003 combined)

When A-0001 is active, the framework can auto-generate `IdempotencyKey` from
the Kafka offset:

```rust
IdempotencyKey {
    request_type: "input_0",         // topic name
    request_id: kafka_offset as u64, // monotonic by construction
    client_id: kafka_producer_id,    // if available from headers
}
```

This gives end-to-end exactly-once semantics: Kafka at-least-once → framework
dedup → user sees each message exactly once. Configurable via
`KafkaIngestorConfig::auto_idempotency: bool`.

## Implementation dependencies

- Depends on: **A-0002** (`Service` safety invariants documented — framework
  needs guarantees about snapshot atomicity)
- Synergistic with: **A-0001** (Kafka offset as automatic request_id source)
- Shares implementation with A-0001: the `FramedSnapshot` format is
  introduced by both. Ship together.

## Changelog

| Date | Change | Trigger |
|---|---|---|
| 2026-04-24 | Initial draft per design discussion | Design discussion with Louis |
| 2026-04-24 | Refined dedup scope: (request_type, request_id), client_id diagnostic-only | Louis clarified scope |
| 2026-04-24 | Added opt-out behaviour: requests without IdempotencyKey flow normally | Louis confirmed skip-framework for business-composite-key cases |
