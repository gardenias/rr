---
title: Framework Idempotency
description: Exactly-once apply on top of at-least-once delivery via (request_type, request_id) keys
---

# Framework Idempotency

> **Available since**: v0.3.0 — see [v0.2 → v0.3 migration guide](../deployment/v0.2-to-v0.3-migration.md).
> **Reference ADR**: [A-0003](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0003-framework-idempotency-layer.md).

The idempotency layer turns rr's *at-least-once* client retries (and Kafka's at-least-once delivery) into **exactly-once apply** semantics for the user `Service::handle()`. Clients opt in per request by attaching an `IdempotencyKey`; requests without a key go through the existing path unchanged.

---

## Why you might need this

The framework already retries failed RPCs at the client SDK level (configurable backoff on `RrClient`). But if a write *commits* on the leader and the response is lost mid-flight (network blip, leader step-down between `client_write` returning and the gRPC reply landing), the client SDK retries the same request — and without idempotency the user `Service::handle()` runs **twice**, double-incrementing counters or double-applying transfers.

Same hazard with Kafka: the Kafka ingestor (see [Kafka Input](kafka-input.md)) re-fetches messages on leader transitions whose snapshot lags the live state. Without dedup, those replayed offsets re-apply.

The idempotency layer closes both gaps.

---

## Identity model

A request is identified by:

```rust
pub struct IdempotencyKey {
    pub request_type: String,        // namespace, e.g. "transfer"
    pub request_id: u64,             // monotonic ID within the namespace
    pub client_id: Option<String>,   // diagnostic ONLY — not part of dedup
}
```

**Dedup identity is `(request_type, request_id)`.** Two keys with the same pair are treated as the same logical request. `client_id` is recorded for forensics — if two different clients reuse a key, the framework returns the cached response (dedup wins) but emits an `op="idempotency_collision"` warning to the audit log.

`PartialEq` and `Hash` on `IdempotencyKey` ignore `client_id` — `key1 == key2` iff they share the dedup tuple.

### Watermarks

For each `request_type` the layer tracks `watermarks[type] = u64`, the largest `request_id` ever applied. A `request_id` at or below the watermark whose response has been evicted from the LRU cache returns `IdempotencyDecision::Expired` instead of being silently replayed (preventing a stale retry from corrupting state after the response cache rolled).

The watermark is **per-`request_type`** — different namespaces are independent. Use this to isolate streams: `"transfer"` vs `"create_user"` vs `"input_42"` (the Kafka topic for group 42) all maintain their own monotone IDs.

The very first request for a `request_type` is always `New`, even if its `request_id == 0` (Kafka offsets start at 0). The watermark distinguishes "never seen" from "seen and forgotten" by entry presence, not by value.

### Decisions

When `decide(key)` is consulted on `apply`, one of four outcomes is returned:

| Outcome | When | What user sees |
|---|---|---|
| `New` | Cache miss + (no watermark OR `request_id > watermark`) | `Service::handle()` runs; response cached under the key |
| `Replay(bytes)` | Cache hit + same `client_id` | Cached response returned; `Service::handle()` does NOT run |
| `Collision { … }` | Cache hit + different `client_id` | Cached response returned; warning emitted |
| `Expired { watermark }` | Cache miss + `request_id <= watermark` | `RrError::IdempotencyExpired` returned to the client |

---

## Client-side: opt in per request

```rust
use rr_client::RrClient;
use rr_core::idempotency::IdempotencyKey;

let client = RrClient::connect(&["node1:50051"]).await?;

// Without idempotency (legacy path, double-apply on retry possible):
let resp = client.send_request(group_id, payload.clone()).await?;

// With idempotency (exactly-once on retry):
let key = IdempotencyKey::new("transfer", request_id_from_app);
let resp = client.send_request_idempotent(group_id, payload, key).await?;
```

The framework retries on transient errors as before; the second attempt finds the key cached on the leader and returns the original response. If the response was evicted from the cache (LRU bound, default 10 000 entries) AND the watermark moved past `request_id`, the SDK surfaces `RrError::IdempotencyExpired` — your application then knows to pick a fresh `request_id` rather than blindly retry.

### How to allocate `request_id`

The `request_id` MUST be monotonic within the `request_type` namespace. Common patterns:

- **Per-process counter**: a `AtomicU64` started at 0 (or persisted across restarts in your DB). Simple, but doesn't survive process restart unless persisted.
- **External coordinator**: Snowflake-style time-bucketed ID, or pulled from a dedicated `request_id` table.
- **Kafka offset**: when the request originated from a Kafka message, use the offset directly via `auto_idempotency` (no client-side allocation needed).

---

## Auto-idempotency for Kafka ingestion

When commands flow in via `rr-ingestor-kafka`, the framework can derive `IdempotencyKey` from each Kafka offset automatically — no client work required.

```rust
use rr_ingestor_kafka::{KafkaIngestor, KafkaIngestorConfig, kafka_offset_to_request_id};
use rr_raft::ingestor_runner::{AutoIdempotency, OffsetMapError};

let cfg = KafkaIngestorConfig {
    bootstrap_servers: vec!["broker1:9092".into()],
    ..Default::default()
};
let ingestor = KafkaIngestor::new(group_id, cfg.clone()).await?;

mgr.attach_ingestor_with_auto_idempotency(
    group_id,
    Box::new(ingestor),
    AutoIdempotency {
        request_type: cfg.topic_for(group_id),         // e.g. "input_42"
        offset_to_request_id: Box::new(|b| {
            kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
        }),
    },
).await?;
```

With auto-idempotency on, re-fetched Kafka messages (after step-down, snapshot-driven seek lag, or network retry) are deduped by the framework — `Service::handle()` runs at most once per logical Kafka offset.

The `request_type` MUST be unique per ingestor stream (recommended: the topic name). The `offset_to_request_id` closure MUST be **monotone** in offset order — hash-based mappings break the watermark by letting new offsets fall below it. Use `kafka_offset_to_request_id` for Kafka i64 offsets; it is **fail-closed**: decode failures and negative sentinel offsets (`INVALID_OFFSET = -1001`) return an error, and the ingestor supervisor exits without proposing or committing the offending message — a sentinel can never silently masquerade as request id `0` and poison the watermark.

---

## Server-side: persistence

The idempotency layer is captured into the framed snapshot's `framework_state.idempotency` slot, so:

- **Snapshot/restore** preserves both watermarks AND the LRU cache.
- **Leader transitions** carry the layer forward via Raft log replay + snapshot install.
- **Node restarts** rebuild the layer from the latest snapshot + log replay.

The cache size is fixed at `DEFAULT_IDEMPOTENCY_CACHE_SIZE = 10 000` entries per group (configurable via `ServiceStateMachineAdapter::with_idempotency_capacity`). Tune based on your retry window: the cache must outlive the longest expected client retry interval, otherwise legitimate retries surface as `IdempotencyExpired`.

---

## Metrics

Per-group counters/gauges (all labeled `group_id`):

| Metric | Type | When |
|---|---|---|
| `rr_idempotency_replays_total` | counter | Cache hit + same client_id (Replay) |
| `rr_idempotency_collisions_total` | counter | Cache hit + different client_id |
| `rr_idempotency_expired_total` | counter | Cache miss + below watermark |
| `rr_idempotency_cache_size` | gauge | LRU cache `len()` after each apply |

A nonzero `_collisions_total` signals two clients are using the same `(request_type, request_id)` independently — usually a coordination bug in your ID allocation.

---

## When NOT to use this

- **Stateless reads**: idempotency is for write-side dedup; reads are naturally idempotent.
- **Streaming writes where ordering > exactly-once**: if you're OK with at-least-once delivery (common for telemetry), the keyless path is faster (no cache, no decode).
- **Composite identity**: if your "logical request" needs more than `(type, id)` (e.g. a multi-field business key), build dedup in your `Service::handle()` directly. The framework deliberately keeps the surface narrow.

---

## See also

- [`Service` Trait Guide](service-trait.md) — what the user-facing handler looks like
- [Kafka Input](kafka-input.md) — using `rr-ingestor-kafka` with auto-idempotency
- [Client SDK Guide](client-sdk.md) — `RrClient::send_request_idempotent`
- ADR [A-0003](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0003-framework-idempotency-layer.md) — design rationale
