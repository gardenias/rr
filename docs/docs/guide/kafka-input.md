---
title: Kafka Input (CommandIngestor)
description: Drive an rr Raft group from a Kafka topic with leader-exclusive consumption and exactly-once apply
---

# Kafka Input — `rr-ingestor-kafka`

> **Available since**: v0.3.0 (in development on `main`, not yet tagged — see the [v0.2 → v0.3 Migration Guide](../deployment/v0.2-to-v0.3-migration.md)).
> **Reference ADR**: [A-0001](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0001-command-ingestor-abstraction.md).

The `rr-ingestor-kafka` crate makes a Kafka topic a first-class write source for an rr Raft group: only the current leader of group `G` reads from `input_{G}` (single partition); followers receive every command via Raft log replication. Combined with the [framework idempotency layer](idempotency.md), Kafka's at-least-once delivery becomes **exactly-once apply** at the user `Service::handle()`.

---

## Plan: one topic per Raft group

The default topic-name pattern is `input_{group_id}` (configurable). Every group gets a dedicated topic with **exactly one partition**:

```
Raft Group 0 ↔ topic input_0 (partition 0)
Raft Group 1 ↔ topic input_1 (partition 0)
…
Raft Group N ↔ topic input_N (partition 0)
```

Single-partition is mandatory. Multiple partitions per topic would let multiple consumers read concurrently, which contradicts leader-exclusive semantics.

The framework uses Kafka's `assign()` (not `subscribe()`) to bind to the partition. Consumer-group rebalances cannot move us off the partition while we hold leadership — the supervisor is the sole arbiter of when consumption starts and stops.

---

## Lifecycle

```text
Node boots
    │
    ▼
Group G becomes Leader
    │
    ├─ Framework reads last_applied_offset from the framed snapshot
    ├─ Framework calls ingestor.seek(last_applied_offset + 1)
    └─ Framework spawns the poll loop:
       loop {
         let batch = ingestor.poll().await?;
         for cmd in batch {
           let envelope = FramedEntry { offset, payload, idem_key }.encode();
           raft.client_write(envelope).await?;     // commit + apply (blocking)
         }
       }
    │
    ▼
Step-down event
    │
    ├─ Framework cancels the poll loop
    └─ Framework drops the ingestor instance
```

The poll loop is naturally rate-limited by Raft commit throughput — `client_write` blocks until commit + apply, so the ingestor cannot run ahead of consensus.

If `seek_offset` fails (corrupt persisted offset, broker unreachable), the supervisor exits cleanly **without** falling through to a free-running poll. Without this guard, Kafka's `auto.offset.reset = "earliest"` would silently replay from offset 0; the supervisor refuses that footgun. Operators see `rr_ingestor_seed_failures_total{group_id}` incremented and a structured error in the supervisor log.

---

## Build prerequisites

The `rr-ingestor-kafka` crate's default features compile on every CI host with no native dependencies — only the in-memory `MockKafkaIngestor` (used in tests and examples) and the public config types.

The real backend is gated behind the `rdkafka` feature:

```toml
# Cargo.toml
[dependencies]
rr-ingestor-kafka = { version = "0.3", features = ["rdkafka"] }
```

Enabling `rdkafka` pulls `rdkafka` with `cmake-build`, which compiles the bundled librdkafka. **`cmake` must be on the build host**:

```bash
# Linux CI
sudo apt-get install -y cmake

# macOS
brew install cmake
```

---

## Wiring an ingestor to a Raft group

```rust
use std::sync::Arc;

use rr_ingestor_kafka::{KafkaIngestor, KafkaIngestorConfig, kafka_offset_to_request_id};
use rr_raft::ingestor_runner::{AutoIdempotency, OffsetMapError};
use rr_raft::RaftGroupManager;

let mgr: Arc<RaftGroupManager> = /* … from RrNode setup */;

let cfg = KafkaIngestorConfig {
    bootstrap_servers: vec!["broker1:9092".into()],
    topic_pattern: "input_{group_id}".to_string(),
    max_batch_size: 100,
    poll_timeout: std::time::Duration::from_millis(200),
    ..Default::default()
};

let ingestor = KafkaIngestor::new(group_id, cfg.clone()).await?;

mgr.attach_ingestor_with_auto_idempotency(
    group_id,
    Box::new(ingestor),
    AutoIdempotency {
        request_type: cfg.topic_for(group_id),
        offset_to_request_id: Box::new(|b| {
            kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
        }),
    },
).await?;
```

`attach_ingestor_with_auto_idempotency` is the **recommended** path — auto-idempotency closes the duplicate-apply gap on leader transitions (see [Idempotency](idempotency.md)). Use the bare `attach_ingestor` only when your `Service::handle()` is itself trivially idempotent.

---

## Security: TLS + SASL

`KafkaSecurityConfig` maps onto rdkafka's `security.protocol` / `ssl.*` / `sasl.*` properties. Sensitive values are pulled from environment variables (the `*_env` fields name the variable to read at attach time) — secrets never live in config files:

```rust
use rr_ingestor_kafka::{KafkaIngestorConfig, KafkaSecurityConfig, SecurityProtocol};

let cfg = KafkaIngestorConfig {
    bootstrap_servers: vec!["broker.prod:9094".into()],
    security: Some(KafkaSecurityConfig {
        protocol: SecurityProtocol::SaslSsl,
        ssl_ca_path: Some("/etc/ssl/kafka/ca.pem".into()),
        ssl_cert_path: None,
        ssl_key_path: None,
        ssl_key_password_env: None,
        sasl_mechanism: Some("SCRAM-SHA-256".into()),
        sasl_username: Some("rr-prod".into()),
        sasl_password_env: Some("KAFKA_PASSWORD".into()),
    }),
    ..Default::default()
};
```

This matches the [F2 security pattern](../deployment/audit-log.md): plaintext-OK metadata in config, secrets in env vars referenced by `*_env`. A missing or empty env var fails fast at `KafkaIngestor::new` with `KafkaIngestorError::MissingSecret`.

---

## Implementing your own ingestor

Any source that satisfies `CommandIngestor` is a valid command stream. To plug a Pulsar/NATS/HTTP-cursor source:

```rust
use async_trait::async_trait;
use rr_core::ingestor::{CommandIngestor, IngressCommand};
use rr_core::error::RrError;

struct MyIngestor { /* … */ }

#[async_trait]
impl CommandIngestor for MyIngestor {
    type Offset = i64;  // any Serialize + DeserializeOwned + Ord + Copy + Debug + Send + Sync

    async fn seek(&mut self, offset: Option<i64>) -> Result<(), RrError> { /* … */ }
    async fn poll(&mut self, max_batch_size: usize)
        -> Result<Vec<IngressCommand<i64>>, RrError> { /* … */ }
    async fn commit_applied(&mut self, offset: i64) -> Result<(), RrError> { /* … */ }
}
```

Safety invariants the implementer MUST uphold (per ADR A-0001):

1. **Single-consumer**: only one instance of your ingestor exists per `(group_id, source)` pair at any time. The framework guarantees this — your ingestor is constructed when the node becomes leader and dropped when it steps down.
2. **Ordered offsets**: `poll()` MUST return commands in strictly monotonically increasing `offset` order. Gaps OK (Kafka topic compaction); regression forbidden.
3. **`seek()` idempotency**: same `seek(O)` called twice MUST have identical effect.
4. **No-commit-before-apply**: `poll()` and `seek()` MUST NOT call any source-side "commit" API. Only `commit_applied()` may.

---

## Metrics

| Metric | Type | When |
|---|---|---|
| `rr_ingestor_seek_total` | counter | `seek()` invoked on leader promotion |
| `rr_ingestor_poll_duration_seconds` | histogram | Each `poll()` invocation |
| `rr_ingestor_seed_failures_total` | counter | `seek()` failed (supervisor exited) |
| `rr_ingestor_step_down_drops_total` | counter | Batch-tail commands dropped on step-down (re-fetched on next leader; deduped by auto-idempotency) |

Pair `rr_ingestor_step_down_drops_total` with `rr_idempotency_replays_total` to see how often Kafka re-delivery is being absorbed by the dedup layer.

---

## Failure modes & operator response

| Symptom | Likely cause | Action |
|---|---|---|
| `rr_ingestor_seed_failures_total > 0` | Corrupt persisted offset OR broker unreachable at promotion | Check broker health; if persistent, use `rr-admin group reset-ingestor-offset --group <G> --offset <N>` to override the persisted offset (`--offset -1` clears it) |
| Endless `ingestor poll failed; backing off` warnings | Broker outage | Backoff capped at 30s; consensus is unaffected. Restore broker. |
| `rr_idempotency_collisions_total > 0` with auto-idempotency | Two ingestors attached to the same group with the same `request_type` | Ensure `topic_for(group_id)` is unique per stream |
| Out-of-range seek warning in logs | Kafka retention trimmed past `last_applied_offset` | The backend validates seeks against `fetch_watermarks` before consuming; if the watermark fetch itself fails it logs `fetch_watermarks failed; skipping seek-range validation` and proceeds — monitor lag externally |

---

## Limitations (v0.3.0)

- **No bundled Pulsar / NATS / RocketMQ backends** — only Kafka. Other backends are out-of-tree (community contributions welcome).
- **Single partition per group** is mandatory; the framework explicitly does not support partition-fanout. To shard load, use multiple groups (each its own topic).
- **`P0-1` resolved** — `seed_offset` first calls `refresh_snapshot_if_stale()`, which triggers a fresh snapshot and waits (up to a bounded deadline) when the latest snapshot lags `last_applied`, so a newly-promoted leader no longer re-fetches large lagging ranges from Kafka. Any residual re-fetch is still deduped by auto-idempotency at apply time.

---

## See also

- [`CommandIngestor` trait docs](https://docs.rs/rr-core/latest/rr_core/ingestor/trait.CommandIngestor.html)
- [Idempotency Guide](idempotency.md)
- [`Service` Trait Guide](service-trait.md)
- ADR [A-0001](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0001-command-ingestor-abstraction.md) — design rationale
