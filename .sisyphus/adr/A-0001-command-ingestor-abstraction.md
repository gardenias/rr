---
title: "CommandIngestor trait + rr-ingestor-kafka reference impl"
status: proposed
created: 2026-04-24
updated: 2026-04-24
tags: [adr, architecture, input, kafka, mq]
---

# A-0001 — CommandIngestor Abstraction

## Context

Today, rr accepts commands **exclusively** via the gRPC `ClientService.Request`
RPC. The current entry-point matrix:

| Transport | Service | Purpose |
|---|---|---|
| gRPC | `ClientService.Request` | Business writes (only business entry) |
| gRPC | `ClientService.GetClusterInfo` | Topology discovery |
| gRPC | `RaftService.*` | Inter-node consensus (internal) |
| gRPC | `AdminService.*` | Operations (optional separate port) |
| HTTP | `/health` `/ready` `/metrics` | Observability only |

There is zero MQ integration: no Kafka, NATS, Pulsar, RocketMQ, RabbitMQ, or
AMQP dependencies anywhere in the workspace. Users who want event-sourced write
paths must build their own bridge (Kafka consumer → `RrClient.send_request()`),
which re-invents leader-exclusive consumption, offset checkpointing, and seek-on-
recovery for every project.

Verified against the codebase on 2026-04-24:

- All `Cargo.toml` files: no MQ-related dependencies
- Workspace grep for `kafka` / `rdkafka` / `kmq` / `consumer` / `subscribe`:
  zero matches (only false positives: `tokio::sync::broadcast::subscribe` and
  `tracing-subscriber`)

## Problem

rr should support **single-topic-single-partition per Raft group** Kafka-style
command ingestion, with the following constraints agreed in design discussion:

1. **Topic layout**: Each Raft group `G` consumes a dedicated topic named
   `input_<G>` (or configurable pattern) with a single partition. This is
   "Plan A" — `N Raft groups ↔ N Kafka partitions across N topics`.
2. **Leader-exclusive consumption**: Only the current leader of group `G` reads
   from `input_<G>`. Followers get data via Raft log replication, not direct
   consumption. This preserves rr's existing replication model.
3. **Offset durability**: On recovery, the new leader must resume consumption
   from the correct offset — no duplicates, no gaps.
4. **Core crate cleanliness**: rr-core must not depend on `librdkafka` (C
   toolchain). MQ integration must be pluggable.
5. **Pulsar / RocketMQ future support**: Design must accommodate additional MQ
   backends as future crates.

## Background: Why leader-exclusive?

A Raft group has one leader that accepts writes and replicates. If both leader
and followers consumed Kafka independently:

- **Data divergence risk**: Followers could process messages the leader hasn't
  yet committed, breaking linearizability
- **Double-apply**: Messages would enter the state machine via both Kafka and
  Raft log replication
- **Offset confusion**: Kafka consumer groups would have `N` members but Raft
  has a single committing leader

The correct pattern is: leader consumes Kafka → proposes each message as a Raft
entry → entry replicated to followers via `AppendEntries` → all replicas apply
from Raft log. Followers are **isolated** from Kafka.

## Background: Why offset-in-log, not offset-in-snapshot?

Rejected approach: "Snapshot stores last_consumed_offset; followers resume from
there on leader transition."

Why this fails:

- Snapshots are **asynchronous** — triggered by `max_in_memory_logs` threshold
  or manual `TriggerSnapshot` RPC. Many Raft entries may be applied between
  snapshots.
- If the leader fails mid-consumption after entry `offset=100` is committed and
  applied but the next snapshot only records `offset=50`, recovery would
  **re-consume** `offset=51..100` — duplicate processing.

Correct approach: **Every Raft log entry carries its source offset**. Snapshot
records `last_applied_offset` which equals the offset of the entry at
`last_applied_log_id`. On recovery:

```
1. Install snapshot → state machine at (last_applied = N, last_offset = O_N)
2. Replay log[N+1..committed]   ← each entry has its own offset, state machine
                                   tracks last_offset by reading from entry
3. After all replay done, leader seeks Kafka consumer to last_offset + 1
```

This guarantees **exactly-once apply** semantics from Kafka's
**at-least-once** delivery (framework deduplicates via Raft log position).

## Options

### Option A — Integrate Kafka directly into rr-core with feature flag

```toml
[dependencies]
rr-core = { version = "0.3", features = ["kafka"] }
```

Pros: single dependency for users

Cons:

- `rdkafka` pulls C toolchain (cmake, OpenSSL dev headers) into every rr-core
  build via cargo feature resolution. Violates rr's "pure Rust where possible"
  principle.
- Feature flags bleed into conditional compilation across rr-core — maintenance
  burden and testing matrix explosion.
- Adding Pulsar later requires another feature flag; flags start fighting each
  other.
- MSRV risk: `rdkafka` MSRV may diverge from rr's.

### Option B — `CommandIngestor` trait in rr-core + separate reference impl crate

```
crates/
├── rr-core/                 ← defines CommandIngestor trait (no MQ deps)
└── rr-ingestor-kafka/       ← depends on rdkafka, implements CommandIngestor
```

User Cargo.toml:

```toml
rr-server = "0.3"
rr-ingestor-kafka = "0.3"   # opt-in; users who don't need it add nothing
```

Pros:

- rr-core stays pure Rust, zero C deps
- Multiple MQ backends are independent crates, no flag fights
- Users who don't need MQ pay nothing (no feature flag overhead)
- Testing each crate in isolation is simpler

Cons:

- Users who need Kafka must add one extra dependency
- Two crates to version-sync (but Cargo workspace handles this)

### Option C — No framework support, let users bridge in application code

Pros: zero framework complexity

Cons:

- Every user reinvents leader-exclusive consumer + offset checkpoint +
  seek-on-new-leader + lifecycle (start/stop on leadership change)
- Easy to get wrong (double-consume, missed messages after leadership change)
- Contradicts rr's "framework handles distributed concerns, users handle
  business" principle

## Decision

**Adopt Option B.** Define `CommandIngestor` trait in rr-core. Ship
`rr-ingestor-kafka` as a reference implementation in the same workspace.

Decision confirmed in discussion: **NATS / Pulsar / RocketMQ are deferred**.
Only Kafka is required for initial release.

### Trait specification

> **Object-safety note**: The `CommandIngestor` trait uses an associated
> `Offset` type, which makes it non-object-safe on its own. The framework
> provides a complementary `DynCommandIngestor` trait (in the same module)
> that erases `Offset` to `Vec<u8>` via bincode, and a blanket
> `IngestorAdapter<I>` wrapper with
> `impl<I: CommandIngestor + 'static> DynCommandIngestor for IngestorAdapter<I>`.
> Framework internals (e.g., `RaftGroupManager`) store
> `Box<dyn DynCommandIngestor>`. Users implement `CommandIngestor` with
> typed offsets; the framework wraps it transparently.

```rust
// In rr-core

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

/// Ingress command pulled from an external MQ source.
pub struct IngressCommand<O> {
    /// Source-system offset (opaque to rr-core; Kafka offset, NATS sequence, etc.)
    pub offset: O,

    /// Command payload to be proposed as a Raft entry.
    pub payload: Vec<u8>,

    /// Optional correlation key for async response delivery (see A-0003).
    pub response_key: Option<Vec<u8>>,
}

/// Source of commands for a single Raft group.
///
/// # Safety Invariants (MUST uphold)
///
/// 1. **Single-consumer**: Only ONE instance of this trait exists per
///    `(group_id, consumer)` pair at any time. The framework guarantees this by
///    starting the ingestor on leader election and stopping it on step-down.
///
/// 2. **Ordered offsets**: `poll()` MUST return commands in strictly
///    monotonically increasing `offset` order. Gaps are permitted (e.g. Kafka
///    topic compaction), but regression is forbidden.
///
/// 3. **seek() idempotency**: Calling `seek(O)` multiple times with the same
///    `O` must have identical effect. This is required because the framework
///    may retry `seek()` on transient errors.
///
/// 4. **No-commit-before-apply**: Implementations MUST NOT call any source-
///    system "commit offset" API from inside `poll()` or `seek()`. Only
///    `commit_applied()` may commit offsets to the source system.
#[async_trait]
pub trait CommandIngestor: Send + Sync {
    /// Offset type opaque to the framework; must be ordered, serializable, and
    /// embeddable in Raft entries.
    type Offset: Serialize + DeserializeOwned + Ord + Copy + Debug + Send + Sync + 'static;

    /// Pull the next batch of commands.
    ///
    /// May block awaiting new messages. Returns empty `Vec` if the underlying
    /// source has nothing available within a reasonable timeout (implementation-
    /// defined, but SHOULD not block indefinitely).
    async fn poll(&mut self) -> Result<Vec<IngressCommand<Self::Offset>>, RrError>;

    /// Reset the read position to the specified offset.
    ///
    /// Called by the framework when a new leader takes over, passing
    /// `last_applied_offset + 1`. Subsequent `poll()` calls must return
    /// commands at or after this offset.
    async fn seek(&mut self, offset: Self::Offset) -> Result<(), RrError>;

    /// Notify the implementation that commands up to (and including) `offset`
    /// have been durably applied to the state machine.
    ///
    /// Implementations MAY use this to checkpoint consumer-group offsets in
    /// the source system (e.g. Kafka `commit_message`) for external
    /// observability, but the framework does NOT rely on this for correctness.
    /// Framework correctness relies on offsets embedded in Raft log entries
    /// and in snapshots.
    async fn commit_applied(&mut self, offset: Self::Offset) -> Result<(), RrError>;
}
```

### Integration semantics

**Lifecycle tied to Raft leadership** (framework responsibility):

```
Node boot
  │
  ▼
Group G becomes Leader
  │
  ├── Framework reads last_applied_offset from state machine
  ├── Framework calls ingestor.seek(last_applied_offset + 1)
  └── Framework spawns poll-loop task:
       loop {
         let batch = ingestor.poll().await?;
         for cmd in batch {
           let entry_payload = RaftPayload { cmd.offset, cmd.payload };
           raft.client_write(bincode(entry_payload)).await?;
           // client_write blocks until committed+applied; exactly-once apply
         }
         // Optional: commit offsets externally for observability
         if let Some(last) = batch.last() {
           ingestor.commit_applied(last.offset).await.ok();
         }
       }
  │
  ▼
Step-down event
  │
  ├── Framework cancels poll-loop task
  └── Framework drops ingestor instance
```

**Framework-owned state machine wrapper** extracts `offset` from each entry
before dispatching to user `Service::handle()`. User service remains unaware of
the MQ source — it receives `payload` bytes identical to the gRPC path.

```rust
// Framework state machine wrapper (simplified)
async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
    let wrapped: RaftPayload = bincode::decode(entry)?;
    if let Some(offset) = wrapped.offset {
        self.last_applied_offset = Some(offset);
    }
    self.service.handle(wrapped.payload).await
}

async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
    let user_snap = self.service.snapshot().await?;
    let framed = FramedSnapshot {
        framework_state: FrameworkState {
            last_applied_offset: self.last_applied_offset,
            /* + idempotency state from A-0003 */
        },
        user_snap,
    };
    bincode::encode(&framed)
}
```

### `rr-ingestor-kafka` reference implementation outline

- Depends on `rdkafka = { version = "0.36", features = ["cmake-build"] }`
- Single-partition subscription via `Consumer::assign` (bypasses consumer groups
  to avoid rebalance semantics conflicting with Raft leadership)
- `Offset = i64` (Kafka offset type)
- `poll()` uses `StreamConsumer::stream().next()` with configurable timeout
- `seek()` uses `Consumer::seek_partitions`
- `commit_applied()` uses `CommitMode::Async` commit with `Offsets::from_offsets`
- TLS/SASL configuration via `rr-ingestor-kafka::KafkaIngestorConfig`

### Topic layout convention

```
Raft Group 0 ↔ topic input_0 (partition 0)
Raft Group 1 ↔ topic input_1 (partition 0)
...
Raft Group N ↔ topic input_N (partition 0)
```

Topic naming is configurable via `KafkaIngestorConfig::topic_pattern` (default:
`input_{group_id}`).

## Consequences

### Positive

- **Event-sourced write paths** become first-class. Users can feed rr from
  existing Kafka pipelines without bespoke bridges.
- **Disaster recovery second layer**: Kafka retention acts as a backup. If rr
  cluster is destroyed, replay from Kafka offset `0` (or last-known-good snapshot
  offset) to rebuild state. This pairs naturally with A-0003 framework
  idempotency — replays are automatically deduplicated.
- **rr-core stays minimal**: No C deps, no feature-flag maze.
- **Parallel backend support**: `rr-ingestor-pulsar`, `rr-ingestor-rocketmq`
  become community contributions without touching rr-core.
- **Clear separation of concerns**: Ingestor handles source-system protocol;
  framework handles leader-exclusive lifecycle and offset-in-Raft-log.

### Negative

- **Leadership-transition latency spike**: New leader must `seek()` before
  first `poll()`. For Kafka with high offset retention, this is ~50-200ms. For
  Pulsar cursors similar. Document in operations runbook.
- **Serialization overhead**: Every entry now carries `Option<Offset>`
  (~16 bytes for Kafka `i64`). Negligible for write-heavy workloads, more
  noticeable for tiny messages.
- **Framework state machine wrapper complexity**: Snapshots now have a framed
  format (`FrameworkState + UserSnapshot`). This is a **breaking change** for
  any existing snapshot files — migration required.
- **Operational surface**: Kafka broker admin is now part of rr deployment; new
  failure modes (broker unreachable, topic deleted, offset out of range).
- **Misconfiguration risk**: Users who accidentally deploy multiple rr clusters
  consuming the same topic would get data duplication. Document
  `topic_pattern` uniqueness and suggest per-cluster topic prefixes.

### Neutral

- **New public trait in rr-core** → minor version bump (trait addition, not
  removal). Existing users unaffected.
- **New crate in workspace** → CI matrix grows by one job.

## Implementation dependencies

This ADR depends on:

- **A-0002** safety invariants (Service::snapshot/apply) being documented and
  enforced, because the framework state machine wrapper relies on atomic
  snapshot capture to snapshot `last_applied_offset` alongside user state.
- **A-0003** idempotency layer is NOT strictly required, but Kafka's at-least-
  once semantics make A-0003 significantly more valuable when A-0001 is
  deployed. Recommendation: ship A-0001 and A-0003 together.

## Open questions for implementation

1. **Framing format version**: `FramedSnapshot` needs a `version: u8` field
   for forward compatibility. Initial value `1`. Needs a documented migration
   story if ever bumped.
2. **Poll batch size**: Default batch size for `poll()` return? Proposal: 100
   messages, configurable via ingestor config.
3. **Backpressure**: If Raft log write is slow, ingestor keeps accumulating in
   memory. Propose a bounded channel between `poll()` output and Raft proposal
   input (capacity: configurable, default 1000 messages).
4. **Metrics**: Per-group `rr_ingestor_lag_offset` gauge, `rr_ingestor_seek_total`
   counter, `rr_ingestor_poll_duration_seconds` histogram. Wire into existing
   Prometheus endpoint.
5. **Error handling**: What if `poll()` returns `Err`? Retry with backoff? Step
   down as leader? Proposal: retry with exponential backoff up to 30s, then log
   warn and keep retrying (do NOT step down — Kafka being down shouldn't
   destabilise Raft).

## Changelog

| Date | Change | Trigger |
|---|---|---|
| 2026-04-24 | Initial draft | Design discussion with Louis |
