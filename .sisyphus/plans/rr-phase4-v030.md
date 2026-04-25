# Phase 4: v0.3.0 — I/O Enhancements

## TL;DR

> **Goal**: Implement ADRs A-0001 + A-0002 + A-0003 together in v0.3.0, cutting
> a single breaking snapshot-format migration window. Delivers:
> (1) `CommandIngestor` trait + `rr-ingestor-kafka` for Kafka-based command
> input; (2) documented Service safety invariants + DurabilityPolicy rename +
> `node-reset` disaster recovery; (3) optional framework-level idempotency
> with `(request_type, request_id)` dedup and auto-snapshot.
>
> **Timeline**: Tier 0 (~3 days, shippable as 0.2.x patches) → Tier 1-4
> (~3-4 weeks to v0.3.0).
>
> **Status**: Drafted 2026-04-24 after I/O design discussion.
>
> **Prerequisites**:
> - `v0.2.0` final cut (depends on Phase 3.1 Tier 0 being complete)
> - ADRs A-0001 / A-0002 / A-0003 reviewed + accepted
> - Agreement: A-0001 + A-0003 ship together to share `FramedSnapshot` format

---

## ADR Mapping

| ADR | Delivered by tiers |
|---|---|
| [A-0001 CommandIngestor](../adr/A-0001-command-ingestor-abstraction.md) | T1 (framework wrapper) + T3 (ingestor crate) |
| [A-0002 Snapshot contract + recovery](../adr/A-0002-snapshot-consistency-and-recovery.md) | T0.1 + T0.2 + T0.3 |
| [A-0003 Idempotency](../adr/A-0003-framework-idempotency-layer.md) | T1 (framework wrapper) + T2 (idempotency layer) |

## Dependency Graph

```
T0.1 ─┐ (docs only, parallel)
T0.2 ─┤
T0.3 ─┘

T1.1 ──> T1.2 ──> T1.3           (framework state machine refactor)
            │
            ├───> T2.1 → T2.2 → T2.3 → T2.4 → T2.5        (A-0003 idempotency)
            │
            └───> T3.1 → T3.2 → T3.3 → T3.4 → T3.5 → T3.6 (A-0001 Kafka)

T2.* + T3.* ──> T4.1 → T4.2 → T4.3 → T4.4 (synergies + docs)
                                   │
                                   └──> T5.1 → T5.2 → T5.3 (release)
```

Tier 0 is independent of everything else. Tier 1 blocks Tiers 2 & 3. Tiers 2
and 3 are **parallel** after T1 lands. Tier 4 gates Tier 5.

---

## Tier 0 — Docs + Quick Wins (Shippable in 0.2.x Patches)

Independent of the framework refactor. Can ship as `v0.2.1` / `v0.2.2` patches
while v0.3.0 work proceeds. ~3 days total.

### T0.1 — Document `Service` safety invariants

- **What**: Add "Safety Invariants" rustdoc section to `Service` trait and
  `StateMachine` trait. Three invariants: (1) `snapshot()` atomic capture, (2)
  `apply()` completes state updates before returning, (3) `restore()` fully
  replaces state. Include incorrect/correct code examples inline.
- **Why**: OpenRaft source verification (see A-0002 §Contract 1) confirmed
  OpenRaft delegates atomicity to the implementer. Current docs are silent on
  this; users will hit silent data corruption without warning.
- **Files**:
  - `crates/rr-core/src/service.rs`
  - `crates/rr-core/src/state_machine.rs`
  - `docs/docs/guide/service-trait.md` — add "Correct implementation patterns"
  - `docs/docs/guide/state-machine-trait.md` — same
- **Verification**: `cargo doc --all` renders; read through rendered HTML;
  code examples compile as `#[doc]` tests.
- **Effort**: ~1 day (writing + review).
- **Blocker**: None.
- **Shippable as**: `v0.2.1` patch.

### T0.2 — Rename `DurabilityPolicy` + add startup warning

- **What**: `Balanced` → `ReplicatedOnly` (new canonical name), `Fast` →
  `TestOnly`. Keep `Balanced`/`Fast` as `#[deprecated(since="0.3.0")]` aliases
  for one minor version. Emit `tracing::warn!` at `RocksStorage::open()` when
  non-Strict is used.
- **Why**: Current names mislead — "Balanced" implies "safe with tradeoffs"
  but actually violates OpenRaft's durability contract (any non-fsync policy
  is unsafe per `getting-started.md#L192-L195`).
- **Files**:
  - `crates/rr-storage/src/rocks.rs` — enum + deprecated aliases + warn log
  - `crates/rr-storage/src/lib.rs` — re-exports
  - All test code using old names — mechanical replacement
  - `docs/docs/reference/config-reference.md` — policy descriptions
  - `docs/docs/guide/storage-engines.md` — usage guidance
  - `CHANGELOG.md` — note the deprecation
- **Verification**: `cargo test --workspace` passes; `cargo build` shows no
  deprecation warnings on internal code; downstream user code compiling with
  old names gets deprecation warning (verify with a test snippet).
- **Effort**: ~0.5 day.
- **Blocker**: None (independent of T0.1).
- **Shippable as**: `v0.2.2` patch (deprecation aliases preserve compat).

### T0.3 — Add `rr-admin node-reset` command + runbook

- **What**: New subcommand `rr-admin node-reset` that orchestrates single-node
  disaster recovery: issue `RemoveMember` → wipe data dir (with confirmation)
  → re-add as `Learner` → optionally wait for catch-up → optionally promote
  to `Voter`. Add runbook explaining when and how to use.
- **Why**: A-0002 §D3 + Phase 3 `snapshot-corruption.md` runbook both
  acknowledged manual recovery is error-prone. A single command materially
  reduces operator mistake risk.
- **Files**:
  - `crates/rr-admin/src/main.rs` — new subcommand definition
  - `crates/rr-admin/src/node_reset.rs` — new module (parallel to
    `backup.rs`)
  - `crates/rr-network/proto/admin.proto` — consider `ResetNode` RPC or
    decide it's unnecessary (existing `ChangeMembership` may suffice)
  - `docs/docs/operations/runbooks/node-reset.md` — new runbook
- **Verification**: Integration test in `tests/integration/`: spawn 3-node
  cluster, write some data, kill node-2, corrupt its data dir, run
  `rr-admin node-reset --target-node-id 2 --data-dir /tmp/rr-2
  --wait-catch-up --promote-after-catch-up`, verify cluster has 3 voters,
  data intact.
- **Effort**: ~1.5 days.
- **Blocker**: None.
- **Shippable as**: `v0.2.3` patch OR folded into `v0.3.0` — prefer patch
  ship so operators benefit sooner.

---

## Tier 1 — Framework State Machine Refactor (Foundation)

Introduces `FramedSnapshot` format. **Blocks** Tiers 2 & 3. ~3 days.

### T1.1 — Introduce `FramedSnapshot` format

- **What**: New struct in `rr-core` with `version: u8` + `framework_state` +
  `user_snapshot: Vec<u8>`. Serialize with bincode v2. No behavior change
  yet — this task just adds the type.
- **Why**: A-0001 needs `last_applied_offset`; A-0003 needs idempotency
  watermarks + cache — both are framework state that must ride snapshots.
  One shared format avoids multiple future migrations.
- **Files**:
  - `crates/rr-core/src/framed_snapshot.rs` — new module
  - `crates/rr-core/src/lib.rs` — export
- **Verification**: Unit tests for serialize/deserialize roundtrip with
  empty + populated `framework_state`; forward-compat test (decode v1 with
  extra trailing bytes succeeds gracefully).
- **Effort**: ~0.5 day.
- **Blocker**: None.

### T1.2 — Migrate `ServiceStateMachineAdapter` to framed format

- **What**: Extend `ServiceStateMachineAdapter::snapshot()` to wrap user's
  `Service::snapshot()` output in `FramedSnapshot { version: 1,
  framework_state: FrameworkState::default(), user_snapshot: <bytes> }`.
  Extend `restore()` with a **detection-and-migration** path:
  - Attempt to decode as `FramedSnapshot` first
  - On decode failure (likely old 0.2.x raw-bytes format), treat the entire
    blob as `user_snapshot` with `framework_state = default`
  - Log `info!` on migration path taken
- **Why**: Transparent upgrade path for existing 0.2.x clusters. On first
  snapshot write after upgrade, new format takes over.
- **Files**:
  - `crates/rr-raft/src/service_adapter.rs` — wrap/unwrap logic
  - `crates/rr-raft/src/sm_adapter.rs` — hand bytes to adapter unchanged
- **Verification**: 
  - Unit: old-format raw bytes decode with default `framework_state`
  - Unit: new-format roundtrip works
  - Integration: 0.2.x cluster produces a snapshot, swap binary to 0.3.0,
    node restarts, restore succeeds, next snapshot is new format
- **Effort**: ~1 day.
- **Blocker**: T1.1.

### T1.3 — `FrameworkState` schema definition

- **What**: Define `FrameworkState` struct with two optional fields:
  `idempotency: Option<IdempotencyState>` and `ingestor: Option<IngestorState>`.
  Both `None` initially (no behavior change). T2 and T3 populate these.
- **Why**: Locks the schema before parallel T2/T3 work avoids merge
  conflicts on the same struct.
- **Files**:
  - `crates/rr-core/src/framed_snapshot.rs` — extend with nested types
- **Verification**: Schema compiles; roundtrip tests for all
  `(None, None) / (Some, None) / (None, Some) / (Some, Some)` combinations.
- **Effort**: ~0.5 day.
- **Blocker**: T1.2.

---

## Tier 2 — A-0003 Idempotency Layer (~4 days, parallel with T3)

### T2.1 — Protocol changes

- **What**: Add `optional IdempotencyKey idem_key` to `ClientRequest` and
  `optional IdempotencyError idem_error` to `ClientResponse`. Regenerate
  generated code via `build.rs`.
- **Files**:
  - `crates/rr-network/proto/client.proto`
- **Verification**: `cargo build -p rr-network` regenerates proto; old
  clients (without `idem_key`) still compile against new server (proto3
  `optional` is backward compatible).
- **Effort**: ~0.5 day.
- **Blocker**: T1.3 (shares schema thinking with `FrameworkState`).

### T2.2 — `IdempotencyLayer` core implementation

- **What**: New module with `watermarks: HashMap<String, u64>` + LRU cache.
  `decide(&IdempotencyKey) → IdempotencyDecision` (New / Replay / Expired /
  Collision). `record(&IdempotencyKey, response)` updates both.
  `snapshot()`/`restore()` serialize via `FramedSnapshot.framework_state.idempotency`.
- **Files**:
  - `crates/rr-raft/src/idempotency.rs` — new module
  - `crates/rr-raft/src/lib.rs` — export
  - `Cargo.toml` — add `lru = "0.12"` dep
- **Verification**:
  - Unit: New key → New decision; same key within cache → Replay; key below
    watermark AND evicted → Expired; same key with different client_id →
    Collision
  - Unit: snapshot/restore roundtrip preserves watermarks and cache
- **Effort**: ~1.5 days.
- **Blocker**: T1.3.

### T2.3 — Integrate `IdempotencyKey` through the full write path

- **What**: `IdempotencyKey` must flow from gRPC request → dispatch →
  Raft entry payload → `apply()` → `IdempotencyLayer::decide()`. Three
  integration points:

  **a) gRPC request extraction** — in `ClientServiceImpl::request()`,
  decode `ClientRequest.idem_key` from proto into an internal
  `Option<IdempotencyKey>`; pass to dispatch.

  **b) Dispatch wrapping** — `ClientDispatch::submit_write()` signature
  changes to accept `Option<IdempotencyKey>`. `ClientDispatchImpl` wraps
  the payload into the `FramedEntry` format from T3.3:
  `bincode({ offset: None, idem_key: Some(key_or_none), payload })`.
  The wrapped bytes go to `raft.client_write()`.

  **c) State machine apply** — in `ServiceStateMachineAdapter::apply()`,
  decode `FramedEntry`; if `idem_key.is_some()`, call
  `idempotency_layer.decide()`:
  - `New` → proceed to `service.handle()`, then
    `idempotency_layer.record()`
  - `Replay(bytes)` → return bytes; do NOT call `service.handle()`
  - `Expired` → return error wrapped as `ClientResponse.idem_error` +
    `success = false`
  - `Collision { cached, incoming }` → `warn!` log, proceed as `Replay`
- **Files**:
  - `crates/rr-network/src/client_service.rs` — proto → internal decode
  - `crates/rr-server/src/dispatch.rs` — `ClientDispatchImpl::submit_write`
    signature + wrapping
  - `crates/rr-core/src/service.rs` (or new `rr-core/src/idempotency.rs`)
    — define public `IdempotencyKey` type used across crates
  - `crates/rr-raft/src/service_adapter.rs` — `apply()` decoding +
    layer call
  - `crates/rr-server/src/config.rs` — `NodeConfig::idempotency_cache_size:
    usize` (default 10000). **Confirmed location**: `NodeConfig` lives in
    `rr-server`, not `rr-core` (per `CONTEXT.md` §Key file index).
- **Verification**: Integration test in
  `tests/integration/src/idempotency.rs`: 3-node cluster, send request
  with `(type="transfer", id=42)`, verify applied + response cached; send
  again with same key, verify `Service::handle()` NOT called (use a
  counter-based mock `Service`), cached response returned. Send with
  `id=41` after watermark=42 AND cache evict simulation, verify
  `IdempotencyExpired` error surfaces at client.
- **Effort**: ~1.5 days (was 1; dispatch path wrapping adds scope).
- **Blocker**: T2.2, T3.3 (shares `FramedEntry` format).

### T2.4 — Client SDK support

- **What**: New method `RrClient::send_request_idempotent(group_id, payload,
  key) → Result<Vec<u8>, RrClientError>`. New error variant
  `IdempotencyExpired { request_type, request_id, watermark }`. New metrics:
  `rr_idempotency_replays_total`, `rr_idempotency_collisions_total`,
  `rr_idempotency_expired_total`, `rr_idempotency_cache_size` (gauge).
- **Files**:
  - `crates/rr-client/src/lib.rs` — new method + error variant
  - `crates/rr-client/src/metrics.rs` — if exists; else add to existing
    metrics module
  - `crates/rr-server/src/health.rs` — no change (metrics auto-export)
- **Verification**: Unit test on `RrClient`; integration test: client
  disconnects mid-request, reconnects, retries with same `IdempotencyKey`,
  receives cached response.
- **Effort**: ~1 day.
- **Blocker**: T2.3.

### T2.5 — Integration test suite

- **What**: Dedicated test file `tests/integration/src/idempotency.rs` with:
  - Happy path (no idem_key) unchanged behavior
  - Opt-in: same (type, id) returns cached
  - Cross-client collision warn-log
  - Expired id returns `IdempotencyExpired`
  - Snapshot survives: trigger snapshot, restart node, verify watermarks +
    cache restored
- **Verification**: `cargo test --workspace idempotency` passes.
- **Effort**: ~1 day.
- **Blocker**: T2.4.

---

## Tier 3 — A-0001 Kafka Ingestor (~7 days, parallel with T2)

### T3.1 — `CommandIngestor` trait + type-erased adapter in `rr-core`

- **What**: Define TWO types in the same module to resolve trait object-safety:
  - **Public trait `CommandIngestor`** with `type Offset: Serialize +
    DeserializeOwned + Ord + Copy + Debug + Send + Sync + 'static` as spec'd
    in A-0001 §Decision. Associated type gives implementers good ergonomics
    (typed Kafka `i64`, future Pulsar `String`, etc). Not object-safe on
    its own because `Self::Offset` appears in method positions.
  - **Framework-internal `DynCommandIngestor` wrapper** — type-erased via
    `Vec<u8>` offset (bincode-encoded `Self::Offset`). This IS object-safe
    and is what `RaftGroupManager` stores as `Box<dyn DynCommandIngestor>`.
    Provide blanket impl `impl<I: CommandIngestor + 'static>
    DynCommandIngestor for IngestorAdapter<I>` that encodes/decodes offsets
    at the trait boundary.
  - Include all 4 Safety Invariants in `CommandIngestor` rustdoc; document
    the erasure pattern + cost (one bincode encode/decode per offset
    operation) in `DynCommandIngestor` rustdoc.
  - NO concrete implementations yet.
- **Why the split**: Trait with associated type is not object-safe. Pure
  generics (`RaftGroupManager<I>`) force the entire type hierarchy to be
  generic over ingestor type, which breaks multi-group heterogeneity (different
  groups may use different ingestors). Type erasure via a wrapper is the
  idiomatic Rust solution.
- **Files**:
  - `crates/rr-core/src/ingestor.rs` — new module (both types)
  - `crates/rr-core/src/lib.rs` — export `CommandIngestor`, `IngressCommand`;
    `DynCommandIngestor` stays `pub(crate)`-ish (public but marked
    "framework-internal" in rustdoc)
- **Verification**:
  - `cargo doc --all` renders both traits + the erasure pattern
    explanation
  - Unit test: mock `CommandIngestor<Offset = i64>` wraps into
    `DynCommandIngestor`; roundtrip through `Vec<u8>` offset preserves
    value
  - `cargo test -p rr-core` passes
- **Effort**: ~1 day (was 0.5; type erasure adds complexity).
- **Blocker**: None (independent of T1).

### T3.2 — Framework lifecycle integration in `RaftGroupManager`

- **What**: `RaftGroupManager` accepts optional
  `Arc<dyn Fn() -> Box<dyn DynCommandIngestor>>` factory per group (note:
  `DynCommandIngestor` is the erased trait from T3.1, which IS object-safe).
  On leadership transition:
  - Becoming leader: read `last_applied_offset_bytes: Option<Vec<u8>>`
    from framework state (T3.3); if `Some(bytes)`, call
    `ingestor.seek(bytes)` before first poll; if `None` (fresh group),
    call `ingestor.seek_to_beginning()` (add to `DynCommandIngestor` as a
    convenience method returning the encoded "start" offset, or document
    that first `poll()` handles this implicitly)
  - Spawn poll loop task (tracked via `JoinHandle` per project invariant),
    push decoded commands into a bounded `tokio::sync::mpsc` channel
    (default capacity 1000, configurable via `KafkaIngestorConfig`), submit
    each to `raft.client_write()` wrapped with the offset as framed payload
  - Becoming follower / stepping down: `JoinHandle.abort()`, drop ingestor
    instance; verify abort completes before allowing next promotion
- **Files**:
  - `crates/rr-raft/src/group_manager.rs` — lifecycle hooks
  - `crates/rr-raft/src/ingestor_loop.rs` — new module for poll loop
  - `crates/rr-raft/src/ingestor_lifecycle.rs` — new module for
    promotion/demotion orchestration
- **Verification**: Integration test with a mock
  `CommandIngestor<Offset = u64>` (backed by in-memory `VecDeque<(u64,
  Vec<u8>)>`): trigger leadership transition, verify `seek()` called with
  bincode-decoded offset equal to `last_applied_offset + 1`, verify
  commands flow into Raft log. Second test: force step-down, verify
  `JoinHandle` completes within timeout, verify no ingestor instance
  leaked.
- **Effort**: ~2 days.
- **Blocker**: T1.3 (framework state machine wrapper is where offsets are
  persisted), T3.1 (`DynCommandIngestor` defined).

### T3.3 — Framework state machine wrapper: offset tracking

- **What**: Offsets stored as `Vec<u8>` throughout the framework state
  path (the type-erased form from T3.1's `DynCommandIngestor` boundary).
  Implementer's typed `Offset` is bincode-encoded at the ingestor adapter
  and decoded when the ingestor reads its own state. Framework never
  inspects offset bytes — they're opaque. Extend `ServiceStateMachineAdapter` to:
  - On each `apply()`: extract `Option<Vec<u8>>` offset from the framed
    entry payload, update `self.last_applied_offset: Option<Vec<u8>>`
  - On `snapshot()`: include `last_applied_offset` in
    `FrameworkState.ingestor.last_applied_offset`
  - On `restore()`: restore `last_applied_offset` from framework state
  - Expose `last_applied_offset() -> Option<&[u8]>` for T3.2 to read on
    leader promotion (passed to `DynCommandIngestor::seek()`)
- **Entry payload framing** (new): Each Raft log entry is
  `bincode({ offset: Option<Vec<u8>>, idem_key: Option<IdempotencyKey>,
  payload: Vec<u8> })`. Document this framing in
  `crates/rr-core/src/framed_entry.rs`. Entries produced by gRPC path
  have `offset = None`; entries from ingestor path have `offset =
  Some(bincode(impl_offset))`.
- **Files**:
  - `crates/rr-raft/src/service_adapter.rs` — apply/snapshot/restore
  - `crates/rr-core/src/framed_snapshot.rs` — `IngestorState`
    `{ last_applied_offset: Option<Vec<u8>> }`
  - `crates/rr-core/src/framed_entry.rs` — new module for entry payload
    framing
  - `crates/rr-core/src/lib.rs` — export `FramedEntry`
- **Verification**:
  - Unit: apply/snapshot/restore roundtrip preserving offset bytes
  - Unit: mixed entries (some with offset, some without) apply correctly;
    `last_applied_offset` updates only when `Some`
  - Integration: send commands via mock ingestor with `Offset = u64`,
    trigger snapshot, decode snapshot bytes, verify
    `IngestorState.last_applied_offset` equals
    `bincode::encode(expected_u64)`
- **Effort**: ~1 day.
- **Blocker**: T1.3, T3.1.

### T3.4 — `rr-ingestor-kafka` crate

- **What**: New crate `crates/rr-ingestor-kafka` with:
  - `KafkaIngestor` struct implementing `CommandIngestor<Offset = i64>`
  - `KafkaIngestorConfig` with brokers, topic_pattern (default
    `input_{group_id}`), TLS/SASL config, poll timeout, batch size
  - Uses `rdkafka::consumer::StreamConsumer::assign()` (single-partition,
    bypasses consumer groups)
  - `seek()` via `seek_partitions()`; `commit_applied()` via async commit
- **Files**:
  - `crates/rr-ingestor-kafka/Cargo.toml` — `rdkafka` dep, feature
    `cmake-build` to compile librdkafka from source (avoids system
    librdkafka version skew)
  - `crates/rr-ingestor-kafka/src/lib.rs`
  - `crates/rr-ingestor-kafka/src/config.rs`
  - Workspace `Cargo.toml` — add to `members`
- **Verification**: Unit tests with `rdkafka`'s in-memory test utilities
  where possible; integration test deferred to T3.6.
- **Effort**: ~2 days.
- **Blocker**: T3.1.

### T3.5 — Backpressure + error handling

- **What**: Poll loop in T3.2 handles:
  - Bounded channel full → backpressure (poll blocks, ingestor pauses
    naturally)
  - `poll()` error → exponential backoff (100ms → 30s cap), keep retrying;
    do NOT step down leader
  - `seek()` error on leader promotion → retry 3x; on repeated failure, log
    error + stay leader but don't consume (human intervention required)
  - `commit_applied()` error → log `warn!` but continue (offsets in Raft
    log are source of truth)
- **Files**:
  - `crates/rr-raft/src/ingestor_loop.rs` — error handling
- **Verification**: Unit test with mock ingestor returning errors; verify
  retry behavior; verify leadership not lost.
- **Effort**: ~0.5 day.
- **Blocker**: T3.4.

### T3.6 — Integration test with real Kafka

- **What**: End-to-end test using `testcontainers-rs` or `testcontainers`
  crate to spawn Kafka + Zookeeper container. Test scenarios:
  - 3-node cluster, group 0 ingests from topic `input_0`
  - Produce 100 messages → verify all applied
  - Kill leader → new leader seeks to correct offset → resumes
  - Kafka broker restart → ingestor reconnects
  - Snapshot + node restart → offset preserved across restart
- **Files**:
  - `tests/integration/src/kafka_ingestor.rs` — new file
  - `tests/integration/Cargo.toml` — add testcontainers dev-dep
  - `.github/workflows/ci.yml` — possibly skip on macOS (Docker-in-Docker
    unreliable)
- **Verification**: `cargo test --workspace kafka_ingestor` passes on
  Linux. Document macOS caveat in test file comment.
- **Effort**: ~1 day.
- **Blocker**: T3.5.

---

## Tier 4 — Synergies + Documentation (~3 days)

### T4.1 — A-0001 + A-0003: `auto_idempotency` mode

- **What**: When Kafka ingestor is active, optionally auto-generate
  `IdempotencyKey` from `(topic_name, offset)` before submitting to Raft.
  Config flag `KafkaIngestorConfig::auto_idempotency: bool` (default true).
  Delivers end-to-end exactly-once semantics (Kafka at-least-once +
  framework dedup).
- **Files**:
  - `crates/rr-ingestor-kafka/src/lib.rs` — generate key in poll loop
  - `crates/rr-raft/src/ingestor_loop.rs` — pass key through to
    `client_write` wrapper
- **Verification**: Integration test: inject Kafka message duplicate at
  same offset (simulating at-least-once), verify `Service::handle()` called
  once.
- **Effort**: ~0.5 day.
- **Blocker**: T2.5, T3.6.

### T4.2 — Documentation overhaul

- **What**: Add new guide pages + update references:
  - `docs/docs/guide/kafka-input.md` — `CommandIngestor` concept + Kafka
    setup + operational notes (leader-exclusive, offset checkpointing,
    leadership transition latency)
  - `docs/docs/guide/idempotency.md` — `IdempotencyKey` concept + usage
    patterns (Snowflake, DB seq, Kafka offset) + when to skip (business-
    layer composite keys)
  - `docs/docs/reference/config-reference.md` — new config options:
    `idempotency_cache_size`, `KafkaIngestorConfig.*`, `DurabilityPolicy`
    rename note
  - `docs/docs/reference/metrics-reference.md` — new metrics:
    `rr_idempotency_*`, `rr_ingestor_*`
  - `docs/docs/architecture/command-flow.md` — new page showing the
    end-to-end flow: Kafka → ingestor → Raft → idempotency → Service
  - `docs/sidebars.ts` — add new pages
- **Verification**: `npm run build` in `docs/` produces zero warnings.
- **Effort**: ~1.5 days.
- **Blocker**: T4.1.

### T4.3 — `examples/kv-store-kafka`

- **What**: New example showing the full stack:
  - KV service with idempotent `Put` operation
  - Kafka ingestor config
  - Docker Compose with Kafka + 3 rr nodes + a producer script
  - README walking through the setup
- **Files**:
  - `examples/kv-store-kafka/` — new directory
  - `examples/kv-store-kafka/Cargo.toml`
  - `examples/kv-store-kafka/src/main.rs`
  - `examples/kv-store-kafka/docker-compose.yml`
  - `examples/kv-store-kafka/README.md`
  - Workspace `Cargo.toml` — add to workspace members
- **Verification**: `cd examples/kv-store-kafka && docker compose up` runs
  without errors; producer script delivers messages; cluster processes them;
  killing Kafka doesn't crash cluster.
- **Effort**: ~1 day.
- **Blocker**: T4.2.

### T4.4 — CHANGELOG + migration guide

- **What**: Update `CHANGELOG.md` with v0.3.0 section listing all features
  + breaking changes. Add `docs/docs/deployment/migration-0.2-to-0.3.md`
  with step-by-step upgrade instructions (especially the snapshot format
  auto-migration).
- **Files**:
  - `CHANGELOG.md`
  - `docs/docs/deployment/migration-0.2-to-0.3.md` — new page
  - `docs/sidebars.ts` — include new page
- **Verification** (repeatable, no human-only steps):
  1. **Docs build clean**: `cd docs && npm run build` exits 0; zero
     broken-link warnings; zero broken-anchor warnings; zero MDX errors.
  2. **Link check**: every internal link in the migration guide resolves
     (use the Docusaurus build output's broken-link scan).
  3. **Breaking-change checklist**: explicit table at top of migration
     guide listing each breaking change with: (a) what changed, (b) impact
     on users, (c) automatic vs manual migration. Verify by grep — every
     task T0–T4 marked as breaking in its §Consequences must appear in the
     table. Specifically: `DurabilityPolicy` rename (T0.2), snapshot
     format wrapping (T1.2), proto field additions (T2.1), new crate dep
     (T3.4), `NodeConfig` new field (T2.3), new CLI subcommand (T0.3).
  4. **Migration simulation**: run the T1.2 migration integration test
     again from this task as a checkpoint — confirms migration path is
     tested as documented.
  5. **Version references**: grep `CHANGELOG.md` for `v0.3.0`; grep
     `docs/` for stale `v0.2` references that should be updated.
  6. **Self-review against CHANGELOG conventions**: all entries follow
     Keep-a-Changelog format (Added / Changed / Deprecated / Removed /
     Fixed / Security sub-headings).
- **Effort**: ~0.5 day.
- **Blocker**: T4.3.

---

## Tier 5 — Release (~2 days)

### T5.1 — `v0.3.0-rc1` tag

- **What**: Tag `v0.3.0-rc1`, push, verify release workflow builds + pushes
  Docker image + Helm chart. Manual smoke test: `docker compose up` with
  the new image, basic write/read works.
- **Effort**: ~0.5 day.
- **Blocker**: T4.4.

### T5.2 — F1-F4 independent reviews

- **F1**: Plan compliance audit — `task(subagent_type="oracle", ...)` with
  this plan + the 3 ADRs. Verify every task's acceptance criteria met.
- **F2**: Protocol review — oracle agent examines new proto fields,
  `FramedSnapshot` format, snapshot migration logic. Look for silent
  compatibility breaks.
- **F3**: Kafka integration smoke test — human-driven: real Kafka broker
  (not testcontainers), verify producer → cluster → consumer round-trip.
- **F4**: Upgrade path verification — take a v0.2.x cluster with real
  data, upgrade binary, verify snapshot auto-migration + cluster still
  functional.
- **Effort**: ~1 day (parallel).
- **Blocker**: T5.1.

### T5.3 — `v0.3.0` final

- **What**: Tag `v0.3.0`, push. Publish to crates.io. Release notes
  announcement.
- **Effort**: ~0.5 day.
- **Blocker**: T5.2 all green.

---

## Acceptance Criteria (Definition of Done for v0.3.0)

- [ ] All ADR consequences (positive, negative, neutral) addressed or
      explicitly documented as deferred
- [ ] `cargo test --workspace --all-features` passes
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] Coverage >= 75% (baseline held; Kafka integration may lower slightly,
      compensate by unit tests on `rr-ingestor-kafka`)
- [ ] Docusaurus builds with zero broken links / zero MDX errors
- [ ] Old v0.2.x snapshot auto-migrates on restart (integration test proves)
- [ ] `examples/kv-store-kafka` runs end-to-end via `docker compose`
- [ ] `CHANGELOG.md` lists all breaking changes
- [ ] F1-F4 reviews closed without blocking issues
- [ ] `v0.3.0` tag pushed, Docker + Helm + crates.io published

## Project Invariants Respected

Throughout implementation, preserve these (per `CONTEXT.md`):

- `rr-core` stays DAG root — `CommandIngestor` trait goes in rr-core;
  `rr-ingestor-kafka` depends on rr-core but not vice versa ✅
- Library crates use `thiserror` not `anyhow` — `rr-ingestor-kafka` must use
  `thiserror` for its error type ✅
- All `tokio::spawn` tracked — T3.2 poll loop has explicit `JoinHandle` that
  is cancelled on step-down ✅
- RocksDB via `spawn_blocking` — snapshot format change doesn't touch
  storage write path ✅
- Hot path no `.unwrap()` — idempotency layer + ingestor all use `?` ✅
- Follow semver — v0.3.0 is minor bump; snapshot format is breaking but
  auto-migrated ✅

## Risks

| Risk | Mitigation |
|---|---|
| `testcontainers` flakes on CI | Mark Kafka tests `#[ignore]` on macOS; require Linux-only in CI matrix |
| `rdkafka` MSRV diverges from rr MSRV | Pin `rdkafka = "0.36"`; verify MSRV match before T3.4 starts |
| Snapshot format migration bug → old clusters can't upgrade | T1.2 has dedicated migration test; F4 review explicitly covers this |
| Proto change breaks 0.2.x clients | Verify proto3 `optional` is forward-compat by running 0.2.x client against 0.3.0 server in T5.2 F2 |
| Leader-transition ingestor handover races | T3.2 uses explicit lifecycle hooks (spawn on promote, cancel on step-down); no concurrent ingestor instances possible |
| librdkafka C build breaks users' environments | `rr-ingestor-kafka` feature `cmake-build` pins source build; document fallback `system-librdkafka` feature |
| Idempotency cache memory blows up | Bounded LRU (default 10K); document tuning; expose `rr_idempotency_cache_size` gauge |

## Open Questions (Resolve Before Starting)

1. **Snapshot format migration scope**: Do we support restoring 0.3.0 on a
   0.2.x-only node? **Answer**: No. Forward-migration only.
2. **DurabilityPolicy deprecation window**: One minor version (`Balanced`
   alias kept in 0.3.0, removed in 0.4.0)? **Answer**: Yes, document in
   `CHANGELOG.md`.
3. **`rr-ingestor-kafka` version independence**: Can it version independently
   of rr-core? **Answer**: Keep version-locked in 0.x (both 0.3.0); allow
   independent versioning from 1.0 onward.
4. **Testcontainers vs mock Kafka**: Should T3.6 also include a pure-Rust
   mock Kafka broker for faster iteration? **Answer**: Defer — start with
   testcontainers; if flake rate unacceptable, build a mock as follow-up.

## Post-v0.3.0 Candidate Work

Moves to Tier 2 roadmap (not part of this plan):

- `rr-ingestor-pulsar` crate (community or dedicated phase)
- `rr-ingestor-rocketmq` crate
- Async response mode (A-0003's deferred Option from ADR §"Background") —
  decouples response delivery from gRPC connection
- Idempotency cache TTL (operational data-driven)
- Per-request-type metrics labels for idempotency
- Kafka exactly-once producer integration (client-side commits via
  transactional producer)

---

## Changelog

| Date | Change | Trigger |
|---|---|---|
| 2026-04-24 | Initial draft | I/O design session with Louis; user requested execution plan for combined A-0001/A-0002/A-0003 shipment |
