# rr Comprehensive Improvements

## Context

Comprehensive improvements to the `rr` distributed multi-Raft consensus framework across 5 areas:
- **Area A**: Testing & CI Infrastructure
- **Area B**: Observability — Metrics & Admin gRPC API
- **Area C**: Developer Experience — CLI, REPL, Examples, Rustdoc
- **Area D**: Performance — Critical Fixes
- **Area E**: Stability — Fault Injection & Chaos Testing

All documentation updates are bilingual (中英文混排): Chinese paragraph first → `---` separator → English paragraph. Section headers use `## 标题 / Title` format.

## Must Have

- [ ] All new code follows TDD (tests written first, then implementation)
- [ ] `cargo test --workspace` passes (default features)
- [ ] `cargo test --workspace --features rr-storage/rocks` passes
- [ ] `cargo clippy --workspace -- -D warnings` passes (zero warnings)
- [ ] `cargo fmt --check` passes
- [ ] `cargo doc --workspace --no-deps` exits 0
- [ ] New Docusaurus pages for new features (bilingual, matching existing style)
- [ ] All new gRPC endpoints have unit tests and integration tests

## Must NOT Have

- No Docker/K8s manifests
- No web dashboard UI
- No i18n plugin
- No new storage backends (sled, lmdb, etc.)
- No streaming snapshot refactor
- No double-serialization refactor (bincode-in-proto stays as-is)
- No changes to existing public API signatures without strong justification
- No `unwrap()` or `expect()` in production paths (only in tests with context)

## Guardrails

- `parking_lot::RwLock` (not `tokio::sync::RwLock`) for the RrLogStorage fix — locks are never held across `.await`
- Admin/metrics gRPC endpoint lives in `rr-server` (not a new crate) to avoid dependency cycles
- CLI binary is a new `rr-admin` crate (separate from rr-server)
- REPL scoped to exactly 6 commands: `get`, `put`, `delete`, `status`, `members`, `help`
- Batch apply dropped from scope — openraft already batches at the framework level
- Rustdoc improvements AND Docusaurus pages updated to reflect new features

---

## Area A: Testing & CI Infrastructure

### A1 — Add cargo-llvm-cov coverage to CI
- [ ] Add `cargo-llvm-cov` to `.github/workflows/ci.yml` as a new `coverage` job
- [ ] Coverage report uploaded to Codecov (requires `CODECOV_TOKEN` secret note in plan)
- [ ] Coverage job runs after `test` job succeeds
- [ ] `.codecov.yml` created with reasonable thresholds (no hard failures on low coverage initially)
- [ ] `README.md` updated with coverage badge

### A2 — Add criterion benchmark suite
- [ ] `Cargo.toml` workspace: add `criterion` as dev-dependency
- [ ] Create `benches/` directory at workspace root with `Cargo.toml` for benchmark crate
- [ ] Benchmark: `bench_log_append` — append 1000 entries to `RrLogStorage`
- [ ] Benchmark: `bench_log_append_rocks` — append 1000 entries to `RocksLogStorage` (feature-gated)
- [ ] Benchmark: `bench_codec_roundtrip` — bincode encode+decode 1000 times
- [ ] Benchmark: `bench_mem_storage_put_scan` — 1000 `put` + `scan` ops on `MemStorage`
- [ ] CI job `bench` added: `cargo bench --workspace -- --test` (validates benches compile and run)

### A3 — Add proptest for property-based testing
- [ ] `proptest` added as workspace dev-dependency
- [ ] Property tests for `BincodeCodec`: encode/decode roundtrip with arbitrary bytes
- [ ] Property tests for `MemStorage`: put/get/delete consistency invariants
- [ ] Property tests for `RocksStorage`: same invariants (feature-gated)
- [ ] Tests in `#[cfg(test)]` modules of respective crates

### A4 — Enhance CI pipeline
- [ ] CI matrix: add `macos-latest` runner alongside `ubuntu-latest`
- [ ] CI matrix: add `nightly` toolchain alongside `stable` (allow failures on nightly)
- [ ] Add `doc-test` job: `cargo test --doc --workspace --no-deps`
- [ ] Add `audit` job: `cargo audit` (security vulnerability scan)
- [ ] Add `msrv` job: first add `rust-version = "1.82"` (or appropriate) to workspace `Cargo.toml`, then verify with `cargo check --workspace` on that version
- [ ] Set `RUSTFLAGS: "--deny=warnings"` for all CI jobs
- [ ] Separate integration test job with 10-minute timeout

---

## Area B: Observability — Metrics & Admin gRPC API

### B1 — Wire metrics recorder (prometheus exporter)
- [ ] Add `metrics-exporter-prometheus` to `rr-server/Cargo.toml`
- [ ] `RrNode::new()` installs a `PrometheusBuilder` recorder at startup (binds to `listen_addr` port+1 or configurable)
- [ ] Metrics HTTP endpoint `/metrics` served on a separate port (default: `node_port + 10000`)
- [ ] `NodeConfig` gains optional `metrics_port: Option<u16>` field (None = disabled)
- [ ] Existing 7 counters now flow to Prometheus (no code change needed — just recorder installation)
- [ ] Unit test: verify recorder is installed and counters increment
- [ ] Integration test: start cluster, verify `/metrics` endpoint returns 200 with `rr_` prefixed metrics

### B2 — Expand metrics coverage
- [ ] Histogram: `rr_proposal_duration_seconds` — time from client proposal to commit (label: `group_id`)
- [ ] Histogram: `rr_replication_batch_entries` — entries per AppendEntries RPC
- [ ] Gauge: `rr_raft_is_leader` — 1 if this node is leader for a group, 0 otherwise (label: `group_id`)
- [ ] Gauge: `rr_raft_current_term` — current Raft term (label: `group_id`)
- [ ] Gauge: `rr_raft_last_log_index` — last log index (label: `group_id`)
- [ ] Counter: `rr_snapshot_installs_total` — snapshots installed (label: `group_id`)
- [ ] All new metrics recorded via the `metrics` crate macros (not prometheus-client directly)
- [ ] Tests for each new metric

### B3 — Admin gRPC service (proto + server)
- [ ] New proto file: `crates/rr-network/proto/admin.proto`
- [ ] Service `AdminService` with RPCs:
  - `GetNodeStatus(Empty) → NodeStatus` — node_id, listen_addr, group_ids, uptime_secs
  - `GetGroupStatus(GroupId) → GroupStatus` — group_id, is_leader, current_term, last_log_index, leader_node_id
  - `TriggerSnapshot(GroupId) → TriggerSnapshotResponse` — trigger manual log compaction
  - `TransferLeader(TransferLeaderRequest) → TransferLeaderResponse` — graceful leadership transfer
  - `GetMetricsSummary(Empty) → MetricsSummary` — top-level metrics snapshot
- [ ] `crates/rr-network/build.rs` updated to include `admin.proto` in `tonic_prost_build::configure().compile_protos()` call (project uses `tonic-prost-build`, NOT `tonic-build`)
- [ ] `AdminServiceImpl` in `crates/rr-server/src/admin.rs` (lives in `rr-server`); `rr-server/Cargo.toml` already depends on `rr-network` which exposes generated proto types
- [ ] `RrNode` starts `AdminService` on `admin_port` (default: `node_port + 1000`)
- [ ] `NodeConfig` gains `admin_port: Option<u16>` field
- [ ] Unit tests for each RPC handler
- [ ] Integration test: start cluster, call `GetNodeStatus` and `GetGroupStatus` via tonic client

### B4 — gRPC health service
- [ ] Add `tonic-health` to `rr-server/Cargo.toml` and `rr-network/Cargo.toml`
- [ ] `RrNode` registers `HealthServer` on the main gRPC listener
- [ ] Health status: `SERVING` when node is running, `NOT_SERVING` during shutdown
- [ ] Integration test: health check returns `SERVING` after cluster start

---

## Area C: Developer Experience

### C1 — New crate: `rr-admin` CLI binary
- [ ] Create `crates/rr-admin/` with `Cargo.toml` depending on `clap`, `tonic`, and `rr-network` (for generated admin proto client types)
- [ ] `clap` subcommands:
  - `rr-admin node status --addr <host:port>` — calls `GetNodeStatus`
  - `rr-admin group status --addr <host:port> --group <id>` — calls `GetGroupStatus`
  - `rr-admin group snapshot --addr <host:port> --group <id>` — calls `TriggerSnapshot`
  - `rr-admin group transfer-leader --addr <host:port> --group <id> --to <node-id>` — calls `TransferLeader`
  - `rr-admin metrics --addr <host:port>` — calls `GetMetricsSummary`
- [ ] Formatted output: human-readable table (use `tabled` or simple manual formatting)
- [ ] `--output json` flag for machine-readable JSON output
- [ ] `rr-admin` binary added to workspace `Cargo.toml`
- [ ] Unit tests for CLI argument parsing
- [ ] Integration test: start cluster, call admin subcommands

### C2 — Interactive REPL (`rr-admin repl`)
- [ ] Add `rustyline` and `bincode` to `rr-admin/Cargo.toml`
- [ ] `rr-admin repl --addr <host:port> [--group <id>]` launches interactive session
- [ ] Exactly 6 commands: `get <key>`, `put <key> <value>`, `delete <key>`, `status`, `members`, `help`
- [ ] **Wire format**: `get`/`put`/`delete` commands encode payloads using the kv-store example's `KvRequest`/`KvResponse` types (re-defined locally in `rr-admin` as identical `serde` structs to avoid depending on the example crate). Encoding uses `bincode::serde::encode_to_vec(&req, bincode::config::standard())`. The resulting bytes are sent as `ClientRequest.payload` via the `ClientService.Request` RPC. Responses are decoded the same way as `KvResponse`. **This means the REPL's get/put/delete commands are specifically designed for services using the kv-store wire format.** The `help` output must state this: "Note: get/put/delete use kv-store wire format (bincode-encoded KvRequest/KvResponse)."
- [ ] `status` calls `GetNodeStatus` admin RPC
- [ ] `members` calls `GetGroupStatus` admin RPC and displays membership
- [ ] `help` prints command list with wire format note
- [ ] History via rustyline defaults (arrow keys, Ctrl+R)
- [ ] Unit tests for command parsing logic
- [ ] Unit test: verify `encode_kv_request("put", "key", "val")` produces correct bincode bytes matching kv-store's `KvRequest::Put`

### C3 — Counter service example
- [ ] Create `examples/counter/` with `Cargo.toml`
- [ ] Implements `Service` trait: `Increment { key, delta }`, `Get { key }`, `Reset { key }`
- [ ] ~80 lines of application code (exclusive of boilerplate)
- [ ] `//!` module-level doc comments explaining the pattern
- [ ] `README.md` in `examples/counter/` with usage (bilingual)
- [ ] Unit tests for `CounterService`
- [ ] Runnable: `cargo run -p counter -- --node-id 1 --port 52001`

### C4 — Rustdoc improvements
- [ ] `rr-storage/src/lib.rs`: Add crate-level `//!` doc with architecture overview and usage example
- [ ] `rr-router/src/lib.rs`: Add crate-level `//!` doc explaining shard routing pattern
- [ ] `rr-config/src/lib.rs`: Add crate-level `//!` doc explaining ConfigProvider and hot-reload pattern
- [ ] `rr-core/src/lib.rs`: Add crate-level `//!` doc explaining the crate's role as the trait boundary layer
- [ ] Each new public type/method in B3/C1 has `///` doc comments
- [ ] `cargo doc --workspace --no-deps` emits zero warnings (`RUSTDOCFLAGS='-D warnings'`)

### C5 — Update Docusaurus for new features
- [ ] Update `docs/docs/guide/getting-started.md`: add section on admin port and metrics port configuration
- [ ] New page: `docs/docs/reference/admin-api.md` — documents all AdminService RPCs (bilingual)
- [ ] New page: `docs/docs/reference/metrics-reference.md` — documents all exposed Prometheus metrics (bilingual)
- [ ] Update `docs/docs/guide/client-sdk.md`: add `rr-admin repl` usage section
- [ ] Update `docs/sidebars.ts`: add new reference pages to sidebar
- [ ] `cd docs && npm run build` exits 0

---

## Area D: Performance — Critical Fixes

### D1 — Replace `std::sync::RwLock` in `RrLogStorage`
- [ ] `crates/rr-raft/Cargo.toml`: add `parking_lot` dependency
- [ ] `crates/rr-raft/src/log_storage.rs`: replace `std::sync::RwLock` → `parking_lot::RwLock`
- [ ] All `read().expect()` and `write().expect()` updated (parking_lot doesn't return `LockResult`)
- [ ] All existing unit tests in `log_storage.rs` still pass
- [ ] No functional behavior change — pure lock implementation swap

### D2 — Connection pool health checking and auto-reconnect
- [ ] **Target file: `crates/rr-raft/src/network_factory.rs`** (NOT `rr-network/src/channel_pool.rs` — `rr-raft` has its own local `ChannelPool` to avoid a cyclic dependency; see the `//! # Cyclic dependency note` at the top of that file)
- [ ] Add `remove_and_reconnect(node_id: NodeId, addr: &str)` method to `rr-raft`'s local `ChannelPool` (defined at line ~129 of `network_factory.rs`): calls `self.channels.remove(&node_id)` then `self.get_or_connect(node_id, addr)` to force a fresh `Channel::connect()`
- [ ] In `GrpcRaftNetwork::append_entries` / `vote` / `install_snapshot`: when `get_or_connect` fails with `NetworkError::ConnectionFailed`, call `self.channel_pool.remove(self.target)` so the stale channel is evicted and the next RPC attempt creates a fresh connection
- [ ] Maximum 1 reconnect attempt per RPC call (no infinite loops — just evict the bad channel and let the caller retry)
- [ ] Metrics counter: `rr_channel_reconnects_total` (label: `node_id`) — increment on each channel eviction
- [ ] Unit tests for reconnect logic: verify that after `remove()`, the next `get_or_connect` creates a new `Channel` (not cached)

---

## Area E: Stability — Fault Injection & Chaos Testing

### E1 — Fault injection layer for integration tests
- [ ] New module: `tests/integration/src/fault_injection.rs`
- [ ] **Design**: Introduce a `ChannelPoolLike` trait in `rr-raft/src/network_factory.rs` so that integration tests can swap in a fault-aware pool. The existing concrete `ChannelPool` already implements the right methods; extract them into a trait. This is an **internal** API change — `ChannelPool` is only used by `GrpcNetworkFactory` and `RaftGroupManager`, both inside `rr-raft`.
- [ ] **Changes to `rr-raft/src/network_factory.rs`**:
  - Add trait `ChannelPoolLike: Send + Sync + 'static` with methods: `async fn get_or_connect(&self, node_id: NodeId, addr: &str) -> Result<Channel, NetworkError>`, `fn remove(&self, node_id: NodeId)`, `fn len(&self) -> usize`, `fn is_empty(&self) -> bool`
  - Implement `ChannelPoolLike` for existing `ChannelPool`
  - Change `GrpcNetworkFactory` to store `Arc<dyn ChannelPoolLike>` instead of `ChannelPool`
  - Change `GrpcRaftNetwork` to store `Arc<dyn ChannelPoolLike>` instead of `ChannelPool`
- [ ] **Changes to `rr-raft/src/group_manager.rs`**:
  - `RaftGroupManager.channel_pool` field changes from `Option<ChannelPool>` to `Option<Arc<dyn ChannelPoolLike>>`
  - `new_with_network()` signature changes to `new_with_network(node_id, sm_factory, channel_pool: Arc<dyn ChannelPoolLike>)`
  - All call sites updated (integration tests pass `Arc::new(ChannelPool::new())` or `Arc::new(FaultAwareChannelPool::new())`)
- [ ] **`FaultAwareChannelPool`** in `tests/integration/src/fault_injection.rs`:
  - Wraps a real `ChannelPool` plus fault state: `blocked: Arc<DashSet<NodeId>>`, `latency: Arc<DashMap<NodeId, Duration>>`, `loss: Arc<DashMap<NodeId, f64>>`
  - Implements `ChannelPoolLike` — `get_or_connect` checks blocklist (returns `NetworkError::ConnectionFailed`), applies latency (`tokio::time::sleep`), applies loss (random reject), then delegates to inner `ChannelPool`
- [ ] **`FaultInjector`** control handle (separate from the pool, holds `Arc` refs to the same fault state):
  - `isolate(node_id)` — adds to blocklist, calls inner `pool.remove(node_id)` to evict cached channel
  - `restore(node_id)` — removes from blocklist
  - `set_packet_loss(node_id, probability: f64)` — probabilistic RPC failure (0.0–1.0)
  - `set_latency(node_id, delay_ms: u64)` — artificial delay on `get_or_connect`
  - `clear_all()` — removes all faults, does NOT evict channels
- [ ] `ClusterBuilder` in `tests/integration/src/lib.rs` gains `with_fault_injector()` method that creates a `FaultAwareChannelPool`, passes it to `RaftGroupManager::new_with_network()`, and returns a `FaultInjector` handle
- [ ] All fault injection is async-compatible (uses `tokio::time::sleep` for latency)
- [ ] Add `dashmap` as dev-dependency to integration test crate (for `DashSet`/`DashMap` in `FaultAwareChannelPool`)

### E2 — Chaos test scenarios
- [ ] `tests/integration/src/lib.rs`: test `test_leader_isolation_and_recovery`
  - Isolate leader → wait for new leader election → restore → verify consistency
- [ ] Test `test_follower_isolation_and_catchup`
  - Isolate follower → write 100 entries → restore → verify follower catches up via snapshot or log
- [ ] Test `test_packet_loss_convergence`
  - Set 50% packet loss on one node → write 50 entries → clear fault → verify all nodes converge
- [ ] Test `test_latency_under_high_load`
  - Add 100ms latency on leader connections → write 100 entries → measure no correctness violations
- [ ] All chaos tests tagged `#[ignore]` by default (run via `cargo test -- --ignored`)
- [ ] Separate CI job `chaos` runs `cargo test --workspace -- --ignored` with 15-minute timeout

### E3 — Defensive store mode
- [ ] `crates/rr-raft/src/log_storage.rs`: add `defensive` feature flag or `RrLogStorageDefensive` wrapper
- [ ] Defensive checks:
  - `append`: assert entries are contiguous with last stored index
  - `truncate`: assert truncation index is not below commit index
  - `get_log_entries`: assert `start <= end`
- [ ] Defensive mode enabled via `#[cfg(feature = "defensive")]` or env var `RR_STORE_DEFENSIVE=on`
- [ ] CI job `test-defensive` added: `RR_STORE_DEFENSIVE=on cargo test --workspace`
- [ ] Defensive mode only fires assertions — never silently corrupts

---

## Review Tasks

### R1 — Integration verification
- [ ] `cargo test --workspace` passes (all 188+ existing tests still pass)
- [ ] `cargo test --workspace --features rr-storage/rocks` passes
- [ ] `cargo clippy --workspace -- -D warnings` reports zero warnings
- [ ] `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS='-D warnings'` exits 0
- [ ] `cd docs && npm run build` exits 0

### R2 — New feature smoke test
- [ ] `cargo run -p counter -- --node-id 1 --port 52001` starts without error
- [ ] `rr-admin node status --addr 127.0.0.1:52001` returns status (after kv-store example started)
- [ ] `/metrics` endpoint returns Prometheus-format text with `rr_` prefixed metrics
- [ ] Chaos tests pass: `cargo test --workspace -- --ignored`

### R3 — Documentation review
- [ ] All new Docusaurus pages are bilingual (Chinese paragraph → `---` → English paragraph)
- [ ] All new public APIs have `///` rustdoc comments
- [ ] Benchmarks page in Docusaurus updated to reference the new criterion bench crate

---

## Parallelization Notes

The 5 areas (A, B, C, D, E) are largely independent and can execute in parallel waves:

**Wave 1 (independent, fully parallelizable)**:
- A1 (coverage CI) — only touches `.github/workflows/ci.yml`
- A2 (criterion benchmarks) — new `benches/` crate
- A3 (proptest) — adds tests to existing crates
- D1 (RwLock fix) — touches only `rr-raft/src/log_storage.rs`
- C3 (counter example) — new `examples/counter/` crate
- C4 (rustdoc) — touches lib.rs files only

**Wave 2 (requires Wave 1 for D1/B3)**:
- B1 (metrics recorder) — requires `rr-server` to be stable
- B2 (expanded metrics) — requires B1
- B3 (admin gRPC service) — requires proto tooling working
- A4 (enhanced CI) — requires A1/A2/A3 done to add those jobs
- E1 (fault injection layer) — modifies `rr-raft` internals (adds `ChannelPoolLike` trait); no dependency on Wave 1 tasks but touches `rr-raft` files (different from D1's `log_storage.rs`)

**Wave 3 (requires Wave 2)**:
- B4 (health service) — requires B3 admin service integrated
- C1 (rr-admin CLI) — requires B3 admin proto client
- C2 (REPL) — requires C1 binary shell
- D2 (connection health) — requires B1 metrics for reconnect counter
- E2 (chaos tests) — requires E1 fault injection layer
- E3 (defensive store) — can run independently

**Wave 4 (requires Wave 3)**:
- C5 (Docusaurus updates) — requires B3/B1/C1 to document
- R1, R2, R3 (review tasks)
