## [2026-03-05] Session Init: Atlas Bootstrap

### Workspace Layout
- Workspace members: rr-core, rr-storage, rr-config, rr-raft, rr-network, rr-router, rr-server, rr-client, examples/kv-store, tests/integration
- Workspace Cargo.toml: uses `resolver = "2"`, no `rust-version` set yet
- No `benches/` crate yet, no `examples/counter` yet

### Key Dependency Versions
- openraft = "0.9.21" (features: storage-v2, serde)
- tonic = "0.14.5"
- prost = "0.14.3"
- bincode = "2.0.0" with features = ["serde"]
- metrics = "0.24"
- dashmap = "6"
- tonic-prost = "0.14.5" (used for codec in rr-raft/network_factory.rs)

### Proto Build System
- Uses `tonic-prost-build` (NOT vanilla `tonic-build`)
- build.rs calls: `tonic_prost_build::configure().compile_protos(...)`
- Proto files live in `crates/rr-network/proto/`

### RrLogStorage (D1 target)
- File: `crates/rr-raft/src/log_storage.rs`
- Uses `std::sync::RwLock` on line 12: `use std::sync::{Arc, RwLock};`
- `.read().expect("RrLogStorage lock poisoned")` and `.write().expect("RrLogStorage lock poisoned")` patterns throughout
- parking_lot::RwLock is correct (locks NOT held across .await)
- Tests exist: test_log_storage_append_and_read, test_log_storage_vote_save_read, etc.

### ChannelPool in rr-raft (D2/E1 target)
- File: `crates/rr-raft/src/network_factory.rs`
- Local ChannelPool defined at ~line 129 (avoids cyclic dep with rr-network)
- Has: get_or_connect, remove, len, is_empty methods
- GrpcNetworkFactory stores ChannelPool (value, not Arc)
- GrpcRaftNetwork stores ChannelPool (value, not Arc)
- rr-network has its OWN ChannelPool - DO NOT confuse the two

### CI State
- `.github/workflows/ci.yml` has: build, build-rocks, test, test-rocks, clippy, fmt jobs
- All run ubuntu-latest with stable toolchain only
- No: coverage, bench, audit, doc-test, msrv, macos, nightly, chaos jobs
- No RUSTFLAGS setting

### Metrics State
- metrics crate v0.24 in workspace
- rr-raft has metrics.workspace = true
- 7 counters exist across 3 files but NO recorder installed (goes nowhere)

### Existing Examples
- examples/kv-store/ — only example
- Wire format: bincode::serde::encode_to_vec(&KvRequest, standard())
- KvRequest: Put { key: String, value: Vec<u8> }, Get { key: String }, Delete { key: String }
- KvResponse: { value: Option<Vec<u8>> }

## Proptest Integration (A3)
- `proptest` v1.10.0 added as workspace dependency, crate-level `[dev-dependencies]` via `workspace = true`
- BincodeCodec is in `rr-core/src/codec.rs`, uses bincode 2.0 API (`bincode::serde::encode_to_vec` / `decode_from_slice`)
- For async storage tests inside `proptest!` macro: create `tokio::runtime::Runtime::new()` and use `rt.block_on(async { ... })` since `proptest!` doesn't natively support async
- proptest closures must return `Result<(), TestCaseError>` — use `Ok(())` at end of async block and `?` on the `block_on` result
- RocksStorage tests are already feature-gated via `#![cfg(feature = "rocks")]` at module level in `rocks.rs`, so proptest tests within the existing `#[cfg(test)]` block automatically inherit the gate
- MemStorage uses `"state_machine"` as a pre-created column family, suitable for proptest without extra setup
- rr-storage needed `tokio` in dev-dependencies with `rt` and `macros` features for the proptest Runtime usage


## A2 — Criterion Benchmark Suite

### Key Decisions
- Used `tokio::runtime::Runtime::new().unwrap().block_on(...)` pattern for async benches since criterion doesn't natively support async
- `log_append_rocks` bench creates a fresh `TempDir` per iteration (includes DB open cost in measurement)
- `mem_storage_put_scan` does 1000 puts then 1000 small-window scans (10-key range) to avoid O(n²) full-range scans
- `codec_roundtrip` uses a custom `TestPayload` struct with mixed types (u64, String, Vec<u8>) for realistic serialization
- All benches use `harness = false` as required by criterion
- Rocks bench uses `required-features = ["rocks"]` in `[[bench]]` section so it's skipped without the feature

### Patterns Discovered
- `RrLogStorage::default()` creates an empty in-memory log storage
- `storage.blocking_append(entries).await` is from `RaftLogStorageExt` trait
- Entries need `LogId::new(CommittedLeaderId::new(term, node_id), index)` with `EntryPayload::Blank`
- `MemStorage::new()` pre-creates `raft_log`, `state_machine`, `metadata` column families
- bincode 2.0 API: `bincode::serde::encode_to_vec(&val, bincode::config::standard())`
- CI `-- --test` flag makes criterion run in test mode (no timing, just validates compilation)

## D1 — parking_lot::RwLock swap in RrLogStorage

### Changes Made
- Added `parking_lot = "0.12"` to `crates/rr-raft/Cargo.toml` [dependencies]
- Replaced `use std::sync::{Arc, RwLock}` with `use std::sync::Arc` + `use parking_lot::RwLock`
- Removed 7 `.expect("RrLogStorage lock poisoned")` calls (parking_lot guards returned directly, not wrapped in Result)
- Lines affected: 51, 65, 87, 92, 105, 115, 128 (original line numbers)

### Key Facts
- parking_lot::RwLock is a drop-in replacement for std::sync::RwLock — same `read()` / `write()` method names
- The only API difference: no `LockResult` wrapper, so `.expect()` / `.unwrap()` must be removed
- `Arc<RwLock<Inner>>` struct field stays identical — just a different RwLock import
- All 41 rr-raft tests pass (including 5 log_storage tests) with zero behavior change
- parking_lot locks never panic on poison (they don't have poisoning), which is the primary benefit
## C3 — Counter Service Example

### Structure
- Single-file approach (`main.rs` only) — simpler than kv-store's multi-file layout
- Types (CounterRequest/CounterResponse), Service impl, CLI, demo, tests all in one file
- ~80 lines of application code (types + service impl), rest is CLI/demo/tests boilerplate

### Patterns Applied
- Same CLI arg parsing as kv-store (--node-id, --port, --peers, --bootstrap, --demo)
- Default port 52001 (vs kv-store's 51001) to avoid conflicts
- bincode 2.0 API: `encode_to_vec(&val, standard())` / `decode_from_slice(&bytes, standard())`
- `RrError::Codec(CodecError::DeserializeFailed(...))` for error mapping
- `Arc<RwLock<HashMap<String, i64>>>` for thread-safe counter storage
- Increment returns current value after operation (useful for clients)
- 9 unit tests covering: create, accumulate, negative delta, get, get-missing, reset, snapshot/restore, name, independence

## A4: CI Pipeline Enhancement (COMPLETED)

### Implementation Details

Successfully enhanced the CI pipeline in `.github/workflows/ci.yml` with the following changes:

1. **Cargo.toml**: Added `[workspace.package]` section with `rust-version = "1.82"` to define MSRV
   - Workspace structure: Uses `[workspace.dependencies]` for shared deps
   - New `[workspace.package]` added to define MSRV

2. **RUSTFLAGS**: Added `RUSTFLAGS: "--deny=warnings"` to all existing 8 jobs:
   - `build`, `build-rocks`, `test`, `test-rocks`, `clippy`, `fmt`, `coverage`, `bench`
   - Added at job-level `env:` section to apply globally to all steps

3. **New Jobs** (6 total):
   - **msrv**: Runs `cargo check --workspace` on Rust 1.82 (uses `dtolnay/rust-toolchain@1.82`)
   - **doc-test**: Runs `cargo test --doc --workspace --no-deps` on stable
   - **audit**: Uses `taiki-e/install-action@cargo-audit` and runs `cargo audit`
   - **nightly**: Runs `cargo build --workspace` on nightly with `continue-on-error: true`
   - **test-macos**: Runs `cargo test --workspace` on `macos-latest`
   - **integration**: Runs `cargo test -p integration` with `timeout-minutes: 10`

### Verification

✓ YAML is valid (14 total jobs)
✓ All existing jobs have RUSTFLAGS
✓ All new jobs added successfully
✓ Nightly job has continue-on-error: true
✓ Integration job has timeout-minutes: 10
✓ MSRV uses Rust 1.82 toolchain
✓ Audit uses taiki-e/install-action@cargo-audit
✓ macOS job uses macos-latest runner

### Notes

- Total jobs increased from 8 to 14
- CI pipeline now covers: MSRV testing, nightly builds, macOS compatibility, doc tests, security audits, integration tests
- All jobs properly cache dependencies for faster runs
- Nightly failures won't block CI (continue-on-error: true)

## B3 — Admin gRPC Service

### Implementation Details
- Proto file: `crates/rr-network/proto/admin.proto` with custom `Empty` message (matches client.proto pattern)
- Service impl: `crates/rr-server/src/admin.rs` — `AdminServiceImpl` struct
- 5 RPCs: GetNodeStatus, GetGroupStatus, TriggerSnapshot, TransferLeader, GetMetricsSummary
- Config: `admin_port: Option<u16>` on `NodeConfig` with `with_admin_port()` builder — defaults to `None`
- Wired into `RrNode::start()` as a separate tonic server on `admin_port` (step 4), with its own `admin_handle`
- Shutdown: admin server reuses the same `shutdown_tx` channel as the main gRPC server

### openraft 0.9.21 API for Admin Operations
- `raft.metrics()` returns `watch::Receiver<RaftMetrics<NodeId, Node>>` — `.borrow().clone()` for snapshot
- `raft.trigger().snapshot().await` — triggers log compaction snapshot (NOT the deprecated `trigger_snapshot()`)
- `raft.trigger().elect().await` — triggers election on this node (NOT `trigger_elect()`)
- openraft 0.9 does NOT have `transfer_leader()` — admin API handles this by triggering election on target if local, or returning error for remote nodes
- `RaftMetrics` fields: `current_leader: Option<NID>`, `current_term: u64`, `last_log_index: Option<u64>`, `state: ServerState`

### Key Patterns
- Proto module export: `tonic::include_proto!("rr.admin")` inside `pub mod admin { }` in rr-network/src/lib.rs
- Admin goes through `RaftGroupManager::get_raft()` → `openraft::Raft<RrTypeConfig>` for per-group operations
- `MetricsSummary` returns zeros for now — real metrics flow through Prometheus recorder (B1 task)
- 5 unit tests covering: construction, node status, group not found, trigger snapshot not found, metrics summary

## E3 — Defensive Store Mode for RrLogStorage

### Implementation Details
- `defensive` feature flag added to `crates/rr-raft/Cargo.toml` `[features]`
- Dual activation: `cfg!(feature = "defensive")` OR `RR_STORE_DEFENSIVE=on` env var
- `is_defensive()` helper function at module level in `log_storage.rs`

### Defensive Checks Added
1. **`append` contiguity check**: Verifies each entry's index == expected_next (last_stored + 1 or last_purged + 1). Handles empty storage gracefully (first entry has no constraint).
2. **`try_get_log_entries` range check**: Extracts start/end from `RangeBounds<u64>` and asserts start <= end when both bounds are finite.
3. **`truncate` commit-index check**: SKIPPED — `Inner` struct does not track commit index, so the check is not applicable.

### Key Patterns
- `LogFlushed::new` is `pub(crate)` in openraft 0.9.21 — cannot construct directly from outside the crate
- Use `RaftLogStorageExt::blocking_append` for tests instead (creates its own callback)
- `#[should_panic]` tests work fine with `blocking_append` — panics propagate through the await chain
- `std::env::set_var("RR_STORE_DEFENSIVE", "on")` in tests to enable defensive mode via env var
- Defensive checks use `assert!` (panics) not `Result::Err` — fail-fast philosophy

### CI
- `test-defensive` job added to `.github/workflows/ci.yml`
- Uses `RR_STORE_DEFENSIVE: "on"` env var (not feature flag) for broader coverage
- Follows same pattern as `test` job (ubuntu-latest, stable, rust-cache, RUSTFLAGS deny warnings)

### Test Results
- 202 tests pass (199 original + 3 defensive: contiguous success, non-contiguous panic, start>end panic)
- clippy passes with zero warnings

## [2026-03-05] Task: B2 — Expand Metrics

### Metrics API (metrics crate v0.24)
- Pattern: `counter!("name", "label" => val).increment(N)`, `histogram!("name", "label" => val).record(value)`, `gauge!("name", "label" => val).set(value)`
- Without a recorder installed, all metrics macros are no-ops (safe for tests)
- Existing convention: `use metrics::counter;` and call `counter!("dotted.name", "label" => val).increment(1)`
- New metrics use underscore-separated names: `rr_proposal_duration_seconds`, `rr_replication_batch_entries`, etc.

### Where metrics are recorded
- `group_manager.rs`: New `write()` method wraps `raft.client_write()` — records `rr_proposal_duration_seconds` histogram, updates `rr_raft_is_leader`/`rr_raft_current_term`/`rr_raft_last_log_index` gauges from `raft.metrics().borrow()`
- `network_factory.rs`: `GrpcRaftNetwork::append_entries()` — records `rr_replication_batch_entries` histogram (`rpc.entries.len()`) before gRPC call
- `service_adapter.rs`: `restore()` method — increments `rr_snapshot_installs_total` counter after successful restore (group_id="unknown" since adapter has no group_id field)

### Label values
- `group_id` label used consistently across all 6 metrics
- `service_adapter.rs` uses `"unknown"` for group_id since the adapter doesn't carry group context
- Other files use `group_id.to_string()` or `self.group_id.to_string()`

### Test approach
- Since metrics macros are no-ops without a recorder, tests just verify code paths execute without panic
- `test_write_records_proposal_metrics`: Single-node cluster init, wait for leader, call write()
- `test_append_entries_records_batch_metric`: Exercise append_entries (fails on connect, but metric recorded before gRPC call)
- `test_restore_records_snapshot_install_metric`: Call restore() directly on adapter

### Test Results
- 205 tests pass (202 previous + 3 new metric tests)
- clippy passes with zero warnings

## [2026-03-05] Task: D2 — Connection Pool Reconnect

### How `remove_and_reconnect` works
- Added `remove_and_reconnect(node_id, addr)` to `ChannelPoolLike` trait and `ChannelPool` struct
- Implementation: evicts the cached channel via `remove()`, then calls `get_or_connect()` to establish a fresh connection
- This is a composite operation — remove first, then reconnect — ensuring stale channels are always cleared

### Error detection pattern for channel eviction
- In `append_entries`, `vote`, `install_snapshot`: intercept `tonic::Status` errors in `.map_err()` closure
- Check `status.code() == tonic::Code::Unavailable` before converting to `RPCError::Unreachable`
- On Unavailable: call `self.channel_pool.remove(self.target)` to evict stale channel
- Increment `rr_channel_reconnects_total` counter with `node_id` label using metrics v0.24 API (`counter!(...).increment(1)`)
- Max 1 eviction per RPC call — no retry loop, just evict and return the error. Next RPC call will create fresh channel via `get_or_connect`

### Test strategy
- `test_remove_clears_channel`: Insert lazy channel via `channels` DashMap, verify `remove()` clears it
- `test_remove_and_reconnect_evicts_old_channel`: Verify old channel is evicted even when reconnect fails (no server)
- `test_unavailable_error_evicts_channel`: Full integration test through `append_entries` with lazy channel to unreachable addr

### Important: trait implementors
- `FaultAwareChannelPool` in `tests/integration/src/fault_injection.rs` also implements `ChannelPoolLike` — had to add `remove_and_reconnect` there too
- Its implementation: delegates `remove` to inner pool, then calls `self.get_or_connect()` (goes through fault injection layer)

## [2026-03-05] Task: B4 — gRPC Health Service

- Added `tonic-health = "0.14"` to `rr-server/Cargo.toml` and `tests/integration/Cargo.toml`
- `tonic_health::server::health_reporter()` returns `(HealthReporter, HealthServer)` — the reporter does NOT need `mut` for `set_service_status`
- Used `set_service_status("", ServingStatus::Serving)` for the overall (empty-string) service name — this is what health clients check by default
- Health service is added to the main gRPC server builder alongside `RaftServiceServer` and `ClientServiceServer`
- `HealthReporter` stored in `Mutex<Option<HealthReporter>>` on `RrNode` — only populated when gRPC server is started (requires `raft_lookup` + `client_dispatch`)
- On shutdown: set NOT_SERVING before sending the shutdown signal to allow in-flight health checks to see the transition
- Integration test uses `tonic_health::pb::health_client::HealthClient` with `connect_lazy()` to verify SERVING status
- The health service is only on the main gRPC port, not the admin port
