# Phase 2: Production Readiness for rr Distributed Framework

## TL;DR

> **Quick Summary**: Extend the rr multi-Raft distributed framework from single-node demo to production-ready cluster: real multi-node networking, persistent RocksDB storage, dynamic partition/node management, client SDK with leader-redirect, full Raft snapshot transfer, and CI/CD automation.
> 
> **Deliverables**:
> - Real multi-node Raft consensus over gRPC (3+ nodes)
> - RocksDB-backed persistent StorageEngine (feature-gated)
> - Dynamic partition/node add/remove at runtime
> - Client SDK crate with auto leader-redirect and retry
> - Complete install_snapshot flow with streaming
> - GitHub Actions CI pipeline (build, test, clippy, fmt)
> 
> **Estimated Effort**: XL (30+ tasks across 6 work areas)
> **Parallel Execution**: YES - 6 waves
> **Critical Path**: T1→T2→T3→T5→T8→T11→T14→T17→T20→T25→T28→FINAL

---

## Context

### Original Request
User requested 6 work areas to bring rr from Phase 1 (single-node demo with 113 tests) to production readiness:
1. 多节点集群测试 — Real multi-node cluster tests
2. 持久化 StorageEngine — RocksDB-backed storage
3. 动态配置 — Runtime add/remove partitions and nodes
4. 客户端 SDK — gRPC client with auto leader-redirect
5. Snapshot 传输 — Complete install_snapshot flow
6. CI/CD — GitHub Actions automated testing

### Interview Summary
**Key Discussions**:
- User approved all 6 work areas as necessary
- TDD methodology continues from Phase 1
- Existing codebase: 7 crates, 113 tests, 14 commits, zero warnings

**Research Findings**:
- **Network Layer**: GrpcNetworkFactory is fully implemented but RaftNode hardcodes NoopNetworkFactory. No production RaftGroupLookup or ClientDispatch impls exist.
- **Storage Layer**: StorageEngine trait is clean (7 methods). Raft log (RrLogStorage) is separate from StorageEngine — uses its own in-memory BTreeMap. RocksDB v0.24.0 is the target.
- **Config Layer**: ShardRouter already dynamic-ready (no caching). RrNode reads config once at startup — no watch loop. RoutingTableChange missing node/membership events.
- **RocksDB Patterns**: TiKV uses shared DB with key prefixes (NOT one CF per Raft group). DB is Send+Sync, wrap in Arc. Use Checkpoint API for snapshots.

### Metis Review
**Identified Gaps** (addressed):
- RrLogStorage is also in-memory — must be persisted for production (included in RocksDB work area)
- Vote storage in-memory risks split-brain after restart (included)
- No `list_cf` or `drop_cf` on StorageEngine trait — may need extension
- Snapshot streaming needs chunked transfer with backpressure
- Client SDK needs service discovery / bootstrap mechanism
- CI needs to handle RocksDB native compilation

---

## Work Objectives

### Core Objective
Transform rr from a single-node demo into a production-ready distributed framework that can run real multi-node Raft clusters with persistent storage, dynamic reconfiguration, and a developer-friendly client SDK.

### Concrete Deliverables
- `RaftGroupLookupImpl` and `ClientDispatchImpl` — production gRPC dispatch
- `RocksStorage` implementing `StorageEngine` — persistent storage with feature gate
- `RocksLogStorage` implementing `RaftLogStorage` — persistent Raft log
- `RrNode` config watch loop — dynamic partition/node management
- `rr-client` crate — client SDK with leader redirect
- Complete snapshot transfer flow
- `.github/workflows/ci.yml` — CI pipeline

### Definition of Done
- [x] 3-node cluster can elect leaders, replicate writes, and survive node restarts
- [x] Data persists across process restarts via RocksDB
- [x] Partitions can be added/removed at runtime
- [x] Client SDK can write to cluster with automatic leader discovery
- [x] Snapshots transfer between nodes
- [x] CI pipeline passes on every PR

### Must Have
- All existing 113 tests continue to pass
- No regressions in existing crates
- Feature-gated RocksDB (don't force native dependency on all users)
- TDD approach for all new code

### Must NOT Have (Guardrails)
- ❌ No `anyhow` in library crates (only examples/tests)
- ❌ No `println!`/`eprintln!` in library crates — use tracing
- ❌ No circular dependencies between crates
- ❌ `rr-core` must remain DAG root with ZERO deps on other rr-* crates
- ❌ No placeholder assertions `assert!(true)`
- ❌ No tokio::spawn without JoinHandle tracking
- ❌ No global state / static singletons
- ❌ No hardcoded addresses or ports
- ❌ No one-CF-per-Raft-group pattern (follow TiKV: shared DB with key prefixes)
- ❌ No blocking RocksDB calls on tokio runtime — must use spawn_blocking
- ❌ No long-lived RocksDB snapshots (prevent compaction stalls)

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed. No exceptions.

### Test Decision
- **Infrastructure exists**: YES (113 tests across 8 crates)
- **Automated tests**: TDD (test-first)
- **Framework**: `cargo test` (Rust built-in)
- **Each task follows**: RED (failing test) → GREEN (minimal impl) → REFACTOR

### QA Policy
Every task MUST include agent-executed QA scenarios.
Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **Multi-node clusters**: Use `interactive_bash` (tmux) — Start multiple nodes, verify leader election, write replication
- **Storage**: Use Bash — Write data, restart process, verify data persists
- **Client SDK**: Use Bash (cargo run) — Connect to cluster, write/read, verify redirect
- **CI/CD**: Use Bash (gh workflow) — Push commit, verify pipeline passes

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Foundation — unblock all other work):
├── T1: Fix RaftNode to accept NetworkFactory [quick]
├── T2: Implement RaftGroupLookupImpl [deep]
├── T3: Implement ClientDispatchImpl [deep]
├── T4: Fix SnapshotResponse proto payload field [quick]
├── T5: Wire GrpcNetworkFactory into RaftGroupManager [deep]
├── T6: RocksDB crate setup + feature gate [quick]
├── T7: RocksStorage implementing StorageEngine [deep]
├── T8: CI/CD GitHub Actions pipeline [quick]

Wave 2 (Core capabilities — after Wave 1):
├── T9: RocksLogStorage implementing openraft RaftLogStorage [deep]
├── T10: RocksDB production config tuning + benchmarks [unspecified-high]
├── T11: Multi-node integration test harness [deep]
├── T12: 3-node leader election + write replication tests [deep]
├── T13: Extend RoutingTableChange with node/membership events [quick]

Wave 3 (Dynamic config + snapshots — after Wave 2):
├── T14: RrNode config watch loop [deep]
├── T15: Expose openraft membership APIs (add_learner, change_membership) [deep]
├── T16: Dynamic partition add at runtime [deep]
├── T17: Complete install_snapshot flow [deep]
├── T18: Snapshot streaming with chunked transfer [deep]

Wave 4 (Client SDK + advanced dynamic — after Wave 3):
├── T19: rr-client crate scaffolding + gRPC client [unspecified-high]
├── T20: Client auto leader-redirect + retry [deep]
├── T21: Dynamic node add/remove at runtime [deep]
├── T22: Node restart with RocksDB persistence test [deep]

Wave 5 (Integration + hardening — after Wave 4):
├── T23: Client SDK integration tests (write through SDK) [deep]
├── T24: Partition migration test (move partition between nodes) [deep]
├── T25: Node failure + recovery test [deep]
├── T26: Snapshot-based node catch-up test [deep]

Wave 6 (Polish — after Wave 5):
├── T27: KV store example updated for multi-node [unspecified-high]
├── T28: End-to-end smoke test (full cluster lifecycle) [deep]

Wave FINAL (After ALL tasks — independent review, 4 parallel):
├── F1: Plan compliance audit (oracle)
├── F2: Code quality review (unspecified-high)
├── F3: Real manual QA (unspecified-high)
├── F4: Scope fidelity check (deep)

Critical Path: T1→T5→T11→T12→T14→T17→T20→T25→T28→FINAL
Parallel Speedup: ~65% faster than sequential
Max Concurrent: 8 (Wave 1)
```

### Dependency Matrix

| Task | Depends On | Blocks |
|------|-----------|--------|
| T1 | — | T5, T9 |
| T2 | — | T5, T11 |
| T3 | — | T5, T11, T20 |
| T4 | — | T17, T18 |
| T5 | T1, T2, T3 | T11, T12 |
| T6 | — | T7, T9, T10 |
| T7 | T6 | T10, T22 |
| T8 | — | — |
| T9 | T1, T6 | T22 |
| T10 | T7 | — |
| T11 | T5 | T12, T24, T25 |
| T12 | T11 | T14, T23 |
| T13 | — | T14, T16, T21 |
| T14 | T12, T13 | T16, T21 |
| T15 | — | T16, T21 |
| T16 | T14, T15 | T24 |
| T17 | T4 | T18, T26 |
| T18 | T17 | T26 |
| T19 | — | T20, T23 |
| T20 | T3, T19 | T23 |
| T21 | T14, T15 | T25 |
| T22 | T7, T9, T11 | T25 |
| T23 | T12, T20 | T28 |
| T24 | T11, T16 | T28 |
| T25 | T11, T21, T22 | T28 |
| T26 | T11, T18 | T28 |
| T27 | T12, T20 | T28 |
| T28 | T23, T24, T25, T26, T27 | FINAL |

### Agent Dispatch Summary

- **Wave 1**: **8** — T1→`quick`, T2→`deep`, T3→`deep`, T4→`quick`, T5→`deep`, T6→`quick`, T7→`deep`, T8→`quick`
- **Wave 2**: **5** — T9→`deep`, T10→`unspecified-high`, T11→`deep`, T12→`deep`, T13→`quick`
- **Wave 3**: **5** — T14→`deep`, T15→`deep`, T16→`deep`, T17→`deep`, T18→`deep`
- **Wave 4**: **4** — T19→`unspecified-high`, T20→`deep`, T21→`deep`, T22→`deep`
- **Wave 5**: **4** — T23→`deep`, T24→`deep`, T25→`deep`, T26→`deep`
- **Wave 6**: **2** — T27→`unspecified-high`, T28→`deep`
- **FINAL**: **4** — F1→`oracle`, F2→`unspecified-high`, F3→`unspecified-high`, F4→`deep`

---

## TODOs


### Wave 1 — Foundation (unblock all other work)

- [x] 1. Refactor RaftNode to Accept NetworkFactory Parameter

  **What to do**:
  - Modify `RaftNode::new()` to accept a `Box<dyn RaftNetworkFactory<RrTypeConfig>>` parameter instead of hardcoding `NoopNetworkFactory`
  - Update the constructor signature: `pub fn new(group_id: GroupId, node_id: NodeId, network_factory: Box<dyn RaftNetworkFactory<RrTypeConfig>>) -> Result<Self, RrError>`
  - Keep `NoopNetworkFactory` as the default for single-node/test scenarios via a `new_single_node()` constructor
  - Update all existing callers (RaftGroupManager, tests) to pass the network factory
  - Write tests verifying both constructors work

  **Must NOT do**:
  - Do not change the Raft TypeConfig
  - Do not modify GrpcNetworkFactory itself
  - Do not break existing single-node tests

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
    - Simple signature refactor with clear before/after

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T2, T3, T4, T6, T7, T8)
  - **Parallel Group**: Wave 1
  - **Blocks**: T5, T9
  - **Blocked By**: None

  **References**:
  **Pattern References**:
  - `crates/rr-raft/src/raft_node.rs:40-63` — Current RaftNode::new() that hardcodes NoopNetworkFactory at line 58
  - `crates/rr-raft/src/raft_node.rs:86-138` — NoopNetworkFactory and NoopNetwork implementations (keep for single-node)
  - `crates/rr-raft/src/network_factory.rs:248-274` — GrpcNetworkFactory implementing RaftNetworkFactory (this is what should be injectable)

  **API/Type References**:
  - `crates/rr-raft/src/type_config.rs` — RrTypeConfig definition
  - openraft `RaftNetworkFactory<RrTypeConfig>` trait — the generic parameter for the factory

  **Test References**:
  - `crates/rr-raft/src/raft_node.rs` — Existing tests that create RaftNode (update to use new_single_node())
  - `tests/integration/src/lib.rs:92` — ClusterBuilder creates RaftNodes

  **Acceptance Criteria**:
  - [x] `RaftNode::new(group_id, node_id, network_factory)` accepts any `RaftNetworkFactory`
  - [x] `RaftNode::new_single_node(group_id, node_id)` uses NoopNetworkFactory
  - [x] `cargo test -p rr-raft` — all existing tests pass
  - [x] `cargo test -p integration` — all 4 integration tests pass

  **QA Scenarios:**
  ```
  Scenario: RaftNode accepts GrpcNetworkFactory
    Tool: Bash
    Preconditions: Workspace compiles
    Steps:
      1. Run `cargo test -p rr-raft -- --nocapture 2>&1`
      2. Verify output contains "test result: ok"
      3. Run `cargo test -p integration -- --nocapture 2>&1`
      4. Verify output contains "test result: ok" with 4 tests
    Expected Result: All tests pass, no compilation errors
    Failure Indicators: "error[E" in output, test failures
    Evidence: .sisyphus/evidence/task-1-raft-node-factory.txt
  ```

  **Commit**: YES (groups with T4)
  - Message: `fix(raft): accept NetworkFactory parameter + fix snapshot proto`
  - Files: `crates/rr-raft/src/raft_node.rs`, `crates/rr-raft/src/group_manager.rs`
  - Pre-commit: `cargo test -p rr-raft`

- [x] 2. Implement Production RaftGroupLookupImpl

  **What to do**:
  - Create `RaftGroupLookupImpl` struct in `crates/rr-server/src/` (new file `lookup.rs`)
  - Implement `RaftGroupLookup` trait (defined in `crates/rr-network/src/raft_service.rs:32-63`)
  - The impl wraps `Arc<RaftGroupManager>` and dispatches incoming Raft RPCs to the correct group's openraft handle
  - For `append_entries`: deserialize bincode request → `raft.append_entries(request).await` → serialize response
  - For `vote`: deserialize bincode request → `raft.vote(request).await` → serialize response  
  - For `install_snapshot`: deserialize bincode request → `raft.install_snapshot(request).await` → serialize response
  - Wire into `NodeConfig` so `RrNode` uses it when starting gRPC server
  - Write tests with mock RaftGroupManager verifying dispatch

  **Must NOT do**:
  - Do not modify the `RaftGroupLookup` trait definition
  - Do not add dependencies from rr-network to rr-raft (would be circular)
  - Do not handle leader forwarding here (that's ClientDispatch's job)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Requires understanding openraft API, bincode serialization, and the trait boundary between rr-network and rr-server

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1, T3, T4, T6, T7, T8)
  - **Parallel Group**: Wave 1
  - **Blocks**: T5, T11
  - **Blocked By**: None

  **References**:
  **Pattern References**:
  - `crates/rr-network/src/raft_service.rs:32-63` — RaftGroupLookup trait definition (bytes-oriented to avoid cyclic deps)
  - `crates/rr-network/src/raft_service.rs:167-201` — EmptyLookup test mock (follow this pattern)
  - `crates/rr-raft/src/group_manager.rs:93-99` — `get_raft(group_id)` returns openraft handle

  **API/Type References**:
  - `crates/rr-raft/src/type_config.rs` — RrTypeConfig (D=Vec<u8>, R=Vec<u8>, NodeId=u64, Node=BasicNode)
  - openraft `Raft::append_entries()`, `Raft::vote()`, `Raft::install_snapshot()` — the APIs to call
  - bincode v2: `bincode::serde::encode_to_vec(&val, config::standard())` and `decode_from_slice`

  **Test References**:
  - `crates/rr-network/src/raft_service.rs:167-201` — EmptyLookup shows the trait's test mock pattern
  - `crates/rr-server/src/node.rs:245-359` — Existing RrNode tests (for wiring pattern)

  **Acceptance Criteria**:
  - [x] `RaftGroupLookupImpl` implements all 3 methods of `RaftGroupLookup`
  - [x] Incoming RPCs are dispatched to correct Raft group by group_id
  - [x] Unknown group_id returns appropriate error
  - [x] `cargo test -p rr-server` — all tests pass including new ones

  **QA Scenarios:**
  ```
  Scenario: Lookup dispatches to correct group
    Tool: Bash
    Preconditions: RaftGroupLookupImpl created
    Steps:
      1. Run `cargo test -p rr-server -- lookup --nocapture 2>&1`
      2. Verify tests for append_entries, vote, install_snapshot dispatch
      3. Verify test for unknown group_id error handling
    Expected Result: All lookup tests pass
    Failure Indicators: "FAILED" or "error[E" in output
    Evidence: .sisyphus/evidence/task-2-lookup-impl.txt

  Scenario: Unknown group returns error
    Tool: Bash
    Preconditions: RaftGroupLookupImpl created
    Steps:
      1. Run `cargo test -p rr-server -- lookup_unknown_group --nocapture 2>&1`
      2. Verify error message contains group_id
    Expected Result: Error returned with meaningful message
    Evidence: .sisyphus/evidence/task-2-lookup-unknown.txt
  ```

  **Commit**: YES (groups with T3)
  - Message: `feat(server): RaftGroupLookupImpl + ClientDispatchImpl`
  - Files: `crates/rr-server/src/lookup.rs`, `crates/rr-server/src/lib.rs`
  - Pre-commit: `cargo test -p rr-server`

- [x] 3. Implement Production ClientDispatchImpl

  **What to do**:
  - Create `ClientDispatchImpl` struct in `crates/rr-server/src/` (new file `dispatch.rs`)
  - Implement `ClientDispatch` trait (defined in `crates/rr-network/src/client_service.rs`)
  - The impl wraps `Arc<RaftGroupManager>` + `Arc<ShardRouter>`
  - `submit_write(group_id, payload)`: find Raft handle → `raft.client_write(payload).await` → return response
  - If not leader: extract leader info from openraft error and return it for redirect
  - `get_cluster_info()`: gather group_ids, current leaders, node addresses
  - Wire into `NodeConfig`
  - Write tests verifying dispatch and leader-redirect info extraction

  **Must NOT do**:
  - Do not implement client-side redirect (that's in rr-client T20)
  - Do not add deps from rr-network to rr-raft
  - Do not route by partition_id here (router already resolves to group_id)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Requires understanding openraft error types for leader extraction

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1, T2, T4, T6, T7, T8)
  - **Parallel Group**: Wave 1
  - **Blocks**: T5, T11, T20
  - **Blocked By**: None

  **References**:
  **Pattern References**:
  - `crates/rr-network/src/client_service.rs:72-94` — ClientServiceImpl.request() showing how ClientDispatch is called
  - `crates/rr-network/src/client_service.rs:113-168` — MockDispatch test implementation
  - `crates/rr-network/src/client_service.rs:84-87` — Leader redirect info population

  **API/Type References**:
  - `crates/rr-network/src/client_service.rs` — ClientDispatch trait: `submit_write(group_id, payload) -> Result<(Vec<u8>, Option<LeaderInfo>)>`
  - `crates/rr-raft/src/group_manager.rs:93-99` — `get_raft(group_id)` for handle lookup
  - `crates/rr-router/src/router.rs` — ShardRouter for partition→group resolution
  - openraft `ClientWriteError::ForwardToLeader { leader_id, leader_node }` — extract redirect info

  **Test References**:
  - `crates/rr-network/src/client_service.rs:113-168` — MockDispatch pattern to follow

  **Acceptance Criteria**:
  - [x] `ClientDispatchImpl` implements `submit_write` and `get_cluster_info`
  - [x] Leader redirect info correctly extracted from openraft errors
  - [x] `cargo test -p rr-server` — all tests pass including new ones

  **QA Scenarios:**
  ```
  Scenario: Client write dispatched to correct group
    Tool: Bash
    Preconditions: ClientDispatchImpl created
    Steps:
      1. Run `cargo test -p rr-server -- dispatch --nocapture 2>&1`
      2. Verify write dispatched to correct Raft group
      3. Verify cluster info returns group IDs and leaders
    Expected Result: All dispatch tests pass
    Failure Indicators: "FAILED" or compilation errors
    Evidence: .sisyphus/evidence/task-3-dispatch-impl.txt

  Scenario: Non-leader returns redirect info
    Tool: Bash
    Preconditions: ClientDispatchImpl created
    Steps:
      1. Run `cargo test -p rr-server -- dispatch_redirect --nocapture 2>&1`
      2. Verify LeaderInfo populated with leader_id and address
    Expected Result: Redirect info contains valid leader address
    Evidence: .sisyphus/evidence/task-3-dispatch-redirect.txt
  ```

  **Commit**: YES (groups with T2)
  - Message: `feat(server): RaftGroupLookupImpl + ClientDispatchImpl`
  - Files: `crates/rr-server/src/dispatch.rs`, `crates/rr-server/src/lib.rs`
  - Pre-commit: `cargo test -p rr-server`

- [x] 4. Fix SnapshotResponse Proto Payload Field

  **What to do**:
  - Add `bytes payload = 2;` field to `SnapshotResponse` message in `raft.proto`
  - Update `GrpcNetworkFactory::install_snapshot()` to deserialize the payload into `InstallSnapshotResponse` instead of returning hardcoded `Vote::new(0,0)`
  - Update `RaftServiceImpl::install_snapshot()` to serialize the openraft response into the payload field
  - Regenerate proto code via `tonic-build`
  - Write test verifying round-trip of InstallSnapshotResponse through proto

  **Must NOT do**:
  - Do not change the streaming chunk format
  - Do not modify AppendEntries or Vote protos

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
    - Straightforward proto field addition + serialization fix

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1, T2, T3, T6, T7, T8)
  - **Parallel Group**: Wave 1
  - **Blocks**: T17, T18
  - **Blocked By**: None

  **References**:
  **Pattern References**:
  - `crates/rr-network/proto/raft.proto` — SnapshotResponse message (currently only has `bool success = 1`)
  - `crates/rr-raft/src/network_factory.rs:357-401` — install_snapshot() with hardcoded Vote::new(0,0) at line 392-400
  - `crates/rr-raft/src/network_factory.rs:297-325` — append_entries() showing correct bincode round-trip pattern to follow

  **API/Type References**:
  - `crates/rr-network/proto/raft.proto` — SnapshotResponse, SnapshotChunk messages
  - openraft `InstallSnapshotResponse` — the type that should be serialized into payload

  **Acceptance Criteria**:
  - [x] `SnapshotResponse` proto has `bytes payload` field
  - [x] `install_snapshot()` correctly deserializes openraft response from payload
  - [x] `cargo build -p rr-network` compiles with updated proto
  - [x] Round-trip test passes for InstallSnapshotResponse

  **QA Scenarios:**
  ```
  Scenario: Snapshot response carries real payload
    Tool: Bash
    Preconditions: Proto updated, code regenerated
    Steps:
      1. Run `cargo test -p rr-raft -- snapshot_response --nocapture 2>&1`
      2. Verify InstallSnapshotResponse round-trips through proto
    Expected Result: Vote data preserved through serialization
    Failure Indicators: Assertion failure on vote values
    Evidence: .sisyphus/evidence/task-4-snapshot-proto.txt
  ```

  **Commit**: YES (groups with T1)
  - Message: `fix(raft): accept NetworkFactory parameter + fix snapshot proto`
  - Files: `crates/rr-network/proto/raft.proto`, `crates/rr-raft/src/network_factory.rs`
  - Pre-commit: `cargo test -p rr-raft -p rr-network`

---

- [x] 5. Wire GrpcNetworkFactory into RaftGroupManager

  **What to do**:
  - Modify `RaftGroupManager::new()` to accept a `ChannelPool` (or `Arc<ChannelPool>`)
  - Modify `start_group()` to create `GrpcNetworkFactory` with the channel pool and pass it to `RaftNode::new()`
  - Ensure each Raft group gets a `GrpcNetworkFactory` that can connect to other nodes
  - Node address resolution: `GrpcNetworkFactory` uses `BasicNode.addr` from openraft membership — ensure `RaftNode::init_cluster()` registers nodes with their addresses
  - Add `init_cluster(group_id, members: BTreeMap<NodeId, BasicNode>)` method to RaftGroupManager for multi-node bootstrap
  - Write integration test: create RaftGroupManager with real GrpcNetworkFactory, verify Raft creation succeeds

  **Must NOT do**:
  - Do not start the gRPC server here (that's RrNode's job)
  - Do not modify GrpcNetworkFactory implementation
  - Do not handle dynamic membership changes here (that's T15)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Critical wiring task connecting multiple subsystems

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 1 (sequential after T1, T2, T3)
  - **Blocks**: T11, T12
  - **Blocked By**: T1, T2, T3

  **References**:
  **Pattern References**:
  - `crates/rr-raft/src/group_manager.rs:46-65` — Current start_group() that creates RaftNode without network
  - `crates/rr-raft/src/network_factory.rs:248-274` — GrpcNetworkFactory::new(pool) constructor
  - `crates/rr-network/src/channel_pool.rs` — ChannelPool for connection reuse

  **API/Type References**:
  - `crates/rr-raft/src/raft_node.rs` — Updated RaftNode::new() from T1 (accepts network factory)
  - openraft `Raft::initialize(members)` — for multi-node cluster bootstrap
  - `BasicNode { addr: String }` — node address for network factory

  **Acceptance Criteria**:
  - [x] `RaftGroupManager` creates groups with `GrpcNetworkFactory`
  - [x] `init_cluster(group_id, members)` bootstraps multi-node Raft cluster
  - [x] `cargo test -p rr-raft` — all tests pass

  **QA Scenarios:**
  ```
  Scenario: Group created with real network factory
    Tool: Bash
    Preconditions: T1, T2, T3 completed
    Steps:
      1. Run `cargo test -p rr-raft -- group_manager_with_network --nocapture 2>&1`
      2. Verify group starts with GrpcNetworkFactory (not NoopNetworkFactory)
    Expected Result: Group created, network factory injected
    Failure Indicators: "NoopNetworkFactory" in logs, test failures
    Evidence: .sisyphus/evidence/task-5-wired-factory.txt
  ```

  **Commit**: YES
  - Message: `feat(raft): wire GrpcNetworkFactory into RaftGroupManager`
  - Files: `crates/rr-raft/src/group_manager.rs`
  - Pre-commit: `cargo test -p rr-raft`

- [x] 6. RocksDB Crate Setup + Feature Gate

  **What to do**:
  - Add `rocksdb = "0.24.0"` to workspace Cargo.toml under `[workspace.dependencies]`
  - Add feature gate to `crates/rr-storage/Cargo.toml`: `[features] rocks = ["dep:rocksdb"]` with `rocksdb = { workspace = true, optional = true }`
  - Create `crates/rr-storage/src/rocks.rs` module (gated with `#[cfg(feature = "rocks")]`)
  - Create basic `RocksStorage` struct with `Arc<rocksdb::DB>` and `Cache`
  - Implement `RocksStorage::open(path, cfs)` constructor:
    - `Options::create_if_missing(true)`, `create_missing_column_families(true)`
    - Shared block cache (configurable size, default 256MB)
    - Open with `DB::open_cf_descriptors` for all requested CFs
  - Implement `RocksStorage::open_default(path)` that creates with standard CFs (`raft_log`, `state_machine`, `metadata`)
  - Verify `cargo build -p rr-storage` works WITHOUT rocks feature
  - Verify `cargo build -p rr-storage --features rocks` works WITH feature

  **Must NOT do**:
  - Do not implement StorageEngine trait yet (that's T7)
  - Do not add rocksdb dependency to any other crate
  - Do not make rocksdb a required dependency

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
    - Straightforward crate setup and feature gating

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1, T2, T3, T4, T5, T8)
  - **Parallel Group**: Wave 1
  - **Blocks**: T7, T9, T10
  - **Blocked By**: None

  **References**:
  **Pattern References**:
  - `crates/rr-storage/Cargo.toml` — Current dependencies (rr-core, async-trait, tokio)
  - `crates/rr-storage/src/lib.rs` — Current module structure
  - `crates/rr-storage/src/mem.rs` — MemStorage for constructor pattern reference

  **External References**:
  - `rocksdb` crate v0.24.0 — `DB::open_cf_descriptors`, `Options`, `Cache::new_lru_cache`
  - TiKV pattern: shared block cache, `create_if_missing`, `create_missing_column_families`

  **Acceptance Criteria**:
  - [x] `cargo build -p rr-storage` compiles without rocks
  - [x] `cargo build -p rr-storage --features rocks` compiles with rocks
  - [x] `RocksStorage::open_default("/tmp/test-db")` creates DB with 3 CFs
  - [x] DB is `Send + Sync` (wrapped in Arc)

  **QA Scenarios:**
  ```
  Scenario: Feature gate works both ways
    Tool: Bash
    Preconditions: rocksdb added to workspace
    Steps:
      1. Run `cargo build -p rr-storage 2>&1` — should succeed without rocks
      2. Run `cargo build -p rr-storage --features rocks 2>&1` — should succeed with rocks
      3. Verify no rocksdb symbols in default build: `nm target/debug/librr_storage.rlib 2>/dev/null | grep -c rocksdb` should be 0 or command fails
    Expected Result: Both builds succeed, rocks is optional
    Failure Indicators: Compilation errors, missing rocksdb when feature enabled
    Evidence: .sisyphus/evidence/task-6-feature-gate.txt
  ```

  **Commit**: YES (groups with T7)
  - Message: `feat(storage): RocksDB StorageEngine impl with feature gate`
  - Files: `Cargo.toml`, `crates/rr-storage/Cargo.toml`, `crates/rr-storage/src/rocks.rs`, `crates/rr-storage/src/lib.rs`
  - Pre-commit: `cargo build -p rr-storage && cargo build -p rr-storage --features rocks`

- [x] 7. RocksStorage Implementing StorageEngine Trait

  **What to do**:
  - Implement `StorageEngine` trait for `RocksStorage` in `crates/rr-storage/src/rocks.rs`
  - ALL RocksDB calls MUST use `tokio::task::spawn_blocking` (RocksDB is synchronous)
  - `put(cf, key, value)`: get CF handle → `db.put_cf(handle, key, value)` in spawn_blocking
  - `get(cf, key)`: get CF handle → `db.get_cf(handle, key)` in spawn_blocking
  - `delete(cf, key)`: get CF handle → `db.delete_cf(handle, key)` in spawn_blocking
  - `scan(cf, start, end)`: create ReadOptions with iterate_range → iterator_cf_opt in spawn_blocking
  - `write_batch(ops)`: create WriteBatch → add all ops → `db.write(batch)` in spawn_blocking
  - `flush()`: `db.flush()` in spawn_blocking
  - `create_cf(name)`: `db.create_cf(name, &Options::default())` in spawn_blocking
  - Error mapping: `rocksdb::Error` → `StorageError::Io(std::io::Error::new(...))`
  - Write comprehensive tests matching `mem.rs` test coverage:
    - put/get/delete basics
    - scan range
    - write_batch atomicity
    - create_cf at runtime
    - non-existent CF error
  - Use `tempdir` for test database paths

  **Must NOT do**:
  - Do not call RocksDB synchronously on tokio runtime (spawn_blocking required)
  - Do not use one CF per Raft group (shared DB with key prefixes)
  - Do not hold RocksDB iterators across await points
  - Do not add `rocksdb` to any crate other than rr-storage

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Core storage implementation requiring careful async wrapping

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1, T2, T3, T4, T5, T8) — depends on T6 but can start in Wave 1 if T6 finishes quickly
  - **Parallel Group**: Wave 1 (after T6)
  - **Blocks**: T10, T22
  - **Blocked By**: T6

  **References**:
  **Pattern References**:
  - `crates/rr-storage/src/mem.rs` — MemStorage implementation (match all method behaviors)
  - `crates/rr-core/src/storage.rs` — StorageEngine trait definition (7 methods + BatchOp)
  - `crates/rr-core/src/error.rs:33-41` — StorageError variants (Io, Corruption, NotFound)

  **External References**:
  - `rocksdb` v0.24.0 — `DB::put_cf`, `get_cf`, `delete_cf`, `iterator_cf_opt`, `WriteBatch`, `create_cf`
  - TiKV RocksEngine pattern: `Arc<DB>` + error mapping via `r2e` function
  - `ReadOptions::set_iterate_range(start..end)` for bounded scans

  **Test References**:
  - `crates/rr-storage/src/mem.rs` — Existing MemStorage tests (replicate same test cases)

  **Acceptance Criteria**:
  - [x] `RocksStorage` implements all 7 methods of `StorageEngine`
  - [x] All RocksDB calls wrapped in `spawn_blocking`
  - [x] `cargo test -p rr-storage --features rocks` — all tests pass (10+ tests)
  - [x] Tests use tempdir for database paths
  - [x] Error mapping preserves error context

  **QA Scenarios:**
  ```
  Scenario: Full StorageEngine compliance
    Tool: Bash
    Preconditions: T6 complete, RocksStorage created
    Steps:
      1. Run `cargo test -p rr-storage --features rocks -- --nocapture 2>&1`
      2. Verify tests for: put/get, delete, scan, write_batch, flush, create_cf
      3. Verify test count >= 10
    Expected Result: All storage tests pass
    Failure Indicators: Test failures, "blocking" warnings from tokio
    Evidence: .sisyphus/evidence/task-7-rocks-storage.txt

  Scenario: Non-existent CF returns error
    Tool: Bash
    Preconditions: RocksStorage created
    Steps:
      1. Run `cargo test -p rr-storage --features rocks -- nonexistent_cf --nocapture 2>&1`
      2. Verify StorageError::NotFound returned for unknown CF
    Expected Result: Appropriate error, not a panic
    Evidence: .sisyphus/evidence/task-7-rocks-cf-error.txt
  ```

  **Commit**: YES (groups with T6)
  - Message: `feat(storage): RocksDB StorageEngine impl with feature gate`
  - Files: `crates/rr-storage/src/rocks.rs`
  - Pre-commit: `cargo test -p rr-storage --features rocks`

- [x] 8. CI/CD GitHub Actions Pipeline

  **What to do**:
  - Create `.github/workflows/ci.yml` with the following jobs:
    - **build**: `cargo build --workspace` (without rocksdb to verify default compilation)
    - **build-rocks**: `cargo build --workspace --features rocks` (with rocksdb)
    - **test**: `cargo test --workspace` (default features)
    - **test-rocks**: `cargo test --workspace --features rocks`
    - **clippy**: `cargo clippy --workspace -- -D warnings`
    - **fmt**: `cargo fmt --check`
  - Trigger on: push to main, pull requests
  - Use `actions/checkout@v4`, `dtolnay/rust-toolchain@stable`
  - Cache cargo registry and target directory with `actions/cache@v4`
  - Install system dependencies for RocksDB: `libclang-dev`, `cmake` on Ubuntu
  - Set `RUST_BACKTRACE=1` for test jobs
  - Matrix strategy for stable + nightly (nightly as allow-failures)

  **Must NOT do**:
  - Do not add deployment steps
  - Do not add release automation (out of scope)
  - Do not add Windows/macOS CI (Linux only for now)

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
    - Standard GitHub Actions setup

  **Parallelization**:
  - **Can Run In Parallel**: YES (with all Wave 1 tasks)
  - **Parallel Group**: Wave 1
  - **Blocks**: None
  - **Blocked By**: None

  **References**:
  **External References**:
  - GitHub Actions docs: `dtolnay/rust-toolchain`, `actions/cache`
  - RocksDB on CI: needs `libclang-dev` for bindgen, `cmake` for building

  **Acceptance Criteria**:
  - [x] `.github/workflows/ci.yml` exists
  - [x] Build, test, clippy, fmt jobs defined
  - [x] RocksDB system deps installed for rocks jobs
  - [x] Cargo cache configured

  **QA Scenarios:**
  ```
  Scenario: CI config is valid YAML
    Tool: Bash
    Preconditions: ci.yml created
    Steps:
      1. Run `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))" 2>&1`
      2. Verify no YAML parse errors
      3. Run `cat .github/workflows/ci.yml | head -5` to verify trigger config
    Expected Result: Valid YAML, triggers on push/PR
    Failure Indicators: YAML parse error
    Evidence: .sisyphus/evidence/task-8-ci-yaml.txt
  ```

  **Commit**: YES
  - Message: `ci: GitHub Actions build/test/clippy/fmt pipeline`
  - Files: `.github/workflows/ci.yml`
  - Pre-commit: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"`

---

### Wave 2 — Core Capabilities (after Wave 1)

---

- [x] 9. RocksDB-Backed RaftLogStorage (Persistent Raft Log)

  **What to do**:
  - Create `RocksLogStorage` in `crates/rr-raft/src/rocks_log_storage.rs` (gated with `#[cfg(feature = "rocks")]`)
  - Add `rocks` feature to `crates/rr-raft/Cargo.toml` that enables `rr-storage/rocks`
  - Implement openraft's `RaftLogStorage` trait using RocksDB via `StorageEngine`:
    - Use `raft_log` CF for log entries (key = log_index as u64 big-endian)
    - Use `metadata` CF for vote and last_purged_log_id
    - `save_vote(vote)`: serialize + put to metadata CF with key `"vote"`
    - `read_vote()`: get from metadata CF
    - `append(entries, callback)`: write_batch all entries to raft_log CF
    - `truncate(log_id)`: delete_range from log_id+1 to max
    - `purge(log_id)`: delete_range from 0 to log_id, save last_purged
    - `get_log_entries(range)`: scan raft_log CF for range
    - `get_log_id(position)`: handle First/Last by scan boundaries
  - Also implement `RaftStateMachine` trait's snapshot-related methods using `StorageEngine`:
    - `applied_state()`: read from metadata CF
    - `begin_receiving_snapshot()` + `install_snapshot()`: coordinate via StorageEngine
  - Write tests matching all `RrLogStorage` (in-memory) test patterns
  - Verify vote persistence survives restarts

  **Must NOT do**:
  - Do not remove the existing in-memory `RrLogStorage` (keep for tests/single-node)
  - Do not bypass StorageEngine trait (use it, don't call rocksdb directly)
  - Do not block tokio runtime with synchronous calls

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Complex trait implementation requiring understanding of openraft log semantics

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T10, T11, T12, T13)
  - **Parallel Group**: Wave 2
  - **Blocks**: T22
  - **Blocked By**: T1, T6

  **References**:
  **Pattern References**:
  - `crates/rr-raft/src/log_storage.rs` — Current in-memory RrLogStorage (replicate all behaviors)
  - `crates/rr-raft/src/sm_adapter.rs` — RrStateMachine handling state + snapshots

  **API/Type References**:
  - openraft `RaftLogStorage` trait — all methods to implement
  - openraft `RaftStateMachine` trait — snapshot-related methods
  - `crates/rr-core/src/storage.rs` — StorageEngine (use this for persistence)

  **Acceptance Criteria**:
  - [x] `RocksLogStorage` implements `RaftLogStorage` completely
  - [x] Vote persists across DB close/reopen
  - [x] Log entries persist across DB close/reopen
  - [x] `cargo test -p rr-raft --features rocks` — all tests pass

  **QA Scenarios:**
  ```
  Scenario: Vote survives restart
    Tool: Bash
    Preconditions: RocksLogStorage created
    Steps:
      1. Run `cargo test -p rr-raft --features rocks -- rocks_vote_persistence --nocapture 2>&1`
      2. Verify test writes vote, drops storage, reopens, reads vote back
    Expected Result: Vote matches after reopen
    Failure Indicators: Vote is None or different after reopen
    Evidence: .sisyphus/evidence/task-9-vote-persistence.txt

  Scenario: Log entries survive restart
    Tool: Bash
    Preconditions: RocksLogStorage created
    Steps:
      1. Run `cargo test -p rr-raft --features rocks -- rocks_log_persistence --nocapture 2>&1`
      2. Verify entries written, storage dropped, reopened, entries read back
    Expected Result: All entries present after reopen
    Evidence: .sisyphus/evidence/task-9-log-persistence.txt
  ```

  **Commit**: YES
  - Message: `feat(raft): RocksDB-backed RaftLogStorage`
  - Files: `crates/rr-raft/src/rocks_log_storage.rs`, `crates/rr-raft/Cargo.toml`, `crates/rr-raft/src/lib.rs`
  - Pre-commit: `cargo test -p rr-raft --features rocks`

- [x] 10. RocksDB Production Config Tuning

  **What to do**:
  - Create `RocksConfig` struct in `crates/rr-storage/src/rocks.rs` with configurable parameters:
    - `block_cache_size_mb: usize` (default 256)
    - `write_buffer_size_mb: usize` (default 64)
    - `max_write_buffer_number: i32` (default 4)
    - `max_background_jobs: i32` (default 4)
    - `compression: CompressionType` (default Lz4)
    - `level_zero_slowdown_trigger: i32` (default 20)
    - `level_zero_stop_trigger: i32` (default 36)
  - Update `RocksStorage::open()` to accept `RocksConfig`
  - Add `RocksConfig::default()` with production-ready defaults
  - Add `RocksConfig::test()` with minimal resources for tests
  - Write benchmark test comparing MemStorage vs RocksStorage for put/get/scan operations
  - Log RocksDB configuration at startup with tracing::info

  **Must NOT do**:
  - Do not expose raw rocksdb::Options to users
  - Do not add config to rr-core (keep in rr-storage)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T9, T11, T12, T13)
  - **Parallel Group**: Wave 2
  - **Blocks**: None
  - **Blocked By**: T7

  **References**:
  **External References**:
  - TiKV tuning: `level_zero_slowdown_writes_trigger(20)`, `level_zero_stop_writes_trigger(36)`, shared block cache
  - rocksdb v0.24.0: `Options`, `BlockBasedOptions`, `Cache::new_lru_cache`
  - Compression per level: L0-L1 No, L2-L4 Lz4, L5-L6 Zstd

  **Acceptance Criteria**:
  - [x] `RocksConfig` struct with builder pattern or defaults
  - [x] Production defaults match TiKV-style tuning
  - [x] `cargo test -p rr-storage --features rocks` — all tests pass

  **QA Scenarios:**
  ```
  Scenario: Config defaults produce working DB
    Tool: Bash
    Steps:
      1. Run `cargo test -p rr-storage --features rocks -- rocks_config --nocapture 2>&1`
      2. Verify default config creates functional RocksDB
    Expected Result: DB opens and operates with default config
    Evidence: .sisyphus/evidence/task-10-config-tuning.txt
  ```

  **Commit**: YES
  - Message: `perf(storage): RocksDB production config tuning`
  - Files: `crates/rr-storage/src/rocks.rs`
  - Pre-commit: `cargo test -p rr-storage --features rocks`

- [x] 11. Multi-Node Integration Test Harness

  **What to do**:
  - Extend `ClusterBuilder` in `tests/integration/src/lib.rs` to support real multi-node clusters:
    - Each node gets its own `RrNode` with gRPC server on unique port
    - Nodes connected via real `GrpcNetworkFactory` + `ChannelPool`
    - `RaftGroupLookupImpl` and `ClientDispatchImpl` wired in
    - Each node starts its own gRPC server
  - Add `ClusterBuilder::with_real_network()` method to opt into real networking (vs current in-process)
  - Implement port allocation: use `0` for OS-assigned ports, then discover actual ports
  - Add `wait_for_cluster_ready(nodes, group_id, timeout)` helper that waits for leader + all nodes connected
  - Add `send_client_write(node, group_id, payload)` helper for sending writes through gRPC client
  - Verify teardown: all gRPC servers shut down cleanly

  **Must NOT do**:
  - Do not break existing 4 integration tests (they use in-process mode)
  - Do not hardcode ports
  - Do not use sleep-based synchronization (use polling with timeout)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Complex multi-process test infrastructure

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T9, T10, T13)
  - **Parallel Group**: Wave 2
  - **Blocks**: T12, T24, T25
  - **Blocked By**: T5

  **References**:
  **Pattern References**:
  - `tests/integration/src/lib.rs` — Current ClusterBuilder (extend, don't replace)
  - `crates/rr-server/src/node.rs:111-150` — RrNode gRPC server wiring
  - `crates/rr-server/src/config.rs:49` — NodeConfig constructor with raft_lookup + client_dispatch

  **API/Type References**:
  - `RaftGroupLookupImpl` from T2, `ClientDispatchImpl` from T3
  - `crates/rr-network/src/channel_pool.rs` — ChannelPool for node-to-node connections

  **Acceptance Criteria**:
  - [x] `ClusterBuilder::with_real_network()` creates nodes with gRPC servers
  - [x] Existing 4 integration tests still pass
  - [x] Port allocation is dynamic (no hardcoded ports)
  - [x] Clean shutdown verified

  **QA Scenarios:**
  ```
  Scenario: Multi-node harness starts and stops
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration -- harness --nocapture 2>&1`
      2. Verify 3 nodes start with gRPC servers
      3. Verify clean shutdown (no leaked tasks)
    Expected Result: Harness creates and tears down 3-node cluster
    Failure Indicators: Port bind errors, timeout on shutdown
    Evidence: .sisyphus/evidence/task-11-multinode-harness.txt

  Scenario: Existing tests still pass
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration -- --nocapture 2>&1`
      2. Verify all 4 existing tests pass
    Expected Result: No regressions
    Evidence: .sisyphus/evidence/task-11-regression.txt
  ```

  **Commit**: YES (groups with T12)
  - Message: `test(integration): multi-node cluster tests`
  - Files: `tests/integration/src/lib.rs`, `tests/integration/Cargo.toml`
  - Pre-commit: `cargo test -p integration`

- [x] 12. Three-Node Leader Election + Write Replication Tests

  **What to do**:
  - Using the multi-node harness from T11, write integration tests:
    1. `test_3node_leader_election`: Start 3 nodes, initialize Raft group, verify exactly 1 leader elected
    2. `test_3node_write_replication`: Write through leader, verify all 3 nodes have the data
    3. `test_3node_write_to_follower_redirect`: Write to follower, verify redirect info returned
    4. `test_3node_leader_failover`: Kill leader, verify new leader elected within timeout
  - Each test uses `ClusterBuilder::with_real_network().build().await`
  - Use `wait_for_cluster_ready()` to ensure cluster is stable before testing
  - Assert concrete values (leader_id, data content), not just "something happened"

  **Must NOT do**:
  - Do not use `sleep()` for synchronization — use polling with timeout
  - Do not use `assert!(true)` placeholder assertions

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Distributed systems testing with race conditions

  **Parallelization**:
  - **Can Run In Parallel**: NO (depends on T11)
  - **Parallel Group**: Wave 2 (after T11)
  - **Blocks**: T14, T23
  - **Blocked By**: T11

  **References**:
  **Pattern References**:
  - `tests/integration/src/lib.rs` — Existing `wait_for_leader()` and test patterns
  - `crates/rr-raft/src/raft_node.rs` — RaftNode API for client_write

  **Acceptance Criteria**:
  - [x] 4 new multi-node tests pass
  - [x] Leader election verified across real gRPC
  - [x] Write replication verified on all 3 nodes
  - [x] Follower redirect returns valid leader info

  **QA Scenarios:**
  ```
  Scenario: 3-node cluster elects leader
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration -- test_3node --nocapture 2>&1`
      2. Verify output shows leader elected (node_id logged)
      3. Verify all 4 new tests pass
    Expected Result: Leader election, replication, redirect, and failover all work
    Failure Indicators: Timeout waiting for leader, data mismatch on followers
    Evidence: .sisyphus/evidence/task-12-3node-tests.txt
  ```

  **Commit**: YES (groups with T11)
  - Message: `test(integration): multi-node cluster tests`
  - Files: `tests/integration/src/lib.rs`
  - Pre-commit: `cargo test -p integration`

- [x] 13. Extend RoutingTableChange with Node/Membership Events

  **What to do**:
  - Add new variants to `RoutingTableChange` enum in `crates/rr-core/src/config.rs`:
    - `NodeAdded { node_id: NodeId, addr: NodeAddr }`
    - `NodeRemoved { node_id: NodeId }`
    - `GroupMembershipChanged { group_id: GroupId, replicas: Vec<NodeId> }`
  - Update `MemConfigProvider` in `crates/rr-config/src/mem.rs`:
    - `add_node()` now broadcasts `NodeAdded` event
    - Add `remove_node(node_id)` method that broadcasts `NodeRemoved`
    - Add `update_group_membership(group_id, replicas)` method
  - Update existing tests to handle new variants
  - Write tests for new events

  **Must NOT do**:
  - Do not modify ShardRouter (it doesn't need changes)
  - Do not break existing MemConfigProvider tests
  - Do not add complex event ordering guarantees

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []
    - Simple enum extension + broadcast additions

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T9, T10, T11, T12)
  - **Parallel Group**: Wave 2
  - **Blocks**: T14, T16, T21
  - **Blocked By**: None

  **References**:
  **Pattern References**:
  - `crates/rr-core/src/config.rs` — Current RoutingTableChange enum (PartitionAdded/Removed/Updated)
  - `crates/rr-config/src/mem.rs` — MemConfigProvider with broadcast channel

  **Acceptance Criteria**:
  - [x] 3 new variants added to RoutingTableChange
  - [x] `add_node()` broadcasts NodeAdded
  - [x] `remove_node()` broadcasts NodeRemoved
  - [x] `update_group_membership()` broadcasts GroupMembershipChanged
  - [x] Existing config tests still pass
  - [x] `cargo test -p rr-config` — all tests pass

  **QA Scenarios:**
  ```
  Scenario: New events broadcast correctly
    Tool: Bash
    Steps:
      1. Run `cargo test -p rr-config -- --nocapture 2>&1`
      2. Verify tests for NodeAdded, NodeRemoved, GroupMembershipChanged events
    Expected Result: All config tests pass including new event tests
    Evidence: .sisyphus/evidence/task-13-config-events.txt
  ```

  **Commit**: YES
  - Message: `feat(core): extend RoutingTableChange events`
  - Files: `crates/rr-core/src/config.rs`, `crates/rr-config/src/mem.rs`
  - Pre-commit: `cargo test -p rr-core -p rr-config`

---

### Wave 3 — Dynamic Config + Snapshots (after Wave 2)

---

- [x] 14. RrNode Config Watch Loop

  **What to do**:
  - Add a background task in `RrNode::start()` that subscribes to `config_provider.watch_changes()`
  - React to events:
    - `PartitionAdded { group_id, .. }`: if this node is in replicas, call `group_manager.start_group(group_id)`
    - `PartitionRemoved { partition_id }`: find associated group, stop if no partitions remain
    - `NodeAdded { node_id, addr }`: register in channel pool for future connections
    - `NodeRemoved { node_id }`: remove from channel pool
    - `GroupMembershipChanged { group_id, replicas }`: if node added/removed from group, start/stop accordingly
  - Track the watch task's JoinHandle for clean shutdown
  - Log all config changes with tracing::info
  - Write tests: start RrNode, add partition via config provider, verify group starts

  **Must NOT do**:
  - Do not implement Raft membership changes here (that's T15)
  - Do not cache the routing table in ShardRouter (it's already dynamic)
  - Do not leak the watch task on shutdown

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Event loop with multiple event types and async coordination

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T15, T17, T18)
  - **Parallel Group**: Wave 3
  - **Blocks**: T16, T21
  - **Blocked By**: T12, T13

  **References**:
  **Pattern References**:
  - `crates/rr-server/src/node.rs:80-110` — Current RrNode::start() (extend with watch loop)
  - `crates/rr-config/src/mem.rs` — watch_changes() returns Stream<RoutingTableChange>
  - `crates/rr-server/src/node.rs:157-187` — Shutdown flow (add watch task cancellation)

  **API/Type References**:
  - `crates/rr-core/src/config.rs` — RoutingTableChange variants (all, including T13 additions)
  - `crates/rr-raft/src/group_manager.rs` — start_group/stop_group

  **Acceptance Criteria**:
  - [x] Watch loop starts with RrNode
  - [x] PartitionAdded triggers group start
  - [x] PartitionRemoved triggers group stop
  - [x] Watch task cancelled on shutdown
  - [x] `cargo test -p rr-server` — all tests pass

  **QA Scenarios:**
  ```
  Scenario: Dynamic partition triggers group start
    Tool: Bash
    Steps:
      1. Run `cargo test -p rr-server -- watch_loop --nocapture 2>&1`
      2. Verify test: start node, add partition via config, verify group started
    Expected Result: Group auto-started on config change
    Failure Indicators: Group not found after partition added
    Evidence: .sisyphus/evidence/task-14-watch-loop.txt
  ```

  **Commit**: YES
  - Message: `feat(server): RrNode config watch loop`
  - Files: `crates/rr-server/src/node.rs`
  - Pre-commit: `cargo test -p rr-server`

- [x] 15. Expose openraft Membership APIs

  **What to do**:
  - Add methods to `RaftNode`:
    - `add_learner(node_id: NodeId, addr: String) -> Result<(), RrError>` — wraps `raft.add_learner(node_id, BasicNode { addr }, true)`
    - `change_membership(members: BTreeSet<NodeId>) -> Result<(), RrError>` — wraps `raft.change_membership(members, false)`
    - `get_membership() -> Result<Membership, RrError>` — returns current membership
  - Add corresponding methods to `RaftGroupManager`:
    - `add_learner_to_group(group_id, node_id, addr)`
    - `change_group_membership(group_id, members)`
    - `get_group_membership(group_id)`
  - Error mapping: openraft errors → RrError variants
  - Write tests verifying membership changes on single-node cluster

  **Must NOT do**:
  - Do not auto-trigger membership changes from config events (that's T16/T21)
  - Do not add new gRPC endpoints for membership (use existing ClientDispatch)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Requires understanding openraft membership semantics

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T14, T17, T18)
  - **Parallel Group**: Wave 3
  - **Blocks**: T16, T21
  - **Blocked By**: None

  **References**:
  **Pattern References**:
  - `crates/rr-raft/src/raft_node.rs:40-63` — RaftNode wrapping openraft::Raft
  - `crates/rr-raft/src/raft_node.rs:65-85` — init_single_node() pattern to follow

  **API/Type References**:
  - openraft `Raft::add_learner(node_id, node, blocking)` — add non-voting member
  - openraft `Raft::change_membership(members, retain)` — change voter set
  - `BTreeSet<NodeId>` — membership is a set of node IDs
  - `BasicNode { addr: String }` — node descriptor

  **Acceptance Criteria**:
  - [x] `add_learner`, `change_membership`, `get_membership` methods exist on RaftNode
  - [x] Same methods exist on RaftGroupManager (delegating to correct group)
  - [x] `cargo test -p rr-raft` — all tests pass

  **QA Scenarios:**
  ```
  Scenario: Membership change API works
    Tool: Bash
    Steps:
      1. Run `cargo test -p rr-raft -- membership --nocapture 2>&1`
      2. Verify add_learner and change_membership work on single node
    Expected Result: Membership APIs return Ok
    Evidence: .sisyphus/evidence/task-15-membership-api.txt
  ```

  **Commit**: YES (groups with T16)
  - Message: `feat(raft): dynamic membership + partition add`
  - Files: `crates/rr-raft/src/raft_node.rs`, `crates/rr-raft/src/group_manager.rs`
  - Pre-commit: `cargo test -p rr-raft`

- [x] 16. Dynamic Partition Add at Runtime

  **What to do**:
  - Integrate T14 (watch loop) + T15 (membership APIs) for full dynamic partition flow:
    1. Admin adds partition via ConfigProvider (e.g., `mem_config.add_partition(...)`) 
    2. Watch loop detects `PartitionAdded` event
    3. RrNode checks if it's in the replica set
    4. If yes: starts Raft group, joins existing cluster as learner, then promotes to voter
  - Implement the join flow in RrNode:
    - New node: `group_manager.start_group(group_id)` → connect to leader → `add_learner()` on leader
    - First node: `group_manager.start_group(group_id)` → `init_cluster()`
  - Write integration test: start 3-node cluster, add 4th partition at runtime, verify it replicates

  **Must NOT do**:
  - Do not implement partition migration (moving partition between nodes — that's T24)
  - Do not auto-rebalance

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Full integration of config + membership + networking

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 3 (after T14 + T15)
  - **Blocks**: T24
  - **Blocked By**: T14, T15

  **References**:
  **Pattern References**:
  - T14 watch loop (RrNode reacting to config changes)
  - T15 membership APIs (add_learner, change_membership)
  - `tests/integration/src/lib.rs` — ClusterBuilder for test setup

  **Acceptance Criteria**:
  - [x] Partition can be added at runtime via ConfigProvider
  - [x] New node joins cluster as learner, then promotes
  - [x] Data replicates to new partition
  - [x] Integration test passes

  **QA Scenarios:**
  ```
  Scenario: Runtime partition add with replication
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration -- dynamic_partition_add --nocapture 2>&1`
      2. Verify: start 3-node cluster, add partition via config, new group starts, data replicates
    Expected Result: New partition operational and replicating
    Failure Indicators: Group not started, replication timeout
    Evidence: .sisyphus/evidence/task-16-dynamic-partition.txt
  ```

  **Commit**: YES (groups with T15)
  - Message: `feat(raft): dynamic membership + partition add`
  - Files: `crates/rr-server/src/node.rs`, `tests/integration/src/lib.rs`
  - Pre-commit: `cargo test -p integration`

- [x] 17. Complete install_snapshot Flow

  **What to do**:
  - Implement full snapshot transfer in `RrStateMachine` (rr-raft):
    - `begin_receiving_snapshot()`: create temp directory for incoming snapshot
    - `install_snapshot()`: apply received snapshot data to state machine, update applied state
    - `get_snapshot_builder()`: return builder that can serialize current state
  - The snapshot data format: bincode-serialized state machine state as `Vec<u8>`
  - Verify the `install_snapshot` RPC path works end-to-end:
    - Leader builds snapshot → sends via streaming gRPC → follower receives and installs
  - Uses the fixed proto from T4 (SnapshotResponse with payload field)
  - Write tests for snapshot creation, transfer, and installation

  **Must NOT do**:
  - Do not implement file-based snapshot transfer yet (use in-memory Vec<u8> for now)
  - Do not implement snapshot compaction/cleanup

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - openraft snapshot lifecycle is complex

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T14, T15, T16)
  - **Parallel Group**: Wave 3
  - **Blocks**: T18, T26
  - **Blocked By**: T4

  **References**:
  **Pattern References**:
  - `crates/rr-raft/src/sm_adapter.rs` — RrStateMachine (snapshot methods to implement/fix)
  - `crates/rr-raft/src/network_factory.rs:357-401` — install_snapshot RPC client (fixed in T4)
  - `crates/rr-network/src/raft_service.rs:112-159` — install_snapshot RPC server (streaming)

  **API/Type References**:
  - openraft `RaftStateMachine::begin_receiving_snapshot()` — returns Box<SnapshotData>
  - openraft `RaftStateMachine::install_snapshot()` — applies received snapshot
  - openraft `RaftStateMachine::get_snapshot_builder()` — for snapshot creation

  **Acceptance Criteria**:
  - [x] Snapshot creation serializes state machine state
  - [x] Snapshot installation restores state machine state
  - [x] End-to-end: leader snapshot → gRPC stream → follower install
  - [x] `cargo test -p rr-raft` — snapshot tests pass

  **QA Scenarios:**
  ```
  Scenario: Snapshot round-trip
    Tool: Bash
    Steps:
      1. Run `cargo test -p rr-raft -- snapshot_transfer --nocapture 2>&1`
      2. Verify snapshot created, transferred, installed
    Expected Result: State machine state matches after snapshot install
    Evidence: .sisyphus/evidence/task-17-snapshot-flow.txt
  ```

  **Commit**: YES (groups with T18)
  - Message: `feat(raft): complete snapshot transfer flow`
  - Files: `crates/rr-raft/src/sm_adapter.rs`
  - Pre-commit: `cargo test -p rr-raft`

- [x] 18. Snapshot Streaming with Chunked Transfer

  **What to do**:
  - Implement chunked snapshot transfer for large snapshots:
    - Leader side: split snapshot `Vec<u8>` into configurable chunks (default 64KB)
    - Stream chunks via `InstallSnapshot` gRPC streaming RPC
    - Each chunk includes: `offset`, `data`, `done` flag, `vote_payload`
  - Follower side: receive chunks, reassemble into full snapshot
  - Add backpressure: if follower can't keep up, slow sender
  - Update `SnapshotChunk` proto if needed for offset/done fields
  - Write test: transfer a snapshot larger than one chunk size

  **Must NOT do**:
  - Do not implement file-based streaming (keep in-memory for now)
  - Do not add compression (out of scope)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Streaming gRPC with backpressure

  **Parallelization**:
  - **Can Run In Parallel**: NO (depends on T17)
  - **Parallel Group**: Wave 3 (after T17)
  - **Blocks**: T26
  - **Blocked By**: T17

  **References**:
  **Pattern References**:
  - `crates/rr-network/proto/raft.proto` — SnapshotChunk message (check current fields)
  - `crates/rr-raft/src/network_factory.rs:357-401` — install_snapshot client streaming
  - `crates/rr-network/src/raft_service.rs:112-159` — install_snapshot server receiving

  **Acceptance Criteria**:
  - [x] Large snapshots chunked into 64KB pieces
  - [x] Chunks reassembled correctly on receiver
  - [x] `cargo test -p rr-raft` — chunked transfer test passes

  **QA Scenarios:**
  ```
  Scenario: Large snapshot transfers in chunks
    Tool: Bash
    Steps:
      1. Run `cargo test -p rr-raft -- chunked_snapshot --nocapture 2>&1`
      2. Verify snapshot > 64KB is split into multiple chunks
    Expected Result: All chunks received, snapshot reassembled correctly
    Evidence: .sisyphus/evidence/task-18-chunked-snapshot.txt
  ```

  **Commit**: YES (groups with T17)
  - Message: `feat(raft): complete snapshot transfer flow`
  - Files: `crates/rr-raft/src/network_factory.rs`, `crates/rr-network/src/raft_service.rs`
  - Pre-commit: `cargo test -p rr-raft`

---

### Wave 4 — Client SDK + Advanced Dynamic (after Wave 3)

---

- [x] 19. rr-client Crate Scaffolding + gRPC Client

  **What to do**:
  - Create new crate `crates/rr-client/` with `Cargo.toml` and `src/lib.rs`
  - Add to workspace `[members]` in root `Cargo.toml`
  - Dependencies: `tonic`, `rr-core`, `tokio`
  - Create `RrClient` struct with:
    - `bootstrap_addrs: Vec<String>` — initial node addresses to connect to
    - `channel_pool: ChannelPool` — reuse connections to nodes
    - Internal `leader_cache: HashMap<GroupId, NodeId>` for leader affinity
  - Implement basic methods:
    - `RrClient::connect(addrs: Vec<String>) -> Result<Self, RrError>`
    - `send_request(group_id: GroupId, payload: Vec<u8>) -> Result<Vec<u8>, RrError>` — basic send without retry/redirect yet
    - `get_cluster_info() -> Result<ClusterInfo, RrError>` — calls GetClusterInfo RPC
  - Generate gRPC client from `client.proto` (reuse existing proto)
  - Write tests: connect to mock server, send request, get cluster info

  **Must NOT do**:
  - Do not implement leader redirect (that's T20)
  - Do not implement retry logic (that's T20)
  - Do not duplicate proto definitions (reuse from rr-network)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T20 prep, T21, T22)
  - **Parallel Group**: Wave 4
  - **Blocks**: T20, T23
  - **Blocked By**: None

  **References**:
  **Pattern References**:
  - `crates/rr-network/src/channel_pool.rs` — ChannelPool pattern to reuse
  - `crates/rr-network/proto/client.proto` — ClientService RPC definitions
  - `crates/rr-network/src/client_service.rs` — Server-side impl (understand the contract)

  **API/Type References**:
  - tonic generated client: `client_service_client::ClientServiceClient`
  - `crates/rr-core/src/types.rs` — GroupId, NodeId, NodeAddr

  **Acceptance Criteria**:
  - [x] `crates/rr-client/` crate exists and compiles
  - [x] `RrClient::connect()` establishes connection
  - [x] `send_request()` sends payload via gRPC
  - [x] `get_cluster_info()` returns cluster state
  - [x] `cargo test -p rr-client` — all tests pass

  **QA Scenarios:**
  ```
  Scenario: Client crate builds and tests pass
    Tool: Bash
    Steps:
      1. Run `cargo build -p rr-client 2>&1`
      2. Run `cargo test -p rr-client -- --nocapture 2>&1`
    Expected Result: Builds and tests pass
    Evidence: .sisyphus/evidence/task-19-client-scaffold.txt
  ```

  **Commit**: YES (groups with T20)
  - Message: `feat(client): rr-client SDK with leader redirect`
  - Files: `crates/rr-client/Cargo.toml`, `crates/rr-client/src/lib.rs`
  - Pre-commit: `cargo test -p rr-client`

- [x] 20. Client Auto Leader-Redirect + Retry

  **What to do**:
  - Enhance `RrClient::send_request()` with leader redirect logic:
    1. Send request to cached leader (or first bootstrap node)
    2. If response contains `LeaderInfo` (redirect): update cache, retry to indicated leader
    3. If connection error: try next bootstrap node, update cache
    4. Max retry limit: 3 attempts before returning error
  - Add `RetryConfig` struct: `max_retries: u32` (default 3), `retry_delay_ms: u64` (default 100)
  - Implement exponential backoff for retries
  - Add `send_request_with_partition(partition_id, payload)`: uses ShardRouter-like logic to find group_id from partition_id
  - Thread-safe leader cache: `Arc<RwLock<HashMap<GroupId, (NodeId, String)>>>`
  - Write tests:
    - Successful write on first try
    - Redirect from follower to leader
    - Connection failure with retry to another node
    - Max retries exceeded error

  **Must NOT do**:
  - Do not implement client-side sharding/routing (server handles this)
  - Do not implement request batching

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
    - Retry logic with state management and error handling

  **Parallelization**:
  - **Can Run In Parallel**: NO (depends on T19)
  - **Parallel Group**: Wave 4 (after T19)
  - **Blocks**: T23
  - **Blocked By**: T3, T19

  **References**:
  **Pattern References**:
  - `crates/rr-network/src/client_service.rs:84-87` — LeaderInfo in response
  - T3 ClientDispatchImpl — how leader info is populated on server side

  **API/Type References**:
  - `crates/rr-network/proto/client.proto` — ClientResponse with leader_info field
  - tonic `Status::code()` for connection error detection

  **Acceptance Criteria**:
  - [x] Leader redirect works: follower → leader → success
  - [x] Connection failure triggers retry to next node
  - [x] Max retries exceeded returns meaningful error
  - [x] Leader cache updated on redirect
  - [x] `cargo test -p rr-client` — all tests pass

  **QA Scenarios:**
  ```
  Scenario: Redirect from follower to leader
    Tool: Bash
    Steps:
      1. Run `cargo test -p rr-client -- redirect --nocapture 2>&1`
      2. Verify client follows redirect and succeeds
    Expected Result: Write succeeds after redirect
    Evidence: .sisyphus/evidence/task-20-client-redirect.txt

  Scenario: Max retries exceeded
    Tool: Bash
    Steps:
      1. Run `cargo test -p rr-client -- max_retries --nocapture 2>&1`
      2. Verify error after 3 failed attempts
    Expected Result: Error returned with retry count
    Evidence: .sisyphus/evidence/task-20-max-retries.txt
  ```

  **Commit**: YES (groups with T19)
  - Message: `feat(client): rr-client SDK with leader redirect`
  - Files: `crates/rr-client/src/lib.rs`
  - Pre-commit: `cargo test -p rr-client`

- [x] 21. Dynamic Node Add/Remove at Runtime

  **What to do**:
  - Implement the full node lifecycle:
    - **Add node**: new RrNode starts → joins existing cluster → starts Raft groups as learner → catches up → promoted to voter
    - **Remove node**: drain leader transfers → remove from membership → stop Raft groups → shutdown
  - In `RrNode`, handle `NodeAdded`/`NodeRemoved` events from watch loop (T14)
  - For join: use `RaftGroupManager::add_learner_to_group()` from T15
  - For leave: use `RaftGroupManager::change_group_membership()` to remove self from voter set
  - Write integration test: start 3-node cluster, add 4th node, verify it replicates, remove a node, verify cluster continues

  **Must NOT do**:
  - Do not implement automatic rebalancing
  - Do not handle split-brain scenarios (Raft handles this)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T19, T22)
  - **Parallel Group**: Wave 4
  - **Blocks**: T25
  - **Blocked By**: T14, T15

  **References**:
  **Pattern References**:
  - T14 watch loop — reacting to NodeAdded/NodeRemoved events
  - T15 membership APIs — add_learner, change_membership
  - `tests/integration/src/lib.rs` — ClusterBuilder

  **Acceptance Criteria**:
  - [x] New node can join existing cluster
  - [x] Node removal drains and leaves gracefully
  - [x] Integration test passes with 4th node join + removal

  **QA Scenarios:**
  ```
  Scenario: Node joins and leaves cluster
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration -- dynamic_node --nocapture 2>&1`
      2. Verify 4th node joins, replicates, then leaves cleanly
    Expected Result: Cluster operates correctly through join/leave
    Evidence: .sisyphus/evidence/task-21-dynamic-node.txt
  ```

  **Commit**: YES
  - Message: `feat(server): dynamic node add/remove`
  - Files: `crates/rr-server/src/node.rs`, `tests/integration/src/lib.rs`
  - Pre-commit: `cargo test -p integration`

- [x] 22. Node Restart with RocksDB Persistence Test

  **What to do**:
  - Write integration test verifying data persistence across node restarts:
    1. Start 3-node cluster with RocksDB storage (feature-gated test)
    2. Write data via client
    3. Stop a node (graceful shutdown)
    4. Restart the node with same RocksDB path
    5. Verify: node rejoins cluster, data is present from disk (not just replication)
    6. Verify: vote persisted (no split-brain risk)
  - Use `#[cfg(feature = "rocks")]` for test gating
  - Verify Raft log entries survive restart
  - Verify state machine state survives restart

  **Must NOT do**:
  - Do not test with MemStorage (that's the existing tests)
  - Do not implement snapshotting for faster restart (that's T17/T26)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T19, T20, T21)
  - **Parallel Group**: Wave 4
  - **Blocks**: T25
  - **Blocked By**: T7, T9, T11

  **References**:
  **Pattern References**:
  - T7 RocksStorage, T9 RocksLogStorage — persistent storage implementations
  - T11 multi-node harness — ClusterBuilder with real networking

  **Acceptance Criteria**:
  - [x] Node restarts and rejoins cluster
  - [x] Data from before restart is present on disk
  - [x] Vote persists (same term/candidate after restart)
  - [x] Test passes: `cargo test -p integration --features rocks -- restart_persistence`

  **QA Scenarios:**
  ```
  Scenario: Data survives node restart
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration --features rocks -- restart_persistence --nocapture 2>&1`
      2. Verify data written before restart is readable after restart
    Expected Result: Data persists, node rejoins cluster
    Evidence: .sisyphus/evidence/task-22-restart-persistence.txt
  ```

  **Commit**: YES
  - Message: `test(integration): node restart persistence`
  - Files: `tests/integration/src/lib.rs`, `tests/integration/Cargo.toml`
  - Pre-commit: `cargo test -p integration --features rocks`

---

### Wave 5 — Integration + Hardening (after Wave 4)

---

- [x] 23. Client SDK Integration Tests

  **What to do**:
  - Write integration tests using `RrClient` against a real 3-node cluster:
    1. `test_client_write_read`: connect via SDK, write KV pair, read back
    2. `test_client_redirect`: connect to follower, verify auto-redirect to leader
    3. `test_client_leader_failover`: kill leader, verify client retries to new leader
    4. `test_client_multiple_groups`: write to different groups via SDK
  - Use ClusterBuilder from T11 for test setup
  - Client connects to bootstrap addresses, discovers leader, writes

  **Must NOT do**:
  - Do not test client internals (unit tests cover that in T19/T20)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T24, T25, T26)
  - **Parallel Group**: Wave 5
  - **Blocks**: T28
  - **Blocked By**: T12, T20

  **References**:
  - T19/T20 RrClient SDK
  - T11/T12 multi-node cluster harness

  **Acceptance Criteria**:
  - [x] 4 integration tests pass
  - [x] Client auto-discovers leader
  - [x] Client survives leader failover

  **QA Scenarios:**
  ```
  Scenario: Client SDK works end-to-end
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration -- test_client --nocapture 2>&1`
      2. Verify all 4 client integration tests pass
    Expected Result: Client SDK operates correctly against real cluster
    Evidence: .sisyphus/evidence/task-23-client-integration.txt
  ```

  **Commit**: YES (groups with T24-T26)
  - Message: `test(integration): comprehensive cluster tests`
  - Files: `tests/integration/src/lib.rs`
  - Pre-commit: `cargo test -p integration`

- [x] 24. Partition Migration Test

  **What to do**:
  - Write integration test for partition migration:
    1. Start 3-node cluster with partition P1 on nodes {1,2,3}
    2. Add node 4
    3. Move P1 to {1,2,4} via membership change
    4. Verify node 4 catches up via snapshot or log replication
    5. Verify node 3 leaves the group
    6. Write to P1, verify replication to new set {1,2,4}

  **Must NOT do**:
  - Do not implement automatic rebalancing logic

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T23, T25, T26)
  - **Parallel Group**: Wave 5
  - **Blocks**: T28
  - **Blocked By**: T11, T16

  **References**:
  - T16 dynamic partition add
  - T15 membership APIs
  - T11 multi-node harness

  **Acceptance Criteria**:
  - [x] Partition migration test passes
  - [x] Data available on new replica set after migration

  **QA Scenarios:**
  ```
  Scenario: Partition moves between nodes
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration -- partition_migration --nocapture 2>&1`
    Expected Result: Partition migrates, data replicates to new set
    Evidence: .sisyphus/evidence/task-24-partition-migration.txt
  ```

  **Commit**: YES (groups with T23, T25, T26)
  - Message: `test(integration): comprehensive cluster tests`
  - Files: `tests/integration/src/lib.rs`
  - Pre-commit: `cargo test -p integration`

- [x] 25. Node Failure + Recovery Test

  **What to do**:
  - Write integration test for node failure scenarios:
    1. Start 3-node cluster, write data
    2. Kill node 2 (simulate crash — drop without graceful shutdown)
    3. Verify cluster continues (nodes 1,3 form majority)
    4. Write more data
    5. Restart node 2 (with RocksDB persistence)
    6. Verify node 2 catches up (via log replication or snapshot)
    7. Verify node 2 has all data (pre-crash + post-crash)

  **Must NOT do**:
  - Do not implement network partitioning tests (out of scope)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T23, T24, T26)
  - **Parallel Group**: Wave 5
  - **Blocks**: T28
  - **Blocked By**: T11, T21, T22

  **References**:
  - T22 RocksDB persistence test
  - T21 dynamic node management
  - T11 multi-node harness

  **Acceptance Criteria**:
  - [x] Cluster survives node crash
  - [x] Recovered node catches up with all data
  - [x] Test passes with RocksDB persistence

  **QA Scenarios:**
  ```
  Scenario: Node crashes and recovers
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration --features rocks -- node_failure_recovery --nocapture 2>&1`
    Expected Result: Node recovers and catches up
    Evidence: .sisyphus/evidence/task-25-node-recovery.txt
  ```

  **Commit**: YES (groups with T23, T24, T26)
  - Message: `test(integration): comprehensive cluster tests`
  - Files: `tests/integration/src/lib.rs`
  - Pre-commit: `cargo test -p integration --features rocks`

- [x] 26. Snapshot-Based Node Catch-Up Test

  **What to do**:
  - Write integration test for snapshot-based catch-up:
    1. Start 3-node cluster, write enough data to trigger snapshot compaction
    2. Compact logs on leader (purge old entries)
    3. Add new node 4 or restart a node with empty state
    4. Verify: new node receives snapshot (not log entries, since they're purged)
    5. Verify: new node has all data after snapshot install
    6. Verify: new node can serve reads after catching up

  **Must NOT do**:
  - Do not implement automatic compaction policies

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T23, T24, T25)
  - **Parallel Group**: Wave 5
  - **Blocks**: T28
  - **Blocked By**: T11, T18

  **References**:
  - T17/T18 snapshot transfer flow
  - T11 multi-node harness
  - openraft `Raft::trigger().purge_log()` — trigger log compaction

  **Acceptance Criteria**:
  - [x] New node catches up via snapshot (not log replay)
  - [x] All data present after snapshot install
  - [x] Test passes

  **QA Scenarios:**
  ```
  Scenario: Node catches up via snapshot
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration -- snapshot_catchup --nocapture 2>&1`
    Expected Result: Node receives snapshot and has all data
    Evidence: .sisyphus/evidence/task-26-snapshot-catchup.txt
  ```

  **Commit**: YES (groups with T23, T24, T25)
  - Message: `test(integration): comprehensive cluster tests`
  - Files: `tests/integration/src/lib.rs`
  - Pre-commit: `cargo test -p integration`

---

### Wave 6 — Polish (after Wave 5)

- [x] 27. Update KV Store Example for Multi-Node

  **What to do**:
  - Update `examples/kv-store/` to demonstrate a multi-node cluster:
    - Accept CLI args: `--node-id`, `--port`, `--peers` (comma-separated peer addresses)
    - Start RrNode with gRPC server, RaftGroupLookupImpl, ClientDispatchImpl
    - Bootstrap cluster if `--bootstrap` flag present
    - Join existing cluster if `--join <leader-addr>` flag present
  - Update `README` comments in `main.rs` with usage instructions
  - Verify example compiles and runs (single-node and multi-node modes)

  **Must NOT do**:
  - Do not add RocksDB to the example (keep it simple with MemStorage)
  - Do not add complex CLI framework (use simple arg parsing)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T28 prep)
  - **Parallel Group**: Wave 6
  - **Blocks**: T28
  - **Blocked By**: T12, T20

  **References**:
  - `examples/kv-store/src/main.rs` — Current single-node example
  - T2 RaftGroupLookupImpl, T3 ClientDispatchImpl

  **Acceptance Criteria**:
  - [x] Example compiles: `cargo build --example kv-store` or `cargo build -p kv-store`
  - [x] Single-node mode still works
  - [x] CLI args for multi-node documented

  **QA Scenarios:**
  ```
  Scenario: KV store example builds
    Tool: Bash
    Steps:
      1. Run `cargo build -p kv-store 2>&1`
      2. Verify no compilation errors
    Expected Result: Example builds successfully
    Evidence: .sisyphus/evidence/task-27-kv-example.txt
  ```

  **Commit**: YES
  - Message: `feat(example): multi-node KV store`
  - Files: `examples/kv-store/src/main.rs`
  - Pre-commit: `cargo build -p kv-store`

- [x] 28. End-to-End Smoke Test (Full Cluster Lifecycle)

  **What to do**:
  - Write comprehensive integration test covering the full lifecycle:
    1. Start 3-node cluster with RocksDB
    2. Verify leader election
    3. Write 100 KV pairs through client SDK
    4. Read back all 100 pairs from each node
    5. Add partition at runtime
    6. Write to new partition
    7. Kill a node
    8. Write more data (cluster should continue)
    9. Restart killed node
    10. Verify recovered node has all data
    11. Verify all nodes consistent
    12. Graceful shutdown all nodes
  - This is the "does everything work together" test
  - Timeout: 60 seconds total

  **Must NOT do**:
  - Do not add stress testing (this is a smoke test, not a load test)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO (final integration test)
  - **Parallel Group**: Wave 6 (after all other tasks)
  - **Blocks**: FINAL
  - **Blocked By**: T23, T24, T25, T26, T27

  **References**:
  - All previous tasks — this test exercises everything

  **Acceptance Criteria**:
  - [x] Full lifecycle test passes within 60 seconds
  - [x] All 12 steps verified with assertions
  - [x] Test passes with RocksDB feature enabled

  **QA Scenarios:**
  ```
  Scenario: Full cluster lifecycle
    Tool: Bash
    Steps:
      1. Run `cargo test -p integration --features rocks -- e2e_smoke --nocapture 2>&1`
      2. Verify all 12 lifecycle steps pass
    Expected Result: Complete lifecycle works end-to-end
    Failure Indicators: Timeout (>60s), data inconsistency, crash
    Evidence: .sisyphus/evidence/task-28-e2e-smoke.txt
  ```

  **Commit**: YES
  - Message: `test(integration): end-to-end smoke test`
  - Files: `tests/integration/src/lib.rs`
  - Pre-commit: `cargo test -p integration --features rocks`

---

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Rejection → fix → re-run.

- [x] F1. **Plan Compliance Audit** — `oracle`
  Read the plan end-to-end. For each "Must Have": verify implementation exists (read file, run test, cargo check). For each "Must NOT Have": search codebase for forbidden patterns (anyhow in lib, println in lib, circular deps, placeholder asserts). Check evidence files exist in `.sisyphus/evidence/`. Compare deliverables against plan.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [N/N] | VERDICT: APPROVE/REJECT`

- [x] F2. **Code Quality Review** — `unspecified-high`
  Run `cargo clippy --workspace -- -D warnings` + `cargo fmt --check` + `cargo test --workspace`. Review all changed files for: `as any` casts, empty catches, `unwrap()` in production paths, `todo!()`/`unimplemented!()`, unused imports. Check AI slop: excessive comments, over-abstraction, generic names.
  Output: `Build [PASS/FAIL] | Clippy [PASS/FAIL] | Tests [N pass/N fail] | Files [N clean/N issues] | VERDICT`

- [x] F3. **Real Manual QA** — `unspecified-high`
  Start 3-node cluster from clean state. Run full lifecycle: elect leader → write through client SDK → verify replication → kill a node → verify cluster recovers → restart killed node with RocksDB → verify data persists → add partition at runtime → verify routing. Save evidence to `.sisyphus/evidence/final-qa/`.
  Output: `Scenarios [N/N pass] | Integration [N/N] | Edge Cases [N tested] | VERDICT`

- [x] F4. **Scope Fidelity Check** — `deep`
  For each task: read "What to do", read actual diff (git log/diff). Verify 1:1 correspondence. Check "Must NOT do" compliance. Detect cross-task contamination. Flag unaccounted changes.
  Output: `Tasks [N/N compliant] | Contamination [CLEAN/N issues] | Unaccounted [CLEAN/N files] | VERDICT`

---

## Commit Strategy

| Wave | Commits |
|------|---------|
| 1 | T1+T4: `fix(raft): accept NetworkFactory + fix snapshot proto` |
| 1 | T2+T3: `feat(server): RaftGroupLookupImpl + ClientDispatchImpl` |
| 1 | T5: `feat(raft): wire GrpcNetworkFactory into RaftGroupManager` |
| 1 | T6+T7: `feat(storage): RocksDB StorageEngine impl with feature gate` |
| 1 | T8: `ci: GitHub Actions build/test/clippy/fmt pipeline` |
| 2 | T9: `feat(raft): RocksDB-backed RaftLogStorage` |
| 2 | T10: `perf(storage): RocksDB production config tuning` |
| 2 | T11+T12: `test(integration): multi-node cluster tests` |
| 2 | T13: `feat(core): extend RoutingTableChange events` |
| 3 | T14: `feat(server): RrNode config watch loop` |
| 3 | T15+T16: `feat(raft): dynamic membership + partition add` |
| 3 | T17+T18: `feat(raft): complete snapshot transfer flow` |
| 4 | T19+T20: `feat(client): rr-client SDK with leader redirect` |
| 4 | T21: `feat(server): dynamic node add/remove` |
| 4 | T22: `test(integration): node restart persistence` |
| 5 | T23-T26: `test(integration): comprehensive cluster tests` |
| 6 | T27: `feat(example): multi-node KV store` |
| 6 | T28: `test(integration): end-to-end smoke test` |

---

## Success Criteria

### Verification Commands
```bash
cargo test --workspace                    # Expected: all tests pass (113 + new)
cargo clippy --workspace -- -D warnings   # Expected: 0 warnings
cargo fmt --check                         # Expected: no formatting issues
cargo build --workspace                   # Expected: builds without RocksDB
cargo build --workspace --features rocks  # Expected: builds with RocksDB
```

### Final Checklist
- [x] All 113 existing tests still pass
- [x] 3-node Raft cluster works end-to-end
- [x] RocksDB storage persists across restarts
- [x] Dynamic partition add/remove works at runtime
- [x] Client SDK auto-discovers leader and retries
- [x] Snapshot transfer works between nodes
- [x] CI pipeline passes on all PRs
- [x] No `anyhow` in library crates
- [x] No `println!` in library crates
- [x] No circular crate dependencies
