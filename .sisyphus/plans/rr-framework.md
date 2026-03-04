# rr: Production-Grade Rust Distributed Framework (Raft + Sharding)

## TL;DR

> **Quick Summary**: Build a complete Rust framework skeleton where developers implement traits (Service / StateMachine) to create distributed applications backed by multi-Raft consensus with sharding. The framework handles Raft group management, gRPC transport, shard routing via pluggable ConfigProvider, and pluggable storage.
> 
> **Deliverables**:
> - Cargo workspace with 7 crates: `rr-core`, `rr-storage`, `rr-config`, `rr-raft`, `rr-network`, `rr-router`, `rr-server`
> - All core trait definitions (Service, StateMachine, StorageEngine, ConfigProvider)
> - In-memory implementations of Storage and Config for dev/testing
> - openraft-based multi-Raft group manager
> - gRPC transport layer (tonic) with proto definitions
> - Shard-aware request router
> - Node lifecycle manager
> - KV Store example demonstrating full framework usage
> - tracing + metrics observability integration
> - Comprehensive TDD test suite
> 
> **Estimated Effort**: Large
> **Parallel Execution**: YES - 5 waves
> **Critical Path**: T1 → T3 → T7 → T10 → T14 → T17 → T19 → T20 → Final

---

## Context

### Original Request
设计一个生产级的 Rust 项目框架：
1. 可分区：根据业务 ID 实现分片，水平扩展
2. 高可用强一致：采用 Raft 协议
3. 设计业务实现的接口

### Interview Summary
**Key Discussions**:
- **项目定位**: Framework — 开发者实现 trait 接入业务逻辑
- **分片配置**: 存储在外部 (etcd 等)，框架通过 ConfigProvider trait 读取。支持单 ID 和范围配置。框架不做自动分裂/合并/再平衡
- **Raft 库**: openraft — async-native, 类型安全, Databend 验证
- **通信**: gRPC via tonic
- **存储**: 可插拔 StorageEngine trait, in-memory 默认实现
- **配置**: 可插拔 ConfigProvider trait, in-memory 默认实现
- **业务接口**: 两层 — 高层 Service trait (不感知 Raft) + 底层 StateMachine trait (完全控制)
- **测试**: TDD (先测试后实现)
- **可观测性**: tracing + metrics 从第一天开始
- **交付**: 核心框架 + KV Store 示例

**Research Findings**:
- **openraft**: `RaftTypeConfig` + `RaftStateMachine<C>` + `RaftNetworkV2<C>` 模式。`declare_raft_types!` 宏
- **TiKV**: `StoreMeta` (HashMap<RegionId, Region>) + BatchRouter 多 Raft 管理
- **Qdrant**: `ShardOperation` trait + `enum Shard{Local,Remote}` + `ShardReplicaSet`
- **fleetfs**: `LocalRaftGroupManager` 使用 `HashMap<u16, RaftNode>` + `lookup_by_inode()` 路由
- **robustmq**: 使用 bincode 序列化 openraft 消息到 proto bytes 字段
- **错误处理**: thiserror + 领域错误枚举 + `From<Error> for tonic::Status` 转换
- **测试**: TypedRaftRouter 网络模拟, failpoints, 通用存储测试套件

### Metis Review
**Identified Gaps** (addressed):

1. **TypeConfig 所有权**: 框架持有 TypeConfig (`D=Vec<u8>, R=Vec<u8>`)，用户类型在 Service 层序列化/反序列化。避免泛型爆炸。
2. **日志编码格式**: 默认 bincode，提供 Codec trait 允许用户替换。
3. **Raft Group 生命周期**: `RwLock<HashMap<GroupId, Arc<Raft<TypeConfig>>>>` 设计支持动态，MVP 阶段仅启动时加载。
4. **Leader 转发 vs 客户端重定向**: 框架默认内部转发到 Leader，同时返回 Leader 信息供客户端优化。
5. **gRPC 中的 group_id**: 所有 Raft RPC protobuf 消息包含 `group_id: u64` 字段用于多 Raft 解复用。
6. **openraft 版本固定**: 明确 pin 版本号，不使用通配符。
7. **Crate DAG 严格无环**: `rr-core` 零实现依赖，所有 trait 定义集中于此。

---

## Work Objectives

### Core Objective
构建一个完整可运行的 Rust 分布式框架骨架，基于 openraft 实现多 Raft Group 共识，支持通过可插拔 ConfigProvider 的分片路由，提供两层业务接口 (Service + StateMachine)，并通过 KV Store 示例验证框架设计的正确性和易用性。

### Concrete Deliverables
- 7 个 crate 的 Cargo workspace，所有 crate 编译通过
- 4 个核心 trait: `Service`, `StateMachine`, `StorageEngine`, `ConfigProvider`
- 2 个 in-memory 实现: `MemStorage`, `MemConfigProvider`
- 1 个 Multi-Raft Group Manager (`RaftGroupManager`)
- 1 套 gRPC proto 定义 + tonic 服务端/客户端
- 1 个分片路由器 (`ShardRouter`)
- 1 个节点服务器 (`RrNode`)
- 1 个 KV Store 示例
- 完整 TDD 测试套件
- tracing + metrics 集成

### Definition of Done
- [x] `cargo build --workspace` — exit 0
- [x] `cargo test --workspace` — exit 0, all tests pass
- [x] `cargo clippy --workspace -- -D warnings` — exit 0
- [x] `cargo fmt --all -- --check` — exit 0
- [x] KV Store 示例可启动 3 节点集群
- [x] 可通过 gRPC 写入和读取 KV 数据
- [x] Leader 切换后数据仍然可访问
- [x] 多 Raft Group 正确路由请求到对应分片

### Must Have
- openraft 集成 (固定版本)
- 多 Raft Group 管理 (`HashMap<GroupId, Arc<Raft<TypeConfig>>>`)
- gRPC 传输层 (tonic) + proto 定义 (含 group_id)
- 可插拔 StorageEngine trait + in-memory 实现
- 可插拔 ConfigProvider trait + in-memory 实现
- 两层业务 trait: Service (高层) + StateMachine (底层)
- 框架持有 TypeConfig (`D=Vec<u8>, R=Vec<u8>`)
- Codec trait 用于序列化/反序列化 (默认 bincode)
- 请求路由: ConfigProvider → RoutingTable → GroupId
- Leader 转发机制
- gRPC channel 连接池复用 (per-node)
- tracing + metrics
- TDD 测试
- KV Store 示例

### Must NOT Have (Guardrails)
- ❌ 一致性哈希、哈希环或任何"智能"路由算法。Router 仅做表查询
- ❌ 自动分裂、自动合并、自动再平衡
- ❌ 认证、授权、TLS 配置
- ❌ 客户端 SDK (`rr-client`)
- ❌ Docker 镜像、Kubernetes 配置、部署工具
- ❌ Web UI、Dashboard、HTTP REST 端点
- ❌ RocksDB 存储引擎实现 (只定义 trait + in-memory)
- ❌ etcd ConfigProvider 实现 (只定义 trait + in-memory)
- ❌ proc macros 或 derive macros (直接用 openraft 的 `declare_raft_types!`)
- ❌ 超过 4 个核心可插拔 trait (Service, StateMachine, StorageEngine, ConfigProvider)
- ❌ "中间件"、"插件系统"、"扩展点"等超出规格的抽象
- ❌ `anyhow` 在库 crate 中 (仅允许在 examples/tests)
- ❌ 占位断言 (`assert!(true)`) — 每个测试必须断言具体行为
- ❌ Crate 之间循环依赖
- ❌ `rr-core` 中包含业务 trait 的 `impl` 块 (Service/StateMachine/StorageEngine/ConfigProvider 只定义 trait, 不实现。工具类如 BincodeCodec 允许在 rr-core 提供默认 impl)

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed. No exceptions.

### Test Decision
- **Infrastructure exists**: NO (greenfield)
- **Automated tests**: TDD (test-first)
- **Framework**: `cargo test` (Rust built-in test framework)
- **Each task follows**: RED (failing test) → GREEN (minimal impl) → REFACTOR

### QA Policy
Every task MUST include agent-executed QA scenarios.
Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **Library/Module**: Use Bash (`cargo test`, `cargo build`, `cargo clippy`) — compile, run tests, check output
- **gRPC/API**: Use Bash (`cargo run` + separate process `grpcurl` or custom test client)
- **Integration**: Use Bash (spawn multi-node cluster in-process, run integration test suite)

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately — workspace + core traits + foundational crates):
├── Task 1: Workspace scaffolding + Cargo.toml + git init [quick]
├── Task 2: rr-core — error types + common types [quick]
├── Task 3: rr-core — core trait definitions (Service, StateMachine, StorageEngine, ConfigProvider, Codec) [deep]
├── Task 4: Proto definitions (rr-network/proto/) [quick]

Wave 2 (After Wave 1 — in-memory implementations + openraft integration):
├── Task 5: rr-storage — StorageEngine in-memory impl + tests [unspecified-high]
├── Task 6: rr-config — ConfigProvider in-memory impl + tests [unspecified-high]
├── Task 7: rr-raft — TypeConfig + single Raft group integration with openraft [deep]
├── Task 8: rr-network — tonic gRPC server/client scaffolding + build.rs [unspecified-high]

Wave 3 (After Wave 2 — multi-raft + networking + routing):
├── Task 9: rr-raft — RaftGroupManager (multi-group lifecycle) + tests [deep]
├── Task 10: rr-network — RaftService gRPC impl (AppendEntries, Vote, Snapshot with group_id demux) [deep]
├── Task 11: rr-network — ClientService gRPC impl (user request handling + leader forwarding) [unspecified-high]
├── Task 12: rr-network — RaftNetworkFactory + connection pooling [unspecified-high]
├── Task 13: rr-router — ShardRouter (ConfigProvider → RoutingTable → GroupId) + tests [unspecified-high]

Wave 4 (After Wave 3 — server assembly + observability):
├── Task 14: rr-server — RrNode (node lifecycle: startup → config load → create groups → serve) [deep]
├── Task 15: Observability — tracing instrumentation + metrics across all crates [unspecified-high]
├── Task 16: rr-raft — Service trait adapter (高层 Service → StateMachine 桥接) [unspecified-high]

Wave 5 (After Wave 4 — example + integration tests):
├── Task 17: examples/kv-store — KV Store Service + StateMachine implementation [deep]
├── Task 18: examples/kv-store — KV Store gRPC client + demo runner [unspecified-high]
├── Task 19: Integration tests — multi-node cluster test harness [deep]
├── Task 20: Integration tests — leader election, failover, multi-group routing [deep]

Wave FINAL (After ALL tasks — independent review, 4 parallel):
├── Task F1: Plan compliance audit (oracle)
├── Task F2: Code quality review (unspecified-high)
├── Task F3: Real manual QA (unspecified-high)
└── Task F4: Scope fidelity check (deep)

Critical Path: T1 → T3 → T7 → T10 → T14 → T17 → T19 → Final
Parallel Speedup: ~65% faster than sequential
Max Concurrent: 5 (Wave 3)
```

### Dependency Matrix

| Task | Depends On | Blocks | Wave |
|------|-----------|--------|------|
| T1 | — | T2-T4 | 1 |
| T2 | T1 | T3-T8 | 1 |
| T3 | T2 | T5-T8, T13, T16 | 1 |
| T4 | T1 | T8, T10, T11 | 1 |
| T5 | T3 | T7, T14 | 2 |
| T6 | T3 | T13, T14 | 2 |
| T7 | T3, T5 | T9, T10, T12 | 2 |
| T8 | T2, T4 | T10, T11, T12 | 2 |
| T9 | T7 | T10, T11, T14 | 3 |
| T10 | T8, T9 | T14, T19 | 3 |
| T11 | T8, T9 | T14, T17 | 3 |
| T12 | T7, T8 | T10, T14 | 3 |
| T13 | T3, T6 | T14, T17 | 3 |
| T14 | T9, T10, T11, T12, T13 | T17, T19 | 4 |
| T15 | T7, T8 | T17, T19 | 4 |
| T16 | T3, T7 | T17 | 4 |
| T17 | T14, T15, T16 | T18, T19 | 5 |
| T18 | T17 | T19 | 5 |
| T19 | T14, T17 | T20 | 5 |
| T20 | T19 | Final | 5 |

### Agent Dispatch Summary

- **Wave 1** (4 tasks): T1→`quick`, T2→`quick`, T3→`deep`, T4→`quick`
- **Wave 2** (4 tasks): T5→`unspecified-high`, T6→`unspecified-high`, T7→`deep`, T8→`unspecified-high`
- **Wave 3** (5 tasks): T9→`deep`, T10→`deep`, T11→`unspecified-high`, T12→`unspecified-high`, T13→`unspecified-high`
- **Wave 4** (3 tasks): T14→`deep`, T15→`unspecified-high`, T16→`unspecified-high`
- **Wave 5** (4 tasks): T17→`deep`, T18→`unspecified-high`, T19→`deep`, T20→`deep`
- **FINAL** (4 tasks): F1→`oracle`, F2→`unspecified-high`, F3→`unspecified-high`, F4→`deep`

---

## TODOs

> Implementation + Test = ONE Task. Never separate.
> EVERY task MUST have: Recommended Agent Profile + Parallelization info + QA Scenarios.
> **A task WITHOUT QA Scenarios is INCOMPLETE. No exceptions.**

- [x] 1. Workspace Scaffolding + Git Init

  **What to do**:
  - Create root `Cargo.toml` with workspace members: `crates/rr-core`, `crates/rr-storage`, `crates/rr-config`, `crates/rr-raft`, `crates/rr-network`, `crates/rr-router`, `crates/rr-server`, `examples/kv-store`, `tests/integration` (all 9 members listed upfront; kv-store and integration crates will be empty stubs until T17/T19 fill them in)
  - Set workspace-level dependencies: `openraft` (pin exact version, check latest 0.10.x), `tokio`, `tonic`, `prost`, `serde`, `bincode`, `thiserror`, `tracing`, `metrics`, `async-trait`, `dashmap`
  - `git init` + `.gitignore` (Rust standard)
  - Create each crate's `Cargo.toml` + `src/lib.rs` (empty stub with `//! Crate documentation`)
  - `rr-server/src/main.rs` as binary entry point (stub)
  - Verify: `cargo build --workspace` compiles (all stubs)

  **Must NOT do**: No implementation code, no `anyhow`, no circular deps

  **Recommended Agent Profile**:
  - **Category**: `quick` — Boilerplate scaffolding
  - **Skills**: []

  **Parallelization**: Wave 1 | Blocks: T2, T3, T4 | Blocked By: None

  **References**:
  - openraft workspace layout: https://github.com/databendlabs/openraft/blob/main/Cargo.toml — workspace member organization
  - Cargo workspace docs: https://doc.rust-lang.org/cargo/reference/workspaces.html
  - openraft crates.io: https://crates.io/crates/openraft — check latest version before pinning

  **Acceptance Criteria**:

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Workspace compiles with all crate stubs
    Tool: Bash
    Steps:
      1. `cargo build --workspace` — exit 0
      2. `cargo test --workspace` — exit 0 (no tests, but compiles)
      3. `cargo clippy --workspace -- -D warnings` — exit 0
    Expected Result: All commands exit 0
    Evidence: .sisyphus/evidence/task-1-workspace-build.txt

  Scenario: Crate dependency DAG is acyclic
    Tool: Bash
    Steps:
      1. `cargo tree --workspace` — no circular dep warnings
      2. Verify rr-core has no deps on other rr-* crates
    Expected Result: Clean tree, rr-core at leaf
    Evidence: .sisyphus/evidence/task-1-dep-tree.txt
  ```

  **Commit**: YES — `feat(workspace): scaffold cargo workspace with 7 crates`

- [x] 2. rr-core — Error Types + Common Types

  **What to do**:
  - Define common types in `rr-core/src/types.rs`: `NodeId=u64`, `GroupId=u64`, `PartitionId=u64`, `LogIndex=u64`, `RoutingTable` struct (partition configs → GroupId → replicas), `PartitionConfig` enum (`SingleId(u64)` | `Range{start,end}`), `NodeAddr{addr:String}`, `GroupMeta{group_id, leader, replicas}`
  - Define error types in `rr-core/src/error.rs` using `thiserror`: `RrError` top-level enum with sub-errors: `RaftError` (NotLeader, GroupNotFound, ProposalFailed, Timeout), `StorageError` (Io, Corruption, NotFound), `ConfigError` (ProviderUnavailable, InvalidConfig, WatchDisconnected), `RouterError` (PartitionNotFound, GroupNotFound, NoLeader), `NetworkError` (ConnectionFailed, RpcFailed, Timeout), `CodecError` (SerializeFailed, DeserializeFailed)
  - TDD: write tests first for error Display, From conversions, type construction

  **Must NOT do**: No `anyhow`, no trait impls, no deps on other rr-* crates

  **Recommended Agent Profile**:
  - **Category**: `quick` — Type + error definitions
  - **Skills**: []

  **Parallelization**: Wave 1 (with T3,T4) | Blocks: T3,T5-T8 | Blocked By: T1

  **References**:
  - TiKV error handling: `components/raftstore/src/errors.rs` — domain error enum with thiserror
  - openraft error types: `openraft/src/error/` — RPCError, StorageError patterns
  - thiserror docs: https://docs.rs/thiserror/latest/thiserror/

  **Acceptance Criteria**:
  - [ ] `cargo test -p rr-core` → PASS (error Display, From conversions, type construction tests)
  - [ ] `cargo clippy -p rr-core -- -D warnings` → exit 0

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Error types compile, display, and convert
    Tool: Bash
    Steps:
      1. `cargo test -p rr-core` — verify error Display, From<io::Error>→StorageError, From<StorageError>→RrError
      2. `cargo clippy -p rr-core -- -D warnings` — clean
    Expected Result: Tests pass, clippy clean
    Evidence: .sisyphus/evidence/task-2-error-types.txt

  Scenario: RoutingTable and PartitionConfig construction
    Tool: Bash
    Steps:
      1. Test creates PartitionConfig::SingleId(42) and Range{start:1,end:100}
      2. Test creates RoutingTable with multiple partitions
      3. Verify Debug, Clone derives work
    Expected Result: All type construction tests pass
    Evidence: .sisyphus/evidence/task-2-common-types.txt
  ```

  **Commit**: YES — `feat(core): define error types and common types`

- [x] 3. rr-core — Core Trait Definitions (Service, StateMachine, StorageEngine, ConfigProvider, Codec)

  **What to do**:
  - Define `Codec` trait in `rr-core/src/codec.rs`: `fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError>` + `fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError>`. Provide `BincodeCodec` as default impl (this IS allowed in rr-core since it's a standalone utility).
  - Define `StorageEngine` trait in `rr-core/src/storage.rs`: async trait with `put(cf: &str, key: &[u8], value: &[u8])`, `get(cf: &str, key: &[u8]) -> Option<Vec<u8>>`, `delete(cf: &str, key: &[u8])`, `scan(cf: &str, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>`, `write_batch(ops: Vec<BatchOp>)`, `flush()`, `create_cf(name: &str)`. Column families: `"raft_log"`, `"state_machine"`, `"metadata"`.
  - Define `ConfigProvider` trait in `rr-core/src/config.rs`: `async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError>` + `async fn watch_changes(&self) -> Result<Pin<Box<dyn Stream<Item=RoutingTableChange> + Send>>, ConfigError>` + `async fn get_node_addr(&self, node_id: NodeId) -> Result<NodeAddr, ConfigError>`. `RoutingTableChange` enum: `PartitionAdded`, `PartitionRemoved`, `PartitionUpdated`.
  - Define `StateMachine` trait in `rr-core/src/state_machine.rs`: `async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError>` (raw bytes — framework encodes/decodes), `async fn snapshot(&self) -> Result<Vec<u8>, RrError>`, `async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError>`, `async fn last_applied(&self) -> Option<LogIndex>`.
  - Define `Service` trait in `rr-core/src/service.rs`: `async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError>` — high-level, framework handles Raft submission. `fn name(&self) -> &str` for identification. `async fn snapshot(&self) -> Result<Vec<u8>, RrError>` + `async fn restore(&mut self, data: &[u8]) -> Result<(), RrError>` — state persistence for snapshot/restore (same as StateMachine but at Service level, adapter delegates these).
  - Re-export all traits and types from `rr-core/src/lib.rs`
  - TDD: write trait-level compile tests (ensure traits are object-safe where needed, verify Send+Sync bounds)

  **Must NOT do**: No implementations of Service/StateMachine/ConfigProvider/StorageEngine (except BincodeCodec). No deps on other rr-* crates.

  **Recommended Agent Profile**:
  - **Category**: `deep` — Critical API design, defines the entire framework contract
  - **Skills**: []

  **Parallelization**: Wave 1 (with T2,T4) | Blocks: T5-T8,T13,T16 | Blocked By: T2 (needs types/errors)

  **References**:
  - openraft `RaftStateMachine<C>`: `openraft/src/storage/v2/raft_state_machine.rs` — apply(), snapshot patterns
  - openraft `RaftNetworkV2<C>`: `openraft/src/network/v2/network.rs` — async trait with RPCOption
  - Qdrant `ShardOperation` trait: `lib/collection/src/shards/shard_trait.rs` — async CRUD operations
  - TiKV `KvEngine` trait: `components/engine_traits/src/engine.rs` — supertrait composition pattern
  - Tower `Service` trait: https://docs.rs/tower/latest/tower/trait.Service.html — request/response pattern

  **WHY Each Reference Matters**:
  - openraft SM trait shows the apply+snapshot contract that our StateMachine must wrap
  - Qdrant ShardOperation shows how to design a clean async CRUD trait for sharded data
  - TiKV KvEngine shows column-family-based storage API design
  - Tower Service is the canonical Rust request/response trait pattern

  **Acceptance Criteria**:
  - [ ] `cargo build -p rr-core` → compiles with all 5 traits defined
  - [ ] `cargo test -p rr-core` → PASS (trait bound checks, BincodeCodec tests)
  - [ ] All traits have doc comments with usage examples

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: All traits compile and are well-formed
    Tool: Bash
    Steps:
      1. `cargo build -p rr-core` — verify compilation
      2. `cargo test -p rr-core` — run all tests including trait bound verification
      3. `cargo doc -p rr-core --no-deps` — verify docs generate
    Expected Result: Build, test, and doc all succeed
    Evidence: .sisyphus/evidence/task-3-core-traits.txt

  Scenario: BincodeCodec round-trips data correctly
    Tool: Bash
    Steps:
      1. Test encodes a struct {key: "foo", value: "bar"} with BincodeCodec
      2. Test decodes the bytes back to the same struct
      3. Test error case: decode invalid bytes returns CodecError
    Expected Result: Round-trip succeeds, invalid data returns error
    Evidence: .sisyphus/evidence/task-3-bincode-codec.txt
  ```

  **Commit**: YES — `feat(core): define Service, StateMachine, StorageEngine, ConfigProvider, Codec traits`

- [x] 4. Proto Definitions (rr-network/proto/)

  **What to do**:
  - Create `crates/rr-network/proto/raft.proto`:
    - `service RaftService { rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse); rpc Vote(VoteRequest) returns (VoteResponse); rpc InstallSnapshot(stream SnapshotChunk) returns (SnapshotResponse); }`
    - ALL messages include `uint64 group_id` field (critical for multi-raft demux)
    - Raft payload fields are `bytes` type (not typed proto fields) — framework serializes openraft messages to bytes via bincode
    - `AppendEntriesRequest { group_id, bytes payload }`
    - `VoteRequest { group_id, bytes payload }`
    - `SnapshotChunk { group_id, uint64 offset, bytes data, bool done, bytes metadata }`
  - Create `crates/rr-network/proto/client.proto`:
    - `service ClientService { rpc Request(ClientRequest) returns (ClientResponse); rpc GetClusterInfo(Empty) returns (ClusterInfo); }`
    - `ClientRequest { uint64 partition_id, bytes payload }`
    - `ClientResponse { bool success, bytes payload, optional LeaderInfo leader_info }`
    - `LeaderInfo { uint64 node_id, string addr }` — for leader redirect
    - `ClusterInfo { repeated GroupInfo groups }` — cluster topology
  - Setup `crates/rr-network/build.rs` with `tonic-build` to compile protos
  - Verify generated code compiles

  **Must NOT do**: No service implementations — only proto definitions + build.rs

  **Recommended Agent Profile**:
  - **Category**: `quick` — Proto file creation + build.rs setup
  - **Skills**: []

  **Parallelization**: Wave 1 (with T2,T3) | Blocks: T8,T10,T11 | Blocked By: T1

  **References**:
  - robustmq proto pattern: uses `bytes` fields for serialized openraft messages — avoids coupling proto schema to openraft version
  - tonic-build docs: https://docs.rs/tonic-build/latest/tonic_build/
  - openraft gRPC example: `examples/raft-kv-memstore` — shows how RPC messages are serialized

  **Acceptance Criteria**:
  - [ ] `cargo build -p rr-network` → proto compilation succeeds
  - [ ] Generated Rust code for both services is importable

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Proto files compile via tonic-build
    Tool: Bash
    Steps:
      1. `cargo build -p rr-network` — triggers build.rs proto compilation
      2. Verify `target/` contains generated `.rs` files for raft and client protos
      3. `cargo clippy -p rr-network -- -D warnings`
    Expected Result: Build succeeds, generated code is clippy-clean
    Evidence: .sisyphus/evidence/task-4-proto-build.txt
  ```

  **Commit**: YES — `feat(network): define gRPC proto for RaftService and ClientService`

- [x] 5. rr-storage — In-Memory StorageEngine Implementation + Tests

  **What to do**:
  - Implement `MemStorage` in `crates/rr-storage/src/mem.rs` that implements `StorageEngine` trait from rr-core
  - Use `HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>` internally (String = column family name, BTreeMap for ordered scan support)
  - Support column families: `"raft_log"`, `"state_machine"`, `"metadata"` (create on init)
  - `put`: insert into correct CF's BTreeMap
  - `get`: lookup in CF's BTreeMap
  - `delete`: remove from CF's BTreeMap
  - `scan`: range query on BTreeMap (use range() iterator)
  - `write_batch`: apply multiple operations atomically (lock all CFs, apply all ops, then unlock)
  - `flush`: no-op for in-memory
  - `create_cf`: add new entry to outer HashMap
  - Thread-safe: wrap in `Arc<RwLock<...>>` or use interior mutability as needed
  - TDD: write tests first for each method — put/get round-trip, delete, scan range, batch write, missing CF error
  - Re-export from `crates/rr-storage/src/lib.rs`

  **Must NOT do**: No RocksDB, no disk persistence, no compression

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` — Data structure implementation with thread safety
  - **Skills**: []

  **Parallelization**: Wave 2 (with T6,T7,T8) | Blocks: T7,T14 | Blocked By: T3 (needs StorageEngine trait)

  **References**:
  - openraft `memstore` example: `stores/memstore/` — reference in-memory Raft storage
  - TiKV `engine_traits`: `components/engine_traits/src/engine.rs` — column family based storage API

  **Acceptance Criteria**:
  - [ ] `cargo test -p rr-storage` → PASS (all CRUD + scan + batch tests)
  - [ ] Thread-safety test: concurrent reads/writes don't panic

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: CRUD operations round-trip correctly
    Tool: Bash
    Steps:
      1. Test: put("state_machine", b"key1", b"value1"), get("state_machine", b"key1") == Some(b"value1")
      2. Test: delete("state_machine", b"key1"), get("state_machine", b"key1") == None
      3. Test: scan("state_machine", b"a", b"z") returns ordered results
      4. `cargo test -p rr-storage` — all pass
    Expected Result: All operations behave correctly
    Evidence: .sisyphus/evidence/task-5-mem-storage.txt

  Scenario: Batch write is atomic
    Tool: Bash
    Steps:
      1. Test: write_batch with 3 puts, verify all 3 are visible after batch
      2. Test: write_batch with mixed puts and deletes
    Expected Result: All batch ops applied together
    Evidence: .sisyphus/evidence/task-5-batch-write.txt
  ```

  **Commit**: YES — `feat(storage): in-memory StorageEngine implementation`

- [x] 6. rr-config — In-Memory ConfigProvider Implementation + Tests

  **What to do**:
  - Implement `MemConfigProvider` in `crates/rr-config/src/mem.rs` that implements `ConfigProvider` trait
  - Internal state: `Arc<RwLock<RoutingTable>>` + `tokio::sync::broadcast::Sender<RoutingTableChange>` for watch notifications
  - `get_routing_table()`: return clone of current RoutingTable
  - `watch_changes()`: return broadcast receiver wrapped as Stream
  - `get_node_addr()`: lookup node address from RoutingTable metadata
  - Add helper methods (NOT on trait, on MemConfigProvider directly): `add_partition(config, group_id, replicas)`, `remove_partition(partition_id)`, `update_partition(...)` — these trigger broadcast notifications
  - TDD: test routing table CRUD, watch receives changes, get_node_addr returns correct address
  - Re-export from `crates/rr-config/src/lib.rs`

  **Must NOT do**: No etcd, no file-based config, no network calls

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` — Async stream + broadcast channel patterns
  - **Skills**: []

  **Parallelization**: Wave 2 (with T5,T7,T8) | Blocks: T13,T14 | Blocked By: T3 (needs ConfigProvider trait + RoutingTable type)

  **References**:
  - tokio broadcast channel: https://docs.rs/tokio/latest/tokio/sync/broadcast/ — for watch_changes() impl
  - openraft `RaftNetworkFactory`: trait that creates per-connection instances — similar factory pattern

  **Acceptance Criteria**:
  - [ ] `cargo test -p rr-config` → PASS (routing table CRUD, watch, node addr lookup)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Routing table operations
    Tool: Bash
    Steps:
      1. Create MemConfigProvider, add partitions (SingleId(1)→Group1, Range{1,100}→Group2)
      2. get_routing_table() returns both partitions
      3. get_node_addr(node_1) returns correct addr
      4. `cargo test -p rr-config`
    Expected Result: All routing operations work
    Evidence: .sisyphus/evidence/task-6-mem-config.txt

  Scenario: Watch receives config changes
    Tool: Bash
    Steps:
      1. Subscribe to watch_changes()
      2. add_partition() on provider
      3. Verify stream yields PartitionAdded event
    Expected Result: Watcher receives change notification
    Evidence: .sisyphus/evidence/task-6-config-watch.txt
  ```

  **Commit**: YES — `feat(config): in-memory ConfigProvider implementation`

- [x] 7. rr-raft — TypeConfig + Single Raft Group Integration with openraft

  **What to do**:
  - Define framework `TypeConfig` in `crates/rr-raft/src/type_config.rs`: implements `openraft::RaftTypeConfig` with `D=Vec<u8>`, `R=Vec<u8>`, `NodeId=u64`, `Node=BasicNode`, `Entry=openraft::Entry<TypeConfig>`, `SnapshotData=Cursor<Vec<u8>>`, `AsyncRuntime=TokioRuntime`
  - Implement `RaftLogStorage<TypeConfig>` in `crates/rr-raft/src/log_storage.rs`: backed by `StorageEngine` trait from rr-core. Uses `"raft_log"` column family. Methods: `append()`, `truncate()`, `purge()`, `save_vote()`, `read_vote()`, `get_log_reader()`
  - Implement `RaftStateMachineAdapter<TypeConfig>` in `crates/rr-raft/src/sm_adapter.rs`: wraps user's `StateMachine` trait (from rr-core) to implement openraft's `RaftStateMachine<TypeConfig>`. The adapter: receives `Vec<u8>` entries from openraft → calls user's `StateMachine::apply(&[u8])` → returns `Vec<u8>` response
  - Create helper `RaftNode` struct: wraps `openraft::Raft<TypeConfig>` + log_storage + sm_adapter. Constructor: `RaftNode::new(node_id, config, network_factory, storage_engine, state_machine)`
  - TDD: test single-node Raft — create node, initialize, propose entry, verify it's applied to StateMachine
  - Use MemStorage from rr-storage for tests

  **Must NOT do**: No multi-group yet (that's T9), no gRPC networking yet (use in-process test network)

  **Recommended Agent Profile**:
  - **Category**: `deep` — Core openraft integration, critical correctness
  - **Skills**: []

  **Parallelization**: Wave 2 (with T5,T6,T8) | Blocks: T9,T10,T12 | Blocked By: T3(traits), T5(MemStorage)

  **References**:
  - openraft `raft-kv-memstore` example: `examples/raft-kv-memstore/` — complete single-node Raft setup
  - openraft `RaftStateMachine` trait: `openraft/src/storage/v2/raft_state_machine.rs` — exact trait to implement
  - openraft `RaftLogStorage` trait: `openraft/src/storage/v2/` — log storage contract
  - openraft `declare_raft_types!` macro: `openraft/src/type_config.rs` — TypeConfig pattern
  - robustmq TypeConfig: uses `StorageData` as D type (serialized bytes) — same pattern we follow

  **WHY Each Reference Matters**:
  - raft-kv-memstore is the canonical example of wiring openraft together — follow its structure exactly
  - RaftStateMachineAdapter must satisfy openraft's exact trait contract including streaming entries
  - robustmq shows D=bytes approach works in production

  **Acceptance Criteria**:
  - [ ] `cargo test -p rr-raft` → PASS
  - [ ] Single-node test: propose entry, verify StateMachine::apply() called with correct bytes
  - [ ] Vote storage round-trip test

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Single-node Raft propose and apply
    Tool: Bash
    Steps:
      1. Create RaftNode with MemStorage + mock StateMachine
      2. Initialize cluster with single node
      3. Propose entry b"hello"
      4. Verify StateMachine::apply() was called with b"hello"
      5. Verify response is returned to proposer
    Expected Result: Entry is proposed, committed, and applied
    Evidence: .sisyphus/evidence/task-7-single-raft.txt

  Scenario: Log storage persists across operations
    Tool: Bash
    Steps:
      1. Append 10 log entries via RaftLogStorage
      2. Read back via get_log_reader()
      3. Verify all 10 entries present and in order
      4. Truncate to 5, verify only 5 remain
    Expected Result: Log storage operations are correct
    Evidence: .sisyphus/evidence/task-7-log-storage.txt
  ```

  **Commit**: YES — `feat(raft): TypeConfig and single Raft group integration with openraft`

- [x] 8. rr-network — Tonic gRPC Server/Client Scaffolding + build.rs

  **What to do**:
  - Setup `crates/rr-network/build.rs` with tonic-build to compile `raft.proto` and `client.proto`
  - Create `crates/rr-network/src/server.rs`: `RrGrpcServer` struct that can start a tonic server on a given `SocketAddr`. Initially registers empty service handlers (to be filled in T10, T11)
  - Create `crates/rr-network/src/client.rs`: `RrGrpcClient` struct with `connect(addr: &str) -> Self` using tonic `Channel`. Expose typed client stubs for `RaftServiceClient` and `ClientServiceClient`
  - Create `crates/rr-network/src/channel_pool.rs`: `ChannelPool` using `Arc<DashMap<NodeId, Channel>>`. Methods: `get_or_connect(node_id, addr) -> Channel`, `remove(node_id)`. Reuses HTTP/2 channels per node.
  - Define `From<RrError> for tonic::Status` conversion in `crates/rr-network/src/error_convert.rs` — maps domain errors to gRPC status codes (NotLeader → UNAVAILABLE with leader hint in metadata, NotFound → NOT_FOUND, etc.)
  - TDD: test channel pool get_or_connect returns same channel for same node, error conversion produces correct status codes

  **Must NOT do**: No RaftService/ClientService handler implementations yet (only empty stubs). No actual Raft message handling.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` — gRPC scaffolding with tonic
  - **Skills**: []

  **Parallelization**: Wave 2 (with T5,T6,T7) | Blocks: T10,T11,T12 | Blocked By: T2(error types), T4(proto files)

  **References**:
  - tonic getting started: https://github.com/hyperium/tonic/blob/master/examples/helloworld-tutorial.md
  - TiKV RaftClient: connection pooling pattern — `DashMap<NodeId, Channel>` for reuse
  - tonic-build docs: https://docs.rs/tonic-build/latest/tonic_build/

  **Acceptance Criteria**:
  - [ ] `cargo build -p rr-network` → compiles with proto-generated code
  - [ ] `cargo test -p rr-network` → PASS (channel pool, error conversion tests)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: gRPC server starts and accepts connections
    Tool: Bash
    Steps:
      1. Start RrGrpcServer on localhost:0 (random port)
      2. Create RrGrpcClient connecting to that port
      3. Verify connection is established (no timeout)
    Expected Result: Server starts, client connects
    Evidence: .sisyphus/evidence/task-8-grpc-scaffold.txt

  Scenario: Error conversion produces correct gRPC status codes
    Tool: Bash
    Steps:
      1. Convert RaftError::NotLeader → tonic::Status — verify code is UNAVAILABLE
      2. Convert StorageError::NotFound → tonic::Status — verify code is NOT_FOUND
      3. Convert NetworkError::Timeout → tonic::Status — verify code is DEADLINE_EXCEEDED
    Expected Result: All error mappings correct
    Evidence: .sisyphus/evidence/task-8-error-convert.txt
  ```

  **Commit**: YES — `feat(network): gRPC server/client scaffolding with channel pool`

- [x] 9. rr-raft — RaftGroupManager (Multi-Group Lifecycle) + Tests

  **What to do**:
  - Implement `RaftGroupManager` in `crates/rr-raft/src/group_manager.rs`:
    - State: `RwLock<HashMap<GroupId, Arc<RaftNode>>>` — designed for dynamic add/remove
    - `async fn create_group(group_id, node_id, config, network_factory, storage_engine, state_machine) -> Result<Arc<RaftNode>>` — creates new Raft instance, inserts into map
    - `async fn get_group(group_id) -> Option<Arc<RaftNode>>` — lookup by ID
    - `async fn remove_group(group_id) -> Result<()>` — shutdown Raft instance + remove
    - `async fn list_groups() -> Vec<GroupId>` — enumerate all active groups
    - `async fn shutdown_all()` — gracefully stop all Raft groups
    - Each group gets isolated storage (separate column family prefix or separate MemStorage instance)
  - TDD: test create 3 groups, lookup each, remove one, verify removed, list remaining
  - Test concurrent access: multiple tasks read/write group manager simultaneously

  **Must NOT do**: No automatic group creation from config (that's T14). No networking (groups use test network).

  **Recommended Agent Profile**:
  - **Category**: `deep` — Core multi-raft architecture, thread-safety critical
  - **Skills**: []

  **Parallelization**: Wave 3 (with T10-T13) | Blocks: T10,T11,T14 | Blocked By: T7 (single Raft group must work first)

  **References**:
  - fleetfs `LocalRaftGroupManager`: `HashMap<u16, RaftNode>` pattern with `lookup_by_inode()` routing
  - TiKV `StoreMeta`: `HashMap<u64, Region>` — region registry pattern
  - Qdrant `ShardReplicaSet`: manages multiple shard replicas with lifecycle states

  **Acceptance Criteria**:
  - [ ] `cargo test -p rr-raft` → PASS (multi-group CRUD, concurrent access)
  - [ ] 3 groups can be created and each can independently propose+apply entries

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Multi-group CRUD lifecycle
    Tool: Bash
    Steps:
      1. Create RaftGroupManager
      2. create_group(1), create_group(2), create_group(3)
      3. get_group(2) returns Some
      4. remove_group(2)
      5. get_group(2) returns None
      6. list_groups() returns [1, 3]
    Expected Result: Full lifecycle works
    Evidence: .sisyphus/evidence/task-9-group-manager.txt

  Scenario: Independent group operation
    Tool: Bash
    Steps:
      1. Create 2 groups with separate StateMachines
      2. Propose entry to group 1
      3. Verify only group 1's StateMachine got the apply call
      4. Group 2's StateMachine remains empty
    Expected Result: Groups are fully isolated
    Evidence: .sisyphus/evidence/task-9-group-isolation.txt
  ```

  **Commit**: YES — `feat(raft): multi-raft RaftGroupManager`

- [x] 10. rr-network — RaftService gRPC Implementation (with group_id demux)

  **What to do**:
  - Implement `RaftServiceImpl` in `crates/rr-network/src/raft_service.rs` that implements the generated `RaftService` trait from proto:
    - Holds reference to `Arc<RaftGroupManager>`
    - `append_entries(request)`: extract `group_id` from request → `group_manager.get_group(group_id)` → deserialize `payload` bytes to openraft's `AppendEntriesRequest` via bincode → call `raft.append_entries(request)` → serialize response → return
    - `vote(request)`: same pattern — demux by group_id, deserialize, call raft.vote(), serialize response
    - `install_snapshot(stream)`: streaming RPC — read chunks, assemble snapshot, call raft.install_snapshot()
    - Error handling: GroupNotFound → tonic::Status::NOT_FOUND, serialization errors → INTERNAL
  - Add `tracing::instrument` on each RPC handler with `group_id` as span field
  - TDD: test with mock RaftGroupManager — verify correct group is looked up, payload is deserialized correctly, errors propagate

  **Must NOT do**: No client request handling (that's T11). No connection pooling (that's done in T12).

  **Recommended Agent Profile**:
  - **Category**: `deep` — Core Raft RPC handling, correctness critical
  - **Skills**: []

  **Parallelization**: Wave 3 (with T9,T11-T13) | Blocks: T14,T19 | Blocked By: T8(gRPC scaffold), T9(GroupManager)

  **References**:
  - robustmq RPC pattern: bincode serialize openraft messages into proto `bytes` fields
  - openraft gRPC example: `examples/raft-kv-memstore/` — shows AppendEntries/Vote RPC handling
  - openraft `Raft::append_entries()`, `Raft::vote()` — the actual methods to call on Raft instance

  **Acceptance Criteria**:
  - [ ] `cargo test -p rr-network` → PASS (RaftService handler tests)
  - [ ] AppendEntries RPC correctly demuxes by group_id

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: AppendEntries demuxes to correct Raft group
    Tool: Bash
    Steps:
      1. Setup RaftServiceImpl with GroupManager containing group 1 and group 2
      2. Send AppendEntriesRequest with group_id=1
      3. Verify group 1's Raft received the request
      4. Send request with group_id=999 (non-existent)
      5. Verify NOT_FOUND status returned
    Expected Result: Correct routing + error handling
    Evidence: .sisyphus/evidence/task-10-raft-service.txt
  ```

  **Commit**: YES — `feat(network): RaftService gRPC implementation with group_id demux`

- [x] 11. rr-network — ClientService gRPC Implementation + Leader Forwarding

  **What to do**:
  - Implement `ClientServiceImpl` in `crates/rr-network/src/client_service.rs`:
    - Holds: `Arc<RaftGroupManager>`, `Arc<ShardRouter>` (from rr-router, or trait reference), `Arc<ChannelPool>`
    - `request(ClientRequest)`: extract `partition_id` → router.route(partition_id) → get group_id → get RaftNode → check if local node is leader:
      - If leader: `raft.client_write(payload)` → return response
      - If not leader: forward request to leader via ChannelPool → return forwarded response. Include `LeaderInfo` in response for client optimization.
      - If no leader known: return UNAVAILABLE with retry hint
    - `get_cluster_info()`: iterate all groups, collect GroupInfo (group_id, leader, replicas)
  - Add `tracing::instrument` on handlers with `partition_id`, `group_id` as span fields
  - Add `metrics::histogram!("client_request_duration")` timing
  - TDD: test leader path (direct write), follower path (forwarding), no-leader path (error)

  **Must NOT do**: No authentication. No client SDK.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` — Request routing + leader forwarding logic
  - **Skills**: []

  **Parallelization**: Wave 3 (with T9,T10,T12,T13) | Blocks: T14,T17 | Blocked By: T8(gRPC), T9(GroupManager)

  **References**:
  - TiKV leader forwarding: follower receives request → checks leader → forwards
  - openraft `Raft::client_write()`: the method to propose a client entry to Raft

  **Acceptance Criteria**:
  - [ ] `cargo test -p rr-network` → PASS (leader write, forwarding, no-leader error)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Leader handles client write directly
    Tool: Bash
    Steps:
      1. Setup single-node cluster (node is leader)
      2. Send ClientRequest with partition_id=1
      3. Verify response.success == true
      4. Verify StateMachine::apply() received the payload
    Expected Result: Direct write succeeds
    Evidence: .sisyphus/evidence/task-11-leader-write.txt

  Scenario: Non-leader returns leader info
    Tool: Bash
    Steps:
      1. Setup node that is NOT leader for group
      2. Send ClientRequest
      3. Verify response includes LeaderInfo with correct node_id and addr
    Expected Result: Client gets leader redirect info
    Evidence: .sisyphus/evidence/task-11-leader-redirect.txt
  ```

  **Commit**: YES — `feat(network): ClientService with leader forwarding`

- [x] 12. rr-network — RaftNetworkFactory + Connection Pooling

  **What to do**:
  - Implement `GrpcNetworkFactory` in `crates/rr-network/src/network_factory.rs`:
    - Implements openraft's `RaftNetworkFactory<TypeConfig>` trait
    - `new_client(target_node_id, target_node)`: return `GrpcNetworkConnection` wrapping a tonic `RaftServiceClient` using ChannelPool to get/reuse channel
  - Implement `GrpcNetworkConnection` in same file:
    - Implements openraft's `RaftNetwork<TypeConfig>` (or `RaftNetworkV2`)
    - `append_entries(rpc)`: serialize to bincode bytes → wrap in proto AppendEntriesRequest with group_id → send via tonic client → deserialize response
    - `vote(rpc)`: same pattern
    - `install_snapshot(vote, snapshot, cancel, option)`: stream snapshot chunks via tonic
    - All RPCs include `group_id` in request (stored in GrpcNetworkConnection from factory context)
  - `group_id` is passed through factory context — each `GrpcNetworkConnection` knows which group it serves
  - TDD: test with real tonic server — verify messages arrive correctly, channel reuse works

  **Must NOT do**: No per-request channel creation (must reuse via pool)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` — openraft trait implementation + tonic integration
  - **Skills**: []

  **Parallelization**: Wave 3 (with T9-T11,T13) | Blocks: T14 | Blocked By: T7(TypeConfig), T8(channel pool)

  **References**:
  - openraft `RaftNetworkFactory` trait: factory pattern for per-target-node connections
  - openraft gRPC example: `NetworkConnection` wrapping tonic client
  - robustmq: bincode serialize → proto bytes pattern

  **Acceptance Criteria**:
  - [ ] `cargo test -p rr-network` → PASS (network factory creates connections, messages round-trip)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Network factory creates working connections
    Tool: Bash
    Steps:
      1. Start RrGrpcServer with RaftServiceImpl
      2. Create GrpcNetworkFactory with ChannelPool
      3. Call new_client(node_1, addr_1)
      4. Use returned connection to send a Vote request
      5. Verify server received and processed the request
    Expected Result: End-to-end RPC works through factory
    Evidence: .sisyphus/evidence/task-12-network-factory.txt
  ```

  **Commit**: YES — `feat(network): RaftNetworkFactory with gRPC connection pooling`

- [x] 13. rr-router — ShardRouter (ConfigProvider → RoutingTable → GroupId) + Tests

  **What to do**:
  - Implement `ShardRouter` in `crates/rr-router/src/router.rs`:
    - Holds: `Arc<dyn ConfigProvider>`, cached `RwLock<RoutingTable>`
    - `async fn start(&self)` — load initial routing table from ConfigProvider, spawn background task to watch_changes() and update cache
    - `fn route(&self, partition_id: PartitionId) -> Result<GroupId, RouterError>` — lookup partition_id in cached RoutingTable:
      - Check SingleId matches: exact match on partition_id
      - Check Range matches: partition_id falls within [start, end)
      - Return GroupId if found, RouterError::PartitionNotFound if not
    - `fn get_group_meta(&self, group_id: GroupId) -> Result<GroupMeta, RouterError>` — get leader + replica info for a group
    - `fn get_leader_addr(&self, group_id: GroupId) -> Result<NodeAddr, RouterError>` — get leader's gRPC address
  - Route lookup should be O(1) for SingleId, O(log n) for Range (use BTreeMap for ranges)
  - TDD: test routing for single IDs, ranges, missing partitions, dynamic config update via watch

  **Must NOT do**: No consistent hashing, no hash rings, no smart algorithms. ONLY table lookup.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` — Routing logic with caching + background refresh
  - **Skills**: []

  **Parallelization**: Wave 3 (with T9-T12) | Blocks: T14,T17 | Blocked By: T3(traits), T6(MemConfigProvider)

  **References**:
  - TiKV StoreMeta: `BTreeMap<Vec<u8>, u64>` for range-based routing (key → region_id)
  - Snapchain ShardRouter: `route_fid(fid, num_shards)` — simpler hash-based routing reference

  **Acceptance Criteria**:
  - [ ] `cargo test -p rr-router` → PASS (single ID, range, missing, dynamic update)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Route single ID and range partitions
    Tool: Bash
    Steps:
      1. Configure: SingleId(42)→Group1, Range{1,100}→Group2
      2. route(42) == Group1
      3. route(50) == Group2 (falls in range)
      4. route(200) == PartitionNotFound error
    Expected Result: Correct routing for all cases
    Evidence: .sisyphus/evidence/task-13-router.txt

  Scenario: Dynamic config update
    Tool: Bash
    Steps:
      1. Start router with initial config (no partition for ID=500)
      2. route(500) returns PartitionNotFound
      3. Add SingleId(500)→Group3 via MemConfigProvider
      4. Wait for watch notification to propagate
      5. route(500) now returns Group3
    Expected Result: Router picks up config changes dynamically
    Evidence: .sisyphus/evidence/task-13-dynamic-route.txt
  ```

  **Commit**: YES — `feat(router): ShardRouter with ConfigProvider-backed routing table`

- [x] 14. rr-server — RrNode (Node Lifecycle: startup → config load → create groups → serve) + Tests

  **What to do**:
  - Create `crates/rr-server/src/node.rs` — `RrNode` struct:
    - Holds: `Arc<RaftGroupManager>`, `Arc<ShardRouter>`, `Arc<dyn ConfigProvider>`, `Arc<dyn StorageEngine>`, `NodeId`, `NodeAddr`
    - `pub async fn new(config: NodeConfig) -> Result<Self, RrError>` — assemble from pre-built components:
      1. Extract `storage_engine` and `config_provider` from NodeConfig (already constructed by caller)
      2. Create RaftGroupManager (inject storage_engine, state_machine_factory from NodeConfig)
      3. Create ShardRouter (inject config_provider)
      4. Return assembled RrNode
    - `pub async fn start(&self) -> Result<(), RrError>` — full startup sequence:
      1. Load routing table from ConfigProvider via ShardRouter::start()
      2. For each group assigned to this node: create Raft group via RaftGroupManager
      3. Start gRPC server (RaftService + ClientService) on configured address
      4. Log startup complete with tracing
    - `pub async fn shutdown(&self) -> Result<(), RrError>` — graceful shutdown:
      1. Stop accepting new requests
      2. Shutdown all Raft groups (flush pending)
      3. Stop gRPC server
      4. Log shutdown complete
  - Create `crates/rr-server/src/config.rs` — `NodeConfig` struct:
    - `node_id: NodeId`
    - `listen_addr: String`
    - `storage_engine: Arc<dyn StorageEngine>`
    - `config_provider: Arc<dyn ConfigProvider>`
    - `state_machine_factory: Box<dyn Fn(GroupId) -> Box<dyn StateMachine> + Send + Sync>` — factory closure, because each Raft group needs its own StateMachine instance
    - Note: `state_machine_factory` is how users inject their business logic. For high-level Service users, wrap with `ServiceStateMachineAdapter`: `|group_id| Box::new(ServiceStateMachineAdapter::new(MyService::new(group_id)))`. For low-level StateMachine users, return the StateMachine directly.
  - Create `crates/rr-server/src/lib.rs` — re-export `RrNode`, `NodeConfig`
  - TDD:
    - Test node construction with in-memory storage + in-memory config
    - Test startup sequence: verify groups are created for assigned partitions
    - Test shutdown: verify clean exit (no panics, resources released)
    - Test error handling: startup with invalid config → proper error, not panic

  **Must NOT do**:
  - No global state / static singletons — all state owned by RrNode
  - No hardcoded addresses or ports — everything from NodeConfig
  - No automatic cluster discovery — node is told what groups it owns via ConfigProvider
  - No tokio::spawn that leaks without JoinHandle tracking — all background tasks must be tracked for shutdown

  **Recommended Agent Profile**:
  - **Category**: `deep` — Complex lifecycle management, error handling across async boundaries, resource cleanup
  - **Skills**: []

  **Parallelization**: Wave 4 (with T15, T16) | Blocks: T17, T19 | Blocked By: T9(RaftGroupManager), T10(RaftService), T11(ClientService), T12(NetworkFactory), T13(ShardRouter)

  **References**:

  **Pattern References**:
  - `crates/rr-raft/src/group_manager.rs` (T9) — RaftGroupManager API: create_group(), remove_group(), get_group()
  - `crates/rr-network/src/server.rs` (T10/T11) — RrGrpcServer setup, RaftServiceImpl, ClientServiceImpl
  - `crates/rr-router/src/router.rs` (T13) — ShardRouter::start(), route()
  - `crates/rr-config/src/mem.rs` (T6) — MemConfigProvider usage pattern
  - `crates/rr-storage/src/mem.rs` (T5) — MemStorage usage pattern

  **API/Type References**:
  - `crates/rr-core/src/types.rs` (T2) — NodeId, GroupId, NodeAddr types
  - `crates/rr-core/src/state_machine.rs` (T3) — StateMachine trait, `crates/rr-core/src/storage.rs` (T3) — StorageEngine, `crates/rr-core/src/config.rs` (T3) — ConfigProvider
  - `crates/rr-core/src/error.rs` (T2) — RrError variants (for startup/shutdown error handling)

  **External References**:
  - openraft `Raft::shutdown()` API — https://docs.rs/openraft/latest/openraft/struct.Raft.html#method.shutdown
  - tonic `Server::serve_with_shutdown()` — https://docs.rs/tonic/latest/tonic/transport/struct.Server.html

  **WHY Each Reference Matters**:
  - RaftGroupManager: RrNode delegates all Raft group lifecycle to this — need to know create_group() signature and what it returns
  - RrGrpcServer: RrNode starts the gRPC server — need to know how to wire RaftServiceImpl + ClientServiceImpl
  - ShardRouter: RrNode starts the router — need to call start() before creating groups, route() during serving
  - Core types: NodeConfig uses these types; error handling must return proper RrError variants

  **Acceptance Criteria**:
  - [ ] `cargo build -p rr-server` → compiles with zero warnings
  - [ ] `cargo test -p rr-server` → PASS (construction, startup, shutdown, error handling)
  - [ ] `cargo clippy -p rr-server -- -D warnings` → exit 0

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Node startup with in-memory components
    Tool: Bash
    Steps:
      1. cargo test -p rr-server -- --test node_startup --nocapture
      2. Verify test creates NodeConfig with MemStorage + MemConfigProvider
      3. Verify RrNode::new() succeeds
      4. Verify RrNode::start() creates Raft groups matching ConfigProvider assignments
      5. Verify tracing output shows "Node {id} started" log
    Expected Result: Node starts, creates assigned groups, logs startup
    Evidence: .sisyphus/evidence/task-14-node-startup.txt

  Scenario: Node graceful shutdown
    Tool: Bash
    Steps:
      1. cargo test -p rr-server -- --test node_shutdown --nocapture
      2. Start a node, then call shutdown()
      3. Verify all Raft groups stopped (get_group() returns None after shutdown)
      4. Verify gRPC server port released (no address-in-use)
      5. Verify no panic during shutdown sequence
    Expected Result: Clean shutdown, all resources released
    Evidence: .sisyphus/evidence/task-14-node-shutdown.txt

  Scenario: Node startup with invalid config
    Tool: Bash
    Steps:
      1. cargo test -p rr-server -- --test node_invalid_config --nocapture
      2. Pass NodeConfig with empty listen_addr
      3. Verify RrNode::new() or start() returns Err(RrError::Config(...))
      4. Verify no panic
    Expected Result: Proper error propagation, not panic
    Evidence: .sisyphus/evidence/task-14-node-invalid-config.txt
  ```

  **Commit**: YES — `feat(server): RrNode lifecycle manager with startup/shutdown`

- [x] 15. Observability — tracing instrumentation + metrics across all crates + Tests

  **What to do**:
  - Add `tracing` dependency to all library crates (rr-core re-exports, others use)
  - Add `metrics` + `metrics-exporter-prometheus` to rr-server (metrics endpoint is server-level)
  - Instrument key operations with tracing spans and metrics:
    - `rr-raft`: `#[instrument]` on RaftGroupManager::create_group(), RaftGroupManager::propose()
      - Span fields: group_id, node_id
      - Counters: `rr_raft_proposals_total` (group_id label), `rr_raft_elections_total`
      - Histograms: `rr_raft_proposal_duration_seconds`
    - `rr-network`: `#[instrument]` on RaftService::append_entries(), Vote, Snapshot handlers
      - Span fields: group_id, from_node, rpc_type
      - Counters: `rr_grpc_requests_total` (method, status labels)
      - Histograms: `rr_grpc_request_duration_seconds`
    - `rr-router`: `#[instrument]` on ShardRouter::route()
      - Span fields: partition_id, result_group_id
      - Counters: `rr_route_lookups_total`, `rr_route_misses_total`
    - `rr-storage`: `#[instrument]` on StorageEngine put/get/delete ops
      - Span fields: cf_name, operation
      - Histograms: `rr_storage_operation_duration_seconds`
    - `rr-server`: Initialize `tracing_subscriber` (fmt layer + env filter) in RrNode::start()
      - Initialize `metrics_exporter_prometheus::PrometheusBuilder` — expose `/metrics` endpoint
  - TDD:
    - Test that tracing spans are emitted (use `tracing_test` crate or custom subscriber)
    - Test that metrics counters increment after operations
    - Test that PrometheusBuilder exports metrics text format

  **Must NOT do**:
  - No `println!` or `eprintln!` — ALL output through tracing
  - No custom logging framework — tracing only
  - No metrics in rr-core (rr-core 的依赖限于 thiserror, serde, bincode, async-trait 等基础 crate，不加 tracing/metrics)
  - No blocking metrics collection — all async-compatible

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` — Cross-cutting concern touching multiple crates, but each change is small (add instrument macro + counter)
  - **Skills**: []

  **Parallelization**: Wave 4 (with T14, T16) | Blocks: T17, T19 | Blocked By: T7(rr-raft exists), T8(rr-network exists)

  **References**:

  **Pattern References**:
  - `crates/rr-raft/src/group_manager.rs` (T9) — Functions to add `#[instrument]` to
  - `crates/rr-network/src/service.rs` (T10) — gRPC handlers to instrument
  - `crates/rr-router/src/router.rs` (T13) — route() to instrument
  - `crates/rr-storage/src/mem.rs` (T5) — Storage ops to instrument

  **External References**:
  - tracing crate: https://docs.rs/tracing/latest/tracing/ — `#[instrument]` macro reference
  - metrics crate: https://docs.rs/metrics/latest/metrics/ — counter!, histogram! macros
  - metrics-exporter-prometheus: https://docs.rs/metrics-exporter-prometheus/ — PrometheusBuilder setup
  - tracing-subscriber: https://docs.rs/tracing-subscriber/ — fmt::Layer + EnvFilter setup

  **WHY Each Reference Matters**:
  - Pattern refs show WHERE to add instrumentation — which functions, what span fields make sense
  - External refs needed for correct API usage — `#[instrument(skip(self), fields(group_id))]` syntax, `counter!()` macro syntax

  **Acceptance Criteria**:
  - [ ] `cargo build --workspace` → compiles with tracing/metrics deps
  - [ ] `cargo test --workspace` → all existing tests still pass (tracing adds zero behavioral changes)
  - [ ] `cargo test -p rr-server -- --test metrics` → metrics counter increments verified
  - [ ] Zero `println!` in any library crate (`grep -r 'println!' crates/ → empty`)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Tracing spans emitted during operations
    Tool: Bash
    Steps:
      1. cargo test --workspace -- --test tracing --nocapture 2>&1 | tee output.txt
      2. Verify output contains span fields: group_id, node_id in raft operations
      3. Verify output contains span fields: partition_id in routing
      4. Verify output contains span fields: method in gRPC handlers
    Expected Result: All instrumented operations emit structured tracing spans
    Evidence: .sisyphus/evidence/task-15-tracing.txt

  Scenario: Metrics counters increment
    Tool: Bash
    Steps:
      1. Write a test that: creates RaftGroupManager, proposes an entry, reads metrics
      2. cargo test -p rr-server -- --test metrics_counter --nocapture
      3. Verify rr_raft_proposals_total counter > 0 after proposal
      4. Verify rr_grpc_requests_total counter > 0 after RPC
    Expected Result: Metrics counters track operations accurately
    Evidence: .sisyphus/evidence/task-15-metrics.txt

  Scenario: No println in library crates
    Tool: Bash
    Steps:
      1. grep -rn 'println!' crates/ --include='*.rs'
      2. grep -rn 'eprintln!' crates/ --include='*.rs'
      3. Verify both commands return empty (exit code 1 = no matches)
    Expected Result: Zero print statements in library code
    Evidence: .sisyphus/evidence/task-15-no-println.txt
  ```

  **Commit**: YES — `feat(observability): add tracing spans and metrics counters across all crates`

- [x] 16. rr-raft — Service Trait Adapter (高层 Service → StateMachine Bridge) + Tests

  **What to do**:
  - Create `crates/rr-raft/src/service_adapter.rs` — `ServiceStateMachineAdapter<S: Service>`:
    - Purpose: Wraps a user's `Service` implementation to make it work as the framework's `StateMachine`
    - Implements the framework's internal StateMachine trait (the one openraft calls)
    - `fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError>`:
      1. Call `self.service.handle(entry.to_vec())` — pass raw bytes directly to the user's Service
      2. Return the result bytes from Service::handle()
      Note: The adapter does NOT interpret entry contents. The Service decides its own serialization format.
    - `fn snapshot(&self) -> Result<Vec<u8>, RrError>` — delegate to `self.service.snapshot()`
    - `fn restore(&mut self, data: &[u8]) -> Result<(), RrError>` — delegate to `self.service.restore(data)`
    - `fn last_applied(&self) -> Option<LogIndex>` — track internally (increment on each apply())
  - The adapter is a thin pass-through: no serialization/deserialization logic, no ServiceRequest envelope.
    Users define their own request/response formats inside their Service::handle() implementation.
  - Optionally define helper types `KvRequest`/`KvResponse` in the kv-store example (NOT in rr-core) to show
    users how to structure their request/response formats with serde.
  - The adapter is what gets passed to RaftGroupManager when user registers a Service (not a raw StateMachine)
  - In `crates/rr-server/src/node.rs` (or config), the `state_machine_factory` can now accept either:
    - `Box<dyn StateMachine>` (low-level users)
    - `Box<dyn Service>` wrapped in `ServiceStateMachineAdapter` (high-level users)
  - TDD:
    - Test adapter with a mock Service: apply entry bytes → service.handle() called with same bytes
    - Test snapshot delegation: adapter.snapshot() → service.snapshot() called, bytes forwarded
    - Test restore delegation: adapter.restore(data) → service.restore(data) called
    - Test error propagation: service returns error → adapter returns error (not panic)
    - Test last_applied tracking: starts at None, increments on each apply()

  **Must NOT do**:
  - No `unsafe` code
  - No additional trait methods beyond what Service defines — adapter is a thin bridge
  - No business logic in the adapter — pure delegation, zero interpretation of entry bytes
  - Do NOT modify the StateMachine trait in rr-core (it's frozen from T3)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` — Well-scoped adapter pattern, serialization + delegation
  - **Skills**: []

  **Parallelization**: Wave 4 (with T14, T15) | Blocks: T17 | Blocked By: T3(Service + StateMachine traits), T7(openraft TypeConfig)

  **References**:

  **Pattern References**:
  - `crates/rr-core/src/service.rs` (T3) — Service trait definition: `handle()` signature
  - `crates/rr-core/src/state_machine.rs` (T3) — StateMachine trait definition: `apply()`, `snapshot()`, `restore()`
  - `crates/rr-core/src/types.rs` (T2) — LogIndex type for last_applied tracking
  - `crates/rr-raft/src/sm_adapter.rs` (T7) — The openraft RaftStateMachineAdapter impl that adapter must match

  **API/Type References**:
  - `rr-core::RrError` (T2) — Error variants that Service::handle() may return

  **External References**:
  - Adapter pattern in Rust: wrapping trait A to implement trait B — standard newtype pattern
  - No external serialization libraries needed — adapter passes raw bytes through

  **WHY Each Reference Matters**:
  - Service trait: need exact handle() signature to call correctly
  - StateMachine trait: need exact apply()/snapshot()/restore() to implement correctly
  - RrError: adapter must propagate errors correctly without wrapping or losing type information
  - T7 state_machine: see how openraft's RaftStateMachine is connected — adapter must produce compatible output

  **Acceptance Criteria**:
  - [ ] `cargo build -p rr-raft` → compiles with service_adapter module
  - [ ] `cargo test -p rr-raft -- --test service_adapter` → PASS (apply, snapshot, restore, error propagation)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Service adapter delegates apply() correctly
    Tool: Bash
    Steps:
      1. cargo test -p rr-raft -- --test service_adapter_apply --nocapture
      2. Test creates a MockService that records calls to handle()
      3. Adapter.apply(b"some_request_bytes")
      4. Verify MockService::handle() was called with request == b"some_request_bytes"
      5. Verify adapter returns MockService's response bytes unchanged
    Expected Result: Adapter passes bytes through to Service::handle() without modification
    Evidence: .sisyphus/evidence/task-16-adapter-apply.txt

  Scenario: Error propagation through adapter
    Tool: Bash
    Steps:
      1. cargo test -p rr-raft -- --test service_adapter_error --nocapture
      2. MockService::handle() returns Err(RrError::Application("not found"))
      3. Adapter.apply() returns Err(RrError::Application("not found"))
      4. Verify error type and message are preserved, not wrapped or lost
    Expected Result: Errors pass through adapter without transformation
    Evidence: .sisyphus/evidence/task-16-adapter-error.txt

  Scenario: Snapshot and restore delegation
    Tool: Bash
    Steps:
      1. cargo test -p rr-raft -- --test service_adapter_snapshot --nocapture
      2. MockService::snapshot() returns Ok(b"snap_data")
      3. Verify adapter.snapshot() returns b"snap_data"
      4. Call adapter.restore(b"snap_data"), verify MockService::restore() was called with same bytes
    Expected Result: Snapshot/restore delegated transparently
    Evidence: .sisyphus/evidence/task-16-adapter-snapshot.txt

  Scenario: last_applied tracking
    Tool: Bash
    Steps:
      1. cargo test -p rr-raft -- --test service_adapter_last_applied --nocapture
      2. Create adapter, verify last_applied() returns None
      3. Call apply() three times with different data
      4. Verify last_applied() returns Some(3)
    Expected Result: Internal counter tracks applied entries
    Evidence: .sisyphus/evidence/task-16-adapter-last-applied.txt

  **Commit**: YES — `feat(raft): ServiceStateMachineAdapter bridging high-level Service to StateMachine`

- [x] 17. examples/kv-store — KV Store Service + StateMachine Implementation + Tests

  **What to do**:
  - Create `examples/kv-store/` as a Cargo binary crate within the workspace:
    - `examples/kv-store/Cargo.toml` — depends on rr-server, rr-core, rr-storage, rr-config, rr-raft
    - `examples/kv-store/src/main.rs` — entry point (defer to T18)
    - `examples/kv-store/src/service.rs` — `KvService` implementing `Service` trait:
      - `handle(request: Vec<u8>) -> Result<Vec<u8>, RrError>`:
        - Internally defines `KvRequest` enum (in `examples/kv-store/src/types.rs`): `Get { key }`, `Put { key, value }`, `Delete { key }`, `Scan { prefix }`
        - Deserializes request bytes → `KvRequest` via bincode
        - Matches on variant:
          - `Get { key }` → lookup in HashMap, return value bytes (or NotFound error)
          - `Put { key, value }` → insert into HashMap, return Ok
          - `Delete { key }` → remove from HashMap, return Ok
          - `Scan { prefix }` → iterate matching entries, return serialized Vec<(key,value)>
        - Invalid deserialization → return Err(RrError::Codec(DeserializeFailed))
      - `snapshot() -> Result<Vec<u8>, RrError>` — serialize entire HashMap via bincode
      - `restore(data: &[u8]) -> Result<(), RrError>` — deserialize HashMap, replace state
    - `examples/kv-store/src/state_machine.rs` — `KvStateMachine` implementing `StateMachine` trait directly:
      - Same underlying HashMap<Vec<u8>, Vec<u8>> storage
      - Shows low-level API usage (alternative to KvService)
      - `apply(entry: &[u8]) -> Result<Vec<u8>, RrError>` — direct entry processing
      - `snapshot()` / `restore()` — same as KvService
  - Both implementations use `HashMap<Vec<u8>, Vec<u8>>` as the in-memory KV store
  - TDD:
    - Test KvService: get/put/delete/scan operations via handle()
    - Test KvStateMachine: apply entries, snapshot/restore round-trip
    - Test both produce identical results for same operations
    - Test error cases: get missing key, invalid payload deserialization

  **Must NOT do**:
  - No persistence to disk — in-memory only (storage engine handles persistence)
  - No custom networking — uses framework's gRPC layer
  - No complex data structures — simple HashMap demonstration
  - No authentication or access control

  **Recommended Agent Profile**:
  - **Category**: `deep` — Demonstrates correct framework usage patterns; must serve as reference implementation for users
  - **Skills**: []

  **Parallelization**: Wave 5 (with T18, T19, T20) | Blocks: T18, T19 | Blocked By: T14(RrNode), T15(observability), T16(ServiceAdapter)

  **References**:

  **Pattern References**:
  - `crates/rr-core/src/service.rs` (T3) — The Service trait KvService must implement
  - `crates/rr-core/src/state_machine.rs` (T3) — The StateMachine trait KvStateMachine must implement
  - `crates/rr-raft/src/service_adapter.rs` (T16) — How ServiceStateMachineAdapter wraps KvService (passes raw bytes through to handle())
  - (no ServiceRequest in rr-core — KvRequest/KvResponse are defined locally in examples/kv-store/src/types.rs)

  **API/Type References**:
  - `rr-core::Codec` (T3) — BincodeCodec for serializing/deserializing KV payloads
  - `rr-core::RrError` (T2) — Error variants for KV operations

  **External References**:
  - bincode for HashMap serialization: https://docs.rs/bincode/latest/bincode/
  - serde derive for request/response types

  **WHY Each Reference Matters**:
  - Service trait: KvService is the PRIMARY example of how users implement this trait — must follow exact signature
  - StateMachine trait: KvStateMachine shows low-level alternative — demonstrates both paths
  - ServiceAdapter: must verify KvService works through the adapter — adapter passes raw bytes, KvService deserializes KvRequest internally
  - KvRequest: KvService::handle() deserializes KvRequest from the raw bytes — this is the example's own format, not a framework type

  **Acceptance Criteria**:
  - [ ] `cargo build -p kv-store` → compiles
  - [ ] `cargo test -p kv-store` → PASS (get, put, delete, scan, snapshot/restore, error cases)
  - [ ] Both KvService and KvStateMachine tested

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: KvService CRUD operations
    Tool: Bash
    Steps:
      1. cargo test -p kv-store -- --test kv_service_crud --nocapture
      2. put("key1", "value1") via Service::handle(serialize(KvRequest::Put{key:"key1", value:"value1"}))
      3. get("key1") returns "value1"
      4. put("key1", "value2") (overwrite)
      5. get("key1") returns "value2"
      6. delete("key1")
      7. get("key1") returns KeyNotFound error or empty
    Expected Result: Full CRUD cycle works correctly
    Evidence: .sisyphus/evidence/task-17-kv-crud.txt

  Scenario: Snapshot and restore round-trip
    Tool: Bash
    Steps:
      1. cargo test -p kv-store -- --test kv_snapshot_restore --nocapture
      2. put 3 keys: a=1, b=2, c=3
      3. Take snapshot → bytes
      4. Create fresh KvService
      5. Restore from snapshot bytes
      6. get(a) == 1, get(b) == 2, get(c) == 3
    Expected Result: State perfectly preserved through snapshot/restore
    Evidence: .sisyphus/evidence/task-17-kv-snapshot.txt

  Scenario: Invalid operation handling
    Tool: Bash
    Steps:
      1. cargo test -p kv-store -- --test kv_error_cases --nocapture
      2. handle("unknown_method", payload) → Err(RrError::Application(...))
      3. handle("get", invalid_bytes) → Err (deserialization failure)
    Expected Result: Graceful error handling, no panics
    Evidence: .sisyphus/evidence/task-17-kv-errors.txt
  ```

  **Commit**: YES — `feat(example): KV Store Service and StateMachine implementations`

- [x] 18. examples/kv-store — KV Store gRPC Client + Demo Runner + Tests

  **What to do**:
  - Create `examples/kv-store/src/client.rs` — `KvClient`:
    - Wraps a tonic gRPC client connected to rr-network's ClientService
    - `pub async fn new(addr: &str) -> Result<Self, RrError>` — connect to a node
    - `pub async fn put(&self, key: &str, value: &str) -> Result<(), RrError>` — serialize put request, send via ClientService, handle leader redirect if needed
    - `pub async fn get(&self, key: &str) -> Result<Option<String>, RrError>` — serialize get request
    - `pub async fn delete(&self, key: &str) -> Result<(), RrError>`
    - `pub async fn scan(&self, prefix: &str) -> Result<Vec<(String, String)>, RrError>`
    - Leader forwarding: if ClientService returns "not leader, leader is X", reconnect to X and retry
  - Complete `examples/kv-store/src/main.rs` — demo runner:
    - Parse CLI args: `--node-id N --addr HOST:PORT --peers PEER1,PEER2,...`
    - Start as server mode: `kv-store serve --node-id 1 --addr 0.0.0.0:50051 --peers 2=0.0.0.0:50052,3=0.0.0.0:50053`
      - Creates RrNode with MemStorage + MemConfigProvider + KvService
      - Configures single shard group with all peers
      - Starts serving
    - Client mode: `kv-store client --addr 0.0.0.0:50051 put key1 value1`
      - Creates KvClient, executes command, prints result
  - TDD:
    - Test KvClient serialization: verify correct proto messages generated
    - Test main arg parsing
    - Note: full integration (actual gRPC calls) tested in T19/T20

  **Must NOT do**:
  - No TUI or interactive REPL — simple CLI commands
  - No retry logic beyond leader forwarding — keep simple for demo
  - No connection pooling in client — single connection (framework handles pooling internally)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high` — CLI + gRPC client wrapper, well-scoped
  - **Skills**: []

  **Parallelization**: Wave 5 (with T17, T19, T20) | Blocks: T19 | Blocked By: T17(KvService/KvStateMachine)

  **References**:

  **Pattern References**:
  - `crates/rr-network/src/client.rs` (T8/T11) — ClientService proto definition + tonic client stubs
  - `crates/rr-network/proto/client.proto` (T4) — ClientRequest/ClientResponse message definitions
  - `examples/kv-store/src/service.rs` (T17) — KvService request/response format to match

  **API/Type References**:
  - `examples/kv-store/src/types.rs` (T17) — `KvRequest`/`KvResponse` enum definitions (the payload format client must serialize)
  - tonic generated client stubs from `client.proto`

  **External References**:
  - tonic client: https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html
  - clap for CLI arg parsing: https://docs.rs/clap/latest/clap/ (or use simple std::env::args if minimal)

  **WHY Each Reference Matters**:
  - ClientService proto: KvClient must send correctly formatted proto messages — exact field names and types
  - KvService format: client serializes payloads that KvService deserializes — must be compatible
  - tonic Channel: for creating gRPC connection and sending requests

  **Acceptance Criteria**:
  - [ ] `cargo build -p kv-store` → compiles (both server and client modes)
  - [ ] `cargo test -p kv-store -- --test client` → PASS (serialization, arg parsing)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: KV client serializes requests correctly
    Tool: Bash
    Steps:
      1. cargo test -p kv-store -- --test client_serialization --nocapture
      2. Create KvClient (mock connection)
      3. Call put("mykey", "myvalue") 
      4. Verify serialized payload is `bincode(KvRequest::Put{key:"mykey", value:"myvalue"})`, NOT a ServiceRequest envelope
    Expected Result: Client produces correctly formatted requests
    Evidence: .sisyphus/evidence/task-18-client-serialize.txt

  Scenario: CLI arg parsing
    Tool: Bash
    Steps:
      1. cargo test -p kv-store -- --test cli_args --nocapture
      2. Parse: serve --node-id 1 --addr 0.0.0.0:50051 --peers 2=addr2,3=addr3
      3. Verify node_id=1, addr="0.0.0.0:50051", peers=[(2,addr2),(3,addr3)]
      4. Parse: client --addr 0.0.0.0:50051 put key1 value1
      5. Verify command=put, key=key1, value=value1
    Expected Result: Both serve and client subcommands parse correctly
    Evidence: .sisyphus/evidence/task-18-cli-args.txt
  ```

  **Commit**: YES — `feat(example): KV Store client CLI and demo runner`

- [x] 19. Integration Tests — Multi-Node Cluster Test Harness + Basic Tests

  **What to do**:
  - Create `tests/integration/` directory structure:
    - `tests/integration/Cargo.toml` — integration test crate (depends on kv-store, rr-server, rr-core, etc.)
    - `tests/integration/src/lib.rs` — test harness re-exports
    - `tests/integration/src/cluster.rs` — `TestCluster` helper:
      - `pub async fn new(num_nodes: usize) -> Self` — create N nodes in-process:
        1. Allocate unique ports for each node (use port 0 or predictable range)
        2. Configure MemStorage + MemConfigProvider for each
        3. Set up peer list (all nodes know about each other)
        4. Create RrNode for each, but DON'T start yet
      - `pub async fn start(&self) -> Result<(), RrError>` — start all nodes concurrently
      - `pub async fn stop(&self)` — shutdown all nodes
      - `pub fn node(&self, idx: usize) -> &RrNode` — access specific node
      - `pub async fn get_leader(&self, group_id: GroupId) -> Option<NodeId>` — find current leader
      - `pub async fn kill_node(&self, idx: usize)` — stop a specific node (for failover tests)
      - `pub async fn restart_node(&self, idx: usize)` — restart a killed node
      - `pub fn client_addr(&self, idx: usize) -> String` — get gRPC address for a node
  - Create basic integration tests in `tests/integration/tests/basic.rs`:
    - `test_cluster_startup`: create 3-node cluster, verify all start without error
    - `test_leader_election`: start 3-node cluster, verify a leader is elected within timeout
    - `test_single_write_read`: write a KV pair to leader, read it back
    - `test_read_from_follower`: write via leader, read via follower (linearizable read)
  - All tests use `#[tokio::test]` with timeout
  - Each test creates its own TestCluster (isolated)

  **Must NOT do**:
  - No shared state between tests — each test gets its own cluster
  - No hardcoded ports that could conflict — use dynamic port allocation
  - No sleep-based synchronization — use polling with timeout for leader election, etc.
  - No flaky timing assumptions — use retry loops with exponential backoff for leader checks

  **Recommended Agent Profile**:
  - **Category**: `deep` — Complex async test infrastructure, multi-node coordination, timing sensitivity
  - **Skills**: []

  **Parallelization**: Wave 5 (with T17, T18, T20) | Blocks: T20 | Blocked By: T14(RrNode), T17(KV Store impl)

  **References**:

  **Pattern References**:
  - `crates/rr-server/src/node.rs` (T14) — RrNode API: new(), start(), shutdown()
  - `crates/rr-server/src/config.rs` (T14) — NodeConfig structure for test setup
  - `crates/rr-storage/src/mem.rs` (T5) — MemStorage for test clusters
  - `crates/rr-config/src/mem.rs` (T6) — MemConfigProvider for test clusters
  - `examples/kv-store/src/service.rs` (T17) — KvService as test workload

  **API/Type References**:
  - `rr-core::types` (T2) — NodeId, GroupId for cluster configuration
  - openraft `Raft::current_leader()` — for leader election checks

  **External References**:
  - openraft test patterns: https://github.com/databendlabs/openraft/tree/main/tests — how openraft's own tests create multi-node clusters
  - tokio::test with timeout: https://docs.rs/tokio/latest/tokio/attr.test.html

  **WHY Each Reference Matters**:
  - RrNode API: TestCluster wraps multiple RrNodes — must know exact construction and lifecycle methods
  - MemStorage/MemConfigProvider: tests use in-memory implementations for speed and isolation
  - openraft tests: reference for how to set up multi-node Raft tests without real network (in-process)

  **Acceptance Criteria**:
  - [ ] `cargo test -p integration-tests` → PASS (cluster startup, leader election, write/read)
  - [ ] Tests complete within 30 seconds (no hanging, proper timeouts)
  - [ ] Each test fully isolated (can run in any order)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: 3-node cluster starts and elects leader
    Tool: Bash
    Steps:
      1. cargo test -p integration-tests -- --test test_leader_election --nocapture 2>&1 | tee output.txt
      2. Verify output shows 3 nodes started
      3. Verify a leader was elected (output contains "leader elected: node X")
      4. Verify elapsed time < 10 seconds
    Expected Result: Cluster starts, leader elected within timeout
    Evidence: .sisyphus/evidence/task-19-leader-election.txt

  Scenario: Write and read KV pair through cluster
    Tool: Bash
    Steps:
      1. cargo test -p integration-tests -- --test test_single_write_read --nocapture 2>&1 | tee output.txt
      2. Write key="test_key", value="test_value" to leader node
      3. Read key="test_key" from same node
      4. Verify returned value == "test_value"
    Expected Result: Write is committed via Raft, read returns committed value
    Evidence: .sisyphus/evidence/task-19-write-read.txt

  Scenario: Test isolation verification
    Tool: Bash
    Steps:
      1. cargo test -p integration-tests --  --nocapture 2>&1 | tee output.txt
      2. Run ALL integration tests
      3. Verify all pass
      4. Verify no port conflict errors
      5. Verify total time < 30 seconds
    Expected Result: All tests pass in isolation, no interference
    Evidence: .sisyphus/evidence/task-19-all-tests.txt
  ```

  **Commit**: YES — `test(integration): multi-node cluster test harness with basic tests`

- [x] 20. Integration Tests — Leader Election, Failover, Multi-Group Routing Tests

  **What to do**:
  - Add advanced integration tests in `tests/integration/tests/advanced.rs`:
    - `test_leader_failover`:
      1. Start 3-node cluster, write key=A
      2. Identify leader, kill leader node
      3. Wait for new leader election (poll with timeout)
      4. Write key=B via new leader → succeeds
      5. Read key=A via new leader → returns original value (data preserved)
      6. Read key=B → returns new value
    - `test_node_rejoin`:
      1. Start 3-node cluster, kill one follower
      2. Write several entries while follower is down
      3. Restart follower
      4. Wait for follower to catch up (snapshot or log replication)
      5. Read from rejoined follower → all entries present
    - `test_multi_group_routing`:
      1. Configure 2 shard groups: Group1 (IDs 1-100), Group2 (IDs 101-200)
      2. Start 3-node cluster with both groups
      3. Write to partition_id=50 (routed to Group1): key="g1key", value="g1val"
      4. Write to partition_id=150 (routed to Group2): key="g2key", value="g2val"
      5. Read from partition_id=50 → "g1val" (from Group1's state machine)
      6. Read from partition_id=150 → "g2val" (from Group2's state machine)
      7. Verify Group1 does NOT have "g2key" and Group2 does NOT have "g1key" (shard isolation)
    - `test_multi_group_independent_leaders`:
      1. Configure 2 groups across 3 nodes
      2. Verify each group independently elects its own leader
      3. Leaders can be different nodes for different groups
  - All tests use TestCluster from T19
  - All tests use `#[tokio::test]` with generous timeout (failover tests need time)

  **Must NOT do**:
  - No sleep() for synchronization — use polling with timeout
  - No assumptions about which node becomes leader — test should work regardless
  - No test-specific hacks in framework code — tests use public API only

  **Recommended Agent Profile**:
  - **Category**: `deep` — Complex distributed system scenarios, timing-sensitive failover testing
  - **Skills**: []

  **Parallelization**: Wave 5 (with T17, T18, T19) | Blocks: Final | Blocked By: T19(test harness)

  **References**:

  **Pattern References**:
  - `tests/integration/src/cluster.rs` (T19) — TestCluster API: kill_node(), restart_node(), get_leader()
  - `tests/integration/tests/basic.rs` (T19) — Basic test patterns to follow
  - `crates/rr-router/src/router.rs` (T13) — ShardRouter route() for multi-group verification
  - `crates/rr-config/src/mem.rs` (T6) — MemConfigProvider for configuring multi-group routing table

  **API/Type References**:
  - `rr-core::types::PartitionConfig` (T2/T3) — SingleId and Range variants for shard config
  - `rr-core::types::GroupId` (T2) — For group identification

  **External References**:
  - openraft cluster tests: https://github.com/databendlabs/openraft/tree/main/tests — failover test patterns
  - Jepsen-style testing principles: linearizability, partition tolerance verification

  **WHY Each Reference Matters**:
  - TestCluster: all tests use this harness — must know kill/restart/get_leader API
  - ShardRouter: multi-group tests verify routing correctness — need to set up PartitionConfig correctly
  - openraft tests: reference for realistic failover scenarios (how long to wait, what to assert)

  **Acceptance Criteria**:
  - [ ] `cargo test -p integration-tests -- --test advanced` → PASS (failover, rejoin, multi-group)
  - [ ] Failover test: new leader elected within 10 seconds of kill
  - [ ] Multi-group test: shard isolation verified (no cross-group data leaks)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Leader failover preserves data
    Tool: Bash
    Steps:
      1. cargo test -p integration-tests -- --test test_leader_failover --nocapture 2>&1 | tee output.txt
      2. Verify initial write succeeds (key=A committed)
      3. Verify leader kill logged
      4. Verify new leader elected (output: "new leader: node X")
      5. Verify write to new leader succeeds (key=B committed)
      6. Verify read key=A returns original value (data survived failover)
    Expected Result: Zero data loss during leader failover
    Evidence: .sisyphus/evidence/task-20-failover.txt

  Scenario: Multi-group shard isolation
    Tool: Bash
    Steps:
      1. cargo test -p integration-tests -- --test test_multi_group_routing --nocapture 2>&1 | tee output.txt
      2. Verify write to Group1 (partition 50) succeeded
      3. Verify write to Group2 (partition 150) succeeded
      4. Verify read from Group1 returns Group1's data only
      5. Verify read from Group2 returns Group2's data only
      6. Verify cross-group isolation: Group1 has no Group2 data
    Expected Result: Each shard group has independent state
    Evidence: .sisyphus/evidence/task-20-multi-group.txt

  Scenario: Node rejoin catches up
    Tool: Bash
    Steps:
      1. cargo test -p integration-tests -- --test test_node_rejoin --nocapture 2>&1 | tee output.txt
      2. Verify follower killed
      3. Verify writes succeed with 2-node majority
      4. Verify follower restarted
      5. Verify follower caught up (read from rejoined node returns all data)
    Expected Result: Rejoined node has all committed data
    Evidence: .sisyphus/evidence/task-20-rejoin.txt
  ```

  **Commit**: YES — `test(integration): leader failover, node rejoin, and multi-group routing tests`


## Final Verification Wave (MANDATORY — after ALL implementation tasks)

>

- [x] F1. **Plan Compliance Audit** — `oracle`
  Read the plan end-to-end. For each "Must Have": verify implementation exists (read file, run `cargo test`, check output). For each "Must NOT Have": search codebase for forbidden patterns (`anyhow` in lib crates, `assert!(true)`, circular deps, RocksDB/etcd impls). Check evidence files exist in `.sisyphus/evidence/`. Compare deliverables against plan.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [N/N] | VERDICT: APPROVE/REJECT`

- [x] F2. **Code Quality Review** — `unspecified-high`
  Run `cargo clippy --workspace -- -D warnings` + `cargo fmt --all -- --check` + `cargo test --workspace`. Review all source files for: `as any` casting, empty catches/unwrap chains, `println!` in lib code, commented-out code, unused imports. Check AI slop: excessive comments, over-abstraction, generic names. Verify crate dependency DAG is acyclic.
  Output: `Build [PASS/FAIL] | Clippy [PASS/FAIL] | Fmt [PASS/FAIL] | Tests [N pass/N fail] | Files [N clean/N issues] | VERDICT`

- [x] F3. **Real Manual QA** — `unspecified-high`
  Start from clean state (`cargo clean && cargo build --workspace`). Launch KV Store example with 3 nodes. Execute EVERY QA scenario from EVERY task — follow exact steps, capture evidence. Test cross-task integration: write via node 1, read via node 2; kill leader, write via new leader; route to different shard groups. Save to `.sisyphus/evidence/final-qa/`.
  Output: `Scenarios [N/N pass] | Integration [N/N] | Edge Cases [N tested] | VERDICT`

- [x] F4. **Scope Fidelity Check** — `deep`
  For each task: read "What to do", read actual source files. Verify 1:1 — everything in spec was built, nothing beyond spec was built. Check "Must NOT do" compliance: search for forbidden patterns (consistent hashing, auth, Docker, web UI, RocksDB impl, etcd impl). Flag unaccounted files. Verify no crate exceeds its defined responsibility.
  Output: `Tasks [N/N compliant] | Contamination [CLEAN/N issues] | Unaccounted [CLEAN/N files] | VERDICT`

---

## Commit Strategy

| Wave | Commit | Message | Files |
|------|--------|---------|-------|
| 1 | 1 | `feat(workspace): scaffold cargo workspace with rr-core traits and proto definitions` | Cargo.toml, crates/rr-core/**, crates/rr-network/proto/** |
| 2 | 2 | `feat(core): add in-memory storage/config implementations and single-raft integration` | crates/rr-storage/**, crates/rr-config/**, crates/rr-raft/** (single), crates/rr-network/** (scaffold) |
| 3 | 3 | `feat(raft): multi-raft group manager with gRPC transport and shard router` | crates/rr-raft/** (multi), crates/rr-network/** (impl), crates/rr-router/** |
| 4 | 4 | `feat(server): node lifecycle, observability, and service adapter` | crates/rr-server/**, tracing/metrics additions |
| 5 | 5 | `feat(example): KV Store example with integration tests` | examples/kv-store/**, tests/** |

---

## Success Criteria

### Verification Commands
```bash
# Full build
cargo build --workspace  # Expected: Compiling rr-core, rr-storage, ... Finished

# All tests pass
cargo test --workspace  # Expected: test result: ok. N passed; 0 failed

# Clippy clean
cargo clippy --workspace -- -D warnings  # Expected: exit 0

# Format clean
cargo fmt --all -- --check  # Expected: exit 0

# KV Store example runs
cargo build --example kv-store  # Expected: Compiling ... Finished
```

### Final Checklist
- [x] All "Must Have" present and working
- [x] All "Must NOT Have" absent (verified by codebase search)
- [x] All tests pass (`cargo test --workspace`)
- [x] Clippy + fmt clean
- [x] KV Store example: 3-node cluster start, write, read, leader failover
- [x] Crate dependency DAG is acyclic
- [x] No `anyhow` in library crates
- [x] No placeholder assertions
- [x] tracing spans on all public trait implementations
- [x] metrics counters/histograms for key operations
