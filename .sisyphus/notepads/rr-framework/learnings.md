# rr-framework — Learnings

## Session: ses_367563ddaffeaTqD93zm1RA6iM | 2026-02-26T08:14:03Z

### Architecture Decisions
- Framework owns TypeConfig: `D=Vec<u8>, R=Vec<u8>` — users serialize at Service boundary, no generic explosion
- Two-layer trait design: `Service` (high-level, no Raft awareness) + `StateMachine` (low-level, full Raft control)
- `ServiceStateMachineAdapter` bridges them: passes raw bytes from Raft through to `Service::handle()`
- Router does table lookup ONLY — no consistent hashing, no smart routing
- All protobuf Raft RPC messages MUST include `group_id: u64` field for multi-Raft demux
- `LogIndex = u64` type alias defined in `rr-core/src/types.rs`
- NodeConfig takes pre-built `Arc<dyn StorageEngine>` + `Arc<dyn ConfigProvider>` instances
- `state_machine_factory: Box<dyn Fn(GroupId) -> Box<dyn StateMachine> + Send + Sync>` for per-group instances

### Library Choices
- Raft: openraft (async-native, Databend-proven) — pin exact 0.10.x version
- Transport: tonic (gRPC) + prost
- Storage: pluggable StorageEngine trait, MemStorage (BTreeMap per CF) for dev
- Config: pluggable ConfigProvider trait, MemConfigProvider for dev
- Serialization: bincode (default, via BincodeCodec), serde
- Error: thiserror (in lib crates), anyhow ONLY in examples/ and tests/
- Concurrency: tokio, dashmap (for ChannelPool + RaftGroupManager)
- Observability: tracing + metrics from day one

### Key File Locations
- `crates/rr-core/src/types.rs` — NodeId, GroupId, PartitionId, LogIndex, RoutingTable, PartitionConfig, NodeAddr, GroupMeta
- `crates/rr-core/src/error.rs` — RrError top-level enum + sub-errors
- `crates/rr-core/src/service.rs` — Service trait
- `crates/rr-core/src/state_machine.rs` — StateMachine trait
- `crates/rr-core/src/storage.rs` — StorageEngine trait
- `crates/rr-core/src/config.rs` — ConfigProvider trait
- `crates/rr-core/src/codec.rs` — Codec trait + BincodeCodec impl
- `crates/rr-storage/src/mem.rs` — MemStorage
- `crates/rr-config/src/mem.rs` — MemConfigProvider
- `crates/rr-raft/src/type_config.rs` — Framework TypeConfig
- `crates/rr-raft/src/log_storage.rs` — RaftLogStorage
- `crates/rr-raft/src/sm_adapter.rs` — RaftStateMachineAdapter (openraft ↔ StateMachine)
- `crates/rr-raft/src/service_adapter.rs` — ServiceStateMachineAdapter (Service → StateMachine)
- `crates/rr-raft/src/group_manager.rs` — RaftGroupManager
- `crates/rr-network/proto/raft.proto` — Raft RPC definitions
- `crates/rr-network/proto/client.proto` — Client RPC definitions
- `crates/rr-network/src/server.rs` — RrGrpcServer
- `crates/rr-network/src/client.rs` — RrGrpcClient
- `crates/rr-network/src/channel_pool.rs` — ChannelPool (DashMap<NodeId, Channel>)
- `crates/rr-router/src/router.rs` — ShardRouter
- `crates/rr-server/src/node.rs` — RrNode
- `crates/rr-server/src/config.rs` — NodeConfig
- `examples/kv-store/src/service.rs` — KvService (Service trait example)
- `examples/kv-store/src/state_machine.rs` — KvStateMachine (StateMachine trait example)
- `examples/kv-store/src/types.rs` — KvRequest/KvResponse enums

### Workspace Members (all 9 in root Cargo.toml from T1)
crates/rr-core, crates/rr-storage, crates/rr-config, crates/rr-raft,
crates/rr-network, crates/rr-router, crates/rr-server,
examples/kv-store, tests/integration

## Task 1 Complete: Workspace Scaffolding + Git Init

### Versions Pinned
- **openraft**: 0.9.21 (NOT 0.10.x - doesn't exist, and features differ)
- **tonic**: 0.12 (gRPC framework)
- **tokio**: 1 (async runtime with full features)
- **prost**: 0.13 (protobuf code generation)
- **bincode**: 2 (serialization, uses v2 API)

### Key Learnings
1. openraft 0.9.21 is the latest; no 0.10.x exists in crates.io
2. openraft 0.9.21 does NOT support the "tokio-runtime" feature - removed it from workspace deps
3. All 9 workspace members compile and pass tests successfully
4. No circular dependencies introduced - DAG structure maintained:
   - rr-core: no deps on other rr-* crates (root)
   - rr-storage, rr-config: depend on rr-core
   - rr-raft: depends on rr-core, rr-storage
   - rr-network: depends on rr-core
   - rr-router: depends on rr-core, rr-config
   - rr-server: depends on all framework crates (orchestration layer)
   - examples/kv-store: depends on rr-core, rr-server (demonstration)
   - tests/integration: depends on rr-core, rr-server, rr-raft (testing)

### Build Status
- ✓ `cargo build --workspace`: 23.36s, SUCCESS
- ✓ `cargo test --workspace`: all crates pass (0 tests = stubs, no failures)
- ✓ `cargo clippy --workspace -- -D warnings`: clean, no warnings

### Structure
```
rr/
├── Cargo.toml (workspace root with 9 members)
├── .gitignore (/target)
├── .git/ (initialized)
├── crates/
│   ├── rr-core/
│   ├── rr-storage/
│   ├── rr-config/
│   ├── rr-raft/
│   ├── rr-network/
│   ├── rr-router/
│   └── rr-server/ (with src/main.rs binary)
├── examples/
│   └── kv-store/ (with src/main.rs binary)
└── tests/
    └── integration/
```

### Workspace Dependencies Strategy
All pinned via `[workspace.dependencies]` for consistency:
- Core: async-trait, futures, serde, bincode, thiserror
- Runtime: tokio (full), tonic, prost, bytes
- Observability: tracing, tracing-subscriber, metrics
- Consensus: openraft 0.9.21
- Utils: dashmap, anyhow (allowed in examples/tests)


## Task 2 Complete: Error Types + Common Types

### Error Hierarchy
- **RrError**: Top-level enum with 7 variants, using thiserror
  - Each sub-error implements From<SubError> for automatic conversion
  - All 6 sub-errors are convertible to RrError via #[from]
  - RrError::Application wraps arbitrary String errors

### Sub-Error Types
1. **RaftError**: NotLeader, GroupNotFound, ProposalFailed, Timeout
2. **StorageError**: Io (from std::io::Error), Corruption, NotFound
3. **ConfigError**: ProviderUnavailable, InvalidConfig, WatchDisconnected
4. **RouterError**: PartitionNotFound, GroupNotFound, NoLeader
5. **NetworkError**: ConnectionFailed, RpcFailed, Timeout
6. **CodecError**: SerializeFailed, DeserializeFailed

### Common Types Pattern
- **Type Aliases**: NodeId, GroupId, PartitionId, LogIndex all = u64
- **Value Types**: NodeAddr, PartitionConfig, GroupMeta with serde support
- **RoutingTable**: Aggregates three HashMaps for distributed routing
  - partitions: PartitionId → (PartitionConfig, GroupId)
  - nodes: NodeId → NodeAddr
  - groups: GroupId → GroupMeta

### Testing Strategy
- TDD: Tests written first (8 total tests)
- Error display/conversion tests verify thiserror behavior
- Type construction/clone tests verify derive macros
- All tests pass; zero clippy warnings

### Dependencies (workspace)
- thiserror v2: For error enum derives
- serde v1 with derive feature: For serialization

## Task 4 Complete - Proto Definitions & build.rs
- Proto packages: rr.raft, rr.client (use tonic::include_proto! with exact package name)
- Raft messages all include group_id for multi-Raft demux (critical for framework to route between groups)
- Client RPC includes partition_id for routing, LeaderInfo for client-side leader caching
- tonic-build in build-dependencies at "0.12", workspace deps for tonic/prost/bytes/tokio
- Generated code accessed via: rr_network::proto::raft::*, rr_network::proto::client::*
- tonic::include_proto! in lib.rs must use exact package name from .proto file (dots preserved)
- Proto3 fields optional by default, no need for "optional" keyword in proto3 syntax
- Streaming RPC syntax: rpc InstallSnapshot(stream SnapshotChunk) returns (SnapshotResponse)

## Task 4 Fixed: Tonic-build version aligned to 0.14.5

- **Issue**: tonic-build 0.12 generates proto code using `tonic::codec::ProstCodec` which doesn't exist in tonic 0.14.5
- **Root cause**: tonic-build versions must match runtime tonic version
- **Solution**: 
  - Upgraded to `tonic-prost-build = "0.14.5"` (tonic split its prost compilation into a separate crate in 0.14+)
  - Added `tonic-prost = "0.14.5"` as runtime dependency (provides the codec API used in generated code)
  - Updated build.rs to use `tonic_prost_build::configure()` API (same as old tonic-build 0.12)
- **Result**: cargo build -p rr-network now succeeds, cargo clippy passes with no warnings
- **Key learning**: In tonic 0.14+, prost compilation was split from tonic-build → use tonic-prost-build for build script, tonic-prost for runtime codec

## Task 3: Core Trait Definitions (2026-03-03)

- **bincode 2.x `serde` feature**: The workspace had `bincode = "2.0.0"` without the `serde` feature. Bincode 2 requires `features = ["serde"]` to enable `bincode::serde::{encode_to_vec, decode_from_slice}`. Had to update workspace Cargo.toml.
- **All 5 core traits are object-safe**: Verified via `Box<dyn Trait>` compile-time checks in tests.
- **Codec trait is NOT async**: Unlike StorageEngine/ConfigProvider/StateMachine/Service, Codec is sync (just `Send + Sync`). Serialization should be fast enough to be sync.
- **BincodeCodec is the only concrete implementation**: All other traits are pure trait definitions — no implementations in rr-core.
- **18 tests total** in rr-core after this task (was 7 before, added 11).
- **Pattern**: Use `PartitionConfig::SingleId(1)` for test data when constructing `RoutingTableChange` variants.
