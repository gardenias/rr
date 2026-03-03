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

## Task 5 Complete: MemStorage — In-memory StorageEngine (2026-03-03)

### Implementation
- `crates/rr-storage/src/mem.rs`: Full `StorageEngine` impl using `Arc<RwLock<HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>>>`
- Pre-populates 3 default column families: `raft_log`, `state_machine`, `metadata`
- `create_cf()` uses `entry().or_insert_with()` — idempotent, no error on duplicate
- Unknown CF operations return `StorageError::NotFound { key: "cf:{name}" }`
- `flush()` is a no-op for in-memory storage
- `scan()` uses `BTreeMap::range()` with `Bound::Included(start)..Bound::Excluded(end)`
- `write_batch()` holds single write lock for atomicity across all ops

### Testing
- 10 tests covering: CRUD, scan range, batch puts, batch mixed put/delete, flush noop, create_cf, unknown CF error, concurrent reads/writes
- Concurrency test: 10 concurrent tokio tasks writing to same CF, all visible after join
- All tests pass; zero clippy warnings

### Key Learnings
- Cargo.toml already had correct deps from T1 scaffolding — no changes needed
- `tokio::sync::RwLock` (not `std::sync::RwLock`) required for async context
- `#[derive(Clone, Default)]` on MemStorage works because `Arc<RwLock<_>>` impls both
- `Default::default()` creates empty HashMap (no default CFs) vs `new()` which pre-populates

## Task 6 Complete: MemConfigProvider — In-memory ConfigProvider (2026-03-03)

### Implementation
- `crates/rr-config/src/mem.rs`: Full `ConfigProvider` impl using `Arc<RwLock<RoutingTable>>` + `broadcast::Sender<RoutingTableChange>`
- `broadcast::channel(64)` for multi-subscriber watch semantics
- `BroadcastStream` from `tokio-stream` (with `sync` feature) wraps `broadcast::Receiver` into `futures::Stream`
- `filter_map(|r| r.ok())` on BroadcastStream silently drops lagged errors
- `add_partition()` creates GroupMeta entry if not present, with first replica as leader
- `remove_partition()` removes from partitions map but leaves group intact (intentional)
- `add_node()` inserts NodeAddr into routing table
- `get_node_addr()` returns `ConfigError::InvalidConfig` for missing nodes

### Dependencies Added
- `tokio-stream = { version = "0.1", features = ["sync"] }` — NOT a workspace dep, only needed by rr-config

### Testing
- 9 tests: empty table, add/remove partition, node addr CRUD, watch stream (added + removed), default impl, multiple partitions
- Watch tests use `tokio::spawn` + `tokio::time::timeout(100ms)` pattern for async stream assertions
- All tests pass; zero clippy warnings

### Key Learnings
- **StreamExt ambiguity**: `tokio_stream::StreamExt` and `futures::StreamExt` both provide `.next()`. When both are in scope (prod code uses tokio_stream, tests import super::*), Rust can't disambiguate. Fix: don't re-import futures::StreamExt in tests when super::* already brings tokio_stream::StreamExt.
- **broadcast send errors are OK**: `let _ = self.change_tx.send(...)` — no subscribers = no error needed
- **tokio-stream sync feature**: Required for `BroadcastStream` wrapper. Without it, the type isn't available.

## Task 8 Complete: gRPC Server/Client Scaffolding with ChannelPool (2026-03-03)

### Implementation
- `crates/rr-network/src/channel_pool.rs`: `ChannelPool` using `Arc<DashMap<NodeId, Channel>>` for lock-free concurrent connection pooling
- `crates/rr-network/src/error_convert.rs`: `IntoStatus` trait + impls for all 7 error types → `tonic::Status`
- `crates/rr-network/src/server.rs`: `RrGrpcServer` stub (addr + builder, service handlers deferred to T10/T11)
- `crates/rr-network/src/client.rs`: `RrGrpcClient` wrapping `RaftServiceClient<Channel>` + `ClientServiceClient<Channel>`
- `crates/rr-network/src/lib.rs`: Updated with all module exports + re-exports (`ChannelPool`, `RrGrpcClient`, `RrGrpcServer`, `IntoStatus`)
- `Cargo.toml` (workspace): Added `dashmap = "6"` to workspace deps
- `crates/rr-network/Cargo.toml`: Added `dashmap = { workspace = true }`

### Key Learnings
- **Orphan rule**: Cannot impl `From<RrError> for tonic::Status` in rr-network because neither type is local. Solution: define local `IntoStatus` trait with `fn into_status(self) -> Status` method.
- **dashmap 6.x**: Current major version, API compatible with prior expectations. `DashMap::get()` returns `Ref<K,V>` which derefs to `V`; `.clone()` on `Ref` clones the value.
- **tonic 0.14.5 Channel API**: `Channel::from_shared(uri_string)` returns `Result<Endpoint, _>`, then `.connect().await` → `Result<Channel, _>`. Channel implements Clone cheaply (inner Arc).
- **RrGrpcServer::builder()**: Returns `tonic::transport::Server` (not a builder in the traditional sense). Service registration happens via `.add_service()` on the router returned by `Server::builder()`.

### Testing
- 16 tests total: 4 channel_pool + 12 error_convert
- Channel pool tests: new/default empty, remove nonexistent, clone shares state
- Error convert tests: every error variant → correct gRPC code mapping (Unavailable, NotFound, DeadlineExceeded, InvalidArgument, DataLoss, Internal)
- All tests pass; zero clippy warnings

## Task 7 Complete: rr-raft — TypeConfig + Single Raft Group (2026-03-03)

### Implementation
- `crates/rr-raft/src/type_config.rs`: `declare_raft_types!` macro defining `RrTypeConfig` with `D=Vec<u8>`, `R=Vec<u8>`, `NodeId=u64`, `Node=BasicNode`, `Entry=openraft::Entry<RrTypeConfig>`, `SnapshotData=Cursor<Vec<u8>>`, `AsyncRuntime=TokioRuntime`
- `crates/rr-raft/src/log_storage.rs`: `RrLogStorage` implementing `RaftLogReader` + `RaftLogStorage` traits with in-memory BTreeMap storage (4 tests)
- `crates/rr-raft/src/sm_adapter.rs`: `RrStateMachine` implementing `RaftStateMachine` trait, bridging to `rr_core::state_machine::StateMachine`. Also `RrSnapshotBuilder` implementing `RaftSnapshotBuilder`.
- `crates/rr-raft/src/raft_node.rs`: `RaftNode` struct wrapping `Raft<RrTypeConfig>`, `NoopNetworkFactory`/`NoopNetwork` for single-node testing, `init_single_node()` helper (1 integration test)
- `crates/rr-raft/src/lib.rs`: Exports all 4 modules and re-exports key types

### Key Learnings
1. **`LogFlushed::new()` is `pub(crate)` in openraft 0.9.21** — cannot construct it from outside the crate. Use `RaftLogStorageExt::blocking_append()` instead for tests.
2. **openraft 0.9.21 `RaftNetwork` trait** requires `RPCOption` parameter on all methods (`append_entries`, `install_snapshot`, `vote`) — different from older docs.
3. **`std::io::Error::new(ErrorKind::Other, ...)` triggers clippy `io_other_error` lint** — must use `std::io::Error::other(...)` instead.
4. **`CommittedLeaderId::new(term, node_id)` is needed** to construct `LogId` values in tests.
5. **Single-node Raft test needs ~1s sleep** after `initialize()` for leader election before `client_write()` succeeds.
6. **`StoredMembership::default()` works** for initial empty membership state.
7. **Error bridging from `RrError` to openraft**: Use `StorageIOError::write_state_machine(AnyError::new(&e))` wrapped in `StorageError::IO { source: ... }`.
8. **Do NOT add `tokio-runtime` feature to openraft** — it does NOT exist in 0.9.21. Use `storage-v2` and `serde` features only.
9. **Do NOT use the old `RaftStorage` single-trait API** — use `RaftLogStorage` + `RaftStateMachine` (storage-v2).
10. **Do NOT import from `openraft::storage::adapter`** — not available in 0.9.21.

### Testing
- 6 tests total: 1 type_config smoke, 4 log_storage (append/read, vote save/read, truncate, get_log_state), 1 single-node raft propose
- All tests pass; zero clippy warnings
- `cargo build --workspace` succeeds

## Task 9 Complete: RaftGroupManager (2026-03-03)

### What was built
- `crates/rr-raft/src/group_manager.rs` — `RaftGroupManager` struct managing lifecycle of multiple `RaftNode`s
- `StateMachineFactory` type alias: `Box<dyn Fn(GroupId) -> Box<dyn StateMachine> + Send + Sync>`
- Methods: `new`, `start_group`, `stop_group`, `get_raft`, `group_ids`, `group_count`

### Key patterns
1. **`Arc<RwLock<HashMap<GroupId, RaftNode>>>`** — standard async-safe pattern for multi-group state. No need for `DashMap`.
2. **`ok_or` over `ok_or_else`** when the error value doesn't involve closures/allocations that should be lazy — clippy flags `unnecessary_lazy_evaluations`.
3. **`Fatal<u64>` → `RrError::Application(e.to_string())`** — openraft's `Fatal` error doesn't impl `Into<RrError>`, so string conversion is the bridge.
4. **Shutdown before remove**: Must call `node.raft.shutdown().await` before dropping to cleanly stop the Raft instance.
5. **Factory pattern for StateMachine**: Each group gets its own SM instance via the factory closure, enabling per-group isolation.

### Testing
- 5 new tests: start_stop, start_duplicate, stop_nonexistent, multiple_groups, get_raft
- Total: 11 tests in rr-raft (6 existing + 5 new)
- All tests pass; zero clippy warnings; workspace builds clean

## Task 13 Complete: ShardRouter (2026-03-03)

### What was built
- `crates/rr-router/src/router.rs` — `ShardRouter` struct with 4 public async methods
- Pure table-lookup routing: partition → group → leader → node addr
- No consistent hashing, no hash rings — routing table is the single source of truth

### API surface
- `ShardRouter::new(config: Arc<dyn ConfigProvider>)` — constructor
- `route(partition_id) → Result<GroupId, RrError>` — partition → group lookup
- `get_leader(group_id) → Result<NodeId, RrError>` — group → leader lookup
- `get_leader_addr(partition_id) → Result<NodeAddr, RrError>` — full chain: partition → group → leader → addr
- `routing_table() → Result<RoutingTable, RrError>` — expose full table

### Key patterns
1. **`ConfigProvider` via `Arc<dyn>`** — enables runtime polymorphism (MemConfigProvider in tests, etcd in prod)
2. **`get_leader_addr` delegates to `config.get_node_addr()`** — avoids semantic mismatch of using `RouterError::GroupNotFound` for missing nodes; propagates `ConfigError` instead
3. **`ok_or` not `ok_or_else`** for cheap error construction — clippy enforces `unnecessary_lazy_evaluations`
4. **`#[from] ConfigError` on `RrError`** — using `?` on `get_routing_table()` auto-converts; `get_node_addr()` also auto-converts
5. **`MemConfigProvider` cloneable** — `Clone` derive enables `Arc::new(provider.clone())` pattern in tests

### Testing
- 8 tests: route found/not_found, get_leader success/group_not_found, get_leader_addr success/partition_not_found/node_not_found, routing_table full
- All pass; zero clippy warnings

### Pre-existing issue
- Workspace has cyclic dependency: rr-raft ↔ rr-network. Prevents `cargo build --workspace` but does NOT affect rr-router (no dependency on either). Tested rr-router in isolation successfully.

## Task 10 Complete: RaftService gRPC Implementation (2026-03-03)

### What was built
- `crates/rr-network/src/raft_service.rs` — gRPC `RaftService` implementation for AppendEntries, Vote, and InstallSnapshot RPCs
- `RaftGroupLookup` trait — bytes-oriented dispatch interface for Raft group demuxing by `group_id`
- `RaftServiceImpl` struct — implements `proto::raft::raft_service_server::RaftService`, delegates to `RaftGroupLookup`
- Streaming `InstallSnapshot` RPC handler — collects `SnapshotChunk` stream, extracts metadata from final chunk, concatenates data, delegates to lookup

### Critical design decision: bytes-oriented `RaftGroupLookup` trait
- **Problem**: Task spec said to add `rr-raft` as dependency of `rr-network`, but `rr-raft` already depends on `rr-network` (added in T9), creating a **cyclic dependency**.
- **Solution**: Instead of `get_raft(group_id) -> Option<Raft<RrTypeConfig>>`, the trait has method-per-RPC (`append_entries`, `vote`, `install_snapshot`) accepting/returning raw bytes.
- This pushes bincode encode/decode into the implementor (in rr-raft) rather than the service layer.
- `rr-network` needs NO openraft or bincode dependency — only tonic, async-trait, tokio-stream, tracing, rr-core.
- The implementor in rr-raft will bincode-decode the openraft request, forward to `Raft<RrTypeConfig>`, and bincode-encode the response.

### Dependencies added to rr-network
- `async-trait = { workspace = true }` — for `RaftGroupLookup` trait definition
- `tracing = { workspace = true }` — for `tracing::debug!` in RPC handlers
- `tokio-stream = "0.1"` — for `StreamExt` in streaming `InstallSnapshot` handler

### Testing
- 3 new tests (19 total in rr-network): `test_raft_service_new`, `test_raft_service_group_not_found`, `test_vote_group_not_found`
- Tests use `EmptyLookup` mock that returns `Status::not_found` for all groups
- All 19 tests pass; zero clippy warnings

### Key learnings
1. **Cyclic deps are a real constraint**: `rr-raft -> rr-network` was added in T9 (network_factory.rs). Cannot add `rr-network -> rr-raft`. Bytes-oriented traits are the escape hatch.
2. **Pre-existing `rr-raft` compilation errors**: `network_factory.rs` uses `http::uri::PathAndQuery` and `tonic::codec::ProstCodec` which don't compile. This is NOT caused by T10 changes — it was already broken.
3. **`tonic::async_trait`** (not `async_trait::async_trait`) for tonic service impls. The trait attribute macro is re-exported by tonic.
4. **Streaming RPC signature**: `install_snapshot(Request<Streaming<SnapshotChunk>>) -> Result<Response<SnapshotResponse>, Status>` — client-streaming pattern in tonic.
5. **No `type InstallSnapshotStream` needed**: That's only for server-streaming. Client-streaming uses `Streaming<T>` in the request parameter.

## Task 12 Complete: GrpcNetworkFactory — RaftNetworkFactory backed by gRPC (2026-03-03)

### What was built
- `crates/rr-raft/src/network_factory.rs` — `GrpcNetworkFactory` + `GrpcRaftNetwork` implementing openraft's `RaftNetworkFactory` + `RaftNetwork` traits
- Local `ChannelPool` (duplicated from rr-network to avoid cyclic dep)
- Local proto message types with matching prost annotations (wire-compatible with `rr.raft.*`)
- Hand-rolled tonic gRPC client (`RaftGrpcClient`) using `tonic_prost::ProstCodec`

### Critical: Cyclic dependency avoidance
- `rr-network` depends on `rr-raft` (for `RrTypeConfig`), so `rr-raft` CANNOT depend on `rr-network`
- Solution: duplicate `ChannelPool` and proto types locally in `network_factory.rs`
- Proto messages are wire-compatible (same prost annotations, same package `rr.raft`)

### Key learnings
1. **`RPCOption::new(hard_ttl: Duration)`** — no `Default` impl in openraft 0.9.21. Must construct explicitly.
2. **`tonic_prost::ProstCodec`** — NOT `tonic::codec::ProstCodec`. The codec was split into `tonic-prost` crate in tonic 0.14+.
3. **Hand-rolled tonic client**: Use `tonic::client::Grpc::new(channel)` + `.unary()` with `ProstCodec::default()` and `PathAndQuery::from_static("/rr.raft.RaftService/MethodName")`.
4. **`tonic::codegen::http`** — access `http` types through tonic's re-export, not a separate `http` crate.
5. **Install snapshot limitation**: Proto `SnapshotResponse` only has `(group_id, success)` — no `payload` for `Vote`. Returns `Vote::new(0, 0)` as documented placeholder.
6. **bincode v2 API**: `bincode::serde::encode_to_vec(&val, bincode::config::standard())` / `decode_from_slice`.

### Dependencies added to rr-raft
- `prost`, `bincode`, `tokio-stream`, `tonic`, `dashmap`, `tonic-prost`

### Testing
- 6 new tests (17 total in rr-raft): channel_pool_new, channel_pool_clone, factory_new, network_with_group_id, connection_error, vote_connection_error
- All 17 tests pass; zero clippy warnings; workspace builds clean

## Task 11 Complete: ClientService gRPC Implementation + Leader Forwarding (2026-03-03)

### What was built
- `crates/rr-network/src/client_service.rs` — gRPC `ClientService` implementation for client-facing RPCs
- `ClientDispatch` trait — bytes-oriented dispatch interface (same pattern as `RaftGroupLookup` from T10)
- `ClientWriteResult` — local struct mapping to `ClientResponse` proto with optional `LeaderInfo`
- `ClientServiceImpl` — implements `proto::client::client_service_server::ClientService`, delegates to `ClientDispatch`
- `lib.rs` updated with `pub mod client_service;` and re-exports of `ClientDispatch`, `ClientServiceImpl`, `ClientWriteResult`

### Design: Bytes-oriented `ClientDispatch` trait
- **2 methods**: `submit_write(partition_id, payload) → ClientWriteResult` and `get_cluster_info() → Vec<GroupInfo>`
- Returns `Result<..., tonic::Status>` (NOT `RrError`) — same as `RaftGroupLookup`
- `GroupInfo` is reused directly from proto (`crate::proto::client::GroupInfo`) — no need for local redefinition
- `ClientWriteResult` has `leader_node_id: Option<u64>` + `leader_addr: Option<String>` for leader forwarding
- Leader info mapping: `(Some(node_id), Some(addr))` → `Some(LeaderInfo { node_id, addr })`, otherwise `None`

### Testing
- 6 new tests (25 total in rr-network): construction, dispatch error (Unavailable), cluster_info success, cluster_info error, request success (no leader), request with leader redirect
- MockDispatch with 3 variants: `success()`, `with_leader_info()`, `failing()`
- All tests assert real behavior — no `assert!(true)` placeholders

### Key learnings
1. **Proto `GroupInfo` can be used directly in trait return types** — no need to create a local wrapper since the proto type is generated in the same crate.
2. **`#[derive(Clone)]` on MockDispatch results** — `tonic::Status` implements `Clone`, so mock dispatch can store `Result<T, tonic::Status>` and clone on each call.
3. **Pattern is identical to T10**: `#[async_trait::async_trait]` on trait definition, `#[tonic::async_trait]` on tonic service impl, `Arc<dyn Trait>` in struct.
4. **No new dependencies needed** — all deps (`async-trait`, `tonic`, `tracing`) were already present from T8/T10.

## Task 16 Complete: ServiceStateMachineAdapter (Service → StateMachine Bridge) (2026-03-03)

### What was built
- `crates/rr-raft/src/service_adapter.rs` — `ServiceStateMachineAdapter<S: Service>` generic struct
- Implements `StateMachine` for any `S: Service` — thin pass-through, no byte interpretation
- `applied_count: u64` counter: `last_applied()` returns `None` when 0, `Some(count)` when > 0
- `apply()` increments counter ONLY on success (error → no increment)
- `snapshot()` and `restore()` delegate directly to `Service::snapshot()` / `Service::restore()`
- `lib.rs` updated with `pub mod service_adapter;` + `pub use service_adapter::ServiceStateMachineAdapter;`

### Testing
- 6 tests total (exceeds required 5):
  1. `apply_passes_bytes_unchanged` — verifies bytes flow through unchanged, records calls
  2. `snapshot_delegates_to_service` — verifies snapshot delegation
  3. `restore_delegates_to_service` — verifies restore delegation via shared Arc<Mutex<Vec<u8>>>
  4. `error_propagation_from_handle` — Service error → StateMachine::apply error
  5. `last_applied_tracking` — None→Some(1)→Some(2)→Some(3) progression
  6. `failed_apply_does_not_increment_counter` — error case doesn't change counter
- All 23 tests pass in rr-raft; zero clippy warnings

### Key patterns
1. **MockService uses `Arc<Mutex<Vec<Vec<u8>>>>` for call recording** — shared between test and adapter
2. **`tokio::sync::Mutex`** (not std) for async test context
3. **`entry.to_vec()` in `apply()`** — `Service::handle()` takes owned `Vec<u8>`, `StateMachine::apply()` takes `&[u8]`
4. **No new dependencies** — `async-trait` and `rr-core` already in Cargo.toml
5. **Generic adapter vs boxed trait object**: `ServiceStateMachineAdapter<S>` is generic; concrete instances like `ServiceStateMachineAdapter<KvService>` impl `StateMachine` and can be `Box<dyn StateMachine>`
