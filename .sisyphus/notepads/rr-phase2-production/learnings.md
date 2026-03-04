# Learnings — rr-phase2-production

## [2026-03-03] Session Start

### API Facts (CRITICAL — verified against actual source)

1. **`RaftGroupLookup` trait** (in `crates/rr-network/src/raft_service.rs`):
   - `append_entries(&self, group_id: GroupId, payload: Vec<u8>) -> Result<Vec<u8>, tonic::Status>`
   - `vote(&self, group_id: GroupId, payload: Vec<u8>) -> Result<Vec<u8>, tonic::Status>`
   - `install_snapshot(&self, group_id: GroupId, metadata: Vec<u8>, data: Vec<u8>) -> Result<Vec<u8>, tonic::Status>`

2. **`ClientDispatch` trait** (in `crates/rr-network/src/client_service.rs`):
   - `submit_write(&self, partition_id: u64, payload: Vec<u8>) -> Result<ClientWriteResult, tonic::Status>`
   - `get_cluster_info(&self) -> Result<Vec<GroupInfo>, tonic::Status>`
   - `ClientWriteResult { success: bool, payload: Vec<u8>, leader_node_id: Option<u64>, leader_addr: Option<String> }`
   - `GroupInfo` is a proto-generated struct from `crates/rr-network/src/proto/client/`

3. **`RaftGroupManager`** (in `crates/rr-raft/src/group_manager.rs`):
   - `get_raft(group_id) -> Option<openraft::Raft<RrTypeConfig>>` — async method
   - `group_ids() -> Vec<GroupId>` — async method
   - `start_group(group_id)` — still uses `NoopNetworkFactory` (T5 will wire real network)

4. **`RaftNode::new_with_factory`** — T1 is DONE, generic constructor exists

5. **`StorageEngine` trait** (7 methods):
   - `put(cf, key, value)`, `get(cf, key)`, `delete(cf, key)`, `scan(cf, start, end)`
   - `write_batch(ops: Vec<BatchOp>)`, `flush()`, `create_cf(name)`

6. **`RocksStorage`** — struct + `open()` + `open_default()` exist, NO `StorageEngine` impl yet

### bincode v2 API
- encode: `bincode::serde::encode_to_vec(&val, bincode::config::standard())`
- decode: `bincode::serde::decode_from_slice::<T, _>(&bytes, bincode::config::standard()).map(|(v, _)| v)`

### Guardrails
- No `anyhow` in library crates
- No `println!`/`eprintln!` — use `tracing`
- No circular deps: rr-network must NOT import rr-raft
- All RocksDB calls must use `spawn_blocking`
- `rr-core` is DAG root — zero deps on other rr-* crates

### Test state: 116 tests, 0 failures (as of 2026-03-03)

## [T13] RoutingTableChange Events Extension — COMPLETED

### Summary
Successfully extended `RoutingTableChange` enum with 3 new variants for node and membership events, updated `MemConfigProvider` to broadcast these events, and implemented comprehensive tests.

### Implementation Details

#### 1. **New Enum Variants** (in `crates/rr-core/src/config.rs`)
```rust
NodeAdded {
    node_id: NodeId,
    addr: NodeAddr,
},
NodeRemoved { node_id: NodeId },
GroupMembershipChanged {
    group_id: GroupId,
    replicas: Vec<NodeId>,
},
```

#### 2. **MemConfigProvider Methods** (in `crates/rr-config/src/mem.rs`)
- `add_node(node_id, addr)`: Broadcasts `NodeAdded` event after inserting node into routing table
- `remove_node(node_id)`: Broadcasts `NodeRemoved` event after removing node
- `update_group_membership(group_id, replicas)`: Broadcasts `GroupMembershipChanged` event after updating group metadata

All methods follow the existing pattern:
1. Acquire write lock on routing table
2. Update state
3. Send event via `self.change_tx.send()` (ignoring error for no-subscriber case)

#### 3. **Test Coverage**
- **config.rs tests** (3 unit tests):
  - `test_routing_table_change_node_added`
  - `test_routing_table_change_node_removed`
  - `test_routing_table_change_group_membership_changed`

- **mem.rs tests** (6 async tests):
  - `test_watch_receives_node_added` — verifies event broadcast via stream
  - `test_watch_receives_node_removed` — verifies event broadcast via stream
  - `test_watch_receives_group_membership_changed` — verifies event broadcast via stream
  - `test_add_node_visible_in_routing_table` — verifies routing table state
  - `test_remove_node_visible_in_routing_table` — verifies routing table state
  - `test_update_group_membership_visible_in_routing_table` — verifies routing table state

### Key Design Decisions
1. **NodeAddr cloning**: Added `.clone()` when inserting into routing table because broadcast event also needs ownership of the addr
2. **Group membership updates**: Created or updated group metadata with proper leader assignment (first replica)
3. **Broadcast error handling**: Followed existing pattern of ignoring send errors (no subscribers is OK)
4. **Type reuse**: Used existing `NodeId`, `NodeAddr`, `GroupId`, `Vec<NodeId>` types — no new dependencies needed

### Test Results
- **rr-core**: 21 tests passed (includes 3 new variant tests)
- **rr-config**: 15 tests passed (includes 6 new event tests)
- **Total**: 36 tests, 0 failures, 0 warnings

### Files Modified
- `crates/rr-core/src/config.rs` — Added 3 enum variants + 3 unit tests
- `crates/rr-config/src/mem.rs` — Added 3 methods + 6 async tests

## [T10] RocksDB Production Config Tuning — COMPLETED

### Summary
Added `RocksConfig` struct to `crates/rr-storage/src/rocks.rs` with production-ready defaults and updated `RocksStorage::open()` to accept it.

### Implementation Details

#### 1. **RocksConfig Struct**
- `#[derive(Debug, Clone)]` — Debug for tracing log, Clone for flexibility
- Re-uses `rocksdb::DBCompressionType` directly (no newtype needed since it's already feature-gated by `#![cfg(feature = "rocks")]`)
- Production defaults: 256MB cache, 64MB write buffer, 4 write buffers, 4 bg jobs, Lz4 compression, L0 slowdown=20, L0 stop=36
- Testing config: 8MB cache, 4MB write buffer, 2 write buffers, 1 bg job, no compression

#### 2. **API Changes**
- `open(path, cfs, config: RocksConfig)` — now takes config parameter, applies all tuning options
- `open_default(path)` — unchanged signature, uses `RocksConfig::default()` internally
- `open_for_testing(path)` — new convenience method using `RocksConfig::for_testing()`
- Config is logged at startup via `tracing::info!`

#### 3. **Backward Compatibility**
- `RocksLogStorage::open()` in `rr-raft` calls `RocksStorage::open_default()` — unchanged, still works
- All existing test helpers updated to use `open_for_testing()` for lighter resource usage

#### 4. **Dependencies Added**
- `tracing.workspace = true` added to `rr-storage/Cargo.toml` for config logging

### Test Results
- **rr-storage (rocks)**: 26 tests passed (16 existing + 3 new config tests + 10 mem tests)
- **rr-raft (rocks)**: 39 tests passed (all RocksLogStorage tests still work)
- **workspace**: 137 tests passed, 0 failures

### Key Learnings
- `rocksdb::DBCompressionType` can be used directly in public API since the entire module is `#[cfg(feature = "rocks")]`
- `Cache::new_lru_cache` takes bytes not MB — multiply by 1024*1024
- Adding `tracing` dependency was needed for config logging (workspace already had it)
