# rr-framework — Issues & Gotchas

## Session: ses_367563ddaffeaTqD93zm1RA6iM | 2026-02-26T08:14:03Z

### Known Gotchas
- openraft version: must pin exact 0.10.x — check crates.io before scaffolding
- tonic requires protoc installed; build.rs uses tonic-build
- async-trait crate needed for trait methods with async fn
- BincodeCodec uses bincode v2 API (bincode::encode_to_vec / bincode::decode_from_slice) NOT v1 API
- StorageEngine trait needs async-trait: all methods are async
- MemStorage: use Arc<RwLock<HashMap<...>>> for thread safety, NOT RefCell
- openraft TypeConfig: use declare_raft_types! macro, NOT manual impl
- openraft RaftStateMachine: streaming apply (process_entry + apply_commit_entry in newer API)
- ChannelPool uses DashMap for lock-free concurrent access
- rr-core must have ZERO deps on other rr-* crates (DAG root)
- No println!/eprintln! in library crates — use tracing macros
