## [2026-03-05] Known Issues & Gotchas

### Cyclic Dependency: rr-raft vs rr-network
- rr-network depends on rr-raft (for RrTypeConfig)
- Therefore rr-raft CANNOT depend on rr-network
- rr-raft has its OWN ChannelPool in network_factory.rs (separate from rr-network/channel_pool.rs)
- When modifying ChannelPool behavior, target rr-raft/src/network_factory.rs ONLY

### tonic-prost-build vs tonic-build
- Project uses tonic-prost-build, NOT tonic-build
- build.rs: tonic_prost_build::configure().compile_protos(...)
- When adding new proto files, use this same pattern

### bincode 2.0 API
- bincode::serde::encode_to_vec(&val, standard()) — for encoding
- bincode::serde::decode_from_slice(&bytes, standard()) — for decoding (returns (val, len))
- NOT the old bincode 1.x serialize/deserialize API

### metrics crate v0.24
- metrics::counter!("name") — creates/increments counter
- metrics::gauge!("name") — creates/sets gauge
- metrics::histogram!("name") — records value
- No recorder = metrics go nowhere (expected until B1 wires prometheus recorder)

### GrpcNetworkFactory stores ChannelPool by VALUE (not Arc)
- GrpcNetworkFactory { channel_pool: ChannelPool, group_id: u64 }
- GrpcRaftNetwork { channel_pool: ChannelPool, ... }
- E1 will change these to Arc<dyn ChannelPoolLike>

### Cargo workspace: examples need to be added to members
- examples/kv-store IS already in members
- examples/counter needs to be ADDED
- benches/ crate also needs to be added
