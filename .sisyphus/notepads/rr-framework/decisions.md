# rr-framework — Decisions

## Session: ses_367563ddaffeaTqD93zm1RA6iM | 2026-02-26T08:14:03Z

### Design Decisions
- D=Vec<u8>, R=Vec<u8> for TypeConfig: avoids generic explosion, framework serializes at boundaries
- anyhow banned in lib crates: thiserror only; anyhow allowed in examples/ and tests/
- No ServiceRequest envelope in rr-core: adapter passes raw bytes to Service::handle(), each service defines its own format
- KvRequest/KvResponse defined locally in examples/kv-store/src/types.rs (NOT rr-core)
- NodeConfig takes pre-built instances (not factories) for StorageEngine and ConfigProvider
- state_machine_factory is a closure Fn(GroupId)->Box<dyn StateMachine> — one instance per group
- Worker test for kv-store: uses cargo integration tests, NOT separate binary
- All proto messages include group_id for multi-Raft demux
- Router does table lookup ONLY, no hashing
