# Task 4: Snapshot Response Payload Integration

## Summary
Successfully fixed the `SnapshotResponse` protobuf message to include a `bytes payload` field for carrying the serialized `InstallSnapshotResponse`. This ensures proper round-trip serialization of snapshot installation responses between client and server.

## Changes Made

### 1. Proto Definition (`crates/rr-network/proto/raft.proto`)
- Added `bytes payload = 3;` field to `SnapshotResponse` message
- Field 3 is appropriate as fields 1-2 were already used

### 2. Server-Side (`crates/rr-network/src/raft_service.rs`)
- Changed `RaftGroupLookup::install_snapshot` trait return type from `Result<bool, tonic::Status>` to `Result<Vec<u8>, tonic::Status>`
- Updated server implementation to return serialized `InstallSnapshotResponse` via payload field
- Updated `EmptyLookup` test mock to match new trait signature
- The trait now follows the bytes-oriented pattern used for `append_entries` and `vote`

### 3. Client-Side (`crates/rr-raft/src/network_factory.rs`)
- Added `payload: Vec<u8>` field to local `ProtoSnapshotResp` struct (mirrors proto)
- Updated `install_snapshot()` method to:
  - Receive `response.payload` from server
  - Deserialize it using bincode: `bincode::serde::decode_from_slice(&response.payload, bincode::config::standard())?`
  - Extract the vote from deserialized `InstallSnapshotResponse`
  - For backward compatibility, defaults to `Vote::new(0, 0)` if payload is empty with a warning

### 4. Test Coverage
- Added `test_snapshot_response_has_payload_field()` in rr-network
- Verifies SnapshotResponse can be constructed with payload and fields are correctly stored
- All 26 rr-network tests pass
- All 25 rr-raft tests pass

## Architecture Pattern
The implementation follows the established bytes-oriented pattern:
1. Request/Response are serialized on client side
2. Sent via proto messages with `bytes payload` fields
3. Server receives, calls lookup, gets serialized response
4. Returns serialized response in proto payload field
5. Client deserializes and extracts openraft types

This pattern avoids circular dependencies between rr-network and rr-raft.

## Test Results
- `cargo build -p rr-network`: ✓ Success
- `cargo test -p rr-raft`: ✓ 25 tests passed
- `cargo test -p rr-network`: ✓ 26 tests passed

## Files Modified
1. `crates/rr-network/proto/raft.proto`
2. `crates/rr-network/src/raft_service.rs`
3. `crates/rr-raft/src/network_factory.rs`
