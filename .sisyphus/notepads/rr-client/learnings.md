# Learnings — rr-client crate (T19 + T20)

## Patterns & Conventions
- Proto client stub path: `rr_network::proto::client::client_service_client::ClientServiceClient`
- Proto types path: `rr_network::proto::client::{ClientRequest, ClientResponse, ClusterInfo, Empty, LeaderInfo}`
- `ClientRequest` uses `partition_id` field (not `group_id`), maps 1:1 to `GroupId`
- `Channel::from_shared(format!("http://{addr}")).expect("valid uri").connect().await` — pattern from `RrGrpcClient::connect()`
- `ChannelPool` uses `Arc<DashMap<NodeId, Channel>>` — requires `NodeId` for keying, not just address strings
- `RrError::Network(NetworkError::ConnectionFailed { addr, reason })` for connection errors
- `RrError::Network(NetworkError::RpcFailed(msg))` for gRPC failures
- Workspace deps use `{ workspace = true }` pattern
- `tokio::sync::RwLock` (not `std::sync::RwLock`) for async-compatible leader cache

## Test Approaches
- `"localhost:1"` (port 1) reliably fails connection — fast for network error tests
- `RetryConfig { max_retries: 0..1, retry_delay_ms: 10 }` for tests to avoid slow retries
- Tests run in ~0.02s total — connection failures are fast
- Leader cache can be tested directly via `update_leader_cache` + `get_cached_leader` helpers
- No need for mocking gRPC — real connection failures validate the error path

## Successful Approaches
- Kept `ChannelPool` in struct for future use, added `channel_pool()` accessor to avoid dead_code warning
- `connect()` is lazy — no actual connections until `send_request` / `get_cluster_info`
- Empty addrs check in `connect()` gives early clear error
- `get_cluster_info` tries all bootstrap addrs sequentially (not retry loop — just fails through)
