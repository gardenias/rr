//! rr-network — gRPC transport layer for the rr distributed framework.
//!
//! This crate provides the generated protobuf types, gRPC service stubs,
//! channel pooling, and error conversions for the rr distributed framework.

pub mod audit;
pub mod auth;
pub mod channel_pool;
pub mod error_convert;
pub mod rate_limit;
pub mod tls;
pub mod tracing;
pub mod proto {
    pub mod raft {
        tonic::include_proto!("rr.raft");
    }
    pub mod client {
        tonic::include_proto!("rr.client");
    }
    pub mod admin {
        tonic::include_proto!("rr.admin");
    }
}
pub mod client_service;
pub mod raft_service;
pub mod tcp_raft_server;

/// Decode ceiling for RaftService (server side). tonic's default 4 MiB decode
/// limit rejects oversized inbound batches — with large payloads a batched
/// AppendEntries crosses it and the follower drops the leader's replication
/// stream, permanently wedging the group (perf-lab W5-16KiB, 2026-09-17).
/// RaftService is internal and mTLS-authenticated, so a generous cap is safe.
pub const RAFT_MAX_DECODING_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

/// Decode ceiling for ClientService and AdminService (server side).
///
/// Client write payloads are bounded by the SDK's batching contract
/// (`rr_client::BatchConfig::max_bytes`, default 1 MiB); 16 MiB leaves
/// ample headroom for frame overhead and admin payloads while still
/// rejecting pathological bodies before they are fully decoded.
/// **Operators who raise `BatchConfig::max_bytes` beyond this cap must
/// raise this ceiling as well**, or the server will reject oversized
/// batches with `RESOURCE_EXHAUSTED` before they reach the dispatch layer.
pub const CLIENT_MAX_DECODING_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

pub use audit::{ServiceLabel, AUDIT_TARGET};
pub use auth::{attach_bearer_token, AuthConfig, BearerInterceptor};
pub use channel_pool::ChannelPool;
pub use rate_limit::{RateLimitConfig, RateLimitInterceptor, RateLimiter};
pub use tls::TlsConfig;
pub use tracing::{
    attach_traceparent, attach_tracestate, extract_traceparent, extract_tracestate,
    TRACEPARENT_HEADER, TRACESTATE_HEADER,
};

/// Bounds a single dial. Without it a black-holed address hangs the caller
/// until the OS TCP timeout fires (minutes).
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Shared endpoint tuning for every outbound gRPC dial in the workspace
/// (rr-network pool, rr-client, rr-raft network factory): connect timeout
/// plus TCP/H2 keepalive so a black-holed or silently half-open connection
/// is torn down and redialed instead of producing permanent heartbeat/RPC
/// timeouts on a pooled channel.
///
/// Defined at crate root (rather than in `channel_pool`) so the pool's
/// internal layout can evolve without moving this cross-crate helper.
pub fn tuned_endpoint(endpoint: tonic::transport::Endpoint) -> tonic::transport::Endpoint {
    endpoint
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .http2_keep_alive_interval(std::time::Duration::from_secs(10))
        .keep_alive_timeout(std::time::Duration::from_secs(5))
        .keep_alive_while_idle(true)
}

/// Compose two interceptors in order: `first` runs, then (if it passes)
/// `second`. Used by rr-server to stack auth (first, cheap fail-fast)
/// before rate-limit (second, only counts authenticated requests).
#[derive(Clone)]
pub struct ChainedInterceptor<A, B> {
    pub first: A,
    pub second: B,
}

impl<A, B> tonic::service::Interceptor for ChainedInterceptor<A, B>
where
    A: tonic::service::Interceptor + Clone,
    B: tonic::service::Interceptor + Clone,
{
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let request = self.first.call(request)?;
        self.second.call(request)
    }
}
pub use client_service::{ClientDispatch, ClientServiceImpl, ClientWriteResult};
pub use error_convert::IntoStatus;
pub use raft_service::{RaftGroupLookup, RaftServiceImpl};
