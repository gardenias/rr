//! rr-network — gRPC transport layer for the rr distributed framework.
//!
//! This crate provides the generated protobuf types, gRPC service stubs,
//! channel pooling, and error conversions for the rr distributed framework.

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

pub use auth::{attach_bearer_token, AuthConfig, BearerInterceptor};
pub use channel_pool::ChannelPool;
pub use rate_limit::{RateLimitConfig, RateLimitInterceptor, RateLimiter};
pub use tls::TlsConfig;
pub use tracing::{
    attach_traceparent, attach_tracestate, extract_traceparent, extract_tracestate,
    TRACEPARENT_HEADER, TRACESTATE_HEADER,
};

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
    fn call(
        &mut self,
        request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let request = self.first.call(request)?;
        self.second.call(request)
    }
}
pub use client_service::{ClientDispatch, ClientServiceImpl, ClientWriteResult};
pub use error_convert::IntoStatus;
pub use raft_service::{RaftGroupLookup, RaftServiceImpl};
