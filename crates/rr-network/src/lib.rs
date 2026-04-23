//! rr-network — gRPC transport layer for the rr distributed framework.
//!
//! This crate provides the generated protobuf types, gRPC service stubs,
//! channel pooling, and error conversions for the rr distributed framework.

pub mod auth;
pub mod channel_pool;
pub mod client;
pub mod error_convert;
pub mod tls;
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
pub mod server;

pub use auth::{attach_bearer_token, AuthConfig, BearerInterceptor};
pub use channel_pool::ChannelPool;
pub use client::RrGrpcClient;
pub use tls::TlsConfig;
pub use client_service::{ClientDispatch, ClientServiceImpl, ClientWriteResult};
pub use error_convert::IntoStatus;
pub use raft_service::{RaftGroupLookup, RaftServiceImpl};
pub use server::RrGrpcServer;
