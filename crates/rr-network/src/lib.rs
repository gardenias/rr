//! rr-network — gRPC transport layer for the rr distributed framework.
//!
//! This crate provides the generated protobuf types, gRPC service stubs,
//! channel pooling, and error conversions for the rr distributed framework.

pub mod channel_pool;
pub mod client;
pub mod error_convert;
pub mod proto {
    pub mod raft {
        tonic::include_proto!("rr.raft");
    }
    pub mod client {
        tonic::include_proto!("rr.client");
    }
}
pub mod server;
pub mod raft_service;
pub mod client_service;

pub use channel_pool::ChannelPool;
pub use client::RrGrpcClient;
pub use server::RrGrpcServer;
pub use error_convert::IntoStatus;
pub use raft_service::{RaftGroupLookup, RaftServiceImpl};
pub use client_service::{ClientDispatch, ClientServiceImpl, ClientWriteResult};
