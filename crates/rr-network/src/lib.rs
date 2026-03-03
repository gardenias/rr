//! rr-network — gRPC transport layer for the rr distributed framework.
//!
//! This crate provides the generated protobuf types and gRPC service stubs.

pub mod proto {
    pub mod raft {
        tonic::include_proto!("rr.raft");
    }
    pub mod client {
        tonic::include_proto!("rr.client");
    }
}
