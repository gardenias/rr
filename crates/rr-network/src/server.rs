//! gRPC server stub for the rr network layer.
//!
//! The actual service implementations (RaftService, ClientService) are filled in
//! by tasks T10 and T11. This module provides the server scaffolding.

use std::net::SocketAddr;

use tonic::transport::Server;

/// A gRPC server for the rr distributed framework.
///
/// Serves both `RaftService` (for Raft consensus RPCs) and
/// `ClientService` (for client requests). Service handlers are
/// injected after construction.
pub struct RrGrpcServer {
    addr: SocketAddr,
}

impl RrGrpcServer {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    /// Returns the address this server is configured to listen on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns a tonic `Server` builder ready for service registration.
    pub fn builder() -> Server {
        Server::builder()
    }
}
