//! gRPC client stubs for the rr network layer.

use crate::proto::client::client_service_client::ClientServiceClient;
use crate::proto::raft::raft_service_client::RaftServiceClient;
use tonic::transport::Channel;

/// A gRPC client providing typed stubs for both RaftService and ClientService.
pub struct RrGrpcClient {
    pub raft: RaftServiceClient<Channel>,
    pub client: ClientServiceClient<Channel>,
}

impl RrGrpcClient {
    /// Connect to a server at the given address.
    pub async fn connect(addr: &str) -> Result<Self, tonic::transport::Error> {
        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("valid uri")
            .connect()
            .await?;
        Ok(Self {
            raft: RaftServiceClient::new(channel.clone()),
            client: ClientServiceClient::new(channel),
        })
    }
}
