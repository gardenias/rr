//! gRPC `ClientService` implementation for client-facing RPCs.
//!
//! Handles `Request` and `GetClusterInfo` RPCs, dispatching them to the
//! appropriate handler via the [`ClientDispatch`] trait.
//!
//! The [`ClientDispatch`] trait uses a bytes-oriented interface so that
//! `rr-network` does not need a dependency on `rr-raft` (which already depends
//! on `rr-network`, and a cycle is not allowed). The implementor (typically in
//! `rr-raft` or `rr-server`) is responsible for routing client requests to the
//! correct Raft group and returning serialized responses.

use std::sync::Arc;

use crate::proto::client::client_service_server;
use crate::proto::client::{
    ClientRequest, ClientResponse, ClusterInfo, Empty, GroupInfo, LeaderInfo,
};

/// Result of submitting a client write request.
///
/// Contains the response payload plus optional leader redirect information
/// for client-side caching and leader forwarding.
#[derive(Debug, Clone)]
pub struct ClientWriteResult {
    pub success: bool,
    pub payload: Vec<u8>,
    pub leader_node_id: Option<u64>,
    pub leader_addr: Option<String>,
}

/// Trait for dispatching client RPCs to the application layer.
///
/// All payloads are raw bytes. The implementor is responsible for routing
/// client requests to the correct Raft group and performing
/// serialization/deserialization.
///
/// This bytes-oriented design avoids a circular dependency between
/// `rr-network` and `rr-raft`.
#[async_trait::async_trait]
pub trait ClientDispatch: Send + Sync {
    /// Submit a write request to the given partition.
    ///
    /// Returns a [`ClientWriteResult`] with the response payload and optional
    /// leader redirect information.
    async fn submit_write(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
    ) -> Result<ClientWriteResult, tonic::Status>;

    /// Get cluster topology information.
    ///
    /// Returns a list of [`GroupInfo`] describing the current cluster state.
    async fn get_cluster_info(&self) -> Result<Vec<GroupInfo>, tonic::Status>;
}

/// gRPC service implementation that receives client RPCs and forwards them
/// to the application layer via [`ClientDispatch`].
pub struct ClientServiceImpl {
    dispatch: Arc<dyn ClientDispatch>,
}

impl ClientServiceImpl {
    /// Create a new `ClientServiceImpl` backed by the given dispatch.
    pub fn new(dispatch: Arc<dyn ClientDispatch>) -> Self {
        Self { dispatch }
    }
}

#[tonic::async_trait]
impl client_service_server::ClientService for ClientServiceImpl {
    async fn request(
        &self,
        request: tonic::Request<ClientRequest>,
    ) -> Result<tonic::Response<ClientResponse>, tonic::Status> {
        let req = request.into_inner();
        tracing::debug!(partition_id = req.partition_id, "client request received");

        let result = self
            .dispatch
            .submit_write(req.partition_id, req.payload)
            .await?;

        let leader_info = match (result.leader_node_id, result.leader_addr) {
            (Some(node_id), Some(addr)) => Some(LeaderInfo { node_id, addr }),
            _ => None,
        };

        Ok(tonic::Response::new(ClientResponse {
            success: result.success,
            payload: result.payload,
            leader_info,
        }))
    }

    async fn get_cluster_info(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ClusterInfo>, tonic::Status> {
        tracing::debug!("get_cluster_info request received");

        let groups = self.dispatch.get_cluster_info().await?;

        Ok(tonic::Response::new(ClusterInfo { groups }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mock dispatch that returns configurable results.
    struct MockDispatch {
        write_result: Result<ClientWriteResult, tonic::Status>,
        cluster_info: Result<Vec<GroupInfo>, tonic::Status>,
    }

    impl MockDispatch {
        fn success() -> Self {
            Self {
                write_result: Ok(ClientWriteResult {
                    success: true,
                    payload: vec![1, 2, 3],
                    leader_node_id: None,
                    leader_addr: None,
                }),
                cluster_info: Ok(vec![GroupInfo {
                    group_id: 1,
                    leader_node_id: 10,
                    replica_node_ids: vec![10, 20, 30],
                }]),
            }
        }

        fn with_leader_info() -> Self {
            Self {
                write_result: Ok(ClientWriteResult {
                    success: false,
                    payload: vec![],
                    leader_node_id: Some(42),
                    leader_addr: Some("127.0.0.1:50051".to_string()),
                }),
                cluster_info: Ok(vec![]),
            }
        }

        fn failing() -> Self {
            Self {
                write_result: Err(tonic::Status::unavailable("no leader for partition")),
                cluster_info: Err(tonic::Status::internal("cluster state unavailable")),
            }
        }
    }

    #[async_trait::async_trait]
    impl ClientDispatch for MockDispatch {
        async fn submit_write(
            &self,
            _partition_id: u64,
            _payload: Vec<u8>,
        ) -> Result<ClientWriteResult, tonic::Status> {
            self.write_result.clone()
        }

        async fn get_cluster_info(&self) -> Result<Vec<GroupInfo>, tonic::Status> {
            self.cluster_info.clone()
        }
    }

    #[test]
    fn test_client_service_new() {
        let svc = ClientServiceImpl::new(Arc::new(MockDispatch::success()));
        // Verify it compiles and the Arc is valid.
        assert!(Arc::strong_count(&svc.dispatch) >= 1);
    }

    #[tokio::test]
    async fn test_request_dispatch_error() {
        let svc = ClientServiceImpl::new(Arc::new(MockDispatch::failing()));

        let proto_req = ClientRequest {
            partition_id: 7,
            payload: vec![10, 20],
        };

        let result =
            client_service_server::ClientService::request(&svc, tonic::Request::new(proto_req))
                .await;

        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("no leader for partition"));
    }

    #[tokio::test]
    async fn test_get_cluster_info_success() {
        let svc = ClientServiceImpl::new(Arc::new(MockDispatch::success()));

        let result = client_service_server::ClientService::get_cluster_info(
            &svc,
            tonic::Request::new(Empty {}),
        )
        .await;

        let resp = result.unwrap().into_inner();
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group_id, 1);
        assert_eq!(resp.groups[0].leader_node_id, 10);
        assert_eq!(resp.groups[0].replica_node_ids, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn test_get_cluster_info_error() {
        let svc = ClientServiceImpl::new(Arc::new(MockDispatch::failing()));

        let result = client_service_server::ClientService::get_cluster_info(
            &svc,
            tonic::Request::new(Empty {}),
        )
        .await;

        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("cluster state unavailable"));
    }

    #[tokio::test]
    async fn test_request_success_no_leader_info() {
        let svc = ClientServiceImpl::new(Arc::new(MockDispatch::success()));

        let proto_req = ClientRequest {
            partition_id: 1,
            payload: vec![42],
        };

        let result =
            client_service_server::ClientService::request(&svc, tonic::Request::new(proto_req))
                .await;

        let resp = result.unwrap().into_inner();
        assert!(resp.success);
        assert_eq!(resp.payload, vec![1, 2, 3]);
        assert!(resp.leader_info.is_none());
    }

    #[tokio::test]
    async fn test_request_with_leader_redirect() {
        let svc = ClientServiceImpl::new(Arc::new(MockDispatch::with_leader_info()));

        let proto_req = ClientRequest {
            partition_id: 5,
            payload: vec![],
        };

        let result =
            client_service_server::ClientService::request(&svc, tonic::Request::new(proto_req))
                .await;

        let resp = result.unwrap().into_inner();
        assert!(!resp.success);
        let leader = resp.leader_info.unwrap();
        assert_eq!(leader.node_id, 42);
        assert_eq!(leader.addr, "127.0.0.1:50051");
    }
}
