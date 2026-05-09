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

use rr_core::idempotency::{IdempotencyError, IdempotencyKey};

use crate::proto::client::client_service_server;
use crate::proto::client::{
    ClientRequest, ClientResponse, ClusterInfo, Empty, GroupInfo, IdempotencyError as ProtoIdemErr,
    IdempotencyKey as ProtoIdemKey, LeaderInfo,
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
    /// Set when the framework rejected an idempotent request because its
    /// `request_id` is below the watermark and has been evicted from the
    /// LRU cache. Mutually exclusive with non-empty `payload`.
    pub idem_error: Option<IdempotencyError>,
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
    /// `idem_key` is the optional [`IdempotencyKey`] from the
    /// `ClientRequest.idem_key` proto field. When `Some`, the dispatch
    /// implementation wraps the payload as a `FramedEntry` so the
    /// state-machine adapter consults its idempotency layer; when
    /// `None`, the payload is forwarded as raw bytes for v0.2.x
    /// compatibility.
    async fn submit_write(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
        idem_key: Option<IdempotencyKey>,
    ) -> Result<ClientWriteResult, tonic::Status>;

    /// Submit a stale-read request to the local replica's state machine.
    ///
    /// Bypasses Raft consensus — may return slightly stale data.
    /// Any node (leader or follower) can serve the request.
    async fn submit_read(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status>;

    /// Get cluster topology information.
    ///
    /// Returns a list of [`GroupInfo`] describing the current cluster state.
    async fn get_cluster_info(&self) -> Result<Vec<GroupInfo>, tonic::Status>;
}

fn proto_to_idem_key(p: ProtoIdemKey) -> IdempotencyKey {
    IdempotencyKey {
        request_type: p.request_type,
        request_id: p.request_id,
        client_id: p.client_id,
    }
}

fn idem_err_to_proto(err: IdempotencyError) -> ProtoIdemErr {
    ProtoIdemErr {
        request_type: err.request_type,
        request_id: err.request_id,
        watermark: err.watermark,
    }
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
        let traceparent = crate::tracing::extract_traceparent(&request);
        let req = request.into_inner();
        let idem_key = req.idem_key.map(proto_to_idem_key);
        tracing::debug!(
            partition_id = req.partition_id,
            keyed = idem_key.is_some(),
            traceparent = traceparent.as_deref().unwrap_or(""),
            "client request received"
        );

        if req.read_only {
            let payload = self
                .dispatch
                .submit_read(req.partition_id, req.payload)
                .await?;
            return Ok(tonic::Response::new(ClientResponse {
                success: true,
                payload,
                leader_info: None,
                idem_error: None,
            }));
        }

        let result = self
            .dispatch
            .submit_write(req.partition_id, req.payload, idem_key)
            .await?;

        let leader_info = match (result.leader_node_id, result.leader_addr) {
            (Some(node_id), Some(addr)) => Some(LeaderInfo { node_id, addr }),
            _ => None,
        };

        Ok(tonic::Response::new(ClientResponse {
            success: result.success,
            payload: result.payload,
            leader_info,
            idem_error: result.idem_error.map(idem_err_to_proto),
        }))
    }

    async fn get_cluster_info(
        &self,
        request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ClusterInfo>, tonic::Status> {
        let traceparent = crate::tracing::extract_traceparent(&request);
        tracing::debug!(
            traceparent = traceparent.as_deref().unwrap_or(""),
            "get_cluster_info request received"
        );

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
                    idem_error: None,
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
                    idem_error: None,
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
            _idem_key: Option<IdempotencyKey>,
        ) -> Result<ClientWriteResult, tonic::Status> {
            self.write_result.clone()
        }

        async fn submit_read(
            &self,
            _partition_id: u64,
            _payload: Vec<u8>,
        ) -> Result<Vec<u8>, tonic::Status> {
            Ok(vec![])
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
                    idem_key: None,
                    read_only: false,
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
                    idem_key: None,
                    read_only: false,
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
                    idem_key: None,
                    read_only: false,
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
