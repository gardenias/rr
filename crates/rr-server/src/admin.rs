//! Admin gRPC service implementation.
//!
//! 节点管理和可观测性 gRPC 服务，提供节点状态查询、Raft 组状态监控、快照触发和领导者转移等功能。
//!
//! ---
//!
//! Admin gRPC service for node management and observability. Provides node status queries,
//! Raft group status monitoring, snapshot triggering, and leader transfer.

use std::sync::Arc;
use std::time::Instant;

use tonic::{Request, Response, Status};
use tracing::info;

use rr_core::types::NodeId;
use rr_network::audit::AUDIT_TARGET;
use rr_network::proto::admin::admin_service_server::AdminService;
use rr_network::proto::admin::{
    Empty, GroupId, GroupStatus, MetricsSummary, NodeStatus, TransferLeaderRequest,
    TransferLeaderResponse, TriggerSnapshotResponse,
};
use rr_raft::group_manager::RaftGroupManager;

fn peer_from_request<T>(req: &Request<T>) -> String {
    req.remote_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Implementation of the AdminService gRPC interface.
///
/// Provides observability and management operations for the local node.
pub struct AdminServiceImpl {
    node_id: NodeId,
    listen_addr: String,
    group_manager: Arc<RaftGroupManager>,
    start_time: Instant,
}

impl AdminServiceImpl {
    /// Create a new `AdminServiceImpl`.
    pub fn new(
        node_id: NodeId,
        listen_addr: String,
        group_manager: Arc<RaftGroupManager>,
    ) -> Self {
        Self {
            node_id,
            listen_addr,
            group_manager,
            start_time: Instant::now(),
        }
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn get_node_status(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<NodeStatus>, Status> {
        let peer = peer_from_request(&request);
        info!(
            target: AUDIT_TARGET,
            op = "GetNodeStatus",
            peer = %peer,
            node_id = self.node_id,
            "admin rpc"
        );
        let group_ids = self.group_manager.group_ids().await;
        Ok(Response::new(NodeStatus {
            node_id: self.node_id,
            listen_addr: self.listen_addr.clone(),
            group_ids,
            uptime_secs: self.start_time.elapsed().as_secs(),
        }))
    }

    async fn get_group_status(
        &self,
        request: Request<GroupId>,
    ) -> Result<Response<GroupStatus>, Status> {
        let peer = peer_from_request(&request);
        let group_id = request.into_inner().group_id;
        info!(
            target: AUDIT_TARGET,
            op = "GetGroupStatus",
            peer = %peer,
            node_id = self.node_id,
            group_id,
            "admin rpc"
        );
        let raft = self
            .group_manager
            .get_raft(group_id)
            .await
            .ok_or_else(|| Status::not_found(format!("group {group_id} not found")))?;

        let metrics = raft.metrics().borrow().clone();
        Ok(Response::new(GroupStatus {
            group_id,
            is_leader: metrics.current_leader == Some(self.node_id),
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index.unwrap_or(0),
            leader_node_id: metrics.current_leader.unwrap_or(0),
        }))
    }

    async fn trigger_snapshot(
        &self,
        request: Request<GroupId>,
    ) -> Result<Response<TriggerSnapshotResponse>, Status> {
        let peer = peer_from_request(&request);
        let group_id = request.into_inner().group_id;
        info!(
            target: AUDIT_TARGET,
            op = "TriggerSnapshot",
            peer = %peer,
            node_id = self.node_id,
            group_id,
            "admin rpc — mutation"
        );
        let raft = self
            .group_manager
            .get_raft(group_id)
            .await
            .ok_or_else(|| Status::not_found(format!("group {group_id} not found")))?;

        let result = raft.trigger().snapshot().await;
        let (success, message) = match &result {
            Ok(()) => (true, "snapshot triggered".to_string()),
            Err(e) => (false, e.to_string()),
        };
        info!(
            target: AUDIT_TARGET,
            op = "TriggerSnapshot",
            peer = %peer,
            group_id,
            success,
            message = %message,
            "admin rpc result"
        );
        Ok(Response::new(TriggerSnapshotResponse { success, message }))
    }

    async fn transfer_leader(
        &self,
        request: Request<TransferLeaderRequest>,
    ) -> Result<Response<TransferLeaderResponse>, Status> {
        let peer = peer_from_request(&request);
        let req = request.into_inner();
        info!(
            target: AUDIT_TARGET,
            op = "TransferLeader",
            peer = %peer,
            node_id = self.node_id,
            group_id = req.group_id,
            target_node_id = req.target_node_id,
            result = "started",
            "admin rpc — mutation"
        );
        let raft = match self.group_manager.get_raft(req.group_id).await {
            Some(r) => r,
            None => {
                info!(
                    target: AUDIT_TARGET,
                    op = "TransferLeader",
                    peer = %peer,
                    group_id = req.group_id,
                    target_node_id = req.target_node_id,
                    success = false,
                    result = "failed",
                    reason = "group_not_found",
                    "admin rpc result"
                );
                return Err(Status::not_found(format!(
                    "group {} not found",
                    req.group_id
                )));
            }
        };

        let response = if req.target_node_id == self.node_id {
            match raft.trigger().elect().await {
                Ok(()) => TransferLeaderResponse {
                    success: true,
                    message: "election triggered on this node".to_string(),
                },
                Err(e) => TransferLeaderResponse {
                    success: false,
                    message: e.to_string(),
                },
            }
        } else {
            TransferLeaderResponse {
                success: false,
                message: format!(
                    "transfer to remote node {} not supported; call the target node's admin API",
                    req.target_node_id
                ),
            }
        };

        info!(
            target: AUDIT_TARGET,
            op = "TransferLeader",
            peer = %peer,
            group_id = req.group_id,
            target_node_id = req.target_node_id,
            success = response.success,
            result = if response.success { "succeeded" } else { "failed" },
            message = %response.message,
            "admin rpc result"
        );
        Ok(Response::new(response))
    }

    async fn get_metrics_summary(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<MetricsSummary>, Status> {
        let peer = peer_from_request(&request);
        info!(
            target: AUDIT_TARGET,
            op = "GetMetricsSummary",
            peer = %peer,
            node_id = self.node_id,
            "admin rpc"
        );
        Ok(Response::new(MetricsSummary {
            total_proposals: 0,
            total_applied: 0,
            total_snapshots: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rr_core::error::RrError;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;
    use rr_raft::group_manager::StateMachineFactory;

    struct NoopSM;

    #[async_trait]
    impl StateMachine for NoopSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }
        async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    fn noop_factory() -> StateMachineFactory {
        Box::new(|_group_id| Box::new(NoopSM))
    }

    fn make_admin_service() -> AdminServiceImpl {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        AdminServiceImpl::new(1, "127.0.0.1:9090".to_string(), mgr)
    }

    #[test]
    fn test_admin_service_new() {
        let svc = make_admin_service();
        assert_eq!(svc.node_id, 1);
        assert_eq!(svc.listen_addr, "127.0.0.1:9090");
    }

    #[tokio::test]
    async fn test_get_node_status() {
        let svc = make_admin_service();
        let resp = svc
            .get_node_status(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.node_id, 1);
        assert_eq!(resp.listen_addr, "127.0.0.1:9090");
        assert!(resp.group_ids.is_empty());
    }

    #[tokio::test]
    async fn test_get_group_status_not_found() {
        let svc = make_admin_service();
        let result = svc
            .get_group_status(Request::new(GroupId { group_id: 42 }))
            .await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_trigger_snapshot_not_found() {
        let svc = make_admin_service();
        let result = svc
            .trigger_snapshot(Request::new(GroupId { group_id: 99 }))
            .await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_get_metrics_summary() {
        let svc = make_admin_service();
        let resp = svc
            .get_metrics_summary(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.total_proposals, 0);
        assert_eq!(resp.total_applied, 0);
        assert_eq!(resp.total_snapshots, 0);
    }
}
