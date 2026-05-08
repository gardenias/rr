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

use openraft::type_config::async_runtime::WatchReceiver;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use rr_core::types::NodeId;
use rr_network::audit::AUDIT_TARGET;
use rr_network::error_convert::IntoStatus;
use rr_network::proto::admin::admin_service_server::AdminService;
use rr_network::proto::admin::{
    online_backup_chunk::Body as OnlineBackupBody, AddLearnerRequest, ChangeMembershipRequest,
    Empty, GroupId, GroupMembership, GroupStatus, MetricsSummary, NodeMember, NodeStatus,
    OnlineBackupChunk, OnlineBackupMetadata, OnlineBackupRequest, ResetIngestorOffsetRequest,
    TransferLeaderRequest,
};
use rr_raft::group_manager::RaftGroupManager;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

/// Per-chunk byte budget for [`AdminServiceImpl::online_backup`].
/// Matches the InstallSnapshot streaming chunk size used elsewhere
/// in rr so one tonic transport can handle either direction.
const ONLINE_BACKUP_CHUNK_BYTES: usize = 64 * 1024;

/// How long the server is willing to wait for a freshly-triggered
/// snapshot to materialise before giving up. Generous default because
/// snapshot generation can be slow on large state machines; tune via
/// future config if needed.
const ONLINE_BACKUP_SNAPSHOT_WAIT: Duration = Duration::from_secs(60);

fn peer_from_request<T>(req: &Request<T>) -> String {
    req.remote_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

async fn wait_for_snapshot(
    raft: &openraft::Raft<rr_raft::type_config::RrTypeConfig, rr_raft::sm_adapter::RrStateMachine>,
    mgr: &Arc<RaftGroupManager>,
    group_id: u64,
) -> Result<Vec<u8>, Status> {
    let deadline = std::time::Instant::now() + ONLINE_BACKUP_SNAPSHOT_WAIT;
    loop {
        if let Some(bytes) = mgr
            .fetch_snapshot_bytes(group_id)
            .await
            .map_err(|e| Status::internal(format!("fetch_snapshot_bytes: {e}")))?
        {
            return Ok(bytes);
        }
        if std::time::Instant::now() > deadline {
            return Err(Status::deadline_exceeded(
                "no snapshot materialised within ONLINE_BACKUP_SNAPSHOT_WAIT; \
                 retry with force_fresh_snapshot=true if the group has no prior snapshot",
            ));
        }
        if raft.metrics().borrow_watched().snapshot.is_some() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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
    pub fn new(node_id: NodeId, listen_addr: String, group_manager: Arc<RaftGroupManager>) -> Self {
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

        let metrics = raft.metrics().borrow_watched().clone();
        Ok(Response::new(GroupStatus {
            group_id,
            is_leader: metrics.current_leader == Some(self.node_id),
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index.unwrap_or(0),
            leader_node_id: metrics.current_leader.unwrap_or(0),
        }))
    }

    async fn trigger_snapshot(&self, request: Request<GroupId>) -> Result<Response<Empty>, Status> {
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

        match raft.trigger().snapshot().await {
            Ok(()) => {
                info!(
                    target: AUDIT_TARGET,
                    op = "TriggerSnapshot",
                    peer = %peer,
                    group_id,
                    success = true,
                    message = "snapshot triggered",
                    "admin rpc result"
                );
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                let message = e.to_string();
                info!(
                    target: AUDIT_TARGET,
                    op = "TriggerSnapshot",
                    peer = %peer,
                    group_id,
                    success = false,
                    message = %message,
                    "admin rpc result"
                );
                Err(Status::internal(message))
            }
        }
    }

    async fn transfer_leader(
        &self,
        request: Request<TransferLeaderRequest>,
    ) -> Result<Response<Empty>, Status> {
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

        if req.target_node_id != self.node_id {
            let message = format!(
                "transfer to remote node {} not supported; call the target node's admin API",
                req.target_node_id
            );
            info!(
                target: AUDIT_TARGET,
                op = "TransferLeader",
                peer = %peer,
                group_id = req.group_id,
                target_node_id = req.target_node_id,
                success = false,
                result = "failed",
                message = %message,
                "admin rpc result"
            );
            return Err(Status::failed_precondition(message));
        }

        match raft.trigger().elect().await {
            Ok(()) => {
                info!(
                    target: AUDIT_TARGET,
                    op = "TransferLeader",
                    peer = %peer,
                    group_id = req.group_id,
                    target_node_id = req.target_node_id,
                    success = true,
                    result = "succeeded",
                    message = "election triggered on this node",
                    "admin rpc result"
                );
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                let message = e.to_string();
                info!(
                    target: AUDIT_TARGET,
                    op = "TransferLeader",
                    peer = %peer,
                    group_id = req.group_id,
                    target_node_id = req.target_node_id,
                    success = false,
                    result = "failed",
                    message = %message,
                    "admin rpc result"
                );
                Err(Status::internal(message))
            }
        }
    }

    async fn add_learner(
        &self,
        request: Request<AddLearnerRequest>,
    ) -> Result<Response<Empty>, Status> {
        let peer = peer_from_request(&request);
        let req = request.into_inner();
        info!(
            target: AUDIT_TARGET,
            op = "AddLearner",
            peer = %peer,
            node_id = self.node_id,
            group_id = req.group_id,
            target_node_id = req.node_id,
            target_addr = %req.addr,
            result = "started",
            "admin rpc — mutation"
        );
        let outcome = self
            .group_manager
            .add_learner(req.group_id, req.node_id, req.addr.clone())
            .await;
        match outcome {
            Ok(()) => {
                info!(
                    target: AUDIT_TARGET,
                    op = "AddLearner",
                    peer = %peer,
                    group_id = req.group_id,
                    target_node_id = req.node_id,
                    success = true,
                    result = "succeeded",
                    message = "learner added",
                    "admin rpc result"
                );
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                let status = e.into_status();
                info!(
                    target: AUDIT_TARGET,
                    op = "AddLearner",
                    peer = %peer,
                    group_id = req.group_id,
                    target_node_id = req.node_id,
                    success = false,
                    result = "failed",
                    grpc_code = ?status.code(),
                    message = %status.message(),
                    "admin rpc result"
                );
                Err(status)
            }
        }
    }

    async fn change_membership(
        &self,
        request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<Empty>, Status> {
        let peer = peer_from_request(&request);
        let req = request.into_inner();
        let voters: BTreeSet<NodeId> = req.voter_node_ids.iter().copied().collect();
        info!(
            target: AUDIT_TARGET,
            op = "ChangeMembership",
            peer = %peer,
            node_id = self.node_id,
            group_id = req.group_id,
            voter_count = voters.len(),
            result = "started",
            "admin rpc — mutation"
        );
        if voters.is_empty() {
            let msg = "voter_node_ids must not be empty";
            info!(
                target: AUDIT_TARGET,
                op = "ChangeMembership",
                peer = %peer,
                group_id = req.group_id,
                success = false,
                result = "failed",
                reason = %msg,
                "admin rpc result"
            );
            return Err(Status::invalid_argument(msg));
        }
        let outcome = self
            .group_manager
            .change_membership(req.group_id, voters)
            .await;
        match outcome {
            Ok(()) => {
                info!(
                    target: AUDIT_TARGET,
                    op = "ChangeMembership",
                    peer = %peer,
                    group_id = req.group_id,
                    success = true,
                    result = "succeeded",
                    message = "membership changed",
                    "admin rpc result"
                );
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                let status = e.into_status();
                info!(
                    target: AUDIT_TARGET,
                    op = "ChangeMembership",
                    peer = %peer,
                    group_id = req.group_id,
                    success = false,
                    result = "failed",
                    grpc_code = ?status.code(),
                    message = %status.message(),
                    "admin rpc result"
                );
                Err(status)
            }
        }
    }

    async fn reset_ingestor_offset(
        &self,
        request: Request<ResetIngestorOffsetRequest>,
    ) -> Result<Response<Empty>, Status> {
        let peer = peer_from_request(&request);
        let req = request.into_inner();
        let group_id = req.group_id;
        let offset = req.offset;
        let new_offset = (offset != -1).then_some(offset);

        match self
            .group_manager
            .set_ingestor_offset(group_id, new_offset)
            .await
        {
            Ok(()) => {
                warn!(
                    target: AUDIT_TARGET,
                    op = "reset_ingestor_offset",
                    peer = %peer,
                    node_id = self.node_id,
                    group_id,
                    new_offset = offset,
                    result = "ok",
                    "admin rpc result"
                );
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                let status = e.into_status();
                warn!(
                    target: AUDIT_TARGET,
                    op = "reset_ingestor_offset",
                    peer = %peer,
                    node_id = self.node_id,
                    group_id,
                    new_offset = offset,
                    result = "failed",
                    grpc_code = ?status.code(),
                    error = %status.message(),
                    "admin rpc result"
                );
                Err(status)
            }
        }
    }

    async fn get_group_membership(
        &self,
        request: Request<GroupId>,
    ) -> Result<Response<GroupMembership>, Status> {
        let peer = peer_from_request(&request);
        let group_id = request.into_inner().group_id;
        info!(
            target: AUDIT_TARGET,
            op = "GetGroupMembership",
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
        let metrics = raft.metrics().borrow_watched().clone();
        let cfg = metrics.membership_config.membership();
        let voter_ids: BTreeSet<NodeId> = cfg.voter_ids().collect();
        let mut voters = Vec::new();
        let mut learners = Vec::new();
        for (nid, node) in cfg.nodes() {
            let member = NodeMember {
                node_id: *nid,
                addr: node.addr.clone(),
            };
            if voter_ids.contains(nid) {
                voters.push(member);
            } else {
                learners.push(member);
            }
        }
        Ok(Response::new(GroupMembership {
            group_id,
            voters,
            learners,
        }))
    }

    type OnlineBackupStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<OnlineBackupChunk, Status>> + Send>,
    >;

    async fn online_backup(
        &self,
        request: Request<OnlineBackupRequest>,
    ) -> Result<Response<Self::OnlineBackupStream>, Status> {
        let peer = peer_from_request(&request);
        let req = request.into_inner();
        let group_id = req.group_id;
        info!(
            target: AUDIT_TARGET,
            op = "OnlineBackup",
            peer = %peer,
            node_id = self.node_id,
            group_id,
            force_fresh_snapshot = req.force_fresh_snapshot,
            result = "started",
            "admin rpc — read"
        );

        let raft = self
            .group_manager
            .get_raft(group_id)
            .await
            .ok_or_else(|| Status::not_found(format!("group {group_id} not found")))?;

        if req.force_fresh_snapshot {
            raft.trigger()
                .snapshot()
                .await
                .map_err(|e| Status::internal(format!("trigger snapshot failed: {e}")))?;
        }

        let snapshot_bytes = wait_for_snapshot(&raft, &self.group_manager, group_id).await?;

        let metadata_metrics = raft.metrics().borrow_watched().clone();
        let metadata = OnlineBackupMetadata {
            group_id,
            last_log_index: metadata_metrics
                .snapshot
                .as_ref()
                .map(|l| l.index)
                .unwrap_or(0),
            last_log_term: metadata_metrics
                .snapshot
                .as_ref()
                .map(|l| l.leader_id.term)
                .unwrap_or(0),
            size_bytes: snapshot_bytes.len() as u64,
            rr_version: env!("CARGO_PKG_VERSION").to_string(),
        };

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let peer_for_audit = peer.clone();
        let node_id = self.node_id;
        tokio::spawn(async move {
            let total = snapshot_bytes.len();
            let metadata_chunk = OnlineBackupChunk {
                body: Some(OnlineBackupBody::Metadata(metadata)),
            };
            if tx.send(Ok(metadata_chunk)).await.is_err() {
                return;
            }
            let mut sent = 0usize;
            for chunk in snapshot_bytes.chunks(ONLINE_BACKUP_CHUNK_BYTES) {
                let data = chunk.to_vec();
                sent += data.len();
                let msg = OnlineBackupChunk {
                    body: Some(OnlineBackupBody::Data(data)),
                };
                if tx.send(Ok(msg)).await.is_err() {
                    info!(
                        target: AUDIT_TARGET,
                        op = "OnlineBackup",
                        peer = %peer_for_audit,
                        node_id,
                        group_id,
                        sent_bytes = sent,
                        total_bytes = total,
                        success = false,
                        result = "failed",
                        reason = "client_disconnected",
                        "admin rpc result"
                    );
                    return;
                }
            }
            info!(
                target: AUDIT_TARGET,
                op = "OnlineBackup",
                peer = %peer_for_audit,
                node_id,
                group_id,
                sent_bytes = sent,
                total_bytes = total,
                success = true,
                result = "succeeded",
                "admin rpc result"
            );
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::OnlineBackupStream
        ))
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
    use bincode::config::standard;
    use rr_core::error::RrError;
    use rr_core::service::Service;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;
    use rr_raft::group_manager::StateMachineFactory;
    use rr_raft::service_adapter::ServiceStateMachineAdapter;

    struct NoopSM;

    #[derive(Clone)]
    struct NoopService;

    #[async_trait]
    impl Service for NoopService {
        async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
            Ok(request)
        }

        fn name(&self) -> &str {
            "noop"
        }

        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(Vec::new())
        }

        async fn restore(&mut self, _data: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
    }

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

    fn offset_capable_factory() -> StateMachineFactory {
        Box::new(|group_id| Box::new(ServiceStateMachineAdapter::new(group_id, NoopService)))
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

    #[tokio::test]
    async fn test_add_learner_unknown_group_returns_not_found() {
        let svc = make_admin_service();
        let result = svc
            .add_learner(Request::new(AddLearnerRequest {
                group_id: 99,
                node_id: 2,
                addr: "127.0.0.1:5001".to_string(),
            }))
            .await;
        let err = result.expect_err("expected add_learner to fail for unknown group");
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_change_membership_empty_voters_rejected() {
        let svc = make_admin_service();
        let result = svc
            .change_membership(Request::new(ChangeMembershipRequest {
                group_id: 1,
                voter_node_ids: vec![],
            }))
            .await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_change_membership_unknown_group_returns_not_found() {
        let svc = make_admin_service();
        let result = svc
            .change_membership(Request::new(ChangeMembershipRequest {
                group_id: 99,
                voter_node_ids: vec![1, 2, 3],
            }))
            .await;
        let err = result.expect_err("expected change_membership to fail for unknown group");
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_get_group_membership_unknown_group() {
        let svc = make_admin_service();
        let result = svc
            .get_group_membership(Request::new(GroupId { group_id: 99 }))
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_online_backup_unknown_group_returns_not_found() {
        let svc = make_admin_service();
        let result = svc
            .online_backup(Request::new(OnlineBackupRequest {
                group_id: 99,
                force_fresh_snapshot: false,
            }))
            .await;
        let err = match result {
            Ok(_) => panic!("expected NotFound, got Ok"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[test]
    fn online_backup_chunk_size_matches_install_snapshot_convention() {
        assert_eq!(ONLINE_BACKUP_CHUNK_BYTES, 64 * 1024);
    }

    #[tokio::test]
    async fn test_reset_ingestor_offset_updates_group_manager_offset() {
        let mgr = Arc::new(RaftGroupManager::new(1, offset_capable_factory()));
        mgr.start_group(1).await.unwrap();
        let svc = AdminServiceImpl::new(1, "127.0.0.1:9090".to_string(), mgr.clone());

        svc.reset_ingestor_offset(Request::new(ResetIngestorOffsetRequest {
            group_id: 1,
            offset: 100,
        }))
        .await
        .unwrap();

        let raw = mgr
            .latest_ingestor_offset(1)
            .await
            .unwrap()
            .expect("offset bytes should be present");
        let (decoded, _): (i64, usize) = bincode::serde::decode_from_slice(&raw, standard())
            .expect("stored bytes should decode as i64");
        assert_eq!(decoded, 100);

        svc.reset_ingestor_offset(Request::new(ResetIngestorOffsetRequest {
            group_id: 1,
            offset: -1,
        }))
        .await
        .unwrap();
        assert!(mgr.latest_ingestor_offset(1).await.unwrap().is_none());
    }
}
