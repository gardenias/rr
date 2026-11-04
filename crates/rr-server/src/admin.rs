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

use rr_core::config::ConfigProvider;
use rr_core::error::RaftError;
use rr_core::membership::{membership_change_is_safe, MIN_VOTERS_AFTER_REMOVAL};
use rr_core::types::NodeId;
use rr_core::{FormationState, LifecycleStore};
use rr_network::audit::AUDIT_TARGET;
use rr_network::error_convert::IntoStatus;
use rr_network::proto::admin::admin_service_server::AdminService;
use rr_network::proto::admin::{
    online_backup_chunk::Body as OnlineBackupBody, AddLearnerRequest, ChangeMembershipRequest,
    DrainRequest, DrainResult, Empty, GroupDrainStatus, GroupId, GroupList, GroupMembership,
    GroupStatus, GroupSummary, MetricsSummary, NodeMember, NodeStatus, OnlineBackupChunk,
    OnlineBackupMetadata, OnlineBackupRequest, PartitionRoute, RemoveGroupMemberRequest,
    ResetIngestorOffsetRequest, RoutingTableSnapshot, SnapshotStatus, TransferLeaderRequest,
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
        if let Some(bytes) = mgr.fetch_snapshot_bytes(group_id).await.map_err(|e| {
            tracing::warn!(
                group_id,
                error = %e,
                "fetch_snapshot_bytes failed (detail sanitized for client)"
            );
            Status::internal("snapshot fetch failed")
        })? {
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
    config_provider: Arc<dyn ConfigProvider>,
    lifecycle_store: Option<Arc<dyn LifecycleStore>>,
    start_time: Instant,
    /// Tracks every spawned `OnlineBackup` streaming task (no
    /// fire-and-forget): each task terminates itself when the client
    /// disconnects (channel send fails), and finished tasks are reaped
    /// on the next `online_backup` call.
    backup_streams: std::sync::Mutex<tokio::task::JoinSet<()>>,
    /// One-shot guard so the `snapshot_in_progress` stub (always `false`)
    /// warns once per service instance instead of on every call.
    snapshot_stub_warned: std::sync::atomic::AtomicBool,
}

impl AdminServiceImpl {
    /// Create a new `AdminServiceImpl`.
    pub fn new(
        node_id: NodeId,
        listen_addr: String,
        group_manager: Arc<RaftGroupManager>,
        config_provider: Arc<dyn ConfigProvider>,
    ) -> Self {
        Self {
            node_id,
            listen_addr,
            group_manager,
            config_provider,
            lifecycle_store: None,
            start_time: Instant::now(),
            backup_streams: std::sync::Mutex::new(tokio::task::JoinSet::new()),
            snapshot_stub_warned: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn with_lifecycle_store(mut self, lifecycle_store: Arc<dyn LifecycleStore>) -> Self {
        self.lifecycle_store = Some(lifecycle_store);
        self
    }

    async fn require_active_group(&self, group_id: u64) -> Result<(), Status> {
        let Some(store) = &self.lifecycle_store else {
            return Ok(());
        };
        let lifecycle = store
            .load_linearizable()
            .await
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "group {group_id} lifecycle state unavailable; retry membership change after store recovery: {error}"
                ))
            })?
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "group {group_id} lifecycle record unavailable; retry membership change after activation"
                ))
            })?;
        if lifecycle
            .record
            .groups
            .get(&group_id)
            .is_some_and(|group| group.state == FormationState::Active)
        {
            Ok(())
        } else {
            Err(Status::failed_precondition(format!(
                "group {group_id} lifecycle is not Active; retry membership change after activation"
            )))
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

    async fn list_groups(&self, request: Request<Empty>) -> Result<Response<GroupList>, Status> {
        let peer = peer_from_request(&request);
        info!(
            target: AUDIT_TARGET,
            op = "ListGroups",
            peer = %peer,
            node_id = self.node_id,
            "admin rpc"
        );
        let mut group_ids = self.group_manager.group_ids().await;
        group_ids.sort_unstable();
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            if let Some(raft) = self.group_manager.get_raft(group_id).await {
                let metrics = raft.metrics().borrow_watched().clone();
                groups.push(GroupSummary {
                    group_id,
                    is_leader: metrics.current_leader == Some(self.node_id),
                    current_term: metrics.current_term,
                    last_log_index: metrics.last_log_index.unwrap_or(0),
                    leader_node_id: metrics.current_leader,
                });
            }
        }
        Ok(Response::new(GroupList { groups }))
    }

    /// Return snapshot state for a group.
    ///
    /// **STUB FIELD — do not rely on `snapshot_in_progress`:** it is
    /// hardcoded `false` for wire compatibility. openraft 0.10 metrics
    /// expose the latest *completed* snapshot id but no "snapshot build in
    /// progress" runtime flag, so there is no truthful value to return
    /// here in v0.3.0. The field is deprecated in `admin.proto`; use
    /// `last_snapshot_index` / `last_snapshot_term` instead. A WARN is
    /// logged once per service instance on first use.
    async fn get_snapshot_status(
        &self,
        request: Request<GroupId>,
    ) -> Result<Response<SnapshotStatus>, Status> {
        let peer = peer_from_request(&request);
        let group_id = request.into_inner().group_id;
        info!(
            target: AUDIT_TARGET,
            op = "GetSnapshotStatus",
            peer = %peer,
            node_id = self.node_id,
            group_id,
            "admin rpc"
        );
        // Loud honesty: the stubbed field below must not pass silently.
        if !self
            .snapshot_stub_warned
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            warn!(
                target: AUDIT_TARGET,
                op = "GetSnapshotStatus",
                peer = %peer,
                node_id = self.node_id,
                group_id,
                "GetSnapshotStatus: snapshot_in_progress is a deprecated stub and is \
                 always false (openraft 0.10 exposes no in-progress flag); use \
                 last_snapshot_index / last_snapshot_term instead"
            );
        }
        let raft = self
            .group_manager
            .get_raft(group_id)
            .await
            .ok_or_else(|| Status::not_found(format!("group {group_id} not found")))?;
        let metrics = raft.metrics().borrow_watched().clone();
        let last_snapshot_index = metrics.snapshot.as_ref().map(|log_id| log_id.index);
        let last_snapshot_term = metrics
            .snapshot
            .as_ref()
            .map(|log_id| log_id.leader_id.term);

        // Deprecated stub — see the method rustdoc; always false.
        let snapshot_in_progress = false;

        Ok(Response::new(SnapshotStatus {
            group_id,
            snapshot_in_progress,
            last_snapshot_index,
            last_snapshot_term,
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

        match raft.trigger().elect(false).await {
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

    async fn drain_node(
        &self,
        request: Request<DrainRequest>,
    ) -> Result<Response<DrainResult>, Status> {
        let peer = peer_from_request(&request);
        let req = request.into_inner();
        info!(
            target: AUDIT_TARGET,
            op = "DrainNode",
            peer = %peer,
            node_id = self.node_id,
            requested_node_id = req.node_id,
            timeout_secs = req.timeout_secs,
            "admin rpc — mutation"
        );

        if req.node_id != self.node_id {
            info!(
                target: AUDIT_TARGET,
                op = "DrainNode",
                peer = %peer,
                node_id = self.node_id,
                requested_node_id = req.node_id,
                success = true,
                message = "drain request is for a different node; no-op",
                "admin rpc result"
            );
            return Ok(Response::new(DrainResult { groups: Vec::new() }));
        }

        let mut group_ids = self.group_manager.group_ids().await;
        group_ids.sort_unstable();
        let timeout = Duration::from_secs(req.timeout_secs as u64);
        let mut groups = Vec::with_capacity(group_ids.len());

        for group_id in group_ids {
            let Some(raft) = self.group_manager.get_raft(group_id).await else {
                groups.push(GroupDrainStatus {
                    group_id,
                    leadership_transferred: false,
                    message: "group not found during drain".to_string(),
                });
                continue;
            };

            let metrics = raft.metrics().borrow_watched().clone();
            let is_local_leader = metrics.current_leader == Some(self.node_id);
            if !is_local_leader {
                groups.push(GroupDrainStatus {
                    group_id,
                    leadership_transferred: true,
                    message: "already not leader".to_string(),
                });
                continue;
            }

            let membership = metrics.membership_config.membership();
            let target = membership
                .voter_ids()
                .find(|node_id| *node_id != self.node_id);
            let Some(target_node_id) = target else {
                groups.push(GroupDrainStatus {
                    group_id,
                    leadership_transferred: false,
                    message: "no other voter available for leadership transfer".to_string(),
                });
                continue;
            };

            if let Err(e) = raft.trigger().transfer_leader(target_node_id).await {
                groups.push(GroupDrainStatus {
                    group_id,
                    leadership_transferred: false,
                    message: format!("transfer_leader trigger failed: {e}"),
                });
                continue;
            }

            if req.timeout_secs == 0 {
                groups.push(GroupDrainStatus {
                    group_id,
                    leadership_transferred: true,
                    message: format!(
                        "transfer requested to node {target_node_id} (best-effort, no wait)"
                    ),
                });
                continue;
            }

            let deadline = std::time::Instant::now() + timeout;
            let mut transferred = false;
            while std::time::Instant::now() < deadline {
                let current = raft.metrics().borrow_watched().clone();
                if current.current_leader != Some(self.node_id) {
                    let leader_desc = current
                        .current_leader
                        .map(|node_id| node_id.to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    groups.push(GroupDrainStatus {
                        group_id,
                        leadership_transferred: true,
                        message: format!("leadership moved to node {leader_desc}"),
                    });
                    transferred = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            if !transferred {
                groups.push(GroupDrainStatus {
                    group_id,
                    leadership_transferred: false,
                    message: format!(
                        "timeout waiting for leadership transfer after {}s",
                        req.timeout_secs
                    ),
                });
            }
        }

        let transferred = groups.iter().filter(|g| g.leadership_transferred).count();
        info!(
            target: AUDIT_TARGET,
            op = "DrainNode",
            peer = %peer,
            node_id = self.node_id,
            requested_node_id = req.node_id,
            success = true,
            groups_total = groups.len(),
            groups_transferred = transferred,
            message = "drain finished",
            "admin rpc result"
        );
        Ok(Response::new(DrainResult { groups }))
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
        self.require_active_group(req.group_id).await?;
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
        let force = req.force;
        info!(
            target: AUDIT_TARGET,
            op = "ChangeMembership",
            peer = %peer,
            node_id = self.node_id,
            group_id = req.group_id,
            voter_count = voters.len(),
            force,
            result = "started",
            "admin rpc — mutation"
        );
        self.require_active_group(req.group_id).await?;
        // 1. Group existence (same not_found semantics as `get_group_membership`).
        let raft = match self.group_manager.get_raft(req.group_id).await {
            Some(raft) => raft,
            None => {
                let status = Status::not_found(format!("group {} not found", req.group_id));
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
                return Err(status);
            }
        };
        // 2. Empty voter set: rejected unconditionally. `force` does NOT
        //    bypass this — n→0 is group teardown and must go through
        //    `node-reset`, not change-membership (ADR A-0012 §4).
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
        // 3. Quorum guard (ADR A-0012): reject target voter sets below
        //    MIN_VOTERS_AFTER_REMOVAL unless the operator passed `force`.
        //    The membership view is read from the same source as
        //    `GetGroupMembership` (leader's `raft.metrics()`); the
        //    decision itself depends only on the target size — openraft's
        //    full-replacement semantics make each applied request
        //    self-sufficient, so no TOCTOU composition is possible.
        let current_voters = {
            let metrics = raft.metrics().borrow_watched().clone();
            metrics.membership_config.membership().voter_ids().count()
        };
        if !membership_change_is_safe(voters.len()) {
            if force {
                warn!(
                    target: AUDIT_TARGET,
                    op = "ChangeMembership",
                    peer = %peer,
                    node_id = self.node_id,
                    group_id = req.group_id,
                    force = true,
                    current_voters,
                    target_voters = voters.len(),
                    // The exact set being forced, not just its size — the
                    // audit trail must reconstruct WHICH voters were forced.
                    voters = ?voters,
                    result = "force_accepted",
                    "quorum guard overridden by operator"
                );
            } else {
                let status = RaftError::UnsafeMembershipChange {
                    target_voters: voters.len(),
                    min: MIN_VOTERS_AFTER_REMOVAL,
                }
                .into_status();
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
                return Err(status);
            }
        }
        // 4. Guard passed — apply through the manager.
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

    async fn remove_group_member(
        &self,
        request: Request<RemoveGroupMemberRequest>,
    ) -> Result<Response<Empty>, Status> {
        let peer = peer_from_request(&request);
        let req = request.into_inner();
        let force = req.force;
        info!(
            target: AUDIT_TARGET,
            op = "RemoveGroupMember",
            peer = %peer,
            node_id = self.node_id,
            group_id = req.group_id,
            target_node_id = req.node_id,
            force,
            result = "started",
            "admin rpc — mutation"
        );
        self.require_active_group(req.group_id).await?;

        let _raft = match self.group_manager.get_raft(req.group_id).await {
            Some(raft) => raft,
            None => {
                let status = Status::not_found(format!("group {} not found", req.group_id));
                info!(
                    target: AUDIT_TARGET,
                    op = "RemoveGroupMember",
                    peer = %peer,
                    group_id = req.group_id,
                    target_node_id = req.node_id,
                    success = false,
                    result = "failed",
                    grpc_code = ?status.code(),
                    message = %status.message(),
                    "admin rpc result"
                );
                return Err(status);
            }
        };

        match self
            .group_manager
            .remove_node_with_policy_report(req.group_id, req.node_id, req.force)
            .await
        {
            Ok(outcome) => {
                if outcome.forced {
                    warn!(
                        target: AUDIT_TARGET,
                        op = "RemoveGroupMember",
                        peer = %peer,
                        node_id = self.node_id,
                        group_id = req.group_id,
                        target_node_id = req.node_id,
                        force = true,
                        current_voters = outcome.current_voters,
                        target_voters = outcome.target_voters,
                        result = "force_accepted",
                        "quorum guard overridden by operator"
                    );
                }
                info!(
                    target: AUDIT_TARGET,
                    op = "RemoveGroupMember",
                    peer = %peer,
                    group_id = req.group_id,
                    target_node_id = req.node_id,
                    success = true,
                    result = "succeeded",
                    message = "group member removed",
                    "admin rpc result"
                );
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                let status = e.into_status();
                info!(
                    target: AUDIT_TARGET,
                    op = "RemoveGroupMember",
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

    async fn reset_ingestor_offset(
        &self,
        request: Request<ResetIngestorOffsetRequest>,
    ) -> Result<Response<Empty>, Status> {
        let peer = peer_from_request(&request);
        let req = request.into_inner();
        let group_id = req.group_id;
        let offset = req.offset;
        let new_offset = (offset != -1).then_some(offset);

        info!(
            target: AUDIT_TARGET,
            op = "reset_ingestor_offset",
            peer = %peer,
            node_id = self.node_id,
            group_id,
            offset,
            "admin rpc — mutation"
        );

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
            raft.trigger().snapshot().await.map_err(|e| {
                tracing::warn!(
                    group_id,
                    error = %e,
                    "trigger snapshot failed (detail sanitized for client)"
                );
                Status::internal("trigger snapshot failed")
            })?;
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
        {
            let mut streams = self
                .backup_streams
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Reap finished stream tasks so the set stays bounded.
            while let Some(outcome) = streams.try_join_next() {
                if let Err(e) = outcome {
                    warn!(error = %e, "online backup stream task failed");
                }
            }
            streams.spawn(async move {
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
        }

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::OnlineBackupStream
        ))
    }

    async fn get_routing_table(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<RoutingTableSnapshot>, Status> {
        let peer = peer_from_request(&request);
        info!(
            target: AUDIT_TARGET,
            op = "GetRoutingTable",
            peer = %peer,
            node_id = self.node_id,
            "admin rpc"
        );
        let table = self
            .config_provider
            .get_routing_table()
            .await
            .map_err(|e| {
                tracing::warn!(
                    error = %e,
                    "get_routing_table failed (detail sanitized for client)"
                );
                Status::internal("routing table unavailable")
            })?;
        let groups_meta = table.groups;
        let mut partitions: Vec<PartitionRoute> = table
            .partitions
            .into_iter()
            .map(|(partition_id, (_cfg, group_id))| PartitionRoute {
                partition_id,
                group_id,
                replica_node_ids: groups_meta
                    .get(&group_id)
                    .map(|meta| meta.replicas.clone())
                    .unwrap_or_default(),
            })
            .collect();
        partitions.sort_unstable_by_key(|route| route.partition_id);
        Ok(Response::new(RoutingTableSnapshot { partitions }))
    }

    /// **NOT IMPLEMENTED in v0.3.0** — returns `Status::unimplemented`.
    ///
    /// This RPC previously returned constant zeros, a silent wrong answer.
    /// The truthful per-node counters live in the Prometheus recorder owned
    /// by the node (not reachable from this service); scrape the HTTP
    /// `/metrics` endpoint instead. The `MetricsSummary` fields are
    /// deprecated in `admin.proto`.
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
        // Loud honesty: this RPC previously returned constant zeros — a
        // silent wrong answer. The real per-node counters live in the
        // Prometheus recorder owned by the node, which this service does
        // not have access to, so there is no truthful value to return
        // here in v0.3.0.
        Err(Status::unimplemented(
            "GetMetricsSummary is not available: it previously returned constant zeros, \
             a silent wrong answer. Scrape the Prometheus /metrics endpoint instead \
             (rr_server_requests_total, rr_raft_applied_entries_total, ...)",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bincode::config::standard;
    use rr_config::mem::MemConfigProvider;
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
        let config_provider = Arc::new(MemConfigProvider::new());
        AdminServiceImpl::new(1, "127.0.0.1:9090".to_string(), mgr, config_provider)
    }

    /// Start and initialize a single-node group so `change_membership`
    /// requests reach the quorum guard with a committed membership view
    /// (and the node is the group's leader, so a guarded pass-through
    /// can actually be applied by raft).
    async fn make_admin_service_with_group() -> AdminServiceImpl {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        mgr.start_group(1).await.unwrap();
        let mut members = std::collections::BTreeMap::new();
        members.insert(1u64, openraft::BasicNode::default());
        mgr.init_cluster(1, members).await.unwrap();
        let raft = mgr.get_raft(1).await.unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let metrics = raft.metrics().borrow_watched().clone();
            if metrics.current_leader == Some(1) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "single-node self-election timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let config_provider = Arc::new(MemConfigProvider::new());
        AdminServiceImpl::new(1, "127.0.0.1:9090".to_string(), mgr, config_provider)
    }

    /// In-memory `MakeWriter` so tests can assert on the `rr::audit`
    /// event stream a handler emits.
    #[derive(Clone, Default)]
    struct AuditBuf {
        bytes: Arc<std::sync::Mutex<Vec<u8>>>,
    }

    impl std::io::Write for AuditBuf {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.bytes.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for AuditBuf {
        type Writer = AuditBuf;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl AuditBuf {
        fn contents(&self) -> String {
            String::from_utf8(self.bytes.lock().unwrap().clone()).unwrap()
        }
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

    // The RPC must fail loudly (Unimplemented) rather than return constant
    // zeros — a silent wrong answer. Real per-node counters are on the
    // Prometheus /metrics endpoint.
    #[tokio::test]
    async fn test_get_metrics_summary() {
        let svc = make_admin_service();
        let err = svc
            .get_metrics_summary(Request::new(Empty {}))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("/metrics"));
    }

    // The deprecated `snapshot_in_progress` stub must stay wire-compatible
    // (always false) AND be loud: exactly one WARN per service instance.
    #[tokio::test]
    async fn test_get_snapshot_status_stub_warns_once_and_reports_false() {
        let svc = make_admin_service_with_group().await;
        let buf = AuditBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        for _ in 0..3 {
            let resp = svc
                .get_snapshot_status(Request::new(GroupId { group_id: 1 }))
                .await
                .unwrap()
                .into_inner();
            assert!(
                !resp.snapshot_in_progress,
                "deprecated stub field must remain wire-compatible (false)"
            );
        }

        let log = buf.contents();
        let hits = log
            .matches("snapshot_in_progress is a deprecated stub")
            .count();
        assert_eq!(hits, 1, "stub warn must fire exactly once, got {hits}");
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
        let svc = make_admin_service_with_group().await;
        // n→0 is teardown, not a membership change: rejected regardless
        // of `force` (operators use `node-reset` instead — ADR A-0012 §4).
        for force in [false, true] {
            let result = svc
                .change_membership(Request::new(ChangeMembershipRequest {
                    group_id: 1,
                    voter_node_ids: vec![],
                    force,
                }))
                .await;
            let status = result.expect_err("empty voter set must be rejected");
            assert_eq!(
                status.code(),
                tonic::Code::InvalidArgument,
                "force={force} must not bypass the empty-set check"
            );
        }
    }

    #[tokio::test]
    async fn test_change_membership_unknown_group_returns_not_found() {
        let svc = make_admin_service();
        let result = svc
            .change_membership(Request::new(ChangeMembershipRequest {
                group_id: 99,
                voter_node_ids: vec![1, 2, 3],
                force: false,
            }))
            .await;
        let err = result.expect_err("expected change_membership to fail for unknown group");
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_change_membership_quorum_guard_rejects_below_minimum() {
        let svc = make_admin_service_with_group().await;
        let result = svc
            .change_membership(Request::new(ChangeMembershipRequest {
                group_id: 1,
                voter_node_ids: vec![1],
                force: false,
            }))
            .await;
        let status = result.expect_err("target voter set below minimum must be rejected");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let msg = status.message();
        assert!(
            msg.contains("has 1 voter") && msg.contains("minimum 2"),
            "message must name the target voter count and the minimum; got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_change_membership_guard_allows_target_at_minimum() {
        let svc = make_admin_service_with_group().await;
        let result = svc
            .change_membership(Request::new(ChangeMembershipRequest {
                group_id: 1,
                voter_node_ids: vec![1, 2],
                force: false,
            }))
            .await;
        // The guard must let a ≥ MIN_VOTERS_AFTER_REMOVAL target through.
        // The request may still fail deeper in raft (node 2 is not a
        // learner in this single-node fixture) — anything but
        // FailedPrecondition proves the guard passed it on.
        match result {
            Ok(_) => {}
            Err(status) => assert_ne!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "guard must not block a 2-voter target"
            ),
        }
    }

    #[tokio::test]
    async fn test_change_membership_force_overrides_guard() {
        let svc = make_admin_service_with_group().await;
        // Single-node group: target {1} equals the current voter set, so
        // once the guard is bypassed the raft call itself succeeds —
        // proving the request reached the manager.
        svc.change_membership(Request::new(ChangeMembershipRequest {
            group_id: 1,
            voter_node_ids: vec![1],
            force: true,
        }))
        .await
        .expect("force must bypass the quorum guard and reach raft");
    }

    #[tokio::test]
    async fn test_change_membership_force_accepted_audit_event() {
        let buf = AuditBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let svc = make_admin_service_with_group().await;
        svc.change_membership(Request::new(ChangeMembershipRequest {
            group_id: 1,
            voter_node_ids: vec![1],
            force: true,
        }))
        .await
        .expect("force must bypass the quorum guard");

        let out = buf.contents();
        assert!(
            out.contains(AUDIT_TARGET) && out.contains("force_accepted"),
            "force override must emit a force_accepted audit event on {AUDIT_TARGET}; got:\n{out}"
        );
        assert!(
            out.contains("force=true"),
            "force_accepted event must carry force=true; got:\n{out}"
        );
    }

    #[tokio::test]
    async fn test_change_membership_guard_rejection_audit_event() {
        let buf = AuditBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let svc = make_admin_service_with_group().await;
        let result = svc
            .change_membership(Request::new(ChangeMembershipRequest {
                group_id: 1,
                voter_node_ids: vec![1],
                force: false,
            }))
            .await;
        assert_eq!(
            result.expect_err("guard must reject").code(),
            tonic::Code::FailedPrecondition
        );

        let out = buf.contents();
        assert!(
            out.contains(AUDIT_TARGET) && out.contains("membership change rejected"),
            "guard rejection must emit a failed audit event with the reason; got:\n{out}"
        );
    }

    #[tokio::test]
    async fn test_remove_group_member_unknown_group_returns_not_found() {
        let svc = make_admin_service();
        let result = svc
            .remove_group_member(Request::new(
                rr_network::proto::admin::RemoveGroupMemberRequest {
                    group_id: 99,
                    node_id: 2,
                    force: false,
                },
            ))
            .await;
        let err = result.expect_err("expected removal to fail for unknown group");
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_remove_group_member_cannot_remove_final_voter_even_with_force() {
        let svc = make_admin_service_with_group().await;
        for force in [false, true] {
            let result = svc
                .remove_group_member(Request::new(
                    rr_network::proto::admin::RemoveGroupMemberRequest {
                        group_id: 1,
                        node_id: 1,
                        force,
                    },
                ))
                .await;
            let status = result.expect_err("removing the final voter must be rejected");
            assert_eq!(
                status.code(),
                tonic::Code::InvalidArgument,
                "force={force} must not bypass the final-voter check"
            );
        }

        let membership = svc
            .get_group_membership(Request::new(GroupId { group_id: 1 }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            membership
                .voters
                .iter()
                .map(|member| member.node_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert!(membership.learners.is_empty());
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
        let config_provider = Arc::new(MemConfigProvider::new());
        let svc = AdminServiceImpl::new(
            1,
            "127.0.0.1:9090".to_string(),
            mgr.clone(),
            config_provider,
        );

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
