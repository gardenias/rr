//! Admin gRPC service implementation.
//!
//! 节点管理和可观测性 gRPC 服务，提供节点状态查询、Raft 组状态监控、快照触发和领导者转移等功能。
//!
//! ---
//!
//! Admin gRPC service for node management and observability. Provides node status queries,
//! Raft group status monitoring, snapshot triggering, and leader transfer.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use openraft::type_config::async_runtime::WatchReceiver;
use openraft::Instant as _;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::boot_journal::{BootRecord, ShutdownState};
use crate::runtime::{BuildSnapshot, NodeRuntime};
use rr_core::config::ConfigProvider;
use rr_core::error::RaftError;
use rr_core::membership::{membership_change_is_safe, MIN_VOTERS_AFTER_REMOVAL};
use rr_core::types::NodeId;
use rr_core::{ClusterOrigin, FormationState, LifecycleStore, VersionedLifecycle};
use rr_network::audit::AUDIT_TARGET;
use rr_network::error_convert::IntoStatus;
use rr_network::proto::admin::admin_service_server::AdminService;
use rr_network::proto::admin::{
    online_backup_chunk::Body as OnlineBackupBody, AddLearnerRequest,
    BootRecord as ProtoBootRecord, BootRecordPage as ProtoBootRecordPage, BootRecordsRequest,
    BootStatus, BuildInfo, ChangeMembershipRequest, ClusterTopology, ClusterTopologyRequest,
    DrainRequest, DrainResult, Empty, FormationStatus, GroupDrainStatus, GroupFormationStatus,
    GroupId, GroupList, GroupMembership, GroupReplicaStatus, GroupStatus, GroupSummary,
    GroupTopology, LeaderReplicaStatus, MetricsSummary, NodeDescriptor as ProtoNodeDescriptor,
    NodeDescriptorResponse, NodeDirectoryEntry, NodeMember, NodeStatus, OnlineBackupChunk,
    OnlineBackupMetadata, OnlineBackupRequest, PartitionRoute, ProtocolVersion,
    RemoveGroupMemberRequest, ResetIngestorOffsetRequest, RoutingTableSnapshot, SnapshotStatus,
    TransferLeaderRequest,
};
use rr_raft::group_manager::RaftGroupManager;
use std::collections::{BTreeMap, BTreeSet};
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
const TOPOLOGY_CURSOR_TTL: Duration = Duration::from_secs(30);
const DEFAULT_TOPOLOGY_PAGE_SIZE: usize = 50;
const MAX_TOPOLOGY_PAGE_SIZE: usize = 200;

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

/// `FormationStatus` shape for a node that holds no lifecycle record — either
/// the node runs without formation tracking (legacy cluster that was never
/// adopted) or the store is empty. This is a normal state, not an error.
fn untracked_formation_status() -> FormationStatus {
    FormationStatus {
        origin: "Untracked".to_string(),
        cluster_uuid: String::new(),
        topology_revision: 0,
        store_revision: 0,
        state: "Untracked".to_string(),
        groups: Vec::new(),
    }
}

/// Map the stored lifecycle record onto the wire shape. Aggregate state is
/// "Forming" while any group is still forming, "Active" once all are active.
fn formation_status_from_record(versioned: VersionedLifecycle) -> FormationStatus {
    let record = versioned.record;
    let origin = match record.origin {
        ClusterOrigin::Genesis => "Genesis",
        ClusterOrigin::LegacyAdopted => "LegacyAdopted",
    }
    .to_string();
    let any_forming = record
        .groups
        .values()
        .any(|group| group.state == FormationState::Forming);
    let groups = record
        .groups
        .iter()
        .map(|(group_id, formation)| GroupFormationStatus {
            group_id: *group_id,
            bootstrap_epoch: formation.bootstrap_epoch,
            state: match formation.state {
                FormationState::Forming => "Forming",
                FormationState::Active => "Active",
            }
            .to_string(),
        })
        .collect();
    FormationStatus {
        origin,
        cluster_uuid: record.cluster_uuid.hyphenated().to_string(),
        topology_revision: record.topology_revision,
        store_revision: versioned.revision,
        state: if any_forming { "Forming" } else { "Active" }.to_string(),
        groups,
    }
}

fn protocol_to_proto(version: rr_core::ProtocolVersion) -> ProtocolVersion {
    ProtocolVersion {
        major: version.major,
        minor: version.minor,
        min_peer_minor: version.min_peer_minor,
    }
}

fn build_info_from_snapshot(
    build: &BuildSnapshot,
    boot_id: &str,
    process_start_unix_ms: u64,
) -> BuildInfo {
    BuildInfo {
        rr_version: build.rr_version.clone(),
        app_version: build.app_version.clone(),
        git_commit: build.git_commit.clone(),
        git_dirty: build.git_dirty,
        build_profile: build.build_profile.clone(),
        admin_api: Some(protocol_to_proto(build.admin_protocol)),
        raft_wire: Some(protocol_to_proto(build.raft_protocol)),
        boot_id: boot_id.to_string(),
        process_start_unix_ms,
        config_fingerprint: build.config_fingerprint.clone(),
    }
}

fn boot_record_to_proto(record: BootRecord) -> ProtoBootRecord {
    ProtoBootRecord {
        boot_id: record.boot_id.clone(),
        started_at_unix_ms: record.started_at_unix_ms,
        ended_at_unix_ms: record.ended_at_unix_ms,
        shutdown_state: match record.shutdown_state {
            ShutdownState::Running => "RUNNING",
            ShutdownState::Graceful => "GRACEFUL",
            ShutdownState::AbnormalOrUncertain => "ABNORMAL_OR_UNCERTAIN",
        }
        .to_string(),
        build_info: Some(build_info_from_snapshot(
            &record.build,
            &record.boot_id,
            record.started_at_unix_ms,
        )),
    }
}

fn leader_replica_status(
    node_id: NodeId,
    last_log_index: Option<u64>,
    matched_index: Option<u64>,
    last_heartbeat_unix_ms: Option<u64>,
) -> LeaderReplicaStatus {
    LeaderReplicaStatus {
        node_id,
        matched_index,
        lag_entries: last_log_index
            .zip(matched_index)
            .map(|(last, matched)| last.saturating_sub(matched)),
        last_heartbeat_unix_ms,
    }
}

fn topology_page_size(raw: u32) -> Result<usize, Status> {
    let page_size = if raw == 0 {
        DEFAULT_TOPOLOGY_PAGE_SIZE
    } else {
        raw as usize
    };
    if page_size > MAX_TOPOLOGY_PAGE_SIZE {
        return Err(Status::invalid_argument(format!(
            "page_size must be within 1..={MAX_TOPOLOGY_PAGE_SIZE}"
        )));
    }
    Ok(page_size)
}

fn decode_topology_cursor(
    cursor: &str,
    snapshot_id: &str,
) -> Result<(usize, usize, usize), Status> {
    let mut parts = cursor.split(':');
    let (Some("v1"), Some(id), Some(nodes), Some(groups), Some(partitions), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(Status::invalid_argument("invalid topology cursor"));
    };
    if id != snapshot_id {
        return Err(Status::out_of_range(
            "topology cursor expired; restart pagination without a cursor",
        ));
    }
    let parse = |value: &str| {
        value
            .parse::<usize>()
            .map_err(|_| Status::invalid_argument("invalid topology cursor offset"))
    };
    Ok((parse(nodes)?, parse(groups)?, parse(partitions)?))
}

fn paginate_topology(
    snapshot: &ClusterTopology,
    cursor: Option<&str>,
    page_size: usize,
) -> Result<ClusterTopology, Status> {
    let (node_offset, group_offset, partition_offset) = match cursor {
        Some(cursor) if !cursor.is_empty() => {
            decode_topology_cursor(cursor, &snapshot.snapshot_id)?
        }
        _ => (0, 0, 0),
    };
    for (offset, len, kind) in [
        (node_offset, snapshot.nodes.len(), "nodes"),
        (group_offset, snapshot.groups.len(), "groups"),
        (partition_offset, snapshot.partitions.len(), "partitions"),
    ] {
        if offset > len {
            return Err(Status::out_of_range(format!(
                "topology {kind} cursor is outside the snapshot"
            )));
        }
    }
    let node_end = (node_offset + page_size).min(snapshot.nodes.len());
    let group_end = (group_offset + page_size).min(snapshot.groups.len());
    let partition_end = (partition_offset + page_size).min(snapshot.partitions.len());
    let has_more = node_end < snapshot.nodes.len()
        || group_end < snapshot.groups.len()
        || partition_end < snapshot.partitions.len();
    let next_cursor = if has_more {
        format!(
            "v1:{}:{node_end}:{group_end}:{partition_end}",
            snapshot.snapshot_id
        )
    } else {
        String::new()
    };
    Ok(ClusterTopology {
        snapshot_id: snapshot.snapshot_id.clone(),
        config_revision: snapshot.config_revision,
        directory_revision: snapshot.directory_revision,
        nodes: snapshot.nodes[node_offset..node_end].to_vec(),
        groups: snapshot.groups[group_offset..group_end].to_vec(),
        partitions: snapshot.partitions[partition_offset..partition_end].to_vec(),
        next_cursor,
    })
}

fn group_replica_status_from_metrics(
    node_id: NodeId,
    group_id: u64,
    metrics: openraft::RaftMetrics<rr_raft::RrTypeConfig>,
    observed_at_unix_ms: u64,
) -> GroupReplicaStatus {
    let leader_replication = metrics
        .replication
        .as_ref()
        .map(|replication| {
            replication
                .iter()
                .map(|(replica_id, matched)| {
                    let matched_index = matched.as_ref().map(|log_id| log_id.index);
                    let last_heartbeat_unix_ms = metrics
                        .heartbeat
                        .as_ref()
                        .and_then(|heartbeat| heartbeat.get(replica_id))
                        .and_then(|heartbeat| heartbeat.as_ref())
                        .map(|heartbeat| {
                            observed_at_unix_ms.saturating_sub(
                                heartbeat.elapsed().as_millis().min(u64::MAX as u128) as u64,
                            )
                        });
                    leader_replica_status(
                        *replica_id,
                        metrics.last_log_index,
                        matched_index,
                        last_heartbeat_unix_ms,
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    GroupReplicaStatus {
        group_id,
        node_id,
        server_state: format!("{:?}", metrics.state),
        leader_node_id: metrics.current_leader,
        current_term: metrics.current_term,
        last_log_index: metrics.last_log_index,
        local_commit_index: metrics.local_committed.as_ref().map(|log_id| log_id.index),
        cluster_commit_index: metrics
            .cluster_committed
            .as_ref()
            .map(|log_id| log_id.index),
        applied_index: metrics.last_applied.as_ref().map(|log_id| log_id.index),
        last_snapshot_index: metrics.snapshot.as_ref().map(|log_id| log_id.index),
        last_snapshot_term: metrics
            .snapshot
            .as_ref()
            .map(|log_id| log_id.leader_id.term),
        leader_replication,
        observed_at_unix_ms,
    }
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

/// Label value for admin operation counters: "ok" when the RPC returned
/// success, "error" otherwise (including validation rejections).
fn admin_result_label(result: &Result<Response<Empty>, Status>) -> &'static str {
    if result.is_ok() {
        "ok"
    } else {
        "error"
    }
}

struct CachedTopology {
    snapshot: ClusterTopology,
    expires_at: Instant,
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
    runtime: Option<Arc<NodeRuntime>>,
    topology_cache: tokio::sync::Mutex<Option<CachedTopology>>,
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
            runtime: None,
            topology_cache: tokio::sync::Mutex::new(None),
            start_time: Instant::now(),
            backup_streams: std::sync::Mutex::new(tokio::task::JoinSet::new()),
            snapshot_stub_warned: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn with_lifecycle_store(mut self, lifecycle_store: Arc<dyn LifecycleStore>) -> Self {
        self.lifecycle_store = Some(lifecycle_store);
        self
    }

    pub fn with_runtime(mut self, runtime: Arc<NodeRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    fn require_runtime(&self) -> Result<&Arc<NodeRuntime>, Status> {
        self.runtime.as_ref().ok_or_else(|| {
            Status::unimplemented("node runtime metadata is not configured for this AdminService")
        })
    }

    async fn build_topology_snapshot(&self) -> Result<ClusterTopology, Status> {
        let mut nodes = Vec::new();
        let mut directory_revision = 0;
        if let Some(directory) = self
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.directory())
        {
            let mut cursor = None;
            loop {
                let page = directory
                    .list(cursor.as_deref(), Some(MAX_TOPOLOGY_PAGE_SIZE))
                    .await
                    .map_err(|error| {
                        tracing::warn!(%error, "node directory list failed (detail sanitized for client)");
                        Status::unavailable("node directory is unavailable")
                    })?;
                if directory_revision == 0 {
                    directory_revision = page.revision;
                } else if directory_revision != page.revision {
                    return Err(Status::aborted(
                        "node directory revision changed while building topology; retry",
                    ));
                }
                let next_cursor = page.next_cursor;
                nodes.extend(page.records.into_iter().map(|record| NodeDirectoryEntry {
                    descriptor: Some(ProtoNodeDescriptor {
                        node_id: record.descriptor.node_id,
                        client_addr: record.descriptor.client_addr,
                        admin_addr: record.descriptor.admin_addr,
                        observability_addr: record.descriptor.observability_addr,
                        advertise_addr: record.descriptor.advertise_addr,
                        environment: record.descriptor.environment,
                        region: record.descriptor.region,
                        zone: record.descriptor.zone,
                        rack: record.descriptor.rack,
                        boot_id: record.descriptor.boot_id,
                    }),
                    revision: record.revision,
                    lease_id: record.lease_id,
                }));
                cursor = next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
        }
        nodes.sort_by_key(|entry| {
            entry
                .descriptor
                .as_ref()
                .map(|descriptor| descriptor.node_id)
                .unwrap_or(0)
        });

        let config_revision = match &self.lifecycle_store {
            Some(store) => store
                .load_linearizable()
                .await
                .map_err(|error| {
                    tracing::warn!(%error, "topology lifecycle load failed (detail sanitized for client)");
                    Status::unavailable("cluster configuration revision is unavailable")
                })?
                .map(|lifecycle| lifecycle.record.topology_revision)
                .unwrap_or(0),
            None => 0,
        };
        let table = self.config_provider.get_routing_table().await.map_err(|error| {
            tracing::warn!(%error, "topology routing table load failed (detail sanitized for client)");
            Status::unavailable("routing table is unavailable")
        })?;
        let groups: BTreeMap<_, _> = table.groups.into_iter().collect();
        let partitions: BTreeMap<_, _> = table.partitions.into_iter().collect();
        let partitions = partitions
            .into_iter()
            .map(|(partition_id, (_config, group_id))| PartitionRoute {
                partition_id,
                group_id,
                replica_node_ids: groups
                    .get(&group_id)
                    .map(|group| group.replicas.clone())
                    .unwrap_or_default(),
            })
            .collect();
        let groups = groups
            .into_values()
            .map(|group| GroupTopology {
                group_id: group.group_id,
                replica_node_ids: group.replicas,
            })
            .collect();
        Ok(ClusterTopology {
            snapshot_id: uuid::Uuid::new_v4().hyphenated().to_string(),
            config_revision,
            directory_revision,
            nodes,
            groups,
            partitions,
            next_cursor: String::new(),
        })
    }

    async fn require_active_group(&self, group_id: u64) -> Result<(), Status> {
        let Some(store) = &self.lifecycle_store else {
            return Ok(());
        };
        let lifecycle = store
            .load_linearizable()
            .await
            .map_err(|error| {
                tracing::warn!(
                    node_id = self.node_id,
                    group_id,
                    %error,
                    "require_active_group: lifecycle store load failed (detail sanitized for client)"
                );
                Status::failed_precondition(format!(
                    "group {group_id} lifecycle state unavailable; retry membership change after store recovery"
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

    async fn trigger_snapshot_inner(
        &self,
        request: Request<GroupId>,
    ) -> Result<Response<Empty>, Status> {
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

    async fn transfer_leader_inner(
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

    async fn add_learner_inner(
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

    async fn change_membership_inner(
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

    async fn remove_group_member_inner(
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
        let result = self.trigger_snapshot_inner(request).await;
        metrics::counter!(
            "rr_admin_snapshot_trigger_total",
            "result" => admin_result_label(&result)
        )
        .increment(1);
        result
    }

    async fn transfer_leader(
        &self,
        request: Request<TransferLeaderRequest>,
    ) -> Result<Response<Empty>, Status> {
        let result = self.transfer_leader_inner(request).await;
        metrics::counter!(
            "rr_admin_transfer_leader_total",
            "result" => admin_result_label(&result)
        )
        .increment(1);
        result
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
        let result = self.add_learner_inner(request).await;
        metrics::counter!(
            "rr_admin_membership_changes_total",
            "op" => "add_learner",
            "result" => admin_result_label(&result)
        )
        .increment(1);
        result
    }

    async fn change_membership(
        &self,
        request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<Empty>, Status> {
        let result = self.change_membership_inner(request).await;
        metrics::counter!(
            "rr_admin_membership_changes_total",
            "op" => "change_membership",
            "result" => admin_result_label(&result)
        )
        .increment(1);
        result
    }

    async fn remove_group_member(
        &self,
        request: Request<RemoveGroupMemberRequest>,
    ) -> Result<Response<Empty>, Status> {
        let result = self.remove_group_member_inner(request).await;
        metrics::counter!(
            "rr_admin_membership_changes_total",
            "op" => "remove_group_member",
            "result" => admin_result_label(&result)
        )
        .increment(1);
        result
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

    /// Return the cluster formation lifecycle status.
    ///
    /// A node without a lifecycle record (no store wired, or the store holds
    /// no record — a legacy cluster that was never adopted) reports the
    /// "Untracked" shape with zeroed fields; that is a normal state, not an
    /// error. A store failure is sanitized: the client gets a fixed
    /// action-oriented message, the detail goes to the logs.
    async fn get_formation_status(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<FormationStatus>, Status> {
        let peer = peer_from_request(&request);
        info!(
            target: AUDIT_TARGET,
            op = "GetFormationStatus",
            peer = %peer,
            node_id = self.node_id,
            "admin rpc"
        );
        let Some(store) = &self.lifecycle_store else {
            return Ok(Response::new(untracked_formation_status()));
        };
        let versioned = store.load_linearizable().await.map_err(|error| {
            tracing::warn!(
                node_id = self.node_id,
                error = %error,
                "get_formation_status: lifecycle store load failed (detail sanitized for client)"
            );
            Status::failed_precondition("formation status unavailable; retry after store recovery")
        })?;
        let Some(versioned) = versioned else {
            return Ok(Response::new(untracked_formation_status()));
        };
        Ok(Response::new(formation_status_from_record(versioned)))
    }

    async fn get_build_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<BuildInfo>, Status> {
        let runtime = self.require_runtime()?;
        Ok(Response::new(build_info_from_snapshot(
            runtime.build(),
            runtime.boot_id(),
            runtime.process_start_unix_ms(),
        )))
    }

    async fn get_node_descriptor(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<NodeDescriptorResponse>, Status> {
        let runtime = self.require_runtime()?;
        let descriptor = runtime
            .descriptor()
            .ok_or_else(|| Status::unimplemented("explicit node descriptor is not configured"))?;
        Ok(Response::new(NodeDescriptorResponse {
            descriptor: Some(ProtoNodeDescriptor {
                node_id: descriptor.node_id,
                client_addr: descriptor.client_addr.clone(),
                admin_addr: descriptor.admin_addr.clone(),
                observability_addr: descriptor.observability_addr.clone(),
                advertise_addr: descriptor.advertise_addr.clone(),
                environment: descriptor.environment.clone(),
                region: descriptor.region.clone(),
                zone: descriptor.zone.clone(),
                rack: descriptor.rack.clone(),
                boot_id: descriptor.boot_id.clone(),
            }),
            directory_revision: runtime.directory_revision(),
        }))
    }

    async fn get_cluster_topology(
        &self,
        request: Request<ClusterTopologyRequest>,
    ) -> Result<Response<ClusterTopology>, Status> {
        let request = request.into_inner();
        let page_size = topology_page_size(request.page_size)?;
        let mut cache = self.topology_cache.lock().await;
        if request.cursor.is_empty() {
            let snapshot = self.build_topology_snapshot().await?;
            *cache = Some(CachedTopology {
                snapshot,
                expires_at: Instant::now() + TOPOLOGY_CURSOR_TTL,
            });
        }
        let cached = cache.as_ref().ok_or_else(|| {
            Status::out_of_range("topology cursor expired; restart pagination without a cursor")
        })?;
        if cached.expires_at <= Instant::now() {
            *cache = None;
            return Err(Status::out_of_range(
                "topology cursor expired; restart pagination without a cursor",
            ));
        }
        Ok(Response::new(paginate_topology(
            &cached.snapshot,
            (!request.cursor.is_empty()).then_some(request.cursor.as_str()),
            page_size,
        )?))
    }

    async fn get_group_replica_status(
        &self,
        request: Request<GroupId>,
    ) -> Result<Response<GroupReplicaStatus>, Status> {
        let group_id = request.into_inner().group_id;
        let raft = self
            .group_manager
            .get_raft(group_id)
            .await
            .ok_or_else(|| Status::not_found(format!("group {group_id} not found")))?;
        let metrics = raft.metrics().borrow_watched().clone();
        Ok(Response::new(group_replica_status_from_metrics(
            self.node_id,
            group_id,
            metrics,
            unix_time_millis(),
        )))
    }

    async fn get_boot_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<BootStatus>, Status> {
        let runtime = self.require_runtime()?;
        Ok(Response::new(BootStatus {
            current: Some(boot_record_to_proto(runtime.journal().current())),
            persistence: runtime.persistence().as_str().to_string(),
        }))
    }

    async fn list_boot_records(
        &self,
        request: Request<BootRecordsRequest>,
    ) -> Result<Response<ProtoBootRecordPage>, Status> {
        let runtime = self.require_runtime()?;
        let request = request.into_inner();
        let page = runtime
            .journal()
            .list(
                (!request.cursor.is_empty()).then_some(request.cursor.as_str()),
                (request.page_size != 0).then_some(request.page_size as usize),
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Response::new(ProtoBootRecordPage {
            records: page.records.into_iter().map(boot_record_to_proto).collect(),
            next_cursor: page.next_cursor.unwrap_or_default(),
            persistence: runtime.persistence().as_str().to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bincode::config::standard;
    use openraft::vote::RaftLeaderId as _;
    use rr_config::mem::MemConfigProvider;
    use rr_config::MemLifecycleStore;
    use rr_core::error::{LifecycleError, RrError};
    use rr_core::service::Service;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;
    use rr_core::{CasOutcome, ClusterLifecycle, GroupFormation};
    use rr_raft::group_manager::StateMachineFactory;
    use rr_raft::service_adapter::ServiceStateMachineAdapter;
    use uuid::Uuid;

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

    fn formation_record(
        origin: ClusterOrigin,
        states: &[(u64, FormationState)],
    ) -> ClusterLifecycle {
        ClusterLifecycle {
            version: 1,
            cluster_uuid: Uuid::from_u128(0x1234),
            topology_revision: 7,
            seed_hash: [9; 32],
            origin,
            groups: states
                .iter()
                .map(|(group_id, state)| {
                    (
                        *group_id,
                        GroupFormation {
                            bootstrap_epoch: group_id + 10,
                            state: *state,
                        },
                    )
                })
                .collect(),
        }
    }

    struct FailingLifecycleStore;

    #[async_trait]
    impl LifecycleStore for FailingLifecycleStore {
        async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError> {
            Err(LifecycleError::Unavailable(
                "etcd endpoint 10.0.0.5:2379 unreachable".to_string(),
            ))
        }

        async fn create_if_absent(
            &self,
            _record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            Err(LifecycleError::Unavailable("injected outage".to_string()))
        }

        async fn compare_and_swap(
            &self,
            _expected_revision: u64,
            _record: ClusterLifecycle,
        ) -> Result<CasOutcome, LifecycleError> {
            Err(LifecycleError::Unavailable("injected outage".to_string()))
        }
    }

    #[tokio::test]
    async fn test_get_formation_status_without_store_reports_untracked() {
        let svc = make_admin_service();
        let resp = svc
            .get_formation_status(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.origin, "Untracked");
        assert_eq!(resp.state, "Untracked");
        assert!(resp.cluster_uuid.is_empty());
        assert_eq!(resp.topology_revision, 0);
        assert_eq!(resp.store_revision, 0);
        assert!(resp.groups.is_empty());
    }

    #[tokio::test]
    async fn test_get_formation_status_with_empty_store_reports_untracked() {
        let store = Arc::new(MemLifecycleStore::new());
        let svc = make_admin_service().with_lifecycle_store(store);
        let resp = svc
            .get_formation_status(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.origin, "Untracked");
        assert_eq!(resp.state, "Untracked");
        assert!(resp.cluster_uuid.is_empty());
        assert_eq!(resp.topology_revision, 0);
        assert_eq!(resp.store_revision, 0);
        assert!(resp.groups.is_empty());
    }

    #[tokio::test]
    async fn test_get_formation_status_maps_populated_record() {
        let store = Arc::new(MemLifecycleStore::new());
        store
            .create_if_absent(formation_record(
                ClusterOrigin::Genesis,
                &[(1, FormationState::Active), (2, FormationState::Forming)],
            ))
            .await
            .unwrap();
        let svc = make_admin_service().with_lifecycle_store(store);

        let resp = svc
            .get_formation_status(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.origin, "Genesis");
        assert_eq!(
            resp.state, "Forming",
            "any forming group must aggregate the cluster state to Forming"
        );
        assert_eq!(
            resp.cluster_uuid,
            Uuid::from_u128(0x1234).hyphenated().to_string()
        );
        assert_eq!(resp.topology_revision, 7);
        assert_eq!(resp.store_revision, 1);
        assert_eq!(resp.groups.len(), 2);
        assert_eq!(resp.groups[0].group_id, 1);
        assert_eq!(resp.groups[0].bootstrap_epoch, 11);
        assert_eq!(resp.groups[0].state, "Active");
        assert_eq!(resp.groups[1].group_id, 2);
        assert_eq!(resp.groups[1].bootstrap_epoch, 12);
        assert_eq!(resp.groups[1].state, "Forming");
    }

    #[tokio::test]
    async fn test_get_formation_status_all_active_reports_active_and_legacy_origin() {
        let store = Arc::new(MemLifecycleStore::new());
        store
            .create_if_absent(formation_record(
                ClusterOrigin::LegacyAdopted,
                &[(1, FormationState::Active), (2, FormationState::Active)],
            ))
            .await
            .unwrap();
        let svc = make_admin_service().with_lifecycle_store(store);

        let resp = svc
            .get_formation_status(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.origin, "LegacyAdopted");
        assert_eq!(resp.state, "Active");
        assert!(
            resp.groups.iter().all(|group| group.state == "Active"),
            "every group must map to Active: {:?}",
            resp.groups
        );
    }

    #[tokio::test]
    async fn test_get_formation_status_store_error_is_sanitized() {
        let svc = make_admin_service().with_lifecycle_store(Arc::new(FailingLifecycleStore));

        let status = svc
            .get_formation_status(Request::new(Empty {}))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            status.message(),
            "formation status unavailable; retry after store recovery"
        );
        assert!(
            !status.message().contains("10.0.0.5"),
            "internal store detail must not leak to the client: {}",
            status.message()
        );
    }

    #[tokio::test]
    async fn test_admin_operation_counters_record_ok_and_error() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);

        // Error paths on a node hosting no groups.
        let svc = make_admin_service();
        let _ = svc
            .trigger_snapshot(Request::new(GroupId { group_id: 99 }))
            .await;
        let _ = svc
            .transfer_leader(Request::new(TransferLeaderRequest {
                group_id: 99,
                target_node_id: 1,
            }))
            .await;
        let _ = svc
            .add_learner(Request::new(AddLearnerRequest {
                group_id: 99,
                node_id: 2,
                addr: "127.0.0.1:5001".to_string(),
            }))
            .await;
        let _ = svc
            .change_membership(Request::new(ChangeMembershipRequest {
                group_id: 99,
                voter_node_ids: vec![1, 2, 3],
                force: false,
            }))
            .await;
        let _ = svc
            .remove_group_member(Request::new(RemoveGroupMemberRequest {
                group_id: 99,
                node_id: 2,
                force: false,
            }))
            .await;

        // Success paths on a single-node group (self is leader).
        let svc = make_admin_service_with_group().await;
        svc.trigger_snapshot(Request::new(GroupId { group_id: 1 }))
            .await
            .unwrap();
        svc.change_membership(Request::new(ChangeMembershipRequest {
            group_id: 1,
            voter_node_ids: vec![1],
            force: true,
        }))
        .await
        .unwrap();
        // Targeting the local node is the supported path: it triggers a
        // fresh election, which a live RaftCore always accepts.
        svc.transfer_leader(Request::new(TransferLeaderRequest {
            group_id: 1,
            target_node_id: 1,
        }))
        .await
        .unwrap();
        // Re-adding the local node (already a voter) is an openraft no-op
        // (`AddNodes` on an existing voter), and the manager skips the
        // line-rate wait for self — a deterministic Ok without a live peer.
        svc.add_learner(Request::new(AddLearnerRequest {
            group_id: 1,
            node_id: 1,
            addr: "127.0.0.1:9090".to_string(),
        }))
        .await
        .unwrap();
        // Seed a dead learner directly (the blocking admin add path would
        // wait forever for it to become line-rate), then remove it: removing
        // a learner bypasses the voter guards and commits a membership write.
        svc.group_manager
            .add_learner_nonblocking(1, 2, "node-2:9001".to_string())
            .await
            .unwrap();
        svc.remove_group_member(Request::new(RemoveGroupMemberRequest {
            group_id: 1,
            node_id: 2,
            force: false,
        }))
        .await
        .unwrap();

        let rendered = handle.render();
        assert!(
            rendered.contains("rr_admin_snapshot_trigger_total{result=\"error\"} 1"),
            "{rendered}"
        );
        assert!(
            rendered.contains("rr_admin_snapshot_trigger_total{result=\"ok\"} 1"),
            "{rendered}"
        );
        assert!(
            rendered.contains("rr_admin_transfer_leader_total{result=\"error\"} 1"),
            "{rendered}"
        );
        assert!(
            rendered.contains("rr_admin_transfer_leader_total{result=\"ok\"} 1"),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "rr_admin_membership_changes_total{op=\"change_membership\",result=\"error\"} 1"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "rr_admin_membership_changes_total{op=\"change_membership\",result=\"ok\"} 1"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "rr_admin_membership_changes_total{op=\"add_learner\",result=\"error\"} 1"
            ),
            "{rendered}"
        );
        assert!(
            rendered
                .contains("rr_admin_membership_changes_total{op=\"add_learner\",result=\"ok\"} 1"),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "rr_admin_membership_changes_total{op=\"remove_group_member\",result=\"error\"} 1"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "rr_admin_membership_changes_total{op=\"remove_group_member\",result=\"ok\"} 1"
            ),
            "{rendered}"
        );
    }

    fn test_runtime() -> Arc<NodeRuntime> {
        Arc::new(
            NodeRuntime::new(crate::runtime::NodeRuntimeConfig::new(
                1,
                "test-fingerprint".into(),
            ))
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn build_and_boot_rpcs_share_one_boot_identity() {
        let svc = make_admin_service().with_runtime(test_runtime());
        let build = svc
            .get_build_info(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        let boot = svc
            .get_boot_status(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(boot.persistence, "VOLATILE");
        assert_eq!(boot.current.unwrap().boot_id, build.boot_id);
        assert_eq!(build.config_fingerprint, "test-fingerprint");
        assert_eq!(build.admin_api.unwrap().major, 1);
        assert_eq!(build.raft_wire.unwrap().min_peer_minor, 0);

        let history = svc
            .list_boot_records(Request::new(BootRecordsRequest {
                cursor: String::new(),
                page_size: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(history.records.len(), 1);
        assert_eq!(history.records[0].boot_id, build.boot_id);
    }

    #[test]
    fn replica_mapping_preserves_no_leader_and_distinct_commit_frontiers() {
        let mut metrics = openraft::RaftMetrics::<rr_raft::RrTypeConfig>::new_initial(2);
        metrics.current_leader = None;
        metrics.last_log_index = Some(9);
        metrics.local_committed = Some(openraft::LogId::new(
            openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 2),
            5,
        ));
        metrics.cluster_committed = Some(openraft::LogId::new(
            openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 2),
            7,
        ));
        metrics.last_applied = Some(openraft::LogId::new(
            openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 2),
            4,
        ));
        let status = group_replica_status_from_metrics(2, 10, metrics, 123);
        assert_eq!(status.leader_node_id, None);
        assert_eq!(status.local_commit_index, Some(5));
        assert_eq!(status.cluster_commit_index, Some(7));
        assert_eq!(status.applied_index, Some(4));
        assert!(status.leader_replication.is_empty());
    }

    #[test]
    fn follower_has_no_leader_replication_view_and_missing_match_has_no_lag() {
        let mut follower = openraft::RaftMetrics::<rr_raft::RrTypeConfig>::new_initial(2);
        follower.current_leader = Some(1);
        follower.replication = None;
        assert!(group_replica_status_from_metrics(2, 10, follower, 123)
            .leader_replication
            .is_empty());

        let status = leader_replica_status(3, Some(9), None, None);
        assert_eq!(status.matched_index, None);
        assert_eq!(status.lag_entries, None);
        assert_eq!(status.last_heartbeat_unix_ms, None);
    }

    fn topology_fixture() -> ClusterTopology {
        ClusterTopology {
            snapshot_id: "snapshot-1".into(),
            config_revision: 7,
            directory_revision: 11,
            nodes: (1..=3)
                .map(|node_id| NodeDirectoryEntry {
                    descriptor: Some(ProtoNodeDescriptor {
                        node_id,
                        ..Default::default()
                    }),
                    revision: node_id,
                    lease_id: node_id as i64,
                })
                .collect(),
            groups: (1..=2)
                .map(|group_id| GroupTopology {
                    group_id,
                    replica_node_ids: vec![1, 2, 3],
                })
                .collect(),
            partitions: vec![PartitionRoute {
                partition_id: 1,
                group_id: 1,
                replica_node_ids: vec![1, 2, 3],
            }],
            next_cursor: String::new(),
        }
    }

    #[test]
    fn topology_pages_are_stable_and_reject_expired_snapshot_cursors() {
        let snapshot = topology_fixture();
        let first = paginate_topology(&snapshot, None, 1).unwrap();
        assert_eq!(first.nodes.len(), 1);
        assert_eq!(first.groups.len(), 1);
        assert_eq!(first.partitions.len(), 1);
        let second = paginate_topology(&snapshot, Some(&first.next_cursor), 1).unwrap();
        assert_eq!(second.nodes[0].descriptor.as_ref().unwrap().node_id, 2);
        let third = paginate_topology(&snapshot, Some(&second.next_cursor), 1).unwrap();
        assert!(third.next_cursor.is_empty());
        assert_eq!(third.nodes.len(), 1);
        assert!(third.groups.is_empty());
        assert!(third.partitions.is_empty());

        let expired = decode_topology_cursor(&first.next_cursor, "another-snapshot").unwrap_err();
        assert_eq!(expired.code(), tonic::Code::OutOfRange);
    }

    #[tokio::test]
    async fn new_metadata_rpcs_work_over_real_grpc_transport() {
        use rr_network::proto::admin::admin_service_client::AdminServiceClient;
        use rr_network::proto::admin::admin_service_server::AdminServiceServer;

        let temp = tempfile::tempdir().unwrap();
        let runtime = Arc::new(
            NodeRuntime::new(crate::runtime::NodeRuntimeConfig {
                node_id: 1,
                app_version: "wire-app".into(),
                config_fingerprint: "wire-fingerprint".into(),
                descriptor: Some(crate::runtime::NodeDescriptorConfig {
                    client_addr: "node-1:5000".into(),
                    admin_addr: "node-1:5001".into(),
                    observability_addr: "node-1:5002".into(),
                    advertise_addr: "node-1:5000".into(),
                    environment: "test".into(),
                    region: None,
                    zone: None,
                    rack: None,
                }),
                journal_dir: Some(temp.path().to_path_buf()),
                directory: None,
            })
            .unwrap(),
        );
        let svc = make_admin_service().with_runtime(runtime);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async {
            let item = listener.accept().await.map(|(stream, _)| stream);
            Some((item, listener))
        });
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(AdminServiceServer::new(svc))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });
        let mut client = AdminServiceClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        let build = client
            .get_build_info(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(build.app_version, "wire-app");
        assert_eq!(build.config_fingerprint, "wire-fingerprint");
        let descriptor = client
            .get_node_descriptor(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(descriptor.directory_revision, 0);
        assert_eq!(descriptor.descriptor.unwrap().boot_id, build.boot_id);
        let boot = client
            .get_boot_status(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(boot.persistence, "PERSISTENT");
        assert_eq!(boot.current.unwrap().boot_id, build.boot_id);
        let boots = client
            .list_boot_records(Request::new(BootRecordsRequest {
                cursor: String::new(),
                page_size: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(boots.records.len(), 1);
        let topology = client
            .get_cluster_topology(Request::new(ClusterTopologyRequest {
                cursor: String::new(),
                page_size: 50,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!topology.snapshot_id.is_empty());
        drop(client);
        let _ = shutdown_tx.send(());
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn replica_rpc_unknown_group_is_not_found() {
        let svc = make_admin_service();
        let error = svc
            .get_group_replica_status(Request::new(GroupId { group_id: 99 }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);
    }
}
