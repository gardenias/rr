//! Manages the lifecycle of multiple Raft groups on a single node.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;
use std::sync::Arc;

use metrics::{counter, gauge, histogram};
use openraft::BasicNode;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};

use rr_core::error::{RaftError, RrError};
use rr_core::state_machine::StateMachine;
use rr_core::types::{GroupId, NodeId};

use crate::network_factory::{ChannelPoolLike, GrpcNetworkFactory};
use crate::raft_node::{NoopNetworkFactory, RaftNode};
use crate::type_config::RrTypeConfig;
/// A factory function type for creating state machine instances per group.
pub type StateMachineFactory = Box<dyn Fn(GroupId) -> Box<dyn StateMachine> + Send + Sync>;

/// Manages multiple Raft groups on a single node.
///
/// Responsible for creating, starting, and stopping Raft group instances.
/// Thread-safe — all state is behind `Arc<RwLock<...>>`.
pub struct RaftGroupManager {
    node_id: NodeId,
    groups: Arc<RwLock<HashMap<GroupId, RaftNode>>>,
    sm_factory: Arc<StateMachineFactory>,
    /// When `Some`, new groups are created with `GrpcNetworkFactory` for real
    /// multi-node networking.  When `None`, `NoopNetworkFactory` is used
    /// (single-node / test mode).
    channel_pool: Option<Arc<dyn ChannelPoolLike>>,
    /// Per-group ingestor supervisor handles. Populated by
    /// [`Self::attach_ingestor`]; stop_group + drop both clean these up.
    ingestor_handles: Arc<RwLock<HashMap<GroupId, crate::ingestor_runner::IngestorHandle>>>,
    /// Per-group metrics tickers (replication_lag etc). Populated by
    /// [`Self::start_group`] / [`Self::start_group_rocks`]; stop_group
    /// cancels and joins them.
    metrics_tickers:
        Arc<RwLock<HashMap<GroupId, crate::metrics_ticker::MetricsTickerHandle>>>,
    /// When `Some`, `start_group_rocks` will use `RocksLogStorage` at `{base_path}/{group_id}`.
    #[cfg(feature = "rocks")]
    rocks_base_path: Option<String>,
}

impl RaftGroupManager {
    /// Create a new `RaftGroupManager` in single-node / test mode.
    ///
    /// Groups started via `start_group` will use `NoopNetworkFactory`.
    pub fn new(node_id: NodeId, sm_factory: StateMachineFactory) -> Self {
        info!(node_id, "creating RaftGroupManager (single-node mode)");
        Self {
            node_id,
            groups: Arc::new(RwLock::new(HashMap::new())),
            sm_factory: Arc::new(sm_factory),
            channel_pool: None,
            ingestor_handles: Arc::new(RwLock::new(HashMap::new())),
            metrics_tickers: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "rocks")]
            rocks_base_path: None,
        }
    }

    /// Create a `RaftGroupManager` backed by real gRPC networking.
    ///
    /// Groups started via `start_group` will use `GrpcNetworkFactory` with the
    /// supplied `ChannelPool` for connection reuse.
    pub fn new_with_network(
        node_id: NodeId,
        sm_factory: StateMachineFactory,
        channel_pool: Arc<dyn ChannelPoolLike>,
    ) -> Self {
        info!(node_id, "creating RaftGroupManager (multi-node mode)");
        Self {
            node_id,
            groups: Arc::new(RwLock::new(HashMap::new())),
            sm_factory: Arc::new(sm_factory),
            channel_pool: Some(channel_pool),
            ingestor_handles: Arc::new(RwLock::new(HashMap::new())),
            metrics_tickers: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "rocks")]
            rocks_base_path: None,
        }
    }

    /// Start a new Raft group on this node.
    ///
    /// Creates a `RaftNode` and stores it. Returns error if group already exists.
    /// When a `ChannelPool` was supplied at construction time, the group will use
    /// `GrpcNetworkFactory`; otherwise `NoopNetworkFactory` is used.
    #[instrument(skip(self))]
    pub async fn start_group(&self, group_id: GroupId) -> Result<(), RrError> {
        let mut guard = self.groups.write().await;

        if guard.contains_key(&group_id) {
            return Err(RrError::Application(format!(
                "group {group_id} already exists"
            )));
        }

        let sm = (self.sm_factory)(group_id);

        let node = if let Some(pool) = &self.channel_pool {
            let factory = GrpcNetworkFactory::new(pool.clone(), group_id);
            RaftNode::new_with_factory(group_id, self.node_id, sm, factory)
                .await
                .map_err(|e| RrError::Application(e.to_string()))?
        } else {
            RaftNode::new_with_factory(group_id, self.node_id, sm, NoopNetworkFactory)
                .await
                .map_err(|e| RrError::Application(e.to_string()))?
        };

        debug!(group_id, node_id = self.node_id, "started raft group");
        let node_id = self.node_id;
        counter!("raft.group.start", "node_id" => node_id.to_string()).increment(1);
        guard.insert(group_id, node);
        Ok(())
    }

    /// Bootstrap a multi-node Raft cluster for a group.
    ///
    /// Calls `openraft::Raft::initialize(members)` on the group identified by
    /// `group_id`.  This must be called exactly once across the whole cluster
    /// to elect an initial leader.
    ///
    /// `members` maps each node's `NodeId` to its `BasicNode` (containing the
    /// gRPC address).  The group must already be started via `start_group`.
    #[instrument(skip(self, members))]
    pub async fn init_cluster(
        &self,
        group_id: GroupId,
        members: BTreeMap<u64, BasicNode>,
    ) -> Result<(), RrError> {
        let raft = self
            .get_raft(group_id)
            .await
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        raft.initialize(members)
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        info!(group_id, node_id = self.node_id, "initialized raft cluster");
        Ok(())
    }

    /// Add a learner node to a Raft group.
    ///
    /// Calls `openraft::Raft::add_learner`. The learner replicates logs but
    /// does not vote. Call `change_membership` afterwards to promote it to voter.
    ///
    /// `addr` is the gRPC address of the new node (e.g. `"10.0.0.2:50051"`).
    #[instrument(skip(self))]
    pub async fn add_learner(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        addr: String,
    ) -> Result<(), RrError> {
        let raft = self
            .get_raft(group_id)
            .await
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        raft.add_learner(node_id, BasicNode { addr }, true)
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        info!(
            group_id,
            node_id, self.node_id, "added learner to raft group"
        );
        Ok(())
    }

    /// Change the voting membership of a Raft group.
    ///
    /// Calls `openraft::Raft::change_membership` with `retain = true` so that
    /// nodes removed from `members` are kept as learners rather than removed
    /// entirely.
    #[instrument(skip(self))]
    pub async fn change_membership(
        &self,
        group_id: GroupId,
        members: BTreeSet<NodeId>,
    ) -> Result<(), RrError> {
        let raft = self
            .get_raft(group_id)
            .await
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        raft.change_membership(members, true)
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        info!(
            group_id,
            node_id = self.node_id,
            "changed membership of raft group"
        );
        Ok(())
    }

    /// Stop and remove a Raft group from this node.
    ///
    /// Calls `raft.shutdown()` and removes from the map. Any attached
    /// [`crate::ingestor_runner`] supervisor and [`crate::metrics_ticker`]
    /// are stopped first so background tasks cannot observe a torn-down
    /// raft instance.
    #[instrument(skip(self))]
    pub async fn stop_group(&self, group_id: GroupId) -> Result<(), RrError> {
        if let Some(handle) = self.ingestor_handles.write().await.remove(&group_id) {
            handle.stop().await;
        }
        if let Some(ticker) = self.metrics_tickers.write().await.remove(&group_id) {
            ticker.stop().await;
        }

        let mut guard = self.groups.write().await;

        let node = guard
            .remove(&group_id)
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        node.raft
            .shutdown()
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        info!(group_id, node_id = self.node_id, "stopped raft group");
        let node_id = self.node_id;
        counter!("raft.group.stop", "node_id" => node_id.to_string()).increment(1);
        Ok(())
    }

    /// Fetch the most recent snapshot bytes for `group_id`, if any.
    ///
    /// Returned bytes are whatever the state machine emitted from
    /// `StateMachine::snapshot()` — for `ServiceStateMachineAdapter`
    /// that is a [`rr_core::framed_snapshot::FramedSnapshot`] envelope.
    /// `Ok(None)` means no snapshot has been taken yet on this group.
    ///
    /// Used by the ingestor lifecycle hook to seed `ingestor.seek()`
    /// on leader promotion without exposing adapter internals.
    pub async fn fetch_snapshot_bytes(
        &self,
        group_id: GroupId,
    ) -> Result<Option<Vec<u8>>, RrError> {
        let raft = self
            .get_raft(group_id)
            .await
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        let snap = raft
            .get_snapshot()
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;
        Ok(snap.map(|s| s.snapshot.into_inner()))
    }

    /// Attach a [`rr_core::ingestor::DynCommandIngestor`] to `group_id`
    /// without auto-idempotency. Re-fetched messages on leader
    /// transitions or step-down recovery will reach the user
    /// `Service::handle()` MORE THAN ONCE — only use this when your
    /// service is itself idempotent or driven from a strictly
    /// non-replayed source.
    ///
    /// Most users wanting Kafka semantics should call
    /// [`Self::attach_ingestor_with_auto_idempotency`] instead.
    ///
    /// Spawns an [`crate::ingestor_runner`] supervisor that runs the
    /// ingestor's poll loop only while this node is the leader of
    /// `group_id`. The handle is owned by the manager and torn down by
    /// [`Self::stop_group`] / [`Self::stop_ingestor`].
    ///
    /// Returns an error if the group has not been started, or if an
    /// ingestor is already attached. To replace an attached ingestor,
    /// call [`Self::stop_ingestor`] first.
    pub async fn attach_ingestor(
        self: &Arc<Self>,
        group_id: GroupId,
        ingestor: Box<dyn rr_core::ingestor::DynCommandIngestor>,
    ) -> Result<(), RrError> {
        self.attach_ingestor_inner(group_id, ingestor, None).await
    }

    /// Attach an ingestor with framework-managed deduplication.
    ///
    /// The runner derives an [`rr_core::idempotency::IdempotencyKey`]
    /// from each command's offset using `auto_idem.offset_to_request_id`
    /// and tags `auto_idem.request_type` as the watermark namespace.
    /// The framework's idempotency layer then dedups any re-fetched
    /// commands (after step-down, snapshot-driven seek lag, or
    /// poll-retry-after-error) so `Service::handle()` runs at most
    /// once per logical Kafka offset — completing the
    /// "Kafka at-least-once + framework dedup = exactly-once apply"
    /// contract from ADR A-0001.
    ///
    /// `request_type` MUST be unique per ingestor stream (recommended:
    /// the topic name). `offset_to_request_id` MUST be monotone in
    /// offset order — see [`crate::ingestor_runner::OffsetToRequestId`]
    /// for why hash-based mappings break the watermark.
    pub async fn attach_ingestor_with_auto_idempotency(
        self: &Arc<Self>,
        group_id: GroupId,
        ingestor: Box<dyn rr_core::ingestor::DynCommandIngestor>,
        auto_idem: crate::ingestor_runner::AutoIdempotency,
    ) -> Result<(), RrError> {
        self.attach_ingestor_inner(group_id, ingestor, Some(auto_idem))
            .await
    }

    async fn attach_ingestor_inner(
        self: &Arc<Self>,
        group_id: GroupId,
        ingestor: Box<dyn rr_core::ingestor::DynCommandIngestor>,
        auto_idem: Option<crate::ingestor_runner::AutoIdempotency>,
    ) -> Result<(), RrError> {
        if self.get_raft(group_id).await.is_none() {
            return Err(RrError::Raft(RaftError::GroupNotFound { group_id }));
        }
        let mut guard = self.ingestor_handles.write().await;
        if guard.contains_key(&group_id) {
            return Err(RrError::Application(format!(
                "ingestor already attached to group {group_id}"
            )));
        }
        let handle = crate::ingestor_runner::spawn(self.clone(), group_id, ingestor, auto_idem);
        guard.insert(group_id, handle);
        info!(group_id, "attached ingestor supervisor");
        Ok(())
    }

    /// Spawn the per-group [`crate::metrics_ticker`] supervisor that
    /// periodically emits `rr_raft_replication_lag` and the other
    /// gauge metrics that need an active leader's view (replication
    /// state, term, log index).
    ///
    /// Idempotent on the same group: a second call returns `Ok(())`
    /// without spawning a duplicate ticker. Production wiring through
    /// `RrNode::start()` calls this automatically after each
    /// `start_group`; standalone tests usually skip it.
    pub async fn start_metrics_ticker(
        self: &Arc<Self>,
        group_id: GroupId,
    ) -> Result<(), RrError> {
        if self.get_raft(group_id).await.is_none() {
            return Err(RrError::Raft(RaftError::GroupNotFound { group_id }));
        }
        let mut guard = self.metrics_tickers.write().await;
        if guard.contains_key(&group_id) {
            return Ok(());
        }
        let handle = crate::metrics_ticker::spawn_for_group(self.clone(), group_id);
        guard.insert(group_id, handle);
        debug!(group_id, "started metrics ticker");
        Ok(())
    }

    /// Stop the ingestor supervisor for `group_id` if one is attached.
    /// Returns `Ok(false)` if no supervisor was running. The Raft group
    /// itself remains active.
    pub async fn stop_ingestor(&self, group_id: GroupId) -> Result<bool, RrError> {
        let handle = self.ingestor_handles.write().await.remove(&group_id);
        match handle {
            Some(h) => {
                h.stop().await;
                info!(group_id, "stopped ingestor supervisor");
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Propose a client write to the Raft group, recording proposal duration
    /// and updating Raft gauge metrics on success.
    #[instrument(skip(self, payload))]
    pub async fn write(&self, group_id: GroupId, payload: Vec<u8>) -> Result<Vec<u8>, RrError> {
        let raft = self
            .get_raft(group_id)
            .await
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        let start = Instant::now();
        let result = raft
            .client_write(payload)
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        let gid = group_id.to_string();
        histogram!("rr_proposal_duration_seconds", "group_id" => gid.clone())
            .record(start.elapsed().as_secs_f64());

        // Update raft gauge metrics from the current raft state.
        let m = raft.metrics().borrow().clone();
        let is_leader = if matches!(m.state, openraft::ServerState::Leader) {
            1.0_f64
        } else {
            0.0_f64
        };
        gauge!("rr_raft_is_leader", "group_id" => gid.clone()).set(is_leader);
        gauge!("rr_raft_current_term", "group_id" => gid.clone()).set(m.current_term as f64);
        gauge!("rr_raft_last_log_index", "group_id" => gid)
            .set(m.last_log_index.unwrap_or(0) as f64);

        Ok(result.data)
    }

    /// Get a reference to a Raft group's `openraft::Raft` instance.
    ///
    /// Returns `None` if the group does not exist.
    #[instrument(skip(self))]
    pub async fn get_raft(&self, group_id: GroupId) -> Option<openraft::Raft<RrTypeConfig>> {
        let guard = self.groups.read().await;
        guard.get(&group_id).map(|node| node.raft.clone())
    }

    #[instrument(skip(self))]
    /// Returns the list of active group IDs.
    pub async fn group_ids(&self) -> Vec<GroupId> {
        let guard = self.groups.read().await;
        guard.keys().copied().collect()
    }

    #[instrument(skip(self))]
    /// Returns the number of active groups.
    pub async fn group_count(&self) -> usize {
        let guard = self.groups.read().await;
        guard.len()
    }
    /// Create a `RaftGroupManager` that uses RocksDB-backed log storage.
    ///
    /// Groups started via `start_group_rocks` will persist their Raft log to
    /// `{base_path}/{group_id}/` on disk.
    #[cfg(feature = "rocks")]
    pub fn new_with_rocks(
        node_id: NodeId,
        sm_factory: StateMachineFactory,
        base_path: String,
    ) -> Self {
        info!(node_id, %base_path, "creating RaftGroupManager (rocks mode)");
        Self {
            node_id,
            groups: Arc::new(RwLock::new(HashMap::new())),
            sm_factory: Arc::new(sm_factory),
            channel_pool: None,
            ingestor_handles: Arc::new(RwLock::new(HashMap::new())),
            metrics_tickers: Arc::new(RwLock::new(HashMap::new())),
            rocks_base_path: Some(base_path),
        }
    }

    /// Start a Raft group backed by RocksDB log storage.
    ///
    /// The database is opened at `{rocks_base_path}/{group_id}`.
    #[cfg(feature = "rocks")]
    #[instrument(skip(self))]
    pub async fn start_group_rocks(&self, group_id: GroupId) -> Result<(), RrError> {
        let base = self
            .rocks_base_path
            .as_ref()
            .ok_or_else(|| RrError::Application("rocks_base_path not set".to_string()))?;
        let path = format!("{base}/{group_id}");

        let mut guard = self.groups.write().await;
        if guard.contains_key(&group_id) {
            return Err(RrError::Application(format!(
                "group {group_id} already exists"
            )));
        }
        let sm = (self.sm_factory)(group_id);
        let node =
            RaftNode::new_with_rocks_factory(group_id, self.node_id, sm, NoopNetworkFactory, &path)
                .await
                .map_err(|e| RrError::Application(e.to_string()))?;

        debug!(
            group_id,
            node_id = self.node_id,
            "started raft group (rocks)"
        );
        let node_id = self.node_id;
        counter!("raft.group.start", "node_id" => node_id.to_string()).increment(1);
        guard.insert(group_id, node);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network_factory::ChannelPool;
    use async_trait::async_trait;
    use rr_core::error::RrError;
    use rr_core::types::LogIndex;

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

    #[tokio::test]
    async fn test_group_manager_start_stop() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        mgr.start_group(1).await.unwrap();
        assert_eq!(mgr.group_count().await, 1);

        mgr.stop_group(1).await.unwrap();
        assert_eq!(mgr.group_count().await, 0);
    }

    #[tokio::test]
    async fn test_group_manager_start_duplicate() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        mgr.start_group(1).await.unwrap();
        let err = mgr.start_group(1).await.unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "expected 'already exists' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_group_manager_stop_nonexistent() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        let err = mgr.stop_group(99).await.unwrap_err();
        assert!(
            err.to_string().contains("group not found"),
            "expected 'group not found' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_group_manager_multiple_groups() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        mgr.start_group(1).await.unwrap();
        mgr.start_group(2).await.unwrap();
        mgr.start_group(3).await.unwrap();

        let mut ids = mgr.group_ids().await;
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);

        mgr.stop_group(2).await.unwrap();

        let mut ids = mgr.group_ids().await;
        ids.sort();
        assert_eq!(ids, vec![1, 3]);
        assert_eq!(mgr.group_count().await, 2);
    }

    #[tokio::test]
    async fn test_group_manager_get_raft() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        // Non-existent group returns None
        assert!(mgr.get_raft(1).await.is_none());

        mgr.start_group(1).await.unwrap();

        // Existing group returns Some
        assert!(mgr.get_raft(1).await.is_some());

        mgr.stop_group(1).await.unwrap();

        // Stopped group returns None
        assert!(mgr.get_raft(1).await.is_none());
    }

    #[tokio::test]
    async fn test_group_manager_with_channel_pool() {
        // Create manager with a real ChannelPool — groups should start successfully
        // even without any actual connections (we just verify construction + start).
        let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
        let mgr = RaftGroupManager::new_with_network(1, noop_factory(), pool);

        // Starting a group with GrpcNetworkFactory should succeed
        mgr.start_group(10).await.unwrap();
        assert_eq!(mgr.group_count().await, 1);
        assert!(mgr.get_raft(10).await.is_some());

        mgr.stop_group(10).await.unwrap();
        assert_eq!(mgr.group_count().await, 0);
    }

    #[tokio::test]
    async fn test_init_cluster_unknown_group_returns_error() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            BasicNode {
                addr: "127.0.0.1:5000".to_string(),
            },
        );

        let err = mgr.init_cluster(99, members).await.unwrap_err();
        assert!(
            err.to_string().contains("group not found"),
            "expected 'group not found' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_init_cluster_single_node() {
        // Single-node cluster: start group, init with self as sole member,
        // wait for leader election.
        let mgr = RaftGroupManager::new(1, noop_factory());
        mgr.start_group(1).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            BasicNode {
                addr: String::new(),
            },
        );

        // init_cluster must succeed
        mgr.init_cluster(1, members).await.unwrap();

        // Wait for leader election
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // The raft instance should have a leader now
        let raft = mgr.get_raft(1).await.unwrap();
        let metrics = raft.metrics().borrow().clone();
        assert!(
            metrics.current_leader.is_some(),
            "expected a leader after init_cluster, got: {:?}",
            metrics.current_leader
        );
    }
    #[tokio::test]
    async fn test_add_learner_unknown_group() {
        let mgr = RaftGroupManager::new(1, noop_factory());
        let err = mgr
            .add_learner(99, 2, "127.0.0.1:5001".to_string())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("group not found"),
            "expected 'group not found' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_change_membership_unknown_group() {
        let mgr = RaftGroupManager::new(1, noop_factory());
        let mut members = BTreeSet::new();
        members.insert(1u64);
        let err = mgr.change_membership(99, members).await.unwrap_err();
        assert!(
            err.to_string().contains("group not found"),
            "expected 'group not found' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_change_membership_single_node() {
        // Start group, init as single-node cluster, wait for leader,
        // then call change_membership({1}) — a no-op that should succeed.
        let mgr = RaftGroupManager::new(1, noop_factory());
        mgr.start_group(1).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            BasicNode {
                addr: String::new(),
            },
        );
        mgr.init_cluster(1, members).await.unwrap();

        // Wait for leader election
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
        let raft = mgr.get_raft(1).await.unwrap();
        assert!(
            raft.metrics().borrow().current_leader.is_some(),
            "expected a leader before change_membership"
        );

        // Change membership to {1} (no-op — already sole voter)
        let mut new_members = BTreeSet::new();
        new_members.insert(1u64);
        mgr.change_membership(1, new_members).await.unwrap();
    }

    #[tokio::test]
    async fn test_write_records_proposal_metrics() {
        // Start group, init as single-node cluster, wait for leader,
        // then call write() — metrics macros are no-ops without a recorder
        // so we just verify the code path doesn't panic.
        let mgr = RaftGroupManager::new(1, noop_factory());
        mgr.start_group(1).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            BasicNode {
                addr: String::new(),
            },
        );
        mgr.init_cluster(1, members).await.unwrap();

        // Wait for leader election
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // Write should succeed and record metrics without panic
        let result = mgr.write(1, vec![1, 2, 3]).await;
        assert!(result.is_ok(), "write failed: {:?}", result.err());
    }

}
