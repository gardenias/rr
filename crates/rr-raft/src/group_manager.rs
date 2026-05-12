//! Manages the lifecycle of multiple Raft groups on a single node.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bincode::config::standard;
use dashmap::DashMap;
use metrics::{counter, gauge, histogram};
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::BasicNode;
use openraft::ReadPolicy;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tracing::{debug, info, instrument};

use rr_core::error::{RaftError, RrError};
use rr_core::state_machine::{
    IngestorOffsetControl, OutboxControl, StateMachine, StateMachineLifecycleHook,
    StateMachineReader,
};
use rr_core::types::{GroupId, NodeId};

use crate::network_factory::{ChannelPoolLike, GrpcNetworkFactory, TlsPaths};
use crate::raft_node::{NoopNetworkFactory, RaftNode};
use crate::tcp_transport::{TcpConnectionPool, TcpRaftNetworkFactory};
use crate::type_config::RrTypeConfig;
/// A factory function type for creating state machine instances per group.
pub type StateMachineFactory = Box<dyn Fn(GroupId) -> Box<dyn StateMachine> + Send + Sync>;

#[derive(Clone)]
struct TcpNetworkConfig {
    tcp_pool: Arc<TcpConnectionPool>,
    grpc_channel_pool: Arc<dyn ChannelPoolLike>,
    port_offset: u16,
    tls: Option<TlsPaths>,
}

/// All mutable state for a single Raft group, protected by a single Mutex.
struct GroupState {
    node: RaftNode,
    ingestor_handle: Option<crate::ingestor_runner::IngestorHandle>,
    output_handle: Option<crate::output_runner::OutputHandle>,
    metrics_ticker_handle: Option<crate::metrics_ticker::MetricsTickerHandle>,
    ingestor_offset_control: Option<Arc<dyn IngestorOffsetControl>>,
    read_handle: Option<Arc<dyn StateMachineReader>>,
    outbox_control: Option<Arc<dyn OutboxControl>>,
    learner_promoter_handle: Option<crate::learner_promoter::LearnerPromoterHandle>,
    lifecycle_hook: Option<Arc<dyn StateMachineLifecycleHook>>,
    /// Tracks in-flight client writes so `stop_group()` can drain them
    /// before shutting down Raft.
    in_flight_writes: Arc<AtomicUsize>,
}

pub struct InFlightWriteGuard {
    counter: Arc<AtomicUsize>,
}

impl InFlightWriteGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for InFlightWriteGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Manages multiple Raft groups on a single node.
///
/// Responsible for creating, starting, and stopping Raft group instances.
/// Thread-safe — all per-group mutable state is behind `Arc<Mutex<GroupState>>`.
pub struct RaftGroupManager {
    node_id: NodeId,
    groups: Arc<DashMap<GroupId, Arc<Mutex<GroupState>>>>,
    sm_factory: Arc<StateMachineFactory>,
    /// When `Some`, new groups are created with `GrpcNetworkFactory` for real
    /// multi-node networking.  When `None`, `NoopNetworkFactory` is used
    /// (single-node / test mode).
    channel_pool: Option<Arc<dyn ChannelPoolLike>>,
    /// When `Some`, new groups are created with `TcpRaftNetworkFactory`
    /// (AppendEntries/Vote over binary TCP, snapshot over gRPC fallback).
    tcp_network: Option<TcpNetworkConfig>,
    /// When `Some`, `start_group_rocks` will use `RocksLogStorage` at `{base_path}/{group_id}`.
    #[cfg(feature = "rocks")]
    rocks_base_path: Option<String>,
    snapshot_logs_since_last: u64,
}

impl RaftGroupManager {
    /// Create a new `RaftGroupManager` in single-node / test mode.
    ///
    /// Groups started via `start_group` will use `NoopNetworkFactory`.
    pub fn new(node_id: NodeId, sm_factory: StateMachineFactory) -> Self {
        info!(node_id, "creating RaftGroupManager (single-node mode)");
        Self {
            node_id,
            groups: Arc::new(DashMap::new()),
            sm_factory: Arc::new(sm_factory),
            channel_pool: None,
            tcp_network: None,
            #[cfg(feature = "rocks")]
            rocks_base_path: None,
            snapshot_logs_since_last: crate::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST,
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
            groups: Arc::new(DashMap::new()),
            sm_factory: Arc::new(sm_factory),
            channel_pool: Some(channel_pool),
            tcp_network: None,
            #[cfg(feature = "rocks")]
            rocks_base_path: None,
            snapshot_logs_since_last: crate::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST,
        }
    }

    /// Create a `RaftGroupManager` backed by TCP transport for append/vote.
    pub fn new_with_tcp_network(
        node_id: NodeId,
        sm_factory: StateMachineFactory,
        tcp_pool: Arc<TcpConnectionPool>,
        grpc_channel_pool: Arc<dyn ChannelPoolLike>,
        port_offset: u16,
        tls: Option<TlsPaths>,
    ) -> Self {
        info!(
            node_id,
            port_offset,
            "creating RaftGroupManager (tcp raft transport mode)"
        );
        Self {
            node_id,
            groups: Arc::new(DashMap::new()),
            sm_factory: Arc::new(sm_factory),
            channel_pool: None,
            tcp_network: Some(TcpNetworkConfig {
                tcp_pool,
                grpc_channel_pool,
                port_offset,
                tls,
            }),
            #[cfg(feature = "rocks")]
            rocks_base_path: None,
            snapshot_logs_since_last: crate::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST,
        }
    }

    /// Override the openraft `LogsSinceLast(N)` snapshot trigger threshold.
    /// See [`crate::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST`] and ADR A-0008.
    pub fn with_snapshot_logs_since_last(mut self, n: u64) -> Self {
        self.snapshot_logs_since_last = n;
        self
    }

    /// Start a new Raft group on this node.
    ///
    /// Creates a `RaftNode` and stores it. Returns error if group already exists.
    /// When a `ChannelPool` was supplied at construction time, the group will use
    /// `GrpcNetworkFactory`; otherwise `NoopNetworkFactory` is used.
    #[instrument(skip(self))]
    pub async fn start_group(&self, group_id: GroupId) -> Result<(), RrError> {
        let sm = (self.sm_factory)(group_id);
        let ctrl = sm.ingestor_offset_control();
        let read = sm.read_handle();
        let outbox = sm.outbox_control();
        let hook = sm.lifecycle_hook();
        let node = if let Some(tcp_cfg) = &self.tcp_network {
            let factory = TcpRaftNetworkFactory::new(
                Arc::clone(&tcp_cfg.tcp_pool),
                Arc::clone(&tcp_cfg.grpc_channel_pool),
                group_id,
                tcp_cfg.port_offset,
                tcp_cfg.tls.clone(),
            );
            RaftNode::new_with_factory(
                group_id,
                self.node_id,
                sm,
                factory,
                self.snapshot_logs_since_last,
            )
            .await
            .map_err(|e| RrError::Application(e.to_string()))?
        } else if let Some(pool) = &self.channel_pool {
            let factory = GrpcNetworkFactory::new(pool.clone(), group_id);
            RaftNode::new_with_factory(
                group_id,
                self.node_id,
                sm,
                factory,
                self.snapshot_logs_since_last,
            )
            .await
            .map_err(|e| RrError::Application(e.to_string()))?
        } else {
            RaftNode::new_with_factory(
                group_id,
                self.node_id,
                sm,
                NoopNetworkFactory,
                self.snapshot_logs_since_last,
            )
            .await
            .map_err(|e| RrError::Application(e.to_string()))?
        };
        self.register_node(group_id, node, ctrl, read, outbox, hook)
            .await
    }

    async fn register_node(
        &self,
        group_id: GroupId,
        node: RaftNode,
        ingestor_offset_control: Option<Arc<dyn IngestorOffsetControl>>,
        read_handle: Option<Arc<dyn StateMachineReader>>,
        outbox_control: Option<Arc<dyn OutboxControl>>,
        lifecycle_hook: Option<Arc<dyn StateMachineLifecycleHook>>,
    ) -> Result<(), RrError> {
        let state = Arc::new(Mutex::new(GroupState {
            node,
            ingestor_handle: None,
            output_handle: None,
            metrics_ticker_handle: None,
            ingestor_offset_control,
            read_handle,
            outbox_control,
            learner_promoter_handle: None,
            lifecycle_hook,
            in_flight_writes: Arc::new(AtomicUsize::new(0)),
        }));

        match self.groups.entry(group_id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(state);
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                let raft = { state.lock().await.node.raft.clone() };
                let _ = raft.shutdown().await;
                return Err(RrError::Application(format!(
                    "group {group_id} already exists"
                )));
            }
        }

        debug!(group_id, node_id = self.node_id, "started raft group");
        let node_id = self.node_id;
        counter!("raft.group.start", "node_id" => node_id.to_string()).increment(1);
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
        let (_, state) = self
            .groups
            .remove(&group_id)
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        let (
            raft,
            ingestor_handle,
            output_handle,
            metrics_ticker_handle,
            learner_promoter_handle,
            lifecycle_hook,
            was_leader,
            in_flight_writes,
        ) = {
            let mut state = state.lock().await;
            let was_leader = matches!(
                state.node.raft.metrics().borrow_watched().state,
                openraft::ServerState::Leader
            );
            (
                state.node.raft.clone(),
                state.ingestor_handle.take(),
                state.output_handle.take(),
                state.metrics_ticker_handle.take(),
                state.learner_promoter_handle.take(),
                state.lifecycle_hook.take(),
                was_leader,
                Arc::clone(&state.in_flight_writes),
            )
        };

        if let Some(handle) = ingestor_handle {
            handle.stop().await;
        }
        if let Some(handle) = output_handle {
            handle.stop().await;
        }
        if let Some(ticker) = metrics_ticker_handle {
            ticker.stop().await;
        }
        if let Some(promoter) = learner_promoter_handle {
            promoter.stop().await;
        }

        let drain_deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while in_flight_writes.load(Ordering::SeqCst) > 0 {
            if tokio::time::Instant::now() >= drain_deadline {
                let remaining = in_flight_writes.load(Ordering::SeqCst);
                tracing::warn!(
                    group_id,
                    in_flight = remaining,
                    "stop_group: drain timeout, proceeding with shutdown"
                );
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        if let Some(hook) = lifecycle_hook {
            if was_leader {
                hook.on_step_down(group_id).await;
            }
            hook.on_shutdown().await;
        }

        raft.shutdown()
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
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;
        let mut guard = state.lock().await;
        if guard.ingestor_handle.is_some() {
            return Err(RrError::Application(format!(
                "ingestor already attached to group {group_id}"
            )));
        }
        let handle = crate::ingestor_runner::spawn(self.clone(), group_id, ingestor, auto_idem);
        guard.ingestor_handle = Some(handle);
        info!(group_id, "attached ingestor supervisor");
        Ok(())
    }

    /// Attach an output runner for `group_id`.
    ///
    /// The runner is leader-driven and retries outputs indefinitely with
    /// exponential backoff (see [ADR A-0007]). Provide at least one of
    /// `rr_client` (for `OutputTarget::RaftWrite`) or `kafka_producer`
    /// (for `OutputTarget::KafkaPublish`); outputs targeting an unconfigured
    /// delivery client will retry forever until the operator wires one in.
    ///
    /// [ADR A-0007]: https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0007-output-outbox-mechanism.md
    pub async fn attach_output_runner(
        self: &Arc<Self>,
        group_id: GroupId,
        rr_client: Option<Arc<rr_client::RrClient>>,
        kafka_producer: Option<Arc<dyn rr_core::output::KafkaProducer>>,
    ) -> Result<(), RrError> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;
        let mut guard = state.lock().await;
        if guard.output_handle.is_some() {
            return Err(RrError::Application(format!(
                "output runner already attached to group {group_id}"
            )));
        }
        let handle =
            crate::output_runner::spawn(self.clone(), group_id, rr_client, kafka_producer);
        guard.output_handle = Some(handle);
        info!(group_id, "attached output runner");
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
    pub async fn start_metrics_ticker(self: &Arc<Self>, group_id: GroupId) -> Result<(), RrError> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;
        let mut guard = state.lock().await;
        if guard.metrics_ticker_handle.is_some() {
            return Ok(());
        }
        let handle = crate::metrics_ticker::spawn_for_group(self.clone(), group_id);
        guard.metrics_ticker_handle = Some(handle);
        debug!(group_id, "started metrics ticker");
        Ok(())
    }

    /// Spawn the per-group [`crate::learner_promoter`] supervisor that
    /// watches replication metrics and auto-promotes caught-up
    /// learners to voters via [`Self::change_membership`].
    ///
    /// Idempotent on the same group: a second call replaces the
    /// previous promoter with the new config (so operators can tune
    /// thresholds without restarting the group). Returns an error if
    /// the group has not been started.
    pub async fn start_learner_promoter(
        self: &Arc<Self>,
        group_id: GroupId,
        config: crate::learner_promoter::AutoPromoteConfig,
    ) -> Result<(), RrError> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;
        let prev = {
            let mut guard = state.lock().await;
            guard.learner_promoter_handle.take()
        };
        if let Some(prev) = prev {
            prev.stop().await;
        }
        let handle = crate::learner_promoter::spawn_for_group(self.clone(), group_id, config);
        state.lock().await.learner_promoter_handle = Some(handle);
        info!(group_id, "started learner-promoter");
        Ok(())
    }

    /// Stop the learner-promoter for `group_id` if one is running.
    /// Returns `Ok(false)` if none was running.
    pub async fn stop_learner_promoter(&self, group_id: GroupId) -> Result<bool, RrError> {
        let Some(state) = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
        else {
            return Ok(false);
        };
        let handle = state.lock().await.learner_promoter_handle.take();
        match handle {
            Some(h) => {
                h.stop().await;
                info!(group_id, "stopped learner-promoter");
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Stop the ingestor supervisor for `group_id` if one is attached.
    /// Returns `Ok(false)` if no supervisor was running. The Raft group
    /// itself remains active.
    pub async fn stop_ingestor(&self, group_id: GroupId) -> Result<bool, RrError> {
        let Some(state) = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
        else {
            return Ok(false);
        };
        let handle = state.lock().await.ingestor_handle.take();
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
        let (raft, _guard) = self
            .get_raft_for_write(group_id)
            .await
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        let start = Instant::now();
        let result = raft
            .client_write(payload.into())
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        let gid = group_id.to_string();
        histogram!("rr_proposal_duration_seconds", "group_id" => gid.clone())
            .record(start.elapsed().as_secs_f64());

        // Update raft gauge metrics from the current raft state.
        let m = raft.metrics().borrow_watched().clone();
        let is_leader = if matches!(m.state, openraft::ServerState::Leader) {
            1.0_f64
        } else {
            0.0_f64
        };
        gauge!("rr_raft_is_leader", "group_id" => gid.clone()).set(is_leader);
        gauge!("rr_raft_current_term", "group_id" => gid.clone()).set(m.current_term as f64);
        gauge!("rr_raft_last_log_index", "group_id" => gid)
            .set(m.last_log_index.unwrap_or(0) as f64);

        Ok(result.data.into())
    }

    /// Submit multiple write entries to a Raft group in a single batch.
    ///
    /// Returns state-machine responses in the same order as input payloads.
    /// Uses OpenRaft `client_write_many()` to reduce per-entry Raft round trips.
    #[instrument(skip(self, payloads))]
    pub async fn write_batch(
        &self,
        group_id: GroupId,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<Vec<u8>>, RrError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let (raft, _guard) = self
            .get_raft_for_write(group_id)
            .await
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        let start = Instant::now();
        let mut stream = raft
            .client_write_many(payloads.into_iter().map(Into::into))
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        let mut responses = Vec::new();
        while let Some(item) = stream.next().await {
            let write_result = item.map_err(|e| RrError::Application(e.to_string()))?;
            match write_result {
                Ok(resp) => responses.push(resp.response.into()),
                Err(forward) => {
                    return Err(RrError::Raft(RaftError::NotLeader {
                        leader: forward.leader_id,
                    }));
                }
            }
        }

        let gid = group_id.to_string();
        histogram!("rr_proposal_duration_seconds", "group_id" => gid.clone())
            .record(start.elapsed().as_secs_f64());

        // Update raft gauge metrics from the current raft state.
        let m = raft.metrics().borrow_watched().clone();
        let is_leader = if matches!(m.state, openraft::ServerState::Leader) {
            1.0_f64
        } else {
            0.0_f64
        };
        gauge!("rr_raft_is_leader", "group_id" => gid.clone()).set(is_leader);
        gauge!("rr_raft_current_term", "group_id" => gid.clone()).set(m.current_term as f64);
        gauge!("rr_raft_last_log_index", "group_id" => gid)
            .set(m.last_log_index.unwrap_or(0) as f64);

        Ok(responses)
    }

    /// Read directly from local state machine without raft coordination.
    ///
    /// This is a stale read and may lag behind leader commit/apply progress.
    #[instrument(skip(self, payload))]
    pub async fn local_read(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| {
                RrError::Application(format!(
                    "group {group_id} state machine does not support local read"
                ))
            })?;
        let read_handle = state.lock().await.read_handle.clone().ok_or_else(|| {
            RrError::Application(format!(
                "group {group_id} state machine does not support local read"
            ))
        })?;

        read_handle.read(payload).await
    }

    /// Read from local state machine with linearizable semantics.
    ///
    /// Uses openraft ReadIndex to confirm leader commit index, then waits
    /// until local state machine has applied through that point before reading.
    #[instrument(skip(self, payload))]
    pub async fn linearizable_read(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        let raft = self
            .get_raft(group_id)
            .await
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        let linearizer = raft
            .get_read_linearizer(ReadPolicy::ReadIndex)
            .await
            .map_err(|e| {
                RrError::Application(format!(
                    "linearizable read index failed for group {group_id}: {e}"
                ))
            })?;

        linearizer.await_ready(&raft).await.map_err(|e| {
            RrError::Application(format!(
                "linearizable read await_ready failed for group {group_id}: {e}"
            ))
        })?;

        self.local_read(group_id, payload).await
    }

    /// Override the state machine's latest ingestor offset for `group_id`.
    ///
    /// The offset is persisted in the adapter-level ingestor state and then
    /// emitted in subsequent snapshots. `None` clears/unsets the value.
    pub async fn set_ingestor_offset(
        &self,
        group_id: GroupId,
        offset: Option<i64>,
    ) -> Result<(), RrError> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        let control = state
            .lock()
            .await
            .ingestor_offset_control
            .clone()
            .ok_or_else(|| {
                RrError::Application(format!(
                    "group {group_id} state machine does not support ingestor offset override"
                ))
            })?;

        let encoded = match offset {
            Some(v) => Some(
                bincode::serde::encode_to_vec(v, standard())
                    .map_err(|e| RrError::Application(format!("encode ingestor offset: {e}")))?,
            ),
            None => None,
        };
        control.set_offset(encoded);
        Ok(())
    }

    /// Read current ingestor offset bytes from state-machine adapter control.
    pub async fn latest_ingestor_offset(
        &self,
        group_id: GroupId,
    ) -> Result<Option<Vec<u8>>, RrError> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;
        let control = state
            .lock()
            .await
            .ingestor_offset_control
            .clone()
            .ok_or_else(|| {
                RrError::Application(format!(
                    "group {group_id} state machine does not support ingestor offset override"
                ))
            })?;
        Ok(control.get_offset())
    }

    /// Get pending outputs for a group.
    pub async fn pending_outputs(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<rr_core::output::Output>, RrError> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;
        let control = state
            .lock()
            .await
            .outbox_control
            .clone()
            .ok_or_else(|| {
                RrError::Application(format!(
                    "group {group_id} state machine does not support outbox"
                ))
            })?;
        Ok(control.pending_outputs())
    }

    /// Advance output sent watermark for a group.
    pub async fn advance_output_watermark(&self, group_id: GroupId, seq: u64) -> Result<(), RrError> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;
        let control = state
            .lock()
            .await
            .outbox_control
            .clone()
            .ok_or_else(|| {
                RrError::Application(format!(
                    "group {group_id} state machine does not support outbox"
                ))
            })?;
        control.advance_watermark(seq);
        Ok(())
    }

    /// Get a reference to a Raft group's `openraft::Raft` instance.
    ///
    /// Returns `None` if the group does not exist.
    #[instrument(skip(self))]
    pub async fn get_raft(
        &self,
        group_id: GroupId,
    ) -> Option<openraft::Raft<RrTypeConfig, crate::sm_adapter::RrStateMachine>> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))?;
        let raft = state.lock().await.node.raft.clone();
        Some(raft)
    }

    /// Get a Raft reference for a write operation, paired with an
    /// [`InFlightWriteGuard`] that tracks the write until it completes.
    ///
    /// Returns `None` if the group does not exist.
    #[instrument(skip(self))]
    pub async fn get_raft_for_write(
        &self,
        group_id: GroupId,
    ) -> Option<(
        openraft::Raft<RrTypeConfig, crate::sm_adapter::RrStateMachine>,
        InFlightWriteGuard,
    )> {
        let state = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))?;
        let guard = state.lock().await;
        let raft = guard.node.raft.clone();
        let counter = Arc::clone(&guard.in_flight_writes);
        drop(guard);
        Some((raft, InFlightWriteGuard::new(counter)))
    }

    #[instrument(skip(self))]
    /// Returns the list of active group IDs.
    pub async fn group_ids(&self) -> Vec<GroupId> {
        self.groups.iter().map(|entry| *entry.key()).collect()
    }

    #[instrument(skip(self))]
    /// Returns the number of active groups.
    pub async fn group_count(&self) -> usize {
        self.groups.len()
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
            groups: Arc::new(DashMap::new()),
            sm_factory: Arc::new(sm_factory),
            channel_pool: None,
            tcp_network: None,
            rocks_base_path: Some(base_path),
            snapshot_logs_since_last: crate::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST,
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
        let sm = (self.sm_factory)(group_id);
        let ctrl = sm.ingestor_offset_control();
        let read = sm.read_handle();
        let outbox = sm.outbox_control();
        let hook = sm.lifecycle_hook();
        let node = RaftNode::new_with_rocks_factory(
            group_id,
            self.node_id,
            sm,
            NoopNetworkFactory,
            &path,
            self.snapshot_logs_since_last,
        )
        .await
        .map_err(|e| RrError::Application(e.to_string()))?;
        self.register_node(group_id, node, ctrl, read, outbox, hook)
            .await
    }

    /// Notify per-group lifecycle hook that leadership was acquired.
    pub(crate) async fn notify_leader_elected(&self, group_id: GroupId) {
        let hook = if let Some(state) = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
        {
            state.lock().await.lifecycle_hook.clone()
        } else {
            None
        };
        if let Some(hook) = hook {
            hook.on_leader_elected(group_id).await;
        }
    }

    /// Notify per-group lifecycle hook that leadership was lost.
    pub(crate) async fn notify_step_down(&self, group_id: GroupId) {
        let hook = if let Some(state) = self
            .groups
            .get(&group_id)
            .map(|entry| Arc::clone(entry.value()))
        {
            state.lock().await.lifecycle_hook.clone()
        } else {
            None
        };
        if let Some(hook) = hook {
            hook.on_step_down(group_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::network_factory::ChannelPool;
    use crate::service_adapter::ServiceStateMachineAdapter;
    use async_trait::async_trait;
    use bincode::config::standard;
    use openraft::type_config::async_runtime::WatchReceiver;
    use rr_core::error::RrError;
    use rr_core::service::Service;
    use rr_core::types::LogIndex;

    struct NoopSM;

    #[derive(Default)]
    struct LifecycleCounters {
        leader_elected: AtomicUsize,
        step_down: AtomicUsize,
        shutdown: AtomicUsize,
    }

    #[async_trait]
    impl StateMachineLifecycleHook for LifecycleCounters {
        async fn on_leader_elected(&self, _group_id: GroupId) {
            self.leader_elected.fetch_add(1, Ordering::SeqCst);
        }

        async fn on_step_down(&self, _group_id: GroupId) {
            self.step_down.fetch_add(1, Ordering::SeqCst);
        }

        async fn on_shutdown(&self) {
            self.shutdown.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct LifecycleSM {
        hook: Arc<LifecycleCounters>,
    }

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

    #[async_trait]
    impl StateMachine for LifecycleSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }

        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(Vec::new())
        }

        async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RrError> {
            Ok(())
        }

        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }

        fn lifecycle_hook(&self) -> Option<Arc<dyn StateMachineLifecycleHook>> {
            Some(Arc::clone(&self.hook) as Arc<dyn StateMachineLifecycleHook>)
        }
    }

    fn noop_factory() -> StateMachineFactory {
        Box::new(|_group_id| Box::new(NoopSM))
    }

    fn offset_capable_factory() -> StateMachineFactory {
        Box::new(|group_id| Box::new(ServiceStateMachineAdapter::new(group_id, NoopService)))
    }

    fn lifecycle_factory(counters: Arc<LifecycleCounters>) -> StateMachineFactory {
        Box::new(move |_group_id| {
            Box::new(LifecycleSM {
                hook: Arc::clone(&counters),
            })
        })
    }

    async fn wait_for_counter(counter: &AtomicUsize, min: usize, timeout: std::time::Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if counter.load(Ordering::SeqCst) >= min {
                return;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for counter to reach {min}");
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
        let metrics = raft.metrics().borrow_watched().clone();
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
            raft.metrics().borrow_watched().current_leader.is_some(),
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

    #[tokio::test]
    async fn test_set_ingestor_offset_updates_adapter_control() {
        let mgr = RaftGroupManager::new(1, offset_capable_factory());
        mgr.start_group(1).await.unwrap();

        mgr.set_ingestor_offset(1, Some(123)).await.unwrap();
        let raw = mgr
            .latest_ingestor_offset(1)
            .await
            .unwrap()
            .expect("offset should be present");
        let (decoded, _): (i64, usize) =
            bincode::serde::decode_from_slice(&raw, standard()).unwrap();
        assert_eq!(decoded, 123);

        mgr.set_ingestor_offset(1, None).await.unwrap();
        assert!(mgr.latest_ingestor_offset(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_lifecycle_hooks_on_leader_elected_step_down_and_shutdown() {
        let counters = Arc::new(LifecycleCounters::default());
        let mgr = Arc::new(RaftGroupManager::new(
            1,
            lifecycle_factory(Arc::clone(&counters)),
        ));

        mgr.start_group(1).await.unwrap();
        mgr.start_metrics_ticker(1).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            BasicNode {
                addr: String::new(),
            },
        );
        mgr.init_cluster(1, members).await.unwrap();

        wait_for_counter(
            &counters.leader_elected,
            1,
            std::time::Duration::from_secs(5),
        )
        .await;

        mgr.stop_group(1).await.unwrap();

        assert!(
            counters.step_down.load(Ordering::SeqCst) >= 1,
            "expected at least one step-down callback"
        );
        assert_eq!(
            counters.shutdown.load(Ordering::SeqCst),
            1,
            "shutdown callback should fire exactly once"
        );
    }
}
