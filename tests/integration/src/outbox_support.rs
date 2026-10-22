//! Shared harness for the INV-4 outbox failover e2e suite
//! (`.sisyphus/plans/rr-inv4-outbox-failover-e2e.md`, waves O-1/O-2).
//!
//! Why this module exists: the harness `ClusterBuilder` hard-codes
//! `echo_factory()` (lib.rs), whose `EchoSM` has no outbox control — an
//! output runner attached to it would park with `OutboxUnsupported` forever
//! (output_runner.rs). There is also no in-repo precedent of a `Service`
//! that emits `Output`s, nor of an `attach_output_runner` caller. This
//! module supplies all of it:
//!
//! - [`build_outbox_cluster`] — a per-group-factory variant of the
//!   `client_batching.rs` real-network cluster template.
//! - [`OutboxEchoService`] — a `Service` emitting exactly one `Output` per
//!   request. **Pitfall**: it overrides `handle_with_outputs_ctx`, NOT
//!   `handle_with_outputs` — the real apply path
//!   (`ServiceStateMachineAdapter`) calls the ctx variant, whose default
//!   impl delegates to `handle_with_ctx` and bypasses
//!   `handle_with_outputs` (rr-core/src/service.rs:184).
//! - [`CapturingKafka`] — a shared-recording [`KafkaProducer`] test double
//!   (template: `crates/rr-raft/src/output_runner.rs:421-468`). All nodes
//!   share one `Arc`; only the leader's runner publishes, so after a
//!   failover the new leader's deliveries land in the same record vec.
//! - [`poll_until`] — the only waiting primitive the suite uses (design
//!   rule: no fixed sleeps in tests; every timing window is poll + timeout).

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use openraft::BasicNode;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::error::RrError;
use rr_core::framed_snapshot::FramedSnapshot;
use rr_core::output::{HandleResult, KafkaProducer, Output, OutputTarget};
use rr_core::service::Service;
use rr_core::storage::StorageEngine;
use rr_core::types::{ApplyContext, GroupId, NodeAddr, NodeId, PartitionConfig};
use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
use rr_raft::service_adapter::ServiceStateMachineAdapter;
use rr_router::router::ShardRouter;
use rr_server::config::NodeConfig;
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;

/// Topic the INV-4 Kafka arm "publishes" to (the sink records it verbatim).
pub const KAFKA_TOPIC: &str = "inv4-outbox";
/// Partition the INV-4 Kafka arm publishes to.
pub const KAFKA_PARTITION: i32 = 0;
/// Message key the INV-4 Kafka arm publishes with.
pub const KAFKA_KEY: &[u8] = b"inv4-key";

/// `KafkaPublish` target used by the INV-4 Kafka arm (`OutboxEchoService`).
pub fn kafka_echo_target() -> OutputTarget {
    OutputTarget::KafkaPublish {
        topic: KAFKA_TOPIC.to_string(),
        partition: KAFKA_PARTITION,
        key: KAFKA_KEY.to_vec(),
    }
}

/// Poll `condition` every `interval` until it returns `Some(value)` or
/// `timeout` elapses. Returns `Some(value)` on success, `None` on timeout.
///
/// Every wait in the INV-4 suite goes through this helper (design rule: no
/// fixed sleeps; timing windows are covered by polling with timeouts).
pub async fn poll_until<T, F, Fut>(
    timeout: Duration,
    interval: Duration,
    mut condition: F,
) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = condition().await {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Build a `node_count`-node real-gRPC cluster hosting `group_count` groups
/// (each group spans all nodes), with per-group state machines built by
/// `factory` — which receives the `group_id`, so callers can dispatch a
/// different service per group.
///
/// Skeleton copied from `tests/integration/src/client_batching.rs:60-127`
/// (`build_service_cluster`) — the third copy of this cluster-construction
/// template in the repo (design-doc risk register accepts the drift risk;
/// extracting a common builder is deferred tech debt). Deltas vs the
/// template: `group_count` loops for partition registration and raft
/// `initialize`, and the caller-supplied `factory` instead of the hard-coded
/// echo adapter.
pub async fn build_outbox_cluster(
    node_count: usize,
    group_count: usize,
    factory: StateMachineFactory,
) -> (Vec<Arc<RrNode>>, Arc<MemConfigProvider>) {
    let provider = Arc::new(MemConfigProvider::new());
    let node_ids: Vec<NodeId> = (1..=(node_count as NodeId)).collect();

    for group_id in 1..=(group_count as GroupId) {
        provider
            .add_partition(
                group_id,
                PartitionConfig::SingleId(group_id),
                group_id,
                node_ids.clone(),
            )
            .await;
    }

    let mut node_addrs: Vec<(NodeId, String)> = Vec::new();
    for &node_id in &node_ids {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // release port; small TOCTOU window is acceptable in tests
        let addr = format!("127.0.0.1:{port}");
        provider
            .add_node(node_id, NodeAddr { addr: addr.clone() })
            .await;
        node_addrs.push((node_id, addr));
    }

    let factory = Arc::new(factory);
    let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
    let mut nodes: Vec<Arc<RrNode>> = Vec::new();
    for (node_id, addr) in &node_addrs {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider_dyn: Arc<dyn ConfigProvider> = Arc::clone(&provider) as _;
        let mgr = Arc::new(RaftGroupManager::new_with_network(
            *node_id,
            {
                let f = Arc::clone(&factory);
                Box::new(move |gid| (f)(gid)) as StateMachineFactory
            },
            Arc::clone(&pool),
        ));
        let router = Arc::new(ShardRouter::new(Arc::clone(&provider_dyn)));
        let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
        let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));
        let config = NodeConfig::new(*node_id, addr.clone(), storage, provider_dyn, {
            // ignored once group_manager is pre-built (template parity)
            let f = Arc::clone(&factory);
            Box::new(move |gid| (f)(gid)) as StateMachineFactory
        })
        .with_group_manager(Arc::clone(&mgr))
        .with_raft_lookup(lookup)
        .with_client_dispatch(dispatch)
        .with_allow_insecure(true);
        let node = Arc::new(RrNode::new(config).unwrap());
        node.start().await.unwrap();
        nodes.push(node);
    }

    let mut members = BTreeMap::new();
    for (node_id, addr) in &node_addrs {
        members.insert(*node_id, BasicNode { addr: addr.clone() });
    }
    for group_id in 1..=(group_count as GroupId) {
        let raft = nodes[0]
            .group_manager()
            .get_raft(group_id)
            .await
            .expect("node 1 hosts every group");
        // `initialize` is idempotent — ignore AlreadyInitialized errors.
        let _ = raft.initialize(members.clone()).await;
    }

    // Let gRPC servers finish coming up before clients dial — same pacing as
    // the template (NOT a correctness wait; readiness is always polled).
    tokio::time::sleep(Duration::from_millis(100)).await;
    (nodes, provider)
}

/// `OutboxEchoService` wired into the framework adapter: every replica of
/// every group built by this factory emits one `Output` per request.
pub fn outbox_echo_factory(target: OutputTarget) -> StateMachineFactory {
    Box::new(move |gid| {
        Box::new(ServiceStateMachineAdapter::new(
            gid,
            OutboxEchoService::new(target.clone()),
        ))
    })
}

/// Echo service that emits exactly one framework `Output` per request.
///
/// The idempotency key is deterministically derived from the request bytes
/// (`"{request}:0"`, per the `output.rs` convention), so tests can compute
/// the expected delivery set without any service-side logging. The output
/// payload is the request bytes (echo semantics) and the target is a
/// constructor parameter — `KafkaPublish` for the Kafka arm, `RaftWrite`
/// for wave O-3.
///
/// Snapshot/restore carry an 8-byte applied-entry counter, EchoSM-style.
pub struct OutboxEchoService {
    target: OutputTarget,
    applied: AtomicU64,
}

impl OutboxEchoService {
    pub fn new(target: OutputTarget) -> Self {
        Self {
            target,
            applied: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Service for OutboxEchoService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        self.applied.fetch_add(1, Ordering::SeqCst);
        Ok(request)
    }

    fn name(&self) -> &str {
        "outbox-echo"
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        Ok(self.applied.load(Ordering::SeqCst).to_be_bytes().to_vec())
    }

    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        self.applied.store(
            if data.len() == 8 {
                u64::from_be_bytes(data.try_into().expect("length checked above"))
            } else {
                // Legacy empty snapshots restore as "no entries applied".
                0
            },
            Ordering::SeqCst,
        );
        Ok(())
    }

    // CRITICAL (design doc "Service 覆盖陷阱"): the real apply path calls
    // `handle_with_outputs_ctx` (ServiceStateMachineAdapter), whose default
    // impl delegates to `handle_with_ctx` and BYPASSES `handle_with_outputs`.
    // Overriding only `handle_with_outputs` would leave outputs permanently
    // empty on a real cluster — the suite would pass vacuously.
    async fn handle_with_outputs_ctx(
        &self,
        request: Vec<u8>,
        ctx: ApplyContext,
    ) -> Result<HandleResult, RrError> {
        let _ = ctx;
        self.applied.fetch_add(1, Ordering::SeqCst);
        let idempotency_key = format!("{}:0", String::from_utf8_lossy(&request));
        Ok(HandleResult::with_outputs(
            request.clone(),
            vec![Output::new(idempotency_key, self.target.clone(), request)],
        ))
    }
}

/// Records every applied request payload into a shared vec — the target-group
/// service for the RaftWrite arm (wave O-3). Same pattern as the harness's
/// `CountingService` (lib.rs:510-553), promoted to the shared module.
#[derive(Clone)]
pub struct CountingService {
    applied: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl CountingService {
    pub fn new() -> (Self, Arc<Mutex<Vec<Vec<u8>>>>) {
        let applied = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                applied: Arc::clone(&applied),
            },
            applied,
        )
    }
}

#[async_trait]
impl Service for CountingService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        self.applied.lock().unwrap().push(request.clone());
        Ok(request)
    }

    fn name(&self) -> &str {
        "outbox-counting"
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        Ok(vec![])
    }

    async fn restore(&mut self, _data: &[u8]) -> Result<(), RrError> {
        Ok(())
    }
}

/// Factory adapter: every replica of the target group records applies into
/// the same shared vec (pattern: `counting_factory`, lib.rs:546-553).
pub fn counting_factory(applied: Arc<Mutex<Vec<Vec<u8>>>>) -> StateMachineFactory {
    Box::new(move |gid| {
        let svc = CountingService {
            applied: Arc::clone(&applied),
        };
        Box::new(ServiceStateMachineAdapter::new(gid, svc))
    })
}

/// One captured Kafka record: `(topic, partition, key, payload, idempotency_key)`.
pub type PublishedRecord = (String, i32, Vec<u8>, Vec<u8>, String);

/// `KafkaProducer` test double recording every publish into a shared,
/// `Arc`-clonable vec (template: `crates/rr-raft/src/output_runner.rs:421-468`,
/// restructured around `Arc<Mutex<_>>` so one instance fans out to all
/// nodes' runners — only the leader's runner actually publishes, and after a
/// failover the new leader's deliveries land in the same vec).
pub struct CapturingKafka {
    published: Arc<Mutex<Vec<PublishedRecord>>>,
    fail_n: AtomicU64,
}

impl CapturingKafka {
    pub fn new() -> Self {
        Self {
            published: Arc::new(Mutex::new(Vec::new())),
            fail_n: AtomicU64::new(0),
        }
    }

    /// Script the first `n` publishes to fail (transient-error testing).
    pub fn fail_first_n(n: u64) -> Self {
        Self {
            published: Arc::new(Mutex::new(Vec::new())),
            fail_n: AtomicU64::new(n),
        }
    }

    /// Wrap in `Arc` for sharing across nodes and runners.
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }

    /// Snapshot of every record published so far.
    pub fn records(&self) -> Vec<PublishedRecord> {
        self.published.lock().unwrap().clone()
    }
}

impl Default for CapturingKafka {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KafkaProducer for CapturingKafka {
    async fn publish(
        &self,
        topic: &str,
        partition: i32,
        key: &[u8],
        payload: &[u8],
        idempotency_key: &str,
    ) -> Result<(), RrError> {
        let remaining = self.fail_n.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_n.fetch_sub(1, Ordering::SeqCst);
            return Err(RrError::Application(format!(
                "scripted failure (remaining={remaining})"
            )));
        }
        self.published.lock().unwrap().push((
            topic.to_string(),
            partition,
            key.to_vec(),
            payload.to_vec(),
            idempotency_key.to_string(),
        ));
        Ok(())
    }
}

/// Trigger a snapshot for `group_id` on `node`, wait for it to materialise,
/// and return the decoded framed envelope.
///
/// openraft's `trigger().snapshot()` schedules the snapshot without waiting
/// for completion (vendor/openraft/src/raft/trigger.rs:82-89), so this polls
/// `fetch_snapshot_bytes` (two-phase precedent: lib.rs:915-943).
///
/// Mechanism fact the INV-4 suite pins with this helper: the outbox
/// `sent_watermark` is leader-local memory — never raft-replicated — so a
/// follower's own snapshot always reports `sent_watermark == 0` even after
/// the leader delivered everything.
pub async fn fetch_framed_snapshot(
    node: &RrNode,
    group_id: GroupId,
    timeout: Duration,
) -> Result<FramedSnapshot> {
    let raft = node
        .group_manager()
        .get_raft(group_id)
        .await
        .ok_or_else(|| anyhow!("node {} missing group {group_id}", node.node_id()))?;
    raft.trigger()
        .snapshot()
        .await
        .map_err(|e| anyhow!("trigger snapshot on node {}: {e}", node.node_id()))?;

    let bytes = poll_until(timeout, Duration::from_millis(100), || {
        let mgr = node.group_manager();
        async move { mgr.fetch_snapshot_bytes(group_id).await.ok().flatten() }
    })
    .await
    .ok_or_else(|| {
        anyhow!(
            "snapshot for group {group_id} on node {} did not materialise within {timeout:?}",
            node.node_id()
        )
    })?;

    FramedSnapshot::decode(&bytes)?
        .ok_or_else(|| anyhow!("group {group_id} snapshot is not a framed snapshot"))
}
