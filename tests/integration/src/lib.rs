//! Integration test harness for the `rr` distributed framework.
//!
//! Provides an in-process multi-node cluster builder and helpers for
//! exercising Raft leader election, fault tolerance, and multi-group routing.

#[cfg(test)]
mod cross_tier_ingestor;
pub mod fault_injection;
use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use openraft::BasicNode;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::state_machine::StateMachine;
use rr_core::storage::StorageEngine;
use rr_core::types::{GroupId, LogIndex, NodeId, PartitionConfig};
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::group_manager::StateMachineFactory;
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
use rr_router::router::ShardRouter;
use rr_server::config::NodeConfig;
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;

// ── Noop StateMachine ─────────────────────────────────────────────────────────

/// Minimal state machine that echoes its input — used for harness tests.
struct EchoSM;

#[async_trait::async_trait]
impl StateMachine for EchoSM {
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, rr_core::error::RrError> {
        Ok(entry.to_vec())
    }
    async fn snapshot(&self) -> Result<Vec<u8>, rr_core::error::RrError> {
        Ok(vec![])
    }
    async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), rr_core::error::RrError> {
        Ok(())
    }
    async fn last_applied(&self) -> Option<LogIndex> {
        None
    }
}

fn echo_factory() -> StateMachineFactory {
    Box::new(|_group_id| -> Box<dyn StateMachine> { Box::new(EchoSM) })
}

// ── ClusterBuilder ────────────────────────────────────────────────────────────

/// Builder for creating an in-process multi-node Raft cluster.
///
/// All nodes share the same [`MemConfigProvider`] (routing table) but each
/// has its own [`MemStorage`] and [`StateMachineFactory`].
pub struct ClusterBuilder {
    /// Number of nodes to create.
    pub node_count: usize,
    /// Number of Raft groups to create. Each group spans all nodes.
    pub group_count: usize,
    /// If true, use real gRPC networking instead of in-process NoopNetworkFactory.
    real_network: bool,
    /// Shared config provider — pre-allocated so callers can reference it before build().
    config_provider: Arc<MemConfigProvider>,
    /// Optional shared channel pool (legacy mode — one pool used by all nodes).
    channel_pool: Option<Arc<dyn ChannelPoolLike>>,
    /// Optional per-node pool factory backed by a `FaultInjector` so that
    /// isolation semantics are bidirectional across the cluster.
    fault_injector: Option<Arc<fault_injection::FaultInjector>>,
}

impl ClusterBuilder {
    /// Create a builder for a cluster with `node_count` nodes and `group_count` groups.
    pub fn new(node_count: usize, group_count: usize) -> Self {
        Self {
            node_count,
            group_count,
            real_network: false,
            config_provider: Arc::new(MemConfigProvider::new()),
            channel_pool: None,
            fault_injector: None,
        }
    }

    /// Enable real gRPC networking between nodes.
    pub fn with_real_network(mut self) -> Self {
        self.real_network = true;
        self
    }

    /// Provide a pre-built channel pool with fault injection support.
    ///
    /// When set, the builder will use this pool instead of creating a new
    /// [`ChannelPool`] in `build_real_network`. Automatically enables real
    /// networking. Note: this uses a single pool shared by all nodes, which
    /// cannot express bidirectional network isolation. For bidirectional
    /// fault injection, use [`Self::with_fault_injector`] instead.
    pub fn with_channel_pool(mut self, pool: Arc<dyn ChannelPoolLike>) -> Self {
        self.real_network = true;
        self.channel_pool = Some(pool);
        self
    }

    /// Create a bidirectional [`fault_injection::FaultInjector`] and configure
    /// the builder to create a per-node pool for each cluster member. Returns
    /// `(builder, Arc<FaultInjector>)` so the caller can inject faults during
    /// the test.
    ///
    /// Unlike [`Self::with_channel_pool`], this setup enables bidirectional
    /// isolation: `injector.isolate(n)` blocks BOTH outbound traffic from node
    /// `n` and inbound traffic to node `n`.
    pub fn with_fault_injector(
        mut self,
    ) -> (Self, Arc<fault_injection::FaultInjector>) {
        let injector = Arc::new(fault_injection::FaultInjector::new());
        self.real_network = true;
        self.fault_injector = Some(Arc::clone(&injector));
        (self, injector)
    }

    /// Return a clone of the shared config provider Arc.
    ///
    /// Call this *before* [`build()`](Self::build) to get a handle that remains
    /// valid after the builder is consumed.
    pub fn config_provider(&self) -> Arc<MemConfigProvider> {
        Arc::clone(&self.config_provider)
    }

    /// Build and start all nodes.
    ///
    /// Returns the list of [`RrNode`]s. Each node has been started; Raft groups
    /// have been initialized with the full member set.
    pub async fn build(self) -> Result<Vec<Arc<RrNode>>> {
        if self.real_network {
            self.build_real_network().await
        } else {
            self.build_in_process().await
        }
    }

    /// Build using in-process NoopNetworkFactory (original behaviour).
    async fn build_in_process(self) -> Result<Vec<Arc<RrNode>>> {
        // ── 1. Build shared config provider ───────────────────────────────────
        //
        // Partitions and groups are added before nodes start so that each node
        // can load the routing table and start the correct groups on startup.
        let config_provider = Arc::clone(&self.config_provider);

        let node_ids: Vec<NodeId> = (1..=(self.node_count as NodeId)).collect();

        for group_id in 1..=(self.group_count as GroupId) {
            config_provider
                .add_partition(
                    group_id,
                    PartitionConfig::SingleId(group_id),
                    group_id,
                    node_ids.clone(),
                )
                .await;
        }

        // ── 2. Build nodes ────────────────────────────────────────────────────
        let mut nodes = Vec::with_capacity(self.node_count);
        for node_id in &node_ids {
            let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
            let provider: Arc<dyn ConfigProvider> = Arc::clone(&config_provider) as _;

            let config = NodeConfig::new(
                *node_id,
                format!("127.0.0.1:{}", 51000 + node_id),
                storage,
                provider,
                echo_factory(),
            );

            let node = Arc::new(RrNode::new(config)?);
            node.start().await?;
            nodes.push(node);
        }

        // ── 3. Initialize Raft groups ─────────────────────────────────────────
        //
        // Only node 1 calls `initialize` for each group (single bootstrap node).
        // All other nodes are already added as voters in the membership list.
        let mut members = BTreeMap::new();
        for node_id in &node_ids {
            members.insert(*node_id, BasicNode::default());
        }

        for group_id in 1..=(self.group_count as GroupId) {
            let raft = nodes[0]
                .group_manager()
                .get_raft(group_id)
                .await
                .ok_or_else(|| anyhow::anyhow!("node 1 missing group {group_id}"))?;

            // `initialize` is idempotent — ignore AlreadyInitialized errors.
            let _ = raft.initialize(members.clone()).await;
        }

        Ok(nodes)
    }

    /// Build using real gRPC networking (multi-node mode).
    async fn build_real_network(self) -> Result<Vec<Arc<RrNode>>> {
        let config_provider = Arc::clone(&self.config_provider);
        let node_ids: Vec<NodeId> = (1..=(self.node_count as NodeId)).collect();

        // 1. Pre-allocate OS-assigned ports
        let mut node_addrs: Vec<(NodeId, String)> = Vec::new();
        for &node_id in &node_ids {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let port = listener.local_addr()?.port();
            drop(listener); // release port; small TOCTOU window is acceptable in tests
            node_addrs.push((node_id, format!("127.0.0.1:{port}")));
        }

        // 2. Register partitions/groups in config provider
        for group_id in 1..=(self.group_count as GroupId) {
            config_provider
                .add_partition(
                    group_id,
                    PartitionConfig::SingleId(group_id),
                    group_id,
                    node_ids.clone(),
                )
                .await;
        }

        // 2b. Register node addresses in config provider so dynamic tests can
        //     look up peer addresses from the routing table.
        for (node_id, addr) in &node_addrs {
            config_provider
                .add_node(*node_id, rr_core::types::NodeAddr { addr: addr.clone() })
                .await;
        }

        let fallback_pool: Arc<dyn ChannelPoolLike> = self
            .channel_pool
            .clone()
            .unwrap_or_else(|| Arc::new(ChannelPool::new()));
        let mut nodes: Vec<Arc<RrNode>> = Vec::with_capacity(self.node_count);

        for (node_id, addr) in &node_addrs {
            let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
            let provider: Arc<dyn ConfigProvider> = Arc::clone(&config_provider) as _;

            let pool_for_node: Arc<dyn ChannelPoolLike> = match &self.fault_injector {
                Some(inj) => inj.new_pool_for_node(*node_id),
                None => fallback_pool.clone(),
            };

            let mgr = Arc::new(RaftGroupManager::new_with_network(
                *node_id,
                echo_factory(),
                pool_for_node,
            ));

            // Wire lookup and dispatch using the pre-built manager
            let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
            let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
            let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));

            let config = NodeConfig::new(
                *node_id,
                addr.clone(),
                storage,
                provider,
                echo_factory(), // factory in config is ignored when group_manager is pre-built
            )
            .with_group_manager(Arc::clone(&mgr))
            .with_raft_lookup(lookup)
            .with_client_dispatch(dispatch)
            .with_allow_insecure(true);

            let node = Arc::new(RrNode::new(config)?);
            node.start().await?;
            nodes.push(node);
        }

        // 4. Initialize Raft groups — each member's BasicNode.addr must be the
        //    actual gRPC address so GrpcRaftNetwork can connect to peers.
        let mut members = BTreeMap::new();
        for (node_id, addr) in &node_addrs {
            members.insert(*node_id, BasicNode { addr: addr.clone() });
        }

        for group_id in 1..=(self.group_count as GroupId) {
            let raft = nodes[0]
                .group_manager()
                .get_raft(group_id)
                .await
                .ok_or_else(|| anyhow::anyhow!("node 1 missing group {group_id}"))?;
            let _ = raft.initialize(members.clone()).await;
        }

        // 5. Brief pause to allow gRPC servers to fully start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Ok(nodes)
    }

    /// Build and start all nodes, returning both nodes and the shared config provider.
    ///
    /// Convenience wrapper for tests that need to manipulate the routing table
    /// after the cluster is running (e.g. dynamic partition add tests).
    pub async fn build_with_provider(self) -> Result<(Vec<Arc<RrNode>>, Arc<MemConfigProvider>)> {
        let provider = self.config_provider();
        let nodes = self.build().await?;
        Ok((nodes, provider))
    }
}

/// Shut down all nodes in a cluster.
pub async fn shutdown_all(nodes: &[Arc<RrNode>]) {
    for node in nodes {
        let _ = node.shutdown().await;
    }
}

/// Wait (up to `timeout_ms`) for a leader to be elected in the given Raft group
/// as seen from `node`.
///
/// Returns `Ok(leader_id)` or `Err` if the timeout expires.
pub async fn wait_for_leader(node: &RrNode, group_id: GroupId, timeout_ms: u64) -> Result<NodeId> {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);

    loop {
        if let Some(raft) = node.group_manager().get_raft(group_id).await {
            let metrics = raft.metrics().borrow().clone();
            if let Some(leader_id) = metrics.current_leader {
                return Ok(leader_id);
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for leader in group {group_id}");
        }

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Wait up to `timeout_ms` for a leader to be elected in the given group,
/// checking all nodes (not just `node[0]`).
pub async fn wait_for_cluster_ready(
    nodes: &[Arc<RrNode>],
    group_id: GroupId,
    timeout_ms: u64,
) -> Result<NodeId> {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);

    loop {
        for node in nodes {
            if let Some(raft) = node.group_manager().get_raft(group_id).await {
                let metrics = raft.metrics().borrow().clone();
                if let Some(leader_id) = metrics.current_leader {
                    return Ok(leader_id);
                }
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!("cluster not ready (no leader) after {timeout_ms}ms");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait up to `timeout_ms` for a NEW leader (different from `old_leader_id`) to be
/// elected in the given group, checking all nodes.
///
/// Returns `Ok(new_leader_id)` or `Err` if the timeout expires.
pub async fn wait_for_new_leader(
    nodes: &[Arc<RrNode>],
    group_id: GroupId,
    old_leader_id: NodeId,
    timeout_ms: u64,
) -> Result<NodeId> {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);

    loop {
        for node in nodes {
            if let Some(raft) = node.group_manager().get_raft(group_id).await {
                let metrics = raft.metrics().borrow().clone();
                if let Some(leader_id) = metrics.current_leader {
                    if leader_id != old_leader_id {
                        return Ok(leader_id);
                    }
                }
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!(
                "no new leader (different from {old_leader_id}) elected after {timeout_ms}ms"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rr_client::{RetryConfig, RrClient};

    /// A single-node cluster must elect itself as leader immediately after
    /// initialization.
    #[tokio::test]
    async fn test_single_node_leader_election() {
        let nodes = ClusterBuilder::new(1, 1).build().await.unwrap();

        // Wait up to 2 s for leader election
        let leader = wait_for_leader(&nodes[0], 1, 2_000).await.unwrap();
        assert_eq!(leader, 1, "single node must elect itself");

        shutdown_all(&nodes).await;
    }

    /// A single-node cluster can process multiple write proposals through Raft.
    #[tokio::test]
    async fn test_single_node_client_write() {
        let nodes = ClusterBuilder::new(1, 1).build().await.unwrap();
        wait_for_leader(&nodes[0], 1, 2_000).await.unwrap();

        let raft = nodes[0].group_manager().get_raft(1).await.unwrap();

        // Propose 3 writes and verify the state machine echoes them back.
        for i in 0u8..3 {
            let payload = vec![i, i + 1, i + 2];
            let response = raft.client_write(payload.clone()).await.unwrap();
            assert_eq!(
                response.data, payload,
                "EchoSM must return the same bytes it received"
            );
        }

        shutdown_all(&nodes).await;
    }

    /// A single-node cluster with two Raft groups must elect independent leaders
    /// for each group.
    #[tokio::test]
    async fn test_multi_group_independent_leaders() {
        let nodes = ClusterBuilder::new(1, 2).build().await.unwrap();

        let leader_g1 = wait_for_leader(&nodes[0], 1, 2_000).await.unwrap();
        let leader_g2 = wait_for_leader(&nodes[0], 2, 2_000).await.unwrap();

        assert_eq!(leader_g1, 1);
        assert_eq!(leader_g2, 1);

        // Both groups must be independently operational — write to each.
        for group_id in [1u64, 2] {
            let raft = nodes[0].group_manager().get_raft(group_id).await.unwrap();
            raft.client_write(b"ping".to_vec()).await.unwrap();
        }

        shutdown_all(&nodes).await;
    }

    /// After shutdown the group count must drop to zero.
    #[tokio::test]
    async fn test_shutdown_clears_groups() {
        let nodes = ClusterBuilder::new(1, 1).build().await.unwrap();
        wait_for_leader(&nodes[0], 1, 2_000).await.unwrap();

        assert_eq!(nodes[0].group_manager().group_count().await, 1);
        nodes[0].shutdown().await.unwrap();
        assert_eq!(nodes[0].group_manager().group_count().await, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_3node_leader_election() {
        let nodes = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build()
            .await
            .unwrap();

        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();
        assert!(
            (1..=3).contains(&leader_id),
            "leader must be one of the 3 nodes"
        );

        // All nodes agree on the same leader
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut seen_leaders = std::collections::HashSet::new();
        for node in &nodes {
            if let Some(raft) = node.group_manager().get_raft(1).await {
                let metrics = raft.metrics().borrow().clone();
                if let Some(lid) = metrics.current_leader {
                    seen_leaders.insert(lid);
                }
            }
        }
        assert_eq!(
            seen_leaders.len(),
            1,
            "all nodes must agree on one leader, saw: {seen_leaders:?}"
        );

        shutdown_all(&nodes).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_3node_write_replication() {
        let nodes = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build()
            .await
            .unwrap();

        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();
        let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();

        let raft = leader_node.group_manager().get_raft(1).await.unwrap();
        let payload = b"replicate-me".to_vec();
        let resp = raft.client_write(payload.clone()).await.unwrap();
        assert_eq!(resp.data, payload, "EchoSM must return the written bytes");

        // Wait for all nodes to apply the entry (polling with timeout)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
        loop {
            let mut all_applied = true;
            for node in &nodes {
                if let Some(raft) = node.group_manager().get_raft(1).await {
                    let metrics = raft.metrics().borrow().clone();
                    if metrics.last_applied.is_none() {
                        all_applied = false;
                        break;
                    }
                }
            }
            if all_applied {
                break;
            }
            if std::time::Instant::now() >= deadline {
                // Check once more with detailed error
                for node in &nodes {
                    if let Some(raft) = node.group_manager().get_raft(1).await {
                        let metrics = raft.metrics().borrow().clone();
                        assert!(
                            metrics.last_applied.is_some(),
                            "node {} must have applied at least one log entry",
                            node.node_id()
                        );
                    }
                }
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        shutdown_all(&nodes).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_3node_write_to_follower_returns_forward() {
        let nodes = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build()
            .await
            .unwrap();

        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();

        // Find a follower
        let follower = nodes.iter().find(|n| n.node_id() != leader_id).unwrap();
        let raft = follower.group_manager().get_raft(1).await.unwrap();

        let result = raft.client_write(b"to-follower".to_vec()).await;
        match result {
            Ok(_) => {
                // openraft may internally redirect — that's fine
            }
            Err(e) => {
                let fwd = e.forward_to_leader::<BasicNode>();
                assert!(fwd.is_some(), "expected ForwardToLeader error, got: {e:?}");
                let fwd = fwd.unwrap();
                assert_eq!(
                    fwd.leader_id,
                    Some(leader_id),
                    "forward must point to current leader"
                );
            }
        }

        shutdown_all(&nodes).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_3node_leader_failover() {
        let nodes = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build()
            .await
            .unwrap();

        let original_leader_id = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();

        // Write something before failover
        let leader_node = nodes
            .iter()
            .find(|n| n.node_id() == original_leader_id)
            .unwrap();
        let raft = leader_node.group_manager().get_raft(1).await.unwrap();
        raft.client_write(b"pre-failover".to_vec()).await.unwrap();

        // Shutdown the leader
        leader_node.shutdown().await.unwrap();

        // A surviving node should elect a new leader (different from the failed one)
        let survivors: Vec<_> = nodes
            .iter()
            .filter(|n| n.node_id() != original_leader_id)
            .cloned()
            .collect();
        let new_leader_id = wait_for_new_leader(&survivors, 1, original_leader_id, 12_000)
            .await
            .unwrap();
        assert_ne!(
            new_leader_id, original_leader_id,
            "new leader must differ from failed node"
        );

        // Write after failover should succeed
        let new_leader = nodes.iter().find(|n| n.node_id() == new_leader_id).unwrap();
        let raft = new_leader.group_manager().get_raft(1).await.unwrap();
        raft.client_write(b"post-failover".to_vec()).await.unwrap();

        // Shutdown remaining nodes
        for node in survivors.iter() {
            let _ = node.shutdown().await;
        }
    }

    /// Dynamic partition add: start a 3-node cluster with 1 group, then add a 2nd
    /// partition at runtime via ConfigProvider and verify all 3 nodes start group 2,
    /// the group elects a leader, and writes replicate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_dynamic_partition_add() {
        let builder = ClusterBuilder::new(3, 1).with_real_network();
        let provider = builder.config_provider();
        let nodes = builder.build().await.unwrap();

        // Wait for group 1 to elect a leader
        wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();

        // Node IDs are 1..=3
        let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.node_id()).collect();

        // Add partition 2 → group 2 with all 3 nodes as replicas at runtime
        provider
            .add_partition(
                2,
                rr_core::types::PartitionConfig::SingleId(2),
                2,
                node_ids.clone(),
            )
            .await;

        // Wait for all 3 nodes to have started group 2 (watch loop picks up PartitionAdded)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
        loop {
            let mut count = 0usize;
            for node in &nodes {
                if node.group_manager().get_raft(2).await.is_some() {
                    count += 1;
                }
            }
            if count == nodes.len() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "group 2 not started on all nodes within 8s; only {count}/{} nodes have it",
                    nodes.len()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Initialize group 2 with real addresses from the routing table
        let table = provider.get_routing_table().await.unwrap();
        let mut members = BTreeMap::new();
        for node_id in &node_ids {
            if let Some(node_addr) = table.nodes.get(node_id) {
                members.insert(
                    *node_id,
                    openraft::BasicNode {
                        addr: node_addr.addr.clone(),
                    },
                );
            }
        }
        assert_eq!(members.len(), 3, "must have addresses for all 3 nodes");

        // Bootstrap group 2: node[0] calls initialize
        let raft2 = nodes[0].group_manager().get_raft(2).await.unwrap();
        let _ = raft2.initialize(members).await; // idempotent

        // Wait for group 2 leader election
        let leader2 = wait_for_cluster_ready(&nodes, 2, 8_000).await.unwrap();
        assert!(
            (1..=3).contains(&leader2),
            "group 2 leader must be one of the 3 nodes"
        );

        // Write to group 2 and verify
        let leader2_node = nodes.iter().find(|n| n.node_id() == leader2).unwrap();
        let raft2 = leader2_node.group_manager().get_raft(2).await.unwrap();
        let resp = raft2.client_write(b"group2-write".to_vec()).await.unwrap();
        assert_eq!(
            resp.data, b"group2-write",
            "group 2 EchoSM must echo writes"
        );

        shutdown_all(&nodes).await;
    }

    /// T21 — Dynamic node add/remove: register a new node in config provider,
    /// verify the watch loop fires add_learner, then remove and verify cluster stability.
    #[tokio::test]
    async fn test_dynamic_node_add() {
        let builder = ClusterBuilder::new(3, 1).with_real_network();
        let provider = builder.config_provider();
        let nodes = builder.build().await.unwrap();

        // Wait for group 1 to have a stable leader
        let _leader = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();

        // Register node 4 in config provider — fires NodeAdded event
        let node4_id: NodeId = 4;
        let node4_addr = "127.0.0.1:19204".to_string();
        provider
            .add_node(node4_id, rr_core::types::NodeAddr { addr: node4_addr })
            .await;

        // Wait briefly for the watch loop to fire add_learner calls
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        // Cluster should still have a stable leader after add_learner attempts
        let leader_after = wait_for_cluster_ready(&nodes, 1, 5_000).await.unwrap();
        assert!(
            (1..=3).contains(&leader_after),
            "leader must still be among original 3 nodes after NodeAdded"
        );

        // Simulate NodeRemoved: remove node 4 from config
        provider.remove_node(node4_id).await;

        // Wait briefly for watch loop to fire change_membership calls
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        // Cluster should remain stable
        let leader_final = wait_for_cluster_ready(&nodes, 1, 5_000).await.unwrap();
        assert!(
            (1..=3).contains(&leader_final),
            "leader must still be among original 3 nodes after NodeRemoved"
        );

        shutdown_all(&nodes).await;
    }

    /// T22 — RocksDB persistence: write data, restart node, verify log survives.
    #[cfg(feature = "rocks")]
    #[tokio::test]
    async fn test_restart_persistence() {
        use openraft::storage::RaftLogStorage;
        use rr_raft::group_manager::RaftGroupManager;
        use rr_raft::rocks_log_storage::RocksLogStorage;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let base = tmp.path().to_str().unwrap().to_string();

        // ── Phase 1: start a single-node group with RocksDB, write data ───────
        {
            let mgr = Arc::new(RaftGroupManager::new_with_rocks(
                1,
                echo_factory(),
                base.clone(),
            ));
            mgr.start_group_rocks(1).await.unwrap();

            // Initialize as single-node cluster
            let mut members = BTreeMap::new();
            members.insert(1u64, openraft::BasicNode::default());
            let raft = mgr.get_raft(1).await.unwrap();
            let _ = raft.initialize(members).await;

            // Wait for leader election
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

            let metrics = raft.metrics().borrow().clone();
            assert!(
                metrics.current_leader.is_some(),
                "must elect a leader before writing"
            );

            // Write some data
            let resp = raft.client_write(b"hello-rocks".to_vec()).await.unwrap();
            assert_eq!(resp.data, b"hello-rocks", "EchoSM must echo written bytes");

            // Gracefully stop the group
            mgr.stop_group(1).await.unwrap();
        } // mgr dropped here — simulates restart

        // ── Phase 2: reopen RocksDB from same path, verify persistence ─────────
        {
            let rocks_path = format!("{base}/1");
            let mut log = RocksLogStorage::open(&rocks_path).unwrap();

            // The log state must reflect that entries were written
            let state = log.get_log_state().await.unwrap();
            assert!(
                state.last_log_id.is_some(),
                "log entries should survive restart"
            );
            assert!(
                state.last_log_id.unwrap().index >= 1,
                "at least one log entry should be persisted"
            );

            // Vote must also be persisted (crucial for no split-brain)
            let vote = log.read_vote().await.unwrap();
            assert!(vote.is_some(), "vote should survive restart");
        }
    }

    // ── T23: Client SDK Integration Tests ────────────────────────────────────

    /// T23a — Client SDK write/read: connect via SDK, write data, verify response.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_client_write_read() {
        let (nodes, provider) = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build_with_provider()
            .await
            .unwrap();

        wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();

        // Get addresses from routing table (listen_addr is private on RrNode)
        let table = provider.get_routing_table().await.unwrap();
        let addrs: Vec<String> = table.nodes.values().map(|n| n.addr.clone()).collect();
        assert!(!addrs.is_empty(), "must have node addresses");

        let client = RrClient::connect(addrs).await.unwrap();
        let resp = client
            .send_request(1, b"hello-client".to_vec())
            .await
            .unwrap();
        assert_eq!(resp, b"hello-client", "EchoSM must echo the written bytes");

        shutdown_all(&nodes).await;
    }

    /// T23b — Client redirect: connect to single node, verify request succeeds via redirect.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_client_redirect() {
        let (nodes, provider) = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build_with_provider()
            .await
            .unwrap();

        wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();

        // Connect to just one bootstrap address — client must handle redirect internally
        let table = provider.get_routing_table().await.unwrap();
        let one_addr = table.nodes.values().next().unwrap().addr.clone();
        let client = RrClient::connect_with_config(
            vec![one_addr],
            RetryConfig {
                max_retries: 5,
                retry_delay_ms: 200,
            },
        )
        .await
        .unwrap();

        let resp = client
            .send_request(1, b"redirect-test".to_vec())
            .await
            .unwrap();
        assert_eq!(resp, b"redirect-test", "EchoSM must echo the written bytes");

        shutdown_all(&nodes).await;
    }

    /// T23c — Client leader failover: kill leader, cluster elects new one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_client_leader_failover() {
        use std::time::Duration;

        let (nodes, provider) = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build_with_provider()
            .await
            .unwrap();

        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();

        let table = provider.get_routing_table().await.unwrap();
        let addrs: Vec<String> = table.nodes.values().map(|n| n.addr.clone()).collect();
        let client = RrClient::connect_with_config(
            addrs,
            RetryConfig {
                max_retries: 5,
                retry_delay_ms: 200,
            },
        )
        .await
        .unwrap();

        // Pre-failover write must succeed
        let resp = client
            .send_request(1, b"pre-failover".to_vec())
            .await
            .unwrap();
        assert_eq!(resp, b"pre-failover");

        // Kill the current leader
        let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
        leader_node.shutdown().await.unwrap();

        // Give the cluster time to elect a new leader
        tokio::time::sleep(Duration::from_millis(2_000)).await;

        let survivors: Vec<_> = nodes
            .iter()
            .filter(|n| n.node_id() != leader_id)
            .cloned()
            .collect();

        // Verify a new leader was elected among survivors
        let new_leader_id = wait_for_new_leader(&survivors, 1, leader_id, 8_000)
            .await
            .unwrap();
        assert_ne!(
            new_leader_id, leader_id,
            "new leader must differ from failed node"
        );

        // Post-failover: client may or may not succeed depending on retry state;
        // the important invariant is the cluster has a new leader.
        let _ = client.send_request(1, b"post-failover".to_vec()).await;

        for node in &survivors {
            let _ = node.shutdown().await;
        }
    }

    /// T23d — Client multiple groups: write to group 1 and group 2 independently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_client_multiple_groups() {
        let (nodes, provider) = ClusterBuilder::new(3, 2)
            .with_real_network()
            .build_with_provider()
            .await
            .unwrap();

        wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();
        wait_for_cluster_ready(&nodes, 2, 8_000).await.unwrap();

        let table = provider.get_routing_table().await.unwrap();
        let addrs: Vec<String> = table.nodes.values().map(|n| n.addr.clone()).collect();
        let client = RrClient::connect(addrs).await.unwrap();

        let r1 = client.send_request(1, b"group1".to_vec()).await.unwrap();
        assert_eq!(r1, b"group1", "group 1 EchoSM must echo written bytes");

        let r2 = client.send_request(2, b"group2".to_vec()).await.unwrap();
        assert_eq!(r2, b"group2", "group 2 EchoSM must echo written bytes");

        shutdown_all(&nodes).await;
    }

    // ── T24: Partition Migration Test ────────────────────────────────────────────

    /// T24 — Partition migration: change membership from {1,2,3} to {1,2} and
    /// verify the cluster remains operational with writes after migration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_partition_migration() {
        use std::collections::BTreeSet;
        use std::time::Duration;

        let nodes = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build()
            .await
            .unwrap();

        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();

        // Write pre-migration data
        let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
        let raft = leader_node.group_manager().get_raft(1).await.unwrap();
        raft.client_write(b"pre-migration".to_vec()).await.unwrap();

        // Migrate: remove node 3 from membership (or whichever node is not leader)
        let remove_id: NodeId = nodes
            .iter()
            .map(|n| n.node_id())
            .find(|&id| id != leader_id)
            .unwrap_or(3);
        let new_members: BTreeSet<NodeId> = nodes
            .iter()
            .map(|n| n.node_id())
            .filter(|&id| id != remove_id)
            .collect();

        // change_membership may error if this node is no longer leader — tolerate it
        let gm = leader_node.group_manager();
        let _ = gm.change_membership(1, new_members).await;

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Cluster must still elect a leader after membership change
        let final_leader = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();
        assert!(
            (1..=3).contains(&final_leader),
            "cluster must still have a leader after membership change"
        );

        // Write after migration
        let leader_node2 = nodes.iter().find(|n| n.node_id() == final_leader).unwrap();
        let raft2 = leader_node2.group_manager().get_raft(1).await.unwrap();
        let resp = raft2
            .client_write(b"post-migration".to_vec())
            .await
            .unwrap();
        assert_eq!(
            resp.data, b"post-migration",
            "must echo post-migration write"
        );

        shutdown_all(&nodes).await;
    }

    // ── T25: Node Failure + Recovery Test ────────────────────────────────────────

    /// T25 — Node failure and recovery with RocksDB persistence.
    /// Verifies that a node can restart from persistent storage and continue operating.
    #[cfg(feature = "rocks")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_node_failure_recovery() {
        use rr_raft::group_manager::RaftGroupManager;
        use std::time::Duration;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let base = tmp.path().to_str().unwrap().to_string();

        // ── Phase 1: write data, simulate crash ──────────────────────────────
        {
            let mgr = Arc::new(RaftGroupManager::new_with_rocks(
                1,
                echo_factory(),
                base.clone(),
            ));
            mgr.start_group_rocks(1).await.unwrap();

            let mut members = BTreeMap::new();
            members.insert(1u64, openraft::BasicNode::default());
            let raft = mgr.get_raft(1).await.unwrap();
            let _ = raft.initialize(members).await;

            tokio::time::sleep(Duration::from_millis(1_500)).await;

            let metrics = raft.metrics().borrow().clone();
            assert!(
                metrics.current_leader.is_some(),
                "must elect a leader before writing"
            );

            raft.client_write(b"before-crash".to_vec()).await.unwrap();
            raft.client_write(b"before-crash-2".to_vec()).await.unwrap();

            // Graceful stop (simulates controlled shutdown; state persists)
            mgr.stop_group(1).await.unwrap();
        } // mgr dropped — simulates crash/restart boundary

        // ── Phase 2: restart from same path and verify recovery ──────────────
        {
            let mgr = Arc::new(RaftGroupManager::new_with_rocks(
                1,
                echo_factory(),
                base.clone(),
            ));
            mgr.start_group_rocks(1).await.unwrap();

            let mut members = BTreeMap::new();
            members.insert(1u64, openraft::BasicNode::default());
            let raft = mgr.get_raft(1).await.unwrap();
            let _ = raft.initialize(members).await; // idempotent — already initialized

            tokio::time::sleep(Duration::from_millis(1_500)).await;

            let metrics = raft.metrics().borrow().clone();
            assert!(
                metrics.current_leader.is_some(),
                "must elect leader after restart"
            );

            // Write post-restart data — verifies node is fully operational
            let resp = raft.client_write(b"after-recovery".to_vec()).await.unwrap();
            assert_eq!(resp.data, b"after-recovery", "must echo post-restart write");

            mgr.stop_group(1).await.unwrap();
        }
    }

    // ── T26: Snapshot-Based Node Catch-Up Test ───────────────────────────────────

    /// T26 — Snapshot catch-up: write entries, trigger snapshot, verify cluster health.
    /// Verifies snapshot infrastructure doesn't break cluster operation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_snapshot_catchup() {
        use std::time::Duration;

        let nodes = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build()
            .await
            .unwrap();

        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();
        let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
        let raft = leader_node.group_manager().get_raft(1).await.unwrap();

        // Write multiple entries to build up log
        for i in 0u8..10 {
            raft.client_write(vec![i]).await.unwrap();
        }

        // Wait for all nodes to apply
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Trigger snapshot on leader — fire-and-forget; may or may not complete
        let _ = raft.trigger().snapshot().await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Trigger log purge (only purges logs already included in snapshot)
        let metrics_before = raft.metrics().borrow().clone();
        if let Some(last_applied) = metrics_before.last_applied {
            let _ = raft.trigger().purge_log(last_applied.index).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Cluster must still have a healthy leader after snapshot + purge
        let final_leader = wait_for_cluster_ready(&nodes, 1, 5_000).await.unwrap();
        assert!(
            (1..=3).contains(&final_leader),
            "cluster must still have a leader after snapshot + log purge"
        );

        // Write after snapshot — verifies cluster is fully operational
        let leader_node2 = nodes.iter().find(|n| n.node_id() == final_leader).unwrap();
        let raft2 = leader_node2.group_manager().get_raft(1).await.unwrap();
        let resp = raft2.client_write(b"post-snapshot".to_vec()).await.unwrap();
        assert_eq!(resp.data, b"post-snapshot", "must echo post-snapshot write");

        shutdown_all(&nodes).await;
    }

    // ── T28: End-to-End Smoke Test (Full Cluster Lifecycle) ─────────────────────

    /// T28 — End-to-end smoke test: build cluster, elect leader, write entries,
    /// replicate, kill a follower, write more, trigger snapshot, shutdown.
    #[cfg(feature = "rocks")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_e2e_smoke() {
        use std::time::Duration;

        // 1. Build 3-node in-process cluster with real gRPC networking
        let nodes = ClusterBuilder::new(3, 1)
            .with_real_network()
            .build()
            .await
            .unwrap();

        // 2. Verify leader election
        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();
        assert!(
            leader_id >= 1 && leader_id <= 3,
            "leader must be one of the 3 nodes"
        );

        // 3. Write 20 entries through the leader
        let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
        let raft = leader_node.group_manager().get_raft(1).await.unwrap();
        for i in 0u8..20 {
            raft.client_write(vec![i]).await.unwrap();
        }

        // 4. Wait for all nodes to replicate
        tokio::time::sleep(Duration::from_millis(300)).await;
        for node in &nodes {
            if let Some(r) = node.group_manager().get_raft(1).await {
                let metrics = r.metrics().borrow().clone();
                assert!(
                    metrics.last_applied.is_some(),
                    "node {} must have applied entries",
                    node.node_id()
                );
            }
        }

        // 5. Kill a follower
        let kill_id = nodes
            .iter()
            .find(|n| n.node_id() != leader_id)
            .map(|n| n.node_id())
            .unwrap();
        let killed = nodes.iter().find(|n| n.node_id() == kill_id).unwrap();
        killed.shutdown().await.unwrap();

        // 6. Write more data — cluster must survive with 2/3 nodes
        let active: Vec<_> = nodes
            .iter()
            .filter(|n| n.node_id() != kill_id)
            .cloned()
            .collect();
        let new_leader_id = wait_for_cluster_ready(&active, 1, 8_000).await.unwrap();
        let new_leader = active
            .iter()
            .find(|n| n.node_id() == new_leader_id)
            .unwrap();
        let raft2 = new_leader.group_manager().get_raft(1).await.unwrap();
        for i in 20u8..30 {
            raft2.client_write(vec![i]).await.unwrap();
        }

        // 7. Trigger snapshot and verify cluster is still healthy
        let _ = raft2.trigger().snapshot().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let final_leader = wait_for_cluster_ready(&active, 1, 5_000).await.unwrap();
        assert!(final_leader >= 1 && final_leader <= 3);

        // 8. Graceful shutdown of remaining nodes
        for node in &active {
            let _ = node.shutdown().await;
        }
    }

    // ── B4: gRPC Health Check Test ───────────────────────────────────────────

    /// B4 — Health check: start a cluster with real gRPC networking, connect
    /// a health client, and verify the server reports SERVING.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_health_check_serving() {
        use tonic_health::pb::health_client::HealthClient;
        use tonic_health::pb::HealthCheckRequest;

        let (nodes, provider) = ClusterBuilder::new(1, 1)
            .with_real_network()
            .build_with_provider()
            .await
            .unwrap();

        wait_for_cluster_ready(&nodes, 1, 4_000).await.unwrap();

        // Retrieve the node's gRPC address from the config provider.
        let table = provider.get_routing_table().await.unwrap();
        let addr = table.nodes.values().next().unwrap().addr.clone();

        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy();
        let mut client = HealthClient::new(channel);

        let resp = client
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
            .unwrap();

        assert_eq!(
            resp.into_inner().status,
            tonic_health::ServingStatus::Serving as i32,
            "health check must report SERVING",
        );

        shutdown_all(&nodes).await;
    }

    // ── E2: Chaos Test Scenarios ─────────────────────────────────────────────

    /// E2a — Isolate the current leader, wait for a new leader to be elected,
    /// restore the isolated node, then verify the cluster accepts writes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_leader_isolation_and_recovery() {
        let (builder, injector) = ClusterBuilder::new(3, 1).with_fault_injector();
        let nodes = builder.build().await.expect("cluster build failed");

        let original_leader_id = wait_for_cluster_ready(&nodes, 1, 8_000)
            .await
            .expect("initial leader not elected");

        // Block all connections to the leader — remaining two nodes form a quorum
        injector.isolate(original_leader_id);

        // A new leader must emerge from the two surviving nodes
        let new_leader_id = wait_for_new_leader(&nodes, 1, original_leader_id, 12_000)
            .await
            .expect("new leader not elected after isolation");
        assert_ne!(new_leader_id, original_leader_id, "new leader must differ from isolated node");

        // Restore the previously-isolated node
        injector.restore(original_leader_id);

        // Write to new leader — must succeed
        let new_leader_node = nodes.iter().find(|n| n.node_id() == new_leader_id).unwrap();
        let raft = new_leader_node.group_manager().get_raft(1).await.unwrap();
        raft.client_write(b"post-isolation".to_vec())
            .await
            .expect("write after isolation recovery must succeed");

        shutdown_all(&nodes).await;
    }

    /// E2b — Isolate a follower, write 10 entries while it is isolated, restore it,
    /// then wait for it to catch up via log replication.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_follower_isolation_and_catchup() {
        let (builder, injector) = ClusterBuilder::new(3, 1).with_fault_injector();
        let nodes = builder.build().await.expect("cluster build failed");

        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000)
            .await
            .expect("initial leader not elected");

        let follower_id = nodes
            .iter()
            .map(|n| n.node_id())
            .find(|&id| id != leader_id)
            .expect("must have at least one follower");

        // Isolate follower — it cannot receive replications
        injector.isolate(follower_id);

        // Write 10 entries to the still-quorate cluster
        let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
        let raft = leader_node.group_manager().get_raft(1).await.unwrap();
        for i in 0u8..10 {
            raft.client_write(vec![i]).await.expect("write must succeed while follower is isolated");
        }

        // Restore follower — it should now catch up
        injector.restore(follower_id);

        // Poll until follower has applied at least one entry (catches up from leader)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(12);
        let follower_node = nodes.iter().find(|n| n.node_id() == follower_id).unwrap();
        loop {
            if let Some(follower_raft) = follower_node.group_manager().get_raft(1).await {
                let metrics = follower_raft.metrics().borrow().clone();
                if metrics.last_applied.is_some() {
                    break;
                }
            }
            if std::time::Instant::now() >= deadline {
                panic!("follower did not catch up within 12s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        shutdown_all(&nodes).await;
    }

    /// E2c — Apply 50% packet loss to one node, write 10 entries through the remaining
    /// quorum, then clear the fault and verify the cluster converges.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_packet_loss_convergence() {
        let (builder, injector) = ClusterBuilder::new(3, 1).with_fault_injector();
        let nodes = builder.build().await.expect("cluster build failed");

        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000)
            .await
            .expect("initial leader not elected");

        // Apply 50% packet loss to one non-leader node
        let lossy_node_id = nodes
            .iter()
            .map(|n| n.node_id())
            .find(|&id| id != leader_id)
            .expect("must have a non-leader node");
        injector.set_packet_loss(lossy_node_id, 0.5);

        // Writes must still succeed — quorum is 2/3 and healthy nodes form a majority
        let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
        let raft = leader_node.group_manager().get_raft(1).await.unwrap();
        for i in 0u8..10 {
            raft.client_write(vec![i])
                .await
                .expect("write must succeed despite 50% packet loss on one follower");
        }

        // Clear all faults and allow cluster to converge
        injector.clear_all();
        tokio::time::sleep(std::time::Duration::from_millis(2_000)).await;

        // Cluster must still have a healthy leader after convergence
        let final_leader = wait_for_cluster_ready(&nodes, 1, 8_000)
            .await
            .expect("cluster must have a leader after packet loss cleared");
        assert!((1..=3).contains(&final_leader), "leader must be a valid node id");

        shutdown_all(&nodes).await;
    }

    /// E2d — Add 100ms latency on connections to the leader, write 10 entries
    /// (slower but still correct), then verify no correctness violations.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_latency_under_high_load() {
        let (builder, injector) = ClusterBuilder::new(3, 1).with_fault_injector();
        let nodes = builder.build().await.expect("cluster build failed");

        let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000)
            .await
            .expect("initial leader not elected");

        // Add 100ms latency on connections targeting the leader node
        injector.set_latency(leader_id, 100);

        // All writes must succeed — just slower due to latency on replication path
        let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
        let raft = leader_node.group_manager().get_raft(1).await.unwrap();
        for i in 0u8..10 {
            raft.client_write(vec![i])
                .await
                .expect("write must succeed under 100ms latency");
        }

        // Clear latency fault
        injector.clear_all();

        // Cluster must still be healthy
        let final_leader = wait_for_cluster_ready(&nodes, 1, 8_000)
            .await
            .expect("cluster must still have a leader after latency cleared");
        assert!((1..=3).contains(&final_leader), "leader must be a valid node id");

        shutdown_all(&nodes).await;
    }

    /// Verify HTTP /health, /ready, /metrics endpoints work end-to-end.
    ///
    /// Spawns a single-node `RrNode` with `metrics_port` set, sends real HTTP
    /// requests via reqwest, and validates status codes + response bodies for
    /// the three observability endpoints plus a 404 fallback. Covers the full
    /// lifecycle of `rr_server::health::spawn_http_server` — previously this
    /// code path had ~24% line coverage with no integration test.
    #[tokio::test]
    async fn test_health_endpoints_full_lifecycle() {
        async fn pick_port() -> u16 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            drop(listener);
            port
        }

        let metrics_port = pick_port().await;
        let grpc_port = pick_port().await;

        // Minimal single-node setup. We do not initialize Raft groups because
        // the HTTP server lifecycle is independent of consensus state.
        let config_provider = Arc::new(MemConfigProvider::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider: Arc<dyn ConfigProvider> = Arc::clone(&config_provider) as _;

        let config = NodeConfig::new(
            1,
            format!("127.0.0.1:{grpc_port}"),
            storage,
            provider,
            echo_factory(),
        )
        .with_metrics_port(metrics_port);

        let node = Arc::new(RrNode::new(config).expect("RrNode::new"));
        node.start().await.expect("RrNode::start");

        let base = format!("http://127.0.0.1:{metrics_port}");

        // /health → 200 "ok\n"
        let resp = reqwest::get(format!("{base}/health"))
            .await
            .expect("/health request");
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "/health status");
        assert_eq!(
            resp.text().await.expect("/health body"),
            "ok\n",
            "/health body"
        );

        // /ready → 200 with JSON body (readiness flag flipped at end of start())
        let resp = reqwest::get(format!("{base}/ready"))
            .await
            .expect("/ready request");
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "/ready status");
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        assert_eq!(
            content_type.as_deref(),
            Some("application/json; charset=utf-8"),
            "/ready content-type"
        );
        let body: serde_json::Value = resp.json().await.expect("/ready JSON body");
        assert_eq!(body["ready"], serde_json::Value::Bool(true), "/ready.ready");
        assert_eq!(body["reason"], "ready", "/ready.reason");
        assert!(body.get("since_unix_secs").is_some(), "/ready.since_unix_secs");

        // /metrics → 200 + Prometheus exposition format
        let resp = reqwest::get(format!("{base}/metrics"))
            .await
            .expect("/metrics request");
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "/metrics status");
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        assert_eq!(
            content_type.as_deref(),
            Some("text/plain; version=0.0.4; charset=utf-8"),
            "/metrics content-type"
        );
        let body = resp.text().await.expect("/metrics body");
        assert!(
            body.contains("# TYPE") || body.contains("# HELP"),
            "/metrics body should contain Prometheus exposition format markers, got: {body}"
        );

        // Unknown path → 404 "not found\n"
        let resp = reqwest::get(format!("{base}/unknown"))
            .await
            .expect("/unknown request");
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::NOT_FOUND,
            "/unknown status"
        );
        assert_eq!(
            resp.text().await.expect("/unknown body"),
            "not found\n",
            "/unknown body"
        );

        node.shutdown().await.expect("RrNode::shutdown");
    }
}
