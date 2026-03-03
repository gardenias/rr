//! Integration test harness for the `rr` distributed framework.
//!
//! Provides an in-process multi-node cluster builder and helpers for
//! exercising Raft leader election, fault tolerance, and multi-group routing.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use openraft::BasicNode;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::state_machine::StateMachine;
use rr_core::storage::StorageEngine;
use rr_core::types::{GroupId, LogIndex, NodeId, PartitionConfig};
use rr_raft::group_manager::StateMachineFactory;
use rr_server::config::NodeConfig;
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
}

impl ClusterBuilder {
    /// Create a builder for a cluster with `node_count` nodes and `group_count` groups.
    pub fn new(node_count: usize, group_count: usize) -> Self {
        Self { node_count, group_count }
    }

    /// Build and start all nodes.
    ///
    /// Returns the list of [`RrNode`]s. Each node has been started; Raft groups
    /// have been initialized with the full member set.
    pub async fn build(self) -> Result<Vec<Arc<RrNode>>> {
        // ── 1. Build shared config provider ───────────────────────────────────
        //
        // Partitions and groups are added before nodes start so that each node
        // can load the routing table and start the correct groups on startup.
        let config_provider = Arc::new(MemConfigProvider::new());

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
pub async fn wait_for_leader(
    node: &RrNode,
    group_id: GroupId,
    timeout_ms: u64,
) -> Result<NodeId> {
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
}
