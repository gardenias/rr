//! ShardRouter — routes partition IDs to Raft group IDs via table lookup.
//!
//! Design: the router does table lookup ONLY.
//! NO consistent hashing, NO hash rings, NO smart routing.

use std::sync::Arc;

use metrics::counter;
use rr_core::config::ConfigProvider;
use rr_core::error::{RouterError, RrError};
use rr_core::types::{GroupId, NodeAddr, NodeId, PartitionId, RoutingTable};
use tracing::instrument;

/// Routes a partition ID to a Raft group ID using a [`ConfigProvider`]-backed routing table.
///
/// The router performs a simple table lookup against the current routing table.
/// It has no awareness of routing algorithms — the routing table is the single
/// source of truth.
pub struct ShardRouter {
    config: Arc<dyn ConfigProvider>,
}

impl ShardRouter {
    /// Create a new `ShardRouter` backed by the given [`ConfigProvider`].
    pub fn new(config: Arc<dyn ConfigProvider>) -> Self {
        Self { config }
    }

    /// Route a `partition_id` to its Raft [`GroupId`].
    ///
    /// Returns [`RouterError::PartitionNotFound`] if the partition is not in
    /// the routing table.
    #[instrument(skip(self))]
    pub async fn route(&self, partition_id: PartitionId) -> Result<GroupId, RrError> {
        let table = self.config.get_routing_table().await?;

        let result = table
            .partitions
            .get(&partition_id)
            .map(|(_, group_id)| *group_id)
            .ok_or(RrError::Router(RouterError::PartitionNotFound {
                partition_id,
            }));
        if result.is_err() {
            counter!("router.route.not_found").increment(1);
        }
        result
    }
    /// Get the current leader node for a group.
    ///
    /// Returns [`RouterError::GroupNotFound`] if the group is not in the
    /// routing table.
    /// Returns [`RouterError::NoLeader`] if the group has no leader.
    #[instrument(skip(self))]
    pub async fn get_leader(&self, group_id: GroupId) -> Result<NodeId, RrError> {
        let table = self.config.get_routing_table().await?;

        let meta = table
            .groups
            .get(&group_id)
            .ok_or(RrError::Router(RouterError::GroupNotFound { group_id }))?;

        meta.leader
            .ok_or(RrError::Router(RouterError::NoLeader { group_id }))
    }

    /// Get the address of the leader node for a partition.
    ///
    /// Combines [`route()`](Self::route) + [`get_leader()`](Self::get_leader) +
    /// node address lookup.
    #[instrument(skip(self))]
    pub async fn get_leader_addr(&self, partition_id: PartitionId) -> Result<NodeAddr, RrError> {
        let group_id = self.route(partition_id).await?;
        let leader_id = self.get_leader(group_id).await?;

        let addr = self.config.get_node_addr(leader_id).await?;
        counter!("router.get_leader_addr.success").increment(1);
        Ok(addr)
    }

    #[instrument(skip(self))]
    /// Get the full routing table.
    pub async fn routing_table(&self) -> Result<RoutingTable, RrError> {
        self.config
            .get_routing_table()
            .await
            .map_err(RrError::Config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rr_config::mem::MemConfigProvider;
    use rr_core::error::RouterError;
    use rr_core::types::{NodeAddr, PartitionConfig};
    use std::sync::Arc;

    fn make_provider() -> MemConfigProvider {
        MemConfigProvider::new()
    }

    fn make_router(provider: &MemConfigProvider) -> ShardRouter {
        // MemConfigProvider is Clone, so we can clone into an Arc
        ShardRouter::new(Arc::new(provider.clone()))
    }

    // ── Test 1: partition not found ───────────────────────────────────
    #[tokio::test]
    async fn test_route_partition_not_found() {
        let provider = make_provider();
        let router = make_router(&provider);

        let result = router.route(999).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            RrError::Router(RouterError::PartitionNotFound { partition_id }) => {
                assert_eq!(partition_id, 999);
            }
            other => panic!("expected PartitionNotFound, got: {other}"),
        }
    }

    // ── Test 2: partition found ──────────────────────────────────────
    #[tokio::test]
    async fn test_route_partition_found() {
        let provider = make_provider();
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![1])
            .await;
        let router = make_router(&provider);

        let group_id = router.route(1).await.unwrap();
        assert_eq!(group_id, 10);
    }

    // ── Test 3: group not found ──────────────────────────────────────
    #[tokio::test]
    async fn test_get_leader_group_not_found() {
        let provider = make_provider();
        let router = make_router(&provider);

        let result = router.get_leader(999).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            RrError::Router(RouterError::GroupNotFound { group_id }) => {
                assert_eq!(group_id, 999);
            }
            other => panic!("expected GroupNotFound, got: {other}"),
        }
    }

    // ── Test 4: get leader success ───────────────────────────────────
    #[tokio::test]
    async fn test_get_leader_success() {
        let provider = make_provider();
        provider
            .add_partition(2, PartitionConfig::SingleId(2), 20, vec![1, 2, 3])
            .await;
        let router = make_router(&provider);

        // MemConfigProvider sets first replica as leader
        let leader = router.get_leader(20).await.unwrap();
        assert_eq!(leader, 1);
    }

    // ── Test 5: get leader addr success ──────────────────────────────
    #[tokio::test]
    async fn test_get_leader_addr_success() {
        let provider = make_provider();
        provider
            .add_partition(3, PartitionConfig::SingleId(3), 30, vec![5])
            .await;
        provider
            .add_node(
                5,
                NodeAddr {
                    addr: "127.0.0.1:50051".to_string(),
                },
            )
            .await;
        let router = make_router(&provider);

        let addr = router.get_leader_addr(3).await.unwrap();
        assert_eq!(addr.addr, "127.0.0.1:50051");
    }

    // ── Test 6: routing table returns full table ─────────────────────
    #[tokio::test]
    async fn test_routing_table_returns_full_table() {
        let provider = make_provider();
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 100, vec![1, 2])
            .await;
        provider
            .add_partition(
                2,
                PartitionConfig::Range { start: 10, end: 20 },
                200,
                vec![3, 4],
            )
            .await;
        provider
            .add_node(
                1,
                NodeAddr {
                    addr: "10.0.0.1:50051".to_string(),
                },
            )
            .await;
        let router = make_router(&provider);

        let table = router.routing_table().await.unwrap();
        assert_eq!(table.partitions.len(), 2);
        assert_eq!(table.groups.len(), 2);
        assert_eq!(table.nodes.len(), 1);
        assert!(table.partitions.contains_key(&1));
        assert!(table.partitions.contains_key(&2));
    }

    // ── Test 7: get leader addr partition not found ──────────────────
    #[tokio::test]
    async fn test_get_leader_addr_partition_not_found() {
        let provider = make_provider();
        let router = make_router(&provider);

        let result = router.get_leader_addr(999).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RrError::Router(RouterError::PartitionNotFound { partition_id }) => {
                assert_eq!(partition_id, 999);
            }
            other => panic!("expected PartitionNotFound, got: {other}"),
        }
    }

    // ── Test 8: get leader addr node not found ───────────────────────
    #[tokio::test]
    async fn test_get_leader_addr_node_not_found() {
        let provider = make_provider();
        provider
            .add_partition(4, PartitionConfig::SingleId(4), 40, vec![99])
            .await;
        // Don't add node 99 address
        let router = make_router(&provider);

        let result = router.get_leader_addr(4).await;
        assert!(result.is_err());
        // Should be a Config error from get_node_addr
        match result.unwrap_err() {
            RrError::Config(_) => {} // expected
            other => panic!("expected Config error, got: {other}"),
        }
    }
}
