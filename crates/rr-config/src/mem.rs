//! In-memory `ConfigProvider` implementation for testing and development.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use rr_core::config::{ConfigProvider, RoutingTableChange};
use rr_core::error::ConfigError;
use rr_core::types::{
    GroupId, GroupMeta, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable,
};

const BROADCAST_CAPACITY: usize = 64;

/// In-memory implementation of [`ConfigProvider`] for testing and development.
///
/// Holds the [`RoutingTable`] behind a [`tokio::sync::RwLock`] as an
/// immutable `Arc` snapshot: mutations copy the table, apply the change, and
/// atomically swap the snapshot (copy-on-write), so
/// `routing_table_snapshot()` is a lock + `Arc` clone with no table copy.
/// Change notifications go out over a [`broadcast`] channel.
#[derive(Clone)]
pub struct MemConfigProvider {
    routing_table: Arc<RwLock<Arc<RoutingTable>>>,
    change_tx: broadcast::Sender<RoutingTableChange>,
}

impl MemConfigProvider {
    /// Create a new empty `MemConfigProvider`.
    pub fn new() -> Self {
        let (change_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            routing_table: Arc::new(RwLock::new(Arc::new(RoutingTable::default()))),
            change_tx,
        }
    }

    /// Apply `f` to a copy of the current table, then swap it in as the new
    /// snapshot. Mutations are rare admin operations; the per-mutation table
    /// clone keeps reads allocation-free.
    async fn mutate(&self, f: impl FnOnce(&mut RoutingTable)) {
        let mut guard = self.routing_table.write().await;
        let mut table = (*guard).as_ref().clone();
        f(&mut table);
        *guard = Arc::new(table);
    }

    /// Add a partition to the routing table and notify watchers.
    ///
    /// If the target group already exists with a DIFFERENT replica set
    /// (e.g. an earlier `add_partition` for a sibling partition), the new
    /// replica set overwrites the old one with a warning — silently
    /// keeping the stale set misconfigures the group. This provider is
    /// for dev/test; production providers should reject such conflicts.
    pub async fn add_partition(
        &self,
        partition_id: PartitionId,
        config: PartitionConfig,
        group_id: GroupId,
        replicas: Vec<NodeId>,
    ) {
        self.mutate(|table| {
            table
                .partitions
                .insert(partition_id, (config.clone(), group_id));
            match table.groups.entry(group_id) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let group = entry.get_mut();
                    if group.replicas != replicas {
                        tracing::warn!(
                            group_id,
                            old = ?group.replicas,
                            new = ?replicas,
                            "add_partition overwrote conflicting group replicas"
                        );
                        group.replicas = replicas.clone();
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(GroupMeta {
                        group_id,
                        replicas: replicas.clone(),
                    });
                }
            }
        })
        .await;
        // Ignore send error — no subscribers is OK.
        let _ = self.change_tx.send(RoutingTableChange::PartitionAdded {
            partition_id,
            config,
            group_id,
        });
    }

    /// Remove a partition from the routing table and notify watchers.
    ///
    /// The owning group is garbage-collected once no remaining partition
    /// references it — otherwise every partition move would leak an empty
    /// group entry (dev/test provider semantics: a group's lifetime is
    /// tied to its partitions here).
    pub async fn remove_partition(&self, partition_id: PartitionId) {
        self.mutate(|table| {
            if let Some((_, group_id)) = table.partitions.remove(&partition_id) {
                let still_used = table.partitions.values().any(|(_, gid)| *gid == group_id);
                if !still_used && table.groups.remove(&group_id).is_some() {
                    tracing::debug!(
                        group_id,
                        "garbage-collected group after its last partition was removed"
                    );
                }
            }
        })
        .await;
        let _ = self
            .change_tx
            .send(RoutingTableChange::PartitionRemoved { partition_id });
    }

    /// Add a node address to the routing table and notify watchers.
    pub async fn add_node(&self, node_id: NodeId, addr: NodeAddr) {
        self.mutate(|table| {
            table.nodes.insert(node_id, addr.clone());
        })
        .await;
        let _ = self
            .change_tx
            .send(RoutingTableChange::NodeAdded { node_id, addr });
    }

    /// Remove a node from the routing table and notify watchers.
    ///
    /// The node is also stripped from every group's replica set — a
    /// dangling replica would keep routing replication/traffic at a dead
    /// node. Each strip is logged (loud) since it changes membership.
    pub async fn remove_node(&self, node_id: NodeId) {
        self.mutate(|table| {
            table.nodes.remove(&node_id);
            for group in table.groups.values_mut() {
                if group.replicas.contains(&node_id) {
                    group.replicas.retain(|replica| *replica != node_id);
                    tracing::warn!(
                        node_id,
                        group_id = group.group_id,
                        "remove_node stripped dangling replica from group"
                    );
                }
            }
        })
        .await;
        let _ = self
            .change_tx
            .send(RoutingTableChange::NodeRemoved { node_id });
    }

    /// Update group membership and notify watchers.
    pub async fn update_group_membership(&self, group_id: GroupId, replicas: Vec<NodeId>) {
        self.mutate(|table| {
            if let Some(group) = table.groups.get_mut(&group_id) {
                group.replicas = replicas.clone();
            } else {
                table.groups.insert(
                    group_id,
                    GroupMeta {
                        group_id,
                        replicas: replicas.clone(),
                    },
                );
            }
        })
        .await;
        let _ = self
            .change_tx
            .send(RoutingTableChange::GroupMembershipChanged { group_id, replicas });
    }
}

impl Default for MemConfigProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConfigProvider for MemConfigProvider {
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError> {
        Ok((*self.routing_table.read().await).as_ref().clone())
    }

    async fn routing_table_snapshot(&self) -> Result<Arc<RoutingTable>, ConfigError> {
        Ok(Arc::clone(&*self.routing_table.read().await))
    }

    async fn watch_changes(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError> {
        let rx = self.change_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|r| match r {
            Ok(change) => Some(change),
            // A lagging consumer loses events at the broadcast ring
            // buffer — never silently: warn + metric so operators see the
            // gap, then keep streaming (consumers should re-fetch the
            // table to reconcile).
            Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                tracing::warn!(
                    skipped,
                    "config watcher lagged; routing-table changes dropped — re-fetch the table to reconcile"
                );
                metrics::counter!("rr_config_mem_watch_lagged_total").increment(skipped);
                None
            }
        });
        Ok(Box::pin(stream))
    }

    async fn get_node_addr(&self, node_id: NodeId) -> Result<NodeAddr, ConfigError> {
        let table = self.routing_table.read().await;
        table
            .nodes
            .get(&node_id)
            .cloned()
            .ok_or_else(|| ConfigError::InvalidConfig(format!("node {node_id} not found")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_routing_table_empty() {
        let provider = MemConfigProvider::new();
        let table = provider.get_routing_table().await.unwrap();
        assert!(table.partitions.is_empty());
        assert!(table.nodes.is_empty());
        assert!(table.groups.is_empty());
    }

    #[tokio::test]
    async fn test_add_partition_visible_in_routing_table() {
        let provider = MemConfigProvider::new();
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 100, vec![1, 2, 3])
            .await;
        let table = provider.get_routing_table().await.unwrap();
        assert_eq!(table.partitions.len(), 1);
        let (cfg, gid) = table.partitions.get(&1).unwrap();
        assert_eq!(*gid, 100);
        assert!(matches!(cfg, PartitionConfig::SingleId(1)));
        // Group should have been created
        let group = table.groups.get(&100).unwrap();
        assert_eq!(group.replicas, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_remove_partition() {
        let provider = MemConfigProvider::new();
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 100, vec![1])
            .await;
        provider.remove_partition(1).await;
        let table = provider.get_routing_table().await.unwrap();
        assert!(table.partitions.is_empty());
    }

    #[tokio::test]
    async fn test_get_node_addr_success() {
        let provider = MemConfigProvider::new();
        let addr = NodeAddr {
            addr: "127.0.0.1:50051".to_string(),
        };
        provider.add_node(1, addr.clone()).await;
        let fetched = provider.get_node_addr(1).await.unwrap();
        assert_eq!(fetched.addr, addr.addr);
    }

    #[tokio::test]
    async fn test_get_node_addr_missing_returns_error() {
        let provider = MemConfigProvider::new();
        let result = provider.get_node_addr(999).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_watch_receives_partition_added() {
        let provider = MemConfigProvider::new();
        let mut stream = provider.watch_changes().await.unwrap();

        let p2 = provider.clone();
        tokio::spawn(async move {
            p2.add_partition(42, PartitionConfig::SingleId(42), 10, vec![1])
                .await;
        });

        let change = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        match change {
            RoutingTableChange::PartitionAdded { partition_id, .. } => {
                assert_eq!(partition_id, 42);
            }
            _ => panic!("expected PartitionAdded"),
        }
    }

    #[tokio::test]
    async fn test_watch_receives_partition_removed() {
        let provider = MemConfigProvider::new();
        provider
            .add_partition(5, PartitionConfig::SingleId(5), 10, vec![1])
            .await;
        let mut stream = provider.watch_changes().await.unwrap();

        let p2 = provider.clone();
        tokio::spawn(async move {
            p2.remove_partition(5).await;
        });

        let change = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        match change {
            RoutingTableChange::PartitionRemoved { partition_id } => {
                assert_eq!(partition_id, 5);
            }
            _ => panic!("expected PartitionRemoved"),
        }
    }

    #[tokio::test]
    async fn test_default_impl() {
        let provider = MemConfigProvider::default();
        let table = provider.get_routing_table().await.unwrap();
        assert!(table.partitions.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_partitions() {
        let provider = MemConfigProvider::new();
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

        let table = provider.get_routing_table().await.unwrap();
        assert_eq!(table.partitions.len(), 2);
        assert_eq!(table.groups.len(), 2);
    }

    #[tokio::test]
    async fn test_watch_receives_node_added() {
        let provider = MemConfigProvider::new();
        let mut stream = provider.watch_changes().await.unwrap();

        let p2 = provider.clone();
        tokio::spawn(async move {
            let addr = NodeAddr {
                addr: "127.0.0.1:9001".to_string(),
            };
            p2.add_node(99, addr).await;
        });

        let change = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        match change {
            RoutingTableChange::NodeAdded { node_id, addr } => {
                assert_eq!(node_id, 99);
                assert_eq!(addr.addr, "127.0.0.1:9001");
            }
            _ => panic!("expected NodeAdded"),
        }
    }

    #[tokio::test]
    async fn test_watch_receives_node_removed() {
        let provider = MemConfigProvider::new();
        let addr = NodeAddr {
            addr: "127.0.0.1:9002".to_string(),
        };
        provider.add_node(88, addr).await;
        let mut stream = provider.watch_changes().await.unwrap();

        let p2 = provider.clone();
        tokio::spawn(async move {
            p2.remove_node(88).await;
        });

        let change = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        match change {
            RoutingTableChange::NodeRemoved { node_id } => {
                assert_eq!(node_id, 88);
            }
            _ => panic!("expected NodeRemoved"),
        }
    }

    #[tokio::test]
    async fn test_watch_receives_group_membership_changed() {
        let provider = MemConfigProvider::new();
        provider
            .add_partition(10, PartitionConfig::SingleId(10), 50, vec![1, 2])
            .await;
        let mut stream = provider.watch_changes().await.unwrap();

        let p2 = provider.clone();
        tokio::spawn(async move {
            p2.update_group_membership(50, vec![1, 2, 3]).await;
        });

        let change = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        match change {
            RoutingTableChange::GroupMembershipChanged { group_id, replicas } => {
                assert_eq!(group_id, 50);
                assert_eq!(replicas, vec![1, 2, 3]);
            }
            _ => panic!("expected GroupMembershipChanged"),
        }
    }

    #[tokio::test]
    async fn test_add_node_visible_in_routing_table() {
        let provider = MemConfigProvider::new();
        let addr = NodeAddr {
            addr: "127.0.0.1:9003".to_string(),
        };
        provider.add_node(77, addr.clone()).await;
        let table = provider.get_routing_table().await.unwrap();
        assert_eq!(table.nodes.len(), 1);
        let node = table.nodes.get(&77).unwrap();
        assert_eq!(node.addr, "127.0.0.1:9003");
    }

    #[tokio::test]
    async fn test_remove_node_visible_in_routing_table() {
        let provider = MemConfigProvider::new();
        let addr = NodeAddr {
            addr: "127.0.0.1:9004".to_string(),
        };
        provider.add_node(66, addr).await;
        provider.remove_node(66).await;
        let table = provider.get_routing_table().await.unwrap();
        assert!(table.nodes.is_empty());
    }

    #[tokio::test]
    async fn test_update_group_membership_visible_in_routing_table() {
        let provider = MemConfigProvider::new();
        provider
            .add_partition(11, PartitionConfig::SingleId(11), 51, vec![1])
            .await;
        provider.update_group_membership(51, vec![1, 2, 3, 4]).await;
        let table = provider.get_routing_table().await.unwrap();
        let group = table.groups.get(&51).unwrap();
        assert_eq!(group.replicas, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_remove_partition_garbage_collects_group_when_last_partition_removed() {
        let provider = MemConfigProvider::new();
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 100, vec![1])
            .await;
        provider
            .add_partition(2, PartitionConfig::SingleId(2), 100, vec![1])
            .await;
        provider
            .add_partition(3, PartitionConfig::SingleId(3), 200, vec![2])
            .await;

        // Group 100 still has partition 2 — no GC yet.
        provider.remove_partition(1).await;
        let table = provider.get_routing_table().await.unwrap();
        assert!(table.groups.contains_key(&100));

        // Last partition of group 100 removed — the group must be GC'd;
        // the unrelated group 200 stays.
        provider.remove_partition(2).await;
        let table = provider.get_routing_table().await.unwrap();
        assert!(
            !table.groups.contains_key(&100),
            "group must be GC'd with its last partition"
        );
        assert!(table.groups.contains_key(&200));
    }

    #[tokio::test]
    async fn test_remove_partition_of_unknown_partition_does_not_gc() {
        let provider = MemConfigProvider::new();
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 100, vec![1])
            .await;
        provider.remove_partition(999).await;
        let table = provider.get_routing_table().await.unwrap();
        assert!(table.groups.contains_key(&100));
    }

    #[tokio::test]
    async fn test_add_partition_overwrites_conflicting_group_replicas() {
        let provider = MemConfigProvider::new();
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 100, vec![1, 2])
            .await;
        // Same group, conflicting replica set — the newest call wins.
        provider
            .add_partition(2, PartitionConfig::SingleId(2), 100, vec![3, 4])
            .await;
        let table = provider.get_routing_table().await.unwrap();
        assert_eq!(table.groups.get(&100).unwrap().replicas, vec![3, 4]);

        // Same replicas again — no-op, no churn.
        provider
            .add_partition(3, PartitionConfig::SingleId(3), 100, vec![3, 4])
            .await;
        let table = provider.get_routing_table().await.unwrap();
        assert_eq!(table.groups.get(&100).unwrap().replicas, vec![3, 4]);
    }

    // INV-6: after a routing-table update the old shard receives no new writes; the routing table is the single source of truth.
    #[tokio::test]
    async fn test_remove_node_strips_dangling_replicas() {
        let provider = MemConfigProvider::new();
        provider
            .add_node(
                1,
                NodeAddr {
                    addr: "10.0.0.1:1".to_string(),
                },
            )
            .await;
        provider
            .add_node(
                2,
                NodeAddr {
                    addr: "10.0.0.2:1".to_string(),
                },
            )
            .await;
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![1, 2])
            .await;
        provider
            .add_partition(2, PartitionConfig::SingleId(2), 20, vec![2])
            .await;

        provider.remove_node(1).await;
        let table = provider.get_routing_table().await.unwrap();
        assert!(!table.nodes.contains_key(&1));
        assert_eq!(
            table.groups.get(&10).unwrap().replicas,
            vec![2],
            "removed node must not dangle in group replicas"
        );
        assert_eq!(table.groups.get(&20).unwrap().replicas, vec![2]);
    }

    // INV-6: after a routing-table update the old shard receives no new writes; the routing table is the single source of truth.
    #[tokio::test]
    async fn test_watch_lagged_consumer_recovers_after_dropped_events() {
        let provider = MemConfigProvider::new();
        let mut stream = provider.watch_changes().await.unwrap();

        // Overflow the 64-event broadcast ring without polling.
        for i in 0..(BROADCAST_CAPACITY as u64 + 6) {
            provider
                .add_partition(i, PartitionConfig::SingleId(i), i + 1000, vec![1])
                .await;
        }

        // The stream must survive the lag and resume with the oldest
        // retained event (the earliest ones were dropped — loudly, via
        // warn + rr_config_mem_watch_lagged_total).
        let change = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .expect("timeout")
            .expect("stream must survive lag");
        match change {
            RoutingTableChange::PartitionAdded { partition_id, .. } => {
                assert!(
                    partition_id > 0,
                    "lag must have dropped the earliest events, got partition_id {partition_id}"
                );
            }
            other => panic!("expected PartitionAdded, got {other:?}"),
        }

        // And it keeps streaming after the gap.
        let next = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .expect("timeout")
            .expect("stream must keep yielding after lag");
        assert!(matches!(next, RoutingTableChange::PartitionAdded { .. }));
    }
}
