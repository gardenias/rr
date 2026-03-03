//! In-memory `ConfigProvider` implementation for testing and development.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use rr_core::config::{ConfigProvider, RoutingTableChange};
use rr_core::error::ConfigError;
use rr_core::types::{GroupId, GroupMeta, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable};

const BROADCAST_CAPACITY: usize = 64;

/// In-memory implementation of [`ConfigProvider`] for testing and development.
///
/// Uses a [`tokio::sync::RwLock`]-protected [`RoutingTable`] and a
/// [`broadcast`] channel to notify watchers of changes.
#[derive(Clone)]
pub struct MemConfigProvider {
    routing_table: Arc<RwLock<RoutingTable>>,
    change_tx: broadcast::Sender<RoutingTableChange>,
}

impl MemConfigProvider {
    /// Create a new empty `MemConfigProvider`.
    pub fn new() -> Self {
        let (change_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            routing_table: Arc::new(RwLock::new(RoutingTable::default())),
            change_tx,
        }
    }

    /// Add a partition to the routing table and notify watchers.
    pub async fn add_partition(
        &self,
        partition_id: PartitionId,
        config: PartitionConfig,
        group_id: GroupId,
        replicas: Vec<NodeId>,
    ) {
        let mut table = self.routing_table.write().await;
        table.partitions.insert(partition_id, (config.clone(), group_id));
        table.groups.entry(group_id).or_insert_with(|| GroupMeta {
            group_id,
            leader: replicas.first().copied(),
            replicas: replicas.clone(),
        });
        // Ignore send error — no subscribers is OK.
        let _ = self.change_tx.send(RoutingTableChange::PartitionAdded {
            partition_id,
            config,
            group_id,
        });
    }

    /// Remove a partition from the routing table and notify watchers.
    pub async fn remove_partition(&self, partition_id: PartitionId) {
        let mut table = self.routing_table.write().await;
        table.partitions.remove(&partition_id);
        let _ = self.change_tx.send(RoutingTableChange::PartitionRemoved { partition_id });
    }

    /// Add a node address to the routing table.
    pub async fn add_node(&self, node_id: NodeId, addr: NodeAddr) {
        let mut table = self.routing_table.write().await;
        table.nodes.insert(node_id, addr);
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
        Ok(self.routing_table.read().await.clone())
    }

    async fn watch_changes(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError> {
        let rx = self.change_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|r| r.ok());
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
        assert_eq!(group.leader, Some(1));
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
            .add_partition(2, PartitionConfig::Range { start: 10, end: 20 }, 200, vec![3, 4])
            .await;

        let table = provider.get_routing_table().await.unwrap();
        assert_eq!(table.partitions.len(), 2);
        assert_eq!(table.groups.len(), 2);
    }
}
