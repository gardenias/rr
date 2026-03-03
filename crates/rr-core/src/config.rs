//! ConfigProvider trait for pluggable cluster configuration sources.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::error::ConfigError;
use crate::types::{GroupId, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable};

/// A change event from the configuration provider.
#[derive(Debug, Clone)]
pub enum RoutingTableChange {
    /// A new partition was added to the routing table.
    PartitionAdded {
        partition_id: PartitionId,
        config: PartitionConfig,
        group_id: GroupId,
    },
    /// A partition was removed from the routing table.
    PartitionRemoved { partition_id: PartitionId },
    /// An existing partition's configuration was updated.
    PartitionUpdated {
        partition_id: PartitionId,
        config: PartitionConfig,
        group_id: GroupId,
    },
}

/// Trait for reading cluster configuration (routing table, node addresses).
///
/// Implementations may read from etcd, a static file, or an in-memory store.
#[async_trait]
pub trait ConfigProvider: Send + Sync {
    /// Get the current full routing table.
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError>;
    /// Subscribe to routing table change events as a stream.
    async fn watch_changes(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError>;
    /// Get the gRPC address for a specific node.
    async fn get_node_addr(&self, node_id: NodeId) -> Result<NodeAddr, ConfigError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routing_table_change_debug() {
        let change = RoutingTableChange::PartitionAdded {
            partition_id: 1,
            config: PartitionConfig::SingleId(1),
            group_id: 42,
        };
        let s = format!("{:?}", change);
        assert!(s.contains("PartitionAdded"));
    }

    #[test]
    fn test_routing_table_change_removed() {
        let change = RoutingTableChange::PartitionRemoved { partition_id: 5 };
        let cloned = change.clone();
        match cloned {
            RoutingTableChange::PartitionRemoved { partition_id } => {
                assert_eq!(partition_id, 5);
            }
            _ => panic!("wrong variant"),
        }
    }

    // Verify ConfigProvider is object-safe
    #[test]
    fn test_config_provider_is_object_safe() {
        let _: Option<Box<dyn ConfigProvider>> = None;
    }
}
