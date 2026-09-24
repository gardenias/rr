//! ConfigProvider trait for pluggable cluster configuration sources.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;

use crate::error::ConfigError;
use crate::types::{GroupId, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable};

/// A change event from the configuration provider.
#[derive(Debug, Clone)]
pub enum RoutingTableChange {
    /// A new partition was added to the routing table.
    PartitionAdded {
        /// ID of the added partition.
        partition_id: PartitionId,
        /// How the partition maps to its Raft group.
        config: PartitionConfig,
        /// Raft group that owns the partition.
        group_id: GroupId,
    },
    /// A partition was removed from the routing table.
    PartitionRemoved {
        /// ID of the removed partition.
        partition_id: PartitionId,
    },

    /// An existing partition's configuration was updated.
    PartitionUpdated {
        /// ID of the updated partition.
        partition_id: PartitionId,
        /// New partition-to-group mapping.
        config: PartitionConfig,
        /// Raft group that now owns the partition.
        group_id: GroupId,
    },
    /// A new node was added.
    NodeAdded {
        /// ID of the added node.
        node_id: NodeId,
        /// Network address of the added node.
        addr: NodeAddr,
    },
    /// A node was removed.
    NodeRemoved {
        /// ID of the removed node.
        node_id: NodeId,
    },
    /// Group membership was changed.
    GroupMembershipChanged {
        /// Raft group whose membership changed.
        group_id: GroupId,
        /// New full replica set of the group.
        replicas: Vec<NodeId>,
    },
}

/// Trait for reading cluster configuration (routing table, node addresses).
///
/// Implementations may read from etcd, a static file, or an in-memory store.
#[async_trait]
pub trait ConfigProvider: Send + Sync {
    /// Get the current full routing table.
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError>;
    /// Get the current routing table as a shared snapshot.
    ///
    /// Semantics are identical to [`get_routing_table`](Self::get_routing_table)
    /// — a point-in-time view of the provider's own table, fetched fresh on
    /// every call (INV-6: the provider stays the single source of truth; no
    /// caller-side cache is introduced). Backends that already hold the table
    /// in memory should override this to return a cheap `Arc` clone instead of
    /// a deep copy; the default falls back to `get_routing_table`.
    async fn routing_table_snapshot(&self) -> Result<Arc<RoutingTable>, ConfigError> {
        Ok(Arc::new(self.get_routing_table().await?))
    }
    /// Subscribe to routing table change events as a stream.
    ///
    /// # Stream semantics (current behavior, contractual as of v0.2.0)
    ///
    /// - **No initial snapshot.** The stream only yields changes that
    ///   happen *after* subscription. Bootstrap by calling
    ///   [`get_routing_table`](Self::get_routing_table) yourself, then
    ///   apply the stream on top.
    /// - **Ordering.** Events are best-effort ordered per key. The etcd
    ///   backend fans three independent watchers (nodes / partitions /
    ///   groups) into one channel, so there is NO global ordering
    ///   guarantee across those domains.
    /// - **Lag behavior differs per backend.** The in-memory backend
    ///   (broadcast channel) DROPS events when a consumer lags — loudly:
    ///   a `warn!` plus the `rr_config_mem_watch_lagged_total` counter.
    ///   The etcd backend (bounded mpsc, 256)
    ///   applies backpressure instead: the watch loop stalls until the
    ///   consumer catches up. A consumer that must never miss a change
    ///   should periodically re-fetch the routing table to reconcile.
    /// - **Disconnect.** The etcd backend reconnects and resumes its
    ///   watches automatically (with reconciliation after compaction);
    ///   other backends may end or stall the stream — consumers should
    ///   treat stream termination as "re-fetch and re-subscribe".
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
        let s = format!("{change:?}");
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

    #[test]
    fn test_routing_table_change_node_added() {
        let addr = NodeAddr {
            addr: "127.0.0.1:9001".to_string(),
        };
        let change = RoutingTableChange::NodeAdded {
            node_id: 42,
            addr: addr.clone(),
        };
        let cloned = change.clone();
        match cloned {
            RoutingTableChange::NodeAdded {
                node_id,
                addr: cloned_addr,
            } => {
                assert_eq!(node_id, 42);
                assert_eq!(cloned_addr.addr, "127.0.0.1:9001");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_routing_table_change_node_removed() {
        let change = RoutingTableChange::NodeRemoved { node_id: 55 };
        let cloned = change.clone();
        match cloned {
            RoutingTableChange::NodeRemoved { node_id } => {
                assert_eq!(node_id, 55);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_routing_table_change_group_membership_changed() {
        let change = RoutingTableChange::GroupMembershipChanged {
            group_id: 100,
            replicas: vec![1, 2, 3],
        };
        let cloned = change.clone();
        match cloned {
            RoutingTableChange::GroupMembershipChanged { group_id, replicas } => {
                assert_eq!(group_id, 100);
                assert_eq!(replicas, vec![1, 2, 3]);
            }
            _ => panic!("wrong variant"),
        }
    }
}
