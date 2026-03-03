//! Core types for the rr distributed framework.

use std::collections::HashMap;

pub type NodeId = u64;
pub type GroupId = u64;
pub type PartitionId = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeAddr {
    pub addr: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PartitionConfig {
    SingleId(u64),
    Range { start: u64, end: u64 },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GroupMeta {
    pub group_id: GroupId,
    pub leader: Option<NodeId>,
    pub replicas: Vec<NodeId>,
}

/// Maps PartitionId → (PartitionConfig, GroupId) and NodeId → NodeAddr
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RoutingTable {
    /// partition_id → (config, group_id)
    pub partitions: HashMap<PartitionId, (PartitionConfig, GroupId)>,
    /// node_id → addr
    pub nodes: HashMap<NodeId, NodeAddr>,
    /// group_id → group metadata
    pub groups: HashMap<GroupId, GroupMeta>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_config_single_id() {
        let config = PartitionConfig::SingleId(42);
        // test Debug, Clone
        let cloned = config.clone();
        assert_eq!(config, cloned);
    }

    #[test]
    fn test_partition_config_range() {
        let config = PartitionConfig::Range { start: 1, end: 100 };
        let cloned = config.clone();
        assert_eq!(config, cloned);
    }

    #[test]
    fn test_routing_table_construction() {
        let mut table = RoutingTable::default();
        table
            .partitions
            .insert(1, (PartitionConfig::SingleId(1), 1));
        table.nodes.insert(
            1,
            NodeAddr {
                addr: "127.0.0.1:50051".to_string(),
            },
        );
        assert_eq!(table.partitions.len(), 1);
        assert_eq!(table.nodes.len(), 1);
    }

    #[test]
    fn test_group_meta() {
        let meta = GroupMeta {
            group_id: 1,
            leader: Some(1),
            replicas: vec![1, 2, 3],
        };
        assert_eq!(meta.replicas.len(), 3);
    }
}
