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

/// Configuration-plane metadata for a Raft group.
///
/// This is **static placement metadata only** — which nodes replicate the
/// group. It deliberately carries no raft runtime state: the current leader
/// is known only to the raft runtime (openraft metrics) and is discovered by
/// clients via `NotLeader` / `LeaderInfo` redirects, never via the routing
/// table (see ADR A-0013).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GroupMeta {
    pub group_id: GroupId,
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
            replicas: vec![1, 2, 3],
        };
        assert_eq!(meta.group_id, 1);
        assert_eq!(meta.replicas.len(), 3);
    }

    #[test]
    fn test_group_meta_json_shape_has_no_leader_key() {
        // ADR A-0013: `GroupMeta` is configuration-plane metadata — the JSON
        // shape must stay free of any runtime leader state.
        let meta = GroupMeta {
            group_id: 7,
            replicas: vec![1, 2, 3],
        };
        let wire = serde_json::to_string(&meta).unwrap();
        assert_eq!(wire, r#"{"group_id":7,"replicas":[1,2,3]}"#);
    }

    #[test]
    fn test_group_meta_deserialize_ignores_legacy_leader_key() {
        // ADR A-0013: payloads serialized before the field removal may still
        // carry a `"leader"` key; serde must silently ignore it (the struct
        // has no `deny_unknown_fields`).
        let legacy = r#"{"group_id":7,"leader":1,"replicas":[1,2,3]}"#;
        let meta: GroupMeta = serde_json::from_str(legacy).unwrap();
        assert_eq!(
            meta,
            GroupMeta {
                group_id: 7,
                replicas: vec![1, 2, 3]
            }
        );
    }
}
