//! Core types for the rr distributed framework.

use std::collections::HashMap;

/// Unique identifier of a node in the cluster.
pub type NodeId = u64;
/// Unique identifier of a Raft group.
pub type GroupId = u64;
/// Unique identifier of a data partition.
pub type PartitionId = u64;
/// Index of an entry in a Raft log.
pub type LogIndex = u64;

/// The Raft apply context of a single committed log entry (R-1 / A-0033).
///
/// Carries the entry's true Raft identity — its log index and the leader term
/// that proposed it — from OpenRaft down to the user's `Service` /
/// `StateMachine`. Before R-1 the framework discarded `entry.log_id` at the
/// `sm_adapter` boundary, so business logic could not know the real log index
/// of the entry it was applying (it relied on a local monotonically-increasing
/// counter as a proxy, which is not a valid fencing basis and does not survive
/// restore).
///
/// Uses (A-0033): intent-record keys upgraded to the real log index (retiring
/// `outbox_seq` proxies), and as the fencing basis for leader-only side-effect
/// drivers.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct ApplyContext {
    /// The Raft log index of the applied entry.
    pub log_index: LogIndex,
    /// The leader term that proposed the applied entry.
    pub log_term: u64,
}

impl ApplyContext {
    /// Construct an `ApplyContext` from a log index and term.
    pub fn new(log_index: LogIndex, log_term: u64) -> Self {
        Self {
            log_index,
            log_term,
        }
    }
}

/// Network address of a cluster node.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeAddr {
    /// gRPC address of the node (e.g. `"127.0.0.1:9001"`).
    pub addr: String,
}

/// How a partition is identified when mapping it to a Raft group.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PartitionConfig {
    /// The partition is a single fixed ID.
    SingleId(u64),
    /// RESERVED — not implemented. The shard router only ever routes by
    /// exact partition-id lookup (`SingleId`); a `Range` partition stored
    /// in the config plane is rejected loudly at the provider boundary
    /// instead of being silently mis-routed. Do not write this variant.
    Range {
        /// First ID in the range.
        start: u64,
        /// Last ID in the range.
        end: u64,
    },
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
    /// ID of the Raft group.
    pub group_id: GroupId,
    /// Nodes that replicate this group.
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
