use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::types::{
    GroupId, GroupMeta, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable,
};

pub use crate::error::LifecycleError;

const SEED_SPEC_VERSION: u32 = 1;
const LIFECYCLE_VERSION: u32 = 1;
const SEED_HASH_DOMAIN: &[u8] = b"rr/seed-spec/v1\0";

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SeedSpec {
    pub version: u32,
    pub cluster_name: String,
    pub nodes: BTreeMap<NodeId, NodeAddr>,
    pub groups: BTreeMap<GroupId, SeedGroup>,
    pub partitions: BTreeMap<PartitionId, SeedPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SeedGroup {
    pub voters: BTreeSet<NodeId>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SeedPartition {
    pub config: PartitionConfig,
    pub group_id: GroupId,
}

impl SeedSpec {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.version != SEED_SPEC_VERSION {
            return Err(LifecycleError::UnsupportedSchema {
                schema: "SeedSpec",
                version: self.version,
            });
        }
        if self.cluster_name.trim().is_empty() {
            return Err(invalid_seed("cluster_name must not be empty"));
        }
        if self.nodes.is_empty() || self.groups.is_empty() || self.partitions.is_empty() {
            return Err(invalid_seed(
                "nodes, groups, and partitions must not be empty",
            ));
        }

        for (node_id, node) in &self.nodes {
            if *node_id == 0 {
                return Err(invalid_seed("node IDs must be non-zero"));
            }
            validate_advertised_addr(&node.addr)?;
        }

        let expected_voters: BTreeSet<NodeId> = self.nodes.keys().copied().collect();
        for (group_id, group) in &self.groups {
            if *group_id == 0 {
                return Err(invalid_seed("group IDs must be non-zero"));
            }
            if group.voters != expected_voters {
                return Err(invalid_seed(format!(
                    "group {group_id} voters must equal the uniform node roster"
                )));
            }
        }

        for (partition_id, partition) in &self.partitions {
            if *partition_id == 0 {
                return Err(invalid_seed("partition IDs must be non-zero"));
            }
            if partition.group_id == 0 || !self.groups.contains_key(&partition.group_id) {
                return Err(invalid_seed(format!(
                    "partition {partition_id} references unknown group {}",
                    partition.group_id
                )));
            }
            match partition.config {
                PartitionConfig::SingleId(config_id) if config_id == *partition_id => {}
                PartitionConfig::SingleId(config_id) => {
                    return Err(invalid_seed(format!(
                        "partition {partition_id} has mismatched SingleId {config_id}"
                    )));
                }
                PartitionConfig::Range { .. } => {
                    return Err(invalid_seed(format!(
                        "partition {partition_id} uses unsupported Range config"
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn seed_hash(&self) -> Result<[u8; 32], LifecycleError> {
        self.validate()?;
        let encoded = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|error| LifecycleError::CorruptData(error.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(SEED_HASH_DOMAIN);
        hasher.update(encoded);
        Ok(hasher.finalize().into())
    }

    pub fn initial_routing_table(&self) -> Result<RoutingTable, LifecycleError> {
        self.validate()?;
        Ok(RoutingTable {
            partitions: self
                .partitions
                .iter()
                .map(|(partition_id, partition)| {
                    (
                        *partition_id,
                        (partition.config.clone(), partition.group_id),
                    )
                })
                .collect(),
            nodes: self
                .nodes
                .iter()
                .map(|(node_id, addr)| (*node_id, addr.clone()))
                .collect(),
            groups: self
                .groups
                .iter()
                .map(|(group_id, group)| {
                    (
                        *group_id,
                        GroupMeta {
                            group_id: *group_id,
                            replicas: group.voters.iter().copied().collect(),
                        },
                    )
                })
                .collect(),
        })
    }

    pub fn stock(
        cluster_name: impl Into<String>,
        nodes: BTreeMap<NodeId, NodeAddr>,
        group_count: u64,
    ) -> Result<Self, LifecycleError> {
        let voters: BTreeSet<NodeId> = nodes.keys().copied().collect();
        let mut groups = BTreeMap::new();
        let mut partitions = BTreeMap::new();
        for group_id in 1..=group_count {
            groups.insert(
                group_id,
                SeedGroup {
                    voters: voters.clone(),
                },
            );
            partitions.insert(
                group_id,
                SeedPartition {
                    config: PartitionConfig::SingleId(group_id),
                    group_id,
                },
            );
        }
        let spec = Self {
            version: SEED_SPEC_VERSION,
            cluster_name: cluster_name.into(),
            nodes,
            groups,
            partitions,
        };
        spec.validate()?;
        Ok(spec)
    }
}

fn invalid_seed(message: impl Into<String>) -> LifecycleError {
    LifecycleError::InvalidSeed(message.into())
}

fn validate_advertised_addr(addr: &str) -> Result<(), LifecycleError> {
    let valid = !addr.is_empty()
        && addr.trim() == addr
        && !addr.chars().any(char::is_whitespace)
        && addr.rsplit_once(':').is_some_and(|(host, port)| {
            valid_advertised_host(host) && port.parse::<u16>().is_ok_and(|port| port != 0)
        });
    if valid {
        Ok(())
    } else {
        Err(invalid_seed(format!("invalid advertised address {addr:?}")))
    }
}

fn valid_advertised_host(host: &str) -> bool {
    if let Some(host) = host.strip_prefix('[') {
        return host
            .strip_suffix(']')
            .is_some_and(|host| host.parse::<std::net::Ipv6Addr>().is_ok());
    }
    !host.is_empty()
        && !host
            .chars()
            .any(|character| matches!(character, ':' | '[' | ']' | '/'))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClusterLifecycle {
    pub version: u32,
    pub cluster_uuid: Uuid,
    pub topology_revision: u64,
    pub seed_hash: [u8; 32],
    pub origin: ClusterOrigin,
    pub groups: BTreeMap<GroupId, GroupFormation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GroupFormation {
    pub bootstrap_epoch: u64,
    pub state: FormationState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FormationState {
    Forming,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClusterOrigin {
    Genesis,
    LegacyAdopted,
}

impl ClusterLifecycle {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.version != LIFECYCLE_VERSION {
            return Err(LifecycleError::UnsupportedSchema {
                schema: "ClusterLifecycle",
                version: self.version,
            });
        }
        if self.cluster_uuid.is_nil() {
            return Err(LifecycleError::CorruptData(
                "cluster_uuid must not be nil".to_string(),
            ));
        }
        if self.groups.is_empty() {
            return Err(LifecycleError::CorruptData(
                "groups must not be empty".to_string(),
            ));
        }
        for (group_id, formation) in &self.groups {
            if *group_id == 0 || formation.bootstrap_epoch == 0 {
                return Err(LifecycleError::CorruptData(format!(
                    "group {group_id} has a zero group ID or bootstrap epoch"
                )));
            }
        }
        Ok(())
    }

    pub fn validate_transition(&self, next: &Self) -> Result<(), LifecycleError> {
        self.validate()?;
        next.validate()?;
        if self.cluster_uuid != next.cluster_uuid {
            return Err(LifecycleError::IdentityMismatch);
        }
        if self.seed_hash != next.seed_hash {
            return Err(LifecycleError::SeedMismatch);
        }
        if self.origin != next.origin {
            return Err(invalid_transition("cluster origin cannot change"));
        }
        if next.topology_revision < self.topology_revision {
            return Err(invalid_transition("topology revision cannot decrease"));
        }
        if !self.groups.keys().eq(next.groups.keys()) {
            return Err(invalid_transition("group key set cannot change"));
        }
        for (group_id, current) in &self.groups {
            let next_formation = next
                .groups
                .get(group_id)
                .ok_or_else(|| invalid_transition(format!("group {group_id} cannot be removed")))?;
            current.validate_transition(next_formation)?;
        }
        Ok(())
    }
}

impl GroupFormation {
    pub fn validate_transition(&self, next: &Self) -> Result<(), LifecycleError> {
        if self.bootstrap_epoch != next.bootstrap_epoch {
            return Err(invalid_transition("bootstrap epoch cannot change"));
        }
        if self.state == FormationState::Active && next.state == FormationState::Forming {
            return Err(invalid_transition("Active group cannot return to Forming"));
        }
        Ok(())
    }
}

fn invalid_transition(message: impl Into<String>) -> LifecycleError {
    LifecycleError::InvalidTransition(message.into())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionedLifecycle {
    pub revision: u64,
    pub record: ClusterLifecycle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasOutcome {
    Applied,
    Conflict,
}

#[async_trait::async_trait]
pub trait LifecycleStore: Send + Sync {
    async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError>;

    async fn create_if_absent(
        &self,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError>;

    async fn compare_and_swap(
        &self,
        expected_revision: u64,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError>;
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use uuid::Uuid;

    use super::{
        CasOutcome, ClusterLifecycle, ClusterOrigin, FormationState, GroupFormation,
        LifecycleError, LifecycleStore, SeedGroup, SeedPartition, SeedSpec, VersionedLifecycle,
    };
    use crate::{NodeAddr, NodeId, PartitionConfig, RrError};

    fn nodes(ids: &[NodeId]) -> BTreeMap<NodeId, NodeAddr> {
        ids.iter()
            .map(|id| {
                (
                    *id,
                    NodeAddr {
                        addr: format!("node-{id}:9001"),
                    },
                )
            })
            .collect()
    }

    fn voters(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    fn valid_spec() -> SeedSpec {
        SeedSpec {
            version: 1,
            cluster_name: "test-cluster".to_string(),
            nodes: nodes(&[1, 2, 3]),
            groups: BTreeMap::from([
                (
                    1,
                    SeedGroup {
                        voters: voters(&[1, 2, 3]),
                    },
                ),
                (
                    2,
                    SeedGroup {
                        voters: voters(&[1, 2, 3]),
                    },
                ),
            ]),
            partitions: BTreeMap::from([
                (
                    1,
                    SeedPartition {
                        config: PartitionConfig::SingleId(1),
                        group_id: 1,
                    },
                ),
                (
                    2,
                    SeedPartition {
                        config: PartitionConfig::SingleId(2),
                        group_id: 2,
                    },
                ),
            ]),
        }
    }

    fn golden_spec() -> SeedSpec {
        SeedSpec {
            version: 1,
            cluster_name: "golden".to_string(),
            nodes: BTreeMap::from([(
                1,
                NodeAddr {
                    addr: "node-1:9001".to_string(),
                },
            )]),
            groups: BTreeMap::from([(
                1,
                SeedGroup {
                    voters: voters(&[1]),
                },
            )]),
            partitions: BTreeMap::from([(
                1,
                SeedPartition {
                    config: PartitionConfig::SingleId(1),
                    group_id: 1,
                },
            )]),
        }
    }

    fn lifecycle() -> ClusterLifecycle {
        ClusterLifecycle {
            version: 1,
            cluster_uuid: Uuid::from_u128(0x1234),
            topology_revision: 7,
            seed_hash: [9; 32],
            origin: ClusterOrigin::Genesis,
            groups: BTreeMap::from([
                (
                    1,
                    GroupFormation {
                        bootstrap_epoch: 1,
                        state: FormationState::Forming,
                    },
                ),
                (
                    2,
                    GroupFormation {
                        bootstrap_epoch: 4,
                        state: FormationState::Active,
                    },
                ),
            ]),
        }
    }

    fn hex(bytes: &[u8; 32]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn lifecycle_seed_hash_matches_fixed_v1_golden_vector() {
        assert_eq!(
            hex(&golden_spec().seed_hash().unwrap()),
            "2c21154784b7b05a94dd1292d791cd6ff64c4e203a8fce1cc0a95e3003f0a826"
        );
    }

    #[test]
    fn lifecycle_seed_hash_is_independent_of_map_insertion_order() {
        let forward = valid_spec();
        let mut reverse = valid_spec();
        reverse.nodes = forward
            .nodes
            .iter()
            .rev()
            .map(|(id, addr)| (*id, addr.clone()))
            .collect();
        reverse.groups = forward
            .groups
            .iter()
            .rev()
            .map(|(id, group)| (*id, group.clone()))
            .collect();
        reverse.partitions = forward
            .partitions
            .iter()
            .rev()
            .map(|(id, partition)| (*id, partition.clone()))
            .collect();

        assert_eq!(forward.seed_hash().unwrap(), reverse.seed_hash().unwrap());
    }

    #[test]
    fn lifecycle_every_valid_v1_semantic_field_changes_seed_hash() {
        let original = valid_spec();
        let original_hash = original.seed_hash().unwrap();
        let mut changed = Vec::new();

        let mut candidate = original.clone();
        candidate.cluster_name.push_str("-other");
        changed.push(candidate);

        let mut candidate = original.clone();
        candidate.nodes.get_mut(&1).unwrap().addr = "node-1:9002".to_string();
        changed.push(candidate);

        let mut candidate = original.clone();
        candidate.nodes = nodes(&[1, 2]);
        candidate.groups = BTreeMap::from([
            (
                1,
                SeedGroup {
                    voters: voters(&[1, 2]),
                },
            ),
            (
                2,
                SeedGroup {
                    voters: voters(&[1, 2]),
                },
            ),
        ]);
        changed.push(candidate);

        let mut candidate = original.clone();
        candidate.groups = BTreeMap::from([
            (1, original.groups[&1].clone()),
            (3, original.groups[&2].clone()),
        ]);
        candidate.partitions.get_mut(&2).unwrap().group_id = 3;
        changed.push(candidate);

        let mut candidate = original.clone();
        candidate.partitions = BTreeMap::from([
            (1, original.partitions[&1].clone()),
            (
                3,
                SeedPartition {
                    config: PartitionConfig::SingleId(3),
                    group_id: 2,
                },
            ),
        ]);
        changed.push(candidate);

        let mut candidate = original.clone();
        candidate.partitions.get_mut(&2).unwrap().group_id = 1;
        changed.push(candidate);

        for candidate in changed {
            candidate.validate().unwrap();
            assert_ne!(original_hash, candidate.seed_hash().unwrap());
        }
    }

    #[test]
    fn lifecycle_seed_rejects_unsupported_schema() {
        let mut spec = valid_spec();
        spec.version = 2;
        assert!(matches!(
            spec.validate(),
            Err(LifecycleError::UnsupportedSchema { .. })
        ));
    }

    #[test]
    fn lifecycle_seed_rejects_empty_required_collections_and_cluster_name() {
        let mut spec = valid_spec();
        spec.cluster_name.clear();
        assert!(spec.validate().is_err());

        let mut spec = valid_spec();
        spec.nodes.clear();
        assert!(spec.validate().is_err());

        let mut spec = valid_spec();
        spec.groups.clear();
        assert!(spec.validate().is_err());

        let mut spec = valid_spec();
        spec.partitions.clear();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn lifecycle_seed_rejects_zero_node_group_partition_and_single_ids() {
        let mut spec = valid_spec();
        spec.nodes = nodes(&[0, 1]);
        spec.groups = BTreeMap::from([(
            1,
            SeedGroup {
                voters: voters(&[0, 1]),
            },
        )]);
        spec.partitions = BTreeMap::from([(
            1,
            SeedPartition {
                config: PartitionConfig::SingleId(1),
                group_id: 1,
            },
        )]);
        assert!(spec.validate().is_err());

        let mut spec = valid_spec();
        spec.groups = BTreeMap::from([(
            0,
            SeedGroup {
                voters: voters(&[1, 2, 3]),
            },
        )]);
        spec.partitions = BTreeMap::from([(
            1,
            SeedPartition {
                config: PartitionConfig::SingleId(1),
                group_id: 0,
            },
        )]);
        assert!(spec.validate().is_err());

        let mut spec = valid_spec();
        spec.partitions = BTreeMap::from([(
            0,
            SeedPartition {
                config: PartitionConfig::SingleId(0),
                group_id: 1,
            },
        )]);
        assert!(spec.validate().is_err());

        let mut spec = valid_spec();
        spec.partitions.get_mut(&1).unwrap().config = PartitionConfig::SingleId(0);
        assert!(spec.validate().is_err());
    }

    #[test]
    fn lifecycle_seed_rejects_unknown_and_non_uniform_voters() {
        let mut spec = valid_spec();
        spec.groups.get_mut(&1).unwrap().voters.insert(4);
        assert!(spec.validate().is_err());

        let mut spec = valid_spec();
        spec.groups.get_mut(&2).unwrap().voters = voters(&[1, 2]);
        assert!(spec.validate().is_err());
    }

    #[test]
    fn lifecycle_seed_rejects_unknown_partition_group() {
        let mut spec = valid_spec();
        spec.partitions.get_mut(&1).unwrap().group_id = 99;
        assert!(spec.validate().is_err());
    }

    #[test]
    fn lifecycle_seed_rejects_range_partitions() {
        let mut spec = valid_spec();
        spec.partitions.get_mut(&1).unwrap().config = PartitionConfig::Range { start: 1, end: 2 };
        assert!(spec.validate().is_err());
    }

    #[test]
    fn lifecycle_seed_rejects_mismatched_single_partition_id() {
        let mut spec = valid_spec();
        spec.partitions.get_mut(&1).unwrap().config = PartitionConfig::SingleId(2);
        assert!(spec.validate().is_err());
    }

    #[test]
    fn lifecycle_seed_rejects_invalid_advertised_addresses() {
        for addr in [
            "",
            "   ",
            "node-1",
            "node-1:not-a-port",
            "node-1:0",
            "node 1:9001",
            "http://node-1:9001",
            "::1:9001",
            "node-1:9001:9002",
            "[not-ipv6]:9001",
        ] {
            let mut spec = golden_spec();
            spec.nodes.get_mut(&1).unwrap().addr = addr.to_string();
            assert!(
                spec.validate().is_err(),
                "accepted invalid address {addr:?}"
            );
        }

        for addr in ["node-1:9001", "127.0.0.1:9001", "[::1]:9001"] {
            let mut spec = golden_spec();
            spec.nodes.get_mut(&1).unwrap().addr = addr.to_string();
            spec.validate().unwrap();
        }
    }

    #[test]
    fn lifecycle_stock_maps_partition_i_to_group_i() {
        let spec = SeedSpec::stock("stock", nodes(&[3, 1, 2]), 3).unwrap();
        let expected_voters = voters(&[1, 2, 3]);
        assert_eq!(spec.groups.len(), 3);
        assert_eq!(spec.partitions.len(), 3);
        for i in 1..=3 {
            assert_eq!(spec.groups[&i].voters, expected_voters);
            assert_eq!(spec.partitions[&i].group_id, i);
            assert_eq!(spec.partitions[&i].config, PartitionConfig::SingleId(i));
        }
        assert!(SeedSpec::stock("stock", nodes(&[1]), 0).is_err());
        assert!(SeedSpec::stock("stock", BTreeMap::new(), 1).is_err());
    }

    #[test]
    fn lifecycle_initial_routing_table_preserves_seed_topology() {
        let table = valid_spec().initial_routing_table().unwrap();
        assert_eq!(table.nodes.len(), 3);
        assert_eq!(table.groups[&1].replicas, vec![1, 2, 3]);
        assert_eq!(table.groups[&2].replicas, vec![1, 2, 3]);
        assert_eq!(table.partitions[&1], (PartitionConfig::SingleId(1), 1));
        assert_eq!(table.partitions[&2], (PartitionConfig::SingleId(2), 2));
    }

    #[test]
    fn lifecycle_forming_group_can_become_active() {
        let current = lifecycle();
        let mut next = current.clone();
        next.groups.get_mut(&1).unwrap().state = FormationState::Active;
        current.validate_transition(&next).unwrap();
    }

    #[test]
    fn lifecycle_active_group_cannot_return_to_forming() {
        let current = lifecycle();
        let mut next = current.clone();
        next.groups.get_mut(&2).unwrap().state = FormationState::Forming;
        assert!(matches!(
            current.validate_transition(&next),
            Err(LifecycleError::InvalidTransition(_))
        ));
    }

    #[test]
    fn lifecycle_bootstrap_epoch_cannot_change() {
        let current = lifecycle();
        let mut next = current.clone();
        next.groups.get_mut(&1).unwrap().bootstrap_epoch += 1;
        assert!(matches!(
            current.validate_transition(&next),
            Err(LifecycleError::InvalidTransition(_))
        ));
    }

    #[test]
    fn lifecycle_transition_rejects_group_addition() {
        let current = lifecycle();
        let mut next = current.clone();
        next.groups.insert(
            3,
            GroupFormation {
                bootstrap_epoch: 1,
                state: FormationState::Forming,
            },
        );
        assert!(matches!(
            current.validate_transition(&next),
            Err(LifecycleError::InvalidTransition(_))
        ));
    }

    #[test]
    fn lifecycle_transition_rejects_identity_and_seed_mismatch() {
        let current = lifecycle();
        let mut next = current.clone();
        next.cluster_uuid = Uuid::from_u128(0x5678);
        assert!(matches!(
            current.validate_transition(&next),
            Err(LifecycleError::IdentityMismatch)
        ));

        let mut next = current.clone();
        next.seed_hash = [8; 32];
        assert!(matches!(
            current.validate_transition(&next),
            Err(LifecycleError::SeedMismatch)
        ));
    }

    #[test]
    fn lifecycle_store_revision_is_separate_from_topology_revision() {
        let versioned = VersionedLifecycle {
            revision: 99,
            record: lifecycle(),
        };
        assert_eq!(versioned.revision, 99);
        assert_eq!(versioned.record.topology_revision, 7);
    }

    #[test]
    fn lifecycle_store_is_object_safe_and_errors_convert_to_rr_error() {
        let _: Option<Box<dyn LifecycleStore>> = None;
        let _: Option<CasOutcome> = Some(CasOutcome::Applied);
        let error = RrError::from(LifecycleError::Unavailable("offline".to_string()));
        assert!(matches!(error, RrError::Lifecycle(_)));
    }
}
