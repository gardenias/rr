use super::config::SeedPeer;
use super::discovery::validate_records;
use super::model::{BoundedLru, Capability, GroupObservation};
use crate::{connect, make_admin_client, AdminClient};
use async_trait::async_trait;
use rr_core::{NodeDescriptor, NodeDirectoryRecord};
use rr_network::proto::admin::{
    BootRecordPage, BootRecordsRequest, BootStatus, BuildInfo, ClusterTopology,
    ClusterTopologyRequest, Empty, FormationStatus, GroupId, GroupMembership, GroupReplicaStatus,
    GroupStatus, GroupSummary, NodeDescriptorResponse, PartitionRoute, SnapshotStatus,
};
use rr_network::tls::TlsConfig;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tonic::{Code, Status};

const TOPOLOGY_PAGE_SIZE: u32 = 200;
const MAX_TOPOLOGY_PAGES: usize = 1024;
const SLOW_CAPABILITY_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub(crate) struct DescriptorEvidence {
    pub descriptor: NodeDescriptor,
    pub directory_revision: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct NodeSnapshot {
    pub node_id: u64,
    pub listen_addr: String,
    pub uptime_secs: u64,
    pub groups: Option<Vec<GroupSummary>>,
    pub routes: Option<Vec<PartitionRoute>>,
    pub formation: Option<FormationStatus>,
    pub descriptor: Option<DescriptorEvidence>,
    pub build: Option<BuildInfo>,
    pub boot: Option<BootStatus>,
    pub topology: Option<ClusterTopology>,
    pub capabilities: Vec<Capability>,
    pub collected_at: SystemTime,
}

impl NodeSnapshot {
    pub fn validate_for(&self, peer: &SeedPeer) -> std::result::Result<(), UpstreamError> {
        if self.node_id != peer.node_id {
            return Err(UpstreamError {
                code: "NODE_ID_MISMATCH",
                message: "AdminService returned a different node ID than its trusted peer"
                    .to_string(),
            });
        }
        if let Some(evidence) = &self.descriptor {
            if evidence.descriptor.node_id != peer.node_id {
                return Err(UpstreamError {
                    code: "NODE_ID_MISMATCH",
                    message: "GetNodeDescriptor returned a different node ID than GetNodeStatus"
                        .to_string(),
                });
            }
            evidence
                .descriptor
                .validate()
                .map_err(|error| UpstreamError {
                    code: "INVALID_NODE_DESCRIPTOR",
                    message: format!("AdminService returned an invalid node descriptor: {error}"),
                })?;
        }
        if let Some(topology) = &self.topology {
            let records = topology_records(topology)?;
            validate_records(&records)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GroupDetail {
    pub status: GroupStatus,
    pub membership: Option<GroupMembership>,
    pub snapshot: Option<SnapshotStatus>,
    pub replica: Option<GroupReplicaStatus>,
    pub capabilities: Vec<Capability>,
    pub collected_at: SystemTime,
}

#[derive(Clone, Debug)]
pub(crate) struct UpstreamError {
    pub code: &'static str,
    pub message: String,
}

#[async_trait]
pub(crate) trait Upstream: Send + Sync {
    async fn collect_node(
        &self,
        peer: &SeedPeer,
    ) -> std::result::Result<NodeSnapshot, UpstreamError>;

    async fn collect_node_with_revision(
        &self,
        peer: &SeedPeer,
        _directory_revision: u64,
    ) -> std::result::Result<NodeSnapshot, UpstreamError> {
        self.collect_node(peer).await
    }

    async fn collect_group(
        &self,
        peer: &SeedPeer,
        group_id: u64,
    ) -> std::result::Result<GroupDetail, UpstreamError>;

    async fn list_boot_records(
        &self,
        _peer: &SeedPeer,
        _cursor: Option<String>,
        _page_size: usize,
    ) -> std::result::Result<BootRecordPage, UpstreamError> {
        Err(UpstreamError {
            code: "UNIMPLEMENTED",
            message: "legacy AdminService does not expose boot records".to_string(),
        })
    }
}

#[derive(Clone)]
struct SlowNodeData {
    descriptor: Option<DescriptorEvidence>,
    topology: Option<ClusterTopology>,
    build: Option<BuildInfo>,
    boot: Option<BootStatus>,
    capabilities: Vec<Capability>,
}

#[derive(Clone)]
struct CachedSlowNode {
    data: SlowNodeData,
    observed_uptime_secs: u64,
    observed_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SlowNodeCacheKey {
    node_id: u64,
    admin_addr: String,
    directory_revision: u64,
    boot_id: String,
}

#[derive(Clone)]
pub(crate) struct TonicUpstream {
    tls: Option<TlsConfig>,
    token: Option<Arc<str>>,
    slow_nodes: Arc<tokio::sync::Mutex<BoundedLru<SlowNodeCacheKey, CachedSlowNode>>>,
}

impl TonicUpstream {
    pub fn new(tls: Option<TlsConfig>, token: Option<Arc<str>>) -> Self {
        Self {
            tls,
            token,
            slow_nodes: Arc::new(tokio::sync::Mutex::new(BoundedLru::new(
                256,
                SLOW_CAPABILITY_INTERVAL,
            ))),
        }
    }

    async fn client(&self, peer: &SeedPeer) -> std::result::Result<AdminClient, UpstreamError> {
        connect(&peer.admin_addr, self.tls.as_ref())
            .await
            .map(|channel| make_admin_client(channel, &self.token))
            .map_err(|_| UpstreamError {
                code: "ADMIN_UNAVAILABLE",
                message: "AdminService connection failed".to_string(),
            })
    }

    async fn cached_slow_data(&self, key: &SlowNodeCacheKey) -> Option<SlowNodeData> {
        self.slow_nodes
            .lock()
            .await
            .get_cloned(key, Instant::now())
            .map(|cached| cached.data)
    }

    async fn cached_slow_data_for_peer(
        &self,
        peer: &SeedPeer,
        directory_revision: u64,
        uptime_secs: u64,
    ) -> Option<SlowNodeData> {
        let now = Instant::now();
        self.slow_nodes
            .lock()
            .await
            .find_cloned(now, |key, cached| {
                key.node_id == peer.node_id
                    && key.admin_addr == peer.admin_addr
                    && key.directory_revision == directory_revision
                    && uptime_secs
                        .saturating_add(now.duration_since(cached.observed_at).as_secs())
                        .saturating_add(1)
                        >= cached.observed_uptime_secs
            })
            .map(|(_, cached)| cached.data)
    }

    async fn slow_node_data(
        &self,
        client: &mut AdminClient,
        peer: &SeedPeer,
        directory_revision: u64,
        uptime_secs: u64,
    ) -> std::result::Result<SlowNodeData, UpstreamError> {
        let node_id = peer.node_id;
        if let Some(cached) = self
            .cached_slow_data_for_peer(peer, directory_revision, uptime_secs)
            .await
        {
            return Ok(cached);
        }
        let mut capabilities = Vec::new();
        let boot = match client.get_boot_status(Empty {}).await {
            Ok(response) => {
                capabilities.push(Capability::supported("boot_status"));
                Some(response.into_inner())
            }
            Err(error) => {
                capabilities.push(capability_for("boot_status", &error));
                None
            }
        };
        let key = SlowNodeCacheKey {
            node_id,
            admin_addr: peer.admin_addr.clone(),
            directory_revision,
            boot_id: boot
                .as_ref()
                .and_then(|boot| boot.current.as_ref())
                .map(|boot| boot.boot_id.clone())
                .unwrap_or_default(),
        };
        let descriptor = match client.get_node_descriptor(Empty {}).await {
            Ok(response) => {
                let response = response.into_inner();
                match descriptor_evidence(response) {
                    Ok(evidence) => {
                        if evidence.descriptor.node_id != node_id {
                            return Err(UpstreamError {
                                code: "NODE_ID_MISMATCH",
                                message: "GetNodeDescriptor returned a different node ID than GetNodeStatus"
                                    .to_string(),
                            });
                        }
                        capabilities.push(Capability::supported("node_descriptor"));
                        Some(evidence)
                    }
                    Err(error) => {
                        capabilities.push(Capability {
                            feature: "node_descriptor".to_string(),
                            status: "ERROR".to_string(),
                            reason: Some(error.message.clone()),
                        });
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                capabilities.push(capability_for("node_descriptor", &error));
                None
            }
        };
        let topology = match collect_cluster_topology(client).await {
            Ok(topology) => {
                capabilities.push(Capability::supported("cluster_topology"));
                Some(topology)
            }
            Err(error) => {
                capabilities.push(capability_for("cluster_topology", &error));
                None
            }
        };
        let build = match client.get_build_info(Empty {}).await {
            Ok(response) => {
                capabilities.push(Capability::supported("build_info"));
                Some(response.into_inner())
            }
            Err(error) => {
                capabilities.push(capability_for("build_info", &error));
                None
            }
        };
        let data = SlowNodeData {
            descriptor,
            topology,
            build,
            boot,
            capabilities,
        };
        self.slow_nodes.lock().await.insert(
            key,
            CachedSlowNode {
                data: data.clone(),
                observed_uptime_secs: uptime_secs,
                observed_at: Instant::now(),
            },
            Instant::now(),
        );
        Ok(data)
    }
}

#[async_trait]
impl Upstream for TonicUpstream {
    async fn collect_node(
        &self,
        peer: &SeedPeer,
    ) -> std::result::Result<NodeSnapshot, UpstreamError> {
        self.collect_node_with_revision(peer, 0).await
    }

    async fn collect_node_with_revision(
        &self,
        peer: &SeedPeer,
        directory_revision: u64,
    ) -> std::result::Result<NodeSnapshot, UpstreamError> {
        let mut client = self.client(peer).await?;
        let status = client
            .get_node_status(Empty {})
            .await
            .map_err(|error| transport_error("AdminService GetNodeStatus failed", &error))?
            .into_inner();
        if status.node_id != peer.node_id {
            return Err(UpstreamError {
                code: "NODE_ID_MISMATCH",
                message: "AdminService returned a different node ID than its trusted peer"
                    .to_string(),
            });
        }

        let mut capabilities = vec![Capability::supported("node_status")];
        let groups = match client.list_groups(Empty {}).await {
            Ok(response) => {
                capabilities.push(Capability::supported("groups"));
                Some(
                    response
                        .into_inner()
                        .groups
                        .into_iter()
                        .map(|mut group| {
                            group.leader_node_id =
                                group.leader_node_id.and_then(normalize_legacy_leader);
                            group
                        })
                        .collect(),
                )
            }
            Err(error) => {
                capabilities.push(capability_for("groups", &error));
                if error.code() != Code::Unimplemented {
                    return Err(transport_error("AdminService ListGroups failed", &error));
                }
                None
            }
        };
        let routes = match client.get_routing_table(Empty {}).await {
            Ok(response) => {
                capabilities.push(Capability::supported("routes"));
                Some(response.into_inner().partitions)
            }
            Err(error) => {
                capabilities.push(capability_for("routes", &error));
                None
            }
        };
        let formation = match client.get_formation_status(Empty {}).await {
            Ok(response) => {
                capabilities.push(Capability::supported("formation"));
                Some(response.into_inner())
            }
            Err(error) => {
                capabilities.push(capability_for("formation", &error));
                None
            }
        };
        let slow = self
            .slow_node_data(&mut client, peer, directory_revision, status.uptime_secs)
            .await?;
        capabilities.extend(slow.capabilities.iter().cloned());
        Ok(NodeSnapshot {
            node_id: status.node_id,
            listen_addr: status.listen_addr,
            uptime_secs: status.uptime_secs,
            groups,
            routes,
            formation,
            descriptor: slow.descriptor,
            build: slow.build,
            boot: slow.boot,
            topology: slow.topology,
            capabilities,
            collected_at: SystemTime::now(),
        })
    }

    async fn collect_group(
        &self,
        peer: &SeedPeer,
        group_id: u64,
    ) -> std::result::Result<GroupDetail, UpstreamError> {
        let mut client = self.client(peer).await?;
        let status = client
            .get_group_status(GroupId { group_id })
            .await
            .map_err(|error| transport_error("AdminService GetGroupStatus failed", &error))?
            .into_inner();
        let membership = match client.get_group_membership(GroupId { group_id }).await {
            Ok(response) => Some(response.into_inner()),
            Err(error) if error.code() == Code::Unimplemented => None,
            Err(error) => {
                return Err(transport_error(
                    "AdminService GetGroupMembership failed",
                    &error,
                ))
            }
        };
        let snapshot = match client.get_snapshot_status(GroupId { group_id }).await {
            Ok(response) => Some(response.into_inner()),
            Err(error) if error.code() == Code::Unimplemented => None,
            Err(error) => {
                return Err(transport_error(
                    "AdminService GetSnapshotStatus failed",
                    &error,
                ))
            }
        };
        let mut capabilities = vec![Capability::supported("group_status")];
        capabilities.push(if membership.is_some() {
            Capability::supported("membership")
        } else {
            Capability::unsupported("membership", "legacy AdminService returned UNIMPLEMENTED")
        });
        capabilities.push(if snapshot.is_some() {
            Capability::supported("snapshots")
        } else {
            Capability::unsupported("snapshots", "legacy AdminService returned UNIMPLEMENTED")
        });
        let replica = match client.get_group_replica_status(GroupId { group_id }).await {
            Ok(response) => {
                let replica = response.into_inner();
                if replica.node_id != peer.node_id || replica.group_id != group_id {
                    return Err(UpstreamError {
                        code: "REPLICA_IDENTITY_MISMATCH",
                        message: "GetGroupReplicaStatus returned a different node or group ID"
                            .to_string(),
                    });
                }
                capabilities.push(Capability::supported("group_replica_status"));
                Some(replica)
            }
            Err(error) => {
                capabilities.push(capability_for("group_replica_status", &error));
                None
            }
        };
        Ok(GroupDetail {
            status,
            membership,
            snapshot,
            replica,
            capabilities,
            collected_at: SystemTime::now(),
        })
    }

    async fn list_boot_records(
        &self,
        peer: &SeedPeer,
        cursor: Option<String>,
        page_size: usize,
    ) -> std::result::Result<BootRecordPage, UpstreamError> {
        let mut client = self.client(peer).await?;
        client
            .list_boot_records(BootRecordsRequest {
                cursor: cursor.unwrap_or_default(),
                page_size: page_size as u32,
            })
            .await
            .map(|response| response.into_inner())
            .map_err(|error| transport_error("AdminService ListBootRecords failed", &error))
    }
}

fn descriptor_evidence(
    response: NodeDescriptorResponse,
) -> std::result::Result<DescriptorEvidence, UpstreamError> {
    let descriptor = response.descriptor.ok_or_else(|| UpstreamError {
        code: "INVALID_NODE_DESCRIPTOR",
        message: "GetNodeDescriptor returned no descriptor".to_string(),
    })?;
    let descriptor = NodeDescriptor {
        node_id: descriptor.node_id,
        client_addr: descriptor.client_addr,
        admin_addr: descriptor.admin_addr,
        observability_addr: descriptor.observability_addr,
        advertise_addr: descriptor.advertise_addr,
        environment: descriptor.environment,
        region: descriptor.region,
        zone: descriptor.zone,
        rack: descriptor.rack,
        boot_id: descriptor.boot_id,
    };
    descriptor.validate().map_err(|error| UpstreamError {
        code: "INVALID_NODE_DESCRIPTOR",
        message: format!("AdminService returned an invalid node descriptor: {error}"),
    })?;
    Ok(DescriptorEvidence {
        descriptor,
        directory_revision: response.directory_revision,
    })
}

pub(crate) fn topology_records(
    topology: &ClusterTopology,
) -> std::result::Result<Vec<NodeDirectoryRecord>, UpstreamError> {
    topology
        .nodes
        .iter()
        .map(|entry| {
            let descriptor = entry.descriptor.clone().ok_or_else(|| UpstreamError {
                code: "INVALID_CLUSTER_TOPOLOGY",
                message: "cluster topology contains a directory entry without a descriptor"
                    .to_string(),
            })?;
            let evidence = descriptor_evidence(NodeDescriptorResponse {
                descriptor: Some(descriptor),
                directory_revision: entry.revision,
            })?;
            Ok(NodeDirectoryRecord {
                descriptor: evidence.descriptor,
                revision: entry.revision,
                lease_id: entry.lease_id,
            })
        })
        .collect()
}

async fn collect_cluster_topology(
    client: &mut AdminClient,
) -> std::result::Result<ClusterTopology, Status> {
    let mut cursor = String::new();
    let mut seen_cursors = HashSet::new();
    let mut combined: Option<ClusterTopology> = None;
    for _ in 0..MAX_TOPOLOGY_PAGES {
        let page = client
            .get_cluster_topology(ClusterTopologyRequest {
                cursor: cursor.clone(),
                page_size: TOPOLOGY_PAGE_SIZE,
            })
            .await?
            .into_inner();
        if let Some(snapshot) = &combined {
            if snapshot.snapshot_id != page.snapshot_id
                || snapshot.config_revision != page.config_revision
                || snapshot.directory_revision != page.directory_revision
            {
                return Err(Status::data_loss(
                    "cluster topology cursor crossed snapshot identities",
                ));
            }
        }
        let next_cursor = page.next_cursor.clone();
        let snapshot = combined.get_or_insert_with(|| ClusterTopology {
            snapshot_id: page.snapshot_id,
            config_revision: page.config_revision,
            directory_revision: page.directory_revision,
            nodes: Vec::new(),
            groups: Vec::new(),
            partitions: Vec::new(),
            next_cursor: String::new(),
        });
        snapshot.nodes.extend(page.nodes);
        snapshot.groups.extend(page.groups);
        snapshot.partitions.extend(page.partitions);
        if next_cursor.is_empty() {
            return Ok(combined.expect("topology snapshot initialized"));
        }
        if next_cursor == cursor || !seen_cursors.insert(next_cursor.clone()) {
            return Err(Status::data_loss(
                "cluster topology returned a repeated pagination cursor",
            ));
        }
        cursor = next_cursor;
    }
    Err(Status::resource_exhausted(
        "cluster topology exceeded the Console page limit",
    ))
}

fn transport_error(message: &str, status: &Status) -> UpstreamError {
    UpstreamError {
        code: match status.code() {
            Code::DeadlineExceeded => "UPSTREAM_TIMEOUT",
            Code::Unimplemented => "UNIMPLEMENTED",
            Code::InvalidArgument | Code::OutOfRange => "INVALID_UPSTREAM_CURSOR",
            _ => "ADMIN_RPC_FAILED",
        },
        message: format!("{message} ({})", status.code()),
    }
}

pub(crate) fn normalize_legacy_leader(node_id: u64) -> Option<u64> {
    (node_id != 0).then_some(node_id)
}

fn capability_for(feature: &str, status: &Status) -> Capability {
    if status.code() == Code::Unimplemented {
        Capability::unsupported(
            feature,
            if status.message().is_empty() {
                "legacy AdminService returned UNIMPLEMENTED"
            } else {
                status.message()
            },
        )
    } else {
        Capability {
            feature: feature.to_string(),
            status: "ERROR".to_string(),
            reason: Some(format!("AdminService returned {}", status.code())),
        }
    }
}

impl From<GroupStatus> for GroupObservation {
    fn from(status: GroupStatus) -> Self {
        Self {
            node_id: 0,
            group_id: status.group_id,
            is_leader: status.is_leader,
            current_term: status.current_term,
            last_log_index: status.last_log_index,
            leader_node_id: normalize_legacy_leader(status.leader_node_id),
            observed_at: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rr_network::proto::admin::{GroupStatus, NodeDirectoryEntry};

    fn snapshot(peer: &SeedPeer) -> NodeSnapshot {
        NodeSnapshot {
            node_id: peer.node_id,
            listen_addr: "127.0.0.1:51002".to_string(),
            uptime_secs: 1,
            groups: Some(vec![]),
            routes: None,
            formation: None,
            descriptor: None,
            build: None,
            boot: None,
            topology: None,
            capabilities: vec![],
            collected_at: SystemTime::now(),
        }
    }

    #[test]
    fn legacy_zero_leader_is_absent_not_a_real_node() {
        assert_eq!(normalize_legacy_leader(0), None);
        assert_eq!(normalize_legacy_leader(7), Some(7));

        let status = GroupStatus {
            group_id: 3,
            is_leader: false,
            current_term: 9,
            last_log_index: 42,
            leader_node_id: 0,
        };
        assert_eq!(GroupObservation::from(status).leader_node_id, None);
    }

    #[test]
    fn unsupported_rpc_is_a_capability_not_empty_success() {
        let capability = capability_for("groups", &Status::unimplemented("legacy node"));
        assert_eq!(capability.status, "UNSUPPORTED");
        assert_eq!(capability.feature, "groups");
        assert_eq!(capability.reason.as_deref(), Some("legacy node"));
    }

    #[tokio::test]
    async fn seed_identity_mismatch_is_rejected_before_caching() {
        let peer = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        let mut snapshot = snapshot(&peer);
        snapshot.node_id = 2;
        assert!(snapshot.validate_for(&peer).unwrap_err().code == "NODE_ID_MISMATCH");
    }

    #[test]
    fn descriptor_identity_and_endpoint_validation_precede_endpoint_trust() {
        let peer = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        let mut snapshot = snapshot(&peer);
        snapshot.descriptor = Some(DescriptorEvidence {
            descriptor: NodeDescriptor {
                node_id: 2,
                client_addr: "127.0.0.1:51002".to_string(),
                admin_addr: "127.0.0.1:52002".to_string(),
                observability_addr: "127.0.0.1:53002".to_string(),
                advertise_addr: "127.0.0.1:54002".to_string(),
                environment: "test".to_string(),
                region: None,
                zone: None,
                rack: None,
                boot_id: "boot-2".to_string(),
            },
            directory_revision: 7,
        });
        assert_eq!(
            snapshot.validate_for(&peer).unwrap_err().code,
            "NODE_ID_MISMATCH"
        );
    }

    #[tokio::test]
    async fn slow_capability_cache_never_crosses_trusted_address_boot_or_revision() {
        let upstream = TonicUpstream::new(None, None);
        let key = |address: &str, revision, boot: &str| SlowNodeCacheKey {
            node_id: 1,
            admin_addr: address.to_string(),
            directory_revision: revision,
            boot_id: boot.to_string(),
        };
        let original = key("127.0.0.1:52001", 1, "boot-a");
        upstream.slow_nodes.lock().await.insert(
            original.clone(),
            CachedSlowNode {
                data: SlowNodeData {
                    descriptor: None,
                    topology: None,
                    build: None,
                    boot: None,
                    capabilities: vec![],
                },
                observed_uptime_secs: 1,
                observed_at: Instant::now(),
            },
            Instant::now(),
        );

        assert!(upstream.cached_slow_data(&original).await.is_some());
        assert!(upstream
            .cached_slow_data(&key("127.0.0.1:52901", 1, "boot-a"))
            .await
            .is_none());
        assert!(upstream
            .cached_slow_data(&key("127.0.0.1:52001", 2, "boot-a"))
            .await
            .is_none());
        assert!(upstream
            .cached_slow_data(&key("127.0.0.1:52001", 1, "boot-b"))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn slow_cache_reuses_steady_state_but_not_a_rebooted_process() {
        let upstream = TonicUpstream::new(None, None);
        let peer = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        let key = SlowNodeCacheKey {
            node_id: 1,
            admin_addr: peer.admin_addr.clone(),
            directory_revision: 7,
            boot_id: "boot-a".to_string(),
        };
        upstream.slow_nodes.lock().await.insert(
            key,
            CachedSlowNode {
                data: SlowNodeData {
                    descriptor: None,
                    topology: None,
                    build: None,
                    boot: None,
                    capabilities: vec![],
                },
                observed_uptime_secs: 100,
                observed_at: Instant::now(),
            },
            Instant::now(),
        );

        assert!(upstream
            .cached_slow_data_for_peer(&peer, 7, 101)
            .await
            .is_some());
        assert!(upstream
            .cached_slow_data_for_peer(&peer, 7, 1)
            .await
            .is_none());
        assert!(upstream
            .cached_slow_data_for_peer(&peer, 8, 101)
            .await
            .is_none());
    }

    #[test]
    fn duplicate_topology_identities_are_rejected_before_roster_merge() {
        let peer = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        let mut snapshot = snapshot(&peer);
        let entry = NodeDirectoryEntry {
            descriptor: Some(rr_network::proto::admin::NodeDescriptor {
                node_id: 2,
                client_addr: "127.0.0.1:51002".to_string(),
                admin_addr: "127.0.0.1:52002".to_string(),
                observability_addr: "127.0.0.1:53002".to_string(),
                advertise_addr: "127.0.0.1:54002".to_string(),
                environment: "test".to_string(),
                region: None,
                zone: None,
                rack: None,
                boot_id: "boot-2".to_string(),
            }),
            revision: 1,
            lease_id: 1,
        };
        snapshot.topology = Some(ClusterTopology {
            snapshot_id: "snapshot".to_string(),
            config_revision: 1,
            directory_revision: 1,
            nodes: vec![entry.clone(), entry],
            groups: vec![],
            partitions: vec![],
            next_cursor: String::new(),
        });
        assert_eq!(
            snapshot.validate_for(&peer).unwrap_err().code,
            "NODE_ID_CONFLICT"
        );
    }
}
