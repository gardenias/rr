use super::config::SeedPeer;
use super::upstream::UpstreamError;
use futures::{Stream, StreamExt};
use rr_core::config::ConfigProvider;
use rr_core::directory::{NodeDirectory, NodeDirectoryPage, NodeDirectoryRecord};
use rr_core::types::{PartitionConfig, RoutingTable};
use rr_network::proto::admin::PartitionRoute;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

const DIRECTORY_PAGE_SIZE: usize = 200;
const MAX_DIRECTORY_PAGES: usize = 1024;

pub(crate) type DiscoveryWatch = Pin<Box<dyn Stream<Item = ()> + Send>>;

#[derive(Clone, Debug)]
pub(crate) struct DiscoverySnapshot {
    pub records: Vec<NodeDirectoryRecord>,
    pub routes: Vec<PartitionRoute>,
    pub groups: BTreeMap<u64, Vec<u64>>,
    pub directory_revision: u64,
}

pub(crate) struct TrustedDiscovery {
    directory: Arc<dyn NodeDirectory>,
    config: Arc<dyn ConfigProvider>,
}

impl TrustedDiscovery {
    pub fn new(directory: Arc<dyn NodeDirectory>, config: Arc<dyn ConfigProvider>) -> Self {
        Self { directory, config }
    }

    pub async fn reconcile(&self) -> Result<DiscoverySnapshot, UpstreamError> {
        let mut records = Vec::new();
        let mut cursor = None;
        let mut seen_cursors = HashSet::new();
        let mut directory_revision = 0;
        for _ in 0..MAX_DIRECTORY_PAGES {
            let page = self
                .directory
                .list(cursor.as_deref(), Some(DIRECTORY_PAGE_SIZE))
                .await
                .map_err(|error| UpstreamError {
                    code: "DIRECTORY_UNAVAILABLE",
                    message: format!("NodeDirectory full reconciliation failed: {error}"),
                })?;
            directory_revision = directory_revision.max(page.revision);
            let NodeDirectoryPage {
                records: mut page_records,
                next_cursor,
                ..
            } = page;
            records.append(&mut page_records);
            let Some(next_cursor) = next_cursor.filter(|cursor| !cursor.is_empty()) else {
                cursor = None;
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(UpstreamError {
                    code: "DIRECTORY_CURSOR_LOOP",
                    message: "NodeDirectory returned a repeated pagination cursor".to_string(),
                });
            }
            cursor = Some(next_cursor);
        }
        if cursor.is_some() {
            return Err(UpstreamError {
                code: "DIRECTORY_PAGE_LIMIT",
                message: "NodeDirectory exceeded the full reconciliation page limit".to_string(),
            });
        }
        validate_records(&records)?;
        let table = self
            .config
            .routing_table_snapshot()
            .await
            .map_err(|error| UpstreamError {
                code: "CONFIG_UNAVAILABLE",
                message: format!("ConfigProvider full reconciliation failed: {error}"),
            })?;
        let (routes, groups) = project_routing_table(&table)?;
        records.sort_by_key(|record| record.descriptor.node_id);
        Ok(DiscoverySnapshot {
            records,
            routes,
            groups,
            directory_revision,
        })
    }

    pub async fn watch(&self) -> Result<DiscoveryWatch, UpstreamError> {
        let directory = self
            .directory
            .watch()
            .await
            .map_err(|error| UpstreamError {
                code: "DIRECTORY_WATCH_UNAVAILABLE",
                message: format!("NodeDirectory watch failed: {error}"),
            })?;
        let config = self
            .config
            .watch_changes()
            .await
            .map_err(|error| UpstreamError {
                code: "CONFIG_WATCH_UNAVAILABLE",
                message: format!("ConfigProvider watch failed: {error}"),
            })?;
        Ok(Box::pin(futures::stream::select(
            directory.map(|_| ()),
            config.map(|_| ()),
        )))
    }
}

fn project_routing_table(
    table: &RoutingTable,
) -> Result<(Vec<PartitionRoute>, BTreeMap<u64, Vec<u64>>), UpstreamError> {
    let mut routes = Vec::with_capacity(table.partitions.len());
    let mut partitions = table.partitions.iter().collect::<Vec<_>>();
    partitions.sort_by_key(|(partition_id, _)| **partition_id);
    for (partition_id, (config, group_id)) in partitions {
        let PartitionConfig::SingleId(config_partition_id) = config else {
            return Err(UpstreamError {
                code: "INVALID_CONFIG_PROJECTION",
                message: format!(
                    "partition {partition_id} uses a reserved range configuration and cannot be projected"
                ),
            });
        };
        if config_partition_id != partition_id {
            return Err(UpstreamError {
                code: "INVALID_CONFIG_PROJECTION",
                message: format!(
                    "partition {partition_id} has mismatched single-ID config {config_partition_id}"
                ),
            });
        }
        routes.push(PartitionRoute {
            partition_id: *partition_id,
            group_id: *group_id,
            replica_node_ids: table
                .groups
                .get(group_id)
                .map(|group| group.replicas.clone())
                .unwrap_or_default(),
        });
    }
    let groups = table
        .groups
        .iter()
        .map(|(group_id, group)| (*group_id, group.replicas.clone()))
        .collect();
    Ok((routes, groups))
}

pub(crate) fn validate_records(
    records: &[NodeDirectoryRecord],
) -> Result<Vec<SeedPeer>, UpstreamError> {
    let mut node_ids = BTreeSet::new();
    let mut addresses = HashMap::new();
    let mut peers = Vec::with_capacity(records.len());
    for record in records {
        let descriptor = &record.descriptor;
        descriptor.validate().map_err(|error| UpstreamError {
            code: "INVALID_DIRECTORY_RECORD",
            message: format!("node {} descriptor is invalid: {error}", descriptor.node_id),
        })?;
        if !node_ids.insert(descriptor.node_id) {
            return Err(UpstreamError {
                code: "NODE_ID_CONFLICT",
                message: format!(
                    "node ID {} occurs in multiple trusted directory records",
                    descriptor.node_id
                ),
            });
        }
        if let Some(existing) = addresses.insert(descriptor.admin_addr.clone(), descriptor.node_id)
        {
            return Err(UpstreamError {
                code: "ADMIN_ADDRESS_CONFLICT",
                message: format!(
                    "AdminService address {} is claimed by nodes {existing} and {}",
                    descriptor.admin_addr, descriptor.node_id
                ),
            });
        }
        peers.push(SeedPeer {
            node_id: descriptor.node_id,
            admin_addr: descriptor.admin_addr.clone(),
        });
    }
    peers.sort_by_key(|peer| peer.node_id);
    Ok(peers)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{validate_records, TrustedDiscovery};
    use async_trait::async_trait;
    use futures::StreamExt;
    use rr_core::config::ConfigProvider;
    use rr_core::directory::{
        NodeDescriptor, NodeDirectory, NodeDirectoryChange, NodeDirectoryPage, NodeDirectoryRecord,
        NodeDirectoryRegistration,
    };
    use rr_core::error::{ConfigError, DirectoryError};
    use rr_core::types::{NodeAddr, PartitionConfig, RoutingTable};
    use std::pin::Pin;
    use std::sync::{Arc, RwLock};
    use tokio::sync::broadcast;

    pub(crate) struct MockDirectory {
        pages: RwLock<Vec<NodeDirectoryPage>>,
        changes: broadcast::Sender<NodeDirectoryChange>,
    }

    impl MockDirectory {
        pub(crate) fn new(pages: Vec<NodeDirectoryPage>) -> Self {
            let (changes, _) = broadcast::channel(16);
            Self {
                pages: RwLock::new(pages),
                changes,
            }
        }

        pub(crate) fn replace_pages(&self, pages: Vec<NodeDirectoryPage>) {
            *self.pages.write().unwrap() = pages;
        }

        pub(crate) fn notify_remove(&self, node_id: u64, revision: u64) {
            self.changes
                .send(NodeDirectoryChange::Remove { node_id, revision })
                .unwrap();
        }
    }

    #[async_trait]
    impl NodeDirectory for MockDirectory {
        async fn get(&self, _node_id: u64) -> Result<Option<NodeDirectoryRecord>, DirectoryError> {
            unimplemented!()
        }

        async fn list(
            &self,
            cursor: Option<&str>,
            _page_size: Option<usize>,
        ) -> Result<NodeDirectoryPage, DirectoryError> {
            let pages = self.pages.read().unwrap();
            let index = cursor
                .and_then(|cursor| cursor.strip_prefix("page-"))
                .and_then(|index| index.parse::<usize>().ok())
                .unwrap_or(0);
            pages.get(index).cloned().ok_or_else(|| {
                DirectoryError::InvalidCursor(cursor.unwrap_or_default().to_string())
            })
        }

        async fn watch(
            &self,
        ) -> Result<Pin<Box<dyn futures::Stream<Item = NodeDirectoryChange> + Send>>, DirectoryError>
        {
            Ok(Box::pin(
                tokio_stream::wrappers::BroadcastStream::new(self.changes.subscribe())
                    .filter_map(|change| async move { change.ok() }),
            ))
        }

        async fn register(
            &self,
            _descriptor: NodeDescriptor,
        ) -> Result<Box<dyn NodeDirectoryRegistration>, DirectoryError> {
            unimplemented!()
        }
    }

    pub(crate) struct MockConfig {
        table: RwLock<RoutingTable>,
        changes: broadcast::Sender<rr_core::RoutingTableChange>,
    }

    impl MockConfig {
        pub(crate) fn new(table: RoutingTable) -> Self {
            let (changes, _) = broadcast::channel(16);
            Self {
                table: RwLock::new(table),
                changes,
            }
        }
    }

    #[async_trait]
    impl ConfigProvider for MockConfig {
        async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError> {
            Ok(self.table.read().unwrap().clone())
        }

        async fn watch_changes(
            &self,
        ) -> Result<
            Pin<Box<dyn futures::Stream<Item = rr_core::RoutingTableChange> + Send>>,
            ConfigError,
        > {
            Ok(Box::pin(
                tokio_stream::wrappers::BroadcastStream::new(self.changes.subscribe())
                    .filter_map(|change| async move { change.ok() }),
            ))
        }

        async fn get_node_addr(&self, node_id: u64) -> Result<NodeAddr, ConfigError> {
            self.table
                .read()
                .unwrap()
                .nodes
                .get(&node_id)
                .cloned()
                .ok_or_else(|| ConfigError::InvalidConfig(format!("node {node_id} not found")))
        }
    }

    fn descriptor(node_id: u64, admin_addr: &str) -> NodeDescriptor {
        NodeDescriptor {
            node_id,
            client_addr: format!("10.0.0.{node_id}:51000"),
            admin_addr: admin_addr.to_string(),
            observability_addr: format!("10.0.0.{node_id}:53000"),
            advertise_addr: format!("10.0.0.{node_id}:54000"),
            environment: "test".to_string(),
            region: None,
            zone: None,
            rack: None,
            boot_id: format!("boot-{node_id}"),
        }
    }

    fn record(node_id: u64, admin_addr: &str, revision: u64) -> NodeDirectoryRecord {
        NodeDirectoryRecord {
            descriptor: descriptor(node_id, admin_addr),
            revision,
            lease_id: 100 + node_id as i64,
        }
    }

    fn page(records: Vec<NodeDirectoryRecord>, next_cursor: Option<&str>) -> NodeDirectoryPage {
        NodeDirectoryPage {
            records,
            next_cursor: next_cursor.map(str::to_string),
            revision: 20,
        }
    }

    #[tokio::test]
    async fn full_reconciliation_reads_every_directory_page_and_config_routes() {
        let directory = Arc::new(MockDirectory::new(vec![
            page(vec![record(1, "10.0.0.1:52001", 10)], Some("page-1")),
            page(vec![record(2, "10.0.0.2:52002", 11)], None),
        ]));
        let mut table = RoutingTable::default();
        table
            .partitions
            .insert(9, (PartitionConfig::SingleId(9), 7));
        table.groups.insert(
            7,
            rr_core::GroupMeta {
                group_id: 7,
                replicas: vec![1, 2],
            },
        );
        table.nodes.insert(
            1,
            NodeAddr {
                addr: "untrusted-client-address:1".to_string(),
            },
        );
        let discovery = TrustedDiscovery::new(directory, Arc::new(MockConfig::new(table)));

        let snapshot = discovery.reconcile().await.unwrap();
        assert_eq!(snapshot.records.len(), 2);
        assert_eq!(snapshot.directory_revision, 20);
        assert_eq!(snapshot.routes[0].partition_id, 9);
        assert_eq!(snapshot.groups.get(&7).unwrap(), &vec![1, 2]);
        let peers = validate_records(&snapshot.records).unwrap();
        assert_eq!(peers[0].admin_addr, "10.0.0.1:52001");
        assert!(peers
            .iter()
            .all(|peer| peer.admin_addr != "untrusted-client-address:1"));
    }

    #[test]
    fn duplicate_node_ids_and_admin_addresses_are_rejected_before_trust() {
        let duplicate_node = vec![
            record(1, "10.0.0.1:52001", 10),
            record(1, "10.0.0.99:52001", 11),
        ];
        assert_eq!(
            validate_records(&duplicate_node).unwrap_err().code,
            "NODE_ID_CONFLICT"
        );

        let duplicate_address = vec![
            record(1, "10.0.0.1:52001", 10),
            record(2, "10.0.0.1:52001", 11),
        ];
        assert_eq!(
            validate_records(&duplicate_address).unwrap_err().code,
            "ADMIN_ADDRESS_CONFLICT"
        );
    }

    #[tokio::test]
    async fn watch_change_is_followed_by_a_full_refresh_that_removes_expired_leases() {
        let directory = Arc::new(MockDirectory::new(vec![page(
            vec![
                record(1, "10.0.0.1:52001", 10),
                record(2, "10.0.0.2:52002", 11),
            ],
            None,
        )]));
        let discovery = TrustedDiscovery::new(
            directory.clone(),
            Arc::new(MockConfig::new(RoutingTable::default())),
        );
        assert_eq!(discovery.reconcile().await.unwrap().records.len(), 2);
        let mut changes = discovery.watch().await.unwrap();

        *directory.pages.write().unwrap() = vec![page(vec![record(1, "10.0.0.1:52001", 12)], None)];
        directory
            .changes
            .send(NodeDirectoryChange::Remove {
                node_id: 2,
                revision: 12,
            })
            .unwrap();
        changes.next().await.unwrap();

        let snapshot = discovery.reconcile().await.unwrap();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].descriptor.node_id, 1);
    }

    #[tokio::test]
    async fn watch_can_be_recreated_without_reusing_incremental_state() {
        let directory = Arc::new(MockDirectory::new(vec![page(vec![], None)]));
        let discovery = TrustedDiscovery::new(
            directory,
            Arc::new(MockConfig::new(RoutingTable::default())),
        );
        let changes = discovery.watch().await.unwrap();
        drop(changes);
        let replacement = discovery.watch().await.unwrap();
        drop(replacement);
        let snapshot = discovery.reconcile().await.unwrap();
        assert!(snapshot.records.is_empty());
        assert!(snapshot.routes.is_empty());
    }

    #[test]
    fn invalid_descriptors_never_become_trusted_peers() {
        let mut invalid = record(1, "0.0.0.0:52001", 10);
        invalid.descriptor.admin_addr = "0.0.0.0:52001".to_string();
        assert_eq!(
            validate_records(&[invalid]).unwrap_err().code,
            "INVALID_DIRECTORY_RECORD"
        );
    }
}
