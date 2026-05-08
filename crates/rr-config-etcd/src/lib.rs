//! etcd-backed [`ConfigProvider`] implementation.
//!
//! This crate provides a read-only view of cluster routing state stored in
//! etcd v3 under:
//! * `/rr/{cluster_id}/nodes/{node_id}`
//! * `/rr/{cluster_id}/partitions/{partition_id}`
//! * `/rr/{cluster_id}/groups/{group_id}/replicas`

use std::pin::Pin;

use async_trait::async_trait;
use etcd_client::{Client, Event, EventType, GetOptions, WatchOptions, WatchStream, Watcher};
use futures::Stream;
use rr_core::error::ConfigError;
use rr_core::types::{
    GroupId, GroupMeta, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub use rr_core::config::{ConfigProvider, RoutingTableChange};

const DEFAULT_CLUSTER_ID: &str = "default";
const WATCH_CHANNEL_CAPACITY: usize = 256;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PartitionValue {
    config: PartitionConfig,
    group_id: GroupId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchDomain {
    Node,
    Partition,
    Group,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawEventType {
    Put,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WatchEntity {
    Node(NodeId),
    Partition(PartitionId),
    Group(GroupId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawWatchEvent {
    event_type: RawEventType,
    entity: WatchEntity,
    value: Option<Vec<u8>>,
    prev_value: Option<Vec<u8>>,
}

impl RawWatchEvent {
    fn from_etcd_event(
        cluster_id: &str,
        domain: WatchDomain,
        event: &Event,
    ) -> Result<Self, ConfigError> {
        let kv = event
            .kv()
            .ok_or_else(|| ConfigError::InvalidConfig("watch event missing kv".to_string()))?;
        let key = kv.key_str().map_err(|err| {
            ConfigError::InvalidConfig(format!("watch event key is not valid UTF-8: {err}"))
        })?;

        let entity = match domain {
            WatchDomain::Node => WatchEntity::Node(parse_node_id_from_key(cluster_id, key)?),
            WatchDomain::Partition => {
                WatchEntity::Partition(parse_partition_id_from_key(cluster_id, key)?)
            }
            WatchDomain::Group => WatchEntity::Group(parse_group_id_from_key(cluster_id, key)?),
        };

        let event_type = match event.event_type() {
            EventType::Put => RawEventType::Put,
            EventType::Delete => RawEventType::Delete,
        };

        let value = if matches!(event_type, RawEventType::Put) {
            Some(kv.value().to_vec())
        } else {
            None
        };

        let prev_value = event.prev_kv().map(|prev| prev.value().to_vec());

        Ok(Self {
            event_type,
            entity,
            value,
            prev_value,
        })
    }
}

fn root_prefix(cluster_id: &str) -> String {
    format!("/rr/{cluster_id}")
}

fn nodes_prefix(cluster_id: &str) -> String {
    format!("{}/nodes/", root_prefix(cluster_id))
}

fn partitions_prefix(cluster_id: &str) -> String {
    format!("{}/partitions/", root_prefix(cluster_id))
}

fn groups_prefix(cluster_id: &str) -> String {
    format!("{}/groups/", root_prefix(cluster_id))
}

pub(crate) fn build_node_key(cluster_id: &str, node_id: NodeId) -> String {
    format!("{}/nodes/{node_id}", root_prefix(cluster_id))
}

#[cfg(test)]
pub(crate) fn build_partition_key(cluster_id: &str, partition_id: PartitionId) -> String {
    format!("{}/partitions/{partition_id}", root_prefix(cluster_id))
}

#[cfg(test)]
pub(crate) fn build_group_replicas_key(cluster_id: &str, group_id: GroupId) -> String {
    format!("{}/groups/{group_id}/replicas", root_prefix(cluster_id))
}

fn parse_u64_suffix(key: &str, prefix: &str, kind: &str) -> Result<u64, ConfigError> {
    let suffix = key
        .strip_prefix(prefix)
        .ok_or_else(|| ConfigError::InvalidConfig(format!("invalid {kind} key: {key}")))?;
    suffix
        .parse::<u64>()
        .map_err(|err| ConfigError::InvalidConfig(format!("invalid {kind} id in key {key}: {err}")))
}

fn parse_node_id_from_key(cluster_id: &str, key: &str) -> Result<NodeId, ConfigError> {
    parse_u64_suffix(key, &nodes_prefix(cluster_id), "node")
}

fn parse_partition_id_from_key(cluster_id: &str, key: &str) -> Result<PartitionId, ConfigError> {
    parse_u64_suffix(key, &partitions_prefix(cluster_id), "partition")
}

fn parse_group_id_from_key(cluster_id: &str, key: &str) -> Result<GroupId, ConfigError> {
    let prefix = groups_prefix(cluster_id);
    let rest = key
        .strip_prefix(&prefix)
        .ok_or_else(|| ConfigError::InvalidConfig(format!("invalid group key: {key}")))?;
    let (group_id_raw, suffix) = rest
        .split_once('/')
        .ok_or_else(|| ConfigError::InvalidConfig(format!("invalid group key: {key}")))?;
    if suffix != "replicas" {
        return Err(ConfigError::InvalidConfig(format!(
            "invalid group key suffix in {key}: expected replicas"
        )));
    }
    group_id_raw
        .parse::<u64>()
        .map_err(|err| ConfigError::InvalidConfig(format!("invalid group id in key {key}: {err}")))
}

fn map_etcd_error(context: &str, err: etcd_client::Error) -> ConfigError {
    tracing::error!(target: "rr::config::etcd", %context, error = %err, "etcd operation failed");
    ConfigError::ProviderUnavailable(format!("{context}: {err}"))
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8], context: &str) -> Result<T, ConfigError> {
    serde_json::from_slice(bytes).map_err(|err| {
        tracing::error!(target: "rr::config::etcd", %context, error = %err, "JSON decode failed");
        ConfigError::InvalidConfig(format!("{context}: {err}"))
    })
}

fn parse_watch_event(raw: RawWatchEvent) -> Result<RoutingTableChange, ConfigError> {
    match raw.entity {
        WatchEntity::Node(node_id) => match raw.event_type {
            RawEventType::Put => {
                let value = raw.value.ok_or_else(|| {
                    ConfigError::InvalidConfig(
                        "node watch PUT event missing value payload".to_string(),
                    )
                })?;
                let addr: NodeAddr = decode_json(&value, "decode node watch payload")?;
                Ok(RoutingTableChange::NodeAdded { node_id, addr })
            }
            RawEventType::Delete => Ok(RoutingTableChange::NodeRemoved { node_id }),
        },
        WatchEntity::Partition(partition_id) => match raw.event_type {
            RawEventType::Put => {
                let value = raw.value.ok_or_else(|| {
                    ConfigError::InvalidConfig(
                        "partition watch PUT event missing value payload".to_string(),
                    )
                })?;
                let part: PartitionValue = decode_json(&value, "decode partition watch payload")?;
                if raw.prev_value.is_some() {
                    Ok(RoutingTableChange::PartitionUpdated {
                        partition_id,
                        config: part.config,
                        group_id: part.group_id,
                    })
                } else {
                    Ok(RoutingTableChange::PartitionAdded {
                        partition_id,
                        config: part.config,
                        group_id: part.group_id,
                    })
                }
            }
            RawEventType::Delete => Ok(RoutingTableChange::PartitionRemoved { partition_id }),
        },
        WatchEntity::Group(group_id) => match raw.event_type {
            RawEventType::Put => {
                let value = raw.value.ok_or_else(|| {
                    ConfigError::InvalidConfig("group watch PUT event missing value payload".into())
                })?;
                let replicas: Vec<NodeId> = decode_json(&value, "decode group watch payload")?;
                Ok(RoutingTableChange::GroupMembershipChanged { group_id, replicas })
            }
            RawEventType::Delete => Ok(RoutingTableChange::GroupMembershipChanged {
                group_id,
                replicas: Vec::new(),
            }),
        },
    }
}

async fn run_watch_loop(
    cluster_id: String,
    domain: WatchDomain,
    mut _watcher: Watcher,
    mut stream: WatchStream,
    _client: Client,
    sender: mpsc::Sender<RoutingTableChange>,
) {
    loop {
        let response = match stream.message().await {
            Ok(Some(response)) => response,
            Ok(None) => {
                tracing::error!(target: "rr::config::etcd", "watch stream ended unexpectedly");
                break;
            }
            Err(err) => {
                tracing::error!(target: "rr::config::etcd", error = %err, "watch stream failed");
                break;
            }
        };

        if response.canceled() {
            tracing::error!(
                target: "rr::config::etcd",
                watch_id = response.watch_id(),
                reason = response.cancel_reason(),
                compact_revision = response.compact_revision(),
                "watch canceled by etcd"
            );
            break;
        }

        for event in response.events() {
            let change = RawWatchEvent::from_etcd_event(&cluster_id, domain, event)
                .and_then(parse_watch_event);
            let change = match change {
                Ok(change) => change,
                Err(err) => {
                    tracing::error!(target: "rr::config::etcd", error = %err, "failed to parse watch event");
                    continue;
                }
            };

            if sender.send(change).await.is_err() {
                tracing::error!(target: "rr::config::etcd", "watch consumer disconnected");
                return;
            }
        }
    }
}

/// etcd-backed implementation of [`ConfigProvider`].
///
/// This type is intentionally read-only at the trait surface:
/// it implements reads and watch subscriptions only.
#[derive(Debug, Clone)]
pub struct EtcdConfigProvider {
    endpoints: Vec<String>,
    cluster_id: String,
}

impl EtcdConfigProvider {
    /// Create a new provider and validate connectivity to etcd.
    pub async fn new(endpoints: Vec<String>, cluster_id: String) -> Result<Self, ConfigError> {
        if endpoints.is_empty() {
            return Err(ConfigError::InvalidConfig(
                "etcd endpoints cannot be empty".to_string(),
            ));
        }

        let resolved_cluster_id = if cluster_id.trim().is_empty() {
            DEFAULT_CLUSTER_ID.to_string()
        } else {
            cluster_id
        };

        let mut client = Client::connect(endpoints.clone(), None)
            .await
            .map_err(|err| map_etcd_error("connect etcd", err))?;

        client
            .status()
            .await
            .map_err(|err| map_etcd_error("status etcd", err))?;

        Ok(Self {
            endpoints,
            cluster_id: resolved_cluster_id,
        })
    }

    async fn connect(&self) -> Result<Client, ConfigError> {
        Client::connect(self.endpoints.clone(), None)
            .await
            .map_err(|err| map_etcd_error("connect etcd", err))
    }

    async fn spawn_watcher(
        &self,
        domain: WatchDomain,
        sender: mpsc::Sender<RoutingTableChange>,
    ) -> Result<(), ConfigError> {
        let mut client = self.connect().await?;

        let prefix = match domain {
            WatchDomain::Node => nodes_prefix(&self.cluster_id),
            WatchDomain::Partition => partitions_prefix(&self.cluster_id),
            WatchDomain::Group => groups_prefix(&self.cluster_id),
        };

        let (watcher, stream) = client
            .watch(
                prefix,
                Some(WatchOptions::new().with_prefix().with_prev_key()),
            )
            .await
            .map_err(|err| map_etcd_error("create watch", err))?;

        let cluster_id = self.cluster_id.clone();
        tokio::spawn(async move {
            run_watch_loop(cluster_id, domain, watcher, stream, client, sender).await;
        });

        Ok(())
    }
}

#[async_trait]
impl ConfigProvider for EtcdConfigProvider {
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError> {
        let mut client = self.connect().await?;
        let scan_prefix = format!("{}/", root_prefix(&self.cluster_id));

        let response = client
            .get(scan_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|err| map_etcd_error("scan routing table", err))?;

        let mut table = RoutingTable::default();

        for kv in response.kvs() {
            let key = kv.key_str().map_err(|err| {
                ConfigError::InvalidConfig(format!("invalid UTF-8 key in etcd response: {err}"))
            })?;

            if key.starts_with(&nodes_prefix(&self.cluster_id)) {
                let node_id = parse_node_id_from_key(&self.cluster_id, key)?;
                let addr: NodeAddr = decode_json(kv.value(), "decode node entry")?;
                table.nodes.insert(node_id, addr);
                continue;
            }

            if key.starts_with(&partitions_prefix(&self.cluster_id)) {
                let partition_id = parse_partition_id_from_key(&self.cluster_id, key)?;
                let part: PartitionValue = decode_json(kv.value(), "decode partition entry")?;
                table
                    .partitions
                    .insert(partition_id, (part.config, part.group_id));
                continue;
            }

            if key.starts_with(&groups_prefix(&self.cluster_id)) {
                let group_id = parse_group_id_from_key(&self.cluster_id, key)?;
                let replicas: Vec<NodeId> = decode_json(kv.value(), "decode group entry")?;
                table.groups.insert(
                    group_id,
                    GroupMeta {
                        group_id,
                        leader: replicas.first().copied(),
                        replicas,
                    },
                );
            }
        }

        for (_, group_id) in table.partitions.values() {
            table.groups.entry(*group_id).or_insert_with(|| GroupMeta {
                group_id: *group_id,
                leader: None,
                replicas: Vec::new(),
            });
        }

        Ok(table)
    }

    async fn watch_changes(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError> {
        let (sender, receiver) = mpsc::channel(WATCH_CHANNEL_CAPACITY);

        self.spawn_watcher(WatchDomain::Node, sender.clone())
            .await?;
        self.spawn_watcher(WatchDomain::Partition, sender.clone())
            .await?;
        self.spawn_watcher(WatchDomain::Group, sender).await?;

        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    async fn get_node_addr(&self, node_id: NodeId) -> Result<NodeAddr, ConfigError> {
        let mut client = self.connect().await?;
        let key = build_node_key(&self.cluster_id, node_id);

        let response = client
            .get(key, None)
            .await
            .map_err(|err| map_etcd_error("get node address", err))?;

        let kv = response.kvs().first().ok_or_else(|| {
            ConfigError::InvalidConfig(format!("node {node_id} not found in cluster config"))
        })?;

        decode_json(kv.value(), "decode node address")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_addr_json_wire_shape() {
        let addr = NodeAddr {
            addr: "127.0.0.1:5001".to_string(),
        };
        let wire = serde_json::to_string(&addr).unwrap();
        assert_eq!(wire, r#"{"addr":"127.0.0.1:5001"}"#);
    }

    #[test]
    fn partition_json_wire_shape() {
        let payload = PartitionValue {
            config: PartitionConfig::Range { start: 10, end: 20 },
            group_id: 7,
        };
        let wire = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            wire,
            r#"{"config":{"Range":{"start":10,"end":20}},"group_id":7}"#
        );
    }

    #[test]
    fn key_builder_paths_match_schema() {
        assert_eq!(build_node_key("c1", 42), "/rr/c1/nodes/42");
        assert_eq!(build_partition_key("c1", 5), "/rr/c1/partitions/5");
        assert_eq!(
            build_group_replicas_key("c1", 9),
            "/rr/c1/groups/9/replicas"
        );
    }

    #[test]
    fn parse_watch_partition_added() {
        let raw = RawWatchEvent {
            event_type: RawEventType::Put,
            entity: WatchEntity::Partition(1),
            value: Some(
                serde_json::to_vec(&PartitionValue {
                    config: PartitionConfig::SingleId(1),
                    group_id: 100,
                })
                .unwrap(),
            ),
            prev_value: None,
        };

        let change = parse_watch_event(raw).unwrap();
        assert!(matches!(
            change,
            RoutingTableChange::PartitionAdded {
                partition_id: 1,
                group_id: 100,
                ..
            }
        ));
    }

    #[test]
    fn parse_watch_partition_updated() {
        let raw = RawWatchEvent {
            event_type: RawEventType::Put,
            entity: WatchEntity::Partition(2),
            value: Some(
                serde_json::to_vec(&PartitionValue {
                    config: PartitionConfig::Range { start: 0, end: 99 },
                    group_id: 8,
                })
                .unwrap(),
            ),
            prev_value: Some(vec![1]),
        };

        let change = parse_watch_event(raw).unwrap();
        assert!(matches!(
            change,
            RoutingTableChange::PartitionUpdated {
                partition_id: 2,
                group_id: 8,
                ..
            }
        ));
    }

    #[test]
    fn parse_watch_partition_removed() {
        let raw = RawWatchEvent {
            event_type: RawEventType::Delete,
            entity: WatchEntity::Partition(3),
            value: None,
            prev_value: None,
        };

        let change = parse_watch_event(raw).unwrap();
        assert!(matches!(
            change,
            RoutingTableChange::PartitionRemoved { partition_id: 3 }
        ));
    }

    #[test]
    fn parse_watch_node_add_and_remove() {
        let add = RawWatchEvent {
            event_type: RawEventType::Put,
            entity: WatchEntity::Node(7),
            value: Some(
                serde_json::to_vec(&NodeAddr {
                    addr: "127.0.0.1:7007".to_string(),
                })
                .unwrap(),
            ),
            prev_value: None,
        };
        let removed = RawWatchEvent {
            event_type: RawEventType::Delete,
            entity: WatchEntity::Node(7),
            value: None,
            prev_value: None,
        };

        let added_change = parse_watch_event(add).unwrap();
        let removed_change = parse_watch_event(removed).unwrap();

        assert!(matches!(
            added_change,
            RoutingTableChange::NodeAdded { node_id: 7, .. }
        ));
        assert!(matches!(
            removed_change,
            RoutingTableChange::NodeRemoved { node_id: 7 }
        ));
    }

    #[test]
    fn parse_watch_group_membership_changed() {
        let put = RawWatchEvent {
            event_type: RawEventType::Put,
            entity: WatchEntity::Group(9),
            value: Some(serde_json::to_vec(&vec![1_u64, 2, 3]).unwrap()),
            prev_value: None,
        };
        let delete = RawWatchEvent {
            event_type: RawEventType::Delete,
            entity: WatchEntity::Group(9),
            value: None,
            prev_value: None,
        };

        let put_change = parse_watch_event(put).unwrap();
        let delete_change = parse_watch_event(delete).unwrap();

        assert!(matches!(
            put_change,
            RoutingTableChange::GroupMembershipChanged {
                group_id: 9,
                replicas
            } if replicas == vec![1, 2, 3]
        ));
        assert!(matches!(
            delete_change,
            RoutingTableChange::GroupMembershipChanged {
                group_id: 9,
                replicas
            } if replicas.is_empty()
        ));
    }

    #[tokio::test]
    async fn constructor_maps_etcd_error_to_config_error() {
        let err = EtcdConfigProvider::new(vec!["://bad-endpoint".to_string()], "c1".to_string())
            .await
            .unwrap_err();
        assert!(matches!(err, ConfigError::ProviderUnavailable(_)));
    }
}
