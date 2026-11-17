use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, EventType, GetOptions, KeyValue, PutOptions,
    SortOrder, SortTarget, Txn, TxnOp, WatchOptions, WatchStream, Watcher,
};
use futures::Stream;
use rr_core::directory::directory_page_size;
use rr_core::{
    DirectoryError, NodeDescriptor, NodeDirectory, NodeDirectoryChange, NodeDirectoryPage,
    NodeDirectoryRecord, NodeDirectoryRegistration,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(15);
const WATCH_CHANNEL_CAPACITY: usize = 256;
const WATCH_BACKOFF_INITIAL: Duration = Duration::from_millis(100);
const WATCH_BACKOFF_MAX: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct EtcdNodeDirectory {
    client: Client,
    cluster_name: String,
    lease_ttl: Duration,
}

impl std::fmt::Debug for EtcdNodeDirectory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EtcdNodeDirectory")
            .field("cluster_name", &self.cluster_name)
            .field("lease_ttl", &self.lease_ttl)
            .finish_non_exhaustive()
    }
}

impl EtcdNodeDirectory {
    pub async fn new(endpoints: Vec<String>, cluster_name: String) -> Result<Self, DirectoryError> {
        Self::connect_with_options(endpoints, cluster_name, None).await
    }

    pub async fn new_with_options(
        endpoints: Vec<String>,
        cluster_name: String,
        options: ConnectOptions,
    ) -> Result<Self, DirectoryError> {
        Self::connect_with_options(endpoints, cluster_name, Some(options)).await
    }

    async fn connect_with_options(
        endpoints: Vec<String>,
        cluster_name: String,
        options: Option<ConnectOptions>,
    ) -> Result<Self, DirectoryError> {
        if endpoints.is_empty() {
            return Err(DirectoryError::InvalidData(
                "etcd endpoints cannot be empty".into(),
            ));
        }
        let cluster_name = crate::validate_cluster_id(&cluster_name)
            .map_err(|error| DirectoryError::InvalidData(error.to_string()))?;
        let mut client = Client::connect(endpoints, options)
            .await
            .map_err(|error| map_etcd_error("connect node directory", error))?;
        client
            .status()
            .await
            .map_err(|error| map_etcd_error("check node directory status", error))?;
        Ok(Self {
            client,
            cluster_name,
            lease_ttl: DEFAULT_LEASE_TTL,
        })
    }

    pub fn with_lease_ttl(mut self, lease_ttl: Duration) -> Self {
        self.lease_ttl = lease_ttl;
        self
    }

    async fn fetch_all(&self) -> Result<(BTreeMap<u64, NodeDirectoryRecord>, u64), DirectoryError> {
        fetch_all(&self.client, &self.cluster_name).await
    }
}

fn map_etcd_error(context: &str, error: etcd_client::Error) -> DirectoryError {
    DirectoryError::ProviderUnavailable(format!("{context}: {error}"))
}

fn node_directory_prefix(cluster_name: &str) -> String {
    format!("/rr/{cluster_name}/directory/nodes/")
}

fn node_directory_key(cluster_name: &str, node_id: u64) -> String {
    format!("{}{node_id}", node_directory_prefix(cluster_name))
}

fn checked_revision(revision: i64) -> Result<u64, DirectoryError> {
    u64::try_from(revision)
        .map_err(|_| DirectoryError::InvalidData(format!("negative etcd revision {revision}")))
}

fn decode_descriptor(expected: u64, bytes: &[u8]) -> Result<NodeDescriptor, DirectoryError> {
    let descriptor: NodeDescriptor = serde_json::from_slice(bytes)
        .map_err(|error| DirectoryError::InvalidData(format!("decode node descriptor: {error}")))?;
    if descriptor.node_id != expected {
        return Err(DirectoryError::NodeIdMismatch {
            expected,
            actual: descriptor.node_id,
        });
    }
    descriptor.validate()?;
    Ok(descriptor)
}

fn node_id_from_key(cluster_name: &str, key: &str) -> Result<u64, DirectoryError> {
    key.strip_prefix(&node_directory_prefix(cluster_name))
        .ok_or_else(|| DirectoryError::InvalidData(format!("invalid directory key {key}")))?
        .parse()
        .map_err(|error| {
            DirectoryError::InvalidData(format!("invalid directory key {key}: {error}"))
        })
}

fn decode_record(cluster_name: &str, kv: &KeyValue) -> Result<NodeDirectoryRecord, DirectoryError> {
    let key = kv.key_str().map_err(|error| {
        DirectoryError::InvalidData(format!("directory key is not UTF-8: {error}"))
    })?;
    let node_id = node_id_from_key(cluster_name, key)?;
    Ok(NodeDirectoryRecord {
        descriptor: decode_descriptor(node_id, kv.value())?,
        revision: checked_revision(kv.mod_revision())?,
        lease_id: kv.lease(),
    })
}

fn encode_cursor(revision: u64, after_node_id: u64) -> String {
    format!("v1:{revision}:{after_node_id}")
}

fn decode_cursor(cursor: &str) -> Result<(u64, u64), DirectoryError> {
    let invalid = || DirectoryError::InvalidCursor(cursor.to_string());
    let mut parts = cursor.split(':');
    let (Some("v1"), Some(revision), Some(node_id), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid());
    };
    let revision = revision.parse().map_err(|_| invalid())?;
    let node_id = node_id.parse().map_err(|_| invalid())?;
    if revision == 0 || node_id == 0 {
        return Err(invalid());
    }
    Ok((revision, node_id))
}

fn directory_range_end(cluster_name: &str) -> String {
    let prefix = node_directory_prefix(cluster_name);
    format!(
        "{}0",
        prefix.strip_suffix('/').expect("prefix ends in slash")
    )
}

fn diff_records(
    baseline: &BTreeMap<u64, NodeDirectoryRecord>,
    fresh: &BTreeMap<u64, NodeDirectoryRecord>,
    revision: u64,
) -> Vec<NodeDirectoryChange> {
    let mut changes = Vec::new();
    for node_id in baseline.keys() {
        if !fresh.contains_key(node_id) {
            changes.push(NodeDirectoryChange::Remove {
                node_id: *node_id,
                revision,
            });
        }
    }
    for (node_id, record) in fresh {
        if baseline.get(node_id) != Some(record) {
            changes.push(NodeDirectoryChange::Upsert(Box::new(record.clone())));
        }
    }
    changes
}

async fn fetch_all(
    client: &Client,
    cluster_name: &str,
) -> Result<(BTreeMap<u64, NodeDirectoryRecord>, u64), DirectoryError> {
    let response = client
        .clone()
        .get(
            node_directory_prefix(cluster_name),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .map_err(|error| map_etcd_error("list node directory", error))?;
    let revision = checked_revision(
        response
            .header()
            .map(|header| header.revision())
            .unwrap_or(0),
    )?;
    let mut records = BTreeMap::new();
    for kv in response.kvs() {
        let record = decode_record(cluster_name, kv)?;
        records.insert(record.descriptor.node_id, record);
    }
    Ok((records, revision))
}

#[async_trait]
impl NodeDirectory for EtcdNodeDirectory {
    async fn get(&self, node_id: u64) -> Result<Option<NodeDirectoryRecord>, DirectoryError> {
        let response = self
            .client
            .clone()
            .get(node_directory_key(&self.cluster_name, node_id), None)
            .await
            .map_err(|error| map_etcd_error("get node descriptor", error))?;
        response
            .kvs()
            .first()
            .map(|kv| decode_record(&self.cluster_name, kv))
            .transpose()
    }

    async fn list(
        &self,
        cursor: Option<&str>,
        page_size: Option<usize>,
    ) -> Result<NodeDirectoryPage, DirectoryError> {
        let page_size = directory_page_size(page_size)?;
        let decoded_cursor = cursor.map(decode_cursor).transpose()?;
        let (key, mut options) = match decoded_cursor {
            Some((revision, after_node_id)) => (
                format!(
                    "{}\0",
                    node_directory_key(&self.cluster_name, after_node_id)
                ),
                GetOptions::new()
                    .with_range(directory_range_end(&self.cluster_name))
                    .with_revision(i64::try_from(revision).map_err(|_| {
                        DirectoryError::InvalidCursor("revision exceeds etcd i64".into())
                    })?),
            ),
            None => (
                node_directory_prefix(&self.cluster_name),
                GetOptions::new().with_prefix(),
            ),
        };
        options = options
            .with_sort(SortTarget::Key, SortOrder::Ascend)
            .with_limit(page_size as i64 + 1);
        let response = self
            .client
            .clone()
            .get(key, Some(options))
            .await
            .map_err(|error| map_etcd_error("page node directory", error))?;
        let revision = match decoded_cursor {
            Some((revision, _)) => revision,
            None => checked_revision(
                response
                    .header()
                    .map(|header| header.revision())
                    .unwrap_or(0),
            )?,
        };
        let mut records = response
            .kvs()
            .iter()
            .map(|kv| decode_record(&self.cluster_name, kv))
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = records.len() > page_size;
        records.truncate(page_size);
        let next_cursor = if has_more {
            records
                .last()
                .map(|record| encode_cursor(revision, record.descriptor.node_id))
        } else {
            None
        };
        Ok(NodeDirectoryPage {
            records,
            next_cursor,
            revision,
        })
    }

    async fn watch(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = NodeDirectoryChange> + Send>>, DirectoryError> {
        let mut client = self.client.clone();
        let prefix = node_directory_prefix(&self.cluster_name);
        let (watcher, stream) = client
            .watch(prefix.clone(), Some(directory_watch_options(0)))
            .await
            .map_err(|error| map_etcd_error("watch node directory", error))?;
        let (sender, receiver) = mpsc::channel(WATCH_CHANNEL_CAPACITY);
        let directory = self.clone();
        let handle = tokio::spawn(async move {
            directory
                .watch_forever(prefix, watcher, stream, sender)
                .await;
        });
        Ok(Box::pin(DirectoryWatchStreamGuard {
            inner: ReceiverStream::new(receiver),
            handle,
        }))
    }

    async fn register(
        &self,
        descriptor: NodeDescriptor,
    ) -> Result<Box<dyn NodeDirectoryRegistration>, DirectoryError> {
        descriptor.validate()?;
        if self.lease_ttl.is_zero() {
            return Err(DirectoryError::InvalidData(
                "directory lease TTL must be greater than zero".into(),
            ));
        }
        let ttl = self.lease_ttl.as_secs().max(1).min(i64::MAX as u64) as i64;
        let lease = self
            .client
            .clone()
            .lease_grant(ttl, None)
            .await
            .map_err(|error| map_etcd_error("grant directory lease", error))?;
        let lease_id = lease.id();
        let key = node_directory_key(&self.cluster_name, descriptor.node_id);
        let encoded = serde_json::to_vec(&descriptor).map_err(|error| {
            DirectoryError::InvalidDescriptor(format!("encode node descriptor: {error}"))
        })?;
        let transaction = Txn::new()
            .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(
                key,
                encoded,
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        let response = self
            .client
            .clone()
            .txn(transaction)
            .await
            .map_err(|error| map_etcd_error("register node descriptor", error))?;
        if !response.succeeded() {
            self.client
                .clone()
                .lease_revoke(lease_id)
                .await
                .map_err(|error| map_etcd_error("release conflicting directory lease", error))?;
            return Err(DirectoryError::AlreadyExists {
                node_id: descriptor.node_id,
            });
        }
        let revision = checked_revision(
            response
                .header()
                .map(|header| header.revision())
                .unwrap_or(0),
        )?;
        Ok(Box::new(EtcdNodeDirectoryRegistration {
            client: self.client.clone(),
            record: NodeDirectoryRecord {
                descriptor,
                revision,
                lease_id,
            },
            revoked: AtomicBool::new(false),
        }))
    }
}

struct EtcdNodeDirectoryRegistration {
    client: Client,
    record: NodeDirectoryRecord,
    revoked: AtomicBool,
}

#[async_trait]
impl NodeDirectoryRegistration for EtcdNodeDirectoryRegistration {
    fn record(&self) -> &NodeDirectoryRecord {
        &self.record
    }

    async fn heartbeat(&self) -> Result<(), DirectoryError> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(DirectoryError::LeaseExpired {
                node_id: self.record.descriptor.node_id,
            });
        }
        let (mut keeper, mut stream) = self
            .client
            .clone()
            .lease_keep_alive(self.record.lease_id)
            .await
            .map_err(|error| map_etcd_error("open directory lease heartbeat", error))?;
        keeper
            .keep_alive()
            .await
            .map_err(|error| map_etcd_error("send directory lease heartbeat", error))?;
        let response = stream
            .message()
            .await
            .map_err(|error| map_etcd_error("receive directory lease heartbeat", error))?
            .ok_or(DirectoryError::LeaseExpired {
                node_id: self.record.descriptor.node_id,
            })?;
        if response.ttl() <= 0 {
            self.revoked.store(true, Ordering::Release);
            return Err(DirectoryError::LeaseExpired {
                node_id: self.record.descriptor.node_id,
            });
        }
        Ok(())
    }

    async fn revoke(&self) -> Result<(), DirectoryError> {
        if self.revoked.load(Ordering::Acquire) {
            return Ok(());
        }
        self.client
            .clone()
            .lease_revoke(self.record.lease_id)
            .await
            .map_err(|error| map_etcd_error("revoke directory lease", error))?;
        self.revoked.store(true, Ordering::Release);
        Ok(())
    }
}

fn directory_watch_options(last_seen_revision: i64) -> WatchOptions {
    let options = WatchOptions::new().with_prefix().with_prev_key();
    match last_seen_revision.checked_add(1) {
        Some(revision) if last_seen_revision > 0 => options.with_start_revision(revision),
        _ => options,
    }
}

struct DirectoryWatchStreamGuard {
    inner: ReceiverStream<NodeDirectoryChange>,
    handle: JoinHandle<()>,
}

impl Stream for DirectoryWatchStreamGuard {
    type Item = NodeDirectoryChange;

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(context)
    }
}

impl Drop for DirectoryWatchStreamGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl EtcdNodeDirectory {
    async fn watch_forever(
        &self,
        prefix: String,
        mut watcher: Watcher,
        mut stream: WatchStream,
        sender: mpsc::Sender<NodeDirectoryChange>,
    ) {
        let mut baseline = BTreeMap::new();
        let mut last_seen_revision = 0_i64;
        let mut backoff = WATCH_BACKOFF_INITIAL;
        loop {
            let mut compacted = false;
            loop {
                let response = match stream.message().await {
                    Ok(Some(response)) => response,
                    Ok(None) => break,
                    Err(error) => {
                        tracing::warn!(target: "rr::config::etcd", %error, "node directory watch failed");
                        break;
                    }
                };
                if let Some(header) = response.header() {
                    last_seen_revision = last_seen_revision.max(header.revision());
                }
                if response.canceled() {
                    compacted = response.compact_revision() > 0;
                    break;
                }
                let response_revision = response
                    .header()
                    .map(|header| header.revision())
                    .unwrap_or(last_seen_revision);
                for event in response.events() {
                    let Some(kv) = event.kv() else {
                        tracing::warn!(target: "rr::config::etcd", "directory watch event missing kv");
                        continue;
                    };
                    let change = match event.event_type() {
                        EventType::Put => decode_record(&self.cluster_name, kv).map(|record| {
                            baseline.insert(record.descriptor.node_id, record.clone());
                            NodeDirectoryChange::Upsert(Box::new(record))
                        }),
                        EventType::Delete => kv
                            .key_str()
                            .map_err(|error| {
                                DirectoryError::InvalidData(format!(
                                    "directory key is not UTF-8: {error}"
                                ))
                            })
                            .and_then(|key| node_id_from_key(&self.cluster_name, key))
                            .and_then(|node_id| {
                                baseline.remove(&node_id);
                                checked_revision(response_revision).map(|revision| {
                                    NodeDirectoryChange::Remove { node_id, revision }
                                })
                            }),
                    };
                    match change {
                        Ok(change) => {
                            if sender.send(change).await.is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(target: "rr::config::etcd", %error, "invalid directory watch event");
                        }
                    }
                }
            }

            if compacted {
                let (fresh, revision) = loop {
                    match self.fetch_all().await {
                        Ok(scanned) => break scanned,
                        Err(error) => {
                            tracing::warn!(target: "rr::config::etcd", %error, "directory compaction resync failed");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(WATCH_BACKOFF_MAX);
                        }
                    }
                };
                let changes = diff_records(&baseline, &fresh, revision);
                baseline = fresh;
                last_seen_revision = revision as i64;
                for change in changes {
                    if sender.send(change).await.is_err() {
                        return;
                    }
                }
            }

            if let Err(error) = watcher.cancel().await {
                tracing::debug!(target: "rr::config::etcd", %error, "cancel of dead directory watch failed");
            }
            let (new_watcher, new_stream) = loop {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCH_BACKOFF_MAX);
                match self
                    .client
                    .clone()
                    .watch(
                        prefix.clone(),
                        Some(directory_watch_options(last_seen_revision)),
                    )
                    .await
                {
                    Ok(watch) => break watch,
                    Err(error) => {
                        tracing::warn!(target: "rr::config::etcd", %error, "directory watch reconnect failed");
                    }
                }
            };
            watcher = new_watcher;
            stream = new_stream;
            backoff = WATCH_BACKOFF_INITIAL;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rr_core::{DirectoryError, NodeDescriptor, NodeDirectoryChange, NodeDirectoryRecord};

    use super::{
        decode_cursor, decode_descriptor, diff_records, encode_cursor, node_directory_key,
        DEFAULT_LEASE_TTL,
    };

    fn descriptor(node_id: u64) -> NodeDescriptor {
        NodeDescriptor {
            node_id,
            client_addr: format!("10.0.0.{node_id}:5000"),
            admin_addr: format!("10.0.0.{node_id}:5001"),
            observability_addr: format!("10.0.0.{node_id}:5002"),
            advertise_addr: format!("10.0.0.{node_id}:6000"),
            environment: "test".into(),
            region: None,
            zone: None,
            rack: None,
            boot_id: format!("boot-{node_id}"),
        }
    }

    fn record(node_id: u64, revision: u64) -> NodeDirectoryRecord {
        NodeDirectoryRecord {
            descriptor: descriptor(node_id),
            revision,
            lease_id: 99,
        }
    }

    #[test]
    fn directory_key_uses_dedicated_namespace_and_default_ttl() {
        assert_eq!(
            node_directory_key("cluster-a", 42),
            "/rr/cluster-a/directory/nodes/42"
        );
        assert_eq!(DEFAULT_LEASE_TTL.as_secs(), 15);
    }

    #[test]
    fn descriptor_decode_rejects_unknown_fields_and_node_id_mismatch() {
        let mut unknown = serde_json::to_value(descriptor(7)).unwrap();
        unknown["unexpected"] = serde_json::Value::Bool(true);
        assert!(matches!(
            decode_descriptor(7, &serde_json::to_vec(&unknown).unwrap()),
            Err(DirectoryError::InvalidData(_))
        ));

        assert!(matches!(
            decode_descriptor(8, &serde_json::to_vec(&descriptor(7)).unwrap()),
            Err(DirectoryError::NodeIdMismatch {
                expected: 8,
                actual: 7
            })
        ));
    }

    #[test]
    fn revision_cursor_round_trips_and_rejects_invalid_values() {
        let cursor = encode_cursor(41, 10);
        assert_eq!(decode_cursor(&cursor).unwrap(), (41, 10));
        for invalid in ["", "not-a-cursor", "v1:0:1", "v1:1:0", "v2:1:1"] {
            assert!(decode_cursor(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn compaction_diff_emits_sorted_upserts_and_removals() {
        let baseline = BTreeMap::from([(1, record(1, 2)), (2, record(2, 3))]);
        let mut changed = record(2, 3);
        changed.descriptor.admin_addr = "10.0.0.2:7001".into();
        let fresh = BTreeMap::from([(2, changed), (3, record(3, 4))]);

        let changes = diff_records(&baseline, &fresh, 10);
        assert_eq!(changes.len(), 3);
        assert!(matches!(
            &changes[0],
            NodeDirectoryChange::Remove {
                node_id: 1,
                revision: 10
            }
        ));
        assert!(matches!(
            &changes[1],
            NodeDirectoryChange::Upsert(record) if record.descriptor.node_id == 2
        ));
        assert!(matches!(
            &changes[2],
            NodeDirectoryChange::Upsert(record) if record.descriptor.node_id == 3
        ));
    }
}
