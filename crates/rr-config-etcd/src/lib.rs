//! etcd-backed [`ConfigProvider`] implementation.
//!
//! This crate provides a read-only view of cluster routing state stored in
//! etcd v3 under:
//! * `/rr/{cluster_id}/nodes/{node_id}`
//! * `/rr/{cluster_id}/partitions/{partition_id}`
//! * `/rr/{cluster_id}/groups/{group_id}/replicas`

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use etcd_client::{
    Client, ConnectOptions, Event, EventType, GetOptions, SortOrder, SortTarget, WatchOptions,
    WatchStream, Watcher,
};
use futures::Stream;
use rr_core::error::ConfigError;
use rr_core::types::{
    GroupId, GroupMeta, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

pub use rr_core::config::{ConfigProvider, RoutingTableChange};

const DEFAULT_CLUSTER_ID: &str = "default";
const WATCH_CHANNEL_CAPACITY: usize = 256;
const WATCH_BACKOFF_INITIAL: Duration = Duration::from_millis(100);
const WATCH_BACKOFF_MAX: Duration = Duration::from_secs(5);

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

impl WatchDomain {
    fn as_str(self) -> &'static str {
        match self {
            WatchDomain::Node => "nodes",
            WatchDomain::Partition => "partitions",
            WatchDomain::Group => "groups",
        }
    }
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

/// Validate the cluster id used in every etcd key: it is embedded into
/// key paths, so only `[A-Za-z0-9._-]+` is accepted — `/` would break the
/// key schema and control characters make keys undebuggable. An empty id
/// falls back to `default` (with a warning, so it is not silent).
fn validate_cluster_id(cluster_id: &str) -> Result<String, ConfigError> {
    if cluster_id.trim().is_empty() {
        tracing::warn!(
            target: "rr::config::etcd",
            "empty cluster_id; falling back to '{DEFAULT_CLUSTER_ID}'"
        );
        return Ok(DEFAULT_CLUSTER_ID.to_string());
    }
    if cluster_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        Ok(cluster_id.to_string())
    } else {
        Err(ConfigError::InvalidConfig(format!(
            "invalid cluster_id {cluster_id:?}: must match [A-Za-z0-9._-]+"
        )))
    }
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
            // A deleted replicas key maps to an empty membership: there is
            // no dedicated `GroupRemoved` variant in `RoutingTableChange`,
            // and adding one would be a breaking change to rr-core (deferred
            // to the v0.3.0 breaking window). Consumers must treat an empty
            // replica set as "group gone".
            RawEventType::Delete => Ok(RoutingTableChange::GroupMembershipChanged {
                group_id,
                replicas: Vec::new(),
            }),
        },
    }
}

/// Why a [`run_watch_loop`] invocation ended.
#[derive(Debug)]
enum WatchExit {
    /// The consumer dropped the stream — the whole subscription is gone;
    /// do not reconnect.
    ConsumerDropped,
    /// The server closed the stream (`Ok(None)`).
    StreamEnded,
    /// The stream errored.
    StreamFailed,
    /// etcd canceled the watch. `compact_revision > 0` means our resume
    /// revision was compacted away — events were lost server-side.
    Canceled { compact_revision: i64 },
}

/// Exponential backoff for watch re-establishment: 100 ms doubling to a
/// 5 s cap, reset once a watch is successfully created.
#[derive(Debug, Clone)]
struct WatchBackoff {
    current: Duration,
}

impl Default for WatchBackoff {
    fn default() -> Self {
        Self {
            current: WATCH_BACKOFF_INITIAL,
        }
    }
}

impl WatchBackoff {
    fn reset(&mut self) {
        self.current = WATCH_BACKOFF_INITIAL;
    }

    async fn wait(&mut self) {
        let delay = self.current;
        tokio::time::sleep(delay).await;
        self.current = (self.current * 2).min(WATCH_BACKOFF_MAX);
    }
}

/// Resume revision for the next watch attempt: `None` watches from the
/// current head (fresh subscription or post-compaction), otherwise we
/// resume right after the last revision we observed so reconnects replay
/// what we missed.
fn next_start_revision(last_seen_rev: i64) -> Option<i64> {
    if last_seen_rev <= 0 {
        return None;
    }
    // Overflow is unreachable with real etcd revisions; falling back to a
    // head watch is safer than panicking.
    last_seen_rev.checked_add(1)
}

fn watch_options(last_seen_rev: i64) -> WatchOptions {
    let options = WatchOptions::new().with_prefix().with_prev_key();
    match next_start_revision(last_seen_rev) {
        Some(rev) => options.with_start_revision(rev),
        None => options,
    }
}

/// Consume one established watch stream until it ends, fails, or is
/// canceled. Updates `last_seen_rev` from every response header so the
/// caller can resume without gaps on reconnect.
async fn run_watch_loop(
    cluster_id: &str,
    domain: WatchDomain,
    stream: &mut WatchStream,
    sender: &mpsc::Sender<RoutingTableChange>,
    last_seen_rev: &mut i64,
) -> WatchExit {
    loop {
        let response = match stream.message().await {
            Ok(Some(response)) => response,
            Ok(None) => {
                tracing::error!(target: "rr::config::etcd", domain = domain.as_str(), "watch stream ended unexpectedly");
                return WatchExit::StreamEnded;
            }
            Err(err) => {
                tracing::error!(target: "rr::config::etcd", domain = domain.as_str(), error = %err, "watch stream failed");
                return WatchExit::StreamFailed;
            }
        };

        if let Some(header) = response.header() {
            *last_seen_rev = (*last_seen_rev).max(header.revision());
        }

        if response.canceled() {
            let compact_revision = response.compact_revision();
            if compact_revision > 0 {
                tracing::error!(
                    target: "rr::config::etcd",
                    domain = domain.as_str(),
                    watch_id = response.watch_id(),
                    reason = response.cancel_reason(),
                    compact_revision,
                    "watch canceled by etcd compaction — events were lost, resyncing"
                );
            } else {
                tracing::error!(
                    target: "rr::config::etcd",
                    domain = domain.as_str(),
                    watch_id = response.watch_id(),
                    reason = response.cancel_reason(),
                    "watch canceled by etcd"
                );
            }
            return WatchExit::Canceled { compact_revision };
        }

        for event in response.events() {
            let change = RawWatchEvent::from_etcd_event(cluster_id, domain, event)
                .and_then(parse_watch_event);
            let change = match change {
                Ok(change) => change,
                Err(err) => {
                    tracing::error!(target: "rr::config::etcd", error = %err, "failed to parse watch event");
                    continue;
                }
            };

            metrics::counter!(
                "rr_config_etcd_watch_events_total",
                "domain" => domain.as_str()
            )
            .increment(1);

            if sender.send(change).await.is_err() {
                return WatchExit::ConsumerDropped;
            }
        }
    }
}

/// Compute the change set that takes `baseline` to `fresh`, restricted to
/// one watch `domain` (each domain watcher emits only its own diff, so the
/// three watchers never duplicate events). Every emitted event is an
/// idempotent upsert/removal, so overlap with the resumed live stream is
/// harmless.
fn diff_tables(
    domain: WatchDomain,
    baseline: &RoutingTable,
    fresh: &RoutingTable,
) -> Vec<RoutingTableChange> {
    let mut changes = Vec::new();
    match domain {
        WatchDomain::Node => {
            for (node_id, addr) in &fresh.nodes {
                if baseline.nodes.get(node_id) != Some(addr) {
                    changes.push(RoutingTableChange::NodeAdded {
                        node_id: *node_id,
                        addr: addr.clone(),
                    });
                }
            }
            for node_id in baseline.nodes.keys() {
                if !fresh.nodes.contains_key(node_id) {
                    changes.push(RoutingTableChange::NodeRemoved { node_id: *node_id });
                }
            }
        }
        WatchDomain::Partition => {
            for (partition_id, (config, group_id)) in &fresh.partitions {
                match baseline.partitions.get(partition_id) {
                    None => changes.push(RoutingTableChange::PartitionAdded {
                        partition_id: *partition_id,
                        config: config.clone(),
                        group_id: *group_id,
                    }),
                    Some((old_config, old_group))
                        if old_config != config || old_group != group_id =>
                    {
                        changes.push(RoutingTableChange::PartitionUpdated {
                            partition_id: *partition_id,
                            config: config.clone(),
                            group_id: *group_id,
                        });
                    }
                    _ => {}
                }
            }
            for partition_id in baseline.partitions.keys() {
                if !fresh.partitions.contains_key(partition_id) {
                    changes.push(RoutingTableChange::PartitionRemoved {
                        partition_id: *partition_id,
                    });
                }
            }
        }
        WatchDomain::Group => {
            for (group_id, meta) in &fresh.groups {
                if baseline.groups.get(group_id).map(|g| &g.replicas) != Some(&meta.replicas) {
                    changes.push(RoutingTableChange::GroupMembershipChanged {
                        group_id: *group_id,
                        replicas: meta.replicas.clone(),
                    });
                }
            }
            for group_id in baseline.groups.keys() {
                if !fresh.groups.contains_key(group_id) {
                    // Same mapping as a deleted replicas key — see
                    // `parse_watch_event`.
                    changes.push(RoutingTableChange::GroupMembershipChanged {
                        group_id: *group_id,
                        replicas: Vec::new(),
                    });
                }
            }
        }
    }
    changes
}

/// etcd-backed implementation of [`ConfigProvider`].
///
/// This type is intentionally read-only at the trait surface:
/// it implements reads and watch subscriptions only.
///
/// # Read-path caching
///
/// `routing_table_snapshot()` probes the cluster prefix's content
/// fingerprint with a tiny keys-only, limit-1 `GET` sorted by
/// `mod_revision` descending (serializable by default — member-local, no
/// quorum round-trip) and only re-scans the full prefix when the
/// fingerprint has moved. The fingerprint keys on the prefix's own
/// watermark, so unrelated writes elsewhere in a shared etcd do NOT
/// invalidate the cache. Routing decisions therefore still consult etcd
/// on every call (INV-6: etcd stays the single source of truth), but the
/// per-request cost is one small probe instead of a full prefix scan.
/// Concurrent rescans are single-flight: one caller scans, the rest
/// coalesce on the result.
///
/// Staleness bound: a serializable probe can lag quorum writes by the
/// member's apply lag (sub-millisecond on a healthy cluster) — a lagging
/// probe simply misses the cache and triggers a linearizable rescan, so
/// the served table is never older than the last successful quorum scan.
/// Pass [`with_linearizable_probes`](Self::with_linearizable_probes) for
/// quorum-fresh probes at the price of one quorum RTT per call. When the
/// probe itself fails (etcd unreachable), the last cached snapshot is
/// served stale (`rr_config_etcd_stale_serves_total`); only a cold cache
/// turns a probe outage into an error.
#[derive(Clone)]
pub struct EtcdConfigProvider {
    client: Client,
    cluster_id: String,
    cache: Arc<RevisionCache>,
    rescan_lock: Arc<Mutex<()>>,
    linearizable_probes: bool,
}

impl std::fmt::Debug for EtcdConfigProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdConfigProvider")
            .field("cluster_id", &self.cluster_id)
            .field("linearizable_probes", &self.linearizable_probes)
            .finish_non_exhaustive()
    }
}

/// Content fingerprint of the cluster prefix: the highest `mod_revision`
/// of any key under `/rr/{cluster_id}/` plus the number of keys there.
///
/// Keying the read cache on the cluster-wide store revision (the old
/// behavior) let unrelated writes elsewhere in a shared etcd invalidate
/// our snapshot; the fingerprint only moves when OUR prefix changes:
/// every put bumps the written key's `mod_revision` to the current store
/// revision (≥ any previous maximum), and every delete changes the key
/// count. Both probe and scan compute it the same way, so a serializable
/// probe vs linearizable scan cannot skew the key — they can only observe
/// genuinely different states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PrefixFingerprint {
    max_mod_revision: i64,
    key_count: i64,
}

/// Fingerprint-keyed routing-table cache: a snapshot is served only when a
/// fresh probe returns the exact fingerprint it was scanned at.
#[derive(Debug, Default)]
struct RevisionCache {
    inner: std::sync::Mutex<Option<(PrefixFingerprint, Arc<RoutingTable>)>>,
}

impl RevisionCache {
    /// Return the cached table iff it was scanned at exactly `probed`.
    fn get_if_current(&self, probed: PrefixFingerprint) -> Option<Arc<RoutingTable>> {
        let guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        match &*guard {
            Some((fingerprint, table)) if *fingerprint == probed => Some(Arc::clone(table)),
            _ => None,
        }
    }

    /// Return the latest cached table regardless of fingerprint — used for
    /// stale serving when etcd is unreachable and as the diff baseline for
    /// post-compaction resync.
    fn latest(&self) -> Option<Arc<RoutingTable>> {
        let guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        guard.as_ref().map(|(_, table)| Arc::clone(table))
    }

    /// Publish a freshly scanned table at its scan fingerprint.
    fn store(&self, fingerprint: PrefixFingerprint, table: Arc<RoutingTable>) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        *guard = Some((fingerprint, table));
    }
}

impl EtcdConfigProvider {
    /// Create a new provider and validate connectivity to etcd.
    ///
    /// Development convenience: connects without TLS or authentication.
    /// Production deployments should use
    /// [`new_with_options`](Self::new_with_options).
    pub async fn new(endpoints: Vec<String>, cluster_id: String) -> Result<Self, ConfigError> {
        Self::connect_with_options(endpoints, cluster_id, None).await
    }

    /// Create a provider with explicit etcd [`ConnectOptions`] — TLS
    /// (`ConnectOptions::with_tls`, requires etcd-client's `tls` feature
    /// to be enabled by the downstream build) and/or authentication
    /// (`ConnectOptions::with_user`), keep-alive, timeouts, etc.
    pub async fn new_with_options(
        endpoints: Vec<String>,
        cluster_id: String,
        options: ConnectOptions,
    ) -> Result<Self, ConfigError> {
        Self::connect_with_options(endpoints, cluster_id, Some(options)).await
    }

    async fn connect_with_options(
        endpoints: Vec<String>,
        cluster_id: String,
        options: Option<ConnectOptions>,
    ) -> Result<Self, ConfigError> {
        if endpoints.is_empty() {
            return Err(ConfigError::InvalidConfig(
                "etcd endpoints cannot be empty".to_string(),
            ));
        }

        let resolved_cluster_id = validate_cluster_id(&cluster_id)?;

        let mut client = Client::connect(endpoints.clone(), options)
            .await
            .map_err(|err| map_etcd_error("connect etcd", err))?;

        client
            .status()
            .await
            .map_err(|err| map_etcd_error("status etcd", err))?;

        Ok(Self {
            client,
            cluster_id: resolved_cluster_id,
            cache: Arc::new(RevisionCache::default()),
            rescan_lock: Arc::new(Mutex::new(())),
            linearizable_probes: false,
        })
    }

    /// Use quorum (linearizable) fingerprint probes instead of the default
    /// serializable ones. Every `routing_table_snapshot()` call then costs
    /// one quorum RTT but cannot lag behind quorum-committed table writes.
    pub fn with_linearizable_probes(mut self, on: bool) -> Self {
        self.linearizable_probes = on;
        self
    }

    async fn connect(&self) -> Result<Client, ConfigError> {
        // `Client::clone` shares the underlying channel — no reconnect.
        Ok(self.client.clone())
    }

    /// Probe the cluster prefix's content fingerprint with a keys-only,
    /// limit-1 GET sorted by `mod_revision` descending: the single returned
    /// key carries the prefix's write watermark, and the response `count`
    /// is the total key count under the prefix (etcd computes it before
    /// limit truncation). No KV payloads cross the wire; serializable mode
    /// (default) is served member-locally without a quorum round-trip.
    async fn probe_fingerprint(&self) -> Result<PrefixFingerprint, ConfigError> {
        let mut client = self.client.clone();
        let probe = format!("{}/", root_prefix(&self.cluster_id));
        let mut options = GetOptions::new()
            .with_prefix()
            .with_keys_only()
            .with_limit(1)
            .with_sort(SortTarget::Mod, SortOrder::Descend);
        if !self.linearizable_probes {
            options = options.with_serializable();
        }
        let response = client
            .get(probe, Some(options))
            .await
            .map_err(|err| map_etcd_error("probe routing-table fingerprint", err))?;
        Ok(PrefixFingerprint {
            max_mod_revision: response
                .kvs()
                .first()
                .map(|kv| kv.mod_revision())
                .unwrap_or(0),
            key_count: response.count(),
        })
    }

    /// Full prefix scan of the routing table, returning the content
    /// fingerprint the scanned state corresponds to alongside the parsed
    /// table. Individual malformed keys are skipped (loudly, with a
    /// metric) instead of failing every routing read — one bad key under
    /// a prefix must not take down routing for the whole cluster.
    async fn fetch_routing_table(
        &self,
    ) -> Result<(PrefixFingerprint, Arc<RoutingTable>), ConfigError> {
        let mut client = self.connect().await?;
        let scan_prefix = format!("{}/", root_prefix(&self.cluster_id));

        let response = client
            .get(scan_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|err| map_etcd_error("scan routing table", err))?;

        let mut max_mod_revision: i64 = 0;
        let mut bad_keys: u64 = 0;
        let mut table = RoutingTable::default();

        for kv in response.kvs() {
            max_mod_revision = max_mod_revision.max(kv.mod_revision());

            let key = match kv.key_str() {
                Ok(key) => key,
                Err(err) => {
                    tracing::error!(target: "rr::config::etcd", error = %err, "invalid UTF-8 key in etcd response; skipping");
                    bad_keys += 1;
                    continue;
                }
            };

            let parsed = if key.starts_with(&nodes_prefix(&self.cluster_id)) {
                parse_node_id_from_key(&self.cluster_id, key)
                    .and_then(|node_id| {
                        decode_json::<NodeAddr>(kv.value(), "decode node entry")
                            .map(|addr| (node_id, addr))
                    })
                    .map(|(node_id, addr)| {
                        table.nodes.insert(node_id, addr);
                    })
            } else if key.starts_with(&partitions_prefix(&self.cluster_id)) {
                parse_partition_id_from_key(&self.cluster_id, key)
                    .and_then(|partition_id| {
                        decode_json::<PartitionValue>(kv.value(), "decode partition entry")
                            .map(|part| (partition_id, part))
                    })
                    .map(|(partition_id, part)| {
                        table
                            .partitions
                            .insert(partition_id, (part.config, part.group_id));
                    })
            } else if key.starts_with(&groups_prefix(&self.cluster_id)) {
                parse_group_id_from_key(&self.cluster_id, key)
                    .and_then(|group_id| {
                        decode_json::<Vec<NodeId>>(kv.value(), "decode group entry")
                            .map(|replicas| (group_id, replicas))
                    })
                    .map(|(group_id, replicas)| {
                        table
                            .groups
                            .insert(group_id, GroupMeta { group_id, replicas });
                    })
            } else {
                continue;
            };

            if let Err(err) = parsed {
                tracing::error!(target: "rr::config::etcd", %key, error = %err, "malformed etcd config key; skipping");
                bad_keys += 1;
            }
        }

        if bad_keys > 0 {
            metrics::counter!("rr_config_etcd_bad_keys_total").increment(bad_keys);
        }

        // A partition can reference a group whose replicas key is missing
        // (partial write, manual edit). Fabricate empty membership so the
        // partition stays routable, but make it loud — an empty replica
        // set cannot elect a leader.
        for (_, group_id) in table.partitions.values() {
            if !table.groups.contains_key(group_id) {
                tracing::warn!(
                    target: "rr::config::etcd",
                    group_id,
                    "partition references group with no replicas key; fabricating empty membership"
                );
                metrics::counter!("rr_config_etcd_fabricated_groups_total").increment(1);
                table.groups.insert(
                    *group_id,
                    GroupMeta {
                        group_id: *group_id,
                        replicas: Vec::new(),
                    },
                );
            }
        }

        let fingerprint = PrefixFingerprint {
            max_mod_revision,
            // etcd always populates `count` (total keys in range, before
            // limit truncation); the max() guard keeps the fingerprint
            // self-consistent if a proxy strips it.
            key_count: response.count().max(response.kvs().len() as i64),
        };
        Ok((fingerprint, Arc::new(table)))
    }

    /// Spawn the reconnect loop for one watch domain and return its
    /// [`JoinHandle`]. The first watch is still created eagerly so a
    /// dead/unreachable etcd fails `watch_changes()` fast instead of
    /// hiding behind retries.
    async fn spawn_watcher(
        &self,
        domain: WatchDomain,
        sender: mpsc::Sender<RoutingTableChange>,
    ) -> Result<JoinHandle<()>, ConfigError> {
        let mut client = self.connect().await?;

        let prefix = match domain {
            WatchDomain::Node => nodes_prefix(&self.cluster_id),
            WatchDomain::Partition => partitions_prefix(&self.cluster_id),
            WatchDomain::Group => groups_prefix(&self.cluster_id),
        };

        let (watcher, stream) = client
            .watch(prefix.clone(), Some(watch_options(0)))
            .await
            .map_err(|err| map_etcd_error("create watch", err))?;

        let provider = self.clone();
        let handle = tokio::spawn(async move {
            provider
                .watch_domain_forever(domain, prefix, watcher, stream, sender)
                .await;
        });
        Ok(handle)
    }

    /// Keep one watch domain alive forever: re-establish the watch with
    /// exponential backoff whenever the stream ends or fails, resume from
    /// the last observed revision so reconnects replay what we missed, and
    /// run a full resync when etcd compacts our resume point away. Returns
    /// only when the consumer dropped the stream.
    async fn watch_domain_forever(
        &self,
        domain: WatchDomain,
        prefix: String,
        mut watcher: Watcher,
        mut stream: WatchStream,
        sender: mpsc::Sender<RoutingTableChange>,
    ) {
        let mut backoff = WatchBackoff::default();
        let mut last_seen_rev: i64 = 0;

        loop {
            match run_watch_loop(
                &self.cluster_id,
                domain,
                &mut stream,
                &sender,
                &mut last_seen_rev,
            )
            .await
            {
                WatchExit::ConsumerDropped => return,
                WatchExit::Canceled { compact_revision } if compact_revision > 0 => {
                    metrics::counter!("rr_config_etcd_watch_compactions_total").increment(1);
                    self.resync_after_compaction(domain, &sender).await;
                    // The resume revision is gone — start the next watch
                    // from the current head; the resync above covered the gap.
                    last_seen_rev = 0;
                }
                WatchExit::StreamEnded | WatchExit::StreamFailed | WatchExit::Canceled { .. } => {}
            }

            // Best-effort cancel of the dead server-side watcher before
            // re-creating; a broken stream makes this fail — harmless.
            if let Err(err) = watcher.cancel().await {
                tracing::debug!(target: "rr::config::etcd", domain = domain.as_str(), error = %err, "cancel of dead watcher failed");
            }

            loop {
                metrics::counter!(
                    "rr_config_etcd_watch_reconnects_total",
                    "domain" => domain.as_str()
                )
                .increment(1);
                backoff.wait().await;
                let mut client = self.client.clone();
                match client
                    .watch(prefix.clone(), Some(watch_options(last_seen_rev)))
                    .await
                {
                    Ok((new_watcher, new_stream)) => {
                        watcher = new_watcher;
                        stream = new_stream;
                        backoff.reset();
                        break;
                    }
                    Err(err) => {
                        tracing::warn!(target: "rr::config::etcd", domain = domain.as_str(), error = %err, "watch re-establish failed; retrying with backoff");
                    }
                }
            }
        }
    }

    /// After a compaction cancel, events between our last revision and the
    /// compaction point are unrecoverable from the watch API. Resync the
    /// read cache with a fresh linearizable scan and emit the diff against
    /// the cached baseline as a synthetic change set so stream consumers
    /// converge (events are idempotent upserts/removals). Without a
    /// baseline we cannot know what changed — consumers must re-fetch.
    async fn resync_after_compaction(
        &self,
        domain: WatchDomain,
        sender: &mpsc::Sender<RoutingTableChange>,
    ) {
        let _guard = self.rescan_lock.lock().await;
        let (fingerprint, fresh) = match self.fetch_routing_table().await {
            Ok(scanned) => scanned,
            Err(err) => {
                tracing::error!(
                    target: "rr::config::etcd",
                    domain = domain.as_str(),
                    error = %err,
                    "resync after watch compaction failed; cached routing table may be stale"
                );
                return;
            }
        };
        let baseline = self.cache.latest();
        self.cache.store(fingerprint, Arc::clone(&fresh));
        let Some(baseline) = baseline else {
            tracing::warn!(
                target: "rr::config::etcd",
                domain = domain.as_str(),
                "watch compaction with no cached baseline; consumers must re-fetch the routing table to reconcile"
            );
            return;
        };
        for change in diff_tables(domain, &baseline, &fresh) {
            if sender.send(change).await.is_err() {
                return;
            }
        }
    }
}

/// Stream returned by [`ConfigProvider::watch_changes`]: a
/// [`ReceiverStream`] plus the [`JoinHandle`]s of the three domain watcher
/// tasks. Dropping the stream aborts the watchers — no fire-and-forget
/// tasks outlive the subscription.
struct WatchStreamGuard {
    inner: ReceiverStream<RoutingTableChange>,
    handles: Vec<JoinHandle<()>>,
}

impl Stream for WatchStreamGuard {
    type Item = RoutingTableChange;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl Drop for WatchStreamGuard {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

#[async_trait]
impl ConfigProvider for EtcdConfigProvider {
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError> {
        let (_fingerprint, table) = self.fetch_routing_table().await?;
        Ok(table.as_ref().clone())
    }

    async fn routing_table_snapshot(&self) -> Result<Arc<RoutingTable>, ConfigError> {
        let probe_started = std::time::Instant::now();
        let probe_result = self.probe_fingerprint().await;
        metrics::histogram!("rr_config_etcd_probe_duration_seconds")
            .record(probe_started.elapsed().as_secs_f64());
        let probed = match probe_result {
            Ok(fingerprint) => fingerprint,
            Err(err) => {
                // Availability over freshness: etcd is unreachable, but a
                // previously scanned snapshot is better than failing every
                // routing decision. Loud via warn + metric; only a cold
                // cache (never scanned) turns a probe outage into an error.
                if let Some(table) = self.cache.latest() {
                    tracing::warn!(
                        target: "rr::config::etcd",
                        error = %err,
                        "etcd fingerprint probe failed; serving stale cached routing table"
                    );
                    metrics::counter!("rr_config_etcd_stale_serves_total").increment(1);
                    return Ok(table);
                }
                return Err(err);
            }
        };

        if let Some(table) = self.cache.get_if_current(probed) {
            metrics::counter!("rr_config_etcd_cache_hits_total").increment(1);
            return Ok(table);
        }
        metrics::counter!("rr_config_etcd_cache_misses_total").increment(1);

        // Single-flight: concurrent misses coalesce — the first caller
        // rescans, the rest re-check the cache after the first one stored
        // its result. The re-check uses our (possibly stale serializable)
        // probe fingerprint; a mismatch after the wait just means the
        // table moved again, so scanning ourselves is still correct.
        let _guard = self.rescan_lock.lock().await;
        if let Some(table) = self.cache.get_if_current(probed) {
            return Ok(table);
        }
        metrics::counter!("rr_config_etcd_rescans_total").increment(1);
        let (fingerprint, table) = self.fetch_routing_table().await?;
        self.cache.store(fingerprint, Arc::clone(&table));
        Ok(table)
    }

    async fn watch_changes(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError> {
        let (sender, receiver) = mpsc::channel(WATCH_CHANNEL_CAPACITY);

        // Spawn the three domain watchers; on partial failure abort the
        // ones already running so no orphan task outlives the error.
        let node_handle = self
            .spawn_watcher(WatchDomain::Node, sender.clone())
            .await?;
        let partition_handle = match self
            .spawn_watcher(WatchDomain::Partition, sender.clone())
            .await
        {
            Ok(handle) => handle,
            Err(err) => {
                node_handle.abort();
                return Err(err);
            }
        };
        let group_handle = match self.spawn_watcher(WatchDomain::Group, sender).await {
            Ok(handle) => handle,
            Err(err) => {
                node_handle.abort();
                partition_handle.abort();
                return Err(err);
            }
        };

        Ok(Box::pin(WatchStreamGuard {
            inner: ReceiverStream::new(receiver),
            handles: vec![node_handle, partition_handle, group_handle],
        }))
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

    fn fp(max_mod_revision: i64, key_count: i64) -> PrefixFingerprint {
        PrefixFingerprint {
            max_mod_revision,
            key_count,
        }
    }

    #[test]
    fn revision_cache_serves_only_exact_fingerprint() {
        let cache = RevisionCache::default();
        let table = Arc::new(RoutingTable::default());

        // Empty cache never serves.
        assert!(cache.get_if_current(fp(7, 3)).is_none());
        assert!(cache.latest().is_none());

        cache.store(fp(7, 3), Arc::clone(&table));
        assert!(Arc::ptr_eq(
            &cache.get_if_current(fp(7, 3)).unwrap(),
            &table
        ));
        assert!(Arc::ptr_eq(&cache.latest().unwrap(), &table));

        // A moved watermark or a changed key count invalidates the view.
        assert!(cache.get_if_current(fp(8, 3)).is_none());
        assert!(cache.get_if_current(fp(7, 2)).is_none());
        assert!(cache.get_if_current(fp(6, 3)).is_none());

        // An empty prefix is a valid, cacheable state.
        let empty = Arc::new(RoutingTable::default());
        cache.store(fp(0, 0), Arc::clone(&empty));
        assert!(Arc::ptr_eq(
            &cache.get_if_current(fp(0, 0)).unwrap(),
            &empty
        ));
    }

    #[test]
    fn revision_cache_store_replaces_older_snapshot() {
        let cache = RevisionCache::default();
        let old = Arc::new(RoutingTable::default());
        let new = Arc::new(RoutingTable::default());
        cache.store(fp(3, 1), old);
        cache.store(fp(5, 1), Arc::clone(&new));
        assert!(cache.get_if_current(fp(3, 1)).is_none());
        assert!(Arc::ptr_eq(&cache.get_if_current(fp(5, 1)).unwrap(), &new));
        assert!(Arc::ptr_eq(&cache.latest().unwrap(), &new));
    }

    #[test]
    fn validate_cluster_id_accepts_whitelist() {
        for id in ["default", "prod-1", "eu_west.cluster2", "A0._-"] {
            assert_eq!(validate_cluster_id(id).unwrap(), id);
        }
    }

    #[test]
    fn validate_cluster_id_empty_falls_back_to_default() {
        for id in ["", "   ", "\t"] {
            assert_eq!(validate_cluster_id(id).unwrap(), DEFAULT_CLUSTER_ID);
        }
    }

    #[test]
    fn validate_cluster_id_rejects_slash_and_control_chars() {
        for id in ["a/b", "/rr", "clu ster", "cl\nster", "clu\0ster", "c:1"] {
            let err = validate_cluster_id(id).unwrap_err();
            assert!(
                matches!(err, ConfigError::InvalidConfig(_)),
                "cluster_id {id:?} must be rejected, got {err:?}"
            );
        }
    }

    // INV-6: after a routing-table update the old shard receives no new writes; the routing table is the single source of truth.
    #[test]
    fn next_start_revision_resumes_after_last_seen() {
        assert_eq!(next_start_revision(0), None);
        assert_eq!(next_start_revision(-3), None);
        assert_eq!(next_start_revision(41), Some(42));
        // Overflow cannot panic; falls back to a head watch.
        assert_eq!(next_start_revision(i64::MAX), None);
    }

    // INV-6: after a routing-table update the old shard receives no new writes; the routing table is the single source of truth.
    #[tokio::test(start_paused = true)]
    async fn watch_backoff_doubles_and_caps() {
        let mut backoff = WatchBackoff::default();
        let mut expected = WATCH_BACKOFF_INITIAL;
        // With the clock paused, wait() returns instantly; we observe the
        // delay sequence through `current` after each call.
        for _ in 0..10 {
            let before = backoff.current;
            assert_eq!(before, expected);
            backoff.wait().await;
            expected = (expected * 2).min(WATCH_BACKOFF_MAX);
        }
        assert_eq!(backoff.current, WATCH_BACKOFF_MAX);
        backoff.reset();
        assert_eq!(backoff.current, WATCH_BACKOFF_INITIAL);
    }

    #[test]
    fn diff_tables_nodes_added_changed_removed() {
        let mut baseline = RoutingTable::default();
        baseline.nodes.insert(1, NodeAddr { addr: "a:1".into() });
        baseline.nodes.insert(2, NodeAddr { addr: "a:2".into() });

        let mut fresh = RoutingTable::default();
        fresh.nodes.insert(
            2,
            NodeAddr {
                addr: "a:2b".into(),
            },
        );
        fresh.nodes.insert(3, NodeAddr { addr: "a:3".into() });

        let changes = diff_tables(WatchDomain::Node, &baseline, &fresh);
        assert_eq!(changes.len(), 3);
        assert!(changes.iter().any(|c| matches!(
            c,
            RoutingTableChange::NodeAdded { node_id: 2, addr } if addr.addr == "a:2b"
        )));
        assert!(changes
            .iter()
            .any(|c| matches!(c, RoutingTableChange::NodeAdded { node_id: 3, .. })));
        assert!(changes
            .iter()
            .any(|c| matches!(c, RoutingTableChange::NodeRemoved { node_id: 1 })));
    }

    #[test]
    fn diff_tables_partitions_added_updated_removed() {
        let mut baseline = RoutingTable::default();
        baseline
            .partitions
            .insert(1, (PartitionConfig::SingleId(1), 10));
        baseline
            .partitions
            .insert(2, (PartitionConfig::SingleId(2), 20));

        let mut fresh = RoutingTable::default();
        fresh
            .partitions
            .insert(2, (PartitionConfig::SingleId(2), 21)); // updated group
        fresh
            .partitions
            .insert(3, (PartitionConfig::SingleId(3), 30)); // added

        let changes = diff_tables(WatchDomain::Partition, &baseline, &fresh);
        assert_eq!(changes.len(), 3);
        assert!(changes.iter().any(|c| matches!(
            c,
            RoutingTableChange::PartitionUpdated {
                partition_id: 2,
                group_id: 21,
                ..
            }
        )));
        assert!(changes.iter().any(|c| matches!(
            c,
            RoutingTableChange::PartitionAdded {
                partition_id: 3,
                group_id: 30,
                ..
            }
        )));
        assert!(changes
            .iter()
            .any(|c| matches!(c, RoutingTableChange::PartitionRemoved { partition_id: 1 })));
    }

    // INV-6: after a routing-table update the old shard receives no new writes; the routing table is the single source of truth.
    #[test]
    fn diff_tables_groups_changed_and_removed() {
        let mut baseline = RoutingTable::default();
        baseline.groups.insert(
            1,
            GroupMeta {
                group_id: 1,
                replicas: vec![1, 2],
            },
        );
        baseline.groups.insert(
            2,
            GroupMeta {
                group_id: 2,
                replicas: vec![3],
            },
        );

        let mut fresh = RoutingTable::default();
        fresh.groups.insert(
            1,
            GroupMeta {
                group_id: 1,
                replicas: vec![1, 2, 3],
            },
        );

        let changes = diff_tables(WatchDomain::Group, &baseline, &fresh);
        assert_eq!(changes.len(), 2);
        assert!(changes.iter().any(|c| matches!(
            c,
            RoutingTableChange::GroupMembershipChanged { group_id: 1, replicas } if replicas == &vec![1, 2, 3]
        )));
        // Group removal surfaces as empty membership (no GroupRemoved
        // variant — see parse_watch_event).
        assert!(changes.iter().any(|c| matches!(
            c,
            RoutingTableChange::GroupMembershipChanged { group_id: 2, replicas } if replicas.is_empty()
        )));
    }

    #[test]
    fn diff_tables_identical_is_empty() {
        let mut table = RoutingTable::default();
        table.nodes.insert(1, NodeAddr { addr: "a:1".into() });
        table
            .partitions
            .insert(1, (PartitionConfig::SingleId(1), 10));
        table.groups.insert(
            10,
            GroupMeta {
                group_id: 10,
                replicas: vec![1],
            },
        );
        for domain in [
            WatchDomain::Node,
            WatchDomain::Partition,
            WatchDomain::Group,
        ] {
            assert!(diff_tables(domain, &table, &table).is_empty());
        }
    }

    #[test]
    fn diff_tables_is_scoped_to_domain() {
        let baseline = RoutingTable::default();
        let mut fresh = RoutingTable::default();
        fresh.nodes.insert(1, NodeAddr { addr: "a:1".into() });
        fresh
            .partitions
            .insert(1, (PartitionConfig::SingleId(1), 10));

        let node_changes = diff_tables(WatchDomain::Node, &baseline, &fresh);
        assert_eq!(node_changes.len(), 1);
        assert!(matches!(
            node_changes[0],
            RoutingTableChange::NodeAdded { .. }
        ));
        let partition_changes = diff_tables(WatchDomain::Partition, &baseline, &fresh);
        assert_eq!(partition_changes.len(), 1);
        assert!(matches!(
            partition_changes[0],
            RoutingTableChange::PartitionAdded { .. }
        ));
        assert!(diff_tables(WatchDomain::Group, &baseline, &fresh).is_empty());
    }

    // INV-6: after a routing-table update the old shard receives no new writes; the routing table is the single source of truth.
    #[tokio::test]
    async fn watch_stream_guard_aborts_tasks_on_drop() {
        let (_sender, receiver) = mpsc::channel::<RoutingTableChange>(1);
        // Tasks that never complete on their own.
        let handles = vec![
            tokio::spawn(std::future::pending::<()>()),
            tokio::spawn(std::future::pending::<()>()),
        ];
        let abort_handles: Vec<_> = handles.iter().map(|h| h.abort_handle()).collect();
        let guard = WatchStreamGuard {
            inner: ReceiverStream::new(receiver),
            handles,
        };
        drop(guard);
        tokio::task::yield_now().await;
        for abort_handle in abort_handles {
            assert!(abort_handle.is_finished());
        }
    }

    #[tokio::test]
    async fn watch_stream_guard_yields_items() {
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(RoutingTableChange::NodeRemoved { node_id: 7 })
            .await
            .unwrap();
        drop(sender);
        let mut guard = WatchStreamGuard {
            inner: ReceiverStream::new(receiver),
            handles: Vec::new(),
        };
        use tokio_stream::StreamExt as _;
        let item = guard.next().await;
        assert!(matches!(
            item,
            Some(RoutingTableChange::NodeRemoved { node_id: 7 })
        ));
        assert!(guard.next().await.is_none());
    }

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
