use super::config::SeedPeer;
use super::metrics::{
    aggregate_histograms, AggregatedHistogram, CounterDeltas, MetricKind, MetricSample,
    MetricsSnapshot,
};
use super::model::{
    age_ms, rfc3339, BoundedLru, Capability, EventView, GroupObservation, ResponseMeta, SourceMeta,
    TimeSeriesPoint,
};
use super::upstream::{GroupDetail, NodeSnapshot, UpstreamError};
use rr_network::proto::admin::{FormationStatus, GroupSummary, PartitionRoute};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::broadcast;

const DEFAULT_EVENT_CAPACITY: usize = 512;
const DEFAULT_POINT_CAPACITY: usize = 20_000;
const MAX_ACTIVE_SERIES: usize = 20_000;
const MAX_CACHED_DETAILS_PER_NODE: usize = 256;
const CACHED_DETAIL_TTL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub(crate) struct NodeMetricsScrape {
    pub peer: SeedPeer,
    pub snapshot: MetricsSnapshot,
    pub observed_at: SystemTime,
}

#[derive(Clone, Debug)]
pub(crate) struct CounterRate {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
    pub reset: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct CachedMetrics {
    pub samples: Arc<Vec<MetricSample>>,
    pub rates: Arc<Vec<CounterRate>>,
    pub source: SourceMeta,
    pub error: Option<UpstreamError>,
    pub last_attempt: SystemTime,
    pub last_success: Option<SystemTime>,
}

type HistogramKey = (String, BTreeMap<String, String>);

#[derive(Default)]
struct ClusterMetrics {
    throughput: BTreeMap<String, f64>,
    histograms: BTreeMap<HistogramKey, AggregatedHistogram>,
    server_histograms: BTreeMap<HistogramKey, AggregatedHistogram>,
    invalid_histograms: bool,
    invalid_server_histograms: bool,
}

pub(crate) struct MetricsBatch {
    cluster: ClusterMetrics,
    latest: Option<SystemTime>,
    initial_batch: bool,
    input_series: usize,
}

impl ClusterMetrics {
    fn observe(&mut self, samples: &[MetricSample], rates: &[CounterRate]) {
        for rate in rates {
            if rate.name == "rr_server_requests_total" {
                if let Some(op) = rate.labels.get("op") {
                    *self.throughput.entry(op.clone()).or_default() += rate.value;
                }
            }
        }
        match aggregate_histograms(samples) {
            Ok(histograms) => merge_histograms(&mut self.histograms, histograms),
            Err(_) => self.invalid_histograms = true,
        }
        let server_samples = samples
            .iter()
            .filter(|sample| {
                sample
                    .name
                    .starts_with("rr_server_request_duration_seconds")
            })
            .cloned()
            .map(|mut sample| {
                sample.labels.remove("group_id");
                sample
            })
            .collect::<Vec<_>>();
        match aggregate_histograms(&server_samples) {
            Ok(histograms) => merge_histograms(&mut self.server_histograms, histograms),
            Err(_) => self.invalid_server_histograms = true,
        }
    }
}

fn merge_histograms(
    combined: &mut BTreeMap<HistogramKey, AggregatedHistogram>,
    histograms: Vec<AggregatedHistogram>,
) {
    for histogram in histograms {
        let key = (histogram.name.clone(), histogram.labels.clone());
        if let Some(existing) = combined.get_mut(&key) {
            existing.count += histogram.count;
            existing.sum += histogram.sum;
            for bucket in histogram.buckets {
                if let Some(existing_bucket) = existing
                    .buckets
                    .iter_mut()
                    .find(|existing| existing.upper_bound.to_bits() == bucket.upper_bound.to_bits())
                {
                    existing_bucket.count += bucket.count;
                } else {
                    existing.buckets.push(bucket);
                }
            }
        } else {
            combined.insert(key, histogram);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProbeResult {
    pub value: Option<bool>,
    pub phase: Option<String>,
    pub error: Option<UpstreamError>,
}

#[derive(Clone, Debug)]
pub(crate) struct CachedObservability {
    pub liveness: Option<bool>,
    pub ready: Option<bool>,
    pub ready_phase: Option<String>,
    pub sources: Vec<SourceMeta>,
    pub degraded: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct CachedNode {
    pub peer: SeedPeer,
    pub snapshot: Option<Arc<NodeSnapshot>>,
    pub details: BTreeMap<u64, GroupDetail>,
    pub metrics: Option<CachedMetrics>,
    pub observability: Option<CachedObservability>,
    pub last_attempt: SystemTime,
    pub last_success: Option<SystemTime>,
    pub error: Option<UpstreamError>,
    pub sources: Vec<SourceMeta>,
}

#[derive(Clone)]
pub(crate) struct CacheView {
    pub nodes: Arc<Vec<CachedNode>>,
    pub cluster_id: String,
    pub partial: bool,
    pub sources: Vec<SourceMeta>,
    pub capabilities: Vec<Capability>,
    pub events: Arc<Vec<EventView>>,
    pub points: Arc<Vec<TimeSeriesPoint>>,
    pub generation: u64,
    pub desired_routes: Option<Vec<PartitionRoute>>,
    pub desired_groups: Option<BTreeMap<u64, Vec<u64>>>,
    pub topology_revision: u64,
    pub source_mode: String,
    pub discovery_error: Option<UpstreamError>,
    pub quarantined: bool,
    stale_after: Duration,
    leader_conflicts: BTreeMap<(u64, u64, Vec<u64>), SystemTime>,
}

impl CacheView {
    pub fn meta(&self) -> ResponseMeta {
        ResponseMeta::new(
            uuid::Uuid::new_v4().to_string(),
            self.cluster_id.clone(),
            self.partial,
            self.sources.clone(),
            self.capabilities.clone(),
        )
    }

    pub fn group_views(&self) -> Vec<Value> {
        group_views_from(self)
    }

    pub fn routes(&self) -> Vec<PartitionRoute> {
        if let Some(routes) = &self.desired_routes {
            return routes.clone();
        }
        self.nodes
            .iter()
            .filter_map(|node| {
                node.snapshot.as_ref().and_then(|snapshot| {
                    snapshot
                        .topology
                        .as_ref()
                        .map(|topology| topology.partitions.clone())
                        .or_else(|| snapshot.routes.clone())
                        .map(|routes| (node.last_success, routes))
                })
            })
            .max_by_key(|(last_success, _)| *last_success)
            .map(|(_, routes)| routes)
            .unwrap_or_default()
    }

    pub fn expected_groups(&self) -> BTreeMap<u64, Vec<u64>> {
        if let Some(groups) = &self.desired_groups {
            return groups.clone();
        }
        self.nodes
            .iter()
            .filter_map(|node| {
                node.snapshot.as_ref().and_then(|snapshot| {
                    snapshot.topology.as_ref().map(|topology| {
                        (
                            node.last_success,
                            topology
                                .groups
                                .iter()
                                .map(|group| (group.group_id, group.replica_node_ids.clone()))
                                .collect::<BTreeMap<_, _>>(),
                        )
                    })
                })
            })
            .max_by_key(|(last_success, _)| *last_success)
            .map(|(_, groups)| groups)
            .unwrap_or_default()
    }

    pub fn formation(&self) -> Option<FormationStatus> {
        self.nodes.iter().find_map(|node| {
            node.snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.formation.clone())
        })
    }

    pub fn trusted_cluster_id(&self) -> Option<String> {
        if self.quarantined {
            return None;
        }
        let mut identities = self.nodes.iter().filter_map(|node| {
            node.snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.formation.as_ref())
                .map(|formation| formation.cluster_uuid.as_str())
                .filter(|identity| !identity.is_empty())
        });
        let first = identities.next()?;
        identities
            .all(|identity| identity == first)
            .then(|| first.to_string())
    }
}

#[derive(Clone)]
struct StoredNode {
    peer: SeedPeer,
    snapshot: Option<Arc<NodeSnapshot>>,
    details: BoundedLru<u64, GroupDetail>,
    metrics: Option<CachedMetrics>,
    observability: Option<CachedObservability>,
    last_boot_id: Option<String>,
    last_version: Option<(String, String, String)>,
    last_attempt: SystemTime,
    last_success: Option<SystemTime>,
    error: Option<UpstreamError>,
}

struct CacheInner {
    nodes: BTreeMap<u64, StoredNode>,
    events: VecDeque<EventView>,
    event_view: Arc<Vec<EventView>>,
    events_dirty: bool,
    points: VecDeque<TimeSeriesPoint>,
    point_view: Arc<Vec<TimeSeriesPoint>>,
    points_dirty: bool,
    generation: u64,
    desired_routes: Option<Vec<PartitionRoute>>,
    desired_groups: Option<BTreeMap<u64, Vec<u64>>>,
    topology_revision: u64,
    source_mode: String,
    discovery_error: Option<UpstreamError>,
    identity_conflict: Option<UpstreamError>,
    leader_conflicts: BTreeMap<(u64, u64, Vec<u64>), SystemTime>,
    leader_conflict_recomputations: usize,
    counter_deltas: CounterDeltas,
    metric_times: BTreeMap<u64, SystemTime>,
}

pub(crate) struct ConsoleCache {
    configured_cluster_name: String,
    stale_after: Duration,
    event_capacity: usize,
    point_capacity: usize,
    inner: RwLock<CacheInner>,
    changes: broadcast::Sender<String>,
}

impl ConsoleCache {
    pub fn new(
        configured_cluster_name: impl Into<String>,
        stale_after: Duration,
        event_capacity: usize,
        point_capacity: usize,
    ) -> Arc<Self> {
        let (changes, _) = broadcast::channel(64);
        Arc::new(Self {
            configured_cluster_name: configured_cluster_name.into(),
            stale_after,
            event_capacity: event_capacity.max(1),
            point_capacity: point_capacity.max(1),
            inner: RwLock::new(CacheInner {
                nodes: BTreeMap::new(),
                events: VecDeque::new(),
                event_view: Arc::new(Vec::new()),
                events_dirty: false,
                points: VecDeque::new(),
                point_view: Arc::new(Vec::new()),
                points_dirty: false,
                generation: 0,
                desired_routes: None,
                desired_groups: None,
                topology_revision: 0,
                source_mode: "EXPLICIT_ADMIN_SEEDS".to_string(),
                discovery_error: None,
                identity_conflict: None,
                leader_conflicts: BTreeMap::new(),
                leader_conflict_recomputations: 0,
                counter_deltas: CounterDeltas::default(),
                metric_times: BTreeMap::new(),
            }),
            changes,
        })
    }

    pub fn production(
        configured_cluster_name: Option<String>,
        poll_interval: Duration,
    ) -> Arc<Self> {
        Self::new(
            configured_cluster_name.unwrap_or_else(|| "untracked".to_string()),
            poll_interval.saturating_mul(3),
            DEFAULT_EVENT_CAPACITY,
            DEFAULT_POINT_CAPACITY,
        )
    }

    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.changes.subscribe()
    }

    pub fn notify(&self, kind: &str) {
        let _ = self.changes.send(kind.to_string());
    }

    pub fn register_peers(&self, peers: &[SeedPeer]) {
        let mut inner = self.inner.write().expect("console cache lock");
        for peer in peers {
            inner
                .nodes
                .entry(peer.node_id)
                .or_insert_with(|| StoredNode {
                    peer: peer.clone(),
                    snapshot: None,
                    details: BoundedLru::new(MAX_CACHED_DETAILS_PER_NODE, CACHED_DETAIL_TTL),
                    metrics: None,
                    observability: None,
                    last_boot_id: None,
                    last_version: None,
                    last_attempt: SystemTime::now(),
                    last_success: None,
                    error: None,
                });
        }
    }

    pub fn set_source_mode(&self, mode: &str) {
        self.inner.write().expect("console cache lock").source_mode = mode.to_string();
    }

    pub fn set_desired_topology(
        &self,
        routes: Vec<PartitionRoute>,
        groups: BTreeMap<u64, Vec<u64>>,
        revision: u64,
    ) {
        let mut inner = self.inner.write().expect("console cache lock");
        if revision != 0 && inner.topology_revision > revision {
            return;
        }
        inner.desired_routes = Some(routes);
        inner.desired_groups = Some(groups);
        inner.topology_revision = revision;
        inner.discovery_error = None;
    }

    pub fn record_discovery_error(&self, code: &'static str, message: &str) {
        let mut inner = self.inner.write().expect("console cache lock");
        inner.discovery_error = Some(UpstreamError {
            code,
            message: message.to_string(),
        });
        drop(inner);
        let _ = self.changes.send("sources".to_string());
    }

    pub fn is_quarantined(&self) -> bool {
        self.inner
            .read()
            .expect("console cache lock")
            .identity_conflict
            .is_some()
    }

    pub fn record_success(
        &self,
        peer: &SeedPeer,
        snapshot: impl Into<Arc<NodeSnapshot>>,
    ) -> Result<(), UpstreamError> {
        self.record_success_inner(peer, snapshot, true)
    }

    pub fn record_success_for_reconcile(
        &self,
        peer: &SeedPeer,
        snapshot: impl Into<Arc<NodeSnapshot>>,
    ) -> Result<(), UpstreamError> {
        self.record_success_inner(peer, snapshot, false)
    }

    fn record_success_inner(
        &self,
        peer: &SeedPeer,
        snapshot: impl Into<Arc<NodeSnapshot>>,
        recompute_conflicts: bool,
    ) -> Result<(), UpstreamError> {
        let snapshot = snapshot.into();
        snapshot.validate_for(peer)?;
        let current_boot_id = snapshot_boot_id(&snapshot).map(str::to_string);
        let current_version = snapshot_version(&snapshot);
        let mut inner = self.inner.write().expect("console cache lock");
        if let Err(error) = validate_identity(&inner, &snapshot) {
            inner.identity_conflict = Some(error.clone());
            return Err(error);
        }
        let recovered = inner
            .nodes
            .get(&peer.node_id)
            .and_then(|node| node.error.as_ref())
            .is_some();
        let previous = inner.nodes.get(&peer.node_id);
        let previous_boot_id = previous.and_then(|node| node.last_boot_id.clone());
        let previous_version = previous.and_then(|node| node.last_version.clone());
        let last_boot_id = current_boot_id.clone().or_else(|| previous_boot_id.clone());
        let last_version = current_version.clone().or_else(|| previous_version.clone());
        let reuse = previous.is_some_and(|previous| {
            previous.peer.admin_addr == peer.admin_addr
                && previous.snapshot.as_ref().is_none_or(|previous| {
                    snapshot_identity(previous) == snapshot_identity(&snapshot)
                })
        });
        let details = previous
            .filter(|_| reuse)
            .map(|node| node.details.clone())
            .unwrap_or_else(|| BoundedLru::new(MAX_CACHED_DETAILS_PER_NODE, CACHED_DETAIL_TTL));
        let metrics = previous
            .filter(|_| reuse)
            .and_then(|node| node.metrics.clone());
        let observability = previous
            .filter(|_| reuse)
            .and_then(|node| node.observability.clone());
        inner.nodes.insert(
            peer.node_id,
            StoredNode {
                peer: peer.clone(),
                snapshot: Some(snapshot.clone()),
                details,
                metrics,
                observability,
                last_boot_id,
                last_version,
                last_attempt: snapshot.collected_at,
                last_success: Some(snapshot.collected_at),
                error: None,
            },
        );
        if recompute_conflicts {
            update_leader_conflicts(&mut inner, snapshot.collected_at);
        }
        push_point(
            &mut inner,
            self.point_capacity,
            peer.node_id,
            1.0,
            snapshot.collected_at,
        );
        if recovered {
            push_event(
                &mut inner,
                self.event_capacity,
                Some(peer.node_id),
                None,
                "INFO",
                "NODE_RECOVERED",
                "AdminService contact recovered",
                snapshot.collected_at,
            );
        }
        if let (Some(previous), Some(current)) = (&previous_boot_id, &current_boot_id) {
            if previous != current {
                push_event(
                    &mut inner,
                    self.event_capacity,
                    Some(peer.node_id),
                    None,
                    "INFO",
                    "NODE_RESTARTED",
                    &format!("Node restarted from boot {previous} to {current}"),
                    snapshot.collected_at,
                );
            }
        }
        if let (Some(previous), Some(current)) = (&previous_version, &current_version) {
            if previous != current {
                push_event(
                    &mut inner,
                    self.event_capacity,
                    Some(peer.node_id),
                    None,
                    "WARN",
                    "NODE_VERSION_CHANGED",
                    &format!(
                        "Node version changed from app {}/{}/{} to app {}/{}/{}",
                        previous.0, previous.1, previous.2, current.0, current.1, current.2
                    ),
                    snapshot.collected_at,
                );
            }
        }
        drop(inner);
        if recompute_conflicts {
            self.sync_group_events(snapshot.collected_at);
        }
        Ok(())
    }

    pub fn record_failure(&self, peer: &SeedPeer, code: &'static str, message: &str) {
        self.record_failure_at(peer, code, message, SystemTime::now());
    }

    pub fn record_failure_for_reconcile(&self, peer: &SeedPeer, code: &'static str, message: &str) {
        self.record_failure_inner(peer, code, message, SystemTime::now(), false);
    }

    fn record_failure_at(
        &self,
        peer: &SeedPeer,
        code: &'static str,
        message: &str,
        now: SystemTime,
    ) {
        self.record_failure_inner(peer, code, message, now, true);
    }

    fn record_failure_inner(
        &self,
        peer: &SeedPeer,
        code: &'static str,
        message: &str,
        now: SystemTime,
        recompute_conflicts: bool,
    ) {
        let mut inner = self.inner.write().expect("console cache lock");
        let previous = inner.nodes.get(&peer.node_id).cloned();
        let reuse = previous
            .as_ref()
            .is_some_and(|previous| previous.peer.admin_addr == peer.admin_addr);
        inner.nodes.insert(
            peer.node_id,
            StoredNode {
                peer: peer.clone(),
                snapshot: previous
                    .as_ref()
                    .filter(|_| reuse)
                    .and_then(|node| node.snapshot.clone()),
                details: previous
                    .as_ref()
                    .filter(|_| reuse)
                    .map(|node| node.details.clone())
                    .unwrap_or_else(|| {
                        BoundedLru::new(MAX_CACHED_DETAILS_PER_NODE, CACHED_DETAIL_TTL)
                    }),
                metrics: previous
                    .as_ref()
                    .filter(|_| reuse)
                    .and_then(|node| node.metrics.clone()),
                observability: previous
                    .as_ref()
                    .filter(|_| reuse)
                    .and_then(|node| node.observability.clone()),
                last_boot_id: previous.as_ref().and_then(|node| node.last_boot_id.clone()),
                last_version: previous.as_ref().and_then(|node| node.last_version.clone()),
                last_attempt: now,
                last_success: previous
                    .as_ref()
                    .filter(|_| reuse)
                    .and_then(|node| node.last_success),
                error: Some(UpstreamError {
                    code,
                    message: message.to_string(),
                }),
            },
        );
        if recompute_conflicts {
            update_leader_conflicts(&mut inner, now);
        }
        push_point(&mut inner, self.point_capacity, peer.node_id, 0.0, now);
        push_event(
            &mut inner,
            self.event_capacity,
            Some(peer.node_id),
            None,
            "ERROR",
            "NODE_UNREACHABLE",
            message,
            now,
        );
        drop(inner);
        if recompute_conflicts {
            self.sync_group_events(now);
        }
        let _ = self.changes.send("events".to_string());
    }

    fn sync_group_events(&self, observed_at: SystemTime) {
        let groups = self.view().group_views();
        let mut inner = self.inner.write().expect("console cache lock");
        let mut active_groups = BTreeSet::new();
        for group in groups {
            let health = group["health"].as_str().unwrap_or("UNKNOWN");
            if !matches!(
                health,
                "LEADERLESS_OBSERVED" | "QUORUM_AT_RISK" | "CONFLICTING_VIEWS"
            ) {
                continue;
            }
            let Some(group_id) = group["group_id"].as_u64() else {
                continue;
            };
            active_groups.insert(group_id);
            push_event(
                &mut inner,
                self.event_capacity,
                None,
                Some(group_id),
                "ERROR",
                "GROUP_HEALTH",
                &format!(
                    "Group health is {health}; leader={}, term={}",
                    group["leader_node_id"], group["current_term"]
                ),
                observed_at,
            );
        }
        let mut resolved = false;
        for event in inner.events.iter_mut().filter(|event| {
            event.kind == "GROUP_HEALTH"
                && event.state == "ACTIVE"
                && event
                    .group_id
                    .is_some_and(|group_id| !active_groups.contains(&group_id))
        }) {
            event.state = "RESOLVED".to_string();
            event.last_observed_at = rfc3339(observed_at);
            resolved = true;
        }
        if resolved {
            inner.events_dirty = true;
        }
    }

    pub fn record_detail(&self, peer: &SeedPeer, group_id: u64, detail: GroupDetail) {
        let mut inner = self.inner.write().expect("console cache lock");
        if let Some(node) = inner.nodes.get_mut(&peer.node_id) {
            if node.peer.admin_addr == peer.admin_addr {
                node.details.insert(group_id, detail, Instant::now());
            }
        }
        drop(inner);
        let _ = self.changes.send("summary".to_string());
    }

    pub fn record_observability(&self, peer: &SeedPeer, health: ProbeResult, ready: ProbeResult) {
        let observed_at = SystemTime::now();
        let health_source = probe_source(peer.node_id, "HEALTH", &health, observed_at);
        let ready_source = probe_source(peer.node_id, "READY", &ready, observed_at);
        let observability = CachedObservability {
            liveness: health.value,
            ready: ready.value,
            ready_phase: ready.phase,
            sources: vec![health_source, ready_source],
            degraded: health.error.is_some()
                || ready.error.is_some()
                || health.value == Some(false)
                || ready.value == Some(false),
        };
        let mut inner = self.inner.write().expect("console cache lock");
        if let Some(node) = inner.nodes.get_mut(&peer.node_id) {
            if node.peer.admin_addr == peer.admin_addr {
                node.observability = Some(observability);
            }
        }
    }

    pub fn begin_metrics_batch(&self) -> MetricsBatch {
        let inner = self.inner.read().expect("console cache lock");
        MetricsBatch {
            cluster: ClusterMetrics::default(),
            latest: None,
            initial_batch: inner.generation == 0,
            input_series: inner
                .nodes
                .values()
                .filter_map(|node| node.metrics.as_ref())
                .map(|metrics| metrics.samples.len() + metrics.rates.len())
                .sum(),
        }
    }

    pub fn record_metrics_scrape(&self, scrape: NodeMetricsScrape, batch: &mut MetricsBatch) {
        let mut inner = self.inner.write().expect("console cache lock");
        let node_id = scrape.peer.node_id;
        if inner
            .nodes
            .get(&node_id)
            .is_none_or(|node| node.peer.admin_addr != scrape.peer.admin_addr)
        {
            return;
        }
        let elapsed = inner
            .metric_times
            .get(&node_id)
            .and_then(|previous| scrape.observed_at.duration_since(*previous).ok())
            .map(|duration| duration.as_secs_f64())
            .filter(|seconds| *seconds > 0.0);
        let deltas = match inner
            .counter_deltas
            .observe_node(node_id, &scrape.snapshot.samples)
        {
            Ok(deltas) => deltas,
            Err(error) => {
                record_metrics_error_inner(
                    &mut inner,
                    &scrape.peer,
                    scrape.observed_at,
                    error.code,
                    &error.message,
                );
                return;
            }
        };
        inner.metric_times.insert(node_id, scrape.observed_at);
        let rates = deltas
            .into_iter()
            .filter_map(|delta| {
                delta.delta.map(|value| CounterRate {
                    name: delta.name,
                    labels: delta.labels,
                    value: value / elapsed.unwrap_or(1.0),
                    reset: delta.reset,
                })
            })
            .filter(|_| elapsed.is_some())
            .collect::<Vec<_>>();
        let source = SourceMeta {
            node_id,
            endpoint_role: "METRICS".to_string(),
            observed_at: Some(rfc3339(scrape.observed_at)),
            age_ms: Some(0),
            status: "ONLINE".to_string(),
            error_code: None,
        };
        project_node_metrics(&mut inner, self.point_capacity, &scrape, &rates);
        batch.latest = batch.latest.max(Some(scrape.observed_at));
        batch.cluster.observe(&scrape.snapshot.samples, &rates);
        let samples = scrape
            .snapshot
            .samples
            .into_iter()
            .filter(current_metric)
            .collect::<Vec<_>>();
        let previous_series = inner
            .nodes
            .get(&node_id)
            .and_then(|node| node.metrics.as_ref())
            .map(|metrics| metrics.samples.len() + metrics.rates.len())
            .unwrap_or(0);
        batch.input_series =
            batch.input_series.saturating_sub(previous_series) + samples.len() + rates.len();
        let (samples, rates) = downsample_scrape(&inner, node_id, samples, rates);
        if let Some(node) = inner.nodes.get_mut(&node_id) {
            node.metrics = Some(CachedMetrics {
                samples: Arc::new(samples),
                rates: Arc::new(rates),
                source,
                error: None,
                last_attempt: scrape.observed_at,
                last_success: Some(scrape.observed_at),
            });
        }
    }

    pub fn finish_metrics_batch(&self, batch: MetricsBatch) {
        let mut inner = self.inner.write().expect("console cache lock");
        enforce_active_series_cap(&mut inner, self.event_capacity, batch.input_series);
        if let Some(observed_at) = batch.latest {
            project_cluster_metrics(&mut inner, self.point_capacity, observed_at, batch.cluster);
        }
        drop(inner);
        if batch.initial_batch {
            let _ = self.changes.send("metrics".to_string());
        }
    }

    #[cfg(test)]
    pub fn record_metrics_batch(&self, scrapes: Vec<NodeMetricsScrape>) {
        let mut batch = self.begin_metrics_batch();
        for scrape in scrapes {
            self.record_metrics_scrape(scrape, &mut batch);
        }
        self.finish_metrics_batch(batch);
    }

    pub fn record_metrics_failure(&self, peer: &SeedPeer, code: &'static str, message: &str) {
        let mut inner = self.inner.write().expect("console cache lock");
        record_metrics_error_inner(&mut inner, peer, SystemTime::now(), code, message);
        drop(inner);
        let _ = self.changes.send("sources".to_string());
    }

    pub fn refresh_leader_conflicts(&self) {
        let now = SystemTime::now();
        let mut inner = self.inner.write().expect("console cache lock");
        update_leader_conflicts(&mut inner, now);
        drop(inner);
        self.sync_group_events(now);
    }

    #[cfg(test)]
    pub fn leader_conflict_recomputations(&self) -> usize {
        self.inner
            .read()
            .expect("console cache lock")
            .leader_conflict_recomputations
    }

    pub fn advance_generation(&self) {
        let mut inner = self.inner.write().expect("console cache lock");
        refresh_view_snapshots(&mut inner);
        inner.generation = inner.generation.saturating_add(1);
    }

    pub fn generation(&self) -> u64 {
        self.inner.read().expect("console cache lock").generation
    }

    pub fn view(&self) -> CacheView {
        self.view_at(SystemTime::now())
    }

    fn view_at(&self, now: SystemTime) -> CacheView {
        let mut inner = self.inner.write().expect("console cache lock");
        refresh_view_snapshots(&mut inner);
        let mut capabilities: BTreeMap<String, Vec<Capability>> = BTreeMap::new();
        let mut nodes = Vec::new();
        let mut sources = Vec::new();
        let mut cluster_id = None;
        for node in inner.nodes.values() {
            let stale = node
                .last_success
                .map(|success| age_ms(now, success) > self.stale_after.as_millis() as u64)
                .unwrap_or(false);
            let status = if node.error.is_some() {
                if node.snapshot.is_some() {
                    "STALE"
                } else {
                    "UNREACHABLE"
                }
            } else if stale {
                "STALE"
            } else if node
                .metrics
                .as_ref()
                .is_some_and(|metrics| metrics.error.is_some())
                || node
                    .observability
                    .as_ref()
                    .is_some_and(|observability| observability.degraded)
            {
                "DEGRADED"
            } else if node.snapshot.is_some() {
                "ONLINE"
            } else {
                "UNKNOWN"
            };
            let source = SourceMeta {
                node_id: node.peer.node_id,
                endpoint_role: "ADMIN".to_string(),
                observed_at: node.last_success.map(rfc3339),
                age_ms: node.last_success.map(|success| age_ms(now, success)),
                status: status.to_string(),
                error_code: node.error.as_ref().map(|error| error.code.to_string()),
            };
            sources.push(source.clone());
            if let Some(observability) = &node.observability {
                sources.extend(observability.sources.iter().cloned());
            }
            let metrics = node.metrics.clone().map(|mut metrics| {
                let stale = metrics
                    .last_success
                    .map(|success| age_ms(now, success) > self.stale_after.as_millis() as u64)
                    .unwrap_or(false);
                metrics.source.status = if metrics.error.is_some() {
                    if metrics.last_success.is_some() {
                        "STALE"
                    } else {
                        "ERROR"
                    }
                } else if stale {
                    "STALE"
                } else {
                    "ONLINE"
                }
                .to_string();
                metrics.source.age_ms = metrics.last_success.map(|success| age_ms(now, success));
                sources.push(metrics.source.clone());
                metrics
            });
            if let Some(snapshot) = &node.snapshot {
                for capability in &snapshot.capabilities {
                    capabilities
                        .entry(capability.feature.clone())
                        .or_default()
                        .push(capability.clone());
                }
                for detail in node.details.values_cloned(Instant::now()) {
                    for capability in &detail.capabilities {
                        capabilities
                            .entry(capability.feature.clone())
                            .or_default()
                            .push(capability.clone());
                    }
                }
                if let Some(formation) = &snapshot.formation {
                    if !formation.cluster_uuid.is_empty() {
                        cluster_id = Some(formation.cluster_uuid.clone());
                    }
                }
            }
            nodes.push(CachedNode {
                peer: node.peer.clone(),
                snapshot: node.snapshot.clone(),
                details: node
                    .details
                    .values_cloned(Instant::now())
                    .into_iter()
                    .map(|detail| (detail.status.group_id, detail))
                    .collect(),
                metrics,
                observability: node.observability.clone(),
                last_attempt: node.last_attempt,
                last_success: node.last_success,
                error: node.error.clone(),
                sources: vec![source],
            });
        }
        let mut capabilities = capabilities
            .into_iter()
            .map(|(feature, values)| {
                if values.iter().all(|value| value.status == "SUPPORTED") {
                    Capability::supported(feature)
                } else {
                    let supported = values.iter().any(|value| value.status == "SUPPORTED");
                    Capability {
                        feature,
                        status: if supported {
                            "PARTIAL".to_string()
                        } else if values.iter().any(|value| value.status == "ERROR") {
                            "ERROR".to_string()
                        } else {
                            "UNSUPPORTED".to_string()
                        },
                        reason: values.into_iter().find_map(|value| value.reason),
                    }
                }
            })
            .collect::<Vec<_>>();
        if cluster_id.is_none()
            && nodes.iter().any(|node| {
                node.snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.formation.as_ref())
                    .is_some_and(|formation| formation.cluster_uuid.is_empty())
            })
        {
            capabilities.push(Capability {
                feature: "cluster_identity".to_string(),
                status: "UNTRACKED".to_string(),
                reason: Some(
                    "all observed nodes are Untracked; the explicit seeds or trusted discovery namespace are the operator's same-cluster assertion"
                        .to_string(),
                ),
            });
        }
        if let Some(error) = &inner.discovery_error {
            capabilities.push(Capability {
                feature: "trusted_discovery".to_string(),
                status: "ERROR".to_string(),
                reason: Some(format!("{}: {}", error.code, error.message)),
            });
        }
        if let Some(error) = &inner.identity_conflict {
            capabilities.push(Capability {
                feature: "cluster_identity".to_string(),
                status: "ERROR".to_string(),
                reason: Some(format!("{}: {}", error.code, error.message)),
            });
        }
        for node in &nodes {
            if let Some(error) = node
                .metrics
                .as_ref()
                .and_then(|metrics| metrics.error.as_ref())
            {
                capabilities.push(Capability {
                    feature: format!("performance_metrics.node.{}", node.peer.node_id),
                    status: "ERROR".to_string(),
                    reason: Some(format!("{}: {}", error.code, error.message)),
                });
            }
        }
        let partial = inner.identity_conflict.is_some()
            || inner.discovery_error.is_some()
            || sources.iter().any(|source| source.status != "ONLINE")
            || capabilities
                .iter()
                .any(|capability| capability.status != "SUPPORTED");
        CacheView {
            nodes: Arc::new(nodes),
            cluster_id: cluster_id.unwrap_or_else(|| self.configured_cluster_name.clone()),
            partial,
            sources,
            capabilities,
            events: inner.event_view.clone(),
            points: inner.point_view.clone(),
            generation: inner.generation,
            desired_routes: inner.desired_routes.clone(),
            desired_groups: inner.desired_groups.clone(),
            topology_revision: inner.topology_revision,
            source_mode: inner.source_mode.clone(),
            discovery_error: inner.discovery_error.clone(),
            quarantined: inner.identity_conflict.is_some(),
            stale_after: self.stale_after,
            leader_conflicts: inner.leader_conflicts.clone(),
        }
    }
}

fn downsample_scrape(
    inner: &CacheInner,
    node_id: u64,
    samples: Vec<MetricSample>,
    rates: Vec<CounterRate>,
) -> (Vec<MetricSample>, Vec<CounterRate>) {
    let current_has_metrics = inner
        .nodes
        .get(&node_id)
        .is_some_and(|node| node.metrics.is_some());
    let node_count = inner
        .nodes
        .values()
        .filter(|node| node.metrics.is_some())
        .count()
        + usize::from(!current_has_metrics);
    let active = inner
        .nodes
        .iter()
        .filter(|(existing_id, _)| **existing_id != node_id)
        .filter_map(|(_, node)| node.metrics.as_ref())
        .map(|metrics| metrics.samples.len() + metrics.rates.len())
        .sum::<usize>()
        + samples.len()
        + rates.len();
    if active <= MAX_ACTIVE_SERIES || node_count == 0 {
        return (samples, rates);
    }
    downsample_metrics(&samples, &rates, MAX_ACTIVE_SERIES / node_count)
}

fn refresh_view_snapshots(inner: &mut CacheInner) {
    if inner.events_dirty {
        inner.event_view = Arc::new(inner.events.iter().cloned().collect());
        inner.events_dirty = false;
    }
    if inner.points_dirty {
        inner.point_view = Arc::new(inner.points.iter().cloned().collect());
        inner.points_dirty = false;
    }
}

fn enforce_active_series_cap(inner: &mut CacheInner, event_capacity: usize, input_series: usize) {
    let node_count = inner
        .nodes
        .values()
        .filter(|node| node.metrics.is_some())
        .count();
    let cached_series = inner
        .nodes
        .values()
        .filter_map(|node| node.metrics.as_ref())
        .map(|metrics| metrics.samples.len() + metrics.rates.len())
        .sum::<usize>();
    let active = input_series.max(cached_series);
    if active <= MAX_ACTIVE_SERIES || node_count == 0 {
        return;
    }

    let quota = MAX_ACTIVE_SERIES / node_count;
    let mut retained = 0;
    for metrics in inner
        .nodes
        .values_mut()
        .filter_map(|node| node.metrics.as_mut())
    {
        if metrics.samples.len() + metrics.rates.len() <= quota {
            retained += metrics.samples.len() + metrics.rates.len();
            continue;
        }
        let (samples, rates) = downsample_metrics(&metrics.samples, &metrics.rates, quota);
        retained += samples.len() + rates.len();
        metrics.samples = Arc::new(samples);
        metrics.rates = Arc::new(rates);
    }
    push_event(
        inner,
        event_capacity,
        None,
        None,
        "WARN",
        "ACTIVE_SERIES_DOWNSAMPLED",
        &format!(
            "normalized active series exceeded {MAX_ACTIVE_SERIES}; downsampled {active} to {retained} cluster-wide series"
        ),
        SystemTime::now(),
    );
}

fn downsample_metrics(
    samples: &[MetricSample],
    rates: &[CounterRate],
    quota: usize,
) -> (Vec<MetricSample>, Vec<CounterRate>) {
    if samples.len() + rates.len() <= quota {
        return (samples.to_vec(), rates.to_vec());
    }
    let sample_groups =
        partition_group_indices(samples, |sample| sample.labels.contains_key("group_id"));
    let rate_groups = partition_group_indices(rates, |rate| rate.labels.contains_key("group_id"));
    let essential_samples = sample_groups.0.len();
    let essential_rates = rate_groups.0.len();
    let essential = essential_samples + essential_rates;
    let (essential_sample_budget, essential_rate_budget, group_sample_budget, group_rate_budget) =
        if essential >= quota {
            let (sample_budget, rate_budget) =
                split_series_budget(essential_samples, essential_rates, quota);
            (sample_budget, rate_budget, 0, 0)
        } else {
            let (sample_budget, rate_budget) = split_series_budget(
                sample_groups.1.len(),
                rate_groups.1.len(),
                quota - essential,
            );
            (
                essential_samples,
                essential_rates,
                sample_budget,
                rate_budget,
            )
        };
    let sample_indices = [
        downsample_indices(&sample_groups.0, essential_sample_budget),
        downsample_indices(&sample_groups.1, group_sample_budget),
    ]
    .concat();
    let rate_indices = [
        downsample_indices(&rate_groups.0, essential_rate_budget),
        downsample_indices(&rate_groups.1, group_rate_budget),
    ]
    .concat();
    (
        select_series(samples, sample_indices),
        select_series(rates, rate_indices),
    )
}

fn partition_group_indices<T>(
    series: &[T],
    grouped: impl Fn(&T) -> bool,
) -> (Vec<usize>, Vec<usize>) {
    let mut essential = Vec::new();
    let mut grouped_series = Vec::new();
    for (index, series) in series.iter().enumerate() {
        if grouped(series) {
            grouped_series.push(index);
        } else {
            essential.push(index);
        }
    }
    (essential, grouped_series)
}

fn split_series_budget(samples: usize, rates: usize, budget: usize) -> (usize, usize) {
    let total = samples + rates;
    if total <= budget {
        return (samples, rates);
    }
    if total == 0 {
        return (0, 0);
    }
    let sample_budget = budget * samples / total;
    (sample_budget, budget - sample_budget)
}

fn downsample_indices(indices: &[usize], budget: usize) -> Vec<usize> {
    if indices.len() <= budget {
        return indices.to_vec();
    }
    if budget == 0 {
        return Vec::new();
    }
    (0..budget)
        .map(|selected| selected * indices.len() / budget)
        .collect()
}

fn select_series<T: Clone>(series: &[T], mut indices: Vec<usize>) -> Vec<T> {
    indices.sort_unstable();
    indices
        .into_iter()
        .map(|index| series[index].clone())
        .collect()
}

fn probe_source(
    node_id: u64,
    role: &str,
    probe: &ProbeResult,
    observed_at: SystemTime,
) -> SourceMeta {
    SourceMeta {
        node_id,
        endpoint_role: role.to_string(),
        observed_at: Some(rfc3339(observed_at)),
        age_ms: Some(0),
        status: if probe.error.is_some() || probe.value == Some(false) {
            "ERROR"
        } else {
            "ONLINE"
        }
        .to_string(),
        error_code: probe.error.as_ref().map(|error| error.code.to_string()),
    }
}

fn record_metrics_error_inner(
    inner: &mut CacheInner,
    peer: &SeedPeer,
    attempted_at: SystemTime,
    code: &'static str,
    message: &str,
) {
    if let Some(node) = inner
        .nodes
        .get_mut(&peer.node_id)
        .filter(|node| node.peer.admin_addr == peer.admin_addr)
    {
        let previous = node.metrics.clone();
        let last_success = previous.as_ref().and_then(|metrics| metrics.last_success);
        node.metrics = Some(CachedMetrics {
            samples: previous
                .as_ref()
                .map(|metrics| metrics.samples.clone())
                .unwrap_or_default(),
            rates: previous
                .as_ref()
                .map(|metrics| metrics.rates.clone())
                .unwrap_or_default(),
            source: SourceMeta {
                node_id: peer.node_id,
                endpoint_role: "METRICS".to_string(),
                observed_at: last_success.map(rfc3339),
                age_ms: None,
                status: "ERROR".to_string(),
                error_code: Some(code.to_string()),
            },
            error: Some(UpstreamError {
                code,
                message: message.to_string(),
            }),
            last_attempt: attempted_at,
            last_success,
        });
    }
}

fn current_metric(sample: &MetricSample) -> bool {
    sample.name.starts_with("rr_node_")
        || matches!(
            sample.name.as_str(),
            "rr_snapshot_installs_total"
                | "rr_snapshot_build_failures_total"
                | "rr_raft_is_leader"
                | "rr_raft_replication_lag"
                | "rr_output_pending"
                | "rr_raft_pending_deferred"
        )
        || sample.name.starts_with("rr_storage_")
}

fn project_node_metrics(
    inner: &mut CacheInner,
    capacity: usize,
    scrape: &NodeMetricsScrape,
    rates: &[CounterRate],
) {
    let node_id = scrape.peer.node_id;
    for rate in rates {
        push_metric_point(
            inner,
            capacity,
            scrape.observed_at,
            Some(node_id),
            counter_metric_name(&rate.name),
            rate.labels.clone(),
            Some(rate.value),
        );
        if rate.reset {
            push_metric_point(
                inner,
                capacity,
                scrape.observed_at,
                Some(node_id),
                counter_metric_name(&rate.name),
                rate.labels.clone(),
                None,
            );
        }
    }
    let leaders = scrape
        .snapshot
        .samples
        .iter()
        .filter(|sample| sample.name == "rr_raft_is_leader" && sample.value == 1.0)
        .filter_map(|sample| sample.labels.get("group_id").cloned())
        .collect::<BTreeSet<_>>();
    for sample in &scrape.snapshot.samples {
        let relevant_gauge = sample.kind == MetricKind::Gauge
            && (sample.name.starts_with("rr_node_")
                || sample.name.starts_with("rr_storage_")
                || matches!(
                    sample.name.as_str(),
                    "rr_output_pending" | "rr_raft_pending_deferred"
                ));
        let leader_lag = sample.name == "rr_raft_replication_lag"
            && sample
                .labels
                .get("group_id")
                .is_some_and(|group_id| leaders.contains(group_id));
        if relevant_gauge || leader_lag {
            push_metric_point(
                inner,
                capacity,
                scrape.observed_at,
                Some(node_id),
                sample.name.clone(),
                sample.labels.clone(),
                Some(sample.value),
            );
        }
    }
}

fn project_cluster_metrics(
    inner: &mut CacheInner,
    capacity: usize,
    observed_at: SystemTime,
    cluster: ClusterMetrics,
) {
    for (op, value) in cluster.throughput {
        push_metric_point(
            inner,
            capacity,
            observed_at,
            None,
            format!("{op}_tps"),
            BTreeMap::new(),
            Some(value),
        );
    }

    if !cluster.invalid_histograms {
        for mut histogram in cluster.histograms.into_values() {
            histogram
                .buckets
                .sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));
            for (suffix, quantile) in [("p50", 0.50), ("p95", 0.95), ("p99", 0.99)] {
                if let Ok(Some(value)) = histogram.quantile(quantile) {
                    push_metric_point(
                        inner,
                        capacity,
                        observed_at,
                        None,
                        format!("{}_{suffix}", histogram.name),
                        histogram.labels.clone(),
                        Some(value),
                    );
                }
            }
        }
    }

    if !cluster.invalid_server_histograms {
        for mut histogram in cluster.server_histograms.into_values() {
            histogram
                .buckets
                .sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));
            let Some(op) = histogram.labels.get("op") else {
                continue;
            };
            for (suffix, quantile) in [("p50", 0.50), ("p95", 0.95), ("p99", 0.99)] {
                if let Ok(Some(value)) = histogram.quantile(quantile) {
                    push_metric_point(
                        inner,
                        capacity,
                        observed_at,
                        None,
                        format!("{op}_{suffix}_ms"),
                        BTreeMap::new(),
                        Some(value * 1_000.0),
                    );
                }
            }
        }
    }
}

fn counter_metric_name(name: &str) -> String {
    match name {
        "rr_server_requests_total" => "server_requests_tps".to_string(),
        "rr_raft_applied_entries_total" => "applied_entries_tps".to_string(),
        "rr_ingestor_commands_committed_total" => "ingestor_commands_tps".to_string(),
        "rr_snapshot_transfer_bytes" => "snapshot_transfer_bytes_per_second".to_string(),
        "rr_snapshot_installs_total" => "snapshot_installs_per_second".to_string(),
        "rr_node_network_bytes_total" => "node_network_bytes_per_second".to_string(),
        _ => format!("{}_per_second", name.trim_end_matches("_total")),
    }
}

fn push_metric_point(
    inner: &mut CacheInner,
    capacity: usize,
    observed_at: SystemTime,
    node_id: Option<u64>,
    metric: String,
    labels: BTreeMap<String, String>,
    value: Option<f64>,
) {
    inner.points_dirty = true;
    inner.points.push_back(TimeSeriesPoint {
        timestamp: rfc3339(observed_at),
        node_id,
        metric,
        labels,
        value,
        source: "PROMETHEUS_SCRAPE".to_string(),
    });
    while inner.points.len() > capacity {
        inner.points.pop_front();
    }
}

fn snapshot_boot_id(snapshot: &NodeSnapshot) -> Option<&str> {
    snapshot
        .boot
        .as_ref()
        .and_then(|boot| boot.current.as_ref())
        .map(|current| current.boot_id.as_str())
        .or_else(|| snapshot.build.as_ref().map(|build| build.boot_id.as_str()))
}

fn snapshot_version(snapshot: &NodeSnapshot) -> Option<(String, String, String)> {
    snapshot.build.as_ref().map(|build| {
        (
            build.app_version.clone(),
            build.rr_version.clone(),
            build.git_commit.clone(),
        )
    })
}

fn snapshot_identity(snapshot: &NodeSnapshot) -> Option<(String, u64)> {
    snapshot.descriptor.as_ref().map(|evidence| {
        (
            evidence.descriptor.boot_id.clone(),
            evidence.directory_revision,
        )
    })
}

fn validate_identity(inner: &CacheInner, snapshot: &NodeSnapshot) -> Result<(), UpstreamError> {
    for existing in inner.nodes.values() {
        let Some(existing_snapshot) = &existing.snapshot else {
            continue;
        };
        let (Some(new_formation), Some(existing_formation)) = (
            snapshot.formation.as_ref(),
            existing_snapshot.formation.as_ref(),
        ) else {
            continue;
        };
        if new_formation.cluster_uuid != existing_formation.cluster_uuid {
            return Err(UpstreamError {
                code: "CLUSTER_IDENTITY_CONFLICT",
                message: "mixed or conflicting cluster UUIDs were quarantined".to_string(),
            });
        }
    }
    Ok(())
}

fn update_leader_conflicts(inner: &mut CacheInner, now: SystemTime) {
    inner.leader_conflict_recomputations = inner.leader_conflict_recomputations.saturating_add(1);
    let mut groups: BTreeMap<u64, BTreeMap<u64, BTreeSet<u64>>> = BTreeMap::new();
    for node in inner.nodes.values() {
        if node.error.is_some() {
            continue;
        }
        let Some(snapshot) = &node.snapshot else {
            continue;
        };
        for group in snapshot.groups.iter().flatten() {
            if let Some(leader) = group.leader_node_id.filter(|leader| *leader != 0) {
                groups
                    .entry(group.group_id)
                    .or_default()
                    .entry(group.current_term)
                    .or_default()
                    .insert(leader);
            }
        }
    }
    let current = groups
        .into_iter()
        .filter(|(_, terms)| terms.len() == 1)
        .filter_map(|(group_id, terms)| {
            terms.into_iter().next().and_then(|(term, leaders)| {
                (leaders.len() > 1).then(|| (group_id, term, leaders.into_iter().collect()))
            })
        })
        .collect::<BTreeSet<_>>();
    inner
        .leader_conflicts
        .retain(|key, _| current.contains(key));
    for key in current {
        inner.leader_conflicts.entry(key).or_insert(now);
    }
}

fn push_event(
    inner: &mut CacheInner,
    capacity: usize,
    node_id: Option<u64>,
    group_id: Option<u64>,
    severity: &str,
    kind: &str,
    message: &str,
    observed_at: SystemTime,
) {
    inner.events_dirty = true;
    let now = rfc3339(observed_at);
    if kind == "NODE_RECOVERED" {
        for event in inner.events.iter_mut().filter(|event| {
            event.kind == "NODE_UNREACHABLE" && event.node_id == node_id && event.state == "ACTIVE"
        }) {
            event.state = "RESOLVED".to_string();
            event.last_observed_at = now.clone();
        }
    }
    if let Some(existing) = inner.events.iter_mut().find(|event| {
        event.kind == kind
            && event.node_id == node_id
            && event.group_id == group_id
            && event.state == "ACTIVE"
    }) {
        existing.severity = severity.to_string();
        existing.last_observed_at = now;
        existing.message = message.to_string();
        return;
    }
    inner.events.push_back(EventView {
        id: uuid::Uuid::new_v4().to_string(),
        severity: severity.to_string(),
        kind: kind.to_string(),
        node_id,
        group_id,
        first_observed_at: now.clone(),
        last_observed_at: now,
        state: if matches!(
            kind,
            "NODE_RECOVERED" | "NODE_RESTARTED" | "NODE_VERSION_CHANGED"
        ) {
            "RESOLVED"
        } else {
            "ACTIVE"
        }
        .to_string(),
        message: message.to_string(),
    });
    while inner.events.len() > capacity {
        inner.events.pop_front();
    }
}

fn push_point(inner: &mut CacheInner, capacity: usize, node_id: u64, value: f64, at: SystemTime) {
    inner.points_dirty = true;
    inner.points.push_back(TimeSeriesPoint {
        timestamp: rfc3339(at),
        node_id: Some(node_id),
        metric: "admin_contact".to_string(),
        labels: BTreeMap::new(),
        value: Some(value),
        source: "AdminService".to_string(),
    });
    while inner.points.len() > capacity {
        inner.points.pop_front();
    }
}

fn leader_evidence_is_complete(
    details: &[(u64, &GroupDetail)],
    leader: Option<u64>,
    expected_voters: &BTreeSet<u64>,
) -> bool {
    let Some(leader) = leader else {
        return false;
    };
    details
        .iter()
        .find(|(node_id, _)| *node_id == leader)
        .and_then(|(_, detail)| detail.replica.as_ref())
        .is_some_and(|replica| {
            replica.server_state == "Leader"
                && expected_voters
                    .iter()
                    .filter(|node_id| **node_id != leader)
                    .all(|node_id| {
                        replica
                            .leader_replication
                            .iter()
                            .find(|follower| follower.node_id == *node_id)
                            .is_some_and(|follower| follower.lag_entries.is_some())
                    })
        })
}

fn group_views_from(view: &CacheView) -> Vec<Value> {
    let mut groups: BTreeMap<u64, Vec<(u64, GroupSummary, SystemTime, bool)>> = BTreeMap::new();
    let mut observation_times = BTreeMap::new();
    for node in view.nodes.iter() {
        let Some(snapshot) = &node.snapshot else {
            continue;
        };
        observation_times.insert(node.peer.node_id, rfc3339(snapshot.collected_at));
        for group in snapshot.groups.iter().flatten() {
            groups.entry(group.group_id).or_default().push((
                node.peer.node_id,
                *group,
                snapshot.collected_at,
                node.error.is_none()
                    && node.last_success.is_some_and(|last_success| {
                        last_success.elapsed().unwrap_or(Duration::MAX) <= view.stale_after
                    }),
            ));
        }
    }
    let expected_groups = view.expected_groups();
    let group_ids = groups
        .keys()
        .chain(expected_groups.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    group_ids
        .into_iter()
        .map(|group_id| {
            let observations = groups.remove(&group_id).unwrap_or_default();
            let expected = expected_groups.get(&group_id).cloned().unwrap_or_default();
            let fresh = observations
                .iter()
                .filter(|(_, _, _, online)| *online)
                .collect::<Vec<_>>();
            let terms = fresh
                .iter()
                .map(|(_, group, _, _)| group.current_term)
                .collect::<BTreeSet<_>>();
            let leaders = fresh
                .iter()
                .filter_map(|(_, group, _, _)| group.leader_node_id.filter(|id| *id != 0))
                .collect::<BTreeSet<_>>();
            let expected_voters = expected.iter().copied().collect::<BTreeSet<_>>();
            let reachable = fresh
                .iter()
                .map(|(node_id, _, _, _)| *node_id)
                .filter(|node_id| expected_voters.contains(node_id))
                .collect::<BTreeSet<_>>()
                .len();
            let majority = expected.len() / 2 + 1;
            let details = view
                .nodes
                .iter()
                .filter_map(|node| node.details.get(&group_id).map(|detail| (node.peer.node_id, detail)))
                .filter(|(_, detail)| {
                    detail.collected_at.elapsed().unwrap_or(Duration::MAX) <= view.stale_after
                })
                .collect::<Vec<_>>();
            let membership_is_complete = details.iter().any(|(_, detail)| {
                detail.membership.as_ref().is_some_and(|membership| {
                    membership
                        .voters
                        .iter()
                        .map(|voter| voter.node_id)
                        .collect::<BTreeSet<_>>()
                        == expected_voters
                })
            });
            let lag_is_complete = leader_evidence_is_complete(
                &details,
                leaders.iter().next().copied(),
                &expected_voters,
            );
            let health = if terms.len() > 1 {
                "SYNCING"
            } else if leaders.len() > 1 {
                let key = (
                    group_id,
                    terms.iter().next().copied().unwrap_or_default(),
                    leaders.iter().copied().collect::<Vec<_>>(),
                );
                let sustained = view.leader_conflicts.get(&key).is_some_and(|first_seen| {
                    SystemTime::now()
                        .duration_since(*first_seen)
                        .unwrap_or(Duration::ZERO)
                        >= Duration::from_secs(10)
                });
                if sustained { "CONFLICTING_VIEWS" } else { "SYNCING" }
            } else if !expected.is_empty() && reachable < majority {
                "QUORUM_AT_RISK"
            } else if fresh.is_empty() {
                "DEGRADED"
            } else if leaders.is_empty() {
                "LEADERLESS_OBSERVED"
            } else if !expected.is_empty() && reachable < expected.len() {
                "DEGRADED"
            } else if expected.is_empty() || !membership_is_complete || !lag_is_complete {
                "DEGRADED"
            } else {
                "HEALTHY"
            };
            let leader = leaders.iter().next().copied();
            let current_term = fresh.iter().map(|(_, group, _, _)| group.current_term).max();
            let last_log_index = fresh.iter().map(|(_, group, _, _)| group.last_log_index).max();
            let observations = observations
                .into_iter()
                .map(|(node_id, group, _, _)| GroupObservation {
                    node_id,
                    group_id: group.group_id,
                    is_leader: group.is_leader,
                    current_term: group.current_term,
                    last_log_index: group.last_log_index,
                    leader_node_id: group.leader_node_id.filter(|id| *id != 0),
                    observed_at: observation_times[&node_id].clone(),
                })
                .collect::<Vec<_>>();
            let conflicting_views = if health == "CONFLICTING_VIEWS" {
                json!(observations.clone())
            } else {
                Value::Null
            };
            json!({
                "group_id": group_id,
                "health": health,
                "leader_node_id": leader,
                "current_term": current_term,
                "last_log_index": last_log_index,
                "expected_replica_node_ids": expected,
                "reachable_voters": reachable,
                "voters_total": if expected.is_empty() { Value::Null } else { json!(expected.len()) },
                "source_count": observations.len(),
                "observations": observations,
                "conflicting_views": conflicting_views,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(node_id: u64) -> SeedPeer {
        SeedPeer {
            node_id,
            admin_addr: format!("127.0.0.1:{}", 52000 + node_id),
        }
    }

    fn snapshot(node_id: u64, leader: Option<u64>, term: u64) -> NodeSnapshot {
        NodeSnapshot {
            node_id,
            listen_addr: format!("127.0.0.1:{}", 51000 + node_id),
            uptime_secs: 10,
            groups: Some(vec![GroupSummary {
                group_id: 1,
                is_leader: leader == Some(node_id),
                current_term: term,
                last_log_index: 20,
                leader_node_id: leader,
            }]),
            routes: Some(vec![]),
            formation: None,
            descriptor: None,
            build: None,
            boot: None,
            topology: None,
            capabilities: vec![Capability::supported("groups")],
            collected_at: SystemTime::now(),
        }
    }

    fn versioned_snapshot(node_id: u64, boot_id: &str, app_version: &str) -> NodeSnapshot {
        let mut snapshot = snapshot(node_id, Some(1), 4);
        let build = rr_network::proto::admin::BuildInfo {
            rr_version: "0.2.0".to_string(),
            app_version: app_version.to_string(),
            git_commit: "abc123".to_string(),
            git_dirty: false,
            build_profile: "test".to_string(),
            admin_api: None,
            raft_wire: None,
            boot_id: boot_id.to_string(),
            process_start_unix_ms: 1_700_000_000_000,
            config_fingerprint: "config-a".to_string(),
        };
        snapshot.build = Some(build.clone());
        snapshot.boot = Some(rr_network::proto::admin::BootStatus {
            current: Some(rr_network::proto::admin::BootRecord {
                boot_id: boot_id.to_string(),
                started_at_unix_ms: 1_700_000_000_000,
                ended_at_unix_ms: None,
                shutdown_state: "RUNNING".to_string(),
                build_info: Some(build),
            }),
            persistence: "PERSISTENT".to_string(),
        });
        snapshot
    }

    #[test]
    fn stale_snapshot_is_retained_and_marked_partial_after_failure() {
        let cache = ConsoleCache::new("test", Duration::from_secs(5), 8, 8);
        cache
            .record_success(&peer(1), snapshot(1, Some(1), 2))
            .unwrap();
        let past = SystemTime::now() - Duration::from_secs(30);
        cache.record_failure_at(&peer(1), "ADMIN_UNAVAILABLE", "offline", past);
        let view = cache.view_at(SystemTime::now());
        assert_eq!(view.nodes[0].snapshot.as_ref().unwrap().uptime_secs, 10);
        assert_eq!(view.nodes[0].sources[0].status, "STALE");
        assert!(view.partial);
        assert_eq!(
            view.nodes[0].sources[0].error_code.as_deref(),
            Some("ADMIN_UNAVAILABLE")
        );
    }

    #[test]
    fn repeated_failures_update_one_event_and_recovery_resolves_it() {
        let cache = ConsoleCache::new("test", Duration::from_secs(5), 8, 8);
        let first = SystemTime::now() - Duration::from_secs(30);
        let second = SystemTime::now() - Duration::from_secs(20);
        cache.record_failure_at(&peer(1), "ADMIN_UNAVAILABLE", "offline-1", first);
        cache.record_failure_at(&peer(1), "ADMIN_UNAVAILABLE", "offline-2", second);

        let initial = cache.view().events.clone();
        assert_eq!(initial.len(), 1, "repeated failures must upsert one event");
        assert_eq!(initial[0].message, "offline-2");
        assert_eq!(initial[0].first_observed_at, rfc3339(first));
        assert_eq!(initial[0].last_observed_at, rfc3339(second));

        let recovered_at = SystemTime::now();
        let mut recovered = snapshot(1, Some(1), 2);
        recovered.collected_at = recovered_at;
        cache.record_success(&peer(1), recovered).unwrap();

        let events = cache.view().events.clone();
        let unreachable = events
            .iter()
            .find(|event| event.kind == "NODE_UNREACHABLE")
            .unwrap();
        assert_eq!(unreachable.id, initial[0].id);
        assert_eq!(unreachable.state, "RESOLVED");
        assert_eq!(unreachable.first_observed_at, rfc3339(first));
        assert_eq!(unreachable.last_observed_at, rfc3339(recovered_at));
        assert!(events.iter().any(|event| {
            event.kind == "NODE_RECOVERED"
                && event.state == "RESOLVED"
                && event.last_observed_at == rfc3339(recovered_at)
        }));
    }

    #[test]
    fn healthy_cold_reconciliation_never_emits_intermediate_group_alerts() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 16, 8);
        cache.set_desired_topology(vec![], BTreeMap::from([(1, vec![1, 2, 3])]), 1);
        assert!(cache
            .view()
            .events
            .iter()
            .all(|event| event.kind != "GROUP_HEALTH"));

        for node_id in [1, 2, 3] {
            cache
                .record_success_for_reconcile(&peer(node_id), snapshot(node_id, Some(1), 4))
                .unwrap();
            assert!(
                cache
                    .view()
                    .events
                    .iter()
                    .all(|event| { event.kind != "GROUP_HEALTH" || event.state != "ACTIVE" }),
                "node {node_id} completed before the batch; no quorum alert is valid yet"
            );
        }
        cache.refresh_leader_conflicts();
        assert!(cache
            .view()
            .events
            .iter()
            .all(|event| event.kind != "GROUP_HEALTH"));
    }

    #[test]
    fn group_health_events_track_group_id_and_resolve_when_alert_conditions_clear() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 16, 8);
        cache.set_desired_topology(vec![], BTreeMap::from([(1, vec![1, 2, 3])]), 1);
        for node_id in [1, 2, 3] {
            cache
                .record_success(&peer(node_id), snapshot(node_id, None, 4))
                .unwrap();
        }
        let events = cache.view().events.clone();
        let leaderless = events
            .iter()
            .find(|event| event.kind == "GROUP_HEALTH" && event.group_id == Some(1))
            .unwrap();
        assert_eq!(leaderless.state, "ACTIVE");
        assert!(leaderless.message.contains("LEADERLESS_OBSERVED"));

        for node_id in [1, 2, 3] {
            cache
                .record_success(&peer(node_id), snapshot(node_id, Some(1), 4))
                .unwrap();
        }
        let events = cache.view().events.clone();
        assert!(
            events
                .iter()
                .find(|event| event.kind == "GROUP_HEALTH" && event.group_id == Some(1))
                .unwrap()
                .state
                == "RESOLVED"
        );

        for node_id in [2, 3] {
            cache.record_failure(&peer(node_id), "ADMIN_UNAVAILABLE", "offline");
            cache.record_failure(&peer(node_id), "ADMIN_UNAVAILABLE", "offline");
        }
        let events = cache.view().events.clone();
        let active = events
            .iter()
            .filter(|event| {
                event.kind == "GROUP_HEALTH" && event.group_id == Some(1) && event.state == "ACTIVE"
            })
            .collect::<Vec<_>>();
        assert_eq!(active.len(), 1);
        assert!(active[0].message.contains("QUORUM_AT_RISK"));

        for node_id in [2, 3] {
            cache
                .record_success(&peer(node_id), snapshot(node_id, Some(1), 4))
                .unwrap();
        }
        let events = cache.view().events.clone();
        assert!(
            events
                .iter()
                .find(|event| event.kind == "GROUP_HEALTH" && event.group_id == Some(1))
                .unwrap()
                .state
                == "RESOLVED"
        );
    }

    #[test]
    fn restart_and_version_changes_are_recorded_as_resolved_observations() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 16, 8);
        cache
            .record_success(&peer(1), versioned_snapshot(1, "boot-a", "app-1"))
            .unwrap();
        cache
            .record_success(&peer(1), versioned_snapshot(1, "boot-a", "app-1"))
            .unwrap();
        cache.record_failure(&peer(1), "ADMIN_UNAVAILABLE", "offline");
        cache
            .record_success(&peer(1), snapshot(1, Some(1), 4))
            .unwrap();
        cache
            .record_success(&peer(1), versioned_snapshot(1, "boot-b", "app-1"))
            .unwrap();
        cache
            .record_success(&peer(1), versioned_snapshot(1, "boot-c", "app-2"))
            .unwrap();

        let events = cache.view().events.clone();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == "NODE_RESTARTED" && event.state == "RESOLVED")
                .count(),
            2
        );
        let version = events
            .iter()
            .find(|event| event.kind == "NODE_VERSION_CHANGED")
            .unwrap();
        assert_eq!(version.state, "RESOLVED");
        assert!(version.message.contains("app-1"));
        assert!(version.message.contains("app-2"));
    }

    #[test]
    fn a_seed_cannot_replace_its_own_cluster_identity() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let mut first = snapshot(1, Some(1), 2);
        first.formation = Some(FormationStatus {
            origin: "Genesis".to_string(),
            cluster_uuid: "cluster-a".to_string(),
            topology_revision: 1,
            store_revision: 1,
            state: "Active".to_string(),
            groups: vec![],
        });
        cache.record_success(&peer(1), first).unwrap();
        let mut changed = snapshot(1, Some(1), 3);
        changed.formation = Some(FormationStatus {
            origin: "Genesis".to_string(),
            cluster_uuid: "cluster-b".to_string(),
            topology_revision: 1,
            store_revision: 1,
            state: "Active".to_string(),
            groups: vec![],
        });
        assert_eq!(
            cache.record_success(&peer(1), changed).unwrap_err().code,
            "CLUSTER_IDENTITY_CONFLICT"
        );
        assert_eq!(
            cache.view().nodes[0]
                .snapshot
                .as_ref()
                .unwrap()
                .formation
                .as_ref()
                .unwrap()
                .cluster_uuid,
            "cluster-a"
        );
    }

    #[test]
    fn unsupported_formation_does_not_conflict_with_a_tracked_peer() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let mut tracked = snapshot(1, Some(1), 2);
        tracked.formation = Some(FormationStatus {
            origin: "Genesis".to_string(),
            cluster_uuid: "cluster-a".to_string(),
            topology_revision: 1,
            store_revision: 1,
            state: "Active".to_string(),
            groups: vec![],
        });
        cache.record_success(&peer(1), tracked).unwrap();
        let legacy = snapshot(2, Some(1), 2);
        assert!(legacy.formation.is_none());
        cache.record_success(&peer(2), legacy).unwrap();
        assert_eq!(cache.view().nodes.len(), 2);
        assert_eq!(cache.view().cluster_id, "cluster-a");
    }

    #[test]
    fn mixed_tracked_and_untracked_cluster_identities_are_quarantined() {
        let cache = ConsoleCache::new("test", Duration::from_secs(5), 8, 8);
        let mut tracked = snapshot(1, Some(1), 2);
        tracked.formation = Some(FormationStatus {
            origin: "Genesis".to_string(),
            cluster_uuid: "cluster-a".to_string(),
            topology_revision: 1,
            store_revision: 1,
            state: "Active".to_string(),
            groups: vec![],
        });
        cache.record_success(&peer(1), tracked).unwrap();
        let mut untracked = snapshot(2, Some(1), 2);
        untracked.formation = Some(FormationStatus {
            origin: "Untracked".to_string(),
            cluster_uuid: String::new(),
            topology_revision: 0,
            store_revision: 0,
            state: "Untracked".to_string(),
            groups: vec![],
        });
        let error = cache.record_success(&peer(2), untracked).unwrap_err();
        assert_eq!(error.code, "CLUSTER_IDENTITY_CONFLICT");
        assert_eq!(cache.view().nodes.len(), 1);
    }

    #[test]
    fn all_untracked_nodes_use_the_operator_identity_with_a_visible_warning() {
        let cache = ConsoleCache::new("operator-cluster", Duration::from_secs(30), 8, 8);
        for node_id in 1..=2 {
            let mut untracked = snapshot(node_id, Some(1), 2);
            untracked.formation = Some(FormationStatus {
                origin: "Untracked".to_string(),
                cluster_uuid: String::new(),
                topology_revision: 0,
                store_revision: 0,
                state: "Untracked".to_string(),
                groups: vec![],
            });
            cache.record_success(&peer(node_id), untracked).unwrap();
        }
        let view = cache.view();
        assert_eq!(view.cluster_id, "operator-cluster");
        assert!(!view.quarantined);
        assert!(view.capabilities.iter().any(|capability| {
            capability.feature == "cluster_identity" && capability.status == "UNTRACKED"
        }));
    }

    #[test]
    fn identity_conflict_latches_quarantine_and_capability_evidence() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let mut first = snapshot(1, Some(1), 2);
        first.formation = Some(FormationStatus {
            origin: "Genesis".to_string(),
            cluster_uuid: "cluster-a".to_string(),
            topology_revision: 1,
            store_revision: 1,
            state: "Active".to_string(),
            groups: vec![],
        });
        cache.record_success(&peer(1), first).unwrap();
        let mut conflicting = snapshot(2, Some(1), 2);
        conflicting.formation = Some(FormationStatus {
            origin: "Genesis".to_string(),
            cluster_uuid: "cluster-b".to_string(),
            topology_revision: 1,
            store_revision: 1,
            state: "Active".to_string(),
            groups: vec![],
        });
        assert!(cache.record_success(&peer(2), conflicting).is_err());
        let view = cache.view();
        assert!(view.quarantined);
        assert!(cache.is_quarantined());
        assert!(view.capabilities.iter().any(|capability| {
            capability.feature == "cluster_identity" && capability.status == "ERROR"
        }));
    }

    #[test]
    fn unchanged_point_and_event_views_share_snapshots_without_losing_evidence() {
        let cache = ConsoleCache::new("test", Duration::from_secs(5), 8, 8);
        cache.record_failure(&peer(1), "ADMIN_UNAVAILABLE", "offline");
        let first = cache.view();
        let second = cache.view();

        assert!(Arc::ptr_eq(&first.points, &second.points));
        assert!(Arc::ptr_eq(&first.events, &second.events));
        assert_eq!(second.points.len(), 1);
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.points[0].labels, first.points[0].labels);

        cache.record_failure(&peer(2), "ADMIN_UNAVAILABLE", "offline");
        let mutated = cache.view();
        assert!(!Arc::ptr_eq(&first.points, &mutated.points));
        assert!(!Arc::ptr_eq(&first.events, &mutated.events));
        assert_eq!(mutated.points.len(), 2);
        assert_eq!(mutated.events.len(), 2);
    }

    #[test]
    fn event_and_point_rings_are_bounded() {
        let cache = ConsoleCache::new("test", Duration::from_secs(5), 2, 2);
        for node_id in 1..=5 {
            cache.record_failure(&peer(node_id), "ADMIN_UNAVAILABLE", "offline");
            cache.record_failure(&peer(node_id + 10), "ADMIN_UNAVAILABLE", "offline");
        }
        let view = cache.view();
        assert!(view.events.len() <= 2);
        assert!(view.points.len() <= 2);
    }

    #[test]
    fn same_term_leader_disagreement_requires_a_grace_period() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        cache
            .record_success(&peer(1), snapshot(1, Some(1), 4))
            .unwrap();
        cache
            .record_success(&peer(2), snapshot(2, Some(2), 4))
            .unwrap();
        let groups = cache.view().group_views();
        assert_eq!(groups[0]["health"], "SYNCING");
        assert!(groups[0]["conflicting_views"].is_null());

        let mut inner = cache.inner.write().unwrap();
        for first_seen in inner.leader_conflicts.values_mut() {
            *first_seen = SystemTime::now() - Duration::from_secs(11);
        }
        drop(inner);
        let groups = cache.view().group_views();
        assert_eq!(groups[0]["health"], "CONFLICTING_VIEWS");
        assert!(groups[0]["conflicting_views"].is_array());
    }

    #[test]
    fn different_terms_are_syncing_not_conflicting_leaders() {
        let cache = ConsoleCache::new("test", Duration::from_secs(5), 8, 8);
        cache
            .record_success(&peer(1), snapshot(1, Some(1), 4))
            .unwrap();
        cache
            .record_success(&peer(2), snapshot(2, Some(2), 5))
            .unwrap();
        let groups = cache.view().group_views();
        assert_eq!(groups[0]["health"], "SYNCING");
        assert!(groups[0]["conflicting_views"].is_null());
    }

    #[test]
    fn fresh_learners_do_not_count_toward_expected_voter_quorum() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        cache.set_desired_topology(vec![], BTreeMap::from([(1, vec![1, 2, 3])]), 1);
        for node_id in [1, 4, 5] {
            cache
                .record_success(&peer(node_id), snapshot(node_id, Some(1), 4))
                .unwrap();
        }

        let groups = cache.view().group_views();
        assert_eq!(groups[0]["reachable_voters"], 1);
        assert_eq!(groups[0]["health"], "QUORUM_AT_RISK");
    }

    #[test]
    fn missing_membership_and_lag_evidence_never_becomes_healthy() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        cache.set_desired_topology(vec![], BTreeMap::from([(1, vec![1, 2])]), 1);
        for node_id in [1, 2] {
            cache
                .record_success(&peer(node_id), snapshot(node_id, Some(1), 4))
                .unwrap();
        }

        assert_eq!(cache.view().group_views()[0]["health"], "DEGRADED");

        let detail = GroupDetail {
            status: rr_network::proto::admin::GroupStatus {
                group_id: 1,
                is_leader: true,
                current_term: 4,
                last_log_index: 20,
                leader_node_id: 1,
            },
            membership: Some(rr_network::proto::admin::GroupMembership {
                group_id: 1,
                voters: vec![
                    rr_network::proto::admin::NodeMember {
                        node_id: 1,
                        addr: "127.0.0.1:52001".to_string(),
                    },
                    rr_network::proto::admin::NodeMember {
                        node_id: 2,
                        addr: "127.0.0.1:52002".to_string(),
                    },
                ],
                learners: vec![],
            }),
            snapshot: None,
            replica: None,
            capabilities: vec![],
            collected_at: SystemTime::now(),
        };
        cache.record_detail(&peer(1), 1, detail);
        assert_eq!(cache.view().group_views()[0]["health"], "DEGRADED");
    }

    #[test]
    fn fresh_expected_voters_with_membership_and_lag_evidence_can_be_healthy() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        cache.set_desired_topology(vec![], BTreeMap::from([(1, vec![1, 2])]), 1);
        for node_id in [1, 2] {
            cache
                .record_success(&peer(node_id), snapshot(node_id, Some(1), 4))
                .unwrap();
        }
        cache.record_detail(
            &peer(1),
            1,
            GroupDetail {
                status: rr_network::proto::admin::GroupStatus {
                    group_id: 1,
                    is_leader: true,
                    current_term: 4,
                    last_log_index: 20,
                    leader_node_id: 1,
                },
                membership: Some(rr_network::proto::admin::GroupMembership {
                    group_id: 1,
                    voters: [1, 2]
                        .into_iter()
                        .map(|node_id| rr_network::proto::admin::NodeMember {
                            node_id,
                            addr: format!("127.0.0.1:5200{node_id}"),
                        })
                        .collect(),
                    learners: vec![],
                }),
                snapshot: None,
                replica: Some(rr_network::proto::admin::GroupReplicaStatus {
                    group_id: 1,
                    node_id: 1,
                    server_state: "Leader".to_string(),
                    leader_node_id: Some(1),
                    current_term: 4,
                    last_log_index: Some(20),
                    local_commit_index: Some(20),
                    cluster_commit_index: Some(20),
                    applied_index: Some(20),
                    last_snapshot_index: None,
                    last_snapshot_term: None,
                    leader_replication: vec![rr_network::proto::admin::LeaderReplicaStatus {
                        node_id: 2,
                        matched_index: Some(20),
                        lag_entries: Some(0),
                        last_heartbeat_unix_ms: None,
                    }],
                    observed_at_unix_ms: 0,
                }),
                capabilities: vec![],
                collected_at: SystemTime::now(),
            },
        );

        assert_eq!(cache.view().group_views()[0]["health"], "HEALTHY");
    }

    #[test]
    fn late_detail_and_metrics_from_previous_address_are_not_cached() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let old_peer = peer(1);
        let new_peer = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52999".to_string(),
        };
        cache
            .record_success(&new_peer, snapshot(1, Some(1), 4))
            .unwrap();
        cache.record_detail(
            &old_peer,
            1,
            GroupDetail {
                status: rr_network::proto::admin::GroupStatus {
                    group_id: 1,
                    is_leader: true,
                    current_term: 4,
                    last_log_index: 20,
                    leader_node_id: 1,
                },
                membership: None,
                snapshot: None,
                replica: None,
                capabilities: vec![],
                collected_at: SystemTime::now(),
            },
        );
        cache.record_metrics_batch(vec![metrics_scrape(1, 10, 1, 1, SystemTime::now())]);

        assert!(cache.view().nodes[0].details.is_empty());
        assert!(cache.view().nodes[0].metrics.is_none());
    }

    #[test]
    fn metrics_degraded_node_remains_fresh_admin_voter_evidence() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        cache.set_desired_topology(vec![], BTreeMap::from([(1, vec![1, 2])]), 1);
        for node_id in [1, 2] {
            cache
                .record_success(&peer(node_id), snapshot(node_id, Some(1), 4))
                .unwrap();
        }
        cache.record_metrics_failure(&peer(2), "SCRAPE_HTTP_ERROR", "metrics failed");

        let view = cache.view();
        assert_eq!(view.nodes[1].sources[0].status, "DEGRADED");
        assert_eq!(view.group_views()[0]["reachable_voters"], 2);
    }

    #[test]
    fn admin_online_with_failed_metrics_is_degraded() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        cache
            .record_success(&peer(1), snapshot(1, Some(1), 4))
            .unwrap();
        cache.record_metrics_failure(&peer(1), "SCRAPE_HTTP_ERROR", "metrics failed");

        assert_eq!(cache.view().nodes[0].sources[0].status, "DEGRADED");
    }

    fn metrics_scrape(
        node_id: u64,
        requests: u64,
        installs: u64,
        leader: u64,
        at: SystemTime,
    ) -> super::NodeMetricsScrape {
        let input = format!(
            r#"# TYPE rr_server_requests_total counter
rr_server_requests_total{{group_id="7",op="write",outcome="success"}} {requests}
# TYPE rr_snapshot_installs_total counter
rr_snapshot_installs_total{{group_id="7"}} {installs}
# TYPE rr_snapshot_transfer_bytes counter
rr_snapshot_transfer_bytes{{group_id="7",direction="send"}} {requests}
# TYPE rr_raft_is_leader gauge
rr_raft_is_leader{{group_id="7"}} {leader}
# TYPE rr_raft_replication_lag gauge
rr_raft_replication_lag{{group_id="7",follower_node_id="2"}} 5
# TYPE rr_server_request_duration_seconds histogram
rr_server_request_duration_seconds_bucket{{group_id="7",op="write",le="0.1"}} 4
rr_server_request_duration_seconds_bucket{{group_id="7",op="write",le="0.5"}} 8
rr_server_request_duration_seconds_bucket{{group_id="7",op="write",le="+Inf"}} 8
rr_server_request_duration_seconds_sum{{group_id="7",op="write"}} 4
rr_server_request_duration_seconds_count{{group_id="7",op="write"}} 8
"#
        );
        super::NodeMetricsScrape {
            peer: peer(node_id),
            snapshot: crate::console::metrics::parse_prometheus(node_id, &input).unwrap(),
            observed_at: at,
        }
    }

    fn active_series_scrape(
        node_id: u64,
        requests: u64,
        at: SystemTime,
    ) -> super::NodeMetricsScrape {
        let mut samples = (0..10_100)
            .map(|group_id| MetricSample {
                node_id,
                name: "rr_raft_is_leader".to_string(),
                labels: BTreeMap::from([("group_id".to_string(), group_id.to_string())]),
                value: 1.0,
                timestamp_ms: None,
                kind: MetricKind::Gauge,
            })
            .collect::<Vec<_>>();
        samples.push(MetricSample {
            node_id,
            name: "rr_node_cpu_usage_ratio".to_string(),
            labels: BTreeMap::new(),
            value: 0.25,
            timestamp_ms: None,
            kind: MetricKind::Gauge,
        });
        samples.extend((0..500).map(|group_id| MetricSample {
            node_id,
            name: "rr_server_requests_total".to_string(),
            labels: BTreeMap::from([
                ("group_id".to_string(), group_id.to_string()),
                ("op".to_string(), "write".to_string()),
            ]),
            value: requests as f64,
            timestamp_ms: None,
            kind: MetricKind::Counter,
        }));
        super::NodeMetricsScrape {
            peer: peer(node_id),
            snapshot: MetricsSnapshot { node_id, samples },
            observed_at: at,
        }
    }

    #[test]
    fn active_series_are_downsampled_cluster_wide_with_a_visible_event() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 64);
        cache.register_peers(&[peer(1), peer(2)]);
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        cache.record_metrics_batch(vec![
            active_series_scrape(1, 10, start),
            active_series_scrape(2, 20, start),
        ]);
        cache.record_metrics_batch(vec![
            active_series_scrape(1, 15, start + Duration::from_secs(5)),
            active_series_scrape(2, 30, start + Duration::from_secs(5)),
        ]);

        let view = cache.view();
        let active = view
            .nodes
            .iter()
            .map(|node| {
                let metrics = node.metrics.as_ref().unwrap();
                metrics.samples.len() + metrics.rates.len()
            })
            .sum::<usize>();
        assert_eq!(active, 20_000);
        assert!(view.nodes.iter().all(|node| {
            node.metrics
                .as_ref()
                .unwrap()
                .samples
                .iter()
                .any(|sample| sample.name == "rr_node_cpu_usage_ratio")
        }));
        assert!(view.events.iter().rev().any(|event| {
            event.kind == "ACTIVE_SERIES_DOWNSAMPLED"
                && event.message.contains("21202")
                && event.message.contains("20000")
        }));
    }

    #[test]
    fn subset_refresh_retains_the_pre_downsample_series_evidence() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 64);
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        cache.register_peers(&[peer(1)]);
        let mut first = active_series_scrape(1, 10, start);
        first.snapshot.samples.truncate(10_000);
        cache.record_metrics_batch(vec![first]);
        cache.register_peers(&[peer(2)]);

        let mut second = active_series_scrape(2, 20, start + Duration::from_secs(5));
        second
            .snapshot
            .samples
            .extend((20_000..24_899).map(|group_id| MetricSample {
                node_id: 2,
                name: "rr_raft_is_leader".to_string(),
                labels: BTreeMap::from([("group_id".to_string(), group_id.to_string())]),
                value: 1.0,
                timestamp_ms: None,
                kind: MetricKind::Gauge,
            }));
        cache.record_metrics_batch(vec![second]);

        let view = cache.view();
        let active = view
            .nodes
            .iter()
            .map(|node| {
                let metrics = node.metrics.as_ref().unwrap();
                metrics.samples.len() + metrics.rates.len()
            })
            .sum::<usize>();
        assert_eq!(active, 20_000);
        assert!(view.events.iter().rev().any(|event| {
            event.kind == "ACTIVE_SERIES_DOWNSAMPLED"
                && event.message.contains("25000")
                && event.message.contains("20000")
        }));
    }

    #[test]
    fn metrics_batches_project_reset_rates_histogram_quantiles_and_leader_only_lag() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 512);
        cache.register_peers(&[peer(1), peer(2)]);
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        cache.record_metrics_batch(vec![
            metrics_scrape(1, 10, 1, 1, start),
            metrics_scrape(2, 20, 2, 0, start),
        ]);
        cache.record_metrics_batch(vec![
            metrics_scrape(1, 15, 3, 1, start + Duration::from_secs(10)),
            metrics_scrape(2, 30, 4, 0, start + Duration::from_secs(10)),
        ]);

        let view = cache.view();
        let rate = view
            .points
            .iter()
            .find(|point| {
                point.node_id == Some(1)
                    && point.metric == "server_requests_tps"
                    && point.timestamp == rfc3339(start + Duration::from_secs(10))
            })
            .unwrap();
        assert_eq!(rate.value, Some(0.5));
        assert_eq!(rate.labels["group_id"], "7");
        assert_eq!(rate.labels["op"], "write");
        assert!(view.points.iter().any(|point| {
            point.node_id == Some(1)
                && point.metric == "rr_raft_replication_lag"
                && point.value == Some(5.0)
        }));
        assert!(!view.points.iter().any(|point| {
            point.node_id == Some(2) && point.metric == "rr_raft_replication_lag"
        }));
        let p99 = view
            .points
            .iter()
            .find(|point| point.node_id.is_none() && point.metric == "write_p99_ms")
            .unwrap();
        assert!((p99.value.unwrap() - 492.0).abs() < 1e-6);
        assert!(p99.labels.is_empty());

        cache.record_metrics_batch(vec![metrics_scrape(
            1,
            2,
            1,
            1,
            start + Duration::from_secs(20),
        )]);
        let view = cache.view();
        let reset_rate = view
            .points
            .iter()
            .find(|point| {
                point.node_id == Some(1)
                    && point.metric == "server_requests_tps"
                    && point.timestamp == rfc3339(start + Duration::from_secs(20))
            })
            .unwrap();
        assert_eq!(reset_rate.value, Some(0.2));
        assert!(view.points.iter().any(|point| {
            point.node_id == Some(1)
                && point.metric == "server_requests_tps"
                && point.value.is_none()
        }));
    }
}
