use super::cache::{ConsoleCache, NodeMetricsScrape, ProbeResult};
use super::config::{PrometheusConfig, SeedPeer};
use super::discovery::{validate_records, DiscoveryWatch, TrustedDiscovery};
use super::metrics::{MetricsResult, PrometheusHistoryClient, PrometheusScraper};
use super::model::BoundedLru;
use super::upstream::{topology_records, GroupDetail, NodeSnapshot, Upstream, UpstreamError};
use rr_core::NodeDirectoryRecord;
use rr_network::proto::admin::{BootRecordPage, ClusterTopology};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{watch, Mutex as AsyncMutex, Semaphore};
use tokio::task::JoinSet;

use futures::StreamExt;

type DetailFlight = Arc<AsyncMutex<()>>;
type DetailKey = (u64, u64, String, u64, Option<String>);
type BootPageKey = (u64, String, u64, Option<String>, Option<String>, usize);
type BootPageResult = Result<BootRecordPage, UpstreamError>;

const NON_SEED_TRUST_LEASE: Duration = Duration::from_secs(15);
#[cfg(not(test))]
const DISCOVERY_RECONCILE_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const DISCOVERY_RECONCILE_TIMEOUT: Duration = Duration::from_millis(50);
const ON_DEMAND_STATE_TTL: Duration = Duration::from_secs(30);
const FAILURE_CACHE_TTL: Duration = Duration::from_secs(1);
const MAX_ON_DEMAND_KEYS: usize = 256;
const MAX_ON_DEMAND_PARALLELISM: usize = 16;
const MAX_METRICS_PARALLELISM: usize = 4;
const MAX_PROBE_BODY_BYTES: usize = 16 * 1024;
const SUMMARY_NOTIFICATION_INTERVAL: Duration = Duration::from_secs(7);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerOrigin {
    ExplicitSeed,
    Directory,
    AuthenticatedTopology,
    AuthenticatedDescriptor,
}

#[derive(Clone, Debug)]
struct PeerEntry {
    peer: SeedPeer,
    origin: PeerOrigin,
    revision: u64,
    boot_id: Option<String>,
    trusted_at: Instant,
}

pub(crate) struct Scheduler {
    upstream: Arc<dyn Upstream>,
    pub cache: Arc<ConsoleCache>,
    peers: RwLock<BTreeMap<u64, PeerEntry>>,
    discovery: Option<Arc<TrustedDiscovery>>,
    scraper: Option<PrometheusScraper>,
    history: RwLock<Option<(PrometheusConfig, Arc<PrometheusHistoryClient>)>>,
    poll_interval: Duration,
    request_timeout: Duration,
    concurrency: Arc<Semaphore>,
    metrics_concurrency: Arc<Semaphore>,
    reconcile_flight: AsyncMutex<()>,
    detail_flights: Mutex<BoundedLru<DetailKey, DetailFlight>>,
    detail_failures: Mutex<BoundedLru<DetailKey, UpstreamError>>,
    boot_flights: Mutex<BoundedLru<BootPageKey, DetailFlight>>,
    boot_pages: Mutex<BoundedLru<BootPageKey, BootPageResult>>,
    prober: Option<HttpProber>,
    last_summary_notification: Mutex<Instant>,
    breakers: Mutex<HashMap<u64, CircuitBreaker>>,
}

impl Scheduler {
    pub fn new(
        upstream: Arc<dyn Upstream>,
        cache: Arc<ConsoleCache>,
        peers: Vec<SeedPeer>,
        poll_interval: Duration,
        request_timeout: Duration,
        max_concurrency: usize,
    ) -> Self {
        let peers = peers
            .into_iter()
            .map(|peer| {
                (
                    peer.node_id,
                    PeerEntry {
                        peer,
                        origin: PeerOrigin::ExplicitSeed,
                        revision: 0,
                        boot_id: None,
                        trusted_at: Instant::now(),
                    },
                )
            })
            .collect();
        Self {
            upstream,
            cache,
            peers: RwLock::new(peers),
            discovery: None,
            scraper: PrometheusScraper::new(request_timeout).ok(),
            history: RwLock::new(None),
            poll_interval,
            request_timeout,
            concurrency: Arc::new(Semaphore::new(max_concurrency.max(1))),
            metrics_concurrency: Arc::new(Semaphore::new(MAX_METRICS_PARALLELISM)),
            reconcile_flight: AsyncMutex::new(()),
            detail_flights: Mutex::new(BoundedLru::new(MAX_ON_DEMAND_KEYS, ON_DEMAND_STATE_TTL)),
            detail_failures: Mutex::new(BoundedLru::new(MAX_ON_DEMAND_KEYS, FAILURE_CACHE_TTL)),
            boot_flights: Mutex::new(BoundedLru::new(MAX_ON_DEMAND_KEYS, ON_DEMAND_STATE_TTL)),
            boot_pages: Mutex::new(BoundedLru::new(MAX_ON_DEMAND_KEYS, ON_DEMAND_STATE_TTL)),
            prober: HttpProber::new(request_timeout).ok(),
            last_summary_notification: Mutex::new(Instant::now() - SUMMARY_NOTIFICATION_INTERVAL),
            breakers: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_discovery(mut self, discovery: Arc<TrustedDiscovery>) -> Self {
        self.cache.set_source_mode("TRUSTED_DIRECTORY_AND_CONFIG");
        self.discovery = Some(discovery);
        self
    }

    pub fn configure_history(&self, config: PrometheusConfig) -> MetricsResult<()> {
        let client = PrometheusHistoryClient::new(
            &config.url,
            config.cluster.clone(),
            self.request_timeout,
        )?;
        *self.history.write().expect("Prometheus history lock") = Some((config, Arc::new(client)));
        Ok(())
    }

    pub fn history_client(&self) -> Option<Arc<PrometheusHistoryClient>> {
        self.history
            .read()
            .expect("Prometheus history lock")
            .as_ref()
            .map(|(_, client)| client.clone())
    }

    pub fn history_config(&self) -> Option<PrometheusConfig> {
        self.history
            .read()
            .expect("Prometheus history lock")
            .as_ref()
            .map(|(config, _)| config.clone())
    }

    fn peers(&self) -> Vec<SeedPeer> {
        self.peers
            .read()
            .expect("scheduler roster lock")
            .values()
            .map(|entry| entry.peer.clone())
            .collect()
    }

    fn peer(&self, node_id: u64) -> Option<SeedPeer> {
        self.expire_non_seed_trust(Instant::now());
        self.peers
            .read()
            .expect("scheduler roster lock")
            .get(&node_id)
            .map(|entry| entry.peer.clone())
    }

    fn record_peer_failure(&self, peer: &SeedPeer, code: &'static str, message: &str) {
        let roster = self.peers.read().expect("scheduler roster lock");
        if roster
            .get(&peer.node_id)
            .is_some_and(|current| current.peer.admin_addr == peer.admin_addr)
        {
            self.cache.record_failure_for_reconcile(peer, code, message);
        }
    }

    fn detail_key(&self, node_id: u64, group_id: u64) -> Option<DetailKey> {
        self.peers
            .read()
            .expect("scheduler roster lock")
            .get(&node_id)
            .map(|entry| {
                (
                    node_id,
                    group_id,
                    entry.peer.admin_addr.clone(),
                    entry.revision,
                    entry.boot_id.clone(),
                )
            })
    }

    fn boot_page_key(
        &self,
        node_id: u64,
        cursor: Option<String>,
        page_size: usize,
    ) -> Option<BootPageKey> {
        self.peers
            .read()
            .expect("scheduler roster lock")
            .get(&node_id)
            .map(|entry| {
                (
                    node_id,
                    entry.peer.admin_addr.clone(),
                    entry.revision,
                    entry.boot_id.clone(),
                    cursor,
                    page_size,
                )
            })
    }

    fn replace_directory_roster(
        &self,
        records: &[NodeDirectoryRecord],
    ) -> Result<Vec<SeedPeer>, UpstreamError> {
        let discovered = validate_records(records)?;
        self.validate_against_explicit_seeds(&discovered)?;
        let (removed, changed) = {
            let mut peers = self.peers.write().expect("scheduler roster lock");
            let mut changed = Vec::new();
            let keep = discovered
                .iter()
                .map(|peer| peer.node_id)
                .collect::<std::collections::BTreeSet<_>>();
            let removed = peers
                .values()
                .filter(|entry| {
                    entry.origin != PeerOrigin::ExplicitSeed && !keep.contains(&entry.peer.node_id)
                })
                .map(|entry| entry.peer.clone())
                .collect::<Vec<_>>();
            peers.retain(|node_id, entry| {
                entry.origin == PeerOrigin::ExplicitSeed || keep.contains(node_id)
            });
            for record in records {
                let node_id = record.descriptor.node_id;
                if peers
                    .get(&node_id)
                    .is_some_and(|entry| entry.origin == PeerOrigin::ExplicitSeed)
                {
                    continue;
                }
                let replacement = SeedPeer {
                    node_id,
                    admin_addr: record.descriptor.admin_addr.clone(),
                };
                if peers
                    .get(&node_id)
                    .is_some_and(|existing| existing.peer.admin_addr != replacement.admin_addr)
                {
                    changed.push(replacement.clone());
                }
                peers.insert(
                    node_id,
                    PeerEntry {
                        peer: replacement,
                        origin: PeerOrigin::Directory,
                        revision: record.revision,
                        boot_id: Some(record.descriptor.boot_id.clone()),
                        trusted_at: Instant::now(),
                    },
                );
            }
            (removed, changed)
        };
        for peer in changed {
            self.cache.record_failure(
                &peer,
                "TRUSTED_ADDRESS_CHANGED",
                "the trusted AdminService address changed; descriptors and on-demand state from the previous address were discarded",
            );
        }
        for peer in removed {
            self.cache.record_failure(
                &peer,
                "DIRECTORY_RECORD_REMOVED",
                "the trusted directory record was removed or its lease expired; credentials will not be forwarded",
            );
        }
        Ok(discovered)
    }

    fn merge_authenticated_topology(
        &self,
        topology: &ClusterTopology,
    ) -> Result<(), UpstreamError> {
        if self.discovery.is_some() {
            return Ok(());
        }
        let records = topology_records(topology)?;
        let discovered = validate_records(&records)?;
        self.validate_against_explicit_seeds(&discovered)?;
        let mut peers = self.peers.write().expect("scheduler roster lock");
        let mut changed = Vec::new();
        for record in records {
            let node_id = record.descriptor.node_id;
            if let Some(existing) = peers.get_mut(&node_id) {
                if existing.origin == PeerOrigin::ExplicitSeed
                    || existing.origin == PeerOrigin::Directory
                {
                    continue;
                }
                if existing.revision >= record.revision
                    && existing.peer.admin_addr == record.descriptor.admin_addr
                {
                    existing.trusted_at = Instant::now();
                    existing.boot_id = Some(record.descriptor.boot_id.clone());
                    continue;
                }
                if existing.revision > record.revision {
                    continue;
                }
            }
            let replacement = SeedPeer {
                node_id,
                admin_addr: record.descriptor.admin_addr.clone(),
            };
            if peers
                .get(&node_id)
                .is_some_and(|existing| existing.peer.admin_addr != replacement.admin_addr)
            {
                changed.push(replacement.clone());
            }
            peers.insert(
                node_id,
                PeerEntry {
                    peer: replacement,
                    origin: PeerOrigin::AuthenticatedTopology,
                    revision: record.revision,
                    boot_id: Some(record.descriptor.boot_id.clone()),
                    trusted_at: Instant::now(),
                },
            );
        }
        drop(peers);
        for peer in changed {
            self.cache.record_failure(
                &peer,
                "TRUSTED_ADDRESS_CHANGED",
                "the authenticated topology changed the AdminService address; previous-address state was discarded",
            );
        }
        Ok(())
    }

    fn merge_authenticated_descriptor(&self, snapshot: &NodeSnapshot) -> Result<(), UpstreamError> {
        let Some(evidence) = &snapshot.descriptor else {
            return Ok(());
        };
        if self.discovery.is_some() {
            return Ok(());
        }
        let replacement = SeedPeer {
            node_id: snapshot.node_id,
            admin_addr: evidence.descriptor.admin_addr.clone(),
        };
        let mut peers = self.peers.write().expect("scheduler roster lock");
        let Some(existing) = peers.get(&snapshot.node_id) else {
            return Ok(());
        };
        if existing.origin == PeerOrigin::ExplicitSeed
            || existing.peer.admin_addr == replacement.admin_addr
        {
            return Ok(());
        }
        let revision = existing.revision.max(evidence.directory_revision);
        if peers.values().any(|entry| {
            entry.peer.node_id != replacement.node_id
                && entry.peer.admin_addr == replacement.admin_addr
        }) {
            return Err(UpstreamError {
                code: "ADMIN_ADDRESS_CONFLICT",
                message:
                    "an authenticated descriptor claims another trusted peer's AdminService address"
                        .to_string(),
            });
        }
        peers.insert(
            replacement.node_id,
            PeerEntry {
                peer: replacement.clone(),
                origin: PeerOrigin::AuthenticatedDescriptor,
                revision,
                boot_id: Some(evidence.descriptor.boot_id.clone()),
                trusted_at: Instant::now(),
            },
        );
        drop(peers);
        self.cache.record_failure(
            &replacement,
            "TRUSTED_ADDRESS_CHANGED",
            "the authenticated AdminService address changed; descriptors and on-demand state from the previous address were discarded",
        );
        Ok(())
    }

    fn refresh_observed_identity(&self, snapshot: &NodeSnapshot) {
        let Some(evidence) = &snapshot.descriptor else {
            return;
        };
        let mut peers = self.peers.write().expect("scheduler roster lock");
        if let Some(existing) = peers
            .get_mut(&snapshot.node_id)
            .filter(|existing| existing.peer.admin_addr == evidence.descriptor.admin_addr)
        {
            existing.boot_id = Some(evidence.descriptor.boot_id.clone());
            existing.revision = existing.revision.max(evidence.directory_revision);
        }
    }

    fn expire_non_seed_trust(&self, now: Instant) {
        let expired = {
            let mut peers = self.peers.write().expect("scheduler roster lock");
            let expired = peers
                .values()
                .filter(|entry| {
                    entry.origin != PeerOrigin::ExplicitSeed
                        && now.duration_since(entry.trusted_at) >= NON_SEED_TRUST_LEASE
                })
                .map(|entry| entry.peer.clone())
                .collect::<Vec<_>>();
            peers.retain(|_, entry| {
                entry.origin == PeerOrigin::ExplicitSeed
                    || now.duration_since(entry.trusted_at) < NON_SEED_TRUST_LEASE
            });
            expired
        };
        for peer in expired {
            self.cache.record_failure(
                &peer,
                "DIRECTORY_LEASE_EXPIRED",
                "trusted reconciliation failed beyond the 15s lease; Admin-token forwarding is paused and stale evidence is retained",
            );
        }
    }

    fn validate_against_explicit_seeds(
        &self,
        discovered: &[SeedPeer],
    ) -> Result<(), UpstreamError> {
        let peers = self.peers.read().expect("scheduler roster lock");
        for candidate in discovered {
            if let Some(conflict) = peers.values().find(|entry| {
                entry.origin == PeerOrigin::ExplicitSeed
                    && entry.peer.node_id != candidate.node_id
                    && entry.peer.admin_addr == candidate.admin_addr
            }) {
                return Err(UpstreamError {
                    code: "ADMIN_ADDRESS_CONFLICT",
                    message: format!(
                        "directory node {} claims explicit seed node {}'s AdminService address",
                        candidate.node_id, conflict.peer.node_id
                    ),
                });
            }
        }
        Ok(())
    }

    pub async fn reconcile(self: &Arc<Self>) {
        let generation = self.cache.generation();
        let _flight = self.reconcile_flight.lock().await;
        if self.cache.generation() != generation || self.cache.is_quarantined() {
            return;
        }
        if let Some(discovery) = &self.discovery {
            match tokio::time::timeout(DISCOVERY_RECONCILE_TIMEOUT, discovery.reconcile()).await {
                Ok(Ok(snapshot)) => {
                    self.cache
                        .set_desired_topology(snapshot.routes, snapshot.groups, 0);
                    if let Err(error) = self.replace_directory_roster(&snapshot.records) {
                        self.expire_non_seed_trust(Instant::now());
                        self.cache
                            .record_discovery_error(error.code, &error.message);
                    }
                }
                Ok(Err(error)) => {
                    self.expire_non_seed_trust(Instant::now());
                    self.cache
                        .record_discovery_error(error.code, &error.message);
                }
                Err(_) => {
                    self.expire_non_seed_trust(Instant::now());
                    self.cache.record_discovery_error(
                        "DISCOVERY_RECONCILE_TIMEOUT",
                        "trusted discovery reconciliation exceeded its hard deadline; non-seed Admin-token forwarding expires at the 15s lease",
                    );
                }
            }
        }
        let mut tasks = JoinSet::new();
        for peer in self.peers() {
            let scheduler = self.clone();
            tasks.spawn(async move { scheduler.collect_peer(peer).await });
        }
        let mut metrics = self.cache.begin_metrics_batch();
        while let Some(result) = tasks.join_next().await {
            if let Ok(Some(scrape)) = result {
                self.cache.record_metrics_scrape(scrape, &mut metrics);
            }
        }
        self.cache.finish_metrics_batch(metrics);
        self.cache.refresh_leader_conflicts();
        self.cache.advance_generation();
        let now = Instant::now();
        let mut last = self
            .last_summary_notification
            .lock()
            .expect("summary notification lock");
        if now.duration_since(*last) >= SUMMARY_NOTIFICATION_INTERVAL {
            *last = now;
            drop(last);
            self.cache.notify("summary");
        }
    }

    async fn collect_peer(self: Arc<Self>, peer: SeedPeer) -> Option<NodeMetricsScrape> {
        {
            let breakers = self.breakers.lock().expect("circuit breaker lock");
            if breakers
                .get(&peer.node_id)
                .is_some_and(|breaker| !breaker.allow(Instant::now()))
            {
                self.record_peer_failure(
                    &peer,
                    "CIRCUIT_OPEN",
                    "AdminService collection is temporarily paused after repeated failures",
                );
                return None;
            }
        }
        let _permit = self
            .concurrency
            .acquire()
            .await
            .expect("scheduler semaphore");
        if self.peer(peer.node_id).as_ref() != Some(&peer) {
            return None;
        }
        let directory_revision = self
            .peers
            .read()
            .expect("scheduler roster lock")
            .get(&peer.node_id)
            .map(|entry| entry.revision)
            .unwrap_or_default();
        let result = tokio::time::timeout(
            self.request_timeout,
            self.upstream
                .collect_node_with_revision(&peer, directory_revision),
        )
        .await;
        let mut trusted_snapshot = None;
        match result {
            Ok(Ok(snapshot)) => {
                let snapshot = Arc::new(snapshot);
                let recorded = {
                    let roster = self.peers.read().expect("scheduler roster lock");
                    if roster
                        .get(&peer.node_id)
                        .is_none_or(|current| current.peer.admin_addr != peer.admin_addr)
                    {
                        return None;
                    }
                    self.cache
                        .record_success_for_reconcile(&peer, snapshot.clone())
                };
                match recorded {
                    Ok(()) => {
                        self.record_success(peer.node_id);
                        self.refresh_observed_identity(&snapshot);
                        if let Some(topology) = &snapshot.topology {
                            if self.discovery.is_none() {
                                self.cache.set_desired_topology(
                                    topology.partitions.clone(),
                                    topology
                                        .groups
                                        .iter()
                                        .map(|group| {
                                            (group.group_id, group.replica_node_ids.clone())
                                        })
                                        .collect(),
                                    topology.config_revision,
                                );
                            }
                            if let Err(error) = self.merge_authenticated_topology(topology) {
                                self.cache
                                    .record_discovery_error(error.code, &error.message);
                            }
                        }
                        if let Err(error) = self.merge_authenticated_descriptor(&snapshot) {
                            self.cache
                                .record_discovery_error(error.code, &error.message);
                        }
                        trusted_snapshot = Some(snapshot);
                    }
                    Err(error) => {
                        self.record_peer_failure(&peer, error.code, &error.message);
                        self.record_failure(peer.node_id);
                    }
                }
            }
            Ok(Err(error)) => {
                self.record_peer_failure(&peer, error.code, &error.message);
                self.record_failure(peer.node_id);
            }
            Err(_) => {
                self.record_peer_failure(
                    &peer,
                    "UPSTREAM_TIMEOUT",
                    "AdminService collection exceeded its total deadline",
                );
                self.record_failure(peer.node_id);
            }
        }
        drop(_permit);
        let snapshot = trusted_snapshot?;
        let scraper = self.scraper.as_ref()?;
        let _metrics_permit = self
            .metrics_concurrency
            .acquire()
            .await
            .expect("metrics semaphore");
        let observability_addr = snapshot
            .descriptor
            .as_ref()
            .map(|evidence| evidence.descriptor.observability_addr.clone())?;
        let endpoint = format!("http://{observability_addr}/metrics");
        let result = tokio::time::timeout(
            self.request_timeout,
            scraper.scrape(peer.node_id, &endpoint),
        )
        .await;
        let metrics = match result {
            Ok(Ok(snapshot)) => Some(NodeMetricsScrape {
                peer: peer.clone(),
                snapshot,
                observed_at: std::time::SystemTime::now(),
            }),
            Ok(Err(error)) => {
                self.cache.record_metrics_failure(
                    &peer,
                    error.code,
                    "the trusted observability endpoint returned invalid or unavailable metrics",
                );
                None
            }
            Err(_) => {
                self.cache.record_metrics_failure(
                    &peer,
                    "UPSTREAM_TIMEOUT",
                    "Prometheus scraping exceeded its total deadline",
                );
                None
            }
        };
        if let Some(prober) = &self.prober {
            let (health, ready) = tokio::join!(
                prober.probe(&observability_addr, ProbeKind::Health),
                prober.probe(&observability_addr, ProbeKind::Ready)
            );
            self.cache.record_observability(&peer, health, ready);
        }
        metrics
    }

    pub async fn collect_group(
        &self,
        node_id: u64,
        group_id: u64,
    ) -> Result<GroupDetail, UpstreamError> {
        let peer = self.peer(node_id).ok_or_else(|| UpstreamError {
            code: "NODE_NOT_FOUND",
            message: "the requested node is not in the trusted AdminService roster".to_string(),
        })?;
        let key = self
            .detail_key(node_id, group_id)
            .ok_or_else(|| UpstreamError {
                code: "NODE_NOT_FOUND",
                message: "the requested node trust expired before collection".to_string(),
            })?;
        let flight = {
            let now = Instant::now();
            let mut flights = self.detail_flights.lock().expect("detail flight lock");
            if flights.get(&key, now).is_none() {
                flights.insert(key.clone(), Arc::new(AsyncMutex::new(())), now);
            }
            flights
                .get(&key, now)
                .cloned()
                .expect("detail flight inserted")
        };
        let _flight = flight.lock().await;
        if let Some(detail) = self
            .cache
            .view()
            .nodes
            .iter()
            .find(|node| node.peer.node_id == node_id)
            .and_then(|node| node.details.get(&group_id))
            .filter(|detail| {
                detail
                    .collected_at
                    .elapsed()
                    .is_ok_and(|age| age < self.poll_interval)
            })
            .cloned()
        {
            return Ok(detail);
        }
        if let Some(error) = self
            .detail_failures
            .lock()
            .expect("detail failure lock")
            .get_cloned(&key, Instant::now())
        {
            return Err(error);
        }
        let _permit = self
            .concurrency
            .acquire()
            .await
            .expect("scheduler semaphore");
        let peer = self
            .peer(node_id)
            .filter(|current| current == &peer)
            .ok_or_else(|| UpstreamError {
                code: "NODE_NOT_FOUND",
                message: "the requested node trust expired before collection".to_string(),
            })?;
        let result = tokio::time::timeout(
            self.request_timeout,
            self.upstream.collect_group(&peer, group_id),
        )
        .await;
        let result = match result {
            Ok(Ok(detail)) => {
                self.record_success(node_id);
                self.cache.record_detail(&peer, group_id, detail.clone());
                Ok(detail)
            }
            Ok(Err(error)) => {
                self.record_failure(node_id);
                Err(error)
            }
            Err(_) => {
                self.record_failure(node_id);
                Err(UpstreamError {
                    code: "UPSTREAM_TIMEOUT",
                    message: "AdminService group detail exceeded its total deadline".to_string(),
                })
            }
        };
        if let Err(error) = &result {
            self.detail_failures
                .lock()
                .expect("detail failure lock")
                .insert(key, error.clone(), Instant::now());
        }
        result
    }

    pub async fn collect_groups(
        &self,
        requests: impl IntoIterator<Item = (u64, u64)>,
    ) -> Vec<(u64, u64, Result<GroupDetail, UpstreamError>)> {
        let mut results = futures::stream::iter(requests.into_iter().enumerate().map(
            |(index, (node_id, group_id))| async move {
                let result = self.collect_group(node_id, group_id).await;
                (index, node_id, group_id, result)
            },
        ))
        .buffer_unordered(MAX_ON_DEMAND_PARALLELISM)
        .collect::<Vec<_>>()
        .await;
        results.sort_by_key(|(index, _, _, _)| *index);
        results
            .into_iter()
            .map(|(_, node_id, group_id, result)| (node_id, group_id, result))
            .collect()
    }

    pub async fn list_boot_records(
        self: &Arc<Self>,
        node_id: u64,
        cursor: Option<String>,
        page_size: usize,
    ) -> Result<BootRecordPage, UpstreamError> {
        if !(1..=200).contains(&page_size) {
            return Err(UpstreamError {
                code: "INVALID_ARGUMENT",
                message: "page_size must be between 1 and 200".to_string(),
            });
        }
        let peer = self.peer(node_id).ok_or_else(|| UpstreamError {
            code: "NODE_NOT_FOUND",
            message: "the requested node is not in the trusted AdminService roster".to_string(),
        })?;
        let key = self
            .boot_page_key(node_id, cursor.clone(), page_size)
            .ok_or_else(|| UpstreamError {
                code: "NODE_NOT_FOUND",
                message: "the requested node trust expired before boot collection".to_string(),
            })?;
        if let Some(result) = self
            .boot_pages
            .lock()
            .expect("boot page lock")
            .get_cloned(&key, Instant::now())
        {
            return result;
        }
        let flight = {
            let now = Instant::now();
            let mut flights = self.boot_flights.lock().expect("boot flight lock");
            if flights.get(&key, now).is_none() {
                flights.insert(key.clone(), Arc::new(AsyncMutex::new(())), now);
            }
            flights
                .get(&key, now)
                .cloned()
                .expect("boot flight inserted")
        };
        let flight = flight.lock_owned().await;
        if let Some(result) = self
            .boot_pages
            .lock()
            .expect("boot page lock")
            .get_cloned(&key, Instant::now())
        {
            return result;
        }
        let scheduler = self.clone();
        tokio::spawn(async move {
            let _flight = flight;
            let result = {
                let _permit = scheduler
                    .concurrency
                    .acquire()
                    .await
                    .expect("scheduler semaphore");
                match scheduler.peer(node_id).filter(|current| current == &peer) {
                    Some(peer) => match tokio::time::timeout(
                        scheduler.request_timeout,
                        scheduler
                            .upstream
                            .list_boot_records(&peer, cursor, page_size),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(UpstreamError {
                            code: "UPSTREAM_TIMEOUT",
                            message: "AdminService boot history exceeded its total deadline"
                                .to_string(),
                        }),
                    },
                    None => Err(UpstreamError {
                        code: "NODE_NOT_FOUND",
                        message: "the requested node trust expired before boot collection"
                            .to_string(),
                    }),
                }
            };
            scheduler.boot_pages.lock().expect("boot page lock").insert(
                key,
                result.clone(),
                Instant::now(),
            );
            result
        })
        .await
        .expect("boot history task")
    }

    pub async fn latest_boot_record(
        self: &Arc<Self>,
        node_id: u64,
    ) -> Result<BootRecordPage, UpstreamError> {
        self.list_boot_records(node_id, None, 1).await
    }

    async fn discovery_watch(&self) -> Option<DiscoveryWatch> {
        match &self.discovery {
            Some(discovery) => match discovery.watch().await {
                Ok(watch) => Some(watch),
                Err(error) => {
                    self.cache
                        .record_discovery_error(error.code, &error.message);
                    None
                }
            },
            None => None,
        }
    }

    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut changes = self.discovery_watch().await;
        loop {
            self.reconcile().await;
            if changes.is_none() && self.discovery.is_some() {
                changes = self.discovery_watch().await;
            }
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {}
                change = next_discovery_change(&mut changes) => {
                    if change.is_none() {
                        changes = None;
                        self.cache.record_discovery_error(
                            "DISCOVERY_WATCH_ENDED",
                            "a discovery watch ended; full reconciliation will continue and the watch will be recreated",
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }

    fn record_success(&self, node_id: u64) {
        self.breakers
            .lock()
            .expect("circuit breaker lock")
            .entry(node_id)
            .or_default()
            .record_success();
    }

    fn record_failure(&self, node_id: u64) {
        self.breakers
            .lock()
            .expect("circuit breaker lock")
            .entry(node_id)
            .or_default()
            .record_failure(Instant::now());
    }
}

#[derive(Clone)]
struct HttpProber {
    client: reqwest::Client,
}

#[derive(Clone, Copy)]
enum ProbeKind {
    Health,
    Ready,
}

impl HttpProber {
    fn new(timeout: Duration) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
        })
    }

    async fn probe(&self, observability_addr: &str, kind: ProbeKind) -> ProbeResult {
        let path = match kind {
            ProbeKind::Health => "health",
            ProbeKind::Ready => "ready",
        };
        let endpoint = format!("http://{observability_addr}/{path}");
        let response = match self.client.get(endpoint).send().await {
            Ok(response) => response,
            Err(error) => {
                return ProbeResult {
                    value: None,
                    phase: None,
                    error: Some(UpstreamError {
                        code: if error.is_timeout() {
                            "UPSTREAM_TIMEOUT"
                        } else {
                            "OBSERVABILITY_UNAVAILABLE"
                        },
                        message: format!("{path} probe failed"),
                    }),
                };
            }
        };
        let status = response.status();
        let body = match kind {
            ProbeKind::Health => Vec::new(),
            ProbeKind::Ready => match read_probe_body(response).await {
                Ok(body) => body,
                Err(error) => {
                    return ProbeResult {
                        value: None,
                        phase: None,
                        error: Some(error),
                    };
                }
            },
        };
        let json = if body.is_empty() {
            None
        } else {
            match serde_json::from_slice::<serde_json::Value>(&body) {
                Ok(json) => Some(json),
                Err(_) => {
                    return ProbeResult {
                        value: None,
                        phase: None,
                        error: Some(UpstreamError {
                            code: "INVALID_OBSERVABILITY_RESPONSE",
                            message: "readiness probe returned invalid JSON".to_string(),
                        }),
                    };
                }
            }
        };
        let value = match kind {
            ProbeKind::Health => Some(status.is_success()),
            ProbeKind::Ready if !status.is_success() => Some(false),
            ProbeKind::Ready => json
                .as_ref()
                .and_then(|json| json.get("ready"))
                .and_then(serde_json::Value::as_bool)
                .or(Some(true)),
        };
        let error = (!status.is_success()).then(|| UpstreamError {
            code: "OBSERVABILITY_HTTP_ERROR",
            message: format!("{path} probe returned HTTP {status}"),
        });
        ProbeResult {
            value,
            phase: json
                .as_ref()
                .and_then(|json| json.get("phase"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            error,
        }
    }
}

async fn read_probe_body(response: reqwest::Response) -> Result<Vec<u8>, UpstreamError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_PROBE_BODY_BYTES as u64)
    {
        return Err(UpstreamError {
            code: "OBSERVABILITY_BODY_TOO_LARGE",
            message: "readiness response exceeded the Console body limit".to_string(),
        });
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| UpstreamError {
            code: "OBSERVABILITY_UNAVAILABLE",
            message: "readiness response stream failed".to_string(),
        })?;
        if body.len() + chunk.len() > MAX_PROBE_BODY_BYTES {
            return Err(UpstreamError {
                code: "OBSERVABILITY_BODY_TOO_LARGE",
                message: "readiness response exceeded the Console body limit".to_string(),
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn next_discovery_change(changes: &mut Option<DiscoveryWatch>) -> Option<()> {
    match changes {
        Some(changes) => changes.next().await,
        None => std::future::pending().await,
    }
}

#[derive(Default)]
struct CircuitBreaker {
    failures: u32,
    open_until: Option<Instant>,
}

impl CircuitBreaker {
    fn allow(&self, now: Instant) -> bool {
        self.open_until.is_none_or(|until| now >= until)
    }

    fn record_success(&mut self) {
        self.failures = 0;
        self.open_until = None;
    }

    fn record_failure(&mut self, now: Instant) {
        self.failures = self.failures.saturating_add(1);
        if self.failures >= 3 {
            let backoff = self.failures.saturating_sub(3).min(3);
            let cooldown = Duration::from_secs(30u64.saturating_mul(1 << backoff))
                .min(Duration::from_secs(300));
            self.open_until = Some(now + cooldown);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::console::model::Capability;
    use crate::console::upstream::NodeSnapshot;
    use rr_core::NodeDescriptor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    struct CountingUpstream {
        calls: AtomicUsize,
        group_calls: AtomicUsize,
        boot_requests: Mutex<Vec<(Option<String>, usize)>>,
        peer_calls: Mutex<Vec<(u64, String)>>,
        observability_addr: Option<String>,
        fail_groups: bool,
        block: bool,
        block_boots: bool,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl Upstream for CountingUpstream {
        async fn collect_node(&self, peer: &SeedPeer) -> Result<NodeSnapshot, UpstreamError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.peer_calls
                .lock()
                .unwrap()
                .push((peer.node_id, peer.admin_addr.clone()));
            if self.block {
                self.release.notified().await;
            }
            Ok(NodeSnapshot {
                node_id: peer.node_id,
                listen_addr: "127.0.0.1:51001".to_string(),
                uptime_secs: 1,
                groups: Some(vec![]),
                routes: Some(vec![]),
                formation: None,
                descriptor: self.observability_addr.as_ref().map(|observability_addr| {
                    crate::console::upstream::DescriptorEvidence {
                        descriptor: NodeDescriptor {
                            node_id: peer.node_id,
                            client_addr: "127.0.0.1:51001".to_string(),
                            admin_addr: peer.admin_addr.clone(),
                            observability_addr: observability_addr.clone(),
                            advertise_addr: "127.0.0.1:54001".to_string(),
                            environment: "test".to_string(),
                            region: None,
                            zone: None,
                            rack: None,
                            boot_id: "boot-1".to_string(),
                        },
                        directory_revision: 1,
                    }
                }),
                build: None,
                boot: None,
                topology: None,
                capabilities: vec![Capability::supported("groups")],
                collected_at: SystemTime::now(),
            })
        }

        async fn collect_group(
            &self,
            _peer: &SeedPeer,
            group_id: u64,
        ) -> Result<GroupDetail, UpstreamError> {
            self.group_calls.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            if self.fail_groups {
                return Err(UpstreamError {
                    code: "NOT_FOUND",
                    message: "group does not exist".to_string(),
                });
            }
            Ok(GroupDetail {
                status: rr_network::proto::admin::GroupStatus {
                    group_id,
                    is_leader: true,
                    current_term: 1,
                    last_log_index: 1,
                    leader_node_id: 1,
                },
                membership: None,
                snapshot: None,
                replica: None,
                capabilities: vec![],
                collected_at: SystemTime::now(),
            })
        }

        async fn list_boot_records(
            &self,
            _peer: &SeedPeer,
            cursor: Option<String>,
            page_size: usize,
        ) -> Result<BootRecordPage, UpstreamError> {
            self.boot_requests
                .lock()
                .unwrap()
                .push((cursor.clone(), page_size));
            if self.block_boots {
                self.release.notified().await;
            }
            Ok(BootRecordPage {
                records: vec![],
                next_cursor: cursor.unwrap_or_default(),
                persistence: "PERSISTENT".to_string(),
            })
        }
    }

    fn scheduler(upstream: Arc<CountingUpstream>) -> Arc<Scheduler> {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let peers = vec![SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        }];
        cache.register_peers(&peers);
        Arc::new(Scheduler::new(
            upstream,
            cache,
            peers,
            Duration::from_secs(5),
            Duration::from_secs(1),
            4,
        ))
    }

    fn upstream() -> Arc<CountingUpstream> {
        Arc::new(CountingUpstream {
            calls: AtomicUsize::new(0),
            group_calls: AtomicUsize::new(0),
            boot_requests: Mutex::new(vec![]),
            peer_calls: Mutex::new(vec![]),
            observability_addr: None,
            fail_groups: false,
            block: true,
            block_boots: false,
            release: Arc::new(tokio::sync::Notify::new()),
        })
    }

    fn record(node_id: u64, admin_addr: &str, revision: u64) -> NodeDirectoryRecord {
        NodeDirectoryRecord {
            descriptor: NodeDescriptor {
                node_id,
                client_addr: format!("127.0.0.1:5100{node_id}"),
                admin_addr: admin_addr.to_string(),
                observability_addr: format!("127.0.0.1:5300{node_id}"),
                advertise_addr: format!("127.0.0.1:5400{node_id}"),
                environment: "test".to_string(),
                region: None,
                zone: None,
                rack: None,
                boot_id: format!("boot-{node_id}"),
            },
            revision,
            lease_id: revision as i64,
        }
    }

    #[tokio::test]
    async fn concurrent_reconcile_requests_share_one_upstream_collection() {
        let upstream = upstream();
        let scheduler = scheduler(upstream.clone());
        let barrier = Arc::new(tokio::sync::Barrier::new(21));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..20 {
            let scheduler = scheduler.clone();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                scheduler.reconcile().await;
            });
        }
        barrier.wait().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        upstream.release.notify_one();
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(upstream.calls.load(Ordering::SeqCst), 1);
        assert_eq!(scheduler.cache.view().nodes.len(), 1);
    }

    #[tokio::test]
    async fn reconcile_recomputes_leader_conflicts_once_per_batch() {
        let mut immediate = upstream();
        Arc::get_mut(&mut immediate).unwrap().block = false;
        let peers = (1..=50)
            .map(|node_id| SeedPeer {
                node_id,
                admin_addr: format!("127.0.0.1:{}", 52000 + node_id),
            })
            .collect::<Vec<_>>();
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        cache.register_peers(&peers);
        let scheduler = Arc::new(Scheduler::new(
            immediate,
            cache.clone(),
            peers,
            Duration::from_secs(5),
            Duration::from_secs(1),
            16,
        ));

        scheduler.reconcile().await;

        assert_eq!(cache.leader_conflict_recomputations(), 1);
    }

    #[tokio::test]
    async fn reconcile_coalesces_node_updates_into_batch_notifications() {
        let mut upstream = upstream();
        Arc::get_mut(&mut upstream).unwrap().block = false;
        let peers = (1..=50)
            .map(|node_id| SeedPeer {
                node_id,
                admin_addr: format!("127.0.0.1:{}", 52000 + node_id),
            })
            .collect::<Vec<_>>();
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        cache.register_peers(&peers);
        let scheduler = Arc::new(Scheduler::new(
            upstream,
            cache.clone(),
            peers,
            Duration::from_secs(5),
            Duration::from_secs(1),
            16,
        ));
        let mut changes = cache.subscribe();

        scheduler.reconcile().await;

        let mut notifications = 0;
        while changes.try_recv().is_ok() {
            notifications += 1;
        }
        assert_eq!(notifications, 2);
    }

    #[tokio::test]
    async fn summary_fanout_is_coalesced_without_reducing_collection_coverage() {
        let mut immediate = upstream();
        Arc::get_mut(&mut immediate).unwrap().block = false;
        let scheduler = scheduler(immediate.clone());
        let mut changes = scheduler.cache.subscribe();
        scheduler.reconcile().await;
        while changes.try_recv().is_ok() {}

        scheduler.reconcile().await;
        assert!(!std::iter::from_fn(|| changes.try_recv().ok()).any(|kind| kind == "summary"));
        *scheduler.last_summary_notification.lock().unwrap() =
            Instant::now() - SUMMARY_NOTIFICATION_INTERVAL;
        scheduler.reconcile().await;
        assert!(std::iter::from_fn(|| changes.try_recv().ok()).any(|kind| kind == "summary"));
        assert_eq!(immediate.calls.load(Ordering::SeqCst), 3);
        assert_eq!(scheduler.cache.view().nodes.len(), 1);
    }

    #[tokio::test]
    async fn reconcile_single_flight_also_shares_the_bounded_metrics_scrape() {
        let scrape_calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = scrape_calls.clone();
        tokio::spawn(async move {
            let app = axum::Router::new().route(
                "/metrics",
                axum::routing::get(move || {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        "# TYPE rr_server_requests_total counter\nrr_server_requests_total{group_id=\"7\",op=\"write\"} 10\n"
                    }
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let mut upstream = upstream();
        Arc::get_mut(&mut upstream).unwrap().observability_addr = Some(address.to_string());
        let scheduler = scheduler(upstream.clone());
        let barrier = Arc::new(tokio::sync::Barrier::new(21));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..20 {
            let scheduler = scheduler.clone();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                scheduler.reconcile().await;
            });
        }
        barrier.wait().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        upstream.release.notify_one();
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(upstream.calls.load(Ordering::SeqCst), 1);
        assert_eq!(scrape_calls.load(Ordering::SeqCst), 1);
        assert!(scheduler.cache.view().nodes[0].metrics.is_some());
    }

    #[tokio::test]
    async fn concurrent_group_detail_requests_share_one_on_demand_rpc() {
        let upstream = upstream();
        let scheduler = scheduler(upstream.clone());
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..20 {
            let scheduler = scheduler.clone();
            tasks.spawn(async move { scheduler.collect_group(1, 7).await });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
        assert_eq!(upstream.group_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn expired_group_detail_is_refreshed_instead_of_cached_forever() {
        let upstream = upstream();
        let scheduler = scheduler(upstream.clone());
        let peer = scheduler.peer(1).unwrap();
        scheduler.collect_group(1, 7).await.unwrap();
        let mut expired = scheduler.collect_group(1, 7).await.unwrap();
        assert_eq!(upstream.group_calls.load(Ordering::SeqCst), 1);
        expired.collected_at = SystemTime::now() - Duration::from_secs(30);
        scheduler.cache.record_detail(&peer, 7, expired);
        scheduler.collect_group(1, 7).await.unwrap();
        assert_eq!(upstream.group_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn boot_history_preserves_upstream_cursor_and_page_size() {
        let upstream = upstream();
        let scheduler = scheduler(upstream.clone());
        let page = scheduler
            .list_boot_records(1, Some("journal-cursor".to_string()), 25)
            .await
            .unwrap();
        assert_eq!(page.next_cursor, "journal-cursor");
        assert_eq!(
            upstream.boot_requests.lock().unwrap().as_slice(),
            &[(Some("journal-cursor".to_string()), 25)]
        );
    }

    #[test]
    fn explicit_seed_addresses_cannot_be_replaced_by_directory_records() {
        let scheduler = scheduler(upstream());
        scheduler
            .replace_directory_roster(&[
                record(1, "127.0.0.1:52999", 2),
                record(2, "127.0.0.1:52002", 2),
            ])
            .unwrap();
        let peers = scheduler.peers();
        assert_eq!(peers[0].admin_addr, "127.0.0.1:52001");
        assert_eq!(peers[1].admin_addr, "127.0.0.1:52002");
    }

    #[test]
    fn authoritative_refresh_removes_expired_discovered_peers_but_retains_seed_and_stale_evidence()
    {
        let scheduler = scheduler(upstream());
        scheduler
            .replace_directory_roster(&[record(2, "127.0.0.1:52002", 2)])
            .unwrap();
        assert_eq!(scheduler.peers().len(), 2);
        scheduler.replace_directory_roster(&[]).unwrap();
        assert_eq!(scheduler.peers().len(), 1);
        let removed = scheduler
            .cache
            .view()
            .nodes
            .iter()
            .find(|node| node.peer.node_id == 2)
            .cloned()
            .unwrap();
        assert_eq!(
            removed.error.as_ref().map(|error| error.code),
            Some("DIRECTORY_RECORD_REMOVED")
        );
    }

    #[test]
    fn directory_record_cannot_claim_another_explicit_seeds_address() {
        let scheduler = scheduler(upstream());
        let error = scheduler
            .replace_directory_roster(&[record(2, "127.0.0.1:52001", 2)])
            .unwrap_err();
        assert_eq!(error.code, "ADMIN_ADDRESS_CONFLICT");
        assert_eq!(scheduler.peers().len(), 1);
    }

    #[tokio::test]
    async fn watch_event_runs_full_reconciliation_and_stops_contacting_expired_peers() {
        use crate::console::discovery::tests::{MockConfig, MockDirectory};
        use rr_core::directory::NodeDirectoryPage;

        let directory = Arc::new(MockDirectory::new(vec![NodeDirectoryPage {
            records: vec![record(2, "127.0.0.1:52002", 2)],
            next_cursor: None,
            revision: 2,
        }]));
        let discovery = Arc::new(TrustedDiscovery::new(
            directory.clone(),
            Arc::new(MockConfig::new(rr_core::RoutingTable::default())),
        ));
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let seed = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        cache.register_peers(&[seed.clone()]);
        let mut immediate = upstream();
        Arc::get_mut(&mut immediate).unwrap().block = false;
        let scheduler = Arc::new(
            Scheduler::new(
                immediate,
                cache,
                vec![seed],
                Duration::from_secs(60),
                Duration::from_secs(1),
                4,
            )
            .with_discovery(discovery),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(scheduler.clone().run(shutdown_rx));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if scheduler
                    .cache
                    .view()
                    .nodes
                    .iter()
                    .any(|node| node.peer.node_id == 2 && node.snapshot.is_some())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            scheduler.cache.view().source_mode,
            "TRUSTED_DIRECTORY_AND_CONFIG"
        );

        directory.replace_pages(vec![NodeDirectoryPage {
            records: vec![],
            next_cursor: None,
            revision: 3,
        }]);
        directory.notify_remove(2, 3);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if scheduler.peers().len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(scheduler
            .cache
            .view()
            .nodes
            .iter()
            .any(|node| node.peer.node_id == 2
                && node
                    .error
                    .as_ref()
                    .is_some_and(|error| error.code == "DIRECTORY_RECORD_REMOVED")));
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn trusted_address_change_drops_descriptors_from_the_previous_address() {
        let scheduler = scheduler(upstream());
        let address_a = record(2, "127.0.0.1:52002", 2);
        scheduler
            .replace_directory_roster(&[address_a.clone()])
            .unwrap();
        let peer_a = scheduler.peer(2).unwrap();
        let snapshot = NodeSnapshot {
            node_id: 2,
            listen_addr: "127.0.0.1:51002".to_string(),
            uptime_secs: 1,
            groups: Some(vec![]),
            routes: Some(vec![]),
            formation: None,
            descriptor: Some(crate::console::upstream::DescriptorEvidence {
                descriptor: address_a.descriptor,
                directory_revision: 2,
            }),
            build: None,
            boot: None,
            topology: None,
            capabilities: vec![],
            collected_at: SystemTime::now(),
        };
        scheduler.cache.record_success(&peer_a, snapshot).unwrap();

        scheduler
            .replace_directory_roster(&[record(2, "127.0.0.1:52902", 3)])
            .unwrap();
        let view = scheduler.cache.view();
        let node = view
            .nodes
            .iter()
            .find(|node| node.peer.node_id == 2)
            .unwrap();
        assert!(node.snapshot.is_none());
        assert!(node.details.is_empty());
        assert!(node.metrics.is_none());
    }

    #[test]
    fn authenticated_topology_cannot_reintroduce_removed_directory_records() {
        let discovery = Arc::new(TrustedDiscovery::new(
            Arc::new(crate::console::discovery::tests::MockDirectory::new(vec![])),
            Arc::new(crate::console::discovery::tests::MockConfig::new(
                rr_core::RoutingTable::default(),
            )),
        ));
        let scheduler = scheduler(upstream());
        let scheduler = Arc::new(
            Scheduler::new(
                upstream(),
                scheduler.cache.clone(),
                scheduler.peers(),
                Duration::from_secs(5),
                Duration::from_secs(1),
                4,
            )
            .with_discovery(discovery),
        );
        scheduler
            .replace_directory_roster(&[record(2, "127.0.0.1:52002", 2)])
            .unwrap();
        scheduler.replace_directory_roster(&[]).unwrap();
        let removed = record(2, "127.0.0.1:52002", 2);
        let topology = ClusterTopology {
            snapshot_id: "snapshot".to_string(),
            config_revision: 1,
            directory_revision: 2,
            nodes: vec![rr_network::proto::admin::NodeDirectoryEntry {
                descriptor: Some(rr_network::proto::admin::NodeDescriptor {
                    node_id: removed.descriptor.node_id,
                    client_addr: removed.descriptor.client_addr,
                    admin_addr: removed.descriptor.admin_addr,
                    observability_addr: removed.descriptor.observability_addr,
                    advertise_addr: removed.descriptor.advertise_addr,
                    environment: removed.descriptor.environment,
                    region: None,
                    zone: None,
                    rack: None,
                    boot_id: removed.descriptor.boot_id,
                }),
                revision: removed.revision,
                lease_id: removed.lease_id,
            }],
            groups: vec![],
            partitions: vec![],
            next_cursor: String::new(),
        };

        scheduler.merge_authenticated_topology(&topology).unwrap();
        assert!(scheduler.peer(2).is_none());
    }

    #[tokio::test]
    async fn reconciliation_outage_expires_non_seed_forwarding_after_the_directory_lease() {
        use crate::console::discovery::tests::{MockConfig, MockDirectory};
        use rr_core::directory::NodeDirectoryPage;

        let directory = Arc::new(MockDirectory::new(vec![NodeDirectoryPage {
            records: vec![record(2, "127.0.0.1:52002", 2)],
            next_cursor: None,
            revision: 2,
        }]));
        let discovery = Arc::new(TrustedDiscovery::new(
            directory.clone(),
            Arc::new(MockConfig::new(rr_core::RoutingTable::default())),
        ));
        let mut immediate = upstream();
        Arc::get_mut(&mut immediate).unwrap().block = false;
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let seed = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        cache.register_peers(&[seed.clone()]);
        let scheduler = Arc::new(
            Scheduler::new(
                immediate.clone(),
                cache,
                vec![seed],
                Duration::from_secs(5),
                Duration::from_secs(1),
                4,
            )
            .with_discovery(discovery),
        );
        scheduler.reconcile().await;
        assert!(scheduler.peer(2).is_some());

        directory.replace_pages(vec![]);
        scheduler
            .peers
            .write()
            .unwrap()
            .get_mut(&2)
            .unwrap()
            .trusted_at = Instant::now() - NON_SEED_TRUST_LEASE;
        immediate.peer_calls.lock().unwrap().clear();
        scheduler.reconcile().await;

        assert!(scheduler.peer(2).is_none());
        assert!(immediate
            .peer_calls
            .lock()
            .unwrap()
            .iter()
            .all(|(node_id, _)| *node_id != 2));
        assert!(scheduler
            .cache
            .view()
            .nodes
            .iter()
            .any(|node| node.peer.node_id == 2 && node.snapshot.is_some()));
    }

    #[tokio::test]
    async fn hanging_reconciliation_expires_all_non_seed_credential_forwarding() {
        use crate::console::discovery::tests::MockConfig;
        use rr_core::directory::{
            NodeDirectory, NodeDirectoryChange, NodeDirectoryPage, NodeDirectoryRegistration,
        };
        use rr_core::error::DirectoryError;
        use std::pin::Pin;

        struct HangingDirectory {
            calls: AtomicUsize,
            record: NodeDirectoryRecord,
        }

        #[async_trait::async_trait]
        impl NodeDirectory for HangingDirectory {
            async fn get(
                &self,
                _node_id: u64,
            ) -> Result<Option<NodeDirectoryRecord>, DirectoryError> {
                unimplemented!()
            }

            async fn list(
                &self,
                _cursor: Option<&str>,
                _page_size: Option<usize>,
            ) -> Result<NodeDirectoryPage, DirectoryError> {
                if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
                    std::future::pending::<()>().await;
                }
                Ok(NodeDirectoryPage {
                    records: vec![self.record.clone()],
                    next_cursor: None,
                    revision: 2,
                })
            }

            async fn watch(
                &self,
            ) -> Result<
                Pin<Box<dyn futures::Stream<Item = NodeDirectoryChange> + Send>>,
                DirectoryError,
            > {
                unimplemented!()
            }

            async fn register(
                &self,
                _descriptor: NodeDescriptor,
            ) -> Result<Box<dyn NodeDirectoryRegistration>, DirectoryError> {
                unimplemented!()
            }
        }

        let directory = Arc::new(HangingDirectory {
            calls: AtomicUsize::new(0),
            record: record(2, "127.0.0.1:52002", 2),
        });
        let discovery = Arc::new(TrustedDiscovery::new(
            directory.clone(),
            Arc::new(MockConfig::new(rr_core::RoutingTable::default())),
        ));
        let mut immediate = upstream();
        Arc::get_mut(&mut immediate).unwrap().block = false;
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let seed = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        cache.register_peers(&[seed.clone()]);
        let scheduler = Arc::new(
            Scheduler::new(
                immediate.clone(),
                cache,
                vec![seed],
                Duration::from_secs(5),
                Duration::from_secs(1),
                4,
            )
            .with_discovery(discovery),
        );
        scheduler.reconcile().await;
        assert!(scheduler.peer(2).is_some());
        scheduler
            .peers
            .write()
            .unwrap()
            .get_mut(&2)
            .unwrap()
            .trusted_at = Instant::now() - NON_SEED_TRUST_LEASE;
        immediate.peer_calls.lock().unwrap().clear();

        let reconciliation = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.reconcile().await }
        });
        tokio::task::yield_now().await;
        assert_eq!(directory.calls.load(Ordering::SeqCst), 2);
        tokio::time::timeout(Duration::from_secs(1), reconciliation)
            .await
            .expect("hanging discovery must reach its reconciliation deadline")
            .unwrap();

        assert!(scheduler.peer(2).is_none());
        assert!(immediate
            .peer_calls
            .lock()
            .unwrap()
            .iter()
            .all(|(node_id, _)| *node_id != 2));
        assert_eq!(
            scheduler.collect_group(2, 7).await.unwrap_err().code,
            "NODE_NOT_FOUND"
        );
        assert_eq!(
            scheduler.latest_boot_record(2).await.unwrap_err().code,
            "NODE_NOT_FOUND"
        );
        assert_eq!(immediate.group_calls.load(Ordering::SeqCst), 0);
        assert!(immediate.boot_requests.lock().unwrap().is_empty());
        let view = scheduler.cache.view();
        let stale = view
            .nodes
            .iter()
            .find(|node| node.peer.node_id == 2)
            .cloned()
            .unwrap();
        assert!(stale.snapshot.is_some());
        assert_eq!(
            stale.error.as_ref().map(|error| error.code),
            Some("DIRECTORY_LEASE_EXPIRED")
        );
    }

    #[tokio::test]
    async fn failed_and_nonexistent_group_flights_are_bounded() {
        let mut failing = upstream();
        Arc::get_mut(&mut failing).unwrap().fail_groups = true;
        let scheduler = scheduler(failing);
        for group_id in 0..300 {
            assert!(scheduler.collect_group(1, group_id).await.is_err());
        }

        assert!(scheduler.detail_flights.lock().unwrap().len() <= 256);
    }

    #[tokio::test]
    async fn cached_group_details_are_bounded() {
        let scheduler = scheduler(upstream());
        for group_id in 0..300 {
            scheduler.collect_group(1, group_id).await.unwrap();
        }

        let view = scheduler.cache.view();
        assert!(view.nodes[0].details.len() <= 256);
        assert!(scheduler.detail_flights.lock().unwrap().len() <= 256);
    }

    #[tokio::test]
    async fn cancelled_boot_leader_does_not_duplicate_the_shared_upstream_request() {
        let mut blocked = upstream();
        Arc::get_mut(&mut blocked).unwrap().block_boots = true;
        let scheduler = scheduler(blocked.clone());
        let first = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.list_boot_records(1, None, 5).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while blocked.boot_requests.lock().unwrap().len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        let _ = first.await;

        let second = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.list_boot_records(1, None, 5).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(blocked.boot_requests.lock().unwrap().len(), 1);
        blocked.release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn twenty_boot_view_subscribers_share_one_cached_upstream_request() {
        let upstream = upstream();
        let scheduler = scheduler(upstream.clone());
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..20 {
            let scheduler = scheduler.clone();
            tasks.spawn(async move { scheduler.list_boot_records(1, None, 50).await });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }

        assert_eq!(upstream.boot_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn twenty_latest_boot_subscribers_share_one_cached_upstream_request() {
        let upstream = upstream();
        let scheduler = scheduler(upstream.clone());
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..20 {
            let scheduler = scheduler.clone();
            tasks.spawn(async move { scheduler.latest_boot_record(1).await });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }

        assert_eq!(
            upstream.boot_requests.lock().unwrap().as_slice(),
            &[(None, 1)]
        );
    }

    #[tokio::test]
    async fn failed_readiness_is_scraped_from_the_trusted_observability_origin() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = axum::Router::new()
                .route("/health", axum::routing::get(|| async { "ok" }))
                .route(
                    "/ready",
                    axum::routing::get(|| async {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            axum::Json(serde_json::json!({"ready": false, "phase": "forming"})),
                        )
                    }),
                )
                .route("/metrics", axum::routing::get(|| async { "" }));
            axum::serve(listener, app).await.unwrap();
        });
        let mut immediate = upstream();
        Arc::get_mut(&mut immediate).unwrap().block = false;
        Arc::get_mut(&mut immediate).unwrap().observability_addr = Some(address.to_string());
        let scheduler = scheduler(immediate);

        scheduler.reconcile().await;

        let view = scheduler.cache.view();
        assert!(view
            .sources
            .iter()
            .any(|source| { source.endpoint_role == "READY" && source.status == "ERROR" }));
        assert_eq!(view.nodes[0].sources[0].status, "DEGRADED");
        let observability = view.nodes[0].observability.as_ref().unwrap();
        assert_eq!(observability.liveness, Some(true));
        assert_eq!(observability.ready, Some(false));
        assert_eq!(observability.ready_phase.as_deref(), Some("forming"));
    }

    #[test]
    fn circuit_breaker_opens_after_bounded_failures_and_recovers_after_cooldown() {
        let mut breaker = CircuitBreaker::default();
        let now = Instant::now();
        for _ in 0..3 {
            breaker.record_failure(now);
        }
        assert!(!breaker.allow(now));
        assert!(breaker.allow(now + Duration::from_secs(31)));
        breaker.record_success();
        assert!(breaker.allow(now));
    }
}
