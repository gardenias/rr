//! [`RrNode`] — the main node lifecycle manager.
//!
//! Coordinates:
//! - [`RaftGroupManager`]: manages Raft group instances
//! - [`ShardRouter`]: routes partition IDs to groups
//! - gRPC server (optional): serves Raft and client RPCs when both
//!   `raft_lookup` and `client_dispatch` are provided

use std::sync::Arc;

use futures::StreamExt;
use metrics::{counter, gauge};
use tokio::sync::{watch, Mutex};
use tracing::{info, instrument, warn};

use rr_core::config::{ConfigProvider, RoutingTableChange};
use rr_core::error::{ConfigError, RrError};
use rr_core::types::NodeId;
use rr_network::client_service::{ClientDispatch, ClientServiceImpl};
use rr_network::proto::admin::admin_service_server::AdminServiceServer;
use rr_network::proto::client::client_service_server::ClientServiceServer;
use rr_network::proto::raft::raft_service_server::RaftServiceServer;
use rr_network::raft_service::{RaftGroupLookup, RaftServiceImpl};
use rr_raft::group_manager::RaftGroupManager;
use rr_router::router::ShardRouter;

use crate::admin::AdminServiceImpl;
use crate::config::{NodeConfig, RaftTransport};
use crate::health::{spawn_http_server, ReadinessFlag};

/// Validate the production security invariants. Returns `Err` describing
/// every gate that is violated unless `config.allow_insecure` opts out.
///
/// Two evaluation phases (ADR A-0014):
///
/// 1. **G7 (hard)**: a TCP raft transport may only bind loopback. This runs
///    unconditionally — it is the first deliberate exception to the
///    "`allow_insecure` short-circuits every gate" pattern, because the TCP
///    frame protocol has no TLS and no peer authentication whatsoever, so
///    there is no configuration in which an off-loopback plaintext TCP raft
///    listener is acceptable (see ADR A-0014 §Option E).
/// 2. **G1–G6**: skipped when `allow_insecure` is set, otherwise ALL
///    violations are aggregated into a single error message so operators
///    fix the config in one pass rather than chasing them one at a time.
fn validate_security_invariants(config: &NodeConfig) -> Result<(), RrError> {
    if let RaftTransport::Tcp { bind_addr } = &config.raft_transport {
        // Fail closed: an unparsable bind address cannot be proven loopback.
        // (The rr-server binary path can never produce one — `listen_addr`
        // is validated as a `SocketAddr` by `NodeConfig::validate` and the
        // TCP bind is derived from it — but library embedders set this field
        // directly.)
        let is_loopback = bind_addr
            .parse::<std::net::SocketAddr>()
            .map(|addr| addr.ip().is_loopback())
            .unwrap_or(false);
        if !is_loopback {
            return Err(RrError::Config(ConfigError::InvalidConfig(format!(
                "rr-server refuses to start: raft_transport = tcp plans to bind \
                 '{bind_addr}', which is not a loopback address (127.0.0.0/8 or \
                 ::1). The TCP raft frame protocol carries AppendEntries/Vote in \
                 plaintext with no TLS and no peer authentication of any kind, \
                 so it is restricted to loopback. NOTE: --allow-insecure does \
                 NOT bypass this gate — it is the one safety gate that applies \
                 in every mode (ADR A-0014). Bind the gRPC listen_addr to a \
                 loopback address (the TCP bind address is derived from it), \
                 or use raft_transport = grpc."
            ))));
        }
    }

    if config.allow_insecure {
        return Ok(());
    }

    let raft_enabled = config.raft_lookup.is_some();
    let admin_enabled = config.admin_port.is_some();
    let tls_enabled = config.tls_config.is_some();
    let auth_required = config.auth_config.admin_required() || config.auth_config.client_required();

    let mut violations: Vec<&'static str> = Vec::new();

    if raft_enabled && !tls_enabled {
        violations.push(
            "RaftService is enabled but TLS is not configured. Inter-node Raft RPCs \
             must run over TLS so peer identity is established at the transport layer.",
        );
    }

    if raft_enabled
        && tls_enabled
        && !config
            .tls_config
            .as_ref()
            .map(|t| t.require_client_cert)
            .unwrap_or(false)
    {
        violations.push(
            "RaftService is enabled with TLS but tls.require_client_cert = false. \
             RaftService has no bearer-token path; mTLS is the ONLY peer-identity \
             check, so require_client_cert must be true.",
        );
    }

    if auth_required && !tls_enabled {
        violations.push(
            "Bearer-token auth is configured (admin_token or client_token) but TLS \
             is not. Tokens would travel in plaintext gRPC metadata.",
        );
    }

    if admin_enabled && !tls_enabled {
        violations.push(
            "AdminService is enabled but TLS is not configured. AdminService \
             exposes leadership transfer and snapshot triggering — running over \
             plaintext is a denial-of-service vector.",
        );
    }

    if admin_enabled && config.auth_config.admin_token.is_none() {
        violations.push(
            "AdminService is enabled but admin_token is not set. Anyone reaching \
             the admin port can transfer leadership or trigger snapshots; this \
             is a cluster-wide DoS vector.",
        );
    }

    // G6: the TCP raft transport has no TLS and no peer authentication in
    // its frame protocol, so it always requires an explicit insecure opt-in
    // — even when the gRPC face is fully TLS/mTLS compliant (AppendEntries/
    // Vote travel over the plaintext TCP port; only InstallSnapshot falls
    // back to gRPC). The loopback part of the TCP rules (G7) is enforced
    // unconditionally above, before the `allow_insecure` short-circuit.
    if matches!(config.raft_transport, RaftTransport::Tcp { .. }) {
        violations.push(
            "Raft transport is TCP (raft_transport = tcp): the TCP frame protocol \
             has no TLS and no peer authentication of any kind, so in v0.2.0 it is \
             a dev/benchmark-only transport. Correctly configured gRPC TLS/mTLS \
             does NOT cover it. To proceed, explicitly opt in with \
             --allow-insecure / RR_ALLOW_INSECURE=1 / [node].allow_insecure=true. \
             (The TCP bind address must additionally be loopback; that gate is \
             hard and cannot be bypassed.)",
        );
    }

    if violations.is_empty() {
        return Ok(());
    }

    let mut msg = String::from(
        "rr-server refuses to start: production safety invariants violated. \
         Either fix the configuration below, or pass --allow-insecure / \
         set RR_ALLOW_INSECURE=1 / set [node].allow_insecure=true in the TOML \
         to opt out (DEV ONLY — see NodeConfig::allow_insecure docs).\n\nViolations:",
    );
    for v in &violations {
        msg.push_str("\n  - ");
        msg.push_str(v);
    }
    Err(RrError::Config(ConfigError::InvalidConfig(msg)))
}

/// Default histogram buckets for `*_seconds` latency metrics (client
/// requests, raft proposals, AppendEntries RTT, ingestor polls).
/// Sub-millisecond to 5s so quantiles stay meaningful from in-process
/// fast paths up to WAN-like chaos latencies.
const LATENCY_HISTOGRAM_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Histogram buckets for `rr_replication_batch_entries` (log entries per
/// AppendEntries RPC).
const BATCH_ENTRIES_HISTOGRAM_BUCKETS: &[f64] =
    &[1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0];

/// The main node lifecycle manager.
///
/// Owns the [`RaftGroupManager`], [`ShardRouter`], and an optional gRPC server.
///
/// # Lifecycle
///
/// ```text
/// NodeConfig → RrNode::new() → .start() → ... → .shutdown()
/// ```
pub struct RrNode {
    node_id: NodeId,
    listen_addr: String,
    group_manager: Arc<RaftGroupManager>,
    router: Arc<ShardRouter>,
    config_provider: Arc<dyn ConfigProvider>,
    raft_lookup: Option<Arc<dyn RaftGroupLookup>>,
    client_dispatch: Option<Arc<dyn ClientDispatch>>,
    shutdown_tx: watch::Sender<bool>,
    /// Handle to the background gRPC server task; `None` if no server was started.
    server_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Handle to the config watch loop task; `None` if not started.
    watch_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Handle to the admin gRPC server task; `None` if admin service is disabled.
    admin_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Handle to the HTTP observability server (/health /ready /metrics); `None` if disabled.
    http_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Port for the admin gRPC service, if enabled.
    admin_port: Option<u16>,
    /// Port for HTTP observability endpoints (/health /ready /metrics).
    metrics_port: Option<u16>,
    /// TLS config applied to all gRPC servers hosted by this node.
    tls_config: Option<rr_network::tls::TlsConfig>,
    /// Bearer-token auth policy. Empty = no auth (dev).
    auth_config: rr_network::auth::AuthConfig,
    /// Rate-limit policy (admin/client only, never Raft).
    rate_limit_config: rr_network::rate_limit::RateLimitConfig,
    /// Readiness signal driving HTTP `/ready`.
    readiness: ReadinessFlag,
    /// Prometheus metrics handle (Some iff metrics_port was configured).
    prom_handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
    /// Health reporter for gRPC health checking protocol.
    health_reporter: Mutex<Option<tonic_health::server::HealthReporter>>,
}

impl RrNode {
    /// Create a new `RrNode` from [`NodeConfig`].
    ///
    /// Validates config, assembles sub-components, but does **not** start any
    /// groups or the gRPC server. Call [`start()`](Self::start) for that.
    pub fn new(config: NodeConfig) -> Result<Self, RrError> {
        config.validate()?;

        validate_security_invariants(&config)?;

        if config.allow_insecure {
            warn!(
                node_id = config.node_id,
                listen_addr = %config.listen_addr,
                raft_service_enabled = config.raft_lookup.is_some(),
                admin_service_enabled = config.admin_port.is_some(),
                tls_enabled = config.tls_config.is_some(),
                admin_auth_required = config.auth_config.admin_required(),
                client_auth_required = config.auth_config.client_required(),
                "INSECURE MODE ACTIVE — production safety gates bypassed. Do NOT \
                 run this configuration in production. See NodeConfig::allow_insecure \
                 for the full list of bypassed checks. Set --allow-insecure=false \
                 (or remove the flag) and configure TLS + auth + admin_token to \
                 disable insecure mode."
            );
        }

        let prom_handle = if config.metrics_port.is_some() {
            // Without explicit buckets the exporter renders histograms as
            // Prometheus *summaries* (quantile label), which leaves every
            // `histogram_quantile(... _bucket ...)` recording rule and
            // dashboard panel with no data.
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
                .set_buckets_for_metric(
                    metrics_exporter_prometheus::Matcher::Suffix("_seconds".to_string()),
                    LATENCY_HISTOGRAM_BUCKETS,
                )
                .map_err(|e| ConfigError::InvalidConfig(format!("metrics bucket config: {e}")))?
                .set_buckets_for_metric(
                    metrics_exporter_prometheus::Matcher::Full(
                        "rr_replication_batch_entries".to_string(),
                    ),
                    BATCH_ENTRIES_HISTOGRAM_BUCKETS,
                )
                .map_err(|e| ConfigError::InvalidConfig(format!("metrics bucket config: {e}")))?
                .build_recorder();
            let handle = recorder.handle();
            let _ = metrics::set_global_recorder(recorder);
            info!("Prometheus recorder installed (metrics served via HTTP observability port)");
            gauge!("rr_security_insecure_mode").set(if config.allow_insecure { 1.0 } else { 0.0 });
            Some(handle)
        } else {
            None
        };

        let group_manager = if let Some(mgr) = config.group_manager {
            mgr
        } else {
            Arc::new(RaftGroupManager::new(
                config.node_id,
                config.state_machine_factory,
            ))
        };

        // Clone the Arc before passing to ShardRouter so we can also store it
        // directly on RrNode for the watch loop.
        let config_provider = Arc::clone(&config.config_provider);
        let router = Arc::new(ShardRouter::new(config.config_provider));

        let (shutdown_tx, _) = watch::channel(false);

        info!(
            node_id = config.node_id,
            listen_addr = %config.listen_addr,
            "RrNode created"
        );

        Ok(Self {
            node_id: config.node_id,
            listen_addr: config.listen_addr,
            group_manager,
            router,
            config_provider,
            raft_lookup: config.raft_lookup,
            client_dispatch: config.client_dispatch,
            shutdown_tx,
            server_handle: Mutex::new(None),
            watch_handle: Mutex::new(None),
            admin_handle: Mutex::new(None),
            http_handle: Mutex::new(None),
            admin_port: config.admin_port,
            metrics_port: config.metrics_port,
            tls_config: config.tls_config,
            auth_config: config.auth_config,
            rate_limit_config: config.rate_limit_config,
            readiness: ReadinessFlag::new(),
            prom_handle,
            health_reporter: Mutex::new(None),
        })
    }

    /// Start the node.
    ///
    /// 1. Loads the routing table.
    /// 2. Starts Raft groups assigned to this node.
    /// 3. Spawns a config-change watch loop.
    /// 4. Spawns a gRPC server if both `raft_lookup` and `client_dispatch`
    ///    are present.
    #[instrument(skip(self))]
    pub async fn start(&self) -> Result<(), RrError> {
        // ── 1. Load routing table and start assigned groups ─────────────────
        let table = self.router.routing_table().await?;

        for (group_id, meta) in &table.groups {
            if meta.replicas.contains(&self.node_id) {
                self.group_manager.start_group(*group_id).await?;
                self.group_manager.start_metrics_ticker(*group_id).await?;
                info!(node_id = self.node_id, group_id, "started Raft group");
                counter!("node.group.started").increment(1);
            }
        }

        // ── 2. Spawn config-change watch loop ────────────────────────────────
        let watch_stream = self
            .config_provider
            .watch_changes()
            .await
            .map_err(RrError::Config)?;

        let gm = Arc::clone(&self.group_manager);
        let cp = Arc::clone(&self.config_provider);
        let node_id = self.node_id;

        let watch_handle = tokio::spawn(async move {
            let mut stream = watch_stream;
            while let Some(change) = stream.next().await {
                match change {
                    RoutingTableChange::PartitionAdded { group_id, .. } => {
                        // The PartitionAdded variant doesn't carry replicas directly —
                        // look them up from the routing table.
                        match cp.get_routing_table().await {
                            Ok(table) => {
                                if let Some(meta) = table.groups.get(&group_id) {
                                    if meta.replicas.contains(&node_id) {
                                        match gm.start_group(group_id).await {
                                            Ok(()) => {
                                                if let Err(e) =
                                                    gm.start_metrics_ticker(group_id).await
                                                {
                                                    warn!(
                                                        group_id,
                                                        "watch: failed to start metrics ticker: {e}"
                                                    );
                                                }
                                                info!(
                                                    group_id,
                                                    "watch: started group on PartitionAdded"
                                                );
                                            }
                                            Err(e)
                                                if e.to_string().contains("already exists") => {}
                                            Err(e) => warn!(
                                                group_id,
                                                "watch: failed to start group on PartitionAdded: {e}"
                                            ),
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("watch: failed to get routing table on PartitionAdded: {e}")
                            }
                        }
                    }
                    RoutingTableChange::PartitionRemoved { partition_id } => {
                        info!(partition_id, "watch: partition removed");
                    }
                    RoutingTableChange::PartitionUpdated {
                        partition_id,
                        group_id,
                        ..
                    } => {
                        info!(partition_id, group_id, "watch: partition updated");
                    }
                    RoutingTableChange::NodeAdded { node_id: nid, addr } => {
                        info!(
                            node_id = nid,
                            ?addr,
                            "watch: node added, adding as learner to all groups"
                        );
                        for group_id in gm.group_ids().await {
                            if let Err(e) = gm.add_learner(group_id, nid, addr.addr.clone()).await {
                                warn!(
                                    group_id,
                                    node_id = nid,
                                    "watch: add_learner failed (may not be leader): {e}"
                                );
                            } else {
                                info!(group_id, node_id = nid, "watch: added learner on NodeAdded");
                            }
                        }
                    }
                    RoutingTableChange::NodeRemoved { node_id: nid } => {
                        info!(
                            node_id = nid,
                            "watch: node removed, updating membership for all groups"
                        );
                        match cp.get_routing_table().await {
                            Ok(table) => {
                                for group_id in gm.group_ids().await {
                                    if let Some(meta) = table.groups.get(&group_id) {
                                        let new_members: std::collections::BTreeSet<NodeId> = meta
                                            .replicas
                                            .iter()
                                            .copied()
                                            .filter(|&id| id != nid)
                                            .collect();
                                        if let Err(e) =
                                            gm.change_membership(group_id, new_members).await
                                        {
                                            warn!(
                                                group_id,
                                                node_id = nid,
                                                "watch: change_membership failed (may not be leader): {e}"
                                            );
                                        } else {
                                            info!(group_id, node_id = nid, "watch: removed node from membership on NodeRemoved");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("watch: failed to get routing table on NodeRemoved: {e}")
                            }
                        }
                    }
                    RoutingTableChange::GroupMembershipChanged { group_id, replicas } => {
                        info!(group_id, ?replicas, "watch: group membership changed");
                        if replicas.contains(&node_id) {
                            // This node is (now) in the group's replica set — start if needed.
                            match gm.start_group(group_id).await {
                                Ok(()) => {
                                    if let Err(e) = gm.start_metrics_ticker(group_id).await {
                                        warn!(
                                            group_id,
                                            "watch: failed to start metrics ticker: {e}"
                                        );
                                    }
                                    info!(
                                        group_id,
                                        "watch: started group on GroupMembershipChanged"
                                    );
                                }
                                Err(e) if e.to_string().contains("already exists") => {}
                                Err(e) => warn!(
                                    group_id,
                                    "watch: failed to start group on GroupMembershipChanged: {e}"
                                ),
                            }
                        } else {
                            // This node is no longer a replica — stop the group if running.
                            if gm.get_raft(group_id).await.is_some() {
                                if let Err(e) = gm.stop_group(group_id).await {
                                    warn!(group_id,
                                        "watch: failed to stop group on GroupMembershipChanged: {e}"
                                    );
                                } else {
                                    info!(
                                        group_id,
                                        "watch: stopped group on GroupMembershipChanged"
                                    );
                                }
                            }
                        }
                    }
                }
            }
            info!("watch: config change stream ended");
        });

        *self.watch_handle.lock().await = Some(watch_handle);
        info!(node_id = self.node_id, "config watch loop started");

        // ── 3. Start gRPC server (optional) ─────────────────────────────────
        if let (Some(raft_lookup), Some(client_dispatch)) =
            (&self.raft_lookup, &self.client_dispatch)
        {
            let addr = self.listen_addr.parse().map_err(|e| {
                RrError::Config(ConfigError::InvalidConfig(format!(
                    "invalid listen_addr '{}': {e}",
                    self.listen_addr
                )))
            })?;

            let raft_svc = RaftServiceServer::new(RaftServiceImpl::new(raft_lookup.clone()))
                .max_decoding_message_size(rr_network::RAFT_MAX_DECODING_MESSAGE_SIZE);
            let client_chain = rr_network::ChainedInterceptor {
                first: self.auth_config.client_interceptor(),
                second: self.rate_limit_config.client_interceptor(),
            };
            let client_svc = ClientServiceServer::with_interceptor(
                ClientServiceImpl::new(client_dispatch.clone()),
                client_chain,
            );
            if self.auth_config.client_required() {
                info!(%addr, "ClientService bearer-token auth enabled");
            }
            if let Some(rps) = self.rate_limit_config.client_rps {
                info!(%addr, rps, "ClientService rate limit enabled");
            }

            let (health_reporter, health_service) = tonic_health::server::health_reporter();

            let mut shutdown_rx = self.shutdown_tx.subscribe();
            let mut builder = tonic::transport::Server::builder();
            if let Some(tls) = &self.tls_config {
                let server_tls = tls.build_server_tls().map_err(|e| {
                    RrError::Config(ConfigError::InvalidConfig(format!("server tls: {e}")))
                })?;
                builder = builder.tls_config(server_tls).map_err(|e| {
                    RrError::Config(ConfigError::InvalidConfig(format!(
                        "tls_config attach failed: {e}"
                    )))
                })?;
                info!(%addr, "gRPC server TLS enabled");
            }

            let handle = tokio::spawn(async move {
                info!(%addr, "gRPC server starting");
                let result = builder
                    .add_service(health_service)
                    .add_service(raft_svc)
                    .add_service(client_svc)
                    .serve_with_shutdown(addr, async move {
                        let _ = shutdown_rx.changed().await;
                    })
                    .await;

                match result {
                    Ok(()) => info!(%addr, "gRPC server stopped"),
                    Err(e) => warn!(%addr, "gRPC server error: {e}"),
                }
            });

            // Mark the overall service as SERVING after spawning the server.
            health_reporter
                .set_service_status("", tonic_health::ServingStatus::Serving)
                .await;
            *self.health_reporter.lock().await = Some(health_reporter);

            *self.server_handle.lock().await = Some(handle);
            info!(node_id = self.node_id, "gRPC server spawned");
        } else {
            info!(
                node_id = self.node_id,
                "gRPC server not started (raft_lookup or client_dispatch missing)"
            );
        }

        // ── 4. Start admin gRPC server (optional) ────────────────────────────
        if let Some(port) = self.admin_port {
            let admin_addr: std::net::SocketAddr =
                format!("0.0.0.0:{port}")
                    .parse()
                    .map_err(|e: std::net::AddrParseError| {
                        RrError::Config(ConfigError::InvalidConfig(e.to_string()))
                    })?;

            let admin_chain = rr_network::ChainedInterceptor {
                first: self.auth_config.admin_interceptor(),
                second: self.rate_limit_config.admin_interceptor(),
            };
            let admin_svc = AdminServiceServer::with_interceptor(
                AdminServiceImpl::new(
                    self.node_id,
                    self.listen_addr.clone(),
                    Arc::clone(&self.group_manager),
                    Arc::clone(&self.config_provider),
                ),
                admin_chain,
            );
            if self.auth_config.admin_required() {
                info!(%admin_addr, "AdminService bearer-token auth enabled");
            } else {
                warn!(
                    %admin_addr,
                    "AdminService has NO auth — anyone reaching this port can transfer \
                     leadership or trigger snapshots. Set admin_token in production!"
                );
            }
            if let Some(rps) = self.rate_limit_config.admin_rps {
                info!(%admin_addr, rps, "AdminService rate limit enabled");
            }

            let mut shutdown_rx = self.shutdown_tx.subscribe();

            let mut admin_builder = tonic::transport::Server::builder();
            if let Some(tls) = &self.tls_config {
                let server_tls = tls.build_server_tls().map_err(|e| {
                    RrError::Config(ConfigError::InvalidConfig(format!("admin server tls: {e}")))
                })?;
                admin_builder = admin_builder.tls_config(server_tls).map_err(|e| {
                    RrError::Config(ConfigError::InvalidConfig(format!(
                        "admin tls_config attach failed: {e}"
                    )))
                })?;
                info!(%admin_addr, "admin gRPC server TLS enabled");
            }

            let handle = tokio::spawn(async move {
                info!(%admin_addr, "admin gRPC server starting");
                let result = admin_builder
                    .add_service(admin_svc)
                    .serve_with_shutdown(admin_addr, async move {
                        let _ = shutdown_rx.changed().await;
                    })
                    .await;

                match result {
                    Ok(()) => info!(%admin_addr, "admin gRPC server stopped"),
                    Err(e) => warn!(%admin_addr, "admin gRPC server error: {e}"),
                }
            });

            *self.admin_handle.lock().await = Some(handle);
            info!(node_id = self.node_id, port, "admin gRPC server spawned");
        }

        if let (Some(port), Some(handle)) = (self.metrics_port, self.prom_handle.clone()) {
            let addr: std::net::SocketAddr =
                format!("0.0.0.0:{port}")
                    .parse()
                    .map_err(|e: std::net::AddrParseError| {
                        RrError::Config(ConfigError::InvalidConfig(e.to_string()))
                    })?;
            let shutdown_rx = self.shutdown_tx.subscribe();
            let http = spawn_http_server(addr, handle, self.readiness.clone(), shutdown_rx).await?;
            *self.http_handle.lock().await = Some(http);
            info!(
                node_id = self.node_id,
                port, "HTTP observability server spawned"
            );
        }

        self.readiness.set(true);
        info!(node_id = self.node_id, "RrNode started");
        counter!("node.started").increment(1);
        Ok(())
    }

    /// Shutdown the node gracefully.
    ///
    /// 1. Signals the gRPC server to stop (5 s timeout).
    /// 2. Aborts the config watch loop (2 s timeout).
    /// 3. Stops all active Raft groups.
    #[instrument(skip(self))]
    pub async fn shutdown(&self) -> Result<(), RrError> {
        info!(node_id = self.node_id, "RrNode shutting down");
        self.readiness.set(false);

        if let Some(reporter) = self.health_reporter.lock().await.as_mut() {
            reporter
                .set_service_status("", tonic_health::ServingStatus::NotServing)
                .await;
        }

        // ── 1. Stop gRPC server ──────────────────────────────────────────────
        let _ = self.shutdown_tx.send(true);

        if let Some(handle) = self.server_handle.lock().await.take() {
            match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
                Ok(Ok(())) => info!("gRPC server shutdown complete"),
                Ok(Err(e)) => warn!("gRPC server task panicked: {e}"),
                Err(_) => warn!("gRPC server shutdown timed out after 5s"),
            }
        }

        if let Some(handle) = self.admin_handle.lock().await.take() {
            match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
                Ok(Ok(())) => info!("admin gRPC server shutdown complete"),
                Ok(Err(e)) => warn!("admin gRPC server task panicked: {e}"),
                Err(_) => warn!("admin gRPC server shutdown timed out after 5s"),
            }
        }

        if let Some(handle) = self.http_handle.lock().await.take() {
            match tokio::time::timeout(std::time::Duration::from_secs(3), handle).await {
                Ok(Ok(())) => info!("HTTP observability server shutdown complete"),
                Ok(Err(e)) => warn!("HTTP observability server task panicked: {e}"),
                Err(_) => warn!("HTTP observability server shutdown timed out after 3s"),
            }
        }

        // ── 2. Stop config watch loop ────────────────────────────────────────
        if let Some(handle) = self.watch_handle.lock().await.take() {
            handle.abort();
            match tokio::time::timeout(std::time::Duration::from_secs(2), handle).await {
                Ok(Ok(())) => info!("config watch loop stopped"),
                Ok(Err(e)) if e.is_cancelled() => info!("config watch loop aborted"),
                Ok(Err(e)) => warn!("config watch loop panicked: {e}"),
                Err(_) => warn!("config watch loop stop timed out after 2s"),
            }
        }

        // ── 3. Stop all Raft groups ──────────────────────────────────────────
        let group_ids = self.group_manager.group_ids().await;
        for group_id in group_ids {
            if let Err(e) = self.group_manager.stop_group(group_id).await {
                warn!(group_id, "error stopping Raft group: {e}");
            }
        }

        info!(node_id = self.node_id, "RrNode shutdown complete");
        counter!("node.shutdown").increment(1);
        Ok(())
    }

    /// Return the [`RaftGroupManager`].
    pub fn group_manager(&self) -> Arc<RaftGroupManager> {
        Arc::clone(&self.group_manager)
    }

    /// Return the [`ShardRouter`].
    pub fn router(&self) -> Arc<ShardRouter> {
        Arc::clone(&self.router)
    }

    /// Return this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rr_config::mem::MemConfigProvider;
    use rr_core::config::ConfigProvider;
    use rr_core::error::RrError;
    use rr_core::state_machine::StateMachine;
    use rr_core::storage::StorageEngine;
    use rr_core::types::{GroupId, LogIndex, PartitionConfig};
    use rr_network::auth::AuthConfig;
    use rr_network::client_service::{ClientDispatch, ClientWriteResult};
    use rr_network::proto::client::GroupInfo;
    use rr_network::raft_service::RaftGroupLookup;
    use rr_network::tls::TlsConfig;
    use rr_raft::group_manager::StateMachineFactory;
    use rr_storage::mem::MemStorage;
    use std::path::PathBuf;

    struct DummyRaftLookup;

    #[async_trait]
    impl RaftGroupLookup for DummyRaftLookup {
        async fn append_entries(&self, _: GroupId, _: Vec<u8>) -> Result<Vec<u8>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn vote(&self, _: GroupId, _: Vec<u8>) -> Result<Vec<u8>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn install_snapshot(
            &self,
            _: GroupId,
            _: Vec<u8>,
            _: Vec<u8>,
        ) -> Result<Vec<u8>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
    }

    struct DummyClientDispatch;

    #[async_trait]
    impl ClientDispatch for DummyClientDispatch {
        async fn submit_write(
            &self,
            _: u64,
            _: Vec<u8>,
            _: Option<rr_core::IdempotencyKey>,
        ) -> Result<ClientWriteResult, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn submit_read(&self, _: u64, _: Vec<u8>) -> Result<Vec<u8>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn submit_linearizable_read(
            &self,
            _: u64,
            _: Vec<u8>,
        ) -> Result<rr_network::client_service::LinearizableReadResult, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
        async fn get_cluster_info(&self) -> Result<Vec<GroupInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented(""))
        }
    }

    fn tls_without_mtls() -> TlsConfig {
        TlsConfig {
            cert_path: PathBuf::from("/tmp/cert.pem"),
            key_path: PathBuf::from("/tmp/key.pem"),
            ca_path: None,
            require_client_cert: false,
            domain_name: None,
        }
    }

    fn tls_with_mtls() -> TlsConfig {
        TlsConfig {
            cert_path: PathBuf::from("/tmp/cert.pem"),
            key_path: PathBuf::from("/tmp/key.pem"),
            ca_path: Some(PathBuf::from("/tmp/ca.pem")),
            require_client_cert: true,
            domain_name: None,
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    struct NoopSM;

    #[async_trait]
    impl StateMachine for NoopSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }
        async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    fn noop_factory() -> StateMachineFactory {
        Box::new(|_group_id| Box::new(NoopSM))
    }

    fn make_config(node_id: NodeId, listen_addr: &str) -> NodeConfig {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider: Arc<dyn ConfigProvider> = Arc::new(MemConfigProvider::new());
        NodeConfig::new(
            node_id,
            listen_addr.to_string(),
            storage,
            provider,
            noop_factory(),
        )
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_node_new_valid_config() {
        let node = RrNode::new(make_config(1, "127.0.0.1:50051")).unwrap();
        assert_eq!(node.node_id(), 1);
    }

    #[test]
    fn test_node_new_invalid_node_id_returns_error() {
        let result = RrNode::new(make_config(0, "127.0.0.1:50051"));
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(
            msg.contains("node_id must be > 0"),
            "expected error about invalid node_id, got: {msg}"
        );
    }

    #[test]
    fn test_node_new_empty_addr_returns_error() {
        let result = RrNode::new(make_config(1, ""));
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(
            msg.contains("listen_addr"),
            "expected error about listen_addr, got: {msg}"
        );
    }

    #[test]
    fn test_node_config_metrics_port() {
        let config = make_config(1, "127.0.0.1:50051");
        assert_eq!(config.metrics_port, None);
        let config = config.with_metrics_port(19091);
        assert_eq!(config.metrics_port, Some(19091));
    }

    #[tokio::test]
    async fn test_node_start_creates_groups_for_this_node() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider = MemConfigProvider::new();
        // Partition 1 assigned to group 10 with replicas [1] — this node is 1
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![1])
            .await;
        let provider: Arc<dyn ConfigProvider> = Arc::new(provider);

        let config = NodeConfig::new(
            1,
            "127.0.0.1:50051".to_string(),
            storage,
            provider,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        assert_eq!(node.group_manager().group_count().await, 1);
        assert!(node.group_manager().get_raft(10).await.is_some());

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_node_shutdown_stops_all_groups() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider = MemConfigProvider::new();
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![1])
            .await;
        let provider: Arc<dyn ConfigProvider> = Arc::new(provider);

        let config = NodeConfig::new(
            1,
            "127.0.0.1:50052".to_string(),
            storage,
            provider,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();
        assert_eq!(node.group_manager().group_count().await, 1);

        node.shutdown().await.unwrap();
        assert_eq!(node.group_manager().group_count().await, 0);
    }

    #[tokio::test]
    async fn test_node_start_skips_groups_for_other_nodes() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider = MemConfigProvider::new();
        // Group 10 replicas are [2, 3] — node 1 is NOT a replica
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![2, 3])
            .await;
        let provider: Arc<dyn ConfigProvider> = Arc::new(provider);

        let config = NodeConfig::new(
            1, // we are node 1
            "127.0.0.1:50053".to_string(),
            storage,
            provider,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        // Node 1 must NOT start group 10 (not in replicas)
        assert_eq!(node.group_manager().group_count().await, 0);

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_node_start_no_groups_in_routing_table() {
        let node = RrNode::new(make_config(1, "127.0.0.1:50054")).unwrap();
        // Empty routing table — start should succeed with 0 groups
        node.start().await.unwrap();
        assert_eq!(node.group_manager().group_count().await, 0);
        node.shutdown().await.unwrap();
    }

    // ── Watch loop tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_watch_loop_starts_group_on_partition_added() {
        // Start the node with empty routing table, then add a partition after
        // start. The watch loop must pick it up and start the group.
        let provider = Arc::new(MemConfigProvider::new());
        let config = NodeConfig::new(
            1,
            "127.0.0.1:50055".to_string(),
            Arc::new(MemStorage::new()),
            provider.clone() as Arc<dyn ConfigProvider>,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        // Verify no groups before adding the partition
        assert_eq!(node.group_manager().group_count().await, 0);

        // Add partition AFTER start — watch loop must pick it up
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![1])
            .await;

        // Poll with timeout (max 500 ms)
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
        loop {
            if node.group_manager().group_count().await == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "group did not start within 500ms after PartitionAdded"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(node.group_manager().get_raft(10).await.is_some());
        node.shutdown().await.unwrap();
    }

    // INV-6: after a routing-table update the old shard receives no new writes; the routing table is the single source of truth.
    #[tokio::test]
    async fn test_watch_loop_skips_group_not_in_replicas() {
        // Add a partition where node 1 is NOT in replicas — group must not start.
        let provider = Arc::new(MemConfigProvider::new());
        let config = NodeConfig::new(
            1,
            "127.0.0.1:50056".to_string(),
            Arc::new(MemStorage::new()),
            provider.clone() as Arc<dyn ConfigProvider>,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        // Partition assigned to group 20 with replicas [2, 3] — NOT node 1
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 20, vec![2, 3])
            .await;

        // Wait a bit and verify group was NOT started
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            node.group_manager().group_count().await,
            0,
            "watch loop should not start group when node is not in replicas"
        );

        node.shutdown().await.unwrap();
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[test]
    fn validate_security_raft_without_tls_rejects() {
        let cfg = make_config(1, "127.0.0.1:50100").with_raft_lookup(Arc::new(DummyRaftLookup));
        let err = RrNode::new(cfg)
            .err()
            .expect("must reject raft without tls");
        let msg = err.to_string();
        assert!(
            msg.contains("RaftService") && msg.contains("TLS"),
            "expected RaftService+TLS error, got: {msg}"
        );
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[test]
    fn validate_security_raft_without_mtls_rejects() {
        let cfg = make_config(1, "127.0.0.1:50101")
            .with_raft_lookup(Arc::new(DummyRaftLookup))
            .with_tls(tls_without_mtls());
        let err = RrNode::new(cfg)
            .err()
            .expect("must reject raft without mtls");
        let msg = err.to_string();
        assert!(
            msg.contains("require_client_cert"),
            "expected require_client_cert error, got: {msg}"
        );
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[test]
    fn validate_security_auth_without_tls_rejects() {
        let cfg = make_config(1, "127.0.0.1:50102").with_auth(AuthConfig {
            admin_token: Some("x".into()),
            client_token: None,
        });
        let err = RrNode::new(cfg)
            .err()
            .expect("must reject bearer auth without tls");
        let msg = err.to_string();
        assert!(
            msg.contains("Bearer-token") && msg.contains("TLS"),
            "expected bearer+TLS error, got: {msg}"
        );
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[test]
    fn validate_security_admin_without_tls_rejects() {
        let cfg = make_config(1, "127.0.0.1:50103")
            .with_admin_port(9999)
            .with_auth(AuthConfig {
                admin_token: Some("x".into()),
                client_token: None,
            });
        let err = RrNode::new(cfg)
            .err()
            .expect("must reject admin port without tls");
        let msg = err.to_string();
        assert!(
            msg.contains("AdminService") && msg.contains("TLS"),
            "expected AdminService+TLS error, got: {msg}"
        );
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[test]
    fn validate_security_admin_without_token_rejects() {
        let cfg = make_config(1, "127.0.0.1:50104")
            .with_admin_port(9999)
            .with_tls(tls_with_mtls());
        let err = RrNode::new(cfg)
            .err()
            .expect("must reject admin port without token");
        let msg = err.to_string();
        assert!(
            msg.contains("admin_token"),
            "expected admin_token error, got: {msg}"
        );
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[test]
    fn validate_security_full_strict_path_passes() {
        let cfg = make_config(1, "127.0.0.1:50105")
            .with_raft_lookup(Arc::new(DummyRaftLookup))
            .with_client_dispatch(Arc::new(DummyClientDispatch))
            .with_admin_port(9999)
            .with_tls(tls_with_mtls())
            .with_auth(AuthConfig {
                admin_token: Some("admin-secret".into()),
                client_token: Some("client-secret".into()),
            });
        let _node = RrNode::new(cfg).expect("strict + correct config must validate");
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[test]
    fn validate_security_allow_insecure_bypasses_all() {
        let cfg = make_config(1, "127.0.0.1:50106")
            .with_raft_lookup(Arc::new(DummyRaftLookup))
            .with_admin_port(9999)
            .with_allow_insecure(true);
        let _node = RrNode::new(cfg).expect("allow_insecure=true must bypass validation");
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[test]
    fn validate_security_minimal_dev_config_still_passes() {
        let _node = RrNode::new(make_config(1, "127.0.0.1:50107"))
            .expect("library users with no raft/admin should not be forced to TLS");
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[test]
    fn validate_security_aggregates_all_violations() {
        let cfg = make_config(1, "127.0.0.1:50108")
            .with_raft_lookup(Arc::new(DummyRaftLookup))
            .with_admin_port(9999);
        let err = RrNode::new(cfg).err().expect("must aggregate violations");
        let msg = err.to_string();
        assert!(msg.contains("RaftService"), "missing raft violation: {msg}");
        assert!(
            msg.contains("AdminService"),
            "missing admin violation: {msg}"
        );
    }

    // ── G6/G7: TCP raft transport gates (ADR A-0014) ────────────────────────

    #[test]
    fn validate_security_tcp_requires_allow_insecure_despite_grpc_tls() {
        // Core regression for P0#4 scenario 1: gRPC TLS/mTLS fully compliant
        // must NOT mask the plaintext TCP transport — G6 still rejects.
        let cfg = make_config(1, "127.0.0.1:50110")
            .with_raft_lookup(Arc::new(DummyRaftLookup))
            .with_tls(tls_with_mtls())
            .with_raft_transport(RaftTransport::Tcp {
                bind_addr: "127.0.0.1:60110".to_string(),
            });
        let err = RrNode::new(cfg)
            .err()
            .expect("G6 must reject tcp transport without allow_insecure");
        let msg = err.to_string();
        assert!(
            msg.contains("raft_transport = tcp"),
            "expected TCP transport violation, got: {msg}"
        );
        assert!(
            msg.contains("--allow-insecure"),
            "expected allow_insecure guidance, got: {msg}"
        );
        // The gRPC face is compliant, so gates 1/2 must NOT fire — G6 is the
        // only violation in the aggregate.
        assert!(
            !msg.contains("RaftService is enabled but TLS"),
            "gRPC-compliant config must not trip gate 1: {msg}"
        );
        assert!(
            !msg.contains("require_client_cert"),
            "gRPC-compliant config must not trip gate 2: {msg}"
        );
    }

    #[test]
    fn validate_security_tcp_loopback_with_allow_insecure_passes() {
        // Positive path: TCP + explicit allow_insecure + loopback bind → Ok.
        for bind_addr in ["127.0.0.1:60111", "[::1]:60111"] {
            let cfg = make_config(1, "127.0.0.1:50111")
                .with_raft_lookup(Arc::new(DummyRaftLookup))
                .with_allow_insecure(true)
                .with_raft_transport(RaftTransport::Tcp {
                    bind_addr: bind_addr.to_string(),
                });
            let _node = RrNode::new(cfg).unwrap_or_else(|e| {
                panic!("tcp + allow_insecure + loopback ({bind_addr}) must pass: {e}")
            });
        }
    }

    #[test]
    fn validate_security_tcp_non_loopback_rejected_even_with_allow_insecure() {
        // G7 is the one gate allow_insecure cannot bypass: wildcard,
        // routable, and IPv6-unspecified binds are all rejected.
        for bind_addr in ["0.0.0.0:60112", "192.168.1.10:60112", "[::]:60112"] {
            let cfg = make_config(1, "127.0.0.1:50112")
                .with_raft_lookup(Arc::new(DummyRaftLookup))
                .with_allow_insecure(true)
                .with_raft_transport(RaftTransport::Tcp {
                    bind_addr: bind_addr.to_string(),
                });
            let err = RrNode::new(cfg).err().unwrap_or_else(|| {
                panic!("G7 must reject non-loopback bind {bind_addr} despite allow_insecure")
            });
            let msg = err.to_string();
            assert!(msg.contains("loopback"), "expected loopback error: {msg}");
            assert!(
                msg.contains("NOT bypass"),
                "error must state allow_insecure cannot bypass G7: {msg}"
            );
        }
    }

    #[test]
    fn validate_security_tcp_without_tls_aggregates_grpc_and_tcp_violations() {
        // TCP + no allow_insecure + no TLS: gate 1 (gRPC face) and G6 (TCP
        // face) both fire and are aggregated in one message.
        let cfg = make_config(1, "127.0.0.1:50113")
            .with_raft_lookup(Arc::new(DummyRaftLookup))
            .with_raft_transport(RaftTransport::Tcp {
                bind_addr: "127.0.0.1:60113".to_string(),
            });
        let err = RrNode::new(cfg)
            .err()
            .expect("must reject tcp without tls and without allow_insecure");
        let msg = err.to_string();
        assert!(
            msg.contains("RaftService is enabled but TLS"),
            "missing gRPC gate-1 violation: {msg}"
        );
        assert!(
            msg.contains("raft_transport = tcp"),
            "missing G6 violation: {msg}"
        );
    }

    #[test]
    fn validate_security_tcp_wildcard_bind_rejected_without_allow_insecure() {
        // ADR startup-test leg 1: 0.0.0.0 bind + no allow_insecure → Err, and
        // because G7 is evaluated before the allow_insecure short-circuit,
        // the readable loopback guidance is what the operator sees (even
        // though G6 would also have fired).
        let cfg = make_config(1, "0.0.0.0:50114")
            .with_raft_lookup(Arc::new(DummyRaftLookup))
            .with_tls(tls_with_mtls())
            .with_raft_transport(RaftTransport::Tcp {
                bind_addr: "0.0.0.0:60114".to_string(),
            });
        let err = RrNode::new(cfg)
            .err()
            .expect("G7 must reject wildcard bind without allow_insecure");
        let msg = err.to_string();
        assert!(msg.contains("loopback"), "expected loopback error: {msg}");
        assert!(
            msg.contains("raft_transport = grpc"),
            "expected grpc fallback guidance: {msg}"
        );
    }

    #[test]
    fn validate_security_tcp_unparseable_bind_addr_rejected() {
        // Library-embedder misuse: a bind_addr that is not a `host:port`
        // `SocketAddr` cannot be proven loopback — fail closed (G7), even
        // with allow_insecure set.
        let cfg = make_config(1, "127.0.0.1:50115")
            .with_raft_lookup(Arc::new(DummyRaftLookup))
            .with_allow_insecure(true)
            .with_raft_transport(RaftTransport::Tcp {
                bind_addr: "not-a-socket-addr".to_string(),
            });
        let err = RrNode::new(cfg)
            .err()
            .expect("G7 must fail closed on unparsable bind_addr");
        assert!(
            err.to_string().contains("loopback"),
            "expected loopback error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_watch_loop_handles_group_membership_changed() {
        // Start node with no groups, then fire GroupMembershipChanged with
        // this node in replicas. The watch loop must start the group.
        let provider = Arc::new(MemConfigProvider::new());

        // Pre-add partition to group 30 with node 2 as replica (not node 1)
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 30, vec![2])
            .await;

        let config = NodeConfig::new(
            1,
            "127.0.0.1:50057".to_string(),
            Arc::new(MemStorage::new()),
            provider.clone() as Arc<dyn ConfigProvider>,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        // Node 1 is not in replicas initially
        assert_eq!(node.group_manager().group_count().await, 0);

        // Fire GroupMembershipChanged with node 1 now included
        provider.update_group_membership(30, vec![1, 2]).await;

        // Poll with timeout (max 500 ms)
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
        loop {
            if node.group_manager().group_count().await == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "group did not start within 500ms after GroupMembershipChanged"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(node.group_manager().get_raft(30).await.is_some());
        node.shutdown().await.unwrap();
    }
}
