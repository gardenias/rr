//! Node configuration for [`RrNode`](super::node::RrNode).

use std::sync::Arc;

use rr_core::config::ConfigProvider;
use rr_core::error::ConfigError;
use rr_core::storage::StorageEngine;
use rr_core::types::NodeId;
use rr_network::auth::AuthConfig;
use rr_network::client_service::ClientDispatch;
use rr_network::raft_service::RaftGroupLookup;
use rr_network::rate_limit::RateLimitConfig;
use rr_network::tls::TlsConfig;
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::group_manager::StateMachineFactory;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::runtime::NodeRuntimeConfig;

/// Which transport carries inter-node Raft RPCs (AppendEntries / Vote).
///
/// This is a *descriptor*: [`RrNode`](super::node::RrNode) does not bind the
/// TCP server itself — the embedding binary does (see `main.rs::start_node`,
/// which derives the bind address from `listen_addr` and passes it here).
/// `RrNode::new` uses this field to enforce the transport-aware security
/// gates (G6/G7, ADR A-0014) before anything binds.
///
/// Note that `InstallSnapshot` always falls back to gRPC, so the gRPC
/// `RaftService` — and its existing TLS/mTLS gates — stay in force under
/// both variants.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RaftTransport {
    /// gRPC transport (default). TLS/mTLS available via `tls_config`.
    Grpc,
    /// Experimental plaintext TCP transport (binary `RRAF` framing).
    ///
    /// The frame protocol has **no TLS and no peer authentication of any
    /// kind**; in v0.2.0 it is a dev/benchmark-only feature. `RrNode::new`
    /// refuses this variant unless:
    ///
    /// - `allow_insecure` is explicitly set (gate G6), **and**
    /// - `bind_addr` parses as a [`std::net::SocketAddr`] whose IP is
    ///   loopback (gate G7 — **not** bypassable by `allow_insecure`).
    Tcp {
        /// Planned bind address (`host:port`) of the TCP raft server.
        bind_addr: String,
    },
}

/// Default bind host for the Admin gRPC service: loopback only. See
/// [`NodeConfig::admin_bind_host`].
pub const DEFAULT_ADMIN_BIND_HOST: &str = "127.0.0.1";

/// Default bind host for the HTTP observability endpoints: loopback only.
/// See [`NodeConfig::metrics_bind_host`].
pub const DEFAULT_METRICS_BIND_HOST: &str = "127.0.0.1";

/// Configuration for creating an [`RrNode`](super::node::RrNode).
pub struct NodeConfig {
    /// Unique identifier for this node in the cluster.
    pub node_id: NodeId,
    /// gRPC listen address (e.g. `"0.0.0.0:50051"`).
    pub listen_addr: String,
    /// Shared storage engine (used by all Raft groups on this node).
    pub storage_engine: Arc<dyn StorageEngine>,
    /// Config provider supplying the routing table and node addresses.
    pub config_provider: Arc<dyn ConfigProvider>,
    /// Factory for creating one `StateMachine` instance per Raft group.
    ///
    /// For high-level [`Service`](rr_core::service::Service) users, wrap with
    /// `ServiceStateMachineAdapter`:
    ///
    /// ```ignore
    /// Box::new(|group_id| Box::new(ServiceStateMachineAdapter::new(group_id, MyService::new())))
    /// ```
    pub state_machine_factory: StateMachineFactory,
    /// Optional Raft RPC lookup — enables inter-node Raft RPCs.
    ///
    /// If `None`, the gRPC server is not started.
    pub raft_lookup: Option<Arc<dyn RaftGroupLookup>>,
    /// Which transport carries inter-node Raft RPCs (AppendEntries / Vote).
    ///
    /// Defaults to [`RaftTransport::Grpc`]. When set to
    /// [`RaftTransport::Tcp`], `RrNode::new` additionally enforces:
    ///
    /// - **G6**: `allow_insecure` must be explicitly set, regardless of
    ///   gRPC TLS/mTLS state (the TCP frame protocol has neither);
    /// - **G7**: the planned `bind_addr` must be loopback. G7 is *hard* —
    ///   it is the one gate `allow_insecure` does **not** bypass.
    ///
    /// See ADR A-0014. Embedded/library users that never enable the TCP
    /// transport are unaffected (the default keeps v0.1.x behavior exactly).
    pub raft_transport: RaftTransport,
    /// Optional client dispatch — enables client-facing RPCs.
    ///
    /// If `None`, the gRPC server is not started.
    pub client_dispatch: Option<Arc<dyn ClientDispatch>>,
    /// Pre-built group manager. When `Some`, `RrNode::new()` uses it directly
    /// instead of creating a fresh one. Required for real-network mode where
    /// `GrpcNetworkFactory` must be pre-wired.
    pub group_manager: Option<Arc<RaftGroupManager>>,
    /// Optional port for Prometheus `/metrics` HTTP endpoint. `None` = disabled.
    pub metrics_port: Option<u16>,
    /// Optional port for the Admin gRPC service. `None` = disabled.
    pub admin_port: Option<u16>,
    /// Bind host for the Admin gRPC service. Defaults to loopback
    /// (`127.0.0.1`) — the admin API exposes leadership transfer and
    /// snapshot triggers, so wildcard bind must be an explicit choice
    /// (`"0.0.0.0"` / `"::"`). Breaking change in v1.0.0: pre-v1.0.0 bound
    /// `0.0.0.0` unconditionally.
    pub admin_bind_host: String,
    /// Bind host for the HTTP observability endpoints
    /// (`/health` `/ready` `/metrics`). Defaults to loopback
    /// (`127.0.0.1`) — `/metrics` exposes internal histograms without
    /// auth. Set `"0.0.0.0"` to allow cross-host Prometheus scrapes.
    /// Breaking change in v1.0.0: pre-v1.0.0 bound `0.0.0.0`
    /// unconditionally.
    pub metrics_bind_host: String,
    /// Override the gRPC server's HTTP/2 `max_concurrent_streams` (tonic/
    /// hyper default: 200). `None` keeps the library default. The cap is
    /// per connection — raising it matters when many unbatched concurrent
    /// requests share one client connection (100k roadmap M3).
    pub grpc_max_concurrent_streams: Option<u32>,
    /// Override the gRPC server's HTTP/2 max frame size in bytes
    /// (default 16 KiB). Larger frames cut per-frame processing overhead
    /// for large (batched) payloads. `None` keeps the library default.
    pub grpc_max_frame_size: Option<u32>,
    /// TLS configuration for the main gRPC server (Client + Raft services)
    /// and the Admin gRPC server. `None` = plaintext gRPC (dev only).
    ///
    /// In production, set this along with a matching
    /// [`rr_raft::network_factory::ChannelPool::with_tls`] for outbound
    /// peer connections; inbound and outbound TLS should use the same
    /// cert/key pair for symmetric mTLS.
    pub tls_config: Option<TlsConfig>,
    /// Bearer-token auth policy for AdminService (always separate token
    /// from ClientService) and optionally ClientService. Defaults to
    /// fully open (dev). **In production you must set at least
    /// `admin_token`** — otherwise any network-reachable peer can
    /// trigger snapshots, transfer leadership, etc.
    pub auth_config: AuthConfig,
    /// Per-service rate limits (admin / client). `RaftService` is never
    /// rate-limited — throttling Raft RPCs destabilizes consensus.
    /// Defaults to unlimited on every service.
    pub rate_limit_config: RateLimitConfig,
    /// Openraft `LogsSinceLast(N)` snapshot trigger threshold.
    /// Default `rr_raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST` (50_000).
    /// See [ADR A-0008] for the trade-off between snapshot frequency and
    /// recovery replay length.
    ///
    /// [ADR A-0008]: https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0008-snapshot-strategy.md
    pub snapshot_logs_since_last: u64,
    /// Automatically promote caught-up learners to voters
    /// (`RaftGroupManager::start_learner_promoter` on every locally started
    /// group). Default `true` — the promoter is quorum-guarded server-side
    /// and only promotes learners whose replication lag stays within
    /// tolerance for a stability window. Set `false` to drive membership
    /// exclusively by hand (rr-admin `change-membership`).
    pub auto_promote_learners: bool,
    /// **DANGEROUS — development escape hatch.**
    ///
    /// When `false` (the default), [`RrNode::new`](super::node::RrNode::new)
    /// refuses to start if any of the production safety invariants are
    /// violated:
    ///
    /// 1. `RaftService` enabled (i.e. `raft_lookup` is `Some`) without TLS.
    /// 2. `RaftService` enabled with TLS but `tls_config.require_client_cert`
    ///    is `false` (RaftService relies on mTLS for peer identity).
    /// 3. Bearer-token auth required (any token in [`AuthConfig`]) without
    ///    TLS — the token would travel in plaintext.
    /// 4. `AdminService` enabled (`admin_port = Some`) without TLS.
    /// 5. `AdminService` enabled with no `admin_token` — anyone reaching
    ///    the port can transfer leadership / trigger snapshots.
    /// 6. `raft_transport` is [`RaftTransport::Tcp`] — the TCP frame
    ///    protocol has no TLS and no peer authentication of any kind, so
    ///    in v0.2.0 it is a dev/benchmark-only transport requiring an
    ///    explicit opt-in, *regardless* of gRPC TLS/mTLS state.
    /// 7. `raft_transport` is [`RaftTransport::Tcp`] and the planned bind
    ///    address is not loopback.
    ///
    /// Setting this to `true` bypasses ALL of the above checks **except
    /// gate 7**: a plaintext, unauthenticated TCP raft listener is never
    /// allowed to bind off-loopback, in any mode. This is the first
    /// deliberate exception to the "bypass everything" semantics — see
    /// ADR A-0014 for the rationale. Use `true` only
    /// for local dev / examples / CI. Production binaries should never
    /// flip this in their default config; operators must opt in
    /// explicitly via `--allow-insecure` (CLI) or
    /// `RR_ALLOW_INSECURE=1` (env).
    ///
    /// When set, the node:
    /// - emits a single multi-line `tracing::warn!` at startup listing
    ///   every gate that was bypassed and the bound `listen_addr`, and
    /// - exposes a Prometheus gauge `rr_security_insecure_mode = 1` so
    ///   monitoring can alert.
    pub allow_insecure: bool,
}

impl NodeConfig {
    /// Create a minimal `NodeConfig`.
    ///
    /// Both `raft_lookup` and `client_dispatch` default to `None`; use the
    /// builder methods to add them.
    pub fn new(
        node_id: NodeId,
        listen_addr: String,
        storage_engine: Arc<dyn StorageEngine>,
        config_provider: Arc<dyn ConfigProvider>,
        state_machine_factory: StateMachineFactory,
    ) -> Self {
        Self {
            node_id,
            listen_addr,
            storage_engine,
            config_provider,
            state_machine_factory,
            raft_lookup: None,
            raft_transport: RaftTransport::Grpc,
            client_dispatch: None,
            group_manager: None,
            metrics_port: None,
            admin_port: None,
            admin_bind_host: DEFAULT_ADMIN_BIND_HOST.to_string(),
            metrics_bind_host: DEFAULT_METRICS_BIND_HOST.to_string(),
            grpc_max_concurrent_streams: None,
            grpc_max_frame_size: None,
            tls_config: None,
            auth_config: AuthConfig::default(),
            rate_limit_config: RateLimitConfig::default(),
            snapshot_logs_since_last: rr_raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST,
            auto_promote_learners: true,
            allow_insecure: false,
        }
    }

    /// Override the openraft `LogsSinceLast(N)` snapshot trigger threshold.
    /// See [`Self::snapshot_logs_since_last`] for guidance.
    pub fn with_snapshot_logs_since_last(mut self, n: u64) -> Self {
        self.snapshot_logs_since_last = n;
        self
    }

    /// Toggle automatic learner promotion. See
    /// [`Self::auto_promote_learners`].
    pub fn with_auto_promote_learners(mut self, enabled: bool) -> Self {
        self.auto_promote_learners = enabled;
        self
    }

    /// Attach a [`RaftGroupLookup`] for inter-node Raft RPCs.
    pub fn with_raft_lookup(mut self, lookup: Arc<dyn RaftGroupLookup>) -> Self {
        self.raft_lookup = Some(lookup);
        self
    }

    /// Declare which transport carries inter-node Raft RPCs. See
    /// [`NodeConfig::raft_transport`] for the security gates (G6/G7) that
    /// [`RaftTransport::Tcp`] triggers in `RrNode::new`.
    pub fn with_raft_transport(mut self, transport: RaftTransport) -> Self {
        self.raft_transport = transport;
        self
    }

    /// Attach a [`ClientDispatch`] for client-facing RPCs.
    pub fn with_client_dispatch(mut self, dispatch: Arc<dyn ClientDispatch>) -> Self {
        self.client_dispatch = Some(dispatch);
        self
    }

    /// Attach a pre-built [`RaftGroupManager`].
    pub fn with_group_manager(mut self, mgr: Arc<RaftGroupManager>) -> Self {
        self.group_manager = Some(mgr);
        self
    }

    /// Enable Prometheus metrics export on the given port.
    pub fn with_metrics_port(mut self, port: u16) -> Self {
        self.metrics_port = Some(port);
        self
    }

    /// Enable the Admin gRPC service on the given port.
    pub fn with_admin_port(mut self, port: u16) -> Self {
        self.admin_port = Some(port);
        self
    }

    /// Set the Admin gRPC bind host (default loopback — see
    /// [`NodeConfig::admin_bind_host`]). Must be an IP literal; combined
    /// with `admin_port` into the bind address at `start()`.
    pub fn with_admin_bind_host(mut self, host: impl Into<String>) -> Self {
        self.admin_bind_host = host.into();
        self
    }

    /// Override the gRPC server's HTTP/2 `max_concurrent_streams`.
    /// See [`Self::grpc_max_concurrent_streams`].
    pub fn with_grpc_max_concurrent_streams(mut self, n: u32) -> Self {
        self.grpc_max_concurrent_streams = Some(n);
        self
    }

    /// Override the gRPC server's HTTP/2 max frame size (bytes).
    /// See [`Self::grpc_max_frame_size`].
    pub fn with_grpc_max_frame_size(mut self, n: u32) -> Self {
        self.grpc_max_frame_size = Some(n);
        self
    }

    /// Set the HTTP observability (`/health` `/ready` `/metrics`) bind
    /// host (default loopback — see [`NodeConfig::metrics_bind_host`]).
    /// Must be an IP literal; combined with `metrics_port` at `start()`.
    pub fn with_metrics_bind_host(mut self, host: impl Into<String>) -> Self {
        self.metrics_bind_host = host.into();
        self
    }

    /// Enable TLS on all gRPC endpoints hosted by this node (Client, Raft, Admin).
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls_config = Some(tls);
        self
    }

    /// Attach bearer-token auth policy. Setting `admin_token` enforces auth on
    /// AdminService; setting `client_token` enforces auth on ClientService.
    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.auth_config = auth;
        self
    }

    /// Attach rate-limit policy (admin / client services only).
    pub fn with_rate_limit(mut self, rl: RateLimitConfig) -> Self {
        self.rate_limit_config = rl;
        self
    }

    /// **DANGEROUS** — bypass startup security validation. See
    /// [`NodeConfig::allow_insecure`] for the full list of gates that
    /// are skipped. Intended only for local dev, examples, and CI.
    pub fn with_allow_insecure(mut self, allow: bool) -> Self {
        self.allow_insecure = allow;
        self
    }

    /// Validate configuration fields and return `Err` on first violation.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.node_id == 0 {
            return Err(ConfigError::InvalidConfig(
                "node_id must be > 0".to_string(),
            ));
        }

        if self.listen_addr.parse::<std::net::SocketAddr>().is_err() {
            return Err(ConfigError::InvalidConfig(format!(
                "listen_addr '{}' is not a valid host:port",
                self.listen_addr
            )));
        }

        // Fail at config time (not first bind) when a bind host is not an
        // IP literal — a typo'd host would otherwise surface as an opaque
        // address-parse error deep in `start()`.
        if self.admin_port.is_some() && self.admin_bind_host.parse::<std::net::IpAddr>().is_err() {
            return Err(ConfigError::InvalidConfig(format!(
                "admin_bind_host '{}' is not a valid IP address literal",
                self.admin_bind_host
            )));
        }
        if self.metrics_port.is_some()
            && self.metrics_bind_host.parse::<std::net::IpAddr>().is_err()
        {
            return Err(ConfigError::InvalidConfig(format!(
                "metrics_bind_host '{}' is not a valid IP address literal",
                self.metrics_bind_host
            )));
        }

        if let Some(ref tls) = self.tls_config {
            if tls.cert_path.as_os_str().is_empty() || tls.key_path.as_os_str().is_empty() {
                return Err(ConfigError::InvalidConfig(
                    "TLS config requires both cert_path and key_path".to_string(),
                ));
            }
        }

        if let Some(n) = self.grpc_max_concurrent_streams {
            if n == 0 {
                return Err(ConfigError::InvalidConfig(
                    "grpc_max_concurrent_streams must be >= 1".to_string(),
                ));
            }
        }
        if let Some(n) = self.grpc_max_frame_size {
            if n == 0 {
                return Err(ConfigError::InvalidConfig(
                    "grpc_max_frame_size must be >= 1 (bytes)".to_string(),
                ));
            }
            // Same coupling the binary entrypoint enforces in config_file.rs:
            // the gRPC ingress frame must never exceed the apply-side bounded
            // decode cap, or a large keyed write would encode and replicate
            // fine but fail FramedEntry::decode at apply time — and apply
            // errors are group-fatal (ADR A-0018). Library embedders get the
            // same protection here, not only via the binary config path.
            let cap = rr_core::bounded_decode::MAX_ENTRY_BODY as u64;
            if u64::from(n) > cap {
                return Err(ConfigError::InvalidConfig(format!(
                    "grpc_max_frame_size ({n} bytes) exceeds the apply-side decode cap \
                     MAX_ENTRY_BODY ({cap} bytes); lower grpc_max_frame_size to at most {cap} \
                     bytes (ADR A-0018)"
                )));
            }
        }

        // Rate-limit field/combination rules (rps ceiling, per-caller
        // requires the global fallback bucket) live next to the policy
        // itself in rr-network.
        self.rate_limit_config.validate()?;

        Ok(())
    }
}

pub(crate) fn config_fingerprint(config: &NodeConfig, runtime: &NodeRuntimeConfig) -> String {
    let descriptor = runtime.descriptor.as_ref().map(|descriptor| {
        json!({
            "client_addr": descriptor.client_addr,
            "admin_addr": descriptor.admin_addr,
            "observability_addr": descriptor.observability_addr,
            "advertise_addr": descriptor.advertise_addr,
            "environment": descriptor.environment,
            "region": descriptor.region,
            "zone": descriptor.zone,
            "rack": descriptor.rack,
        })
    });
    let raft_transport = match &config.raft_transport {
        RaftTransport::Grpc => json!({"kind": "grpc"}),
        RaftTransport::Tcp { bind_addr } => json!({"kind": "tcp", "bind_addr": bind_addr}),
    };
    let safe = json!({
        "schema": 1,
        "node_id": config.node_id,
        "listen_addr": config.listen_addr,
        "admin_port": config.admin_port,
        "admin_bind_host": config.admin_bind_host,
        "metrics_port": config.metrics_port,
        "metrics_bind_host": config.metrics_bind_host,
        "grpc_max_concurrent_streams": config.grpc_max_concurrent_streams,
        "grpc_max_frame_size": config.grpc_max_frame_size,
        "raft_transport": raft_transport,
        "snapshot_logs_since_last": config.snapshot_logs_since_last,
        "auto_promote_learners": config.auto_promote_learners,
        "allow_insecure": config.allow_insecure,
        "tls": {
            "enabled": config.tls_config.is_some(),
            "require_client_cert": config.tls_config.as_ref().is_some_and(|tls| tls.require_client_cert),
        },
        "auth": {
            "admin_required": config.auth_config.admin_required(),
            "client_required": config.auth_config.client_required(),
        },
        "rate_limit": {
            "admin_rps": config.rate_limit_config.admin_rps,
            "client_rps": config.rate_limit_config.client_rps,
            "burst": config.rate_limit_config.burst,
            "per_caller_admin_rps": config.rate_limit_config.per_caller_admin_rps,
            "per_caller_client_rps": config.rate_limit_config.per_caller_client_rps,
            "per_caller_burst": config.rate_limit_config.per_caller_burst,
        },
        "runtime": {
            "app_version": runtime.app_version,
            "journal_dir": runtime.journal_dir,
            "descriptor": descriptor,
        },
    });
    let encoded = serde_json::to_vec(&safe).expect("secret-free config JSON is serializable");
    format!("{:x}", Sha256::digest(encoded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rr_config::mem::MemConfigProvider;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;
    use rr_storage::mem::MemStorage;

    struct Noop;

    #[async_trait::async_trait]
    impl StateMachine for Noop {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, rr_core::RrError> {
            Ok(entry.to_vec())
        }

        async fn snapshot(&self) -> Result<Vec<u8>, rr_core::RrError> {
            Ok(Vec::new())
        }

        async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), rr_core::RrError> {
            Ok(())
        }

        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    fn config() -> NodeConfig {
        NodeConfig::new(
            1,
            "127.0.0.1:5000".into(),
            Arc::new(MemStorage::new()),
            Arc::new(MemConfigProvider::new()),
            Box::new(|_| Box::new(Noop)),
        )
    }

    #[test]
    fn fingerprint_is_deterministic_and_excludes_secret_material() {
        let first = config().with_auth(AuthConfig {
            admin_token: Some("admin-secret".into()),
            client_token: Some("client-secret".into()),
        });
        let second = config().with_auth(AuthConfig {
            admin_token: Some("different-admin-secret".into()),
            client_token: Some("different-client-secret".into()),
        });
        let runtime = NodeRuntimeConfig::new(1, String::new());
        let fingerprint = config_fingerprint(&first, &runtime);
        assert_eq!(fingerprint.len(), 64);
        assert_eq!(fingerprint, config_fingerprint(&first, &runtime));
        assert!(!fingerprint.contains("secret"));
        assert_eq!(fingerprint, config_fingerprint(&second, &runtime));
        assert_ne!(
            fingerprint,
            config_fingerprint(&config().with_metrics_port(9000), &runtime)
        );
    }
}
