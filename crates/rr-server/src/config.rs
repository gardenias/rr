//! Node configuration for [`RrNode`](super::node::RrNode).

use std::sync::Arc;

use rr_core::config::ConfigProvider;
use rr_core::storage::StorageEngine;
use rr_core::types::NodeId;
use rr_network::auth::AuthConfig;
use rr_network::client_service::ClientDispatch;
use rr_network::raft_service::RaftGroupLookup;
use rr_network::rate_limit::RateLimitConfig;
use rr_network::tls::TlsConfig;
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::group_manager::StateMachineFactory;

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
    /// Box::new(|_group_id| Box::new(ServiceStateMachineAdapter::new(MyService::new())))
    /// ```
    pub state_machine_factory: StateMachineFactory,
    /// Optional Raft RPC lookup — enables inter-node Raft RPCs.
    ///
    /// If `None`, the gRPC server is not started.
    pub raft_lookup: Option<Arc<dyn RaftGroupLookup>>,
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
            client_dispatch: None,
            group_manager: None,
            metrics_port: None,
            admin_port: None,
            tls_config: None,
            auth_config: AuthConfig::default(),
            rate_limit_config: RateLimitConfig::default(),
        }
    }

    /// Attach a [`RaftGroupLookup`] for inter-node Raft RPCs.
    pub fn with_raft_lookup(mut self, lookup: Arc<dyn RaftGroupLookup>) -> Self {
        self.raft_lookup = Some(lookup);
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
}
