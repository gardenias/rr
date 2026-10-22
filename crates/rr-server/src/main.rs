//! rr-server — generic distributed-consensus node binary.
//!
//! Ships with a built-in [`EchoStateMachine`](rr_server::builtin_service::EchoStateMachine)
//! for smoke testing. Applications with real business logic should fork
//! `examples/kv-store` or `examples/counter` and embed the rr crates
//! directly as a library, rather than customizing this binary.

use std::collections::BTreeMap;
use std::sync::Arc;

// Allocator A/B switch (docs/perf/roadmap-100k.md M2): with `--features
// mimalloc` the whole process — tonic/h2, openraft, storage — allocates via
// mimalloc. Off by default; promote only on measured improvement.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use openraft::error::{InitializeError, RaftError};
use openraft::BasicNode;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::state_machine::StateMachine;
use rr_core::storage::StorageEngine;
use rr_core::types::{NodeAddr, NodeId, PartitionConfig};
use rr_network::auth::AuthConfig;
use rr_network::rate_limit::RateLimitConfig;
use rr_network::tcp_raft_server::TcpRaftServer;
use rr_network::tls::TlsConfig;
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike, TlsPaths};
use rr_raft::tcp_transport::TcpConnectionPool;
use rr_raft::RrTypeConfig;
use rr_router::router::ShardRouter;
use rr_server::builtin_service::EchoStateMachine;
use rr_server::cli::{Cli, Command, ServeArgs};
use rr_server::config::{NodeConfig, RaftTransport};
#[cfg(feature = "rocks")]
use rr_server::config_file::DurabilityChoice;
use rr_server::config_file::{RaftTransportKind, ResolvedConfig, ResolvedTls, StorageBackend};
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!("rr-server {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Command::Serve(args) => {
            install_crypto_provider();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build tokio runtime")?;
            runtime.block_on(async { serve(args).await })
        }
    }
}

fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

async fn serve(args: ServeArgs) -> Result<()> {
    init_tracing(&args.log_level);

    let cfg = ResolvedConfig::resolve(&args).context("failed to resolve node configuration")?;

    info!(
        node_id = cfg.node_id,
        listen_addr = %cfg.listen_addr,
        storage = ?cfg.storage_backend,
        durability = ?cfg.durability,
        bootstrap = cfg.bootstrap,
        peers = ?cfg.peers,
        "starting rr-server"
    );

    let (node, tcp_raft_handle) = start_node(&cfg)
        .await
        .context("failed to start rr-server")?;

    info!(
        node_id = cfg.node_id,
        "rr-server running — send SIGTERM or Ctrl-C to shut down"
    );
    wait_for_shutdown_signal().await?;

    info!(node_id = cfg.node_id, "shutdown signal received");
    if let Some(handle) = tcp_raft_handle {
        // Stop accepting new raft RPCs BEFORE tearing the groups down —
        // AppendEntries arriving mid-shutdown would race stop_group.
        handle.abort();
        let _ = handle.await;
    }
    node.shutdown().await.context("graceful shutdown failed")?;
    info!(node_id = cfg.node_id, "rr-server shut down cleanly");
    Ok(())
}

fn init_tracing(filter: &str) {
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .try_init();
}

async fn start_node(
    cfg: &ResolvedConfig,
) -> Result<(Arc<RrNode>, Option<tokio::task::JoinHandle<()>>)> {
    let config_provider = build_config_provider(cfg).await?;
    let provider: Arc<dyn ConfigProvider> = Arc::clone(&config_provider) as _;
    let storage = build_storage(cfg)?;

    let tls_config = build_tls_config(cfg.tls.as_ref());
    let pool: Arc<dyn ChannelPoolLike> = if let Some(tls) = cfg.tls.as_ref() {
        Arc::new(ChannelPool::with_tls(TlsPaths {
            cert_path: tls.cert_path.clone(),
            key_path: tls.key_path.clone(),
            ca_path: tls.ca_path.clone(),
            domain_name: tls.domain_name.clone(),
        }))
    } else {
        Arc::new(ChannelPool::new())
    };

    let mgr = match cfg.raft_transport {
        RaftTransportKind::Grpc => {
            RaftGroupManager::new_with_network(cfg.node_id, echo_factory(), Arc::clone(&pool))
                .with_snapshot_logs_since_last(cfg.snapshot_logs_since_last)
        }
        RaftTransportKind::Tcp => {
            let tcp_pool = Arc::new(TcpConnectionPool::new(8));
            let grpc_port = parse_port_from_addr(&cfg.listen_addr)
                .context("failed to parse gRPC listen_addr port for tcp raft transport")?;
            let tcp_port = cfg.raft_tcp_port.ok_or_else(|| {
                anyhow!("raft_tcp_port must be set or derivable when raft_transport=tcp")
            })?;
            if tcp_port == grpc_port {
                // Equal ports derive a TCP bind address identical to the
                // gRPC listen_addr — it passes G7 when loopback and then
                // fails later with a bare "address already in use" (after
                // gRPC has already bound), and the outbound pool would dial
                // peers' gRPC ports with RRAF frames. Fail fast here instead.
                return Err(anyhow!(
                    "raft_tcp_port ({tcp_port}) must differ from the gRPC listen port ({grpc_port})"
                ));
            }
            let port_offset = tcp_port.checked_sub(grpc_port).ok_or_else(|| {
                anyhow!(
                    "raft_tcp_port ({tcp_port}) must be >= listen_addr port ({grpc_port}) for offset-based derivation"
                )
            })?;
            RaftGroupManager::new_with_tcp_network(
                cfg.node_id,
                echo_factory(),
                tcp_pool,
                Arc::clone(&pool),
                port_offset,
                cfg.tls.as_ref().map(|tls| TlsPaths {
                    cert_path: tls.cert_path.clone(),
                    key_path: tls.key_path.clone(),
                    ca_path: tls.ca_path.clone(),
                    domain_name: tls.domain_name.clone(),
                }),
            )
            .with_snapshot_logs_since_last(cfg.snapshot_logs_since_last)
        }
    };
    // ADR A-0016: with the rocks storage backend, persist the Raft log too
    // (`{data_dir}/raft-log/{group_id}`). Previously `--storage rocks` covered
    // only the SM-side storage — the default deployment's strict durability
    // contract did not hold across a full-cluster power loss.
    #[cfg(feature = "rocks")]
    let mgr = with_raft_log_dir(mgr, cfg);
    let mgr = Arc::new(mgr);

    // Derive the TCP raft bind address ONCE — single source of truth shared
    // by the `NodeConfig::raft_transport` descriptor (security gates G6/G7
    // run against it in `RrNode::new`, before anything binds) and by the
    // actual `TcpRaftServer::bind` below. Same `replace_port` derivation as
    // before, just hoisted so both consumers see the identical address.
    let tcp_bind_addr = if matches!(cfg.raft_transport, RaftTransportKind::Tcp) {
        let tcp_port = cfg.raft_tcp_port.ok_or_else(|| {
            anyhow!("raft_tcp_port must be set or derivable when raft_transport=tcp")
        })?;
        Some(
            replace_port(&cfg.listen_addr, tcp_port)
                .context("failed to derive tcp raft bind address")?,
        )
    } else {
        None
    };

    let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
    let lookup: Arc<dyn rr_network::raft_service::RaftGroupLookup> =
        Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
    let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));

    let mut node_config = NodeConfig::new(
        cfg.node_id,
        cfg.listen_addr.clone(),
        storage,
        provider,
        echo_factory(),
    )
    .with_group_manager(Arc::clone(&mgr))
    .with_raft_lookup(Arc::clone(&lookup))
    .with_client_dispatch(dispatch)
    .with_raft_transport(match &tcp_bind_addr {
        Some(bind_addr) => RaftTransport::Tcp {
            bind_addr: bind_addr.clone(),
        },
        None => RaftTransport::Grpc,
    });

    if let Some(port) = cfg.admin_port {
        node_config = node_config.with_admin_port(port);
    }
    if let Some(port) = cfg.metrics_port {
        node_config = node_config.with_metrics_port(port);
    }
    node_config = node_config
        .with_admin_bind_host(cfg.admin_bind_host.clone())
        .with_metrics_bind_host(cfg.metrics_bind_host.clone());
    if let Some(n) = cfg.grpc_max_concurrent_streams {
        node_config = node_config.with_grpc_max_concurrent_streams(n);
    }
    if let Some(n) = cfg.grpc_max_frame_size {
        node_config = node_config.with_grpc_max_frame_size(n);
    }
    if let Some(tls) = tls_config {
        node_config = node_config.with_tls(tls);
    }

    let auth_config = AuthConfig {
        admin_token: cfg.auth.admin_token.clone(),
        client_token: cfg.auth.client_token.clone(),
    };
    node_config = node_config.with_auth(auth_config);

    let rl_config = RateLimitConfig {
        admin_rps: cfg.rate_limit.admin_rps,
        client_rps: cfg.rate_limit.client_rps,
        burst: cfg.rate_limit.burst,
        per_caller_admin_rps: cfg.rate_limit.per_caller_admin_rps,
        per_caller_client_rps: cfg.rate_limit.per_caller_client_rps,
        per_caller_burst: cfg.rate_limit.per_caller_burst,
    };
    node_config = node_config.with_rate_limit(rl_config);

    node_config = node_config.with_snapshot_logs_since_last(cfg.snapshot_logs_since_last);
    node_config = node_config.with_auto_promote_learners(cfg.auto_promote_learners);

    if cfg.allow_insecure {
        node_config = node_config.with_allow_insecure(true);
    }

    let node = Arc::new(RrNode::new(node_config)?);
    node.start().await?;

    // From here on the node owns running raft groups: any startup failure
    // must exit through node.shutdown() so the log-close contract
    // (stop_group → RocksLogStorage::close, ADR A-0011) is honored instead
    // of being bypassed by process exit.
    let post_start_result: anyhow::Result<Option<tokio::task::JoinHandle<()>>> = async {
        let tcp_raft_handle = if let Some(tcp_bind_addr) = tcp_bind_addr {
            // G6/G7 already vetted this address in `RrNode::new` above; binding
            // here can only happen after those gates passed.
            let server = TcpRaftServer::bind(&tcp_bind_addr, Arc::clone(&lookup))
                .await
                .with_context(|| format!("failed to bind tcp raft server at {tcp_bind_addr}"))?;
            let local_addr = server.local_addr()?;
            info!(
                node_id = cfg.node_id,
                tcp_raft_addr = %local_addr,
                "tcp raft server listening"
            );
            Some(tokio::spawn(async move {
                if let Err(e) = server.serve().await {
                    tracing::error!(error = %e, "tcp raft server terminated");
                }
            }))
        } else {
            None
        };

        if cfg.bootstrap {
            bootstrap_hosted_groups(&node, cfg).await?;
        }
        Ok(tcp_raft_handle)
    }
    .await;

    let tcp_raft_handle = match post_start_result {
        Ok(handle) => handle,
        Err(err) => {
            if let Err(se) = node.shutdown().await {
                tracing::error!(
                    error = %se,
                    "graceful shutdown after startup failure also failed"
                );
            }
            return Err(err);
        }
    };

    Ok((node, tcp_raft_handle))
}

fn build_tls_config(resolved: Option<&ResolvedTls>) -> Option<TlsConfig> {
    resolved.map(|t| TlsConfig {
        cert_path: t.cert_path.clone(),
        key_path: t.key_path.clone(),
        ca_path: t.ca_path.clone(),
        require_client_cert: t.require_client_cert,
        domain_name: t.domain_name.clone(),
    })
}

async fn build_config_provider(cfg: &ResolvedConfig) -> Result<Arc<MemConfigProvider>> {
    let provider = Arc::new(MemConfigProvider::new());

    provider
        .add_node(
            cfg.node_id,
            NodeAddr {
                addr: cfg.listen_addr.clone(),
            },
        )
        .await;

    let all_node_ids: Vec<NodeId> = if cfg.bootstrap || cfg.peers.is_empty() {
        vec![cfg.node_id]
    } else {
        let mut ids = vec![cfg.node_id];
        for (i, _) in cfg.peers.iter().enumerate() {
            let peer_id = (i + 1) as NodeId;
            if peer_id != cfg.node_id {
                ids.push(peer_id);
            }
        }
        ids
    };

    // Stock topology: partition i → group i, every group replicated across
    // all peers. Multi-group (`--groups N`) exists for benchmarking/demos.
    for group_id in 1..=cfg.groups {
        provider
            .add_partition(
                group_id,
                PartitionConfig::SingleId(group_id),
                group_id,
                all_node_ids.clone(),
            )
            .await;
    }

    Ok(provider)
}

/// ADR A-0016: with the rocks storage backend, also persist the Raft log at
/// `{data_dir}/raft-log/{group_id}` (previously only the SM-side storage was
/// durable). Mem backend keeps the in-memory log, unchanged.
#[cfg(feature = "rocks")]
fn with_raft_log_dir(mgr: RaftGroupManager, cfg: &ResolvedConfig) -> RaftGroupManager {
    if !matches!(cfg.storage_backend, StorageBackend::Rocks) {
        return mgr;
    }
    // `build_storage` runs first and already requires data_dir for rocks.
    match cfg.data_dir.as_ref() {
        Some(dir) => mgr.with_rocks_base_path(format!("{}/raft-log", dir.display())),
        None => mgr,
    }
}

fn build_storage(cfg: &ResolvedConfig) -> Result<Arc<dyn StorageEngine>> {
    match cfg.storage_backend {
        StorageBackend::Mem => Ok(Arc::new(MemStorage::new())),
        StorageBackend::Rocks => {
            #[cfg(feature = "rocks")]
            {
                use rr_storage::rocks::{DurabilityPolicy, RocksConfig, RocksStorage};
                let data_dir = cfg
                    .data_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("rocks backend requires data_dir"))?;
                let rocks_cfg = RocksConfig {
                    durability: match cfg.durability {
                        DurabilityChoice::Strict => DurabilityPolicy::Strict,
                        DurabilityChoice::ReplicatedOnly => DurabilityPolicy::ReplicatedOnly,
                        DurabilityChoice::TestOnly => DurabilityPolicy::TestOnly,
                    },
                    ..RocksConfig::default()
                };
                let storage = RocksStorage::open(
                    data_dir
                        .to_str()
                        .ok_or_else(|| anyhow!("data_dir path is not valid UTF-8"))?,
                    vec!["raft_log", "state_machine", "metadata"],
                    rocks_cfg,
                )
                .with_context(|| format!("failed to open RocksDB at {}", data_dir.display()))?;
                Ok(Arc::new(storage))
            }
            #[cfg(not(feature = "rocks"))]
            {
                let _ = &cfg.durability;
                Err(anyhow!(
                    "storage = rocks requires rr-server to be built with the `rocks` feature"
                ))
            }
        }
    }
}

fn echo_factory() -> rr_raft::group_manager::StateMachineFactory {
    Box::new(|_group_id| -> Box<dyn StateMachine> { Box::new(EchoStateMachine::new()) })
}

/// Initialize every hosted group (`1..=cfg.groups`) with this node as the
/// single voter. Runs ONCE per cluster life (`--bootstrap`).
///
/// Idempotency contract: re-running against an already-initialized group
/// (restarted node still holding the `--bootstrap` flag, or a node rejoining
/// an existing cluster) must be a silent no-op, never a hard failure.
async fn bootstrap_hosted_groups(node: &Arc<RrNode>, cfg: &ResolvedConfig) -> Result<()> {
    for group_id in 1..=cfg.groups {
        let raft = node
            .group_manager()
            .get_raft(group_id)
            .await
            .ok_or_else(|| anyhow!("group {group_id} not started after node.start()"))?;

        // Primary gate: read membership state BEFORE calling initialize(), so
        // the common re-bootstrap path never enters the error branch at all.
        match raft.is_initialized().await {
            Ok(true) => {
                info!(
                    node_id = cfg.node_id,
                    group_id, "group already initialized; skipping bootstrap"
                );
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                return Err(anyhow!(
                    "raft.is_initialized failed for group {group_id}: {e}"
                ));
            }
        }

        let mut members = BTreeMap::new();
        members.insert(
            cfg.node_id,
            BasicNode {
                addr: cfg.listen_addr.clone(),
            },
        );

        match raft.initialize(members).await {
            Ok(()) => info!(node_id = cfg.node_id, group_id, "cluster bootstrapped"),
            Err(e) if is_already_initialized(&e) => {
                // Typed fallback for the race between the is_initialized()
                // check and initialize(): openraft documents NotAllowed as
                // "the cluster is already up and running — safe to ignore".
                warn!(
                    node_id = cfg.node_id,
                    group_id,
                    error = %e,
                    "cluster already initialized; continuing"
                );
            }
            Err(e) => {
                return Err(anyhow!("raft.initialize failed for group {group_id}: {e}"));
            }
        }
    }
    Ok(())
}

/// True when an `initialize()` failure denotes an already-initialized group
/// and bootstrap must degrade to an idempotent no-op.
///
/// Matching the typed [`InitializeError::NotAllowed`] variant keeps the
/// idempotency contract independent of the vendored openraft error-text
/// wording (the old `"already"/"Initialized"` substring match broke whenever
/// openraft rephrased its Display strings).
fn is_already_initialized(err: &RaftError<RrTypeConfig, InitializeError<RrTypeConfig>>) -> bool {
    matches!(err, RaftError::APIError(InitializeError::NotAllowed(_)))
}

async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?;
        let mut sigint =
            signal(SignalKind::interrupt()).context("failed to install SIGINT handler")?;
        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = sigint.recv() => info!("received SIGINT"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for Ctrl-C")?;
        info!("received Ctrl-C");
    }
    Ok(())
}

fn parse_port_from_addr(addr: &str) -> Result<u16> {
    let (_, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("listen addr '{addr}' must include port"))?;
    port.parse::<u16>()
        .with_context(|| format!("listen addr '{addr}' has invalid port"))
}

fn replace_port(addr: &str, new_port: u16) -> Result<String> {
    let (host, _) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("listen addr '{addr}' must include port"))?;
    Ok(format!("{host}:{new_port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_port_derives_tcp_bind_from_listen_addr() {
        assert_eq!(
            replace_port("127.0.0.1:51001", 61001).unwrap(),
            "127.0.0.1:61001"
        );
        assert_eq!(
            replace_port("0.0.0.0:51001", 61001).unwrap(),
            "0.0.0.0:61001"
        );
        assert!(replace_port("no-port", 61001).is_err());
    }

    #[test]
    fn derived_tcp_bind_addr_aligns_with_g7_loopback_gate() {
        // The single-derivation contract (ADR A-0014): whatever start_node
        // hands to both `NodeConfig::raft_transport` (G7 gate) and
        // `TcpRaftServer::bind` comes from this one `replace_port` call, so
        // the address the gate vets is the address that gets bound.
        let loopback = replace_port("127.0.0.1:51001", 61001).unwrap();
        assert!(loopback
            .parse::<std::net::SocketAddr>()
            .unwrap()
            .ip()
            .is_loopback());
        let wildcard = replace_port("0.0.0.0:51001", 61001).unwrap();
        assert!(!wildcard
            .parse::<std::net::SocketAddr>()
            .unwrap()
            .ip()
            .is_loopback());
    }

    #[test]
    fn already_initialized_matches_only_typed_not_allowed_variant() {
        use openraft::error::{NotAllowed, NotInMembers};
        use openraft::impls::Vote;
        use openraft::Membership;

        type InitError = RaftError<RrTypeConfig, InitializeError<RrTypeConfig>>;

        // The "already initialized" case: openraft InitializeError::NotAllowed
        // (e.g. re-bootstrap of a restarted node). Must be treated as an
        // idempotent no-op regardless of the vendored error-text wording.
        let not_allowed =
            InitError::APIError(InitializeError::NotAllowed(NotAllowed::<RrTypeConfig> {
                last_log_id: None,
                vote: Vote::new(0, 0),
            }));
        assert!(is_already_initialized(&not_allowed));

        // A genuine configuration error (this node is not in the initial
        // membership) must stay a hard failure.
        let membership = Membership::new(
            vec![std::iter::once(1u64).collect::<std::collections::BTreeSet<_>>()],
            std::collections::BTreeMap::from([(
                1u64,
                BasicNode {
                    addr: "127.0.0.1:51001".to_string(),
                },
            )]),
        )
        .unwrap();
        let not_in_members = InitError::APIError(InitializeError::NotInMembers(NotInMembers::<
            RrTypeConfig,
        > {
            node_id: 2,
            membership,
        }));
        assert!(!is_already_initialized(&not_in_members));

        // A fatal raft error (core stopped / storage failure) must stay a
        // hard failure.
        let fatal = InitError::Fatal(openraft::error::Fatal::Stopped);
        assert!(!is_already_initialized(&fatal));
    }
}
