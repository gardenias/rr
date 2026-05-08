//! rr-server — generic distributed-consensus node binary.
//!
//! Ships with a built-in [`EchoStateMachine`](rr_server::builtin_service::EchoStateMachine)
//! for smoke testing. Applications with real business logic should fork
//! `examples/kv-store` or `examples/counter` and embed the rr crates
//! directly as a library, rather than customizing this binary.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use openraft::BasicNode;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::state_machine::StateMachine;
use rr_core::storage::StorageEngine;
use rr_core::types::{NodeAddr, NodeId, PartitionConfig};
use rr_network::auth::AuthConfig;
use rr_network::rate_limit::RateLimitConfig;
use rr_network::tls::TlsConfig;
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike, TlsPaths};
use rr_router::router::ShardRouter;
use rr_server::builtin_service::EchoStateMachine;
use rr_server::cli::{Cli, Command, ServeArgs};
use rr_server::config::NodeConfig;
#[cfg(feature = "rocks")]
use rr_server::config_file::DurabilityChoice;
use rr_server::config_file::{ResolvedConfig, ResolvedTls, StorageBackend};
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

    let node = start_node(&cfg)
        .await
        .context("failed to start rr-server")?;

    info!(
        node_id = cfg.node_id,
        "rr-server running — send SIGTERM or Ctrl-C to shut down"
    );
    wait_for_shutdown_signal().await?;

    info!(node_id = cfg.node_id, "shutdown signal received");
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

async fn start_node(cfg: &ResolvedConfig) -> Result<Arc<RrNode>> {
    let config_provider = build_config_provider(cfg).await?;
    let provider: Arc<dyn ConfigProvider> = Arc::clone(&config_provider) as _;
    let storage = build_storage(cfg)?;

    let tls_config = build_tls_config(cfg.tls.as_ref());
    let pool: Arc<dyn ChannelPoolLike> = match cfg.tls.as_ref() {
        Some(tls) => Arc::new(ChannelPool::with_tls(TlsPaths {
            cert_path: tls.cert_path.clone(),
            key_path: tls.key_path.clone(),
            ca_path: tls.ca_path.clone(),
            domain_name: tls.domain_name.clone(),
        })),
        None => Arc::new(ChannelPool::new()),
    };

    let mgr = Arc::new(RaftGroupManager::new_with_network(
        cfg.node_id,
        echo_factory(),
        pool,
    ));

    let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
    let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
    let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));

    let mut node_config = NodeConfig::new(
        cfg.node_id,
        cfg.listen_addr.clone(),
        storage,
        provider,
        echo_factory(),
    )
    .with_group_manager(Arc::clone(&mgr))
    .with_raft_lookup(lookup)
    .with_client_dispatch(dispatch);

    if let Some(port) = cfg.admin_port {
        node_config = node_config.with_admin_port(port);
    }
    if let Some(port) = cfg.metrics_port {
        node_config = node_config.with_metrics_port(port);
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
        per_caller_admin_rps: None,
        per_caller_client_rps: None,
        per_caller_burst: None,
    };
    node_config = node_config.with_rate_limit(rl_config);

    if cfg.allow_insecure {
        node_config = node_config.with_allow_insecure(true);
    }

    let node = Arc::new(RrNode::new(node_config)?);
    node.start().await?;

    if cfg.bootstrap {
        bootstrap_single_member(&node, cfg).await?;
    }

    Ok(node)
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

    provider
        .add_partition(1, PartitionConfig::SingleId(1), 1, all_node_ids)
        .await;

    Ok(provider)
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
                let mut rocks_cfg = RocksConfig::default();
                rocks_cfg.durability = match cfg.durability {
                    DurabilityChoice::Strict => DurabilityPolicy::Strict,
                    DurabilityChoice::ReplicatedOnly => DurabilityPolicy::ReplicatedOnly,
                    DurabilityChoice::TestOnly => DurabilityPolicy::TestOnly,
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

async fn bootstrap_single_member(node: &Arc<RrNode>, cfg: &ResolvedConfig) -> Result<()> {
    let raft = node
        .group_manager()
        .get_raft(1)
        .await
        .ok_or_else(|| anyhow!("group 1 not started after node.start()"))?;

    let mut members = BTreeMap::new();
    members.insert(
        cfg.node_id,
        BasicNode {
            addr: cfg.listen_addr.clone(),
        },
    );

    match raft.initialize(members).await {
        Ok(()) => info!(node_id = cfg.node_id, "cluster bootstrapped"),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("already") || msg.contains("Initialized") {
                warn!(
                    node_id = cfg.node_id,
                    "cluster already initialized; continuing"
                );
            } else {
                return Err(anyhow!("raft.initialize failed: {msg}"));
            }
        }
    }
    Ok(())
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
