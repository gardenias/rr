//! KV Store demo — single-node and multi-node `rr` distributed framework example.
//!
//! # Usage
//!
//! ## Single-node (demo mode, no gRPC server needed):
//! ```text
//! cargo run -p kv-store
//! ```
//!
//! ## Multi-node cluster — bootstrap the first node:
//! ```text
//! cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap
//! ```
//!
//! ## Multi-node cluster — join additional nodes:
//! ```text
//! cargo run -p kv-store -- --node-id 2 --port 51002 --peers 127.0.0.1:51001
//! cargo run -p kv-store -- --node-id 3 --port 51003 --peers 127.0.0.1:51001,127.0.0.1:51002
//! ```
//!
//! ## Arguments:
//! - `--node-id <N>`       — Node ID (u64). Defaults to 1 for single-node demo.
//! - `--port <PORT>`       — gRPC listen port. Defaults to 51001.
//! - `--peers <ADDR,...>`  — Comma-separated peer addresses for multi-node mode.
//! - `--bootstrap`         — Bootstrap as cluster leader (only on first node).
//! - `--demo`              — Run KV demo writes and exit (default when no args given).
//!
//! # Architecture
//!
//! Each node is an `RrNode` with:
//! - `MemStorage` for key/value data (no RocksDB in this example)
//! - `KvService` wrapped in `ServiceStateMachineAdapter` as the state machine
//! - `RaftGroupLookupImpl` for intra-cluster Raft RPCs
//! - `ClientDispatchImpl` for client write routing
//! - `ShardRouter` for partition-to-group mapping

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use openraft::BasicNode;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::state_machine::StateMachine;
use rr_core::storage::StorageEngine;
use rr_core::types::{NodeId, PartitionConfig};
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
use rr_raft::service_adapter::ServiceStateMachineAdapter;
use rr_router::router::ShardRouter;
use rr_server::config::NodeConfig;
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use kv_store::service::KvService;
use kv_store::types::{KvRequest, KvResponse};

// ── CLI argument parsing ──────────────────────────────────────────────────────

/// Parsed CLI configuration.
struct CliConfig {
    node_id: NodeId,
    port: u16,
    peers: Vec<String>,
    bootstrap: bool,
    demo: bool,
}

impl CliConfig {
    fn parse() -> Self {
        let args: Vec<String> = std::env::args().skip(1).collect();

        if args.is_empty() {
            // No args → run the single-node demo
            return Self {
                node_id: 1,
                port: 51001,
                peers: vec![],
                bootstrap: false,
                demo: true,
            };
        }

        let mut node_id: NodeId = 1;
        let mut port: u16 = 51001;
        let mut peers: Vec<String> = vec![];
        let mut bootstrap = false;
        let mut demo = false;
        let mut i = 0;

        while i < args.len() {
            match args[i].as_str() {
                "--node-id" => {
                    i += 1;
                    node_id = args[i].parse().expect("--node-id must be a u64");
                }
                "--port" => {
                    i += 1;
                    port = args[i].parse().expect("--port must be a u16");
                }
                "--peers" => {
                    i += 1;
                    peers = args[i].split(',').map(|s| s.trim().to_string()).collect();
                }
                "--bootstrap" => bootstrap = true,
                "--demo" => demo = true,
                other => {
                    tracing::warn!("Unknown argument: {other}");
                }
            }
            i += 1;
        }

        Self {
            node_id,
            port,
            peers,
            bootstrap,
            demo,
        }
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // ── 1. Initialise tracing ─────────────────────────────────────────────────
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .try_init();

    let cli = CliConfig::parse();

    if cli.demo {
        run_demo().await
    } else {
        run_node(cli).await
    }
}

// ── Demo mode (single-node, no gRPC) ─────────────────────────────────────────

async fn run_demo() -> Result<()> {
    info!("=== rr KV-Store demo (single-node, no gRPC) ===");

    // Partition 1  →  group 1  →  replica [node 1]
    let config_provider = MemConfigProvider::new();
    config_provider
        .add_partition(1, PartitionConfig::SingleId(1), 1, vec![1])
        .await;
    let config_provider: Arc<dyn ConfigProvider> = Arc::new(config_provider);

    let storage_engine: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());

    let sm_factory = Box::new(|group_id| -> Box<dyn StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(group_id, KvService::new()))
    });

    let config = NodeConfig::new(
        1,                         // node_id
        "127.0.0.1:0".to_string(), // port 0 = OS picks one (no gRPC in this demo)
        storage_engine,
        config_provider,
        sm_factory,
    );

    let node = RrNode::new(config)?;
    node.start().await?;
    info!(
        "Node started — group count: {}",
        node.group_manager().group_count().await
    );

    // Initialize the single-member Raft group
    let raft = node
        .group_manager()
        .get_raft(1)
        .await
        .expect("group 1 must exist after start()");

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members)
        .await
        .map_err(|e| anyhow!("Raft init failed: {e}"))?;
    info!("Raft group 1 initialized");

    // Wait for leader election (single-node, should be instant)
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Run demo operations
    demo_put(&raft, "hello", b"world").await?;
    demo_get(&raft, "hello").await?;
    demo_put(&raft, "counter", b"1").await?;
    demo_delete(&raft, "hello").await?;
    demo_get(&raft, "hello").await?; // should be None after delete
    demo_get(&raft, "counter").await?;

    node.shutdown().await?;
    info!("=== rr KV-Store demo complete ===");
    Ok(())
}

// ── Multi-node mode (with gRPC server) ───────────────────────────────────────

async fn run_node(cli: CliConfig) -> Result<()> {
    let listen_addr = format!("127.0.0.1:{}", cli.port);
    info!(
        node_id = cli.node_id,
        addr = %listen_addr,
        peers = ?cli.peers,
        bootstrap = cli.bootstrap,
        "=== rr KV-Store multi-node starting ==="
    );

    // ── Build shared config provider ──────────────────────────────────────────
    let config_provider = Arc::new(MemConfigProvider::new());

    // Register this node
    config_provider
        .add_node(
            cli.node_id,
            rr_core::types::NodeAddr {
                addr: listen_addr.clone(),
            },
        )
        .await;

    // Register partition 1 → group 1
    // In bootstrap mode: this node is the sole initial replica.
    // In join mode: all provided peers + this node are replicas.
    let all_node_ids: Vec<NodeId> = if cli.bootstrap {
        vec![cli.node_id]
    } else {
        // Collect IDs: this node + all peers (IDs inferred from order for simplicity)
        let mut ids = vec![cli.node_id];
        // Peers don't carry their node IDs in addr form; for real deployments
        // you would use a discovery service. Here we register them by index.
        for (i, _peer) in cli.peers.iter().enumerate() {
            let peer_id = (i + 1) as NodeId;
            if peer_id != cli.node_id {
                ids.push(peer_id);
            }
        }
        ids
    };

    config_provider
        .add_partition(1, PartitionConfig::SingleId(1), 1, all_node_ids.clone())
        .await;

    let provider: Arc<dyn ConfigProvider> = Arc::clone(&config_provider) as _;

    // ── Build RaftGroupManager with real gRPC network ─────────────────────────
    let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
    let sm_factory = Box::new(|group_id| -> Box<dyn StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(group_id, KvService::new()))
    });

    let mgr = Arc::new(RaftGroupManager::new_with_network(
        cli.node_id,
        sm_factory,
        pool.clone(),
    ));

    // ── Wire lookup and dispatch ──────────────────────────────────────────────
    let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
    let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
    let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));

    // ── Build and start RrNode ────────────────────────────────────────────────
    let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
    let dummy_factory = Box::new(|group_id| -> Box<dyn StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(group_id, KvService::new()))
    });

    let config = NodeConfig::new(
        cli.node_id,
        listen_addr.clone(),
        storage,
        provider,
        dummy_factory,
    )
    .with_group_manager(Arc::clone(&mgr))
    .with_raft_lookup(lookup)
    .with_client_dispatch(dispatch)
    .with_allow_insecure(true);

    let node = Arc::new(RrNode::new(config)?);
    node.start().await?;
    info!("Node {} started — listening on {listen_addr}", cli.node_id);

    // ── Bootstrap or initialize cluster ──────────────────────────────────────
    if cli.bootstrap {
        // Bootstrap: initialize single-node cluster, then wait for peers to join
        let raft = node
            .group_manager()
            .get_raft(1)
            .await
            .ok_or_else(|| anyhow!("group 1 not started"))?;

        let mut members = BTreeMap::new();
        members.insert(
            cli.node_id,
            BasicNode {
                addr: listen_addr.clone(),
            },
        );
        let _ = raft.initialize(members).await; // idempotent
        info!("Cluster bootstrapped on node {}", cli.node_id);
    } else if !cli.peers.is_empty() {
        info!("Joining existing cluster via peers: {:?}", cli.peers);
        // In a full implementation this would call the leader's add_learner API.
        // For this demo we just note the peers — the watch loop handles dynamic adds.
    }

    // ── Keep node running until Ctrl-C ────────────────────────────────────────
    info!("Node {} running. Press Ctrl-C to shut down.", cli.node_id);
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| anyhow!("failed to listen for Ctrl-C: {e}"))?;

    info!("Shutting down node {}...", cli.node_id);
    node.shutdown().await?;
    info!("Node {} shut down cleanly.", cli.node_id);
    Ok(())
}

// ── Demo helpers ──────────────────────────────────────────────────────────────

type RaftInstance =
    openraft::Raft<rr_raft::type_config::RrTypeConfig, rr_raft::sm_adapter::RrStateMachine>;

async fn propose(raft: &RaftInstance, req: &KvRequest) -> Result<KvResponse> {
    use bincode::config::standard;

    let payload = bincode::serde::encode_to_vec(req, standard())
        .map_err(|e| anyhow!("encode failed: {e}"))?;

    let response = raft
        .client_write(payload.into())
        .await
        .map_err(|e| anyhow!("client_write failed: {e}"))?;

    let (resp, _): (KvResponse, _) =
        bincode::serde::decode_from_slice(response.data.as_ref(), standard())
            .map_err(|e| anyhow!("decode failed: {e}"))?;

    Ok(resp)
}

async fn demo_put(raft: &RaftInstance, key: &str, value: &[u8]) -> Result<()> {
    let req = KvRequest::Put {
        key: key.to_string(),
        value: value.to_vec(),
    };
    propose(raft, &req).await?;
    info!("PUT {key} = {:?}", String::from_utf8_lossy(value));
    Ok(())
}

async fn demo_get(raft: &RaftInstance, key: &str) -> Result<()> {
    let req = KvRequest::Get {
        key: key.to_string(),
    };
    let resp = propose(raft, &req).await?;
    match &resp.value {
        Some(v) => info!("GET {key} = {:?}", String::from_utf8_lossy(v)),
        None => info!("GET {key} = <not found>"),
    }
    Ok(())
}

async fn demo_delete(raft: &RaftInstance, key: &str) -> Result<()> {
    let req = KvRequest::Delete {
        key: key.to_string(),
    };
    propose(raft, &req).await?;
    info!("DEL {key}");
    Ok(())
}
