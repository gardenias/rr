//! KV Store demo — single-node and multi-node `rr` distributed framework example.
//!
//! # Usage
//!
//! ## Single-node (demo mode, no gRPC server needed):
//! ```text
//! cargo run -p kv-store
//! ```
//!
//! ## Multi-node cluster — every node gets the FULL roster as `id@addr` pairs:
//! ```text
//! cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003
//! cargo run -p kv-store -- --node-id 2 --port 51002 --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003
//! cargo run -p kv-store -- --node-id 3 --port 51003 --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003
//! ```
//!
//! ## Container-style run — bind all interfaces, advertise via the roster:
//! ```text
//! kv-store --node-id 1 --listen-addr 0.0.0.0 --port 51001 --metrics-port 9100 \
//!     --bootstrap --peers 1@kv-1:51001,2@kv-2:51001,3@kv-3:51001
//! ```
//!
//! ## With Kafka input (requires `--features rdkafka`):
//! ```text
//! cargo run -p kv-store --features rdkafka -- --node-id 1 --bootstrap \
//!     --kafka-brokers redpanda:9092 --kafka-topic-prefix input_
//! ```
//!
//! ## Arguments:
//! - `--node-id <N>`            — Node ID (u64). Defaults to 1 for single-node demo.
//! - `--port <PORT>`            — gRPC listen port. Defaults to 51001.
//!   The admin API is served on PORT + 1000 (51001 → 52001).
//! - `--listen-addr <HOST>`     — gRPC bind host. Defaults to 127.0.0.1. Affects binding
//!   only; the advertised address always comes from `--peers`
//!   (single-node runs stay on 127.0.0.1).
//! - `--peers <ID@ADDR,...>`    — Full cluster roster, comma-separated `id@addr` pairs
//!   (must include this node). Empty means single-node roster.
//! - `--metrics-port <PORT>`    — Also serve /health, /ready and Prometheus /metrics
//!   over HTTP on this port. Off by default.
//! - `--kafka-brokers <A,...>`  — Comma-separated bootstrap brokers; consumes group 1's
//!   command topic with framework-managed exactly-once apply.
//!   Requires a binary built with `--features rdkafka`.
//! - `--kafka-topic-prefix <P>` — Topic prefix; group 1 consumes `{P}1`. Default `input_`.
//! - `--bootstrap`              — Initialize Raft membership (run on exactly one node).
//! - `--demo`                   — Run KV demo writes and exit (default when no args given).
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
    listen_addr: String,
    peers: Vec<String>,
    metrics_port: Option<u16>,
    kafka_brokers: Option<Vec<String>>,
    kafka_topic_prefix: String,
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
                listen_addr: "127.0.0.1".to_string(),
                peers: vec![],
                metrics_port: None,
                kafka_brokers: None,
                kafka_topic_prefix: "input_".to_string(),
                bootstrap: false,
                demo: true,
            };
        }

        let mut node_id: NodeId = 1;
        let mut port: u16 = 51001;
        let mut listen_addr = "127.0.0.1".to_string();
        let mut peers: Vec<String> = vec![];
        let mut metrics_port: Option<u16> = None;
        let mut kafka_brokers: Option<Vec<String>> = None;
        let mut kafka_topic_prefix = "input_".to_string();
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
                "--listen-addr" => {
                    i += 1;
                    listen_addr = args[i].clone();
                }
                "--peers" => {
                    i += 1;
                    peers = args[i].split(',').map(|s| s.trim().to_string()).collect();
                }
                "--metrics-port" => {
                    i += 1;
                    metrics_port = Some(args[i].parse().expect("--metrics-port must be a u16"));
                }
                "--kafka-brokers" => {
                    i += 1;
                    kafka_brokers =
                        Some(args[i].split(',').map(|s| s.trim().to_string()).collect());
                }
                "--kafka-topic-prefix" => {
                    i += 1;
                    kafka_topic_prefix = args[i].clone();
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
            listen_addr,
            peers,
            metrics_port,
            kafka_brokers,
            kafka_topic_prefix,
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
    // Fail fast when --kafka-brokers was passed to a binary without rdkafka.
    check_kafka_support(&cli)?;

    // The bind address comes from --listen-addr; the address advertised to
    // peers comes exclusively from the roster (--peers), so containers can
    // bind 0.0.0.0 while peers dial the roster hostname. With no --peers the
    // single-node roster stays on loopback regardless of --listen-addr.
    let bind_addr = format!("{}:{}", cli.listen_addr, cli.port);
    let self_advertise = format!("127.0.0.1:{}", cli.port);

    // ── Build shared config provider ──────────────────────────────────────────
    let config_provider = Arc::new(MemConfigProvider::new());

    // Parse the full cluster roster (`id@addr` pairs, including this node).
    let roster = parse_roster(&cli.peers, cli.node_id, &self_advertise)?;

    info!(
        node_id = cli.node_id,
        bind_addr = %bind_addr,
        advertise_addr = %roster[&cli.node_id],
        peers = ?cli.peers,
        bootstrap = cli.bootstrap,
        "=== rr KV-Store multi-node starting ==="
    );

    // Register every node so all members share the same routing table.
    for (id, addr) in &roster {
        config_provider
            .add_node(*id, rr_core::types::NodeAddr { addr: addr.clone() })
            .await;
    }

    // Register partition 1 → group 1 with the full replica set (sorted IDs).
    // Every node therefore starts group 1 during `RrNode::start`; non-bootstrap
    // groups stay uninitialized until the leader's AppendEntries arrive.
    let all_node_ids: Vec<NodeId> = roster.keys().copied().collect();

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
        bind_addr.clone(),
        storage,
        provider,
        dummy_factory,
    )
    .with_group_manager(Arc::clone(&mgr))
    .with_raft_lookup(lookup)
    .with_client_dispatch(dispatch)
    .with_admin_port(cli.port + 1000)
    .with_allow_insecure(true);
    let config = match cli.metrics_port {
        Some(port) => config.with_metrics_port(port),
        None => config,
    };

    let node = Arc::new(RrNode::new(config)?);
    node.start().await?;
    info!(
        "Node {} started — binding {bind_addr}, advertising {}",
        cli.node_id, roster[&cli.node_id]
    );
    if let Some(port) = cli.metrics_port {
        info!(
            "Node {} serving /health /ready /metrics on 0.0.0.0:{port}",
            cli.node_id
        );
    }

    // ── Bootstrap cluster membership ─────────────────────────────────────────
    if cli.bootstrap {
        // Initialize group 1 with the FULL voter set. openraft dials the other
        // voters via their `BasicNode.addr`; their groups are already started
        // (but uninitialized) by `RrNode::start`, so they join as soon as the
        // leader's AppendEntries arrive.
        let raft = node
            .group_manager()
            .get_raft(1)
            .await
            .ok_or_else(|| anyhow!("group 1 not started"))?;

        let members: BTreeMap<NodeId, BasicNode> = roster
            .iter()
            .map(|(id, addr)| (*id, BasicNode { addr: addr.clone() }))
            .collect();

        initialize_with_retry(&raft, members).await?;
        info!(
            "Cluster bootstrapped on node {} — voters: {:?}",
            cli.node_id, all_node_ids
        );
    }

    // ── Optional Kafka ingest (rdkafka builds only) ──────────────────────────
    // Group 1 exists by now: `RrNode::start` started it on every node and the
    // bootstrap node initialized membership. The framework supervisor runs the
    // consumer's poll loop only while this node leads the group, so attaching
    // on every node is safe.
    #[cfg(feature = "rdkafka")]
    attach_kafka_ingestor(&cli, &mgr).await?;

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

// ── Kafka ingest (optional; requires `--features rdkafka`) ───────────────────

/// Fail fast when `--kafka-brokers` was passed to a binary built without the
/// `rdkafka` feature — there is no Kafka backend to serve the request.
#[cfg(not(feature = "rdkafka"))]
fn check_kafka_support(cli: &CliConfig) -> Result<()> {
    if cli.kafka_brokers.is_some() {
        return Err(anyhow!(
            "--kafka-brokers requires Kafka support, but this binary was built \
             without it — rebuild with `cargo build -p kv-store --features rdkafka`"
        ));
    }
    // Only the rdkafka build reads this field; keep non-rdkafka builds
    // warning-free.
    let _ = &cli.kafka_topic_prefix;
    Ok(())
}

/// rdkafka builds accept `--kafka-brokers` unconditionally; validation happens
/// in [`KafkaIngestor::new`](rr_ingestor_kafka::KafkaIngestor).
#[cfg(feature = "rdkafka")]
fn check_kafka_support(_cli: &CliConfig) -> Result<()> {
    Ok(())
}

/// Wire the configured Kafka topic into Raft group 1 with framework-managed
/// exactly-once apply (`Kafka at-least-once + offset dedup`).
#[cfg(feature = "rdkafka")]
async fn attach_kafka_ingestor(cli: &CliConfig, mgr: &Arc<RaftGroupManager>) -> Result<()> {
    use rr_ingestor_kafka::{kafka_offset_to_request_id, KafkaIngestor, KafkaIngestorConfig};
    use rr_raft::ingestor_runner::{AutoIdempotency, OffsetMapError};

    const GROUP_ID: u64 = 1;

    let Some(brokers) = &cli.kafka_brokers else {
        return Ok(());
    };

    let cfg = KafkaIngestorConfig {
        bootstrap_servers: brokers.clone(),
        topic_pattern: format!("{}{{group_id}}", cli.kafka_topic_prefix),
        ..Default::default()
    };
    let topic = cfg.topic_for(GROUP_ID);

    let ingestor = KafkaIngestor::new(GROUP_ID, cfg)
        .await
        .map_err(|e| anyhow!("failed to create Kafka ingestor: {e}"))?;

    mgr.attach_ingestor_with_auto_idempotency(
        GROUP_ID,
        Box::new(ingestor),
        AutoIdempotency {
            request_type: topic.clone(),
            offset_to_request_id: Box::new(|b| {
                kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
            }),
        },
    )
    .await
    .map_err(|e| anyhow!("failed to attach Kafka ingestor to group {GROUP_ID}: {e}"))?;

    info!(
        brokers = ?brokers,
        topic = %topic,
        "Kafka ingest armed for group {GROUP_ID} (leader-exclusive consumption)"
    );
    Ok(())
}

// ── Cluster bootstrap helpers ─────────────────────────────────────────────────

/// Parse `--peers` entries of the form `id@addr` into a node-ID → addr roster.
///
/// The roster must cover the FULL cluster, including this node; an empty
/// `--peers` means a single-node roster. Bare `addr` entries (no `id@`) are
/// rejected — guessing node IDs from list order breaks as soon as the order
/// differs between terminals.
fn parse_roster(
    peers: &[String],
    self_id: NodeId,
    self_addr: &str,
) -> Result<BTreeMap<NodeId, String>> {
    let mut roster = BTreeMap::new();
    for entry in peers {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| anyhow!("invalid --peers entry '{entry}': expected <node-id>@<addr>"))?;
        let id: NodeId = id
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid --peers entry '{entry}': node id must be a u64"))?;
        let addr = addr.trim();
        if addr.is_empty() {
            return Err(anyhow!("invalid --peers entry '{entry}': empty address"));
        }
        if roster.insert(id, addr.to_string()).is_some() {
            return Err(anyhow!("invalid --peers: duplicate node id {id}"));
        }
    }

    if roster.is_empty() {
        // No --peers → single-node roster (e.g. `--bootstrap` alone).
        roster.insert(self_id, self_addr.to_string());
    } else if !roster.contains_key(&self_id) {
        return Err(anyhow!(
            "--peers roster must include this node ({self_id}@{self_addr})"
        ));
    }
    Ok(roster)
}

/// Initialize the Raft group, retrying until it succeeds or the deadline hits.
///
/// Users start their terminals sequentially, so transient failures while the
/// cluster comes up are retried every 500 ms for up to 30 s. A `NotAllowed`
/// rejection means the group is already initialized — treat it as success.
async fn initialize_with_retry(
    raft: &RaftInstance,
    members: BTreeMap<NodeId, BasicNode>,
) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match raft.initialize(members.clone()).await {
            Ok(()) => return Ok(()),
            Err(openraft::error::RaftError::APIError(
                openraft::error::InitializeError::NotAllowed(_),
            )) => {
                info!("Raft group 1 already initialized");
                return Ok(());
            }
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    return Err(anyhow!("raft initialize failed after retries: {e}"));
                }
                info!("raft initialize not ready yet ({e}); retrying in 500ms");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
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
