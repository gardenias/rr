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
//! ## Managed Genesis — no manual bootstrap; every node uses the same seed roster:
//! ```text
//! kv-store --node-id 1 --port 51001 --formation genesis --cluster-name example \
//!     --etcd-endpoints http://127.0.0.1:2379 \
//!     --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003
//! ```
//!
//! # Bootstrap rules
//!
//! - The no-argument demo self-initializes; a persistent manual single-node service bootstraps once.
//! - Manual multi-node startup uses one identical full roster everywhere and bootstraps exactly one
//!   fresh node, which initializes every voter; all other nodes omit `--bootstrap`.
//! - Genesis and Adopt omit `--bootstrap`. Normal restarts and restarts/recovery of already initialized
//!   nodes, including durable ones, also omit it; fresh durable initial formation may bootstrap.
//! - An already-initialized rejection treated as success is not a restart or membership-change strategy.
//! - The gRPC Pre-Vote failure was not caused by `--bootstrap`; never use it to repair election or
//!   network-isolation failures.
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
//! ## Secure production shape (TLS + mTLS + bearer tokens — perf-lab):
//! ```text
//! RR_ADMIN_TOKEN=<secret> RR_CLIENT_TOKEN=<secret> \
//! kv-store --node-id 1 --listen-addr 0.0.0.0 --port 51001 --metrics-port 9100 \
//!     --bootstrap --peers 1@kv-1:51001,2@kv-2:51001,3@kv-3:51001 \
//!     --data-dir /var/lib/rr/data \
//!     --tls-cert /etc/rr/certs/node1.pem --tls-key /etc/rr/certs/node1.key \
//!     --tls-ca /etc/rr/certs/ca.pem
//! ```
//! Secure mode is all-or-nothing: all three `--tls-cert/--tls-key/--tls-ca`
//! flags plus both token env vars are required together, and the node then
//! runs the full `RrNode` G1–G7 security gates (NO `allow_insecure` opt-out):
//! TLS on every gRPC endpoint, mandatory client certificates (mTLS is the
//! only RaftService peer-identity check), and enforced admin/client bearer
//! tokens. With no `--tls-*` flags the historical plaintext dev mode stays.
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
//! - `--metrics-bind-host <IP>` — Bind host for the metrics/HTTP endpoints.
//!   Defaults to 127.0.0.1 (loopback only, mirroring rr-server's
//!   `metrics_bind_host` default). Set 0.0.0.0 in containers so published
//!   ports and cross-container Prometheus scrapes can reach it.
//! - `--kafka-brokers <A,...>`  — Comma-separated bootstrap brokers; consumes group 1's
//!   command topic with framework-managed exactly-once apply.
//!   Requires a binary built with `--features rdkafka`.
//! - `--kafka-topic-prefix <P>` — Topic prefix; group 1 consumes `{P}1`. Default `input_`.
//! - `--data-dir <PATH>`        — Durable mode (ADR A-0016): the Raft log is persisted
//!   to `{PATH}/raft-log/{group_id}`; managed formation also persists framework identity
//!   under `{PATH}/framework`. Requires `--features rocks`. Unset = in-memory log/metadata.
//! - `--formation <MODE>`       — Managed formation: `genesis` or `adopt` (legacy adoption).
//!   Requires `--cluster-name` and `--etcd-endpoints`; cannot be combined with `--bootstrap`.
//! - `--cluster-name <NAME>`    — Managed cluster identity used for the Etcd lifecycle key.
//! - `--etcd-endpoints <A,...>` — Comma-separated Etcd endpoints for managed formation.
//! - `--tls-cert <PATH>`        — Server identity PEM; also presented as the client
//!   identity when dialing peers. Requires `--tls-key` and `--tls-ca` (all-or-nothing).
//! - `--tls-key <PATH>`         — Private key matching `--tls-cert`.
//! - `--tls-ca <PATH>`          — CA bundle verifying peers (outbound) and client
//!   certificates (inbound mTLS — always required in TLS mode; RaftService has no
//!   bearer-token path, so mTLS is the only peer-identity check).
//! - `--tls-domain <NAME>`      — Optional TLS server-name (SNI) override used when
//!   dialing peers; defaults to the host part of each peer's roster address.
//! - `--snapshot-logs-since-last <N>` — Snapshot trigger threshold override
//!   (default 50_000, ADR A-0008). W7 snapshot-pressure runs lower it (e.g.
//!   5_000) so snapshot cost shows up in latency time series.
//! - `RR_ADMIN_TOKEN` / `RR_CLIENT_TOKEN` (env) — Bearer tokens enforced on the
//!   Admin/Client services. REQUIRED in TLS mode (secrets come from the
//!   environment, never argv — same convention as rr-server's
//!   `auth.*_token_env`); ignored in plaintext dev mode.
//! - `--bootstrap`              — Manual-only Raft initialization (run on exactly one legacy node).
//! - `--demo`                   — Run KV demo writes and exit (default when no args given).
//!
//! Invalid arguments (unknown flags, missing or malformed values) print a
//! usage message and exit with code 2. `--port` must be ≤ 64535 so the
//! admin port (PORT + 1000) fits a u16.
//!
//! # Architecture
//!
//! Each node is an `RrNode` with:
//! - In-memory business KV state; with `--data-dir`, RocksDB persists the Raft log
//!   and (in managed mode only) framework identity metadata, never the business KV map
//! - `KvService` wrapped in `ServiceStateMachineAdapter` as the state machine
//! - `RaftGroupLookupImpl` for intra-cluster Raft RPCs
//! - `ClientDispatchImpl` for client write routing
//! - `ShardRouter` for partition-to-group mapping

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use openraft::BasicNode;
use rr_config::mem::MemConfigProvider;
use rr_config_etcd::EtcdLifecycleStore;
use rr_core::config::ConfigProvider;
use rr_core::state_machine::StateMachine;
use rr_core::storage::StorageEngine;
use rr_core::types::{NodeAddr, NodeId, PartitionConfig};
use rr_core::{LifecycleStore, SeedSpec};
use rr_network::auth::AuthConfig;
use rr_network::tls::TlsConfig;
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike, TlsPaths};
use rr_raft::service_adapter::ServiceStateMachineAdapter;
use rr_router::router::ShardRouter;
use rr_server::config::NodeConfig;
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::formation::{FormationConfig, FormationMode};
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;
use tracing::{info, warn, Level};
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
    metrics_bind_host: Option<String>,
    kafka_brokers: Option<Vec<String>>,
    kafka_topic_prefix: String,
    data_dir: Option<String>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_ca: Option<String>,
    tls_domain: Option<String>,
    snapshot_logs_since_last: Option<u64>,
    formation: Option<FormationMode>,
    cluster_name: Option<String>,
    etcd_endpoints: Vec<String>,
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
                metrics_bind_host: None,
                kafka_brokers: None,
                kafka_topic_prefix: "input_".to_string(),
                data_dir: None,
                tls_cert: None,
                tls_key: None,
                tls_ca: None,
                tls_domain: None,
                snapshot_logs_since_last: None,
                formation: None,
                cluster_name: None,
                etcd_endpoints: vec![],
                bootstrap: false,
                demo: true,
            };
        }

        let mut node_id: NodeId = 1;
        let mut port: u16 = 51001;
        let mut listen_addr = "127.0.0.1".to_string();
        let mut peers: Vec<String> = vec![];
        let mut metrics_port: Option<u16> = None;
        let mut metrics_bind_host: Option<String> = None;
        let mut kafka_brokers: Option<Vec<String>> = None;
        let mut kafka_topic_prefix = "input_".to_string();
        let mut data_dir: Option<String> = None;
        let mut tls_cert: Option<String> = None;
        let mut tls_key: Option<String> = None;
        let mut tls_ca: Option<String> = None;
        let mut tls_domain: Option<String> = None;
        let mut snapshot_logs_since_last: Option<u64> = None;
        let mut formation = None;
        let mut cluster_name = None;
        let mut etcd_endpoints = vec![];
        let mut bootstrap = false;
        let mut demo = false;
        let mut i = 0;

        while i < args.len() {
            match args[i].as_str() {
                "--node-id" => {
                    i += 1;
                    node_id = parse_flag_value(&args, i, "--node-id");
                }
                "--port" => {
                    i += 1;
                    port = parse_flag_value(&args, i, "--port");
                }
                "--listen-addr" => {
                    i += 1;
                    listen_addr = flag_value(&args, i, "--listen-addr").to_string();
                }
                "--peers" => {
                    i += 1;
                    peers = flag_value(&args, i, "--peers")
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect();
                }
                "--metrics-port" => {
                    i += 1;
                    metrics_port = Some(parse_flag_value(&args, i, "--metrics-port"));
                }
                "--metrics-bind-host" => {
                    i += 1;
                    metrics_bind_host =
                        Some(flag_value(&args, i, "--metrics-bind-host").to_string());
                }
                "--kafka-brokers" => {
                    i += 1;
                    kafka_brokers = Some(
                        flag_value(&args, i, "--kafka-brokers")
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .collect(),
                    );
                }
                "--kafka-topic-prefix" => {
                    i += 1;
                    kafka_topic_prefix = flag_value(&args, i, "--kafka-topic-prefix").to_string();
                }
                "--data-dir" => {
                    i += 1;
                    data_dir = Some(flag_value(&args, i, "--data-dir").to_string());
                }
                "--tls-cert" => {
                    i += 1;
                    tls_cert = Some(flag_value(&args, i, "--tls-cert").to_string());
                }
                "--tls-key" => {
                    i += 1;
                    tls_key = Some(flag_value(&args, i, "--tls-key").to_string());
                }
                "--tls-ca" => {
                    i += 1;
                    tls_ca = Some(flag_value(&args, i, "--tls-ca").to_string());
                }
                "--tls-domain" => {
                    i += 1;
                    tls_domain = Some(flag_value(&args, i, "--tls-domain").to_string());
                }
                "--snapshot-logs-since-last" => {
                    i += 1;
                    snapshot_logs_since_last =
                        Some(parse_flag_value(&args, i, "--snapshot-logs-since-last"));
                }
                "--formation" => {
                    i += 1;
                    formation = Some(match flag_value(&args, i, "--formation") {
                        "genesis" => FormationMode::Genesis,
                        "adopt" => FormationMode::AdoptLegacy,
                        other => {
                            eprintln!(
                                "error: invalid --formation '{other}': expected 'genesis' or 'adopt'"
                            );
                            usage();
                        }
                    });
                }
                "--cluster-name" => {
                    i += 1;
                    cluster_name = Some(flag_value(&args, i, "--cluster-name").to_string());
                }
                "--etcd-endpoints" => {
                    i += 1;
                    etcd_endpoints = flag_value(&args, i, "--etcd-endpoints")
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                }
                "--bootstrap" => bootstrap = true,
                "--demo" => demo = true,
                other => {
                    eprintln!("error: unknown argument '{other}'");
                    usage();
                }
            }
            i += 1;
        }

        // The admin API is served on PORT + 1000; reject ports where that
        // would overflow u16 instead of wrapping/panicking deep in startup.
        if port > 64535 {
            eprintln!(
                "error: --port {port} is too large: the admin port is PORT + 1000 \
                 and must fit a u16 (max --port 64535)"
            );
            usage();
        }

        Self {
            node_id,
            port,
            listen_addr,
            peers,
            metrics_port,
            metrics_bind_host,
            kafka_brokers,
            kafka_topic_prefix,
            data_dir,
            tls_cert,
            tls_key,
            tls_ca,
            tls_domain,
            snapshot_logs_since_last,
            formation,
            cluster_name,
            etcd_endpoints,
            bootstrap,
            demo,
        }
    }
}

/// Print usage and exit with the conventional CLI misuse code (2).
fn usage() -> ! {
    eprintln!(
        "Usage: kv-store [--node-id N] [--port PORT] [--listen-addr HOST] \
         [--peers ID@ADDR,...] [--metrics-port PORT] [--metrics-bind-host IP] \
         [--kafka-brokers A,...] [--kafka-topic-prefix P] [--data-dir PATH] \
         [--tls-cert PATH --tls-key PATH --tls-ca PATH [--tls-domain NAME]] \
         [--snapshot-logs-since-last N] [--formation genesis|adopt \
         --cluster-name NAME --etcd-endpoints URL,...] [--bootstrap] [--demo]"
    );
    std::process::exit(2);
}

/// Return the value following the flag at position `i`, or exit(2) with
/// usage when the flag is the last argument (missing value).
fn flag_value<'a>(args: &'a [String], i: usize, flag: &str) -> &'a str {
    match args.get(i) {
        Some(v) => v,
        None => {
            eprintln!("error: {flag} requires a value");
            usage();
        }
    }
}

/// Parse a numeric flag value, exiting with usage on malformed input
/// (instead of panicking).
fn parse_flag_value<T: std::str::FromStr>(args: &[String], i: usize, flag: &str) -> T {
    let v = flag_value(args, i, flag);
    v.parse().unwrap_or_else(|_| {
        eprintln!(
            "error: {flag} expects {}, got '{v}'",
            std::any::type_name::<T>()
        );
        usage();
    })
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // rustls (tonic TLS on the server side and in the inter-node ChannelPool)
    // needs a process-global crypto provider; install ring's. Idempotent —
    // the error only fires when a provider is already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

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
    // Fail fast when --data-dir was passed to a binary without RocksDB.
    check_raft_log_support(&cli)?;
    // Resolve the TLS/mTLS/token surface (all-or-nothing; see doc header).
    // `None` keeps the historical plaintext dev mode with allow_insecure.
    let secure = resolve_secure_config(&cli)?;

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

    let formation = match cli.formation {
        Some(mode) => {
            if cli.bootstrap {
                return Err(anyhow!(
                    "managed formation and manual --bootstrap are mutually exclusive"
                ));
            }
            let cluster_name = cli
                .cluster_name
                .clone()
                .ok_or_else(|| anyhow!("managed formation requires --cluster-name"))?;
            if cli.etcd_endpoints.is_empty() {
                return Err(anyhow!("managed formation requires --etcd-endpoints"));
            }
            let nodes = roster
                .iter()
                .map(|(id, addr)| (*id, NodeAddr { addr: addr.clone() }))
                .collect();
            let seed = SeedSpec::stock(cluster_name.clone(), nodes, 1)?;
            let store: Arc<dyn LifecycleStore> =
                Arc::new(EtcdLifecycleStore::new(cli.etcd_endpoints.clone(), cluster_name).await?);
            Some(FormationConfig { seed, store, mode })
        }
        None => {
            if cli.cluster_name.is_some() || !cli.etcd_endpoints.is_empty() {
                return Err(anyhow!(
                    "--cluster-name and --etcd-endpoints require --formation"
                ));
            }
            None
        }
    };

    info!(
        node_id = cli.node_id,
        bind_addr = %bind_addr,
        advertise_addr = %roster[&cli.node_id],
        peers = ?cli.peers,
        bootstrap = cli.bootstrap,
        tls = secure.is_some(),
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
    // Secure mode: every inter-node channel (AppendEntries/Vote/snapshot) runs
    // over mTLS — this node presents its own cert as the client identity.
    let pool: Arc<dyn ChannelPoolLike> = match &secure {
        Some(s) => Arc::new(ChannelPool::with_tls(s.pool_tls.clone())),
        None => Arc::new(ChannelPool::new()),
    };
    let sm_factory = Box::new(|group_id| -> Box<dyn StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(group_id, KvService::new()))
    });

    let mgr = RaftGroupManager::new_with_network(cli.node_id, sm_factory, pool.clone());
    // ADR A-0016: `--data-dir` turns on the durable Raft log at
    // `{data-dir}/raft-log/{group_id}` (rocks builds only). Unset = in-memory
    // log, the historical behavior of this example.
    #[cfg(feature = "rocks")]
    let mgr = match &cli.data_dir {
        Some(dir) => mgr.with_rocks_base_path(format!("{dir}/raft-log")),
        None => mgr,
    };
    let mgr = Arc::new(mgr);

    // ── Wire lookup and dispatch ──────────────────────────────────────────────
    let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
    let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
    let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));

    // ── Build and start RrNode ────────────────────────────────────────────────
    let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
    #[cfg(feature = "rocks")]
    let storage: Arc<dyn StorageEngine> = match &cli.data_dir {
        Some(dir) if cli.formation.is_some() => Arc::new(
            rr_storage::rocks::RocksStorage::open_default(&format!("{dir}/framework"))?,
        ),
        _ => storage,
    };
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
    .with_admin_port(cli.port + 1000);
    // Secure mode runs the full G1–G7 gate set (no opt-out); plaintext mode
    // keeps the historical dev-only `allow_insecure` bypass.
    let config = match &secure {
        Some(s) => config.with_tls(s.tls.clone()).with_auth(s.auth.clone()),
        None => {
            warn!("plaintext dev mode: no TLS/auth — running with allow_insecure (never use outside local dev)");
            config.with_allow_insecure(true)
        }
    };
    let config = match cli.metrics_port {
        Some(port) => config.with_metrics_port(port),
        None => config,
    };
    // Container runs need the metrics/HTTP server on 0.0.0.0: the compose
    // healthcheck passes either way (it probes loopback from inside), but
    // published ports and cross-container scrapes only reach a wildcard
    // bind. rr-server's default is loopback — kv-store must opt out
    // explicitly for the lab stacks.
    let config = match &cli.metrics_bind_host {
        Some(host) => config.with_metrics_bind_host(host.clone()),
        None => config,
    };
    // W7 snapshot-pressure runs lower the trigger (e.g. 5k) to make snapshot
    // cost visible in the time series; default keeps 50k (ADR A-0008).
    let config = match cli.snapshot_logs_since_last {
        Some(n) => config.with_snapshot_logs_since_last(n),
        None => config,
    };

    let node = match formation {
        Some(formation) => Arc::new(RrNode::new_with_formation(config, formation)?),
        None => Arc::new(RrNode::new(config)?),
    };
    node.start().await?;
    info!(
        "Node {} started — binding {bind_addr}, advertising {}",
        cli.node_id, roster[&cli.node_id]
    );
    if let Some(port) = cli.metrics_port {
        let host = cli.metrics_bind_host.as_deref().unwrap_or("127.0.0.1");
        info!(
            "Node {} serving /health /ready /metrics on {host}:{port}",
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

/// Fail fast when `--data-dir` was passed to a binary built without the
/// `rocks` feature — durable Raft logs and managed identity are unavailable.
#[cfg(not(feature = "rocks"))]
fn check_raft_log_support(cli: &CliConfig) -> Result<()> {
    if cli.data_dir.is_some() {
        return Err(anyhow!(
            "--data-dir requires RocksDB support, but this binary was built \
             without it — rebuild with `cargo build -p kv-store --features rocks`"
        ));
    }
    Ok(())
}

/// rocks builds accept `--data-dir` unconditionally; the directory is created
/// on demand when the first group starts.
#[cfg(feature = "rocks")]
fn check_raft_log_support(_cli: &CliConfig) -> Result<()> {
    Ok(())
}

// ── Secure mode (TLS + mTLS + bearer tokens) ─────────────────────────────────

/// Fully-resolved secure-mode configuration: server-side TLS for every gRPC
/// endpoint, client-side TLS for the inter-node channel pool, and the
/// admin/client bearer-token policy.
struct SecureConfig {
    tls: TlsConfig,
    pool_tls: TlsPaths,
    auth: AuthConfig,
}

/// Resolve the `--tls-*` flags and token env vars into a [`SecureConfig`].
///
/// Secure mode is all-or-nothing: passing any of `--tls-cert/--tls-key/
/// --tls-ca` requires all three, plus non-empty `RR_ADMIN_TOKEN` and
/// `RR_CLIENT_TOKEN` env vars (secrets never travel on argv — same convention
/// as rr-server's `auth.*_token_env`). With no TLS flags at all, returns
/// `None` and the node keeps its historical plaintext dev mode
/// (`allow_insecure = true`).
///
/// mTLS is not optional in secure mode: RaftService has no bearer-token path,
/// so the client certificate is the ONLY peer-identity check (gate G2) —
/// there is deliberately no `--tls-require-client-cert` opt-out flag.
fn resolve_secure_config(cli: &CliConfig) -> Result<Option<SecureConfig>> {
    let tls_flags = [&cli.tls_cert, &cli.tls_key, &cli.tls_ca];
    let given = tls_flags.iter().filter(|f| f.is_some()).count();
    if given == 0 {
        if cli.tls_domain.is_some() {
            return Err(anyhow!(
                "--tls-domain only makes sense with --tls-cert/--tls-key/--tls-ca"
            ));
        }
        return Ok(None);
    }

    let (Some(cert), Some(key), Some(ca)) = (&cli.tls_cert, &cli.tls_key, &cli.tls_ca) else {
        return Err(anyhow!(
            "TLS mode is all-or-nothing: --tls-cert, --tls-key and --tls-ca must be \
             given together (plus RR_ADMIN_TOKEN / RR_CLIENT_TOKEN in the environment)"
        ));
    };

    let admin_token = read_token_env("RR_ADMIN_TOKEN", "admin")?;
    let client_token = read_token_env("RR_CLIENT_TOKEN", "client")?;

    Ok(Some(SecureConfig {
        tls: TlsConfig {
            cert_path: PathBuf::from(cert),
            key_path: PathBuf::from(key),
            ca_path: Some(PathBuf::from(ca)),
            require_client_cert: true,
            // Server-side configs ignore domain_name; peer dialing uses the
            // roster host unless --tls-domain overrides it (pool_tls below).
            domain_name: None,
        },
        pool_tls: TlsPaths {
            cert_path: PathBuf::from(cert),
            key_path: PathBuf::from(key),
            ca_path: Some(PathBuf::from(ca)),
            domain_name: cli.tls_domain.clone(),
        },
        auth: AuthConfig {
            admin_token: Some(admin_token),
            client_token: Some(client_token),
        },
    }))
}

/// Read a non-empty bearer token from `var_name`; secure mode fails fast
/// otherwise (mirrors `rr-server` config-file semantics).
fn read_token_env(var_name: &str, role: &str) -> Result<String> {
    match std::env::var(var_name) {
        Ok(v) if !v.is_empty() => Ok(v),
        Ok(_) => Err(anyhow!(
            "{role} token env var '{var_name}' is set but empty — \
             set {var_name}=<non-empty-secret>, or drop the --tls-* flags for dev mode"
        )),
        Err(_) => Err(anyhow!(
            "TLS mode requires the {role} bearer token in environment variable \
             '{var_name}' (e.g. `{var_name}=<secret> kv-store --tls-cert … --tls-key … \
             --tls-ca …`), or drop the --tls-* flags for plaintext dev mode"
        )),
    }
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
