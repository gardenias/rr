//! Counter service demo — single-node and multi-node `rr` distributed framework example.
//!
//! This example demonstrates the **distributed counter** pattern: named counters
//! that support atomic `Increment`, `Get`, and `Reset` operations, all replicated
//! through Raft consensus.  It is intentionally simpler than the KV store example
//! to serve as a minimal starting point for new `rr` services.
//!
//! # Usage
//!
//! ## Single-node (demo mode, no gRPC server needed):
//! ```text
//! cargo run -p counter
//! ```
//!
//! ## Multi-node cluster — every node gets the FULL roster as `id@addr` pairs:
//! ```text
//! cargo run -p counter -- --node-id 1 --port 52001 --bootstrap --peers 1@127.0.0.1:52001,2@127.0.0.1:52002,3@127.0.0.1:52003
//! cargo run -p counter -- --node-id 2 --port 52002 --peers 1@127.0.0.1:52001,2@127.0.0.1:52002,3@127.0.0.1:52003
//! cargo run -p counter -- --node-id 3 --port 52003 --peers 1@127.0.0.1:52001,2@127.0.0.1:52002,3@127.0.0.1:52003
//! ```
//!
//! ## Arguments:
//! - `--node-id <N>`         — Node ID (u64). Defaults to 1 for single-node demo.
//! - `--port <PORT>`         — gRPC listen port. Defaults to 52001.
//!                             The admin API is served on PORT + 1000 (52001 → 53001).
//! - `--peers <ID@ADDR,...>` — Full cluster roster, comma-separated `id@addr` pairs
//!                             (must include this node). Empty means single-node roster.
//! - `--bootstrap`           — Initialize Raft membership (run on exactly one node).
//! - `--demo`                — Run counter demo and exit (default when no args given).
//!
//! # Architecture
//!
//! Each node is an `RrNode` with:
//! - `MemStorage` for counter data (no RocksDB in this example)
//! - `CounterService` wrapped in `ServiceStateMachineAdapter` as the state machine
//! - `RaftGroupLookupImpl` for intra-cluster Raft RPCs
//! - `ClientDispatchImpl` for client write routing
//! - `ShardRouter` for partition-to-group mapping

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bincode::config::standard;
use clap::Parser;
use openraft::BasicNode;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::error::{CodecError, RrError};
use rr_core::service::Service;
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
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, Level};
use tracing_subscriber::FmtSubscriber;

// ── Request / Response types ────────────────────────────────────────────────

/// A counter operation proposed through Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CounterRequest {
    /// Atomically add `delta` to the named counter (creates it if absent).
    Increment { key: String, delta: i64 },
    /// Read the current value of a counter.
    Get { key: String },
    /// Reset a counter to zero (removes it).
    Reset { key: String },
}

/// The result of a counter operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CounterResponse {
    /// Current value after the operation, or `None` if the counter does not exist.
    pub value: Option<i64>,
}

impl CounterResponse {
    /// Construct a response with no value.
    fn none() -> Self {
        Self { value: None }
    }

    /// Construct a response carrying a value.
    fn some(value: i64) -> Self {
        Self { value: Some(value) }
    }
}

// ── CounterService ──────────────────────────────────────────────────────────

/// In-memory distributed counter service implementing [`Service`].
///
/// State is shared via `Arc<RwLock<...>>` so that snapshots can read while
/// writes are serialized through Raft.
#[derive(Clone)]
pub struct CounterService {
    counters: Arc<RwLock<HashMap<String, i64>>>,
}

impl CounterService {
    /// Create a new `CounterService` with no counters.
    pub fn new() -> Self {
        Self {
            counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for CounterService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for CounterService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        let (req, _): (CounterRequest, _) = bincode::serde::decode_from_slice(&request, standard())
            .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;

        let response = match req {
            CounterRequest::Increment { key, delta } => {
                debug!(%key, delta, "Counter Increment");
                let mut guard = self.counters.write().await;
                let entry = guard.entry(key).or_insert(0);
                *entry += delta;
                CounterResponse::some(*entry)
            }
            CounterRequest::Get { key } => {
                debug!(%key, "Counter Get");
                let guard = self.counters.read().await;
                match guard.get(&key) {
                    Some(&v) => CounterResponse::some(v),
                    None => CounterResponse::none(),
                }
            }
            CounterRequest::Reset { key } => {
                debug!(%key, "Counter Reset");
                self.counters.write().await.remove(&key);
                CounterResponse::none()
            }
        };

        bincode::serde::encode_to_vec(&response, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    fn name(&self) -> &str {
        "counter"
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let guard = self.counters.read().await;
        bincode::serde::encode_to_vec(&*guard, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        let (restored, _): (HashMap<String, i64>, _) =
            bincode::serde::decode_from_slice(data, standard())
                .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;
        *self.counters.write().await = restored;
        Ok(())
    }
}

// ── CLI argument parsing ────────────────────────────────────────────────────

/// Distributed counter service built on the `rr` multi-Raft framework.
#[derive(Parser)]
#[command(name = "counter", about = "Distributed counter service on rr")]
struct CliConfig {
    /// Node ID (u64).
    #[arg(long, default_value_t = 1)]
    node_id: NodeId,

    /// gRPC listen port.
    #[arg(long, default_value_t = 52001)]
    port: u16,

    /// Full cluster roster as comma-separated `id@addr` pairs (must include
    /// this node). Empty means single-node roster.
    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,

    /// Bootstrap as cluster leader (only on first node).
    #[arg(long)]
    bootstrap: bool,

    /// Run counter demo and exit (default when no args given).
    #[arg(long)]
    demo: bool,
}

// ── Entry point ─────────────────────────────────────────────────────────────

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

// ── Demo mode (single-node, no gRPC) ───────────────────────────────────────

async fn run_demo() -> Result<()> {
    info!("=== rr Counter demo (single-node, no gRPC) ===");

    // Partition 1  →  group 1  →  replica [node 1]
    let config_provider = MemConfigProvider::new();
    config_provider
        .add_partition(1, PartitionConfig::SingleId(1), 1, vec![1])
        .await;
    let config_provider: Arc<dyn ConfigProvider> = Arc::new(config_provider);

    let storage_engine: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());

    let sm_factory = Box::new(|group_id| -> Box<dyn StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(
            group_id,
            CounterService::new(),
        ))
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
    demo_increment(&raft, "hits", 1).await?;
    demo_increment(&raft, "hits", 5).await?;
    demo_get(&raft, "hits").await?; // should be 6
    demo_increment(&raft, "errors", 1).await?;
    demo_reset(&raft, "hits").await?;
    demo_get(&raft, "hits").await?; // should be None after reset
    demo_get(&raft, "errors").await?; // should be 1

    node.shutdown().await?;
    info!("=== rr Counter demo complete ===");
    Ok(())
}

// ── Multi-node mode (with gRPC server) ─────────────────────────────────────

async fn run_node(cli: CliConfig) -> Result<()> {
    let listen_addr = format!("127.0.0.1:{}", cli.port);
    info!(
        node_id = cli.node_id,
        addr = %listen_addr,
        peers = ?cli.peers,
        bootstrap = cli.bootstrap,
        "=== rr Counter multi-node starting ==="
    );

    // ── Build shared config provider ────────────────────────────────────────
    let config_provider = Arc::new(MemConfigProvider::new());

    // Parse the full cluster roster (`id@addr` pairs, including this node).
    let roster = parse_roster(&cli.peers, cli.node_id, &listen_addr)?;

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

    // ── Build RaftGroupManager with real gRPC network ───────────────────────
    let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
    let sm_factory = Box::new(|group_id| -> Box<dyn StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(
            group_id,
            CounterService::new(),
        ))
    });

    let mgr = Arc::new(RaftGroupManager::new_with_network(
        cli.node_id,
        sm_factory,
        pool.clone(),
    ));

    // ── Wire lookup and dispatch ────────────────────────────────────────────
    let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
    let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
    let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));

    // ── Build and start RrNode ──────────────────────────────────────────────
    let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
    let dummy_factory = Box::new(|group_id| -> Box<dyn StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(
            group_id,
            CounterService::new(),
        ))
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
    .with_admin_port(cli.port + 1000)
    .with_allow_insecure(true);

    let node = Arc::new(RrNode::new(config)?);
    node.start().await?;
    info!("Node {} started — listening on {listen_addr}", cli.node_id);

    // ── Bootstrap cluster membership ───────────────────────────────────────
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

    // ── Keep node running until Ctrl-C ──────────────────────────────────────
    info!("Node {} running. Press Ctrl-C to shut down.", cli.node_id);
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| anyhow!("failed to listen for Ctrl-C: {e}"))?;

    info!("Shutting down node {}...", cli.node_id);
    node.shutdown().await?;
    info!("Node {} shut down cleanly.", cli.node_id);
    Ok(())
}

// ── Cluster bootstrap helpers ─────────────────────────────────────────────

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
        let (id, addr) = entry.split_once('@').ok_or_else(|| {
            anyhow!("invalid --peers entry '{entry}': expected <node-id>@<addr>")
        })?;
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
async fn initialize_with_retry(raft: &RaftInstance, members: BTreeMap<NodeId, BasicNode>) -> Result<()> {
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

// ── Demo helpers ────────────────────────────────────────────────────────────

type RaftInstance =
    openraft::Raft<rr_raft::type_config::RrTypeConfig, rr_raft::sm_adapter::RrStateMachine>;

async fn propose(raft: &RaftInstance, req: &CounterRequest) -> Result<CounterResponse> {
    let payload = bincode::serde::encode_to_vec(req, standard())
        .map_err(|e| anyhow!("encode failed: {e}"))?;

    let response = raft
        .client_write(payload.into())
        .await
        .map_err(|e| anyhow!("client_write failed: {e}"))?;

    let (resp, _): (CounterResponse, _) =
        bincode::serde::decode_from_slice(response.data.as_ref(), standard())
            .map_err(|e| anyhow!("decode failed: {e}"))?;

    Ok(resp)
}

async fn demo_increment(raft: &RaftInstance, key: &str, delta: i64) -> Result<()> {
    let req = CounterRequest::Increment {
        key: key.to_string(),
        delta,
    };
    let resp = propose(raft, &req).await?;
    info!("INCREMENT {key} += {delta} → {:?}", resp.value);
    Ok(())
}

async fn demo_get(raft: &RaftInstance, key: &str) -> Result<()> {
    let req = CounterRequest::Get {
        key: key.to_string(),
    };
    let resp = propose(raft, &req).await?;
    match resp.value {
        Some(v) => info!("GET {key} = {v}"),
        None => info!("GET {key} = <not found>"),
    }
    Ok(())
}

async fn demo_reset(raft: &RaftInstance, key: &str) -> Result<()> {
    let req = CounterRequest::Reset {
        key: key.to_string(),
    };
    propose(raft, &req).await?;
    info!("RESET {key}");
    Ok(())
}

// ── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_req(req: CounterRequest) -> Vec<u8> {
        bincode::serde::encode_to_vec(&req, standard()).expect("encode CounterRequest")
    }

    fn decode_resp(bytes: Vec<u8>) -> CounterResponse {
        let (r, _): (CounterResponse, _) =
            bincode::serde::decode_from_slice(&bytes, standard()).expect("decode CounterResponse");
        r
    }

    #[tokio::test]
    async fn test_increment_creates_counter() {
        let svc = CounterService::new();
        let req = encode_req(CounterRequest::Increment {
            key: "hits".into(),
            delta: 1,
        });
        let resp = decode_resp(svc.handle(req).await.expect("handle Increment"));
        assert_eq!(resp.value, Some(1));
    }

    #[tokio::test]
    async fn test_increment_accumulates() {
        let svc = CounterService::new();

        let req1 = encode_req(CounterRequest::Increment {
            key: "hits".into(),
            delta: 3,
        });
        svc.handle(req1).await.expect("handle Increment 1");

        let req2 = encode_req(CounterRequest::Increment {
            key: "hits".into(),
            delta: 7,
        });
        let resp = decode_resp(svc.handle(req2).await.expect("handle Increment 2"));
        assert_eq!(resp.value, Some(10));
    }

    #[tokio::test]
    async fn test_increment_negative_delta() {
        let svc = CounterService::new();

        let req1 = encode_req(CounterRequest::Increment {
            key: "x".into(),
            delta: 5,
        });
        svc.handle(req1).await.expect("handle Increment +5");

        let req2 = encode_req(CounterRequest::Increment {
            key: "x".into(),
            delta: -3,
        });
        let resp = decode_resp(svc.handle(req2).await.expect("handle Increment -3"));
        assert_eq!(resp.value, Some(2));
    }

    #[tokio::test]
    async fn test_get_existing_counter() {
        let svc = CounterService::new();

        let inc = encode_req(CounterRequest::Increment {
            key: "c".into(),
            delta: 42,
        });
        svc.handle(inc).await.expect("handle Increment");

        let get = encode_req(CounterRequest::Get { key: "c".into() });
        let resp = decode_resp(svc.handle(get).await.expect("handle Get"));
        assert_eq!(resp.value, Some(42));
    }

    #[tokio::test]
    async fn test_get_missing_counter_returns_none() {
        let svc = CounterService::new();
        let get = encode_req(CounterRequest::Get {
            key: "missing".into(),
        });
        let resp = decode_resp(svc.handle(get).await.expect("handle Get"));
        assert_eq!(resp.value, None);
    }

    #[tokio::test]
    async fn test_reset_removes_counter() {
        let svc = CounterService::new();

        let inc = encode_req(CounterRequest::Increment {
            key: "r".into(),
            delta: 10,
        });
        svc.handle(inc).await.expect("handle Increment");

        let reset = encode_req(CounterRequest::Reset { key: "r".into() });
        let resp = decode_resp(svc.handle(reset).await.expect("handle Reset"));
        assert_eq!(resp.value, None);

        let get = encode_req(CounterRequest::Get { key: "r".into() });
        let resp = decode_resp(svc.handle(get).await.expect("handle Get after Reset"));
        assert_eq!(resp.value, None);
    }

    #[tokio::test]
    async fn test_snapshot_and_restore() {
        let svc = CounterService::new();

        let inc = encode_req(CounterRequest::Increment {
            key: "snap".into(),
            delta: 99,
        });
        svc.handle(inc).await.expect("handle Increment");

        let snapshot = svc.snapshot().await.expect("snapshot");

        let mut svc2 = CounterService::new();
        svc2.restore(&snapshot).await.expect("restore");

        let get = encode_req(CounterRequest::Get { key: "snap".into() });
        let resp = decode_resp(svc2.handle(get).await.expect("handle Get after restore"));
        assert_eq!(resp.value, Some(99));
    }

    #[test]
    fn test_service_name() {
        let svc = CounterService::new();
        assert_eq!(svc.name(), "counter");
    }

    #[tokio::test]
    async fn test_multiple_counters_are_independent() {
        let svc = CounterService::new();

        let inc_a = encode_req(CounterRequest::Increment {
            key: "a".into(),
            delta: 1,
        });
        let inc_b = encode_req(CounterRequest::Increment {
            key: "b".into(),
            delta: 100,
        });
        svc.handle(inc_a).await.expect("handle Increment a");
        svc.handle(inc_b).await.expect("handle Increment b");

        let get_a = encode_req(CounterRequest::Get { key: "a".into() });
        let get_b = encode_req(CounterRequest::Get { key: "b".into() });
        let resp_a = decode_resp(svc.handle(get_a).await.expect("handle Get a"));
        let resp_b = decode_resp(svc.handle(get_b).await.expect("handle Get b"));

        assert_eq!(resp_a.value, Some(1));
        assert_eq!(resp_b.value, Some(100));
    }
}
