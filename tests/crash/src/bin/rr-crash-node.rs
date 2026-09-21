//! `rr-crash-node` — the node binary spawned by the crash-recovery harness.
//!
//! Assembled from the same production components as `rr-server`:
//!
//! - `RaftNode::new_with_rocks_factory` — RocksDB-backed Raft log
//!   (`RocksLogStorage` → `IoBatcher` group commit → `RocksStorage` with
//!   `DurabilityPolicy::Strict`, the INV-1 enforcement chain),
//! - `GrpcNetworkFactory` over the production `ChannelPool` for inter-node
//!   AppendEntries/Vote,
//! - production gRPC service impls (`RaftServiceImpl`, `ClientServiceImpl`)
//!   from `rr-network`,
//! - the production `ServiceStateMachineAdapter` (FramedEntry decode +
//!   idempotency layer + ApplyOutcome wrap) around the harness's service.
//!
//! Two deliberate differences from stock `rr-server` (see crate-level docs
//! for the full rationale):
//!
//! 1. The state machine is a **counting echo service** wrapped in the
//!    idempotency adapter, so keyed writes take the idempotency path. Stock
//!    `rr-server`'s raw `EchoStateMachine` bypasses the adapter and cannot
//!    serve keyed writes at all.
//! 2. Groups are started with rocks log storage directly on `RaftNode`,
//!    because `RrNode`/`RaftGroupManager` today only combine multi-node
//!    networking with in-memory log storage.
//!
//! Bootstrap uses a static roster (`--roster "1@addr,..."`): node 1 passes
//! `--init` and calls `raft.initialize` with the full voter set — the
//! config-roster equivalent of the rr-admin add-learner + change-membership
//! flow used by rr-perf-bench, without needing an admin service.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use clap::Parser;
use openraft::{BasicNode, Raft, ReadPolicy};
use rr_core::error::RrError;
use rr_core::framed_entry::FramedEntry;
use rr_core::idempotency::{ApplyOutcome, IdempotencyKey};
use rr_core::service::Service;
use rr_network::client_service::{
    ClientDispatch, ClientServiceImpl, ClientWriteResult, LinearizableReadResult,
};
use rr_network::proto::client::client_service_server::ClientServiceServer;
use rr_network::proto::client::GroupInfo;
use rr_network::proto::raft::raft_service_server::RaftServiceServer;
use rr_network::raft_service::{RaftGroupLookup, RaftServiceImpl};
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike, GrpcNetworkFactory};
use rr_raft::{RaftNode, RrStateMachine, RrTypeConfig, ServiceStateMachineAdapter};
use tracing::info;
use tracing_subscriber::EnvFilter;

/// The harness drives exactly one Raft group; `partition_id == group_id`.
const GROUP_ID: u64 = 1;

/// Openraft `LogsSinceLast` snapshot trigger. Deliberately the production
/// default: harness runs stay far below it, so crash recovery is exercised
/// through the pure log-replay path (the hotter, more fragile one).
const SNAPSHOT_LOGS_SINCE_LAST: u64 = 50_000;

/// Grace period after `raft.shutdown()` before process exit. The IoBatcher
/// flushes accepted appends every batching window (milliseconds); once gRPC
/// is down, in-flight writes have drained, and raft is shut down, no new
/// appends can arrive, so this short tail wait covers the final partial
/// batch (the explicit `IoBatcher::close()` handle is `pub(crate)` to
/// rr-raft and not reachable from a binary — see crate docs gap note).
const BATCHER_TAIL_GRACE: Duration = Duration::from_millis(300);

#[derive(Debug, Parser)]
#[command(
    name = "rr-crash-node",
    about = "crash-harness node (test infrastructure, not production)"
)]
struct Args {
    /// Node identifier (must match a roster entry).
    #[arg(long)]
    node_id: u64,
    /// gRPC bind address for client + raft services (loopback only).
    #[arg(long)]
    listen_addr: String,
    /// RocksDB data directory (reused across restarts).
    #[arg(long)]
    data_dir: PathBuf,
    /// Static cluster roster: "1@host:port,2@host:port,3@host:port".
    #[arg(long)]
    roster: String,
    /// Initialize the cluster with the full roster as voters (node 1 only;
    /// tolerated as a no-op after restart).
    #[arg(long)]
    init: bool,
    /// Log level filter (tracing EnvFilter syntax).
    #[arg(long, default_value = "info")]
    log_level: String,
    /// Openraft `LogsSinceLast` snapshot trigger. Defaults to the production
    /// value; scenario C lowers it so snapshot/purge cycles land inside the
    /// short harness window.
    #[arg(long, default_value_t = SNAPSHOT_LOGS_SINCE_LAST)]
    snapshot_logs_since_last: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing(&args.log_level);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;
    runtime.block_on(run(args))
}

fn init_tracing(filter: &str) {
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .try_init();
}

async fn run(args: Args) -> Result<()> {
    let listen_addr: SocketAddr = args
        .listen_addr
        .parse()
        .with_context(|| format!("invalid --listen-addr {}", args.listen_addr))?;
    if !listen_addr.ip().is_loopback() {
        // The harness never exercises TLS; binding off-loopback plaintext
        // raft/client services would be a security hole in a test tool.
        bail!("--listen-addr must be loopback, got {listen_addr}");
    }
    let roster = parse_roster(&args.roster)?;
    if !roster.contains_key(&args.node_id) {
        bail!("node {} is not in --roster", args.node_id);
    }

    // ── State machine: counting echo behind the production idempotency adapter
    let applied_count = Arc::new(Mutex::new(0u64));
    let service = CountService {
        applied: Arc::clone(&applied_count),
    };
    let state_machine = ServiceStateMachineAdapter::new(GROUP_ID, service);

    // ── Raft node: rocks log (strict durability) + production gRPC networking
    let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
    let network = GrpcNetworkFactory::new(pool, GROUP_ID);
    let rocks_path = args.data_dir.join(format!("group-{GROUP_ID}"));
    let rocks_path_str = rocks_path
        .to_str()
        .ok_or_else(|| anyhow!("data dir path is not valid UTF-8"))?;
    let node = RaftNode::new_with_rocks_factory(
        GROUP_ID,
        args.node_id,
        Box::new(state_machine),
        network,
        rocks_path_str,
        args.snapshot_logs_since_last,
    )
    .await
    .map_err(|e| anyhow!("raft node start failed: {e}"))?;
    let raft = node.raft.clone();

    if args.init {
        initialize_cluster(&raft, roster).await?;
    }

    // ── gRPC server (production service impls, harness dispatch/lookup glue)
    let in_flight = Arc::new(AtomicUsize::new(0));
    let dispatch = Arc::new(HarnessDispatch {
        raft: raft.clone(),
        applied_count,
        in_flight: Arc::clone(&in_flight),
    });
    let lookup = Arc::new(HarnessLookup { raft: raft.clone() });

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tonic::transport::Server::builder()
        .add_service(
            RaftServiceServer::new(RaftServiceImpl::new(lookup))
                .max_decoding_message_size(rr_network::RAFT_MAX_DECODING_MESSAGE_SIZE),
        )
        .add_service(ClientServiceServer::new(ClientServiceImpl::new(dispatch)))
        .serve_with_shutdown(listen_addr, async move {
            let _ = shutdown_rx.await;
        });
    let server_handle = tokio::spawn(server);

    info!(
        node_id = args.node_id,
        addr = %listen_addr,
        data_dir = %args.data_dir.display(),
        "rr-crash-node running"
    );

    wait_for_shutdown_signal().await?;
    info!(node_id = args.node_id, "shutdown signal received");

    // Graceful close protocol (mirrors RrNode::shutdown ordering):
    // 1. stop gRPC (no new work accepted; in-flight handlers drain),
    let _ = shutdown_tx.send(());
    match tokio::time::timeout(Duration::from_secs(5), server_handle).await {
        Ok(Ok(Ok(()))) => info!("gRPC server stopped cleanly"),
        Ok(Ok(Err(e))) => tracing::warn!(error = %e, "gRPC server error"),
        Ok(Err(e)) => tracing::warn!(error = %e, "gRPC server task join failed"),
        Err(_) => tracing::warn!("gRPC server shutdown timed out after 5s"),
    }
    // 2. wait for in-flight client writes to resolve (stop_group's
    //    in_flight_writes equivalent),
    let drain_deadline = Instant::now() + Duration::from_secs(5);
    while in_flight.load(Ordering::SeqCst) > 0 && Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // 3. shut raft down (openraft drains its core), then give the batcher's
    //    final partial batch a wide margin to hit the fsync'd write_batch.
    if let Err(e) = raft.shutdown().await {
        tracing::warn!(error = %e, "raft shutdown failed");
    }
    tokio::time::sleep(BATCHER_TAIL_GRACE).await;

    info!(node_id = args.node_id, "rr-crash-node shut down cleanly");
    Ok(())
}

/// `raft.initialize` with the full roster as the voter set. Only ever called
/// on the first boot of node 1 (the harness omits `--init` on restarts), so
/// an error here is a real failure, not something to tolerate.
async fn initialize_cluster(
    raft: &Raft<RrTypeConfig, RrStateMachine>,
    roster: BTreeMap<u64, BasicNode>,
) -> Result<()> {
    raft.initialize(roster)
        .await
        .map_err(|e| anyhow!("raft.initialize failed: {e}"))?;
    info!("cluster initialized with full roster");
    Ok(())
}

fn parse_roster(s: &str) -> Result<BTreeMap<u64, BasicNode>> {
    let mut map = BTreeMap::new();
    for entry in s.split(',') {
        let (id, addr) = entry
            .split_once('@')
            .with_context(|| format!("roster entry {entry:?} must be id@host:port"))?;
        let id: u64 = id
            .parse()
            .with_context(|| format!("bad roster id {id:?}"))?;
        map.insert(
            id,
            BasicNode {
                addr: addr.to_string(),
            },
        );
    }
    if map.is_empty() {
        bail!("--roster must not be empty");
    }
    Ok(map)
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

// ── Counting echo service ────────────────────────────────────────────────────

/// The harness's state-machine service: echoes the payload back (so clients
/// can verify response integrity) and counts unique applies (so the harness
/// can observe catch-up through the local read path).
///
/// The idempotency adapter only invokes `handle` on the `New` decision, so
/// `applied` counts **unique** keys — replays never double-count.
///
/// Safety invariants (see `rr_core::service::Service` docs): the entire
/// state is one `u64` behind one `std::sync::Mutex`; `snapshot` holds that
/// lock for the whole serialization (Invariant 1), `handle` completes its
/// mutation before returning (Invariant 2), and `restore` replaces the value
/// wholesale (Invariant 3).
#[derive(Clone)]
struct CountService {
    applied: Arc<Mutex<u64>>,
}

#[async_trait]
impl Service for CountService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        let mut count = self.applied.lock().unwrap();
        *count += 1;
        Ok(request)
    }

    fn name(&self) -> &str {
        "crash-count"
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let count = *self.applied.lock().unwrap();
        bincode::serde::encode_to_vec(count, bincode::config::standard())
            .map_err(|e| RrError::Application(format!("count snapshot encode: {e}")))
    }

    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        let (count, _): (u64, _) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| RrError::Application(format!("count snapshot decode: {e}")))?;
        *self.applied.lock().unwrap() = count;
        Ok(())
    }
}

// ── Client dispatch (thin mirror of rr-server's ClientDispatchImpl) ─────────

/// Tracks in-flight client writes so shutdown can drain them before stopping
/// raft — the same pattern as `RaftGroupManager`'s `InFlightWriteGuard`.
struct InFlightGuard(Arc<AtomicUsize>);

impl InFlightGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct HarnessDispatch {
    raft: Raft<RrTypeConfig, RrStateMachine>,
    applied_count: Arc<Mutex<u64>>,
    in_flight: Arc<AtomicUsize>,
}

#[async_trait]
impl ClientDispatch for HarnessDispatch {
    async fn submit_write(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
        idem_key: Option<IdempotencyKey>,
    ) -> Result<ClientWriteResult, tonic::Status> {
        if partition_id != GROUP_ID {
            return Err(tonic::Status::not_found(format!(
                "partition {partition_id} not found (harness serves group {GROUP_ID})"
            )));
        }
        let _guard = InFlightGuard::new(Arc::clone(&self.in_flight));

        // Keyed requests go through FramedEntry so the state-machine
        // adapter's idempotency layer acts on them — identical to the
        // production dispatch.
        let keyed = idem_key.is_some();
        let entry_bytes = if let Some(key) = idem_key {
            FramedEntry::from_payload(payload)
                .with_idem_key(key)
                .encode()
                .map_err(|e| {
                    tracing::error!(error = %e, "framed entry encode failed");
                    tonic::Status::internal("internal encode error")
                })?
        } else {
            payload
        };

        match self.raft.client_write(entry_bytes.into()).await {
            Ok(resp) => {
                if keyed {
                    match ApplyOutcome::decode(resp.data.as_ref()) {
                        Ok(ApplyOutcome::Success(bytes)) => Ok(ClientWriteResult {
                            success: true,
                            payload: bytes,
                            leader_node_id: None,
                            leader_addr: None,
                            idem_error: None,
                        }),
                        Ok(ApplyOutcome::IdempotencyExpired(err)) => Ok(ClientWriteResult {
                            success: false,
                            payload: vec![],
                            leader_node_id: None,
                            leader_addr: None,
                            idem_error: Some(err),
                        }),
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "apply outcome decode failed (keyed request)"
                            );
                            Err(tonic::Status::internal("internal decode error"))
                        }
                    }
                } else {
                    Ok(ClientWriteResult {
                        success: true,
                        payload: resp.data.into(),
                        leader_node_id: None,
                        leader_addr: None,
                        idem_error: None,
                    })
                }
            }
            Err(ref e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    Ok(ClientWriteResult {
                        success: false,
                        payload: vec![],
                        leader_node_id: fwd.leader_id,
                        leader_addr: fwd.leader_node.as_ref().map(|n| n.addr.clone()),
                        idem_error: None,
                    })
                } else {
                    tracing::error!(error = %e, "client_write failed (detail sanitized)");
                    Err(tonic::Status::internal("client write failed"))
                }
            }
        }
    }

    async fn submit_read(
        &self,
        partition_id: u64,
        _payload: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status> {
        if partition_id != GROUP_ID {
            return Err(tonic::Status::not_found(format!(
                "partition {partition_id} not found"
            )));
        }
        let count = *self.applied_count.lock().unwrap();
        Ok(count.to_string().into_bytes())
    }

    async fn submit_linearizable_read(
        &self,
        partition_id: u64,
        _payload: Vec<u8>,
    ) -> Result<LinearizableReadResult, tonic::Status> {
        if partition_id != GROUP_ID {
            return Err(tonic::Status::not_found(format!(
                "partition {partition_id} not found"
            )));
        }
        // Mirrors RaftGroupManager::linearizable_read: ReadIndex round-trip,
        // await readiness, then serve locally; followers get a redirect.
        let linearizer = match self.raft.get_read_linearizer(ReadPolicy::ReadIndex).await {
            Ok(l) => l,
            Err(ref e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    return Ok(LinearizableReadResult {
                        payload: vec![],
                        leader_node_id: fwd.leader_id,
                        leader_addr: fwd.leader_node.as_ref().map(|n| n.addr.clone()),
                    });
                }
                return Err(tonic::Status::internal("linearizable read failed"));
            }
        };
        linearizer
            .await_ready(&self.raft)
            .await
            .map_err(|_| tonic::Status::internal("linearizable read not ready"))?;
        let count = *self.applied_count.lock().unwrap();
        Ok(LinearizableReadResult {
            payload: count.to_string().into_bytes(),
            leader_node_id: None,
            leader_addr: None,
        })
    }

    async fn get_cluster_info(&self) -> Result<Vec<GroupInfo>, tonic::Status> {
        Ok(vec![GroupInfo {
            group_id: GROUP_ID,
            // Placeholder, mirrors production: leader state lives in the
            // raft runtime, not in config-plane responses (ADR A-0013).
            leader_node_id: 0,
            replica_node_ids: vec![],
        }])
    }
}

// ── Raft lookup (thin single-group mirror of rr-server's RaftGroupLookupImpl)

struct HarnessLookup {
    raft: Raft<RrTypeConfig, RrStateMachine>,
}

#[async_trait]
impl RaftGroupLookup for HarnessLookup {
    async fn append_entries(
        &self,
        group_id: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status> {
        if group_id != GROUP_ID {
            return Err(tonic::Status::not_found(format!(
                "group {group_id} not found"
            )));
        }
        let req: openraft::raft::AppendEntriesRequest<RrTypeConfig> =
            bincode::serde::decode_from_slice(&payload, bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|_| tonic::Status::invalid_argument("invalid request payload"))?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|_| tonic::Status::internal("raft rpc failed"))?;
        bincode::serde::encode_to_vec(&resp, bincode::config::standard())
            .map_err(|_| tonic::Status::internal("internal encode error"))
    }

    async fn vote(&self, group_id: u64, payload: Vec<u8>) -> Result<Vec<u8>, tonic::Status> {
        if group_id != GROUP_ID {
            return Err(tonic::Status::not_found(format!(
                "group {group_id} not found"
            )));
        }
        let req: openraft::raft::VoteRequest<RrTypeConfig> =
            bincode::serde::decode_from_slice(&payload, bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|_| tonic::Status::invalid_argument("invalid request payload"))?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|_| tonic::Status::internal("raft rpc failed"))?;
        bincode::serde::encode_to_vec(&resp, bincode::config::standard())
            .map_err(|_| tonic::Status::internal("internal encode error"))
    }

    async fn install_snapshot(
        &self,
        group_id: u64,
        metadata: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status> {
        if group_id != GROUP_ID {
            return Err(tonic::Status::not_found(format!(
                "group {group_id} not found"
            )));
        }
        let (vote, meta): (
            openraft::type_config::alias::VoteOf<RrTypeConfig>,
            openraft::type_config::alias::SnapshotMetaOf<RrTypeConfig>,
        ) = bincode::serde::decode_from_slice(&metadata, bincode::config::standard())
            .map(|(v, _)| v)
            .map_err(|_| tonic::Status::invalid_argument("invalid request payload"))?;
        let snapshot = openraft::Snapshot {
            meta,
            snapshot: std::io::Cursor::new(data),
        };
        let resp = self
            .raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|_| tonic::Status::internal("raft rpc failed"))?;
        bincode::serde::encode_to_vec(&resp, bincode::config::standard())
            .map_err(|_| tonic::Status::internal("internal encode error"))
    }
}
