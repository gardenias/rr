//! Leader-exclusive lifecycle for [`DynCommandIngestor`] instances.
//!
//! [ADR A-0001](../../../.sisyphus/adr/A-0001-command-ingestor-abstraction.md)
//! mandates: only the current leader of a Raft group reads from its
//! upstream command source; followers consume via Raft log replication.
//! [`spawn`] enforces that contract by watching the group's openraft
//! metrics channel and starting/stopping the inner poll loop on every
//! leadership transition; the returned [`IngestorHandle`] is how the
//! framework cancels the supervisor on shutdown.
//!
//! # State machine
//!
//! ```text
//!  ┌────────────┐  becomes Leader   ┌──────────────────┐
//!  │ Following  │ ─────────────────▶│ Seeking + Polling│
//!  │  (parked)  │ ◀─────────────────│ (poll → write)   │
//!  └────────────┘  steps down        └──────────────────┘
//! ```
//!
//! # Backpressure
//!
//! The poll loop calls [`crate::group_manager::RaftGroupManager::write_batch`]
//! once per fetched ingestor batch. This reduces per-entry Raft round
//! trips while preserving per-entry offset commit semantics after apply.
//! Backpressure still comes from Raft commit/apply throughput.
//!
//! # Failure handling
//!
//! Errors from `ingestor.poll_bytes` trigger exponential backoff
//! (capped at [`MAX_POLL_BACKOFF`]). The supervisor never steps the
//! Raft leader down on ingestor failure — a Kafka outage must not
//! destabilise consensus (ADR A-0001 §"Open questions" #5).
//!
//! Errors from `mgr.write_batch` (e.g. the leader stepped down between
//! poll and write) cause the poll loop to exit cleanly; the
//! supervisor parks waiting for the next leadership transition.
//!
//! Offset→request-id mapping failures are **fail-closed** (ADR A-0011
//! C7): the offending message is neither proposed nor committed and the
//! supervisor task exits with an error (same path as a `seed_offset`
//! failure), leaving the poison message for an operator to handle.

use std::sync::Arc;
use std::time::Duration;

use openraft::type_config::async_runtime::WatchReceiver;
use openraft::ServerState;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use rr_core::error::RrError;
use rr_core::framed_entry::FramedEntry;
use rr_core::framed_snapshot::FramedSnapshot;
use rr_core::idempotency::IdempotencyKey;
use rr_core::ingestor::DynCommandIngestor;
use rr_core::types::GroupId;

use crate::group_manager::RaftGroupManager;
use crate::type_config::RrTypeConfig;

type RrRaftMetrics = openraft::RaftMetrics<RrTypeConfig>;
type RrMetricsRx = openraft::type_config::alias::WatchReceiverOf<RrTypeConfig, RrRaftMetrics>;

/// Error returned by an [`OffsetToRequestId`] mapping when the offset
/// bytes cannot be converted into a monotone `u64` request id.
///
/// A mapping failure is fail-closed (ADR A-0011 C7): the ingestor
/// supervisor exits with an error instead of proposing a command whose
/// request id regressed below the idempotency watermark or collided with
/// another message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetMapError(String);

impl OffsetMapError {
    /// Create a mapping error from any displayable diagnostic (e.g. the
    /// backend-specific error of `kafka_offset_to_request_id`).
    pub fn new(message: impl std::fmt::Display) -> Self {
        Self(message.to_string())
    }
}

impl std::fmt::Display for OffsetMapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OffsetMapError {}

/// Closure that converts an opaque ingestor offset (bincode bytes) to
/// a monotonic `u64` for use as an [`IdempotencyKey::request_id`].
/// The framework requires monotonicity: hash-derived IDs would let new
/// offsets fall below the watermark and be wrongly rejected as
/// `Expired`. The mapping MUST NOT silently degrade unparseable input
/// (e.g. by returning `0`): that would regress the watermark or collide
/// with a legitimate first message. Return [`OffsetMapError`] instead and
/// the supervisor exits fail-closed without proposing or committing the
/// offending message. For Kafka, see
/// `rr_ingestor_kafka::kafka_offset_to_request_id`.
pub type OffsetToRequestId = Box<dyn Fn(&[u8]) -> Result<u64, OffsetMapError> + Send + Sync>;

/// Auto-idempotency configuration for an ingestor attached via
/// [`RaftGroupManager::attach_ingestor_with_auto_idempotency`].
///
/// When set, the runner derives an [`IdempotencyKey`] for every
/// command pulled from the ingestor and the framework's idempotency
/// layer dedups re-fetched messages on leader transitions or
/// retry-after-step-down. Without this, replayed Kafka messages would
/// re-execute `Service::handle()` on the new leader.
pub struct AutoIdempotency {
    /// Stable namespace identifying this ingestor's request stream.
    /// Recommended: the topic name (e.g. `"input_42"`). All commands
    /// pulled from one ingestor instance MUST share one
    /// `request_type` because the watermark is per-type.
    pub request_type: String,
    /// Maps the bincode offset bytes to a `u64`. MUST be monotone in
    /// offset order (hash-based mappings break the watermark). On
    /// unparseable or sentinel offsets it MUST return [`OffsetMapError`]
    /// — the runner then exits fail-closed (no propose, no offset
    /// commit) rather than guessing an id (ADR A-0011 C7).
    pub offset_to_request_id: OffsetToRequestId,
}

/// Default number of commands fetched per `ingestor.poll_bytes` call.
/// Matches ADR A-0001 §"Open questions" #2 default.
pub const DEFAULT_POLL_BATCH_SIZE: usize = 100;

/// Initial backoff after the first ingestor error.
pub const INITIAL_POLL_BACKOFF: Duration = Duration::from_millis(100);

/// Maximum backoff between failed poll attempts. Capped per ADR A-0001
/// §"Open questions" #5 so a permanently-down Kafka still gets retried
/// roughly twice a minute.
pub const MAX_POLL_BACKOFF: Duration = Duration::from_secs(30);

/// Returned by [`IngestorSupervisor::detach`] to allow the caller to
/// `await` the supervisor task to completion. Useful in tests; in
/// production the framework just drops it.
pub struct IngestorHandle {
    join: JoinHandle<()>,
    cancel: watch::Sender<bool>,
}

impl IngestorHandle {
    /// Signal the supervisor to stop and wait for it to exit.
    pub async fn stop(self) {
        let _ = self.cancel.send(true);
        let _ = self.join.await;
    }

    /// Whether the supervisor task is still running.
    pub fn is_running(&self) -> bool {
        !self.join.is_finished()
    }
}

/// Spawn a supervisor task that runs `ingestor` whenever this node is
/// the leader of `group_id`. The supervisor exits when the cancellation
/// flag is set or when `mgr` is dropped.
///
/// `auto_idem`, if `Some`, derives a per-entry [`IdempotencyKey`] from
/// each ingestor offset so that re-fetched commands (after step-down,
/// stale-snapshot seek, or transient errors) are deduped by the
/// framework's idempotency layer instead of double-applied. See
/// [`AutoIdempotency`] for the contract on `request_type` and
/// `offset_to_request_id`.
///
/// Returns immediately; the actual ingestor work runs in the background.
pub fn spawn(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    ingestor: Box<dyn DynCommandIngestor>,
    auto_idem: Option<AutoIdempotency>,
) -> IngestorHandle {
    spawn_with_batch_size(mgr, group_id, ingestor, auto_idem, DEFAULT_POLL_BATCH_SIZE)
}

/// Variant of [`spawn`] that accepts an explicit batch size. Tests pin
/// it to small values (e.g. 1) to make assertions simpler.
pub fn spawn_with_batch_size(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    ingestor: Box<dyn DynCommandIngestor>,
    auto_idem: Option<AutoIdempotency>,
    batch_size: usize,
) -> IngestorHandle {
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let join = tokio::spawn(async move {
        if let Err(e) = supervise(mgr, group_id, ingestor, auto_idem, batch_size, cancel_rx).await {
            warn!(group_id, error = %e, "ingestor supervisor exited with error");
        }
    });
    IngestorHandle {
        join,
        cancel: cancel_tx,
    }
}

async fn supervise(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    mut ingestor: Box<dyn DynCommandIngestor>,
    auto_idem: Option<AutoIdempotency>,
    batch_size: usize,
    mut cancel_rx: watch::Receiver<bool>,
) -> Result<(), RrError> {
    let raft = mgr
        .get_raft(group_id)
        .await
        .ok_or_else(|| RrError::Application(format!("ingestor: group {group_id} not started")))?;

    let mut metrics_rx = raft.metrics();
    info!(group_id, "ingestor supervisor started");

    loop {
        if *cancel_rx.borrow() {
            debug!(group_id, "ingestor supervisor cancelled while parked");
            return Ok(());
        }

        if !is_leader(&metrics_rx) {
            tokio::select! {
                _ = cancel_rx.changed() => {
                    if *cancel_rx.borrow() {
                        debug!(group_id, "ingestor supervisor cancelled while waiting for leadership");
                        return Ok(());
                    }
                }
                changed = metrics_rx.changed() => {
                    if changed.is_err() {
                        debug!(group_id, "raft metrics channel closed; supervisor exiting");
                        return Ok(());
                    }
                }
            }
            continue;
        }

        info!(group_id, "ingestor became leader; seeking + polling");
        if let Err(e) = seed_offset(&mgr, group_id, ingestor.as_mut()).await {
            metrics::counter!(
                "rr_ingestor_seed_failures_total",
                "group_id" => group_id.to_string()
            )
            .increment(1);
            return Err(RrError::Application(format!(
                "ingestor seed_offset failed: {e}; supervisor exiting to prevent silent \
                 replay-from-earliest under auto.offset.reset"
            )));
        }

        run_poll_loop(
            &mgr,
            group_id,
            ingestor.as_mut(),
            auto_idem.as_ref(),
            batch_size,
            &mut metrics_rx,
            &mut cancel_rx,
        )
        .await?;

        if *cancel_rx.borrow() {
            debug!(group_id, "ingestor supervisor cancelled after step-down");
            return Ok(());
        }
        info!(group_id, "ingestor lost leadership; parking");
    }
}

async fn seed_offset(
    mgr: &RaftGroupManager,
    group_id: GroupId,
    ingestor: &mut dyn DynCommandIngestor,
) -> Result<(), RrError> {
    refresh_snapshot_if_stale(mgr, group_id).await;
    let snapshot_bytes = mgr.fetch_snapshot_bytes(group_id).await?;
    let offset = match snapshot_bytes
        .as_deref()
        .map(FramedSnapshot::decode)
        .transpose()?
        .flatten()
    {
        Some(framed) => framed
            .framework_state
            .ingestor
            .and_then(|i| i.last_applied_offset),
        None => None,
    };
    metrics::counter!(
        "rr_ingestor_seek_total",
        "group_id" => group_id.to_string()
    )
    .increment(1);
    ingestor.seek_bytes(offset.as_deref()).await
}

/// If the latest snapshot lags `last_applied`, trigger a fresh
/// snapshot and wait up to [`SNAPSHOT_REFRESH_DEADLINE`] for it to
/// catch up. Closes the P0-1 perf gap from the Wave 4d Oracle review:
/// without this, a newly-promoted leader whose snapshot lags applied
/// state by N entries re-fetches those N Kafka messages (auto-idem
/// dedups them at apply, so it's only wasted I/O, but on large lags
/// this is meaningful).
///
/// Errors are logged at debug and the supervisor proceeds with
/// whatever snapshot already exists — non-blocking so a transient
/// broker hiccup doesn't stall the leader's recovery.
async fn refresh_snapshot_if_stale(mgr: &RaftGroupManager, group_id: GroupId) {
    let Some(raft) = mgr.get_raft(group_id).await else {
        return;
    };
    let metrics = raft.metrics().borrow_watched().clone();
    let target_index = match metrics.last_applied.as_ref().map(|l| l.index) {
        Some(0) | None => return,
        Some(idx) => idx,
    };
    let current_snap_index = metrics.snapshot.as_ref().map(|l| l.index).unwrap_or(0);
    if current_snap_index >= target_index {
        return;
    }
    debug!(
        group_id,
        target_index,
        current_snap_index,
        lag = target_index - current_snap_index,
        "snapshot lags last_applied; triggering refresh before ingestor seek"
    );
    if let Err(e) = raft.trigger().snapshot().await {
        debug!(
            group_id,
            error = %e,
            "trigger snapshot failed; falling back to existing snapshot bytes"
        );
        return;
    }
    let deadline = std::time::Instant::now() + SNAPSHOT_REFRESH_DEADLINE;
    while std::time::Instant::now() < deadline {
        let curr = raft
            .metrics()
            .borrow_watched()
            .snapshot
            .as_ref()
            .map(|l| l.index)
            .unwrap_or(0);
        if curr >= target_index {
            metrics::counter!(
                "rr_ingestor_seed_snapshot_refreshes_total",
                "group_id" => group_id.to_string()
            )
            .increment(1);
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    warn!(
        group_id,
        target_index,
        "snapshot refresh did not catch up within deadline; \
         seeding from stale snapshot — auto-idempotency will dedup any duplicate fetches"
    );
}

/// How long [`refresh_snapshot_if_stale`] waits for a triggered
/// snapshot to materialise before giving up. Chosen to bound the
/// leader-promotion latency: 30s is generous enough for snapshot
/// generation on a typical state machine yet won't deadlock a
/// node that has a deeper problem.
const SNAPSHOT_REFRESH_DEADLINE: Duration = Duration::from_secs(30);

async fn run_poll_loop(
    mgr: &RaftGroupManager,
    group_id: GroupId,
    ingestor: &mut dyn DynCommandIngestor,
    auto_idem: Option<&AutoIdempotency>,
    batch_size: usize,
    metrics_rx: &mut RrMetricsRx,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<(), RrError> {
    let mut backoff = INITIAL_POLL_BACKOFF;
    loop {
        if *cancel_rx.borrow() || !is_leader(metrics_rx) {
            return Ok(());
        }

        let poll_started = std::time::Instant::now();
        let batch_result = tokio::select! {
            _ = cancel_rx.changed() => return Ok(()),
            _ = metrics_rx.changed() => continue,
            r = ingestor.poll_bytes(batch_size) => r,
        };
        metrics::histogram!(
            "rr_ingestor_poll_duration_seconds",
            "group_id" => group_id.to_string()
        )
        .record(poll_started.elapsed().as_secs_f64());

        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    group_id,
                    error = %e,
                    backoff_ms = backoff.as_millis() as u64,
                    "ingestor poll failed; backing off"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_POLL_BACKOFF);
                continue;
            }
        };
        backoff = INITIAL_POLL_BACKOFF;

        if batch.is_empty() {
            tokio::select! {
                _ = cancel_rx.changed() => return Ok(()),
                _ = metrics_rx.changed() => {
                    if !is_leader(metrics_rx) { return Ok(()); }
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
            continue;
        }

        let mut payloads = Vec::with_capacity(batch.len());
        let mut commit_offsets = Vec::with_capacity(batch.len());

        for cmd in batch {
            let mut entry = FramedEntry::from_payload(cmd.payload).with_offset(cmd.offset.clone());
            if let Some(cfg) = auto_idem {
                let request_id = match (cfg.offset_to_request_id)(&cmd.offset) {
                    Ok(id) => id,
                    Err(e) => {
                        // Fail-closed (ADR A-0011 C7): a message whose offset
                        // cannot be mapped is never proposed and its offset is
                        // never committed; the supervisor exits like it does
                        // on seed_offset failure, so a poison message cannot
                        // hot-loop and is left for an operator to handle.
                        tracing::error!(
                            group_id,
                            error = %e,
                            "ingestor: offset→request_id mapping failed; supervisor exiting \
                             fail-closed without proposing or committing this message"
                        );
                        metrics::counter!(
                            "rr_ingestor_offset_map_failures_total",
                            "group_id" => group_id.to_string()
                        )
                        .increment(1);
                        return Err(RrError::Application(format!(
                            "ingestor offset→request_id mapping failed: {e}"
                        )));
                    }
                };
                entry =
                    entry.with_idem_key(IdempotencyKey::new(cfg.request_type.clone(), request_id));
            }

            let bytes = match entry.encode() {
                Ok(b) => b,
                Err(e) => {
                    warn!(group_id, error = %e, "ingestor: framed entry encode failed; skipping batch");
                    break;
                }
            };

            payloads.push(bytes);
            commit_offsets.push(cmd.offset);
        }

        if payloads.is_empty() {
            continue;
        }

        match mgr.write_batch(group_id, payloads).await {
            Ok(responses) => {
                if responses.len() != commit_offsets.len() {
                    let dropped = commit_offsets.len() as u64;
                    metrics::counter!(
                        "rr_ingestor_step_down_drops_total",
                        "group_id" => group_id.to_string()
                    )
                    .increment(dropped);
                    warn!(
                        group_id,
                        dropped,
                        expected = commit_offsets.len(),
                        got = responses.len(),
                        "ingestor: write_batch response count mismatch; treating as step-down"
                    );
                    return Ok(());
                }

                for offset in commit_offsets {
                    if let Err(e) = ingestor.commit_applied_bytes(&offset).await {
                        debug!(
                            group_id,
                            error = %e,
                            "ingestor: commit_applied failed (non-fatal — Raft log is the \
                             authoritative offset; this only affects upstream observability)"
                        );
                    }
                }
            }
            Err(e) => {
                let dropped = commit_offsets.len() as u64;
                metrics::counter!(
                    "rr_ingestor_step_down_drops_total",
                    "group_id" => group_id.to_string()
                )
                .increment(dropped);
                debug!(
                    group_id,
                    error = %e,
                    dropped,
                    "ingestor: batch write failed; likely stepped down"
                );
                return Ok(());
            }
        }
    }
}

fn is_leader(metrics_rx: &RrMetricsRx) -> bool {
    matches!(metrics_rx.borrow_watched().state, ServerState::Leader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rr_core::ingestor::{CommandIngestor, IngressCommand};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use crate::group_manager::{RaftGroupManager, StateMachineFactory};
    use openraft::BasicNode;

    struct ScriptedIngestor {
        next_offset: AtomicU64,
        cap: u64,
        seek_calls: Mutex<Vec<Option<u64>>>,
        poll_called: Arc<AtomicBool>,
    }

    impl ScriptedIngestor {
        fn new(cap: u64) -> (Self, Arc<AtomicBool>) {
            let poll_called = Arc::new(AtomicBool::new(false));
            (
                Self {
                    next_offset: AtomicU64::new(0),
                    cap,
                    seek_calls: Mutex::new(Vec::new()),
                    poll_called: Arc::clone(&poll_called),
                },
                poll_called,
            )
        }
    }

    #[async_trait]
    impl CommandIngestor for ScriptedIngestor {
        type Offset = u64;

        async fn seek(&mut self, offset: Option<u64>) -> Result<(), RrError> {
            self.seek_calls.lock().unwrap().push(offset);
            if let Some(o) = offset {
                self.next_offset.store(o + 1, Ordering::SeqCst);
            }
            Ok(())
        }

        async fn poll(&mut self, _max: usize) -> Result<Vec<IngressCommand<u64>>, RrError> {
            self.poll_called.store(true, Ordering::SeqCst);
            let off = self.next_offset.fetch_add(1, Ordering::SeqCst);
            if off >= self.cap {
                std::future::pending::<()>().await;
                unreachable!()
            }
            Ok(vec![IngressCommand {
                offset: off,
                payload: format!("cmd-{off}").into_bytes(),
            }])
        }

        async fn commit_applied(&mut self, _offset: u64) -> Result<(), RrError> {
            Ok(())
        }
    }

    struct AlwaysErrIngestor {
        poll_attempts: Arc<AtomicU64>,
    }

    #[async_trait]
    impl CommandIngestor for AlwaysErrIngestor {
        type Offset = u64;

        async fn seek(&mut self, _offset: Option<u64>) -> Result<(), RrError> {
            Ok(())
        }

        async fn poll(&mut self, _max: usize) -> Result<Vec<IngressCommand<u64>>, RrError> {
            self.poll_attempts.fetch_add(1, Ordering::SeqCst);
            Err(RrError::Application("simulated kafka outage".to_string()))
        }

        async fn commit_applied(&mut self, _offset: u64) -> Result<(), RrError> {
            Ok(())
        }
    }

    fn echo_factory() -> StateMachineFactory {
        Box::new(|_gid| {
            Box::new(crate::service_adapter::ServiceStateMachineAdapter::new(
                _gid,
                EchoService,
            ))
        })
    }

    struct EchoService;

    #[async_trait]
    impl rr_core::service::Service for EchoService {
        async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
            Ok(request)
        }

        fn name(&self) -> &str {
            "echo"
        }

        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }

        async fn restore(&mut self, _data: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
    }

    async fn make_single_node_leader() -> Arc<RaftGroupManager> {
        let mgr = Arc::new(RaftGroupManager::new(1, echo_factory()));
        mgr.start_group(1).await.unwrap();
        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            BasicNode {
                addr: String::new(),
            },
        );
        mgr.init_cluster(1, members).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let raft = mgr.get_raft(1).await.unwrap();
        assert!(
            raft.metrics().borrow_watched().current_leader.is_some(),
            "expected leader after init"
        );
        mgr
    }

    #[tokio::test]
    async fn supervisor_calls_seek_and_polls_when_leader() {
        let mgr = make_single_node_leader().await;
        let (ingestor, poll_called) = ScriptedIngestor::new(3);
        let handle = spawn_with_batch_size(Arc::clone(&mgr), 1, Box::new(ingestor), None, 1);

        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !poll_called.load(Ordering::SeqCst) {
            if std::time::Instant::now() > deadline {
                panic!("supervisor never polled within 3s");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        handle.stop().await;
    }

    #[tokio::test]
    async fn supervisor_stops_on_explicit_cancel() {
        let mgr = make_single_node_leader().await;
        let (ingestor, _poll_called) = ScriptedIngestor::new(1000);
        let handle = spawn_with_batch_size(Arc::clone(&mgr), 1, Box::new(ingestor), None, 1);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(handle.is_running());
        handle.stop().await;
    }

    #[tokio::test]
    async fn supervisor_backs_off_on_repeated_poll_errors() {
        let mgr = make_single_node_leader().await;
        let attempts = Arc::new(AtomicU64::new(0));
        let ingestor = AlwaysErrIngestor {
            poll_attempts: Arc::clone(&attempts),
        };
        let handle = spawn_with_batch_size(Arc::clone(&mgr), 1, Box::new(ingestor), None, 1);

        tokio::time::sleep(Duration::from_millis(500)).await;
        let n = attempts.load(Ordering::SeqCst);
        assert!(
            (1..=10).contains(&n),
            "expected a small number of retries under backoff in 500ms, got {n}"
        );
        handle.stop().await;
    }

    #[tokio::test]
    async fn supervisor_exits_when_group_unknown() {
        let mgr = Arc::new(RaftGroupManager::new(1, echo_factory()));
        let (ingestor, _poll_called) = ScriptedIngestor::new(1);
        let handle = spawn_with_batch_size(Arc::clone(&mgr), 999, Box::new(ingestor), None, 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !handle.is_running(),
            "supervisor must exit promptly when group is missing"
        );
        handle.stop().await;
    }

    struct SeedFailIngestor;

    #[async_trait]
    impl CommandIngestor for SeedFailIngestor {
        type Offset = u64;

        async fn seek(&mut self, _offset: Option<u64>) -> Result<(), RrError> {
            Err(RrError::Application("seed failure".to_string()))
        }

        async fn poll(&mut self, _max: usize) -> Result<Vec<IngressCommand<u64>>, RrError> {
            panic!("poll must not be called when seek fails — supervisor must abort first")
        }

        async fn commit_applied(&mut self, _offset: u64) -> Result<(), RrError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn supervisor_exits_when_seed_offset_fails() {
        let mgr = make_single_node_leader().await;
        let handle =
            spawn_with_batch_size(Arc::clone(&mgr), 1, Box::new(SeedFailIngestor), None, 1);
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while handle.is_running() {
            if std::time::Instant::now() > deadline {
                panic!("supervisor must exit on seed_offset failure (P0-2)");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn auto_idempotency_attaches_request_id_via_extractor() {
        let auto_idem = AutoIdempotency {
            request_type: "test_topic".into(),
            offset_to_request_id: Box::new(|bytes| {
                let (i, _): (u64, _) =
                    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                        .map_err(OffsetMapError::new)?;
                Ok(i)
            }),
        };
        assert_eq!(auto_idem.request_type, "test_topic");
        let mut buf = Vec::new();
        bincode::serde::encode_into_std_write(42_u64, &mut buf, bincode::config::standard())
            .unwrap();
        assert_eq!((auto_idem.offset_to_request_id)(&buf).unwrap(), 42);
        // Empty bytes cannot decode as a bincode u64 → Err.
        assert!((auto_idem.offset_to_request_id)(b"").is_err());
    }

    /// Ingestor whose offsets always fail the `offset_to_request_id`
    /// mapping — the supervisor must exit fail-closed (ADR A-0011 C7).
    struct UnmappableOffsetIngestor {
        commits: Arc<AtomicU64>,
    }

    #[async_trait]
    impl CommandIngestor for UnmappableOffsetIngestor {
        type Offset = u64;

        async fn seek(&mut self, _offset: Option<u64>) -> Result<(), RrError> {
            Ok(())
        }

        async fn poll(&mut self, _max: usize) -> Result<Vec<IngressCommand<u64>>, RrError> {
            Ok(vec![IngressCommand {
                offset: 0,
                payload: b"poison".to_vec(),
            }])
        }

        async fn commit_applied(&mut self, _offset: u64) -> Result<(), RrError> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn supervisor_exits_on_offset_map_failure() {
        let mgr = make_single_node_leader().await;
        let raft = mgr.get_raft(1).await.unwrap();
        let log_index_before = raft.metrics().borrow_watched().last_log_index;

        let commits = Arc::new(AtomicU64::new(0));
        let ingestor = UnmappableOffsetIngestor {
            commits: Arc::clone(&commits),
        };
        let auto_idem = AutoIdempotency {
            request_type: "poison_topic".into(),
            offset_to_request_id: Box::new(|_| Err(OffsetMapError::new("unmappable offset"))),
        };
        let handle =
            spawn_with_batch_size(Arc::clone(&mgr), 1, Box::new(ingestor), Some(auto_idem), 1);

        // The supervisor must terminate (fail-closed), not hot-loop.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while handle.is_running() {
            if std::time::Instant::now() > deadline {
                panic!("supervisor must exit on offset→request_id mapping failure (C7)");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            commits.load(Ordering::SeqCst),
            0,
            "no offset may be committed when mapping fails"
        );
        assert_eq!(
            raft.metrics().borrow_watched().last_log_index,
            log_index_before,
            "nothing may be proposed when mapping fails"
        );
        handle.stop().await;
    }
}
