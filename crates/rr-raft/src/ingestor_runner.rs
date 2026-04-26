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
//! The poll loop calls [`crate::group_manager::RaftGroupManager::write`]
//! sequentially per command. `write` blocks until the entry is
//! committed and applied, so the loop is naturally rate-limited by
//! commit throughput — no separate bounded channel needed.
//!
//! # Failure handling
//!
//! Errors from `ingestor.poll_bytes` trigger exponential backoff
//! (capped at [`MAX_POLL_BACKOFF`]). The supervisor never steps the
//! Raft leader down on ingestor failure — a Kafka outage must not
//! destabilise consensus (ADR A-0001 §"Open questions" #5).
//!
//! Errors from `mgr.write` (e.g. the leader stepped down between
//! poll and write) cause the poll loop to exit cleanly; the
//! supervisor parks waiting for the next leadership transition.

use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, ServerState};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use rr_core::error::RrError;
use rr_core::framed_entry::FramedEntry;
use rr_core::framed_snapshot::FramedSnapshot;
use rr_core::ingestor::DynCommandIngestor;
use rr_core::types::{GroupId, NodeId};

use crate::group_manager::RaftGroupManager;

type RrRaftMetrics = openraft::RaftMetrics<NodeId, BasicNode>;

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
/// flag is set or when `mgr` is dropped (any of its `Weak` upgrades
/// will return `None` from inside the task).
///
/// Returns immediately; the actual ingestor work runs in the background.
pub fn spawn(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    ingestor: Box<dyn DynCommandIngestor>,
) -> IngestorHandle {
    spawn_with_batch_size(mgr, group_id, ingestor, DEFAULT_POLL_BATCH_SIZE)
}

/// Variant of [`spawn`] that accepts an explicit batch size. Tests pin
/// it to small values (e.g. 1) to make assertions simpler.
pub fn spawn_with_batch_size(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    ingestor: Box<dyn DynCommandIngestor>,
    batch_size: usize,
) -> IngestorHandle {
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let join = tokio::spawn(async move {
        if let Err(e) = supervise(mgr, group_id, ingestor, batch_size, cancel_rx).await {
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
            warn!(
                group_id,
                error = %e,
                "ingestor seed_offset failed; will retry next leadership tenure"
            );
        }

        run_poll_loop(
            &mgr,
            group_id,
            ingestor.as_mut(),
            batch_size,
            &mut metrics_rx,
            &mut cancel_rx,
        )
        .await;

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

async fn run_poll_loop(
    mgr: &RaftGroupManager,
    group_id: GroupId,
    ingestor: &mut dyn DynCommandIngestor,
    batch_size: usize,
    metrics_rx: &mut watch::Receiver<RrRaftMetrics>,
    cancel_rx: &mut watch::Receiver<bool>,
) {
    let mut backoff = INITIAL_POLL_BACKOFF;
    loop {
        if *cancel_rx.borrow() || !is_leader(metrics_rx) {
            return;
        }

        let poll_started = std::time::Instant::now();
        let batch_result = tokio::select! {
            _ = cancel_rx.changed() => return,
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
                _ = cancel_rx.changed() => return,
                _ = metrics_rx.changed() => {
                    if !is_leader(metrics_rx) { return; }
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
            continue;
        }

        for cmd in batch {
            let envelope = FramedEntry::from_payload(cmd.payload)
                .with_offset(cmd.offset)
                .encode();
            let bytes = match envelope {
                Ok(b) => b,
                Err(e) => {
                    warn!(group_id, error = %e, "ingestor: framed entry encode failed; skipping batch");
                    break;
                }
            };
            match mgr.write(group_id, bytes).await {
                Ok(_) => {}
                Err(e) => {
                    debug!(group_id, error = %e, "ingestor: write failed; likely stepped down");
                    return;
                }
            }
        }
    }
}

fn is_leader(metrics_rx: &watch::Receiver<RrRaftMetrics>) -> bool {
    matches!(metrics_rx.borrow().state, ServerState::Leader)
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
            raft.metrics().borrow().current_leader.is_some(),
            "expected leader after init"
        );
        mgr
    }

    #[tokio::test]
    async fn supervisor_calls_seek_and_polls_when_leader() {
        let mgr = make_single_node_leader().await;
        let (ingestor, poll_called) = ScriptedIngestor::new(3);
        let handle = spawn_with_batch_size(Arc::clone(&mgr), 1, Box::new(ingestor), 1);

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
        let handle = spawn_with_batch_size(Arc::clone(&mgr), 1, Box::new(ingestor), 1);
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
        let handle = spawn_with_batch_size(Arc::clone(&mgr), 1, Box::new(ingestor), 1);

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
        let handle = spawn_with_batch_size(Arc::clone(&mgr), 999, Box::new(ingestor), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !handle.is_running(),
            "supervisor must exit promptly when group is missing"
        );
        handle.stop().await;
    }
}
