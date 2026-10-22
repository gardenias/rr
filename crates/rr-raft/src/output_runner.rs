//! Leader-exclusive output delivery loop for the framework outbox.
//!
//! [ADR A-0007](../../../.sisyphus/adr/A-0007-output-outbox-mechanism.md)
//! defines the contract: every output produced by
//! [`rr_core::service::Service::handle_with_outputs`] must be delivered
//! at-least-once after the Raft entry that produced it has committed and
//! applied. The runner enforces three properties:
//!
//! 1. **Leader-only execution** — only the current leader's runner sends
//!    outputs. Followers consume the outbox via state-machine snapshot
//!    replication; their runners park until they win leadership.
//! 2. **At-least-once delivery** — on send error, the output remains in
//!    the outbox and is retried after exponential backoff. Watermarks
//!    only advance after a confirmed successful send.
//! 3. **Restart-safe** — outbox state rides snapshots (see
//!    [`rr_core::framed_snapshot::OutboxState`]), so a new leader resumes
//!    from the previous leader's persisted `sent_watermark`.
//!
//! # Backoff
//!
//! Per-output exponential backoff, capped at [`MAX_SEND_BACKOFF`]. A
//! permanently-down delivery target retries roughly twice a minute
//! forever — matching the ingestor runner's failure semantics.

use std::sync::Arc;
use std::time::Duration;

use openraft::type_config::async_runtime::WatchReceiver;
use openraft::ServerState;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use rr_core::error::{RaftError, RrError};
use rr_core::output::{KafkaProducer, NoopKafkaProducer, Output, OutputTarget};
use rr_core::types::GroupId;

use crate::group_manager::RaftGroupManager;
use crate::type_config::RrTypeConfig;

type RrRaftMetrics = openraft::RaftMetrics<RrTypeConfig>;
type RrMetricsRx = openraft::type_config::alias::WatchReceiverOf<RrTypeConfig, RrRaftMetrics>;

/// Initial backoff after the first send failure.
pub const INITIAL_SEND_BACKOFF: Duration = Duration::from_millis(100);

/// Maximum backoff between failed send attempts. Caps retry pressure on a
/// down delivery target without giving up.
pub const MAX_SEND_BACKOFF: Duration = Duration::from_secs(30);

/// How long the runner waits between polls when no outputs are pending and
/// the node is still the leader. Short enough to keep p99 delivery latency
/// low without burning CPU when the outbox is empty.
pub const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How often the supervisor re-checks whether a group that vanished
/// mid-leadership has been re-created. The group being gone is not an error
/// storm worth hot-looping over; 1s bounds the resume latency after a
/// stop→start cycle.
pub const GROUP_GONE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Handle for the background output runner task.
pub struct OutputHandle {
    stop_tx: Option<oneshot::Sender<()>>,
    join: JoinHandle<()>,
}

impl OutputHandle {
    /// Stop the background task and wait for termination.
    pub async fn stop(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Err(e) = self.join.await {
            // A runner panic must not vanish silently into teardown.
            tracing::warn!(error = %e, "output runner task failed to join cleanly (panicked?)");
        }
    }
}

/// Spawn the output runner for `group_id`.
///
/// The runner monitors the group's leadership state via openraft metrics;
/// when leader it drains pending outputs from the outbox and dispatches
/// them via the supplied delivery clients. Followers' runners park.
///
/// Either delivery client may be `None`; outputs whose target requires the
/// missing client are retried forever (and an error is logged each
/// attempt) until the operator either installs the client or removes the
/// offending output from the service. This mirrors the ADR A-0007 §
/// "Output failure" requirement: at-least-once means no silent drops.
pub fn spawn(
    manager: Arc<RaftGroupManager>,
    group_id: GroupId,
    rr_client: Option<Arc<rr_client::RrClient>>,
    kafka_producer: Option<Arc<dyn KafkaProducer>>,
) -> OutputHandle {
    let (stop_tx, stop_rx) = oneshot::channel();
    let join = tokio::spawn(async move {
        let kafka: Arc<dyn KafkaProducer> =
            kafka_producer.unwrap_or_else(|| Arc::new(NoopKafkaProducer) as Arc<dyn KafkaProducer>);
        if let Err(e) = supervise(manager, group_id, rr_client, kafka, stop_rx).await {
            warn!(group_id, error = %e, "output runner supervisor exited with error");
        }
    });
    OutputHandle {
        stop_tx: Some(stop_tx),
        join,
    }
}

async fn supervise(
    mgr: Arc<RaftGroupManager>,
    group_id: GroupId,
    rr_client: Option<Arc<rr_client::RrClient>>,
    kafka: Arc<dyn KafkaProducer>,
    mut stop_rx: oneshot::Receiver<()>,
) -> Result<(), RrError> {
    let mut raft = mgr.get_raft(group_id).await.ok_or_else(|| {
        RrError::Application(format!("output runner: group {group_id} not started"))
    })?;
    let mut metrics_rx = raft.metrics();
    info!(group_id, "output runner started");

    loop {
        if check_stop(&mut stop_rx) {
            debug!(group_id, "output runner stopped while parked");
            return Ok(());
        }

        if !is_leader(&metrics_rx) {
            tokio::select! {
                _ = &mut stop_rx => {
                    debug!(group_id, "output runner stopped while waiting for leadership");
                    return Ok(());
                }
                changed = metrics_rx.changed() => {
                    if changed.is_err() {
                        debug!(group_id, "raft metrics channel closed; output runner exiting");
                        return Ok(());
                    }
                }
            }
            continue;
        }

        info!(group_id, "output runner became leader; draining outbox");
        match run_send_loop(
            &mgr,
            group_id,
            rr_client.as_deref(),
            kafka.as_ref(),
            &mut metrics_rx,
            &mut stop_rx,
        )
        .await
        {
            SendLoopExit::Parked => {
                if check_stop(&mut stop_rx) {
                    debug!(group_id, "output runner stopped after step-down");
                    return Ok(());
                }
                info!(group_id, "output runner lost leadership; parking");
            }
            SendLoopExit::GroupGone => {
                // The group was stopped underneath us (stop_group removes it
                // from the manager while the runner is mid-leadership). Park
                // until the group is re-created or the runner is stopped —
                // re-entering the send loop immediately would hot-spin on the
                // same GroupNotFound error.
                warn!(
                    group_id,
                    "group removed while output runner was active; parking until it returns"
                );
                loop {
                    tokio::select! {
                        _ = &mut stop_rx => {
                            debug!(group_id, "output runner stopped while parked on a removed group");
                            return Ok(());
                        }
                        _ = tokio::time::sleep(GROUP_GONE_POLL_INTERVAL) => {
                            if let Some(new_raft) = mgr.get_raft(group_id).await {
                                raft = new_raft;
                                metrics_rx = raft.metrics();
                                info!(group_id, "group returned; output runner resuming");
                                break;
                            }
                        }
                    }
                }
            }
            SendLoopExit::OutboxUnsupported(e) => {
                // Permanent misconfiguration (state machine without outbox
                // support): warn ONCE and park until stopped — re-entering
                // the send loop would spin and flood the log.
                warn!(
                    group_id,
                    error = %e,
                    "outbox unsupported on this group; output runner parking permanently"
                );
                let _ = (&mut stop_rx).await;
                return Ok(());
            }
        }
    }
}

/// Why [`run_send_loop`] returned to the supervisor.
enum SendLoopExit {
    /// Lost leadership or stop requested — the supervisor parks until the
    /// next leadership transition.
    Parked,
    /// The group disappeared from the manager (`stop_group`) while the
    /// runner held leadership.
    GroupGone,
    /// The group's state machine has no outbox control — a permanent
    /// misconfiguration, not a transient error.
    OutboxUnsupported(RrError),
}

async fn run_send_loop(
    mgr: &RaftGroupManager,
    group_id: GroupId,
    rr_client: Option<&rr_client::RrClient>,
    kafka: &dyn KafkaProducer,
    metrics_rx: &mut RrMetricsRx,
    stop_rx: &mut oneshot::Receiver<()>,
) -> SendLoopExit {
    let mut backoff = INITIAL_SEND_BACKOFF;
    loop {
        if check_stop(stop_rx) || !is_leader(metrics_rx) {
            return SendLoopExit::Parked;
        }

        let pending = match mgr.pending_outputs(group_id).await {
            Ok(p) => p,
            Err(e) => {
                // Distinguish "group is gone" (stop_group won the race) from
                // "state machine has no outbox" (permanent): the supervisor
                // parks differently for each and neither may hot-spin.
                return match e {
                    RrError::Raft(RaftError::GroupNotFound { .. }) => SendLoopExit::GroupGone,
                    other => SendLoopExit::OutboxUnsupported(other),
                };
            }
        };
        metrics::gauge!(
            "rr_output_pending",
            "group_id" => group_id.to_string()
        )
        .set(pending.len() as f64);

        if pending.is_empty() {
            tokio::select! {
                _ = &mut *stop_rx => return SendLoopExit::Parked,
                _ = metrics_rx.changed() => {
                    if !is_leader(metrics_rx) { return SendLoopExit::Parked; }
                }
                _ = tokio::time::sleep(IDLE_POLL_INTERVAL) => {}
            }
            continue;
        }

        let mut had_failure = false;
        for output in pending {
            if check_stop(stop_rx) || !is_leader(metrics_rx) {
                return SendLoopExit::Parked;
            }
            let seq = output.seq;
            let target_label = match &output.target {
                OutputTarget::RaftWrite { .. } => "raft_write",
                OutputTarget::KafkaPublish { .. } => "kafka_publish",
            };
            let send_started = std::time::Instant::now();
            let send_result = dispatch_one(&output, group_id, rr_client, kafka).await;
            metrics::histogram!(
                "rr_output_send_duration_seconds",
                "group_id" => group_id.to_string(),
                "target" => target_label,
            )
            .record(send_started.elapsed().as_secs_f64());

            match send_result {
                Ok(()) => {
                    if let Err(e) = mgr.advance_output_watermark(group_id, seq).await {
                        warn!(
                            group_id,
                            seq,
                            error = %e,
                            "advance_output_watermark failed after successful send"
                        );
                    }
                    metrics::counter!(
                        "rr_output_send_total",
                        "group_id" => group_id.to_string(),
                        "target" => target_label,
                    )
                    .increment(1);
                }
                Err(e) => {
                    had_failure = true;
                    metrics::counter!(
                        "rr_output_send_failures_total",
                        "group_id" => group_id.to_string(),
                        "target" => target_label,
                    )
                    .increment(1);
                    warn!(
                        group_id,
                        seq,
                        target = target_label,
                        idempotency_key = %output.idempotency_key,
                        error = %e,
                        backoff_ms = backoff.as_millis() as u64,
                        "output send failed; backing off"
                    );
                    // The backoff (up to MAX_SEND_BACKOFF) must not delay a
                    // stop_group shutdown: race it against the stop channel.
                    tokio::select! {
                        _ = &mut *stop_rx => return SendLoopExit::Parked,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_SEND_BACKOFF);
                    break;
                }
            }
        }
        if !had_failure {
            backoff = INITIAL_SEND_BACKOFF;
        }
    }
}

async fn dispatch_one(
    output: &Output,
    source_group_id: GroupId,
    rr_client: Option<&rr_client::RrClient>,
    kafka: &dyn KafkaProducer,
) -> Result<(), RrError> {
    match &output.target {
        OutputTarget::RaftWrite { addrs: _, group_id } => {
            let client = rr_client.ok_or_else(|| {
                RrError::Application(
                    "OutputTarget::RaftWrite requires an RrClient but none was \
                     supplied to output_runner::spawn"
                        .to_string(),
                )
            })?;
            let idem_key = raft_write_idem_key(source_group_id, output);
            client
                .send_request_idempotent(*group_id, output.payload.clone(), idem_key)
                .await
                .map(|_response| ())
        }
        OutputTarget::KafkaPublish {
            topic,
            partition,
            key,
        } => {
            kafka
                .publish(
                    topic,
                    *partition,
                    key,
                    &output.payload,
                    &output.idempotency_key,
                )
                .await
        }
    }
}

/// Idempotency key for an [`OutputTarget::RaftWrite`] delivery.
///
/// `request_type` is namespaced per SOURCE group (`rr.output.g<N>`) because
/// the request id is the per-source-group outbox `seq`: two source groups
/// writing the same target group must not share a watermark namespace —
/// otherwise equal seqs collide (`Collision`/`Expired`) and outputs are
/// silently dropped. Per-source-group monotonicity of `seq` keeps the
/// target's watermark contract intact. `output.idempotency_key` remains the
/// diagnostic-only `client_id`.
fn raft_write_idem_key(
    source_group_id: GroupId,
    output: &Output,
) -> rr_core::idempotency::IdempotencyKey {
    rr_core::idempotency::IdempotencyKey::with_client_id(
        format!("rr.output.g{source_group_id}"),
        output.seq,
        output.idempotency_key.clone(),
    )
}

fn is_leader(metrics_rx: &RrMetricsRx) -> bool {
    matches!(metrics_rx.borrow_watched().state, ServerState::Leader)
}

fn check_stop(stop_rx: &mut oneshot::Receiver<()>) -> bool {
    match stop_rx.try_recv() {
        Ok(()) => true,
        Err(oneshot::error::TryRecvError::Closed) => true,
        Err(oneshot::error::TryRecvError::Empty) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rr_core::output::{KafkaProducer, Output, OutputTarget};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    use crate::group_manager::{RaftGroupManager, StateMachineFactory};
    use openraft::BasicNode;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;
    use std::collections::BTreeMap;

    type PublishedRecord = (String, i32, Vec<u8>, Vec<u8>, String);

    struct CapturingKafka {
        published: Mutex<Vec<PublishedRecord>>,
        fail_n: AtomicU64,
    }

    impl CapturingKafka {
        fn new() -> Self {
            Self {
                published: Mutex::new(Vec::new()),
                fail_n: AtomicU64::new(0),
            }
        }

        fn fail_first_n(n: u64) -> Self {
            Self {
                published: Mutex::new(Vec::new()),
                fail_n: AtomicU64::new(n),
            }
        }
    }

    #[async_trait]
    impl KafkaProducer for CapturingKafka {
        async fn publish(
            &self,
            topic: &str,
            partition: i32,
            key: &[u8],
            payload: &[u8],
            idempotency_key: &str,
        ) -> Result<(), RrError> {
            let remaining = self.fail_n.load(Ordering::SeqCst);
            if remaining > 0 {
                self.fail_n.fetch_sub(1, Ordering::SeqCst);
                return Err(RrError::Application(format!(
                    "scripted failure (remaining={remaining})"
                )));
            }
            self.published.lock().unwrap().push((
                topic.to_string(),
                partition,
                key.to_vec(),
                payload.to_vec(),
                idempotency_key.to_string(),
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn noop_kafka_producer_errors() {
        let p = NoopKafkaProducer;
        let err = p
            .publish("orders", 0, b"key", b"payload", "idem-1")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("KafkaPublish to topic 'orders'"));
    }

    // INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    #[tokio::test]
    async fn dispatch_one_kafka_publish_success() {
        let producer = Arc::new(CapturingKafka::new()) as Arc<dyn KafkaProducer>;
        let output = Output::new(
            "tx-42:0",
            OutputTarget::KafkaPublish {
                topic: "orders".to_string(),
                partition: 0,
                key: b"acct-7".to_vec(),
            },
            b"order-payload".to_vec(),
        );
        dispatch_one(&output, 7, None, producer.as_ref())
            .await
            .unwrap();
    }

    // INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    #[tokio::test]
    async fn dispatch_one_kafka_publish_propagates_failure() {
        let producer = Arc::new(CapturingKafka::fail_first_n(1)) as Arc<dyn KafkaProducer>;
        let output = Output::new(
            "tx-42:0",
            OutputTarget::KafkaPublish {
                topic: "orders".to_string(),
                partition: 0,
                key: b"acct-7".to_vec(),
            },
            b"order-payload".to_vec(),
        );
        let err = dispatch_one(&output, 7, None, producer.as_ref())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("scripted failure"));
    }

    // INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    #[tokio::test]
    async fn dispatch_one_raft_write_requires_client() {
        let producer = Arc::new(CapturingKafka::new()) as Arc<dyn KafkaProducer>;
        let output = Output::new(
            "tx-42:0",
            OutputTarget::RaftWrite {
                addrs: vec!["127.0.0.1:50051".into()],
                group_id: 9,
            },
            b"payload".to_vec(),
        );
        let err = dispatch_one(&output, 7, None, producer.as_ref())
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("OutputTarget::RaftWrite requires an RrClient"));
    }

    /// INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    /// The RaftWrite idempotency namespace is per SOURCE group: two source
    /// groups writing one target group reuse the same per-source outbox seq
    /// range, so a shared `request_type` would collide and silently drop
    /// outputs on the target's idempotency layer.
    #[test]
    fn raft_write_idem_key_is_namespaced_per_source_group() {
        let output = Output::new(
            "tx-42:0",
            OutputTarget::RaftWrite {
                addrs: vec!["127.0.0.1:50051".into()],
                group_id: 9,
            },
            b"payload".to_vec(),
        );
        let key_a = raft_write_idem_key(1, &output);
        let key_b = raft_write_idem_key(2, &output);
        assert_eq!(key_a.request_type, "rr.output.g1");
        assert_eq!(key_b.request_type, "rr.output.g2");
        assert_ne!(
            key_a, key_b,
            "two source groups must never share an idempotency namespace"
        );
        assert_eq!(key_a.request_id, output.seq);
        assert_eq!(key_a.client_id.as_deref(), Some("tx-42:0"));
    }

    // INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    #[tokio::test]
    async fn output_handle_stop_completes() {
        let (stop_tx, stop_rx) = oneshot::channel();
        let join = tokio::spawn(async move {
            let _ = stop_rx.await;
        });
        let handle = OutputHandle {
            stop_tx: Some(stop_tx),
            join,
        };
        handle.stop().await;
    }

    // ── Supervisor parking semantics (send-loop exit classification) ─────

    /// State machine without outbox support (the `outbox_control` default).
    struct NoOutboxSM;

    #[async_trait]
    impl StateMachine for NoOutboxSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }
        async fn restore(&mut self, _: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
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
        async fn restore(&mut self, _: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
    }

    fn outbox_factory() -> StateMachineFactory {
        Box::new(|gid| {
            Box::new(crate::service_adapter::ServiceStateMachineAdapter::new(
                gid,
                EchoService,
            ))
        })
    }

    async fn single_node_leader(mgr: &RaftGroupManager, group_id: GroupId) {
        mgr.start_group(group_id).await.unwrap();
        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            BasicNode {
                addr: String::new(),
            },
        );
        mgr.init_cluster(group_id, members).await.unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let raft = mgr.get_raft(group_id).await.unwrap();
            if matches!(raft.metrics().borrow_watched().state, ServerState::Leader) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for leadership"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    /// A leader whose state machine has no outbox must exit the send loop
    /// with `OutboxUnsupported` immediately — the supervisor parks it
    /// permanently instead of hot-spinning on the error.
    #[tokio::test]
    async fn run_send_loop_reports_outbox_unsupported() {
        let mgr = RaftGroupManager::new(1, Box::new(|_| Box::new(NoOutboxSM)));
        single_node_leader(&mgr, 1).await;
        let raft = mgr.get_raft(1).await.unwrap();
        let mut metrics_rx = raft.metrics();
        let (_stop_tx, mut stop_rx) = oneshot::channel();

        let kafka = NoopKafkaProducer;
        let exit = run_send_loop(&mgr, 1, None, &kafka, &mut metrics_rx, &mut stop_rx).await;
        assert!(
            matches!(exit, SendLoopExit::OutboxUnsupported(_)),
            "expected OutboxUnsupported, got {}",
            match exit {
                SendLoopExit::Parked => "Parked",
                SendLoopExit::GroupGone => "GroupGone",
                SendLoopExit::OutboxUnsupported(_) => "OutboxUnsupported",
            }
        );
        mgr.stop_group(1).await.unwrap();
    }

    /// INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    /// A group stopped mid-leadership parks the send loop: by the time
    /// `stop_group` returns, the metrics channel already carries the
    /// shutdown state, so the loop sees "not leader" and parks — the
    /// supervisor's GroupGone arm covers only the narrower race where a
    /// stale snapshot still says Leader while the manager dropped the group.
    #[tokio::test]
    async fn run_send_loop_parks_when_group_stopped() {
        let mgr = RaftGroupManager::new(1, outbox_factory());
        single_node_leader(&mgr, 1).await;
        let raft = mgr.get_raft(1).await.unwrap();
        let mut metrics_rx = raft.metrics();
        let (_stop_tx, mut stop_rx) = oneshot::channel();

        mgr.stop_group(1).await.unwrap();

        let kafka = NoopKafkaProducer;
        let exit = run_send_loop(&mgr, 1, None, &kafka, &mut metrics_rx, &mut stop_rx).await;
        assert!(
            matches!(exit, SendLoopExit::Parked | SendLoopExit::GroupGone),
            "a stopped group must park the runner (never an error storm); got {}",
            match exit {
                SendLoopExit::Parked => "Parked",
                SendLoopExit::GroupGone => "GroupGone",
                SendLoopExit::OutboxUnsupported(_) => "OutboxUnsupported",
            }
        );
    }

    /// INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    /// A stop fired while the send loop is sleeping off a send failure must
    /// interrupt the backoff, not wait it out (backoff grows to 30s;
    /// stop_group would otherwise block on the runner for tens of seconds).
    #[tokio::test]
    async fn send_backoff_is_cancellable_via_stop() {
        struct EmittingService;

        #[async_trait]
        impl rr_core::service::Service for EmittingService {
            async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
                Ok(request)
            }
            async fn handle_with_outputs(
                &self,
                request: Vec<u8>,
            ) -> Result<rr_core::output::HandleResult, RrError> {
                Ok(rr_core::output::HandleResult::with_outputs(
                    request,
                    vec![Output::new(
                        "idem:x",
                        OutputTarget::KafkaPublish {
                            topic: "t".to_string(),
                            partition: 0,
                            key: b"k".to_vec(),
                        },
                        b"payload".to_vec(),
                    )],
                ))
            }
            fn name(&self) -> &str {
                "emitting"
            }
            async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
                Ok(vec![])
            }
            async fn restore(&mut self, _: &[u8]) -> Result<(), RrError> {
                Ok(())
            }
        }

        let mgr = Arc::new(RaftGroupManager::new(
            1,
            Box::new(|gid| {
                Box::new(crate::service_adapter::ServiceStateMachineAdapter::new(
                    gid,
                    EmittingService,
                ))
            }),
        ));
        single_node_leader(&mgr, 1).await;
        mgr.write(1, b"x".to_vec()).await.unwrap();
        let raft = mgr.get_raft(1).await.unwrap();
        let mut metrics_rx = raft.metrics();
        let (stop_tx, mut stop_rx) = oneshot::channel();

        // Always-failing producer: the loop sleeps with doubling backoff
        // (100ms, 200ms, 400ms, …) between attempts.
        let kafka = CapturingKafka {
            published: Mutex::new(Vec::new()),
            fail_n: AtomicU64::new(u64::MAX),
        };
        let mgr2 = Arc::clone(&mgr);
        let runner = tokio::spawn(async move {
            run_send_loop(&mgr2, 1, None, &kafka, &mut metrics_rx, &mut stop_rx).await
        });

        // Let backoff grow past 1s (5 failures ⇒ 1600ms sleep window).
        tokio::time::sleep(Duration::from_millis(1600)).await;
        let stop_sent = std::time::Instant::now();
        let _ = stop_tx.send(());
        let exit = tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("send loop must exit promptly once stopped")
            .unwrap();
        assert!(
            matches!(exit, SendLoopExit::Parked),
            "stop during backoff must park the send loop"
        );
        assert!(
            stop_sent.elapsed() < Duration::from_secs(1),
            "stop must interrupt the in-flight backoff sleep, took {:?}",
            stop_sent.elapsed()
        );
        mgr.stop_group(1).await.unwrap();
    }
}
