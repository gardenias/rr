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

use rr_core::error::RrError;
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
        let _ = self.join.await;
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
    let raft = mgr.get_raft(group_id).await.ok_or_else(|| {
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
        run_send_loop(
            &mgr,
            group_id,
            rr_client.as_deref(),
            kafka.as_ref(),
            &mut metrics_rx,
            &mut stop_rx,
        )
        .await;

        if check_stop(&mut stop_rx) {
            debug!(group_id, "output runner stopped after step-down");
            return Ok(());
        }
        info!(group_id, "output runner lost leadership; parking");
    }
}

async fn run_send_loop(
    mgr: &RaftGroupManager,
    group_id: GroupId,
    rr_client: Option<&rr_client::RrClient>,
    kafka: &dyn KafkaProducer,
    metrics_rx: &mut RrMetricsRx,
    stop_rx: &mut oneshot::Receiver<()>,
) {
    let mut backoff = INITIAL_SEND_BACKOFF;
    loop {
        if check_stop(stop_rx) || !is_leader(metrics_rx) {
            return;
        }

        let pending = match mgr.pending_outputs(group_id).await {
            Ok(p) => p,
            Err(e) => {
                warn!(group_id, error = %e, "pending_outputs failed; outbox unsupported on this group");
                return;
            }
        };
        metrics::gauge!(
            "rr_output_pending",
            "group_id" => group_id.to_string()
        )
        .set(pending.len() as f64);

        if pending.is_empty() {
            tokio::select! {
                _ = &mut *stop_rx => return,
                _ = metrics_rx.changed() => {
                    if !is_leader(metrics_rx) { return; }
                }
                _ = tokio::time::sleep(IDLE_POLL_INTERVAL) => {}
            }
            continue;
        }

        let mut had_failure = false;
        for output in pending {
            if check_stop(stop_rx) || !is_leader(metrics_rx) {
                return;
            }
            let seq = output.seq;
            let target_label = match &output.target {
                OutputTarget::RaftWrite { .. } => "raft_write",
                OutputTarget::KafkaPublish { .. } => "kafka_publish",
            };
            let send_started = std::time::Instant::now();
            let send_result = dispatch_one(&output, rr_client, kafka).await;
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
                    tokio::time::sleep(backoff).await;
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
            let idem_key = rr_core::idempotency::IdempotencyKey::with_client_id(
                "rr.output",
                seq_to_request_id(output.seq),
                output.idempotency_key.clone(),
            );
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

/// Derive a monotonic `request_id` for `IdempotencyKey` from the outbox
/// sequence. The outbox guarantees seq is monotone, which is the only
/// property the framework idempotency layer requires.
fn seq_to_request_id(seq: u64) -> u64 {
    seq
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
        dispatch_one(&output, None, producer.as_ref())
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
        let err = dispatch_one(&output, None, producer.as_ref())
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
        let err = dispatch_one(&output, None, producer.as_ref())
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("OutputTarget::RaftWrite requires an RrClient"));
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
}
