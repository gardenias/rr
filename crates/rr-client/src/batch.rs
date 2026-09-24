//! Opt-in client-side write batching (Kafka-producer style), off by default.
//!
//! When enabled via [`RrClientBuilder::with_batching`](crate::RrClientBuilder::with_batching),
//! unkeyed `send_request` writes accumulate per partition and flush as ONE
//! Raft entry carrying a [`rr_core::batch`] frame, cutting gRPC frame /
//! syscall density on the client→server path (100k-roadmap M1). Idempotency
//! -keyed writes (`send_request_idempotent`) always bypass the batcher.
//!
//! Mirrors Kafka producer knobs:
//! - [`BatchConfig::max_entries`] — flush when this many writes accumulated;
//! - [`BatchConfig::max_bytes`] — flush when accumulated bytes cross it;
//! - [`BatchConfig::linger`] — max wait of the first queued write
//!   (linger.ms); `ZERO` / sub-millisecond flushes every write immediately;
//! - [`BatchConfig::max_buffered`] / [`BatchConfig::max_buffered_bytes`] —
//!   total buffered-writes caps across partitions; past either, submits fail
//!   fast (backpressure).
//!
//! Flushes of one partition are serialized (Kafka `max.in.flight=1`
//! analogue): a younger batch cannot overtake an older one that is still
//! redirecting / backing off, so per-partition commit order matches
//! submission order.
//!
//! Requires every server to run a framework version whose apply path splits
//! batch frames; an older server echoes the frame into the service as one
//! opaque payload, and this client fails the batch loudly on the missing
//! response frame instead of reporting fake successes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rr_core::error::{NetworkError, RaftError, RrError};
use tokio::sync::oneshot;

/// Linger values below 1ms (including `Duration::ZERO`) select the
/// flush-immediately path — Kafka `linger.ms=0` semantics.
const IMMEDIATE_LINGER: Duration = Duration::from_millis(1);

/// Kafka-producer-style batching knobs. Batching is OFF unless explicitly
/// enabled; with no batching the write path is byte-for-byte the v0.2.x one.
///
/// ⚠️ Batched writes are NOT idempotent: an unkeyed batch that is retried
/// (leader redirect, backoff) can be applied twice server-side, once per
/// attempt. Batch only idempotent-by-business-key workloads, or use
/// `send_request_idempotent` (which always bypasses the batcher).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchConfig {
    /// Flush a partition's buffer when this many writes have accumulated.
    pub max_entries: usize,
    /// Flush a partition's buffer when accumulated payload bytes reach this.
    pub max_bytes: usize,
    /// Maximum time the first queued write of a batch waits before the batch
    /// flushes anyway (Kafka `linger.ms`). `Duration::ZERO` (or any
    /// sub-millisecond value) does NOT disable the timer — it flushes every
    /// write immediately, one RPC per write.
    pub linger: Duration,
    /// Backpressure cap on the TOTAL buffered-but-unflushed writes across
    /// all partitions (Kafka `buffer.memory`, in entries). Submitting past
    /// the cap fails fast instead of growing memory unboundedly.
    pub max_buffered: usize,
    /// Backpressure cap on the TOTAL buffered-but-unflushed payload bytes
    /// across all partitions (default 32 MiB). A submit that would push the
    /// buffer past the cap fails fast, alongside [`Self::max_buffered`].
    pub max_buffered_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_entries: 64,
            max_bytes: 1 << 20, // 1 MiB
            linger: Duration::from_millis(5),
            max_buffered: 8192,
            max_buffered_bytes: 32 << 20, // 32 MiB
        }
    }
}

impl BatchConfig {
    /// Validate the knobs: zero `max_entries` / `max_buffered` /
    /// `max_buffered_bytes` would wedge (nothing ever flushes / nothing is
    /// ever accepted).
    pub fn validate(&self) -> Result<(), RrError> {
        if self.max_entries == 0 {
            return Err(RrError::Network(NetworkError::InvalidConfig(
                "batching: max_entries must be > 0".to_string(),
            )));
        }
        if self.max_buffered == 0 {
            return Err(RrError::Network(NetworkError::InvalidConfig(
                "batching: max_buffered must be > 0".to_string(),
            )));
        }
        if self.max_buffered_bytes == 0 {
            return Err(RrError::Network(NetworkError::InvalidConfig(
                "batching: max_buffered_bytes must be > 0".to_string(),
            )));
        }
        Ok(())
    }
}

type PendingTx = oneshot::Sender<Result<Vec<u8>, RrError>>;
type PendingItem = (Vec<u8>, PendingTx);

#[derive(Default)]
struct Accumulator {
    items: Vec<PendingItem>,
    bytes: usize,
    /// A linger timer is sleeping for the current window.
    linger_armed: bool,
}

#[derive(Default)]
struct BatcherInner {
    accumulators: HashMap<u64, Accumulator>,
    /// Total buffered-but-unflushed writes across partitions.
    buffered_total: usize,
    /// Total buffered-but-unflushed payload bytes across partitions.
    buffered_bytes: usize,
    /// Per-partition flush generation, bumped every time a window closes. A
    /// linger timer may flush only while the partition's generation still
    /// equals the one it was armed under — a stale timer from an
    /// already-flushed window must not flush the NEW window early. Kept
    /// apart from `accumulators` so removing a drained accumulator cannot
    /// reset the generation and resurrect a stale timer.
    generations: HashMap<u64, u64>,
}

impl BatcherInner {
    /// Close `partition`'s current window: take its buffered items, charge
    /// the totals down, bump the flush generation, and drop the drained
    /// accumulator so idle partitions do not linger in the map.
    fn take_window(&mut self, partition: u64) -> Vec<PendingItem> {
        let Some(acc) = self.accumulators.get_mut(&partition) else {
            return Vec::new();
        };
        acc.linger_armed = false;
        let items = std::mem::take(&mut acc.items);
        let bytes = acc.bytes;
        acc.bytes = 0;
        self.buffered_total -= items.len();
        self.buffered_bytes -= bytes;
        *self.generations.entry(partition).or_insert(0) += 1;
        self.accumulators.remove(&partition);
        items
    }

    /// Linger timer woke: take the window only when the timer was armed for
    /// the partition's CURRENT flush generation.
    fn take_window_if_current(
        &mut self,
        partition: u64,
        armed_generation: u64,
    ) -> Option<Vec<PendingItem>> {
        if self.generations.get(&partition).copied().unwrap_or(0) != armed_generation {
            return None;
        }
        let items = self.take_window(partition);
        (!items.is_empty()).then_some(items)
    }
}

/// Shared state of the client batcher. Owned by `RrClient`; flush tasks hold
/// an `RrClient` clone, never the reverse — no reference cycle.
pub(crate) struct BatchState {
    config: BatchConfig,
    inner: Mutex<BatcherInner>,
    /// Per-partition flush serializers (Kafka `max.in.flight=1` analogue): a
    /// flush holds its partition's lock for the whole send — redirects and
    /// backoff included — so a younger batch cannot commit ahead of an older
    /// one. Retained per partition ever seen; partition ids are the
    /// cluster's Raft group ids, so the map stays small.
    flush_locks: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
}

impl BatchState {
    pub(crate) fn new(config: BatchConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(BatcherInner::default()),
            flush_locks: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn config(&self) -> &BatchConfig {
        &self.config
    }

    fn flush_lock(&self, partition: u64) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.flush_locks.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(
            locks
                .entry(partition)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }
}

/// What the submitter must do after releasing the batcher lock.
enum PostSubmit {
    /// Nothing — the item waits for a threshold or linger flush.
    Wait,
    /// This submit closed a window: flush the taken items now.
    FlushNow(Vec<PendingItem>, &'static str),
    /// This submit started a fresh batch window: arm the linger timer for
    /// the partition's current flush generation.
    ArmLinger(u64),
}

fn batch_protocol_err(reason: &str) -> RrError {
    RrError::Network(NetworkError::RpcFailed(format!(
        "client batching: {reason}"
    )))
}

/// Re-create `err` for one parked waiter, preserving the error KIND
/// (`Timeout` stays `Timeout`, `NotLeader` stays `NotLeader`, ...) so
/// callers can still match on it; the shared flush error itself is not
/// `Clone`.
fn clone_for_waiter(err: &RrError) -> RrError {
    match err {
        RrError::Network(NetworkError::Timeout) => RrError::Network(NetworkError::Timeout),
        RrError::Network(NetworkError::ConnectionFailed { addr, reason }) => {
            RrError::Network(NetworkError::ConnectionFailed {
                addr: addr.clone(),
                reason: reason.clone(),
            })
        }
        RrError::Network(NetworkError::InvalidConfig(msg)) => {
            RrError::Network(NetworkError::InvalidConfig(msg.clone()))
        }
        RrError::Network(NetworkError::RpcFailed(msg)) => {
            RrError::Network(NetworkError::RpcFailed(msg.clone()))
        }
        RrError::Raft(RaftError::NotLeader {
            leader,
            leader_addr,
        }) => RrError::Raft(RaftError::NotLeader {
            leader: *leader,
            leader_addr: leader_addr.clone(),
        }),
        RrError::IdempotencyExpired {
            request_type,
            request_id,
            watermark,
        } => RrError::IdempotencyExpired {
            request_type: request_type.clone(),
            request_id: *request_id,
            watermark: *watermark,
        },
        other => RrError::Network(NetworkError::RpcFailed(other.to_string())),
    }
}

impl crate::RrClient {
    /// Enqueue `payload` into partition `partition`'s batch and await its
    /// individual response. Drives size-triggered and linger-triggered
    /// flushes; both call back into the normal write path
    /// (`send_request_inner`) so redirect/retry semantics are unchanged.
    ///
    /// Cancellation caveat: dropping the returned future does NOT cancel the
    /// server-side apply — the payload has already joined a batch window and
    /// the detached flush task will still commit it; only the response is
    /// lost.
    pub(crate) async fn batch_submit(
        &self,
        partition: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        let batch = match &self.batch {
            Some(b) => Arc::clone(b),
            None => return Err(batch_protocol_err("batcher not enabled")),
        };
        let config = *batch.config();
        let (tx, rx) = oneshot::channel();
        let payload_len = payload.len();

        let post = {
            let mut guard = batch.inner.lock().unwrap_or_else(|p| p.into_inner());
            if guard.buffered_total >= config.max_buffered {
                metrics::counter!(
                    "rr_client_batch_buffer_rejected_total",
                    "limit" => "entries"
                )
                .increment(1);
                return Err(batch_protocol_err(
                    "batch buffer full (max_buffered) — apply backpressure upstream",
                ));
            }
            if guard.buffered_bytes + payload_len > config.max_buffered_bytes {
                metrics::counter!(
                    "rr_client_batch_buffer_rejected_total",
                    "limit" => "bytes"
                )
                .increment(1);
                return Err(batch_protocol_err(
                    "batch buffer full (max_buffered_bytes) — apply backpressure upstream",
                ));
            }

            let (window_closed, arm_linger) = {
                let acc = guard.accumulators.entry(partition).or_default();
                acc.bytes += payload_len;
                acc.items.push((payload, tx));
                // Kafka linger.ms=0: no wait at all, every write closes its
                // own window and flushes immediately.
                let closed = acc.items.len() >= config.max_entries
                    || acc.bytes >= config.max_bytes
                    || config.linger < IMMEDIATE_LINGER;
                let arm = !closed && !acc.linger_armed;
                if arm {
                    acc.linger_armed = true;
                }
                (closed, arm)
            };
            guard.buffered_total += 1;
            guard.buffered_bytes += payload_len;

            if window_closed {
                // The window is non-empty by construction (just pushed into).
                let items = guard.take_window(partition);
                let trigger = if config.linger < IMMEDIATE_LINGER {
                    "immediate"
                } else {
                    "size"
                };
                PostSubmit::FlushNow(items, trigger)
            } else if arm_linger {
                let generation = *guard.generations.entry(partition).or_insert(0);
                PostSubmit::ArmLinger(generation)
            } else {
                PostSubmit::Wait
            }
        };

        match post {
            PostSubmit::Wait => {}
            PostSubmit::FlushNow(items, trigger) => {
                let client = self.clone();
                tokio::spawn(async move { client.flush_batch(partition, items, trigger).await });
            }
            PostSubmit::ArmLinger(generation) => {
                let client = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(config.linger).await;
                    let taken = batch
                        .inner
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .take_window_if_current(partition, generation);
                    if let Some(items) = taken {
                        client.flush_batch(partition, items, "linger").await;
                    }
                });
            }
        }

        rx.await
            .unwrap_or_else(|_| Err(batch_protocol_err("flush task dropped the waiter")))
    }

    /// Pack the taken items into one batch frame, send it through the normal
    /// write path (leader redirect + retry intact), then resolve each parked
    /// waiter with its positional response. Flushes are serialized per
    /// partition, so a retrying/redirected batch cannot be overtaken by a
    /// younger one. Any failure fails every waiter loudly, preserving the
    /// error kind — never a fake success.
    async fn flush_batch(&self, partition: u64, items: Vec<PendingItem>, trigger: &'static str) {
        let count = items.len();
        metrics::counter!(
            "rr_client_batch_flush_total",
            "partition_id" => partition.to_string(),
            "trigger" => trigger
        )
        .increment(1);
        metrics::histogram!(
            "rr_client_batch_entries",
            "partition_id" => partition.to_string()
        )
        .record(count as f64);

        let (payloads, txs): (Vec<Vec<u8>>, Vec<PendingTx>) = items.into_iter().unzip();

        let fail_all = |txs: Vec<PendingTx>, err: &RrError| {
            tracing::warn!(
                partition_id = partition,
                waiters = txs.len(),
                error = %err,
                "batch flush failed — failing every parked waiter"
            );
            for tx in txs {
                let _ = tx.send(Err(clone_for_waiter(err)));
            }
        };

        let Some(batch) = self.batch.as_ref() else {
            fail_all(txs, &batch_protocol_err("batcher not enabled"));
            return;
        };
        // Kafka max.in.flight=1: hold the partition's flush lock for the
        // whole send, redirects and backoff included.
        let flush_lock = batch.flush_lock(partition);
        let _flush_guard = flush_lock.lock().await;

        let frame = rr_core::batch::encode_batch(&payloads);
        let outcome = self.send_request_inner(partition, frame, None).await;

        match outcome {
            Err(e) => fail_all(txs, &e),
            Ok(bytes) => match rr_core::batch::decode_batch(&bytes) {
                Ok(Some(responses)) if responses.len() == count => {
                    for (tx, resp) in txs.into_iter().zip(responses) {
                        let _ = tx.send(Ok(resp));
                    }
                }
                // A response frame with MORE or FEWER items than waiters has
                // no trustworthy positional mapping — fail everyone loudly
                // instead of delivering some items and truncating the rest.
                Ok(Some(responses)) => fail_all(
                    txs,
                    &batch_protocol_err(&format!(
                        "server returned {} response(s) for a {count}-item batch — \
                         count mismatch; refusing to guess which items committed",
                        responses.len()
                    )),
                ),
                _ => fail_all(
                    txs,
                    &batch_protocol_err(
                        "server did not return a batch response frame — it likely runs an \
                         older framework without batch-apply support; disable batching",
                    ),
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::spawn_fake_client_service;
    use crate::{RetryConfig, RrClientBuilder};
    use rr_network::proto::client::client_service_server::ClientService as ClientServiceTrait;
    use rr_network::proto::client::{
        ClientRequest, ClientResponse, ClusterInfo, Empty, ReadLocalRequest, ReadLocalResponse,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn validate_rejects_zero_knobs() {
        let cfg = BatchConfig {
            max_entries: 0,
            ..BatchConfig::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(RrError::Network(NetworkError::InvalidConfig(_)))
        ));
        let cfg = BatchConfig {
            max_buffered: 0,
            ..BatchConfig::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(RrError::Network(NetworkError::InvalidConfig(_)))
        ));
        let cfg = BatchConfig {
            max_buffered_bytes: 0,
            ..BatchConfig::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(RrError::Network(NetworkError::InvalidConfig(_)))
        ));
        assert!(BatchConfig::default().validate().is_ok());
    }

    /// A linger timer armed under an old flush generation must not flush the
    /// NEW window that opened after an intervening flush — otherwise a stale
    /// sleeping timer fires and flushes the new window early.
    #[test]
    fn stale_linger_timer_cannot_flush_new_window() {
        let mut inner = BatcherInner::default();

        // Window 1 opens under generation 0 with its timer armed.
        let gen0 = *inner.generations.entry(7).or_insert(0);
        let (tx1, _rx1) = oneshot::channel();
        {
            let acc = inner.accumulators.entry(7).or_default();
            acc.bytes += 1;
            acc.items.push((b"a".to_vec(), tx1));
            acc.linger_armed = true;
        }
        inner.buffered_total += 1;
        inner.buffered_bytes += 1;

        // The current timer may take window 1; the drained accumulator must
        // be removed from the map and the generation bumped.
        assert_eq!(
            inner.take_window_if_current(7, gen0).map(|w| w.len()),
            Some(1)
        );
        assert_eq!(inner.generations[&7], gen0 + 1);
        assert!(!inner.accumulators.contains_key(&7));
        assert_eq!(inner.buffered_total, 0);
        assert_eq!(inner.buffered_bytes, 0);

        // Window 2 opens under generation 1.
        let (tx2, _rx2) = oneshot::channel();
        {
            let acc = inner.accumulators.entry(7).or_default();
            acc.bytes += 1;
            acc.items.push((b"b".to_vec(), tx2));
            acc.linger_armed = true;
        }
        inner.buffered_total += 1;
        inner.buffered_bytes += 1;

        // The stale timer from window 1 fires now: it must NOT take window 2.
        assert!(inner.take_window_if_current(7, gen0).is_none());
        assert_eq!(inner.buffered_total, 1, "window 2 must stay buffered");
        // Window 2's own timer may take it.
        assert_eq!(
            inner.take_window_if_current(7, gen0 + 1).map(|w| w.len()),
            Some(1)
        );
        assert!(!inner.accumulators.contains_key(&7));
    }

    /// Kafka linger.ms=0: a ZERO (or sub-millisecond) linger must flush
    /// immediately — a lone write must not park in the buffer forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn linger_zero_flushes_immediately() {
        for linger in [Duration::ZERO, Duration::from_micros(500)] {
            let client = RrClientBuilder::new(vec!["127.0.0.1:1".to_string()])
                .with_retry_config(RetryConfig {
                    max_retries: 0,
                    retry_delay_ms: 1,
                })
                .with_batching(BatchConfig {
                    max_entries: 1000,
                    max_bytes: usize::MAX,
                    linger,
                    max_buffered: 16,
                    max_buffered_bytes: 1 << 20,
                })
                .build()
                .await
                .unwrap();

            let result = tokio::time::timeout(
                Duration::from_secs(5),
                client.send_request(1, b"x".to_vec()),
            )
            .await
            .expect("linger=0 must flush immediately, not park the write");
            assert!(result.is_err(), "dead server must surface an error");
        }
    }

    /// The linger timer must still flush a lone parked write once its window
    /// expires (guards the generation-checked linger path).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn linger_flush_resolves_parked_write() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:1".to_string()])
            .with_retry_config(RetryConfig {
                max_retries: 0,
                retry_delay_ms: 1,
            })
            .with_batching(BatchConfig {
                max_entries: 1000,
                max_bytes: usize::MAX,
                linger: Duration::from_millis(30),
                max_buffered: 16,
                max_buffered_bytes: 1 << 20,
            })
            .build()
            .await
            .unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.send_request(1, b"x".to_vec()),
        )
        .await
        .expect("the linger timer must flush the parked write");
        assert!(result.is_err(), "dead server must surface an error");
    }

    /// Past `max_buffered` the submitter gets a loud, immediate error —
    /// the batcher must never grow memory unboundedly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_buffer_full_fails_fast() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:1".to_string()])
            .with_batching(BatchConfig {
                max_entries: 1000,
                max_bytes: usize::MAX,
                linger: Duration::from_secs(3600),
                max_buffered: 2,
                max_buffered_bytes: 1 << 20,
            })
            .build()
            .await
            .unwrap();

        let c1 = client.clone();
        let t1 = tokio::spawn(async move { c1.send_request(1, b"a".to_vec()).await });
        let c2 = client.clone();
        let t2 = tokio::spawn(async move { c2.send_request(1, b"b".to_vec()).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let third = client.send_request(1, b"c".to_vec()).await;
        let err = third.expect_err("third submit must be rejected by the buffer cap");
        assert!(
            err.to_string().contains("buffer full"),
            "unexpected error: {err}"
        );
        t1.abort();
        t2.abort();
    }

    /// The byte cap fails fast alongside the entry cap: a submit that would
    /// push the buffered bytes past `max_buffered_bytes` is rejected
    /// immediately, and an oversized payload is rejected even into an empty
    /// buffer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_buffer_full_bytes_fails_fast() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:1".to_string()])
            .with_batching(BatchConfig {
                max_entries: 1000,
                max_bytes: usize::MAX,
                linger: Duration::from_secs(3600),
                max_buffered: 100,
                max_buffered_bytes: 10,
            })
            .build()
            .await
            .unwrap();

        // 6 bytes park in the buffer.
        let c1 = client.clone();
        let t1 = tokio::spawn(async move { c1.send_request(1, b"123456".to_vec()).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 6 + 6 crosses the 10-byte cap.
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            client.send_request(1, b"abcdef".to_vec()),
        )
        .await
        .expect("byte-cap rejection must be immediate")
        .expect_err("submit crossing max_buffered_bytes must be rejected");
        assert!(
            err.to_string().contains("max_buffered_bytes"),
            "unexpected error: {err}"
        );

        // An 11-byte payload can never fit under a 10-byte cap.
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            client.send_request(1, vec![0u8; 11]),
        )
        .await
        .expect("byte-cap rejection must be immediate")
        .expect_err("oversized payload must be rejected");
        assert!(
            err.to_string().contains("max_buffered_bytes"),
            "unexpected error: {err}"
        );
        t1.abort();
    }

    /// A size-triggered flush against a dead server must fail every parked
    /// waiter loudly (no hang, no fake success).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn threshold_flush_failure_fails_all_waiters() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:1".to_string()])
            .with_retry_config(RetryConfig {
                max_retries: 0,
                retry_delay_ms: 1,
            })
            .with_batching(BatchConfig {
                max_entries: 2,
                max_bytes: usize::MAX,
                linger: Duration::from_secs(3600),
                max_buffered: 16,
                max_buffered_bytes: 1 << 20,
            })
            .build()
            .await
            .unwrap();

        let c1 = client.clone();
        let t1 = tokio::spawn(async move { c1.send_request(1, b"a".to_vec()).await });
        let c2 = client.clone();
        let t2 = tokio::spawn(async move { c2.send_request(1, b"b".to_vec()).await });

        let r1 = tokio::time::timeout(Duration::from_secs(10), t1)
            .await
            .expect("waiter 1 must resolve")
            .unwrap();
        let r2 = tokio::time::timeout(Duration::from_secs(10), t2)
            .await
            .expect("waiter 2 must resolve")
            .unwrap();
        assert!(r1.is_err(), "dead server must surface an error");
        assert!(r2.is_err(), "dead server must surface an error");
    }

    /// Keyed (idempotent) writes bypass the batcher even when enabled —
    /// they hit the direct path and fail fast against a dead server
    /// instead of parking in the buffer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_writes_bypass_the_batcher() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:1".to_string()])
            .with_retry_config(RetryConfig {
                max_retries: 0,
                retry_delay_ms: 1,
            })
            .with_batching(BatchConfig {
                max_entries: 1000,
                max_bytes: usize::MAX,
                linger: Duration::from_secs(3600),
                max_buffered: 16,
                max_buffered_bytes: 1 << 20,
            })
            .build()
            .await
            .unwrap();

        let key = rr_core::idempotency::IdempotencyKey::new("t", 1);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.send_request_idempotent(1, b"k".to_vec(), key),
        )
        .await
        .expect("keyed write must not park in the batch buffer")
        .expect_err("dead server must surface an error");
        assert!(result.to_string().contains("network error"));
    }

    // ── Fake-transport scenarios (in-process tonic server) ─────────────────

    fn batch_test_config(max_entries: usize, linger: Duration) -> BatchConfig {
        BatchConfig {
            max_entries,
            max_bytes: usize::MAX,
            linger,
            max_buffered: 64,
            max_buffered_bytes: 1 << 20,
        }
    }

    fn batch_test_builder(addr: String, config: BatchConfig) -> RrClientBuilder {
        RrClientBuilder::new(vec![addr])
            .with_retry_config(RetryConfig {
                max_retries: 0,
                retry_delay_ms: 1,
            })
            .with_batching(config)
    }

    /// Fake node that decodes each incoming batch frame and echoes the items
    /// back as the response frame. The FIRST request parks on `gate` until
    /// the test opens it — driving two overlapping in-flight flushes.
    struct GatedBatchService {
        arrivals: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
        commits: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
        gate: Arc<tokio::sync::Notify>,
        gated: AtomicBool,
    }

    #[tonic::async_trait]
    impl ClientServiceTrait for GatedBatchService {
        async fn request(
            &self,
            request: tonic::Request<ClientRequest>,
        ) -> Result<tonic::Response<ClientResponse>, tonic::Status> {
            let payload = request.into_inner().payload;
            let items = rr_core::batch::decode_batch(&payload)
                .map_err(|e| tonic::Status::internal(e.to_string()))?
                .ok_or_else(|| tonic::Status::internal("expected a batch frame"))?;
            let tag = items.first().cloned().unwrap_or_default();
            let _ = self.arrivals.send(tag.clone());
            if !self.gated.swap(true, Ordering::SeqCst) {
                self.gate.notified().await;
            }
            let _ = self.commits.send(tag);
            Ok(tonic::Response::new(ClientResponse {
                success: true,
                payload: rr_core::batch::encode_batch(&items),
                leader_info: None,
                idem_error: None,
            }))
        }

        async fn read_local(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn read_linearizable(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn get_cluster_info(
            &self,
            _request: tonic::Request<Empty>,
        ) -> Result<tonic::Response<ClusterInfo>, tonic::Status> {
            Ok(tonic::Response::new(ClusterInfo { groups: vec![] }))
        }
    }

    /// Two overlapping flushes of the same partition must commit in
    /// submission order: a younger batch cannot overtake an older one that
    /// is still in flight (Kafka `max.in.flight=1` analogue).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overlapping_flushes_commit_in_submission_order() {
        let (arrivals_tx, mut arrivals_rx) = tokio::sync::mpsc::unbounded_channel();
        let (commits_tx, mut commits_rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(tokio::sync::Notify::new());
        let svc = GatedBatchService {
            arrivals: arrivals_tx,
            commits: commits_tx,
            gate: Arc::clone(&gate),
            gated: AtomicBool::new(false),
        };
        let (addr, server) = spawn_fake_client_service(svc).await;

        // max_entries=1: every write closes its own window → the two
        // submissions produce two overlapping flush tasks.
        let builder = batch_test_builder(addr, batch_test_config(1, Duration::from_secs(3600)));
        let client = builder.build().await.unwrap();

        let c = client.clone();
        let a = tokio::spawn(async move { c.send_request(1, b"a".to_vec()).await });
        // Flush A reached the server and is now parked on the gate.
        let arrived = tokio::time::timeout(Duration::from_secs(5), arrivals_rx.recv())
            .await
            .expect("flush A must reach the server")
            .expect("arrivals channel open");
        assert_eq!(arrived, b"a".to_vec());

        let c = client.clone();
        let b = tokio::spawn(async move { c.send_request(1, b"b".to_vec()).await });
        // Give flush B ample time to reach the server; with per-partition
        // flush serialization it must instead wait behind A.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            arrivals_rx.try_recv().is_err(),
            "flush B overtook flush A — per-partition ordering is broken"
        );

        gate.notify_one();
        let ra = tokio::time::timeout(Duration::from_secs(5), a)
            .await
            .expect("flush A must resolve")
            .unwrap()
            .expect("flush A must succeed");
        let rb = tokio::time::timeout(Duration::from_secs(5), b)
            .await
            .expect("flush B must resolve")
            .unwrap()
            .expect("flush B must succeed");
        assert_eq!(ra, b"a".to_vec());
        assert_eq!(rb, b"b".to_vec());

        let first = tokio::time::timeout(Duration::from_secs(5), commits_rx.recv())
            .await
            .expect("commit A recorded");
        let second = tokio::time::timeout(Duration::from_secs(5), commits_rx.recv())
            .await
            .expect("commit B recorded");
        assert_eq!(first, Some(b"a".to_vec()), "commits must be ordered");
        assert_eq!(second, Some(b"b".to_vec()), "commits must be ordered");
        server.abort();
    }

    /// Fake node that answers with a batch frame whose item count is off by
    /// `delta` from the request's.
    struct MiscountingService {
        delta: isize,
    }

    #[tonic::async_trait]
    impl ClientServiceTrait for MiscountingService {
        async fn request(
            &self,
            request: tonic::Request<ClientRequest>,
        ) -> Result<tonic::Response<ClientResponse>, tonic::Status> {
            let payload = request.into_inner().payload;
            let items = rr_core::batch::decode_batch(&payload)
                .map_err(|e| tonic::Status::internal(e.to_string()))?
                .ok_or_else(|| tonic::Status::internal("expected a batch frame"))?;
            let mut responses = items.clone();
            if self.delta < 0 {
                responses.truncate(responses.len().saturating_sub(self.delta.unsigned_abs()));
            } else if self.delta > 0 {
                for _ in 0..self.delta {
                    responses.push(b"extra".to_vec());
                }
            }
            Ok(tonic::Response::new(ClientResponse {
                success: true,
                payload: rr_core::batch::encode_batch(&responses),
                leader_info: None,
                idem_error: None,
            }))
        }

        async fn read_local(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn read_linearizable(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn get_cluster_info(
            &self,
            _request: tonic::Request<Empty>,
        ) -> Result<tonic::Response<ClusterInfo>, tonic::Status> {
            Ok(tonic::Response::new(ClusterInfo { groups: vec![] }))
        }
    }

    /// A response frame whose item count disagrees with the waiter count —
    /// fewer OR more — has no trustworthy positional mapping: every waiter
    /// must fail loudly, none may get a fake/partial success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn response_count_mismatch_fails_all_waiters() {
        for delta in [-1isize, 1] {
            let (addr, server) = spawn_fake_client_service(MiscountingService { delta }).await;
            let builder = batch_test_builder(addr, batch_test_config(2, Duration::from_secs(3600)));
            let client = builder.build().await.unwrap();

            let c1 = client.clone();
            let t1 = tokio::spawn(async move { c1.send_request(1, b"a".to_vec()).await });
            let c2 = client.clone();
            let t2 = tokio::spawn(async move { c2.send_request(1, b"b".to_vec()).await });

            let r1 = tokio::time::timeout(Duration::from_secs(10), t1)
                .await
                .expect("waiter 1 must resolve")
                .unwrap();
            let r2 = tokio::time::timeout(Duration::from_secs(10), t2)
                .await
                .expect("waiter 2 must resolve")
                .unwrap();
            let e1 = r1.expect_err("count mismatch must fail waiter 1");
            let e2 = r2.expect_err("count mismatch must fail waiter 2");
            assert!(
                e1.to_string().contains("count mismatch"),
                "unexpected error: {e1}"
            );
            assert!(
                e2.to_string().contains("count mismatch"),
                "unexpected error: {e2}"
            );
            server.abort();
        }
    }

    /// Fake node that never answers, driving the client's per-attempt
    /// request timeout.
    struct ParkingService;

    #[tonic::async_trait]
    impl ClientServiceTrait for ParkingService {
        async fn request(
            &self,
            _request: tonic::Request<ClientRequest>,
        ) -> Result<tonic::Response<ClientResponse>, tonic::Status> {
            std::future::pending::<Result<tonic::Response<ClientResponse>, tonic::Status>>().await
        }

        async fn read_local(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn read_linearizable(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn get_cluster_info(
            &self,
            _request: tonic::Request<Empty>,
        ) -> Result<tonic::Response<ClusterInfo>, tonic::Status> {
            Ok(tonic::Response::new(ClusterInfo { groups: vec![] }))
        }
    }

    /// A timed-out flush must surface `NetworkError::Timeout` to EVERY
    /// waiter, not a rewrapped string — callers match on the error kind for
    /// their own retry/policy logic.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_failure_preserves_error_kind() {
        let (addr, server) = spawn_fake_client_service(ParkingService).await;
        let client = batch_test_builder(addr, batch_test_config(2, Duration::from_secs(3600)))
            .with_request_timeout(Duration::from_millis(50))
            .build()
            .await
            .unwrap();

        let c1 = client.clone();
        let t1 = tokio::spawn(async move { c1.send_request(1, b"a".to_vec()).await });
        let c2 = client.clone();
        let t2 = tokio::spawn(async move { c2.send_request(1, b"b".to_vec()).await });

        for (name, t) in [("waiter 1", t1), ("waiter 2", t2)] {
            let result = tokio::time::timeout(Duration::from_secs(10), t)
                .await
                .unwrap_or_else(|_| panic!("{name} must resolve"));
            let err = result
                .unwrap()
                .expect_err("timed-out flush must fail loudly");
            assert!(
                matches!(err, RrError::Network(NetworkError::Timeout)),
                "{name}: error kind must survive fan-out, got: {err}"
            );
        }
        server.abort();
    }
}
