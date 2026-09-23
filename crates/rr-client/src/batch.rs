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
//! - [`BatchConfig::linger`] — max wait of the first queued write (linger.ms);
//! - [`BatchConfig::max_buffered`] — total buffered-writes cap across
//!   partitions; past it, submits fail fast (backpressure).
//!
//! Requires every server to run a framework version whose apply path splits
//! batch frames; an older server echoes the frame into the service as one
//! opaque payload, and this client fails the batch loudly on the missing
//! response frame instead of reporting fake successes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rr_core::error::{NetworkError, RrError};
use tokio::sync::oneshot;

/// Kafka-producer-style batching knobs. Batching is OFF unless explicitly
/// enabled; with no batching the write path is byte-for-byte the v0.2.x one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchConfig {
    /// Flush a partition's buffer when this many writes have accumulated.
    pub max_entries: usize,
    /// Flush a partition's buffer when accumulated payload bytes reach this.
    pub max_bytes: usize,
    /// Maximum time the first queued write of a batch waits before the batch
    /// flushes anyway (Kafka `linger.ms`). `Duration::ZERO` disables the
    /// linger path — batches flush only on size thresholds.
    pub linger: Duration,
    /// Backpressure cap on the TOTAL buffered-but-unflushed writes across
    /// all partitions (Kafka `buffer.memory`, in entries). Submitting past
    /// the cap fails fast instead of growing memory unboundedly.
    pub max_buffered: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_entries: 64,
            max_bytes: 1 << 20, // 1 MiB
            linger: Duration::from_millis(5),
            max_buffered: 8192,
        }
    }
}

impl BatchConfig {
    /// Validate the knobs: zero `max_entries` / `max_buffered` would wedge
    /// (nothing ever flushes / nothing is ever accepted).
    pub fn validate(&self) -> Result<(), RrError> {
        if self.max_entries == 0 {
            return Err(RrError::Network(NetworkError::RpcFailed(
                "batching: max_entries must be > 0".to_string(),
            )));
        }
        if self.max_buffered == 0 {
            return Err(RrError::Network(NetworkError::RpcFailed(
                "batching: max_buffered must be > 0".to_string(),
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
    linger_armed: bool,
}

#[derive(Default)]
struct BatcherInner {
    accumulators: HashMap<u64, Accumulator>,
    /// Total buffered-but-unflushed writes across partitions.
    buffered_total: usize,
}

/// Shared state of the client batcher. Owned by `RrClient`; flush tasks hold
/// an `RrClient` clone, never the reverse — no reference cycle.
pub(crate) struct BatchState {
    config: BatchConfig,
    inner: Mutex<BatcherInner>,
}

impl BatchState {
    pub(crate) fn new(config: BatchConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(BatcherInner::default()),
        }
    }

    pub(crate) fn config(&self) -> &BatchConfig {
        &self.config
    }
}

/// What the submitter must do after releasing the batcher lock.
enum PostSubmit {
    /// Nothing — the item waits for a threshold or linger flush.
    Wait,
    /// This submit crossed a size threshold: flush the taken items now.
    FlushNow(Vec<PendingItem>),
    /// This submit started a fresh batch window: arm the linger timer.
    ArmLinger,
}

fn batch_protocol_err(reason: &str) -> RrError {
    RrError::Network(NetworkError::RpcFailed(format!(
        "client batching: {reason}"
    )))
}

impl crate::RrClient {
    /// Enqueue `payload` into partition `partition`'s batch and await its
    /// individual response. Drives size-triggered and linger-triggered
    /// flushes; both call back into the normal write path
    /// (`send_request_inner`) so redirect/retry semantics are unchanged.
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

        let post = {
            let mut guard = batch.inner.lock().unwrap_or_else(|p| p.into_inner());
            let BatcherInner {
                accumulators,
                buffered_total,
            } = &mut *guard;
            if *buffered_total >= config.max_buffered {
                return Err(batch_protocol_err(
                    "batch buffer full (max_buffered) — apply backpressure upstream",
                ));
            }
            let acc = accumulators.entry(partition).or_default();
            acc.bytes += payload.len();
            acc.items.push((payload, tx));
            *buffered_total += 1;

            if acc.items.len() >= config.max_entries || acc.bytes >= config.max_bytes {
                let items = std::mem::take(&mut acc.items);
                acc.bytes = 0;
                acc.linger_armed = false;
                *buffered_total -= items.len();
                PostSubmit::FlushNow(items)
            } else if !acc.linger_armed && config.linger > Duration::ZERO {
                acc.linger_armed = true;
                PostSubmit::ArmLinger
            } else {
                PostSubmit::Wait
            }
        };

        match post {
            PostSubmit::Wait => {}
            PostSubmit::FlushNow(items) => {
                let client = self.clone();
                tokio::spawn(async move { client.flush_batch(partition, items, "size").await });
            }
            PostSubmit::ArmLinger => {
                let client = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(config.linger).await;
                    let taken = {
                        let mut guard = batch.inner.lock().unwrap_or_else(|p| p.into_inner());
                        let BatcherInner {
                            accumulators,
                            buffered_total,
                        } = &mut *guard;
                        let acc = accumulators.entry(partition).or_default();
                        acc.linger_armed = false;
                        if acc.items.is_empty() {
                            None
                        } else {
                            let items = std::mem::take(&mut acc.items);
                            acc.bytes = 0;
                            *buffered_total -= items.len();
                            Some(items)
                        }
                    };
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
    /// waiter with its positional response. Any failure fails every waiter
    /// loudly — never a fake success.
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
        let frame = rr_core::batch::encode_batch(&payloads);
        let outcome = self.send_request_inner(partition, frame, None).await;

        let fail_all = |txs: Vec<PendingTx>, err: RrError| {
            let msg = err.to_string();
            for tx in txs {
                let _ = tx.send(Err(RrError::Network(NetworkError::RpcFailed(msg.clone()))));
            }
        };

        match outcome {
            Err(e) => fail_all(txs, e),
            Ok(bytes) => match rr_core::batch::decode_batch(&bytes) {
                Ok(Some(responses)) => {
                    let mut responses = responses.into_iter();
                    for tx in txs {
                        match responses.next() {
                            Some(resp) => {
                                let _ = tx.send(Ok(resp));
                            }
                            None => {
                                let _ = tx.send(Err(batch_protocol_err(
                                    "server returned fewer responses than batch items",
                                )));
                            }
                        }
                    }
                }
                _ => fail_all(
                    txs,
                    batch_protocol_err(
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
    use crate::{RetryConfig, RrClientBuilder};

    #[test]
    fn validate_rejects_zero_knobs() {
        let cfg = BatchConfig {
            max_entries: 0,
            ..BatchConfig::default()
        };
        assert!(cfg.validate().is_err());
        let cfg = BatchConfig {
            max_buffered: 0,
            ..BatchConfig::default()
        };
        assert!(cfg.validate().is_err());
        assert!(BatchConfig::default().validate().is_ok());
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
}
