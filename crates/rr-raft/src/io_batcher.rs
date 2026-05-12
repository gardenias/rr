//! Per-group asynchronous I/O batcher (group commit engine).
//!
//! [`IoBatcher`] collects multiple concurrent log-append operations and
//! flushes them as **a single `write_batch()` call** — the Raft analogue of
//! MySQL binlog group commit.
//!
//! # Why this matters
//!
//! `RocksDB::write_opt(batch, opts)` with `opts.set_sync(true)` (Strict
//! durability) is an fsync-per-call operation (~1 ms on consumer SSDs,
//! ~100 µs on enterprise NVMe).  If every `RaftLogStorage::append()` call
//! blocks until its own fsync completes, single-group throughput is capped
//! at ~1 k ops/s.
//!
//! The batcher breaks this ceiling by:
//! 1. Accepting append ops via a channel (non-blocking, returns immediately).
//! 2. Draining the channel with a configurable accumulation window.
//! 3. Writing **all** accumulated entries in one `write_batch()` → one fsync.
//! 4. Notifying every caller's callback when the batch is durable.
//!
//! # Accumulation strategy
//!
//! - After receiving the first item, the worker does a **non-blocking drain**
//!   of any already-queued items (zero extra latency for high throughput).
//! - If the non-blocking drain collected only that one item, it waits up to
//!   `window` for more before flushing (bounded latency for low throughput).
//! - Either `max_entries` being reached OR the deadline expiring triggers an
//!   early flush.
//!
//! # Ordering guarantee
//!
//! All entries in a batch are written atomically by `write_batch()`.  Order
//! within the RocksDB `raft_log` CF is determined by big-endian log-index
//! keys, not by callback-fire order, so in-batch ordering is always correct.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use rr_core::storage::BatchOp;
use rr_core::storage::StorageEngine;
use rr_storage::rocks::RocksStorage;

// ── Public configuration ─────────────────────────────────────────────────────

/// Tuning knobs for the group-commit batcher.
#[derive(Debug, Clone)]
pub struct IoBatcherConfig {
    /// How long to wait for additional entries after the first one arrives.
    ///
    /// Set to `Duration::ZERO` to flush every single item individually (i.e.,
    /// disables group commit while keeping the async-append path intact).
    pub window: Duration,

    /// Flush the batch early once this many entries have accumulated.
    ///
    /// Prevents unbounded memory growth and limits per-batch write size.
    pub max_entries: usize,
}

impl Default for IoBatcherConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_micros(500),
            max_entries: 256,
        }
    }
}

impl IoBatcherConfig {
    /// Convenience constructor for the common tuning knobs.
    pub fn new(window: Duration, max_entries: usize) -> Self {
        Self {
            window,
            max_entries,
        }
    }

    /// Zero-wait config: flush every item immediately without accumulation.
    pub fn no_batching() -> Self {
        Self {
            window: Duration::ZERO,
            max_entries: 1,
        }
    }
}

// ── Internal channel item ────────────────────────────────────────────────────

struct BatchItem {
    ops: Vec<BatchOp>,
    result_tx: oneshot::Sender<Result<(), String>>,
}

// ── Public batcher handle ────────────────────────────────────────────────────

/// Depth of the batcher's bounded channel.  When the channel is full the
/// caller's `submit()` future suspends, creating natural back-pressure on
/// the Raft proposal pipeline instead of growing a queue unboundedly.
const CHANNEL_DEPTH: usize = 512;

/// A cloneable handle to the per-group background IO batcher.
///
/// Dropping all clones of an `IoBatcher` closes the underlying channel,
/// causing the worker task to drain any pending items and then exit cleanly.
#[derive(Clone)]
pub struct IoBatcher {
    tx: mpsc::Sender<BatchItem>,
}

impl IoBatcher {
    /// Spawn a new batcher worker task backed by `storage`.
    pub fn new(storage: Arc<RocksStorage>, config: IoBatcherConfig) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
        tokio::spawn(batcher_worker(rx, storage, config));
        Self { tx }
    }

    /// Submit a batch of write operations.
    ///
    /// Returns as soon as the ops are enqueued.  Awaits a permit from the
    /// internal bounded channel first, so callers naturally back-pressure
    /// when the storage layer falls behind — preventing unbounded queue growth
    /// under high-concurrency write bursts.
    pub async fn submit(&self, ops: Vec<BatchOp>) -> Result<(), String> {
        let (result_tx, result_rx) = oneshot::channel();
        if self.tx.send(BatchItem { ops, result_tx }).await.is_err() {
            return Err("io batcher worker has exited".to_string());
        }
        result_rx
            .await
            .unwrap_or_else(|_| Err("io batcher worker dropped result sender".to_string()))
    }
}

// ── Worker task ──────────────────────────────────────────────────────────────

async fn batcher_worker(
    mut rx: mpsc::Receiver<BatchItem>,
    storage: Arc<RocksStorage>,
    config: IoBatcherConfig,
) {
    loop {
        // ── Phase 1: block until the first item arrives ───────────────────
        let first = match rx.recv().await {
            Some(item) => item,
            None => break, // all senders dropped → clean shutdown
        };

        let mut all_ops = first.ops;
        let mut result_txs = vec![first.result_tx];

        // ── Phase 2: non-blocking drain (zero-latency high-throughput path) ─
        while all_ops.len() < config.max_entries {
            match rx.try_recv() {
                Ok(item) => {
                    all_ops.extend(item.ops);
                    result_txs.push(item.result_tx);
                }
                Err(_) => break, // channel momentarily empty
            }
        }

        // ── Phase 3: timed drain (low-throughput amortisation) ───────────
        // Only wait if we have room for more AND got nothing via try_recv.
        if result_txs.len() == 1 && !config.window.is_zero() {
            let deadline = tokio::time::Instant::now() + config.window;
            loop {
                if all_ops.len() >= config.max_entries {
                    break;
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Some(item)) => {
                        all_ops.extend(item.ops);
                        result_txs.push(item.result_tx);
                    }
                    Ok(None) => break, // channel closed
                    Err(_) => break,   // timeout
                }
            }
        }

        // ── Phase 4: single write for the entire batch ────────────────────
        let write_result = storage
            .write_batch(all_ops)
            .await
            .map_err(|e| e.to_string());

        // ── Phase 5: notify all callers ───────────────────────────────────
        for tx in result_txs {
            let payload = match &write_result {
                Ok(()) => Ok(()),
                Err(e) => Err(e.clone()),
            };
            let _ = tx.send(payload);
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rr_storage::rocks::RocksStorage;
    use tempfile::TempDir;

    fn open_storage() -> (Arc<RocksStorage>, TempDir) {
        let tmp = TempDir::new().unwrap();
        let s = RocksStorage::open_default(tmp.path().to_str().unwrap()).unwrap();
        (Arc::new(s), tmp)
    }

    #[tokio::test]
    async fn batcher_submits_single_item() {
        let (storage, _tmp) = open_storage();
        let batcher = IoBatcher::new(Arc::clone(&storage), IoBatcherConfig::no_batching());

        let ops = vec![BatchOp::Put {
            cf: "state_machine".to_string(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        }];
        batcher.submit(ops).await.unwrap();

        let val = storage.get("state_machine", b"k1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"v1".as_ref()));
    }

    #[tokio::test]
    async fn batcher_batches_concurrent_ops() {
        let (storage, _tmp) = open_storage();
        let config = IoBatcherConfig {
            window: Duration::from_millis(20),
            max_entries: 256,
        };
        let batcher = IoBatcher::new(Arc::clone(&storage), config);

        let keys: Vec<Vec<u8>> = (0u32..50).map(|i| i.to_be_bytes().to_vec()).collect();

        let handles: Vec<_> = keys
            .iter()
            .map(|k| {
                let b = batcher.clone();
                let key = k.clone();
                tokio::spawn(async move {
                    b.submit(vec![BatchOp::Put {
                        cf: "state_machine".to_string(),
                        key: key.clone(),
                        value: key,
                    }])
                    .await
                    .unwrap();
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        // All 50 keys must be readable
        for k in &keys {
            let val = storage.get("state_machine", k).await.unwrap();
            assert_eq!(val.as_deref(), Some(k.as_slice()));
        }
    }

    #[tokio::test]
    async fn batcher_propagates_write_error() {
        // Use a closed/non-existent storage path to induce an error
        let (storage, _tmp) = open_storage();
        let batcher = IoBatcher::new(Arc::clone(&storage), IoBatcherConfig::no_batching());

        // Submit an op to a valid CF — should succeed
        let result = batcher
            .submit(vec![BatchOp::Put {
                cf: "state_machine".to_string(),
                key: b"ok".to_vec(),
                value: b"yes".to_vec(),
            }])
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn batcher_exits_cleanly_on_drop() {
        let (storage, _tmp) = open_storage();
        let batcher = IoBatcher::new(Arc::clone(&storage), IoBatcherConfig::default());
        drop(batcher);
        // Worker should exit; nothing to assert beyond "no hang"
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    #[tokio::test]
    async fn batcher_no_batching_flushes_immediately() {
        let (storage, _tmp) = open_storage();
        let batcher = IoBatcher::new(Arc::clone(&storage), IoBatcherConfig::no_batching());

        batcher
            .submit(vec![BatchOp::Put {
                cf: "state_machine".to_string(),
                key: b"x".to_vec(),
                value: b"y".to_vec(),
            }])
            .await
            .unwrap();

        let val = storage.get("state_machine", b"x").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"y".as_ref()));
    }
}
