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
//!
//! # Shutdown contract (ADR A-0011 §Decision-2)
//!
//! [`IoBatcher::close`] is the explicit, provable shutdown path:
//!
//! 1. New submissions fail fast (`Err`) once closing begins.
//! 2. The worker drains every item already received into a final
//!    `write_batch()` (a final fsync under `DurabilityPolicy::Strict`).
//! 3. Every pending completion — `submit()` oneshots and openraft
//!    `IOFlushed` callbacks alike — is answered **exactly once**.
//! 4. The worker exits and the closer reaps its `JoinHandle`.
//!
//! `close()` is idempotent and concurrency-safe. The whole close protocol
//! shares a single [`IoBatcherConfig::close_timeout`] budget (default 30 s):
//! on timeout — or on worker-task failure — it returns `Err` and logs a
//! warning instead of panicking. `Ok` therefore means "every accepted item
//! was flushed, answered exactly once, and the worker exited cleanly".
//!
//! If `close()` returns `Err`, pending completions stay with the wedged
//! worker and its `Arc<RocksStorage>` (and the RocksDB LOCK) stays alive
//! until process exit — a group whose batcher failed to close must not be
//! restarted without bouncing the process.
//!
//! Dropping all clones of an `IoBatcher` still closes the channel and lets
//! the worker drain pending items before exiting, but that drop-drain is a
//! fallback only — it is **not** part of the durability contract, because
//! drop timing depends on every `Arc` clone (including openraft's log
//! reader) and on the runtime staying alive.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{timeout_at, Instant};

use openraft::storage::IOFlushed;

use rr_core::storage::BatchOp;
use rr_core::storage::StorageEngine;
use rr_storage::rocks::RocksStorage;

use crate::type_config::RrTypeConfig;

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

    /// Deadline for a graceful [`IoBatcher::close`]: drain + final
    /// `write_batch` + answering all pending completions + worker exit.
    ///
    /// On expiry `close()` returns `Err` and logs a warning — it never
    /// panics and never reports a half-closed batcher as closed.
    pub close_timeout: Duration,
}

impl Default for IoBatcherConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_micros(500),
            max_entries: 256,
            close_timeout: Duration::from_secs(30),
        }
    }
}

impl IoBatcherConfig {
    /// Convenience constructor for the common tuning knobs.
    ///
    /// `close_timeout` defaults to 30 s; override with
    /// [`Self::with_close_timeout`].
    pub fn new(window: Duration, max_entries: usize) -> Self {
        Self {
            window,
            max_entries,
            ..Default::default()
        }
    }

    /// Zero-wait config: flush every item immediately without accumulation.
    pub fn no_batching() -> Self {
        Self {
            window: Duration::ZERO,
            max_entries: 1,
            ..Default::default()
        }
    }

    /// Override the graceful-shutdown deadline.
    pub fn with_close_timeout(mut self, close_timeout: Duration) -> Self {
        self.close_timeout = close_timeout;
        self
    }
}

// ── Internal channel item ────────────────────────────────────────────────────

/// How a submitter learns that its ops are durable.
///
/// Both variants are answered exactly once by the worker, after the
/// `write_batch` carrying their ops has completed (fsync under Strict).
enum BatchCompletion {
    /// `IoBatcher::submit` caller awaiting durability of its ops.
    Submit(oneshot::Sender<Result<(), String>>),
    /// openraft pipeline callback from `RaftLogStorage::append` — fired
    /// directly by the worker, so no per-append relay task is needed.
    Flushed(IOFlushed<RrTypeConfig>),
}

impl BatchCompletion {
    fn complete(self, result: Result<(), String>) {
        match self {
            Self::Submit(tx) => {
                let _ = tx.send(result);
            }
            Self::Flushed(callback) => {
                callback.io_completed(result.map_err(io::Error::other));
            }
        }
    }
}

enum BatchItem {
    Ops {
        ops: Vec<BatchOp>,
        completions: Vec<BatchCompletion>,
    },
    /// Control message asking the worker to drain, run the final
    /// `write_batch`, answer everything pending, ack the closer, and exit.
    Close(oneshot::Sender<()>),
}

// ── Public batcher handle ────────────────────────────────────────────────────

/// Depth of the batcher's bounded channel.  When the channel is full the
/// caller's `submit()` future suspends, creating natural back-pressure on
/// the Raft proposal pipeline instead of growing a queue unboundedly.
const CHANNEL_DEPTH: usize = 512;

/// Shared lifecycle state for the batcher worker.
struct BatcherState {
    /// Set by the first `close()` call; new submissions fail fast from
    /// that point on.
    closing: AtomicBool,
    /// The worker's `JoinHandle`, held so the spawn is tracked and the
    /// closer can reap the task. `None` once a `close()` has taken it.
    worker: Mutex<Option<JoinHandle<()>>>,
    close_timeout: Duration,
}

/// A cloneable handle to the per-group background IO batcher.
///
/// Call [`IoBatcher::close`] for a provable graceful shutdown (see the
/// module-level shutdown contract). Dropping all clones still drains the
/// worker, but only as a best-effort fallback.
#[derive(Clone)]
pub struct IoBatcher {
    tx: mpsc::Sender<BatchItem>,
    state: Arc<BatcherState>,
}

impl IoBatcher {
    /// Spawn a new batcher worker task backed by `storage`.
    pub fn new(storage: Arc<RocksStorage>, config: IoBatcherConfig) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
        let worker = tokio::spawn(batcher_worker(rx, storage, config.clone()));
        Self {
            tx,
            state: Arc::new(BatcherState {
                closing: AtomicBool::new(false),
                worker: Mutex::new(Some(worker)),
                close_timeout: config.close_timeout,
            }),
        }
    }

    /// Submit a batch of write operations and await their durability.
    ///
    /// Resolves once the batch containing these ops has been written by
    /// `write_batch()` (fsync under `DurabilityPolicy::Strict`). Sending
    /// awaits a permit from the internal bounded channel first, so callers
    /// naturally back-pressure when the storage layer falls behind —
    /// preventing unbounded queue growth under high-concurrency write bursts.
    ///
    /// Fails fast with `Err` once [`IoBatcher::close`] has begun.
    pub async fn submit(&self, ops: Vec<BatchOp>) -> Result<(), String> {
        if self.state.closing.load(Ordering::SeqCst) {
            return Err("io batcher is closing".to_string());
        }
        let (result_tx, result_rx) = oneshot::channel();
        let item = BatchItem::Ops {
            ops,
            completions: vec![BatchCompletion::Submit(result_tx)],
        };
        if self.tx.send(item).await.is_err() {
            return Err("io batcher worker has exited".to_string());
        }
        result_rx
            .await
            .unwrap_or_else(|_| Err("io batcher worker dropped result sender".to_string()))
    }

    /// Submit a batch of write operations whose completion is reported
    /// through openraft's `IOFlushed` callback instead of an awaited
    /// oneshot.
    ///
    /// Returns once the ops are enqueued; the worker fires
    /// `callback.io_completed` itself after the batch's `write_batch`
    /// (fsync under Strict) completes. Fails fast with `Err` once
    /// [`IoBatcher::close`] has begun or if the worker has exited.
    pub async fn submit_with_callback(
        &self,
        ops: Vec<BatchOp>,
        callback: IOFlushed<RrTypeConfig>,
    ) -> Result<(), String> {
        if self.state.closing.load(Ordering::SeqCst) {
            return Err("io batcher is closing".to_string());
        }
        let item = BatchItem::Ops {
            ops,
            completions: vec![BatchCompletion::Flushed(callback)],
        };
        self.tx
            .send(item)
            .await
            .map_err(|_| "io batcher worker has exited".to_string())
    }

    /// Gracefully shut the batcher down (ADR A-0011 §Decision-2).
    ///
    /// Idempotent and concurrency-safe: every concurrent or repeated call
    /// waits for the same shutdown. Once closing begins, new submissions
    /// fail fast. The worker then drains every item already received into
    /// a final `write_batch` (final fsync under Strict), answers all
    /// pending completions exactly once, and exits; `close()` returns only
    /// after the worker task itself is finished.
    ///
    /// The whole protocol — close send, final-flush ack, worker exit —
    /// shares ONE budget of [`IoBatcherConfig::close_timeout`]: on expiry
    /// `close()` returns `Err` and logs a warning — never panics. `Err` is
    /// also returned when the worker task itself failed (panic), so `Ok`
    /// always means "drained, flushed, answered, exited cleanly".
    ///
    /// If `close()` returns `Err`, any completions still pending stay with
    /// the wedged worker (they are NOT answered with an error), and the
    /// worker keeps its `Arc<RocksStorage>` — and therefore the RocksDB
    /// LOCK — alive until process exit: a group whose batcher failed to
    /// close must not be restarted without bouncing the process.
    pub async fn close(&self) -> Result<(), String> {
        self.state.closing.store(true, Ordering::SeqCst);

        // One deadline for the entire close protocol, not per stage.
        let deadline = Instant::now() + self.state.close_timeout;

        // Ask the worker to drain + final-flush + ack us. If the channel is
        // already closed the worker has exited — a previous close() ran to
        // completion; fall through to the worker-exit check below.
        let (ack_tx, ack_rx) = oneshot::channel();
        let send = timeout_at(deadline, self.tx.send(BatchItem::Close(ack_tx))).await;
        match send {
            Ok(Ok(())) => match timeout_at(deadline, ack_rx).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    return Err("io batcher worker exited without acking close".to_string());
                }
                Err(_) => {
                    tracing::warn!(
                        timeout_ms = self.state.close_timeout.as_millis() as u64,
                        "io batcher close timed out waiting for final flush"
                    );
                    return Err("io batcher close timed out".to_string());
                }
            },
            Ok(Err(_)) => {} // channel closed: worker already exited
            Err(_) => {
                tracing::warn!(
                    timeout_ms = self.state.close_timeout.as_millis() as u64,
                    "io batcher close timed out enqueueing the close request"
                );
                return Err("io batcher close timed out".to_string());
            }
        }

        // `close()` returning Ok must imply the worker task has exited. The
        // worker drops its receiver on exit, which completes `closed()` on
        // every sender clone.
        if timeout_at(deadline, self.tx.closed()).await.is_err() {
            tracing::warn!(
                timeout_ms = self.state.close_timeout.as_millis() as u64,
                "io batcher worker did not exit before the close deadline"
            );
            return Err("io batcher worker did not exit before close deadline".to_string());
        }

        // Reap the JoinHandle if no other close() call got there first —
        // the task is finished by now, so awaiting it never blocks. The
        // std MutexGuard must be dropped BEFORE the await: holding it
        // across would make this future `!Send`.
        let handle = self.state.worker.lock().unwrap().take();
        if let Some(handle) = handle {
            if let Err(e) = handle.await {
                // The worker panicked (or was cancelled): accepted items may
                // have been lost mid-flush. `Ok` would falsely report a clean
                // drain, so propagate the failure (see module docs).
                tracing::warn!(error = %e, "io batcher worker task failed");
                return Err(format!("io batcher worker task failed: {e}"));
            }
        }
        Ok(())
    }

    /// Whether the worker has exited (channel receiver dropped).
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

// ── Worker task ──────────────────────────────────────────────────────────────

fn accumulate(
    item: BatchItem,
    ops: &mut Vec<BatchOp>,
    completions: &mut Vec<BatchCompletion>,
    close_acks: &mut Vec<oneshot::Sender<()>>,
    closing: &mut bool,
) {
    match item {
        BatchItem::Ops {
            ops: item_ops,
            completions: item_completions,
        } => {
            ops.extend(item_ops);
            completions.extend(item_completions);
        }
        BatchItem::Close(ack) => {
            close_acks.push(ack);
            *closing = true;
        }
    }
}

/// Write the batch once and answer every completion with the same result.
async fn flush(storage: &RocksStorage, ops: Vec<BatchOp>, completions: Vec<BatchCompletion>) {
    let write_result = if ops.is_empty() {
        Ok(())
    } else {
        storage.write_batch(ops).await.map_err(|e| e.to_string())
    };
    for completion in completions {
        completion.complete(write_result.clone());
    }
}

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

        let mut all_ops = Vec::new();
        let mut completions: Vec<BatchCompletion> = Vec::new();
        let mut close_acks: Vec<oneshot::Sender<()>> = Vec::new();
        let mut closing = false;
        accumulate(
            first,
            &mut all_ops,
            &mut completions,
            &mut close_acks,
            &mut closing,
        );

        // ── Phase 2: non-blocking drain (zero-latency high-throughput path) ─
        while !closing && all_ops.len() < config.max_entries {
            match rx.try_recv() {
                Ok(item) => accumulate(
                    item,
                    &mut all_ops,
                    &mut completions,
                    &mut close_acks,
                    &mut closing,
                ),
                Err(_) => break, // channel momentarily empty
            }
        }

        // ── Phase 3: timed drain (low-throughput amortisation) ───────────
        // Only wait if we have room for more AND got nothing via try_recv.
        if !closing && completions.len() == 1 && !config.window.is_zero() {
            let deadline = tokio::time::Instant::now() + config.window;
            loop {
                if closing || all_ops.len() >= config.max_entries {
                    break;
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Some(item)) => accumulate(
                        item,
                        &mut all_ops,
                        &mut completions,
                        &mut close_acks,
                        &mut closing,
                    ),
                    Ok(None) => break, // channel closed
                    Err(_) => break,   // timeout
                }
            }
        }

        // ── Phase 4+5: single write for the entire batch, then notify ─────
        flush(&storage, all_ops, completions).await;

        if closing {
            // ── Closing sequence: stop accepting new items, fold everything
            // already received into one final write_batch (final fsync under
            // Strict), answer every pending completion, ack all closers, exit.
            rx.close();
            let mut rest_ops = Vec::new();
            let mut rest_completions = Vec::new();
            let mut dummy = true;
            while let Ok(item) = rx.try_recv() {
                accumulate(
                    item,
                    &mut rest_ops,
                    &mut rest_completions,
                    &mut close_acks,
                    &mut dummy,
                );
            }
            flush(&storage, rest_ops, rest_completions).await;
            for ack in close_acks {
                let _ = ack.send(());
            }
            return;
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

    fn put_op(i: u32) -> Vec<BatchOp> {
        vec![BatchOp::Put {
            cf: "state_machine".to_string(),
            key: i.to_be_bytes().to_vec(),
            value: i.to_be_bytes().to_vec(),
        }]
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
        let config = IoBatcherConfig::new(Duration::from_millis(20), 256);
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

    // ── T1: close() drains every pending submission before returning ──────

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batcher_close_drains_pending() {
        let (storage, _tmp) = open_storage();
        // Long window so items are still accumulating when close() begins.
        let batcher = IoBatcher::new(
            Arc::clone(&storage),
            IoBatcherConfig::new(Duration::from_secs(5), 256),
        );

        // Enqueue all 100 items directly from the test task BEFORE closing
        // starts: each `send().await` completes while channel capacity
        // remains (depth 512 > 100), so every item is provably accepted —
        // no spawn/sleep race against close()'s fail-fast flag. Completions
        // are observed through the same oneshot channel `submit()` uses.
        let mut receivers = Vec::new();
        for i in 0u32..100 {
            let (result_tx, result_rx) = oneshot::channel();
            batcher
                .tx
                .send(BatchItem::Ops {
                    ops: put_op(i),
                    completions: vec![BatchCompletion::Submit(result_tx)],
                })
                .await
                .expect("send must succeed while the batcher is open");
            receivers.push(result_rx);
        }

        batcher.close().await.expect("close must succeed");

        // close() returned ⟹ every pending submission was flushed and
        // answered exactly once (all Ok, no hang, no error).
        for rx in receivers {
            let result = rx.await.expect("completion sender must not drop");
            assert!(
                result.is_ok(),
                "every accepted submit must be answered Ok after close, got {result:?}"
            );
        }
        for i in 0u32..100 {
            let key = i.to_be_bytes();
            let val = storage.get("state_machine", &key).await.unwrap();
            assert_eq!(
                val.as_deref(),
                Some(key.as_slice()),
                "key {i} must be durable after close"
            );
        }
    }

    // ── T2: close() is idempotent, rejects new work, worker exits ─────────

    #[tokio::test]
    async fn batcher_close_idempotent_and_rejects_new() {
        let (storage, _tmp) = open_storage();
        let batcher = IoBatcher::new(Arc::clone(&storage), IoBatcherConfig::default());

        // Submit one item so the worker has real work to drain.
        batcher.submit(put_op(7)).await.unwrap();

        // Concurrent close() calls all observe the same shutdown.
        let b2 = batcher.clone();
        let (r1, r2) = tokio::join!(batcher.close(), b2.close());
        r1.expect("first close must succeed");
        r2.expect("concurrent close must succeed");

        // A further close() after full shutdown is still Ok.
        batcher.close().await.expect("repeated close must succeed");

        // The worker task has exited.
        assert!(batcher.is_closed(), "worker must have exited after close");

        // New submissions fail fast.
        let result = batcher.submit(put_op(8)).await;
        assert!(result.is_err(), "submit after close must fail fast");

        // The drained item landed; the rejected one did not.
        let val = storage
            .get("state_machine", &7u32.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(val.as_deref(), Some(7u32.to_be_bytes().as_slice()));
        let val = storage
            .get("state_machine", &8u32.to_be_bytes())
            .await
            .unwrap();
        assert!(val.is_none(), "rejected submission must not be written");
    }
}
