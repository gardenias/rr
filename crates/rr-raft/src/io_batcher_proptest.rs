//! Property-based interleaving tests for [`IoBatcher`] (reliability program
//! R-6; INV-1). Compiled only with `--features rocks`, matching the module
//! under test (`crate::io_batcher`).
//!
//! Randomised axes: submit concurrency × accumulation window × `max_entries`,
//! with a mix of `submit` (oneshot) and `submit_with_callback` (openraft
//! `IOFlushed`) completions and empty-op submissions.
//!
//! Properties under test:
//!
//! 1. **Exactly-once completion**: every accepted submission is answered
//!    exactly once, within a hard deadline (no hang — the empty-append case
//!    included, mirroring `rocks_log_storage.rs::test_append_empty_entries_does_not_hang`).
//!    For the oneshot/`IOFlushed` channels a second answer is structurally
//!    impossible to observe, so the externally checkable form is: all N
//!    accepted submissions resolve Ok, all N writes are durable, and
//!    `close()` — whose `Ok` contract is "every accepted item was flushed,
//!    answered exactly once, worker exited" — succeeds.
//! 2. **Big-endian ordering**: ops are keyed by `u64` log indexes dealt
//!    round-shuffled across submissions, so a single batch provably carries
//!    out-of-order keys; after completion, `scan` returns them in ascending
//!    index order with exactly the submitted values (ordering comes from the
//!    big-endian keys, not from callback-fire order — see the ordering note
//!    in `io_batcher.rs`'s module docs).
//! 3. **Batch-wide error propagation**: when every op targets a
//!    non-existent column family, every formed batch fails and EVERY caller
//!    of those batches receives `Err` — no spurious `Ok`, no hang — and the
//!    worker survives to serve later valid writes.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use openraft::storage::IOFlushed;
use openraft::type_config::async_runtime::{AsyncRuntime, Oneshot};
use openraft::RaftTypeConfig;

use rr_core::storage::{BatchOp, StorageEngine};
use rr_storage::rocks::RocksStorage;

use crate::io_batcher::{IoBatcher, IoBatcherConfig};
use crate::type_config::RrTypeConfig;

/// The real log CF — keys are big-endian `u64` log indexes, exactly like
/// `rocks_log_storage`'s `index_key`.
const CF_LOG: &str = "raft_log";
/// A CF that `open_default` never creates: writes to it make
/// `RocksStorage::write_batch` fail, which is how we induce batch errors.
const POISON_CF: &str = "no_such_cf";
/// Hard deadline per completion. Far above any real fsync latency; its only
/// job is to turn "worker wedged" into a fast test failure instead of a hang.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(20);

/// Same shape as the helper in `io_batcher.rs`'s tests.
fn open_storage() -> (Arc<RocksStorage>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let s = RocksStorage::open_default(tmp.path().to_str().unwrap()).unwrap();
    (Arc::new(s), tmp)
}

/// Same construction as `rocks_log_storage.rs`'s `flushed_signal`: an
/// `IOFlushed::signal` callback plus its receiver, the way openraft builds it.
fn flushed_signal() -> (
    IOFlushed<RrTypeConfig>,
    oneshot::Receiver<Result<(), io::Error>>,
) {
    let (tx, rx) =
        <<RrTypeConfig as RaftTypeConfig>::AsyncRuntime as AsyncRuntime>::Oneshot::channel();
    (IOFlushed::signal(tx), rx)
}

fn put_op(cf: &str, index: u64) -> BatchOp {
    BatchOp::Put {
        cf: cf.to_string(),
        key: index.to_be_bytes().to_vec(),
        value: format!("val-{index}").into_bytes(),
    }
}

#[derive(Debug, Clone)]
struct Submission {
    /// true → `submit_with_callback` (openraft `IOFlushed`); false → `submit`.
    via_callback: bool,
    /// Log indexes this submission writes. Empty = the empty-append probe.
    indexes: Vec<u64>,
}

fn batcher_config_strategy() -> impl Strategy<Value = IoBatcherConfig> {
    (
        prop::sample::select(vec![0u64, 500, 2_000]),
        prop::sample::select(vec![1usize, 2, 5, 64]),
    )
        .prop_map(|(window_us, max_entries)| {
            IoBatcherConfig::new(Duration::from_micros(window_us), max_entries)
        })
}

/// Random submission set whose log indexes are a random permutation of
/// `0..total`, dealt across submissions — so wherever batch boundaries fall,
/// a batch provably holds out-of-order big-endian keys, and every key is
/// globally unique (final storage state is deterministic regardless of how
/// submissions interleave into batches).
fn scenario_strategy() -> impl Strategy<Value = (IoBatcherConfig, Vec<Submission>)> {
    (
        batcher_config_strategy(),
        prop::collection::vec((any::<bool>(), 0usize..=3usize), 1..=16usize),
    )
        .prop_flat_map(|(config, specs)| {
            let total: usize = specs.iter().map(|s| s.1).sum();
            (
                Just(config),
                Just(specs),
                Just((0..total as u64).collect::<Vec<u64>>()).prop_shuffle(),
            )
        })
        .prop_map(|(config, specs, shuffled)| {
            let mut it = shuffled.into_iter();
            let submissions = specs
                .into_iter()
                .map(|(via_callback, len)| Submission {
                    via_callback,
                    indexes: it.by_ref().take(len).collect(),
                })
                .collect::<Vec<_>>();
            (config, submissions)
        })
}

/// One outstanding completion, either flavour the batcher supports.
enum Completion {
    Submit(JoinHandle<Result<(), String>>),
    Callback(oneshot::Receiver<Result<(), io::Error>>),
}

impl Completion {
    /// Wait for the single answer with a hard deadline (no-hang assertion).
    async fn wait(self) -> Result<Result<(), String>, String> {
        match self {
            Completion::Submit(jh) => match tokio::time::timeout(COMPLETION_TIMEOUT, jh).await {
                Ok(Ok(res)) => Ok(res),
                Ok(Err(e)) => Err(format!("submission task panicked: {e}")),
                Err(_) => Err("submission timed out (batcher hang)".to_string()),
            },
            Completion::Callback(rx) => match tokio::time::timeout(COMPLETION_TIMEOUT, rx).await {
                Ok(Ok(res)) => Ok(res.map_err(|e| e.to_string())),
                Ok(Err(_)) => Err("callback sender dropped without answering".to_string()),
                Err(_) => Err("callback timed out (batcher hang)".to_string()),
            },
        }
    }
}

proptest! {
    // Bounded case counts: each case opens a real tempdir RocksDB and runs
    // real (Strict-durability) write_batches; this runs in the CI test job.
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[test]
    fn proptest_io_batcher_interleavings_exactly_once_and_ordered(
        (config, submissions) in scenario_strategy(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (storage, _tmp) = open_storage();
            let batcher = IoBatcher::new(storage.clone(), config);

            // Ground truth: key = big-endian index, value = "val-<index>".
            let mut expected: Vec<(Vec<u8>, Vec<u8>)> = submissions
                .iter()
                .flat_map(|s| s.indexes.iter())
                .map(|&idx| (idx.to_be_bytes().to_vec(), format!("val-{idx}").into_bytes()))
                .collect();
            expected.sort();

            // Fire every submission concurrently; each yields ONE completion handle.
            let mut completions = Vec::with_capacity(submissions.len());
            for sub in &submissions {
                let ops: Vec<BatchOp> =
                    sub.indexes.iter().map(|&idx| put_op(CF_LOG, idx)).collect();
                if sub.via_callback {
                    let (callback, rx) = flushed_signal();
                    batcher
                        .submit_with_callback(ops, callback)
                        .await
                        .map_err(|e| TestCaseError::fail(format!(
                            "submit_with_callback on an open batcher must be accepted, got {e}"
                        )))?;
                    completions.push(Completion::Callback(rx));
                } else {
                    let b = batcher.clone();
                    completions.push(Completion::Submit(tokio::spawn(async move {
                        b.submit(ops).await
                    })));
                }
            }

            // 1: every accepted submission is answered exactly once, Ok, in time.
            for c in completions {
                let res = c.wait().await.map_err(TestCaseError::fail)?;
                prop_assert_eq!(
                    res,
                    Ok(()),
                    "every accepted submission must complete Ok (valid CF, no close in flight)"
                );
            }

            // 2: storage holds exactly the submitted ops, in ascending
            // big-endian log-index order regardless of batch/callback order.
            let start = 0u64.to_be_bytes().to_vec();
            let end = u64::MAX.to_be_bytes().to_vec();
            let scanned = storage.scan(CF_LOG, &start, &end).await.unwrap();
            prop_assert_eq!(
                scanned.len(),
                expected.len(),
                "every submitted op must be durable exactly once"
            );
            prop_assert_eq!(&scanned, &expected);
            prop_assert!(
                scanned.windows(2).all(|w| w[0].0 < w[1].0),
                "scan must return strictly ascending big-endian keys"
            );

            // close() Ok = "drained, flushed, answered exactly once, exited".
            batcher.close().await.map_err(|e| TestCaseError::fail(format!(
                "close must succeed after a drained run, got {e}"
            )))?;
            prop_assert!(batcher.is_closed(), "worker must have exited after close");
            batcher.close().await.map_err(|e| TestCaseError::fail(format!(
                "close must be idempotent, got {e}"
            )))?;
            prop_assert!(
                batcher.submit(vec![put_op(CF_LOG, 9_999)]).await.is_err(),
                "submit after close must fail fast"
            );
            Ok(())
        })?;
    }

    /// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[test]
    fn proptest_io_batcher_write_error_propagates_to_whole_batch(
        n_submissions in 1usize..=12,
        config in batcher_config_strategy(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (storage, _tmp) = open_storage();
            let batcher = IoBatcher::new(storage.clone(), config);

            // Every submission writes to a non-existent CF, so every formed
            // batch fails — the error must reach EVERY caller of those batches.
            let mut completions = Vec::with_capacity(n_submissions);
            for i in 0..n_submissions {
                let b = batcher.clone();
                let ops = vec![put_op(POISON_CF, i as u64)];
                completions.push(Completion::Submit(tokio::spawn(async move {
                    b.submit(ops).await
                })));
            }
            for c in completions {
                let res = c.wait().await.map_err(TestCaseError::fail)?;
                prop_assert!(
                    res.is_err(),
                    "a failed write_batch must surface Err to every caller of the batch, got {:?}",
                    res
                );
            }

            // The worker survives a failed batch: later valid writes succeed.
            let res = batcher.submit(vec![put_op(CF_LOG, 0)]).await;
            prop_assert_eq!(
                res,
                Ok(()),
                "batcher must keep serving after a failed batch"
            );
            let val = storage
                .get(CF_LOG, &0u64.to_be_bytes())
                .await
                .unwrap();
            prop_assert_eq!(val.as_deref(), Some(b"val-0".as_ref()));

            batcher.close().await.map_err(|e| TestCaseError::fail(format!(
                "close must succeed after error propagation, got {e}"
            )))?;
            Ok(())
        })?;
    }
}
