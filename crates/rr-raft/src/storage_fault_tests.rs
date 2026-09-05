//! R-4 consumer tests: prove `rr_core::storage_fault::FaultInjectingStorage`
//! slots into the **group-commit write path** — the reason `IoBatcher` was
//! generalised from `Arc<RocksStorage>` to `Arc<dyn StorageEngine>` (R-4
//! design, option (a)).
//!
//! Every test drives an [`IoBatcher`] whose worker writes through
//! `Arc<FaultInjectingStorage<RocksStorage>>` — exactly the object graph
//! `RocksLogStorage::new_with_config` builds, minus the encoding shim.
//! (`RocksLogStorage` itself keeps a concrete `Arc<RocksStorage>` field for
//! its direct `scan`/`get`/`put` calls; the batcher is the injection point
//! this layer delivers.)
//!
//! One test per fault mode, per the R-4 acceptance criteria: fail fsync,
//! torn write, ENOSPC, slow disk.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tempfile::TempDir;

use rr_core::error::StorageError;
use rr_core::storage::BatchOp;
use rr_core::storage::StorageEngine;
use rr_core::storage_fault::{FaultConfig, FaultInjectingStorage, TornPrefix, TornWrite, Trigger};
use rr_storage::rocks::RocksStorage;

use crate::io_batcher::{IoBatcher, IoBatcherConfig};

/// The real log CF; keys are big-endian `u64` log indexes, exactly like
/// `rocks_log_storage`'s `index_key`.
const CF_LOG: &str = "raft_log";

type FaultedRocks = FaultInjectingStorage<RocksStorage>;

/// Open a RocksDB in a tempdir, wrap it in the fault injector (all faults
/// disarmed, seeded), and build a batcher whose worker writes through the
/// wrapper. Returns the batcher, the wrapper (for arm/disarm + white-box
/// reads), and the tempdir guard.
fn batcher_over_faulted_storage(
    seed: u64,
    config: IoBatcherConfig,
) -> (IoBatcher, Arc<FaultedRocks>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let rocks = RocksStorage::open_default(tmp.path().to_str().unwrap()).unwrap();
    let faulted = Arc::new(FaultInjectingStorage::new(rocks, FaultConfig::seeded(seed)));
    // The R-4 deliverable: the batcher's storage is `Arc<dyn StorageEngine>`,
    // so the wrapper coerces straight into the group-commit path.
    let batcher = IoBatcher::new(faulted.clone(), config);
    (batcher, faulted, tmp)
}

fn put_op(index: u64) -> BatchOp {
    BatchOp::Put {
        cf: CF_LOG.to_string(),
        key: index.to_be_bytes().to_vec(),
        value: format!("val-{index}").into_bytes(),
    }
}

async fn scan_all(storage: &FaultedRocks) -> Vec<(Vec<u8>, Vec<u8>)> {
    storage
        .scan(CF_LOG, &0u64.to_be_bytes(), &u64::MAX.to_be_bytes())
        .await
        .unwrap()
}

/// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
///
/// Fail fsync: with `fail_flush` armed, `flush()` on the storage under the
/// group-commit path surfaces `Err(StorageError::Io)` to its caller — not a
/// panic, not a silent `Ok`. The batcher itself never calls `flush()` (group
/// commit fsyncs inside `write_batch` under Strict), so its writes keep
/// completing while the fault is armed — proving the arming is selective and
/// the worker is not poisoned. After disarming, both flush and a subsequent
/// batched write succeed (worker survives).
#[tokio::test]
async fn fail_flush_surfaces_err_and_batcher_worker_survives() {
    let (batcher, faulted, _tmp) =
        batcher_over_faulted_storage(101, IoBatcherConfig::no_batching());

    faulted.arm_fail_flush(Trigger::EveryCall);

    // The fsync failure surfaces as an Err to the flush caller — never a
    // panic, never a silent Ok.
    let err = faulted.flush().await.unwrap_err();
    assert!(
        matches!(err, StorageError::Io(_)),
        "fail_flush must surface StorageError::Io, got {err:?}"
    );
    assert!(
        err.to_string().contains("fsync"),
        "error must identify the simulated fsync failure: {err}"
    );

    // The group-commit path (`write_batch`) is not the faulted method: a
    // batched write still completes while the flush fault is armed.
    batcher.submit(vec![put_op(1)]).await.unwrap();

    // Disarm: flush recovers, and the batcher worker survived the whole
    // arm/fire/disarm cycle — a subsequent write through it succeeds.
    faulted.disarm_all();
    faulted.flush().await.unwrap();
    batcher.submit(vec![put_op(2)]).await.unwrap();

    assert_eq!(
        scan_all(&faulted).await,
        vec![
            (1u64.to_be_bytes().to_vec(), b"val-1".to_vec()),
            (2u64.to_be_bytes().to_vec(), b"val-2".to_vec()),
        ]
    );
    batcher.close().await.expect("close must succeed");
}

/// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
///
/// Torn write: a batch that tears applies only a prefix of its ops and
/// reports `Err`. Assert the honest post-state: the prefix is durable and
/// NOT rolled back (the explicit, configurable form of the partial-apply
/// semantics called out in `MemStorage::write_batch`); no op from the
/// suffix is observable via `scan` — a reader never sees a half-written
/// entry, because each op is atomic and only whole ops from the prefix
/// landed. The error propagates to the batch caller.
#[tokio::test]
async fn torn_write_batch_leaves_prefix_only_and_error_propagates() {
    let (batcher, faulted, _tmp) =
        batcher_over_faulted_storage(103, IoBatcherConfig::no_batching());

    faulted.arm_torn_write(TornWrite {
        trigger: Trigger::EveryCall,
        prefix: TornPrefix::Fixed(1),
    });

    // One submit = one write_batch of 3 ops → tears after the first op.
    let err = batcher
        .submit(vec![put_op(1), put_op(2), put_op(3)])
        .await
        .unwrap_err();
    assert!(
        err.contains("torn"),
        "torn-write error must propagate to the batch caller: {err}"
    );

    // Honest post-state: exactly the prefix (entry 1) is observable; the
    // suffix (entries 2, 3) is absent — no half entry.
    assert_eq!(
        scan_all(&faulted).await,
        vec![(1u64.to_be_bytes().to_vec(), b"val-1".to_vec())],
        "only the applied prefix may be observable after a torn batch"
    );

    // The worker survives; disarmed, the retried batch lands in full.
    faulted.disarm_all();
    batcher
        .submit(vec![put_op(1), put_op(2), put_op(3)])
        .await
        .unwrap();
    assert_eq!(scan_all(&faulted).await.len(), 3);

    batcher.close().await.expect("close must succeed");
}

/// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
///
/// ENOSPC: with the full-disk fault armed, every write fails and the error
/// propagates to **all** pending batch callers — no caller is answered `Ok`
/// for a write that never happened, nobody hangs, nothing panics. The
/// wrapper's write paths report `StorageFull`-class `StorageError::Io`.
#[tokio::test]
async fn enospc_propagates_to_all_pending_batch_callers() {
    // A small accumulation window lets concurrent submits share batches;
    // every caller of every formed batch must see the error either way.
    let (batcher, faulted, _tmp) =
        batcher_over_faulted_storage(107, IoBatcherConfig::new(Duration::from_millis(20), 64));

    faulted.arm_enospc(Trigger::EveryCall);

    // The StorageError level carries the ENOSPC class (StorageFull).
    match faulted.put(CF_LOG, b"k", b"v").await {
        Err(StorageError::Io(e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::StorageFull, "got {e}")
        }
        other => panic!("put under ENOSPC must fail with StorageFull, got {other:?}"),
    }

    let mut handles = Vec::new();
    for i in 1..=8u64 {
        let b = batcher.clone();
        handles.push(tokio::spawn(async move { b.submit(vec![put_op(i)]).await }));
    }
    for h in handles {
        let result = h.await.expect("submitter task must not panic");
        assert!(
            result.is_err(),
            "every pending batch caller must see the ENOSPC error, got {result:?}"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("no space left"),
            "error must identify ENOSPC: {err}"
        );
    }

    // Nothing was applied by any torn/errored batch.
    assert!(scan_all(&faulted).await.is_empty());

    // The worker survives: disarm, and writes succeed again.
    faulted.disarm_all();
    batcher.submit(vec![put_op(9)]).await.unwrap();
    assert_eq!(scan_all(&faulted).await.len(), 1);

    batcher.close().await.expect("close must succeed");
}

/// Slow disk (R-4 acceptance: no latency-induced failure may be misread as
/// data loss). With a delay injected before every storage method, operations
/// through the group-commit path still complete **correctly** — every caller
/// is answered `Ok`, the data is intact and readable — they are just slower.
/// Not an INV-1 durability test per se: it guards the ACK boundary against
/// spurious timeout/failure under slow storage.
#[tokio::test]
async fn slow_disk_latency_completes_correctly_just_slower() {
    let delay = Duration::from_millis(25);
    let (batcher, faulted, _tmp) =
        batcher_over_faulted_storage(109, IoBatcherConfig::no_batching());

    faulted.arm_latency(delay);

    let start = Instant::now();
    batcher.submit(vec![put_op(1)]).await.unwrap();
    // tokio's timer resolution can land a touch early; allow 2 ms slack.
    assert!(
        start.elapsed() + Duration::from_millis(2) >= delay,
        "the injected latency must actually delay the batched write"
    );

    // Reads are delayed too (every method), and the data is intact.
    let start = Instant::now();
    let got = faulted.get(CF_LOG, &1u64.to_be_bytes()).await.unwrap();
    assert!(
        start.elapsed() + Duration::from_millis(2) >= delay,
        "the injected latency must delay reads as well"
    );
    assert_eq!(got.as_deref(), Some(b"val-1".as_ref()));

    // No spurious failure anywhere along the way; graceful close unaffected.
    faulted.disarm_all();
    batcher.close().await.expect("close must succeed");
}
