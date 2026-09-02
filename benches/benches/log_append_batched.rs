//! Benchmark: IoBatcher group-commit throughput for concurrent single-entry proposals.
//!
//! The batcher's advantage is visible when multiple proposals arrive concurrently.
//! N proposals share a single fsync instead of paying N individual fsyncs.
//!
//! Two scenarios:
//! - `sequential_single`: one entry at a time (worst case for batcher latency)
//! - `bulk_batch`: all entries in a single blocking_append (best case: 1 fsync)
//!
//! Run with: `cargo bench --bench log_append_batched --features rocks`

#![cfg(feature = "rocks")]

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use openraft::storage::RaftLogStorageExt;
use openraft::type_config::alias::EntryOf;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use openraft::vote::RaftLeaderId;
use openraft::{EntryPayload, LogId};
use rr_raft::rocks_log_storage::RocksLogStorage;
use rr_raft::{IoBatcherConfig, RrTypeConfig};
use tokio::sync::Mutex;

fn make_entry(index: u64) -> EntryOf<RrTypeConfig> {
    EntryOf::<RrTypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
        payload: EntryPayload::Normal(vec![0u8; 64].into()),
    }
}

/// Sequential single-entry appends — measures worst-case batcher overhead vs
/// raw per-entry fsync cost.
fn bench_sequential_single(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("log_append_sequential_single");
    group.sample_size(20);

    for n in [10usize, 100] {
        group.bench_with_input(BenchmarkId::new("no_batching", n), &n, |b, &count| {
            b.iter(|| {
                rt.block_on(async {
                    let tmp = tempfile::TempDir::new().unwrap();
                    let storage = Arc::new(Mutex::new(
                        RocksLogStorage::open_with_config(
                            tmp.path().to_str().unwrap(),
                            IoBatcherConfig::no_batching(),
                        )
                        .unwrap(),
                    ));
                    for i in 1u64..=(count as u64) {
                        storage
                            .lock()
                            .await
                            .blocking_append(vec![make_entry(i)])
                            .await
                            .unwrap();
                    }
                });
            });
        });

        group.bench_with_input(BenchmarkId::new("batched_bulk", n), &n, |b, &count| {
            b.iter(|| {
                rt.block_on(async {
                    let tmp = tempfile::TempDir::new().unwrap();
                    let mut storage = RocksLogStorage::open_with_config(
                        tmp.path().to_str().unwrap(),
                        IoBatcherConfig::new(Duration::from_micros(500), 512),
                    )
                    .unwrap();
                    // All entries in one blocking_append → 1 fsync via batcher
                    let entries: Vec<_> = (1u64..=(count as u64)).map(make_entry).collect();
                    storage.blocking_append(entries).await.unwrap();
                });
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_sequential_single);
criterion_main!(benches);
