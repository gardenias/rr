//! TPS (transactions per second) measurement for RocksLogStorage.
//!
//! Simulates realistic Raft workloads where N concurrent proposals each
//! write a single entry and wait for the callback.  Measures wall-clock
//! throughput rather than per-operation latency.
//!
//! Three scenarios:
//! 1. Sequential (1 in-flight at a time) — the fsync-per-proposal baseline
//! 2. Concurrent with no-batching — parallel submits, individual fsyncs
//! 3. Concurrent with IoBatcher — parallel submits share a single fsync
//!
//! Run with: `cargo bench --bench log_tps --features rocks`

#![cfg(feature = "rocks")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use openraft::type_config::alias::EntryOf;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use openraft::vote::RaftLeaderId;
use openraft::{EntryPayload, LogId};
use rr_raft::io_batcher::IoBatcher;
use rr_raft::{IoBatcherConfig, RrTypeConfig};
use rr_storage::rocks::RocksStorage;
use tokio::sync::Mutex;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_entry(index: u64) -> EntryOf<RrTypeConfig> {
    EntryOf::<RrTypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
        payload: EntryPayload::Normal(vec![0u8; 128].into()),
    }
}

/// Sequential single-entry appends via RocksLogStorage.
async fn run_sequential(
    storage: &Arc<Mutex<rr_raft::rocks_log_storage::RocksLogStorage>>,
    n: usize,
) -> Duration {
    use openraft::storage::RaftLogStorageExt;

    let start = Instant::now();
    for i in 1u64..=(n as u64) {
        storage
            .lock()
            .await
            .blocking_append(vec![make_entry(i)])
            .await
            .unwrap();
    }
    start.elapsed()
}

// ── TPS: sequential proposals ─────────────────────────────────────────────────

fn bench_tps_sequential(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("tps_sequential_proposals");
    group.sample_size(20);

    for n in [10usize, 50, 100] {
        // no-batching: each proposal gets its own write_batch + fsync
        group.bench_with_input(BenchmarkId::new("no_batching", n), &n, |b, &count| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += rt.block_on(async {
                        let tmp = tempfile::TempDir::new().unwrap();
                        let storage = Arc::new(Mutex::new(
                            rr_raft::rocks_log_storage::RocksLogStorage::open_with_config(
                                tmp.path().to_str().unwrap(),
                                IoBatcherConfig::no_batching(),
                            )
                            .unwrap(),
                        ));
                        run_sequential(&storage, count).await
                    });
                }
                total
            });
        });

        // batched: proposals accumulate in the batcher window before fsync
        group.bench_with_input(BenchmarkId::new("batched_500us", n), &n, |b, &count| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += rt.block_on(async {
                        let tmp = tempfile::TempDir::new().unwrap();
                        let storage = Arc::new(Mutex::new(
                            rr_raft::rocks_log_storage::RocksLogStorage::open_with_config(
                                tmp.path().to_str().unwrap(),
                                IoBatcherConfig::new(Duration::from_micros(500), 512),
                            )
                            .unwrap(),
                        ));
                        run_sequential(&storage, count).await
                    });
                }
                total
            });
        });
    }

    group.finish();
}

// ── TPS: concurrent proposals via direct batcher ──────────────────────────────

fn bench_tps_concurrent_batcher(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("tps_concurrent_batcher");
    group.sample_size(20);

    for n in [10usize, 50, 100, 500] {
        // Simulate N concurrent proposals all hitting the batcher simultaneously.
        // The batcher accumulates them and issues a single write_batch.
        group.bench_with_input(
            BenchmarkId::new("concurrent_proposals", n),
            &n,
            |b, &count| {
                b.iter_custom(|iters| {
                    use rr_core::storage::BatchOp;

                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += rt.block_on(async {
                            let tmp = tempfile::TempDir::new().unwrap();
                            let rocks = Arc::new(
                                RocksStorage::open_default(tmp.path().to_str().unwrap()).unwrap(),
                            );
                            let batcher = Arc::new(IoBatcher::new(
                                Arc::clone(&rocks),
                                IoBatcherConfig::new(Duration::from_micros(500), 1024),
                            ));

                            let start = Instant::now();

                            // Spawn N concurrent tasks each submitting 1 entry
                            let handles: Vec<_> = (0u64..count as u64)
                                .map(|i| {
                                    let b = Arc::clone(&batcher);
                                    tokio::spawn(async move {
                                        let entry = make_entry(i + 1);
                                        let key = (i + 1).to_be_bytes().to_vec();
                                        let value = bincode::serde::encode_to_vec(
                                            &entry,
                                            bincode::config::standard(),
                                        )
                                        .unwrap();
                                        b.submit(vec![BatchOp::Put {
                                            cf: "raft_log".to_string(),
                                            key,
                                            value,
                                        }])
                                        .await
                                        .unwrap();
                                    })
                                })
                                .collect();

                            for h in handles {
                                h.await.unwrap();
                            }

                            start.elapsed()
                        });
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_tps_sequential, bench_tps_concurrent_batcher);
criterion_main!(benches);
