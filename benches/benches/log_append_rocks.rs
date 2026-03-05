//! Benchmark: append 1000 entries to `RocksLogStorage` (feature-gated behind `rocks`).

#![cfg(feature = "rocks")]

use criterion::{criterion_group, criterion_main, Criterion};
use openraft::storage::RaftLogStorageExt;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use rr_raft::rocks_log_storage::RocksLogStorage;
use rr_raft::type_config::RrTypeConfig;

fn make_entries(count: u64) -> Vec<Entry<RrTypeConfig>> {
    (1..=count)
        .map(|i| Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), i),
            payload: EntryPayload::Blank,
        })
        .collect()
}

fn bench_log_append_rocks(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("log_append_rocks_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tmp = tempfile::TempDir::new().unwrap();
                let mut storage =
                    RocksLogStorage::open(tmp.path().to_str().unwrap()).unwrap();
                let entries = make_entries(1000);
                storage.blocking_append(entries).await.unwrap();
            });
        });
    });
}

criterion_group!(benches, bench_log_append_rocks);
criterion_main!(benches);
