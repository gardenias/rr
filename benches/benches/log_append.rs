//! Benchmark: append 1000 entries to `RrLogStorage` (in-memory).

use criterion::{criterion_group, criterion_main, Criterion};
use openraft::storage::RaftLogStorageExt;
use openraft::type_config::alias::EntryOf;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use openraft::vote::RaftLeaderId;
use openraft::{EntryPayload, LogId};
use rr_raft::log_storage::RrLogStorage;
use rr_raft::type_config::RrTypeConfig;

fn make_entries(count: u64) -> Vec<EntryOf<RrTypeConfig>> {
    (1..=count)
        .map(|i| EntryOf::<RrTypeConfig> {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), i),
            payload: EntryPayload::Blank,
        })
        .collect()
}

fn bench_log_append(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("log_append_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut storage = RrLogStorage::default();
                let entries = make_entries(1000);
                storage.blocking_append(entries).await.unwrap();
            });
        });
    });
}

criterion_group!(benches, bench_log_append);
criterion_main!(benches);
