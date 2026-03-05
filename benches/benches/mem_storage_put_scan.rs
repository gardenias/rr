//! Benchmark: 1000 `put` + `scan` operations on `MemStorage`.

use criterion::{criterion_group, criterion_main, Criterion};
use rr_core::storage::StorageEngine;
use rr_storage::MemStorage;

fn bench_mem_storage_put_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("mem_storage_put_scan_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = MemStorage::new();
                // 1000 puts
                for i in 0u64..1000 {
                    let key = i.to_be_bytes().to_vec();
                    let value = key.clone();
                    storage
                        .put("state_machine", &key, &value)
                        .await
                        .unwrap();
                }
                // 1000 scans (scan entire range each time would be too heavy;
                // instead scan small windows of 10 keys each)
                for i in 0u64..1000 {
                    let start = (i % 990).to_be_bytes().to_vec();
                    let end = ((i % 990) + 10).to_be_bytes().to_vec();
                    let _ = storage
                        .scan("state_machine", &start, &end)
                        .await
                        .unwrap();
                }
            });
        });
    });
}

criterion_group!(benches, bench_mem_storage_put_scan);
criterion_main!(benches);
