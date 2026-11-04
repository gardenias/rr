//! Benchmark: [`ShardRouter::route`] — the per-request routing-table lookup
//! on the data plane.
//!
//! The router reads the provider's copy-on-write `Arc<RoutingTable>`
//! snapshot on every call (INV-6: no cached view in the router), so the
//! measured cost is: snapshot `Arc` clone + `HashMap` probe (+ the
//! not-found metric on the miss path). The provider here is
//! [`MemConfigProvider`]; the etcd provider is out of scope (needs a live
//! server).
//!
//! `route()` is async but never actually pends against the in-memory
//! provider, so a current-thread runtime driving 1 000 lookups per
//! iteration keeps executor overhead amortized to noise.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rr_config::mem::MemConfigProvider;
use rr_core::types::PartitionConfig;
use rr_router::router::ShardRouter;

/// Routing-table sizes: 64 partitions (small deployment) and 256 (the
/// order of magnitude of a mid-size shard layout).
const TABLE_SIZES: [u64; 2] = [64, 256];
/// Lookups per timed iteration — amortizes the `block_on` wrapper.
const OPS_PER_ITER: u64 = 1_000;

/// Build a provider with `n` partitions, 8 partitions per group.
async fn make_router(n: u64) -> ShardRouter {
    let provider = MemConfigProvider::new();
    for partition in 0..n {
        provider
            .add_partition(
                partition,
                PartitionConfig::SingleId(partition),
                partition / 8,
                vec![1],
            )
            .await;
    }
    ShardRouter::new(Arc::new(provider))
}

fn bench_route_hit(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("shard_router_route_hit");
    group.throughput(Throughput::Elements(OPS_PER_ITER));
    for n in TABLE_SIZES {
        let router = rt.block_on(make_router(n));
        group.bench_with_input(BenchmarkId::new("partitions", n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    // Rotate over the whole table so the measurement is a
                    // representative hash-map probe, not a single cached key.
                    for i in 0..OPS_PER_ITER {
                        let group_id = router.route(i % n).await.unwrap();
                        criterion::black_box(group_id);
                    }
                })
            });
        });
    }
    group.finish();
}

fn bench_route_miss(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("shard_router_route_miss");
    group.throughput(Throughput::Elements(OPS_PER_ITER));
    for n in TABLE_SIZES {
        let router = rt.block_on(make_router(n));
        group.bench_with_input(BenchmarkId::new("partitions", n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for i in 0..OPS_PER_ITER {
                        // Outside the populated range: PartitionNotFound +
                        // the `router.route.not_found` counter increment.
                        let err = router.route(n + i).await.unwrap_err();
                        criterion::black_box(err);
                    }
                })
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_route_hit, bench_route_miss);
criterion_main!(benches);
