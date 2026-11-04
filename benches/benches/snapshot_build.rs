//! Benchmark: snapshot build / serialize / persist at the raft layer.
//!
//! Seams covered (no live cluster):
//! - `snapshot_build/in_memory`: the full framework build path —
//!   `RrStateMachine::get_snapshot_builder` → `Service::snapshot()` →
//!   `FramedSnapshot` encode → meta assembly → `build_snapshot()`.
//! - `snapshot_build/durable`: the same with a [`SnapshotStore`] attached,
//!   so every build also runs the T1 durable persist (bincode encode of
//!   `PersistentSnapshot` + atomic tmp-write/fsync/rename) — the rocks
//!   deployment shape (ADR A-0016).
//! - `framed_snapshot_encode` / `framed_snapshot_decode`: the pure
//!   envelope serialize seam in isolation.
//!
//! Out of scope (documented boundary): the gRPC `SnapshotChunk` stream
//! encode lives inline in `GrpcRaftNetwork::install_snapshot` (private,
//! needs a live channel); the 64 KiB chunking itself is a slice copy and
//! its cost is represented here by the `framed_snapshot_*` byte-throughput
//! cases.

use std::io;

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::type_config::alias::EntryOf;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use openraft::vote::RaftLeaderId;
use openraft::{Entry, EntryPayload, LogId, RaftSnapshotBuilder};
use rr_core::error::RrError;
use rr_core::framed_snapshot::FramedSnapshot;
use rr_core::service::Service;
use rr_raft::{RrStateMachine, RrTypeConfig, ServiceStateMachineAdapter, SnapshotStore};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// Service holding a fixed-size pre-filled state; `snapshot()` clones it,
/// mirroring the serialize-whole-state contract of real services.
struct FixedStateService {
    state: Vec<u8>,
}

#[async_trait]
impl Service for FixedStateService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        Ok(request)
    }
    fn name(&self) -> &str {
        "snapshot-bench"
    }
    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        Ok(self.state.clone())
    }
    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        self.state = data.to_vec();
        Ok(())
    }
}

fn make_state(size: usize) -> Vec<u8> {
    // Non-uniform content so encode cost reflects real byte handling
    // (bincode copies verbatim; content matters only against pathological
    // same-byte optimizations in memcpy).
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn make_sm(size: usize, store: Option<SnapshotStore>) -> RrStateMachine {
    let adapter = ServiceStateMachineAdapter::new(
        1,
        FixedStateService {
            state: make_state(size),
        },
    );
    let sm = RrStateMachine::new(Box::new(adapter));
    match store {
        Some(store) => sm.with_snapshot_store(store).unwrap(),
        None => sm,
    }
}

/// Apply one entry so `last_applied = Some(1)`: the durable persist then
/// takes the steady-state path (seeded monotonic-guard cache) instead of
/// the unseeded load-fallback on every build.
async fn apply_one(sm: &mut RrStateMachine) {
    let entry: EntryOf<RrTypeConfig> = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Normal(vec![1u8].into()),
    };
    let items = vec![Ok::<EntryResponder<RrTypeConfig>, io::Error>((entry, None))];
    RaftStateMachine::apply(sm, tokio_stream::iter(items))
        .await
        .unwrap();
}

fn bench_snapshot_build(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("snapshot_build");

    for size in [MIB, 4 * MIB, 16 * MIB] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("in_memory", size), &size, |b, &size| {
            let mut sm = rt.block_on(async {
                let mut sm = make_sm(size, None);
                apply_one(&mut sm).await;
                sm
            });
            b.iter(|| {
                rt.block_on(async {
                    let mut builder = RaftStateMachine::get_snapshot_builder(&mut sm).await;
                    let snap = builder.build_snapshot().await.unwrap();
                    criterion::black_box(snap.snapshot.into_inner().len());
                })
            });
        });
    }

    for size in [MIB, 4 * MIB] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("durable", size), &size, |b, &size| {
            let dir = tempfile::tempdir().unwrap();
            let store = SnapshotStore::new(dir.path().join("sm-snapshot.bin"));
            let mut sm = rt.block_on(async {
                let mut sm = make_sm(size, Some(store));
                apply_one(&mut sm).await;
                sm
            });
            b.iter(|| {
                rt.block_on(async {
                    let mut builder = RaftStateMachine::get_snapshot_builder(&mut sm).await;
                    let snap = builder.build_snapshot().await.unwrap();
                    criterion::black_box(snap.snapshot.into_inner().len());
                })
            });
        });
    }

    group.finish();
}

fn bench_framed_snapshot_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("framed_snapshot_encode");
    for size in [64 * KIB, MIB, 16 * MIB] {
        let framed = FramedSnapshot::new(make_state(size));
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("bytes", size), &framed, |b, framed| {
            b.iter(|| {
                criterion::black_box(framed.encode().unwrap());
            });
        });
    }
    group.finish();
}

fn bench_framed_snapshot_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("framed_snapshot_decode");
    for size in [64 * KIB, MIB, 16 * MIB] {
        let bytes = FramedSnapshot::new(make_state(size)).encode().unwrap();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("bytes", size), &bytes, |b, bytes| {
            b.iter(|| {
                let decoded = FramedSnapshot::decode(criterion::black_box(bytes))
                    .unwrap()
                    .expect("framed snapshot");
                criterion::black_box(decoded);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_snapshot_build,
    bench_framed_snapshot_encode,
    bench_framed_snapshot_decode
);
criterion_main!(benches);
