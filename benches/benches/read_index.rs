//! Benchmark: ReadIndex — the linearizable-read request path in rr-raft.
//!
//! [`RaftGroupManager::linearizable_read`] runs openraft
//! `ReadPolicy::ReadIndex` (`get_read_linearizer` → `await_ready`) and then
//! serves the payload from the local state machine via the read handle.
//!
//! Boundary (documented per the remediation brief): a single-node elected
//! leader — a quorum of one — so the bench covers the full raft-side
//! ReadIndex round trip (leader-lease/quorum check through the raft core,
//! apply-frontier wait, read-handle dispatch) but no follower heartbeat
//! RPC fan-out, which requires a live multi-node cluster. A `local_read`
//! baseline (no raft coordination) is measured alongside so the ReadIndex
//! premium is visible by diffing the two groups.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::BasicNode;
use rr_core::error::RrError;
use rr_core::service::Service;
use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};
use rr_raft::ServiceStateMachineAdapter;

const GROUP: u64 = 1;

/// Minimal service: echo the read payload back.
struct EchoService;

#[async_trait]
impl Service for EchoService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        Ok(request)
    }
    fn name(&self) -> &str {
        "read-index-bench"
    }
    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        Ok(Vec::new())
    }
    async fn restore(&mut self, _data: &[u8]) -> Result<(), RrError> {
        Ok(())
    }
}

fn service_factory() -> StateMachineFactory {
    Box::new(|group_id| {
        Box::new(ServiceStateMachineAdapter::new(group_id, EchoService))
            as Box<dyn rr_core::state_machine::StateMachine>
    })
}

/// Single-node group with an elected leader (snapshots disabled so a
/// policy trigger cannot perturb a sample).
async fn setup() -> Arc<RaftGroupManager> {
    let mgr = Arc::new(
        RaftGroupManager::new(1, service_factory()).with_snapshot_logs_since_last(u64::MAX),
    );
    mgr.start_group(GROUP).await.unwrap();
    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    mgr.init_cluster(GROUP, members).await.unwrap();

    let raft = mgr.get_raft(GROUP).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let metrics = raft.metrics().borrow_watched().clone();
        if metrics.current_leader == Some(1) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "single-node self-election timed out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    mgr
}

fn bench_linearizable_read(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mgr = rt.block_on(setup());

    let mut group = c.benchmark_group("read_index");
    for payload_size in [64usize, 1024] {
        let payload = vec![0u8; payload_size];
        group.bench_with_input(
            BenchmarkId::new("linearizable_read", payload_size),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || payload.clone(),
                    |p| {
                        let bytes = rt.block_on(mgr.linearizable_read(GROUP, p)).unwrap();
                        criterion::black_box(bytes);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_local_read_baseline(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mgr = rt.block_on(setup());

    let mut group = c.benchmark_group("read_index");
    for payload_size in [64usize, 1024] {
        let payload = vec![0u8; payload_size];
        group.bench_with_input(
            BenchmarkId::new("local_read_baseline", payload_size),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || payload.clone(),
                    |p| {
                        let bytes = rt.block_on(mgr.local_read(GROUP, p)).unwrap();
                        criterion::black_box(bytes);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_linearizable_read, bench_local_read_baseline);
criterion_main!(benches);
