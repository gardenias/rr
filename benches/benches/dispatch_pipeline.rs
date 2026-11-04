//! Benchmark: dispatch/apply pipeline — the per-request server path from
//! [`ClientDispatchImpl::submit_write`] / [`submit_linearizable_read`]
//! through routing, framing, a single-node Raft propose→commit→apply, and
//! the apply-side frame handling in [`ServiceStateMachineAdapter`]
//! (FramedEntry decode → idempotency layer → service → ApplyOutcome
//! encode → dispatch-side decode).
//!
//! Boundary (documented per the remediation brief): an in-process
//! single-node group — `NoopNetworkFactory`, in-memory `RrLogStorage` —
//! so the bench covers everything except the gRPC boundary, multi-node
//! replication RPCs, and disk fsync. This is the same harness the
//! `rr-server` dispatch unit tests use. Snapshotting is disabled
//! (`LogsSinceLast(u64::MAX)`) so a policy-triggered snapshot cannot
//! perturb a sample.
//!
//! Keyed writes use a fresh `request_id` per iteration (the New path:
//! decide + service + record); the Replay path is already covered by
//! `idempotency_throughput`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::BasicNode;
use rr_config::mem::MemConfigProvider;
use rr_core::error::RrError;
use rr_core::idempotency::IdempotencyKey;
use rr_core::service::Service;
use rr_core::types::PartitionConfig;
use rr_network::client_service::{ClientDispatch, LinearizableReadResult};
use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};
use rr_raft::ServiceStateMachineAdapter;
use rr_router::router::ShardRouter;
use rr_server::dispatch::ClientDispatchImpl;

const GROUP: u64 = 1;
const PARTITION: u64 = 7;

/// Minimal service: echo the payload back (pure in-memory work, so the
/// measurement is framework overhead, not application logic).
struct EchoService;

#[async_trait]
impl Service for EchoService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        Ok(request)
    }
    fn name(&self) -> &str {
        "dispatch-bench"
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

/// Start a single-node group, initialize membership, and wait for
/// self-election (same pattern as `rr-server` dispatch unit tests).
async fn setup() -> ClientDispatchImpl {
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

    let config = Arc::new(MemConfigProvider::new());
    config
        .add_partition(
            PARTITION,
            PartitionConfig::SingleId(PARTITION),
            GROUP,
            vec![1],
        )
        .await;
    let router = Arc::new(ShardRouter::new(config));
    ClientDispatchImpl::new(mgr, router)
}

fn bench_submit_write(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dispatch = rt.block_on(setup());
    // Unique request ids so every keyed write takes the New path.
    let request_counter = AtomicU64::new(1);

    let mut group = c.benchmark_group("dispatch_submit_write");
    for payload_size in [64usize, 1024] {
        let payload = vec![0u8; payload_size];

        group.bench_with_input(
            BenchmarkId::new("unkeyed", payload_size),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || payload.clone(),
                    |p| {
                        let result = rt
                            .block_on(dispatch.submit_write(PARTITION, p, None))
                            .unwrap();
                        // Plain assert (not debug_assert): bench profile has
                        // debug-assertions off, and a redirect here would
                        // silently measure the wrong path.
                        assert!(result.success);
                        criterion::black_box(result);
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("keyed", payload_size),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || {
                        let key = IdempotencyKey {
                            request_type: "bench".to_string(),
                            request_id: request_counter.fetch_add(1, Ordering::Relaxed),
                            client_id: None,
                        };
                        (payload.clone(), key)
                    },
                    |(p, key)| {
                        let result = rt
                            .block_on(dispatch.submit_write(PARTITION, p, Some(key)))
                            .unwrap();
                        assert!(result.success);
                        criterion::black_box(result);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_submit_linearizable_read(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dispatch = rt.block_on(setup());

    let mut group = c.benchmark_group("dispatch_linearizable_read");
    for payload_size in [64usize, 1024] {
        let payload = vec![0u8; payload_size];
        group.bench_with_input(
            BenchmarkId::new("served", payload_size),
            &payload,
            |b, payload| {
                b.iter_batched(
                    || payload.clone(),
                    |p| {
                        let result = rt
                            .block_on(dispatch.submit_linearizable_read(PARTITION, p))
                            .unwrap();
                        assert!(matches!(result, LinearizableReadResult::Served(_)));
                        criterion::black_box(result);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_submit_write, bench_submit_linearizable_read);
criterion_main!(benches);
