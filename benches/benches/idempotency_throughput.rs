//! Benchmark: [`IdempotencyLayer`] decide + record throughput.
//!
//! The idempotency layer runs synchronously on the Raft apply path, so its
//! per-call overhead directly affects write throughput. Two scenarios are
//! measured: fresh writes (New decisions) and replays (cache-hit path).

use criterion::{criterion_group, criterion_main, Criterion};
use rr_core::{IdempotencyDecision, IdempotencyKey};
use rr_raft::IdempotencyLayer;

fn make_key(i: u64) -> IdempotencyKey {
    IdempotencyKey {
        request_type: "write".to_string(),
        request_id: i,
        client_id: None,
    }
}

fn bench_idempotency_decide_record_1000(c: &mut Criterion) {
    c.bench_function("idempotency_decide_new_record_1000", |b| {
        b.iter(|| {
            let mut layer = IdempotencyLayer::new(2000);
            for i in 0u64..1000 {
                let key = make_key(i);
                let decision = layer.decide(&key);
                assert!(matches!(decision, IdempotencyDecision::New));
                layer.record(&key, vec![42u8]);
            }
        });
    });
}

fn bench_idempotency_decide_replay_1000(c: &mut Criterion) {
    let mut layer = IdempotencyLayer::new(2000);
    for i in 0u64..1000 {
        let key = make_key(i);
        layer.record(&key, vec![42u8]);
    }

    c.bench_function("idempotency_decide_replay_1000", |b| {
        b.iter(|| {
            for i in 0u64..1000 {
                let key = make_key(i);
                let decision = layer.decide(&key);
                assert!(matches!(decision, IdempotencyDecision::Replay(_)));
            }
        });
    });
}

criterion_group!(
    benches,
    bench_idempotency_decide_record_1000,
    bench_idempotency_decide_replay_1000
);
criterion_main!(benches);
