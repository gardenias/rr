//! Benchmark: [`rr_core::batch`] frame encode / decode / validate — the
//! client-write batching hot path (rr-client opt-in batching).
//!
//! A batch frame packs several unkeyed application payloads into ONE Raft
//! log entry; the apply side splits it (`decode_batch`) and ingress
//! validation inspects it without materializing items
//! (`validate_batch_frame`). Item counts follow the production envelope:
//! the client batcher's default caps at `BatchConfig::default().max_entries`
//! = 64 items, but the field is public and configurable — 256 here exercises
//! the frame beyond the DEFAULT envelope, not a hard bound.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rr_core::batch::{decode_batch, encode_batch, validate_batch_frame};

/// Item counts exercised: 1 (degenerate single-item batch), 8 (typical),
/// 64 and 256 (bounded upper end).
const ITEM_COUNTS: [usize; 4] = [1, 8, 64, 256];
/// Representative per-item payload size (small KV-style writes).
const ITEM_BYTES: usize = 128;

fn make_items(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut item = vec![0u8; ITEM_BYTES];
            // Distinct leading bytes per item so encode cannot collapse
            // to a repeated-slice memcpy pattern.
            item[..8].copy_from_slice(&(i as u64).to_be_bytes());
            item
        })
        .collect()
}

fn bench_batch_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_encode");
    for count in ITEM_COUNTS {
        let items = make_items(count);
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new("items", count), &items, |b, items| {
            b.iter(|| {
                criterion::black_box(encode_batch(criterion::black_box(items)));
            });
        });
    }
    group.finish();
}

fn bench_batch_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_decode");
    for count in ITEM_COUNTS {
        let frame = encode_batch(&make_items(count));
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new("items", count), &frame, |b, frame| {
            b.iter(|| {
                let decoded = decode_batch(criterion::black_box(frame))
                    .unwrap()
                    .expect("frame is a batch");
                assert_eq!(decoded.len(), count);
                criterion::black_box(decoded);
            });
        });
    }
    group.finish();
}

/// Ingress-validation seam: structural check without per-item allocation —
/// the path every unkeyed client payload takes before propose.
fn bench_batch_validate(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_validate");
    for count in ITEM_COUNTS {
        let frame = encode_batch(&make_items(count));
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new("items", count), &frame, |b, frame| {
            b.iter(|| {
                let valid = validate_batch_frame(criterion::black_box(frame)).unwrap();
                assert!(valid.is_some());
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_batch_encode,
    bench_batch_decode,
    bench_batch_validate
);
criterion_main!(benches);
