//! Benchmark: [`FramedEntry`] encode / decode — the actual hot-path framing
//! used for every Raft log entry since v0.3.0.
//!
//! Complements the generic `codec_roundtrip` bench with the real framework
//! type so that changes to the envelope format are reflected in CI regression
//! numbers.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rr_core::framed_entry::FramedEntry;
use rr_core::IdempotencyKey;

fn bench_framing_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("framed_entry_encode");

    for payload_size in [64usize, 256, 1024, 4096] {
        let payload = vec![0u8; payload_size];
        let entry = FramedEntry::from_payload(payload);

        group.bench_with_input(
            BenchmarkId::new("plain", payload_size),
            &entry,
            |b, e| {
                b.iter(|| {
                    criterion::black_box(e.encode().unwrap());
                });
            },
        );

        let key = IdempotencyKey {
            request_type: "write".to_string(),
            request_id: 1,
            client_id: Some("bench".to_string()),
        };
        let entry_with_key = FramedEntry {
            version: rr_core::framed_entry::FRAMED_ENTRY_VERSION,
            offset: None,
            idem_key: Some(key),
            payload: vec![0u8; payload_size],
        };

        group.bench_with_input(
            BenchmarkId::new("with_idem_key", payload_size),
            &entry_with_key,
            |b, e| {
                b.iter(|| {
                    criterion::black_box(e.encode().unwrap());
                });
            },
        );
    }

    group.finish();
}

fn bench_framing_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("framed_entry_decode");

    for payload_size in [64usize, 256, 1024, 4096] {
        let bytes = FramedEntry::from_payload(vec![0u8; payload_size])
            .encode()
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new("plain", payload_size),
            &bytes,
            |b, raw| {
                b.iter(|| {
                    criterion::black_box(FramedEntry::decode(raw).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn bench_framing_roundtrip_1000(c: &mut Criterion) {
    let payload = vec![0u8; 256];

    c.bench_function("framing_roundtrip_1000x256b", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let entry = FramedEntry::from_payload(payload.clone());
                let bytes = entry.encode().unwrap();
                criterion::black_box(FramedEntry::decode(&bytes).unwrap());
            }
        });
    });
}

criterion_group!(
    benches,
    bench_framing_encode,
    bench_framing_decode,
    bench_framing_roundtrip_1000
);
criterion_main!(benches);
