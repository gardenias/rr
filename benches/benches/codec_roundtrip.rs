//! Benchmark: bincode encode+decode 1000 times.

use criterion::{criterion_group, criterion_main, Criterion};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestPayload {
    id: u64,
    key: String,
    data: Vec<u8>,
}

fn bench_codec_roundtrip(c: &mut Criterion) {
    let payload = TestPayload {
        id: 42,
        key: "bench-key".to_string(),
        data: vec![0u8; 256],
    };

    c.bench_function("codec_roundtrip_1000", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let encoded =
                    bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
                let (_decoded, _): (TestPayload, _) =
                    bincode::serde::decode_from_slice(&encoded, bincode::config::standard())
                        .unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_codec_roundtrip);
criterion_main!(benches);
