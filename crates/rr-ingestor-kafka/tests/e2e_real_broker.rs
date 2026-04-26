//! End-to-end smoke test against a real Kafka broker.
//!
//! Skipped unless `RR_TEST_KAFKA_BOOTSTRAP` is set to a reachable
//! bootstrap server list (e.g. `localhost:9092`). To run:
//!
//! ```bash
//! # 1. Start a Kafka broker (e.g. via docker compose):
//! #    docker run -d -p 9092:9092 --name kafka apache/kafka:3.9.0
//! # 2. Create the test topic:
//! #    docker exec kafka /opt/kafka/bin/kafka-topics.sh \
//! #      --create --topic input_1 --partitions 1 --bootstrap-server localhost:9092
//! # 3. Run the test with env var + feature flag:
//! RR_TEST_KAFKA_BOOTSTRAP=localhost:9092 \
//!   cargo test -p rr-ingestor-kafka --features rdkafka \
//!   --test e2e_real_broker -- --include-ignored
//! ```
//!
//! Each test self-checks the env var; without it they early-return as
//! `Ok` so unconditional `cargo test` runs stay green.

#![cfg(feature = "rdkafka")]

use rr_core::ingestor::CommandIngestor;
use rr_ingestor_kafka::{KafkaIngestor, KafkaIngestorConfig};

fn bootstrap_or_skip() -> Option<Vec<String>> {
    let raw = std::env::var("RR_TEST_KAFKA_BOOTSTRAP").ok()?;
    if raw.is_empty() {
        return None;
    }
    Some(raw.split(',').map(|s| s.trim().to_string()).collect())
}

#[tokio::test]
#[ignore = "requires a live Kafka broker; opt in via RR_TEST_KAFKA_BOOTSTRAP env var"]
async fn ingestor_constructs_against_live_broker() {
    let Some(bootstrap_servers) = bootstrap_or_skip() else {
        eprintln!("RR_TEST_KAFKA_BOOTSTRAP unset; skipping live-broker test");
        return;
    };
    let cfg = KafkaIngestorConfig {
        bootstrap_servers,
        ..Default::default()
    };
    let ingestor = KafkaIngestor::new(1, cfg)
        .await
        .expect("KafkaIngestor::new should succeed against a live broker");
    drop(ingestor);
}

#[tokio::test]
#[ignore = "requires a live Kafka broker; opt in via RR_TEST_KAFKA_BOOTSTRAP env var"]
async fn ingestor_seek_to_beginning_does_not_error() {
    let Some(bootstrap_servers) = bootstrap_or_skip() else {
        eprintln!("RR_TEST_KAFKA_BOOTSTRAP unset; skipping live-broker test");
        return;
    };
    let cfg = KafkaIngestorConfig {
        bootstrap_servers,
        ..Default::default()
    };
    let mut ingestor = KafkaIngestor::new(1, cfg).await.unwrap();
    ingestor
        .seek(None)
        .await
        .expect("seek(None) should succeed on a live broker");
}
