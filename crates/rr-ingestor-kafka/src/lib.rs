//! Kafka [`rr_core::ingestor::CommandIngestor`] backend.
//!
//! Implements [ADR
//! A-0001](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0001-command-ingestor-abstraction.md)
//! Plan A: each rr Raft group `G` consumes a dedicated Kafka topic
//! (`input_{G}` by default) with a single partition; only the current
//! leader of `G` reads, followers replicate via the Raft log.
//!
//! # Build prerequisites
//!
//! The default crate features ship only the in-memory
//! [`MockKafkaIngestor`] and the public config types — no native
//! dependencies. The real backend is gated behind the `rdkafka`
//! feature, which pulls `rdkafka` with `cmake-build`. That requires
//! `cmake` on the build host (used to compile the bundled
//! librdkafka). On Linux CI:
//!
//! ```bash
//! sudo apt-get install -y cmake
//! cargo build -p rr-ingestor-kafka --features rdkafka
//! ```
//!
//! On macOS:
//!
//! ```bash
//! brew install cmake
//! cargo build -p rr-ingestor-kafka --features rdkafka
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use rr_ingestor_kafka::{KafkaIngestor, KafkaIngestorConfig, kafka_offset_to_request_id};
//! use rr_raft::ingestor_runner::AutoIdempotency;
//! use std::sync::Arc;
//!
//! let mgr: Arc<rr_raft::RaftGroupManager> = /* … */;
//! let cfg = KafkaIngestorConfig {
//!     bootstrap_servers: vec!["broker1:9092".into()],
//!     ..Default::default()
//! };
//! let ingestor = KafkaIngestor::new(7, cfg.clone()).await?;
//! mgr.attach_ingestor_with_auto_idempotency(
//!     7,
//!     Box::new(ingestor),
//!     AutoIdempotency {
//!         request_type: cfg.topic_for(7),
//!         offset_to_request_id: Box::new(kafka_offset_to_request_id),
//!     },
//! ).await?;
//! ```

pub mod config;
pub mod mock;

pub use config::{KafkaIngestorConfig, KafkaSecurityConfig, SecurityProtocol};
pub use mock::{MockKafkaHandle, MockKafkaIngestor};

/// Decode a bincode-encoded `i64` Kafka offset (the on-the-wire form
/// used by [`MockKafkaIngestor`] and the rdkafka backend) into a
/// monotone `u64` suitable for [`IdempotencyKey::request_id`].
///
/// Use this with
/// [`rr_raft::ingestor_runner::AutoIdempotency::offset_to_request_id`]
/// when attaching a Kafka ingestor to enable framework-managed
/// exactly-once apply on top of Kafka's at-least-once delivery.
///
/// Negative offsets (Kafka sentinel values like `INVALID_OFFSET = -1001`)
/// clamp to `0` rather than wrapping into the high `u64` range — this
/// keeps the watermark monotone for legitimate offsets without an
/// instant jump to `u64::MAX - …` on a single sentinel.
///
/// [`IdempotencyKey::request_id`]: rr_core::idempotency::IdempotencyKey::request_id
pub fn kafka_offset_to_request_id(bytes: &[u8]) -> u64 {
    let (i, _): (i64, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .unwrap_or((0_i64, 0));
    i.max(0) as u64
}

#[cfg(feature = "rdkafka")]
pub mod backend;

#[cfg(feature = "rdkafka")]
pub use backend::{KafkaIngestor, KafkaIngestorError};

#[cfg(test)]
mod helper_tests {
    use super::*;

    fn encode_i64(v: i64) -> Vec<u8> {
        bincode::serde::encode_to_vec(v, bincode::config::standard()).unwrap()
    }

    #[test]
    fn helper_round_trips_positive_offsets() {
        assert_eq!(kafka_offset_to_request_id(&encode_i64(0)), 0);
        assert_eq!(kafka_offset_to_request_id(&encode_i64(42)), 42);
        assert_eq!(
            kafka_offset_to_request_id(&encode_i64(i64::MAX)),
            i64::MAX as u64
        );
    }

    #[test]
    fn helper_clamps_negative_sentinels_to_zero() {
        assert_eq!(kafka_offset_to_request_id(&encode_i64(-1)), 0);
        assert_eq!(kafka_offset_to_request_id(&encode_i64(-1001)), 0);
        assert_eq!(kafka_offset_to_request_id(&encode_i64(i64::MIN)), 0);
    }

    #[test]
    fn helper_is_monotone_for_increasing_offsets() {
        let offsets = [0_i64, 1, 10, 100, 1000, 100_000, i64::MAX / 2];
        let mut prev = 0_u64;
        for o in offsets {
            let id = kafka_offset_to_request_id(&encode_i64(o));
            assert!(
                id >= prev,
                "expected monotone IDs for monotone offsets; offset {o} → {id} not >= {prev}"
            );
            prev = id;
        }
    }

    #[test]
    fn helper_returns_zero_for_garbage_bytes() {
        assert_eq!(kafka_offset_to_request_id(b""), 0);
        assert_eq!(kafka_offset_to_request_id(&[0xff_u8; 32]), 0);
    }
}
