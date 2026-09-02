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
//! use rr_raft::ingestor_runner::{AutoIdempotency, OffsetMapError};
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
//!         offset_to_request_id: Box::new(|b| {
//!             kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
//!         }),
//!     },
//! ).await?;
//! ```

pub mod config;
pub mod mock;

pub use config::{KafkaIngestorConfig, KafkaSecurityConfig, SecurityProtocol};
pub use mock::{MockKafkaHandle, MockKafkaIngestor};

/// Error returned by [`kafka_offset_to_request_id`] when the offset bytes
/// cannot be mapped to a monotone `u64` request id.
///
/// Mapping failures are **fail-closed** (ADR A-0011 C7): the ingestor
/// supervisor exits with an error rather than proposing a command whose
/// request id regressed or collided with another message.
#[derive(Debug, thiserror::Error)]
pub enum KafkaOffsetError {
    /// The bytes are not a bincode-encoded `i64`.
    #[error("offset bytes are not a bincode-encoded i64: {0}")]
    DecodeFailed(String),
    /// Negative offsets are Kafka sentinel values (e.g. `INVALID_OFFSET =
    /// -1001`). Clamping them to `0` would collide with the real first
    /// message of the topic and poison the idempotency watermark.
    #[error("negative kafka offset {0} (sentinel value); refusing to map")]
    NegativeOffset(i64),
}

/// Decode a bincode-encoded `i64` Kafka offset (the on-the-wire form
/// used by [`MockKafkaIngestor`] and the rdkafka backend) into a
/// monotone `u64` suitable for [`IdempotencyKey::request_id`].
///
/// Use this with
/// [`rr_raft::ingestor_runner::AutoIdempotency::offset_to_request_id`]
/// when attaching a Kafka ingestor to enable framework-managed
/// exactly-once apply on top of Kafka's at-least-once delivery:
///
/// ```ignore
/// offset_to_request_id: Box::new(|b| kafka_offset_to_request_id(b).map_err(OffsetMapError::new))
/// ```
///
/// Fail-closed contract: decode failures and negative offsets (Kafka
/// sentinel values like `INVALID_OFFSET = -1001`) are returned as
/// [`KafkaOffsetError`]. Real messages from a committed consumer position
/// always carry `offset >= 0`, so legitimate traffic is unaffected; a
/// sentinel or corrupt record stops the supervisor instead of silently
/// mapping to request id `0`, which would either drop the message at the
/// idempotency layer or collide with the topic's genuine first message.
///
/// [`IdempotencyKey::request_id`]: rr_core::idempotency::IdempotencyKey::request_id
pub fn kafka_offset_to_request_id(bytes: &[u8]) -> Result<u64, KafkaOffsetError> {
    let (i, _): (i64, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| KafkaOffsetError::DecodeFailed(e.to_string()))?;
    if i < 0 {
        return Err(KafkaOffsetError::NegativeOffset(i));
    }
    Ok(i as u64)
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
        assert_eq!(kafka_offset_to_request_id(&encode_i64(0)).unwrap(), 0);
        assert_eq!(kafka_offset_to_request_id(&encode_i64(42)).unwrap(), 42);
        assert_eq!(
            kafka_offset_to_request_id(&encode_i64(i64::MAX)).unwrap(),
            i64::MAX as u64
        );
    }

    #[test]
    fn helper_rejects_negative_sentinels() {
        for v in [-1_i64, -1001, i64::MIN] {
            let result = kafka_offset_to_request_id(&encode_i64(v));
            assert!(
                matches!(result, Err(KafkaOffsetError::NegativeOffset(n)) if n == v),
                "negative sentinel {v} must be rejected, got {result:?}"
            );
        }
    }

    #[test]
    fn helper_is_monotone_for_increasing_offsets() {
        let offsets = [0_i64, 1, 10, 100, 1000, 100_000, i64::MAX / 2];
        let mut prev = 0_u64;
        for o in offsets {
            let id = kafka_offset_to_request_id(&encode_i64(o)).unwrap();
            assert!(
                id >= prev,
                "expected monotone IDs for monotone offsets; offset {o} → {id} not >= {prev}"
            );
            prev = id;
        }
    }

    #[test]
    fn helper_rejects_garbage_bytes() {
        // Empty input and reserved varint discriminants cannot decode as an
        // i64 (bincode standard config uses varint + zigzag).
        for bytes in [&b""[..], &[0xff_u8; 32][..]] {
            let result = kafka_offset_to_request_id(bytes);
            assert!(
                matches!(result, Err(KafkaOffsetError::DecodeFailed(_))),
                "garbage bytes must be rejected as decode failures, got {result:?}"
            );
        }
        // A lone odd varint byte zigzag-decodes to a negative value and must
        // be rejected as a sentinel, not clamped.
        let result = kafka_offset_to_request_id(&[1_u8, 2, 3]);
        assert!(
            matches!(result, Err(KafkaOffsetError::NegativeOffset(_))),
            "odd zigzag byte must be rejected as negative, got {result:?}"
        );
    }
}
