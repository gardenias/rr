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
//! use rr_ingestor_kafka::{KafkaIngestor, KafkaIngestorConfig};
//! use std::sync::Arc;
//!
//! let mgr: Arc<rr_raft::RaftGroupManager> = /* … */;
//! let cfg = KafkaIngestorConfig {
//!     bootstrap_servers: vec!["broker1:9092".into()],
//!     ..Default::default()
//! };
//! let ingestor = KafkaIngestor::new(7, cfg).await?;
//! mgr.attach_ingestor(7, Box::new(ingestor)).await?;
//! ```

pub mod config;
pub mod mock;

pub use config::{KafkaIngestorConfig, KafkaSecurityConfig, SecurityProtocol};
pub use mock::{MockKafkaHandle, MockKafkaIngestor};

#[cfg(feature = "rdkafka")]
pub mod backend;

#[cfg(feature = "rdkafka")]
pub use backend::{KafkaIngestor, KafkaIngestorError};
