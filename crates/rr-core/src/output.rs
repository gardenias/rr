//! Side-effect outputs produced by committed Raft entries.
//!
//! A [`Service`](crate::Service) may return [`Output`]s alongside its
//! response via [`HandleResult`]; the framework's output runner delivers
//! each output to its [`OutputTarget`] at-least-once after the originating
//! Raft entry is committed and applied.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::RrError;
use crate::types::GroupId;

/// Where an Output should be delivered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputTarget {
    /// Send as a write to another rr group (via RrClient).
    RaftWrite {
        /// Bootstrap addresses of the target cluster node(s).
        addrs: Vec<String>,
        /// Target group/partition ID.
        group_id: GroupId,
    },
    /// Publish to a Kafka topic (requires rdkafka feature in the application).
    KafkaPublish {
        /// Kafka topic to publish to.
        topic: String,
        /// Kafka partition to publish to.
        partition: i32,
        /// Optional Kafka message key for partition routing.
        key: Vec<u8>,
    },
}

/// A side effect produced by Service::handle_with_outputs() that must be
/// delivered at-least-once after the Raft entry is committed and applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Output {
    /// Monotonic sequence number assigned by the framework.
    /// Set to 0 when constructed by user; framework assigns actual value.
    pub seq: u64,
    /// Idempotency key. Receivers must deduplicate on this key.
    /// Typically: "{input_idempotency_key}:{output_index}".
    pub idempotency_key: String,
    /// Where the output should be delivered.
    pub target: OutputTarget,
    /// Opaque payload delivered to the target.
    pub payload: Vec<u8>,
}

impl Output {
    /// Create an output with `seq` set to 0; the framework assigns the
    /// actual sequence number when the output is recorded.
    pub fn new(idempotency_key: impl Into<String>, target: OutputTarget, payload: Vec<u8>) -> Self {
        Self {
            seq: 0,
            idempotency_key: idempotency_key.into(),
            target,
            payload,
        }
    }
}

/// Return value of Service::handle_with_outputs().
pub struct HandleResult {
    /// Response bytes sent back to the original caller.
    pub response: Vec<u8>,
    /// Side-effect outputs to deliver after this apply commits.
    pub outputs: Vec<Output>,
}

impl HandleResult {
    /// Build a result with a response and no side-effect outputs.
    pub fn from_response(response: Vec<u8>) -> Self {
        Self {
            response,
            outputs: vec![],
        }
    }

    /// Build a result with a response and side-effect outputs to deliver
    /// after the apply commits.
    pub fn with_outputs(response: Vec<u8>, outputs: Vec<Output>) -> Self {
        Self { response, outputs }
    }
}

/// Producer abstraction for [`OutputTarget::KafkaPublish`] delivery.
///
/// The framework's output runner calls [`Self::publish`] for each
/// `KafkaPublish` output. Implementations should be idempotent at the
/// Kafka level when possible (e.g. using `enable.idempotence=true` on
/// the underlying producer), since the rr framework guarantees only
/// at-least-once delivery — duplicate `publish` calls may occur after
/// leader transitions or process crashes.
#[async_trait]
pub trait KafkaProducer: Send + Sync {
    /// Publish one message. Returns `Ok(())` only after the broker has
    /// acknowledged the write; transient errors should be returned so
    /// the framework can retry with exponential backoff.
    async fn publish(
        &self,
        topic: &str,
        partition: i32,
        key: &[u8],
        payload: &[u8],
        idempotency_key: &str,
    ) -> Result<(), RrError>;
}

/// `KafkaProducer` that always errors — installed when the application
/// has not wired a real Kafka producer. Output runners surfaced a
/// `KafkaPublish` target will retry forever against this stub until
/// the operator either installs a real producer or removes the
/// `KafkaPublish` outputs from the service.
pub struct NoopKafkaProducer;

#[async_trait]
impl KafkaProducer for NoopKafkaProducer {
    async fn publish(
        &self,
        topic: &str,
        _partition: i32,
        _key: &[u8],
        _payload: &[u8],
        _idempotency_key: &str,
    ) -> Result<(), RrError> {
        Err(RrError::Application(format!(
            "KafkaPublish to topic '{topic}' attempted but no KafkaProducer is configured"
        )))
    }
}
