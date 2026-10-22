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
#[derive(Debug, Clone)]
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── Output construction contract ──────────────────────────────────

    // INV-4 chain 1: the framework (service_adapter) assigns the real `seq`
    // when the output is recorded; user-constructed outputs must start at 0
    // so the assignment is distinguishable from an explicit sequence.
    #[test]
    fn output_new_assigns_zero_seq_and_carries_fields() {
        let out = Output::new(
            "req-1:0",
            OutputTarget::KafkaPublish {
                topic: "orders".to_string(),
                partition: 3,
                key: b"key".to_vec(),
            },
            b"payload".to_vec(),
        );
        assert_eq!(out.seq, 0, "framework assigns seq; user-built outputs start at 0");
        assert_eq!(out.idempotency_key, "req-1:0");
        assert_eq!(out.payload, b"payload".to_vec());
        match &out.target {
            OutputTarget::KafkaPublish {
                topic,
                partition,
                key,
            } => {
                assert_eq!(topic, "orders");
                assert_eq!(*partition, 3);
                assert_eq!(key, &b"key".to_vec());
            }
            other => panic!("expected KafkaPublish target, got: {other:?}"),
        }
    }

    #[test]
    fn output_raft_write_target_carries_addrs_and_group() {
        let out = Output::new(
            "req-2:0",
            OutputTarget::RaftWrite {
                addrs: vec!["127.0.0.1:9001".to_string()],
                group_id: 7,
            },
            b"x".to_vec(),
        );
        match &out.target {
            OutputTarget::RaftWrite { addrs, group_id } => {
                assert_eq!(addrs, &vec!["127.0.0.1:9001".to_string()]);
                assert_eq!(*group_id, 7);
            }
            other => panic!("expected RaftWrite target, got: {other:?}"),
        }
    }

    // ── HandleResult builders ─────────────────────────────────────────

    #[test]
    fn handle_result_from_response_has_empty_outputs() {
        let result = HandleResult::from_response(b"resp".to_vec());
        assert_eq!(result.response, b"resp".to_vec());
        assert!(result.outputs.is_empty());
    }

    #[test]
    fn handle_result_with_outputs_carries_both() {
        let output = Output::new(
            "req-3:0",
            OutputTarget::RaftWrite {
                addrs: vec!["10.0.0.1:50051".to_string()],
                group_id: 11,
            },
            b"out".to_vec(),
        );
        let result = HandleResult::with_outputs(b"resp".to_vec(), vec![output.clone()]);
        assert_eq!(result.response, b"resp".to_vec());
        assert_eq!(result.outputs.len(), 1);
        let carried = &result.outputs[0];
        assert_eq!(carried.seq, output.seq);
        assert_eq!(carried.idempotency_key, output.idempotency_key);
        assert_eq!(carried.payload, output.payload);
        assert_eq!(
            format!("{:?}", carried.target),
            format!("{:?}", output.target)
        );
    }

    // ── serde contract ────────────────────────────────────────────────

    // INV-4 chain 5: outbox state (including user outputs) is persisted in
    // FramedSnapshot.framework_state; pin the bincode field-level contract so
    // a serde drift cannot silently break snapshot restore across versions.
    #[test]
    fn output_bincode_roundtrip_preserves_fields() {
        for target in [
            OutputTarget::RaftWrite {
                addrs: vec!["10.0.0.1:50051".to_string(), "10.0.0.2:50051".to_string()],
                group_id: 42,
            },
            OutputTarget::KafkaPublish {
                topic: "orders".to_string(),
                partition: 5,
                key: b"k".to_vec(),
            },
        ] {
            let out = Output {
                seq: 17,
                idempotency_key: "req-9:2".to_string(),
                target,
                payload: b"payload".to_vec(),
            };
            let encoded =
                bincode::serde::encode_to_vec(&out, bincode::config::standard()).unwrap();
            let (decoded, n): (Output, _) =
                bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
            assert_eq!(n, encoded.len());
            // Output intentionally has no PartialEq; byte-identical re-encode
            // plus field asserts pins the wire contract just as strongly.
            let re_encoded =
                bincode::serde::encode_to_vec(&decoded, bincode::config::standard()).unwrap();
            assert_eq!(re_encoded, encoded, "re-encode must be byte-identical");
            assert_eq!(decoded.seq, out.seq);
            assert_eq!(decoded.idempotency_key, out.idempotency_key);
            assert_eq!(decoded.payload, out.payload);
            assert_eq!(
                format!("{:?}", decoded.target),
                format!("{:?}", out.target)
            );
        }
    }

    // ── KafkaProducer surface ─────────────────────────────────────────

    #[test]
    fn kafka_producer_is_object_safe() {
        let _: Option<Box<dyn KafkaProducer>> = None;
    }

    // INV-4: with no producer wired, a KafkaPublish target must fail LOUDLY
    // — the output runner retries forever against this stub, and the error
    // names the topic so operators can find the misconfigured service.
    #[tokio::test]
    async fn noop_kafka_producer_errors_naming_topic() {
        let producer = NoopKafkaProducer;
        let err = producer
            .publish("orders", 0, b"key", b"payload", "idem-1")
            .await
            .expect_err("stub producer must refuse delivery");
        match err {
            RrError::Application(msg) => {
                assert!(
                    msg.contains("orders"),
                    "error must name the topic for operability, got: {msg}"
                );
                assert!(
                    msg.contains("no KafkaProducer is configured"),
                    "error must explain the missing wiring, got: {msg}"
                );
            }
            other => panic!("expected RrError::Application, got: {other:?}"),
        }
    }
}
