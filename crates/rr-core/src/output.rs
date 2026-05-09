use serde::{Deserialize, Serialize};

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
        topic: String,
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
    pub target: OutputTarget,
    pub payload: Vec<u8>,
}

impl Output {
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
    pub fn from_response(response: Vec<u8>) -> Self {
        Self {
            response,
            outputs: vec![],
        }
    }

    pub fn with_outputs(response: Vec<u8>, outputs: Vec<Output>) -> Self {
        Self { response, outputs }
    }
}
