//! KV Store request and response types.

use serde::{Deserialize, Serialize};

/// A KV store operation proposed through Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvRequest {
    /// Insert or update a key.
    Put { key: String, value: Vec<u8> },
    /// Retrieve a key.
    Get { key: String },
    /// Delete a key.
    Delete { key: String },
}

/// The result of a KV store operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvResponse {
    /// The value associated with the key, if any.
    ///
    /// - `Put` → `None` (no return value)
    /// - `Get` → `Some(value)` if found, `None` if not found
    /// - `Delete` → `None`
    pub value: Option<Vec<u8>>,
}

impl KvResponse {
    /// Construct a response with no value.
    pub fn none() -> Self {
        Self { value: None }
    }

    /// Construct a response with a value.
    pub fn some(value: Vec<u8>) -> Self {
        Self { value: Some(value) }
    }
}
