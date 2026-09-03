//! KV Store request and response types.

use serde::{Deserialize, Serialize};

/// A KV store operation proposed through Raft.
///
/// Wire-format note: bincode encodes the variant by its ordinal, so new
/// variants MUST be appended at the end — reordering breaks every existing
/// consumer (e.g. rr-admin's repl mirrors this enum).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvRequest {
    /// Insert or update a key.
    Put { key: String, value: Vec<u8> },
    /// Retrieve a key.
    Get { key: String },
    /// Delete a key.
    Delete { key: String },
    /// Add `delta` to a numeric counter, creating the key at `delta` when absent.
    ///
    /// The counter is stored as its decimal UTF-8 string (e.g. `b"42"`), so a
    /// plain `Get` — or the rr-admin repl `get` — prints it directly. An
    /// existing value that does not parse as an `i64` (e.g. raw bytes from a
    /// `Put`) is treated as `0`. Returns the NEW counter value in
    /// [`KvResponse::value`], encoded the same way.
    ///
    /// Kept LAST in the enum: bincode variant ordinals are positional, and
    /// `Put`/`Get`/`Delete` ordinals must not shift.
    Increment { key: String, delta: i64 },
}

/// The result of a KV store operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvResponse {
    /// The value associated with the key, if any.
    ///
    /// - `Put` → `None` (no return value)
    /// - `Get` → `Some(value)` if found, `None` if not found
    /// - `Delete` → `None`
    /// - `Increment` → `Some(new counter value)` as decimal UTF-8 bytes
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
