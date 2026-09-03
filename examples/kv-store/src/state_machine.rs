//! KV Store StateMachine — implements the low-level [`StateMachine`] trait directly.
//!
//! Unlike `KvService`, this implementation does not use the `ServiceStateMachineAdapter`.
//! It owns its state mutably and tracks the last applied log index, making it suitable
//! for use as a raw Raft state machine.

use std::collections::HashMap;

use async_trait::async_trait;
use bincode::config::standard;
use tracing::{debug, instrument};

use rr_core::error::{CodecError, RrError};
use rr_core::state_machine::StateMachine;
use rr_core::types::LogIndex;

use crate::types::{KvRequest, KvResponse};

/// In-memory KV store that directly implements [`StateMachine`].
///
/// Applies Raft log entries as serialized [`KvRequest`] values and
/// returns serialized [`KvResponse`] values.
pub struct KvStateMachine {
    store: HashMap<String, Vec<u8>>,
    last_applied: Option<LogIndex>,
}

impl KvStateMachine {
    /// Create a new, empty `KvStateMachine`.
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
            last_applied: None,
        }
    }
}

impl Default for KvStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateMachine for KvStateMachine {
    /// Apply a committed Raft log entry.
    ///
    /// `entry` is encoded as `(LogIndex, KvRequest)` — the first 8 bytes are the
    /// `LogIndex` (little-endian u64), followed by the bincode-encoded `KvRequest`.
    /// Returns bincode-encoded `KvResponse`.
    #[instrument(skip(self, entry))]
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
        // First 8 bytes = log index (u64 little-endian).
        if entry.len() < 8 {
            return Err(RrError::Codec(CodecError::DeserializeFailed(
                "entry too short: expected at least 8 bytes for log index".into(),
            )));
        }
        let (index_bytes, payload) = entry.split_at(8);
        let index = u64::from_le_bytes(index_bytes.try_into().unwrap());

        let (req, _): (KvRequest, _) = bincode::serde::decode_from_slice(payload, standard())
            .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;

        let response = match req {
            KvRequest::Put { key, value } => {
                debug!(%key, "KvStateMachine Put");
                self.store.insert(key, value);
                KvResponse::none()
            }
            KvRequest::Get { key } => {
                debug!(%key, "KvStateMachine Get");
                KvResponse {
                    value: self.store.get(&key).cloned(),
                }
            }
            KvRequest::Delete { key } => {
                debug!(%key, "KvStateMachine Delete");
                self.store.remove(&key);
                KvResponse::none()
            }
            KvRequest::Increment { key, delta } => {
                debug!(%key, delta, "KvStateMachine Increment");
                // Same semantics as KvService: counters are decimal UTF-8
                // strings; an unparseable existing value counts as 0.
                let current: i64 = self
                    .store
                    .get(&key)
                    .and_then(|v| std::str::from_utf8(v).ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let next = current + delta;
                let encoded = next.to_string().into_bytes();
                self.store.insert(key, encoded.clone());
                KvResponse::some(encoded)
            }
        };

        self.last_applied = Some(index);

        bincode::serde::encode_to_vec(&response, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        bincode::serde::encode_to_vec(&self.store, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError> {
        let (restored, _): (HashMap<String, Vec<u8>>, _) =
            bincode::serde::decode_from_slice(snapshot, standard())
                .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;
        self.store = restored;
        Ok(())
    }

    async fn last_applied(&self) -> Option<LogIndex> {
        self.last_applied
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Encode a `(LogIndex, KvRequest)` into the wire format expected by `apply`.
pub fn encode_entry(index: LogIndex, req: &KvRequest) -> Vec<u8> {
    let mut buf = index.to_le_bytes().to_vec();
    let payload = bincode::serde::encode_to_vec(req, standard()).unwrap();
    buf.extend_from_slice(&payload);
    buf
}

/// Decode the `KvResponse` returned by `apply`.
pub fn decode_response(bytes: &[u8]) -> KvResponse {
    let (r, _): (KvResponse, _) = bincode::serde::decode_from_slice(bytes, standard()).unwrap();
    r
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_apply_put_and_get() {
        let mut sm = KvStateMachine::new();

        let entry = encode_entry(
            1,
            &KvRequest::Put {
                key: "foo".into(),
                value: b"bar".to_vec(),
            },
        );
        let resp_bytes = sm.apply(&entry).await.unwrap();
        let resp = decode_response(&resp_bytes);
        assert_eq!(resp.value, None); // Put returns no value

        let entry = encode_entry(2, &KvRequest::Get { key: "foo".into() });
        let resp_bytes = sm.apply(&entry).await.unwrap();
        let resp = decode_response(&resp_bytes);
        assert_eq!(resp.value, Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn test_apply_delete_removes_key() {
        let mut sm = KvStateMachine::new();

        sm.apply(&encode_entry(
            1,
            &KvRequest::Put {
                key: "x".into(),
                value: vec![9],
            },
        ))
        .await
        .unwrap();
        sm.apply(&encode_entry(2, &KvRequest::Delete { key: "x".into() }))
            .await
            .unwrap();

        let resp_bytes = sm
            .apply(&encode_entry(3, &KvRequest::Get { key: "x".into() }))
            .await
            .unwrap();
        let resp = decode_response(&resp_bytes);
        assert_eq!(resp.value, None);
    }

    #[tokio::test]
    async fn test_get_missing_key_returns_none() {
        let mut sm = KvStateMachine::new();
        let resp_bytes = sm
            .apply(&encode_entry(1, &KvRequest::Get { key: "nope".into() }))
            .await
            .unwrap();
        assert_eq!(decode_response(&resp_bytes).value, None);
    }

    #[tokio::test]
    async fn test_snapshot_and_restore() {
        let mut sm = KvStateMachine::new();
        sm.apply(&encode_entry(
            1,
            &KvRequest::Put {
                key: "snap".into(),
                value: b"shot".to_vec(),
            },
        ))
        .await
        .unwrap();

        let snapshot = sm.snapshot().await.unwrap();

        let mut sm2 = KvStateMachine::new();
        sm2.restore(&snapshot).await.unwrap();

        let resp_bytes = sm2
            .apply(&encode_entry(2, &KvRequest::Get { key: "snap".into() }))
            .await
            .unwrap();
        assert_eq!(decode_response(&resp_bytes).value, Some(b"shot".to_vec()));
    }

    #[tokio::test]
    async fn test_last_applied_tracking() {
        let mut sm = KvStateMachine::new();
        assert_eq!(sm.last_applied().await, None);

        sm.apply(&encode_entry(
            42,
            &KvRequest::Put {
                key: "a".into(),
                value: b"b".to_vec(),
            },
        ))
        .await
        .unwrap();
        assert_eq!(sm.last_applied().await, Some(42));

        sm.apply(&encode_entry(99, &KvRequest::Delete { key: "a".into() }))
            .await
            .unwrap();
        assert_eq!(sm.last_applied().await, Some(99));
    }
}
