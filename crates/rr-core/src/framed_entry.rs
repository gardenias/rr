//! Per-Raft-log-entry payload framing.
//!
//! From v0.3.0 onward, `ServiceStateMachineAdapter` wraps each user
//! payload submitted via `client_write` (or pushed by a
//! [`crate::ingestor::CommandIngestor`]) in a [`FramedEntry`] envelope
//! before handing it to OpenRaft. The envelope carries:
//!
//! - **`offset`** — bincode-encoded ingestor offset bytes when the entry
//!   came from an ingestor (Wave 4 Tier 3). `None` for entries proposed
//!   via gRPC.
//! - **`idem_key`** — [`crate::idempotency::IdempotencyKey`] when the
//!   client opted in (Wave 4 Tier 2). `None` for unkeyed requests, which
//!   skip the idempotency layer entirely (preserving v0.2.x semantics
//!   for downstream services that build their own dedup).
//! - **`payload`** — the user-supplied bytes that get handed to
//!   `Service::handle`.
//!
//! Adapters and the apply path identify framed entries by the
//! [`FRAMED_ENTRY_MAGIC`] prefix. Entries lacking the prefix are
//! treated as legacy raw user payloads (v0.2.x compatibility).

use serde::{Deserialize, Serialize};

use crate::error::{CodecError, RrError};
use crate::idempotency::IdempotencyKey;

/// Magic byte prefix identifying a framed entry. Distinct from
/// [`crate::framed_snapshot::FRAMED_SNAPSHOT_MAGIC`] so the two
/// envelopes can never be mistaken for each other.
pub const FRAMED_ENTRY_MAGIC: [u8; 8] = *b"\0rrENT01";

/// Per-entry envelope wrapping user payload + framework metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FramedEntry {
    /// Bincode-encoded ingestor offset, opaque to the framework. `Some`
    /// iff this entry was produced by a `CommandIngestor`.
    pub offset: Option<Vec<u8>>,
    /// Client-supplied idempotency key. `Some` iff the client opted in
    /// (or `auto_idempotency` derived a key from the ingestor offset).
    pub idem_key: Option<IdempotencyKey>,
    /// User-supplied bytes; passed verbatim to `Service::handle`.
    pub payload: Vec<u8>,
}

impl FramedEntry {
    /// Create an envelope with no offset and no idempotency key —
    /// the most common shape, equivalent to v0.2.x semantics.
    pub fn from_payload(payload: Vec<u8>) -> Self {
        Self {
            offset: None,
            idem_key: None,
            payload,
        }
    }

    /// Attach an idempotency key to this envelope.
    pub fn with_idem_key(mut self, key: IdempotencyKey) -> Self {
        self.idem_key = Some(key);
        self
    }

    /// Attach an ingestor offset to this envelope.
    pub fn with_offset(mut self, offset: Vec<u8>) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Encode as `[magic][bincode body]`.
    pub fn encode(&self) -> Result<Vec<u8>, RrError> {
        let body = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))?;
        let mut out = Vec::with_capacity(FRAMED_ENTRY_MAGIC.len() + body.len());
        out.extend_from_slice(&FRAMED_ENTRY_MAGIC);
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Decode an envelope. Returns:
    /// - `Ok(Some(framed))` for bytes with a valid magic prefix and
    ///   well-formed body,
    /// - `Ok(None)` for bytes lacking the prefix (legacy / unframed
    ///   user payloads — caller treats the input as the raw payload),
    /// - `Err` for bytes with the prefix but a malformed body
    ///   (failing-loud here prevents silent state corruption).
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, RrError> {
        if bytes.len() < FRAMED_ENTRY_MAGIC.len() {
            return Ok(None);
        }
        if bytes[..FRAMED_ENTRY_MAGIC.len()] != FRAMED_ENTRY_MAGIC {
            return Ok(None);
        }
        let body = &bytes[FRAMED_ENTRY_MAGIC.len()..];
        let (framed, _) = bincode::serde::decode_from_slice::<FramedEntry, _>(
            body,
            bincode::config::standard(),
        )
        .map_err(|e| {
            RrError::Codec(CodecError::DeserializeFailed(format!(
                "framed entry decode failed: {e}"
            )))
        })?;
        Ok(Some(framed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_payload_roundtrip() {
        let original = FramedEntry::from_payload(b"hello".to_vec());
        let encoded = original.encode().unwrap();
        let decoded = FramedEntry::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded.payload, b"hello");
        assert!(decoded.offset.is_none());
        assert!(decoded.idem_key.is_none());
    }

    #[test]
    fn payload_with_idem_key_roundtrip() {
        let key = IdempotencyKey::new("transfer", 42);
        let original = FramedEntry::from_payload(b"value".to_vec()).with_idem_key(key.clone());
        let encoded = original.encode().unwrap();
        let decoded = FramedEntry::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded.payload, b"value");
        assert_eq!(decoded.idem_key, Some(key));
    }

    #[test]
    fn payload_with_offset_roundtrip() {
        let offset = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let original = FramedEntry::from_payload(b"x".to_vec()).with_offset(offset.clone());
        let encoded = original.encode().unwrap();
        let decoded = FramedEntry::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded.offset, Some(offset));
    }

    #[test]
    fn legacy_unframed_bytes_decode_to_none() {
        let legacy = b"raw user payload from v0.2.x".to_vec();
        let decoded = FramedEntry::decode(&legacy).unwrap();
        assert!(decoded.is_none());
    }

    #[test]
    fn empty_bytes_decode_to_none() {
        let decoded = FramedEntry::decode(&[]).unwrap();
        assert!(decoded.is_none());
    }

    #[test]
    fn malformed_framed_body_returns_err() {
        let mut bytes = Vec::from(FRAMED_ENTRY_MAGIC);
        bytes.extend_from_slice(b"not bincode");
        let result = FramedEntry::decode(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn entry_magic_is_distinct_from_snapshot_magic() {
        // Belt-and-suspenders: ensure the two envelope formats can
        // never be confused at decode time.
        assert_ne!(
            FRAMED_ENTRY_MAGIC,
            crate::framed_snapshot::FRAMED_SNAPSHOT_MAGIC
        );
    }
}
