//! Framework-level idempotency types.
//!
//! Implements the type contract of [ADR
//! A-0003](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0003-framework-idempotency-layer.md).
//!
//! Clients opt in per-request by attaching an [`IdempotencyKey`]; requests
//! without a key go through the existing path unchanged. The framework
//! deduplicates on `(request_type, request_id)` — same key = same outcome,
//! regardless of how many retries the client sends. The optional
//! `client_id` is **diagnostic only** (logged in collisions) and is NOT
//! part of the dedup identity.
//!
//! The layer's runtime state and serialization implementation live in
//! `rr_raft::idempotency`; this module only defines the wire-level types
//! shared across crates.

use serde::{Deserialize, Serialize};

/// Identity of a logical request for idempotency dedup.
///
/// Two `ClientRequest`s with equal `(request_type, request_id)` are
/// treated as the **same** logical request: the second one returns the
/// cached response from the first instead of re-applying.
///
/// `client_id` is recorded for diagnostics only and is **explicitly not
/// part of the dedup identity** — `PartialEq` / `Eq` / `Hash` are
/// hand-implemented to compare only `(request_type, request_id)` so
/// that `==` matches the layer's behaviour. If two different `client_id`
/// values share a `(request_type, request_id)`, the framework emits an
/// `op="idempotency_collision"` warning to the audit log but still
/// returns the cached response (the request_id space is shared across
/// clients by design — multiple writers MUST coordinate ID allocation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyKey {
    /// User-defined namespace (e.g. `"transfer"`, `"create_user"`). The
    /// `(request_type, request_id)` pair is the dedup identity.
    pub request_type: String,
    /// Monotonic ID within the `request_type` namespace. Reused IDs that
    /// fall below the framework's watermark (and have been evicted from
    /// the response cache) return [`IdempotencyDecision::Expired`]
    /// instead of being silently re-applied.
    pub request_id: u64,
    /// Optional client identifier for diagnostic logging only. Not part
    /// of the dedup identity. `None` is acceptable.
    pub client_id: Option<String>,
}

impl PartialEq for IdempotencyKey {
    fn eq(&self, other: &Self) -> bool {
        self.request_type == other.request_type && self.request_id == other.request_id
    }
}

impl Eq for IdempotencyKey {}

impl std::hash::Hash for IdempotencyKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.request_type.hash(state);
        self.request_id.hash(state);
    }
}

impl IdempotencyKey {
    /// Construct a key without a `client_id` (most common path).
    pub fn new(request_type: impl Into<String>, request_id: u64) -> Self {
        Self {
            request_type: request_type.into(),
            request_id,
            client_id: None,
        }
    }

    /// Construct a key with a `client_id` for diagnostics. The
    /// `client_id` does not affect dedup behaviour.
    pub fn with_client_id(
        request_type: impl Into<String>,
        request_id: u64,
        client_id: impl Into<String>,
    ) -> Self {
        Self {
            request_type: request_type.into(),
            request_id,
            client_id: Some(client_id.into()),
        }
    }
}

/// Outcome of consulting the idempotency layer for a key.
///
/// Returned by `IdempotencyLayer::decide` (in `rr-raft`); the
/// state-machine adapter dispatches on the variant before deciding
/// whether to call `Service::handle`.
#[derive(Debug, Clone)]
pub enum IdempotencyDecision {
    /// First time seeing this key — proceed with normal processing.
    /// Caller MUST follow up with `record(key, response)` once the
    /// service has produced its response.
    New,
    /// Already-applied request; the cached response bytes are returned
    /// verbatim without invoking the user service.
    Replay(Vec<u8>),
    /// Request ID is below the watermark AND has been evicted from the
    /// LRU response cache. Cannot be safely replayed (the client must
    /// pick a fresh ID). Surfaces as [`IdempotencyError::Expired`] on
    /// the wire.
    Expired {
        /// Request type whose watermark was tripped.
        request_type: String,
        /// Request ID the client supplied.
        request_id: u64,
        /// Current high-water mark for this request_type — IDs at or
        /// below this with no cache entry are expired.
        watermark: u64,
    },
    /// Two different `client_id`s reused the same
    /// `(request_type, request_id)`. The cached response is still
    /// returned (dedup wins) but the ambiguity is flagged for the
    /// audit log and metrics.
    Collision {
        /// `client_id` recorded with the original request.
        cached_client_id: Option<String>,
        /// `client_id` on the incoming retry.
        incoming_client_id: Option<String>,
        /// Cached response bytes returned to the caller.
        cached_response: Vec<u8>,
    },
}

/// Wire-level error surfaced when [`IdempotencyDecision::Expired`] is
/// returned. Maps onto the proto `IdempotencyError` field on
/// `ClientResponse`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyError {
    /// Request type whose watermark was tripped.
    pub request_type: String,
    /// Request ID the client submitted.
    pub request_id: u64,
    /// Current watermark for that request_type. Clients should pick a
    /// new ID > `watermark` to retry.
    pub watermark: u64,
}

impl std::fmt::Display for IdempotencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "idempotency expired: request_type={}, request_id={}, watermark={}",
            self.request_type, self.request_id, self.watermark
        )
    }
}

/// Wire-level outcome of applying a *keyed* request through the idempotency
/// layer.
///
/// The variant tag exists because openraft's `client_write` returns
/// `Result<Vec<u8>, RaftError>` and the framework needs a way to distinguish
/// "the keyed request returned expired" from "the keyed request returned
/// these bytes" without going through openraft's error channel (which would
/// lose the structured `IdempotencyError`).
///
/// Only entries that carry an `IdempotencyKey` are wrapped in this envelope;
/// unkeyed v0.2.x-style entries continue to return raw `Vec<u8>` from
/// `apply` to preserve wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyOutcome {
    /// User-service response (normal Replay / New / Collision paths all
    /// land here — Collision returns the cached response so the client
    /// can't tell it apart from a legitimate Replay).
    Success(Vec<u8>),
    /// The request_id was below the watermark and had been evicted from
    /// the LRU cache. Client must pick a fresh ID.
    IdempotencyExpired(IdempotencyError),
}

/// Tag byte prefixed to `ApplyOutcome` bytes. Identifies the bytes as
/// a framework-wrapped apply response so a mistakenly-decoded raw
/// user payload fails fast rather than silently producing a
/// `Success(garbage)` result. Distinct from
/// [`crate::framed_snapshot::FRAMED_SNAPSHOT_MAGIC`] /
/// [`crate::framed_entry::FRAMED_ENTRY_MAGIC`] (one byte vs eight)
/// because the response wire path is single-source — the dispatch
/// layer is the only encoder/decoder, so a short tag is enough to
/// catch type errors without the cost of a full magic header.
pub const APPLY_OUTCOME_TAG: u8 = 0xAA;

/// Schema version for the `ApplyOutcome` envelope. Bumped if a new
/// variant is added with non-backward-compatible semantics.
pub const APPLY_OUTCOME_VERSION: u8 = 1;

impl ApplyOutcome {
    /// Encode the outcome as `[tag][version][bincode body]` bytes for
    /// `apply`'s `Vec<u8>` return.
    pub fn encode(&self) -> Result<Vec<u8>, crate::error::RrError> {
        let body =
            bincode::serde::encode_to_vec(self, bincode::config::standard()).map_err(|e| {
                crate::error::RrError::Codec(crate::error::CodecError::SerializeFailed(
                    e.to_string(),
                ))
            })?;
        let mut out = Vec::with_capacity(2 + body.len());
        out.push(APPLY_OUTCOME_TAG);
        out.push(APPLY_OUTCOME_VERSION);
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Decode bytes returned from `apply`. Caller MUST only invoke this
    /// for entries that were submitted with an `IdempotencyKey`;
    /// unkeyed responses are raw user bytes and will fail the tag
    /// check at the head of the decode rather than silently
    /// mis-interpreting the bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::error::RrError> {
        if bytes.len() < 2 {
            metrics::counter!(
                "rr_framing_decode_failures_total",
                "kind" => "apply_outcome_too_short"
            )
            .increment(1);
            return Err(crate::error::RrError::Codec(
                crate::error::CodecError::DeserializeFailed(
                    "apply outcome bytes shorter than the 2-byte header".to_string(),
                ),
            ));
        }
        if bytes[0] != APPLY_OUTCOME_TAG {
            metrics::counter!(
                "rr_framing_decode_failures_total",
                "kind" => "apply_outcome_bad_tag"
            )
            .increment(1);
            return Err(crate::error::RrError::Codec(
                crate::error::CodecError::DeserializeFailed(format!(
                    "apply outcome tag mismatch: expected {APPLY_OUTCOME_TAG:#x}, got {:#x}",
                    bytes[0]
                )),
            ));
        }
        if bytes[1] > APPLY_OUTCOME_VERSION {
            metrics::counter!(
                "rr_framing_decode_failures_total",
                "kind" => "apply_outcome_future_version"
            )
            .increment(1);
            return Err(crate::error::RrError::Codec(
                crate::error::CodecError::DeserializeFailed(format!(
                    "apply outcome version {} is newer than this binary supports ({APPLY_OUTCOME_VERSION})",
                    bytes[1]
                )),
            ));
        }
        let (decoded, _) =
            bincode::serde::decode_from_slice(&bytes[2..], bincode::config::standard()).map_err(
                |e| {
                    metrics::counter!(
                        "rr_framing_decode_failures_total",
                        "kind" => "apply_outcome_body"
                    )
                    .increment(1);
                    crate::error::RrError::Codec(crate::error::CodecError::DeserializeFailed(
                        format!("apply outcome decode failed: {e}"),
                    ))
                },
            )?;
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_equality_ignores_client_id() {
        let a = IdempotencyKey::with_client_id("transfer", 42, "alice");
        let b = IdempotencyKey::with_client_id("transfer", 42, "bob");
        assert_eq!(
            a, b,
            "PartialEq must compare only (request_type, request_id); \
             client_id is diagnostic-only metadata"
        );
    }

    #[test]
    fn key_equality_distinguishes_request_id() {
        let a = IdempotencyKey::new("transfer", 42);
        let b = IdempotencyKey::new("transfer", 43);
        assert_ne!(a, b);
    }

    #[test]
    fn key_equality_distinguishes_request_type() {
        let a = IdempotencyKey::new("transfer", 42);
        let b = IdempotencyKey::new("withdraw", 42);
        assert_ne!(a, b);
    }

    #[test]
    fn key_hash_ignores_client_id() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let a = IdempotencyKey::with_client_id("transfer", 42, "alice");
        let b = IdempotencyKey::with_client_id("transfer", 42, "bob");
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(
            ha.finish(),
            hb.finish(),
            "Hash must mirror PartialEq; client_id must not change the hash"
        );
    }

    #[test]
    fn key_serde_roundtrip() {
        let original = IdempotencyKey::with_client_id("create_user", 7, "client-1");
        let bytes = bincode::serde::encode_to_vec(&original, bincode::config::standard()).unwrap();
        let (decoded, _): (IdempotencyKey, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn idempotency_error_displays_human_readable() {
        let err = IdempotencyError {
            request_type: "transfer".into(),
            request_id: 100,
            watermark: 200,
        };
        let s = err.to_string();
        assert!(s.contains("transfer"));
        assert!(s.contains("100"));
        assert!(s.contains("200"));
    }

    #[test]
    fn idempotency_error_serde_roundtrip() {
        let err = IdempotencyError {
            request_type: "x".into(),
            request_id: 1,
            watermark: 0,
        };
        let bytes = bincode::serde::encode_to_vec(&err, bincode::config::standard()).unwrap();
        let (decoded, _): (IdempotencyError, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded, err);
    }
}
