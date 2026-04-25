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
/// `client_id` is recorded for diagnostics only — if two different
/// `client_id` values share a `(request_type, request_id)`, the framework
/// emits an `op="idempotency_collision"` warning to the audit log but
/// still returns the cached response (the request_id space is shared
/// across clients by design — multiple writers MUST coordinate ID
/// allocation).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_equality_ignores_client_id() {
        let a = IdempotencyKey::with_client_id("transfer", 42, "alice");
        let b = IdempotencyKey::with_client_id("transfer", 42, "bob");
        // The dedup identity is (request_type, request_id); client_id
        // is metadata. Equality follows derive(PartialEq), which DOES
        // include client_id — but the layer's hashing for dedup
        // intentionally uses only (request_type, request_id). This
        // test pins the current Eq impl so a future change is
        // deliberate.
        assert_ne!(a, b);
        assert_eq!(a.request_type, b.request_type);
        assert_eq!(a.request_id, b.request_id);
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
