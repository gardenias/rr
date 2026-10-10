//! Framed snapshot envelope used by `ServiceStateMachineAdapter`.
//!
//! From v0.3.0 onward, [`crate::service::Service`]-based state machines no
//! longer write a user's raw [`crate::service::Service::snapshot`] bytes
//! directly to the Raft snapshot. Instead, the framework wraps user bytes in
//! a [`FramedSnapshot`] envelope that also carries framework-owned state
//! (idempotency layer's watermarks/cache, command-ingestor offsets, future
//! framework metadata).
//!
//! # Wire format
//!
//! `[magic (8 bytes)] [bincode-encoded FramedSnapshot]`
//!
//! The magic prefix [`FRAMED_SNAPSHOT_MAGIC`] discriminates new framed
//! snapshots from legacy raw-bytes snapshots produced by v0.2.x and earlier.
//! On `restore()` the framework attempts framed decode first; on
//! magic-mismatch it falls back to treating the entire input as the user's
//! raw snapshot bytes (transparent migration). After the next `snapshot()`
//! call the on-disk format is the new framed envelope.
//!
//! # Forward compatibility
//!
//! `FramedSnapshot::version` lets future framework changes evolve the
//! envelope without breaking existing on-disk snapshots. New fields added
//! to [`FrameworkState`] should be `Option<…>` so older versions can decode
//! newer snapshots gracefully (bincode v2 ignores unknown trailing bytes
//! unless the schema explicitly demands them, so additive changes are
//! safe).

use serde::{Deserialize, Serialize};

use crate::error::{CodecError, RrError};

/// Magic byte prefix identifying a framed snapshot. Chosen so it is highly
/// unlikely to collide with a user's raw `Service::snapshot()` bytes (no
/// printable header, embedded NULs, version digit).
///
/// Wire implementation detail — the on-the-wire format commitment is
/// documented in `docs/reference/compatibility.md`, not here.
#[doc(hidden)]
pub const FRAMED_SNAPSHOT_MAGIC: [u8; 8] = *b"\0rrSNAP1";

/// Current envelope schema version. Bumped when [`FrameworkState`] adds a
/// non-optional field or changes the meaning of an existing one.
///
/// Wire implementation detail — see [`FRAMED_SNAPSHOT_MAGIC`].
#[doc(hidden)]
pub const FRAMED_SNAPSHOT_VERSION: u8 = 1;

/// Wraps a user's raw `Service::snapshot()` bytes plus framework-owned
/// state into a single bincode-encoded envelope written to the Raft
/// snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FramedSnapshot {
    /// Schema version; readers should reject `version > FRAMED_SNAPSHOT_VERSION`
    /// rather than guess at an unknown layout.
    pub version: u8,
    /// Framework-owned state: idempotency watermarks/cache, ingestor
    /// offsets, etc. All sub-fields are optional so a node that does
    /// not use a feature emits `None` and a future feature can be added
    /// without bumping `version`.
    pub framework_state: FrameworkState,
    /// Raw user-emitted bytes from `Service::snapshot()`. The framework
    /// does not interpret these.
    pub user_snapshot: Vec<u8>,
}

/// Framework-owned state carried alongside the user snapshot. Each
/// optional field is populated only when the corresponding feature is
/// active on this group. Defaults (`None` for every field) make this
/// safe to construct with `..FrameworkState::default()` when adding
/// new fields in future minor versions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FrameworkState {
    /// Idempotency layer state (watermarks + cache). `Some` iff the
    /// `(request_type, request_id)` deduplication path has been
    /// exercised on this group. Populated by Wave 4 Tier 2.
    pub idempotency: Option<IdempotencyState>,
    /// Command-ingestor state (last-applied offset). `Some` iff a
    /// `CommandIngestor` is bound to this group. Populated by Wave 4
    /// Tier 3.
    pub ingestor: Option<IngestorState>,
    /// Outbox state: pending outputs and sent watermark.
    /// None if no outputs have been produced on this group.
    pub outbox: Option<OutboxState>,
}

/// Idempotency layer's serialized state, populated by
/// `rr_raft::idempotency::IdempotencyLayer::capture`. `data` is an
/// opaque bincode blob owned by the layer (not the framing layer);
/// readers must round-trip through the layer's `restore` to get
/// usable state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdempotencyState {
    /// Bincode-encoded `(watermarks: HashMap<request_type, u64>,
    /// cache: Vec<…>)`. Empty `Vec` means "no captured state" and
    /// `IdempotencyLayer::restore` treats it as a no-op (used by
    /// `Default`).
    #[serde(default, alias = "reserved")]
    pub data: Vec<u8>,
}

/// Placeholder for the command-ingestor's serialized state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestorState {
    /// Bincode-encoded form of the implementer's typed
    /// `CommandIngestor::Offset`. Opaque to the framework: only the
    /// ingestor adapter knows how to interpret these bytes. `None`
    /// before any commands have been consumed.
    pub last_applied_offset: Option<Vec<u8>>,
}

/// Outbox state captured in snapshots so output delivery can resume after
/// leader changes or process restarts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OutboxState {
    /// Outputs awaiting delivery (seq > sent_watermark).
    pub pending: Vec<crate::output::Output>,
    /// Highest seq that has been successfully sent. 0 = none sent.
    pub sent_watermark: u64,
    /// Next seq to assign (monotonically increasing).
    pub next_seq: u64,
}

impl FramedSnapshot {
    /// Create a fresh envelope at the current schema version with empty
    /// framework state.
    pub fn new(user_snapshot: Vec<u8>) -> Self {
        Self {
            version: FRAMED_SNAPSHOT_VERSION,
            framework_state: FrameworkState::default(),
            user_snapshot,
        }
    }

    /// Encode the envelope as `[magic][bincode body]`.
    pub fn encode(&self) -> Result<Vec<u8>, RrError> {
        let body = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))?;
        let mut out = Vec::with_capacity(FRAMED_SNAPSHOT_MAGIC.len() + body.len());
        out.extend_from_slice(&FRAMED_SNAPSHOT_MAGIC);
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Attempt to decode an envelope.
    ///
    /// Returns:
    /// - `Ok(Some(framed))` — bytes start with [`FRAMED_SNAPSHOT_MAGIC`]
    ///   and decoded successfully.
    /// - `Ok(None)` — bytes do not start with the magic prefix; the
    ///   caller should treat the input as legacy raw user-snapshot
    ///   bytes (auto-migration path).
    /// - `Err(_)` — bytes start with the magic prefix but the body is
    ///   malformed or has an unsupported version. Failing here is
    ///   correct: returning `Ok(None)` would silently corrupt state by
    ///   handing partially-magic'd bytes back to the user as a raw
    ///   snapshot.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, RrError> {
        if bytes.len() < FRAMED_SNAPSHOT_MAGIC.len() {
            return Ok(None);
        }
        if bytes[..FRAMED_SNAPSHOT_MAGIC.len()] != FRAMED_SNAPSHOT_MAGIC {
            return Ok(None);
        }
        let body = &bytes[FRAMED_SNAPSHOT_MAGIC.len()..];
        let (framed, _) = crate::bounded_decode::decode_snapshot_body::<FramedSnapshot>(body)
            .map_err(|e| {
                metrics::counter!(
                    "rr_framing_decode_failures_total",
                    "kind" => "framed_snapshot_body"
                )
                .increment(1);
                RrError::Codec(CodecError::DeserializeFailed(format!(
                    "framed snapshot decode failed: {e}"
                )))
            })?;
        if framed.version > FRAMED_SNAPSHOT_VERSION {
            metrics::counter!(
                "rr_framing_decode_failures_total",
                "kind" => "framed_snapshot_future_version"
            )
            .increment(1);
            return Err(RrError::Codec(CodecError::DeserializeFailed(format!(
                "framed snapshot version {} is newer than this binary supports ({})",
                framed.version, FRAMED_SNAPSHOT_VERSION
            ))));
        }
        Ok(Some(framed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostile_length_claim_is_rejected_without_oom() {
        // Same failure family as the fuzz-nightly 2026-10-10 FramedEntry
        // OOM: three None options, then a user_snapshot Vec claiming
        // u64::MAX bytes. Must fail fast, not allocate.
        let mut bytes = Vec::from(FRAMED_SNAPSHOT_MAGIC);
        bytes.extend_from_slice(&[
            0x01, // version
            0x00, 0x00, 0x00, // idempotency/ingestor/outbox = None
            0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // varint u64::MAX length
        ]);
        assert!(FramedSnapshot::decode(&bytes).is_err());
    }

    #[test]
    fn empty_user_snapshot_roundtrip() {
        let original = FramedSnapshot::new(Vec::new());
        let encoded = original.encode().unwrap();
        let decoded = FramedSnapshot::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded.version, FRAMED_SNAPSHOT_VERSION);
        assert!(decoded.user_snapshot.is_empty());
        assert!(decoded.framework_state.idempotency.is_none());
        assert!(decoded.framework_state.ingestor.is_none());
        assert!(decoded.framework_state.outbox.is_none());
    }

    #[test]
    fn populated_user_snapshot_roundtrip() {
        let payload = b"some user state".to_vec();
        let original = FramedSnapshot::new(payload.clone());
        let encoded = original.encode().unwrap();
        let decoded = FramedSnapshot::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded.user_snapshot, payload);
    }

    #[test]
    fn framework_state_with_ingestor_roundtrip() {
        let mut original = FramedSnapshot::new(b"user".to_vec());
        original.framework_state.ingestor = Some(IngestorState {
            last_applied_offset: Some(vec![0, 1, 2, 3, 4, 5, 6, 7]),
        });
        let encoded = original.encode().unwrap();
        let decoded = FramedSnapshot::decode(&encoded).unwrap().unwrap();
        let ingestor = decoded.framework_state.ingestor.unwrap();
        assert_eq!(
            ingestor.last_applied_offset,
            Some(vec![0, 1, 2, 3, 4, 5, 6, 7])
        );
    }

    #[test]
    fn framework_state_with_outbox_roundtrip() {
        let mut original = FramedSnapshot::new(b"user".to_vec());
        original.framework_state.outbox = Some(OutboxState {
            pending: vec![crate::output::Output::new(
                "req-1:0",
                crate::output::OutputTarget::RaftWrite {
                    addrs: vec!["127.0.0.1:50051".to_string()],
                    group_id: 7,
                },
                b"payload".to_vec(),
            )],
            sent_watermark: 0,
            next_seq: 1,
        });
        let encoded = original.encode().unwrap();
        let decoded = FramedSnapshot::decode(&encoded).unwrap().unwrap();
        let outbox = decoded.framework_state.outbox.expect("outbox should exist");
        assert_eq!(outbox.pending.len(), 1);
        assert_eq!(outbox.sent_watermark, 0);
        assert_eq!(outbox.next_seq, 1);
    }

    #[test]
    fn legacy_unframed_bytes_decode_to_none() {
        let legacy_bytes = b"raw user snapshot bytes from v0.2.x".to_vec();
        let decoded = FramedSnapshot::decode(&legacy_bytes).unwrap();
        assert!(
            decoded.is_none(),
            "non-magic bytes must signal legacy/unframed"
        );
    }

    #[test]
    fn empty_bytes_decode_to_none() {
        let decoded = FramedSnapshot::decode(&[]).unwrap();
        assert!(decoded.is_none());
    }

    #[test]
    fn short_bytes_below_magic_length_decode_to_none() {
        let decoded = FramedSnapshot::decode(b"abc").unwrap();
        assert!(decoded.is_none());
    }

    #[test]
    fn malformed_framed_body_returns_err() {
        let mut bytes = Vec::from(FRAMED_SNAPSHOT_MAGIC);
        bytes.extend_from_slice(b"not a valid bincode payload at all");
        let result = FramedSnapshot::decode(&bytes);
        assert!(
            result.is_err(),
            "magic + invalid body must error rather than fall back to legacy"
        );
    }

    #[test]
    fn future_version_is_rejected() {
        let future = FramedSnapshot {
            version: 99,
            framework_state: FrameworkState::default(),
            user_snapshot: vec![],
        };
        let mut bytes = Vec::from(FRAMED_SNAPSHOT_MAGIC);
        bytes.extend_from_slice(
            &bincode::serde::encode_to_vec(&future, bincode::config::standard()).unwrap(),
        );
        let err = FramedSnapshot::decode(&bytes).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("version") && msg.contains("99"),
            "expected version-mismatch error, got: {msg}"
        );
    }

    #[test]
    fn user_snapshot_starting_with_partial_magic_is_safe() {
        let user_bytes_with_unlucky_prefix = b"\0rr".to_vec();
        let decoded = FramedSnapshot::decode(&user_bytes_with_unlucky_prefix).unwrap();
        assert!(
            decoded.is_none(),
            "partial-magic prefix on user bytes must NOT be treated as framed"
        );
    }
}
