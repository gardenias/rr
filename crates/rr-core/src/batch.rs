//! Client-write batch frame — packs several unkeyed application payloads
//! into a single Raft log entry (rr-client opt-in batching, off by default).
//!
//! The frame travels as an ordinary `ClientRequest.payload` and is split on
//! the apply side (`ServiceStateMachineAdapter`): each item is handed to the
//! service individually (immediate, non-deferred handling) and the per-item
//! responses are packed into a response frame with the same layout. The whole
//! batch commits as ONE Raft entry — one replication frame, one fsync unit —
//! while every item keeps its own apply ordering (frame order) and its own
//! response slot (positional).
//!
//! Constraints (enforced by design, documented in `docs/reference/`):
//! - **Unkeyed only.** Idempotency-keyed writes bypass the batcher; a batch
//!   frame never carries `FramedEntry` envelopes.
//! - **Immediate apply only.** Items are applied via the service's immediate
//!   path (`handle_with_outputs_ctx`); services using the deferred
//!   (two-phase) apply of A-0033 must not enable client batching.
//! - **All-or-nothing commit.** If any item's `handle` fails, the entry's
//!   apply fails as a whole — identical to a single failing write today.
//! - **No intra-batch atomicity on failure.** Items ahead of the failing
//!   one have already produced their user effects and outbox outputs, and
//!   since apply errors are group-fatal (ADR A-0018) a restart replays the
//!   whole entry — those items execute AGAIN (unkeyed writes have no
//!   idempotency layer). Services must tolerate that re-execution (make
//!   item effects idempotent at the application level).
//!
//! # Wire format (big-endian)
//!
//! ```text
//! magic:   8 bytes  — BATCH_FRAME_MAGIC
//! version: u8       — BATCH_FRAME_VERSION; readers reject version > current
//! count:   u32      — number of items
//! items:   count × { len: u32, bytes: [u8; len] }
//! ```

use crate::error::{CodecError, RrError};

/// Magic byte prefix identifying a batch frame. Distinct from
/// [`crate::framed_entry::FRAMED_ENTRY_MAGIC`] and
/// [`crate::framed_snapshot::FRAMED_SNAPSHOT_MAGIC`] so the envelopes can
/// never be mistaken for one another.
pub const BATCH_FRAME_MAGIC: [u8; 8] = *b"\0rrBAT01";

/// Current batch-frame schema version. Rejecting `version > current` on
/// decode (instead of guessing at an unknown layout) is the house
/// convention — same as `FramedEntry` / `FramedSnapshot` / `ApplyOutcome`.
pub const BATCH_FRAME_VERSION: u8 = 1;

const HEADER_LEN: usize = BATCH_FRAME_MAGIC.len() + 1 + 4;

/// Returns `true` iff `bytes` carries the batch-frame magic prefix.
/// A false result means "not a batch" — callers fall back to the raw
/// payload path; the magic is long enough that collision with a real
/// application payload is negligible (same convention as `FramedEntry`).
pub fn is_batch_frame(bytes: &[u8]) -> bool {
    bytes.starts_with(&BATCH_FRAME_MAGIC)
}

/// Pack `items` into one batch frame. Panics on > u32::MAX items or an
/// item larger than u32::MAX — both unreachable in practice (batching is
/// bounded by `BatchConfig` far below those limits).
pub fn encode_batch(items: &[Vec<u8>]) -> Vec<u8> {
    let total = items.iter().map(Vec::len).sum::<usize>();
    let mut out = Vec::with_capacity(HEADER_LEN + 4 * items.len() + total);
    out.extend_from_slice(&BATCH_FRAME_MAGIC);
    out.push(BATCH_FRAME_VERSION);
    let count = u32::try_from(items.len()).expect("batch item count exceeds u32::MAX");
    out.extend_from_slice(&count.to_be_bytes());
    for item in items {
        let len = u32::try_from(item.len()).expect("batch item exceeds u32::MAX bytes");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(item);
    }
    out
}

/// Decode a batch frame. Returns `Ok(None)` when `bytes` is not a batch
/// (no magic) so the caller continues down the raw-payload path; returns
/// `Err` only when the magic is present but the frame is malformed
/// (truncated header, truncated item, trailing garbage) or carries a
/// newer schema version than this build understands.
pub fn decode_batch(bytes: &[u8]) -> Result<Option<Vec<Vec<u8>>>, RrError> {
    let Some(spans) = parse_batch_frame(bytes)? else {
        return Ok(None);
    };
    Ok(Some(
        spans
            .into_iter()
            .map(|(s, e)| bytes[s..e].to_vec())
            .collect(),
    ))
}

/// Structural validation of a batch frame without materializing the items
/// (no per-item allocation) — the ingress-validation hot path
/// (`crate::ingress::validate_client_payload`). Return contract identical
/// to [`decode_batch`]: `Ok(None)` = not a batch, `Ok(Some(()))` =
/// well-formed, `Err` = malformed or future version.
pub fn validate_batch_frame(bytes: &[u8]) -> Result<Option<()>, RrError> {
    Ok(parse_batch_frame(bytes)?.map(|_| ()))
}

/// Parse the frame, returning the `(start, end)` spans of each item payload.
/// All cursor arithmetic is overflow-checked so malformed length fields can
/// never wrap (including 32-bit `usize` targets); slicing happens only after
/// bounds are proven.
fn parse_batch_frame(bytes: &[u8]) -> Result<Option<Vec<(usize, usize)>>, RrError> {
    if !is_batch_frame(bytes) {
        return Ok(None);
    }
    let malformed = |msg: &str| {
        RrError::Codec(CodecError::DeserializeFailed(format!(
            "malformed batch frame: {msg}"
        )))
    };
    if bytes.len() < HEADER_LEN {
        return Err(malformed("header truncated"));
    }
    let version = bytes[8];
    if version > BATCH_FRAME_VERSION {
        return Err(malformed(&format!(
            "batch frame version {version} exceeds supported {BATCH_FRAME_VERSION} — upgrade the framework"
        )));
    }
    let count_bytes: [u8; 4] = bytes
        .get(9..13)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| malformed("header truncated"))?;
    let count = u32::from_be_bytes(count_bytes) as usize;
    let mut spans = Vec::with_capacity(count.min(1 << 16));
    let mut cursor = HEADER_LEN;
    for _ in 0..count {
        let end = cursor
            .checked_add(4)
            .ok_or_else(|| malformed("item length prefix offset overflow"))?;
        let len_bytes: [u8; 4] = bytes
            .get(cursor..end)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| malformed("item length prefix truncated"))?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        cursor = end;
        let end = cursor
            .checked_add(len)
            .ok_or_else(|| malformed("item payload offset overflow"))?;
        if end > bytes.len() {
            return Err(malformed("item payload truncated"));
        }
        spans.push((cursor, end));
        cursor = end;
    }
    if cursor != bytes.len() {
        return Err(malformed("trailing bytes after last item"));
    }
    Ok(Some(spans))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_multiple_items() {
        let items = vec![b"alpha".to_vec(), vec![], b"gamma".to_vec()];
        let frame = encode_batch(&items);
        assert!(is_batch_frame(&frame));
        let decoded = decode_batch(&frame).unwrap().expect("is a batch");
        assert_eq!(decoded, items);
    }

    #[test]
    fn roundtrip_empty_batch() {
        let frame = encode_batch(&[]);
        let decoded = decode_batch(&frame).unwrap().expect("is a batch");
        assert!(decoded.is_empty());
    }

    #[test]
    fn non_batch_returns_none() {
        assert!(decode_batch(b"plain application payload")
            .unwrap()
            .is_none());
        assert!(decode_batch(&[]).unwrap().is_none());
        // Same length as the magic but different content: not a batch.
        assert!(decode_batch(b"\0rrENT0").unwrap().is_none());
    }

    #[test]
    fn malformed_frames_error_loudly() {
        let frame = encode_batch(&[b"hello".to_vec()]);
        // Truncated item payload.
        assert!(decode_batch(&frame[..frame.len() - 2]).is_err());
        // Truncated length prefix.
        assert!(decode_batch(&frame[..HEADER_LEN + 2]).is_err());
        // Trailing garbage.
        let mut with_tail = frame.clone();
        with_tail.push(0xAB);
        assert!(decode_batch(&with_tail).is_err());
        // Magic only, no version/count.
        assert!(decode_batch(&BATCH_FRAME_MAGIC).is_err());
    }

    #[test]
    fn future_version_is_rejected_loudly() {
        let mut frame = encode_batch(&[b"hello".to_vec()]);
        frame[8] = BATCH_FRAME_VERSION + 1;
        let err = decode_batch(&frame).expect_err("future version must be rejected");
        assert!(
            err.to_string().contains("version"),
            "error should name the version problem: {err}"
        );
    }

    #[test]
    fn extreme_length_fields_error_instead_of_panicking() {
        // count = 1, len = u32::MAX on a tiny buffer: must Err, never panic
        // (cursor arithmetic is overflow-checked, also on 32-bit targets).
        let mut frame = Vec::from(BATCH_FRAME_MAGIC);
        frame.push(BATCH_FRAME_VERSION);
        frame.extend_from_slice(&1u32.to_be_bytes());
        frame.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_batch(&frame).is_err());
        assert!(validate_batch_frame(&frame).is_err());

        // count = u32::MAX with no items at all: pre-allocation is capped,
        // the first length prefix is already truncated.
        let mut frame = Vec::from(BATCH_FRAME_MAGIC);
        frame.push(BATCH_FRAME_VERSION);
        frame.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_batch(&frame).is_err());
    }

    #[test]
    fn validate_batch_frame_mirrors_decode_contract() {
        let good = encode_batch(&[vec![], b"abc".to_vec()]);
        assert!(validate_batch_frame(&good).unwrap().is_some());
        assert!(validate_batch_frame(b"plain").unwrap().is_none());
        let mut bad = good.clone();
        bad.pop();
        assert!(validate_batch_frame(&bad).is_err());
    }
}
