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
    out.extend_from_slice(&(items.len() as u32).to_be_bytes());
    for item in items {
        out.extend_from_slice(&(item.len() as u32).to_be_bytes());
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
    let count = u32::from_be_bytes(bytes[9..13].try_into().expect("4 bytes")) as usize;
    let mut items = Vec::with_capacity(count.min(1 << 16));
    let mut cursor = HEADER_LEN;
    for _ in 0..count {
        let end = cursor + 4;
        if end > bytes.len() {
            return Err(malformed("item length prefix truncated"));
        }
        let len = u32::from_be_bytes(bytes[cursor..end].try_into().expect("4 bytes")) as usize;
        cursor = end;
        let end = cursor + len;
        if end > bytes.len() {
            return Err(malformed("item payload truncated"));
        }
        items.push(bytes[cursor..end].to_vec());
        cursor = end;
    }
    if cursor != bytes.len() {
        return Err(malformed("trailing bytes after last item"));
    }
    Ok(Some(items))
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
}
