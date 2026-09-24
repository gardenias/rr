//! Ingress validation for raw client payloads (log-poison guard).
//!
//! The apply path unconditionally interprets a payload carrying a house
//! magic prefix — [`FramedEntry`] or a batch frame — as that envelope, and a
//! malformed or future-version frame fails the apply. Apply failures are
//! fatal to the Raft group (the state-machine worker dies and RaftCore goes
//! `Fatal`), so a client that — deliberately or by bug — writes
//! `\0rrBAT01`-prefixed garbage as an *unkeyed* payload would poison the
//! committed log: every node dies re-applying that entry, including after
//! restart. [`validate_client_payload`] is the deterministic ingress check:
//! run it before proposing client bytes so a bad frame is rejected to the
//! caller instead of committed to the log.
//!
//! Cost: payloads without a house magic (the overwhelming majority) pay two
//! prefix comparisons only; batch frames pay a structural walk with no
//! per-item allocation.

use crate::batch::validate_batch_frame;
use crate::error::RrError;
use crate::framed_entry::FramedEntry;

/// Reject client payloads that collide with a framework envelope magic but
/// are malformed or carry a future schema version.
///
/// Returns `Ok(())` for magic-less payloads and for well-formed frames
/// (both are interpreted by the apply path as documented); returns
/// `Err(RrError::Codec(..))` exactly when the apply path would fail on
/// these bytes.
pub fn validate_client_payload(payload: &[u8]) -> Result<(), RrError> {
    FramedEntry::decode(payload)?;
    validate_batch_frame(payload)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{encode_batch, BATCH_FRAME_MAGIC, BATCH_FRAME_VERSION};
    use crate::framed_entry::FRAMED_ENTRY_MAGIC;

    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[test]
    fn magicless_payloads_pass_untouched() {
        assert!(validate_client_payload(b"plain application payload").is_ok());
        assert!(validate_client_payload(&[]).is_ok());
        assert!(validate_client_payload(b"\0rrENT0").is_ok()); // near-miss, not magic
    }

    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[test]
    fn well_formed_batch_frame_passes() {
        let frame = encode_batch(&[b"a".to_vec(), b"b".to_vec()]);
        assert!(validate_client_payload(&frame).is_ok());
    }

    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[test]
    fn malformed_batch_frame_is_rejected() {
        let mut poison = Vec::from(BATCH_FRAME_MAGIC);
        poison.extend_from_slice(b"garbage");
        assert!(validate_client_payload(&poison).is_err());
    }

    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[test]
    fn future_batch_version_is_rejected() {
        let mut frame = encode_batch(&[b"a".to_vec()]);
        frame[8] = BATCH_FRAME_VERSION + 1;
        assert!(validate_client_payload(&frame).is_err());
    }

    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[test]
    fn malformed_framed_entry_is_rejected() {
        let mut poison = Vec::from(FRAMED_ENTRY_MAGIC);
        poison.extend_from_slice(b"not bincode");
        assert!(validate_client_payload(&poison).is_err());
    }
}
