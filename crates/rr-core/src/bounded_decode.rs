//! Bounded bincode decoding for bytes that originate outside the process
//! (raft log entries, snapshot files, apply responses).
//!
//! bincode 2.0.1 sizes container allocations from the *claimed* length
//! prefix before validating it against the remaining input
//! (`features/impl_alloc.rs`: `vec![0u8; len]` immediately follows
//! `claim_container_read`, which is a no-op unless the configuration
//! carries a `Limit`). A corrupt or hostile frame can therefore turn a
//! handful of input bytes into a multi-gibibyte `calloc` — and Rust
//! aborts the process on allocation failure. (Found by fuzz-nightly
//! 2026-10-10: a 17-byte `FramedEntry` input claiming a 2.5 GiB string.)
//!
//! The fix is bincode's intended guard: decode with a
//! `Configuration::Limit<N>`. Every container claim is then checked
//! against the running byte total BEFORE the allocation, so an absurd
//! length fails with `DecodeError::LimitExceeded` instead of allocating.
//! The limit does not change the wire format, so golden vectors and
//! mixed-version peers are unaffected.

use serde::de::DeserializeOwned;

/// Decode cap for a single raft-log-entry body ([`crate::framed_entry::FramedEntry`])
/// and for apply-response bodies ([`crate::idempotency::ApplyOutcome`]).
///
/// Legitimate bodies are bounded well below this: client payloads ingress
/// through gRPC (tonic's default 4 MiB message cap, tunable via
/// `grpc_max_frame_size`) and entries traverse AppendEntries replication
/// under the same cap. 16 MiB is 4× headroom over the default ingress
/// ceiling while keeping any corrupt length prefix far from
/// allocation-abort territory. Operators who raise `grpc_max_frame_size`
/// beyond ~16 MiB must also raise this constant.
pub(crate) const MAX_ENTRY_BODY: usize = 16 * 1024 * 1024;

/// Decode cap for [`crate::framed_snapshot::FramedSnapshot`] bodies.
///
/// Snapshot bodies embed the entire service state, so the ceiling must
/// stay far above realistic state sizes; the bound exists to reject
/// multi-gibibyte claimed lengths from a corrupt file header before
/// bincode allocates them. 1 GiB also stays under the fuzz harness's
/// 2 GiB RSS limit. Note this makes 1 GiB a hard ceiling on snapshot
/// body size until the format grows a length-validating prelude.
pub(crate) const MAX_SNAPSHOT_BODY: usize = 1024 * 1024 * 1024;

/// Decode `body` as `T`, rejecting any container length claim that would
/// push total claimed bytes past `MAX_ENTRY_BODY` — before the memory is
/// allocated. Wire-compatible with the unbounded
/// `bincode::serde::decode_from_slice` on `bincode::config::standard()`.
pub(crate) fn decode_entry_body<T: DeserializeOwned>(
    body: &[u8],
) -> Result<(T, usize), bincode::error::DecodeError> {
    bincode::serde::decode_from_slice(
        body,
        bincode::config::standard().with_limit::<MAX_ENTRY_BODY>(),
    )
}

/// Same guard as [`decode_entry_body`], with the snapshot ceiling.
pub(crate) fn decode_snapshot_body<T: DeserializeOwned>(
    body: &[u8],
) -> Result<(T, usize), bincode::error::DecodeError> {
    bincode::serde::decode_from_slice(
        body,
        bincode::config::standard().with_limit::<MAX_SNAPSHOT_BODY>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact input fuzz-nightly 2026-10-10 OOM'd on (base64
    /// `AHJyRU5UMDEoAAH8AAAyo2E=`): a `FramedEntry` body whose first
    /// `Option<String>`-adjacent length claims 0xA3320000 ≈ 2.55 GiB in
    /// 9 bytes of actual input.
    #[test]
    fn absurd_length_claim_is_rejected_without_allocating() {
        let body: [u8; 9] = [0x28, 0x00, 0x01, 0xfc, 0x00, 0x00, 0x32, 0xa3, 0x61];
        let result = decode_entry_body::<crate::framed_entry::FramedEntry>(&body);
        assert!(matches!(
            result,
            Err(bincode::error::DecodeError::LimitExceeded)
        ));
    }

    #[test]
    fn honest_small_body_still_decodes() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Small {
            name: String,
            data: Vec<u8>,
        }
        let value = Small {
            name: "ok".to_string(),
            data: vec![1, 2, 3],
        };
        let bytes = bincode::serde::encode_to_vec(&value, bincode::config::standard()).unwrap();
        let (decoded, _) = decode_entry_body::<Small>(&bytes).unwrap();
        assert_eq!(value, decoded);
    }
}
