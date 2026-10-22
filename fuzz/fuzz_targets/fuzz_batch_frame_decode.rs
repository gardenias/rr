#![no_main]

use libfuzzer_sys::fuzz_target;
use rr_core::batch::{decode_batch, validate_batch_frame};

fuzz_target!(|data: &[u8]| {
    // No-panic contract over arbitrary bytes: `Ok(None)` (not a batch),
    // `Ok(Some)` (well-formed) and `Err` (malformed / future version) are
    // all acceptable outcomes — only a panic / abort / oom fails the run.
    //
    // Consistency contract (2026-10-21 review, L7b): both public entry
    // points share one parser (`parse_batch_frame`), so for ANY input their
    // accept/reject verdicts must agree — `decode_batch` materializes the
    // items while `validate_batch_frame` is the no-alloc zero-copy path used
    // by ingress validation; a verdict skew would let a frame past ingress
    // that the apply-side decoder rejects (or vice versa). Rejection
    // REASONS may differ (both are opaque `Err`), but the accept/reject
    // decision itself must not. Mirrored by the fixed-input unit test
    // `validate_batch_frame_mirrors_decode_contract` in
    // `crates/rr-core/src/batch.rs`.
    let decoded = decode_batch(data);
    let validated = validate_batch_frame(data);
    match (&decoded, &validated) {
        (Err(_), Err(_)) | (Ok(None), Ok(None)) | (Ok(Some(_)), Ok(Some(()))) => {}
        _ => panic!(
            "decode/validate verdict skew on {} bytes: decode_batch -> {}, \
             validate_batch_frame -> {}",
            data.len(),
            match &decoded {
                Ok(None) => "Ok(None)".to_string(),
                Ok(Some(items)) => format!("Ok(Some(..{} items))", items.len()),
                Err(e) => format!("Err({e})"),
            },
            match &validated {
                Ok(None) => "Ok(None)".to_string(),
                Ok(Some(())) => "Ok(Some(()))".to_string(),
                Err(e) => format!("Err({e})"),
            },
        ),
    }
});
