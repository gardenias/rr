#![no_main]

use libfuzzer_sys::fuzz_target;
use rr_core::batch::{decode_batch, validate_batch_frame};

fuzz_target!(|data: &[u8]| {
    // No-panic contract over arbitrary bytes: `Ok(None)` (not a batch),
    // `Ok(Some)` (well-formed) and `Err` (malformed / future version) are
    // all acceptable outcomes — only a panic / abort / oom fails the run.
    // Both public entry points share one parser (`parse_batch_frame`);
    // exercising both keeps the materializing and the zero-copy validation
    // path under the same input.
    let _ = decode_batch(data);
    let _ = validate_batch_frame(data);
});
