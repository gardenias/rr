#![no_main]

use libfuzzer_sys::fuzz_target;
use rr_core::batch::{decode_batch, encode_batch, validate_batch_frame};

fuzz_target!(|data: &[u8]| {
    // Only inputs the decoder *accepts as a batch frame* have semantics
    // that can drift: `Ok(None)` means "not a batch" and `Err` means
    // rejected — neither carries a decoded value to round-trip.
    let Ok(Some(first)) = decode_batch(data) else {
        return;
    };

    let encoded = encode_batch(&first);
    let second = decode_batch(&encoded)
        .expect("re-decoding our own encode output must succeed")
        .expect("our own encode output must carry the batch magic");

    assert_eq!(first, second, "batch items drifted across encode→decode");

    // The zero-copy validator must agree with the decoder on our own
    // output (both share `parse_batch_frame`; a future divergence of the
    // two entry points would turn this into a real catch).
    assert!(
        validate_batch_frame(&encoded)
            .expect("validator must not reject our own encode output")
            .is_some(),
        "validator lost the batch magic on our own encode output"
    );

    // Byte-level idempotence: encoding the twice-decoded value must
    // reproduce the exact same bytes (the frame layout is deterministic —
    // fixed header, explicit per-item lengths).
    let encoded_twice = encode_batch(&second);
    assert_eq!(encoded, encoded_twice, "encode is not idempotent");
});
