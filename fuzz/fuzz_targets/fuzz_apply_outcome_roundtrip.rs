#![no_main]

use libfuzzer_sys::fuzz_target;
use rr_core::idempotency::ApplyOutcome;

fuzz_target!(|data: &[u8]| {
    // Only inputs the decoder accepts have semantics that can drift;
    // rejected inputs (bad tag / future version / malformed body) carry
    // nothing to round-trip.
    let Ok(first) = ApplyOutcome::decode(data) else {
        return;
    };

    let encoded = first
        .encode()
        .expect("re-encoding a value the decoder accepted must succeed");
    let second =
        ApplyOutcome::decode(&encoded).expect("re-decoding our own encode output must succeed");

    match (&first, &second) {
        (ApplyOutcome::Success(a), ApplyOutcome::Success(b)) => {
            assert_eq!(a, b, "Success payload drifted");
        }
        (ApplyOutcome::IdempotencyExpired(a), ApplyOutcome::IdempotencyExpired(b)) => {
            assert_eq!(a, b, "IdempotencyExpired payload drifted");
        }
        _ => panic!("ApplyOutcome variant drifted across encode→decode"),
    }

    // Byte-level idempotence: encoding the twice-decoded value must
    // reproduce the exact same bytes (bincode is deterministic for this
    // schema — no maps involved).
    let encoded_twice = second
        .encode()
        .expect("re-encoding a value the decoder accepted must succeed");
    assert_eq!(encoded, encoded_twice, "encode is not idempotent");
});
