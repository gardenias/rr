#![no_main]

use libfuzzer_sys::fuzz_target;
use rr_core::idempotency::ApplyOutcome;

fuzz_target!(|data: &[u8]| {
    let _ = ApplyOutcome::decode(data);
});
