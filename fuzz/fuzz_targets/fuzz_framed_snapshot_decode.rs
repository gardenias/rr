#![no_main]

use libfuzzer_sys::fuzz_target;
use rr_core::framed_snapshot::FramedSnapshot;

fuzz_target!(|data: &[u8]| {
    let _ = FramedSnapshot::decode(data);
});
