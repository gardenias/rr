#![no_main]

use libfuzzer_sys::fuzz_target;
use rr_core::framed_entry::FramedEntry;

fuzz_target!(|data: &[u8]| {
    let _ = FramedEntry::decode(data);
});
