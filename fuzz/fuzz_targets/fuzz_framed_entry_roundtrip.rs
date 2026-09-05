#![no_main]

use libfuzzer_sys::fuzz_target;
use rr_core::framed_entry::FramedEntry;

fuzz_target!(|data: &[u8]| {
    // Only inputs the decoder *accepts as framed* have semantics that can
    // drift: `Ok(None)` means "legacy/unframed bytes" and `Err` means
    // rejected — neither carries a decoded value to round-trip.
    let Ok(Some(first)) = FramedEntry::decode(data) else {
        return;
    };

    let encoded = first
        .encode()
        .expect("re-encoding a value the decoder accepted must succeed");
    let second = FramedEntry::decode(&encoded)
        .expect("re-decoding our own encode output must succeed")
        .expect("our own encode output must carry the framed magic");

    assert_eq!(first.version, second.version, "version drifted");
    assert_eq!(first.offset, second.offset, "offset drifted");
    assert_eq!(first.payload, second.payload, "payload drifted");
    // IdempotencyKey's PartialEq deliberately ignores `client_id` (dedup
    // identity is only (request_type, request_id)), but the wire format
    // must round-trip `client_id` too, so compare the fields directly.
    match (&first.idem_key, &second.idem_key) {
        (None, None) => {}
        (Some(a), Some(b)) => {
            assert_eq!(a.request_type, b.request_type, "idem request_type drifted");
            assert_eq!(a.request_id, b.request_id, "idem request_id drifted");
            assert_eq!(a.client_id, b.client_id, "idem client_id drifted");
        }
        _ => panic!("idem_key presence drifted across encode→decode"),
    }

    // Byte-level idempotence: encoding the twice-decoded value must
    // reproduce the exact same bytes (bincode is deterministic for this
    // schema — no maps involved).
    let encoded_twice = second
        .encode()
        .expect("re-encoding a value the decoder accepted must succeed");
    assert_eq!(encoded, encoded_twice, "encode is not idempotent");
});
