//! Codec trait for serializing and deserializing framework data.

use serde::{de::DeserializeOwned, Serialize};

use crate::error::CodecError;

/// Trait for encoding and decoding values to/from bytes.
///
/// The default implementation uses bincode for binary serialization.
///
/// # Deprecation
///
/// Deprecated in the 1.0.0 breaking window: [`Codec::decode`] is UNBOUNDED —
/// it trusts the input's length prefixes. Never use it on untrusted bytes
/// (network input, on-disk frames from other parties): a corrupt length
/// claim can exhaust memory before bincode notices. The framework's own
/// wire paths decode through the bounded helpers in
/// [`crate::bounded_decode`] instead; downstream users handling untrusted
/// input should do the same.
#[deprecated(note = "unbounded bincode decode on untrusted bytes is an OOM risk; \
            use rr_core::bounded_decode or the frame codecs (FramedEntry/FramedSnapshot) instead")]
pub trait Codec: Send + Sync {
    /// Encode a value to bytes.
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError>;
    /// Decode bytes back to a value.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError>;
}

/// Default codec using bincode v2 for binary serialization.
#[deprecated(note = "unbounded bincode decode on untrusted bytes is an OOM risk; \
            use rr_core::bounded_decode or the frame codecs (FramedEntry/FramedSnapshot) instead")]
pub struct BincodeCodec;

// Zero workspace consumers of these items remain (verified by workspace-wide
// grep); the impl is kept for the deprecated public surface and exercised
// only by this module's tests until removal in a future MAJOR.
#[allow(deprecated)]
impl Codec for BincodeCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|e| CodecError::SerializeFailed(e.to_string()))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        let (value, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| CodecError::DeserializeFailed(e.to_string()))?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    // These tests intentionally pin the legacy unbounded codec's behavior
    // until the deprecated items are removed in a future MAJOR.
    // Production wire paths decode through bounded_decode / frame codecs.
    #![allow(deprecated)]

    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestData {
        key: String,
        value: u64,
    }

    #[test]
    fn test_bincode_codec_roundtrip() {
        let codec = BincodeCodec;
        let data = TestData {
            key: "foo".to_string(),
            value: 42,
        };
        let bytes = codec.encode(&data).expect("encode should succeed");
        let decoded: TestData = codec.decode(&bytes).expect("decode should succeed");
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_bincode_codec_invalid_bytes() {
        let codec = BincodeCodec;
        let result: Result<TestData, _> = codec.decode(b"invalid bytes!!!!");
        assert!(result.is_err());
    }

    use proptest::prelude::*;

    proptest! {
        // R-6 review item L7a: persist failing seeds into
        // `crates/rr-core/proptest-regressions/` (git-tracked) so a failure
        // reproduces across runs; `SourceParallel` resolves relative to
        // `src/lib.rs`. (`ProptestConfig::default()` already enables this;
        // the explicit setting pins the contract.)
        #![proptest_config(ProptestConfig {
            failure_persistence: Some(Box::new(
                proptest::test_runner::FileFailurePersistence::SourceParallel(
                    "proptest-regressions",
                ),
            )),
            ..ProptestConfig::default()
        })]
        #[test]
        fn proptest_bincode_roundtrip_vec_u8(data: Vec<u8>) {
            let codec = BincodeCodec;
            let encoded = codec.encode(&data).expect("encode should succeed");
            let decoded: Vec<u8> = codec.decode(&encoded).expect("decode should succeed");
            prop_assert_eq!(data, decoded);
        }

        #[test]
        fn proptest_bincode_roundtrip_string(data: String) {
            let codec = BincodeCodec;
            let encoded = codec.encode(&data).expect("encode should succeed");
            let decoded: String = codec.decode(&encoded).expect("decode should succeed");
            prop_assert_eq!(data, decoded);
        }

        #[test]
        fn proptest_bincode_roundtrip_struct(key: String, value: u64) {
            let codec = BincodeCodec;
            let data = TestData { key, value };
            let encoded = codec.encode(&data).expect("encode should succeed");
            let decoded: TestData = codec.decode(&encoded).expect("decode should succeed");
            prop_assert_eq!(data, decoded);
        }

        #[test]
        fn proptest_bincode_encode_deterministic(data: Vec<u8>) {
            let codec = BincodeCodec;
            let encoded1 = codec.encode(&data).expect("encode should succeed");
            let encoded2 = codec.encode(&data).expect("encode should succeed");
            prop_assert_eq!(encoded1, encoded2);
        }

        #[test]
        fn proptest_bincode_encode_decode_encode_stability(data: Vec<u8>) {
            let codec = BincodeCodec;
            let encoded1 = codec.encode(&data).expect("encode should succeed");
            let decoded: Vec<u8> = codec.decode(&encoded1).expect("decode should succeed");
            let encoded2 = codec.encode(&decoded).expect("re-encode should succeed");
            prop_assert_eq!(encoded1, encoded2);
        }
    }
}
