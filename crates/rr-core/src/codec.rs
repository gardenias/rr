//! Codec trait for serializing and deserializing framework data.

use serde::{de::DeserializeOwned, Serialize};

use crate::error::CodecError;

/// Trait for encoding and decoding values to/from bytes.
///
/// The default implementation uses bincode for binary serialization.
pub trait Codec: Send + Sync {
    /// Encode a value to bytes.
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError>;
    /// Decode bytes back to a value.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError>;
}

/// Default codec using bincode v2 for binary serialization.
pub struct BincodeCodec;

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
}
