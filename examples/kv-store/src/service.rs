//! KV Store Service — implements the high-level [`Service`] trait.
//!
//! This is the user-facing API layer. It uses bincode to serialize/deserialize
//! [`KvRequest`] and [`KvResponse`], and stores state in a `HashMap` behind
//! an `Arc<RwLock<...>>` so that the adapter can share the store across clones.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bincode::config::standard;
use tokio::sync::RwLock;
use tracing::{debug, instrument};

use rr_core::error::{CodecError, RrError};
use rr_core::service::Service;

use crate::types::{KvRequest, KvResponse};

/// In-memory KV store that implements [`Service`].
///
/// State is shared via `Arc<RwLock<...>>` so that snapshots can read while
/// writes are serialized through Raft.
#[derive(Clone)]
pub struct KvService {
    store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl KvService {
    /// Create a new, empty `KvService`.
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for KvService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for KvService {
    #[instrument(skip(self, request))]
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        let (req, _): (KvRequest, _) =
            bincode::serde::decode_from_slice(&request, standard()).map_err(|e| {
                RrError::Codec(CodecError::DeserializeFailed(e.to_string()))
            })?;

        let response = match req {
            KvRequest::Put { key, value } => {
                debug!(%key, "KV Put");
                self.store.write().await.insert(key, value);
                KvResponse::none()
            }
            KvRequest::Get { key } => {
                debug!(%key, "KV Get");
                let guard = self.store.read().await;
                KvResponse { value: guard.get(&key).cloned() }
            }
            KvRequest::Delete { key } => {
                debug!(%key, "KV Delete");
                self.store.write().await.remove(&key);
                KvResponse::none()
            }
        };

        let encoded = bincode::serde::encode_to_vec(&response, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))?;
        Ok(encoded)
    }

    fn name(&self) -> &str {
        "kv-store"
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let guard = self.store.read().await;
        bincode::serde::encode_to_vec(&*guard, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        let (restored, _): (HashMap<String, Vec<u8>>, _) =
            bincode::serde::decode_from_slice(data, standard()).map_err(|e| {
                RrError::Codec(CodecError::DeserializeFailed(e.to_string()))
            })?;
        *self.store.write().await = restored;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::config::standard;

    fn encode_req(req: KvRequest) -> Vec<u8> {
        bincode::serde::encode_to_vec(&req, standard()).unwrap()
    }

    fn decode_resp(bytes: Vec<u8>) -> KvResponse {
        let (r, _): (KvResponse, _) =
            bincode::serde::decode_from_slice(&bytes, standard()).unwrap();
        r
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let svc = KvService::new();
        let put = encode_req(KvRequest::Put {
            key: "hello".into(),
            value: b"world".to_vec(),
        });
        svc.handle(put).await.unwrap();

        let get = encode_req(KvRequest::Get { key: "hello".into() });
        let resp = decode_resp(svc.handle(get).await.unwrap());
        assert_eq!(resp.value, Some(b"world".to_vec()));
    }

    #[tokio::test]
    async fn test_get_missing_key_returns_none() {
        let svc = KvService::new();
        let get = encode_req(KvRequest::Get { key: "missing".into() });
        let resp = decode_resp(svc.handle(get).await.unwrap());
        assert_eq!(resp.value, None);
    }

    #[tokio::test]
    async fn test_delete_removes_key() {
        let svc = KvService::new();
        let put = encode_req(KvRequest::Put {
            key: "x".into(),
            value: vec![1, 2, 3],
        });
        svc.handle(put).await.unwrap();

        let del = encode_req(KvRequest::Delete { key: "x".into() });
        svc.handle(del).await.unwrap();

        let get = encode_req(KvRequest::Get { key: "x".into() });
        let resp = decode_resp(svc.handle(get).await.unwrap());
        assert_eq!(resp.value, None);
    }

    #[tokio::test]
    async fn test_snapshot_and_restore() {
        let svc = KvService::new();
        let put = encode_req(KvRequest::Put {
            key: "snap_key".into(),
            value: b"snap_val".to_vec(),
        });
        svc.handle(put).await.unwrap();

        let snapshot = svc.snapshot().await.unwrap();

        let mut svc2 = KvService::new();
        svc2.restore(&snapshot).await.unwrap();

        let get = encode_req(KvRequest::Get { key: "snap_key".into() });
        let resp = decode_resp(svc2.handle(get).await.unwrap());
        assert_eq!(resp.value, Some(b"snap_val".to_vec()));
    }

    #[test]
    fn test_service_name() {
        let svc = KvService::new();
        assert_eq!(svc.name(), "kv-store");
    }
}
