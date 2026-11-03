use bincode::config::standard;
use rr_core::error::{CodecError, LifecycleError, RrError, StorageError};
use rr_core::storage::StorageEngine;
use rr_core::types::NodeId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const LOCAL_CLUSTER_IDENTITY_VERSION: u32 = 1;
pub const LOCAL_CLUSTER_IDENTITY_KEY: &[u8] = b"rr/cluster-identity/v1";
const METADATA_CF: &str = "metadata";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalClusterIdentity {
    pub version: u32,
    pub cluster_uuid: Uuid,
    pub node_id: NodeId,
}

impl LocalClusterIdentity {
    fn validate(&self) -> Result<(), RrError> {
        if self.version != LOCAL_CLUSTER_IDENTITY_VERSION {
            return Err(LifecycleError::UnsupportedSchema {
                schema: "LocalClusterIdentity",
                version: self.version,
            }
            .into());
        }
        if self.cluster_uuid.is_nil() {
            return Err(StorageError::Corruption(
                "local cluster identity UUID must not be nil".to_string(),
            )
            .into());
        }
        if self.node_id == 0 {
            return Err(StorageError::Corruption(
                "local cluster identity node ID must not be zero".to_string(),
            )
            .into());
        }
        Ok(())
    }
}

pub async fn load(storage: &dyn StorageEngine) -> Result<Option<LocalClusterIdentity>, RrError> {
    let Some(bytes) = storage.get(METADATA_CF, LOCAL_CLUSTER_IDENTITY_KEY).await? else {
        return Ok(None);
    };
    let (identity, consumed): (LocalClusterIdentity, usize) =
        bincode::serde::decode_from_slice(&bytes, standard()).map_err(|error| {
            StorageError::Corruption(format!("invalid local cluster identity: {error}"))
        })?;
    if consumed != bytes.len() {
        return Err(StorageError::Corruption(
            "local cluster identity contains trailing bytes".to_string(),
        )
        .into());
    }
    identity.validate()?;
    Ok(Some(identity))
}

pub async fn store(
    storage: &dyn StorageEngine,
    identity: &LocalClusterIdentity,
) -> Result<(), RrError> {
    static IDENTITY_STORE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    let _store_guard = IDENTITY_STORE_LOCK.lock().await;
    identity.validate()?;
    match load(storage).await? {
        Some(existing) if existing == *identity => return Ok(()),
        Some(_) => return Err(LifecycleError::IdentityMismatch.into()),
        None => {}
    }
    let bytes = bincode::serde::encode_to_vec(identity, standard())
        .map_err(|error| CodecError::SerializeFailed(error.to_string()))?;
    storage
        .put(METADATA_CF, LOCAL_CLUSTER_IDENTITY_KEY, &bytes)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use rr_core::error::{LifecycleError, RrError, StorageError};
    use rr_core::storage::{BatchOp, StorageEngine};
    use rr_storage::mem::MemStorage;
    use uuid::Uuid;

    use super::{load, store, LocalClusterIdentity};

    fn identity(cluster_uuid: Uuid, node_id: u64) -> LocalClusterIdentity {
        LocalClusterIdentity {
            version: 1,
            cluster_uuid,
            node_id,
        }
    }

    #[tokio::test]
    async fn identity_missing_legacy_record_loads_as_none() {
        let storage = MemStorage::new();
        assert_eq!(load(&storage).await.unwrap(), None);
    }

    #[tokio::test]
    async fn identity_store_load_roundtrip_uses_versioned_metadata_key() {
        let storage = MemStorage::new();
        let expected = identity(Uuid::from_u128(0x1234), 7);

        store(&storage, &expected).await.unwrap();
        store(&storage, &expected)
            .await
            .expect("storing an identical identity must be idempotent");

        assert_eq!(load(&storage).await.unwrap(), Some(expected));
        let raw = storage
            .get("metadata", b"rr/cluster-identity/v1")
            .await
            .unwrap()
            .expect("identity must use the versioned metadata key");
        let (decoded, consumed): (LocalClusterIdentity, usize) =
            bincode::serde::decode_from_slice(&raw, bincode::config::standard()).unwrap();
        assert_eq!(consumed, raw.len());
        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn identity_store_rejects_cluster_uuid_mismatch_without_replacing_record() {
        let storage = MemStorage::new();
        let original = identity(Uuid::from_u128(0x1234), 7);
        store(&storage, &original).await.unwrap();

        let err = store(&storage, &identity(Uuid::from_u128(0x5678), 7))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RrError::Lifecycle(LifecycleError::IdentityMismatch)
        ));
        assert_eq!(load(&storage).await.unwrap(), Some(original));
    }

    #[tokio::test]
    async fn identity_store_rejects_node_id_mismatch_without_replacing_record() {
        let storage = MemStorage::new();
        let cluster_uuid = Uuid::from_u128(0x1234);
        let original = identity(cluster_uuid, 7);
        store(&storage, &original).await.unwrap();

        let err = store(&storage, &identity(cluster_uuid, 8))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RrError::Lifecycle(LifecycleError::IdentityMismatch)
        ));
        assert_eq!(load(&storage).await.unwrap(), Some(original));
    }

    #[tokio::test]
    async fn identity_load_rejects_corrupt_and_trailing_bytes() {
        for bytes in [b"not bincode".to_vec(), {
            let mut encoded = bincode::serde::encode_to_vec(
                identity(Uuid::from_u128(0x1234), 7),
                bincode::config::standard(),
            )
            .unwrap();
            encoded.push(0xff);
            encoded
        }] {
            let storage = MemStorage::new();
            storage
                .put("metadata", b"rr/cluster-identity/v1", &bytes)
                .await
                .unwrap();
            let err = load(&storage).await.unwrap_err();
            assert!(
                matches!(err, RrError::Storage(StorageError::Corruption(_))),
                "corrupt identity must fail closed, got {err}"
            );
        }
    }

    #[tokio::test]
    async fn identity_load_rejects_unsupported_version() {
        let storage = MemStorage::new();
        let bytes = bincode::serde::encode_to_vec(
            LocalClusterIdentity {
                version: 2,
                cluster_uuid: Uuid::from_u128(0x1234),
                node_id: 7,
            },
            bincode::config::standard(),
        )
        .unwrap();
        storage
            .put("metadata", b"rr/cluster-identity/v1", &bytes)
            .await
            .unwrap();

        let err = load(&storage).await.unwrap_err();
        assert!(matches!(
            err,
            RrError::Lifecycle(LifecycleError::UnsupportedSchema {
                schema: "LocalClusterIdentity",
                version: 2
            })
        ));
    }

    struct FailingPutStorage {
        inner: MemStorage,
    }

    #[async_trait]
    impl StorageEngine for FailingPutStorage {
        async fn put(&self, _cf: &str, _key: &[u8], _value: &[u8]) -> Result<(), StorageError> {
            Err(std::io::Error::other("injected identity write failure").into())
        }

        async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
            self.inner.get(cf, key).await
        }

        async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), StorageError> {
            self.inner.delete(cf, key).await
        }

        async fn scan(
            &self,
            cf: &str,
            start: &[u8],
            end: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
            self.inner.scan(cf, start, end).await
        }

        async fn write_batch(&self, ops: Vec<BatchOp>) -> Result<(), StorageError> {
            self.inner.write_batch(ops).await
        }

        async fn flush(&self) -> Result<(), StorageError> {
            self.inner.flush().await
        }

        async fn create_cf(&self, name: &str) -> Result<(), StorageError> {
            self.inner.create_cf(name).await
        }
    }

    #[tokio::test]
    async fn identity_store_propagates_write_failure() {
        let storage = FailingPutStorage {
            inner: MemStorage::new(),
        };
        let err = store(&storage, &identity(Uuid::from_u128(0x1234), 7))
            .await
            .unwrap_err();
        assert!(matches!(err, RrError::Storage(StorageError::Io(_))));
    }

    struct YieldingStorage {
        inner: MemStorage,
    }

    #[async_trait]
    impl StorageEngine for YieldingStorage {
        async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
            tokio::task::yield_now().await;
            self.inner.put(cf, key, value).await
        }

        async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
            tokio::task::yield_now().await;
            self.inner.get(cf, key).await
        }

        async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), StorageError> {
            self.inner.delete(cf, key).await
        }

        async fn scan(
            &self,
            cf: &str,
            start: &[u8],
            end: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
            self.inner.scan(cf, start, end).await
        }

        async fn write_batch(&self, ops: Vec<BatchOp>) -> Result<(), StorageError> {
            self.inner.write_batch(ops).await
        }

        async fn flush(&self) -> Result<(), StorageError> {
            self.inner.flush().await
        }

        async fn create_cf(&self, name: &str) -> Result<(), StorageError> {
            self.inner.create_cf(name).await
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn identity_concurrent_store_has_one_winner_and_preserves_record() {
        let storage = YieldingStorage {
            inner: MemStorage::new(),
        };
        let first = identity(Uuid::from_u128(0x1234), 7);
        let second = identity(Uuid::from_u128(0x5678), 7);

        let (first_result, second_result) =
            tokio::join!(store(&storage, &first), store(&storage, &second),);
        let expected = match (first_result, second_result) {
            (Ok(()), Err(RrError::Lifecycle(LifecycleError::IdentityMismatch))) => first,
            (Err(RrError::Lifecycle(LifecycleError::IdentityMismatch)), Ok(())) => second,
            (first_result, second_result) => panic!(
                "concurrent stores must have one winner and one mismatch: \
                 first={first_result:?}, second={second_result:?}"
            ),
        };

        assert_eq!(load(&storage).await.unwrap(), Some(expected));
    }
}
