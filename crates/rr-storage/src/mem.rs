//! In-memory [`StorageEngine`] implementation backed by `BTreeMap` per column family.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use rr_core::error::StorageError;
use rr_core::storage::{BatchOp, StorageEngine};

type Cf = BTreeMap<Vec<u8>, Vec<u8>>;

/// An in-memory storage engine using `BTreeMap` per column family.
///
/// Suitable for development, testing, and ephemeral workloads.
/// Data is **not** persisted to disk.
#[derive(Clone)]
pub struct MemStorage {
    inner: Arc<RwLock<HashMap<String, Cf>>>,
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemStorage {
    /// Create a new `MemStorage` pre-populated with the framework's default column families:
    /// `raft_log`, `state_machine`, and `metadata`.
    pub fn new() -> Self {
        let mut map = HashMap::new();
        map.insert("raft_log".to_string(), BTreeMap::new());
        map.insert("state_machine".to_string(), BTreeMap::new());
        map.insert("metadata".to_string(), BTreeMap::new());
        Self {
            inner: Arc::new(RwLock::new(map)),
        }
    }
}

#[async_trait]
impl StorageEngine for MemStorage {
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut guard = self.inner.write().await;
        let cf_map = guard.get_mut(cf).ok_or_else(|| StorageError::NotFound {
            key: format!("cf:{cf}"),
        })?;
        cf_map.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let guard = self.inner.read().await;
        let cf_map = guard.get(cf).ok_or_else(|| StorageError::NotFound {
            key: format!("cf:{cf}"),
        })?;
        Ok(cf_map.get(key).cloned())
    }

    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), StorageError> {
        let mut guard = self.inner.write().await;
        let cf_map = guard.get_mut(cf).ok_or_else(|| StorageError::NotFound {
            key: format!("cf:{cf}"),
        })?;
        cf_map.remove(key);
        Ok(())
    }

    async fn scan(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let guard = self.inner.read().await;
        let cf_map = guard.get(cf).ok_or_else(|| StorageError::NotFound {
            key: format!("cf:{cf}"),
        })?;
        let result = if start > end {
            Vec::new()
        } else {
            cf_map
                .range((
                    Bound::Included(start.to_vec()),
                    Bound::Excluded(end.to_vec()),
                ))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        Ok(result)
    }

    async fn last_key_value(&self, cf: &str) -> Result<Option<(Vec<u8>, Vec<u8>)>, StorageError> {
        let guard = self.inner.read().await;
        let cf_map = guard.get(cf).ok_or_else(|| StorageError::NotFound {
            key: format!("cf:{cf}"),
        })?;
        Ok(cf_map.last_key_value().map(|(k, v)| (k.clone(), v.clone())))
    }

    async fn write_batch(&self, ops: Vec<BatchOp>) -> Result<(), StorageError> {
        let mut guard = self.inner.write().await;
        for op in &ops {
            let cf = match op {
                BatchOp::Put { cf, .. } | BatchOp::Delete { cf, .. } => cf,
            };
            if !guard.contains_key(cf) {
                return Err(StorageError::NotFound {
                    key: format!("cf:{cf}"),
                });
            }
        }
        for op in ops {
            match op {
                BatchOp::Put { cf, key, value } => {
                    guard.get_mut(&cf).expect("cf validated above").insert(key, value);
                }
                BatchOp::Delete { cf, key } => {
                    guard.get_mut(&cf).expect("cf validated above").remove(&key);
                }
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), StorageError> {
        // no-op for in-memory storage
        Ok(())
    }

    async fn create_cf(&self, name: &str) -> Result<(), StorageError> {
        let mut guard = self.inner.write().await;
        guard.entry(name.to_string()).or_insert_with(BTreeMap::new);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get_roundtrip() {
        let storage = MemStorage::new();
        storage
            .put("state_machine", b"key1", b"value1")
            .await
            .unwrap();
        let val = storage.get("state_machine", b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_get_missing_returns_none() {
        let storage = MemStorage::new();
        let val = storage.get("state_machine", b"missing").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = MemStorage::new();
        storage
            .put("state_machine", b"key1", b"value1")
            .await
            .unwrap();
        storage.delete("state_machine", b"key1").await.unwrap();
        let val = storage.get("state_machine", b"key1").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_scan_range() {
        let storage = MemStorage::new();
        storage.put("state_machine", b"b", b"2").await.unwrap();
        storage.put("state_machine", b"a", b"1").await.unwrap();
        storage.put("state_machine", b"c", b"3").await.unwrap();
        let results = storage.scan("state_machine", b"a", b"c").await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"a".to_vec());
        assert_eq!(results[1].0, b"b".to_vec());
    }

    #[tokio::test]
    async fn test_last_key_value() {
        let storage = MemStorage::new();
        assert_eq!(storage.last_key_value("state_machine").await.unwrap(), None);

        storage.put("state_machine", b"b", b"2").await.unwrap();
        storage.put("state_machine", b"a", b"1").await.unwrap();
        storage.put("state_machine", b"c", b"3").await.unwrap();
        assert_eq!(
            storage.last_key_value("state_machine").await.unwrap(),
            Some((b"c".to_vec(), b"3".to_vec()))
        );

        // Greatest key moves down after a delete of the tail.
        storage.delete("state_machine", b"c").await.unwrap();
        assert_eq!(
            storage.last_key_value("state_machine").await.unwrap(),
            Some((b"b".to_vec(), b"2".to_vec()))
        );

        let err = storage.last_key_value("nonexistent_cf").await.unwrap_err();
        assert!(matches!(err, StorageError::NotFound { .. }));
    }

    #[tokio::test]
    async fn test_write_batch_puts() {
        let storage = MemStorage::new();
        let ops = vec![
            BatchOp::Put {
                cf: "state_machine".into(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            BatchOp::Put {
                cf: "state_machine".into(),
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
            },
            BatchOp::Put {
                cf: "state_machine".into(),
                key: b"k3".to_vec(),
                value: b"v3".to_vec(),
            },
        ];
        storage.write_batch(ops).await.unwrap();
        assert_eq!(
            storage.get("state_machine", b"k1").await.unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            storage.get("state_machine", b"k2").await.unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            storage.get("state_machine", b"k3").await.unwrap(),
            Some(b"v3".to_vec())
        );
    }

    #[tokio::test]
    async fn test_write_batch_mixed_put_delete() {
        let storage = MemStorage::new();
        storage
            .put("state_machine", b"existing", b"val")
            .await
            .unwrap();
        let ops = vec![
            BatchOp::Put {
                cf: "state_machine".into(),
                key: b"new_key".to_vec(),
                value: b"new_val".to_vec(),
            },
            BatchOp::Delete {
                cf: "state_machine".into(),
                key: b"existing".to_vec(),
            },
        ];
        storage.write_batch(ops).await.unwrap();
        assert_eq!(
            storage.get("state_machine", b"new_key").await.unwrap(),
            Some(b"new_val".to_vec())
        );
        assert_eq!(
            storage.get("state_machine", b"existing").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_flush_is_noop() {
        let storage = MemStorage::new();
        // flush should succeed silently
        storage.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_create_cf() {
        let storage = MemStorage::new();
        storage.create_cf("custom_cf").await.unwrap();
        storage.put("custom_cf", b"key", b"val").await.unwrap();
        let val = storage.get("custom_cf", b"key").await.unwrap();
        assert_eq!(val, Some(b"val".to_vec()));
    }

    #[tokio::test]
    async fn test_unknown_cf_returns_error() {
        let storage = MemStorage::new();
        let result = storage.get("nonexistent_cf", b"key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_default_equals_new() {
        let storage = MemStorage::default();
        storage
            .put("state_machine", b"key", b"val")
            .await
            .expect("default() must pre-populate the framework CFs like new()");
        assert_eq!(
            storage.get("state_machine", b"key").await.unwrap(),
            Some(b"val".to_vec())
        );
    }

    #[tokio::test]
    async fn test_write_batch_is_atomic_on_unknown_cf() {
        let storage = MemStorage::new();
        let ops = vec![
            BatchOp::Put {
                cf: "state_machine".into(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            BatchOp::Put {
                cf: "nonexistent_cf".into(),
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
            },
        ];
        let err = storage.write_batch(ops).await.unwrap_err();
        assert!(matches!(err, StorageError::NotFound { .. }));
        assert_eq!(
            storage.get("state_machine", b"k1").await.unwrap(),
            None,
            "a failed batch must not apply any prefix of its ops"
        );
    }

    #[tokio::test]
    async fn test_scan_start_after_end_returns_empty() {
        let storage = MemStorage::new();
        storage.put("state_machine", b"a", b"1").await.unwrap();
        let results = storage.scan("state_machine", b"z", b"a").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_concurrent_reads_and_writes() {
        let storage = Arc::new(MemStorage::new());
        let mut handles = vec![];
        for i in 0..10u8 {
            let s = storage.clone();
            handles.push(tokio::spawn(async move {
                s.put("state_machine", &[i], &[i * 2]).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // All 10 writes should be visible
        for i in 0..10u8 {
            let val = storage.get("state_machine", &[i]).await.unwrap();
            assert_eq!(val, Some(vec![i * 2]));
        }
    }

    use proptest::prelude::*;

    proptest! {
        // R-6 review item L7a: persist failing seeds into the git-tracked
        // `crates/rr-storage/proptest-regressions/` so a failure reproduces
        // across runs; `SourceParallel` resolves relative to `src/lib.rs`.
        #![proptest_config(ProptestConfig {
            failure_persistence: Some(Box::new(
                proptest::test_runner::FileFailurePersistence::SourceParallel(
                    "proptest-regressions",
                ),
            )),
            ..ProptestConfig::default()
        })]
        #[test]
        fn proptest_mem_put_get_roundtrip(key: Vec<u8>, value: Vec<u8>) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let storage = MemStorage::new();
                storage.put("state_machine", &key, &value).await.unwrap();
                let got = storage.get("state_machine", &key).await.unwrap();
                prop_assert_eq!(got, Some(value));
                Ok(())
            })?;
        }

        #[test]
        fn proptest_mem_delete_removes_key(key: Vec<u8>, value: Vec<u8>) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let storage = MemStorage::new();
                storage.put("state_machine", &key, &value).await.unwrap();
                storage.delete("state_machine", &key).await.unwrap();
                let got = storage.get("state_machine", &key).await.unwrap();
                prop_assert_eq!(got, None);
                Ok(())
            })?;
        }

        #[test]
        fn proptest_mem_last_write_wins(key: Vec<u8>, val1: Vec<u8>, val2: Vec<u8>) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let storage = MemStorage::new();
                storage.put("state_machine", &key, &val1).await.unwrap();
                storage.put("state_machine", &key, &val2).await.unwrap();
                let got = storage.get("state_machine", &key).await.unwrap();
                prop_assert_eq!(got, Some(val2));
                Ok(())
            })?;
        }

        #[test]
        fn proptest_mem_get_missing_returns_none(key: Vec<u8>) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let storage = MemStorage::new();
                let got = storage.get("state_machine", &key).await.unwrap();
                prop_assert_eq!(got, None);
                Ok(())
            })?;
        }
    }
}
