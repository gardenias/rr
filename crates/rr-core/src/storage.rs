//! StorageEngine trait for pluggable storage backends.

use async_trait::async_trait;

use crate::error::StorageError;

/// A batch operation for atomic multi-key writes.
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Insert or update a key-value pair in a column family.
    Put {
        /// Column family to write into.
        cf: String,
        /// Key to insert or update.
        key: Vec<u8>,
        /// Value to store.
        value: Vec<u8>,
    },
    /// Delete a key from a column family.
    Delete {
        /// Column family to delete from.
        cf: String,
        /// Key to delete.
        key: Vec<u8>,
    },
}

/// Trait for a pluggable storage engine with column family support.
///
/// Column families in use by the framework:
/// - `"raft_log"`: Raft log entries
/// - `"state_machine"`: User state machine data
/// - `"metadata"`: Framework metadata (votes, snapshots, etc.)
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Insert or update a key-value pair in a column family.
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    /// Get a value by key from a column family. Returns None if not found.
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    /// Delete a key from a column family.
    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), StorageError>;
    /// Scan a range of keys [start, end) in a column family.
    async fn scan(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>;
    /// Apply multiple operations atomically.
    async fn write_batch(&self, ops: Vec<BatchOp>) -> Result<(), StorageError>;
    /// Flush pending writes to durable storage (no-op for in-memory).
    async fn flush(&self) -> Result<(), StorageError>;
    /// Create a new column family.
    async fn create_cf(&self, name: &str) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_op_put_construction() {
        let op = BatchOp::Put {
            cf: "state_machine".to_string(),
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };
        match op {
            BatchOp::Put { cf, key, value } => {
                assert_eq!(cf, "state_machine");
                assert_eq!(key, b"key1");
                assert_eq!(value, b"value1");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_batch_op_delete_construction() {
        let op = BatchOp::Delete {
            cf: "raft_log".to_string(),
            key: b"log_1".to_vec(),
        };
        match op {
            BatchOp::Delete { cf, key } => {
                assert_eq!(cf, "raft_log");
                assert_eq!(key, b"log_1");
            }
            _ => panic!("wrong variant"),
        }
    }

    // Verify StorageEngine is object-safe (can be used as trait object)
    #[test]
    fn test_storage_engine_is_object_safe() {
        // This is a compile-time check — if StorageEngine wasn't object-safe, this wouldn't compile
        let _: Option<Box<dyn StorageEngine>> = None;
    }
}
