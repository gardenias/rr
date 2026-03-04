//! In-memory log storage for a single Raft group.
//!
//! Uses `Arc<RwLock<Inner>>` so that the log reader returned from
//! `get_log_reader()` shares state with the writer — critical for Raft
//! replication, which calls `get_log_reader()` and then reads entries
//! written after the reader was obtained.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::Entry;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::Vote;

use crate::type_config::RrTypeConfig;

/// Inner state of [`RrLogStorage`], protected by a shared `RwLock`.
#[derive(Default)]
struct Inner {
    /// Log entries indexed by log index.
    entries: BTreeMap<u64, Entry<RrTypeConfig>>,
    /// Current persisted vote.
    vote: Option<Vote<u64>>,
    /// The last purged log id.
    last_purged_log_id: Option<LogId<u64>>,
}

/// In-memory log storage for a single Raft group.
///
/// Cloning this value is cheap (`Arc` clone) and produces a handle that
/// shares the same underlying storage — which is required so that the
/// `LogReader` returned by [`RaftLogStorage::get_log_reader`] sees entries
/// appended after the reader was obtained.
#[derive(Clone, Default)]
pub struct RrLogStorage {
    inner: Arc<RwLock<Inner>>,
}

impl RaftLogReader<RrTypeConfig> for RrLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RrTypeConfig>>, StorageError<u64>> {
        let inner = self.inner.read().expect("RrLogStorage lock poisoned");
        let entries: Vec<Entry<RrTypeConfig>> = inner
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect();
        Ok(entries)
    }
}

impl RaftLogStorage<RrTypeConfig> for RrLogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<RrTypeConfig>, StorageError<u64>> {
        let inner = self.inner.read().expect("RrLogStorage lock poisoned");
        let last_log_id = inner
            .entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(inner.last_purged_log_id);

        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    /// Returns a reader that **shares** the same underlying data.
    ///
    /// Because `RrLogStorage` wraps `Arc<RwLock<Inner>>`, cloning it is an
    /// `Arc` clone — both the reader and the writer see the same entries.
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.write().expect("RrLogStorage lock poisoned").vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.read().expect("RrLogStorage lock poisoned").vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RrTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<RrTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        {
            let mut inner = self.inner.write().expect("RrLogStorage lock poisoned");
            for entry in entries {
                inner.entries.insert(entry.log_id.index, entry);
            }
        } // lock released before callback
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write().expect("RrLogStorage lock poisoned");
        let to_remove: Vec<u64> = inner
            .entries
            .range(log_id.index..)
            .map(|(k, _)| *k)
            .collect();
        for key in to_remove {
            inner.entries.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write().expect("RrLogStorage lock poisoned");
        let to_remove: Vec<u64> = inner
            .entries
            .range(..=log_id.index)
            .map(|(k, _)| *k)
            .collect();
        for key in to_remove {
            inner.entries.remove(&key);
        }
        inner.last_purged_log_id = Some(log_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::RaftLogStorageExt;
    use openraft::CommittedLeaderId;
    use openraft::EntryPayload;

    fn make_entry(index: u64, term: u64) -> Entry<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    fn make_normal_entry(index: u64, term: u64, data: Vec<u8>) -> Entry<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(data),
        }
    }

    #[tokio::test]
    async fn test_log_storage_append_and_read() {
        let mut storage = RrLogStorage::default();

        let entries = vec![
            make_normal_entry(1, 1, vec![1]),
            make_normal_entry(2, 1, vec![2]),
            make_normal_entry(3, 1, vec![3]),
        ];

        storage.blocking_append(entries).await.unwrap();

        let result = storage.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].log_id.index, 1);
        assert_eq!(result[1].log_id.index, 2);
        assert_eq!(result[2].log_id.index, 3);
    }

    #[tokio::test]
    async fn test_log_storage_vote_save_read() {
        let mut storage = RrLogStorage::default();

        assert_eq!(storage.read_vote().await.unwrap(), None);

        let vote = Vote::new(1, 0);
        storage.save_vote(&vote).await.unwrap();

        let read_back = storage.read_vote().await.unwrap();
        assert_eq!(read_back, Some(vote));
    }

    #[tokio::test]
    async fn test_log_storage_truncate() {
        let mut storage = RrLogStorage::default();

        let entries = vec![
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 1),
            make_entry(4, 1),
            make_entry(5, 1),
        ];

        storage.blocking_append(entries).await.unwrap();

        // Truncate from index 3 (inclusive), so entries 3, 4, 5 are removed
        storage
            .truncate(LogId::new(CommittedLeaderId::new(1, 0), 3))
            .await
            .unwrap();

        let remaining = storage.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 1);
        assert_eq!(remaining[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn test_log_storage_get_log_state() {
        let mut storage = RrLogStorage::default();

        // Empty state
        let state = storage.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, None);

        // After appending
        let entries = vec![make_entry(1, 1), make_entry(2, 1)];
        storage.blocking_append(entries).await.unwrap();

        let state = storage.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn test_log_reader_shares_state_with_writer() {
        // This is the critical regression test: reader obtained before writes
        // must see entries written after it was obtained.
        let mut storage = RrLogStorage::default();
        let mut reader = storage.get_log_reader().await;

        // Write entries AFTER obtaining the reader
        let entries = vec![make_normal_entry(1, 1, b"hello".to_vec())];
        storage.blocking_append(entries).await.unwrap();

        // Reader must see the new entry
        let result = reader.try_get_log_entries(1..2).await.unwrap();
        assert_eq!(
            result.len(),
            1,
            "reader must see entries written after it was obtained"
        );
        assert_eq!(result[0].log_id.index, 1);
    }
}
