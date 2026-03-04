//! RocksDB-backed Raft log storage.
//!
//! Uses [`RocksStorage`] (which already wraps RocksDB behind `spawn_blocking`)
//! to persist Raft log entries and metadata across restarts.

#![cfg(feature = "rocks")]

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::AnyError;
use openraft::Entry;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;

use rr_core::storage::BatchOp;
use rr_core::storage::StorageEngine;
use rr_storage::rocks::RocksStorage;

use crate::type_config::RrTypeConfig;

// ─── Column family / key constants ──────────────────────────────────────────

const CF_LOG: &str = "raft_log";
const CF_META: &str = "metadata";
const KEY_VOTE: &[u8] = b"vote";
const KEY_LAST_PURGED: &[u8] = b"last_purged";

// ─── Serialization helpers ───────────────────────────────────────────────────

fn ser<T: serde::Serialize>(val: &T) -> Result<Vec<u8>, StorageError<u64>> {
    bincode::serde::encode_to_vec(val, bincode::config::standard()).map_err(|e| StorageError::IO {
        source: StorageIOError::write_logs(AnyError::error(e.to_string())),
    })
}

fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError<u64>> {
    let (val, _): (T, _) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard()).map_err(|e| {
            StorageError::IO {
                source: StorageIOError::read_logs(AnyError::error(e.to_string())),
            }
        })?;
    Ok(val)
}

fn to_read_err(e: rr_core::error::StorageError) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::read_logs(AnyError::error(e.to_string())),
    }
}

fn to_write_err(e: rr_core::error::StorageError) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::write_logs(AnyError::error(e.to_string())),
    }
}

// ─── Key encoding ────────────────────────────────────────────────────────────

/// Encode a log index as a big-endian 8-byte key for lexicographic order.
fn index_key(index: u64) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

fn range_start(bound: Bound<&u64>) -> Vec<u8> {
    match bound {
        Bound::Included(&i) => index_key(i),
        Bound::Excluded(&i) => index_key(i.saturating_add(1)),
        Bound::Unbounded => index_key(0),
    }
}

fn range_end(bound: Bound<&u64>) -> Vec<u8> {
    match bound {
        // scan end is exclusive, so Included(i) → i+1
        Bound::Included(&i) => index_key(i.saturating_add(1)),
        Bound::Excluded(&i) => index_key(i),
        Bound::Unbounded => index_key(u64::MAX),
    }
}

// ─── RocksLogStorage ─────────────────────────────────────────────────────────

/// A persistent Raft log storage backed by RocksDB.
///
/// Wraps [`RocksStorage`] in an [`Arc`] so it can be cheaply cloned to satisfy
/// the `type LogReader = Self` requirement while sharing the same underlying DB.
#[derive(Clone)]
pub struct RocksLogStorage {
    storage: Arc<RocksStorage>,
}

impl RocksLogStorage {
    /// Create a `RocksLogStorage` from an existing [`RocksStorage`].
    pub fn new(storage: RocksStorage) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }

    /// Open (or create) a RocksDB instance at `path` with the standard column
    /// families (`raft_log`, `state_machine`, `metadata`).
    pub fn open(path: &str) -> Result<Self, rr_core::error::StorageError> {
        let storage = RocksStorage::open_default(path)?;
        Ok(Self::new(storage))
    }
}

// ─── RaftLogReader ───────────────────────────────────────────────────────────

impl RaftLogReader<RrTypeConfig> for RocksLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RrTypeConfig>>, StorageError<u64>> {
        let start = range_start(range.start_bound());
        let end = range_end(range.end_bound());

        let pairs = self
            .storage
            .scan(CF_LOG, &start, &end)
            .await
            .map_err(to_read_err)?;

        let mut entries = Vec::with_capacity(pairs.len());
        for (_, v) in pairs {
            entries.push(de::<Entry<RrTypeConfig>>(&v)?);
        }
        Ok(entries)
    }
}

// ─── RaftLogStorage ──────────────────────────────────────────────────────────

impl RaftLogStorage<RrTypeConfig> for RocksLogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<RrTypeConfig>, StorageError<u64>> {
        // Read last_purged from metadata CF
        let last_purged = self
            .storage
            .get(CF_META, KEY_LAST_PURGED)
            .await
            .map_err(to_read_err)?
            .map(|b| de::<LogId<u64>>(&b))
            .transpose()?;

        // Find the last log entry by scanning all keys in the log CF
        let start = index_key(0);
        let end = index_key(u64::MAX);
        let pairs = self
            .storage
            .scan(CF_LOG, &start, &end)
            .await
            .map_err(to_read_err)?;

        let last_log_id = pairs
            .last()
            .map(|(_, v)| de::<Entry<RrTypeConfig>>(v))
            .transpose()?
            .map(|e| e.log_id)
            .or(last_purged);

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let bytes = ser(vote)?;
        self.storage
            .put(CF_META, KEY_VOTE, &bytes)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(AnyError::error(e.to_string())),
            })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.storage
            .get(CF_META, KEY_VOTE)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_vote(AnyError::error(e.to_string())),
            })?
            .map(|b| {
                // Vote is serialized as (leader_id, committed) — use `de` for consistency
                bincode::serde::decode_from_slice::<Vote<u64>, _>(
                    &b,
                    bincode::config::standard(),
                )
                .map(|(v, _)| v)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::read_vote(AnyError::error(e.to_string())),
                })
            })
            .transpose()
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
        let mut ops = Vec::new();
        for entry in entries {
            let key = index_key(entry.log_id.index);
            let value = ser(&entry)?;
            ops.push(BatchOp::Put {
                cf: CF_LOG.to_string(),
                key,
                value,
            });
        }
        if !ops.is_empty() {
            self.storage
                .write_batch(ops)
                .await
                .map_err(to_write_err)?;
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // Remove all entries with index >= log_id.index
        let start = index_key(log_id.index);
        let end = index_key(u64::MAX);

        let pairs = self
            .storage
            .scan(CF_LOG, &start, &end)
            .await
            .map_err(to_read_err)?;

        let ops: Vec<BatchOp> = pairs
            .into_iter()
            .map(|(k, _)| BatchOp::Delete {
                cf: CF_LOG.to_string(),
                key: k,
            })
            .collect();

        if !ops.is_empty() {
            self.storage.write_batch(ops).await.map_err(to_write_err)?;
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // Remove all entries with index <= log_id.index, and persist last_purged
        let start = index_key(0);
        let end = index_key(log_id.index.saturating_add(1)); // exclusive end → includes log_id.index

        let pairs = self
            .storage
            .scan(CF_LOG, &start, &end)
            .await
            .map_err(to_read_err)?;

        let mut ops: Vec<BatchOp> = pairs
            .into_iter()
            .map(|(k, _)| BatchOp::Delete {
                cf: CF_LOG.to_string(),
                key: k,
            })
            .collect();

        // Persist the last_purged marker
        let purge_bytes = ser(&log_id)?;
        ops.push(BatchOp::Put {
            cf: CF_META.to_string(),
            key: KEY_LAST_PURGED.to_vec(),
            value: purge_bytes,
        });

        self.storage.write_batch(ops).await.map_err(to_write_err)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::RaftLogStorageExt;
    use openraft::CommittedLeaderId;
    use openraft::EntryPayload;
    use tempfile::TempDir;

    fn make_entry(index: u64, term: u64) -> Entry<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    fn make_data_entry(index: u64, term: u64, data: Vec<u8>) -> Entry<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(data),
        }
    }

    fn open_storage() -> (RocksLogStorage, TempDir) {
        let tmp = TempDir::new().unwrap();
        let s = RocksLogStorage::open(tmp.path().to_str().unwrap()).expect("open db");
        (s, tmp)
    }

    // ── 1: append and read back ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_rocks_append_and_read() {
        let (mut s, _tmp) = open_storage();
        let entries = vec![
            make_data_entry(1, 1, vec![1]),
            make_data_entry(2, 1, vec![2]),
            make_data_entry(3, 1, vec![3]),
        ];
        s.blocking_append(entries).await.unwrap();

        let got = s.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id.index, 1);
        assert_eq!(got[1].log_id.index, 2);
        assert_eq!(got[2].log_id.index, 3);
    }

    // ── 2: vote save + read ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_rocks_vote_save_read() {
        let (mut s, _tmp) = open_storage();
        assert_eq!(s.read_vote().await.unwrap(), None);

        let vote = Vote::new(1, 0);
        s.save_vote(&vote).await.unwrap();

        assert_eq!(s.read_vote().await.unwrap(), Some(vote));
    }

    // ── 3: truncate removes entries from index onwards ──────────────────────

    #[tokio::test]
    async fn test_rocks_truncate() {
        let (mut s, _tmp) = open_storage();
        let entries = vec![
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 1),
            make_entry(4, 1),
            make_entry(5, 1),
        ];
        s.blocking_append(entries).await.unwrap();

        // truncate from index 3 → removes 3, 4, 5
        s.truncate(LogId::new(CommittedLeaderId::new(1, 0), 3))
            .await
            .unwrap();

        let remaining = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 1);
        assert_eq!(remaining[1].log_id.index, 2);
    }

    // ── 4: truncate_from_middle ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_rocks_truncate_from_middle() {
        let (mut s, _tmp) = open_storage();
        let entries = (1..=10).map(|i| make_entry(i, 1)).collect::<Vec<_>>();
        s.blocking_append(entries).await.unwrap();

        s.truncate(LogId::new(CommittedLeaderId::new(1, 0), 6))
            .await
            .unwrap();

        let remaining = s.try_get_log_entries(0..20).await.unwrap();
        assert_eq!(remaining.len(), 5);
        assert_eq!(remaining.last().unwrap().log_id.index, 5);
    }

    // ── 5: purge ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_rocks_purge() {
        let (mut s, _tmp) = open_storage();
        let entries = vec![
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 1),
            make_entry(4, 1),
        ];
        s.blocking_append(entries).await.unwrap();

        // purge up to index 2 (inclusive)
        s.purge(LogId::new(CommittedLeaderId::new(1, 0), 2))
            .await
            .unwrap();

        let remaining = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 3);
        assert_eq!(remaining[1].log_id.index, 4);
    }

    // ── 6: purge saves last_purged ──────────────────────────────────────────

    #[tokio::test]
    async fn test_rocks_purge_saves_last_purged() {
        let (mut s, _tmp) = open_storage();
        let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
        s.blocking_append(entries).await.unwrap();

        let purge_id = LogId::new(CommittedLeaderId::new(1, 0), 2);
        s.purge(purge_id).await.unwrap();

        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(purge_id));
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    // ── 7: get_log_state empty ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_rocks_get_log_state_empty() {
        let (mut s, _tmp) = open_storage();
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, None);
    }

    // ── 8: get_log_state after append ───────────────────────────────────────

    #[tokio::test]
    async fn test_rocks_get_log_state_after_append() {
        let (mut s, _tmp) = open_storage();
        let entries = vec![make_entry(1, 1), make_entry(2, 1)];
        s.blocking_append(entries).await.unwrap();

        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id.unwrap().index, 2);
    }

    // ── 9: vote persists across reopen ─────────────────────────────────────

    #[tokio::test]
    async fn test_rocks_vote_persistence() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        {
            let mut s = RocksLogStorage::open(&path).unwrap();
            let vote = Vote::new(3, 1);
            s.save_vote(&vote).await.unwrap();
        }

        // Reopen — vote must survive
        let mut s2 = RocksLogStorage::open(&path).unwrap();
        let recovered = s2.read_vote().await.unwrap();
        assert_eq!(recovered, Some(Vote::new(3, 1)));
    }

    // ── 10: log entries persist across reopen ──────────────────────────────

    #[tokio::test]
    async fn test_rocks_log_persistence() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        {
            let mut s = RocksLogStorage::open(&path).unwrap();
            let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)];
            s.blocking_append(entries).await.unwrap();
        }

        // Reopen — entries must survive
        let mut s2 = RocksLogStorage::open(&path).unwrap();
        let got = s2.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id.index, 1);
        assert_eq!(got[2].log_id.index, 3);
        assert_eq!(got[2].log_id.leader_id.term, 2);
    }

    // ── 11: blocking_append successfully completes (tests callback path) ───────

    #[tokio::test]
    async fn test_rocks_append_callback_called() {
        // blocking_append uses LogFlushed internally and awaits the callback result;
        // if the callback isn't called the await would never complete.
        let (mut s, _tmp) = open_storage();
        let entries = vec![make_entry(1, 1), make_entry(2, 1)];
        s.blocking_append(entries).await.unwrap();

        // Verify entries landed, confirming the callback round-trip worked.
        let got = s.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(got.len(), 2);
    }
}
