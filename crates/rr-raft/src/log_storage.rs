//! In-memory log storage for a single Raft group.
//!
//! Uses `Arc<RwLock<Inner>>` so that the log reader returned from
//! `get_log_reader()` shares state with the writer — critical for Raft
//! replication, which calls `get_log_reader()` and then reads entries
//! written after the reader was obtained.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

use parking_lot::RwLock;

use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::VoteOf;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;

use crate::type_config::RrTypeConfig;

type RrEntry = <RrTypeConfig as RaftTypeConfig>::Entry;
type RrVote = VoteOf<RrTypeConfig>;
type RrLogId = LogIdOf<RrTypeConfig>;

/// Returns `true` when defensive-mode checks should be enabled.
///
/// Defensive mode is active when either:
/// - The `defensive` cargo feature is compiled in, **or**
/// - The environment variable `RR_STORE_DEFENSIVE` is set to `"on"`.
///
/// The env lookup is cached in a [`std::sync::OnceLock`] (first read wins):
/// it previously ran on EVERY read/append — a hot-path syscall per log
/// operation. Production behavior is thus fixed at first use. Test builds
/// bypass the cache and read the variable live, because the defensive-mode
/// tests flip `RR_STORE_DEFENSIVE` mid-process.
fn is_defensive() -> bool {
    if cfg!(feature = "defensive") {
        return true;
    }
    #[cfg(test)]
    {
        std::env::var("RR_STORE_DEFENSIVE").as_deref() == Ok("on")
    }
    #[cfg(not(test))]
    {
        static DEFENSIVE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *DEFENSIVE.get_or_init(|| std::env::var("RR_STORE_DEFENSIVE").as_deref() == Ok("on"))
    }
}
/// Inner state of [`RrLogStorage`], protected by a shared `RwLock`.
#[derive(Default)]
struct Inner {
    /// Log entries indexed by log index.
    entries: BTreeMap<u64, RrEntry>,
    /// Current persisted vote.
    vote: Option<RrVote>,
    /// The last purged log id.
    last_purged_log_id: Option<RrLogId>,
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
    ) -> Result<Vec<RrEntry>, io::Error> {
        if is_defensive() {
            let start = match range.start_bound() {
                Bound::Included(&s) => Some(s),
                Bound::Excluded(&s) => Some(s + 1),
                Bound::Unbounded => None,
            };
            let end = match range.end_bound() {
                Bound::Included(&e) => Some(e),
                Bound::Excluded(&e) if e > 0 => Some(e - 1),
                Bound::Excluded(_) => Some(0),
                Bound::Unbounded => None,
            };
            if let (Some(s), Some(e)) = (start, end) {
                assert!(
                    s <= e,
                    "defensive: try_get_log_entries called with start ({s}) > end ({e})"
                );
            }
        }

        let inner = self.inner.read();
        let entries: Vec<RrEntry> = inner
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect();
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<RrVote>, io::Error> {
        Ok(self.inner.read().vote)
    }
}

impl RaftLogStorage<RrTypeConfig> for RrLogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<RrTypeConfig>, io::Error> {
        let inner = self.inner.read();
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

    async fn save_vote(&mut self, vote: &RrVote) -> Result<(), io::Error> {
        self.inner.write().vote = Some(*vote);
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<RrTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = RrEntry> + Send,
        I::IntoIter: Send,
    {
        {
            let mut inner = self.inner.write();
            if is_defensive() {
                let mut expected_next = inner
                    .entries
                    .last_key_value()
                    .map(|(k, _)| *k + 1)
                    .or_else(|| inner.last_purged_log_id.map(|id| id.index + 1));
                for entry in entries {
                    if let Some(expected) = expected_next {
                        assert!(
                            entry.log_id.index == expected,
                            "defensive: append entries not contiguous — expected index {expected}, got {}",
                            entry.log_id.index
                        );
                    }
                    expected_next = Some(entry.log_id.index + 1);
                    inner.entries.insert(entry.log_id.index, entry);
                }
            } else {
                for entry in entries {
                    inner.entries.insert(entry.log_id.index, entry);
                }
            }
        } // lock released before callback
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, log_id: Option<RrLogId>) -> Result<(), io::Error> {
        let mut inner = self.inner.write();
        let to_remove: Vec<u64> = if let Some(log_id) = log_id {
            inner
                .entries
                .range((log_id.index + 1)..)
                .map(|(k, _)| *k)
                .collect()
        } else {
            inner.entries.keys().copied().collect()
        };
        for key in to_remove {
            inner.entries.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: RrLogId) -> Result<(), io::Error> {
        let mut inner = self.inner.write();
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
    use openraft::type_config::alias::EntryOf;
    use openraft::vote::leader_id_adv::CommittedLeaderId;
    use openraft::vote::RaftLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::Vote;

    fn make_entry(index: u64, term: u64) -> EntryOf<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    fn make_normal_entry(index: u64, term: u64, data: Vec<u8>) -> EntryOf<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(data.into()),
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

        assert_eq!(RaftLogReader::read_vote(&mut storage).await.unwrap(), None);

        let vote = Vote::new(1, 0);
        storage.save_vote(&vote).await.unwrap();

        let read_back = RaftLogReader::read_vote(&mut storage).await.unwrap();
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

        // truncate_after(Some(3)) keeps 3 and removes 4,5.
        storage
            .truncate_after(Some(LogId::new(CommittedLeaderId::new(1, 0), 3)))
            .await
            .unwrap();

        let remaining = storage.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining[0].log_id.index, 1);
        assert_eq!(remaining[1].log_id.index, 2);
        assert_eq!(remaining[2].log_id.index, 3);
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

/// Tests for defensive mode checks.
///
/// These tests enable defensive mode via the `RR_STORE_DEFENSIVE` env var
/// rather than the feature flag, so they run in normal `cargo test` builds.
#[cfg(test)]
mod defensive_tests {
    use super::*;
    use openraft::storage::RaftLogStorageExt;
    use openraft::type_config::alias::EntryOf;
    use openraft::vote::leader_id_adv::CommittedLeaderId;
    use openraft::vote::RaftLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;

    fn make_entry(index: u64, term: u64) -> EntryOf<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    #[tokio::test]
    #[should_panic(expected = "defensive: append entries not contiguous")]
    async fn test_defensive_append_non_contiguous_panics() {
        std::env::set_var("RR_STORE_DEFENSIVE", "on");
        let mut storage = RrLogStorage::default();

        // First batch: entries 1, 2
        let entries = vec![make_entry(1, 1), make_entry(2, 1)];
        storage.blocking_append(entries).await.unwrap();

        // Second batch: gap — skip index 3, jump to 5
        let entries = vec![make_entry(5, 1)];
        storage.blocking_append(entries).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "defensive: try_get_log_entries called with start")]
    // Intentionally constructs a reversed range (5..3) to exercise the
    // defensive panic path. Clippy's reversed_empty_ranges lint flags this
    // as a "this range is empty" mistake; the lint is exactly inverted from
    // what the test wants to verify.
    #[allow(clippy::reversed_empty_ranges)]
    async fn test_defensive_get_log_entries_start_gt_end_panics() {
        std::env::set_var("RR_STORE_DEFENSIVE", "on");
        let mut storage = RrLogStorage::default();

        // start (5) > end (2) — must panic
        let _ = storage.try_get_log_entries(5..3).await;
    }

    #[tokio::test]
    async fn test_defensive_append_contiguous_succeeds() {
        std::env::set_var("RR_STORE_DEFENSIVE", "on");
        let mut storage = RrLogStorage::default();

        let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
        storage.blocking_append(entries).await.unwrap();

        // Second contiguous batch succeeds
        let entries = vec![make_entry(4, 1), make_entry(5, 1)];
        storage.blocking_append(entries).await.unwrap();

        let inner = storage.inner.read();
        assert_eq!(inner.entries.len(), 5);
    }
}
