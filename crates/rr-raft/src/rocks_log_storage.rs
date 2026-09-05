//! RocksDB-backed Raft log storage with group-commit IO batching.
//!
//! Uses [`RocksStorage`] (which already wraps RocksDB behind `spawn_blocking`)
//! to persist Raft log entries and metadata across restarts.
//!
//! The [`append`](RaftLogStorage::append) implementation is **truly async**:
//! it enqueues operations to an [`IoBatcher`] and returns immediately, so
//! OpenRaft's pipeline support is engaged.  The batcher accumulates concurrent
//! appends and flushes them in a single `write_batch()` (group commit), then
//! fires each append's `IOFlushed` callback itself — no relay tasks.
//!
//! # The "submitted ⇒ visible" contract (R-4 finding, ADR A-0015)
//!
//! OpenRaft (0.10-alpha) gates its apply and replication pipelines on log-IO
//! being **submitted**, not flushed: `Engine::next_progress_driven_command`
//! computes the applicable frontier from `log_progress.submitted()`. The
//! storage contract is therefore: once `append()` returns `Ok`, a subsequent
//! [`try_get_log_entries`](RaftLogReader::try_get_log_entries) MUST observe
//! those entries. An enqueue-and-return `append()` alone violates that —
//! the sm worker's `entries_stream` then reads RocksDB before the batcher
//! has flushed, and openraft's debug-build assertion
//! (`sm/worker.rs: assert end-1 == got_last_index`) kills the state-machine
//! worker (found by the R-4 crash harness: `tests/crash`, both scenarios;
//! in release builds the same race silently skips applies).
//!
//! `RocksLogStorage` restores the contract with a **pending overlay**: a
//! shared `BTreeMap<index, entry>` of submitted-but-unflushed entries. Reads
//! clone the in-range overlay slice **before** scanning RocksDB and merge the
//! two (overlay wins): RocksDB iterators pin their view at creation, so a
//! scan can miss a batch that flushed mid-read — but such a batch's entries
//! are still in the pre-scan snapshot, and entries pruned before the snapshot
//! are necessarily visible to the post-snapshot scan (ADR A-0015). Entries
//! are pruned by a post-flush hook the batcher fires after the covering
//! `write_batch`.
//!
//! The same race exists between queued appends and the **direct** writes of
//! `truncate_after`/`purge`: without ordering, a batch flushed after the
//! delete would resurrect truncated entries. Both therefore await
//! [`IoBatcher::flush_barrier`] first (read-your-submit ordering), then drop
//! the affected overlay entries, then issue the delete.
//!
//! Shutdown goes through [`RocksLogStorage::close`] (ADR A-0011 C3): after
//! it returns, every accepted append is durable and every callback answered.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};

use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::VoteOf;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;

use rr_core::storage::BatchOp;
use rr_core::storage::StorageEngine;
use rr_storage::rocks::RocksStorage;

use crate::io_batcher::{IoBatcher, IoBatcherConfig};
use crate::type_config::RrTypeConfig;

type RrEntry = <RrTypeConfig as RaftTypeConfig>::Entry;
type RrVote = VoteOf<RrTypeConfig>;
type RrLogId = LogIdOf<RrTypeConfig>;

// ─── Column family / key constants ──────────────────────────────────────────

const CF_LOG: &str = "raft_log";
const CF_META: &str = "metadata";
const KEY_VOTE: &[u8] = b"vote";
const KEY_LAST_PURGED: &[u8] = b"last_purged";

// ─── Serialization helpers ───────────────────────────────────────────────────

fn ser<T: serde::Serialize>(val: &T) -> Result<Vec<u8>, io::Error> {
    bincode::serde::encode_to_vec(val, bincode::config::standard())
        .map_err(|e| io::Error::other(e.to_string()))
}

fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, io::Error> {
    let (val, _): (T, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(val)
}

fn to_read_err(e: rr_core::error::StorageError) -> io::Error {
    io::Error::other(e.to_string())
}

fn to_write_err(e: rr_core::error::StorageError) -> io::Error {
    io::Error::other(e.to_string())
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
/// An [`IoBatcher`] runs as a per-instance background task that accumulates
/// concurrent `append()` calls and flushes them in a single `write_batch()`
/// (group commit), dramatically increasing single-group write throughput.
///
/// All clones share the `pending` overlay (see module docs), so reads through
/// openraft's log reader (a clone) observe entries appended through any other
/// clone.
#[derive(Clone)]
pub struct RocksLogStorage {
    storage: Arc<RocksStorage>,
    batcher: Arc<IoBatcher>,
    /// Submitted-but-unflushed log entries, keyed by log index. Restores
    /// openraft's "submitted ⇒ visible" contract for the async append path
    /// (module docs). Bounded by the batcher channel depth × max batch size;
    /// pruned by the post-flush hook attached to every append.
    pending: Arc<Mutex<BTreeMap<u64, RrEntry>>>,
}

impl RocksLogStorage {
    /// Create a `RocksLogStorage` from an existing [`RocksStorage`] with
    /// custom batcher tuning.
    pub fn new_with_config(storage: RocksStorage, config: IoBatcherConfig) -> Self {
        let storage = Arc::new(storage);
        let batcher = Arc::new(IoBatcher::new(storage.clone(), config));
        Self {
            storage,
            batcher,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Create a `RocksLogStorage` with default batcher configuration.
    pub fn new(storage: RocksStorage) -> Self {
        Self::new_with_config(storage, IoBatcherConfig::default())
    }

    /// Open (or create) a RocksDB instance at `path` with default batcher config.
    pub fn open(path: &str) -> Result<Self, rr_core::error::StorageError> {
        let storage = RocksStorage::open_default(path)?;
        Ok(Self::new(storage))
    }

    /// Open with custom batcher tuning (window + max_entries).
    pub fn open_with_config(
        path: &str,
        config: IoBatcherConfig,
    ) -> Result<Self, rr_core::error::StorageError> {
        let storage = RocksStorage::open_default(path)?;
        Ok(Self::new_with_config(storage, config))
    }

    /// Flush every append accepted so far and shut the IO batcher down.
    ///
    /// ADR A-0011 C3: when `close()` returns `Ok`, all queued entries have
    /// been folded into a final `write_batch` (fsync under
    /// `DurabilityPolicy::Strict`) and every outstanding `IOFlushed`
    /// callback has been answered exactly once. Idempotent; after closing
    /// begins, new `append` calls fail fast. Dropping the storage without
    /// `close()` still drains the batcher, but only as a best-effort
    /// fallback — the durability contract is enforced by `close()`.
    pub async fn close(&self) -> Result<(), io::Error> {
        self.batcher.close().await.map_err(io::Error::other)
    }

    /// Wait until every append accepted by the batcher **so far** has been
    /// flushed to RocksDB (and its overlay entries pruned).
    ///
    /// `truncate_after`/`purge` write directly (bypassing the batcher), so
    /// without this barrier a batch flushed *after* their delete would
    /// resurrect entries they just removed (module docs, ADR A-0015). A
    /// barrier failure means the worker is gone — nothing queued can
    /// overtake the delete, so it is safe to proceed; logged for forensics.
    async fn await_batcher_drain(&self) {
        if let Err(e) = self.batcher.flush_barrier().await {
            tracing::warn!(
                error = %e,
                "io batcher barrier failed before direct write; proceeding \
                 (worker is gone, no queued append can overtake)"
            );
        }
    }

    /// Read the log in `range`: clone the in-range overlay entries FIRST
    /// (a consistent snapshot of what is submitted-but-unflushed), then scan
    /// RocksDB, then merge (overlay wins, ordered by log index).
    ///
    /// Why clone-first (ADR A-0015): RocksDB iterators pin their view at
    /// creation, so a scan started before a flush does not see it. The two
    /// sources are nonetheless complete when snapshotted in this order:
    ///
    /// - an entry pruned from the overlay *before* the clone had its flush
    ///   completed before the clone, so the scan (iterating after the clone)
    ///   sees it;
    /// - an entry pruned *after* the clone is in the clone;
    /// - an entry staged *after* the clone belongs to an append whose
    ///   submit-marker fired after the reader computed `range`, so it cannot
    ///   be the range head (markers and staging are index-ordered) — at
    ///   worst a tail entry is missing, which openraft's replication layer
    ///   tolerates as "nothing new to send" and retries.
    ///
    /// A head gap — a non-empty result not starting at `lo`, or an empty
    /// result with overlay data lost — is therefore impossible; the warn
    /// below is a permanent tripwire for regressions of this argument.
    async fn read_log_range<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &self,
        range: RB,
    ) -> Result<Vec<RrEntry>, io::Error> {
        let lo = match range.start_bound() {
            Bound::Included(&i) => i,
            Bound::Excluded(&i) => i.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let hi = match range.end_bound() {
            // exclusive end for the overlay range below
            Bound::Included(&i) => i.saturating_add(1),
            Bound::Excluded(&i) => i,
            Bound::Unbounded => u64::MAX,
        };

        // 1. Overlay snapshot (short lock, never held across the scan).
        let staged: Vec<RrEntry> = {
            let pending = self.pending.lock().unwrap();
            pending
                .range(lo..hi)
                .map(|(_, entry)| entry.clone())
                .collect()
        };

        // 2. RocksDB scan (iterator view pins AFTER the overlay snapshot).
        let start = range_start(range.start_bound());
        let end = range_end(range.end_bound());
        let pairs = self
            .storage
            .scan(CF_LOG, &start, &end)
            .await
            .map_err(to_read_err)?;

        // 3. Merge: durable scan + staged snapshot (snapshot wins on ties —
        //    same bytes, but keep the newest submitted version by contract).
        let mut merged: BTreeMap<u64, RrEntry> = BTreeMap::new();
        for (_, v) in pairs {
            let entry = de::<RrEntry>(&v)?;
            merged.insert(entry.log_id.index, entry);
        }
        for entry in staged {
            merged.insert(entry.log_id.index, entry);
        }
        let result: Vec<RrEntry> = merged.into_values().collect();

        // Regression tripwire for the ordering argument above (cheap check;
        // diagnostics only computed on violation).
        if lo < hi {
            let head_gap = match result.first() {
                Some(first) => first.log_id.index != lo,
                None => false,
            };
            if head_gap {
                let overlay_keys: Vec<u64> = {
                    let pending = self.pending.lock().unwrap();
                    pending.range(lo..hi).map(|(k, _)| *k).collect()
                };
                let result_keys: Vec<u64> = result.iter().map(|e| e.log_id.index).collect();
                tracing::warn!(
                    lo,
                    hi,
                    ?overlay_keys,
                    ?result_keys,
                    "A-0015 head gap: log read missed the range head — \
                     submitted-⇒-visible ordering regressed"
                );
            }
        }
        Ok(result)
    }
}

// ─── RaftLogReader ───────────────────────────────────────────────────────────

impl RaftLogReader<RrTypeConfig> for RocksLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RrEntry>, io::Error> {
        // Snapshot-first read: openraft's apply/replicate pipelines are gated
        // on log-IO *submission*, so entries still queued in the batcher must
        // be visible here (module docs, ADR A-0015).
        self.read_log_range(range).await
    }

    async fn read_vote(&mut self) -> Result<Option<RrVote>, io::Error> {
        self.storage
            .get(CF_META, KEY_VOTE)
            .await
            .map_err(to_read_err)?
            .map(|b| de::<RrVote>(&b))
            .transpose()
    }
}

// ─── RaftLogStorage ──────────────────────────────────────────────────────────

impl RaftLogStorage<RrTypeConfig> for RocksLogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<RrTypeConfig>, io::Error> {
        // Read last_purged from metadata CF
        let last_purged = self
            .storage
            .get(CF_META, KEY_LAST_PURGED)
            .await
            .map_err(to_read_err)?
            .map(|b| de::<RrLogId>(&b))
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
            .map(|(_, v)| de::<RrEntry>(v))
            .transpose()?
            .map(|e| e.log_id)
            .or(last_purged);

        // Include the submitted-but-unflushed tail: openraft treats submitted
        // appends as part of the log (module docs, ADR A-0015). In practice
        // `get_log_state` runs at startup when the overlay is empty; the merge
        // keeps the mid-run behavior honest if that ever changes.
        let last_log_id = {
            let pending = self.pending.lock().unwrap();
            match pending.values().next_back() {
                Some(entry) if Some(entry.log_id) > last_log_id => Some(entry.log_id),
                _ => last_log_id,
            }
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &RrVote) -> Result<(), io::Error> {
        let bytes = ser(vote)?;
        self.storage
            .put(CF_META, KEY_VOTE, &bytes)
            .await
            .map_err(to_write_err)
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
        let mut ops = Vec::new();
        let mut staged = Vec::new();
        for entry in entries {
            let key = index_key(entry.log_id.index);
            let value = ser(&entry)?;
            ops.push(BatchOp::Put {
                cf: CF_LOG.to_string(),
                key,
                value,
            });
            staged.push(entry);
        }

        if ops.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }

        // "submitted ⇒ visible" (ADR A-0015): stage the entries in the
        // pending overlay BEFORE handing the ops to the batcher, so a reader
        // racing the flush (openraft's apply/replicate pipelines) observes
        // them. The batcher fires the prune hook after the covering
        // `write_batch` completes — on the error path too, since entries that
        // failed to write must not linger in the overlay.
        let indices: Vec<u64> = staged.iter().map(|e| e.log_id.index).collect();
        {
            let mut pending = self.pending.lock().unwrap();
            for entry in staged {
                pending.insert(entry.log_id.index, entry);
            }
        }
        let hook = {
            let pending = Arc::clone(&self.pending);
            let indices = indices.clone();
            move |_result: Result<(), String>| {
                let mut pending = pending.lock().unwrap();
                for idx in &indices {
                    pending.remove(idx);
                }
            }
        };

        // Hand the ops AND the completion callback to the batcher: the
        // worker fires `callback.io_completed` itself after the batch's
        // `write_batch` (fsync under Strict) completes — no per-append
        // relay task on the hot path (ADR A-0011 C1/P4).  We return
        // immediately so that the RaftCore task can continue processing
        // the next proposal (pipeline support).
        if let Err(e) = self.batcher.submit_with_hook(ops, callback, hook).await {
            // The worker never accepted the ops: the hook will not fire, so
            // unstage the entries here or they would be readable (but never
            // durable) forever.
            let mut pending = self.pending.lock().unwrap();
            for idx in &indices {
                pending.remove(idx);
            }
            return Err(io::Error::other(e));
        }
        Ok(())
    }

    async fn truncate_after(&mut self, log_id: Option<RrLogId>) -> Result<(), io::Error> {
        // Ordering barrier (ADR A-0015): queued appends must be flushed before
        // this delete lands, or a later batch flush would resurrect them.
        self.await_batcher_drain().await;

        // Drop the truncated tail from the overlay too.
        {
            let mut pending = self.pending.lock().unwrap();
            match &log_id {
                Some(id) => {
                    let _tail = pending.split_off(&id.index.saturating_add(1));
                }
                None => pending.clear(),
            }
        }

        // Remove all entries with index > log_id.index.
        // If None, remove all entries.
        let start = match log_id {
            Some(log_id) => index_key(log_id.index.saturating_add(1)),
            None => index_key(0),
        };
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

    async fn purge(&mut self, log_id: RrLogId) -> Result<(), io::Error> {
        // Ordering barrier, same rationale as `truncate_after` (ADR A-0015).
        self.await_batcher_drain().await;

        // Purged entries leave the overlay as well.
        self.pending
            .lock()
            .unwrap()
            .retain(|&idx, _| idx > log_id.index);

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
    use openraft::type_config::alias::EntryOf;
    use openraft::vote::leader_id_adv::CommittedLeaderId;
    use openraft::vote::RaftLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::Vote;
    use tempfile::TempDir;

    fn make_entry(index: u64, term: u64) -> EntryOf<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    fn make_data_entry(index: u64, term: u64, data: Vec<u8>) -> EntryOf<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(data.into()),
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
        assert_eq!(RaftLogReader::read_vote(&mut s).await.unwrap(), None);

        let vote = Vote::new(1, 0);
        s.save_vote(&vote).await.unwrap();

        assert_eq!(RaftLogReader::read_vote(&mut s).await.unwrap(), Some(vote));
    }

    // ── 3: truncate_after keeps boundary log id ─────────────────────────────

    #[tokio::test]
    async fn test_rocks_truncate_after() {
        let (mut s, _tmp) = open_storage();
        let entries = vec![
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 1),
            make_entry(4, 1),
            make_entry(5, 1),
        ];
        s.blocking_append(entries).await.unwrap();

        // truncate_after(Some(3)) keeps 3 and removes 4,5.
        s.truncate_after(Some(LogId::new(CommittedLeaderId::new(1, 0), 3)))
            .await
            .unwrap();

        let remaining = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining[0].log_id.index, 1);
        assert_eq!(remaining[1].log_id.index, 2);
        assert_eq!(remaining[2].log_id.index, 3);
    }

    // ── 4: truncate_after_from_middle ───────────────────────────────────────

    #[tokio::test]
    async fn test_rocks_truncate_after_from_middle() {
        let (mut s, _tmp) = open_storage();
        let entries = (1..=10).map(|i| make_entry(i, 1)).collect::<Vec<_>>();
        s.blocking_append(entries).await.unwrap();

        s.truncate_after(Some(LogId::new(CommittedLeaderId::new(1, 0), 6)))
            .await
            .unwrap();

        let remaining = s.try_get_log_entries(0..20).await.unwrap();
        assert_eq!(remaining.len(), 6);
        assert_eq!(remaining.last().unwrap().log_id.index, 6);
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

    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[tokio::test]
    async fn test_rocks_vote_persistence() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        {
            let mut s = RocksLogStorage::open(&path).unwrap();
            let vote = Vote::new(3, 1);
            s.save_vote(&vote).await.unwrap();
        }
        // Yield to let the batcher background task drop its Arc<RocksStorage>
        // before we try to reopen the same path.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Reopen — vote must survive
        let mut s2 = RocksLogStorage::open(&path).unwrap();
        let recovered = RaftLogReader::read_vote(&mut s2).await.unwrap();
        assert_eq!(recovered, Some(Vote::new(3, 1)));
    }

    // ── 10: log entries persist across reopen ──────────────────────────────

    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[tokio::test]
    async fn test_rocks_log_persistence() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        {
            let mut s = RocksLogStorage::open(&path).unwrap();
            let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)];
            s.blocking_append(entries).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

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

    // ── IoBatcher-specific tests ────────────────────────────────────────────

    #[tokio::test]
    async fn test_async_append_entries_are_readable() {
        let (mut s, _tmp) = open_storage();
        let entries = (1..=10).map(|i| make_entry(i, 1)).collect::<Vec<_>>();
        s.blocking_append(entries).await.unwrap();

        let got = s.try_get_log_entries(1..11).await.unwrap();
        assert_eq!(got.len(), 10);
        for (i, e) in got.iter().enumerate() {
            assert_eq!(e.log_id.index, (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn test_concurrent_appends_all_land() {
        use crate::io_batcher::IoBatcherConfig;
        use std::time::Duration;

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        let mut s = RocksLogStorage::open_with_config(
            path,
            IoBatcherConfig::new(Duration::from_millis(5), 512),
        )
        .unwrap();

        for i in 1u64..=50 {
            s.blocking_append(vec![make_data_entry(i, 1, vec![i as u8])])
                .await
                .unwrap();
        }

        let got = s.try_get_log_entries(1..51).await.unwrap();
        assert_eq!(got.len(), 50);
        assert_eq!(got[49].log_id.index, 50);
    }

    #[tokio::test]
    async fn test_batch_window_config_zero_works() {
        use crate::io_batcher::IoBatcherConfig;

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        let mut s =
            RocksLogStorage::open_with_config(path, IoBatcherConfig::no_batching()).unwrap();

        s.blocking_append(vec![make_entry(1, 1), make_entry(2, 1)])
            .await
            .unwrap();

        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn test_append_empty_entries_does_not_hang() {
        let (mut s, _tmp) = open_storage();
        // Empty iterator: callback must fire immediately without waiting on batcher.
        s.blocking_append(vec![]).await.unwrap();
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, None);
    }

    #[tokio::test]
    async fn test_truncate_after_async_append() {
        let (mut s, _tmp) = open_storage();
        s.blocking_append((1..=5).map(|i| make_entry(i, 1)).collect::<Vec<_>>())
            .await
            .unwrap();

        s.truncate_after(Some(LogId::new(CommittedLeaderId::new(1, 0), 3)))
            .await
            .unwrap();

        let remaining = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining.last().unwrap().log_id.index, 3);
    }

    // ── T3: append callbacks survive close + reopen (ADR A-0011 C3) ──────

    /// Build an `IOFlushed::signal` callback + its tokio receiver, the way
    /// openraft constructs it internally.
    fn flushed_signal() -> (
        IOFlushed<RrTypeConfig>,
        tokio::sync::oneshot::Receiver<Result<(), io::Error>>,
    ) {
        use openraft::type_config::async_runtime::{AsyncRuntime, Oneshot};
        let (tx, rx) =
            <<RrTypeConfig as RaftTypeConfig>::AsyncRuntime as AsyncRuntime>::Oneshot::channel();
        (IOFlushed::signal(tx), rx)
    }

    #[tokio::test]
    async fn test_rocks_append_close_reopen() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        {
            let mut s = RocksLogStorage::open(&path).unwrap();
            let mut waiters = Vec::new();
            for i in 1..=50u64 {
                let (callback, rx) = flushed_signal();
                s.append(vec![make_entry(i, 1)], callback).await.unwrap();
                waiters.push(rx);
            }

            // Graceful close: final write_batch (fsync) + every pending
            // callback answered exactly once.
            s.close().await.expect("close must succeed");

            for rx in waiters {
                let result = rx.await.expect("callback channel must not be dropped");
                assert!(result.is_ok(), "callback must report Ok after close");
            }
            // The batcher worker has exited, so reopening needs no settle
            // sleep (contrast: drop-drain tests above).
        }

        // Reopen and verify all 50 entries survived.
        let mut s2 = RocksLogStorage::open(&path).unwrap();
        let got = s2.try_get_log_entries(1..51).await.unwrap();
        assert_eq!(got.len(), 50, "all appended entries must survive close");
        assert_eq!(got[0].log_id.index, 1);
        assert_eq!(got[49].log_id.index, 50);
    }

    // ── close() rejects new appends (fail-fast after closing begins) ──────

    #[tokio::test]
    async fn test_rocks_append_after_close_fails() {
        let (mut s, _tmp) = open_storage();
        s.blocking_append(vec![make_entry(1, 1)]).await.unwrap();
        s.close().await.unwrap();

        let (callback, _rx) = flushed_signal();
        let result = s.append(vec![make_entry(2, 1)], callback).await;
        assert!(result.is_err(), "append after close must fail fast");

        // close() is idempotent.
        s.close().await.unwrap();
    }

    // ── ADR A-0015: "submitted ⇒ visible" contract (R-4 finding) ────────
    //
    // openraft gates apply/replication on log-IO submission, not flush, so
    // reads racing an unflushed append must still observe it. The R-4 crash
    // harness caught the pre-fix behavior: the sm worker's debug assert
    // (`entries_stream` short read) killed the apply loop on live nodes.

    /// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[tokio::test]
    async fn test_append_visible_before_flush_via_overlay() {
        let tmp = TempDir::new().unwrap();
        // 5s window: without the overlay, the appended entry would be
        // invisible until the batcher flushed.
        let mut s = RocksLogStorage::open_with_config(
            tmp.path().to_str().unwrap(),
            IoBatcherConfig::new(std::time::Duration::from_secs(5), 256),
        )
        .unwrap();

        let (callback, rx) = flushed_signal();
        s.append(vec![make_entry(1, 1), make_entry(2, 1)], callback)
            .await
            .unwrap();

        // Sanity: the batcher has NOT flushed yet (direct engine read).
        let direct = s
            .storage
            .scan(CF_LOG, &index_key(0), &index_key(u64::MAX))
            .await
            .unwrap();
        assert!(
            direct.is_empty(),
            "long-window batch must still be queued, not flushed"
        );

        // The openraft read path must observe the submitted entries NOW.
        let got = s.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(
            got.len(),
            2,
            "submitted-but-unflushed entries must be readable (ADR A-0015)"
        );
        assert_eq!(got[1].log_id.index, 2);

        // get_log_state reports the submitted tail as well.
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 2);

        // Flush lands the entries; the overlay is pruned by the hook.
        rx.await.unwrap().expect("flush ok");
        let got = s.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(got.len(), 2, "post-flush reads stay correct");
        assert!(
            s.pending.lock().unwrap().is_empty(),
            "post-flush hook must prune the overlay"
        );
        s.close().await.unwrap();
    }

    /// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[tokio::test]
    async fn test_truncate_after_does_not_resurrect_pending_appends() {
        let tmp = TempDir::new().unwrap();
        let mut s = RocksLogStorage::open_with_config(
            tmp.path().to_str().unwrap(),
            IoBatcherConfig::new(std::time::Duration::from_secs(5), 256),
        )
        .unwrap();

        // Queue 5 entries (unflushed), then truncate to 3.
        let (callback, rx) = flushed_signal();
        s.append(
            (1..=5).map(|i| make_entry(i, 1)).collect::<Vec<_>>(),
            callback,
        )
        .await
        .unwrap();
        s.truncate_after(Some(LogId::new(CommittedLeaderId::new(1, 0), 3)))
            .await
            .unwrap();

        // The barrier inside truncate_after flushed the queue first; the
        // delete then removed 4,5 — a later flush must NOT resurrect them.
        rx.await.unwrap().expect("flush ok");
        let remaining = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(
            remaining.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "truncated entries must stay deleted after the queued flush lands"
        );
        assert!(s.pending.lock().unwrap().is_empty());
        s.close().await.unwrap();
    }

    /// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[tokio::test]
    async fn test_purge_does_not_resurrect_pending_appends() {
        let tmp = TempDir::new().unwrap();
        let mut s = RocksLogStorage::open_with_config(
            tmp.path().to_str().unwrap(),
            IoBatcherConfig::new(std::time::Duration::from_secs(5), 256),
        )
        .unwrap();

        let (callback, rx) = flushed_signal();
        s.append(
            (1..=5).map(|i| make_entry(i, 1)).collect::<Vec<_>>(),
            callback,
        )
        .await
        .unwrap();
        s.purge(LogId::new(CommittedLeaderId::new(1, 0), 2))
            .await
            .unwrap();

        rx.await.unwrap().expect("flush ok");
        let remaining = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(
            remaining.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![3, 4, 5],
            "purged entries must stay deleted after the queued flush lands"
        );
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);
        s.close().await.unwrap();
    }

    // ── ADR A-0015 stress: pipelined appends + concurrent reads never gap ──
    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    //
    // Reproduction shape of the R-4 crash-harness finding: openraft asserts
    // (debug) that a read of [k, ..) never returns a suffix missing k.
    // Sequential pipelined appends (leader write path) + a reader probing
    // random ranges inside the appended frontier: any non-empty result must
    // start exactly at the probed index and be internally gap-free.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_pipelined_appends_concurrent_reads_never_gap() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let tmp = TempDir::new().unwrap();
        let storage = RocksLogStorage::open_with_config(
            tmp.path().to_str().unwrap(),
            IoBatcherConfig::new(std::time::Duration::from_millis(2), 64),
        )
        .unwrap();

        let frontier = Arc::new(AtomicU64::new(0));
        let reader_frontier = Arc::clone(&frontier);
        let mut reader = storage.clone();
        let reader_task = tokio::spawn(async move {
            let mut violations = Vec::new();
            let mut probes = 0u64;
            while probes < 40_000 {
                let f = reader_frontier.load(Ordering::Acquire);
                if f < 2 {
                    tokio::task::yield_now().await;
                    continue;
                }
                probes += 1;
                // Bias hard to the unsubmitted/unflushed TIP: openraft's
                // replication reads race exactly there (watch channel fires
                // before the append call stages the overlay).
                let k = match probes % 4 {
                    0 => f,                      // beyond/at the tip
                    1 => f - 1,                  // at the tip
                    2 => 1 + (probes % (f - 1)), // uniform inside frontier
                    _ => f.saturating_sub(2).max(1),
                };
                let got = reader
                    .try_get_log_entries(k..k + 3)
                    .await
                    .expect("read must not error");
                let Some(first) = got.first() else {
                    if k <= f {
                        violations.push(format!(
                            "range [{k},{}) empty though {k} was submitted",
                            k + 3
                        ));
                    }
                    continue;
                };
                if first.log_id.index != k {
                    violations.push(format!(
                        "range [{k},{}) starts at {} — gap at the head",
                        k + 3,
                        first.log_id.index
                    ));
                }
                for w in got.windows(2) {
                    if w[1].log_id.index != w[0].log_id.index + 1 {
                        violations.push(format!(
                            "interior gap: {} then {}",
                            w[0].log_id.index, w[1].log_id.index
                        ));
                    }
                }
            }
            violations
        });

        let mut writer = storage.clone();
        const N: u64 = 2_000;
        let mut rxs = Vec::with_capacity(N as usize);
        for i in 1..=N {
            let (callback, rx) = flushed_signal();
            writer
                .append(vec![make_entry(i, 1)], callback)
                .await
                .expect("append accepted");
            frontier.store(i, Ordering::Release);
            rxs.push(rx);
        }
        for rx in rxs {
            rx.await.expect("callback channel").expect("flush ok");
        }

        let violations = reader_task.await.expect("reader task");
        assert!(
            violations.is_empty(),
            "read-after-submit violations ({}): {:?}",
            violations.len(),
            &violations[..violations.len().min(5)]
        );
        storage.close().await.unwrap();
    }
}
