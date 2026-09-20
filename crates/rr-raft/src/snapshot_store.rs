//! Durable, atomic on-disk store for a Raft state-machine snapshot.
//!
//! # Why this exists (T1 / R-P0)
//!
//! `RrStateMachine` keeps the application state in memory (a *transient* state
//! machine in OpenRaft's taxonomy). The Raft log is durable (RocksDB), but
//! once OpenRaft takes a snapshot it purges the covered log prefix — so a
//! snapshot that lives only in memory is a data-loss trap: after
//! snapshot → purge → restart, the pre-snapshot state is gone and the node
//! silently rebuilds an empty one (see `docs/architecture/durability.md`).
//!
//! The fix is the standard OpenRaft "transient SM + persistent snapshot"
//! pattern (`raft_state_machine.rs` trait docs): every snapshot is persisted
//! to disk, and on startup `get_current_snapshot()` returns the persisted
//! bytes + meta so OpenRaft's `StorageHelper::restore_from_snapshot`
//! re-installs it and replays the un-purged log tail on top.
//!
//! # Layout & durability
//!
//! One live snapshot per Raft group, stored as a single bincode file next to
//! the group's RocksDB directory (following the `sm-membership.bin` sidecar
//! precedent). Writes are atomic: the payload is written to a `*.tmp` sibling,
//! `fsync`ed, then `rename`d over the live file and the parent directory is
//! `fsync`ed. A crash mid-write therefore leaves either the old or the new
//! snapshot — never a torn one — and a leftover `*.tmp` is ignored on load.

use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::SnapshotMetaOf;
use openraft::type_config::alias::StoredMembershipOf;

use crate::type_config::RrTypeConfig;

/// The persisted unit: the snapshot payload plus the two pieces of meta
/// OpenRaft needs to resume from it (`SnapshotMeta` itself is
/// deserialize-only, so its fields are stored separately and reassembled).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistentSnapshot {
    /// `SnapshotMeta::last_log_id` — the last log entry included in `data`.
    pub last_log_id: Option<LogIdOf<RrTypeConfig>>,
    /// `SnapshotMeta::last_membership` — membership as of `last_log_id`.
    pub last_membership: StoredMembershipOf<RrTypeConfig>,
    /// The state-machine snapshot bytes emitted by `StateMachine::snapshot()`.
    pub data: Vec<u8>,
}

impl PersistentSnapshot {
    /// Reassemble the OpenRaft [`SnapshotMetaOf`] for this persisted snapshot.
    pub(crate) fn meta(&self) -> SnapshotMetaOf<RrTypeConfig> {
        SnapshotMetaOf::<RrTypeConfig> {
            last_log_id: self.last_log_id,
            last_membership: self.last_membership.clone(),
        }
    }

    /// The `last_log_id.index` this snapshot covers, or `0` for the empty
    /// snapshot. Used by the monotonic guard that refuses to overwrite a
    /// newer persisted snapshot with an older one.
    fn covered_index(&self) -> u64 {
        self.last_log_id.map(|l| l.index).unwrap_or(0)
    }
}

/// Atomic file-backed store holding a group's single live snapshot.
///
/// Clone is cheap (a `PathBuf`); clones share the same on-disk file. The
/// store performs no in-process caching — `save` writes through and `load`
/// reads from disk — because OpenRaft drives both at snapshot/build/install
/// boundaries, not on the hot apply path.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    /// Final path of the live snapshot file, e.g. `{group_dir}/sm-snapshot.bin`.
    path: PathBuf,
}

impl SnapshotStore {
    /// A store whose live snapshot file is `path`.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The live snapshot file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The transient sibling a save writes before the atomic rename.
    fn tmp_path(&self) -> PathBuf {
        let mut tmp = self.path.clone().into_os_string();
        tmp.push(".tmp");
        PathBuf::from(tmp)
    }

    /// Load the persisted snapshot, or `Ok(None)` if none exists yet.
    ///
    /// # Failure policy — fail closed
    ///
    /// A present-but-undecodable snapshot is a genuine emergency (the log
    /// prefix it covers has been purged, so this file is the only copy of
    /// that state). Returning `None` here would silently restart the state
    /// machine empty — exactly the data-loss wedge this store exists to
    /// close — so a read/decode failure surfaces as an `Err` and fails node
    /// startup loudly instead. The recovery runbook is to inspect/delete the
    /// corrupt file and let the node rebuild from the leader.
    pub(crate) fn load(&self) -> Result<Option<PersistentSnapshot>, io::Error> {
        let bytes = match std::fs::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!(
                        "snapshot store: failed to read {}: {e}",
                        self.path.display()
                    ),
                ))
            }
        };
        let (snap, _): (PersistentSnapshot, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).map_err(
                |e| {
                    io::Error::other(format!(
                        "snapshot store: failed to decode {} ({} bytes): {e}",
                        self.path.display(),
                        bytes.len()
                    ))
                },
            )?;
        Ok(Some(snap))
    }

    /// Atomically persist `snap` as the group's live snapshot.
    ///
    /// `write tmp → fsync file → rename → fsync dir`, so the live file is
    /// never torn. Under the Strict durability contract this makes the
    /// snapshot as durable as the Raft log before OpenRaft purges the covered
    /// prefix (which only happens *after* a successful build/install).
    pub(crate) fn save(&self, snap: &PersistentSnapshot) -> Result<(), io::Error> {
        let bytes = bincode::serde::encode_to_vec(snap, bincode::config::standard())
            .map_err(|e| io::Error::other(format!("snapshot store: encode failed: {e}")))?;

        let tmp = self.tmp_path();
        {
            let mut f = std::fs::File::create(&tmp).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("snapshot store: create {} failed: {e}", tmp.display()),
                )
            })?;
            f.write_all(&bytes)
                .and_then(|()| f.sync_all())
                .map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!("snapshot store: write/fsync {} failed: {e}", tmp.display()),
                    )
                })?;
        }
        std::fs::rename(&tmp, &self.path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "snapshot store: rename {} → {} failed: {e}",
                    tmp.display(),
                    self.path.display()
                ),
            )
        })?;
        // Durable-rename: fsync the parent directory so the directory entry
        // itself survives a power loss (kill -9 alone never needs this, but
        // Strict promises power-loss durability).
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    /// Persist `snap` only if it covers at least as much log as the currently
    /// persisted snapshot. Snapshots are produced monotonically in normal
    /// operation, so this guard is a cheap tripwire against a stale overwrite
    /// (e.g. an unexpected startup rebuild racing a real snapshot). Returns
    /// `Ok(true)` if written, `Ok(false)` if the existing snapshot is newer.
    pub(crate) fn save_if_newer(&self, snap: &PersistentSnapshot) -> Result<bool, io::Error> {
        if let Some(existing) = self.load()? {
            if existing.covered_index() > snap.covered_index() {
                tracing::warn!(
                    existing = existing.covered_index(),
                    new = snap.covered_index(),
                    "snapshot store: refusing to overwrite a newer persisted snapshot \
                     with an older one"
                );
                return Ok(false);
            }
        }
        self.save(snap)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::vote::leader_id_adv::CommittedLeaderId;
    use openraft::vote::RaftLeaderId;
    use openraft::{LogId, Membership, StoredMembership};
    use std::collections::BTreeSet;

    fn make_meta(
        index: u64,
    ) -> (
        Option<LogIdOf<RrTypeConfig>>,
        StoredMembershipOf<RrTypeConfig>,
    ) {
        let log_id = LogId::new(CommittedLeaderId::new(1, 1), index);
        let membership = StoredMembership::new(
            Some(log_id),
            Membership::new_with_defaults(vec![BTreeSet::from([1u64, 2, 3])], vec![]),
        );
        (Some(log_id), membership)
    }

    #[test]
    fn save_then_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path().join("sm-snapshot.bin"));

        assert!(store.load().unwrap().is_none(), "no snapshot initially");

        let (last_log_id, last_membership) = make_meta(42);
        let snap = PersistentSnapshot {
            last_log_id,
            last_membership,
            data: b"snapshot-bytes".to_vec(),
        };
        store.save(&snap).unwrap();

        let loaded = store.load().unwrap().expect("snapshot must load");
        assert_eq!(loaded.data, b"snapshot-bytes");
        assert_eq!(loaded.last_log_id.unwrap().index, 42);
        assert_eq!(loaded.last_membership.membership().nodes().count(), 3);
        // Reassembled meta matches.
        assert_eq!(loaded.meta().last_log_id.unwrap().index, 42);
    }

    #[test]
    fn save_overwrites_atomically_and_cleans_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sm-snapshot.bin");
        let store = SnapshotStore::new(path.clone());

        for (idx, tag) in [(1u64, b"first".as_slice()), (2, b"second"), (3, b"third")] {
            let (last_log_id, last_membership) = make_meta(idx);
            store
                .save(&PersistentSnapshot {
                    last_log_id,
                    last_membership,
                    data: tag.to_vec(),
                })
                .unwrap();
        }
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.data, b"third");
        assert_eq!(loaded.last_log_id.unwrap().index, 3);
        // The transient sibling must not linger after a clean save.
        assert!(!store.tmp_path().exists());
    }

    #[test]
    fn save_if_newer_refuses_older_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path().join("sm-snapshot.bin"));

        let (id5, m5) = make_meta(5);
        store
            .save(&PersistentSnapshot {
                last_log_id: id5,
                last_membership: m5,
                data: b"newer".to_vec(),
            })
            .unwrap();

        let (id3, m3) = make_meta(3);
        let wrote = store
            .save_if_newer(&PersistentSnapshot {
                last_log_id: id3,
                last_membership: m3,
                data: b"older".to_vec(),
            })
            .unwrap();
        assert!(!wrote, "older snapshot must not overwrite newer");
        assert_eq!(store.load().unwrap().unwrap().data, b"newer");

        // Equal index is allowed (idempotent re-persist of the same point).
        let (id5b, m5b) = make_meta(5);
        let wrote = store
            .save_if_newer(&PersistentSnapshot {
                last_log_id: id5b,
                last_membership: m5b,
                data: b"same-point".to_vec(),
            })
            .unwrap();
        assert!(wrote);
    }

    #[test]
    fn load_corrupt_snapshot_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sm-snapshot.bin");
        std::fs::write(&path, b"not-valid-bincode\x00\x01").unwrap();

        let store = SnapshotStore::new(path);
        let err = store.load().unwrap_err();
        // Fail-closed: corruption surfaces as an error, never as "no snapshot"
        // (which would silently restart the state machine empty).
        assert!(
            err.to_string().contains("decode"),
            "expected a decode error, got: {err}"
        );
    }

    #[test]
    fn leftover_tmp_is_ignored_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sm-snapshot.bin");
        let store = SnapshotStore::new(path.clone());

        // Simulate a crash mid-save: a torn tmp file with garbage, no live file.
        std::fs::write(store.tmp_path(), b"torn-partial-write").unwrap();

        assert!(
            store.load().unwrap().is_none(),
            "a torn tmp must not be mistaken for a snapshot"
        );
        assert!(store.tmp_path().exists(), "tmp is left for inspection");
    }
}
