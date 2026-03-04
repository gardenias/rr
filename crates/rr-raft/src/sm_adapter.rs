//! State machine adapter bridging openraft's `RaftStateMachine` to our `StateMachine` trait.

use std::io::Cursor;

use openraft::storage::RaftStateMachine;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;

use rr_core::state_machine::StateMachine;

use crate::type_config::RrTypeConfig;

/// Bridges openraft's `RaftStateMachine` to the user-defined `StateMachine` trait.
pub struct RrStateMachine {
    inner: Box<dyn StateMachine>,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    /// Cached snapshot bytes from the inner state machine.
    snapshot_bytes: Vec<u8>,
    /// Snapshot index counter for unique IDs.
    snapshot_idx: u64,
}

impl RrStateMachine {
    /// Create a new `RrStateMachine` wrapping the given `StateMachine`.
    pub fn new(inner: Box<dyn StateMachine>) -> Self {
        Self {
            inner,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_bytes: Vec::new(),
            snapshot_idx: 0,
        }
    }
}

impl RaftStateMachine<RrTypeConfig> for RrStateMachine {
    type SnapshotBuilder = RrSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, BasicNode>,
        ),
        StorageError<u64>,
    > {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Vec<u8>>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<RrTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut results = Vec::new();

        for entry in entries {
            self.last_applied = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    results.push(Vec::new());
                }
                EntryPayload::Normal(ref data) => {
                    let res = self.inner.apply(data).await.map_err(|e| {
                        StorageError::IO {
                            source: StorageIOError::write_state_machine(
                                openraft::AnyError::new(&e),
                            ),
                        }
                    })?;
                    results.push(res);
                }
                EntryPayload::Membership(ref mem) => {
                    self.last_membership =
                        StoredMembership::new(Some(entry.log_id), mem.clone());
                    results.push(Vec::new());
                }
            }
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let data = self.inner.snapshot().await.unwrap_or_default();
        self.snapshot_bytes = data.clone();
        RrSnapshotBuilder {
            data,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_idx: self.snapshot_idx,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        self.inner.restore(&data).await.map_err(|e| StorageError::IO {
            source: StorageIOError::write_state_machine(openraft::AnyError::new(&e)),
        })?;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.snapshot_bytes = data;
        self.snapshot_idx += 1;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RrTypeConfig>>, StorageError<u64>> {
        let data = self.inner.snapshot().await.map_err(|e| StorageError::IO {
            source: StorageIOError::read_state_machine(openraft::AnyError::new(&e)),
        })?;

        if data.is_empty() && self.last_applied.is_none() {
            return Ok(None);
        }

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id: format!("snap-{}", self.snapshot_idx),
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

/// Snapshot builder that captures a point-in-time snapshot.
pub struct RrSnapshotBuilder {
    data: Vec<u8>,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    snapshot_idx: u64,
}

impl RaftSnapshotBuilder<RrTypeConfig> for RrSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<RrTypeConfig>, StorageError<u64>> {
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id: format!("snap-{}", self.snapshot_idx),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(self.data.clone())),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use openraft::{Entry, EntryPayload, LogId};
    use openraft::SnapshotMeta;
    use openraft::StoredMembership;

    use rr_core::error::RrError;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;

    use super::{RrStateMachine};
    use crate::type_config::RrTypeConfig;
    use openraft::storage::RaftStateMachine;
    use openraft::RaftSnapshotBuilder;

    /// Test state machine that stores applied entries as concatenated bytes.
    /// `snapshot()` serializes the current state; `restore()` replaces it.
    struct VecSM {
        state: Vec<u8>,
    }

    #[async_trait::async_trait]
    impl StateMachine for VecSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            self.state.extend_from_slice(entry);
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(self.state.clone())
        }
        async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError> {
            self.state = snapshot.to_vec();
            Ok(())
        }
        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    fn make_sm() -> RrStateMachine {
        RrStateMachine::new(Box::new(VecSM { state: vec![] }))
    }


    fn data_entry(index: u64, data: Vec<u8>) -> Entry<RrTypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(data),
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    /// get_snapshot_builder + build_snapshot round-trip: snapshot data equals state.
    #[tokio::test]
    async fn test_build_snapshot_captures_state() {
        let mut sm = make_sm();

        // Apply some entries
        sm.apply(vec![data_entry(1, b"hello".to_vec())]).await.unwrap();
        sm.apply(vec![data_entry(2, b" world".to_vec())]).await.unwrap();

        // Build snapshot
        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        // Snapshot data equals accumulated state
        let data = snap.snapshot.into_inner();
        assert_eq!(data, b"hello world", "snapshot must contain accumulated state");
        assert_eq!(snap.meta.snapshot_id, "snap-0");
        assert_eq!(snap.meta.last_log_id, Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 2)));
    }

    /// begin_receiving_snapshot returns an empty cursor.
    #[tokio::test]
    async fn test_begin_receiving_snapshot_empty() {
        let mut sm = make_sm();
        let cursor = sm.begin_receiving_snapshot().await.unwrap();
        assert!(cursor.into_inner().is_empty(), "fresh snapshot buffer must be empty");
    }

    /// install_snapshot restores state machine from snapshot bytes.
    #[tokio::test]
    async fn test_install_snapshot_restores_state() {
        let mut sm = make_sm();

        // Create snapshot with known data
        let snap_data = b"restored-state".to_vec();
        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 10)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-42".to_string(),
        };

        sm.install_snapshot(&meta, Box::new(Cursor::new(snap_data.clone()))).await.unwrap();

        // Verify state was restored
        let current = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.snapshot.into_inner(), snap_data);
        assert_eq!(current.meta.last_log_id, meta.last_log_id);
        assert_eq!(current.meta.snapshot_id, "snap-1"); // idx incremented to 1
    }

    /// Full round-trip: apply entries, build snapshot, create fresh SM, install snapshot.
    #[tokio::test]
    async fn test_snapshot_round_trip() {
        // Source SM: apply data
        let mut source = make_sm();
        source.apply(vec![data_entry(1, b"foo".to_vec())]).await.unwrap();
        source.apply(vec![data_entry(2, b"bar".to_vec())]).await.unwrap();

        // Build snapshot from source
        let mut builder = source.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        let snap_meta = snap.meta.clone();
        let snap_data = snap.snapshot.into_inner();

        // Target SM: fresh, then install snapshot
        let mut target = make_sm();
        target
            .install_snapshot(&snap_meta, Box::new(Cursor::new(snap_data.clone())))
            .await
            .unwrap();

        // Target must now have the same state as source
        let target_snap = target.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(target_snap.snapshot.into_inner(), snap_data, "target state must match source");
        assert_eq!(target_snap.meta.last_log_id, snap_meta.last_log_id);
    }

    /// get_current_snapshot returns None when no data has been applied.
    #[tokio::test]
    async fn test_get_current_snapshot_empty() {
        let mut sm = make_sm();
        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_none(), "fresh SM has no snapshot");
    }

    /// Snapshot after install has incremented snapshot_idx.
    #[tokio::test]
    async fn test_snapshot_idx_increments_on_install() {
        let mut sm = make_sm();
        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 5)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-x".to_string(),
        };
        sm.install_snapshot(&meta, Box::new(Cursor::new(b"data1".to_vec())))
            .await
            .unwrap();
        let s1 = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(s1.meta.snapshot_id, "snap-1");

        sm.install_snapshot(&meta, Box::new(Cursor::new(b"data2".to_vec())))
            .await
            .unwrap();
        let s2 = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(s2.meta.snapshot_id, "snap-2");
    }
}
