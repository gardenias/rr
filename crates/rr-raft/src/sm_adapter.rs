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
