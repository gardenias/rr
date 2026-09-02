//! State machine adapter bridging openraft's `RaftStateMachine` to our `StateMachine` trait.

use std::io;
use std::io::Cursor;

use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::SnapshotMetaOf;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::StoredMembershipOf;
use openraft::EntryPayload;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

use rr_core::state_machine::StateMachine;

use crate::type_config::RrTypeConfig;

/// Bridges openraft's `RaftStateMachine` to the user-defined `StateMachine` trait.
pub struct RrStateMachine {
    inner: Box<dyn StateMachine>,
    last_applied: Option<LogIdOf<RrTypeConfig>>,
    last_membership: StoredMembershipOf<RrTypeConfig>,
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

    /// Fallible snapshot capture shared by both openraft entry points
    /// (`get_snapshot_builder` and `try_create_snapshot_builder`).
    ///
    /// ADR A-0011 C6: a snapshot is either complete bytes from a successful
    /// `inner.snapshot()` call or it does not exist — never "empty bytes +
    /// valid meta". Callers decide how to surface the failure.
    async fn snapshot_builder_inner(&mut self) -> Result<RrSnapshotBuilder, io::Error> {
        let data = self
            .inner
            .snapshot()
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.snapshot_bytes = data.clone();
        Ok(RrSnapshotBuilder {
            data,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_idx: self.snapshot_idx,
            build_error: None,
        })
    }

    /// Common failure path: log + count, then hand the error to the caller.
    fn snapshot_build_failed(error: &io::Error) {
        tracing::error!(error = %error, "state machine snapshot capture failed");
        metrics::counter!("rr_snapshot_build_failures_total").increment(1);
    }
}

impl RaftStateMachine<RrTypeConfig> for RrStateMachine {
    type SnapshotBuilder = RrSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<RrTypeConfig>>,
            StoredMembershipOf<RrTypeConfig>,
        ),
        io::Error,
    > {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<RrTypeConfig>, io::Error>> + Send + Unpin,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            self.last_applied = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => Vec::new(),
                EntryPayload::Normal(ref data) => self
                    .inner
                    .apply(data.as_ref())
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))?,
                EntryPayload::Membership(ref mem) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    Vec::new()
                }
            };

            if let Some(responder) = responder {
                responder.send(response.into());
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Legacy entry point (signature has no `Result`). On failure return
        // a poisoned builder so the error surfaces via `build_snapshot()`
        // instead of producing an empty-but-valid-looking snapshot (C6).
        match self.snapshot_builder_inner().await {
            Ok(builder) => builder,
            Err(e) => {
                Self::snapshot_build_failed(&e);
                RrSnapshotBuilder::poisoned(
                    e,
                    self.last_applied,
                    self.last_membership.clone(),
                    self.snapshot_idx,
                )
            }
        }
    }

    async fn try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder> {
        match self.snapshot_builder_inner().await {
            Ok(builder) => Some(builder),
            Err(e) => {
                Self::snapshot_build_failed(&e);
                if force {
                    // Startup rebuild path (openraft `storage/helper.rs`
                    // unwraps the `Option`): return a poisoned builder so
                    // `Raft::new` fails with a diagnosable `StorageError`
                    // instead of panicking inside openraft on `None`.
                    Some(RrSnapshotBuilder::poisoned(
                        e,
                        self.last_applied,
                        self.last_membership.clone(),
                        self.snapshot_idx,
                    ))
                } else {
                    // Policy-triggered build: defer. openraft records
                    // `BuildSnapshotDone(None)` and retries on the next
                    // `SnapshotPolicy` trigger.
                    None
                }
            }
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<RrTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        self.inner
            .restore(&data)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.snapshot_bytes = data;
        self.snapshot_idx += 1;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<RrTypeConfig>>, io::Error> {
        let data = self
            .inner
            .snapshot()
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;

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
            snapshot: Cursor::new(data),
        }))
    }
}

/// Snapshot builder that captures a point-in-time snapshot.
pub struct RrSnapshotBuilder {
    data: Vec<u8>,
    last_applied: Option<LogIdOf<RrTypeConfig>>,
    last_membership: StoredMembershipOf<RrTypeConfig>,
    snapshot_idx: u64,
    /// Captured snapshot-capture error. When set, `build_snapshot` returns
    /// it instead of emitting an "empty bytes + valid meta" snapshot
    /// (ADR A-0011 C6 poisoned builder).
    build_error: Option<io::Error>,
}

impl RrSnapshotBuilder {
    /// A builder whose `build_snapshot` re-surfaces the capture error, so
    /// the failure propagates through openraft's `Result` channel as a
    /// `StorageError` instead of producing a corrupt snapshot.
    fn poisoned(
        error: io::Error,
        last_applied: Option<LogIdOf<RrTypeConfig>>,
        last_membership: StoredMembershipOf<RrTypeConfig>,
        snapshot_idx: u64,
    ) -> Self {
        Self {
            data: Vec::new(),
            last_applied,
            last_membership,
            snapshot_idx,
            build_error: Some(error),
        }
    }
}

impl RaftSnapshotBuilder<RrTypeConfig> for RrSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<RrTypeConfig>, io::Error> {
        // Borrow, don't take: a poisoned builder must keep failing on every
        // call. openraft calls `build_snapshot` exactly once per builder
        // today, but a `take()` would let a second call fall through and
        // emit exactly the "empty bytes + valid meta" snapshot C6 outlaws.
        if let Some(error) = &self.build_error {
            return Err(io::Error::other(error.to_string()));
        }
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id: format!("snap-{}", self.snapshot_idx),
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(self.data.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::io::Cursor;

    use openraft::storage::EntryResponder;
    use openraft::type_config::alias::EntryOf;
    use openraft::type_config::alias::SnapshotMetaOf;
    use openraft::vote::leader_id_adv::CommittedLeaderId;
    use openraft::vote::RaftLeaderId;
    use openraft::SnapshotMeta;
    use openraft::StoredMembership;
    use openraft::{Entry, EntryPayload, LogId};

    use rr_core::error::RrError;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;

    use super::RrStateMachine;
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

    fn data_entry(index: u64, data: Vec<u8>) -> EntryOf<RrTypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(data.into()),
        }
    }

    async fn apply_entries(sm: &mut RrStateMachine, entries: Vec<EntryOf<RrTypeConfig>>) {
        let items = entries
            .into_iter()
            .map(|entry| Ok::<EntryResponder<RrTypeConfig>, io::Error>((entry, None)));
        sm.apply(tokio_stream::iter(items)).await.unwrap();
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    /// get_snapshot_builder + build_snapshot round-trip: snapshot data equals state.
    #[tokio::test]
    async fn test_build_snapshot_captures_state() {
        let mut sm = make_sm();

        // Apply some entries
        apply_entries(&mut sm, vec![data_entry(1, b"hello".to_vec())]).await;
        apply_entries(&mut sm, vec![data_entry(2, b" world".to_vec())]).await;

        // Build snapshot
        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        // Snapshot data equals accumulated state
        let data = snap.snapshot.into_inner();
        assert_eq!(
            data, b"hello world",
            "snapshot must contain accumulated state"
        );
        assert_eq!(snap.meta.snapshot_id, "snap-0");
        assert_eq!(
            snap.meta.last_log_id,
            Some(LogId::new(CommittedLeaderId::new(1, 1), 2))
        );
    }

    /// begin_receiving_snapshot returns an empty cursor.
    #[tokio::test]
    async fn test_begin_receiving_snapshot_empty() {
        let mut sm = make_sm();
        let cursor = sm.begin_receiving_snapshot().await.unwrap();
        assert!(
            cursor.into_inner().is_empty(),
            "fresh snapshot buffer must be empty"
        );
    }

    /// install_snapshot restores state machine from snapshot bytes.
    #[tokio::test]
    async fn test_install_snapshot_restores_state() {
        let mut sm = make_sm();

        // Create snapshot with known data
        let snap_data = b"restored-state".to_vec();
        let meta: SnapshotMetaOf<RrTypeConfig> = SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 10)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-42".to_string(),
        };

        sm.install_snapshot(&meta, Cursor::new(snap_data.clone()))
            .await
            .unwrap();

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
        apply_entries(&mut source, vec![data_entry(1, b"foo".to_vec())]).await;
        apply_entries(&mut source, vec![data_entry(2, b"bar".to_vec())]).await;

        // Build snapshot from source
        let mut builder = source.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        let snap_meta = snap.meta.clone();
        let snap_data = snap.snapshot.into_inner();

        // Target SM: fresh, then install snapshot
        let mut target = make_sm();
        target
            .install_snapshot(&snap_meta, Cursor::new(snap_data.clone()))
            .await
            .unwrap();

        // Target must now have the same state as source
        let target_snap = target.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(
            target_snap.snapshot.into_inner(),
            snap_data,
            "target state must match source"
        );
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
        let meta: SnapshotMetaOf<RrTypeConfig> = SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 5)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-x".to_string(),
        };
        sm.install_snapshot(&meta, Cursor::new(b"data1".to_vec()))
            .await
            .unwrap();
        let s1 = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(s1.meta.snapshot_id, "snap-1");

        sm.install_snapshot(&meta, Cursor::new(b"data2".to_vec()))
            .await
            .unwrap();
        let s2 = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(s2.meta.snapshot_id, "snap-2");
    }

    // ── T5: snapshot failure propagation (ADR A-0011 C6) ─────────────────

    /// State machine whose `snapshot()` always fails — exercises the
    /// fail-closed snapshot paths.
    struct FailingSnapshotSM;

    #[async_trait::async_trait]
    impl StateMachine for FailingSnapshotSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Err(RrError::Application(
                "simulated snapshot failure".to_string(),
            ))
        }
        async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    fn make_failing_sm() -> RrStateMachine {
        RrStateMachine::new(Box::new(FailingSnapshotSM))
    }

    /// force=false (policy-triggered): failure must return `None` so
    /// openraft defers to the next SnapshotPolicy trigger.
    #[tokio::test]
    async fn test_snapshot_failure_defers_policy_build() {
        let mut sm = make_failing_sm();
        let builder = sm.try_create_snapshot_builder(false).await;
        assert!(
            builder.is_none(),
            "policy-triggered build must defer (None) on snapshot failure"
        );
    }

    /// force=true (startup rebuild): failure must return a poisoned builder
    /// whose `build_snapshot()` errors — never `None` (openraft unwraps it).
    #[tokio::test]
    async fn test_snapshot_failure_forced_build_errors() {
        let mut sm = make_failing_sm();
        let builder = sm.try_create_snapshot_builder(true).await;
        let mut builder = builder.expect("forced build must yield a poisoned builder, not None");
        let result = builder.build_snapshot().await;
        assert!(
            result.is_err(),
            "poisoned builder must surface the capture error"
        );
    }

    /// Legacy entry point: on failure `build_snapshot()` must return Err,
    /// never an empty-but-valid snapshot.
    #[tokio::test]
    async fn test_legacy_get_snapshot_builder_never_empty_on_error() {
        let mut sm = make_failing_sm();
        let mut builder = sm.get_snapshot_builder().await;
        let result = builder.build_snapshot().await;
        assert!(
            result.is_err(),
            "legacy builder must fail closed instead of emitting an empty snapshot"
        );
    }
}
