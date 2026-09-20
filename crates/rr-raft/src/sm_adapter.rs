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

use rr_core::deferred::DeferredApply;
use rr_core::state_machine::StateMachine;
use rr_core::types::ApplyContext;

use crate::snapshot_store::PersistentSnapshot;
use crate::snapshot_store::SnapshotStore;
use crate::type_config::RrTypeConfig;

/// Bridges openraft's `RaftStateMachine` to the user-defined `StateMachine` trait.
pub struct RrStateMachine {
    inner: Box<dyn StateMachine>,
    last_applied: Option<LogIdOf<RrTypeConfig>>,
    last_membership: StoredMembershipOf<RrTypeConfig>,
    /// Cached snapshot bytes from the inner state machine.
    snapshot_bytes: Vec<u8>,
    /// Meta of the snapshot currently held in `snapshot_bytes` — the durable
    /// recovery anchor. `Some` once a snapshot has been built or installed
    /// (and persisted, when a [`SnapshotStore`] is attached). Tracked
    /// separately from `last_applied` because the snapshot legitimately lags
    /// the applied frontier between snapshot triggers.
    snapshot_meta: Option<SnapshotMetaOf<RrTypeConfig>>,
    /// Counts installed snapshots (openraft's `SnapshotMeta` no longer
    /// carries a `snapshot_id`; kept so tests can observe installs).
    snapshot_idx: u64,
    /// Durable sidecar (bincode-encoded `StoredMembership`) preserving the
    /// committed membership across restarts; see `with_membership_sidecar`.
    membership_sidecar: Option<std::path::PathBuf>,
    /// Durable snapshot store (T1 / R-P0). When attached (the durable-log /
    /// rocks path), every built or installed snapshot is persisted so a
    /// restart recovers the snapshotted state instead of silently rebuilding
    /// an empty state machine after the log prefix is purged.
    snapshot_store: Option<SnapshotStore>,
    /// R-2 / A-0033 deferred response: client responders parked by a
    /// `DeferredApply::Pending` apply, keyed by `correlation_id`, completed by
    /// a later `DeferredApply::Complete`. In-memory only (a parked responder
    /// is a live client wait); dropped on stop/restart, which errors the held
    /// `client_write` so the client retries — it is never snapshotted.
    pending_deferred: std::collections::HashMap<
        rr_core::deferred::CorrelationId,
        openraft::storage::ApplyResponder<RrTypeConfig>,
    >,
}

impl RrStateMachine {
    /// Create a new `RrStateMachine` wrapping the given `StateMachine`.
    pub fn new(inner: Box<dyn StateMachine>) -> Self {
        Self {
            inner,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_bytes: Vec::new(),
            snapshot_meta: None,
            snapshot_idx: 0,
            membership_sidecar: None,
            snapshot_store: None,
            pending_deferred: std::collections::HashMap::new(),
        }
    }

    /// Attach a durable membership sidecar, loading it if present.
    ///
    /// Why this exists: with a durable (purged) Raft log but non-persisted SM
    /// snapshots — the A-0016 shape — a restart replays only the un-purged
    /// tail, so the membership entry written once at initialize is gone. Both
    /// this adapter and openraft's startup fallback (`sm_mem`) then rebuild an
    /// EMPTY membership: zero voters, no elections, lease dead, and every
    /// `client_write` is rejected with `ForwardToLeader::empty()` while
    /// metrics still report the persisted vote's stale leadership (perf-lab
    /// W7 reproduced this 2/2). Only committed memberships are written
    /// (apply/install paths), never last_applied — replay positioning must
    /// keep coming from openraft, since the inner SM's data is not durable.
    pub fn with_membership_sidecar(mut self, path: std::path::PathBuf) -> Self {
        match std::fs::read(&path) {
            Ok(bytes) => {
                match bincode::serde::decode_from_slice(&bytes, bincode::config::standard()) {
                    Ok((membership, _)) => {
                        self.last_membership = membership;
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            path = %path.display(),
                            "membership sidecar undecodable, starting empty"
                        );
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    path = %path.display(),
                    "membership sidecar unreadable, starting empty"
                );
            }
        }
        self.membership_sidecar = Some(path);
        self
    }

    /// Best-effort sidecar write; a failure must never fail the raft apply
    /// path (membership changes are rare and the file is tiny).
    fn persist_membership(&self) {
        let Some(path) = &self.membership_sidecar else {
            return;
        };
        let bytes =
            match bincode::serde::encode_to_vec(&self.last_membership, bincode::config::standard())
            {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(error = %e, "membership sidecar encode failed");
                    return;
                }
            };
        if let Err(e) = std::fs::write(path, bytes) {
            tracing::warn!(error = %e, "membership sidecar write failed");
        }
    }

    /// Attach a durable snapshot store, loading any persisted snapshot.
    ///
    /// This is the T1 / R-P0 fix that makes snapshots survive a restart. With
    /// a transient (in-memory) inner state machine + a durable-but-purgeable
    /// Raft log, an un-persisted snapshot is a data-loss wedge: once OpenRaft
    /// purges the covered log prefix, a restart loses that state and silently
    /// rebuilds an empty SM. Attaching a store makes `build_snapshot` /
    /// `install_snapshot` persist every snapshot, and makes
    /// [`Self::get_current_snapshot`] return the persisted bytes + meta so
    /// OpenRaft's startup `restore_from_snapshot` re-installs them.
    ///
    /// On construction the persisted snapshot's bytes and meta are loaded into
    /// memory (so `get_current_snapshot` can hand them to OpenRaft), but
    /// `last_applied` is deliberately left `None`: the inner SM is still
    /// empty, and OpenRaft must drive the restore through `install_snapshot`
    /// so `applied_state()` never claims an applied point the inner SM has not
    /// actually reached.
    ///
    /// Fails (node startup aborts loudly) if a present snapshot cannot be
    /// read or decoded — see [`SnapshotStore::load`].
    pub fn with_snapshot_store(mut self, store: SnapshotStore) -> Result<Self, io::Error> {
        if let Some(persisted) = store.load()? {
            let meta = persisted.meta();
            self.snapshot_bytes = persisted.data;
            self.snapshot_meta = Some(meta);
        }
        self.snapshot_store = Some(store);
        Ok(self)
    }

    /// Persist the given snapshot bytes + meta through the attached store.
    ///
    /// No-op when no store is attached (in-memory path). Fail-closed when a
    /// store IS attached: a persist error is propagated so the caller
    /// (`build_snapshot` / `install_snapshot`) fails and OpenRaft does **not**
    /// purge the covered log prefix — the unpurged log remains the durable
    /// copy and the snapshot is retried later. Uses a monotonic guard so a
    /// stale snapshot never overwrites a newer persisted one.
    fn persist_snapshot(
        &self,
        last_applied: Option<LogIdOf<RrTypeConfig>>,
        last_membership: &StoredMembershipOf<RrTypeConfig>,
        data: &[u8],
    ) -> Result<(), io::Error> {
        let Some(store) = &self.snapshot_store else {
            return Ok(());
        };
        let snap = PersistentSnapshot {
            last_log_id: last_applied,
            last_membership: last_membership.clone(),
            data: data.to_vec(),
        };
        store.save_if_newer(&snap).map(|_written| ())
    }

    /// Fallible snapshot capture shared by both openraft entry points
    /// (`get_snapshot_builder` and `try_create_snapshot_builder`).
    ///
    /// ADR A-0011 C6: a snapshot is either complete bytes from a successful
    /// `inner.snapshot()` call or it does not exist — never "empty bytes +
    /// valid meta". Callers decide how to surface the failure.
    ///
    /// T1 / R-P0: when a [`SnapshotStore`] is attached, the snapshot is
    /// persisted here — *before* `build_snapshot()` returns and OpenRaft
    /// purges the covered log prefix. The persist is fail-closed: an error
    /// aborts the build, so the durable log is never purged without a durable
    /// snapshot behind it.
    async fn snapshot_builder_inner(&mut self) -> Result<RrSnapshotBuilder, io::Error> {
        let data = self
            .inner
            .snapshot()
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;

        // Persist first (no-op on the in-memory path). Only after this
        // succeeds is it safe for OpenRaft to purge the covered log prefix.
        self.persist_snapshot(self.last_applied, &self.last_membership, &data)?;

        self.snapshot_bytes = data.clone();
        self.snapshot_meta = Some(SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
        });
        Ok(RrSnapshotBuilder {
            data,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
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
    type SnapshotData = Cursor<Vec<u8>>;
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
            let (entry, mut responder) = item?;
            self.last_applied = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    if let Some(r) = responder.take() {
                        r.send(Vec::new().into());
                    }
                }
                EntryPayload::Membership(ref mem) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    self.persist_membership();
                    if let Some(r) = responder.take() {
                        r.send(Vec::new().into());
                    }
                }
                EntryPayload::Normal(ref data) => {
                    // R-1/R-2 / A-0033: thread the entry's real log id (R-1) and
                    // drive the two-phase (deferred) apply dispatch (R-2).
                    let ctx = ApplyContext::new(entry.log_id.index, entry.log_id.leader_id.term);
                    let effect = self
                        .inner
                        .apply_deferred(data.as_ref(), ctx)
                        .await
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    match effect {
                        DeferredApply::Immediate(bytes) => {
                            if let Some(r) = responder.take() {
                                r.send(bytes.into());
                            }
                        }
                        DeferredApply::Pending { correlation_id } => {
                            // R-2: park this entry's client responder under the
                            // correlation id; a later `Complete` delivers the
                            // response. On a follower (no responder) there is
                            // nothing to park.
                            if let Some(r) = responder.take() {
                                self.pending_deferred.insert(correlation_id, r);
                            }
                        }
                        DeferredApply::Complete {
                            completions,
                            response,
                        } => {
                            for completion in completions {
                                if let Some(r) =
                                    self.pending_deferred.remove(&completion.correlation_id)
                                {
                                    r.send(completion.response.into());
                                }
                            }
                            if let Some(r) = responder.take() {
                                r.send(response.into());
                            }
                        }
                    }
                }
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
                RrSnapshotBuilder::poisoned(e, self.last_applied, self.last_membership.clone())
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

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<RrTypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();

        // T1 / R-P0: persist the incoming snapshot BEFORE mutating any
        // in-memory state. OpenRaft purges this node's log up to
        // `meta.last_log_id` once the install succeeds; if the snapshot were
        // not durable first, a restart after that purge would lose the covered
        // state. Fail-closed: a persist error aborts the install, leaving the
        // SM untouched and the log retained so replication can retry.
        self.persist_snapshot(meta.last_log_id, &meta.last_membership, &data)?;

        self.inner
            .restore(&data)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.persist_membership();
        self.snapshot_bytes = data;
        self.snapshot_meta = Some(meta.clone());
        self.snapshot_idx += 1;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<RrTypeConfig, Self::SnapshotData>>, io::Error> {
        // T1 / R-P0: serve the persisted snapshot ONLY while the inner SM is
        // still behind it — i.e. the startup-recovery window before OpenRaft's
        // `restore_from_snapshot` re-installs it. That is what lets a restarted
        // node recover the persisted state instead of regenerating an empty one
        // from the (transient, now-empty) inner SM.
        //
        // Once the SM has caught up to the snapshot (`last_applied >=
        // snapshot.last_log_id`), fall through to a fresh capture so runtime
        // callers — follower catch-up and the ingestor-offset seeding path
        // (`fetch_snapshot_bytes`) — always observe the latest applied state,
        // not a stale snapshot taken at an earlier index.
        let persisted_is_ahead = match (&self.snapshot_meta, &self.last_applied) {
            (Some(meta), last_applied) => *last_applied < meta.last_log_id,
            (None, _) => false,
        };
        if persisted_is_ahead {
            let meta = self.snapshot_meta.clone().expect("checked above");
            return Ok(Some(Snapshot {
                meta,
                snapshot: Cursor::new(self.snapshot_bytes.clone()),
            }));
        }

        // Fresh capture from the live SM (original behavior).
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
    ) -> Self {
        Self {
            data: Vec::new(),
            last_applied,
            last_membership,
            build_error: Some(error),
        }
    }
}

impl RaftSnapshotBuilder<RrTypeConfig> for RrSnapshotBuilder {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<RrTypeConfig, Self::SnapshotData>, io::Error> {
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

    /// INV-5: a snapshot is the consistent state at one log index; install fully replaces state, never merges.
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
        assert_eq!(
            snap.meta.last_log_id,
            Some(LogId::new(CommittedLeaderId::new(1, 1), 2))
        );
    }

    /// INV-5: a snapshot is the consistent state at one log index; install fully replaces state, never merges.
    /// install_snapshot restores state machine from snapshot bytes.
    #[tokio::test]
    async fn test_install_snapshot_restores_state() {
        let mut sm = make_sm();

        // Create snapshot with known data
        let snap_data = b"restored-state".to_vec();
        let meta: SnapshotMetaOf<RrTypeConfig> = SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 10)),
            last_membership: StoredMembership::default(),
        };

        sm.install_snapshot(&meta, Cursor::new(snap_data.clone()))
            .await
            .unwrap();

        // Verify state was restored
        let current = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.snapshot.into_inner(), snap_data);
        assert_eq!(current.meta.last_log_id, meta.last_log_id);
        assert_eq!(sm.snapshot_idx, 1); // idx incremented to 1
    }

    /// INV-5: a snapshot is the consistent state at one log index; install fully replaces state, never merges.
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
        };
        sm.install_snapshot(&meta, Cursor::new(b"data1".to_vec()))
            .await
            .unwrap();
        assert_eq!(sm.snapshot_idx, 1);

        sm.install_snapshot(&meta, Cursor::new(b"data2".to_vec()))
            .await
            .unwrap();
        assert_eq!(sm.snapshot_idx, 2);
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

    // ── membership sidecar (A-0016 restart wedge fix) ────────────────────

    /// The committed membership must survive a simulated process restart
    /// (fresh adapter over the same sidecar path), because with a purged
    /// durable log it is the only place the membership survives.
    #[tokio::test]
    async fn test_membership_sidecar_survives_restart() {
        use openraft::Membership;

        let dir = tempfile::tempdir().unwrap();
        let sidecar = dir.path().join("sm-membership.bin");

        let membership = Membership::new_with_defaults(
            vec![std::collections::BTreeSet::from([1u64, 2, 3])],
            vec![],
        );
        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 0),
            payload: EntryPayload::Membership(membership.clone()),
        };

        let mut sm = RrStateMachine::new(Box::new(VecSM { state: vec![] }))
            .with_membership_sidecar(sidecar.clone());
        apply_entries(&mut sm, vec![entry]).await;
        assert!(
            sidecar.exists(),
            "membership apply must persist the sidecar"
        );
        drop(sm);

        // Simulated restart: fresh adapter, same path.
        let mut restarted =
            RrStateMachine::new(Box::new(VecSM { state: vec![] })).with_membership_sidecar(sidecar);
        let (last_applied, restored) = restarted.applied_state().await.unwrap();
        assert_eq!(
            restored.membership().nodes().count(),
            3,
            "membership must be restored from the sidecar after restart"
        );
        assert!(
            last_applied.is_none(),
            "last_applied must NOT be restored (inner SM data is not durable)"
        );
    }

    /// Without a sidecar file (first boot), construction starts empty.
    #[tokio::test]
    async fn test_membership_sidecar_first_boot_empty() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar = dir.path().join("nonexistent.bin");
        let mut sm = RrStateMachine::new(Box::new(VecSM { state: vec![] }))
            .with_membership_sidecar(sidecar.clone());
        let (_, membership) = sm.applied_state().await.unwrap();
        assert_eq!(membership.membership().nodes().count(), 0);
        assert!(
            !sidecar.exists(),
            "no sidecar may be created before a membership is applied"
        );
    }

    // ── T1 / R-P0: durable snapshot store ────────────────────────────────

    use crate::snapshot_store::SnapshotStore;

    fn make_sm_with_store(path: &std::path::Path) -> RrStateMachine {
        RrStateMachine::new(Box::new(VecSM { state: vec![] }))
            .with_snapshot_store(SnapshotStore::new(path.to_path_buf()))
            .unwrap()
    }

    /// T1 core: a snapshot built with a store attached is persisted to disk,
    /// and a simulated restart (fresh adapter over the same path) offers it
    /// back through `get_current_snapshot` so OpenRaft re-installs it. The
    /// inner SM must NOT claim an applied point before the install runs.
    #[tokio::test]
    async fn test_snapshot_persisted_and_offered_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("sm-snapshot.bin");

        // Node A: apply two entries, then build a snapshot.
        let mut a = make_sm_with_store(&snap_path);
        apply_entries(&mut a, vec![data_entry(1, b"foo".to_vec())]).await;
        apply_entries(&mut a, vec![data_entry(2, b"bar".to_vec())]).await;
        let mut builder = a.try_create_snapshot_builder(true).await.unwrap();
        let snap = builder.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id.unwrap().index, 2);
        assert!(snap_path.exists(), "snapshot must be persisted to disk");
        drop(a);

        // Node B (restart): fresh adapter over the same store. Before any
        // install, the persisted snapshot is offered with its true meta while
        // the (transient) inner SM still reports no applied state.
        let mut b = make_sm_with_store(&snap_path);
        let (applied_before, _) = b.applied_state().await.unwrap();
        assert!(
            applied_before.is_none(),
            "transient SM must not claim an applied point before install"
        );
        let current = b
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("persisted snapshot must be offered after restart");
        let meta = current.meta.clone();
        assert_eq!(meta.last_log_id.unwrap().index, 2);
        assert_eq!(current.snapshot.into_inner(), b"foobar");

        // OpenRaft's startup path installs it; applied_state then reflects it.
        b.install_snapshot(&meta, Cursor::new(b"foobar".to_vec()))
            .await
            .unwrap();
        let (applied_after, _) = b.applied_state().await.unwrap();
        assert_eq!(applied_after.unwrap().index, 2);
    }

    /// Once the SM has applied past the tracked snapshot, `get_current_snapshot`
    /// re-captures the live state — runtime callers (follower catch-up, ingestor
    /// offset seeding) must observe the latest applied state, not a stale
    /// snapshot. The persisted snapshot is only served while the SM lags it
    /// (the startup-recovery window, covered by
    /// `test_snapshot_persisted_and_offered_across_restart`).
    #[tokio::test]
    async fn test_get_current_snapshot_regenerates_when_sm_ahead() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = make_sm_with_store(&dir.path().join("sm-snapshot.bin"));

        apply_entries(&mut sm, vec![data_entry(1, b"abc".to_vec())]).await;
        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id.unwrap().index, 1);

        // Apply more AFTER the snapshot: the SM is now AHEAD of the snapshot,
        // so get_current_snapshot re-captures the live state (index 2, "abcdef").
        apply_entries(&mut sm, vec![data_entry(2, b"def".to_vec())]).await;
        let current = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta.last_log_id.unwrap().index, 2);
        assert_eq!(current.snapshot.into_inner(), b"abcdef");
    }

    /// A store-backed fresh node (no snapshot persisted yet) still reports no
    /// snapshot, preserving the empty-start contract.
    #[tokio::test]
    async fn test_store_backed_fresh_node_has_no_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = make_sm_with_store(&dir.path().join("sm-snapshot.bin"));
        assert!(sm.get_current_snapshot().await.unwrap().is_none());
    }

    // ── R-1 / A-0033: ApplyContext threading ─────────────────────────────

    use rr_core::types::ApplyContext;

    /// State machine that records the `ApplyContext` of each `apply_with_ctx`.
    struct CtxSM {
        seen: std::sync::Arc<std::sync::Mutex<Vec<ApplyContext>>>,
    }

    #[async_trait::async_trait]
    impl StateMachine for CtxSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }
        async fn apply_with_ctx(
            &mut self,
            entry: &[u8],
            ctx: ApplyContext,
        ) -> Result<Vec<u8>, RrError> {
            self.seen.lock().unwrap().push(ctx);
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }
        async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    /// R-1: `RrStateMachine::apply` must pass each entry's real Raft log id
    /// (index + term) to the state machine instead of discarding it.
    #[tokio::test]
    async fn test_apply_threads_real_log_id_to_state_machine() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut sm = RrStateMachine::new(Box::new(CtxSM {
            seen: std::sync::Arc::clone(&seen),
        }));

        // `data_entry` uses CommittedLeaderId term 1; apply indices 5 and 6.
        apply_entries(
            &mut sm,
            vec![data_entry(5, b"a".to_vec()), data_entry(6, b"b".to_vec())],
        )
        .await;

        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[ApplyContext::new(5, 1), ApplyContext::new(6, 1)],
            "each applied entry's log index + term must reach the state machine"
        );
    }
}
