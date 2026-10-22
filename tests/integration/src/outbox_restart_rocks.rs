//! INV-4 outbox failover — rocks restart arm (design wave O-4 of
//! `.sisyphus/plans/rr-inv4-outbox-failover-e2e.md`).
//!
//! Cluster shape: a SINGLE-node group built directly on
//! `RaftGroupManager::new_with_rocks` (NoopNetwork, RocksDB log + persisted
//! SM snapshot under a tempfile dir) — no `RrNode`, no gRPC — running
//! `OutboxEchoService` with a `KafkaPublish` target. One `CapturingKafka` is
//! shared across BOTH manager lifecycles: the in-memory sink outlives the
//! restart, so re-deliveries land in the same record vec as first deliveries.
//!
//! Why this arm exists (design doc "关键机制事实", last bullet): in a healthy
//! in-process cluster the sent watermark can only reach a new leader via
//! InstallSnapshot or a disk restart — the failover arms (O-2/O-3) can never
//! pin "watermark survives snapshot recovery". Here N outputs are delivered
//! and snapshotted, M more arrive unsnapshotted, and the manager is rebuilt
//! on the same data_dir: the snapshot-restored watermark must suppress
//! re-delivery of the first N seqs, while the M post-snapshot pending outputs
//! are still redelivered at-least-once.
//!
//! Mechanism facts pinned here:
//! - The rocks raft path persists every built snapshot through
//!   `SnapshotStore` (`{rocks_path}/sm-snapshot.bin`) and re-installs it at
//!   startup (raft_node.rs), so the watermark genuinely crosses the restart.
//! - `trigger().snapshot()` schedules without waiting
//!   (vendor/openraft/src/raft/trigger.rs) — every trigger is paired with a
//!   `fetch_snapshot_bytes` poll until materialised, and the poll re-triggers
//!   because the runner advances the watermark just after the sink records
//!   the publish (a just-materialised snapshot can legitimately lag by one).
//! - `outbox_support::fetch_framed_snapshot` is `RrNode`-bound and this arm
//!   has no node, so the two-phase trigger+poll pattern lives locally here
//!   (lib.rs two-phase precedent).

// Gated `cfg(all(test, feature = "rocks"))` (not just `feature = "rocks"`) so
// the module vanishes entirely from non-test lib builds — otherwise its
// helpers/imports are dead code there and fail the workspace clippy gate
// (same pattern as raft_log_durability.rs).
#[cfg(all(test, feature = "rocks"))]
mod rocks_outbox_restart {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::Duration;

    use openraft::type_config::async_runtime::WatchReceiver;
    use openraft::BasicNode;
    use rr_core::framed_snapshot::FramedSnapshot;
    use rr_core::output::KafkaProducer;
    use rr_core::types::{GroupId, NodeId};
    use rr_raft::group_manager::RaftGroupManager;
    use tempfile::TempDir;

    use crate::outbox_support::{
        kafka_echo_target, outbox_echo_factory, poll_until, CapturingKafka, PublishedRecord,
    };

    const GROUP: GroupId = 1;
    const NODE: NodeId = 1;
    /// Writes delivered BEFORE the snapshot: their seqs (1..=N) must not be
    /// redelivered after the restart — the watermark crossed it.
    const N: usize = 4;
    /// Writes delivered AFTER the snapshot (never snapshotted): still pending
    /// at shutdown, so at-least-once redelivery after the restart is expected.
    const M: usize = 3;

    fn snapshotted_requests() -> Vec<String> {
        (0..N).map(|i| format!("inv4-snap-{i}")).collect()
    }

    fn post_snapshot_requests() -> Vec<String> {
        (0..M).map(|i| format!("inv4-post-{i}")).collect()
    }

    /// Expected idempotency key for a request payload: `"{request}:0"`.
    fn key(request: &str) -> String {
        format!("{request}:0")
    }

    /// Build a fresh single-node rocks-backed manager on `base`, start the
    /// group and initialize the single-node membership (idempotent — on the
    /// restart lifecycle the membership is already persisted in the log).
    async fn start_rocks_group(base: &str) -> Arc<RaftGroupManager> {
        let mgr = Arc::new(RaftGroupManager::new_with_rocks(
            NODE,
            outbox_echo_factory(kafka_echo_target()),
            base.to_string(),
        ));
        mgr.start_group_rocks(GROUP)
            .await
            .expect("start rocks group");
        let mut members = BTreeMap::new();
        members.insert(NODE, BasicNode::default());
        let _ = mgr
            .get_raft(GROUP)
            .await
            .expect("raft handle")
            .initialize(members)
            .await;
        mgr
    }

    /// Poll until the single-node group observes itself as leader (replaces
    /// the fixed 1500ms sleeps of the test_restart_persistence template —
    /// design rule: every wait is poll-based).
    async fn await_self_leadership(mgr: &RaftGroupManager, timeout: Duration) {
        poll_until(timeout, Duration::from_millis(50), || async {
            mgr.get_raft(GROUP)
                .await
                .filter(|raft| raft.metrics().borrow_watched().current_leader == Some(NODE))
                .map(|_| ())
        })
        .await
        .unwrap_or_else(|| panic!("node {NODE} must elect itself within {timeout:?}"));
    }

    /// Write `requests` through the raft handle: the first write rides the
    /// lease-ready retry (openraft fast-fails proposals until the fresh
    /// leader commits its blank entry), the rest go straight through.
    async fn write_requests(
        raft: &openraft::Raft<rr_raft::RrTypeConfig, rr_raft::sm_adapter::RrStateMachine>,
        requests: &[String],
    ) {
        crate::write_when_lease_ready(raft, requests[0].as_bytes(), 5_000)
            .await
            .expect("first write while lease establishes");
        for req in requests.iter().skip(1) {
            raft.client_write(req.clone().into_bytes().into())
                .await
                .expect("client_write on the established leader");
        }
    }

    /// Poll the sink until at least `n` records arrive (runner budget: 50ms
    /// idle poll + 100ms first backoff — a 10s timeout is generous).
    async fn await_sink_records(kafka: &Arc<CapturingKafka>, n: usize) -> Vec<PublishedRecord> {
        poll_until(Duration::from_secs(10), Duration::from_millis(50), || {
            let sink = Arc::clone(kafka);
            async move {
                let got = sink.records();
                (got.len() >= n).then_some(got)
            }
        })
        .await
        .unwrap_or_else(|| panic!("sink must record at least {n} deliveries within 10s"))
    }

    /// Trigger a snapshot and poll until one materialises carrying
    /// `sent_watermark == watermark`, returning the decoded framed envelope.
    ///
    /// The poll re-triggers every round: the sink records a publish just
    /// before the runner advances the watermark, so a just-materialised
    /// snapshot can legitimately lag by one — re-triggering produces a newer
    /// snapshot until one observes the watermark (O-1 pattern).
    async fn snapshot_with_watermark(mgr: &RaftGroupManager, watermark: u64) -> FramedSnapshot {
        poll_until(
            Duration::from_secs(10),
            Duration::from_millis(200),
            || async {
                let raft = mgr.get_raft(GROUP).await?;
                raft.trigger().snapshot().await.ok()?;
                let bytes = poll_until(
                    Duration::from_secs(2),
                    Duration::from_millis(100),
                    || async { mgr.fetch_snapshot_bytes(GROUP).await.ok().flatten() },
                )
                .await?;
                let framed = FramedSnapshot::decode(&bytes).ok().flatten()?;
                match framed.framework_state.outbox.as_ref() {
                    Some(outbox) if outbox.sent_watermark == watermark => Some(framed),
                    _ => None,
                }
            },
        )
        .await
        .unwrap_or_else(|| {
            panic!("snapshot with sent_watermark == {watermark} never materialised within 10s")
        })
    }

    /// INV-4: the sent watermark survives snapshot recovery — after a
    /// snapshot + disk restart, outputs at or below the restored watermark
    /// are NOT redelivered, post-snapshot pending outputs still are
    /// (at-least-once), and dedup by idempotency key loses nothing.
    ///
    /// rocks restart arm (wave O-4): N=4 writes are delivered and then
    /// snapshotted (watermark pinned inside the framed snapshot), M=3 more
    /// are delivered but deliberately never snapshotted, and the manager is
    /// gracefully shut down and rebuilt on the same data_dir. The restored
    /// watermark must hold back the first N seqs while the M unsnapshotted
    /// outputs redeliver — the "delivered-not-snapshotted" window the
    /// failover arms get from the mechanism for free, here constructed
    /// explicitly on disk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_inv4_outbox_watermark_survives_snapshot_restart() {
        let tmp = TempDir::new().expect("temp data dir");
        let base = tmp.path().to_str().expect("utf8 temp path").to_string();
        // The sink outlives the restart: same Arc across both lifecycles.
        let kafka = CapturingKafka::new().shared();

        let batch_one = snapshotted_requests();
        let batch_two = post_snapshot_requests();
        let keys_one: BTreeSet<String> = batch_one.iter().map(|r| key(r)).collect();
        let keys_two: BTreeSet<String> = batch_two.iter().map(|r| key(r)).collect();

        // ── Phase 1: write N, snapshot, write M (never snapshotted) ────────
        {
            let mgr = start_rocks_group(&base).await;
            mgr.attach_output_runner(
                GROUP,
                None,
                Some(Arc::clone(&kafka) as Arc<dyn KafkaProducer>),
            )
            .await
            .expect("attach output runner");
            await_self_leadership(&mgr, Duration::from_secs(10)).await;

            let raft = mgr.get_raft(GROUP).await.expect("raft handle");
            write_requests(&raft, &batch_one).await;
            let records = await_sink_records(&kafka, N).await;
            assert_eq!(
                records.len(),
                N,
                "no restart yet: exactly one delivery per output"
            );

            // Snapshot and pin the mechanism: the framed envelope must carry
            // sent_watermark == N with an empty pending set — this snapshot
            // is what carries the watermark across the restart.
            let framed = snapshot_with_watermark(&mgr, N as u64).await;
            let outbox = framed
                .framework_state
                .outbox
                .expect("outbox state in snapshot");
            assert!(
                outbox.pending.is_empty(),
                "everything delivered before the snapshot: pending must be empty, got {:?}",
                outbox.pending
            );
            assert_eq!(
                outbox.next_seq, N as u64,
                "seqs run 1..=N; `next_seq` is incremented before assignment"
            );

            // Post-snapshot window: M more writes, delivered, and
            // deliberately NEVER snapshotted — they stay pending on disk.
            write_requests(&raft, &batch_two).await;
            let records = await_sink_records(&kafka, N + M).await;
            assert_eq!(
                records.len(),
                N + M,
                "still one delivery per output: the restart has not happened yet"
            );

            // Graceful shutdown (NOT a kill substitute — stop_group stops
            // the output runner first and closes the rocks log); the manager
            // drops at scope end, simulating the restart boundary.
            mgr.stop_group(GROUP).await.expect("graceful stop");
        }

        // ── Phase 2: rebuild on the same data_dir — the restored watermark
        // must suppress re-delivery of the first N seqs. ────────────────────
        {
            let mgr = start_rocks_group(&base).await;
            mgr.attach_output_runner(
                GROUP,
                None,
                Some(Arc::clone(&kafka) as Arc<dyn KafkaProducer>),
            )
            .await
            .expect("re-attach output runner after restart");
            await_self_leadership(&mgr, Duration::from_secs(10)).await;

            // Settle: exactly the M post-snapshot pending outputs redeliver —
            // sink total grows from N+M to N+M+M and no further.
            let total = poll_until(Duration::from_secs(10), Duration::from_millis(50), || {
                let sink = Arc::clone(&kafka);
                async move {
                    let got = sink.records();
                    (got.len() >= N + M + M).then_some(got.len())
                }
            })
            .await
            .expect("the M post-snapshot outputs must redeliver within 10s of the restart");

            // Freeze window: no late ghost deliveries beyond the M redeliveries.
            let gained = poll_until(Duration::from_secs(2), Duration::from_millis(50), || {
                let sink = Arc::clone(&kafka);
                async move {
                    let got = sink.records();
                    (got.len() > total).then_some(got.len())
                }
            })
            .await;
            assert!(
                gained.is_none(),
                "sink kept growing after the redeliveries settled — watermark restore over-delivered"
            );

            let records = kafka.records();

            // Per-key delivery counts across BOTH lifecycles.
            let mut counts: BTreeMap<String, usize> = BTreeMap::new();
            for r in &records {
                *counts.entry(r.4.clone()).or_default() += 1;
            }

            // Assertion 1 — zero re-delivery of seqs ≤ watermark: the
            // snapshot carried the watermark to disk, restore must honor it.
            for k in &keys_one {
                assert_eq!(
                    counts.get(k).copied().unwrap_or(0),
                    1,
                    "snapshotted key {k} was redelivered after the restart — \
                     the watermark did not survive the snapshot"
                );
            }

            // Assertion 2 — re-delivery really happened for the post-snapshot
            // keys (non-vacuous arm: at-least-once must survive the restart).
            for k in &keys_two {
                assert!(
                    counts.get(k).copied().unwrap_or(0) >= 2,
                    "post-snapshot key {k} was not redelivered after the restart — \
                     at-least-once is broken"
                );
            }

            // Upper bound (design doc: total = N+M + at most M redeliveries):
            // with the infallible sink and the counts above this pins exactly
            // one redelivery per post-snapshot key and none for the rest.
            assert_eq!(
                records.len(),
                N + 2 * M,
                "expected exactly the M post-snapshot redeliveries, counts: {counts:?}"
            );

            // Assertion 3 — no loss + effective-once payloads: duplicates are
            // byte-identical replays, so dedup by idempotency key reproduces
            // exactly the N+M written payloads.
            let mut payloads: BTreeMap<String, BTreeSet<Vec<u8>>> = BTreeMap::new();
            for r in &records {
                payloads.entry(r.4.clone()).or_default().insert(r.3.clone());
            }
            for (k, set) in &payloads {
                assert_eq!(
                    set.len(),
                    1,
                    "payloads for {k} diverged across deliveries: {set:?}"
                );
            }
            let expected_keys: BTreeSet<String> = keys_one.union(&keys_two).cloned().collect();
            let deduped_keys: BTreeSet<String> = counts.into_keys().collect();
            assert_eq!(
                deduped_keys, expected_keys,
                "no loss: the dedup key set must be exactly the N+M written keys"
            );
            let deduped_payloads: BTreeSet<Vec<u8>> =
                payloads.values().flatten().cloned().collect();
            let expected_payloads: BTreeSet<Vec<u8>> = batch_one
                .iter()
                .chain(batch_two.iter())
                .map(|r| r.as_bytes().to_vec())
                .collect();
            assert_eq!(
                deduped_payloads, expected_payloads,
                "no loss, payload view: deduped payloads must equal the N+M written requests"
            );

            mgr.stop_group(GROUP).await.expect("graceful stop");
        }
    }
}
