//! INV-4 outbox failover — KafkaPublish arm (design waves O-1 + O-2 of
//! `.sisyphus/plans/rr-inv4-outbox-failover-e2e.md`).
//!
//! Cluster shape (all three tests): 3 real-network nodes, group 1 running
//! `OutboxEchoService` with a `KafkaPublish` target, deliveries captured by a
//! single `CapturingKafka` shared across all nodes.
//!
//! Mechanism facts under test (design doc "关键机制事实"):
//! - `attach_output_runner` is an orphan API — `RrNode::start` never wires
//!   it, so every node must be attached explicitly; non-leader runners park.
//! - The outbox `sent_watermark` is leader-local memory, never
//!   raft-replicated. Healthy followers' watermark stays 0, so after a
//!   graceful leader kill the new leader redelivers every pending output
//!   from seq 1 — no snapshot manipulation needed to open the window.
//! - Kill = graceful `shutdown().await`, which stops the old runner via
//!   `stop_group`. `FaultInjector::isolate` is NOT a kill substitute here:
//!   the mock producer bypasses the fault-aware pool, so an isolated
//!   ex-leader would keep delivering and break the assertions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use rr_core::output::KafkaProducer;
use rr_core::types::{GroupId, NodeId};
use rr_server::node::RrNode;

use crate::outbox_support::{
    build_outbox_cluster, fetch_framed_snapshot, kafka_echo_target, outbox_echo_factory,
    poll_until, CapturingKafka, KAFKA_KEY, KAFKA_PARTITION, KAFKA_TOPIC,
};
use crate::{shutdown_all, wait_for_cluster_ready, wait_for_new_leader};

const GROUP: GroupId = 1;
const N: usize = 5;

/// Request payloads for one test round, e.g. `inv4-req-0` .. `inv4-req-4`.
fn requests() -> Vec<String> {
    (0..N).map(|i| format!("inv4-req-{i}")).collect()
}

/// Expected idempotency key for a request payload: `"{request}:0"`.
fn expected_keys(requests: &[String]) -> BTreeSet<String> {
    requests.iter().map(|r| format!("{r}:0")).collect()
}

/// Attach a shared `CapturingKafka` output runner on every node. Returns the
/// shared sink handle (the same `Arc` the runners hold).
async fn attach_kafka_runners(nodes: &[Arc<RrNode>], kafka: &Arc<CapturingKafka>) {
    for node in nodes {
        node.group_manager()
            .attach_output_runner(
                GROUP,
                None,
                Some(Arc::clone(kafka) as Arc<dyn KafkaProducer>),
            )
            .await
            .expect("attach output runner");
    }
}

/// Write `requests` through the leader's raft handle: the first write rides
/// the lease-ready retry (openraft fast-fails proposals until the fresh
/// leader commits its blank entry; lib.rs:475), the rest go straight through
/// the now-established leader.
async fn write_via_leader(leader_node: &Arc<RrNode>, requests: &[String]) {
    let raft = leader_node
        .group_manager()
        .get_raft(GROUP)
        .await
        .expect("leader hosts the group");
    crate::write_when_lease_ready(&raft, requests[0].as_bytes(), 5_000)
        .await
        .expect("first write while lease establishes");
    for req in requests.iter().skip(1) {
        raft.client_write(req.clone().into_bytes().into())
            .await
            .expect("client_write on the established leader");
    }
}

/// Poll the sink until at least `n` records arrive (runner budget: 50ms idle
/// poll + 100ms first backoff — a 10s timeout is generous).
async fn await_sink_records(
    kafka: &Arc<CapturingKafka>,
    n: usize,
) -> Vec<crate::outbox_support::PublishedRecord> {
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

/// Gracefully kill the current leader and wait until EVERY survivor observes
/// the SAME new leader (leader_redirect_failover.rs:78-91 pattern — a lagging
/// survivor would otherwise answer clients with no leader_info). Returns the
/// survivor set for cleanup.
async fn kill_leader_and_await_agreement(
    nodes: &[Arc<RrNode>],
    leader_id: NodeId,
) -> (Vec<Arc<RrNode>>, NodeId) {
    let leader_node = nodes
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader node present");
    leader_node
        .shutdown()
        .await
        .expect("graceful leader shutdown");

    let survivors: Vec<_> = nodes
        .iter()
        .filter(|n| n.node_id() != leader_id)
        .cloned()
        .collect();
    let mut new_leader = None;
    for node in &survivors {
        let observed = wait_for_new_leader(std::slice::from_ref(node), GROUP, leader_id, 12_000)
            .await
            .expect("new leader elected within 12s");
        assert_ne!(
            observed, leader_id,
            "new leader must differ from killed node"
        );
        if let Some(prev) = new_leader {
            assert_eq!(prev, observed, "survivors must agree on the new leader");
        } else {
            new_leader = Some(observed);
        }
    }
    (survivors, new_leader.expect("at least one survivor"))
}

/// INV-4: every committed output is eventually delivered (at-least-once);
/// the sent watermark advances only after confirmed delivery — and that
/// watermark is leader-local memory, never raft-replicated.
///
/// Smoke test (wave O-1): N=5 writes to a 3-node `OutboxEchoService` group
/// must produce exactly N sink records with field-exact
/// `(topic, partition, key, payload, idempotency_key)` 5-tuples, and a
/// snapshot decode must show `sent_watermark == 5` with an empty pending set
/// on the leader versus `sent_watermark == 0` with all 5 outputs still
/// pending on a follower — the mechanism premise that makes the O-2 failover
/// window deterministic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_inv4_outbox_delivers_outputs_kafka() {
    let (nodes, _provider) =
        build_outbox_cluster(3, 1, outbox_echo_factory(kafka_echo_target())).await;

    let kafka = CapturingKafka::new().shared();
    attach_kafka_runners(&nodes, &kafka).await;

    let leader_id = wait_for_cluster_ready(&nodes, GROUP, 8_000)
        .await
        .expect("leader elected");
    let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
    let requests = requests();
    write_via_leader(leader_node, &requests).await;

    // O-1 acceptance #1: the sink collects exactly N deliveries.
    let records = await_sink_records(&kafka, N).await;
    assert_eq!(
        records.len(),
        N,
        "no failover in this test: each pending output is delivered exactly once"
    );

    // O-1 acceptance #2: field-exact 5-tuples, keyed by the deterministic
    // idempotency key (oracle needs no service-side logging).
    let expected: BTreeMap<String, (String, i32, Vec<u8>, Vec<u8>)> = requests
        .iter()
        .map(|r| {
            (
                format!("{r}:0"),
                (
                    KAFKA_TOPIC.to_string(),
                    KAFKA_PARTITION,
                    KAFKA_KEY.to_vec(),
                    r.as_bytes().to_vec(),
                ),
            )
        })
        .collect();
    let mut seen = BTreeSet::new();
    for (topic, partition, key, payload, idem) in &records {
        let want = expected
            .get(idem)
            .unwrap_or_else(|| panic!("unexpected idempotency key {idem}"));
        assert_eq!(topic, &want.0, "topic mismatch for {idem}");
        assert_eq!(partition, &want.1, "partition mismatch for {idem}");
        assert_eq!(key, &want.2, "key mismatch for {idem}");
        assert_eq!(payload, &want.3, "payload mismatch for {idem}");
        seen.insert(idem.clone());
    }
    assert_eq!(
        seen,
        expected.keys().cloned().collect::<BTreeSet<_>>(),
        "every expected idempotency key delivered exactly once"
    );

    // O-1 acceptance #3 (mechanism pin): the sent watermark lives only in
    // the leader's memory. Its own snapshot must show watermark == N with an
    // empty pending set; a follower's selfie snapshot shows watermark 0 with
    // all N outputs still pending. The poll retries the trigger — the sink
    // records a publish just before the runner advances the watermark, so a
    // just-materialised snapshot can legitimately lag by one; re-triggering
    // produces a newer snapshot until one observes the watermark.
    let leader_outbox = poll_until(
        Duration::from_secs(10),
        Duration::from_millis(200),
        || async {
            fetch_framed_snapshot(leader_node, GROUP, Duration::from_secs(2))
                .await
                .ok()
                .and_then(|f| f.framework_state.outbox)
                .filter(|s| s.sent_watermark == N as u64)
        },
    )
    .await
    .expect("leader snapshot must show sent_watermark == N");
    assert!(
        leader_outbox.pending.is_empty(),
        "leader delivered everything: pending must be empty, got {:?}",
        leader_outbox.pending
    );
    assert_eq!(
        leader_outbox.next_seq, N as u64,
        "seqs run 1..=N; `next_seq` is incremented before assignment, so after \
         N outputs it equals N — and is deterministic across replicas"
    );

    let follower_node = nodes.iter().find(|n| n.node_id() != leader_id).unwrap();
    let follower_outbox = poll_until(
        Duration::from_secs(10),
        Duration::from_millis(200),
        || async {
            fetch_framed_snapshot(follower_node, GROUP, Duration::from_secs(2))
                .await
                .ok()
                .and_then(|f| f.framework_state.outbox)
                .filter(|s| s.pending.len() == N)
        },
    )
    .await
    .expect("follower must apply all N outputs before its snapshot shows them");
    assert_eq!(
        follower_outbox.sent_watermark, 0,
        "watermark is leader-local and never raft-replicated"
    );
    assert_eq!(follower_outbox.next_seq, N as u64);
    let follower_keys: BTreeSet<String> = follower_outbox
        .pending
        .iter()
        .map(|o| o.idempotency_key.clone())
        .collect();
    assert_eq!(
        follower_keys,
        expected_keys(&requests),
        "follower pending set must replicate the leader's exactly"
    );

    shutdown_all(&nodes).await;
}

/// INV-4: after a leader failover, every pending output is redelivered
/// at-least-once — nothing is lost, re-delivery really happens, and
/// duplicates are byte-identical, so downstream dedup by idempotency key is
/// effectively-once.
///
/// Core test (wave O-2): the failover window is opened by the mechanism
/// itself — the watermark is never raft-replicated, so the new leader starts
/// from 0 and redelivers the whole pending outbox. NO snapshot is triggered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_inv4_outbox_redelivers_after_leader_failover_kafka() {
    let (nodes, _provider) =
        build_outbox_cluster(3, 1, outbox_echo_factory(kafka_echo_target())).await;

    let kafka = CapturingKafka::new().shared();
    attach_kafka_runners(&nodes, &kafka).await;

    let old_leader = wait_for_cluster_ready(&nodes, GROUP, 8_000)
        .await
        .expect("leader elected");
    let leader_node = nodes.iter().find(|n| n.node_id() == old_leader).unwrap();
    let requests = requests();
    write_via_leader(leader_node, &requests).await;

    // Old leader delivers all N. Deliberately no snapshot anywhere: the
    // "delivered but not snapshotted" window is guaranteed by the mechanism.
    let _ = await_sink_records(&kafka, N).await;

    let (survivors, _new_leader) = kill_leader_and_await_agreement(&nodes, old_leader).await;

    let expected = expected_keys(&requests);

    // The new leader's runner resumes from watermark 0 and redelivers the
    // full pending outbox (in practice the total reaches 2N; assert the
    // lower bounds — unique key set complete AND strictly more than N
    // records — never the ceiling).
    let records = poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            let got = kafka.records();
            let keys: BTreeSet<&String> = got.iter().map(|r| &r.4).collect();
            (keys.len() == expected.len() && got.len() > N).then_some(got)
        },
    )
    .await
    .expect(
        "redelivery after failover within 10s (election ~300ms + idle poll 50ms + backoff 100ms)",
    );

    // Assertion 1 — no loss: every expected idempotency key was published.
    let published_keys: BTreeSet<String> = records.iter().map(|r| r.4.clone()).collect();
    for key in &expected {
        assert!(
            published_keys.contains(key),
            "lost output after failover: {key} never published"
        );
    }

    // Assertion 2 — re-delivery really happened: at least one key published
    // twice (mechanism evidence; a full redelivery makes every key ≥ 2).
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for r in &records {
        *counts.entry(r.4.clone()).or_default() += 1;
    }
    assert!(
        counts.values().any(|&c| c >= 2),
        "expected at-least-one redelivered key, counts: {counts:?}"
    );

    // Assertion 3 — effective-once: dedup by idempotency_key leaves
    // byte-identical payloads (duplicates are exact replays, so downstream
    // dedup converges on the single original delivery).
    let mut payloads: BTreeMap<String, BTreeSet<Vec<u8>>> = BTreeMap::new();
    for r in &records {
        payloads.entry(r.4.clone()).or_default().insert(r.3.clone());
    }
    for (key, set) in &payloads {
        assert_eq!(
            set.len(),
            1,
            "payloads for {key} diverged across deliveries: {set:?}"
        );
    }
    let deduped_payloads: BTreeSet<Vec<u8>> =
        payloads.values().flat_map(|s| s.iter().cloned()).collect();
    let expected_payloads: BTreeSet<Vec<u8>> =
        requests.iter().map(|r| r.as_bytes().to_vec()).collect();
    assert_eq!(
        deduped_payloads, expected_payloads,
        "no loss, payload view: deduped payloads must equal the N written requests"
    );

    shutdown_all(&survivors).await;
}

/// INV-4: output delivery requires an attached output runner — a failover to
/// a node with no runner attached delivers nothing. Negative control proving
/// the positive redelivery assertions are not vacuous.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_inv4_outbox_no_delivery_without_attached_runner() {
    let (nodes, _provider) =
        build_outbox_cluster(3, 1, outbox_echo_factory(kafka_echo_target())).await;

    // Attach the runner ONLY on the initial leader — the whole point.
    let leader_id = wait_for_cluster_ready(&nodes, GROUP, 8_000)
        .await
        .expect("leader elected");
    let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
    let kafka = CapturingKafka::new().shared();
    leader_node
        .group_manager()
        .attach_output_runner(
            GROUP,
            None,
            Some(Arc::clone(&kafka) as Arc<dyn KafkaProducer>),
        )
        .await
        .expect("attach output runner on the leader");

    let requests = requests();
    write_via_leader(leader_node, &requests).await;

    // The attached leader delivers all N.
    let records = await_sink_records(&kafka, N).await;
    assert_eq!(records.len(), N);

    let (survivors, _new_leader) = kill_leader_and_await_agreement(&nodes, leader_id).await;

    // No survivor has a runner attached: across a ~3s settle window the sink
    // must stay frozen. (If this assertion ever fails, the positive tests'
    // delivery path would not be the runner, and their assertions would be
    // meaningless.)
    let before = kafka.records().len();
    let gained = poll_until(Duration::from_secs(3), Duration::from_millis(50), || {
        let sink = Arc::clone(&kafka);
        async move {
            let got = sink.records();
            (got.len() > before).then_some(got.len())
        }
    })
    .await;
    assert!(
        gained.is_none(),
        "sink gained records with no runner attached — the positive tests would be vacuous"
    );
    assert_eq!(kafka.records().len(), before, "sink must stay frozen");

    shutdown_all(&survivors).await;
}
