//! INV-4 outbox failover — RaftWrite arm (design wave O-3 of
//! `.sisyphus/plans/rr-inv4-outbox-failover-e2e.md`).
//!
//! Cluster shape: 3 real-network nodes hosting TWO raft groups. Group 1 runs
//! `OutboxEchoService` with an `OutputTarget::RaftWrite` target into group 2;
//! group 2 runs the harness `CountingService`, whose shared apply vec is the
//! absorption oracle. One output runner per node is attached to group 1, all
//! sharing a single `RrClient` built from the routing-table bootstrap addrs
//! (`leader_redirect_failover.rs` pattern). The `addrs` field of the
//! `RaftWrite` target is ignored by dispatch — targeting comes from the
//! client that was attached to the runner.
//!
//! What this arm proves that the Kafka arm cannot: re-delivery is absorbed
//! by the FRAMEWORK idempotency layer of the target group, not by a test
//! double. The runner re-sends each output via `send_request_idempotent`
//! under a per-source-group namespace (`rr.output.g<N>`, outbox seq as the
//! request id); the target group's apply path decides `Replay`, so the user
//! service's `handle` is never re-invoked. The target apply count therefore
//! stays pinned at 3N across the failover even though the target raft log
//! grows by the N re-delivered entries.
//!
//! Mechanism facts under test (design doc "关键机制事实"):
//! - The outbox `sent_watermark` is leader-local memory, never
//!   raft-replicated. Followers' watermarks stay 0 forever, so after a
//!   graceful source-leader kill the new leader MUST re-deliver the whole
//!   pending outbox from seq 1.
//! - Kill = graceful `shutdown().await` (stops the old runner via
//!   `stop_group`). The killed node is also a target-group replica, so both
//!   groups need a leader check afterwards — the target group keeps its
//!   leader when the killed node was only a follower, and re-elects when it
//!   was the leader; `wait_for_new_leader` covers both (it only requires a
//!   leader different from the killed node).
//! - `pending_outputs()` returns outputs with seq > watermark, so on the new
//!   leader it drains N → 0 only through successful deliveries.
//! - N is deliberately tiny (3): the default 10k-entry idempotency LRU can
//!   never evict the re-delivered keys, so a re-delivery can never degrade
//!   into `Expired` (which would wedge the runner in permanent backoff).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::type_config::async_runtime::WatchReceiver;
use rr_client::{RetryConfig, RrClient};
use rr_core::config::ConfigProvider;
use rr_core::output::OutputTarget;
use rr_core::types::{GroupId, NodeId};
use rr_raft::group_manager::StateMachineFactory;
use rr_server::node::RrNode;

use crate::outbox_support::{
    build_outbox_cluster, counting_factory, outbox_echo_factory, poll_until,
};
use crate::{shutdown_all, wait_for_cluster_ready, wait_for_new_leader, write_when_lease_ready};

const SOURCE_GROUP: GroupId = 1;
const TARGET_GROUP: GroupId = 2;
const N: usize = 3;

/// Request payloads for one test round, e.g. `inv4-rw-0` .. `inv4-rw-2`.
/// Kept tiny (design doc §O-3): the default 10k idempotency LRU must never
/// expire a re-delivered key.
fn requests() -> Vec<String> {
    (0..N).map(|i| format!("inv4-rw-{i}")).collect()
}

/// Write `requests` through the source leader's raft handle: the first write
/// rides the lease-ready retry (openraft fast-fails proposals until the fresh
/// leader commits its blank entry), the rest go straight through the
/// now-established leader.
async fn write_via_leader(leader_node: &Arc<RrNode>, requests: &[String]) {
    let raft = leader_node
        .group_manager()
        .get_raft(SOURCE_GROUP)
        .await
        .expect("leader hosts the source group");
    write_when_lease_ready(&raft, requests[0].as_bytes(), 5_000)
        .await
        .expect("first write while lease establishes");
    for req in requests.iter().skip(1) {
        raft.client_write(req.clone().into_bytes().into())
            .await
            .expect("client_write on the established leader");
    }
}

/// Poll until the shared target-group apply vec holds exactly `n` entries
/// (every committed entry applies once per target replica).
async fn await_target_applies(applied: &Arc<Mutex<Vec<Vec<u8>>>>, n: usize) {
    poll_until(Duration::from_secs(10), Duration::from_millis(50), || {
        let applied = Arc::clone(applied);
        async move {
            let len = applied.lock().unwrap().len();
            (len == n).then_some(len)
        }
    })
    .await
    .unwrap_or_else(|| panic!("target group must apply exactly {n} entries within 10s"));
}

/// The `last_log_index` of the node that currently self-identifies as
/// `group_id`'s leader, or `None` while no node in `nodes` does.
async fn leader_log_index(nodes: &[Arc<RrNode>], group_id: GroupId) -> Option<u64> {
    for node in nodes {
        if let Some(raft) = node.group_manager().get_raft(group_id).await {
            let metrics = raft.metrics().borrow_watched().clone();
            if metrics.current_leader == Some(node.node_id()) {
                return metrics.last_log_index;
            }
        }
    }
    None
}

/// Poll until some node self-identifies as `group_id`'s leader and returns
/// its `last_log_index`.
async fn await_leader_log_index(nodes: &[Arc<RrNode>], group_id: GroupId) -> u64 {
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async { leader_log_index(nodes, group_id).await },
    )
    .await
    .expect("a leader with a readable last_log_index must exist within 10s")
}

/// Gracefully kill the source-group leader (also a target-group replica, so
/// both groups need a leader re-check) and wait until EVERY survivor agrees
/// on the same new leader for BOTH groups. For the target group this is
/// immediate when the killed node was a mere follower, and covers the
/// re-election when it was the leader. Returns the survivor set and the new
/// source-group leader.
async fn kill_source_leader_and_await_agreement(
    nodes: &[Arc<RrNode>],
    old_leader: NodeId,
) -> (Vec<Arc<RrNode>>, NodeId) {
    let leader_node = nodes
        .iter()
        .find(|n| n.node_id() == old_leader)
        .expect("leader node present");
    leader_node
        .shutdown()
        .await
        .expect("graceful source-leader shutdown");

    let survivors: Vec<_> = nodes
        .iter()
        .filter(|n| n.node_id() != old_leader)
        .cloned()
        .collect();

    let mut new_source_leader = None;
    for group_id in [SOURCE_GROUP, TARGET_GROUP] {
        let mut agreed = None;
        for node in &survivors {
            let observed =
                wait_for_new_leader(std::slice::from_ref(node), group_id, old_leader, 12_000)
                    .await
                    .expect("leader agreement within 12s");
            assert_ne!(
                observed, old_leader,
                "group {group_id}'s new leader must differ from the killed node"
            );
            if let Some(prev) = agreed {
                assert_eq!(
                    prev, observed,
                    "survivors must agree on group {group_id}'s leader"
                );
            } else {
                agreed = Some(observed);
            }
        }
        if group_id == SOURCE_GROUP {
            new_source_leader = agreed;
        }
    }
    (
        survivors,
        new_source_leader.expect("source group re-elected"),
    )
}

/// INV-4: after a source-group leader failover, every pending output is
/// re-delivered at-least-once, and the target group's framework idempotency
/// layer absorbs the duplicates — the target service executes effectively
/// once (no loss, no duplicate execution).
///
/// Core test (wave O-3): N=3 writes to group 1 produce 3 outputs; the old
/// leader's runner delivers each to group 2 via `send_request_idempotent`
/// (3 committed entries × 3 target replicas = 9 service applies). The
/// source leader is then killed gracefully. Because the sent watermark is
/// never raft-replicated, the new source leader starts from watermark 0 and
/// MUST re-deliver all N outputs; the target group absorbs them as `Replay`
/// decisions, so its apply count stays pinned at exactly 3N while its raft
/// log still grows by the N re-delivered entries — the framework-level
/// effectively-once the Kafka arm can only simulate.
///
/// Evidence-chain note (deviation from the design doc's literal step 5):
/// "poll the new leader's `pending_outputs(1)` until full, then until empty"
/// is only directly observable when the killed node was ALSO the target
/// group's leader — then the target re-election blocks the re-delivering
/// runner for hundreds of milliseconds and the full N → empty transition is
/// caught on the poll. In the common case (killed node is a target-group
/// follower) the target leader is intact and the runner drains all N outputs
/// within milliseconds of the election — before the test thread, which only
/// learns of the election on its 50ms poll, can observe the full state. The
/// "full" premise is therefore pinned deterministically BEFORE the kill on
/// both followers (their watermarks are 0 forever, so their pending is the
/// full N — exactly the mechanism fact under test), and "empty" is observed
/// on the actual new leader after the failover. Re-delivery itself is proven
/// twice: the drain (pending can only empty through successful deliveries)
/// and the target leader's log index growing by ≥ N (nothing but the runner
/// ever writes to group 2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_inv4_outbox_raft_write_duplicates_absorbed_by_target_group() {
    // Target-group apply recorder shared by all target replicas, and a
    // per-group factory: group 1 = outbox-emitting echo service whose
    // `RaftWrite` target names group 2 (addrs ignored — the attached client
    // routes), group 2 = the counting service.
    let applied = Arc::new(Mutex::new(Vec::new()));
    let target = OutputTarget::RaftWrite {
        addrs: vec![],
        group_id: TARGET_GROUP,
    };
    let factory: StateMachineFactory = Box::new({
        let applied = Arc::clone(&applied);
        move |gid| match gid {
            SOURCE_GROUP => outbox_echo_factory(target.clone())(gid),
            TARGET_GROUP => counting_factory(Arc::clone(&applied))(gid),
            _ => panic!("outbox raft-write arm: unexpected group {gid}"),
        }
    });

    let (nodes, provider) = build_outbox_cluster(3, 2, factory).await;

    // One delivery client shared by every node's runner: bootstrap from the
    // routing table, 10 retries × 100ms to ride out BOTH groups re-electing
    // when the killed node was the target group's leader too.
    let table = provider.get_routing_table().await.expect("routing table");
    let addrs: Vec<String> = table.nodes.values().map(|n| n.addr.clone()).collect();
    assert_eq!(addrs.len(), 3, "bootstrap addrs for all three nodes");
    let rr_client = Arc::new(
        RrClient::connect_with_config(
            addrs,
            RetryConfig {
                max_retries: 10,
                retry_delay_ms: 100,
            },
        )
        .await
        .expect("build RrClient"),
    );
    for node in &nodes {
        node.group_manager()
            .attach_output_runner(SOURCE_GROUP, Some(Arc::clone(&rr_client)), None)
            .await
            .expect("attach output runner");
    }

    let old_leader = wait_for_cluster_ready(&nodes, SOURCE_GROUP, 8_000)
        .await
        .expect("source group leader elected");
    let leader_node = nodes.iter().find(|n| n.node_id() == old_leader).unwrap();
    let requests = requests();
    write_via_leader(leader_node, &requests).await;

    // Every output delivered exactly once by the old leader: each output
    // becomes one committed entry on group 2, applying once per replica —
    // 3 payloads × 3 target replicas = 3N.
    await_target_applies(&applied, 3 * N).await;

    // Premise pin (the watermark is leader-local memory, never replicated):
    // while the old leader's watermark sits at N, EVERY follower still shows
    // the full pending outbox — whichever survivor wins the election must
    // re-deliver all N outputs from seq 1.
    let followers: Vec<_> = nodes
        .iter()
        .filter(|n| n.node_id() != old_leader)
        .cloned()
        .collect();
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            for node in &followers {
                match node.group_manager().pending_outputs(SOURCE_GROUP).await {
                    Ok(pending) if pending.len() == N => {}
                    _ => return None,
                }
            }
            Some(())
        },
    )
    .await
    .expect("both followers must show the full pending outbox (watermark never replicated)");

    // Baseline the target leader's log index: after this point the only
    // writer to group 2 is the source group's output runner.
    let baseline_log = await_leader_log_index(&nodes, TARGET_GROUP).await;

    let (survivors, new_source_leader) =
        kill_source_leader_and_await_agreement(&nodes, old_leader).await;
    let new_leader_node = survivors
        .iter()
        .find(|n| n.node_id() == new_source_leader)
        .expect("new source leader among survivors");

    // Re-delivery execution proof: the new leader's pending outbox drains to
    // empty — pending can only shrink via the watermark, and the watermark
    // only advances after a successful delivery. Track the high-water mark
    // for diagnostics (in the joint-leader case it IS observed at N; in the
    // fast-drain case the followers' full-pending pin above substitutes).
    let max_pending = Arc::new(AtomicUsize::new(0));
    poll_until(Duration::from_secs(10), Duration::from_millis(50), || {
        let mgr = new_leader_node.group_manager();
        let max_pending = Arc::clone(&max_pending);
        async move {
            let pending = mgr.pending_outputs(SOURCE_GROUP).await.ok()?;
            max_pending.fetch_max(pending.len(), Ordering::SeqCst);
            pending.is_empty().then_some(())
        }
    })
    .await
    .unwrap_or_else(|| {
        panic!(
            "new leader's runner must re-deliver and drain the pending outbox within 10s \
             (max pending observed: {})",
            max_pending.load(Ordering::SeqCst)
        )
    });

    // Re-delivery commitment proof: group 2's log grew by at least the N
    // re-delivered entries (a target-group re-election may add one blank
    // entry on top — hence ≥, not ==).
    let final_log = await_leader_log_index(&survivors, TARGET_GROUP).await;
    assert!(
        final_log >= baseline_log + N as u64,
        "target log must grow by the {N} re-delivered entries: baseline {baseline_log}, final {final_log}"
    );

    // Absorption: across a settle window the service-level apply count must
    // stay pinned at exactly 3N — the re-delivered entries committed on the
    // target group's log but were absorbed as idempotency Replays before the
    // service's `handle` (a re-execution would append 2 more applies per
    // surviving replica).
    let regressed = poll_until(Duration::from_secs(3), Duration::from_millis(50), || {
        let applied = Arc::clone(&applied);
        async move {
            let len = applied.lock().unwrap().len();
            (len != 3 * N).then_some(len)
        }
    })
    .await;
    assert!(
        regressed.is_none(),
        "target apply count must stay pinned at {} after failover (Replay absorption), got {regressed:?}",
        3 * N
    );

    // No loss, payload view: each of the N payloads applied exactly once per
    // target replica (3 replicas were alive for the original delivery).
    let mut seen = applied.lock().unwrap().clone();
    seen.sort();
    let mut expected: Vec<Vec<u8>> = requests
        .iter()
        .flat_map(|r| vec![r.as_bytes().to_vec(); 3])
        .collect();
    expected.sort();
    assert_eq!(
        seen, expected,
        "target group must execute each payload exactly once per replica"
    );
    let seen_set: BTreeSet<Vec<u8>> = seen.into_iter().collect();
    let expected_set: BTreeSet<Vec<u8>> = requests.iter().map(|r| r.as_bytes().to_vec()).collect();
    assert_eq!(
        seen_set, expected_set,
        "no payload lost across the failover"
    );

    shutdown_all(&survivors).await;
}
