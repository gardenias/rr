//! Integration tests for the server-side change-membership quorum guard
//! (ADR A-0012 / roadmap WS-6).
//!
//! A 3-node real-network cluster is booted with the AdminService enabled
//! on a per-node admin port (`allow_insecure` test mode). The test drives
//! `AdminServiceClient` against the leader's admin endpoint and asserts:
//!
//! | step | request | expected |
//! |---|---|---|
//! | A | `voters=[leader]` (3→1, no force) | `FailedPrecondition`; membership unchanged |
//! | B | same call on a follower | NOT `FailedPrecondition` (not-leader path) |
//! | C | `voters=[leader, other]` (3→2) | OK; membership converges to 2 voters |
//! | D | `voters=[leader]` (2→1, `force=true`) | OK; membership converges to 1 voter |
//!
//! All steps share one cluster so state transitions are ordered and
//! observable; sub-test failures name the step in the assert message.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::{BasicNode, LogIdOptionExt};
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::storage::StorageEngine;
use rr_core::types::{NodeId, PartitionConfig};
use rr_network::proto::admin::admin_service_client::AdminServiceClient;
use rr_network::proto::admin::{ChangeMembershipRequest, GroupId};
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
use rr_router::router::ShardRouter;
use rr_server::config::NodeConfig;
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;
use tonic::transport::Channel;

use crate::{echo_factory, shutdown_all, wait_for_cluster_ready};

const GROUP_ID: u64 = 1;

/// Build a 3-node real-network cluster with the AdminService exposed on
/// each node. Returns the nodes plus their admin ports (index-aligned).
async fn build_3node_admin_cluster() -> Result<(Vec<Arc<RrNode>>, Vec<u16>)> {
    let node_ids: Vec<NodeId> = vec![1, 2, 3];
    let config_provider = Arc::new(MemConfigProvider::new());

    config_provider
        .add_partition(
            GROUP_ID,
            PartitionConfig::SingleId(GROUP_ID),
            GROUP_ID,
            node_ids.clone(),
        )
        .await;

    // Pre-allocate OS-assigned ports for both the raft/client gRPC
    // endpoint and the admin endpoint of every node.
    let mut node_addrs: Vec<(NodeId, String)> = Vec::with_capacity(node_ids.len());
    let mut admin_ports: Vec<u16> = Vec::with_capacity(node_ids.len());
    for &node_id in &node_ids {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        drop(listener); // release port; small TOCTOU window is acceptable in tests
        node_addrs.push((node_id, format!("127.0.0.1:{port}")));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let admin_port = listener.local_addr()?.port();
        drop(listener);
        admin_ports.push(admin_port);
    }

    for (node_id, addr) in &node_addrs {
        config_provider
            .add_node(*node_id, rr_core::types::NodeAddr { addr: addr.clone() })
            .await;
    }

    let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
    let mut nodes: Vec<Arc<RrNode>> = Vec::with_capacity(node_ids.len());

    for ((node_id, addr), admin_port) in node_addrs.iter().zip(&admin_ports) {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider: Arc<dyn ConfigProvider> = Arc::clone(&config_provider) as _;

        let mgr = Arc::new(RaftGroupManager::new_with_network(
            *node_id,
            echo_factory(),
            pool.clone(),
        ));

        let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
        let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
        let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));

        let config = NodeConfig::new(*node_id, addr.clone(), storage, provider, echo_factory())
            .with_group_manager(Arc::clone(&mgr))
            .with_raft_lookup(lookup)
            .with_client_dispatch(dispatch)
            .with_admin_port(*admin_port)
            .with_allow_insecure(true);

        let node = Arc::new(RrNode::new(config)?);
        node.start().await?;
        nodes.push(node);
    }

    let mut members = BTreeMap::new();
    for (node_id, addr) in &node_addrs {
        members.insert(*node_id, BasicNode { addr: addr.clone() });
    }
    let raft = nodes[0]
        .group_manager()
        .get_raft(GROUP_ID)
        .await
        .ok_or_else(|| anyhow::anyhow!("node 1 missing group {GROUP_ID}"))?;
    let _ = raft.initialize(members).await;

    // No fixed sleep here: `admin_client` retries connects within a
    // deadline until the fire-and-forget gRPC servers are bound, and the
    // raft side converges through `wait_for_voters` / leader polling.
    Ok((nodes, admin_ports))
}

async fn admin_client(admin_port: u16) -> Result<AdminServiceClient<Channel>> {
    // The admin gRPC server is spawned fire-and-forget by `RrNode::start`,
    // and an eager `connect()` fails fast on connection-refused (no retry).
    // Poll within a deadline so a stalled CI scheduler cannot flake the
    // first RPC of a fresh cluster.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let attempt = Channel::from_shared(format!("http://127.0.0.1:{admin_port}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .connect()
            .await;
        match attempt {
            Ok(chan) => return Ok(AdminServiceClient::new(chan)),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(anyhow::anyhow!(
                        "admin server on port {admin_port} did not accept connections within 10s: {e}"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Poll `GetGroupMembership` until the voter set equals `expected`
/// (order-insensitive) or the deadline passes.
async fn wait_for_voters(
    client: &mut AdminServiceClient<Channel>,
    expected: &[u64],
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let resp = client
            .get_group_membership(GroupId { group_id: GROUP_ID })
            .await?
            .into_inner();
        let mut voters: Vec<u64> = resp.voters.iter().map(|m| m.node_id).collect();
        voters.sort_unstable();
        let mut want = expected.to_vec();
        want.sort_unstable();
        if voters == want {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "membership did not converge within {timeout:?}: want {want:?}, last seen {voters:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_change_membership_quorum_guard_e2e() {
    let (nodes, admin_ports) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let leader_idx = (leader_id - 1) as usize;

    let mut leader_client = admin_client(admin_ports[leader_idx])
        .await
        .expect("connect to leader admin port");

    // ── Step A: 3→1 without force is rejected; membership unchanged ────────
    let err = leader_client
        .change_membership(ChangeMembershipRequest {
            group_id: GROUP_ID,
            voter_node_ids: vec![leader_id],
            force: false,
        })
        .await
        .expect_err("step A: shrinking to a single voter must be rejected");
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "step A: quorum-guard rejection must be FailedPrecondition, got {err:?}"
    );
    wait_for_voters(&mut leader_client, &[1, 2, 3], Duration::from_secs(5))
        .await
        .expect("step A: membership must be unchanged after a rejected change");

    // ── Step B: the guard does not regress the not-leader path ────────────
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let mut follower_client = admin_client(admin_ports[follower_idx])
        .await
        .expect("connect to follower admin port");
    let err = follower_client
        .change_membership(ChangeMembershipRequest {
            group_id: GROUP_ID,
            voter_node_ids: vec![1, 2],
            force: false,
        })
        .await
        .expect_err("step B: change_membership on a follower must fail");
    assert_ne!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "step B: follower rejection must come from the not-leader path, not the guard; got {err:?}"
    );

    // ── Step C: 3→2 is allowed (target == minimum keeps a majority) ───────
    let other_id = (1..=3u64).find(|id| *id != leader_id).unwrap();
    leader_client
        .change_membership(ChangeMembershipRequest {
            group_id: GROUP_ID,
            voter_node_ids: vec![leader_id, other_id],
            force: false,
        })
        .await
        .expect("step C: 3→2 must be allowed");
    wait_for_voters(
        &mut leader_client,
        &[leader_id, other_id],
        Duration::from_secs(5),
    )
    .await
    .expect("step C: membership must converge to the 2-voter set");

    // ── Step D: 2→1 with force succeeds and is applied ─────────────────────
    leader_client
        .change_membership(ChangeMembershipRequest {
            group_id: GROUP_ID,
            voter_node_ids: vec![leader_id],
            force: true,
        })
        .await
        .expect("step D: force must override the quorum guard");
    wait_for_voters(&mut leader_client, &[leader_id], Duration::from_secs(5))
        .await
        .expect("step D: forced 2→1 must be applied");

    shutdown_all(&nodes).await;
}

async fn wait_for_membership(
    mgr: &RaftGroupManager,
    expected_voters: BTreeSet<NodeId>,
    expected_learners: BTreeSet<NodeId>,
) -> Result<()> {
    let raft = mgr
        .get_raft(GROUP_ID)
        .await
        .ok_or_else(|| anyhow::anyhow!("missing group {GROUP_ID}"))?;
    let metrics_rx = raft.metrics();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let membership = metrics_rx
            .borrow_watched()
            .membership_config
            .membership()
            .clone();
        let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        let learners: BTreeSet<NodeId> = membership.learner_ids().collect();
        if voters == expected_voters && learners == expected_learners {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "membership did not converge: want voters {expected_voters:?} and learners {expected_learners:?}, last seen voters {voters:?} and learners {learners:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_configuration_settled(mgr: &RaftGroupManager) -> Result<()> {
    let raft = mgr
        .get_raft(GROUP_ID)
        .await
        .ok_or_else(|| anyhow::anyhow!("missing group {GROUP_ID}"))?;
    let metrics_rx = raft.metrics();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let metrics = metrics_rx.borrow_watched().clone();
        let joint_configs = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .len();
        let membership_committed = metrics.membership_config.as_ref()
            == metrics.committed_membership_config.as_ref();
        let logs_committed = metrics
            .last_log_index
            .is_none_or(|last| metrics.local_committed.index() == Some(last));
        if joint_configs == 1 && membership_committed && logs_committed {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "configuration did not settle: joint_configs={joint_configs}, membership_committed={membership_committed}, logs_committed={logs_committed}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_remove_node_removes_voter_without_touching_other_learners() {
    let (nodes, _) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let removed_id = (1..=3).find(|id| *id != leader_id).unwrap();
    let mgr = nodes[(leader_id - 1) as usize].group_manager();

    mgr.add_learner_nonblocking(GROUP_ID, 4, "127.0.0.1:1".to_string())
        .await
        .unwrap();
    mgr.remove_node(GROUP_ID, removed_id).await.unwrap();

    let expected_voters = (1..=3)
        .filter(|id| *id != removed_id)
        .collect::<BTreeSet<_>>();
    wait_for_membership(mgr.as_ref(), expected_voters, BTreeSet::from([4]))
        .await
        .expect("remove_node must delete only the target voter and its learner entry");
    shutdown_all(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_remove_node_removes_learner_only_node() {
    let (nodes, _) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let mgr = nodes[(leader_id - 1) as usize].group_manager();

    mgr.add_learner_nonblocking(GROUP_ID, 4, "127.0.0.1:1".to_string())
        .await
        .unwrap();
    mgr.remove_node(GROUP_ID, 4).await.unwrap();

    wait_for_membership(mgr.as_ref(), BTreeSet::from([1, 2, 3]), BTreeSet::new())
        .await
        .expect("remove_node must remove a learner-only node");
    shutdown_all(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_promote_learners_does_not_restore_removed_voter() {
    let (nodes, _) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let removed_id = (1..=3).find(|id| *id != leader_id).unwrap();
    let mgr = nodes[(leader_id - 1) as usize].group_manager();

    mgr.add_learner_nonblocking(GROUP_ID, 4, "127.0.0.1:1".to_string())
        .await
        .unwrap();
    let promotable = BTreeSet::from([4]);
    let remaining_voters = (1..=3)
        .filter(|id| *id != removed_id)
        .collect::<BTreeSet<_>>();
    mgr.change_membership(GROUP_ID, remaining_voters.clone())
        .await
        .unwrap();
    mgr.promote_learners(GROUP_ID, promotable).await.unwrap();

    let expected_voters = remaining_voters
        .into_iter()
        .chain(std::iter::once(4))
        .collect::<BTreeSet<_>>();
    wait_for_membership(mgr.as_ref(), expected_voters, BTreeSet::from([removed_id]))
        .await
        .expect("promotion must add learner 4 without restoring the removed voter");
    shutdown_all(&nodes).await;
}

async fn wait_for_admin_membership(
    client: &mut AdminServiceClient<Channel>,
    expected_voters: &[u64],
    expected_learners: &[u64],
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let resp = client
            .get_group_membership(GroupId { group_id: GROUP_ID })
            .await?
            .into_inner();
        let mut voters: Vec<u64> = resp.voters.iter().map(|m| m.node_id).collect();
        let mut learners: Vec<u64> = resp.learners.iter().map(|m| m.node_id).collect();
        voters.sort_unstable();
        learners.sort_unstable();
        let mut want_voters = expected_voters.to_vec();
        let mut want_learners = expected_learners.to_vec();
        want_voters.sort_unstable();
        want_learners.sort_unstable();
        if voters == want_voters && learners == want_learners {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "membership did not converge within {timeout:?}: want voters {want_voters:?} and learners {want_learners:?}, last seen voters {voters:?} and learners {learners:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_remove_group_member_voter_guard_and_force_e2e() {
    let (nodes, admin_ports) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let mut client = admin_client(admin_ports[(leader_id - 1) as usize])
        .await
        .expect("connect to leader admin port");
    let removed_id = (1..=3).find(|id| *id != leader_id).unwrap();
    let other_follower_id = (1..=3)
        .find(|id| *id != leader_id && *id != removed_id)
        .unwrap();
    let mgr = nodes[(leader_id - 1) as usize].group_manager();
    mgr.add_learner_nonblocking(GROUP_ID, 4, "127.0.0.1:1".to_string())
        .await
        .unwrap();

    client
        .remove_group_member(rr_network::proto::admin::RemoveGroupMemberRequest {
            group_id: GROUP_ID,
            node_id: removed_id,
            force: false,
        })
        .await
        .expect("3→2 voter removal must be allowed");
    wait_for_admin_membership(
        &mut client,
        &[leader_id, other_follower_id],
        &[4],
        Duration::from_secs(5),
    )
    .await
    .expect("removed voter must be absent from both voters and learners");

    let err = client
        .remove_group_member(rr_network::proto::admin::RemoveGroupMemberRequest {
            group_id: GROUP_ID,
            node_id: other_follower_id,
            force: false,
        })
        .await
        .expect_err("2→1 voter removal without force must be rejected");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    wait_for_admin_membership(
        &mut client,
        &[leader_id, other_follower_id],
        &[4],
        Duration::from_secs(5),
    )
    .await
    .expect("rejected removal must leave membership unchanged");

    client
        .remove_group_member(rr_network::proto::admin::RemoveGroupMemberRequest {
            group_id: GROUP_ID,
            node_id: other_follower_id,
            force: true,
        })
        .await
        .expect("force must allow a 2→1 voter removal");
    wait_for_admin_membership(&mut client, &[leader_id], &[4], Duration::from_secs(5))
        .await
        .expect("forced removal must erase the target from both membership roles");

    shutdown_all(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_remove_group_member_learner_only_e2e() {
    let (nodes, admin_ports) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let mut client = admin_client(admin_ports[(leader_id - 1) as usize])
        .await
        .expect("connect to leader admin port");
    let mgr = nodes[(leader_id - 1) as usize].group_manager();
    mgr.add_learner_nonblocking(GROUP_ID, 4, "127.0.0.1:1".to_string())
        .await
        .unwrap();
    wait_for_admin_membership(&mut client, &[1, 2, 3], &[4], Duration::from_secs(5))
        .await
        .expect("learner must be visible before removal");

    client
        .remove_group_member(rr_network::proto::admin::RemoveGroupMemberRequest {
            group_id: GROUP_ID,
            node_id: 4,
            force: false,
        })
        .await
        .expect("learner-only removal must succeed");
    wait_for_admin_membership(&mut client, &[1, 2, 3], &[], Duration::from_secs(5))
        .await
        .expect("removed learner must be absent from both voters and learners");

    shutdown_all(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_removed_online_voter_stays_absent_after_promotion_window() {
    let (nodes, admin_ports) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let removed_id = (1..=3).find(|id| *id != leader_id).unwrap();
    let remaining_id = (1..=3)
        .find(|id| *id != leader_id && *id != removed_id)
        .unwrap();
    let mut client = admin_client(admin_ports[(leader_id - 1) as usize])
        .await
        .expect("connect to leader admin port");

    client
        .remove_group_member(rr_network::proto::admin::RemoveGroupMemberRequest {
            group_id: GROUP_ID,
            node_id: removed_id,
            force: false,
        })
        .await
        .expect("online voter removal must succeed");
    wait_for_admin_membership(
        &mut client,
        &[leader_id, remaining_id],
        &[],
        Duration::from_secs(5),
    )
    .await
    .expect("removed voter must initially be absent from both membership roles");

    tokio::time::sleep(
        rr_raft::learner_promoter::DEFAULT_POLL_INTERVAL
            * (rr_raft::learner_promoter::DEFAULT_STABILITY_TICKS + 1),
    )
    .await;

    let membership = client
        .get_group_membership(GroupId { group_id: GROUP_ID })
        .await
        .expect("membership must remain readable after the promotion window")
        .into_inner();
    assert!(
        membership
            .voters
            .iter()
            .all(|member| member.node_id != removed_id),
        "removed voter returned after auto-promotion stability ticks: {:?}",
        membership.voters
    );
    assert!(
        membership
            .learners
            .iter()
            .all(|member| member.node_id != removed_id),
        "removed voter returned as a learner: {:?}",
        membership.learners
    );

    shutdown_all(&nodes).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_concurrent_remove_group_member_rechecks_committed_voters() {
    use rr_network::proto::admin::admin_service_server::AdminService;
    use rr_server::admin::AdminServiceImpl;
    use tonic::Request;

    let (nodes, _) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let first_removed = (1..=3).find(|id| *id != leader_id).unwrap();
    let second_removed = (1..=3)
        .find(|id| *id != leader_id && *id != first_removed)
        .unwrap();
    let mgr = nodes[(leader_id - 1) as usize].group_manager();
    let svc = AdminServiceImpl::new(
        leader_id,
        "127.0.0.1:9090".to_string(),
        mgr,
        Arc::new(MemConfigProvider::new()),
    );

    // `join!` polls the first request into its Raft await and then polls the
    // second before Raft core can apply the first. Without an atomic manager
    // boundary, both guards therefore observe three voters and both succeed.
    let (first, second) = tokio::join!(
        svc.remove_group_member(Request::new(
            rr_network::proto::admin::RemoveGroupMemberRequest {
                group_id: GROUP_ID,
                node_id: first_removed,
                force: false,
            },
        )),
        svc.remove_group_member(Request::new(
            rr_network::proto::admin::RemoveGroupMemberRequest {
                group_id: GROUP_ID,
                node_id: second_removed,
                force: false,
            },
        ))
    );
    first.expect("the first 3→2 removal must succeed");
    let err = second.expect_err("the second removal must revalidate and reject 2→1");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    let membership = svc
        .get_group_membership(Request::new(GroupId { group_id: GROUP_ID }))
        .await
        .unwrap()
        .into_inner();
    let mut voters: Vec<u64> = membership.voters.iter().map(|m| m.node_id).collect();
    voters.sort_unstable();
    let mut expected = vec![leader_id, second_removed];
    expected.sort_unstable();
    assert_eq!(voters, expected);
    assert!(membership.learners.is_empty());

    shutdown_all(&nodes).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_initialize_for_formation_reentry_releases_membership_lock() {
    use rr_raft::group_manager::InitializeOutcome;

    let (nodes, _) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let mgr = nodes[(leader_id - 1) as usize].group_manager();
    let raft = mgr.get_raft(GROUP_ID).await.unwrap();
    let members = raft
        .metrics()
        .borrow_watched()
        .membership_config
        .membership()
        .nodes()
        .map(|(node_id, node)| (*node_id, node.clone()))
        .collect::<BTreeMap<_, _>>();

    let (first, second) = tokio::join!(
        mgr.initialize_for_formation(GROUP_ID, members.clone()),
        mgr.initialize_for_formation(GROUP_ID, members)
    );
    assert_eq!(first.unwrap(), InitializeOutcome::AlreadyInitialized);
    assert_eq!(second.unwrap(), InitializeOutcome::AlreadyInitialized);

    mgr.add_learner_nonblocking(GROUP_ID, 4, "127.0.0.1:1".to_string())
        .await
        .expect("a later membership mutation must acquire the released group lock");
    wait_for_membership(mgr.as_ref(), BTreeSet::from([1, 2, 3]), BTreeSet::from([4]))
        .await
        .unwrap();
    shutdown_all(&nodes).await;
}

async fn exceed_learner_catch_up_threshold(mgr: &RaftGroupManager) {
    mgr.write_batch(GROUP_ID, vec![vec![0]; 5_001])
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_blocking_add_learner_allows_bounded_learner_removal() {
    let (nodes, _) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let mgr = nodes[(leader_id - 1) as usize].group_manager();
    exceed_learner_catch_up_threshold(mgr.as_ref()).await;

    let add_mgr = Arc::clone(&mgr);
    let addition = tokio::spawn(async move {
        add_mgr
            .add_learner(GROUP_ID, 4, "127.0.0.1:1".to_string())
            .await
    });
    wait_for_membership(mgr.as_ref(), BTreeSet::from([1, 2, 3]), BTreeSet::from([4]))
        .await
        .expect("unreachable learner submission must complete before catch-up waiting");

    let removal = tokio::time::timeout(Duration::from_secs(2), mgr.remove_node(GROUP_ID, 4)).await;
    addition.abort();
    let _ = addition.await;
    removal
        .expect("an unreachable blocking addition must not hold the membership lock")
        .expect("learner removal must succeed");
    wait_for_membership(mgr.as_ref(), BTreeSet::from([1, 2, 3]), BTreeSet::new())
        .await
        .expect("the unreachable learner must be removed");
    shutdown_all(&nodes).await;
}

#[derive(Clone, Default)]
struct MembershipAuditBuf {
    bytes: Arc<std::sync::Mutex<Vec<u8>>>,
}

impl MembershipAuditBuf {
    fn contents(&self) -> String {
        String::from_utf8(self.bytes.lock().unwrap().clone()).unwrap()
    }
}

impl std::io::Write for MembershipAuditBuf {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.bytes.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MembershipAuditBuf {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_force_audit_waits_for_authoritative_locked_decision() {
    use rr_network::proto::admin::admin_service_server::AdminService;
    use rr_server::admin::AdminServiceImpl;
    use tonic::Request;

    let (nodes, _) = build_3node_admin_cluster().await.unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must elect a leader");
    let first_removed = (1..=3).find(|id| *id != leader_id).unwrap();
    let forced_removed = (1..=3)
        .find(|id| *id != leader_id && *id != first_removed)
        .unwrap();
    let mgr = nodes[(leader_id - 1) as usize].group_manager();
    mgr.remove_node(GROUP_ID, first_removed).await.unwrap();
    let remaining = BTreeSet::from([leader_id, forced_removed]);
    wait_for_membership(mgr.as_ref(), remaining.clone(), BTreeSet::new())
        .await
        .unwrap();
    exceed_learner_catch_up_threshold(mgr.as_ref()).await;
    let leader_id = wait_for_cluster_ready(&nodes, GROUP_ID, 8_000)
        .await
        .expect("cluster must retain a leader after the write setup");
    let forced_removed = *remaining
        .iter()
        .find(|id| **id != leader_id)
        .expect("the other remaining voter must be removable");
    let mgr = nodes[(leader_id - 1) as usize].group_manager();

    let add_mgr = Arc::clone(&mgr);
    let addition = tokio::spawn(async move {
        add_mgr
            .add_learner(GROUP_ID, 4, "127.0.0.1:1".to_string())
            .await
    });
    wait_for_membership(mgr.as_ref(), remaining, BTreeSet::from([4]))
        .await
        .unwrap();
    wait_for_configuration_settled(mgr.as_ref())
        .await
        .unwrap();

    let buf = MembershipAuditBuf::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .without_time()
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let svc = Arc::new(AdminServiceImpl::new(
        leader_id,
        "127.0.0.1:9090".to_string(),
        Arc::clone(&mgr),
        Arc::new(MemConfigProvider::new()),
    ));
    let removal_svc = Arc::clone(&svc);
    let mut removal = tokio::spawn(async move {
        removal_svc
            .remove_group_member(Request::new(
                rr_network::proto::admin::RemoveGroupMemberRequest {
                    group_id: GROUP_ID,
                    node_id: forced_removed,
                    force: true,
                },
            ))
            .await
    });

    tokio::time::sleep(Duration::from_millis(250)).await;
    if !removal.is_finished() {
        let early = buf.contents();
        assert!(
            !early.contains("force_accepted"),
            "force audit must not be emitted before the locked decision completes: {early}"
        );
    }

    addition.abort();
    let _ = addition.await;
    tokio::time::timeout(Duration::from_secs(5), &mut removal)
        .await
        .expect("forced removal must complete")
        .expect("removal task must not panic")
        .expect("forced removal must succeed");
    let audit = buf.contents();
    assert_eq!(audit.matches("force_accepted").count(), 1, "{audit}");
    assert!(audit.contains("current_voters=2"), "{audit}");
    assert!(audit.contains("target_voters=1"), "{audit}");
    wait_for_membership(
        mgr.as_ref(),
        BTreeSet::from([leader_id]),
        BTreeSet::from([4]),
    )
    .await
    .unwrap();
    shutdown_all(&nodes).await;
}
