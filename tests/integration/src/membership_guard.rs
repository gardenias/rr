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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use openraft::BasicNode;
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
