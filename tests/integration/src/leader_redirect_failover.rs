//! Integration test for ADR A-0013 / roadmap WS-7: with `GroupMeta.leader`
//! removed from the routing table, the client SDK must still converge on the
//! real raft leader after a leader failover — purely via its leader cache,
//! bootstrap round-robin, and `leader_info` redirects.
//!
//! Scenario:
//! 1. Boot a 3-node real-network cluster, elect a leader for group 1.
//! 2. Client write succeeds (populates the client's leader cache via redirect).
//! 3. Kill the current leader; wait until every surviving node sees the NEW
//!    leader in its raft metrics.
//! 4. Client writes again: the cached (dead) leader fails, the client falls
//!    back to bootstrap round-robin, follows the `leader_info` redirect, and
//!    the write must succeed — without any routing-table leader hint.
//!
//! The test never constructs or asserts `GroupMeta.leader`; that field no
//! longer exists.

use rr_client::{RetryConfig, RrClient};
use rr_core::config::ConfigProvider;

use crate::{wait_for_new_leader, ClusterBuilder};

/// WS-7 acceptance — client converges on the new leader via redirect after
/// the old leader is killed, with no `GroupMeta.leader` in the routing table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_client_redirect_converges_after_leader_kill() {
    let (nodes, provider) = ClusterBuilder::new(3, 1)
        .with_real_network()
        .build_with_provider()
        .await
        .unwrap();

    let leader_id = crate::wait_for_cluster_ready(&nodes, 1, 8_000)
        .await
        .unwrap();

    // Sanity: the routing table describes placement only — group metadata is
    // `{group_id, replicas}`; there is no leader field to consult.
    let table = provider.get_routing_table().await.unwrap();
    let group = table.groups.get(&1).expect("group 1 in routing table");
    assert_eq!(group.replicas.len(), 3, "group 1 must span all 3 nodes");

    let addrs: Vec<String> = table.nodes.values().map(|n| n.addr.clone()).collect();
    assert_eq!(addrs.len(), 3, "bootstrap addrs for all nodes");
    let client = RrClient::connect_with_config(
        addrs,
        RetryConfig {
            max_retries: 10,
            retry_delay_ms: 100,
        },
    )
    .await
    .unwrap();

    // Pre-failover write succeeds and populates the client's leader cache
    // (via `leader_info` redirect if the first hop was a follower).
    let resp = client
        .send_request(1, b"pre-failover".to_vec())
        .await
        .unwrap();
    assert_eq!(resp, b"pre-failover", "EchoSM must echo the written bytes");

    // Kill the current leader.
    let leader_node = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
    leader_node.shutdown().await.unwrap();

    let survivors: Vec<_> = nodes
        .iter()
        .filter(|n| n.node_id() != leader_id)
        .cloned()
        .collect();
    assert_eq!(survivors.len(), 2);

    // Wait until EVERY survivor observes the new leader in its raft metrics —
    // otherwise a lagging survivor would answer the client with
    // `success=false` and no `leader_info` (leader not yet known locally).
    let mut new_leader_id = None;
    for node in &survivors {
        let observed = wait_for_new_leader(std::slice::from_ref(node), 1, leader_id, 8_000)
            .await
            .unwrap();
        assert_ne!(
            observed, leader_id,
            "new leader must differ from killed node"
        );
        if let Some(prev) = new_leader_id {
            assert_eq!(prev, observed, "survivors must agree on the new leader");
        } else {
            new_leader_id = Some(observed);
        }
    }

    // Post-failover: the cached leader is dead. The client must invalidate it,
    // fall back to bootstrap round-robin, follow the `leader_info` redirect
    // from a survivor, and complete the write on the new leader.
    let resp = client
        .send_request(1, b"post-failover".to_vec())
        .await
        .expect("client must converge on the new leader via redirect");
    assert_eq!(
        resp, b"post-failover",
        "EchoSM must echo the written bytes after redirect convergence"
    );

    // The leader cache now points at the new leader: a subsequent write hits
    // it directly and succeeds.
    let resp = client
        .send_request(1, b"post-failover-2".to_vec())
        .await
        .unwrap();
    assert_eq!(resp, b"post-failover-2");

    for node in &survivors {
        let _ = node.shutdown().await;
    }
}
