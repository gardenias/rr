use std::sync::Arc;

use openraft::type_config::async_runtime::WatchReceiver;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::storage::StorageEngine;
use rr_core::types::NodeId;
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::network_factory::ChannelPool;
use rr_router::router::ShardRouter;
use rr_server::config::NodeConfig;
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;

use crate::{echo_factory, shutdown_all, wait_for_cluster_ready, write_when_lease_ready};

async fn restart_mem_node(
    node_id: NodeId,
    addr: String,
    provider: Arc<MemConfigProvider>,
) -> Arc<RrNode> {
    let provider_trait: Arc<dyn ConfigProvider> = provider;
    let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
    let pool = Arc::new(ChannelPool::new());
    let mgr = Arc::new(RaftGroupManager::new_with_network(
        node_id,
        echo_factory(),
        pool,
    ));
    let router = Arc::new(ShardRouter::new(Arc::clone(&provider_trait)));
    let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
    let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));
    let config = NodeConfig::new(node_id, addr, storage, provider_trait, echo_factory())
        .with_group_manager(mgr)
        .with_raft_lookup(lookup)
        .with_client_dispatch(dispatch)
        .with_allow_insecure(true);

    let node = Arc::new(RrNode::new(config).expect("restart node config must be valid"));
    node.start().await.expect("restarted node must start");
    node
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mem_cluster_recovers_after_two_empty_nodes_restart() {
    let (nodes, provider) = crate::ClusterBuilder::new(3, 1)
        .with_real_network()
        .build_with_provider()
        .await
        .unwrap();
    let leader_id = wait_for_cluster_ready(&nodes, 1, 8_000).await.unwrap();
    let leader = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
    let leader_raft = leader.group_manager().get_raft(1).await.unwrap();
    write_when_lease_ready(&leader_raft, b"before-restart", 8_000)
        .await
        .unwrap();

    let table = provider.get_routing_table().await.unwrap();
    let addr2 = table.nodes[&2].addr.clone();
    let addr3 = table.nodes[&3].addr.clone();
    nodes[1].shutdown().await.unwrap();
    nodes[2].shutdown().await.unwrap();

    let node2 = restart_mem_node(2, addr2, Arc::clone(&provider)).await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let node3 = restart_mem_node(3, addr3, Arc::clone(&provider)).await;

    write_when_lease_ready(&leader_raft, b"after-restart", 8_000)
        .await
        .expect("surviving leader must recover quorum and accept writes");

    let recovered = vec![Arc::clone(leader), node2, node3];
    let expected_index = leader_raft
        .metrics()
        .borrow_watched()
        .last_log_index
        .expect("leader has a log after the recovered write");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
    for node in &recovered {
        loop {
            let raft = node.group_manager().get_raft(1).await.unwrap();
            let metrics = raft.metrics().borrow_watched().clone();
            if metrics.current_leader == Some(leader_id)
                && metrics.last_log_index >= Some(expected_index)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node {} did not catch up: {metrics:?}",
                node.node_id()
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    shutdown_all(&recovered).await;
}
