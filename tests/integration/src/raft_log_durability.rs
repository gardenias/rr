//! ADR A-0016: replicated + durable Raft log — rolling-restart e2e.
//!
//! Three nodes with real gRPC networking AND RocksDB-backed raft logs
//! (`RaftGroupManager::new_with_network` + `with_rocks_base_path`): acked
//! writes must survive a rolling restart of every node, the cluster must
//! keep committing with one node down, recovered nodes must catch up via
//! replication and serve linearizable reads. This is the production
//! configuration that was missing before A-0016 (`rr-prod-perf.md` G8).

// Gated `cfg(all(test, feature = "rocks"))` (not just `feature = "rocks"`) so
// the module vanishes entirely from non-test lib builds — otherwise its
// helpers/imports are dead code there and fail the workspace clippy gate.
#[cfg(all(test, feature = "rocks"))]
mod rocks_raft_log_durability {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::{anyhow, Result};
    use openraft::type_config::async_runtime::WatchReceiver;
    use openraft::BasicNode;
    use rr_config::mem::MemConfigProvider;
    use rr_core::config::ConfigProvider;
    use rr_core::storage::StorageEngine;
    use rr_core::types::{NodeAddr, NodeId, PartitionConfig};
    use rr_raft::group_manager::RaftGroupManager;
    use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
    use rr_router::router::ShardRouter;
    use rr_server::config::NodeConfig;
    use rr_server::dispatch::ClientDispatchImpl;
    use rr_server::lookup::RaftGroupLookupImpl;
    use rr_server::node::RrNode;
    use rr_storage::mem::MemStorage;
    use tempfile::TempDir;

    use crate::echo_factory;

    const GROUP: u64 = 1;

    struct TestNode {
        id: NodeId,
        addr: String,
        data_dir: PathBuf,
        node: Arc<RrNode>,
    }

    async fn start_node(
        id: NodeId,
        addr: &str,
        data_dir: PathBuf,
        provider: Arc<dyn ConfigProvider>,
    ) -> Result<TestNode> {
        let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
        let mgr = Arc::new(
            RaftGroupManager::new_with_network(id, echo_factory(), pool)
                .with_rocks_base_path(format!("{}/raft-log", data_dir.display())),
        );
        let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
        let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
        let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let config = NodeConfig::new(id, addr.to_string(), storage, provider, echo_factory())
            .with_group_manager(Arc::clone(&mgr))
            .with_raft_lookup(lookup)
            .with_client_dispatch(dispatch)
            .with_allow_insecure(true);
        let node = Arc::new(RrNode::new(config)?);
        node.start().await?;
        Ok(TestNode {
            id,
            addr: addr.to_string(),
            data_dir,
            node,
        })
    }

    async fn stop_node(tn: TestNode) -> Result<(NodeId, String, PathBuf)> {
        tn.node.shutdown().await?;
        Ok((tn.id, tn.addr, tn.data_dir))
    }

    async fn wait_leader(nodes: &[TestNode], timeout: Duration) -> Result<NodeId> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for tn in nodes {
                if let Some(raft) = tn.node.group_manager().get_raft(GROUP).await {
                    if let Some(leader) = raft.metrics().borrow_watched().current_leader {
                        return Ok(leader);
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!("no leader within {timeout:?}"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_log_frontier(tn: &TestNode, min: u64, timeout: Duration) -> Result<u64> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(raft) = tn.node.group_manager().get_raft(GROUP).await {
                if let Some(idx) = raft.metrics().borrow_watched().last_log_index {
                    if idx >= min {
                        return Ok(idx);
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!(
                    "node {} log frontier did not reach {min} within {timeout:?}",
                    tn.id
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Write via any node that currently accepts (leader moves during
    /// restarts; followers answer NotLeader).
    async fn write_any(nodes: &[TestNode], payload: Vec<u8>, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for tn in nodes {
                if tn
                    .node
                    .group_manager()
                    .write(GROUP, payload.clone())
                    .await
                    .is_ok()
                {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!("no node accepted a write within {timeout:?}"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// INV-1 / ADR A-0016: acked writes survive a rolling restart of every
    /// node when the raft log is RocksDB-backed AND replicated (the
    /// production configuration).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_rocks_raft_log_rolling_restart_preserves_acked_writes() -> Result<()> {
        let tmp = TempDir::new()?;
        let provider = Arc::new(MemConfigProvider::new());
        let provider_dyn: Arc<dyn ConfigProvider> = provider.clone();

        // Pre-allocate ports (released before node start; TOCTOU is
        // acceptable in tests). Restarting a node re-binds the same address.
        let mut addrs: Vec<(NodeId, String)> = Vec::new();
        for id in 1..=3u64 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let port = listener.local_addr()?.port();
            drop(listener);
            addrs.push((id, format!("127.0.0.1:{port}")));
        }
        provider
            .add_partition(GROUP, PartitionConfig::SingleId(1), GROUP, vec![1, 2, 3])
            .await;
        for (id, addr) in &addrs {
            provider
                .add_node(*id, NodeAddr { addr: addr.clone() })
                .await;
        }

        // ── Phase 1: start the 3-node cluster, bootstrap, write 50 acked ──
        let mut nodes: Vec<TestNode> = Vec::new();
        for (id, addr) in &addrs {
            let dir = tmp.path().join(format!("node{id}"));
            nodes.push(start_node(*id, addr, dir, Arc::clone(&provider_dyn)).await?);
        }
        let members: BTreeMap<u64, BasicNode> = addrs
            .iter()
            .map(|(id, addr)| (*id, BasicNode { addr: addr.clone() }))
            .collect();
        nodes[0]
            .node
            .group_manager()
            .init_cluster(GROUP, members)
            .await?;
        let leader = wait_leader(&nodes, Duration::from_secs(10)).await?;

        for i in 0..50u8 {
            nodes[(leader - 1) as usize]
                .node
                .group_manager()
                .write(GROUP, vec![i])
                .await
                .map_err(|e| anyhow!("phase-1 write {i} failed: {e}"))?;
        }
        // init membership + 50 writes ⇒ frontier ≥ 51; followers converge.
        let frontier =
            wait_log_frontier(&nodes[(leader - 1) as usize], 51, Duration::from_secs(10)).await?;
        for tn in &nodes {
            wait_log_frontier(tn, frontier, Duration::from_secs(10)).await?;
        }

        // ── Phase 2: rolling restart — one node down at a time; the other
        //    two keep quorum and keep committing; the recovered node catches
        //    up via replication from its on-disk log + leader stream ──
        for i in 0..3 {
            let (id, addr, dir) = stop_node(nodes.remove(i)).await?;
            write_any(&nodes, b"while-down".to_vec(), Duration::from_secs(10)).await?;
            let tn = start_node(id, &addr, dir, Arc::clone(&provider_dyn)).await?;
            nodes.insert(i, tn);
            wait_log_frontier(&nodes[i], frontier + 1, Duration::from_secs(15)).await?;
        }

        // ── Phase 3: post-restart convergence, fresh writes, lin reads ──
        let leader = wait_leader(&nodes, Duration::from_secs(10)).await?;
        for i in 0..10u8 {
            nodes[(leader - 1) as usize]
                .node
                .group_manager()
                .write(GROUP, vec![100 + i])
                .await
                .map_err(|e| anyhow!("phase-3 write {i} failed: {e}"))?;
        }
        let final_frontier = wait_log_frontier(
            &nodes[(leader - 1) as usize],
            frontier + 13,
            Duration::from_secs(10),
        )
        .await?;
        for tn in &nodes {
            wait_log_frontier(tn, final_frontier, Duration::from_secs(10)).await?;
            // A node that recovered its log from disk still serves
            // linearizable reads (ReadIndex machinery works post-recovery).
            let probe = b"ping".to_vec();
            match tn
                .node
                .group_manager()
                .linearizable_read(GROUP, probe.clone())
                .await
            {
                Ok(v) => assert_eq!(v, probe),
                Err(follower_err) => {
                    // ForwardToLeader on a follower is legitimate; retry on leader.
                    let v = nodes[(leader - 1) as usize]
                        .node
                        .group_manager()
                        .linearizable_read(GROUP, probe.clone())
                        .await
                        .map_err(|e| {
                            anyhow!(
                                "lin read on node {} failed ({follower_err}); leader retry: {e}",
                                tn.id
                            )
                        })?;
                    assert_eq!(v, probe);
                }
            }
        }

        // ── Phase 4: byte-level proof — after a graceful shutdown, open one
        //    node's log DB directly and verify the full frontier is on disk ──
        let (stopped_id, _addr, dir) = stop_node(nodes.remove(0)).await?;
        {
            use openraft::storage::RaftLogStorage;
            use rr_raft::rocks_log_storage::RocksLogStorage;

            let mut log = RocksLogStorage::open(&format!("{}/raft-log/1", dir.display()))
                .map_err(|e| anyhow!("open log DB of node {stopped_id}: {e}"))?;
            let state = log.get_log_state().await?;
            assert!(
                state.last_log_id.map(|id| id.index) >= Some(final_frontier),
                "node {stopped_id} log DB must contain the full frontier: {state:?}"
            );
            log.close().await?;
        }

        for tn in nodes {
            tn.node.shutdown().await?;
        }
        Ok(())
    }
}
