//! [`RrNode`] — the main node lifecycle manager.
//!
//! Coordinates:
//! - [`RaftGroupManager`]: manages Raft group instances
//! - [`ShardRouter`]: routes partition IDs to groups
//! - gRPC server (optional): serves Raft and client RPCs when both
//!   `raft_lookup` and `client_dispatch` are provided

use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::{watch, Mutex};
use tracing::{info, instrument, warn};
use metrics::counter;


use rr_core::config::{ConfigProvider, RoutingTableChange};
use rr_core::error::{ConfigError, RrError};
use rr_core::types::NodeId;
use rr_network::client_service::{ClientDispatch, ClientServiceImpl};
use rr_network::proto::client::client_service_server::ClientServiceServer;
use rr_network::proto::raft::raft_service_server::RaftServiceServer;
use rr_network::raft_service::{RaftGroupLookup, RaftServiceImpl};
use rr_raft::group_manager::RaftGroupManager;
use rr_router::router::ShardRouter;

use crate::config::NodeConfig;

/// The main node lifecycle manager.
///
/// Owns the [`RaftGroupManager`], [`ShardRouter`], and an optional gRPC server.
///
/// # Lifecycle
///
/// ```text
/// NodeConfig → RrNode::new() → .start() → ... → .shutdown()
/// ```
pub struct RrNode {
    node_id: NodeId,
    listen_addr: String,
    group_manager: Arc<RaftGroupManager>,
    router: Arc<ShardRouter>,
    config_provider: Arc<dyn ConfigProvider>,
    raft_lookup: Option<Arc<dyn RaftGroupLookup>>,
    client_dispatch: Option<Arc<dyn ClientDispatch>>,
    shutdown_tx: watch::Sender<bool>,
    /// Handle to the background gRPC server task; `None` if no server was started.
    server_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Handle to the config watch loop task; `None` if not started.
    watch_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RrNode {
    /// Create a new `RrNode` from [`NodeConfig`].
    ///
    /// Validates config, assembles sub-components, but does **not** start any
    /// groups or the gRPC server. Call [`start()`](Self::start) for that.
    pub fn new(config: NodeConfig) -> Result<Self, RrError> {
        if config.listen_addr.is_empty() {
            return Err(RrError::Config(ConfigError::InvalidConfig(
                "listen_addr cannot be empty".to_string(),
            )));
        }

        let group_manager = if let Some(mgr) = config.group_manager {
            mgr
        } else {
            Arc::new(RaftGroupManager::new(
                config.node_id,
                config.state_machine_factory,
            ))
        };

        // Clone the Arc before passing to ShardRouter so we can also store it
        // directly on RrNode for the watch loop.
        let config_provider = Arc::clone(&config.config_provider);
        let router = Arc::new(ShardRouter::new(config.config_provider));

        let (shutdown_tx, _) = watch::channel(false);

        info!(
            node_id = config.node_id,
            listen_addr = %config.listen_addr,
            "RrNode created"
        );

        Ok(Self {
            node_id: config.node_id,
            listen_addr: config.listen_addr,
            group_manager,
            router,
            config_provider,
            raft_lookup: config.raft_lookup,
            client_dispatch: config.client_dispatch,
            shutdown_tx,
            server_handle: Mutex::new(None),
            watch_handle: Mutex::new(None),
        })
    }

    /// Start the node.
    ///
    /// 1. Loads the routing table.
    /// 2. Starts Raft groups assigned to this node.
    /// 3. Spawns a config-change watch loop.
    /// 4. Spawns a gRPC server if both `raft_lookup` and `client_dispatch`
    ///    are present.
    #[instrument(skip(self))]
    pub async fn start(&self) -> Result<(), RrError> {
        // ── 1. Load routing table and start assigned groups ─────────────────
        let table = self.router.routing_table().await?;

        for (group_id, meta) in &table.groups {
            if meta.replicas.contains(&self.node_id) {
                self.group_manager.start_group(*group_id).await?;
                info!(
                    node_id = self.node_id,
                    group_id,
                    "started Raft group"
                );
                counter!("node.group.started").increment(1);
            }
        }

        // ── 2. Spawn config-change watch loop ────────────────────────────────
        let watch_stream = self
            .config_provider
            .watch_changes()
            .await
            .map_err(RrError::Config)?;

        let gm = Arc::clone(&self.group_manager);
        let cp = Arc::clone(&self.config_provider);
        let node_id = self.node_id;

        let watch_handle = tokio::spawn(async move {
            let mut stream = watch_stream;
            while let Some(change) = stream.next().await {
                match change {
                    RoutingTableChange::PartitionAdded { group_id, .. } => {
                        // The PartitionAdded variant doesn't carry replicas directly —
                        // look them up from the routing table.
                        match cp.get_routing_table().await {
                            Ok(table) => {
                                if let Some(meta) = table.groups.get(&group_id) {
                                    if meta.replicas.contains(&node_id) {
                                        match gm.start_group(group_id).await {
                                            Ok(()) => info!(
                                                group_id,
                                                "watch: started group on PartitionAdded"
                                            ),
                                            Err(e)
                                                if e.to_string().contains("already exists") => {}
                                            Err(e) => warn!(
                                                group_id,
                                                "watch: failed to start group on PartitionAdded: {e}"
                                            ),
                                        }
                                    }
                                }
                            }
                            Err(e) => warn!(
                                "watch: failed to get routing table on PartitionAdded: {e}"
                            ),
                        }
                    }
                    RoutingTableChange::PartitionRemoved { partition_id } => {
                        info!(partition_id, "watch: partition removed");
                    }
                    RoutingTableChange::PartitionUpdated {
                        partition_id,
                        group_id,
                        ..
                    } => {
                        info!(partition_id, group_id, "watch: partition updated");
                    }
                    RoutingTableChange::NodeAdded { node_id: nid, addr } => {
                        info!(node_id = nid, ?addr, "watch: node added");
                    }
                    RoutingTableChange::NodeRemoved { node_id: nid } => {
                        info!(node_id = nid, "watch: node removed");
                    }
                    RoutingTableChange::GroupMembershipChanged { group_id, replicas } => {
                        info!(group_id, ?replicas, "watch: group membership changed");
                        if replicas.contains(&node_id) {
                            // This node is (now) in the group's replica set — start if needed.
                            match gm.start_group(group_id).await {
                                Ok(()) => info!(
                                    group_id,
                                    "watch: started group on GroupMembershipChanged"
                                ),
                                Err(e) if e.to_string().contains("already exists") => {}
                                Err(e) => warn!(
                                    group_id,
                                    "watch: failed to start group on GroupMembershipChanged: {e}"
                                ),
                            }
                        } else {
                            // This node is no longer a replica — stop the group if running.
                            if gm.get_raft(group_id).await.is_some() {
                                if let Err(e) = gm.stop_group(group_id).await {
                                    warn!(group_id,
                                        "watch: failed to stop group on GroupMembershipChanged: {e}"
                                    );
                                } else {
                                    info!(
                                        group_id,
                                        "watch: stopped group on GroupMembershipChanged"
                                    );
                                }
                            }
                        }
                    }
                }
            }
            info!("watch: config change stream ended");
        });

        *self.watch_handle.lock().await = Some(watch_handle);
        info!(node_id = self.node_id, "config watch loop started");

        // ── 3. Start gRPC server (optional) ─────────────────────────────────
        if let (Some(raft_lookup), Some(client_dispatch)) =
            (&self.raft_lookup, &self.client_dispatch)
        {
            let addr = self.listen_addr.parse().map_err(|e| {
                RrError::Config(ConfigError::InvalidConfig(
                    format!("invalid listen_addr '{}': {e}", self.listen_addr),
                ))
            })?;

            let raft_svc =
                RaftServiceServer::new(RaftServiceImpl::new(raft_lookup.clone()));
            let client_svc =
                ClientServiceServer::new(ClientServiceImpl::new(client_dispatch.clone()));

            let mut shutdown_rx = self.shutdown_tx.subscribe();

            let handle = tokio::spawn(async move {
                info!(%addr, "gRPC server starting");
                let result = tonic::transport::Server::builder()
                    .add_service(raft_svc)
                    .add_service(client_svc)
                    .serve_with_shutdown(addr, async move {
                        let _ = shutdown_rx.changed().await;
                    })
                    .await;

                match result {
                    Ok(()) => info!(%addr, "gRPC server stopped"),
                    Err(e) => warn!(%addr, "gRPC server error: {e}"),
                }
            });

            *self.server_handle.lock().await = Some(handle);
            info!(node_id = self.node_id, "gRPC server spawned");
        } else {
            info!(
                node_id = self.node_id,
                "gRPC server not started (raft_lookup or client_dispatch missing)"
            );
        }

        info!(node_id = self.node_id, "RrNode started");
        counter!("node.started").increment(1);
        Ok(())
    }

    /// Shutdown the node gracefully.
    ///
    /// 1. Signals the gRPC server to stop (5 s timeout).
    /// 2. Aborts the config watch loop (2 s timeout).
    /// 3. Stops all active Raft groups.
    #[instrument(skip(self))]
    pub async fn shutdown(&self) -> Result<(), RrError> {
        info!(node_id = self.node_id, "RrNode shutting down");

        // ── 1. Stop gRPC server ──────────────────────────────────────────────
        let _ = self.shutdown_tx.send(true);

        if let Some(handle) = self.server_handle.lock().await.take() {
            match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
                Ok(Ok(())) => info!("gRPC server shutdown complete"),
                Ok(Err(e)) => warn!("gRPC server task panicked: {e}"),
                Err(_) => warn!("gRPC server shutdown timed out after 5s"),
            }
        }

        // ── 2. Stop config watch loop ────────────────────────────────────────
        if let Some(handle) = self.watch_handle.lock().await.take() {
            handle.abort();
            match tokio::time::timeout(std::time::Duration::from_secs(2), handle).await {
                Ok(Ok(())) => info!("config watch loop stopped"),
                Ok(Err(e)) if e.is_cancelled() => info!("config watch loop aborted"),
                Ok(Err(e)) => warn!("config watch loop panicked: {e}"),
                Err(_) => warn!("config watch loop stop timed out after 2s"),
            }
        }

        // ── 3. Stop all Raft groups ──────────────────────────────────────────
        let group_ids = self.group_manager.group_ids().await;
        for group_id in group_ids {
            if let Err(e) = self.group_manager.stop_group(group_id).await {
                warn!(group_id, "error stopping Raft group: {e}");
            }
        }

        info!(node_id = self.node_id, "RrNode shutdown complete");
        counter!("node.shutdown").increment(1);
        Ok(())
    }

    /// Return the [`RaftGroupManager`].
    pub fn group_manager(&self) -> Arc<RaftGroupManager> {
        Arc::clone(&self.group_manager)
    }

    /// Return the [`ShardRouter`].
    pub fn router(&self) -> Arc<ShardRouter> {
        Arc::clone(&self.router)
    }

    /// Return this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rr_config::mem::MemConfigProvider;
    use rr_core::config::ConfigProvider;
    use rr_core::error::RrError;
    use rr_core::state_machine::StateMachine;
    use rr_core::storage::StorageEngine;
    use rr_core::types::{LogIndex, PartitionConfig};
    use rr_raft::group_manager::StateMachineFactory;
    use rr_storage::mem::MemStorage;

    // ── Helpers ───────────────────────────────────────────────────────────────

    struct NoopSM;

    #[async_trait]
    impl StateMachine for NoopSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
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

    fn noop_factory() -> StateMachineFactory {
        Box::new(|_group_id| Box::new(NoopSM))
    }

    fn make_config(node_id: NodeId, listen_addr: &str) -> NodeConfig {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider: Arc<dyn ConfigProvider> = Arc::new(MemConfigProvider::new());
        NodeConfig::new(
            node_id,
            listen_addr.to_string(),
            storage,
            provider,
            noop_factory(),
        )
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_node_new_valid_config() {
        let node = RrNode::new(make_config(1, "127.0.0.1:50051")).unwrap();
        assert_eq!(node.node_id(), 1);
    }

    #[test]
    fn test_node_new_empty_addr_returns_error() {
        let result = RrNode::new(make_config(1, ""));
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(
            msg.contains("listen_addr cannot be empty"),
            "expected error about empty listen_addr, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_node_start_creates_groups_for_this_node() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider = MemConfigProvider::new();
        // Partition 1 assigned to group 10 with replicas [1] — this node is 1
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![1])
            .await;
        let provider: Arc<dyn ConfigProvider> = Arc::new(provider);

        let config = NodeConfig::new(
            1,
            "127.0.0.1:50051".to_string(),
            storage,
            provider,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        assert_eq!(node.group_manager().group_count().await, 1);
        assert!(node.group_manager().get_raft(10).await.is_some());

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_node_shutdown_stops_all_groups() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider = MemConfigProvider::new();
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![1])
            .await;
        let provider: Arc<dyn ConfigProvider> = Arc::new(provider);

        let config = NodeConfig::new(
            1,
            "127.0.0.1:50052".to_string(),
            storage,
            provider,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();
        assert_eq!(node.group_manager().group_count().await, 1);

        node.shutdown().await.unwrap();
        assert_eq!(node.group_manager().group_count().await, 0);
    }

    #[tokio::test]
    async fn test_node_start_skips_groups_for_other_nodes() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider = MemConfigProvider::new();
        // Group 10 replicas are [2, 3] — node 1 is NOT a replica
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![2, 3])
            .await;
        let provider: Arc<dyn ConfigProvider> = Arc::new(provider);

        let config = NodeConfig::new(
            1,                              // we are node 1
            "127.0.0.1:50053".to_string(),
            storage,
            provider,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        // Node 1 must NOT start group 10 (not in replicas)
        assert_eq!(node.group_manager().group_count().await, 0);

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_node_start_no_groups_in_routing_table() {
        let node = RrNode::new(make_config(1, "127.0.0.1:50054")).unwrap();
        // Empty routing table — start should succeed with 0 groups
        node.start().await.unwrap();
        assert_eq!(node.group_manager().group_count().await, 0);
        node.shutdown().await.unwrap();
    }

    // ── Watch loop tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_watch_loop_starts_group_on_partition_added() {
        // Start the node with empty routing table, then add a partition after
        // start. The watch loop must pick it up and start the group.
        let provider = Arc::new(MemConfigProvider::new());
        let config = NodeConfig::new(
            1,
            "127.0.0.1:50055".to_string(),
            Arc::new(MemStorage::new()),
            provider.clone() as Arc<dyn ConfigProvider>,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        // Verify no groups before adding the partition
        assert_eq!(node.group_manager().group_count().await, 0);

        // Add partition AFTER start — watch loop must pick it up
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 10, vec![1])
            .await;

        // Poll with timeout (max 500 ms)
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(500);
        loop {
            if node.group_manager().group_count().await == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "group did not start within 500ms after PartitionAdded"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(node.group_manager().get_raft(10).await.is_some());
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_watch_loop_skips_group_not_in_replicas() {
        // Add a partition where node 1 is NOT in replicas — group must not start.
        let provider = Arc::new(MemConfigProvider::new());
        let config = NodeConfig::new(
            1,
            "127.0.0.1:50056".to_string(),
            Arc::new(MemStorage::new()),
            provider.clone() as Arc<dyn ConfigProvider>,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        // Partition assigned to group 20 with replicas [2, 3] — NOT node 1
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 20, vec![2, 3])
            .await;

        // Wait a bit and verify group was NOT started
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            node.group_manager().group_count().await,
            0,
            "watch loop should not start group when node is not in replicas"
        );

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_watch_loop_handles_group_membership_changed() {
        // Start node with no groups, then fire GroupMembershipChanged with
        // this node in replicas. The watch loop must start the group.
        let provider = Arc::new(MemConfigProvider::new());

        // Pre-add partition to group 30 with node 2 as replica (not node 1)
        provider
            .add_partition(1, PartitionConfig::SingleId(1), 30, vec![2])
            .await;

        let config = NodeConfig::new(
            1,
            "127.0.0.1:50057".to_string(),
            Arc::new(MemStorage::new()),
            provider.clone() as Arc<dyn ConfigProvider>,
            noop_factory(),
        );
        let node = RrNode::new(config).unwrap();
        node.start().await.unwrap();

        // Node 1 is not in replicas initially
        assert_eq!(node.group_manager().group_count().await, 0);

        // Fire GroupMembershipChanged with node 1 now included
        provider.update_group_membership(30, vec![1, 2]).await;

        // Poll with timeout (max 500 ms)
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(500);
        loop {
            if node.group_manager().group_count().await == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "group did not start within 500ms after GroupMembershipChanged"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(node.group_manager().get_raft(30).await.is_some());
        node.shutdown().await.unwrap();
    }
}
