//! Manages the lifecycle of multiple Raft groups on a single node.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use openraft::BasicNode;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};
use metrics::counter;


use rr_core::error::{RaftError, RrError};
use rr_core::state_machine::StateMachine;
use rr_core::types::{GroupId, NodeId};

use crate::network_factory::{ChannelPool, GrpcNetworkFactory};
use crate::raft_node::{NoopNetworkFactory, RaftNode};
use crate::type_config::RrTypeConfig;
/// A factory function type for creating state machine instances per group.
pub type StateMachineFactory = Box<dyn Fn(GroupId) -> Box<dyn StateMachine> + Send + Sync>;

/// Manages multiple Raft groups on a single node.
///
/// Responsible for creating, starting, and stopping Raft group instances.
/// Thread-safe — all state is behind `Arc<RwLock<...>>`.
pub struct RaftGroupManager {
    node_id: NodeId,
    groups: Arc<RwLock<HashMap<GroupId, RaftNode>>>,
    sm_factory: Arc<StateMachineFactory>,
    /// When `Some`, new groups are created with `GrpcNetworkFactory` for real
    /// multi-node networking.  When `None`, `NoopNetworkFactory` is used
    /// (single-node / test mode).
    channel_pool: Option<ChannelPool>,
}

impl RaftGroupManager {
    /// Create a new `RaftGroupManager` in single-node / test mode.
    ///
    /// Groups started via `start_group` will use `NoopNetworkFactory`.
    pub fn new(node_id: NodeId, sm_factory: StateMachineFactory) -> Self {
        info!(node_id, "creating RaftGroupManager (single-node mode)");
        Self {
            node_id,
            groups: Arc::new(RwLock::new(HashMap::new())),
            sm_factory: Arc::new(sm_factory),
            channel_pool: None,
        }
    }

    /// Create a `RaftGroupManager` backed by real gRPC networking.
    ///
    /// Groups started via `start_group` will use `GrpcNetworkFactory` with the
    /// supplied `ChannelPool` for connection reuse.
    pub fn new_with_network(
        node_id: NodeId,
        sm_factory: StateMachineFactory,
        channel_pool: ChannelPool,
    ) -> Self {
        info!(node_id, "creating RaftGroupManager (multi-node mode)");
        Self {
            node_id,
            groups: Arc::new(RwLock::new(HashMap::new())),
            sm_factory: Arc::new(sm_factory),
            channel_pool: Some(channel_pool),
        }
    }

    /// Start a new Raft group on this node.
    ///
    /// Creates a `RaftNode` and stores it. Returns error if group already exists.
    /// When a `ChannelPool` was supplied at construction time, the group will use
    /// `GrpcNetworkFactory`; otherwise `NoopNetworkFactory` is used.
    #[instrument(skip(self))]
    pub async fn start_group(&self, group_id: GroupId) -> Result<(), RrError> {
        let mut guard = self.groups.write().await;

        if guard.contains_key(&group_id) {
            return Err(RrError::Application(format!(
                "group {group_id} already exists"
            )));
        }

        let sm = (self.sm_factory)(group_id);

        let node = if let Some(pool) = &self.channel_pool {
            let factory = GrpcNetworkFactory::new(pool.clone(), group_id);
            RaftNode::new_with_factory(group_id, self.node_id, sm, factory)
                .await
                .map_err(|e| RrError::Application(e.to_string()))?
        } else {
            RaftNode::new_with_factory(group_id, self.node_id, sm, NoopNetworkFactory)
                .await
                .map_err(|e| RrError::Application(e.to_string()))?
        };

        debug!(group_id, node_id = self.node_id, "started raft group");
        let node_id = self.node_id;
        counter!("raft.group.start", "node_id" => node_id.to_string()).increment(1);
        guard.insert(group_id, node);
        Ok(())
    }

    /// Bootstrap a multi-node Raft cluster for a group.
    ///
    /// Calls `openraft::Raft::initialize(members)` on the group identified by
    /// `group_id`.  This must be called exactly once across the whole cluster
    /// to elect an initial leader.
    ///
    /// `members` maps each node's `NodeId` to its `BasicNode` (containing the
    /// gRPC address).  The group must already be started via `start_group`.
    #[instrument(skip(self, members))]
    pub async fn init_cluster(
        &self,
        group_id: GroupId,
        members: BTreeMap<u64, BasicNode>,
    ) -> Result<(), RrError> {
        let raft = self
            .get_raft(group_id)
            .await
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        raft.initialize(members)
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        info!(group_id, node_id = self.node_id, "initialized raft cluster");
        Ok(())
    }

    /// Stop and remove a Raft group from this node.
    ///
    /// Calls `raft.shutdown()` and removes from the map.
    #[instrument(skip(self))]
    pub async fn stop_group(&self, group_id: GroupId) -> Result<(), RrError> {
        let mut guard = self.groups.write().await;

        let node = guard
            .remove(&group_id)
            .ok_or(RrError::Raft(RaftError::GroupNotFound { group_id }))?;

        node.raft
            .shutdown()
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        info!(group_id, node_id = self.node_id, "stopped raft group");
        let node_id = self.node_id;
        counter!("raft.group.stop", "node_id" => node_id.to_string()).increment(1);
        Ok(())
    }

    /// Get a reference to a Raft group's `openraft::Raft` instance.
    ///
    /// Returns `None` if the group does not exist.
    #[instrument(skip(self))]
    pub async fn get_raft(
        &self,
        group_id: GroupId,
    ) -> Option<openraft::Raft<RrTypeConfig>> {
        let guard = self.groups.read().await;
        guard.get(&group_id).map(|node| node.raft.clone())
    }

    #[instrument(skip(self))]
    /// Returns the list of active group IDs.
    pub async fn group_ids(&self) -> Vec<GroupId> {
        let guard = self.groups.read().await;
        guard.keys().copied().collect()
    }

    #[instrument(skip(self))]
    /// Returns the number of active groups.
    pub async fn group_count(&self) -> usize {
        let guard = self.groups.read().await;
        guard.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rr_core::error::RrError;
    use rr_core::types::LogIndex;

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

    #[tokio::test]
    async fn test_group_manager_start_stop() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        mgr.start_group(1).await.unwrap();
        assert_eq!(mgr.group_count().await, 1);

        mgr.stop_group(1).await.unwrap();
        assert_eq!(mgr.group_count().await, 0);
    }

    #[tokio::test]
    async fn test_group_manager_start_duplicate() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        mgr.start_group(1).await.unwrap();
        let err = mgr.start_group(1).await.unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "expected 'already exists' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_group_manager_stop_nonexistent() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        let err = mgr.stop_group(99).await.unwrap_err();
        assert!(
            err.to_string().contains("group not found"),
            "expected 'group not found' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_group_manager_multiple_groups() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        mgr.start_group(1).await.unwrap();
        mgr.start_group(2).await.unwrap();
        mgr.start_group(3).await.unwrap();

        let mut ids = mgr.group_ids().await;
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);

        mgr.stop_group(2).await.unwrap();

        let mut ids = mgr.group_ids().await;
        ids.sort();
        assert_eq!(ids, vec![1, 3]);
        assert_eq!(mgr.group_count().await, 2);
    }

    #[tokio::test]
    async fn test_group_manager_get_raft() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        // Non-existent group returns None
        assert!(mgr.get_raft(1).await.is_none());

        mgr.start_group(1).await.unwrap();

        // Existing group returns Some
        assert!(mgr.get_raft(1).await.is_some());

        mgr.stop_group(1).await.unwrap();

        // Stopped group returns None
        assert!(mgr.get_raft(1).await.is_none());
    }

    #[tokio::test]
    async fn test_group_manager_with_channel_pool() {
        // Create manager with a real ChannelPool — groups should start successfully
        // even without any actual connections (we just verify construction + start).
        let pool = ChannelPool::new();
        let mgr = RaftGroupManager::new_with_network(1, noop_factory(), pool);

        // Starting a group with GrpcNetworkFactory should succeed
        mgr.start_group(10).await.unwrap();
        assert_eq!(mgr.group_count().await, 1);
        assert!(mgr.get_raft(10).await.is_some());

        mgr.stop_group(10).await.unwrap();
        assert_eq!(mgr.group_count().await, 0);
    }

    #[tokio::test]
    async fn test_init_cluster_unknown_group_returns_error() {
        let mgr = RaftGroupManager::new(1, noop_factory());

        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode { addr: "127.0.0.1:5000".to_string() });

        let err = mgr.init_cluster(99, members).await.unwrap_err();
        assert!(
            err.to_string().contains("group not found"),
            "expected 'group not found' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_init_cluster_single_node() {
        // Single-node cluster: start group, init with self as sole member,
        // wait for leader election.
        let mgr = RaftGroupManager::new(1, noop_factory());
        mgr.start_group(1).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode { addr: String::new() });

        // init_cluster must succeed
        mgr.init_cluster(1, members).await.unwrap();

        // Wait for leader election
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // The raft instance should have a leader now
        let raft = mgr.get_raft(1).await.unwrap();
        let metrics = raft.metrics().borrow().clone();
        assert!(
            metrics.current_leader.is_some(),
            "expected a leader after init_cluster, got: {:?}",
            metrics.current_leader
        );
    }
}
