//! Manages the lifecycle of multiple Raft groups on a single node.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info, instrument};
use metrics::counter;


use rr_core::error::{RaftError, RrError};
use rr_core::state_machine::StateMachine;
use rr_core::types::{GroupId, NodeId};

use crate::raft_node::RaftNode;
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
}

impl RaftGroupManager {
    /// Create a new `RaftGroupManager` for a node.
    pub fn new(node_id: NodeId, sm_factory: StateMachineFactory) -> Self {
        info!(node_id, "creating RaftGroupManager");
        Self {
            node_id,
            groups: Arc::new(RwLock::new(HashMap::new())),
            sm_factory: Arc::new(sm_factory),
        }
    }

    /// Start a new Raft group on this node.
    ///
    /// Creates a `RaftNode` and stores it. Returns error if group already exists.
    #[instrument(skip(self))]
    pub async fn start_group(&self, group_id: GroupId) -> Result<(), RrError> {
        let mut guard = self.groups.write().await;

        if guard.contains_key(&group_id) {
            return Err(RrError::Application(format!(
                "group {group_id} already exists"
            )));
        }

        let sm = (self.sm_factory)(group_id);
        let node = RaftNode::new(group_id, self.node_id, sm)
            .await
            .map_err(|e| RrError::Application(e.to_string()))?;

        debug!(group_id, node_id = self.node_id, "started raft group");
        let node_id = self.node_id;
        counter!("raft.group.start", "node_id" => node_id.to_string()).increment(1);
        guard.insert(group_id, node);
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
}
