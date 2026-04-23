//! Production implementation of [`ClientDispatch`] for routing client writes.

use std::sync::Arc;

use async_trait::async_trait;
use openraft::BasicNode;
use rr_core::types::GroupId;
use rr_network::client_service::{ClientDispatch, ClientWriteResult};
use rr_network::proto::client::GroupInfo;
use rr_raft::group_manager::RaftGroupManager;
use rr_router::router::ShardRouter;
use tracing::instrument;

/// Production implementation of [`ClientDispatch`].
///
/// Routes client writes to the correct Raft group via [`ShardRouter`],
/// then submits to openraft. Translates `ForwardToLeader` errors into
/// redirect responses.
pub struct ClientDispatchImpl {
    group_manager: Arc<RaftGroupManager>,
    shard_router: Arc<ShardRouter>,
}

impl ClientDispatchImpl {
    /// Create a new `ClientDispatchImpl`.
    pub fn new(group_manager: Arc<RaftGroupManager>, shard_router: Arc<ShardRouter>) -> Self {
        Self {
            group_manager,
            shard_router,
        }
    }
}

#[async_trait]
impl ClientDispatch for ClientDispatchImpl {
    #[instrument(skip(self, payload), fields(payload_len = payload.len()))]
    async fn submit_write(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
    ) -> Result<ClientWriteResult, tonic::Status> {
        let group_id: GroupId = self.shard_router.route(partition_id).await.map_err(|e| {
            tonic::Status::not_found(format!("partition {partition_id} not found: {e}"))
        })?;

        let raft = self
            .group_manager
            .get_raft(group_id)
            .await
            .ok_or_else(|| tonic::Status::not_found(format!("group {group_id} not found")))?;

        match raft.client_write(payload).await {
            Ok(resp) => Ok(ClientWriteResult {
                success: true,
                payload: resp.data,
                leader_node_id: None,
                leader_addr: None,
            }),
            Err(ref e) => {
                if let Some(fwd) = e.forward_to_leader::<BasicNode>() {
                    Ok(ClientWriteResult {
                        success: false,
                        payload: vec![],
                        leader_node_id: fwd.leader_id,
                        leader_addr: fwd.leader_node.as_ref().map(|n: &BasicNode| n.addr.clone()),
                    })
                } else {
                    Err(tonic::Status::internal(format!("client_write error: {e}")))
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn get_cluster_info(&self) -> Result<Vec<GroupInfo>, tonic::Status> {
        let group_ids = self.group_manager.group_ids().await;
        let groups = group_ids
            .into_iter()
            .map(|group_id| GroupInfo {
                group_id,
                leader_node_id: 0,
                replica_node_ids: vec![],
            })
            .collect();
        Ok(groups)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use async_trait::async_trait;
    use rr_config::mem::MemConfigProvider;
    use rr_core::error::RrError;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;
    use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};

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

    #[test]
    fn test_dispatch_new() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let config = Arc::new(MemConfigProvider::default());
        let router = Arc::new(ShardRouter::new(config));
        let _dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);
    }

    #[tokio::test]
    async fn test_dispatch_unknown_partition() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let config = Arc::new(MemConfigProvider::default());
        let router = Arc::new(ShardRouter::new(config));
        let dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);

        let result = dispatch.submit_write(999, vec![1, 2, 3]).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_dispatch_get_cluster_info_empty() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let config = Arc::new(MemConfigProvider::default());
        let router = Arc::new(ShardRouter::new(config));
        let dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);

        let result = dispatch.get_cluster_info().await;
        assert!(result.is_ok());
        let groups = result.unwrap();
        assert!(groups.is_empty());
    }
}
