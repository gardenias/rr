//! Production implementation of [`RaftGroupLookup`] for dispatching Raft RPCs.
//!
//! Receives serialized Raft RPC payloads from the network layer, deserializes
//! them, forwards to the correct openraft instance, and serializes the response.

use std::sync::Arc;

use async_trait::async_trait;

use rr_core::types::GroupId;
use rr_network::raft_service::RaftGroupLookup;
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::type_config::RrTypeConfig;

/// Production implementation of [`RaftGroupLookup`].
///
/// Wraps a [`RaftGroupManager`] and dispatches incoming Raft RPCs to the
/// correct openraft instance by `group_id`. All payloads are bincode-serialized.
pub struct RaftGroupLookupImpl {
    group_manager: Arc<RaftGroupManager>,
}

impl RaftGroupLookupImpl {
    /// Create a new `RaftGroupLookupImpl` backed by the given group manager.
    pub fn new(group_manager: Arc<RaftGroupManager>) -> Self {
        Self { group_manager }
    }
}

#[async_trait]
impl RaftGroupLookup for RaftGroupLookupImpl {
    async fn append_entries(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status> {
        let raft = self
            .group_manager
            .get_raft(group_id)
            .await
            .ok_or_else(|| tonic::Status::not_found(format!("group {group_id} not found")))?;

        let req: openraft::raft::AppendEntriesRequest<RrTypeConfig> =
            bincode::serde::decode_from_slice(&payload, bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|e| tonic::Status::invalid_argument(format!("decode error: {e}")))?;

        let resp = raft
            .append_entries(req)
            .await
            .map_err(|e| tonic::Status::internal(format!("append_entries error: {e}")))?;

        bincode::serde::encode_to_vec(&resp, bincode::config::standard())
            .map_err(|e| tonic::Status::internal(format!("encode error: {e}")))
    }

    async fn vote(&self, group_id: GroupId, payload: Vec<u8>) -> Result<Vec<u8>, tonic::Status> {
        let raft = self
            .group_manager
            .get_raft(group_id)
            .await
            .ok_or_else(|| tonic::Status::not_found(format!("group {group_id} not found")))?;

        let req: openraft::raft::VoteRequest<u64> =
            bincode::serde::decode_from_slice(&payload, bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|e| tonic::Status::invalid_argument(format!("decode error: {e}")))?;

        let resp = raft
            .vote(req)
            .await
            .map_err(|e| tonic::Status::internal(format!("vote error: {e}")))?;

        bincode::serde::encode_to_vec(&resp, bincode::config::standard())
            .map_err(|e| tonic::Status::internal(format!("encode error: {e}")))
    }

    async fn install_snapshot(
        &self,
        group_id: GroupId,
        metadata: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status> {
        let raft = self
            .group_manager
            .get_raft(group_id)
            .await
            .ok_or_else(|| tonic::Status::not_found(format!("group {group_id} not found")))?;

        // Decode the InstallSnapshotRequest from metadata bytes; data is the raw snapshot.
        let mut req: openraft::raft::InstallSnapshotRequest<RrTypeConfig> =
            bincode::serde::decode_from_slice(&metadata, bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|e| {
                    tonic::Status::invalid_argument(format!("decode metadata error: {e}"))
                })?;

        // Inject the snapshot data into the request.
        req.data = data;

        let resp = raft
            .install_snapshot(req)
            .await
            .map_err(|e| tonic::Status::internal(format!("install_snapshot error: {e}")))?;

        bincode::serde::encode_to_vec(&resp, bincode::config::standard())
            .map_err(|e| tonic::Status::internal(format!("encode error: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rr_core::error::RrError;
    use rr_core::types::LogIndex;
    use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};

    struct NoopSM;

    #[async_trait]
    impl rr_core::state_machine::StateMachine for NoopSM {
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
    fn test_lookup_new() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let lookup = RaftGroupLookupImpl::new(Arc::clone(&mgr));
        // Verify construction succeeds and holds the arc
        drop(lookup);
    }

    #[tokio::test]
    async fn test_lookup_unknown_group_append_entries() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let lookup = RaftGroupLookupImpl::new(Arc::clone(&mgr));

        let result = lookup.append_entries(99, vec![]).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("99"));
    }

    #[tokio::test]
    async fn test_lookup_unknown_group_vote() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let lookup = RaftGroupLookupImpl::new(Arc::clone(&mgr));

        let result = lookup.vote(42, vec![]).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("42"));
    }

    #[tokio::test]
    async fn test_lookup_unknown_group_install_snapshot() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let lookup = RaftGroupLookupImpl::new(Arc::clone(&mgr));

        let result = lookup.install_snapshot(7, vec![], vec![]).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("7"));
    }
}
