//! gRPC `RaftService` implementation for inter-node Raft consensus RPCs.
//!
//! Handles `AppendEntries`, `Vote`, and `InstallSnapshot` RPCs, demultiplexing
//! them to the correct Raft group by `group_id`.
//!
//! The [`RaftGroupLookup`] trait uses a bytes-oriented interface so that
//! `rr-network` does not need a dependency on `rr-raft` (which already depends
//! on `rr-network`, and a cycle is not allowed). The implementor (typically in
//! `rr-raft`) is responsible for bincode-decoding the openraft request,
//! forwarding to the `Raft` handle, and bincode-encoding the response.

use std::sync::Arc;

use rr_core::types::GroupId;
use tokio_stream::StreamExt;

use crate::proto::raft::raft_service_server;
use crate::proto::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotChunk, SnapshotResponse, VoteRequest,
    VoteResponse,
};

/// Trait for dispatching Raft RPCs to the correct group.
///
/// All payloads are raw bytes (bincode-serialized openraft messages).
/// The implementor is responsible for looking up the `Raft<RrTypeConfig>`
/// instance and performing serialization/deserialization.
///
/// This bytes-oriented design avoids a circular dependency between
/// `rr-network` and `rr-raft`.
#[async_trait::async_trait]
pub trait RaftGroupLookup: Send + Sync {
    /// Forward an `AppendEntries` request payload to the given group.
    ///
    /// Returns the bincode-serialized `AppendEntriesResponse`.
    async fn append_entries(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status>;

    /// Forward a `Vote` request payload to the given group.
    ///
    /// Returns the bincode-serialized `VoteResponse`.
    async fn vote(&self, group_id: GroupId, payload: Vec<u8>) -> Result<Vec<u8>, tonic::Status>;

    /// Install a snapshot on the given group.
    ///
    /// `metadata` is the bincode-serialized snapshot metadata from the final
    /// chunk, and `data` is the concatenated raw snapshot bytes.
    ///
    /// Returns the bincode-serialized `InstallSnapshotResponse`.
    async fn install_snapshot(
        &self,
        group_id: GroupId,
        metadata: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status>;
}

/// gRPC service implementation that receives Raft RPCs and forwards them
/// to the appropriate Raft group instance via [`RaftGroupLookup`].
pub struct RaftServiceImpl {
    groups: Arc<dyn RaftGroupLookup>,
}

impl RaftServiceImpl {
    /// Create a new `RaftServiceImpl` backed by the given group lookup.
    pub fn new(groups: Arc<dyn RaftGroupLookup>) -> Self {
        Self { groups }
    }
}

#[tonic::async_trait]
impl raft_service_server::RaftService for RaftServiceImpl {
    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let traceparent = crate::tracing::extract_traceparent(&request);
        let req = request.into_inner();
        let group_id = req.group_id;
        tracing::debug!(
            group_id,
            traceparent = traceparent.as_deref().unwrap_or(""),
            "append_entries RPC received"
        );

        let payload = self.groups.append_entries(group_id, req.payload).await?;

        Ok(tonic::Response::new(AppendEntriesResponse {
            group_id,
            payload,
        }))
    }

    async fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let traceparent = crate::tracing::extract_traceparent(&request);
        let req = request.into_inner();
        let group_id = req.group_id;
        tracing::debug!(
            group_id,
            traceparent = traceparent.as_deref().unwrap_or(""),
            "vote RPC received"
        );

        let payload = self.groups.vote(group_id, req.payload).await?;

        Ok(tonic::Response::new(VoteResponse { group_id, payload }))
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<SnapshotChunk>>,
    ) -> Result<tonic::Response<SnapshotResponse>, tonic::Status> {
        let mut stream = request.into_inner();
        let mut chunks: Vec<SnapshotChunk> = Vec::new();

        while let Some(chunk) = stream.next().await {
            chunks.push(chunk.map_err(|e| tonic::Status::internal(format!("stream error: {e}")))?);
        }

        if chunks.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "empty snapshot stream: no chunks received",
            ));
        }

        let group_id = chunks[0].group_id;
        tracing::debug!(
            group_id,
            num_chunks = chunks.len(),
            "install_snapshot RPC received"
        );

        // Find the final chunk (done == true) and extract metadata from it.
        let final_chunk = chunks.iter().find(|c| c.done).ok_or_else(|| {
            tonic::Status::invalid_argument("no final chunk (done=true) in snapshot stream")
        })?;
        let metadata = final_chunk.metadata.clone();

        // Concatenate all data chunks.
        let mut data = Vec::new();
        for chunk in &chunks {
            data.extend_from_slice(&chunk.data);
        }

        let payload = self
            .groups
            .install_snapshot(group_id, metadata, data)
            .await?;

        Ok(tonic::Response::new(SnapshotResponse {
            group_id,
            success: true,
            payload,
        }))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    /// An empty group lookup that always returns `not_found` for every group.
    struct EmptyLookup;

    #[async_trait::async_trait]
    impl RaftGroupLookup for EmptyLookup {
        async fn append_entries(
            &self,
            group_id: GroupId,
            _payload: Vec<u8>,
        ) -> Result<Vec<u8>, tonic::Status> {
            Err(tonic::Status::not_found(format!(
                "group {group_id} not found"
            )))
        }

        async fn vote(
            &self,
            group_id: GroupId,
            _payload: Vec<u8>,
        ) -> Result<Vec<u8>, tonic::Status> {
            Err(tonic::Status::not_found(format!(
                "group {group_id} not found"
            )))
        }

        async fn install_snapshot(
            &self,
            group_id: GroupId,
            _metadata: Vec<u8>,
            _data: Vec<u8>,
        ) -> Result<Vec<u8>, tonic::Status> {
            Err(tonic::Status::not_found(format!(
                "group {group_id} not found"
            )))
        }
    }

    #[test]
    fn test_raft_service_new() {
        let svc = RaftServiceImpl::new(Arc::new(EmptyLookup));
        // Verify it compiles and the Arc is valid.
        assert!(Arc::strong_count(&svc.groups) >= 1);
    }

    #[tokio::test]
    async fn test_raft_service_group_not_found() {
        let svc = RaftServiceImpl::new(Arc::new(EmptyLookup));

        // Build a dummy AppendEntriesRequest proto message.
        let proto_req = AppendEntriesRequest {
            group_id: 1,
            payload: vec![],
        };

        let result =
            raft_service_server::RaftService::append_entries(&svc, tonic::Request::new(proto_req))
                .await;

        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("group 1 not found"));
    }

    #[tokio::test]
    async fn test_vote_group_not_found() {
        let svc = RaftServiceImpl::new(Arc::new(EmptyLookup));

        let proto_req = VoteRequest {
            group_id: 42,
            payload: vec![],
        };

        let result =
            raft_service_server::RaftService::vote(&svc, tonic::Request::new(proto_req)).await;

        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("group 42 not found"));
    }

    #[test]
    fn test_snapshot_response_has_payload_field() {
        // Test that SnapshotResponse can hold a payload field
        // This verifies the proto has been updated correctly
        let test_payload = vec![1, 2, 3, 4, 5];

        let proto_resp = SnapshotResponse {
            group_id: 42,
            success: true,
            payload: test_payload.clone(),
        };

        // Verify the payload field is present and correctly stored
        assert_eq!(proto_resp.group_id, 42);
        assert!(proto_resp.success);
        assert_eq!(proto_resp.payload, test_payload);
    }
}
