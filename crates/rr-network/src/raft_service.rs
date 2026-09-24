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
use std::time::Duration;

use rr_core::types::GroupId;
use tokio_stream::StreamExt;

use crate::proto::raft::raft_service_server;
use crate::proto::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotChunk, SnapshotResponse, VoteRequest,
    VoteResponse,
};

/// Hard cap on the total number of bytes accepted across all chunks of a
/// single `InstallSnapshot` stream. Prevents an authenticated peer from
/// exhausting node memory by streaming an unbounded snapshot.
///
/// Snapshots in rr are application state plus framework metadata. Even
/// large state machines (millions of keys) typically serialize to a few
/// hundred megabytes; 4 GiB is a generous ceiling. Operators with
/// genuinely larger snapshots should refactor toward chunked apply
/// (out of scope for this protection).
pub const MAX_INSTALL_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024 * 1024;

/// Overall timeout for receiving one `InstallSnapshot` chunk stream.
///
/// Mirrors the openraft `install_snapshot_timeout` (60 s, configured on
/// `RaftNode` in rr-raft) that bounds the *sending* side; without a
/// receive-side bound a stalled or trickling peer could pin a connection
/// and its accumulated buffer indefinitely.
const INSTALL_SNAPSHOT_RECV_TIMEOUT: Duration = Duration::from_secs(60);

use crate::tracing::debug_traceparent;

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
        let traceparent = debug_traceparent!(&request);
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
        let traceparent = debug_traceparent!(&request);
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
        let collected = receive_with_timeout(&mut stream, INSTALL_SNAPSHOT_RECV_TIMEOUT).await?;

        tracing::debug!(
            group_id = collected.group_id,
            num_chunks = collected.num_chunks,
            total_bytes = collected.total_bytes,
            "install_snapshot RPC received"
        );

        let payload = self
            .groups
            .install_snapshot(collected.group_id, collected.metadata, collected.data)
            .await?;

        Ok(tonic::Response::new(SnapshotResponse {
            group_id: collected.group_id,
            success: true,
            payload,
        }))
    }
}

/// Wrap [`collect_snapshot_stream`] in an overall receive timeout; on
/// expiry the stream is dropped and the peer gets `DEADLINE_EXCEEDED`.
async fn receive_with_timeout<S>(
    stream: &mut S,
    timeout: Duration,
) -> Result<CollectedSnapshot, tonic::Status>
where
    S: tokio_stream::Stream<Item = Result<SnapshotChunk, tonic::Status>> + Unpin,
{
    match tokio::time::timeout(timeout, collect_snapshot_stream(stream)).await {
        Ok(result) => result,
        Err(_) => {
            tracing::error!(
                timeout_secs = timeout.as_secs(),
                "install_snapshot stream receive timed out; aborting (DoS mitigation)"
            );
            Err(tonic::Status::deadline_exceeded(
                "install_snapshot stream timed out",
            ))
        }
    }
}

/// A fully received `InstallSnapshot` stream.
#[derive(Debug)]
struct CollectedSnapshot {
    group_id: GroupId,
    metadata: Vec<u8>,
    data: Vec<u8>,
    num_chunks: usize,
    total_bytes: usize,
}

/// Drain a `SnapshotChunk` stream into one contiguous buffer.
///
/// Chunks are appended to a single growing `Vec` as they arrive instead
/// of being retained and concatenated at the end, so peak memory is ≈1×
/// (not ≈2×) the snapshot size (modulo amortized-growth reallocation).
/// The running total is capped at [`MAX_INSTALL_SNAPSHOT_BYTES`], and a
/// chunk arriving after `done = true` is rejected — `done` is terminal.
async fn collect_snapshot_stream<S>(stream: &mut S) -> Result<CollectedSnapshot, tonic::Status>
where
    S: tokio_stream::Stream<Item = Result<SnapshotChunk, tonic::Status>> + Unpin,
{
    let mut data: Vec<u8> = Vec::new();
    let mut metadata: Option<Vec<u8>> = None;
    let mut group_id: Option<GroupId> = None;
    let mut num_chunks: usize = 0;
    let mut total_bytes: usize = 0;
    let mut done_seen = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| tonic::Status::internal(format!("stream error: {e}")))?;
        if done_seen {
            return Err(tonic::Status::invalid_argument(
                "protocol violation: chunk received after done = true (done is terminal)",
            ));
        }
        total_bytes = total_bytes.saturating_add(chunk.data.len());
        if total_bytes > MAX_INSTALL_SNAPSHOT_BYTES {
            tracing::error!(
                group_id = chunk.group_id,
                total_bytes,
                cap = MAX_INSTALL_SNAPSHOT_BYTES,
                "install_snapshot stream exceeded byte cap; aborting (DoS mitigation)"
            );
            return Err(tonic::Status::resource_exhausted(
                "install_snapshot stream exceeded server byte cap",
            ));
        }
        group_id.get_or_insert(chunk.group_id);
        data.extend_from_slice(&chunk.data);
        num_chunks += 1;
        if chunk.done {
            done_seen = true;
            metadata = Some(chunk.metadata);
        }
    }

    // `group_id` is set by the first chunk, so a missing group_id means
    // the stream was empty.
    let Some(group_id) = group_id else {
        return Err(tonic::Status::invalid_argument(
            "empty snapshot stream: no chunks received",
        ));
    };
    let metadata = metadata.ok_or_else(|| {
        tonic::Status::invalid_argument("no final chunk (done=true) in snapshot stream")
    })?;

    Ok(CollectedSnapshot {
        group_id,
        metadata,
        data,
        num_chunks,
        total_bytes,
    })
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

    fn chunk(group_id: u64, data: &[u8], done: bool, metadata: &[u8]) -> SnapshotChunk {
        SnapshotChunk {
            group_id,
            offset: 0,
            data: data.to_vec(),
            done,
            metadata: metadata.to_vec(),
        }
    }

    #[tokio::test]
    async fn collect_snapshot_stream_concatenates_chunks_in_order() {
        let mut stream = tokio_stream::iter(vec![
            Ok(chunk(7, b"aa", false, &[])),
            Ok(chunk(7, b"bb", false, &[])),
            Ok(chunk(7, b"cc", true, b"meta")),
        ]);
        let collected = collect_snapshot_stream(&mut stream).await.unwrap();
        assert_eq!(collected.group_id, 7);
        assert_eq!(collected.data, b"aabbcc");
        assert_eq!(collected.metadata, b"meta");
        assert_eq!(collected.num_chunks, 3);
        assert_eq!(collected.total_bytes, 6);
    }

    #[tokio::test]
    async fn collect_snapshot_stream_rejects_chunk_after_done() {
        // Regression: `done` must be terminal — a sender that keeps
        // streaming after the final chunk can otherwise grow the retained
        // buffer without bound within the 4 GiB cap.
        let mut stream = tokio_stream::iter(vec![
            Ok(chunk(7, b"aa", true, b"meta")),
            Ok(chunk(7, b"late", false, &[])),
        ]);
        let err = collect_snapshot_stream(&mut stream).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("done"),
            "expected done-terminal error, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn collect_snapshot_stream_rejects_empty_stream() {
        let mut stream = tokio_stream::iter(Vec::<Result<SnapshotChunk, tonic::Status>>::new());
        let err = collect_snapshot_stream(&mut stream).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("empty snapshot stream"));
    }

    // INV-5: a snapshot is the consistent state at one log index; install fully replaces state, never merges.
    #[tokio::test]
    async fn collect_snapshot_stream_rejects_missing_done_chunk() {
        let mut stream = tokio_stream::iter(vec![Ok(chunk(7, b"aa", false, &[]))]);
        let err = collect_snapshot_stream(&mut stream).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("no final chunk"));
    }

    // INV-5: a snapshot is the consistent state at one log index; install fully replaces state, never merges.
    #[tokio::test]
    async fn receive_with_timeout_aborts_stalled_stream() {
        // Regression: a stalled peer stream must not pin the connection
        // forever — the overall receive timeout bounds it.
        let mut stream = tokio_stream::pending::<Result<SnapshotChunk, tonic::Status>>();
        let err = receive_with_timeout(&mut stream, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    }
}
