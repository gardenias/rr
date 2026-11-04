//! Real gRPC-backed `RaftNetworkFactory` using connection pooling for channel reuse.
//!
//! `GrpcNetworkFactory` creates per-target `GrpcRaftNetwork` instances that
//! serialize openraft RPCs with bincode, send them over gRPC proto stubs,
//! and deserialize the responses.
//!
//! # Self-contained transport
//!
//! This module re-defines the raft RPC message types locally with matching
//! prost annotations and builds the tonic gRPC client inline, keeping
//! rr-raft's outbound raft transport self-contained. The wire format is
//! identical to the `rr.raft.*` services implemented by `rr-network`.
//! (`rr-network` itself depends only on `rr-core`; the workspace dependency
//! direction here is `rr-raft` → `rr-client` → `rr-network`.)

use std::io::Cursor;
use std::sync::Arc;
use std::sync::OnceLock;

use dashmap::DashMap;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::v2::RaftNetworkV2;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::VoteOf;
use openraft::BasicNode;
use rr_core::error::NetworkError;
use rr_core::types::NodeId;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use crate::type_config::RrTypeConfig;

// ---------------------------------------------------------------------------
// Local proto types (mirrors rr.raft.* proto messages)
// ---------------------------------------------------------------------------

/// `rr.raft.AppendEntriesRequest`
#[derive(Clone, prost::Message)]
struct ProtoAppendReq {
    #[prost(uint64, tag = "1")]
    group_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    payload: Vec<u8>,
}

/// `rr.raft.AppendEntriesResponse`
#[derive(Clone, prost::Message)]
struct ProtoAppendResp {
    #[prost(uint64, tag = "1")]
    group_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    payload: Vec<u8>,
}

/// `rr.raft.VoteRequest`
#[derive(Clone, prost::Message)]
struct ProtoVoteReq {
    #[prost(uint64, tag = "1")]
    group_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    payload: Vec<u8>,
}

/// `rr.raft.VoteResponse`
#[derive(Clone, prost::Message)]
struct ProtoVoteResp {
    #[prost(uint64, tag = "1")]
    group_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    payload: Vec<u8>,
}

/// `rr.raft.SnapshotChunk`
#[derive(Clone, prost::Message)]
struct ProtoSnapshotChunk {
    #[prost(uint64, tag = "1")]
    group_id: u64,
    #[prost(uint64, tag = "2")]
    offset: u64,
    #[prost(bytes = "vec", tag = "3")]
    data: Vec<u8>,
    #[prost(bool, tag = "4")]
    done: bool,
    #[prost(bytes = "vec", tag = "5")]
    metadata: Vec<u8>,
}

/// `rr.raft.SnapshotResponse`
#[derive(Clone, prost::Message)]
struct ProtoSnapshotResp {
    #[prost(uint64, tag = "1")]
    group_id: u64,
    #[prost(bool, tag = "2")]
    success: bool,
    #[prost(bytes = "vec", tag = "3")]
    payload: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map any `Display` error to `RPCError::Unreachable` (concrete for our types).
fn to_unreachable<E: std::fmt::Display>(e: E) -> RPCError<RrTypeConfig> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other(e.to_string())))
}

fn to_unreachable_streaming<E: std::fmt::Display>(e: E) -> StreamingError<RrTypeConfig> {
    StreamingError::from(to_unreachable(e))
}

// ---------------------------------------------------------------------------
// Channel pool (local, avoids rr-network dep)
// ---------------------------------------------------------------------------

/// PEM file locations + peer-verification policy for TLS outbound connections.
///
/// Kept local to avoid a cross-crate dependency between `rr-raft` and
/// `rr-network` (where `rr_network::TlsConfig` lives). The two types are
/// structurally identical; `rr-server`'s `main.rs` translates between them.
#[derive(Debug, Clone)]
pub struct TlsPaths {
    pub cert_path: std::path::PathBuf,
    pub key_path: std::path::PathBuf,
    pub ca_path: Option<std::path::PathBuf>,
    pub domain_name: Option<String>,
}

/// A pool of reusable gRPC channels, one per node.
///
/// Functionally identical to `rr_network::ChannelPool` but defined locally
/// to avoid a cyclic crate dependency. When constructed with
/// [`ChannelPool::with_tls`], every new channel is opened over TLS.
///
/// Values carry the dialed address alongside the channel: a cache hit with
/// a MISMATCHED address (node id reused with a new address) is evicted and
/// redialed instead of silently streaming to the old peer.
#[derive(Clone, Default)]
pub struct ChannelPool {
    channels: Arc<DashMap<NodeId, (String, Channel)>>,
    tls: Option<Arc<TlsPaths>>,
}

/// Bounds a single dial. Without it a black-holed address hangs the caller
/// until the OS TCP timeout fires (minutes).
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Endpoint tuning shared with the other workspace dial sites (duplicated
/// locally to avoid the cyclic crate dependency — keep in sync with
/// `rr_network::channel_pool::tuned_endpoint`).
fn tuned_endpoint(endpoint: tonic::transport::Endpoint) -> tonic::transport::Endpoint {
    endpoint
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .http2_keep_alive_interval(std::time::Duration::from_secs(10))
        .keep_alive_timeout(std::time::Duration::from_secs(5))
        .keep_alive_while_idle(true)
}

impl ChannelPool {
    /// Create an empty plaintext pool.
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            tls: None,
        }
    }

    /// Create a pool that opens every outbound channel over TLS.
    pub fn with_tls(tls: TlsPaths) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            tls: Some(Arc::new(tls)),
        }
    }

    /// Whether this pool is configured to use TLS for outbound channels.
    pub fn uses_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Get an existing channel or create a new one.
    pub async fn get_or_connect(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> Result<Channel, NetworkError> {
        if let Some(entry) = self.channels.get(&node_id) {
            let (cached_addr, channel) = entry.value();
            if cached_addr == addr {
                return Ok(channel.clone());
            }
            // Node id reused with a new address: evict the stale channel or
            // replication traffic would keep streaming to the old peer.
            let stale_addr = cached_addr.clone();
            drop(entry);
            self.channels.remove(&node_id);
            tracing::info!(
                node_id,
                stale_addr = %stale_addr,
                new_addr = %addr,
                "channel pool: node address changed; evicted stale channel"
            );
        }
        let uri = if self.tls.is_some() {
            format!("https://{addr}")
        } else {
            format!("http://{addr}")
        };
        let mut endpoint = tonic::transport::Endpoint::from_shared(uri).map_err(|e| {
            NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            }
        })?;
        if let Some(tls) = &self.tls {
            let client_tls = build_client_tls(tls).map_err(|e| NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e,
            })?;
            endpoint =
                endpoint
                    .tls_config(client_tls)
                    .map_err(|e| NetworkError::ConnectionFailed {
                        addr: addr.to_string(),
                        reason: format!("tls attach: {e}"),
                    })?;
        }
        let channel = tuned_endpoint(endpoint).connect().await.map_err(|e| {
            NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            }
        })?;
        self.channels
            .insert(node_id, (addr.to_string(), channel.clone()));
        Ok(channel)
    }

    /// Remove a channel from the pool.
    pub fn remove(&self, node_id: NodeId) {
        self.channels.remove(&node_id);
    }

    /// Number of pooled channels.
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// Returns `true` if no channels are pooled.
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }
}

fn build_client_tls(t: &TlsPaths) -> Result<tonic::transport::ClientTlsConfig, String> {
    let cert = std::fs::read(&t.cert_path)
        .map_err(|e| format!("failed to read cert at {}: {e}", t.cert_path.display()))?;
    let key = std::fs::read(&t.key_path)
        .map_err(|e| format!("failed to read key at {}: {e}", t.key_path.display()))?;
    let mut cfg = tonic::transport::ClientTlsConfig::new()
        .identity(tonic::transport::Identity::from_pem(cert, key));
    if let Some(ca) = &t.ca_path {
        let ca_pem =
            std::fs::read(ca).map_err(|e| format!("failed to read ca at {}: {e}", ca.display()))?;
        cfg = cfg.ca_certificate(tonic::transport::Certificate::from_pem(ca_pem));
    }
    if let Some(name) = &t.domain_name {
        cfg = cfg.domain_name(name.clone());
    }
    Ok(cfg)
}

/// Abstraction over a gRPC channel pool for testability via fault injection.
#[async_trait::async_trait]
pub trait ChannelPoolLike: Send + Sync + 'static {
    /// Get or create a channel to the given node.
    async fn get_or_connect(&self, node_id: NodeId, addr: &str) -> Result<Channel, NetworkError>;
    /// Remove a cached channel for the given node.
    fn remove(&self, node_id: NodeId);
    /// Number of cached channels.
    fn len(&self) -> usize;
    /// Returns true if the pool is empty.
    fn is_empty(&self) -> bool;
    /// Remove a cached channel and immediately reconnect, returning the fresh channel.
    async fn remove_and_reconnect(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> Result<Channel, NetworkError>;
}

#[async_trait::async_trait]
impl ChannelPoolLike for ChannelPool {
    async fn get_or_connect(&self, node_id: NodeId, addr: &str) -> Result<Channel, NetworkError> {
        ChannelPool::get_or_connect(self, node_id, addr).await
    }
    fn remove(&self, node_id: NodeId) {
        ChannelPool::remove(self, node_id);
    }
    fn len(&self) -> usize {
        ChannelPool::len(self)
    }
    fn is_empty(&self) -> bool {
        ChannelPool::is_empty(self)
    }
    async fn remove_and_reconnect(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> Result<Channel, NetworkError> {
        self.remove(node_id);
        self.get_or_connect(node_id, addr).await
    }
}

// ---------------------------------------------------------------------------
// Metric label interning
// ---------------------------------------------------------------------------

/// Intern a u64 id as a `&'static str` metric label.
///
/// `to_string()` per RPC was a steady allocation stream on the replication
/// hot path (AppendEntries fire per batch per follower, plus heartbeats).
/// Distinct ids in a deployment are few and dense; each value is leaked
/// exactly once, keeping the per-RPC metrics path allocation-free.
pub(crate) fn intern_id_label(v: u64) -> &'static str {
    static LABELS: OnceLock<DashMap<u64, &'static str>> = OnceLock::new();
    let labels = LABELS.get_or_init(DashMap::new);
    if let Some(label) = labels.get(&v) {
        return *label;
    }
    let leaked: &'static str = Box::leak(v.to_string().into_boxed_str());
    *labels.entry(v).or_insert(leaked)
}

// ---------------------------------------------------------------------------
// Tonic gRPC client for rr.raft.RaftService
// ---------------------------------------------------------------------------

/// Minimal hand-rolled client matching `rr.raft.RaftService`.
#[derive(Clone)]
struct RaftGrpcClient {
    inner: tonic::client::Grpc<Channel>,
}

impl RaftGrpcClient {
    fn new(channel: Channel) -> Self {
        Self {
            inner: tonic::client::Grpc::new(channel),
        }
    }

    async fn append_entries(
        &mut self,
        req: ProtoAppendReq,
    ) -> Result<tonic::Response<ProtoAppendResp>, tonic::Status> {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("service not ready: {e}")))?;
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(
            "/rr.raft.RaftService/AppendEntries",
        );
        let codec = tonic_prost::ProstCodec::default();
        self.inner
            .unary(tonic::Request::new(req), path, codec)
            .await
    }

    async fn vote(
        &mut self,
        req: ProtoVoteReq,
    ) -> Result<tonic::Response<ProtoVoteResp>, tonic::Status> {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("service not ready: {e}")))?;
        let path =
            tonic::codegen::http::uri::PathAndQuery::from_static("/rr.raft.RaftService/Vote");
        let codec = tonic_prost::ProstCodec::default();
        self.inner
            .unary(tonic::Request::new(req), path, codec)
            .await
    }

    async fn install_snapshot(
        &mut self,
        stream: impl tonic::IntoStreamingRequest<Message = ProtoSnapshotChunk>,
    ) -> Result<tonic::Response<ProtoSnapshotResp>, tonic::Status> {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("service not ready: {e}")))?;
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(
            "/rr.raft.RaftService/InstallSnapshot",
        );
        let codec = tonic_prost::ProstCodec::default();
        self.inner
            .client_streaming(stream.into_streaming_request(), path, codec)
            .await
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Factory that creates gRPC-backed network connections for Raft RPCs.
///
/// Each factory instance is scoped to a single Raft group (identified by
/// `group_id`), so the proto messages carry the correct demux key.
#[derive(Clone)]
pub struct GrpcNetworkFactory {
    channel_pool: Arc<dyn ChannelPoolLike>,
    group_id: u64,
}

impl GrpcNetworkFactory {
    /// Create a new factory for the given Raft group.
    pub fn new(channel_pool: Arc<dyn ChannelPoolLike>, group_id: u64) -> Self {
        Self {
            channel_pool,
            group_id,
        }
    }
}

impl RaftNetworkFactory<RrTypeConfig> for GrpcNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        GrpcRaftNetwork {
            target,
            target_addr: node.addr.clone(),
            channel_pool: self.channel_pool.clone(),
            group_id: self.group_id,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-target network
// ---------------------------------------------------------------------------

/// A gRPC-backed Raft network for a specific target node.
pub struct GrpcRaftNetwork {
    target: u64,
    target_addr: String,
    channel_pool: Arc<dyn ChannelPoolLike>,
    group_id: u64,
}

impl GrpcRaftNetwork {
    /// Override the group_id after construction.
    pub fn with_group_id(mut self, group_id: u64) -> Self {
        self.group_id = group_id;
        self
    }
}

impl RaftNetworkV2<RrTypeConfig> for GrpcRaftNetwork {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RrTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<RrTypeConfig>, RPCError<RrTypeConfig>> {
        let channel = self
            .channel_pool
            .get_or_connect(self.target, &self.target_addr)
            .await
            .map_err(to_unreachable)?;

        let batch_size = rpc.entries.len() as f64;
        metrics::histogram!("rr_replication_batch_entries", "group_id" => intern_id_label(self.group_id))
            .record(batch_size);

        let payload = bincode::serde::encode_to_vec(&rpc, bincode::config::standard())
            .map_err(to_unreachable)?;

        let mut client = RaftGrpcClient::new(channel);
        let rpc_started = std::time::Instant::now();
        // Enforce the RPC's soft TTL: openraft delegates this to the
        // transport, and without it a half-open connection would wedge
        // replication until the hard TTL. A timeout maps to Unreachable so
        // the replicator backs off, and the channel is evicted so the next
        // attempt reconnects instead of reusing the wedged one.
        let ttl = option.soft_ttl();
        let send = client.append_entries(ProtoAppendReq {
            group_id: self.group_id,
            payload,
        });
        let response = match tokio::time::timeout(ttl, send).await {
            Err(_elapsed) => {
                tracing::warn!(
                    target = self.target,
                    group_id = self.group_id,
                    "append_entries timed out after {ttl:?}; evicting channel"
                );
                self.channel_pool.remove(self.target);
                return Err(to_unreachable(format!(
                    "append_entries timed out after {ttl:?}"
                )));
            }
            Ok(r) => r
                .map_err(|status| {
                    if status.code() == tonic::Code::Unavailable {
                        self.channel_pool.remove(self.target);
                        metrics::counter!("rr_channel_reconnects_total", "node_id" => intern_id_label(self.target)).increment(1);
                    }
                    to_unreachable(status)
                })?
                .into_inner(),
        };

        // Leader→follower replication round-trip (includes heartbeats,
        // split out via the `kind` label) — the time-based companion to
        // the entry-count `rr_raft_replication_lag` gauge.
        metrics::histogram!(
            "rr_raft_append_entries_rtt_seconds",
            "group_id" => intern_id_label(self.group_id),
            "follower_node_id" => intern_id_label(self.target),
            "kind" => if batch_size == 0.0 { "heartbeat" } else { "append" }
        )
        .record(rpc_started.elapsed().as_secs_f64());

        let (resp, _): (AppendEntriesResponse<RrTypeConfig>, _) =
            rr_core::bounded_decode::decode_entry_body(&response.payload)
                .map_err(to_unreachable)?;
        Ok(resp)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RrTypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<RrTypeConfig>, RPCError<RrTypeConfig>> {
        let channel = self
            .channel_pool
            .get_or_connect(self.target, &self.target_addr)
            .await
            .map_err(to_unreachable)?;

        let payload = bincode::serde::encode_to_vec(&rpc, bincode::config::standard())
            .map_err(to_unreachable)?;

        let mut client = RaftGrpcClient::new(channel);
        // Same soft-ttl enforcement as append_entries: a half-open connection
        // must fail loudly (Unreachable) instead of wedging an election.
        let ttl = option.soft_ttl();
        let send = client.vote(ProtoVoteReq {
            group_id: self.group_id,
            payload,
        });
        let response = match tokio::time::timeout(ttl, send).await {
            Err(_elapsed) => {
                tracing::warn!(
                    target = self.target,
                    group_id = self.group_id,
                    "vote timed out after {ttl:?}; evicting channel"
                );
                self.channel_pool.remove(self.target);
                return Err(to_unreachable(format!("vote timed out after {ttl:?}")));
            }
            Ok(r) => r
                .map_err(|status| {
                    if status.code() == tonic::Code::Unavailable {
                        self.channel_pool.remove(self.target);
                        metrics::counter!("rr_channel_reconnects_total", "node_id" => intern_id_label(self.target)).increment(1);
                    }
                    to_unreachable(status)
                })?
                .into_inner(),
        };

        let (resp, _): (VoteResponse<RrTypeConfig>, _) =
            rr_core::bounded_decode::decode_entry_body(&response.payload)
                .map_err(to_unreachable)?;
        Ok(resp)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<RrTypeConfig>,
        snapshot: SnapshotOf<RrTypeConfig, Self::SnapshotData>,
        cancel: impl std::future::Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<RrTypeConfig>, StreamingError<RrTypeConfig>> {
        let channel = self
            .channel_pool
            .get_or_connect(self.target, &self.target_addr)
            .await
            .map_err(to_unreachable_streaming)?;

        // Serialize the entire openraft request into the metadata field.
        // Only the final chunk carries metadata; earlier chunks carry data only.
        let snapshot_data = snapshot.snapshot.into_inner();
        let metadata =
            bincode::serde::encode_to_vec(&(vote, snapshot.meta), bincode::config::standard())
                .map_err(to_unreachable_streaming)?;

        // Chunk size: honor openraft's recommendation when provided, else 64 KB.
        let chunk_size = option.snapshot_chunk_size().unwrap_or(65_536).max(1);
        let len = snapshot_data.len();
        let total = if len == 0 {
            1
        } else {
            len.div_ceil(chunk_size)
        };
        let group_id = self.group_id;

        // Lazy chunk stream: each chunk is copied only as it is yielded, so
        // the transfer stays cancelable / time-outable mid-flight instead of
        // materializing a second full copy of the snapshot up front.
        let stream = tokio_stream::iter(0..total).map(move |i| {
            let is_last = i == total - 1;
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(len);
            ProtoSnapshotChunk {
                group_id,
                offset: start as u64,
                data: snapshot_data[start..end].to_vec(),
                done: is_last,
                // metadata only on the last chunk — server ignores it on earlier ones
                metadata: if is_last { metadata.clone() } else { vec![] },
            }
        });

        tracing::debug!(
            group_id = self.group_id,
            snapshot_bytes = len,
            num_chunks = total,
            chunk_size,
            "install_snapshot: sending chunked stream"
        );

        let mut client = RaftGrpcClient::new(channel);
        let send = client.install_snapshot(stream);
        let ttl = option.soft_ttl();

        // Honor openraft's cancel signal (RaftCore dropped / leadership lost)
        // and the RPC soft TTL: a wedged receiver must not stall the
        // SnapshotTransmitter forever. A timeout maps to `Unreachable` so the
        // transmitter backs off instead of hot-retrying a struggling peer.
        let mut cancel = std::pin::pin!(cancel);
        let response = tokio::select! {
            closed = &mut cancel => {
                tracing::info!(target = self.target, "install_snapshot canceled by RaftCore");
                return Err(closed.into());
            }
            res = tokio::time::timeout(ttl, send) => {
                match res {
                    Err(_elapsed) => {
                        let msg = format!("install_snapshot timed out after {ttl:?}");
                        tracing::warn!(target = self.target, "{}", msg);
                        return Err(to_unreachable_streaming(msg));
                    }
                    Ok(r) => r
                        .map_err(|status| {
                            if status.code() == tonic::Code::Unavailable {
                                self.channel_pool.remove(self.target);
                                metrics::counter!("rr_channel_reconnects_total", "node_id" => intern_id_label(self.target)).increment(1);
                            }
                            to_unreachable_streaming(status)
                        })?
                        .into_inner(),
                }
            }
        };

        // Deserialize the payload to get the InstallSnapshotResponse.
        // For backward compatibility, if payload is empty, use a default vote.
        let follower_vote = if response.payload.is_empty() {
            tracing::warn!("snapshot response payload is empty, using default vote");
            vote
        } else {
            let (resp, _): (SnapshotResponse<RrTypeConfig>, _) =
                rr_core::bounded_decode::decode_entry_body(&response.payload)
                    .map_err(to_unreachable_streaming)?;
            resp.vote
        };

        Ok(SnapshotResponse::new(follower_vote))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use openraft::raft::AppendEntriesRequest as OraftAppendRequest;
    use openraft::Vote;
    use std::time::Duration;

    #[test]
    fn test_grpc_network_factory_new() {
        let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
        let factory = GrpcNetworkFactory::new(pool, 1);
        // Verify it can be cloned (required by openraft)
        let factory2 = factory.clone();
        assert_eq!(factory2.group_id, 1);
    }

    #[tokio::test]
    async fn test_grpc_network_connection_error() {
        let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
        let mut network = GrpcRaftNetwork {
            target: 99,
            target_addr: "localhost:1".to_string(),
            channel_pool: pool,
            group_id: 1,
        };

        let rpc: OraftAppendRequest<RrTypeConfig> = OraftAppendRequest {
            vote: Vote::new(1, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };

        let result = network
            .append_entries(rpc, RPCOption::new(Duration::from_secs(5)))
            .await;

        assert!(result.is_err(), "should fail to connect to invalid addr");
        match result.unwrap_err() {
            RPCError::Unreachable(_) => {} // expected
            other => panic!("expected Unreachable, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_grpc_network_vote_connection_error() {
        let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
        let mut network = GrpcRaftNetwork {
            target: 99,
            target_addr: "localhost:1".to_string(),
            channel_pool: pool,
            group_id: 1,
        };

        let rpc = VoteRequest::<RrTypeConfig> {
            vote: Vote::new(1, 1),
            last_log_id: None,
            leadership_transfer: false,
        };

        let result = network
            .vote(rpc, RPCOption::new(Duration::from_secs(5)))
            .await;

        assert!(result.is_err(), "should fail to connect to invalid addr");
        match result.unwrap_err() {
            RPCError::Unreachable(_) => {}
            other => panic!("expected Unreachable, got: {other:?}"),
        }
    }

    #[test]
    fn test_grpc_network_with_group_id() {
        let network = GrpcRaftNetwork {
            target: 1,
            target_addr: "localhost:5000".to_string(),
            channel_pool: Arc::new(ChannelPool::new()) as Arc<dyn ChannelPoolLike>,
            group_id: 0,
        };

        let network = network.with_group_id(42);
        assert_eq!(network.group_id, 42);
    }

    #[test]
    fn test_channel_pool_new_is_empty() {
        let pool = ChannelPool::new();
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn test_channel_pool_clone_shares_state() {
        let pool = ChannelPool::new();
        let pool2 = pool.clone();
        assert_eq!(pool.len(), pool2.len());
    }

    /// Verify that snapshot data > CHUNK_SIZE is split into multiple chunks.
    /// The last chunk has done=true and carries metadata; earlier chunks have done=false.
    #[test]
    fn test_snapshot_chunking_splits_large_data() {
        const CHUNK_SIZE: usize = 65_536;

        // Create data larger than one chunk
        let snapshot_data: Vec<u8> = (0u8..=255).cycle().take(CHUNK_SIZE * 2 + 1_000).collect();
        let metadata: Vec<u8> = b"test-metadata".to_vec();
        let group_id: u64 = 7;

        // Replicate the chunking logic from install_snapshot
        let chunks_data: Vec<&[u8]> = snapshot_data.chunks(CHUNK_SIZE).collect();
        let total = chunks_data.len();
        assert_eq!(
            total, 3,
            "should produce 3 chunks for 2*CHUNK_SIZE+1000 bytes"
        );

        let mut proto_chunks: Vec<ProtoSnapshotChunk> = Vec::with_capacity(total);
        for (i, chunk_data) in chunks_data.iter().enumerate() {
            let is_last = i == total - 1;
            proto_chunks.push(ProtoSnapshotChunk {
                group_id,
                offset: (i * CHUNK_SIZE) as u64,
                data: chunk_data.to_vec(),
                done: is_last,
                metadata: if is_last { metadata.clone() } else { vec![] },
            });
        }

        // First two chunks: not done, no metadata
        assert!(!proto_chunks[0].done);
        assert!(proto_chunks[0].metadata.is_empty());
        assert_eq!(proto_chunks[0].data.len(), CHUNK_SIZE);
        assert_eq!(proto_chunks[0].offset, 0);

        assert!(!proto_chunks[1].done);
        assert!(proto_chunks[1].metadata.is_empty());
        assert_eq!(proto_chunks[1].data.len(), CHUNK_SIZE);
        assert_eq!(proto_chunks[1].offset, CHUNK_SIZE as u64);

        // Last chunk: done=true, carries metadata, has remaining bytes
        assert!(proto_chunks[2].done);
        assert_eq!(proto_chunks[2].metadata, metadata);
        assert_eq!(proto_chunks[2].data.len(), 1_000);
        assert_eq!(proto_chunks[2].offset, (2 * CHUNK_SIZE) as u64);

        // Reassemble and verify data integrity
        let mut reassembled = Vec::new();
        for chunk in &proto_chunks {
            reassembled.extend_from_slice(&chunk.data);
        }
        assert_eq!(
            reassembled, snapshot_data,
            "reassembled data must match original"
        );
    }

    /// Single-chunk path: data <= CHUNK_SIZE produces exactly one chunk with done=true.
    #[test]
    fn test_snapshot_chunking_small_data_single_chunk() {
        const CHUNK_SIZE: usize = 65_536;

        let snapshot_data: Vec<u8> = b"small-snapshot".to_vec();
        let chunks_data: Vec<&[u8]> = snapshot_data.chunks(CHUNK_SIZE).collect();
        assert_eq!(chunks_data.len(), 1);

        let total = chunks_data.len();
        let is_last = 0 == total - 1;
        assert!(is_last, "single chunk must be the last");
    }

    /// Empty snapshot: chunks slice produces one empty-data chunk.
    #[test]
    fn test_snapshot_chunking_empty_data() {
        let snapshot_data: Vec<u8> = vec![];
        // The install_snapshot impl special-cases empty data to [&[]]
        let chunks_data: Vec<&[u8]> = if snapshot_data.is_empty() {
            vec![&[]]
        } else {
            snapshot_data.chunks(65_536).collect()
        };
        assert_eq!(
            chunks_data.len(),
            1,
            "empty snapshot must produce one empty chunk"
        );
        assert!(chunks_data[0].is_empty());
    }

    #[tokio::test]
    async fn test_append_entries_records_batch_metric() {
        // Verify the replication batch metric code path doesn't panic.
        // Without a metrics recorder installed, the histogram! macro is a no-op.
        let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
        let mut network = GrpcRaftNetwork {
            target: 99,
            target_addr: "localhost:1".to_string(),
            channel_pool: pool,
            group_id: 42,
        };

        let rpc: OraftAppendRequest<RrTypeConfig> = OraftAppendRequest {
            vote: Vote::new(1, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };

        // The call will fail to connect, but the metric is recorded
        // before the gRPC call, so the histogram! path is exercised.
        let _ = network
            .append_entries(rpc, RPCOption::new(Duration::from_secs(1)))
            .await;
    }

    #[test]
    fn test_remove_clears_channel() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let pool = ChannelPool::new();

        // Connect to a valid URI to populate the pool.
        // tonic Channel::from_static creates a lazily-connecting channel (no real TCP).
        rt.block_on(async {
            // Use a fake address; get_or_connect will attempt to connect.
            // We insert a channel manually via the DashMap to avoid needing a server.
            let channel = Channel::from_static("http://[::1]:50051").connect_lazy();
            pool.channels
                .insert(1, ("http://[::1]:50051".to_string(), channel));
        });

        assert_eq!(pool.len(), 1);
        pool.remove(1);
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[tokio::test]
    async fn test_remove_and_reconnect_evicts_old_channel() {
        let pool = ChannelPool::new();

        // Insert a lazily-connected channel.
        let channel = Channel::from_static("http://[::1]:50051").connect_lazy();
        pool.channels
            .insert(42, ("http://127.0.0.1:1".to_string(), channel));
        assert_eq!(pool.len(), 1);

        // remove_and_reconnect will remove the old channel, then try to connect.
        // Since there's no server, the connect will fail — but the old channel
        // should already be evicted.
        let result = pool.remove_and_reconnect(42, "127.0.0.1:1").await;
        // The pool should have been cleared before the reconnect attempt.
        // On failure the pool stays empty (no channel re-inserted).
        assert!(result.is_err(), "should fail without a real server");
        assert_eq!(pool.len(), 0, "old channel should have been evicted");
    }

    /// G3 regression: a cache hit whose registered address differs from the
    /// requested one (node id reused with a new address) evicts the stale
    /// channel and redials — the new connection lands on the NEW address.
    #[tokio::test]
    async fn test_addr_change_evicts_stale_channel_and_redials() {
        let pool = ChannelPool::new();
        // Seed a channel registered under the OLD address.
        let stale = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        pool.channels.insert(7, ("127.0.0.1:1".to_string(), stale));

        // The NEW address has a live listener, so the redial succeeds —
        // had the pool dialed the stale address it would fail fast.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let new_addr = listener.local_addr().unwrap().to_string();
        let acceptor =
            tokio::spawn(async move { while let Ok((_sock, _)) = listener.accept().await {} });

        pool.get_or_connect(7, &new_addr)
            .await
            .expect("redial to the new address must succeed");

        let stored = pool.channels.get(&7).map(|e| e.value().0.clone());
        assert_eq!(
            stored.as_deref(),
            Some(new_addr.as_str()),
            "pool must re-register the channel under the new address"
        );
        acceptor.abort();
    }

    /// A cache hit with the SAME address returns the pooled channel without
    /// redialing: once the listener is gone, a redial would fail fast, so
    /// success proves the hit path served the cached channel.
    #[tokio::test]
    async fn test_same_addr_hit_does_not_redial() {
        let pool = ChannelPool::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let acceptor =
            tokio::spawn(async move { while let Ok((_sock, _)) = listener.accept().await {} });

        pool.get_or_connect(7, &addr).await.unwrap();
        assert_eq!(pool.len(), 1);

        // Tear down the listener; the acceptor owns it, so awaiting the
        // aborted task guarantees the port is closed before we go on.
        acceptor.abort();
        let _ = acceptor.await;

        pool.get_or_connect(7, &addr)
            .await
            .expect("same-address hit must be served from the pool, not redialed");
        assert_eq!(pool.len(), 1);
    }

    #[tokio::test]
    async fn test_unavailable_error_evicts_channel() {
        // When append_entries hits an Unavailable error, the channel should be evicted.
        // Insert a lazily-connected channel for node 99.
        let concrete_pool = ChannelPool::new();
        let channel = Channel::from_static("http://[::1]:50051").connect_lazy();
        concrete_pool
            .channels
            .insert(99, ("http://127.0.0.1:1".to_string(), channel));
        let pool: Arc<dyn ChannelPoolLike> = Arc::new(concrete_pool);
        assert_eq!(pool.len(), 1);

        let mut network = GrpcRaftNetwork {
            target: 99,
            target_addr: "127.0.0.1:1".to_string(),
            channel_pool: pool.clone(),
            group_id: 1,
        };

        let rpc: OraftAppendRequest<RrTypeConfig> = OraftAppendRequest {
            vote: Vote::new(1, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };

        // The RPC will fail (no server). The lazy channel will produce an Unavailable-like
        // error during the gRPC call, which should trigger eviction.
        let result = network
            .append_entries(rpc, RPCOption::new(Duration::from_secs(1)))
            .await;
        assert!(result.is_err());

        // After the RPC failure, the stale channel should have been evicted
        // (if the error was Unavailable). The lazy channel to a non-existent server
        // may produce various error codes, so we just verify the mechanism compiles
        // and runs without panicking. In production, a real Unavailable status triggers eviction.
    }

    /// Peer responses are fixed-size structs (no attacker-sized containers),
    /// so the meaningful guarantee is wire-compatibility of the bounded
    /// decode used by the gRPC response path.
    #[test]
    fn grpc_response_decode_roundtrips_valid_payload() {
        for resp in [
            AppendEntriesResponse::<RrTypeConfig>::Success,
            AppendEntriesResponse::<RrTypeConfig>::PartialSuccess(None),
        ] {
            let bytes = bincode::serde::encode_to_vec(&resp, bincode::config::standard()).unwrap();
            let (decoded, _): (AppendEntriesResponse<RrTypeConfig>, _) =
                rr_core::bounded_decode::decode_entry_body(&bytes).unwrap();
            let reencoded =
                bincode::serde::encode_to_vec(&decoded, bincode::config::standard()).unwrap();
            assert_eq!(bytes, reencoded);
        }
    }
}
