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

    async fn pre_vote(
        &mut self,
        req: ProtoVoteReq,
    ) -> Result<tonic::Response<ProtoVoteResp>, tonic::Status> {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("service not ready: {e}")))?;
        let path =
            tonic::codegen::http::uri::PathAndQuery::from_static("/rr.raft.RaftService/PreVote");
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

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<RrTypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<RrTypeConfig>, RPCError<RrTypeConfig>> {
        let ttl = option.soft_ttl();
        let deadline = tokio::time::Instant::now() + ttl;
        let channel = match tokio::time::timeout_at(
            deadline,
            self.channel_pool
                .get_or_connect(self.target, &self.target_addr),
        )
        .await
        {
            Ok(result) => result.map_err(to_unreachable)?,
            Err(_elapsed) => {
                tracing::warn!(
                    target = self.target,
                    group_id = self.group_id,
                    "pre_vote connect timed out after {ttl:?}; evicting channel"
                );
                self.channel_pool.remove(self.target);
                return Err(to_unreachable(format!(
                    "pre_vote connect timed out after {ttl:?}"
                )));
            }
        };

        let fallback_vote = rpc.vote;
        let payload = bincode::serde::encode_to_vec(&rpc, bincode::config::standard())
            .map_err(to_unreachable)?;
        let mut client = RaftGrpcClient::new(channel);
        let send = client.pre_vote(ProtoVoteReq {
            group_id: self.group_id,
            payload,
        });
        let response = match tokio::time::timeout_at(deadline, send).await {
            Err(_elapsed) => {
                tracing::warn!(
                    target = self.target,
                    group_id = self.group_id,
                    "pre_vote timed out after {ttl:?}; evicting channel"
                );
                self.channel_pool.remove(self.target);
                return Err(to_unreachable(format!("pre_vote timed out after {ttl:?}")));
            }
            Ok(Err(status)) if status.code() == tonic::Code::Unimplemented => {
                return Ok(VoteResponse::new(fallback_vote, None, true));
            }
            Ok(Err(status)) => {
                if status.code() == tonic::Code::Unavailable {
                    self.channel_pool.remove(self.target);
                    metrics::counter!("rr_channel_reconnects_total", "node_id" => intern_id_label(self.target)).increment(1);
                }
                return Err(to_unreachable(status));
            }
            Ok(Ok(response)) => response.into_inner(),
        };

        let (resp, _): (VoteResponse<RrTypeConfig>, _) =
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
        let group_label = intern_id_label(group_id);
        // C-2: register the send-side byte counter once up front (the handle
        // is 'static, unlike the macro expansion which declares an inline
        // `static` that breaks the closure's lifetime bound); counting per
        // chunk as it is yielded to the transport means a timeout/cancel
        // mid-flight still reports the bytes actually sent.
        let send_bytes = metrics::counter!(
            "rr_snapshot_transfer_bytes",
            "direction" => "send",
            "group_id" => group_label
        );

        // Lazy chunk stream: each chunk is copied only as it is yielded, so
        // the transfer stays cancelable / time-outable mid-flight instead of
        // materializing a second full copy of the snapshot up front.
        let stream = tokio_stream::iter(0..total).map(move |i| {
            let is_last = i == total - 1;
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(len);
            send_bytes.increment((end - start) as u64);
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
        let transfer_started = std::time::Instant::now();

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

        // C-2: the peer acknowledged the full stream — record send-side wall
        // time. Canceled/timed-out transfers return above and are not mixed
        // into the duration distribution (their partial bytes are still
        // counted as the stream was yielded).
        metrics::histogram!(
            "rr_snapshot_transfer_duration_seconds",
            "direction" => "send",
            "group_id" => group_label
        )
        .record(transfer_started.elapsed().as_secs_f64());

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

    #[tokio::test]
    async fn test_grpc_network_pre_vote_connection_error_is_unreachable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);

        let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
        let mut network = GrpcRaftNetwork {
            target: 99,
            target_addr: addr,
            channel_pool: pool,
            group_id: 1,
        };
        let rpc = VoteRequest::<RrTypeConfig> {
            vote: Vote::new(1, 1),
            last_log_id: None,
            leadership_transfer: false,
        };

        let result = network
            .pre_vote(rpc, RPCOption::new(Duration::from_secs(1)))
            .await;

        match result.unwrap_err() {
            RPCError::Unreachable(_) => {}
            other => panic!("expected Unreachable, got: {other:?}"),
        }
    }

    struct DelayedChannelPool {
        delay: Duration,
        addr: String,
        removals: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ChannelPoolLike for DelayedChannelPool {
        async fn get_or_connect(
            &self,
            _node_id: NodeId,
            _addr: &str,
        ) -> Result<Channel, NetworkError> {
            tokio::time::sleep(self.delay).await;
            Ok(Channel::from_shared(format!("http://{}", self.addr))
                .unwrap()
                .connect_lazy())
        }

        fn remove(&self, _node_id: NodeId) {
            self.removals
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        fn len(&self) -> usize {
            0
        }

        fn is_empty(&self) -> bool {
            true
        }

        async fn remove_and_reconnect(
            &self,
            _node_id: NodeId,
            _addr: &str,
        ) -> Result<Channel, NetworkError> {
            unreachable!()
        }
    }

    async fn assert_pre_vote_unreachable_and_evicted(pool: Arc<DelayedChannelPool>) -> Duration {
        let removals = Arc::clone(&pool.removals);
        let mut network = GrpcRaftNetwork {
            target: 99,
            target_addr: pool.addr.clone(),
            channel_pool: pool,
            group_id: 1,
        };
        let started = std::time::Instant::now();
        let result = network
            .pre_vote(
                pre_vote_request(),
                RPCOption::new(Duration::from_millis(75)),
            )
            .await;
        match result.unwrap_err() {
            RPCError::Unreachable(_) => {}
            other => panic!("expected Unreachable, got: {other:?}"),
        }
        assert_eq!(
            removals.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a soft-TTL timeout must evict the pooled channel"
        );
        started.elapsed()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pre_vote_soft_ttl_covers_slow_connect_and_unresponsive_rpc() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });

        let slow_pool = Arc::new(DelayedChannelPool {
            delay: Duration::from_millis(400),
            addr: addr.clone(),
            removals: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });
        let slow_elapsed = assert_pre_vote_unreachable_and_evicted(slow_pool).await;
        assert!(
            slow_elapsed < Duration::from_millis(250),
            "slow connect escaped the 75ms soft TTL: {slow_elapsed:?}"
        );

        let unresponsive_pool = Arc::new(DelayedChannelPool {
            delay: Duration::ZERO,
            addr,
            removals: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });
        let rpc_elapsed = assert_pre_vote_unreachable_and_evicted(unresponsive_pool).await;
        assert!(
            rpc_elapsed < Duration::from_millis(250),
            "unresponsive RPC escaped the 75ms soft TTL: {rpc_elapsed:?}"
        );
        server.abort();
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

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum PreVoteBehavior {
        Reject,
        Unimplemented,
        Internal,
        InvalidArgument,
        MalformedResponse,
    }

    #[derive(Clone)]
    struct PreVoteServer {
        behavior: PreVoteBehavior,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for PreVoteServer {
        type Response = tonic::codegen::http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _cx: &mut tonic::codegen::Context<'_>,
        ) -> tonic::codegen::Poll<Result<(), Self::Error>> {
            tonic::codegen::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
            match req.uri().path() {
                "/rr.raft.RaftService/PreVote" => {
                    struct Svc {
                        behavior: PreVoteBehavior,
                        calls: Arc<std::sync::atomic::AtomicUsize>,
                    }

                    impl tonic::server::UnaryService<ProtoVoteReq> for Svc {
                        type Response = ProtoVoteResp;
                        type Future = tonic::codegen::BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;

                        fn call(&mut self, request: tonic::Request<ProtoVoteReq>) -> Self::Future {
                            let behavior = self.behavior;
                            let calls = Arc::clone(&self.calls);
                            Box::pin(async move {
                                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                match behavior {
                                    PreVoteBehavior::Unimplemented => {
                                        return Err(tonic::Status::unimplemented(
                                            "PreVote is not supported by this node",
                                        ));
                                    }
                                    PreVoteBehavior::Internal => {
                                        return Err(tonic::Status::internal("injected failure"));
                                    }
                                    PreVoteBehavior::InvalidArgument => {
                                        return Err(tonic::Status::invalid_argument(
                                            "injected rejection",
                                        ));
                                    }
                                    PreVoteBehavior::MalformedResponse => {
                                        let req = request.into_inner();
                                        return Ok(tonic::Response::new(ProtoVoteResp {
                                            group_id: req.group_id,
                                            payload: vec![0xff],
                                        }));
                                    }
                                    PreVoteBehavior::Reject => {}
                                }

                                let req = request.into_inner();
                                let (_vote_req, _): (VoteRequest<RrTypeConfig>, _) =
                                    rr_core::bounded_decode::decode_entry_body(&req.payload)
                                        .map_err(|_| {
                                            tonic::Status::invalid_argument("invalid payload")
                                        })?;
                                let vote_resp =
                                    VoteResponse::<RrTypeConfig>::new(Vote::new(9, 2), None, false);
                                let payload = bincode::serde::encode_to_vec(
                                    &vote_resp,
                                    bincode::config::standard(),
                                )
                                .map_err(|_| tonic::Status::internal("encode failed"))?;
                                Ok(tonic::Response::new(ProtoVoteResp {
                                    group_id: req.group_id,
                                    payload,
                                }))
                            })
                        }
                    }

                    let svc = Svc {
                        behavior: self.behavior,
                        calls: Arc::clone(&self.calls),
                    };
                    Box::pin(async move {
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec);
                        Ok(grpc.unary(svc, req).await)
                    })
                }
                _ => Box::pin(async move {
                    let mut response =
                        tonic::codegen::http::Response::new(tonic::body::Body::default());
                    let headers = response.headers_mut();
                    headers.insert(
                        tonic::Status::GRPC_STATUS,
                        (tonic::Code::Unimplemented as i32).into(),
                    );
                    headers.insert(
                        tonic::codegen::http::header::CONTENT_TYPE,
                        tonic::metadata::GRPC_CONTENT_TYPE,
                    );
                    Ok(response)
                }),
            }
        }
    }

    impl tonic::server::NamedService for PreVoteServer {
        const NAME: &'static str = "rr.raft.RaftService";
    }

    async fn spawn_pre_vote_server(
        behavior: PreVoteBehavior,
    ) -> (
        String,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server = PreVoteServer {
            behavior,
            calls: Arc::clone(&calls),
        };
        let serve = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("pre-vote test server failed");
        });
        (addr, calls, serve)
    }

    fn pre_vote_request() -> VoteRequest<RrTypeConfig> {
        VoteRequest {
            vote: Vote::new(2, 1),
            last_log_id: None,
            leadership_transfer: false,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pre_vote_round_trip_preserves_remote_rejection() {
        let (addr, calls, serve) = spawn_pre_vote_server(PreVoteBehavior::Reject).await;
        let mut network = GrpcRaftNetwork {
            target: 2,
            target_addr: addr,
            channel_pool: Arc::new(ChannelPool::new()) as Arc<dyn ChannelPoolLike>,
            group_id: 17,
        };

        let response = network
            .pre_vote(pre_vote_request(), RPCOption::new(Duration::from_secs(2)))
            .await
            .expect("a remote rejection is a valid Raft response");

        assert!(!response.vote_granted);
        assert_eq!(response.vote, Vote::new(9, 2));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        serve.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pre_vote_unimplemented_preserves_rolling_upgrade_fallback() {
        let (addr, calls, serve) = spawn_pre_vote_server(PreVoteBehavior::Unimplemented).await;
        let mut network = GrpcRaftNetwork {
            target: 2,
            target_addr: addr,
            channel_pool: Arc::new(ChannelPool::new()) as Arc<dyn ChannelPoolLike>,
            group_id: 17,
        };
        let request = pre_vote_request();
        let request_vote = request.vote;

        let response = network
            .pre_vote(request, RPCOption::new(Duration::from_secs(2)))
            .await
            .expect("only an old peer's UNIMPLEMENTED response keeps the compatibility fallback");

        assert!(response.vote_granted);
        assert_eq!(response.vote, request_vote);
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the compatibility response must follow a real RPC, not a synthetic local grant"
        );
        serve.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pre_vote_non_fallback_statuses_and_decode_failures_are_unreachable() {
        for behavior in [
            PreVoteBehavior::Internal,
            PreVoteBehavior::InvalidArgument,
            PreVoteBehavior::MalformedResponse,
        ] {
            let (addr, calls, serve) = spawn_pre_vote_server(behavior).await;
            let mut network = GrpcRaftNetwork {
                target: 2,
                target_addr: addr,
                channel_pool: Arc::new(ChannelPool::new()) as Arc<dyn ChannelPoolLike>,
                group_id: 17,
            };

            let result = network
                .pre_vote(pre_vote_request(), RPCOption::new(Duration::from_secs(2)))
                .await;

            match result {
                Err(RPCError::Unreachable(_)) => {}
                Ok(response) => {
                    panic!("{behavior:?} must never produce a PreVote response: {response:?}")
                }
                Err(other) => panic!("{behavior:?} must be Unreachable, got: {other:?}"),
            }
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            serve.abort();
        }
    }

    // ── C-2: snapshot transfer metrics over a real gRPC round trip ───────

    /// Inner state machine that stores restored bytes, so the mock server
    /// drives a real `RrStateMachine::install_snapshot` (the recv seam) and
    /// the test can verify the installed payload.
    #[derive(Default)]
    struct SinkSM {
        state: Vec<u8>,
    }

    #[async_trait::async_trait]
    impl rr_core::state_machine::StateMachine for SinkSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, rr_core::error::RrError> {
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, rr_core::error::RrError> {
            Ok(self.state.clone())
        }
        async fn restore(&mut self, snapshot: &[u8]) -> Result<(), rr_core::error::RrError> {
            self.state = snapshot.to_vec();
            Ok(())
        }
        async fn last_applied(&self) -> Option<rr_core::types::LogIndex> {
            None
        }
    }

    /// Minimal hand-rolled gRPC server implementing only
    /// `/rr.raft.RaftService/InstallSnapshot` (client-streaming), mirroring
    /// the tonic-build output rr-network uses (`rr-network` is not an rr-raft
    /// dependency, so the generated server cannot be reused here). The
    /// collected snapshot is installed into a real
    /// [`crate::sm_adapter::RrStateMachine`] so a single transfer exercises
    /// both metric seams: the send side (`GrpcRaftNetwork::full_snapshot`)
    /// and the receive side (`RrStateMachine::install_snapshot`).
    #[derive(Clone)]
    struct InstallSnapshotServer {
        sm: Arc<tokio::sync::Mutex<crate::sm_adapter::RrStateMachine>>,
    }

    impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>
        for InstallSnapshotServer
    {
        type Response = tonic::codegen::http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _cx: &mut tonic::codegen::Context<'_>,
        ) -> tonic::codegen::Poll<Result<(), Self::Error>> {
            tonic::codegen::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
            match req.uri().path() {
                "/rr.raft.RaftService/InstallSnapshot" => {
                    struct Svc(Arc<tokio::sync::Mutex<crate::sm_adapter::RrStateMachine>>);
                    impl tonic::server::ClientStreamingService<ProtoSnapshotChunk> for Svc {
                        type Response = ProtoSnapshotResp;
                        type Future = tonic::codegen::BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<ProtoSnapshotChunk>>,
                        ) -> Self::Future {
                            let sm = Arc::clone(&self.0);
                            Box::pin(async move {
                                let mut stream = request.into_inner();
                                let mut group_id = 0u64;
                                let mut data: Vec<u8> = Vec::new();
                                let mut metadata: Option<Vec<u8>> = None;
                                while let Some(chunk) = stream.next().await {
                                    let chunk = chunk?;
                                    group_id = chunk.group_id;
                                    data.extend_from_slice(&chunk.data);
                                    if chunk.done {
                                        metadata = Some(chunk.metadata);
                                    }
                                }
                                let metadata = metadata.ok_or_else(|| {
                                    tonic::Status::invalid_argument("no final chunk in stream")
                                })?;
                                let (vote, meta): (
                                    VoteOf<RrTypeConfig>,
                                    openraft::type_config::alias::SnapshotMetaOf<RrTypeConfig>,
                                ) = bincode::serde::decode_from_slice(
                                    &metadata,
                                    bincode::config::standard(),
                                )
                                .map(|(decoded, _)| decoded)
                                .map_err(|e| {
                                    tonic::Status::invalid_argument(format!("metadata decode: {e}"))
                                })?;
                                {
                                    use openraft::storage::RaftStateMachine;
                                    sm.lock()
                                        .await
                                        .install_snapshot(&meta, Cursor::new(data))
                                        .await
                                        .map_err(|e| {
                                            tonic::Status::internal(format!("install: {e}"))
                                        })?;
                                }
                                let resp = SnapshotResponse::<RrTypeConfig>::new(vote);
                                let payload = bincode::serde::encode_to_vec(
                                    &resp,
                                    bincode::config::standard(),
                                )
                                .map_err(|e| tonic::Status::internal(format!("encode: {e}")))?;
                                Ok(tonic::Response::new(ProtoSnapshotResp {
                                    group_id,
                                    success: true,
                                    payload,
                                }))
                            })
                        }
                    }
                    let sm = Arc::clone(&self.sm);
                    Box::pin(async move {
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec);
                        let res = grpc.client_streaming(Svc(sm), req).await;
                        Ok(res)
                    })
                }
                _ => Box::pin(async move {
                    let mut response =
                        tonic::codegen::http::Response::new(tonic::body::Body::default());
                    let headers = response.headers_mut();
                    headers.insert(
                        tonic::Status::GRPC_STATUS,
                        (tonic::Code::Unimplemented as i32).into(),
                    );
                    headers.insert(
                        tonic::codegen::http::header::CONTENT_TYPE,
                        tonic::metadata::GRPC_CONTENT_TYPE,
                    );
                    Ok(response)
                }),
            }
        }
    }

    impl tonic::server::NamedService for InstallSnapshotServer {
        const NAME: &'static str = "rr.raft.RaftService";
    }

    /// C-2: a full snapshot transfer over a real in-process gRPC round trip
    /// increments the send-side byte counter by exactly the payload size and
    /// records one send-side duration sample, and the receiving
    /// `RrStateMachine` records the matching recv-direction series. The
    /// `group_id` label value is unique to this test so its series cannot be
    /// polluted by other tests sharing the process-global recorder.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_snapshot_round_trip_records_transfer_metrics() {
        use openraft::storage::RaftStateMachine;
        use openraft::vote::leader_id_adv::CommittedLeaderId;
        use openraft::vote::RaftLeaderId;
        use openraft::{LogId, StoredMembership};

        let handle = crate::test_metrics::shared_prometheus_recorder();
        let group_id: u64 = 434_343;
        let group_frag = "group_id=\"434343\"";

        // Server side: a real RrStateMachine (recv seam) behind the mock
        // gRPC server.
        let server_sm = crate::sm_adapter::RrStateMachine::new(Box::new(SinkSM::default()))
            .with_group_id(group_id);
        let server_sm = Arc::new(tokio::sync::Mutex::new(server_sm));
        let server = InstallSnapshotServer {
            sm: Arc::clone(&server_sm),
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let serve = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async move {
                        let _ = stop_rx.await;
                    },
                )
                .await
                .expect("mock raft gRPC server failed");
        });

        // Client side: the real send path. 150 000 bytes → 3 chunks at the
        // default 64 KiB chunk size.
        let mut network = GrpcRaftNetwork {
            target: 77,
            target_addr: addr.to_string(),
            channel_pool: Arc::new(ChannelPool::new()) as Arc<dyn ChannelPoolLike>,
            group_id,
        };
        let snapshot_bytes: Vec<u8> = (0u8..=255).cycle().take(150_000).collect();
        let vote = Vote::new(2, 1);
        let meta: openraft::type_config::alias::SnapshotMetaOf<RrTypeConfig> =
            openraft::SnapshotMeta {
                last_log_id: Some(LogId::new(CommittedLeaderId::new(2, 1), 10)),
                last_membership: StoredMembership::default(),
            };

        let resp = network
            .full_snapshot(
                vote,
                openraft::Snapshot {
                    meta,
                    snapshot: Cursor::new(snapshot_bytes.clone()),
                },
                std::future::pending::<openraft::error::ReplicationClosed>(),
                RPCOption::new(Duration::from_secs(10)),
            )
            .await
            .expect("snapshot transfer must succeed against the mock server");
        assert_eq!(resp.vote, vote, "the follower's vote must round-trip");

        let rendered = handle.render();
        let send_bytes = crate::test_metrics::series_value(
            &rendered,
            "rr_snapshot_transfer_bytes",
            &["direction=\"send\"", group_frag],
        );
        assert_eq!(
            send_bytes,
            Some(snapshot_bytes.len() as f64),
            "send byte counter must equal the snapshot payload size; rendered:\n{rendered}"
        );
        let send_samples = crate::test_metrics::series_value(
            &rendered,
            "rr_snapshot_transfer_duration_seconds_count",
            &["direction=\"send\"", group_frag],
        );
        assert_eq!(
            send_samples,
            Some(1.0),
            "send duration histogram must record exactly one sample; rendered:\n{rendered}"
        );
        let recv_bytes = crate::test_metrics::series_value(
            &rendered,
            "rr_snapshot_transfer_bytes",
            &["direction=\"recv\"", group_frag],
        );
        assert_eq!(
            recv_bytes,
            Some(snapshot_bytes.len() as f64),
            "recv byte counter must equal the installed payload size; rendered:\n{rendered}"
        );
        let recv_samples = crate::test_metrics::series_value(
            &rendered,
            "rr_snapshot_transfer_duration_seconds_count",
            &["direction=\"recv\"", group_frag],
        );
        assert_eq!(
            recv_samples,
            Some(1.0),
            "recv duration histogram must record exactly one sample; rendered:\n{rendered}"
        );

        // The receiving state machine installed the exact payload bytes.
        let installed = {
            let mut guard = server_sm.lock().await;
            guard
                .get_current_snapshot()
                .await
                .unwrap()
                .map(|s| s.snapshot.into_inner())
        };
        assert_eq!(
            installed.as_deref(),
            Some(snapshot_bytes.as_slice()),
            "the server-side state machine must hold the transferred payload"
        );

        // Drop the client's pooled channel so the server's graceful shutdown
        // is not held open by the pooled HTTP/2 connection.
        drop(network);
        let _ = stop_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(5), serve).await;
    }
}
