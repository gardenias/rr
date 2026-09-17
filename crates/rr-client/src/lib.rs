//! `rr-client` — User-facing client for the rr distributed framework.
//!
//! Provides [`RrClient`] with automatic leader-redirect, retry logic,
//! framework-level idempotency, TLS/mTLS, and bearer-token auth — all
//! over gRPC.
//!
//! # Quick start
//!
//! For dev / loopback (no TLS, no auth) use [`RrClient::connect`]:
//!
//! ```rust,ignore
//! let client = RrClient::connect(vec!["127.0.0.1:51001".into()]).await?;
//! let resp = client.send_request(group_id, payload).await?;
//! ```
//!
//! For production (TLS + auth) use [`RrClientBuilder`]:
//!
//! ```rust,ignore
//! use rr_network::tls::TlsConfig;
//! let tls = TlsConfig {
//!     cert_path: "/etc/rr/client.crt".into(),
//!     key_path: "/etc/rr/client.key".into(),
//!     ca_path: Some("/etc/rr/ca.crt".into()),
//!     require_client_cert: false, // ignored on client side
//!     domain_name: Some("rr.example.com".into()),
//! };
//! let client = RrClientBuilder::new(vec!["rr.example.com:51001".into()])
//!     .with_tls(tls)
//!     .with_bearer_token(std::env::var("RR_CLIENT_TOKEN")?)
//!     .build().await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use rr_core::error::{NetworkError, RaftError, RrError};
use rr_core::idempotency::IdempotencyKey;
use rr_core::types::{GroupId, NodeId};
use rr_network::auth::attach_bearer_token;
use rr_network::proto::client::client_service_client::ClientServiceClient;
use rr_network::proto::client::{
    ClientRequest, ClusterInfo, Empty, IdempotencyKey as ProtoIdemKey, ReadLocalRequest,
};
use rr_network::tls::TlsConfig;
use rr_network::ChannelPool;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::instrument;

fn idem_key_to_proto(key: IdempotencyKey) -> ProtoIdemKey {
    ProtoIdemKey {
        request_type: key.request_type,
        request_id: key.request_id,
        client_id: key.client_id,
    }
}

/// Configuration for retry behavior on transient failures and leader redirects.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts before returning an error.
    pub max_retries: u32,
    /// Base delay in milliseconds between retries (doubled each attempt).
    pub retry_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay_ms: 100,
        }
    }
}

/// User-facing client for the rr distributed framework.
///
/// Connects to an rr cluster and provides methods to send requests with
/// automatic leader-redirect and exponential-backoff retry logic. Supports
/// optional TLS/mTLS and bearer-token authentication via [`RrClientBuilder`].
///
/// For dev / loopback use [`RrClient::connect`]. For production deployments
/// that enable TLS or bearer auth on rr-server, use [`RrClientBuilder`].
#[derive(Clone)]
pub struct RrClient {
    bootstrap_addrs: Vec<String>,
    channel_pool: ChannelPool,
    /// Addr-keyed gRPC channel cache. Dialing a fresh TCP/H2 connection per
    /// RPC (the previous behavior) collapses under concurrency: ephemeral
    /// port / listen-backlog exhaustion on loopback, and accept-flood
    /// starvation on servers. Channels are created lazily on first use and
    /// reused; a cached channel is evicted on `Code::Unavailable` so the
    /// next attempt redials.
    channels: Arc<RwLock<HashMap<String, Channel>>>,
    leader_cache: Arc<RwLock<HashMap<GroupId, (NodeId, String)>>>,
    retry_config: RetryConfig,
    tls_config: Option<TlsConfig>,
    bearer_token: Option<String>,
    /// Optional per-request timeout applied to a single gRPC `request` attempt
    /// (R-2 / A-0033 "超时映射"). `None` (default) = unbounded wait, preserving
    /// prior behavior. See [`RrClientBuilder::with_request_timeout`].
    request_timeout_ms: Option<u64>,
}

/// Builder for [`RrClient`] with optional TLS and bearer-token configuration.
///
/// Use this when the rr cluster has TLS or bearer auth enabled. For
/// no-TLS no-auth dev clusters, [`RrClient::connect`] is sufficient.
///
/// # Security
///
/// Bearer tokens are sent in plaintext over the wire inside the gRPC
/// metadata `authorization` header. Pairing [`Self::with_bearer_token`]
/// without [`Self::with_tls`] is therefore equivalent to no authentication
/// against an active network attacker. By default this combination logs a
/// `tracing::warn!` (matching the server-side `allow_insecure` opt-in
/// pattern). Call [`Self::with_strict_security`] to instead reject the
/// configuration at [`Self::build`] time.
pub struct RrClientBuilder {
    bootstrap_addrs: Vec<String>,
    retry_config: RetryConfig,
    tls_config: Option<TlsConfig>,
    bearer_token: Option<String>,
    strict_security: bool,
    request_timeout_ms: Option<u64>,
}

impl RrClientBuilder {
    /// Create a new builder with the given bootstrap addresses.
    pub fn new(addrs: Vec<String>) -> Self {
        Self {
            bootstrap_addrs: addrs,
            retry_config: RetryConfig::default(),
            tls_config: None,
            bearer_token: None,
            strict_security: false,
            request_timeout_ms: None,
        }
    }

    /// Attach TLS / mTLS configuration. The connection scheme switches
    /// from `http://` to `https://` and `endpoint.tls_config(...)` is
    /// applied to every channel built by the resulting [`RrClient`].
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls_config = Some(tls);
        self
    }

    /// Attach a bearer token. The token is added to every outbound
    /// request's `authorization: Bearer <token>` metadata header.
    ///
    /// Pair with [`Self::with_tls`] in production: bearer tokens are
    /// transmitted in plaintext. See [`Self`] type-level docs for the
    /// strict-mode opt-in.
    pub fn with_bearer_token(mut self, token: String) -> Self {
        self.bearer_token = Some(token);
        self
    }

    /// Override the default retry configuration.
    pub fn with_retry_config(mut self, cfg: RetryConfig) -> Self {
        self.retry_config = cfg;
        self
    }

    /// Bound each single gRPC `request` attempt to `timeout` (R-2 / A-0033
    /// "超时映射"). When a response is deferred (R-2 two-phase apply), the
    /// client's gRPC call parks until the `Complete` arrives; this timeout
    /// maps that wait to a retryable [`NetworkError::Timeout`] instead of an
    /// unbounded hang (safe under business-key idempotency — at-least-once +
    /// dedup, per A-0033). Set it for `respMode=full`-style callers that park
    /// on deferred responses. Omit to keep the default unbounded wait.
    pub fn with_request_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.request_timeout_ms = Some(timeout.as_millis() as u64);
        self
    }

    /// Reject configurations with `bearer_token` set but no `tls_config`
    /// at [`Self::build`] time, instead of warning. Default is `false`
    /// (warn-only) for parity with rr-server's `allow_insecure` opt-in.
    pub fn with_strict_security(mut self, enabled: bool) -> Self {
        self.strict_security = enabled;
        self
    }

    /// Validate the configuration and produce an [`RrClient`].
    ///
    /// Returns `RrError::Network(NetworkError::ConnectionFailed)` if:
    /// - No bootstrap addresses were provided.
    /// - Strict security is on and a bearer token was set without TLS.
    pub async fn build(self) -> Result<RrClient, RrError> {
        if self.bootstrap_addrs.is_empty() {
            return Err(RrError::Network(NetworkError::ConnectionFailed {
                addr: "<none>".to_string(),
                reason: "no bootstrap addresses provided".to_string(),
            }));
        }

        let token_without_tls = self.bearer_token.is_some() && self.tls_config.is_none();
        if token_without_tls {
            if self.strict_security {
                return Err(RrError::Network(NetworkError::ConnectionFailed {
                    addr: "<config>".to_string(),
                    reason: "strict security: bearer token set without TLS — \
                             enable with_tls() or disable with_strict_security(false) for dev"
                        .to_string(),
                }));
            }
            tracing::warn!(
                "rr-client: bearer token configured without TLS — token will travel in plaintext. \
                 This is acceptable only for loopback / mTLS-elsewhere setups. \
                 In production, attach with_tls(...) or enable with_strict_security(true)."
            );
        }

        tracing::info!(
            bootstrap_count = self.bootstrap_addrs.len(),
            tls = self.tls_config.is_some(),
            bearer = self.bearer_token.is_some(),
            "RrClient connecting"
        );
        Ok(RrClient {
            bootstrap_addrs: self.bootstrap_addrs,
            channel_pool: ChannelPool::new(),
            channels: Arc::new(RwLock::new(HashMap::new())),
            leader_cache: Arc::new(RwLock::new(HashMap::new())),
            retry_config: self.retry_config,
            tls_config: self.tls_config,
            bearer_token: self.bearer_token,
            request_timeout_ms: self.request_timeout_ms,
        })
    }
}

impl RrClient {
    /// Connect to an rr cluster using the given bootstrap addresses.
    ///
    /// Does not immediately open connections — channels are created lazily
    /// on first request. No TLS, no bearer auth. For TLS or auth, use
    /// [`RrClientBuilder`] instead.
    pub async fn connect(addrs: Vec<String>) -> Result<Self, RrError> {
        RrClientBuilder::new(addrs).build().await
    }

    /// Connect with a custom retry configuration. No TLS, no bearer auth.
    /// For TLS or auth, use [`RrClientBuilder::with_tls`] /
    /// [`RrClientBuilder::with_bearer_token`].
    pub async fn connect_with_config(
        addrs: Vec<String>,
        retry_config: RetryConfig,
    ) -> Result<Self, RrError> {
        RrClientBuilder::new(addrs)
            .with_retry_config(retry_config)
            .build()
            .await
    }

    /// Returns a reference to the bootstrap addresses.
    pub fn bootstrap_addrs(&self) -> &[String] {
        &self.bootstrap_addrs
    }

    /// Returns a reference to the channel pool.
    pub fn channel_pool(&self) -> &ChannelPool {
        &self.channel_pool
    }

    /// Returns a reference to the retry configuration.
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Update the leader cache for a given group.
    async fn update_leader_cache(&self, group_id: GroupId, node_id: NodeId, addr: String) {
        let mut cache = self.leader_cache.write().await;
        cache.insert(group_id, (node_id, addr));
    }

    /// Get the cached leader for a given group, if any.
    async fn get_cached_leader(&self, group_id: GroupId) -> Option<(NodeId, String)> {
        let cache = self.leader_cache.read().await;
        cache.get(&group_id).cloned()
    }

    /// Remove a leader from the cache (e.g. on connection failure).
    async fn remove_cached_leader(&self, group_id: GroupId) {
        let mut cache = self.leader_cache.write().await;
        cache.remove(&group_id);
    }

    async fn connect_to_addr(&self, addr: &str) -> Result<ClientServiceClient<Channel>, RrError> {
        // Cache hit: reuse the existing H2 channel (clone is cheap and shares
        // the underlying connection).
        if let Some(channel) = self.channels.read().await.get(addr) {
            return Ok(ClientServiceClient::new(channel.clone()));
        }

        let scheme = if self.tls_config.is_some() {
            "https"
        } else {
            "http"
        };
        let mut endpoint = Channel::from_shared(format!("{scheme}://{addr}")).map_err(|e| {
            RrError::Network(NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: format!("invalid uri: {e}"),
            })
        })?;
        if let Some(tls) = &self.tls_config {
            let client_tls = tls.build_client_tls().map_err(RrError::Network)?;
            endpoint = endpoint.tls_config(client_tls).map_err(|e| {
                RrError::Network(NetworkError::ConnectionFailed {
                    addr: addr.to_string(),
                    reason: format!("tls config rejected by tonic: {e}"),
                })
            })?;
        }
        let channel = endpoint.connect().await.map_err(|e| {
            RrError::Network(NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            })
        })?;
        // Concurrent first-dials race here; last writer wins, the dropped
        // channel's connection closes — harmless.
        self.channels
            .write()
            .await
            .insert(addr.to_string(), channel.clone());
        Ok(ClientServiceClient::new(channel))
    }

    /// Drop a cached channel after a transport-level failure so the next
    /// attempt redials instead of reusing a broken connection.
    async fn evict_channel(&self, addr: &str) {
        self.channels.write().await.remove(addr);
    }

    fn build_request<T>(&self, body: T) -> Result<tonic::Request<T>, RrError> {
        let mut req = tonic::Request::new(body);
        if let Some(token) = &self.bearer_token {
            attach_bearer_token(&mut req, token).map_err(|status| {
                RrError::Network(NetworkError::RpcFailed(format!(
                    "failed to attach bearer token: {}",
                    status.message()
                )))
            })?;
        }
        Ok(req)
    }

    /// Send a request to a specific group with automatic leader-redirect and retry.
    ///
    /// The retry loop:
    /// 1. Try cached leader for `group_id` (or first bootstrap addr if no cache entry)
    /// 2. If response has `leader_info` (a redirect): update cache, retry to that addr
    /// 3. If transport error: remove from cache, try next bootstrap addr round-robin
    /// 4. Max `retry_config.max_retries` total attempts before returning error
    /// 5. Exponential backoff: `retry_delay_ms * 2^attempt`
    #[instrument(skip(self, payload), fields(group_id, payload_len = payload.len()))]
    pub async fn send_request(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        self.send_request_inner(group_id, payload, None).await
    }

    /// Read from any cluster node (stale read).
    ///
    /// Routes to a local follower replica without Raft consensus. No leader redirect.
    /// Lowest latency; the returned data may be slightly behind the latest committed write.
    #[instrument(skip(self, payload), fields(group_id, payload_len = payload.len()))]
    #[allow(clippy::explicit_counter_loop)]
    pub async fn read(&self, group_id: GroupId, payload: Vec<u8>) -> Result<Vec<u8>, RrError> {
        let max_attempts = self.retry_config.max_retries + 1;
        let mut bootstrap_index: usize = 0;
        let mut last_error: Option<RrError> = None;

        for attempt in 0..max_attempts {
            let addr = self.bootstrap_addrs[bootstrap_index % self.bootstrap_addrs.len()].clone();
            bootstrap_index += 1;

            let request = ReadLocalRequest {
                partition_id: group_id,
                payload: payload.clone(),
            };
            let tonic_req = match self.build_request(request) {
                Ok(req) => req,
                Err(e) => return Err(e),
            };

            match self.connect_to_addr(&addr).await {
                Ok(mut client) => match client.read_local(tonic_req).await {
                    Ok(resp) => return Ok(resp.into_inner().payload),
                    Err(e) => {
                        last_error = Some(RrError::Network(NetworkError::RpcFailed(e.to_string())));
                        if e.code() == tonic::Code::Unavailable {
                            self.evict_channel(&addr).await;
                        }
                        if attempt + 1 < max_attempts {
                            let delay = self
                                .retry_config
                                .retry_delay_ms
                                .saturating_mul(2u64.saturating_pow(attempt.min(20)));
                            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                        }
                    }
                },
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            RrError::Network(NetworkError::RpcFailed(
                "all read attempts exhausted".into(),
            ))
        }))
    }

    /// Read with linearizable semantics.
    ///
    /// Server-side flow (per ADR A-0009): the contacted replica calls
    /// openraft `ReadPolicy::ReadIndex`. If that replica is the leader,
    /// it confirms its commit index, awaits local apply, and serves the
    /// read directly. If it is a follower, openraft returns
    /// `ForwardToLeader`; the server flattens this into a
    /// `ReadLocalResponse.leader_info` redirect (mirrors the write
    /// path) and this SDK retries against the leader's address.
    ///
    /// Retry semantics: on transport failure, round-robin bootstrap
    /// addrs. On a follower redirect (`leader_info` present), dial the
    /// indicated leader on the next attempt. Total attempts capped by
    /// [`RetryConfig::max_retries`].
    #[instrument(skip(self, payload), fields(group_id, payload_len = payload.len()))]
    #[allow(clippy::explicit_counter_loop)]
    pub async fn read_linearizable(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        let max_attempts = self.retry_config.max_retries + 1;
        let mut bootstrap_index: usize = 0;
        let mut redirect_addr: Option<String> = None;
        let mut last_error: Option<RrError> = None;

        for attempt in 0..max_attempts {
            let addr = if let Some(a) = redirect_addr.take() {
                a
            } else {
                let a = self.bootstrap_addrs[bootstrap_index % self.bootstrap_addrs.len()].clone();
                bootstrap_index += 1;
                a
            };

            let request = ReadLocalRequest {
                partition_id: group_id,
                payload: payload.clone(),
            };
            let tonic_req = match self.build_request(request) {
                Ok(req) => req,
                Err(e) => return Err(e),
            };

            match self.connect_to_addr(&addr).await {
                Ok(mut client) => match client.read_linearizable(tonic_req).await {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        if let Some(leader) = inner.leader_info {
                            redirect_addr = Some(leader.addr);
                            last_error = Some(RrError::Network(NetworkError::RpcFailed(
                                "follower redirected to leader".into(),
                            )));
                            continue;
                        }
                        return Ok(inner.payload);
                    }
                    Err(e) => {
                        last_error = Some(RrError::Network(NetworkError::RpcFailed(e.to_string())));
                        if e.code() == tonic::Code::Unavailable {
                            self.evict_channel(&addr).await;
                        }
                        if attempt + 1 < max_attempts {
                            let delay = self
                                .retry_config
                                .retry_delay_ms
                                .saturating_mul(2u64.saturating_pow(attempt.min(20)));
                            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                        }
                    }
                },
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            RrError::Network(NetworkError::RpcFailed(
                "all linearizable read attempts exhausted".into(),
            ))
        }))
    }

    /// Send an *idempotent* request: the framework deduplicates retries
    /// of the same `(request_type, request_id)` and returns the cached
    /// response from the first apply.
    ///
    /// Behavior beyond [`send_request`](Self::send_request):
    ///
    /// - The retry loop is safe to run any number of times — duplicates
    ///   are absorbed by the framework's idempotency layer.
    /// - If the framework rejects the request because its `request_id`
    ///   has fallen below the watermark and been evicted from the LRU
    ///   cache, this method returns
    ///   [`RrError::IdempotencyExpired`](rr_core::error::RrError::IdempotencyExpired)
    ///   with the watermark info. The caller should pick a fresh
    ///   `request_id` greater than the returned `watermark`.
    ///
    /// `client_id` on the key is diagnostic-only — collisions with a
    /// different `client_id` still return the cached response (the
    /// dedup identity is `(request_type, request_id)`).
    #[instrument(skip(self, payload, key), fields(group_id, payload_len = payload.len(), request_type = %key.request_type, request_id = key.request_id))]
    pub async fn send_request_idempotent(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
        key: IdempotencyKey,
    ) -> Result<Vec<u8>, RrError> {
        self.send_request_inner(group_id, payload, Some(key)).await
    }

    async fn send_request_inner(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
        idem_key: Option<IdempotencyKey>,
    ) -> Result<Vec<u8>, RrError> {
        let max_attempts = self.retry_config.max_retries + 1;
        let mut bootstrap_index: usize = 0;
        let mut last_error: Option<RrError> = None;
        let request_start = std::time::Instant::now();
        let gid_label = group_id.to_string();

        for attempt in 0..max_attempts {
            if attempt > 0 {
                metrics::counter!(
                    "rr_client_retries_total",
                    "group_id" => gid_label.clone()
                )
                .increment(1);
            }

            let addr = if let Some((_node_id, cached_addr)) = self.get_cached_leader(group_id).await
            {
                metrics::counter!(
                    "rr_client_leader_cache_hits_total",
                    "group_id" => gid_label.clone()
                )
                .increment(1);
                cached_addr
            } else {
                metrics::counter!(
                    "rr_client_leader_cache_misses_total",
                    "group_id" => gid_label.clone()
                )
                .increment(1);
                let addr =
                    self.bootstrap_addrs[bootstrap_index % self.bootstrap_addrs.len()].clone();
                bootstrap_index += 1;
                addr
            };

            tracing::debug!(
                attempt,
                group_id,
                addr = %addr,
                "sending request"
            );

            // Try to connect and send
            let mut grpc_client = match self.connect_to_addr(&addr).await {
                Ok(client) => client,
                Err(e) => {
                    tracing::warn!(
                        attempt,
                        addr = %addr,
                        error = %e,
                        "connection failed, removing from cache"
                    );
                    self.remove_cached_leader(group_id).await;
                    last_error = Some(e);

                    // Exponential backoff before retry
                    if attempt + 1 < max_attempts {
                        let delay_ms = self
                            .retry_config
                            .retry_delay_ms
                            .saturating_mul(2u64.saturating_pow(attempt.min(20)));
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                    continue;
                }
            };

            let request = ClientRequest {
                partition_id: group_id,
                payload: payload.clone(),
                idem_key: idem_key.clone().map(idem_key_to_proto),
            };

            let tonic_req = match self.build_request(request) {
                Ok(req) => req,
                Err(e) => return Err(e),
            };

            // R-2 / A-0033 "超时映射": bound a single request attempt. A deferred
            // (two-phase) response parks this call; the timeout maps that wait to
            // a retryable NetworkError::Timeout instead of an unbounded hang.
            let request_future = grpc_client.request(tonic_req);
            let request_outcome = match self.request_timeout_ms {
                Some(ms) => match tokio::time::timeout(
                    std::time::Duration::from_millis(ms),
                    request_future,
                )
                .await
                {
                    Ok(res) => res,
                    Err(_elapsed) => {
                        tracing::warn!(
                            attempt,
                            group_id,
                            timeout_ms = ms,
                            "request attempt timed out (deferred response not completed in time)"
                        );
                        self.remove_cached_leader(group_id).await;
                        last_error = Some(RrError::Network(NetworkError::Timeout));
                        if attempt + 1 < max_attempts {
                            let delay_ms = self
                                .retry_config
                                .retry_delay_ms
                                .saturating_mul(2u64.saturating_pow(attempt.min(20)));
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        }
                        continue;
                    }
                },
                None => request_future.await,
            };

            match request_outcome {
                Ok(response) => {
                    let resp = response.into_inner();

                    if let Some(idem_err) = resp.idem_error {
                        metrics::histogram!(
                            "rr_client_request_duration_seconds",
                            "group_id" => gid_label.clone(),
                            "outcome" => "idempotency_expired",
                        )
                        .record(request_start.elapsed().as_secs_f64());
                        return Err(RrError::IdempotencyExpired {
                            request_type: idem_err.request_type,
                            request_id: idem_err.request_id,
                            watermark: idem_err.watermark,
                        });
                    }

                    if resp.success {
                        metrics::histogram!(
                            "rr_client_request_duration_seconds",
                            "group_id" => gid_label.clone(),
                            "outcome" => "success",
                        )
                        .record(request_start.elapsed().as_secs_f64());
                        return Ok(resp.payload);
                    }

                    // `success == false` means the contacted node is not (or
                    // not yet) the leader — the write was NOT applied. Never
                    // surface this as `Ok`: that would acknowledge a write
                    // that never happened (silent fake success). Treat it as
                    // retryable until the redirect converges or attempts run
                    // out.
                    if let Some(leader_info) = resp.leader_info {
                        let leader_node_id = leader_info.node_id;
                        let leader_addr = leader_info.addr;
                        tracing::info!(
                            group_id,
                            leader_node_id,
                            leader_addr = %leader_addr,
                            "leader redirect received, updating cache"
                        );
                        self.update_leader_cache(group_id, leader_node_id, leader_addr.clone())
                            .await;
                        last_error = Some(RrError::Raft(RaftError::NotLeader {
                            leader: Some(leader_node_id),
                            leader_addr: Some(leader_addr),
                        }));
                    } else {
                        // Follower does not know the current leader (election
                        // window). Drop any stale cache entry so the next
                        // attempt falls back to bootstrap polling.
                        tracing::debug!(
                            attempt,
                            group_id,
                            addr = %addr,
                            "node has no known leader yet, retrying via bootstrap"
                        );
                        self.remove_cached_leader(group_id).await;
                        last_error = Some(RrError::Raft(RaftError::NotLeader {
                            leader: None,
                            leader_addr: None,
                        }));
                    }

                    if attempt + 1 < max_attempts {
                        let delay_ms = self
                            .retry_config
                            .retry_delay_ms
                            .saturating_mul(2u64.saturating_pow(attempt.min(20)));
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                }
                Err(status) => {
                    tracing::warn!(
                        attempt,
                        addr = %addr,
                        code = ?status.code(),
                        message = %status.message(),
                        "rpc failed"
                    );
                    self.remove_cached_leader(group_id).await;
                    if status.code() == tonic::Code::Unavailable {
                        self.evict_channel(&addr).await;
                    }
                    last_error = Some(RrError::Network(NetworkError::RpcFailed(
                        status.message().to_string(),
                    )));

                    // Exponential backoff before retry
                    if attempt + 1 < max_attempts {
                        let delay_ms = self
                            .retry_config
                            .retry_delay_ms
                            .saturating_mul(2u64.saturating_pow(attempt.min(20)));
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }

        metrics::histogram!(
            "rr_client_request_duration_seconds",
            "group_id" => gid_label.clone(),
            "outcome" => "failure",
        )
        .record(request_start.elapsed().as_secs_f64());
        Err(last_error.unwrap_or_else(|| {
            RrError::Network(NetworkError::RpcFailed("max retries exhausted".to_string()))
        }))
    }

    /// Send a request routed by partition ID directly.
    ///
    /// Currently maps 1:1 to `send_request` since partition_id and group_id
    /// are equivalent in the current routing model.
    #[instrument(skip(self, payload), fields(partition_id, payload_len = payload.len()))]
    pub async fn send_request_with_partition(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        self.send_request(partition_id, payload).await
    }

    /// Get cluster topology information from the first reachable bootstrap node.
    #[instrument(skip(self))]
    pub async fn get_cluster_info(&self) -> Result<ClusterInfo, RrError> {
        let mut last_error: Option<RrError> = None;

        for addr in &self.bootstrap_addrs {
            tracing::debug!(addr = %addr, "fetching cluster info");

            let mut grpc_client = match self.connect_to_addr(addr).await {
                Ok(client) => client,
                Err(e) => {
                    tracing::warn!(
                        addr = %addr,
                        error = %e,
                        "failed to connect for cluster info"
                    );
                    last_error = Some(e);
                    continue;
                }
            };

            let tonic_req = match self.build_request(Empty {}) {
                Ok(req) => req,
                Err(e) => return Err(e),
            };

            match grpc_client.get_cluster_info(tonic_req).await {
                Ok(response) => {
                    return Ok(response.into_inner());
                }
                Err(status) => {
                    tracing::warn!(
                        addr = %addr,
                        code = ?status.code(),
                        message = %status.message(),
                        "get_cluster_info rpc failed"
                    );
                    if status.code() == tonic::Code::Unavailable {
                        self.evict_channel(addr).await;
                    }
                    last_error = Some(RrError::Network(NetworkError::RpcFailed(
                        status.message().to_string(),
                    )));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            RrError::Network(NetworkError::ConnectionFailed {
                addr: "<all>".to_string(),
                reason: "no reachable bootstrap nodes".to_string(),
            })
        }))
    }

    // ── Blocking interface ────────────────────────────────────────────────────

    /// Validate the runtime requirements of the blocking interface and
    /// return the current handle.
    ///
    /// `block_in_place` only works on a multi-thread runtime; historically
    /// calling the blocking API on a `current_thread` runtime (or outside
    /// any runtime) panicked deep inside tokio. Fail fast with a typed
    /// error instead.
    fn blocking_runtime_handle() -> Result<tokio::runtime::Handle, RrError> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            RrError::Network(NetworkError::RpcFailed(
                "blocking interface requires an active tokio multi-thread runtime".into(),
            ))
        })?;
        if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
            return Err(RrError::Network(NetworkError::RpcFailed(
                "blocking interface is unsupported on a current_thread runtime; \
                 use the async or reactive API instead"
                    .into(),
            )));
        }
        Ok(handle)
    }

    /// Blocking write — blocks the calling OS thread until the response
    /// arrives.  Wraps [`send_request`] via `block_in_place` so it is safe
    /// to call from within a tokio **multi-thread** runtime without starving
    /// the scheduler.  Returns an error (not a panic) when called from a
    /// `current_thread` runtime or outside any runtime.
    pub fn send_request_blocking(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        let handle = Self::blocking_runtime_handle()?;
        tokio::task::block_in_place(|| handle.block_on(self.send_request(group_id, payload)))
    }

    /// Idempotent blocking write.  See [`send_request_idempotent`] and
    /// [`send_request_blocking`] for details.
    pub fn send_request_idempotent_blocking(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
        key: IdempotencyKey,
    ) -> Result<Vec<u8>, RrError> {
        let handle = Self::blocking_runtime_handle()?;
        tokio::task::block_in_place(|| {
            handle.block_on(self.send_request_idempotent(group_id, payload, key))
        })
    }

    /// Blocking stale read.  See [`read`] and [`send_request_blocking`].
    pub fn read_blocking(&self, group_id: GroupId, payload: Vec<u8>) -> Result<Vec<u8>, RrError> {
        let handle = Self::blocking_runtime_handle()?;
        tokio::task::block_in_place(|| handle.block_on(self.read(group_id, payload)))
    }

    /// Blocking linearizable read.  See [`read_linearizable`] and
    /// [`send_request_blocking`] for the blocking-runtime requirements.
    pub fn read_linearizable_blocking(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        let handle = Self::blocking_runtime_handle()?;
        tokio::task::block_in_place(|| handle.block_on(self.read_linearizable(group_id, payload)))
    }

    // ── Reactive interface (callback-driven) ──────────────────────────────────

    /// Reactive write: fires the request and returns immediately.
    ///
    /// `callback` is invoked on a tokio worker thread when the response
    /// (or error) arrives.  The calling context is never blocked.
    pub fn send_request_reactive<F>(&self, group_id: GroupId, payload: Vec<u8>, callback: F)
    where
        F: FnOnce(Result<Vec<u8>, RrError>) + Send + 'static,
    {
        let client = self.clone();
        tokio::spawn(async move {
            callback(client.send_request(group_id, payload).await);
        });
    }

    /// Reactive idempotent write.  See [`send_request_reactive`].
    pub fn send_request_idempotent_reactive<F>(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
        key: IdempotencyKey,
        callback: F,
    ) where
        F: FnOnce(Result<Vec<u8>, RrError>) + Send + 'static,
    {
        let client = self.clone();
        tokio::spawn(async move {
            callback(client.send_request_idempotent(group_id, payload, key).await);
        });
    }

    /// Reactive stale read.  See [`send_request_reactive`].
    pub fn read_reactive<F>(&self, group_id: GroupId, payload: Vec<u8>, callback: F)
    where
        F: FnOnce(Result<Vec<u8>, RrError>) + Send + 'static,
    {
        let client = self.clone();
        tokio::spawn(async move {
            callback(client.read(group_id, payload).await);
        });
    }

    /// Reactive linearizable read.  See [`send_request_reactive`].
    pub fn read_linearizable_reactive<F>(&self, group_id: GroupId, payload: Vec<u8>, callback: F)
    where
        F: FnOnce(Result<Vec<u8>, RrError>) + Send + 'static,
    {
        let client = self.clone();
        tokio::spawn(async move {
            callback(client.read_linearizable(group_id, payload).await);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connect_stores_addrs() {
        let addrs = vec!["127.0.0.1:9000".to_string(), "127.0.0.1:9001".to_string()];
        let client = RrClient::connect(addrs.clone())
            .await
            .expect("connect should succeed");

        assert_eq!(client.bootstrap_addrs(), &addrs);
        assert_eq!(client.bootstrap_addrs().len(), 2);
    }

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay_ms, 100);
    }

    #[tokio::test]
    async fn test_leader_cache_update() {
        let client = RrClient::connect(vec!["127.0.0.1:9000".to_string()])
            .await
            .expect("connect should succeed");

        // Initially empty
        assert!(client.get_cached_leader(1).await.is_none());

        // Update the cache (simulating a redirect)
        client
            .update_leader_cache(1, 42, "10.0.0.1:50051".to_string())
            .await;

        // Verify update
        let cached = client.get_cached_leader(1).await;
        assert!(cached.is_some());
        let (node_id, addr) = cached.unwrap();
        assert_eq!(node_id, 42);
        assert_eq!(addr, "10.0.0.1:50051");

        // Update to a different leader
        client
            .update_leader_cache(1, 99, "10.0.0.2:50051".to_string())
            .await;
        let cached = client.get_cached_leader(1).await;
        let (node_id, addr) = cached.unwrap();
        assert_eq!(node_id, 99);
        assert_eq!(addr, "10.0.0.2:50051");

        // Remove from cache
        client.remove_cached_leader(1).await;
        assert!(client.get_cached_leader(1).await.is_none());
    }

    #[tokio::test]
    async fn test_send_request_fails_no_server() {
        let client = RrClient::connect_with_config(
            vec!["localhost:1".to_string()],
            RetryConfig {
                max_retries: 1,
                retry_delay_ms: 10,
            },
        )
        .await
        .expect("connect should succeed");

        let result = client.send_request(1, vec![]).await;
        assert!(result.is_err(), "send_request to invalid addr should fail");
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("network error") || err_str.contains("connection"),
            "error should be network-related, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_get_cluster_info_fails_no_server() {
        let client = RrClient::connect(vec!["localhost:1".to_string()])
            .await
            .expect("connect should succeed");

        let result = client.get_cluster_info().await;
        assert!(
            result.is_err(),
            "get_cluster_info to invalid addr should fail"
        );
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("network error") || err_str.contains("connection"),
            "error should be network-related, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_connect_empty_addrs_fails() {
        let result = RrClient::connect(vec![]).await;
        assert!(result.is_err(), "connect with empty addrs should fail");
    }

    #[tokio::test]
    async fn test_connect_with_custom_retry_config() {
        let config = RetryConfig {
            max_retries: 5,
            retry_delay_ms: 200,
        };
        let client = RrClient::connect_with_config(vec!["127.0.0.1:9000".to_string()], config)
            .await
            .expect("connect should succeed");

        assert_eq!(client.retry_config().max_retries, 5);
        assert_eq!(client.retry_config().retry_delay_ms, 200);
    }

    #[tokio::test]
    async fn test_send_request_with_partition() {
        // send_request_with_partition is a thin wrapper over send_request,
        // so it should also fail with no server running
        let client = RrClient::connect_with_config(
            vec!["localhost:1".to_string()],
            RetryConfig {
                max_retries: 0,
                retry_delay_ms: 10,
            },
        )
        .await
        .expect("connect should succeed");

        let result = client.send_request_with_partition(42, vec![1, 2, 3]).await;
        assert!(
            result.is_err(),
            "send_request_with_partition to invalid addr should fail"
        );
    }

    #[tokio::test]
    async fn test_builder_default_no_tls_no_token() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:9000".to_string()])
            .build()
            .await
            .expect("build should succeed");
        assert!(client.tls_config.is_none());
        assert!(client.bearer_token.is_none());
        assert_eq!(client.bootstrap_addrs(), &["127.0.0.1:9000".to_string()]);
    }

    #[tokio::test]
    async fn test_builder_with_bearer_token_stores_token() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:9000".to_string()])
            .with_bearer_token("secret-test-token".to_string())
            .build()
            .await
            .expect("build should succeed (warn-only by default)");
        assert_eq!(client.bearer_token.as_deref(), Some("secret-test-token"));
        assert!(client.tls_config.is_none());
    }

    #[tokio::test]
    async fn test_builder_strict_security_rejects_token_without_tls() {
        let result = RrClientBuilder::new(vec!["127.0.0.1:9000".to_string()])
            .with_bearer_token("secret-test-token".to_string())
            .with_strict_security(true)
            .build()
            .await;
        let Err(err) = result else {
            panic!("strict mode should reject token-without-tls");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("strict security") || msg.contains("bearer token"),
            "error should mention strict security or bearer token, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_builder_with_retry_config_overrides_default() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:9000".to_string()])
            .with_retry_config(RetryConfig {
                max_retries: 7,
                retry_delay_ms: 250,
            })
            .build()
            .await
            .expect("build should succeed");
        assert_eq!(client.retry_config().max_retries, 7);
        assert_eq!(client.retry_config().retry_delay_ms, 250);
    }

    #[tokio::test]
    async fn test_builder_empty_addrs_rejected() {
        let result = RrClientBuilder::new(vec![]).build().await;
        assert!(result.is_err(), "empty addrs should fail");
    }

    #[tokio::test]
    async fn test_build_request_attaches_bearer_token_when_configured() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:9000".to_string()])
            .with_bearer_token("test-token-xyz".to_string())
            .build()
            .await
            .expect("build should succeed");

        let req = client
            .build_request(Empty {})
            .expect("build_request should succeed");
        let auth_header = req
            .metadata()
            .get("authorization")
            .expect("authorization header must be present");
        assert_eq!(
            auth_header
                .to_str()
                .expect("authorization header must be ASCII"),
            "Bearer test-token-xyz"
        );
    }

    #[tokio::test]
    async fn test_build_request_omits_metadata_when_no_token() {
        let client = RrClientBuilder::new(vec!["127.0.0.1:9000".to_string()])
            .build()
            .await
            .expect("build should succeed");

        let req = client
            .build_request(Empty {})
            .expect("build_request should succeed");
        assert!(
            req.metadata().get("authorization").is_none(),
            "authorization header must NOT be present without bearer token"
        );
    }

    // ── Clone tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn clone_shares_leader_cache() {
        let client = RrClient::connect(vec!["127.0.0.1:9000".to_string()])
            .await
            .expect("connect");
        let clone = client.clone();

        client
            .update_leader_cache(1, 7, "10.0.0.7:50051".to_string())
            .await;

        let from_clone = clone.get_cached_leader(1).await;
        assert!(
            from_clone.is_some(),
            "clone shares leader cache via Arc — update on original must be visible"
        );
        assert_eq!(from_clone.unwrap().0, 7);
    }

    #[test]
    fn clone_preserves_bootstrap_addrs() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = rt
            .block_on(RrClient::connect(vec![
                "127.0.0.1:9001".to_string(),
                "127.0.0.1:9002".to_string(),
            ]))
            .expect("connect");
        let clone = client.clone();
        assert_eq!(client.bootstrap_addrs(), clone.bootstrap_addrs());
    }

    // ── Blocking interface tests ───────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_send_returns_error_when_no_server() {
        let client = RrClient::connect_with_config(
            vec!["localhost:1".to_string()],
            RetryConfig {
                max_retries: 0,
                retry_delay_ms: 10,
            },
        )
        .await
        .expect("connect");

        let result = client.send_request_blocking(1, b"ping".to_vec());
        assert!(
            result.is_err(),
            "blocking send to unreachable addr must fail"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_read_returns_error_when_no_server() {
        let client = RrClient::connect_with_config(
            vec!["localhost:1".to_string()],
            RetryConfig {
                max_retries: 0,
                retry_delay_ms: 10,
            },
        )
        .await
        .expect("connect");

        let result = client.read_blocking(1, b"q".to_vec());
        assert!(
            result.is_err(),
            "blocking read to unreachable addr must fail"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_send_propagates_network_error_kind() {
        let client = RrClient::connect_with_config(
            vec!["localhost:1".to_string()],
            RetryConfig {
                max_retries: 0,
                retry_delay_ms: 10,
            },
        )
        .await
        .expect("connect");

        let err = client
            .send_request_blocking(1, vec![])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("network error") || err.contains("connection"),
            "expected network error, got: {err}"
        );
    }

    /// The blocking API must fail fast with a typed error — not panic deep
    /// inside tokio — when invoked on a current_thread runtime.
    #[tokio::test(flavor = "current_thread")]
    async fn blocking_send_on_current_thread_runtime_returns_error_not_panic() {
        let client = RrClient::connect(vec!["localhost:1".to_string()])
            .await
            .expect("connect");

        let err = client
            .send_request_blocking(1, vec![])
            .expect_err("current_thread runtime must reject blocking calls");
        let msg = err.to_string();
        assert!(
            msg.contains("current_thread"),
            "error must name the current_thread limitation, got: {msg}"
        );
    }

    /// Outside any tokio runtime the blocking API must also return an error
    /// rather than panic.
    #[test]
    fn blocking_send_outside_runtime_returns_error_not_panic() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = rt
            .block_on(RrClient::connect(vec!["localhost:1".to_string()]))
            .expect("connect");
        drop(rt);

        let err = client
            .send_request_blocking(1, vec![])
            .expect_err("no-runtime context must reject blocking calls");
        assert!(
            err.to_string().contains("multi-thread runtime"),
            "error must state the runtime requirement, got: {err}"
        );
    }

    // ── Reactive interface tests ───────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reactive_send_fires_callback_on_error() {
        let client = RrClient::connect_with_config(
            vec!["localhost:1".to_string()],
            RetryConfig {
                max_retries: 0,
                retry_delay_ms: 10,
            },
        )
        .await
        .expect("connect");

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<Vec<u8>, RrError>>();
        client.send_request_reactive(1, b"ping".to_vec(), move |result| {
            let _ = tx.send(result);
        });

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("callback must fire within 5s")
            .expect("oneshot sender must not be dropped");

        assert!(
            result.is_err(),
            "reactive send to unreachable addr must fire error callback"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reactive_send_returns_immediately() {
        let client = RrClient::connect_with_config(
            vec!["localhost:1".to_string()],
            RetryConfig {
                max_retries: 0,
                retry_delay_ms: 10,
            },
        )
        .await
        .expect("connect");

        let (tx, _rx) = tokio::sync::oneshot::channel::<bool>();
        let t0 = std::time::Instant::now();
        client.send_request_reactive(1, b"ping".to_vec(), move |_| {
            let _ = tx.send(true);
        });
        let elapsed = t0.elapsed();

        assert!(
            elapsed < std::time::Duration::from_millis(50),
            "send_request_reactive must return immediately (took {elapsed:?})"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reactive_multiple_callbacks_all_fire() {
        let client = RrClient::connect_with_config(
            vec!["localhost:1".to_string()],
            RetryConfig {
                max_retries: 0,
                retry_delay_ms: 10,
            },
        )
        .await
        .expect("connect");

        let count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let done_tx = Arc::new(std::sync::Mutex::new(Some(done_tx)));

        const N: u32 = 5;
        for _ in 0..N {
            let c = count.clone();
            let dt = done_tx.clone();
            client.send_request_reactive(1, b"x".to_vec(), move |_| {
                let prev = c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if prev + 1 == N {
                    if let Ok(mut guard) = dt.lock() {
                        if let Some(sender) = guard.take() {
                            let _ = sender.send(());
                        }
                    }
                }
            });
        }

        tokio::time::timeout(std::time::Duration::from_secs(10), done_rx)
            .await
            .expect("all callbacks must fire within 10s")
            .expect("oneshot not dropped");

        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), N);
    }

    // ── Fake-success regression tests (leader-unknown window) ────────────
    //
    // A follower that is asked for a write but does not know the current
    // leader (election window) answers `success = false` with NO
    // `leader_info`. The client must treat that as retryable — returning
    // `Ok` here would acknowledge a write that was never applied.

    use rr_network::proto::client::client_service_server::{
        ClientService as ClientServiceTrait, ClientServiceServer,
    };
    use rr_network::proto::client::{ClientResponse, ReadLocalResponse};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Fake node that always answers "not leader, and I don't know who is".
    #[derive(Default)]
    struct LeaderUnknownService {
        hits: Arc<AtomicUsize>,
    }

    /// Fake node that plays "leader unknown" for the first `unknown_for`
    /// requests, then answers as a healthy leader.
    struct LeaderAppearsService {
        hits: Arc<AtomicUsize>,
        unknown_for: usize,
        payload: Vec<u8>,
    }

    #[tonic::async_trait]
    impl ClientServiceTrait for LeaderUnknownService {
        async fn request(
            &self,
            _request: tonic::Request<ClientRequest>,
        ) -> Result<tonic::Response<ClientResponse>, tonic::Status> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            Ok(tonic::Response::new(ClientResponse {
                success: false,
                payload: vec![],
                leader_info: None,
                idem_error: None,
            }))
        }

        async fn read_local(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn read_linearizable(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn get_cluster_info(
            &self,
            _request: tonic::Request<Empty>,
        ) -> Result<tonic::Response<ClusterInfo>, tonic::Status> {
            Ok(tonic::Response::new(ClusterInfo { groups: vec![] }))
        }
    }

    #[tonic::async_trait]
    impl ClientServiceTrait for LeaderAppearsService {
        async fn request(
            &self,
            _request: tonic::Request<ClientRequest>,
        ) -> Result<tonic::Response<ClientResponse>, tonic::Status> {
            let hit = self.hits.fetch_add(1, Ordering::SeqCst);
            if hit < self.unknown_for {
                Ok(tonic::Response::new(ClientResponse {
                    success: false,
                    payload: vec![],
                    leader_info: None,
                    idem_error: None,
                }))
            } else {
                Ok(tonic::Response::new(ClientResponse {
                    success: true,
                    payload: self.payload.clone(),
                    leader_info: None,
                    idem_error: None,
                }))
            }
        }

        async fn read_local(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn read_linearizable(
            &self,
            _request: tonic::Request<ReadLocalRequest>,
        ) -> Result<tonic::Response<ReadLocalResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not under test"))
        }

        async fn get_cluster_info(
            &self,
            _request: tonic::Request<Empty>,
        ) -> Result<tonic::Response<ClusterInfo>, tonic::Status> {
            Ok(tonic::Response::new(ClusterInfo { groups: vec![] }))
        }
    }

    /// Spawn the given service on an ephemeral localhost port; returns the
    /// dial address and the server task handle (aborted on test end).
    async fn spawn_fake_client_service<S>(svc: S) -> (String, tokio::task::JoinHandle<()>)
    where
        S: ClientServiceTrait + Send + Sync + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ClientServiceServer::new(svc))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("fake client service");
        });
        (format!("127.0.0.1:{port}"), handle)
    }

    #[tokio::test]
    async fn test_send_request_no_fake_success_when_leader_unknown() {
        let hits = Arc::new(AtomicUsize::new(0));
        let svc = LeaderUnknownService {
            hits: Arc::clone(&hits),
        };
        let (addr, server) = spawn_fake_client_service(svc).await;

        let client = RrClient::connect_with_config(
            vec![addr],
            RetryConfig {
                max_retries: 2,
                retry_delay_ms: 5,
            },
        )
        .await
        .expect("connect");

        let result = client.send_request(1, b"write".to_vec()).await;
        server.abort();

        let Err(err) = result else {
            panic!("success=false without leader_info must NOT silently succeed");
        };
        assert!(
            matches!(err, RrError::Raft(RaftError::NotLeader { .. })),
            "expected NotLeader after retries exhausted, got: {err}"
        );
        // max_attempts = max_retries + 1 = 3: the client must keep polling
        // the bootstrap list instead of giving up (or fake-succeeding) on
        // the first "leader unknown" reply.
        assert_eq!(
            hits.load(Ordering::SeqCst),
            3,
            "client must retry via bootstrap polling until attempts run out"
        );
    }

    #[tokio::test]
    async fn test_send_request_converges_once_leader_is_known() {
        let svc = LeaderAppearsService {
            hits: Arc::new(AtomicUsize::new(0)),
            unknown_for: 2,
            payload: b"applied".to_vec(),
        };
        let (addr, server) = spawn_fake_client_service(svc).await;

        let client = RrClient::connect_with_config(
            vec![addr],
            RetryConfig {
                max_retries: 4,
                retry_delay_ms: 5,
            },
        )
        .await
        .expect("connect");

        let result = client
            .send_request(1, b"write".to_vec())
            .await
            .expect("must succeed once the node knows a leader");
        server.abort();

        assert_eq!(result, b"applied".to_vec());
    }
}
