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

use rr_core::error::{NetworkError, RrError};
use rr_core::idempotency::IdempotencyKey;
use rr_core::types::{GroupId, NodeId};
use rr_network::auth::attach_bearer_token;
use rr_network::proto::client::client_service_client::ClientServiceClient;
use rr_network::proto::client::{
    ClientRequest, ClusterInfo, Empty, IdempotencyKey as ProtoIdemKey,
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
pub struct RrClient {
    bootstrap_addrs: Vec<String>,
    channel_pool: ChannelPool,
    leader_cache: Arc<RwLock<HashMap<GroupId, (NodeId, String)>>>,
    retry_config: RetryConfig,
    tls_config: Option<TlsConfig>,
    bearer_token: Option<String>,
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
            leader_cache: Arc::new(RwLock::new(HashMap::new())),
            retry_config: self.retry_config,
            tls_config: self.tls_config,
            bearer_token: self.bearer_token,
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
    pub async fn update_leader_cache(&self, group_id: GroupId, node_id: NodeId, addr: String) {
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
        Ok(ClientServiceClient::new(channel))
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

    /// Read from a local replica without Raft coordination.
    ///
    /// This is a stale read and may return slightly out-of-date data.
    #[instrument(skip(self, payload), fields(group_id, payload_len = payload.len(), consistency = "stale"))]
    pub async fn read(&self, group_id: GroupId, payload: Vec<u8>) -> Result<Vec<u8>, RrError> {
        self.send_read_inner(group_id, payload, false).await
    }

    /// Read with linearizable consistency.
    ///
    /// The target node coordinates with leader via ReadIndex, waits for local
    /// apply to catch up, then serves from local state machine.
    #[instrument(skip(self, payload), fields(group_id, payload_len = payload.len(), consistency = "linearizable"))]
    pub async fn read_linearizable(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        self.send_read_inner(group_id, payload, true).await
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
                        let delay_ms = self.retry_config.retry_delay_ms.saturating_mul(2u64.saturating_pow(attempt.min(20)));
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                    continue;
                }
            };

            let request = ClientRequest {
                partition_id: group_id,
                payload: payload.clone(),
                idem_key: idem_key.clone().map(idem_key_to_proto),
                read_only: false,
                linearizable: false,
            };

            let tonic_req = match self.build_request(request) {
                Ok(req) => req,
                Err(e) => return Err(e),
            };

            match grpc_client.request(tonic_req).await {
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

                    if let Some(leader_info) = resp.leader_info {
                        tracing::info!(
                            group_id,
                            leader_node_id = leader_info.node_id,
                            leader_addr = %leader_info.addr,
                            "leader redirect received, updating cache"
                        );
                        self.update_leader_cache(group_id, leader_info.node_id, leader_info.addr)
                            .await;

                        if !resp.success && attempt + 1 < max_attempts {
                            let delay_ms = self.retry_config.retry_delay_ms.saturating_mul(2u64.saturating_pow(attempt.min(20)));
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                            continue;
                        }
                    }

                    metrics::histogram!(
                        "rr_client_request_duration_seconds",
                        "group_id" => gid_label.clone(),
                        "outcome" => "success",
                    )
                    .record(request_start.elapsed().as_secs_f64());
                    return Ok(resp.payload);
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
                    last_error = Some(RrError::Network(NetworkError::RpcFailed(
                        status.message().to_string(),
                    )));

                    // Exponential backoff before retry
                    if attempt + 1 < max_attempts {
                        let delay_ms = self.retry_config.retry_delay_ms.saturating_mul(2u64.saturating_pow(attempt.min(20)));
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

    async fn send_read_inner(
        &self,
        group_id: GroupId,
        payload: Vec<u8>,
        linearizable: bool,
    ) -> Result<Vec<u8>, RrError> {
        let max_attempts = self.retry_config.max_retries + 1;
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

            let addr = self.bootstrap_addrs[(attempt as usize) % self.bootstrap_addrs.len()].clone();

            let mut grpc_client = match self.connect_to_addr(&addr).await {
                Ok(client) => client,
                Err(e) => {
                    last_error = Some(e);
                    if attempt + 1 < max_attempts {
                        let delay_ms = self.retry_config.retry_delay_ms.saturating_mul(2u64.saturating_pow(attempt.min(20)));
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                    continue;
                }
            };

            let request = ClientRequest {
                partition_id: group_id,
                payload: payload.clone(),
                idem_key: None,
                read_only: true,
                linearizable,
            };

            let tonic_req = self.build_request(request)?;

            match grpc_client.request(tonic_req).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.success {
                        metrics::histogram!(
                            "rr_client_request_duration_seconds",
                            "group_id" => gid_label.clone(),
                            "outcome" => "success",
                        )
                        .record(request_start.elapsed().as_secs_f64());
                        return Ok(resp.payload);
                    }

                    if let Some(leader_info) = resp.leader_info {
                        tracing::debug!(
                            group_id,
                            leader_node_id = leader_info.node_id,
                            leader_addr = %leader_info.addr,
                            "read response carried unexpected leader redirect"
                        );
                    }

                    last_error = Some(RrError::Network(NetworkError::RpcFailed(
                        "read request not successful".to_string(),
                    )));
                }
                Err(status) => {
                    last_error = Some(RrError::Network(NetworkError::RpcFailed(
                        status.message().to_string(),
                    )));
                }
            }

            if attempt + 1 < max_attempts {
                let delay_ms = self.retry_config.retry_delay_ms.saturating_mul(2u64.saturating_pow(attempt.min(20)));
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
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
}
