//! Connection pool for reusing gRPC channels per node.

use std::sync::Arc;

use dashmap::DashMap;
use rr_core::error::NetworkError;
use rr_core::types::NodeId;
use tonic::transport::{Channel, Endpoint};

use crate::tls::TlsConfig;

/// A pool of reusable gRPC channels, one per node.
///
/// Uses `DashMap` for lock-free concurrent access. When constructed with
/// [`ChannelPool::with_tls`], every new channel is opened over TLS using
/// the shared [`TlsConfig`]; the URI scheme is automatically `https://`.
///
/// Values carry the dialed address alongside the channel: a cache hit with
/// a MISMATCHED address (node id reused with a new address) is evicted and
/// redialed instead of silently streaming to the old peer.
#[derive(Clone, Default)]
pub struct ChannelPool {
    channels: Arc<DashMap<NodeId, (String, Channel)>>,
    tls: Option<Arc<TlsConfig>>,
}

impl ChannelPool {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            tls: None,
        }
    }

    /// Create a pool that opens every channel over TLS using the given config.
    pub fn with_tls(tls: TlsConfig) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            tls: Some(Arc::new(tls)),
        }
    }

    /// Whether this pool is configured to use TLS for outbound channels.
    pub fn uses_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Get an existing channel for a node, or create a new one.
    ///
    /// A cached channel is reused only when it was dialed for the SAME
    /// `addr`; a node id reused with a new address evicts the stale channel
    /// and redials, so traffic follows the address change immediately
    /// instead of waiting for an error-driven eviction.
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
        let mut endpoint =
            Endpoint::from_shared(uri).map_err(|e| NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            })?;
        if let Some(tls) = &self.tls {
            let client_tls =
                tls.build_client_tls()
                    .map_err(|e| NetworkError::ConnectionFailed {
                        addr: addr.to_string(),
                        reason: format!("tls config: {e}"),
                    })?;
            endpoint =
                endpoint
                    .tls_config(client_tls)
                    .map_err(|e| NetworkError::ConnectionFailed {
                        addr: addr.to_string(),
                        reason: format!("tls endpoint: {e}"),
                    })?;
        }
        let channel = crate::tuned_endpoint(endpoint)
            .connect()
            .await
            .map_err(|e| NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            })?;
        self.channels
            .insert(node_id, (addr.to_string(), channel.clone()));
        Ok(channel)
    }

    /// Remove a channel from the pool (e.g. when a node goes down).
    pub fn remove(&self, node_id: NodeId) {
        self.channels.remove(&node_id);
    }

    /// Returns the number of active channels in the pool.
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// Returns `true` if the pool contains no channels.
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_pool_new_is_empty() {
        let pool = ChannelPool::new();
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn test_channel_pool_default_is_empty() {
        let pool = ChannelPool::default();
        assert!(pool.is_empty());
    }

    #[test]
    fn test_channel_pool_remove_nonexistent() {
        let pool = ChannelPool::new();
        // Should not panic
        pool.remove(42);
        assert!(pool.is_empty());
    }

    #[test]
    fn test_channel_pool_clone_shares_state() {
        let pool = ChannelPool::new();
        let pool2 = pool.clone();
        // Both clones share the same underlying DashMap via Arc
        assert_eq!(pool.len(), pool2.len());
    }

    /// A cache hit with the SAME address returns the pooled channel without
    /// redialing: once the listener is gone, a redial would fail fast, so
    /// success proves the hit path served the cached channel.
    #[tokio::test]
    async fn test_same_addr_hit_does_not_redial() {
        let pool = ChannelPool::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let acceptor = tokio::spawn(async move {
            while let Ok((_sock, _)) = listener.accept().await {}
        });

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
        let acceptor = tokio::spawn(async move {
            while let Ok((_sock, _)) = listener.accept().await {}
        });

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
}
