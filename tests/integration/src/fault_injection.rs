//! Fault injection utilities for integration tests.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use rr_core::error::NetworkError;
use rr_core::types::NodeId;
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
use tonic::transport::Channel;

/// A [`ChannelPoolLike`] implementation with fault injection support.
pub struct FaultAwareChannelPool {
    inner: Arc<ChannelPool>,
    blocked: Arc<DashSet<NodeId>>,
    latency_ms: Arc<DashMap<NodeId, u64>>,
    loss: Arc<DashMap<NodeId, f64>>,
}

/// Handle for controlling fault injection in a [`FaultAwareChannelPool`].
pub struct FaultInjector {
    inner: Arc<ChannelPool>,
    blocked: Arc<DashSet<NodeId>>,
    latency_ms: Arc<DashMap<NodeId, u64>>,
    loss: Arc<DashMap<NodeId, f64>>,
}

impl FaultAwareChannelPool {
    /// Create a new pool and its paired [`FaultInjector`].
    pub fn new() -> (Self, FaultInjector) {
        let inner = Arc::new(ChannelPool::new());
        let blocked = Arc::new(DashSet::new());
        let latency_ms: Arc<DashMap<NodeId, u64>> = Arc::new(DashMap::new());
        let loss: Arc<DashMap<NodeId, f64>> = Arc::new(DashMap::new());
        let pool = Self {
            inner: inner.clone(),
            blocked: blocked.clone(),
            latency_ms: latency_ms.clone(),
            loss: loss.clone(),
        };
        let injector = FaultInjector {
            inner,
            blocked,
            latency_ms,
            loss,
        };
        (pool, injector)
    }
}

#[async_trait]
impl ChannelPoolLike for FaultAwareChannelPool {
    async fn get_or_connect(&self, node_id: NodeId, addr: &str) -> Result<Channel, NetworkError> {
        if self.blocked.contains(&node_id) {
            return Err(NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: format!("node {node_id} is isolated by fault injector"),
            });
        }
        if let Some(p) = self.loss.get(&node_id) {
            if rand::random::<f64>() < *p {
                return Err(NetworkError::ConnectionFailed {
                    addr: addr.to_string(),
                    reason: format!("simulated packet loss for node {node_id}"),
                });
            }
        }
        if let Some(ms) = self.latency_ms.get(&node_id) {
            tokio::time::sleep(Duration::from_millis(*ms)).await;
        }
        self.inner.get_or_connect(node_id, addr).await
    }

    fn remove(&self, node_id: NodeId) {
        self.inner.remove(node_id);
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    async fn remove_and_reconnect(&self, node_id: NodeId, addr: &str) -> Result<Channel, NetworkError> {
        self.inner.remove(node_id);
        self.get_or_connect(node_id, addr).await
    }
}

impl FaultInjector {
    /// Block all connections to `node_id` and evict any cached channel.
    pub fn isolate(&self, node_id: NodeId) {
        self.blocked.insert(node_id);
        self.inner.remove(node_id);
    }

    /// Restore connectivity to `node_id`.
    pub fn restore(&self, node_id: NodeId) {
        self.blocked.remove(&node_id);
    }

    /// Set packet loss probability (0.0–1.0) for `node_id`.
    pub fn set_packet_loss(&self, node_id: NodeId, probability: f64) {
        self.loss.insert(node_id, probability);
    }

    /// Add artificial latency (milliseconds) for `node_id`.
    pub fn set_latency(&self, node_id: NodeId, delay_ms: u64) {
        self.latency_ms.insert(node_id, delay_ms);
    }

    /// Clear all faults (does not evict cached channels).
    pub fn clear_all(&self) {
        self.blocked.clear();
        self.loss.clear();
        self.latency_ms.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolate_restore() {
        let (pool, injector) = FaultAwareChannelPool::new();
        injector.isolate(1);
        assert!(pool.blocked.contains(&1));
        injector.restore(1);
        assert!(!pool.blocked.contains(&1));
    }

    #[test]
    fn test_packet_loss_set() {
        let (_pool, injector) = FaultAwareChannelPool::new();
        injector.set_packet_loss(2, 0.5);
        assert_eq!(*injector.loss.get(&2).unwrap(), 0.5);
    }

    #[test]
    fn test_latency_set() {
        let (_pool, injector) = FaultAwareChannelPool::new();
        injector.set_latency(3, 100);
        assert_eq!(*injector.latency_ms.get(&3).unwrap(), 100);
    }

    #[test]
    fn test_clear_all() {
        let (pool, injector) = FaultAwareChannelPool::new();
        injector.isolate(1);
        injector.set_packet_loss(2, 0.5);
        injector.set_latency(3, 100);
        injector.clear_all();
        assert!(!pool.blocked.contains(&1));
        assert!(injector.loss.is_empty());
        assert!(injector.latency_ms.is_empty());
    }
}
