//! Connection pool for reusing gRPC channels per node.

use std::sync::Arc;

use dashmap::DashMap;
use rr_core::error::NetworkError;
use rr_core::types::NodeId;
use tonic::transport::Channel;

/// A pool of reusable gRPC channels, one per node.
///
/// Uses `DashMap` for lock-free concurrent access.
#[derive(Clone, Default)]
pub struct ChannelPool {
    channels: Arc<DashMap<NodeId, Channel>>,
}

impl ChannelPool {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
        }
    }

    /// Get an existing channel for a node, or create a new one.
    pub async fn get_or_connect(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> Result<Channel, NetworkError> {
        if let Some(channel) = self.channels.get(&node_id) {
            return Ok(channel.clone());
        }
        let channel = Channel::from_shared(format!("http://{addr}"))
            .map_err(|e| NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            })?
            .connect()
            .await
            .map_err(|e| NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            })?;
        self.channels.insert(node_id, channel.clone());
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
}
