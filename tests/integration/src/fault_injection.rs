//! Fault injection utilities for integration tests.
//!
//! Supports **bidirectional network isolation** — when a node is isolated,
//! it can neither send to nor receive from other nodes. This is implemented
//! by giving each cluster member its own `FaultAwareChannelPool` that shares
//! a common `FaultState`; isolation of node N is enforced both at N's own
//! pool (blocking all outbound) and at every other pool (blocking destination N).
//!
//! # Randomness discipline (packet loss)
//!
//! Packet-loss coin flips draw from a single `StdRng` per [`FaultInjector`],
//! seeded at construction: `RR_FAULT_SEED=<n>` pins the seed (same env var
//! as `crate::fault_schedule`, so one value reproduces schedule AND loss
//! stream), otherwise entropy is used and the seed is printed the first time
//! packet loss is configured. Given the same seed and the same call
//! sequence, the loss decisions replay exactly (see
//! `tests::packet_loss_draws_are_seed_reproducible`). Caveat: concurrent
//! `get_or_connect` calls share one stream, so task-scheduling jitter can
//! reorder draws within a run — per-call attribution under full concurrency
//! needs a deterministic scheduler (the R-3 turmoil track); what the seed
//! guarantees here is a reproducible stream for a reproducible call order.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rr_core::error::NetworkError;
use rr_core::types::NodeId;
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
use tonic::transport::Channel;

struct FaultState {
    isolated: DashSet<NodeId>,
    latency_ms: DashMap<NodeId, u64>,
    loss: DashMap<NodeId, f64>,
    /// Seeded stream for packet-loss coin flips (see module docs). One
    /// stream per injector: pools sharing this state draw from it under a
    /// short lock (never held across `.await`).
    loss_rng: Mutex<StdRng>,
    loss_seed: u64,
    /// Print the replay line once, the first time loss is configured.
    loss_seed_announced: AtomicBool,
}

impl FaultState {
    fn new(loss_seed: u64) -> Self {
        Self {
            isolated: DashSet::new(),
            latency_ms: DashMap::new(),
            loss: DashMap::new(),
            loss_rng: Mutex::new(StdRng::seed_from_u64(loss_seed)),
            loss_seed,
            loss_seed_announced: AtomicBool::new(false),
        }
    }

    /// Announce the loss seed once per injector, the first time packet loss
    /// is armed — the replay breadcrumb for a failed run.
    fn announce_loss_seed(&self) {
        if !self.loss_seed_announced.swap(true, Ordering::Relaxed) {
            eprintln!(
                "[fault-injection] packet-loss RNG seed={} (replay: RR_FAULT_SEED={})",
                self.loss_seed, self.loss_seed
            );
        }
    }

    /// One seeded coin flip: `true` with probability `p`.
    fn loss_roll(&self, p: f64) -> bool {
        self.loss_rng.lock().unwrap().gen::<f64>() < p
    }
}

/// Resolve the packet-loss seed: `RR_FAULT_SEED` wins (aligning with the
/// fault-schedule replay knob), else entropy.
fn resolve_loss_seed() -> u64 {
    std::env::var("RR_FAULT_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or_else(rand::random)
}

/// A [`ChannelPoolLike`] implementation with fault injection support.
///
/// Each pool may be "owned" by a specific node, enabling bidirectional
/// isolation: when the owner node is isolated, ALL outbound calls from
/// this pool fail; when any destination node is isolated, calls TO it fail.
pub struct FaultAwareChannelPool {
    inner: Arc<ChannelPool>,
    owner_node: Option<NodeId>,
    state: Arc<FaultState>,
}

/// Handle for controlling fault injection across one or more pools
/// sharing a common [`FaultState`].
pub struct FaultInjector {
    state: Arc<FaultState>,
    per_node_inner: DashMap<NodeId, Arc<ChannelPool>>,
}

impl FaultAwareChannelPool {
    /// Create a new standalone pool (no owner) and its paired [`FaultInjector`].
    ///
    /// Used for unit tests that only need a single pool. Isolation of a node
    /// via this pool blocks only outbound connections *to* that node
    /// (one-directional). For bidirectional isolation across a real cluster,
    /// use [`FaultInjector::new`] + [`FaultInjector::new_pool_for_node`].
    ///
    /// The packet-loss stream is seeded from `RR_FAULT_SEED` when set, else
    /// from entropy (see module docs); use [`Self::new_seeded`] to pin it.
    pub fn new() -> (Self, FaultInjector) {
        Self::new_seeded(resolve_loss_seed())
    }

    /// Like [`Self::new`] but with an explicit packet-loss seed.
    pub fn new_seeded(seed: u64) -> (Self, FaultInjector) {
        let state = Arc::new(FaultState::new(seed));
        let pool = Self {
            inner: Arc::new(ChannelPool::new()),
            owner_node: None,
            state: Arc::clone(&state),
        };
        let injector = FaultInjector {
            state,
            per_node_inner: DashMap::new(),
        };
        (pool, injector)
    }
}

#[async_trait]
impl ChannelPoolLike for FaultAwareChannelPool {
    async fn get_or_connect(&self, node_id: NodeId, addr: &str) -> Result<Channel, NetworkError> {
        if let Some(owner) = self.owner_node {
            if self.state.isolated.contains(&owner) {
                return Err(NetworkError::ConnectionFailed {
                    addr: addr.to_string(),
                    reason: format!("source node {owner} is isolated by fault injector"),
                });
            }
        }
        if self.state.isolated.contains(&node_id) {
            return Err(NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: format!("destination node {node_id} is isolated by fault injector"),
            });
        }
        // Copy the probability out of the DashMap guard before taking the
        // RNG lock (single lock-ordering direction, no guard held across
        // the draw). The draw itself is synchronous — the RNG mutex is
        // never held across an `.await`.
        let loss_p = self.state.loss.get(&node_id).map(|p| *p);
        if let Some(p) = loss_p {
            if self.state.loss_roll(p) {
                return Err(NetworkError::ConnectionFailed {
                    addr: addr.to_string(),
                    reason: format!("simulated packet loss for node {node_id}"),
                });
            }
        }
        if let Some(ms) = self.state.latency_ms.get(&node_id) {
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
    async fn remove_and_reconnect(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> Result<Channel, NetworkError> {
        self.inner.remove(node_id);
        self.get_or_connect(node_id, addr).await
    }
}

impl FaultInjector {
    /// Create a new bidirectional fault injector with no pools attached.
    ///
    /// Use [`FaultInjector::new_pool_for_node`] to build per-node pools.
    ///
    /// The packet-loss stream is seeded from `RR_FAULT_SEED` when set, else
    /// from entropy (the seed is printed on the first [`Self::set_packet_loss`]
    /// call); use [`Self::new_seeded`] to pin it explicitly.
    pub fn new() -> Self {
        Self::new_seeded(resolve_loss_seed())
    }

    /// Like [`Self::new`] but with an explicit packet-loss seed.
    pub fn new_seeded(seed: u64) -> Self {
        Self {
            state: Arc::new(FaultState::new(seed)),
            per_node_inner: DashMap::new(),
        }
    }

    /// The seed feeding the packet-loss stream (for replay breadcrumbs).
    pub fn loss_seed(&self) -> u64 {
        self.state.loss_seed
    }

    /// Create a new pool owned by `owner_node`, sharing this injector's state.
    ///
    /// Isolation of `owner_node` via [`Self::isolate`] blocks all outbound
    /// calls from the returned pool; isolation of any other node blocks calls
    /// TO that node from this (and every other) pool sharing the same state.
    pub fn new_pool_for_node(&self, owner_node: NodeId) -> Arc<FaultAwareChannelPool> {
        let inner = Arc::new(ChannelPool::new());
        self.per_node_inner.insert(owner_node, Arc::clone(&inner));
        Arc::new(FaultAwareChannelPool {
            inner,
            owner_node: Some(owner_node),
            state: Arc::clone(&self.state),
        })
    }

    /// Isolate a node from the cluster (bidirectional).
    ///
    /// After `isolate(n)`:
    /// - No other node can connect to node `n`.
    /// - Node `n` cannot connect to any other node (if the pool it owns
    ///   was created via [`Self::new_pool_for_node`]).
    ///
    /// Also evicts cached channels to/from `n` across all known pools so
    /// existing open connections fail-fast on next use.
    pub fn isolate(&self, node_id: NodeId) {
        self.state.isolated.insert(node_id);
        for entry in self.per_node_inner.iter() {
            entry.value().remove(node_id);
        }
    }

    /// Restore connectivity to `node_id`.
    pub fn restore(&self, node_id: NodeId) {
        self.state.isolated.remove(&node_id);
    }

    /// Set packet loss probability (0.0–1.0) for `node_id`.
    ///
    /// Coin flips draw from the injector's seeded stream (see module docs);
    /// the first call prints the seed for replay.
    pub fn set_packet_loss(&self, node_id: NodeId, probability: f64) {
        self.state.announce_loss_seed();
        self.state.loss.insert(node_id, probability);
    }

    /// Add artificial latency (milliseconds) for `node_id`.
    pub fn set_latency(&self, node_id: NodeId, delay_ms: u64) {
        self.state.latency_ms.insert(node_id, delay_ms);
    }

    /// Clear all faults (does not evict cached channels).
    pub fn clear_all(&self) {
        self.state.isolated.clear();
        self.state.loss.clear();
        self.state.latency_ms.clear();
    }

    #[cfg(test)]
    pub(crate) fn is_isolated(&self, node_id: NodeId) -> bool {
        self.state.isolated.contains(&node_id)
    }

    #[cfg(test)]
    pub(crate) fn loss_for(&self, node_id: NodeId) -> Option<f64> {
        self.state.loss.get(&node_id).map(|v| *v)
    }

    #[cfg(test)]
    pub(crate) fn latency_for(&self, node_id: NodeId) -> Option<u64> {
        self.state.latency_ms.get(&node_id).map(|v| *v)
    }
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolate_restore() {
        let (_pool, injector) = FaultAwareChannelPool::new();
        injector.isolate(1);
        assert!(injector.is_isolated(1));
        injector.restore(1);
        assert!(!injector.is_isolated(1));
    }

    #[test]
    fn test_packet_loss_set() {
        let (_pool, injector) = FaultAwareChannelPool::new();
        injector.set_packet_loss(2, 0.5);
        assert_eq!(injector.loss_for(2), Some(0.5));
    }

    #[test]
    fn test_latency_set() {
        let (_pool, injector) = FaultAwareChannelPool::new();
        injector.set_latency(3, 100);
        assert_eq!(injector.latency_for(3), Some(100));
    }

    #[test]
    fn test_clear_all() {
        let (_pool, injector) = FaultAwareChannelPool::new();
        injector.isolate(1);
        injector.set_packet_loss(2, 0.5);
        injector.set_latency(3, 100);
        injector.clear_all();
        assert!(!injector.is_isolated(1));
        assert_eq!(injector.loss_for(2), None);
        assert_eq!(injector.latency_for(3), None);
    }

    #[tokio::test]
    async fn test_bidirectional_isolation_blocks_outbound_from_isolated_node() {
        let injector = FaultInjector::new();
        let pool_1 = injector.new_pool_for_node(1);
        let _pool_2 = injector.new_pool_for_node(2);

        injector.isolate(1);

        let result = pool_1.get_or_connect(2, "127.0.0.1:9999").await;
        assert!(
            matches!(result, Err(NetworkError::ConnectionFailed { reason, .. }) if reason.contains("source node 1 is isolated"))
        );
    }

    #[tokio::test]
    async fn test_bidirectional_isolation_blocks_inbound_to_isolated_node() {
        let injector = FaultInjector::new();
        let _pool_1 = injector.new_pool_for_node(1);
        let pool_2 = injector.new_pool_for_node(2);

        injector.isolate(1);

        let result = pool_2.get_or_connect(1, "127.0.0.1:9999").await;
        assert!(
            matches!(result, Err(NetworkError::ConnectionFailed { reason, .. }) if reason.contains("destination node 1 is isolated"))
        );
    }

    #[tokio::test]
    async fn test_restore_clears_bidirectional_isolation() {
        let injector = FaultInjector::new();
        let pool_1 = injector.new_pool_for_node(1);
        injector.isolate(1);
        injector.restore(1);

        let result = pool_1.get_or_connect(99, "127.0.0.1:0").await;
        assert!(
            !matches!(&result, Err(NetworkError::ConnectionFailed { reason, .. }) if reason.contains("isolated")),
            "after restore, the isolation-specific error should not fire (any other connection error is fine)"
        );
    }

    /// L6 (2026-10-21 review): the packet-loss coin flips must be
    /// seed-reproducible — two injectors seeded identically, driven with the
    /// same sequential call pattern, must drop/pass the same calls. (Under
    /// concurrent traffic the draw ORDER can interleave differently; the
    /// guaranteed unit is one sequential stream — see module docs.)
    #[tokio::test]
    async fn packet_loss_draws_are_seed_reproducible() {
        async fn loss_pattern(seed: u64) -> Vec<bool> {
            let injector = FaultInjector::new_seeded(seed);
            let pool = injector.new_pool_for_node(1);
            injector.set_packet_loss(2, 0.5);
            let mut pattern = Vec::new();
            for _ in 0..64 {
                // Nothing listens on 127.0.0.1:9999 — every call fails; a
                // loss-drawn failure is identifiable by its reason string.
                match pool.get_or_connect(2, "127.0.0.1:9999").await {
                    Err(NetworkError::ConnectionFailed { reason, .. }) => {
                        pattern.push(reason.contains("simulated packet loss"));
                    }
                    Err(other) => panic!("unexpected error variant: {other:?}"),
                    Ok(_) => panic!("unexpected successful connection to a closed port"),
                }
            }
            pattern
        }

        let a = loss_pattern(0x5EED_0001).await;
        let b = loss_pattern(0x5EED_0001).await;
        assert_eq!(
            a, b,
            "same seed + same call sequence must replay the same loss sequence"
        );
        assert!(
            a.iter().any(|&dropped| dropped) && a.iter().any(|&dropped| !dropped),
            "p=0.5 over 64 draws must produce a non-degenerate mix (got {a:?})"
        );
        // Fixed seeds make this deterministic: StdRng streams for distinct
        // seeds diverging is the property under test, not a probabilistic one.
        let c = loss_pattern(0x5EED_0002).await;
        assert_ne!(a, c, "distinct seeds must produce distinct loss sequences");
    }
}
