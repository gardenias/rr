//! rr-router — Partition-to-group routing for the rr distributed framework.
//!
//! This crate maps **partition IDs** to **Raft group IDs** using a simple
//! table lookup against the current [`RoutingTable`](rr_core::types::RoutingTable).
//! There is no consistent hashing or hash-ring logic — the routing table
//! provided by a [`ConfigProvider`](rr_core::config::ConfigProvider) is the
//! single source of truth.
//!
//! ## Architecture
//!
//! The main entry point is [`ShardRouter`], which wraps a
//! [`ConfigProvider`](rr_core::config::ConfigProvider) and exposes three
//! operations:
//!
//! * [`ShardRouter::route`] — resolve a partition ID to a group ID.
//! * [`ShardRouter::get_leader`] — find the current leader node for a group.
//! * [`ShardRouter::get_leader_addr`] — get the network address of the
//!   leader for a given partition (combines route + leader + address lookup).
//!
//! ## Usage
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use rr_router::ShardRouter;
//! use rr_config::MemConfigProvider;
//!
//! let config = Arc::new(MemConfigProvider::new());
//! let router = ShardRouter::new(config);
//!
//! // Route partition 1 to its Raft group.
//! let group_id = router.route(1).await?;
//!
//! // Get the leader's network address for partition 1.
//! let addr = router.get_leader_addr(1).await?;
//! ```

pub mod router;
pub use router::ShardRouter;
