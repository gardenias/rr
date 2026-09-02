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
//! [`ConfigProvider`](rr_core::config::ConfigProvider) and exposes two
//! operations:
//!
//! * [`ShardRouter::route`] — resolve a partition ID to a group ID.
//! * [`ShardRouter::routing_table`] — fetch the full routing table snapshot.
//!
//! Leader discovery is deliberately **not** part of routing: the routing
//! table is configuration-plane data and never carries raft runtime state.
//! Clients converge on the real leader via `NotLeader` / `LeaderInfo`
//! redirects and the client-side leader cache (see ADR A-0013).
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
//! ```

pub mod router;
pub use router::ShardRouter;
