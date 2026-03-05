//! rr-config — Configuration providers for the rr distributed framework.
//!
//! This crate provides concrete implementations of the
//! [`ConfigProvider`](rr_core::config::ConfigProvider) trait defined in
//! `rr-core`. A `ConfigProvider` gives the rest of the framework read-access
//! to the cluster's [`RoutingTable`](rr_core::types::RoutingTable) and a
//! change-notification stream for hot-reload / dynamic reconfiguration.
//!
//! ## Architecture
//!
//! The primary implementation shipped here is [`MemConfigProvider`], an
//! in-memory provider backed by a `tokio::sync::RwLock` and a `broadcast`
//! channel. It is designed for **testing and development**; production
//! deployments would typically plug in an etcd- or file-backed provider
//! that implements the same trait.
//!
//! Key capabilities:
//!
//! * **Routing table reads** — `get_routing_table()` returns a snapshot of
//!   partitions, groups, and node addresses.
//! * **Change watching** — `watch_changes()` returns a `Stream` of
//!   [`RoutingTableChange`](rr_core::config::RoutingTableChange) events so
//!   consumers can react to partition/node/group mutations in real time.
//! * **Node address lookup** — `get_node_addr(node_id)` resolves a node to
//!   its gRPC address.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rr_config::MemConfigProvider;
//! use rr_core::config::ConfigProvider;
//! use rr_core::types::PartitionConfig;
//!
//! let provider = MemConfigProvider::new();
//!
//! // Register a partition (partition_id=1, group_id=100, replicas=[1,2,3]).
//! provider
//!     .add_partition(1, PartitionConfig::SingleId(1), 100, vec![1, 2, 3])
//!     .await;
//!
//! // Read the routing table back.
//! let table = provider.get_routing_table().await.unwrap();
//! assert_eq!(table.partitions.len(), 1);
//! ```

pub mod mem;
pub use mem::MemConfigProvider;
