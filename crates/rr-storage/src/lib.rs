//! rr-storage — Pluggable storage backends for the rr distributed framework.
//!
//! This crate provides concrete [`StorageEngine`](rr_core::storage::StorageEngine)
//! implementations used by the Raft log, state machine, and metadata layers.
//!
//! ## Architecture
//!
//! Two backends are available:
//!
//! * **[`MemStorage`]** — An in-memory backend backed by `BTreeMap` per column
//!   family. Ideal for testing, development, and ephemeral workloads. Data is
//!   **not** persisted to disk.
//! * **`RocksStorage`** *(feature = `"rocks"`)* — A persistent backend built on
//!   RocksDB. Enable it with `cargo build --features rocks`.
//!
//! Both backends implement the same `StorageEngine` trait from `rr-core`, so
//! they can be swapped transparently at the application level.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use rr_storage::MemStorage;
//! use rr_core::storage::StorageEngine;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let storage = MemStorage::new();
//!
//! // Write and read from the pre-created "state_machine" column family.
//! storage.put("state_machine", b"key", b"value").await?;
//! let val = storage.get("state_machine", b"key").await?;
//! assert_eq!(val, Some(b"value".to_vec()));
//! # Ok(())
//! # }
//! ```

pub mod mem;
pub use mem::MemStorage;

#[cfg(feature = "rocks")]
pub mod rocks;
