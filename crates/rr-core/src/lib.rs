//! rr-core — Core types, traits, and errors for the rr distributed framework.
//!
//! This crate is the **trait boundary layer** for the rr multi-Raft framework.
//! It defines the shared abstractions that all other `rr-*` crates depend on,
//! without providing any concrete implementations. By centralising trait
//! definitions here, downstream crates can be compiled and tested independently
//! while remaining compatible at the type level.
//!
//! ## Architecture
//!
//! `rr-core` exports the following key items:
//!
//! | Module | Key exports |
//! |----------------|----------------------------------------------|
//! | `service` | [`Service`] — high-level request handler |
//! | `state_machine`| [`StateMachine`] — low-level Raft SM trait |
//! | `storage` | [`StorageEngine`], [`BatchOp`] |
//! | `config` | [`ConfigProvider`], [`RoutingTableChange`] |
//! | `codec` | [`Codec`], [`BincodeCodec`] |
//! | `error` | [`RrError`] and domain-specific error enums |
//! | `types` | [`NodeId`], [`GroupId`], [`RoutingTable`], … |
//!
//! No networking, no storage backends, no Raft implementation — just
//! the contracts that glue the system together.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rr_core::{Service, Codec, BincodeCodec, RrError, NodeId};
//!
//! // Implement the Service trait to define your application logic.
//! // The framework handles Raft consensus and replication.
//! struct MyService;
//!
//! #[async_trait::async_trait]
//! impl Service for MyService {
//!     async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
//!         Ok(request) // echo
//!     }
//!     fn name(&self) -> &str { "echo" }
//!     async fn snapshot(&self) -> Result<Vec<u8>, RrError> { Ok(vec![]) }
//!     async fn restore(&mut self, _data: &[u8]) -> Result<(), RrError> { Ok(()) }
//! }
//! ```

pub mod codec;
pub mod config;
pub mod error;
pub mod framed_snapshot;
pub mod service;
pub mod state_machine;
pub mod storage;
pub mod types;

pub use codec::{BincodeCodec, Codec};
pub use config::{ConfigProvider, RoutingTableChange};
pub use error::{
    CodecError, ConfigError, NetworkError, RaftError, RouterError, RrError, StorageError,
};
pub use framed_snapshot::{
    FramedSnapshot, FrameworkState, IdempotencyState, IngestorState, FRAMED_SNAPSHOT_MAGIC,
    FRAMED_SNAPSHOT_VERSION,
};
pub use service::Service;
pub use state_machine::StateMachine;
pub use storage::{BatchOp, StorageEngine};
pub use types::{
    GroupId, GroupMeta, LogIndex, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable,
};
