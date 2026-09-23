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
//! | `membership` | [`MIN_VOTERS_AFTER_REMOVAL`] — change-membership quorum policy |
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

pub mod batch;
pub mod codec;
pub mod config;
pub mod deferred;
pub mod error;
pub mod framed_entry;
pub mod framed_snapshot;
pub mod idempotency;
pub mod ingestor;
pub mod membership;
pub mod output;
pub mod service;
pub mod state_machine;
pub mod storage;
#[cfg(feature = "test-utils")]
pub mod storage_fault;
pub mod types;

pub use codec::{BincodeCodec, Codec};
pub use config::{ConfigProvider, RoutingTableChange};
pub use deferred::{CompletedResponse, CorrelationId, DeferredApply, DeferredHandleResult};
pub use error::{
    CodecError, ConfigError, NetworkError, RaftError, RouterError, RrError, StorageError,
};
pub use framed_entry::{FramedEntry, FRAMED_ENTRY_MAGIC};
pub use framed_snapshot::{
    FramedSnapshot, FrameworkState, IdempotencyState, IngestorState, OutboxState,
    FRAMED_SNAPSHOT_MAGIC, FRAMED_SNAPSHOT_VERSION,
};
pub use idempotency::{ApplyOutcome, IdempotencyDecision, IdempotencyError, IdempotencyKey};
pub use ingestor::{CommandIngestor, DynCommandIngestor, DynIngressCommand, IngressCommand};
pub use membership::{membership_change_is_safe, MIN_VOTERS_AFTER_REMOVAL};
pub use output::{HandleResult, Output, OutputTarget};
pub use service::Service;
pub use state_machine::StateMachine;
pub use storage::{BatchOp, StorageEngine};
pub use types::{
    ApplyContext, GroupId, GroupMeta, LogIndex, NodeAddr, NodeId, PartitionConfig, PartitionId,
    RoutingTable,
};
