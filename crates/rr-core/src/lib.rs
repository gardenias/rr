//! rr-core — Core types, traits, and errors for the rr distributed framework.

pub mod codec;
pub mod config;
pub mod error;
pub mod service;
pub mod state_machine;
pub mod storage;
pub mod types;

pub use codec::{BincodeCodec, Codec};
pub use config::{ConfigProvider, RoutingTableChange};
pub use error::{
    CodecError, ConfigError, NetworkError, RaftError, RouterError, RrError, StorageError,
};
pub use service::Service;
pub use state_machine::StateMachine;
pub use storage::{BatchOp, StorageEngine};
pub use types::{
    GroupId, GroupMeta, LogIndex, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable,
};
