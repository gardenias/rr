//! rr-core — Core types, traits, and errors for the rr distributed framework.

pub mod error;
pub mod types;

pub use error::{
    CodecError, ConfigError, NetworkError, RaftError, RouterError, RrError, StorageError,
};
pub use types::{
    GroupId, GroupMeta, LogIndex, NodeAddr, NodeId, PartitionConfig, PartitionId, RoutingTable,
};
