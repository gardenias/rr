//! rr-raft — Raft consensus implementation and log replication
//!
//! Part of the `rr` distributed framework.

pub mod type_config;
pub mod log_storage;
pub mod sm_adapter;
pub mod raft_node;
pub mod group_manager;
pub mod network_factory;

pub use type_config::RrTypeConfig;
pub use log_storage::RrLogStorage;
pub use sm_adapter::RrStateMachine;
pub use raft_node::RaftNode;
pub use group_manager::RaftGroupManager;
pub use network_factory::GrpcNetworkFactory;
pub use network_factory::GrpcRaftNetwork;
