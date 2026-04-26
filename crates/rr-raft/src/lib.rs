//! rr-raft — Raft consensus implementation and log replication
//!
//! Part of the `rr` distributed framework.

pub mod group_manager;
pub mod idempotency;
pub mod log_storage;
pub mod network_factory;
pub mod raft_node;
pub mod service_adapter;
pub mod sm_adapter;
pub mod type_config;

#[cfg(feature = "rocks")]
pub mod rocks_log_storage;
#[cfg(feature = "rocks")]
pub use rocks_log_storage::RocksLogStorage;

pub use group_manager::RaftGroupManager;
pub use idempotency::{IdempotencyLayer, DEFAULT_IDEMPOTENCY_CACHE_SIZE};
pub use log_storage::RrLogStorage;
pub use network_factory::GrpcNetworkFactory;
pub use network_factory::GrpcRaftNetwork;
pub use raft_node::RaftNode;
pub use service_adapter::ServiceStateMachineAdapter;
pub use sm_adapter::RrStateMachine;
pub use type_config::RrTypeConfig;
