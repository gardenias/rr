//! rr-raft — Raft consensus implementation and log replication
//!
//! Part of the `rr` distributed framework.

/// Default value for openraft's [`openraft::SnapshotPolicy::LogsSinceLast`].
///
/// At 100k ops/s this triggers a snapshot roughly every 500ms — see
/// [ADR A-0008](../../../.sisyphus/adr/A-0008-snapshot-strategy.md) for the
/// trade-off table between snapshot frequency and recovery replay length.
/// Override per node via `NodeConfig::snapshot_logs_since_last`.
pub const DEFAULT_SNAPSHOT_LOGS_SINCE_LAST: u64 = 50_000;

pub mod group_manager;
pub mod idempotency;
pub mod ingestor_runner;
pub mod learner_promoter;
pub mod log_storage;
pub mod metrics_ticker;
pub mod network_factory;
pub mod output_runner;
pub mod raft_node;
pub mod service_adapter;
pub mod sm_adapter;
pub mod tcp_transport;
pub mod type_config;

#[cfg(feature = "rocks")]
pub mod io_batcher;
#[cfg(feature = "rocks")]
pub use io_batcher::{IoBatcher, IoBatcherConfig};

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
pub use tcp_transport::{TcpConnectionPool, TcpRaftNetwork, TcpRaftNetworkFactory};
pub use type_config::RrTypeConfig;
