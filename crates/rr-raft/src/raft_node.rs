//! `RaftNode` — wraps `openraft::Raft<RrTypeConfig>` for a single Raft group.

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;

use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::v2::RaftNetworkV2;
use openraft::network::NetSnapshot;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::VoteOf;
use openraft::BasicNode;
use openraft::Config;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::SnapshotPolicy;

use rr_core::error::{ConfigError, RaftError, RrError};
use rr_core::state_machine::StateMachine;
use rr_core::types::GroupId;

use crate::log_storage::RrLogStorage;
use crate::sm_adapter::RrStateMachine;
use crate::type_config::RrTypeConfig;

/// A single Raft group node.
pub struct RaftNode {
    /// The group this node belongs to.
    pub group_id: GroupId,
    /// The underlying openraft instance.
    pub raft: Raft<RrTypeConfig, RrStateMachine>,
    /// Close handle for the RocksDB-backed log storage, retained so
    /// `RaftGroupManager::stop_group` can flush pending appends after
    /// `raft.shutdown()` (ADR A-0011 C3). `None` for in-memory storage.
    #[cfg(feature = "rocks")]
    pub(crate) log_storage_close: Option<crate::rocks_log_storage::RocksLogStorage>,
}

impl RaftNode {
    /// Create a new `RaftNode` with in-memory storage and a provided network factory.
    ///
    /// The network factory is generic over the actual implementation, allowing
    /// different network transports (gRPC, mocked, etc.) to be injected.
    ///
    /// Returns an error if the Raft config is invalid or if openraft fails
    /// to start (e.g., storage I/O error). Does not panic.
    pub async fn new_with_factory<NF>(
        group_id: GroupId,
        node_id: u64,
        state_machine: Box<dyn StateMachine>,
        network_factory: NF,
        snapshot_logs_since_last: u64,
    ) -> Result<Self, RrError>
    where
        NF: RaftNetworkFactory<RrTypeConfig> + 'static,
        NF::Network: NetSnapshot<RrTypeConfig, SnapshotData = Cursor<Vec<u8>>>,
    {
        let config = Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                enable_pre_vote: Some(true),
                install_snapshot_timeout: 60_000,
                snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_logs_since_last),
                ..Default::default()
            }
            .validate()
            .map_err(|e| {
                RrError::Config(ConfigError::InvalidConfig(format!(
                    "raft config validation failed: {e}"
                )))
            })?,
        );

        let log_storage = RrLogStorage::default();
        let sm = RrStateMachine::new(state_machine).with_group_id(group_id);
        let network = network_factory;

        let raft = Raft::new(node_id, config, network, log_storage, sm)
            .await
            .map_err(|e| {
                RrError::Raft(RaftError::InitializationFailed {
                    reason: format!("openraft::Raft::new failed: {e}"),
                })
            })?;

        Ok(Self {
            group_id,
            raft,
            #[cfg(feature = "rocks")]
            log_storage_close: None,
        })
    }

    /// Create a new `RaftNode` for single-node clusters using `NoopNetworkFactory`.
    ///
    /// This is a convenience constructor for scenarios where no inter-node networking
    /// is needed (e.g., single-node clusters or tests). Uses the framework default
    /// snapshot policy (`LogsSinceLast(50_000)`) — for tunable values use
    /// [`Self::new_with_factory`] directly.
    pub async fn new_single_node(
        group_id: GroupId,
        node_id: u64,
        state_machine: Box<dyn StateMachine>,
    ) -> Result<Self, RrError> {
        Self::new_with_factory(
            group_id,
            node_id,
            state_machine,
            NoopNetworkFactory,
            crate::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST,
        )
        .await
    }

    /// Initialize this node as a single-node cluster.
    pub async fn init_single_node(&self, node_id: u64) -> Result<(), rr_core::error::RrError> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            node_id,
            BasicNode {
                addr: String::new(),
            },
        );
        self.raft.initialize(nodes).await.map_err(|e| {
            rr_core::error::RrError::Raft(rr_core::error::RaftError::InitializationFailed {
                reason: e.to_string(),
            })
        })?;
        Ok(())
    }

    /// Create a new `RaftNode` with RocksDB-backed log storage and a provided network factory.
    ///
    /// This is feature-gated behind `#[cfg(feature = "rocks")]`. The log storage
    /// is opened from `rocks_path` (a directory path for the RocksDB database).
    ///
    /// Returns an error if the Raft config is invalid, RocksDB cannot be opened
    /// (disk full, permission denied, corrupt DB, etc.), or openraft fails to
    /// start. Does not panic.
    #[cfg(feature = "rocks")]
    pub async fn new_with_rocks_factory<NF>(
        group_id: GroupId,
        node_id: u64,
        state_machine: Box<dyn StateMachine>,
        network_factory: NF,
        rocks_path: &str,
        snapshot_logs_since_last: u64,
    ) -> Result<Self, RrError>
    where
        NF: RaftNetworkFactory<RrTypeConfig> + 'static,
        NF::Network: NetSnapshot<RrTypeConfig, SnapshotData = Cursor<Vec<u8>>>,
    {
        let config = Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                enable_pre_vote: Some(true),
                install_snapshot_timeout: 60_000,
                snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_logs_since_last),
                ..Default::default()
            }
            .validate()
            .map_err(|e| {
                RrError::Config(ConfigError::InvalidConfig(format!(
                    "raft config validation failed: {e}"
                )))
            })?,
        );

        let log_storage =
            crate::rocks_log_storage::RocksLogStorage::open(rocks_path).map_err(|e| {
                RrError::Raft(RaftError::InitializationFailed {
                    reason: format!("failed to open RocksLogStorage at {rocks_path}: {e}"),
                })
            })?;
        // Keep a cheap clone (Arc inside) as the close handle: `stop_group`
        // flushes the batcher after `raft.shutdown()` (ADR A-0011 C3).
        let log_storage_close = Some(log_storage.clone());
        // Durability sidecars (A-0016 membership + T1/R-P0 snapshot). With a
        // durable-but-purgeable Raft log and an in-memory state machine, the
        // committed membership and every SM snapshot must be persisted or a
        // restart after a log purge loses them: the membership wedge (zero
        // voters, perf-lab W7) and the silent empty-SM rebuild respectively.
        // `with_snapshot_store` loads any persisted snapshot so OpenRaft's
        // startup `restore_from_snapshot` re-installs it; a present-but-
        // undecodable snapshot fails startup loudly rather than losing data.
        let sm = match RrStateMachine::new(state_machine)
            .with_group_id(group_id)
            .with_membership_sidecar(std::path::PathBuf::from(rocks_path).join("sm-membership.bin"))
            .with_snapshot_store(crate::snapshot_store::SnapshotStore::new(
                std::path::PathBuf::from(rocks_path).join("sm-snapshot.bin"),
            )) {
            Ok(sm) => sm,
            Err(e) => {
                // The batcher spawned by RocksLogStorage::open holds an
                // Arc<RocksStorage> — and with it the RocksDB LOCK — until
                // the worker drains. Close explicitly so a failed construct
                // releases the LOCK promptly instead of depending on
                // Arc-drop timing.
                if let Err(close_err) = log_storage.close().await {
                    tracing::warn!(
                        error = %close_err,
                        path = %rocks_path,
                        "log storage close failed during startup cleanup"
                    );
                }
                return Err(RrError::Raft(RaftError::InitializationFailed {
                    reason: format!(
                        "failed to load persisted snapshot at {rocks_path}/sm-snapshot.bin: {e}"
                    ),
                }));
            }
        };
        let network = network_factory;

        let raft = match Raft::new(node_id, config, network, log_storage, sm).await {
            Ok(raft) => raft,
            Err(e) => {
                // Same cleanup as above: drain the batcher now so the RocksDB
                // LOCK is released before we return the error.
                if let Some(handle) = &log_storage_close {
                    if let Err(close_err) = handle.close().await {
                        tracing::warn!(
                            error = %close_err,
                            path = %rocks_path,
                            "log storage close failed during startup cleanup"
                        );
                    }
                }
                return Err(RrError::Raft(RaftError::InitializationFailed {
                    reason: format!("openraft::Raft::new failed: {e}"),
                }));
            }
        };

        Ok(Self {
            group_id,
            raft,
            log_storage_close,
        })
    }
}

// ---------------------------------------------------------------------------
// No-op network for single-node / test scenarios
// ---------------------------------------------------------------------------

pub struct NoopNetworkFactory;

impl RaftNetworkFactory<RrTypeConfig> for NoopNetworkFactory {
    type Network = NoopNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        NoopNetwork
    }
}

pub struct NoopNetwork;

impl RaftNetworkV2<RrTypeConfig> for NoopNetwork {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<RrTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<RrTypeConfig>, RPCError<RrTypeConfig>> {
        Err(RPCError::Unreachable(Unreachable::new(
            &std::io::Error::other("noop network"),
        )))
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<RrTypeConfig>,
        _snapshot: SnapshotOf<RrTypeConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<RrTypeConfig>, StreamingError<RrTypeConfig>> {
        Err(StreamingError::from(RPCError::Unreachable(
            Unreachable::new(&std::io::Error::other("noop network")),
        )))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<RrTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RrTypeConfig>, RPCError<RrTypeConfig>> {
        Err(RPCError::Unreachable(Unreachable::new(
            &std::io::Error::other("noop network"),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rr_core::error::RrError;
    use rr_core::types::LogIndex;
    use std::sync::{Arc, Mutex};

    struct TestSM {
        applied: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl TestSM {
        fn new() -> (Self, Arc<Mutex<Vec<Vec<u8>>>>) {
            let applied = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    applied: applied.clone(),
                },
                applied,
            )
        }
    }

    #[async_trait]
    impl StateMachine for TestSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            self.applied.lock().unwrap().push(entry.to_vec());
            Ok(entry.to_vec())
        }

        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }

        async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RrError> {
            Ok(())
        }

        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    #[tokio::test]
    async fn test_single_node_raft_propose() {
        let (sm, applied) = TestSM::new();
        let node = RaftNode::new_single_node(1, 1, Box::new(sm)).await.unwrap();

        // Initialize as single-node cluster
        node.init_single_node(1).await.unwrap();

        // Wait briefly for the leader to be elected
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Propose an entry
        let result = node.raft.client_write(vec![1u8, 2, 3].into()).await;
        assert!(result.is_ok(), "client_write failed: {:?}", result.err());

        // Verify the state machine received the entry
        let entries = applied.lock().unwrap();
        assert!(
            entries.contains(&vec![1u8, 2, 3]),
            "state machine should have applied the entry, got: {:?}",
            *entries,
        );
    }

    #[tokio::test]
    async fn test_raft_node_accepts_noop_factory() {
        let (sm, _applied) = TestSM::new();
        let node = RaftNode::new_with_factory(
            1,
            1,
            Box::new(sm),
            NoopNetworkFactory,
            crate::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST,
        )
        .await
        .unwrap();
        assert_eq!(node.group_id, 1);
    }

    #[tokio::test]
    async fn test_raft_node_accepts_injected_factory() {
        let (sm, _applied) = TestSM::new();
        // Verify we can pass a custom factory implementation
        // For now, we reuse NoopNetworkFactory as both the injected and expected factory
        let node = RaftNode::new_with_factory(
            2,
            2,
            Box::new(sm),
            NoopNetworkFactory,
            crate::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST,
        )
        .await
        .unwrap();
        assert_eq!(node.group_id, 2);
    }

    #[tokio::test]
    async fn test_raft_node_accepts_custom_snapshot_policy() {
        let (sm, _applied) = TestSM::new();
        // 100 logs since last — verifies the parameter is plumbed all the way
        // into openraft's `Config::snapshot_policy`.
        let node = RaftNode::new_with_factory(3, 3, Box::new(sm), NoopNetworkFactory, 100)
            .await
            .unwrap();
        assert_eq!(node.group_id, 3);
    }

    /// A failed construct must release the RocksDB LOCK promptly: the batcher
    /// spawned by `RocksLogStorage::open` holds an `Arc<RocksStorage>` until
    /// drained, so the error paths close it explicitly instead of relying on
    /// Arc-drop timing.
    #[cfg(feature = "rocks")]
    #[tokio::test]
    async fn test_failed_rocks_construct_releases_lock() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("db");
        std::fs::create_dir_all(&path).unwrap();
        // Corrupt the persisted snapshot: `with_snapshot_store` fails closed.
        std::fs::write(path.join("sm-snapshot.bin"), b"not-a-snapshot-envelope").unwrap();

        let (sm, _applied) = TestSM::new();
        let result = RaftNode::new_with_rocks_factory(
            1,
            1,
            Box::new(sm),
            NoopNetworkFactory,
            path.to_str().unwrap(),
            100,
        )
        .await;
        assert!(result.is_err(), "corrupt snapshot must fail construction");

        // Without the explicit close on the error path, reopening would race
        // the RocksDB LOCK held by the still-draining batcher worker.
        let reopened = crate::rocks_log_storage::RocksLogStorage::open(path.to_str().unwrap())
            .expect("reopen must succeed immediately after the failed construct");
        reopened.close().await.unwrap();
    }
}
