use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use kv_store::{KvRequest, KvResponse, KvService};
use openraft::type_config::async_runtime::WatchReceiver as _;
use openraft::BasicNode;
use rr_config::{MemConfigProvider, MemLifecycleStore};
use rr_core::config::ConfigProvider;
use rr_core::error::{LifecycleError, RrError};
use rr_core::state_machine::StateMachine;
use rr_core::storage::StorageEngine;
use rr_core::types::{NodeAddr, NodeId};
use rr_core::{
    CasOutcome, ClusterLifecycle, ClusterOrigin, FormationState, GroupFormation, LifecycleStore,
    SeedSpec, VersionedLifecycle,
};
use rr_network::proto::admin::admin_service_client::AdminServiceClient;
use rr_network::proto::admin::{
    AddLearnerRequest, ChangeMembershipRequest, RemoveGroupMemberRequest,
};
use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
use rr_raft::service_adapter::ServiceStateMachineAdapter;
use rr_router::router::ShardRouter;
use rr_server::config::NodeConfig;
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::formation::{FormationConfig, FormationMode};
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;

use crate::wait_for_cluster_ready;

const GROUP_ID: u64 = 1;
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum Backend {
    Mem,
    #[cfg(feature = "rocks")]
    Rocks,
}

struct FlakyLifecycleStore {
    inner: MemLifecycleStore,
    unavailable: AtomicBool,
    activation_blocked: AtomicBool,
    first_activation_node: std::sync::atomic::AtomicU64,
    activation_started: tokio::sync::Notify,
    release_activation: tokio::sync::Notify,
}

impl FlakyLifecycleStore {
    fn new() -> Self {
        Self {
            inner: MemLifecycleStore::new(),
            unavailable: AtomicBool::new(false),
            activation_blocked: AtomicBool::new(false),
            first_activation_node: std::sync::atomic::AtomicU64::new(0),
            activation_started: tokio::sync::Notify::new(),
            release_activation: tokio::sync::Notify::new(),
        }
    }

    fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::SeqCst);
    }

    fn block_activation(&self) {
        self.activation_blocked.store(true, Ordering::SeqCst);
    }

    fn release_activation(&self) {
        self.activation_blocked.store(false, Ordering::SeqCst);
        self.release_activation.notify_waiters();
    }

    fn check_available(&self) -> Result<(), LifecycleError> {
        if self.unavailable.load(Ordering::SeqCst) {
            Err(LifecycleError::Unavailable("injected outage".to_string()))
        } else {
            Ok(())
        }
    }

    async fn compare_and_swap_for_node(
        &self,
        node_id: NodeId,
        expected_revision: u64,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError> {
        self.check_available()?;
        if self.activation_blocked.load(Ordering::SeqCst) {
            if self.first_activation_node.swap(node_id, Ordering::SeqCst) == 0 {
                self.activation_started.notify_one();
            }
            self.release_activation.notified().await;
        }
        self.inner.compare_and_swap(expected_revision, record).await
    }
}

#[async_trait]
impl LifecycleStore for FlakyLifecycleStore {
    async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError> {
        self.check_available()?;
        self.inner.load_linearizable().await
    }

    async fn create_if_absent(
        &self,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError> {
        self.check_available()?;
        self.inner.create_if_absent(record).await
    }

    async fn compare_and_swap(
        &self,
        expected_revision: u64,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError> {
        self.compare_and_swap_for_node(0, expected_revision, record)
            .await
    }
}

struct NodeLifecycleStore {
    node_id: NodeId,
    inner: Arc<FlakyLifecycleStore>,
}

#[async_trait]
impl LifecycleStore for NodeLifecycleStore {
    async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError> {
        self.inner.load_linearizable().await
    }

    async fn create_if_absent(
        &self,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError> {
        self.inner.create_if_absent(record).await
    }

    async fn compare_and_swap(
        &self,
        expected_revision: u64,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError> {
        self.inner
            .compare_and_swap_for_node(self.node_id, expected_revision, record)
            .await
    }
}

struct NodeSpec {
    node_id: NodeId,
    addr: String,
    admin_port: u16,
    #[cfg_attr(not(feature = "rocks"), allow(dead_code))]
    data_dir: Option<PathBuf>,
}

#[derive(Clone)]
struct TestNode {
    node: Arc<RrNode>,
    provider: Arc<MemConfigProvider>,
}

struct Cluster {
    backend: Backend,
    seed: SeedSpec,
    store: Arc<FlakyLifecycleStore>,
    specs: Vec<NodeSpec>,
    nodes: Vec<TestNode>,
    _temp_dir: Option<tempfile::TempDir>,
}

impl Cluster {
    fn new(node_count: usize, backend: Backend) -> Self {
        let temp_dir: Option<tempfile::TempDir> = match backend {
            Backend::Mem => None,
            #[cfg(feature = "rocks")]
            Backend::Rocks => Some(tempfile::TempDir::new().unwrap()),
        };
        let mut nodes = BTreeMap::new();
        let mut specs = Vec::new();
        for node_id in 1..=(node_count as NodeId) {
            let port = reserve_port();
            let addr = format!("127.0.0.1:{port}");
            nodes.insert(node_id, NodeAddr { addr: addr.clone() });
            let data_dir = temp_dir
                .as_ref()
                .map(|dir| dir.path().join(format!("node-{node_id}")));
            specs.push(NodeSpec {
                node_id,
                addr,
                admin_port: reserve_port(),
                data_dir,
            });
        }
        let seed = SeedSpec::stock("homogeneous-formation", nodes, 1).unwrap();
        Self {
            backend,
            seed,
            store: Arc::new(FlakyLifecycleStore::new()),
            specs,
            nodes: Vec::new(),
            _temp_dir: temp_dir,
        }
    }

    async fn make_node(&self, index: usize, seed: &SeedSpec) -> TestNode {
        self.make_node_with_formation(index, seed, FormationMode::Genesis)
            .await
    }

    async fn make_node_with_formation(
        &self,
        index: usize,
        seed: &SeedSpec,
        mode: FormationMode,
    ) -> TestNode {
        let spec = &self.specs[index];
        let provider = Arc::new(MemConfigProvider::new());
        let table = seed.initial_routing_table().unwrap();
        for (partition_id, (config, group_id)) in &table.partitions {
            provider
                .add_partition(
                    *partition_id,
                    config.clone(),
                    *group_id,
                    table.groups[group_id].replicas.clone(),
                )
                .await;
        }
        for (node_id, addr) in &table.nodes {
            provider.add_node(*node_id, addr.clone()).await;
        }

        let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
        let mgr = RaftGroupManager::new_with_network(spec.node_id, kv_factory(), pool);
        #[cfg(feature = "rocks")]
        let mgr = match &spec.data_dir {
            Some(dir) => mgr.with_rocks_base_path(format!("{}/raft-log", dir.display())),
            None => mgr,
        };
        let mgr = Arc::new(mgr);
        let provider_dyn: Arc<dyn ConfigProvider> = provider.clone();
        let router = Arc::new(ShardRouter::new(provider_dyn.clone()));
        let lookup = Arc::new(RaftGroupLookupImpl::new(mgr.clone()));
        let dispatch = Arc::new(ClientDispatchImpl::new(mgr.clone(), router));

        let storage: Arc<dyn StorageEngine> = match self.backend {
            Backend::Mem => Arc::new(MemStorage::new()),
            #[cfg(feature = "rocks")]
            Backend::Rocks => {
                let dir = spec.data_dir.as_ref().unwrap().join("framework");
                std::fs::create_dir_all(&dir).unwrap();
                Arc::new(
                    rr_storage::rocks::RocksStorage::open_default(dir.to_str().unwrap()).unwrap(),
                )
            }
        };
        let config = NodeConfig::new(
            spec.node_id,
            spec.addr.clone(),
            storage,
            provider_dyn,
            kv_factory(),
        )
        .with_group_manager(mgr)
        .with_raft_lookup(lookup)
        .with_client_dispatch(dispatch)
        .with_admin_port(spec.admin_port)
        .with_allow_insecure(true);
        let node = Arc::new(
            RrNode::new_with_formation(
                config,
                FormationConfig {
                    seed: seed.clone(),
                    store: Arc::new(NodeLifecycleStore {
                        node_id: spec.node_id,
                        inner: self.store.clone(),
                    }),
                    mode,
                },
            )
            .unwrap(),
        );
        TestNode { node, provider }
    }

    async fn start_all(&mut self) {
        for index in 0..self.specs.len() {
            let node = self.make_node(index, &self.seed.clone()).await;
            self.nodes.push(node);
        }
        start_nodes(&self.nodes).await;
        wait_ready(&self.nodes).await;
        assert_full_voters(&self.nodes, &self.seed.nodes.keys().copied().collect()).await;
    }

    async fn restart(&mut self, index: usize) {
        let previous = self.nodes.remove(index);
        previous.node.shutdown().await.unwrap();
        drop(previous);
        let replacement = self.make_node(index, &self.seed.clone()).await;
        self.nodes.insert(index, replacement);
        self.nodes[index].node.start().await.unwrap();
        assert_full_voters(&self.nodes, &self.seed.nodes.keys().copied().collect()).await;
    }

    async fn shutdown(&self) {
        for node in &self.nodes {
            let _ = node.node.shutdown().await;
        }
    }
}

fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn admin_client(admin_port: u16) -> AdminServiceClient<tonic::transport::Channel> {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        match tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{admin_port}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .connect()
            .await
        {
            Ok(channel) => return AdminServiceClient::new(channel),
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "admin connection failed: {error}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

fn kv_factory() -> StateMachineFactory {
    Box::new(|group_id| -> Box<dyn StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(group_id, KvService::new()))
    })
}

async fn start_nodes(nodes: &[TestNode]) {
    let mut tasks = Vec::new();
    for node in nodes {
        let node = node.node.clone();
        tasks.push(tokio::spawn(async move {
            node.start().await.map_err(|error| error.to_string())
        }));
    }
    tokio::time::timeout(TEST_TIMEOUT, async {
        for task in tasks {
            task.await.unwrap().unwrap();
        }
    })
    .await
    .expect("managed nodes must start concurrently");
}

async fn wait_ready(nodes: &[TestNode]) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        if nodes.iter().all(|node| node.node.readiness().is_ready()) {
            return;
        }
        assert!(Instant::now() < deadline, "managed readiness timed out");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn voters(node: &RrNode) -> BTreeSet<NodeId> {
    let raft = node.group_manager().get_raft(GROUP_ID).await.unwrap();
    raft.metrics()
        .borrow_watched()
        .membership_config
        .membership()
        .voter_ids()
        .collect()
}

async fn assert_full_voters(nodes: &[TestNode], expected: &BTreeSet<NodeId>) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let mut complete = true;
        for node in nodes {
            if voters(&node.node).await != *expected {
                complete = false;
                break;
            }
        }
        if complete {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "full voter convergence timed out"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn last_applied(node: &RrNode) -> Option<u64> {
    let raft = node.group_manager().get_raft(GROUP_ID).await.unwrap();
    raft.metrics()
        .borrow_watched()
        .last_applied
        .map(|log_id| log_id.index)
}

async fn await_caught_up(node: &RrNode, target: u64) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        if last_applied(node)
            .await
            .is_some_and(|index| index >= target)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node {} did not catch up to log index {target}",
            node.node_id()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn encode_request(request: &KvRequest) -> Vec<u8> {
    bincode::serde::encode_to_vec(request, bincode::config::standard()).unwrap()
}

fn decode_response(bytes: &[u8]) -> KvResponse {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .unwrap()
        .0
}

async fn request_on_node(node: &RrNode, request: &KvRequest) -> Result<KvResponse, RrError> {
    let raft = node.group_manager().get_raft(GROUP_ID).await.unwrap();
    let response = raft
        .client_write(encode_request(request).into())
        .await
        .map_err(|error| RrError::Application(error.to_string()))?;
    Ok(decode_response(response.data.as_ref()))
}

async fn request_on_leader(nodes: &[TestNode], request: &KvRequest) -> KvResponse {
    let node_refs: Vec<_> = nodes.iter().map(|node| node.node.clone()).collect();
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let leader_id = wait_for_cluster_ready(&node_refs, GROUP_ID, 5_000)
            .await
            .unwrap();
        let leader = node_refs
            .iter()
            .find(|node| node.node_id() == leader_id)
            .unwrap();
        match request_on_node(leader, request).await {
            Ok(response) => return response,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("leader request failed: {error}"),
        }
    }
}

async fn put(nodes: &[TestNode], key: &str, value: &[u8]) {
    let response = request_on_leader(
        nodes,
        &KvRequest::Put {
            key: key.to_string(),
            value: value.to_vec(),
        },
    )
    .await;
    assert_eq!(response.value, None);
}

async fn get(nodes: &[TestNode], key: &str) -> Option<Vec<u8>> {
    request_on_leader(
        nodes,
        &KvRequest::Get {
            key: key.to_string(),
        },
    )
    .await
    .value
}

async fn leader_applied_index(nodes: &[TestNode]) -> u64 {
    let node_refs: Vec<_> = nodes.iter().map(|node| node.node.clone()).collect();
    let leader_id = wait_for_cluster_ready(&node_refs, GROUP_ID, 5_000)
        .await
        .unwrap();
    let leader = node_refs
        .iter()
        .find(|node| node.node_id() == leader_id)
        .unwrap();
    last_applied(leader).await.unwrap()
}

fn permuted_seed(seed: &SeedSpec, rotation: usize) -> SeedSpec {
    let mut permuted = seed.clone();
    let nodes: Vec<_> = seed.nodes.iter().collect();
    permuted.nodes = (0..nodes.len())
        .map(|offset| {
            let (id, addr) = nodes[(offset + rotation) % nodes.len()];
            (*id, addr.clone())
        })
        .collect();
    permuted.groups = seed
        .groups
        .iter()
        .rev()
        .map(|(id, group)| (*id, group.clone()))
        .collect();
    permuted.partitions = seed
        .partitions
        .iter()
        .rev()
        .map(|(id, partition)| (*id, partition.clone()))
        .collect();
    permuted
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn homogeneous_concurrent_genesis_forms_one_cluster_for_1_3_5_nodes() {
    for node_count in [1, 3, 5] {
        let mut cluster = Cluster::new(node_count, Backend::Mem);
        cluster.start_all().await;

        let lifecycle = cluster.store.load_linearizable().await.unwrap().unwrap();
        assert_eq!(lifecycle.record.origin, ClusterOrigin::Genesis);
        assert_eq!(
            lifecycle.record.groups[&GROUP_ID].state,
            FormationState::Active
        );
        put(&cluster.nodes, "genesis", b"ack").await;
        assert_eq!(get(&cluster.nodes, "genesis").await, Some(b"ack".to_vec()));
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn admin_membership_changes_require_active_lifecycle_cluster_wide() {
    let mut cluster = Cluster::new(3, Backend::Mem);
    cluster.store.block_activation();
    for index in 0..cluster.specs.len() {
        let node = cluster.make_node(index, &cluster.seed.clone()).await;
        cluster.nodes.push(node);
    }
    let first_starting = {
        let nodes = cluster.nodes[..1].to_vec();
        tokio::spawn(async move { start_nodes(&nodes).await })
    };
    cluster.store.activation_started.notified().await;
    let remaining_starting = {
        let nodes = cluster.nodes[1..].to_vec();
        tokio::spawn(async move { start_nodes(&nodes).await })
    };

    let remote_index = 1;
    let mut admin = admin_client(cluster.specs[remote_index].admin_port).await;

    let error = admin
        .change_membership(ChangeMembershipRequest {
            group_id: GROUP_ID,
            voter_node_ids: vec![1, 2],
            force: false,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("not Active"), "{error}");
    assert!(error.message().contains("retry"), "{error}");

    let error = admin
        .add_learner(AddLearnerRequest {
            group_id: GROUP_ID,
            node_id: 4,
            addr: "127.0.0.1:65000".to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    let error = admin
        .remove_group_member(RemoveGroupMemberRequest {
            group_id: GROUP_ID,
            node_id: 3,
            force: true,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        voters(&cluster.nodes[remote_index].node).await,
        BTreeSet::from([1, 2, 3])
    );

    cluster.store.release_activation();
    first_starting.await.unwrap();
    remaining_starting.await.unwrap();
    let lifecycle = cluster.store.load_linearizable().await.unwrap().unwrap();
    assert_eq!(
        lifecycle.record.groups[&GROUP_ID].state,
        FormationState::Active
    );
    assert_eq!(
        voters(&cluster.nodes[remote_index].node).await,
        BTreeSet::from([1, 2, 3])
    );

    let node_refs: Vec<_> = cluster.nodes.iter().map(|node| node.node.clone()).collect();
    let leader_id = wait_for_cluster_ready(&node_refs, GROUP_ID, 5_000)
        .await
        .unwrap();
    let mut active_admin = admin_client(cluster.specs[(leader_id - 1) as usize].admin_port).await;
    active_admin
        .change_membership(ChangeMembershipRequest {
            group_id: GROUP_ID,
            voter_node_ids: vec![1, 2, 3],
            force: false,
        })
        .await
        .unwrap();
    cluster.store.set_unavailable(true);
    let error = active_admin
        .change_membership(ChangeMembershipRequest {
            group_id: GROUP_ID,
            voter_node_ids: vec![1, 2, 3],
            force: false,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("unavailable"), "{error}");
    assert!(error.message().contains("retry"), "{error}");
    cluster.store.set_unavailable(false);
    cluster.shutdown().await;
}

/// INV-9: managed formation preserves membership, data, and routing across recovery and adoption.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn homogeneous_roster_permutation_preserves_seed_identity_and_forms() {
    let mut cluster = Cluster::new(3, Backend::Mem);
    let expected_hash = cluster.seed.seed_hash().unwrap();
    for index in 0..cluster.specs.len() {
        let seed = permuted_seed(&cluster.seed, index);
        assert_eq!(seed.seed_hash().unwrap(), expected_hash);
        let node = cluster.make_node(index, &seed).await;
        cluster.nodes.push(node);
    }
    start_nodes(&cluster.nodes).await;
    wait_ready(&cluster.nodes).await;
    assert_full_voters(&cluster.nodes, &BTreeSet::from([1, 2, 3])).await;
    assert_eq!(
        cluster
            .store
            .load_linearizable()
            .await
            .unwrap()
            .unwrap()
            .record
            .seed_hash,
        expected_hash
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn homogeneous_conflicting_seed_fails_without_creating_second_cluster() {
    let mut cluster = Cluster::new(1, Backend::Mem);
    cluster.start_all().await;
    let before = cluster.store.load_linearizable().await.unwrap().unwrap();

    let mut conflicting = Cluster::new(1, Backend::Mem);
    conflicting.store = cluster.store.clone();
    conflicting.seed.cluster_name = "conflicting-seed".to_string();
    let node = conflicting.make_node(0, &conflicting.seed.clone()).await;
    let error = node.node.start().await.unwrap_err();
    assert!(matches!(
        error,
        RrError::Lifecycle(LifecycleError::SeedMismatch)
    ));
    assert!(voters(&node.node).await.is_empty());
    node.node.shutdown().await.unwrap();

    let after = cluster.store.load_linearizable().await.unwrap().unwrap();
    assert_eq!(after.record.cluster_uuid, before.record.cluster_uuid);
    assert_eq!(after.revision, before.revision);
    cluster.shutdown().await;
}

/// INV-9: managed formation preserves membership, data, and routing across recovery and adoption.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn homogeneous_late_initial_node_recovers_from_active_cluster() {
    let mut cluster = Cluster::new(3, Backend::Mem);
    for index in 0..2 {
        let node = cluster.make_node(index, &cluster.seed.clone()).await;
        cluster.nodes.push(node);
    }
    start_nodes(&cluster.nodes).await;
    wait_ready(&cluster.nodes).await;
    put(&cluster.nodes, "before-late-node", b"ack").await;
    let active = cluster.store.load_linearizable().await.unwrap().unwrap();

    let late = cluster.make_node(2, &cluster.seed.clone()).await;
    cluster.nodes.push(late);
    cluster.nodes[2].node.start().await.unwrap();
    assert_full_voters(&cluster.nodes, &BTreeSet::from([1, 2, 3])).await;
    let target = leader_applied_index(&cluster.nodes).await;
    await_caught_up(&cluster.nodes[2].node, target).await;
    assert_eq!(
        get(&cluster.nodes, "before-late-node").await,
        Some(b"ack".to_vec())
    );
    assert_eq!(
        cluster
            .store
            .load_linearizable()
            .await
            .unwrap()
            .unwrap()
            .revision,
        active.revision
    );
    cluster.shutdown().await;
}

/// INV-9: managed formation never initializes from Active or partially initialized local state.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn homogeneous_active_and_pristine_never_initializes() {
    let mut cluster = Cluster::new(3, Backend::Mem);
    let record = ClusterLifecycle {
        version: 1,
        cluster_uuid: "12345678-1234-1234-1234-123456789abc".parse().unwrap(),
        topology_revision: 1,
        seed_hash: cluster.seed.seed_hash().unwrap(),
        origin: ClusterOrigin::Genesis,
        groups: BTreeMap::from([(
            GROUP_ID,
            GroupFormation {
                bootstrap_epoch: 1,
                state: FormationState::Active,
            },
        )]),
    };
    cluster.store.create_if_absent(record).await.unwrap();
    let node = cluster.make_node(0, &cluster.seed.clone()).await;
    cluster.nodes.push(node);
    cluster.nodes[0].node.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(voters(&cluster.nodes[0].node).await.is_empty());
    assert!(!cluster.nodes[0].node.readiness().is_ready());
    assert_eq!(
        cluster
            .store
            .load_linearizable()
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
    cluster.shutdown().await;
}

/// INV-9: managed formation preserves membership, data, and routing across recovery and adoption.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn legacy_adoption_preserves_ack_and_expands_only_through_manual_membership() {
    let mut cluster = Cluster::new(3, Backend::Mem);
    let legacy = cluster
        .make_node_with_formation(0, &cluster.seed.clone(), FormationMode::AdoptLegacy)
        .await;
    legacy
        .node
        .group_manager()
        .start_group(GROUP_ID)
        .await
        .unwrap();
    legacy
        .node
        .group_manager()
        .init_cluster(
            GROUP_ID,
            BTreeMap::from([(
                1,
                BasicNode {
                    addr: cluster.specs[0].addr.clone(),
                },
            )]),
        )
        .await
        .unwrap();
    cluster.nodes.push(legacy);
    put(&cluster.nodes, "legacy-ack", b"ack").await;
    assert_eq!(voters(&cluster.nodes[0].node).await, BTreeSet::from([1]));

    cluster.nodes[0].node.start().await.unwrap();
    wait_ready(&cluster.nodes).await;
    let adopted = cluster.store.load_linearizable().await.unwrap().unwrap();
    assert_eq!(adopted.record.origin, ClusterOrigin::LegacyAdopted);
    assert_eq!(
        adopted.record.groups[&GROUP_ID].state,
        FormationState::Active
    );
    assert_eq!(voters(&cluster.nodes[0].node).await, BTreeSet::from([1]));
    assert_eq!(
        get(&cluster.nodes, "legacy-ack").await,
        Some(b"ack".to_vec())
    );

    for index in 1..cluster.specs.len() {
        let pristine = cluster
            .make_node_with_formation(index, &cluster.seed.clone(), FormationMode::AdoptLegacy)
            .await;
        pristine.node.start().await.unwrap();
        assert!(voters(&pristine.node).await.is_empty());
        assert!(!pristine.node.readiness().is_ready());
        cluster.nodes.push(pristine);
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(voters(&cluster.nodes[0].node).await, BTreeSet::from([1]));
    assert!(voters(&cluster.nodes[1].node).await.is_empty());
    assert!(voters(&cluster.nodes[2].node).await.is_empty());

    let manager = cluster.nodes[0].node.group_manager();
    for index in 1..cluster.specs.len() {
        manager
            .add_learner(
                GROUP_ID,
                cluster.specs[index].node_id,
                cluster.specs[index].addr.clone(),
            )
            .await
            .unwrap();
    }
    manager
        .change_membership(GROUP_ID, BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();

    assert_full_voters(&cluster.nodes, &BTreeSet::from([1, 2, 3])).await;
    wait_ready(&cluster.nodes).await;
    assert_eq!(
        get(&cluster.nodes, "legacy-ack").await,
        Some(b"ack".to_vec())
    );
    let expected_partitions = cluster.seed.initial_routing_table().unwrap().partitions;
    for node in &cluster.nodes {
        assert_eq!(
            node.provider.get_routing_table().await.unwrap().partitions,
            expected_partitions
        );
    }
    assert_eq!(
        cluster
            .store
            .load_linearizable()
            .await
            .unwrap()
            .unwrap()
            .revision,
        adopted.revision
    );
    cluster.shutdown().await;
}

/// INV-9: managed formation never initializes from Active or partially initialized local state.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn legacy_adoption_rejects_mixed_initialized_and_pristine_groups() {
    let mut cluster = Cluster::new(1, Backend::Mem);
    cluster.seed = SeedSpec::stock("homogeneous-formation", cluster.seed.nodes.clone(), 2).unwrap();
    let node = cluster
        .make_node_with_formation(0, &cluster.seed.clone(), FormationMode::AdoptLegacy)
        .await;
    node.node.group_manager().start_group(1).await.unwrap();
    node.node
        .group_manager()
        .init_cluster(
            1,
            BTreeMap::from([(
                1,
                BasicNode {
                    addr: cluster.specs[0].addr.clone(),
                },
            )]),
        )
        .await
        .unwrap();
    assert_full_voters(std::slice::from_ref(&node), &BTreeSet::from([1])).await;

    let error = node.node.start().await.unwrap_err();

    assert!(error.to_string().contains("all local groups"), "{error}");
    assert!(cluster.store.load_linearizable().await.unwrap().is_none());
    assert_eq!(voters(&node.node).await, BTreeSet::from([1]));
    let second = node.node.group_manager().get_raft(2).await.unwrap();
    assert!(second
        .metrics()
        .borrow_watched()
        .membership_config
        .membership()
        .voter_ids()
        .next()
        .is_none());
    node.node.shutdown().await.unwrap();
}

/// INV-9: managed formation never initializes from Active or partially initialized local state.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn legacy_adoption_rejects_pristine_only_local_state() {
    let cluster = Cluster::new(1, Backend::Mem);
    let node = cluster
        .make_node_with_formation(0, &cluster.seed.clone(), FormationMode::AdoptLegacy)
        .await;

    let error = node.node.start().await.unwrap_err();

    assert!(error.to_string().contains("all local groups"), "{error}");
    assert!(cluster.store.load_linearizable().await.unwrap().is_none());
    assert!(voters(&node.node).await.is_empty());
    assert!(!node.node.readiness().is_ready());
    node.node.shutdown().await.unwrap();
}

/// INV-9: managed formation preserves membership, data, and routing across recovery and adoption.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn homogeneous_mem_rolling_restart_preserves_acked_writes() {
    let mut cluster = Cluster::new(3, Backend::Mem);
    cluster.start_all().await;
    let active = cluster.store.load_linearizable().await.unwrap().unwrap();

    for index in 0..cluster.specs.len() {
        let key = format!("rolling-{index}");
        put(&cluster.nodes, &key, b"ack").await;
        let target = leader_applied_index(&cluster.nodes).await;
        cluster.restart(index).await;
        await_caught_up(&cluster.nodes[index].node, target).await;
        assert_eq!(get(&cluster.nodes, &key).await, Some(b"ack".to_vec()));
    }
    for index in 0..cluster.specs.len() {
        assert_eq!(
            get(&cluster.nodes, &format!("rolling-{index}")).await,
            Some(b"ack".to_vec())
        );
    }
    assert_eq!(
        cluster
            .store
            .load_linearizable()
            .await
            .unwrap()
            .unwrap()
            .revision,
        active.revision
    );
    cluster.shutdown().await;
}

/// INV-9: managed formation preserves membership, data, and routing across recovery and adoption.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn homogeneous_wiped_mem_follower_recovers_ack_from_surviving_replicas() {
    let mut cluster = Cluster::new(3, Backend::Mem);
    cluster.start_all().await;
    put(&cluster.nodes, "wiped-follower", b"ack").await;
    let target = leader_applied_index(&cluster.nodes).await;
    let node_refs: Vec<_> = cluster.nodes.iter().map(|node| node.node.clone()).collect();
    let leader_id = wait_for_cluster_ready(&node_refs, GROUP_ID, 5_000)
        .await
        .unwrap();
    let wiped = cluster
        .nodes
        .iter()
        .position(|node| node.node.node_id() != leader_id)
        .unwrap();

    cluster.restart(wiped).await;
    await_caught_up(&cluster.nodes[wiped].node, target).await;
    let recovered = &cluster.nodes[wiped];
    let recovered_id = recovered.node.node_id();
    let recovered_addr = recovered.provider.get_routing_table().await.unwrap().nodes[&recovered_id]
        .addr
        .clone();
    let client = rr_client::RrClient::connect(vec![recovered_addr])
        .await
        .unwrap();
    let response = client
        .read(
            GROUP_ID,
            encode_request(&KvRequest::Get {
                key: "wiped-follower".to_string(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(decode_response(&response).value, Some(b"ack".to_vec()));
    cluster.shutdown().await;
}

/// INV-9: managed formation preserves membership, data, and routing across recovery and adoption.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn homogeneous_store_outage_keeps_writes_and_partition_routes_unchanged() {
    let mut cluster = Cluster::new(3, Backend::Mem);
    cluster.start_all().await;
    let before: Vec<_> = cluster
        .nodes
        .iter()
        .map(|node| node.provider.get_routing_table())
        .collect();
    let mut before_routes = Vec::new();
    for table in before {
        before_routes.push(table.await.unwrap().partitions);
    }

    cluster.store.set_unavailable(true);
    tokio::time::sleep(Duration::from_millis(250)).await;
    put(&cluster.nodes, "during-outage", b"ack").await;
    assert_eq!(
        get(&cluster.nodes, "during-outage").await,
        Some(b"ack".to_vec())
    );
    for (index, node) in cluster.nodes.iter().enumerate() {
        assert!(!node.node.readiness().is_ready());
        assert_eq!(
            node.provider.get_routing_table().await.unwrap().partitions,
            before_routes[index]
        );
    }
    cluster.store.set_unavailable(false);
    cluster.shutdown().await;
}

#[cfg(feature = "rocks")]
/// INV-9: managed formation preserves membership, data, and routing across recovery and adoption.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn homogeneous_rocks_rolling_restart_recovers_without_replaying_seed() {
    let mut cluster = Cluster::new(3, Backend::Rocks);
    cluster.start_all().await;
    let active = cluster.store.load_linearizable().await.unwrap().unwrap();

    for index in 0..cluster.specs.len() {
        let key = format!("rocks-{index}");
        put(&cluster.nodes, &key, b"durable-ack").await;
        let target = leader_applied_index(&cluster.nodes).await;
        cluster.restart(index).await;
        await_caught_up(&cluster.nodes[index].node, target).await;
        assert_eq!(
            get(&cluster.nodes, &key).await,
            Some(b"durable-ack".to_vec())
        );
    }
    for index in 0..cluster.specs.len() {
        assert_eq!(
            get(&cluster.nodes, &format!("rocks-{index}")).await,
            Some(b"durable-ack".to_vec())
        );
    }
    let after = cluster.store.load_linearizable().await.unwrap().unwrap();
    assert_eq!(after.record.cluster_uuid, active.record.cluster_uuid);
    assert_eq!(after.revision, active.revision);
    cluster.shutdown().await;
}
