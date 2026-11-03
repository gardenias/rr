use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kv_store::{KvRequest, KvResponse};
use rr_client::{RetryConfig, RrClient, RrClientBuilder};
use rr_config_etcd::EtcdLifecycleStore;
use rr_core::{ClusterOrigin, FormationState, LifecycleStore, NodeAddr, SeedSpec};
use rr_network::proto::admin::{admin_service_client::AdminServiceClient, GroupId};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

const GROUP_ID: u64 = 1;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct NodeSpec {
    node_id: u64,
    port: u16,
    data_dir: Option<PathBuf>,
}

impl NodeSpec {
    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

struct NodeProcess {
    child: Child,
    logs: Arc<Mutex<String>>,
}

impl NodeProcess {
    fn spawn(spec: &NodeSpec, peers: &str, endpoint: &str, cluster_name: &str) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_kv-store"));
        command
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .arg("--node-id")
            .arg(spec.node_id.to_string())
            .arg("--port")
            .arg(spec.port.to_string())
            .arg("--peers")
            .arg(peers)
            .arg("--formation")
            .arg("genesis")
            .arg("--cluster-name")
            .arg(cluster_name)
            .arg("--etcd-endpoints")
            .arg(endpoint);
        if let Some(data_dir) = &spec.data_dir {
            command.arg("--data-dir").arg(data_dir);
        }
        let mut child = command.spawn().expect("kv-store process must spawn");
        let logs = Arc::new(Mutex::new(String::new()));
        capture_output(child.stdout.take().unwrap(), logs.clone());
        capture_output(child.stderr.take().unwrap(), logs.clone());
        Self { child, logs }
    }

    fn spawn_legacy(spec: &NodeSpec, peers: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kv-store"))
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .arg("--node-id")
            .arg(spec.node_id.to_string())
            .arg("--port")
            .arg(spec.port.to_string())
            .arg("--peers")
            .arg(peers)
            .arg("--bootstrap")
            .spawn()
            .expect("legacy kv-store process must spawn");
        let logs = Arc::new(Mutex::new(String::new()));
        capture_output(child.stdout.take().unwrap(), logs.clone());
        capture_output(child.stderr.take().unwrap(), logs.clone());
        Self { child, logs }
    }

    async fn stop(&mut self) {
        self.child
            .kill()
            .await
            .expect("kv-store child must be killed");
        let _ = self.child.wait().await;
    }

    fn logs(&self) -> String {
        self.logs.lock().unwrap().clone()
    }
}

fn capture_output(
    reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    logs: Arc<Mutex<String>>,
) {
    tokio::spawn(async move {
        let mut reader = reader;
        let mut buffer = [0_u8; 4096];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => logs
                    .lock()
                    .unwrap()
                    .push_str(&String::from_utf8_lossy(&buffer[..read])),
            }
        }
    });
}

fn contains_genesis_initialization(logs: &str) -> bool {
    logs.contains("initialized raft cluster") || logs.contains("raft initialize")
}

fn reserve_port() -> u16 {
    loop {
        let client = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = client.local_addr().unwrap().port();
        if port > 64_535 {
            continue;
        }
        let admin = std::net::TcpListener::bind(("127.0.0.1", port + 1000));
        let admin_available = admin.is_ok();
        drop(admin);
        if admin_available {
            drop(client);
            return port;
        }
    }
}

fn specs(data_root: Option<&Path>) -> Vec<NodeSpec> {
    (1..=3_u64)
        .map(|node_id| NodeSpec {
            node_id,
            port: reserve_port(),
            data_dir: data_root.map(|root| root.join(format!("node-{node_id}"))),
        })
        .collect()
}

fn peers(specs: &[NodeSpec]) -> String {
    specs
        .iter()
        .map(|spec| format!("{}@{}", spec.node_id, spec.addr()))
        .collect::<Vec<_>>()
        .join(",")
}

fn spawn_all(
    specs: &[NodeSpec],
    peers: &str,
    endpoint: &str,
    cluster_name: &str,
) -> Vec<NodeProcess> {
    specs
        .iter()
        .map(|spec| NodeProcess::spawn(spec, peers, endpoint, cluster_name))
        .collect()
}

async fn client(specs: &[NodeSpec]) -> RrClient {
    RrClientBuilder::new(specs.iter().map(|spec| spec.addr()).collect())
        .with_retry_config(RetryConfig {
            max_retries: 3,
            retry_delay_ms: 100,
        })
        .with_request_timeout(Duration::from_secs(2))
        .build()
        .await
        .unwrap()
}

fn encode(request: &KvRequest) -> Vec<u8> {
    bincode::serde::encode_to_vec(request, bincode::config::standard()).unwrap()
}

fn decode(bytes: &[u8]) -> KvResponse {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .unwrap()
        .0
}

async fn get_value(client: &RrClient, key: &str) -> Result<Option<Vec<u8>>, String> {
    let response = client
        .send_request(
            GROUP_ID,
            encode(&KvRequest::Get {
                key: key.to_string(),
            }),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(decode(&response).value)
}

async fn put_and_get(client: &RrClient, key: &str, value: &[u8]) -> Result<(), String> {
    client
        .send_request(
            GROUP_ID,
            encode(&KvRequest::Put {
                key: key.to_string(),
                value: value.to_vec(),
            }),
        )
        .await
        .map_err(|error| error.to_string())?;
    let actual = get_value(client, key).await?;
    if actual.as_deref() == Some(value) {
        Ok(())
    } else {
        Err(format!("unexpected value for {key}: {actual:?}"))
    }
}

async fn wait_for_kv(client: &RrClient, key: &str, value: &[u8], processes: &[NodeProcess]) {
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        match put_and_get(client, key, value).await {
            Ok(()) => return,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "cluster did not become ready for KV traffic: {error}; logs: {:?}",
                    processes.iter().map(NodeProcess::logs).collect::<Vec<_>>()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(feature = "rocks")]
async fn wait_for_existing_value(
    client: &RrClient,
    key: &str,
    expected: &[u8],
    processes: &[NodeProcess],
) {
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        match get_value(client, key).await {
            Ok(actual) if actual.as_deref() == Some(expected) => return,
            result => {
                assert!(
                    Instant::now() < deadline,
                    "persisted value was not recovered for {key}: {result:?}; logs: {:?}",
                    processes.iter().map(NodeProcess::logs).collect::<Vec<_>>()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn voters(spec: &NodeSpec) -> Result<BTreeSet<u64>, String> {
    let mut client = AdminServiceClient::connect(format!("http://127.0.0.1:{}", spec.port + 1000))
        .await
        .map_err(|error| error.to_string())?;
    let membership = client
        .get_group_membership(GroupId { group_id: GROUP_ID })
        .await
        .map_err(|error| error.to_string())?
        .into_inner();
    Ok(membership
        .voters
        .into_iter()
        .map(|member| member.node_id)
        .collect())
}

async fn wait_for_complete_voters(specs: &[NodeSpec]) {
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    let expected = BTreeSet::from([1, 2, 3]);
    loop {
        let mut complete = true;
        for spec in specs {
            match voters(spec).await {
                Ok(actual) if actual == expected => {}
                _ => {
                    complete = false;
                    break;
                }
            }
        }
        if complete {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "complete voter membership timed out"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn lifecycle(endpoint: &str, cluster_name: &str) -> rr_core::VersionedLifecycle {
    EtcdLifecycleStore::new(vec![endpoint.to_string()], cluster_name.to_string())
        .await
        .unwrap()
        .load_linearizable()
        .await
        .unwrap()
        .unwrap()
}

async fn start_etcd() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("quay.io/coreos/etcd", "v3.5.13")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_cmd(vec![
            "/usr/local/bin/etcd",
            "--name=node1",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://0.0.0.0:2379",
            "--listen-peer-urls=http://0.0.0.0:2380",
            "--initial-advertise-peer-urls=http://0.0.0.0:2380",
            "--initial-cluster=node1=http://0.0.0.0:2380",
            "--initial-cluster-state=new",
        ])
        .start()
        .await
        .expect("local etcd image must start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(2379.tcp()).await.unwrap();
    (container, format!("http://{host}:{port}"))
}

fn live_etcd_enabled() -> bool {
    matches!(std::env::var("RR_TEST_ETCD").as_deref(), Ok("1"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_manual_bootstrap_remains_available_without_etcd() {
    let spec = NodeSpec {
        node_id: 1,
        port: reserve_port(),
        data_dir: None,
    };
    let peers = format!("1@{}", spec.addr());
    let mut process = NodeProcess::spawn_legacy(&spec, &peers);
    let client = client(std::slice::from_ref(&spec)).await;
    wait_for_kv(
        &client,
        "legacy-bootstrap",
        b"ack",
        std::slice::from_ref(&process),
    )
    .await;
    assert!(process.logs().contains("Cluster bootstrapped"));
    process.stop().await;
}

#[test]
fn initialization_log_detection_covers_managed_and_legacy_wording() {
    assert!(contains_genesis_initialization(
        "initialized raft cluster group_id=1"
    ));
    assert!(contains_genesis_initialization(
        "raft initialize failed after retries"
    ));
    assert!(!contains_genesis_initialization(
        "Raft group 1 already initialized"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_flags_reach_lifecycle_store_without_bootstrap() {
    let port = reserve_port();
    let addr = format!("127.0.0.1:{port}");
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        Command::new(env!("CARGO_BIN_EXE_kv-store"))
            .kill_on_drop(true)
            .arg("--node-id")
            .arg("1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--peers")
            .arg(format!("1@{addr}"))
            .arg("--formation")
            .arg("genesis")
            .arg("--cluster-name")
            .arg("cli-smoke")
            .arg("--etcd-endpoints")
            .arg("http://127.0.0.1:1")
            .output(),
    )
    .await
    .expect("managed CLI smoke must not hang")
    .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "an unavailable lifecycle store must fail startup"
    );
    assert!(
        !stderr.contains("unknown argument"),
        "formation flags must be accepted: {stderr}"
    );
    assert!(
        stderr.contains("lifecycle"),
        "startup must reach the lifecycle store: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "requires Docker, the local etcd v3.5.13 image, and RR_TEST_ETCD=1"]
async fn three_process_genesis_forms_one_cluster_with_complete_voters_and_kv() {
    if !live_etcd_enabled() {
        eprintln!("RR_TEST_ETCD != 1; real-Etcd process coverage was not executed");
        return;
    }
    let (_etcd, endpoint) = start_etcd().await;
    let specs = specs(None);
    let peers = peers(&specs);
    let cluster_name = "kv-process-genesis";
    let mut processes = spawn_all(&specs, &peers, &endpoint, cluster_name);
    let client = client(&specs).await;

    wait_for_kv(&client, "process-genesis", b"ack", &processes).await;
    wait_for_complete_voters(&specs).await;
    let record = lifecycle(&endpoint, cluster_name).await;
    assert_eq!(
        record.record.groups[&GROUP_ID].state,
        FormationState::Active
    );
    assert_eq!(record.record.origin, ClusterOrigin::Genesis);
    let expected_nodes = specs
        .iter()
        .map(|spec| (spec.node_id, NodeAddr { addr: spec.addr() }))
        .collect();
    let expected_seed = SeedSpec::stock(cluster_name, expected_nodes, 1).unwrap();
    assert_eq!(record.record.seed_hash, expected_seed.seed_hash().unwrap());

    for process in &mut processes {
        process.stop().await;
    }
}

#[cfg(feature = "rocks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "requires Docker, the local etcd v3.5.13 image, and RR_TEST_ETCD=1"]
async fn rocks_process_restart_recovers_without_genesis_reinitialization() {
    if !live_etcd_enabled() {
        eprintln!("RR_TEST_ETCD != 1; real-Etcd Rocks process coverage was not executed");
        return;
    }
    let (_etcd, endpoint) = start_etcd().await;
    let data_root = tempfile::TempDir::new().unwrap();
    let specs = specs(Some(data_root.path()));
    let peers = peers(&specs);
    let cluster_name = "kv-process-rocks";
    let mut processes = spawn_all(&specs, &peers, &endpoint, cluster_name);
    let client = client(&specs).await;
    wait_for_kv(&client, "rocks-before-restart", b"durable-ack", &processes).await;
    wait_for_complete_voters(&specs).await;
    let before = lifecycle(&endpoint, cluster_name).await;

    for index in 0..specs.len() {
        processes[index].stop().await;
        processes[index] = NodeProcess::spawn(&specs[index], &peers, &endpoint, cluster_name);
        wait_for_kv(
            &client,
            &format!("rocks-after-{index}"),
            b"durable-ack",
            &processes,
        )
        .await;
        wait_for_complete_voters(&specs).await;
        let logs = processes[index].logs();
        assert!(
            !logs.contains("Cluster bootstrapped"),
            "manual bootstrap ran in managed mode: {logs}"
        );
        assert!(
            !contains_genesis_initialization(&logs),
            "Genesis initialization ran during managed recovery: {logs}"
        );
        assert!(
            !logs.contains("Raft group 1 already initialized"),
            "Active seed was replayed: {logs}"
        );
    }

    wait_for_existing_value(&client, "rocks-before-restart", b"durable-ack", &processes).await;
    let after = lifecycle(&endpoint, cluster_name).await;
    assert_eq!(after.record.cluster_uuid, before.record.cluster_uuid);
    assert_eq!(after.revision, before.revision);
    for process in &mut processes {
        process.stop().await;
    }
}
