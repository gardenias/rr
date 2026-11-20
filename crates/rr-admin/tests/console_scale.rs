use axum::extract::State;
use axum::routing::get;
use axum::Router;
use rr_network::proto::admin::admin_service_server::{AdminService, AdminServiceServer};
use rr_network::proto::admin::{
    AddLearnerRequest, BootRecord, BootRecordPage, BootRecordsRequest, BootStatus, BuildInfo,
    ChangeMembershipRequest, ClusterTopology, ClusterTopologyRequest, DrainRequest, DrainResult,
    Empty, FormationStatus, GroupId, GroupList, GroupMembership, GroupReplicaStatus, GroupStatus,
    GroupSummary, LeaderReplicaStatus, MetricsSummary, NodeDescriptor, NodeDescriptorResponse,
    NodeDirectoryEntry, NodeStatus, OnlineBackupChunk, OnlineBackupRequest, PartitionRoute,
    ProtocolVersion, RemoveGroupMemberRequest, ResetIngestorOffsetRequest, RoutingTableSnapshot,
    SnapshotStatus, TransferLeaderRequest,
};
use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response as TonicResponse, Status};

const NODE_COUNT: u64 = 50;
const GROUP_COUNT: u64 = 500;
const MAX_SCRAPE_BYTES: usize = 8 * 1024 * 1024;
const MAX_INPUT_SERIES: usize = 100_000;
const MAX_ACTIVE_SERIES: usize = 20_000;
const MAX_POINTS: usize = 2_000_000;
const MAX_BFF_RSS_BYTES: u64 = 512 * 1024 * 1024;
const INPUT_SERIES_PER_NODE: usize = GROUP_COUNT as usize * 9 + 6;
const NORMALIZED_SAMPLES_PER_NODE: usize = GROUP_COUNT as usize * 3 + 6;
const RATE_SERIES_PER_NODE: usize = GROUP_COUNT as usize + 2;

#[derive(Default)]
struct ProcessCounters {
    admin_rpcs: AtomicU64,
    metric_scrapes: AtomicU64,
    boot_history_rpcs: AtomicU64,
}

struct SimulatedUpstream {
    node_id: u64,
    admin_addr: SocketAddr,
    observability_addr: SocketAddr,
    groups: Vec<GroupSummary>,
    group_ids: Vec<u64>,
    counters: Arc<ProcessCounters>,
    history_unresponsive: Arc<AtomicBool>,
}

impl SimulatedUpstream {
    fn new(
        node_id: u64,
        admin_addr: SocketAddr,
        observability_addr: SocketAddr,
        counters: Arc<ProcessCounters>,
        history_unresponsive: Arc<AtomicBool>,
    ) -> Self {
        let groups = (1..=GROUP_COUNT)
            .map(|group_id| GroupSummary {
                group_id,
                is_leader: node_id == 1,
                current_term: 7,
                last_log_index: 10_000 + group_id,
                leader_node_id: Some(1),
            })
            .collect();
        Self {
            node_id,
            admin_addr,
            observability_addr,
            groups,
            group_ids: (1..=GROUP_COUNT).collect(),
            counters,
            history_unresponsive,
        }
    }

    fn boot_record(&self) -> BootRecord {
        BootRecord {
            boot_id: format!("scale-boot-{}", self.node_id),
            started_at_unix_ms: 1,
            ended_at_unix_ms: None,
            shutdown_state: "RUNNING".to_string(),
            build_info: Some(BuildInfo {
                boot_id: format!("scale-boot-{}", self.node_id),
                ..BuildInfo::default()
            }),
        }
    }

    fn descriptor(&self) -> NodeDescriptor {
        NodeDescriptor {
            node_id: self.node_id,
            client_addr: format!("127.0.0.1:{}", 40_000 + self.node_id),
            admin_addr: self.admin_addr.to_string(),
            observability_addr: self.observability_addr.to_string(),
            advertise_addr: format!("127.0.0.1:{}", 41_000 + self.node_id),
            environment: "scale-test".to_string(),
            region: None,
            zone: None,
            rack: None,
            boot_id: format!("scale-boot-{}", self.node_id),
        }
    }

    fn count_admin(&self) {
        self.counters.admin_rpcs.fetch_add(1, Ordering::Relaxed);
    }
}

#[tonic::async_trait]
impl AdminService for SimulatedUpstream {
    async fn get_node_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<TonicResponse<NodeStatus>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(NodeStatus {
            node_id: self.node_id,
            listen_addr: format!("127.0.0.1:{}", 40_000 + self.node_id),
            group_ids: self.group_ids.clone(),
            uptime_secs: 600,
        }))
    }

    async fn get_group_status(
        &self,
        request: Request<GroupId>,
    ) -> Result<TonicResponse<GroupStatus>, Status> {
        self.count_admin();
        let group_id = request.into_inner().group_id;
        Ok(TonicResponse::new(GroupStatus {
            group_id,
            is_leader: self.node_id == 1,
            current_term: 7,
            last_log_index: 10_000 + group_id,
            leader_node_id: 1,
        }))
    }

    async fn list_groups(
        &self,
        _request: Request<Empty>,
    ) -> Result<TonicResponse<GroupList>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(GroupList {
            groups: self.groups.clone(),
        }))
    }

    async fn get_snapshot_status(
        &self,
        request: Request<GroupId>,
    ) -> Result<TonicResponse<SnapshotStatus>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(SnapshotStatus {
            group_id: request.into_inner().group_id,
            snapshot_in_progress: false,
            last_snapshot_index: Some(9_000),
            last_snapshot_term: Some(6),
        }))
    }

    async fn trigger_snapshot(
        &self,
        _request: Request<GroupId>,
    ) -> Result<TonicResponse<Empty>, Status> {
        Err(Status::unimplemented("read-only scale upstream"))
    }

    async fn transfer_leader(
        &self,
        _request: Request<TransferLeaderRequest>,
    ) -> Result<TonicResponse<Empty>, Status> {
        Err(Status::unimplemented("read-only scale upstream"))
    }

    async fn drain_node(
        &self,
        _request: Request<DrainRequest>,
    ) -> Result<TonicResponse<DrainResult>, Status> {
        Err(Status::unimplemented("read-only scale upstream"))
    }

    async fn add_learner(
        &self,
        _request: Request<AddLearnerRequest>,
    ) -> Result<TonicResponse<Empty>, Status> {
        Err(Status::unimplemented("read-only scale upstream"))
    }

    async fn change_membership(
        &self,
        _request: Request<ChangeMembershipRequest>,
    ) -> Result<TonicResponse<Empty>, Status> {
        Err(Status::unimplemented("read-only scale upstream"))
    }

    async fn remove_group_member(
        &self,
        _request: Request<RemoveGroupMemberRequest>,
    ) -> Result<TonicResponse<Empty>, Status> {
        Err(Status::unimplemented("read-only scale upstream"))
    }

    async fn reset_ingestor_offset(
        &self,
        _request: Request<ResetIngestorOffsetRequest>,
    ) -> Result<TonicResponse<Empty>, Status> {
        Err(Status::unimplemented("read-only scale upstream"))
    }

    async fn get_group_membership(
        &self,
        request: Request<GroupId>,
    ) -> Result<TonicResponse<GroupMembership>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(GroupMembership {
            group_id: request.into_inner().group_id,
            voters: Vec::new(),
            learners: Vec::new(),
        }))
    }

    type OnlineBackupStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<OnlineBackupChunk, Status>> + Send>>;

    async fn online_backup(
        &self,
        _request: Request<OnlineBackupRequest>,
    ) -> Result<TonicResponse<Self::OnlineBackupStream>, Status> {
        Err(Status::unimplemented("read-only scale upstream"))
    }

    async fn get_routing_table(
        &self,
        _request: Request<Empty>,
    ) -> Result<TonicResponse<RoutingTableSnapshot>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(RoutingTableSnapshot {
            partitions: vec![PartitionRoute {
                partition_id: 0,
                group_id: 1,
                replica_node_ids: vec![1],
            }],
        }))
    }

    async fn get_metrics_summary(
        &self,
        _request: Request<Empty>,
    ) -> Result<TonicResponse<MetricsSummary>, Status> {
        Err(Status::unimplemented("deprecated"))
    }

    async fn get_formation_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<TonicResponse<FormationStatus>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(FormationStatus {
            origin: "Untracked".to_string(),
            cluster_uuid: String::new(),
            topology_revision: 0,
            store_revision: 0,
            state: "Untracked".to_string(),
            groups: Vec::new(),
        }))
    }

    async fn get_build_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<TonicResponse<BuildInfo>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(BuildInfo {
            rr_version: "scale".to_string(),
            app_version: "scale".to_string(),
            git_commit: "scale".to_string(),
            git_dirty: false,
            build_profile: "test".to_string(),
            admin_api: Some(ProtocolVersion {
                major: 1,
                minor: 0,
                min_peer_minor: 0,
            }),
            raft_wire: Some(ProtocolVersion {
                major: 1,
                minor: 0,
                min_peer_minor: 0,
            }),
            boot_id: format!("scale-boot-{}", self.node_id),
            process_start_unix_ms: 1,
            config_fingerprint: "scale".to_string(),
        }))
    }

    async fn get_node_descriptor(
        &self,
        _request: Request<Empty>,
    ) -> Result<TonicResponse<NodeDescriptorResponse>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(NodeDescriptorResponse {
            node_descriptor: Some(self.descriptor()),
            directory_revision: 1,
        }))
    }

    async fn get_cluster_topology(
        &self,
        _request: Request<ClusterTopologyRequest>,
    ) -> Result<TonicResponse<ClusterTopology>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(ClusterTopology {
            snapshot_id: "scale-topology".to_string(),
            config_revision: 1,
            directory_revision: 1,
            nodes: vec![NodeDirectoryEntry {
                node_descriptor: Some(self.descriptor()),
                revision: 1,
                lease_id: 1,
            }],
            groups: Vec::new(),
            partitions: Vec::new(),
            next_cursor: String::new(),
        }))
    }

    async fn get_group_replica_status(
        &self,
        request: Request<GroupId>,
    ) -> Result<TonicResponse<GroupReplicaStatus>, Status> {
        self.count_admin();
        let group_id = request.into_inner().group_id;
        let observed_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let leader_replication = if self.node_id == 1 {
            (2..=NODE_COUNT)
                .map(|node_id| LeaderReplicaStatus {
                    node_id,
                    matched_index: Some(9_999),
                    lag_entries: Some(1),
                    last_heartbeat_unix_ms: Some(observed_at_unix_ms),
                })
                .collect()
        } else {
            Vec::new()
        };
        Ok(TonicResponse::new(GroupReplicaStatus {
            group_id,
            node_id: self.node_id,
            server_state: if self.node_id == 1 {
                "Leader".to_string()
            } else {
                "Follower".to_string()
            },
            leader_node_id: Some(1),
            current_term: 7,
            last_log_index: Some(10_000 + group_id),
            local_commit_index: Some(10_000 + group_id),
            cluster_commit_index: Some(10_000 + group_id),
            applied_index: Some(10_000 + group_id),
            last_snapshot_index: None,
            last_snapshot_term: None,
            leader_replication,
            observed_at_unix_ms,
        }))
    }

    async fn get_boot_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<TonicResponse<BootStatus>, Status> {
        self.count_admin();
        Ok(TonicResponse::new(BootStatus {
            current: Some(self.boot_record()),
            persistence: "VOLATILE".to_string(),
        }))
    }

    async fn list_boot_records(
        &self,
        _request: Request<BootRecordsRequest>,
    ) -> Result<TonicResponse<BootRecordPage>, Status> {
        self.count_admin();
        self.counters
            .boot_history_rpcs
            .fetch_add(1, Ordering::Relaxed);
        if self.history_unresponsive.load(Ordering::Relaxed) {
            std::future::pending::<()>().await;
        }
        Ok(TonicResponse::new(BootRecordPage {
            records: vec![self.boot_record()],
            next_cursor: String::new(),
            persistence: "VOLATILE".to_string(),
        }))
    }
}

fn representative_metrics(node_id: u64) -> String {
    let mut body = String::with_capacity(600_000);
    body.push_str("# TYPE rr_server_requests_total counter\n");
    body.push_str("# TYPE rr_raft_is_leader gauge\n");
    body.push_str("# TYPE rr_raft_replication_lag gauge\n");
    body.push_str("# TYPE rr_storage_live_bytes gauge\n");
    body.push_str("# TYPE rr_server_request_duration_seconds histogram\n");
    for group_id in 1..=GROUP_COUNT {
        body.push_str(&format!(
            "rr_server_requests_total{{group_id=\"{group_id}\",op=\"write\",outcome=\"success\"}} 1000\n"
        ));
        body.push_str(&format!(
            "rr_raft_is_leader{{group_id=\"{group_id}\"}} {}\n",
            u64::from(node_id == 1)
        ));
        body.push_str(&format!(
            "rr_raft_replication_lag{{group_id=\"{group_id}\",follower_node_id=\"2\"}} 3\n"
        ));
        body.push_str(&format!(
            "rr_storage_live_bytes{{group_id=\"{group_id}\"}} 1048576\n"
        ));
        for (bound, count) in [("0.1", 40), ("0.5", 80), ("+Inf", 100)] {
            body.push_str(&format!(
                "rr_server_request_duration_seconds_bucket{{group_id=\"{group_id}\",op=\"write\",le=\"{bound}\"}} {count}\n"
            ));
        }
        body.push_str(&format!(
            "rr_server_request_duration_seconds_sum{{group_id=\"{group_id}\",op=\"write\"}} 25\n"
        ));
        body.push_str(&format!(
            "rr_server_request_duration_seconds_count{{group_id=\"{group_id}\",op=\"write\"}} 100\n"
        ));
    }
    body.push_str("# TYPE rr_node_cpu_usage_ratio gauge\nrr_node_cpu_usage_ratio 0.25\n");
    body.push_str("# TYPE rr_node_cpu_cores gauge\nrr_node_cpu_cores 8\n");
    body.push_str("# TYPE rr_node_memory_bytes gauge\nrr_node_memory_bytes{scope=\"process\",kind=\"used\"} 1048576\n");
    body.push_str("# TYPE rr_node_network_bytes_total counter\nrr_node_network_bytes_total{direction=\"receive\"} 2048\n");
    body.push_str("# TYPE rr_node_filesystem_bytes gauge\nrr_node_filesystem_bytes{kind=\"available\"} 1073741824\n");
    body.push_str("# TYPE rr_snapshot_installs_total counter\nrr_snapshot_installs_total 2\n");
    body
}

fn too_many_series() -> String {
    (0..=MAX_INPUT_SERIES)
        .map(|index| format!("scale_series_{index} 1\n"))
        .collect()
}

#[test]
fn simulated_upstream_process() {
    if std::env::var_os("RR_CONSOLE_SCALE_UPSTREAM").is_none() {
        return;
    }
    let node_id = std::env::var("RR_CONSOLE_SCALE_NODE_ID")
        .expect("scale child node ID")
        .parse()
        .expect("numeric scale child node ID");
    let admin_addr = std::env::var("RR_CONSOLE_SCALE_ADMIN_ADDR")
        .expect("scale child Admin address")
        .parse()
        .expect("valid scale child Admin address");
    let observability_addr = std::env::var("RR_CONSOLE_SCALE_OBSERVABILITY_ADDR")
        .expect("scale child observability address")
        .parse()
        .expect("valid scale child observability address");
    let mode = std::env::var("RR_CONSOLE_SCALE_METRICS_MODE").expect("scale child metrics mode");
    let metrics_body = match mode.as_str() {
        "representative" => representative_metrics(node_id),
        "oversize-body" => " ".repeat(MAX_SCRAPE_BYTES + 1),
        "too-many-series" => too_many_series(),
        other => panic!("unknown scale metrics mode {other}"),
    };
    let counters = Arc::new(ProcessCounters::default());
    let history_unresponsive = Arc::new(AtomicBool::new(false));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("scale child Tokio runtime");
    runtime.block_on(async move {
        let service = SimulatedUpstream::new(
            node_id,
            admin_addr,
            observability_addr,
            counters.clone(),
            history_unresponsive.clone(),
        );
        let admin = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(AdminServiceServer::new(service))
                .serve(admin_addr),
        );
        let metrics_listener = tokio::net::TcpListener::bind(observability_addr)
            .await
            .expect("bind scale child metrics listener");
        let app = Router::new()
            .route(
                "/metrics",
                get(|State(state): State<Arc<MetricHttpState>>| async move {
                    state.counters.metric_scrapes.fetch_add(1, Ordering::Relaxed);
                    state.body.clone()
                }),
            )
            .route(
                "/counters",
                get(|State(state): State<Arc<MetricHttpState>>| async move {
                    format!(
                        "{{\"admin_rpcs\":{},\"metric_scrapes\":{},\"boot_history_rpcs\":{}}}",
                        state.counters.admin_rpcs.load(Ordering::Relaxed),
                        state.counters.metric_scrapes.load(Ordering::Relaxed),
                        state.counters.boot_history_rpcs.load(Ordering::Relaxed)
                    )
                }),
            )
            .route(
                "/control/unresponsive-history",
                get(|State(state): State<Arc<MetricHttpState>>| async move {
                    state.history_unresponsive.store(true, Ordering::Relaxed);
                    "enabled"
                }),
            )
            .with_state(Arc::new(MetricHttpState {
                body: metrics_body,
                counters,
                history_unresponsive,
            }));
        let metrics = tokio::spawn(async move {
            axum::serve(metrics_listener, app)
                .await
                .expect("serve scale child metrics");
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("READY");
        std::io::stdout().flush().expect("flush scale child readiness");
        tokio::select! {
            result = admin => result.expect("join scale child AdminService").expect("serve scale child AdminService"),
            result = metrics => result.expect("join scale child metrics server"),
        }
    });
}

struct MetricHttpState {
    body: String,
    counters: Arc<ProcessCounters>,
    history_unresponsive: Arc<AtomicBool>,
}

struct ChildProcess {
    child: Child,
}

impl ChildProcess {
    fn terminate(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        self.terminate();
    }
}

struct UpstreamProcess {
    node_id: u64,
    admin_addr: SocketAddr,
    observability_addr: SocketAddr,
    process: ChildProcess,
}

impl UpstreamProcess {
    fn spawn(node_id: u64, metrics_mode: &str) -> Self {
        let admin_addr = unused_addr();
        let observability_addr = unused_addr();
        let mut command = Command::new(std::env::current_exe().expect("scale test executable"));
        command
            .args([
                "--exact",
                "simulated_upstream_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("RR_CONSOLE_SCALE_UPSTREAM", "1")
            .env("RR_CONSOLE_SCALE_NODE_ID", node_id.to_string())
            .env("RR_CONSOLE_SCALE_ADMIN_ADDR", admin_addr.to_string())
            .env(
                "RR_CONSOLE_SCALE_OBSERVABILITY_ADDR",
                observability_addr.to_string(),
            )
            .env("RR_CONSOLE_SCALE_METRICS_MODE", metrics_mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command.spawn().expect("spawn simulated upstream process");
        let stdout = child.stdout.take().expect("scale child stdout");
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let ready = loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break false,
                    Ok(_) if line.trim().ends_with("READY") => break true,
                    Ok(_) => {}
                }
            };
            let _ = ready_tx.send(ready);
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink);
        });
        if !ready_rx
            .recv_timeout(Duration::from_secs(15))
            .unwrap_or(false)
        {
            let _ = child.kill();
            let _ = child.wait();
            panic!("simulated upstream {node_id} did not become ready");
        }
        Self {
            node_id,
            admin_addr,
            observability_addr,
            process: ChildProcess { child },
        }
    }

    fn counters(&self) -> (u64, u64, u64) {
        let response = http_request(self.observability_addr, "/counters", Duration::from_secs(5));
        assert_eq!(response.status, 200, "{}", response.body);
        let json = response.json();
        (
            json["admin_rpcs"].as_u64().expect("Admin RPC counter"),
            json["metric_scrapes"]
                .as_u64()
                .expect("metrics scrape counter"),
            json["boot_history_rpcs"]
                .as_u64()
                .expect("boot history RPC counter"),
        )
    }

    fn make_history_unresponsive(&self) {
        let response = http_request(
            self.observability_addr,
            "/control/unresponsive-history",
            Duration::from_secs(5),
        );
        assert_eq!(response.status, 200, "{}", response.body);
    }
}

struct ConsoleProcess {
    addr: SocketAddr,
    process: ChildProcess,
    _state_dir: tempfile::TempDir,
}

impl ConsoleProcess {
    fn spawn(upstreams: &[UpstreamProcess], poll_interval_secs: u64) -> Self {
        let addr = unused_addr();
        let state_dir = tempfile::tempdir().expect("temporary scale Console state directory");
        let peers = upstreams
            .iter()
            .map(|upstream| format!("{}@{}", upstream.node_id, upstream.admin_addr))
            .collect::<Vec<_>>()
            .join(",");
        let child = Command::new(env!("CARGO_BIN_EXE_rr-admin"))
            .args([
                "console",
                "--listen",
                &addr.to_string(),
                "--peers",
                &peers,
                "--poll-interval-secs",
                &poll_interval_secs.to_string(),
                "--state-dir",
                &state_dir.path().to_string_lossy(),
            ])
            .env_remove("RR_CONSOLE_TOKEN")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn Console BFF");
        let mut process = ChildProcess { child };
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = process.child.try_wait().expect("inspect Console BFF") {
                panic!("Console BFF exited before listening: {status}");
            }
            if TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "Console BFF listen timeout");
            thread::sleep(Duration::from_millis(25));
        }
        Self {
            addr,
            process,
            _state_dir: state_dir,
        }
    }

    fn get(&self, path: &str) -> HttpResponse {
        http_request(self.addr, path, Duration::from_secs(120))
    }
}

struct HttpResponse {
    status: u16,
    body: String,
}

impl HttpResponse {
    fn parse(bytes: Vec<u8>) -> Self {
        let split = bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("HTTP header terminator");
        let head = String::from_utf8(bytes[..split].to_vec()).expect("UTF-8 HTTP headers");
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|status| status.parse().ok())
            .expect("HTTP status");
        let body = String::from_utf8(bytes[split + 4..].to_vec()).expect("UTF-8 HTTP body");
        Self { status, body }
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|error| panic!("invalid JSON HTTP {} response: {error}", self.status))
    }
}

fn http_request(addr: SocketAddr, path: &str, timeout: Duration) -> HttpResponse {
    let mut stream = TcpStream::connect_timeout(&addr, timeout).expect("connect HTTP endpoint");
    stream
        .set_read_timeout(Some(timeout))
        .expect("set HTTP read timeout");
    stream
        .set_write_timeout(Some(timeout))
        .expect("set HTTP write timeout");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .expect("write HTTP request");
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).expect("read HTTP response");
    HttpResponse::parse(bytes)
}

fn unused_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read ephemeral address")
}

fn resident_bytes(pid: u32) -> u64 {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("inspect BFF RSS with ps");
    assert!(output.status.success(), "ps failed for BFF pid {pid}");
    String::from_utf8(output.stdout)
        .expect("UTF-8 ps output")
        .trim()
        .parse::<u64>()
        .expect("numeric RSS KiB")
        * 1024
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    task: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start(pid: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(resident_bytes(pid)));
        let task = {
            let stop = stop.clone();
            let peak = peak.clone();
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    peak.fetch_max(resident_bytes(pid), Ordering::Relaxed);
                    thread::sleep(Duration::from_millis(100));
                }
            })
        };
        Self {
            stop,
            peak,
            task: Some(task),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.task.take().unwrap().join().expect("join RSS sampler");
        self.peak.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct BudgetReport {
    failures: Vec<String>,
}

impl BudgetReport {
    fn check(&mut self, condition: bool, detail: impl Into<String>) {
        if !condition {
            self.failures.push(detail.into());
        }
    }

    fn finish(self) {
        if !self.failures.is_empty() {
            panic!(
                "Task 9B scale budget failures ({}):\n{}",
                self.failures.len(),
                self.failures.join("\n")
            );
        }
    }
}

fn percentile_95(mut durations: Vec<Duration>) -> Duration {
    durations.sort();
    durations[(durations.len() * 95).div_ceil(100) - 1]
}

fn measure_cached_apis(console: &ConsoleProcess) -> (Duration, Duration, usize, usize) {
    let mut summary = Vec::new();
    let mut groups = Vec::new();
    let mut summary_body_bytes = 0;
    let mut groups_body_bytes = 0;
    for _ in 0..40 {
        let started = Instant::now();
        let response = console.get("/api/v1/console/summary?window=15m");
        summary.push(started.elapsed());
        assert_eq!(response.status, 200);
        summary_body_bytes = summary_body_bytes.max(response.body.len());

        let started = Instant::now();
        let response = console.get("/api/v1/console/groups");
        groups.push(started.elapsed());
        assert_eq!(response.status, 200);
        groups_body_bytes = groups_body_bytes.max(response.body.len());
    }
    (
        percentile_95(summary),
        percentile_95(groups),
        summary_body_bytes,
        groups_body_bytes,
    )
}

fn wait_for_scale_cache(console: &ConsoleProcess) -> Value {
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let response = console.get("/api/v1/console/sources");
        assert_eq!(response.status, 200);
        last = response.json();
        let sources = last["data"]["sources"].as_array().expect("source list");
        if sources.len() == NODE_COUNT as usize
            && sources
                .iter()
                .all(|source| source["error"].is_null() && source["metrics"]["status"] == "ONLINE")
        {
            return last;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("scale cache did not become fully online: {last}");
}

fn run_sse_subscribers(console: &ConsoleProcess, duration: Duration) -> Duration {
    let stop = Arc::new(AtomicBool::new(false));
    let mut subscribers = Vec::new();
    for subscriber in 0..20 {
        let addr = console.addr;
        let stop = stop.clone();
        subscribers.push(thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).expect("connect SSE subscriber");
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("set SSE header timeout");
            write!(
                stream,
                "GET /api/v1/console/stream HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n"
            )
            .expect("write SSE request");
            let mut bytes = [0; 16 * 1024];
            let mut headers = Vec::new();
            while !headers.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut bytes).expect("read SSE headers");
                assert!(read > 0, "SSE subscriber {subscriber} closed before headers");
                headers.extend_from_slice(&bytes[..read]);
            }
            assert!(
                String::from_utf8_lossy(&headers).starts_with("HTTP/1.1 200"),
                "SSE subscriber {subscriber} failed: {}",
                String::from_utf8_lossy(&headers)
            );
            stream
                .set_read_timeout(Some(Duration::from_millis(500)))
                .expect("set SSE body timeout");
            while !stop.load(Ordering::Relaxed) {
                match stream.read(&mut bytes) {
                    Ok(0) => panic!("SSE subscriber {subscriber} closed unexpectedly"),
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) => {}
                    Err(error) => panic!("SSE subscriber {subscriber} read failed: {error}"),
                }
            }
        }));
    }
    let started = Instant::now();
    thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    for subscriber in subscribers {
        subscriber.join().expect("join SSE subscriber");
    }
    started.elapsed()
}

fn wait_for_next_scrape(upstream: &UpstreamProcess) {
    let initial = upstream.counters().1;
    let deadline = Instant::now() + Duration::from_secs(15);
    while upstream.counters().1 == initial {
        assert!(Instant::now() < deadline, "next metrics scrape timeout");
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_cap_errors(console: &ConsoleProcess) -> (String, String) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut body_code = String::new();
    let mut series_code = String::new();
    while Instant::now() < deadline {
        let response = console.get("/api/v1/console/sources");
        let json = response.json();
        for source in json["data"]["sources"].as_array().expect("source list") {
            let code = source["metrics"]["error"]["code"]
                .as_str()
                .unwrap_or_default();
            match source["node_id"].as_u64() {
                Some(1) => body_code = code.to_string(),
                Some(2) => series_code = code.to_string(),
                _ => {}
            }
        }
        if !body_code.is_empty() && !series_code.is_empty() {
            return (body_code, series_code);
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("metrics caps were not enforced before timeout: body={body_code}, series={series_code}");
}

#[derive(Debug)]
struct TimedResponse {
    status: u16,
    error: String,
    items: usize,
    latency: Duration,
}

fn timed_get(addr: SocketAddr, path: &str) -> TimedResponse {
    let started = Instant::now();
    let response = http_request(addr, path, Duration::from_secs(10));
    let latency = started.elapsed();
    let json = response.json();
    TimedResponse {
        status: response.status,
        error: json["error"]["code"].as_str().unwrap_or("NONE").to_string(),
        items: json["data"]["items"].as_array().map_or(0, Vec::len),
        latency,
    }
}

fn response_p95(responses: &[TimedResponse]) -> Duration {
    percentile_95(responses.iter().map(|response| response.latency).collect())
}

fn response_max(responses: &[TimedResponse]) -> Duration {
    responses
        .iter()
        .map(|response| response.latency)
        .max()
        .expect("viewer responses")
}

struct HistoryWave {
    overview: Vec<TimedResponse>,
    lifecycle: Vec<TimedResponse>,
    latest: Vec<TimedResponse>,
    elapsed: Duration,
    history_calls: Vec<u64>,
    history_qps: Vec<f64>,
    total_admin_qps: Vec<f64>,
}

fn run_history_viewers(console: &ConsoleProcess, upstreams: &[UpstreamProcess]) -> HistoryWave {
    let before = upstreams
        .iter()
        .map(|upstream| upstream.counters())
        .collect::<Vec<_>>();
    let barrier = Arc::new(Barrier::new(20));
    let started = Instant::now();
    let mut viewers = Vec::new();
    for _ in 0..20 {
        let addr = console.addr;
        let barrier = barrier.clone();
        viewers.push(thread::spawn(move || {
            barrier.wait();
            let overview = timed_get(addr, "/api/v1/console/summary?window=15m");
            let lifecycle = timed_get(addr, "/api/v1/console/lifecycle?view=restarts&page_size=1");
            let latest = (1..=NODE_COUNT)
                .map(|node_id| {
                    timed_get(
                        addr,
                        &format!("/api/v1/console/nodes/{node_id}/boots?page_size=1"),
                    )
                })
                .collect::<Vec<_>>();
            (overview, lifecycle, latest)
        }));
    }
    let mut overview = Vec::new();
    let mut lifecycle = Vec::new();
    let mut latest = Vec::new();
    for viewer in viewers {
        let (viewer_overview, viewer_lifecycle, mut viewer_latest) =
            viewer.join().expect("join history viewer");
        overview.push(viewer_overview);
        lifecycle.push(viewer_lifecycle);
        latest.append(&mut viewer_latest);
    }
    if started.elapsed() < Duration::from_secs(2) {
        thread::sleep(Duration::from_secs(2) - started.elapsed());
    }
    let elapsed = started.elapsed();
    let after = upstreams
        .iter()
        .map(|upstream| upstream.counters())
        .collect::<Vec<_>>();
    let history_calls = before
        .iter()
        .zip(&after)
        .map(|(before, after)| after.2 - before.2)
        .collect::<Vec<_>>();
    let history_qps = history_calls
        .iter()
        .map(|calls| *calls as f64 / elapsed.as_secs_f64())
        .collect::<Vec<_>>();
    let total_admin_qps = before
        .iter()
        .zip(&after)
        .map(|(before, after)| (after.0 - before.0) as f64 / elapsed.as_secs_f64())
        .collect::<Vec<_>>();
    HistoryWave {
        overview,
        lifecycle,
        latest,
        elapsed,
        history_calls,
        history_qps,
        total_admin_qps,
    }
}

struct DeadlineWave {
    lifecycle: Vec<TimedResponse>,
    latest: Vec<TimedResponse>,
    elapsed: Duration,
    history_calls: u64,
}

fn run_deadline_viewers(console: &ConsoleProcess, upstreams: &[UpstreamProcess]) -> DeadlineWave {
    upstreams[(NODE_COUNT - 1) as usize].make_history_unresponsive();
    let before = upstreams
        .iter()
        .map(|upstream| upstream.counters().2)
        .sum::<u64>();
    let barrier = Arc::new(Barrier::new(20));
    let started = Instant::now();
    let mut viewers = Vec::new();
    for _ in 0..20 {
        let addr = console.addr;
        let barrier = barrier.clone();
        viewers.push(thread::spawn(move || {
            barrier.wait();
            let lifecycle = timed_get(addr, "/api/v1/console/lifecycle?view=restarts&page_size=3");
            let latest = timed_get(addr, "/api/v1/console/nodes/50/boots?page_size=5");
            (lifecycle, latest)
        }));
    }
    let mut lifecycle = Vec::new();
    let mut latest = Vec::new();
    for viewer in viewers {
        let (viewer_lifecycle, viewer_latest) = viewer.join().expect("join deadline viewer");
        lifecycle.push(viewer_lifecycle);
        latest.push(viewer_latest);
    }
    let elapsed = started.elapsed();
    let after = upstreams
        .iter()
        .map(|upstream| upstream.counters().2)
        .sum::<u64>();
    DeadlineWave {
        lifecycle,
        latest,
        elapsed,
        history_calls: after - before,
    }
}

#[test]
#[ignore = "Task 9B spawns 50 upstream processes and measures a live Console BFF"]
fn real_scheduler_parser_scale_gate() {
    let upstreams = (1..=NODE_COUNT)
        .map(|node_id| UpstreamProcess::spawn(node_id, "representative"))
        .collect::<Vec<_>>();
    assert_eq!(upstreams.len(), 50);
    assert!(upstreams[0].process.child.id() > 0);
    let console = ConsoleProcess::spawn(&upstreams, 5);
    let sources = wait_for_scale_cache(&console);
    let warm_rss = resident_bytes(console.process.child.id());
    let sampler = RssSampler::start(console.process.child.id());

    let summary = console.get("/api/v1/console/summary?window=15m").json();
    assert_eq!(summary["data"]["totals"]["nodes_total"], NODE_COUNT);
    assert_eq!(summary["data"]["totals"]["groups_total"], GROUP_COUNT);
    assert_eq!(summary["data"]["groups"].as_array().unwrap().len(), 500);
    assert_eq!(
        summary["data"]["groups"][0]["observations"]
            .as_array()
            .unwrap()
            .len(),
        50
    );

    let (summary_p95, groups_p95, summary_body_bytes, groups_body_bytes) =
        measure_cached_apis(&console);
    let rss_before_topology = resident_bytes(console.process.child.id());
    let cold_started = Instant::now();
    let cold_topology = console.get("/api/v1/console/topology?view=nodes");
    let cold_topology_elapsed = cold_started.elapsed();
    let cold_topology_status = cold_topology.status;
    let cold_topology = cold_topology.json();
    let rss_after_cold_topology = resident_bytes(console.process.child.id());
    let cold_topology_edges = cold_topology["data"]["edges"].as_array().unwrap();
    let cold_topology_edge_count = cold_topology_edges.len();
    let cold_topology_collection_truncated = cold_topology["data"]["collection_truncated"]
        .as_bool()
        .unwrap();
    let warm_topology_timings = (0..20)
        .map(|_| {
            let started = Instant::now();
            let response = console.get("/api/v1/console/topology?view=nodes");
            assert_eq!(response.status, 200);
            started.elapsed()
        })
        .collect::<Vec<_>>();
    let warm_topology_p95 = percentile_95(warm_topology_timings);
    let rss_after_warm_topology = resident_bytes(console.process.child.id());
    let groups_default = console.get("/api/v1/console/groups").json();
    let groups_default_len = groups_default["data"]["items"].as_array().unwrap().len();
    let groups_max_response = console.get("/api/v1/console/groups?page_size=200");
    let groups_max_status = groups_max_response.status;
    let groups_max = groups_max_response.json();
    let groups_max_len = groups_max["data"]["items"].as_array().map_or(0, Vec::len);
    let groups_max_error = groups_max["error"]["code"].as_str().unwrap_or("NONE");
    let groups_rejected = console.get("/api/v1/console/groups?page_size=201");
    let rss_after_groups = resident_bytes(console.process.child.id());
    let topology_all = console.get("/api/v1/console/topology?view=raft").json();
    let topology_all_len = topology_all["data"]["groups"].as_array().unwrap().len();
    let topology_selected = console
        .get("/api/v1/console/topology?view=raft&group_id=250")
        .json();
    let topology_selected_len = topology_selected["data"]["groups"]
        .as_array()
        .unwrap()
        .len();
    let rss_after_raft_topology = resident_bytes(console.process.child.id());
    let performance = console
        .get("/api/v1/console/metrics/performance?window=15m")
        .json();
    let performance_series = performance["data"]["series"].as_array().unwrap();
    let performance_series_count = performance_series.len();
    let performance_point_count = performance_series
        .iter()
        .map(|series| series["points"].as_array().unwrap().len())
        .sum::<usize>();
    let rss_after_performance = resident_bytes(console.process.child.id());
    let retained_points = sources["data"]["retained_observation_points"]
        .as_u64()
        .unwrap() as usize;
    let events = console.get("/api/v1/console/events?page_size=200");
    let active_series_cap_signal = format!("{}{}", sources, events.body)
        .to_ascii_lowercase()
        .contains("series");

    wait_for_next_scrape(&upstreams[0]);
    let before = upstreams
        .iter()
        .map(|upstream| upstream.counters())
        .collect::<Vec<_>>();
    let elapsed = run_sse_subscribers(&console, Duration::from_secs(21));
    let after = upstreams
        .iter()
        .map(|upstream| upstream.counters())
        .collect::<Vec<_>>();
    let mut admin_qps = before
        .iter()
        .zip(&after)
        .map(|(before, after)| (after.0 - before.0) as f64 / elapsed.as_secs_f64())
        .collect::<Vec<_>>();
    let mut scrape_qps = before
        .iter()
        .zip(&after)
        .map(|(before, after)| (after.1 - before.1) as f64 / elapsed.as_secs_f64())
        .collect::<Vec<_>>();
    let admin_qps_by_node = admin_qps
        .iter()
        .map(|qps| format!("{qps:.6}"))
        .collect::<Vec<_>>()
        .join(",");
    let scrape_qps_by_node = scrape_qps
        .iter()
        .map(|qps| format!("{qps:.6}"))
        .collect::<Vec<_>>()
        .join(",");
    admin_qps.sort_by(f64::total_cmp);
    scrape_qps.sort_by(f64::total_cmp);
    let max_admin_qps = *admin_qps.last().unwrap();
    let max_scrape_qps = *scrape_qps.last().unwrap();
    let nodes_with_admin_traffic = admin_qps.iter().filter(|qps| **qps > 0.0).count();
    let nodes_with_scrapes = scrape_qps.iter().filter(|qps| **qps > 0.0).count();
    let rss_before_history = resident_bytes(console.process.child.id());
    let history = run_history_viewers(&console, &upstreams);
    let deadlines = run_deadline_viewers(&console, &upstreams);
    let peak_rss = sampler.stop();

    println!(
        "SCALE_EVIDENCE upstream_processes={} groups_per_node={} representative_body_bytes={} input_series_per_node={} input_series_total={} normalized_cached_series_total={} counter_rate_series_total={}",
        upstreams.len(),
        GROUP_COUNT,
        representative_metrics(1).len(),
        INPUT_SERIES_PER_NODE,
        INPUT_SERIES_PER_NODE * NODE_COUNT as usize,
        NORMALIZED_SAMPLES_PER_NODE * NODE_COUNT as usize,
        RATE_SERIES_PER_NODE * NODE_COUNT as usize
    );
    println!(
        "SCALE_EVIDENCE cached_summary_p95_ms={:.3} cached_groups_p95_ms={:.3} summary_body_bytes={} groups_body_bytes={}",
        summary_p95.as_secs_f64() * 1_000.0,
        groups_p95.as_secs_f64() * 1_000.0,
        summary_body_bytes,
        groups_body_bytes
    );
    println!(
        "SCALE_EVIDENCE cold_node_topology_ms={:.3} warm_node_topology_p95_ms={:.3} node_topology_edges={} collection_truncated={} cold_status={}",
        cold_topology_elapsed.as_secs_f64() * 1_000.0,
        warm_topology_p95.as_secs_f64() * 1_000.0,
        cold_topology_edge_count,
        cold_topology_collection_truncated,
        cold_topology_status
    );
    println!(
        "SCALE_EVIDENCE sse_subscribers=20 qps_window_seconds={:.3} max_node_admin_qps={:.6} max_node_scrape_qps={:.6} admin_qps_min={:.6} scrape_qps_min={:.6} nodes_with_admin_traffic={} nodes_with_scrapes={}",
        elapsed.as_secs_f64(),
        max_admin_qps,
        max_scrape_qps,
        admin_qps[0],
        scrape_qps[0],
        nodes_with_admin_traffic,
        nodes_with_scrapes
    );
    println!(
        "SCALE_EVIDENCE admin_qps_by_node=[{}] scrape_qps_by_node=[{}]",
        admin_qps_by_node, scrape_qps_by_node
    );
    println!(
        "SCALE_EVIDENCE history_viewers={} overview_requests={} lifecycle_requests={} latest_boot_requests={} history_window_seconds={:.3} history_rpcs_per_node_min={} history_rpcs_per_node_max={} max_history_qps={:.6} max_total_admin_qps={:.6} overview_p95_ms={:.3} lifecycle_p95_ms={:.3} latest_boot_p95_ms={:.3}",
        history.overview.len(),
        history.overview.len(),
        history.lifecycle.len(),
        history.latest.len(),
        history.elapsed.as_secs_f64(),
        history.history_calls.iter().min().unwrap(),
        history.history_calls.iter().max().unwrap(),
        history.history_qps.iter().copied().fold(0.0, f64::max),
        history.total_admin_qps.iter().copied().fold(0.0, f64::max),
        response_p95(&history.overview).as_secs_f64() * 1_000.0,
        response_p95(&history.lifecycle).as_secs_f64() * 1_000.0,
        response_p95(&history.latest).as_secs_f64() * 1_000.0
    );
    println!(
        "SCALE_EVIDENCE deadline_viewers={} deadline_window_seconds={:.3} deadline_history_rpcs={} lifecycle_deadline_p95_ms={:.3} lifecycle_deadline_max_ms={:.3} lifecycle_status={} lifecycle_error={} latest_deadline_p95_ms={:.3} latest_deadline_max_ms={:.3} latest_status={} latest_error={}",
        deadlines.lifecycle.len(),
        deadlines.elapsed.as_secs_f64(),
        deadlines.history_calls,
        response_p95(&deadlines.lifecycle).as_secs_f64() * 1_000.0,
        response_max(&deadlines.lifecycle).as_secs_f64() * 1_000.0,
        deadlines.lifecycle[0].status,
        deadlines.lifecycle[0].error,
        response_p95(&deadlines.latest).as_secs_f64() * 1_000.0,
        response_max(&deadlines.latest).as_secs_f64() * 1_000.0,
        deadlines.latest[0].status,
        deadlines.latest[0].error
    );
    println!(
        "SCALE_EVIDENCE rss_before_topology={} rss_after_cold_topology={} rss_after_warm_topology={} rss_after_groups={} rss_after_raft_topology={} rss_after_performance={} rss_before_history={}",
        rss_before_topology, rss_after_cold_topology, rss_after_warm_topology, rss_after_groups, rss_after_raft_topology, rss_after_performance, rss_before_history
    );
    println!(
        "SCALE_EVIDENCE bff_rss_warm_bytes={} bff_rss_peak_bytes={} bff_rss_peak_mib={:.3}",
        warm_rss,
        peak_rss,
        peak_rss as f64 / (1024.0 * 1024.0)
    );
    println!(
        "SCALE_EVIDENCE groups_default_items={} groups_max_items={} groups_max_status={} groups_max_error={} groups_201_status={} topology_unselected_groups={} topology_selected_groups={} retained_points={} performance_series={} performance_points={} active_series_cap_signal={}",
        groups_default_len,
        groups_max_len,
        groups_max_status,
        groups_max_error,
        groups_rejected.status,
        topology_all_len,
        topology_selected_len,
        retained_points,
        performance_series_count,
        performance_point_count,
        active_series_cap_signal
    );

    let mut report = BudgetReport::default();
    report.check(
        summary_p95 <= Duration::from_millis(300),
        format!(
            "cached summary P95 {:.3}ms exceeds 300ms",
            summary_p95.as_secs_f64() * 1_000.0
        ),
    );
    report.check(
        groups_p95 <= Duration::from_millis(300),
        format!(
            "cached group list P95 {:.3}ms exceeds 300ms",
            groups_p95.as_secs_f64() * 1_000.0
        ),
    );
    report.check(
        max_admin_qps <= 1.0,
        format!("max per-node Admin QPS {max_admin_qps:.6} exceeds 1.0"),
    );
    report.check(
        max_scrape_qps <= 0.2,
        format!("max per-node scrape QPS {max_scrape_qps:.6} exceeds 0.2"),
    );
    report.check(
        nodes_with_admin_traffic == NODE_COUNT as usize
            && nodes_with_scrapes == NODE_COUNT as usize,
        format!("only {nodes_with_admin_traffic}/50 nodes received Admin polls and {nodes_with_scrapes}/50 received metrics scrapes during the subscriber window"),
    );
    report.check(
        peak_rss <= MAX_BFF_RSS_BYTES,
        format!("BFF peak RSS {peak_rss} bytes exceeds {MAX_BFF_RSS_BYTES}"),
    );
    report.check(
        active_series_cap_signal,
        format!(
            "{} normalized cached samples per node ({} cluster-wide) plus {} rate series per node produced no active-series rejection/downsampling signal; budget is {} cluster-wide",
            NORMALIZED_SAMPLES_PER_NODE,
            NORMALIZED_SAMPLES_PER_NODE * NODE_COUNT as usize,
            RATE_SERIES_PER_NODE,
            MAX_ACTIVE_SERIES
        ),
    );
    report.check(
        retained_points <= MAX_POINTS && performance_point_count <= MAX_POINTS,
        format!("retained/performance points {retained_points}/{performance_point_count} exceed {MAX_POINTS}"),
    );
    report.check(
        performance_series_count <= MAX_ACTIVE_SERIES,
        format!("performance series {performance_series_count} exceeds {MAX_ACTIVE_SERIES}"),
    );
    report.check(
        groups_default_len == 50
            && groups_max_status == 200
            && groups_max_len == 200
            && groups_rejected.status == 400,
        format!("group pagination bounds are default={groups_default_len}, max={groups_max_len} (status={groups_max_status}, error={groups_max_error}), 201 status={}", groups_rejected.status),
    );
    report.check(
        cold_topology_status == 200
            && cold_topology_edge_count == NODE_COUNT as usize - 1
            && cold_topology_collection_truncated
            && cold_topology_edges
                .iter()
                .all(|edge| edge["group_count"] == 200),
        format!(
            "cold nodes topology returned status={cold_topology_status}, edges={cold_topology_edge_count}, truncated={cold_topology_collection_truncated}, evidence={}",
            cold_topology["data"]["edges"]
        ),
    );
    report.check(
        cold_topology_elapsed <= Duration::from_millis(2_500)
            && warm_topology_p95 <= Duration::from_millis(300),
        format!(
            "nodes topology cold={:.3}ms warm P95={:.3}ms exceeds the 2s deadline plus 500ms tolerance / 300ms warm budget",
            cold_topology_elapsed.as_secs_f64() * 1_000.0,
            warm_topology_p95.as_secs_f64() * 1_000.0
        ),
    );
    report.check(
        topology_all_len <= 200,
        format!("unselected Raft topology renders {topology_all_len} groups; bound is 200"),
    );
    report.check(
        topology_selected_len == 1,
        format!("selected Raft topology returned {topology_selected_len} groups instead of 1"),
    );
    report.check(
        history.overview.len() == 20
            && history.lifecycle.len() == 20
            && history.latest.len() == 1_000
            && history
                .overview
                .iter()
                .all(|response| response.status == 200)
            && history
                .lifecycle
                .iter()
                .all(|response| response.status == 200 && response.items == 1)
            && history
                .latest
                .iter()
                .all(|response| response.status == 200 && response.items == 1),
        format!(
            "responsive history viewers failed: overview={:?}, lifecycle={:?}, latest={:?}",
            history
                .overview
                .iter()
                .map(|response| response.status)
                .collect::<Vec<_>>(),
            history
                .lifecycle
                .iter()
                .map(|response| (response.status, response.items))
                .collect::<Vec<_>>(),
            history
                .latest
                .iter()
                .filter(|response| response.status != 200 || response.items != 1)
                .map(|response| (response.status, response.items))
                .collect::<Vec<_>>()
        ),
    );
    report.check(
        history.history_calls.iter().all(|calls| *calls == 2),
        format!(
            "20 viewers produced boot-history RPCs per node {:?}; expected exactly 2 per node",
            history.history_calls
        ),
    );
    report.check(
        history.history_qps.iter().copied().fold(0.0, f64::max) <= 1.0,
        format!(
            "maximum shared boot/latest-history QPS {:.6} exceeds 1.0 RPC/s/node",
            history.history_qps.iter().copied().fold(0.0, f64::max)
        ),
    );
    report.check(
        response_p95(&history.overview) <= Duration::from_millis(300),
        format!(
            "20-viewer overview P95 {:.3}ms exceeds 300ms",
            response_p95(&history.overview).as_secs_f64() * 1_000.0
        ),
    );
    report.check(
        response_p95(&history.lifecycle) <= Duration::from_secs(2)
            && response_p95(&history.latest) <= Duration::from_secs(2),
        format!(
            "responsive on-demand P95s lifecycle={:.3}ms latest={:.3}ms exceed 2000ms",
            response_p95(&history.lifecycle).as_secs_f64() * 1_000.0,
            response_p95(&history.latest).as_secs_f64() * 1_000.0
        ),
    );
    report.check(
        deadlines.lifecycle.len() == 20
            && deadlines.latest.len() == 20
            && deadlines.lifecycle.iter().all(|response| {
                response.status == 504 && response.error == "HANDLER_DEADLINE_EXCEEDED"
            })
            && deadlines.latest.iter().all(|response| {
                response.status == 504 && response.error == "HANDLER_DEADLINE_EXCEEDED"
            }),
        format!(
            "unresponsive-history responses were lifecycle={:?}, latest={:?}",
            deadlines
                .lifecycle
                .iter()
                .map(|response| (response.status, &response.error))
                .collect::<Vec<_>>(),
            deadlines
                .latest
                .iter()
                .map(|response| (response.status, &response.error))
                .collect::<Vec<_>>()
        ),
    );
    report.check(
        response_p95(&deadlines.lifecycle) <= Duration::from_millis(2_500)
            && response_p95(&deadlines.latest) <= Duration::from_millis(2_500),
        format!(
            "on-demand deadline P95s lifecycle={:.3}ms latest={:.3}ms exceed the 2s budget plus 500ms scheduling tolerance",
            response_p95(&deadlines.lifecycle).as_secs_f64() * 1_000.0,
            response_p95(&deadlines.latest).as_secs_f64() * 1_000.0
        ),
    );
    report.check(
        deadlines.history_calls == NODE_COUNT + 1,
        format!(
            "20 unresponsive-history viewers triggered {} upstream history RPCs; expected {} from shared per-node/per-page flights",
            deadlines.history_calls,
            NODE_COUNT + 1
        ),
    );

    drop(console);
    drop(upstreams);

    let mut cap_upstreams = vec![
        UpstreamProcess::spawn(1, "oversize-body"),
        UpstreamProcess::spawn(2, "too-many-series"),
    ];
    let cap_console = ConsoleProcess::spawn(&cap_upstreams, 1);
    let (body_cap_error, series_cap_error) = wait_for_cap_errors(&cap_console);
    println!(
        "SCALE_EVIDENCE oversize_body_bytes={} body_cap_error={} input_series_over_cap={} series_cap_error={}",
        MAX_SCRAPE_BYTES + 1,
        body_cap_error,
        MAX_INPUT_SERIES + 1,
        series_cap_error
    );
    report.check(
        body_cap_error == "METRICS_BODY_TOO_LARGE",
        format!("8MiB+1 body produced {body_cap_error:?}, expected METRICS_BODY_TOO_LARGE"),
    );
    report.check(
        series_cap_error == "TOO_MANY_SERIES",
        format!("100001 series produced {series_cap_error:?}, expected TOO_MANY_SERIES"),
    );
    drop(cap_console);
    for upstream in &mut cap_upstreams {
        upstream.process.terminate();
    }
    report.finish();
}
