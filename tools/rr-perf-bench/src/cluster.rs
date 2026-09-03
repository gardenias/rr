use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use tempfile::TempDir;
use tokio::process::{Child, Command};

/// Port layout for a single node.
#[derive(Clone, Debug)]
pub struct NodePorts {
    pub id: u64,
    pub grpc: u16,     // client + raft gRPC
    pub admin: u16,    // admin gRPC (rr-admin target)
    pub metrics: u16,  // HTTP /health /metrics
    pub raft_tcp: u16, // tcp raft append/vote
}

impl NodePorts {
    fn for_node(id: u64, port_base: u16) -> Self {
        let base = port_base + (id as u16 - 1) * 10;
        NodePorts {
            id,
            grpc: base + 1,
            admin: base + 2,
            metrics: base + 3,
            raft_tcp: base + 4,
        }
    }

    pub fn grpc_addr(&self) -> String {
        format!("127.0.0.1:{}", self.grpc)
    }

    pub fn admin_addr(&self) -> String {
        format!("127.0.0.1:{}", self.admin)
    }
}

/// Configuration for the benchmark cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub server_bin: PathBuf,
    pub admin_bin: PathBuf,
    pub data_dir: PathBuf,
    pub storage: StorageKind,
    pub durability: DurabilityKind,
    pub log_to_files: bool,
    /// Number of voter nodes in the cluster (1, 3 or 5).
    pub members: u8,
    /// Base port for the cluster.  Node n uses ports
    /// `base + (n-1)*10 + {1,2,3,4}` for gRPC / admin / metrics / raft_tcp.
    /// Default 47000 (avoids the 51000 range used by other tools).
    pub port_base: u16,
    pub use_tcp_transport: bool,
    /// Optional per-node client rate limit (req/s), passed as `--client-rps`.
    pub client_rps: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Mem,
    Rocks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityKind {
    Strict,
    ReplicatedOnly,
    TestOnly,
}

impl DurabilityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            DurabilityKind::Strict => "strict",
            DurabilityKind::ReplicatedOnly => "replicated_only",
            DurabilityKind::TestOnly => "test_only",
        }
    }
}

/// Handle for the running cluster.
pub struct Cluster {
    nodes: Vec<RunningNode>,
    pub ports: Vec<NodePorts>,
    _data_dir: Option<TempDir>,
    admin_bin: PathBuf,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            let _ = n.child.start_kill();
        }
    }
}

struct RunningNode {
    child: Child,
    ports: NodePorts,
}

impl Cluster {
    /// Start the cluster: spawn, bootstrap, wait for leader.
    pub async fn start(cfg: &ClusterConfig) -> Result<Self> {
        let (data_dir_owned, base_dir) = if cfg.data_dir.as_os_str().is_empty() {
            let tmp = TempDir::new().context("create tempdir")?;
            let path = tmp.path().to_owned();
            (Some(tmp), path)
        } else {
            std::fs::create_dir_all(&cfg.data_dir).context("create data dir")?;
            (None, cfg.data_dir.clone())
        };

        let port_base = cfg.port_base;
        let ports: Vec<NodePorts> = (1..=u64::from(cfg.members))
            .map(|id| NodePorts::for_node(id, port_base))
            .collect();
        let mut nodes = Vec::new();

        for (i, p) in ports.iter().enumerate() {
            let node_dir = base_dir.join(format!("node{}", p.id));
            std::fs::create_dir_all(&node_dir)?;

            let log_dir = base_dir.join("logs");
            std::fs::create_dir_all(&log_dir)?;

            let child = spawn_server(
                &cfg.server_bin,
                p,
                &node_dir,
                i == 0, // node 1 bootstraps
                cfg.storage,
                cfg.durability,
                cfg.use_tcp_transport,
                cfg.client_rps,
                cfg.log_to_files
                    .then(|| log_dir.join(format!("node{}.log", p.id))),
            )
            .await
            .with_context(|| format!("spawn node {}", p.id))?;

            nodes.push(RunningNode {
                child,
                ports: p.clone(),
            });
        }

        // Wrap in Cluster now so Drop fires if anything below fails,
        // killing all spawned children automatically.
        let cluster = Cluster {
            nodes,
            ports: ports.clone(),
            _data_dir: data_dir_owned,
            admin_bin: cfg.admin_bin.clone(),
        };

        // Wait for gRPC ports + (when TCP transport enabled) TCP Raft ports
        for p in &ports {
            wait_tcp(&format!("127.0.0.1:{}", p.grpc), 20)
                .await
                .with_context(|| format!("node {} gRPC port did not open", p.id))?;
            if cfg.use_tcp_transport {
                wait_tcp(&format!("127.0.0.1:{}", p.raft_tcp), 20)
                    .await
                    .with_context(|| format!("node {} TCP Raft port did not open", p.id))?;
            }
        }

        // Bootstrap: add remaining nodes as learners, then promote to voters
        cluster.bootstrap_cluster().await?;

        // Wait until a leader is elected
        cluster
            .wait_leader(30)
            .await
            .context("timeout waiting for leader after bootstrap")?;

        // Allow the leader to establish outbound connections to all followers
        // before returning.  Without this, the first few proposals arrive
        // while TCP connections are still being set up and fail with timeouts.
        tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(cluster)
    }

    /// gRPC addresses for all nodes (for RrClient bootstrap list).
    pub fn all_grpc_addrs(&self) -> Vec<String> {
        self.ports.iter().map(|p| p.grpc_addr()).collect()
    }

    /// Send SIGKILL to the current leader. Returns its node_id.
    // Reserved for the crash-recovery / soak harness (reliability program R-4/R-8).
    #[allow(dead_code)]
    pub async fn kill_leader(&mut self) -> Result<u64> {
        let leader_id = self.find_leader_id().await?;
        let node = self
            .nodes
            .iter_mut()
            .find(|n| n.ports.id == leader_id)
            .ok_or_else(|| anyhow!("leader node {leader_id} not in list"))?;
        node.child.kill().await.context("kill leader process")?;
        Ok(leader_id)
    }

    /// Wait until a node is the leader; return the leader's gRPC address.
    pub async fn wait_leader(&self, timeout_secs: u64) -> Result<String> {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        while Instant::now() < deadline {
            if let Ok(id) = self.find_leader_id().await {
                let p = self.ports.iter().find(|p| p.id == id).unwrap();
                return Ok(p.grpc_addr());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        bail!("no leader elected within {timeout_secs}s")
    }

    /// Graceful shutdown (SIGTERM all nodes).
    pub async fn shutdown(mut self) {
        for n in &mut self.nodes {
            let _ = n.child.kill().await;
        }
        for n in &mut self.nodes {
            let _ = n.child.wait().await;
        }
    }

    // ── private helpers ────────────────────────────────────────────────────

    async fn bootstrap_cluster(&self) -> Result<()> {
        // A single-node group self-initializes via --bootstrap; nothing to do.
        if self.ports.len() == 1 {
            return Ok(());
        }

        let admin_addr = self.ports[0].admin_addr();

        // Give node 1 a moment to finish raft.initialize() after bootstrap
        tokio::time::sleep(Duration::from_millis(500)).await;

        for p in &self.ports[1..] {
            run_admin(
                &self.admin_bin,
                &admin_addr,
                &[
                    "group",
                    "add-learner",
                    "--group",
                    "1",
                    "--node-id",
                    &p.id.to_string(),
                    "--addr",
                    &format!("127.0.0.1:{}", p.grpc),
                ],
            )
            .await
            .with_context(|| format!("add-learner node {}", p.id))?;
        }

        let voters = self
            .ports
            .iter()
            .map(|p| p.id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        run_admin(
            &self.admin_bin,
            &admin_addr,
            &[
                "group",
                "change-membership",
                "--group",
                "1",
                "--voters",
                voters.as_str(),
            ],
        )
        .await
        .with_context(|| format!("change-membership to voters {voters}"))?;

        Ok(())
    }

    async fn find_leader_id(&self) -> Result<u64> {
        for p in &self.ports {
            let out = Command::new(&self.admin_bin)
                .args([
                    "--addr",
                    &p.admin_addr(),
                    "--output",
                    "json",
                    "group",
                    "status",
                    "--group",
                    "1",
                ])
                .output()
                .await?;
            if let Ok(j) = serde_json::from_slice::<serde_json::Value>(&out.stdout) {
                if j.get("is_leader").and_then(|v| v.as_bool()) == Some(true) {
                    return Ok(p.id);
                }
            }
        }
        bail!("no leader found")
    }
}

// Process-spawn parameter bag; grouping into a struct adds indirection without
// payoff for this internal helper.
#[allow(clippy::too_many_arguments)]
async fn spawn_server(
    bin: &Path,
    ports: &NodePorts,
    data_dir: &Path,
    bootstrap: bool,
    storage: StorageKind,
    durability: DurabilityKind,
    use_tcp_transport: bool,
    client_rps: Option<u32>,
    log_file: Option<PathBuf>,
) -> Result<Child> {
    let mut cmd = Command::new(bin);
    cmd.arg("serve")
        .arg("--node-id")
        .arg(ports.id.to_string())
        .arg("--listen-addr")
        .arg(format!("127.0.0.1:{}", ports.grpc))
        .arg("--admin-port")
        .arg(ports.admin.to_string())
        .arg("--metrics-port")
        .arg(ports.metrics.to_string())
        .arg("--storage")
        .arg(match storage {
            StorageKind::Mem => "mem",
            StorageKind::Rocks => "rocks",
        })
        .arg("--durability")
        .arg(durability.as_str())
        .arg("--allow-insecure")
        .arg("--log-level")
        .arg("warn");

    if storage == StorageKind::Rocks {
        cmd.arg("--data-dir").arg(data_dir);
    }

    if bootstrap {
        cmd.arg("--bootstrap");
    }

    if use_tcp_transport {
        cmd.arg("--raft-transport")
            .arg("tcp")
            .arg("--raft-tcp-port")
            .arg(ports.raft_tcp.to_string());
    }

    if let Some(rps) = client_rps {
        cmd.arg("--client-rps").arg(rps.to_string());
    }

    if let Some(log_path) = log_file {
        let file = std::fs::File::create(&log_path)
            .with_context(|| format!("create log file {}", log_path.display()))?;
        cmd.stdout(Stdio::from(file.try_clone()?));
        cmd.stderr(Stdio::from(file));
    } else {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }

    cmd.spawn().context("spawn rr-server")
}

async fn run_admin(bin: &Path, addr: &str, args: &[&str]) -> Result<()> {
    let out = Command::new(bin)
        .arg("--addr")
        .arg(addr)
        .args(args)
        .output()
        .await
        .context("run rr-admin")?;

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        bail!(
            "rr-admin {:?} failed (status={}):\nstdout: {}\nstderr: {}",
            args,
            out.status,
            stdout.trim(),
            stderr.trim()
        );
    }
    Ok(())
}

/// Poll a TCP address until it accepts a connection or the timeout expires.
async fn wait_tcp(addr: &str, timeout_secs: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("timeout waiting for TCP port {addr}")
}

/// Locate the rr-server/rr-admin binaries.
///
/// Resolution order:
/// 1. Environment variable (`RR_SERVER_BIN` / `RR_ADMIN_BIN`)
/// 2. Sibling of the current executable in the same `target/{profile}/` dir
/// 3. `cargo build` auto-build if neither is found
pub fn find_bin(name: &str, env_var: &str) -> Result<PathBuf> {
    if let Ok(path) = std::env::var(env_var) {
        let p = PathBuf::from(path);
        if p.exists() {
            return Ok(p);
        }
    }

    // Try sibling in the same target directory as this binary
    if let Ok(exe) = std::env::current_exe() {
        let sibling = exe.parent().map(|d| d.join(name));
        if let Some(p) = sibling {
            if p.exists() {
                return Ok(p);
            }
        }
    }

    bail!(
        "cannot find `{name}` binary — set `{env_var}` env var or run \
         `cargo build -p rr-server -p rr-admin` first"
    )
}
