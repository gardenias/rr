//! Process orchestration for the crash harness: spawn/kill/restart real
//! `rr-crash-node` processes, each with its own data dir and log file.
//!
//! Port layout, `wait_tcp` probing, and binary location follow the verified
//! patterns of `tools/rr-perf-bench/src/cluster.rs`, with two deliberate
//! changes:
//!
//! - **Dynamic port base.** rr-perf-bench hardcodes 47000; that range belongs
//!   to the bench tool, so we probe free ports instead (`CRASH_PORT_BASE`
//!   overrides for debugging).
//! - **Stronger reaping.** `Drop` SIGKILLs every still-running child and then
//!   reaps with `try_wait` polling, so a panicking/failed test never leaks
//!   `rr-crash-node` processes.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use tempfile::TempDir;
use tokio::process::{Child, Command};

use crate::journal::{now_ms, FaultEvent, Journal};

/// The single Raft group the harness drives (`partition_id == group_id`).
pub const GROUP_ID: u64 = 1;

/// Signal send wrapper: one narrowly-scoped `unsafe` for process control.
///
/// SAFETY: `kill(2)` with a valid signal number is always memory-safe; the
/// only failure modes are ESrch (already dead — fine, kill is idempotent
/// for our purposes) and EPERM (not our child — cannot happen: the pid came
/// from the `Child` we spawned). There is no wrapper in `std` for sending
/// arbitrary signals, and `libc` is already in the workspace lockfile, so a
/// `Command`-based `kill -9` subprocess fallback would add failure modes
/// (PATH lookup, shell quoting of pids) for no portability gain on the
/// unix-only CI this harness targets.
#[cfg(unix)]
fn send_signal(pid: u32, sig: libc::c_int) -> std::io::Result<()> {
    // SAFETY: see fn docs — pid belongs to a child we spawned.
    let rc = unsafe { libc::kill(pid as libc::pid_t, sig) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Port layout for one node. Mirrors rr-perf-bench's `NodePorts`: a 10-wide
/// stride per node so related services stay adjacent. The harness node only
/// serves one combined gRPC port (client + raft), but the layout keeps slots
/// for admin/metrics so future harness extensions (or a switch back to the
/// stock binary) don't reshuffle the numbering.
#[derive(Clone, Debug)]
pub struct NodePorts {
    pub id: u64,
    /// Client + raft gRPC.
    pub grpc: u16,
}

impl NodePorts {
    pub fn for_node(id: u64, port_base: u16) -> Self {
        let base = port_base + (id as u16 - 1) * 10;
        NodePorts { id, grpc: base + 1 }
    }

    pub fn grpc_addr(&self) -> String {
        format!("127.0.0.1:{}", self.grpc)
    }
}

/// Probe for a port base whose three node gRPC slots are all free.
///
/// TOCTOU is inherent (the port can be grabbed between probe and bind); the
/// cluster start retries with a fresh base on early child exit, which covers
/// the race in practice.
fn find_free_port_base() -> Result<u16> {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::from_entropy();
    for _ in 0..64 {
        let base = rng.gen_range(20_000u16..=55_000);
        let all_free = (1..=3u64)
            .map(|id| NodePorts::for_node(id, base).grpc)
            .all(|port| std::net::TcpListener::bind(("127.0.0.1", port)).is_ok());
        if all_free {
            return Ok(base);
        }
    }
    bail!("could not find a free port base after 64 probes")
}

/// Resolve the port base: `CRASH_PORT_BASE` env wins, else probe.
pub fn port_base() -> Result<u16> {
    if let Ok(v) = std::env::var("CRASH_PORT_BASE") {
        return v
            .parse::<u16>()
            .with_context(|| format!("CRASH_PORT_BASE={v} is not a valid u16"));
    }
    find_free_port_base()
}

/// Locate a harness binary.
///
/// Resolution order (extends rr-perf-bench's `find_bin` with case 3):
/// 1. Environment variable (`RR_CRASH_NODE_BIN`).
/// 2. Sibling of the current executable in the same `target/{profile}/` dir
///    (covers running from a bin target).
/// 3. Parent of the current executable's dir (covers `cargo test`, whose
///    test binaries live in `target/{profile}/deps/` one level below the
///    built binary).
pub fn find_bin(name: &str, env_var: &str) -> Result<PathBuf> {
    if let Ok(path) = std::env::var(env_var) {
        let p = PathBuf::from(path);
        if p.exists() {
            return Ok(p);
        }
        bail!("{env_var}={} does not exist", p.display());
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join(name);
            if sibling.exists() {
                return Ok(sibling);
            }
            if let Some(profile_dir) = dir.parent() {
                let above = profile_dir.join(name);
                if above.exists() {
                    return Ok(above);
                }
            }
        }
    }

    bail!(
        "cannot find `{name}` binary — set `{env_var}` or run \
         `cargo build -p crash-tests --features rocks` first"
    )
}

/// Configuration for [`CrashCluster::start`].
pub struct ClusterConfig {
    pub server_bin: PathBuf,
    /// Directory for node logs + journals. Persisted across the run (NOT a
    /// TempDir) so CI can upload it on failure.
    pub run_dir: PathBuf,
    pub port_base: u16,
    pub log_level: String,
    /// Per-node `--snapshot-logs-since-last` override (scenario C lowers it
    /// to force snapshot/purge cycles); `None` keeps the production default.
    pub snapshot_logs_since_last: Option<u64>,
}

struct NodeSpec {
    ports: NodePorts,
    data_dir: PathBuf,
    boot: u32,
}

struct RunningNode {
    spec: NodeSpec,
    child: Option<Child>,
}

/// A running 3-node cluster of real `rr-crash-node` processes.
///
/// Dropping kills (SIGKILL) and reaps every still-running child — tests get
/// correct cleanup on panic paths without any explicit teardown call.
pub struct CrashCluster {
    nodes: Vec<RunningNode>,
    pub ports: Vec<NodePorts>,
    server_bin: PathBuf,
    run_dir: PathBuf,
    log_level: String,
    snapshot_logs_since_last: Option<u64>,
    /// Owns the per-node data dirs; dropped last (after children are dead).
    _data_root: TempDir,
}

impl Drop for CrashCluster {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            if let Some(mut child) = n.child.take() {
                if let Some(pid) = child.id() {
                    let _ = send_signal(pid, libc::SIGKILL);
                }
                // Reap best-effort: Drop cannot await and may run with the
                // tokio runtime already gone, so poll try_wait briefly.
                let deadline = Instant::now() + Duration::from_secs(2);
                loop {
                    match child.try_wait() {
                        Ok(Some(_)) => break,
                        Ok(None) if Instant::now() < deadline => {
                            std::thread::sleep(Duration::from_millis(50));
                        }
                        _ => break,
                    }
                }
            }
        }
    }
}

impl CrashCluster {
    /// Start a 3-node cluster: spawn, wait for gRPC ports, wait until the
    /// group is writable. Retries with fresh port bases to absorb probe
    /// TOCTOU races.
    pub async fn start(cfg: &ClusterConfig) -> Result<Self> {
        let mut last_err = None;
        for attempt in 1..=3 {
            let base = if attempt == 1 {
                cfg.port_base
            } else {
                find_free_port_base()?
            };
            match Self::start_once(cfg, base).await {
                Ok(cluster) => return Ok(cluster),
                Err(e) => {
                    eprintln!("[crash-harness] cluster start attempt {attempt} failed: {e:#}");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("cluster start failed")))
    }

    async fn start_once(cfg: &ClusterConfig, port_base: u16) -> Result<Self> {
        let data_root = TempDir::new().context("create data-dir tempdir")?;
        let ports: Vec<NodePorts> = (1..=3)
            .map(|id| NodePorts::for_node(id, port_base))
            .collect();
        let log_dir = cfg.run_dir.join("logs");
        std::fs::create_dir_all(&log_dir).context("create node log dir")?;

        let roster = roster_arg(&ports);
        let mut nodes = Vec::new();
        for p in &ports {
            let data_dir = data_root.path().join(format!("node{}", p.id));
            std::fs::create_dir_all(&data_dir)?;
            let spec = NodeSpec {
                ports: p.clone(),
                data_dir,
                boot: 1,
            };
            let child = spawn_node(
                &cfg.server_bin,
                &spec,
                &roster,
                &cfg.log_level,
                cfg.snapshot_logs_since_last,
                &log_dir,
            )
            .with_context(|| format!("spawn node {}", p.id))?;
            nodes.push(RunningNode {
                spec,
                child: Some(child),
            });
        }

        // Construct the cluster now so Drop reaps children if anything below
        // fails (e.g. a node exits immediately on a port race).
        let mut cluster = CrashCluster {
            nodes,
            ports: ports.clone(),
            server_bin: cfg.server_bin.clone(),
            run_dir: cfg.run_dir.clone(),
            log_level: cfg.log_level.clone(),
            snapshot_logs_since_last: cfg.snapshot_logs_since_last,
            _data_root: data_root,
        };

        for p in &ports {
            cluster
                .wait_node_tcp(p.id, 20)
                .await
                .with_context(|| format!("node {} gRPC port did not open", p.id))?;
        }

        // Wait until the cluster elects a leader and serves writes. The probe
        // write uses a separate request_type so it never lands in the ack log.
        let client = crate::driver::connect(&cluster.all_grpc_addrs()).await?;
        crate::driver::wait_writable(&client, Duration::from_secs(30))
            .await
            .context("cluster did not become writable after bootstrap")?;

        Ok(cluster)
    }

    /// gRPC addresses of all nodes (client bootstrap list).
    pub fn all_grpc_addrs(&self) -> Vec<String> {
        self.ports.iter().map(|p| p.grpc_addr()).collect()
    }

    /// Directory holding this node's logs (one file per boot).
    pub fn log_dir(&self) -> PathBuf {
        self.run_dir.join("logs")
    }

    /// Log file of the node's current boot.
    pub fn node_log_path(&self, id: u64) -> PathBuf {
        let node = self.node(id);
        self.log_dir()
            .join(format!("node{}.boot{}.log", id, node.spec.boot))
    }

    /// Path of the node's persisted state-machine snapshot (the T1 durable
    /// artifact). The `group-{GROUP_ID}` segment mirrors rr-crash-node's
    /// `data_dir/group-{GROUP_ID}` rocks layout.
    pub fn sm_snapshot_path(&self, id: u64) -> PathBuf {
        self.node(id)
            .spec
            .data_dir
            .join(format!("group-{GROUP_ID}"))
            .join("sm-snapshot.bin")
    }

    /// SIGKILL a node and reap it. Returns after the process is dead.
    pub async fn kill_node(&mut self, id: u64) -> Result<()> {
        let node = self.node_mut(id);
        let child = node
            .child
            .as_mut()
            .ok_or_else(|| anyhow!("node {id} is not running"))?;
        let pid = child.id().ok_or_else(|| anyhow!("node {id} pid gone"))?;
        send_signal(pid, libc::SIGKILL).with_context(|| format!("SIGKILL node {id}"))?;
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .context("timeout reaping SIGKILLed node")??;
        node.child = None;
        Ok(())
    }

    /// SIGTERM a node; returns the exit status once it exits (deadline
    /// enforced by the caller-visible timeout). `Ok(None)` on timeout means
    /// the process did NOT exit in time — the graceful-shutdown contract
    /// failed.
    pub async fn term_node(
        &mut self,
        id: u64,
        deadline: Duration,
    ) -> Result<Option<std::process::ExitStatus>> {
        let node = self.node_mut(id);
        let child = node
            .child
            .as_mut()
            .ok_or_else(|| anyhow!("node {id} is not running"))?;
        let pid = child.id().ok_or_else(|| anyhow!("node {id} pid gone"))?;
        send_signal(pid, libc::SIGTERM).with_context(|| format!("SIGTERM node {id}"))?;
        match tokio::time::timeout(deadline, child.wait()).await {
            Ok(status) => {
                let status = status.context("wait on SIGTERMed node")?;
                node.child = None;
                Ok(Some(status))
            }
            Err(_) => Ok(None),
        }
    }

    /// Restart a dead node with the same data dir and ports.
    pub async fn restart_node(&mut self, id: u64) -> Result<()> {
        let log_dir = self.log_dir();
        let roster = roster_arg(&self.ports);
        let server_bin = self.server_bin.clone();
        let log_level = self.log_level.clone();
        let snapshot_logs_since_last = self.snapshot_logs_since_last;
        let node = self.node_mut(id);
        if node.child.is_some() {
            bail!("node {id} is still running — kill/term it first");
        }
        node.spec.boot += 1;
        let child = spawn_node(
            &server_bin,
            &node.spec,
            &roster,
            &log_level,
            snapshot_logs_since_last,
            &log_dir,
        )
        .with_context(|| format!("respawn node {id}"))?;
        node.child = Some(child);
        self.wait_node_tcp(id, 30)
            .await
            .with_context(|| format!("node {id} gRPC port did not reopen after restart"))?;
        Ok(())
    }

    /// Wait until node `id` has applied at least `frontier` unique keyed
    /// writes, observed through its own local read path. `frontier` is the
    /// ack-log length sampled at kill time: every acknowledged write must be
    /// present on a fully caught-up replica.
    pub async fn wait_caught_up(&self, id: u64, frontier: u64, timeout: Duration) -> Result<u64> {
        let addr = self.ports[(id - 1) as usize].grpc_addr();
        let deadline = Instant::now() + timeout;
        let mut last = 0u64;
        while Instant::now() < deadline {
            match read_applied_count(&addr).await {
                Ok(n) => {
                    last = n;
                    if n >= frontier {
                        return Ok(n);
                    }
                }
                Err(_) => { /* node still starting / mid-election; retry */ }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // Diagnose the most likely wedge before bailing: a dead raft core
        // leaves gRPC serving while apply is frozen.
        let hint = if log_contains(&self.node_log_path(id), "panicked at") {
            " — node log contains a PANIC: its raft core is dead. \
             (Regression guard: R-4 found openraft's debug-build \
             `entries_stream` read-after-submit assert killing the sm worker \
             when appends raced the batcher flush; fixed by the pending \
             overlay in RocksLogStorage, ADR A-0015. A recurrence means the \
             overlay regressed.)"
        } else {
            ""
        };
        bail!("node {id} did not catch up: applied={last} < frontier={frontier}{hint}")
    }

    async fn wait_node_tcp(&mut self, id: u64, timeout_secs: u64) -> Result<()> {
        let addr = self.ports[(id - 1) as usize].grpc_addr();
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        while Instant::now() < deadline {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                return Ok(());
            }
            // Fail fast (with log context) when the process died instead of
            // waiting out the full timeout — e.g. port race or bad args.
            let exited = {
                let node = self.node_mut(id);
                match node.child.as_mut() {
                    Some(child) => child.try_wait().ok().flatten(),
                    None => None,
                }
            };
            if let Some(status) = exited {
                let tail = log_tail(&self.node_log_path(id), 40);
                bail!("node {id} exited early ({status}) during startup; log tail:\n{tail}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let tail = log_tail(&self.node_log_path(id), 40);
        bail!("timeout waiting for TCP port {addr}; node log tail:\n{tail}")
    }

    fn node(&self, id: u64) -> &RunningNode {
        &self.nodes[(id - 1) as usize]
    }

    fn node_mut(&mut self, id: u64) -> &mut RunningNode {
        &mut self.nodes[(id - 1) as usize]
    }
}

/// Read the node's applied-unique-key count via its local (stale) read path.
async fn read_applied_count(grpc_addr: &str) -> Result<u64> {
    let client = rr_client::RrClient::connect(vec![grpc_addr.to_string()])
        .await
        .context("connect for count read")?;
    let bytes = client
        .read(GROUP_ID, crate::driver::COUNT_READ_QUERY.to_vec())
        .await
        .context("count read")?;
    let text = String::from_utf8(bytes).context("count read not utf-8")?;
    text.trim()
        .parse::<u64>()
        .with_context(|| format!("count read not a number: {text:?}"))
}

/// `"1@addr1,2@addr2,3@addr3"` roster string shared by all nodes.
fn roster_arg(ports: &[NodePorts]) -> String {
    ports
        .iter()
        .map(|p| format!("{}@{}", p.id, p.grpc_addr()))
        .collect::<Vec<_>>()
        .join(",")
}

fn spawn_node(
    bin: &Path,
    spec: &NodeSpec,
    roster: &str,
    log_level: &str,
    snapshot_logs_since_last: Option<u64>,
    log_dir: &Path,
) -> Result<Child> {
    let log_path = log_dir.join(format!("node{}.boot{}.log", spec.ports.id, spec.boot));
    let log_file = open_log_append(&log_path)?;

    let mut cmd = Command::new(bin);
    cmd.arg("--node-id")
        .arg(spec.ports.id.to_string())
        .arg("--listen-addr")
        .arg(spec.ports.grpc_addr())
        .arg("--data-dir")
        .arg(&spec.data_dir)
        .arg("--roster")
        .arg(roster)
        .arg("--log-level")
        .arg(log_level)
        // Panic backtraces into the node log — crash forensics depend on it.
        .env("RUST_BACKTRACE", "1")
        .stdout(Stdio::from(log_file.try_clone()?))
        .stderr(Stdio::from(log_file));
    if let Some(n) = snapshot_logs_since_last {
        cmd.arg("--snapshot-logs-since-last").arg(n.to_string());
    }
    if spec.ports.id == 1 && spec.boot == 1 {
        // Node 1 initializes the full roster membership — exactly once per
        // data dir. A restarted node (boot >= 2) recovers membership from
        // its persisted log; calling initialize again would be rejected by
        // openraft (`InitializeError::NotAllowed`).
        cmd.arg("--init");
    }
    cmd.spawn()
        .with_context(|| format!("spawn {}", bin.display()))
}

/// Open a node log file for appending (a restarted node gets a fresh,
/// boot-stamped file instead of truncating the previous boot's log).
fn open_log_append(path: &Path) -> Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open node log {}", path.display()))
}

fn log_tail(path: &Path, lines: usize) -> String {
    match std::fs::read_to_string(path) {
        Ok(content) => content
            .lines()
            .rev()
            .take(lines)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n"),
        Err(e) => format!("<cannot read {}: {e}>", path.display()),
    }
}

fn log_contains(path: &Path, needle: &str) -> bool {
    std::fs::read_to_string(path)
        .map(|c| c.contains(needle))
        .unwrap_or(false)
}

/// Record one fault-lifecycle event; small helper so call sites stay terse.
pub fn log_fault(
    log: &mut Journal,
    seed: u64,
    event: &str,
    node: Option<u64>,
    round: Option<u32>,
    planned_offset_ms: Option<u64>,
    detail: Option<String>,
) -> Result<()> {
    log.append(&FaultEvent {
        ts_ms: now_ms(),
        event: event.to_string(),
        seed,
        node,
        round,
        planned_offset_ms,
        detail,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_layout_uses_10_stride() {
        let p1 = NodePorts::for_node(1, 30_000);
        let p2 = NodePorts::for_node(2, 30_000);
        let p3 = NodePorts::for_node(3, 30_000);
        assert_eq!(p1.grpc, 30_001);
        assert_eq!(p2.grpc, 30_011);
        assert_eq!(p3.grpc, 30_021);
        assert_eq!(p2.grpc_addr(), "127.0.0.1:30011");
    }

    #[test]
    fn free_port_probe_finds_usable_base() {
        let base = find_free_port_base().unwrap();
        // Slots must still be bindable right after the probe returns.
        let listeners: Vec<_> = (1..=3)
            .map(|id| NodePorts::for_node(id, base).grpc)
            .map(|port| std::net::TcpListener::bind(("127.0.0.1", port)).unwrap())
            .collect();
        assert_eq!(listeners.len(), 3);
    }
}
