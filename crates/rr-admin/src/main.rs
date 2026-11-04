//! `rr-admin` — Command-line tool for administering an `rr` cluster.
//!
//! 提供命令行界面，用于查询节点状态、Raft 组状态、集群 formation 生命周期状态、触发快照、转移领导者和查看指标摘要。
//! 支持人类可读的表格输出和 `--output json` 机器可读模式，以及交互式 REPL。
//!
//! ---
//!
//! Provides a CLI for querying node status, Raft group status, cluster formation
//! status, triggering snapshots, transferring leadership, and viewing metrics
//! summaries.
//! Supports human-readable table output, `--output json` machine-readable mode,
//! and an interactive REPL for kv-store operations.

use anyhow::Result;
use bincode::config::standard;
use clap::{Parser, Subcommand, ValueEnum};
use rr_network::proto::admin::admin_service_client::AdminServiceClient;
use rr_network::proto::admin::{
    online_backup_chunk::Body as OnlineBackupBody, AddLearnerRequest, ChangeMembershipRequest,
    DrainRequest, Empty, FormationStatus, GroupId, OnlineBackupRequest, ResetIngestorOffsetRequest,
    TransferLeaderRequest,
};
use rr_network::proto::client::client_service_client::ClientServiceClient;
use rr_network::proto::client::{ClientRequest, ClientResponse};
use rr_network::tls::TlsConfig;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::{Code, Response, Status};

mod backup;
mod decommission;
mod node_reset;

// ── TLS / bearer-token client plumbing ──────────────────────────────────────

/// Request interceptor attaching the bearer token when one is configured
/// (no-op otherwise, so every call site shares one client type).
#[derive(Clone)]
pub(crate) struct TokenInterceptor {
    token: Option<Arc<str>>,
}

impl Interceptor for TokenInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, Status> {
        if let Some(token) = &self.token {
            rr_network::auth::attach_bearer_token(&mut req, token)?;
        }
        Ok(req)
    }
}

/// Client channel type shared by all admin/kv call sites (intercepted,
/// token optional).
pub(crate) type GrpcChannel =
    tonic::service::interceptor::InterceptedService<Channel, TokenInterceptor>;
pub(crate) type AdminClient = AdminServiceClient<GrpcChannel>;
pub(crate) type KvClient = ClientServiceClient<GrpcChannel>;

/// Total budget for establishing a gRPC channel, TCP connect AND HTTP/2
/// handshake included — a firewall middlebox can accept the TCP connection
/// and then stall the handshake, so the whole `connect()` is wrapped, not
/// just the endpoint's TCP-level `connect_timeout`.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Establish a gRPC channel to the given host:port address — plaintext by
/// default, HTTPS + mTLS when `tls` is set.
async fn connect(addr: &str, tls: Option<&TlsConfig>) -> Result<Channel> {
    let scheme = if tls.is_some() { "https" } else { "http" };
    let endpoint = format!("{scheme}://{addr}");
    let endpoint = tonic::transport::Channel::from_shared(endpoint)?
        // Fail fast on a dead/wrong address instead of hanging the CLI on
        // the OS TCP timeout.
        .connect_timeout(CONNECT_TIMEOUT);
    let endpoint = match tls {
        Some(t) => endpoint.tls_config(
            t.build_client_tls()
                .map_err(|e| anyhow::anyhow!("TLS client config: {e}"))?,
        )?,
        None => endpoint,
    };
    let channel = match tokio::time::timeout(CONNECT_TIMEOUT, endpoint.connect()).await {
        Ok(Ok(channel)) => channel,
        Ok(Err(e)) => return Err(connect_error(addr, e)),
        Err(_) => return Err(connect_timeout_error(addr)),
    };
    Ok(channel)
}

/// Remediation checklist appended to every connection-failure message: the
/// operator gets the next steps, not just the symptom — the same
/// problem-plus-fix convention as the rr-server startup safety-gate errors.
const CONNECT_REMEDY: &str = concat!(
    "next steps: (1) verify the target process is alive and listening on the dialed address; ",
    "(2) confirm the port role — the admin port and the kv client port are different listeners ",
    "(kv-store convention: admin port = client port + 1000); ",
    "(3) check TLS (--tls-ca/--tls-cert/--tls-key) and the admin token (--token-env); ",
    "(4) see playbook › Troubleshooting (docs/docs/guide/playbook.md)"
);

/// Connect-timeout error with remediation guidance.
fn connect_timeout_error(addr: &str) -> anyhow::Error {
    anyhow::anyhow!("connect to {addr} timed out after {CONNECT_TIMEOUT:?}\n{CONNECT_REMEDY}")
}

/// Transport-level connect failure (refused/reset/TLS handshake) with
/// remediation guidance. The source chain is preserved so the concrete cause
/// still shows up as `Caused by:` under the context + remedy.
fn connect_error(addr: &str, err: impl std::error::Error + Send + Sync + 'static) -> anyhow::Error {
    anyhow::Error::new(err).context(format!("connect to {addr} failed\n{CONNECT_REMEDY}"))
}

/// One-line rendering of a `connect()` failure for per-channel status lines:
/// first message line plus the root cause, WITHOUT the remediation checklist
/// (the caller prints that once at the summary/site level).
fn connect_error_brief(err: &anyhow::Error) -> String {
    let first_line = err
        .to_string()
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    match err.chain().nth(1) {
        Some(cause) => format!("{first_line}: {cause}"),
        None => first_line,
    }
}

/// REPL member-discovery failure message with remediation guidance. Failover
/// stays limited to the dialed --addr until the admin endpoint answers.
fn member_discovery_message(err: &Status, addr: &str) -> String {
    format!(
        "member discovery failed: {err}; automatic failover is limited to {addr}\n{CONNECT_REMEDY}"
    )
}

/// REPL reconnect-failure message naming both port roles, with remediation
/// guidance.
fn reconnect_failed_message(kv_addr: &str, admin_addr: &str) -> String {
    format!(
        "reconnect failed (will retry on next command): client port {kv_addr}, admin port {admin_addr}\n{CONNECT_REMEDY}"
    )
}

fn make_admin_client(channel: Channel, token: &Option<Arc<str>>) -> AdminClient {
    AdminServiceClient::with_interceptor(
        channel,
        TokenInterceptor {
            token: token.clone(),
        },
    )
}

fn make_kv_client(channel: Channel, token: &Option<Arc<str>>) -> KvClient {
    ClientServiceClient::with_interceptor(
        channel,
        TokenInterceptor {
            token: token.clone(),
        },
    )
}

/// Validate the --tls-* trio (all-or-nothing, kv-store convention) and build
/// the client-side TLS config.
fn tls_config_from_args(cli: &Cli) -> Result<Option<TlsConfig>> {
    let any = cli.tls_ca.is_some() || cli.tls_cert.is_some() || cli.tls_key.is_some();
    if !any {
        anyhow::ensure!(
            cli.tls_domain.is_none(),
            "--tls-domain requires the --tls-ca/--tls-cert/--tls-key trio"
        );
        return Ok(None);
    }
    anyhow::ensure!(
        cli.tls_ca.is_some() && cli.tls_cert.is_some() && cli.tls_key.is_some(),
        "--tls-ca/--tls-cert/--tls-key are all-or-nothing"
    );
    Ok(Some(TlsConfig {
        cert_path: cli.tls_cert.clone().expect("ensured above"),
        key_path: cli.tls_key.clone().expect("ensured above"),
        ca_path: Some(cli.tls_ca.clone().expect("ensured above")),
        require_client_cert: false, // server-side knob, ignored on clients
        domain_name: cli.tls_domain.clone(),
    }))
}

/// Read a bearer token from the env var named by the flag; missing/empty is
/// a hard error (explicit flag ⇒ operator intent).
fn token_from_env(env_var: &Option<String>, flag: &str) -> Result<Option<Arc<str>>> {
    let Some(var) = env_var else { return Ok(None) };
    let token = std::env::var(var)
        .ok()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{flag} {var}: variable not set or empty"))?;
    Ok(Some(Arc::from(token)))
}

// ── KV wire types (mirrors kv-store's KvRequest/KvResponse) ────────────────

/// KV request type, matching the kv-store example wire format.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum KvRequest {
    Put { key: String, value: Vec<u8> },
    Get { key: String },
    Delete { key: String },
}

/// KV response type, matching the kv-store example wire format.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct KvResponse {
    value: Option<Vec<u8>>,
}

// ── CLI definition ─────────────────────────────────────────────────────────────

/// Output format for command results.
#[derive(Clone, Debug, Default, ValueEnum)]
enum OutputFormat {
    /// Human-readable table (default).
    #[default]
    Human,
    /// Machine-readable JSON.
    Json,
}

/// rr-admin: administrative CLI for an rr cluster node.
#[derive(Parser, Debug)]
#[command(name = "rr-admin", about = "Administrative CLI for an rr cluster node")]
struct Cli {
    /// gRPC address of the target node (host:port).
    #[arg(long, default_value = "127.0.0.1:52001")]
    addr: String,

    /// Output format.
    #[arg(long, value_enum, default_value = "human")]
    output: OutputFormat,

    /// CA bundle for verifying the server certificate. The
    /// --tls-ca/--tls-cert/--tls-key trio is all-or-nothing (kv-store
    /// convention); without them the client speaks plaintext (dev default).
    #[arg(long, value_name = "PATH")]
    tls_ca: Option<PathBuf>,

    /// Client certificate presented for mTLS.
    #[arg(long, value_name = "PATH")]
    tls_cert: Option<PathBuf>,

    /// Client private key for --tls-cert.
    #[arg(long, value_name = "PATH")]
    tls_key: Option<PathBuf>,

    /// SNI / expected server name override when dialing by an address that
    /// does not match the certificate SAN.
    #[arg(long, value_name = "NAME")]
    tls_domain: Option<String>,

    /// Name of the environment variable holding the ADMIN bearer token (e.g.
    /// RR_ADMIN_TOKEN). Secrets stay off argv — same convention as
    /// rr-server's `auth.admin_token_env`. Fails fast if unset or empty.
    #[arg(long, value_name = "ENV_VAR")]
    token_env: Option<String>,

    /// Name of the environment variable holding the CLIENT bearer token
    /// (e.g. RR_CLIENT_TOKEN) — used only by `repl` kv commands.
    #[arg(long, value_name = "ENV_VAR")]
    client_token_env: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Node-level operations.
    Node {
        #[command(subcommand)]
        cmd: NodeCommands,
    },
    /// Raft group operations.
    Group {
        #[command(subcommand)]
        cmd: GroupCommands,
    },
    /// Configuration inspection commands.
    Config {
        #[command(subcommand)]
        cmd: ConfigCommands,
    },
    /// Cluster formation lifecycle inspection commands.
    Formation {
        #[command(subcommand)]
        cmd: FormationCommands,
    },
    /// Show top-level metrics summary.
    Metrics,
    /// Launch an interactive REPL for kv-store operations.
    Repl {
        /// gRPC address of the node's ClientService — the kv data port
        /// (host:port, e.g. 51001 for kv-store's default).
        #[arg(long, default_value = "127.0.0.1:51001")]
        addr: String,
        /// gRPC address of the node's AdminService — used by the `status`
        /// and `members` commands. Defaults to --addr's host with
        /// port + 1000 (the kv-store admin-port convention, e.g. 51001 →
        /// 52001). Pass explicitly when the node uses a different scheme.
        #[arg(long)]
        admin_addr: Option<String>,
        /// Raft group / partition ID to route kv commands to.
        #[arg(long, default_value = "1")]
        group: u64,
    },
    /// Create an offline backup of a stopped node's data_dir.
    ///
    /// The target node MUST be stopped. See the `backup` module docs for
    /// the rolling-backup procedure.
    Backup {
        /// Path to the node's data_dir (contains RocksDB files).
        #[arg(long)]
        data_dir: PathBuf,
        /// Output `.tar.gz` file path.
        #[arg(long)]
        output: PathBuf,
        /// Optional: node_id to record in backup metadata (for audit).
        #[arg(long)]
        node_id: Option<u64>,
        /// Optional free-form comment stored in metadata.
        #[arg(long)]
        comment: Option<String>,
    },
    /// Restore a backup tarball into an empty data_dir.
    ///
    /// The target `data_dir` MUST be empty or absent. The node MUST be
    /// stopped before restore and started fresh after.
    Restore {
        /// Path to the backup `.tar.gz` file.
        #[arg(long)]
        input: PathBuf,
        /// Path to the target data_dir (must be empty or absent).
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Show metadata of a backup tarball without unpacking it.
    InspectBackup {
        /// Path to the backup `.tar.gz` file.
        #[arg(long)]
        input: PathBuf,
    },
    /// Reset a stopped node's `data_dir` so it can be re-added as a
    /// fresh learner.
    ///
    /// **DESTRUCTIVE** — wipes all files in `--data-dir`. Use only when:
    /// (a) the node is stopped, AND (b) you've confirmed the path is
    /// correct. Pre-flight checks reject non-rr directories, paths
    /// holding a live RocksDB lock (Unix lock probe; best-effort
    /// elsewhere), and non-existent paths. `--force` is REQUIRED to
    /// wipe — it IS the operator confirmation; there is no interactive
    /// prompt.
    ///
    /// After reset: start the node and have another cluster member
    /// re-add it via `rr-admin group add-learner --node-id <ID>
    /// --addr <HOST:PORT>` then promote via `rr-admin group
    /// change-membership` once it has caught up.
    NodeReset {
        /// Path to the node's `data_dir` (contains RocksDB files).
        #[arg(long)]
        data_dir: PathBuf,
        /// Skip the safety prompt. Required to actually wipe.
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
enum NodeCommands {
    /// Get node status (node_id, listen_addr, groups, uptime).
    Status,
    /// Drain + remove a node from every Raft group it participates
    /// in. The orchestrator transfers leadership away if needed,
    /// then calls `change_membership` to remove the target. Refuses
    /// to leave any group with fewer than 2 voters.
    Decommission {
        /// Node ID to decommission.
        #[arg(long = "node-id")]
        node_id: u64,
        /// Print the planned changes without making them.
        #[arg(long)]
        dry_run: bool,
    },
    /// Drain local leadership away from this node before shutdown.
    Drain {
        /// Node ID to drain (must be this local node on the target admin endpoint).
        #[arg(long = "node-id")]
        node_id: u64,
        /// Max wait time for each leadership transfer.
        #[arg(long, default_value = "30")]
        timeout_secs: u32,
    },
}

#[derive(Subcommand, Debug)]
enum GroupCommands {
    /// List all local Raft groups with basic status.
    List,
    /// Get Raft group status.
    Status {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
    },
    /// Show snapshot status for a Raft group.
    SnapshotStatus {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
    },
    /// Trigger a manual snapshot / log compaction for a group.
    Snapshot {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
    },
    /// Transfer group leadership to another node.
    TransferLeader {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
        /// Target node ID to transfer leadership to.
        #[arg(long)]
        to: u64,
    },
    /// Add a learner node to a Raft group.
    ///
    /// Learner replicates logs but does not vote. Run
    /// `change-membership` afterwards to promote it to voter.
    AddLearner {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
        /// Node ID of the learner to add.
        #[arg(long = "node-id")]
        node_id: u64,
        /// gRPC address of the learner (`host:port`).
        #[arg(long)]
        addr: String,
    },
    /// Replace the voter set of a Raft group.
    ///
    /// Members not listed in `--voters` are kept as learners
    /// (`retain = true`). Must be called on the current leader.
    /// The server rejects a target voter set smaller than 2 with
    /// `failed_precondition` unless `--force` is given.
    ChangeMembership {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
        /// Comma-separated list of voter node IDs (e.g. `1,2,3`).
        #[arg(long)]
        voters: String,
        /// Override the server-side quorum guard that rejects a target
        /// voter set smaller than 2. Reserved for disaster recovery
        /// (e.g. rebuilding after permanent loss of a majority); every
        /// forced change is recorded in the server's audit log.
        #[arg(long)]
        force: bool,
    },
    /// Override persisted ingestor offset for disaster recovery.
    ///
    /// Use `--offset -1` to clear/unset the persisted offset.
    ResetIngestorOffset {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
        /// New ingestor offset to persist. `-1` means clear/unset.
        #[arg(long, allow_hyphen_values = true)]
        offset: i64,
    },
    /// Show the current voter + learner membership of a Raft group.
    Membership {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
    },
    /// Stream a non-disruptive snapshot of a Raft group to a local
    /// file. Unlike `rr-admin backup` (offline), this works against a
    /// running node by fetching the latest committed snapshot.
    OnlineBackup {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
        /// Path to write the snapshot bytes to (binary file).
        #[arg(long)]
        output: PathBuf,
        /// Trigger a fresh snapshot before streaming. Use when the
        /// group has never produced a snapshot yet.
        #[arg(long)]
        force_fresh: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigCommands {
    /// Show current routing table snapshot.
    Show,
}

#[derive(Subcommand, Debug)]
enum FormationCommands {
    /// Show cluster formation lifecycle status: origin, aggregate state,
    /// cluster identity, lifecycle-store revisions, and per-group
    /// formation state (Forming/Active). `Untracked` means the cluster
    /// predates formation tracking — see playbook › Troubleshooting ›
    /// Formation for the adoption path.
    Status,
}

// ── Entry point ────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // TLS/token are resolved once, up front: a bad flag fails fast before any
    // subcommand runs (file-only subcommands like backup ignore them).
    let tls = tls_config_from_args(&cli)?;
    let admin_token = token_from_env(&cli.token_env, "--token-env")?;
    let client_token = token_from_env(&cli.client_token_env, "--client-token-env")?;

    match &cli.command {
        Commands::Repl {
            addr,
            admin_addr,
            group,
        } => {
            run_repl(
                addr,
                admin_addr.clone(),
                *group,
                tls.as_ref(),
                &admin_token,
                &client_token,
            )
            .await?;
        }
        Commands::Backup {
            data_dir,
            output,
            node_id,
            comment,
        } => {
            let meta = backup::create_backup(data_dir, output, *node_id, comment.clone())?;
            match cli.output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&meta)?);
                }
                OutputFormat::Human => {
                    println!("Backup created: {}", output.display());
                    println!("  rr_version     : {}", meta.rr_version);
                    println!("  format_version : {}", meta.format_version);
                    println!("  source_node_id : {:?}", meta.source_node_id);
                    println!("  taken_at       : {}", meta.taken_at);
                    if let Some(c) = &meta.comment {
                        println!("  comment        : {c}");
                    }
                }
            }
        }
        Commands::Restore { input, data_dir } => {
            let meta = backup::restore_backup(input, data_dir)?;
            match cli.output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&meta)?);
                }
                OutputFormat::Human => {
                    println!("Restored to: {}", data_dir.display());
                    println!("  rr_version     : {}", meta.rr_version);
                    println!("  format_version : {}", meta.format_version);
                    println!("  source_node_id : {:?}", meta.source_node_id);
                    println!("  taken_at       : {}", meta.taken_at);
                }
            }
        }
        Commands::InspectBackup { input } => {
            let meta = backup::read_metadata(input)?;
            match cli.output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&meta)?);
                }
                OutputFormat::Human => {
                    println!("Backup Metadata: {}", input.display());
                    println!("  rr_version     : {}", meta.rr_version);
                    println!("  format_version : {}", meta.format_version);
                    println!("  source_node_id : {:?}", meta.source_node_id);
                    println!("  taken_at       : {}", meta.taken_at);
                    if let Some(c) = &meta.comment {
                        println!("  comment        : {c}");
                    }
                }
            }
        }
        Commands::NodeReset { data_dir, force } => {
            let report = node_reset::reset_node_data_dir(data_dir, *force)?;
            match cli.output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                }
                OutputFormat::Human => {
                    println!("Node Reset OK");
                    println!("  data_dir         : {}", report.data_dir);
                    println!("  files_removed    : {}", report.files_removed);
                    println!("  dirs_removed     : {}", report.dirs_removed);
                    println!("  bytes_reclaimed  : {}", report.bytes_reclaimed);
                    println!();
                    println!("Next steps:");
                    println!("  1. Start the node fresh.");
                    println!("  2. From another cluster member, run:");
                    println!("       rr-admin --addr <leader-host:port> group add-learner \\");
                    println!(
                        "           --group <ID> --node-id <this-node-id> --addr <this-host:port>"
                    );
                    println!("  3. Wait for replication to catch up, then promote with:");
                    println!(
                        "       rr-admin --addr <leader-host:port> group change-membership \\"
                    );
                    println!("           --group <ID> --voters <new-voter-set>");
                }
            }
        }
        _ => {
            let channel = connect(&cli.addr, tls.as_ref()).await?;
            let mut client = make_admin_client(channel, &admin_token);

            match &cli.command {
                Commands::Node {
                    cmd: NodeCommands::Decommission { node_id, dry_run },
                } => {
                    let tls_dial = tls.clone();
                    let token_dial = admin_token.clone();
                    let report = decommission::decommission_node(
                        &mut client,
                        move |addr: &str| {
                            let tls = tls_dial.clone();
                            let token = token_dial.clone();
                            let addr = addr.to_owned();
                            async move {
                                let channel = connect(&addr, tls.as_ref()).await?;
                                Ok(make_admin_client(channel, &token))
                            }
                        },
                        *node_id,
                        *dry_run,
                    )
                    .await?;
                    match cli.output {
                        OutputFormat::Json => {
                            println!("{}", serde_json::to_string_pretty(&report)?);
                        }
                        OutputFormat::Human => {
                            println!(
                                "Decommission Report (target_node_id={}, dry_run={})",
                                report.target_node_id, report.dry_run
                            );
                            for caveat in &report.caveats {
                                println!("WARNING: {caveat}");
                            }
                            for g in &report.groups {
                                let action = serde_json::to_value(&g.action)
                                    .ok()
                                    .and_then(|v| v.as_str().map(str::to_owned))
                                    .unwrap_or_else(|| format!("{:?}", g.action));
                                println!("  group {}: {} — {}", g.group_id, action, g.message);
                            }
                            if report.has_failures() {
                                println!();
                                println!(
                                    "DECOMMISSION INCOMPLETE/BLOCKED — investigate and re-run; \
                                     RemoveGroupMember is idempotent so partial progress is safe"
                                );
                            } else if report.made_progress() {
                                println!();
                                if report.dry_run {
                                    println!("(dry-run) re-run without --dry-run to apply.");
                                } else {
                                    println!(
                                        "All reported membership removals completed successfully."
                                    );
                                }
                            } else {
                                println!();
                                println!(
                                    "No-op: target was absent from voters and learners in all reported groups."
                                );
                            }
                        }
                    }
                    // Loud failure semantics: failed, quorum-blocked, or
                    // incompletely covered decommissions must not exit 0
                    // (CI/CD wrappers key on the exit code).
                    if report.has_failures() {
                        std::process::exit(1);
                    }
                }
                Commands::Node {
                    cmd:
                        NodeCommands::Drain {
                            node_id,
                            timeout_secs,
                        },
                } => {
                    let resp = client
                        .drain_node(DrainRequest {
                            node_id: *node_id,
                            timeout_secs: *timeout_secs,
                        })
                        .await?
                        .into_inner();
                    let mut groups = resp.groups;
                    groups.sort_unstable_by_key(|g| g.group_id);
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "node_id": node_id,
                                "timeout_secs": timeout_secs,
                                "groups": groups.iter().map(|g| serde_json::json!({
                                    "group_id": g.group_id,
                                    "leadership_transferred": g.leadership_transferred,
                                    "message": g.message,
                                })).collect::<Vec<_>>(),
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("Node Drain (node_id={node_id}, timeout_secs={timeout_secs})");
                            if groups.is_empty() {
                                println!("  (no groups drained; likely remote-node no-op)");
                            } else {
                                println!("  {:<10} {:<11} message", "group_id", "transferred");
                                for g in &groups {
                                    println!(
                                        "  {:<10} {:<11} {}",
                                        g.group_id, g.leadership_transferred, g.message
                                    );
                                }
                            }
                        }
                    }
                    // Loud failure semantics, same as decommission above: a
                    // drain that left any group's leadership unmoved must not
                    // exit 0 (CI/CD wrappers key on the exit code). A remote
                    // no-op (empty group list) stays exit 0.
                    if groups.iter().any(|g| !g.leadership_transferred) {
                        std::process::exit(1);
                    }
                }
                Commands::Node {
                    cmd: NodeCommands::Status,
                } => {
                    let resp = client.get_node_status(Empty {}).await?.into_inner();
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "node_id": resp.node_id,
                                "listen_addr": resp.listen_addr,
                                "group_ids": resp.group_ids,
                                "uptime_secs": resp.uptime_secs,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("Node Status");
                            println!("  node_id    : {}", resp.node_id);
                            println!("  listen_addr: {}", resp.listen_addr);
                            println!("  group_ids  : {:?}", resp.group_ids);
                            println!("  uptime_secs: {}", resp.uptime_secs);
                        }
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::List,
                } => {
                    let mut groups = client.list_groups(Empty {}).await?.into_inner().groups;
                    groups.sort_unstable_by_key(|g| g.group_id);
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "groups": groups.iter().map(|g| serde_json::json!({
                                    "group_id": g.group_id,
                                    "is_leader": g.is_leader,
                                    "current_term": g.current_term,
                                    "last_log_index": g.last_log_index,
                                    "leader_node_id": g.leader_node_id,
                                })).collect::<Vec<_>>(),
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!(
                                "{:<10} {:<10} {:<8} {:<14} leader",
                                "group_id", "is_leader", "term", "last_log_index"
                            );
                            for g in &groups {
                                let leader = g
                                    .leader_node_id
                                    .map(|id| id.to_string())
                                    .unwrap_or_else(|| "none".to_string());
                                println!(
                                    "{:<10} {:<10} {:<8} {:<14} {}",
                                    g.group_id,
                                    g.is_leader,
                                    g.current_term,
                                    g.last_log_index,
                                    leader
                                );
                            }
                        }
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::Status { group },
                } => {
                    let resp = client
                        .get_group_status(GroupId { group_id: *group })
                        .await?
                        .into_inner();
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "group_id": resp.group_id,
                                "is_leader": resp.is_leader,
                                "current_term": resp.current_term,
                                "last_log_index": resp.last_log_index,
                                "leader_node_id": resp.leader_node_id,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("Group Status (group {})", resp.group_id);
                            println!("  is_leader     : {}", resp.is_leader);
                            println!("  current_term  : {}", resp.current_term);
                            println!("  last_log_index: {}", resp.last_log_index);
                            println!("  leader_node_id: {}", resp.leader_node_id);
                        }
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::SnapshotStatus { group },
                } => {
                    let resp = client
                        .get_snapshot_status(GroupId { group_id: *group })
                        .await?
                        .into_inner();
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "group_id": resp.group_id,
                                "snapshot_in_progress": resp.snapshot_in_progress,
                                "last_snapshot_index": resp.last_snapshot_index,
                                "last_snapshot_term": resp.last_snapshot_term,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("Snapshot Status (group {})", resp.group_id);
                            println!("  snapshot_in_progress: {}", resp.snapshot_in_progress);
                            println!(
                                "  last_snapshot_index : {}",
                                resp.last_snapshot_index
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "none".to_string())
                            );
                            println!(
                                "  last_snapshot_term  : {}",
                                resp.last_snapshot_term
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "none".to_string())
                            );
                        }
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::Snapshot { group },
                } => {
                    let (success, message) = admin_action_outcome(
                        client.trigger_snapshot(GroupId { group_id: *group }).await,
                        "snapshot triggered",
                    );
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "success": success,
                                "message": message,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("TriggerSnapshot: success={success} message={message}");
                        }
                    }
                    if !success {
                        std::process::exit(1);
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::TransferLeader { group, to },
                } => {
                    let (success, message) = admin_action_outcome(
                        client
                            .transfer_leader(TransferLeaderRequest {
                                group_id: *group,
                                target_node_id: *to,
                            })
                            .await,
                        "election triggered on this node",
                    );
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "success": success,
                                "message": message,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("TransferLeader: success={success} message={message}");
                        }
                    }
                    if !success {
                        std::process::exit(1);
                    }
                }
                Commands::Group {
                    cmd:
                        GroupCommands::AddLearner {
                            group,
                            node_id,
                            addr,
                        },
                } => {
                    let (success, message) = admin_action_outcome(
                        client
                            .add_learner(AddLearnerRequest {
                                group_id: *group,
                                node_id: *node_id,
                                addr: addr.clone(),
                            })
                            .await,
                        "learner added",
                    );
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "success": success,
                                "message": message,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("AddLearner: success={success} message={message}");
                        }
                    }
                    if !success {
                        std::process::exit(1);
                    }
                }
                Commands::Group {
                    cmd:
                        GroupCommands::ChangeMembership {
                            group,
                            voters,
                            force,
                        },
                } => {
                    let voter_node_ids: Vec<u64> = voters
                        .split(',')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.parse::<u64>())
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| anyhow::anyhow!("invalid --voters: {e}"))?;
                    if voter_node_ids.is_empty() {
                        anyhow::bail!("--voters must list at least one node id");
                    }
                    let (success, message) = admin_action_outcome(
                        client
                            .change_membership(ChangeMembershipRequest {
                                group_id: *group,
                                voter_node_ids,
                                force: *force,
                            })
                            .await,
                        "membership changed",
                    );
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "success": success,
                                "message": message,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("ChangeMembership: success={success} message={message}");
                        }
                    }
                    if !success {
                        std::process::exit(1);
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::ResetIngestorOffset { group, offset },
                } => {
                    let (success, message) = admin_action_outcome(
                        client
                            .reset_ingestor_offset(ResetIngestorOffsetRequest {
                                group_id: *group,
                                offset: *offset,
                            })
                            .await,
                        "ingestor offset reset",
                    );
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "success": success,
                                "message": message,
                                "group_id": group,
                                "offset": offset,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!(
                                "ResetIngestorOffset: success={success} group={group} offset={offset} message={message}",
                            );
                        }
                    }
                    if !success {
                        std::process::exit(1);
                    }
                }
                Commands::Group {
                    cmd:
                        GroupCommands::OnlineBackup {
                            group,
                            output,
                            force_fresh,
                        },
                } => {
                    use std::io::Write;
                    let mut stream = client
                        .online_backup(OnlineBackupRequest {
                            group_id: *group,
                            force_fresh_snapshot: *force_fresh,
                        })
                        .await?
                        .into_inner();
                    let mut file = std::fs::File::create(output)?;
                    let mut received_bytes: u64 = 0;
                    let mut metadata_seen: Option<rr_network::proto::admin::OnlineBackupMetadata> =
                        None;
                    use futures::StreamExt;
                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk?;
                        match chunk.body {
                            Some(OnlineBackupBody::Metadata(m)) => {
                                metadata_seen = Some(m);
                            }
                            Some(OnlineBackupBody::Data(bytes)) => {
                                file.write_all(&bytes)?;
                                received_bytes += bytes.len() as u64;
                            }
                            None => {}
                        }
                    }
                    file.sync_all()?;
                    drop(file);
                    let meta = metadata_seen.ok_or_else(|| {
                        anyhow::anyhow!("server stream ended without metadata chunk")
                    })?;
                    if received_bytes != meta.size_bytes {
                        anyhow::bail!(
                            "byte-count mismatch: header said {} bytes, received {}",
                            meta.size_bytes,
                            received_bytes
                        );
                    }
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "group_id": meta.group_id,
                                "last_log_index": meta.last_log_index,
                                "last_log_term": meta.last_log_term,
                                "size_bytes": meta.size_bytes,
                                "rr_version": meta.rr_version,
                                "output": output.display().to_string(),
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("Online Backup OK");
                            println!("  output         : {}", output.display());
                            println!("  group_id       : {}", meta.group_id);
                            println!("  last_log_index : {}", meta.last_log_index);
                            println!("  last_log_term  : {}", meta.last_log_term);
                            println!("  size_bytes     : {}", meta.size_bytes);
                            println!("  rr_version     : {}", meta.rr_version);
                        }
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::Membership { group },
                } => {
                    let resp = client
                        .get_group_membership(GroupId { group_id: *group })
                        .await?
                        .into_inner();
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "group_id": resp.group_id,
                                "voters": resp.voters.iter().map(|m| serde_json::json!({
                                    "node_id": m.node_id,
                                    "addr": m.addr,
                                })).collect::<Vec<_>>(),
                                "learners": resp.learners.iter().map(|m| serde_json::json!({
                                    "node_id": m.node_id,
                                    "addr": m.addr,
                                })).collect::<Vec<_>>(),
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("Group {} Membership", resp.group_id);
                            println!("  Voters:");
                            for m in &resp.voters {
                                println!("    {:>4}  {}", m.node_id, m.addr);
                            }
                            println!("  Learners:");
                            if resp.learners.is_empty() {
                                println!("    (none)");
                            } else {
                                for m in &resp.learners {
                                    println!("    {:>4}  {}", m.node_id, m.addr);
                                }
                            }
                        }
                    }
                }
                Commands::Metrics => {
                    let resp = client.get_metrics_summary(Empty {}).await?.into_inner();
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "total_proposals": resp.total_proposals,
                                "total_applied": resp.total_applied,
                                "total_snapshots": resp.total_snapshots,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("Metrics Summary");
                            println!("  total_proposals: {}", resp.total_proposals);
                            println!("  total_applied  : {}", resp.total_applied);
                            println!("  total_snapshots: {}", resp.total_snapshots);
                        }
                    }
                }
                Commands::Config {
                    cmd: ConfigCommands::Show,
                } => {
                    let mut partitions = client
                        .get_routing_table(Empty {})
                        .await?
                        .into_inner()
                        .partitions;
                    partitions.sort_unstable_by_key(|p| p.partition_id);
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "partitions": partitions.iter().map(|p| serde_json::json!({
                                    "partition_id": p.partition_id,
                                    "group_id": p.group_id,
                                    "replica_node_ids": p.replica_node_ids,
                                })).collect::<Vec<_>>(),
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("{:<12} {:<10} replicas", "partition_id", "group_id");
                            for p in &partitions {
                                let replicas = if p.replica_node_ids.is_empty() {
                                    "(none)".to_string()
                                } else {
                                    p.replica_node_ids
                                        .iter()
                                        .map(std::string::ToString::to_string)
                                        .collect::<Vec<_>>()
                                        .join(",")
                                };
                                println!("{:<12} {:<10} {}", p.partition_id, p.group_id, replicas);
                            }
                        }
                    }
                }
                Commands::Formation {
                    cmd: FormationCommands::Status,
                } => {
                    let mut resp = client.get_formation_status(Empty {}).await?.into_inner();
                    resp.groups.sort_unstable_by_key(|g| g.group_id);
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "origin": resp.origin,
                                "cluster_uuid": resp.cluster_uuid,
                                "topology_revision": resp.topology_revision,
                                "store_revision": resp.store_revision,
                                "state": resp.state,
                                "groups": resp.groups.iter().map(|g| serde_json::json!({
                                    "group_id": g.group_id,
                                    "bootstrap_epoch": g.bootstrap_epoch,
                                    "state": g.state,
                                })).collect::<Vec<_>>(),
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("{}", format_formation_status(&resp));
                        }
                    }
                }
                Commands::Repl { .. }
                | Commands::Backup { .. }
                | Commands::Restore { .. }
                | Commands::InspectBackup { .. }
                | Commands::NodeReset { .. } => unreachable!("handled above"),
            }
        }
    }

    Ok(())
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Render formation lifecycle status as human-readable text — shared by the
/// `formation status` subcommand and the REPL `formation status` command.
/// An untracked cluster (a legacy cluster predating formation tracking) gets
/// a playbook pointer instead of an empty group table.
fn format_formation_status(resp: &FormationStatus) -> String {
    let uuid = if resp.cluster_uuid.is_empty() {
        "(none)"
    } else {
        resp.cluster_uuid.as_str()
    };
    let mut out = format!(
        "Formation Status\n  {:<17}: {}\n  {:<17}: {}\n  {:<17}: {}\n  {:<17}: {}\n  {:<17}: {}",
        "origin",
        resp.origin,
        "state",
        resp.state,
        "cluster_uuid",
        uuid,
        "topology_revision",
        resp.topology_revision,
        "store_revision",
        resp.store_revision,
    );
    if resp.origin == "Untracked" || resp.state == "Untracked" {
        out.push_str(
            "\ncluster is not tracked by formation lifecycle (legacy cluster); \
             see playbook › Troubleshooting › Formation",
        );
    } else {
        out.push_str(&format!(
            "\n  {:<10} {:<15} state",
            "group_id", "bootstrap_epoch"
        ));
        for g in &resp.groups {
            out.push_str(&format!(
                "\n  {:<10} {:<15} {}",
                g.group_id, g.bootstrap_epoch, g.state
            ));
        }
    }
    out
}

fn admin_action_outcome<T>(
    result: Result<Response<T>, Status>,
    ok_message: &str,
) -> (bool, String) {
    match result {
        Ok(_) => (true, ok_message.to_string()),
        Err(status) => {
            let code = status.code();
            let detail = status.message();
            let message = match code {
                Code::NotFound => format!("not found [{code}]: {detail}"),
                Code::InvalidArgument => format!("invalid argument [{code}]: {detail}"),
                Code::FailedPrecondition => {
                    format!("failed precondition [{code}]: {detail}")
                }
                Code::AlreadyExists => format!("already exists [{code}]: {detail}"),
                Code::Internal => format!("internal error [{code}]: {detail}"),
                _ => format!("rpc error [{code}]: {detail}"),
            };
            (false, message)
        }
    }
}

// ── REPL command model ─────────────────────────────────────────────────────────

/// Parsed REPL command returned by [`parse_repl_line`].
///
/// All string fields are owned so the command can be matched and dispatched
/// without borrowing the original input line.
#[derive(Debug, PartialEq)]
enum ReplCommand {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    Status,
    Members,
    /// `formation status` — cluster formation lifecycle status (admin RPC).
    FormationStatus,
    Help,
    /// User typed "reconnect" explicitly.
    Reconnect,
    /// A known command that requires arguments, but arguments were absent.
    MissingArg {
        cmd: &'static str,
        usage: &'static str,
    },
    /// A command that is not recognised.
    Unknown {
        cmd: String,
    },
    /// Empty or whitespace-only input.
    Empty,
}

/// Parse a single REPL input line into a [`ReplCommand`].
///
/// This function is pure (no I/O) and therefore fully unit-testable without a
/// running node or a rustyline instance.
fn parse_repl_line(line: &str) -> ReplCommand {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return ReplCommand::Empty;
    }
    // splitn(3) so that "put key value with spaces" treats everything after the
    // second space as the value, matching the original behaviour.
    let tokens: Vec<&str> = trimmed.splitn(3, ' ').collect();
    match tokens[0] {
        "get" => {
            if tokens.len() < 2 {
                ReplCommand::MissingArg {
                    cmd: "get",
                    usage: "get <key>",
                }
            } else {
                ReplCommand::Get {
                    key: tokens[1].to_string(),
                }
            }
        }
        "put" => {
            if tokens.len() < 3 {
                ReplCommand::MissingArg {
                    cmd: "put",
                    usage: "put <key> <value>",
                }
            } else {
                ReplCommand::Put {
                    key: tokens[1].to_string(),
                    value: tokens[2].to_string(),
                }
            }
        }
        "delete" => {
            if tokens.len() < 2 {
                ReplCommand::MissingArg {
                    cmd: "delete",
                    usage: "delete <key>",
                }
            } else {
                ReplCommand::Delete {
                    key: tokens[1].to_string(),
                }
            }
        }
        "status" => ReplCommand::Status,
        "members" => ReplCommand::Members,
        "formation" => {
            if tokens.len() < 2 {
                ReplCommand::MissingArg {
                    cmd: "formation",
                    usage: "formation status",
                }
            } else if tokens[1] == "status" {
                ReplCommand::FormationStatus
            } else {
                ReplCommand::Unknown {
                    cmd: tokens[0].to_string(),
                }
            }
        }
        "help" => ReplCommand::Help,
        "reconnect" => ReplCommand::Reconnect,
        other => ReplCommand::Unknown {
            cmd: other.to_string(),
        },
    }
}

/// Decode a serialised [`KvResponse`] payload (bincode) into an optional
/// display string.
///
/// Returns `Ok(None)` when the key was not present, `Ok(Some(s))` when a
/// value was found, or an error if the bytes cannot be decoded.
fn decode_kv_response(bytes: &[u8]) -> anyhow::Result<Option<String>> {
    let (resp, _): (KvResponse, _) = bincode::serde::decode_from_slice(bytes, standard())?;
    Ok(resp.value.map(|v| String::from_utf8_lossy(&v).to_string()))
}

// ── Interactive REPL ───────────────────────────────────────────────────────────

/// Derive the default AdminService address from the client address: same
/// host, port + 1000 (the kv-store admin-port convention, e.g. a node
/// serving ClientService on 51001 serves AdminService on 52001).
fn default_admin_addr(addr: &str) -> Result<String> {
    let (host, port) = addr.rsplit_once(':').ok_or_else(|| {
        anyhow::anyhow!("--addr '{addr}' is not host:port; pass --admin-addr explicitly")
    })?;
    let port: u16 = port.parse().map_err(|_| {
        anyhow::anyhow!("--addr '{addr}' has no numeric port; pass --admin-addr explicitly")
    })?;
    let admin_port = port.checked_add(1000).ok_or_else(|| {
        anyhow::anyhow!("--addr port {port} + 1000 overflows u16; pass --admin-addr explicitly")
    })?;
    Ok(format!("{host}:{admin_port}"))
}

/// Decide whether the admin channel should follow a kv leader redirect.
/// An explicit `--admin-addr` always wins (returns `None`); otherwise the
/// admin endpoint is re-derived from the new leader's client address
/// (host, port + 1000 — same rule as startup).
fn admin_follows_redirect(admin_explicit: bool, leader_addr: &str) -> Option<Result<String>> {
    if admin_explicit {
        None
    } else {
        Some(default_admin_addr(leader_addr))
    }
}

/// Submit one kv request, following leader redirects and trying known peers
/// when the current transport fails or a follower has no leader hint.
async fn kv_call(
    kv_client: &mut KvClient,
    kv_addr: &mut String,
    peers: &[String],
    group: u64,
    payload: &[u8],
    tls: Option<&TlsConfig>,
    client_token: &Option<Arc<str>>,
) -> std::result::Result<ClientResponse, Status> {
    let mut pending: std::collections::VecDeque<String> = peers
        .iter()
        .filter(|addr| *addr != kv_addr)
        .cloned()
        .collect();
    let mut last_response = None;
    let mut last_error = None;

    for _ in 0..peers.len() + 2 {
        match kv_client
            .request(ClientRequest {
                partition_id: group,
                payload: payload.to_vec(),
                idem_key: None,
            })
            .await
        {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success || resp.idem_error.is_some() || resp.ns_limit_error.is_some() {
                    return Ok(resp);
                }
                if let Some(info) = resp
                    .leader_info
                    .as_ref()
                    .filter(|info| !info.addr.is_empty())
                {
                    pending.retain(|addr| addr != &info.addr);
                    pending.push_front(info.addr.clone());
                }
                last_response = Some(resp);
                last_error = None;
            }
            Err(e) => {
                if e.code() != Code::Unavailable {
                    return Err(e);
                }
                last_response = None;
                last_error = Some(e);
            }
        }

        let mut connected = false;
        while let Some(addr) = pending.pop_front() {
            match connect(&addr, tls).await {
                Ok(channel) => {
                    *kv_client = make_kv_client(channel, client_token);
                    println!("reconnected to {addr}");
                    *kv_addr = addr;
                    connected = true;
                    break;
                }
                Err(e) => {
                    last_response = None;
                    // The connect() error already names the address and
                    // carries the remediation checklist — pass its message
                    // through verbatim instead of adding a second
                    // "connect ... failed" prefix around it.
                    last_error = Some(Status::unavailable(e.to_string()));
                }
            }
        }
        if !connected {
            break;
        }
    }

    if let Some(e) = last_error {
        Err(e)
    } else if let Some(resp) = last_response {
        Ok(resp)
    } else {
        Err(Status::unavailable("no reachable cluster peers"))
    }
}

/// After a kv leader redirect moved `kv_addr`, move the admin channel along
/// so `status`/`members` keep describing the node the kv commands are
/// talking to. An explicit `--admin-addr` stays pinned. `status` is
/// node-level, so the switch is announced with that caveat.
async fn follow_admin_after_redirect(
    kv_addr_before: &str,
    kv_addr: &str,
    admin_explicit: bool,
    admin_addr: &mut String,
    admin_client: &mut AdminClient,
    tls: Option<&TlsConfig>,
    admin_token: &Option<Arc<str>>,
) {
    if kv_addr == kv_addr_before {
        return;
    }
    let Some(new_admin) = admin_follows_redirect(admin_explicit, kv_addr) else {
        return;
    };
    match new_admin {
        Err(e) => println!("admin channel could not derive an endpoint from {kv_addr}: {e}"),
        Ok(new_admin) => match connect(&new_admin, tls).await {
            Ok(ch) => {
                *admin_client = make_admin_client(ch, admin_token);
                *admin_addr = new_admin;
                println!(
                    "admin channel switched to {admin_addr} — note: 'status' is node-level \
                     and now describes the leader node"
                );
            }
            Err(e) => println!(
                "admin channel could not follow the leader redirect ({new_admin}): {} — \
                 status/members still target {admin_addr}\n{CONNECT_REMEDY}",
                connect_error_brief(&e)
            ),
        },
    }
}

/// Render a loud, actionable message for a `success=false` kv response
/// (the request was NOT applied — never let it look like an OK).
fn kv_failure_detail(resp: &ClientResponse) -> String {
    if let Some(idem) = &resp.idem_error {
        return format!(
            "request rejected: idempotency key expired (request_id={}, watermark={}) — \
             NOT applied; retry with a fresh request_id above the watermark",
            idem.request_id, idem.watermark
        );
    }
    if let Some(ns) = &resp.ns_limit_error {
        return format!(
            "request rejected: idempotency namespace limit exceeded (request_type={:?}, \
             {} namespaces tracked, max {}) — NOT applied; a fresh request_id does NOT help, \
             resubmit under an already-tracked request_type",
            ns.request_type, ns.namespaces, ns.max
        );
    }
    if let Some(info) = &resp.leader_info {
        return format!(
            "request rejected by a non-leader (current leader: node {} at {}) — NOT applied",
            info.node_id, info.addr
        );
    }
    "request rejected (success=false, no leader hint) — NOT applied".to_string()
}

/// Report an RPC error. gRPC `Unimplemented` means the dialed address does
/// not serve this service at all (wrong port — client vs admin listener),
/// which is a configuration error, NOT a transient one: no reconnect.
fn handle_rpc_error(e: &Status, service: &str, dialed: &str, needs_reconnect: &mut bool) {
    if e.code() == Code::Unimplemented {
        println!("RPC error: {e}");
        println!(
            "hint: {dialed} does not implement {service} — check --addr (kv client port) \
             vs --admin-addr (admin port; default: client port + 1000)"
        );
    } else {
        *needs_reconnect = true;
        println!("RPC error: {e}");
    }
}

/// Run the interactive REPL.
///
/// 支持 8 个命令：get、put、delete、status、members、formation status、help、reconnect。
/// get/put/delete 使用 kv-store 的 bincode wire 格式，发往 ClientService
/// 端口（--addr）；status/members/formation status 发往 AdminService 端口
/// （--admin-addr，默认为客户端端口 + 1000）。
///
/// ---
///
/// Supports 8 commands: get, put, delete, status, members, formation status,
/// help, reconnect.
/// get/put/delete use the kv-store bincode wire format and go to the
/// ClientService port (`--addr`); status/members/formation status go to the
/// AdminService port (`--admin-addr`, default: client port + 1000).
async fn run_repl(
    addr: &str,
    admin_addr: Option<String>,
    group: u64,
    tls: Option<&TlsConfig>,
    admin_token: &Option<Arc<str>>,
    client_token: &Option<Arc<str>>,
) -> Result<()> {
    // Admin and client APIs are separate listeners on the node, so the REPL
    // keeps one channel per service.
    //
    // An explicit `--admin-addr` is pinned for the session; a derived one
    // follows kv leader redirects so `status`/`members` keep describing the
    // node the kv commands are talking to.
    let admin_explicit = admin_addr.is_some();
    let mut admin_addr = match admin_addr {
        Some(a) => a,
        None => default_admin_addr(addr)?,
    };
    let kv_channel = connect(addr, tls).await?;
    let admin_channel = connect(&admin_addr, tls).await?;
    let mut admin_client = make_admin_client(admin_channel, admin_token);
    let mut kv_client = make_kv_client(kv_channel, client_token);
    let mut kv_addr = addr.to_string();
    let mut kv_peers = vec![addr.to_string()];
    match admin_client
        .get_group_membership(GroupId { group_id: group })
        .await
    {
        Ok(response) => {
            let membership = response.into_inner();
            for member in membership.voters.into_iter().chain(membership.learners) {
                if !member.addr.is_empty() && !kv_peers.contains(&member.addr) {
                    kv_peers.push(member.addr);
                }
            }
        }
        Err(e) => println!("{}", member_discovery_message(&e, addr)),
    }
    let mut needs_reconnect = false;
    println!("connected: client={addr} admin={admin_addr} group={group}");

    let mut rl = rustyline::DefaultEditor::new()?;

    loop {
        let line = match rl.readline("rr> ") {
            Ok(l) => l,
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(rustyline::error::ReadlineError::Interrupted) => break,
            Err(e) => return Err(e.into()),
        };
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let _ = rl.add_history_entry(&line);

        if needs_reconnect || line.trim() == "reconnect" {
            let kv = connect(&kv_addr, tls).await;
            let admin = connect(&admin_addr, tls).await;
            match (kv, admin) {
                (Ok(kv_ch), Ok(admin_ch)) => {
                    kv_client = make_kv_client(kv_ch, client_token);
                    admin_client = make_admin_client(admin_ch, admin_token);
                    needs_reconnect = false;
                    if line.trim() == "reconnect" {
                        println!("reconnected: client={kv_addr} admin={admin_addr}");
                        continue;
                    }
                }
                (kv, admin) => {
                    if let Err(e) = kv {
                        println!(
                            "kv reconnect to {kv_addr} failed: {}",
                            connect_error_brief(&e)
                        );
                    }
                    if let Err(e) = admin {
                        println!(
                            "admin reconnect to {admin_addr} failed: {}",
                            connect_error_brief(&e)
                        );
                    }
                    println!("{}", reconnect_failed_message(&kv_addr, &admin_addr));
                    continue;
                }
            }
        }
        if line.trim() == "reconnect" {
            continue;
        }

        match parse_repl_line(&line) {
            ReplCommand::Empty | ReplCommand::Reconnect => {}
            ReplCommand::MissingArg { usage, .. } => {
                println!("Usage: {usage}");
            }
            ReplCommand::Unknown { cmd } => {
                println!("Unknown command: '{cmd}'. Type 'help' for available commands.");
            }
            ReplCommand::Get { key } => {
                let req = KvRequest::Get { key };
                let payload = bincode::serde::encode_to_vec(&req, standard())?;
                let kv_before = kv_addr.clone();
                let resp = kv_call(
                    &mut kv_client,
                    &mut kv_addr,
                    &kv_peers,
                    group,
                    &payload,
                    tls,
                    client_token,
                )
                .await;
                follow_admin_after_redirect(
                    &kv_before,
                    &kv_addr,
                    admin_explicit,
                    &mut admin_addr,
                    &mut admin_client,
                    tls,
                    admin_token,
                )
                .await;
                match resp {
                    Ok(inner) if inner.success => match decode_kv_response(&inner.payload) {
                        Ok(Some(s)) => println!("{s}"),
                        Ok(None) => println!("(nil)"),
                        Err(e) => println!("decode error: {e}"),
                    },
                    Ok(inner) => println!("Error: {}", kv_failure_detail(&inner)),
                    Err(e) => handle_rpc_error(&e, "ClientService", &kv_addr, &mut needs_reconnect),
                }
            }
            ReplCommand::Put { key, value } => {
                let req = KvRequest::Put {
                    key,
                    value: value.as_bytes().to_vec(),
                };
                let payload = bincode::serde::encode_to_vec(&req, standard())?;
                let kv_before = kv_addr.clone();
                let resp = kv_call(
                    &mut kv_client,
                    &mut kv_addr,
                    &kv_peers,
                    group,
                    &payload,
                    tls,
                    client_token,
                )
                .await;
                follow_admin_after_redirect(
                    &kv_before,
                    &kv_addr,
                    admin_explicit,
                    &mut admin_addr,
                    &mut admin_client,
                    tls,
                    admin_token,
                )
                .await;
                match resp {
                    Ok(inner) if inner.success => println!("OK"),
                    Ok(inner) => println!("Error: {}", kv_failure_detail(&inner)),
                    Err(e) => handle_rpc_error(&e, "ClientService", &kv_addr, &mut needs_reconnect),
                }
            }
            ReplCommand::Delete { key } => {
                let req = KvRequest::Delete { key };
                let payload = bincode::serde::encode_to_vec(&req, standard())?;
                let kv_before = kv_addr.clone();
                let resp = kv_call(
                    &mut kv_client,
                    &mut kv_addr,
                    &kv_peers,
                    group,
                    &payload,
                    tls,
                    client_token,
                )
                .await;
                follow_admin_after_redirect(
                    &kv_before,
                    &kv_addr,
                    admin_explicit,
                    &mut admin_addr,
                    &mut admin_client,
                    tls,
                    admin_token,
                )
                .await;
                match resp {
                    Ok(inner) if inner.success => println!("OK"),
                    Ok(inner) => println!("Error: {}", kv_failure_detail(&inner)),
                    Err(e) => handle_rpc_error(&e, "ClientService", &kv_addr, &mut needs_reconnect),
                }
            }
            ReplCommand::Status => match admin_client.get_node_status(Empty {}).await {
                Ok(resp) => {
                    let s = resp.into_inner();
                    println!("node_id    : {}", s.node_id);
                    println!("listen_addr: {}", s.listen_addr);
                    println!("group_ids  : {:?}", s.group_ids);
                    println!("uptime_secs: {}", s.uptime_secs);
                }
                Err(e) => handle_rpc_error(&e, "AdminService", &admin_addr, &mut needs_reconnect),
            },
            ReplCommand::Members => {
                match admin_client
                    .get_group_status(GroupId { group_id: group })
                    .await
                {
                    Ok(resp) => {
                        let s = resp.into_inner();
                        println!("group_id      : {}", s.group_id);
                        println!("is_leader     : {}", s.is_leader);
                        println!("current_term  : {}", s.current_term);
                        println!("last_log_index: {}", s.last_log_index);
                        println!("leader_node_id: {}", s.leader_node_id);
                    }
                    Err(e) => {
                        handle_rpc_error(&e, "AdminService", &admin_addr, &mut needs_reconnect)
                    }
                }
            }
            ReplCommand::FormationStatus => {
                match admin_client.get_formation_status(Empty {}).await {
                    Ok(resp) => {
                        let mut s = resp.into_inner();
                        s.groups.sort_unstable_by_key(|g| g.group_id);
                        println!("{}", format_formation_status(&s));
                    }
                    Err(e) => {
                        handle_rpc_error(&e, "AdminService", &admin_addr, &mut needs_reconnect)
                    }
                }
            }
            ReplCommand::Help => {
                println!("Available commands:");
                println!("  get <key>             — fetch a value");
                println!("  put <key> <value>     — store a value");
                println!("  delete <key>          — remove a key");
                println!("  status                — show node status (via admin RPC)");
                println!("  members               — show group membership (via admin RPC)");
                println!("  formation status      — show cluster formation lifecycle status (via admin RPC)");
                println!("  help                  — show this help");
                println!();
                println!(
                    "Note: get/put/delete use kv-store wire format \
                     (bincode-encoded KvRequest/KvResponse) and go to --addr \
                     (client port); status/members/formation status go to \
                     --admin-addr (default: client port + 1000). A kv leader \
                     redirect moves \
                     the derived admin channel along (an explicit --admin-addr \
                     stays pinned); 'status' is node-level and then describes \
                     the leader node."
                );
            }
        }
    }

    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// Verify the CLI definition compiles and --help parses without panic.
    #[test]
    fn test_cli_help_does_not_panic() {
        Cli::command().debug_assert();
    }

    /// Verify node status subcommand parses correctly.
    #[test]
    fn test_parse_node_status() {
        let cli = Cli::try_parse_from(["rr-admin", "--addr", "127.0.0.1:52001", "node", "status"])
            .expect("should parse node status");
        assert_eq!(cli.addr, "127.0.0.1:52001");
        assert!(matches!(
            cli.command,
            Commands::Node {
                cmd: NodeCommands::Status
            }
        ));
    }

    /// Verify group status subcommand parses correctly.
    #[test]
    fn test_parse_group_status() {
        let cli = Cli::try_parse_from(["rr-admin", "group", "status", "--group", "1"])
            .expect("should parse group status");
        assert!(matches!(
            cli.command,
            Commands::Group {
                cmd: GroupCommands::Status { group: 1 }
            }
        ));
    }

    #[test]
    fn test_parse_group_list() {
        let cli =
            Cli::try_parse_from(["rr-admin", "group", "list"]).expect("should parse group list");
        assert!(matches!(
            cli.command,
            Commands::Group {
                cmd: GroupCommands::List
            }
        ));
    }

    #[test]
    fn test_parse_group_snapshot_status() {
        let cli = Cli::try_parse_from(["rr-admin", "group", "snapshot-status", "--group", "2"])
            .expect("should parse group snapshot-status");
        assert!(matches!(
            cli.command,
            Commands::Group {
                cmd: GroupCommands::SnapshotStatus { group: 2 }
            }
        ));
    }

    /// Verify group snapshot subcommand parses correctly.
    #[test]
    fn test_parse_group_snapshot() {
        let cli = Cli::try_parse_from(["rr-admin", "group", "snapshot", "--group", "2"])
            .expect("should parse group snapshot");
        assert!(matches!(
            cli.command,
            Commands::Group {
                cmd: GroupCommands::Snapshot { group: 2 }
            }
        ));
    }

    /// Verify group transfer-leader subcommand parses correctly.
    #[test]
    fn test_parse_group_transfer_leader() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "group",
            "transfer-leader",
            "--group",
            "1",
            "--to",
            "3",
        ])
        .expect("should parse transfer-leader");
        assert!(matches!(
            cli.command,
            Commands::Group {
                cmd: GroupCommands::TransferLeader { group: 1, to: 3 }
            }
        ));
    }

    #[test]
    fn test_parse_node_decommission() {
        let cli = Cli::try_parse_from(["rr-admin", "node", "decommission", "--node-id", "4"])
            .expect("should parse decommission");
        match cli.command {
            Commands::Node {
                cmd: NodeCommands::Decommission { node_id, dry_run },
            } => {
                assert_eq!(node_id, 4);
                assert!(!dry_run);
            }
            other => panic!("expected Decommission, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_node_decommission_dry_run() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "node",
            "decommission",
            "--node-id",
            "5",
            "--dry-run",
        ])
        .expect("should parse decommission --dry-run");
        match cli.command {
            Commands::Node {
                cmd: NodeCommands::Decommission { node_id, dry_run },
            } => {
                assert_eq!(node_id, 5);
                assert!(dry_run);
            }
            other => panic!("expected Decommission, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_node_drain() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "node",
            "drain",
            "--node-id",
            "4",
            "--timeout-secs",
            "45",
        ])
        .expect("should parse node drain");
        match cli.command {
            Commands::Node {
                cmd:
                    NodeCommands::Drain {
                        node_id,
                        timeout_secs,
                    },
            } => {
                assert_eq!(node_id, 4);
                assert_eq!(timeout_secs, 45);
            }
            other => panic!("expected Drain, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_group_add_learner() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "group",
            "add-learner",
            "--group",
            "1",
            "--node-id",
            "4",
            "--addr",
            "127.0.0.1:51004",
        ])
        .expect("should parse add-learner");
        assert!(matches!(
            cli.command,
            Commands::Group {
                cmd: GroupCommands::AddLearner {
                    group: 1,
                    node_id: 4,
                    ..
                }
            }
        ));
    }

    #[test]
    fn test_parse_group_change_membership() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "group",
            "change-membership",
            "--group",
            "1",
            "--voters",
            "1,2,3",
        ])
        .expect("should parse change-membership");
        match cli.command {
            Commands::Group {
                cmd:
                    GroupCommands::ChangeMembership {
                        group,
                        voters,
                        force,
                    },
            } => {
                assert_eq!(group, 1);
                assert_eq!(voters, "1,2,3");
                assert!(!force, "force defaults to false");
            }
            other => panic!("expected ChangeMembership, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_group_change_membership_with_force() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "group",
            "change-membership",
            "--group",
            "1",
            "--voters",
            "1",
            "--force",
        ])
        .expect("should parse change-membership --force");
        match cli.command {
            Commands::Group {
                cmd: GroupCommands::ChangeMembership { force, .. },
            } => assert!(force),
            other => panic!("expected ChangeMembership, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_group_reset_ingestor_offset() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "group",
            "reset-ingestor-offset",
            "--group",
            "1",
            "--offset",
            "100",
        ])
        .expect("should parse reset-ingestor-offset");
        match cli.command {
            Commands::Group {
                cmd: GroupCommands::ResetIngestorOffset { group, offset },
            } => {
                assert_eq!(group, 1);
                assert_eq!(offset, 100);
            }
            other => panic!("expected ResetIngestorOffset, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_group_reset_ingestor_offset_negative_one() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "group",
            "reset-ingestor-offset",
            "--group",
            "1",
            "--offset",
            "-1",
        ])
        .expect("should parse reset-ingestor-offset --offset -1");
        match cli.command {
            Commands::Group {
                cmd: GroupCommands::ResetIngestorOffset { group, offset },
            } => {
                assert_eq!(group, 1);
                assert_eq!(offset, -1);
            }
            other => panic!("expected ResetIngestorOffset, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_group_online_backup() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "group",
            "online-backup",
            "--group",
            "1",
            "--output",
            "/tmp/snap.bin",
            "--force-fresh",
        ])
        .expect("should parse online-backup");
        match cli.command {
            Commands::Group {
                cmd:
                    GroupCommands::OnlineBackup {
                        group,
                        output,
                        force_fresh,
                    },
            } => {
                assert_eq!(group, 1);
                assert_eq!(output.to_str(), Some("/tmp/snap.bin"));
                assert!(force_fresh);
            }
            other => panic!("expected OnlineBackup, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_group_membership() {
        let cli = Cli::try_parse_from(["rr-admin", "group", "membership", "--group", "7"])
            .expect("should parse group membership");
        assert!(matches!(
            cli.command,
            Commands::Group {
                cmd: GroupCommands::Membership { group: 7 }
            }
        ));
    }

    #[test]
    fn test_parse_node_reset_requires_force() {
        let cli = Cli::try_parse_from(["rr-admin", "node-reset", "--data-dir", "/tmp/rr/data"])
            .expect("should parse node-reset without --force");
        match cli.command {
            Commands::NodeReset { force, .. } => assert!(!force),
            other => panic!("expected NodeReset, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_node_reset_with_force() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "node-reset",
            "--data-dir",
            "/tmp/rr/data",
            "--force",
        ])
        .expect("should parse node-reset --force");
        match cli.command {
            Commands::NodeReset { data_dir, force } => {
                assert_eq!(data_dir.to_str(), Some("/tmp/rr/data"));
                assert!(force);
            }
            other => panic!("expected NodeReset, got {other:?}"),
        }
    }

    /// Verify metrics subcommand parses correctly.
    #[test]
    fn test_parse_metrics() {
        let cli = Cli::try_parse_from(["rr-admin", "metrics"]).expect("should parse metrics");
        assert!(matches!(cli.command, Commands::Metrics));
    }

    #[test]
    fn test_parse_config_show() {
        let cli =
            Cli::try_parse_from(["rr-admin", "config", "show"]).expect("should parse config show");
        assert!(matches!(
            cli.command,
            Commands::Config {
                cmd: ConfigCommands::Show
            }
        ));
    }

    /// Verify --output json flag parses correctly.
    #[test]
    fn test_parse_json_output_flag() {
        let cli = Cli::try_parse_from(["rr-admin", "--output", "json", "metrics"])
            .expect("should parse --output json");
        assert!(matches!(cli.output, OutputFormat::Json));
    }

    /// Verify default addr is 127.0.0.1:52001.
    #[test]
    fn test_default_addr() {
        let cli = Cli::try_parse_from(["rr-admin", "metrics"]).expect("should parse metrics");
        assert_eq!(cli.addr, "127.0.0.1:52001");
    }

    /// Verify repl subcommand parses correctly with defaults.
    #[test]
    fn test_parse_repl_subcommand() {
        let cli = Cli::try_parse_from(["rr-admin", "repl"]).expect("should parse repl");
        assert!(matches!(cli.command, Commands::Repl { group: 1, .. }));
    }

    /// Verify repl subcommand parses --group flag.
    #[test]
    fn test_parse_repl_with_group() {
        let cli = Cli::try_parse_from(["rr-admin", "repl", "--group", "2"])
            .expect("should parse repl --group 2");
        assert!(matches!(cli.command, Commands::Repl { group: 2, .. }));
    }

    /// Verify repl subcommand parses --addr/--admin-addr.
    #[test]
    fn test_parse_repl_with_admin_addr() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "repl",
            "--addr",
            "127.0.0.1:51002",
            "--admin-addr",
            "127.0.0.1:53002",
        ])
        .expect("should parse repl --admin-addr");
        match cli.command {
            Commands::Repl {
                addr, admin_addr, ..
            } => {
                assert_eq!(addr, "127.0.0.1:51002");
                assert_eq!(admin_addr.as_deref(), Some("127.0.0.1:53002"));
            }
            other => panic!("expected Repl, got {other:?}"),
        }
    }

    #[test]
    fn test_repl_defaults_to_client_port() {
        let cli = Cli::try_parse_from(["rr-admin", "repl"]).expect("should parse repl");
        match cli.command {
            Commands::Repl {
                addr, admin_addr, ..
            } => {
                assert_eq!(addr, "127.0.0.1:51001");
                assert!(admin_addr.is_none(), "admin addr defaults to derived");
            }
            other => panic!("expected Repl, got {other:?}"),
        }
    }

    #[test]
    fn test_default_admin_addr_derivation() {
        assert_eq!(
            default_admin_addr("127.0.0.1:51001").unwrap(),
            "127.0.0.1:52001"
        );
        assert_eq!(
            default_admin_addr("10.0.0.2:60000").unwrap(),
            "10.0.0.2:61000"
        );
        assert!(default_admin_addr("no-port").is_err());
        assert!(default_admin_addr("host:abc").is_err());
        assert!(
            default_admin_addr("host:65535").is_err(),
            "port+1000 overflow"
        );
    }

    #[test]
    fn test_admin_follows_redirect() {
        // An explicit --admin-addr never moves with the kv channel.
        assert!(admin_follows_redirect(true, "10.0.0.9:51003").is_none());
        // A derived admin endpoint follows the leader: same host, port+1000.
        let derived = admin_follows_redirect(false, "10.0.0.9:51003")
            .expect("derived admin addr should follow")
            .unwrap();
        assert_eq!(derived, "10.0.0.9:52003");
        // A malformed leader address surfaces the derivation error.
        assert!(admin_follows_redirect(false, "no-port")
            .expect("derived admin addr should follow")
            .is_err());
    }

    #[test]
    fn test_kv_failure_detail_variants() {
        let resp = ClientResponse {
            success: false,
            payload: vec![],
            leader_info: Some(rr_network::proto::client::LeaderInfo {
                node_id: 3,
                addr: "10.0.0.3:51001".to_string(),
            }),
            idem_error: None,
            ns_limit_error: None,
        };
        let msg = kv_failure_detail(&resp);
        assert!(msg.contains("non-leader") && msg.contains("10.0.0.3:51001"));
        assert!(msg.contains("NOT applied"));

        let resp = ClientResponse {
            success: false,
            payload: vec![],
            leader_info: None,
            idem_error: None,
            ns_limit_error: None,
        };
        assert!(kv_failure_detail(&resp).contains("NOT applied"));

        let resp = ClientResponse {
            success: false,
            payload: vec![],
            leader_info: None,
            idem_error: None,
            ns_limit_error: Some(rr_network::proto::client::NamespaceLimitError {
                request_type: "orders".to_string(),
                request_id: 7,
                namespaces: 1024,
                max: 1024,
            }),
        };
        let msg = kv_failure_detail(&resp);
        assert!(msg.contains("namespace limit exceeded"));
        assert!(msg.contains("NOT applied"));
    }

    /// Verify KvRequest::Put encodes and decodes correctly via bincode.
    #[test]
    fn test_encode_kv_put_roundtrip() {
        let req = KvRequest::Put {
            key: "hello".to_string(),
            value: b"world".to_vec(),
        };
        let bytes = bincode::serde::encode_to_vec(&req, standard()).expect("encode");
        let (decoded, _): (KvRequest, _) =
            bincode::serde::decode_from_slice(&bytes, standard()).expect("decode");
        assert!(matches!(decoded, KvRequest::Put { .. }));
        assert!(!bytes.is_empty());
    }

    /// Verify KvRequest::Get encodes to non-empty bytes.
    #[test]
    fn test_encode_kv_get_produces_bytes() {
        let req = KvRequest::Get {
            key: "mykey".to_string(),
        };
        let bytes = bincode::serde::encode_to_vec(&req, standard()).expect("encode");
        assert!(!bytes.is_empty());
    }

    /// Verify KvRequest::Delete encodes to non-empty bytes.
    #[test]
    fn test_encode_kv_delete_produces_bytes() {
        let req = KvRequest::Delete {
            key: "mykey".to_string(),
        };
        let bytes = bincode::serde::encode_to_vec(&req, standard()).expect("encode");
        assert!(!bytes.is_empty());
    }

    // ── parse_repl_line tests ──────────────────────────────────────────────────

    #[test]
    fn test_parse_repl_empty_line() {
        assert_eq!(parse_repl_line(""), ReplCommand::Empty);
        assert_eq!(parse_repl_line("   "), ReplCommand::Empty);
    }

    #[test]
    fn test_parse_repl_get() {
        assert_eq!(
            parse_repl_line("get mykey"),
            ReplCommand::Get {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_repl_get_missing_key() {
        assert!(matches!(
            parse_repl_line("get"),
            ReplCommand::MissingArg { cmd: "get", .. }
        ));
    }

    #[test]
    fn test_parse_repl_put() {
        assert_eq!(
            parse_repl_line("put k v"),
            ReplCommand::Put {
                key: "k".to_string(),
                value: "v".to_string()
            }
        );
    }

    #[test]
    fn test_parse_repl_put_value_with_spaces() {
        assert_eq!(
            parse_repl_line("put k hello world"),
            ReplCommand::Put {
                key: "k".to_string(),
                value: "hello world".to_string()
            }
        );
    }

    #[test]
    fn test_parse_repl_put_missing_value() {
        assert!(matches!(
            parse_repl_line("put k"),
            ReplCommand::MissingArg { cmd: "put", .. }
        ));
    }

    #[test]
    fn test_parse_repl_put_missing_both() {
        assert!(matches!(
            parse_repl_line("put"),
            ReplCommand::MissingArg { cmd: "put", .. }
        ));
    }

    #[test]
    fn test_parse_repl_delete() {
        assert_eq!(
            parse_repl_line("delete k"),
            ReplCommand::Delete {
                key: "k".to_string()
            }
        );
    }

    #[test]
    fn test_parse_repl_delete_missing_key() {
        assert!(matches!(
            parse_repl_line("delete"),
            ReplCommand::MissingArg { cmd: "delete", .. }
        ));
    }

    #[test]
    fn test_parse_repl_status() {
        assert_eq!(parse_repl_line("status"), ReplCommand::Status);
    }

    #[test]
    fn test_parse_repl_members() {
        assert_eq!(parse_repl_line("members"), ReplCommand::Members);
    }

    #[test]
    fn test_parse_repl_help() {
        assert_eq!(parse_repl_line("help"), ReplCommand::Help);
    }

    #[test]
    fn test_parse_repl_reconnect() {
        assert_eq!(parse_repl_line("reconnect"), ReplCommand::Reconnect);
    }

    #[test]
    fn test_parse_repl_unknown() {
        assert_eq!(
            parse_repl_line("foobar"),
            ReplCommand::Unknown {
                cmd: "foobar".to_string()
            }
        );
    }

    #[test]
    fn test_parse_repl_leading_whitespace_trimmed() {
        assert_eq!(
            parse_repl_line("  get mykey"),
            ReplCommand::Get {
                key: "mykey".to_string()
            }
        );
    }

    // ── decode_kv_response tests ───────────────────────────────────────────────

    #[test]
    fn test_decode_kv_response_none_value() {
        let resp = KvResponse { value: None };
        let bytes = bincode::serde::encode_to_vec(&resp, standard()).unwrap();
        assert_eq!(decode_kv_response(&bytes).unwrap(), None);
    }

    #[test]
    fn test_decode_kv_response_some_value() {
        let resp = KvResponse {
            value: Some(b"hello".to_vec()),
        };
        let bytes = bincode::serde::encode_to_vec(&resp, standard()).unwrap();
        assert_eq!(
            decode_kv_response(&bytes).unwrap(),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_decode_kv_response_utf8_value() {
        let resp = KvResponse {
            value: Some("世界".as_bytes().to_vec()),
        };
        let bytes = bincode::serde::encode_to_vec(&resp, standard()).unwrap();
        assert_eq!(
            decode_kv_response(&bytes).unwrap(),
            Some("世界".to_string())
        );
    }

    #[test]
    fn test_decode_kv_response_invalid_bytes() {
        assert!(decode_kv_response(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    // ── formation status tests ───────────────────────────────────────────────

    /// Verify formation status subcommand parses correctly.
    #[test]
    fn test_parse_formation_status() {
        let cli = Cli::try_parse_from(["rr-admin", "formation", "status"])
            .expect("should parse formation status");
        assert!(matches!(
            cli.command,
            Commands::Formation {
                cmd: FormationCommands::Status
            }
        ));
    }

    #[test]
    fn test_parse_repl_formation_status() {
        assert_eq!(
            parse_repl_line("formation status"),
            ReplCommand::FormationStatus
        );
    }

    #[test]
    fn test_parse_repl_formation_missing_subcommand() {
        assert!(matches!(
            parse_repl_line("formation"),
            ReplCommand::MissingArg {
                cmd: "formation",
                usage: "formation status"
            }
        ));
    }

    /// An unknown formation subcommand echoes the first token, matching the
    /// established Unknown convention for every other command.
    #[test]
    fn test_parse_repl_formation_unknown_subcommand() {
        assert_eq!(
            parse_repl_line("formation foo"),
            ReplCommand::Unknown {
                cmd: "formation".to_string()
            }
        );
    }

    /// Tracked cluster: identity/revision fields first, then the per-group
    /// table (group_id, bootstrap_epoch, state).
    #[test]
    fn test_format_formation_status_tracked_renders_fields_and_table() {
        let resp = FormationStatus {
            origin: "Genesis".to_string(),
            cluster_uuid: "3f8c9d2e-1111-4222-8333-abcdef012345".to_string(),
            topology_revision: 3,
            store_revision: 12,
            state: "Active".to_string(),
            groups: vec![
                rr_network::proto::admin::GroupFormationStatus {
                    group_id: 1,
                    bootstrap_epoch: 7,
                    state: "Active".to_string(),
                },
                rr_network::proto::admin::GroupFormationStatus {
                    group_id: 2,
                    bootstrap_epoch: 7,
                    state: "Forming".to_string(),
                },
            ],
        };
        let out = format_formation_status(&resp);
        assert!(out.contains("origin"));
        assert!(out.contains("Genesis"));
        assert!(out.contains("cluster_uuid"));
        assert!(out.contains("3f8c9d2e-1111-4222-8333-abcdef012345"));
        assert!(out.contains("topology_revision: 3"));
        assert!(out.contains("store_revision   : 12"));
        assert!(out.contains("group_id"));
        assert!(out.contains("bootstrap_epoch"));
        assert!(out.contains("Active"));
        assert!(out.contains("Forming"));
        assert!(!out.contains("not tracked"));
    }

    /// Untracked cluster: the output must point at the playbook instead of
    /// rendering an empty group table.
    #[test]
    fn test_format_formation_status_untracked_shows_playbook_hint() {
        let resp = FormationStatus {
            origin: "Untracked".to_string(),
            cluster_uuid: String::new(),
            topology_revision: 0,
            store_revision: 0,
            state: "Untracked".to_string(),
            groups: vec![],
        };
        let out = format_formation_status(&resp);
        assert!(out.contains("not tracked by formation lifecycle"));
        assert!(out.contains("legacy cluster"));
        assert!(out.contains("playbook"));
        assert!(!out.contains("bootstrap_epoch"));
    }

    /// Proto contract arm: the aggregate `state` can be "Untracked" even when
    /// `origin` carries a real value — the hint must still appear and the
    /// group table must not render.
    #[test]
    fn test_format_formation_status_cluster_level_untracked_state() {
        let resp = FormationStatus {
            origin: "Genesis".to_string(),
            cluster_uuid: "3f8c9d2e-1111-4222-8333-abcdef012345".to_string(),
            topology_revision: 0,
            store_revision: 0,
            state: "Untracked".to_string(),
            groups: vec![],
        };
        let out = format_formation_status(&resp);
        assert!(out.contains("Genesis"));
        assert!(out.contains("not tracked by formation lifecycle"));
        assert!(out.contains("playbook"));
        assert!(!out.contains("bootstrap_epoch"));
    }

    // ── E-3 connection-error remediation tests ──────────────────────────────

    /// Every connection failure must carry the remediation checklist: port
    /// role (admin vs client port), process liveness, TLS/admin_token, and a
    /// playbook pointer.
    #[test]
    fn test_connect_timeout_error_carries_remediation() {
        let msg = connect_timeout_error("127.0.0.1:52001").to_string();
        assert!(msg.contains("timed out"));
        assert!(msg.contains("127.0.0.1:52001"));
        for kw in [
            "admin port",
            "client port",
            "alive",
            "TLS",
            "token",
            "playbook",
        ] {
            assert!(msg.contains(kw), "missing {kw:?} in: {msg}");
        }
    }

    #[test]
    fn test_connect_error_carries_remediation() {
        let source =
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let err = connect_error("127.0.0.1:52001", source);
        let msg = err.to_string();
        assert!(msg.contains("connect to 127.0.0.1:52001 failed"));
        for kw in [
            "admin port",
            "client port",
            "alive",
            "TLS",
            "token",
            "playbook",
        ] {
            assert!(msg.contains(kw), "missing {kw:?} in: {msg}");
        }
        // The source chain is preserved: the concrete cause (refused/TLS/DNS)
        // stays visible as `Caused by:` in the debug rendering.
        let debug = format!("{err:?}");
        assert!(
            debug.contains("Caused by"),
            "missing cause chain in: {debug}"
        );
        assert!(debug.contains("connection refused"));
    }

    /// Per-channel status lines use the brief rendering: first message line
    /// plus root cause, never the remediation checklist (printed once at the
    /// summary/site level).
    #[test]
    fn test_connect_error_brief_strips_remedy_keeps_cause() {
        let source =
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let err = connect_error("127.0.0.1:52001", source);
        let brief = connect_error_brief(&err);
        assert!(brief.contains("connect to 127.0.0.1:52001 failed"));
        assert!(brief.contains("connection refused"));
        assert!(
            !brief.contains("next steps"),
            "brief must not carry the remedy: {brief}"
        );
        // The timeout error has no source — the brief falls back to its
        // first message line.
        let timeout = connect_timeout_error("127.0.0.1:52001");
        let brief = connect_error_brief(&timeout);
        assert!(brief.contains("timed out"));
        assert!(!brief.contains("next steps"));
    }

    #[test]
    fn test_member_discovery_message_carries_remediation() {
        let msg = member_discovery_message(&Status::unavailable("refused"), "127.0.0.1:51001");
        assert!(msg.contains("member discovery failed"));
        assert!(msg.contains("automatic failover is limited to 127.0.0.1:51001"));
        for kw in [
            "admin port",
            "client port",
            "alive",
            "TLS",
            "token",
            "playbook",
        ] {
            assert!(msg.contains(kw), "missing {kw:?} in: {msg}");
        }
    }

    #[test]
    fn test_reconnect_failed_message_carries_remediation() {
        let msg = reconnect_failed_message("127.0.0.1:51001", "127.0.0.1:52001");
        assert!(msg.contains("will retry on next command"));
        assert!(msg.contains("client port 127.0.0.1:51001"));
        assert!(msg.contains("admin port 127.0.0.1:52001"));
        for kw in ["alive", "TLS", "token", "playbook"] {
            assert!(msg.contains(kw), "missing {kw:?} in: {msg}");
        }
    }

    struct StaticClientService;

    #[tonic::async_trait]
    impl rr_network::proto::client::client_service_server::ClientService for StaticClientService {
        async fn request(
            &self,
            _request: tonic::Request<ClientRequest>,
        ) -> std::result::Result<Response<ClientResponse>, Status> {
            Ok(Response::new(ClientResponse {
                success: true,
                payload: b"leader-ok".to_vec(),
                leader_info: None,
                idem_error: None,
                ns_limit_error: None,
            }))
        }

        async fn get_cluster_info(
            &self,
            _request: tonic::Request<rr_network::proto::client::Empty>,
        ) -> std::result::Result<Response<rr_network::proto::client::ClusterInfo>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn read_local(
            &self,
            _request: tonic::Request<rr_network::proto::client::ReadLocalRequest>,
        ) -> std::result::Result<Response<rr_network::proto::client::ReadLocalResponse>, Status>
        {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn read_linearizable(
            &self,
            _request: tonic::Request<rr_network::proto::client::ReadLocalRequest>,
        ) -> std::result::Result<Response<rr_network::proto::client::ReadLocalResponse>, Status>
        {
            Err(Status::unimplemented("not used by this test"))
        }
    }

    async fn spawn_static_client_service() -> (
        String,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        use rr_network::proto::client::client_service_server::ClientServiceServer;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test client service");
        let addr = listener.local_addr().expect("test service address");
        drop(listener);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ClientServiceServer::new(StaticClientService))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("test client service must run");
        });
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        (format!("127.0.0.1:{}", addr.port()), shutdown_tx, handle)
    }

    #[tokio::test]
    async fn test_kv_call_reconnects_to_a_surviving_peer_after_transport_failure() {
        let (dead_addr, dead_shutdown, dead_handle) = spawn_static_client_service().await;
        let mut kv_addr = dead_addr.clone();
        let mut kv_client = make_kv_client(connect(&dead_addr, None).await.unwrap(), &None);
        dead_shutdown.send(()).unwrap();
        dead_handle.await.unwrap();

        let (leader_addr, leader_shutdown, leader_handle) = spawn_static_client_service().await;
        let peers = vec![dead_addr, leader_addr.clone()];
        let response = kv_call(
            &mut kv_client,
            &mut kv_addr,
            &peers,
            1,
            b"request",
            None,
            &None,
        )
        .await
        .expect("the request must reconnect to the surviving peer");

        assert!(response.success);
        assert_eq!(response.payload, b"leader-ok");
        assert_eq!(kv_addr, leader_addr);
        leader_shutdown.send(()).unwrap();
        leader_handle.await.unwrap();
    }
}
