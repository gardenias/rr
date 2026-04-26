//! `rr-admin` — Command-line tool for administering an `rr` cluster.
//!
//! 提供命令行界面，用于查询节点状态、Raft 组状态、触发快照、转移领导者和查看指标摘要。
//! 支持人类可读的表格输出和 `--output json` 机器可读模式，以及交互式 REPL。
//!
//! ---
//!
//! Provides a CLI for querying node status, Raft group status, triggering snapshots,
//! transferring leadership, and viewing metrics summaries.
//! Supports human-readable table output, `--output json` machine-readable mode,
//! and an interactive REPL for kv-store operations.

use anyhow::Result;
use bincode::config::standard;
use clap::{Parser, Subcommand, ValueEnum};
use rr_network::proto::admin::admin_service_client::AdminServiceClient;
use rr_network::proto::admin::{
    online_backup_chunk::Body as OnlineBackupBody, AddLearnerRequest, ChangeMembershipRequest,
    Empty, GroupId, OnlineBackupRequest, TransferLeaderRequest,
};
use rr_network::proto::client::client_service_client::ClientServiceClient;
use rr_network::proto::client::ClientRequest;
use std::path::PathBuf;
use tonic::transport::Channel;

mod backup;
mod node_reset;

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
    /// Show top-level metrics summary.
    Metrics,
    /// Launch an interactive REPL for kv-store operations.
    Repl {
        /// gRPC address of the target node (host:port).
        #[arg(long, default_value = "127.0.0.1:52001")]
        addr: String,
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
    /// holding a live RocksDB lock, and non-existent paths. Pass
    /// `--force` to skip the safety prompt after manual confirmation.
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
}

#[derive(Subcommand, Debug)]
enum GroupCommands {
    /// Get Raft group status.
    Status {
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
    ChangeMembership {
        /// Raft group ID.
        #[arg(long)]
        group: u64,
        /// Comma-separated list of voter node IDs (e.g. `1,2,3`).
        #[arg(long)]
        voters: String,
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

// ── Entry point ────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Repl { addr, group } => {
            run_repl(addr, *group).await?;
        }
        Commands::Backup {
            data_dir,
            output,
            node_id,
            comment,
        } => {
            let meta =
                backup::create_backup(data_dir, output, *node_id, comment.clone())?;
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
                    println!("           --group <ID> --node-id <this-node-id> --addr <this-host:port>");
                    println!("  3. Wait for replication to catch up, then promote with:");
                    println!("       rr-admin --addr <leader-host:port> group change-membership \\");
                    println!("           --group <ID> --voters <new-voter-set>");
                }
            }
        }
        _ => {
            let channel = connect(&cli.addr).await?;
            let mut client = AdminServiceClient::new(channel);

            match &cli.command {
                Commands::Node { cmd: NodeCommands::Status } => {
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
                Commands::Group { cmd: GroupCommands::Status { group } } => {
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
                Commands::Group { cmd: GroupCommands::Snapshot { group } } => {
                    let resp = client
                        .trigger_snapshot(GroupId { group_id: *group })
                        .await?
                        .into_inner();
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "success": resp.success,
                                "message": resp.message,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("TriggerSnapshot: success={} message={}", resp.success, resp.message);
                        }
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::TransferLeader { group, to },
                } => {
                    let resp = client
                        .transfer_leader(TransferLeaderRequest {
                            group_id: *group,
                            target_node_id: *to,
                        })
                        .await?
                        .into_inner();
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "success": resp.success,
                                "message": resp.message,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("TransferLeader: success={} message={}", resp.success, resp.message);
                        }
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::AddLearner { group, node_id, addr },
                } => {
                    let resp = client
                        .add_learner(AddLearnerRequest {
                            group_id: *group,
                            node_id: *node_id,
                            addr: addr.clone(),
                        })
                        .await?
                        .into_inner();
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "success": resp.success,
                                "message": resp.message,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("AddLearner: success={} message={}", resp.success, resp.message);
                        }
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::ChangeMembership { group, voters },
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
                    let resp = client
                        .change_membership(ChangeMembershipRequest {
                            group_id: *group,
                            voter_node_ids,
                        })
                        .await?
                        .into_inner();
                    match cli.output {
                        OutputFormat::Json => {
                            let val = serde_json::json!({
                                "success": resp.success,
                                "message": resp.message,
                            });
                            println!("{}", serde_json::to_string_pretty(&val)?);
                        }
                        OutputFormat::Human => {
                            println!("ChangeMembership: success={} message={}", resp.success, resp.message);
                        }
                    }
                }
                Commands::Group {
                    cmd: GroupCommands::OnlineBackup { group, output, force_fresh },
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

/// Establish a gRPC channel to the given host:port address.
async fn connect(addr: &str) -> Result<Channel> {
    let endpoint = format!("http://{addr}");
    let channel = tonic::transport::Channel::from_shared(endpoint)?
        .connect()
        .await?;
    Ok(channel)
}

/// Run the interactive REPL.
///
/// 支持 6 个命令：get、put、delete、status、members、help。
/// get/put/delete 使用 kv-store 的 bincode wire 格式。
///
/// ---
///
/// Supports exactly 6 commands: get, put, delete, status, members, help.
/// get/put/delete use the kv-store bincode wire format.
async fn run_repl(addr: &str, group: u64) -> Result<()> {
    let channel = connect(addr).await?;
    let mut admin_client = AdminServiceClient::new(channel.clone());
    let mut kv_client = ClientServiceClient::new(channel);

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
        let tokens: Vec<&str> = line.splitn(3, ' ').collect();
        match tokens[0] {
            "get" => {
                if tokens.len() < 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                let req = KvRequest::Get { key: tokens[1].to_string() };
                let payload = bincode::serde::encode_to_vec(&req, standard())?;
                match kv_client
                    .request(ClientRequest {
                        partition_id: group,
                        payload,
                        idem_key: None,
                    })
                    .await
                {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        if inner.success {
                            match bincode::serde::decode_from_slice::<KvResponse, _>(
                                &inner.payload,
                                standard(),
                            ) {
                                Ok((kv_resp, _)) => match kv_resp.value {
                                    Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                                    None => println!("(nil)"),
                                },
                                Err(e) => println!("decode error: {e}"),
                            }
                        } else {
                            println!("Error: request not successful");
                        }
                    }
                    Err(e) => println!("RPC error: {e}"),
                }
            }
            "put" => {
                if tokens.len() < 3 {
                    println!("Usage: put <key> <value>");
                    continue;
                }
                let req = KvRequest::Put {
                    key: tokens[1].to_string(),
                    value: tokens[2].as_bytes().to_vec(),
                };
                let payload = bincode::serde::encode_to_vec(&req, standard())?;
                match kv_client
                    .request(ClientRequest {
                        partition_id: group,
                        payload,
                        idem_key: None,
                    })
                    .await
                {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("RPC error: {e}"),
                }
            }
            "delete" => {
                if tokens.len() < 2 {
                    println!("Usage: delete <key>");
                    continue;
                }
                let req = KvRequest::Delete { key: tokens[1].to_string() };
                let payload = bincode::serde::encode_to_vec(&req, standard())?;
                match kv_client
                    .request(ClientRequest {
                        partition_id: group,
                        payload,
                        idem_key: None,
                    })
                    .await
                {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("RPC error: {e}"),
                }
            }
            "status" => {
                match admin_client.get_node_status(Empty {}).await {
                    Ok(resp) => {
                        let s = resp.into_inner();
                        println!("node_id    : {}", s.node_id);
                        println!("listen_addr: {}", s.listen_addr);
                        println!("group_ids  : {:?}", s.group_ids);
                        println!("uptime_secs: {}", s.uptime_secs);
                    }
                    Err(e) => println!("RPC error: {e}"),
                }
            }
            "members" => {
                match admin_client.get_group_status(GroupId { group_id: group }).await {
                    Ok(resp) => {
                        let s = resp.into_inner();
                        println!("group_id      : {}", s.group_id);
                        println!("is_leader     : {}", s.is_leader);
                        println!("current_term  : {}", s.current_term);
                        println!("last_log_index: {}", s.last_log_index);
                        println!("leader_node_id: {}", s.leader_node_id);
                    }
                    Err(e) => println!("RPC error: {e}"),
                }
            }
            "help" => {
                println!("Available commands:");
                println!("  get <key>             — fetch a value");
                println!("  put <key> <value>     — store a value");
                println!("  delete <key>          — remove a key");
                println!("  status                — show node status (via admin RPC)");
                println!("  members               — show group membership (via admin RPC)");
                println!("  help                  — show this help");
                println!();
                println!("Note: get/put/delete use kv-store wire format (bincode-encoded KvRequest/KvResponse).");
            }
            other => {
                println!("Unknown command: '{other}'. Type 'help' for available commands.");
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
        assert!(matches!(cli.command, Commands::Node { cmd: NodeCommands::Status }));
    }

    /// Verify group status subcommand parses correctly.
    #[test]
    fn test_parse_group_status() {
        let cli = Cli::try_parse_from(["rr-admin", "group", "status", "--group", "1"])
            .expect("should parse group status");
        assert!(matches!(
            cli.command,
            Commands::Group { cmd: GroupCommands::Status { group: 1 } }
        ));
    }

    /// Verify group snapshot subcommand parses correctly.
    #[test]
    fn test_parse_group_snapshot() {
        let cli = Cli::try_parse_from(["rr-admin", "group", "snapshot", "--group", "2"])
            .expect("should parse group snapshot");
        assert!(matches!(
            cli.command,
            Commands::Group { cmd: GroupCommands::Snapshot { group: 2 } }
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
                cmd: GroupCommands::AddLearner { group: 1, node_id: 4, .. }
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
                cmd: GroupCommands::ChangeMembership { group, voters },
            } => {
                assert_eq!(group, 1);
                assert_eq!(voters, "1,2,3");
            }
            other => panic!("expected ChangeMembership, got {other:?}"),
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
                cmd: GroupCommands::OnlineBackup { group, output, force_fresh },
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
        let cli =
            Cli::try_parse_from(["rr-admin", "group", "membership", "--group", "7"])
                .expect("should parse group membership");
        assert!(matches!(
            cli.command,
            Commands::Group { cmd: GroupCommands::Membership { group: 7 } }
        ));
    }

    #[test]
    fn test_parse_node_reset_requires_force() {
        let cli = Cli::try_parse_from([
            "rr-admin",
            "node-reset",
            "--data-dir",
            "/tmp/rr/data",
        ])
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
        let cli = Cli::try_parse_from(["rr-admin", "repl"])
            .expect("should parse repl");
        assert!(matches!(cli.command, Commands::Repl { group: 1, .. }));
    }

    /// Verify repl subcommand parses --group flag.
    #[test]
    fn test_parse_repl_with_group() {
        let cli = Cli::try_parse_from(["rr-admin", "repl", "--group", "2"])
            .expect("should parse repl --group 2");
        assert!(matches!(cli.command, Commands::Repl { group: 2, .. }));
    }

    /// Verify KvRequest::Put encodes and decodes correctly via bincode.
    #[test]
    fn test_encode_kv_put_roundtrip() {
        let req = KvRequest::Put { key: "hello".to_string(), value: b"world".to_vec() };
        let bytes = bincode::serde::encode_to_vec(&req, standard()).expect("encode");
        let (decoded, _): (KvRequest, _) =
            bincode::serde::decode_from_slice(&bytes, standard()).expect("decode");
        assert!(matches!(decoded, KvRequest::Put { .. }));
        assert!(!bytes.is_empty());
    }

    /// Verify KvRequest::Get encodes to non-empty bytes.
    #[test]
    fn test_encode_kv_get_produces_bytes() {
        let req = KvRequest::Get { key: "mykey".to_string() };
        let bytes = bincode::serde::encode_to_vec(&req, standard()).expect("encode");
        assert!(!bytes.is_empty());
    }

    /// Verify KvRequest::Delete encodes to non-empty bytes.
    #[test]
    fn test_encode_kv_delete_produces_bytes() {
        let req = KvRequest::Delete { key: "mykey".to_string() };
        let bytes = bincode::serde::encode_to_vec(&req, standard()).expect("encode");
        assert!(!bytes.is_empty());
    }
}
