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
    DrainRequest, Empty, GroupId, OnlineBackupRequest, ResetIngestorOffsetRequest,
    TransferLeaderRequest,
};
use rr_network::proto::client::client_service_client::ClientServiceClient;
use rr_network::proto::client::ClientRequest;
use std::path::PathBuf;
use tonic::{transport::Channel, Code, Response, Status};

mod backup;
mod decommission;
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
    /// Configuration inspection commands.
    Config {
        #[command(subcommand)]
        cmd: ConfigCommands,
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
            let channel = connect(&cli.addr).await?;
            let mut client = AdminServiceClient::new(channel);

            match &cli.command {
                Commands::Node {
                    cmd: NodeCommands::Decommission { node_id, dry_run },
                } => {
                    let report =
                        decommission::decommission_node(&mut client, *node_id, *dry_run).await?;
                    match cli.output {
                        OutputFormat::Json => {
                            println!("{}", serde_json::to_string_pretty(&report)?);
                        }
                        OutputFormat::Human => {
                            println!(
                                "Decommission Report (target_node_id={}, dry_run={})",
                                report.target_node_id, report.dry_run
                            );
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
                                    "FAILURES present — investigate and re-run; \
                                     change_membership is idempotent so partial progress is safe"
                                );
                            } else if report.made_progress() {
                                println!();
                                if report.dry_run {
                                    println!("(dry-run) re-run without --dry-run to apply.");
                                } else {
                                    println!("All eligible groups processed successfully.");
                                }
                            } else {
                                println!();
                                println!(
                                    "No-op: target node was not a voter in any reported group."
                                );
                            }
                        }
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

/// Run the interactive REPL.
///
/// 支持 7 个命令：get、put、delete、status、members、help、reconnect。
/// get/put/delete 使用 kv-store 的 bincode wire 格式。
///
/// ---
///
/// Supports 7 commands: get, put, delete, status, members, help, reconnect.
/// get/put/delete use the kv-store bincode wire format.
async fn run_repl(addr: &str, group: u64) -> Result<()> {
    let channel = connect(addr).await?;
    let mut admin_client = AdminServiceClient::new(channel.clone());
    let mut kv_client = ClientServiceClient::new(channel);
    let mut needs_reconnect = false;

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
            match connect(addr).await {
                Ok(ch) => {
                    admin_client = AdminServiceClient::new(ch.clone());
                    kv_client = ClientServiceClient::new(ch);
                    needs_reconnect = false;
                    if line.trim() == "reconnect" {
                        println!("reconnected to {addr}");
                        continue;
                    }
                }
                Err(e) => {
                    println!("reconnect failed: {e}  (will retry on next command)");
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
                            match decode_kv_response(&inner.payload) {
                                Ok(Some(s)) => println!("{s}"),
                                Ok(None) => println!("(nil)"),
                                Err(e) => println!("decode error: {e}"),
                            }
                        } else {
                            println!("Error: request not successful");
                        }
                    }
                    Err(e) => {
                        needs_reconnect = true;
                        println!("RPC error: {e}");
                    }
                }
            }
            ReplCommand::Put { key, value } => {
                let req = KvRequest::Put {
                    key,
                    value: value.as_bytes().to_vec(),
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
                    Err(e) => {
                        needs_reconnect = true;
                        println!("RPC error: {e}");
                    }
                }
            }
            ReplCommand::Delete { key } => {
                let req = KvRequest::Delete { key };
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
                    Err(e) => {
                        needs_reconnect = true;
                        println!("RPC error: {e}");
                    }
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
                Err(e) => {
                    needs_reconnect = true;
                    println!("RPC error: {e}");
                }
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
                        needs_reconnect = true;
                        println!("RPC error: {e}");
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
                println!("  help                  — show this help");
                println!();
                println!(
                    "Note: get/put/delete use kv-store wire format \
                     (bincode-encoded KvRequest/KvResponse)."
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
}
