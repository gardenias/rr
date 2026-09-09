use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use rr_client::RrClient;
use serde::Serialize;

mod cluster;
mod metrics;
mod report;
mod soak;
mod workload;

use cluster::{find_bin, Cluster, ClusterConfig, DurabilityKind, StorageKind};
use metrics::MetricsSnapshot;
use workload::ReadKind;

#[derive(Parser, Debug)]
#[command(
    name = "rr-perf-bench",
    about = "Integration perf benchmark: spawn a local rr cluster (1/3/5 processes) or attach to an already-running one via --cluster-addrs; real gRPC traffic"
)]
struct Args {
    /// Scenario(s) to run.
    #[arg(long, value_enum, default_value = "all")]
    scenario: Scenario,

    /// Attach to an already-running cluster (comma-separated host:port list,
    /// e.g. a Docker Compose stack) instead of spawning local rr-server
    /// processes. No binaries are needed and nothing is shut down afterwards.
    #[arg(
        long,
        value_delimiter = ',',
        conflicts_with_all = [
            "members",
            "storage",
            "durability",
            "client_rps",
            "use_tcp_transport",
            "port_base",
            "server_bin",
            "admin_bin",
            "log_servers",
            "data_dir",
        ]
    )]
    cluster_addrs: Option<Vec<String>>,

    /// Number of cluster nodes (voters).
    #[arg(long, default_value = "3", value_parser = parse_members)]
    members: u8,

    /// Warmup duration before measuring (seconds).
    #[arg(long, default_value = "5")]
    warmup_secs: u64,

    /// Measurement window per scenario / per concurrency level (seconds).
    #[arg(long, default_value = "20")]
    duration_secs: u64,

    /// Payload size in bytes sent with each write request.
    #[arg(long, default_value = "128")]
    payload: usize,

    /// Concurrency levels for S2/S3, comma-separated.
    #[arg(long, default_value = "1,10,50,100,200,500")]
    concurrency: String,

    /// Storage backend.
    #[arg(long, value_enum, default_value = "mem")]
    storage: StorageArg,

    /// Durability policy (only relevant when storage=rocks).
    #[arg(long, value_enum, default_value = "strict")]
    durability: DurabilityArg,

    /// Write server and admin logs to files in data-dir instead of discarding them.
    #[arg(long)]
    log_servers: bool,

    /// Base directory for node data and log files.
    /// Defaults to a fresh temporary directory.
    #[arg(long, default_value = "")]
    data_dir: String,

    /// Path to rr-server binary. Defaults to sibling in the same target/ dir.
    #[arg(long, env = "RR_SERVER_BIN")]
    server_bin: Option<PathBuf>,

    /// Path to rr-admin binary. Defaults to sibling in the same target/ dir.
    #[arg(long, env = "RR_ADMIN_BIN")]
    admin_bin: Option<PathBuf>,

    /// Base port for the cluster (node n uses base+(n-1)*10+{1,2,3}).
    #[arg(long, default_value = "47000")]
    port_base: u16,

    /// Use TCP Raft transport for AppendEntries/Vote (snapshot still gRPC).
    #[arg(long)]
    use_tcp_transport: bool,

    /// Optional per-service client-side rate limit (requests/sec) passed to
    /// every rr-server node (`--client-rps`). Use to measure the cluster
    /// under self-protecting backpressure.
    #[arg(long)]
    client_rps: Option<u32>,

    /// Print an interim progress line every N seconds during S1 (elapsed,
    /// ok/err counters, instantaneous TPS since the previous line).
    /// Useful for long runs; S2/S3 already print per-level tables.
    #[arg(long)]
    progress_secs: Option<u64>,

    /// Write a machine-readable JSON report (meta + per-scenario metrics)
    /// to the given path after all selected scenarios have run.
    /// Works with --cluster-addrs (storage/durability are then "external").
    #[arg(long, value_name = "PATH")]
    json_out: Option<PathBuf>,

    // ── Soak scenario (R-8) knobs ─────────────────────────────────────────
    /// [soak] Fixed write rate (keyed idempotent writes/second).
    #[arg(long, default_value = "200")]
    rate: u32,

    /// [soak] Ack-log path (JSONL, fdatasync per record) — the oracle's
    /// ground truth. Required by `--scenario soak`.
    #[arg(long, value_name = "PATH")]
    ack_log: Option<PathBuf>,

    /// [soak] Optional metrics journal path (counters snapshot appended
    /// every --metrics-snapshot-secs).
    #[arg(long, value_name = "PATH")]
    metrics_log: Option<PathBuf>,

    /// [soak] Seconds between linearizable-read probes of the probe key.
    #[arg(long, default_value = "10")]
    read_probe_secs: u64,

    /// [soak] Seconds between metrics-journal records.
    #[arg(long, default_value = "300")]
    metrics_snapshot_secs: u64,

    /// [soak] Max ack-log records the oracle replays (uniform-stride sample
    /// that always includes the first/last 100).
    #[arg(long, default_value = "5000")]
    oracle_sample: usize,

    /// [soak] Skip the load phase and run ONLY the oracle against an
    /// existing ack log (used by the soak harness after cluster convergence).
    #[arg(long)]
    oracle_only: bool,

    /// [soak] Skip the post-load oracle replay.
    #[arg(long)]
    no_oracle: bool,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum Scenario {
    S1,
    S2,
    S3,
    /// R-8 soak: fixed-rate keyed idempotent writes + read probes + ack log
    /// + loss oracle. See soak.rs.
    Soak,
    All,
}

#[derive(Clone, ValueEnum, Debug)]
enum StorageArg {
    Mem,
    Rocks,
}

#[derive(Clone, ValueEnum, Debug)]
enum DurabilityArg {
    Strict,
    ReplicatedOnly,
    TestOnly,
}

fn parse_members(s: &str) -> Result<u8, String> {
    match s.parse::<u8>() {
        Ok(v @ (1 | 3 | 5)) => Ok(v),
        _ => Err(format!("expected 1, 3 or 5, got '{s}'")),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Client-side diagnostics (retry/redirect decisions) are invisible without
    // a subscriber. Default to `warn`; override with RUST_LOG, e.g.
    // `RUST_LOG=rr_client=debug`.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let args = Args::parse();

    let concurrency_levels: Vec<usize> = args
        .concurrency
        .split(',')
        .map(|s| s.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
        .context("--concurrency must be comma-separated integers")?;

    let warmup = Duration::from_secs(args.warmup_secs);
    let duration = Duration::from_secs(args.duration_secs);
    let progress = args.progress_secs.map(Duration::from_secs);

    println!("=== rr-perf-bench ===");
    println!("  payload={}B", args.payload);
    println!(
        "  warmup={}s  measure={}s",
        args.warmup_secs, args.duration_secs
    );

    let (client, cluster, storage_label, durability_label) = match &args.cluster_addrs {
        Some(addrs) => {
            let addrs: Vec<String> = addrs.iter().map(|a| a.trim().to_string()).collect();
            println!("  mode: external cluster ({})", addrs.join(", "));
            println!();

            let client = Arc::new(
                RrClient::connect(addrs)
                    .await
                    .context("RrClient connect failed")?,
            );
            // Storage/durability are unknown on a cluster we didn't spawn.
            (client, None, "external", "external")
        }
        None => {
            let server_bin = match args.server_bin {
                Some(p) => p,
                None => find_bin("rr-server", "RR_SERVER_BIN")?,
            };
            let admin_bin = match args.admin_bin {
                Some(p) => p,
                None => find_bin("rr-admin", "RR_ADMIN_BIN")?,
            };

            let storage = match args.storage {
                StorageArg::Mem => StorageKind::Mem,
                StorageArg::Rocks => StorageKind::Rocks,
            };
            let durability = match args.durability {
                DurabilityArg::Strict => DurabilityKind::Strict,
                DurabilityArg::ReplicatedOnly => DurabilityKind::ReplicatedOnly,
                DurabilityArg::TestOnly => DurabilityKind::TestOnly,
            };
            let storage_label = storage.as_str();
            let durability_label = durability.as_str();

            let cfg = ClusterConfig {
                server_bin,
                admin_bin,
                data_dir: PathBuf::from(&args.data_dir),
                storage,
                durability,
                log_to_files: args.log_servers,
                members: args.members,
                port_base: args.port_base,
                use_tcp_transport: args.use_tcp_transport,
                client_rps: args.client_rps,
            };

            println!(
                "  mode: spawned cluster  storage={:?}  durability={:?}",
                cfg.storage, cfg.durability
            );
            println!();
            print!("Starting {}-node cluster … ", cfg.members);

            let cluster = Cluster::start(&cfg)
                .await
                .context("cluster startup failed")?;
            let addrs = cluster.all_grpc_addrs();

            println!("done");
            println!("  nodes: {}", addrs.join(", "));

            let client = Arc::new(
                RrClient::connect(addrs)
                    .await
                    .context("RrClient connect failed")?,
            );
            (client, Some(cluster), storage_label, durability_label)
        }
    };

    let mut s1_snap: Option<MetricsSnapshot> = None;
    match args.scenario {
        Scenario::S1 | Scenario::All => {
            if progress.is_some() {
                println!("S1 serial …");
            } else {
                print!("S1 serial … ");
            }
            let m = workload::run_s1_serial(
                Arc::clone(&client),
                args.payload,
                duration,
                warmup,
                progress,
            )
            .await;
            let snap = m.snapshot();
            report::print_s1(&snap, args.payload);
            s1_snap = Some(snap);
        }
        _ => {}
    }

    let mut s2_results: Vec<(usize, MetricsSnapshot)> = Vec::new();
    match args.scenario {
        Scenario::S2 | Scenario::All => {
            report::print_s2_header(args.payload, args.duration_secs);
            let results = workload::run_s2_pipeline_sweep(
                Arc::clone(&client),
                args.payload,
                &concurrency_levels,
                duration,
                warmup,
            )
            .await;
            for (k, snap) in &results {
                report::print_s2_row(*k, snap);
            }
            s2_results = results;
        }
        _ => {}
    }

    let mut s3_results: Vec<(ReadKind, usize, MetricsSnapshot)> = Vec::new();
    match args.scenario {
        Scenario::S3 | Scenario::All => {
            report::print_s3_header(args.payload, args.duration_secs);
            let results = workload::run_s3_read_sweep(
                Arc::clone(&client),
                args.payload,
                &concurrency_levels,
                duration,
                warmup,
            )
            .await;
            for (kind, k, snap) in &results {
                report::print_s3_row(*kind, *k, snap);
            }
            s3_results = results;
        }
        _ => {}
    }

    let mut soak_summary: Option<soak::SoakSummary> = None;
    if args.scenario == Scenario::Soak {
        let ack_log = args
            .ack_log
            .clone()
            .context("--scenario soak requires --ack-log <path>")?;
        let cfg = soak::SoakConfig {
            rate_per_sec: args.rate,
            duration,
            payload_size: args.payload,
            ack_log,
            metrics_log: args.metrics_log.clone(),
            read_probe_interval: Duration::from_secs(args.read_probe_secs),
            metrics_interval: Duration::from_secs(args.metrics_snapshot_secs),
            oracle_sample: args.oracle_sample,
            skip_oracle: args.no_oracle || args.oracle_only,
        };

        if args.oracle_only {
            println!(
                "soak oracle replay: {} (sample ≤ {})",
                cfg.ack_log.display(),
                cfg.oracle_sample
            );
            let oracle = soak::run_oracle(Arc::clone(&client), &cfg).await?;
            println!(
                "soak oracle: {}/{} verified in {:.1}s",
                oracle.verified, oracle.checked, oracle.elapsed_secs
            );
            soak_summary = Some(soak::SoakSummary {
                duration_secs: 0,
                rate_per_sec: args.rate,
                sent: 0,
                acked: 0,
                failed: 0,
                reads_ok: 0,
                reads_failed: 0,
                ack_log_records: oracle.sampled_from,
                oracle: Some(oracle),
            });
        } else {
            println!(
                "soak: {} writes/s for {}s, ack log {}",
                cfg.rate_per_sec,
                cfg.duration.as_secs(),
                cfg.ack_log.display()
            );
            let mut summary = soak::run_soak_load(Arc::clone(&client), &cfg).await?;
            println!(
                "soak load done: sent={} acked={} failed={} reads_ok={} reads_failed={}",
                summary.sent, summary.acked, summary.failed, summary.reads_ok, summary.reads_failed
            );
            if !cfg.skip_oracle {
                let oracle = soak::run_oracle(Arc::clone(&client), &cfg).await?;
                println!(
                    "soak oracle: {}/{} verified in {:.1}s",
                    oracle.verified, oracle.checked, oracle.elapsed_secs
                );
                summary.oracle = Some(oracle);
            }
            soak_summary = Some(summary);
        }
    }

    // Machine-readable report — written before cluster shutdown so a write
    // failure surfaces while the cluster is still up for inspection.
    if let Some(path) = &args.json_out {
        let report = JsonReport {
            meta: JsonMeta {
                tool: "rr-perf-bench",
                version: env!("CARGO_PKG_VERSION"),
                git_sha: cmd_stdout("git", &["rev-parse", "HEAD"], &workspace_root()),
                rustc: cmd_stdout("rustc", &["--version"], &workspace_root()),
                os: std::env::consts::OS,
                arch: std::env::consts::ARCH,
                storage: storage_label,
                durability: durability_label,
                payload_bytes: args.payload,
                warmup_secs: args.warmup_secs,
                duration_secs: args.duration_secs,
                concurrency_levels: concurrency_levels.clone(),
                captured_unix_secs: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            },
            s1: s1_snap,
            s2: s2_results
                .into_iter()
                .map(|(concurrency, snap)| S2Entry { concurrency, snap })
                .collect(),
            s3: s3_results
                .into_iter()
                .map(|(kind, concurrency, snap)| S3Entry {
                    kind: kind.as_str(),
                    concurrency,
                    snap,
                })
                .collect(),
            soak: soak_summary,
        };
        let json = serde_json::to_string_pretty(&report).context("serialize JSON report")?;
        std::fs::write(path, json)
            .with_context(|| format!("write JSON report to {}", path.display()))?;
        println!("JSON report written to {}", path.display());
    }

    println!();
    if let Some(cluster) = cluster {
        println!("Shutting down cluster …");
        cluster.shutdown().await;
        println!("done.");
    }

    Ok(())
}

/// Top-level machine-readable report written by `--json-out`.
#[derive(Serialize)]
struct JsonReport {
    meta: JsonMeta,
    /// S1 snapshot; `null` unless scenario S1 ran.
    s1: Option<MetricsSnapshot>,
    s2: Vec<S2Entry>,
    s3: Vec<S3Entry>,
    /// Soak summary; `null` unless `--scenario soak` ran.
    soak: Option<soak::SoakSummary>,
}

#[derive(Serialize)]
struct JsonMeta {
    tool: &'static str,
    version: &'static str,
    git_sha: String,
    rustc: String,
    os: &'static str,
    arch: &'static str,
    /// "external" when attached via --cluster-addrs.
    storage: &'static str,
    durability: &'static str,
    payload_bytes: usize,
    warmup_secs: u64,
    duration_secs: u64,
    concurrency_levels: Vec<usize>,
    captured_unix_secs: u64,
}

/// S2 row: snapshot fields are flattened into the entry object.
#[derive(Serialize)]
struct S2Entry {
    concurrency: usize,
    #[serde(flatten)]
    snap: MetricsSnapshot,
}

/// S3 row: `kind` is the lowercase read-kind string used in the text table.
#[derive(Serialize)]
struct S3Entry {
    kind: &'static str,
    concurrency: usize,
    #[serde(flatten)]
    snap: MetricsSnapshot,
}

/// Workspace root, derived from this crate's compile-time manifest dir
/// (`tools/rr-perf-bench` → `../..`).
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Best-effort capture of a command's trimmed stdout; `"unknown"` on any
/// failure (missing binary, non-zero exit, non-UTF8 or empty output).
/// Blocking std::process is fine here: it runs once, after all scenarios.
fn cmd_stdout(cmd: &str, args: &[&str], cwd: &Path) -> String {
    std::process::Command::new(cmd)
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}
