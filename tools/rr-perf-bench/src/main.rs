use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use rr_client::RrClient;

mod cluster;
mod metrics;
mod report;
mod workload;

use cluster::{find_bin, Cluster, ClusterConfig, DurabilityKind, StorageKind};

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
}

#[derive(Clone, ValueEnum, Debug)]
enum Scenario {
    S1,
    S2,
    S3,
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

    let (client, cluster) = match &args.cluster_addrs {
        Some(addrs) => {
            let addrs: Vec<String> = addrs.iter().map(|a| a.trim().to_string()).collect();
            println!("  mode: external cluster ({})", addrs.join(", "));
            println!();

            let client = Arc::new(
                RrClient::connect(addrs)
                    .await
                    .context("RrClient connect failed")?,
            );
            (client, None)
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
            (client, Some(cluster))
        }
    };

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
            report::print_s1(&m.snapshot(), args.payload);
        }
        _ => {}
    }

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
        }
        _ => {}
    }

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
        }
        _ => {}
    }

    println!();
    if let Some(cluster) = cluster {
        println!("Shutting down cluster …");
        cluster.shutdown().await;
        println!("done.");
    }

    Ok(())
}
