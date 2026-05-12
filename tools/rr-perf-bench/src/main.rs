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

use cluster::{Cluster, ClusterConfig, DurabilityKind, StorageKind, find_bin};

#[derive(Parser, Debug)]
#[command(
    name = "rr-perf-bench",
    about = "Integration perf benchmark: 3-process rr cluster, real gRPC traffic"
)]
struct Args {
    /// Scenario(s) to run.
    #[arg(long, value_enum, default_value = "all")]
    scenario: Scenario,

    /// Warmup duration before measuring (seconds).
    #[arg(long, default_value = "5")]
    warmup_secs: u64,

    /// Measurement window per scenario / per concurrency level (seconds).
    #[arg(long, default_value = "20")]
    duration_secs: u64,

    /// Payload size in bytes sent with each write request.
    #[arg(long, default_value = "128")]
    payload: usize,

    /// Concurrency levels for S2, comma-separated.
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

    /// Base port for the 3-node cluster (node n uses base+(n-1)*10+{1,2,3}).
    #[arg(long, default_value = "47000")]
    port_base: u16,

    /// Use TCP Raft transport for AppendEntries/Vote (snapshot still gRPC).
    #[arg(long)]
    use_tcp_transport: bool,
}

#[derive(Clone, ValueEnum, Debug)]
enum Scenario {
    S1,
    S2,
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

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

    let concurrency_levels: Vec<usize> = args
        .concurrency
        .split(',')
        .map(|s| s.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
        .context("--concurrency must be comma-separated integers")?;

    let warmup = Duration::from_secs(args.warmup_secs);
    let duration = Duration::from_secs(args.duration_secs);

    let cfg = ClusterConfig {
        server_bin,
        admin_bin,
        data_dir: PathBuf::from(&args.data_dir),
        storage,
        durability,
        log_to_files: args.log_servers,
        port_base: args.port_base,
        use_tcp_transport: args.use_tcp_transport,
    };

    println!("=== rr-perf-bench ===");
    println!(
        "  storage={:?}  durability={:?}  payload={}B",
        cfg.storage, cfg.durability, args.payload
    );
    println!("  warmup={}s  measure={}s", args.warmup_secs, args.duration_secs);
    println!();
    print!("Starting 3-node cluster … ");

    let cluster = Cluster::start(&cfg).await.context("cluster startup failed")?;
    let addrs = cluster.all_grpc_addrs();

    println!("done");
    println!("  nodes: {}", addrs.join(", "));

    let client = Arc::new(
        RrClient::connect(addrs)
            .await
            .context("RrClient connect failed")?,
    );

    match args.scenario {
        Scenario::S1 | Scenario::All => {
            print!("S1 serial … ");
            let m = workload::run_s1_serial(
                Arc::clone(&client),
                args.payload,
                duration,
                warmup,
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

    println!();
    println!("Shutting down cluster …");
    cluster.shutdown().await;
    println!("done.");

    Ok(())
}
