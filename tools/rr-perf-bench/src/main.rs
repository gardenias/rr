use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use rr_client::{RrClient, RrClientBuilder};
use rr_network::tls::TlsConfig;
use serde::Serialize;

mod cluster;
mod metrics;
mod openloop;
mod prom;
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
    /// [soak|mix] Fixed rate: soak keyed idempotent writes/sec; mix total
    /// ops/sec (open-loop).
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

    /// [soak] Writer lanes (idempotency namespace shards). A single writer is
    /// closed-loop serial (~1/RTT, far below the nominal rate); lanes keep
    /// per-namespace monotonicity while the aggregate reaches --rate.
    #[arg(long, default_value = "32")]
    lanes: u32,

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

    // ── Open-loop scenarios (mix / wladder, rr-prod-perf M2) ────────────────
    // These knobs apply ONLY to `--scenario mix|wladder`; they are ignored
    // (with a note) for S1/S2/S3/soak, whose flags and output are unchanged.
    /// [open-loop] Planned-time scheduling is implied for mix/wladder (they
    /// have no closed-loop mode); the flag exists so scripts read as
    /// self-documenting (plan §5.2 CLI surface).
    #[arg(long)]
    open_loop: bool,

    /// [open-loop] Poisson arrivals (exponential inter-arrival) instead of a
    /// constant-rate grid. The arrival stream is seeded via --seed.
    #[arg(long)]
    poisson: bool,

    /// [open-loop] Operation mix, e.g. "write:90,local:5,lin:5". Weights are
    /// relative; kinds: write (idempotent Put), local (local read), lin
    /// (linearizable read). Default: write:100.
    #[arg(long, value_name = "SPEC")]
    mix: Option<String>,

    /// [open-loop] Number of distinct KV keys.
    #[arg(long, default_value = "1000000")]
    key_space: u64,

    /// [open-loop] Key selection distribution. `zipfian` uses --zipf-theta.
    #[arg(long, value_enum)]
    key_dist: Option<KeyDistArg>,

    /// [open-loop] Zipfian skew parameter (W4 uses 0.99).
    #[arg(long, default_value = "0.99")]
    zipf_theta: f64,

    /// [open-loop] Client identifier recorded in the JSON report meta —
    /// aggregation key when several load containers fan out (W1 large rate
    /// tiers).
    #[arg(long, default_value = "0")]
    client_id: u32,

    /// [open-loop] Seed for arrival/op/key streams (SplitMix64). Same seed +
    /// same flags reproduce the same offered-load trace.
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,

    /// [open-loop] Cap on simultaneously in-flight ops. On saturation the
    /// firing loop waits for a slot and the slip is recorded as schedule lag
    /// (honest backpressure, no silent load shedding).
    #[arg(long, default_value = "32768")]
    max_in_flight: usize,

    /// [open-loop|soak] Per-op deadline (seconds). A slow op past this
    /// deadline is counted as an error and its in-flight slot freed, bounding
    /// the drain on a saturated cluster (open-loop) and preventing a wedged
    /// RPC from hanging the soak (tonic has no default RPC deadline).
    /// Default 30s = the HDR histogram ceiling.
    #[arg(long, default_value = "30")]
    op_timeout_secs: u64,

    /// [wladder] Step-up rate rungs (ops/s), comma-separated; each rung runs
    /// for --duration-secs. Warmup runs once at the first rung's rate.
    #[arg(long, default_value = "100,500,1000,2000,4000,8000,16000,32000")]
    ladder_rates: String,

    /// [open-loop] Write a 1s-bucket time series (JSONL: t, op, count, err,
    /// p50_us, p99_us, plus rung for wladder) to the given path.
    #[arg(long, value_name = "PATH")]
    timeseries_out: Option<PathBuf>,

    /// Embed a server-side Prometheus summary of the measurement window into
    /// the JSON report (mix/wladder/soak only). Queries run at window end
    /// against the raw histogram/counter families; failures degrade fields
    /// to null with a note, never fail the run. Example:
    /// --prom-url http://prometheus:9090
    #[arg(long, value_name = "URL")]
    prom_url: Option<String>,

    // ── TLS / auth client (rr-prod-perf M2, item 5) ─────────────────────────
    /// CA bundle for verifying the server certificate. The
    /// --tls-ca/--tls-cert/--tls-key trio is all-or-nothing (same convention
    /// as kv-store): with any one present, all are required. Without them the
    /// client speaks plaintext (unchanged default).
    #[arg(long, value_name = "PATH")]
    tls_ca: Option<PathBuf>,

    /// Client certificate presented for mTLS.
    #[arg(long, value_name = "PATH")]
    tls_cert: Option<PathBuf>,

    /// Client private key for --tls-cert.
    #[arg(long, value_name = "PATH")]
    tls_key: Option<PathBuf>,

    /// SNI / expected server name override when dialing by address that does
    /// not match the certificate SAN. Not needed in the perf-lab (nodes are
    /// dialed by their kv-N hostnames, which are the SANs).
    #[arg(long, value_name = "NAME")]
    tls_domain: Option<String>,

    /// Name of the environment variable holding the bearer token (e.g.
    /// RR_CLIENT_TOKEN). Secrets stay off argv — same convention as
    /// rr-server's `auth.*_token_env`. Fails fast if unset or empty.
    #[arg(long, value_name = "ENV_VAR")]
    token_env: Option<String>,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum Scenario {
    S1,
    S2,
    S3,
    /// R-8 soak: fixed-rate keyed idempotent writes + read probes + ack log
    /// + loss oracle. See soak.rs.
    Soak,
    /// Open-loop fixed-rate load with an operation mix (rr-prod-perf W2–W5).
    /// Speaks the kv-store KV wire protocol — do not run against EchoSM
    /// (spawned rr-server clusters); use S1/S2/S3 there.
    Mix,
    /// Open-loop step-up rate ladder (rr-prod-perf W1): --ladder-rates rungs
    /// of --duration-secs each, one continuous schedule. Same wire protocol
    /// constraint as mix.
    Wladder,
    All,
}

#[derive(Clone, Copy, ValueEnum, Debug)]
enum KeyDistArg {
    Uniform,
    Zipfian,
}

/// Default seed for the open-loop arrival/op/key streams.
const DEFAULT_SEED: u64 = 0x5EED_5EED_5EED_5EED;

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

    // Open-loop knobs are silently meaningless for the closed-loop scenarios —
    // say so instead of letting a typo'd scenario flag eat them.
    if !matches!(args.scenario, Scenario::Mix | Scenario::Wladder) {
        let mut ignored: Vec<&str> = Vec::new();
        if args.open_loop {
            ignored.push("--open-loop");
        }
        if args.poisson {
            ignored.push("--poisson");
        }
        if args.mix.is_some() {
            ignored.push("--mix");
        }
        if args.key_dist.is_some() {
            ignored.push("--key-dist");
        }
        if args.client_id != 0 {
            ignored.push("--client-id");
        }
        if args.seed != DEFAULT_SEED {
            ignored.push("--seed");
        }
        if args.timeseries_out.is_some() {
            ignored.push("--timeseries-out");
        }
        if !ignored.is_empty() {
            eprintln!(
                "[rr-perf-bench] note: {} apply to --scenario mix|wladder only; ignoring",
                ignored.join(" ")
            );
        }
    }

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

            let client = Arc::new(connect_client(addrs, &args).await?);
            // Storage/durability are unknown on a cluster we didn't spawn.
            (client, None, "external", "external")
        }
        None => {
            let server_bin = match &args.server_bin {
                Some(p) => p.clone(),
                None => find_bin("rr-server", "RR_SERVER_BIN")?,
            };
            let admin_bin = match &args.admin_bin {
                Some(p) => p.clone(),
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
            if args.tls_ca.is_some() || args.token_env.is_some() {
                eprintln!(
                    "[rr-perf-bench] note: --tls-*/--token-env are meant for --cluster-addrs \
                     mode; spawned rr-server clusters are plaintext"
                );
            }
            println!();
            print!("Starting {}-node cluster … ", cfg.members);

            let cluster = Cluster::start(&cfg)
                .await
                .context("cluster startup failed")?;
            let addrs = cluster.all_grpc_addrs();

            println!("done");
            println!("  nodes: {}", addrs.join(", "));

            let client = Arc::new(connect_client(addrs, &args).await?);
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

    // ── Open-loop scenarios (rr-prod-perf M2): mix = one fixed-rate window,
    // wladder = step-up rungs. One engine, one code path (openloop.rs).
    let mut mix_report: Option<openloop::ScenarioReport> = None;
    let mut wladder_report: Option<openloop::ScenarioReport> = None;
    if matches!(args.scenario, Scenario::Mix | Scenario::Wladder) {
        let is_ladder = args.scenario == Scenario::Wladder;
        let mix = openloop::MixSpec::parse(args.mix.as_deref().unwrap_or("write:100"))?;
        let key_dist = match args.key_dist {
            None | Some(KeyDistArg::Uniform) => openloop::KeyDist::Uniform,
            Some(KeyDistArg::Zipfian) => openloop::KeyDist::Zipfian {
                theta: args.zipf_theta,
            },
        };
        let rates = if is_ladder {
            args.ladder_rates
                .split(',')
                .map(|s| s.trim().parse::<u32>())
                .collect::<Result<Vec<_>, _>>()
                .context("--ladder-rates must be comma-separated positive integers")?
        } else {
            vec![args.rate]
        };
        let cfg = openloop::OpenLoopConfig {
            rates,
            rung_duration: duration,
            warmup,
            poisson: args.poisson,
            mix,
            key_space: args.key_space,
            key_dist,
            payload_size: args.payload,
            client_id: args.client_id,
            seed: args.seed,
            max_in_flight: args.max_in_flight,
            op_timeout: Duration::from_secs(args.op_timeout_secs),
            timeseries: args.timeseries_out.is_some(),
        };

        let label = if is_ladder { "WLADDER" } else { "MIX" };
        println!(
            "{label} open-loop … rates={:?} rung={}s warmup={}s",
            cfg.rates,
            cfg.rung_duration.as_secs(),
            cfg.warmup.as_secs(),
        );
        let outcome = openloop::run(Arc::clone(&client), &cfg).await?;
        let mut report = outcome.report;
        if let Some(prom_url) = &args.prom_url {
            let start = report.started_unix_secs;
            let end = start + report.rung_secs * report.rates.len() as u64;
            report.server_side = Some(prom::capture(prom_url, start, end));
        }
        report::print_openloop(label, &report);
        if let Some(path) = &args.timeseries_out {
            std::fs::write(path, &outcome.timeseries_jsonl)
                .with_context(|| format!("write time series to {}", path.display()))?;
            println!("time series written to {}", path.display());
        }
        if is_ladder {
            wladder_report = Some(report);
        } else {
            mix_report = Some(report);
        }
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
            lanes: args.lanes,
            oracle_sample: args.oracle_sample,
            skip_oracle: args.no_oracle || args.oracle_only,
            op_timeout: Duration::from_secs(args.op_timeout_secs),
        };

        if args.oracle_only {
            println!(
                "soak oracle replay: {} (sample ≤ {})",
                cfg.ack_log.display(),
                cfg.oracle_sample
            );
            let oracle = soak::run_oracle(Arc::clone(&client), &cfg).await?;
            println!(
                "soak oracle: {}/{} verified in {:.1}s (replay {} + survival-only {})",
                oracle.verified,
                oracle.checked,
                oracle.elapsed_secs,
                oracle.replay_verified,
                oracle.survival_only
            );
            soak_summary = Some(soak::SoakSummary {
                duration_secs: 0,
                rate_per_sec: args.rate,
                lanes: args.lanes,
                sent: 0,
                acked: 0,
                failed: 0,
                reads_ok: 0,
                reads_failed: 0,
                ack_log_records: oracle.sampled_from,
                started_unix_secs: None,
                server_side: None,
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
                    "soak oracle: {}/{} verified in {:.1}s (replay {} + survival-only {})",
                    oracle.verified,
                    oracle.checked,
                    oracle.elapsed_secs,
                    oracle.replay_verified,
                    oracle.survival_only
                );
                summary.oracle = Some(oracle);
            }
            // Server-side window covers the LOAD phase (oracle replay is
            // post-window; its traffic is deliberately excluded).
            if let (Some(prom_url), Some(start)) = (&args.prom_url, summary.started_unix_secs) {
                summary.server_side = Some(prom::capture(
                    prom_url,
                    start,
                    start + summary.duration_secs,
                ));
            }
            soak_summary = Some(summary);
        }
    }

    // --prom-url only has a window to query for mix/wladder/soak.
    if args.prom_url.is_some()
        && !matches!(
            args.scenario,
            Scenario::Mix | Scenario::Wladder | Scenario::Soak
        )
    {
        eprintln!("[rr-perf-bench] note: --prom-url applies to mix/wladder/soak only; ignoring");
    }

    // Machine-readable report — written before cluster shutdown so a write
    // failure surfaces while the cluster is still up for inspection.
    if let Some(path) = &args.json_out {
        let openloop_ran = mix_report.is_some() || wladder_report.is_some();
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
                client_id: openloop_ran.then_some(args.client_id),
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
            mix: mix_report,
            wladder: wladder_report,
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
    /// Open-loop mix report; key ABSENT unless `--scenario mix` ran (additive
    /// schema: S1/S2/S3/soak consumers see a byte-identical key set).
    #[serde(skip_serializing_if = "Option::is_none")]
    mix: Option<openloop::ScenarioReport>,
    /// Open-loop ladder report; key absent unless `--scenario wladder` ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    wladder: Option<openloop::ScenarioReport>,
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
    /// Present only when an open-loop scenario ran; labels multi-client
    /// fan-out reports for aggregation.
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<u32>,
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

/// Build the cluster client. Plaintext by default (unchanged); the
/// `--tls-ca/--tls-cert/--tls-key` trio (all-or-nothing, same convention as
/// kv-store) switches every channel to TLS/mTLS, and `--token-env` attaches
/// a bearer token read from the named environment variable — secrets stay
/// off argv, same convention as rr-server's `auth.*_token_env`.
async fn connect_client(addrs: Vec<String>, args: &Args) -> Result<RrClient> {
    let any_tls = args.tls_ca.is_some() || args.tls_cert.is_some() || args.tls_key.is_some();
    let mut builder = RrClientBuilder::new(addrs);
    if any_tls {
        let tls = TlsConfig {
            cert_path: args
                .tls_cert
                .clone()
                .context("--tls-cert is required together with --tls-ca/--tls-key")?,
            key_path: args
                .tls_key
                .clone()
                .context("--tls-key is required together with --tls-ca/--tls-cert")?,
            ca_path: Some(
                args.tls_ca
                    .clone()
                    .context("--tls-ca is required together with --tls-cert/--tls-key")?,
            ),
            // Server-side knob, ignored on client configs (rr-network docs).
            require_client_cert: false,
            domain_name: args.tls_domain.clone(),
        };
        builder = builder.with_tls(tls);
    } else if args.tls_domain.is_some() {
        anyhow::bail!("--tls-domain requires the --tls-ca/--tls-cert/--tls-key trio");
    }
    if let Some(env_var) = &args.token_env {
        let token = std::env::var(env_var)
            .ok()
            .filter(|t| !t.is_empty())
            .with_context(|| format!("--token-env {env_var}: variable not set or empty"))?;
        builder = builder.with_bearer_token(token);
    }
    builder.build().await.context("RrClient connect failed")
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
