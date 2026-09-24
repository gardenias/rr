//! CLI definitions for the `rr-server` binary.
//!
//! Argument resolution order (highest precedence first):
//! 1. CLI flags
//! 2. Environment variables (`RR_*` prefix)
//! 3. Config file fields
//! 4. Built-in defaults

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

/// Distributed multi-Raft consensus node server.
#[derive(Debug, Parser)]
#[command(
    name = "rr-server",
    version,
    about = "rr distributed multi-Raft consensus framework node",
    long_about = None,
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

// `Serve(ServeArgs)` is materially larger than the other variants, but the
// CLI is parsed exactly once at process startup, so boxing the args would
// trade allocation for no perceptible benefit. Keep the inline variant.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start an rr node and serve gRPC + metrics.
    Serve(ServeArgs),
    /// Print version information and exit.
    Version,
}

/// Arguments accepted by the `serve` subcommand.
#[derive(Debug, Args, Default, Clone)]
pub struct ServeArgs {
    /// Path to a TOML config file. All other flags override values from this file.
    #[arg(short = 'c', long = "config", env = "RR_CONFIG")]
    pub config: Option<PathBuf>,

    /// Node identifier (u64, must be unique within the cluster).
    #[arg(long = "node-id", env = "RR_NODE_ID")]
    pub node_id: Option<u64>,

    /// gRPC bind address for Client + Raft services (e.g. `0.0.0.0:51001`).
    #[arg(long = "listen-addr", env = "RR_LISTEN_ADDR")]
    pub listen_addr: Option<String>,

    /// Data directory. Required when `--storage rocks` is selected.
    #[arg(long = "data-dir", env = "RR_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Storage backend: `mem` (ephemeral) or `rocks` (persistent, feature-gated).
    #[arg(long = "storage", env = "RR_STORAGE")]
    pub storage: Option<String>,

    /// RocksDB durability policy: `strict` (default), `replicated_only`, or
    /// `test_only`. Weakening this below `strict` breaks OpenRaft's durability
    /// contract. Deprecated aliases `balanced` → `replicated_only` and
    /// `fast` → `test_only` still parse but emit a startup warning.
    #[arg(long = "durability", env = "RR_DURABILITY")]
    pub durability: Option<String>,

    /// Admin gRPC bind port (separate from the main listen_addr). Omit to disable.
    #[arg(long = "admin-port", env = "RR_ADMIN_PORT")]
    pub admin_port: Option<u16>,

    /// Admin gRPC bind host (IP literal). Defaults to `127.0.0.1` — the admin
    /// API can transfer leadership and trigger snapshots, so exposing it
    /// cluster-wide requires an explicit `0.0.0.0` here.
    #[arg(long = "admin-bind-host", env = "RR_ADMIN_BIND_HOST")]
    pub admin_bind_host: Option<String>,

    /// Prometheus `/metrics` HTTP bind port. Omit to disable.
    #[arg(long = "metrics-port", env = "RR_METRICS_PORT")]
    pub metrics_port: Option<u16>,

    /// Metrics/health HTTP bind host (IP literal). Defaults to `127.0.0.1` —
    /// `/metrics` is unauthenticated and exposes internal histograms. Set
    /// `0.0.0.0` to allow cross-host Prometheus scrapes.
    #[arg(long = "metrics-bind-host", env = "RR_METRICS_BIND_HOST")]
    pub metrics_bind_host: Option<String>,

    /// Override the gRPC server's HTTP/2 max_concurrent_streams (tonic/hyper
    /// default: 200). Perf-tuning knob; raise for unbatched high-concurrency
    /// client paths.
    #[arg(
        long = "grpc-max-concurrent-streams",
        env = "RR_GRPC_MAX_CONCURRENT_STREAMS"
    )]
    pub grpc_max_concurrent_streams: Option<u32>,

    /// Override the gRPC server's HTTP/2 max frame size in bytes (default
    /// 16384). Perf-tuning knob for large (batched) payloads.
    #[arg(long = "grpc-max-frame-size", env = "RR_GRPC_MAX_FRAME_SIZE")]
    pub grpc_max_frame_size: Option<u32>,

    /// Internal Raft transport for AppendEntries/Vote: `grpc` (default) or `tcp`.
    /// `tcp` is a plaintext frame protocol with no TLS and no peer
    /// authentication — dev/benchmark-only in v0.2.0. Selecting it requires
    /// `--allow-insecure` AND a loopback `--listen-addr` (the TCP bind address
    /// is derived from it); the loopback gate is NOT bypassable.
    #[arg(long = "raft-transport", env = "RR_RAFT_TRANSPORT")]
    pub raft_transport: Option<String>,

    /// TCP port for Raft AppendEntries/Vote when `--raft-transport tcp`.
    #[arg(long = "raft-tcp-port", env = "RR_RAFT_TCP_PORT")]
    pub raft_tcp_port: Option<u16>,

    /// Bootstrap this node as a single-member cluster (use ONCE per cluster life).
    #[arg(long = "bootstrap", env = "RR_BOOTSTRAP")]
    pub bootstrap: bool,

    /// Comma-separated peer addresses to join (e.g. `127.0.0.1:51001,127.0.0.1:51002`).
    #[arg(long = "peers", env = "RR_PEERS", value_delimiter = ',')]
    pub peers: Vec<String>,

    /// Number of Raft groups (and partitions) the node hosts. The stock
    /// topology maps partition i → group i with all peers as replicas;
    /// default 1. Multi-group setups exist for benchmarking and demos —
    /// dynamic group management goes through rr-admin / ConfigProvider.
    #[arg(long = "groups", env = "RR_GROUPS")]
    pub groups: Option<u64>,

    /// Log level filter (passed to tracing-subscriber EnvFilter).
    /// Examples: `info`, `debug`, `rr_raft=debug,info`.
    #[arg(long = "log-level", env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Path to PEM-encoded TLS certificate. When set with `--tls-key`,
    /// the node serves gRPC over TLS.
    #[arg(long = "tls-cert", env = "RR_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// Path to PEM-encoded TLS private key.
    #[arg(long = "tls-key", env = "RR_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// Path to PEM-encoded CA bundle for peer verification.
    /// Required if `--tls-require-client-cert` is set.
    #[arg(long = "tls-ca", env = "RR_TLS_CA")]
    pub tls_ca: Option<PathBuf>,

    /// Require clients to present a valid certificate (mTLS). Ignored
    /// unless `--tls-cert` and `--tls-ca` are also set.
    #[arg(long = "tls-require-client-cert", env = "RR_TLS_REQUIRE_CLIENT_CERT")]
    pub tls_require_client_cert: bool,

    /// SNI domain name to verify in the server cert when dialing peers.
    #[arg(long = "tls-domain", env = "RR_TLS_DOMAIN")]
    pub tls_domain: Option<String>,

    /// Max requests/sec for AdminService. Omit = unlimited.
    /// **Never** rate-limits Raft RPCs.
    #[arg(long = "admin-rps", env = "RR_ADMIN_RPS")]
    pub admin_rps: Option<u32>,

    /// Max requests/sec for ClientService. Omit = unlimited.
    #[arg(long = "client-rps", env = "RR_CLIENT_RPS")]
    pub client_rps: Option<u32>,

    /// Rate-limit burst size (max concurrent-acceptance window).
    /// If unset, defaults to `max(rps, 10)` per service.
    #[arg(long = "rate-limit-burst", env = "RR_RATE_LIMIT_BURST")]
    pub rate_limit_burst: Option<u32>,

    /// Per-caller rps limit for AdminService. When set, each authenticated
    /// caller gets its own token bucket; unauthenticated requests share the
    /// global `--admin-rps` bucket, which then becomes REQUIRED (validation
    /// rejects per-caller without the global fallback). See ADR A-0005.
    #[arg(long = "per-caller-admin-rps", env = "RR_PER_CALLER_ADMIN_RPS")]
    pub per_caller_admin_rps: Option<u32>,

    /// Per-caller rps limit for ClientService. When set, each authenticated
    /// caller gets its own token bucket; unauthenticated requests share the
    /// global `--client-rps` bucket, which then becomes REQUIRED (validation
    /// rejects per-caller without the global fallback). See ADR A-0005.
    #[arg(long = "per-caller-client-rps", env = "RR_PER_CALLER_CLIENT_RPS")]
    pub per_caller_client_rps: Option<u32>,

    /// Per-caller burst size. If unset, defaults to `max(per_caller_rps, 10)`.
    #[arg(long = "per-caller-burst", env = "RR_PER_CALLER_BURST")]
    pub per_caller_burst: Option<u32>,

    /// Openraft `LogsSinceLast(N)` snapshot trigger threshold. Default 50_000.
    /// Lower values → more frequent snapshots, shorter recovery replay.
    /// Higher values → fewer snapshots, longer recovery replay.
    /// See ADR A-0008 for the trade-off table.
    #[arg(long = "snapshot-logs-since-last", env = "RR_SNAPSHOT_LOGS_SINCE_LAST")]
    pub snapshot_logs_since_last: Option<u64>,

    /// DANGEROUS — bypass production security gates (no TLS / no mTLS / no
    /// admin_token / plaintext TCP raft transport). Intended only for local
    /// dev, examples, and CI. Enabling this also emits a startup warning and
    /// sets the Prometheus gauge `rr_security_insecure_mode = 1`.
    /// NOTE: the one gate this does NOT bypass is the TCP raft loopback
    /// restriction — `--raft-transport tcp` may never bind off-loopback.
    #[arg(long = "allow-insecure", env = "RR_ALLOW_INSECURE")]
    pub allow_insecure: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// Serializes tests that mutate process-wide env vars — `cargo test`
    /// runs tests in threads sharing the process environment. Any test that
    /// calls `std::env::set_var`/`remove_var` MUST hold this lock for its
    /// whole body.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Removes the raft-transport env vars on drop, so a panicking test
    /// cannot leak them into other tests.
    struct RaftTransportEnvGuard;

    impl Drop for RaftTransportEnvGuard {
        fn drop(&mut self) {
            std::env::remove_var("RR_RAFT_TRANSPORT");
            std::env::remove_var("RR_RAFT_TCP_PORT");
        }
    }

    #[test]
    fn cli_parses_version_subcommand() {
        let cli = Cli::try_parse_from(["rr-server", "version"]).expect("parse");
        assert!(matches!(cli.command, Command::Version));
    }

    #[test]
    fn cli_parses_serve_with_flags() {
        let cli = Cli::try_parse_from([
            "rr-server",
            "serve",
            "--node-id",
            "42",
            "--listen-addr",
            "0.0.0.0:51001",
            "--bootstrap",
            "--peers",
            "host1:51001,host2:51001",
        ])
        .expect("parse");
        let Command::Serve(args) = cli.command else {
            panic!("expected Serve");
        };
        assert_eq!(args.node_id, Some(42));
        assert_eq!(args.listen_addr.as_deref(), Some("0.0.0.0:51001"));
        assert!(args.bootstrap);
        assert_eq!(args.peers, vec!["host1:51001", "host2:51001"]);
    }

    #[test]
    fn cli_parses_serve_with_config_only() {
        let cli = Cli::try_parse_from(["rr-server", "serve", "--config", "/etc/rr/config.toml"])
            .expect("parse");
        let Command::Serve(args) = cli.command else {
            panic!("expected Serve");
        };
        assert_eq!(
            args.config.as_deref(),
            Some(std::path::Path::new("/etc/rr/config.toml"))
        );
        assert_eq!(args.node_id, None);
    }

    #[test]
    fn clap_cli_definition_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn cli_parses_tcp_raft_transport_flags() {
        let cli = Cli::try_parse_from([
            "rr-server",
            "serve",
            "--node-id",
            "1",
            "--listen-addr",
            "127.0.0.1:51001",
            "--raft-transport",
            "tcp",
            "--raft-tcp-port",
            "61001",
        ])
        .expect("parse");
        let Command::Serve(args) = cli.command else {
            panic!("expected Serve");
        };
        assert_eq!(args.raft_transport.as_deref(), Some("tcp"));
        assert_eq!(args.raft_tcp_port, Some(61001));
    }

    #[test]
    fn cli_reads_raft_transport_from_env() {
        // Env layer of the config surface (clap `env = ...`). Cargo runs
        // tests in threads sharing the process environment, so every test
        // that mutates env vars MUST hold this lock for its whole body —
        // otherwise a future parse test asserting defaults would race with
        // this window.
        let _lock = ENV_LOCK.lock().unwrap();
        // Panic-safe cleanup: remove the vars even if the parse/assert fails.
        let _cleanup = RaftTransportEnvGuard;
        std::env::set_var("RR_RAFT_TRANSPORT", "tcp");
        std::env::set_var("RR_RAFT_TCP_PORT", "61001");
        let parsed = Cli::try_parse_from([
            "rr-server",
            "serve",
            "--node-id",
            "1",
            "--listen-addr",
            "127.0.0.1:51001",
        ]);
        std::env::remove_var("RR_RAFT_TRANSPORT");
        std::env::remove_var("RR_RAFT_TCP_PORT");
        let cli = parsed.expect("parse with env");
        let Command::Serve(args) = cli.command else {
            panic!("expected Serve");
        };
        assert_eq!(args.raft_transport.as_deref(), Some("tcp"));
        assert_eq!(args.raft_tcp_port, Some(61001));
    }
}
