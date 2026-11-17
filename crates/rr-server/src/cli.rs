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

    /// Explicit client/Raft address advertised to trusted discovery clients.
    #[arg(long = "advertise-addr", env = "RR_ADVERTISE_ADDR")]
    pub advertise_addr: Option<String>,

    /// Explicit AdminService address advertised to trusted discovery clients.
    #[arg(long = "admin-advertise-addr", env = "RR_ADMIN_ADVERTISE_ADDR")]
    pub admin_advertise_addr: Option<String>,

    /// Explicit health/metrics address advertised to trusted discovery clients.
    #[arg(
        long = "observability-advertise-addr",
        env = "RR_OBSERVABILITY_ADVERTISE_ADDR"
    )]
    pub observability_advertise_addr: Option<String>,

    /// Application version reported separately from the Rr framework version.
    #[arg(long = "app-version", env = "RR_APP_VERSION")]
    pub app_version: Option<String>,

    /// Deployment environment published in the node descriptor.
    #[arg(long = "environment", env = "RR_ENVIRONMENT")]
    pub environment: Option<String>,

    #[arg(long = "region", env = "RR_REGION")]
    pub region: Option<String>,

    #[arg(long = "zone", env = "RR_ZONE")]
    pub zone: Option<String>,

    #[arg(long = "rack", env = "RR_RACK")]
    pub rack: Option<String>,

    /// Data directory. Required when `--storage rocks` is selected.
    #[arg(long = "data-dir", env = "RR_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Boot journal directory. RocksDB defaults to DATA_DIR/boot-journal;
    /// mem nodes stay VOLATILE unless this is explicitly set.
    #[arg(long = "boot-journal-dir", env = "RR_BOOT_JOURNAL_DIR")]
    pub boot_journal_dir: Option<PathBuf>,

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
    /// Overrides `[cluster].bootstrap` in the config file; a file-level `true`
    /// can be forced back off with `--no-bootstrap`.
    #[arg(long = "bootstrap", env = "RR_BOOTSTRAP")]
    pub bootstrap: bool,

    /// Force `[cluster].bootstrap = false`, overriding a `true` set in the
    /// config file. Conflicts with `--bootstrap`.
    #[arg(long = "no-bootstrap", conflicts_with = "bootstrap")]
    pub no_bootstrap: bool,

    /// Comma-separated peer list that ONLY sizes the local bootstrap routing
    /// table: one entry per cluster node, node IDs guessed 1-based in list
    /// order (so this node's `--node-id` must be in `1..=N`). The address
    /// strings are never dialed — raft peers are reached via the addresses
    /// registered in membership / the ConfigProvider. For real multi-node
    /// topologies (non-contiguous IDs, later membership changes) use an
    /// etcd-backed ConfigProvider instead.
    #[arg(long = "peers", env = "RR_PEERS", value_delimiter = ',')]
    pub peers: Vec<String>,

    /// Cluster name used for the managed formation lifecycle record in etcd.
    #[arg(long = "cluster-name", env = "RR_CLUSTER_NAME")]
    pub cluster_name: Option<String>,

    /// Comma-separated managed roster entries in `id@advertised_addr` form.
    #[arg(
        long = "cluster-nodes",
        env = "RR_CLUSTER_NODES",
        value_delimiter = ','
    )]
    pub cluster_nodes: Vec<String>,

    /// Managed cluster formation mode: `genesis` or `adopt`.
    #[arg(long = "formation", env = "RR_FORMATION")]
    pub formation: Option<String>,

    /// Comma-separated etcd endpoints used by managed formation.
    #[arg(
        long = "etcd-endpoints",
        env = "RR_ETCD_ENDPOINTS",
        value_delimiter = ','
    )]
    pub etcd_endpoints: Vec<String>,

    /// Comma-separated etcd endpoints used only for leased node discovery.
    /// Managed formation reuses its etcd endpoints when this is omitted.
    #[arg(
        long = "directory-etcd-endpoints",
        env = "RR_DIRECTORY_ETCD_ENDPOINTS",
        value_delimiter = ','
    )]
    pub directory_etcd_endpoints: Vec<String>,

    /// CA bundle used to verify etcd server certificates.
    #[arg(long = "etcd-ca", env = "RR_ETCD_CA")]
    pub etcd_ca: Option<PathBuf>,

    /// Client certificate presented to etcd for mTLS.
    #[arg(long = "etcd-cert", env = "RR_ETCD_CERT")]
    pub etcd_cert: Option<PathBuf>,

    /// Private key for the etcd client certificate.
    #[arg(long = "etcd-key", env = "RR_ETCD_KEY")]
    pub etcd_key: Option<PathBuf>,

    /// SNI/verification domain override for etcd TLS connections.
    #[arg(long = "etcd-domain", env = "RR_ETCD_DOMAIN")]
    pub etcd_domain: Option<String>,

    /// Username for etcd authentication.
    #[arg(long = "etcd-username", env = "RR_ETCD_USERNAME")]
    pub etcd_username: Option<String>,

    /// Name of the environment variable holding the etcd password.
    #[arg(long = "etcd-password-env", env = "RR_ETCD_PASSWORD_ENV")]
    pub etcd_password_env: Option<String>,

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
    /// unless `--tls-cert` and `--tls-ca` are also set. Overrides
    /// `[tls].require_client_cert` in the config file; a file-level `true`
    /// can be forced back off with `--no-require-client-cert`.
    #[arg(long = "tls-require-client-cert", env = "RR_TLS_REQUIRE_CLIENT_CERT")]
    pub tls_require_client_cert: bool,

    /// Force `[tls].require_client_cert = false`, overriding a `true` set
    /// in the config file. Conflicts with `--tls-require-client-cert`.
    #[arg(
        long = "no-require-client-cert",
        conflicts_with = "tls_require_client_cert"
    )]
    pub no_require_client_cert: bool,

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
    /// Overrides `[node].allow_insecure` in the config file; a file-level
    /// `true` can be forced back off with `--no-allow-insecure`.
    #[arg(long = "allow-insecure", env = "RR_ALLOW_INSECURE")]
    pub allow_insecure: bool,

    /// Force `[node].allow_insecure = false`, overriding a `true` set in the
    /// config file. Conflicts with `--allow-insecure`.
    #[arg(long = "no-allow-insecure", conflicts_with = "allow_insecure")]
    pub no_allow_insecure: bool,
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
    fn cli_parses_etcd_security_flags() {
        let cli = Cli::try_parse_from([
            "rr-server",
            "serve",
            "--etcd-ca",
            "/etc/rr/etcd-ca.pem",
            "--etcd-cert",
            "/etc/rr/etcd-client.pem",
            "--etcd-key",
            "/etc/rr/etcd-client.key",
            "--etcd-domain",
            "etcd.internal",
            "--etcd-username",
            "rr-node",
            "--etcd-password-env",
            "RR_ETCD_PASSWORD",
        ])
        .expect("parse");
        let Command::Serve(args) = cli.command else {
            panic!("expected Serve");
        };
        assert_eq!(args.etcd_ca, Some(PathBuf::from("/etc/rr/etcd-ca.pem")));
        assert_eq!(
            args.etcd_cert,
            Some(PathBuf::from("/etc/rr/etcd-client.pem"))
        );
        assert_eq!(
            args.etcd_key,
            Some(PathBuf::from("/etc/rr/etcd-client.key"))
        );
        assert_eq!(args.etcd_domain.as_deref(), Some("etcd.internal"));
        assert_eq!(args.etcd_username.as_deref(), Some("rr-node"));
        assert_eq!(args.etcd_password_env.as_deref(), Some("RR_ETCD_PASSWORD"));
    }

    struct FormationEnvGuard;

    impl Drop for FormationEnvGuard {
        fn drop(&mut self) {
            std::env::remove_var("RR_CLUSTER_NAME");
            std::env::remove_var("RR_CLUSTER_NODES");
            std::env::remove_var("RR_FORMATION");
            std::env::remove_var("RR_ETCD_ENDPOINTS");
        }
    }

    #[test]
    fn cli_parses_managed_formation_flags() {
        let cli = Cli::try_parse_from([
            "rr-server",
            "serve",
            "--cluster-name",
            "stock",
            "--cluster-nodes",
            "1@node-1:51001,2@node-2:51002",
            "--formation",
            "genesis",
            "--etcd-endpoints",
            "http://etcd-1:2379,http://etcd-2:2379",
        ])
        .expect("parse");
        let Command::Serve(args) = cli.command else {
            panic!("expected Serve");
        };
        assert_eq!(args.cluster_name.as_deref(), Some("stock"));
        assert_eq!(args.cluster_nodes, vec!["1@node-1:51001", "2@node-2:51002"]);
        assert_eq!(args.formation.as_deref(), Some("genesis"));
        assert_eq!(
            args.etcd_endpoints,
            vec!["http://etcd-1:2379", "http://etcd-2:2379"]
        );
    }

    #[test]
    fn cli_reads_managed_formation_from_env() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _cleanup = FormationEnvGuard;
        std::env::set_var("RR_CLUSTER_NAME", "stock-env");
        std::env::set_var("RR_CLUSTER_NODES", "1@node-1:51001");
        std::env::set_var("RR_FORMATION", "adopt");
        std::env::set_var("RR_ETCD_ENDPOINTS", "http://etcd:2379");
        let cli = Cli::try_parse_from(["rr-server", "serve"]).expect("parse with env");
        let Command::Serve(args) = cli.command else {
            panic!("expected Serve");
        };
        assert_eq!(args.cluster_name.as_deref(), Some("stock-env"));
        assert_eq!(args.cluster_nodes, vec!["1@node-1:51001"]);
        assert_eq!(args.formation.as_deref(), Some("adopt"));
        assert_eq!(args.etcd_endpoints, vec!["http://etcd:2379"]);
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

    #[test]
    fn cli_parses_negative_boolean_flags() {
        let cli = Cli::try_parse_from([
            "rr-server",
            "serve",
            "--node-id",
            "1",
            "--listen-addr",
            "127.0.0.1:51001",
            "--no-bootstrap",
            "--no-allow-insecure",
            "--no-require-client-cert",
        ])
        .expect("parse");
        let Command::Serve(args) = cli.command else {
            panic!("expected Serve");
        };
        assert!(args.no_bootstrap);
        assert!(args.no_allow_insecure);
        assert!(args.no_require_client_cert);
        // Negated flags must not imply their positive counterparts.
        assert!(!args.bootstrap);
        assert!(!args.allow_insecure);
        assert!(!args.tls_require_client_cert);
    }

    #[test]
    fn cli_positive_and_negative_bootstrap_conflict() {
        let err = Cli::try_parse_from(["rr-server", "serve", "--bootstrap", "--no-bootstrap"])
            .expect_err("--bootstrap and --no-bootstrap must conflict");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn cli_positive_and_negative_allow_insecure_conflict() {
        let err = Cli::try_parse_from([
            "rr-server",
            "serve",
            "--allow-insecure",
            "--no-allow-insecure",
        ])
        .expect_err("--allow-insecure and --no-allow-insecure must conflict");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn cli_positive_and_negative_require_client_cert_conflict() {
        let err = Cli::try_parse_from([
            "rr-server",
            "serve",
            "--tls-require-client-cert",
            "--no-require-client-cert",
        ])
        .expect_err("--tls-require-client-cert and --no-require-client-cert must conflict");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }
}
