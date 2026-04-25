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

    /// RocksDB durability policy: `strict` (default), `balanced`, or `fast`.
    /// Weakening this below `strict` breaks Raft's durability contract.
    #[arg(long = "durability", env = "RR_DURABILITY")]
    pub durability: Option<String>,

    /// Admin gRPC bind port (separate from the main listen_addr). Omit to disable.
    #[arg(long = "admin-port", env = "RR_ADMIN_PORT")]
    pub admin_port: Option<u16>,

    /// Prometheus `/metrics` HTTP bind port. Omit to disable.
    #[arg(long = "metrics-port", env = "RR_METRICS_PORT")]
    pub metrics_port: Option<u16>,

    /// Bootstrap this node as a single-member cluster (use ONCE per cluster life).
    #[arg(long = "bootstrap", env = "RR_BOOTSTRAP")]
    pub bootstrap: bool,

    /// Comma-separated peer addresses to join (e.g. `127.0.0.1:51001,127.0.0.1:51002`).
    #[arg(long = "peers", env = "RR_PEERS", value_delimiter = ',')]
    pub peers: Vec<String>,

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

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
        assert_eq!(args.config.as_deref(), Some(std::path::Path::new("/etc/rr/config.toml")));
        assert_eq!(args.node_id, None);
    }

    #[test]
    fn clap_cli_definition_is_well_formed() {
        Cli::command().debug_assert();
    }
}
