//! TOML config file loading and merging with CLI overrides.
//!
//! The merge order (highest precedence first):
//! 1. CLI flags (from [`crate::cli::ServeArgs`])
//! 2. Environment variables (already parsed into `ServeArgs` by clap)
//! 3. TOML config file values
//! 4. Built-in defaults (applied inside [`ResolvedConfig::resolve`])

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

use crate::cli::ServeArgs;

/// Schema for the TOML config file.
///
/// Every field is optional so that a partial file is valid. Anything unset
/// falls through to either the CLI value or the built-in default.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {
    #[serde(default)]
    pub node: NodeSection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub cluster: ClusterSection,
    #[serde(default)]
    pub tls: TlsSection,
    #[serde(default)]
    pub auth: AuthSection,
    #[serde(default)]
    pub rate_limit: RateLimitSection,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSection {
    pub id: Option<u64>,
    pub listen_addr: Option<String>,
    pub data_dir: Option<PathBuf>,
    pub admin_port: Option<u16>,
    pub metrics_port: Option<u16>,
    pub raft_tcp_port: Option<u16>,
    /// Openraft `LogsSinceLast(N)` snapshot trigger. Default 50_000.
    /// See ADR A-0008.
    pub snapshot_logs_since_last: Option<u64>,
    /// DANGEROUS — bypass production security validation. Dev/CI only.
    /// See `NodeConfig::allow_insecure` rustdoc for the full list of
    /// gates that get bypassed. NOTE: the TCP raft loopback gate (G7,
    /// ADR A-0014) is NOT bypassed by this flag.
    pub allow_insecure: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageSection {
    pub backend: Option<String>,
    pub durability: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterSection {
    pub bootstrap: Option<bool>,
    pub peers: Option<Vec<String>>,
    /// `grpc` (default) or `tcp`. `tcp` is plaintext with no peer
    /// authentication: requires `allow_insecure` and a loopback
    /// `listen_addr` (G6/G7, ADR A-0014).
    pub raft_transport: Option<String>,
}

/// `[tls]` section — when present, gRPC runs over TLS.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub ca_path: Option<PathBuf>,
    #[serde(default)]
    pub require_client_cert: bool,
    pub domain_name: Option<String>,
}

/// `[auth]` section — bearer-token auth. Tokens come from env vars
/// referenced by name, never stored in the config file directly.
///
/// Example:
/// ```toml
/// [auth]
/// admin_token_env  = "RR_ADMIN_TOKEN"
/// client_token_env = "RR_CLIENT_TOKEN"
/// ```
/// Then set `RR_ADMIN_TOKEN=<secret>` in the environment.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSection {
    /// Name of env var holding the Admin bearer token. When set AND the
    /// env var exists, Admin RPCs require this token.
    pub admin_token_env: Option<String>,
    /// Name of env var holding the Client bearer token. When set AND the
    /// env var exists, Client RPCs require this token.
    pub client_token_env: Option<String>,
}

/// `[rate_limit]` section — per-service rate limits. RaftService is
/// intentionally not configurable: throttling Raft destabilizes
/// consensus.
///
/// Per-caller fields (`per_caller_*`) enable exclusive-mode per-caller
/// limits keyed on the bearer-token fingerprint. When set, each
/// authenticated caller gets its own token bucket and the matching
/// global `admin_rps`/`client_rps` is ignored. Unauthenticated requests
/// fall through to the global bucket. See ADR A-0005.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitSection {
    pub admin_rps: Option<u32>,
    pub client_rps: Option<u32>,
    pub burst: Option<u32>,
    pub per_caller_admin_rps: Option<u32>,
    pub per_caller_client_rps: Option<u32>,
    pub per_caller_burst: Option<u32>,
}

/// Fully-resolved configuration after merging file + CLI + defaults.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub node_id: u64,
    pub listen_addr: String,
    pub data_dir: Option<PathBuf>,
    pub admin_port: Option<u16>,
    pub metrics_port: Option<u16>,
    pub raft_tcp_port: Option<u16>,
    pub raft_transport: RaftTransportKind,
    pub storage_backend: StorageBackend,
    pub durability: DurabilityChoice,
    pub bootstrap: bool,
    pub peers: Vec<String>,
    pub log_level: String,
    pub tls: Option<ResolvedTls>,
    pub auth: ResolvedAuth,
    pub rate_limit: ResolvedRateLimit,
    /// Openraft `LogsSinceLast(N)` snapshot trigger. See ADR A-0008.
    pub snapshot_logs_since_last: u64,
    /// DANGEROUS — see `NodeConfig::allow_insecure`.
    pub allow_insecure: bool,
}

/// Resolved rate-limit settings. All fields default to `None` = unlimited.
///
/// Per-caller fields (`per_caller_*`), when set, switch the matching
/// service into exclusive per-caller mode (one token bucket per
/// authenticated caller). See ADR A-0005.
#[derive(Debug, Clone, Default)]
pub struct ResolvedRateLimit {
    pub admin_rps: Option<u32>,
    pub client_rps: Option<u32>,
    pub burst: Option<u32>,
    pub per_caller_admin_rps: Option<u32>,
    pub per_caller_client_rps: Option<u32>,
    pub per_caller_burst: Option<u32>,
}

/// Resolved bearer-token auth settings.
#[derive(Debug, Clone, Default)]
pub struct ResolvedAuth {
    pub admin_token: Option<String>,
    pub client_token: Option<String>,
}

/// Resolved TLS settings (present if all required paths are supplied).
#[derive(Debug, Clone)]
pub struct ResolvedTls {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: Option<PathBuf>,
    pub require_client_cert: bool,
    pub domain_name: Option<String>,
}

/// Selected storage backend at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackend {
    Mem,
    Rocks,
}

/// Selected durability policy string; converted to the actual enum at storage
/// construction time (keeps this module free of the `rocks` feature gate).
///
/// Names changed in v0.3.0:
/// - `"balanced"` → `"replicated_only"` (canonical) — old name still parses
///   with a deprecation warning.
/// - `"fast"` → `"test_only"` (canonical) — old name still parses with a
///   deprecation warning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityChoice {
    Strict,
    ReplicatedOnly,
    TestOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftTransportKind {
    Grpc,
    Tcp,
}

impl ResolvedConfig {
    /// Resolve the final config by merging `file` (if any), `args` (CLI + env),
    /// and hard-coded defaults. Fails with a clear error if required fields
    /// are missing or invalid.
    pub fn resolve(args: &ServeArgs) -> Result<Self> {
        let file = match &args.config {
            Some(path) => Some(Self::load_file(path)?),
            None => None,
        };
        let file = file.unwrap_or_default();

        let node_id = args.node_id.or(file.node.id).ok_or_else(|| {
            anyhow!("missing node id: set via --node-id, RR_NODE_ID, or [node].id in config")
        })?;

        let listen_addr = args
            .listen_addr
            .clone()
            .or(file.node.listen_addr.clone())
            .ok_or_else(|| {
                anyhow!("missing listen_addr: set via --listen-addr, RR_LISTEN_ADDR, or [node].listen_addr")
            })?;

        let storage_backend = match args
            .storage
            .as_deref()
            .or(file.storage.backend.as_deref())
            .unwrap_or("mem")
        {
            "mem" => StorageBackend::Mem,
            "rocks" => StorageBackend::Rocks,
            other => {
                return Err(anyhow!(
                    "invalid storage backend '{other}': expected 'mem' or 'rocks'"
                ))
            }
        };

        let durability_str = args
            .durability
            .as_deref()
            .or(file.storage.durability.as_deref())
            .unwrap_or("strict");
        let durability = match durability_str {
            "strict" => DurabilityChoice::Strict,
            "replicated_only" => DurabilityChoice::ReplicatedOnly,
            "test_only" => DurabilityChoice::TestOnly,
            "balanced" => {
                tracing::warn!(
                    "durability='balanced' is deprecated; rename to 'replicated_only' \
                     (same on-disk behavior). The old name will be removed in v0.4.0."
                );
                DurabilityChoice::ReplicatedOnly
            }
            "fast" => {
                tracing::warn!(
                    "durability='fast' is deprecated; rename to 'test_only' (same \
                     on-disk behavior). The old name will be removed in v0.4.0."
                );
                DurabilityChoice::TestOnly
            }
            other => {
                return Err(anyhow!(
                    "invalid durability '{other}': expected 'strict', \
                     'replicated_only', or 'test_only' (deprecated aliases: \
                     'balanced' → 'replicated_only', 'fast' → 'test_only')"
                ))
            }
        };

        let data_dir = args.data_dir.clone().or(file.node.data_dir.clone());

        if matches!(storage_backend, StorageBackend::Rocks) && data_dir.is_none() {
            return Err(anyhow!(
                "storage = rocks requires data_dir: set via --data-dir, RR_DATA_DIR, or [node].data_dir"
            ));
        }

        let admin_port = args.admin_port.or(file.node.admin_port);
        let metrics_port = args.metrics_port.or(file.node.metrics_port);
        let raft_tcp_port = args.raft_tcp_port.or(file.node.raft_tcp_port);

        let raft_transport = match args
            .raft_transport
            .as_deref()
            .or(file.cluster.raft_transport.as_deref())
            .unwrap_or("grpc")
        {
            "grpc" => RaftTransportKind::Grpc,
            "tcp" => RaftTransportKind::Tcp,
            other => {
                return Err(anyhow!(
                    "invalid raft transport '{other}': expected 'grpc' or 'tcp'"
                ))
            }
        };

        let raft_tcp_port = if matches!(raft_transport, RaftTransportKind::Tcp)
            && raft_tcp_port.is_none()
        {
            let grpc_port = parse_port_from_addr(&listen_addr)?;
            Some(grpc_port.checked_add(10_000).ok_or_else(|| {
                anyhow!(
                    "failed to derive raft_tcp_port from listen_addr '{listen_addr}' with default +10000 offset"
                )
            })?)
        } else {
            raft_tcp_port
        };

        let bootstrap = args.bootstrap || file.cluster.bootstrap.unwrap_or(false);
        let peers = if !args.peers.is_empty() {
            args.peers.clone()
        } else {
            file.cluster.peers.clone().unwrap_or_default()
        };

        if bootstrap && !peers.is_empty() {
            return Err(anyhow!(
                "--bootstrap and --peers are mutually exclusive: a bootstrapping node must not list peers"
            ));
        }

        let tls = resolve_tls(args, &file.tls)?;
        let auth = resolve_auth(&file.auth)?;
        let rate_limit = ResolvedRateLimit {
            admin_rps: args.admin_rps.or(file.rate_limit.admin_rps),
            client_rps: args.client_rps.or(file.rate_limit.client_rps),
            burst: args.rate_limit_burst.or(file.rate_limit.burst),
            per_caller_admin_rps: args
                .per_caller_admin_rps
                .or(file.rate_limit.per_caller_admin_rps),
            per_caller_client_rps: args
                .per_caller_client_rps
                .or(file.rate_limit.per_caller_client_rps),
            per_caller_burst: args.per_caller_burst.or(file.rate_limit.per_caller_burst),
        };
        let allow_insecure = args.allow_insecure || file.node.allow_insecure.unwrap_or(false);

        let snapshot_logs_since_last = args
            .snapshot_logs_since_last
            .or(file.node.snapshot_logs_since_last)
            .unwrap_or(rr_raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST);
        if snapshot_logs_since_last == 0 {
            return Err(anyhow!(
                "snapshot_logs_since_last must be > 0; got 0 (would disable snapshots)"
            ));
        }

        Ok(Self {
            node_id,
            listen_addr,
            data_dir,
            admin_port,
            metrics_port,
            raft_tcp_port,
            raft_transport,
            storage_backend,
            durability,
            bootstrap,
            peers,
            log_level: args.log_level.clone(),
            tls,
            auth,
            rate_limit,
            snapshot_logs_since_last,
            allow_insecure,
        })
    }

    fn load_file(path: &Path) -> Result<ConfigFile> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let parsed: ConfigFile = toml::from_str(&raw)
            .with_context(|| format!("failed to parse TOML config {}", path.display()))?;
        Ok(parsed)
    }
}

fn resolve_auth(file: &AuthSection) -> Result<ResolvedAuth> {
    let admin_token = match &file.admin_token_env {
        Some(var) => Some(read_token_env(var, "admin")?),
        None => None,
    };
    let client_token = match &file.client_token_env {
        Some(var) => Some(read_token_env(var, "client")?),
        None => None,
    };
    Ok(ResolvedAuth {
        admin_token,
        client_token,
    })
}

fn read_token_env(var_name: &str, role: &str) -> Result<String> {
    match std::env::var(var_name) {
        Ok(v) if !v.is_empty() => Ok(v),
        Ok(_) => Err(anyhow!(
            "auth.{role}_token_env points to environment variable '{var_name}' \
             which is set but EMPTY. Refusing to start with an empty token: this would \
             silently disable {role} auth (fail-open is a security anti-pattern). \
             Set {var_name}=<non-empty-secret>, or remove auth.{role}_token_env from \
             the config to explicitly disable {role} auth."
        )),
        Err(_) => Err(anyhow!(
            "auth.{role}_token_env references environment variable '{var_name}' \
             which is NOT SET. Refusing to start: this would silently disable {role} \
             auth (fail-open). Either set {var_name}=<secret>, or remove \
             auth.{role}_token_env from the config to explicitly disable {role} auth."
        )),
    }
}

fn resolve_tls(args: &ServeArgs, file: &TlsSection) -> Result<Option<ResolvedTls>> {
    let cert_path = args.tls_cert.clone().or_else(|| file.cert_path.clone());
    let key_path = args.tls_key.clone().or_else(|| file.key_path.clone());
    let ca_path = args.tls_ca.clone().or_else(|| file.ca_path.clone());
    let domain_name = args.tls_domain.clone().or_else(|| file.domain_name.clone());
    let require_client_cert = args.tls_require_client_cert || file.require_client_cert;

    match (cert_path, key_path) {
        (None, None) => {
            if ca_path.is_some() || require_client_cert {
                return Err(anyhow!(
                    "TLS options specified but tls_cert / tls_key are missing; TLS cannot be enabled"
                ));
            }
            Ok(None)
        }
        (Some(_), None) | (None, Some(_)) => Err(anyhow!(
            "tls_cert and tls_key must both be set to enable TLS"
        )),
        (Some(cert), Some(key)) => {
            if require_client_cert && ca_path.is_none() {
                return Err(anyhow!(
                    "tls_require_client_cert = true requires tls_ca to verify client certs"
                ));
            }
            Ok(Some(ResolvedTls {
                cert_path: cert,
                key_path: key,
                ca_path,
                require_client_cert,
                domain_name,
            }))
        }
    }
}

fn parse_port_from_addr(addr: &str) -> Result<u16> {
    let (_, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("listen_addr '{addr}' must include a port"))?;
    port.parse::<u16>()
        .with_context(|| format!("listen_addr '{addr}' has invalid port"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn args_with_config(path: PathBuf) -> ServeArgs {
        ServeArgs {
            config: Some(path),
            log_level: "info".to_string(),
            ..Default::default()
        }
    }

    fn write_toml(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn missing_node_id_errors_clearly() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("node id"), "got: {err}");
    }

    #[test]
    fn missing_listen_addr_errors_clearly() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("listen_addr"), "got: {err}");
    }

    #[test]
    fn cli_overrides_file_values() {
        let f = write_toml(
            r#"
            [node]
            id = 10
            listen_addr = "0.0.0.0:60000"
            "#,
        );
        let args = ServeArgs {
            config: Some(f.path().to_path_buf()),
            log_level: "info".to_string(),
            node_id: Some(42),
            ..Default::default()
        };
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.node_id, 42);
        assert_eq!(resolved.listen_addr, "0.0.0.0:60000");
    }

    #[test]
    fn file_values_used_when_cli_omits() {
        let f = write_toml(
            r#"
            [node]
            id = 7
            listen_addr = "127.0.0.1:51001"
            metrics_port = 9100

            [storage]
            backend = "mem"
            durability = "replicated_only"

            [cluster]
            bootstrap = true
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.node_id, 7);
        assert_eq!(resolved.listen_addr, "127.0.0.1:51001");
        assert_eq!(resolved.metrics_port, Some(9100));
        assert!(matches!(resolved.storage_backend, StorageBackend::Mem));
        assert!(matches!(
            resolved.durability,
            DurabilityChoice::ReplicatedOnly
        ));
        assert!(resolved.bootstrap);
    }

    #[test]
    fn balanced_alias_resolves_to_replicated_only() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [storage]
            durability = "balanced"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(matches!(
            resolved.durability,
            DurabilityChoice::ReplicatedOnly
        ));
    }

    #[test]
    fn fast_alias_resolves_to_test_only() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [storage]
            durability = "fast"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(matches!(resolved.durability, DurabilityChoice::TestOnly));
    }

    #[test]
    fn snapshot_logs_since_last_default_is_framework_constant() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(
            resolved.snapshot_logs_since_last,
            rr_raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST
        );
    }

    #[test]
    fn snapshot_logs_since_last_from_toml() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            snapshot_logs_since_last = 5000
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.snapshot_logs_since_last, 5000);
    }

    #[test]
    fn per_caller_rate_limit_from_toml_propagates_to_resolved() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [rate_limit]
            admin_rps = 50
            client_rps = 200
            per_caller_admin_rps = 5
            per_caller_client_rps = 20
            per_caller_burst = 10
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.rate_limit.admin_rps, Some(50));
        assert_eq!(resolved.rate_limit.client_rps, Some(200));
        assert_eq!(resolved.rate_limit.per_caller_admin_rps, Some(5));
        assert_eq!(resolved.rate_limit.per_caller_client_rps, Some(20));
        assert_eq!(resolved.rate_limit.per_caller_burst, Some(10));
    }

    #[test]
    fn per_caller_rate_limit_cli_overrides_toml() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [rate_limit]
            per_caller_admin_rps = 5
            per_caller_client_rps = 20
            per_caller_burst = 10
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.per_caller_admin_rps = Some(99);
        args.per_caller_client_rps = Some(199);
        args.per_caller_burst = Some(50);
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.rate_limit.per_caller_admin_rps, Some(99));
        assert_eq!(resolved.rate_limit.per_caller_client_rps, Some(199));
        assert_eq!(resolved.rate_limit.per_caller_burst, Some(50));
    }

    #[test]
    fn per_caller_rate_limit_defaults_to_none() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(resolved.rate_limit.per_caller_admin_rps.is_none());
        assert!(resolved.rate_limit.per_caller_client_rps.is_none());
        assert!(resolved.rate_limit.per_caller_burst.is_none());
    }

    #[test]
    fn snapshot_logs_since_last_cli_overrides_toml() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            snapshot_logs_since_last = 5000
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.snapshot_logs_since_last = Some(123_456);
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.snapshot_logs_since_last, 123_456);
    }

    #[test]
    fn snapshot_logs_since_last_zero_rejected() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            snapshot_logs_since_last = 0
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(
            err.to_string().contains("must be > 0"),
            "expected zero-rejection error, got: {err}"
        );
    }

    #[test]
    fn replicated_only_canonical_resolves_directly() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [storage]
            durability = "replicated_only"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(matches!(
            resolved.durability,
            DurabilityChoice::ReplicatedOnly
        ));
    }

    #[test]
    fn rocks_backend_requires_data_dir() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            storage: Some("rocks".to_string()),
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("data_dir"), "got: {err}");
    }

    #[test]
    fn invalid_storage_backend_errors() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            storage: Some("sled".to_string()),
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("storage backend"), "got: {err}");
    }

    #[test]
    fn invalid_durability_errors() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            durability: Some("loose".to_string()),
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("durability"), "got: {err}");
    }

    #[test]
    fn bootstrap_and_peers_are_mutually_exclusive() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            bootstrap: true,
            peers: vec!["host2:51001".to_string()],
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "got: {err}");
    }

    #[test]
    fn unknown_toml_fields_are_rejected() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            
            [bogus]
            what = "huh"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        let full_chain = format!("{err:#}");
        assert!(
            full_chain.contains("unknown field") || full_chain.contains("bogus"),
            "got: {full_chain}"
        );
    }

    #[test]
    fn defaults_when_nothing_specified() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            ..Default::default()
        };
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(matches!(resolved.storage_backend, StorageBackend::Mem));
        assert!(matches!(resolved.durability, DurabilityChoice::Strict));
        assert!(!resolved.bootstrap);
        assert!(resolved.peers.is_empty());
        assert_eq!(resolved.metrics_port, None);
        assert_eq!(resolved.admin_port, None);
        assert_eq!(resolved.raft_tcp_port, None);
        assert!(matches!(resolved.raft_transport, RaftTransportKind::Grpc));
        assert!(resolved.tls.is_none());
    }

    #[test]
    fn raft_transport_tcp_from_cli_derives_default_tcp_port() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            raft_transport: Some("tcp".to_string()),
            ..Default::default()
        };
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(matches!(resolved.raft_transport, RaftTransportKind::Tcp));
        assert_eq!(resolved.raft_tcp_port, Some(61001));
    }

    #[test]
    fn raft_transport_tcp_port_from_toml_is_used() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"
            raft_tcp_port = 52001

            [cluster]
            raft_transport = "tcp"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(matches!(resolved.raft_transport, RaftTransportKind::Tcp));
        assert_eq!(resolved.raft_tcp_port, Some(52001));
    }

    #[test]
    fn raft_transport_cli_overrides_toml() {
        // Priority: CLI flag beats TOML for both the transport kind and the
        // TCP port. TOML says grpc, CLI says tcp → tcp wins, and the CLI
        // port suppresses the +10000 derivation from listen_addr.
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"
            raft_tcp_port = 52001

            [cluster]
            raft_transport = "grpc"
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.raft_transport = Some("tcp".to_string());
        args.raft_tcp_port = Some(62001);
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(matches!(resolved.raft_transport, RaftTransportKind::Tcp));
        assert_eq!(resolved.raft_tcp_port, Some(62001));

        // And the reverse direction: TOML says tcp, CLI forces grpc → the
        // TCP port is irrelevant and the transport is grpc.
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            raft_transport = "tcp"
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.raft_transport = Some("grpc".to_string());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(matches!(resolved.raft_transport, RaftTransportKind::Grpc));
    }

    #[test]
    fn raft_transport_tcp_toml_derives_bind_inputs_for_gate() {
        // ADR A-0014 test-matrix item 2: TOML-selected tcp transport resolves
        // to (listen_addr, raft_tcp_port) — the inputs from which main.rs
        // derives the single TCP bind address carried by
        // `NodeConfig::raft_transport` into the G6/G7 gates.
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            raft_transport = "tcp"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(matches!(resolved.raft_transport, RaftTransportKind::Tcp));
        // Default +10000 derivation.
        assert_eq!(resolved.raft_tcp_port, Some(61001));
        // The derived bind address (host from listen_addr) must be loopback —
        // G7 would reject this config otherwise.
        let bind_addr = format!(
            "{}:{}",
            resolved.listen_addr.rsplit_once(':').unwrap().0,
            resolved.raft_tcp_port.unwrap()
        );
        let parsed: std::net::SocketAddr = bind_addr.parse().unwrap();
        assert!(parsed.ip().is_loopback());
    }

    #[test]
    fn tls_is_disabled_by_default() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            ..Default::default()
        };
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert!(resolved.tls.is_none());
    }

    #[test]
    fn tls_cert_alone_without_key_errors() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            tls_cert: Some(PathBuf::from("/tmp/cert.pem")),
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("tls_cert and tls_key"));
    }

    #[test]
    fn tls_require_client_cert_without_ca_errors() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            tls_cert: Some(PathBuf::from("/tmp/cert.pem")),
            tls_key: Some(PathBuf::from("/tmp/key.pem")),
            tls_require_client_cert: true,
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("tls_ca"));
    }

    #[test]
    fn tls_enabled_with_cert_and_key() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            tls_cert: Some(PathBuf::from("/etc/rr/cert.pem")),
            tls_key: Some(PathBuf::from("/etc/rr/key.pem")),
            tls_ca: Some(PathBuf::from("/etc/rr/ca.pem")),
            tls_require_client_cert: true,
            tls_domain: Some("rr.internal".to_string()),
            ..Default::default()
        };
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        let tls = resolved.tls.expect("tls should be configured");
        assert_eq!(tls.cert_path, PathBuf::from("/etc/rr/cert.pem"));
        assert_eq!(tls.key_path, PathBuf::from("/etc/rr/key.pem"));
        assert_eq!(tls.ca_path, Some(PathBuf::from("/etc/rr/ca.pem")));
        assert!(tls.require_client_cert);
        assert_eq!(tls.domain_name.as_deref(), Some("rr.internal"));
    }

    #[test]
    fn auth_env_var_unset_fails_closed() {
        let unique_var = format!("RR_TEST_UNSET_{}", std::process::id());
        std::env::remove_var(&unique_var);
        let f = write_toml(&format!(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [auth]
            admin_token_env = "{unique_var}"
            "#
        ));
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("NOT SET") && msg.contains(&unique_var),
            "expected fail-closed error mentioning unset var, got: {msg}"
        );
    }

    #[test]
    fn auth_env_var_empty_fails_closed() {
        let unique_var = format!("RR_TEST_EMPTY_{}", std::process::id());
        std::env::set_var(&unique_var, "");
        let f = write_toml(&format!(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [auth]
            client_token_env = "{unique_var}"
            "#
        ));
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        std::env::remove_var(&unique_var);
        let msg = format!("{err:#}");
        assert!(
            msg.contains("EMPTY") && msg.contains(&unique_var),
            "expected fail-closed error for empty var, got: {msg}"
        );
    }

    #[test]
    fn auth_env_var_set_loads_token() {
        let unique_var = format!("RR_TEST_SET_{}", std::process::id());
        std::env::set_var(&unique_var, "supersecret");
        let f = write_toml(&format!(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [auth]
            admin_token_env = "{unique_var}"
            "#
        ));
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        std::env::remove_var(&unique_var);
        assert_eq!(resolved.auth.admin_token.as_deref(), Some("supersecret"));
        assert_eq!(resolved.auth.client_token, None);
    }

    #[test]
    fn auth_no_env_var_configured_yields_no_tokens() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.auth.admin_token, None);
        assert_eq!(resolved.auth.client_token, None);
    }

    #[test]
    fn tls_from_toml_file_loads_correctly() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [tls]
            cert_path = "/srv/rr/cert.pem"
            key_path = "/srv/rr/key.pem"
            ca_path = "/srv/rr/ca.pem"
            require_client_cert = true
            domain_name = "rr.prod"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        let tls = resolved.tls.expect("tls from toml");
        assert_eq!(tls.cert_path, PathBuf::from("/srv/rr/cert.pem"));
        assert!(tls.require_client_cert);
    }
}
