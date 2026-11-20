//! TOML config file loading and merging with CLI overrides.
//!
//! The merge order (highest precedence first):
//! 1. CLI flags (from [`crate::cli::ServeArgs`])
//! 2. Environment variables (already parsed into `ServeArgs` by clap)
//! 3. TOML config file values
//! 4. Built-in defaults (applied inside [`ResolvedConfig::resolve`])
//!
//! Boolean flags with a `--no-*` negation (`bootstrap`, `allow_insecure`,
//! `tls.require_client_cert`) merge by explicit precedence:
//! CLI affirmative > CLI negation > config file > default `false`. The
//! negations exist because a plain `||` merge could never turn a file-level
//! `true` back off.

use rr_core::error::ConfigError;
use rr_core::types::NodeAddr;
use rr_core::SeedSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::cli::ServeArgs;
use crate::formation::FormationMode;

/// Module-local result type: config resolution fails with typed
/// [`ConfigError`]s, keeping `anyhow` out of the library's public surface
/// (the `rr-server` binary may still use `anyhow` internally).
type Result<T> = std::result::Result<T, ConfigError>;

/// Build an `InvalidConfig` error with a caller-context message.
fn invalid(msg: impl Into<String>) -> ConfigError {
    ConfigError::InvalidConfig(msg.into())
}

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
    pub boot_journal_dir: Option<PathBuf>,
    pub advertise_addr: Option<String>,
    pub admin_advertise_addr: Option<String>,
    pub observability_advertise_addr: Option<String>,
    pub app_version: Option<String>,
    pub environment: Option<String>,
    pub region: Option<String>,
    pub zone: Option<String>,
    pub rack: Option<String>,
    pub admin_port: Option<u16>,
    /// Admin gRPC bind host (IP literal). Default `127.0.0.1` (loopback);
    /// set `0.0.0.0` explicitly to expose the admin API cluster-wide.
    pub admin_bind_host: Option<String>,
    pub metrics_port: Option<u16>,
    /// Metrics/health HTTP bind host (IP literal). Default `127.0.0.1`;
    /// set `0.0.0.0` to allow cross-host Prometheus scrapes.
    pub metrics_bind_host: Option<String>,
    /// gRPC server HTTP/2 max_concurrent_streams override (perf knob).
    pub grpc_max_concurrent_streams: Option<u32>,
    /// gRPC server HTTP/2 max frame size override in bytes (perf knob).
    pub grpc_max_frame_size: Option<u32>,
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
    pub name: Option<String>,
    pub nodes: Option<Vec<String>>,
    pub formation: Option<FormationSection>,
    /// Optional etcd namespace used for leased node discovery.
    pub directory_etcd_endpoints: Option<Vec<String>>,
    /// Shared TLS/auth settings for formation and directory etcd clients.
    pub etcd_ca_path: Option<PathBuf>,
    pub etcd_cert_path: Option<PathBuf>,
    pub etcd_key_path: Option<PathBuf>,
    pub etcd_domain_name: Option<String>,
    pub etcd_username: Option<String>,
    /// Name of the environment variable holding the etcd password.
    pub etcd_password_env: Option<String>,
    /// Number of Raft groups/partitions the node hosts (stock topology:
    /// partition i → group i, replicas = all peers). Default 1.
    pub groups: Option<u64>,
    /// `grpc` (default) or `tcp`. `tcp` is plaintext with no peer
    /// authentication: requires `allow_insecure` and a loopback
    /// `listen_addr` (G6/G7, ADR A-0014).
    pub raft_transport: Option<String>,
    /// Automatically promote caught-up learners to voters on hosted groups
    /// (default true; quorum-guarded). Set false to drive membership only
    /// by hand.
    pub auto_promote_learners: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormationSection {
    pub mode: Option<String>,
    pub etcd_endpoints: Option<Vec<String>>,
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
/// Per-caller fields (`per_caller_*`) enable per-caller limits keyed on
/// the bearer-token fingerprint. When set, each authenticated caller gets
/// its own token bucket; unauthenticated requests share the global bucket,
/// so the matching global `admin_rps`/`client_rps` is REQUIRED (config
/// validation rejects per-caller without it). See ADR A-0005.
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
    pub boot_journal_dir: Option<PathBuf>,
    pub app_version: String,
    pub descriptor: Option<ResolvedNodeDescriptor>,
    pub directory_etcd_endpoints: Vec<String>,
    pub cluster_name: Option<String>,
    pub etcd: Option<ResolvedEtcd>,
    pub admin_port: Option<u16>,
    /// Resolved admin gRPC bind host (defaults to loopback `127.0.0.1`).
    pub admin_bind_host: String,
    pub metrics_port: Option<u16>,
    /// Resolved metrics/health HTTP bind host (defaults to loopback).
    pub metrics_bind_host: String,
    /// Resolved gRPC HTTP/2 max_concurrent_streams override (None = tonic default).
    pub grpc_max_concurrent_streams: Option<u32>,
    /// Resolved gRPC HTTP/2 max frame size override, bytes (None = 16 KiB default).
    pub grpc_max_frame_size: Option<u32>,
    pub raft_tcp_port: Option<u16>,
    pub raft_transport: RaftTransportKind,
    pub storage_backend: StorageBackend,
    pub durability: DurabilityChoice,
    pub bootstrap: bool,
    pub peers: Vec<String>,
    pub formation: Option<ResolvedFormation>,
    /// Number of Raft groups/partitions the node hosts (stock topology:
    /// partition i → group i, replicas = all peers). Default 1.
    pub groups: u64,
    /// Auto-promote caught-up learners to voters (default true).
    pub auto_promote_learners: bool,
    pub log_level: String,
    pub tls: Option<ResolvedTls>,
    pub auth: ResolvedAuth,
    pub rate_limit: ResolvedRateLimit,
    /// Openraft `LogsSinceLast(N)` snapshot trigger. See ADR A-0008.
    pub snapshot_logs_since_last: u64,
    /// DANGEROUS — see `NodeConfig::allow_insecure`.
    pub allow_insecure: bool,
}

#[derive(Debug, Clone)]
pub struct ResolvedNodeDescriptor {
    pub client_addr: String,
    pub admin_addr: String,
    pub observability_addr: String,
    pub advertise_addr: String,
    pub environment: String,
    pub region: Option<String>,
    pub zone: Option<String>,
    pub rack: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedFormation {
    pub seed: SeedSpec,
    pub mode: FormationMode,
    pub etcd_endpoints: Vec<String>,
}

#[derive(Clone)]
pub struct ResolvedEtcd {
    pub ca_path: Option<PathBuf>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub domain_name: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl std::fmt::Debug for ResolvedEtcd {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedEtcd")
            .field("ca_path", &self.ca_path)
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .field("domain_name", &self.domain_name)
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Resolved rate-limit settings. All fields default to `None` = unlimited.
///
/// Per-caller fields (`per_caller_*`), when set, give each authenticated
/// caller its own token bucket; unauthenticated requests share the
/// matching global bucket (`admin_rps` / `client_rps`, then required).
/// See ADR A-0005.
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
/// The v0.2.x aliases `"balanced"` and `"fast"` were REMOVED in v1.0.0 —
/// they now fail with a hard error naming the canonical values.
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
            invalid("missing node id: set via --node-id, RR_NODE_ID, or [node].id in config")
        })?;

        let listen_addr = args
            .listen_addr
            .clone()
            .or(file.node.listen_addr.clone())
            .ok_or_else(|| {
                invalid(
                    "missing listen_addr: set via --listen-addr, RR_LISTEN_ADDR, or [node].listen_addr",
                )
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
                return Err(invalid(format!(
                    "invalid storage backend '{other}': expected 'mem' or 'rocks'"
                )));
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
            "balanced" | "fast" => {
                let canonical = if durability_str == "balanced" {
                    "replicated_only"
                } else {
                    "test_only"
                };
                return Err(invalid(format!(
                    "invalid durability '{durability_str}': the v0.2.x alias was removed in \
                     v1.0.0 — use '{canonical}'"
                )));
            }
            other => {
                return Err(invalid(format!(
                    "invalid durability '{other}': expected 'strict', \
                     'replicated_only', or 'test_only'"
                )));
            }
        };

        let data_dir = args.data_dir.clone().or(file.node.data_dir.clone());

        if matches!(storage_backend, StorageBackend::Rocks) && data_dir.is_none() {
            return Err(invalid(
                "storage = rocks requires data_dir: set via --data-dir, RR_DATA_DIR, or [node].data_dir",
            ));
        }
        let boot_journal_dir = args
            .boot_journal_dir
            .clone()
            .or(file.node.boot_journal_dir.clone())
            .or_else(|| {
                matches!(storage_backend, StorageBackend::Rocks)
                    .then(|| data_dir.as_ref().unwrap().join("boot-journal"))
            });
        let app_version = args
            .app_version
            .clone()
            .or(file.node.app_version.clone())
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        let descriptor = resolve_node_descriptor(args, &file)?;

        let admin_port = args.admin_port.or(file.node.admin_port);
        let metrics_port = args.metrics_port.or(file.node.metrics_port);
        let admin_bind_host = resolve_bind_host(
            args.admin_bind_host.as_deref(),
            file.node.admin_bind_host.as_deref(),
            crate::config::DEFAULT_ADMIN_BIND_HOST,
            "admin_bind_host",
        )?;
        let metrics_bind_host = resolve_bind_host(
            args.metrics_bind_host.as_deref(),
            file.node.metrics_bind_host.as_deref(),
            crate::config::DEFAULT_METRICS_BIND_HOST,
            "metrics_bind_host",
        )?;
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
                return Err(invalid(format!(
                    "invalid raft transport '{other}': expected 'grpc' or 'tcp'"
                )));
            }
        };

        let raft_tcp_port = if matches!(raft_transport, RaftTransportKind::Tcp)
            && raft_tcp_port.is_none()
        {
            let grpc_port = parse_port_from_addr(&listen_addr)?;
            Some(grpc_port.checked_add(10_000).ok_or_else(|| {
                invalid(format!(
                    "failed to derive raft_tcp_port from listen_addr '{listen_addr}' with default +10000 offset"
                ))
            })?)
        } else {
            raft_tcp_port
        };

        // Boolean precedence: CLI affirmative > CLI negation (`--no-bootstrap`)
        // > config file > default false. A plain `||` merge could never turn a
        // file-level `true` back off (the I5 contract: CLI beats the file in
        // both directions).
        let bootstrap = if args.bootstrap {
            true
        } else if args.no_bootstrap {
            false
        } else {
            file.cluster.bootstrap.unwrap_or(false)
        };
        let peers = if !args.peers.is_empty() {
            args.peers.clone()
        } else {
            file.cluster.peers.clone().unwrap_or_default()
        };

        if bootstrap && !peers.is_empty() {
            return Err(invalid(
                "--bootstrap and --peers are mutually exclusive: a bootstrapping node must not list peers",
            ));
        }

        let groups = args.groups.or(file.cluster.groups).unwrap_or(1);
        if groups == 0 {
            return Err(invalid(
                "groups must be >= 1 (0 would start no Raft group at all)",
            ));
        }
        let auto_promote_learners = file.cluster.auto_promote_learners.unwrap_or(true);
        let formation = resolve_formation(
            args,
            &file.cluster,
            node_id,
            groups,
            bootstrap,
            !peers.is_empty(),
        )?;
        let cluster_name = args.cluster_name.clone().or(file.cluster.name.clone());
        let mut directory_etcd_endpoints = if !args.directory_etcd_endpoints.is_empty() {
            args.directory_etcd_endpoints.clone()
        } else {
            file.cluster
                .directory_etcd_endpoints
                .clone()
                .unwrap_or_default()
        };
        if directory_etcd_endpoints.is_empty() && descriptor.is_some() {
            directory_etcd_endpoints = formation
                .as_ref()
                .map(|formation| formation.etcd_endpoints.clone())
                .unwrap_or_default();
        }
        if !directory_etcd_endpoints.is_empty() {
            if descriptor.is_none() {
                return Err(invalid(
                    "directory discovery requires a complete explicit node descriptor",
                ));
            }
            if cluster_name
                .as_deref()
                .is_none_or(|name| name.trim().is_empty())
            {
                return Err(invalid(
                    "directory discovery requires a non-empty cluster name",
                ));
            }
        }
        let etcd = resolve_etcd(
            args,
            &file.cluster,
            formation.is_some() || !directory_etcd_endpoints.is_empty(),
        )?;
        if etcd.is_some() {
            let insecure_endpoint = formation
                .as_ref()
                .into_iter()
                .flat_map(|formation| formation.etcd_endpoints.iter())
                .chain(directory_etcd_endpoints.iter())
                .find(|endpoint| !endpoint.starts_with("https://"));
            if let Some(endpoint) = insecure_endpoint {
                return Err(invalid(format!(
                    "etcd TLS/auth requires https endpoints; insecure endpoint '{endpoint}'"
                )));
            }
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
        // Same boolean precedence as `bootstrap` above: CLI affirmative >
        // CLI negation (`--no-allow-insecure`) > config file > default false.
        let allow_insecure = if args.allow_insecure {
            true
        } else if args.no_allow_insecure {
            false
        } else {
            file.node.allow_insecure.unwrap_or(false)
        };

        let grpc_max_concurrent_streams = args
            .grpc_max_concurrent_streams
            .or(file.node.grpc_max_concurrent_streams);
        if grpc_max_concurrent_streams == Some(0) {
            return Err(invalid("grpc_max_concurrent_streams must be >= 1"));
        }
        let grpc_max_frame_size = args.grpc_max_frame_size.or(file.node.grpc_max_frame_size);
        if grpc_max_frame_size == Some(0) {
            return Err(invalid("grpc_max_frame_size must be >= 1 (bytes)"));
        }
        if let Some(n) = grpc_max_frame_size {
            let cap = rr_core::bounded_decode::MAX_ENTRY_BODY as u64;
            if u64::from(n) > cap {
                return Err(invalid(format!(
                    "grpc_max_frame_size ({n} bytes) exceeds the apply-side decode cap \
                     MAX_ENTRY_BODY ({cap} bytes): a keyed write larger than the cap would \
                     encode and replicate fine but fail FramedEntry::decode at apply time, \
                     and apply errors are group-fatal (ADR A-0018). Lower grpc_max_frame_size \
                     to at most {cap} bytes"
                )));
            }
        }

        let snapshot_logs_since_last = args
            .snapshot_logs_since_last
            .or(file.node.snapshot_logs_since_last)
            .unwrap_or(rr_raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST);
        if snapshot_logs_since_last == 0 {
            return Err(invalid(
                "snapshot_logs_since_last must be > 0; got 0 (would disable snapshots)",
            ));
        }

        Ok(Self {
            node_id,
            listen_addr,
            data_dir,
            boot_journal_dir,
            app_version,
            descriptor,
            directory_etcd_endpoints,
            cluster_name,
            etcd,
            admin_port,
            admin_bind_host,
            metrics_port,
            metrics_bind_host,
            grpc_max_concurrent_streams,
            grpc_max_frame_size,
            raft_tcp_port,
            raft_transport,
            storage_backend,
            durability,
            bootstrap,
            peers,
            formation,
            groups,
            auto_promote_learners,
            log_level: args.log_level.clone(),
            tls,
            auth,
            rate_limit,
            snapshot_logs_since_last,
            allow_insecure,
        })
    }

    fn load_file(path: &Path) -> Result<ConfigFile> {
        let raw = fs::read_to_string(path).map_err(|e| {
            invalid(format!(
                "failed to read config file {}: {e}",
                path.display()
            ))
        })?;
        let parsed: ConfigFile = toml::from_str(&raw).map_err(|e| {
            invalid(format!(
                "failed to parse TOML config {}: {e}",
                path.display()
            ))
        })?;
        Ok(parsed)
    }
}

fn resolve_node_descriptor(
    args: &ServeArgs,
    file: &ConfigFile,
) -> Result<Option<ResolvedNodeDescriptor>> {
    let client_addr = args
        .advertise_addr
        .clone()
        .or(file.node.advertise_addr.clone());
    let admin_addr = args
        .admin_advertise_addr
        .clone()
        .or(file.node.admin_advertise_addr.clone());
    let observability_addr = args
        .observability_advertise_addr
        .clone()
        .or(file.node.observability_advertise_addr.clone());
    let environment = args.environment.clone().or(file.node.environment.clone());
    let region = args.region.clone().or(file.node.region.clone());
    let zone = args.zone.clone().or(file.node.zone.clone());
    let rack = args.rack.clone().or(file.node.rack.clone());
    if [
        &client_addr,
        &admin_addr,
        &observability_addr,
        &environment,
        &region,
        &zone,
        &rack,
    ]
    .iter()
    .all(|value| value.is_none())
    {
        return Ok(None);
    }
    let required = |value: Option<String>, name: &str| {
        value.ok_or_else(|| {
            invalid(format!(
                "incomplete node descriptor: {name} is required when any descriptor field is set"
            ))
        })
    };
    let descriptor = ResolvedNodeDescriptor {
        advertise_addr: required(client_addr.clone(), "advertise_addr")?,
        client_addr: required(client_addr, "advertise_addr")?,
        admin_addr: required(admin_addr, "admin_advertise_addr")?,
        observability_addr: required(observability_addr, "observability_advertise_addr")?,
        environment: required(environment, "environment")?,
        region,
        zone,
        rack,
    };
    rr_core::NodeDescriptor {
        node_id: 1,
        client_addr: descriptor.client_addr.clone(),
        admin_addr: descriptor.admin_addr.clone(),
        observability_addr: descriptor.observability_addr.clone(),
        advertise_addr: descriptor.advertise_addr.clone(),
        environment: descriptor.environment.clone(),
        region: descriptor.region.clone(),
        zone: descriptor.zone.clone(),
        rack: descriptor.rack.clone(),
        boot_id: "configuration-validation".into(),
    }
    .validate()
    .map_err(|error| invalid(error.to_string()))?;
    Ok(Some(descriptor))
}

fn resolve_formation(
    args: &ServeArgs,
    cluster: &ClusterSection,
    node_id: u64,
    groups: u64,
    bootstrap: bool,
    has_peers: bool,
) -> Result<Option<ResolvedFormation>> {
    let file_formation = cluster.formation.as_ref();
    if args.formation.is_none() && file_formation.is_none() {
        return Ok(None);
    }
    if bootstrap {
        return Err(invalid(
            "managed formation and manual bootstrap are mutually exclusive",
        ));
    }
    if has_peers {
        return Err(invalid(
            "managed formation and legacy peers are mutually exclusive",
        ));
    }

    let mode = match args
        .formation
        .as_deref()
        .or_else(|| file_formation.and_then(|formation| formation.mode.as_deref()))
    {
        Some("genesis") => FormationMode::Genesis,
        Some("adopt") => FormationMode::AdoptLegacy,
        Some(other) => {
            return Err(invalid(format!(
                "invalid formation mode '{other}': expected 'genesis' or 'adopt'"
            )));
        }
        None => {
            return Err(invalid(
                "missing formation mode: expected 'genesis' or 'adopt'",
            ));
        }
    };
    let etcd_endpoints = if !args.etcd_endpoints.is_empty() {
        args.etcd_endpoints.clone()
    } else {
        file_formation
            .and_then(|formation| formation.etcd_endpoints.clone())
            .unwrap_or_default()
    };
    if etcd_endpoints.is_empty() {
        return Err(invalid(
            "managed formation requires at least one etcd_endpoints entry",
        ));
    }

    let node_entries = if !args.cluster_nodes.is_empty() {
        args.cluster_nodes.clone()
    } else {
        cluster.nodes.clone().unwrap_or_default()
    };
    let mut nodes = BTreeMap::new();
    for entry in node_entries {
        let (raw_id, addr) = entry.split_once('@').ok_or_else(|| {
            invalid(format!(
                "invalid cluster node '{entry}': expected 'id@advertised_addr'"
            ))
        })?;
        if addr.is_empty() {
            return Err(invalid(format!(
                "invalid advertised address in cluster node '{entry}': address is empty"
            )));
        }
        let id = raw_id.parse::<u64>().map_err(|_| {
            invalid(format!(
                "invalid cluster node ID '{raw_id}': expected a u64"
            ))
        })?;
        if nodes
            .insert(
                id,
                NodeAddr {
                    addr: addr.to_string(),
                },
            )
            .is_some()
        {
            return Err(invalid(format!("duplicate cluster node ID {id}")));
        }
    }
    if !nodes.contains_key(&node_id) {
        return Err(invalid(format!(
            "local node {node_id} is missing from the managed formation roster"
        )));
    }
    let cluster_name = args
        .cluster_name
        .clone()
        .or_else(|| cluster.name.clone())
        .unwrap_or_default();
    let seed = SeedSpec::stock(cluster_name, nodes, groups)
        .map_err(|error| invalid(format!("invalid managed formation seed: {error}")))?;

    Ok(Some(ResolvedFormation {
        seed,
        mode,
        etcd_endpoints,
    }))
}

fn resolve_etcd(
    args: &ServeArgs,
    cluster: &ClusterSection,
    enabled: bool,
) -> Result<Option<ResolvedEtcd>> {
    let ca_path = args.etcd_ca.clone().or(cluster.etcd_ca_path.clone());
    let cert_path = args.etcd_cert.clone().or(cluster.etcd_cert_path.clone());
    let key_path = args.etcd_key.clone().or(cluster.etcd_key_path.clone());
    let domain_name = args
        .etcd_domain
        .clone()
        .or(cluster.etcd_domain_name.clone());
    let username = args.etcd_username.clone().or(cluster.etcd_username.clone());
    let password_env = args
        .etcd_password_env
        .clone()
        .or(cluster.etcd_password_env.clone());
    let tls_requested =
        ca_path.is_some() || cert_path.is_some() || key_path.is_some() || domain_name.is_some();
    let auth_requested = username.is_some() || password_env.is_some();
    if !tls_requested && !auth_requested {
        return Ok(None);
    }
    if !enabled {
        return Err(invalid(
            "etcd TLS/auth settings require managed formation or directory discovery",
        ));
    }
    if cert_path.is_some() != key_path.is_some() {
        return Err(invalid(
            "etcd_cert_path and etcd_key_path must both be set for etcd mTLS",
        ));
    }
    if username
        .as_deref()
        .is_some_and(|name| name.trim().is_empty())
    {
        return Err(invalid("etcd_username must not be empty"));
    }
    let password = match (username.as_ref(), password_env.as_deref()) {
        (Some(_), Some(var_name)) => Some(read_etcd_password_env(var_name)?),
        (None, None) => None,
        _ => {
            return Err(invalid(
                "etcd_username and etcd_password_env must both be set for etcd authentication",
            ))
        }
    };
    Ok(Some(ResolvedEtcd {
        ca_path,
        cert_path,
        key_path,
        domain_name,
        username,
        password,
    }))
}

fn read_etcd_password_env(var_name: &str) -> Result<String> {
    match std::env::var(var_name) {
        Ok(password) if !password.is_empty() => Ok(password),
        Ok(_) => Err(invalid(format!(
            "etcd_password_env points to environment variable '{var_name}' which is set but EMPTY; \
             refusing to start without etcd authentication"
        ))),
        Err(_) => Err(invalid(format!(
            "etcd_password_env references environment variable '{var_name}' which is NOT SET; \
             refusing to start without etcd authentication"
        ))),
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
        Ok(_) => Err(invalid(format!(
            "auth.{role}_token_env points to environment variable '{var_name}' \
             which is set but EMPTY. Refusing to start with an empty token: this would \
             silently disable {role} auth (fail-open is a security anti-pattern). \
             Set {var_name}=<non-empty-secret>, or remove auth.{role}_token_env from \
             the config to explicitly disable {role} auth."
        ))),
        Err(_) => Err(invalid(format!(
            "auth.{role}_token_env references environment variable '{var_name}' \
             which is NOT SET. Refusing to start: this would silently disable {role} \
             auth (fail-open). Either set {var_name}=<secret>, or remove \
             auth.{role}_token_env from the config to explicitly disable {role} auth."
        ))),
    }
}

fn resolve_tls(args: &ServeArgs, file: &TlsSection) -> Result<Option<ResolvedTls>> {
    let cert_path = args.tls_cert.clone().or_else(|| file.cert_path.clone());
    let key_path = args.tls_key.clone().or_else(|| file.key_path.clone());
    let ca_path = args.tls_ca.clone().or_else(|| file.ca_path.clone());
    let domain_name = args.tls_domain.clone().or_else(|| file.domain_name.clone());
    // Boolean precedence: CLI affirmative > CLI negation
    // (`--no-require-client-cert`) > config file > default false.
    let require_client_cert = if args.tls_require_client_cert {
        true
    } else if args.no_require_client_cert {
        false
    } else {
        file.require_client_cert
    };

    match (cert_path, key_path) {
        (None, None) => {
            if ca_path.is_some() || require_client_cert {
                return Err(invalid(
                    "TLS options specified but tls_cert / tls_key are missing; TLS cannot be enabled",
                ));
            }
            Ok(None)
        }
        (Some(_), None) | (None, Some(_)) => Err(invalid(
            "tls_cert and tls_key must both be set to enable TLS",
        )),
        (Some(cert), Some(key)) => {
            if require_client_cert && ca_path.is_none() {
                return Err(invalid(
                    "tls_require_client_cert = true requires tls_ca to verify client certs",
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
        .ok_or_else(|| invalid(format!("listen_addr '{addr}' must include a port")))?;
    port.parse::<u16>()
        .map_err(|_| invalid(format!("listen_addr '{addr}' has invalid port")))
}

/// Resolve an admin/metrics bind host: CLI/env wins over the config file,
/// default is loopback. The host must be an IP literal — it is combined
/// with the port into a `SocketAddr` at startup, and a typo'd hostname
/// would otherwise fail deep in `RrNode::start` with an opaque parse error.
fn resolve_bind_host(
    cli: Option<&str>,
    file: Option<&str>,
    default: &'static str,
    field: &'static str,
) -> Result<String> {
    let host = cli.or(file).unwrap_or(default);
    if host.parse::<std::net::IpAddr>().is_err() {
        return Err(invalid(format!(
            "{field} '{host}' is not a valid IP address literal"
        )));
    }
    Ok(host.to_string())
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
    fn bind_hosts_default_to_loopback() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".to_string()),
            ..Default::default()
        };
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.admin_bind_host, "127.0.0.1");
        assert_eq!(resolved.metrics_bind_host, "127.0.0.1");
    }

    #[test]
    fn bind_hosts_file_and_cli_precedence() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"
            admin_bind_host = "0.0.0.0"
            metrics_bind_host = "192.168.1.5"
            "#,
        );
        let args = ServeArgs {
            config: Some(f.path().to_path_buf()),
            log_level: "info".to_string(),
            ..Default::default()
        };
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.admin_bind_host, "0.0.0.0");
        assert_eq!(resolved.metrics_bind_host, "192.168.1.5");

        // CLI wins over the file.
        let args = ServeArgs {
            config: Some(f.path().to_path_buf()),
            log_level: "info".to_string(),
            admin_bind_host: Some("10.0.0.2".to_string()),
            metrics_bind_host: Some("::1".to_string()),
            ..Default::default()
        };
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.admin_bind_host, "10.0.0.2");
        assert_eq!(resolved.metrics_bind_host, "::1");
    }

    #[test]
    fn bind_host_rejects_non_ip_literal() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("127.0.0.1:51001".to_string()),
            admin_bind_host: Some("localhost".to_string()),
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("admin_bind_host"), "got: {err}");
    }

    #[test]
    fn groups_default_override_and_validation() {
        // Default: single group (historical behavior).
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("127.0.0.1:51001".to_string()),
            ..Default::default()
        };
        assert_eq!(ResolvedConfig::resolve(&args).unwrap().groups, 1);

        // CLI wins over the file.
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"
            [cluster]
            groups = 3
            "#,
        );
        let args = ServeArgs {
            config: Some(f.path().to_path_buf()),
            log_level: "info".to_string(),
            groups: Some(5),
            ..Default::default()
        };
        assert_eq!(ResolvedConfig::resolve(&args).unwrap().groups, 5);
    }

    #[test]
    fn auto_promote_learners_defaults_true_and_toml_override() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        assert!(
            ResolvedConfig::resolve(&args)
                .unwrap()
                .auto_promote_learners
        );

        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"

            [cluster]
            auto_promote_learners = false
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        assert!(
            !ResolvedConfig::resolve(&args)
                .unwrap()
                .auto_promote_learners
        );

        // 0 is rejected — it would start no Raft group at all.
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("127.0.0.1:51001".to_string()),
            groups: Some(0),
            ..Default::default()
        };
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("groups"), "got: {err}");
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
    fn balanced_alias_is_a_hard_error() {
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
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("replicated_only"),
            "the removed alias must name its canonical replacement: {msg}"
        );
    }

    #[test]
    fn fast_alias_is_a_hard_error() {
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
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("test_only"),
            "the removed alias must name its canonical replacement: {msg}"
        );
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
    fn grpc_max_frame_size_above_entry_body_cap_rejected() {
        let over = rr_core::bounded_decode::MAX_ENTRY_BODY as u64 + 1;
        let f = write_toml(&format!(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            grpc_max_frame_size = {over}
            "#,
        ));
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(
            err.to_string().contains("MAX_ENTRY_BODY"),
            "expected the cap-coupling error, got: {err}"
        );
    }

    #[test]
    fn grpc_max_frame_size_at_entry_body_cap_accepted() {
        let at = rr_core::bounded_decode::MAX_ENTRY_BODY as u64;
        let f = write_toml(&format!(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            grpc_max_frame_size = {at}
            "#,
        ));
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        assert_eq!(resolved.grpc_max_frame_size, Some(at as u32));
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
    fn managed_formation_config_resolves() {
        let f = write_toml(
            r#"
            [node]
            id = 2
            listen_addr = "0.0.0.0:51002"

            [cluster]
            name = "stock"
            nodes = ["1@node-1:51001", "2@node-2:51002"]
            groups = 2

            [cluster.formation]
            mode = "genesis"
            etcd_endpoints = ["http://127.0.0.1:2379"]
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        ResolvedConfig::resolve(&args).unwrap();
    }

    #[test]
    fn managed_formation_cli_overrides_file_and_preserves_bind_address() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            name = "file-cluster"
            nodes = ["1@file-node:51001"]
            groups = 2

            [cluster.formation]
            mode = "genesis"
            etcd_endpoints = ["http://file-etcd:2379"]
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.node_id = Some(2);
        args.listen_addr = Some("0.0.0.0:52002".to_string());
        args.cluster_name = Some("cli-cluster".to_string());
        args.cluster_nodes = vec![
            "1@advertised-1:52001".to_string(),
            "2@advertised-2:52002".to_string(),
        ];
        args.formation = Some("adopt".to_string());
        args.etcd_endpoints = vec!["http://cli-etcd:2379".to_string()];

        let resolved = ResolvedConfig::resolve(&args).unwrap();
        let formation = resolved.formation.as_ref().unwrap();
        assert_eq!(resolved.listen_addr, "0.0.0.0:52002");
        assert_eq!(formation.seed.cluster_name, "cli-cluster");
        assert_eq!(formation.seed.nodes.len(), 2);
        assert_eq!(formation.seed.nodes[&2].addr, "advertised-2:52002");
        assert_eq!(formation.seed.groups.len(), 2);
        assert_eq!(formation.mode, FormationMode::AdoptLegacy);
        assert_eq!(formation.etcd_endpoints, vec!["http://cli-etcd:2379"]);
    }

    #[test]
    fn managed_formation_conflicts_with_manual_bootstrap() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            name = "stock"
            nodes = ["1@node-1:51001"]
            bootstrap = true

            [cluster.formation]
            mode = "genesis"
            etcd_endpoints = ["http://127.0.0.1:2379"]
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("bootstrap"), "got: {message}");
        assert!(!message.contains("unknown field"), "got: {message}");
    }

    #[test]
    fn managed_formation_conflicts_with_legacy_peers() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            name = "stock"
            nodes = ["1@node-1:51001"]
            peers = ["node-1:51001"]

            [cluster.formation]
            mode = "genesis"
            etcd_endpoints = ["http://127.0.0.1:2379"]
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("peers"), "got: {message}");
        assert!(!message.contains("unknown field"), "got: {message}");
    }

    #[test]
    fn managed_formation_requires_local_node_in_roster() {
        let f = write_toml(
            r#"
            [node]
            id = 3
            listen_addr = "127.0.0.1:51003"

            [cluster]
            name = "stock"
            nodes = ["1@node-1:51001", "2@node-2:51002"]

            [cluster.formation]
            mode = "genesis"
            etcd_endpoints = ["http://127.0.0.1:2379"]
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("node 3"), "got: {err}");
    }

    #[test]
    fn managed_formation_rejects_duplicate_node_ids() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            name = "stock"
            nodes = ["1@node-1:51001", "1@node-1-other:51001"]

            [cluster.formation]
            mode = "genesis"
            etcd_endpoints = ["http://127.0.0.1:2379"]
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("duplicate"), "got: {err}");
    }

    #[test]
    fn managed_formation_rejects_empty_advertised_address() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            name = "stock"
            nodes = ["1@"]

            [cluster.formation]
            mode = "genesis"
            etcd_endpoints = ["http://127.0.0.1:2379"]
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("address"), "got: {err}");
    }

    #[test]
    fn managed_formation_requires_etcd_endpoints() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            name = "stock"
            nodes = ["1@node-1:51001"]

            [cluster.formation]
            mode = "adopt"
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("etcd_endpoints"), "got: {err}");
    }

    #[test]
    fn managed_formation_rejects_unknown_mode() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            name = "stock"
            nodes = ["1@node-1:51001"]

            [cluster.formation]
            mode = "automatic"
            etcd_endpoints = ["http://127.0.0.1:2379"]
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let err = ResolvedConfig::resolve(&args).unwrap_err();
        assert!(err.to_string().contains("formation mode"), "got: {err}");
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

    // ── Boolean CLI-negation merge semantics (I5) ──────────────────────────
    // Contract: CLI affirmative > CLI negation > config file > default false.

    #[test]
    fn bootstrap_file_true_with_cli_negation_resolves_false() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            bootstrap = true
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.no_bootstrap = true;
        assert!(!ResolvedConfig::resolve(&args).unwrap().bootstrap);
    }

    #[test]
    fn bootstrap_file_true_without_cli_flag_stays_true() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            bootstrap = true
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        assert!(ResolvedConfig::resolve(&args).unwrap().bootstrap);
    }

    #[test]
    fn bootstrap_cli_affirmative_beats_file_false() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [cluster]
            bootstrap = false
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.bootstrap = true;
        assert!(ResolvedConfig::resolve(&args).unwrap().bootstrap);
    }

    #[test]
    fn bootstrap_defaults_false_when_nothing_set() {
        let args = ServeArgs {
            log_level: "info".to_string(),
            node_id: Some(1),
            listen_addr: Some("127.0.0.1:51001".to_string()),
            ..Default::default()
        };
        assert!(!ResolvedConfig::resolve(&args).unwrap().bootstrap);
    }

    #[test]
    fn allow_insecure_file_true_with_cli_negation_resolves_false() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"
            allow_insecure = true
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.no_allow_insecure = true;
        assert!(!ResolvedConfig::resolve(&args).unwrap().allow_insecure);
    }

    #[test]
    fn allow_insecure_file_true_without_cli_flag_stays_true() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"
            allow_insecure = true
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        assert!(ResolvedConfig::resolve(&args).unwrap().allow_insecure);
    }

    #[test]
    fn require_client_cert_file_true_with_cli_negation_resolves_false() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [tls]
            cert_path = "/srv/rr/cert.pem"
            key_path = "/srv/rr/key.pem"
            ca_path = "/srv/rr/ca.pem"
            require_client_cert = true
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.no_require_client_cert = true;
        let tls = ResolvedConfig::resolve(&args).unwrap().tls.expect("tls");
        assert!(!tls.require_client_cert);
    }

    #[test]
    fn require_client_cert_file_true_without_cli_flag_stays_true() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [tls]
            cert_path = "/srv/rr/cert.pem"
            key_path = "/srv/rr/key.pem"
            ca_path = "/srv/rr/ca.pem"
            require_client_cert = true
            "#,
        );
        let args = args_with_config(f.path().to_path_buf());
        let tls = ResolvedConfig::resolve(&args).unwrap().tls.expect("tls");
        assert!(tls.require_client_cert);
    }

    #[test]
    fn require_client_cert_cli_affirmative_beats_file_false() {
        let f = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "127.0.0.1:51001"

            [tls]
            cert_path = "/srv/rr/cert.pem"
            key_path = "/srv/rr/key.pem"
            ca_path = "/srv/rr/ca.pem"
            "#,
        );
        let mut args = args_with_config(f.path().to_path_buf());
        args.tls_require_client_cert = true;
        let tls = ResolvedConfig::resolve(&args).unwrap().tls.expect("tls");
        assert!(tls.require_client_cert);
    }

    #[test]
    fn boot_journal_defaults_to_volatile_for_mem_and_data_dir_for_rocks() {
        let mem = ResolvedConfig::resolve(&ServeArgs {
            node_id: Some(1),
            listen_addr: Some("127.0.0.1:51001".into()),
            log_level: "info".into(),
            ..Default::default()
        })
        .unwrap();
        assert!(mem.boot_journal_dir.is_none());

        let rocks = ResolvedConfig::resolve(&ServeArgs {
            node_id: Some(1),
            listen_addr: Some("127.0.0.1:51001".into()),
            storage: Some("rocks".into()),
            data_dir: Some(PathBuf::from("/var/lib/rr")),
            log_level: "info".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            rocks.boot_journal_dir,
            Some(PathBuf::from("/var/lib/rr/boot-journal"))
        );
    }

    #[test]
    fn explicit_descriptor_rejects_wildcards_and_incomplete_endpoints() {
        let incomplete = ResolvedConfig::resolve(&ServeArgs {
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".into()),
            advertise_addr: Some("node-1:51001".into()),
            log_level: "info".into(),
            ..Default::default()
        })
        .unwrap_err();
        assert!(incomplete
            .to_string()
            .contains("incomplete node descriptor"));

        let wildcard = ResolvedConfig::resolve(&ServeArgs {
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".into()),
            advertise_addr: Some("0.0.0.0:51001".into()),
            admin_advertise_addr: Some("node-1:52001".into()),
            observability_advertise_addr: Some("node-1:9100".into()),
            environment: Some("test".into()),
            log_level: "info".into(),
            ..Default::default()
        })
        .unwrap_err();
        assert!(wildcard.to_string().contains("wildcard"));
    }

    #[test]
    fn etcd_tls_and_auth_resolve_without_exposing_the_password_in_debug() {
        let unique_var = format!("RR_TEST_ETCD_PASSWORD_{}", std::process::id());
        std::env::set_var(&unique_var, "etcd-secret");
        let f = write_toml(&format!(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            advertise_addr = "node-1:51001"
            admin_advertise_addr = "node-1:52001"
            observability_advertise_addr = "node-1:9100"
            environment = "test"

            [cluster]
            name = "stock"
            directory_etcd_endpoints = ["https://etcd:2379"]
            etcd_ca_path = "/etc/rr/etcd-ca.pem"
            etcd_cert_path = "/etc/rr/etcd-client.pem"
            etcd_key_path = "/etc/rr/etcd-client.key"
            etcd_domain_name = "etcd.internal"
            etcd_username = "rr-node"
            etcd_password_env = "{unique_var}"
            "#
        ));
        let args = args_with_config(f.path().to_path_buf());
        let resolved = ResolvedConfig::resolve(&args).unwrap();
        std::env::remove_var(&unique_var);

        let etcd = resolved.etcd.as_ref().unwrap();
        assert_eq!(etcd.ca_path, Some(PathBuf::from("/etc/rr/etcd-ca.pem")));
        assert_eq!(
            etcd.cert_path,
            Some(PathBuf::from("/etc/rr/etcd-client.pem"))
        );
        assert_eq!(
            etcd.key_path,
            Some(PathBuf::from("/etc/rr/etcd-client.key"))
        );
        assert_eq!(etcd.domain_name.as_deref(), Some("etcd.internal"));
        assert_eq!(etcd.username.as_deref(), Some("rr-node"));
        assert_eq!(etcd.password.as_deref(), Some("etcd-secret"));
        assert!(!format!("{resolved:?}").contains("etcd-secret"));
    }

    #[test]
    fn etcd_auth_requires_complete_credentials_and_https() {
        let incomplete = write_toml(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            advertise_addr = "node-1:51001"
            admin_advertise_addr = "node-1:52001"
            observability_advertise_addr = "node-1:9100"
            environment = "test"

            [cluster]
            name = "stock"
            directory_etcd_endpoints = ["https://etcd:2379"]
            etcd_username = "rr-node"
            "#,
        );
        let error = ResolvedConfig::resolve(&args_with_config(incomplete.path().to_path_buf()))
            .unwrap_err();
        assert!(error.to_string().contains("etcd_password_env"));

        let unique_var = format!("RR_TEST_ETCD_HTTP_PASSWORD_{}", std::process::id());
        std::env::set_var(&unique_var, "etcd-secret");
        let plaintext = write_toml(&format!(
            r#"
            [node]
            id = 1
            listen_addr = "0.0.0.0:51001"
            advertise_addr = "node-1:51001"
            admin_advertise_addr = "node-1:52001"
            observability_advertise_addr = "node-1:9100"
            environment = "test"

            [cluster]
            name = "stock"
            directory_etcd_endpoints = ["http://etcd:2379"]
            etcd_username = "rr-node"
            etcd_password_env = "{unique_var}"
            "#
        ));
        let error =
            ResolvedConfig::resolve(&args_with_config(plaintext.path().to_path_buf())).unwrap_err();
        std::env::remove_var(&unique_var);
        assert!(error.to_string().contains("https"), "got: {error}");
    }

    #[test]
    fn managed_formation_reuses_etcd_for_explicit_directory_descriptor() {
        let resolved = ResolvedConfig::resolve(&ServeArgs {
            node_id: Some(1),
            listen_addr: Some("0.0.0.0:51001".into()),
            advertise_addr: Some("node-1:51001".into()),
            admin_advertise_addr: Some("node-1:52001".into()),
            observability_advertise_addr: Some("node-1:9100".into()),
            environment: Some("test".into()),
            cluster_name: Some("stock".into()),
            cluster_nodes: vec!["1@node-1:51001".into()],
            formation: Some("genesis".into()),
            etcd_endpoints: vec!["http://etcd:2379".into()],
            log_level: "info".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(resolved.directory_etcd_endpoints, vec!["http://etcd:2379"]);
        assert!(resolved.descriptor.is_some());
    }
}
