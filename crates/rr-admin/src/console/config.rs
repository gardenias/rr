use super::metrics::ClusterMatcher;
use anyhow::{ensure, Context, Result};
use clap::Args;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Args, Clone, Debug)]
pub(crate) struct ConsoleArgs {
    /// Address for the same-origin Console HTTP/SSE server.
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,

    /// Comma-separated AdminService seeds (node_id@host:port), not client/bootstrap peers.
    #[arg(long, value_name = "NODE@ADMIN_ADDR,...")]
    pub peers: Option<String>,

    /// Trusted NodeDirectory and ConfigProvider discovery backend.
    #[arg(long, value_name = "URLS")]
    pub discovery: Option<String>,

    /// CA bundle used to authenticate the etcd discovery server.
    #[arg(long, value_name = "PATH", requires = "discovery")]
    pub discovery_tls_ca: Option<PathBuf>,

    /// Client certificate presented to etcd for mutual TLS.
    #[arg(long, value_name = "PATH", requires = "discovery")]
    pub discovery_tls_cert: Option<PathBuf>,

    /// Client private key paired with --discovery-tls-cert.
    #[arg(long, value_name = "PATH", requires = "discovery")]
    pub discovery_tls_key: Option<PathBuf>,

    /// SNI and certificate name override for etcd discovery.
    #[arg(long, value_name = "NAME", requires = "discovery")]
    pub discovery_tls_domain: Option<String>,

    /// Environment variable containing the etcd password; never accepts a password in argv.
    #[arg(long, value_name = "ENV_VAR", requires = "discovery")]
    pub discovery_password_env: Option<String>,

    /// etcd username used with --discovery-password-env.
    #[arg(long, value_name = "NAME", requires = "discovery")]
    pub discovery_username: Option<String>,

    /// Explicit development opt-in for unauthenticated plaintext etcd discovery.
    #[arg(long, requires = "discovery")]
    pub discovery_allow_insecure: bool,

    /// Cluster namespace used by discovery and as the untracked display identity.
    #[arg(long, value_name = "NAME", requires = "discovery")]
    pub cluster_name: Option<String>,

    /// Environment variable containing the viewer credential; never accepts a token in argv.
    #[arg(long, value_name = "ENV_VAR", default_value = "RR_CONSOLE_TOKEN")]
    pub console_token_env: String,

    /// Development escape hatch for loopback only. Public listeners are always rejected.
    #[arg(long)]
    pub allow_insecure: bool,

    /// Console HTTPS certificate (requires --tls-key).
    #[arg(long, value_name = "PATH", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,

    /// Console HTTPS private key (requires --tls-cert).
    #[arg(long, value_name = "PATH", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,

    /// Shared cluster summary polling interval.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=300))]
    pub poll_interval_secs: u64,

    /// External Prometheus base URL for retained history; requires --prometheus-cluster.
    #[arg(long, value_name = "URL", requires = "prometheus_cluster")]
    pub prometheus_url: Option<String>,

    /// Exact cluster matcher required for every fixed history query (label=value).
    #[arg(long, value_name = "LABEL=VALUE", requires = "prometheus_url")]
    pub prometheus_cluster: Option<String>,

    /// Static Next.js export directory. A debug fallback is served when absent.
    #[arg(long, value_name = "PATH", default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/console/out"))]
    pub static_dir: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SeedPeer {
    pub node_id: u64,
    pub admin_addr: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PrometheusConfig {
    pub url: String,
    pub cluster: ClusterMatcher,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DiscoverySecurityConfig {
    pub tls_ca: Option<PathBuf>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_domain: Option<String>,
    pub username: Option<String>,
    pub password_env: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DiscoveryTransportConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub keep_alive_interval: Duration,
    pub keep_alive_timeout: Duration,
    pub tcp_keepalive: Duration,
}

impl Default for DiscoveryTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            keep_alive_interval: Duration::from_secs(10),
            keep_alive_timeout: Duration::from_secs(5),
            tcp_keepalive: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ConsoleConfig {
    pub listen: SocketAddr,
    pub peers: Vec<SeedPeer>,
    pub discovery: Option<String>,
    pub discovery_endpoints: Vec<String>,
    pub discovery_security: DiscoverySecurityConfig,
    pub discovery_transport: DiscoveryTransportConfig,
    pub cluster_name: Option<String>,
    pub viewer_token: Option<Arc<str>>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub poll_interval: Duration,
    pub prometheus: Option<PrometheusConfig>,
    pub static_dir: PathBuf,
}

impl ConsoleConfig {
    pub fn using_tls(&self) -> bool {
        self.tls_cert.is_some()
    }
}

impl ConsoleArgs {
    pub fn validate(&self) -> Result<ConsoleConfig> {
        self.validate_with(|name| std::env::var(name).ok())
    }

    fn validate_with(&self, env: impl Fn(&str) -> Option<String>) -> Result<ConsoleConfig> {
        ensure!(
            self.peers.is_some() || self.discovery.is_some(),
            "console requires --peers AdminService seeds or --discovery"
        );
        if self.discovery.is_some() {
            ensure!(
                self.cluster_name
                    .as_deref()
                    .is_some_and(|name| !name.trim().is_empty()),
                "--cluster-name is required with --discovery"
            );
            ensure!(
                !(self.discovery_tls_ca.is_some() && self.discovery_allow_insecure),
                "--discovery-tls-ca cannot be combined with --discovery-allow-insecure"
            );
            ensure!(
                self.discovery_tls_ca.is_some() || self.discovery_allow_insecure,
                "--discovery requires --discovery-tls-ca or explicit --discovery-allow-insecure for legacy plaintext"
            );
            ensure!(
                self.discovery_tls_cert.is_some() == self.discovery_tls_key.is_some(),
                "--discovery-tls-cert and --discovery-tls-key must be provided together"
            );
            ensure!(
                self.discovery_tls_cert.is_none() || self.discovery_tls_ca.is_some(),
                "--discovery-tls-cert and --discovery-tls-key require --discovery-tls-ca"
            );
            ensure!(
                self.discovery_tls_domain.is_none() || self.discovery_tls_ca.is_some(),
                "--discovery-tls-domain requires --discovery-tls-ca"
            );
            ensure!(
                self.discovery_username.is_some() == self.discovery_password_env.is_some(),
                "--discovery-username and --discovery-password-env must be provided together"
            );
            if let (Some(username), Some(password_env)) = (
                self.discovery_username.as_deref(),
                self.discovery_password_env.as_deref(),
            ) {
                ensure!(
                    !username.trim().is_empty(),
                    "--discovery-username must not be empty"
                );
                ensure!(
                    !password_env.trim().is_empty(),
                    "--discovery-password-env must not be empty"
                );
                ensure!(
                    self.discovery_tls_ca.is_some(),
                    "etcd discovery authentication requires TLS via --discovery-tls-ca"
                );
                ensure!(
                    env(password_env).is_some_and(|password| !password.is_empty()),
                    "etcd discovery password environment variable ${password_env} must be set and non-empty"
                );
            }
        }
        ensure!(
            !self.listen.ip().is_unspecified() || !self.allow_insecure,
            "--allow-insecure is loopback-only; a non-loopback Console always requires HTTPS and authentication"
        );
        ensure!(
            self.tls_cert.is_some() == self.tls_key.is_some(),
            "--tls-cert and --tls-key must be provided together"
        );

        let viewer_token = env(&self.console_token_env).filter(|token| !token.is_empty());
        if !self.listen.ip().is_loopback() {
            ensure!(!self.allow_insecure, "--allow-insecure is loopback-only");
            ensure!(
                self.tls_cert.is_some(),
                "non-loopback Console listeners require TLS (--tls-cert/--tls-key) and a viewer token"
            );
            ensure!(
                viewer_token.is_some(),
                "non-loopback Console listeners require a viewer token in ${}",
                self.console_token_env
            );
        }

        let discovery_endpoints = self
            .discovery
            .as_deref()
            .map(parse_discovery_endpoints)
            .transpose()?
            .unwrap_or_default();
        let peers = match self.peers.as_deref() {
            Some(_) => parse_peers(self.peers.as_deref())?,
            None => Vec::new(),
        };
        let prometheus = match (&self.prometheus_url, &self.prometheus_cluster) {
            (None, None) => None,
            (Some(url), Some(cluster)) => Some(PrometheusConfig {
                url: parse_prometheus_url(url)?,
                cluster: parse_cluster_matcher(cluster)?,
            }),
            _ => anyhow::bail!(
                "--prometheus-url and --prometheus-cluster label=value must be provided together"
            ),
        };
        Ok(ConsoleConfig {
            listen: self.listen,
            peers,
            discovery: self.discovery.clone(),
            discovery_endpoints,
            discovery_security: DiscoverySecurityConfig {
                tls_ca: self.discovery_tls_ca.clone(),
                tls_cert: self.discovery_tls_cert.clone(),
                tls_key: self.discovery_tls_key.clone(),
                tls_domain: self.discovery_tls_domain.clone(),
                username: self.discovery_username.clone(),
                password_env: self.discovery_password_env.clone(),
            },
            discovery_transport: DiscoveryTransportConfig::default(),
            cluster_name: self.cluster_name.clone(),
            viewer_token: viewer_token.map(Arc::from),
            tls_cert: self.tls_cert.clone(),
            tls_key: self.tls_key.clone(),
            poll_interval: Duration::from_secs(self.poll_interval_secs),
            prometheus,
            static_dir: self.static_dir.clone(),
        })
    }
}

fn parse_prometheus_url(input: &str) -> Result<String> {
    let url =
        reqwest::Url::parse(input).context("--prometheus-url must be an absolute HTTP URL")?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
        "--prometheus-url must be an absolute HTTP or HTTPS URL"
    );
    Ok(input.to_string())
}

fn parse_cluster_matcher(input: &str) -> Result<ClusterMatcher> {
    let (label, value) = input
        .split_once('=')
        .context("--prometheus-cluster must be an exact label=value matcher")?;
    let mut label_bytes = label.bytes();
    ensure!(
        label_bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
            && label_bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "--prometheus-cluster has an invalid Prometheus label name"
    );
    ensure!(
        !value.is_empty() && !value.contains('='),
        "--prometheus-cluster must contain exactly one non-empty label=value matcher"
    );
    Ok(ClusterMatcher {
        label: label.to_string(),
        value: value.to_string(),
    })
}

pub(crate) fn parse_discovery_endpoints(input: &str) -> Result<Vec<String>> {
    let input = input
        .strip_prefix("etcd://")
        .context("--discovery must use etcd://host:port[,host:port...]")?;
    let mut endpoints = Vec::new();
    let mut unique = HashSet::new();
    for endpoint in input.split(',') {
        let endpoint = endpoint.trim();
        ensure!(
            !endpoint.is_empty(),
            "--discovery contains an empty endpoint"
        );
        ensure!(
            !endpoint.contains("://") && !endpoint.chars().any(char::is_whitespace),
            "discovery endpoint {endpoint:?} must be host:port without a nested scheme"
        );
        let (host, port) = endpoint.rsplit_once(':').with_context(|| {
            format!("discovery endpoint {endpoint:?} must include host and port")
        })?;
        let host = host.trim_matches(['[', ']']);
        ensure!(
            !host.is_empty(),
            "discovery endpoint {endpoint:?} has no host"
        );
        ensure!(
            host != "0.0.0.0" && host != "::",
            "discovery endpoint {endpoint:?} uses a wildcard address"
        );
        ensure!(
            port.parse::<u16>().is_ok_and(|port| port != 0),
            "discovery endpoint {endpoint:?} has an invalid port"
        );
        ensure!(
            unique.insert(endpoint.to_string()),
            "duplicate discovery endpoint {endpoint}"
        );
        endpoints.push(endpoint.to_string());
    }
    ensure!(
        !endpoints.is_empty(),
        "at least one discovery endpoint is required"
    );
    Ok(endpoints)
}

pub(crate) fn parse_peers(input: Option<&str>) -> Result<Vec<SeedPeer>> {
    let input = input.context("at least one --peers AdminService seed is required")?;
    let mut peers = Vec::new();
    let mut node_ids = HashSet::new();
    let mut addresses = HashSet::new();
    for value in input.split(',') {
        let value = value.trim();
        ensure!(!value.is_empty(), "--peers contains an empty seed");
        let (node_id, admin_addr) = value.split_once('@').with_context(|| {
            format!("invalid AdminService seed {value:?}; expected node_id@host:port")
        })?;
        let node_id: u64 = node_id
            .parse()
            .with_context(|| format!("invalid node ID in AdminService seed {value:?}"))?;
        ensure!(
            node_id != 0,
            "AdminService seed node IDs must be greater than zero"
        );
        ensure!(
            !admin_addr.is_empty(),
            "AdminService seed {value:?} has no address"
        );
        ensure!(
            !admin_addr.contains("://") && !admin_addr.chars().any(char::is_whitespace),
            "AdminService seed {value:?} must be host:port without a scheme"
        );
        let host = admin_addr
            .rsplit_once(':')
            .map(|(host, _)| host.trim_matches(['[', ']']))
            .unwrap_or_default();
        ensure!(
            !host.is_empty(),
            "AdminService seed {value:?} must include host and port"
        );
        ensure!(
            host != "0.0.0.0" && host != "::",
            "AdminService seed {value:?} uses a wildcard address"
        );
        admin_addr
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .filter(|port| *port != 0)
            .with_context(|| format!("AdminService seed {value:?} has an invalid port"))?;
        ensure!(
            node_ids.insert(node_id),
            "duplicate AdminService seed node ID {node_id}"
        );
        ensure!(
            addresses.insert(admin_addr),
            "duplicate AdminService address {admin_addr}"
        );
        peers.push(SeedPeer {
            node_id,
            admin_addr: admin_addr.to_string(),
        });
    }
    ensure!(
        !peers.is_empty(),
        "at least one --peers AdminService seed is required"
    );
    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        console: ConsoleArgs,
    }

    fn args(listen: &str) -> ConsoleArgs {
        ConsoleArgs {
            listen: listen.parse().unwrap(),
            peers: Some("1@10.0.0.1:52001, 2@10.0.0.2:52001".to_string()),
            discovery: None,
            discovery_tls_ca: None,
            discovery_tls_cert: None,
            discovery_tls_key: None,
            discovery_tls_domain: None,
            discovery_password_env: None,
            discovery_username: None,
            discovery_allow_insecure: false,
            cluster_name: None,
            console_token_env: "RR_CONSOLE_TOKEN".to_string(),
            allow_insecure: false,
            tls_cert: None,
            tls_key: None,
            poll_interval_secs: 5,
            prometheus_url: None,
            prometheus_cluster: None,
            static_dir: PathBuf::from("unused"),
        }
    }

    #[test]
    fn parses_explicit_admin_seeds_without_deriving_ports() {
        let peers = parse_peers(args("127.0.0.1:8080").peers.as_deref()).unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id, 1);
        assert_eq!(peers[0].admin_addr, "10.0.0.1:52001");
        assert_eq!(peers[1].admin_addr, "10.0.0.2:52001");
    }

    #[test]
    fn rejects_empty_duplicate_and_wildcard_seeds() {
        for peers in [
            "1@10.0.0.1:52001,1@10.0.0.2:52001",
            "1@10.0.0.1:52001,2@10.0.0.1:52001",
            "1@0.0.0.0:52001",
            "1@:52001",
            "not-a-peer",
            "0@10.0.0.1:52001",
        ] {
            assert!(parse_peers(Some(peers)).is_err(), "accepted {peers}");
        }
        assert!(parse_peers(None).is_err());
        assert!(parse_peers(Some("")).is_err());
    }

    #[test]
    fn non_loopback_requires_https_and_a_viewer_token() {
        let mut public = args("0.0.0.0:8443");
        let error = public.validate_with(|_| None).unwrap_err().to_string();
        assert!(error.contains("non-loopback") && error.contains("TLS") && error.contains("token"));

        public.tls_cert = Some(PathBuf::from("cert.pem"));
        public.tls_key = Some(PathBuf::from("key.pem"));
        assert!(public.validate_with(|_| None).is_err());
        let config = public
            .validate_with(|_| Some("viewer".to_string()))
            .unwrap();
        assert!(config.viewer_token.is_some());
        assert!(config.using_tls());
    }

    #[test]
    fn allow_insecure_cannot_make_a_public_listener_valid() {
        let mut public = args("0.0.0.0:8443");
        public.allow_insecure = true;
        public.tls_cert = Some(PathBuf::from("cert.pem"));
        public.tls_key = Some(PathBuf::from("key.pem"));
        let error = public
            .validate_with(|_| Some("viewer".to_string()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("--allow-insecure") && error.contains("loopback"));
    }

    #[test]
    fn loopback_viewer_authentication_is_optional_but_explicit_tls_is_a_pair() {
        let local = args("127.0.0.1:8080").validate_with(|_| None).unwrap();
        assert!(local.viewer_token.is_none());
        assert!(!local.using_tls());

        let mut invalid = args("127.0.0.1:8080");
        invalid.tls_cert = Some(PathBuf::from("cert.pem"));
        assert!(invalid
            .validate_with(|_| None)
            .unwrap_err()
            .to_string()
            .contains("--tls-key"));
    }

    #[test]
    fn prometheus_history_requires_an_exact_paired_cluster_matcher() {
        let mut configured = args("127.0.0.1:8080");
        configured.prometheus_url = Some("http://127.0.0.1:9090".to_string());
        let error = configured.validate_with(|_| None).unwrap_err().to_string();
        assert!(error.contains("--prometheus-cluster"));

        configured.prometheus_cluster = Some("cluster_id=prod-east".to_string());
        let config = configured.validate_with(|_| None).unwrap();
        let prometheus = config.prometheus.unwrap();
        assert_eq!(prometheus.url, "http://127.0.0.1:9090");
        assert_eq!(prometheus.cluster.label, "cluster_id");
        assert_eq!(prometheus.cluster.value, "prod-east");

        let mut missing_url = args("127.0.0.1:8080");
        missing_url.prometheus_cluster = Some("cluster_id=prod-east".to_string());
        assert!(missing_url.validate_with(|_| None).is_err());

        for matcher in [
            "",
            "cluster_id",
            "=prod-east",
            "cluster_id=",
            "bad-label=prod",
            "cluster_id=prod=unexpected",
        ] {
            configured.prometheus_cluster = Some(matcher.to_string());
            assert!(
                configured.validate_with(|_| None).is_err(),
                "accepted {matcher:?}"
            );
        }
        configured.prometheus_cluster = Some("cluster_id=prod".to_string());
        configured.prometheus_url = Some("file:///tmp/prometheus".to_string());
        assert!(configured.validate_with(|_| None).is_err());
    }

    #[test]
    fn discovery_requires_cluster_name_and_a_roster_source_is_required() {
        let mut discovery = args("127.0.0.1:8080");
        discovery.peers = None;
        discovery.discovery = Some("etcd://127.0.0.1:2379".to_string());
        assert!(discovery
            .validate_with(|_| None)
            .unwrap_err()
            .to_string()
            .contains("--cluster-name"));
        discovery.cluster_name = Some("prod".to_string());
        discovery.discovery_allow_insecure = true;
        discovery.discovery =
            Some("etcd://127.0.0.1:2379, 127.0.0.2:2379,127.0.0.3:2379".to_string());
        let config = discovery.validate_with(|_| None).unwrap();
        assert_eq!(
            config.discovery_endpoints,
            vec!["127.0.0.1:2379", "127.0.0.2:2379", "127.0.0.3:2379"]
        );

        for invalid in [
            "http://127.0.0.1:2379",
            "etcd://",
            "etcd://0.0.0.0:2379",
            "etcd://127.0.0.1",
            "etcd://127.0.0.1:0",
            "etcd://127.0.0.1:2379,127.0.0.1:2379",
        ] {
            discovery.discovery = Some(invalid.to_string());
            assert!(
                discovery.validate_with(|_| None).is_err(),
                "accepted {invalid}"
            );
        }

        discovery.discovery = None;
        assert!(discovery.validate_with(|_| None).is_err());
    }

    #[test]
    fn discovery_validation_applies_bounded_transport_defaults() {
        let mut discovery = args("127.0.0.1:8080");
        discovery.peers = None;
        discovery.discovery = Some("etcd://127.0.0.1:2379".to_string());
        discovery.cluster_name = Some("prod".to_string());
        discovery.discovery_allow_insecure = true;
        let config = discovery.validate_with(|_| None).unwrap();
        assert_eq!(
            config.discovery_transport,
            DiscoveryTransportConfig {
                connect_timeout: Duration::from_secs(3),
                request_timeout: Duration::from_secs(5),
                keep_alive_interval: Duration::from_secs(10),
                keep_alive_timeout: Duration::from_secs(5),
                tcp_keepalive: Duration::from_secs(30),
            }
        );
    }

    #[test]
    fn secured_discovery_tls_and_auth_flags_are_accepted() {
        let parsed = TestCli::try_parse_from([
            "test",
            "--peers",
            "1@127.0.0.1:52001",
            "--discovery",
            "etcd://127.0.0.1:2379",
            "--cluster-name",
            "prod",
            "--discovery-tls-ca",
            "ca.pem",
            "--discovery-tls-cert",
            "client.pem",
            "--discovery-tls-key",
            "client.key",
            "--discovery-tls-domain",
            "etcd.internal",
            "--discovery-username",
            "console",
            "--discovery-password-env",
            "RR_CONSOLE_ETCD_PASSWORD",
        ]);
        let cli =
            parsed.unwrap_or_else(|error| panic!("secured discovery flags were rejected: {error}"));
        let secret = "not-for-argv-or-logs";
        let config = cli
            .console
            .validate_with(|name| (name == "RR_CONSOLE_ETCD_PASSWORD").then(|| secret.to_string()))
            .unwrap();
        assert_eq!(
            config.discovery_security.tls_ca,
            Some(PathBuf::from("ca.pem"))
        );
        assert_eq!(
            config.discovery_security.tls_domain.as_deref(),
            Some("etcd.internal")
        );
        assert_eq!(
            config.discovery_security.username.as_deref(),
            Some("console")
        );
        assert!(!format!("{config:?}").contains(secret));
    }

    #[test]
    fn legacy_plaintext_discovery_requires_explicit_opt_in() {
        let mut discovery = args("127.0.0.1:8080");
        discovery.peers = None;
        discovery.discovery = Some("etcd://127.0.0.1:2379".to_string());
        discovery.cluster_name = Some("prod".to_string());
        let error = discovery
            .validate_with(|_| None)
            .expect_err("plaintext discovery must not be implicit")
            .to_string();
        assert!(error.contains("--discovery-allow-insecure"));

        discovery.discovery_allow_insecure = true;
        assert!(discovery.validate_with(|_| None).is_ok());
    }

    #[test]
    fn discovery_security_rejects_malformed_pairs_and_plaintext_credentials() {
        let secure = || {
            let mut configured = args("127.0.0.1:8080");
            configured.peers = None;
            configured.discovery = Some("etcd://127.0.0.1:2379".to_string());
            configured.cluster_name = Some("prod".to_string());
            configured.discovery_tls_ca = Some(PathBuf::from("ca.pem"));
            configured
        };

        let mut cert_without_key = secure();
        cert_without_key.discovery_tls_cert = Some(PathBuf::from("client.pem"));
        let mut key_without_cert = secure();
        key_without_cert.discovery_tls_key = Some(PathBuf::from("client.key"));
        let mut identity_without_ca = secure();
        identity_without_ca.discovery_tls_ca = None;
        identity_without_ca.discovery_allow_insecure = true;
        identity_without_ca.discovery_tls_cert = Some(PathBuf::from("client.pem"));
        identity_without_ca.discovery_tls_key = Some(PathBuf::from("client.key"));
        let mut domain_without_tls = secure();
        domain_without_tls.discovery_tls_ca = None;
        domain_without_tls.discovery_allow_insecure = true;
        domain_without_tls.discovery_tls_domain = Some("etcd.internal".to_string());
        let mut username_without_password = secure();
        username_without_password.discovery_username = Some("console".to_string());
        let mut password_without_username = secure();
        password_without_username.discovery_password_env = Some("ETCD_PASSWORD".to_string());
        let mut plaintext_credentials = secure();
        plaintext_credentials.discovery_tls_ca = None;
        plaintext_credentials.discovery_allow_insecure = true;
        plaintext_credentials.discovery_username = Some("console".to_string());
        plaintext_credentials.discovery_password_env = Some("ETCD_PASSWORD".to_string());
        let mut missing_password = secure();
        missing_password.discovery_username = Some("console".to_string());
        missing_password.discovery_password_env = Some("ETCD_PASSWORD".to_string());

        for (case, expected) in [
            (cert_without_key, "--discovery-tls-key"),
            (key_without_cert, "--discovery-tls-cert"),
            (identity_without_ca, "--discovery-tls-ca"),
            (domain_without_tls, "--discovery-tls-domain"),
            (username_without_password, "--discovery-password-env"),
            (password_without_username, "--discovery-username"),
            (plaintext_credentials, "TLS"),
            (missing_password, "ETCD_PASSWORD"),
        ] {
            let error = case
                .validate_with(|_| None)
                .expect_err("malformed discovery security must be rejected")
                .to_string();
            assert!(
                error.contains(expected),
                "error {error:?} does not mention {expected:?}"
            );
        }
    }
}
