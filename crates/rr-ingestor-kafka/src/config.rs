//! Configuration for [`crate::KafkaIngestor`] (and its mock).
//!
//! The config types are always compiled — only the rdkafka-backed
//! [`crate::KafkaIngestor`] is gated behind the `rdkafka` feature.
//! Tests and the mock backend use these same types so that switching
//! between mock and real backend requires zero call-site changes.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Connection + subscription parameters for a single Raft group's
/// upstream Kafka topic.
///
/// Per ADR A-0001 §"Topic layout convention", every Raft group `G`
/// consumes a dedicated topic. The topic name is derived from
/// [`Self::topic_pattern`] with `{group_id}` substituted at attach
/// time — e.g. `input_{group_id}` becomes `input_42` for group 42.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaIngestorConfig {
    /// Bootstrap broker list, e.g. `["broker1:9092", "broker2:9092"]`.
    pub bootstrap_servers: Vec<String>,
    /// Topic-name template; `{group_id}` is substituted at attach
    /// time. Default: `input_{group_id}`.
    pub topic_pattern: String,
    /// Single partition consumed per group. Defaults to 0 — ADR A-0001
    /// §"Plan A" mandates one partition per group, but the config is
    /// kept explicit so an operator can shift to a non-zero partition
    /// if their topic was provisioned differently.
    pub partition: i32,
    /// Maximum number of messages returned per `poll()` call. Maps to
    /// the framework's [`rr_core::ingestor::CommandIngestor`] batch
    /// size. Default: 100 (matches ADR A-0001 §"Open questions" #2).
    pub max_batch_size: usize,
    /// Wall-clock budget for a single `poll()` invocation; the
    /// underlying rdkafka stream is awaited up to this long before
    /// returning an empty batch. Default: 200 ms.
    pub poll_timeout: Duration,
    /// Optional security/auth configuration. `None` means
    /// `security.protocol = PLAINTEXT` (development only).
    pub security: Option<KafkaSecurityConfig>,
    /// Free-form rdkafka `client.id` set on the consumer. Default
    /// `rr-ingestor-{group_id}`.
    pub client_id_pattern: String,
    /// Auto-commit interval for the upstream Kafka consumer-group
    /// offsets. Note: rr's correctness does NOT depend on this — the
    /// authoritative offset lives in the framed snapshot. This setting
    /// only affects external observability (e.g. Burrow/CMAK lag).
    pub auto_commit_interval: Duration,
}

impl Default for KafkaIngestorConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: Vec::new(),
            topic_pattern: "input_{group_id}".to_string(),
            partition: 0,
            max_batch_size: 100,
            poll_timeout: Duration::from_millis(200),
            security: None,
            client_id_pattern: "rr-ingestor-{group_id}".to_string(),
            auto_commit_interval: Duration::from_secs(5),
        }
    }
}

impl KafkaIngestorConfig {
    /// Resolve the topic name for `group_id` by substituting
    /// `{group_id}` in [`Self::topic_pattern`].
    pub fn topic_for(&self, group_id: u64) -> String {
        self.topic_pattern
            .replace("{group_id}", &group_id.to_string())
    }

    /// Resolve the rdkafka `client.id` for `group_id`.
    pub fn client_id_for(&self, group_id: u64) -> String {
        self.client_id_pattern
            .replace("{group_id}", &group_id.to_string())
    }
}

/// Security configuration. Maps to rdkafka `security.protocol` /
/// `ssl.*` / `sasl.*` properties.
///
/// Sensitive values (passwords, keys) are pulled from environment
/// variables rather than embedded in config files — the `*_env`
/// fields name the variable to read at attach time. This matches the
/// project's `*_token_env` pattern from F2 security hardening.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaSecurityConfig {
    /// `security.protocol`: `PLAINTEXT` / `SSL` / `SASL_PLAINTEXT` /
    /// `SASL_SSL`.
    pub protocol: SecurityProtocol,
    /// Path to PEM-encoded CA bundle for verifying broker certs.
    /// `None` falls back to the system trust store.
    pub ssl_ca_path: Option<String>,
    /// Path to PEM-encoded client certificate (mTLS).
    pub ssl_cert_path: Option<String>,
    /// Path to PEM-encoded client private key (mTLS).
    pub ssl_key_path: Option<String>,
    /// Environment variable name holding the SSL key passphrase.
    pub ssl_key_password_env: Option<String>,
    /// SASL mechanism: e.g. `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.
    pub sasl_mechanism: Option<String>,
    /// SASL username (plaintext OK — non-secret).
    pub sasl_username: Option<String>,
    /// Environment variable name holding the SASL password.
    pub sasl_password_env: Option<String>,
}

/// Top-level Kafka security protocol, mapped 1:1 onto rdkafka.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    /// rdkafka property string for this protocol.
    pub fn as_rdkafka(&self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "PLAINTEXT",
            SecurityProtocol::Ssl => "SSL",
            SecurityProtocol::SaslPlaintext => "SASL_PLAINTEXT",
            SecurityProtocol::SaslSsl => "SASL_SSL",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_for_substitutes_group_id() {
        let cfg = KafkaIngestorConfig {
            topic_pattern: "input_{group_id}".to_string(),
            ..Default::default()
        };
        assert_eq!(cfg.topic_for(0), "input_0");
        assert_eq!(cfg.topic_for(42), "input_42");
    }

    #[test]
    fn topic_for_supports_custom_prefix() {
        let cfg = KafkaIngestorConfig {
            topic_pattern: "rr.prod.cmd.{group_id}".to_string(),
            ..Default::default()
        };
        assert_eq!(cfg.topic_for(7), "rr.prod.cmd.7");
    }

    #[test]
    fn client_id_for_substitutes_group_id() {
        let cfg = KafkaIngestorConfig::default();
        assert_eq!(cfg.client_id_for(3), "rr-ingestor-3");
    }

    #[test]
    fn security_protocol_round_trips_through_serde() {
        let cfg = SecurityProtocol::SaslSsl;
        let s = serde_json::to_string(&cfg).unwrap();
        assert_eq!(s, "\"SASL_SSL\"");
        let back: SecurityProtocol = serde_json::from_str(&s).unwrap();
        assert_eq!(back, cfg);
    }

    #[test]
    fn security_protocol_as_rdkafka_matches_known_strings() {
        assert_eq!(SecurityProtocol::Plaintext.as_rdkafka(), "PLAINTEXT");
        assert_eq!(SecurityProtocol::Ssl.as_rdkafka(), "SSL");
        assert_eq!(
            SecurityProtocol::SaslPlaintext.as_rdkafka(),
            "SASL_PLAINTEXT"
        );
        assert_eq!(SecurityProtocol::SaslSsl.as_rdkafka(), "SASL_SSL");
    }

    #[test]
    fn default_config_has_sensible_defaults() {
        let cfg = KafkaIngestorConfig::default();
        assert_eq!(cfg.topic_pattern, "input_{group_id}");
        assert_eq!(cfg.partition, 0);
        assert_eq!(cfg.max_batch_size, 100);
        assert_eq!(cfg.poll_timeout, Duration::from_millis(200));
        assert!(cfg.security.is_none());
    }
}
