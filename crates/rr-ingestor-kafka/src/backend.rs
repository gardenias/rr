//! Real Kafka backend wrapping rdkafka's `StreamConsumer`.
//!
//! Compiled only when the `rdkafka` feature is enabled. Implements
//! [`rr_core::ingestor::CommandIngestor`] over a single
//! `(topic, partition)` assignment so leader-exclusive semantics are
//! enforced at the assignment level — no consumer-group rebalances
//! can move us off the partition while we're leader.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset as KafkaOffset, TopicPartitionList};
use thiserror::Error;
use tracing::{debug, info, warn};

use rr_core::error::RrError;
use rr_core::ingestor::{CommandIngestor, IngressCommand};

use crate::config::{KafkaIngestorConfig, KafkaSecurityConfig, SecurityProtocol};

/// Crate-specific error type. Wrapped into [`RrError::Application`]
/// at the trait boundary so it composes with the framework's
/// existing error chain.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum KafkaIngestorError {
    /// rdkafka returned an error from `subscribe` / `assign` /
    /// `seek_partitions` / `recv` etc.
    #[error("rdkafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    /// Invalid configuration detected at `new()` (e.g. empty
    /// bootstrap_servers).
    #[error("invalid kafka ingestor config: {0}")]
    Config(String),
    /// `*_env` environment variable referenced by the security config
    /// was unset or empty.
    #[error("kafka security env var {var} unset or empty")]
    MissingSecret { var: String },
}

impl From<KafkaIngestorError> for RrError {
    fn from(e: KafkaIngestorError) -> Self {
        match &e {
            KafkaIngestorError::Config(_) | KafkaIngestorError::MissingSecret { .. } => {
                RrError::Config(rr_core::error::ConfigError::InvalidConfig(e.to_string()))
            }
            KafkaIngestorError::Kafka(_) => RrError::Application(format!("kafka: {e}")),
        }
    }
}

/// Real librdkafka-backed ingestor for a single Raft group.
///
/// Bound to one `(topic, partition)`; the topic is derived from
/// [`KafkaIngestorConfig::topic_pattern`] at construction.
/// Leader-exclusive consumption is enforced by the framework
/// supervisor — this struct is itself agnostic of leadership.
pub struct KafkaIngestor {
    consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    topic: String,
    partition: i32,
    poll_timeout: Duration,
    group_id: u64,
}

/// Default timeout for `Consumer::fetch_watermarks`. Watermark fetch
/// hits the broker; 5s is generous on a healthy network and keeps
/// the seek path bounded under broker latency spikes.
const FETCH_WATERMARKS_TIMEOUT: Duration = Duration::from_secs(5);

impl KafkaIngestor {
    /// Validate that `target_offset` is inside the partition's
    /// `[low, high]` watermark window before letting `assign` accept
    /// it. Three cases:
    ///
    /// - Below `low`: data was retention-trimmed; rdkafka's
    ///   `auto.offset.reset = "earliest"` would silently jump to
    ///   `low`, **skipping** the gap. We log a warn + bump
    ///   `rr_ingestor_seek_below_watermark_total{group_id}` so
    ///   operators see the data loss; we don't return an error
    ///   because the rr supervisor's only recovery is to restart
    ///   from `low`, which is what auto.offset.reset already does.
    /// - Above `high`: seeking past the end of the topic. rdkafka
    ///   will block forever waiting for new messages — this is
    ///   acceptable for tailing but suggests a stale persisted
    ///   offset, so we emit a warn + metric.
    /// - Otherwise: clean.
    ///
    /// Watermark-fetch errors are logged but non-fatal: if the
    /// broker is reachable enough to serve the next `poll`, the
    /// validation simply degrades to a no-op for this seek.
    async fn validate_seek_target(&self, target_offset: i64) {
        let topic = self.topic.clone();
        let partition = self.partition;
        let consumer = Arc::clone(&self.consumer);
        let group_id_label = self.group_id.to_string();

        let watermarks = tokio::task::spawn_blocking(move || {
            consumer.fetch_watermarks(&topic, partition, FETCH_WATERMARKS_TIMEOUT)
        })
        .await;
        let (low, high) = match watermarks {
            Ok(Ok(wm)) => wm,
            Ok(Err(e)) => {
                debug!(
                    group_id = self.group_id,
                    topic = %self.topic,
                    error = %e,
                    "fetch_watermarks failed; skipping seek-range validation"
                );
                return;
            }
            Err(e) => {
                debug!(
                    group_id = self.group_id,
                    topic = %self.topic,
                    error = %e,
                    "fetch_watermarks task join failed; skipping seek-range validation"
                );
                return;
            }
        };

        if target_offset < low {
            warn!(
                group_id = self.group_id,
                topic = %self.topic,
                partition = self.partition,
                target = target_offset,
                low,
                high,
                "kafka seek target below low watermark — auto.offset.reset will jump to {low}, \
                 skipping {} entries; investigate retention vs rr's persisted offset",
                low - target_offset
            );
            metrics::counter!(
                "rr_ingestor_seek_below_watermark_total",
                "group_id" => group_id_label.clone()
            )
            .increment(1);
        } else if target_offset > high {
            warn!(
                group_id = self.group_id,
                topic = %self.topic,
                partition = self.partition,
                target = target_offset,
                low,
                high,
                "kafka seek target above high watermark — poll will block until new messages \
                 arrive; check that the persisted offset matches the topic"
            );
            metrics::counter!(
                "rr_ingestor_seek_above_watermark_total",
                "group_id" => group_id_label
            )
            .increment(1);
        }
    }

    /// Construct a `KafkaIngestor` bound to `group_id`'s topic.
    ///
    /// Resolves env-var-backed security settings, builds a
    /// `StreamConsumer`, and `assign()`s it to the configured
    /// `(topic, partition)`. Does NOT seek — the framework supervisor
    /// calls `seek()` first thing on leader promotion.
    pub async fn new(group_id: u64, cfg: KafkaIngestorConfig) -> Result<Self, KafkaIngestorError> {
        if cfg.bootstrap_servers.is_empty() {
            return Err(KafkaIngestorError::Config(
                "bootstrap_servers must be non-empty".to_string(),
            ));
        }

        let topic = cfg.topic_for(group_id);
        let client_id = cfg.client_id_for(group_id);

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", cfg.bootstrap_servers.join(","))
            .set("client.id", &client_id)
            .set("group.id", &client_id)
            .set("enable.auto.commit", "true")
            .set(
                "auto.commit.interval.ms",
                cfg.auto_commit_interval.as_millis().to_string(),
            )
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "30000")
            .set("auto.offset.reset", "earliest")
            .set_log_level(RDKafkaLogLevel::Warning);

        if let Some(sec) = &cfg.security {
            apply_security(&mut client_config, sec)?;
        } else {
            client_config.set("security.protocol", "PLAINTEXT");
        }

        let consumer: StreamConsumer<DefaultConsumerContext> =
            client_config.create().map_err(KafkaIngestorError::Kafka)?;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(&topic, cfg.partition);
        consumer.assign(&tpl).map_err(KafkaIngestorError::Kafka)?;

        info!(
            group_id,
            topic = %topic,
            partition = cfg.partition,
            client_id = %client_id,
            "kafka ingestor constructed"
        );

        Ok(Self {
            consumer: Arc::new(consumer),
            topic,
            partition: cfg.partition,
            poll_timeout: cfg.poll_timeout,
            group_id,
        })
    }
}

fn apply_security(
    cfg: &mut ClientConfig,
    sec: &KafkaSecurityConfig,
) -> Result<(), KafkaIngestorError> {
    cfg.set("security.protocol", sec.protocol.as_rdkafka());

    if matches!(
        sec.protocol,
        SecurityProtocol::Ssl | SecurityProtocol::SaslSsl
    ) {
        if let Some(p) = &sec.ssl_ca_path {
            cfg.set("ssl.ca.location", p);
        }
        if let Some(p) = &sec.ssl_cert_path {
            cfg.set("ssl.certificate.location", p);
        }
        if let Some(p) = &sec.ssl_key_path {
            cfg.set("ssl.key.location", p);
        }
        if let Some(var) = &sec.ssl_key_password_env {
            let pw = read_secret_env(var)?;
            cfg.set("ssl.key.password", &pw);
        }
    }

    if matches!(
        sec.protocol,
        SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
    ) {
        if let Some(m) = &sec.sasl_mechanism {
            cfg.set("sasl.mechanism", m);
        }
        if let Some(u) = &sec.sasl_username {
            cfg.set("sasl.username", u);
        }
        if let Some(var) = &sec.sasl_password_env {
            let pw = read_secret_env(var)?;
            cfg.set("sasl.password", &pw);
        }
    }

    Ok(())
}

fn read_secret_env(var: &str) -> Result<String, KafkaIngestorError> {
    match std::env::var(var) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(KafkaIngestorError::MissingSecret {
            var: var.to_string(),
        }),
    }
}

#[async_trait]
impl CommandIngestor for KafkaIngestor {
    type Offset = i64;

    async fn seek(&mut self, offset: Option<i64>) -> Result<(), RrError> {
        let target = match offset {
            Some(o) => KafkaOffset::Offset(o + 1),
            None => KafkaOffset::Beginning,
        };
        debug!(
            group_id = self.group_id,
            topic = %self.topic,
            partition = self.partition,
            offset = ?target,
            "kafka ingestor seek"
        );
        if let Some(target_offset) = offset.map(|o| o + 1) {
            self.validate_seek_target(target_offset).await;
        }
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.topic, self.partition, target)
            .map_err(KafkaIngestorError::Kafka)?;
        self.consumer
            .assign(&tpl)
            .map_err(KafkaIngestorError::Kafka)?;
        Ok(())
    }

    async fn poll(&mut self, max_batch_size: usize) -> Result<Vec<IngressCommand<i64>>, RrError> {
        let mut batch = Vec::with_capacity(max_batch_size.min(64));
        let mut stream = self.consumer.stream();
        let deadline = tokio::time::sleep(self.poll_timeout);
        tokio::pin!(deadline);

        while batch.len() < max_batch_size {
            tokio::select! {
                _ = &mut deadline => break,
                msg = stream.next() => {
                    match msg {
                        Some(Ok(borrowed)) => {
                            let payload = borrowed.payload().map(|p| p.to_vec()).unwrap_or_default();
                            batch.push(IngressCommand {
                                offset: borrowed.offset(),
                                payload,
                            });
                        }
                        Some(Err(e)) => {
                            warn!(
                                group_id = self.group_id,
                                error = %e,
                                "kafka ingestor poll error"
                            );
                            return Err(KafkaIngestorError::Kafka(e).into());
                        }
                        None => break,
                    }
                }
            }
        }
        Ok(batch)
    }

    async fn commit_applied(&mut self, offset: i64) -> Result<(), RrError> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.topic, self.partition, KafkaOffset::Offset(offset + 1))
            .map_err(KafkaIngestorError::Kafka)?;
        self.consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(KafkaIngestorError::Kafka)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KafkaIngestorConfig;

    #[tokio::test]
    async fn new_rejects_empty_bootstrap_servers() {
        let cfg = KafkaIngestorConfig::default();
        let err = KafkaIngestor::new(1, cfg).await.err().unwrap();
        assert!(matches!(err, KafkaIngestorError::Config(_)));
    }

    fn set_env(key: &str, val: &str) {
        unsafe { std::env::set_var(key, val) }
    }

    fn unset_env(key: &str) {
        unsafe { std::env::remove_var(key) }
    }

    #[test]
    fn read_secret_env_rejects_unset() {
        unset_env("RR_TEST_KAFKA_SECRET");
        let err = read_secret_env("RR_TEST_KAFKA_SECRET").unwrap_err();
        assert!(matches!(err, KafkaIngestorError::MissingSecret { .. }));
    }

    #[test]
    fn read_secret_env_rejects_empty() {
        set_env("RR_TEST_KAFKA_SECRET_EMPTY", "");
        let err = read_secret_env("RR_TEST_KAFKA_SECRET_EMPTY").unwrap_err();
        assert!(matches!(err, KafkaIngestorError::MissingSecret { .. }));
        unset_env("RR_TEST_KAFKA_SECRET_EMPTY");
    }

    #[test]
    fn read_secret_env_returns_value() {
        set_env("RR_TEST_KAFKA_SECRET_OK", "topsecret");
        let v = read_secret_env("RR_TEST_KAFKA_SECRET_OK").unwrap();
        assert_eq!(v, "topsecret");
        unset_env("RR_TEST_KAFKA_SECRET_OK");
    }

    #[test]
    fn apply_security_sets_protocol_and_sasl() {
        set_env("RR_TEST_KAFKA_PASSWORD", "p4ss");
        let mut cfg = ClientConfig::new();
        let sec = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            ssl_ca_path: Some("/etc/ssl/ca.pem".into()),
            ssl_cert_path: None,
            ssl_key_path: None,
            ssl_key_password_env: None,
            sasl_mechanism: Some("SCRAM-SHA-256".into()),
            sasl_username: Some("rr".into()),
            sasl_password_env: Some("RR_TEST_KAFKA_PASSWORD".into()),
        };
        apply_security(&mut cfg, &sec).unwrap();
        assert_eq!(cfg.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(cfg.get("ssl.ca.location"), Some("/etc/ssl/ca.pem"));
        assert_eq!(cfg.get("sasl.mechanism"), Some("SCRAM-SHA-256"));
        assert_eq!(cfg.get("sasl.username"), Some("rr"));
        assert_eq!(cfg.get("sasl.password"), Some("p4ss"));
        unset_env("RR_TEST_KAFKA_PASSWORD");
    }
}
