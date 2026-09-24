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
    /// The requested seek offset is above the partition's high
    /// watermark: the persisted offset belongs to a different/recreated
    /// topic. Fail-closed — with `auto.offset.reset = "earliest"` an
    /// out-of-range seek would silently replay the WHOLE topic.
    #[error(
        "kafka seek target {target} above high watermark {high} (low {low}) for \
         {topic}[{partition}]; refusing to seek — check that the persisted offset matches the topic"
    )]
    SeekAboveWatermark {
        topic: String,
        partition: i32,
        target: i64,
        low: i64,
        high: i64,
    },
    /// `offset + 1` overflowed `i64` in Kafka's next-offset arithmetic.
    /// Fail-closed instead of wrapping to a negative sentinel.
    #[error("kafka offset {0} has no successor (i64 overflow)")]
    OffsetOverflow(i64),
}

impl From<KafkaIngestorError> for RrError {
    fn from(e: KafkaIngestorError) -> Self {
        match &e {
            KafkaIngestorError::Config(_) | KafkaIngestorError::MissingSecret { .. } => {
                RrError::Config(rr_core::error::ConfigError::InvalidConfig(e.to_string()))
            }
            KafkaIngestorError::Kafka(_)
            | KafkaIngestorError::SeekAboveWatermark { .. }
            | KafkaIngestorError::OffsetOverflow(_) => RrError::Application(format!("kafka: {e}")),
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
    /// - Above `high`: the persisted offset points past the end of the
    ///   topic — it belongs to a different or recreated topic. With
    ///   `auto.offset.reset = "earliest"` the out-of-range seek would
    ///   NOT block: rdkafka resets to the earliest offset and replays
    ///   the WHOLE topic. That is fail-closed territory, so this is a
    ///   hard [`KafkaIngestorError::SeekAboveWatermark`] (plus the
    ///   `rr_ingestor_seek_above_watermark_total{group_id}` metric).
    /// - Otherwise: clean.
    ///
    /// Watermark-fetch errors are logged but non-fatal: if the
    /// broker is reachable enough to serve the next `poll`, the
    /// validation simply degrades to a no-op for this seek.
    async fn validate_seek_target(&self, target_offset: i64) -> Result<(), KafkaIngestorError> {
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
                return Ok(());
            }
            Err(e) => {
                debug!(
                    group_id = self.group_id,
                    topic = %self.topic,
                    error = %e,
                    "fetch_watermarks task join failed; skipping seek-range validation"
                );
                return Ok(());
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
            return Ok(());
        }
        if target_offset > high {
            metrics::counter!(
                "rr_ingestor_seek_above_watermark_total",
                "group_id" => group_id_label
            )
            .increment(1);
            return Err(KafkaIngestorError::SeekAboveWatermark {
                topic: self.topic.clone(),
                partition: self.partition,
                target: target_offset,
                low,
                high,
            });
        }
        Ok(())
    }

    /// Construct a `KafkaIngestor` bound to `group_id`'s topic.
    ///
    /// Resolves env-var-backed security settings, builds a
    /// `StreamConsumer`, and `assign()`s it to the configured
    /// `(topic, partition)`. Does NOT seek — the framework supervisor
    /// calls `seek()` first thing on leader promotion.
    pub async fn new(group_id: u64, cfg: KafkaIngestorConfig) -> Result<Self, KafkaIngestorError> {
        validate_config(&cfg)?;

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

/// Fail-fast config validation, before any broker round-trip. Catching
/// these at attach time beats a consumer that silently misbehaves
/// (zero-sized polls, negative partition, half-configured SASL).
fn validate_config(cfg: &KafkaIngestorConfig) -> Result<(), KafkaIngestorError> {
    let invalid = |msg: &str| KafkaIngestorError::Config(msg.to_string());

    if cfg.bootstrap_servers.is_empty() {
        return Err(invalid("bootstrap_servers must be non-empty"));
    }
    if cfg.max_batch_size == 0 {
        return Err(invalid("max_batch_size must be > 0"));
    }
    if cfg.poll_timeout.is_zero() {
        return Err(invalid("poll_timeout must be > 0"));
    }
    if cfg.partition < 0 {
        return Err(invalid("partition must be >= 0"));
    }
    if let Some(sec) = &cfg.security
        && matches!(
            sec.protocol,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        )
    {
        if sec.sasl_mechanism.is_none() {
            return Err(invalid(
                "sasl_mechanism must be set when using a SASL security protocol",
            ));
        }
        match (&sec.sasl_username, &sec.sasl_password_env) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(invalid(
                    "sasl_username and sasl_password_env must be set together",
                ));
            }
            _ => {}
        }
    }
    Ok(())
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
        // Kafka seeks/commits address the NEXT offset to consume.
        // checked_add: i64::MAX would wrap to a negative sentinel in
        // release (panic in debug) — fail closed instead.
        let target_offset = offset
            .map(|o| {
                o.checked_add(1)
                    .ok_or(KafkaIngestorError::OffsetOverflow(o))
            })
            .transpose()?;
        let target = match target_offset {
            Some(o) => KafkaOffset::Offset(o),
            None => KafkaOffset::Beginning,
        };
        debug!(
            group_id = self.group_id,
            topic = %self.topic,
            partition = self.partition,
            offset = ?target,
            "kafka ingestor seek"
        );
        if let Some(target_offset) = target_offset {
            self.validate_seek_target(target_offset).await?;
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
                                batch_len = batch.len(),
                                "kafka ingestor poll error"
                            );
                            // Messages already in `batch` advanced the
                            // consumer position — returning Err would
                            // discard them, and the supervisor retries
                            // WITHOUT re-seek, skipping those commands for
                            // the rest of the leader tenure. Flush the
                            // partial batch instead; the error resurfaces
                            // on the next poll and still triggers backoff.
                            if !batch.is_empty() {
                                return Ok(batch);
                            }
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
        let committed = offset
            .checked_add(1)
            .ok_or(KafkaIngestorError::OffsetOverflow(offset))?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.topic, self.partition, KafkaOffset::Offset(committed))
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

    fn valid_cfg() -> KafkaIngestorConfig {
        KafkaIngestorConfig {
            bootstrap_servers: vec!["localhost:9092".into()],
            ..Default::default()
        }
    }

    #[test]
    fn validate_config_accepts_valid() {
        validate_config(&valid_cfg()).unwrap();
    }

    #[test]
    fn validate_config_rejects_zero_max_batch_size() {
        let cfg = KafkaIngestorConfig {
            max_batch_size: 0,
            ..valid_cfg()
        };
        let err = validate_config(&cfg).unwrap_err();
        assert!(matches!(err, KafkaIngestorError::Config(_)));
        assert!(err.to_string().contains("max_batch_size"));
    }

    #[test]
    fn validate_config_rejects_zero_poll_timeout() {
        let cfg = KafkaIngestorConfig {
            poll_timeout: Duration::ZERO,
            ..valid_cfg()
        };
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("poll_timeout"));
    }

    #[test]
    fn validate_config_rejects_negative_partition() {
        let cfg = KafkaIngestorConfig {
            partition: -1,
            ..valid_cfg()
        };
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("partition"));
    }

    fn sasl_security() -> KafkaSecurityConfig {
        KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            ssl_ca_path: None,
            ssl_cert_path: None,
            ssl_key_path: None,
            ssl_key_password_env: None,
            sasl_mechanism: Some("SCRAM-SHA-256".into()),
            sasl_username: Some("rr".into()),
            sasl_password_env: Some("RR_TEST_KAFKA_PASSWORD".into()),
        }
    }

    #[test]
    fn validate_config_rejects_sasl_without_mechanism() {
        let cfg = KafkaIngestorConfig {
            security: Some(KafkaSecurityConfig {
                sasl_mechanism: None,
                ..sasl_security()
            }),
            ..valid_cfg()
        };
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("sasl_mechanism"));
    }

    #[test]
    fn validate_config_rejects_unpaired_sasl_credentials() {
        for (username, password_env) in [
            (Some("rr".to_string()), None),
            (None, Some("RR_TEST_KAFKA_PASSWORD".to_string())),
        ] {
            let cfg = KafkaIngestorConfig {
                security: Some(KafkaSecurityConfig {
                    sasl_username: username,
                    sasl_password_env: password_env,
                    ..sasl_security()
                }),
                ..valid_cfg()
            };
            let err = validate_config(&cfg).unwrap_err();
            assert!(
                err.to_string().contains("sasl_username"),
                "expected paired-credential error, got {err}"
            );
        }
    }

    #[test]
    fn validate_config_accepts_complete_sasl() {
        let cfg = KafkaIngestorConfig {
            security: Some(sasl_security()),
            ..valid_cfg()
        };
        validate_config(&cfg).unwrap();
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
