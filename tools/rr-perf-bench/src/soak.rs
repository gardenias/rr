//! Soak scenario (R-8, `.sisyphus/plans/rr-reliability-program.md`):
//! fixed-rate keyed idempotent writes + periodic linearizable-read probes +
//! an fdatasync'd ack log + a post-load loss oracle.
//!
//! # The oracle
//!
//! Every acknowledged write lands in the ack log (`request_id`, value hash).
//! After the load window the oracle replays each sampled record:
//!
//! 1. re-issue a `Put` with a **different probe value** under the same
//!    idempotency key — the idempotency layer must serve the cached original
//!    response without touching the state machine (dedup survives restarts);
//! 2. `read_linearizable(Get)` the key — the value must be the ORIGINAL one
//!    (the probe must never have been applied).
//!
//! Together these prove: no acknowledged write was lost, and no replay was
//! double-applied — the exactly-once durability face, same strength as the
//! R-4 crash harness oracle (tests/crash/src/oracle.rs).
//!
//! # Schema lineage
//!
//! The ack log / metrics log formats mirror tests/crash's journal (JSONL,
//! fdatasync per record). crash-tests is a `publish = false` test crate and
//! not a dependency surface, so the ~60 lines of journal logic are
//! deliberately duplicated here rather than extracted.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use rr_client::RrClient;
use rr_core::idempotency::IdempotencyKey;

use crate::metrics::Metrics;
use crate::workload::SharedClient;

/// Partition the soak drives (the lab / bench clusters serve group 1).
const PARTITION_ID: u64 = 1;

/// Idempotency `request_type` namespace for recorded writes.
const REQUEST_TYPE: &str = "rr-perf-bench-soak";
/// `client_id` shared by writer and oracle (Replay path, not Collision).
const CLIENT_ID: &str = "rr-perf-bench";
/// Warmup probes use a separate namespace and never enter the ack log.
const WARMUP_REQUEST_TYPE: &str = "rr-perf-bench-soak-warmup";

/// Key written once at warmup and re-read by every linearizable probe.
const PROBE_KEY: &str = "soak:probe";

/// Key for write `request_id` (one KV key per write, so the oracle's `Get`
/// reads back exactly that write).
fn key_for(request_id: u64) -> String {
    format!("soak:{request_id:08}")
}

/// Deterministic original value for `request_id`.
fn value_for(request_id: u64, payload_size: usize) -> Vec<u8> {
    let head = format!("soak-value-{request_id:08}-");
    let mut v = head.into_bytes();
    v.resize(payload_size.max(v.len()), b'v');
    v
}

/// Probe value for `request_id` — deliberately different bytes; if a replayed
/// write was freshly applied (original lost), the `Get` returns THIS.
fn probe_value_for(request_id: u64, payload_size: usize) -> Vec<u8> {
    let head = format!("soak-probe-{request_id:08}-");
    let mut v = head.into_bytes();
    v.resize(payload_size.max(v.len()), b'p');
    v
}

/// FNV-1a 64-bit, hex — stable across processes (offline-inspectable).
fn value_hash(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}

// ── KV wire shapes (mirror of examples/kv-store/src/types.rs) ───────────────
//
// bincode variant ordinals are positional; keep the order EXACTLY as in
// kv-store (Put=0, Get=1, Delete=2, Increment=3).
//
// pub(crate): shared with the open-loop engine (openloop.rs, M2) so both
// scenarios single-source the wire format; the ordinal test below trips on
// drift for either consumer.

#[derive(Serialize, Deserialize)]
pub(crate) enum KvRequest {
    Put {
        key: String,
        value: Vec<u8>,
    },
    Get {
        key: String,
    },
    #[allow(dead_code)]
    Delete {
        key: String,
    },
    #[allow(dead_code)]
    Increment {
        key: String,
        delta: i64,
    },
}

#[derive(Deserialize)]
pub(crate) struct KvResponse {
    #[allow(dead_code)]
    pub(crate) value: Option<Vec<u8>>,
}

pub(crate) fn encode_req(req: &KvRequest) -> Vec<u8> {
    bincode::serde::encode_to_vec(req, bincode::config::standard()).expect("KvRequest encodes")
}

pub(crate) fn decode_resp(bytes: &[u8]) -> Result<KvResponse> {
    let (resp, _): (KvResponse, _) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .context("KvResponse decode failed")?;
    Ok(resp)
}

// ── Ack log / metrics log ────────────────────────────────────────────────────

/// One client-observed acknowledged write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckRecord {
    pub request_id: u64,
    pub key: String,
    pub value_hash: String,
    pub ts_ms: u64,
}

/// One periodic counters snapshot, appended to metrics.jsonl.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakMetricsRecord {
    pub ts_ms: u64,
    pub sent: u64,
    pub acked: u64,
    pub failed: u64,
    pub reads_ok: u64,
    pub reads_failed: u64,
    /// Instantaneous acked writes/s since the previous record.
    pub window_tps: f64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Append-only JSONL writer, fdatasync per record (the ack log is the
/// oracle's ground truth — same discipline as tests/crash's journal).
struct Journal {
    file: std::fs::File,
}

impl Journal {
    fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create journal dir {}", parent.display()))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("create journal {}", path.display()))?;
        Ok(Self { file })
    }

    fn append<T: Serialize>(&mut self, record: &T) -> Result<()> {
        use std::io::Write;
        let mut line = serde_json::to_vec(record).context("serialize journal record")?;
        line.push(b'\n');
        self.file.write_all(&line).context("journal append")?;
        self.file.sync_data().context("journal fdatasync")?;
        Ok(())
    }
}

/// Read every record of a JSONL journal.
pub fn read_ack_log(path: &Path) -> Result<Vec<AckRecord>> {
    use std::io::BufRead;
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("open {}", path.display())),
    };
    let mut out = Vec::new();
    for (lineno, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("read {}:{}", path.display(), lineno + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        out.push(
            serde_json::from_str(&line)
                .with_context(|| format!("parse {}:{}", path.display(), lineno + 1))?,
        );
    }
    Ok(out)
}

// ── Soak run ─────────────────────────────────────────────────────────────────

/// Tunables for one soak run.
pub struct SoakConfig {
    pub rate_per_sec: u32,
    pub duration: Duration,
    pub payload_size: usize,
    pub ack_log: PathBuf,
    pub metrics_log: Option<PathBuf>,
    pub read_probe_interval: Duration,
    pub metrics_interval: Duration,
    /// Max records the oracle replays (uniform-stride sample + head/tail).
    pub oracle_sample: usize,
    /// Skip the post-load oracle (e.g. when a harness runs it separately
    /// after cluster convergence).
    pub skip_oracle: bool,
}

/// Counters shared with the final report.
#[derive(Default)]
pub struct SoakStats {
    pub sent: AtomicU64,
    pub acked: AtomicU64,
    pub failed: AtomicU64,
    pub reads_ok: AtomicU64,
    pub reads_failed: AtomicU64,
}

/// Outcome of a soak run (load + optional oracle).
#[derive(Serialize)]
pub struct SoakSummary {
    pub duration_secs: u64,
    pub rate_per_sec: u32,
    pub sent: u64,
    pub acked: u64,
    pub failed: u64,
    pub reads_ok: u64,
    pub reads_failed: u64,
    pub ack_log_records: usize,
    pub oracle: Option<OracleSummary>,
}

#[derive(Serialize)]
pub struct OracleSummary {
    pub checked: usize,
    pub verified: usize,
    pub sampled_from: usize,
    pub elapsed_secs: f64,
}

/// Wait until the cluster accepts a keyed write (leader elected, dispatch
/// live), then write the probe key used by the read probes.
async fn warmup(client: &RrClient, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut probe_id = 0u64;
    loop {
        probe_id += 1;
        let key = IdempotencyKey::with_client_id(WARMUP_REQUEST_TYPE, probe_id, CLIENT_ID);
        let put = encode_req(&KvRequest::Put {
            key: PROBE_KEY.to_string(),
            value: b"soak-probe-value".to_vec(),
        });
        match client.send_request_idempotent(PARTITION_ID, put, key).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                if Instant::now() >= deadline {
                    anyhow::bail!("cluster not writable within {timeout:?}: {e}");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// The soak load loop: fixed-rate serial keyed writes, ack log on every Ok,
/// periodic read probes and metrics records.
pub async fn run_soak_load(client: SharedClient, cfg: &SoakConfig) -> Result<SoakSummary> {
    warmup(&client, Duration::from_secs(60)).await?;

    let stats = Arc::new(SoakStats::default());
    let mut ack_log = Journal::create(&cfg.ack_log)?;
    let mut metrics_log = match &cfg.metrics_log {
        Some(p) => Some(Journal::create(p)?),
        None => None,
    };

    let latencies = Metrics::new();
    let interval = Duration::from_secs_f64(1.0 / f64::from(cfg.rate_per_sec.max(1)));
    let started = Instant::now();
    let deadline = started + cfg.duration;
    let mut next_probe = started + cfg.read_probe_interval;
    let mut next_metrics = started + cfg.metrics_interval;
    let mut request_id = 0u64;
    let mut last_metrics_acked = 0u64;
    let mut last_metrics_at = started;

    while Instant::now() < deadline {
        let tick = Instant::now();
        request_id += 1;
        stats.sent.fetch_add(1, Ordering::Relaxed);

        let value = value_for(request_id, cfg.payload_size);
        let put = encode_req(&KvRequest::Put {
            key: key_for(request_id),
            value: value.clone(),
        });
        let key = IdempotencyKey::with_client_id(REQUEST_TYPE, request_id, CLIENT_ID);
        match client.send_request_idempotent(PARTITION_ID, put, key).await {
            Ok(resp) => {
                // The Put response shape is fixed (value: None); a decodable
                // response proves the apply path ran.
                match decode_resp(&resp) {
                    Ok(_) => {
                        stats.acked.fetch_add(1, Ordering::Relaxed);
                        latencies.record(tick.elapsed(), true);
                        ack_log
                            .append(&AckRecord {
                                request_id,
                                key: key_for(request_id),
                                value_hash: value_hash(&value),
                                ts_ms: now_ms(),
                            })
                            .context("ack log append failed — run is invalid")?;
                    }
                    Err(e) => {
                        stats.failed.fetch_add(1, Ordering::Relaxed);
                        latencies.record(tick.elapsed(), false);
                        tracing::warn!(request_id, error = %e, "soak write response undecodable");
                    }
                }
            }
            Err(e) => {
                // Expected during fault windows: not acked, nothing owed.
                stats.failed.fetch_add(1, Ordering::Relaxed);
                latencies.record(tick.elapsed(), false);
                static LOGGED: AtomicU64 = AtomicU64::new(0);
                if LOGGED.fetch_add(1, Ordering::Relaxed) < 5 {
                    tracing::warn!(request_id, error = %e, "soak write failed (sampled)");
                }
            }
        }

        let now = Instant::now();
        if now >= next_probe {
            next_probe = now + cfg.read_probe_interval;
            let get = encode_req(&KvRequest::Get {
                key: PROBE_KEY.to_string(),
            });
            match client.read_linearizable(PARTITION_ID, get).await {
                Ok(resp) => match decode_resp(&resp) {
                    Ok(r) if r.value.as_deref() == Some(b"soak-probe-value") => {
                        stats.reads_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(r) => {
                        stats.reads_failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            value_len = r.value.as_ref().map(|v| v.len()),
                            "soak probe read returned wrong value"
                        );
                    }
                    Err(e) => {
                        stats.reads_failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(error = %e, "soak probe read undecodable");
                    }
                },
                Err(e) => {
                    stats.reads_failed.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(error = %e, "soak probe read failed (fault window?)");
                }
            }
        }
        if now >= next_metrics {
            if let Some(log) = metrics_log.as_mut() {
                let acked = stats.acked.load(Ordering::Relaxed);
                let window =
                    (acked - last_metrics_acked) as f64 / (now - last_metrics_at).as_secs_f64();
                log.append(&SoakMetricsRecord {
                    ts_ms: now_ms(),
                    sent: stats.sent.load(Ordering::Relaxed),
                    acked,
                    failed: stats.failed.load(Ordering::Relaxed),
                    reads_ok: stats.reads_ok.load(Ordering::Relaxed),
                    reads_failed: stats.reads_failed.load(Ordering::Relaxed),
                    window_tps: window,
                })?;
                last_metrics_acked = acked;
                last_metrics_at = now;
            }
            next_metrics = now + cfg.metrics_interval;
        }

        let elapsed = tick.elapsed();
        if elapsed < interval {
            tokio::time::sleep(interval - elapsed).await;
        }
    }

    let records = read_ack_log(&cfg.ack_log)?.len();
    Ok(SoakSummary {
        duration_secs: started.elapsed().as_secs(),
        rate_per_sec: cfg.rate_per_sec,
        sent: stats.sent.load(Ordering::Relaxed),
        acked: stats.acked.load(Ordering::Relaxed),
        failed: stats.failed.load(Ordering::Relaxed),
        reads_ok: stats.reads_ok.load(Ordering::Relaxed),
        reads_failed: stats.reads_failed.load(Ordering::Relaxed),
        ack_log_records: records,
        oracle: None,
    })
}

// ── Oracle ───────────────────────────────────────────────────────────────────

/// Pick the records to replay: all when small; otherwise a uniform-stride
/// sample of `max` records that ALWAYS includes the first and last 100
/// (edge losses cluster at the run's ends). Loud about sampling.
fn sample_records(records: &[AckRecord], max: usize) -> Vec<AckRecord> {
    if records.len() <= max {
        return records.to_vec();
    }
    let edge = 100.min(records.len());
    let mut picked: Vec<AckRecord> = records[..edge].to_vec();
    let stride = records.len() / (max.saturating_sub(2 * edge).max(1)).max(1);
    let mut i = edge;
    while i < records.len().saturating_sub(edge) && picked.len() < max - edge {
        picked.push(records[i].clone());
        i += stride.max(1);
    }
    picked.extend_from_slice(&records[records.len() - edge..]);
    eprintln!(
        "[soak-oracle] WARNING: ack log has {} records > {} max; replaying a {}-record \
         sample (first/last {} + stride {})",
        records.len(),
        max,
        picked.len(),
        edge,
        stride
    );
    picked
}

/// Replay the ack log: every record must survive (probe Put deduped via the
/// idempotency layer, `Get` returns the ORIGINAL value).
pub async fn run_oracle(client: SharedClient, cfg: &SoakConfig) -> Result<OracleSummary> {
    let records = read_ack_log(&cfg.ack_log)?;
    let sample = sample_records(&records, cfg.oracle_sample);
    let started = Instant::now();
    let mut verified = 0usize;

    for (i, record) in sample.iter().enumerate() {
        verify_one(&client, record, cfg.payload_size)
            .await
            .with_context(|| {
                format!(
                    "ack log record request_id={} (sample index {i}/{})",
                    record.request_id,
                    sample.len()
                )
            })?;
        verified += 1;
        if verified.is_multiple_of(500) {
            eprintln!(
                "[soak-oracle] {}/{} verified ({:?})",
                verified,
                sample.len(),
                started.elapsed()
            );
        }
    }

    Ok(OracleSummary {
        checked: sample.len(),
        verified,
        sampled_from: records.len(),
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

async fn verify_one(client: &RrClient, record: &AckRecord, payload_size: usize) -> Result<()> {
    let original = value_for(record.request_id, payload_size);
    let expected_hash = value_hash(&original);
    anyhow::ensure!(
        record.value_hash == expected_hash,
        "ack log hash mismatch: recorded {} vs regenerated {expected_hash} — \
         ack log corrupted or value format drifted",
        record.value_hash
    );

    // 1. Probe Put under the same idempotency key: the idempotency layer must
    //    answer from its cache (the state machine never sees the probe).
    let probe = encode_req(&KvRequest::Put {
        key: record.key.clone(),
        value: probe_value_for(record.request_id, payload_size),
    });
    let key = IdempotencyKey::with_client_id(REQUEST_TYPE, record.request_id, CLIENT_ID);
    client
        .send_request_idempotent(PARTITION_ID, probe, key)
        .await
        .context("probe Put failed after client retries")?;

    // 2. Linearizable read: the value must be the ORIGINAL — any other value
    //    means the original acked write was lost (or the probe leaked through).
    let get = encode_req(&KvRequest::Get {
        key: record.key.clone(),
    });
    let resp = client
        .read_linearizable(PARTITION_ID, get)
        .await
        .context("oracle Get failed after client retries")?;
    let got = decode_resp(&resp)?.value;
    anyhow::ensure!(
        got.as_deref() == Some(original.as_slice()),
        "INV-1 VIOLATION: acknowledged write {} does not read back its original value \
         (got {} bytes{:?}) — the entry did not survive, or the probe was freshly applied",
        record.request_id,
        got.as_ref().map(|v| v.len()).unwrap_or(0),
        got.as_ref().map(|v| &v[..v.len().min(16)])
    );
    Ok(())
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_and_probe_are_deterministic_and_distinct() {
        assert_eq!(value_for(7, 64), value_for(7, 64));
        assert_eq!(probe_value_for(7, 64), probe_value_for(7, 64));
        assert_ne!(value_for(7, 64), probe_value_for(7, 64));
        assert!(value_for(7, 64).len() >= 64);
    }

    #[test]
    fn hash_is_stable() {
        assert_eq!(value_hash(b""), "cbf29ce484222325");
        assert_eq!(value_hash(b"a"), "af63dc4c8601ec8c");
    }

    #[test]
    fn kv_wire_shapes_match_kv_store_ordinals() {
        // Put must encode variant ordinal 0, Get 1 (positional bincode).
        let put = encode_req(&KvRequest::Put {
            key: "k".into(),
            value: b"v".to_vec(),
        });
        assert_eq!(put[0], 0, "Put variant ordinal");
        let get = encode_req(&KvRequest::Get { key: "k".into() });
        assert_eq!(get[0], 1, "Get variant ordinal");
    }

    #[test]
    fn journal_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("ack.jsonl");
        {
            let mut j = Journal::create(&path).unwrap();
            for i in 1..=3u64 {
                j.append(&AckRecord {
                    request_id: i,
                    key: key_for(i),
                    value_hash: value_hash(&value_for(i, 32)),
                    ts_ms: now_ms(),
                })
                .unwrap();
            }
        }
        let records = read_ack_log(&path).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[2].request_id, 3);
    }

    #[test]
    fn sample_keeps_edges_and_covers_middle() {
        let records: Vec<AckRecord> = (1..=10_000u64)
            .map(|i| AckRecord {
                request_id: i,
                key: key_for(i),
                value_hash: String::new(),
                ts_ms: 0,
            })
            .collect();
        let sample = sample_records(&records, 1000);
        assert!(sample.len() <= 1000);
        assert_eq!(sample.first().unwrap().request_id, 1);
        assert_eq!(sample.last().unwrap().request_id, 10_000);
        // Middle coverage: some record from the middle third is present.
        assert!(sample
            .iter()
            .any(|r| (3_334..=6_666).contains(&r.request_id)));
        // Small logs are replayed in full.
        let small: Vec<AckRecord> = records[..500].to_vec();
        assert_eq!(sample_records(&small, 1000).len(), 500);
    }
}
