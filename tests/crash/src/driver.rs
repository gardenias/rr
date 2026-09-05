//! Client driver: a background task issuing keyed idempotent writes at a
//! fixed rate. Every `Ok` response is appended to the ack log (fdatasync'd
//! by [`crate::journal::Journal`]) — the ack log is the oracle's ground
//! truth, with the ACK boundary defined by what the *client* observed.
//!
//! The driver is deliberately serial (one write in flight at a time): at the
//! rates used here (default 50/s) per-request latency is a few milliseconds,
//! so a serial loop keeps ordering trivially consistent with request ids and
//! never buries a failover window under a reconnect storm. "Heavy write load"
//! during kills comes from the driver never pausing across kill rounds, not
//! from concurrency.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rr_client::{RetryConfig, RrClient};
use rr_core::idempotency::IdempotencyKey;

use crate::cluster::GROUP_ID;
use crate::journal::{now_ms, AckRecord, Journal};
use crate::{payload_for, payload_hash, CLIENT_ID, REQUEST_TYPE};

/// Payload the harness sends through the local-read path to ask a node how
/// many unique keyed writes its state machine has applied. Handled by the
/// node's counting service; never used as a write payload.
pub const COUNT_READ_QUERY: &[u8] = b"count?";

/// Request type for writability probes during cluster bootstrap. Kept out of
/// the driver's namespace so probes never land in the ack log.
const WARMUP_REQUEST_TYPE: &str = "crash-warmup";

/// Connect with retry settings sized for failover windows: with 100ms base
/// backoff the attempt ladder 0.1+0.2+…+12.8s gives a killed leader ~25s to
/// be replaced before a write surfaces an error (which then simply is not
/// acked — never a correctness event).
pub async fn connect(addrs: &[String]) -> Result<RrClient> {
    RrClient::connect_with_config(
        addrs.to_vec(),
        RetryConfig {
            max_retries: 8,
            retry_delay_ms: 100,
        },
    )
    .await
    .context("connect driver client")
}

/// Block until the cluster accepts a keyed write (leader elected, dispatch
/// path live). Uses a fast-failing probe client so the loop stays responsive.
pub async fn wait_writable(client: &RrClient, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut probe_id = 0u64;
    while Instant::now() < deadline {
        probe_id += 1;
        let key = IdempotencyKey::with_client_id(WARMUP_REQUEST_TYPE, probe_id, CLIENT_ID);
        match client
            .send_request_idempotent(GROUP_ID, b"warmup".to_vec(), key)
            .await
        {
            Ok(_) => return Ok(()),
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    anyhow::bail!("cluster not writable within {timeout:?}")
}

/// Live counters for the driver task; read by the test body for progress
/// logging and sanity assertions.
#[derive(Default)]
pub struct DriverStats {
    pub sent: AtomicU64,
    pub acked: AtomicU64,
    /// Writes that exhausted retries (expected during kill windows).
    pub failed: AtomicU64,
    /// Responses whose payload did not echo the request (never expected —
    /// a non-empty sample list fails the scenario).
    pub corrupted: AtomicU64,
}

/// A running driver. Stop with [`Driver::stop`]; the task also stops on drop.
pub struct Driver {
    stats: Arc<DriverStats>,
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
    /// First few error samples for post-mortem logging.
    errors: Arc<Mutex<Vec<String>>>,
}

impl Driver {
    /// Start writing at `rate_per_sec`, appending acks to `ack_log`.
    pub fn start(client: RrClient, rate_per_sec: u32, ack_log: Journal) -> Self {
        let stats = Arc::new(DriverStats::default());
        let stop = Arc::new(AtomicBool::new(false));
        let errors = Arc::new(Mutex::new(Vec::new()));

        let task = {
            let stats = Arc::clone(&stats);
            let stop = Arc::clone(&stop);
            let errors = Arc::clone(&errors);
            let mut ack_log = ack_log;
            tokio::spawn(async move {
                let interval = Duration::from_secs_f64(1.0 / f64::from(rate_per_sec.max(1)));
                let mut next_id = 1u64;
                while !stop.load(Ordering::Relaxed) {
                    let tick = Instant::now();
                    let request_id = next_id;
                    next_id += 1;
                    stats.sent.fetch_add(1, Ordering::Relaxed);

                    let payload = payload_for(request_id);
                    let key = IdempotencyKey::with_client_id(REQUEST_TYPE, request_id, CLIENT_ID);
                    match client
                        .send_request_idempotent(GROUP_ID, payload.clone(), key)
                        .await
                    {
                        Ok(resp) if resp == payload => {
                            stats.acked.fetch_add(1, Ordering::Relaxed);
                            let record = AckRecord {
                                request_id,
                                key: REQUEST_TYPE.to_string(),
                                payload_hash: payload_hash(&payload),
                                ts_ms: now_ms(),
                            };
                            if let Err(e) = ack_log.append(&record) {
                                // The ack log is the oracle — if we cannot
                                // persist it, the run is invalid. Stop loudly.
                                record_error(&errors, format!("ack log append failed: {e:#}"));
                                stats.failed.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                        Ok(resp) => {
                            // Server answered Ok but with bytes != what we sent:
                            // the echo contract broke (or worse, a stale replay
                            // served another key's response).
                            stats.corrupted.fetch_add(1, Ordering::Relaxed);
                            record_error(
                                &errors,
                                format!(
                                    "request {request_id}: response payload mismatch \
                                     ({} bytes, first 16: {:02x?})",
                                    resp.len(),
                                    &resp[..resp.len().min(16)]
                                ),
                            );
                        }
                        Err(e) => {
                            // Expected during kill/election windows: the write
                            // was not acked, so nothing is owed. Sampled for
                            // post-mortems.
                            stats.failed.fetch_add(1, Ordering::Relaxed);
                            record_error(&errors, format!("request {request_id}: {e}"));
                        }
                    }

                    let elapsed = tick.elapsed();
                    if elapsed < interval {
                        tokio::time::sleep(interval - elapsed).await;
                    }
                }
            })
        };

        Self {
            stats,
            stop,
            task: Some(task),
            errors,
        }
    }

    pub fn stats(&self) -> &Arc<DriverStats> {
        &self.stats
    }

    /// Number of acknowledged writes so far.
    pub fn acked(&self) -> u64 {
        self.stats.acked.load(Ordering::Relaxed)
    }

    /// Sampled driver errors (bounded to the first 8).
    pub fn error_samples(&self) -> Vec<String> {
        self.errors.lock().unwrap().clone()
    }

    /// Stop the write loop and wait for the task to finish.
    pub async fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(task) = self.task.take() {
            // The task may be blocked inside a client retry ladder (bounded
            // ~25s); abort rather than wait it out on the teardown path.
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn record_error(errors: &Mutex<Vec<String>>, msg: String) {
    let mut guard = errors.lock().unwrap();
    if guard.len() < 8 {
        guard.push(msg);
    }
}

#[cfg(test)]
mod tests {
    use super::COUNT_READ_QUERY;

    #[test]
    fn count_query_never_collides_with_write_payloads() {
        for id in [0, 1, 42, u64::MAX] {
            assert!(!crate::payload_for(id).starts_with(COUNT_READ_QUERY));
            assert!(!crate::probe_payload_for(id).starts_with(COUNT_READ_QUERY));
        }
    }
}
