//! The replay oracle — the heart of the harness.
//!
//! After a kill/restart round, replay every entry of the ack log by
//! re-issuing the same keyed write with a **different probe payload**:
//!
//! - **Response == original payload** → the idempotency layer held the key
//!   and served the cached response without touching the state machine.
//!   Since the layer is rebuilt from the persisted log (snapshot + replay),
//!   this proves the original entry survived the crash *and* was not
//!   re-applied — exactly-once across restart.
//! - **Response == probe payload** → the key was unknown to the restarted
//!   cluster, so the probe was freshly applied: the original, *acknowledged*
//!   write was lost. INV-1 violated; the scenario fails with the offending
//!   request id.
//! - **`IdempotencyExpired`** → the watermark advanced past the key but the
//!   cached response is gone. Not expected at this scale (LRU capacity
//!   10 000 ≫ run size); treated as a failure because survival can no longer
//!   be proven.
//!
//! This is strictly stronger than comparing `last_log_index` against acked
//! indices (the in-process test's oracle): it validates the full chain
//! log → state machine → idempotency cache → client-visible response.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rr_client::RrClient;
use rr_core::error::RrError;
use rr_core::idempotency::IdempotencyKey;

use crate::cluster::GROUP_ID;
use crate::journal::{AckRecord, Journal};
use crate::{payload_for, payload_hash, probe_payload_for, CLIENT_ID};

/// Hard ceiling for one full ack-log replay. Without it, a wedged cluster
/// (e.g. a raft core that died mid-run) makes the scenario hang until the
/// outer test timeout; with it, the failure is fast and says why.
const ORACLE_TIMEOUT: Duration = Duration::from_secs(120);

/// Outcome of a full ack-log replay.
pub struct OracleReport {
    pub checked: usize,
    pub verified: usize,
    pub elapsed: Duration,
}

/// INV-1 count-bound check for the batched unkeyed lane.
///
/// The counting echo service's applied count is rebuilt from the durable log
/// (snapshot + tail replay) after a restart, so it is a client-visible proof
/// that batched entries survived the crash:
///
/// - `count < acked_floor` — an **acknowledged** write was lost (INV-1
///   violated). `acked_floor` = keyed acks + batched lane acks.
/// - `count > sent_ceiling` — the cluster applied writes the harness never
///   sent (double-apply/phantom). `sent_ceiling` = keyed sent + batched sent
///   (sampled *after* the count read so in-flight writes can't trip it).
///
/// Returns the observed count.
pub async fn verify_applied_count_bounds(
    client: &RrClient,
    acked_floor: u64,
    sent_ceiling_after: impl Fn() -> (u64, u64),
) -> Result<u64> {
    let bytes = client
        .read(GROUP_ID, crate::driver::COUNT_READ_QUERY.to_vec())
        .await
        .context("applied-count read failed")?;
    let text = String::from_utf8(bytes).context("applied-count read not utf-8")?;
    let count: u64 = text
        .trim()
        .parse()
        .with_context(|| format!("applied-count read not a number: {text:?}"))?;
    let (keyed_sent, batch_sent) = sent_ceiling_after();
    let ceiling = keyed_sent + batch_sent;
    if count < acked_floor {
        bail!(
            "INV-1 VIOLATION: applied count {count} < acked floor {acked_floor} — \
             an acknowledged write did not survive the crash"
        );
    }
    if count > ceiling {
        bail!(
            "applied count {count} > sent ceiling {ceiling} — the cluster applied \
             writes the harness never sent (double-apply/phantom)"
        );
    }
    Ok(count)
}

/// Replay every ack-log record against the cluster and verify each one takes
/// the idempotent-replay path with the original payload intact.
///
/// Fails (Err) on the first lost/corrupted/unverifiable write — the message
/// carries the request id and the evidence class for the post-mortem.
pub async fn replay_ack_log(client: &RrClient, ack_log_path: &Path) -> Result<OracleReport> {
    match tokio::time::timeout(ORACLE_TIMEOUT, replay_inner(client, ack_log_path)).await {
        Ok(res) => res,
        Err(_) => bail!(
            "oracle replay exceeded {ORACLE_TIMEOUT:?} — cluster wedged mid-replay; \
             check node logs for raft-core panics (a recurrence of the \
             debug-build `entries_stream` read-after-submit assert would mean \
             the RocksLogStorage pending overlay regressed — see ADR A-0015)"
        ),
    }
}

async fn replay_inner(client: &RrClient, ack_log_path: &Path) -> Result<OracleReport> {
    let records: Vec<AckRecord> = Journal::read_all(ack_log_path)
        .with_context(|| format!("read ack log {}", ack_log_path.display()))?;
    let started = Instant::now();
    let mut verified = 0usize;

    for record in &records {
        verify_one(client, record)
            .await
            .with_context(|| format!("ack log entry request_id={}", record.request_id))?;
        verified += 1;
    }

    Ok(OracleReport {
        checked: records.len(),
        verified,
        elapsed: started.elapsed(),
    })
}

async fn verify_one(client: &RrClient, record: &AckRecord) -> Result<()> {
    let original = payload_for(record.request_id);
    let probe = probe_payload_for(record.request_id);

    // Cross-check the journal's hash against the regenerated payload — guards
    // against ack-log corruption and against payload-format drift between
    // driver and oracle.
    let expected_hash = payload_hash(&original);
    if record.payload_hash != expected_hash {
        bail!(
            "ack log hash mismatch: recorded {} vs regenerated {expected_hash} — \
             ack log corrupted or payload format drifted",
            record.payload_hash
        );
    }

    let key = IdempotencyKey::with_client_id(&record.key, record.request_id, CLIENT_ID);
    match client
        .send_request_idempotent(GROUP_ID, probe.clone(), key)
        .await
    {
        Ok(resp) if resp == original => Ok(()),
        Ok(resp) if resp == probe => bail!(
            "INV-1 VIOLATION: acknowledged write was freshly re-applied after crash \
             (probe payload echoed) — the original log entry did not survive restart"
        ),
        Ok(resp) => bail!(
            "unexpected response ({} bytes, first 16: {:02x?}): neither the original \
             payload nor the probe payload",
            resp.len(),
            &resp[..resp.len().min(16)]
        ),
        Err(RrError::IdempotencyExpired {
            request_type,
            request_id,
            watermark,
        }) => bail!(
            "idempotency expired for ({request_type}, {request_id}) watermark={watermark}: \
             watermark survived but the cached response is gone — cannot prove survival"
        ),
        Err(e) => bail!("oracle probe failed after client retries: {e}"),
    }
}
