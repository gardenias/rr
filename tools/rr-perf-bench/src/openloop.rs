//! Open-loop load engine (rr-prod-perf plan §5.2 items 1–5, milestone M2):
//! planned-time scheduling with coordinated-omission-corrected latency
//! accounting, operation mixes, key distributions and 1s time-series output.
//!
//! # Why open-loop
//!
//! Closed-loop drivers (S1/S2/S3) let the next request wait for the previous
//! completion, so a slow server throttles the offered load and the measured
//! percentiles systematically UNDERSTATE true tail latency (coordinated
//! omission). This engine schedules every request at a planned instant
//! (constant grid or Poisson arrivals) and records three latencies per op:
//!
//! - `lat` — scheduled → completion: the client-perceived truth, includes
//!   queueing behind a saturated server;
//! - `svc` — actual send → completion: the pure service time;
//! - `lag` — scheduled → actual send: scheduler/backpressure debt (semaphore
//!   waits, timer slew). `lag` ≈ 0 means the driver kept up with the plan.
//!
//! # Wire protocol
//!
//! The engine speaks the **kv-store KV protocol** (bincode `KvRequest`,
//! shared with soak.rs). Writes go through the PLAIN `send_request` path
//! (real Raft + apply + fsync + KV decode, no idempotency layer), because
//! keyed writes under open-loop concurrency trip the framework's idempotency
//! contract by construction: the layer requires monotonically ordered
//! `request_id`s per `request_type` namespace (`rr-raft/src/idempotency.rs`
//! watermark rule — apply 26 before 25 and 25 is Expired), while concurrent
//! in-flight writes are exactly what an open-loop driver exists to produce.
//! The keyed write path is measured continuously by the soak scenario (W9)
//! and the Kafka ingest path (W6); the open-loop engine measures the write
//! envelope underneath it.
//!
//! The EchoSM behind stock `rr-server` does not decode KV payloads; run
//! mix/wladder against kv-store (perf-lab or a local node), not against a
//! spawned rr-server cluster.
//!
//! # Reproducibility
//!
//! All randomness (arrival process, op selection, key selection) derives from
//! seeded SplitMix64 streams — the same public-domain generator family as
//! rr-core's fault injection (`storage_fault.rs`; private there, duplicated
//! per this crate's no-extraction convention, cf. soak.rs's journal). Same
//! seed + same flags ⇒ same offered-load trace.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use hdrhistogram::Histogram;
use rr_client::RrClient;
use serde::Serialize;
use tokio::sync::{mpsc, Semaphore};

use crate::soak::{decode_resp, encode_req, KvRequest};
use crate::workload::SharedClient;

/// Partition the engine drives (kv-store serves group 1, same as soak).
const PARTITION_ID: u64 = 1;

/// Safety net: how long to wait for in-flight ops after the last scheduled
/// instant before counting them incomplete. Client retries are bounded, so a
/// healthy drain takes seconds; this only trips on a wedged cluster.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

// ── Seeded RNG (SplitMix64) ─────────────────────────────────────────────────
//
// Byte-for-byte the rr-core storage_fault.rs generator (module is private
// there; this crate deliberately duplicates, see module docs).

struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform double in `[0, 1)` (top 53 bits — the standard construction).
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

// ── Operation mix ────────────────────────────────────────────────────────────

/// Which client operation the engine fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchOp {
    Write,
    ReadLocal,
    ReadLin,
}

impl BenchOp {
    const ALL: [BenchOp; 3] = [BenchOp::Write, BenchOp::ReadLocal, BenchOp::ReadLin];

    pub fn as_str(self) -> &'static str {
        match self {
            BenchOp::Write => "write",
            BenchOp::ReadLocal => "local",
            BenchOp::ReadLin => "lin",
        }
    }

    fn idx(self) -> usize {
        match self {
            BenchOp::Write => 0,
            BenchOp::ReadLocal => 1,
            BenchOp::ReadLin => 2,
        }
    }

    fn from_token(token: &str) -> Option<Self> {
        match token {
            "write" => Some(BenchOp::Write),
            "local" => Some(BenchOp::ReadLocal),
            "lin" => Some(BenchOp::ReadLin),
            _ => None,
        }
    }
}

/// Weighted operation mix, e.g. `write:90,local:5,lin:5`. Weights are
/// relative (they need not sum to 100); selection is a seeded weighted draw.
#[derive(Clone, Debug)]
pub struct MixSpec {
    entries: Vec<(BenchOp, u64)>,
    total: u64,
}

impl MixSpec {
    pub fn parse(spec: &str) -> Result<Self> {
        let mut entries = Vec::new();
        for part in spec.split(',') {
            let part = part.trim();
            let (name, weight) = part
                .split_once(':')
                .with_context(|| format!("bad --mix entry '{part}' (expected kind:weight)"))?;
            let op = BenchOp::from_token(name.trim())
                .with_context(|| format!("bad --mix op '{name}' (write|local|lin)"))?;
            let weight: u64 = weight
                .trim()
                .parse()
                .with_context(|| format!("bad --mix weight '{weight}' (positive integer)"))?;
            if weight == 0 {
                bail!("bad --mix entry '{part}': weight must be > 0");
            }
            if entries.iter().any(|(o, _)| *o == op) {
                bail!("bad --mix spec '{spec}': duplicate op '{name}'");
            }
            entries.push((op, weight));
        }
        let total: u64 = entries.iter().map(|(_, w)| *w).sum();
        if total == 0 {
            bail!("--mix must contain at least one op with weight > 0");
        }
        Ok(MixSpec { entries, total })
    }

    fn pick(&self, rng: &mut SplitMix64) -> BenchOp {
        let mut draw = rng.next_u64() % self.total;
        for (op, weight) in &self.entries {
            if draw < *weight {
                return *op;
            }
            draw -= *weight;
        }
        self.entries[0].0 // unreachable with total > 0, but don't panic in a bench
    }

    /// Canonical string form (config echo in reports).
    pub fn canonical(&self) -> String {
        self.entries
            .iter()
            .map(|(op, w)| format!("{}:{w}", op.as_str()))
            .collect::<Vec<_>>()
            .join(",")
    }
}

// ── Key distribution ─────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum KeyDist {
    Uniform,
    Zipfian { theta: f64 },
}

impl KeyDist {
    pub fn as_str(&self) -> String {
        match self {
            KeyDist::Uniform => "uniform".to_string(),
            KeyDist::Zipfian { theta } => format!("zipfian({theta})"),
        }
    }
}

/// Zipfian CDF tables cost 8 bytes per key; refuse absurd spaces so a typo'd
/// `--key-space` cannot OOM the load container.
const ZIPFIAN_MAX_SPACE: u64 = 16_777_216;

/// Seeded key generator over `[0, key_space)`.
struct KeyGen {
    space: u64,
    /// CDF table for zipfian: `cdf[k] = Σ_{i=1..=k+1} 1/i^theta`.
    cdf: Option<Vec<f64>>,
}

impl KeyGen {
    fn new(space: u64, dist: KeyDist) -> Result<Self> {
        if space == 0 {
            bail!("--key-space must be >= 1");
        }
        let cdf = match dist {
            KeyDist::Uniform => None,
            KeyDist::Zipfian { theta } => {
                if !(theta > 0.0 && theta.is_finite()) {
                    bail!("--zipf-theta must be a positive finite number, got {theta}");
                }
                if space > ZIPFIAN_MAX_SPACE {
                    bail!(
                        "--key-space {space} too large for zipfian (max {ZIPFIAN_MAX_SPACE}); \
                         use uniform or a smaller space"
                    );
                }
                let mut cdf = Vec::with_capacity(space as usize);
                let mut acc = 0.0f64;
                for i in 1..=space {
                    acc += 1.0 / (i as f64).powf(theta);
                    cdf.push(acc);
                }
                Some(cdf)
            }
        };
        Ok(KeyGen { space, cdf })
    }

    fn next(&mut self, rng: &mut SplitMix64) -> u64 {
        match &self.cdf {
            None => rng.next_u64() % self.space,
            Some(cdf) => {
                let draw = rng.next_f64() * cdf[cdf.len() - 1];
                // First index whose cumulative weight >= draw.
                cdf.partition_point(|&c| c < draw) as u64
            }
        }
    }
}

/// KV key for `key_id`. The space is deliberately shared across bench
/// clients: cross-client contention on a hot key is the realism W4 measures.
fn kv_key(key_id: u64) -> String {
    format!("bench:{key_id:010}")
}

/// Write value carrying its key + sequence for server-side observability
/// (plan §5.2 item 2), padded to `payload_size` like soak's `value_for`.
fn put_value(key: &str, seq: u64, payload_size: usize) -> Vec<u8> {
    let head = format!("v:{key}:{seq:012}-");
    let mut v = head.into_bytes();
    v.resize(payload_size.max(v.len()), b'v');
    v
}

// ── Time series (1s buckets, JSONL) ─────────────────────────────────────────

/// One second of one op kind: a small HDR (2 significant figures is plenty
/// for per-second p99) plus counters. True latency only (scheduled →
/// completion) — the G5 spike-visibility series.
struct Bucket {
    hdr: Histogram<u64>,
    err: u64,
}

impl Bucket {
    fn new() -> Self {
        Bucket {
            hdr: Histogram::<u64>::new_with_bounds(1_000, 30_000_000_000, 2)
                .expect("valid histogram bounds"),
            err: 0,
        }
    }
}

/// Per-second buckets indexed from the measurement window start. Buckets are
/// attributed by SCHEDULED time (offered-load alignment), so completions
/// spilling past a window edge still land in the second that was offered.
pub struct TimeSeries {
    origin: Instant,
    /// `[second][op]` — grown on demand, so long runs cost ~1 KiB/op/second.
    buckets: Vec<[Option<Bucket>; 3]>,
}

impl TimeSeries {
    pub fn new(origin: Instant) -> Self {
        TimeSeries {
            origin,
            buckets: Vec::new(),
        }
    }

    fn record(&mut self, op: BenchOp, scheduled: Instant, latency: Duration, ok: bool) {
        let Some(since) = scheduled.checked_duration_since(self.origin) else {
            return; // warmup op (scheduled before the window)
        };
        let sec = since.as_secs() as usize;
        while self.buckets.len() <= sec {
            self.buckets.push([None, None, None]);
        }
        let slot = &mut self.buckets[sec][op.idx()];
        if slot.is_none() {
            *slot = Some(Bucket::new());
        }
        let bucket = slot.as_mut().expect("just filled");
        if ok {
            let _ = bucket.hdr.record(latency.as_nanos() as u64);
        } else {
            bucket.err += 1;
        }
    }

    /// Render JSONL: one line per (second, op) with any activity.
    /// `rung_secs` adds a `rung` field (ladder runs); `None` for mix.
    pub fn to_jsonl(&self, rung_secs: Option<u64>) -> String {
        #[derive(Serialize)]
        struct Line {
            t: u64,
            op: &'static str,
            count: u64,
            err: u64,
            p50_us: u64,
            p99_us: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            rung: Option<u64>,
        }

        let mut out = String::new();
        for (sec, ops) in self.buckets.iter().enumerate() {
            for op in BenchOp::ALL {
                let Some(bucket) = &ops[op.idx()] else {
                    continue;
                };
                if bucket.hdr.is_empty() && bucket.err == 0 {
                    continue;
                }
                let line = Line {
                    t: sec as u64,
                    op: op.as_str(),
                    count: bucket.hdr.len(),
                    err: bucket.err,
                    p50_us: bucket.hdr.value_at_percentile(50.0) / 1_000,
                    p99_us: bucket.hdr.value_at_percentile(99.0) / 1_000,
                    rung: rung_secs.map(|d| sec as u64 / d),
                };
                out.push_str(&serde_json::to_string(&line).expect("TsLine serializes"));
                out.push('\n');
            }
        }
        out
    }
}

// ── Reports ──────────────────────────────────────────────────────────────────

/// Per-op rollup for one fixed-rate window. The three latency families map
/// to the module docs: `p*`/`max` = true latency (scheduled → completion),
/// `svc_*` = service time, `lag_*` = scheduler debt (scheduled → actual).
#[derive(Serialize)]
pub struct OpReport {
    pub op: &'static str,
    pub offered: u64,
    pub ok: u64,
    pub err: u64,
    /// Scheduled but still in-flight at the drain deadline.
    pub incomplete: u64,
    pub tps: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub svc_p50_us: u64,
    pub svc_p99_us: u64,
    pub lag_p50_us: u64,
    pub lag_p99_us: u64,
}

#[derive(Serialize)]
pub struct RunReport {
    pub rung: usize,
    pub rate: u32,
    pub offered: u64,
    pub window_secs: u64,
    pub ops: Vec<OpReport>,
}

/// One scenario run (mix = single rung, wladder = N rungs) with its full
/// configuration echo — every number in `runs` must be reproducible from
/// these fields plus the same binary.
#[derive(Serialize)]
pub struct ScenarioReport {
    pub rates: Vec<u32>,
    pub rung_secs: u64,
    pub warmup_secs: u64,
    pub poisson: bool,
    pub mix: String,
    pub key_dist: String,
    pub key_space: u64,
    pub payload_bytes: usize,
    pub client_id: u32,
    pub seed: u64,
    pub max_in_flight: usize,
    pub op_timeout_ms: u64,
    /// Wall-clock start of the measurement window (post-warmup) — aligns the
    /// client HDRs with the Prometheus window (plan §5.2 item 6).
    pub started_unix_secs: u64,
    /// Server-side rollup over the same window; present iff --prom-url was
    /// given (set by main after the run, post-drain).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_side: Option<crate::prom::ServerSideWindow>,
    pub runs: Vec<RunReport>,
}

// ── Engine ───────────────────────────────────────────────────────────────────

pub struct OpenLoopConfig {
    /// Per-rung rates; a single-element vector is the fixed-rate mix case.
    pub rates: Vec<u32>,
    pub rung_duration: Duration,
    pub warmup: Duration,
    pub poisson: bool,
    pub mix: MixSpec,
    pub key_space: u64,
    pub key_dist: KeyDist,
    pub payload_size: usize,
    pub client_id: u32,
    pub seed: u64,
    pub max_in_flight: usize,
    /// Per-op deadline: a slow op past this is counted as an error and its
    /// permit freed — bounds the drain (a wedged cluster can stall the
    /// firing loop's semaphore wait forever otherwise; W5-16KiB smoke found
    /// this). Default matches the HDR ceiling (30s).
    pub op_timeout: Duration,
    /// 1s-bucket time series recorded alongside the aggregate histograms.
    pub timeseries: bool,
}

/// Engine output: the aggregate report plus the rendered time series (empty
/// string when disabled).
pub struct EngineOutcome {
    pub report: ScenarioReport,
    pub timeseries_jsonl: String,
}

/// Per-(rung, op) accumulator, owned by the single recorder task (no locks
/// on the hot path beyond one mutex per completion message).
struct OpAccum {
    lat: Histogram<u64>,
    svc: Histogram<u64>,
    lag: Histogram<u64>,
    ok: u64,
    err: u64,
}

impl OpAccum {
    fn new() -> Self {
        let mk = || Histogram::<u64>::new_with_bounds(1_000, 30_000_000_000, 3).expect("bounds");
        OpAccum {
            lat: mk(),
            svc: mk(),
            lag: mk(),
            ok: 0,
            err: 0,
        }
    }
}

struct Completion {
    op: BenchOp,
    scheduled: Instant,
    actual: Instant,
    done: Instant,
    ok: bool,
}

/// Which rung does `t` fall in? Warmup (before `measure_start`) → None.
fn rung_of(
    t: Instant,
    measure_start: Instant,
    rung_duration: Duration,
    rungs: usize,
) -> Option<usize> {
    let since = t.checked_duration_since(measure_start)?;
    let r = (since.as_secs_f64() / rung_duration.as_secs_f64()) as usize;
    (r < rungs).then_some(r)
}

fn rate_for(rung: Option<usize>, rates: &[u32]) -> f64 {
    // Warmup runs at the first rung's rate so the cluster is warm at window 0.
    f64::from(rates[rung.unwrap_or(0)])
}

/// Inter-arrival for the arrival process: exact constant grid, or Poisson
/// via inverse-CDF on Exp(rate).
fn next_interval(rng: &mut SplitMix64, rate: f64, poisson: bool) -> Duration {
    let secs = if poisson {
        let u = rng.next_f64();
        // u ∈ [0,1); 1-u ∈ (0,1] — ln never sees 0.
        -f64::ln(1.0 - u) / rate
    } else {
        1.0 / rate
    };
    Duration::from_secs_f64(secs)
}

/// Execute one scheduled op. `true` = completed with a decodable response
/// (the apply path demonstrably ran); any transport error or undecodable
/// response counts as a failure, matching soak's strictness. Writes are
/// unkeyed (see module docs); reads decode-validate the `KvResponse`.
async fn exec_op(client: &RrClient, op: BenchOp, payload: Vec<u8>) -> bool {
    let result = match op {
        BenchOp::Write => client.send_request(PARTITION_ID, payload).await,
        BenchOp::ReadLocal => client.read(PARTITION_ID, payload).await,
        BenchOp::ReadLin => client.read_linearizable(PARTITION_ID, payload).await,
    };
    match result {
        Ok(resp) => match decode_resp(&resp) {
            Ok(_) => true,
            Err(e) => {
                log_failure(op, &format_args!("response undecodable: {e}"));
                false
            }
        },
        Err(e) => {
            log_failure(op, &e);
            false
        }
    }
}

/// Sample-log the first few failures at WARN (same pattern as workload.rs).
fn log_failure<E: std::fmt::Display>(op: BenchOp, e: &E) {
    static LOGGED: AtomicUsize = AtomicUsize::new(0);
    let n = LOGGED.fetch_add(1, Ordering::Relaxed);
    if n < 5 {
        tracing::warn!(op = ?op, error = %e, "open-loop op failed (sampled)");
    }
}

/// Run the engine: one continuous schedule with piecewise-constant rates
/// (the ladder), a warmup prelude at the first rung's rate, and a bounded
/// drain. Records are attributed by scheduled instant, so inter-rung queue
/// spillover stays correctly attributed.
pub async fn run(client: SharedClient, cfg: &OpenLoopConfig) -> Result<EngineOutcome> {
    if cfg.rates.is_empty() {
        bail!("open-loop engine needs at least one rate");
    }
    if cfg.rates.contains(&0) {
        bail!("rates must be >= 1 ops/s");
    }
    if cfg.max_in_flight == 0 {
        bail!("--max-in-flight must be >= 1");
    }
    if cfg.op_timeout.is_zero() {
        bail!("--op-timeout-secs must be >= 1");
    }

    let rungs = cfg.rates.len();
    let wall0 = crate::prom::now_unix_secs();
    let t0 = Instant::now();
    let measure_start = t0 + cfg.warmup;
    let end = measure_start + cfg.rung_duration * rungs as u32;

    let sem = Arc::new(Semaphore::new(cfg.max_in_flight));
    let in_flight = Arc::new(AtomicUsize::new(0));
    // Offered counters are bumped in the firing loop: [rung][op].
    let offered: Vec<Vec<AtomicU64>> = (0..rungs)
        .map(|_| (0..3).map(|_| AtomicU64::new(0)).collect())
        .collect();
    let offered = Arc::new(offered);

    let (tx, mut rx) = mpsc::unbounded_channel::<Completion>();

    // ── Firing loop: awaits only the timer and the in-flight semaphore, never
    // the network; each op runs in its own task.
    let fire = {
        let client = Arc::clone(&client);
        let offered = Arc::clone(&offered);
        let in_flight = Arc::clone(&in_flight);
        let sem = Arc::clone(&sem);
        let tx = tx.clone();
        async move {
            let mut sched_rng = SplitMix64::new(cfg.seed);
            let mut mix_rng = SplitMix64::new(cfg.seed ^ 0x9E37_79B9_7F4A_7C15);
            let mut key_rng = SplitMix64::new(cfg.seed ^ 0x3C79_AC49_2BA7_B653);
            let mut keygen = KeyGen::new(cfg.key_space, cfg.key_dist)?;
            let op_timeout = cfg.op_timeout;
            let payload_size = cfg.payload_size;
            let mut seq = 0u64;
            let mut next = t0;

            loop {
                let rate = rate_for(
                    rung_of(next, measure_start, cfg.rung_duration, rungs),
                    &cfg.rates,
                );
                next += next_interval(&mut sched_rng, rate, cfg.poisson);
                if next >= end {
                    break;
                }
                // Sleep only when AHEAD of the plan; when behind, iterate
                // immediately so one poll fires the whole catch-up burst —
                // per-op awaits would let executor pressure starve the driver
                // and silently turn the run closed-loop (the exact bias this
                // engine exists to expose; lag records the real debt).
                if next > Instant::now() {
                    tokio::time::sleep_until(next.into()).await;
                }

                // A saturated server fills the in-flight cap and parks this
                // acquire; if the schedule window ends while parked, stop
                // instead of hanging (W5-16KiB smoke).
                let permit = tokio::select! {
                    p = Arc::clone(&sem).acquire_owned() => p.expect("semaphore closed"),
                    _ = tokio::time::sleep_until(end.into()) => break,
                };
                let actual = Instant::now();
                seq += 1;

                let op = cfg.mix.pick(&mut mix_rng);
                let key_id = keygen.next(&mut key_rng);

                if let Some(r) = rung_of(next, measure_start, cfg.rung_duration, rungs) {
                    offered[r][op.idx()].fetch_add(1, Ordering::Relaxed);
                }
                in_flight.fetch_add(1, Ordering::Relaxed);

                let client = Arc::clone(&client);
                let tx_cb = tx.clone();
                let in_flight_cb = Arc::clone(&in_flight);
                // The payload is built ON THE OP TASK (not the firing loop):
                // per-op encode work parallelizes across worker threads and
                // cannot tax the driver's iteration rate.
                tokio::spawn(async move {
                    let key = kv_key(key_id);
                    let payload = match op {
                        BenchOp::Write => encode_req(&KvRequest::Put {
                            value: put_value(&key, seq, payload_size),
                            key,
                        }),
                        BenchOp::ReadLocal | BenchOp::ReadLin => {
                            encode_req(&KvRequest::Get { key })
                        }
                    };
                    // Per-op deadline bounds every op's lifetime — and hence
                    // the drain — even when the cluster is wedged.
                    let ok = match tokio::time::timeout(op_timeout, exec_op(&client, op, payload))
                        .await
                    {
                        Ok(ok) => ok,
                        Err(_) => {
                            log_failure(op, &"op deadline exceeded (--op-timeout-secs)");
                            false
                        }
                    };
                    let done = Instant::now();
                    drop(permit);
                    in_flight_cb.fetch_sub(1, Ordering::Relaxed);
                    let _ = tx_cb.send(Completion {
                        op,
                        scheduled: next,
                        actual,
                        done,
                        ok,
                    });
                });
            }
            anyhow::Ok(())
        }
    };

    // ── Recorder: single consumer; owns the aggregate histograms. State sits
    // behind one mutex so it survives a drain-timeout drop of the record
    // future (timeout cancels only at `.await` points, where no lock is held).
    struct RecState {
        stats: Vec<Vec<OpAccum>>,
        timeseries: Option<TimeSeries>,
    }
    let rec_state = Arc::new(Mutex::new(RecState {
        stats: (0..rungs)
            .map(|_| (0..3).map(|_| OpAccum::new()).collect())
            .collect(),
        timeseries: cfg.timeseries.then(|| TimeSeries::new(measure_start)),
    }));

    let rung_duration = cfg.rung_duration;
    let record = {
        let rec_state = Arc::clone(&rec_state);
        async move {
            while let Some(c) = rx.recv().await {
                let Some(rung) = rung_of(c.scheduled, measure_start, rung_duration, rungs) else {
                    continue; // warmup completion
                };
                let lat = c.done.saturating_duration_since(c.scheduled);
                let svc = c.done.saturating_duration_since(c.actual);
                let lag = c.actual.saturating_duration_since(c.scheduled);
                let mut g = rec_state.lock().expect("recorder state poisoned");
                let acc = &mut g.stats[rung][c.op.idx()];
                if c.ok {
                    acc.ok += 1;
                    let _ = acc.lat.record(lat.as_nanos() as u64);
                    let _ = acc.svc.record(svc.as_nanos() as u64);
                    let _ = acc.lag.record(lag.as_nanos() as u64);
                } else {
                    acc.err += 1;
                }
                if let Some(ts) = g.timeseries.as_mut() {
                    ts.record(c.op, c.scheduled, lat, c.ok);
                }
            }
        }
    };

    // The record loop ends when every sender drops: the firing loop's clone
    // drops when it returns, each spawned op's clone drops on completion, and
    // the original sender is dropped here. `timeout` bounds the wait so a
    // wedged cluster cannot hang the run; stragglers surface as `incomplete`.
    drop(tx);
    let record_budget = end.saturating_duration_since(Instant::now()) + DRAIN_TIMEOUT;
    let (fire_result, record_timed_out) = tokio::join!(fire, async move {
        tokio::time::timeout(record_budget, record).await.is_err()
    });
    fire_result.context("firing loop failed")?;
    if record_timed_out {
        eprintln!(
            "[open-loop] WARNING: drain deadline hit with {} ops still in flight; \
             they are counted as incomplete",
            in_flight.load(Ordering::Relaxed)
        );
    }

    // ── Roll up.
    let mut rec = rec_state.lock().expect("recorder state poisoned");
    let window_secs = cfg.rung_duration.as_secs_f64();
    let mut runs = Vec::with_capacity(rungs);
    for (r, rate) in cfg.rates.iter().enumerate() {
        let mut ops = Vec::new();
        for op in BenchOp::ALL {
            let acc = &mut rec.stats[r][op.idx()];
            let offered_n = offered[r][op.idx()].load(Ordering::Relaxed);
            let incomplete = offered_n.saturating_sub(acc.ok + acc.err);
            if offered_n == 0 && incomplete == 0 {
                continue; // op not present in this window
            }
            ops.push(OpReport {
                op: op.as_str(),
                offered: offered_n,
                ok: acc.ok,
                err: acc.err,
                incomplete,
                tps: acc.ok as f64 / window_secs,
                p50_us: acc.lat.value_at_percentile(50.0) / 1_000,
                p95_us: acc.lat.value_at_percentile(95.0) / 1_000,
                p99_us: acc.lat.value_at_percentile(99.0) / 1_000,
                p999_us: acc.lat.value_at_percentile(99.9) / 1_000,
                max_us: acc.lat.max() / 1_000,
                svc_p50_us: acc.svc.value_at_percentile(50.0) / 1_000,
                svc_p99_us: acc.svc.value_at_percentile(99.0) / 1_000,
                lag_p50_us: acc.lag.value_at_percentile(50.0) / 1_000,
                lag_p99_us: acc.lag.value_at_percentile(99.0) / 1_000,
            });
        }
        runs.push(RunReport {
            rung: r,
            rate: *rate,
            offered: ops.iter().map(|o| o.offered).sum(),
            window_secs: cfg.rung_duration.as_secs(),
            ops,
        });
    }

    let rung_secs = (rungs > 1).then_some(cfg.rung_duration.as_secs());
    let timeseries_jsonl = rec
        .timeseries
        .as_ref()
        .map(|ts| ts.to_jsonl(rung_secs))
        .unwrap_or_default();

    Ok(EngineOutcome {
        report: ScenarioReport {
            rates: cfg.rates.clone(),
            rung_secs: cfg.rung_duration.as_secs(),
            warmup_secs: cfg.warmup.as_secs(),
            poisson: cfg.poisson,
            mix: cfg.mix.canonical(),
            key_dist: cfg.key_dist.as_str(),
            key_space: cfg.key_space,
            payload_bytes: cfg.payload_size,
            client_id: cfg.client_id,
            seed: cfg.seed,
            max_in_flight: cfg.max_in_flight,
            op_timeout_ms: cfg.op_timeout.as_millis() as u64,
            // Wall-clock window start = process wall time + warmup (Instant
            // and SystemTime aren't linked; the two are captured together at
            // entry and the offset is exact).
            started_unix_secs: wall0 + cfg.warmup.as_secs(),
            server_side: None,
            runs,
        },
        timeseries_jsonl,
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_matches_reference_vector() {
        // Well-known SplitMix64 sequence for seed 0 (public-domain reference
        // implementation); pins the generator so seeded runs stay
        // reproducible across refactors. Same values rr-core's
        // storage_fault.rs produces.
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
    }

    #[test]
    fn splitmix64_is_reproducible() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn mix_parse_happy_path() {
        let m = MixSpec::parse("write:90, local:5,lin:5").unwrap();
        assert_eq!(m.entries.len(), 3);
        assert_eq!(m.total, 100);
        assert_eq!(m.canonical(), "write:90,local:5,lin:5");
        // Weights are relative — no sum-to-100 requirement.
        let m = MixSpec::parse("write:1,lin:1").unwrap();
        assert_eq!(m.total, 2);
    }

    #[test]
    fn mix_parse_rejects_bad_specs() {
        assert!(MixSpec::parse("").is_err());
        assert!(MixSpec::parse("write").is_err());
        assert!(MixSpec::parse("write:0").is_err());
        assert!(MixSpec::parse("write:abc").is_err());
        assert!(MixSpec::parse("bogus:5").is_err());
        assert!(MixSpec::parse("write:5,write:5").is_err());
    }

    #[test]
    fn mix_pick_is_reproducible_and_respects_weights() {
        let m = MixSpec::parse("write:100").unwrap();
        let mut rng = SplitMix64::new(7);
        for _ in 0..100 {
            assert_eq!(m.pick(&mut rng), BenchOp::Write);
        }
        let m = MixSpec::parse("write:50,local:50").unwrap();
        let mut a = (0usize, 0usize);
        let mut rng = SplitMix64::new(7);
        for _ in 0..10_000 {
            match m.pick(&mut rng) {
                BenchOp::Write => a.0 += 1,
                BenchOp::ReadLocal => a.1 += 1,
                _ => unreachable!(),
            }
        }
        // 50/50 ± 5% band.
        assert!((4500..=5500).contains(&a.0), "write count {}", a.0);
    }

    #[test]
    fn uniform_keys_stay_in_space() {
        let mut g = KeyGen::new(1000, KeyDist::Uniform).unwrap();
        let mut rng = SplitMix64::new(1);
        for _ in 0..10_000 {
            assert!(g.next(&mut rng) < 1000);
        }
    }

    #[test]
    fn zipfian_is_skewed_bounded_and_reproducible() {
        let space = 1000;
        let mut g = KeyGen::new(space, KeyDist::Zipfian { theta: 0.99 }).unwrap();
        let mut rng = SplitMix64::new(9);
        let mut counts = vec![0usize; space as usize];
        for _ in 0..100_000 {
            let k = g.next(&mut rng);
            assert!(k < space);
            counts[k as usize] += 1;
        }
        // theta=0.99, space=1000: H ≈ 7.8 ⇒ p(key 0) ≈ 13%; uniform would be
        // 0.1%. Demand a wide, implementation-independent band.
        let top = counts[0] as f64 / 100_000.0;
        assert!(top > 0.05, "key 0 frequency {top} not zipf-skewed");
        assert!(top < 0.30, "key 0 frequency {top} absurdly high");
        // Reproducibility: same seed ⇒ same trace.
        let mut g2 = KeyGen::new(space, KeyDist::Zipfian { theta: 0.99 }).unwrap();
        let mut r1 = SplitMix64::new(5);
        let mut r2 = SplitMix64::new(5);
        for _ in 0..1000 {
            assert_eq!(g2.next(&mut r1), g2.next(&mut r2));
        }
    }

    #[test]
    fn zipfian_rejects_bad_params() {
        assert!(KeyGen::new(0, KeyDist::Uniform).is_err());
        assert!(KeyGen::new(10, KeyDist::Zipfian { theta: 0.0 }).is_err());
        assert!(KeyGen::new(10, KeyDist::Zipfian { theta: f64::NAN }).is_err());
        assert!(KeyGen::new(ZIPFIAN_MAX_SPACE + 1, KeyDist::Zipfian { theta: 0.99 }).is_err());
        assert!(KeyGen::new(ZIPFIAN_MAX_SPACE + 1, KeyDist::Uniform).is_ok());
    }

    #[test]
    fn constant_schedule_is_an_exact_grid() {
        let mut rng = SplitMix64::new(3);
        for _ in 0..100 {
            assert_eq!(
                next_interval(&mut rng, 100.0, false),
                Duration::from_millis(10)
            );
        }
    }

    #[test]
    fn poisson_schedule_has_exp_mean_and_is_reproducible() {
        let rate = 1000.0;
        let mut rng = SplitMix64::new(11);
        let mut total = Duration::ZERO;
        let mut zeros = 0usize;
        let n = 100_000;
        for _ in 0..n {
            // Must never panic (u=0 included); a zero interval is legal for
            // an exponential tail (sub-nanosecond rounding).
            let d = next_interval(&mut rng, rate, true);
            if d.is_zero() {
                zeros += 1;
            }
            total += d;
        }
        assert!(zeros <= n / 1000, "too many zero intervals: {zeros}");
        let mean = total.as_secs_f64() / n as f64;
        // E[Exp(λ)] = 1/λ; 100k draws ⇒ tight band.
        assert!((mean - 1.0 / rate).abs() < 0.05 / rate, "mean {mean}");

        let mut r1 = SplitMix64::new(11);
        let mut r2 = SplitMix64::new(11);
        for _ in 0..100 {
            assert_eq!(
                next_interval(&mut r1, rate, true),
                next_interval(&mut r2, rate, true)
            );
        }
    }

    #[test]
    fn timeseries_buckets_by_scheduled_second() {
        let origin = Instant::now();
        let mut ts = TimeSeries::new(origin);
        ts.record(
            BenchOp::Write,
            origin + Duration::from_millis(100),
            Duration::from_micros(500),
            true,
        );
        ts.record(
            BenchOp::Write,
            origin + Duration::from_millis(900),
            Duration::from_micros(1500),
            true,
        );
        ts.record(
            BenchOp::Write,
            origin + Duration::from_millis(1100),
            Duration::from_micros(700),
            true,
        );
        ts.record(
            BenchOp::ReadLin,
            origin + Duration::from_millis(1100),
            Duration::from_micros(900),
            false,
        );
        // Warmup record (before origin) is dropped.
        ts.record(
            BenchOp::Write,
            origin - Duration::from_millis(1),
            Duration::from_micros(1),
            true,
        );

        let jsonl = ts.to_jsonl(Some(30));
        let lines: Vec<&str> = jsonl.lines().collect();
        assert_eq!(lines.len(), 3);
        let l0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(l0["t"], 0);
        assert_eq!(l0["op"], "write");
        assert_eq!(l0["count"], 2);
        assert_eq!(l0["err"], 0);
        assert_eq!(l0["rung"], 0);
        let l1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(l1["t"], 1);
        assert_eq!(l1["count"], 1);
        let l2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(l2["op"], "lin");
        assert_eq!(l2["err"], 1);
    }

    #[test]
    fn put_value_carries_key_and_hits_size() {
        let v = put_value("bench:0000000042", 7, 256);
        assert_eq!(v.len(), 256);
        assert!(v.starts_with(b"v:bench:0000000042:000000000007-"));
        // Header longer than payload: no truncation panic, header wins.
        let v = put_value("bench:0000000042", 7, 4);
        assert!(v.len() > 4);
    }
}
