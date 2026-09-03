use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rr_client::RrClient;
use tokio::sync::{mpsc, Semaphore};

use crate::metrics::{Metrics, MetricsSnapshot};

pub type SharedClient = Arc<RrClient>;

const PARTITION_ID: u64 = 1;

/// Which client operation the reactive pipeline fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    Write,
    ReadLocal,
    ReadLinearizable,
}

/// Read path under test in S3.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadKind {
    Local,
    Linearizable,
}

impl ReadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ReadKind::Local => "local",
            ReadKind::Linearizable => "linearizable",
        }
    }
}

/// S1: serial blocking writes — one in-flight at a time.
///
/// Exercises the **blocking** SDK interface (`send_request_blocking`).
/// The OS thread is occupied for the entire round-trip; this measures
/// the minimum achievable per-request latency with zero concurrency.
pub async fn run_s1_serial(
    client: SharedClient,
    payload_size: usize,
    duration: Duration,
    warmup: Duration,
) -> Metrics {
    let payload = vec![0u8; payload_size];

    let warmup_end = Instant::now() + warmup;
    while Instant::now() < warmup_end {
        let _ = client.send_request_blocking(PARTITION_ID, payload.clone());
    }

    let m = Metrics::new();
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        let t0 = Instant::now();
        let ok = client
            .send_request_blocking(PARTITION_ID, payload.clone())
            .is_ok();
        m.record(t0.elapsed(), ok);
    }
    m
}

/// S2: reactive pipeline sweep.
///
/// Exercises the **reactive** SDK interface (`send_request_reactive`).
/// For each `pipeline_depth` in `levels`, keeps exactly that many requests
/// simultaneously in-flight. Responses drive new requests via callbacks —
/// the firing task never blocks on network I/O.
pub async fn run_s2_pipeline_sweep(
    client: SharedClient,
    payload_size: usize,
    levels: &[usize],
    duration: Duration,
    warmup: Duration,
) -> Vec<(usize, MetricsSnapshot)> {
    let mut results = Vec::new();

    for &depth in levels {
        let snap = run_reactive_pipeline(
            Arc::clone(&client),
            payload_size,
            depth,
            duration,
            warmup,
            OpKind::Write,
        )
        .await;
        results.push((depth, snap));

        // Cool-down between levels to let the cluster stabilize
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    results
}

/// S3: read-path sweep — local (stale) reads vs linearizable reads.
///
/// Exercises the **reactive** read interfaces (`read_reactive` and
/// `read_linearizable_reactive`).  Both read kinds run over the same
/// concurrency levels; rows come back grouped by kind (all `local`
/// levels first, then `linearizable`).
pub async fn run_s3_read_sweep(
    client: SharedClient,
    payload_size: usize,
    levels: &[usize],
    duration: Duration,
    warmup: Duration,
) -> Vec<(ReadKind, usize, MetricsSnapshot)> {
    let mut results = Vec::new();

    for (kind, op) in [
        (ReadKind::Local, OpKind::ReadLocal),
        (ReadKind::Linearizable, OpKind::ReadLinearizable),
    ] {
        for &depth in levels {
            let snap = run_reactive_pipeline(
                Arc::clone(&client),
                payload_size,
                depth,
                duration,
                warmup,
                op,
            )
            .await;
            results.push((kind, depth, snap));

            // Cool-down between levels to let the cluster stabilize
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    results
}

/// Core reactive pipeline: `pipeline_depth` requests always in-flight via callbacks.
///
/// Uses a [`Semaphore`] to cap concurrency and the reactive SDK call for `op`
/// so the firing loop awaits only the semaphore (not the network).  Each
/// callback drops its permit upon completion, unblocking the next fire
/// immediately.
async fn run_reactive_pipeline(
    client: SharedClient,
    payload_size: usize,
    pipeline_depth: usize,
    duration: Duration,
    warmup: Duration,
    op: OpKind,
) -> MetricsSnapshot {
    let payload = Arc::new(vec![0u8; payload_size]);
    let measure_start = Instant::now() + warmup;
    let measure_end = measure_start + duration;
    let m = Metrics::new();

    let sem = Arc::new(Semaphore::new(pipeline_depth));
    let (tx, mut rx) = mpsc::unbounded_channel::<(Instant, Duration, bool)>();
    let in_flight = Arc::new(AtomicUsize::new(0));

    // ── Firing loop ────────────────────────────────────────────────────────
    // Awaits only the semaphore — never blocks on gRPC I/O.
    let fire_loop = {
        let sem = Arc::clone(&sem);
        let in_flight = Arc::clone(&in_flight);
        let tx = tx.clone();
        let client = Arc::clone(&client);
        let payload = Arc::clone(&payload);
        async move {
            loop {
                let permit = Arc::clone(&sem)
                    .acquire_owned()
                    .await
                    .expect("semaphore closed");

                if Instant::now() >= measure_end {
                    break; // permit dropped here
                }

                let t0 = Instant::now();
                let tx_cb = tx.clone();
                let inf = Arc::clone(&in_flight);
                inf.fetch_add(1, Ordering::Relaxed);

                // Reactive: returns immediately; callback fires on a tokio
                // worker thread when the cluster responds.
                fire_op(&client, op, (*payload).clone(), move |ok| {
                    drop(permit); // release slot → unblocks firing loop
                    inf.fetch_sub(1, Ordering::Relaxed);
                    let _ = tx_cb.send((t0, t0.elapsed(), ok));
                });
            }
        }
    };

    // Drop our tx clone so the channel closes once all callbacks complete.
    drop(tx);

    // ── Recording loop ────────────────────────────────────────────────────
    let record_loop = async {
        while let Some((sent_at, elapsed, ok)) = rx.recv().await {
            if sent_at >= measure_start && sent_at < measure_end {
                m.record(elapsed, ok);
            }
        }
    };

    tokio::join!(fire_loop, record_loop);
    m.snapshot()
}

/// Sample-log the first few failures of the process at WARN (the SDK read
/// paths don't log retry failures themselves) — without this a benchmark run
/// can report thousands of errors with zero diagnosable detail.
fn log_op_error<E: std::fmt::Display>(op: OpKind, e: &E) {
    static LOGGED: AtomicUsize = AtomicUsize::new(0);
    let n = LOGGED.fetch_add(1, Ordering::Relaxed);
    if n < 5 {
        tracing::warn!(op = ?op, error = %e, "bench op failed (sampled)");
    }
}

/// Fire one reactive `op` against the cluster; `callback` receives the
/// success flag on a tokio worker thread when the cluster responds.
fn fire_op(
    client: &RrClient,
    op: OpKind,
    payload: Vec<u8>,
    callback: impl FnOnce(bool) + Send + 'static,
) {
    match op {
        OpKind::Write => client.send_request_reactive(PARTITION_ID, payload, move |r| {
            if let Err(e) = &r {
                log_op_error(op, e);
            }
            callback(r.is_ok())
        }),
        OpKind::ReadLocal => client.read_reactive(PARTITION_ID, payload, move |r| {
            if let Err(e) = &r {
                log_op_error(op, e);
            }
            callback(r.is_ok())
        }),
        OpKind::ReadLinearizable => {
            client.read_linearizable_reactive(PARTITION_ID, payload, move |r| {
                if let Err(e) = &r {
                    log_op_error(op, e);
                }
                callback(r.is_ok())
            })
        }
    }
}
