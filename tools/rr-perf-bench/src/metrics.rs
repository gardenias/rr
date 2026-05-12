use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

/// Shared, thread-safe metrics collector.
#[derive(Clone)]
pub struct Metrics(Arc<Inner>);

struct Inner {
    hist: Mutex<Histogram<u64>>,
    ok: AtomicU64,
    err: AtomicU64,
    start: Instant,
}

impl Metrics {
    pub fn new() -> Self {
        // 1µs – 30s range, 3 significant figures
        let hist = Histogram::<u64>::new_with_bounds(1_000, 30_000_000_000, 3)
            .expect("valid histogram bounds");
        Metrics(Arc::new(Inner {
            hist: Mutex::new(hist),
            ok: AtomicU64::new(0),
            err: AtomicU64::new(0),
            start: Instant::now(),
        }))
    }

    /// Record one completed request.  `latency` is measured from just before
    /// the async call to just after.
    pub fn record(&self, latency: Duration, success: bool) {
        let ns = latency.as_nanos() as u64;
        if success {
            self.0.ok.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut h) = self.0.hist.lock() {
                let _ = h.record(ns);
            }
        } else {
            self.0.err.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn ok_count(&self) -> u64 {
        self.0.ok.load(Ordering::Relaxed)
    }

    pub fn err_count(&self) -> u64 {
        self.0.err.load(Ordering::Relaxed)
    }

    pub fn elapsed(&self) -> Duration {
        self.0.start.elapsed()
    }

    pub fn tps(&self) -> f64 {
        let secs = self.elapsed().as_secs_f64();
        if secs < 0.001 {
            return 0.0;
        }
        self.ok_count() as f64 / secs
    }

    /// Return the value at the given percentile, in microseconds.
    pub fn percentile_us(&self, p: f64) -> u64 {
        self.0
            .hist
            .lock()
            .map(|h| h.value_at_percentile(p) / 1_000)
            .unwrap_or(0)
    }

    /// Snapshot of key percentiles (in µs).
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            ok: self.ok_count(),
            err: self.err_count(),
            elapsed_secs: self.elapsed().as_secs_f64(),
            tps: self.tps(),
            p50_us: self.percentile_us(50.0),
            p95_us: self.percentile_us(95.0),
            p99_us: self.percentile_us(99.0),
            p999_us: self.percentile_us(99.9),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub ok: u64,
    pub err: u64,
    pub elapsed_secs: f64,
    pub tps: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
}
