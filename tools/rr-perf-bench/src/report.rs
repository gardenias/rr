use crate::metrics::MetricsSnapshot;
use crate::openloop::ScenarioReport;
use crate::workload::ReadKind;

fn fmt_us(us: u64) -> String {
    if us >= 1_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{us}µs")
    }
}

fn fmt_tps(tps: f64) -> String {
    if tps >= 1_000.0 {
        format!("{:.1}k", tps / 1000.0)
    } else {
        format!("{tps:.0}")
    }
}

pub fn print_s1(snap: &MetricsSnapshot, payload_bytes: usize) {
    println!();
    println!("── S1: Serial Write (1 in-flight) ─────────────────────────────────");
    println!(
        "  payload={payload_bytes}B  duration={:.1}s  ok={}  err={}",
        snap.elapsed_secs, snap.ok, snap.err
    );
    println!(
        "  TPS={tps:<10} p50={p50:<10} p95={p95:<10} p99={p99:<10} p999={p999}",
        tps = fmt_tps(snap.tps),
        p50 = fmt_us(snap.p50_us),
        p95 = fmt_us(snap.p95_us),
        p99 = fmt_us(snap.p99_us),
        p999 = fmt_us(snap.p999_us),
    );
}

pub fn print_s2_header(payload_bytes: usize, duration_secs: u64) {
    println!();
    println!("── S2: Concurrent Write Sweep ──────────────────────────────────────");
    println!("  payload={payload_bytes}B  window={duration_secs}s (excl. warmup)");
    println!(
        "  {conc:<12} {tps:<12} {p50:<12} {p95:<12} {p99:<12} {p999:<12} Errors",
        conc = "Concurrency",
        tps = "TPS",
        p50 = "p50",
        p95 = "p95",
        p99 = "p99",
        p999 = "p999",
    );
    println!("  {}", "─".repeat(80));
}

pub fn print_s2_row(concurrency: usize, snap: &MetricsSnapshot) {
    println!(
        "  {conc:<12} {tps:<12} {p50:<12} {p95:<12} {p99:<12} {p999:<12} {err}",
        conc = concurrency,
        tps = fmt_tps(snap.tps),
        p50 = fmt_us(snap.p50_us),
        p95 = fmt_us(snap.p95_us),
        p99 = fmt_us(snap.p99_us),
        p999 = fmt_us(snap.p999_us),
        err = snap.err,
    );
}

pub fn print_s3_header(payload_bytes: usize, duration_secs: u64) {
    println!();
    println!("── S3: Read-Path Sweep (local vs linearizable) ─────────────────────");
    println!("  payload={payload_bytes}B  window={duration_secs}s (excl. warmup)");
    println!(
        "  {kind:<14} {conc:<12} {tps:<12} {p50:<12} {p95:<12} {p99:<12} {p999:<12} Errors",
        kind = "ReadKind",
        conc = "Concurrency",
        tps = "TPS",
        p50 = "p50",
        p95 = "p95",
        p99 = "p99",
        p999 = "p999",
    );
    println!("  {}", "─".repeat(95));
}

pub fn print_s3_row(kind: ReadKind, concurrency: usize, snap: &MetricsSnapshot) {
    println!(
        "  {kind:<14} {conc:<12} {tps:<12} {p50:<12} {p95:<12} {p99:<12} {p999:<12} {err}",
        kind = kind.as_str(),
        conc = concurrency,
        tps = fmt_tps(snap.tps),
        p50 = fmt_us(snap.p50_us),
        p95 = fmt_us(snap.p95_us),
        p99 = fmt_us(snap.p99_us),
        p999 = fmt_us(snap.p999_us),
        err = snap.err,
    );
}

/// Open-loop report (mix = one rung, wladder = N rungs). Latency columns are
/// the coordinated-omission-corrected ones (scheduled → completion);
/// `svc-p99`/`lag-p99` split service time from scheduler debt so a driver
/// that cannot keep up with its own plan is visible in the output.
pub fn print_openloop(title: &str, report: &ScenarioReport) {
    println!();
    println!("── {title} ────────────────────────────────────────────────────");
    println!(
        "  mix={}  key-dist={} space={}  poisson={}  payload={}B  client-id={}  seed={:#x}",
        report.mix,
        report.key_dist,
        report.key_space,
        report.poisson,
        report.payload_bytes,
        report.client_id,
        report.seed,
    );
    for run in &report.runs {
        if report.runs.len() > 1 {
            println!(
                "  rung {}: {} ops/s  window={}s",
                run.rung, run.rate, run.window_secs
            );
        } else {
            println!("  rate={} ops/s  window={}s", run.rate, run.window_secs);
        }
        println!(
            "  {op:<8} {off:<10} {ok:<10} {err:<8} {inc:<8} {tps:<9} {p50:<10} {p99:<10} {p999:<10} {max:<10} {svc:<10} {lag:<10}",
            op = "op",
            off = "offered",
            ok = "ok",
            err = "err",
            inc = "incompl",
            tps = "TPS",
            p50 = "p50",
            p99 = "p99",
            p999 = "p999",
            max = "max",
            svc = "svc-p99",
            lag = "lag-p99",
        );
        println!("  {}", "─".repeat(116));
        for op in &run.ops {
            println!(
                "  {op:<8} {off:<10} {ok:<10} {err:<8} {inc:<8} {tps:<9} {p50:<10} {p99:<10} {p999:<10} {max:<10} {svc:<10} {lag:<10}",
                op = op.op,
                off = op.offered,
                ok = op.ok,
                err = op.err,
                inc = op.incomplete,
                tps = fmt_tps(op.tps),
                p50 = fmt_us(op.p50_us),
                p99 = fmt_us(op.p99_us),
                p999 = fmt_us(op.p999_us),
                max = fmt_us(op.max_us),
                svc = fmt_us(op.svc_p99_us),
                lag = fmt_us(op.lag_p99_us),
            );
        }
    }
}
