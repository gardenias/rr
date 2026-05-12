use crate::metrics::MetricsSnapshot;

fn fmt_us(us: u64) -> String {
    if us >= 1_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{}µs", us)
    }
}

fn fmt_tps(tps: f64) -> String {
    if tps >= 1_000.0 {
        format!("{:.1}k", tps / 1000.0)
    } else {
        format!("{:.0}", tps)
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
        tps  = fmt_tps(snap.tps),
        p50  = fmt_us(snap.p50_us),
        p95  = fmt_us(snap.p95_us),
        p99  = fmt_us(snap.p99_us),
        p999 = fmt_us(snap.p999_us),
    );
}

pub fn print_s2_header(payload_bytes: usize, duration_secs: u64) {
    println!();
    println!("── S2: Concurrent Write Sweep ──────────────────────────────────────");
    println!(
        "  payload={payload_bytes}B  window={duration_secs}s (excl. warmup)"
    );
    println!(
        "  {conc:<12} {tps:<12} {p50:<12} {p95:<12} {p99:<12} {p999:<12} {err}",
        conc = "Concurrency",
        tps  = "TPS",
        p50  = "p50",
        p95  = "p95",
        p99  = "p99",
        p999 = "p999",
        err  = "Errors",
    );
    println!("  {}", "─".repeat(80));
}

pub fn print_s2_row(concurrency: usize, snap: &MetricsSnapshot) {
    println!(
        "  {conc:<12} {tps:<12} {p50:<12} {p95:<12} {p99:<12} {p999:<12} {err}",
        conc = concurrency,
        tps  = fmt_tps(snap.tps),
        p50  = fmt_us(snap.p50_us),
        p95  = fmt_us(snap.p95_us),
        p99  = fmt_us(snap.p99_us),
        p999 = fmt_us(snap.p999_us),
        err  = snap.err,
    );
}
