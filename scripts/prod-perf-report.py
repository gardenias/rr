#!/usr/bin/env python3
"""prod-perf report renderer (rr-prod-perf M3, plan §5.2 item 7).

Reads one archive directory produced by scripts/prod-perf.sh (raw-*.json,
ts-*.jsonl, ledger.txt, meta.txt, docker-stats.json) and writes report.md
into it. Every number comes from the raw files — never hand-edited. Missing
or malformed tier files degrade to "n/a" rows; the ledger marks failed tiers.
"""
import glob
import json
import os
import sys


def load(path):
    with open(path) as f:
        return json.load(f)


def fmt_us(us):
    if us is None:
        return "n/a"
    us = float(us)
    return f"{us / 1000.0:.1f}ms" if us >= 1000 else f"{us:.0f}µs"


def fmt_rate(x):
    if x is None:
        return "n/a"
    x = float(x)
    return f"{x / 1000.0:.1f}k" if x >= 1000 else f"{x:.0f}"


def op_row(run, opname="write"):
    ops = {o["op"]: o for o in run["ops"]}
    o = ops.get(opname)
    if not o:
        return None
    return o


def ladder_rows(doc):
    """One markdown row per rung of a wladder raw file (write op)."""
    rows = []
    for run in doc["runs"]:
        o = op_row(run)
        if not o:
            continue
        rows.append(
            (run["rate"], o["offered"], o["ok"], o["err"], fmt_rate(o["tps"]),
             fmt_us(o["p50_us"]), fmt_us(o["p99_us"]), fmt_us(o["p999_us"]),
             fmt_us(o["lag_p99_us"]))
        )
    return rows


def knee_rate(doc):
    """First rung whose p99 more than doubles the previous rung's → the knee
    is the PREVIOUS rung's offered rate (plan §9 definition)."""
    prev = None
    for run in doc["runs"]:
        o = op_row(run)
        if not o or o["ok"] == 0:
            continue
        if prev and o["p99_us"] > 2 * prev["p99_us"]:
            return prev["rate"]
        prev = {"rate": run["rate"], "p99_us": o["p99_us"]}
    return None


def ladder_md(doc, title):
    out = [f"### {title}", "",
           "| rung ops/s | offered | ok | err | TPS | p50 | p99 | p999 | lag-p99 |",
           "|---|---|---|---|---|---|---|---|---|"]
    for r in ladder_rows(doc):
        out.append("| " + " | ".join(str(x) for x in r) + " |")
    k = knee_rate(doc)
    out.append("")
    out.append(f"knee (p99 > 2× previous rung): **{fmt_rate(k) if k else 'not reached in ladder'}**")
    ss = doc.get("server_side")
    if ss and ss.get("server_p99_us") is not None:
        out.append(f"server-side window: rate={fmt_rate(ss.get('server_request_rate'))}/s "
                   f"p99={fmt_us(ss.get('server_p99_us'))} "
                   f"proposal-p99={fmt_us(ss.get('proposal_p99_us'))} "
                   f"apply-p99={fmt_us(ss.get('apply_p99_us'))} "
                   f"appendRTT-p99={fmt_us(ss.get('append_rtt_p99_us'))} "
                   f"lag-max={ss.get('replication_lag_max_entries')}")
    out.append("")
    return "\n".join(out)


def mix_md(doc, title):
    out = [f"### {title}", "",
           "| op | offered | ok | err | TPS | p50 | p99 | p999 | svc-p99 | lag-p99 |",
           "|---|---|---|---|---|---|---|---|---|---|"]
    for run in doc["runs"]:
        for o in run["ops"]:
            out.append("| " + " | ".join([
                o["op"], str(o["offered"]), str(o["ok"]), str(o["err"]),
                fmt_rate(o["tps"]), fmt_us(o["p50_us"]), fmt_us(o["p99_us"]),
                fmt_us(o["p999_us"]), fmt_us(o["svc_p99_us"]), fmt_us(o["lag_p99_us"]),
            ]) + " |")
    cfg = f"mix={doc['mix']} key-dist={doc['key_dist']} space={doc['key_space']} payload={doc['payload_bytes']}B poisson={doc['poisson']}"
    out += ["", f"config: `{cfg}`", ""]
    return "\n".join(out)


def main():
    out_dir = sys.argv[1]
    L = []
    meta = {}
    meta_path = os.path.join(out_dir, "meta.txt")
    if os.path.exists(meta_path):
        for line in open(meta_path):
            if "=" in line:
                k, v = line.strip().split("=", 1)
                meta[k] = v

    L.append(f"# rr production perf report — {meta.get('date', '?')} (git `{meta.get('git_sha', '?')}`)")
    L.append("")
    L.append("Rendered by `scripts/prod-perf-report.py` from raw JSON/JSONL in this directory; "
             "no number is hand-copied. rr-prod-perf plan: `.sisyphus/plans/rr-prod-perf.md`.")
    L.append("")
    L.append("## Meta")
    L.append("")
    L.append(f"- host: `{meta.get('uname', '?')}` | docker: `{meta.get('docker', '?')}` | smoke: `{meta.get('smoke', '?')}`")
    first_raw = sorted(glob.glob(os.path.join(out_dir, "raw-*.json")))
    if first_raw:
        try:
            m = load(first_raw[0])["meta"]
            L.append(f"- rustc: `{m.get('rustc', '?')}` | tool: rr-perf-bench `{m.get('version', '?')}`")
        except Exception:
            pass
    L.append("- cluster: 3× kv-store (cpuset 2 cores + 2GiB each, see `.env`), durable Raft log, "
             "TLS+mTLS+token secure mode unless a leg says otherwise; load runs in-network")
    L.append("")

    # Tier ledger
    ledger_path = os.path.join(out_dir, "ledger.txt")
    if os.path.exists(ledger_path):
        L.append("## Tier status")
        L.append("")
        L.append("| tier | status | note |")
        L.append("|---|---|---|")
        for line in open(ledger_path):
            parts = line.strip().split(None, 2)
            if len(parts) == 3:
                mark = "✅" if parts[1] == "ok" else "❌"
                L.append(f"| {parts[0]} | {mark} | {parts[2]} |")
        L.append("")

    def tier_file(name):
        p = os.path.join(out_dir, f"raw-{name}.json")
        return load(p) if os.path.exists(p) else None

    # W1 ladders
    w1 = {}
    for payload_file in sorted(glob.glob(os.path.join(out_dir, "raw-w1-*.json"))):
        name = os.path.basename(payload_file)[4:-5]
        try:
            w1[name] = load(payload_file)
        except Exception:
            pass
    if w1:
        L.append("## W1 — write rate ladder (open-loop, knee detection)")
        L.append("")
        for name, doc in w1.items():
            L.append(ladder_md(doc.get("wladder", {}), f"payload {name}B"))

    # W2 mixes
    for tag in sorted(glob.glob(os.path.join(out_dir, "raw-w2-*.json"))):
        doc = load(tag)
        L.append("## W2 — mixed read/write" if "W2 —" not in "\n".join(L[-2:]) else "")
        L.append(mix_md(doc["mix"], os.path.basename(tag)[4:-5]))

    w3_files = sorted(glob.glob(os.path.join(out_dir, "raw-w3-*.json")))
    if w3_files:
        L.append("## W3 — linearizable-read share")
        L.append("")
        for f in w3_files:
            L.append(mix_md(load(f)["mix"], os.path.basename(f)[4:-5]))

    w4a, w4b = tier_file("w4-zipfian"), tier_file("w4-uniform")
    if w4a or w4b:
        L.append("## W4 — hot keys (zipfian vs uniform, same rate)")
        L.append("")
        if w4a:
            L.append(mix_md(w4a["mix"], "zipfian(0.99)"))
        if w4b:
            L.append(mix_md(w4b["mix"], "uniform"))
        if w4a and w4b:
            za = op_row(w4a["mix"]["runs"][0])
            ua = op_row(w4b["mix"]["runs"][0])
            if za and ua and ua["p99_us"]:
                L.append(f"hot-key tax: zipfian p99 / uniform p99 = "
                         f"**{za['p99_us'] / ua['p99_us']:.2f}×**")
                L.append("")

    w5_files = sorted(glob.glob(os.path.join(out_dir, "raw-w5-*.json")))
    if w5_files:
        L.append("## W5 — payload scan")
        L.append("")
        L.append("| payload | TPS | p50 | p99 | p999 |")
        L.append("|---|---|---|---|---|")
        for f in w5_files:
            doc = load(f)["mix"]
            o = op_row(doc["runs"][0])
            if o:
                L.append(f"| {doc['payload_bytes']}B | {fmt_rate(o['tps'])} | "
                         f"{fmt_us(o['p50_us'])} | {fmt_us(o['p99_us'])} | {fmt_us(o['p999_us'])} |")
        L.append("")

    w6 = tier_file("w6")
    if w6:
        L.append("## W6 — Kafka ingest path")
        L.append("")
        L.append(f"- window: {w6.get('window_secs', '?')}s | ingestor batch commit rate: "
                 f"**{fmt_rate(w6.get('ingestor_batch_commit_rate'))}/s** | "
                 f"batch proposal p99: {fmt_us(w6.get('ingestor_batch_p99_us'))}")
        L.append("- producer log: `producer-w6.log`")
        L.append("")

    w7 = tier_file("w7")
    if w7:
        L.append("## W7 — snapshot pressure")
        L.append("")
        L.append(ladder_md(w7.get("wladder", {}), "snapshot-logs-since-last lowered"))
        L.append("Compare the time series against W1 (`ts-w7.jsonl` vs `ts-w1-128.jsonl`): "
                 "snapshot spikes show as 1s-bucket p99 jumps.")
        L.append("")

    w8 = tier_file("w8")
    if w8:
        L.append("## W8 — security tax (A/B)")
        L.append("")
        L.append(ladder_md(w8.get("wladder", {}), "INSECURE control leg (plaintext, no mTLS/token)"))
        ref = w1.get("128")
        if ref:
            pairs = []
            secure_runs = {r["rate"]: op_row(r) for r in ref["wladder"]["runs"]}
            for r in w8["wladder"]["runs"]:
                o = op_row(r)
                s = secure_runs.get(r["rate"])
                if o and s and s["tps"] and o["p99_us"] and s["p99_us"]:
                    pairs.append((r["rate"], o["tps"] / s["tps"],
                                  (o["p99_us"] - s["p99_us"]) / s["p99_us"]))
            if pairs:
                L.append("| rung ops/s | insecure/secure TPS | p99 delta |")
                L.append("|---|---|---|")
                for rate, ratio, delta in pairs:
                    L.append(f"| {fmt_rate(rate)} | {ratio:.2f}× | {delta:+.0%} |")
                L.append("")

    w9 = tier_file("w9")
    if w9 and w9.get("soak"):
        s = w9["soak"]
        L.append("## W9 — production soak")
        L.append("")
        L.append(f"- duration: {s['duration_secs']}s @ {s['rate_per_sec']}/s | sent={s['sent']} "
                 f"acked={s['acked']} failed={s['failed']} | reads ok/failed: "
                 f"{s['reads_ok']}/{s['reads_failed']}")
        if s.get("oracle"):
            o = s["oracle"]
            L.append(f"- oracle: **{o['verified']}/{o['checked']} verified** "
                     f"(sampled from {o['sampled_from']} ack records)")
        L.append("- a `TransferLeader` was issued mid-window (see bench-w9.log)")
        L.append("")

    w11 = tier_file("w11")
    if w11:
        L.append("## W11 — durable raft-log cost (A/B, ADR A-0010 re-judgement)")
        L.append("")
        L.append(ladder_md(w11.get("wladder", {}), "mem-log control leg (pre-M0 shape)"))
        ref = w1.get("128")
        if ref:
            pairs = []
            rocks_runs = {r["rate"]: op_row(r) for r in ref["wladder"]["runs"]}
            for r in w11["wladder"]["runs"]:
                o = op_row(r)
                s = rocks_runs.get(r["rate"])
                if o and s and o["tps"] and s["tps"] and o["p99_us"] and s["p99_us"]:
                    pairs.append((r["rate"], o["tps"] / s["tps"],
                                  (o["p99_us"] - s["p99_us"]) / s["p99_us"]))
            if pairs:
                L.append("| rung ops/s | mem-log/rocks-log TPS | p99 delta |")
                L.append("|---|---|---|")
                for rate, ratio, delta in pairs:
                    L.append(f"| {fmt_rate(rate)} | {ratio:.2f}× | {delta:+.0%} |")
                L.append("")
        L.append("> Reading note: the control leg also switches the KV state machine to memory"
                 " (mem-SM confound; R-7 measured mem-vs-rocks SM at <2%). The raft-log fsync"
                 " path is the dominant term being isolated.")
        L.append("")

    # Criteria table (plan §9) — computed where data exists, else marked.
    L.append("## Criteria (plan §9)")
    L.append("")
    L.append("| metric | value |")
    L.append("|---|---|")
    k = knee_rate(w1["128"]["wladder"]) if w1.get("128") else None
    L.append(f"| knee TPS (write, 128B) | {fmt_rate(k) if k else 'not reached / not run'} |")
    L.append("| security tax (W8) | see W8 table |")
    L.append("| raft-log persistence cost (W11) | see W11 table — feeds the ADR A-0010 re-judgement |")
    L.append("")

    with open(os.path.join(out_dir, "report.md"), "w") as f:
        f.write("\n".join(L))
    print(os.path.join(out_dir, "report.md"))


if __name__ == "__main__":
    main()
