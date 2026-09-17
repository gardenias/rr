# rr production perf report — 20260913-100629 (git `65fc548`)

Rendered by `scripts/prod-perf-report.py` from raw JSON/JSONL in this directory; no number is hand-copied. rr-prod-perf plan: `.sisyphus/plans/rr-prod-perf.md`.

## Meta

- host: `Darwin 25.6.0 arm64` | docker: `29.4.0` | smoke: `0`
- rustc: `unknown` | tool: rr-perf-bench `0.2.0`
- cluster: 3× kv-store (cpuset 2 cores + 2GiB each, see `.env`), durable Raft log, TLS+mTLS+token secure mode unless a leg says otherwise; load runs in-network

## Tier status

| tier | status | note |
|---|---|---|
| w9 | ✅ | soak 7200s @ 2000/s + TransferLeader |

## W9 — production soak

- duration: 7200s @ 2000/s | sent=908258 acked=908258 failed=0 | reads ok/failed: 719/0
- a `TransferLeader` was issued mid-window (see bench-w9.log)

## Criteria (plan §9)

| metric | value |
|---|---|
| knee TPS (write, 128B) | not reached / not run |
| security tax (W8) | see W8 table |
| raft-log persistence cost (W11) | see W11 table — feeds the ADR A-0010 re-judgement |
