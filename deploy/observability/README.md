# rr — Prometheus & Grafana

Drop-in observability artifacts for an rr cluster.

## Files

| File | Purpose |
|---|---|
| `prometheus/rr-recording-rules.yml` | Pre-computed aggregations (max lag, leader count, p99 latency, idempotency rates) |
| `prometheus/rr-alerts.yml` | Alert rules (insecure mode, idempotency collisions & cache, leader flap, no leader, high replication lag, ingestor seed/offset failures, WAL flush errors, framing decode failures) |
| `grafana/rr-cluster-overview.json` | 20-panel dashboard in 4 row groups (throughput / latency / replication / cluster health): applied-entries TPS, per-op client latency, proposal latency, AppendEntries RTT, per-follower lag, leaders, idempotency rates & cache, learner promotion outcomes, ingestor health, framing decode failures. (No auth-rejects panel: auth failures are emitted as `rr::audit` tracing events, not metrics.) |

## Prometheus integration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: rr
    static_configs:
      - targets: ['rr-1:9100', 'rr-2:9100', 'rr-3:9100']

rule_files:
  - /etc/prometheus/rr/recording-rules.yml
  - /etc/prometheus/rr/alerts.yml
```

`NodeConfig.metrics_port` defaults to `None` (metrics endpoint disabled) — there
is no built-in default port. `9100` is the convention used by every manifest in
this repo (lab/perf compose, k8s StatefulSet, Helm values); set the targets to
whatever port you configured.

## Grafana import

1. Grafana → Dashboards → Import.
2. Upload `grafana/rr-cluster-overview.json` (or paste).
3. Pick your Prometheus data source when prompted.

The dashboard uses the recording rules above, so import them into Prometheus first; otherwise some panels render empty until enough raw data accumulates.

## Panels

Existing `rr-cluster-overview.json` panels are preserved. Added panels:

- `Replication lag by follower (leader-only)` — heatmap of `rr_raft_replication_lag` per `(group_id, follower_node_id)`, explicitly predicated on `rr_raft_is_leader==1`.
- `Idempotency rates (replay/collision/expired)` — 5m rates for replay, collision, and expired idempotency outcomes.
- `Idempotency cache usage (%)` — `rr_idempotency_cache_size` normalized by `${IDEMPOTENCY_CACHE_MAX}` with 50%/90% warning thresholds.
- `Ingestor health event rates` — stacked rates for step-down drops, seed failures, and seek below/above watermark events.
- `Framing decode failure rate by kind` — stacked decode failure rates grouped by `kind` label.
- `Learner promotion outcomes (1h)` — bar gauge for successful vs failed learner promotions over 1h.

## Recording rules

Existing recording rules are preserved. Added rules:

- `cluster:rr_replication_lag:max_per_group` — max follower lag per group, filtered with `rr_raft_is_leader == 1`.
- `rr:idempotency_replay_rate_5m_raw` — raw 5m replay rate series for per-instance drill-down.
- `rr:ingestor_offset_skew_total` — per-group combined 5m rate of seek-above + seek-below watermark events.
- `rr:framing_decode_failure_rate_5m` — 5m decode failure rate aggregated by `kind`.

## Alert thresholds

The defaults in `rr-alerts.yml` are tuned for a healthy 3-5 node cluster on a stable network:

- `RrLeaderFlapping`: > 0.5 elections/min for 2m → warning.
- `RrReplicationLagHigh`: max follower lag > 1000 entries for 5m → warning.
- `RrNoLeader`: no node has `is_leader=1` for 30s → critical.
- `RrInsecureModeInProduction`: `allow_insecure=1` ever → critical.
- `RrIdempotencyCollisionsObserved`: any collision rate > 0 for 5m → warning (indicates uncoordinated request_id allocation).
- `RrIngestorSeedFailures`: any seed failure → critical (supervisor exited; ingestor offline).

Adjust to your SLOs. The recording-rules + alerts split lets you tune thresholds without rewriting the queries.

## Alerts

Existing alert rules are preserved. Added alerts:

- `RrIdempotencyCacheNearFull` (`warning`) — idempotency cache stayed above 45k entries for 10m; near eviction pressure.
- `RrIngestorOffsetDrift` (`warning`) — ingestor repeatedly seeks outside Kafka watermark range for 10m.
- `RrFramingDecodeFailures` (`critical`) — non-zero framing decode failure rate for 5m; possible wire-format incompatibility or corruption.

## See also

- [`docs/docs/reference/metrics-reference.md`](../../docs/docs/reference/metrics-reference.md) — full per-metric reference.
- [`docs/docs/operations/runbooks/`](../../docs/docs/operations/runbooks/) — runbooks for triggered alerts.

Last updated: 2026-09-23
