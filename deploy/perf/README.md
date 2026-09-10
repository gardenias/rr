# rr perf-lab (`deploy/perf/`)

生产形态性能基准集群 / production-shape performance benchmark cluster —
the secure twin of `deploy/lab/`. Plan: `.sisyphus/plans/rr-prod-perf.md`
(M1). **Local perf lab only** — self-signed certs, local `.env` tokens.

| 维度 | perf-lab (this stack) | lab (`deploy/lab/`, frozen) |
|---|---|---|
| 用途 | 生产形态性能包线 (W1–W11) | 开发/可靠性实验 (R-8 soak) |
| Raft log | **durable** (RocksDB, `--data-dir`, ADR A-0016) | in-memory |
| gRPC 安全 | **TLS + mTLS + admin/client bearer tokens**(G1–G7 全开) | plaintext + `allow_insecure` |
| 资源隔离 | 每节点 cpuset 2 核 + 2Gi(可 env 覆盖) | 无 |
| 负载位置 | **网内** bench 容器(cpuset 独占) | 网内 bench profile |

## Quickstart

```bash
# 1. Tokens (gitignored .env) + certs (kv-N SANs, gitignored certs/)
cp deploy/perf/.env.example deploy/perf/.env   # edit the two tokens
deploy/perf/gen-certs.sh

# 2. Build + bring up (kv-1/2/3 + Prometheus + Grafana)
docker compose -f deploy/perf/docker-compose.yml up -d --build

# 3. Security-gate self-check (plaintext/mTLS negative + positive controls)
deploy/perf/selfcheck.sh           # add --keep to leave the stack running
```

Grafana: <http://127.0.0.1:3100> (admin/admin by default). Prometheus:
<http://127.0.0.1:9190>. **No node ports are published to the host** — load
and probes run inside the `rrperf` network (leader redirects advertise the
container hostnames; host-side clients cannot follow them).

## Security model

- kv-store runs with `--tls-cert/--tls-key/--tls-ca` (+ `RR_ADMIN_TOKEN` /
  `RR_CLIENT_TOKEN` env): TLS on all gRPC endpoints, **mandatory client
  certificates** (mTLS is the only RaftService peer-identity check, gate G2),
  bearer tokens enforced on Admin/Client services. `RrNode` runs the full
  G1–G7 gate set — there is no `allow_insecure` anywhere in this stack.
- Inter-node Raft traffic (AppendEntries/Vote/snapshot) rides the same mTLS
  via `ChannelPool::with_tls`; peer identities are the roster hostnames
  `kv-1/2/3`, matched by the cert SANs (`gen-certs.sh`).
- The HTTP observability endpoint (`:9100` /health /ready /metrics) stays
  plaintext **inside** the network — Prometheus scrapes it in-network.
- Negative/positive controls are executable: `selfcheck.sh` fails if a
  plaintext or certificate-less client is served, or if a valid mTLS client
  is rejected.

## Resource isolation

Each kv node: `cpuset` (default 2 dedicated cores: kv-1=0-1, kv-2=2-3,
kv-3=4-5) + `mem_limit: 2g` (aligned with the Helm limits); the bench
container gets its own cpuset (default 6-7). Override via `.env`
(`KV1_CPUSET` … `BENCH_CPUSET`) on hosts with <8 cores — **numbers are only
comparable within the same quota matrix**; record the actual quotas plus
hardware/OS in every report's meta section.

macOS note: cpusets apply to the Docker Desktop VM, not the host, and its
network/filesystem layers distort absolute numbers (plan §8). The official
baselines are collected on a Linux host; macOS numbers are indicative only.

## Layout

```
deploy/perf/
  docker-compose.yml     # the stack (kv ×3 + prom + grafana + bench/probe profiles)
  Dockerfile             # rr-perf:dev — kv-store --features rdkafka,rocks + tools
  gen-certs.sh           # CA + nodeN/client certs with kv-N SANs (certs/, gitignored)
  selfcheck.sh           # G1–G7 executable self-check (see above)
  prometheus.yml         # scrape config (recording rules/alerts from deploy/observability)
  grafana/provisioning/  # datasource + dashboard provider
  .env.example           # token + cpuset knobs (copy to .env, gitignored)
```

Run artifacts land in `docs/perf/prod/<date>-<sha>/` (raw JSON + timeseries +
Prom snapshot + report.md) once M3 (`scripts/prod-perf.sh`) lands. W6 (Kafka
ingest profile) and W10 (Toxiproxy WAN profile) are wired in M3 as well —
the M1 stack is intentionally just the secure cluster + observability.

## Debug snippets

```bash
docker compose -f deploy/perf/docker-compose.yml logs -f kv-2
docker compose -f deploy/perf/docker-compose.yml exec kv-1 \
    curl -fsS http://127.0.0.1:9100/metrics | grep rr_raft_is_leader
docker compose -f deploy/perf/docker-compose.yml --profile selfcheck run --rm probe -c \
    'curl -sS --cacert /etc/rr/certs/ca.pem --cert /etc/rr/certs/client.pem \
     --key /etc/rr/certs/client.key -o /dev/null -w "%{http_code}\n" https://kv-2:51001/'
```
