# deploy/compose — 3-node rr-server dev cluster

> **仅本地开发 / LOCAL DEV ONLY** — no TLS, no auth tokens; every node runs with
> `RR_ALLOW_INSECURE=true` (set in `docker-compose.yml`, deliberately NOT in the
> TOML configs). 禁止用于任何真实部署 / never reuse outside local development.
> Hardening path: `docs/docs/deployment/upgrading.md` (TLS/mTLS + `[auth]` tokens).

This is the minimal stock-cluster template referenced by `CHANGELOG.md` and
`docs/docs/intro.md`: **3 × rr-server (built-in echo state machine) + 1 × etcd**.
etcd backs managed genesis formation (`[cluster.formation]` in `config/node*.toml`):
the nodes CAS a lifecycle record Forming → Active and initialize one shared Raft
group (group 1, all three nodes as voters). No Kafka, no kv-store, no Grafana —
for the full local lab see `deploy/lab/`.

## 组件 / Components

| 服务 / Service | 镜像 / Image | 端口（host，仅 loopback） / Ports | 说明 / Notes |
|---|---|---|---|
| rr-1/2/3 | `rr-compose:dev` | `127.0.0.1:5200N` admin；`127.0.0.1:910N` HTTP | raft `rr-N:51001` 仅网内 |
| etcd | `quay.io/coreos/etcd:v3.5.13` | （不发布 / not published） | formation lifecycle store |

`rr-compose:dev` 由仓库根 Dockerfile 构建（rr-server + rr-admin，rocks 后端）/ built from the
repo-root Dockerfile (rr-server + rr-admin, rocks backend).

Host ports are loopback-only on purpose: with the insecure opt-out the admin API
is unauthenticated, so it must not land on LAN interfaces.

## 快速开始 / Quickstart

```bash
# 启动（首次构建需数分钟：release 编译 + RocksDB）/ start (first build takes minutes):
docker compose -f deploy/compose/docker-compose.yml up -d --build

# 验证 formation：/ready 仅在组形成且观察到 leader 后返回 200
curl -fsS http://127.0.0.1:9101/ready   # rr-1（rr-2/rr-3 用 9102/9103）

# 通过镜像内的 rr-admin 查询 admin API（确认 leader 存在）
docker exec rr-1 rr-admin --addr 127.0.0.1:52001 node status
docker exec rr-1 rr-admin --addr 127.0.0.1:52001 group status --group 1

# 或使用本机构建的 rr-admin / or a host-built rr-admin:
#   ./target/debug/rr-admin --addr 127.0.0.1:52001 group status --group 1

# 看日志 / logs:
docker compose -f deploy/compose/docker-compose.yml logs -f rr-1

# 拆除（-v 清空节点与 etcd 数据）/ tear down (-v wipes node + etcd data):
docker compose -f deploy/compose/docker-compose.yml down -v
```

Node and etcd data live in named volumes, so plain `down` / `up` keeps the formed
cluster. Use `down -v` for a fresh genesis — and after any roster change in
`config/`: the seed hash is pinned in etcd, so a changed `[cluster]` section
fails closed until the volumes are wiped.

## 加固 / Hardening

Before any real deployment: drop `RR_ALLOW_INSECURE`, add `[tls]` (+ mTLS) and
`[auth] admin_token` to the configs, and follow the rolling-enablement procedure
in `docs/docs/deployment/upgrading.md`. The TOML files here are intentionally
production-shaped so that diff is the only change needed.
