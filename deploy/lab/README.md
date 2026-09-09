# rr Local Lab (OrbStack / Docker)

> **仅本地实验环境 / LOCAL LAB ONLY** — 本目录所有编排均无 TLS、无认证，
> kv-store 以 allow_insecure 运行。禁止用于任何真实部署。
> No TLS, no auth, allow_insecure — never reuse outside a local lab.

一个可复现的本地实验环境：**3 节点 kv-store 集群（真实 KV 语义）+ Redpanda（Kafka 协议）命令输入 + Prometheus/Grafana 可观测性**，用于压力、混沌与稳定性测试。

## 组件 / Components

| 服务 / Service | 镜像 / Image | 端口（host） / Ports | 说明 / Notes |
|---|---|---|---|
| kv-1/2/3 | `rr-lab:dev`（本目录 Dockerfile 构建） | 5110N (client+raft), 5210N (admin), 911N (HTTP) | 真实 KV 状态机；Kafka ingest 接 group 1 |
| redpanda | redpandadata/redpanda | 19092 | 单容器 Kafka API；topic `input_1` 由 topic-init 创建 |
| prometheus | prom/prometheus | 9090 | 挂载 `deploy/observability/prometheus/` 的 rules/alerts |
| grafana | grafana/grafana | 3000 (admin/admin) | 预置 `rr-cluster-overview` 看板 |
| bench（profile） | `rr-lab:dev` | — | 网内运行 rr-perf-bench 外部模式 |

## 快速开始 / Quickstart

```bash
# 如使用 OrbStack 而当前 context 是 Docker Desktop：
# docker context use orbstack

# 启动全栈（首次构建约需几分钟：rdkafka 会编译捆绑的 librdkafka）
docker compose -f deploy/lab/docker-compose.yml up -d --build

# 验证集群：三节点 group 1 应一致（同一 leader、同一 last_log_index）
docker exec kv-1 rr-admin --addr 127.0.0.1:52001 group status --group 1
docker exec kv-2 rr-admin --addr 127.0.0.1:52001 group status --group 1

# 从宿主机读写（真实 KV 语义）
./target/debug/rr-admin repl --addr 127.0.0.1:51101 --group 1
# rr> put foo bar / get foo

# Grafana: http://localhost:3000 （admin/admin）→ "rr Cluster Overview"
# Prometheus: http://localhost:9090

# 压力测试（在 lab 网络内运行，leader-redirect 地址可解析）
docker compose -f deploy/lab/docker-compose.yml --profile bench run --rm bench

# 拆除（-v 同时清空卷）
docker compose -f deploy/lab/docker-compose.yml down -v
```

## 目录内容 / Contents

- `Dockerfile` — 构建 `rr-lab:dev`（kv-store[rdkafka] + rr-admin + rr-perf-bench）
- `docker-compose.yml` — 基础栈（直连，基线/压力/soak 用）
- `docker-compose.chaos.yml` — 混沌 override（Toxiproxy 介入节点间流量），见 `chaos/`
- `prometheus.yml`, `grafana/provisioning/` — 可观测性接线
- `chaos/` — 混沌脚本套件（kill/pause/分区/延迟/丢包/删卷/滚动重启）
- `soak.sh` — 小时级稳定性长跑 + 报告生成（输出到 `reports/`）

## 相关 harness

- **`scripts/soak.sh`(仓库根,R-8)**:故障注入 + 数据丢失 oracle 的严格版 soak
  (seed 化故障循环 / ack-log 回放 / 三件套 oracle;报告入 `docs/perf/soak/`)。
  负载与 oracle 均经 `--profile bench` 在 lab 网内运行(见下"已知语义"的重定向条)。

## 已知语义 / Known semantics

- kv-store 容器使用 **MemStorage**：容器重启 = 全量状态丢失，靠 leader 的
  InstallSnapshot 追平（`chaos/state-wipe-restart.sh` 专门验证这一点）。
- leader-redirect 返回的是**网内主机名地址**——负载与自动重试必须在 lab
  网络内运行；宿主机上的手动 repl/验证请使用已发布端口（127.0.0.1:5110N）。
