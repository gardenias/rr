# Rr Local Lab (OrbStack / Docker)

> **仅本地实验环境 / LOCAL LAB ONLY** — 基础编排无 TLS、无认证，kv-store 以 allow_insecure 运行。
> 可选 Console overlay 仅在 Browser→Console 边界启用 HTTPS 和 viewer token，不改变节点面的安全设置。
> 禁止用于任何真实部署。The base stack has no TLS or auth and runs kv-store with allow_insecure; the optional Console
> overlay adds HTTPS and viewer authentication only at the Browser→Console boundary. Never reuse outside a local lab.

一个可复现的本地实验环境：**3 节点 kv-store 集群（真实 KV 语义）+ Redpanda（Kafka 协议）命令输入 +
Prometheus/Grafana 可观测性 + 可选只读 Cluster Admin Console**，用于压力、混沌与稳定性测试。
A reproducible local lab with a three-node kv-store cluster, Redpanda (Kafka API), Prometheus/Grafana, and an optional
read-only Cluster Admin Console for stress, chaos, and soak testing.

## 组件 / Components

| 服务 / Service | 镜像 / Image | 端口（host） / Ports | 说明 / Notes |
|---|---|---|---|
| kv-1/2/3 | `rr-lab:dev`（本目录 Dockerfile 构建） | 5110N (client+raft), 5210N (admin), 911N (HTTP) | 真实 KV 状态机；Kafka ingest 接 group 1 |
| redpanda | redpandadata/redpanda | 19092 | 单容器 Kafka API；topic `input_1` 由 topic-init 创建 |
| prometheus | prom/prometheus | 9090 | 挂载 `deploy/observability/prometheus/` 的 rules/alerts |
| grafana | grafana/grafana | 3000 (admin/admin) | 预置 `rr-cluster-overview` 看板 |
| console（opt-in overlay） | `rr-lab:dev` | 127.0.0.1:8080 (HTTPS) | 只读 viewer；复用 rr-admin BFF 与静态导出 / Read-only viewer using the rr-admin BFF and static export |
| bench（profile） | `rr-lab:dev` | — | 网内运行 rr-perf-bench 外部模式 |

## 快速开始 / Quickstart

```bash
# 如使用 OrbStack 而当前 context 是 Docker Desktop：
# docker context use orbstack

# 生成 Dockerfile 所需的 Console 静态导出（需要 just 和 pnpm；运行时不需要 Node.js）
just console-build

# 启动全栈（首次构建约需几分钟：rdkafka 会编译捆绑的 librdkafka）
docker compose -f deploy/lab/docker-compose.yml up -d --build

# 验证集群：三节点 group 1 应一致（同一 leader、同一 last_log_index）
docker exec kv-1 rr-admin --addr 127.0.0.1:52001 group status --group 1
docker exec kv-2 rr-admin --addr 127.0.0.1:52001 group status --group 1

# 从宿主机读写（真实 KV 语义）
./target/debug/rr-admin repl --addr 127.0.0.1:51101 --group 1
# rr> put foo bar / get foo

# Grafana: http://localhost:3000 （admin/admin）→ "Rr Cluster Overview"
# Prometheus: http://localhost:9090

# 压力测试（在 lab 网络内运行，leader-redirect 地址可解析）
docker compose -f deploy/lab/docker-compose.yml --profile bench run --rm bench

# 拆除（-v 同时清空卷；若启用 Console overlay，改用下方命令）
docker compose -f deploy/lab/docker-compose.yml down -v
```

## 可选只读 Console overlay / Optional Read-only Console Overlay

先完成上述 `just console-build`。Dockerfile 会把 `crates/rr-admin/console/out` 复制到镜像，由同一个 `rr-admin`
二进制提供静态前端和 BFF，无需 Node.js 生产服务。设置四个非空环境变量后，在基础 compose 文件上叠加现有 overlay：

Run `just console-build` first. The Dockerfile copies `crates/rr-admin/console/out` into the image, and the same
`rr-admin` binary serves the static frontend and BFF without a Node.js production server. Set four non-empty environment
variables, then layer the existing overlay on the base compose file:

```bash
export RR_CONSOLE_TOKEN='local-viewer-token'
export RR_CONSOLE_TLS_CERT=/absolute/path/to/console.crt
export RR_CONSOLE_TLS_KEY=/absolute/path/to/console.key
export RR_CONSOLE_CA_CERT=/absolute/path/to/ca.crt

docker compose -f deploy/lab/docker-compose.yml \
  -f deploy/lab/docker-compose.console.yml up -d --build
```

将路径替换为实际文件。Console 证书 SAN 必须覆盖 `localhost` 和 `127.0.0.1`，CA 必须签发该证书；三个文件必须
可由容器 UID `10001` 读取。CA 同时用于容器 HTTPS healthcheck 和浏览器信任。`RR_CONSOLE_TOKEN` 仅是浏览器
viewer token，不是节点 Admin token；基础 lab 的节点 AdminService 仍为无认证明文。

Replace the paths with actual files. The Console certificate SANs must cover `localhost` and `127.0.0.1`, and the CA
must have signed it. All three files must be readable by container UID `10001`. The CA is used by both the container
HTTPS healthcheck and browser trust. `RR_CONSOLE_TOKEN` is only the browser viewer token, not a node Admin token; the
base lab's node AdminService remains plaintext without authentication.

信任 CA 后访问 **`https://localhost:8080`** 并用 viewer token 登录；该端口只绑定宿主机 loopback。BFF 通过 lab 网内
Admin gRPC（`kv-N:52001`）读取节点、组和拓扑等 Admin-backed 数据。当前 `kv-store` 未配置 Console 节点描述符，
因此不会提供受信 `observability_addr`：`/health`、`/ready` 和 `/metrics` 采集以及 `CONSOLE_MEMORY` 实时趋势会按
能力降级，开放 9100 端口本身不会启用这条路径。完整实时观测需要节点另行提供完整描述符，不能把本 overlay 当作
该能力的验收环境。

After trusting the CA, open **`https://localhost:8080`** and log in with the viewer token; the port is bound only to
host loopback. The BFF reads Admin-backed node, group, and topology data through in-network Admin gRPC
(`kv-N:52001`). The current `kv-store` does not configure a Console node descriptor and therefore provides no trusted
`observability_addr`: `/health`, `/ready`, and `/metrics` collection and `CONSOLE_MEMORY` live trends degrade by
capability. Publishing port 9100 alone does not enable this path. Full live observability requires a complete node
descriptor; this overlay is not an acceptance environment for that capability.

Console 是只读 viewer：reconcile POST 仅刷新数据，不修改 membership、不强制选主，也不执行 TriggerSnapshot、
DrainNode、offset reset 或进程重启。受控写操作仍由 Admin CLI 执行，停止 Console 不影响 Raft 节点。拆除含
Console 的完整栈：

The Console is a read-only viewer: reconcile POST only refreshes data and performs no membership changes, forced
leadership, TriggerSnapshot, DrainNode, offset reset, or process restart. Controlled writes remain with the Admin CLI,
and stopping the Console does not affect Raft nodes. Tear down the full stack including Console with:

```bash
docker compose -f deploy/lab/docker-compose.yml \
  -f deploy/lab/docker-compose.console.yml down -v
```

## 目录内容 / Contents

- `Dockerfile` — 构建 `rr-lab:dev`（kv-store[rdkafka] + rr-admin + rr-perf-bench + Console 静态导出）
- `docker-compose.yml` — 基础栈（直连，基线/压力/soak 用）
- `docker-compose.console.yml` — 可选只读 Console overlay（HTTPS + viewer token，host `127.0.0.1:8080`）
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
