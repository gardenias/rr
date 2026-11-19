---
sidebar_position: 7
---

# 部署：生产拓扑 / Deployment: Production Topology

示例使用三个跨可用区节点，分离 Client/Raft、Admin 和 metrics 端口。`--storage rocks` 按 group 持久化
Raft 日志、vote 和 `sm-snapshot.bin`；`--storage mem` 不跨重启保留状态。

The example uses three nodes across availability zones, with separate Client/Raft, Admin, and metrics ports.
`--storage rocks` persists each group's Raft log, vote, and `sm-snapshot.bin`; `--storage mem` does not preserve
state across restart.

## 图示 / Diagram

```plantuml
@startuml
!include <C4/C4_Deployment.puml>

title Deployment Diagram - 3-node Rr Cluster

Deployment_Node(client_zone, "Application Tier", "VPC / Subnet") {
  Deployment_Node(app_host, "App Server", "Linux, multiple instances") {
    Container(user_app, "User Application", "Rust + rr-client SDK", "Embeds rr-client; bootstrap list = [node-1, node-2, node-3]")
  }
}

Deployment_Node(ops_zone, "Operations Zone", "Isolated operator network") {
  Deployment_Node(operator_host, "Operator Workstation", "Developer/SRE laptop or bastion") {
    Container(admin_cli, "rr-admin CLI", "Rust binary", "Run ad-hoc controlled administration; no daemon")
    Container(console_ui, "Console Web UI", "React 19 + Next.js static export in browser", "Read-only viewer")
  }
  Deployment_Node(console_host, "Console Host", "Linux") {
    Container(console_bff, "rr-admin Multi-cluster Console BFF", "Rust + Axum; rr-admin console", "No-source startup; same-origin static files, connection CRUD, scoped JSON /api/v1/console/connections/{id} and SSE; per-runtime Admin credentials")
    Container(console_assets, "Console Static Export", "Next.js static files: console/out", "Shipped with rr-admin or selected with --static-dir; no Node.js production service")
    ContainerDb(console_profiles, "Console Profile Store", "Versioned local 0600 JSON", "Persistent non-sensitive profiles in --state-dir; memory/keychain secrets; no auto-connect on restart")
  }
}

Deployment_Node(cluster_zone, "Rr Cluster", "VPC / 3 availability zones") {
  Deployment_Node(host1, "Node Host 1", "Linux, AZ-a") {
    Container(rr1, "rr-server #1", "Rust binary", "node_id=1, listen=:51001, admin=:52001, metrics=:9100")
    ContainerDb(disk1, "Data Volume 1", "Local SSD / EBS", "/var/lib/rr/data — per-group RocksDB + sm-snapshot.bin with --storage rocks")
  }
  Deployment_Node(host2, "Node Host 2", "Linux, AZ-b") {
    Container(rr2, "rr-server #2", "Rust binary", "node_id=2, listen=:51001, admin=:52001, metrics=:9100")
    ContainerDb(disk2, "Data Volume 2", "Local SSD / EBS", "/var/lib/rr/data — per-group RocksDB + sm-snapshot.bin with --storage rocks")
  }
  Deployment_Node(host3, "Node Host 3", "Linux, AZ-c") {
    Container(rr3, "rr-server #3", "Rust binary", "node_id=3, listen=:51001, admin=:52001, metrics=:9100")
    ContainerDb(disk3, "Data Volume 3", "Local SSD / EBS", "/var/lib/rr/data — per-group RocksDB + sm-snapshot.bin with --storage rocks")
  }
}

Deployment_Node(obs_zone, "Observability", "Monitoring") {
  Container_Ext(prometheus, "Prometheus", "Time-series DB", "Scrapes each node :9100/metrics; optional fixed-template history backend for Console")
}

Deployment_Node(cfg_zone, "Control Plane (optional)", "Config store") {
  Container_Ext(etcd, "etcd / Config Store", "KV store", "Holds routing table and leased NodeDirectory records for production ConfigProvider and optional Console discovery")
}

Rel(user_app, rr1, "Client RPCs", "gRPC :51001")
Rel(user_app, rr2, "Client RPCs (bootstrap alt)", "gRPC :51001")
Rel(user_app, rr3, "Client RPCs (bootstrap alt)", "gRPC :51001")

Rel(admin_cli, rr1, "Runs controlled read/write administration", "Admin gRPC :52001 (TLS/mTLS + Admin token)")

Rel(console_ui, console_bff, "Loads static UI; reads JSON and subscribes to SSE", "HTTPS :8443 (example) + viewer session")
Rel(console_bff, console_assets, "Reads and serves static UI files", "Local file I/O")
Rel(console_bff, console_profiles, "Loads and atomically saves non-sensitive profiles; restart leaves runtimes disconnected", "Local file I/O; --state-dir")
Rel(console_bff, rr1, "Reads node, group and lifecycle state", "Admin gRPC :52001 (TLS/mTLS + Admin token)")
Rel(console_bff, rr2, "Reads node, group and lifecycle state", "Admin gRPC :52001 (TLS/mTLS + Admin token)")
Rel(console_bff, rr3, "Reads node, group and lifecycle state", "Admin gRPC :52001 (TLS/mTLS + Admin token)")
Rel(console_bff, rr1, "Fetches /health and /ready; scrapes /metrics", "HTTP :9100 (trusted observability network)")
Rel(console_bff, rr2, "Fetches /health and /ready; scrapes /metrics", "HTTP :9100 (trusted observability network)")
Rel(console_bff, rr3, "Fetches /health and /ready; scrapes /metrics", "HTTP :9100 (trusted observability network)")

Rel(rr1, rr2, "Raft RPCs", "gRPC :51001")
Rel(rr1, rr3, "Raft RPCs", "gRPC :51001")
Rel(rr2, rr3, "Raft RPCs", "gRPC :51001")

Rel(rr1, disk1, "Reads/writes", "File I/O")
Rel(rr2, disk2, "Reads/writes", "File I/O")
Rel(rr3, disk3, "Reads/writes", "File I/O")

Rel(prometheus, rr1, "Scrapes /metrics", "HTTP :9100")
Rel(prometheus, rr2, "Scrapes /metrics", "HTTP :9100")
Rel(prometheus, rr3, "Scrapes /metrics", "HTTP :9100")
Rel(console_bff, prometheus, "Queries fixed history templates (optional)", "HTTP/HTTPS /api/v1/query_range")

Rel(rr1, etcd, "Loads routing table (optional)", "etcd v3 gRPC")
Rel(rr2, etcd, "Loads routing table (optional)", "etcd v3 gRPC")
Rel(rr3, etcd, "Loads routing table (optional)", "etcd v3 gRPC")
Rel(console_bff, etcd, "Reads NodeDirectory and ConfigProvider discovery (optional)", "etcd v3 gRPC (TLS/mTLS + authentication)")

LAYOUT_WITH_LEGEND()
@enduml
```

图中的 `Container` 是 C4 可部署单元，不表示 Docker 容器。图中只画出一套三节点集群以控制图面复杂度；
一个 Console BFF 可通过独立 runtime 连接多套这样的集群，各连接的 Admin/observability 网络访问需分别放通。
Console BFF、`console/out` 和 versioned profile store 随同一 `rr-admin` 部署单元配置；静态导出由
`--static-dir` 指向，profile 由 `--state-dir` 指向持久独占目录。Axum 同源提供静态文件、连接控制、
connection-scoped JSON 和 SSE，不需要独立 Node.js 生产服务。

Here `Container` is a C4 deployable unit, not a Docker container. The diagram shows one three-node cluster for
readability; one Console BFF can connect to multiple such clusters through isolated runtimes, with Admin/observability
network access allowed for each target. Deploy the Console BFF, `console/out`, and versioned profile store as one
`rr-admin` configuration: select assets with `--static-dir` and a persistent exclusive profile directory with
`--state-dir`. Axum serves static files, connection control, scoped JSON, and SSE from one origin without a Node.js server.

Console 是只读 viewer，不是 Raft 控制面；`POST /connections/{id}/sources/reconcile` 也只刷新该 runtime 的采集，
受控写操作仍由 Admin CLI 执行。实时趋势按连接保留在 BFF 的有界内存中；Prometheus 仅提供可选的固定模板历史查询，
不是 Console 或集群的必需依赖。重启保留 profile 但清空 runtime/session/内存趋势，所有 profile 从 Disconnected 开始。

The Console is a read-only viewer, not a Raft control plane; `POST /connections/{id}/sources/reconcile` only refreshes
that runtime's collection, and controlled writes remain with the Admin CLI. Live trends stay in bounded per-connection BFF
memory. Prometheus provides only optional fixed-template history and is not required by Console or the cluster. Restart
retains profiles but clears runtimes, sessions, and in-memory trends; every profile starts Disconnected.

## 端口与网络安全 / Ports and Network Security

| 端口 / Port | 协议 / Protocol | 服务 / Service | 暴露范围 / Exposed To |
| :--- | :--- | :--- | :--- |
| 8080 / 8443（示例，examples） | HTTP / HTTPS | Console 静态文件 + `/api/v1/console` JSON + SSE / Console static files + JSON + SSE | 仅运维网络；非 loopback 必须 HTTPS + viewer token / Operations network only; non-loopback requires HTTPS + viewer token |
| **51001** | gRPC / HTTP2 | `ClientService` + `RaftService` | 应用子网与集群节点 / App subnet and peer nodes |
| **52001** | gRPC / HTTP2 | `AdminService` | 仅 Admin CLI 与 Console BFF 所在运维区 / Admin CLI and Console BFF in operations zone only |
| **9100** | HTTP | `/health`, `/ready`, `/metrics` | 监控、探针与 Console BFF 网络 / Monitoring, probe, and Console BFF networks |

Client 与 Raft 共享端口减少防火墙规则；Admin 使用独立端口并应限制访问。Admin 和 metrics 默认只绑定
`127.0.0.1`；跨主机访问必须显式设置 `--admin-bind-host` 和 `--metrics-bind-host`，并配合网络策略。
需要进一步隔离 Raft 与 Client 时，需要自定义传输装配。

Sharing the Client/Raft port reduces firewall rules; Admin uses a separate port and must be access-restricted.
Admin and metrics bind to `127.0.0.1` by default; cross-host access requires explicit `--admin-bind-host` and
`--metrics-bind-host` settings plus network policy. Further Raft/Client isolation requires custom transport wiring.

图中的 8080/8443 只是 `--listen` 的示例监听端口，不是必须固定使用的默认端口；实际地址由部署配置决定。
非 loopback 监听必须同时启用 Console HTTPS 和 viewer token，`--allow-insecure` 不能绕过此门禁。浏览器登录后
获得 15 分钟内存 session；节点 Admin token 只留在 BFF，不下发到浏览器。

The diagram's 8080/8443 are example `--listen` ports, not mandatory fixed defaults; deployment configuration selects
the actual address. A non-loopback listener requires both Console HTTPS and a viewer token, and `--allow-insecure`
cannot bypass this gate. Browser login creates a 15-minute in-memory session. Node Admin tokens remain only in the
BFF and are never sent to the browser.

生产环境应限制 Console 监听地址仅对运维区开放，并仅允许 BFF 访问各节点 Admin 和 observability 地址。BFF 到
AdminService 使用 TLS/mTLS + Admin token；到节点 observability 当前使用 HTTP，且端点可能未认证，因此必须用
受信内网或网络策略保护。Console 不向浏览器代理原始指标。

In production, expose the Console listener only to the operations zone and allow the BFF to reach only the intended
node Admin and observability addresses. BFF-to-AdminService uses TLS/mTLS plus an Admin token. Node observability
currently uses HTTP and may be unauthenticated, so protect it with a trusted network or network policy. The Console
does not proxy raw metrics to browsers.

## 硬件示例 / Hardware Examples

这些是容量规划起点，不是基准测试结果。

These are capacity-planning starting points, not benchmark results.

| 资源 / Resource | 开发/测试 / Dev/Test | 小型生产 / Small Prod | 中型生产 / Medium Prod |
| :--- | :--- | :--- | :--- |
| Nodes | 1 | 3 | 5 |
| vCPU / node | 2 | 4–8 | 16+ |
| RAM / node | 4 GB | 8–16 GB | 32+ GB |
| Disk / node | SSD 20 GB | NVMe SSD 200 GB | NVMe SSD 1 TB+ |
| Network | Localhost | 1 Gbps | 10 Gbps, multi-AZ |

- 通常选择奇数节点数；4 节点与 3 节点都只能容忍 1 个节点故障，但 4 节点 quorum 要求更高。
  Odd node counts are typical: four nodes tolerate the same single failure as three, while requiring a larger
  quorum.
- Raft 日志需要低延迟持久化磁盘；rocks 模式应使用 SSD/NVMe。
  Raft logs need low-latency durable storage; use SSD/NVMe for rocks mode.
- 节点应跨至少 3 个 AZ，避免单 AZ 故障失去 quorum。
  Spread nodes across at least three AZs so one AZ failure cannot remove the quorum.

## 启动顺序 / Bootstrap Sequence

```bash
# Node 1 — initialize hosted groups once per cluster life
rr-server serve \
  --node-id 1 --listen-addr 0.0.0.0:51001 \
  --admin-port 52001 --metrics-port 9100 \
  --admin-bind-host 0.0.0.0 --metrics-bind-host 0.0.0.0 \
  --data-dir /var/lib/rr/data --storage rocks \
  --bootstrap \
  --allow-insecure   # DEV ONLY; configure TLS and tokens in production

# Nodes 2 and 3 — populate the local bootstrap routing table
rr-server serve \
  --node-id 2 --listen-addr 0.0.0.0:51001 \
  --admin-port 52001 --metrics-port 9100 \
  --admin-bind-host 0.0.0.0 --metrics-bind-host 0.0.0.0 \
  --data-dir /var/lib/rr/data --storage rocks \
  --peers host-1:51001 \
  --allow-insecure

rr-server serve \
  --node-id 3 --listen-addr 0.0.0.0:51001 \
  --admin-port 52001 --metrics-port 9100 \
  --admin-bind-host 0.0.0.0 --metrics-bind-host 0.0.0.0 \
  --data-dir /var/lib/rr/data --storage rocks \
  --peers host-1:51001,host-2:51001 \
  --allow-insecure
```

- `--bootstrap` 只用于集群初始化，不应作为常规重启参数；已初始化 group 会跳过重复初始化。
  Use `--bootstrap` for cluster initialization, not routine restarts; already initialized groups skip repeated
  initialization.
- 默认安全门禁会拒绝缺少 TLS/mTLS 或 `admin_token` 的生产配置。`--allow-insecure` 和
  `RR_ALLOW_INSECURE=1` 仅用于开发/CI；生产应配置 `--tls-cert`、`--tls-key`、`--tls-ca`，并在需要
  mTLS 时设置 `--tls-require-client-cert` 和 admin token。
  Default safety gates reject production configurations missing TLS/mTLS or `admin_token`.
  `--allow-insecure` and `RR_ALLOW_INSECURE=1` are for development/CI only. Production should configure
  `--tls-cert`, `--tls-key`, `--tls-ca`, plus `--tls-require-client-cert` for mTLS and an admin token.
- `--peers` 只按列表位置推测 node id 并填充本节点内存路由表，不会自动变更 Raft 成员。
  `--peers` only infers node ids by list position and populates the local in-memory routing table; it does not
  change Raft membership.

默认 `auto_promote_learners = true`，追平且满足 quorum guard 的 learner 会自动提升。禁用自动提升时，
在现有 leader 上手动执行：

With `auto_promote_learners = true` by default, caught-up learners that satisfy the quorum guard are promoted
automatically. If auto-promotion is disabled, run these commands against the existing leader:

```bash
rr-admin --addr host-1:52001 group add-learner --group 1 --node-id 2 --addr host-2:51001
# Wait for catch-up before promotion.
rr-admin --addr host-1:52001 group change-membership --group 1 --voters 1,2
```

## 失败模式 / Failure Modes

| 失败 / Failure | 集群影响 / Cluster Impact | 恢复 / Recovery |
| :--- | :--- | :--- |
| 3 节点中 1 个宕机 / 1 of 3 down | 仍有 quorum / Quorum remains | 替换或恢复节点，按需同步快照 / Replace or restore; sync snapshot as needed |
| 3 节点中 2 个宕机 / 2 of 3 down | 写入阻塞 / Writes blocked | 恢复至少 2 个成员 / Restore at least two members |
| leader 磁盘满 / Leader disk full | 写入或快照失败 / Writes or snapshots fail | 释放或扩容磁盘并检查错误 / Free or expand disk and inspect errors |
| leader 位于少数分区 / Leader in minority | 多数侧选出新 leader / Majority elects a leader | 修复分区，旧 leader 作为 follower 追赶 / Heal partition; old leader catches up |
| Console BFF 停止 / Console BFF down | 所有目标节点与 Raft 服务不受影响；全部连接查看和采集中断 / All target nodes and Raft services continue; viewing and collection stop for every connection | 重启 Console；profile 保留但 runtime 不自动恢复，session/内存趋势清空，内存凭证需重新输入 / Restart Console; profiles remain disconnected, sessions/trends clear, and memory credentials must be re-entered |

故障流程见 [leader 选举](./dynamic-leader-election.md) 和 [快照传输](./dynamic-snapshot-transfer.md)。

See [leader election](./dynamic-leader-election.md) and [snapshot transfer](./dynamic-snapshot-transfer.md) for the
failure flows.

## 监控示例 / Monitoring Examples

阈值是告警起点，应按工作负载校准。

Thresholds are starting points and should be calibrated to the workload.

| Metric | 含义 / Meaning | 告警示例 / Example Alert |
| :--- | :--- | :--- |
| `rr_raft_current_term{group_id}` | 每 group 当前 term / Current term per group | 频繁增长 / Frequent increases |
| `rr_raft_is_leader{group_id}` | 本节点是否为 leader / Whether this node is leader | 每 group 聚合后没有 leader / No leader after per-group aggregation |
| `rr_proposal_duration_seconds{group_id}` | 写提案延迟 / Proposal latency | p99 持续超标 / Sustained p99 regression |
| `rr_raft_replication_lag{group_id, follower_node_id}` | leader 侧 follower 日志延迟 / Leader-side follower lag | 持续增长 / Sustained growth |
| `rr_snapshot_installs_total` | 累计快照安装 / Cumulative snapshot installs | 速率突增 / Rate spike |
| `rr_client_retries_total{group_id}` | 客户端重试 / Client retries | 持续增长 / Sustained increase |
| `up{job="rr"}` | 节点存活 / Node liveness | 节点掉线 / Node down |

`rr_client_*` 指标由使用 rr-client 的应用进程暴露，不是 rr-server。<br />`rr_client_*` metrics are exposed by the application process using rr-client, not rr-server.<br />[完整指标清单](../../reference/metrics-reference.md) / [Full metrics catalog](../../reference/metrics-reference.md)

## 相关 / Related

- [容器结构](./containers.md)<br />[Containers](./containers.md)
- [集群管理控制台](../../operations/cluster-admin-console.md)<br />[Cluster Admin Console](../../operations/cluster-admin-console.md)
- [部署指南](../../deployment/deployment.md)<br />[Deployment guide](../../deployment/deployment.md)
