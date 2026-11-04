# KV Store — 参考 KV 服务示例 / Reference KV Service Example

## 简介 / Introduction

rr 框架的主参考示例：一个真实语义的分布式 KV 服务（`Put` / `Get` / `Delete` / `Increment`），演示业务服务
接入框架的完整路径——实现 `Service` trait，由 `ServiceStateMachineAdapter` 提供 FramedEntry 编解码、
keyed 幂等与框架元数据快照，再经 `ShardRouter` 分片路由对外提供 Client / Admin / Raft 三个 gRPC 服务面。
不同于 stock `rr-server` 内置的 EchoStateMachine 冒烟服务（`get` 回显 key 本身），本示例是真实 KV 语义，
也是 perf-lab / soak 等生产形态验证的被测服务。

---

The framework's main reference example: a real-semantics distributed KV service (`Put` / `Get` / `Delete` /
`Increment`) showing the full integration path — implement the `Service` trait, let
`ServiceStateMachineAdapter` supply FramedEntry codecs, keyed idempotency and framework-state snapshots,
and serve the Client / Admin / Raft gRPC surfaces behind the `ShardRouter`. Unlike stock `rr-server`'s
built-in EchoStateMachine smoke service (`get` echoes the key itself), this example has real KV semantics
and is the system under test for the perf-lab and soak harnesses.

## 演示的框架能力 / What It Exercises

- **三节点真实共识** — `--peers` 全量花名册 + `--bootstrap` 手动引导；或 `--formation genesis
  --cluster-name <N> --etcd-endpoints <URL>` 同构 Genesis（P1：经 etcd lifecycle 记录 CAS 自动组队，
  无 bootstrap 节点）/ real three-node consensus via manual `--peers` + `--bootstrap`, or managed
  homogeneous Genesis formation (P1: automatic group formation through an etcd lifecycle CAS record,
  no bootstrap node)
- **持久化 Raft 日志** — `--features rocks` + `--data-dir <PATH>` 把 Raft 日志落到
  `{PATH}/raft-log/{group_id}`（ADR A-0016）；业务 KV 始终为内存态 / durable Raft log via
  `--features rocks` + `--data-dir` (ADR A-0016); the business KV map stays in memory
- **安全生产形态** — `--tls-cert/--tls-key/--tls-ca` 全有或全无 + `RR_ADMIN_TOKEN` / `RR_CLIENT_TOKEN`
  环境变量，过 G1–G7 启动安全门；无 TLS 旗标则保持明文开发模式 / all-or-nothing TLS flags plus token
  env vars pass the full G1–G7 startup gates; without TLS flags the node stays in plaintext dev mode
- **Kafka 摄入** — `--features rdkafka --kafka-brokers <A,...>`，leader 独占消费 + 自动幂等
  exactly-once apply / leader-exclusive Kafka ingest with auto-idempotency exactly-once apply
- **可观测性** — `--metrics-port` 暴露 `/health` `/ready` `/metrics`；`--snapshot-logs-since-last`
  调节快照触发阈值（默认 50_000）/ HTTP health/readiness/Prometheus endpoints; tunable snapshot
  trigger threshold (default 50_000)

## 使用方法 / Usage

### 单节点演示 / Single-Node Demo

零配置，进程内 Raft 自选主，跑一组读写后退出（约 0.2s）/ Zero config; in-process Raft, self-elects,
runs a few reads/writes and exits in ~0.2s:

```bash
cargo run -p kv-store
```

### 单节点服务 + REPL 客户端 / Single-Node Server + REPL Client

```bash
# 终端 1：引导节点（阻塞运行，Ctrl-C 停止）/ Terminal 1: bootstrap node (blocks; Ctrl-C to stop)
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap

# 终端 2：REPL 打客户端端口 51001；status/members 等 admin 命令自动推导到 port+1000=52001
# Terminal 2: the REPL targets the client port 51001; admin commands derive port+1000=52001
cargo run -p rr-admin -- repl --addr 127.0.0.1:51001 --group 1
# rr> put k1 v1   → OK
# rr> get k1      → v1
# rr> delete k1   → OK
```

### 多节点集群 / Multi-Node Cluster

`--peers` 携带全量 `id@addr` 花名册（必须包含本节点），三个终端使用同一份花名册。每个节点在 `5100N`
提供 ClientService，在 `5200N`（port+1000）提供 AdminService。

`--peers` carries the full `id@addr` roster (which must include the node itself), identical across all
terminals. Each node serves ClientService on `5100N` and AdminService on `5200N` (port+1000).

```bash
# 终端 1 — 引导节点 / Terminal 1 — bootstrap node
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap \
  --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003

# 终端 2 / Terminal 2
cargo run -p kv-store -- --node-id 2 --port 51002 \
  --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003

# 终端 3 / Terminal 3
cargo run -p kv-store -- --node-id 3 --port 51003 \
  --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003
```

验证（引导节点日志出现 `Cluster bootstrapped on node 1 — voters: [1, 2, 3]` 后）：

Verify (after the bootstrap node logs `Cluster bootstrapped on node 1 — voters: [1, 2, 3]`):

```bash
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group status --group 1  # 三节点 leader 一致
cargo run -p rr-admin -- repl --addr 127.0.0.1:51001 --group 1          # 真实 KV 读写
```

故障转移（实测通过）：Ctrl-C 掉 leader，其余节点数秒内选出新 leader，写入继续且已提交数据保留。

Failover (verified): Ctrl-C the leader — the survivors elect a new one within seconds; writes continue
and committed data survives.

### just 一键 / One-Shot via just

```bash
just kv-cluster-up        # 三节点内存集群（--bootstrap 路径，端口 53101+）/ 3-node in-memory cluster
just kv-cluster-put k v   # REPL 写读探针 / REPL write+read probe
just kv-cluster-status    # 三节点 group status / group status on all three nodes
just kv-cluster-down      # 清理（PID/日志在 target/just/）/ teardown (state under target/just/)

just etcd-up              # Genesis 前置：本地 etcd（需已有 quay.io/coreos/etcd:v3.5.13 镜像）
just genesis-up           # 三节点同构 Genesis 集群（--formation genesis 路径，端口 53201+）
just genesis-status && just genesis-down
```

## 参数说明 / Arguments

kv-store 的 CLI 为手卷解析，`src/main.rs` 顶部 doc 注释是单一事实来源。核心旗标：

The CLI is hand-rolled; the `src/main.rs` doc header is the single source of truth. Core flags:

| 参数 / Argument | 说明 / Description |
|---|---|
| `--node-id <N>` | 节点 ID（u64），默认 1 / node ID (u64), default 1 |
| `--port <PORT>` | gRPC 端口（默认 51001）；Admin API 恒为 PORT+1000 / gRPC port; Admin API is PORT+1000 |
| `--listen-addr <HOST>` | 绑定地址（默认 127.0.0.1）；广告地址来自花名册 / bind host; advertise from roster |
| `--peers <ID@ADDR,...>` | 全量花名册（须含本节点）；空 = 单节点 / full roster (incl. self); empty = single node |
| `--bootstrap` | 手动初始化 Raft 组（仅一个节点）；与 `--formation` 互斥 / manual group init (one node only) |
| `--formation genesis\|adopt` | 托管组队（P1）；需 `--cluster-name` + `--etcd-endpoints` / managed formation (P1) |
| `--data-dir <PATH>` | 持久化 Raft 日志；需 rocks 构建 / durable Raft log; requires `--features rocks` |
| `--tls-cert/--tls-key/--tls-ca` | 安全形态（全有或全无）/ secure mode, all-or-nothing |
| `RR_ADMIN_TOKEN`/`RR_CLIENT_TOKEN` | 安全模式必需；从环境变量读 / required in secure mode; read from env |
| `--metrics-port <PORT>` | 另开 HTTP 服务 `/health` `/ready` `/metrics` / also serve HTTP health/ready/metrics |
| `--kafka-brokers <A,...>` | Kafka 摄入（需 rdkafka 构建）/ Kafka ingest (rdkafka build) |
| `--snapshot-logs-since-last <N>` | 快照触发阈值（默认 50_000）/ snapshot trigger threshold |

## 操作类型 / Operations

| 操作 / Operation | 说明 / Description |
|---|---|
| `Put { key, value }` | 写入或覆盖键值 / Insert or overwrite a key |
| `Get { key }` | 读取键值（不存在返回 `None`）/ Read a key (`None` when absent) |
| `Delete { key }` | 删除键 / Delete a key |
| `Increment { key, delta }` | 计数器加 `delta`（缺省创建为 `delta`）；十进制存储 / counter += `delta` (decimal UTF-8) |

## 延伸阅读 / Further Reading

- [Getting Started](https://rr-framework.github.io/guide/getting-started) — 从零接入框架 / framework onboarding
- [Operations Playbook](https://rr-framework.github.io/guide/playbook) — Genesis / adopt / 故障转移 / failover
- [TLS 部署 / TLS](https://rr-framework.github.io/deployment/tls) — 证书与 G1–G7 安全门
- [Kafka 摄入 / Kafka Input](https://rr-framework.github.io/guide/kafka-input) — leader 独占消费 + 自动幂等
- [幂等 / Idempotency](https://rr-framework.github.io/guide/idempotency) — keyed 写的 exactly-once 语义

## 测试 / Tests

```bash
cargo test -p kv-store   # 单元测试 / unit tests
just test-etcd           # 真实 etcd 验收（含进程级 Genesis，需 Docker + 本地 etcd 镜像）
```
