# Counter — 分布式计数器示例 / Distributed Counter Example

## 简介 / Introduction

基于 `rr` 分布式多 Raft 共识框架的分布式计数器示例。展示如何使用 `Service` trait 实现一个支持原子递增、读取和重置操作的命名计数器服务。所有状态变更通过 Raft 共识协议复制，保证强一致性。

---

A distributed counter example built on the `rr` multi-Raft consensus framework. Demonstrates how to implement named counters with atomic `Increment`, `Get`, and `Reset` operations using the `Service` trait. All state mutations are replicated through Raft consensus for strong consistency.

## 功能 / Features

- **原子递增** — 支持正负增量的原子计数器操作 / Atomic increment with positive or negative deltas
- **命名计数器** — 支持多个独立命名的计数器 / Multiple independently named counters
- **快照与恢复** — 完整的快照/恢复支持，用于 Raft 日志压缩 / Full snapshot/restore for Raft log compaction
- **单节点演示** — 零配置运行演示模式 / Zero-config demo mode
- **多节点集群** — `--peers` 全量花名册 + `--bootstrap` 组成真实三节点 Raft 集群（实测含故障转移）/ Real three-node Raft cluster via a full `--peers` roster + `--bootstrap` (failover verified)

## 使用方法 / Usage

### 单节点演示 / Single-Node Demo

```bash
cargo run -p counter
```

---

### 多节点集群 / Multi-Node Cluster

`--peers` 携带全量 `id@addr` 花名册（必须包含本节点），三个终端使用同一份花名册。每个节点在 `5200N` 提供 ClientService，在 `5300N`（port+1000）提供 AdminService。

`--peers` carries the full `id@addr` roster (which must include the node itself), identical across all terminals. Each node serves ClientService on `5200N` and AdminService on `5300N` (port+1000).

```bash
# 终端 1 — 引导节点 / Terminal 1 — bootstrap node
cargo run -p counter -- --node-id 1 --port 52001 --bootstrap --peers 1@127.0.0.1:52001,2@127.0.0.1:52002,3@127.0.0.1:52003

# 终端 2 / Terminal 2
cargo run -p counter -- --node-id 2 --port 52002 --peers 1@127.0.0.1:52001,2@127.0.0.1:52002,3@127.0.0.1:52003

# 终端 3 / Terminal 3
cargo run -p counter -- --node-id 3 --port 52003 --peers 1@127.0.0.1:52001,2@127.0.0.1:52002,3@127.0.0.1:52003
```

验证（引导节点日志出现 `Cluster bootstrapped on node 1 — voters: [1, 2, 3]` 后）：

Verify (after the bootstrap node logs `Cluster bootstrapped on node 1 — voters: [1, 2, 3]`):

```bash
cargo run -p rr-admin -- --addr 127.0.0.1:53001 group status --group 1      # leader_node_id=1
cargo run -p rr-admin -- --addr 127.0.0.1:53001 group membership --group 1  # Voters: 1,2,3
```

写入必须发往 leader 的监听端口；复制进度以各节点 `group status` 的 `last_log_index` 为准（counter 没有自带读回客户端）。

Writes must go to the leader's listen port; verify replication via `last_log_index` in each node's `group status` (counter ships no read-back client).

## 参数说明 / Arguments

| 参数 / Argument | 说明 / Description |
|---|---|
| `--node-id <N>` | 节点 ID（u64），默认 1 / Node ID (u64), defaults to 1 |
| `--port <PORT>` | gRPC 监听端口，默认 52001 / gRPC listen port, defaults to 52001 |
| `--peers <ID@ADDR,...>` | 全量花名册（逗号分隔的 `id@addr`，须含本节点）/ Full cluster roster (comma-separated `id@addr`, must include this node) |
| `--bootstrap` | 以全量成员初始化 Raft 组（仅第一个节点）/ Initialize the Raft group with the full membership (first node only) |
| `--demo` | 运行演示并退出 / Run demo operations and exit |

## 操作类型 / Operations

| 操作 / Operation | 说明 / Description |
|---|---|
| `Increment { key, delta }` | 将计数器增加 `delta`（不存在则创建）/ Add `delta` to counter (creates if absent) |
| `Get { key }` | 读取计数器当前值 / Read current counter value |
| `Reset { key }` | 重置计数器（删除）/ Reset counter to zero (removes it) |

## 测试 / Tests

```bash
cargo test -p counter
```
