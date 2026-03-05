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
- **多节点集群** — 支持多节点 Raft 集群部署 / Multi-node Raft cluster deployment

## 使用方法 / Usage

### 单节点演示 / Single-Node Demo

```bash
cargo run -p counter
```

---

### 多节点集群 / Multi-Node Cluster

启动引导节点（第一个节点）：

Bootstrap the first node:

```bash
cargo run -p counter -- --node-id 1 --port 52001 --bootstrap
```

加入其他节点：

Join additional nodes:

```bash
cargo run -p counter -- --node-id 2 --port 52002 --peers 127.0.0.1:52001
cargo run -p counter -- --node-id 3 --port 52003 --peers 127.0.0.1:52001,127.0.0.1:52002
```

## 参数说明 / Arguments

| 参数 / Argument | 说明 / Description |
|---|---|
| `--node-id <N>` | 节点 ID（u64），默认 1 / Node ID (u64), defaults to 1 |
| `--port <PORT>` | gRPC 监听端口，默认 52001 / gRPC listen port, defaults to 52001 |
| `--peers <ADDR,...>` | 逗号分隔的对等节点地址 / Comma-separated peer addresses |
| `--bootstrap` | 作为集群领导者引导（仅第一个节点）/ Bootstrap as cluster leader (first node only) |
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
