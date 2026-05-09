---
title: "Snapshot strategy — periodic auto-trigger via openraft SnapshotPolicy"
status: accepted
created: 2026-05-09
updated: 2026-05-09
tags: [adr, snapshot, performance, reliability]
---

# A-0008 — Snapshot Strategy

## Context

rr node 的唯一输入是 Raft apply。快照必须在 apply 路径内部触发，不能依赖外部 API（外部 API 调用本身也需要经过 Raft，否则破坏单一输入原则）。

openraft 0.10 提供了内置机制：`SnapshotPolicy::LogsSinceLast(N)` — 自上次快照以来 Raft log 增长超过 N 条时自动触发。

## Problem

当前 rr 的 `raft_node.rs` Config 未设置 `snapshot_policy`，使用 openraft 默认值 `LogsSinceLast(5000)`。这对大多数场景合理但缺乏显式文档和可配置性。

另外：
- 快照太频繁：性能开销大（序列化用户状态）
- 快照太稀疏：crash 后需要重放更多 Raft log（慢恢复 + Kafka 重投）

## Decision

### 1. 使用 openraft 的 SnapshotPolicy::LogsSinceLast

```rust
// raft_node.rs — 明确设置（替代隐式默认）
Config {
    snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(snapshot_logs_since_last),
    // ...
}
```

openraft 在后台 SM worker 任务中异步执行快照，不阻塞 apply 热路径。

### 2. 默认值推荐

| 场景 | snapshot_logs_since_last | 恢复时最多重放 | 快照频率估算 |
|---|---|---|---|
| 开发/测试 | 500 | 500 条 | 每 5s（100 ops/s） |
| 生产低频 | 5,000 | 5,000 条 | 每 50s（100 ops/s） |
| **生产高频（推荐）** | **50,000** | **50,000 条** | **每 500ms（100k ops/s）** |
| 生产极高频 | 200,000 | 200,000 条 | 每 2s（100k ops/s） |

**推荐默认：50,000**（在 100k ops/s 目标下每 500ms 触发一次快照）

### 3. 快照与 Kafka offset 的协调

快照时自动包含：
- 用户业务状态 (`user_snapshot`)
- Kafka ingestor offset (`framework_state.ingestor.last_applied_offset`)
- Idempotency watermarks (`framework_state.idempotency`)
- **Output outbox 状态**（ADR A-0007，`framework_state.outbox`）

快照 = 完整的恢复基点。Kafka 消费从 snapshot 的 offset 恢复；Raft log 从 snapshot 的 last_log_id 之后重放。

### 4. 快照持久化与 Raft log 的关系

- `DurabilityPolicy::Strict`（fsync per write）：log 写盘在 apply 之前 100% 完成，snapshot.last_log_id 对应的 log 一定在磁盘上。无需额外协调。
- `DurabilityPolicy::ReplicatedOnly`（RF≥3，无 fsync）：snapshot 的 `last_log_id` 可能大于本节点 durable log index。openraft 的 recovery 路径显式处理此 case（purge hole，从其他节点 AppendEntries 补全）。

### 5. 不需要"触发快照的 Raft 命令"

快照是框架管理的 meta 操作，不是业务命令。openraft 的 `SnapshotPolicy::LogsSinceLast` 让框架自主决定时机，完全符合"单一业务输入（Raft apply）"的设计约束。

## Consequences

### Positive
- 零代码改动（openraft 原生支持）
- 快照触发与 apply 路径完全解耦，不影响吞吐
- 通过调整 `snapshot_logs_since_last` 可在 recovery 速度与快照频率之间权衡

### Negative
- 快照触发时间不确定（基于 log 增长量而非时间）
- 高吞吐下快照可能非常频繁（需确保快照速度 > 触发速度）

## Changelog

| Date | Change |
|------|--------|
| 2026-05-09 | Initial design |
