---
title: "Output/Outbox mechanism for guaranteed side-effect delivery"
status: accepted
created: 2026-05-09
updated: 2026-05-09
tags: [adr, architecture, output, outbox, reliability, at-least-once]
---

# A-0007 — Output / Outbox Mechanism

## Context

rr 的状态机遵循 Disruptor 单 worker 线程模式：

```
input (Kafka/gRPC) → Raft log → serial apply → response
```

目前 `Service::handle()` 只返回响应字节，不支持副作用（Output）。  
真实业务场景（如 CEX 清算/撮合系统）中，一次 apply 必然产生输出：

- **类型 A — gRPC 调用**：发送指令到另一个 rr group（如清算 → 撮合发送订单）
- **类型 B — Kafka 发布**：向下游发送事件（如撮合 → 行情 topic）
- **混合**：同一次 input 可能同时产生 A 和 B

**正确性要求**：
1. 每个 output 必须至少成功发送一次（at-least-once）
2. 发送失败必须卡住并无限重试，不能静默丢弃
3. leader 切换后，未发送的 output 必须由新 leader 继续发送
4. output 状态必须纳入快照，随 Raft 副本复制

## Problem

当前框架对 output 的支持完全空白：
- `Service::handle()` 返回 `Vec<u8>`，无法携带 outputs
- 没有 outbox 存储
- 没有 OutputRunner（仅有 IngestorRunner 处理输入）
- 重试逻辑全部在客户端（max_retries=3），不满足"无限重试"要求
- output 不在快照中，leader 切换后丢失

## Options

### Option 1 — Output 通过额外 Raft 条目确认

每次 output 成功发送后写入 `OutputAck` Raft 条目来持久化 sent_watermark。

**Pros**: 强一致，sent_watermark 完全持久化  
**Cons**: 每次 output 额外一次 Raft round trip，吞吐下降

### Option 2 — Transactional Outbox（选定方案）

Output 随 apply 写入 outbox（状态机内存状态的一部分），随快照持久化。  
已发送的 output 通过内存中的 `sent_watermark` 追踪，快照时将 watermark 持久化。  
Leader 切换时，新 leader 从快照恢复 outbox，re-send watermark 之后的所有条目（at-least-once）。

**Pros**:
- 无额外 Raft round trip
- 符合 Disruptor 串行模型（output 随 apply 同步追加到 outbox）
- At-least-once 语义，消费方幂等处理即可

**Cons**:
- 消费方必须幂等（需 output `idempotency_key`）
- sent_watermark 仅在快照时落盘；最坏情况：leader crash → 新 leader 从上一次快照位置重发

## Decision

采用 **Option 2 (Transactional Outbox)**。

### 核心数据结构

```rust
// rr-core/src/output.rs

/// 单个输出目标。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputTarget {
    /// 写入另一个 rr group（走 RrClient 幂等写）。
    RaftWrite { addr: String, group_id: u64 },
    /// 发布到 Kafka topic。
    KafkaPublish { topic: String, partition: i32, key: Vec<u8> },
}

/// 由 Service::handle() 产生的一个 output。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Output {
    /// 框架分配的单调递增序号（用户创建时填 0，框架填充）。
    pub seq: u64,
    /// 防重幂等键，消费方使用此 key 去重。
    pub idempotency_key: String,
    pub target: OutputTarget,
    pub payload: Vec<u8>,
}

/// Service::handle() 的完整返回值（含 outputs）。
pub struct HandleResult {
    /// 同步返回给调用方的响应字节。
    pub response: Vec<u8>,
    /// 本次 apply 产生的所有 outputs（可为空）。
    pub outputs: Vec<Output>,
}
```

### Service trait 变更（向后兼容）

```rust
// rr-core/src/service.rs — 新增 handle_with_outputs，保留 handle 作为默认委托

pub trait Service: Send + Sync {
    /// 处理请求并可选地产生 outputs。
    /// 默认实现委托给 handle()，返回空 outputs。
    /// 需要 outputs 的实现者覆盖此方法。
    async fn handle_with_outputs(&self, request: Vec<u8>) -> Result<HandleResult, RrError> {
        let response = self.handle(request).await?;
        Ok(HandleResult::from_response(response))
    }

    /// 无 outputs 的简化入口，向后兼容。
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError>;
    // ...
}
```

框架内部改为调用 `handle_with_outputs()`；已有实现只要没有 outputs 需求，无需修改。

### Outbox 状态（纳入快照）

```rust
// FrameworkState 新增字段
pub struct FrameworkState {
    pub idempotency: Option<IdempotencyState>,
    pub ingestor: Option<IngestorState>,
    pub outbox: Option<OutboxState>,     // 新增
}

pub struct OutboxState {
    /// 所有未确认的 outputs（seq > sent_watermark）。
    pub pending: Vec<Output>,
    /// 已发送的最高 seq。小于等于此值的可以丢弃。
    pub sent_watermark: u64,
    /// 下一个 output 的 seq（全局单调递增）。
    pub next_seq: u64,
}
```

### OutputRunner（与 IngestorRunner 对称）

```rust
// rr-raft/src/output_runner.rs

pub struct OutputRunner {
    group_id: GroupId,
    mgr: Arc<RaftGroupManager>,
    rr_client: Arc<RrClient>,         // 用于 RaftWrite 类型
    kafka_producer: Option<Arc<KafkaProducer>>, // 用于 KafkaPublish 类型
}

// 运行逻辑（仅 leader 运行）
// loop:
//   outputs = mgr.pending_outputs(group_id).await
//   for output in outputs:
//     loop:
//       match send(output):
//         Ok(_) => mgr.advance_output_watermark(group_id, output.seq).await; break
//         Err(e) => { tracing::warn!(...); sleep(backoff); }
```

`advance_output_watermark` 是内存操作（更新 in-memory sent_watermark），不写 Raft 日志。下次快照时自动持久化。

### Outbox 生命周期

```
apply(input)
  → service.handle_with_outputs(payload) → (response, Vec<Output>)
  → 为每个 Output 分配 seq（next_seq++）
  → 追加到 pending_outputs（内存 outbox）
  → 返回 response

OutputRunner（leader-only background task）:
  监听 pending_outputs 变化
  → send each output（重试直到成功）
  → 成功：advance in-memory sent_watermark

Snapshot（每 N apply 后异步触发）:
  → 序列化 user_state + outbox（pending 列表 + sent_watermark + next_seq）
  → 随快照落盘

Leader 切换：
  → 新 leader restore 快照
  → 恢复 outbox.pending + outbox.sent_watermark
  → 启动 OutputRunner，从 sent_watermark+1 开始重发
```

### 幂等性设计

- 每个 output 有 `idempotency_key`（用户负责构造，通常是 `"{input_idempotency_key}:{output_index}"`）
- gRPC 目标：通过 rr 的框架级幂等（IdempotencyKey）去重
- Kafka 目标：consumer 基于 key 去重，或使用 Kafka 事务性 producer（idempotent producer）

## Consequences

### Positive
- At-least-once output delivery，无需额外 Raft round trip
- 单一 apply 路径（串行），outbox 操作无并发问题
- Leader 切换后完整恢复，无 output 丢失
- 可扩展：`OutputTarget` enum 未来可加更多类型

### Negative
- 消费方必须幂等处理 output（允许重复接收）
- sent_watermark 仅在快照时落盘；crash 窗口内新 leader 可能重发若干 outputs
- 快照大小增加（outbox pending 列表）

### Neutral
- `Service::handle_with_outputs()` 完全向后兼容，已有实现无需修改
- OutputRunner 不依赖 Raft log，only 依赖 snapshot/outbox 内存状态

## Changelog

| Date | Change |
|------|--------|
| 2026-05-09 | Initial design from Disruptor pattern correctness review |
