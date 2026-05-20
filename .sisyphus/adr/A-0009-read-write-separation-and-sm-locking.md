---
title: "Read/write RPC separation and state machine locking model"
status: implemented
created: 2026-05-09
updated: 2026-05-13
tags: [adr, architecture, read, write, locking, safety]
---

# A-0009 — Read/Write RPC Separation and State Machine Locking Model

## Context

rr 的核心模型是 Disruptor 单 worker 线程（串行 apply）：

```
input (Kafka/gRPC) → Raft log → serial apply → response
                                               ↑
                    stale read ────────────────┘ (绕过 Raft, 直接读本地 SM)
```

两类请求需要明确区分：
- **写请求**：经过 Raft 共识，串行 apply，保证线性一致
- **读请求（stale）**：读本地 state machine，不经过 Raft，可能略旧

## Problem

### P1: Boolean flag 的安全隐患

最初实现用 `ClientRequest.read_only: bool` 区分读写：

```protobuf
message ClientRequest {
    uint64 partition_id = 1;
    bytes payload = 2;
    optional IdempotencyKey idem_key = 3;
    bool read_only = 4;   // ← 危险
}
```

问题：调用方设错了 `read_only=true` 时，写请求被路由到 local_read()：
- 服务端**不报错**
- 状态机**不修改**
- 数据**静默丢失**

这比返回错误更危险，因为调用方不知道写失败了。

### P2: 类型系统没有强制保证

布尔 flag 依赖调用方的纪律性，而非编译期约束。
任何直接调用 gRPC 的客户端都可能犯这个错误。

## Decision

**使用独立的 gRPC RPC 方法区分读写，由协议层强制语义分离。**

```protobuf
service ClientService {
    // 写请求：经过 Raft 共识。服务端无条件走 submit_write()。
    rpc Request(ClientRequest) returns (ClientResponse);

    // 从节点本地读（stale）：绕过 Raft。服务端无条件走 local_read()。
    rpc ReadLocal(ReadLocalRequest) returns (ReadLocalResponse);

    rpc GetClusterInfo(Empty) returns (ClusterInfo);
}

message ReadLocalRequest {
    uint64 partition_id = 1;
    bytes payload = 2;
}

message ReadLocalResponse {
    bytes payload = 1;
}
```

`ClientRequest` 不再有 `read_only` 字段。两个 RPC 的路由逻辑互不干扰：
- 调用 `Request` → 服务端必然走写路径（Raft commit → apply）
- 调用 `ReadLocal` → 服务端必然走读路径（本地 SM 直接响应）

不存在"设错布尔值导致静默故障"的可能性。

## Locking Model for Concurrent Reads

### 问题背景

`ServiceStateMachineAdapter<S>` 持有 `service: Arc<TokioRwLock<S>>`。  
apply（写路径）和 stale read（读路径）都会调用 `service.handle()`。

`Service::handle()` 签名是 `&self`（共享引用），意味着它是线程安全的只读操作。

### 锁的真正目的

**apply 和 stale read 之间不需要互斥**（两者都持读锁，可以完全并发）。

锁存在的原因是保护**少数需要独占访问的操作**：

| 操作 | 锁类型 | 原因 |
|---|---|---|
| `apply()` 热路径 | `read()` | `handle(&self)` 是共享引用，并发安全 |
| Stale read | `read()` | 同上，与 apply 完全并发 |
| `on_leader_elected()` | **`write()`** | `&mut self`，leader 切换时修改 service 状态 |
| `on_step_down()` | **`write()`** | 同上 |
| `shutdown()` | **`write()`** | 释放 leader-only 资源 |
| `restore()` | **`write()`** | 整体替换状态，必须独占 |

**关键约束**：lifecycle hooks（leader 切换）持写锁期间，正在进行的 apply 和 stale read 必须等待。这是正确的语义——leader 切换时不应有并发的业务处理。

### 为什么不用 `Arc<S>`？

`Arc<S>`（无锁）适用于 apply + stale read（两者都是 `&self`），但 lifecycle hooks 需要 `&mut S`：

```rust
async fn on_leader_elected(&mut self, group_id: GroupId);  // &mut self
async fn on_step_down(&mut self, group_id: GroupId);       // &mut self
async fn restore(&mut self, data: &[u8]);                  // &mut self
```

这些方法从 metrics_ticker（独立 tokio task）调用，无法通过 apply 的串行性保证。

`Arc<TokioRwLock<S>>` 是最简方案：
- apply 和 stale read：`read()` lock（并发，~100-500ns overhead）
- lifecycle hooks 和 restore：`write()` lock（独占，极少触发）

### 性能分析

| 场景 | 实际阻塞 | 原因 |
|---|---|---|
| apply 与 stale read 并发 | ❌ 不阻塞 | 两者都是 read lock |
| 多个 stale reads 并发 | ❌ 不阻塞 | 都是 read lock |
| apply 与 leader 切换并发 | ✅ 阻塞（正确） | write lock 等待 read lock 释放 |
| stale read 与 leader 切换并发 | ✅ 阻塞（正确） | 同上 |

**结论**：`Arc<TokioRwLock<S>>` 的开销只在 apply 热路径（~100-500ns/entry 的 read lock 获取），不影响读写并发性。

## Alternative: Channel-based lifecycle hooks

更激进的优化方案：
- `service: S`（无 Arc，无锁）
- Lifecycle 通知通过 channel 发给 apply loop，在 apply 之间的间隙串行处理

优点：apply 热路径完全无锁
缺点：增加代码复杂度，lifecycle 事件有额外延迟

当前选择（RwLock 方案）简单可靠，在 100k ops/sec 目标下 ~100-500ns 开销可接受。
如果未来需要更极致的性能，可以迁移到 channel 方案。

## Consequences

### Positive
- 读写路径类型安全，编译期保证，无法误用
- stale read 与 apply 真正并发（两者都是 read lock）
- lifecycle hooks 正确互斥（write lock 保护 &mut self）

### Negative
- 多一个 gRPC RPC（`ReadLocal`），API surface 略增加
- 只读和只写客户端需要使用不同方法

### Neutral
- RrClient SDK 提供 `read()` 和 `send_request()` 两个独立方法，调用方不接触 proto 细节
- `Arc<TokioRwLock<S>>` 开销可接受，有明确的升级路径（channel 方案）

## Changelog

| Date | Change |
|------|--------|
| 2026-05-09 | Initial: boolean flag → separate RPC; document locking model |
| 2026-05-13 | Status → `implemented`. `ClientService.Request` (write) and `ClientService.ReadLocal` (stale read) already shipped separately with the `read_only` flag removed from `ClientRequest`; the `Arc<TokioRwLock<S>>` locking model in `ServiceStateMachineAdapter` is already in place. This session adds the missing **linearizable read** wire-up: new `ClientService.ReadLinearizable` RPC (re-uses `ReadLocalRequest` / `ReadLocalResponse`), `ClientDispatch::submit_linearizable_read` trait method + production impl that calls `RaftGroupManager::linearizable_read` (existing openraft `ReadPolicy::ReadIndex` + `await_ready` plumbing), and SDK methods `RrClient::read_linearizable` / `read_linearizable_blocking` / `read_linearizable_reactive`. Two new RPC-handler unit tests assert that `ReadLocal` and `ReadLinearizable` route to the correct dispatch methods. |
| 2026-05-13 | **Follower-redirect fix** (Oracle review). Initial wire-up dropped openraft `LinearizableReadError::ForwardToLeader` on the floor — non-leader replicas would error instead of redirecting. Fixed by: (1) extending `RaftError::NotLeader` with `leader_addr: Option<String>`; (2) `RaftGroupManager::linearizable_read` now pattern-matches the openraft error and returns `NotLeader { leader, leader_addr }` populated from `ForwardToLeader.leader_node.addr`; (3) `ReadLocalResponse` proto gains `optional LeaderInfo leader_info = 2` (wire-additive); (4) `ClientDispatch::submit_linearizable_read` return type changed to `LinearizableReadResult { payload, leader_node_id, leader_addr }`; (5) `RrClient::read_linearizable` honours `leader_info` redirects by dialing the indicated leader on the next retry attempt. New unit test `test_read_linearizable_surfaces_leader_redirect_to_proto` validates the follower-dispatch → proto-redirect plumbing. **Outstanding**: a 3-node integration test calling `read_linearizable` against a known follower (Oracle's P1) is not in this change. Status remains `implemented` because the code path is now structurally correct and unit-covered; the integration gap is tracked as a follow-up. |
