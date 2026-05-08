---
title: "rr Framework Deep Review"
tags: [research, quality, architecture, rr]
status: active
created: 2026-05-08
updated: 2026-05-08
---

# rr Framework Deep Review

> 系统性框架审查报告。基于 explore agent 三轮并行分析 + 项目深度历史知识综合产出。

## 一、框架能力（截至 2026-05-08）

| 能力 | 状态 | 说明 |
|---|---|---|
| Multi-Raft 共识 | ✅ 成熟 | openraft 0.10.0-alpha.19，leader 选举+日志复制+快照 |
| 存储可插拔 | ✅ 成熟 | MemStorage（测试）+ RocksDB（生产，feature-gated） |
| gRPC 网络层 | ✅ 成熟 | tonic 0.14，TLS/mTLS，bearer-token，rate limiting |
| 客户端 SDK | ✅ 成熟 | Rust + Java，auto leader-redirect，idempotent SDK |
| 框架级幂等 | ✅ 成熟 | (request_type, request_id) dedup，随 snapshot 持久化 |
| Kafka 输入 | ✅ 成熟 | rr-ingestor-kafka，leader-exclusive，auto-idempotency |
| 快照一致性 | ✅ 成熟 | FramedSnapshot magic+version，自动迁移 |
| 可观测性 | ✅ 成熟 | 18 metrics，Grafana dashboard，alerts，W3C traceparent |
| etcd ConfigProvider | ✅ 成熟 | ADR A-0004，read-only，测试覆盖 |
| 运维 CLI | ✅ 成熟 | learner/membership/backup/decommission/node-reset |
| Follower Read | ✅ 新增 | stale (local SM) + linearizable (ReadIndex, openraft 0.10) |
| 安全加固 | ✅ 成熟 | 5-gate startup，mTLS，bearer，rate limit，audit log |
| 混沌测试 | ✅ 成熟 | 双向隔离，flapping/partition 场景 |
| Per-caller 限流 | ✅ 新增 | DashMap-based，TTL eviction，ADR A-0005 |
| RocksDB CF 调优 | ✅ 新增 | Per-CF options（raft_log vs state_machine），ADR A-0006 |

## 二、已知缺陷

### P0 — 数据损失 / 级联崩溃

#### P0-1: Lock Poisoning 在热路径（已记录 fix 位置）

如果任意线程 panic 时持有以下锁，所有后续调用均 panic：

| 文件 | 行 | 问题 |
|---|---|---|
| `crates/rr-raft/src/service_adapter.rs` | 105, 111, 149, 159 | ingestor offset RwLock `.expect()` |
| `crates/rr-network/src/rate_limit.rs` | 231, 235, 245 | per-caller 限流 std::RwLock `.unwrap()` |

**修复**：`.unwrap_or_else(|e| e.into_inner())`

#### P0-2: RetryConfig 指数退避溢出

```
crates/rr-client/src/lib.rs:435,484,485,513,514,558,559,609,610
2u64.pow(attempt)  →  当 attempt>=64 时 panic
```

**修复**：`2u64.saturating_pow(attempt.min(20))`

#### P0-3: unreachable!() 在 ingestor poll loop

```
crates/rr-raft/src/ingestor_runner.rs:496  — test mock unreachable!()
```

Test mock 代码，task 取消时可能触发，导致 supervisor 崩溃。

### P1 — 生产崩溃风险

#### P1-1: Ingestor 无批量提交（吞吐天花板）

Kafka batch 100 条但逐条 `raft.client_write()`，实际吞吐 ~1000 msgs/sec/group（vs 理论 10k+）。

**Fix**: `RaftGroupManager::write_batch()` + ingestor_runner 批量提交路径。

#### P1-2: Service trait 缺少生命周期钩子

用户必须手动监听 Raft metrics 来感知 leader 变化，无 `on_leader_elected` / `on_step_down` / `shutdown` 标准钩子。`stop_group()` 不等 in-flight 请求完成（客户端看到 `GroupNotFound` 错误）。

### P2 — 设计问题

#### P2-1: GroupManager 7 个独立 RwLock

7 个 `Arc<RwLock<HashMap>>` 串行争锁，`stop_group()` 顺序获取 6 个写锁。
~500 groups 时开始显现，每次 group 操作 +2-5ms。

**Fix**: 合并为单 `Arc<RwLock<GroupState>>` 或 DashMap per-group。

#### P2-2: Proto 错误码非 gRPC-idiomatic

Admin RPC 用 `bool success + string message` 而非 gRPC Status codes，`IdempotencyError` 作为 optional response 字段。客户端只能 parse 字符串判断错误类型。

### P3 — 小缺陷

#### P3-1: Rate Limiter 无后台 GC

`std::sync::RwLock<HashMap>` 仅在写路径触发 lazy eviction，10k caller × 300s TTL 可能导致内存增长。

**Fix**: 后台 tokio task 每 60s 清理过期 entry。

#### P3-2: Backup 无 checksum

`rr-admin backup/restore` 不验证完整性，corrupt backup 被静默 restore。

**Fix**: 在 backup metadata 写入 SHA256 checksum，restore 时验证。

## 三、性能天花板

| 组件 | 天花板 | 根因 |
|---|---|---|
| 单节点写吞吐 | ~100-1000 ops/sec | RocksDB Strict fsync 1-10ms/write |
| Ingestor 吞吐 | ~1000 msgs/sec/group | 每条消息单独 client_write()，无批量 |
| Group 数量 | ~500/节点 | 7 个独立 RwLock 争锁 |
| Per-caller 限流内存 | ~10k callers → OOM | 无后台 GC |
| In-memory 日志 | 取决于 snapshot 频率 | BTreeMap 无上限，生产应用 RocksDB |

## 四、框架边界

**rr 负责**：Raft 共识、日志持久化、快照传输、leader 选举、网络路由、幂等去重、Kafka ingestor、可观测性。

**用户负责**：Service 业务逻辑的并发安全（Service::handle 是 &self，内部需自行同步）、snapshot/restore 的原子性（ADR A-0002）、分片路由策略（MemConfigProvider / etcd）、跨分片事务（rr 不支持 2PC）、动态分片（未实现 split/merge）。

**不适合场景**：
- 高吞吐写密集（>10k ops/sec/group）
- 百万 QPS 读（无缓存层）
- 跨 shard 事务
- 动态分片

## Changelog

| Date | Change |
|---|---|
| 2026-05-08 | 初始版本，三轮 explore agent 并行分析产出 |
