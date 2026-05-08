---
title: "rr 框架测试补强需求"
aliases: [rr-testing, rr-test-requirements]
tags: [adr, testing, rr, raft, distributed-systems]
up: "[[00-moc/architecture]]"
related: ["[[design-decisions]]", "[[../research/2026-05-07-p007-testing-strategy]]"]
status: active
created: 2026-05-07
updated: 2026-05-07
---

# rr 框架测试补强需求

> 本文档由 my-biz（CEX 业务项目）整理，供 **rr 项目**参考实施。
> 记录 CEX 业务侧对 rr 框架的可靠性假设，以及建议 rr 补强的测试方向。

---

## 背景

rr 是 CEX 业务的核心框架（openraft multi-Raft 封装）。CEX 侧视 rr 为黑盒，依赖 rr 保证：

- Raft 共识正确性（线性一致）
- exactly-once 语义（不重复 apply）
- 事件可靠投递（Outbox → Kafka at-least-once）
- Snapshot/restore 一致性

这些保证若失效，业务层的财务 invariant 测试也无从建立。

---

## 一、CEX 对 rr 的业务假设（契约）

以下 6 条假设是 CEX 代码正确性的前提。**rr 必须保证**，CEX 侧会写契约测试（L2）验证。

### C-001：`Service::apply` 幂等性

**假设**：同一个请求（由 `client_oid + account_id` 唯一标识）无论 apply 多少次，业务状态只变化一次。

**典型触发场景**：
- Raft leader 在 apply 后、响应 client 前 crash，client 重试
- 网络重传导致同一条 Raft log 被 apply 两次（理论上不应发生，但需验证）

**验证方法**：同一请求提交 N 次，结果与提交 1 次一致。余额、仓位、订单状态均不应翻倍。

---

### C-002：`HandleResult` 正确传播

**假设**：`Service::apply` 返回的 `HandleResult` 与 client 收到的响应完全一致，不丢失、不替换、不乱序。

**典型触发场景**：
- leader 切换发生在 apply 之后但在响应 client 之前
- pipeline 满时背压下的响应顺序

**验证方法**：并发提交 N 个请求，每个请求的响应内容与 apply 返回值严格匹配。

---

### C-003：Outbox at-least-once 投递

**假设**：所有写入 Raft state 的 Outbox 事件，在任何故障（leader crash、Kafka broker 宕机、网络分区）后，最终必投递到对应 Kafka topic，不丢失。

**典型触发场景**：
- Outbox tailer 在 leader 切换时丢失游标
- Kafka broker 短暂不可用期间的消息堆积和重投
- Raft snapshot 压缩后 Outbox 历史被截断

**验证方法**：注入各类故障，统计 apply 产生的事件数 vs Kafka 收到的消息数，二者一致（允许重复，不允许丢失）。

---

### C-004：Snapshot/restore 状态一致性

**假设**：节点从 snapshot 恢复后，其业务 state（订单、余额、仓位、dedup 状态）与 crash 前完全一致，bit-by-bit 相同。

**典型触发场景**：
- 节点磁盘故障后用 snapshot 重新加入集群
- 新节点通过 snapshot install 追赶进度
- 压缩后的 snapshot 仍包含完整 dedup 窗口

**验证方法（参考 TigerBeetle StorageChecker）**：两个节点分别经历不同故障路径后达到相同 commit index，对比 snapshot 文件 checksum 或 state digest 应完全一致。

---

### C-005：`client_oid` dedup 跨重启有效

**假设**：`(account_id, client_oid)` 的 dedup 状态持久化在 Raft state 中。leader 切换或节点重启后，dedup 仍然有效——已处理的 client_oid 不会被重复 apply。

**dedup TTL 约定（来自 D-015）**：`open` 期间 + terminal 后 1 分钟。

**典型触发场景**：
- client 在 leader 切换窗口内重试同一 client_oid
- snapshot 压缩导致过期 dedup 记录被清理（应在 TTL 后清理，不应提前）

**验证方法**：提交请求 → kill leader → client 重试（同 client_oid） → 验证结果仍为原始 apply 的结果，而非重复执行。

---

### C-006：跨 shard 路由表同步正确性

**假设**：`AccountId → ClearingShardId` 和 `AccountId → MatchingShardId` 的路由表，在 Matching shard 接收到路由更新指令后，后续所有跨 shard 消息均路由到正确 shard，不出现旧路由导致的消息投错 shard。

**典型触发场景**：
- 系统初始化时路由表为空，首批订单到达
- 账户重新分片（shard 扩容）时的路由切换窗口
- 路由更新指令在 Raft log 中乱序 apply

**验证方法**：模拟路由表变更，验证变更后的所有消息均落在新 shard，无消息落在旧 shard 或被丢弃。

---

## 二、建议 rr 补强的测试方向

### T-001：确定性模拟（Deterministic Simulation Testing）

**优先级**：P0

**起点**：openraft 已在 2026-04 合并 turmoil 支持（PR #1714，`tests-turmoil` crate）。

**建议扩展**：
- 在 openraft tests-turmoil 基础上，增加 rr 业务层（Service trait、HandleResult、Outbox）的模拟
- 实现 VOPR 风格双模式：
  - Safety 模式：均匀随机故障注入，验证 invariant 不违反
  - Liveness 模式：选定 quorum core 并 heal，冻结其余故障，验证系统最终收敛
- seed 可保存，CI 可复现
- 建议 24/7 跑，不同 seed 持续探索状态空间

**工具**：`turmoil`（Raft 层）+ `madsim` + `madsim-rdkafka`（包含 Kafka 的全栈模拟）

---

### T-002：Snapshot/restore 一致性验证

**优先级**：P0

**参考**：TigerBeetle `StorageChecker`——两个节点经不同故障路径达到相同 commit index 后，state digest 必须 bit-by-bit 相同。

**建议**：
- 为 rr 的 snapshot 格式（bincode）实现 `state_digest()` 或 `checksum()`
- 在模拟测试中，任意两个 caught-up 节点的 digest 相同为 invariant
- 覆盖场景：normal snapshot、crash-during-install、snapshot + incremental log

---

### T-003：Outbox 投递完整性验证

**优先级**：P0

**场景**：
- leader crash 在 Outbox tailer 扫描游标之前
- Kafka broker 不可用，重投后顺序是否保证
- Raft snapshot 压缩后，未投递的 Outbox 事件是否仍可追回

**验证指标**：`produced_count == consumed_count`（at-least-once），无消息丢失；允许重复（由消费者幂等处理）。

---

### T-004：Jepsen 线性一致性验证

**优先级**：P1（发布前周期性运行，非持续 CI）

**工具**：Jepsen + Elle checker，或自研轻量 checker（记录 op history → 验证 read-your-writes + monotonic reads）。

**场景**：
- 5 节点集群，随机 partition + kill + pause
- 验证：任何 committed write 对所有后续 read 可见
- 验证：没有 stale read 在 leader 切换后出现

---

### T-005：Raft 关键场景覆盖

**优先级**：P1

以下场景在 DST 中必须覆盖（参考 Redis-Raft Jepsen 报告的常见 bug 来源）：

| 场景 | 验证点 |
|---|---|
| Leader 在 commit 后、apply 前 crash | exactly-once：新 leader apply 同一 log，结果不翻倍 |
| 网络分区导致双 leader 短暂并存 | 旧 leader 的 write 不被 commit，不影响 state |
| Follower 极慢（IO stall） | Cluster 继续 commit，不因单 follower 停滞 |
| 新节点通过 snapshot install 追赶 | State 与已有节点一致，后续可正常参与 Raft |
| Leader 在发送 AppendEntries 后 crash | Follower 不重复 apply 同一 entry |
| 时钟偏移（clock skew） | 选举 timeout 不因 skew 导致频繁 leader 切换 |

---

## 三、优先级汇总

| 编号 | 内容 | 优先级 |
|---|---|:---:|
| C-001 ~ C-006 | CEX 契约假设（rr 需保证，CEX 侧会验证） | P0 |
| T-001 | 确定性模拟（turmoil + madsim） | P0 |
| T-002 | Snapshot/restore 一致性 | P0 |
| T-003 | Outbox 投递完整性 | P0 |
| T-004 | Jepsen 线性一致性 | P1 |
| T-005 | Raft 关键场景覆盖 | P1 |

---

## Changelog

- **2026-05-07**: 初始版本。由 my-biz P-007 讨论产出，记录 CEX 对 rr 的 6 条业务假设 + 5 个 rr 测试补强方向。
