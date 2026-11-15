# formal/tla — Rr 自有协议的 TLA+ 形式化(R-2)

> 工作项:`.sisyphus/plans/rr-reliability-program.md` R-2。
> 范围:**只验证 Rr 在 openraft 之上自建的扩展协议**;openraft 核心(leader 选举、
> 日志复制、joint consensus)属上游已验证范围,不重复。

## 目录

| 文件 | 协议 | 实现锚点 | 不变量 | 小模型 TLC |
|---|---|---|---|---|
| `MembershipGuard.tla/.cfg` | change-membership quorum guard(ADR A-0012) | `crates/rr-server/src/admin.rs` `change_membership`、`crates/rr-core/src/membership.rs` | INV-7 | 11,545 态零反例 |
| `OffsetExactlyOnce.tla/.cfg` | offset→request_id + 幂等 watermark/LRU(ADR A-0003) | `crates/rr-ingestor-kafka/src/lib.rs` `kafka_offset_to_request_id`、`crates/rr-raft/src/idempotency.rs` | INV-3 | 558 态零反例 |
| `OutputWatermark.tla/.cfg` | outbox watermark(ADR A-0007) | `crates/rr-raft/src/service_adapter.rs`、`output_runner.rs` | INV-4 | 280,478 态零反例 |
| `RoutingTableSwitch.tla` + 双 cfg | 路由表切换(INV-6;atomic/incremental 两种 watch 语义) | `crates/rr-router/src/router.rs`、`crates/rr-config-etcd/src/lib.rs` | INV-6 | 1,411 / 4,731 态零反例 |
| `bugs/` | 故意植入的反例变体(不进 CI 绿集) | 见下"植入反例实证" | — | TLC 均抓到 |

每份 spec 头部注释含 spec ↔ 实现 file ↔ INV 的三向链接。

## 本地运行

```bash
# 一次性安装(约 2.2MB):tla2tools.jar 放到任意稳定路径
curl -sL -o ~/.kimi-code/tools/tla2tools/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar

cd formal/tla
java -XX:+UseParallelGC -jar ~/.kimi-code/tools/tla2tools/tla2tools.jar -deadlock MembershipGuard.tla
java -XX:+UseParallelGC -jar ~/.kimi-code/tools/tla2tools/tla2tools.jar -deadlock OffsetExactlyOnce.tla
java -XX:+UseParallelGC -jar ~/.kimi-code/tools/tla2tools/tla2tools.jar -deadlock OutputWatermark.tla
java -XX:+UseParallelGC -jar ~/.kimi-code/tools/tla2tools/tla2tools.jar \
  -deadlock -config RoutingTableSwitch.cfg RoutingTableSwitch.tla
java -XX:+UseParallelGC -jar ~/.kimi-code/tools/tla2tools/tla2tools.jar \
  -deadlock -config RoutingTableSwitchIncremental.cfg RoutingTableSwitch.tla
```

约定:

- **`-deadlock`**:四个模型都是有限事件预算(`MaxRequests`/`MaxEvents`/`MaxSteps`/`MaxRoutes`),
  预算用尽即正常终止——死锁检查在此语义下无意义,故关闭。
- **小状态空间是刻意的**(节点 ≤3、事件 ≤8):TLC 穷举所有交错,小模型已覆盖协议交错的核心;
  放大常数(本地手动)主要涨运行时间,不涨覆盖维度。
- **状态约束写在 spec 里**(`StateConstraint`),cfg 里 `CONSTRAINT` 引用;改预算只动 cfg。

## 植入反例实证(R-2 验收第 4 条)

两个 buggy 变体(`bugs/`,不进 CI 绿集),TLC 均在亚秒级抓到反例:

1. **`bugs/MembershipGuardBuggy.tla`** —— 移除 quorum guard 的拒绝路径(review P0#7 修复前的真实
   形态:低于 MinVoters 的非空目标无 force 也提交)。TLC 抓到 `QuorumLossOnlyWithAudit` 违反,
   反例 3 步:`committed={n1,n2,n3} → Submit(target={n1}, force=FALSE) → Resolve → committed={n1},
   forcedCommitted=FALSE, auditLog={}`(2026-09-10 实测)。
2. **`bugs/OffsetClampBug.tla`** —— 负 offset 哨兵 clamp 到 0(review P0#6 的原始缺陷形态:
   `i.max(0)` 使哨兵占用真实首条消息的幂等身份)。TLC 抓到 `ResponseBelongsToGenuine` 违反
   (2026-09-10 实测)。

这两个反例恰好是 8-16 review 的两个 P0 的模型化——spec 对"如果当时没有修复"给出了
可穷举的回归护栏。

## 性质与实现的对照

### MembershipGuard(INV-7)

- S1 `NoEmptyCommitted`:n→0 无条件拒绝(`admin.rs` empty 检查,force 不可绕过)。
- S2 `QuorumLossOnlyWithAudit` / S3 `AuditComplete`:低于下限 ⇒ 必有 force + 先写审计
  (`admin.rs` 的 `force_accepted` warn 在 apply 之前)。
- 活性:请求有界耗尽后无悬挂裁决。
- 模型边界:openraft full-replacement 语义使 guard 决策只依赖目标大小(无 TOCTOU 组合)——
  spec 把这层论证形式化;joint consensus 内部不建模。

### OffsetExactlyOnce(INV-3)

- S1 `ExactlyOnceApply`:任一 offset 至多 apply 一次。
- S2 `ResponseBelongsToGenuine`:缓存响应永远属于对应 offset 的真实消息(哨兵不得污染 id 0)。
- S3 `WatermarkIsMaxApplied`:watermark = 已 apply 的最大 offset(record 处 max 推进的直接语义)。
- S4 `NoFalseExpired`:Expired 绝不落在从未 apply 的 offset 上(否则 = 静默丢失)。
- 活性:终止形态仅三种(流读完 / fail-closed / 预算用尽);永续 redelivery 不前进被
  `WF(DeliverNext)` 排除——对应 ingestor 前向消费纪律。
- 到达纪律(关键假设,与实现一致):单分区有序 + 已 apply 者可重复 + 哨兵 fail-closed。

### OutputWatermark(INV-4)

- S1 `NoPhantomAdvance`:`seq ≤ watermark ⟹ 已真实投递`(不静默丢失)。
- S2 `InOrderCursor`:inflight ∈ {0, watermark+1}(按序遍历 pending 的直接形式化)。
- 重复是承诺语义:Failover 落在 SendOk 与 Advance 之间 ⇒ 新 runner 从 watermark+1 重发
  ⇒ `deliveries[s] > 1` 合法;唯一违规是"未投递却 advance"。
- 活性:公平 Advance/Respawn 下 watermark 最终追平已产生序列。

### RoutingTableSwitch(INV-6)

- S1 `RouteDeterminedByView`:同 (node, partition, view 版本) 的路由结果唯一(单归属)。
- S3 `NoStaleRouteAfterConverge`:收敛后无旧版本路由。
- 活性:切换提交后所有节点最终收敛(WF watch)。
- 双 cfg 分别验证 MemConfigProvider(整表原子翻页)与 rr-config-etcd(按 key 增量,
  reassign 为 upsert、无删除中间态)两种传播语义;混合 view 窗口期的"旧/新组并存路由"
  是承诺语义(收敛前允许),不判违规。

## CI

`.github/workflows/formal.yml`:每周日 05:29 UTC + `workflow_dispatch`,非阻塞
(不向 PR 门禁收敛之前不 blocking)。runner 下载 tla2tools.jar 跑全部 5 个 cfg;
失败上传 TLC 输出 artifact,人工分类为"spec 滞后于实现"(更新 spec)或"实现 bug"
(开 issue 转 P 级)。

## 漂移防护

R-1 执法链路延伸:改动 `crates/rr-server/src/admin.rs` 的 change_membership /
`crates/rr-raft/src/idempotency.rs` / `crates/rr-raft/src/{service_adapter,output_runner}.rs`
的 outbox 段 / `crates/rr-router/src/router.rs` / `crates/rr-config-etcd/src/lib.rs` 的 PR,
reviewer 需确认对应 spec 是否仍成立(PR 模板已有检查项,见 R-1)。
