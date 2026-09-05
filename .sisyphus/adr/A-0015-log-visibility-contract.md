---
title: "Raft log visibility contract: submitted ⇒ visible(pending overlay + flush barrier)"
status: implemented
created: 2026-09-06
updated: 2026-09-06
tags: [adr, durability, io-batcher, openraft, correctness, crash-recovery]
---

# A-0015 — Raft 日志可见性契约:append 提交即可见

> 发现：R-4 crash harness(`tests/crash/`,真 SIGKILL/SIGTERM + ack-log oracle)。
> 修复前置:IoBatcher 泛化 `Arc<dyn StorageEngine>`(R-4 选项 a,已落地)。
> 关联:A-0011(shutdown durability 契约)、INV-1(`docs/docs/reference/invariants.md`)。

## Context

### IoBatcher 异步 append 管线(既有设计)

`RocksLogStorage::append(entries, IOFlushed)` 把 ops 投入 per-group `IoBatcher`
通道后**立即返回**(openraft pipeline 支持);worker 在 500µs 窗口内聚合多个
append,单次 `write_batch`(Strict = fsync)落盘后逐一触发 `IOFlushed` 回调。
耐久性信号走回调,不走 append 返回值——这本身是正确且经过 benchmark 验证的
(group commit 实测 +26% TPS)。

### openraft 0.10.0-alpha.34 的提交门控(已核 registry 源码)

`Engine::next_progress_driven_command`(`engine/engine_impl.rs:831-872`)计算可
apply 前沿:

```rust
let applicable_upto = log_submitted.min(apply_accepted);
//                  ^^^^^^^^^^^^^^^^ log_progress.SUBMITTED(非 flushed)
```

`log_progress.submit(io_id)` 在 `RaftCore::run_append_entries` 里先于
`log_store.append()` 调用(`core/raft_core.rs:2310-2326`),注释明确:
"other commands relying on it can then be processed. For example, `Replicate`
command cannot run until this IO request is submitted **(no need to be flushed)**,
because it needs to **read the log entry from the log store**"。

即上游契约是:**`append()` 返回 Ok 之后,随后的 `try_get_log_entries` 必须能读
到这批 entry**(可见性同步于 submit;耐久性异步于 flush)。sm worker 在 debug
构建用断言执法该契约(`core/sm/worker.rs:211`:`assert_eq!(end - 1,
got_last_index)`),release 构建无此断言。

## Problem

R-4 harness 首跑即在两个场景抓到同一 panic(种子 `4242424242`,3 节点,
Strict durability):

```
sm/worker.rs:211  assertion `left == right` failed
  scenario-b node1.boot2: left=121,  right=120   (重启追平窗口)
  scenario-a node2.boot1: left=156,  right=0     (正常负载突发,未杀该节点)
  scenario-a node3.boot2: left=137,  right=0
  scenario-b node2.boot1: left=161,  right=0
```

机制:`append` 入队即返回 → RaftCore 立即把 `log_progress.submitted` 推进到
新前沿 → `next_progress_driven_command` 生成 `SaveCommittedAndApply` →
sm worker `entries_stream(since..end)` 读 RocksDB —— 而 entry 还在 batcher
队列里(最长一个窗口)。`right=120` 是短读一截,`right=0` 是整个 catch-up
区间全在队列里。

后果分级:

- **debug 构建**:断言 panic 杀死 sm worker task → 之后所有 apply 报
  "channel closed" → 该节点 apply 永久冻结(gRPC 仍服务,假活)。harness 的
  oracle 因此无法区分"数据丢失"与"节点假死",scenario 失败。
- **release 构建**(断言被 `cfg(debug_assertions)` 编译掉):`entries_stream`
  短读被静默接受,`ApplyResult.last_applied` 却按命令前沿记账 —— **entry 被
  永久跳过 apply**,副本间状态机静默分叉;未匹配的 client responder 被 drop,
  对应写永不应答(客户端超时)。这是比 panic 更危险的静默正确性漏洞。
- 附带同根 race:`truncate_after`/`purge` 走**直连** `write_batch` 删除,不经过
  batcher;已入队未 flush 的 append 可在 delete 之后落盘 → **被截断/清除的
  entry 复活**。

## Options

### A. 同步化 append(等 flush 再返回)

正确性最直白,但 RaftCore 串行 await append → batcher 永远看不到并发 →
group commit 名存实亡,实测吞吐退回 ~26% 增幅之前。违背项目"极高吞吐"目标。
**否决。**

### B. 读路径 miss 时等 flush(flush-on-read)

`try_get_log_entries` 发现尾部缺口时向 batcher 查询已受理前沿并等待 flush
后重扫。问题:开放区间(`120..`)无法区分"没有更多 entry"与"entry 在队列里",
需要额外的前沿簿记;且热读路径(apply 逐批)每次都耦合 flush 延迟。复杂度
不低、收益不完整。**否决。**

### C. pending overlay + flush barrier(采纳)

读侧:`RocksLogStorage` 持 `Arc<Mutex<BTreeMap<u64, RrEntry>>>` 覆盖层,
`append` 先入覆盖层再提交 batcher;`try_get_log_entries` **先克隆区间覆盖层快照、
再扫 RocksDB、后合并**(覆盖层赢);batcher flush 完成后经 post-flush hook
精确摘除对应 index。可见性恢复为 submit 语义,写路径零等待。

**快照序的完备性论证**(次轮发现,harness 仪器化抓到):RocksDB 迭代器在创建时
钉住一致视图,扫描开始后完成的 flush 对该次扫描不可见;而 hook 摘除与扫描并发。
 naive 的"先扫后并"存在窗口:迭代器创建早于 flush、merge 晚于 hook 摘除 → entry
两源皆失(实证:`lo=923 hi=925 → first=924`,node1 复制流断言命中)。
克隆-在-扫描-前使两源互补:快照前被摘除的 entry 其 flush 必已完成,对快照后创建的
迭代器可见;快照后被摘除的 entry 仍在快照里。范围头部的 entry 先于范围内任何更新
entry 完成 staging(同一 RaftCore 顺序执行),故头缺口不可能;尾部缺口退化为前缀读,
openraft 复制层按心跳处理并重试。

写侧次序:`IoBatcher` 新增 `flush_barrier()`(有序通道上的读己所提交屏障),
`truncate_after`/`purge` 先过屏障再做直连删除,并同步清理覆盖层 —— 消除
entry 复活 race。

## Decision

采纳 Option C,全部落在既有类型内部,公开 API 只增不破:

1. **`IoBatcher`**(`crates/rr-raft/src/io_batcher.rs`):
   - `BatchCompletion::Hook(Box<dyn FnOnce(Result) + Send>)` —— flush 后与
     `IOFlushed` 同结果触发的动作(成功/失败路径都触发,失败时 entry 永不
     落盘,同样必须摘出覆盖层);
   - `submit_with_hook(ops, callback, hook)` —— append 专用双完成通道;
   - `BatchItem::Barrier` + `flush_barrier()` —— 屏障返回时,先于它入队的
     全部 item 已 flush;close 排空序列同样应答屏障;worker 已退出时返回
     `Err`(语义:不再有队列项可超车,调用方可安全继续);
   - **Phase 3 闸门修正**:原 `completions.len() == 1` 在 hook 引入后(每个
     append 携带 2 个 completion)会让 500µs 聚合窗口对 append 路径完全失效
     ——改为按 submission(BatchItem::Ops 条数)计数。
2. **`RocksLogStorage`**(`crates/rr-raft/src/rocks_log_storage.rs`):
   - 新私有字段 `pending: Arc<Mutex<BTreeMap<u64, RrEntry>>>`,Clone 共享
     (openraft 经 `get_log_reader()` 的 clone 读,append 经原句柄写);
   - `append`:先入覆盖层 → `submit_with_hook`(失败路径原地摘除,防 stale);
   - `try_get_log_entries`:扫描 + 覆盖层合并(覆盖层为空时零开销快路径);
   - `get_log_state`:尾部的 last_log_id 纳入覆盖层(启动时覆盖层恒空,此处
     仅为契约完整性);
   - `truncate_after`/`purge`:先 `await_batcher_drain()`(屏障失败仅 warn,
     worker 不在即无超车可能)→ 清理覆盖层相应区间 → 直连删除。
3. **回归测试**:
   - `io_batcher`:屏障覆盖先提交项/空闲即返/close 中应答;hook 成功与错误
     路径各触发恰好一次(后者经 `FaultInjectingStorage` torn-write 注入);
   - `rocks_log_storage`:`test_append_visible_before_flush_via_overlay`
     (5s 窗口下 append 后立即可读 + flush 后覆盖层摘除)、
     `test_truncate_after_does_not_resurrect_pending_appends`、
     `test_purge_does_not_resurrect_pending_appends`;
   - 验收终证:`tests/crash` scenario A(SIGTERM)+ B(3 轮 SIGKILL)在修复后
     全绿,节点日志零 panic。

## Consequences

- openraft submit 门控契约在 rocks 日志路径上恢复;debug 断言不再误杀
  sm worker,release 构建的静默跳 apply 风险同源消除。
- 热路径成本:append 多一次 BTreeMap 插入(Mutex 短临界区)+ 一次 Box 分配;
  读路径在覆盖层为空(稳态)时零额外开销。group commit 窗口行为经 Phase 3
  闸门修正后与 hook 引入前完全一致,benchmark 预期无回归(R-7 基线对账)。
- `truncate_after`/`purge` 增加一次屏障等待(≤ 当前队列排空),两者皆为
  冷路径(换主/快照压缩),无感。
- 覆盖层内存有界:通道深度(512)× 单批上限(256)entry,且随 flush 即摘。
- worker panic 的极端情形:覆盖层残留由进程级假死兜底(harness 能观测),
  不额外处理 —— 与 A-0011 "batcher 关闭失败不得原地重启 group" 一脉相承。
- 对上游的提示:该 alpha 版断言实质是存储契约的 debug 执法器;升级 openraft
  时若断言语义变化,以本 ADR + 回归测试为准复核。

## Changelog

| Date | Change |
|---|---|
| 2026-09-06 | Created (status `implemented`):R-4 harness 首跑发现 → 同 session 修复 + 回归测试 + 场景复跑验证 |
