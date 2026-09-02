---
title: "Shutdown durability contract: snapshot 错误传播、offset→request_id fail-closed、IoBatcher 显式生命周期"
status: implemented
created: 2026-08-28
updated: 2026-08-29
tags: [adr, durability, shutdown, snapshot, idempotency, io-batcher, correctness]
---

# A-0011 — Shutdown Durability Contract 与正确性三连

> 工作项：roadmap WS-4(`.sisyphus/plans/rr-v0.2.0-release-roadmap.md`)。
> 依据：全量 review P0 #3/#5/#6 + Oracle Q1/Q5 + P1(raft 核心热路径)(`.sisyphus/evidence/rr-full-review-2026-08-16.md`,HEAD=`ba69cc8`)。
> 实施计划:`.sisyphus/plans/rr-durability-hardening.md`。

## Context

### 写路径数据流(Strict 策略下)

```
client_write → RaftCore → RaftLogStorage::append(entries, IOFlushed)   (rocks_log_storage.rs:214)
                                │  立即返回(pipeline),spawn relay task  (rocks_log_storage.rs:243)
                                ▼
                        IoBatcher channel → worker 聚合成批              (io_batcher.rs:139-203)
                                ▼
                        RocksStorage::write_batch — set_sync(true)      (rocks.rs:518-545, :112-127)
                                ▼
                        callback.io_completed(Ok)  ← fsync 完成点
                                ▼
                        openraft 推进 committed → apply → client response
```

### openraft 0.10.0-alpha.19 契约(版本精确钉死 `= 0.10.0-alpha.19`,已核 registry 源码)

- `RaftLogStorage::append(entries, callback)` 立即返回;`IOFlushed::io_completed`(`callback.rs:80-117`)是 IO 完成信号，**必须恰好调用一次**——未调用则 RaftCore 永不推进 committed，调用两次或携带错误结果分别触发对应上游语义。
- `Raft::shutdown()`(`raft/mod.rs:1743-1758`)向 RaftCore 发 shutdown 信号并 join core task;RaftCore 收到信号后**立即**以 `Fatal::Stopped` 返回(`raft_core.rs:1215-1217`)——**上游契约就是不停下来排空 pending IO**。
- `RaftStateMachine::get_snapshot_builder(&mut self) -> Self::SnapshotBuilder`(`v2/raft_state_machine.rs:124`)签名**无 `Result`**,0.10 起 deprecated;替代者 `try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder>`(`:113-116`,默认委托旧方法)。0.10.0-alpha.19 内部调用点全部走新方法:`sm/worker.rs:244` 以 `force=false` 调用,返回 `None` 时记 `BuildSnapshotDone(None)` 并按 `SnapshotPolicy` 延期重试(`sm/worker.rs:244-256`);`storage/helper.rs:182` 以 `force=true` 调用且对 `Option` **`.unwrap()`**(启动时"snapshot 缺失但 log 已 purge"的重建路径)。错误唯一可传播的通道是 `RaftSnapshotBuilder::build_snapshot() -> Result<_, io::Error>` → `sto_write_snapshot` → `StorageError`。
- 补充核实(任务要求的 SnapshotSignature 核查结论):`SnapshotSignature`(`storage/snapshot_signature.rs:14`)只是 snapshot id 的载体,用于流式复制的去重/取消,**不是错误通道**。一个"空字节 + 合法 meta"的 snapshot 拥有完全合法的 signature,下游无法识别其已损坏——这正是必须在源头 fail-closed 的原因。

### 当前生命周期事实(全部已核源码)

- `IoBatcher` worker 的 `tokio::spawn` 丢弃 `JoinHandle`(`io_batcher.rs:116`);shutdown 语义靠 rustdoc 自述的隐式 drop-drain("Dropping all clones of an `IoBatcher` closes the underlying channel, causing the worker task to drain any pending items and then exit cleanly",`io_batcher.rs:104-110`)。`RocksLogStorage` 持有 `Arc<IoBatcher>` 且 `get_log_reader()` 的每次 clone 都再持一份(`rocks_log_storage.rs:202-204`),worker 实际退出时机取决于**所有** clone 的 drop——不可预测。
- 每次 `append` 再 spawn 一个 untracked relay task 等 batcher 结果后触发 callback(`rocks_log_storage.rs:243-249`)。
- `stop_group`(`group_manager.rs:359-434`)只 drain 在途 client write(`in_flight_writes`,5s deadline,`:405-417`),随后 `raft.shutdown()`——**从不触达 batcher**。`RrNode::shutdown` 顺序停全部 group(`node.rs:640-647`)后进程即可退出,runtime teardown 可取消任何 mid-flush 的 untracked task。
- 好的先例已经存在:`IngestorHandle`/`OutputHandle`/metrics ticker/learner promoter 全部持 `JoinHandle` + 显式 `stop()`(如 `ingestor_runner.rs:102-118`)。IoBatcher 是写路径上唯一不守这个规矩的。

### exactly-once 链(offset→request_id 的上下文)

Kafka at-least-once + 框架幂等去重 = exactly-once apply(ADR A-0001/A-0003)。链条:`AutoIdempotency.offset_to_request_id`(`ingestor_runner.rs:66`,类型 `Box<dyn Fn(&[u8]) -> u64 + Send + Sync>`)把 opaque offset bytes 映射为 `IdempotencyKey.request_id`(`ingestor_runner.rs:391-397`)→ 幂等层按 `(request_type, request_id)` 去重,watermark = 该 type 已应用的最大 request_id,`request_id <= watermark` 且 cache miss → `Expired`(`rr-raft/src/idempotency.rs` 模块文档规则 3)→ adapter 跳过用户服务,返回 `ApplyOutcome::IdempotencyExpired`(`service_adapter.rs:291-341`)→ 但 ingestor 在 `write_batch` 成功后**无条件 commit offset**(`ingestor_runner.rs:415-443`)。

## Problem

### P1 — snapshot 失败静默吞错(review P0 #3)

`get_snapshot_builder` 对 `self.inner.snapshot()` 的错误 `unwrap_or_default()`(`sm_adapter.rs:92-101`,吞错点 `:93`):产生**空字节 + 合法 meta(含 `last_applied`)**的 snapshot。该 snapshot 可被持久化、复制、安装——`install_snapshot` 无内容校验直接 `restore`(`sm_adapter.rs:107-122`)。后果不是"少一个 snapshot",而是 **follower/重启节点的状态机被静默清空**,且 signature 合法、无任何报错。`ServiceStateMachineAdapter::snapshot()` 的失败源是真实的:用户 service snapshot、`idempotency.capture()`、`FramedSnapshot::encode`(`service_adapter.rs:375-402`)。对照组:`get_current_snapshot`(`sm_adapter.rs:124-147`)早已正确传播错误——同文件两条路径,一条 fail-closed 一条吞错。

### P2 — `kafka_offset_to_request_id` 非法 bytes 静默回 0(review P0 #6)

`rr-ingestor-kafka/src/lib.rs:74-78`:bincode decode 失败 → `unwrap_or((0_i64, 0))` → 返回 `0`;负数 offset(Kafka 哨兵值如 `INVALID_OFFSET = -1001`)`i.max(0)` clamp 到 `0`,且有测试把"garbage → 0"固化为行为(`lib.rs:126-129`)。两个方向都击穿 exactly-once:

1. **request_id 回退**:watermark 已 ≥ 0 时,garbage bytes → request_id 0 → `Expired` → 该消息**不被 apply**,但 offset 照常被 commit(`ingestor_runner.rs:434-443`)→ 静默丢消息;
2. **身份碰撞**:负数哨兵 clamp 到 0,与真实 offset 0 的消息撞 `(request_type, 0)`——真实首条消息被误判 Replay/Expired。

### P3 — shutdown durability 靠隐式 drop-drain,不可证明(review P0 #5,Oracle Q1)

Oracle 判定原文:"ACK 边界(fsync 后才 ACK)没被违反,但三个隐式行为叠加使契约不可证明"。三个隐式行为:untracked worker(`io_batcher.rs:116`)、untracked relay(`rocks_log_storage.rs:243`)、`stop_group` 不 await flush。叠加 `Raft::shutdown` 的"立即停止、不排空 IO"上游语义,今天"碰巧安全"的唯一前提是 runtime 在进程退出前活得够久、且所有 `Arc<IoBatcher>` clone 及时 drop——**没有任何代码路径能证明"stop_group 返回时,已接受的 append 全部 fsync 完毕"**。这正是 review 元根因("不变量存在,但没有执法者")在 durability 上的实例。

### P4 — 热路径双高开销(review P1,顺带修复)

每次 `append` 付出一个 `tokio::spawn` + 一对 oneshot(channel)+ 一次 task 调度,只为把 batcher 结果转交给 callback。caller(openraft RaftCore)本来就有自己的完成等待机制(`IOFlushed::Notify` 的 watch channel),relay task 是纯开销。

### 依赖方向审计(Oracle Q5:"offset→request_id 映射是否住错层")

已核 `Cargo.toml`:`rr-raft` 依赖 `rr-core`/`rr-client`/`rr-storage`/`openraft`,**不依赖** `rr-ingestor-kafka`;`rr-ingestor-kafka` 仅依赖 `rr-core`(rdkafka 为 optional feature)。offset 在 rr-raft 内部全程是 opaque `&[u8]`(`DynCommandIngestor` 类型擦除,`rr-core/src/ingestor.rs:12-30` 模块文档),Kafka 特有的 `i64` 解码住在 `rr-ingestor-kafka`,注入点 `OffsetToRequestId` 是 Kafka 无关的闭包类型。**结论:层没有住错——rr-raft 核心代码中没有 Kafka/具体 offset 概念(仅 doc 注释提及);真正住错的是"不可失败的函数签名"(`Fn(&[u8]) -> u64` 无法表达 decode 失败,逼出了静默回 0)。修签名,不动层。**

## Options

### 针对 P3/P4(IoBatcher 生命周期)

**Option A(采纳):显式 `close()` 协议 + tracked spawn + completion 入 BatchItem(删 relay spawn)。**

- `IoBatcher::new` 的 worker spawn 持有 `JoinHandle`(与 `IngestorHandle` 先例一致);
- `BatchItem` 直接携带完成通道(`IOFlushed` 或内部 oneshot 包装),worker 在 fsync 后逐个应答——relay task 与 per-append oneshot 同时消失,P3 与 P4 一并解决;
- `close()`:拒绝新 submit → 排空 channel 内全部已接收 item → 合并最终 `write_batch`(Strict 下即 final fsync)→ **应答全部 pending completion** → 通知 closer → 退出;closer await `JoinHandle`。幂等、并发安全、带 deadline(超时返回 `Err` 并 warn,不 panic);
- `stop_group` 在 `raft.shutdown()` 之后 await 该 `close()` 才返回(顺序:先停 appends 的源头 RaftCore,再排空白斗)。

**Option B(否决):维持隐式 drop-drain + 文档化。**
文档不执法——本次 review 的元根因,不再重复。且 drop 时机依赖全部 `Arc` clone(含 openraft log reader)的析构序,runtime teardown 可在 drain 完成前取消 untracked task;"N 条 pending + stop + restart"测试无法对隐式行为做确定性断言。

**Option C(否决):`Drop` impl 内 `block_on` flush。**
`Drop` 不能可靠执行 async;runtime 关闭中 `block_on` 直接 panic;把 durability 系于析构序与 tokio 语义相悖。

**Option D(否决):依赖 openraft shutdown 自行排空。**
已核源码:上游契约就是 `Fatal::Stopped` 立即返回(`raft_core.rs:1215-1217`),排空责任在存储层——就是我们。

**Option E(否决):保留 relay spawn,但把 `JoinHandle` 收进 JoinSet tracker。**
仍付 per-append spawn + oneshot 开销,不解 P4;completion 入 `BatchItem` 以更低的复杂度同时解掉两者。

### 针对 P1(snapshot 错误传播)

**Option A(采纳):override `try_create_snapshot_builder(force)`,双通道 fail-closed。**

- 抽出 `snapshot_builder_inner() -> Result<RrSnapshotBuilder, io::Error>`(调用 `inner.snapshot()`);
- `force=false`(policy 触发):失败 → error 日志 + `rr_snapshot_build_failures_total` metric + 返回 `None`(延期;openraft 记 `BuildSnapshotDone(None)`,下个触发周期重试,`sm/worker.rs:244-256`);
- `force=true`(启动重建):返回 **poisoned builder**——其 `build_snapshot()` 返回捕获的 `Err`,经 `sto_write_snapshot` 变 `StorageError`,使 `Raft::new` 以可诊断错误失败;
- 旧方法 `get_snapshot_builder`(trait 必填、签名无 `Result`)委托同一 inner:失败同样返回 poisoned builder——**任何路径都不再可能产出"空字节 + 合法 meta"**。

**Option B(否决):`force=true` 也返回 `None`。**
`storage/helper.rs:182` 对返回的 `Option` 直接 `.unwrap()`——返回 `None` 是在 openraft 内部制造 panic,比吞错更糟。

**Option C(否决):adapter 内 retry-with-backoff。**
阻塞 openraft 的 sm worker,且 openraft 已有自己的重试节奏(`SnapshotPolicy` 触发);错误应上抛,由框架层决策。

**Option D(否决):维持 `unwrap_or_default` + 文档警告。**
同 Option B 的否决理由:文档不执法。

### 针对 P2(offset→request_id fail-closed)

**Option A(采纳):签名 Result 化 + runner fail-closed。**

- `kafka_offset_to_request_id(bytes) -> Result<u64, KafkaOffsetError>`;`OffsetToRequestId` 类型别名改为 `Box<dyn Fn(&[u8]) -> Result<u64, OffsetMapError> + Send + Sync>`;
- decode 失败 **与负数 offset** 均为 `Err`(rdkafka 真实消息 offset ≥ 0,负数即哨兵/bug;clamp 到 0 与真实首条消息撞车,见 Problem P2);
- `run_poll_loop` 遇映射失败:不 propose、不 commit offset、supervisor 以 `Err` 退出——沿用同文件 `seed_offset` 失败即退出的 fail-closed 先例(`ingestor_runner.rs:203-213`),把 poison 消息留给 operator 处置而非静默吞掉。

**Option B(否决):skip-and-continue(跳过该消息继续)。**
后续消息的 offset commit 会隐式提交被跳过者(Kafka commit N 语义 = 提交全部 < N)——静默丢数据,比现状只是换了个漏点。

**Option C(否决):park-and-retry(挂起重试同一消息)。**
supervise 主循环在仍为 leader 时立即 re-seed/re-poll(`ingestor_runner.rs:178-231`),poison 消息造成无进展热循环。

**Option D(否决):保留负数 clamp、只修 decode 失败。**
clamp 本身就是 watermark 污染源(与 offset 0 碰撞);合法路径不产负数,fail-closed 对合法流量零影响。

## Decision

采纳上述全部 Option A。核心产出是一份**精确的、可测试的 durability 契约**,三个修复都是它的执法点。

### 1. Durability 契约(精确陈述)

- **C1(ACK 边界)**:`DurabilityPolicy::Strict` 下,写请求向 client 返回 `Ok` ⟹ 该 entry 所在 batch 的 `write_batch` 已完成 fsync(`set_sync(true)`,`rocks.rs:112-127`)。顺序不可逆:enqueue → 聚批 → write_batch(fsync)→ `io_completed` → committed/apply → response。fsync 永远先于 ACK。
- **C2(vote)**:`save_vote` 返回 `Ok` ⟹ vote 已 fsync。vote 走 `storage.put` 直连、有意绕过 batcher(`rocks_log_storage.rs:206-212`)——vote 稀少且不可等 batch window。
- **C3(shutdown 排空)**:`stop_group` 返回 `Ok` ⟹ (a) 在途 client write 已应答或按既有 5s drain 语义放弃(`group_manager.rs:405-417`);(b) RaftCore 已停止,不再接受新 append;(c) batcher 已 `close()`:channel 内全部已接收 append 完成最终 `write_batch`(final fsync)、全部 pending completion 已应答、worker 已退出(join)。全部 group 的 `stop_group` 返回后(`RrNode::shutdown`,`node.rs:640-647`),进程退出是安全的。
- **C4(崩溃语义)**:kill -9 下,`io_completed(Ok)` 已触发的 entry 必须存活(由 C1 的 fsync 顺序保证);未触发者不做承诺——Raft 重启恢复协议对"已持久化但未上报"与"未持久化"两种情形都能正确处理。
- **C5(策略条件)**:C1/C2/C4 仅在 `Strict` 下成立;`ReplicatedOnly`/`Balanced`/`TestOnly`/`Fast` 的弱化是 `rr-storage` 既有文档化行为(`rocks.rs:30-69` rustdoc),部署文档必须复述,不在本 ADR 改动范围。
- **C6(snapshot)**:被持久化/复制/安装的 snapshot,要么来自 `inner.snapshot()` 成功产生的完整字节,要么不存在(build 失败 = `StorageError`/启动失败)。**绝不存在"空字节 + 合法 meta"的 snapshot。**
- **C7(exactly-once 链)**:被 propose 的 ingestor 消息,其 `request_id` 必由 offset 经单调映射**成功**得出;映射失败的消息绝不 propose、绝不 commit offset,supervisor fail-closed 退出。

(C1–C7 是 WS-8 `invariants.md` 的直接输入;每条在本 ADR 与实施计划中标注了强制点与测试。)

### 2. `IoBatcher::close()` 协议

1. closer 调用 `close()`(幂等,并发安全);
2. batcher 进入 closing 状态:新 `submit`/`append` 立即 fail-fast(`Err`,不排队);
3. worker 排空 channel 内全部已接收 item(receiver 侧 close 后 recv 至 `None`);
4. 全部剩余 ops 合并为最终一次 `write_batch`(Strict 下即 final fsync);
5. 逐个应答全部 pending completion——**每个恰好一次**(`io_completed` 契约);
6. 应答 closer、worker 退出;closer await `JoinHandle` 返回;
7. deadline(建议 30s,可配):超时返回 `Err` 并 warn,不 panic、不遗留"半关闭"假象。

`Drop` 路径的 drain 保留为兜底,但**不再是契约的一部分**——契约的唯一执法点是 `close()`。

### 3. `stop_group` 顺序与接线

`RaftNode::new_with_rocks_factory`(`raft_node.rs:135-180`)在把 `RocksLogStorage` 移交给 `Raft::new` 前保留一份 clone(`RocksLogStorage: Clone`,廉价 `Arc`),作为 close handle。`stop_group` 顺序固定为:

```
停 supervisors(ingestor/output/ticker/promoter)
  → drain in_flight_writes(既有 5s 语义)
  → lifecycle hooks(on_step_down / on_shutdown)
  → raft.shutdown()            ← 先停 appends 源头
  → log_storage.close().await  ← 再排空汇点(C3 的执法点)
  → 返回
```

`RrLogStorage`(内存)无 close 概念,该步骤为 no-op;`RrNode::shutdown` 逐组走完整序。

### 4. snapshot 错误传播

按 Options-P1-A 实施。配套:错误日志 + `rr_snapshot_build_failures_total` counter;`get_snapshot_builder` 与 `try_create_snapshot_builder` 委托同一 fallible inner,保证两条入口语义一致;`get_current_snapshot` 维持现状(已正确)。

### 5. offset→request_id Result 化

按 Options-P2-A 实施。波及面(实施计划列全):`rr-ingestor-kafka/src/lib.rs`(函数 + 4 个 helper 测试)、`rr-raft/src/ingestor_runner.rs`(类型别名 + poll loop fail-closed + 测试)、`examples/kv-store-kafka/src/main.rs:70-90`、docs(`docs/docs/guide/kafka-input.md:93-114`、`docs/docs/guide/idempotency.md:95`)。

### 6. 依赖方向审计结论(记录在案)

维持现有 crate 边界:`OffsetToRequestId` 留在 `rr-raft::ingestor_runner`(opaque bytes,Kafka 无关),Kafka `i64` 解码留在 `rr-ingestor-kafka`。审计方法(逐 Cargo.toml + 全仓 grep `kafka` 于 `crates/rr-raft/src/`)与结论如上 §Problem 末节;后续任何把具体 offset 类型引入 rr-raft 的改动应在本 ADR 的 Changelog 中留痕复审。

## Consequences

### Positive

- durability 从"侥幸安全"变为**有契约、有执法点、有测试**:C3 由 `stop_group → close()` 强制,"N 条 pending + stop + restart 数据不丢"测试直接钉住;C1/C4 由 fsync-先于-ACK 的既有顺序 + 回归测试钉住。
- snapshot 损坏从源头消失:两条 openraft 入口(新旧方法)共享同一 fallible inner,错误经 `build_snapshot` 的 `Result` 通道上抛为可诊断的 `StorageError`。
- exactly-once 链补上最弱环:映射失败 = fail-closed 退出,不再静默丢消息/污染 watermark。
- 热路径每次 append 省掉一个 spawn + 一对 oneshot(正确性与性能双收)。
- 写路径生命周期与框架其余 supervisor(全部 tracked spawn + 显式 stop)对齐,心智模型统一。

### Negative

- `stop_group` 延迟增加:最多 batch window + 一次 fsync + close deadline 上限;graceful 路径变慢是正确性的价格。
- `IoBatcher` 与 `RocksLogStorage` 复杂度略增(closing 状态机、幂等 close 协调)。
- `force=true` 下 poisoned builder 使 `Raft::new` 失败——发生在"snapshot 缺失 + log 已 purge + SM snapshot 失败"三重叠加时;fail-closed 优于启动一个状态残缺的节点,但 operator 需要读懂错误信息(实施时保证 message 含 group/path 上下文)。

### Neutral / 对 v0.2.0 的 breaking 判定

- **Breaking(2 处窄面,必须赶在 WS-3 API freeze 前)**:
  1. `OffsetToRequestId` 类型别名签名变更(`Fn(&[u8]) -> u64` → `Fn(&[u8]) -> Result<_, _>`),影响 `attach_ingestor_with_auto_idempotency` 的全部调用方;
  2. `kafka_offset_to_request_id` 返回类型变更(`u64` → `Result<u64, _>`)。
  可接受:crates 均未发布(0.1.0,freeze 在 WS-3);属正确性修复;必须进 CHANGELOG 0.2.0 breaking 清单与 release notes;examples 与 docs 同步更新。
- **非 breaking**:`IoBatcher`/`RocksLogStorage` 新增方法(additive);`stop_group` 语义强化(更慢但更强,返回类型不变);snapshot 错误路径行为变化(bug fix——旧行为产生的 snapshot 本就不可安全使用);relay spawn 删除(内部实现细节)。
- **wire/磁盘格式**:不动 proto、不动 RocksDB key 编码、不动 FramedSnapshot——无迁移负担。
- 内存 log 路径(`RrLogStorage`)与 gRPC/TCP transport 行为零变化。

## Changelog

| Date | Change |
|------|--------|
| 2026-08-28 | Initial: 陈述 durability 契约 C1–C7(ACK 边界 / vote / shutdown 排空 / 崩溃语义 / 策略条件 / snapshot 完整性 / exactly-once 链);采纳 `IoBatcher::close()` 显式生命周期(tracked spawn、completion 入 `BatchItem`、drain → final fsync → 应答 pending → 退出)并否决隐式 drop-drain + 文档化、Drop 内 block_on、依赖 openraft 排空、JoinSet tracker 四个备选;采纳 snapshot 双通道 fail-closed(`try_create_snapshot_builder` override:`force=false` 返回 `None` 延期、`force=true` 返回 poisoned builder)并否决 force=true 返回 `None`(openraft `helper.rs:182` unwrap panic)、adapter 内 retry、文档警告;采纳 offset→request_id Result 化 + runner fail-closed(decode 失败与负数均 Err)并否决 skip-and-continue、park-and-retry、保留 clamp;记录 Oracle Q5 依赖方向审计结论(层未住错,修签名不动层);判定 2 处窄面 breaking(`OffsetToRequestId`、`kafka_offset_to_request_id`)须赶在 WS-3 API freeze 前并进 CHANGELOG。 |
| 2026-08-29 | Implemented(WS-4):全部按 Option A 落地——sm_adapter 双入口委托 fallible `snapshot_builder_inner` + poisoned builder(T5 三测试);`kafka_offset_to_request_id` → `Result<u64, KafkaOffsetError>`(decode 失败/负数均 Err),`OffsetToRequestId` → `Result<u64, OffsetMapError>`,runner 映射失败 fail-closed 退出(T6);`IoBatcher` tracked spawn + `BatchItem` 携带 completion(删 per-append relay spawn)+ 幂等 `close()`(30s `close_timeout` 可配);`stop_group`/`register_node` 重复分支接线 `RocksLogStorage::close()`(C3);T1–T4 新测试全绿,`cargo test -p rr-raft --features rocks` 130 通过、rr-ingestor-kafka 17 通过、integration 持久化四测试(含 T4 `test_stop_group_flush_preserves_acked_and_pending`)全绿,clippy 零告警。CHANGELOG breaking 清单由发布流程收口。 |
