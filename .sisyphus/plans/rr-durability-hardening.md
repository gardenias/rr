# rr durability hardening — shutdown 契约与正确性三连实施计划

> Companion to ADR **A-0011**(`.sisyphus/adr/A-0011-shutdown-durability-contract.md`)——契约陈述 C1–C7 与被否决备选以 ADR 为准,本文件只承载任务分解。
> 工作项:roadmap **WS-4**(`.sisyphus/plans/rr-v0.2.0-release-roadmap.md`)。
> Status: **proposed**(2026-08-28 制定)。基线 HEAD=`ba69cc8`;文中行号均对该基线核实。

## Pre-conditions(按序先做)

1. **WS-1 落地(CI rocks feature 修复)**。本计划 Phase 5 的多条验收依赖 rocks 持久化测试在 CI 真实运行;`.github/workflows/ci.yml:93` 的 feature 传播未修之前,Phase 3/4 的改动只能靠本地 `--features rocks` 验证。若 WS-1 未先行,Phase 1/2 可先动,Phase 3 起的合并必须等 CI 能说真话。
2. **openraft 保持 `= 0.10.0-alpha.19` 钉版**。本计划的设计(`try_create_snapshot_builder` override、`Fatal::Stopped` 不排空语义)对该版本做过源码级核对;升级 openraft 不在本计划范围(roadmap 风险登记已单列)。
3. **通读 ADR A-0011** 与以下源码(任何改动前):`crates/rr-raft/src/io_batcher.rs` 全文件、`crates/rr-raft/src/rocks_log_storage.rs` 全文件、`crates/rr-raft/src/sm_adapter.rs:92-147`、`crates/rr-raft/src/group_manager.rs:359-434`(`stop_group`)、`crates/rr-raft/src/ingestor_runner.rs:60-160/334-462`、`crates/rr-ingestor-kafka/src/lib.rs:59-129`。

## Implementation tasks

### Phase 1 — snapshot 错误传播(`crates/rr-raft/src/sm_adapter.rs`)

- [ ] 抽出私有 `async fn snapshot_builder_inner(&mut self) -> Result<RrSnapshotBuilder, io::Error>`:调用 `self.inner.snapshot().await`,成功时写 `self.snapshot_bytes` 缓存并构造 builder;失败原样转为 `io::Error`。
- [ ] override `try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder>`:
  - `inner` 成功 → `Some(builder)`;
  - `inner` 失败且 `force == false` → error 日志 + `metrics::counter!("rr_snapshot_build_failures_total", "group_id" …)`(若无 group 上下文则不带 label)+ 返回 `None`(延期,openraft 按 `SnapshotPolicy` 重试);
  - `inner` 失败且 `force == true` → 返回 `Some(poisoned builder)`:builder 捕获该错误,`build_snapshot()` 返回 `Err(io::Error)`——让 `storage/helper.rs:182` 的启动重建路径以 `StorageError` 失败,而不是 `None` 触发 openraft 内部 `.unwrap()` panic。
- [ ] `get_snapshot_builder`(trait 必填、无 `Result`)委托同一 inner:失败返回 poisoned builder。**两条入口都不允许产出"空字节 + 合法 meta"**。
- [ ] `RrSnapshotBuilder` 增加内部错误槽(如 `build_error: Option<io::Error>`),`build_snapshot` 先检查该槽。
- [ ] 单元测试(新):
  - `test_snapshot_failure_defers_policy_build`:SM 的 `snapshot()` 返回 `Err` → `try_create_snapshot_builder(false)` 返回 `None`;
  - `test_snapshot_failure_forced_build_errors`:`force=true` → `build_snapshot()` 返回 `Err`;
  - `test_legacy_get_snapshot_builder_never_empty_on_error`:旧入口失败后 `build_snapshot()` 返回 `Err`(不返回空 snapshot);
  - 既有 6 个 snapshot 测试(`sm_adapter.rs:241-360`)保持全绿(成功路径不变)。

### Phase 2 — offset→request_id Result 化 + runner fail-closed

- [ ] `crates/rr-ingestor-kafka/src/lib.rs:74-78`:`kafka_offset_to_request_id` 改为 `Result<u64, KafkaOffsetError>`(新 error 类型,thiserror;该 crate 已依赖 thiserror)。decode 失败与负数 offset 均 `Err`(负数含 Kafka 哨兵值;合法消息 offset ≥ 0)。更新 rustdoc:删除"clamp to 0"叙述,写明 fail-closed 契约与理由(与真实 offset 0 碰撞 → watermark 污染)。
- [ ] `crates/rr-raft/src/ingestor_runner.rs:66`:`OffsetToRequestId` 类型别名改为 `Box<dyn Fn(&[u8]) -> Result<u64, OffsetMapError> + Send + Sync>`;`OffsetMapError` 定义在 rr-raft(轻量 error 类型)或复用 `RrError`——实施时择简,保持 runner 内处理单一。`AutoIdempotency` rustdoc 同步。
- [ ] `run_poll_loop`(`ingestor_runner.rs:391-409`):映射失败 → error 日志 + `rr_ingestor_offset_map_failures_total` metric → **不 propose、不 commit offset**,让 supervisor 以 `Err` 退出(与 `seed_offset` 失败同路径,`ingestor_runner.rs:203-213`)。注意避免 supervise 主循环的热循环:失败必须终结 supervisor task,而非仅跳出 poll loop。
- [ ] 测试更新与新测试:
  - `rr-ingestor-kafka` helper 测试改写:garbage bytes → `Err`;负数(-1/-1001/i64::MIN)→ `Err`;正数 round-trip 与单调性断言不变(`lib.rs:94-129` 四个测试改写为 Result 断言);
  - runner 级新测试 `test_supervisor_exits_on_offset_map_failure`:mock ingestor 产出无法映射的 offset,断言 supervisor 退出、无 `write_batch` 发生、无 offset 被 commit(沿用 `ScriptedIngestor`/`seek_calls` 测试夹具模式,`ingestor_runner.rs:480-530`);
  - `auto_idempotency_attaches_request_id_via_extractor`(`ingestor_runner.rs:695-710`)适配新签名。
- [ ] 波及面同步(breaking,进 CHANGELOG 0.2.0):
  - `examples/kv-store-kafka/src/main.rs:70-90` 两处 `Box::new(kafka_offset_to_request_id)`;
  - `docs/docs/guide/kafka-input.md:93-114`、`docs/docs/guide/idempotency.md:95` 代码块;
  - `crates/rr-ingestor-kafka/src/lib.rs:32-51` 模块级 doc 示例。

### Phase 3 — IoBatcher 显式生命周期(`crates/rr-raft/src/io_batcher.rs`、`rocks_log_storage.rs`)

- [ ] `BatchItem` 重构:携带完成通道而非仅有 oneshot——内部 enum 同时容纳 openraft `IOFlushed<RrTypeConfig>` 与 submit 用 oneshot;worker Phase 5 统一逐个应答(每 item 恰好一次)。
- [ ] `IoBatcher::new` 持有 worker `JoinHandle`(结构参考 `IngestorHandle`,`ingestor_runner.rs:102-118`);handle 放入可幂等取用的容器(如 `Arc<Mutex<Option<JoinHandle>>>`)。
- [ ] 实现 `pub async fn close(&self) -> Result<(), String>`:置 closing 标志(新 submit fail-fast `Err`)→ 通知 worker(控制消息或 receiver 侧 close)→ worker 排空全部已接收 item → 合并最终 `write_batch`(final fsync)→ 应答全部 pending completion → 应答 closer → 退出;closer await `JoinHandle`。幂等:并发/重复 `close()` 均等待同一次关闭结果。deadline 默认 30s(`IoBatcherConfig` 增 `close_timeout`),超时返回 `Err` + warn,不 panic。
- [ ] `IoBatcher::submit` 保持现有签名与语义(await durability),内部改走新 `BatchItem`(oneshot 变体)——既有调用方与测试零适配。
- [ ] `RocksLogStorage::append`(`rocks_log_storage.rs:214-252`):**删除 `tokio::spawn` relay(`:243-249`)**,直接把 `callback` 随 ops 交给 batcher(新 `submit_with_callback` 或等价 API);空 ops 立即 `io_completed(Ok(()))` 的早返回路径(`:234-237`)保留。
- [ ] `RocksLogStorage` 增加 `pub async fn close(&self) -> Result<(), io::Error>`(委托 batcher close)。
- [ ] 单元测试适配与新增见 Phase 5 T1–T3;既有 `io_batcher.rs:219-316` 五个测试与 `rocks_log_storage.rs` 全部 16 个测试保持全绿(drop 兜底 drain 行为保留,`batcher_exits_cleanly_on_drop` 不删)。

### Phase 4 — stop_group flush 接线(`crates/rr-raft/src/raft_node.rs`、`group_manager.rs`)

- [ ] `RaftNode` 增加 rocks 情形的 log-storage close handle:`new_with_rocks_factory`(`raft_node.rs:135-180`)在把 `RocksLogStorage` 移交 `Raft::new` 前保留一份 clone(`RocksLogStorage: Clone`),存入 `RaftNode` 的 `Option` 字段(feature `rocks` gate);内存路径该字段为 `None`。
- [ ] `RaftGroupManager::stop_group`(`group_manager.rs:359-434`)在 `raft.shutdown()` 成功之后、返回之前:取出该 handle,`close().await`;close 失败记 error 日志并向上返回 `Err`(不再静默放行)。顺序固定为 ADR A-0011 §Decision-3:停 supervisors → drain in-flight(既有 5s)→ lifecycle hooks → `raft.shutdown()` → `close()` → 返回。
- [ ] `register_node` 的重复启动分支(`group_manager.rs:249-260`,`raft.shutdown()` 后丢弃 node)同样走 close(避免重复注册路径泄漏未 flush 的 batcher)。
- [ ] 内存路径(`start_group`)行为不变:no handle → 跳过 close;既有 `group_manager.rs` 测试全绿。
- [ ] `RrNode::shutdown`(`node.rs:640-647`)无需改动——逐组 `stop_group` 已自动获得新语义;rustdoc 补一句 C3 契约指针(指向 ADR A-0011)。

### Phase 5 — 测试设计(核心: "N 条 pending + stop + restart 数据不丢")

- [ ] **T1 `batcher_close_drains_pending`**(io_batcher 单测):window=50ms;并发 spawn 100 个 task 各 `submit` 一组 Put(不 await);主 task 立即 `close().await`;close 返回后 join 全部 100 个 handle——全部 `Ok`;storage 读回全部 100 个 key。**断言:close 返回 ⟹ 全部 pending 已 fsync 并应答。**
- [ ] **T2 `batcher_close_idempotent_and_rejects_new`**:`close()` 两次均 `Ok`;close 后 `submit` 立即 `Err`;close 后 worker `JoinHandle` 已完成。
- [ ] **T3 `rocks_append_close_reopen`**(rocks_log_storage 单测):`RaftLogStorage::append` 提交 50 条 entry(callback 用 `IOFlushed::signal`,先不 await)→ `close().await` → 全部 50 个 callback 收到 `Ok` → 同 path reopen,`try_get_log_entries` 读回 50 条。**这是 relay-spawn 删除后 callback 契约的直接回归。**
- [ ] **T4 `test_stop_group_flush_preserves_acked_and_pending`**(tests/integration,rocks feature):single-node rocks group(`new_with_rocks` + `start_group_rocks`,沿用 `test_restart_persistence` 的夹具,`tests/integration/src/lib.rs:1057-1125`);并发发出 20 个 `client_write`(**不 await**);在多数仍在途时调 `stop_group`;随后同 path 重启(重开 `RocksLogStorage` 验 log,或新 manager 重新起组)。断言:(a) 所有**已返回 `Ok`** 的 write 对应的 log index 全部 ≤ 重启后 `last_log_id`(已 ACK 不丢,C4);(b) `stop_group` 返回前全部在途 write 均有定论(不挂起,C3);(c) vote 持久。与既有 `test_crash_during_write_preserves_acked_entries`(`lib.rs:1926`,abrupt-drop 路径)互补:T4 走优雅 close,前者走 drop 兜底,两者都必须绿。
- [ ] **T5/T6**:Phase 1/2 的 snapshot 与 offset 测试(见各 Phase 清单)。
- [ ] 回归:`cargo test -p rr-raft --features rocks`、`cargo test -p rr-ingestor-kafka`、integration 的三个持久化测试(`test_restart_persistence` / `test_node_failure_recovery` / `test_crash_during_write_preserves_acked_entries`)在 **CI**(经 WS-1 修复后的 feature 传播)真实执行且全绿。
- [ ] `clippy -p rr-raft -p rr-ingestor-kafka --features rocks` 无新增 warning。

### Phase 6 — 收尾

- [ ] CHANGELOG 0.2.0 breaking 清单收录:`OffsetToRequestId` 与 `kafka_offset_to_request_id` 签名变更(2 处,见 ADR A-0011 §Consequences)。
- [ ] `docs/docs/reference/invariants.md`(WS-8 交付物)登记 C1–C7 时,强制点列引用本计划落地的 `file:line`(实施后回填)。
- [ ] ADR A-0011 status → `implemented`(随全部验收通过)。
- [ ] GitNexus index 刷新:`npx gitnexus analyze`(.gitnexus/meta.json 显示 embeddings > 0 则加 `--embeddings`)。

## Acceptance criteria

1. **grep 验收(写路径无 untracked spawn)**:`git grep -n "tokio::spawn" -- crates/rr-raft/src/io_batcher.rs crates/rr-raft/src/rocks_log_storage.rs` 的结果中,每个 spawn 的 `JoinHandle` 均被持有并可 await;per-append relay spawn 不存在。
2. **grep 验收(无吞错)**:`git grep -n "unwrap_or_default\|unwrap_or((" -- crates/rr-raft/src/sm_adapter.rs crates/rr-ingestor-kafka/src/lib.rs` 在 snapshot/offset 映射路径上为空。
3. `kafka_offset_to_request_id` 与 `OffsetToRequestId` 均返回 `Result`;runner 对映射失败 fail-closed(T6 证明:不 propose、不 commit、supervisor 退出)。
4. 新测试 T1–T6 全绿;既有 rocks/持久化/崩溃测试在 CI 真实运行且全绿(依赖 WS-1)。
5. **T4 通过**:N=20 pending + `stop_group` + restart,已 ACK 数据零丢失、`stop_group` 不挂起。
6. CHANGELOG 收录 2 处 breaking;examples/kv-store-kafka 与两份 docs guide 同步更新且 `cargo check --workspace --all-features`(含 examples)通过。

## Risk register

| Risk | Mitigation |
|---|---|
| `close()` 死锁/挂起:worker 已退出导致控制消息无人应答,或 closer 与 worker 互相等待 | 双通道(控制消息 oneshot + `JoinHandle`)+ `close_timeout`(默认 30s)封顶;超时返回 `Err` 不 panic;T2 覆盖"close 后 worker 必退出" |
| `stop_group` 延迟增加(多一段 flush)拖累滚动重启 | close 只付"剩余 channel + 一次 write_batch"的成本,deadline 封顶;既有 `group_manager.rs` 全部 stop 路径测试保持绿 |
| openraft alpha 对 `try_create_snapshot_builder` 默认委托旧方法——override 新方法后旧入口语义漂移 | 两入口委托同一 fallible inner;T5 三条测试分别钉住 `force=false`/`force=true`/旧入口 |
| `force=true` poisoned builder 使 `Raft::new` 失败的行为 surprise | 仅在"snapshot 缺失 + log 已 purge + SM snapshot 失败"三重叠加触发;错误信息必须含可诊断上下文;fail-closed 优于启动残缺节点(ADR 已裁决) |
| `OffsetToRequestId` breaking 波及面遗漏(examples/docs/SDK 叙述) | Phase 2 清单逐一打勾;`cargo check --workspace --all-features` + docs 代码块人工核对;CHANGELOG 收录 |
| WS-1 未先行导致 rocks 测试在 CI 仍跳过,Phase 3/4 回归不可见 | Pre-condition #1:Phase 3 起的合并以 WS-1 落地为前置 |
| ingestor fail-closed 退出后 poison 消息无人处置,生产表现为"消费停滞" | 这是设计意图(同 `seed_offset` 先例):supervisor 退出有 warn 日志 + metric;runbook/docs 补一句处置指引(修 ingestor 或重置 offset);0.2.1 可评估 supervisor 自动重启策略,不在本期 |
| relay-spawn 删除改变 callback 触发时序(worker 直接应答 vs 独立 task 应答) | 时序语义不变(均在 fsync 后);T3 + 既有 `test_rocks_append_callback_called` 等 16 个 rocks 测试回归 |

## Out of scope(本期不做)

- **A-0010 WAL backend**——维持 deferred;R-7 release 基线是其前置门槛,基线出来前不动工。
- **kill -9 混沌 harness / fsync 故障注入**——R-4 的工作;R-4 复用本计划的 `close()` 协议与 T4 测试骨架。
- **`DurabilityPolicy` 语义本身**——Strict 以下模式的弱化是既有文档化行为(C5);不动。
- **rr-server 生产 binary 的 rocks log storage 接线**——现状是 binary 走 `RaftGroupManager::new_with_network`/`new_with_tcp_network`(内存 log,`main.rs:123-152`),`start_group_rocks` 尚未接入 binary;接线的取舍(内存 log + 复制 vs 持久 log)是独立工作项,本计划只硬化 rocks 路径本身的契约。
- **WS-5(TCP gate,A-0014)/ WS-6(membership guard,A-0012)/ WS-7(删 `GroupMeta.leader`,A-0013)**——各自有 ADR,不混入。
- **性能基准数字**——R-7;本计划只删 per-append spawn(方向性优化),不设数值目标。
- **openraft 升级**——钉版不动;升级单独开 ADR。
