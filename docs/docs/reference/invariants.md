---
sidebar_position: 8
---

# 系统级不变量目录 / System Invariants

本文档是 rr **对外宣称(advertised)的系统级属性**的权威登记表:每条属性被精确陈述为一条
**可判定的不变量**,并标注它从入口到落点经过的不变量链、代码中的强制点(`file:line`)、
覆盖它的现有测试,以及当前已知的执法缺口。

---

This document is the authoritative registry of rr's advertised system-level
properties. Each property is stated as a decidable invariant with its
enforcement chain, code enforcement points (`file:line`), existing tests, and
honestly-recorded enforcement gaps.

## 本文档的目的

- **living document**。本文档随代码演进,不是一次性快照。凡新增对外属性(新的一致性承诺、
  新的投递语义、新的安全门控……),**必须先在此登记不变量**,再实现代码;未登记的对外属性
  视为不存在。
- **元根因修复**。2026-08-16 全量 review(`.sisyphus/evidence/rr-full-review-2026-08-16.md`)
  的 Oracle 判定是:几乎所有 P0/P1 符合同一 pattern——"不变量 X 存在,但 layer Y 没强制"。
  本文档是修复该元根因的第一步(路线图 WS-8):先让每条不变量的强制点显式化,缺口才无处隐藏。

## 维护规则

1. **改代码动强制点时,必须同步本文档。** 强制点列出的 `file:line` 若因你的改动而移动或失效,
   同一 PR 内更新本文档;评审者应把"强制点行号是否仍然准确"作为 review 项。
2. 行号引用的基线为 `HEAD = ba69cc8`(2026-08 全量 review 基线);此后以"同步更新"维持准确,
   而非追求对任意历史 commit 可回溯。
3. 缺口条目必须引用证据(review 的 P0/P1/P2 条目或后续 issue)与对应的修复工作流
   (WS-x / R-x,见 `.sisyphus/plans/rr-v0.2.0-release-roadmap.md`);不允许"已知有问题"但
   无归属的条目。
4. 一条不变量被修复工作流闭环后,**缺口条目转为"已闭环"并保留**(注明闭环版本),而不是删除
   ——历史缺口是后来者的排雷图。
5. **登记触发规则(元规则)**:凡 (a) CHANGELOG `### Added` 中出现能力性宣称、(b) README/docs
   中出现 guarantee / must / never 级措辞、(c) 新增 pub API 带语义承诺,三者任一命中,即触发
   本文档的登记义务——先登记 INV 条目(强制点 + 关联测试)再合入。该义务由
   `scripts/check-invariants.sh`(CI `invariants-lint` job,阻塞式)与 PR 模板检查项共同执法。

## 与 ADR 体系的关系

- **ADR(`.sisyphus/adr/`)记录"为什么这样设计"**——决策时点上的权衡与备选;状态机为
  `proposed → accepted → implemented`。
- **本文档记录"系统当前宣称并强制什么"**——持续有效的运行时宣称及其执法现状。
- 两者互补:ADR 可以冻结不变更,本文档必须始终为真。新增/变更一条对外不变量通常伴随一份
  ADR(设计决策)+ 本文档的一条登记/更新(执法状态)。每条不变量下方标注其相关 ADR。

## 如何阅读每条不变量

- **不变量陈述**:精确、可判定的陈述——"在任何情况下都……",不含"尽量""通常"。
- **不变量链**:从入口到落点经过哪些层,每层承担什么责任。
- **强制点**:该不变量在代码中被强制的位置(`file:line`,已对照源码核实)。
- **现有测试**:覆盖该不变量的测试。
- **已知缺口**:诚实标注强制缺失或不可证明的环节,引用 review 条目与修复工作流。

---

## INV-1 Durable Append — 已 ACK 的写必存活

### 不变量陈述

client 收到写 ACK(`client_write` 成功返回)的 log entry,在任何**单点进程崩溃/机器断电 +
重启**之后,必须仍存在于该节点的 Raft log(或由其产生的状态机状态/快照)中。推论:vote
必须同等级持久化(防脑裂)。

该承诺以 `DurabilityPolicy::Strict` 为前提;弱于 Strict 的策略(`ReplicatedOnly`/`TestOnly`)
显式放弃单节点持久性,不属于本不变量覆盖范围(框架在启动时告警,见强制点 3)。

### 不变量链

1. **入口**:`ClientService.request`(gRPC)→ `ClientDispatchImpl::submit_write`
   (`crates/rr-server/src/dispatch.rs:77` 调 `raft.client_write`)。
2. **Raft 核心**:openraft `RaftCore` 将 entry 交给 `RaftLogStorage::append`,并持有
   `IOFlushed` callback——entry 只有在 `io_completed(Ok)` 后才被计为已持久化、才可能
   commit/apply/ACK。
3. **异步 append**:`RocksLogStorage::append` 把 entry 编码为 `BatchOp` 并提交给 per-group
   `IoBatcher`(`crates/rr-raft/src/rocks_log_storage.rs:214-252`);callback 在 batcher
   确认后触发(`:243-249`)。
4. **组提交**:`IoBatcher` worker 把累积的整批 ops 合并为**一次** `write_batch()` 调用,且
   只有在 `write_batch` 返回后才应答全部等待者(`crates/rr-raft/src/io_batcher.rs:188-201`)。
5. **fsync 边界**:`RocksStorage::write_batch` 用 `durability.write_options()` 执行
   `db.write_opt`(`crates/rr-storage/src/rocks.rs:518-543`);Strict 策略下
   `WriteOptions::set_sync(true)`(`crates/rr-storage/src/rocks.rs:118-119`),即每次
   write_batch 伴随 WAL fsync。
6. **停机路径**:`stop_group` 先停后台任务,再等待 in-flight 写计数归零(5 秒上限),最后
   `raft.shutdown()`(`crates/rr-raft/src/group_manager.rs:359-434`)。

### 强制点

| # | 位置 | 强制内容 |
|---|---|---|
| 1 | `crates/rr-storage/src/rocks.rs:114-129` | `DurabilityPolicy::write_options()`:Strict → `set_sync(true)`;TestOnly → `disable_wal(true)` |
| 2 | `crates/rr-raft/src/io_batcher.rs:188-201` | 整批 `write_batch()` 成功后才向所有 caller 发送 `Ok`——ACK 不先于落盘 |
| 3 | `crates/rr-storage/src/rocks.rs:333-339` | 非 Strict 策略启动时 `warn!`:"violates OpenRaft's durability contract"(软门控) |
| 4 | `crates/rr-raft/src/rocks_log_storage.rs:234-249` | 空批次立即 `io_completed(Ok)`;非空批次经 batcher,无"先 ACK 后落盘"路径 |
| 5 | `crates/rr-raft/src/group_manager.rs:405-417` | `stop_group` 等待 `in_flight_writes` 归零后才 shutdown raft |
| 6 | `crates/rr-server/src/main.rs` `with_raft_log_dir` | 生产接线(ADR A-0016):`--storage rocks` 时 raft log 落盘 `{data_dir}/raft-log/{group_id}`——默认部署形态的 durable log 自此成立 |
| 7 | `crates/rr-raft/src/io_batcher.rs` `acquire_bytes`/`max_inflight_bytes` | 在途未落盘字节预算(默认 64 MiB):submit 前先取信号量许可,permit 随批次持有到 flush 完成才释放——异步尾部有界,背压进入提案管线(断"无界积压→OOM 重启波"链,W9 flake 2026-09-17) |

### 现有测试

- `tests/integration/src/lib.rs:1057` `test_restart_persistence`(rocks,重启后 log/vote 存活)
- `tests/integration/src/lib.rs:1347` `test_node_failure_recovery`(rocks,节点故障恢复)
- `tests/integration/src/lib.rs:1926` `test_crash_during_write_preserves_acked_entries`(rocks,
  20 条已 ACK 写在 abrupt drop 后全部存活)
- `crates/rr-raft/src/rocks_log_storage.rs:490` `test_rocks_vote_persistence`、`:512`
  `test_rocks_log_persistence`(reopen 存活)
- `crates/rr-raft/src/rocks_log_storage.rs`
  `test_save_vote_waits_for_pending_appends`(vote 写入必须先排空 append 积压:
  vote 完成会把 openraft vote 主导的 flushed 光标推过未落盘日志尾)
- `crates/rr-raft/src/io_batcher.rs:292` `batcher_exits_cleanly_on_drop`(drop 后 worker 退出)
- `tests/integration/src/lib.rs:2010` `test_concurrent_writes_under_chaos_linearizability`
  (chaos 下"已 ACK 不丢"压力验证)
- `crates/rr-raft/src/io_batcher_proptest.rs` `proptest_io_batcher_interleavings_exactly_once_and_ordered`、
  `crates/rr-raft/src/io_batcher_proptest.rs` `proptest_io_batcher_write_error_propagates_to_whole_batch`
  (R-6 proptest:随机 submit 并发 × window × max_entries;每个 submission 恰好完成一次、
  批内按 big-endian log-index 排序、写错误传播给整批 caller、无 hang;`--features rocks` 下运行)
- `crates/rr-raft/src/rocks_log_storage.rs` `test_append_visible_before_flush_via_overlay`、
  `test_truncate_after_does_not_resurrect_pending_appends`、
  `test_purge_does_not_resurrect_pending_appends`、
  `test_pipelined_appends_concurrent_reads_never_gap`(R-4,ADR A-0015 回归:
  submitted⇒visible 契约、直连删除不复活未落盘 append、管线写 + 并发读无缺口)
- `crates/rr-raft/src/storage_fault_tests.rs`(R-4 故障注入 × IoBatcher 组提交路径,
  rocks-gated):`fail_flush_surfaces_err_and_batcher_worker_survives`、
  `torn_write_batch_leaves_prefix_only_and_error_propagates`、
  `enospc_propagates_to_all_pending_batch_callers`、
  `slow_disk_latency_completes_correctly_just_slower`
- `crates/rr-raft/src/io_batcher.rs` `flush_barrier_*`、`submit_with_hook_*`
  (屏障排序与 post-flush hook 语义)
- `crates/rr-raft/src/io_batcher.rs` `byte_budget_backpressures_submitters`
  (字节预算背压:预算耗尽时 submit 挂起,直到在途批次 flush 完成释放许可)
- `crates/rr-raft/src/group_manager.rs` `test_start_group_persists_log_with_rocks_base_path`、
  `test_start_group_rocks_shim_still_persists`(ADR A-0016,rocks-gated:统一 log 选择 +
  shim 兼容,直开存储字节级验证 + `Raft::new` 恢复路径验证)
- `tests/integration/src/raft_log_durability.rs`
  `test_rocks_raft_log_rolling_restart_preserves_acked_writes`(ADR A-0016,rocks-gated:
  3 节点复制 + rocks log 双开,50 条已 ACK 写,逐节点滚动重启,quorum 保持、复制收敛、
  恢复节点服务 linearizable read、直开 log DB 字节级验证)
- `tests/crash/src/scenarios.rs` `scenario_a_graceful_sigterm`(R-4 进程级 harness,
  `--features rocks`,CI 由 `crash-nightly.yml` 承载:SIGTERM 优雅关闭 → 重启 → 全量
  ack-log 回放,验证 WS-4 close 协议:已 ACK 全存活 + 进程 20s 内以 0 码退出 +
  clean-shutdown 标记)、`tests/crash/src/scenarios.rs` `scenario_b_sigkill`
  (seed 化随机 SIGKILL × N 轮 + 同 data-dir 重启 + 追平 + 每轮 oracle 回放;
  `CRASH_SEED` 精确复现)
- R-8 soak harness(脚本入口 scripts/soak.sh,nightly 由 soak-nightly.yml 承载):
  三件套 oracle —— ack-log 探针回放(同幂等键异值 Put + linearizable Get 读回原始值)、
  节点日志零 panic、lag 收敛;`SOAK_SEED` 复现故障序列。2026-09-10 本地 60s/90s smoke
  全绿,且两个负面对照实测命中(伪造未写记录 → INV-1 VIOLATION;篡改 hash →
  hash-mismatch 报错);2h+ 绿由 nightly 累积。

### 已知缺口

- **【review P0#5 → WS-4 / ADR A-0011】shutdown durability 依赖隐式 drop-drain,不可证明。**
  `IoBatcher::new` 的 worker 是 untracked `tokio::spawn`(`crates/rr-raft/src/io_batcher.rs:116`),
  不持 `JoinHandle`;batcher 的"drain 后退出"仅靠 channel 关闭语义(doc 见 `io_batcher.rs:105-106`)。
  `RocksLogStorage::append` 的 callback 转发同样是 untracked spawn
  (`crates/rr-raft/src/rocks_log_storage.rs:243`)。`stop_group` 只 drain **client 写计数**,
  不 flush batcher;且 5 秒 drain 超时后仅 `warn!` 继续 shutdown
  (`crates/rr-raft/src/group_manager.rs:407-415`)。ACK 边界(先 fsync 后 ACK)本身未被违反,
  但"已入 batcher 未落盘 + 进程退出"的窗口没有显式 `close()` 协议兜底。

  **✅ 已闭环(2026-08-29,WS-4 / ADR A-0011)**:`IoBatcher` 显式 `close()` 协议落地——
  `crates/rr-raft/src/io_batcher.rs:280-341`(幂等 close:drain + final flush + 应答全部 pending
  completion + reap worker `JoinHandle`,`close_timeout` 有界、超时报错不 panic);
  `crates/rr-raft/src/rocks_log_storage.rs:144`(`RocksLogStorage::close`)与 `:214-266`
  (append 经 `submit_with_callback`,`BatchItem` 直接携带 completion,删除 per-append relay spawn);
  `crates/rr-raft/src/group_manager.rs:458-472`(`stop_group` 在 `raft.shutdown()` 之后接线
  `log_storage.close()`,close 失败即 `Err`);`crates/rr-raft/src/raft_node.rs:45`
  (`log_storage_close` 句柄随 node 构造保存;`group_manager.rs:255-269` register 分支同样接线)。
  测试:T1 `batcher_close_drains_pending`(`io_batcher.rs:597`)、T2
  `batcher_close_idempotent_and_rejects_new`(`:641`)、T3 `test_rocks_append_close_reopen`
  (`rocks_log_storage.rs:657`)、T4 `test_stop_group_flush_preserves_acked_and_pending`
  (`tests/integration/src/lib.rs:2012`,优雅 close 路径,与 abrupt-drop 路径的
  `test_crash_during_write_preserves_acked_entries` 互补)。
- **【review P0#2 → WS-1】上述 3 个 rocks 持久化集成测试在 CI 实际不运行。** 它们被
  `#[cfg(feature = "rocks")]` 门控(`tests/integration/src/lib.rs:1055/1345/1924`),而 CI
  的 `cargo test --workspace --features rr-storage/rocks`(`.github/workflows/ci.yml:93`)
  不会开启 `integration` crate 自身的 `rocks` feature(`tests/integration/Cargo.toml:7`)
  ——本不变量最强的端到端证据目前只在本地可得。

  **✅ 已闭环(2026-08-29,WS-1)**:CI `test-rocks` 现传
  `cargo test --workspace --features rr-storage/rocks,integration/rocks`
  (`.github/workflows/ci.yml:93-95`,行内注释说明 `integration` crate 必须开启自身 `rocks`
  feature),llvm-cov 同步(`:176-177`)——3 个 rocks 持久化集成测试与 T4 在 CI 实际运行。
- **【2026-08-29 WS-7 实施期发现 → RC 批次】SDK 在"follower 未知 leader"窗口对写请求静默假 ACK。**
  `rr-client` 的 `send_request_inner` 在 server 回 `success=false` 且无 `leader_info`
  (新当选 leader 尚未被 follower 感知的窗口)时直接 `return Ok(空 payload)`——
  客户端认为写已成功,实际从未 propose,INV-1 的 ACK 边界在 SDK 侧被架空。
  同类第二条路径:redirect 链末次 attempt 拿到 `success=false` + `leader_info` 时同样落到 `Ok(空)`。

  **✅ 已闭环(2026-09-04,RC 批次)**:`send_request_inner` 重构——`success=false` 一律按可重试处理
  (有 `leader_info` 更新 redirect 缓存;无则清缓存回退 bootstrap 轮询),重试耗尽返回
  `RaftError::NotLeader`,`Ok` 仅在 `success=true` 时返回(`crates/rr-client/src/lib.rs`
  `send_request_inner`)。回归测试:`test_send_request_no_fake_success_when_leader_unknown`、
  `test_send_request_converges_once_leader_is_known`(mock gRPC server 模拟未知/已知窗口);
  既有 redirect/failover 集成测试(`test_client_redirect`、`test_client_leader_failover`、
  `leader_redirect_failover`)实测全绿。
- **【2026-09-06 R-4 crash harness 首跑发现 → 当批次闭环】异步 append 违反 openraft
  "submitted ⇒ visible" 契约。** `RocksLogStorage::append` 入队 IoBatcher 即返回,而
  openraft 0.10.0-alpha.34 的 apply/复制管线以 log-IO **submit**(非 flush)为门控
  (`engine_impl.rs` `next_progress_driven_command`),follower/leader 在追平或负载突发时
  于 batcher flush 前读日志:debug 构建 sm worker 断言(`sm/worker.rs:211`)/复制流断言
  (`replication/stream_state.rs:105`)杀死任务,节点 apply 冻结但 gRPC 假活;release 构建
  无断言,**静默跳过 apply** —— 副本间状态机分叉。次轮仪器化又抓到 RocksDB 迭代器
  创建即钉视图 + hook 摘除竞态("先扫后并"窗口:`lo=923 → first=924`)。

  **✅ 已闭环(2026-09-06,ADR A-0015)**:`RocksLogStorage` 新增 pending overlay
  (append 先 stage 再提交;读路径**先克隆区间 overlay 快照、再扫 RocksDB、后合并**——
  快照序使两源互补,头部缺口不可能,尾部缺口退化为 openraft 可容忍的前缀读);
  `IoBatcher` 新增 `BatchCompletion::Hook`(post-flush 精确摘除,错误路径同样摘除)与
  `BatchItem::Barrier`/`flush_barrier()`(读己所提交屏障);`truncate_after`/`purge`
  先过屏障再直连删除(消除"被截断 entry 被后续 flush 复活"的同根 race);Phase 3 聚合窗口
  闸门由 completion 数修正为 submission 数(hook 引入后不致窗口失效)。回归测试见上方
  "现有测试" R-4 条目组;终证:`tests/crash` scenario A(SIGTERM)+ scenario B
  (10 轮 SIGKILL,3266/3266 oracle 全活,节点日志零 panic、零 head-gap 告警)。

## INV-2 Linearizable Read — 线性一致读

### 不变量陈述

`ReadLinearizable` RPC 返回的读结果,必须反映**至少到该读发起时刻**已完成 Raft commit 的全
部写。非 leader 节点不得自行作答——必须返回携带 `leader_id`/`leader_addr` 的重定向,由 SDK
转向 leader 重试。

### 不变量链

1. **入口**:`ClientService.read_linearizable`(`crates/rr-network/src/client_service.rs:213-239`;
   契约见 `crates/rr-network/proto/client.proto:13-18`)。
2. **路由**:`ClientDispatchImpl::submit_linearizable_read` 经 `ShardRouter` 定位 group
   (`crates/rr-server/src/dispatch.rs:152-159`)。
3. **ReadIndex**:`RaftGroupManager::linearizable_read` 以
   `get_read_linearizer(ReadPolicy::ReadIndex)` 发起 ReadIndex 轮次
   (`crates/rr-raft/src/group_manager.rs:810`)——leader 借此确认自己仍持有多数派、并拿到
   当前 commit index。
4. **非 leader 重定向**:openraft 返回 `ForwardToLeader` 时,映射为
   `RaftError::NotLeader { leader, leader_addr }`(`group_manager.rs:813-817`),dispatch 层
   转为 proto 的 `leader_info`(`dispatch.rs:166-173`),SDK 据此重定向。
5. **本地 apply 追平**:`linearizer.await_ready(&raft)` 阻塞到本地状态机 apply 越过 ReadIndex
   确认的 commit 点(`group_manager.rs:825-829`)。
6. **读本地状态机**:只有此时才 `local_read`(`group_manager.rs:831`)。

### 强制点

| # | 位置 | 强制内容 |
|---|---|---|
| 1 | `crates/rr-raft/src/group_manager.rs:810` | 强制走 openraft `ReadPolicy::ReadIndex`,不存在绕过一致性检查的"快路径" |
| 2 | `crates/rr-raft/src/group_manager.rs:825` | `await_ready` 保证读到的不旧于 ReadIndex 返回的 commit index |
| 3 | `crates/rr-server/src/dispatch.rs:166-173` | 非 leader 必须回 `leader_info` 重定向而非错误/陈旧数据 |
| 4 | `crates/rr-network/src/client_service.rs:95-99`(trait 契约) | `submit_linearizable_read` 的 doc 契约:非 leader 必须回填 `leader_*` |

### 现有测试

- `crates/rr-network/src/client_service.rs:486` `test_read_linearizable_routes_to_submit_linearizable_read`
- `crates/rr-network/src/client_service.rs:539` `test_read_linearizable_surfaces_leader_redirect_to_proto`
  (follower → `leader_info` 重定向直通 proto 层)
- `tests/integration/src/lib.rs` `test_read_linearizable_redirect_from_follower`
  (3 节点真集群:SDK 仅指向已知 follower,经 `leader_info` 重定向在 leader 收敛读成功)
- `tests/integration/src/linearizability.rs` `checker_minimizes_violation_to_conflicting_pair`
  (R-5 porcupine 风格 register 检查器:反例最小化到冲突对;同文件 13 个校准/性能测试
  `checker_rejects_*` ×4 / `checker_accepts_*` ×5 / `checker_perf_*` ×2 /
  `checker_downsamples_oversized_history` / `history_records_completed_and_unknown_ops`
  均携带 INV-2 反链,完整清单见文件)
- `tests/integration/src/linearizability.rs` `test_kv_register_linearizable_under_partition`
  (3 节点 kv register,2 writer + 2 linearizable reader + read_local 混负载 +
  follower 轮转隔离;写 + `read_linearizable` 历史必须过 checker,stale-able
  `read_local` 历史单独记录不送检)
- `tests/integration/src/lib.rs` `test_concurrent_writes_under_chaos_linearizability`
  (E5 计数断言保留为回归兜底,记录的写历史同时送 checker——INV-2 的写侧压力面)

### 已知缺口

- **【A-0009 遗留 follow-up → 路线图 RC 并行项】无 3 节点集成测试。** 线性一致读目前只有
  mock dispatch 级的单元测试;"对已知 follower 调 `read_linearizable`、断言经重定向在 leader
  上读到最新写"的端到端路径无覆盖。无对应 review P 条目(该 RPC 为 0.2.0 新增,review 未单列)。

  **✅ 已闭环(2026-09-04,RC 批次)**:`test_read_linearizable_redirect_from_follower`
  (`tests/integration/src/lib.rs`,3 节点 real-network 集群,SDK 只配 follower 地址,
  断言经 redirect 收敛);集成测试 EchoSM 补 `read_handle`(镜像生产
  `EchoStateMachine` 的 read 路径)。

## INV-3 Exactly-Once Ingest — 摄入恰好一次

### 不变量陈述

同一 Kafka offset 的消息无论被重放多少次(leader 切换、step-down 后恢复、seek 回退、Kafka
at-least-once 重投),用户可见的 `Service::handle()` 效果**恰好发生一次**;重复投递返回首次
执行的缓存响应。

边界条件(如实声明):去重窗口受 LRU 容量限制(默认 10 000,`crates/rr-raft/src/idempotency.rs:68`);
窗口外的重放被判 `IdempotencyExpired` 并**拒绝执行**(fail-closed),而不是重复执行。

### 不变量链

1. **leader-only 摄入**:ingestor supervisor 只在 leader 状态下 poll
   (`crates/rr-raft/src/ingestor_runner.rs:184`、`:345`);follower 经 Raft log 复制获得同样
   的 entry,不各自消费 Kafka。
2. **offset → request_id**:每条消息经 `AutoIdempotency::offset_to_request_id` 映射为单调
   `request_id`,连同 `request_type` 组成 `IdempotencyKey` 写入 `FramedEntry`
   (`ingestor_runner.rs:391-397`)。
3. **Raft 复制**:整批 entry 经 `write_batch` 提交(`ingestor_runner.rs:415`);apply 成功后
   才 `commit_applied` 推进上游 offset(`:434-443`)——Raft log 是 offset 的权威记录。
4. **幂等层去重**:apply 时 `ServiceStateMachineAdapter::apply` 解码 `FramedEntry`,带 key 的
   走 `apply_keyed`(`crates/rr-raft/src/service_adapter.rs:355-373` → `:285-350`);
   `IdempotencyLayer::decide` 四路分派——Replay(命中缓存,直接回缓存响应)/Collision(同
   key 不同 client,回缓存响应 + audit)/Expired(低于水位且已逐出,拒绝)/New(执行并
   `record`)(`crates/rr-raft/src/idempotency.rs:107-143`)。
5. **重启/换主恢复**:幂等层状态(watermarks + LRU)随快照持久化
   (`idempotency.rs:148-160` capture / `:175-194` restore,replace-not-merge);
   新 leader 从快照的 `framework_state.ingestor.last_applied_offset` 恢复 seek 点
   (`ingestor_runner.rs:234-259`);seed 失败时 supervisor 直接退出,拒绝在
   `auto.offset.reset` 下静默从头重放(`ingestor_runner.rs:203-213`)。

### 强制点

| # | 位置 | 强制内容 |
|---|---|---|
| 1 | `crates/rr-raft/src/ingestor_runner.rs:184`、`:345` | 仅 leader poll;失主即退出 poll 循环 |
| 2 | `crates/rr-raft/src/ingestor_runner.rs:391-397` | 每条 entry 强制携带由 offset 派生的 `IdempotencyKey` |
| 3 | `crates/rr-raft/src/idempotency.rs:107-131` | `decide` 的 Replay/Collision/Expired/New 四路语义 |
| 4 | `crates/rr-raft/src/service_adapter.rs:291-348` | `apply_keyed`:Replay 不调用用户 service;New 才执行并 `record` |
| 5 | `crates/rr-raft/src/service_adapter.rs:375-401`、`:403-456` | 幂等层状态随 `FramedSnapshot` 快照/恢复,跨重启保持 |
| 6 | `crates/rr-raft/src/ingestor_runner.rs:203-213` | seed 失败 = supervisor 退出 + 显式错误,无静默 replay-from-earliest |

### 现有测试

- `crates/rr-raft/src/idempotency.rs` 单元测试 ×12:`recorded_key_replays`(:245)、
  `expired_when_below_watermark_and_evicted`(:276)、`first_request_id_zero_is_new_not_expired`
  (:383)、`capture_restore_roundtrip_preserves_replay_behaviour`(:320) 等
- `crates/rr-raft/src/service_adapter.rs:719` `apply_keyed_replay_returns_cached_without_calling_service`、
  `:742` `apply_keyed_expired_returns_idempotency_expired_outcome`、`:765`
  `snapshot_restore_preserves_idempotency_replay_behaviour`
- `tests/integration/src/lib.rs:2091` `test_idempotency_dedup_survives_leader_switch`(换主后重试不重复执行)
- `tests/integration/src/lib.rs:2163` `test_committed_entry_applies_exactly_once_after_leader_crash`(leader 崩溃后重试返回缓存)
- `tests/integration/src/cross_tier_ingestor.rs:286` `test_ingestor_survives_leader_switch_no_message_loss`(重叠 offset 流去重,10 条恰好 10 次)
- `crates/rr-raft/src/ingestor_runner.rs:681` `supervisor_exits_when_seed_offset_fails`
- `crates/rr-raft/src/idempotency_proptest.rs` `proptest_idempotency_lru_watermark_model_parity`
  (R-6 model-based proptest:对照无界 HashMap oracle + 同容量 twin LRU,随机 decide/record 序列 +
  capacity 压力 + capture/restore;断言 watermark 单调、Replay 返回原值、Expired 仅在
  `request_id <= watermark` 且 cache miss 时发生)
- 形式化(R-2):`formal/tla/OffsetExactlyOnce.tla`(TLC 小模型 558 态零反例,2026-09-10;
  exactly-once apply / 响应归属 / watermark 语义 / 无虚假 Expired;植入的 clamp-to-0
  反例被 TLC 抓到——`formal/tla/bugs/OffsetClampBug.tla`)

### 已知缺口

- **【review P0#6 → WS-4 / ADR A-0011 配套】`kafka_offset_to_request_id` 对非法 bytes 静默回 0。**
  `crates/rr-ingestor-kafka/src/lib.rs:74-78` 用 `.unwrap_or((0_i64, 0))` 处理解码失败:任何
  损坏的 offset 都映射为 `request_id = 0`。多个不同消息因此共享同一去重身份——后果是要么相互
  误判 Replay(错误地返回彼此缓存响应),要么触发 `IdempotencyExpired`,exactly-once 承诺被
  击穿。注意 `helper_returns_zero_for_garbage_bytes` 测试(`lib.rs:126-129`)目前把该行为当
  规格固化,修复时须同步改写为返回 `Result`。

  **✅ 已闭环(2026-08-29,WS-4 / ADR A-0011)**:`kafka_offset_to_request_id` 改为返回
  `Result<u64, KafkaOffsetError>`(`crates/rr-ingestor-kafka/src/lib.rs:101`;`KafkaOffsetError`
  定义于 `:68`,decode 失败与负数 sentinel 均为 `Err`,不存在静默回 0 路径);runner 侧
  fail-closed——映射失败的消息不 propose、不 commit offset,supervisor 退出并计
  `rr_ingestor_offset_map_failures_total`(`crates/rr-raft/src/ingestor_runner.rs:432-452`,
  与 seed 失败同一语义)。测试:T6 `supervisor_exits_on_offset_map_failure`
  (`ingestor_runner.rs:803`);上文点名的"回 0 即规格"测试已按要求改写为 `Err` 断言——
  现为 `helper_rejects_garbage_bytes` 与 `helper_rejects_negative_sentinels`
  (`rr-ingestor-kafka/src/lib.rs:135`/`:160`)。
- **附属观察(非 review 条目)**:review Oracle Q5 建议依赖审计"offset→request_id 映射是否
  住错层"(rr-raft 核心不应出现 Kafka/offset 概念),随 WS-4 一并评估。

## INV-4 At-Least-Once Output — 输出至少一次投递

### 不变量陈述

apply 阶段由 `Service::handle_with_outputs` 产生的每个 Output,只要其源 entry 已 commit 并
apply,就**最终会被投递**到目标(Raft 写/Kafka 发布):投递失败按指数退避无限重试,水线只在
确认成功后推进,任何路径都不静默丢弃。语义是 **at-least-once**——换主后可能重投,下游以
`idempotency_key` 去重(见下)。

### 不变量链

1. **产出**:apply 时 `handle_and_collect_outputs` 收集用户 outputs,`append_outputs` 赋单调
   `seq` 并入 `pending`(`crates/rr-raft/src/service_adapter.rs:234-238`、`:219-232`)。
   每个 output 携带用户提供的 `idempotency_key`。
2. **leader-only 投递**:`output_runner` 监视 raft metrics,仅 leader 进入 send loop,follower
   park(`crates/rr-raft/src/output_runner.rs:122-136`、`:167`)。
3. **逐条投递**:`dispatch_one` 按 target 分发(`output_runner.rs:260-300`);RaftWrite 目标
   以 `seq` 派生幂等键走 `send_request_idempotent`(`:274-281`),KafkaPublish 透传
   `idempotency_key`(`:289-297`)。
4. **水线推进**:仅当投递成功才 `advance_output_watermark(seq)`
   (`output_runner.rs:214-230` → `crates/rr-raft/src/group_manager.rs:918-936` →
   `service_adapter.rs:160-170`,同时从 `pending` 剔除 ≤ 水线的项);失败则固定退避
   (100ms 起、封顶 30s,`output_runner.rs:45-49`)后重试,`pending` 不动(`:231-251`)。
5. **重启/换主恢复**:outbox 状态(pending/sent_watermark/next_seq)随快照持久化
   (`service_adapter.rs:387-398` 快照 / `:420-432` 恢复),新 leader 从快照恢复续投。
   注意:水线推进本身**不经 Raft 复制**,靠快照传播——快照点之后已推进的水线对新 leader
   不可见,这正是"可能重投"(at-least-once 而非 exactly-once)的来源;重投由目标侧的
   `idempotency_key` 吸收。

### 强制点

| # | 位置 | 强制内容 |
|---|---|---|
| 1 | `crates/rr-raft/src/output_runner.rs:122`、`:167`、`:197` | 非 leader 不投递(每轮循环与每条 output 前都复查) |
| 2 | `crates/rr-raft/src/output_runner.rs:216` | 水线仅在 `dispatch_one` 返回 `Ok` 后推进 |
| 3 | `crates/rr-raft/src/output_runner.rs:248-250` | 失败退避重试,无丢弃分支;"永不下线"目标以 30s 封顶退避无限重试 |
| 4 | `crates/rr-raft/src/service_adapter.rs:152-158` | `pending_outputs` = `seq > sent_watermark`,语义单一 |
| 5 | `crates/rr-raft/src/service_adapter.rs:387-398`、`:420-432` | outbox 状态入/出 `FramedSnapshot.framework_state.outbox` |

### 现有测试

- `crates/rr-raft/src/output_runner.rs` 单元测试:`dispatch_one_kafka_publish_success`(:391)、
  `dispatch_one_kafka_publish_propagates_failure`(:406)、`dispatch_one_raft_write_requires_client`(:424)、
  `output_handle_stop_completes`(:443)
- `crates/rr-raft/src/service_adapter.rs:966` `apply_collects_outputs_and_assigns_monotonic_seq`、
  `:985` `snapshot_restore_roundtrip_preserves_outbox_state`
- 形式化(R-2):`formal/tla/OutputWatermark.tla`(TLC 小模型 280k 态零反例,2026-09-10;
  无幻影推进 / 保序游标 / 切换重复窗口显式允许;性质↔实现映射见 formal/tla/README.md)

### 已知缺口

- **无 review P0/P1 条目**(ADR A-0007 落地与契约一致)。如实记录两处覆盖空白:
  - 缺**集群级端到端测试**:leader 切换/节点重启后 outbox 经快照恢复、续投不重不漏的场景,
    目前只有 `snapshot_restore_roundtrip_preserves_outbox_state` 单测,无 3 节点集成测试。
    建议随可靠性专项 R-4/R-8 补齐。
  - `pending_outputs` 失败(组不支持 outbox)时 runner 仅 `warn!` 后 `return`
    (`output_runner.rs:171-176`)——该组 output 将停止投递直至下次成为 leader 重入循环;
    属"显式降级"而非静默丢弃,但无 metric 之外的告警通道。

## INV-5 Snapshot Consistency — 快照一致性

### 不变量陈述

快照是某个**确定 log index** 的一致状态点:`SnapshotMeta.last_log_id` 之前的全部 apply 效果
(用户状态 + 框架状态:幂等层/ingestor offset/outbox)都包含在内,之后的不含。restore 是
**完整替换**而非合并。快照经 `InstallSnapshot` 传输安装后,follower 的状态与 leader 在该
index 的状态一致(相关契约经 openraft 源码级核对,见 ADR A-0002)。

### 不变量链

1. **生成**:openraft 触发快照 → `RrStateMachine::get_snapshot_builder`
   (`crates/rr-raft/src/sm_adapter.rs:92-101`)捕获 `inner.snapshot()` 字节 +
   `last_applied` + `last_membership`;`RrSnapshotBuilder::build_snapshot`
   (`sm_adapter.rs:159-170`)将其固化为带 `SnapshotMeta` 的不可变快照。sm 层的
   `apply`/`get_snapshot_builder` 均为 `&mut self`,由 openraft sm worker 串行调度,
   因此 `last_log_id` 与快照字节天然对应同一时点(A-0002 Contract 1 的实现责任由此满足);
   用户 `Service` 侧仍须遵守 A-0002 的 Service 契约(snapshot/restore 是完整状态的序列化/替换)。
2. **信封**:`ServiceStateMachineAdapter::snapshot` 产出 `FramedSnapshot`(magic + version),
   `framework_state` 携带幂等层/ingestor offset/outbox(`crates/rr-raft/src/service_adapter.rs:375-401`)。
3. **安装**:`install_snapshot` 将字节交给 `inner.restore`(完整替换用户状态),并把
   `last_applied`/`last_membership` 替换为快照 meta 的值(`sm_adapter.rs:107-122`);
   框架状态同样整体替换:幂等层 replace-not-merge(`crates/rr-raft/src/idempotency.rs:175-194`),
   outbox 无快照态时清空(`service_adapter.rs:420-432`)。
4. **传输**:openraft 的 InstallSnapshot 分块传输;安装后 follower 从快照
   `last_log_id` 继续复制(A-0002 Contract 2 的恢复路径)。

### 强制点

| # | 位置 | 强制内容 |
|---|---|---|
| 1 | `crates/rr-raft/src/sm_adapter.rs:92-101` | builder 捕获与 `last_applied` 同时点的状态字节 |
| 2 | `crates/rr-raft/src/sm_adapter.rs:112-121` | install = 完整替换(restore + meta 替换),无合并路径 |
| 3 | `crates/rr-raft/src/service_adapter.rs:403-456` | framed 信封解码;legacy 无信封字节按用户快照透传并自动迁移 |
| 4 | `crates/rr-raft/src/idempotency.rs:165-174` | restore 语义注释即契约:replace-not-merge,保证可重复安装幂等 |
| 5 | `.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md` | openraft 契约(快照原子性、恢复锚点)的源码级核对记录 |

### 现有测试

- `crates/rr-raft/src/sm_adapter.rs:242` `test_build_snapshot_captures_state`、`:279`
  `test_install_snapshot_restores_state`、`:303` `test_snapshot_round_trip`
- `crates/rr-raft/src/service_adapter.rs:646` `snapshot_then_restore_roundtrip_through_adapter`、
  `:660` `restore_malformed_framed_bytes_errors`、`:838` `restore_records_snapshot_install_metric`
- `crates/rr-raft/src/idempotency.rs:352` `restore_replaces_existing_cache_entries_not_merge`
- `tests/integration/src/lib.rs:802` `test_snapshot_state_digest_consistent_across_nodes`(3 节点快照字节一致)
- `tests/integration/src/lib.rs:1430` `test_snapshot_catchup`(落后 follower 经快照追平)

### 已知缺口

- **【review P0#3 → WS-4 / ADR A-0011】快照生成失败被静默吞掉。**
  `get_snapshot_builder` 用 `.unwrap_or_default()`(`crates/rr-raft/src/sm_adapter.rs:93`):
  用户 `snapshot()` 返回 `Err` 时,框架以**空字节**继续构造快照——空快照带合法的
  `last_log_id` meta,可能被传播给 follower,造成"状态被空快照替换"级事故。修复方向为错误
  传播(`get_current_snapshot` 已正确传播错误,`sm_adapter.rs:127-131`,仅 builder 路径有洞)。

  **✅ 已闭环(2026-08-29,WS-4 / ADR A-0011)**:`unwrap_or_default()` 已删除,两个入口统一委托
  fallible `snapshot_builder_inner`(`crates/rr-raft/src/sm_adapter.rs:53`)——`get_snapshot_builder`
  失败返回 `None`(本次快照推迟,openraft 后续重试),`try_create_snapshot_builder`(`:138`)失败
  返回 poisoned builder(`:227`),由 `build_snapshot()` 把捕获错误表面化给 openraft——不再产出
  带合法 meta 的空快照;新增指标 `rr_snapshot_build_failures_total`(`sm_adapter.rs:72`)。
  测试:T5×3——`test_snapshot_failure_defers_policy_build`(`:481`)、
  `test_snapshot_failure_forced_build_errors`(`:493`)、
  `test_legacy_get_snapshot_builder_never_empty_on_error`(`:507`)。

## INV-6 Routing Transition Safety — 路由切换安全

### 不变量陈述

路由表更新生效后,旧 shard(group)不再收到该 partition 的新写;切换前已 ACK 的写不丢
(留在旧 group 的 Raft log/状态机中,由 INV-1 覆盖);新写全部路由到新 shard。路由判定以
ConfigProvider 中的路由表为**单一事实源**,不存在第二份缓存视图。

### 不变量链

1. **表更新**:`MemConfigProvider::add_partition` / etcd watch 更新路由表并广播
   `RoutingTableChange`(`crates/rr-config/src/mem.rs:48-62`)。
2. **每请求实时查表**:`ShardRouter::route` 不做缓存,每次请求都 `get_routing_table()` 查
   最新表(`crates/rr-router/src/router.rs:34-48`)——"切换生效"的时点就是表更新的时点。
3. **写路径路由**:`ClientDispatchImpl::submit_write` 先 `route(partition_id)` 再向该 group
   提交(`crates/rr-server/src/dispatch.rs:54-62`);未登记的 partition 直接 `NotFound`,
   无默认落点。
4. **group 生命周期跟随表**:`RrNode` watch loop 处理 `PartitionAdded`(本节点在副本集中则
   `start_group`)、`GroupMembershipChanged`(加入则 start、移出则 `stop_group`)
   (`crates/rr-server/src/node.rs:271-321`、`:374-412`)。
5. **非 leader 不写**:请求落到非 leader 节点时,openraft `ForwardToLeader` → SDK 重定向
   (`dispatch.rs:115-122`),不会因路由表之外的线索把写发给错误节点。

### 强制点

| # | 位置 | 强制内容 |
|---|---|---|
| 1 | `crates/rr-router/src/router.rs:34-48` | 路由 = 每次实时查表,partition → group 映射唯一;未命中计数 `router.route.not_found` |
| 2 | `crates/rr-server/src/dispatch.rs:54-62` | 写提交前强制路由;group 不存在即失败,无旁路 |
| 3 | `crates/rr-server/src/node.rs:374-412` | 成员变化驱动 start/stop group,使节点上运行的 group 集合收敛于路由表 |
| 4 | `crates/rr-server/src/dispatch.rs:115-122` | 非 leader 仅重定向,不在本地强制提交 |

### 现有测试

- `tests/integration/src/lib.rs:2463` `test_routing_table_update_routes_to_new_shard`(更新后新写进新 group)
- `tests/integration/src/lib.rs:2509` `test_routing_no_messages_to_old_shard_after_update`(更新后旧 group
  `last_log_index` 不变、新 group 增长)
- `tests/integration/src/lib.rs:1281` `test_partition_migration`(partition 迁移)
- `crates/rr-router/src/router.rs` 单元测试 ×8(not found / leader 查询 / 全表读取)
- `crates/rr-server/src/node.rs:963` `test_watch_loop_skips_group_not_in_replicas` 等 watch loop 测试
- `crates/rr-router/tests/router_proptest.rs` `proptest_router_transitions_route_consistency`
  (R-6 proptest:随机 add_partition/remove/reassign/membership 序列(T32/T33 的 router 层对应),
  每步后对全 partition 宇宙断言 `route()` 与独立模型一致——恰好一个 group 或明确的
  `PartitionNotFound`,无 panic、无缓存陈旧视图)
- 形式化(R-2):`formal/tla/RoutingTableSwitch.tla`(TLC 零反例,2026-09-10;Mem 整表快照与
  etcd 按 key 增量两种 watch 语义各一份 cfg;路由由视图唯一决定 / 收敛后无旧路由 /
  watch 传播最终收敛)

### 已知缺口

- **【review P0#8 → WS-7 / ADR A-0013】`GroupMeta.leader` 是错误抽象。** leader 是 raft 运行时
  易变状态,却放在静态路由表里(`crates/rr-core/src/types.rs:22-26`);两个 provider 都以
  `replicas.first().copied()` 填充(`crates/rr-config/src/mem.rs:54`、
  `crates/rr-config-etcd/src/lib.rs:404`),必然 stale;`ShardRouter::get_leader`
  (`crates/rr-router/src/router.rs:63`)把它当真实 leader 消费。**缓解现状**:写/读路径不依赖
  它做正确性决策——真实 leader 发现走 openraft 的 `ForwardToLeader`/`NotLeader{leader_addr}`
  重定向(0.2.0 已带 `leader_addr`),所以该字段当前影响的是 `get_leader_addr` 调用方的命中率
  而非正确性;修复方式(WS-7)是**删除字段**而非修启发式。

  **✅ 已闭环(2026-08-29,WS-7 / ADR A-0013)**:`GroupMeta.leader` 字段连同其消费链
  (`ShardRouter::get_leader`/`get_leader_addr`、`RouterError::NoLeader` 及 gRPC 映射、
  `router.get_leader_addr.success` metric)全部删除;`GroupMeta` 补 rustdoc 声明纯配置面语义
  (`crates/rr-core/src/types.rs:22-29`);serde 防线测试锁定新 shape 无 `leader` 键,并验证旧含
  `"leader"` 键的 JSON 被静默忽略(`types.rs:91`/`:103`);新 e2e
  `tests/integration/src/leader_redirect_failover.rs:26`
  `test_client_redirect_converges_after_leader_kill` 证明 kill leader 后 client 经 bootstrap
  轮询 + `leader_info` redirect 收敛,全程不依赖路由表 leader;`dispatch.rs:193-198` 的
  `leader_node_id: 0` 占位加约束注释:真实值只能取自 raft `metrics.current_leader`,不得恢复
  路由表字段。
- **切换窗口无 fence/epoch(非 review 条目,如实记录)**:已 route 到旧 group、尚未 commit 的
  in-flight 写在切换窗口内的语义未定义——它们会正常 commit 在旧 group(不丢,由 INV-1 覆盖),
  但"切换后旧 shard 立刻不可写"并无强制;现有集成测试以固定 300ms sleep 等待表生效
  (`tests/integration/src/lib.rs:2488`、`:2538`),属于 review P1 记录的 sleep/固定端口 flaky
  模式,根治方向见可靠性专项 R-3(确定性仿真)与 R-6(router 变迁 proptest)。
  **(2026-08-29 状态确认:仍 open,归 R-3/R-6;A-0013 只闭环 `GroupMeta.leader` 抽象,不涉及切换窗口语义。)**

## INV-7 Membership Safety — 成员变更不丢 quorum

### 不变量陈述

任何 change-membership 操作不得把任何一个 Raft group 的 voter 集合缩减到丧失 quorum 的规模
(对 rr 的部署基线:变更后 voter 数 ≥ 2;单节点集群拆除属于显式运维动作,须走专门命令而非
普通 membership 变更)。

### 不变量链(现状:链条断裂,如实记录)

1. **入口 A(Admin RPC)**:`AdminService.change_membership`
   (`crates/rr-server/src/admin.rs:535-600`)——当前**唯一**校验是 `voter_node_ids` 非空
   (`admin.rs:552-565`),随后直通 `RaftGroupManager::change_membership`
   (`crates/rr-raft/src/group_manager.rs:330-350`)→ openraft。**无 quorum-loss 守卫。**
2. **入口 B(config watch loop)**:`NodeRemoved` 事件会让每个 group 以"剔除该节点后的副本集"
   直接调 `change_membership`(`crates/rr-server/src/node.rs:340-372`),同样无守卫。
3. **入口 C(client-side 工作流,唯一存在的守卫)**:`rr-admin` 的 decommission 编排器在
   移除前检查 `remaining.len() < MIN_VOTERS_AFTER_REMOVAL`(= 2,
   `crates/rr-admin/src/decommission.rs:57`、`:174-184`),违反则 `AbortedQuorumLoss` 且不触
   集群。**该守卫只在 CLI 工作流里,绕开它的调用方(直接 Admin RPC、watch loop)不受保护。**

### 强制点

| # | 位置 | 强制内容 |
|---|---|---|
| 1 | `crates/rr-admin/src/decommission.rs:57`、`:174-184` | client-side 唯一守卫:移除后 voter < 2 即中止 |
| 2 | `crates/rr-server/src/admin.rs:552-565` | server 端仅有"非空"校验——**不构成 quorum 守卫,列为缺口证据** |

### 现有测试

- `crates/rr-admin/src/decommission.rs:339` `report_quorum_loss_does_not_count_as_progress`、
  `:396` `min_voters_after_removal_constant_is_two`(client-side 守卫的间接覆盖)
- 形式化(R-2):`formal/tla/MembershipGuard.tla`(TLC 小模型 11,545 态零反例,2026-09-10;
  n→0 无条件拒绝 / 低于下限必经 force+审计 / 审计完备 / 并发提交任意交错安全;
  植入的去 guard 反例被 TLC 抓到——`formal/tla/bugs/MembershipGuardBuggy.tla`)

### 已知缺口

- **【review P0#7 → WS-6 / ADR A-0012】server-side 无 quorum guard。** AdminService RPC 本身
  只查 empty(`admin.rs:552`/`568`);quorum-loss check 仅存在于 decommission 的 client-side
  工作流。任何直达 Admin RPC 或经 watch loop 的变更都可将 group 缩减到 1/0 voter 而丧失
  容错/可用性。修复方向(WS-6):守卫上移至 server 端共享逻辑,提供显式 `force` 逃生参数且
  必须落 audit 事件;同时修正 CONTEXT.md 中"服务端已有 quorum-loss check"的不实记载。
  验收:3 节点集群试图摘除 2 个 voter 在 server 端被拒(集成测试)。

  **✅ server 端已闭环(2026-08-29,WS-6 / ADR A-0012)**:`AdminServiceImpl::change_membership`
  守卫落地(`crates/rr-server/src/admin.rs:537-638`),判定顺序:group 存在性(not_found)→
  空集合(invalid_argument,`force` 不绕过)→ quorum guard(目标 voter <
  `MIN_VOTERS_AFTER_REMOVAL` 且未 `force` → `RaftError::UnsafeMembershipChange` →
  `Status::failed_precondition`);策略常量单一定义于 `crates/rr-core/src/membership.rs:19`,
  client-side decommission 预检查保留(UX fail-fast)但改引用共享常量;`force` 为 wire-additive
  proto 字段,放行产 warn 级 `force_accepted` audit 事件。测试:rr-core 谓词真值表 +
  admin handler 守卫测试组(`admin.rs:1119-1260`,含 force/拒绝两条 audit 断言)+ e2e
  `tests/integration/src/membership_guard.rs:166`(3→1 拒绝 / 3→2 放行 / 2→1 force 放行)
  ——验收标准达成。

  **仍 open(A-0012 follow-up)**:入口 B——watch-loop `NodeRemoved` 自动摘除
  (`crates/rr-server/src/node.rs:391-421`)——仍无守卫。A-0012 Option B 已论证不在 manager
  层下沉(否则 raft membership 与 routing table 持久分歧,且该路径无 `force` 语义);
  自动化路径守卫需单独设计,连同 R-2 TLA+ 建模一并考虑。

## INV-8 Security Gates — 生产配置无未加门控的监听端口

### 不变量陈述

在未显式设置 `allow_insecure` 的生产配置下,节点拒绝以任何"未加门控的监听端口"启动:
gRPC 平面的五个 gate 全部通过才允许 `RrNode` 构造成功;`allow_insecure=true` 是显式、带
运行时告警、带 `rr_security_insecure_mode` 指标暴露的 dev-only 逃生门。auth token 一律经
环境变量间接引用,配置文件中不落明文;引用的环境变量未设置或为空时 fail-closed(拒绝启动)。

### 不变量链

1. **配置解析**:TOML/CLI/env → `ResolvedConfig`;`[auth]` 段只存环境变量**名**
   (`crates/rr-server/src/config_file.rs:82-99`),变量未设置/为空即解析失败
   (fail-closed,`config_file.rs:946-980` 的测试)。
2. **启动门控**:`RrNode::new` 首先调用 `validate_security_invariants`
   (`crates/rr-server/src/node.rs:36-108`,调用点 `node.rs:162`),聚合**全部**违规一次报出;
   `allow_insecure` 短路(`node.rs:37-39`)并在通过时打 WARN + 暴露指标
   (`node.rs:164-187`)。
3. **五个 gate**(`node.rs:48-91`):
   - G1:RaftService 启用 ⇒ 必须配 TLS(`:48-53`);
   - G2:RaftService + TLS ⇒ `require_client_cert = true`(mTLS 是 Raft 平面**唯一**的
     对端身份机制,无 bearer 路径)(`:55-68`);
   - G3:配置了 bearer token ⇒ 必须配 TLS(token 不走明文)(`:70-75`);
   - G4:AdminService 启用 ⇒ 必须配 TLS(`:77-83`);
   - G5:AdminService 启用 ⇒ 必须设 `admin_token`(`:85-91`)。
4. **运行时强制**:gRPC server 挂 TLS(`node.rs:452-462`);ClientService 经
   `ChainedInterceptor`(bearer auth → rate limit)(`node.rs:433-440`);token 比对用
   常数时间比较(`crates/rr-network/src/auth.rs:116` 的 `BearerInterceptor::call`);
   RaftService 结构性不限流(共识流量不受 rate-limit 影响,设计决策见 A-0005)。
5. **可观测平面**:`/health` `/ready` `/metrics` 由独立 hyper server 提供
   (`crates/rr-server/src/health.rs:126-138`,请求分发 `:192-214`),`RrNode` 在
   `metrics_port` 配置时启动(`node.rs:565-573`)。

### 强制点

| # | 位置 | 强制内容 |
|---|---|---|
| 1 | `crates/rr-server/src/node.rs:36-108` | 五 gate 聚合校验,违规即 `Err` 拒绝构造 |
| 2 | `crates/rr-server/src/node.rs:162` | 门控在 `RrNode::new` 第一步执行,先于任何端口绑定 |
| 3 | `crates/rr-server/src/config_file.rs:946-980`(测试) | auth 环境变量未设置/为空 → fail-closed |
| 4 | `crates/rr-network/src/auth.rs:116` | Bearer token 校验拦截器 |
| 5 | `crates/rr-server/src/node.rs:452-462` | gRPC server TLS 装配 |

### 现有测试

- `crates/rr-server/src/node.rs:993-1111` 九个 gate 测试:`validate_security_raft_without_tls_rejects`(:993)、
  `..._raft_without_mtls_rejects`(:1006)、`..._auth_without_tls_rejects`(:1021)、
  `..._admin_without_tls_rejects`(:1037)、`..._admin_without_token_rejects`(:1055)、
  `..._full_strict_path_passes`(:1070)、`..._allow_insecure_bypasses_all`(:1084)、
  `..._minimal_dev_config_still_passes`(:1093)、`..._aggregates_all_violations`(:1099)
- `tests/integration/src/tls_auth_e2e.rs:226` `tls_mtls_bearer_auth_e2e_matrix`(TLS/mTLS/bearer 端到端矩阵)
- `crates/rr-network/src/auth.rs` 单元测试(缺/错 token 拒绝、报错不区分失败模式 `:266` 等)
- `crates/rr-server/src/config_file.rs:946-980` fail-closed 测试组

### 已知缺口

- **【review P0#4 → WS-5 / ADR A-0014】TCP raft transport 在五个 gate 之外。**
  `--raft-transport tcp` 时,`TcpRaftServer` 在 `RrNode::new` 通过门控**之后**才 bind
  (`crates/rr-server/src/main.rs:210-232`,对照 `main.rs:207`);`validate_security_invariants`
  只看 `config.raft_lookup`(gRPC 平面),不知道 TCP transport 的存在——明文 TCP Raft 可在
  无任何 gate 的情况下监听任意地址。修复方向(WS-5):门控泛化为 transport-aware;TCP 无 TLS
  时要求显式 `allow_insecure` 且 bind 必须是 loopback;**不给 TCP 加 TLS**(0.2.1+ 再议)。

  **✅ 已闭环(2026-08-29,WS-5 / ADR A-0014)**:`NodeConfig` 新增 `RaftTransport` 描述字段
  (`crates/rr-server/src/config.rs:29`,默认 `Grpc` 行为逐字节不变),`main.rs:161-176` 把
  `replace_port` 派生 hoist 为单一事实源,同时喂给门控与实际 `TcpRaftServer::bind`;
  G7(TCP bind 必须 loopback,地址不可解析同样 fail-closed)为硬 gate,前置于 `allow_insecure`
  短路(`node.rs:46-69`,短路在 `:71-73`)——**不可绕过**;G6(TCP 必须显式 `allow_insecure`,
  不论 gRPC TLS/mTLS 是否合规)并入违规聚合(`node.rs:127-143`)。测试:6 条 TCP gate 单测
  (`node.rs:1163-1282`)+ 配置解析三层测试(`config_file.rs:838-934`,CLI/TOML/优先级与
  bind 派生);原 9 条 gate 测试零回归。
- **【review P2 → 0.2.1 backlog】/metrics /ready /health 明文无 auth。**
  `crates/rr-server/src/health.rs:192-214` 的请求分发无任何鉴权;绑定地址由
  `metrics_port` 决定(`node.rs:565-573`),无 loopback 限制。修复方向:至少默认绑 loopback
  + 文档警示。**(2026-08-29 状态确认:仍 open,归 0.2.1 backlog。)**
- **【review P1 → RC 并行项】audit 配对不完整**:`TriggerSnapshot` 无 started、
  `DrainNode` 无 finished、`reset_ingestor_offset` 无 started;**ClientService mutation 完全
  无 audit**(`crates/rr-server/src/admin.rs:223`/`:358`/`:602-647` 等处)。安全事件的
  可审计性链不完整。**(2026-08-29 状态确认:仍 open,归 RC 并行项;A-0012 的
  `ChangeMembership` started/failed/force_accepted 三联为新落地部分,不在此缺口范围内。)**
- **【review P1 → RC 并行项】`constant_time_eq` 手写且长度早返回**
  (`crates/rr-network/src/auth.rs:202-211`),应换 `subtle`;`token_fingerprint` 仅 32-bit
  (`auth.rs:173-181`)。**(2026-08-29 状态确认:仍 open,归 RC 并行项。)**
- **【review P1 → WS-1】Grafana dashboard 引用不存在的 `rr_auth_rejects_total`**
  (`deploy/observability` grafana json),auth 拒绝事件实际无可观测指标——可观测性在说部分
  假话,与本不变量的"可验证性"直接相关。

  **✅ 已闭环(2026-08-29,WS-1)**:dashboard 中引用幽灵 metric 的 panel 已删除
  (`deploy/observability` grafana json 不再含 `rr_auth_rejects_total`),可观测面不再说谎;
  `rr_auth_rejects_total` 的真实实现(auth 拒绝计数)排入 RC 批次,届时 panel 随真实 metric
  一并恢复。

---

## 缺口 → 修复工作流对照表

| 缺口 | review 条目 | 所在不变量 | 修复工作流 | 设计文档 |
|---|---|---|---|---|
| rocks 持久化测试 CI 实际跳过(✅ 已闭环 2026-08-29:ci.yml test-rocks 传 `rr-storage/rocks,integration/rocks`) | P0#2 | INV-1 | WS-1(CI 真实性) | 路线图内任务级 |
| dashboard 幽灵 metric `rr_auth_rejects_total`(✅ 已闭环 2026-08-29:panel 已删;metric 实现排入 RC 批次) | P1 | INV-8 | WS-1 | 路线图内任务级 |
| snapshot builder `unwrap_or_default()` 静默吞错(✅ 已闭环 2026-08-29:fallible `snapshot_builder_inner` + poisoned builder + `rr_snapshot_build_failures_total`) | P0#3 | INV-5 | WS-4(正确性三连) | ADR A-0011 |
| IoBatcher/callback untracked spawn + `stop_group` 不 flush(✅ 已闭环 2026-08-29:显式 `close()` 协议 + `stop_group` 接线) | P0#5 | INV-1 | WS-4(shutdown durability 契约) | ADR A-0011 |
| `kafka_offset_to_request_id` 非法 bytes 静默回 0(✅ 已闭环 2026-08-29:返回 `Result` + runner fail-closed) | P0#6 | INV-3 | WS-4(返回 `Result` + 分层审计) | ADR A-0011 |
| TCP raft transport 无安全 gate(✅ 已闭环 2026-08-29:G6/G7 transport-aware 门控,G7 不可绕过) | P0#4 | INV-8 | WS-5(transport-aware 门控) | ADR A-0014 |
| change-membership 无 server-side quorum guard(✅ server 端已闭环 2026-08-29;**watch-loop `NodeRemoved` 自动摘除仍 open** → A-0012 follow-up,连同 R-2 设计) | P0#7 | INV-7 | WS-6(守卫上移 + force 逃生) | ADR A-0012 |
| `GroupMeta.leader` stale 错误抽象(✅ 已闭环 2026-08-29:字段与消费链删除,leader 发现走 SDK redirect) | P0#8 / 设计#5 | INV-6 | WS-7(删除字段,leader 发现走 SDK redirect) | ADR A-0013 |
| 无 linearizable read 3 节点集成测试 | A-0009 follow-up | INV-2 | 路线图 RC 并行项 | — |
| audit 配对不完整 / ClientService mutation 无 audit | P1 | INV-8 | 路线图 RC 并行项 | — |
| `constant_time_eq` 手写 / `token_fingerprint` 32-bit | P1 | INV-8 | 路线图 RC 并行项 | — |
| 路由切换无 fence/epoch,测试依赖固定 sleep | P1(sleep/flaky)关联 | INV-6 | R-3(确定性仿真)/ R-6(proptest) | 可靠性专项 |
| /metrics /ready 明文无 auth | P2 | INV-8 | 0.2.1 backlog | — |
| outbox 缺集群级换主续投端到端测试 | 无(review 未列) | INV-4 | R-4 / R-8 | 可靠性专项 |

修复工作流定义见 `.sisyphus/plans/rr-v0.2.0-release-roadmap.md`(WS-1~WS-8)与
`.sisyphus/plans/rr-reliability-program.md`(R-1~R-8)。

## 后续方向:不变量执法

本文档(v1)是不变量执法的**登记表**;登记之后是让它**可执行**。路线图 Phase 1 可靠性专项
(`.sisyphus/plans/rr-reliability-program.md`)中的两个项目直接以此为目标:

- **R-2(TLA+ 形式化)**:对 rr 扩展协议建模——membership guard(INV-7)、routing 切换
  (INV-6)、output 水线(INV-4)、offset→request_id 链(INV-3)。openraft 核心已有自己的
  验证,不重复。本文档的不变量陈述是 TLA+ spec 中 `Invariant` 定义的直接输入。
- **R-4(crash-recovery harness + 存储故障注入)**:kill -9 随机点 + 重启 + durability
  oracle("已 ACK 必存活" = INV-1 的可执行形式);fail fsync/torn write/ENOSPC 注入层。与
  WS-4 的 `close()` 协议衔接,把 INV-1 从"代码走读可论证"变为"每夜 CI 可证伪"。

配套地,R-5(线性一致检查器)把 INV-2 变为历史检查,R-6(proptest/fuzz 扩展)把 INV-3/INV-6
的边界(幂等 LRU 逐出、router 变迁交错)变为性质测试。每条不变量闭环后,按维护规则第 4 条
更新本文档对应缺口条目。
