---
sidebar_position: 8
---

# 系统级不变量目录 / System Invariants

本文是 Rr 对外系统属性的登记表：每条属性必须有可判定的不变量、强制链、代码强制点
（`file:line`）、测试和已知缺口。
This document registers Rr's advertised system-level properties. Each property has a decidable invariant,
an enforcement chain, code enforcement points (`file:line`), tests, and known gaps.

## 目的与维护 / Purpose and Maintenance

本文随代码演进。新增一致性、投递或安全承诺前，必须先登记不变量；未登记的对外属性不视为已承诺。
2026-08-16 全量评审（`.sisyphus/evidence/rr-full-review-2026-08-16.md`）发现的主要根因是“不变量存在，
但某层未强制”；本登记用于显式追踪这些强制点。
This is a living document. Register new consistency, delivery, or security commitments before implementing
them; unregistered properties are not advertised guarantees. The 2026-08-16 review
(`.sisyphus/evidence/rr-full-review-2026-08-16.md`) identified a recurring root cause: an invariant existed
but one layer did not enforce it. This registry makes those enforcement points explicit.

ADR（`.sisyphus/adr/`）记录设计决策及权衡；本文记录当前运行时承诺及执法状态。ADR 可冻结，本文必须
保持准确。
ADRs in `.sisyphus/adr/` record design decisions and tradeoffs. This document records current runtime
commitments and enforcement. ADRs may remain frozen; this registry must stay accurate.

维护规则 / Maintenance rules:

1. 改动强制点时，同一 PR 更新本文的 `file:line`；评审者检查行号是否仍准确。
   Update `file:line` references in the same PR as enforcement changes; reviewers must check them.
2. 行号基线为 `HEAD = ba69cc8`（2026-08 全量评审），之后靠同步更新维持准确，不保证任意历史 commit
   可回溯。
   Line references use `HEAD = ba69cc8` (the 2026-08 review) as their baseline and are maintained by later
   updates; they are not reproducible against arbitrary historical commits.
3. 缺口须引用评审条目或 issue 及修复工作流（WS-x/R-x，见
   `.sisyphus/plans/rr-v0.2.0-release-roadmap.md`）。

   Every gap must cite a review item or issue and its WS-x/R-x remediation workflow in that roadmap.
4. 闭环缺口保留并注明版本，不删除历史记录。
   Keep resolved gaps with their closing version; do not erase their history.
5. CHANGELOG `Added` 的能力宣称、文档中的 guarantee/must/never、带语义承诺的新 public API，均须先登记
   INV 与测试。`scripts/check-invariants.sh` 的阻塞式 `invariants-lint` 和 PR 模板负责检查。
   Capability claims in CHANGELOG `Added`, guarantee/must/never language in docs, and new public APIs with
   semantic commitments all require an INV entry and tests first. The blocking `invariants-lint` check in
   `scripts/check-invariants.sh` and the PR template enforce this rule.

**阅读约定 / Reading convention**：“不变量”是可判定承诺；“链”分配各层责任；“强制点”列出代码位置；
“测试”列出证据；“缺口”区分开放问题与已闭环历史。
An invariant is a decidable promise; the chain assigns layer responsibilities; enforcement points locate
the code; tests provide evidence; gaps distinguish open issues from resolved history.

## INV-1 Durable Append — 已 ACK 的写必存活 / Acknowledged Writes Survive

### 不变量 / Invariant

`client_write` 成功 ACK 的 entry，在单点进程崩溃或机器断电并重启后，必须仍存在于该节点 Raft log，或
由其产生的状态机状态/快照中；vote 必须同等级持久化以防止脑裂。
After a successful `client_write` ACK, the entry must survive a single-process crash or machine power loss
and restart in that node's Raft log, derived state-machine state, or snapshot. Votes require equivalent
durability to prevent split brain.

此承诺以 `DurabilityPolicy::Strict` 为前提。`ReplicatedOnly`/`TestOnly` 显式放弃单节点持久性，不在本
不变量范围内；启动时会告警。
This promise requires `DurabilityPolicy::Strict`. `ReplicatedOnly`/`TestOnly` explicitly give up
single-node durability and are outside this invariant; startup emits a warning.

### 强制链 / Enforcement Chain

1. `ClientService.request` → `ClientDispatchImpl::submit_write`
   （`crates/rr-server/src/dispatch.rs:77`）调用 `raft.client_write`。
   The client request enters `submit_write`, which calls `raft.client_write`.
2. openraft `RaftCore` 调用 `RaftLogStorage::append`，持有 `IOFlushed` callback；只有
   `io_completed(Ok)` 后 entry 才可 commit/apply/ACK。
   Openraft persists through `append`; only `io_completed(Ok)` permits commit, apply, and ACK.
3. `RocksLogStorage::append` 编码 `BatchOp` 并提交 per-group `IoBatcher`
   （`crates/rr-raft/src/rocks_log_storage.rs:214-252`）。

   Append submits encoded batch operations to the per-group batcher.
4. `IoBatcher` 合并为一次 `write_batch()`，成功后才应答等待者
   （`crates/rr-raft/src/io_batcher.rs:188-201`）。

   The batcher answers waiters only after the combined `write_batch()` succeeds.
5. `RocksStorage::write_batch` 使用 `durability.write_options()` 调 `db.write_opt`
   （`crates/rr-storage/src/rocks.rs:518-543`）；Strict 下 `WriteOptions::set_sync(true)`，每次批写
   伴随 WAL fsync。
   Strict sets synchronous writes, so each batch includes a WAL fsync.
6. `stop_group` 停后台任务、等待在途写归零（上限 5 秒），再 `raft.shutdown()`
   （`crates/rr-raft/src/group_manager.rs:359-434`）。

   Shutdown stops background work, bounds the in-flight-write drain to five seconds, then shuts down Raft.

### 强制点 / Enforcement Points

| # | 位置 / Location | 强制内容 / Enforcement |
| --- | :--- | :--- |
| 1 | `crates/rr-storage/src/rocks.rs:114-129` | Strict → `set_sync(true)`；TestOnly → `disable_wal(true)`。 / Durability options implement Strict and TestOnly semantics. |
| 2 | `crates/rr-raft/src/io_batcher.rs:188-201` | 整批成功后才向全部 caller 返回 `Ok`，ACK 不先于落盘。 / All callers succeed only after the batch write; no ACK precedes persistence. |
| 3 | `crates/rr-storage/src/rocks.rs:333-339` | 非 Strict 启动时 `warn!`，属于软门控。 / Non-Strict startup warns; this is a soft gate. |
| 4 | `crates/rr-raft/src/rocks_log_storage.rs:234-249` | 空批次立即完成；非空批次经 batcher，无先 ACK 路径。 / Empty batches complete immediately; all others pass through the batcher. |
| 5 | `crates/rr-raft/src/group_manager.rs:405-417` | 等 `in_flight_writes` 归零后 shutdown Raft。 / Raft shutdown waits for in-flight writes to drain. |
| 6 | `crates/rr-server/src/main.rs` `with_raft_log_dir` | ADR A-0016：`--storage rocks` 将日志写入 `{data_dir}/raft-log/{group_id}`。 / Rocks mode persists each group's Raft log under that path. |
| 7 | `crates/rr-raft/src/io_batcher.rs` `acquire_bytes` / `max_inflight_bytes` | 默认 64 MiB 在途字节预算；permit 持有到 flush 完成，向提案管线施加背压。 / The default 64 MiB budget holds permits through flush and backpressures producers. |
| 8 | `crates/rr-core/src/ingress.rs:29`; callers `crates/rr-server/src/dispatch.rs:98`, `crates/rr-raft/src/group_manager.rs:826`, `:887` | propose 前以 `INVALID_ARGUMENT` 拒绝带 house magic 的畸形/未来版本帧。 / Malformed or future house-magic frames are rejected before propose. |
| 9 | `crates/rr-raft/src/tcp_transport.rs:125-132`, `:245-256`, `:307-309` | 池级单调 request ID + `(group_id, request_id)` demux；错组不投递，撞号响亮失败。 / Monotonic pool IDs and compound demux prevent cross-group delivery and fail loudly on collisions. |

### 现有测试 / Tests

- `tests/integration/src/lib.rs` `test_restart_persistence`、`test_node_failure_recovery`、
  `test_sequenced_writes_preserved_across_restart`（2026-10-21 前名
  `test_crash_during_write_preserves_acked_entries`）、`test_concurrent_writes_under_chaos_linearizability`。
  覆盖重启/故障恢复、abrupt drop 后 ACK 写存活、chaos 下持久性与线性一致回归。
  These cover restart/failure recovery, acknowledged writes across abrupt drop, and durability plus
  linearizability under chaos.
- `crates/rr-raft/src/rocks_log_storage.rs` `test_rocks_vote_persistence`、`test_rocks_log_persistence`、
  `test_save_vote_waits_for_pending_appends`、`test_append_visible_before_flush_via_overlay`、
  `test_truncate_after_does_not_resurrect_pending_appends`、
  `test_purge_does_not_resurrect_pending_appends`、`test_pipelined_appends_concurrent_reads_never_gap`、
  `test_rocks_get_log_state_recovers_tail_of_large_log`。
  覆盖 vote/log reopen、vote 前排空 append、pending overlay 的 submitted⇒visible、删除屏障和大日志尾部恢复。
  These cover vote/log reopen, vote ordering, submitted⇒visible through the pending overlay, deletion
  barriers, and large-log tail recovery.
- `crates/rr-raft/src/io_batcher.rs`、`io_batcher_proptest.rs`、`storage_fault_tests.rs`：
  `batcher_exits_cleanly_on_drop`、`flush_barrier_*`、`submit_with_hook_*`、
  `byte_budget_backpressures_submitters`、`proptest_io_batcher_interleavings_exactly_once_and_ordered`、
  `proptest_io_batcher_write_error_propagates_to_whole_batch`、`fail_flush_surfaces_err_and_batcher_worker_survives`、
  `torn_write_batch_leaves_prefix_only_and_error_propagates`、`enospc_propagates_to_all_pending_batch_callers`、
  `slow_disk_latency_completes_correctly_just_slower`。
  覆盖 worker 生命周期、屏障/hook、背压、并发恰好一次与顺序，以及故障注入。
  These cover worker lifecycle, barriers/hooks, backpressure, exactly-once ordering under concurrency, and
  injected storage faults.
- `tests/crash/src/scenarios.rs` `scenario_a_graceful_sigterm`、`scenario_b_sigkill`、
  `scenario_c_snapshot_purge_kill`；`CRASH_SEED` 可复现随机故障。
  Process-level SIGTERM/SIGKILL and snapshot-purge crash tests cover acknowledged-write recovery;
  `CRASH_SEED` reproduces fault sequences.
- `tests/integration/src/raft_log_durability.rs`：
  `test_rocks_raft_log_rolling_restart_preserves_acked_writes`；`crates/rr-raft/src/group_manager.rs`：
  `test_start_group_persists_log_with_rocks_base_path`、`test_start_group_rocks_shim_still_persists`。
  覆盖 ADR A-0016 的滚动重启、复制收敛、线性一致读和持久化日志接线。
  These cover rolling restart, replication convergence, linearizable reads, and persisted-log wiring.
- `scripts/soak.sh`：ack-log 回放、零 panic、lag 收敛三种 oracle；`SOAK_SEED` 复现，
  `--soak-unkeyed-ratio` 覆盖 unkeyed 道，批处理 × soak 空转为硬错误。
  Soak uses ack-log replay, panic-free logs, and lag convergence as oracles. `SOAK_SEED` reproduces faults;
  unkeyed traffic is configurable, and idle batching-plus-soak is a hard error.
- `crates/rr-core/src/ingress.rs`、`crates/rr-server/src/dispatch.rs`、
  `crates/rr-raft/src/group_manager.rs` `magicless_payloads_pass_untouched`、
  `well_formed_batch_frame_passes`、`malformed_batch_frame_is_rejected`、`future_batch_version_is_rejected`、
  `malformed_framed_entry_is_rejected`、`test_dispatch_rejects_malformed_house_magic_payload`、
  `test_write_batch_rejects_malformed_frame_before_propose`、`test_write_rejects_malformed_framework_frame`。
  覆盖无 magic 透传、合法批帧、畸形/未来帧在写与批写入口的拒绝。
  These cover magicless passthrough, valid batches, and rejection of malformed/future frames at both write
  entry points.
- `crates/rr-raft/src/tcp_transport.rs` `pool_allocates_unique_request_ids`、
  `response_with_wrong_group_is_not_delivered`、`concurrent_groups_share_connection_without_overwrite_or_misdelivery`。
  覆盖跨组共享连接的 request ID 唯一性与响应归属。
  These verify unique request IDs and correct response ownership on shared cross-group connections.
- 另有 `crates/rr-storage/src/rocks.rs` `test_flush_covers_every_cf`、
  `crates/rr-raft/src/sm_adapter.rs` `test_membership_sidecar_torn_tmp_does_not_resurrect`、
  `crates/rr-admin/src/node_reset.rs` `refuses_when_lock_file_is_flock_held`。

  Additional tests cover all-CF flush, torn membership-sidecar recovery, and fail-closed node-reset locking.

### 缺口 / Gaps

当前无开放缺口；已闭环的 shutdown drain、CI feature、SDK 假 ACK、pending overlay、日志投毒、命名空间
上限、TCP demux 与 node-reset 锁问题见文末历史表。
No gaps are currently open. Resolved shutdown-drain, CI-feature, SDK-fake-ACK, pending-overlay,
log-poisoning, namespace-cap, TCP-demux, and node-reset-lock issues are listed in the history table below.

## INV-2 Linearizable Read — 线性一致读 / Linearizable Reads

### 不变量 / Invariant

`ReadLinearizable` 的结果必须反映至少到该读发起时已完成 Raft commit 的全部写。非 Leader 不得自行作答，
必须返回 `leader_id`/`leader_addr` 重定向，由 SDK 转向 Leader 重试。
A `ReadLinearizable` result must reflect every write committed by Raft at least through the read's
invocation. A non-leader must not answer locally; it returns `leader_id`/`leader_addr` for SDK redirect and
retry.

### 强制链 / Enforcement Chain

1. `ClientService.read_linearizable`（`crates/rr-network/src/client_service.rs:213-239`；契约见
   `crates/rr-network/proto/client.proto:13-18`）。

   The protocol exposes a distinct linearizable-read RPC.
2. `ClientDispatchImpl::submit_linearizable_read` 经 `ShardRouter` 定位组
   （`crates/rr-server/src/dispatch.rs:152-159`）。

   Dispatch routes the request to its group.
3. `RaftGroupManager::linearizable_read` 使用 `get_read_linearizer(ReadPolicy::ReadIndex)`
   （`crates/rr-raft/src/group_manager.rs:810`）。

   The leader confirms quorum and obtains the commit point through ReadIndex.
4. openraft `ForwardToLeader` 映射为 `RaftError::NotLeader { leader, leader_addr }`
   （`group_manager.rs:813-817`），dispatch 转为 proto `leader_info`（`dispatch.rs:166-173`）。

   Forwarding errors become typed leader redirects on the wire.
5. `linearizer.await_ready(&raft)` 等本地 apply 越过 ReadIndex commit 点
   （`group_manager.rs:825-829`）。

   The local state machine catches up through the ReadIndex commit point.
6. 只有追平后才 `local_read`（`group_manager.rs:831`）。

   Only then is the local state machine read.

### 强制点 / Enforcement Points

| # | 位置 / Location | 强制内容 / Enforcement |
| --- | :--- | :--- |
| 1 | `crates/rr-raft/src/group_manager.rs:810` | 强制 `ReadPolicy::ReadIndex`，无绕过一致性检查的快路径。 / ReadIndex is mandatory; no consistency-bypass fast path. |
| 2 | 同文件 `:825` / Same file | `await_ready` 保证结果不旧于 ReadIndex commit index。 / The result cannot predate the ReadIndex commit point. |
| 3 | `crates/rr-server/src/dispatch.rs:241-267` | 非 Leader 仅回 `NotLeader`；Leader 未知时 gRPC 映射为可重试 `Unavailable`，禁止空 payload 假成功。 / Non-leaders return no payload; unknown leaders become retriable `Unavailable`, never fake success. |
| 4 | `crates/rr-network/src/client_service.rs:101-124` | trait 契约要求非 Leader 返回 `NotLeader`，`(None, None)` 报 `Unavailable`。 / The trait requires redirect and maps ambiguous empty hints to `Unavailable`. |

### 现有测试 / Tests

- `crates/rr-network/src/client_service.rs`：
  `test_read_linearizable_routes_to_submit_linearizable_read`、
  `test_read_linearizable_surfaces_leader_redirect_to_proto`、
  `test_read_linearizable_leader_unknown_maps_to_unavailable`、
  `test_read_linearizable_ambiguous_redirect_maps_to_unavailable`。
  覆盖路由、重定向、未知或歧义 Leader 的 `Unavailable` 映射。
  These cover routing, redirects, and `Unavailable` for unknown or ambiguous leaders.
- `crates/rr-server/src/dispatch.rs` `test_dispatch_linearizable_read_leader_unknown_is_not_served`；
  `crates/rr-client/src/lib.rs` `test_read_linearizable_no_fake_success_when_leader_unknown` /
  `test_read_linearizable_converges_once_leader_is_known`。
  覆盖 dispatch 不假答及 SDK 从未知 Leader 收敛。
  These verify no fake dispatch answer and SDK convergence once the leader becomes known.
- `tests/integration/src/lib.rs` `test_read_linearizable_redirect_from_follower`；
  `test_concurrent_writes_under_chaos_linearizability` 同时把混合读写历史送 checker。
  Integration covers follower redirect and mixed read/write histories under chaos.
- `tests/integration/src/linearizability.rs`：
  `checker_minimizes_violation_to_conflicting_pair`、`checker_rejects_*` ×4、`checker_accepts_*` ×5、
  `checker_perf_*` ×2、`checker_downsamples_oversized_history`、
  `history_records_completed_and_unknown_ops`、`test_kv_register_linearizable_under_partition`。
  Porcupine 风格检查器覆盖反例最小化、接受/拒绝校准、性能和分区下 KV register 历史。
  The porcupine-style checker covers counterexample minimization, acceptance/rejection calibration,
  performance, and KV-register histories under partitions.

### 缺口 / Gaps

当前无开放缺口；原“缺 3 节点集成测试”已于 2026-09-04 闭环，见文末。
No gap is open; the missing 3-node integration test was resolved on 2026-09-04, as recorded below.

## INV-3 Exactly-Once Ingest — 摄入恰好一次 / Exactly-Once Ingest

### 不变量 / Invariant

同一 Kafka offset 无论因换主、恢复、seek 或 at-least-once 重投而重放多少次，用户可见的
`Service::handle()` 效果恰好一次；重复投递返回首次执行的缓存响应。
No matter how often one Kafka offset is replayed through leadership changes, recovery, seeks, or
at-least-once delivery, the user-visible `Service::handle()` effect occurs exactly once and duplicates
return the first cached response.

去重窗口受 LRU 容量限制（默认 10 000，`crates/rr-raft/src/idempotency.rs:68`）。窗口外重放返回
`IdempotencyExpired` 并拒绝执行，属于 fail-closed，不会重复执行。
The deduplication window is LRU-bounded (default 10,000). Replays outside it return `IdempotencyExpired`
and fail closed instead of executing again.

### 强制链 / Enforcement Chain

1. ingestor supervisor 仅 Leader poll（`crates/rr-raft/src/ingestor_runner.rs:184`, `:345`）；
   follower 从 Raft log 获得 entry，不各自消费 Kafka。
   Only leaders poll Kafka; followers receive the same entries through Raft.
2. `AutoIdempotency::offset_to_request_id` 生成单调 request ID，与 `request_type` 组成
   `IdempotencyKey` 写入 `FramedEntry`（`ingestor_runner.rs:391-397`）。
   Each offset becomes a monotonic request ID in a framed idempotency key.
3. 整批经 `write_batch` 提交（`:415`），apply 成功后才 `commit_applied` 推进 offset（`:434-443`）。
   The batch commits through Raft before the upstream offset advances.
4. `ServiceStateMachineAdapter::apply` 走 `apply_keyed`
   （`crates/rr-raft/src/service_adapter.rs:355-373` → `:285-350`）；`IdempotencyLayer::decide`
   分派 Replay/Collision/Expired/New（`crates/rr-raft/src/idempotency.rs:107-143`）。Replay 与 Collision
   均返回缓存成功响应；Collision 只比较 `client_id` 并记录警告，不比较 payload、不拒绝；Expired 拒绝，
   New 执行并记录。
   Apply returns cached successful responses for Replay and Collision. Collision compares only `client_id`
   and logs a warning, without comparing payloads or rejecting; Expired rejects, while New executes and records.
5. watermarks + LRU 随快照 capture/restore（`idempotency.rs:148-160`, `:175-194`，replace-not-merge）；
   新 Leader 从 `framework_state.ingestor.last_applied_offset` 恢复 seek
   （`ingestor_runner.rs:234-259`）。seed 失败直接退出，禁止借 `auto.offset.reset` 静默从头重放
   （`:203-213`）。

   Snapshot state restores deduplication and seek position. Seed failure stops the supervisor rather than
   silently replaying from earliest.

### 强制点 / Enforcement Points

| # | 位置 / Location | 强制内容 / Enforcement |
| --- | :--- | :--- |
| 1 | `ingestor_runner.rs:184`, `:345` | 仅 Leader poll，失主即退出。 / Only leaders poll; stepping down exits the loop. |
| 2 | `ingestor_runner.rs:391-397` | 每条 entry 携带 offset 派生的幂等键。 / Every entry carries an offset-derived key. |
| 3 | `crates/rr-raft/src/idempotency.rs:107-131` | `Replay`、`Collision`、`Expired`、`New` 四路语义。 / Four-way decisions: `Replay`, `Collision`, `Expired`, and `New`. |
| 4 | `crates/rr-raft/src/service_adapter.rs:291-348` | Replay 不调用 Service，只有 New 执行并 record。 / Replay skips the Service; only New executes and records. |
| 5 | `service_adapter.rs:375-401`, `:403-456` | 幂等状态随 `FramedSnapshot` 快照/恢复。 / Idempotency state is captured and restored in framed snapshots. |
| 6 | `ingestor_runner.rs:203-213` | seed 失败为显式错误，无静默 replay-from-earliest。 / Seed failure is explicit and never silently replays from earliest. |
| 7 | `service_adapter.rs:288-292` | 批拆分仅对 raw client bytes；FramedEntry/ingestor entry 原样 apply。 / Batch splitting applies only to raw client bytes; framed ingestor entries apply verbatim. |
| 8 | `crates/rr-ingestor-kafka/src/backend.rs:397-414` | poll 错误前先交付已消费部分批，当前任期不静默跳命令。 / A poll error first delivers the consumed partial batch, avoiding silent command skips. |

### 现有测试 / Tests

- `crates/rr-raft/src/idempotency.rs` 12 个单测，包括 `recorded_key_replays`、
  `expired_when_below_watermark_and_evicted`、`first_request_id_zero_is_new_not_expired`、
  `capture_restore_roundtrip_preserves_replay_behaviour`、`new_namespace_beyond_cap_is_rejected_loudly`。
  覆盖重放、过期、ID 0、快照恢复与 1024 namespace 上限。
  Unit tests cover replay, expiry, ID zero, snapshot restore, and the 1024-namespace cap.
- `crates/rr-raft/src/service_adapter.rs`：
  `apply_keyed_replay_returns_cached_without_calling_service`、
  `apply_keyed_expired_returns_idempotency_expired_outcome`、
  `snapshot_restore_preserves_idempotency_replay_behaviour`、
  `apply_ingestor_entry_with_batch_magic_payload_applies_verbatim`、
  `apply_framed_entry_does_not_split_inner_batch_payload`。
  覆盖 keyed 重放/过期、恢复与 ingestor 帧不被批拆分干扰。
  These cover keyed replay/expiry, restoration, and verbatim framed-ingestor application.
- `tests/integration/src/lib.rs` `test_idempotency_dedup_survives_leader_switch`、
  `test_committed_entry_applies_exactly_once_after_leader_crash`；
  `tests/integration/src/cross_tier_ingestor.rs`：
  `test_ingestor_survives_leader_switch_no_message_loss`。
  集成覆盖换主/崩溃去重与重叠 offset 流不丢不重。
  Integration covers deduplication across leader changes/crashes and overlapping offset streams.
- `crates/rr-raft/src/ingestor_runner.rs` `supervisor_exits_when_seed_offset_fails`、
  `parked_supervisor_exits_when_cancel_sender_dropped`；`crates/rr-ingestor-kafka/src/mock.rs`：
  `fail_next_poll_after_returns_partial_batch_then_errors`、
  `fail_next_poll_after_more_than_queued_returns_all_then_errors`。
  覆盖 seed fail-closed、取消生命周期与 poll 错误前部分批交付。
  These cover fail-closed seeding, cancellation lifecycle, and partial-batch delivery before poll errors.
- `crates/rr-raft/src/idempotency_proptest.rs`
  `proptest_idempotency_lru_watermark_model_parity`；形式化 `formal/tla/OffsetExactlyOnce.tla`
  （2026-09-10，TLC 558 态零反例），反例模型 `formal/tla/bugs/OffsetClampBug.tla`。
  Model-based property tests and TLA+ pin watermark, replay, and expiry semantics.
- `crates/rr-server/src/dispatch.rs` `test_dispatch_keyed_payload_bypasses_ingress_validation` 固定结构前提：
  keyed `FramedEntry` 在 apply 侧解码校验。
  This test pins the structural assumption that keyed framed entries are decoded and validated at apply.

### 缺口与观察 / Gaps and Observations

当前无开放正确性缺口；非法 offset 静默回 0 与 Kafka poll 丢部分批已闭环，见文末。映射失败现在由
`rr_ingestor_offset_map_failures_total` 计数并保持 fail-closed。
No correctness gap is open; silent zero mapping for invalid offsets and Kafka partial-batch loss were
resolved, as recorded below. Mapping failures now increment `rr_ingestor_offset_map_failures_total` and
remain fail-closed.

分层观察：rr-raft 核心出现 Kafka/offset 概念是否合适仍需依赖审计，属于架构观察而非已确认正确性缺口。
Architectural observation: whether Kafka/offset concepts belong outside rr-raft still warrants dependency
review; this is not a confirmed correctness gap.

## INV-4 At-Least-Once Output — 输出至少一次 / At-Least-Once Output

### 不变量 / Invariant

`Service::handle_with_outputs` 在已 commit 并 apply 的 entry 上产生的每个 Output 最终都会投递到目标；
失败按指数退避无限重试，水线只在成功后推进，不静默丢弃。语义为 at-least-once，换主可能重投，下游以
`idempotency_key` 去重。
Every Output produced by `Service::handle_with_outputs` for a committed and applied entry is eventually
delivered. Failures retry indefinitely with exponential backoff, the watermark advances only after success,
and no path silently drops output. Delivery is at-least-once: failover can redeliver, and downstream uses
`idempotency_key` for deduplication.

### 强制链 / Enforcement Chain

1. `handle_and_collect_outputs` 收集输出，`append_outputs` 赋单调 `seq` 并加入 `pending`
   （`crates/rr-raft/src/service_adapter.rs:234-238`, `:219-232`）。

   Apply assigns monotonic sequence numbers and records pending outputs.
2. `output_runner` 仅 Leader 进入 send loop，follower park
   （`crates/rr-raft/src/output_runner.rs:122-136`, `:167`）。

   Only leaders send; followers remain parked.
3. `dispatch_one` 按 target 分发（`:260-300`）：RaftWrite 以 `seq` 派生幂等键，KafkaPublish 透传
   `idempotency_key`。

   Dispatch derives idempotency for Raft writes and forwards the Kafka key.
4. 仅成功后 `advance_output_watermark(seq)`（`:214-230` →
   `crates/rr-raft/src/group_manager.rs:918-936` → `service_adapter.rs:160-170`）；失败从 100ms
   退避到 30s 上限后重试（`output_runner.rs:45-49`, `:231-251`）。
   Success advances the watermark and removes delivered entries; failure leaves them pending and retries.
5. pending/sent_watermark/next_seq 随快照持久化（`service_adapter.rs:387-398`, `:420-432`）。水线推进
   本身不经 Raft 复制，快照点后的推进对新 Leader 不可见，因此允许重投。
   Outbox state survives through snapshots. The watermark itself is not Raft-replicated, so post-snapshot
   progress may be invisible to a new leader, permitting redelivery.

### 强制点 / Enforcement Points

| # | 位置 / Location | 强制内容 / Enforcement |
| --- | :--- | :--- |
| 1 | `output_runner.rs:122`, `:167`, `:197` | 每轮与每条 output 前复查 Leader。 / Leadership is checked each loop and before each output. |
| 2 | `output_runner.rs:216` | 只有 `dispatch_one` 返回 `Ok` 才推进水线。 / Only successful dispatch advances the watermark. |
| 3 | `output_runner.rs:248-250` | 失败退避重试，无丢弃分支。 / Failure retries with backoff; there is no drop branch. |
| 4 | `service_adapter.rs:152-158` | `pending_outputs` 明确定义为 `seq > sent_watermark`。 / Pending output is exactly the sequence range above the watermark. |
| 5 | `service_adapter.rs:387-398`, `:420-432` | outbox 状态进出 `FramedSnapshot.framework_state.outbox`。 / Outbox state round-trips through framed snapshots. |
| 6 | `output_runner.rs:379-388` | `raft_write_idem_key` 使用 `rr.output.g<N>` 按源组命名空间化，消除 seq 跨组碰撞。 / Source-group namespacing prevents cross-group sequence collisions. |

### 现有测试 / Tests

- `crates/rr-raft/src/output_runner.rs` `dispatch_one_kafka_publish_success`、
  `dispatch_one_kafka_publish_propagates_failure`、`dispatch_one_raft_write_requires_client`、
  `output_handle_stop_completes`、`raft_write_idem_key_is_namespaced_per_source_group`、
  `run_send_loop_reports_outbox_unsupported`、`run_send_loop_parks_when_group_stopped`、
  `send_backoff_is_cancellable_via_stop`。
  覆盖目标分发、错误传播、命名空间幂等键、unsupported 上报、停组与可取消退避。
  These cover dispatch, failure propagation, namespaced keys, unsupported reporting, stopped groups, and
  cancellable backoff.
- `crates/rr-raft/src/service_adapter.rs` `apply_collects_outputs_and_assigns_monotonic_seq`、
  `snapshot_restore_roundtrip_preserves_outbox_state`；`formal/tla/OutputWatermark.tla`
  （2026-09-10，TLC 280k 态零反例）。
  覆盖单调 seq、快照恢复与水线的无幻影推进/保序/换主重复窗口。
  These cover monotonic sequences, snapshot restoration, and watermark safety with explicit failover
  duplicates.
- `crates/rr-core/src/output.rs` 与 `deferred.rs` 覆盖 Output 构造/target/bincode roundtrip、
  `HandleResult`、KafkaProducer object-safety、noop 响亮错误、`DeferredApply::into_response` 与
  非 deferred 调用方的 Pending 防御与 `rr_deferred_pending_via_non_deferred_total` 计数。
  Core tests cover output construction and encoding, result forms, producer behavior, and defensive
  deferred-response mapping with `rr_deferred_pending_via_non_deferred_total`.
- `tests/integration/src/outbox_failover_kafka.rs`：
  `test_inv4_outbox_delivers_outputs_kafka`、`test_inv4_outbox_redelivers_after_leader_failover_kafka`、
  `test_inv4_outbox_no_delivery_without_attached_runner`。
  覆盖 Kafka 投递、换主重投后去重恰好一次，以及无 runner 的负面对照。
  These cover Kafka delivery, deduplicated exactly-once effects after failover redelivery, and a no-runner
  negative control.
- `tests/integration/src/outbox_failover_raft_write.rs`
  `test_inv4_outbox_raft_write_duplicates_absorbed_by_target_group`；
  `tests/integration/src/outbox_restart_rocks.rs`
  `test_inv4_outbox_watermark_survives_snapshot_restart`（rocks-gated）。
  覆盖目标组幂等吸收重投与 rocks 重启后的快照水线连续性。
  These cover target-group duplicate absorption and snapshot-watermark continuity across RocksDB restart.

### 缺口 / Gaps

组不支持 outbox 时，`pending_outputs` 失败会使 runner `warn!` 后退出（`output_runner.rs:171-176`），
该组暂停投递直到下次成为 Leader。这是显式降级而非静默丢弃，但除日志/metric 外没有额外告警通道。
If a group does not support outbox, a `pending_outputs` failure makes the runner warn and exit. Delivery
pauses until that group next becomes leader. This is explicit degradation, not silent loss, but no alert
channel exists beyond logs/metrics.

原集群级换主/重启覆盖空白已于 2026-10-22 闭环；RaftWrite 跨源组键碰撞已于 2026-09-23 闭环，见文末。
The former cluster failover/restart coverage gap closed on 2026-10-22, and cross-source-group RaftWrite key
collisions closed on 2026-09-23; see below.

## INV-5 Snapshot Consistency — 快照一致性 / Snapshot Consistency

### 不变量 / Invariant

快照对应确定的 log index：`SnapshotMeta.last_log_id` 之前的全部 apply 效果（用户状态及幂等层、ingestor
offset、outbox）都包含在内，之后的不含。restore 完整替换而非合并；InstallSnapshot 后 follower 与 Leader
在该 index 的状态一致。
A snapshot represents one log index. It contains every apply effect through `SnapshotMeta.last_log_id`,
including user state, idempotency, ingestor offset, and outbox, and excludes later effects. Restore replaces
rather than merges; after InstallSnapshot, the follower matches the leader at that index.

### 强制链 / Enforcement Chain

1. `RrStateMachine::get_snapshot_builder`（`crates/rr-raft/src/sm_adapter.rs:92-101`）捕获
   `inner.snapshot()`、`last_applied`、`last_membership`；`RrSnapshotBuilder::build_snapshot`
   （`sm_adapter.rs:159-170`）固化为带 meta 的快照。openraft sm worker 串行调度 `apply` 与 builder，
   用户 Service 仍须遵守 A-0002 的一致快照契约。
   Snapshot construction captures state and metadata at one point; user Services must still honor A-0002.
2. `ServiceStateMachineAdapter::snapshot` 产出带 magic/version 的 `FramedSnapshot`，
   `framework_state` 携带框架状态（`crates/rr-raft/src/service_adapter.rs:375-401`）。
   The adapter frames user and framework state together.
3. `install_snapshot` 调 `inner.restore` 并替换 `last_applied`/`last_membership`
   （`sm_adapter.rs:107-122`）；幂等层 replace-not-merge，outbox 无快照态时清空。
   Installation replaces user state, metadata, and framework state rather than merging them.
4. openraft 分块传输 InstallSnapshot，follower 从快照 `last_log_id` 继续复制。
   Openraft streams the snapshot and resumes replication from its last log ID.

### 强制点 / Enforcement Points

| # | 位置 / Location | 强制内容 / Enforcement |
| --- | :--- | :--- |
| 1 | `sm_adapter.rs:92-101` | builder 同时捕获状态与 `last_applied`。 / The builder captures state and last-applied together. |
| 2 | `sm_adapter.rs:112-121` | install = restore + meta 替换，无合并。 / Install restores and replaces metadata without merging. |
| 3 | `service_adapter.rs:403-456` | 解码 framed 信封；legacy 无信封字节按用户快照透传并迁移。 / Framed snapshots are decoded; legacy unframed user snapshots pass through and migrate. |
| 4 | `crates/rr-raft/src/idempotency.rs:165-174` | restore 为 replace-not-merge，可重复安装。 / Restore is replace-not-merge and installation is idempotent. |
| 5 | `.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md` | openraft 快照原子性与恢复锚点的源码核对。 / Source-level review of openraft atomicity and recovery anchors. |

### 现有测试 / Tests

- `crates/rr-raft/src/sm_adapter.rs` `test_build_snapshot_captures_state`、
  `test_install_snapshot_restores_state`、`test_snapshot_round_trip`；
  `crates/rr-raft/src/service_adapter.rs` `snapshot_then_restore_roundtrip_through_adapter`、
  `restore_malformed_framed_bytes_errors`、`restore_records_snapshot_install_metric`。
  覆盖快照捕获、安装、往返、畸形输入和指标。
  These cover capture, installation, round trips, malformed input, and metrics.
- `crates/rr-raft/src/idempotency.rs` `restore_replaces_existing_cache_entries_not_merge`；
  `tests/integration/src/lib.rs` `test_snapshot_state_digest_consistent_across_nodes`、
  `test_snapshot_trigger_purge_keeps_cluster_operational`（2026-10-21 前名 `test_snapshot_catchup`）。
  覆盖替换语义、跨节点快照一致性与 trigger/purge 后集群可用性。
  These cover replacement semantics, cross-node snapshot identity, and cluster health after trigger/purge.
- `tests/crash/src/scenarios.rs` `scenario_c_snapshot_purge_kill`；
  `crates/rr-raft/src/snapshot_store.rs` `save_if_newer_refuses_stale_write_from_cache_alone`、
  `save_if_newer_unseeded_handle_falls_back_to_disk`、`atomic_write_enveloped_round_trip_and_tmp_cleanup`。
  覆盖崩溃恢复、陈旧快照拒绝、磁盘回退与原子写。
  These cover crash recovery, stale-snapshot rejection, disk fallback, and atomic writes.
- `crates/rr-network/src/raft_service.rs` `collect_snapshot_stream_rejects_missing_done_chunk`、
  `receive_with_timeout_aborts_stalled_stream`。
  覆盖缺 done 块拒绝与停滞流总超时。
  These cover missing-terminal-chunk rejection and total timeout for stalled streams.

### 缺口 / Gaps

当前无开放缺口；快照构建失败被吞的问题已于 2026-08-29 以 fallible/poisoned builder 和
`rr_snapshot_build_failures_total` 闭环，见文末。
No gap is open. Silent snapshot-build failure was resolved on 2026-08-29 with fallible/poisoned builders
and `rr_snapshot_build_failures_total`; see below.

## INV-6 Routing Transition Safety — 路由切换安全 / Routing Transition Safety

### 不变量 / Invariant

路由表更新生效后，旧 shard 不再收到该 partition 的新写；切换前已 ACK 的写由 INV-1 保证不丢；新写全部
路由到新 shard。ConfigProvider 是唯一事实源，不存在第二份调用方缓存视图。
After a routing-table update takes effect, the old shard receives no new writes for that partition. INV-1
preserves writes acknowledged before the switch, and new writes go to the new shard. The ConfigProvider is
the single source of truth; callers hold no second cached view.

### 强制链 / Enforcement Chain

1. `MemConfigProvider::add_partition` 和 etcd watch 更新并广播 `RoutingTableChange`
   （`crates/rr-config/src/mem.rs`）；mem 使用 copy-on-write `Arc` 快照。

   `MemConfigProvider::add_partition` and etcd watch update the table and publish `RoutingTableChange`;
   the in-memory provider swaps copy-on-write `Arc` snapshots.
2. `ShardRouter::route` 每请求调用 `routing_table_snapshot()`：mem 为读锁 + `Arc` clone；etcd 每次做
   keys-only revision 探针，未变才用缓存，变化立即全量重扫
   （`crates/rr-router/src/router.rs`, `crates/rr-config-etcd/src/lib.rs`）。

   Every request obtains a fresh provider snapshot; etcd validates its revision cache on every call.
3. `ClientDispatchImpl::submit_write` 先 route 再提交（`crates/rr-server/src/dispatch.rs:54-62`）；
   未登记 partition 直接 `NotFound`。
   Writes route before submission, and unknown partitions have no default destination.
4. `RrNode` watch loop 处理 PartitionAdded/GroupMembershipChanged，按副本集 start/stop group
   （`crates/rr-server/src/node.rs:271-321`, `:374-412`）。

   Group lifecycle converges on the table's replica sets.
5. 非 Leader 经 openraft ForwardToLeader → SDK 重定向，不在本地强制提交。
   Non-leaders redirect through openraft and the SDK rather than committing locally.

### 强制点 / Enforcement Points

| # | 位置 / Location | 强制内容 / Enforcement |
| --- | :--- | :--- |
| 1 | `crates/rr-router/src/router.rs` `ShardRouter::route` | 每请求取表快照，partition→group 唯一；未命中计 `router.route.not_found`。 / Every route uses a provider snapshot and has one explicit result. |
| 2 | `crates/rr-server/src/dispatch.rs` `submit_write` | 写前强制路由，组不存在即失败，无旁路。 / Writes must route successfully; missing groups fail without bypass. |
| 3 | `crates/rr-server/src/node.rs:374-412` | 成员变化驱动 start/stop，收敛本地组集合。 / Membership changes converge local groups on the table. |
| 4 | `crates/rr-server/src/dispatch.rs:115-122` | 非 Leader 仅重定向。 / Non-leaders only redirect. |
| 5 | `crates/rr-config-etcd/src/lib.rs:836-897`, inner `run_watch_loop` `:349` | watch 指数退避重连、last-seen revision 续接、compaction 对账、WatchStreamGuard abort-on-drop 与 `rr_config_etcd_*` 指标。 / Watches reconnect, resume by revision, reconcile compaction, abort tasks on guard drop, and emit `rr_config_etcd_*` metrics. |

### 现有测试 / Tests

- `tests/integration/src/lib.rs` `test_routing_table_update_routes_to_new_shard`、
  `test_routing_no_messages_to_old_shard_after_update`、`test_partition_migration`。
  覆盖更新后新路由、旧组日志不增长与 partition 迁移。
  Integration covers new routing, no old-group growth after updates, and partition migration.
- `crates/rr-router/src/router.rs` 8 个单测；
  `crates/rr-router/tests/router_proptest.rs` `proptest_router_transitions_route_consistency`；
  `formal/tla/RoutingTableSwitch.tla`（2026-09-10，TLC 零反例）。
  覆盖路由基础行为、随机变迁与独立模型一致、mem/etcd watch 语义下收敛。
  Unit, property, and TLA+ tests cover routing behavior, transition consistency, and convergence under both
  watch models.
- `crates/rr-config-etcd/src/lib.rs` `watch_backoff_doubles_and_caps`、
  `next_start_revision_resumes_after_last_seen`、`watch_stream_guard_aborts_tasks_on_drop`、
  `diff_tables_groups_changed_and_removed`。
  覆盖重连退避、revision 续接、guard 生命周期与差异计算。
  These cover reconnect backoff, revision resume, guard lifecycle, and table diffs.
- `crates/rr-config/src/mem.rs` `test_watch_lagged_consumer_recovers_after_dropped_events`、
  `test_remove_node_strips_dangling_replicas`；`crates/rr-server/src/node.rs`：
  `test_watch_loop_skips_group_not_in_replicas`、`test_watch_loop_skips_duplicate_start_via_typed_precheck`。
  覆盖滞后恢复、表完整性与重复/非副本事件处理。
  These cover lag recovery, table integrity, and duplicate/non-replica watch events.

### 缺口 / Gaps

:::warning 切换窗口没有栅栏或纪元 / No Fence or Epoch Across the Switch
已 route 到旧组但尚未 commit 的在途写仍可能在旧组 commit；INV-1 保证不丢，但不保证“切换后旧 shard
立刻不可写”。现有集成测试使用固定 300ms sleep 等表生效（`tests/integration/src/lib.rs:2488`, `:2538`），
该开放问题归 R-3/R-6。
An in-flight write already routed to the old group may still commit there. INV-1 prevents loss, but there is
no guarantee that the old shard becomes unwritable immediately after the switch. Existing integration tests
use fixed 300ms sleeps; this remains open under R-3/R-6.
:::

`GroupMeta.leader` 错误抽象已于 2026-08-29 删除；etcd watch 永久失明已于 2026-09-23 闭环，见文末。
The stale `GroupMeta.leader` abstraction was removed on 2026-08-29, and permanent etcd-watch blindness was
resolved on 2026-09-23; see below.

## INV-7 Membership Safety — 成员变更不丢 quorum / Membership Changes Preserve Quorum

### 不变量 / Invariant

任何 change-membership 操作不得把 Raft 组 voter 集缩减到丧失 quorum；部署基线要求变更后 voter 数 ≥2。
单节点集群拆除是显式运维动作，须走专门命令而非普通成员变更。
No membership change may reduce a Raft group's voter set enough to lose quorum. The deployment baseline
requires at least two voters after a change. Single-node teardown is an explicit operation through a
dedicated command, not ordinary membership change.

### 强制链 / Enforcement Chain

1. Admin RPC `AdminService.change_membership`（`crates/rr-server/src/admin.rs:535-600`）在 server 端
   执行共享 quorum 守卫；空集合即使 `force` 也拒绝。
   The Admin RPC enforces the shared server-side quorum guard; `force` never permits an empty voter set.
2. config watch 的 `NodeRemoved`（`crates/rr-server/src/node.rs:421-454`）复用同一谓词；目标 voter < 2
   时拒绝并 warn，自动路径没有 `force`。
   Automatic removal uses the same predicate, rejects fewer than two voters, and deliberately has no force.
3. `rr-admin` decommission 在客户端预检查 `remaining.len() < MIN_VOTERS_AFTER_REMOVAL`
   （=2，`crates/rr-admin/src/decommission.rs:57`, `:174-184`），用于 fail-fast UX；server 守卫才是
   所有入口的安全边界。
   The CLI retains a fail-fast precheck, while the server guard protects every entry point.

### 强制点 / Enforcement Points

| # | 位置 / Location | 强制内容 / Enforcement |
| --- | :--- | :--- |
| 1 | `crates/rr-admin/src/decommission.rs:57`, `:174-184` | 客户端预检查 voter < 2 即中止。 / Client-side precheck aborts below two voters. |
| 2 | `crates/rr-server/src/admin.rs:537-638`; `crates/rr-core/src/membership.rs:19` | group 存在性 → 非空 → quorum guard；未 force 的 unsafe 变更映射 `FAILED_PRECONDITION`，force 写 warn audit。 / Validation order is existence, nonempty, quorum; forced acceptance emits a warning audit. |
| 3 | `crates/rr-server/src/node.rs` NodeRemoved branch | 自动摘除共享谓词，voter < 2 拒绝并 warn。 / Automatic removal shares the guard and logs rejection. |

### 现有测试 / Tests

- `crates/rr-admin/src/decommission.rs` `report_quorum_loss_does_not_count_as_progress`、
  `min_voters_after_removal_constant_is_two`。
  覆盖 CLI 进度与最小 voter 常量。
  These cover CLI progress accounting and the minimum-voter constant.
- rr-core 谓词真值表、`crates/rr-server/src/admin.rs:1119-1260` handler 测试组、
  `tests/integration/src/membership_guard.rs:166`（3→1 拒绝 / 3→2 放行 / 2→1 force 放行）。
  覆盖共享谓词、force/audit 与端到端守卫。
  Predicate, handler, and integration tests cover the shared rule, force/audit behavior, and end-to-end
  enforcement.
- `tests/integration/src/lib.rs` `test_node_removed_quorum_guard`；
  `formal/tla/MembershipGuard.tla`（2026-09-10，TLC 11,545 态零反例），反例模型
  `formal/tla/bugs/MembershipGuardBuggy.tla`。
  覆盖 watch 路径守卫及空集、force、audit、并发交错性质。
  These cover the watch path and formal properties for empty sets, force, audit, and concurrent
  interleavings.

### 缺口 / Gaps

当前无开放缺口；server 端与 watch-loop 守卫分别于 2026-08-29、2026-09-21 闭环，见文末。
No gap is open; server-side and watch-loop guards closed on 2026-08-29 and 2026-09-21 respectively.

## INV-8 Security Gates — 生产监听必须加门控 / Production Listeners Require Gates

### 不变量 / Invariant

未显式设置 `allow_insecure` 时，节点拒绝以未加门控的监听端口启动：五个 gRPC gate 全部通过后 `RrNode`
才可构造；TCP transport 另受 G6/G7 约束。`allow_insecure=true` 是带运行时 WARN 和
`rr_security_insecure_mode` 指标的 dev-only 逃生门。
Without explicit `allow_insecure`, a node refuses to start with ungated listeners: all five gRPC gates must
pass before `RrNode` construction, and TCP transport additionally enforces G6/G7. `allow_insecure=true` is
a development-only escape with a runtime warning and the `rr_security_insecure_mode` metric.

auth token 只通过环境变量名间接引用，配置文件不落明文；变量未设置或为空时 fail-closed。
G7（TCP 必须 loopback）是硬门控，`allow_insecure` 也不能绕过。
Auth tokens are referenced by environment-variable name, never stored verbatim in config; missing or empty
variables fail closed. G7 requires TCP loopback and cannot be bypassed by `allow_insecure`.

### 强制链 / Enforcement Chain

1. TOML/CLI/env 解析为 `ResolvedConfig`；`[auth]` 只存环境变量名
   （`crates/rr-server/src/config_file.rs:82-99`），缺失/空值解析失败。
   Configuration stores only environment-variable names and rejects missing or empty secrets.
2. `RrNode::new` 首先调用 `validate_security_invariants`
   （`crates/rr-server/src/node.rs:36-108`，调用点 `:162`），聚合全部违规；`allow_insecure` 通过时
   WARN 并暴露指标（`:164-187`）。
   Construction validates all gates before binding listeners and reports all violations together.
3. G1 RaftService⇒TLS；G2 RaftService+TLS⇒`require_client_cert=true`（Raft 平面只接受 mTLS，无 bearer）；
   G3 bearer⇒TLS；G4 AdminService⇒TLS；G5 AdminService⇒`admin_token`（`node.rs:48-91`）。

   The five gRPC gates require Raft TLS/mTLS, TLS for bearer tokens, and Admin TLS plus a separate token.
4. G6 TCP 必须显式 `allow_insecure`；G7 TCP bind 必须 loopback，地址不可解析也 fail-closed。
   TCP requires explicit insecure mode and an unbypassable loopback bind.
5. gRPC 使用 TLS；ClientService 经 bearer→rate-limit 的 `ChainedInterceptor`；token 常数时间比较
   （`crates/rr-network/src/auth.rs:116`）；RaftService 结构性不限流（A-0005）。
   Runtime wiring applies TLS, ClientService authentication and rate limiting, constant-time token checks,
   and no Raft rate limiter.
6. `/health` `/ready` `/metrics` 由独立 hyper server 提供
   （`crates/rr-server/src/health.rs:126-138`, `:192-214`），配置 `metrics_port` 时启动。
   Observability endpoints run on a separate HTTP server when configured.

### 强制点 / Enforcement Points

| # | 位置 / Location | 强制内容 / Enforcement |
| --- | :--- | :--- |
| 1 | `crates/rr-server/src/node.rs:36-108` | 聚合校验，违规即 `Err`。 / Aggregated validation returns `Err` on violations. |
| 2 | `node.rs:162` | 门控先于端口绑定。 / Gates run before any listener binds. |
| 3 | `crates/rr-server/src/config_file.rs:946-980` | 测试锁定 auth 环境变量缺失/为空 fail-closed。 / Tests pin fail-closed secret resolution. |
| 4 | `crates/rr-network/src/auth.rs:116` | Bearer token 拦截器。 / Bearer-token interceptor. |
| 5 | `crates/rr-server/src/node.rs:452-462` | gRPC TLS 装配。 / gRPC TLS wiring. |
| 6 | `node.rs:262` → `crates/rr-server/src/config.rs:346-359` | `metrics_bind_host`/`admin_bind_host` 默认 loopback，非 IP 字面量拒绝。 / Observability and Admin bind hosts default to loopback and reject non-IP literals. |

### 现有测试 / Tests

- `crates/rr-server/src/node.rs:993-1111` 九个 gRPC gate 测试，包括严格路径、allow_insecure、最小 dev
  配置和违规聚合；`:1163-1282` 六个 TCP gate 测试；`config_file.rs:838-934` 覆盖 CLI/TOML/优先级。
  Tests cover all gRPC gates, strict/insecure/development paths, aggregation, TCP gates, and config
  precedence.
- `tests/integration/src/tls_auth_e2e.rs:226` `tls_mtls_bearer_auth_e2e_matrix`；
  `crates/rr-network/src/auth.rs` 单测覆盖缺/错 token 与脱敏错误。
  End-to-end and unit tests cover TLS/mTLS/bearer behavior and sanitized authentication failures.
- `crates/rr-server/src/config_file.rs:946-980` 覆盖 secret fail-closed；
  `crates/rr-server/src/node.rs` `bind_hosts_default_to_loopback`、
  `validate_metrics_bind_host_must_be_ip_literal`、`validate_admin_bind_host_must_be_ip_literal`。

  Tests cover fail-closed secrets and loopback/IP-literal bind validation.
- `crates/rr-network/src/rate_limit.rs`
  `per_caller_config_limits_unauthenticated_via_global_fallback`。
  未认证调用方强制走全局限流兜底桶。
  Unauthenticated callers are forced into the global fallback rate-limit bucket.

### 开放缺口 / Open Gaps

- `/metrics` `/ready` `/health` 仍是明文无 auth。2026-09-23 已默认 loopback 并支持显式
  `metrics_bind_host`/`admin_bind_host`，部署制品可显式 `0.0.0.0`；明文无 auth 半段仍开放。
  The observability endpoints remain plaintext and unauthenticated. The 2026-09-23 change added loopback
  defaults and explicit bind hosts, and deployment artifacts may choose `0.0.0.0`; plaintext/no-auth remains
  open.
- audit 配对仍不完整：`TriggerSnapshot` 无 started、`DrainNode` 无 finished、
  `reset_ingestor_offset` 无 started，ClientService mutation 无 audit。A-0012 的 ChangeMembership
  started/failed/force_accepted 不在此缺口内。
  Audit pairing remains incomplete for those operations and all ClientService mutations. The separate
  ChangeMembership started/failed/force_accepted events are not part of this gap.
原手写 `constant_time_eq` 与 32-bit fingerprint 缺口已闭环：`token_fingerprint` 现输出 16 个十六进制字符，
仅用于非加密的审计关联（`crates/rr-network/src/auth.rs:180-201`）；`constant_time_eq` 使用
`subtle::ConstantTimeEq`（`auth.rs:216-225`）。长度不匹配仍提前返回，源码明确接受这一长度 oracle。
The former hand-written `constant_time_eq` and 32-bit-fingerprint gap is resolved. `token_fingerprint` now
returns 16 hexadecimal characters for non-cryptographic audit correlation
(`crates/rr-network/src/auth.rs:180-201`), and `constant_time_eq` uses `subtle::ConstantTimeEq`
(`auth.rs:216-225`). Length mismatches still return early; the source explicitly accepts this length oracle.

TCP transport gate 与 dashboard 幽灵 `rr_auth_rejects_total` 已闭环，见文末。
The TCP transport gate and the dashboard's nonexistent `rr_auth_rejects_total` reference were resolved; see
below.

## INV-9 Homogeneous Formation Safety — 组建授权与旧集群收养 / Formation Authorization and Legacy Adoption

### 不变量 / Invariant

相同 SeedSpec 下，只有 `Genesis + Forming` 可按完整 seed roster 初始化；`Active` 记录下的 pristine 本地组
只能等待复制。`AdoptLegacy` 首次创建记录前要求本地所有托管组已初始化，直接写入 `LegacyAdopted + Active`，
不得 initialize 或改变 voter/learner。后续收敛只能走显式 Job/Admin learner + change-membership。
For the same SeedSpec, only `Genesis + Forming` may initialize the full seed roster. A pristine local group
under an `Active` record can only await replication. Before its first record, `AdoptLegacy` requires every
hosted local group to be initialized, writes `LegacyAdopted + Active` directly, and must not initialize or
change voters/learners. Later convergence uses only explicit Job/Admin learner plus change-membership flows.

SeedSpec 是初始路由/Genesis 授权，不是当前 membership；P1 不提供动态扩缩容、副本替换或 resharding。
SeedSpec authorizes initial routing/Genesis and is not current membership. P1 does not provide dynamic
scaling, replica replacement, or resharding.

lifecycle store 不可用时，已初始化节点可用本地 Raft 状态继续服务，但 readiness 必须 degraded；pristine
节点必须 fail closed，不得把 store outage 当成初始化授权。
When the lifecycle store is unavailable, initialized nodes may serve from local Raft state with degraded
readiness. Pristine nodes must fail closed and must not treat store outage as authorization to initialize.

### 强制链 / Enforcement Chain

1. TOML/CLI 要求完整 `id@advertised_addr` roster、本节点在 roster 内、非空 etcd endpoints，并拒绝 managed
   formation 与 bootstrap/peers 混用。
   Parsing requires a complete roster containing this node and nonempty etcd endpoints, and rejects mixed
   managed/legacy bootstrap configuration.
2. `FormationSupervisor::authorize_or_create` 区分 Genesis/AdoptLegacy，校验本地状态、seed/origin，并通过
   `LifecycleStore` CAS 创建唯一记录。
   Authorization validates local state and identity before creating one record through CAS.
3. `FormationSupervisor::reconcile_inner` 只允许 Genesis/Forming 初始化并核对实际 seed membership；
   Active/pristine 返回 `AwaitingReplication`；AdoptLegacy 只报告现状，不调 membership API。
   Reconcile initializes only Genesis/Forming, leaves Active/pristine awaiting replication, and never
   changes membership during legacy adoption.
4. managed `RrNode` 禁用旧 watch-loop 成员副作用与自动 promotion；readiness 要求 formed + observed
   leader，store outage 为 degraded/not-ready。
   Managed nodes disable legacy membership side effects and require formation plus an observed leader for
   readiness; store outages degrade readiness.

### 强制点 / Enforcement Points

| # | 位置 / Location | 强制内容 / Enforcement |
| --- | :--- | :--- |
| 1 | `crates/rr-server/src/config_file.rs:538-644` | 解析 `roster` 和 `formation`，拒绝冲突、缺本节点与空 endpoint。 / Parses `roster` and `formation`; rejects conflicts, missing self, and empty endpoints. |
| 2 | `crates/rr-server/src/formation.rs:245-307` | `Genesis` 和 `AdoptLegacy` 的本地状态门槛、`origin` 和 `state` 校验与 CAS。 / `Genesis` and `AdoptLegacy` local-state gates, `origin` and `state` validation, and CAS creation. |
| 3 | `formation.rs:96-243` | `Active` 和 `pristine` 不初始化；`Genesis` 核对 seed；`AdoptLegacy` 不改 membership。 / No `Active` or `pristine` initialization; `Genesis` checks seed; `AdoptLegacy` preserves membership. |
| 4 | `crates/rr-server/src/node.rs:906-935` | managed reconcile 与 readiness 循环。 / Managed reconciliation and readiness loop. |
| 5 | `crates/rr-server/src/formation.rs:390-419` | `awaiting`、`degraded` 和 `leader` readiness 判定。 / `awaiting`, `degraded`, and `leader` readiness decisions. |

### 现有测试 / Tests

- `tests/integration/src/homogeneous_formation.rs` `homogeneous_active_and_pristine_never_initializes`、
  `legacy_adoption_preserves_ack_and_expands_only_through_manual_membership`、
  `legacy_adoption_rejects_mixed_initialized_and_pristine_groups`、
  `legacy_adoption_rejects_pristine_only_local_state`。
  覆盖 Active/pristine 零初始化、ACK 保留、显式成员收敛、混合与全空收养拒绝。
  These cover no Active/pristine initialization, ACK preservation, explicit membership convergence, and
  rejection of mixed or wholly pristine adoption.
- 同文件 `homogeneous_store_outage_keeps_writes_and_partition_routes_unchanged` 覆盖 store outage 下 ACK 写
  与路由不变、readiness degraded。
  Store-outage coverage verifies unchanged acknowledged writes and routes with degraded readiness.
- `crates/rr-server/src/formation.rs` 单测覆盖 CAS conflict、seed/origin/identity、store outage、实际
  membership 不匹配和 managed watch 副作用禁用。
  Unit tests cover CAS conflicts, identity validation, store outage, membership mismatch, and disabled
  legacy watch side effects.

### 限制与验证边界 / Limits and Verification Boundary

real-etcd 进程级场景需要本地 etcd 容器镜像；镜像缺失时这些场景不执行，不能用 Mem/etcd 单测替代声明其
通过。P1 的动态成员、替换与 resharding 不在承诺范围内。
Real-etcd process scenarios require a local etcd container image. When it is absent, those scenarios do not
run and must not be reported as passing based on Mem/etcd unit tests. P1 dynamic membership, replacement,
and resharding remain outside this promise.

## 缺口与修复工作流 / Gap and Workflow History

已闭环条目只在此保留一份；开放条目的详细语义见对应 INV。
Resolved entries appear once here; see the corresponding INV for details of open gaps.

| 缺口 / Gap | 状态 / Status | INV | 工作流 / Workflow | 设计 / Design |
| :--- | :--- | :--- | :--- | :--- |
| rocks 持久化测试 CI 跳过 / Rocks persistence tests skipped in CI | 已闭环（2026-08-29）：启用 `rr-storage/rocks,integration/rocks` / Resolved (2026-08-29): enabled `rr-storage/rocks,integration/rocks` | INV-1 | WS-1 | 路线图 / Roadmap |
| `IoBatcher` 和 callback 未跟踪 spawn，`stop_group` 不 flush / Untracked `IoBatcher` callback spawns and no `stop_group` flush | 已闭环（2026-08-29）：显式 `close()`、drain 与 final flush / Resolved (2026-08-29): explicit `close()`, drain, and final flush | INV-1 | WS-4 | ADR A-0011 |
| Rust SDK 在 Leader 未知时假 ACK；Java 曾重犯 / SDK fake ACK with unknown leader; Java regressed similarly | Rust 已闭环（2026-09-04）；Java 已闭环（2026-10-21）：`success=false` 不再返回空成功 / Rust resolved (2026-09-04); Java resolved (2026-10-21): `success=false` no longer returns an empty success | INV-1 | RC 批次 / RC batch | — |
| 异步 append 违反 submitted⇒visible / Async append violated submitted⇒visible | 已闭环（2026-09-06）：pending overlay 与 barrier/hook / Resolved (2026-09-06): pending overlay and barrier/hook | INV-1 | R-4 | ADR A-0015 |
| 日志投毒导致崩溃循环 / Malformed house frames caused crash loops | 已闭环（2026-09-23）：ingress 校验与 apply gate / Resolved (2026-09-23): ingress validation and apply gate | INV-1 | 批次 A / Batch A | ADR A-0018 |
| TCP request ID 跨组碰撞 / Cross-group TCP request-ID collisions | 已闭环（2026-09-23）：池级 ID 与二元 demux / Resolved (2026-09-23): pool-level IDs and compound demux | INV-1 | 批次 A / Batch A | — |
| node-reset 锁检测恒 false / node-reset lock check always false | 已闭环（2026-09-23）：fcntl/flock 双探测 fail-closed / Resolved (2026-09-23): fail-closed fcntl/flock dual probes | INV-1 | 批次 A / Batch A | — |
| 幂等 namespace 上限在 apply 返回 Err / Namespace cap returned apply `Err` | 已闭环（2026-10-21）：类型化 outcome，不再 group-fatal / Resolved (2026-10-21): typed outcome, no longer group-fatal | INV-1, INV-3 | 2026-10-21 批次 / 2026-10-21 batch | ADR A-0018 |
| 缺线性一致读 3 节点测试 / Missing 3-node linearizable-read test | 已闭环（2026-09-04）：真实网络 follower redirect / Resolved (2026-09-04): real-network follower redirect | INV-2 | RC 并行项 / RC parallel item | A-0009 后续 / A-0009 follow-up |
| 非法 Kafka offset 静默回 0 / Invalid Kafka offsets silently mapped to zero | 已闭环（2026-08-29）：返回 `Result`，runner fail-closed / Resolved (2026-08-29): returns `Result`; runner fails closed | INV-3 | WS-4 | ADR A-0011 |
| Kafka poll 错误丢部分批 / Kafka poll errors dropped partial batches | 已闭环（2026-09-23）：先交付部分批 / Resolved (2026-09-23): delivers the partial batch first | INV-3 | 批次 A / Batch A | — |
| outbox 缺集群换主/重启 e2e / Missing outbox failover/restart e2e | 已闭环（2026-10-22）：Kafka、RaftWrite、rocks 三臂 / Resolved (2026-10-22): Kafka, RaftWrite, and rocks arms | INV-4 | R-4 / R-8 | `.sisyphus/plans/rr-inv4-outbox-failover-e2e.md` |
| RaftWrite 幂等键跨源组碰撞 / RaftWrite keys collided across source groups | 已闭环（2026-09-23）：`rr.output.g<N>` 命名空间 / Resolved (2026-09-23): `rr.output.g<N>` namespace | INV-4 | 批次 A / Batch A | — |
| 快照构建失败被吞 / Snapshot-build failures swallowed | 已闭环（2026-08-29）：fallible/poisoned builder 与 metric / Resolved (2026-08-29): fallible/poisoned builder and metric | INV-5 | WS-4 | ADR A-0011 |
| `GroupMeta.leader` 陈旧 / Stale routing-table leader | 已闭环（2026-08-29）：删除字段与消费链，走 SDK redirect / Resolved (2026-08-29): removed the field and consumer chain; uses SDK redirect | INV-6 | WS-7 | ADR A-0013 |
| 路由切换无 fence/epoch，测试固定 sleep / No routing fence/epoch; fixed sleeps | 开放：在途旧组写语义未定义 / Open: semantics of in-flight writes to the old group are undefined | INV-6 | R-3 / R-6 | 可靠性专项 / Reliability program |
| etcd watch 无重连恢复 / Etcd watch had no reconnect/recovery | 已闭环（2026-09-23）：revision 续接、guard 与 metrics / Resolved (2026-09-23): revision resume, guard, and metrics | INV-6 | 批次 A / Batch A | — |
| change-membership 无 server quorum guard / No server-side quorum guard | server 已闭环（2026-08-29）；watch 已闭环（2026-09-21） / Server resolved (2026-08-29); watch resolved (2026-09-21) | INV-7 | WS-6 | ADR A-0012 |
| TCP Raft transport 无安全 gate / TCP transport bypassed security gates | 已闭环（2026-08-29）：G6/G7，G7 不可绕过 / Resolved (2026-08-29): G6/G7; G7 cannot be bypassed | INV-8 | WS-5 | ADR A-0014 |
| `/metrics` `/ready` 明文无 auth / Plaintext unauthenticated observability | 部分闭环（2026-09-23）：默认 loopback；明文无 auth 仍开放 / Partially resolved (2026-09-23): loopback by default; plaintext without auth remains open | INV-8 | 0.2.1 backlog | — |
| audit 配对不完整，ClientService mutation 无 audit / Incomplete audit pairing; ClientService mutations have no audit | 开放 / Open | INV-8 | RC 并行项 / RC parallel item | — |
| 手写 `constant_time_eq`，32-bit fingerprint / Hand-written `constant_time_eq` and 32-bit fingerprint | 已闭环：当前实现使用 16 hex 非加密指纹与 `subtle::ConstantTimeEq`；保留长度 oracle / Resolved: current implementation uses a 16-hex non-cryptographic fingerprint and `subtle::ConstantTimeEq`; length oracle retained | INV-8 | RC 并行项 / RC parallel item | — |
| dashboard 引用不存在的 `rr_auth_rejects_total` / Dashboard referenced a nonexistent metric | 已闭环（2026-08-29）：删除幽灵 panel / Resolved (2026-08-29): removed the ghost panel | INV-8 | WS-1 | 路线图 / Roadmap |

工作流定义见 `.sisyphus/plans/rr-v0.2.0-release-roadmap.md`（WS-1~WS-8）与
`.sisyphus/plans/rr-reliability-program.md`（R-1~R-8）；`fresh-review` 指
`.sisyphus/evidence/rr-fresh-review-2026-09-23.md`，批次 A–E 为其修复波次。
Workflow definitions live in those WS-1~WS-8 and R-1~R-8 plans. `fresh-review` refers to the 2026-09-23
review, and batches A–E are its remediation waves.
