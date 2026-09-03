---
sidebar_position: 7
---

# 持久化与恢复 / Durability & Recovery

本页说明 rr 的持久化边界：哪些数据落在磁盘上、哪些只在内存里，节点重启 / 崩溃后如何恢复，以及当前可用的备份与灾难恢复工具。存储引擎的接口抽象见 [storage.md](./storage.md)；本文聚焦**持久化语义与恢复契约**。

---

This page defines rr's durability boundary: what lands on disk, what lives only in memory, how a node recovers from restart or crash, and which backup / disaster-recovery tools exist today. See [storage.md](./storage.md) for the storage-engine abstraction; this page focuses on **durability semantics and the recovery contract**.

## 持久化边界一览 / The durability boundary at a glance

| 数据 / Data | `mem` 模式 / mem mode | `rocks` 模式 / rocks mode |
| :--- | :--- | :--- |
| Raft 日志 / Raft log | 内存 `BTreeMap`，重启即丢失 / in-memory, lost on restart | 每组独立 RocksDB 实例（`{base_path}/{group_id}`），fsync 由 `DurabilityPolicy` 控制 |
| Raft vote / `last_purged` 元数据 | 同上，不持久化 | `metadata` CF，仅 `vote` 与 `last_purged` 两个键；vote 写直连 fsync（绕过 batcher） |
| 快照 / Snapshot | **仅内存**（`Vec<u8>`） | **同样仅内存**——见下文"快照"一节 |
| 用户业务状态 / User service state | 用户 `Service` 自行负责 | 用户 `Service` 自行负责（框架不替你落盘） |
| 框架状态（幂等/ingestor/outbox） | 随内存快照存活 | 随快照复制传播；重启后同样取决于日志与 Service 持久化 |

---

| Data | mem mode | rocks mode |
| :--- | :--- | :--- |
| Raft log | In-memory `BTreeMap`, lost on restart | Independent RocksDB instance per group (`{base_path}/{group_id}`); fsync governed by `DurabilityPolicy` |
| Raft vote / `last_purged` metadata | Not persisted | `metadata` CF, exactly two keys: `vote` and `last_purged`; vote writes bypass the batcher and fsync directly |
| Snapshot | **In-memory only** (`Vec<u8>`) | **Also in-memory only** — see "Snapshots" below |
| User service state | The user `Service`'s own responsibility | Same — the framework does not persist it for you |
| Framework state (idempotency / ingestor / outbox) | Lives with the in-memory snapshot | Propagates via snapshot replication; after restart it depends on the log and the Service's own persistence |

## 写入路径：IoBatcher 组提交 / Write path: IoBatcher group commit

RocksDB 模式下，日志写入经过**组提交**（group commit）管线（`crates/rr-raft/src/io_batcher.rs`）：

```text
RaftLogStorage::append(entries, IOFlushed)
    → 立即返回（pipeline），操作进入 IoBatcher channel
    → worker 聚批：首个条目后非阻塞排空；不足则等待 window（默认 500µs）
    → 达到 max_entries（默认 256）或窗口到期 → 单次 write_batch()
    → Strict 策略下 write_batch 带 set_sync(true) —— 一次 fsync 覆盖整批
    → 逐个点火每个 append 的 IOFlushed 回调（恰好一次）
    → openraft 推进 committed → apply → client response
```

---

In rocks mode, log appends flow through a **group-commit** pipeline (`crates/rr-raft/src/io_batcher.rs`):

```text
RaftLogStorage::append(entries, IOFlushed)
    → returns immediately (pipelined); op enters the IoBatcher channel
    → worker accumulates: non-blocking drain after the first item, else waits
      up to `window` (default 500µs)
    → flush on `max_entries` (default 256) or window expiry → one write_batch()
    → under Strict the write_batch carries set_sync(true) — one fsync per batch
    → each append's IOFlushed callback fires exactly once
    → openraft advances committed → apply → client response
```

关键事实：

- **ACK 边界（ADR A-0011 C1）**：`DurabilityPolicy::Strict` 下，写请求向 client 返回 `Ok` ⟹ 该 entry 所在 batch 已完成 fsync。fsync 永远先于 ACK。
- **vote 直连（C2）**：`save_vote` 有意绕过 batcher，走 `storage.put` 直接 fsync——vote 稀少且等不起 batch window。
- **优雅关停（C3）**：`IoBatcher::close()` 是唯一契约化的关停路径——排空 channel、最终一次 `write_batch`（final fsync）、每个 pending completion 恰好应答一次、worker 退出；默认 30s 截止，超时返回 `Err` 而非 panic。`RaftGroupManager::stop_group` 的顺序固定为：停 supervisor → drain 在途写 → `raft.shutdown()` → `log_storage.close()`。

---

Key facts:

- **ACK boundary (ADR A-0011 C1)**: under `DurabilityPolicy::Strict`, a write returning `Ok` to the client implies the batch containing that entry has completed fsync. fsync always precedes the ACK.
- **Vote writes are direct (C2)**: `save_vote` deliberately bypasses the batcher and fsyncs via `storage.put` — votes are rare and cannot wait out a batch window.
- **Graceful shutdown (C3)**: `IoBatcher::close()` is the only contractual shutdown path — drain the channel, one final `write_batch` (final fsync), answer every pending completion exactly once, worker exits; default 30s deadline, returning `Err` (not panic) on timeout. `RaftGroupManager::stop_group` ordering is fixed: stop supervisors → drain in-flight writes → `raft.shutdown()` → `log_storage.close()`.

### DurabilityPolicy

`crates/rr-storage/src/rocks.rs:46-86` 定义三档策略（v0.3.0 起改名，旧名保留为 deprecated 别名一个次版本）：

| 策略 / Policy | 行为 / Behavior | 适用 / Use |
| :--- | :--- | :--- |
| `Strict`（默认） | 每次写 WAL fsync（`set_sync(true)` + `set_use_fsync(true)`） | **生产唯一安全选项**——唯一满足 openraft 持久化契约的策略 |
| `ReplicatedOnly`（旧名 `Balanced`） | WAL 写但不 fsync；进程崩溃可恢复，**断电/内核 panic 丢最后几秒** | 仅当 RF≥3、节点跨供电域、有外部备份时 |
| `TestOnly`（旧名 `Fast`） | 无 fsync、无 WAL；崩溃即全丢 | 测试 / 一次性缓存负载，永不用于生产 |

CLI 选择：`--durability strict|replicated_only|test_only`（env `RR_DURABILITY`）。

---

`crates/rr-storage/src/rocks.rs:46-86` defines three policies (renamed in v0.3.0; old names remain as deprecated aliases for one minor version):

| Policy | Behavior | Use |
| :--- | :--- | :--- |
| `Strict` (default) | fsync the WAL on every write (`set_sync(true)` + `set_use_fsync(true)`) | **The only production-safe option** — the only policy upholding openraft's durability contract |
| `ReplicatedOnly` (formerly `Balanced`) | WAL written but not fsynced; survives a process crash, **loses the last ~seconds on power loss / kernel panic** | Only with RF≥3, independent power domains, and external backup |
| `TestOnly` (formerly `Fast`) | No fsync, no WAL; total data loss on crash | Tests / throwaway caches; never production |

CLI: `--durability strict|replicated_only|test_only` (env `RR_DURABILITY`).

## 存储布局 / Storage layout

- **`mem` 模式**：每个 Raft 组一个独立的内存 `RrLogStorage`（`crates/rr-raft/src/log_storage.rs:57`，内部 `BTreeMap<u64, Entry>`）。进程重启后日志、vote、元数据全部丢失。
- **`rocks` 模式**：每个 Raft 组一个**独立的 RocksDB 实例**，路径 `{base_path}/{group_id}`（`crates/rr-raft/src/group_manager.rs:1071`）。组与组之间不共享 DB 句柄——单组的 compaction / 故障不影响其他组。
- **列族**：`raft_log`（日志条目，大端序 index 作 key 保证范围扫描有序）、`metadata`（**只有两个键**：`vote` 与 `last_purged`，`crates/rr-raft/src/rocks_log_storage.rs:41-44`；快照元数据不落盘）、`state_machine`（经 `StorageEngine` 的用户数据）。

---

- **mem mode**: each Raft group gets its own in-memory `RrLogStorage` (`crates/rr-raft/src/log_storage.rs:57`, a `BTreeMap<u64, Entry>` inside). Log, vote, and metadata are all lost on process restart.
- **rocks mode**: each Raft group gets an **independent RocksDB instance** at `{base_path}/{group_id}` (`crates/rr-raft/src/group_manager.rs:1071`). Groups share no DB handle — one group's compaction or failure does not affect the others.
- **Column families**: `raft_log` (entries keyed by big-endian index so range scans stay ordered), `metadata` (**exactly two keys**: `vote` and `last_purged`, `crates/rr-raft/src/rocks_log_storage.rs:41-44`; snapshot metadata is not persisted), `state_machine` (user data via `StorageEngine`).

## 快照 / Snapshots

快照由 openraft 的 `SnapshotPolicy::LogsSinceLast(N)` 在 apply 路径内自动触发（`crates/rr-raft/src/raft_node.rs:74`）——每追加 N 条日志触发一次。**默认 N = 50_000**（`rr-raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST`），可用 `NodeConfig::snapshot_logs_since_last` / `--snapshot-logs-since-last` / `RR_SNAPSHOT_LOGS_SINCE_LAST` 覆盖（ADR A-0008 的取舍：N 越小恢复时重放越短，但快照开销越频繁）。

**快照是纯内存的。** `RrStateMachine` 把快照字节缓存在 `snapshot_bytes: Vec<u8>`（`crates/rr-raft/src/sm_adapter.rs:30`）；`get_current_snapshot()` **每次调用都重新调用用户 `Service::snapshot()` 捕获**（`sm_adapter.rs:176-198`），不存在"从磁盘读回快照"的路径。快照的唯一传输方向是 leader → follower 的 `InstallSnapshot` 流式 RPC。

快照构建是 fail-closed 的（ADR A-0011 C6）：要么拿到 `Service::snapshot()` 成功产生的完整字节，要么不存在——绝不会有"空字节 + 合法 meta"的快照。构建失败计入 `rr_snapshot_build_failures_total` 指标。

---

Snapshots are triggered inside the apply path by openraft's `SnapshotPolicy::LogsSinceLast(N)` (`crates/rr-raft/src/raft_node.rs:74`) — one snapshot every N appended entries. **N defaults to 50_000** (`rr-raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST`) and can be overridden via `NodeConfig::snapshot_logs_since_last` / `--snapshot-logs-since-last` / `RR_SNAPSHOT_LOGS_SINCE_LAST` (the ADR A-0008 trade-off: smaller N shortens recovery replay but pays snapshot cost more often).

**Snapshots are memory-only.** `RrStateMachine` caches snapshot bytes in `snapshot_bytes: Vec<u8>` (`crates/rr-raft/src/sm_adapter.rs:30`); `get_current_snapshot()` **re-captures from the user `Service::snapshot()` on every call** (`sm_adapter.rs:176-198`) — there is no "read snapshot back from disk" path. The only direction a snapshot travels is leader → follower via the streaming `InstallSnapshot` RPC.

Snapshot building is fail-closed (ADR A-0011 C6): a snapshot is either complete bytes from a successful `Service::snapshot()` or it does not exist — never "empty bytes + valid meta". Build failures increment the `rr_snapshot_build_failures_total` counter.

### 恢复契约 / The recovery contract

:::warning 每个 Service 实现者都必须理解 / Every Service implementor must understand this
快照**无法在进程重启后存活**。重启恢复 = openraft 从持久化日志（rocks 模式）重放 + vote 恢复；快照只在节点**在线**时用于 leader → follower 同步。因此：

- rocks 模式 + 日志未被清理：重启后状态机由**日志重放**重建，无需任何快照。
- 日志已清理（purge）且本地状态落后：唯一出路是 `rr-admin node-reset` 清空后作为 learner 重新加入，从集群快照传输追赶（ADR A-0002 恢复路径 case 4）。
- **如果你需要超出 Raft 日志本身的持久性，你的 `Service` 必须自行持久化业务数据**（自己的数据库、自己的文件）。框架的 `snapshot()`/`restore()` 契约是"从字节重建内存视图"，不是"替你落盘"。
:::

Snapshots **do not survive a process restart**. Restart recovery = openraft replays the persisted log (rocks mode) + restores the vote; snapshots exist only for leader → follower synchronization while nodes are **online**. Therefore:

- rocks mode with the log intact: the state machine is rebuilt by **log replay** after restart, no snapshot involved.
- Log purged and local state behind: the only way out is `rr-admin node-reset` (wipe), rejoin as a learner, and catch up via snapshot transfer from the cluster (ADR A-0002 recovery flow, case 4).
- **If you need durability beyond the Raft log itself, your `Service` must persist its own business data** (its own database, its own files). The framework's `snapshot()`/`restore()` contract is "rebuild an in-memory view from bytes", not "persist on your behalf".

### FramedSnapshot 信封 / The FramedSnapshot envelope

`ServiceStateMachineAdapter` 不再把用户的原始 `Service::snapshot()` 字节直接写进 Raft 快照，而是包一层信封（`crates/rr-core/src/framed_snapshot.rs`）：

```text
[ magic: 8 字节 "\0rrSNAP1" ] [ bincode(FramedSnapshot) ]

FramedSnapshot {
    version: u8,                    // 当前 1；解码器拒绝更高版本
    framework_state: FrameworkState {
        idempotency: Option<IdempotencyState>,   // 幂等层 watermark + LRU cache（bincode blob）
        ingestor:    Option<IngestorState>,      // last_applied_offset（opaque bytes）
        outbox:      Option<OutboxState>,        // pending outputs + sent_watermark + next_seq
    },
    user_snapshot: Vec<u8>,         // 用户 Service::snapshot() 原始字节，框架不解释
}
```

效果：幂等去重状态、Kafka 消费位点、outbox 未发输出都**随快照一起复制**——leader 切换后新 leader 从快照恢复这些状态，不会重复 apply、不会从错误 offset 重拉、不会丢 output。`restore()` 先尝试 framed 解码，magic 不匹配则按旧版裸字节快照处理（透明迁移）；下一次 `snapshot()` 产生的就是新信封格式。

---

`ServiceStateMachineAdapter` no longer writes the user's raw `Service::snapshot()` bytes into the Raft snapshot directly; it wraps them in an envelope (`crates/rr-core/src/framed_snapshot.rs`):

```text
[ magic: 8 bytes "\0rrSNAP1" ] [ bincode(FramedSnapshot) ]

FramedSnapshot {
    version: u8,                    // currently 1; decoders reject higher versions
    framework_state: FrameworkState {
        idempotency: Option<IdempotencyState>,   // idempotency watermarks + LRU cache (bincode blob)
        ingestor:    Option<IngestorState>,      // last_applied_offset (opaque bytes)
        outbox:      Option<OutboxState>,        // pending outputs + sent_watermark + next_seq
    },
    user_snapshot: Vec<u8>,         // raw user bytes from Service::snapshot(); not interpreted
}
```

Net effect: dedup state, Kafka consumption offsets, and undelivered outbox outputs **replicate with the snapshot** — after a leader change the new leader restores them, avoiding duplicate applies, wrong-offset re-fetches, and lost outputs. `restore()` tries the framed decode first and falls back to treating the input as legacy raw bytes (transparent migration); the next `snapshot()` writes the new envelope.

## 备份与灾难恢复 / Backup & disaster recovery

当前**可用**的工具链：

| 工具 / Tool | 在线？ / Online? | 说明 / Notes |
| :--- | :--- | :--- |
| `OnlineBackup` Admin RPC | ✅ 节点运行中 | 可选 `force_fresh_snapshot` 先触发新快照，随后把快照字节按 **64 KiB** chunk 流式吐出（`crates/rr-server/src/admin.rs:39` `ONLINE_BACKUP_CHUNK_BYTES`），等待快照最长 60s。粒度 = 单 group 的快照（即 FramedSnapshot 信封） |
| `rr-admin backup` / `restore` / `inspect-backup` | ❌ 节点必须停止 | 离线备份：`data_dir` 打成 `.tar.gz`（`metadata.json` + `data/...`），带 `format_version`、rr 版本、时间戳与**全量文件 SHA-256**（`crates/rr-admin/src/backup.rs`）。`restore` 要求目标目录为空；`inspect-backup` 不解包即可看元数据 |
| `DrainNode` RPC + `rr-admin node drain` | ✅ | 编排式关停前置：把本节点所有 group 的 leader 转走，客户端流量平滑切走 |
| `rr-admin node decommission` | ✅ | 多 group 编排：逐组 drain → 预检 quorum（剩 < 2 voter 则中止该组）→ `change_membership` 摘除 → 复查确认 |
| `rr-admin node-reset` | ❌ 节点必须停止 | **整个集群失去 quorum** 或数据目录损坏时的恢复：清空 `data_dir` 后以 learner 重新加入。有 pre-flight 检查（RocksDB LOCK、目录标记）+ `--force` 确认 |
| `rr-admin group reset-ingestor-offset` | ✅ | ingestor 灾难恢复：人工覆盖持久化的消费位点；`--offset -1` 表示清除。warn 级审计 |

缩容侧的意外 quorum 丢失由成员变更守卫（`MIN_VOTERS_AFTER_REMOVAL = 2`）兜底——见 [../deployment/security.md](../deployment/security.md) 的"成员变更安全护栏"一节。

---

The toolchain that **exists and works** today:

| Tool | Online? | Notes |
| :--- | :--- | :--- |
| `OnlineBackup` Admin RPC | ✅ node running | Optionally triggers a fresh snapshot first (`force_fresh_snapshot`), then streams snapshot bytes in **64 KiB** chunks (`crates/rr-server/src/admin.rs:39` `ONLINE_BACKUP_CHUNK_BYTES`), waiting up to 60s for materialisation. Granularity = one group's snapshot (the FramedSnapshot envelope) |
| `rr-admin backup` / `restore` / `inspect-backup` | ❌ node must be stopped | Offline backup: `data_dir` packed into `.tar.gz` (`metadata.json` + `data/...`) carrying `format_version`, rr version, timestamp, and a **SHA-256 over all files** (`crates/rr-admin/src/backup.rs`). `restore` requires an empty target dir; `inspect-backup` reads metadata without unpacking |
| `DrainNode` RPC + `rr-admin node drain` | ✅ | Pre-shutdown orchestration: transfers this node's leadership away on every group so client traffic moves smoothly |
| `rr-admin node decommission` | ✅ | Multi-group orchestration: per-group drain → quorum precheck (aborts a group if fewer than 2 voters would remain) → `change_membership` removal → post-check |
| `rr-admin node-reset` | ❌ node must be stopped | Recovery from **total quorum loss** or a corrupt data dir: wipe `data_dir`, rejoin as learner. Pre-flight checks (RocksDB LOCK, dir markers) + `--force` confirmation |
| `rr-admin group reset-ingestor-offset` | ✅ | Ingestor disaster recovery: manually override the persisted consumption offset; `--offset -1` clears it. Warn-level audit |

Accidental quorum loss on the shrink path is backstopped by the membership guard (`MIN_VOTERS_AFTER_REMOVAL = 2`) — see [../deployment/security.md](../deployment/security.md).

## 现状说明与务实建议 / Honest status & what to do today

:::warning rocks 模式当前的接线状态 / Current wiring status of rocks mode
**官方 rr-server 二进制目前不会把 Raft 日志接入 RocksDB**：无论 `--storage` 选什么，组管理器始终走 `RaftGroupManager::new_with_network`（内存日志）；`--storage rocks` 打开的 `RocksStorage` 只落入 `NodeConfig.storage_engine`，该字段当前**没有消费者**（`crates/rr-server/src/main.rs:125-169, 315-350`）。RocksDB 日志持久化目前只在集成测试（`new_with_rocks` / `start_group_rocks`）中生效，或由库使用方自行接线。这一限制与 [storage.md](./storage.md) 中的说明一致。
:::

**The stock rr-server binary does not wire Raft logs to RocksDB today**: regardless of `--storage`, the group manager always goes through `RaftGroupManager::new_with_network` (in-memory log); the `RocksStorage` opened by `--storage rocks` only lands in `NodeConfig.storage_engine`, which currently has **no consumer** (`crates/rr-server/src/main.rs:125-169, 315-350`). RocksDB log persistence currently takes effect only in integration tests (`new_with_rocks` / `start_group_rocks`), or when library users wire it themselves. This mirrors the note in [storage.md](./storage.md).

在此基础上，生产用户的务实建议：

1. **把"重启 = 状态丢失"当作默认假设**，除非你在 `Service` 里自行持久化业务数据并能从它重建。这是当前二进制下最重要的一条。
2. **库用户**：现在就通过 `RaftGroupManager::new_with_rocks` + `start_group_rocks` 接线 RocksDB 日志（集成测试已覆盖该路径），并始终使用 `DurabilityPolicy::Strict`。
3. **合理设置 `--snapshot-logs-since-last`**：默认 50_000 在 100k ops/s 下约每 500ms 一次快照；调小可缩短崩溃后的日志重放，代价是更频繁的快照序列化。
4. **关机前先 drain**：`rr-admin node drain`（或停机流程中的 `DrainNode`）把 leader 转走；退役节点用 `rr-admin node decommission`，不要直接 `change_membership` 缩到 1 voter。
5. **定期备份**：在线用 `OnlineBackup`（每 group 快照）；需要整目录归档时按滚动流程（转 leader → SIGTERM → `rr-admin backup` → 重启）做离线备份，并校验 SHA-256。
6. **监控 `rr_snapshot_build_failures_total`**：非零意味着快照构建在失败，leader 切换后的状态恢复能力在退化。
7. **Kafka 输入场景**：灾难后消费位点异常时用 `rr-admin group reset-ingestor-offset` 显式修正，而不是让 offset 猜。

---

On top of that, practical guidance for production users:

1. **Treat "restart = state loss" as the default assumption** unless your `Service` persists its own business data and can rebuild from it. This is the single most important point under the current binary.
2. **Library users**: wire RocksDB log storage now via `RaftGroupManager::new_with_rocks` + `start_group_rocks` (the path is covered by integration tests), and always use `DurabilityPolicy::Strict`.
3. **Tune `--snapshot-logs-since-last`**: the default 50_000 snapshots roughly every 500ms at 100k ops/s; lowering it shortens post-crash log replay at the cost of more frequent snapshot serialization.
4. **Drain before shutdown**: `rr-admin node drain` (or `DrainNode` in your stop procedure) moves leadership away; retire nodes with `rr-admin node decommission` instead of shrinking straight to 1 voter via `change_membership`.
5. **Back up regularly**: `OnlineBackup` for live per-group snapshots; for whole-dir archives follow the rolling procedure (transfer leader → SIGTERM → `rr-admin backup` → restart) and verify the SHA-256.
6. **Monitor `rr_snapshot_build_failures_total`**: non-zero means snapshot builds are failing and post-leader-change state recovery is degrading.
7. **Kafka input**: after a disaster, fix a bad consumption offset explicitly with `rr-admin group reset-ingestor-offset` rather than letting offsets be guessed.

## 相关 / Related

- 存储引擎抽象与列族 / Storage engine abstraction & column families: [storage.md](./storage.md)
- 快照传输流程 / Snapshot transfer flow: [c4/dynamic-snapshot-transfer.md](./c4/dynamic-snapshot-transfer.md)
- 成员变更守卫 / Membership guard: [../deployment/security.md](../deployment/security.md)
- ADR A-0002 / A-0008 / A-0011 / A-0012 索引 / ADR index: [design-decisions.md](./design-decisions.md)
- 运维手册 / Runbooks: [../operations/runbooks/](../operations/runbooks/)
