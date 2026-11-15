---
sidebar_position: 7
---

# 持久化与恢复 / Durability & Recovery

本文定义磁盘与内存的边界、重启或崩溃后的恢复语义，以及现有备份工具。存储接口见 [storage.md](./storage.md)。

This page defines what is durable, what is memory-only, how restart and crash recovery work, and which backup tools exist. For storage interfaces, see [storage.md](./storage.md).

## 持久化边界 / Durability Boundary

| 数据 / Data | `mem` 模式<br />`mem` mode | `rocks` 模式<br />`rocks` mode |
| :--- | :--- | :--- |
| Raft 日志 / Raft log | 内存 `BTreeMap`，重启即丢失。<br />In-memory `BTreeMap`, lost on restart. | 每组独立 RocksDB：`{base_path}/{group_id}`；fsync 由 `DurabilityPolicy` 控制。<br />One RocksDB per group at `{base_path}/{group_id}`; fsync is governed by `DurabilityPolicy`. |
| vote / `last_purged` | 不持久化。<br />Not persisted. | `metadata` CF 中仅有这两个键；vote 绕过 batcher，直接 fsync。<br />Only these two keys in the `metadata` CF; votes bypass the batcher and fsync directly. |
| 快照 / Snapshot | 仅内存 `Vec<u8>`。<br />In-memory `Vec<u8>` only. | `SnapshotStore` 原子写入 `{base_path}/{group_id}/sm-snapshot.bin`，失败即中止。<br />`SnapshotStore` atomically writes `{base_path}/{group_id}/sm-snapshot.bin` and aborts on failure. |
| 用户业务状态 / User service state | 由用户 `Service` 负责。<br />Owned by the user `Service`. | 框架持久化快照字节，不负责业务级备份。<br />The framework persists snapshot bytes, not business-level backups. |
| 框架状态 / Framework state | 幂等、ingestor、outbox 随内存快照存活。<br />Idempotency, ingestor, and outbox state live in the in-memory snapshot. | 随持久化快照保存，并在重启后结合日志尾部恢复。<br />Persisted with snapshots and recovered with the log tail after restart. |

## 写入路径：IoBatcher 组提交 / Write Path: IoBatcher Group Commit

rocks 模式的日志追加经过 `crates/rr-raft/src/io_batcher.rs` 的组提交管线。

In rocks mode, log appends use the group-commit pipeline in `crates/rr-raft/src/io_batcher.rs`.

```text
RaftLogStorage::append(entries, IOFlushed)
    → return immediately; enqueue to IoBatcher
    → non-blocking drain after the first item
    → if only one item and window != 0, wait up to window;
      a second submission flushes early (default window = 0)
    → flush at max_entries (default 256) or window expiry
    → one write_batch(); Strict sets set_sync(true)
    → fire every append's IOFlushed callback exactly once
    → openraft commits → apply → client response
```

- **ACK 边界（ADR A-0011 C1）/ ACK boundary**：`Strict` 下，客户端收到 `Ok` 前，包含该 entry 的 batch 已完成 fsync。<br />Under `Strict`, the batch containing an entry has completed fsync before the client receives `Ok`.
- **vote 直连（C2）/ Direct vote writes**：`save_vote` 绕过 batcher，经 `storage.put` 直接 fsync，避免等待 batch window。<br />`save_vote` bypasses the batcher and fsyncs through `storage.put`, avoiding the batch window.
- **优雅关停（C3）/ Graceful shutdown**：`IoBatcher::close()` 排空 channel，执行最终 `write_batch`，每个 pending completion 恰好应答一次后退出。默认截止 30 秒，超时返回 `Err` 而非 panic。`RaftGroupManager::stop_group` 的顺序固定为停 supervisor → drain 在途写 → `raft.shutdown()` → `log_storage.close()`。<br />`IoBatcher::close()` drains the channel, performs a final `write_batch`, answers every pending completion exactly once, and exits. The default deadline is 30 seconds; timeout returns `Err`, not a panic. `RaftGroupManager::stop_group` stops supervisors → drains in-flight writes → calls `raft.shutdown()` → calls `log_storage.close()`.

### DurabilityPolicy

`crates/rr-storage/src/rocks.rs:46-86` 定义三档策略。v0.2.0 改名，deprecated 别名已在 v0.3.0 移除，旧字符串会被拒绝。

`crates/rr-storage/src/rocks.rs:46-86` defines three policies. They were renamed in v0.2.0; deprecated aliases were removed in v0.3.0, and old strings are rejected.

| 策略 / Policy | 行为 / Behavior | 适用范围 / Intended Use |
| :--- | :--- | :--- |
| `Strict`（默认 / default） | 每次写 WAL fsync：`set_sync(true)` + `set_use_fsync(true)`。<br />Every write fsyncs the WAL with `set_sync(true)` + `set_use_fsync(true)`. | 生产环境唯一满足 openraft 持久化契约的选项。<br />The only production option that satisfies openraft's durability contract. |
| `ReplicatedOnly`（旧名 `Balanced`）<br />`ReplicatedOnly` (formerly `Balanced`) | 写 WAL 但不 fsync；进程崩溃可恢复，断电或内核 panic 可丢失最后几秒。<br />Writes WAL without fsync; survives process crash, but power loss or kernel panic can lose the last seconds. | 仅限 RF≥3、跨供电域且有外部备份的场景。<br />Only with RF≥3, independent power domains, and external backup. |
| `TestOnly`（旧名 `Fast`）<br />`TestOnly` (formerly `Fast`) | 无 fsync、无 WAL；崩溃可丢失全部数据。<br />No fsync or WAL; a crash can lose all data. | 测试或一次性缓存，禁止用于生产。<br />Tests or disposable caches only; never production. |

CLI：`--durability strict|replicated_only|test_only`；环境变量：`RR_DURABILITY`。

CLI: `--durability strict|replicated_only|test_only`; environment variable: `RR_DURABILITY`.

**作用域**：`--durability` 只控制状态机侧 `rr-server --storage rocks` 引擎。每组 `RocksLogStorage` 始终以 `Strict` 打开，不能降级日志侧持久性（`crates/rr-raft/src/rocks_log_storage.rs`）。

**Scope**: `--durability` controls only the state-machine-side engine selected by `rr-server --storage rocks`. Every group's `RocksLogStorage` always opens as `Strict`; log durability cannot be downgraded (`crates/rr-raft/src/rocks_log_storage.rs`).

## 存储布局 / Storage Layout

- **`mem`**：每组使用独立的内存 `RrLogStorage`（`crates/rr-raft/src/log_storage.rs:57`，内部 `BTreeMap<u64, Entry>`）；日志、vote 和元数据在重启后丢失。<br />**`mem`**: Each group uses an independent in-memory `RrLogStorage` (`crates/rr-raft/src/log_storage.rs:57`, backed by `BTreeMap<u64, Entry>`); logs, votes, and metadata are lost on restart.
- **`rocks`**：每组在 `{base_path}/{group_id}` 打开独立 RocksDB（`crates/rr-raft/src/group_manager.rs:1071`）；组间不共享 DB 句柄，单组 compaction 或故障不直接波及其他组。<br />**`rocks`**: Each group opens its own RocksDB at `{base_path}/{group_id}` (`crates/rr-raft/src/group_manager.rs:1071`). Groups share no DB handle, so one group's compaction or failure does not directly affect another.
- **列族 / Column families**：`raft_log` 使用大端序 index 保持范围扫描有序；`metadata` 保存 `vote` 与 `last_purged`（`crates/rr-raft/src/rocks_log_storage.rs:41-44`）；`state_machine` 经 `StorageEngine` 保存用户数据。SM 快照由 `SnapshotStore` 写入 `sm-snapshot.bin`。<br />`raft_log` keys entries by big-endian index for ordered scans; `metadata` stores `vote` and `last_purged` (`crates/rr-raft/src/rocks_log_storage.rs:41-44`); `state_machine` stores user data through `StorageEngine`. SM snapshots are written by `SnapshotStore` to `sm-snapshot.bin`.

## 快照 / Snapshots

openraft 在 apply 路径按 `SnapshotPolicy::LogsSinceLast(N)` 触发快照（`crates/rr-raft/src/raft_node.rs:74`）。默认 `N = 50_000`（`rr-raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST`），可通过 `NodeConfig::snapshot_logs_since_last`、`--snapshot-logs-since-last` 或 `RR_SNAPSHOT_LOGS_SINCE_LAST` 覆盖。较小的 N 缩短恢复重放，但增加快照开销（ADR A-0008）。

Openraft triggers snapshots in the apply path with `SnapshotPolicy::LogsSinceLast(N)` (`crates/rr-raft/src/raft_node.rs:74`). The default is `N = 50_000` (`rr-raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST`), configurable through `NodeConfig::snapshot_logs_since_last`, `--snapshot-logs-since-last`, or `RR_SNAPSHOT_LOGS_SINCE_LAST`. Smaller N shortens recovery replay but increases snapshot cost (ADR A-0008).

**rocks 持久化**：每个构建或安装的快照经 `rr_raft::snapshot_store::SnapshotStore` 按 tmp 写入 → 文件 fsync → rename → 目录 fsync 的顺序原子保存到 `sm-snapshot.bin`，由 `RaftNode::new_with_rocks_factory` 自动接线。启动恢复窗口内（`last_applied < snapshot.last_log_id`），`get_current_snapshot()` 返回持久化快照，openraft 的 `StorageHelper::restore_from_snapshot` 先恢复它，再重放未清理的日志尾部；SM 追平后恢复为实时捕获，follower 追赶和 ingestor offset 播种行为不变。

**Rocks persistence**: every built or installed snapshot is atomically stored by `rr_raft::snapshot_store::SnapshotStore` as `sm-snapshot.bin` using tmp write → file fsync → rename → directory fsync, wired by `RaftNode::new_with_rocks_factory`. During startup recovery (`last_applied < snapshot.last_log_id`), `get_current_snapshot()` returns the persisted snapshot; openraft's `StorageHelper::restore_from_snapshot` reinstalls it before replaying the un-purged log tail. Once caught up, `get_current_snapshot()` captures live state again, leaving follower catch-up and ingestor-offset seeding unchanged.

持久化和构建均为 fail-closed：写盘失败会中止构建或安装，openraft 不会 purge 缺少持久快照的日志前缀；损坏文件会使节点启动失败，而非从空状态静默重启。快照要么包含 `Service::snapshot()` 成功返回的完整字节，要么不存在；失败计入 `rr_snapshot_build_failures_total`（ADR A-0011 C6）。mem 模式仍只保存内存快照。

Persistence and construction are fail-closed: a write failure aborts build or install, and openraft does not purge a log prefix without a durable snapshot. A corrupt file fails startup instead of silently starting empty. A snapshot either contains complete bytes from a successful `Service::snapshot()` or does not exist; failures increment `rr_snapshot_build_failures_total` (ADR A-0011 C6). Mem mode remains memory-only.

**版本说明**：持久化快照文件使用独立的 magic + version 信封；无信封的 0.2.x 文件会被拒绝，不提供 legacy fallback。

**Version caveat**: persisted snapshot files have their own magic + version envelope; unenveloped 0.2.x files are rejected without a legacy fallback.

### 恢复契约 / Recovery Contract

:::warning Service 实现者必须遵守 / Required for Service Implementors
rocks 模式重启时，openraft 从 `sm-snapshot.bin` 恢复到快照点，再重放日志尾部；`sm-membership.bin` 保存已提交成员关系。`kill -9` 重启幸存有验收单测覆盖。mem 模式重启即丢失全部状态。

In rocks mode, openraft restores from `sm-snapshot.bin` and replays the log tail; `sm-membership.bin` preserves committed membership. Survival across `kill -9` restart is covered by an acceptance unit test. Mem mode loses all state on restart.

- rocks：状态由持久化快照和快照点之后的日志重建。<br />Rocks: state is rebuilt from the persisted snapshot plus subsequent log entries.
- rocks 且快照与日志均不可用：使用 `rr-admin node-reset` 清空数据目录，以 learner 重新加入并通过集群快照追赶（ADR A-0002 case 4）。<br />Rocks with unusable snapshot and log: wipe with `rr-admin node-reset`, rejoin as a learner, and catch up through cluster snapshot transfer (ADR A-0002 case 4).
- mem：只能通过重新加入集群从 leader 同步。<br />Mem: rejoin the cluster and synchronize from the leader.
- 若需要独立审计或跨集群副本等业务级持久性，`Service` 仍需自行实现。框架负责从快照字节重建内存视图，并在 rocks 模式保存这些字节，但不提供业务级备份。<br />If you need business-level durability such as independent audit or cross-cluster copies, the `Service` must still implement it. The framework rebuilds in-memory views from snapshot bytes and persists those bytes in rocks mode, but does not provide business-level backup.
:::

### FramedSnapshot 信封 / FramedSnapshot Envelope

`ServiceStateMachineAdapter` 将用户快照包入 `FramedSnapshot`，而不是直接写入原始字节（`crates/rr-core/src/framed_snapshot.rs`）。

`ServiceStateMachineAdapter` wraps user snapshots in `FramedSnapshot` instead of writing raw bytes directly (`crates/rr-core/src/framed_snapshot.rs`).

```text
[ magic: 8 bytes "\0rrSNAP1" ] [ bincode(FramedSnapshot) ]

FramedSnapshot {
    version: u8,                    // currently 1; higher versions rejected
    framework_state: FrameworkState {
        idempotency: Option<IdempotencyState>,   // watermark + LRU cache
        ingestor:    Option<IngestorState>,      // last_applied_offset
        outbox:      Option<OutboxState>,        // outputs + sent_watermark + next_seq
    },
    user_snapshot: Vec<u8>,         // opaque Service::snapshot() bytes
}
```

幂等状态、Kafka 消费位点和未发送 outbox 随快照复制。leader 切换后可恢复这些状态，避免重复 apply、错误 offset 重拉和 output 丢失。`restore()` 先尝试 framed 解码，magic 不匹配时按旧版裸字节处理；下一次 `snapshot()` 写入新信封。

Idempotency state, Kafka offsets, and unsent outbox outputs replicate with snapshots. After a leader change, restoring them prevents duplicate applies, wrong-offset refetches, and lost outputs. `restore()` tries framed decoding first and treats magic mismatches as legacy raw bytes; the next `snapshot()` writes the new envelope.

## 备份与灾难恢复 / Backup & Disaster Recovery

| 工具 / Tool | 在线 / Online | 行为 / Behavior |
| :--- | :--- | :--- |
| `OnlineBackup` Admin RPC | 是 / Yes | 可选 `force_fresh_snapshot`；按 64 KiB chunk 流式返回单组 FramedSnapshot（`crates/rr-server/src/admin.rs:39` `ONLINE_BACKUP_CHUNK_BYTES`），等待快照最长 60 秒。<br />Optionally uses `force_fresh_snapshot`; streams one group's FramedSnapshot in 64 KiB chunks (`crates/rr-server/src/admin.rs:39`, `ONLINE_BACKUP_CHUNK_BYTES`), waiting up to 60 seconds. |
| `rr-admin backup` / `restore` / `inspect-backup` | 否；须停节点 / No; stop node | 将 `data_dir` 打成含 `metadata.json` 和 `data/...` 的 `.tar.gz`，记录 `format_version`、Rr 版本、时间戳及全量文件 SHA-256（`crates/rr-admin/src/backup.rs`）。`restore` 要求空目录；`inspect-backup` 不解包读取元数据。<br />Archives `data_dir` as `.tar.gz` with `metadata.json` and `data/...`, including `format_version`, Rr version, timestamp, and SHA-256 for all files (`crates/rr-admin/src/backup.rs`). `restore` requires an empty target; `inspect-backup` reads metadata without unpacking. |
| `DrainNode` RPC + `rr-admin node drain` | 是 / Yes | 转走本节点各组 leader，再切换客户端流量。<br />Transfers leadership away on every local group before moving client traffic. |
| `rr-admin node decommission` | 是 / Yes | 逐组 drain → quorum 预检（剩余不足 2 voter 则中止该组）→ `change_membership` 摘除 → 复查。<br />Per group: drain → quorum precheck (abort if fewer than 2 voters remain) → `change_membership` removal → post-check. |
| `rr-admin node-reset` | 否；须停节点 / No; stop node | 集群失去 quorum 或目录损坏时清空 `data_dir`，再以 learner 重挂；包含 RocksDB LOCK、目录标记 pre-flight 和 `--force` 确认。<br />For total quorum loss or directory corruption, wipes `data_dir` before rejoining as a learner; includes RocksDB LOCK and directory-marker preflight checks plus `--force` confirmation. |
| `rr-admin group reset-ingestor-offset` | 是 / Yes | 人工覆盖消费位点；`--offset -1` 清除；记录 warn 级审计。<br />Overrides the consumption offset; `--offset -1` clears it; emits a warn-level audit event. |

缩容由 `MIN_VOTERS_AFTER_REMOVAL = 2` 守卫兜底，见 [成员变更安全护栏](../deployment/security.md)。

Membership shrinking is guarded by `MIN_VOTERS_AFTER_REMOVAL = 2`; see [membership safety guards](../deployment/security.md).

## 当前接线与生产建议 / Current Wiring and Production Guidance

:::warning rocks 模式接线 / rocks-mode Wiring
`rr-server --storage rocks` 为组管理器追加 `with_rocks_base_path("{data_dir}/raft-log")`（`crates/rr-server/src/main.rs`，ADR A-0016）。`start_group` 在 `{data_dir}/raft-log/{group_id}` 打开独立 `RocksLogStorage`，并由 `RaftNode::new_with_rocks_factory` 接入 `sm-snapshot.bin` 与 membership sidecar。`--storage mem` 全内存。`NodeConfig.storage_engine` 是节点共享引擎，并供 managed formation 使用；日志与快照的 rocks 接线仍由 `with_rocks_base_path` 和 `--storage rocks` 控制。

`rr-server --storage rocks` adds `with_rocks_base_path("{data_dir}/raft-log")` to the group manager (`crates/rr-server/src/main.rs`, ADR A-0016). `start_group` opens an independent `RocksLogStorage` at `{data_dir}/raft-log/{group_id}`, and `RaftNode::new_with_rocks_factory` attaches `sm-snapshot.bin` and the membership sidecar. `--storage mem` is memory-only. `NodeConfig.storage_engine` is the node's shared engine and is used by managed formation; log and snapshot wiring is still controlled by `with_rocks_base_path` and `--storage rocks`.
:::

1. **选择后端 / Choose the backend**：需要跨重启保留状态的部署使用 `--storage rocks`；mem 的默认假设是重启即丢失。<br />Use `--storage rocks` when state must survive restart; mem assumes state loss on restart.
2. **库用户 / Library users**：通过 `RaftGroupManager::new_with_network(...)` / `new_with_tcp_network(...)` + `with_rocks_base_path(...)` 接线，并始终使用 `DurabilityPolicy::Strict`。<br />Wire `RaftGroupManager::new_with_network(...)` / `new_with_tcp_network(...)` with `with_rocks_base_path(...)`, and always use `DurabilityPolicy::Strict`.
3. **调整快照 / Tune snapshots**：默认 50_000 在 100k ops/s 下约每 500ms 快照一次；调小可缩短重放，但增加序列化频率。<br />The default 50_000 snapshots about every 500ms at 100k ops/s; lowering it shortens replay but increases serialization frequency.
4. **先 drain / Drain first**：关机前使用 `rr-admin node drain` 或 `DrainNode`；退役使用 `rr-admin node decommission`，不要直接缩到 1 voter。<br />Use `rr-admin node drain` or `DrainNode` before shutdown; decommission with `rr-admin node decommission` instead of shrinking directly to one voter.
5. **定期备份 / Back up regularly**：在线使用 `OnlineBackup`；整目录归档按转 leader → SIGTERM → `rr-admin backup` → 重启执行，并校验 SHA-256。<br />Use `OnlineBackup` for live snapshots; for full-directory archives, transfer leadership → SIGTERM → `rr-admin backup` → restart, then verify SHA-256.
6. **监控失败 / Monitor failures**：`rr_snapshot_build_failures_total` 非零表示快照构建失败，leader 切换后的恢复能力会下降。<br />A nonzero `rr_snapshot_build_failures_total` means snapshot builds are failing and recovery after leader changes is degrading.
7. **修复 Kafka 位点 / Repair Kafka offsets**：灾难后使用 `rr-admin group reset-ingestor-offset` 显式修正异常消费位点。<br />After a disaster, explicitly repair bad consumption offsets with `rr-admin group reset-ingestor-offset`.

## 相关 / Related

- [存储引擎抽象与列族 / Storage engine and column families](./storage.md)
- [快照传输 / Snapshot transfer](./c4/dynamic-snapshot-transfer.md)
- [成员变更守卫 / Membership guard](../deployment/security.md)
- [ADR A-0002 / A-0008 / A-0011 / A-0012 与索引 / ADR A-0002 / A-0008 / A-0011 / A-0012 and index](./design-decisions.md)
- [运维手册 / Runbooks](../operations/runbooks/)
