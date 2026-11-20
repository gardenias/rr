---
sidebar_position: 5
---

# 存储引擎 / Storage Engines

`StorageEngine` 抽象带列族的键值存储。仓库提供内存 `MemStorage` 和 feature-gated 的 `RocksStorage`，并在组装节点时传入 `NodeConfig`。
`StorageEngine` abstracts column-family key-value storage. The repository provides in-memory `MemStorage` and feature-gated `RocksStorage`, supplied to `NodeConfig` during node assembly.

| 后端 / Backend | 持久化 / Persistence | 构建要求 / Build Requirement | 用途 / Use |
| :--- | :--- | :--- | :--- |
| `MemStorage` | 重启丢失 / Lost on restart | 无额外 feature / No extra feature | 开发、测试、临时状态 / Development, tests, ephemeral state |
| `RocksStorage` | 本地磁盘持久化 / Persistent on local disk | `rocks` feature、磁盘路径 / `rocks` feature and a disk path | 需要重启恢复的节点 / Nodes requiring restart recovery |

## 内存存储 / MemStorage

`MemStorage` 用 `BTreeMap` 和 `RwLock` 管理各列族，无磁盘 I/O。进程退出或重启后数据丢失。
`MemStorage` manages column families with `BTreeMap` and `RwLock`, with no disk I/O. Data is lost when the process exits or restarts.

```rust
use rr_storage::mem::MemStorage;

let storage = MemStorage::new();
```

## RocksDB 存储 / RocksStorage

在依赖中显式启用 `rocks`；path 依赖按实际仓库位置调整：
Enable `rocks` explicitly in the dependency, adjusting the path to the local checkout:

```toml
[dependencies]
rr-storage = { path = "../rr/crates/rr-storage", features = ["rocks"] }
```

```bash
cargo build --features rocks
cargo test --features rocks
```

`RocksStorage::open_default` 使用 `RocksConfig::default()` 和 `raft_log`、`state_machine`、`metadata` 三个列族。自定义配置通过 `open` 传入。
`RocksStorage::open_default` uses `RocksConfig::default()` and the `raft_log`, `state_machine`, and `metadata` column families. Pass custom settings through `open`.

```rust
use rr_storage::rocks::{RocksConfig, RocksStorage};

let storage = RocksStorage::open_default("/var/lib/rr/node-1")?;

let config = RocksConfig {
    block_cache_size_mb: 512,
    write_buffer_size_mb: 128,
    ..RocksConfig::default()
};
let storage = RocksStorage::open(
    "/var/lib/rr/node-1",
    vec!["raft_log", "state_machine", "metadata"],
    config,
)?;
```

块缓存和写缓冲以 MiB 配置；还可按列族覆盖部分参数。打开非 Strict 策略会记录 WARN。
Block cache and write buffer sizes are in MiB, with selected per-column-family overrides. Opening a non-Strict policy logs a warning.

## 持久性策略 / Durability Policy

`RocksConfig.durability` 控制 WAL 和同步行为。只有 `Strict` 满足 OpenRaft 的持久性契约；较弱策略在掉电时可能丢失已提交日志。
`RocksConfig.durability` controls WAL and synchronization behavior. Only `Strict` satisfies OpenRaft's durability contract; weaker policies can lose committed entries after power loss.

| 策略 / Policy | 配置字符串 / Config String | 行为 / Behavior | 使用边界 / Boundary |
| :--- | :--- | :--- | :--- |
| `DurabilityPolicy::Strict` | `strict` | 每次写同步 WAL / Sync WAL on every write | 默认；生产使用 / Default; production |
| `DurabilityPolicy::ReplicatedOnly` | `replicated_only` | 写 WAL 但不 fsync / Write WAL without fsync | 仅在明确接受多数派同时掉电的数据丢失风险时使用 / Only when correlated quorum power-loss risk is accepted |
| `DurabilityPolicy::TestOnly` | `test_only` | 禁用 WAL 和同步 / Disable WAL and sync | 仅测试 / Tests only |

`ReplicatedOnly` 可应对进程崩溃，但多数派同时掉电可能丢失已确认写入。`group_commit_interval_ms` 可配置周期性 WAL flush，但不会把该策略变为 Strict。
`ReplicatedOnly` can recover from process crashes, but correlated power loss across a majority can lose acknowledged writes. `group_commit_interval_ms` configures periodic WAL flushing; it does not make the policy Strict.

旧字符串 `balanced` 和 `fast` 已在 v1.0.0 移除，配置解析会直接报错；分别改用 `replicated_only` 和 `test_only`。
The legacy strings `balanced` and `fast` were removed in v1.0.0 and now fail configuration parsing; use `replicated_only` and `test_only` instead.

`rr-server --storage rocks` 还会把每组 Raft 日志写入 `{data_dir}/raft-log/{group_id}/`，该日志存储始终使用 Strict。`--durability` 只调整状态机侧的 `RocksStorage`。
`rr-server --storage rocks` also writes each group's Raft log under `{data_dir}/raft-log/{group_id}/`; that log storage always uses Strict. `--durability` affects only the state-machine-side `RocksStorage`.

框架组装可用 `RaftGroupManager::with_rocks_base_path()` 启用同类日志持久化。`kv-store --data-dir` 需要 `rocks` 构建，但其业务 KV map 仍在内存。
Framework assemblies can enable the same durable logs with `RaftGroupManager::with_rocks_base_path()`. `kv-store --data-dir` requires a `rocks` build, while its business KV map remains in memory.

## 自定义存储引擎 / Custom Storage Engines

实现 `rr-core::storage::StorageEngine` 时必须提供 `put`、`get`、`delete`、`scan`、`write_batch`、`flush` 和 `create_cf`。`write_batch` 必须原子执行；`scan` 使用 `[start, end)` 区间。
Implement `rr-core::storage::StorageEngine` with `put`, `get`, `delete`, `scan`, `write_batch`, `flush`, and `create_cf`. `write_batch` must be atomic, and `scan` uses a `[start, end)` range.

`last_key_value` 有 O(n) 扫描默认实现。支持大范围有序 key 的后端应覆盖它；允许 key 达到或超过内部 64 字节 `0xff` 哨兵的后端必须覆盖，否则默认实现可能漏掉尾部 key。
`last_key_value` has an O(n) scan-based default. Ordered backends should override it; backends allowing keys at or beyond the internal 64-byte `0xff` sentinel must override it, or the default may miss the tail key.

`flush()` 是维护性 flush，不是已确认写入的持久性屏障；持久性由每次写的策略决定。
`flush()` is a maintenance operation, not a durability barrier for acknowledged writes; per-write policy determines durability.

## 列族约定 / Column-Family Contract

| 列族 / Column Family | 内容 / Contents |
| :--- | :--- |
| `raft_log` | Raft 日志项 / Raft log entries |
| `state_machine` | 用户状态机数据 / User state-machine data |
| `metadata` | 仅含 `vote` 和 `last_purged`：投票及最近日志清理位置 / Only `vote` and `last_purged`: the vote and last log-purge position |

快照字节及重建所需元数据由 `SnapshotStore` 封装到 `sm-snapshot.bin`，不写入 `metadata` 列族。
Snapshot bytes and their reconstruction metadata are enveloped by `SnapshotStore` in `sm-snapshot.bin`, not stored in the `metadata` column family.

自定义引擎必须保持列族隔离，并按上述契约实现原子写、范围扫描和持久性；不要只让方法编译通过而忽略错误路径。
Custom engines must preserve column-family isolation and the contracts for atomic writes, range scans, and durability; compiling the methods without correct error behavior is insufficient.
