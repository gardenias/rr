---
sidebar_position: 5
---

# 存储引擎 / Storage Engines

## 概述 / Overview

rr 使用 `StorageEngine` trait 来抽象底层存储实现。这允许用户根据具体的应用场景（如开发测试、生产环境、高吞吐需求等）选择不同的存储引擎。

---

rr uses the `StorageEngine` trait to abstract over storage backends. This allows users to choose different storage implementations based on their specific application needs, such as development/testing, production environments, or high-throughput requirements.

rr 内置了两种存储引擎实现：
1. **MemStorage**: 基于内存的存储。
2. **RocksStorage**: 基于 RocksDB 的持久化存储。

存储引擎在启动时作为参数传递给 `NodeConfig` 或直接传递给 `RrNode::new`。

---

rr comes with two built-in storage engine implementations:
1. **MemStorage**: In-memory storage.
2. **RocksStorage**: Persistent storage backed by RocksDB.

The storage engine is provided during startup as a parameter to `NodeConfig` or directly to `RrNode::new`.

## 内存存储引擎 / MemStorage

`MemStorage` 是默认的存储引擎，不需要任何配置即可使用。它在内部使用 `BTreeMap` 来管理每个列族的数据。

---

`MemStorage` is the default storage engine and requires zero configuration. It uses `BTreeMap` internally to manage data for each column family.

**特点 / Characteristics:**
- **非持久化**: 数据在进程重启后会丢失。
- **高性能**: 纯内存操作，无磁盘 I/O 开销。
- **线程安全**: 内部使用 `RwLock` 保证并发访问安全。
- **适用场景**: 开发、自动化测试、或数据不需要持久化的临时任务。

---

- **Non-persistent**: Data is lost once the process restarts.
- **High Performance**: Pure in-memory operations with no disk I/O overhead.
- **Thread-safe**: Uses `RwLock` internally to ensure safe concurrent access.
- **Use Cases**: Development, automated testing, or ephemeral workloads where data persistence isn't required.

**代码示例 / Code Example:**

```rust
use rr_storage::mem::MemStorage;

// 创建默认预定义列族的 MemStorage
// Creates a MemStorage with default column families
let storage = MemStorage::new();

// 在 RrNode 启动时使用
// Use when initializing RrNode
// let node = RrNode::new(config, storage, state_machine, config_provider).await?;
```

## RocksDB 存储引擎 / RocksStorage

`RocksStorage` 是生产环境推荐的存储引擎。它基于 [RocksDB](https://rocksdb.org/)，提供持久化存储和高性能的 LSM-tree 存储架构。

---

`RocksStorage` is the recommended storage engine for production environments. It is based on [RocksDB](https://rocksdb.org/) and provides persistent storage with a high-performance LSM-tree architecture.

### 启用特性 / Enabling the Feature

`RocksStorage` 需要启用 `rocks` feature 才能使用。

---

`RocksStorage` requires the `rocks` feature flag to be enabled.

在 `Cargo.toml` 中配置：
```toml
[dependencies]
rr-storage = { version = "0.2.0", features = ["rocks"] }
```

编译和测试：
```bash
# 使用 rocks 特性编译
cargo build --features rocks

# 运行相关测试
cargo test --features rocks
```

### 配置与使用 / Configuration and Usage

`RocksStorage` 提供了 `RocksConfig` 结构体来调整性能参数，如块缓存大小、写缓冲区大小等。

---

`RocksStorage` provides a `RocksConfig` struct for tuning performance parameters, such as block cache size, write buffer size, etc.

**代码示例 / Code Example:**

```rust
use rr_storage::rocks::{RocksStorage, RocksConfig};

// 使用生产环境默认配置
// Using production-ready default configuration
let config = RocksConfig::default();
let path = "/var/lib/rr/node-1";

// 打开带有默认列族的存储 (raft_log, state_machine, metadata)
// Open storage with default column families (raft_log, state_machine, metadata)
let storage = RocksStorage::open_default(path)?;

// 或者使用自定义配置打开
// Or open with a custom configuration
/*
let custom_config = RocksConfig {
    block_cache_size_mb: 512,
    write_buffer_size_mb: 128,
    ..RocksConfig::default()
};
let storage = RocksStorage::open(path, vec!["raft_log", "state_machine", "metadata"], custom_config)?;
*/
```

### 持久性策略 / Durability Policy

`RocksConfig.durability` 控制 RocksDB 何时把写入 fsync 到磁盘。**只有
`Strict` 满足 OpenRaft 的持久性契约**——其它策略在 power loss 下可能丢失
已 commit 的 log entry，违反 Raft 的安全保证。

---

`RocksConfig.durability` controls when RocksDB fsyncs writes to disk.
**Only `Strict` upholds OpenRaft's durability contract** — any weaker
policy can lose committed log entries on power loss, violating Raft's
safety guarantees.

| Variant | TOML / CLI string | Behaviour | Safe in production? |
| :--- | :--- | :--- | :--- |
| `DurabilityPolicy::Strict` | `"strict"` | fsync WAL on every write | ✅ Yes (default) |
| `DurabilityPolicy::ReplicatedOnly` | `"replicated_only"` | WAL written, no fsync | ⚠️ Only with replication ≥ 3 + external backup |
| `DurabilityPolicy::TestOnly` | `"test_only"` | No WAL, no fsync | ❌ Tests only |

**v0.2.0 rename**: 旧名称 `Balanced` / `Fast` 被重命名为
`ReplicatedOnly` / `TestOnly` 以更明确表达意图。旧名仍作为 `#[deprecated]`
alias 保留一个 minor version；TOML / CLI 也接受旧字符串
（`"balanced"` / `"fast"`）但会在启动时发出 deprecation warning。在 v0.3.0
之前请迁移到新名。

---

**v0.2.0 rename**: the old variants `Balanced` / `Fast` were renamed to
`ReplicatedOnly` / `TestOnly` for clearer intent. The old names remain as
`#[deprecated]` aliases for one minor version; TOML / CLI still accept the
old strings (`"balanced"` / `"fast"`) but emit a deprecation warning at
startup. Migrate to the new names before v0.3.0.

`RocksStorage::open()` 在 policy 不是 `Strict` 时也会在启动日志里发一条
`tracing::warn!`，提醒 ops 这个节点违反了 Raft 的持久性契约。

---

`RocksStorage::open()` additionally emits a `tracing::warn!` at startup
when the policy is not `Strict`, reminding operators that the node
violates Raft's durability contract.

## 选择存储引擎 / Choosing a Storage Engine

根据你的应用阶段和需求选择合适的引擎：

---

Choose the appropriate engine based on your application stage and requirements:

| 特性 / Feature | MemStorage | RocksStorage |
| :--- | :--- | :--- |
| **持久性 / Persistence** | ❌ 重启丢失 / Lost on restart | ✅ 持久存储 / Survives restarts |
| **安装 / Setup** | 无需配置 / Zero config | 需要磁盘路径 / Requires path |
| **Feature 标志 / Flag** | 无 / None | `rocks` |
| **性能 / Performance** | 极快 (内存) / Extremely fast (Memory) | 高吞吐 (LSM) / High throughput (LSM) |
| **场景 / Use Case** | 开发与测试 / Dev & Testing | 生产环境 / Production |

## 实现自定义存储引擎 / Implementing a Custom StorageEngine

如果 rr 内置的存储引擎不能满足需求，你可以通过实现 `rr-core` 中的 `StorageEngine` trait 来创建自定义引擎。

---

If the built-in storage engines don't meet your needs, you can create a custom engine by implementing the `StorageEngine` trait from `rr-core`.

你需要实现以下方法：
- `put`: 插入或更新键值对。
- `get`: 根据键查询值。
- `delete`: 删除键。
- `scan`: 范围扫描。
- `write_batch`: 原子性批量写入。
- `flush`: 刷盘（对于内存引擎可以是 no-op）。
- `create_cf`: 创建新的列族。

---

You need to implement the following methods:
- `put`: Insert or update a key-value pair.
- `get`: Retrieve a value by key.
- `delete`: Remove a key.
- `scan`: Range scan.
- `write_batch`: Atomic batch writes.
- `flush`: Flush to disk (can be a no-op for in-memory engines).
- `create_cf`: Create a new column family.

## 列族 / Column Families

rr 框架内部使用列族（Column Families）来组织不同类型的数据，确保它们在逻辑上是隔离的：

---

The rr framework uses Column Families internally to organize different types of data, ensuring logical isolation:

- **`raft_log`**: 存储 Raft 日志项。 / Stores Raft log entries.
- **`state_machine`**: 存储用户状态机的数据。 / Stores user state machine data.
- **`metadata`**: 存储框架元数据（如投票记录、快照元信息等）。 / Stores framework metadata (e.g., votes, snapshot info).

在使用自定义存储引擎或直接操作 RocksDB 时，请确保正确处理这些列族。

---

When using custom storage engines or interacting with RocksDB directly, ensure these column families are handled correctly.
