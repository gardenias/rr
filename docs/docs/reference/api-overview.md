---
sidebar_position: 1
---

# API 概览 / API Overview

## 概述 / Overview

本页摘要了 rr 框架各 crate 的公共 API。
---
This page summarizes the public API of each rr crate.

如需查看完整的 API 文档，请生成 rustdoc：
---
For full API documentation, please generate rustdoc:

```bash
cargo doc --workspace --no-deps --open
```

## 各 Crate 摘要 / Per-Crate Summary

| Crate | 描述 / Description | 核心公共类型 / Key Public Types | 核心 Trait / Key Traits |
|-------|-------------|-----------------|------------|
| `rr-core` | 基础类型、Trait 和错误 / Foundation types, traits, errors | `NodeId`, `GroupId`, `PartitionId`, `LogIndex`, `NodeAddr`, `RoutingTable`, `RrError` | `Service`, `StateMachine`, `StorageEngine`, `ConfigProvider` |
| `rr-config` | 配置提供者实现 / Config provider implementations | `MemConfigProvider` | `ConfigProvider` |
| `rr-storage` | 存储引擎实现 / Storage engine implementations | `MemStorage`, `RocksStorage`, `RocksConfig` | `StorageEngine` |
| `rr-raft` | Raft 共识引擎 / Raft consensus engine | `RaftGroupManager`, `RaftNode` | — |
| `rr-network` | gRPC 传输层 / gRPC transport | `RaftServiceImpl`, `ClientServiceImpl`, `ChannelPool`, `BearerInterceptor`, `RateLimitInterceptor` | `RaftGroupLookup`, `ClientDispatch` |
| `rr-router` | 分片路由 / Partition routing | `ShardRouter` | — |
| `rr-client` | 客户端 SDK / Client SDK | `RrClient`, `RetryConfig` | — |
| `rr-server` | 节点编排 / Node orchestration | `RrNode`, `NodeConfig` | — |

## 核心 Trait 签名 / Core Trait Signatures

### Service Trait

`Service` 是用于实现高层业务逻辑的 Trait，用户无需关心 Raft 内部细节。
---
`Service` is a high-level trait for implementing business logic without worrying about Raft internals.

```rust
#[async_trait]
pub trait Service: Send + Sync {
    /// 处理客户端请求 / Handle an incoming client request.
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError>;

    /// 返回服务名称 / Return the name of this service.
    fn name(&self) -> &str;

    /// 创建状态快照 / Create a snapshot of the service state.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// 从快照恢复状态 / Restore service state from a snapshot.
    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError>;
}
```

### StateMachine Trait

`StateMachine` 是底层 Trait，直接接收 Raft 日志条目。
---
`StateMachine` is a low-level trait that directly receives Raft log entries.

```rust
#[async_trait]
pub trait StateMachine: Send + Sync {
    /// 应用已提交的日志条目 / Apply a committed Raft log entry.
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError>;

    /// 创建一致性状态快照 / Create a consistent snapshot of the current state.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// 从快照恢复状态 / Restore state from a snapshot.
    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError>;

    /// 返回最后应用的日志索引 / Return the index of the last applied log entry.
    async fn last_applied(&self) -> Option<LogIndex>;
}
```

### StorageEngine Trait

`StorageEngine` 定义了可插拔的存储后端接口。
---
`StorageEngine` defines the interface for pluggable storage backends.

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// 插入或更新键值对 / Insert or update a key-value pair.
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError>;

    /// 根据键获取值 / Get a value by key.
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;

    /// 删除键 / Delete a key.
    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), StorageError>;

    /// 范围扫描 / Scan a range of keys [start, end).
    async fn scan(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>;

    /// 原子应用批量操作 / Apply multiple operations atomically.
    async fn write_batch(&self, ops: Vec<BatchOp>) -> Result<(), StorageError>;

    /// 刷新写入到持久化存储 / Flush pending writes to durable storage.
    async fn flush(&self) -> Result<(), StorageError>;

    /// 创建列族 / Create a new column family.
    async fn create_cf(&self, name: &str) -> Result<(), StorageError>;
}
```

### ConfigProvider Trait

`ConfigProvider` 用于读取集群配置（如路由表、节点地址）。
---
`ConfigProvider` is used for reading cluster configuration (e.g., routing table, node addresses).

```rust
#[async_trait]
pub trait ConfigProvider: Send + Sync {
    /// 获取当前完整的路由表 / Get the current full routing table.
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError>;

    /// 当前路由表的共享快照；内存后端应覆写为廉价 Arc clone。
    /// 默认实现包装 get_routing_table()。每次调用都取最新快照（INV-6）。
    /// Current table as a shared snapshot; in-memory backends should override
    /// with a cheap Arc clone. Default wraps get_routing_table().
    async fn routing_table_snapshot(&self) -> Result<Arc<RoutingTable>, ConfigError>;

    /// 订阅路由表变更事件流 / Subscribe to routing table change events as a stream.
    async fn watch_changes(&self) -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError>;

    /// 获取特定节点的 gRPC 地址 / Get the gRPC address for a specific node.
    async fn get_node_addr(&self, node_id: NodeId) -> Result<NodeAddr, ConfigError>;
}
```

## 类型别名 / Type Aliases

定义在 `rr-core` 中的核心类型别名：
---
Core type aliases defined in `rr-core`:

- `NodeId = u64`
- `GroupId = u64`
- `PartitionId = u64`
- `LogIndex = u64`
- `NodeAddr`: 包含 `addr: String` 的结构体 / Struct containing `addr: String`

## 生成本地文档 / Generating Local Docs

你可以通过以下命令在本地生成并查看完整的 API 文档：
---
You can generate and view the full API documentation locally using the following commands:

```bash
# 为所有 crate 生成并打开 rustdoc / Generate and open rustdoc for all crates
cargo doc --workspace --no-deps --open

# 生成包含私有项的文档（供贡献者使用） / Generate with private items (for contributors)
cargo doc --workspace --no-deps --document-private-items --open
```
