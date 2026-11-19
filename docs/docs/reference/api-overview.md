---
sidebar_position: 1
---

# API 概览 / API Overview

本页汇总各 crate 的公共 API；完整签名、默认方法与 trait 契约以 rustdoc 为准。
This page summarizes each crate's public API. Rustdoc is authoritative for complete signatures, default
methods, and trait contracts.

## 各 Crate 摘要 / Per-Crate Summary

| Crate | 用途 / Purpose | 核心公共类型 / Key Public Types | 核心 Trait / Key Traits |
| :--- | :--- | :--- | :--- |
| `rr-core` | 基础类型、Trait 和错误 / Core types, traits, and errors | `NodeId`, `GroupId`, `PartitionId`, `LogIndex`, `NodeAddr`, `RoutingTable`, `RrError` | `Service`, `StateMachine`, `StorageEngine`, `ConfigProvider` |
| `rr-config` | 内存配置提供者 / In-memory config provider | `MemConfigProvider` | `ConfigProvider` |
| `rr-storage` | 存储引擎实现 / Storage engines | `MemStorage`, `RocksStorage`, `RocksConfig` | `StorageEngine` |
| `rr-raft` | Raft 共识引擎 / Raft consensus | `RaftGroupManager`, `RaftNode` | — |
| `rr-network` | gRPC 传输 / gRPC transport | `RaftServiceImpl`, `ClientServiceImpl`, `ChannelPool`, `BearerInterceptor`, `RateLimitInterceptor` | `RaftGroupLookup`, `ClientDispatch` |
| `rr-router` | 分片路由 / Partition routing | `ShardRouter` | — |
| `rr-client` | 客户端 SDK / Client SDK | `RrClient`, `RetryConfig` | — |
| `rr-server` | 节点编排 / Node orchestration | `RrNode`, `NodeConfig` | — |
| `rr-admin` | 管理 CLI/REPL 与只读 Console BFF / Admin CLI/REPL and read-only Console BFF | — | — |

## 管理客户端与 Console / Admin Clients and Console

`rr-admin` CLI/REPL 直接调用节点 `AdminService` gRPC；`rr-admin console` 则是该服务的只读多连接 BFF 客户端。
浏览器使用同源 `/api/v1/console/connections` 管理 profile，并用 `/api/v1/console/connections/{id}/...` 读取
目标集群 JSON/SSE，不直接连接节点 Admin gRPC。Admin 写 RPC 仍由 CLI 或其他受权客户端执行。详见
[Admin API Reference](admin-api.md) 和 [Cluster Admin Console](../operations/cluster-admin-console.md)。
The `rr-admin` CLI/REPL calls node `AdminService` gRPC directly, while `rr-admin console` is a read-only multi-connection
BFF client. Browsers manage profiles through same-origin `/api/v1/console/connections` and read a target's JSON/SSE under
`/api/v1/console/connections/{id}/...`, never connecting to node Admin gRPC directly. Admin write RPCs remain with the CLI
or other authorized clients. See [Admin API Reference](admin-api.md) and
[Cluster Admin Console](../operations/cluster-admin-console.md).

## 核心 Trait / Core Traits

以下片段列出主要方法；带默认实现的扩展方法、生命周期钩子和控制接口见 rustdoc。
The snippets show the main methods. See rustdoc for default extension methods, lifecycle hooks, and
control interfaces.

### `Service`

面向业务逻辑的高层接口，由框架适配到 Raft 状态机。
The high-level business-logic interface, adapted by the framework to a Raft state machine.

```rust
#[async_trait]
pub trait Service: Send + Sync {
    /// 处理客户端请求 / Handle a client request.
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError>;

    /// 服务名称 / Service name.
    fn name(&self) -> &str;

    /// 创建一致的状态快照 / Create a consistent state snapshot.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// 以快照完整替换状态 / Replace all state from a snapshot.
    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError>;
}
```

`snapshot()` 必须与 `handle()` 并发安全；`handle()` 返回前须完成状态更新；`restore()` 必须替换而非
合并状态。完整安全契约见 [Service Trait 指南](../guide/service-trait.md)。
`snapshot()` must be safe to run concurrently with `handle()`; `handle()` must finish mutations before
returning; `restore()` must replace rather than merge state. See the
[Service trait guide](../guide/service-trait.md) for the full safety contract.

### `StateMachine`

直接处理 Raft 日志字节的底层接口。
The low-level interface that applies raw Raft log entries.

```rust
#[async_trait]
pub trait StateMachine: Send + Sync {
    /// 应用已提交日志 / Apply a committed log entry.
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError>;

    /// 创建一致的状态快照 / Create a consistent state snapshot.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// 以快照完整替换状态 / Replace all state from a snapshot.
    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError>;

    /// 最后应用的日志索引 / Last applied log index.
    async fn last_applied(&self) -> Option<LogIndex>;
}
```

### `StorageEngine`

可插拔的列族存储接口。`scan` 的范围为 `[start, end)`。
The pluggable column-family storage interface. `scan` covers `[start, end)`.

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), StorageError>;
    async fn scan(&self, cf: &str, start: &[u8], end: &[u8])
        -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>;
    async fn write_batch(&self, ops: Vec<BatchOp>) -> Result<(), StorageError>;
    async fn flush(&self) -> Result<(), StorageError>;
    async fn create_cf(&self, name: &str) -> Result<(), StorageError>;
}
```

`flush()` 是维护操作，不是已 ACK 写入的持久化屏障；持久性由每次写入的 durability 选项决定。
另有默认 O(n) 的 `last_key_value()`，有序存储后端应覆写它。
`flush()` is maintenance, not a durability barrier for acknowledged writes; per-write durability options
govern that contract. A default O(n) `last_key_value()` is also available and should be overridden by
ordered backends.

### `ConfigProvider`

读取路由表与节点地址，并订阅配置变更。
Reads routing tables and node addresses, and subscribes to configuration changes.

```rust
#[async_trait]
pub trait ConfigProvider: Send + Sync {
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError>;

    /// 默认包装 get_routing_table()；内存后端可覆写为廉价 Arc clone。
    /// Defaults to get_routing_table(); in-memory backends can override with a cheap Arc clone.
    async fn routing_table_snapshot(&self) -> Result<Arc<RoutingTable>, ConfigError>;

    async fn watch_changes(&self)
        -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError>;

    async fn get_node_addr(&self, node_id: NodeId) -> Result<NodeAddr, ConfigError>;
}
```

`watch_changes()` 不发送初始快照；调用方应先读取完整路由表。内存后端在消费者滞后时丢弃事件并告警，
etcd 后端施加背压并在断线后重连。需要零遗漏的消费者应定期全量对账。
`watch_changes()` sends no initial snapshot; fetch the full table first. The in-memory backend drops and
reports events when a consumer lags, while etcd applies backpressure and reconnects after disconnects.
Consumers that cannot miss changes should periodically reconcile the full table.

## 类型别名 / Type Aliases

`rr-core` 定义以下别名 / `rr-core` defines these aliases:

- `NodeId = u64`
- `GroupId = u64`
- `PartitionId = u64`
- `LogIndex = u64`
- `NodeAddr`：包含 `addr: String` 的结构体<br />`NodeAddr`: struct containing `addr: String`

## 生成本地文档 / Generating Local Docs

生成并打开完整 rustdoc / Generate and open the full rustdoc:

```bash
cargo doc --workspace --no-deps --open

# 包含私有项，供贡献者使用 / Include private items for contributors
cargo doc --workspace --no-deps --document-private-items --open
```
