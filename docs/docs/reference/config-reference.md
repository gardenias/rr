---
sidebar_position: 3
---

# 配置参考 / Configuration Reference

本页列出核心 Rust 配置类型；完整字段、TOML 配置和部署选项见相应指南与 rustdoc。
This page covers core Rust configuration types. See the relevant guides and rustdoc for complete fields,
TOML configuration, and deployment options.

## 节点配置 / Node Configuration

`NodeConfig` 配置 `RrNode` 的身份、监听地址和核心组件。下表包含构造参数与常用可选字段。
Configures an `RrNode`'s identity, listen address, and core components. The table covers constructor
arguments and common optional fields.

| 字段 / Field | 类型 / Type | 必填 / Required | 默认值 / Default | 描述 / Description |
| :--- | :--- | :--- | :--- | :--- |
| `node_id` | `NodeId (u64)` | 是 / Yes | — | 集群内唯一节点 ID / Unique node ID |
| `listen_addr` | `String` | 是 / Yes | — | gRPC 监听地址 / gRPC listen address |
| `storage_engine` | `Arc<dyn StorageEngine>` | 是 / Yes | — | 所有 Raft 组共享的存储 / Storage shared by all Raft groups |
| `config_provider` | `Arc<dyn ConfigProvider>` | 是 / Yes | — | 路由表和节点地址来源 / Routing-table and node-address source |
| `state_machine_factory` | `StateMachineFactory` | 是 / Yes | — | 按 Raft 组创建状态机 / Creates a state machine per group |
| `raft_lookup` | `Option<Arc<dyn RaftGroupLookup>>` | 否 / No | `None` | 启用节点间 Raft RPC / Enables inter-node Raft RPCs |
| `client_dispatch` | `Option<Arc<dyn ClientDispatch>>` | 否 / No | `None` | 启用客户端 RPC / Enables client RPCs |
| `group_manager` | `Option<Arc<RaftGroupManager>>` | 否 / No | `None` | 预构建组管理器 / Pre-built group manager |
| `metrics_port` | `Option<u16>` | 否 / No | `None` | Prometheus HTTP 端口 / Prometheus HTTP port |
| `admin_port` | `Option<u16>` | 否 / No | `None` | Admin gRPC 端口 / Admin gRPC port |
| `admin_bind_host` | `String` | 否 / No | `127.0.0.1` | Admin 绑定地址 / Admin bind host |
| `metrics_bind_host` | `String` | 否 / No | `127.0.0.1` | 可观测端点绑定地址 / Observability bind host |

## 重试配置 / Retry Configuration

`RetryConfig` 定义 `RrClient` 对瞬时故障与 Leader 切换的重试策略。
Defines how `RrClient` retries transient failures and leader changes.

| 字段 / Field | 类型 / Type | 默认值 / Default | 描述 / Description |
| :--- | :--- | :--- | :--- |
| `max_retries` | `u32` | `3` | 返回错误前的最大重试次数 / Maximum retries before returning an error |
| `retry_delay_ms` | `u64` | `100` | 基础重试延迟（毫秒），逐次翻倍 / Base retry delay in ms, doubled per attempt |

## RocksDB 配置 / RocksDB Configuration

`RocksConfig` 调优 RocksDB 后端。`for_testing()` 使用另一组测试专用默认值，不适用下表。
Tunes the RocksDB backend. `for_testing()` uses separate test-only defaults not shown here.

| 字段 / Field | 类型 / Type | 默认值 / Default | 描述 / Description |
| :--- | :--- | :--- | :--- |
| `block_cache_size_mb` | `usize` | `256` | 共享 LRU 块缓存（MB）/ Shared LRU block cache in MB |
| `write_buffer_size_mb` | `usize` | `64` | 每个 memtable 的大小（MB）/ Size of each memtable in MB |
| `max_write_buffer_number` | `i32` | `4` | 写入停顿前的最大 memtable 数 / Maximum memtables before writes stall |
| `max_background_jobs` | `i32` | `4` | 后台压缩与刷写任务数 / Background compaction and flush jobs |
| `compression` | `DBCompressionType` | `Lz4` | SST 压缩算法 / SST compression |
| `level_zero_slowdown_trigger` | `i32` | `20` | 触发写入放缓的 L0 文件数 / L0 files triggering write slowdown |
| `level_zero_stop_trigger` | `i32` | `36` | 触发写入停止的 L0 文件数 / L0 files triggering a full write stop |
| `durability` | `DurabilityPolicy` | `Strict` | 持久化与 fsync 策略 / Durability and fsync policy |
| `raft_log_cf` | `Option<CfOptions>` | `None` | Raft 日志列族覆写 / Raft-log column-family overrides |
| `state_machine_cf` | `Option<CfOptions>` | `None` | 状态机列族覆写 / State-machine column-family overrides |
| `group_commit_interval_ms` | `Option<u64>` | `None` | 后台 WAL 刷写间隔（毫秒）/ Background WAL flush interval in ms |

:::warning 持久性 / Durability
将 `durability` 降为 `Strict` 以下会违反 Raft 持久性契约；仅用于明确接受该风险的场景。
Setting `durability` below `Strict` violates Raft's durability contract. Do so only when that risk is
explicitly accepted.
:::

## 路由表 / Routing Table

`RoutingTable` 定义分区、节点与 Raft 组的配置面拓扑。
Defines the configuration-plane topology of partitions, nodes, and Raft groups.

```rust
pub struct RoutingTable {
    /// partition_id -> (分区配置, 组 ID) / partition ID -> (config, group ID)
    pub partitions: HashMap<PartitionId, (PartitionConfig, GroupId)>,
    /// node_id -> 节点地址 / node ID -> address
    pub nodes: HashMap<NodeId, NodeAddr>,
    /// group_id -> 组元数据 / group ID -> metadata
    pub groups: HashMap<GroupId, GroupMeta>,
}

pub enum PartitionConfig {
    SingleId(u64),
    Range { start: u64, end: u64 },
}

pub struct GroupMeta {
    pub group_id: GroupId,
    pub replicas: Vec<NodeId>,
}
```

`Range` 目前为保留变体：路由器只支持按 `SingleId` 精确查找，配置提供者会响亮拒绝 `Range`，
不要写入该变体。`GroupMeta` 仅描述静态副本位置，不包含运行时 Leader。
`Range` is reserved: routing supports exact `SingleId` lookups only, and providers reject `Range`
loudly. Do not write it. `GroupMeta` describes static replica placement and contains no runtime leader.
