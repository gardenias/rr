---
sidebar_position: 3
---

# 配置参考 / Configuration Reference

## 概述 / Overview

本页面是 rr 框架中所有配置结构的快速参考。关于如何使用这些配置的上下文，请参阅相应的指南页面。

---

This page is a quick reference for all configuration structs in the rr framework. For context on how to use each configuration, please see the respective guide pages.

## NodeConfig (节点配置)

`NodeConfig` 用于配置 `RrNode` 的运行时行为，包括身份标识、网络监听地址和核心组件。

---

`NodeConfig` is used to configure the runtime behavior of an `RrNode`, including identity, network listening address, and core components.

| 字段 / Field | 类型 / Type | 必填 / Required | 默认值 / Default | 描述 / Description |
| :--- | :--- | :--- | :--- | :--- |
| `node_id` | `NodeId (u64)` | 是 / Yes | — | 集群中节点的唯一标识符 / Unique identifier for the node in the cluster |
| `listen_addr` | `String` | 是 / Yes | — | gRPC 监听地址 (如 "0.0.0.0:51001") / gRPC listen address (e.g., "0.0.0.0:51001") |
| `storage_engine` | `Arc<dyn StorageEngine>` | 是 / Yes | — | 共享存储引擎（供所有 Raft 组使用） / Shared storage engine used by all Raft groups |
| `config_provider` | `Arc<dyn ConfigProvider>` | 是 / Yes | — | 提供路由表和节点地址的配置提供者 / Config provider for routing table and node addresses |
| `state_machine_factory` | `StateMachineFactory` | 是 / Yes | — | 为每个 Raft 组创建状态机实例的工厂 / Factory for creating state machine instances per Raft group |
| `raft_lookup` | `Option<Arc<dyn RaftGroupLookup>>` | 否 / No | `None` | 启用节点间 Raft RPC 的查找接口 / Lookup interface for inter-node Raft RPCs |
| `client_dispatch` | `Option<Arc<dyn ClientDispatch>>` | 否 / No | `None` | 启用面向客户端 RPC 的调度接口 / Dispatch interface for client-facing RPCs |
| `group_manager` | `Option<Arc<RaftGroupManager>>` | 否 / No | `None` | 预构建的组管理器（用于高级网络配置） / Pre-built group manager for advanced network setups |

## RetryConfig (重试配置)

`RetryConfig` 定义了 `RrClient` 在面对网络抖动或领导者切换时的重试策略。

---

`RetryConfig` defines the retry strategy for `RrClient` when encountering network jitters or leader changes.

| 字段 / Field | 类型 / Type | 默认值 / Default | 描述 / Description |
| :--- | :--- | :--- | :--- |
| `max_retries` | `u32` | `3` | 返回错误前的最大重试次数 / Maximum retry attempts before returning an error |
| `retry_delay_ms` | `u64` | `100` | 重试之间的基础延迟（毫秒），每次尝试会翻倍 / Base delay between retries in ms (doubled each attempt) |

## RocksConfig (RocksDB 配置)

`RocksConfig` 用于调优基于 RocksDB 的存储后端性能。

---

`RocksConfig` is used to tune the performance of the RocksDB-based storage backend.

| 字段 / Field | 类型 / Type | 默认值 / Default | 描述 / Description |
| :--- | :--- | :--- | :--- |
| `block_cache_size_mb` | `usize` | `256` | 共享 LRU 块缓存的大小 (MB) / Size of shared LRU block cache in MB |
| `write_buffer_size_mb` | `usize` | `64` | 每个 memtable 的大小 (MB) / Size of each memtable in MB |
| `max_write_buffer_number` | `i32` | `4` | 写入停顿前的最大 memtable 数量 / Maximum number of memtables before stalling writes |
| `max_background_jobs` | `i32` | `4` | 后台压缩和刷写任务的最大数量 / Max number of background compaction/flush jobs |
| `compression` | `DBCompressionType` | `Lz4` | SST 文件使用的压缩算法 / Compression algorithm for SST files |
| `level_zero_slowdown_trigger` | `i32` | `20` | 触发写入放缓的 L0 文件数量 / L0 files that trigger write slowdown |
| `level_zero_stop_trigger` | `i32` | `36` | 触发完全停止写入的 L0 文件数量 / L0 files that trigger full write stop |

## RoutingTable (路由表)

路由表定义了系统的拓扑结构和分片规则。

---

The RoutingTable defines the system topology and shard rules.

```rust
pub struct RoutingTable {
    /// partition_id -> (分区配置, 组 ID)
    pub partitions: HashMap<PartitionId, (PartitionConfig, GroupId)>,
    /// node_id -> 节点地址
    pub nodes: HashMap<NodeId, NodeAddr>,
    /// group_id -> 组元数据
    pub groups: HashMap<GroupId, GroupMeta>,
}

pub enum PartitionConfig {
    /// 单个 ID 匹配
    SingleId(u64),
    /// 范围匹配 (start..end)
    Range { start: u64, end: u64 },
}

pub struct GroupMeta {
    pub group_id: GroupId,
    pub leader: Option<NodeId>,
    pub replicas: Vec<NodeId>,
}
```
