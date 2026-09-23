---
sidebar_position: 4
---

# 配置提供者 / ConfigProvider

## 什么是 ConfigProvider / What is ConfigProvider

`ConfigProvider` 是 `rr` 框架中用于抽象集群拓扑和路由信息的接口。它负责维护和分发整个集群的状态，包括：

- **分片路由 (Shard Routing)**：分片 ID（Partition ID）到 Raft 组（Group ID）的映射。
- **节点拓扑 (Node Topology)**：节点 ID（Node ID）到网络地址（NodeAddr）的映射。
- **副本管理 (Replica Management)**：Raft 组内的副本列表及其元数据。

---

`ConfigProvider` is the abstraction in the `rr` framework for cluster topology and routing information. It's responsible for maintaining and distributing the state of the entire cluster, including:

- **Shard Routing**: Mapping between Partition IDs and Raft Group IDs.
- **Node Topology**: Mapping between Node IDs and network addresses (NodeAddr).
- **Replica Management**: List of replicas within Raft groups and their metadata.

`ConfigProvider` 可以由不同的后端实现，例如用于本地开发的内存存储、用于生产环境的 etcd 或 ZooKeeper。

---

`ConfigProvider` can be backed by different implementations, such as in-memory storage for local development, or etcd/ZooKeeper for production environments.

## ConfigProvider Trait

核心接口定义在 `rr-core` crate 中。任何配置源都必须实现以下 trait 方法：

---

The core interface is defined in the `rr-core` crate. Any configuration source must implement the following trait methods:

```rust
#[async_trait]
pub trait ConfigProvider: Send + Sync {
    /// 获取当前的完整路由表 / Get the current full routing table.
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError>;

    /// 当前表的共享快照；内存后端覆写为廉价 Arc clone，默认包装
    /// get_routing_table()。每次调用取最新快照，不引入调用方缓存（INV-6）。
    /// Shared snapshot of the current table; in-memory backends override with
    /// a cheap Arc clone. Default wraps get_routing_table(). Every call sees
    /// the latest table — no caller-side cache (INV-6).
    async fn routing_table_snapshot(&self) -> Result<Arc<RoutingTable>, ConfigError> {
        Ok(Arc::new(self.get_routing_table().await?))
    }

    /// 订阅路由表变更事件流 / Subscribe to routing table change events as a stream.
    async fn watch_changes(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError>;

    /// 获取特定节点的 gRPC 地址 / Get the gRPC address for a specific node.
    async fn get_node_addr(&self, node_id: NodeId) -> Result<NodeAddr, ConfigError>;
}
```

## 路由表结构 / RoutingTable Structure

路由表是集群状态的快照。

---

The routing table is a snapshot of the cluster state.

```rust
pub struct RoutingTable {
    /// 分片 ID -> (配置, 组 ID) / partition_id -> (config, group_id)
    pub partitions: HashMap<PartitionId, (PartitionConfig, GroupId)>,
    /// 节点 ID -> 地址 / node_id -> addr
    pub nodes: HashMap<NodeId, NodeAddr>,
    /// 组 ID -> 组元数据 / group_id -> group metadata
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

## 路由表变更事件 / RoutingTableChange Events

当集群拓扑发生变化时，`ConfigProvider` 通过 `watch_changes` 发送事件：

---

When the cluster topology changes, `ConfigProvider` sends events via `watch_changes`:

- `PartitionAdded { partition_id, config, group_id }`: 添加了新的分片。
- `PartitionRemoved { partition_id }`: 删除了现有分片。
- `PartitionUpdated { partition_id, config, group_id }`: 分片配置已更新。
- `NodeAdded { node_id, addr }`: 新节点加入集群。
- `NodeRemoved { node_id }`: 节点已离开集群。
- `GroupMembershipChanged { group_id, replicas }`: Raft 组的副本集发生了变化。

## 内存配置提供者 / MemConfigProvider

`rr-config` 提供了 `MemConfigProvider`，这是一个线程安全的内存实现，非常适合单元测试和单机演示。

---

`rr-config` provides `MemConfigProvider`, a thread-safe in-memory implementation ideal for unit testing and local demos.

```rust
use rr_config::mem::MemConfigProvider;
use rr_core::types::{NodeAddr, PartitionConfig};

let config = MemConfigProvider::new();

// 添加节点 / Add nodes
config.add_node(1, NodeAddr { addr: "127.0.0.1:51001".to_string() }).await;

// 添加分片和 Raft 组 / Add partition and Raft group
config.add_partition(
    0,                      // Partition ID
    PartitionConfig::SingleId(0), 
    1,                      // Group ID
    vec![1, 2, 3]           // Replicas (Node IDs)
).await;
```

## etcd 配置提供者 / EtcdConfigProvider

`rr-config-etcd` crate 提供了基于 etcd v3 的生产级后端 `EtcdConfigProvider`：

---

The `rr-config-etcd` crate provides a production-grade etcd v3 backend, `EtcdConfigProvider`:

```rust
use rr_config_etcd::EtcdConfigProvider;

// 构造函数会立即验证与 etcd 的连通性
// The constructor validates connectivity to etcd immediately
let provider = EtcdConfigProvider::new(
    vec!["http://localhost:2379".to_string()], // etcd endpoints
    "default".to_string(), // cluster_id，用于 key 前缀隔离 / cluster_id, used as key-prefix isolation
).await?;
```

- **只读实现 / Read-only**：`EtcdConfigProvider` 只实现 `get_routing_table` / `routing_table_snapshot` / `watch_changes` / `get_node_addr`。rr 永远不会把路由信息写回 etcd——路由表由外部工具或运维流程写入 etcd，rr 只负责消费。 / It implements only `get_routing_table` / `routing_table_snapshot` / `watch_changes` / `get_node_addr`. rr never writes routing state back to etcd — the routing table is provisioned into etcd by external tooling or operator workflows, and rr only consumes it.
- **读路径缓存 / Read-path caching**：`routing_table_snapshot()` 每次调用只做一次 keys-only 的 revision 探针（默认 serializable，成员本地应答、无 quorum 往返）；revision 未变即服务缓存的 `Arc` 快照，变了才全量重扫。路由判定每次都咨询 etcd（INV-6 不变），但成本从"逐请求全前缀扫描 + 重连"降为一次轻量探针。serializable 探针可能落后 quorum 写一个 apply 延迟（健康集群亚毫秒）；需要 quorum 级新鲜度时用 `with_linearizable_probes(true)`（每调用一次 quorum RTT）。 / `routing_table_snapshot()` performs a tiny keys-only revision probe per call (serializable by default — member-local, no quorum RTT) and only re-scans the full prefix when the revision moved. Serializable probes may lag quorum writes by the member's apply lag; use `with_linearizable_probes(true)` for quorum-fresh probes.
- **Key 布局 / Key layout**：`/rr/{cluster_id}/nodes/{node_id}`、`/rr/{cluster_id}/partitions/{partition_id}`、`/rr/{cluster_id}/groups/{group_id}/replicas`。
- **示例 / Example**：`examples/etcd-config-quickstart` 演示了完整的读取 + watch 流程，运行前需要在 `localhost:2379` 启动一个 etcd v3 实例。 / `examples/etcd-config-quickstart` demonstrates the full read + watch flow; it requires an etcd v3 instance running at `localhost:2379`.

## 实现自定义提供者 / Implementing a Custom Provider

要实现自定义的 `ConfigProvider`（例如基于 ZooKeeper 或 Consul）：

---

To implement a custom `ConfigProvider` (e.g., based on ZooKeeper or Consul):

1. 为你的存储后端实现 `ConfigProvider` trait。
2. 在 `watch_changes` 中，利用后端的监听机制（如 etcd watch）返回一个事件流。
3. `RrNode` 在启动时会调用 `watch_changes` 并在后台循环中自动响应拓扑变化。

---

1. Implement the `ConfigProvider` trait for your storage backend.
2. In `watch_changes`, use the backend's notification mechanism (e.g., etcd watch) to return a stream of events.
3. `RrNode` calls `watch_changes` on startup and automatically reacts to topology changes in a background loop.

## 动态重新配置 / Dynamic Reconfiguration

`RrNode` 运行一个后台任务来监听配置变更：

---

`RrNode` runs a background task to listen for configuration changes:

- **新增分片**：如果当前节点在副本列表中，`RrNode` 会自动启动对应的 Raft 组。
- **成员变更**：`RrNode` 会自动调整本地 Raft 组的成员，或根据需要停止不再属于本节点的 Raft 组。
- **节点更新**：更新内部路由表，确保请求能正确转发到新的节点地址。

整个过程不需要手动重启节点，实现了真正的动态缩扩容。

---

- **New Partitions**: If the current node is in the replica list, `RrNode` automatically starts the corresponding Raft group.
- **Membership Changes**: `RrNode` automatically adjusts local Raft group membership, or stops groups that no longer belong to this node.
- **Node Updates**: Updates the internal routing table to ensure requests are correctly forwarded to new node addresses.

The entire process requires no manual node restarts, enabling true dynamic scaling.
