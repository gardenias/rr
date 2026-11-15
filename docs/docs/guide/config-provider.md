---
sidebar_position: 4
---

# 配置提供者 / ConfigProvider

`ConfigProvider` 提供分片到 Raft 组的路由、节点地址和组副本元数据。仓库实现包括 `MemConfigProvider` 和只读 etcd 后端 `EtcdConfigProvider`。
`ConfigProvider` supplies partition-to-group routing, node addresses, and group replica metadata. Implementations include `MemConfigProvider` and the read-only etcd-backed `EtcdConfigProvider`.

## Trait 与路由表 / Trait and Routing Table

接口定义在 `rr-core`：
The interface is defined in `rr-core`:

```rust
#[async_trait]
pub trait ConfigProvider: Send + Sync {
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError>;

    async fn routing_table_snapshot(&self) -> Result<Arc<RoutingTable>, ConfigError> {
        Ok(Arc::new(self.get_routing_table().await?))
    }

    async fn watch_changes(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError>;

    async fn get_node_addr(&self, node_id: NodeId) -> Result<NodeAddr, ConfigError>;
}
```

每次读取都获取提供方当前的 point-in-time 快照，不在调用方引入另一份权威缓存。内存后端可用廉价 `Arc` clone 覆盖默认实现。
Each read obtains the provider's current point-in-time snapshot without introducing another caller-side authority. In-memory backends can override the default with a cheap `Arc` clone.

```rust
pub struct RoutingTable {
    pub partitions: HashMap<PartitionId, (PartitionConfig, GroupId)>,
    pub nodes: HashMap<NodeId, NodeAddr>,
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

## 变更流语义 / Change Stream Semantics

`watch_changes` 的事件包括 `PartitionAdded`、`PartitionRemoved`、`PartitionUpdated`、`NodeAdded`、`NodeRemoved` 和 `GroupMembershipChanged`。
`watch_changes` emits `PartitionAdded`, `PartitionRemoved`, `PartitionUpdated`, `NodeAdded`, `NodeRemoved`, and `GroupMembershipChanged` events.

- 流不包含初始快照；先调用 `get_routing_table()`，再应用订阅后的事件。/ There is no initial snapshot; fetch the table first, then apply post-subscription events.
- etcd 的 nodes、partitions、groups 是独立 watcher，不保证跨域全局顺序。/ etcd uses independent watchers with no global cross-domain ordering.
- 内存后端在消费者落后时丢弃事件并记录 WARN/指标；etcd 有界通道施加背压。/ The in-memory backend drops lagged events with a warning and metric; etcd's bounded channel applies backpressure.
- 不能丢事件的消费者应定期重新读取完整表；流终止时重新读取并订阅。/ Consumers that cannot miss events should periodically refetch the table; on termination, refetch and resubscribe.

组删除当前表示为空副本集，没有独立 `GroupRemoved` 事件。内存 `remove_node` 会从副本列表删除节点但不另发 `GroupMembershipChanged`；etcd 不做这项删除。
Group deletion currently appears as an empty replica set; there is no separate `GroupRemoved`. In-memory `remove_node` strips the node from replica lists without another membership event; etcd does not strip it.

## 内存提供者 / MemConfigProvider

`MemConfigProvider` 适合测试和本地演示：
`MemConfigProvider` is intended for tests and local demos:

```rust
use rr_config::mem::MemConfigProvider;
use rr_core::types::{NodeAddr, PartitionConfig};

let config = MemConfigProvider::new();
config.add_node(1, NodeAddr {
    addr: "127.0.0.1:51001".to_string(),
}).await;
config.add_partition(
    0,
    PartitionConfig::SingleId(0),
    1,
    vec![1, 2, 3],
).await;
```

这些写入只修改本地内存表，不会自动配置其他进程。
These writes modify only the local in-memory table; they do not configure other processes.

## etcd 提供者 / EtcdConfigProvider

`rr-config-etcd` 的构造函数会立即验证连通性。`cluster_id` 用于 key 前缀隔离。
The `rr-config-etcd` constructor validates connectivity immediately. `cluster_id` isolates key prefixes.

```rust
use rr_config_etcd::EtcdConfigProvider;

let provider = EtcdConfigProvider::new(
    vec!["http://localhost:2379".to_string()],
    "default".to_string(),
).await?;
```

该实现只读，Rr 不把路由写回 etcd；外部工具负责写入以下 key：
The provider is read-only; Rr never writes routing state back to etcd. External tooling provisions these keys:

```text
/rr/{cluster_id}/nodes/{node_id}
/rr/{cluster_id}/partitions/{partition_id}
/rr/{cluster_id}/groups/{group_id}/replicas
```

`routing_table_snapshot()` 默认以 serializable keys-only revision 探针检查缓存，revision 变化才全量重扫。需要 quorum 新鲜度时调用 `with_linearizable_probes(true)`，代价是每次调用一次 quorum 往返。
`routing_table_snapshot()` uses a serializable keys-only revision probe by default and rescans only when the revision changes. Use `with_linearizable_probes(true)` for quorum-fresh probes at the cost of one quorum round trip per call.

完整读取和 watch 示例见 `examples/etcd-config-quickstart`，运行前在 `localhost:2379` 启动 etcd v3。
See `examples/etcd-config-quickstart` for reading and watching; start etcd v3 at `localhost:2379` first.

## 自定义提供者 / Custom Providers

自定义后端需实现 trait，将后端通知转换为变更流，并明确初始快照、断线、顺序和 lag 行为。`RrNode` 启动时读取表并运行 watch loop。
A custom backend implements the trait, converts backend notifications into the change stream, and defines snapshot, disconnect, ordering, and lag behavior. `RrNode` reads the table at startup and runs a watch loop.

## 托管 SeedSpec 与生命周期 / Managed SeedSpec and Lifecycle

配置 `[cluster.formation]` 后，节点从相同 SeedSpec 构造静态路由，并使用独立 `LifecycleStore` 授权 Genesis、恢复和旧集群收养。`[cluster.formation].etcd_endpoints` 属于生命周期存储，不会让 `EtcdConfigProvider` 变成可写成员表。
With `[cluster.formation]`, nodes build static routing from an identical SeedSpec and use a separate `LifecycleStore` to authorize Genesis, recovery, and legacy adoption. Formation endpoints belong to the lifecycle store; they do not make `EtcdConfigProvider` a writable membership authority.

- SeedSpec 的 groups/replicas 只描述初始路由和 Genesis 授权；当前 voter/learner 以 Raft membership 为准。/ Seed groups and replicas describe initial routing and Genesis authorization; current Raft membership is authoritative.
- managed 节点禁用旧 watch loop 的成员副作用；Active 记录下的空节点等待复制。/ Managed nodes suppress legacy watch-loop membership side effects; pristine nodes with an Active record wait for replication.
- 修改 SeedSpec 会改变 seed identity，并被现有生命周期记录拒绝；该路径不支持动态扩缩容或 resharding。/ Changing SeedSpec changes seed identity and is rejected by the existing record; this path does not support dynamic scaling or resharding.
- 存储中断不会清空路由：已初始化节点可继续 Raft 流量但 readiness degraded，空节点不初始化且不就绪。/ A store outage does not clear routing: initialized nodes may continue Raft traffic with degraded readiness, while pristine nodes neither initialize nor become ready.

## 旧模式动态行为 / Legacy Dynamic Behavior

以下副作用仅适用于未配置 `[cluster.formation]` 的旧模式节点：新分片可在本节点副本列表包含本节点时启动组；成员事件可调整或停止本地组；节点事件更新路由地址。
The following side effects apply only to legacy nodes without `[cluster.formation]`: new partitions can start groups when the local node is a replica; membership events can adjust or stop local groups; node events update routing addresses.

不要把这些 watch 副作用当作当前 Raft membership 的权威来源，也不要据此假定所有扩缩容操作都已自动完成。
Do not treat these watch side effects as the authority for current Raft membership or assume that every scaling operation completes automatically.
