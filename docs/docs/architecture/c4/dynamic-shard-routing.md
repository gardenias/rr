---
sidebar_position: 11
---

# 动态：分片路由 / Dynamic: Shard Routing

每个请求携带 `partition_id`。server 将它解析为 `group_id`，client 再缓存该 group 的当前 leader。

Every request carries a `partition_id`. The server resolves it to a `group_id`, and the client caches the group's
current leader.

## 高层流程 / High-level Flow

```plantuml
@startuml
!include <C4/C4_Dynamic.puml>

title Dynamic Diagram - Partition → Group Resolution

Container(client, "rr-client SDK", "Rust library", "User application")

Container_Boundary(node, "rr-server Node (any)") {
  Component(grpc, "ClientServiceImpl", "rr-network", "gRPC handler")
  Component(dispatch, "ClientDispatchImpl", "rr-server", "Request dispatcher")
  Component(router, "ShardRouter", "rr-router", "Partition → group resolver (no local cache)")
  Component(cfg, "ConfigProvider", "rr-config", "Source of truth; current Arc snapshot per call")
}

Rel(client, grpc, "1. ClientRequest(partition_id=42)", "gRPC")
Rel(grpc, dispatch, "2. dispatch(request)")
Rel(dispatch, router, "3. route(42)")

Rel(router, cfg, "4. routing_table_snapshot() → current Arc<RoutingTable>; partitions[42] → group_id=7")
Rel(router, dispatch, "5. group_id=7 (leader redirect, if any, comes from the raft runtime)")

Rel(cfg, router, "(no subscription in the router — RrNode's watch loop consumes RoutingTableChange events)")

@enduml
```

## 详细时序 / Detailed Sequence

```plantuml
@startuml
autonumber
participant Client as "rr-client SDK"
participant Node as "rr-server Node"
participant Router as "ShardRouter"
participant Cfg as "ConfigProvider"
participant Raft as "RaftGroupManager"

group #LightBlue SERVER ROUTING — every request
  Client -> Node : ClientRequest(partition=42)
  Node -> Router : route(42)
  Router -> Cfg : routing_table_snapshot()
  Cfg --> Router : Arc<RoutingTable> (current point-in-time snapshot)
  Router -> Router : partitions.get(&42) → group_id=7
  Router --> Node : group_id=7

  alt This node IS leader of group 7
    Node -> Raft : client_write on G7
    Raft --> Node : response
    Node --> Client : ClientResponse{success=true, payload}
  else This node is NOT leader
    Node -> Raft : client_write fails (ForwardToLeader from openraft)
    Node --> Client : ClientResponse{success=false, leader_info={node_id:2, addr:"host-2:51001"}}
  end
end

group #LightGreen CLIENT FAST PATH — cached leader
  Client -> Client : leader_cache[group=7] hit → use "host-2:51001"
  Client -> Node : direct to node_2
  note over Node : server still resolves route() on each request:\ncurrent provider snapshot + O(1) hashmap lookup
  Node -> Raft : client_write
  Raft --> Client : ClientResponse{success=true, payload}
end

group #Wheat CHANGE — routing table updates
  note over Cfg : The table is mutated out-of-band (MemConfigProvider\ninherent methods / external store), NOT via the trait.
  Cfg -> Cfg : emit RoutingTableChange::GroupMembershipChanged{group_id:7, replicas:[1,2,4]}
  note over Node : RrNode's watch loop consumes the event\n(start/stop groups as needed). The router holds no state —\nits NEXT route(42) reads the updated provider snapshot.
end
@enduml
```

## 两层路由 / Two Routing Layers

### 第 1 层：服务端分片路由 / Layer 1: Server-side Shard Routing

每个节点的 `ShardRouter` 只负责 `partition_id → group_id`。它每次读取 provider 当前快照，没有本地缓存；
内存 provider 返回 `Arc` 快照，因此不复制整表。跨节点是否一致取决于各节点 provider 的更新。

Each node's `ShardRouter` only resolves `partition_id → group_id`. It reads the provider's current snapshot on
every call and has no local cache; an in-memory provider returns an `Arc` snapshot without copying the table.
Cross-node consistency depends on each node's provider being updated.

`RrNode` 消费 `watch_changes()` 以启停 group；router 本身不订阅，热路径不跨节点通信。

`RrNode` consumes `watch_changes()` to start or stop groups; the router does not subscribe, and its hot path makes
no inter-node calls.

### 第 2 层：客户端 Leader 缓存 / Layer 2: Client-side Leader Cache

`RrClient` 维护无 TTL 的 `HashMap<GroupId, (NodeId, String)>`，避免每次先访问 follower。该缓存由
`leader_info` 重定向刷新；`GetClusterInfo` 虽能报告运行时拓扑，SDK 不用它轮询 leader。

`RrClient` maintains a `HashMap<GroupId, (NodeId, String)>` without TTL to avoid contacting a follower first.
`leader_info` redirects refresh this cache; although `GetClusterInfo` reports runtime topology, the SDK does not
poll it for leaders.

| 场景 / Scenario | 缓存 / Cache | 结果 / Outcome |
| :--- | :--- | :--- |
| Leader 变更 / Leader changed | 过期 / Stale | 请求旧 leader，收到重定向或连接失败，然后更新并重试 / Contact old leader, redirect or fail, then update and retry |
| 新分片或 group / New partition or group | 未命中 / Missing | 轮询 bootstrap；成功或重定向响应填充缓存 / Use bootstrap; success or redirect fills the cache |

## 由应用计算 Partition ID / Application-computed Partition IDs

分区策略属于应用语义，rr 不猜测按用户、地域还是时间分片。应用必须保持计算稳定，否则相同 key 可能映射
到不同 partition。

Partitioning is application semantics: rr does not choose between user, geography, time, or another key. The
application must keep the computation stable, or the same key can map to different partitions.

```rust
let partition_id = my_hash(key) % NUM_PARTITIONS;
let resp = client.send_request(partition_id, payload).await?;
```

## 路由表变更 / Routing-table Changes

`RoutingTableChange` 有 6 个变体。router 的下一次调用读取新快照；事件由 `RrNode` watch loop 消费。

`RoutingTableChange` has six variants. The router's next call reads the new snapshot; `RrNode`'s watch loop
consumes the events.

| 事件 / Event | Variant | RrNode 反应 / Reaction |
| :--- | :--- | :--- |
| 新分片 / Partition added | `PartitionAdded { partition_id, config, group_id }` | 本节点是副本时启动 group / Start group if local node is a replica |
| 删除分片 / Partition removed | `PartitionRemoved { partition_id }` | 记录日志 / Log only |
| 更新分片 / Partition updated | `PartitionUpdated { partition_id, config, group_id }` | 记录日志 / Log only |
| 新节点 / Node added | `NodeAdded { node_id, addr }` | 对本地 group 尝试 `add_learner` / Try `add_learner` on local groups |
| 删除节点 / Node removed | `NodeRemoved { node_id }` | 在 quorum guard 下移除成员 / Remove membership with quorum guard |
| 副本集变更 / Replicas changed | `GroupMembershipChanged { group_id, replicas }` | 按本地是否在副本集中启停 group / Start or stop local group |

leader 选举不产生路由表事件。客户端通过 `leader_info` 或连接失败发现变化，更新缓存后重试。

Leader elections produce no routing-table events. Clients detect changes through `leader_info` or connection
failure, update their cache, and retry.

## 相关 / Related

- [配置传播 / Config propagation](./dynamic-config-update.md)
- [客户端重定向 / Client redirects](./dynamic-put-request.md)
- [路由组件 / Router components](./components-router.md)
