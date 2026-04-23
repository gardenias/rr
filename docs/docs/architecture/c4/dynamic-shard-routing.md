---
sidebar_position: 11
---

# Dynamic — Shard Routing (Partition → Group → Leader)

每个请求都携带一个 `partition_id`。rr 需要把它解析成**"该发给哪个节点的哪个 Raft group"**。本图展示完整的解析链路和缓存策略。

---

Every request carries a `partition_id`. rr must resolve it into **"which Raft group on which node should handle this"**. This diagram shows the full resolution path and caching strategy.

## 高层流程（C4 Dynamic）/ High-Level Flow (C4 Dynamic)

```mermaid
C4Dynamic
  title Dynamic Diagram - Partition → Group → Leader Resolution

  Container(client, "rr-client SDK", "Rust library", "User application")

  Container_Boundary(node, "rr-server Node (any)") {
    Component(grpc, "ClientServiceImpl", "rr-network", "gRPC handler")
    Component(dispatch, "ClientDispatchImpl", "rr-server", "Request dispatcher")
    Component(router, "ShardRouter", "rr-router", "Partition → group → leader resolver")
    Component(table, "Cached RoutingTable", "rr-core::types", "ArcSwap snapshot")
  }

  Container_Ext(cfg, "ConfigProvider", "rr-config", "Source of truth")

  Rel(client, grpc, "1. ClientRequest(partition_id=42)", "gRPC")
  Rel(grpc, dispatch, "2. dispatch(request)")
  Rel(dispatch, router, "3. route(42)")

  Rel(router, table, "4a. lookup partitions[42] → GroupMeta{group_id=7, leader=node_2}")
  Rel(router, table, "4b. lookup nodes[node_2] → addr='host-2:51001'")
  Rel(router, dispatch, "5. ResolvedRoute{group_id=7, leader_node=2, leader_addr='host-2:51001'}")

  Rel(cfg, table, "(async) RoutingTableChange events keep snapshot fresh")

  UpdateLayoutConfig($c4ShapeInRow="4", $c4BoundaryInRow="1")
```

## 详细时序图 / Detailed Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Client as rr-client SDK
    participant Node as rr-server Node
    participant Router as ShardRouter
    participant Snap as ArcSwap<RoutingTable>
    participant Cfg as ConfigProvider
    participant Raft as RaftGroupManager

    rect rgb(240, 248, 255)
        Note over Client,Raft: COLD PATH — first request after boot
    end

    Client->>Node: ClientRequest(partition=42)
    Node->>Router: route(42)
    Router->>Snap: load() → RoutingTable snapshot
    Snap-->>Router: &RoutingTable
    Router->>Router: partitions.get(&42) → GroupMeta{group_id:7, members:[1,2,3], leader:2}
    Router->>Router: nodes.get(&2) → NodeAddr("host-2:51001")
    Router-->>Node: Resolved(group=7, leader_id=2, addr="host-2:51001")

    alt This node IS leader of group 7
        Node->>Raft: client_write on G7
        Raft-->>Node: response
        Node-->>Client: ClientWriteResponse
    else This node is NOT leader
        Node-->>Client: ForwardToLeader{node_id:2, addr:"host-2:51001"}
    end

    rect rgb(240, 255, 240)
        Note over Client,Raft: HOT PATH — subsequent requests for partition 42
    end

    Client->>Client: leader_cache[group=7] hit → use "host-2:51001"
    Client->>Node: direct to node_2
    Note over Node: (same route() resolution happens on each node<br/>— but resolution is O(1) hashmap lookup)
    Node->>Raft: client_write
    Raft-->>Client: ClientWriteResponse

    rect rgb(255, 248, 240)
        Note over Client,Raft: CHANGE — routing table updates
    end

    Cfg->>Snap: broadcast RoutingTableChange::LeaderChanged{group:7, leader:3}
    Snap->>Snap: ArcSwap::store(new RoutingTable)
    Note over Router: Next route(42) sees leader=3 without locking<br/>(ArcSwap gives us lock-free snapshot reads)
```

## 查询的两层 / The Two Layers

### 层 1: 服务端路由（ShardRouter）/ Layer 1: Server-side routing
每个 rr-server 都有 `ShardRouter`。它拿到一个 `partition_id`，返回 `(group_id, leader_addr)`。

Every rr-server has a `ShardRouter`. Given a `partition_id`, returns `(group_id, leader_addr)`.

**特性 / Characteristics**:
- **O(1) 查询**：两次 HashMap lookup（partition → group → node_addr）
- **无锁读**：通过 `ArcSwap<RoutingTable>` 实现
- **最终一致**：路由表变更通过 `ConfigProvider::subscribe_changes` 异步传播
- **per-node**：每个节点独立缓存；热路径不跨节点通信

---

- **O(1) lookup**: two HashMap lookups (partition → group → node_addr)
- **Lock-free reads**: via `ArcSwap<RoutingTable>`
- **Eventually consistent**: routing table changes propagate asynchronously via `ConfigProvider::subscribe_changes`
- **Per-node**: each node caches independently; hot path has zero inter-node traffic

### 层 2: 客户端 leader 缓存 / Layer 2: Client-side leader cache
`RrClient` 额外维护自己的 `HashMap<GroupId, CachedLeader>`——避免每次请求都先打到 follower 再被重定向。

`RrClient` additionally maintains its own `HashMap<GroupId, CachedLeader>` — avoiding the follower-then-redirect roundtrip on every request.

**注意这**两个缓存可能不同步**：**

**Note these two caches may drift**:

| 场景 / Scenario | Server-side | Client-side | 结果 / Outcome |
| :--- | :--- | :--- | :--- |
| Leader changed recently | Updated (got broadcast) | Stale | Client gets ForwardToLeader → updates |
| Leader changed very recently | Stale (not yet broadcast) | Stale | Client sends to old leader → old leader knows it stepped down → returns error → client retries, eventually hits new leader |
| Partition added | Updated | Missing | Client gets ClusterInfo refresh → updates |

**设计原则 / Design principle**：
*宁可让两层缓存短暂不一致，也不要在热路径上加锁。*

*Allow brief inconsistency between the two caches rather than locking the hot path.*

## 为什么 partition_id 来自客户端？/ Why Does partition_id Come From Client?

rr 把**分区键计算**的责任留给应用：

rr leaves **partition key computation** to the application:

```rust
// Application code:
let partition_id = my_hash(key) % NUM_PARTITIONS;
let resp = client.send_request(partition_id, payload).await?;
```

**为什么不让 rr 内部做？/ Why not have rr compute it internally?**

1. **语义 / Semantics**：分区策略是业务决策——按 user_id？按地理位置？按时间范围？框架无法猜测。
2. **稳定性 / Stability**：哈希函数变化会导致所有数据重新分片；应用控制比框架内置更灵活。
3. **调试 / Debugging**：应用层能打印 `partition_id`，方便故障排查。

**代价 / Tradeoff**：应用必须保持分区键计算的一致性——否则会出现"相同的 key 映射到不同 partition"的分裂数据。

---

1. **Semantics**: partition strategy is a business decision — by user_id? geography? time range? The framework can't guess.
2. **Stability**: hash function changes reshard all data; app control is more flexible than framework-baked.
3. **Debugging**: app layer can log `partition_id` for troubleshooting.

**Cost**: the app must keep partition key computation consistent — otherwise "same key maps to different partitions" split-data scenarios occur.

## 路由表失效场景 / Routing Table Invalidation Scenarios

| 事件 / Event | How ShardRouter Finds Out |
| :--- | :--- |
| Leader election completes | `RoutingTableChange::LeaderChanged` broadcast (if ConfigProvider wired to Raft events) |
| Membership change (add learner → promote) | `RoutingTableChange::GroupMembershipChanged` |
| New partition created | `RoutingTableChange::PartitionAdded` |
| Node address changes | `RoutingTableChange::NodeAddrUpdated` |
| Node removed (decommission) | `RoutingTableChange::NodeRemoved` |

如果 `ConfigProvider` 没有与 Raft 事件连接（例如 `MemConfigProvider` 默认不自动追踪 leader 变化），`ShardRouter` 可能返回**陈旧的 leader**。这种情况下：
- 客户端会收到 `ForwardToLeader`（老 leader 仍在本地——但不再是 leader），
- 或者老 leader 已 step down 并返回 `NotLeader` 错误，
- 最终自然收敛到正确的 leader，代价是几次额外 RPC。

---

If `ConfigProvider` isn't wired to Raft events (e.g., default `MemConfigProvider` does not auto-track leader changes), `ShardRouter` may return a **stale leader**. In that case:
- The client gets `ForwardToLeader` (the old leader locally — but not the real leader anymore),
- Or the former leader has stepped down and returns `NotLeader`,
- Eventually it converges on the real leader at the cost of a few extra RPCs.

## 相关 / Related

- ConfigProvider 与路由表变更：[dynamic-config-update.md](./dynamic-config-update.md)
- 客户端重定向：[dynamic-put-request.md](./dynamic-put-request.md)
- 路由组件细节：[components-router.md](./components-router.md)
