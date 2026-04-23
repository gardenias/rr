---
sidebar_position: 12
---

# Dynamic — Dynamic Config Update Propagation

rr 的一大特性是**运行时动态配置**：添加分片、迁移 group 成员、调整路由，都不需要重启集群。本图展示从运维触发变更到所有节点收敛的完整流程。

---

A key rr feature is **runtime dynamic configuration**: adding partitions, migrating group members, adjusting routing — all without cluster restart. This diagram shows the complete flow from operator trigger to cluster-wide convergence.

## 高层流程（C4 Dynamic）/ High-Level Flow (C4 Dynamic)

```mermaid
C4Dynamic
  title Dynamic Diagram - Routing Table Change Propagation

  Person(operator, "Operator")

  Container_Boundary(target_node, "Target Node (receives admin RPC)") {
    Component(admin_svc, "AdminService", "rr-network", "gRPC handler")
    Component(admin_hdl, "Admin Handlers", "rr-server::admin", "Routing-level + Raft-level orchestration")
    Component(cfg_local, "ConfigProvider (local)", "rr-config", "Broadcast-based change pub/sub")
    Component(router_t, "ShardRouter (local)", "rr-router", "Subscribed consumer")
    Component(raft_mgr_t, "RaftGroupManager", "rr-raft", "Manages group members")
  }

  Container_Ext(external_cfg, "External ConfigProvider", "etcd / custom", "Production-only source of truth")

  Container_Boundary(other_node, "Other Node") {
    Component(cfg_other, "ConfigProvider (peer)", "rr-config", "Receives watch events")
    Component(router_o, "ShardRouter (peer)", "rr-router", "Updates its cache")
    Component(raft_mgr_o, "RaftGroupManager (peer)", "rr-raft", "May start/stop groups")
  }

  Container_Ext(client, "rr-client SDK", "Rust library", "User application")

  Rel(operator, admin_svc, "1. rr-admin group add-learner --group=7 --node=4", "gRPC")
  Rel(admin_svc, admin_hdl, "2. dispatch")
  Rel(admin_hdl, cfg_local, "3. update_group_membership(7, [1,2,3,4])")

  Rel(cfg_local, external_cfg, "4a. Persist to external store (production path)")
  Rel(cfg_local, router_t, "4b. Broadcast RoutingTableChange")
  Rel(cfg_local, raft_mgr_t, "4c. RrNode watches changes → add_learner(7, 4)")

  Rel(external_cfg, cfg_other, "5. Subscribed peers receive update")
  Rel(cfg_other, router_o, "6. Broadcast to local subscribers")
  Rel(cfg_other, raft_mgr_o, "7. Node 4: start_group(7) as learner")

  Rel(client, router_o, "8. Next GetClusterInfo → sees new topology (via any node)")

  UpdateLayoutConfig($c4ShapeInRow="3", $c4BoundaryInRow="2")
```

## 详细时序图 / Detailed Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator
    participant CLI as rr-admin CLI
    participant N1 as Node 1 (target)
    participant Cfg1 as ConfigProvider<br/>(on Node 1)
    participant Ext as External<br/>ConfigProvider (etcd)
    participant Router1 as ShardRouter<br/>(on Node 1)
    participant RMgr1 as RaftGroupManager<br/>(on Node 1)
    participant Cfg4 as ConfigProvider<br/>(on new Node 4)
    participant RMgr4 as RaftGroupManager<br/>(on new Node 4)

    Op->>CLI: rr-admin group add-learner --group 7 --node 4 --addr host-4:51001
    CLI->>N1: gRPC AdminService.AddLearner(group=7, node_id=4, addr=...)

    N1->>Cfg1: add_node(4, "host-4:51001")
    N1->>Cfg1: update_group_membership(7, members=[1,2,3], learners=[4])

    par Persist & distribute
        Cfg1->>Ext: PUT /rr/routing_table {...}
        Ext-->>Cfg1: ack
    and Local broadcast
        Cfg1->>Router1: tx.send(RoutingTableChange::NodeAdded{4, ...})
        Cfg1->>Router1: tx.send(RoutingTableChange::GroupMembershipChanged{7, ...})
    end

    Router1->>Router1: ArcSwap::store(updated RoutingTable)

    Note over N1: N1 is Leader of group 7 → it's the one to execute the Raft membership change
    N1->>RMgr1: add_learner(group=7, node_id=4, addr=host-4:51001)
    RMgr1->>RMgr1: openraft::Raft::add_learner(4, node_4_info)

    Note over Ext: External store notifies all subscribers
    Ext->>Cfg4: Watch event: routing_table updated
    Cfg4->>Cfg4: Update internal state
    Cfg4->>RMgr4: (via RrNode watcher) start_group(7) if not already running
    RMgr4->>RMgr4: Create RaftNode for group 7<br/>(as learner — will catch up via AppendEntries / snapshot)

    Note over RMgr1,RMgr4: Normal Raft replication starts<br/>(may trigger snapshot transfer if log-gap too large)

    Op->>CLI: (later) rr-admin group promote-learner --group 7 --node 4
    CLI->>N1: gRPC AdminService.ChangeMembership(group=7, voters=[1,2,3,4])
    N1->>RMgr1: change_membership(7, [1,2,3,4])
    RMgr1->>RMgr1: openraft joint consensus<br/>(new config must be committed by both old and new quorums)

    Note over Cfg1,Ext: After joint consensus commits, update routing table
    Cfg1->>Ext: PUT /rr/routing_table (promote node 4 to voter)
    Ext->>Cfg4: Watch event
    Cfg4->>Router1: LeaderChanged broadcast etc.
```

## ConfigProvider 的两种形态 / Two Shapes of ConfigProvider

### 形态 1: MemConfigProvider（开发）/ Shape 1: MemConfigProvider (development)

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Node 1    │  │   Node 2    │  │   Node 3    │
│             │  │             │  │             │
│ MemCfgProv  │  │ MemCfgProv  │  │ MemCfgProv  │
│  (isolated) │  │  (isolated) │  │  (isolated) │
└─────────────┘  └─────────────┘  └─────────────┘
```

- 每个节点独立的内存状态
- **变更不会自动传播到其他节点**
- 使用场景：单节点开发、单元测试、integration test（测试 harness 主动调用所有节点）
- **不要在生产中使用**

Each node has isolated in-memory state. **Changes do NOT auto-propagate to other nodes.** Use cases: single-node development, unit tests, integration tests (test harness calls every node manually). **Do not use in production.**

### 形态 2: 外部存储 ConfigProvider（生产）/ Shape 2: External Store ConfigProvider (production)

```
┌──────────────────────────────────────────────┐
│             External Store (etcd)            │
│   ┌──────────────────────────────────────┐   │
│   │  Routing Table + Watch Channel       │   │
│   └──────────────────────────────────────┘   │
└──────────────────────────────────────────────┘
         ▲              ▲              ▲
         │              │              │
    ┌────┴────┐    ┌────┴────┐    ┌────┴────┐
    │ Node 1  │    │ Node 2  │    │ Node 3  │
    │  EtcdCP │    │  EtcdCP │    │  EtcdCP │
    └─────────┘    └─────────┘    └─────────┘
```

- 单一事实源在外部存储
- 每个节点本地 `ConfigProvider` 订阅 watch 事件
- **变更一次，全集群收敛**
- 使用场景：生产部署

Single source of truth lives in external store. Each node's local `ConfigProvider` subscribes to watch events. **Change once, whole cluster converges.** Use case: production deployment.

> **rr 本身没有内置 etcd 实现** — 用户需要实现 `ConfigProvider` trait。这是一个有意的设计选择，避免 rr 绑定到某个特定外部依赖。社区将来可能提供 `rr-config-etcd` 作为独立 crate。
>
> **rr does NOT ship an etcd implementation** — users implement `ConfigProvider` themselves. This is an intentional design choice to avoid binding rr to a specific external dependency. A community `rr-config-etcd` may appear as a separate crate.

## 两类变更：Raft-level vs Routing-level / Two Classes of Change

rr 的配置变更实际上分两层：

rr's configuration changes span two layers:

### Raft-level（共识层）
- `add_learner(group_id, node_id, addr)` — 新 learner 开始从日志追赶
- `change_membership(group_id, voters)` — joint consensus 切换 voter 集合

**这些变更通过 Raft 自身持久化** —— 写入每个成员的日志、提交后应用。不能"丢失"。

**These changes are persisted by Raft itself** — written to every member's log, applied after commit. Cannot be "lost".

### Routing-level（路由层）
- `add_partition(partition_id, group_id)` — 新分片映射
- `migrate_partition(partition_id, from_group, to_group)` — 迁移分片

**这些变更通过 `ConfigProvider` 持久化** —— 如果使用 MemConfigProvider 且只更新了单个节点，其他节点不会看到！

**These changes are persisted by `ConfigProvider`** — if using MemConfigProvider and only one node is updated, other nodes don't see it!

### 协调两者 / Coordinating Both

典型的"添加成员"操作需要：

A typical "add member" operation requires:

```
1. Routing level:  ConfigProvider.add_node(4, addr)         → nodes[4] = addr
2. Routing level:  ConfigProvider.update_group(7, ...)       → mark node 4 as learner in group 7
3. Raft level:     RaftGroupManager.add_learner(7, 4, addr)  → openraft knows about the new member
4. Wait for catchup via AppendEntries + snapshot
5. Raft level:     RaftGroupManager.change_membership(7, [1,2,3,4])  → promote to voter (joint consensus)
6. Routing level:  ConfigProvider.update_group(7, ..., leader_unchanged)  → reflect voter status
```

**顺序很重要 / Order matters**:

- **Step 3 必须在 step 1 和 2 之后**：Raft 需要路由表里已经有 node 4 的地址才能发送 AppendEntries
- **Step 5 必须在 step 4 catchup 完成后**：否则把一个远远落后的 learner 提升为 voter，会立刻影响 quorum
- Admin handlers 负责维护这个顺序 — 运维不需要手动协调

---

**Order matters**:

- **Step 3 must come after steps 1–2**: Raft needs node 4's address in the routing table before it can send AppendEntries.
- **Step 5 must come after step 4 catchup**: otherwise promoting a far-behind learner to voter immediately affects quorum.
- Admin handlers enforce this order — operators don't coordinate manually.

## 观察配置变更 / Observing Config Changes

```bash
# Dump current routing table on a node
rr-admin --addr host-1:52001 node status

# Watch for changes (if your ConfigProvider supports watch)
rr-admin --addr host-1:52001 config watch

# Verify all nodes have converged
for host in host-1 host-2 host-3; do
  rr-admin --addr $host:52001 node status --output json | jq '.routing_table.version'
done
# All should output the same version; if not, propagation is lagging
```

## 相关 / Related

- 分片路由的静态视图：[components-router.md](./components-router.md)
- 分片路由的动态解析：[dynamic-shard-routing.md](./dynamic-shard-routing.md)
- 多 Raft 架构概念：[../multi-raft.md](../multi-raft.md)
