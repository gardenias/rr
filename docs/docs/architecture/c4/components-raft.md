---
sidebar_position: 4
---

# Level 3 — rr-raft 组件图 / rr-raft Components

`rr-raft` 是 rr 的共识引擎。本图展示其如何在单个节点上同时运行多个独立的 Raft 组，并通过适配器把用户的 `Service` trait 挂到 openraft 的状态机接口上。

---

`rr-raft` is rr's consensus engine. This diagram shows how it runs multiple independent Raft groups on a single node, and how adapters bridge the user's `Service` trait to openraft's state machine interface.

## 图示 / Diagram

```plantuml
@startuml
!include <C4/C4_Component.puml>

title Component Diagram - rr-raft (Multi-Raft Engine)

Container_Ext(rr_server, "rr-server / RrNode", "Rust binary", "Owns & supervises the group manager")
Component_Ext(storage, "RocksDB (per-group instance)", "rr-storage (rocks feature)", "Opened by RocksLogStorage at {base_path}/{group_id}; not shared across groups")
Component_Ext(network, "Peer node RaftService", "served via rr-network-generated code", "Remote AppendEntries / Vote / InstallSnapshot")
Component_Ext(user_service, "User Service / StateMachine", "rr-core::Service trait impl", "Application-defined business logic")

Container_Boundary(rr_raft, "rr-raft") {
  Component(group_mgr, "RaftGroupManager", "rr-raft::group_manager", "Owns DashMap<GroupId, Arc<Mutex<GroupState>>>. Starts/stops groups, initializes clusters, adds learners, changes membership")

  Component(raft_node, "RaftNode", "rr-raft::raft_node", "Wraps openraft::Raft<RrTypeConfig> for one group. Holds references to log storage, state machine, network factory")
  Component(type_cfg, "RrTypeConfig", "rr-raft::type_config", "declare_raft_types! macro binding: NodeId=u64, Entry, Snapshot, D=bytes, R=bytes")

  Component(log_storage, "RrLogStorage", "rr-raft::log_storage", "In-memory implementation of openraft::RaftLogStorage (BTreeMap)")
  Component(rocks_log, "RocksLogStorage", "rr-raft::rocks_log_storage", "Persistent openraft::RaftLogStorage — per-group independent RocksDB instance at {base_path}/{group_id} (rocks feature)")
  Component(sm_adapter, "RrStateMachine", "rr-raft::sm_adapter", "Implements openraft::RaftStateMachine. Delegates apply() to ServiceStateMachineAdapter; owns snapshot creation via bincode")
  Component(svc_adapter, "ServiceStateMachineAdapter", "rr-raft::service_adapter", "Bridges Service trait (async handle / snapshot / restore) to rr's internal StateMachine trait")

  Component(net_factory, "GrpcNetworkFactory", "rr-raft::network_factory", "Implements openraft::RaftNetworkFactory. For each peer node, creates a GrpcRaftNetwork")
  Component(net_client, "GrpcRaftNetwork", "rr-raft::network_factory", "Implements openraft::RaftNetwork. Converts openraft RPC calls → bincode-serialized bytes → gRPC messages via ChannelPool")
}

Rel(rr_server, group_mgr, "Constructs + supervises")

Rel(group_mgr, raft_node, "start_group → new RaftNode", "1 per GroupId")
Rel(raft_node, type_cfg, "Parameterized by")
Rel(raft_node, log_storage, "Passes to openraft::Raft::new")
Rel(raft_node, rocks_log, "Alternative (rocks feature)")
Rel(raft_node, sm_adapter, "Passes to openraft::Raft::new")
Rel(raft_node, net_factory, "Passes to openraft::Raft::new")

Rel(sm_adapter, svc_adapter, "Delegates apply / snapshot / restore")
Rel(svc_adapter, user_service, "Invokes Service::handle / snapshot / restore")

Rel(log_storage, storage, "None — pure in-memory BTreeMap per group")
Rel(rocks_log, storage, "Opens its own RocksDB (raft_log / metadata CFs) + IoBatcher group commit")

Rel(net_factory, network, "Uses rr-raft's local ChannelPool")
Rel(net_client, network, "Sends bincode-in-proto frames")

LAYOUT_WITH_LEGEND()
@enduml
```

## 分层适配 / Adapter Layering

rr-raft 通过三层适配把**业务 `Service` trait**（用户写的）对接到 **openraft 的 `RaftStateMachine` trait**（共识引擎要求的）：

rr-raft uses three adapter layers to bridge the **business `Service` trait** (user-written) to openraft's **`RaftStateMachine` trait** (required by the consensus engine):

```
openraft::RaftStateMachine           ← 共识引擎接口 / what openraft needs
        │
        │ implemented by
        ▼
RrStateMachine                       ← rr 内部类型 / rr's internal wrapper
        │
        │ delegates to
        ▼
rr_core::state_machine::StateMachine ← rr 抽象 trait / rr's abstract contract
        │
        │ implemented by
        ▼
ServiceStateMachineAdapter           ← 桥 / the bridge
        │
        │ wraps
        ▼
rr_core::service::Service            ← 用户写的业务逻辑 / user's business logic
```

**为什么这么多层？/ Why all these layers?**

1. `Service` trait 面向应用开发者（`async handle(bytes) → bytes`），**与共识/日志无关**。
2. `StateMachine` trait 是 rr 内部中间层，支持未来添加更低层次的用户（例如用户绕过 Service 直接写日志）。
3. `RrStateMachine` 实现 openraft 要求的 `RaftStateMachine`（带快照元数据、last_applied 追踪等）。
4. 每层职责单一，允许**测试替身**与**特性切换**（`rocks` feature 只改变 LogStorage 层，不影响上层）。

---

1. The `Service` trait is for application developers (`async handle(bytes) → bytes`) — **consensus-agnostic**.
2. The `StateMachine` trait is rr's internal midlayer, allowing future lower-level users (e.g. bypass `Service` and write directly to the log).
3. `RrStateMachine` implements openraft's `RaftStateMachine` (snapshot metadata, last_applied tracking, etc.).
4. Single-responsibility layers enable **test doubles** and **feature toggles** (the `rocks` feature only replaces the LogStorage layer; nothing above changes).

## 多组的实现细节 / Multi-Group Implementation Details

### RaftGroupManager
- 内部：`DashMap<GroupId, Arc<Mutex<GroupState>>>` —— `GroupState` 持有 `RaftNode` 及该 group 的后台任务句柄（ingestor / output / metrics ticker / learner promoter）与在途写计数。
- `start_group(group_id)`：创建 `RaftNode`（启动一个新的 openraft 任务），注册到 DashMap。
- `stop_group(group_id)`：先停止 ingestor / output / metrics ticker / learner promoter 任务，排空在途写（5 秒上限），调用 `raft.shutdown()`，最后关闭并 flush Rocks 日志的 IoBatcher。**不会删除该 group 的任何日志数据**——日志保留在存储中供重启恢复。
- `init_cluster(group_id, members)` / `add_learner` / `change_membership`：转发到 `openraft::Raft` 的对应方法。

---

- Internal: `DashMap<GroupId, Arc<Mutex<GroupState>>>` — `GroupState` holds the `RaftNode` plus the group's background task handles (ingestor / output / metrics ticker / learner promoter) and an in-flight write counter.
- `start_group(group_id)`: creates a `RaftNode` (starts a new openraft task), registers in the DashMap.
- `stop_group(group_id)`: stops the ingestor / output / metrics ticker / learner promoter tasks, drains in-flight writes (5 s cap), calls `raft.shutdown()`, then closes + flushes the Rocks log's IoBatcher. It **never deletes the group's log data** — the log stays in storage for restart recovery.
- `init_cluster` / `add_learner` / `change_membership`: forwards to the corresponding `openraft::Raft` methods.

### 并发行为 / Concurrency Behavior
- **每个 group 有独立的 openraft 任务** —— 一个组的选举超时不会阻塞另一个组。
- **每 group 独立存储** —— 各 group 的日志互不相同：mem 模式下每个 group 一个独立的 `RrLogStorage`（内存 BTreeMap）；rocks 模式下每个 group 在 `{base_path}/{group_id}` 打开**独立的 RocksDB 实例**。不存在共享 StorageEngine + group_id 前缀的隔离方式。（`rr-server` 的 `NodeConfig.storage_engine` 字段目前没有任何消费方——Raft 日志不经过它。）
- **共享网络** —— 所有 group 的 Raft RPC 通过同一个 `ChannelPool` 和相同的 gRPC 端口；`group_id` 字段在 RPC level 做 demux。

---

- **Each group has an independent openraft task** — an election timeout in one group does not block another.
- **Per-group storage** — group logs are never mixed: in mem mode each group gets its own `RrLogStorage` (in-memory BTreeMap); in rocks mode each group opens an **independent RocksDB instance** at `{base_path}/{group_id}`. There is no shared-StorageEngine-with-group_id-prefix isolation. (rr-server's `NodeConfig.storage_engine` field currently has no consumer — Raft logs do not flow through it.)
- **Shared network** — all groups' Raft RPCs go through one `ChannelPool` and the same gRPC port; `group_id` field does RPC-level demux.

### 本地 proto 重定义 / Local proto redefinition

rr-raft 需要发送 Raft RPC，但它**不依赖** rr-network（实际依赖方向是 `rr-raft → rr-client → rr-network`，`rr-network` 仅依赖 `rr-core`，不存在循环）。作为替代，`rr-raft` 在 `network_factory.rs` 本地重新定义了与 `rr.raft.*` **prost 注解一致（wire-compatible）的 proto 消息结构**，并内联构建 tonic 客户端；openraft 类型的 bincode 编码只发生在消息的 `payload` 字节字段内部。连接复用也使用 rr-raft **本地定义的 `ChannelPool`**（注释原文："Functionally identical to rr_network::ChannelPool but defined locally"）。详见 [components-network.md](./components-network.md)。

---

rr-raft needs to send Raft RPCs, but it does **not** depend on rr-network (the actual dependency direction is `rr-raft → rr-client → rr-network`; `rr-network` depends only on `rr-core`, so there is no cycle). Instead, `rr-raft` locally redefines proto message structs in `network_factory.rs` with **matching prost annotations (wire-compatible with `rr.raft.*`)** and builds the tonic client inline; bincode encoding of openraft types happens only inside the messages' `payload` bytes field. Connection reuse likewise goes through rr-raft's **locally defined `ChannelPool`** (the code comment reads: "Functionally identical to rr_network::ChannelPool but defined locally"). See [components-network.md](./components-network.md).

## 相关 / Related

- 执行流：[dynamic-leader-election.md](./dynamic-leader-election.md), [dynamic-snapshot-transfer.md](./dynamic-snapshot-transfer.md)
- 存储细节：[../storage.md](../storage.md)
- 多 Raft 概念：[../multi-raft.md](../multi-raft.md)
