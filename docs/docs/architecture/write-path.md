---
title: "完整写入链路 / Complete Write Path"
tags: [architecture, raft, write-path, performance]
up: "[[00-moc/architecture]]"
status: active
created: 2026-05-12
updated: 2026-09-03
---

# 完整写入链路 / Complete Write Path

从 `send_request()` 到客户端响应，写入依次跨越进程、Tokio task、Raft、业务和存储边界。

From `send_request()` to the client response, a write crosses process, Tokio task, Raft, business-logic, and storage boundaries.

## 进程与 Task 总览 / Process and Task Overview

```plantuml
@startuml write-path-overview
!include <C4/C4_Component>

title Write-Path Process and Task Overview

Person(client, "Application client", "Calls the RrClient SDK")

System_Boundary(node1, "Process B: rr-server Node 1 (Leader)") {
    Component(grpc_task, "gRPC Task", "tonic", "Receives ClientRequest and awaits the oneshot result")
    Component(dispatch, "ClientDispatch\n+ ShardRouter", "Rust async", "Routes partition_id → group_id")
    Component(raft_core, "RaftCore Task\n[Raft boundary]", "openraft", "Assigns log IDs, commits entries, and coordinates AppendEntries")
    Component(io_batcher, "IoBatcher Task\n[Raft log boundary]", "tokio task", "Group Commit:\nMultiple proposals share one write_batch")
    Component(rocksdb, "RocksDB\nraft_log CF\n[Storage boundary]", "RocksDB", "Persists the Raft log")
    Component(network_task, "RaftNetwork Task\n[Raft boundary]", "openraft", "Sends AppendEntries to followers")
    Component(sm_worker, "SM Worker Task", "openraft", "Drives state-machine apply")
    Component(service, "Service::handle()\n[Business boundary]", "User code", "Sole business-logic entry point")
}

System_Boundary(node2, "Process C: rr-server Node 2 (Follower)") {
    Component(f2_grpc, "RaftService gRPC Task", "tonic", "Receives AppendEntries")
    Component(f2_core, "RaftCore Task\n[Raft boundary]", "openraft", "Handles and persists AppendEntries")
    Component(f2_batcher, "IoBatcher Task\n[Raft log boundary]", "tokio task", "Followers also use Group Commit")
    Component(f2_sm, "SM Worker Task\n[Business boundary]", "openraft", "Followers also execute apply")
}

System_Boundary(node3, "Process D: rr-server Node 3 (Follower)") {
    Component(f3_core, "RaftCore Task\n[Raft boundary]", "openraft", "Same as Node 2")
}

Rel(client, grpc_task, "gRPC ClientRequest", "TCP")
Rel(grpc_task, dispatch, "submit_write()")
Rel(dispatch, raft_core, "raft.client_write()")
Rel(raft_core, io_batcher, "append(entries, callback)\nReturns immediately")
Rel(io_batcher, rocksdb, "write_batch() ±fdatasync")
Rel(raft_core, network_task, "Triggers AppendEntries")
Rel(network_task, f2_grpc, "AppendEntries gRPC", "TCP")
Rel(network_task, f3_core, "AppendEntries gRPC", "TCP")
Rel(f2_grpc, f2_core, "handle()")
Rel(f2_core, f2_batcher, "append() returns immediately")
Rel(f2_core, network_task, "ACK", "TCP")
Rel(raft_core, sm_worker, "Apply(entries)")
Rel(sm_worker, service, "Service::handle(payload)")

@enduml
```

## 完整时序图 / Full Sequence

```plantuml
@startuml write-path-sequence
skinparam backgroundColor #FAFAFA
skinparam sequenceMessageAlign center
skinparam roundcorner 6
skinparam sequenceArrowThickness 1.5

title Complete Write-Path Sequence (3-node Raft Group, IoBatcher enabled)

box "Process A: RrClient SDK" #E3F2FD
    participant "Application code" as App
    participant "RrClient" as SDK
end box

box "Process B: rr-server Node 1  [Leader]" #FFF8E1
    participant "gRPC Task" as GRPC
    participant "ClientDispatch\n+ShardRouter" as Dispatch
    participant "RaftCore Task\n[Raft]" as RaftCore #FFCCBC
    participant "IoBatcher Task\n[Raft Log]" as Batcher #DCEDC8
    participant "RocksDB\nraft_log CF\n[Storage]" as Rocks #C8E6C9
    participant "RaftNetwork\nTask [Raft]" as NetTask #FFCCBC
    participant "SM Worker\nTask" as SMWorker
    participant "Service::handle()\n[Business logic]" as Service #EDE7F6
end box

box "Process C: Node 2  [Follower]" #F1F8E9
    participant "RaftService\ngRPC Task" as F2GRPC
    participant "RaftCore Task\n[Raft]" as F2Core #FFCCBC
    participant "IoBatcher\n[Raft Log]" as F2Batcher #DCEDC8
end box

box "Process D: Node 3  [Follower]" #F1F8E9
    participant "RaftCore Task\n[Raft]" as F3Core #FFCCBC
end box

== 1. Client sends a write request ==

App -> SDK : send_request(group_id, payload)\n[blocking / await / reactive]
activate SDK

SDK -> GRPC : gRPC ClientRequest\n{ partition_id, payload }\n── process boundary: TCP ──
activate GRPC

GRPC -> Dispatch : submit_write(partition_id, payload)
activate Dispatch

Dispatch -> Dispatch : shard_router.route()\n→ group_id = 1

Dispatch -> RaftCore : raft.client_write(FramedEntry)\n[channel send + await oneshot]
activate RaftCore

note right of Dispatch
**FramedEntry (framework envelope)**
version / offset / idem_key / payload
end note

== 2. RaftCore assigns log_id + async append ==

RaftCore -> RaftCore : Assign log_id (term=T, index=N)

RaftCore -> Batcher : append(entries, callback)\n**Returns immediately; does not await fsync**
note right of Batcher #DCEDC8
**IoBatcher Group Commit**

Byte-budget backpressure: submissions acquire byte permits
(`IoBatcherConfig::max_inflight_bytes`, default 64 MiB, keys + values)
before enqueueing; permits are released only after the covering flush
completes. Exhaustion back-pressures the Raft proposal pipeline itself,
bounding the accepted-but-unflushed tail in bytes (the entry-count
channel bound alone is payload-blind). `crates/rr-raft/src/io_batcher.rs`.

Phase 2: try_recv() drains without waiting
Phase 3: Optional window (default 0: no wait; a second submission flushes early)
Phase 4: One write_batch()
Phase 5: Trigger all callbacks together
→ N proposals share 1 fdatasync
end note

RaftCore -> NetTask : Trigger AppendEntries (without awaiting ACK)
activate NetTask

== 3. IoBatcher writes to RocksDB ==

Batcher -> Rocks : write_batch(ops)\nStrict: +fdatasync\nReplicatedOnly: WAL only
activate Rocks
Rocks --> Batcher : ok
deactivate Rocks

Batcher --> RaftCore : callback.io_completed(Ok)\n[Log persisted]

== 4. AppendEntries sent to followers ==

NetTask -> F2GRPC : AppendEntries gRPC\n── process boundary: TCP ──
activate F2GRPC

NetTask -> F3Core : AppendEntries gRPC\n── process boundary: TCP ──
activate F3Core

F2GRPC -> F2Core : handle_append_entries()
activate F2Core
F2Core -> F2Batcher : append(entries, callback)\nReturns immediately
F2Batcher -> F2Batcher : write_batch() ±fdatasync
F2Batcher --> F2Core : Trigger callback

F2Core --> F2GRPC : response (success)
deactivate F2Core
F2GRPC --> NetTask : ACK ── process boundary ──
deactivate F2GRPC

F3Core --> NetTask : ACK ── process boundary ──
deactivate F3Core

== 5. Majority ACK → leader commits ==

NetTask --> RaftCore : Majority ACK received\n(self + any follower = 2/3)
deactivate NetTask

RaftCore -> RaftCore : Advance commit_index to N
RaftCore -> SMWorker : Apply(entries[N])
activate SMWorker

== 6. State-machine apply [business boundary] ==

SMWorker -> SMWorker : decode FramedEntry
SMWorker -> SMWorker : IdempotencyLayer::decide()\n→ New / Replay / Expired

SMWorker -> Service : **Service::handle(payload)**\n← Sole business-code entry point
activate Service
Service --> SMWorker : response bytes
deactivate Service

SMWorker -> SMWorker : IdempotencyLayer::record()
SMWorker --> RaftCore : Trigger responder oneshot
deactivate SMWorker

note over F2Core #F1F8E9
Node 2 SM Worker also applies the same entry
(asynchronously, without blocking the leader response)
end note

== 7. Response returned to client ==

RaftCore --> Dispatch : Ok(response_bytes)
deactivate RaftCore

Dispatch --> GRPC : ClientResponse
deactivate Dispatch

GRPC --> SDK : gRPC Response ── process boundary: TCP ──
deactivate GRPC

SDK --> App : Result<Vec<u8>, RrError>
deactivate SDK

@enduml
```

## 关键边界 / Key Boundaries

### 进程边界 / Process Boundary

图中是三个独立 OS 进程，仅通过 TCP gRPC 通信。进程崩溃不会直接破坏其他进程的内存，但该组失去多数派时仍无法提交写入。

The diagram shows three independent OS processes communicating only over TCP gRPC. A crash does not directly damage another process's memory, but the group still cannot commit writes after losing quorum.

### Raft 边界 / Raft Boundary

`RaftCore` 和 SM Worker 由 openraft 驱动；`IoBatcher` 是 rr-raft 的日志存储组件。应用业务代码不能注入这些协议路径。

Openraft drives `RaftCore` and the SM Worker; `IoBatcher` belongs to rr-raft's log storage. Application business code cannot inject logic into these protocol paths.

| 保证 / Guarantee | 条件与含义 / Condition and Meaning |
| :--- | :--- |
| 持久性 / Durability | `Strict` 下，`client_write()` 返回 `Ok` 前，条目已在多数派完成日志持久化。<br />Under `Strict`, `client_write()` returns `Ok` only after the entry's log persistence completes on a majority. |
| 顺序性 / Ordering | 各节点按相同 log index 顺序 apply。<br />Every node applies entries in the same log-index order. |
| Leader 唯一性 / Leader uniqueness | 同一 term 只有一个 leader 接受写入。<br />Only one leader accepts writes within a term. |

### 业务边界 / Business Boundary

`Service::handle(payload)` 是业务逻辑入口。

`Service::handle(payload)` is the business-logic entry point.

- SM Worker 会等待其完成；不要执行阻塞 IO，以免阻塞 apply pipeline。<br />The SM Worker awaits it; do not perform blocking IO and stall the apply pipeline.
- 接收者是 `&self`；内部可变状态需要自行同步，例如 `Arc<RwLock<HashMap>>`。<br />The receiver is `&self`; synchronize interior mutable state, for example with `Arc<RwLock<HashMap>>`.
- leader 和 follower 都按相同顺序执行，因此实现必须确定。<br />Leaders and followers execute it in the same order, so implementations must be deterministic.

### 存储边界 / Storage Boundary

| 存储 / Data | 位置 / Location | 策略与备注 / Policy and Notes |
| :--- | :--- | :--- |
| Raft 日志 / Raft log | `raft_log` CF | `Strict` 按批 fsync；`ReplicatedOnly` 只写 WAL。由 IoBatcher 写入，快照后可 purge。<br />`Strict` fsyncs per batch; `ReplicatedOnly` writes WAL without sync. IoBatcher writes entries; snapshots allow purging. |
| Raft 元数据 / Raft metadata | `metadata` CF | vote 写入直接 fsync；保存 vote 和 `last_purged`。<br />Vote writes fsync directly; stores vote and `last_purged`. |
| 业务快照 / State-machine snapshot | 内存缓存；rocks 模式写入 `sm-snapshot.bin`。<br />Memory-cached; rocks mode writes `sm-snapshot.bin`. | rocks 模式按 tmp → fsync → rename 原子落盘，用于 InstallSnapshot 和重启恢复；mem 模式仅内存。<br />Memory-cached; rocks mode atomically writes tmp → fsync → rename for InstallSnapshot and restart recovery, while mem mode is memory-only. |

### IoBatcher 组提交 / IoBatcher Group Commit

| 场景 / Scenario | 每次 fdatasync 覆盖的提案 / Proposals per fdatasync | 理想化 TPS 上限 / Idealized TPS Ceiling |
| :--- | :--- | :--- |
| 无组提交 / No group commit | 1 | ≈ 1 / fsync_latency |
| 组提交，并发 N / Group commit with concurrency N | N | ≈ N / fsync_latency |

该模型只描述 fsync 摊销，不包含 CPU、网络、锁或 RocksDB 其他开销，不代表实测吞吐保证。

This model describes only fsync amortization. It excludes CPU, network, locking, and other RocksDB costs and is not a measured-throughput guarantee.

## 线程与 Task / Threads and Tasks

| Task | 数量 / Count | 阻塞 OS 线程 / Blocks OS Thread | 职责 / Responsibility |
| :--- | :--- | :--- | :--- |
| gRPC accept loop | 1/server | 否 / No | 接受 HTTP/2 连接。<br />Accept HTTP/2 connections. |
| gRPC request task | 1/request | 否；等待 oneshot / No; awaits oneshot | 解码 ClientRequest 并等待 Raft 提交。<br />Decode ClientRequest and await Raft commit. |
| `RaftCore` task | 1/group | 否 / No | 分配 log_id、协调提交并触发 apply。<br />Assign log IDs, coordinate commit, and trigger apply. |
| `IoBatcher` task | 1/group | 否；RocksDB 写由 `RocksStorage` 的 `spawn_blocking` 执行 / No; RocksDB writes use `RocksStorage` `spawn_blocking` | 聚合提案并执行单次 `write_batch`。<br />Coalesce proposals into one `write_batch`. |
| RaftNetwork task | 1/follower/group | 否 / No | 向每个 follower 复制 AppendEntries。<br />Replicate AppendEntries to each follower. |
| SM Worker task | 1/group | 取决于 `Service::handle` / Depends on `Service::handle` | decode → 幂等判断 → `Service::handle`。<br />Decode → idempotency decision → `Service::handle`. |
| metrics ticker | 1/group | 否 / No | 每 5 秒发射 replication lag 指标。<br />Emit replication-lag metrics every 5 seconds. |
| config watch loop | 1/node | 否 / No | 监听 ConfigProvider 分区变化。<br />Watch ConfigProvider partition changes. |
| HTTP observability server | 1/node | 否 / No | 提供 `/health`、`/ready`、`/metrics`。<br />Serve `/health`, `/ready`, and `/metrics`. |
| admin gRPC server | 1/node | 否 / No | 提供 snapshot、membership、backup 等管理 RPC。<br />Serve snapshot, membership, backup, and other admin RPCs. |
