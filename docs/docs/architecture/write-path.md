---
title: "完整写入链路 / Complete Write Path"
tags: [architecture, raft, write-path, performance]
up: "[[00-moc/architecture]]"
status: active
created: 2026-05-12
updated: 2026-09-03
---

# 完整写入链路

从 `send_request()` 到客户端收到响应，完整追踪每一个**进程边界**、**线程/Task 边界**、
**Raft 边界**、**业务边界**和**存储边界**。

---

## 进程与 Task 总览

```plantuml
@startuml write-path-overview
!include <C4/C4_Component>

title 写入链路进程与 Task 概览

Person(client, "应用客户端", "调用 RrClient SDK")

System_Boundary(node1, "Process B: rr-server Node 1（Leader）") {
    Component(grpc_task, "gRPC Task", "tonic", "接收 ClientRequest，等 oneshot 结果")
    Component(dispatch, "ClientDispatch\n+ ShardRouter", "Rust async", "partition_id → group_id 路由")
    Component(raft_core, "RaftCore Task\n[Raft 边界]", "openraft", "日志编号、提交、AppendEntries 协调")
    Component(io_batcher, "IoBatcher Task\n[Raft Log 边界]", "tokio task", "Group Commit:\n多提案共享单次 write_batch")
    Component(rocksdb, "RocksDB\nraft_log CF\n[存储边界]", "RocksDB", "Raft 日志持久化")
    Component(network_task, "RaftNetwork Task\n[Raft 边界]", "openraft", "向 Follower 发送 AppendEntries")
    Component(sm_worker, "SM Worker Task", "openraft", "驱动状态机 apply")
    Component(service, "Service::handle()\n[业务边界]", "用户代码", "唯一的业务逻辑入口")
}

System_Boundary(node2, "Process C: rr-server Node 2（Follower）") {
    Component(f2_grpc, "RaftService gRPC Task", "tonic", "接收 AppendEntries")
    Component(f2_core, "RaftCore Task\n[Raft 边界]", "openraft", "处理 AppendEntries，持久化")
    Component(f2_batcher, "IoBatcher Task\n[Raft Log 边界]", "tokio task", "Follower 也用 Group Commit")
    Component(f2_sm, "SM Worker Task\n[业务边界]", "openraft", "Follower 也执行 apply")
}

System_Boundary(node3, "Process D: rr-server Node 3（Follower）") {
    Component(f3_core, "RaftCore Task\n[Raft 边界]", "openraft", "同 Node 2")
}

Rel(client, grpc_task, "gRPC ClientRequest", "TCP")
Rel(grpc_task, dispatch, "submit_write()")
Rel(dispatch, raft_core, "raft.client_write()")
Rel(raft_core, io_batcher, "append(entries, callback)\n立即返回")
Rel(io_batcher, rocksdb, "write_batch() ±fdatasync")
Rel(raft_core, network_task, "触发 AppendEntries")
Rel(network_task, f2_grpc, "AppendEntries gRPC", "TCP")
Rel(network_task, f3_core, "AppendEntries gRPC", "TCP")
Rel(f2_grpc, f2_core, "handle()")
Rel(f2_core, f2_batcher, "append() 立即返回")
Rel(f2_core, network_task, "ACK", "TCP")
Rel(raft_core, sm_worker, "Apply(entries)")
Rel(sm_worker, service, "Service::handle(payload)")

@enduml
```

---

## 完整时序图

```plantuml
@startuml write-path-sequence
skinparam backgroundColor #FAFAFA
skinparam sequenceMessageAlign center
skinparam roundcorner 6
skinparam sequenceArrowThickness 1.5

title 完整写入链路时序（3-node Raft Group，IoBatcher 已启用）

box "进程 A：RrClient SDK" #E3F2FD
    participant "应用代码" as App
    participant "RrClient" as SDK
end box

box "进程 B：rr-server Node 1  [Leader]" #FFF8E1
    participant "gRPC Task" as GRPC
    participant "ClientDispatch\n+ShardRouter" as Dispatch
    participant "RaftCore Task\n[Raft]" as RaftCore #FFCCBC
    participant "IoBatcher Task\n[Raft Log]" as Batcher #DCEDC8
    participant "RocksDB\nraft_log CF\n[Storage]" as Rocks #C8E6C9
    participant "RaftNetwork\nTask [Raft]" as NetTask #FFCCBC
    participant "SM Worker\nTask" as SMWorker
    participant "Service::handle()\n[业务逻辑]" as Service #EDE7F6
end box

box "进程 C：Node 2  [Follower]" #F1F8E9
    participant "RaftService\ngRPC Task" as F2GRPC
    participant "RaftCore Task\n[Raft]" as F2Core #FFCCBC
    participant "IoBatcher\n[Raft Log]" as F2Batcher #DCEDC8
end box

box "进程 D：Node 3  [Follower]" #F1F8E9
    participant "RaftCore Task\n[Raft]" as F3Core #FFCCBC
end box

== 1. 客户端发送写入请求 ==

App -> SDK : send_request(group_id, payload)\n[blocking / await / reactive]
activate SDK

SDK -> GRPC : gRPC ClientRequest\n{ partition_id, payload }\n── 进程边界 TCP ──
activate GRPC

GRPC -> Dispatch : submit_write(partition_id, payload)
activate Dispatch

Dispatch -> Dispatch : shard_router.route()\n→ group_id = 1

Dispatch -> RaftCore : raft.client_write(FramedEntry)\n[channel send + 等 oneshot]
activate RaftCore

note right of Dispatch
**FramedEntry（框架层封装）**
version / offset / idem_key / payload
end note

== 2. RaftCore 分配 log_id + 异步 append ==

RaftCore -> RaftCore : 分配 log_id (term=T, index=N)

RaftCore -> Batcher : append(entries, callback)\n**立即返回，不等 fsync！**
note right of Batcher #DCEDC8
**IoBatcher Group Commit**

字节背压 / Byte-budget backpressure: submissions acquire byte permits
(`IoBatcherConfig::max_inflight_bytes`, default 64 MiB, keys + values)
before enqueueing; permits are released only after the covering flush
completes. Exhaustion back-pressures the Raft proposal pipeline itself,
bounding the accepted-but-unflushed tail in bytes (the entry-count
channel bound alone is payload-blind). `crates/rr-raft/src/io_batcher.rs`.


Phase 2: try_recv() 零等待 drain
Phase 3: 可选 window（默认 0，不等；第二提交提前冲刷）
Phase 4: 单次 write_batch()
Phase 5: 所有 callback 统一触发
→ N 个提案共享 1 次 fdatasync
end note

RaftCore -> NetTask : 触发 AppendEntries（不等 ACK）
activate NetTask

== 3. IoBatcher 写入 RocksDB ==

Batcher -> Rocks : write_batch(ops)\nStrict: +fdatasync\nReplicatedOnly: WAL only
activate Rocks
Rocks --> Batcher : ok
deactivate Rocks

Batcher --> RaftCore : callback.io_completed(Ok)\n[日志已落盘]

== 4. AppendEntries 发往 Follower ==

NetTask -> F2GRPC : AppendEntries gRPC\n── 进程边界 TCP ──
activate F2GRPC

NetTask -> F3Core : AppendEntries gRPC\n── 进程边界 TCP ──
activate F3Core

F2GRPC -> F2Core : handle_append_entries()
activate F2Core
F2Core -> F2Batcher : append(entries, callback)\n立即返回
F2Batcher -> F2Batcher : write_batch() ±fdatasync
F2Batcher --> F2Core : callback 触发

F2Core --> F2GRPC : response (success)
deactivate F2Core
F2GRPC --> NetTask : ACK ── 进程边界 ──
deactivate F2GRPC

F3Core --> NetTask : ACK ── 进程边界 ──
deactivate F3Core

== 5. 多数派 ACK → Leader 提交 ==

NetTask --> RaftCore : 收到多数派 ACK\n(self + Node2 + Node3 = 3/3)
deactivate NetTask

RaftCore -> RaftCore : commit_index 推进至 N
RaftCore -> SMWorker : Apply(entries[N])
activate SMWorker

== 6. 状态机 Apply  [业务边界] ==

SMWorker -> SMWorker : decode FramedEntry
SMWorker -> SMWorker : IdempotencyLayer::decide()\n→ New / Replay / Expired

SMWorker -> Service : **Service::handle(payload)**\n← 唯一业务代码入口
activate Service
Service --> SMWorker : response bytes
deactivate Service

SMWorker -> SMWorker : IdempotencyLayer::record()
SMWorker --> RaftCore : Responder oneshot 触发
deactivate SMWorker

note over F2Core #F1F8E9
Node 2 SM Worker 也 apply 同一条目
（异步执行，不阻塞 Leader 回复）
end note

== 7. 响应返回客户端 ==

RaftCore --> Dispatch : Ok(response_bytes)
deactivate RaftCore

Dispatch --> GRPC : ClientResponse
deactivate Dispatch

GRPC --> SDK : gRPC Response ── 进程边界 TCP ──
deactivate GRPC

SDK --> App : Result<Vec<u8>, RrError>
deactivate SDK

@enduml
```

---

## 关键边界说明

### 进程边界

三个独立 OS 进程，内存完全隔离，仅通过 TCP gRPC 通信。一个进程崩溃不影响其他进程。

### Raft 边界

`RaftCore Task` 和 `IoBatcher Task` 由 openraft 管理，应用代码无法注入逻辑。
Raft 协议保证：

| 保证 | 说明 |
|---|---|
| 持久性 | `client_write()` 返回前，条目已写入多数派磁盘 |
| 顺序性 | 所有节点按相同 index 顺序 apply |
| Leader 唯一性 | 同 term 内只有一个 Leader 接受写入 |

### 业务边界

`Service::handle(payload)` 是**唯一**的业务逻辑入口：

- 由 SM Worker Task 同步调用——**不要在此做阻塞 IO**，否则会拖慢整个 apply pipeline
- `&self` 接收者，内部状态需自行并发保护（如 `Arc<RwLock<HashMap>>`）
- **Leader 和所有 Follower 都执行**，且严格按相同顺序——用户代码必须是确定性的

### 存储边界

| 存储 | CF | 持久化策略 | 备注 |
|---|---|---|---|
| Raft 日志 | `raft_log` | Strict: fsync/batch；ReplicatedOnly: WAL only | IoBatcher 写入；snapshot 后被 purge |
| Raft 元数据 | `metadata` | 每次写 fsync | vote / last_purged（写入频率极低）|
| 业务快照 | 内存缓存 + rocks 模式落盘 `sm-snapshot.bin` | rocks 模式原子落盘（tmp→fsync→rename） | openraft 管理；用于 InstallSnapshot + 重启恢复；mem 模式仅内存 |

### IoBatcher Group Commit 效果

| 场景 | 每次 fdatasync 覆盖提案数 | TPS 上限 |
|---|---|---|
| 无 IoBatcher（旧） | 1 | ≈ 1 / fsync_latency |
| IoBatcher + 并发 N | N | ≈ N / fsync_latency（随并发线性增长）|

---

## 线程 / Task 一览

| Task | 数量 | 阻塞 OS 线程？ | 职责 |
|---|---|---|---|
| gRPC accept loop | 1/server | 否 | 接受 HTTP/2 连接 |
| gRPC request task | 1/request | 否（await oneshot） | 解码 ClientRequest，等待 Raft 提交 |
| **RaftCore Task** | 1/group | 否 | Raft 核心：log_id 分配、提交协调、apply 触发 |
| **IoBatcher Task** | 1/group | 否（RocksDB 写由 `RocksStorage` 内的 spawn_blocking 执行）| Group commit：积累多提案，单次 write_batch |
| RaftNetwork Task | 1/follower/group | 否 | 向每个 Follower 流式发送 AppendEntries |
| **SM Worker Task** | 1/group | 取决于 Service::handle | decode → 幂等 → Service::handle |
| metrics ticker | 1/group | 否 | 每 5s 发射 replication lag 指标 |
| config watch loop | 1/node | 否 | 监听 ConfigProvider 分区变化 |
| HTTP 观测 server | 1/node | 否 | `/health` `/ready` `/metrics` |
| admin gRPC server | 1/node | 否 | snapshot / membership / backup 等管理 RPC |

## Changelog

| Date | Change |
|---|---|
| 2026-05-12 | 初始版本，基于 session 架构深度分析 |
| 2026-09-03 | 修正存储边界表：业务快照仅保存在内存（不落盘），重启后状态机经日志重放恢复 |
