---
sidebar_position: 6
---

# 第 3 级：rr-network 组件 / Level 3: rr-network Components

`rr-network` 用 tonic/gRPC 承载 Client、Raft 和 Admin RPC。Raft 与 Client 在 `listen_addr` 上共享
`tonic::Server`；Admin 在配置 `admin_port` 时使用独立 server。

`rr-network` carries Client, Raft, and Admin RPCs over tonic/gRPC. Raft and Client share a `tonic::Server` on
`listen_addr`; Admin uses a separate server when `admin_port` is configured.

## 图示 / Diagram

```plantuml
@startuml
!include <C4/C4_Component.puml>

title Component Diagram - rr-network (gRPC Transport)

Container_Ext(rr_client, "rr-client SDK", "Rust library", "User application")
Container_Ext(rr_admin, "rr-admin CLI", "Rust binary", "Operator tool")
Container_Ext(peer_node, "Peer rr-server Node", "Rust binary", "Other Raft members")
Container_Ext(raft_mgr, "RaftGroupManager (local)", "rr-raft", "Holds all local groups")
Container_Ext(dispatch, "ClientDispatchImpl (local)", "rr-server", "ClientDispatch trait impl")
Container_Ext(admin_hdl, "Admin Handlers (local)", "rr-server", "AdminService trait impl")

Container_Boundary(rr_network, "rr-network") {
  Component(proto_raft, "raft.proto → RaftService", "tonic-generated", "AppendEntries, Vote, InstallSnapshot (streaming). Bytes field carries bincode-encoded openraft types")
  Component(proto_client, "client.proto → ClientService", "tonic-generated", "Request, ReadLocal, ReadLinearizable, GetClusterInfo")
  Component(proto_admin, "admin.proto → AdminService", "tonic-generated", "16 RPCs: GetNodeStatus, GetGroupStatus, ListGroups, GetSnapshotStatus, TriggerSnapshot, TransferLeader, DrainNode, AddLearner, ChangeMembership, RemoveGroupMember, ResetIngestorOffset, GetGroupMembership, OnlineBackup (streaming), GetRoutingTable, GetMetricsSummary, GetFormationStatus")

  Component(raft_svc_impl, "RaftServiceImpl", "rr-network::raft_service", "gRPC handler. Delegates to injected RaftGroupLookup trait")
  Component(client_svc_impl, "ClientServiceImpl", "rr-network::client_service", "gRPC handler. Delegates to injected ClientDispatch trait")

  Component(channel_pool, "ChannelPool", "rr-network::channel_pool", "DashMap<NodeId, (String, Channel)>. Lazy same-address reuse; callers evict on Code::Unavailable")

  Component(err_convert, "error_convert", "rr-network::error_convert", "IntoStatus trait: one-way Rr error types → tonic::Status, with info-leak hardening (used by rr-server's admin handlers)")
}

Rel(rr_client, client_svc_impl, "Client RPCs", "gRPC")
Rel(rr_admin, proto_admin, "Admin RPCs", "gRPC")
Rel(peer_node, raft_svc_impl, "Raft RPCs", "gRPC")

Rel(raft_svc_impl, raft_mgr, "Via RaftGroupLookup trait")
Rel(client_svc_impl, dispatch, "Via ClientDispatch trait")
Rel(proto_admin, admin_hdl, "Delegates to handlers")

Rel(channel_pool, peer_node, "TCP + HTTP/2 connection", "Lazy + reused")

Rel(admin_hdl, err_convert, "IntoStatus on error")

LAYOUT_WITH_LEGEND()
@enduml
```

## Protobuf 服务 / Protobuf Services

`crates/rr-network/proto/` 中的三个文件由 `tonic-build` 编译。

Three files under `crates/rr-network/proto/` are compiled by `tonic-build`.

### Raft 服务：`raft.proto` / Raft Service: `raft.proto`

节点间 Raft 共识 RPC。

Inter-node Raft consensus RPCs.

```proto
service RaftService {
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
  rpc Vote(VoteRequest) returns (VoteResponse);
  rpc InstallSnapshot(stream SnapshotChunk) returns (SnapshotResponse);
}
```

每个请求包含 `uint64 group_id` 和 `bytes payload`；`payload` 承载 bincode 编码的 openraft 类型，
因此 proto 不需要跟随 openraft 类型变化。

Every request contains `uint64 group_id` and `bytes payload`; the payload carries bincode-encoded openraft types,
so the proto need not change with those types.

### Client 服务：`client.proto` / Client Service: `client.proto`

客户端 SDK 的入口。

Entry point for the client SDK.

```proto
service ClientService {
  rpc Request(ClientRequest) returns (ClientResponse);
  rpc ReadLocal(ReadLocalRequest) returns (ReadLocalResponse);
  rpc ReadLinearizable(ReadLocalRequest) returns (ReadLocalResponse);
  rpc GetClusterInfo(Empty) returns (ClusterInfo);
}
```

`ClientResponse` 可携带 `LeaderInfo { node_id, addr }`，这是 `success=false` 普通响应中的重定向字段，
不是 gRPC `Status`。`GetClusterInfo` 返回 Raft 运行时拓扑（leader 来自 metrics，选举中可为 `0`），但
SDK 的 leader 发现依赖重定向，而不是轮询该 RPC。

`ClientResponse` can carry `LeaderInfo { node_id, addr }` as a normal `success=false` response field, not a gRPC
`Status`. `GetClusterInfo` reports Raft runtime topology (leader from metrics, possibly `0` during election), but
the SDK discovers leaders through redirects rather than polling this RPC.

### Admin 服务：`admin.proto` / Admin Service: `admin.proto`

运维 CLI 使用的管理接口。

The administration interface used by the operator CLI.

```proto
service AdminService {
  rpc GetNodeStatus(Empty) returns (NodeStatus);
  rpc GetGroupStatus(GroupId) returns (GroupStatus);
  rpc ListGroups(Empty) returns (GroupList);
  rpc GetSnapshotStatus(GroupId) returns (SnapshotStatus);
  rpc TriggerSnapshot(GroupId) returns (Empty);
  rpc TransferLeader(TransferLeaderRequest) returns (Empty);
  rpc DrainNode(DrainRequest) returns (DrainResult);
  rpc AddLearner(AddLearnerRequest) returns (Empty);
  rpc ChangeMembership(ChangeMembershipRequest) returns (Empty);
  rpc RemoveGroupMember(RemoveGroupMemberRequest) returns (Empty);
  rpc ResetIngestorOffset(ResetIngestorOffsetRequest) returns (Empty);
  rpc GetGroupMembership(GroupId) returns (GroupMembership);
  rpc OnlineBackup(OnlineBackupRequest) returns (stream OnlineBackupChunk);
  rpc GetRoutingTable(Empty) returns (RoutingTableSnapshot);
  rpc GetMetricsSummary(Empty) returns (MetricsSummary);
  rpc GetFormationStatus(Empty) returns (FormationStatus);
}
```

按 group 操作的请求类型名为 `GroupId`，不是 `GroupIdRequest`。

Per-group requests use the message type `GroupId`, not `GroupIdRequest`.

## 连接池 / ChannelPool

连接池按节点复用 tonic `Channel`，避免每次 RPC 都重建 TCP/TLS/HTTP/2 连接。

The pool reuses a tonic `Channel` per node instead of rebuilding TCP/TLS/HTTP/2 connections for every RPC.

```rust
struct ChannelPool {
    channels: Arc<DashMap<NodeId, (String, tonic::transport::Channel)>>,
    tls: Option<Arc<TlsConfig>>,
}

// Shared API: get_or_connect(node_id, addr), remove(node_id).
// with_tls(...) selects https:// URIs and configures client TLS.
```

- 首次调用 `get_or_connect` 时建立连接；缓存地址与传入 `addr` 不同时会驱逐旧 channel 并重连。
  The first `get_or_connect` establishes a channel; if the cached address differs from `addr`, the stale channel
  is evicted and redialed.
- 连接池不做健康探测。rr-raft 的 `GrpcRaftNetwork` 在 AppendEntries、Vote、InstallSnapshot 路径收到
  `Code::Unavailable` 时驱逐其本地 pool 中的 channel；rr-client 只驱逐 leader cache。
  The pool does no health probing. rr-raft's `GrpcRaftNetwork` evicts from its local pool on
  `Code::Unavailable` in AppendEntries, Vote, and InstallSnapshot; rr-client evicts only its leader cache.
- `DashMap` 按分片管理并发访问；单个 tonic `Channel` 可复用 HTTP/2 stream。
  `DashMap` shards concurrent access; one tonic `Channel` multiplexes HTTP/2 streams.

## 与 rr-raft 解耦 / Decoupling from rr-raft

`rr-network` 只依赖 `rr-core`，实际依赖方向为 `rr-raft → rr-client → rr-network`，没有 crate 循环。

`rr-network` depends only on `rr-core`; the actual direction is `rr-raft → rr-client → rr-network`, with no crate
cycle.

1. 入站：`rr-network` 定义 `RaftGroupLookup`，rr-server 用 `RaftGroupLookupImpl` 实现；
   `RaftServiceImpl` 只依赖 trait。
   Inbound: `rr-network` defines `RaftGroupLookup`, rr-server implements `RaftGroupLookupImpl`, and
   `RaftServiceImpl` depends only on the trait.
2. 出站：rr-raft 在 `network_factory.rs` 本地定义具有相同 prost 注解的消息、构建 tonic 客户端，
   并使用本地 `ChannelPool`。两处 proto 字段必须保持 wire-compatible。
   Outbound: rr-raft locally defines messages with matching prost annotations in `network_factory.rs`, builds
   tonic clients, and uses its own `ChannelPool`. Both proto definitions must remain wire-compatible.

## 错误转换 / Error Conversion

`error_convert.rs` 的 `IntoStatus` 将 Rr 错误单向转换为 `tonic::Status`。Config、Network 等分支会把
peer 地址、文件路径和库内部细节留在服务端日志；具体响应内容以各错误分支为准。

`error_convert.rs` provides one-way conversion from Rr errors to `tonic::Status` through `IntoStatus`. Config,
Network, and other hardened branches keep peer addresses, file paths, and library internals in server logs; exact
response content depends on the error branch.

```text
RrError / RaftError / StorageError / ConfigError / RouterError / NetworkError / CodecError
        │  IntoStatus::into_status()
        ▼
tonic::Status (gRPC wire)
```

leader 重定向不经过 `Status`：`ClientDispatchImpl` 将 openraft `ForwardToLeader` 写入
`ClientResponse.leader_info`。`error_convert` 只处理真正的错误，例如 `RaftError::NotLeader` 映射为
`Status::unavailable`。

Leader redirects bypass `Status`: `ClientDispatchImpl` translates openraft `ForwardToLeader` into
`ClientResponse.leader_info`. `error_convert` handles genuine errors, such as `RaftError::NotLeader` mapping to
`Status::unavailable`.

## 相关 / Related

- [rr-server 装配 / rr-server wiring](./components-server.md)
- [客户端重定向 / Client redirects](./dynamic-put-request.md)
- [快照流 / Snapshot streaming](./dynamic-snapshot-transfer.md)
- [网络文档 / Networking guide](../networking.md)
