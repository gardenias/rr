---
sidebar_position: 2
---

# Protobuf 参考 / Protobuf Reference

rr 使用 Protocol Buffers 定义 gRPC 通信：`raft.proto` 承载节点间 Raft RPC，`client.proto` 承载客户端
请求与读取，`admin.proto` 承载运维管理。protobuf 的 `bytes` 字段传输不透明负载；Raft 内部负载使用
bincode，业务负载可选择应用自己的编码，框架另可能添加带版本的信封。

rr uses Protocol Buffers for gRPC: `raft.proto` carries inter-node Raft RPCs, `client.proto` carries
client requests and reads, and `admin.proto` carries administration. Protobuf `bytes` fields carry opaque
payloads. Raft internals use bincode; applications can choose their own payload encoding, while the
framework may add versioned envelopes.

## Raft 服务 / Raft Service

`RaftService` 用于内部共识；请求中的 `group_id` 在单节点多 Raft 组时用于分解。
`RaftService` provides internal consensus RPCs. `group_id` demultiplexes multiple Raft groups on one
node.

```protobuf
service RaftService {
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
    rpc Vote(VoteRequest) returns (VoteResponse);
    rpc InstallSnapshot(stream SnapshotChunk) returns (SnapshotResponse);
}
```

| 消息 / Message | 字段 / Fields | 说明 / Notes |
| :--- | :--- | :--- |
| `AppendEntriesRequest` / `AppendEntriesResponse` | `group_id` (`uint64`)<br/>`payload` (`bytes`) | 日志复制、心跳及响应；payload 为 bincode 编码的 openraft 数据。 / Replication, heartbeats, and responses; payload contains bincode-encoded openraft data. |
| `VoteRequest` / `VoteResponse` | `group_id` (`uint64`)<br/>`payload` (`bytes`) | Leader 选举及响应。 / Leader-election request and response. |
| `SnapshotChunk` | `group_id` (`uint64`)<br/>`offset` (`uint64`)<br/>`data` (`bytes`)<br/>`done` (`bool`)<br/>`metadata` (`bytes`) | 流式快照分片；`metadata` 仅在最后分片发送。 / Streaming snapshot chunk; `metadata` is sent only in the final chunk. |
| `SnapshotResponse` | `group_id` (`uint64`)<br/>`success` (`bool`)<br/>`payload` (`bytes`) | 快照安装响应。 / Snapshot-install response. |

## 客户端服务 / Client Service

`ClientService` 提供写入、本地读、线性一致读和拓扑查询。
`ClientService` provides writes, local reads, linearizable reads, and topology discovery.

```protobuf
service ClientService {
    rpc Request(ClientRequest) returns (ClientResponse);
    rpc ReadLocal(ReadLocalRequest) returns (ReadLocalResponse);
    rpc ReadLinearizable(ReadLocalRequest) returns (ReadLocalResponse);
    rpc GetClusterInfo(Empty) returns (ClusterInfo);
}
```

| 消息 / Message | 字段 / Fields | 说明 / Notes |
| :--- | :--- | :--- |
| `ClientRequest` | `partition_id` (`uint64`)<br/>`payload` (`bytes`)<br/>`optional IdempotencyKey idem_key` | `partition_id` 选择 Raft 组；`idem_key` 可选启用框架去重。 / `partition_id` selects the Raft group; optional `idem_key` enables framework deduplication. |
| `IdempotencyKey` | `request_type` (`string`)<br/>`request_id` (`uint64`)<br/>`optional string client_id` | 去重身份为 `(request_type, request_id)`；`client_id` 仅用于诊断。 / Dedup identity is `(request_type, request_id)`; `client_id` is diagnostic only. |
| `ClientResponse` | `success` (`bool`)<br/>`payload` (`bytes`)<br/>`LeaderInfo leader_info`<br/>`optional IdempotencyError idem_error`<br/>`optional NamespaceLimitError ns_limit_error` | 携带结果、Leader 重定向或类型化幂等拒绝。 / Carries the result, leader redirect, or typed idempotency rejection. |
| `LeaderInfo` | `node_id` (`uint64`)<br/>`addr` (`string`) | 客户端可缓存的 Leader 线索。 / Leader hint that clients may cache. |
| `ReadLocalRequest` | `partition_id` (`uint64`)<br/>`payload` (`bytes`) | 本地读与线性一致读共用的请求。 / Request shared by local and linearizable reads. |
| `ReadLocalResponse` | `payload` (`bytes`)<br/>`optional LeaderInfo leader_info` | `leader_info` 存在时必须重定向；只有未设置时 `payload` 才是读结果。 / A present `leader_info` requires redirect; `payload` is a read result only when it is absent. |
| `ClusterInfo` | `repeated GroupInfo groups` | 集群拓扑。 / Cluster topology. |
| `GroupInfo` | `group_id` (`uint64`)<br/>`leader_node_id` (`uint64`)<br/>`repeated uint64 replica_node_ids` | 组 Leader 与副本节点 ID。 / Group leader and replica node IDs. |

`IdempotencyError` 包含 `request_type`、`request_id`、`watermark`；`NamespaceLimitError` 包含
`request_type`、`request_id`、`namespaces`、`max`。两者均与有效 `payload` 互斥，且 `success=false`。
`IdempotencyError` contains `request_type`, `request_id`, and `watermark`; `NamespaceLimitError` contains
`request_type`, `request_id`, `namespaces`, and `max`. Both are mutually exclusive with meaningful
`payload` and set `success=false`.

## 管理服务 / Admin Service

节点状态、成员变更、快照、备份、路由与 formation 状态见 `admin.proto`；常用 RPC 的说明见
[Admin API 参考](admin-api.md)。
See `admin.proto` for node status, membership, snapshots, backup, routing, and formation status; common
RPCs are covered in the [Admin API reference](admin-api.md).

## 负载编码 / Payload Encoding

1. proto 层把业务数据与 Raft 内部状态视为不透明 `bytes`。
   The proto layer treats application data and internal Raft state as opaque `bytes`.
2. Raft 内部数据使用 bincode v2 `standard()`；应用可选择 JSON、Protobuf 或其他业务编码。
   Raft internals use bincode v2 `standard()`; applications may choose JSON, Protobuf, or another encoding.
3. 客户端原始字节可能由框架包装为 `FramedEntry` 或批处理帧，不再保证逐字节等同于 Raft 日志中的
   payload；格式与版本规则见[兼容性承诺](compatibility.md#字节格式版本约定--byte-format-versioning-policy)。
   Raw client bytes may be wrapped in a `FramedEntry` or batch frame, so the Raft log payload is not
   guaranteed to be byte-for-byte identical. See the
   [compatibility policy](compatibility.md#字节格式版本约定--byte-format-versioning-policy) for framing and
   versioning rules.

## 文件位置 / File Locations

```text
crates/rr-network/
├── proto/
│   ├── raft.proto      # 内部 Raft RPC / Internal Raft RPCs
│   ├── client.proto    # 客户端 RPC / Client RPCs
│   └── admin.proto     # 管理 RPC / Admin RPCs
└── build.rs            # prost 代码生成 / prost code generation
```
