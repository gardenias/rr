---
sidebar_position: 2
---

# Protobuf 参考 / Protobuf Reference

## 概述 / Overview

rr 使用 Protocol Buffers (protobuf) 处理所有的 gRPC 通信。该框架包含两个主要的 proto 文件：`raft.proto` 用于内部 Raft 共识节点间通信，`client.proto` 用于面向客户端的 RPC 服务。

为了保持灵活性，二进制负载（如 Raft 日志条目和状态机数据）首先使用 bincode v2 进行序列化，然后封装在 protobuf 的 `bytes` 字段中。这种分层方法允许应用层在需要时选择不同的序列化格式，同时保持统一的网络传输层。

---

rr uses Protocol Buffers (protobuf) for all gRPC communication. The framework defines two primary proto files: `raft.proto` for internal Raft consensus between nodes, and `client.proto` for client-facing RPC services.

To maintain flexibility, binary payloads (such as Raft log entries and state machine data) are serialized with bincode v2 before being wrapped in protobuf `bytes` fields. This layered approach allows the application layer to choose different serialization formats if needed while maintaining a unified network transport layer.

## Raft 服务协议 / Raft Service Proto

`RaftService` 定义了节点间用于达成共识的内部 RPC。所有请求都包含一个 `group_id` 字段，用于在单节点多 Raft 实例的环境中进行多路分解（demultiplexing）。

---

`RaftService` defines the internal RPCs used between nodes to reach consensus. All requests include a `group_id` field to enable demultiplexing across multiple Raft instances on a single node.

### 服务定义 / Service Definition

```protobuf
service RaftService {
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
    rpc Vote(VoteRequest) returns (VoteResponse);
    rpc InstallSnapshot(stream SnapshotChunk) returns (SnapshotResponse);
}
```

### 核心消息 / Core Messages

| 消息 / Message | 字段 / Fields | 描述 / Description |
| :--- | :--- | :--- |
| **AppendEntriesRequest** | `group_id` (u64)<br/>`payload` (bytes) | 日志复制和心跳请求。`payload` 是 bincode 编码的 openraft 请求。 / Log replication and heartbeat. `payload` is bincode-encoded openraft request. |
| **VoteRequest** | `group_id` (u64)<br/>`payload` (bytes) | 领导者选举投票请求。 / Leader election vote request. |
| **SnapshotChunk** | `group_id` (u64)<br/>`offset` (u64)<br/>`data` (bytes)<br/>`done` (bool)<br/>`metadata` (bytes) | 流式快照传输分片。`metadata` 仅在最后一个分片中发送。 / Streaming snapshot chunk. `metadata` is only sent in the final chunk. |

## 客户端服务协议 / Client Service Proto

`ClientService` 是集群的对外接口，允许客户端提交指令并查询集群状态。

---

`ClientService` is the public interface to the cluster, allowing clients to submit commands and query cluster state.

### 服务定义 / Service Definition

```protobuf
service ClientService {
    rpc Request(ClientRequest) returns (ClientResponse);
    rpc GetClusterInfo(Empty) returns (ClusterInfo);
}
```

### 核心消息 / Core Messages

| 消息 / Message | 字段 / Fields | 描述 / Description |
| :--- | :--- | :--- |
| **ClientRequest** | `partition_id` (u64)<br/>`payload` (bytes) | 客户端提交的业务请求。`partition_id` 用于路由到对应的 Raft 组。 / Business request. `partition_id` routes to the correct Raft group. |
| **ClientResponse** | `success` (bool)<br/>`payload` (bytes)<br/>`leader_info` (msg) | 请求结果。如果请求发送到了非领导者节点，会返回领导者重定向信息。 / Request result. Includes leader redirect info if sent to a non-leader. |
| **ClusterInfo** | `groups` (repeated msg) | 集群拓扑信息，包括各组的领导者和副本节点 ID。 / Cluster topology, including leader and replica IDs for each group. |

## 负载编码 / Payload Encoding

rr 的设计原则是将共识层与数据序列化层解耦：

1. **不透明传输**：Proto 层将业务数据和 Raft 内部状态视为不透明的 `bytes`。
2. **应用灵活性**：虽然框架默认使用 bincode v2 (`standard()` 配置)，但您可以根据需要使用 JSON、Protobuf 或其他格式包装您的业务负载。
3. **日志项结构**：Raft 日志条目直接存储客户端发送的原始字节流。

---

rr is designed to decouple the consensus layer from the data serialization layer:

1. **Opaque Transport**: The Proto layer treats business data and internal Raft state as opaque `bytes`.
2. **Application Flexibility**: While the framework defaults to bincode v2 (with `standard()` config), you can wrap your business payloads in JSON, Protobuf, or any other format.
3. **Log Entry Structure**: Raft log entries store the raw byte streams exactly as received from the client.

## Proto 文件位置 / Proto File Locations

源代码树中相关文件的位置：

---

Location of relevant files in the source tree:

```text
crates/rr-network/
├── proto/
│   ├── raft.proto      # 内部 Raft 共识 RPC / Internal Raft consensus RPCs
│   └── client.proto    # 面向客户端的请求 RPC / Client-facing request RPCs
└── build.rs            # prost 代码生成配置 / prost codegen configuration
```
