---
sidebar_position: 6
---

# Admin API 参考 / Admin API Reference

`AdminService` 是一个 gRPC 服务，运行在 `rr-server` 节点的 Admin 端口（默认为主 gRPC 端口 + 1000）。
通过 `NodeConfig::with_admin_port(port)` 启用。可使用 `rr-admin` CLI 或任何 gRPC 客户端访问。

---

`AdminService` is a gRPC service running on the admin port of an `rr-server` node
(default: main gRPC port + 1000). Enable it via `NodeConfig::with_admin_port(port)`.
Accessible via the `rr-admin` CLI or any gRPC client.

## 启用 Admin 端口 / Enabling the Admin Port

```rust
let config = NodeConfig::new(node_id, listen_addr, storage, config_provider, sm_factory)
    .with_admin_port(52001); // Admin gRPC 端口 / Admin gRPC port
```

## RPC 方法 / RPC Methods

### `GetNodeStatus(Empty) → NodeStatus`

返回节点级别的状态信息：节点 ID、监听地址、所有 Raft 组 ID 和节点运行时间。

---

Returns node-level status: node ID, listen address, all Raft group IDs, and node uptime.

**请求 / Request**: `Empty {}`

**响应字段 / Response Fields**:

| 字段 / Field | 类型 / Type | 描述 / Description |
| :--- | :--- | :--- |
| `node_id` | `uint64` | 节点唯一标识符 / Unique node identifier |
| `listen_addr` | `string` | gRPC 监听地址 / gRPC listen address |
| `group_ids` | `repeated uint64` | 该节点托管的 Raft 组 ID 列表 / Raft group IDs hosted on this node |
| `uptime_secs` | `uint64` | 节点运行时间（秒） / Node uptime in seconds |

**rr-admin 用法 / rr-admin usage**:

```bash
rr-admin node status --addr 127.0.0.1:52001
rr-admin --output json node status
```

---

### `GetGroupStatus(GroupId) → GroupStatus`

返回单个 Raft 组的状态信息：是否为 Leader、当前任期、最新日志索引和 Leader 节点 ID。

---

Returns status for a single Raft group: whether this node is leader, current term,
last log index, and leader node ID.

**请求 / Request**: `GroupId { group_id: uint64 }`

**响应字段 / Response Fields**:

| 字段 / Field | 类型 / Type | 描述 / Description |
| :--- | :--- | :--- |
| `group_id` | `uint64` | Raft 组 ID / Raft group ID |
| `is_leader` | `bool` | 当前节点是否为该组的 Leader / Whether this node is leader for the group |
| `current_term` | `uint64` | 当前 Raft 任期 / Current Raft term |
| `last_log_index` | `uint64` | 最新日志条目索引 / Index of the last log entry |
| `leader_node_id` | `uint64` | 当前已知的 Leader 节点 ID（0 表示未知） / Current known leader node ID (0 = unknown) |

**rr-admin 用法 / rr-admin usage**:

```bash
rr-admin group status --group 1
rr-admin --output json group status --group 1
```

---

### `TriggerSnapshot(GroupId) → TriggerSnapshotResponse`

手动触发指定 Raft 组的日志压缩（快照）操作。不影响正常写入流程。

---

Manually triggers log compaction (snapshot) for the specified Raft group.
Does not interrupt normal write operations.

**请求 / Request**: `GroupId { group_id: uint64 }`

**响应字段 / Response Fields**:

| 字段 / Field | 类型 / Type | 描述 / Description |
| :--- | :--- | :--- |
| `success` | `bool` | 快照是否成功触发 / Whether the snapshot was triggered successfully |
| `message` | `string` | 状态消息或错误信息 / Status message or error description |

**rr-admin 用法 / rr-admin usage**:

```bash
rr-admin group snapshot --group 1
```

---

### `TransferLeader(TransferLeaderRequest) → TransferLeaderResponse`

请求将指定 Raft 组的 Leader 转移到目标节点。当前节点必须是该组的 Leader。

---

Requests leadership transfer of the specified Raft group to a target node.
The current node must be the leader of the group.

**请求字段 / Request Fields**:

| 字段 / Field | 类型 / Type | 描述 / Description |
| :--- | :--- | :--- |
| `group_id` | `uint64` | Raft 组 ID / Raft group ID |
| `target_node_id` | `uint64` | 目标 Leader 的节点 ID / Target node ID for leader transfer |

**响应字段 / Response Fields**:

| 字段 / Field | 类型 / Type | 描述 / Description |
| :--- | :--- | :--- |
| `success` | `bool` | 转移是否成功发起 / Whether the transfer was initiated successfully |
| `message` | `string` | 状态消息或错误信息 / Status message or error description |

**rr-admin 用法 / rr-admin usage**:

```bash
rr-admin group transfer-leader --group 1 --to 3
```

---

### `GetMetricsSummary(Empty) → MetricsSummary`

返回顶层指标摘要：总提案数、总应用数和总快照安装数。

---

Returns a top-level metrics snapshot: total proposals, total applied entries,
and total snapshot installs.

**请求 / Request**: `Empty {}`

**响应字段 / Response Fields**:

| 字段 / Field | 类型 / Type | 描述 / Description |
| :--- | :--- | :--- |
| `total_proposals` | `uint64` | 自节点启动以来的总提案数 / Total proposals since node start |
| `total_applied` | `uint64` | 已应用的日志条目总数 / Total log entries applied |
| `total_snapshots` | `uint64` | 已安装的快照总数 / Total snapshots installed |

**rr-admin 用法 / rr-admin usage**:

```bash
rr-admin metrics
rr-admin --output json metrics
```
