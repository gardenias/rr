---
sidebar_position: 6
---

# 管理 API 参考 / Admin API Reference

`AdminService` 是 `rr-server` 节点的 Admin gRPC 服务。`NodeConfig.admin_port` 默认为 `None`，即服务禁用；
通过 `NodeConfig::with_admin_port(port)` 显式启用后，可使用 `rr-admin` 或任意 gRPC 客户端访问。
`examples/kv-store` 使用主 gRPC 端口 + 1000，这是该示例的端口约定，不是 `rr-server` 默认值。

`AdminService` is an `rr-server` node's admin gRPC service. `NodeConfig.admin_port` defaults to `None`,
leaving it disabled; enable it explicitly with `NodeConfig::with_admin_port(port)`, then use `rr-admin` or
any gRPC client. `examples/kv-store` uses the main gRPC port + 1000 as an example-specific convention, not
an `rr-server` default.

## 启用 Admin 端口 / Enabling the Admin Port

```rust
let config = NodeConfig::new(node_id, listen_addr, storage, config_provider, sm_factory)
    .with_admin_port(52001); // Admin gRPC 端口 / Admin gRPC port
```

完整服务与消息定义见 `crates/rr-network/proto/admin.proto`。
The complete service and message definitions live in `crates/rr-network/proto/admin.proto`.

## Cluster Admin Console 访问 / Cluster Admin Console Access

`rr-admin console` 是同一 `rr-admin` 二进制中的只读 BFF，也是 `AdminService` 的 gRPC 客户端。无 peers/discovery
启动时它不连接节点；operator 在 `/connections/` 保存并显式连接后，每个 runtime 使用自己的 TLS/mTLS、Admin token、
调度和缓存。浏览器不直接访问节点 Admin gRPC：控制面使用 `/api/v1/console/connections`，集群 JSON/SSE 使用
`/api/v1/console/connections/{id}/...`，其中 stream 为 `/api/v1/console/connections/{id}/stream`。
本地明文无认证模式仍受支持，节点 Admin token 只保留在 BFF 内。
`rr-admin console` is a read-only BFF in the same `rr-admin` binary and a gRPC client of `AdminService`. Starting without
peers/discovery contacts no node; after an operator saves and explicitly connects through `/connections/`, each runtime
uses its own TLS/mTLS, Admin token, scheduler, and cache. Browsers never access node Admin gRPC directly. The control API
uses `/api/v1/console/connections`, while cluster JSON/SSE uses `/api/v1/console/connections/{id}/...`, including
`/api/v1/console/connections/{id}/stream`. Local plaintext unauthenticated mode remains supported, and node Admin tokens
stay inside the BFF.

Console 不调用 `TriggerSnapshot`、`TransferLeader`、membership 或 `DrainNode` 等写 RPC；
`POST /api/v1/console/connections/{id}/sources/reconcile` 也只刷新该连接的只读采集。旧无作用域路由只绑定 legacy
临时启动连接，无该连接时返回 `CONNECTION_REQUIRED`，不会任意选择已保存集群。Admin 写 RPC 仍由 `rr-admin` CLI
或其他受权客户端执行。完整连接 CRUD、错误和 OpenAPI 位置见 [Cluster Admin Console](../operations/cluster-admin-console.md)。
The Console does not call write RPCs such as `TriggerSnapshot`, `TransferLeader`, membership changes, or `DrainNode`;
`POST /api/v1/console/connections/{id}/sources/reconcile` only refreshes that connection's read-only collection. Legacy
unscoped routes bind only to the ephemeral startup connection and return `CONNECTION_REQUIRED` without it, never choosing
an arbitrary saved cluster. Admin write RPCs remain with the `rr-admin` CLI or other authorized clients. See
[Cluster Admin Console](../operations/cluster-admin-console.md) for connection CRUD, errors, and the OpenAPI location.

## RPC 方法 / RPC Methods

### `GetNodeStatus(Empty) → NodeStatus`

返回节点 ID、监听地址、本地全部 Raft 组 ID 和运行时间。
Returns the node ID, listen address, local Raft group IDs, and uptime.

请求 / Request：`Empty {}`

响应字段 / Response fields：

| 字段 / Field | 类型 / Type | 描述 / Description |
| :--- | :--- | :--- |
| `node_id` | `uint64` | 节点 ID / Node ID |
| `listen_addr` | `string` | gRPC 监听地址 / gRPC listen address |
| `group_ids` | `repeated uint64` | 本地托管的 Raft 组 / Locally hosted Raft groups |
| `uptime_secs` | `uint64` | 运行秒数 / Uptime in seconds |

```bash
rr-admin node status --addr 127.0.0.1:52001
rr-admin --output json node status
```

### `GetGroupStatus(GroupId) → GroupStatus`

返回单个 Raft 组的 Leader 状态、任期、最后日志索引和已知 Leader ID。
Returns leadership state, current term, last log index, and the known leader ID for one Raft group.

请求 / Request：`GroupId { group_id: uint64 }`

响应字段 / Response fields：

| 字段 / Field | 类型 / Type | 描述 / Description |
| :--- | :--- | :--- |
| `group_id` | `uint64` | Raft 组 ID / Raft group ID |
| `is_leader` | `bool` | 当前节点是否为 Leader / Whether this node is leader |
| `current_term` | `uint64` | 当前 Raft 任期 / Current Raft term |
| `last_log_index` | `uint64` | 最后日志索引 / Last log index |
| `leader_node_id` | `uint64` | 已知 Leader ID；0 表示未知 / Known leader ID; 0 means unknown |

```bash
rr-admin group status --group 1
rr-admin --output json group status --group 1
```

### `TriggerSnapshot(GroupId) → Empty`

手动触发指定 Raft 组的快照与日志压缩，不中断正常写入。成功返回 `Empty`；失败通过 gRPC 状态返回，
没有 `success` 或 `message` 响应字段。

Manually triggers snapshotting and log compaction for a Raft group without interrupting normal writes.
Success returns `Empty`; failures are gRPC status errors. There are no `success` or `message` response
fields.

请求 / Request：`GroupId { group_id: uint64 }`

```bash
rr-admin group snapshot --group 1
```

### `TransferLeader(TransferLeaderRequest) → Empty`

请求**被调用节点**发起选举，成为指定 Raft 组的 Leader（`raft.trigger().elect(false)`）。
`target_node_id` 必须等于该节点自身的 node ID；远程转移会以 `FAILED_PRECONDITION` 拒绝。
要让节点 N 成为 Leader，应调用节点 N 的 Admin 端口；要迁出某节点的全部 Leader，使用 `DrainNode`，
它同样只作用于被调用节点。失败通过 gRPC 状态返回。

Asks the **called node** to start an election for the Raft group (`raft.trigger().elect(false)`).
`target_node_id` must equal that node's own ID; remote transfers are rejected with
`FAILED_PRECONDITION`. To make node N leader, call node N's admin port. To move all leadership off a
node, use `DrainNode`, which is also self-only. Failures are returned as gRPC status errors.

| 字段 / Field | 类型 / Type | 描述 / Description |
| :--- | :--- | :--- |
| `group_id` | `uint64` | Raft 组 ID / Raft group ID |
| `target_node_id` | `uint64` | 必须等于被调用节点 ID / Must equal the called node's ID |

```bash
rr-admin group transfer-leader --group 1 --to 3
```

### `GetMetricsSummary(Empty) → MetricsSummary`

:::warning 已弃用 / Deprecated
自 v1.0.0 起，此 RPC 在服务端返回 `UNIMPLEMENTED`；旧实现恒定返回零，不能作为真实指标使用。
请抓取 Prometheus `/metrics` 端点。消息和字段号为保持 wire 兼容性而保留，不得重用。

Since v1.0.0, the server returns `UNIMPLEMENTED`; the old implementation returned constant zeros and
was not a truthful metrics source. Scrape the Prometheus `/metrics` endpoint instead. The message and
field numbers remain reserved for wire compatibility and must not be reused.
:::

保留的响应 schema 如下 / The retained response schema is:

| 字段 / Field | 类型 / Type | 原用途 / Former purpose |
| :--- | :--- | :--- |
| `total_proposals` | `uint64` | 总提案数 / Total proposals |
| `total_applied` | `uint64` | 已应用日志条目数 / Applied log entries |
| `total_snapshots` | `uint64` | 已安装快照数 / Installed snapshots |
