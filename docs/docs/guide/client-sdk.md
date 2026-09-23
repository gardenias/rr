---
sidebar_position: 6
---

# 客户端 SDK / Client SDK

## 概述 / Overview

`RrClient` 为连接到 rr 集群提供了客户端 SDK。它位于 `rr-client` crate 中，负责处理领导者发现、领导者变更时的自动重试以及连接管理。

---

`RrClient` provides the client-side SDK for connecting to rr clusters. Located in the `rr-client` crate, it handles leader discovery, automatic retry on leader changes, and connection management.

## 连接到集群 / Connecting to a Cluster

你可以使用默认的重试配置或自定义配置来连接到集群。连接时需要提供一个或多个引导地址（bootstrap addresses）。

---

You can connect to a cluster using either the default retry configuration or a custom one. One or more bootstrap addresses must be provided during connection.

```rust
use rr_client::{RrClient, RetryConfig};

// 使用默认重试配置连接 / Connect with default retry config
let client = RrClient::connect(vec!["127.0.0.1:51001".to_string()]).await?;

// 使用自定义重试配置连接 / Connect with custom retry config
let client = RrClient::connect_with_config(
    vec!["127.0.0.1:51001".to_string()],
    RetryConfig { 
        max_retries: 5, 
        retry_delay_ms: 200 
    },
).await?;
```

## 重试配置 / RetryConfig

`RetryConfig` 结构体用于控制发生暂时性错误或领导者重定向时的重试行为。

---

The `RetryConfig` struct controls retry behavior during transient failures or leader redirects.

- `max_retries: u32` — 返回错误前的最大重试次数（默认值：3）。
- `retry_delay_ms: u64` — 重试之间的基础延迟，单位为毫秒。重试采用指数退避策略，每次尝试延迟会翻倍（默认值：100）。

---

- `max_retries: u32` — Maximum number of retry attempts before returning an error (default: 3).
- `retry_delay_ms: u64` — Base delay between retries in milliseconds. It uses an exponential backoff strategy, doubling the delay each attempt (default: 100).

## 发送请求 / Sending Requests

`RrClient` 提供了多种发送请求的方法。通常，你需要先将请求编码为字节向量（如使用 `bincode`）。

---

`RrClient` provides several methods for sending requests. Typically, you need to encode your request into a byte vector first (e.g., using `bincode`).

- `send_request(group_id, payload)` — 向特定的 Raft 组发送请求。
- `send_request_with_partition(partition_id, payload)` — 根据分区 ID 路由请求（目前与 `group_id` 1:1 映射）。

---

- `send_request(group_id, payload)` — Send a request to a specific Raft group.
- `send_request_with_partition(partition_id, payload)` — Route a request by partition ID (currently maps 1:1 to `group_id`).

### 代码示例 / Code Example

```rust
// 编码请求 / Encode request
let request = KvRequest::Put { key: "hello".to_string(), value: b"world".to_vec() };
let payload = bincode::serde::encode_to_vec(&request, bincode::config::standard())?;

// 发送到组 1 / Send to group 1
let response_bytes = client.send_request(1, payload).await?;

// 解码响应 / Decode response
let (response, _): (KvResponse, _) = bincode::serde::decode_from_slice(&response_bytes, bincode::config::standard())?;
```

### 读取路径 / Read Paths

除了写入用的 `send_request`，`RrClient` 还提供两种读取 API：

- `read(group_id, payload)` — 本地/过期读（stale read）：由任一副本直接应答，不经过 Raft 共识，延迟最低，但数据可能略落后于最新已提交的写入。
- `read_linearizable(group_id, payload)` — 线性一致读：服务端通过 Raft ReadIndex 确认提交进度后再应答；若打到 follower 会自动重定向到 leader 并重试。

---

Besides `send_request` for writes, `RrClient` offers two read APIs:

- `read(group_id, payload)` — stale/local read: served directly by any replica without Raft consensus; lowest latency, but the data may lag slightly behind the latest committed write.
- `read_linearizable(group_id, payload)` — linearizable read: the server confirms the commit index via Raft ReadIndex before answering; follower redirects to the leader are retried automatically.

## 写批处理 / Client Write Batching（opt-in）

`RrClientBuilder::with_batching(BatchConfig)` 开启 Kafka-producer 式写批处理（**默认关闭**）。
开启后，未带幂等键的 `send_request` 先按 partition 进入客户端缓冲，满足任一条件即整批
作为**一条 Raft entry** 提交：攒够 `max_entries` 条、或累计 `max_bytes` 字节、或首条入队后
等待满 `linger`。apply 侧把批帧逐条拆开调用 `Service::handle`，响应按序打包回客户端，
每个等待者拿到自己的响应。带幂等键的写（`send_request_idempotent`）始终绕过批处理。

```rust
let client = RrClientBuilder::new(addrs)
    .with_batching(rr_client::BatchConfig {
        max_entries: 64,                       // 批大小（条数）
        max_bytes: 1 << 20,                    // 批大小（字节）
        linger: std::time::Duration::from_millis(5), // 时间限制（linger.ms）
        max_buffered: 8192,                    // 缓冲上限——超限立即报错（背压）
    })
    .build()
    .await?;
```

- 收益（S2 k=200、128B、64 条/批、linger 1ms 实测）：**175.2k TPS vs ~29k（6×）**，p99 2.4ms。
- 代价：串行写要付 linger（S1 p50 161µs → ~3.1ms）。**延迟敏感的串行写场景请保持关闭**。
- 兼容性：需要服务端框架带批帧拆分（本版本起）；对旧服务端开启会在首个批次响亮报错
  （绝不假成功）。与 A-0033 deferred（两阶段）apply 的服务不兼容。
- 背压：缓冲总量超过 `max_buffered` 时 `send_request` 立即返回错误，由调用方降速/重试。

---

`RrClientBuilder::with_batching(BatchConfig)` enables Kafka-producer-style write batching
(**off by default**). Unkeyed `send_request` writes accumulate per partition and flush as
**one Raft entry** when `max_entries` / `max_bytes` is reached or `linger` elapses; the
apply side splits the batch frame and calls `Service::handle` per item, packing responses
positionally. Idempotency-keyed writes always bypass the batcher. Measured gain: 6×
throughput on concurrent writes; cost: serial writes pay the linger — keep it off for
latency-bound callers. Requires servers with batch-aware apply; incompatible with deferred
(A-0033) services; `max_buffered` caps memory with fail-fast backpressure.

## 自动领导者重定向 / Auto Leader Redirect

`RrClient` 实现了透明的领导者重定向逻辑：

1. 当客户端向非领导者节点发送请求时，节点会返回包含当前已知领导者信息的错误。
2. `RrClient` 捕获此错误，自动更新其内部的领导者缓存。
3. 客户端随后向真实的领导者重新发送请求。
4. 这种机制对调用者是完全透明的，你只需调用 `send_request()`。

---

`RrClient` implements transparent leader redirect logic:

1. When a client sends a request to a non-leader node, the node returns an error containing the current known leader's information.
2. `RrClient` catches this error and automatically updates its internal leader cache.
3. The client then retries the request to the actual leader.
4. This mechanism is completely transparent to the caller — you just use `send_request()`.

## 集群信息 / Cluster Info

你可以使用 `get_cluster_info()` 获取集群的组列表。它会尝试连接引导地址，直到有一个节点响应。

---

You can use `get_cluster_info()` to retrieve the cluster's group list. It attempts to connect to the bootstrap addresses until one node responds.

```rust
let cluster_info = client.get_cluster_info().await?;
println!("Groups in cluster: {:?}", cluster_info.groups);
```

:::info
`GetClusterInfo` 返回真实运行时拓扑（`leader_node_id` 来自 raft metrics，选主窗口期为 `0`——0 不是合法 NodeId；`replica_node_ids` 为完整成员集）。

`GetClusterInfo` returns real runtime topology (`leader_node_id` comes from raft metrics and is `0` mid-election — 0 is not a valid NodeId; `replica_node_ids` is the full membership).
:::

## 错误处理 / Error Handling

客户端在交互过程中可能会遇到以下常见的 `RrError` 变体：

---

Clients may encounter the following common `RrError` variants during interaction:

- `RrError::Network(NetworkError::ConnectionFailed)` — 无法连接到节点（如网络断开或地址错误）。
- `RrError::Network(NetworkError::RpcFailed)` — gRPC 调用失败。
- `RrError::Raft(RaftError::NotLeader)` — 目标不是领导者（通常由 `RrClient` 内部处理）。
- `RrError::Router(RouterError::PartitionNotFound)` — 路由表中找不到请求的分区 ID。

---

- `RrError::Network(NetworkError::ConnectionFailed)` — Failed to connect to a node (e.g., network down or wrong address).
- `RrError::Network(NetworkError::RpcFailed)` — gRPC call failed.
- `RrError::Raft(RaftError::NotLeader)` — Target is not the leader (usually handled internally by `RrClient`).
- `RrError::Router(RouterError::PartitionNotFound)` — Partition ID not found in the routing table.

---

## rr-admin repl 交互式输入 / rr-admin Interactive REPL

`rr-admin repl` 提供了一个交互式命令行界面，可直接对使用 kv-store wire 格式的集群进行临时测试和调试。
它支持 readline 历史（方向键 + Ctrl+R）和标准终端编辑功能。

---

`rr-admin repl` provides an interactive command-line interface for ad-hoc testing and debugging
clusters that use the kv-store wire format. It supports readline history (arrow keys, Ctrl+R)
and standard terminal editing.

### 启动 REPL / Launching the REPL

```bash
# 连接默认地址和组 / Connect to default address and group
rr-admin repl

# 指定地址和组 / Specify address and group
rr-admin repl --addr 127.0.0.1:52001 --group 1
```

### 支持的命令 / Supported Commands

| 命令 / Command | 说明 / Description |
| :--- | :--- |
| `get <key>` | 读取一个键的值 / Fetch a value by key |
| `put <key> <value>` | 写入一个键值对 / Write a key-value pair |
| `delete <key>` | 删除一个键 / Delete a key |
| `status` | 显示节点状态（通过 Admin RPC）/ Show node status (via Admin RPC) |
| `members` | 显示组成员信息（通过 Admin RPC）/ Show group membership (via Admin RPC) |
| `help` | 显示帮助信息 / Show help |

:::note
REPL 的 `get`/`put`/`delete` 命令使用 kv-store wire 格式（bincode 编码的 `KvRequest`/`KvResponse`）。
This means the REPL is specifically designed for services using the kv-store wire format.

`get`/`put`/`delete` use kv-store wire format (bincode-encoded `KvRequest`/`KvResponse`).
:::

