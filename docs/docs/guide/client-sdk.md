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

你可以使用 `get_cluster_info()` 获取集群的拓扑信息。它会尝试连接引导地址，直到有一个节点响应。

---

You can use `get_cluster_info()` to retrieve cluster topology information. It attempts to connect to the bootstrap addresses until one node responds.

```rust
let cluster_info = client.get_cluster_info().await?;
println!("Nodes in cluster: {:?}", cluster_info.nodes);
```

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
