---
sidebar_position: 6
---

# 客户端 SDK / Client SDK

Rust `rr-client` 和 `sdks/java` 中的 Java SDK 使用同一 gRPC 协议，负责 leader 发现、重试和连接管理。
Rust `rr-client` and the Java SDK under `sdks/java` share the gRPC protocol and handle leader discovery, retries, and connection management.

## Rust 连接与重试 / Rust Connection and Retries

提供一个或多个引导地址。默认重试 3 次、基础延迟 100 ms，采用带抖动的指数退避。
Provide one or more bootstrap addresses. Defaults are three retries and a 100 ms base delay with jittered exponential backoff.

```rust
use rr_client::{RetryConfig, RrClient};

let client = RrClient::connect(vec!["127.0.0.1:51001".to_string()]).await?;

let client = RrClient::connect_with_config(
    vec!["127.0.0.1:51001".to_string()],
    RetryConfig {
        max_retries: 5,
        retry_delay_ms: 200,
    },
).await?;
```

TLS 与 mTLS、bearer token、严格安全检查、单次尝试超时和连接扇出通过 `RrClientBuilder` 配置。bearer token 应与 TLS 一起使用。
Configure TLS/mTLS, bearer authentication, strict security, per-attempt timeout, and channel fanout through `RrClientBuilder`. Use bearer tokens together with TLS.

## 请求与读取 / Requests and Reads

负载是服务定义的字节格式。`kv-store` 使用 bincode v2：
Payloads use the service-defined byte format. `kv-store` uses bincode v2:

```rust
let request = KvRequest::Put {
    key: "hello".to_string(),
    value: b"world".to_vec(),
};
let payload = bincode::serde::encode_to_vec(&request, bincode::config::standard())?;
let response = client.send_request(1, payload).await?;
let (response, _): (KvResponse, _) =
    bincode::serde::decode_from_slice(&response, bincode::config::standard())?;
```

- `send_request(group_id, payload)`：向指定组写入。<br />Write to a specific group.
- `send_request_with_partition(partition_id, payload)`：当前实现直接把 `partition_id` 作为 `group_id` 调用 `send_request`，不查询分区映射；通用 `RoutingTable` 仍允许多个分区映射到同一组。<br />The current implementation passes `partition_id` directly as `group_id` to `send_request` without a partition lookup; the general `RoutingTable` still allows multiple partitions to map to one group.
- `send_request_idempotent(group_id, payload, key)`：携带幂等键，重试时由框架去重。<br />Attach a key for framework deduplication across retries.
- `read(group_id, payload)`：本地读，可能过期。<br />Local, potentially stale read.
- `read_linearizable(group_id, payload)`：通过 ReadIndex 线性一致读。<br />Linearizable read through ReadIndex.

写或线性一致读落到 follower 时，服务端返回 leader 提示而不转发；客户端更新缓存并重试。最终拒绝或重试耗尽会向调用方返回错误。
When a write or linearizable read reaches a follower, the server returns a leader hint instead of proxying. The client updates its cache and retries; final rejection or exhausted retries returns an error.

```rust
let cluster_info = client.get_cluster_info().await?;
println!("Groups: {:?}", cluster_info.groups);
```

`GetClusterInfo` 返回运行时拓扑；选举期间 `leader_node_id` 为 0。正常写路由使用 `leader_info`，不依赖该查询。
`GetClusterInfo` returns runtime topology, with `leader_node_id = 0` during elections. Normal write routing uses `leader_info`, not this query.

## 客户端写批处理 / Client Write Batching

:::warning 未发布特性 / Unreleased Feature
该 API 不在 v0.2.x 中，计划随 v0.3.0 提供且默认关闭。只批处理无幂等键的写；批次重试可能重复应用，需要恰好一次效果时使用 `send_request_idempotent`，它会绕过批处理。
This API is absent from v0.2.x, planned for v0.3.0, and off by default. It batches only unkeyed writes, which may be applied again after a retry. Use `send_request_idempotent` for exactly-once apply; it bypasses batching.
:::

`RrClientBuilder::with_batching(BatchConfig)` 按分区缓冲写，在条数、字节数或 linger 达到阈值时将整批作为一条 Raft entry 提交。apply 侧逐条调用 `Service::handle` 并按序返回响应。
`RrClientBuilder::with_batching(BatchConfig)` buffers writes per partition and commits a batch as one Raft entry when its entry count, byte size, or linger threshold is reached. Apply calls `Service::handle` for each item and returns responses in order.

```rust
let client = RrClientBuilder::new(addrs)
    .with_batching(rr_client::BatchConfig {
        max_entries: 64,
        max_bytes: 1 << 20,
        linger: std::time::Duration::from_millis(5),
        max_buffered: 8192,
    })
    .build()
    .await?;
```

缓冲超过 `max_buffered` 时立即报错，由调用方降速或稍后重试。串行低延迟写应保持关闭。服务端必须支持批帧拆分；旧服务端和 A-0033 deferred apply 服务不兼容。
Exceeding `max_buffered` fails immediately so the caller can slow down or retry. Keep batching off for latency-sensitive serial writes. Servers must support batch-frame splitting; older servers and A-0033 deferred-apply services are incompatible.

## 常见错误 / Common Errors

| 错误 / Error | 含义与处理 / Meaning and Action |
| :--- | :--- |
| `NetworkError::ConnectionFailed` | 地址或网络不可用；检查端点后重试 / Address or network unavailable; check endpoints and retry |
| `NetworkError::RpcFailed` | gRPC 调用失败 / gRPC call failed |
| `NetworkError::RpcRejected` | 确定性拒绝，保留 gRPC code；修正认证或请求 / Deterministic rejection with gRPC code; fix authentication or request |
| `RaftError::NotLeader` | 非 leader；通常由 SDK 重定向 / Not leader; normally redirected by the SDK |
| `RouterError::PartitionNotFound` | 路由表没有该分区 / Partition absent from the routing table |
| `RrError::IdempotencyExpired` | 幂等响应已淘汰且 ID 低于水位；分配新 ID / Cached response evicted and ID expired; allocate a fresh ID |

## Java SDK 安装 / Java SDK Installation

Java SDK 需要 JDK 17+。构件为 `io.github.gardenias:rr-client:0.2.0`，发布在 GitHub Packages 而非 Maven Central，读取需要 GitHub 凭证。
The Java SDK requires JDK 17+. Artifact `io.github.gardenias:rr-client:0.2.0` is published to GitHub Packages, not Maven Central, and requires GitHub credentials to read.

```kotlin
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/gardenias/rr")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("io.github.gardenias:rr-client:0.2.0")
}
```

```xml
<dependency>
  <groupId>io.github.gardenias</groupId>
  <artifactId>rr-client</artifactId>
  <version>0.2.0</version>
</dependency>
```

也可从源码发布到 Maven Local，并在消费方启用 `mavenLocal()`：
Alternatively, publish from source to Maven Local and enable `mavenLocal()` in the consumer:

```bash
just java-publish-local
```

## Java 快速上手 / Java Quick Start

SDK 传输不透明字节；以下负载必须与目标服务的协议一致。
The SDK transports opaque bytes; these payloads must match the target service's protocol.

```java
import io.github.gardenias.rr.client.RrClient;
import java.nio.charset.StandardCharsets;
import java.util.List;

RrClient client = RrClient.connect(List.of("127.0.0.1:51001")).join();
byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
byte[] response = client.sendRequest(1L, payload).join();
byte[] value = client.readLinearizable(1L, payload).join();
client.close();
```

响应式方法返回 `CompletableFuture`，另有 `sendRequestSync`、`readSync`、`readLinearizableSync` 和 `getClusterInfoSync` 阻塞变体。TLS、认证、超时和重试通过 `RrClientBuilder` 配置；幂等写使用 `sendRequestIdempotent(groupId, payload, IdempotencyKey)`。
Reactive methods return `CompletableFuture`, with `sendRequestSync`, `readSync`, `readLinearizableSync`, and `getClusterInfoSync` blocking variants. Configure TLS, authentication, timeouts, and retries through `RrClientBuilder`; use `sendRequestIdempotent(groupId, payload, IdempotencyKey)` for keyed writes.

完整示例见 [Java SDK README](https://github.com/gardenias/rr/blob/main/sdks/java/README.md) 和 [响应式使用手册](https://github.com/gardenias/rr/blob/main/sdks/java/docs/reactive-guide.md)。
See the [Java SDK README](https://github.com/gardenias/rr/blob/main/sdks/java/README.md) and [reactive guide](https://github.com/gardenias/rr/blob/main/sdks/java/docs/reactive-guide.md) for complete examples.

## 能力对照 / Capability Comparison

| 能力 / Capability | Rust | Java |
| :--- | :--- | :--- |
| leader 发现与重定向 / Leader discovery and redirect | `RrClient` | `RrClient` |
| 超时、TLS、认证 / Timeout, TLS, authentication | `RrClientBuilder` | `RrClientBuilder` |
| 幂等写 / Idempotent writes | `send_request_idempotent` | `sendRequestIdempotent` |
| 本地读与线性一致读 / Local and linearizable reads | `read`, `read_linearizable` | `read`, `readLinearizable` |
| 异步与阻塞 API / Async and blocking APIs | async, `*_blocking`, `*_reactive` | `CompletableFuture`, `*Sync` |
| 指标 / Metrics | `metrics` crate | Micrometer `rr.client.*` |
| 写批处理 / Write batching | `with_batching`，未发布 / `with_batching`, unreleased | 不支持 / Not available |
| 连接扇出 / Channel fanout | `with_channel_fanout` | 每地址一条 channel / One channel per address |

Java 的 `CompletableFuture` 并发组合不是写合并；不要把响应式手册中的批量请求等同于 Rust 写批处理。
Java `CompletableFuture` composition is not write coalescing; concurrent requests in the reactive guide are not equivalent to Rust write batching.

## rr-admin REPL / rr-admin REPL

REPL 直接测试使用 kv-store bincode wire 格式的服务。默认客户端地址是 `127.0.0.1:51001`，可显式指定地址和组：
The REPL tests services using the kv-store bincode wire format. Its default client address is `127.0.0.1:51001`; address and group can be set explicitly:

```bash
rr-admin repl
rr-admin repl --addr 127.0.0.1:51001 --group 1
```

| 命令 / Command | 行为 / Behavior |
| :--- | :--- |
| `get <key>` | 读取键 / Read a key |
| `put <key> <value>` | 写入键值 / Write a key-value pair |
| `delete <key>` | 删除键 / Delete a key |
| `status` | 通过 Admin RPC 查询状态 / Query status through Admin RPC |
| `members` | 通过 Admin RPC 查询成员 / Query membership through Admin RPC |
| `help` | 显示帮助 / Show help |

KV 命令使用客户端端口，不是 Admin 端口。REPL 只有收到 `success=true` 才打印 `OK`；其他服务若没有相同 wire 格式，不能使用这些 KV 命令。
KV commands use the client port, not the Admin port. The REPL prints `OK` only after `success=true`; services without the same wire format cannot use these KV commands.
