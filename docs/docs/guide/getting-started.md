---
sidebar_position: 1
---

# 快速入门 / Getting Started

本指南将通过一个简单的键值存储（KV Store）示例，引导你快速了解如何使用 `rr` 框架构建分布式应用。

This guide will walk you through building a simple distributed Key-Value store to show how to use the `rr` framework.

---

## 前置要求 / Prerequisites

在开始之前，请确保你的开发环境满足以下要求：

Before starting, ensure your environment meets these requirements:

- **Rust toolchain**: stable, 1.98+
- **Protocol Buffer compiler (`protoc`)**:
  - macOS: `brew install protobuf`
  - Ubuntu: `apt install protobuf-compiler`
- (可选 / Optional) **RocksDB dependencies**: `llvm` and `clang` (仅在使用 `rocks` 特性时需要 / Only if using the `rocks` feature)

---

## 添加 rr 到项目 / Adding rr to Your Project

目前 `rr` 尚未发布到 crates.io，你需要通过路径依赖将其添加到 `Cargo.toml` 中：

Since `rr` is not yet on crates.io, add it to your `Cargo.toml` via path dependencies:

```toml
[dependencies]
rr-core = { path = "../rr/crates/rr-core" }
rr-server = { path = "../rr/crates/rr-server" }
rr-client = { path = "../rr/crates/rr-client" }
rr-raft = { path = "../rr/crates/rr-raft" }
rr-config = { path = "../rr/crates/rr-config" }
rr-storage = { path = "../rr/crates/rr-storage" }
serde = { version = "1.0", features = ["derive"] }
async-trait = "0.1"
tokio = { version = "1.0", features = ["full"] }
bincode = { version = "2.0.0", features = ["serde"] }
```

---

## 步骤 1: 定义请求/响应类型 / Step 1: Define Request/Response Types

首先，定义你的应用将处理的请求和响应消息。它们必须实现 `Serialize` 和 `Deserialize`。

First, define the request and response messages for your application. They must implement `Serialize` and `Deserialize`.

```rust
use serde::{Deserialize, Serialize};

/// KV 存储操作请求 / A KV store operation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvRequest {
    /// 插入或更新键值对 / Insert or update a key.
    Put { key: String, value: Vec<u8> },
    /// 获取键对应的值 / Retrieve a key.
    Get { key: String },
    /// 删除键 / Delete a key.
    Delete { key: String },
}

/// KV 存储操作响应 / The result of a KV store operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvResponse {
    pub value: Option<Vec<u8>>,
}
```

---

## 步骤 2: 实现 Service Trait / Step 2: Implement the Service Trait

`Service` 是 `rr` 中的核心抽象，定义了如何处理业务逻辑以及如何进行状态快照。

The `Service` trait is the core abstraction in `rr`, defining business logic and state snapshotting.

```rust
use async_trait::async_trait;
use bincode::config::standard;
use rr_core::error::{CodecError, RrError};
use rr_core::service::Service;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct KvService {
    store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl Service for KvService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        let (req, _): (KvRequest, _) = bincode::serde::decode_from_slice(&request, standard())
            .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;

        let response = match req {
            KvRequest::Put { key, value } => {
                self.store.write().await.insert(key, value);
                KvResponse { value: None }
            }
            KvRequest::Get { key } => {
                let guard = self.store.read().await;
                KvResponse { value: guard.get(&key).cloned() }
            }
            KvRequest::Delete { key } => {
                self.store.write().await.remove(&key);
                KvResponse { value: None }
            }
        };

        let encoded = bincode::serde::encode_to_vec(&response, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))?;
        Ok(encoded)
    }

    fn name(&self) -> &str { "kv-store" }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let guard = self.store.read().await;
        bincode::serde::encode_to_vec(&*guard, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        let (restored, _): (HashMap<String, Vec<u8>>, _) =
            bincode::serde::decode_from_slice(data, standard())
                .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;
        *self.store.write().await = restored;
        Ok(())
    }
}
```

---

## 步骤 3: 配置并启动节点 / Step 3: Configure and Start a Node

使用 `NodeConfig` 和 `RrNode` 来初始化并启动你的分布式节点。

Use `NodeConfig` and `RrNode` to initialize and start your distributed node.

```rust
use rr_server::node::RrNode;
use rr_server::config::NodeConfig;
use rr_storage::mem::MemStorage;
use rr_config::mem::MemConfigProvider;
use rr_raft::service_adapter::ServiceStateMachineAdapter;
use rr_core::state_machine::StateMachine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = 1;
    let listen_addr = "127.0.0.1:51001".to_string();
    let storage = Arc::new(MemStorage::new());
    let config_provider = Arc::new(MemConfigProvider::new());
    
    // 将 Service 包装为状态机 / Wrap Service as a StateMachine
    // 返回类型标注不可省：闭包须产出 trait 对象 / the return-type
    // annotation is required so the closure yields the trait object
    let sm_factory = Box::new(|group_id| -> Box<dyn StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(group_id, KvService::default()))
    });

    let config = NodeConfig::new(
        node_id,
        listen_addr,
        storage,
        config_provider,
        sm_factory,
    );

    let node = RrNode::new(config)?;
    node.start().await?;

    // 保持运行 / Keep running
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

---

## 运行示例 / Running the Examples

以下四条路径均已在本机实测通过，建议按顺序尝试。

All four run paths below are verified against live runs — try them in order.

### 运行 1：零配置演示 / Run 1: Zero-Config Demo

```bash
git clone <repo>
cd rr
cargo run -p kv-store
```

无参数运行时，`kv-store` 在进程内初始化一个单副本 Raft 组，自选举为 leader，依次执行一组 KV 操作后自动退出（不启动 gRPC 服务，约 0.2s）。

With no arguments, `kv-store` initializes a single-replica Raft group in-process, elects itself leader, runs a set of KV operations, then exits (no gRPC server, ~0.2s).

预期输出（裁剪后）/ Expected output (trimmed):

```text
INFO === rr KV-Store demo (single-node, no gRPC) ===
INFO PUT hello = "world"
INFO GET hello = "world"
INFO PUT counter = "1"
INFO DEL hello
INFO GET hello = <not found>
INFO GET counter = "1"
INFO === rr KV-Store demo complete ===
```

成功标准：以退出码 0 结束，最后一行为 `=== rr KV-Store demo complete ===`。

Success criterion: exits with code 0, ending with `=== rr KV-Store demo complete ===`.

### 运行 2：单节点服务器 + REPL 客户端 / Run 2: Single-Node Server + REPL Client

```bash
# 终端 1：引导一个常驻 gRPC 节点（阻塞运行，Ctrl-C 停止）
# Terminal 1: bootstrap a long-running gRPC node (blocks until Ctrl-C)
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap
# 日志出现 "Cluster bootstrapped on node 1" 即就绪
# ready when the log shows "Cluster bootstrapped on node 1"

# 终端 2：用 rr-admin REPL 作为 KV 客户端连接
# Terminal 2: connect the rr-admin REPL as a KV client
cargo run -p rr-admin -- repl --addr 127.0.0.1:51001 --group 1
```

实测会话 / Verified session:

```text
> put alpha one
OK
> get alpha
one
> delete alpha
OK
> get alpha
(nil)
```

:::warning
REPL 的 KV 操作（`put`/`get`/`delete`）必须指向 `kv-store` 的监听端口 **51001**（KvService 状态机，真实的 KV 语义）。管理命令在 **Admin 端口**上：每个 kv-store 节点在 `port + 1000`（本例 52001）提供 AdminService，例如 `cargo run -p rr-admin -- --addr 127.0.0.1:52001 group status --group 1`。

For KV operations (`put`/`get`/`delete`), the REPL must target kv-store's listen port **51001** (KvService state machine, real KV semantics). Admin commands live on the **admin port**: every kv-store node serves AdminService on `port + 1000` (52001 here) — e.g. `cargo run -p rr-admin -- --addr 127.0.0.1:52001 group status --group 1`.
:::

:::note
想直接看多节点共识？跳到运行 3 —— `kv-store` 支持真实的三节点集群（`--peers` 携带全量 `id@addr` 花名册）。

Want to jump straight to multi-node consensus? See Run 3 — `kv-store` supports a real three-node cluster (`--peers` carries a full `id@addr` roster).
:::

### 运行 3：三节点 kv-store 共识集群 / Run 3: Three-Node kv-store Consensus Cluster

`kv-store` 示例可以组成真实的多节点共识集群（实测通过，含故障转移）。`--peers` 携带全量 `id@addr` 花名册（必须包含本节点），三个终端使用同一份花名册：

The `kv-store` example forms a real multi-node consensus cluster (verified, including failover). `--peers` carries the full `id@addr` roster (which must include the node itself), identical across all three terminals:

```bash
# 终端 1 — 引导节点 / Terminal 1 — bootstrap node
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003

# 终端 2 / Terminal 2
cargo run -p kv-store -- --node-id 2 --port 51002 --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003

# 终端 3 / Terminal 3
cargo run -p kv-store -- --node-id 3 --port 51003 --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003
```

引导节点日志出现 `Cluster bootstrapped on node 1 — voters: [1, 2, 3]`；其余节点的 Raft 组保持未初始化状态，由 leader 通过日志复制自动接管。每个节点的 Admin 端口为 `port + 1000`（即 52001/52002/52003）。验证：

The bootstrap node logs `Cluster bootstrapped on node 1 — voters: [1, 2, 3]`; the other nodes' Raft groups stay uninitialized until the leader takes over via log replication. Each node's admin port is `port + 1000` (52001/52002/52003). Verify:

```bash
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group status --group 1      # leader_node_id=1
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group membership --group 1  # Voters: 1,2,3

# 在 leader 的监听端口上做 KV 读写 / KV reads & writes on the leader's listen port
cargo run -p rr-admin -- repl --addr 127.0.0.1:51001 --group 1
# rr> put k1 v1   → OK
# rr> get k1      → v1
# 之后三个节点 group status 的 last_log_index 同步推进 / then last_log_index advances on all three
```

故障转移（实测）：Ctrl-C 掉 leader，几秒内幸存节点选出新 leader（term 递增）；向新 leader 的监听端口写入成功，已提交数据保留。

Failover (verified): Ctrl-C the leader — within seconds the survivors elect a new one (term increments); writes to the new leader's listen port succeed and committed data survives.

:::warning
写入必须发往 **leader** 的监听端口。现行 REPL 会在收到 `leader_info` 时自动跟随一次重定向（打印 `redirected to leader node N at ...`），且只在服务端返回 `success=true` 时打印 `OK`；被拒绝的写会打印 `Error: request rejected ...`。复制进度仍以各节点 `group status` 的 `last_log_index` 为准。

Writes must go to the **leader's** listen port. The current REPL automatically follows one `leader_info` redirect (printing `redirected to leader node N at ...`) and prints `OK` only when the server answers `success=true`; rejected writes print `Error: request rejected ...`. Verify replication via `last_log_index` in each node's `group status` as before.
:::

### 运行 4：stock `rr-server` 三节点集群 / Run 4: Three-Node Cluster with Stock `rr-server`

这是实测通过的多节点路径，使用 stock `rr-server` 与 `rr-admin` 二进制。

This is the verified multi-node path, using the stock `rr-server` and `rr-admin` binaries.

```bash
# 终端 1 — 节点 1（引导）/ Terminal 1 — node 1 (bootstrap)
cargo run -p rr-server -- serve --node-id 1 --listen-addr 127.0.0.1:51001 --admin-port 52001 --bootstrap --allow-insecure

# 终端 2 — 节点 2 / Terminal 2 — node 2
cargo run -p rr-server -- serve --node-id 2 --listen-addr 127.0.0.1:51002 --admin-port 52002 --peers 127.0.0.1:51001 --allow-insecure

# 终端 3 — 节点 3 / Terminal 3 — node 3
cargo run -p rr-server -- serve --node-id 3 --listen-addr 127.0.0.1:51003 --admin-port 52003 --peers 127.0.0.1:51001,127.0.0.1:51002 --allow-insecure
```

然后通过节点 1 的 **Admin 端口**（52001）组建 Raft 组：

Then form the Raft group via node 1's **admin port** (52001):

```bash
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group add-learner --group 1 --node-id 2 --addr 127.0.0.1:51002
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group add-learner --group 1 --node-id 3 --addr 127.0.0.1:51003
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group change-membership --group 1 --voters 1,2,3
```

三条命令均返回 `success=true`。随后在每个节点的 Admin 端口上执行 `group status`，三个节点结果一致（`leader_node_id=1`，`last_log_index` 相同）：

All three commands return `success=true`. `group status` against each node's admin port then agrees across all three (`leader_node_id=1`, same `last_log_index`):

```bash
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group status --group 1
cargo run -p rr-admin -- --addr 127.0.0.1:52002 group status --group 1
cargo run -p rr-admin -- --addr 127.0.0.1:52003 group status --group 1
```

向 leader 的监听端口（51001）写入后，三个节点的 `last_log_index` 同步推进，即复制成功。

After a write to the leader's listen port (51001), `last_log_index` advances on all three nodes — replication is working.

:::warning 必读注意事项 / Must-read caveats
以下均经实测验证 / All verified against live runs:

- **`--peers` 与共识组建无关**：它只填充本地路由表（peer 的节点 ID 按列表顺序猜测，见 `crates/rr-server/src/main.rs:339-353`）；Raft 组实际由上面的 `add-learner` + `change-membership` 命令组建。
- **写请求必须发往 leader 的监听端口**（51001）：stock `ClientDispatchImpl` 不会把请求转发给 leader；发往 follower 时 REPL 会跟随一次 `leader_info` 重定向，仍失败则打印 `Error: request rejected ...`（拒绝原因见 `crates/rr-admin/src/main.rs:1339` 的 `kv_failure_detail`）。
- **REPL 的 `OK` 以服务端 `success=true` 为准**（`crates/rr-admin/src/main.rs:1297` 的 `kv_call`），不再出现"假 OK"；但复制进度仍应以每个节点 `group status` 的 `last_log_index` 验证。
- **stock `rr-server` 是冒烟服务**：内置 EchoStateMachine，`get <key>` 会回显 key 本身（即使该 key 从未写入），不是 KV 语义。
- **端口职责分离**：AdminService 只在 `--admin-port` 上提供；ClientService 只在 `--listen-addr` 上提供。
- **`GetClusterInfo` 返回 raft 运行时拓扑**：group id、leader（选举中为 0）、副本列表均来自 raft `metrics`（`crates/rr-server/src/dispatch.rs`）。逐 group 的更详细状态仍可用 Admin 端口上的 `group status` / `group membership`。

- **`--peers` is irrelevant to consensus formation**: it only seeds the local routing table (peer node IDs are guessed by list index — see `crates/rr-server/src/main.rs:339-353`); the Raft group is actually formed by the `add-learner` + `change-membership` commands above.
- **Client writes must go to the leader's listen port** (51001): the stock `ClientDispatchImpl` does not forward to the leader — the REPL follows one `leader_info` redirect, then prints `Error: request rejected ...` if the write is still refused (see `kv_failure_detail` at `crates/rr-admin/src/main.rs:1339`).
- **The REPL's `OK` now requires the server's `success=true`** (`kv_call` at `crates/rr-admin/src/main.rs:1297`) — fake OKs are gone; still verify replication via `last_log_index` in `group status` on every node.
- **Stock `rr-server` is a smoke service**: the built-in EchoStateMachine makes `get <key>` echo the key itself (even for never-written keys) — not KV semantics.
- **Ports have separate roles**: AdminService is served only on `--admin-port`; ClientService only on `--listen-addr`.
- **`GetClusterInfo` returns raft-runtime topology**: group ids, leader (0 mid-election), and replica lists are all sourced from raft `metrics` (`crates/rr-server/src/dispatch.rs`). For per-group detail use `group status` / `group membership` on the admin port.
:::

:::note 配置文件方式 / Config-file variant
单节点 stock 服务器也可以用配置文件启动，但必须额外加两个 flag：

A single-node stock server can also start from a config file, but two extra flags are required:

```bash
cargo run -p rr-server -- serve --config examples/config/single-node.toml --allow-insecure --admin-port 52001
```

`single-node.toml` 未设置 `allow_insecure`（TLS 安全门会拒绝明文启动，直接退出），且 `admin_port` 被注释掉（不加 flag 时 `rr-admin` 默认的 52001 连不上）。`examples/config/3node-cluster-node{1,2,3}.toml` 是**多机部署模板**（需要 `rocks` feature 构建、TLS 证书与可解析主机名），不是本地快速启动。

`single-node.toml` does not set `allow_insecure` (the TLS safety gate refuses plaintext startup and the process exits) and has `admin_port` commented out (without the flag, rr-admin's default 52001 cannot connect). `examples/config/3node-cluster-node{1,2,3}.toml` are **multi-machine deployment templates** (they need a `rocks`-feature build, TLS certs, and resolvable hostnames) — not a local quickstart.
:::

---

## 管理端口与指标端口配置 / Admin Port & Metrics Port Configuration

每个 `RrNode` 支持可选的管理 gRPC 端口（Admin gRPC port）和 Prometheus 指标端口（Metrics HTTP port）。
通过 `NodeConfig` 的构建器方法可以启用它们：

---

Each `RrNode` supports an optional admin gRPC port and a Prometheus metrics HTTP port.
Enable them via `NodeConfig` builder methods:

```rust
let config = NodeConfig::new(node_id, listen_addr, storage, config_provider, sm_factory)
    .with_admin_port(52001)    // Admin gRPC 端口 / Admin gRPC port
    .with_metrics_port(62001); // Prometheus /metrics HTTP 端口 / Prometheus /metrics HTTP port
```

启用后：
- **Admin 端口**：暴露 `AdminService` gRPC API，可通过 `rr-admin` CLI 进行管理。
- **Metrics 端口**：在 `http://<host>:<metrics_port>/metrics` 暴露 Prometheus 格式的指标。

---

When enabled:
- **Admin port**: exposes the `AdminService` gRPC API, usable via the `rr-admin` CLI.
- **Metrics port**: serves Prometheus-format metrics at `http://<host>:<metrics_port>/metrics`.

---

## 示例一览 / Examples Overview

仓库共提供 5 个示例，建议从 `kv-store` 开始：

The repository ships 5 examples — start with `kv-store`:

- **`examples/kv-store`** — 本文使用的 KV 示例：零配置演示 + 单节点/三节点 gRPC 集群。**从这里开始。** / The KV example used in this guide: zero-config demo + single-node/three-node gRPC cluster. **Start here.**
- **`examples/counter`** — 分布式计数器（Increment/Get/Reset）；与 kv-store 相同的多节点花名册模式（默认端口 52001+，Admin 端口 53001+）。/ Distributed counter (Increment/Get/Reset); same multi-node roster pattern as kv-store (default ports 52001+, admin 53001+).
- **`examples/etcd-config-quickstart`** — 以 etcd v3 作为 ConfigProvider；需要本地 `localhost:2379` 运行 etcd（端点硬编码于 `examples/etcd-config-quickstart/src/main.rs:15`）。/ etcd v3-backed ConfigProvider; requires etcd running at `localhost:2379` (endpoint hardcoded at `examples/etcd-config-quickstart/src/main.rs:15`).
- **`examples/kv-store-kafka`** — Kafka ingestor 演示：Kafka 风格命令流写入 Raft 组，自动幂等（默认使用内存 MockKafkaIngestor）。/ Kafka ingestor demo: a Kafka-style command stream into a Raft group with auto-idempotency (uses the in-memory MockKafkaIngestor by default).
- **`examples/otel-client`** — 本地 traceparent 传播演示（stdout exporter），不连接集群。/ Local traceparent propagation demo (stdout exporter); does not connect to a cluster.

---

## 下一步 / Next Steps

深入了解 `rr` 的核心概念：

Dive deeper into `rr`'s core concepts:

- [Service Trait 指南 / Service Trait Guide](/guide/service-trait)
- [StateMachine Trait 指南 / StateMachine Trait Guide](/guide/state-machine-trait)
- [Client SDK 使用 / Client SDK Guide](/guide/client-sdk)
- [部署指南 / Deployment Guide](../deployment/deployment.md)
- [Admin API 参考 / Admin API Reference](/reference/admin-api)
- [Metrics 参考 / Metrics Reference](/reference/metrics-reference)
