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

- **Rust toolchain**: stable, 1.75+
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
bincode = { version = "2.0.0-rc.3", features = ["serde"] }
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
use rr_core::error::RrError;
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
            .map_err(|e| RrError::Codec(e.to_string()))?;

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

        Ok(bincode::serde::encode_to_vec(&response, standard())?)
    }

    fn name(&self) -> &str { "kv-store" }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let guard = self.store.read().await;
        Ok(bincode::serde::encode_to_vec(&*guard, standard())?)
    }

    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        let (restored, _): (HashMap<String, Vec<u8>>, _) = 
            bincode::serde::decode_from_slice(data, standard())?;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = 1;
    let listen_addr = "127.0.0.1:51001".to_string();
    let storage = Arc::new(MemStorage::new());
    let config_provider = Arc::new(MemConfigProvider::new());
    
    // 将 Service 包装为状态机 / Wrap Service as a StateMachine
    let sm_factory = Box::new(|_group_id| {
        Box::new(ServiceStateMachineAdapter::new(KvService::new()))
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

## 运行示例 / Running the Example

你可以直接运行项目中的 `kv-store` 示例来查看效果。

You can run the built-in `kv-store` example to see it in action.

```bash
# 克隆并构建 / Clone and build
git clone <repo>
cd rr
cargo build

# 启动单节点模式 / Start single node mode
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap

# 启动 3 节点集群（在 3 个终端中运行） / Start a 3-node cluster (run in 3 terminals)

# 终端 1: 引导首个节点 / Terminal 1: Bootstrap first node
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap

# 终端 2: 加入第二个节点 / Terminal 2: Join second node
cargo run -p kv-store -- --node-id 2 --port 51002 --peers 127.0.0.1:51001

# 终端 3: 加入第三个节点 / Terminal 3: Join third node
cargo run -p kv-store -- --node-id 3 --port 51003 --peers 127.0.0.1:51001,127.0.0.1:51002
```

---

## 接下来发生什么 / What Happens Next

当集群启动后：
1. **节点 1** 作为 Leader 引导集群。
2. **节点 2 和 3** 作为 Follower 加入。
3. 集群完成 **Leader 选举**。
4. 现在你可以通过客户端 SDK 或内置演示程序向集群发送写请求。

When the cluster starts:
1. **Node 1** bootstraps as the leader.
2. **Nodes 2 and 3** join as followers.
3. **Leader election** completes.
4. The cluster is ready to handle write requests via the Client SDK or built-in demo.

---

## 下一步 / Next Steps

深入了解 `rr` 的核心概念：

Dive deeper into `rr`'s core concepts:

- [Service Trait 指南 / Service Trait Guide](/guide/service-trait)
- [StateMachine Trait 指南 / StateMachine Trait Guide](/guide/state-machine-trait)
- [Client SDK 使用 / Client SDK Guide](/guide/client-sdk)
