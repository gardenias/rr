# Rr — Distributed Multi-Raft Consensus Framework

[![Coverage](https://codecov.io/gh/gardenias/rr/branch/main/graph/badge.svg)](https://codecov.io/gh/gardenias/rr)

## 简介 / Introduction

Rr 是一个用 Rust 编写的分布式多 Raft 共识框架，专为构建高可用、强一致性的分布式系统而设计。它在单个节点上支持多个独立的 Raft 组，通过表格式的分片路由实现灵活的数据分布。核心特性包括可插拔的存储后端、基于 gRPC 的高效网络通信、动态的分布式配置管理和成熟的客户端 SDK，大大降低了开发分布式应用的复杂度。

---

Rr is a distributed multi-Raft consensus framework written in Rust, designed for building highly available and strongly consistent distributed systems. It supports multiple independent Raft groups on a single node with flexible data distribution through table-based shard routing. Key features include pluggable storage backends, efficient gRPC-based networking, dynamic distributed configuration management, and a mature client SDK that significantly reduces the complexity of building distributed applications.

## 特性 / Features

- **Multi-Raft Consensus Groups** — 单节点支持多个独立的 Raft 共识组 / Single node supports multiple independent Raft consensus groups
- **Table-Based Shard Routing** — 使用路由表进行分片映射，无需哈希环 / Shard mapping via routing table without hash ring
- **Pluggable Storage Backends** — 支持内存存储和 RocksDB（通过 `rocks` feature 启用）/ Support for in-memory and RocksDB storage via `rocks` feature
- **Durable Raft Log & Snapshots** — `--storage rocks` 持久化 Raft 日志与状态机快照（fail-closed）/ Durable raft log and state-machine snapshots with `--storage rocks` (fail-closed)
- **gRPC Networking with Connection Pooling** — 高效的 gRPC 网络层和连接池管理 / Efficient gRPC networking with connection pooling
- **TLS/mTLS + Bearer Auth + Rate Limiting** — 生产安全默认（G1–G7 启动安全门）/ Production-grade security by default (G1–G7 startup gates)
- **Dynamic Configuration Management** — 通过 ConfigProvider 动态管理分布式配置和分区（内存与 etcd 后端）/ Dynamic distributed configuration via ConfigProvider (in-memory and etcd backends)
- **Client SDK with Auto Leader-Redirect** — 智能客户端 SDK 自动处理领导者重定向和重试（Rust + Java）/ Smart client SDK with auto leader-redirect and retry (Rust + Java)
- **Snapshot Transfer Between Nodes** — 节点间的快照传输支持快速同步 / Snapshot transfer between nodes for fast synchronization
- **Linearizable Reads** — 协议层强一致读（ReadIndex + leader 重定向）/ Protocol-level linearizable reads (ReadIndex + leader redirect)
- **Framework Idempotency & Kafka Ingest** — exactly-once apply 语义与 Kafka 输入 / Exactly-once apply semantics and Kafka input
- **Backup / Restore** — 离线备份、在线快照流式备份与恢复 / Offline backup, online snapshot streaming backup, and restore

## Architecture

A C4 Container view of a deployed Rr cluster — what processes run, what they talk, and over which protocols.

```mermaid
C4Container
  title Container Diagram - Rr Cluster (condensed)

  Person(developer, "App Developer")
  Person(operator, "Operator")

  System_Boundary(client_side, "Client Side") {
    Container(user_app, "User Application", "Rust + rr-client SDK", "Auto leader-redirect & retry")
    Container(admin_cli, "rr-admin CLI", "Rust binary", "Cluster administration")
  }

  System_Boundary(cluster, "Rr Cluster (N nodes)") {
    Container(rr_node, "rr-server Node", "Rust binary", "Hosts multiple Raft groups; serves Client + Raft + Admin gRPC")
    ContainerDb(rocks, "RocksDB Data Dir", "Local SSD", "Raft log + state (optional rocks feature)")
  }

  System_Ext(prometheus, "Prometheus", "Metrics scraping")

  Rel(developer, user_app, "Builds & deploys")
  Rel(operator, admin_cli, "Runs commands")
  Rel(user_app, rr_node, "ClientRequest / GetClusterInfo", "gRPC")
  Rel(admin_cli, rr_node, "Admin RPCs", "gRPC")
  Rel(rr_node, rr_node, "Raft RPCs (AppendEntries / Vote / InstallSnapshot)", "gRPC")
  Rel(rr_node, rocks, "Reads/writes log", "File I/O")
  Rel(rr_node, prometheus, "Exposes /metrics", "HTTP")

  UpdateRelStyle(rr_node, rr_node, $offsetX="40", $offsetY="-40")
```

For the full C4 documentation — Context, Component, Deployment, and Dynamic flows (client PUT with leader redirect, leader election, snapshot transfer, shard routing, config updates) — see [`docs/docs/architecture/c4/`](./docs/docs/architecture/c4/README.md). For the crate dependency layering, see [`docs/docs/architecture/crate-map.md`](./docs/docs/architecture/crate-map.md).

## Quick Start

### Define Types

Create request and response types that implement `Serialize`/`Deserialize`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvRequest {
    Put { key: String, value: Vec<u8> },
    Get { key: String },
    Delete { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvResponse {
    pub value: Option<Vec<u8>>,
}
```

### Implement Service Trait

Create a service that handles client requests:

```rust
use async_trait::async_trait;
use bincode::config::standard;
use rr_core::error::{CodecError, RrError};
use rr_core::service::Service;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct KvService {
    store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl KvService {
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
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
                KvResponse {
                    value: guard.get(&key).cloned(),
                }
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

    fn name(&self) -> &str {
        "kv-store"
    }

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

### Run It

零配置运行内置 KV Store 演示（进程内 Raft，自选主后执行一组读写并退出，约 0.2s）：

Run the built-in KV Store demo with zero config (in-process Raft, self-elects a leader, runs a few reads/writes, then exits in ~0.2s):

```bash
cargo run -p kv-store
```

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

再启动一个常驻的单节点 gRPC 服务，并用 `rr-admin` REPL 作为 KV 客户端连接：

Then bootstrap a long-running single-node gRPC server and connect with the `rr-admin` REPL as a KV client:

```bash
# Terminal 1: 引导节点（阻塞运行，Ctrl-C 停止）/ bootstrap node (blocks; Ctrl-C to stop)
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap

# Terminal 2: REPL 必须连接 kv-store 的监听端口 51001（不是 rr-admin 默认的 52001）
# Terminal 2: the REPL must target kv-store's listen port 51001 (NOT rr-admin's default 52001)
cargo run -p rr-admin -- repl --addr 127.0.0.1:51001 --group 1
```

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

> **Note**: `kv-store` 会把 Admin API 挂在客户端端口 +1000（51001 → 52001）。REPL 的管理命令默认按 `--addr` 的主机 + 端口+1000 推导 admin 端点（`--admin-addr` 可显式覆盖），所以指向客户端端口 51001 时 `status`/`members` 也会自动打到 52001；一次性子命令用 `--addr 127.0.0.1:52001`（如 `cargo run -p rr-admin -- --addr 127.0.0.1:52001 node status`）。
> `kv-store` serves its Admin API on the client port +1000 (51001 → 52001). The REPL derives the admin endpoint as the `--addr` host with port +1000 by default (override with `--admin-addr`), so pointing it at the client port 51001 still reaches admin commands on 52001; for one-shot subcommands use `--addr 127.0.0.1:52001` (e.g. `cargo run -p rr-admin -- --addr 127.0.0.1:52001 node status`).

想以编程方式嵌入节点，请参考 [`examples/kv-store/src/main.rs`](./examples/kv-store/src/main.rs) 和[快速入门指南](https://rr-framework.github.io/guide/getting-started)。真实 API 为 `NodeConfig::new(node_id, listen_addr, storage, config_provider, sm_factory)` → `RrNode::new(config)?`（同步、单参数）→ `node.start().await?`。

To embed a node programmatically, see [`examples/kv-store/src/main.rs`](./examples/kv-store/src/main.rs) and the [Getting Started guide](https://rr-framework.github.io/guide/getting-started). The real API is `NodeConfig::new(node_id, listen_addr, storage, config_provider, sm_factory)` → `RrNode::new(config)?` (synchronous, single argument) → `node.start().await?`.

## Managed Cluster Formation

Fresh clusters can use homogeneous Genesis formation. Every node receives the same full advertised roster and
`[cluster.formation]` settings; no ordinal-specific bootstrap or peers list is derived. Genesis initializes the
full roster only through a linearizable lifecycle record and does not run the ordinary cluster-form job.

Existing initialized groups use `mode = "adopt"` instead. Adopt initialized nodes first, verify that ACK data
and membership are unchanged, then start pristine nodes and converge them through the existing formation job or
manual `add-learner` + `change-membership`. Adoption never initializes a group or expands membership by itself.

SeedSpec describes the initial routing/Genesis roster, not current Raft membership. P1 does not support dynamic
membership expansion, replica replacement, or resharding. During a lifecycle-store outage, initialized nodes can
continue Raft traffic with unchanged routes but remain unready/degraded; pristine nodes fail closed. See the
[getting-started guide](docs/docs/guide/getting-started.md) and [upgrade runbook](docs/docs/deployment/upgrading.md).

## Build Instructions

### Basic Build

```bash
# Build all crates
cargo build

# Run all tests
cargo test

# Build with RocksDB storage (rocks feature)
cargo build --features rocks

# Run the KV store example
cargo run -p kv-store
```

### Multi-Node Consensus Cluster

以下是本机实测通过的三节点真实共识集群（`kv-store` 示例，真实 KV 语义）：

The following three-node real-consensus cluster is verified on a single machine (the `kv-store` example, real KV semantics):

```bash
# Terminal 1 — node 1 (bootstrap)
# --peers 携带全量 id@addr 花名册（含自己），三个节点保持一致
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003

# Terminal 2 — node 2
cargo run -p kv-store -- --node-id 2 --port 51002 --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003

# Terminal 3 — node 3
cargo run -p kv-store -- --node-id 3 --port 51003 --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003
```

每个节点在 `5100N` 提供 ClientService，在 `5200N`（port+1000）提供 AdminService。引导节点以全量成员初始化 Raft 组，其余节点通过日志复制自动跟上。验证：

Each node serves ClientService on `5100N` and AdminService on `5200N` (port+1000). The bootstrap node initializes the Raft group with the full membership; the others catch up via log replication. Verify:

```bash
# 三节点 group status 一致（leader_node_id 相同、last_log_index 同步推进）
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group status --group 1

# 在 leader 的监听端口上做 KV 读写（真实 KV 语义）
cargo run -p rr-admin -- repl --addr 127.0.0.1:51001 --group 1
# rr> put k1 v1   → OK
# rr> get k1      → v1
```

故障转移（实测通过）：Ctrl-C 掉 leader 节点，几秒内其余节点选出新 leader，写入可继续且已提交数据保留。

Failover (verified): Ctrl-C the leader node — within seconds the survivors elect a new leader, writes continue, and committed data survives.

注意事项（均经实测）/ Caveats (all verified against live runs):

- **`--peers` 花名册格式为 `id@addr`**：三个节点使用相同的花名册且必须包含本节点；畸形条目会在启动时报错。/ **The `--peers` roster uses `id@addr` entries**: all nodes take the same roster, which must include the node itself; malformed entries fail fast at startup.
- **服务端不在节点间转发客户端写请求**：发往 follower 会返回 `success=false`；REPL 和客户端 SDK 负责自动寻找 leader。/ **The server does not forward client writes between nodes**: a follower returns `success=false`; the REPL and client SDK locate the leader automatically.
- **REPL 的 `OK` 以服务端 `success=true` 为准**：REPL 启动时读取组成员地址，跟随 `leader_info` 重定向，并在当前节点断连或没有 leader 提示时尝试其他成员；最终被拒绝的写打印 `Error: request rejected ...`（不再有假 OK）。复制进度仍请用每个节点 `group status` 的 `last_log_index` 验证。/ **The REPL's `OK` requires the server's `success=true`**: it discovers member addresses at startup, follows `leader_info` redirects, and tries other members when the current node disconnects or has no leader hint. Refused writes print `Error: request rejected ...` (no fake OKs) — still verify replication via `last_log_index` in `group status` on each node.
- **默认内存模式的恢复边界**：重启节点会丢失本地 Raft 日志和 vote，存活 leader 会重置其复制进度并从头追平，因此三节点失去多数派后可在两个空节点重启后恢复。全部节点重启仍会丢失数据；需要跨重启持久化时使用 `rocks` feature 和 `--data-dir`。/ **Recovery boundary of the default in-memory mode**: a restarted node loses its local Raft log and vote; the surviving leader resets its replication progress and catches it up from scratch, so a three-node cluster can recover after two empty nodes restart. Restarting every node still loses data; use the `rocks` feature and `--data-dir` for durable restart recovery.
- **生产式路径**：使用 stock `rr-server` + `rr-admin` 显式成员管理（`add-learner` + `change-membership`）的三节点集群同样实测可用，详见 [getting-started 指南](docs/docs/guide/getting-started.md)。注意 stock `rr-server` 内置的是 EchoStateMachine 冒烟服务（`get` 回显 key 本身，非 KV 语义）。/ **Production-style path**: a three-node cluster driven by stock `rr-server` + explicit `rr-admin` membership management (`add-learner` + `change-membership`) is also verified — see the [getting-started guide](docs/docs/guide/getting-started.md). Note the stock `rr-server` ships an EchoStateMachine smoke service (`get` echoes the key itself — not KV semantics).
- **`examples/config/3node-cluster-node{1,2,3}.toml` 是多机部署模板**（已含 `[tls]`/`[auth]` 段：需要 `rocks` feature 构建、scripts/gen-dev-certs.sh 生成的 TLS 证书、可解析主机名与 `RR_ADMIN_TOKEN`/`RR_CLIENT_TOKEN` 环境变量），不是本地快速启动；成员组建命令见 node1 模板头注释。/ **`examples/config/3node-cluster-node{1,2,3}.toml` are multi-machine deployment templates** (they ship `[tls]`/`[auth]` sections and need a `rocks`-feature build, TLS certs from scripts/gen-dev-certs.sh, resolvable hostnames, and the `RR_ADMIN_TOKEN`/`RR_CLIENT_TOKEN` env vars) — not a local quickstart; the membership commands are in the node1 template's header comment.

## Developer Workflow with just

日常命令已封装在根目录的 [justfile](./justfile)（需安装 [just](https://github.com/casey/just)）。深度操作手册（集群探玩、Genesis/Adoption、故障转移、真实验收）见
[Operations Playbook](docs/docs/guide/playbook.md)；Java SDK 用法见 [sdks/java/README.md](sdks/java/README.md)。

Common commands are wrapped in the root [justfile](./justfile) (requires [just](https://github.com/casey/just)). For the deep operations walkthrough — cluster exploration, Genesis/adoption, failover, real acceptance suites — see the [Operations Playbook](docs/docs/guide/playbook.md); for the Java SDK see [sdks/java/README.md](sdks/java/README.md).

```bash
just --list          # 列出全部命令 / list every recipe
just doctor          # 环境自检（必需与可选工具分开报告）/ environment self-check
just test            # cargo test 全量测试 / full test suite
just kv-cluster-up   # 一键三节点内存 KV 集群（--bootstrap 路径）/ 3-node in-memory KV cluster
just genesis-up      # 一键三节点同构 Genesis 集群（需本地 etcd）/ homogeneous Genesis cluster
just test-etcd       # 真实 etcd 验收测试 / real-etcd acceptance tests
just helm-test       # Helm chart 渲染测试 / Helm render tests
just java-test       # Java SDK 单元测试 / Java SDK unit tests
just nightly         # 本地质量门禁（invariants+全量测试+crash+fuzz+TLA）/ local quality gates
```

后台进程的 PID 与日志统一放在 `target/just/`，用 `just kv-cluster-down` / `just genesis-down` / `just etcd-down` 清理。
Recipes track background processes via PID and log files under `target/just/`; tear them down with `just kv-cluster-down` / `just genesis-down` / `just etcd-down`.

## Documentation

📖 For detailed API documentation, architectural deep-dives, and advanced configuration, see the [📖 Documentation](https://rr-framework.github.io)

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contributing

We welcome contributions! Please feel free to open issues or submit pull requests on GitHub.
