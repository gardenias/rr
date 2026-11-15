---
sidebar_position: 1
---

# 快速入门 / Getting Started

先运行仓库内的 `kv-store`，再按需要实现服务或启动 stock `rr-server`。
Start with the bundled `kv-store`, then implement your own service or start the stock `rr-server` as needed.

## 前置要求 / Prerequisites

需要 Rust 1.98 或更高版本。构建使用 gRPC 的 crate 时还需要 `protoc`；只有启用 `rocks` feature 时才需要 RocksDB 的本地编译依赖。
Install Rust 1.98 or newer. gRPC crates also require `protoc`; native RocksDB build dependencies are needed only with the `rocks` feature.

```bash
# macOS
brew install protobuf

# Ubuntu
sudo apt-get install protobuf-compiler
```

## 运行单进程演示 / Run the In-Process Demo

在仓库根目录执行：
Run from the repository root:

```bash
cargo run -p kv-store
```

无参数运行会初始化单副本内存 Raft 组，执行写入、读取和删除后退出；不会启动 gRPC 服务，也不会持久化数据。
With no arguments, the example initializes a single-replica in-memory Raft group, performs writes, reads, and a delete, then exits. It starts no gRPC server and persists no data.

成功时退出码为 0，末尾包含：
A successful run exits with code 0 and ends with output including:

```text
INFO === rr KV-Store demo (single-node, no gRPC) ===
INFO PUT hello = "world"
INFO GET hello = "world"
INFO DEL hello
INFO GET hello = <not found>
INFO === rr KV-Store demo complete ===
```

## 启动单节点并使用 REPL / Start One Node and Use the REPL

终端 1 启动常驻节点；节点会一直运行，按 Ctrl-C 停止。
Start a long-running node in terminal 1; press Ctrl-C to stop it.

```bash
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap
```

日志出现 `Cluster bootstrapped on node 1` 后，在终端 2 连接客户端端口 51001。
After the log shows `Cluster bootstrapped on node 1`, connect to client port 51001 in terminal 2.

```bash
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

KV 命令使用客户端端口；管理命令使用独立的 Admin 端口。`kv-store` 将 Admin 端口设为主端口加 1000，本例为 52001。
KV commands use the client port; management commands use the separate Admin port. `kv-store` sets it to the main port plus 1000, or 52001 in this example.

```bash
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group status --group 1
```

## 启动三节点 KV 集群 / Start a Three-Node KV Cluster

三个节点必须使用同一份完整 `id@addr` 花名册，且花名册必须包含本节点。只在一个节点上设置 `--bootstrap`。
All three nodes must use the same complete `id@addr` roster, including the local node. Set `--bootstrap` on exactly one node.

```bash
# 终端 1：引导节点 / Terminal 1: bootstrap node
cargo run -p kv-store -- --node-id 1 --port 51001 \
  --bootstrap --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003

# 终端 2 / Terminal 2
cargo run -p kv-store -- --node-id 2 --port 51002 \
  --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003

# 终端 3 / Terminal 3
cargo run -p kv-store -- --node-id 3 --port 51003 \
  --peers 1@127.0.0.1:51001,2@127.0.0.1:51002,3@127.0.0.1:51003
```

引导日志应包含 `Cluster bootstrapped on node 1 — voters: [1, 2, 3]`。三个 Admin 端口依次为 52001、52002、52003。
The bootstrap log should include `Cluster bootstrapped on node 1 — voters: [1, 2, 3]`. The Admin ports are 52001, 52002, and 52003.

```bash
cargo run -p rr-admin -- --addr 127.0.0.1:52001 group membership --group 1
cargo run -p rr-admin -- repl --addr 127.0.0.1:51001 --group 1
```

写入后，逐个查询 `group status`；`last_log_index` 应在各节点推进。服务端不转发写请求，REPL 会按 `leader_info` 重定向；只有服务端返回 `success=true` 时才打印 `OK`。
After a write, query `group status` on each node and check that `last_log_index` advances. Servers do not proxy writes; the REPL follows `leader_info` redirects and prints `OK` only when the server returns `success=true`.

停止 leader 后，多数幸存节点会选出新 leader。内存模式下，重启全部节点不会保留数据；持久化验证见 [运维手册](playbook.md)。
After stopping the leader, the surviving majority elects a new one. In-memory mode does not preserve data if every node restarts; see the [operations playbook](playbook.md) for durability checks.

## 启动 stock rr-server / Start the Stock rr-server

stock 二进制内置 `EchoStateMachine`，仅用于协议和部署冒烟，不提供 KV 语义。以下命令启动单节点；不要再用 `--peers` 推断 Raft 成员关系。
The stock binary contains `EchoStateMachine` for protocol and deployment smoke tests, not KV semantics. This command starts one node; do not use `--peers` to infer Raft membership.

```bash
cargo run -p rr-server -- serve \
  --node-id 1 --listen-addr 127.0.0.1:51001 \
  --admin-port 52001 --bootstrap --allow-insecure
```

也可使用仓库配置；该文件已设置 `allow_insecure = true`，只需补上默认未启用的 Admin 端口：
You can also use the bundled configuration. It already sets `allow_insecure = true`; only the otherwise-disabled Admin port needs to be added:

```bash
cargo run -p rr-server -- serve \
  --config examples/config/single-node.toml --admin-port 52001
```

`--allow-insecure` 仅限本地开发。stock 多节点托管组建使用 `--cluster-nodes` 的完整 `id@advertised_addr` 花名册和 etcd 生命周期存储，不使用旧 `--peers` 流程。
`--allow-insecure` is for local development only. Managed stock clusters use a complete `--cluster-nodes` roster in `id@advertised_addr` form plus an etcd lifecycle store, not the legacy `--peers` flow.

## 在应用中实现 Service / Implement Service in an Application

Rr 尚未发布到 crates.io。下游项目可使用固定 tag 的 git 依赖；本地联调可使用 path 依赖，详见 [外部项目引入指南](consumer-guide.md)。
Rr is not published to crates.io. Downstream projects can use tag-pinned git dependencies or local path dependencies; see [Consuming Rr from External Projects](consumer-guide.md).

请求和响应由应用定义并编码为字节。`Service` 负责请求处理、快照和恢复；框架通过 `ServiceStateMachineAdapter` 将它接入 Raft。
The application defines and encodes requests and responses as bytes. `Service` handles requests, snapshots, and restoration; `ServiceStateMachineAdapter` connects it to Raft.

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

完整实现见 `examples/kv-store/src/service.rs`。实现时必须满足三个条件：快照覆盖同一原子视图；`handle()` 返回前完成全部状态修改；`restore()` 替换而不是合并旧状态。细节和错误示例见 [Service Trait 指南](/docs/guide/service-trait)。
See `examples/kv-store/src/service.rs` for the complete implementation. Snapshots must capture one atomic view, `handle()` must finish all mutations before returning, and `restore()` must replace rather than merge old state. See the [Service Trait guide](/docs/guide/service-trait) for details and failure examples.

节点由 `NodeConfig` 和 `RrNode` 组装。以下代码展示最小组装方式；实际运行前需向 `MemConfigProvider` 注册分片、组和副本，并初始化 Raft 成员，完整流程见 `examples/kv-store/src/main.rs`。
Assemble a node with `NodeConfig` and `RrNode`. This is the minimal wiring; a runnable node also needs partition, group, and replica registration plus Raft initialization, as shown in `examples/kv-store/src/main.rs`.

```rust
use std::sync::Arc;
use rr_config::mem::MemConfigProvider;
use rr_core::state_machine::StateMachine;
use rr_raft::service_adapter::ServiceStateMachineAdapter;
use rr_server::{config::NodeConfig, node::RrNode};
use rr_storage::mem::MemStorage;

let sm_factory = Box::new(|group_id| -> Box<dyn StateMachine> {
    Box::new(ServiceStateMachineAdapter::new(group_id, KvService::default()))
});
let config = NodeConfig::new(
    1,
    "127.0.0.1:51001".to_string(),
    Arc::new(MemStorage::new()),
    Arc::new(MemConfigProvider::new()),
    sm_factory,
)
.with_admin_port(52001)
.with_metrics_port(62001);

let node = RrNode::new(config)?;
node.start().await?;
```

启用后，AdminService 位于 Admin gRPC 端口，Prometheus 指标位于 `http://<host>:<metrics_port>/metrics`。
When enabled, AdminService uses the Admin gRPC port and Prometheus metrics are served at `http://<host>:<metrics_port>/metrics`.

## 下一步 / Next Steps

- [Service Trait 指南 / Service Trait Guide](/docs/guide/service-trait)
- [StateMachine Trait 指南 / StateMachine Trait Guide](/docs/guide/state-machine-trait)
- [客户端 SDK / Client SDK](/docs/guide/client-sdk)
- [运维手册 / Operations Playbook](playbook.md)
- [部署指南 / Deployment Guide](../deployment/deployment.md)
- [Admin API 参考 / Admin API Reference](/docs/reference/admin-api)
- [Metrics 参考 / Metrics Reference](/docs/reference/metrics-reference)
