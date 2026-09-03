---
sidebar_position: 2
---

# Level 2 — 容器图 / Container Diagram

容器图展示 rr 系统的**可部署单元**——独立运行的进程、CLI 工具和外部依赖。它是大多数团队理解 rr 部署形态的最重要视图。

---

The Container diagram shows **deployable units** in an rr system — independently running processes, CLI tools, and external dependencies. It's the most important diagram for most teams to understand rr's deployed shape.

## 图示 / Diagram

```plantuml
@startuml
!include <C4/C4_Container.puml>

title Container Diagram - rr Cluster

Person(developer, "Application Developer")
Person(operator, "Cluster Operator")
Person_Ext(end_user, "End User")

System_Boundary(client_side, "Client Side") {
  Container(user_app, "User Application", "Rust + rr-client SDK", "Implements Service trait; uses RrClient with auto leader-redirect & retry")
  Container(admin_cli, "rr-admin CLI", "Rust binary, clap + tonic", "Interactive REPL & one-shot commands for cluster administration")
}

System_Boundary(cluster, "rr Cluster (N nodes)") {
  Container(rr_node, "rr-server Node", "Rust binary built from examples/* or custom server", "Hosts multiple Raft groups; serves Client + Raft gRPC on listen_addr (51001 by convention), Admin gRPC on a separate admin_port, /metrics on metrics_port")
  ContainerDb(rocks, "RocksDB Data Dir", "Local SSD filesystem", "Per-group independent RocksDB at {base_path}/{group_id} (rocks feature). Caveat: the stock binary never wires RocksDB raft-log storage — used today only by integration tests")
}

System_Ext(prometheus, "Prometheus", "Scrapes /metrics over HTTP")
System_Ext(config_store, "External ConfigProvider", "etcd / file / custom - optional in prod")

Rel(end_user, user_app, "Sends domain requests", "Any protocol")
Rel(user_app, rr_node, "Submits ClientRequest, receives leader_info redirect / ClusterInfo (group ids only; leader & replicas are placeholders)", "gRPC / HTTP2")
Rel(admin_cli, rr_node, "Queries status, triggers snapshot, transfers leader", "gRPC (AdminService)")

Rel_U(rr_node, rr_node, "Raft RPCs (AppendEntries, Vote, InstallSnapshot)", "gRPC / HTTP2")
Rel(rr_node, rocks, "Reads/writes raft log (per-group instance; not wired by stock binary)", "File I/O")
Rel(rr_node, config_store, "Loads routing table, subscribes to changes", "ConfigProvider trait")
Rel(rr_node, prometheus, "Exposes metrics", "HTTP /metrics")

Rel(developer, user_app, "Builds & deploys")
Rel(operator, admin_cli, "Runs commands")

LAYOUT_WITH_LEGEND()
@enduml
```

## 容器清单 / Container Inventory

### 部署单元 / Deployable

| Container | Technology | Deployable Form | Runs Where |
| :--- | :--- | :--- | :--- |
| **rr-server Node** | Rust binary (built from `examples/kv-store`, `examples/counter`, or custom) | Linux binary, Docker image, systemd unit | 1 process per node, N nodes per cluster (typically 3, 5, or 7) |
| **rr-admin CLI** | Rust binary (`crates/rr-admin`) | CLI tool | Operator workstation or bastion host |
| **User Application** | Rust binary (or any language with a future gRPC SDK) | Varies — could be a web server, batch job, cronjob | Any machine that can reach the cluster over gRPC |

### 外部系统 / External

| External | Role |
| :--- | :--- |
| **RocksDB Data Dir** | Per-group persistent Raft log storage (`{base_path}/{group_id}`, one independent RocksDB per group). Not a separate process — a library embedded in the node writing to local disk. **Caveat**: the stock `rr-server` binary always uses in-memory raft-log storage (`RaftGroupManager::new_with_network`); RocksDB raft-log persistence is currently exercised only by integration tests. Shown here for deployment clarity. |
| **Prometheus** | Optional metrics scraper |
| **External ConfigProvider** | Optional production source of routing table (vs. in-process `MemConfigProvider` used in dev/tests) |

## Library Crates - NOT Containers

以下 crate 是 **库 / libraries**，被链接进 rr-server 或用户应用进程——它们不是独立部署的容器，不会在容器图中出现：

The following crates are **libraries** linked into rr-server or the user application — they are not independently deployed containers and do not appear in the Container diagram:

| Crate | Linked Into |
| :--- | :--- |
| `rr-core` | rr-server, rr-client, rr-admin, every other crate |
| `rr-storage` | rr-server (provides `MemStorage` / `RocksStorage`) |
| `rr-config` | rr-server (provides `MemConfigProvider`) |
| `rr-raft` | rr-server (provides `RaftGroupManager`, `RaftNode`) |
| `rr-router` | rr-server (provides `ShardRouter`) |
| `rr-network` | rr-server, rr-client, rr-admin (provides gRPC stubs & `ChannelPool`) |
| `rr-client` | User Application |

For internal breakdown of each crate, see the [Component-level diagrams](./components-server.md).

## 通信矩阵 / Communication Matrix

| From → To | Protocol | Service / Port | Purpose |
| :--- | :--- | :--- | :--- |
| User App → rr-server | gRPC | `ClientService` @ 51001 | `Request`, `ReadLocal`, `ReadLinearizable`, `GetClusterInfo` |
| rr-admin → rr-server | gRPC | `AdminService` @ `admin_port` | Node/group status, snapshots, leader transfer, membership, backup, metrics |
| rr-server ↔ rr-server | gRPC (bidirectional) | `RaftService` @ 51001 | `AppendEntries`, `Vote`, `InstallSnapshot` (streaming) |
| Prometheus → rr-server | HTTP | `/metrics` @ `metrics_port` | Metrics scraping |
| rr-server → Disk | File I/O (RocksDB) | Local path | Raft log persistence (rocks feature; not wired by the stock binary — see above) |
| rr-server → ConfigProvider | Trait call | Impl-defined | Load routing table (`get_routing_table`), watch `RoutingTableChange` stream |

**关键点 / Key points**:

1. **一个端口承载两种服务** — `RaftService` 与 `ClientService` 默认共享同一个 gRPC 端口（51001）。这简化部署，但生产环境可配置 `admin_port` 与 `metrics_port` 单独暴露。
2. **客户端是"智能客户端"** — `RrClient` 会缓存每个 group 的 leader 地址，在收到携带 `leader_info` 的重定向响应（`success=false`）时自动更新缓存、退避后重试。
3. **没有独立网关 / 代理** — 集群没有独立的路由层；任何节点都能接收客户端请求并路由到正确 group 的 leader。

---

1. **One port hosts two services** — `RaftService` and `ClientService` share the same gRPC port (51001 by default). This simplifies deployment, but in production the `admin_port` and `metrics_port` can be exposed separately.
2. **Smart client** — `RrClient` caches each group's leader address and, upon receiving a redirect response carrying `leader_info` (`success=false`), updates its cache and retries after a backoff.
3. **No dedicated gateway / proxy** — there is no separate routing tier; any node can receive a client request and route it to the correct group's leader.

## 下钻 / Drill-down

- 进入节点内部 → [components-server.md](./components-server.md)
- 多 Raft 组细节 → [components-raft.md](./components-raft.md)
- 分片路由细节 → [components-router.md](./components-router.md)
- gRPC 层细节 → [components-network.md](./components-network.md)
- 物理部署 → [deployment.md](./deployment.md)
