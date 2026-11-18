---
sidebar_position: 10
---

# C4 架构图 / C4 Architecture Diagrams

本目录用 [C4 模型](https://c4model.com/)分层次描述 Rr：系统上下文、容器、组件，以及部署和动态视图。

This directory uses the [C4 Model](https://c4model.com/) to describe Rr through context, container, component,
deployment, and dynamic views.

## 阅读顺序 / Reading Order

| 读者 / Audience | 推荐顺序 / Recommended Order |
| :--- | :--- |
| 应用开发者 / Application developers | Context → Containers → Dynamic: PUT Request → Dynamic: Shard Routing |
| 框架贡献者 / Framework contributors | Containers → Components (all, including Console BFF) → Dynamic (all) |
| 运维 / SRE / Operators | Context → Containers → Components: Console → Deployment → Dynamic: Leader Election, Snapshot Transfer |

## 图谱目录 / Diagram Index

### 第 1 级：上下文 / Level 1: Context

- [**context.md**](./context.md) — 系统上下文：Rr 的边界、用户和外部系统。<br />System context: Rr's boundary, users, and external systems.

### 第 2 级：容器 / Level 2: Containers

- [**containers.md**](./containers.md) — 容器：可部署单元与通信协议。<br />Containers: deployable units and communication protocols.

### 第 3 级：组件 / Level 3: Components

- [**components-server.md**](./components-server.md) — rr-server 内部组件。<br />rr-server internals.
- [**components-raft.md**](./components-raft.md) — rr-raft 多组共识引擎。<br />Multi-Raft engine.
- [**components-router.md**](./components-router.md) — 路由与配置。<br />Routing and configuration.
- [**components-network.md**](./components-network.md) — gRPC 传输层。<br />gRPC transport.
- [**components-console.md**](./components-console.md) — `rr-admin console` 只读 BFF：同源入口、共享采集、证据缓存与可选历史。<br />Read-only `rr-admin console` BFF: same-origin entry, shared collection, evidence cache, and optional history.

### 部署 / Deployment

- [**deployment.md**](./deployment.md) — 三节点拓扑、端口、存储与监控。<br />Three-node topology, ports, storage, and monitoring.

### 动态视图 / Dynamic

- [**dynamic-put-request.md**](./dynamic-put-request.md) — 客户端写入与 leader 重定向。<br />Client write with leader redirect.
- [**dynamic-leader-election.md**](./dynamic-leader-election.md) — Raft 领导者选举。<br />Raft leader election.
- [**dynamic-snapshot-transfer.md**](./dynamic-snapshot-transfer.md) — 快照传输。<br />Snapshot transfer.
- [**dynamic-shard-routing.md**](./dynamic-shard-routing.md) — 分片到 group 的路由。<br />Partition-to-group routing.
- [**dynamic-config-update.md**](./dynamic-config-update.md) — 配置变更传播。<br />Config update propagation.

## C4 符号 / C4 Notation

| 符号 / Symbol | 含义 / Meaning |
| :--- | :--- |
| **Person** | 人类角色 / Human actor |
| **System** | 软件系统的顶层抽象 / Top-level software system |
| **Container** | 可独立运行的部署单元 / Independently runnable unit |
| **Component** | 容器内的逻辑模块 / Logical module inside a container |
| **Rel** | 带意图和技术标注的有向关系 / Directed relationship with intent and technology |

> **注意**：C4 的 **Container** 不是 Docker container；它可以是一个二进制、服务器进程或加载
> rr-client 的应用进程。
>
> **Note**: A C4 **Container** is not necessarily a Docker container; it can be a binary, a server process, or an
> application process using rr-client.

## 相关架构文档 / Related Architecture Documents

- [overview.md](../overview.md) — 架构概览。<br />Architecture overview.
- [crate-map.md](../crate-map.md) — Crate 依赖。<br />Crate dependencies.
- [data-flow.md](../data-flow.md) — 数据流。<br />Data flow.
- [multi-raft.md](../multi-raft.md) — 多 Raft 架构。<br />Multi-Raft architecture.
- [networking.md](../networking.md) — 网络。<br />Networking.
- [storage.md](../storage.md) — 存储。<br />Storage.
