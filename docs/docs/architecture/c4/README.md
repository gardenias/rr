---
sidebar_position: 10
---

# C4 架构图 / C4 Architecture Diagrams

本目录使用 [C4 模型](https://c4model.com/) 从多个抽象层次描绘 rr 框架的架构。C4 模型通过四个层次（Context、Container、Component、Code）和两类补充视图（Deployment、Dynamic）渐进式地呈现系统结构。

---

This directory uses the [C4 Model](https://c4model.com/) to describe the rr framework's architecture at multiple levels of abstraction. C4 progressively reveals system structure through four levels (Context, Container, Component, Code) and two supplementary views (Deployment, Dynamic).

## 阅读顺序 / Reading Order

| 读者 / Audience | 推荐顺序 / Recommended Order |
| :--- | :--- |
| 应用开发者 / Application Developers | Context → Containers → Dynamic: PUT Request → Dynamic: Shard Routing |
| 框架贡献者 / Framework Contributors | Containers → Components (all) → Dynamic (all) |
| 运维 / SRE / Operators | Context → Containers → Deployment → Dynamic: Leader Election, Snapshot Transfer |

## 图谱目录 / Diagram Index

### Level 1 — Context

- [**context.md**](./context.md) — 系统上下文 / System Context
  - rr 集群在更广阔系统中的位置；应用开发者、运维、监控、外部存储等外部行为者
  - The rr cluster's place in the wider system; external actors (developers, operators, monitoring, external storage)

### Level 2 — Containers

- [**containers.md**](./containers.md) — 容器图 / Container Diagram
  - 可部署单元（rr-server 节点、rr-admin CLI、用户应用）与通信协议
  - Deployable units (rr-server nodes, rr-admin CLI, user application) and communication protocols

### Level 3 — Components

- [**components-server.md**](./components-server.md) — rr-server 内部组件 / rr-server Internals
- [**components-raft.md**](./components-raft.md) — rr-raft 多组共识引擎 / Multi-Raft Engine
- [**components-router.md**](./components-router.md) — rr-router + rr-config 路由与配置 / Routing & Configuration
- [**components-network.md**](./components-network.md) — rr-network gRPC 传输层 / gRPC Transport

### Deployment

- [**deployment.md**](./deployment.md) — 生产部署拓扑 / Production Deployment Topology
  - 3 节点集群、网络端口、存储卷、监控
  - 3-node cluster, network ports, storage volumes, monitoring

### Dynamic

- [**dynamic-put-request.md**](./dynamic-put-request.md) — 客户端 PUT + 领导者重定向 / Client PUT with Leader Redirect
- [**dynamic-leader-election.md**](./dynamic-leader-election.md) — Raft 领导者选举 / Raft Leader Election
- [**dynamic-snapshot-transfer.md**](./dynamic-snapshot-transfer.md) — 快照传输 / Snapshot Transfer
- [**dynamic-shard-routing.md**](./dynamic-shard-routing.md) — 分片路由 / Shard Routing (Partition → Group → Leader)
- [**dynamic-config-update.md**](./dynamic-config-update.md) — 动态配置更新 / Dynamic Config Update Propagation

## 关于 C4 符号 / About C4 Notation

- **Person** — 人类角色（开发者、运维）/ Human actor
- **System** — 软件系统的顶层抽象 / Top-level software system
- **Container** — 可独立部署、独立运行的进程 / Independently deployable/runnable unit
- **Component** — 容器内部的逻辑模块（非独立部署）/ Logical module inside a container (not deployable alone)
- **Rel** — 有向关系，标注意图与技术 / Directed relationship with intent and technology

> **注意 / Note**：C4 图中的 **Container** 不等于 Docker Container。它指"一个独立运行的进程 / 应用"，可能是一个可执行文件（rr-admin）、一个服务器进程（rr-server 实例）或者一个库的使用者进程（加载 rr-client SDK 的用户应用）。

## 与其他架构文档的关系 / Relationship to Other Architecture Docs

本目录专注于 **结构性 C4 图**。补充性的散文解释在以下文档中：

This directory focuses on **structural C4 diagrams**. Complementary prose explanations live in:

- [overview.md](../overview.md) — 架构概览 / Architecture Overview
- [crate-map.md](../crate-map.md) — Crate 依赖图 / Crate Dependency Map
- [data-flow.md](../data-flow.md) — 数据流详细说明 / Detailed Data Flow
- [multi-raft.md](../multi-raft.md) — 多 Raft 架构 / Multi-Raft Architecture
- [networking.md](../networking.md) — 网络架构 / Networking
- [storage.md](../storage.md) — 存储层 / Storage Layer
