---
sidebar_position: 1
---

# 架构概览 / Architecture Overview

:::tip C4 架构图 / C4 Architecture Diagrams
本页提供高层散文说明。若需要**视觉化的多层次结构图**（Context / Container / Component / Deployment / Dynamic），请参阅 [**C4 架构图目录 / C4 Architecture Diagrams**](./c4/README.md)。

This page provides a prose overview. For **visual multi-level structural diagrams** (Context / Container / Component / Deployment / Dynamic), see the [**C4 Architecture Diagrams index**](./c4/README.md).
- 系统级定位 / System-level framing → [c4/context.md](./c4/context.md)
- 部署单元 / Deployable units → [c4/containers.md](./c4/containers.md)
:::

## rr 是什么 / What is rr

rr 是一个基于 Rust 开发的分布式多 Raft 共识框架。它的核心目标是提供一个高度模块化、易于扩展且生产可用的共识层，让开发者可以专注于业务逻辑（通过 `Service` 或 `StateMachine` 接口），而无需处理复杂的共识算法细节、网络传输和成员变更。

rr 采用了多 Raft（Multi-Raft）设计哲学。与传统的单 Raft 实例不同，rr 允许在同一个节点上运行多个 Raft 共识组（Raft Groups）。这种设计极大地提升了系统的水平扩展能力和并发性能，是构建分布式数据库、分布式锁服务等高可用系统的理想选择。

---

rr is a distributed multi-Raft consensus framework built with Rust. Its primary goal is to provide a highly modular, extensible, and production-ready consensus layer, allowing developers to focus on business logic via `Service` or `StateMachine` traits without dealing with the complexities of consensus algorithms, networking, and membership changes.

rr adopts a multi-Raft design philosophy. Unlike traditional single-Raft instances, rr allows multiple Raft consensus groups to run on a single node. This design significantly improves horizontal scalability and concurrency, making it an ideal choice for building high-availability systems like distributed databases and lock services.

## 核心概念 / Key Concepts

| 概念 / Concept | 描述 / Description |
| :--- | :--- |
| **Node (节点)** | 运行 rr 实例的物理机或虚拟机，由唯一的 `NodeId` 标识。 |
| **Group (共识组)** | 一个 Raft 共识实例，包含多个 Peer 节点，由 `GroupId` 标识。 |
| **Partition (分片)** | 数据路由的最小单位，映射到具体的共识组，由 `PartitionId` 标识。 |
| **Leader/Follower** | Raft 角色，只有 Leader 节点可以处理写请求。 |
| **RoutingTable (路由表)** | 维护分片到节点、分片到共识组的映射关系。 |
| **ConfigProvider (配置提供者)** | 提供动态配置更新的抽象接口，支持内存、文件或外部存储。 |

## 系统架构 / System Architecture

```plantuml
@startuml
!include <C4/C4_Container.puml>

title Container Diagram - rr System Architecture

Container(client, "Client App", "Rust + rr-client", "Application using the cluster")

Container_Boundary(server, "rr-server / RrNode") {
  Component(router, "ShardRouter", "rr-router", "Partition → group resolver")
  Component(raft_mgr, "RaftGroupManager", "rr-raft", "Manages multiple Raft groups")
  Component(network, "gRPC Services", "rr-network", "RaftService + ClientService + AdminService")
  Component(raft1, "Raft Group 1", "openraft", "Independent consensus group")
  Component(raftN, "Raft Group N", "openraft", "Independent consensus group")
}

ComponentDb(storage, "StorageEngine", "rr-storage", "Mem or RocksDB backend")
Component(config, "ConfigProvider", "rr-config", "Routing table source")

Rel(client, network, "Client RPCs", "gRPC")
Rel(network, router, "Routes requests via")
Rel(network, raft_mgr, "Dispatches Raft RPCs to")
Rel(raft_mgr, raft1, "Manages", "Multiple groups")
Rel(raft_mgr, raftN, "Manages", "Multiple groups")
Rel(raft1, storage, "Log + SM persistence")
Rel(raftN, storage, "Log + SM persistence")
Rel(router, config, "Loads routing table")

@enduml
```

## 设计哲学 / Design Philosophy

- **模块化 Crate / Modular Crates**: 核心功能被拆分为多个独立 Crate，通过 Trait 进行解耦。
- **可插拔后端 / Pluggable Backends**: 存储（Storage）和配置（Config）提供者均可根据需求定制。
- **无魔法路由 / No Magic Routing**: 路由逻辑清晰，完全基于路由表（Routing Table）查找。
- **业务导向 / Business Oriented**: 通过简单的 `Service` 或 `StateMachine` 接口嵌入业务逻辑。

---

- **Modular Crates**: Core functionalities are split into independent crates, decoupled via traits.
- **Pluggable Backends**: Both storage and config providers can be customized as needed.
- **No Magic Routing**: Routing logic is explicit and based entirely on routing table lookups.
- **Business Oriented**: Embed business logic easily through simple `Service` or `StateMachine` traits.

## 与 openraft 的关系 / Relationship with openraft

rr 并不是从零开始实现 Raft 算法，而是深度封装了 [openraft](https://github.com/datafuselabs/openraft) (v0.9.x)。rr 在 openraft 之上添加了多共识组管理、gRPC 网络层、自动化的路由发现以及统一的生命周期管理，从而提供了一个开箱即用的分布式框架。

---

rr is not a Raft implementation from scratch. Instead, it deeply wraps [openraft](https://github.com/datafuselabs/openraft) (v0.9.x). rr adds multi-group management, a gRPC networking layer, automated routing discovery, and unified lifecycle management on top of openraft to provide an out-of-the-box distributed framework.
