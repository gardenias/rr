---
sidebar_position: 1
title: Welcome to rr Framework
slug: /
---

# rr Framework

## 欢迎来到 rr 框架

**rr** 是一个为 Rust 设计的分布式多 Raft 共识框架。它提供了高效的、容错的分布式系统构建块，支持多个独立的 Raft 集群在单个网络中的共存。该框架由 11 个库 crate 组成，其中 8 个核心 crate：

- **rr-core**: 核心数据结构和共识逻辑
- **rr-config**: 配置管理和参数调优
- **rr-storage**: 存储抽象与后端实现（内存存储，及 `rocks` feature 下的 RocksDB）
- **rr-raft**: Raft 协议实现
- **rr-network**: 网络通信和 RPC 框架
- **rr-router**: 请求路由和负载均衡
- **rr-client**: 客户端库和 API 绑定
- **rr-server**: 服务器实现和部署工具

另有 3 个附加 crate：**rr-config-etcd**（基于 etcd 的只读配置提供者）、**rr-ingestor-kafka**（Kafka 数据接入）和 **rr-admin**（运维 CLI 与 REPL）。

## rr 适合谁 / Who is rr for

- 你在构建**自己的**分布式 KV、锁、元数据或协调服务，需要强一致 + 分片，而不是部署一个成品数据库；
- 你评估过 [openraft](https://github.com/datafuselabs/openraft)，发现还需要多共识组管理、分片路由、客户端 SDK 和运维工具链——这正是 rr 补齐的部分（详见 [与 openraft 的关系](architecture/openraft-relationship.md)）；
- 你希望以库的形式把共识层嵌入自己的 Rust 二进制，实现 `Service` / `StateMachine` trait 即可接入业务逻辑；
- **不适合**的场景：需要 SQL/事务/二级索引、AP 语义、或跨地域低延迟写入——更多取舍讨论见 [常见问题](faq.md)。

开始探索我们的 [架构文档](/architecture/overview) 以了解系统设计，或查看 [入门指南](/guide/getting-started)。

---

## Welcome to rr Framework

**rr** is a distributed multi-Raft consensus framework designed for Rust. It provides efficient, fault-tolerant building blocks for distributed systems, supporting the coexistence of multiple independent Raft clusters within a single network. The framework comprises 11 library crates, of which 8 are core crates:

- **rr-core**: Core data structures and consensus logic
- **rr-config**: Configuration management and parameter tuning
- **rr-storage**: Storage abstraction and backends (in-memory, plus RocksDB behind the `rocks` feature)
- **rr-raft**: Raft protocol implementation
- **rr-network**: Network communication and RPC framework
- **rr-router**: Request routing and load balancing
- **rr-client**: Client library and API bindings
- **rr-server**: Server implementation and deployment tools

Three additional add-on crates complete the workspace: **rr-config-etcd** (read-only etcd-backed config provider), **rr-ingestor-kafka** (Kafka ingest), and **rr-admin** (ops CLI and REPL).

## Who is rr for

- You are building **your own** distributed KV, lock, metadata, or coordination service and need strong consistency + sharding — not a turnkey database;
- You have evaluated [openraft](https://github.com/datafuselabs/openraft) and found you still need multi-group management, shard routing, a client SDK, and an operational toolchain — exactly what rr adds (see [Relationship with openraft](architecture/openraft-relationship.md));
- You want to embed the consensus layer as a library in your own Rust binary, plugging in business logic via the `Service` / `StateMachine` traits;
- **Not a fit** when you need SQL/transactions/secondary indexes, AP semantics, or low-latency cross-region writes — see the [FAQ](faq.md) for more trade-off discussion.

Start exploring our [architecture documentation](/architecture/overview) to understand the system design, or check out our [getting-started guide](/guide/getting-started).
