---
sidebar_position: 1
title: Welcome to rr Framework
slug: /
---

# rr Framework

## 欢迎来到 rr 框架

**rr** 是一个为 Rust 设计的分布式多 Raft 共识框架。它提供了高效的、容错的分布式系统构建块，支持多个独立的 Raft 集群在单个网络中的共存。该框架由 11 个 crate 组成（9 个库 crate + 2 个二进制 crate rr-server/rr-admin），其中 8 个核心 crate：

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

## 项目目标 / Project Goals

**愿景**：成为 Rust 生态里"从共识库到生产系统"的标准答案——业务方只需实现 `Service` / `StateMachine` 两个 trait，
共识、分片、路由、客户端、运维由框架一次性补齐并完成集成联测。

**使命**：让开发者以分钟级成本构建分布式、高可用、可水平分片的内存化业务服务，
并在正确性、性能、可运维性三个维度同时达到生产级标准。

三大支柱，优先级 **G1 > G2 > G3**——目标冲突时按此序裁决：

### G1. 分布式正确性与鲁棒性（不可妥协）

- Raft CP 强一致；`read` / `read_linearizable` 显式分离，杜绝静默降级
- 多层验证网：TLA+ 形式化规约 + crash 恢复测试 + fuzz + 不变量守护（INV-x）
- 持久化契约：fsync 先于 ACK（关停契约 C1–C7）；快照要么完整要么不存在；失败一律 fail-closed
- 故障自愈：leader 秒级重选且不丢已提交数据；快照传输追赶落后节点；成员变更 quorum 守卫
- 集群 Formation（同构，P1）：genesis / adopt-legacy 双模式——所有节点携带同一份 SeedSpec 花名册，
  经 LifecycleStore 的线性化 CAS 逐组推进 Forming → Active；花名册分歧或生命周期存储中断一律 fail-closed

### G2. 高吞吐 & 低延迟

- 单共识组 100k ops/s 量级；聚合吞吐靠 multi-raft 分片水平扩展（而非加副本）——当前实测边界：同机多组聚合吞吐尚未随组数增长（受单 h2 连接串行点限制，见 `docs/perf/roadmap-100k.md`），跨机扩展不受此限
- IoBatcher 批量落盘 + 字节级背压；gRPC 连接池；读写 RPC 分离，本地读走最低延迟路径
- benches + perf-lab + soak + 基线对比：性能回归必须能被自动化发现

### G3. 可观测性 & 易用性

- 接入成本：实现两个 trait + 零配置 demo，分钟级拉起三节点真实共识集群
- 客户端：SDK 自动 leader 发现 / 重定向 / 重试 / 超时映射，含跨语言 SDK
- 运维工具链：rr-admin CLI/REPL（含 `formation status` 自省）、TLS/mTLS 与 G1–G7 启动安全自检、per-caller 限流与审计
- 可观测：Prometheus `/metrics`、tracing 结构化日志、compose / k8s / helm 部署模板

### 非目标 / Non-Goals

- 不是数据库：不提供 SQL、事务、二级索引
- 不做 AP 语义："加副本提升写吞吐"不在设计空间内
- 不优化跨地域低延迟写入：quorum 往返是硬下限
- 不替代 etcd / consul 等成品已覆盖的场景

开始探索我们的 [架构文档](/architecture/overview) 以了解系统设计，或查看 [入门指南](/guide/getting-started)。

---

## Welcome to rr Framework

**rr** is a distributed multi-Raft consensus framework designed for Rust. It provides efficient, fault-tolerant building blocks for distributed systems, supporting the coexistence of multiple independent Raft clusters within a single network. The framework comprises 11 crates (9 library crates plus the rr-server/rr-admin binaries), of which 8 are core crates:

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

## Project Goals

**Vision**: be the Rust ecosystem's standard answer for "from consensus library to production system" — implement two
traits (`Service` / `StateMachine`) and the framework supplies consensus, sharding, routing, the client SDK, and the
operational toolchain, integrated and jointly tested.

**Mission**: let developers build distributed, highly-available, horizontally-sharded in-memory business services in
minutes, meeting production-grade standards in correctness, performance, and operability at the same time.

Three pillars, in priority order **G1 > G2 > G3** — conflicts are resolved in this order:

### G1. Distributed correctness & robustness (non-negotiable)

- Raft CP strong consistency; `read` / `read_linearizable` are explicit separate RPCs — no silent downgrade
- Layered verification: TLA+ specs + crash-recovery tests + fuzz + invariant guards (INV-x)
- Durability contracts: fsync before ACK (shutdown contract C1–C7); snapshots are complete or nonexistent; fail-closed
- Self-healing: leader re-election in seconds without losing committed data; snapshot-transfer catch-up; membership quorum guard
- Managed cluster formation (P1): homogeneous genesis / adopt-legacy modes — every node carries the same SeedSpec
  roster, and a linearizable LifecycleStore CAS drives each group Forming → Active; roster divergence or
  lifecycle-store outage fails closed

### G2. High throughput & low latency

- ~100k ops/s per consensus group; aggregate throughput scales horizontally via multi-raft sharding (not replicas) — current measured boundary: same-host multi-group aggregate does NOT yet scale with group count (single-h2-connection serialization point, see `docs/perf/roadmap-100k.md`); multi-host scaling is unaffected
- IoBatcher batched fsync with byte-level backpressure; gRPC connection pooling; read/write RPC separation with a lowest-latency local read path
- benches + perf-lab + soak + baseline comparison: performance regressions must be caught automatically

### G3. Observability & ease of use

- Onboarding: two traits + a zero-config demo; a real three-node consensus cluster in minutes
- Client: SDK with automatic leader discovery / redirect / retry / timeout mapping, plus cross-language SDKs
- Ops toolchain: rr-admin CLI/REPL (with `formation status` introspection), TLS/mTLS with G1–G7
  startup security self-checks, per-caller rate limiting and audit
- Observability: Prometheus `/metrics`, tracing structured logs, compose / k8s / helm deployment templates

### Non-Goals

- Not a database: no SQL, transactions, or secondary indexes
- No AP semantics: "more replicas to scale writes" is outside the design space
- No low-latency cross-region writes: quorum round-trips are a hard floor
- Not a replacement where turnkey systems like etcd / consul already fit

Start exploring our [architecture documentation](/architecture/overview) to understand the system design, or check out our [getting-started guide](/guide/getting-started).
