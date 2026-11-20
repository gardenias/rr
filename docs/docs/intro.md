---
sidebar_position: 1
title: 欢迎使用 Rr / Welcome to Rr
slug: /
---

# 欢迎使用 Rr / Welcome to Rr

## Rr 是什么 / What is Rr

Rr 是一个面向 Rust 的分布式 multi-Raft 共识框架。你可以把它嵌入自己的二进制，通过实现
`Service` / `StateMachine` trait 接入业务逻辑；框架提供多共识组管理、分片路由、客户端和运维工具。
它不是开箱即用的数据库，标准 `rr-server` 内置的也只是用于冒烟测试的 `EchoStateMachine`。

Rr is a distributed multi-Raft consensus framework for Rust. Embed it in your own binary and implement the
`Service` / `StateMachine` traits to supply your business logic; the framework provides multi-group management,
shard routing, clients, and operational tooling. It is not a turnkey database, and the stock `rr-server` includes
only an `EchoStateMachine` for smoke tests.

工作区包含 11 个 Rr crate，按职责可分为：

The workspace contains 11 Rr crates, grouped here by responsibility:

- `rr-core`、`rr-raft`、`rr-storage`：核心类型、Raft 协议和存储抽象；默认使用内存存储，`rocks` feature
  启用 RocksDB。<br />
  Core types, Raft, and storage abstractions; in-memory storage is the default, with RocksDB behind `rocks`.
- `rr-network`、`rr-router`、`rr-client`：RPC 网络、分片路由和客户端 SDK。<br />
  RPC networking, shard routing, and the client SDK.
- `rr-config`、`rr-config-etcd`：配置管理，以及基于 etcd 的只读配置提供者。<br />
  Configuration management and a read-only etcd-backed config provider.
- `rr-server`、`rr-admin`：服务器二进制，以及运维 CLI / REPL 与同源只读 Web Console（静态前端、版本化 JSON
  和 SSE）。<br />
  The server binary and operational CLI / REPL plus a same-origin, read-only Web Console (static frontend, versioned
  JSON, and SSE).
- `rr-ingestor-kafka`：Kafka 数据接入。<br />
  Kafka ingestion.

## 适合谁 / Who Rr is for

- 你在构建自己的分布式 KV、锁、元数据或协调服务，需要强一致和分片，而不是部署成品数据库。<br />
  You are building your own distributed KV, lock, metadata, or coordination service and need strong consistency
  and sharding rather than a turnkey database.
- 你已评估 [openraft](https://github.com/datafuselabs/openraft)，还需要多共识组生命周期管理、路由、
  客户端和运维工具。<br />
  You have evaluated [openraft](https://github.com/datafuselabs/openraft) and also need multi-group lifecycle
  management, routing, clients, and operational tooling.
- 你希望把共识层作为库嵌入 Rust 服务，并自己定义状态机和业务语义。<br />
  You want to embed consensus as a library in a Rust service while defining your own state machine and semantics.

## 不适合谁 / Who should not use Rr

- 你需要 SQL、事务、二级索引或现成的数据模型。<br />
  You need SQL, transactions, secondary indexes, or a ready-made data model.
- 你需要 AP 语义，或希望通过增加副本提升单个共识组的写吞吐；Rr 使用 Raft CP，扩展方式是跨组分片。<br />
  You need AP semantics or expect more replicas to increase a single group's write throughput; Rr uses Raft CP
  and scales writes by sharding across groups.
- etcd、rqlite 或 consul 已经满足需求。<br />
  etcd, rqlite, or consul already meets your needs.
- 你要求跨地域低延迟写入；多数派往返延迟是强一致写入的硬下限。<br />
  You require low-latency cross-region writes; quorum round trips impose a hard latency floor.

更多取舍见 [常见问题](faq.md) 和 [与 openraft 的关系](architecture/openraft-relationship.md)。<br />
See the [FAQ](faq.md) and [Relationship with openraft](architecture/openraft-relationship.md) for more trade-offs.

## 一致性、存储与运维 / Consistency, storage, and operations

- `read` 是可能过期的本地读；`read_linearizable` 通过 ReadIndex 提供线性一致读，但需要多数派确认。<br />
  `read` is a local read that may be stale; `read_linearizable` uses ReadIndex for linearizable reads at the cost
  of quorum confirmation.
- 默认 `mem` 后端不跨重启保留数据；RocksDB 持久化需要 `rocks` feature，生产使用前仍应完成业务侧验证。<br />
  The default `mem` backend does not survive restarts; RocksDB persistence requires the `rocks` feature and still
  needs workload-specific validation before production use.
- 写请求由客户端根据 `leader_info` 重定向，服务端不转发；生产部署需要配置 TLS/mTLS 和认证。<br />
  Clients redirect writes using `leader_info` rather than server-side forwarding; production deployments require
  TLS/mTLS and authentication.
- `rr-admin console` 通过 Admin gRPC 和受信 observability HTTP 端点展示节点、Raft 组、生命周期与性能，
  是不影响节点服务的只读 viewer，不是 Raft 控制面；受控写操作仍由 Admin CLI 执行。详见
  [Cluster Admin Console](operations/cluster-admin-console.md)。<br />
  `rr-admin console` uses Admin gRPC and trusted observability HTTP endpoints to display nodes, Raft groups,
  lifecycle, and performance. It is a read-only viewer whose failure does not affect node service, not a Raft control
  plane; controlled writes remain in the Admin CLI. See [Cluster Admin Console](operations/cluster-admin-console.md).

详见 [读一致性模型](guide/read-consistency.md)、[存储引擎](guide/storage-engines.md) 和
[TLS 配置](deployment/tls.md)。<br />
See [Read Consistency](guide/read-consistency.md), [Storage Engines](guide/storage-engines.md), and
[TLS Configuration](deployment/tls.md).

## 版本、测试与性能 / Status, testing, and performance

当前工作区版本为 **1.0.0**。Rr 遵循 Semantic Versioning；破坏性变更只进入新的 MAJOR 版本，升级前请查看
[兼容性说明](reference/compatibility.md)。

The workspace is at **1.0.0**. Rr follows Semantic Versioning, with breaking changes reserved for new MAJOR releases;
review the [compatibility policy](reference/compatibility.md) before upgrading.

仓库包含单元测试、集成与故障注入测试、进程级 crash 测试、fuzz、proptest、线性一致检查，以及 4 份经
TLC 小模型检查的 TLA+ 规约。这些验证不等同于对你的部署和业务负载作出生产保证。

The repository includes unit, integration, and fault-injection tests, process-level crash tests, fuzzing,
proptest, linearizability checking, and four TLA+ specifications checked with small TLC models. These checks are
not a production guarantee for your deployment or workload.

性能数字不是 SLA。2026 年 11 月的三节点单机 loopback、128B 写基准中，默认配置约为 31k TPS；
169.3k TPS（RocksDB strict）和 174.2k TPS（mem）来自 v1.0.0 起提供、默认关闭的 batch64 + fanout4。
固定宿主机上增加 Raft 组不会提升聚合吞吐；多组扩展的前提是跨机且副本集合不相交。测试条件和原始数据见
[性能基准](reference/benchmarks.md) 与 [容量规划](deployment/capacity-planning.md)。

Performance figures are not an SLA. In November 2026 three-node, single-host loopback benchmarks with 128B writes,
the default configuration reached about 31k TPS; 169.3k TPS with RocksDB strict and 174.2k TPS with mem required
batch64 + fanout4, available since v1.0.0 and off by default. Adding groups on a fixed host does not increase
aggregate throughput; multi-group scaling assumes multiple hosts with disjoint replica sets. See
[Benchmarks](reference/benchmarks.md) and [Capacity Planning](deployment/capacity-planning.md) for test conditions
and raw data.

从 [入门指南](/docs/guide/getting-started) 和 [架构概览](/docs/architecture/overview) 开始。<br />
Start with the [getting-started guide](/docs/guide/getting-started) and [architecture overview](/docs/architecture/overview).
