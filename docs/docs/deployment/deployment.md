---
sidebar_position: 1
---

# 部署指南 / Deployment Guide

本指南介绍了如何在生产环境中构建、配置和运行 `rr` 框架。

This guide covers building, configuring, and running the `rr` framework in production environments.

## 生产构建 / Production Build

在生产环境中，建议使用 `--release` 标志进行构建以启用编译器优化。

In production environments, always build with the `--release` flag to enable compiler optimizations.

```bash
# 构建优化后的发布二进制文件
# Build optimized release binary
cargo build --release

# 构建支持 RocksDB 生产存储的版本
# Build with RocksDB for production storage
cargo build --release --features rocks

# 二进制文件将位于 target/release/ 目录下
# The binary will be at target/release/ directory
```

---

## 生产环境节点配置 / Node Configuration for Production

在生产部署时，需要配置持久化存储和正确的网络接口。

For production deployment, you need to configure persistent storage and correct network interfaces.

- **持久化存储 (Persistence)**: 启用 `rocks` 特性并使用 `RocksStorage`。
- **监听地址 (Listen Address)**: 将 `listen_addr` 设置为真实的网卡 IP（如 `0.0.0.0` 或内网 IP）。
- **配置提供者 (Config Provider)**: 在分布式集群中，建议使用支持持久化和通知的 `ConfigProvider` 实现（如对接 etcd、Consul 或数据库）。

---

## 运行 3 节点集群 / Running a 3-Node Cluster

以下是使用 `kv-store` 示例运行 3 节点集群的步骤：

Below are the steps to run a 3-node cluster using the `kv-store` example:

**节点 1 (引导节点 / Node 1 - Bootstrap)**:
```bash
RUST_LOG=info ./target/release/kv-store \
  --node-id 1 \
  --port 51001 \
  --bootstrap
```

**节点 2 (加入集群 / Node 2 - Join)**:
```bash
RUST_LOG=info ./target/release/kv-store \
  --node-id 2 \
  --port 51002 \
  --peers 192.168.1.1:51001
```

**节点 3 (加入集群 / Node 3 - Join)**:
```bash
RUST_LOG=info ./target/release/kv-store \
  --node-id 3 \
  --port 51003 \
  --peers 192.168.1.1:51001,192.168.1.2:51002
```

---

## 监控 / Monitoring

`rr` 提供了多层面的监控支持：

`rr` provides multi-level monitoring support:

- **结构化日志 (Structured Logging)**: 使用 `tracing` 库。可以通过环境变量 `RUST_LOG` 设置日志级别（如 `info`, `debug`）。
- **Raft 指标 (Raft Metrics)**: `openraft` 暴露了内部指标，包括 Raft 状态、日志索引和提交索引。可以通过 `raft.metrics()` 访问。
- **性能分析 (Profiling)**: 在开发或调试阶段，可以使用 `tokio-console` 进行异步运行时分析。

---

## 备份与恢复 / Backup and Restore

确保数据安全和可恢复性至关重要：

Ensuring data safety and recoverability is critical:

- **快照 (Snapshots)**: `openraft` 会按 `SnapshotPolicy::LogsSinceLast(N)`（默认 50_000 条日志，可通过 `NodeConfig::snapshot_logs_since_last` 调整）自动创建快照。注意快照仅保存在内存中，不随重启持久化（详见 [持久化与恢复](../architecture/durability.md)）。
- **RocksDB 备份**: 如果使用 RocksDB，可以使用其自带的备份 API 或直接对数据目录进行文件系统级快照。
- **恢复 (Restore)**: 恢复时，只需将备份的数据目录放回原位并重启节点，`rr` 会通过重放 Raft 日志恢复状态机状态。

---

## 添加新节点 / Adding a New Node

向现有集群添加节点：

Adding a node to an existing cluster:

1. 启动新节点，并将 `--peers` 指向集群中的现有成员。
2. 新节点会首先以 `Learner` 身份加入。
3. 更新 `ConfigProvider` 以包含新节点信息。
4. 调用 `change_membership` 将 `Learner` 提升为投票节点（`Voter`）。

---

## 移除节点 / Removing a Node

安全地移除集群节点：

Safely removing a node from the cluster:

1. 更新 `ConfigProvider` 以移除该节点。
2. 调用 `change_membership` 缩小集群规模并更新共识组配置。
3. 停止节点进程。
4. 确保剩余节点仍能达到法定人数（Quorum，通常为 `(N+1)/2`）。

---

## 网络要求 / Network Requirements

稳定的网络是分布式一致性的基础：

A stable network is the foundation of distributed consistency:

- **低延迟 (Low Latency)**: 节点间延迟应尽量低，因为 Raft 的每次写入都需要多次往返（RTT）。
- **开放端口 (Open Ports)**: 确保节点间的 gRPC 端口（默认如 51001, 51002 等）互通。
- **拓扑稳定 (Stable Topology)**: 避免频繁的网络分区，否则会导致频繁的领导者选举。
