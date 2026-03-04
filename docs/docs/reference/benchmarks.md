---
sidebar_position: 5
---

# 基准测试 / Benchmarks

## 现状 / Status

本项目尚未正式实施基准测试。此页面概述了基准测试计划，并为贡献者提供了指导。

---

Benchmarks have not yet been formally implemented for this project. This page outlines the benchmark plan and guidance for contributors.

## 待测试项目 / What to Benchmark

我们需要测量的关键指标包括：

- **写吞吐量 (Write Throughput)**: 客户端写入的每秒请求数，针对不同集群规模（1、3、5 个节点）进行测试。
- **写延迟 (Write Latency)**: 客户端写入的 p50、p95、p99 延迟。
- **领导者选举时间 (Leader Election Time)**: 从领导者失效到建立新领导者所需的时间。
- **快照传输时间 (Snapshot Transfer Time)**: 将快照传输到落后跟随者所需的时间。
- **读吞吐量 (Read Throughput)**: 经过领导者读取的每秒请求数。

---

Key metrics to measure include:

- **Write throughput**: requests per second for client writes, measured across group sizes (1, 3, 5 nodes).
- **Write latency**: p50, p95, p99 latency for client writes.
- **Leader election time**: time from leader failure to new leader established.
- **Snapshot transfer time**: time to transfer a snapshot to a lagging follower.
- **Read throughput**: requests per second for reads (via leader).

## 如何进行基准测试 / How to Benchmark

以下是进行基准测试的指导建议：

- 使用 [criterion.rs](https://crates.io/crates/criterion) 进行微型基准测试 (Micro-benchmarks)。
- 使用 [tokio-console](https://crates.io/crates/tokio-console) 进行异步运行时性能剖析。
- 使用 `tests/integration/` 中的集成测试基础设施 (`ClusterBuilder`) 进行端到端基准测试。
- 建议设置：在物理隔离的机器或虚拟机上部署 3-5 个节点，以获得真实的性能数据。

---

Guidance for benchmarking:

- Use [criterion.rs](https://crates.io/crates/criterion) for micro-benchmarks.
- Use [tokio-console](https://crates.io/crates/tokio-console) for async runtime profiling.
- Use integration test infrastructure (`ClusterBuilder` in `tests/integration/`) for end-to-end benchmarks.
- Suggested setup: 3-5 nodes on separate machines or VMs for realistic numbers.

## 参考基准 / Reference Benchmarks

关于性能对比的参考信息：

- rr 基于 openraft 构建，可参考 [openraft benchmarks](https://datafuselabs.github.io/openraft/getting-started.html)。
- 网络延迟是 Raft 写入延迟的主要决定因素。
- 典型的 Raft 写入延迟：2-3 个 RTT（提议 → 复制 → 提交）。

---

Reference information for comparison:

- For comparison, see [openraft benchmarks](https://datafuselabs.github.io/openraft/getting-started.html) — rr builds on openraft.
- Network latency is the dominant factor for Raft write latency.
- Typical Raft write latency: 2-3 RTTs (propose → replicate → commit).

## 贡献基准测试 / Contributing Benchmarks

如果您运行了基准测试，请通过 Pull Request 将其添加到 `benches/` 目录中。请在报告中包含硬件规格、操作系统和 Rust 版本。

---

If you run benchmarks, please open a PR to add them to `benches/` directory. Include hardware specs, OS, Rust version in benchmark reports.
