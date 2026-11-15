---
title: 容量规划与性能部署 / Capacity Planning & Performance Deployment
description: 按目标 TPS 选择部署拓扑与客户端开关：实测基线、多组扩展规则、durability 选择、成本估算。
---

# 容量规划与性能部署 / Capacity Planning & Performance Deployment

以下测量来自 Apple M3 Pro loopback、128B payload；除注明外为 3 节点、S2 k=200。原始 JSON 位于 `docs/perf/baselines/`。跨
机扩展数字与成本是模型估算，不是多机实测。

Measurements use an Apple M3 Pro over loopback with 128B payloads; unless noted, tests use three nodes and S2 k=200. Raw
JSON is in `docs/perf/baselines/`. Multi-host scaling figures and costs are modeled estimates, not multi-host
measurements.

## 性能模型 / Performance Model

写路径为“复制到多数派 + 本地持久化 + apply”。本次测量的瓶颈依次是：

The write path is replicate-to-majority + local persistence + apply. In these measurements, the bottlenecks are:

1. **宿主机 CPU。** gRPC/h2 与请求分配/拷贝占主导；2026-09-22 采样中网络 syscall/h2 约 35%、分配器约 13%，openraft
   核心约 0.5%。<br />
   **Host CPU.** gRPC/h2 and request allocation/copy dominate; the 2026-09-22 sample attributed about 35% to network
   syscall/h2, 13% to allocation, and 0.5% to openraft core.
2. **单 h2 连接。** 默认每地址一条连接，服务端单连接任务串行处理帧；`with_channel_fanout(N)` 可缓解。<br />
   **One h2 connection.** The default is one connection per address, with serial frame handling;
   `with_channel_fanout(N)` relieves it.
3. **组提交窗口。** 自 2026-11-14（`73aded6`）默认 `window=0`；串行写由 443 提升至 4,831 TPS。<br />
   **Group-commit window.** Since 2026-11-14 (`73aded6`), `window=0` is the default; serial writes improved from 443 to
   4,831 TPS.
4. **磁盘。** 2026-11-14 进程内单操作批探针测得 Strict `RocksStorage::write_batch`（含 fsync）中位 30µs；该结论只适用于
   本次 loopback 环境。<br />
   **Disk.** A 2026-11-14 in-process, single-operation-batch probe measured Strict `RocksStorage::write_batch`, including
   fsync, at 30µs median; this applies only to the loopback setup.

串行写约 200µs 是本次环境中的 quorum RTT + apply 下限。扩吞吐的手段是连接扇出、客户端批处理和跨机多组分片。

The roughly 200µs serial-write latency is the quorum-RTT + apply floor in this environment. Throughput levers are
channel fanout, client batching, and multi-group sharding across machines.

## 实测基线 / Measured Baselines

### 单组吞吐 / Single-Group Throughput

| 配置 / Configuration | rocks-strict | mem |
| :--- | ---: | ---: |
| 默认，k=200 / Default, k=200 | 30.9k TPS | 30.7k TPS |
| S1 串行，k=1 / S1 serial, k=1 | 4,831 TPS · p50 198µs | ~4,762 TPS |
| fanout=4 | 61.9k TPS · p50 2.0ms | 72.0k TPS |
| batch64 + fanout4 | **169.3k TPS** · p50 900µs | **174.2k TPS** |

原始数据：`raw-20261114-s2-rocks-strict.json`、`raw-s2-mem.json`、`raw-20261114-s1-rocks-strict.json`、
`raw-20261114-s2-groups1-fan4-rocks-strict.json`、`raw-20261114-s2-batch64-fan4-rocks-strict.json`、
`raw-20261104-s2-batch64-fan4-mem.json`。

Raw evidence: `raw-20261114-s2-rocks-strict.json`, `raw-s2-mem.json`, `raw-20261114-s1-rocks-strict.json`,
`raw-20261114-s2-groups1-fan4-rocks-strict.json`, `raw-20261114-s2-batch64-fan4-rocks-strict.json`, and
`raw-20261104-s2-batch64-fan4-mem.json`.

三节点读路径：ReadLocal 约 35k TPS，可能读到略旧数据；ReadIndex 线性一致读平台期约 19–21k TPS。取舍见
[读一致性指南](../guide/read-consistency.md)。

Three-node read paths: ReadLocal reaches about 35k TPS and may return slightly stale data; linearizable ReadIndex
plateaus around 19–21k TPS. See [Read Consistency](../guide/read-consistency.md).

### 同机多组反模式 / Same-Host Multi-Group Anti-Pattern

rocks-strict，3 节点同机，k=200 / rocks-strict, three nodes on one host, k=200：

| 组数 / Groups | fanout=1 | fanout=4 |
| :--- | ---: | ---: |
| 1 | 33.0k | 61.9k |
| 2 | 30.7k | 52.0k |
| 4 | 27.0k | 44.7k |

同机加组会共享 CPU 和调度资源，聚合吞吐反而下降。多组线性扩展的前提是跨机且副本集合不相交。

Adding groups on the same hosts shares CPU and scheduling capacity and reduces aggregate throughput. Linear multi-group
scaling requires multiple machines with disjoint replica sets.

## 按目标 TPS 部署 / Deployment Recipes by Target TPS

### 不超过 30k TPS / Up to 30k TPS

单组三节点、默认参数，Rust 或 Java 客户端均可，无需额外调优。

Use one three-node group with default settings and either the Rust or Java client; no extra tuning is needed.

### 约 50k TPS：仅扇出 / About 50k TPS: Fanout Only

三节点单组，Rust 客户端 `with_channel_fanout(4)`。实测 61.9k TPS（rocks-strict），p50 2.0ms，相对 50k 目标余量约 24%。成
本为 3 台机器。

Use one three-node group with Rust client `with_channel_fanout(4)`. Measured throughput is 61.9k TPS on rocks-strict,
p50 2.0ms, about 24% headroom over a 50k target. Cost: three machines.

### 100k 级：批处理加扇出 / Around 100k TPS: Batching plus Fanout

三节点单组，使用 `with_batching(batch64, linger 1ms)` + `with_channel_fanout(4)`。实测 169.3k TPS（rocks-strict），p50
900µs / p99 1.5ms。需要 v0.3.0+；v0.2.x 没有该批处理 API，发布后仍默认关闭。

Use one three-node group with `with_batching(batch64, linger 1ms)` + `with_channel_fanout(4)`. Measured throughput is
169.3k TPS on rocks-strict, p50 900µs / p99 1.5ms. This requires v0.3.0+; v0.2.x has no batching API, and it remains
opt-in after release.

只用于按业务键天然幂等的 unkeyed 写，批次重试可能重复应用。需要恰好一次语义的写必须使用绕过批处理的
`send_request_idempotent`。详见 [客户端写批处理](../guide/client-sdk.md#客户端写批处理--client-write-batching)。成本为 3
台机器。

Use only for unkeyed writes idempotent by business key; a retried batch can be applied twice. Writes needing
exactly-once semantics must use `send_request_idempotent`, which bypasses batching. See
[Client Write Batching](../guide/client-sdk.md#客户端写批处理--client-write-batching). Cost: three machines.

### 100k 级：跨机多组 / Around 100k TPS: Multi-Host Groups

每组 3 voter，组间副本集合不相交。按单组 33k 实测值线性外推：2 组 6 台约 66k，3 组 9 台约 99k，4 组 12 台约 132k。以上均
为模型估算。默认参数即可，Java 客户端也适用；在线加组边界见下节。

Use three voters per group with disjoint replica sets. Linear extrapolation from the measured 33k single-group baseline
gives about 66k for two groups/six machines, 99k for three groups/nine machines, and 132k for four groups/twelve
machines. These are modeled estimates. Default settings work, including with Java clients; online-addition limits follow
below.

超过 100k 时可继续按每新增一组三台约 33k TPS 外推，也可叠加批处理与扇出；同样需要先做多机实测。

Beyond 100k, extrapolate about 33k TPS per additional three-machine group, or add batching and fanout. Validate with
multi-host measurements first.

## 多组扩展规则 / Multi-Group Scaling Rules

- 不要同机加组；每组使用独立三机副本集合。Kubernetes 可每组一个 StatefulSet，并配置组间反亲和。<br />
  Do not add same-host groups. Use a disjoint three-machine replica set per group; on Kubernetes, use one StatefulSet per
  group with inter-group anti-affinity.
- 在线加组只适用于 legacy 动态 ConfigProvider，并且只路由新增 partition；已有 partition 数据不会自动 rebalance。Managed
  formation（SeedSpec/P1）不支持该扩容或 resharding，见 [ConfigProvider 指南](../guide/config-provider.md)。<br />
  Online group addition applies only to legacy dynamic ConfigProvider deployments and only to new partitions; existing
  partition data is not rebalanced. Managed formation (SeedSpec/P1) supports neither this expansion nor resharding; see
  the [ConfigProvider guide](../guide/config-provider.md).
- 删组前需要迁移数据，目前不支持，规划时预留容量。<br />
  Group removal requires prior data migration, which is currently unsupported; reserve capacity up front.
- 同构 managed 部署的所有节点必须共享相同 roster。<br />
  Every node in a homogeneous managed deployment must share the same roster.

## Durability 模式 / Durability Modes

| 模式 / Mode | 本地 I/O / Local I/O | 故障覆盖 / Failure Coverage | 生产建议 / Production |
| :--- | :--- | :--- | :--- |
| `strict`（默认 / default） | WAL + fsync 先于 ACK / WAL + fsync before ACK | 在存储正确持久化 fsync 的前提下，覆盖进程崩溃、单机或多数派掉电 / With correctly durable fsync, covers process crash and single-node or majority power loss | 默认 / Default |
| `replicated_only` | WAL 写入但不 fsync / WAL without fsync | 多数派同时掉电可丢已 ACK 写 / Majority power loss can lose acknowledged writes | 仅限可重建数据 / Rebuildable data only |
| `test_only` | 不写 WAL / No WAL | 不保证 / No guarantee | 仅测试 / Tests only |

Kafka `acks=all` 的持久化强度大致对应 `replicated_only`，不是 `strict`。S2 k=500 实测 strict 与 replicated_only 吞吐为
24.9k vs 26.1k TPS（2026-11-04），差约 5%。

Kafka `acks=all` roughly matches `replicated_only`, not `strict`, in durability strength. S2 k=500 measured 24.9k versus
26.1k TPS for strict and replicated_only (2026-11-04), about a 5% difference.

`--durability` 只作用于状态机侧存储引擎；每组 Raft 日志始终按 `Strict` 打开。详见
[存储引擎指南](../guide/storage-engines.md)。

`--durability` affects only the state-machine-side storage engine; every group's Raft log always opens as `Strict`. See
[Storage Engines](../guide/storage-engines.md).

## 客户端开关 / Client Knobs and Limits

| 开关 / Knob | 作用 / Effect | 限制 / Limits |
| :--- | :--- | :--- |
| `with_batching(BatchConfig)` | 多写合并为一条 Raft entry / Combines writes into one Raft entry | 仅 Rust；v0.3.0+；unkeyed 重试可重复；deferred 不兼容；keyed 写绕过 / Rust only; v0.3.0+; unkeyed retries may duplicate; incompatible with deferred; keyed writes bypass |
| `with_channel_fanout(N)` | 每地址 N 条 h2 连接轮询 / Round-robins N h2 connections per address | 仅 Rust；默认 1；N=4 后收益亚线性 / Rust only; default 1; sublinear gains beyond N=4 |
| `--grpc-max-concurrent-streams` | 配合扇出的 h2 调参 / HTTP/2 tuning for fanout | 服务端 CLI/TOML / Server CLI/TOML |
| `--grpc-max-frame-size` | h2 帧大小 / HTTP/2 frame size | 框架校验 `≤ MAX_ENTRY_BODY` / Validated against `MAX_ENTRY_BODY` |
| IoBatcher `window` | 组提交定时聚合窗口 / Timed group-commit window | 默认 0；非零窗口会引入调度粒度延迟 / Default 0; nonzero values add scheduler-granularity delay |

Java SDK 当前没有 batching 和 channel fanout，扩容使用跨机多组。见
[客户端 SDK 能力对照](../guide/client-sdk.md#能力对照--capability-comparison)。

The Java SDK currently has neither batching nor channel fanout; scale Java deployments with multi-host groups. See the
[client capability matrix](../guide/client-sdk.md#能力对照--capability-comparison).

## 成本估算 / Cost Estimation

以 4–8 vCPU + NVMe 云主机为参照，AWS m6i.xlarge 按需约 $140/月/台，仅用于数量级估算，不是当前报价。

For order-of-magnitude estimates only, assume 4–8 vCPU + NVMe instances at about $140/month each for an AWS m6i.xlarge
on-demand reference; this is not a current price quote.

| 目标 / Target | 方案 / Option | 机器 / Machines | 月成本量级 / Monthly Cost |
| :--- | :--- | ---: | ---: |
| ≤ 30k | 默认 / Default | 3 | ~$420 |
| 50k | 扇出 / Fanout | 3 | ~$420 |
| 100k | 批处理 / Batching | 3 | ~$420 |
| 50k，不改客户端 / no client change | 2 组 / 2 groups | 6 | ~$840 |
| 100k，不改客户端 / no client change | 3–4 组 / groups | 9–12 | ~$1260–1680 |

128B payload 下 31k TPS 约产生 4MB/s WAL 写入，50–100GB NVMe 可作起点；日志在快照边界清理，容量按快照保留策略规划。复制
流量约为 TPS × 128B × 2 副本，31k TPS 约 8MB/s，千兆内网足够本次估算。同机多组不会降低成本。

At 128B payloads, 31k TPS produces about 4MB/s of WAL writes; 50–100GB NVMe is a starting point. Logs are purged at
snapshot boundaries, so size for the snapshot retention policy. Replication traffic is approximately TPS × 128B × 2
replicas, about 8MB/s at 31k TPS; gigabit networking suffices for this estimate. Same-host groups do not reduce cost.

## 复现与验证 / Reproduce and Verify

```bash
cargo build --release -p rr-perf-bench -p rr-server --features rr-server/rocks -p rr-admin

./target/release/rr-perf-bench --scenario s2 --storage rocks --durability strict \
  --warmup-secs 2 --concurrency 200 --duration-secs 10 --json-out /tmp/s2.json

./target/release/rr-perf-bench --scenario s2 --storage rocks --durability strict \
  --channel-fanout 4 --warmup-secs 2 --concurrency 200 --duration-secs 10 --json-out /tmp/s2-fan4.json

./target/release/rr-perf-bench --scenario s2 --storage rocks --durability strict \
  --client-batching --batch-max-entries 64 --batch-linger-ms 1 \
  --channel-fanout 4 --warmup-secs 2 --concurrency 200 --duration-secs 10 --json-out /tmp/s2-batch.json

./target/release/rr-perf-bench --scenario s2 --storage rocks --durability strict \
  --groups 4 --channel-fanout 4 --warmup-secs 2 --concurrency 200 --duration-secs 10 --json-out /tmp/s2-g4.json
```

完整矩阵见 `scripts/perf-baseline.sh`；指标解释见 [指标参考](../reference/metrics-reference.md)；100k 路线图见
`docs/perf/roadmap-100k.md` 与 ADR A-0019。

For the full matrix, see `scripts/perf-baseline.sh`; for metric meanings, see
[Metrics Reference](../reference/metrics-reference.md); for the 100k roadmap, see `docs/perf/roadmap-100k.md` and ADR
A-0019.
