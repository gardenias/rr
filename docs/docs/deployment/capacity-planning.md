---
title: 容量规划与性能部署 / Capacity Planning & Performance Deployment
description: 按目标 TPS 选择部署拓扑与客户端开关：实测基线、多组扩展规则、durability 选择、成本估算。
---

# 容量规划与性能部署 / Capacity Planning & Performance Deployment

本文回答三个问题：rr 集群能跑多快、要达到目标 TPS 需要怎样的部署、代价是多少。
测量数字均为本机实测（Apple M3 Pro loopback，128B payload，除注明外均为 3 节点、
S2 k=200），原始 JSON 证据见 `docs/perf/baselines/`；**跨机扩展数字与成本为模型估算
（已单独标注）**，尚未有多机实测。

This page answers three questions: how fast an rr cluster can go, what deployment a
target TPS requires, and what it costs. Measured numbers come from an Apple M3 Pro
(loopback, 128B payloads, 3 nodes, S2 k=200 unless noted); raw JSON evidence lives in
`docs/perf/baselines/`. **Multi-host scaling figures and costs are modeled estimates
(labeled as such), not multi-host measurements.**

---

## 1. 性能模型 / Performance Model

rr 的写路径是 **复制到多数派 + 本地持久化 + apply**。吞吐瓶颈按优先级：

1. **宿主机 CPU**：gRPC/h2 传输 + 每请求分配/拷贝（2026-09-22 符号级采样：
   网络 syscall/h2 帧处理 ~35%、分配器 ~13%；openraft 核心仅 ~0.5%）。
2. **单 h2 连接串行点**：默认每地址一条连接，server 侧单连接任务串行处理所有帧。
   客户端 `with_channel_fanout(N)` 消除它（实测单组 33k → 62k）。
3. **组提交窗口**：IoBatcher 的定时聚合窗口曾让串行写每笔等 500µs。
   **自 2026-11-14 起默认 window=0**（`73aded6`），串行写 443 → 4831 TPS。
   非阻塞 drain 在并发下本来就在攒批，定时窗口不再需要默认开启。
4. **磁盘不是本次测量的瓶颈**：一次性存储探针（2026-11-14，单 op 批、进程内）
   测得 `RocksStorage::write_batch`（Strict，含 fsync）中位 30µs；
   该结论限于 loopback 场景。

An rr write is replicated to a majority, persisted locally, then applied. The
bottleneck order is host CPU (gRPC/h2 transport + per-request allocation/copy) >
single-connection serialization (removed by channel fanout) > the group-commit timed
window (default zero since 2026-11-14, commit `73aded6`) > disk. Disk was not the
bottleneck in these loopback runs: a one-off storage probe (2026-11-14, single-op
batches, in-process) measured a Strict `write_batch` (fsync included) at 30µs median.

串行写的 ~200µs 是 quorum RTT + apply 的硬下限；吞吐提升只有三条路：
**连接扇出、客户端批处理、多组跨机分片**（见下文各节）。

The ~200µs serial-write latency is the hard quorum-RTT + apply floor. Throughput
scales via exactly three levers: channel fanout, client batching, and multi-group
sharding across machines.

---

## 2. 实测基线 / Measured Baselines

### 单组吞吐 / Single-group throughput

| 配置 / Configuration | rocks-strict | mem |
| :--- | ---: | ---: |
| 默认（无扇出无批处理），k=200 | 30.9k TPS | 30.7k TPS |
| S1 串行（k=1） | 4,831 TPS · p50 198µs | ~4,762 TPS |
| 通道扇出 fanout=4 | 61.9k TPS · p50 2.0ms | 72.0k TPS |
| 批处理 batch64 + fanout4 | **169.3k TPS** · p50 900µs | **174.2k TPS** |

原始数据 / Raw evidence：

- 默认 strict：`raw-20261114-s2-rocks-strict.json`；mem：`raw-s2-mem.json`（2026-11-04 重采）
- S1 串行：`raw-20261114-s1-rocks-strict.json`
- fanout=4：`raw-20261114-s2-groups1-fan4-rocks-strict.json`
- 批处理：`raw-20261114-s2-batch64-fan4-rocks-strict.json`、`raw-20261104-s2-batch64-fan4-mem.json`

读路径（3 节点）：ReadLocal（可读到略旧数据）~35k TPS；线性一致读（ReadIndex）
平台期 ~19-21k TPS。取舍见 [读一致性指南](../guide/read-consistency.md)。

Read paths (3 nodes): ReadLocal ~35k TPS; linearizable reads (ReadIndex) plateau at
~19-21k TPS. Trade-offs: [Read Consistency](../guide/read-consistency.md).

### 同机多组：反模式实测 / Same-host multi-group: a measured anti-pattern

rocks-strict，3 节点同机，k=200 / rocks-strict, 3 nodes on one host, k=200：

| 组数 / Groups | fanout=1 | fanout=4 |
| :--- | ---: | ---: |
| 1 | 33.0k | 61.9k |
| 2 | 30.7k | 52.0k |
| 4 | 27.0k | 44.7k |

**同机增加组数不扩反降**：固定节点的宿主机 CPU/调度是天花板，每组都要付固定的
raft 状态机与复制开销。多组只在**跨机且副本集合不相交**时线性扩展。

Adding groups on the same hosts does NOT scale — it declines. Fixed-node host
CPU/scheduling is the ceiling, and every group pays fixed raft/replication overhead.
Multi-group scales linearly only across machines with disjoint replica sets.

---

## 3. 部署配方 / Deployment Recipes by Target TPS

### ≤ 30k TPS — 默认三节点 / Default 3-node

3 节点单组、默认参数、任意客户端（Rust/Java 均可）。无额外调优。

A single 3-node group with default settings; any client (Rust or Java). No tuning.

### ~50k TPS — 方案 C：扇出（3 台）/ Option C: fanout only (3 machines)

- 拓扑：3 节点单组；客户端 `with_channel_fanout(4)`。
- 实测：61.9k TPS（rocks-strict），余量 ~24%；p50 2.0ms。
- 要求：Rust 客户端；对业务完全透明（无批处理语义变化）。
- 成本：3 台（见 §7）。

Topology: one 3-node group with client `with_channel_fanout(4)`. Measured 61.9k TPS
(rocks-strict), ~24% headroom, p50 2.0ms. Rust client required; fully transparent to
the application. Cost: 3 machines (§7).

### 100k 级 — 方案 A：批处理+扇出（3 台）/ Option A: batching + fanout (3 machines)

- 拓扑：3 节点单组；客户端 `with_batching(batch64, linger 1ms)` + `with_channel_fanout(4)`。
- 实测：**169.3k TPS**（rocks-strict），容量余量 1.69×；p50 900µs / p99 1.5ms。
- 版本前提：**v0.3.0+；当前 v0.2.x 尚未发布批处理 API**，发布后仍默认关闭。
- 一致性前提：仅用于按业务键天然幂等的 unkeyed 写；批次重试可能**重复应用**。
  需要恰好一次语义的写必须使用 `send_request_idempotent`（绕过批处理），详见
  [客户端写批处理](../guide/client-sdk.md#写批处理--client-write-batchingopt-in)。
- 成本：3 台（见 §7）。**这是 10 万级成本最低的方案。**

Topology: one 3-node group with client `with_batching(batch64, linger 1ms)` +
`with_channel_fanout(4)`. Measured 169.3k TPS (rocks-strict) with 1.69× measured capacity;
p50 900µs / p99 1.5ms. This requires v0.3.0+; v0.2.x does NOT include the batching API.
Only use it for unkeyed writes that are idempotent by business key: a retried batch can be
applied twice. Writes needing exactly-once semantics must use `send_request_idempotent`
(which bypasses batching) — see [Client Write Batching](../guide/client-sdk.md#写批处理--client-write-batchingopt-in).
Cost: 3 machines — the cheapest way to reach 100k.

### 100k 级 — 方案 B：多组跨机（不改客户端）/ Option B: multi-group multi-host

- 拓扑：N 组 × 3 voter，**组间副本集合不相交**（每组独占 3 台）：
  - 5 万级：2 组 6 台 → ~66k TPS
  - 10 万级：3 组 9 台 → ~99k TPS；要余量则 4 组 12 台 → ~132k
  （以上为按单组 33k 实测值线性外推的**模型估算**，尚无多机实测）
- 要求：无客户端要求（Java 也可以）；默认参数。
- 成本：6/9/12 台（见 §7）。
- 扩容：见 §4 的适用边界（仅 legacy 动态 ConfigProvider 部署 + 新增分区）。

Topology: N groups × 3 voters with disjoint replica sets (each group owns 3 machines):
2 groups / 6 machines ≈ 66k; 3 groups / 9 machines ≈ 99k; 4 groups / 12 machines
≈ 132k. These are MODELED estimates extrapolating the measured 33k single-group
baseline — no multi-host measurement exists yet. Works with any client on default
settings.

### > 100k — 继续加组 / Beyond 100k

方案 B 按组线性外推：每加 1 组（3 台）≈ +33k TPS（模型估算）。也可以在跨机多组上
叠加方案 A 的客户端批处理，进一步放大每组的吞吐。

Option B extrapolates linearly (~+33k TPS per added group of 3 machines, modeled).
Option A's client batching can additionally be stacked on top of multi-group
deployments to multiply per-group throughput.

---

## 4. 多组扩展规则 / Multi-Group Scaling Rules

- **不要同机加组**：实测聚合吞吐随组数下降（§2 反模式表）。宿主机的 CPU 是共享
  天花板；组内 Mutex 不是约束（ADR A-0019 ② 复测结论）。
- **正确拓扑**：每组 3 voter，组间副本集合不相交；K8s 上每组一个 StatefulSet +
  组间反亲和（anti-affinity）调度即可表达。
- **加组（在线扩容）的适用边界**：仅 **legacy 动态 ConfigProvider** 部署支持
  "新增 group + 配置表新增分片路由"的扩容（无需数据迁移，但只适用于**新增分区**——
  已存在分区的数据不会自动 rebalance）；**managed formation（SeedSpec/P1）目前不支持
  这种动态扩容/resharding**（见 `guide/config-provider.md`）。
- **删组（缩容）**：需要先迁移该组数据——**目前不支持**，规划容量时请预留。
- 同构部署要求：同一集群所有节点共享同一 roster（formation 的 SeedSpec 校验）。

Do NOT add groups on the same hosts (measured regression). The correct topology is
3 voters per group with disjoint replica sets — on Kubernetes, one StatefulSet per
group with inter-group anti-affinity. Online group addition applies ONLY to legacy
dynamic-ConfigProvider deployments and only for NEW partitions (existing partition data
is never rebalanced); managed formation (SeedSpec/P1) does NOT currently support this
scale-out path — see `guide/config-provider.md`. Removing a group requires migrating
its data first and is NOT currently supported, so plan capacity with headroom. All
nodes of a cluster share one identical roster (formation SeedSpec validation).

---

## 5. Durability 模式选择 / Choosing a Durability Mode

| 模式 / Mode | 本地 I/O | 故障覆盖 / Failure coverage | 生产建议 / Production |
| :--- | :--- | :--- | :--- |
| `strict`（默认） | WAL + fsync 先于 ACK | 进程崩溃、单机掉电、**多数派同时掉电**均不丢已 ACK 写 | ✅ 默认 |
| `replicated_only` | WAL 写入不 fsync | 单机故障可由复制恢复；**多数派同时掉电可丢已 ACK 写** | 仅限可重建数据 |
| `test_only` | 不写 WAL | 不保证 | ❌ 仅测试 |

- 与 Kafka 对照：Kafka `acks=all`（默认）= 我们的 `replicated_only` 同级
  （复制到多数派的页缓存，应答路径不 fsync）；我们的 `strict` 契约更强。
- **性能上几乎没有理由放弃 strict**：并发下与 replicated_only 的吞吐差仅 ~5%
  （S2 k=500 实测 24.9k vs 26.1k TPS，2026-11-04）；window=0 后串行写也与 mem 同级。
- `--durability` 只作用于状态机侧存储引擎；每组 Raft 日志恒按 `Strict` 打开
  （fail-safe 设计），详见 [存储引擎指南](../guide/storage-engines.md)。

Kafka comparison: `acks=all` (default) corresponds to our `replicated_only`
(replication to the majority's page cache, no fsync on the ack path); our `strict` is
a stronger contract. There is almost no performance reason to give up `strict`:
concurrent throughput differs by only ~5%, and serial writes match the in-memory path
under the zero-window default. Note `--durability` affects the state-machine-side
engine only; every group's Raft log always opens as `Strict` — see
[Storage Engines](../guide/storage-engines.md).

---

## 6. 客户端开关与限制 / Client Knobs and Their Limits

| 开关 / Knob | 作用 / Effect | 限制 / Limits |
| :--- | :--- | :--- |
| `with_batching(BatchConfig)` | 多写合并为一条 raft entry | 仅 Rust；v0.3.0+；unkeyed 重试可重复应用；deferred 不兼容；keyed 写绕过；跨批顺序默认保持 |
| `with_channel_fanout(N)` | 每地址 N 条 h2 连接轮询，消除单连接串行点 | 仅 Rust；默认 1；N=4 收益最大（4→8 亚线性） |
| `--grpc-max-concurrent-streams` | 配合扇出的 h2 调参 | 服务端 CLI/TOML 配置 |
| `--grpc-max-frame-size` | h2 帧大小 | 框架校验 `≤ MAX_ENTRY_BODY` |
| IoBatcher `window` | 组提交定时聚合窗口 | 默认 0；**勿轻易调大**——定时器粒度 ~0.5-1ms，非零窗口按调度粒度计费 |

Java SDK 目前**两个开关都没有**（batching / channel fanout 为 Rust 独有，见
[客户端 SDK 能力对照](../guide/client-sdk.md#java-sdk)）。Java 客户端的扩容路径是
方案 B（多组跨机）。

The Java SDK has NEITHER knob today (batching and channel fanout are Rust-only —
see the [capability matrix](../guide/client-sdk.md#java-sdk)). Java clients scale via
option B (multi-group multi-host).

---

## 7. 成本估算 / Cost Estimation

以 4-8 vCPU + NVMe 云主机为参照（AWS m6i.xlarge 按需 ≈ $140/月/台，仅作量级参照；
自有 NVMe 机器无云溢价）/ Using 4-8 vCPU + NVMe instances as reference (AWS
m6i.xlarge on-demand ≈ $140/month each, order-of-magnitude only):

| 目标 / Target | 方案 / Option | 机器 / Machines | 月成本量级 / Monthly cost |
| :--- | :--- | ---: | ---: |
| ≤ 30k | 默认 | 3 | ~$420 |
| 50k | C（扇出） | 3 | ~$420 |
| 100k | A（批处理） | 3 | ~$420 |
| 50k（不改客户端） | B：2 组 | 6 | ~$840 |
| 100k（不改客户端） | B：3-4 组 | 9-12 | ~$1260-1680 |

配套资源（实测有余量）/ Supporting resources (measured headroom)：

- 磁盘：128B 下 31k TPS ≈ 4MB/s WAL 写入；50-100GB NVMe 起步（日志由快照驱动
  purge，容量按快照保留策略规划）。
- 网络：内网复制流量 ≈ TPS × 128B × 2 副本（31k TPS ≈ 8MB/s），千兆内网足够。
- 同机多组不会省钱：组数增加反而降低聚合吞吐（§2）。

Disk: at 128B payloads, 31k TPS ≈ 4MB/s WAL writes; 50-100GB NVMe is a fine start
(logs are purged on snapshot boundaries). Network: inter-node replication is
TPS × 128B × 2 replicas (~8MB/s at 31k TPS) — gigabit suffices. Same-host multi-group
saves no money: more groups on fixed hosts reduce aggregate throughput.

---

## 8. 复现与验证 / Reproduce & Verify

```bash
cargo build --release -p rr-perf-bench -p rr-server --features rr-server/rocks -p rr-admin

# 默认基线 / default baseline
./target/release/rr-perf-bench --scenario s2 --storage rocks --durability strict \
  --warmup-secs 2 --concurrency 200 --duration-secs 10 --json-out /tmp/s2.json

# 扇出 / fanout
./target/release/rr-perf-bench --scenario s2 --storage rocks --durability strict \
  --channel-fanout 4 --warmup-secs 2 --concurrency 200 --duration-secs 10 --json-out /tmp/s2-fan4.json

# 批处理+扇出 / batching + fanout
./target/release/rr-perf-bench --scenario s2 --storage rocks --durability strict \
  --client-batching --batch-max-entries 64 --batch-linger-ms 1 \
  --channel-fanout 4 --warmup-secs 2 --concurrency 200 --duration-secs 10 --json-out /tmp/s2-batch.json

# 多组（同机反模式对照）/ multi-group (same-host anti-pattern control)
./target/release/rr-perf-bench --scenario s2 --storage rocks --durability strict \
  --groups 4 --channel-fanout 4 --warmup-secs 2 --concurrency 200 --duration-secs 10 --json-out /tmp/s2-g4.json
```

更完整的基线矩阵见 `scripts/perf-baseline.sh`；指标解读见
[指标参考](../reference/metrics-reference.md)；100k 路线图与裁定见
`docs/perf/roadmap-100k.md` 与 ADR A-0019。

For the full baseline matrix see `scripts/perf-baseline.sh`; metric meanings:
[Metrics Reference](../reference/metrics-reference.md); the 100k roadmap and its
rulings: `docs/perf/roadmap-100k.md` and ADR A-0019.
