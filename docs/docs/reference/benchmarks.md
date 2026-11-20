---
sidebar_position: 5
---

# 基准测试 / Benchmarks

## 测试设施 / Test Facilities

- **端到端压测 / End-to-end load**：`tools/rr-perf-bench` 在单机启动真实 1/3/5 进程 `rr-server`
  集群，通过 `rr-client` 发送真实 gRPC 流量；本页数据来自这里。
  `tools/rr-perf-bench` starts real 1/3/5-process `rr-server` clusters on one machine and sends real gRPC
  traffic through `rr-client`; it produced the numbers on this page.
- **微基准 / Microbenchmarks**：`benches/` 使用 criterion 运行 `log_append`、`log_append_rocks`
  （需 `rocks` feature）、`codec_roundtrip`、`framing_roundtrip`、`idempotency_throughput` 等，
  隔离子系统开销。
  Criterion benchmarks in `benches/` isolate subsystem costs, including `log_append`, `log_append_rocks`
  (requires `rocks`), `codec_roundtrip`, `framing_roundtrip`, and `idempotency_throughput`.

:::warning 如何读这些数字 / How to Read These Numbers
下方 mem/loopback 表是框架开销基准，对标 [openraft minimal benchmark](https://github.com/databendlabs/openraft/tree/main/benchmarks/minimal)：
单机 loopback、空 `EchoStateMachine`、内存 Raft 日志，无业务逻辑或真实存储 I/O，只测量 gRPC + 分发 +
Raft + 内存存储链路，不是业务负载预测。同页 rocks-strict 等持久化结果不适用“无存储 I/O”限定。
openraft 官方数百万 put/s 使用进程内函数调用模拟 RPC，不能与这里的真实 gRPC 数字直接比较。
The mem/loopback tables below are framework-overhead benchmarks mirroring the [openraft minimal benchmark](https://github.com/databendlabs/openraft/tree/main/benchmarks/minimal):
single-machine loopback, an empty `EchoStateMachine`, and in-memory Raft logs, with no business logic or real
storage I/O. They measure the gRPC + dispatch + Raft + in-memory-storage path, not application workloads.
The rocks-strict and other persisted results on this page are outside that no-storage-I/O qualification.
Openraft's published millions of puts/s use in-process function calls as RPC and are not directly comparable
to these real-gRPC results.
:::

## 测试环境 / Environment

| 项 / Item | 值 / Value |
| :--- | :--- |
| 硬件 / Hardware | Apple M3 Pro, 36 GB RAM |
| 操作系统 / OS | macOS 26.6.1 |
| 工具链 / Toolchain | rustc 1.98.1, `--release` |
| 拓扑 / Topology | 单机 loopback；1/3/5 个 `rr-server` 进程 / Single-machine loopback; 1/3/5 `rr-server` processes |
| 存储 / Storage | `MemStorage`；内存 Raft 日志。stock 二进制自 v1.0.0 起支持 `--storage rocks`（ADR A-0016） / `MemStorage`; in-memory Raft log. Stock binaries also support `--storage rocks` since v1.0.0 (ADR A-0016) |
| 负载 / Load | 128B（默认）或 1KiB payload；5s 预热 + 20s 计量，短窗口复测另标 / 128B default or 1KiB payload; 5s warmup + 20s measurement unless noted |

## 场景 / Scenarios

| 场景 / Scenario | 定义 / Definition |
| :--- | :--- |
| S1 串行写 / Serial writes | 单客户端逐条 `send_request_blocking`，测量单笔提交延迟。 / One blocking client uses `send_request_blocking` to measure per-commit latency. |
| S2 并发写 / Concurrent writes | `send_request_reactive` 管线以信号量限制并发，扫描 1 → 500。 / A semaphore-bounded `send_request_reactive` pipeline sweeps concurrency from 1 to 500. |
| S3 读路径 / Read paths | 分别测量 `read`（ReadLocal）与 `read_linearizable`（ReadIndex）。 / Measures `read` (ReadLocal) and `read_linearizable` (ReadIndex). |

## 结果 / Results

采样于 2026-11-04，commit `04589df`，rustc 1.98.1，所有运行 0 错误。原始数据位于
`docs/perf/baselines/`：`raw-20261104-*.json`；canonical `raw-s1-rocks-strict.json`、
`raw-s2-{mem,rocks-strict,rocks-replicated-only,rocks-test-only}.json` 与
`cpu-samples-s2-rocks-strict.txt` 已由 `scripts/perf-baseline.sh` 原地刷新。除单独标注日期外，
loopback 表格均为此次采样。
Sampled on 2026-11-04 at commit `04589df` with rustc 1.98.1 and zero errors in every run. Raw data is in
`docs/perf/baselines/`: `raw-20261104-*.json`, the canonical `raw-s1-rocks-strict.json`,
`raw-s2-{mem,rocks-strict,rocks-replicated-only,rocks-test-only}.json`, and
`cpu-samples-s2-rocks-strict.txt`, refreshed by `scripts/perf-baseline.sh`. Loopback tables use this
capture unless a row states another date.

### S1：串行写延迟（128B）/ Serial Write Latency

| 配置 / Config | TPS | p50 | p99 | p999 | 错误 / Errors |
| :--- | ---: | ---: | ---: | ---: | ---: |
| 1 节点，gRPC / 1 node | 13.4k | 67µs | 223µs | 545µs | 0 |
| 3 节点，gRPC / 3 nodes | 4.8k | 189µs | 501µs | 1.2ms | 0 |
| 5 节点，gRPC / 5 nodes | 3.5k | 264µs | 700µs | 1.8ms | 0 |
| 3 节点，实验性 TCP Raft / 3 nodes, experimental TCP | 7.8k | 123µs | 219µs | 393µs | 0 |
| 3 节点，gRPC，1KiB payload / 3 nodes, gRPC, 1KiB payload | 5.6k | 171µs | 335µs | 546µs | 0 |
| 3 节点，gRPC，`--client-rps 2000` / 3 nodes, gRPC, `--client-rps 2000` | 2.0k | 168µs | 500µs | 102ms | 0 |

第 4–6 行是 2026-09 的独立场景，本轮未重测。quorum 从 1 → 3 → 5 节点时 p50 为
67µs → 189µs → 264µs，每增加一个复制跳点约 +75–120µs；实验性 TCP 在 2026-09 采样中比 gRPC 快约
21%（156µs → 123µs）。

Rows 4–6 are separate 2026-09 captures, not re-run in this round. Moving from 1 → 3 → 5 nodes changes p50
from 67µs → 189µs → 264µs, about +75–120µs per replication hop. Experimental TCP was about 21% faster
than gRPC in the 2026-09 capture (156µs → 123µs).

### S2：并发写吞吐（128B）/ Concurrent Write Throughput

| 并发 / Concurrency | 1 节点 TPS / 1 node | 3 节点 TPS / 3 nodes | 5 节点 TPS / 5 nodes | 3 节点 p50/p99 / 3 nodes p50/p99 | 错误 / Errors |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 10.3k | 3.6k | 2.8k | 187µs / 531µs | 0 |
| 10 | 19.7k | 16.9k | 15.5k | 429µs / 1.0ms | 0 |
| 50 | 30.3k | 28.6k | 27.7k | 1.3ms / 3.5ms | 0 |
| 100 | 32.9k | 30.6k | 30.2k | 2.5ms / 5.5ms | 0 |
| 200 | 33.9k | 30.7k | 31.5k | 5.0ms / 10.6ms | 0 |
| 500 | 33.2k | 31.0k | 31.5k | 12.6ms / 25.9ms | 0 |

并发提高后，组提交批处理（IoBatcher）主导吞吐；k≥50 时约 28–34k TPS，与集群规模基本无关，延迟随队列
增长。1KiB payload 在 k=200 时约 28.2k TPS，为同窗口 128B 的 82%
（`raw-20261104-s2-payload1024-mem.json`）。

At higher concurrency, IoBatcher group commit dominates throughput: about 28–34k TPS at k≥50, largely
independent of cluster size, while queueing raises latency. With 1KiB payloads, k=200 reaches about 28.2k
TPS, 82% of the same-window 128B result (`raw-20261104-s2-payload1024-mem.json`).

### S3：读路径（128B，3 节点）/ Read Paths (3 Nodes)

| 并发 / Concurrency | ReadLocal TPS | Linearizable TPS | Lin. p50/p99 | 错误 / Errors |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 13.2k | 5.7k | 119µs / 377µs | 0 |
| 10 | 27.9k | 16.1k | 449µs / 1.1ms | 0 |
| 50 | 32.9k | 21.1k | 1.8ms / 4.3ms | 0 |
| 100 | 34.3k | 20.5k | 3.7ms / 8.4ms | 0 |
| 200 | 35.0k | 19.3k | 7.8ms / 18.1ms | 0 |
| 500 | 33.8k | 19.9k | 19.3ms / 39.4ms | 0 |

ReadLocal 可能读到略旧数据，吞吐约 34–35k；线性一致读受 ReadIndex quorum 往返限制，3 节点平台期约
19–21k TPS。5 节点约 14k TPS 为 2026-09 数据，本轮未重测。取舍见[读一致性指南](../guide/read-consistency.md)。
ReadLocal can return slightly stale data and reaches about 34–35k TPS. Linearizable reads are bounded by
ReadIndex quorum round trips, plateauing around 19–21k TPS on 3 nodes. The roughly 14k TPS 5-node figure is
from 2026-09 and was not re-run. See the [read-consistency guide](../guide/read-consistency.md).

### 限流保护 / Rate-Limit Protection

`--client-rps 2000`（每节点）下，超额请求收到 `RESOURCE_EXHAUSTED`，SDK 指数退避重试。写吞吐收敛到约
1.4–1.5k TPS（写均打到 Leader）；低并发延迟正常，超限后 p95 出现约 100ms 退避尾巴，并发 ≥50 才出现
重试耗尽错误。持续超载下集群保持稳定。
With `--client-rps 2000` per node, excess requests receive `RESOURCE_EXHAUSTED` and the SDK retries with
exponential backoff. Writes converge to about 1.4–1.5k TPS because all writes hit the leader. Latency is
normal below the limit; p95 gains an approximately 100ms backoff tail above it, and retry exhaustion starts
at concurrency ≥50. The cluster remains stable under sustained overload.

### 实验性 TCP Raft 传输 / Experimental TCP Raft Transport

写入与 ReadLocal 正常（2026-09：S1 p50=123µs，S2 峰值 31.9k TPS；本轮未重测）。
Writes and ReadLocal are healthy over TCP (2026-09: S1 p50=123µs, S2 peak 31.9k TPS; not re-run).

:::warning 已知问题与使用边界 / Known Issue and Usage Boundary
TCP 下线性一致读的 follower→leader 重定向在并发 ≥10 时不稳定，约 1–2% 请求重定向后不收敛并报
`follower redirected to leader`；gRPC 无此问题。TCP 是强制 loopback 的明文实验特性，不建议生产使用。
With TCP, follower→leader redirects for linearizable reads are unstable at concurrency ≥10: about 1–2% of
requests fail to converge and report `follower redirected to leader`. gRPC is unaffected. TCP is a
plaintext, loopback-only experimental feature and is not recommended for production.
:::

### 压测暴露的修复 / Fixes Found by Benchmarks

以下缺陷已修复，上述数字均为修复后复测 / These defects were fixed; the numbers above are post-fix:

1. **客户端每请求新建 TCP 连接 / New TCP connection per request**：`connect_to_addr` 绕过连接池，
   并发 ≥10 时耗尽 loopback 端口或 backlog。现按地址缓存 H2 channel，在 `Unavailable` 时驱逐重连；
   500 并发全场景零错误。
   `connect_to_addr` bypassed pooling and exhausted loopback ports or backlogs at concurrency ≥10. Channels
   are now cached by address and evicted on `Unavailable`; all scenarios at concurrency 500 have zero errors.
2. **stock `rr-server` 读 RPC 恒失败 / Stock reads always failed**：`EchoStateMachine` 未实现
   `read_handle`；补齐后 `ReadLocal`/`ReadLinearizable` 可用于冒烟服务。
   `EchoStateMachine` lacked `read_handle`; implementing it enabled `ReadLocal` and `ReadLinearizable` for the smoke service.
3. **`submit_read` 失败无服务端日志 / Failed `submit_read` calls had no server log**：现与写路径一致，记录脱敏 ERROR。
   `submit_read` failures now emit sanitized ERROR logs, matching the write path.

### 容器网络基线 / Container-Network Baseline (Lab)

同套场景在 OrbStack 容器网络中运行 3 节点 kv-store，使用真实 KV 状态机；负载由 `rr-lab:dev` 容器发起，
见[本地实验环境](../operations/local-lab.md)。
The same scenarios ran against a 3-node kv-store with a real KV state machine on an OrbStack container
network. The `rr-lab:dev` container generated load; see the [local lab guide](../operations/local-lab.md).

| 场景 / Scenario | 并发 / Conc. | TPS | p50 | p99 | 错误 / Errors |
| :--- | ---: | ---: | ---: | ---: | ---: |
| S1 串行写 / Serial write | 1 | 3.8k | 253µs | 468µs | 0 |
| S2 并发写 / Write sweep | 100 | 38.8k | 2.0ms | 3.6ms | 0 |
| S2 并发写 / Write sweep | 500 | 40.7k | 8.9ms | 20.7ms | 0 |
| S3 ReadLocal | 500 | 52.8k | 5.8ms | 24.1ms | 0 |
| S3 Linearizable | 500 | 17.8k | 21.3ms | 48.8ms | 0 |

差异来自容器网桥与真实 kv-store 状态机等环境因素，不是单一变量；不要跨环境直接比较绝对值。
Differences come from the environment, including the container bridge and real kv-store state machine, not
one isolated variable. Do not compare absolute values across environments.

## 复现 / Reproduce

```bash
# 构建一次 / Build once
cargo build --release -p rr-server -p rr-admin -p rr-perf-bench

# 默认全场景（S1+S2+S3），3 节点 / Default: all scenarios, 3 nodes
./target/release/rr-perf-bench

# 常用参数 / Common options
./target/release/rr-perf-bench --members 5                 # 集群规模 / cluster size (1|3|5)
./target/release/rr-perf-bench --scenario s1               # 仅串行写 / serial writes only
./target/release/rr-perf-bench --payload 1024              # payload 字节数 / payload bytes
./target/release/rr-perf-bench --use-tcp-transport         # 实验性 TCP / experimental TCP
./target/release/rr-perf-bench --client-rps 2000           # 服务端限流 / server-side rate limit
./target/release/rr-perf-bench --groups 4                  # 4 个 Raft 组，按分片轮转 / 4 groups, round-robin partitions
./target/release/rr-perf-bench --concurrency 1,10,100 --duration-secs 20

# 微基准 / Microbenchmarks
cargo bench -p rr-benches
cargo bench -p rr-benches --features rocks                 # 含 RocksDB / include RocksDB
```

### 多组与连接扇出 / Multi-Group and Connection Fanout

2026-11-04 复测：M3 Pro，3 节点 loopback，mem/strict，128B，k=200，2s 预热 + 10s 窗口；原始文件为
`raw-20261104-s2-groups{1,2,4}-fan{1,4}-mem.json`。

Retested on 2026-11-04: M3 Pro, 3-node loopback, mem/strict, 128B, k=200, 2s warmup + 10s window. Raw
files: `raw-20261104-s2-groups{1,2,4}-fan{1,4}-mem.json`.

| 组数 \ `--channel-fanout` / Groups | 1 | 4 |
| :--- | ---: | ---: |
| 1 | 34.3k | 72.0k |
| 2 | 34.3k | 63.8k |
| 4 | 33.0k | 61.2k |

多组聚合吞吐仍不随组数增长。fanout=4 将单组从 34.3k 提至 72.0k（2.1×，与 2026-09-23 的 1.91×
一致），但 1→4 组聚合从 72.0k 降至 61.2k（−15%），p50 保持约 2.0ms。单组即可达 72k，说明绑定约束不是
`group_manager.rs` 的 per-group Mutex，而是固定 3 节点的宿主 CPU/调度与每组 raft 核、心跳、apply 开销。
Aggregate throughput still does not scale with group count. Fanout=4 raises one group from 34.3k to 72.0k
(2.1×, consistent with 1.91× on 2026-09-23), but aggregate throughput falls from 72.0k to 61.2k (−15%)
from 1→4 groups while p50 stays near 2.0ms. Since one group reaches 72k, the per-group Mutex in
`group_manager.rs` is not the binding constraint; host CPU/scheduling and per-group Raft, heartbeat, and
apply costs on the fixed 3 nodes are.

multi-Raft 扩展前提仍是副本集合不相交并增加机器；容量按“每组吞吐 ≈ 单组基线，组数随节点池扩展”建模。
同窗口 opt-in 批处理为 batch64+fan1 = 168.3k、batch64+fan4 = 174.2k（p50 ≈0.8ms；
`raw-20261104-s2-batch64-fan{1,4}-mem.json`），批处理已摊薄连接串行点，fanout 仅增加 3.5%。
The multi-Raft scaling premise remains disjoint replica sets across a growing node pool. Model capacity as
per-group TPS near the single-group baseline, with group count scaling with the node pool. Same-window
opt-in batching reached 168.3k with batch64+fan1 and 174.2k with batch64+fan4 (p50 ≈0.8ms;
`raw-20261104-s2-batch64-fan{1,4}-mem.json`); batching already amortizes connection serialization, so
fanout adds only 3.5%.

更多参数见 `./target/release/rr-perf-bench --help`；用 `RUST_LOG=rr_client=debug` 开启客户端重试与重定向
诊断。
See `./target/release/rr-perf-bench --help` for more options. Set `RUST_LOG=rr_client=debug` for client
retry and redirect diagnostics.

## Open-Loop 场景 / Open-Loop Scenarios (`mix` / `wladder`)

S1/S2/S3 是 closed-loop，响应驱动下一请求，会因协调遗漏（coordinated omission）系统性低估尾延迟。
`--scenario mix|wladder` 按计划时刻发起请求（恒定网格或 `--poisson` 指数到达），分别输出计划→完成、
实际发起→完成、计划→实际发起（`lag`）三组分布；驱动端跟不上计划时 `lag` 会暴露它。
S1/S2/S3 are closed-loop: each response drives the next request, so coordinated omission systematically
underestimates tail latency. `--scenario mix|wladder` schedules requests on a fixed grid or with `--poisson`
exponential arrivals. It reports scheduled-to-complete, actual-start-to-complete, and scheduled-to-start
(`lag`) distributions; `lag` exposes a load generator that cannot keep up.

Open-loop 使用 kv-store KV 线协议（bincode），应指向 kv-store 集群（如 `deploy/perf/` perf-lab），不要指向
stock `rr-server`（EchoSM）。写使用 plain `send_request`，不带幂等键；并发在途写会乱序并触发 watermark
过期语义，keyed 路径由 soak 与 Kafka ingestor 覆盖。
Open-loop scenarios use the kv-store bincode wire protocol and must target a kv-store cluster such as the
`deploy/perf/` perf-lab, not stock `rr-server` (EchoSM). Writes use plain `send_request` without idempotency
keys; concurrent in-flight writes can reorder and trigger watermark-expiry semantics. Soak scenarios and the
Kafka ingestor cover keyed paths.

```bash
# 定速混合：5k ops/s，90/10 写/本地读，zipfian 热点，1s 时间序列
# Fixed-rate mix: 5k ops/s, 90/10 writes/local reads, Zipfian keys, 1s time series
rr-perf-bench --cluster-addrs kv-1:51001,kv-2:51001,kv-3:51001 \
  --scenario mix --rate 5000 --mix write:90,local:10 \
  --key-dist zipfian --key-space 100000 --duration-secs 60 \
  --timeseries-out ts.jsonl --json-out mix.json

# W1 阶梯：每档持续 --duration-secs / Step-up ladder: each rung lasts --duration-secs
rr-perf-bench --cluster-addrs ... --scenario wladder --ladder-rates 100,500,1000,2000,4000

# TLS/mTLS + token / Secure client
rr-perf-bench --cluster-addrs ... --scenario mix \
  --tls-ca ca.pem --tls-cert client.pem --tls-key client.key --token-env RR_CLIENT_TOKEN
```

所有随机源由 `--seed` 化的 SplitMix64 驱动；相同 seed 与 flags 复现相同负载轨迹。`--client-id` 用于多负载
容器扇出后的 JSON meta 聚合。
A seeded SplitMix64 drives every random source; the same seed and flags reproduce the same workload.
`--client-id` supports JSON metadata aggregation when multiple load containers fan out.

`--prom-url <url>` 将与客户端窗口墙钟对齐的服务端汇总嵌入 JSON：server/proposal/apply/append-RTT
p50/p99、复制 lag、ingestor 提交速率。Prometheus 不可达时字段为 `null`，不会拖垮运行。
`--op-timeout-secs`（默认 30）限制每个 op，使饱和集群上的运行有界收尾。
`--prom-url <url>` embeds server summaries aligned to the client wall-clock window:
server/proposal/apply/append-RTT p50/p99, replication lag, and ingestor commit rate. If Prometheus is
unreachable, fields become `null` without failing the run. `--op-timeout-secs` (default 30) bounds each
operation so runs finish even on a saturated cluster.

生产形态矩阵（W1–W9 + W11 对照档）由 `scripts/prod-perf.sh` 执行，见 `deploy/perf/README.md`；产物归档到
`docs/perf/prod/<date>-<sha>/`（raw JSON、时间序列与 report.md）。
`scripts/prod-perf.sh` runs the production-shape matrix (W1–W9 plus the W11 control); see
`deploy/perf/README.md`. Artifacts are archived in `docs/perf/prod/<date>-<sha>/` as raw JSON, time series,
and report.md.

## 当前限制 / Current Limitations

- **单机 loopback / Single-machine loopback**：跨机器 RTT 会主导真实写延迟，Raft 写通常为 2–3 个 RTT。
  Cross-machine RTT dominates real write latency; a Raft write typically costs 2–3 RTTs.
- **空状态机 / Empty state machine**：无业务逻辑、反序列化或快照流量。
  No business logic, deserialization, or snapshot traffic.
- **loopback 表使用内存 Raft 日志 / In-memory Raft log in loopback tables**：`--storage mem`。自 ADR
  A-0016 起，`--storage rocks` 同时持久化 Raft 日志到 `{data_dir}/raft-log/{group_id}`，见
  [持久化与恢复](../architecture/durability.md)。因此 2026-11-04 的 `raw-s2-rocks-*.json` 与 A-0016 前
  同名 raw 不可直接比较。生产形态数字使用 `deploy/perf/` perf-lab 与 open-loop；存储层开销使用 criterion。

  The loopback tables use `--storage mem`. Since ADR A-0016, `--storage rocks` also persists Raft logs at
  `{data_dir}/raft-log/{group_id}`; see [Durability & Recovery](../architecture/durability.md). The
  2026-11-04 `raw-s2-rocks-*.json` matrix is not directly comparable to pre-A-0016 files with the same
  names. Use `deploy/perf/` perf-lab and open-loop for production-shape numbers, and criterion for
  storage-layer costs.
- **无 Jepsen 式正确性压测 / No Jepsen-style correctness testing**：本页只覆盖性能；故障下一致性验证见
  `tests/integration/`（含 chaos）。
  This page covers performance only; fault correctness tests live in `tests/integration/`, including chaos.

## 提交基准结果 / Contributing Benchmark Results

通过 PR 提交结果时，请附硬件、OS、Rust 版本和完整命令行；本页表格均可由上述命令复现。
When submitting results in a PR, include hardware, OS, Rust version, and the full command line. Every table
on this page is reproducible with the commands above.

按目标 TPS 选择部署拓扑与客户端开关，见[容量规划与性能部署](../deployment/capacity-planning.md)。
For deployment topologies and client options by target TPS, see
[Capacity Planning & Performance Deployment](../deployment/capacity-planning.md).
