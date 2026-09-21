---
sidebar_position: 5
---

# 基准测试 / Benchmarks

## 概览 / Overview

本项目有两层基准测试设施：

- **端到端压测（`tools/rr-perf-bench`）**：在单机上拉起真实的 1/3/5 进程 `rr-server` 集群，通过 `rr-client` SDK 打真实 gRPC 流量。这是本页数据的来源。
- **存储/编解码微基准（`benches/`，criterion）**：`log_append`、`log_append_rocks`（需 `rocks` feature）、`codec_roundtrip`、`framing_roundtrip`、`idempotency_throughput` 等，用于隔离子系统开销。

:::warning 如何读这些数字 / How to read these numbers
以下数据是**框架开销基准**（对标 [openraft 的 minimal benchmark](https://github.com/databendlabs/openraft/tree/main/benchmarks/minimal)）：跑在单机 loopback 上，状态机是空的 `EchoStateMachine`（回显入参，无业务逻辑、无真实存储 I/O），因此数字反映的是「gRPC + 分发 + Raft 共识 + 内存存储」这条链路本身的开销，**不是**真实业务负载的预测值。openraft 官方数字（数百万 put/s）使用进程内函数调用模拟 RPC，与本页的真实 gRPC 数字不可直接比较。

The numbers below are **framework-overhead benchmarks** (mirroring openraft's minimal benchmark): single-machine loopback, an empty `EchoStateMachine` (echoes input; no business logic, no real storage I/O). They measure the cost of the gRPC + dispatch + Raft consensus + in-memory storage path itself — they are **not** predictions for real workloads. openraft's official numbers (millions of puts/s) use in-process function calls as RPC and are not directly comparable to the real-gRPC numbers here.
:::

## 测试环境 / Environment

| 项 / Item | 值 / Value |
|---|---|
| 硬件 / Hardware | Apple M3 Pro, 36 GB RAM |
| 操作系统 / OS | macOS 26.6.1 |
| 工具链 / Toolchain | rustc 1.98.0, `--release` |
| 拓扑 / Topology | 单机 loopback；1/3/5 个 `rr-server` 进程 |
| 存储 / Storage | `MemStorage`（内存 raft 日志；stock 二进制尚未接线 RocksDB 日志存储） |
| 负载 / Load | 128B（默认）或 1KiB payload；4s 预热 + 10s 计量窗口 |

## 场景定义 / Scenarios

- **S1 串行写 / Serial writes**：单客户端逐条阻塞写入（`send_request_blocking`），测量每笔提交的最低延迟。
- **S2 并发写扫描 / Concurrent write sweep**：信号量封顶的 reactive 管线（`send_request_reactive`），并发度 1 → 500，测量吞吐/延迟曲线。
- **S3 读路径扫描 / Read-path sweep**：同一管线分别打 `read`（ReadLocal，本地副本读）与 `read_linearizable`（ReadIndex 线性一致读）。

## 结果 / Results

### S1：串行写延迟（128B）/ Serial write latency

集群规模与传输对**单笔写延迟**的影响：

| 配置 / Config | TPS | p50 | p99 | p999 | 错误 / Errors |
|---|---|---|---|---|---|
| 1 节点， gRPC | 14.6k | 67µs | 94µs | 197µs | 0 |
| 3 节点， gRPC | 6.1k | 156µs | 260µs | 572µs | 0 |
| 5 节点， gRPC | 4.5k | 216µs | 371µs | 697µs | 0 |
| 3 节点， TCP raft 传输（实验性） | 7.8k | 123µs | 219µs | 393µs | 0 |
| 3 节点， gRPC，1KiB payload | 5.6k | 171µs | 335µs | 546µs | 0 |
| 3 节点， gRPC，`--client-rps 2000` | 2.0k | 168µs | 500µs | 102ms | 0 |

要点：串行延迟由 quorum 规模决定（1 → 3 → 5 节点：67µs → 156µs → 216µs，每加一个复制跳点约 +60-90µs）；实验性 TCP raft 传输比 gRPC 快约 21%（156µs → 123µs）。

### S2：并发写吞吐扫描（128B）/ Concurrent write throughput

| 并发 / Concurrency | 1 节点 TPS | 3 节点 TPS | 5 节点 TPS | 3 节点 p50/p99 | 错误 / Errors |
|---|---|---|---|---|---|
| 1 | 10.1k | 4.4k | 3.2k | 151µs / 232µs | 0 |
| 10 | 19.9k | 18.4k | 15.9k | 367µs / 650µs | 0 |
| 50 | 26.6k | 28.3k | 25.9k | 1.2ms / 2.2ms | 0 |
| 100 | 30.3k | 30.1k | 29.2k | 2.3ms / 4.1ms | 0 |
| 200 | 31.1k | 31.5k | 30.1k | 4.5ms / 7.4ms | 0 |
| 500 | 31.0k | 32.1k | 31.3k | 11.1ms / 16.9ms | 0 |

要点：并发起来后吞吐由**组提交批处理**（IoBatcher）主导，与集群规模基本无关（≈31-32k TPS）；延迟随并发线性增长（排队的正常表现）。1KiB payload 时峰值约 26.2k TPS（−18%）。

---

### S3：读路径（128B，3 节点）/ Read paths

| 并发 / Concurrency | ReadLocal TPS | Linearizable TPS | Lin. p50/p99 | 错误 / Errors |
|---|---|---|---|---|
| 1 | 12.3k | 5.9k | 117µs / 150µs | 0 |
| 10 | 27.2k | 15.1k | 452µs / 799µs | 0 |
| 50 | 30.4k | 18.9k | 1.8ms / 3.4ms | 0 |
| 100 | 32.3k | 19.3k | 3.6ms / 6.2ms | 0 |
| 200 | 33.5k | 19.0k | 7.5ms / 12.4ms | 0 |
| 500 | 33.2k | 19.2k | 18.7ms / 29.4ms | 0 |

要点：ReadLocal（可能读到略旧数据）与写吞吐持平（≈33k）；线性一致读受 ReadIndex quorum 往返限制，3 节点平台期 ≈19k TPS、5 节点 ≈14k TPS。两者的取舍见 [读一致性指南](../guide/read-consistency.md)。

### 限流自我保护 / Rate-limit self-protection

`--client-rps 2000`（每节点）下，超出限额的请求收到 `RESOURCE_EXHAUSTED`，SDK 按指数退避重试：写吞吐收敛到 ≈1.4-1.5k TPS（写全部打到 leader，受 leader 单节点限额约束），低并发时延迟保持正常，超限时 p95 出现 ≈100ms 的退避尾巴，并发 ≥50 才开始出现重试耗尽的错误。集群在持续超载下保持稳定、无崩溃——与未限流时的行为对比见下文「压测驱动的修复」。

Under `--client-rps 2000` per node, excess requests get `RESOURCE_EXHAUSTED` and the SDK retries with exponential backoff: write throughput converges to ≈1.4-1.5k TPS (all writes hit the leader's limiter), latency stays normal below the limit, ~100ms backoff tails appear once over it, and retry-exhaustion errors only appear at concurrency ≥50. The cluster stays stable under sustained overload.

### 实验性 TCP raft 传输 / Experimental TCP raft transport

写入与 ReadLocal 在 TCP 传输下正常（S1 p50=123µs，S2 峰值 31.9k TPS）。**已知问题**：TCP 传输下线性一致读的 follower→leader 重定向在并发 ≥10 时不稳定（约 1-2% 的请求重定向后不收敛，报 `follower redirected to leader`；gRPC 传输无此问题）。TCP 传输为明文实验特性（强制 loopback），不建议生产使用。

Writes and ReadLocal are healthy over the TCP transport (S1 p50=123µs, S2 peak 31.9k TPS). **Known issue**: with the TCP transport, follower→leader redirects for linearizable reads are unstable at concurrency ≥10 (~1-2% of requests fail to converge after redirect; the gRPC transport is unaffected). The TCP transport is a plaintext experimental feature (loopback-only) and not production-recommended.

## 压测驱动的修复 / Benchmark-driven fixes

这轮压测暴露并已修复三个真实缺陷（数字是修复后复测的）：

This benchmark round exposed — and fixed — three real defects (all numbers above are post-fix):

1. **客户端每请求新建 TCP 连接**（`rr-client` 的 `connect_to_addr` 绕过连接池）：并发 ≥10 时 loopback 端口耗尽/listen backlog 打满，读路径 98% 失败、写路径大面积错误，甚至拖垮 leader 心跳导致集群震荡。修复：按地址缓存 H2 channel，`Unavailable` 时驱逐重连。修复后 500 并发全场景零错误。
2. **stock `rr-server` 的读 RPC 恒失败**：`EchoStateMachine` 未实现 `read_handle`。修复后 `ReadLocal`/`ReadLinearizable` 对冒烟服务可用。
3. **`submit_read` 失败无服务端日志**：现已与写路径对齐（脱敏 ERROR 日志）。

## 复现 / Reproduce

### 容器网络环境（lab 基线）/ Container-network baseline (lab)

同一套场景在 OrbStack 容器网络中跑 3 节点 kv-store（真实 KV 状态机；负载经 `rr-lab:dev` 容器在网内发起，见 [本地实验环境](../operations/local-lab.md)）：

| 场景 / Scenario | 并发 / Conc. | TPS | p50 | p99 | 错误 / Errors |
|---|---|---|---|---|---|
| S1 串行写 / serial write | 1 | 3.8k | 253µs | 468µs | 0 |
| S2 并发写 / write sweep | 100 | 38.8k | 2.0ms | 3.6ms | 0 |
| S2 并发写 / write sweep | 500 | 40.7k | 8.9ms | 20.7ms | 0 |
| S3 ReadLocal | 500 | 52.8k | 5.8ms | 24.1ms | 0 |
| S3 Linearizable | 500 | 17.8k | 21.3ms | 48.8ms | 0 |

与本机 loopback 数字的差异来自环境（容器网桥 + kv-store 真实状态机 vs stock echo）而非单一变量；两组都如实呈现，不要跨环境直接比绝对值。

Numbers differ from the loopback tables due to environment (container bridge + kv-store's real state machine vs stock echo) rather than a single variable — both are presented as-is; do not compare absolute values across environments.

## 复现 / Reproduce

```bash
# 构建（一次）/ Build once
cargo build --release -p rr-server -p rr-admin -p rr-perf-bench

# 默认全场景（S1+S2+S3），3 节点 / Default: all scenarios, 3 nodes
./target/release/rr-perf-bench

# 常用维度 / Common knobs
./target/release/rr-perf-bench --members 5                 # 集群规模 / cluster size (1|3|5)
./target/release/rr-perf-bench --scenario s1               # 只跑串行写 / serial writes only
./target/release/rr-perf-bench --payload 1024              # payload 字节 / payload size
./target/release/rr-perf-bench --use-tcp-transport         # 实验性 TCP raft 传输 / experimental TCP raft transport
./target/release/rr-perf-bench --client-rps 2000           # 服务端限流 / server-side rate limit
./target/release/rr-perf-bench --groups 4                  # 多组扩展:4 个 Raft 组,S1/S2/S3 按分片轮转 / multi-group: 4 Raft groups, ops round-robin across partitions
./target/release/rr-perf-bench --concurrency 1,10,100 --duration-secs 20

# 微基准 / Micro-benchmarks
cargo bench -p rr-benches                                  # 内存存储/编解码 / in-memory & codec
cargo bench -p rr-benches --features rocks                 # 含 RocksDB 存储层 / incl. RocksDB layer
```

> **多组扩展实测(2026-09-22, M3 Pro 3 节点 loopback, mem/strict, 128B)**:`--groups 1/2/4`
> 在 k=100/200 下聚合吞吐均为 ~26k TPS——**持平而非增长**。原因:stock 拓扑里每个组都复制到全部
> 3 个节点,单机 CPU 已饱和(见 v0.2.0 基线 Finding 4),加组不产生新 CPU。multi-raft 的扩展性
> 前提是**组的副本集合不相交 + 加机器**(如 3 组 × 3 副本部署在 9 台主机);在固定 3 节点上堆组
> 只是换来更多上下文切换。容量规划请按"每组吞吐 ≈ 单组基线,组数随节点池扩展"建模。
>
> Measured 2026-09-22 (M3 Pro, 3-node loopback, mem/strict, 128B): aggregate TPS stays flat
> (~26k) for `--groups 1/2/4` at k=100/200. With the stock topology every group replicates to
> all 3 nodes and the host CPU is already saturated (baseline Finding 4) — more groups add no
> CPU. Multi-raft scaling assumes **disjoint replica sets over a growing node pool**; piling
> groups onto fixed nodes only buys context-switching.

更多参数见 `./target/release/rr-perf-bench --help`。压测期间的客户端重试/重定向诊断可用 `RUST_LOG=rr_client=debug` 打开。

## Open-loop 场景（mix / wladder）

S1/S2/S3 是 closed-loop(响应驱动下一请求),会系统性低估尾延迟(协调遗漏 / coordinated omission)。`--scenario mix|wladder` 是 open-loop 驱动:请求按**计划时刻**发起(恒定网格或 `--poisson` 指数到达),延迟按 计划→完成 计量,并同时输出 实际发起→完成(服务时间)与 计划→实际发起(调度滞后,`lag`)三组分布——驱动端跟不上计划时速率时 `lag` 会如实暴露。

Open-loop 场景使用 **kv-store KV 线协议**(bincode),应指向 kv-store 集群(如 `deploy/perf/` perf-lab),不要对 stock rr-server(EchoSM)使用。写操作走 plain `send_request`(无幂等键——并发在途写入天然乱序,会触发幂等层的 watermark 过期语义;keyed 路径由 soak 场景与 Kafka ingestor 覆盖)。

```bash
# 定速混合负载:5k ops/s,90/10 写/本地读,zipfian 热点 key,1s 时间序列 / fixed-rate mix
rr-perf-bench --cluster-addrs kv-1:51001,kv-2:51001,kv-3:51001 \
  --scenario mix --rate 5000 --mix write:90,local:10 \
  --key-dist zipfian --key-space 100000 --duration-secs 60 \
  --timeseries-out ts.jsonl --json-out mix.json

# W1 阶梯:速率递增至饱和 / step-up ladder (rungs × --duration-secs each)
rr-perf-bench --cluster-addrs ... --scenario wladder --ladder-rates 100,500,1000,2000,4000

# TLS/mTLS + token(perf-lab 形态)/ secure client
rr-perf-bench --cluster-addrs ... --scenario mix \
  --tls-ca ca.pem --tls-cert client.pem --tls-key client.key --token-env RR_CLIENT_TOKEN
```

全部随机源(到达过程、op 选择、key 选择)来自 `--seed` 化的 SplitMix64:同 seed 同 flag 复现同一负载轨迹。`--client-id` 供多负载容器扇出时在 JSON meta 聚合。

`--prom-url <url>` 会把**服务端窗口汇总**嵌入 JSON 报告(与客户端窗口同墙钟对齐:server/proposal/apply/append-RTT p50/p99、复制 lag、ingestor 提交速率),用于把客户端毛刺归因到服务端环节;Prometheus 不可达时字段降级为 null,不会拖垮 run。`--op-timeout-secs`(默认 30)给每个 op 上期限,饱和集群上 run 必然有界收尾。

生产形态全矩阵(W1–W9 + W11 对照档)由 `scripts/prod-perf.sh` 一键执行(见 `deploy/perf/README.md`),产出归档 `docs/perf/prod/<date>-<sha>/`(raw JSON + 时间序列 + report.md)。

## 当前限制 / Current Limitations

- **单机 loopback**：跨机器网络 RTT 会主导真实部署的写延迟（Raft 写通常 2-3 个 RTT）。
- **空状态机**：无业务逻辑、无反序列化成本、无快照流量。
- **上述 loopback 表格为内存 Raft 日志**：基线采集早于 ADR A-0016——此后 `--storage rocks` 会同时持久化 Raft 日志（`{data_dir}/raft-log/{group_id}`，见 [持久化与恢复](../architecture/durability.md)）；生产形态数字（durable log + TLS/mTLS + 负载机隔离）请用 `deploy/perf/` perf-lab 与上文 open-loop 场景；存储层开销请用 `benches/` 的 criterion 微基准测量。
- **无 Jepsen 式正确性压测**：本页只覆盖性能；一致性验证见 `tests/integration/`（含 chaos 用例）。

---

- **Single-machine loopback**: cross-machine network RTT dominates real deployments (a Raft write typically costs 2-3 RTTs).
- **Empty state machine**: no business logic, no deserialization cost, no snapshot traffic.
- **In-memory Raft log in these tables**: the loopback baselines above predate ADR A-0016 — since then `--storage rocks` also persists the Raft log (`{data_dir}/raft-log/{group_id}`, see [Durability & Recovery](../architecture/durability.md)). For production-shape numbers (durable log + TLS/mTLS + isolated load generator) use the perf-lab in `deploy/perf/` with the open-loop scenarios above; use the criterion micro-benches in `benches/` for storage-layer numbers.
- **No Jepsen-style correctness testing**: this page covers performance only; correctness under faults lives in `tests/integration/` (including chaos cases).

## 贡献基准测试 / Contributing Benchmarks

如果您运行了基准测试，请通过 Pull Request 提交。请在报告中包含硬件规格、操作系统、Rust 版本与完整的命令行参数（本页所有表格都可通过上面的命令复现）。

---

If you run benchmarks, please open a PR. Include hardware specs, OS, Rust version, and the full command line (every table on this page is reproducible with the commands above).
