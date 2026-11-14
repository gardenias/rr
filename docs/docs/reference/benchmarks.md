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
| 工具链 / Toolchain | rustc 1.98.1, `--release` |
| 拓扑 / Topology | 单机 loopback；1/3/5 个 `rr-server` 进程 |
| 存储 / Storage | `MemStorage`（本基线使用内存 raft 日志；stock 二进制自 v0.3.0 起亦支持 `--storage rocks` 持久化日志——见 ADR A-0016） |
| 负载 / Load | 128B（默认）或 1KiB payload；5s 预热 + 20s 计量窗口（短窗口复测单独标注） |

## 场景定义 / Scenarios

- **S1 串行写 / Serial writes**：单客户端逐条阻塞写入（`send_request_blocking`），测量每笔提交的最低延迟。
- **S2 并发写扫描 / Concurrent write sweep**：信号量封顶的 reactive 管线（`send_request_reactive`），并发度 1 → 500，测量吞吐/延迟曲线。
- **S3 读路径扫描 / Read-path sweep**：同一管线分别打 `read`（ReadLocal，本地副本读）与 `read_linearizable`（ReadIndex 线性一致读）。

## 结果 / Results

> **采样 / Sampled**：2026-11-04，commit `04589df`，rustc 1.98.1，全部运行 0 错误。原始数据见
> `docs/perf/baselines/`：`raw-20261104-*.json`（本轮新采）；`raw-s1-rocks-strict.json`、
> `raw-s2-{mem,rocks-strict,rocks-replicated-only,rocks-test-only}.json` 与
> `cpu-samples-s2-rocks-strict.txt`（`scripts/perf-baseline.sh` 的 canonical 输出，本轮已原地刷新）。
> 除单独标注日期的行外，本页 loopback 表格均为当日数据。
>
> Re-sampled 2026-11-04 at commit `04589df`, rustc 1.98.1, zero errors in every run. Raw data in
> `docs/perf/baselines/`: `raw-20261104-*.json` (this round); the canonical `raw-s1-rocks-strict.json` /
> `raw-s2-*.json` and `cpu-samples-s2-rocks-strict.txt` (refreshed in place by `scripts/perf-baseline.sh`).
> Unless a row carries its own date, every loopback table on this page is from this capture.

### S1：串行写延迟（128B）/ Serial write latency

集群规模与传输对**单笔写延迟**的影响：

| 配置 / Config | TPS | p50 | p99 | p999 | 错误 / Errors |
|---|---|---|---|---|---|
| 1 节点， gRPC | 13.4k | 67µs | 223µs | 545µs | 0 |
| 3 节点， gRPC | 4.8k | 189µs | 501µs | 1.2ms | 0 |
| 5 节点， gRPC | 3.5k | 264µs | 700µs | 1.8ms | 0 |
| 3 节点， TCP raft 传输（实验性） | 7.8k | 123µs | 219µs | 393µs | 0 |
| 3 节点， gRPC，1KiB payload | 5.6k | 171µs | 335µs | 546µs | 0 |
| 3 节点， gRPC，`--client-rps 2000` | 2.0k | 168µs | 500µs | 102ms | 0 |

注：第 4–6 行（TCP 传输 / 1KiB payload / 限流）为 2026-09 采集的独立场景，本轮未重测。
Rows 4–6 (TCP transport, 1KiB payload, rate limit) are distinct scenarios captured in 2026-09, not re-run this round.

要点：串行延迟由 quorum 规模决定（1 → 3 → 5 节点：67µs → 189µs → 264µs，每加一个复制跳点约
+75-120µs）；实验性 TCP raft 传输在 2026-09 的采集中比 gRPC 快约 21%（156µs → 123µs）。

### S2：并发写吞吐扫描（128B）/ Concurrent write throughput

| 并发 / Concurrency | 1 节点 TPS | 3 节点 TPS | 5 节点 TPS | 3 节点 p50/p99 | 错误 / Errors |
|---|---|---|---|---|---|
| 1 | 10.3k | 3.6k | 2.8k | 187µs / 531µs | 0 |
| 10 | 19.7k | 16.9k | 15.5k | 429µs / 1.0ms | 0 |
| 50 | 30.3k | 28.6k | 27.7k | 1.3ms / 3.5ms | 0 |
| 100 | 32.9k | 30.6k | 30.2k | 2.5ms / 5.5ms | 0 |
| 200 | 33.9k | 30.7k | 31.5k | 5.0ms / 10.6ms | 0 |
| 500 | 33.2k | 31.0k | 31.5k | 12.6ms / 25.9ms | 0 |

要点：并发起来后吞吐由**组提交批处理**（IoBatcher）主导，k≥50 时与集群规模基本无关（≈28-34k TPS，
单节点略高）；延迟随并发线性增长（排队的正常表现）。1KiB payload 时 k=200 约 28.2k TPS
（同窗口 128B 的 82%，raw-20261104-s2-payload1024-mem.json）。

---

### S3：读路径（128B，3 节点）/ Read paths

| 并发 / Concurrency | ReadLocal TPS | Linearizable TPS | Lin. p50/p99 | 错误 / Errors |
|---|---|---|---|---|
| 1 | 13.2k | 5.7k | 119µs / 377µs | 0 |
| 10 | 27.9k | 16.1k | 449µs / 1.1ms | 0 |
| 50 | 32.9k | 21.1k | 1.8ms / 4.3ms | 0 |
| 100 | 34.3k | 20.5k | 3.7ms / 8.4ms | 0 |
| 200 | 35.0k | 19.3k | 7.8ms / 18.1ms | 0 |
| 500 | 33.8k | 19.9k | 19.3ms / 39.4ms | 0 |

要点：ReadLocal（可能读到略旧数据）与写吞吐持平（≈34-35k）；线性一致读受 ReadIndex quorum 往返
限制，3 节点平台期 ≈19-21k TPS；5 节点 ≈14k TPS 为 2026-09 采集，本轮未重测。两者的取舍见
[读一致性指南](../guide/read-consistency.md)。

### 限流自我保护 / Rate-limit self-protection

`--client-rps 2000`（每节点）下，超出限额的请求收到 `RESOURCE_EXHAUSTED`，SDK 按指数退避重试：写吞吐收敛到 ≈1.4-1.5k TPS（写全部打到 leader，受 leader 单节点限额约束），低并发时延迟保持正常，超限时 p95 出现 ≈100ms 的退避尾巴，并发 ≥50 才开始出现重试耗尽的错误。集群在持续超载下保持稳定、无崩溃——与未限流时的行为对比见下文「压测驱动的修复」。

Under `--client-rps 2000` per node, excess requests get `RESOURCE_EXHAUSTED` and the SDK retries with exponential backoff: write throughput converges to ≈1.4-1.5k TPS (all writes hit the leader's limiter), latency stays normal below the limit, ~100ms backoff tails appear once over it, and retry-exhaustion errors only appear at concurrency ≥50. The cluster stays stable under sustained overload.

### 实验性 TCP raft 传输 / Experimental TCP raft transport

写入与 ReadLocal 在 TCP 传输下正常（2026-09 采集：S1 p50=123µs，S2 峰值 31.9k TPS；本轮未重测）。
**已知问题**：TCP 传输下线性一致读的 follower→leader 重定向在并发 ≥10 时不稳定（约 1-2% 的请求
重定向后不收敛，报 `follower redirected to leader`；gRPC 传输无此问题）。TCP 传输为明文实验特性
（强制 loopback），不建议生产使用。

Writes and ReadLocal are healthy over the TCP transport (2026-09 capture: S1 p50=123µs, S2 peak 31.9k
TPS; not re-run this round). **Known issue**: with the TCP transport, follower→leader redirects for
linearizable reads are unstable at concurrency ≥10 (~1-2% of requests fail to converge after redirect;
the gRPC transport is unaffected). The TCP transport is a plaintext experimental feature (loopback-only)
and not production-recommended.

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

> **多组 × 连接扇出复测（2026-11-04，M3 Pro 3 节点 loopback，mem/strict，128B，k=200，2s 预热 + 10s 窗口）**
> ——回答「扇出移除单 h2 连接串行点后，多组聚合吞吐是否开始扩展」（D-3②；
> raw-20261104-s2-groups{1,2,4}-fan{1,4}-mem.json）：
>
> | 组数 \ `--channel-fanout` | 1 | 4 |
> |---|---|---|
> | 1 | 34.3k | 72.0k |
> | 2 | 34.3k | 63.8k |
> | 4 | 33.0k | 61.2k |
>
> 结论：**多组聚合吞吐仍不随组数增长。** 扇出确实移除了单连接串行点（单组 34.3k → 72.0k = 2.1×，
> 与 2026-09-23 的 1.91× 一致）；但 fanout=4 下 1→4 组聚合反而 −15%（72.0k → 61.2k），p50 持平
> ≈2.0ms——绑定约束不是 per-group Mutex（`group_manager.rs` 的 `get_raft_for_write`：单组即可达
> 72k，锁若绑死会把多组压到单组之下并推高延迟），而是固定 3 节点上的宿主 CPU/调度，加组只增加
> raft 核/心跳/apply 的固定开销。multi-raft 扩展性前提不变——**组副本集合不相交 + 加机器**；容量
> 规划按「每组吞吐 ≈ 单组基线，组数随节点池扩展」建模。同窗口 opt-in 批处理复测：batch64+fan1
> = 168.3k、batch64+fan4 = 174.2k（p50 ≈0.8ms；raw-20261104-s2-batch64-fan{1,4}-mem.json）——
> 批处理自身已摊薄连接串行点，扇出边际收益仅 +3.5%。
>
> Retest 2026-11-04 (M3 Pro, 3-node loopback, mem/strict, 128B, k=200, 2s warmup + 10s window), answering
> "does multi-group aggregate throughput scale once the single-h2-connection serializer is removed?" (D-3②).
> **It does not.** Fanout does remove the connection serializer (one group: 34.3k → 72.0k = 2.1×, matching
> the 1.91× of 2026-09-23), but at fanout=4 the aggregate *falls* 72.0k → 61.2k from 1 → 4 groups while p50
> stays ≈2.0ms — the binding constraint is not the per-group Mutex (`get_raft_for_write` in
> `group_manager.rs`: a single group already reaches 72k; a binding lock would push multi-group *below*
> single-group and inflate latency) but host CPU/scheduling on the fixed 3 nodes, where each extra group adds
> fixed raft-core/heartbeat/apply cost. The multi-raft scaling premise is unchanged: **disjoint replica sets
> over a growing node pool** — model capacity as "per-group TPS ≈ single-group baseline, group count scales
> with the node pool". Same-window opt-in batching: batch64+fan1 = 168.3k, batch64+fan4 = 174.2k — batching
> alone amortizes the connection serializer, so fanout adds only +3.5%.

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
- **上述 loopback 表格为内存 Raft 日志**（`--storage mem`）：自 ADR A-0016 起 `--storage rocks` 会同时
  持久化 Raft 日志（`{data_dir}/raft-log/{group_id}`，见 [持久化与恢复](../architecture/durability.md)），
  因此 2026-11-04 归档的 rocks 矩阵（`raw-s2-rocks-*.json`）与 A-0016 前的同名 raw **不可直接比较**
  （durable raft log 是新的在途开销）。生产形态数字（durable log + TLS/mTLS + 负载机隔离）请用
  `deploy/perf/` perf-lab 与上文 open-loop 场景；存储层开销请用 `benches/` 的 criterion 微基准测量。
- **无 Jepsen 式正确性压测**：本页只覆盖性能；一致性验证见 `tests/integration/`（含 chaos 用例）。

---

- **Single-machine loopback**: cross-machine network RTT dominates real deployments (a Raft write typically costs 2-3 RTTs).
- **Empty state machine**: no business logic, no deserialization cost, no snapshot traffic.
- **In-memory Raft log in these tables** (`--storage mem`): since ADR A-0016, `--storage rocks` also persists
  the Raft log (`{data_dir}/raft-log/{group_id}`, see [Durability & Recovery](../architecture/durability.md)),
  so the rocks matrix archived on 2026-11-04 (`raw-s2-rocks-*.json`) is **not directly comparable** to the
  pre-A-0016 raws of the same name (a durable raft log is new on-path cost). For production-shape numbers
  (durable log + TLS/mTLS + isolated load generator) use the perf-lab in `deploy/perf/` with the open-loop
  scenarios above; use the criterion micro-benches in `benches/` for storage-layer numbers.
- **No Jepsen-style correctness testing**: this page covers performance only; correctness under faults lives in `tests/integration/` (including chaos cases).

## 贡献基准测试 / Contributing Benchmarks

如果您运行了基准测试，请通过 Pull Request 提交。请在报告中包含硬件规格、操作系统、Rust 版本与完整的命令行参数（本页所有表格都可通过上面的命令复现）。

---

If you run benchmarks, please open a PR. Include hardware specs, OS, Rust version, and the full command line (every table on this page is reproducible with the commands above).

---

**按目标 TPS 选部署拓扑与客户端开关，见 [容量规划与性能部署](../deployment/capacity-planning.md)。**
For deployment topologies and client knobs by target TPS, see
[Capacity Planning & Performance Deployment](../deployment/capacity-planning.md).
