# 100k ops/s 里程碑路线图 / Roadmap to 100k ops/s

> 状态:2026-09-22 首版。目标来自项目 Goal G2(单共识组 100k ops/s 量级)。
> 证据基线:`v0.2.0-baseline.md`(2026-09-05)+ 2026-09-22 符号级采样(本文档)。

## 现状 / Where we are

| 指标 | 实测值 | 来源 |
|---|---|---|
| 单组饱和吞吐(3 节点 loopback, 128B, mem/strict) | ~30–34k TPS | v0.2.0 基线 S2 + 2026-09-22 复测(30.1k @ k=200) |
| 串行写 RTT 下限 | p50 ≈ 161µs | v0.2.0 基线 S1 |
| 瓶颈层 | **CPU**(fsync/RocksDB 均 <2% 影响) | 基线 Finding 2–4 |
| 多组扩展(1/2/4 组, 固定 3 节点) | **持平 ~26k** | raw-s2-groups{1,2,4}-mem.json |

## 符号级热点(2026-09-22, leader 进程, `sample` 8s @ 30k TPS)

非空闲 leaf 帧归因(leader,mem/strict,k=200):

| 类别 | 占比(非空闲) | 代表帧 |
|---|---|---|
| 网络 syscall / HTTP-2 帧处理 | ~35% | `writev` 721、`__recvfrom` 293、h2 hpack/streams ~170 |
| 内存分配器 | ~13% | `_xzm_free` 429、`_xzm_xzone_malloc*` 450+、`memset` 80 |
| 内存拷贝/比较 | ~5% | `_platform_memmove` 387、`_platform_memcmp` 71 |
| 锁 | ~3% | `pthread_mutex_*` ~270 |
| tokio 调度 | ~7% | tokio leaf 632 |
| rr-raft(框架 raft 路径) | ~11% | 980 |
| rr-server(dispatch 等) | ~8% | 672 |
| rr-network(gRPC 服务层) | ~6% | 516 |
| openraft 核心 | ~0.5% | 41 |
| prost/bincode 编解码(leaf) | <1% | 多被内联进 tonic/rr 帧 |

**结论:热路径在"传输 + 每请求分配/拷贝",不在 raft 逻辑本身,也不在磁盘。**
openraft 核心与编解码几乎不占 leaf 时间;gRPC/HTTP-2 传输栈和每次请求的
分配/拷贝/锁是主要 CPU 去向。

## 里程碑 / Milestones

每个里程碑的验收 = `scripts/perf-baseline.sh` 重采基线 + `bench-compare.sh` 无回归;
吞吐目标为同环境(M3 Pro loopback 3 节点, 128B)S2 k=200 饱和值。

### M1 — 传输层效率(预期最大收益)

- [x] Raft 复制走 TCP(RRAF 帧,已实现且 loopback 门控)→ **已量化(2026-09-22)**:
      S2 k=200 mem,`--use-tcp-transport` 31.9k vs gRPC 30.2k TPS(**+5.6%**),
      p99 7.8ms vs 10.1ms(-23%)。结论:复制面 TCP 是温和正收益,但**不是 3× 杠杆**
      ——瓶颈大头在 client→server gRPC 路径与每请求 CPU。不推广 RRAF 到 client 路径;
      TCP 保持实验性 loopback 选项。数据:`docs/perf/baselines/raw-20260922-s2-*-mem.json`。
- [ ] tonic/h2 调参:initial window、max frame size、每连接并发流的实测扫描;
- [x] 评估 client→server 的请求级批处理(2026-09-23,**主杠杆确认,目标达成**):
      rr-client 新增 opt-in 批处理(`RrClientBuilder::with_batching(BatchConfig)`,
      默认关;Kafka 式旋钮 max_entries/max_bytes/linger/max_buffered 背压),
      批内 N 个写合并为**一条 Raft entry**(一次复制、一次提交),apply 侧按
      `rr_core::batch` 帧逐条拆分、响应按位打包;keyed 写绕开批处理。
      **实测(同环境 S2 k=200 mem,64 条/批,linger 1ms):175.2k TPS(6×),
      p50 686µs / p99 2.4ms,0 错误**(`raw-20260923-s2-grpc-clientbatch64-mem.json`)。
      代价:S1 串行写 p50 161µs → 3.1ms(linger 主导)——批处理是并发吞吐特性,
      串行延迟敏感场景保持关闭(故默认关)。
      约束:全集群需带 apply 拆分的框架版本;deferred(A-0033)服务不兼容。

### M2 — 分配与拷贝削减

- [ ] 每请求 buffer 复用(Bytes/arena),消除 dispatch/adapter 层的 Vec 逐请求分配;
- [x] 审查热路径 clone(2026-09-22):
      - `rocks_log_storage.rs:257` overlay 读路径 `entry.clone()` —— **判定:不可消除**。
        openraft `try_get_log_entries` 契约要求返回 owned `Vec<Entry>`;pending 里的
        条目在 flush 前 refcount≥2,`Arc` 化也无法 `try_unwrap`。深拷贝是该 trait
        owned-return 契约的固有成本,跳过;
      - `service_adapter.rs` unkeyed 路径 `entry.to_vec()` —— **判定:被 `Service`
        trait 的 owned `Vec<u8>` 签名强制**(`handle`/`handle_with_outputs_ctx` 均按值
        接收)。消除 = 破坏性 API 变更 → 记入 v0.3.0 候选(Bytes 化);
      - **已修**:metrics 标签逐请求 `to_string()`(dispatch.rs 每写 3 次 +
        network_factory.rs 每 AppendEntries RPC 3 次 + service_adapter.rs 每 applied
        entry 2 次)→ 全部改为进程内 intern 的 `&'static str`,热路径 metrics 零分配;
      - **已修**:`ShardRouter::route` 每请求全表 clone(mem)/etcd 全前缀扫描(prod)
        → `ConfigProvider::routing_table_snapshot()` 新增带默认实现的 trait 方法
        (semver-minor),`MemConfigProvider` 改 copy-on-write `Arc<RoutingTable>` 快照,
        route() 每次调用取 provider 当前快照(INV-6 语义不变:无第二份缓存视图)。
        **遗留**:etcd provider 仍逐调用全扫描(默认方法,行为等价于现状),
        revision 感知缓存是 follow-up。
- [x] 评估分配器(mimalloc/jemalloc)作为 feature 开关的 A/B(2026-09-22):
      `rr-server/mimalloc` + `rr-perf-bench/mimalloc` feature 已落地(默认关)。
      **macOS arm64 实测:mimalloc 25.4k / 23.6k TPS(两轮)vs 系统 xzm 30.1k/30.2k
      —— 稳定劣化 ~-20%,macOS 上否决 mimalloc**;Linux 待定(jemalloc/mimalloc 在
      glibc malloc 对照组下通常结论不同,M5 Linux 基线时重测)。

### M3 — 并发结构

- [x] **单 h2 连接串行点假设 —— 实证成立(2026-09-23,主杠杆)**:review 提出
      "30k 平台与多组 26k 平顶的共同串行点 = 每 addr 单条 h2 连接"(hyper 默认
      `max_concurrent_streams=200` 恰=bench k;server 侧单 conn task 做全部帧处理)。
      落地 `RrClientBuilder::with_channel_fanout`(默认 1,轮询)+ rr-server
      `--grpc-max-concurrent-streams/--grpc-max-frame-size`(CLI/env/TOML 三层)。
      **实测(S2 k=200 mem,raw-20260923-s2-m3-*-mem.json)**:

      | 配置 | TPS | p50 | p99 | vs 基线 |
      |---|---|---|---|---|
      | 基线 fanout=1 | 28.1k | 6.0ms | 10.9ms | — |
      | fanout=4 | 53.6k | 2.4ms | 15.1ms | **1.91×** |
      | fanout=8 | 67.4k | 2.1ms | 11.7ms | **2.40×** |
      | streams1024+frame256k(fanout=1) | 31.1k | 5.5ms | 9.5ms | 1.11× |
      | fanout=4 + 同上旋钮 | 59.3k | 2.3ms | 13.7ms | 2.11× |

      结论:连接扇出是唯一大杠杆(h2 旋钮单独仅 +11%);4→8 亚线性(1.9→2.4×),
      下一个串行点浮现(per-group Mutex/tokio 调度/客户端 CPU——留 M3 续查)。
      扇出默认保持 1(行为不变),bench/prod 按需开。
      **与批处理叠加**(raw-20260923-s2-m3-batch64-fan{1,4}-mem.json):
      batch64 fan1 = 76.6k / fan4 = **121.2k**(p99 3.0ms)。注意:本波修复给批处理
      加了 per-partition flush 串行化(跨批乱序修正,Kafka max.in.flight=1 类比),
      fan1 从 175.2k → 76.6k 是**正确性代价**(稳态下批 RPC 不再重叠);
      fan4 恢复到 121k。若需兼得,候选是给 BatchConfig 加 `ordered: bool`(默认 true)
      让吞吐优先用户显式放弃跨批顺序——留作 v0.3.0 kickoff 决策点。
      → 已裁定 defer 至 v0.3.0 kickoff(2026-11-04,见
      `.sisyphus/adr/A-0019-g2-decisions.md` ①;候选登记 `.sisyphus/plans/rr-v030-scope.md`)。
- [ ] tokio worker 数与 CPU 亲和性扫描(当前默认)——未做,留 M3 续;
- [ ] per-group 串行段(apply 管线、metrics tick)的跨组并行度验证——多组平顶根因
      已由扇出实验解释大半(单连接),组内 Mutex(`group_manager.rs` get_raft_for_write)
      是否次要点待扇出后复测。复测已排入 D-1(2026-11-04);结果回填后由维护者勾选
      (裁定见 `.sisyphus/adr/A-0019-g2-decisions.md` ②)。

### 退出判据 / Exit criteria

- 同环境 S2 k=200 ≥ 100k TPS,且 crash/fuzz/soak/TLA 门禁全绿;
- bench-compare 硬门禁(2026-09-22 已转 blocking)全程无红灯;
- 若 M1+M2 后仍 <100k:如实记录单机上限,把 100k 改写为多机组扩展目标
  (前置条件:组副本集合不相交,见 `docs/docs/reference/benchmarks.md` 多组实测注记)。

**判据状态(2026-09-24)**:吞吐项 **条件达成** —— opt-in 批处理+扇出下 121.2k TPS
(无扇出 76.6k;扇出对未批路径 28.1k→67.4k=2.4×)。
批处理开启态门禁 **首轮实证全绿(本地)**:
- crash harness 全量 **11/11**(批处理道默认开:三场景 sent=785/168/1171,
  `corrupted=0`;主 oracle 571/571 · 125/125 · 949/949);
- fuzz 双 batch target 各 241s 共 **81M runs 零 crash**(decode 30.9M + roundtrip 50.2M);
- soak 批处理+unkeyed 道 smoke(宿主机路径,180s,1×SIGKILL+重启):
  **oracle 5000/5000**(replay 1232 + survival 3275 + **unkeyed 493**,线性一致读验证),
  lag 60s 内收敛(last_log_index=35954 三节点一致),零 panic。

证据边界(如实记录,2026-10-04 刷新):Docker soak 路径因本机缺 `rust:1.98-bookworm`
等基础镜像且 registry 不可达受阻——`soak.sh` 已改为只拉起其 oracle 实际使用的 kv 服务
(`SOAK_WITH_OBSERVABILITY=1` 恢复 prom/grafana),Docker 跑通仍待镜像可得;
远端幻影事故 ~10-01 解除,但首轮真实 nightly 全灭:三个根因互相独立且全部在
harness/CI/制品层面(fuzz 安装版本解析、lab compose 传入 kv-store 不认识的 flag、
crash oracle 的 10k LRU 容量假设被慢 runner 规模突破),修复见 `0bf2e9b`;
crash/fuzz/soak nightly 的批处理覆盖累积待修复后 scheduled run 复核。
转正式剩余项:跨批顺序串行化的吞吐代价评估(见 M3 节 ordered 旋钮决策点,
v0.3.0 kickoff)+ nightly 累积;之后评估是否对特定 profile 默认开启
(串行延迟代价 161µs→3.1ms,默认关维持不变)。

## 复现 / Reproduce

```bash
cargo build --release -p rr-server -p rr-admin -p rr-perf-bench
./target/release/rr-perf-bench --scenario s2 --concurrency 200 --warmup-secs 3 \
  --duration-secs 20 --port-base 47500 --data-dir /tmp/rr-prof &
sleep 12
for p in $(pgrep -f "target/release/rr-server serve"); do sample "$p" 8 -file "/tmp/rr-sample-$p.txt" & done
wait
```

多组实测复现:`--groups 1|2|4`(见 benchmarks.md 注记与 raw-s2-groups*-mem.json)。
