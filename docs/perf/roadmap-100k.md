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

- [ ] Raft 复制走 TCP(RRAF 帧,已实现且 loopback 门控)→ 量化 `--use-tcp-transport`
      对 S2 的提升,决定是否把 RRAF 推广到 client 路径;
- [ ] tonic/h2 调参:initial window、max frame size、每连接并发流的实测扫描;
- [ ] 评估 client→server 的请求级批处理(单连接多请求已有多路复用,瓶颈在帧/系统调用密度)。

### M2 — 分配与拷贝削减

- [ ] 每请求 buffer 复用(Bytes/arena),消除 dispatch/adapter 层的 Vec 逐请求分配;
- [ ] 审查热路径 clone:`rocks_log_storage.rs` overlay 读路径的 `entry.clone()`、
      FramedEntry 封装拷贝;
- [ ] 评估分配器(mimalloc/jemalloc)作为 feature 开关的 A/B。

### M3 — 并发结构

- [ ] tokio worker 数与 CPU 亲和性扫描(当前默认);
- [ ] per-group 串行段(apply 管线、metrics tick)的跨组并行度验证。

### 退出判据 / Exit criteria

- 同环境 S2 k=200 ≥ 100k TPS,且 crash/fuzz/soak/TLA 门禁全绿;
- bench-compare 硬门禁(2026-09-22 已转 blocking)全程无红灯;
- 若 M1+M2 后仍 <100k:如实记录单机上限,把 100k 改写为多机组扩展目标
  (前置条件:组副本集合不相交,见 `docs/docs/reference/benchmarks.md` 多组实测注记)。

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
