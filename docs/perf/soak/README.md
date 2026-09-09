# Soak reports (R-8)

> 长稳验证报告存档。harness: `scripts/soak.sh`(仓库根);计划:
> `.sisyphus/plans/rr-reliability-program.md` R-8。

## 分层

| 层 | 时长 | 触发 | 说明 |
|---|---|---|---|
| nightly smoke | 2h | `.github/workflows/soak-nightly.yml` 每日 04:17 UTC | 信息性,攒 oracle 信号 |
| release soak | 24h | `workflow_dispatch`(SOAK_DURATION=86400)或维护者本机 | 0.3.0 起成为 minor release 门禁 |

## 报告判据(三件套 oracle)

1. **无数据丢失** —— ack-log 回放:keyed 幂等写的探针重放(同键异值 Put)
   必须走幂等缓存路径,`read_linearizable` 读回**原始值**。由
   `rr-perf-bench --scenario soak --oracle-only` 执行。
2. **无 panic** —— 三节点日志 `panicked at` 零匹配;容器重启次数 ==
   注入的 kill 数(无意外崩溃)。
3. **lag 收敛** —— 最后一次故障平息后 60s 内三节点
   `rr_raft_last_log_index` 追平(节点重启先等 /ready)。

## 复现

每份报告的 fault-log.jsonl 带 `seed`;同一故障序列重放:

```bash
SOAK_SEED=<seed> scripts/soak.sh
```

## 故障集说明

smoke 层故障 = SIGKILL / pause / 网络分区。ENOSPC 不在其中:docker volume
无容量上限,从测试填爆宿主机磁盘不可接受 —— ENOSPC 由 L1 的
`FaultInjectingStorage`(crates/rr-core/src/storage_fault.rs)覆盖,
release 层再入带容量上限的专用磁盘。

## 已知语义

- lab 栈 kv-store 用 **MemStorage**:容器重启 = 全量状态丢失,靠 snapshot
  复制追平。因此本 harness 的 kill/restart 覆盖的是"snapshot 恢复 + 幂等
  缓存重建"路径;RocksDB/strict 的日志级崩溃恢复由 R-4 的 `tests/crash`
  harness(真 rocks + SIGKILL)覆盖。
- 集群对客户端广播的 redirect 地址是**网内主机名**(kv-N:51001)——负载与
  oracle 必须在 lab 网内运行(`--profile bench` 容器),不能从宿主机直连。

## 与 deploy/lab/soak.sh 的关系

`deploy/lab/soak.sh`(2026-09-04 起)是**无故障**稳定性 soak(选举次数 /
RSS 漂移 / 日志一致 / Kafka exactly-once 链路)。本 harness 是故障注入 +
数据丢失 oracle 的严格加强版;两者并存,前者继续用于纯稳定性回归。
