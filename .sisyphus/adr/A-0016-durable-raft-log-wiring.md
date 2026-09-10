---
title: "Durable raft log in production binaries(log storage × network factory 正交化)"
status: implemented
created: 2026-09-10
updated: 2026-09-10
tags: [adr, durability, raft-log, rocksdb, production, correctness]
---

# A-0016 — 生产二进制接线 durable raft log

> 发现:生产性能测试方案(`rr-prod-perf.md` G8,2026-09-10 动工前查实)。
> 关联:A-0011(shutdown durability 契约)、A-0015(日志可见性契约)、
> A-0010(WAL,其前置判据 2 需在本 ADR 落地后重测)、INV-1、
> `tests/crash`(rr-crash-node 是当时唯一的双开组装)。

## Context

v0.2.0 及以前,**所有生产二进制的 raft log 都是内存存储**:

- `RaftGroupManager::start_group` → `RaftNode::new_with_factory` → `RrLogStorage`
  (内存 log,`raft_node.rs`);stock `rr-server` 与 `kv-store` 均经此路径。
- `--storage rocks` 只影响经 `NodeConfig.storage_engine` 传入的 SM 侧
  `RocksStorage`(且该字段当前无生产消费者),**不接 raft log 路径**。
- `RaftGroupManager::start_group_rocks` / `RocksLogStorage` 存在,但
  (a) 生产路径零调用(仅 5 个集成测试经 `new_with_rocks` 使用);
  (b) 其硬编码 `NoopNetworkFactory`——"durable log + 真实复制"的组合在
  生产代码中不存在。唯一的双开组装是 `tests/crash` 的 rr-crash-node
  (测试制品,手工 `new_with_rocks_factory` + `GrpcNetworkFactory`)。

后果两重:

1. **正确性**:默认部署形态(Helm/K8s 跑 stock rr-server)下,全集群同时断电
   会丢失未快照的已提交日志——strict 耐久契约(INV-1 族)在默认形态不成立。
   crash harness 当时已识别此缺口并以自搭二进制绕过。
2. **性能判据**:R-7 基线的写路径最热的一段(raft log append + IoBatcher +
   fsync)不在测量回路内,A-0010 前置判据 2("存储非绑定约束")证据不充分。

复制(AppendEntries)本身一直是开启的(`new_with_network` →
`GrpcNetworkFactory`);缺口仅在 log 持久化维度。

## Decision

### 1. log storage 与 network factory 正交化(rr-raft)

`RaftGroupManager` 新增 builder(cfg `rocks`):

```rust
pub fn with_rocks_base_path(mut self, base_path: String) -> Self
```

`start_group` 统一 log 选择:**`rocks_base_path` 已设时用
`RocksLogStorage`(`{base_path}/{group_id}`,Strict 默认),否则
`RrLogStorage`(内存)**;network factory 恒按构造时配置
(Grpc / Tcp / Noop),两个维度完全正交。节点构造收敛为单一
`build_node<NF>` helper,`new_with_factory` / `new_with_rocks_factory`
二选一在其中完成。

### 2. `start_group_rocks` 退化为兼容 shim

保持 pub API 不删(v0.2.0 已发布,semver):断言 `rocks_base_path` 已设
(原有错误语义不变),随后委托 `start_group`。行为变化仅一处:network
factory 由硬编码 `NoopNetworkFactory` 改为构造时配置——唯一既有调用形态
(`new_with_rocks`)配置的就是 NoopNetwork,故**无可观察行为变化**(5 个
集成测试不受影响),而 `new_with_rocks(...).with_rocks_base_path(...)` 之外的
新组合(如 `new_with_network + with_rocks_base_path`)获得双开能力。

### 3. rr-server 接线(stock 二进制,Helm/K8s 部署形态)

`--storage rocks` 时,`start_node` 为 group manager 追加
`with_rocks_base_path("{data_dir}/raft-log")`(gRPC 与 TCP 两个分支同;
`data_dir` 已由 `build_storage` 前置校验)。`--storage mem` 行为不变
(内存 log)。落盘布局:`{data_dir}/raft-log/{group_id}/`(在 data 卷内,
K8s PVC / compose named volume 天然覆盖;`rr-admin backup` 打包 data_dir
自动包含)。

### 4. kv-store 接线(perf-lab 被测服务)

新增 `--data-dir <PATH>` flag:设置时(需 `rocks` feature 构建)经
`with_rocks_base_path("{data_dir}/raft-log")` 开启 durable raft log;
未设置 = 内存 log(现状,单节点 demo 与 lab 行为不变)。非 rocks 构建传
`--data-dir` → fail fast 并提示重编译(与 `--kafka-brokers` 的
`check_kafka_support` 同模式)。

**不设 `--storage` / `--durability` flag**(对计划的 M0④ 修正):
`NodeConfig.storage_engine` 在全仓无生产消费者(仅 config.rs 存储),
`--storage rocks` 在 kv-store 上会是假 knob;`RocksLogStorage` 固定
Strict(`RocksStorage::open_default` → `RocksConfig::default()`),
durability 调参属于 A-0010/WAL 时代的事,不在本 ADR 引入。

### 5. 验证

- 单测(rr-raft,rocks-gated):`start_group` 在 base path 已设时写→停→
  同路径重开可读回;`start_group_rocks` 兼容路径不回归。
- e2e(tests/integration,rocks-gated):3 节点 `new_with_network` +
  `with_rocks_base_path` 集群,写 N 条已 ACK,逐节点滚动重启,
  已 ACK 全存活、复制收敛、零 head-gap。
- 既有回归:rr-raft rocks 130、workspace 全量测试、crash scenario A/B。

### 6. 判据联动

`rr-prod-perf.md` W11(mem-log vs rocks-log A/B)实测后,**A-0010 前置
判据 2 必须重判**并把结论回写 `rr-wal-impl.md`。

## Consequences

### 正面

- 默认部署形态(Helm/K8s/compose 跑 stock rr-server `--storage rocks`)
  首次满足全断电 strict 耐久契约;正确性缺口 G8 关闭。
- 生产性能测量覆盖完整写路径(client → dispatch → raft append →
  IoBatcher → fsync → apply),A-0010 前置判据 2 获得有效证据。
- 纯加法:`start_group` 旧行为(mem)在 base path 未设时逐字节不变;
  pub API 零删除,semver 兼容。

### 代价 / 注意

- `rr-server --storage rocks` 启动后新增 `{data_dir}/raft-log/` 目录与每组
  一个 RocksDB 实例(资源占用略升;CF 调优归 A-0006 既有机制)。
- 磁盘布局变化需在部署文档声明(升级注意事项);旧数据目录不含 raft-log
  子目录,升级后首启自动创建,无迁移。
- watch-loop 动态建组(`node.rs:363/459`)同样经统一入口,durable 语义
  对动态组一致生效。
