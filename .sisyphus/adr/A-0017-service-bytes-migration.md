---
title: "Service trait Bytes 化 — 消除 apply 路径强制拷贝"
status: drafted
created: 2026-09-23
updated: 2026-09-23
tags: [adr, api-break, performance, v0.3.0, service-trait]
---

# A-0017 — Service Trait Bytes 化（消除 apply 路径强制拷贝）

> 立项来源:2026-09-22 全项目 review(100k roadmap M2)认定 unkeyed apply 路径
> 每条 entry 一次 `entry.to_vec()` 是 `Service::handle(Vec<u8>)` 所有权签名的强制
> 成本,消除它 = 破坏性 API 变更 → 按惯例记入 v0.3.0 候选并单独立项。
> 规划接线:`.sisyphus/plans/rr-phase4-v030.md`。

## Context

`Service` trait(`crates/rr-core/src/service.rs`)的全部业务入口按**值**接收请求:

```rust
async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError>;
async fn handle_with_outputs_ctx(&self, request: Vec<u8>, ctx: ApplyContext) -> ...;
async fn handle_deferred(&self, request: Vec<u8>, ctx: ApplyContext) -> ...;
```

同样地,openraft 的 `D`(应用数据)类型是 `RrAppData(pub Vec<u8>)`
(`crates/rr-raft/src/type_config.rs`)。

这条链路上每次写至少发生一次本可避免的全量拷贝:

1. **apply 拆分点**(`service_adapter.rs::apply_entry`):unkeyed 未带帧的 entry 经
   `FramedEntry::decode` 返回 `None` 后,`entry.to_vec()` 复制 payload 再交给
   service——只为满足 owned 签名;
2. **log 读取**:`try_get_log_entries` 返回 owned `Vec<Entry>`,replication 与 apply
   各自反序列化/搬运 `Vec<u8>` payload;
3. **client 批处理响应**(2026-09-22 落地)等 framed 路径同样按 `Vec<u8>` 打包。

基线采样(`docs/perf/roadmap-100k.md`,2026-09-22):分配器 ~13%、拷贝 ~5% 的
leader CPU。Bytes 化把链路上的 buffer 从"逐跳深拷贝"变为"引用计数共享",
是 M2 剩余项中结构性最强的一个。

## Problem

-  owned `Vec<u8>` 签名让框架在"只想借读"的位置被迫复制;
-  直接改签名是**破坏性变更**:`Service` 是公开 trait(examples、用户服务、
   文档示例全部实现它);`RrAppData` 的 bincode 字节布局变更会影响**已持久化的
   Raft log / 快照**的解码(v0.2.x 磁盘格式兼容承诺);
-  v0.3.0 已计划一个 snapshot 格式破坏窗口(A-0001 + A-0003 共用
  `FramedSnapshot` 迁移),Bytes 化应搭同一窗口,避免两次破坏。

## Decision(草案,待 v0.3.0 kickoff 评审)

1. **`Service` 请求/响应类型 `Vec<u8>` → `bytes::Bytes`**(`handle`、
   `handle_with_outputs`、`handle_with_ctx`、`handle_with_outputs_ctx`、
   `handle_deferred` 及其结果结构)。`Bytes` 提供零拷贝切片与廉价 clone。
2. **`RrAppData` 内部 buffer `Vec<u8>` → `Bytes`**。序列化格式迁移:
   `serde` 下 `Bytes` 以 bytes 形式编码,与 `Vec<u8>` 的 seq-of-u8 编码**不同**;
   bincode 下两者均为 len+bytes,字节兼容(需 golden corpus 钉死,见验证节)。
3. **v0.3.0 内提供过渡桥**:`Service` 增加 `handle_bytes` 默认方法
   (`Bytes` → 旧 `handle(Vec)` 桥接)一个 minor 周期,或反过来(旧 Vec 入口
   deprecated 桥接到新 Bytes 入口)——方向在 kickoff 时按"下游改动量最小"裁定。
4. **快照/日志兼容**:T1 持久快照与 RocksDB log 条目里的 payload 字节在 bincode
   下不变(验证节第 3 条);`FramedSnapshot`/`FramedEntry` 外层 envelope 不动。
5. **批次与 idempotency 格式不动**:`rr_core::batch` 帧与幂等 LRU 的 key 语义与
   buffer 类型无关,不受影响。
6. **顺手补齐版本位(2026-09-23 审计结论)**:两个已发布但无 magic/版本位的持久化
   文件纳入同一破坏窗口——`sm-snapshot.bin`(T1 `PersistentSnapshot`)与
   `sm-membership.bin`(A-0016 sidecar)。新规:带 `\0rrSNAP1`-系信封 + 版本位,
   解码端先试新信封、无 magic 回退 legacy 裸 bincode 解码(一次性自动迁移,
   迁移成功后按新格式落盘)。版本位约定见
   `docs/docs/reference/compatibility.md` 的"字节格式版本约定"。

### 否决的替代方案

- **保持 `Vec<u8>`,只优化热点**:单点修补(如 apply 借用)被 trait 所有权语义
  挡回,收益碎片化——否决;
- **直接换 `&[u8]` 借用签名**:`handle` 允许服务持有请求数据(存入自身状态),
  借用生命周期无法表达,只能退回 `to_vec`——否决,`Bytes` 是唯一同时满足
  "可持有 + 零拷贝"的形态。

## Impact(改动面盘点)

| 面 | 文件/位置 | 量 |
|---|---|---|
| trait 签名 | `rr-core/src/service.rs`(5 方法 + HandleResult/DeferredHandleResult) | 小 |
| 适配器 | `rr-raft/src/service_adapter.rs`(apply_entry 链)、`sm_adapter.rs` | 中 |
| 类型配置 | `rr-raft/src/type_config.rs`(`RrAppData`) | 小 |
| 内置/示例服务 | `rr-server/builtin_service.rs`、examples ×5、integration harness | 中 |
| 测试 | 全部 MockService / 手写 Service impl | 中(机械) |
| 文档 | guide/service-trait.md、api-overview.md、compatibility.md | 小 |

## Verification

1. `cargo semver-checks` 预期报 major —— 与 v0.3.0 窗口对齐,CI 白名单登记;
2. workspace 全量测试 + integration 81 + crash harness 10 全绿;
3. **golden corpus 钉格式**:对同一 payload 分别用旧 `Vec<u8>` 与新 `Bytes`
   编码,bincode 字节逐位一致(`rr-core/tests/golden/` 扩展样本);
4. 基线复采:S2 k=200 对比 v0.2.x,预期消除 apply 段单次拷贝,p50/p99 下探;
5. kv-store 示例迁移作为"下游改动量"的实证样板。

## Status

drafted(2026-09-23)。前置:v0.3.0 kickoff 评审;与 A-0001/A-0003 共用
`FramedSnapshot` 破坏窗口。
