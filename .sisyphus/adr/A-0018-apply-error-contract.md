---
title: "Apply 期错误契约 — group-fatal vs error-as-applied-response"
status: drafted
created: 2026-09-23
updated: 2026-09-23
tags: [adr, reliability, apply-path, error-contract, v0.3.0]
---

# A-0018 — Apply 期错误契约（group-fatal vs error-as-applied-response）

> 立项来源:2026-09-23 全项目 review(`.sisyphus/evidence/rr-fresh-review-2026-09-23.md`)
> P0-1「日志投毒 → apply 崩溃循环」。当日已落地修复的第一刀(入口校验),本 ADR 记录
> 剩余的设计问题与选项,供 v0.3.0 kickoff 评审拍板。

## Context

apply 链路对**单条 entry 的错误**(decode 失败或业务 `Service::handle` 返回 `Err`)
今天的语义是 **group-fatal**:

1. `service_adapter.rs::apply_entry` 的 decode 错误(`FramedEntry::decode` /
   `rr_core::batch::decode_batch`)与 `Service::handle` 的业务错误向上传播,
   经 `sm_adapter.rs` 转 `io::Error`;
2. openraft 的 sm worker 循环在第一个错误即退出
   (`vendor/openraft/src/core/sm/worker.rs:97`);
3. RaftCore 转为 `Fatal::StorageError`(`raft_core.rs:2092-2094`),该 Raft 组
   **永久 Shutdown**,rr 侧无看守重启;
4. rocks 日志下节点重启会重放同一毒条目 → 再次 Fatal,形成**崩溃循环**。

2026-09-23 落地的修复(`rr_core::ingress::validate_client_payload`,接在
dispatch/write 路径)在 propose 之前以确定性校验拒掉畸形/伪造的 house-magic
payload,**关闭了客户端可达的触发路径**;批拆分也已 gate 到 `offset.is_none()`
(ingestor 字节永不重解)。入口守卫回归测试见 `crates/rr-core/src/ingress.rs`
与 dispatch 侧测试;相关不变量登记见 INV registry
(`docs/docs/reference/invariants.md`)。

## Problem

入口校验只挡住"客户端可达"的毒条目。仍然存在、且需要设计决策的剩余面:

- **业务错误仍是 group-fatal**:`Service::handle` 对某条 entry 返回 `Err`
  (例如业务校验失败、反序列化用户层 envelope 失败),今天的语义 = 杀整个
  Raft 组。对"单请求坏数据"而言惩罚过重;但反过来,把它吞掉又违反
  「错误路径必须响亮」的项目铁律与状态机确定性要求。
- **入口守卫无法覆盖全部来源**:框架内部产生的 entry(ingestor、outbox
  RaftWrite、框架信封)不经 client dispatch;这些路径自身的 bug 产生的畸形
  entry 仍会走 group-fatal。
- **崩溃循环的恢复成本高**:唯一出路是 `rr-admin node-reset` + learner 重挂
  (ADR A-0002 case 4),或外科手术式删日志——都没有 runbook 化。

核心设计问题:**apply 期的单条 entry 失败(decode 或 `Service::handle` 错误)
是否应当变成确定性的「已应用的错误响应」(APPLIED error),而不是 group-fatal?**

## Options

### (a) 现状 + 入口守卫(status quo + ingress guard)

维持 group-fatal 语义;依靠 2026-09-23 的入口校验把客户端可达触发面清零,
并把「`Service::handle` 返回 `Err` = 组级致命」作为**文档化契约**写进
service-trait 文档。

- 优点:零行为变更;确定性天然成立(全副本在同一 entry 一起 Fatal,不存在
  状态分叉);错误最响亮(组直接死,运维不可能看不见)。
- 缺点:一条坏 entry = 整组不可用 + 重启崩溃循环;业务侧偶发错误(非框架
  bug)也会杀死整个组;恢复依赖人工 node-reset。

### (b) error-as-applied-response 契约

apply 期单条 entry 的 decode/业务错误不再上抛杀 worker,而是编码为
**确定性的错误响应**并正常推进 `last_applied`——客户端收到带错误标记的
APPLIED 响应,组继续存活。

- 可行性前提:**确定性**。相同字节在任何副本上必然产生相同的 decode/校验
  结果(decode 是纯函数;`Service::handle` 的输入校验若只依赖 entry 字节本身
  也满足),因此「错误响应」可以像正常响应一样逐副本一致地落地,**不会造成
  副本间状态分叉**。注意:依赖外部状态(时间、随机、本地文件)的业务错误
  不满足该前提,契约上必须禁止或仍按 Fatal 处理。
- 优点:单请求坏数据不再炸组;客户端得到明确的 per-request 错误而非整组
  超时;消除崩溃循环(毒条目 apply 一次即越过)。
- 缺点:**改变 `Service` 错误语义**(今天 `Err` = 组级致命,改后 = per-request
  错误响应)——破坏性变更,所有现有 Service 实现的错误预期都要重审;需要
  新的响应信封字段(error kind)与指标;「业务状态依赖型错误」与「字节确定型
  错误」的分类边界需要严格定义,否则会引入真正的分叉风险。

### (c) Fatal watchdog + poison-skip runbook

维持 group-fatal,但补上运维闭环:rr 侧看守 RaftCore `Fatal`(metrics +
tracing::error! + 可选的 supervisor 告警钩子),并把「定位毒条目 → 离线
跳过 → learner 重挂」写成 runbook(node-reset 流程的参数化变体)。

- 优点:不改语义、不碰确定性前提;把 MTTR 从"翻代码猜"降到"按手册执行";
  watchdog 让 group-fatal 至少可观测(指标/告警),符合响亮失败铁律。
- 缺点:崩溃循环本身仍在,只是恢复更快;watchdog 不能自动跳过毒条目
  (自动跳过 = 静默改日志,违反不丢数据原则,必须人工确认)。

## Decision(草案,待 v0.3.0 kickoff 评审)

**短期 = (a) + (c)**:维持 group-fatal 语义与 2026-09-23 入口守卫;补 Fatal
watchdog 的可观测性(RaftCore Fatal 计指标 + error 级日志 + runbook 化的
poison-skip 流程),并把「`Service::handle` 错误 = 组级致命」写入
service-trait 文档。

**中期 = 评审 (b)**:error-as-applied-response 是对的方向,但它是破坏性契约
变更,应搭 v0.3.0 破坏窗口(与 A-0017 Service Bytes 化同窗口评审),前提是
把「字节确定型错误」与「外部状态依赖型错误」的分类钉死并有测试锁定。

## Consequences

- 已落地(2026-09-23,不在本 ADR 范围):`rr_core::ingress::validate_client_payload`
  入口校验 + 批拆分 `offset.is_none()` gate,关闭 P0-1 的客户端可达触发路径。
- 待办((c)):RaftCore Fatal 指标/watchdog;poison-skip runbook;
  service-trait 文档的错误语义小节。
- 待评审((b)):v0.3.0 kickoff 时与 A-0017 同窗口裁定;若采纳,需要响应
  信封的 error-kind 演进(遵循 compatibility.md 的字节格式版本约定)、
  全 Service 实现的重审清单、以及确定性分类的回归测试。
- 不变量联动:apply 错误契约若变更,INV registry
  (`docs/docs/reference/invariants.md`)中相关条目需同步更新。

## References

- 评审实证:`.sisyphus/evidence/rr-fresh-review-2026-09-23.md` — P0-1
  (日志投毒 → apply 崩溃循环),含完整 file:line 链。
- INV registry:`docs/docs/reference/invariants.md`。
- 相关 ADR:A-0002(快照/恢复路径)、A-0011(关停持久化契约)、
  A-0017(Service Bytes 化,v0.3.0 同破坏窗口)。
- openraft 源码:sm worker 首错即退 `vendor/openraft/src/core/sm/worker.rs:97`;
  RaftCore Fatal `vendor/openraft/src/core/raft_core.rs:2092-2094`。

## Changelog

- 2026-09-23 — drafted(立项自 2026-09-23 全项目 review P0-1)。
