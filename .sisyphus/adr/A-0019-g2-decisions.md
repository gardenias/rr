---
title: "G2 决策点裁定 — BatchConfig::ordered 旋钮 / 多组扇出后复测 / 单组 100k 立场"
status: accepted
created: 2026-11-04
updated: 2026-11-04
tags: [adr, performance, g2, batching, multi-group, v0.3.0]
---

# A-0019 — G2 决策点裁定(ordered 旋钮 / 多组复测 / 100k 立场)

> 立项来源:2026-11-04 全量 review(`.sisyphus/evidence/rr-full-review-2026-11-04.md`
> 遗留问题 #6「G2 决策点悬置」),修复计划 D-3 项
> (`.sisyphus/plans/rr-review-2026-11-04-remediation.md`)。三个决策点均由项目 owner
> 于 2026-11-04 裁定,本 ADR 如实记录裁定结果与依据;各点不再重开讨论。

## Context

G2 = 项目目标「单共识组 100k ops/s 量级」,执行载体是
`docs/perf/roadmap-100k.md`(M1 传输层 / M2 分配与拷贝 / M3 并发结构)。
2026-09-23/24 的关键实证(M3 Pro loopback 3 节点、mem/strict、128B、S2 k=200):

- opt-in 客户端批处理(batch64, linger 1ms)无扇出:**175.2k TPS**;
  同波修复给批处理加了 per-partition flush 串行化(跨批乱序修正,Kafka
  `max.in.flight=1` 类比)后,fan1 降到 **76.6k TPS**——稳态下批 RPC 不再重叠,
  是正确性代价;
- 叠加通道扇出 fanout=4 恢复到 **121.2k TPS**(p99 3.0ms),超过 100k 目标,
  且未弱化任何默认语义(批处理与扇出均为 opt-in,默认关/默认 1);
- 默认配置(无批处理、fanout=1)饱和 **~30–34k TPS**,瓶颈在 CPU 的
  「传输 + 每请求分配/拷贝」(2026-09-22 符号级采样,openraft 核心与编解码
  几乎不占 leaf 时间);
- 多组扩展(`--groups 1/2/4`,固定 3 节点)聚合吞吐持平 ~26k——扇出实验
  解释了平顶根因的大半(每 addr 单条 h2 连接是共同串行点),但扇出后的
  多组复测一直未做。

由此留下三个悬置决策点(Problem),2026-11-04 review 将其列为 D-3 裁定项。

## Problem

1. **`BatchConfig::ordered` 旋钮要不要做**:给吞吐优先用户一个显式放弃跨批
   顺序的开关(`ordered: bool`,默认 true),可在 fan1 下找回 76.6k → 175.2k
   的差量;但它放松的是顺序语义,属于「以正确性换吞吐」类旋钮。
2. **扇出后的多组复测排不排**:单连接串行点移除后,组内 Mutex
   (`crates/rr-raft/src/group_manager.rs:1368` `get_raft_for_write`)是否成为
   多组聚合吞吐的新约束,roadmap M3 对应复测框一直未勾选。
3. **「单组 100k」的对外立场**:121.2k 只在 opt-in 批处理 + 扇出组合下测得,
   默认配置距目标仍有 ~3×;目标是降为「已达成」、维持「aspirational」、
   还是改写为多机扩展目标。

## Options

各决策点的候选选项与裁定见 Decision;此处只列公共约束:

- 项目优先级铁律 **G1(正确性)> G2(性能)> G3**,任何放松语义的性能旋钮
  必须先证明不侵蚀 G1 契约;
- roadmap-100k.md 的退出判据含 fallback:「若 M1+M2 后仍 <100k,如实记录单机
  上限,把 100k 改写为多机组扩展目标」——该判据在裁定时仍然有效;
- 个人项目语境(2026-11-04 owner 裁定):破坏性/语义类变更的评审窗口收敛到
  v0.3.0 kickoff 一次处理,不为单个旋钮单开流程。

## Decision(2026-11-04 owner 裁定)

### ① `BatchConfig::ordered` 旋钮 —— DEFER 至 v0.3.0 kickoff

**裁定:不现在做;列为 v0.3.0 kickoff 评审候选(登记于
`.sisyphus/plans/rr-v030-scope.md`)。**

理由:

- 正确性优先(G1 > G2 不可违背)。`ordered: false` 让吞吐优先用户放弃跨批
  顺序,但「安全地放弃顺序」需要先完成与 deferred/两阶段服务(A-0033)及
  幂等层(A-0003)的交互分析——批内乱序、重放、deferred intent 的生命周期
  在无序假设下的行为尚未论证,贸然暴露旋钮等于把未分析的正确性风险转嫁给
  用户;
- 目标已达成,无紧迫性:batch64 + fanout4 实测 121.2k TPS 已越过 100k,
  且没有弱化任何默认语义;旋钮只服务「fan1 也要满吞吐」的更窄场景;
- 代价面已如实记录:fan1 175.2k → 76.6k 是跨批顺序串行化的正确性代价
  (roadmap-100k.md M3 节),不是回归。

### ② 多组扇出后复测 —— SCHEDULED,并入任务 D-1(2026-11-04)

**裁定:排期执行,并入 2026-11-04 修复计划的 D-1(重采基线)任务;
roadmap M3 对应框只标注、不勾选,结果由维护者在 D-1 回填后勾选。**

待答问题:单 h2 连接串行点(已由扇出实验证实并给出 fanout 旋钮)移除后,
per-group Mutex(`group_manager.rs` `get_raft_for_write`)是否成为多组聚合
吞吐的绑定约束。标注文本(已落入 roadmap-100k.md M3 对应条目):
「复测已排入 D-1(2026-11-04);结果回填后由维护者勾选」。

### ③ 「单组 100k」立场 —— KEEP 为 aspirational goal,如实标注现状

**裁定:维持 100k 作为追求目标,不宣布「默认达成」,对外如实陈述状态。**

现状口径:

- 121.2k TPS 单组**仅**在 opt-in 批处理(batch64)+ 通道扇出(4)组合下测得
  (2026-09-23/24;crash harness 11/11、fuzz 双 batch target 81M runs 零 crash、
  soak oracle 5000/5000,门禁首轮全绿,见 roadmap-100k.md「判据状态」);
- 默认配置(批处理关、fanout=1)饱和 ~30–34k TPS,CPU 受限于传输栈与
  每请求分配/拷贝(2026-09-22 符号级采样);
- 让 100k 成为默认能力取决于 M2 剩余项(分配/拷贝消除,与 A-0017 Service
  Bytes 化同窗口)与 M3 残余项(含 ② 的复测结论);
- roadmap 退出判据的 fallback 继续有效:若 M1+M2 后默认配置仍 <100k,
  如实记录单机上限并把 100k 改写为多机组扩展目标(前提:组副本集合不相交,
  见 `docs/docs/reference/benchmarks.md` 多组实测注记)。

用户可见文档(intro.md、benchmarks.md 等)的措辞更新由 controller 持 D-1
新鲜数据另行处理,不在本 ADR 范围。

## Consequences

- **正面**:三个悬置点全部闭环为「有裁定、有排期、有口径」;roadmap 与
  对外文档不再背负未裁定的歧义;默认语义零弱化。
- **负面/代价**:fan1 批处理吞吐留在 76.6k 直至 v0.3.0 kickoff 复审 ①;
  多组复测结论依赖 D-1 执行,roadmap M3 框在回填前保持未勾。
- **联动**:
  - ① 的候选资格登记在 `.sisyphus/plans/rr-v030-scope.md`(F-1);
  - ② 的回填动作挂在 `.sisyphus/plans/rr-review-2026-11-04-remediation.md`
    D-1 之后;
  - ③ 的对外措辞更新由 controller 负责(controller-owned 文件,本 ADR 不动)。

## References

- 路线图与判据状态:`docs/perf/roadmap-100k.md`(M1/M3 节、「退出判据」、
  「判据状态(2026-09-24)」)。
- 原始基线(`docs/perf/baselines/`):
  - 批处理 175.2k:`raw-20260923-s2-grpc-clientbatch64-mem.json`;
  - 批处理 + 顺序串行化:`raw-20260923-s2-m3-batch64-fan1-mem.json`(76.6k)、
    `raw-20260923-s2-m3-batch64-fan4-mem.json`(121.2k);
  - 扇出扫描:`raw-20260923-s2-m3-{base,fan4,fan8,fan4streams,streams1}-mem.json`;
  - 默认配置复测:`raw-20260922-s2-grpc-final-mem.json`(30.1k @ k=200);
  - 多组平顶:`raw-s2-groups{1,2,4}-mem.json`。
- 多组实测注记:`docs/docs/reference/benchmarks.md`(「多组扩展实测」段)。
- 相关设计引用:deferred/两阶段服务 A-0033(交互分析前提,见
  `docs/docs/guide/client-sdk.md` 批处理不兼容声明);幂等 A-0003;
  Service Bytes 化 A-0017。
- 评审依据:`.sisyphus/evidence/rr-full-review-2026-11-04.md` 遗留问题 #6;
  修复计划 D-3:`.sisyphus/plans/rr-review-2026-11-04-remediation.md`。

## Changelog

- 2026-11-04 — accepted(立项自 2026-11-04 全量 review D-3;三点裁定当日由
  owner 作出,本 ADR 记录定稿)。
