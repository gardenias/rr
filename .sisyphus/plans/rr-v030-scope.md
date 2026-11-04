# rr v0.3.0 范围注记(F-1)

> 来源:2026-11-04 全量 review F-1 项(`.sisyphus/evidence/rr-full-review-2026-11-04.md`;
> 修复计划 `.sisyphus/plans/rr-review-2026-11-04-remediation.md` Phase F)。
> 性质:一页范围基线注记,不是执行计划;v0.3.0 kickoff 时逐项裁定纳入/顺延。

## 为什么没有范围基线

原 Phase 4(v0.3.0)规划的内容——A-0001 Kafka ingestor、A-0002 快照契约、
A-0003 幂等层,以及 linearizable read / outbox / snapshot strategy 等——已提前
并入 v0.2.0(2026-09-10 final tag;见 `release-notes/v0.2.0.md` 与
`.sisyphus/plans/rr-phase4-v030.md`)。v0.3.0 因此没有既定范围基线,main 上
CHANGELOG `[Unreleased]` 持续累积变更而无 release 归属。

## 现行裁定(2026-11-04 owner)

个人项目、无外部用户:**破坏性变更在 `[Unreleased]` 堆积被接受**,不构成治理
风险;版本卫生(version bump + 迁移注记汇总)在**发版 cut 时**执行,不做持续
治理。发版瞬间的核对清单见 `docs/docs/operations/release-checklist.md`(F-2)。

## v0.3.0 候选项

- **A-0017 Service trait Bytes 化**(`.sisyphus/adr/A-0017-service-bytes-migration.md`,
  drafted)——破坏性 API 变更;消除 apply 路径 `entry.to_vec()` 强制拷贝。
- **A-0018 (b) error-as-applied-response**(`.sisyphus/adr/A-0018-apply-error-contract.md`
  Decision 中期项)——破坏性契约变更;与 A-0017 同破坏窗口评审。
- **`BatchConfig::ordered` 旋钮裁定**(`.sisyphus/adr/A-0019-g2-decisions.md` ①)——
  kickoff 评审;需先做 A-0033/幂等交互分析。
- **`DurabilityPolicy` deprecated alias 拆除**——v0.2.0 freeze checklist D 节裁决
  「removed in v0.3.0」(`crates/rr-storage/src/rocks.rs` 的 `Balanced`/`Fast` 别名);
  破坏性,拆除即 enum 定稿。
- **workspace version bump**——根 `Cargo.toml` `[workspace.package] version`
  (0.2.0 → 0.3.0)+ `deploy/helm/rr/Chart.yaml` `version`/`appVersion`;
  发版动作,随 cut 执行。
- **sdks/java 版本同步**——`sdks/java/gradle.properties` 的 `sdkVersion` 与
  `build.gradle.kts` 的 `getOrElse` fallback;发版动作,随 cut 执行。
- **Java SDK 能力差距**(batching / channel-fanout)——修复计划 E-2 的差距清单项;
  不强迫本期,kickoff 裁定是否补齐。

### roadmap-100k 未勾选遗留(全部转入 v0.3.0 候选)

- **M1**:tonic/h2 调参(initial window / max frame size / 每连接并发流实测扫描);
- **M2**:每请求 buffer 复用(Bytes/arena,消除 dispatch/adapter 层逐请求 Vec
  分配)——与 A-0017 同窗口;
- **M2**:mimalloc/jemalloc 在 Linux 的 A/B 复测(macOS 已否决;待 M5 Linux
  基线环境);
- **M3**:tokio worker 数与 CPU 亲和性扫描;
- **M3**:per-group 串行段跨组并行度验证——复测已排入 D-1(2026-11-04),
  见 `.sisyphus/adr/A-0019-g2-decisions.md` ②;结果回填 roadmap 后随
  v0.3.0 范围一并裁定。

## 边界

- 本注记不冻结任何东西;kickoff 评审可增删。
- 100k 目标的对外口径维持 A-0019 ③(aspirational,如实标注);若 kickoff 时
  默认配置仍 <100k,按 roadmap 退出判据 fallback 改写为多机扩展目标。

## Changelog

- 2026-11-04 — 初版(F-1 落地;候选项自 A-0017/A-0018/A-0019、v0.2.0 freeze
  checklist D 节、roadmap-100k 未勾选项与修复计划 E-2 汇总)。
