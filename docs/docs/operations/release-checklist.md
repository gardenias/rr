# 发版前清单 / Pre-release Checklist

> 适用对象:release owner 在 cut 任何 tag 之前逐项打勾。每项附检查命令。
> For the release owner: tick every item before cutting a tag. Each item
> carries its verification command.

:::info 为什么只有发版瞬间才核对 / Why this runs only at release cut
项目为个人项目、无外部用户(owner 裁定 2026-11-04):破坏性变更在
`CHANGELOG.md` 的 `[Unreleased]` 段长期堆积是**被接受的**,版本卫生不做持续
治理——但**发版瞬间**必须完成本清单,否则版本号、迁移注记、发布制品会
对不上。v0.2.0 的完整 freeze 记录见 `.sisyphus/checklists/v0.2.0-api-freeze.md`。

Personal project ruling (2026-11-04): breaking changes may accumulate in
`[Unreleased]` between releases. Hygiene applies **at the release cut** — this
checklist is that gate.
:::

## 1. Workspace version bump

- [ ] 根 `Cargo.toml` 的 `[workspace.package] version` 提到目标版本;全部
      workspace 成员继承(`version.workspace = true`)。
- [ ] `deploy/helm/rr/Chart.yaml` 的 `version` 与 `appVersion` 同步。

```bash
grep -n '^version' Cargo.toml                       # [workspace.package] 段
grep -rn 'version = "0\.' --include Cargo.toml crates examples tests tools \
  | grep -v 'version.workspace'                     # 成员不应有硬编码版本
grep -E '^(version|appVersion)' deploy/helm/rr/Chart.yaml
```

注:`fuzz/Cargo.toml` 为 `0.0.0` 且非 workspace 成员,属预期残留。

## 2. sdks/java 版本同步

- [ ] `sdks/java/gradle.properties` 的 `sdkVersion` = 目标版本;
      `sdks/java/build.gradle.kts` 的 `getOrElse("...")` fallback 同步。

```bash
grep -n 'sdkVersion' sdks/java/gradle.properties
grep -n 'getOrElse' sdks/java/build.gradle.kts
```

## 3. CHANGELOG 切段

- [ ] `[Unreleased]` 段改为 `[vX.Y.Z] — YYYY-MM-DD`(发版日期),并新开空的
      `[Unreleased]` 段。
- [ ] 条目维持 Keep-a-Changelog 分组(Added / Changed / Deprecated / Removed /
      Fixed / Security)。

## 4. 破坏性变更迁移注记汇总

- [ ] `[Unreleased]`(即将切段)中每条 breaking 项都有对应迁移说明,汇总进
      `docs/docs/deployment/` 的迁移页(先例:`v0.1-to-v0.2-migration.md`);
      每条含:变更内容、对用户影响、自动 vs 手动迁移。
- [ ] 评审 `cargo semver-checks` 报告,确认 breaking 清单与迁移注记一一对应;
      弃用项拆除时点与 [compatibility 承诺](../reference/compatibility.md)一致。

## 5. 文档版本一致性扫描

- [ ] 全站 grep 旧版本号叙述,确认无「以旧版为当前版」的残留(v0.2.0 RC 曾修
      一批 0.3.0→0.2.0 漂移,先例见 `.sisyphus/plans/rr-v0.2.0-release-roadmap.md`
      RC 并行项);
- [ ] 检查 `Unreleased (vX.Y.Z)` 类标注(如 metrics-reference)在新版叙述中落地;
- [ ] 本周期内 shipped 的 ADR 状态翻转为 `implemented` 并更新
      `.sisyphus/adr/README.md` 索引(implemented = 随发布版本落地并在
      CHANGELOG 引用)。

```bash
grep -rn 'Unreleased' docs/docs/ --include='*.md'
grep -rn 'v0\.2\.0' docs/docs/ --include='*.md' | grep -iv migration   # 抽查语境
```

## 6. Tag 与 release notes

- [ ] 新建 `release-notes/vX.Y.Z.md`(格式先例:`release-notes/v0.2.0.md` —
      头部 Tag/Released/Previous/Compare 链接 + Summary + Highlights;正文即
      GitHub Release 描述,贴进 release UI);
- [ ] `git tag vX.Y.Z` 并推送;
- [ ] tag 后首个 commit:semver-checks 基线滚动到新 tag(v0.2.0 先例:去掉
      `continue-on-error`,baseline 从 rc 滚到 final,转为阻塞门禁)。

---

全部勾选(或显式记录豁免理由)后由 release owner 签字发版。逐项通过前不 cut tag。

All items ticked — or waivers explicitly recorded — before the tag is cut.
