# 发版前清单 / Pre-Release Checklist

Release owner 在创建任何 tag 前逐项确认；每项必须执行检查，或记录明确豁免。

The release owner must verify every item before creating a tag, or record an explicit waiver.

:::info 发版门禁 / Release-Cut Gate
破坏性变更可在两次发版之间积累于 `CHANGELOG.md` 的 `[Unreleased]`，但创建 tag 前必须完成版本、迁移注记和制品核对。v0.2.0
freeze 记录见 `.sisyphus/checklists/v0.2.0-api-freeze.md`。

Breaking changes may accumulate in `CHANGELOG.md` under `[Unreleased]` between releases, but versions, migration notes,
and artifacts must be reconciled at the release cut. The v0.2.0 freeze record is
`.sisyphus/checklists/v0.2.0-api-freeze.md`.
:::

## 1. Workspace 版本 / Workspace Version

- [ ] 根 `Cargo.toml` 的 `[workspace.package] version` 是目标版本，workspace 成员使用 `version.workspace = true`。<br />
  The root workspace version is the target, and members inherit it.
- [ ] `deploy/helm/rr/Chart.yaml` 的 `version` 与 `appVersion` 同步。<br />
  Chart `version` and `appVersion` are synchronized.

```bash
git grep -n '^version' -- Cargo.toml
git grep -n 'version = "0\.' -- 'crates/**/Cargo.toml' 'examples/**/Cargo.toml' 'tests/**/Cargo.toml' 'tools/**/Cargo.toml'
git grep -n -E '^(version|appVersion)' -- deploy/helm/rr/Chart.yaml
```

`fuzz/Cargo.toml` 为 `0.0.0` 且不是 workspace 成员，属于预期。

`fuzz/Cargo.toml` remains `0.0.0` and is not a workspace member; this is expected.

## 2. Java SDK 版本 / Java SDK Version

- [ ] `sdks/java/gradle.properties` 的 `sdkVersion` 是目标版本，`sdks/java/build.gradle.kts` 的 `getOrElse("...")`
  fallback 同步。 / `sdkVersion` and the Gradle fallback both match the target.

```bash
git grep -n 'sdkVersion' -- sdks/java/gradle.properties
git grep -n 'getOrElse' -- sdks/java/build.gradle.kts
```

## 3. CHANGELOG 切段 / Cut the CHANGELOG Section

- [ ] 把 `[Unreleased]` 改为 `[vX.Y.Z] — YYYY-MM-DD`，并新建空 `[Unreleased]`。<br />
  Rename the section with version and release date, then add a fresh empty `[Unreleased]`.
- [ ] 保持 Keep-a-Changelog 分组：Added / Changed / Deprecated / Removed / Fixed / Security。<br />
  Keep the Keep-a-Changelog groups: Added / Changed / Deprecated / Removed / Fixed / Security.

## 4. 迁移注记 / Migration Notes

- [ ] 每条 breaking 项都在 `docs/docs/deployment/` 的迁移页说明变更、用户影响、自动或手动迁移；先例为
  `v0.1-to-v0.2-migration.md`。<br />
  Every breaking item has migration documentation covering the change, impact, and automatic/manual path.
- [ ] 评审 `cargo semver-checks` 报告，使 breaking 清单与迁移注记一一对应，弃用拆除时点符合
  [兼容性承诺](../reference/compatibility.md)。<br />
  Review `cargo semver-checks` and reconcile every breaking item with migration notes and the
  [compatibility policy](../reference/compatibility.md).

## 5. 文档版本一致性 / Documentation Version Consistency

- [ ] 扫描旧版本号，确认没有把旧版写成当前版的残留。<br />
  Scan old version references and remove stale “current version” wording.
- [ ] 检查 `Unreleased (vX.Y.Z)` 标注已在新版叙述中落地。 / Resolve `Unreleased (vX.Y.Z)` markers for the release.
- [ ] 本周期已发布 ADR 标为 `implemented`，并更新 `.sisyphus/adr/README.md`；implemented 表示随发布落地且在 CHANGELOG 引
  用。 / Mark shipped ADRs `implemented`, update `.sisyphus/adr/README.md`, and reference them from the CHANGELOG.

```bash
git grep -n 'Unreleased' -- 'docs/docs/*.md' 'docs/docs/**/*.md'
git grep -n 'v0\.2\.0' -- 'docs/docs/*.md' 'docs/docs/**/*.md'
```

第二条命令只用于定位，必须结合迁移页和历史版本语境人工判断，不要机械替换。

The second command only locates references; review migration and historical context manually instead of replacing them
mechanically.

## 6. Tag 与发布说明 / Tag and Release Notes

- [ ] 新建 `release-notes/vX.Y.Z.md`，参照 `release-notes/v0.2.0.md` 包含 Tag/Released/Previous/Compare、Summary 和
  Highlights；正文用于 GitHub Release 描述。<br />
  Create the release note with the established metadata, Summary, and Highlights, and use it as the GitHub Release
  description.
- [ ] 完成全部检查后创建并推送 `vX.Y.Z`。<br />
  Create and push `vX.Y.Z` only after every check is complete.
- [ ] tag 后首个提交滚动 semver-checks 基线到新 tag，并按项目约定调整门禁。<br />
  In the first post-tag commit, move the semver-checks baseline to the new tag and adjust the gate according to project
  policy.

全部勾选或记录豁免后，release owner 才能签字发版；逐项通过前不要创建 tag。

The release owner signs off only after every item passes or has a recorded waiver. Do not cut the tag before then.
