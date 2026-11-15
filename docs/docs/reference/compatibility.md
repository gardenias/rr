---
sidebar_position: 10
---

# 兼容性与 API 稳定性 / Compatibility & API Stability

本文声明 rr 的版本兼容承诺，用于选择版本和规划升级。
This document states rr's compatibility commitments for choosing versions and planning upgrades.

## 版本号方案 / Versioning Scheme

rr 遵循 [Semantic Versioning 2.0.0](https://semver.org/)：`MAJOR.MINOR.PATCH`。
rr follows [Semantic Versioning 2.0.0](https://semver.org/): `MAJOR.MINOR.PATCH`.

| 版本位 / Component | 何时递增 / When it bumps |
| :--- | :--- |
| `MAJOR` | public Rust API、gRPC wire、磁盘格式或 config schema 的破坏性变更 / Breaking changes to public Rust APIs, gRPC wire, on-disk formats, or config schema |
| `MINOR` | 向后兼容的功能、API、gRPC 方法或可选 config 字段 / Backward-compatible features, APIs, gRPC methods, or optional config fields |
| `PATCH` | 向后兼容的缺陷与安全修复 / Backward-compatible bug and security fixes |

### Pre-1.0 语义 / Pre-1.0 Semantics

`1.0.0` 前采用 Cargo 惯例：`0.x.y` 中 `x` 递增可包含破坏性变更。因此 `0.2.0 → 0.3.0` 可破坏
API，`0.2.0 → 0.2.1` 不可。
Before `1.0.0`, rr follows Cargo convention: incrementing `x` in `0.x.y` may break APIs. Thus
`0.2.0 → 0.3.0` may break APIs, while `0.2.0 → 0.2.1` may not.

## 兼容性层次 / Compatibility Tiers

### 1. Rust 公共 API / Rust Public API

同一 MAJOR 内，docs.rs 可见的 `pub` 项保持源码兼容；`cargo update` 到同 MAJOR 的新版本后应无需
修改代码即可编译。
Within one MAJOR, every `pub` item visible on docs.rs remains source-compatible; `cargo update` within
that MAJOR should not require code changes.

以下不在承诺内 / Explicit non-commitments:

- 标记为 `#[doc(hidden)]` 的 API。
  APIs marked `#[doc(hidden)]`.
- 模块路径含 `__` 或 `internal` 的 API。
  APIs whose module path contains `__` or `internal`.
- 外部 trait 的 impl，因为 upstream 可添加默认方法。
  Implementations of external traits, because upstream may add default methods.
- MSRV 提升属于 MINOR，而非 MAJOR。
  MSRV increases are MINOR, not MAJOR, changes.

### 2. gRPC Wire Protocol

同一 MAJOR 内，已发布 `.proto` 字段保持 wire 兼容，新旧 client/server 可互通。新增字段必须向后兼容
（新的 `optional` 或 `repeated`），已分配字段号永不重用。
Within one MAJOR, published `.proto` fields remain wire-compatible so old and new clients and servers
interoperate. New fields must be backward-compatible (`optional` or `repeated`), and assigned field
numbers are never reused.

混合版本集群 / Mixed-version clusters:

- **patch 混合**（如 `0.2.0` + `0.2.1`）：支持，推荐滚动升级。
  **Patch-level mixing** (for example `0.2.0` + `0.2.1`) is supported and rolling upgrades are
  recommended.
- **pre-1.0 minor 混合**（如 `0.2.x` + `0.3.x`）：不支持，请一次性升级整个集群。
  **Pre-1.0 minor mixing** (for example `0.2.x` + `0.3.x`) is not supported; upgrade the whole cluster
  together.
- **1.0 之后**：minor 混合将被显式支持并记录在 CHANGELOG。
  **After 1.0**, supported minor-version mixing will be stated explicitly in the CHANGELOG.

### 3. 磁盘格式 / On-Disk Format (RocksDB Schema)

patch 升级（`0.2.0 → 0.2.1`）可直接滚动重启，无需迁移。
Patch upgrades (`0.2.0 → 0.2.1`) support direct rolling restarts without migration.

Raft 日志 payload 对存储层是不透明字节，apply 侧按 magic 前缀解释：`\0rrENT01` = `FramedEntry`，
`\0rrBAT01` = client 批处理帧（apply 时拆分），无 magic = 原始应用字节（v0.2.x 语义）。
Raft log payloads are opaque to storage and interpreted by magic prefix at apply: `\0rrENT01` means
`FramedEntry`, `\0rrBAT01` means a client batch frame split during apply, and no magic means raw
application bytes (v0.2.x semantics).

:::warning 批处理回滚 / Batching rollback
批处理开启期间产生的 `\0rrBAT01` 条目会被旧二进制作为单条不透明 payload 交给 service。回滚前
必须关闭 client 批处理并滚动重启。
Older binaries hand `\0rrBAT01` entries written while batching was enabled to the service as one opaque
payload. Disable client batching and perform a rolling restart before downgrading.
:::

跨 MAJOR 或 pre-1.0 MINOR 可能需要先 `rr-admin backup` → shutdown → clean `data_dir` → upgrade →
restore，或使用专门迁移工具。具体步骤会在该版本 CHANGELOG 说明。
Across MAJOR or pre-1.0 MINOR boundaries, migration may require `rr-admin backup` → shutdown → clean
`data_dir` → upgrade → restore, or a dedicated migration tool. The release CHANGELOG states the procedure.

### 4. Config File Schema

同一 MAJOR 内，已有 TOML 键的含义与默认值不变；新键始终 optional，默认值保持旧行为。重命名或删除键
需要 MAJOR bump。
Within one MAJOR, existing TOML keys retain their meaning and defaults. New keys are optional and default
to the old behavior. Renaming or removing a key requires a MAJOR bump.

rr-server 使用 `#[serde(deny_unknown_fields)]` 拒绝未知键，使拼写错误 fail-fast，而非静默降级。
rr-server uses `#[serde(deny_unknown_fields)]` to reject unknown keys, so typos fail fast instead of
silently degrading behavior.

### 5. CLI Flags & Env Vars

CLI 标志和 `RR_*` 环境变量在同一 MAJOR 内只做添加型变更；改名或删除需要 MAJOR。变更记录在
CHANGELOG 的 `Changed` / `Deprecated` / `Removed`。
CLI flags and `RR_*` environment variables are additive within one MAJOR; renaming or removal requires a
MAJOR bump. Changes appear under `Changed` / `Deprecated` / `Removed` in the CHANGELOG.

### 6. Metrics

同一 MAJOR 内，已发布 metric 名称和标签稳定；删除或重命名需要 MAJOR，新增 metric 或 label 属于 MINOR。
Published metric names and labels are stable within one MAJOR. Deleting or renaming one requires a MAJOR
bump; adding a metric or label is a MINOR change.

## 字节格式版本约定 / Byte-Format Versioning Policy

自 2026-09-23 起，任何新引入的协议层或持久化字节格式必须携带类型魔数前缀和显式版本位。解码端必须
拒绝 `version > 当前支持版本`，不得猜测未知布局。版本位仅在非兼容演进时递增；同版本内只允许新增
可选尾部字段。`crates/rr-core/tests/golden/` 金色语料与畸形/未来版本拒绝测试用于锁定格式。
Since 2026-09-23, every new protocol-layer or persisted byte format must carry a type magic prefix and an
explicit version field. Decoders must reject `version > supported` rather than guess an unknown layout.
Bump the version only for incompatible evolution; within one version, only optional trailer fields may be
added. The golden corpus in `crates/rr-core/tests/golden/` and malformed/future-version rejection tests
pin these formats.

同版本可选尾部字段支持老 reader/新 writer（老节点忽略尾部），新 reader 读到老 writer 的缺失字段时按
`None` 处理。新字段的生产者只能在全集群部署可解码新版本的二进制后开启：先全量部署，再逐台打开写入
开关。无法执行两阶段滚动时必须递增版本位，让老节点响亮拒解。
Optional trailer fields within one version support old readers with new writers, which ignore the trailer;
a new reader sees an absent old-writer field as `None`. Enable a producer of a new field only after every
node runs a binary that can decode it: deploy first, then enable writes node by node. If this two-phase
rollout is impossible, bump the version so old nodes reject the format loudly.

### 现有格式 / Existing Formats

| 格式 / Format | 位置 / Location | 魔数 / Magic | 版本 / Version | 备注 / Notes |
| :--- | :--- | :--- | :--- | :--- |
| `FramedEntry` | `rr-core/src/framed_entry.rs` | `\0rrENT01` | `u8 = 1`，拒绝未来版本 / `u8 = 1`; rejects future versions | 金色语料 / Golden corpus |
| `FramedSnapshot` | `rr-core/src/framed_snapshot.rs` | `\0rrSNAP1` | `u8 = 1` | 同上 / Same |
| `ApplyOutcome` | `rr-core/src/idempotency.rs` | `0xAA` | `version = 1` | 同上 / Same |
| 批处理帧 / Client batch frame | `rr-core/src/batch.rs` | `\0rrBAT01` | `version = 1` | 同上 / Same |
| RRAF TCP 复制帧 / RRAF TCP replication frame | `rr-network/tcp_raft_server.rs`, `rr-raft/tcp_transport.rs` | `RRAF` | `FRAME_VERSION = 1`，不匹配即拒 / `FRAME_VERSION = 1`; mismatch rejected | 实验性，仅 loopback / Experimental, loopback-only |
| gRPC wire (proto3) | `rr-network/proto/*.proto` | — | protobuf 字段号演进 / protobuf field-number evolution | 仅 additive 字段 / Additive fields only |
| openraft 内部 bincode / openraft internal bincode | `rr-raft` 存储/网络适配层 / `rr-raft` storage and network adapters | — | 无 / None | 精确钉版 `openraft =0.10.0-alpha.34`；升级即格式评审 / Exact pin `openraft =0.10.0-alpha.34`; upgrades require format review |
| `PersistentSnapshot` (`sm-snapshot.bin`, T1) | `rr-raft/src/snapshot_store.rs` | `\0rrPST01` | `version = 1` | 无 legacy 回退；旧裸 bincode fail-closed 拒启 / No legacy fallback; old raw bincode fails closed at startup |
| 成员 sidecar / Membership sidecar (`sm-membership.bin`, A-0016) | `rr-raft/src/sm_adapter.rs` | `\0rrMBR01` | `version = 1` | 旧文件 warn 后从空启动，刻意 fail-open / Old `sm-membership.bin` files warn and start empty, deliberately fail-open |

2026-09-23 格式升级未保留 legacy 兼容代码。0.2.x `sm-snapshot.bin` 会 fail-closed 启动失败，错误信息
指引删除后从 Leader 重建；`sm-membership.bin` 会 warn 后从空启动，再由 Raft 日志回放或快照恢复成员。
The 2026-09-23 format upgrade intentionally retained no legacy compatibility code. A 0.2.x
`sm-snapshot.bin` fails closed at startup, with recovery instructions to delete it and rebuild from the
leader. A 0.2.x `sm-membership.bin` is skipped with a warning and membership is rebuilt from the Raft log
or snapshot.

## 最低支持 Rust 版本 / Minimum Supported Rust Version (MSRV)

当前为 Rust `1.98`（workspace `Cargo.toml` 的 `rust-version`）。
The current MSRV is Rust `1.98`, set by `rust-version` in the workspace `Cargo.toml`.

- MSRV 提升属于 MINOR，并记录在 CHANGELOG `Changed`。
  MSRV bumps are MINOR changes recorded under CHANGELOG `Changed`.
- PATCH 不提升 MSRV。
  PATCH releases never raise MSRV.
- 目标为 `stable - 6 months`，为下游预留升级时间。
  The target is `stable - 6 months`, giving downstream users time to upgrade.

## 弃用流程 / Deprecation Process

1. **宣布 / Announce**：添加 `#[deprecated(since="x.y.z", note="use Foo instead")]`，并记录在
   CHANGELOG `Deprecated`。

   Add the attribute and record it under CHANGELOG `Deprecated`.
2. **宽限期 / Grace period**：至少保留一个完整 MINOR 周期，期间功能仍可用并可能发出警告。
   Keep the API functional for at least one full MINOR cycle, possibly with warnings.
3. **删除 / Remove**：在下一个 MAJOR 删除，并记录在 CHANGELOG `Removed`。
   Remove it in the next MAJOR and record it under CHANGELOG `Removed`.

pre-1.0 下“下一个 MAJOR”指下一个破坏性 MINOR。例如 v0.2.0 弃用的 API 可在 v0.3.0 删除，
`DurabilityPolicy::Balanced`/`Fast` 即为例。
Under pre-1.0 semantics, “next MAJOR” means the next breaking MINOR. For example, an API deprecated in
v0.2.0 may be removed in v0.3.0, as happened with `DurabilityPolicy::Balanced`/`Fast`.

## 支持政策 / Support Policy

| 版本线 / Branch | 接收内容 / Receives |
| :--- | :--- |
| Latest MAJOR | 新功能、bug 与安全修复 / Features, bug fixes, and security fixes |
| Previous MAJOR | 仅 N-1，6 个月安全与严重 bug 修复 / N-1 only: security and critical bug fixes for 6 months |
| Older | 不受支持，需升级 / Unsupported; upgrade required |

## 检查升级兼容性 / Checking Upgrade Compatibility

按顺序检查 / Check in this order:

1. **CHANGELOG.md**：破坏性变更列在 `## [x.y.z]` 下的 `### Changed` / `### Removed` / `### Breaking`。
   Breaking changes appear under `### Changed`, `### Removed`, or `### Breaking` below `## [x.y.z]`.
2. **所用 crate 的 docs.rs diff**：检查消失的公共 API。
   The docs.rs diff for your crate shows removed public APIs.
3. **本地 `cargo check`**：通过通常表示源码兼容；失败应由前两项解释。
   Your own `cargo check` verifies source compatibility; failures should be explained by the first two.

## 报告兼容性 Bug / Reporting Compatibility Bugs

PATCH 升级后 `cargo check` 失败或 wire mix 失败，请提交 bug，附最小复现、升级前后版本、实际与预期行为。
If `cargo check` or wire interoperability fails after a PATCH upgrade, report a bug with a minimal
reproducer, before/after versions, and actual versus expected behavior.

**邮件 / Email**：[gardenia.wu@outlook.com](mailto:gardenia.wu@outlook.com)（或提交 GitHub issue / or open a GitHub issue）。

## 相关文档 / Related Documents

- [CHANGELOG.md](https://github.com/gardenias/rr/blob/main/CHANGELOG.md)
- 开发流程 / Development workflow：[CONTRIBUTING.md](https://github.com/gardenias/rr/blob/main/CONTRIBUTING.md)
