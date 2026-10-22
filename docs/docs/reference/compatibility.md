---
sidebar_position: 10
---

# 兼容性与 API 稳定性 / Compatibility & API Stability

本文档声明 rr 项目的版本兼容性承诺，让用户能安全地选择版本、规划升级、判断何时需要更新代码。

---

This document states rr's versioning and compatibility commitments so users
can confidently choose a version, plan upgrades, and predict when code
changes will be required.

## 版本号方案 / Versioning Scheme

rr 遵循 [Semantic Versioning 2.0.0](https://semver.org/)：`MAJOR.MINOR.PATCH`。

rr follows [Semantic Versioning 2.0.0](https://semver.org/): `MAJOR.MINOR.PATCH`.

| 版本位 / Component | 何时递增 / When it bumps |
| :--- | :--- |
| `MAJOR` | 破坏性变更到 public Rust API / gRPC wire protocol / on-disk format / config schema |
| `MINOR` | 向后兼容的新功能、新 API、新 gRPC 方法、新 config 字段（可选） |
| `PATCH` | 向后兼容的 bug 修复，含安全修复 |

### Pre-1.0 语义 / Pre-1.0 Semantics

在达到 `1.0.0` 前，rr 采用 **Cargo 惯例**：`0.x.y` 版本中 `x` 的递增被视作破坏性变更。也就是说 `0.2.0 → 0.3.0` 可以有 breaking API changes；`0.2.0 → 0.2.1` 不可以。

Before `1.0.0`, rr follows the **Cargo convention**: a bump in `x` within
`0.x.y` is a breaking change. So `0.2.0 → 0.3.0` may break APIs; `0.2.0 →
0.2.1` may not.

## 兼容性层次 / Compatibility Tiers

不同的 rr 表面（API、wire、on-disk）有不同的稳定性强度。以下表格对每个层次给出具体保证。

Different surfaces of rr (Rust API, wire protocol, on-disk format, config)
have different stability guarantees. The table below spells out what each
tier commits to.

### 1. Rust Public API

**承诺 / Commitment**: 在同一个 MAJOR 版本内，任何在 docs.rs 上可见的 `pub` 项保持**源码级兼容**。你的代码在 `cargo update` 到同 major 的新版本后应无需修改即可编译。

**Within a single MAJOR release, any `pub` item visible on docs.rs is
source-compatible.** Your code should compile without modification after
`cargo update` within the same major.

**非承诺 / Explicit non-commitments**:
- `#[doc(hidden)]` 标注的 API
- 任何模块路径含 `__` 或 `internal` 的 API
- trait impl on 外部 trait（upstream 可能添加默认方法）
- MSRV 提升仅视为 MINOR 变更，不视为 MAJOR

### 2. gRPC Wire Protocol

**承诺 / Commitment**: 在同一个 MAJOR 版本内，所有已发布的 `.proto` 消息字段保持**线兼容**（老 client/server 能与新版本互通）。新增字段总是以**向后兼容**的方式（新 `optional` 或 `repeated`），字段编号一经使用永不重用。

**Within a single MAJOR release, all published `.proto` fields are
wire-compatible.** Old clients / servers can interoperate with new
versions. New fields are added in backward-compatible ways (new `optional`
or `repeated`); field numbers, once assigned, are never reused.

**混合版本集群 / Mixed-version clusters**:
- **patch-level mix** (例如 `0.2.0` + `0.2.1`): 支持，滚动升级推荐流程
- **minor-level mix** (例如 `0.2.x` + `0.3.x`): 跨 pre-1.0 版本 NOT supported —— 请一次性升级整个集群
- **1.0 之后**: minor 混合会被显式支持并在 CHANGELOG 记录

### 3. On-Disk Format (RocksDB schema)

**承诺 / Commitment**: patch-level 升级（`0.2.0 → 0.2.1`）可以直接滚动重启；无需迁移。

**日志 payload 解释约定 / Log payload interpretation**: Raft log 条目的 payload 对存储层是
不透明字节,但 apply 侧按 magic 前缀解释信封:`\0rrENT01` = `FramedEntry`(offset/幂等键),
`\0rrBAT01` = client 批处理帧(apply 时拆分,逐条进 service)。无 magic = 原始应用字节
(v0.2.x 语义)。**回滚注意**:批处理开启期间产生的 `\0rrBAT01` 条目被旧版本(无拆分逻辑)
按单条不透明 payload 交给 service——回滚前请关闭 client 批处理并滚动重启。

Entry payloads are opaque to storage but interpreted by magic prefix at apply:
`\0rrENT01` = `FramedEntry`, `\0rrBAT01` = client batch frame (split at apply),
no magic = raw application bytes (v0.2.x semantics). **Rollback**: entries written
while client batching was enabled are handed to the service as one opaque payload
by older binaries — disable client batching before downgrading.

在 MAJOR 或 pre-1.0 MINOR 间，可能需要:
- 先运行 `rr-admin backup` → shutdown → clean data_dir → upgrade → restore
- 或提供专门的迁移工具（会在该版本 CHANGELOG 说明）

**Across MAJOR or pre-1.0 MINOR boundaries**, migration may be required.
The CHANGELOG will explicitly state the procedure (offline backup+restore
or a dedicated migration tool). Check `CHANGELOG.md` before upgrading
across these boundaries.

### 4. Config File Schema

**承诺 / Commitment**: 已存在的 TOML 键在同 MAJOR 内保持其含义和默认值不变。新增键总是 optional（默认值保持"老行为"）。重命名 / 删除 键是 MAJOR bump。

**Within a MAJOR, existing TOML keys retain their meaning and defaults.**
New keys are always optional (with a default that preserves the old
behavior). Renaming or removing a key requires a MAJOR bump.

**严格模式检查 / Strict schema**: rr-server 用 `#[serde(deny_unknown_fields)]` 拒绝未知键，所以 typo 会 fail-fast，不会静默降级。

### 5. CLI Flags & Env Vars

**承诺 / Commitment**: CLI 标志和 `RR_*` 环境变量在同 MAJOR 内保持**添加型**（新增 OK、改名/删除需 MAJOR）。变更会在 CHANGELOG 的 `Changed` / `Deprecated` / `Removed` 小节声明。

CLI flags and `RR_*` env vars are **additive within a MAJOR**. Renaming or
removal requires a MAJOR bump. Changes are declared in CHANGELOG under
`Changed` / `Deprecated` / `Removed`.

### 6. Metrics

**承诺 / Commitment**: 已发布的 metric 名称和标签在同 MAJOR 内稳定。metric 的**删除或重命名**需要 MAJOR bump；**新增** metric 或 label 是 MINOR。

**Published metric names and labels are stable within a MAJOR.** Deleting
or renaming a metric requires a MAJOR bump; adding a new metric or label
is a MINOR.

## 字节格式版本约定 / Byte-Format Versioning Policy

**规则（2026-09-23 起生效）**：任何**新引入**的协议层/持久化字节格式必须携带
（a）类型魔数前缀与（b）显式版本位；解码端必须拒绝 `version > 当前支持版本`
（响亮失败，绝不猜测未知布局）。版本位只在该格式的**非兼容**演进时递增；
同版本内只允许新增可选尾部字段。回归由金色语料（`crates/rr-core/tests/golden/`）
与畸形/未来版本拒绝测试钉死。

**Rule (effective 2026-09-23)**: every NEW protocol-layer or persisted byte format
MUST carry (a) a type magic prefix and (b) an explicit version field, and decoders
MUST reject `version > supported` loudly. Bump the version only on incompatible
evolution; same-version changes may only append optional trailer fields. The golden
corpus (`crates/rr-core/tests/golden/`) plus malformed/future-version rejection
tests pin the formats.

**前向兼容边界 / Forward-compatibility boundary**：同版本内新增可选尾部字段只在
**老 reader / 新 writer** 方向安全（老节点忽略不认识的尾部字节）。反过来——老 writer /
新 reader 读到缺失字段——解码端按 `None` 处理，同样安全；但**新字段的生产者**只有在
全集群都跑上新二进制后才允许开启（两阶段滚动：先全量部署"能解码新字段"的版本，再逐台
打开写入开关）。无法安排两阶段滚动时，必须递增版本位，让老节点响亮拒解而不是猜。

Additive optional trailer fields within one format version are safe in the
**old-reader / new-writer** direction only (old nodes ignore unknown trailer
bytes). The reverse — a new reader seeing the field absent — decodes as
`None` and is likewise safe; but a **producer of a new field may only be
enabled after every node runs the new binary** (two-phase rollout: deploy the
"can-decode" build fleet-wide first, then flip the write switch node by node).
When a two-phase rollout cannot be scheduled, the version byte MUST be bumped
so old nodes reject loudly instead of guessing.

### 现有格式盘点 / Inventory of Existing Formats

| 格式 / Format | 位置 | 魔数 | 版本位 | 备注 |
|---|---|---|---|---|
| `FramedEntry`（日志条目信封） | `rr-core/src/framed_entry.rs` | `\0rrENT01` | `version: u8`（=1），拒绝未来版本 | 金色语料钉死 |
| `FramedSnapshot`（快照信封） | `rr-core/src/framed_snapshot.rs` | `\0rrSNAP1` | `version: u8`（=1） | 同上 |
| `ApplyOutcome`（apply 响应信封） | `rr-core/src/idempotency.rs` | tag `0xAA` | `version`（=1） | 同上 |
| 批处理帧（client batching） | `rr-core/src/batch.rs` | `\0rrBAT01` | `version`（=1） | 同上 |
| RRAF TCP 复制帧 | `rr-network/tcp_raft_server.rs`、`rr-raft/tcp_transport.rs` | `RRAF` | 头部 `FRAME_VERSION`（=1），不匹配即拒 | 实验性，loopback 门控 |
| gRPC wire（proto3） | `rr-network/proto/*.proto` | — | 字段号演进（protobuf 内建） | 只允许 additive 字段 |
| openraft 内部 bincode（raft log 条目、RPC payload） | `rr-raft` 存储/网络适配层 | — | 无（依赖 `openraft =0.10.0-alpha.34` 精确钉版承担格式稳定） | openraft 升级即格式评审点 |
| `PersistentSnapshot`（`sm-snapshot.bin`，T1） | `rr-raft/src/snapshot_store.rs` | `\0rrPST01` | `version`（=1） | 2026-09-23 包信封；**无 legacy 回退**——旧裸 bincode 文件按 fail-closed 响亮拒启（错误信息附恢复 runbook） |
| 成员 sidecar（`sm-membership.bin`，A-0016） | `rr-raft/src/sm_adapter.rs` | `\0rrMBR01` | `version`（=1） | 同上日期包信封；旧文件 warn + 从空启动（刻意 fail-open 语义不变），无 legacy 回退 |

**2026-09-23 格式升级说明**：上两行的两个持久化文件在该日一步包入信封（项目处于
预采用阶段，刻意**不保留** legacy 兼容代码）。0.2.x 旧文件：`sm-snapshot.bin`
触发 fail-closed 启动失败（错误信息指引删除后从 leader 重建）；`sm-membership.bin`
warn 后从空启动，成员关系由 raft 日志回放/快照恢复重建。

**Format upgrade note (2026-09-23)**: both persisted files were enveloped in one
step with NO legacy fallback (project is pre-adoption). A 0.2.x `sm-snapshot.bin`
now aborts startup fail-closed (the error names the recovery runbook);
a 0.2.x `sm-membership.bin` is skipped with a warn and membership is rebuilt
from the raft log/snapshot as before.

## Minimum Supported Rust Version (MSRV)

**当前 / Current**: Rust `1.98`（workspace `rust-version` field in `Cargo.toml`）。

**Policy**:
- MSRV 提升视为 **MINOR** 变更，在 CHANGELOG `Changed` 小节显式声明。
- 不会在 PATCH 内提升 MSRV。
- 目标是跟踪 `stable - 6 months` 的 Rust 版本，给下游提供缓冲。

An MSRV bump is a **MINOR** change, announced in CHANGELOG `Changed`. MSRV
will not rise in a PATCH. Target is `stable - 6 months` to give
downstream users time to catch up.

## 弃用流程 / Deprecation Process

1. **Announce** — 在即将弃用的 API 上加 `#[deprecated(since="x.y.z", note="use Foo instead")]`；CHANGELOG 的 `Deprecated` 小节记录。
2. **Grace period** — 至少一整个 MINOR 周期保留已弃用 API；期间继续工作（可能发出警告）。
3. **Remove** — 在下一个 MAJOR 删除；CHANGELOG 的 `Removed` 小节记录。

> pre-1.0 语义下，"下一个 MAJOR" 指**下一个 breaking（MINOR）版本**——例如
> v0.2.0 弃用的 API 在 v0.3.0 删除（`DurabilityPolicy::Balanced`/`Fast` 即此例）。
> Under pre-1.0, "next MAJOR" means the **next breaking (MINOR) release** —
> an API deprecated in v0.2.0 is removed in v0.3.0 (e.g.
> `DurabilityPolicy::Balanced`/`Fast`).

1. **Announce** — add `#[deprecated(since="x.y.z", note="use Foo instead")]`;
   log under CHANGELOG `Deprecated`.
2. **Grace period** — keep the deprecated API for at least one full MINOR
   cycle (still functional, may emit warnings).
3. **Remove** — drop in the next MAJOR; log under CHANGELOG `Removed`.

## 支持政策 / Support Policy

| 版本线 / Branch | 接收 / Receives |
| :--- | :--- |
| Latest MAJOR | 新功能、bug 修复、安全修复 |
| Previous MAJOR | 安全修复 + 严重 bug 修复 (N-1 only)，为期 6 个月 |
| Older | 不受支持，需升级 |

| Branch | Receives |
| :--- | :--- |
| Latest MAJOR | Features, bug fixes, security fixes |
| Previous MAJOR | Security + critical bug fixes (N-1 only), for 6 months |
| Older | Unsupported; upgrade required |

## 如何判断一次升级是否破坏兼容性 / How to Tell If an Upgrade Breaks You

按顺序查看三个地方：

Look at three things, in order:

1. **CHANGELOG.md** — 所有破坏性变更会在 `## [x.y.z]` section 下明确列在 `### Changed` / `### Removed` / `### Breaking` 里。
2. **The diff in docs.rs for the crate you use** — 消失的 public API 会明确列出。
3. **Your own `cargo check`** — 通过就几乎可以确定是安全的；失败的话，上面两处应该能解释原因。

## 报告兼容性 bug / Reporting Compatibility Bugs

如果在 PATCH 升级后 `cargo check` 失败（或 wire mix 失败），请作为 bug 报告：

If `cargo check` fails after a PATCH upgrade (or wire mix breaks), please
file it as a bug:

- 最小复现 / Minimal reproducer
- 升级前/后的版本号 / Before-and-after versions
- 实际 vs 预期行为 / Actual vs expected behavior

邮件 / Email: gardenia.wu@outlook.com（或 GitHub issue）。

## 相关 / Related

- [CHANGELOG.md](https://github.com/gardenias/rr/blob/main/CHANGELOG.md)
- [CONTRIBUTING.md](https://github.com/gardenias/rr/blob/main/CONTRIBUTING.md) — 开发者流程 / dev workflow
