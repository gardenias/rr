---
sidebar_position: 7
---

# 在外部项目中引入 rr / Consuming rr from External Projects

本指南介绍如何在业务项目中把 rr 当作依赖引入。目前 rr 尚未发布到
crates.io，因此推荐通过 **git 依赖 + tag 锁版本** 的方式消费。

---

This guide shows how to depend on rr from a downstream business project. rr
is not yet published to crates.io, so the recommended path is a **git
dependency pinned to a tag**.

## 三种消费方式 / Three Consumption Modes

| 方式 | 上手成本 | 版本可控 | 适用场景 |
|---|---|---|---|
| **Git 依赖 + tag** | 低 | ✅ | 生产业务项目、CI、多人协作 |
| **Path 依赖** | 零 | ❌ | 本地联调 rr 和业务项目时 |
| **crates.io 发布** | 高 | ✅ | 对外分发 / 公开库（需要前置工作，见下） |

## 方式 A：Git + Tag（推荐）/ Git Dependency with Tag (Recommended)

### 前置条件 / Prerequisites

`rr` 目前是私有仓库。消费方必须满足：

1. 对 `github.com/gardenias/rr` 有读权限（SSH key 或 HTTPS PAT）
2. 在业务项目根添加 `.cargo/config.toml`，让 Cargo 用系统 git（libgit2 不支持
   很多本地凭据路径）：

   ```toml
   # my-biz/.cargo/config.toml
   [net]
   git-fetch-with-cli = true
   ```
3. **Rust 工具链 ≥ 1.98**。rr 的 workspace `rust-version = "1.98"`（edition2024 及
   新稳定特性所需），仓库根目录的 `rust-toolchain.toml` 会自动锁定对应工具链。
   推荐业务项目 `rust-toolchain.toml` 锁定 `channel = "1.98"` 或更高。

没有这三项任意一个，`cargo build` 会直接报错。

---

`rr` is a private GitHub repo. Consumers must (1) have read access via SSH
key or HTTPS PAT, (2) add `.cargo/config.toml` with
`net.git-fetch-with-cli = true` so Cargo uses the system `git` binary for
auth, and (3) use **Rust ≥ 1.88** — rr's declared `rust-version = "1.82"` is
stale. Transitive deps pull in `clap_lex` (requires `edition2024`, stable in
1.85) and `validit` (uses `let_chains`, stable in 1.88).

### 典型的 workspace `Cargo.toml`

```toml
[workspace]
members = ["crates/*"]
resolver = "2"

[workspace.package]
edition = "2021"
rust-version = "1.82"

[workspace.dependencies]
# ─── rr framework (lock all crates to the same tag) ──────────────────────
# Private repo — use SSH URL. For public release, switch to https://.
rr-core    = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v0.2.0-rc2" }
rr-server  = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v0.2.0-rc2" }
rr-client  = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v0.2.0-rc2" }
rr-raft    = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v0.2.0-rc2" }
rr-config  = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v0.2.0-rc2" }
rr-storage = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v0.2.0-rc2" }
rr-network = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v0.2.0-rc2" }
rr-router  = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v0.2.0-rc2" }

# ─── rr 公共 API 暴露的依赖 —— 版本必须与 rr 对齐 ────────────────────────
openraft           = { version = "0.9.21", features = ["serde"] }
tokio              = { version = "1", features = ["full"] }
serde              = { version = "1", features = ["derive"] }
bincode            = { version = "2.0.0", features = ["serde"] }
async-trait        = "0.1"
tracing            = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
anyhow             = "1"
```

### 根据角色选 crate / Pick crates by role

| 业务项目角色 | 需要的 crate |
|---|---|
| 纯客户端（调用已部署集群） | `rr-core` + `rr-client` |
| 自建 node（实现 `Service` + 跑 `RrNode`） | `rr-core` + `rr-server` + `rr-raft` + `rr-config` + `rr-storage` + `rr-network` + `rr-router` |
| 全栈（node + CLI 工具） | 上述全部 + `rr-client` |

### 为什么不推荐 `branch = "main"` / Why not track `main`

跟着 `main` 会在每次 `cargo update` 时漂到最新 commit，导致：

- CI 构建不可复现（同一个提交，两台机器结果不同）
- rr 侧的 breaking change 未经你同意就进入业务项目
- `Cargo.lock` 频繁变动，PR review 噪声大

固定到 tag 后，只有显式 `cargo update -p rr-core` + 手动改 `tag = "vX.Y.Z"`
才会升级。

---

Tracking `main` causes unreproducible CI builds and silent breaking changes
on `cargo update`. Pin to a tag and upgrade deliberately.

## 方式 B：Path 依赖 / Path Dependency

在 rr 和业务项目目录结构如下时：

```
Projects/
├── rr/              # this repo
└── my-biz/          # your business project
```

业务项目 `Cargo.toml`：

```toml
[dependencies]
rr-core   = { path = "../rr/crates/rr-core" }
rr-client = { path = "../rr/crates/rr-client" }
```

- 改 rr 代码立刻影响业务项目，适合联调
- **不能提交到公共 git**（别人 clone 不到 `../rr`）
- 不能 `cargo publish`（path 依赖必须带 `version =`）

## 方式 C：crates.io 发布 / Publish to crates.io

目前 rr 的 crate `Cargo.toml` 缺少发布必需字段。要发布需先做：

### 1. 补齐每个 crate 的元数据

```toml
[package]
name        = "rr-core"
version     = "0.2.0"
edition     = "2021"
license     = "MIT"
description = "Core types and traits for the rr distributed multi-Raft framework"
repository  = "https://github.com/gardenias/rr"
readme      = "../../README.md"
rust-version.workspace = true
```

### 2. 给内部 path 依赖加 version

crates.io 要求 path 依赖同时声明 version：

```toml
rr-core = { path = "../rr-core", version = "0.2.0" }
```

### 3. 按拓扑顺序发布 / Publish bottom-up

```
rr-core → rr-storage / rr-config → rr-network → rr-raft → rr-router → rr-client → rr-server
```

推荐用 [`release-plz`](https://release-plz.ieni.dev/) 或
[`cargo-workspaces`](https://github.com/pksunkara/cargo-workspaces) 自动化多
crate 发版。

## 常见陷阱 / Common Gotchas

### 1. 版本必须与 rr 对齐 / Version alignment is mandatory

`rr` 公共 API 暴露了 `openraft::BasicNode`、`tokio` 的 `Runtime`、
`bincode::serde::...` 等类型。业务项目用的版本**必须**和 rr 的
`workspace.dependencies` 一致，否则会报：

```
error[E0308]: mismatched types
 = note: expected struct `openraft::BasicNode` (0.9.21)
         found    struct `openraft::BasicNode` (0.9.0)
```

同一个 crate 的两个不同 patch 版本在 Cargo 看来是两个完全不同的类型。

---

rr exposes types from `openraft`, `tokio`, `bincode` in its public API.
Consumers must align those versions exactly, or Cargo will see them as
distinct types and fail type checking.

### 2. `rocks` feature 不会自动传递 / Features don't propagate automatically

业务项目想启用 RocksDB 存储时，必须在自己的 `[features]` 里显式打开：

```toml
[features]
default = []
rocks = ["rr-server/rocks", "rr-storage/rocks", "rr-raft/rocks"]
```

然后：

```bash
cargo build --features rocks
```

### 3. 首次构建耗时 / First build is slow

Cargo 会 clone 整个 rr workspace（包括所有 crate、examples、tests）并编译。
首次约 3-5 分钟。后续缓存在 `~/.cargo/git/` 和 `target/`。

若想加速：考虑把 rr vendor 到业务仓库内部，或配置 cargo sparse protocol。

### 4. Git 凭据配置 / Git credentials for private repos

见 **方式 A 前置条件**。私有仓库必须用 `net.git-fetch-with-cli = true`
（推荐放在业务项目的 `.cargo/config.toml`，而非 `~/.cargo/`，这样仓库自带
配置，新成员 clone 后即可 `cargo build`）。

---

See **Prerequisites** under Mode A. For private repos, put
`net.git-fetch-with-cli = true` in the **project-local** `.cargo/config.toml`
(not `~/.cargo/`) so new team members get the right config on clone.

### 5. `Cargo.lock` 策略 / Lockfile policy

业务项目（二进制 workspace）应该 **提交 `Cargo.lock`**。这样即便 rr 的 `main`
又前进了，`cargo build` 仍会用 lock 文件里固定的 git revision，构建可复现。

只有发布库 crate 时才建议把 `Cargo.lock` 加入 `.gitignore`。

## 升级 rr 版本 / Upgrading rr Version

```bash
# 1. 查看可用 tag
git ls-remote --tags https://github.com/gardenias/rr.git | grep -v '\^{}'

# 2. 全局替换业务项目里的 tag
sed -i '' 's|tag = "v0.2.0-rc2"|tag = "v0.2.0-rc3"|g' Cargo.toml

# 3. 更新 lock 文件里的 rr crates
cargo update -p rr-core -p rr-server -p rr-client -p rr-raft \
             -p rr-config -p rr-storage -p rr-network -p rr-router

# 4. 验证
cargo check --workspace --all-targets
cargo test
```

## 参考示例 / Reference Example

See the [`kv-store`](https://github.com/gardenias/rr/tree/main/examples/kv-store)
example in the rr repo for a complete node + service implementation. The same
layout works for any business domain (matching engine, counter, queue, etc.):

```
my-biz/
├── Cargo.toml                # workspace + [workspace.dependencies]
├── crates/
│   ├── biz-types/            # Serde request/response wire types
│   ├── biz-engine/           # Service trait impl + state machine
│   ├── biz-node/             # Binary: wires RrNode, ConfigProvider, Storage
│   └── biz-cli/              # Binary: uses RrClient to talk to the cluster
```
