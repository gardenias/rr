---
sidebar_position: 7
---

# 在外部项目中引入 Rr / Consuming Rr from External Projects

Rr 尚未发布到 crates.io。业务项目应使用固定 tag 的 git 依赖；path 依赖只用于本机联调。
Rr is not published to crates.io. Downstream projects should use tag-pinned git dependencies; path dependencies are for local development.

| 方式 / Mode | 版本固定 / Reproducible Version | 用途 / Use |
| :--- | :--- | :--- |
| git + tag | 是，并提交 `Cargo.lock` / Yes, with `Cargo.lock` committed | CI 和业务项目 / CI and downstream projects |
| path | 取决于本机 checkout / Depends on the local checkout | 同机修改 Rr 与业务项目 / Local Rr and application development |
| crates.io | 当前不可用 / Not currently available | 需先完成发布准备 / Requires release preparation |

## 使用 Git 和 Tag / Use Git with a Tag

私有仓库消费方需要 `github.com/gardenias/rr` 的读权限，以及 SSH key 或 HTTPS PAT。让 Cargo 调用系统 git 以复用本机认证：
Consumers of the private repository need read access to `github.com/gardenias/rr` through an SSH key or HTTPS PAT. Let Cargo invoke system git to reuse local authentication:

```toml
# <business-project>/.cargo/config.toml
[net]
git-fetch-with-cli = true
```

使用 Rust 1.98 或更高版本。将所有 Rr crate 固定到同一 tag：
Use Rust 1.98 or newer and pin every Rr crate to the same tag:

```toml
[workspace]
members = ["crates/*"]
resolver = "2"

[workspace.package]
edition = "2021"
rust-version = "1.98"

[workspace.dependencies]
rr-core = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v1.0.0" }
rr-server = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v1.0.0" }
rr-client = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v1.0.0" }
rr-raft = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v1.0.0" }
rr-config = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v1.0.0" }
rr-storage = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v1.0.0" }
rr-network = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v1.0.0" }
rr-router = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v1.0.0" }

openraft = { version = "=0.10.0-alpha.34", features = ["serde"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
bincode = { version = "2.0.0", features = ["serde"] }
async-trait = "0.1"

# Rr workspace 的 [patch] 不会传递给下游；必须显式使用同 tag 的 vendored openraft。
# Rr's workspace patch is not transitive; select the vendored openraft explicitly.
[patch.crates-io]
openraft = { git = "ssh://git@github.com/gardenias/rr.git", tag = "v1.0.0" }
```

按角色选择依赖：
Select crates by role:

| 项目角色 / Project Role | 依赖 / Dependencies |
| :--- | :--- |
| 只调用集群 / Client only | `rr-core`, `rr-client` |
| 自建节点 / Custom node | `rr-core`, `rr-server`, `rr-raft`, `rr-config`, `rr-storage`, `rr-network`, `rr-router` |
| 节点与客户端工具 / Node and client tools | 上述全部及 `rr-client` / All of the above plus `rr-client` |

不要用 `branch = "main"` 代替固定 tag；否则 `cargo update` 会引入未经选择的变更。
Do not replace the tag with `branch = "main"`; otherwise `cargo update` can introduce changes you did not select.

## 使用 Path 依赖 / Use Path Dependencies

当 Rr 与业务项目位于同一父目录时：
When Rr and the business project share a parent directory:

```text
Projects/
├── rr/
└── my-biz/
```

```toml
[dependencies]
rr-core = { path = "../rr/crates/rr-core" }
rr-client = { path = "../rr/crates/rr-client" }
```

path 依赖可提交，但其他机器必须有相同目录布局。发布 crate 时，path 依赖还需同时声明 `version`。
Path dependencies can be committed, but other machines need the same layout. Publishing a crate also requires a `version` on each path dependency.

## 准备 crates.io 发布 / Preparing a crates.io Release

发布前，每个 crate 需具备完整 package metadata、license、description、repository 和 readme，内部 path 依赖需声明匹配版本。按依赖拓扑从底层向上发布，并逐个验证 `cargo package`。
Before publishing, every crate needs complete package metadata, license, description, repository, and readme, with matching versions on internal path dependencies. Publish in dependency order and verify each crate with `cargo package`.

当前指南不代表这些 crate 已可发布；实际发布状态以 crates.io 和各 manifest 为准。
This guide does not imply that the crates are currently publishable; check crates.io and the manifests for actual status.

## 依赖与 Feature 陷阱 / Dependency and Feature Pitfalls

**公共依赖版本 / Public dependency versions**

Rr 的公共 API 暴露 openraft、tokio、bincode 等类型。业务项目必须与 Rr workspace 对齐这些版本，否则同名类型会来自不同 crate 版本并报 `mismatched types`。
Rr exposes openraft, tokio, bincode, and other types in public APIs. Align them with the Rr workspace or Cargo will report mismatched types from different crate versions.

仅添加 Rr 的 git 依赖不会传递根仓库的 `[patch.crates-io]`。如果省略上面的 openraft patch，下游会解析到未修补的
crates.io 包。用 `cargo tree -i openraft` 确认唯一来源是所选 Rr tag，再继续构建。

Rr git dependencies alone do not propagate the root repository's `[patch.crates-io]`. Without the openraft patch above,
downstream builds resolve the unpatched crates.io package. Run `cargo tree -i openraft` and confirm that its only source
is the selected Rr tag before building.

**Rocks 特性 / Rocks feature**

下游启用 RocksDB 时显式转发 feature：
Forward the feature explicitly when the downstream project needs RocksDB:

```toml
[features]
default = []
rocks = ["rr-server/rocks", "rr-storage/rocks", "rr-raft/rocks"]
```

```bash
cargo build --features rocks
```

**锁文件 / Lockfile**

二进制应用 workspace 应提交 `Cargo.lock`，以固定 git revision 和传递依赖。库是否提交 lockfile 按项目发布策略决定。
Application workspaces should commit `Cargo.lock` to pin git revisions and transitive dependencies. Libraries should follow their own release policy.

## 升级版本 / Upgrading

查看远端 tag，编辑所有 Rr 依赖的 tag，再更新 lockfile 并验证：
List remote tags, edit every Rr dependency to the selected tag, then update and verify:

```bash
git ls-remote --tags https://github.com/gardenias/rr.git
cargo update -p rr-core -p rr-server -p rr-client -p rr-raft \
  -p rr-config -p rr-storage -p rr-network -p rr-router
cargo check --workspace --all-targets
cargo test
```

不要只升级部分 Rr crate；混合 tag 容易产生类型或协议不兼容。
Do not upgrade only a subset of Rr crates; mixed tags can cause type or protocol incompatibilities.

## 项目结构参考 / Reference Layout

完整节点与服务实现见 [`kv-store`](https://github.com/gardenias/rr/tree/main/examples/kv-store)。业务项目可按职责拆分：
See [`kv-store`](https://github.com/gardenias/rr/tree/main/examples/kv-store) for a complete node and service. A downstream project can split responsibilities as follows:

```text
my-biz/
├── Cargo.toml
└── crates/
    ├── biz-types/    # wire 类型 / wire types
    ├── biz-engine/   # Service 实现 / Service implementation
    ├── biz-node/     # 节点组装 / node assembly
    └── biz-cli/      # 客户端工具 / client tooling
```
