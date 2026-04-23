# rr — Project Context

> Top-level session-handoff / onboarding file. Each session should UPDATE
> the sections below rather than rewrite. Per-phase deep dives live in
> `.sisyphus/notepads/<phase>/{decisions,learnings,problems}.md`.

**Last updated**: 2026-04-23
**Current phase**: Phase 3 (production-readiness) — core tasks complete, v0.2.0-rc1 shipped
**Active plan**: `.sisyphus/plans/rr-phase3-prodready.md`
**Latest tag**: `v0.2.0-rc1`
**Main branch HEAD**: pushed to `origin/main`

---

## 项目一句话 / Project One-Liner

rr 是 Rust 写的分布式 Multi-Raft 共识框架：openraft 0.9 + tonic gRPC + RocksDB，
9 个 crate（core / storage / config / raft / network / router / server / client / admin），
两个 example（counter, kv-store），完整的集成测试 + chaos framework + Docusaurus 文档站。

---

## 已完成的工作 / Completed Work

### Phase 2 基线 (v0.1.0, 2026-04 完成)
- Multi-Raft 共识（单节点承载多个独立 Raft group）
- gRPC networking with ChannelPool
- 可插拔存储：MemStorage + RocksStorage (feature-gated)
- Dynamic ConfigProvider trait（MemConfigProvider 实现）
- Client SDK 带自动 leader-redirect + 指数退避重试
- Snapshot streaming
- 24 个集成测试 + fault-injection 框架（但 4 chaos tests 被 `#[ignore]` 禁用）
- CI：13 个 job，cargo-audit + llvm-cov + chaos + macOS

### Phase 3 核心交付 (2026-04-22~23, v0.2.0-rc1 + rc2 候选)

**Wave A — Quick wins & correctness**:
- LICENSE (MIT), CHANGELOG, CONTRIBUTING, CODE_OF_CONDUCT, PR template
- `DurabilityPolicy { Strict, Balanced, Fast }` — **修复了严重 bug**：RocksDB
  之前既没调 `set_sync(true)` 也没 `set_use_fsync(true)`，Raft 持久性合约被
  破坏。现默认 `Strict`。
- `RaftNode::new_*` 返回 `Result<Self, RrError>`，不再 panic；新增
  `RaftError::InitializationFailed`。
- **Chaos tests 启用**：重构 `FaultAwareChannelPool` 为**双向隔离**
  （per-node pool sharing FaultState）。之前只阻断入站连接，leader
  "被隔离"后仍能发 heartbeat，所以 `test_leader_isolation_and_recovery`
  永远超时。修复后 4 个 `#[ignore]` chaos tests 全部通过。
- C4 架构图套件（13 个 Mermaid 文件：Context/Containers/4 × Components/
  Deployment/5 × Dynamic flows）。
- Metrics 文档补齐 14 个（之前只有 6 个）。

**Wave B — Deployability**:
- **rr-server 二进制**（替换 4 行 stub）：clap CLI + TOML config + env var
  overrides + SIGTERM/SIGINT graceful shutdown + built-in EchoStateMachine。
- 3 个示例 config（single-node, 3node-cluster-node{1,2,3}）。
- Dockerfile（multi-stage，非 root UID 10001）+ docker-compose + K8s
  StatefulSet + Helm chart → `ghcr.io/gardenias/rr`。
- `.github/workflows/release.yml` 在 tag push 时推 Docker + Helm OCI。

**Wave C — Security**:
- TLS via rustls (`tonic tls-ring` feature) + optional mTLS。NodeConfig
  带 `tls_config`；rr-raft/ChannelPool 本地重复定义 `TlsPaths` 避免循环
  依赖。
- `scripts/gen-dev-certs.sh` — CA + per-node 自签名证书，SAN 覆盖
  `localhost`, `127.0.0.1`, `rr-N`, `nodeN.rr.local`。
- `docs/docs/deployment/tls.md` — 完整 TLS + 故障排查指南。
- Bearer-token auth（`rr-network::auth`）：AdminService 独立 token，
  ClientService 可选，RaftService 只靠 mTLS。Constant-time compare
  防止 timing attack。Token 通过 env var 引用 —— 不存在 config 文件。
- **Rate limiting**（T18）：`tokio::Semaphore` 自实现 token bucket（零新依赖），
  `RateLimitInterceptor` + `ChainedInterceptor<Auth, RateLimit>` 合成。
  **RaftService 永远不限流**（代码结构上禁止）—— throttling Raft 会
  破坏选举。耗尽返回 `RESOURCE_EXHAUSTED`。
- **Audit log** via tracing target `rr::audit`：每个 AdminService RPC
  发 structured event（op/peer/node_id/result）。Mutation RPC 发 2 个
  event 做取证配对。`docs/docs/deployment/audit-log.md` 讲 schema + 路由。

**Wave D — Operations**:
- **Backup/restore**（offline）：`rr-admin backup/restore/inspect-backup` —
  `tar.gz(metadata.json + data/)` 格式。Node 必须停机。有 `format_version`
  做版本兼容性。6 个 unit tests + e2e roundtrip 验过。
- **5 个运维 runbook** 在 `docs/docs/operations/runbooks/`：disk-full,
  leader-flapping, network-partition, snapshot-corruption,
  configprovider-unreachable。统一 Symptom→Detection→Mitigation→RCA→
  Recovery→Prevention 格式。

**Wave E — Observability + Release**:
- **HTTP /health /ready /metrics**（`rr-server::health`）— hyper 单 server
  共用 `metrics_port`。`/ready` 由 `ReadinessFlag` 驱动（start() 置 true，
  shutdown() 置 false）。K8s probe 从 TCP 改成 HTTP。
- **W3C traceparent**（`rr-network::tracing`）— 轻量 header 透传，不含
  OpenTelemetry SDK（~12MB deps）。用户 bring-your-own。
- **rr-client 客户端 metrics** — request_duration_seconds / retries_total
  / leader_cache_hits_total / _misses_total，全部 `group_id` 标签。
- `#[instrument]` 覆盖 ClientDispatchImpl / RaftGroupLookupImpl /
  RrClient::send_request*。
- **Semver policy doc**（`docs/docs/reference/compatibility.md`）— 分
  surface 的稳定性承诺、MSRV 策略、deprecation 流程。

### 本次 session 最后做的（2026-04-23 午后）

- **系统代码覆盖率报告** — `cargo-llvm-cov 0.8.5` 全工作区：
  - **76.6% 行 / 79.3% region / 77.9% 函数**
  - 最强：rr-router 97.5%, rr-config 95.9%, rr-core 95.8%, rr-client 94.4%
  - 最弱：rr-admin 39.3%（main.rs + REPL 没测）, rr-server 68.6%
    （main.rs + health.rs 没测）
  - HTML + LCOV + JSON 报告在 `target/coverage/`
  - Nightly `--branch` 触发 LLVM SIGSEGV，已知 bug；stable 的 region
    coverage 实际等价于分支覆盖率（region 粒度包含条件分支）

---

## 未完成 / 需要重做 / Open Work

### 仍在 Phase 3 计划但未做

- **F1-F4 独立评审**（before `v0.2.0` final）：
  - F1 Plan compliance audit（建议用 oracle agent 跑）
  - F2 Security review（oracle）
  - F3 Manual Docker smoke test
  - F4 Upgrade path from Phase 2 → v0.2.0
- **`v0.2.0-rc2` 标签** — rc1 之后又 push 了 8 个 commit，如果要让 release
  workflow 发最新的镜像和 Helm chart，需要再打一个 rc2。

### 覆盖率维度的后续（基于最新的报告）

- **删除 vestigial stubs**（0% 覆盖的死代码）：
  - `rr-network/src/client.rs`（11 行）— `RrGrpcClient::connect` 没被调用
  - `rr-network/src/server.rs`（9 行）— `RrGrpcServer` 未使用
- **补 `rr-server/src/health.rs` 集成测试**（当前 23.6%）— spawn node，
  curl /health /ready /metrics，验证 200/503 + body。可一次拉到 90%+。
- **修 CI 覆盖率命令** — `.github/workflows/ci.yml` 的 coverage job 用
  `cargo llvm-cov --workspace --lcov` 没带 `--features rr-storage/rocks`，
  Codecov 里 RocksDB 路径完全缺失。一行改。
- **补 rr-admin REPL 测试**（当前 15.2%）— 或者把 REPL 拆到单独 module
  更容易 mock。

### 已知跨 session 的待办（非本 session 引入）

- Online backup via snapshot-fetch RPC（当前只有 offline）
- ChangeMembership RPC 自动化（当前 join learner 后需要手动提升 voter）
- 多语言 client SDK（Go/Python/Java）— proto-first 设计让这个 tractable
- Jepsen-style linearizability testing

---

## 下一步工作 / Next Session's Candidate Work

**完整计划已写入 `.sisyphus/plans/rr-phase3.1-hardening.md`**（Phase 3.1
分 Tier 0 / Tier 1 / Tier 2，含依赖矩阵和验收标准）。

摘要 / Summary:

**Tier 0 — v0.2.0 Final 前必做（~3-4 天）**:
1. 验证 `v0.2.0-rc1` release workflow 在 GitHub Actions 跑通（GHCR 权限检查）
2. Three-in-one cleanup commit：删 vestigial stubs + CI coverage 加 rocks feature + 修 32 个 `io_other_error` clippy warning
3. `rr-server/src/health.rs` 集成测试（24% → 90%）
4. F1-F4 independent reviews via oracle agent
5. 打 `v0.2.0-rc2`（Phase 3 continuation 8 个 commit 进镜像）
6. 切 `v0.2.0` final + crates.io 首发
7. Release notes + announcement

**Tier 1 — Phase 3.1 Hardening（~6-8 周，三条线并行）**:
- Theme A 可观测：`rr_raft_replication_lag` 指标、caller identity → audit log、
  Grafana dashboard + Prometheus alerts、OpenTelemetry 全栈示例
- Theme B 运维自动化：online backup via snapshot-fetch RPC、
  auto-promote learner → voter、`DecommissionNode` workflow、
  `/ready` 返回详细 JSON body
- Theme C 测试加固：TLS+auth e2e 集成测试、proto fuzzing、
  iptables 级真网络分区 chaos、crash-during-write 耐久性测试、
  Jepsen-style linearizability

**Tier 2 — Post-v1.0 愿景**:
多语言 SDK（Go/Python/Java）、etcd ConfigProvider、
secret rotation、cosign 镜像签名、SBOM、alt storage backends、
cross-partition transactions、partition split/merge 等。

---

## 常见陷阱 / Common Pitfalls (Lessons Learned This Phase)

- **RocksDB fsync 不配置 = Raft 持久性被破坏**。默认行为是 WAL 不 fsync，
  节点 crash 可能丢已 commit 的条目。永远用 `DurabilityPolicy::Strict`
  除非你真的理解后果（replicated 数据 + 定期备份）。
- **`cargo-llvm-cov --branch` 在 nightly 上 crash LLVM**（v0.8.5 +
  nightly 1.97）。目前不能依赖。Stable 的 region coverage 是替代。
- **tonic 0.14 的 TLS 需要显式 `features = ["tls-ring"]`**，而且 rustls
  0.23 要求 `install_default()` crypto provider，否则 first handshake
  panic。rr-server 在 `main()` 最早处调用了。
- **rr-raft 和 rr-network 有本地的 ChannelPool 重复定义** — 故意打破循环
  依赖。改一边要记得改另一边（包括 `TlsPaths` 新字段）。
- **`FaultAwareChannelPool` isolate 语义必须是双向的** — 单向（只拦入站）
  不能触发 leader 的 step-down，所有 leader-isolation 相关 chaos test 都
  会失败。
- **tonic interceptor 不能 Clone 是个坑** — `AsciiMetadataValue::parse`
  在某些 ASCII-like unicode（比如 💩）上不会 fail，测试要用 `\n\r` 这类
  strict invalid chars。
- **`main.rs` 的 `clap` derive 里的 `///` 会变成 `--help` 输出** — 不
  是纯 dev 文档，任何 CLI flag 的文档字符串都是用户界面。
- **gitnexus 索引会被 commit 使其陈旧**，运行 `npx gitnexus analyze`
  刷新（不带 embeddings，否则会删除）。

---

## 未来想法记录 / Future Ideas

- **Per-caller rate limiting**（现在只有 per-service）：需要知道 caller
  identity，依赖 auth module 的 subject claim。和 audit log 的 caller
  identity 工作可以合并。
- **`rr-client` OpenTelemetry 集成示例** — 在 examples/otel-client/ 里
  演示用 tracing-opentelemetry 把 trace context 传到 rr 集群。
- **etcd-backed ConfigProvider** 放在 `contrib/rr-config-etcd` 独立
  crate（避免 rr-config 绑定到 etcd client 依赖）。
- **Raft LogStorage 的 RocksDB column family 单独调优** — 分离
  raft_log 和 state_machine 的 block_cache_size / write_buffer。
- **Prometheus recording rules** — 发布一组推荐的 query（leader 稳定
  性、proposal p99、replication lag 等），让 ops 直接 import。
- **`rr-admin change-membership` 自动化** — 当前 `add_learner` 后必须
  手动调 `change_membership` 提升为 voter；应该支持 "auto-promote after
  caught-up" 模式。

---

## 关键文件索引 / Key File Index

### 架构文档
- `docs/docs/architecture/c4/README.md` — C4 图套件入口（13 个图）
- `docs/docs/architecture/{overview,crate-map,multi-raft,networking,storage,data-flow}.md` — 散文版

### 部署
- `docs/docs/deployment/deployment.md` — 通用部署
- `docs/docs/deployment/tls.md` — TLS/mTLS 完整指南
- `docs/docs/deployment/audit-log.md` — 审计日志架构
- `docs/docs/deployment/kubernetes.md` — K8s + Helm
- `docs/docs/operations/runbooks/*.md` — 5 个运维手册

### 参考
- `docs/docs/reference/metrics-reference.md` — 14 个 server-side + 4 个 client-side metric
- `docs/docs/reference/compatibility.md` — semver / API stability policy

### 源码关键入口
- `crates/rr-server/src/main.rs` — 生产二进制
- `crates/rr-server/src/node.rs` — RrNode 生命周期
- `crates/rr-raft/src/group_manager.rs` — Multi-Raft 核心
- `crates/rr-network/src/{auth,tls,rate_limit,tracing}.rs` — 安全/可观测性中间件
- `crates/rr-admin/src/backup.rs` — 备份/恢复

### 本次 session 的计划
- `.sisyphus/plans/rr-phase3-prodready.md` — Phase 3 完整 roadmap（~30 任务 × 5 wave）

### 覆盖率报告
- `target/coverage/html/html/index.html` — HTML 可点击报告
- `target/coverage/lcov.info` — Codecov 兼容格式
- `target/coverage/coverage.json` — 可编程消费的 summary

---

## Git 状态

- `main` 已 push 到 `origin/main`（最新 commit: `acc3c8b` rate limiting）
- `v0.2.0-rc1` 标签已 push（指向 `a9edbb9`）
- v0.2.0-rc1 之后的 8 个 commit 还没有 tag。
- 工作树干净，无 uncommitted 改动。

---

## 项目不变量 / Project Invariants (NEVER break)

这些是已经确立的决定，后续 session 不应违反：

- **`rr-core` 必须是 DAG root**，零依赖其他 `rr-*` crate。
- **Library crate 不用 `anyhow`**（binary / example / test 可以）。
- **Library crate 不用 `println!` / `eprintln!`**，只用 `tracing`。
- **Hot path 不用 `.unwrap()` / `.expect()`**，init 代码可以。
- **所有 `tokio::spawn` 必须有 tracked JoinHandle**，不要 fire-and-forget。
- **RocksDB 必须走 `spawn_blocking`**，不要在 tokio 主 runtime 里直接 `db.put()`。
- **没有全局可变状态 / static singleton** — state 归 `RrNode` 拥有。
- **`RaftService` 永远不限流**（T18 文档+代码双重强调）。
- **不要 `rm -rf` 任何 RocksDB 文件**（即使是 `.log`）— RocksDB 依赖完整文件布局。
- **遵循 semver**（`docs/docs/reference/compatibility.md`）— 0.x.y 里 x 是 breaking。
