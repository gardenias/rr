# rr — Project Context

> Top-level session-handoff / onboarding file. Each session should UPDATE
> the sections below rather than rewrite. Per-phase deep dives live in
> `.sisyphus/notepads/<phase>/{decisions,learnings,problems}.md`.
> Forward-looking design decisions live in `.sisyphus/adr/A-NNNN-*.md`.

**Last updated**: 2026-04-26
**Current phase**: Phase 4 v0.3.0 — feature-complete, awaiting tag/release
**Active plan**: `.sisyphus/plans/rr-phase4-v030.md` + `.sisyphus/plans/rr-phase3.1-hardening.md`
**Latest tag**: `v0.2.0-rc1` (v0.3.0 candidate is current `main` HEAD)
**Main branch HEAD**: 13 commits ahead of `origin/main` since 2026-04-25 — UNPUSHED
**Latest Oracle review**: 2026-04-26 (`bg_1d8162f6`) — Wave 4d cleared all P0 ship-blockers

---

## 项目一句话 / Project One-Liner

rr 是 Rust 写的分布式 Multi-Raft 共识框架：openraft 0.9 + tonic gRPC + RocksDB，
**10 个 crate**（core / storage / config / raft / network / router / server / client / admin /
**ingestor-kafka 自 v0.3**），**4 个 example**（counter, kv-store, **kv-store-kafka,
otel-client 自 v0.3**），完整的集成测试 + chaos framework + Docusaurus 文档站
+ **drop-in Prometheus/Grafana 套件 自 v0.3**。

---

## 已完成的工作 / Completed Work

### Phase 4 v0.3.0 candidate (2026-04-25 ~ 2026-04-26, 13 commits, unpushed)

**Wave 1 — Security hardening** (`7db26ec`)
- F1/F2 完整：audit module, ServiceLabel, BearerInterceptor + RateLimitInterceptor reject 事件,
  sanitized errors, `validate_security_invariants`（5-gate startup check：raft+TLS, raft+mTLS,
  auth+TLS, admin+TLS, admin+token）
- `NodeConfig::allow_insecure` opt-in（CLI/env/TOML），fail-closed `*_token_env`
- `MAX_INSTALL_SNAPSHOT_BYTES = 4 GiB`, `rr_snapshot_installs_total{group_id}` label
- `ServiceStateMachineAdapter::new(group_id, service)` — **breaking** signature change
- `docs/deployment/upgrading.md` + Dev compose 设置 `allow_insecure=true`
- Oracle bg_1b5e149d 审查：7 处必修全部 applied

**Wave 2 — Tier 0 docs patches** (`02bfbe2`)
- T0.1 Service + StateMachine Safety Invariants rustdoc（per ADR A-0002 §D1）
- T0.2 `DurabilityPolicy` rename: `Balanced→ReplicatedOnly`, `Fast→TestOnly`,
  `#[deprecated]` aliases, `is_strict()`, `canonical_name()`, runtime warn 非 Strict 时

**Wave 3 — Tier 1 FramedSnapshot** (`c341bd5`)
- `rr_core::framed_snapshot::FramedSnapshot` (8-byte magic `\0rrSNAP1` + bincode body, version 1)
- `decode()`：3 状态返回（Some/None/Err）防止 silent corruption
- 自动迁移 v0.2.x raw bytes
- `FrameworkState { idempotency, ingestor }` schema locked

**Wave 4a — Tier 2/3 shared types** (`7ff6526`)
- `rr_core::idempotency::{IdempotencyKey, IdempotencyDecision, IdempotencyError, ApplyOutcome}`
- `rr_core::framed_entry::FramedEntry` (magic `\0rrENT01`, version field added in 4d)
- `rr_core::ingestor::{CommandIngestor, DynCommandIngestor}` 双 trait（typed + erased）解决 object-safety
- `RrError::IdempotencyExpired` variant + proto3 `optional` 字段（back-compat）

**Wave 4b — IdempotencyLayer + 写路径** (`4852448`)
- `rr_raft::idempotency::IdempotencyLayer`：watermarks HashMap + LRU cache, decide/record/capture/restore
- 5 位 client（rr-client, rr-server dispatch, rr-network ClientServiceImpl, rr-server node,
  rr-raft service_adapter）线性穿透 IdempotencyKey
- `RrClient::send_request_idempotent(group_id, payload, key)` 公开 API
- `outcome="idempotency_expired"` 直方图标签 + `RrError::IdempotencyExpired` 客户端错误

**Wave 4c.1+4c.2 — Ingestor lifecycle** (`8938114`)
- `ServiceStateMachineAdapter` 跟踪 `latest_ingestor_offset`（仅成功 apply 时推进，失败不推进）
- `snapshot()` 写入 `FrameworkState.ingestor.last_applied_offset`（非空时才写）
- `restore()` 把 offset 重新水合到 adapter
- `latest_ingestor_offset()` 公开访问器
- 新模块 `rr_raft::ingestor_runner`（~370 行）：`spawn(mgr, group_id, ingestor, auto_idem)`,
  `IngestorHandle::stop()`, 监听 raft.metrics()，leader → seek+poll，step-down → park
- 错误处理：poll 错误 expo backoff（cap 30s，绝不下台 leader），write 错误 → 退出 poll loop（park 等下次 leadership）
- `RaftGroupManager::{attach_ingestor, attach_ingestor_with_auto_idempotency, stop_ingestor,
  fetch_snapshot_bytes}` 公开 API；`stop_group` 先停 supervisor 再 stop raft

**Wave 4c.3 — rr-ingestor-kafka crate** (`bfa0d37`)
- 新 workspace crate `crates/rr-ingestor-kafka/`：
  - `config`: `KafkaIngestorConfig`, `KafkaSecurityConfig`（env-resolved 密码 per F2 模式），
    `SecurityProtocol` enum
  - `mock`: `MockKafkaIngestor` + `MockKafkaHandle`（与真实 backend 同 `Offset = i64`，便于框架 e2e 测试）
  - `backend`（`rdkafka` feature gate）：`KafkaIngestor` 包 `StreamConsumer`，单 partition `assign()`
    避开 consumer-group rebalance；`seek` 用 `offset+1` resume after persisted；`poll` deadline-bounded
    `tokio::select!`；async commit
- 默认特性 = `[]`，工作区构建无需 cmake；`--features rdkafka` 拉 librdkafka via cmake-build
- e2e 测试 `tests/e2e_real_broker.rs` gated 在 `RR_TEST_KAFKA_BOOTSTRAP` env var

**Wave 4 verify — 跨层集成测试** (`6949356`)
- `tests/integration/src/cross_tier_ingestor.rs`：3 个测试证明 Tier 1 + Tier 3 端到端协作
  - `ingestor_pushed_commands_apply_through_full_stack` — 5 个 mock kafka 消息流过 supervisor → raft
    → adapter → user Service，断言 exactly-once apply 顺序
  - `snapshot_after_ingestor_apply_carries_offset_into_framework_state` — apply offset 100..103，
    手动触发 snapshot，断言 `framework_state.ingestor.last_applied_offset` = 102
  - `restored_adapter_seeds_new_ingestor_with_persisted_offset` — fresh adapter 从 snapshot 恢复，
    bytes 经 `DynCommandIngestor::seek_bytes` 回到新 ingestor 实例

**Wave 4d — Oracle ship-blocker fixes** (`e0a8d1e`)
Oracle review (`bg_1d8162f6`) 找到 3 P0 + 5 P1。本 wave 修了 P0-2/P0-3/P1-1/P1-2 + 1 个发现的 latent watermark bug：
- **P0-2** `seed_offset` 失败现在 fatal — supervisor 退出而非 fall through 到 free-running poll
  （之前会触发 `auto.offset.reset=earliest` 导致 mass replay）
- **P0-3** Auto-idempotency on ingestor write path：新 `AutoIdempotency { request_type,
  offset_to_request_id }` config + `RaftGroupManager::attach_ingestor_with_auto_idempotency`
  方法。Runner 派生 `IdempotencyKey` per command；re-fetched messages 由 idempotency 层 dedup，
  不再 double-apply。`rr-ingestor-kafka::kafka_offset_to_request_id` 是 Kafka i64 的标准转换器
  （负 sentinel clamp 到 0 防 watermark 跳到 u64::MAX）
- **P1-1** `FramedEntry` 添加 `version: u8` 字段 + `FRAMED_ENTRY_VERSION = 1` 常量；decode 拒绝
  newer-than-supported；与 `FramedSnapshot` 的 versioning 对等
- **P1-2** `IdempotencyKey` 手写 `PartialEq` / `Eq` / `Hash` 只比较 `(request_type, request_id)` ——
  对应 layer 的 cache key + `client_id` 是 diagnostic-only 的文档承诺
- **Latent watermark bug 修复（Wave 4d 集成测试发现）**：`decide()` 旧逻辑 `cache miss + req_id <=
  watermarks.get().unwrap_or(0)` 会让首次 id=0 被错判为 Expired。新逻辑用 HashMap entry 的"存在
  与否"作为 watermark presence 信号，Kafka offset 0 第一条消息正确路由到 New
- **P1-5** observability：`rr_ingestor_step_down_drops_total{group_id}` 计数 step-down 时
  丢弃的批尾命令，pair 上 `rr_ingestor_seed_failures_total` 让操作员看到所有"沉默"故障
- 新增端到端测试 `auto_idempotency_dedups_replayed_kafka_offsets` 证明 P0-3 关闭

**Wave 5 — Tier 4 docs + example** (`e0a0337`)
- `docs/docs/guide/idempotency.md` (T4.2)：完整指南——身份模型（带 4d hand-impl 语义）、watermark、
  4-way decisions、客户端 opt-in、auto-idempotency、持久化、metrics、何时不该用
- `docs/docs/guide/kafka-input.md` (T4.2)：rr-ingestor-kafka 指南——topic 布局、生命周期、build
  prereq（cmake）、wiring example、TLS/SASL config、自定义 ingestor 实现、metrics、failure
  modes、v0.3.0 limitations 包括 P0-1 deferred 性能 issue
- `docs/docs/deployment/v0.2-to-v0.3-migration.md` (T4.4)：操作员升级指南——breaking change 清单、
  wire-format 自动迁移、rolling upgrade procedure、optional-feature adoption、downgrade procedure
- `examples/kv-store-kafka/` (T4.3)：参考示例，默认特性用 MockKafkaIngestor 所以无 cmake 即可编译；
  演示 attach_ingestor_with_auto_idempotency + replay-with-dedup
- `docs/sidebars.ts` 注册三页；`npm run build` 全清

**Wave 6.1 — Theme A.1 + A.2 observability** (`e5fdb48`)
- `crates/rr-raft/src/metrics_ticker.rs`：每组定时器，每 5s 读 `RaftMetrics.replication` 发射
  `rr_raft_replication_lag{group_id, follower_node_id}` = `leader.last_log_index - follower.matched.index`。
  **仅 leader 发射**——follower 不发射（避免 Prometheus 误读为"lag=0"）。告警时必须 predicate 在
  `rr_raft_is_leader == 1`，模块 rustdoc + metrics-reference.md 都强调
- `RaftGroupManager::start_metrics_ticker(self: &Arc<Self>, group_id)` 公开 API；`RrNode::start()`
  在每个 `start_group` 后自动调用（含三条 watch 路径：bootstrap, PartitionAdded,
  GroupMembershipChanged）
- 旧 metrics（`rr_raft_is_leader`, `rr_raft_current_term`, `rr_raft_last_log_index`）也由 ticker
  在 idle 时持续更新——以前只在 write 时更新
- `BearerInterceptor::call` reject 事件加 `caller_token_fp` 字段——非密码学 8-hex siphash
  fingerprint，让操作员关联同一 caller 的多次试探，不暴露 token
- `pub fn token_fingerprint` 公开 helper，rustdoc 大写"NOT a security primitive"防止误用
- `docs/docs/deployment/audit-log.md` 增"调用者身份"章节，更新 metrics-reference

**Wave 6.2 — Theme A.3 + A.4 ops + OTEL** (`b9776cc`)
- `deploy/observability/prometheus/rr-recording-rules.yml`：6 条预计算 PromQL（leaders observed,
  leader churn rate, p99 proposal latency, idempotency replay/collision rates, max replication lag）
- `deploy/observability/prometheus/rr-alerts.yml`：6 条默认 alert（RrInsecureModeInProduction,
  RrIdempotencyCollisionsObserved, RrIngestorSeedFailures, RrLeaderFlapping, RrNoLeader,
  RrReplicationLagHigh）
- `deploy/observability/grafana/rr-cluster-overview.json`：6-panel 仪表盘
- `deploy/observability/README.md`：wiring 说明 + 阈值理由 + 交叉引用
- `examples/otel-client/`：tracing-opentelemetry 0.27 + opentelemetry 0.26 + opentelemetry-stdout
  exporter，演示 W3C traceparent 从 client span 提取 + 注入到 outbound rr-client request 的完整 pattern
- 验证：`cargo run -p otel-client` 真实导出 traceparent + span via stdout

**Wave 7 — Theme B.4 /ready JSON body** (`582cc6d`)
- `/ready` 替换为 JSON 体：`{"ready": bool, "reason": str, "since_unix_secs": u64}`
- `reason` 自由文本（"starting", "ready", "shutting_down", "degraded:lost_quorum", ...）
- `ReadinessFlag::set_with_reason(bool, reason)` 新方法；`set(bool)` 保留 backwards-compat
- 内部存储 `Arc<AtomicBool>` → `Arc<std::sync::RwLock<ReadinessState>>`（非热路径，开销可忽略）
- K8s probe 仍只看 status code；ops tooling 可 parse body
- 7 个新单测 + 现有 `test_health_endpoints_full_lifecycle` 集成测试更新为 JSON 解析

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

### 2026-04-24 session — Mermaid→PlantUML 转换 + I/O 设计 ADR 三连

**文档层（docs/）**:

- **Mermaid → C4-PlantUML 全量转换**：`docs/docs/` 下 18 个 markdown 文件、
  26 个 mermaid 代码块 100% 替换。7 个 C4 图（Context/Container/4×
  Components/Deployment）用 `!include <C4/C4_*.puml>`；5 个 C4Dynamic 用
  `C4_Dynamic`；7 个 sequenceDiagram 用 PlantUML 原生 sequence；6 个
  graph/flowchart 视情况用 C4-PlantUML 或 PlantUML component/package 图。
- **移除 `@docusaurus/theme-mermaid`**：`package.json` + `docusaurus.config.ts`
  + `package-lock.json` 同步；npm install 通过，`npx tsc --noEmit` 零错误。
- **MDX 解析错误修复**：`disk-full.md` + `metrics-reference.md` 里 `<N%` 被
  MDX v3 当成 JSX 标签，用反引号 `` `<N%` `` 包成 inline code 解决。
- **Broken links 清零**：~30 处 `/docs/intro` 改 `/`（`routeBasePath:'/'` +
  `slug:/`）；`compatibility.md` 引用 `../../../CHANGELOG.md` 改 GitHub 绝对
  链接（Docusaurus 不解析 docs 目录外的相对链接）。
- **Broken anchor 修复**：`dynamic-snapshot-transfer.md#snapshot-handling` →
  `#快照处理--snapshot-handling`（Docusaurus 为双语 heading 生成含中文的
  slug）。
- **验证**：`npm run build` 最终零警告，26 个 plantuml 代码块，0 个 mermaid。

**框架能力调研（5 个并行 explore agent）**:

研究了 rr 框架的 5 个维度并向用户报告：
- 入口方法（gRPC 三服务 + HTTP 观测 + SDK + CLI；**无 Kafka/MQ**，全 grep 零命中）
- Raft log 持久化与恢复（RocksDB 3 CF；Strict/Balanced/Fast durability；openraft 自动恢复）
- 快照机制（`Service::snapshot/restore` trait + 64KB chunked gRPC 流式传输 + 自动 restore-on-start）
- Req-resp 模式（纯同步 blocking；无 request-id；断链 = 状态已变更+响应丢失+无法恢复）
- 业务输出（metrics + 结构化日志 + tracing + admin status + topology + config watch + health；**无 CDC/watch stream/state callback**）

**设计讨论 → 三份 ADR（`.sisyphus/adr/`，英文，`proposed` 状态）**:

- **A-0001 CommandIngestor abstraction** — trait 放 rr-core（无 MQ 依赖），
  `rr-ingestor-kafka` 独立 crate（拒绝 NATS/Pulsar 当前范围）。Per-group 独立
  topic（`input_<G>`），leader-exclusive 消费，offset 进每个 Raft log entry，
  snapshot 记 `last_applied_offset`。
- **A-0002 Snapshot consistency + recovery** — 通过 **librarian 拉 openraft
  源码（`databendlabs/openraft @ 30dc1f0c`）**权威核对 3 条关键契约，纠正了
  讨论中几个不准确的说法（见下方"常见陷阱"新增条目）。计划改动：
  `DurabilityPolicy` rename + deprecate、`Service` safety invariants 文档化、
  `rr-admin node-reset` 单节点灾难恢复命令、6 步 recovery flow 表。
- **A-0003 Framework idempotency** — per `(request_type, request_id)` 去重，
  `client_id` 仅做诊断字段；opt-in per request（不带 key 走原路径，兼容业务
  自己做多字段复合幂等）；watermarks + bounded LRU，随框架 snapshot 自动
  持久化；新增 `IdempotencyExpired` error。

**Phase 4 执行计划**:

- `.sisyphus/plans/rr-phase4-v030.md` — Tier 0 (docs patches) → Tier 1
  (FramedSnapshot 基础) → Tier 2 (幂等) + Tier 3 (Kafka) 并行 → Tier 4
  (协同+文档) → Tier 5 (release)，约 3-4 周到 v0.3.0。
- **Momus 两轮审阅**（session `ses_2404bbf16ffeKOS5yqjsWAPjYa`）：
  第 1 轮 NEEDS-CHANGES（3 个 issue：T3.1-3 object-safety、T2.3 文件路径错+
  缺 dispatch、T4.4 QA 不可重复）→ 修复后第 2 轮 OKAY。
- 主要修复：`CommandIngestor` 用双 trait（implementer 面 associated type +
  framework 面 `DynCommandIngestor` 类型擦除 `Vec<u8>` offset）解决
  object-safety；T2.3 补全 gRPC → dispatch → apply 三段链路；T4.4 换成 6 步
  可重复 QA 清单。

---

## 未完成 / 需要重做 / Open Work

### v0.3.0 release operations（next session）

- **Push 13 unpushed commits 到 origin/main**（Wave 1 ~ Wave 7）。
- **打 `v0.3.0` 标签**（或先 `v0.3.0-rc1`，validate release workflow，再 final）。
  Release notes 可从 11 个 wave commit messages 拼出来；CHANGELOG.md 需更新。
- **F1-F4 独立评审**（如仍要做，建议在 push 前由 oracle agent 跑）。
  Wave 4d 已经过 Oracle review (`bg_1d8162f6`)，全部 P0 ship-blockers 已 cleared，
  P1 部分有遗留（详见下方 v0.3.x 跟进列表）。

### v0.3.x 跟进（patch 版本，无 ship 阻塞）

**来自 Wave 4d Oracle review 的 P1 / P2 遗留**：
- **P0-1（降级到 P1）** — `seed_offset` 只读 snapshot 而非 live state machine；
  leader 转换时如 snapshot 滞后于 applied state，会重新拉取消息。
  现在由 auto-idempotency dedup 保证正确性，仅是性能 issue。修复路径：
  (a) 通过 side-channel registry 暴露 live offset，或 (b) 在 seek 前触发 snapshot。
- **P1-3** — Magic-prefix collision on legitimate raw payloads：~2^-64 概率但应加
  `rr_framing_decode_failures_total{kind}` metric 让 ops 看见。
- **P1-4** — Out-of-range seek 静默 reset 到 earliest（auto.offset.reset 行为）；
  应在 seek 前用 `Consumer::fetch_watermarks` 验证 target ∈ [low, high]，超界则报错。
- **P2-1 ~ P2-6** — `commit_applied` 在 runner 中从未调用、`IdempotencyLayer.restore`
  merge 而非 replace、`ApplyOutcome::decode` 无运行时 guard、`attach_ingestor` TOCTOU
  smell、`IdempotencyState.reserved` 字段名混淆、缺 `rr-admin reset-ingestor-offset`
  admin 命令。

**来自 Wave 7 范围缩减的 deferred 项目**（Phase 3.1 Theme B + T0.3）：
- **T0.3** `rr-admin node-reset` 单节点灾难恢复 CLI（依赖 add_learner / change_membership
  RPCs 的 admin 层暴露——底层 `RaftGroupManager` 方法在 Wave 1 已有）。
- **B.1** Online backup via snapshot-fetch RPC（当前只有 offline `rr-admin backup`）。
- **B.2** Auto-promote learner → voter after caught-up（当前需手动 change_membership）。
- **B.3** `DecommissionNode` workflow（drain → remove → confirm three-step ops procedure）。
- **add_learner / change_membership 暴露成 admin gRPC + `rr-admin` CLI**。

**Wave 8 — Phase 3.1 Theme C test hardening**（全部 deferred 到 v0.3.x）：
- C.1 TLS+auth e2e integration test
- C.2 Proto fuzzing via cargo-fuzz
- C.3 iptables-level network partition chaos test
- C.4 Crash-during-write durability test (kill -9 mid-fsync)
- C.5 Jepsen / Maelstrom-style linearizability testing

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

**第一优先级 — 切 v0.3.0**：
1. Push 13 unpushed commits 到 origin/main
2. 更新 CHANGELOG.md 用 wave commit messages（v0.3.0 是 v0.2.0-rc1 之后第一个 minor）
3. 打 `v0.3.0-rc1` 标签 → validate GitHub Actions release workflow 跑通
4. （可选）多打 1-2 个 rc 修发现的 release 问题
5. 打 `v0.3.0` final + crates.io 发布

**第二优先级 — 上面的 v0.3.x deferred 项目**（按需拣选）：
- 修 P0-1 的性能 issue
- 把 admin RPCs (add_learner / change_membership) 暴露到 `rr-admin` CLI
- T0.3 `rr-admin node-reset`
- 上面 P1-3 / P1-4 / P2-* 的可观测性 + hardening

**第三优先级 — Wave 8 测试加固**：
按照 Phase 3.1 Theme C 计划逐项实施。这些都不是 ship 阻塞，但提升信心。

---

**Line A 摘要（原 Phase 3.1 计划）**:

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

### OpenRaft 契约纠错（2026-04-24 sourcecheck，commit `30dc1f0c`）

- **OpenRaft 不保证 snapshot 原子性**——`build_snapshot()` 在 spawned task
  里跑，和 `apply()` 并发。实现者（`Service::snapshot()`）**必须**自己用
  lock 或 immutable view 保证原子。证据：`openraft/src/core/sm/worker.rs#L233-L237`。
  → 已写入 ADR A-0002 §Contract 1 + `Service` trait 计划中的 Safety
  Invariants doc。
- **`applied_state > log_state` 不是错误**——这是 crash-after-snapshot-install
  的正常恢复路径。OpenRaft 会 purge logs to last_applied。证据：
  `openraft/src/storage/helper.rs#L153-L165`。→ 已写入 ADR A-0002 §Contract 3。
- **真正的不可恢复场景**：`last_applied < committed` AND 所需 log 已经被
  purge（`start < last_purged_log_id+1`）→ `StorageError: read_log_at_index`。
  这才是需要 `rr-admin node-reset` 的场景。
- **OpenRaft 允许三种 apply 策略**：persistent SM（apply 时持久化）、
  persistent snapshot only（靠 replay）、transient SM（in-memory + 必须实现
  `save_committed()`）。用户 `Service` 属于哪种影响恢复行为，要文档化。

### Docusaurus / MDX 陷阱

- **MDX v3 把 `<N%` 当 JSX 标签**，build 报 "Unexpected character"。用
  反引号包成 inline code：`` `<20%` `` 而不是 `<20%`。
- **Docusaurus 不解析 `docs/docs/` 目录外的相对链接**（例如
  `../../../CHANGELOG.md`）。引用仓库根文件要用 GitHub 绝对 URL。
- **双语 heading 的 slug 含中文**：`## 快照处理 / Snapshot Handling` 生成
  `#快照处理--snapshot-handling`，不是 `#snapshot-handling`。引用 anchor
  要用完整 slug。
- **`routeBasePath: '/'` + `slug: /` 的 docs 不能用 `/docs/intro` 引用**，
  用 `/` 或 `/intro`（看 slug 定义）。

### Rust trait design 陷阱

- **Trait with associated type 不是 object-safe**——`Box<dyn Trait>` 编不过
  如果 associated type 出现在方法签名里。解法是双 trait 模式：公共 trait 保
  implementer 的 typed 体验；framework-internal 类型擦除 trait（例如用
  `Vec<u8>` 代替 `Self::Assoc`）提供 object-safety。ADR A-0001 的
  `CommandIngestor` + `DynCommandIngestor` 是例子。

### 原有陷阱（Phase 3）

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

### 正式 ADR（`.sisyphus/adr/`）

2026-04-24 的 design session 产出了三份 ADR，`proposed` 状态，尚未排期：

- **[A-0001](./adr/A-0001-command-ingestor-abstraction.md)** —
  `CommandIngestor` trait in rr-core + `rr-ingestor-kafka` 独立 crate。支持
  Kafka topic 作为命令源（每 Raft group 对应独立 topic `input_<G>`，单
  partition，leader-exclusive 消费）。offset 作为字段进入每个 Raft log
  entry，snapshot 记 `last_applied_offset`。明确拒绝：不做 feature flag
  方案（拒绝 librdkafka C 依赖污染 rr-core）；暂不做 NATS / Pulsar /
  RocketMQ 支持。
- **[A-0002](./adr/A-0002-snapshot-consistency-and-recovery.md)** —
  Snapshot 一致性契约 + 灾难恢复路径。包含对 OpenRaft 契约的**权威核对**
  （librarian task `bg_e072f2fd` 拉 openraft `30dc1f0c` 源码验证），纠正了
  几条之前文档里不准确的说法：
  1. OpenRaft **不**保证 snapshot 原子性——是实现者（`Service::snapshot()`）
     的责任。
  2. Recovery 流程分 6 步（install snapshot → replay logs → purge hole）。
  3. `applied > log_state` 是**正常恢复路径**，不是错误。
  4. `DurabilityPolicy::{Balanced, Fast}` 违反 OpenRaft durability 契约，
     计划 rename + deprecate。
  5. 新增 `rr-admin node-reset` 单节点灾难恢复命令。
- **[A-0003](./adr/A-0003-framework-idempotency-layer.md)** —
  框架级幂等：`(request_type, request_id)` 去重，`client_id` 仅作诊断字段。
  Opt-in per request（`ClientRequest.idem_key` optional），不带 key 的请求
  照常走原路径。watermarks + bounded LRU response cache，随框架 snapshot
  自动持久化。新增 `IdempotencyExpired` error。和 A-0001 天然协同——Kafka
  at-least-once + 框架 dedup = exactly-once apply。

三份 ADR 关系：**A-0002 是 A-0001/A-0003 的前置**（后两者依赖框架 state
machine wrapper 对 `Service::snapshot()` 原子性的假设）。A-0001 + A-0003
**强烈建议一起发布**（共用 `FramedSnapshot` 格式，避免多次 snapshot 迁移）。

### 未成 ADR 的零散想法

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

### 前瞻设计 ADR
- `.sisyphus/adr/README.md` — ADR 索引 + 状态图
- `.sisyphus/adr/A-0001-command-ingestor-abstraction.md` — Kafka 输入
- `.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md` — 快照契约 + 灾难恢复
- `.sisyphus/adr/A-0003-framework-idempotency-layer.md` — 框架幂等

### Phase 4 执行计划
- `.sisyphus/plans/rr-phase4-v030.md` — v0.3.0 I/O enhancements 执行计划
  （Tier 0 docs patches + Tier 1 framework wrapper + Tier 2 idempotency +
  Tier 3 Kafka + Tier 4 synergies + Tier 5 release，~3 周）。
  **Momus 审阅通过**（session `ses_2404bbf16ffeKOS5yqjsWAPjYa`，两轮审阅，
  最终 OKAY）。

### 覆盖率报告
- `target/coverage/html/html/index.html` — HTML 可点击报告
- `target/coverage/lcov.info` — Codecov 兼容格式
- `target/coverage/coverage.json` — 可编程消费的 summary

---

## Git 状态

- `main` 当前 HEAD 比 `origin/main` 超前 **13 个 commit**（**全部未 push**）。
  按时间顺序：
  - `c760efe` docs(evidence) — F1-F4 review report
  - `7db26ec` feat(security) — Wave 1
  - `02bfbe2` feat(core,storage) — Wave 2
  - `c341bd5` feat(core,raft) — Wave 3 FramedSnapshot
  - `7ff6526` feat(core,network) — Wave 4a 共享类型
  - `4852448` feat(raft,server,client) — Wave 4b idempotency
  - `8938114` feat(raft) — Wave 4c.1+4c.2 ingestor lifecycle
  - `bfa0d37` feat(ingestor) — Wave 4c.3 rr-ingestor-kafka crate
  - `6949356` test(integration) — Wave 4 verify cross-tier
  - `e0a8d1e` feat(raft,core,ingestor) — Wave 4d Oracle P0/P1 fixes
  - `e0a0337` docs(guide,deployment) — Wave 5 Tier 4 docs + example
  - `e5fdb48` feat(raft,network) — Wave 6.1 Theme A.1+A.2
  - `b9776cc` feat(observability,examples) — Wave 6.2 Theme A.3+A.4
  - `582cc6d` feat(server) — Wave 7 Theme B.4 /ready JSON
  （加这次 CONTEXT 更新 commit = 14 个）
- `v0.2.0-rc1` 标签已 push（指向 `a9edbb9`）；当前 main HEAD 是 v0.3.0 候选。
- 工作树干净（仅 CONTEXT.md 当前 session 的更新待 commit）。

**下一步**：见 "下一步工作"。第一件事是 push + 切 v0.3.0。

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
