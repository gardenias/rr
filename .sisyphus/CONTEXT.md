# rr — Project Context

> Top-level session-handoff / onboarding file. Each session should UPDATE
> the sections below rather than rewrite. Per-phase deep dives live in
> `.sisyphus/notepads/<phase>/{decisions,learnings,problems}.md`.
> Forward-looking design decisions live in `.sisyphus/adr/A-NNNN-*.md`.

**Last updated**: 2026-05-12 late (A-0006/A-0007/A-0008 implemented; output runner real send loop)
**Current phase**: ADR follow-through (Phase 4 wrap-up)
**Active plan**: 无活跃 plan；follow-up 方向见下方"下一步工作"
**Latest tags**: `v0.2.0-rc1` + `v0.2.0-rc2` (both pushed). `v0.2.0` final never cut.
**Main branch HEAD**: prior 4 commits unpushed + new A-0006/A-0007/A-0008 work
**Latest Oracle review**: 2026-04-26 (`bg_1d8162f6`)

---

## 项目一句话 / Project One-Liner

rr 是 Rust 写的分布式 Multi-Raft 共识框架：openraft `= 0.10.0-alpha.19` + tonic gRPC + RocksDB，
**11 个 crate**（core / storage / config / config-etcd / raft / network / router / server / client / admin /
**ingestor-kafka**），**5 个 example**（counter, kv-store, kv-store-kafka, otel-client, etcd-config-quickstart），
完整的集成测试 + chaos framework + Docusaurus 文档站 + drop-in Prometheus/Grafana 套件。

### Session 2026-05-12 (late) — A-0006/A-0007/A-0008 implemented

- **A-0008 配置化**：`rr_raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST` (50_000) + `RaftNode::new_with_factory` / `new_with_rocks_factory` 接受 `snapshot_logs_since_last: u64` 参数；`RaftGroupManager::with_snapshot_logs_since_last` builder；`NodeConfig` 新字段 + builder；CLI `--snapshot-logs-since-last` + env `RR_SNAPSHOT_LOGS_SINCE_LAST` + TOML `[node].snapshot_logs_since_last`；零值拒绝；4 个 config_file 测试 + 1 个 raft_node 测试。
- **A-0007 OutputRunner 真实实现**：替换 42 行 no-op placeholder 为完整 send loop。新增 `rr_core::output::{KafkaProducer, NoopKafkaProducer}` trait。Output runner 监听 raft 元数据通道实现 leader-only 执行，drain pending_outputs，按 OutputTarget 分发（RaftWrite via RrClient::send_request_idempotent / KafkaPublish via KafkaProducer），成功 advance_output_watermark，失败 exponential backoff（100ms → 30s cap）。`attach_output_runner` 扩展接受 kafka producer 参数。新增 metrics：`rr_output_send_total{group_id,target}` / `rr_output_send_failures_total` / `rr_output_send_duration_seconds` / `rr_output_pending`。5 个新单元测试。
- **A-0006 状态收尾**：代码自 2026-05-07 已实现且测试齐备，本 session 仅做 ADR status → `implemented` + CHANGELOG 收录 + README banner 更新。
- **ADR README banner 更新**：从 "proposed designs ... not yet implemented" 改为按 ADR 索引各自状态。A-0001/2/3/4/6/7/8 全部 `implemented`；A-0005/9 仍 `accepted`。
- **Pre-existing clippy warning fixes**：`tcp_raft_server.rs` 未用 import / `config_file.rs` + `health.rs` uninlined_format_args / `main.rs` field_reassign_with_default。`cargo clippy -p rr-core -p rr-raft -p rr-server --features rocks --all-targets -- -D warnings` clean。
- **测试通过**：rr-raft (122) / rr-server (92) / rr-core (57) / rr-storage (44) 全绿。

---

## 已完成的工作 / Completed Work

### Session 2026-05-12 — 性能分析 + IoBatcher + SDK reactive/blocking + rr-perf-bench

**代码质量 — REPL 可测试化**（`crates/rr-admin/src/main.rs`）：
- 从 `run_repl` 提取 `parse_repl_line(&str) -> ReplCommand` 和 `decode_kv_response(&[u8]) -> Result<Option<String>>`
- +19 个单元测试（69/69 全绿）
- 覆盖率：rr-admin REPL 从 ~15% 提升

**IoBatcher — Raft log group commit 引擎**（`crates/rr-raft/src/io_batcher.rs`，新文件）：
- 实现 per-group 异步 IO batcher，多提案共享单次 write_batch + fdatasync
- 三阶段积累：Phase2 try_recv 零等待 drain / Phase3 500µs 窗口 / Phase4 单次写入
- `RocksLogStorage::append()` 改为真正异步（channel 投递立即返回，callback 异步触发）
- 开放 API：`IoBatcherConfig::default/no_batching/new()`，`RocksLogStorage::open_with_config()`
- 新增 5 个 batcher 专项测试 + `log_append_batched` benchmark（26% 提升实测）
- 修复预存 bug：`crates/rr-server/src/node.rs` `PrometheusBuilder::build()` → `build_recorder()`
  （原实现会启动内置 HTTP server，多进程并发启动时端口冲突导致 node 2/3 崩溃）

**新 benchmark 文件**（`benches/benches/`，4 个新文件）：
- `framing_roundtrip.rs`：FramedEntry encode/decode 各 payload size
- `idempotency_throughput.rs`：IdempotencyLayer decide+record 吞吐
- `log_append_batched.rs`：group commit vs sequential 对比
- `log_tps.rs`：TPS 直接测量（serial blocking / reactive pipeline）

**Rust SDK — 阻塞 + 响应式接口**（`crates/rr-client/src/lib.rs`）：
- `#[derive(Clone)]` to `RrClient`（所有字段 Arc-backed，clone 共享 leader cache）
- 阻塞接口：`send_request_blocking / send_request_idempotent_blocking / read_blocking`
  → `tokio::task::block_in_place` + `block_on`，需多线程 runtime
- 响应式接口：`send_request_reactive / send_request_idempotent_reactive / read_reactive`
  → `tokio::spawn` 内部 await，callback 在 tokio worker thread 触发，调用方立刻返回
- 新增 10 个测试（23 总，全绿）：Clone 共享状态 / 阻塞失败路径 / reactive 立即返回 / 多 callback 全触发

**Java SDK — 真响应式重构**（`sdks/java/src/main/java/.../RrClient.java`）：
- 原实现：`CompletableFuture.supplyAsync(() -> blockingCall)` = 假异步（线程被占用）
- 新实现：`ClientServiceGrpc.newFutureStub()` + `ListenableFuture` → `CompletableFuture`
  → Netty event loop 驱动 I/O，无线程在 gRPC 等待期间被占用
- 异步 retry 链：递归 `sendRequestAsync` via `thenCompose` + `exceptionallyCompose`
- 非阻塞 backoff：`CompletableFuture.delayedExecutor()`（不用 `Thread.sleep`）
- 公开阻塞接口：`sendRequestSync / readSync / getClusterInfoSync` → `blockOn(future.get())`
- 新增 `sdks/java/src/test/.../RrClientTest.java`：11 个测试（21 总，全绿）
  验证：阻塞接口抛 RrClientException / reactive 立即返回 / callback 触发 / 调用线程不阻塞

**rr-perf-bench — 3 进程集成性能测试工具**（`tools/rr-perf-bench/`，新 crate）：
- 4 个源文件：`cluster.rs / metrics.rs / workload.rs / report.rs / main.rs`
- 启动 3 个 rr-server 进程，bootstrap 3-node cluster，用 rr-admin CLI 注册节点
- S1：`send_request_blocking()` — 串行阻塞，测最低延迟基线
- S2：`send_request_reactive()` + Semaphore — pipeline_depth 个请求始终 in-flight，响应驱动
- HDR Histogram 延迟采集，table 输出 TPS / p50 / p95 / p99 / p999 / Errors
- CLI：`--scenario / --storage / --durability / --payload / --concurrency / --warmup-secs / --duration-secs`

**实测性能数据**（debug build，3 进程 localhost，RocksDB）：
- S1 串行（Strict）：~568 TPS，p50=1.4ms（bottleneck：debug build CPU + gRPC RTT，非 fsync）
- S2 reactive（ReplicatedOnly，depth=100）：~485 TPS，p99=720ms（server 侧饱和）
- 三种 durability 几乎相同（< 2% 差异）→ debug build 下 CPU 是瓶颈，非 storage
- release build 预估：2k-5k TPS（serial）；高并发 IoBatcher 后 50k+ TPS

**关键发现（写入链路分析）**：
- `append()` 之前 blocking await：RaftCore task 被卡，openraft pipeline 失效
- `IoBatcher` 修复后：append 异步，多提案可同时 in-flight，group commit 生效
- Java SDK 原 "假响应式"根因：`newBlockingV2Stub` 线程占用全程
- 真响应式必须用 `newFutureStub` 把 I/O 交给 Netty

---

### Session 2026-05-08 — Framework Review + Quality Hardening + openraft 0.10 + Follower Read

**Framework Deep Review**（`.sisyphus/evidence/rr-framework-review-2026-05-08.md`）：
- 三轮并行 explore agent 分析：错误处理、API 完整性、性能/并发
- 产出完整的能力/限制/缺陷/边界报告

**openraft 0.9.21 → 0.10.0-alpha.19 迁移**（`c19d258`）：
- 移除 storage-v2 feature（0.10 已合并）
- RrTypeConfig 新增 Term/LeaderId/Vote/Responder 关联类型
- RaftStateMachine::apply() 从 IntoIterator → Stream<EntryResponder>
- RaftLogStorage：truncate → truncate_after，移除 read_vote，全 io::Error
- RaftNetwork → RaftNetworkV2：full_snapshot，stream_append/transfer_leader
- Raft<C> → Raft<C, SM>，RaftMetrics 类型参数统一

**Follower Read 特性**（`c742a98`）：
- 两种一致性：stale（本地直读）+ linearizable（ReadIndex via openraft 0.10）
- Proto: ClientRequest 新增 read_only=4, linearizable=5
- SDK: RrClient::read() + read_linearizable()
- ServiceStateMachineAdapter 改为 Arc<RwLock<S>> 支持并发读

**Quality Hardening P0-P3**（`7d183d8`, `35943e7`, `069f24c`）：

P0（已修）：
- Lock poisoning：service_adapter/rate_limit 5 处 .expect() → .unwrap_or_else(|p| p.into_inner())
- RetryConfig 溢出：5 处 2u64.pow(attempt) → saturating_mul/pow(min(20))

P1（已修）：
- Ingestor batch write：write_batch() via client_write_many()，吞吐 ~1k → ~10k+ msgs/sec/group
- Service lifecycle hooks：on_leader_elected/on_step_down/shutdown，自动 via metrics_ticker

P2（已修）：
- GroupManager 7 RwLocks → 1 DashMap<GroupId, Arc<Mutex<GroupState>>>
- Admin proto 错误码 gRPC-idiomatic：5 RPC 改用 Status 替代 bool success+message

P3（已修）：
- Rate limiter 后台 GC task（Weak Arc，60s 间隔）
- Backup/restore SHA-256 checksum（data_sha256 in metadata.json，verify on restore）

**ADR 新增**（同 session）：
- A-0005: Per-caller rate limiting design
- A-0006: RocksDB CF-level tuning design

**新增 contract tests**（同 session）：
- test_idempotency_dedup_survives_leader_switch (P0/C-005)
- test_committed_entry_applies_exactly_once_after_leader_crash (T-005)
- test_snapshot_state_digest_consistent_across_nodes (T-002)
- test_ingestor_survives_leader_switch_no_message_loss (T-003)

### Session 2026-04-27 — multi-language SDK + etcd contrib + dashboard polish（4 个并行交付，未 commit）

用户指示：做"java&rust sdk + etcd ConfigProvider + dashboard 持续打磨"，跳过 P2-6 reset-ingestor-offset CLI。Phase A 串行（先 review Rust SDK 找 gap），Phase B/C/D 并行委派给 deep-类目 subagent。

**Phase A — rr-client (Rust SDK) 审查 + 优化** (`crates/rr-client/`, 自己做)
- 审查报告：`.sisyphus/evidence/rr-client-sdk-review-2026-04-27.md`。结论：核心 API（retry / leader cache / idempotency / metrics / tracing）合格保持原状，但 3 个 P0 阻塞 + 1 个 P2 polish 必修。
- **F1 修复 — TLS/mTLS**：新 `RrClientBuilder::with_tls(TlsConfig)`，`connect_to_addr` 按 `tls_config` 选 `https`/`http` scheme，wire `endpoint.tls_config(tls.build_client_tls()?)`。
- **F2 修复 — bearer token**：`with_bearer_token(String)` + 内部 `build_request<T>` helper 调 `rr_network::auth::attach_bearer_token` 给所有 outbound（`send_request_inner` + `get_cluster_info`）。
- **F3 修复 — Cargo.toml metadata**：description / license=MIT / repository / homepage / documentation / readme / keywords / categories 全补齐，`rust-version.workspace = true`。`cargo publish --dry-run` 仅卡 rr-core/rr-network path-dep version（workspace-wide 问题，与"暂不做版本管理"对齐，不阻塞 SDK 改动）。
- **F4 修复 — README**：`crates/rr-client/README.md`（quick start dev / production TLS+auth / idempotency / cluster discovery / metrics / security notes / license / links），~140 行。
- **安全语义**：TLS-less bearer 默认 `tracing::warn!`（与 server `allow_insecure` opt-in 对齐）；`with_strict_security(true)` 时 build 拒绝（`NetworkError::ConnectionFailed`）。
- **向后兼容**：`RrClient::connect` / `connect_with_config` 保留为 builder 薄包装，无 `#[deprecated]`。
- **测试**：8 原有 + 7 新增 = 15 unit tests 全绿（builder defaults / with_bearer_token_stores_token / strict_security_rejects_token_without_tls / with_retry_config_overrides / empty_addrs_rejected / build_request_attaches_bearer_token / build_request_omits_metadata_when_no_token）。
- **L1 验证**：cargo check workspace clean / clippy `-D warnings` clean / cargo doc clean。
- **版本**：仍 `0.1.0`（用户决定保持，等 workspace 统一发版时再 bump）。

**Phase B — Java SDK** (`sdks/java/`, 委派 `bg_af216421` 21min, 已独立验证)
- 完整 Gradle Kotlin DSL 项目骨架：`build.gradle.kts`, `settings.gradle.kts`, `gradle.properties`, Gradle wrapper, JDK 17 toolchain。
- Maven groupId `io.github.gardenias`，Java root package `io.github.gardenias.rr.client`。
- protos 通过 `protobuf-gradle-plugin` 直接消费 `crates/rr-network/proto/*.proto` —— **不复制 proto 到 Java 树，single source of truth**。
- 公开类：`RrClient` / `RrClientBuilder` / `RetryConfig` / `TlsConfig` / `IdempotencyKey` / `LeaderCache` / `BearerTokenInterceptor` + 异常族（`RrClientException` → `NetworkException` → `RpcFailedException` + `IdempotencyExpiredException`）。
- 公开方法用 Java 命名：`sendRequest` / `sendRequestIdempotent` / `sendRequestWithPartition` / `getClusterInfo`，全部返 `CompletableFuture`（async idiom 对齐 Rust `async fn`）。
- Builder 方法：`newBuilder(List<String>)`（Java keyword `new` 不能作方法名，README 已说明） / `withTls(TlsConfig)` / `withBearerToken(String)` / `withRetryConfig(RetryConfig)` / `withStrictSecurity(boolean)` / `build() -> CompletableFuture<RrClient>`。
- 依赖（轻量）：grpc-netty-shaded 1.66 + grpc-protobuf + grpc-stub + protobuf-java + Micrometer 1.13（必选）+ slf4j-api 2.0（必选）。**无** OpenTelemetry / Resilience4j / Lombok / Guava / Apache Commons / Spring。
- **bearer auth**：`io.grpc.ClientInterceptor` 实现，`Metadata.Key.of("authorization", ASCII)` 注入 `"Bearer " + token`，与 Rust SDK 同 wire shape。
- **TLS**：`NettyChannelBuilder.useTransportSecurity()` + `GrpcSslContexts.forClient().keyManager().trustManager().build()`，`domainName` 切 SNI override authority。
- **strict security**：`build()` 时 token+tls=null+strict 抛 `IllegalStateException`，与 Rust SDK 同语义；warn-only mode 用 slf4j WARN。
- **Metrics**：Micrometer `MeterRegistry`（默认 `Metrics.globalRegistry`，可 builder 覆盖）。Timer `rr.client.request.duration`{group_id, outcome} + Counter `rr.client.retries` / `rr.client.leader_cache.hits|misses`。Prometheus 导出后是 `rr_client_*`，与 Rust SDK 指标 surface 对齐。
- **测试**：9/9 unit tests pass（`BearerTokenInterceptorTest` 2 + `LeaderCacheTest` 3 + `RrClientBuilderTest` 4），0 failures/errors/skipped。
- **集成测试**：`RrClientIntegrationTest` 在独立 source set，gated by `RR_TEST_DOCKER=1`，本环境 ghcr.io 镜像不可达 → fallback 到 `InProcessChannelBuilder` 假 service（happy path 验证）。
- **CI**：`.github/workflows/sdks-java.yml` —— `actions/setup-java@v4 (temurin 17)` + `gradle test build`，tag publish 到 GitHub Packages（不发 Maven Central）。
- **GH Packages publish**：wired in `build.gradle.kts` via `maven-publish`，仅配置不真发。
- **Verification**：`./gradlew test build check javadoc --warning-mode all` 全绿，`build/libs/rr-client-0.1.0.jar` 产出。
- 主分支独立 re-run `./gradlew test --rerun-tasks`：BUILD SUCCESSFUL 9 tasks executed。

**Phase C — etcd ConfigProvider** (`crates/rr-config-etcd/`, 委派 `bg_f2d7a2de` 14min, 已独立验证)
- 新 workspace crate `rr-config-etcd v0.1.0`，依赖 `etcd-client 0.14`（加到 `[workspace.dependencies]`）+ `tokio-stream 0.1`。
- `EtcdConfigProvider::new(endpoints: Vec<String>, cluster_id: String)` async constructor。
- 实现 `ConfigProvider` trait 三方法：`get_routing_table`（prefix 扫 + 装配 RoutingTable）、`watch_changes`（3 个 etcd watcher → mpsc → Stream<RoutingTableChange>）、`get_node_addr`（直 Get）。
- **read-only**：trait surface 不暴露写入。测试 setup 用 raw `etcd_client::Client` 写 key（非公共 API）。
- **ETCD KEY SCHEMA**（`.sisyphus/adr/A-0004-etcd-config-schema.md` 文档化）：
  - `/rr/{cluster_id}/nodes/{node_id}` → JSON `NodeAddr { addr }`
  - `/rr/{cluster_id}/partitions/{partition_id}` → JSON `{ config, group_id }`
  - `/rr/{cluster_id}/groups/{group_id}/replicas` → JSON `[node_id, ...]`
  - cluster_id 默认 `"default"`，构造器可改
  - 用 JSON 而非 bincode：操作员可 `etcdctl get --prefix /rr/` 直接看
- **Cargo metadata**：description / license=MIT / repository / keywords / categories 完整。
- **测试**：9/9 unit tests pass（serde wire shapes / key path generation / 5 个 watch event variants 解析 / etcd 错误映射），1 个 integration test ignored（gated `RR_TEST_ETCD=1` + Docker，跟 rr-ingestor-kafka 的 `RR_TEST_KAFKA_BOOTSTRAP` 同 pattern）。
- **示例 crate**：`examples/etcd-config-quickstart/`（连 localhost:2379, fetch table, await one watch event）。
- **L1 验证**：`cargo check -p rr-config-etcd` clean / `cargo clippy --all-targets -D warnings` clean / `cargo doc` clean / `cargo build -p etcd-config-quickstart` clean / **`cargo check --workspace` 全绿**。
- **ADR A-0004**：`proposed` 状态，登记到 `.sisyphus/adr/README.md`。

**Phase D — Prometheus / Grafana 持续打磨** (`deploy/observability/`, 委派 `bg_bbc98e7c` 6min, 已独立验证)
- **Dashboard `rr-cluster-overview.json`**：6 → 12 panels（保留原 6，新增 6）：
  1. `rr_raft_replication_lag` heatmap（`{{group_id}}/{{follower_node_id}}`）+ 强制 `rr_raft_is_leader == 1` predicate（panel 描述强调 follower lag=0 是误读）
  2. Idempotency rates（replays/collisions/expired）3 条 rate
  3. `rr_idempotency_cache_size` gauge stat（按 `${IDEMPOTENCY_CACHE_MAX}` 模板变量归一化，阈值 50/90）
  4. Ingestor health（step_down_drops + seed_failures + seek_below/above_watermark）stacked timeseries
  5. `rr_framing_decode_failures_total{kind}` 按 kind stacked
  6. Learner promotion outcomes（success/failure）bargauge
- **新增模板变量**：`group_id` / `follower_node_id` / `decode_kind`（multi-value）+ `IDEMPOTENCY_CACHE_MAX`（textbox，默认 50000）。
- **Recording rules**：6 → 10：`cluster:rr_replication_lag:max_per_group`, `rr:idempotency_replay_rate_5m_raw`（旧名占用，后缀 `_raw`）, `rr:ingestor_offset_skew_total`, `rr:framing_decode_failure_rate_5m`。
- **Alerts**：6 → 9：`RrIdempotencyCacheNearFull`(warn) / `RrIngestorOffsetDrift`(warn) / `RrFramingDecodeFailures`(critical)。
- **Datasource**：所有 panel 用 `${DS_PROMETHEUS}` 变量，不硬编码 UID。
- **指标存在性 grep 验证**：12 个引用的 metric 全部 `grep -r "rr_<name>" crates/` 命中代码；列表见 `bg_bbc98e7c` task output。
- **README 更新**：`deploy/observability/README.md` Panels/Recording rules/Alerts 三段全更新 + 文末 `Last updated: 2026-04-27`。
- **未拆 dashboard**：12 panels 在 14 阈值下，未创建独立 `rr-ingestor.json`。
- **JSON/YAML 校验**：`json.load` + `yaml.safe_load` 双 parse 通过；总计 12 panels / 10 rules / 9 alerts (rr_correctness=6 + rr_availability=3)。

**未 commit**：四个 phase 全部工作树挂着，按用户"暂不做版本管理"约束。

**关键决策记录**：
- 跳过 P2-6 `rr-admin reset-ingestor-offset` CLI（用户明确 deprioritize）
- Java SDK groupId `io.github.gardenias`（GitHub-hosted org 标准 reverse-DNS）
- Java 发 GitHub Packages 不发 Maven Central（避免 GPG / Sonatype 流程）
- etcd Provider read-only（仅 ConfigProvider trait）
- rr-client 保持 0.1.0（不与 framework v0.3 同步 bump）
- 所有"轻量"约束严格执行（Java 无 OpenTelemetry/Resilience4j/Lombok）

---

### Continuation session (2026-04-26 later, 11 commits, Phase 3.1 + Oracle follow-ups + test hardening)

完成所有 Phase 3.1 Theme B 运维自动化、Oracle review P1/P2 跟进、Phase 3.1 Theme C 测试加固。

**Wave 8 — admin add_learner + change_membership** (`89abdf3`)
- proto: AddLearner / ChangeMembership / GetGroupMembership RPCs + types
- rr-server: AdminServiceImpl 实现，含空 voter 拒绝、quorum-loss check
- rr-admin: `group add-learner / change-membership / membership` 命令
- 4 unit + 3 CLI parse tests

**Wave 9 — T0.3 rr-admin node-reset** (`0ab0918`)
- 新模块 `crates/rr-admin/src/node_reset.rs`：3 步前置检查（路径存在 + 看着像 rr 数据
  目录 + RocksDB LOCK 未被持有）+ 必须 `--force`，防止 `--data-dir /` 类拍脑袋
- CLI `rr-admin node-reset --data-dir <path> [--force]`，输出后续 add-learner +
  change-membership 步骤说明
- 8 unit + 2 CLI parse tests

**Wave 10 — B.2 auto-promote learner** (`917c113`)
- 新模块 `crates/rr-raft/src/learner_promoter.rs`：tokio task 监听
  `raft.metrics()`，仅 leader 时扫 membership，per-learner 跟踪 stability ticks
  （连续 3 次 `lag <= max_lag=50` → promote）
- 步下台清空 stability counter；可在运行时 hot-reload config
- audit-log 配对 started/succeeded|failed 事件，新指标
  `rr_learner_promotions_total{group_id}` + `rr_learner_promotion_failures_total`
- `RaftGroupManager::start_learner_promoter` / `stop_learner_promoter` 公开 API

**Wave 11 — B.1 online backup streaming RPC** (`2f03e4a`)
- proto: `OnlineBackup(OnlineBackupRequest) returns (stream OnlineBackupChunk)`
  with `OnlineBackupMetadata { group_id, last_log_index, last_log_term, size_bytes,
  rr_version }`
- rr-server: tokio_stream + 64 KiB chunks，匹配 InstallSnapshot 的 chunk 大小；
  `force_fresh_snapshot=true` 触发新 snapshot；wait-up-to-60s 让 snapshot
  materialize；client-disconnect 在 audit log 中可见
- rr-admin: `group online-backup --group <ID> --output <FILE> [--force-fresh]`，
  byte-count 校验 + JSON/human 输出

**Wave 12 — B.3 DecommissionNode workflow** (`7d42363`)
- 新模块 `crates/rr-admin/src/decommission.rs`：5-step orchestrator（inspect →
  refuse on quorum loss → drain via TransferLeader → ChangeMembership → confirm）
- `MIN_VOTERS_AFTER_REMOVAL = 2`：拒绝把任何 group 留到 < 2 voters；操作员需先
  add 一个新 voter 或用 node-reset
- CLI `rr-admin node decommission --node-id <ID> [--dry-run]`，per-group action
  enum 区分 Skipped/AbortedQuorumLoss/WouldRemove/DrainedAndRemoved/Removed/Failed
- 8 unit + 2 CLI parse tests

**Wave 13 — Oracle P1/P2 review 跟进** (`d97f2b3`)
- **P0-1**（降级 perf）：`refresh_snapshot_if_stale` 在 seed_offset 前触发
  `raft.trigger().snapshot()` if `snapshot_index < last_applied`，bound
  leader-promotion re-fetch 浪费到 ≤ 30s 而非"全部 post-snapshot 条目"
- **P1-3**：`rr_framing_decode_failures_total{kind}` 在 FramedSnapshot /
  FramedEntry / ApplyOutcome decode 错误路径
- **P1-4**：Kafka seek 用 `Consumer::fetch_watermarks` 校验 [low, high]，
  out-of-range 发 `rr_ingestor_seek_below_watermark_total` /
  `rr_ingestor_seek_above_watermark_total`
- **P2-1**：runner 调用 `commit_applied_bytes` 让 Kafka `__consumer_offsets`
  反映 rr 进度（仅 observability，rr 自身不依赖）
- **P2-2**：`IdempotencyLayer::restore` 改 replace（不 merge）→ 双重 restore
  幂等
- **P2-3**：`ApplyOutcome` 加 `[0xAA][0x01]` magic+version header，防止 raw
  user bytes 被误 decode 成 Success(garbage)
- **P2-4**：`attach_ingestor` TOCTOU fix（先取 ingestor_handles 写锁，再
  check get_raft，与 stop_group 的取锁顺序一致）
- **P2-5**：`IdempotencyState.reserved` → `data` 重命名，`#[serde(alias =
  "reserved")]` 保后向兼容（bincode v2 标准配置是 positional 编码，wire 不变）
- P2-6（reset-ingestor-offset CLI）defer 到 follow-up — 罕见用例，需独立设计

**Wave 14 — Theme C.1 TLS+mTLS+auth e2e** (`c1d0d41`)
- `tests/integration/src/tls_auth_e2e.rs`：`rcgen 0.13` 生成内存中 CA + server
  cert + client cert，spawn 真实 tonic Server（mTLS required + bearer auth），
  客户端 4 种凭据状态全测：valid / wrong-token / no-token / no-client-cert
- 一个 async test 函数承载 4 个 scenario，cert+server 开销摊销

**Wave 15 — Theme C.2 cargo-fuzz** (`a4c8fe6`)
- 新 `fuzz/` sub-crate（独立 workspace，避免 panic=abort + nightly 污染主 workspace）
- 4 个 fuzz target：FramedSnapshot / FramedEntry / ApplyOutcome decode +
  ClientRequest proto decode
- `fuzz/README.md` 含 nightly + cargo-fuzz install + run-all snippets +
  CI workflow 模板（建议作为 nightly cron 而非 per-PR）

**Wave 16 — Theme C.3 网络分区病理 case** (`b9f53ef`)
- 3 个新 chaos test（用现有 `FaultAwareChannelPool` 而非真 iptables — 双向隔离
  在 channel 层等价于 kernel packet drop）：
  - **E3a flapping**：5 次快速 isolate/restore leader，断言无 election storm
  - **E3b minority partition**：5 节点中隔离 leader+1 follower，断言 3-node
    majority 选新 leader 且 minority NOT 接受 writes（防 split-brain）
  - **E3c double-follower isolation**：3 节点中隔离 2 followers，断言
    leader-of-minority NOT commit；restore 一个后 quorum 恢复

**Wave 17 — Theme C.4 crash-during-write 持久性** (`eea3988`)
- `test_crash_during_write_preserves_acked_entries`（`rocks` feature gated）
- Phase 1 写 20 条记录每个 ack 的 last_log_index，Drop manager **不调用
  stop_group**（最近似 in-process kill -9）
- 500ms settle delay 让 RocksDB process-global LOCK 状态释放
- Phase 2 用 fresh `RocksLogStorage::open` 读，断言 `last_log_id.index >=
  max(acked_indices)` + vote 持久化

**Wave 18 — Theme C.5 linearizability stress** (`e27a8f8`)
- `test_concurrent_writes_under_chaos_linearizability`：3 节点，5 并发 writers ×
  20 writes（100 总）+ 6 cycles of (isolate-follower 150ms + restore 100ms)
  叠加进行
- 断言 3 不变量：`last_applied >= ack_count`（无 acked-but-lost）+ `acked >= 1`
  （非 trivial）+ stable leader after chaos clears
- Pragmatic Jepsen-style stress（不引入 Maelstrom / porcupine-rs 重依赖），
  catches lost ack / divergent SM / election storm 三大类 bug

### Phase 4 v0.3.0 base (2026-04-25 ~ 2026-04-26 早段, 14 commits)

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

### 框架功能开发：**全部完成** ✓

Phase 4 (v0.3 核心) + Phase 3.1 (Theme A 可观测性 / Theme B 运维自动化 /
Theme C 测试加固) + Oracle review P0/P1/P2 跟进 — 全部 implemented，仅以下小残留：

- **P2-6** `rr-admin reset-ingestor-offset` CLI — Oracle review 项中唯一未做。
  **罕见用例**（操作员手动修复 corrupt 持久 offset）+ 需独立 admin RPC 设计；
  defer 到后续 minor version。所有现实用例可由 Wave 9 的 `node-reset` 替代。

### 跨 session 的非紧迫待办（来自原 Phase 3 计划）

- ✅ **多语言 client SDK** — Java SDK done (`sdks/java/` 2026-04-27)；Rust SDK polished (`crates/rr-client/` 同期)。Go / Python 仍 deferred 待社区驱动。
- ✅ **etcd ConfigProvider** — done (`crates/rr-config-etcd/` 2026-04-27, ADR A-0004)。
- ✅ **Prometheus recording rules / dashboard 持续打磨** — done (2026-04-27, 6 → 12 panels / 6 → 10 rules / 6 → 9 alerts)。
- **Per-caller rate limiting**（依赖 caller identity）—— Wave 6.1 的
  `caller_token_fp` 是基础。
- **Raft LogStorage 的 RocksDB CF 单独调优**——分离 raft_log / state_machine 的
  block_cache_size / write_buffer。
- **rr-client / rr-core / rr-network 的 workspace 级 publish metadata 补齐** —— 当前 rr-client 自身 metadata 已齐，但 path-deps 缺 version，`cargo publish` 阻塞。等用户决定整体发版时一起做。

### 覆盖率维度的后续（2026-05-12 更新）

- ✅ **删除 vestigial stubs** — `rr-network/src/client.rs` + `server.rs` 文件不存在（已在早期 session 删除）
- ✅ **health.rs 集成测试** — 已有 17 个测试含 6 个真实 HTTP server 测试（见 health.rs）
- ✅ **CI coverage `--features rr-storage/rocks`** — ci.yml line 171 已有
- ✅ **rr-admin REPL 测试** — 2026-05-12 提取 `parse_repl_line` + `decode_kv_response`，新增 19 个测试，覆盖率从 15% 提升

### 已知跨 session 的待办（非本 session 引入）

- ✅ Online backup via snapshot-fetch RPC（已 done — Wave 11 `OnlineBackup` streaming RPC）
- ✅ ChangeMembership RPC 自动化（已 done — Wave 8 `change_membership` + Wave 10 auto-promote learner）
- ✅ 多语言 client SDK — Java SDK + Rust SDK polish done（2026-04-27）；Go/Python 仍 deferred
- Jepsen-style linearizability testing（pragmatic stress 已 done Wave 18，full porcupine-rs / Maelstrom 仍 deferred）

---

## 下一步工作 / Next Session's Candidate Work

**2026-05-12 更新**：本 session 专注于性能分析、IoBatcher group commit、SDK 响应式/阻塞接口、rr-perf-bench。所有变更在工作树（未 commit）。

**性能方向（按优先级）**:
1. **release build 压测** — 当前数字全是 debug build（CPU 瓶颈），release 会有 3-5× 提升
2. **IoBatcher 实际效果验证** — 需要真正并发提案才能体现 group commit（可用 rr-perf-bench S2 reactive 模式）
3. **WAL log storage** 替换 RocksDB log（raft-wal 或自实现顺序文件，去掉 LSM overhead）
4. **ReplicatedOnly 生产建议** — 3 副本下去掉 per-write fsync，100k+ TPS 可达

**SDK 方向**:
- Java SDK 测试：`./gradlew test` 21 个全绿；reactive 接口用 `newFutureStub` 真异步（已实现）
- 可选：Rust SDK `send_request_with_partition` 等其余方法补 blocking/reactive 变体

**代码质量**:
- 所有变更未 commit（工作树状态），需要用户触发 commit
- rr-admin REPL 测试 ✅ / 4 个新 benchmark 文件 ✅ / IoBatcher ✅ / SDK 两侧 ✅

**可选 follow-up（原有，未变化）**:
- P2-6 `rr-admin reset-ingestor-offset` CLI（defer）
- 多语言 client SDK Go / Python（Java + Rust 已完成）
- workspace 级发版准备

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

### 性能 / SDK 陷阱（2026-05-12）

- **`PrometheusBuilder::build()` 启动内置 HTTP server（默认端口 9000）**：`rr-server/node.rs` 原来用 `build()` 而非 `build_recorder()`，导致多进程并发启动时端口冲突，第 2/3 个节点崩溃。修复：改用 `build_recorder()`（只创建 recorder，不启动 HTTP）。
- **`RocksLogStorage::append()` blocking await 让 openraft pipeline 失效**：原实现 `self.storage.write_batch(ops).await?` 阻塞等 fsync，RaftCore task 被卡住，无法处理下一个提案。修复：IoBatcher channel + 异步 callback，RaftCore 立即返回。
- **Java CompletableFuture.supplyAsync(() -> blockingCall) 是假响应式**：只是把阻塞移到 ForkJoinPool 线程，线程仍然被 I/O 占满。真响应式必须用 `newFutureStub()` 让 Netty event loop 处理 I/O。
- **debug build 下 fsync 不是瓶颈**：三种 durability（Strict/ReplicatedOnly/TestOnly）TPS 差异 < 2%，因为 CPU/序列化开销远大于 fsync。真实生产数字必须用 release build 测量。
- **rr-perf-bench 进程泄漏 → 端口冲突**：`Cluster::start()` 失败时 `RocksLogStorage` drop 不杀子进程。修复：实现 `Drop for Cluster`，调用 `child.start_kill()` 清理所有子进程。
- **RrClient 阻塞方法需 multi-thread runtime**：`send_request_blocking` 用 `block_in_place`，在 `current_thread` runtime 下会 panic。测试必须用 `#[tokio::test(flavor = "multi_thread")]`。

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

### 测试基础设施陷阱（Phase 3.1 Theme C waves）

- **RocksDB process-global LOCK 不在 Drop 时即时释放**。in-process 测试
  drop manager → reopen 同 path 的 RocksDB 时遇 "lock hold by current
  process"。需要 sleep ~500ms 让 RocksDB 内部 lock 状态清空。Wave 17
  `test_crash_during_write_preserves_acked_entries` 学到。
- **`rcgen 0.13` 默认 features 不含 crypto**。`KeyPair::generate()` 需要
  `features = ["crypto", "ring"]`。新版 rcgen 把所有加密 backend 设成 opt-in。
- **`tonic` 0.14 的 `Channel::connect()` 可能不在 TLS 失败时立即报错**——
  握手延迟到第一个 RPC。所以 mTLS 缺 client cert 的测试要在 `connect_result`
  成功时也尝试 issue 一个 RPC，确保 fallback 检查触发。
- **`cargo-fuzz` 必须是独立 workspace**——它要 `panic = "abort"` profile +
  nightly toolchain + libfuzzer-sys（链 C++ runtime），跟主 workspace 的
  stable/unwind 配置冲突。`fuzz/Cargo.toml` 用 `[workspace]` 自包含。
- **clippy `single_match` 在测试 panic 路径上很烦**——`match Result { Ok(_)
  => panic!(...), _ => {} }` 必须改 `if let Ok(_) = result { panic!(...) }`。
  虽然语义等价，clippy 死磕。
- **clippy `uninlined_format_args` 触发于 assert! 的 message 里 `{:?}`** —
  修：`assert!(..., "got {:?}", err)` → `assert!(..., "got {err:?}")`。

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
- `crates/rr-client/src/lib.rs` — Rust client SDK（blocking + reactive 接口，Clone，23 tests，2026-05-12）
- `crates/rr-raft/src/io_batcher.rs` — IoBatcher group commit 引擎（2026-05-12 新增）
- `crates/rr-raft/src/rocks_log_storage.rs` — 真异步 append + IoBatcherConfig（2026-05-12）
- `crates/rr-admin/src/main.rs` — REPL 可测试化：parse_repl_line / decode_kv_response（2026-05-12）
- `crates/rr-config-etcd/src/lib.rs` — etcd-backed ConfigProvider, read-only, ADR A-0004 schema (2026-04-27)
- `sdks/java/src/main/java/io/github/gardenias/rr/client/RrClient.java` — 真响应式（newFutureStub）+ blocking API，21 tests（2026-05-12）
- `tools/rr-perf-bench/src/` — 3 进程集成性能测试工具（2026-05-12 新增）
- `deploy/observability/{grafana,prometheus}/` — 12 panels / 10 rules / 9 alerts (2026-04-27 polish)

### 性能 benchmark 文件（2026-05-12 新增）
- `benches/benches/framing_roundtrip.rs` — FramedEntry encode/decode
- `benches/benches/idempotency_throughput.rs` — IdempotencyLayer 吞吐
- `benches/benches/log_append_batched.rs` — group commit vs sequential
- `benches/benches/log_tps.rs` — TPS 直接测量

### 本次 session 的计划
- `.sisyphus/plans/rr-phase3-prodready.md` — Phase 3 完整 roadmap（~30 任务 × 5 wave）

### 前瞻设计 ADR
- `.sisyphus/adr/README.md` — ADR 索引 + 状态图
- `.sisyphus/adr/A-0001-command-ingestor-abstraction.md` — Kafka 输入
- `.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md` — 快照契约 + 灾难恢复
- `.sisyphus/adr/A-0003-framework-idempotency-layer.md` — 框架幂等
- `.sisyphus/adr/A-0004-etcd-config-schema.md` — etcd ConfigProvider 的 key 布局 + JSON wire shape（2026-04-27）

### 本次 session 评审
- `.sisyphus/evidence/rr-client-sdk-review-2026-04-27.md` — rr-client SDK 审查报告（F1/F2/F3 P0 阻塞 + F4 polish，全部已修）

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

- `main` 当前 HEAD 比 `origin/main` 超前 **25 个 commit**（**全部未 push**）。
  按时间顺序：

  **Phase 4 v0.3 核心（前一段 session）**:
  - `c760efe` docs(evidence) — F1-F4 review report
  - `7db26ec` feat(security) — Wave 1 F1/F2 hardening
  - `02bfbe2` feat(core,storage) — Wave 2 Tier 0 docs
  - `c341bd5` feat(core,raft) — Wave 3 FramedSnapshot
  - `7ff6526` feat(core,network) — Wave 4a Tier 2/3 shared types
  - `4852448` feat(raft,server,client) — Wave 4b IdempotencyLayer
  - `8938114` feat(raft) — Wave 4c.1+4c.2 ingestor lifecycle
  - `bfa0d37` feat(ingestor) — Wave 4c.3 rr-ingestor-kafka crate
  - `6949356` test(integration) — Wave 4 verify cross-tier
  - `e0a8d1e` feat(raft,core,ingestor) — Wave 4d Oracle P0 fixes
  - `e0a0337` docs(guide,deployment) — Wave 5 Tier 4 docs + example
  - `e5fdb48` feat(raft,network) — Wave 6.1 Theme A.1+A.2
  - `b9776cc` feat(observability,examples) — Wave 6.2 Theme A.3+A.4
  - `582cc6d` feat(server) — Wave 7 Theme B.4 /ready JSON
  - `c6ec8cd` docs(context) — handoff CONTEXT update

  **Phase 3.1 Theme B + Oracle 跟进 + Theme C（本 session）**:
  - `89abdf3` feat(admin) — Wave 8 add_learner/change_membership
  - `0ab0918` feat(admin) — Wave 9 T0.3 rr-admin node-reset
  - `917c113` feat(raft) — Wave 10 B.2 auto-promote learner
  - `2f03e4a` feat(admin,server) — Wave 11 B.1 online backup streaming
  - `7d42363` feat(admin) — Wave 12 B.3 DecommissionNode
  - `d97f2b3` feat(core,raft,ingestor) — Wave 13 Oracle P1/P2 follow-ups
  - `c1d0d41` test(integration) — Wave 14 C.1 TLS+mTLS+auth e2e
  - `a4c8fe6` test(fuzz) — Wave 15 C.2 cargo-fuzz proto fuzzer
  - `b9f53ef` test(integration) — Wave 16 C.3 partition pathology
  - `eea3988` test(integration) — Wave 17 C.4 crash-during-write durability
  - `e27a8f8` test(integration) — Wave 18 C.5 linearizability stress

  （加本次 CONTEXT 更新 = 26 个）

- 工作树干净（CONTEXT.md 当前 session 的更新待 commit）。

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
