---
sidebar_position: 8
---

# 设计决策记录（ADR 索引）/ Design Decisions (ADR Index)

:::info 本文档的性质 / About this page
rr 的架构决策以 **ADR A-0001 … A-0014** 编号散落在代码注释中（`grep -rn "ADR A-" crates/`），完整 ADR 文档位于仓库的 `.sisyphus/adr/` 目录（不参与 Docusaurus 发布）。本页是对这些**代码内引用 + ADR 文档**的提炼索引；当本页与代码注释不一致时，**以代码注释为准**。每个条目都标注了决策在代码中的强制执行点（锚点）。
:::

rr records architecture decisions as **ADR A-0001 … A-0014**, referenced from code comments (`grep -rn "ADR A-" crates/`); the full ADR documents live in the repo under `.sisyphus/adr/` (not published to Docusaurus). This page distils those **in-code references + ADR documents** into an index; where this page and the code comments disagree, **the code comments win**. Each entry names the enforcement point (anchor) in code.

## 总表 / Index

| ADR | 主题 | 状态 / Status | 决策要点 | 代码锚点 |
| :--- | :--- | :--- | :--- | :--- |
| A-0001 | CommandIngestor 抽象 + Kafka 参考实现 | proposed | 在 rr-core 定义 MQ 无关的 `CommandIngestor` trait；仅 leader 消费上游，follower 经 Raft 复制获取；首版只交付 Kafka | `rr-core/src/ingestor.rs:66`、`rr-raft/src/ingestor_runner.rs`、`rr-ingestor-kafka/` |
| A-0002 | 快照一致性契约 + 灾难恢复路径 | proposed | `Service` 安全不变量写进 rustdoc（原子快照 / apply 完成才返回 / restore 全量替换）；`DurabilityPolicy` 语义澄清并改名 | `rr-core/src/service.rs:131`、`rr-core/src/state_machine.rs:99-130`、`rr-storage/src/rocks.rs:34` |
| A-0003 | 框架幂等层 | proposed | 按 `(request_type, request_id)` 去重；`client_id` 仅诊断用；watermark + 有界 LRU 缓存响应；per-request opt-in | `rr-raft/src/idempotency.rs:1-49`、`rr-core/src/idempotency.rs` |
| A-0004 | etcd 配置 schema | proposed | `/rr/{cluster_id}/{nodes,partitions,groups}` 三段 typed prefix + JSON value；`ConfigProvider` 保持只读 | `rr-config-etcd/src/lib.rs:5-7,105-121`（无代码内 ADR 注释） |
| A-0005 | 按调用方限流 | implemented | per-caller 独立令牌桶（按 token 指纹）；未认证请求回落全局桶；开 per-caller 后忽略全局 rps | `rr-network/src/rate_limit.rs`、`rr-server/src/config_file.rs:115,155` |
| A-0006 | RocksDB 按列族调参 | implemented | `RocksConfig` 增加可选 per-CF 覆盖（`raft_log_cf`/`state_machine_cf`）；`metadata` 有意不开放；推荐值写文档不落代码 | `rr-storage/src/rocks.rs:19-26,177-180,253`（无代码内 ADR 注释） |
| A-0007 | Output/Outbox 机制 | implemented | Transactional outbox：`handle_with_outputs` 返回 outputs；leader-only runner 无限重试（at-least-once，不静默丢）；outbox 状态随快照复制 | `rr-core/src/service.rs:149-155`、`rr-raft/src/output_runner.rs:3` |
| A-0008 | 快照策略 | implemented | 显式 `SnapshotPolicy::LogsSinceLast(N)`，默认 50_000；快照是框架 meta 操作而非业务命令（单一输入原则） | `rr-raft/src/lib.rs:11`、`rr-raft/src/raft_node.rs:74`、`rr-server/src/config.rs:115-118` |
| A-0009 | 读写 RPC 分离 + 状态机锁模型 | implemented | 用两个独立 RPC（`Request` vs `ReadLocal`）替代 `read_only` 布尔位——杜绝"设错布尔值导致静默降级"；apply 与 stale read 共享读锁，lifecycle hooks 持写锁 | `rr-client/src/lib.rs:377` |
| A-0010 | WAL 日志存储后端 | accepted（实现暂缓） | 为 append-only 的 Raft log 定制分段 WAL，替代 LSM 开销；**先出 release profile 再动手**；作为第三种 `StorageBackend` 并存，不换默认 | 无实现（`.sisyphus/plans/rr-wal-impl.md`） |
| A-0011 | 关停持久化契约 C1–C7 | implemented | fsync 先于 ACK（C1）；vote 直连绕过 batcher（C2）；`stop_group` 固定排空序 + `IoBatcher::close()`（C3）；快照要么完整要么不存在（C6）；offset→request_id 映射失败 fail-closed（C7） | `rr-raft/src/io_batcher.rs:36-62,277`、`rr-raft/src/rocks_log_storage.rs:12,137`、`rr-raft/src/group_manager.rs:465` |
| A-0012 | 成员变更 quorum 守卫 | implemented | `MIN_VOTERS_AFTER_REMOVAL = 2` 上移 rr-core 全仓共享；服务端强制 + `--force` 逃生（强制 warn 级审计）；n→0 永不允许，走 `node-reset` | `rr-core/src/membership.rs:19`、`rr-server/src/admin.rs:582-643` |
| A-0013 | 删除 `GroupMeta.leader` | implemented | leader 是 raft 运行时状态，不属于配置面路由表；leader 发现归位到 client redirect 缓存；对 etcd 存量与 wire 零影响 | `rr-core/src/types.rs:27,92-104`、`rr-server/src/dispatch.rs:201`、`rr-router/src/lib.rs:21` |
| A-0014 | TCP transport 安全门 | implemented | `NodeConfig.raft_transport` 描述符；G6（TCP 必须显式 `allow_insecure`）+ G7（仅 loopback，**不可绕过**——`allow_insecure` 短路语义的首次有意例外） | `rr-server/src/node.rs:34-69`、`rr-server/src/config.rs:29-45`、`rr-network/src/tcp_raft_server.rs:1-15` |

---

| ADR | Topic | Status | Decision gist | Code anchors |
| :--- | :--- | :--- | :--- | :--- |
| A-0001 | CommandIngestor abstraction + Kafka reference impl | proposed | MQ-agnostic `CommandIngestor` trait in rr-core; leader-only upstream consumption, followers fed by Raft replication; only Kafka ships initially | `rr-core/src/ingestor.rs:66`, `rr-raft/src/ingestor_runner.rs`, `rr-ingestor-kafka/` |
| A-0002 | Snapshot consistency contract + disaster recovery | proposed | `Service` safety invariants documented in rustdoc (atomic snapshot / apply completes before returning / restore fully replaces); `DurabilityPolicy` semantics clarified and renamed | `rr-core/src/service.rs:131`, `rr-core/src/state_machine.rs:99-130`, `rr-storage/src/rocks.rs:34` |
| A-0003 | Framework idempotency layer | proposed | Dedup on `(request_type, request_id)`; `client_id` diagnostic-only; watermark + bounded LRU response cache; per-request opt-in | `rr-raft/src/idempotency.rs:1-49`, `rr-core/src/idempotency.rs` |
| A-0004 | etcd config schema | proposed | Typed prefixes `/rr/{cluster_id}/{nodes,partitions,groups}` with JSON values; `ConfigProvider` stays read-only | `rr-config-etcd/src/lib.rs:5-7,105-121` (no in-code ADR comment) |
| A-0005 | Per-caller rate limiting | implemented | Exclusive per-caller token buckets keyed by token fingerprint; unauthenticated traffic falls through to the global bucket; per-caller mode ignores the global rps | `rr-network/src/rate_limit.rs`, `rr-server/src/config_file.rs:115,155` |
| A-0006 | Per-CF RocksDB tuning | implemented | Optional per-CF overrides in `RocksConfig` (`raft_log_cf`/`state_machine_cf`); `metadata` deliberately excluded; recommended values documented, not hardcoded | `rr-storage/src/rocks.rs:19-26,177-180,253` (no in-code ADR comment) |
| A-0007 | Output/Outbox mechanism | implemented | Transactional outbox: `handle_with_outputs` returns outputs; leader-only runner retries forever (at-least-once, never silently drops); outbox state rides snapshots | `rr-core/src/service.rs:149-155`, `rr-raft/src/output_runner.rs:3` |
| A-0008 | Snapshot strategy | implemented | Explicit `SnapshotPolicy::LogsSinceLast(N)`, default 50_000; snapshots are framework meta-ops, not business commands (single-input principle) | `rr-raft/src/lib.rs:11`, `rr-raft/src/raft_node.rs:74`, `rr-server/src/config.rs:115-118` |
| A-0009 | Read/write RPC separation + SM locking | implemented | Two distinct RPCs (`Request` vs `ReadLocal`) instead of a `read_only` boolean — a mis-set flag can no longer silently downgrade consistency; apply and stale reads share a read lock, lifecycle hooks take the write lock | `rr-client/src/lib.rs:377` |
| A-0010 | WAL log-storage backend | accepted (implementation deferred) | Purpose-built segmented WAL for the append-only Raft log, replacing LSM overhead; **profile-first sequencing**; a third `StorageBackend` alongside Mem/Rocks, defaults unchanged | No implementation yet (`.sisyphus/plans/rr-wal-impl.md`) |
| A-0011 | Shutdown durability contract C1–C7 | implemented | fsync precedes ACK (C1); vote writes bypass the batcher (C2); fixed `stop_group` drain order + `IoBatcher::close()` (C3); snapshots are complete or nonexistent (C6); offset→request_id mapping failures are fail-closed (C7) | `rr-raft/src/io_batcher.rs:36-62,277`, `rr-raft/src/rocks_log_storage.rs:12,137`, `rr-raft/src/group_manager.rs:465` |
| A-0012 | Membership quorum guard | implemented | `MIN_VOTERS_AFTER_REMOVAL = 2` lives in rr-core, shared repo-wide; server-side enforcement + `--force` escape hatch (mandatory warn-level audit); n→0 never allowed, use `node-reset` | `rr-core/src/membership.rs:19`, `rr-server/src/admin.rs:582-643` |
| A-0013 | Remove `GroupMeta.leader` | implemented | Leader is raft runtime state, not config-plane routing data; leader discovery belongs to the client redirect cache; zero impact on etcd contents and wire protocols | `rr-core/src/types.rs:27,92-104`, `rr-server/src/dispatch.rs:201`, `rr-router/src/lib.rs:21` |
| A-0014 | TCP transport security gate | implemented | `NodeConfig.raft_transport` descriptor; G6 (TCP always needs explicit `allow_insecure`) + G7 (loopback-only bind, **not bypassable** — the first deliberate exception to the allow_insecure short-circuit) | `rr-server/src/node.rs:34-69`, `rr-server/src/config.rs:29-45`, `rr-network/src/tcp_raft_server.rs:1-15` |

> **编号无空缺**：A-0001 至 A-0014 全部存在（`.sisyphus/adr/`）。其中 A-0004 / A-0006 / A-0010 没有代码内的 `ADR A-` 注释锚点（上表给出其描述模块的锚点）；A-0010 已 accepted 但**实现暂缓**，仓库中尚无任何 WAL 代码。
>
> **No numbering gaps**: A-0001 through A-0014 all exist (`.sisyphus/adr/`). A-0004 / A-0006 / A-0010 have no in-code `ADR A-` comment anchors (the table points at the modules they describe instead); A-0010 is accepted but **deferred** — no WAL code exists in the repo yet.

## A-0001 — CommandIngestor 抽象 / CommandIngestor Abstraction

在 rr-core 定义 MQ 无关的 `CommandIngestor` trait（`poll` / `seek` / `commit_applied`，关联 `Offset` 类型经 bincode 擦除为 `DynCommandIngestor`），同 workspace 交付 `rr-ingestor-kafka` 参考实现；NATS / Pulsar / RocketMQ 明确推迟。核心安全不变量：每个 `(group_id, consumer)` 同时只有一个 ingestor 实例——框架在 leader 当选时启动、下台时停止；`poll` 必须返回单调递增 offset；实现者不得在 `poll`/`seek` 内提交上游位点，只有 `commit_applied` 可以。Kafka 约定每组一个单 partition topic（`input_{group_id}`）。

---

Defines an MQ-agnostic `CommandIngestor` trait in rr-core (`poll` / `seek` / `commit_applied`, with the associated `Offset` type erased to bytes via bincode in `DynCommandIngestor`) and ships `rr-ingestor-kafka` as the reference implementation in the same workspace; NATS / Pulsar / RocketMQ are explicitly deferred. Core safety invariants: exactly one ingestor instance per `(group_id, consumer)` at any time — the framework starts it on leader election and stops it on step-down; `poll` must return monotonically increasing offsets; implementations must not commit upstream offsets inside `poll`/`seek` — only `commit_applied` may. Kafka convention: one single-partition topic per group (`input_{group_id}`).

## A-0002 — 快照一致性与灾难恢复 / Snapshot Consistency and Disaster Recovery

把三条 `Service` 安全不变量写成契约：`snapshot()` 必须捕获原子一致视图（框架在与 `apply` 并发的任务里调用它）；`apply()` 返回前必须完成全部状态更新（禁止 fire-and-forget 的 `tokio::spawn` 写任务）；`restore()` 全量替换状态。同时澄清 `DurabilityPolicy` 语义并把 `Balanced`/`Fast` 改名为 `ReplicatedOnly`/`TestOnly`（旧名保留一个次版本的 deprecated 别名）。ADR 还枚举了恢复失败模式：当 `last_applied < committed` 且日志已被清理时，唯一出路是 `node-reset` 后靠集群快照传输追赶。

---

Codifies three `Service` safety invariants: `snapshot()` must capture an atomically consistent view (the framework calls it from a task running concurrently with `apply`); `apply()` must finish all state updates before returning (no fire-and-forget `tokio::spawn` writes); `restore()` replaces state wholesale. It also clarifies `DurabilityPolicy` semantics and renames `Balanced`/`Fast` to `ReplicatedOnly`/`TestOnly` (old names kept as deprecated aliases for one minor version). The ADR enumerates recovery failure modes: when `last_applied < committed` and the log has been purged, the only way out is `node-reset` plus snapshot transfer from the cluster.

## A-0003 — 框架幂等层 / Framework Idempotency Layer

客户端按请求 opt-in 一个 `IdempotencyKey { request_type, request_id, client_id }`；去重身份是 `(request_type, request_id)`，`client_id` 不参与去重，仅作诊断与碰撞检测。状态机侧用 watermark（每个 request_type 已应用的最大 id）+ 有界 LRU 响应缓存做四路判定：New（正常执行）/ Replay（直接返回缓存响应）/ Expired（返回 `IdempotencyExpired`，客户端须换新 id）/ Collision（不同 client_id 撞同一身份，记录审计仍返回缓存响应）。不带 key 的请求完全走原路径，零开销。

---

Clients opt in per request with an `IdempotencyKey { request_type, request_id, client_id }`; the dedup identity is `(request_type, request_id)` — `client_id` is diagnostic-only, used for collision detection. State-machine side, a watermark (highest applied id per request_type) plus a bounded LRU response cache drives a four-way decision: New (execute) / Replay (return cached response) / Expired (return `IdempotencyExpired`; the client must pick a fresh id) / Collision (different `client_id` reusing an identity — audited, cached response still returned). Unkeyed requests flow through the original path with zero overhead.

## A-0004 — etcd 配置 schema / etcd Config Schema

为 `ConfigProvider` 的生产实现定义 etcd v3 key 布局：`/rr/{cluster_id}/nodes/{node_id}`、`/rr/{cluster_id}/partitions/{partition_id}`、`/rr/{cluster_id}/groups/{group_id}/replicas`，value 用 JSON 编码（groups 的 value 是 `Vec<NodeId>` 数组）。`cluster_id` 空输入时默认 `"default"`；`ConfigProvider` 保持只读，写操作只允许出现在测试里。该 ADR 在 crates 代码注释中无 `ADR A-` 锚点，强制点就是 `rr-config-etcd` 模块本身。

---

Defines the etcd v3 key layout for the production `ConfigProvider`: `/rr/{cluster_id}/nodes/{node_id}`, `/rr/{cluster_id}/partitions/{partition_id}`, `/rr/{cluster_id}/groups/{group_id}/replicas`, with JSON-encoded values (the groups value is a `Vec<NodeId>` array). Blank `cluster_id` input defaults to `"default"`; `ConfigProvider` stays read-only — writes are allowed only in test setup. This ADR has no in-code `ADR A-` anchor; the enforcement point is the `rr-config-etcd` module itself.

## A-0005 — 按调用方限流 / Per-Caller Rate Limiting

在 per-service 令牌桶之上增加 per-caller 独立桶：以 bearer token 的 siphash 指纹为键，每个已认证调用方各吃一桶配额，解决 noisy neighbour；未认证请求回落到全局桶。设置 `per_caller_*_rps` 后对应服务的全局 rps 被忽略（exclusive 语义）；桶用 `RwLock<HashMap>` 存放，空闲 5 分钟自动逐出。拒绝事件带 `caller_token_fp` 进审计流，可聚合分析撞库/滥用。

---

Adds exclusive per-caller buckets on top of the per-service token bucket: keyed by the bearer token's siphash fingerprint, each authenticated caller draws from its own quota, solving the noisy-neighbour problem; unauthenticated requests fall through to the global bucket. Setting `per_caller_*_rps` makes the service ignore its global rps (exclusive semantics); buckets live in an `RwLock<HashMap>` and idle entries are evicted after 5 minutes. Rejects carry `caller_token_fp` into the audit stream for abuse forensics.

## A-0006 — RocksDB 按列族调参 / Per-CF RocksDB Options Tuning

`RocksConfig` 增加可选的 per-CF 覆盖（`raft_log_cf` / `state_machine_cf`，各含 `write_buffer_size_mb`、`block_cache_size_mb`、`compression`），让运维按负载特征调参而不改代码。`metadata` CF 有意不开放覆盖——活动量太低，默认永远合适。推荐起始值（如 `state_machine` 配大 block cache + Zstd、`raft_log` 配大 write buffer + Lz4）只写进文档，不做硬编码默认。代码注释中无 ADR 锚点，实现在 `rr-storage/src/rocks.rs` 的 `build_cf_options`。

---

Adds optional per-CF overrides to `RocksConfig` (`raft_log_cf` / `state_machine_cf`, each with `write_buffer_size_mb`, `block_cache_size_mb`, `compression`) so operators can tune per workload without code changes. The `metadata` CF is deliberately excluded — its activity is low enough that defaults always fit. Suggested starting values (large block cache + Zstd for `state_machine`, large write buffer + Lz4 for `raft_log`) are documented recommendations, not hardcoded defaults. No in-code ADR anchor; the implementation is `build_cf_options` in `rr-storage/src/rocks.rs`.

## A-0007 — Output/Outbox 机制 / Output / Outbox Mechanism

真实业务的 apply 必然产生副作用（调另一个 rr group、发 Kafka 事件），采用 Transactional Outbox：`Service::handle_with_outputs` 返回 `HandleResult { response, outputs }`，框架为每个 output 填单调递增 `seq` 与幂等键。delivery 由 leader-only 的 output runner 承担：发送失败无限指数退避重试（100ms 起、30s 封顶），**绝不静默丢弃**；`sent_watermark` 只在确认发送后推进；outbox 状态纳入快照随 Raft 复制，leader 切换后新 leader 从 watermark 续发。`handle` 保留为默认委托，向后兼容。

---

Real-world applies inevitably produce side effects (calling another rr group, publishing Kafka events), so the framework adopts a Transactional Outbox: `Service::handle_with_outputs` returns `HandleResult { response, outputs }`, and the framework stamps each output with a monotonic `seq` and an idempotency key. Delivery is led by a leader-only output runner: failed sends retry with exponential backoff forever (100ms initial, capped at 30s) and are **never silently dropped**; the `sent_watermark` advances only after confirmed delivery; outbox state rides snapshots so a new leader resumes from the watermark. `handle` remains as a default delegation — backward compatible.

## A-0008 — 快照策略 / Snapshot Strategy

快照必须在 apply 路径内部触发（外部触发 API 本身也要过 Raft，否则破坏单一业务输入原则），因此采用 openraft 内置的 `SnapshotPolicy::LogsSinceLast(N)` 并显式配置，默认 **N = 50_000**——在 100k ops/s 目标下约每 500ms 一次。N 是"快照开销 vs 崩溃后日志重放长度"的取舍旋钮，经 `snapshot_logs_since_last` 暴露到 `NodeConfig`、TOML 与 CLI。快照内容 = 用户状态 + 框架状态（ingestor offset、幂等 watermark、outbox），即完整恢复基点。

---

Snapshots must trigger inside the apply path (an external trigger API would itself need Raft, breaking the single-business-input principle), so the framework uses openraft's built-in `SnapshotPolicy::LogsSinceLast(N)` configured explicitly, defaulting to **N = 50_000** — roughly one snapshot per 500ms at the 100k ops/s target. N is the "snapshot cost vs post-crash replay length" knob, exposed via `snapshot_logs_since_last` on `NodeConfig`, TOML, and CLI. Snapshot content = user state + framework state (ingestor offset, idempotency watermarks, outbox) — a complete recovery base point.

## A-0009 — 读写 RPC 分离与状态机锁模型 / Read/Write RPC Separation & SM Locking

最初用 `ClientRequest.read_only: bool` 区分读写——一个设错的布尔值会把本该线性一致的写**静默降级**成 stale read。决策：协议层强制分离为两个 RPC（`Request` 必然过 Raft；`ReadLocal` 必然读本地状态机），从类型上消除设错可能；后续又增加 `ReadLinearizable`（ReadIndex + 等待 apply 追平）。锁模型：`Arc<TokioRwLock<Service>>`，apply 热路径与 stale read 都持读锁（完全并发），`on_leader_elected` / `on_step_down` / `shutdown` / `restore` 持写锁（leader 切换时不应有并发业务处理）。

---

The original design used a `ClientRequest.read_only: bool` flag — one mis-set boolean could **silently downgrade** a linearizable write to a stale read. Decision: enforce separation at the protocol layer with two distinct RPCs (`Request` always goes through Raft; `ReadLocal` always reads the local state machine), eliminating the failure mode by construction; `ReadLinearizable` (ReadIndex + wait for apply to catch up) was added later. Locking model: `Arc<TokioRwLock<Service>>` — the apply hot path and stale reads both take the read lock (fully concurrent), while `on_leader_elected` / `on_step_down` / `shutdown` / `restore` take the write lock (no business processing should race a leadership transition).

## A-0010 — WAL 日志存储后端 / WAL Log-Storage Backend

Raft log 负载是 append-only、顺序、时序的——LSM（RocksDB）的写放大、compaction、WriteThread group-commit 锁全是我们用不到的能力的代价。决策（Option E）：用 `std::fs` 自研分段 WAL（magic + CRC32C 帧、64 MiB 段滚动、vote/last_purged 小文件原子改名），状态机与快照仍留 RocksDB。**实现暂缓**：当前性能数据全是 debug build，瓶颈在 CPU + gRPC RTT；先跑 release-build profile 证明 LSM 是主瓶颈再动工，落地后作为 `StorageBackend::Wal` 与 Mem/Rocks 并存，默认不换。

---

The Raft log workload is append-only, sequential, and time-ordered — an LSM (RocksDB) charges write amplification, compaction, and the WriteThread group-commit mutex for capabilities the log never uses. Decision (Option E): build a custom segmented WAL on `std::fs` (magic + CRC32C frames, 64 MiB segment rollover, atomic-rename sidecar files for vote/last_purged); state machine and snapshots stay on RocksDB. **Implementation is deferred**: all current performance numbers are debug-build and point at CPU + gRPC RTT; a release-build profile must prove the LSM is the bottleneck before work starts, and once landed it becomes `StorageBackend::Wal` alongside Mem/Rocks without flipping the default.

## A-0011 — 关停持久化契约 / Shutdown Durability Contract (C1–C7)

一份精确、可测试的 durability 契约：**C1** Strict 下 fsync 永远先于 client ACK；**C2** `save_vote` 直连存储、有意绕过 batcher；**C3** `stop_group` 固定顺序（停 supervisor → drain 在途写 → `raft.shutdown()` → `log_storage.close()`），`IoBatcher::close()` 保证排空 + final fsync + 每个 completion 恰好应答一次，30s 截止、超时返回 `Err` 不 panic；**C4** kill -9 下 `io_completed(Ok)` 已点火的 entry 必须存活；**C5** 上述仅在 Strict 下成立；**C6** 快照要么完整要么不存在（poisoned builder + `rr_snapshot_build_failures_total`）；**C7** ingestor 消息的 offset→request_id 映射失败 fail-closed——不 propose、不提交 offset、supervisor 退出。

---

A precise, testable durability contract: **C1** under Strict, fsync always precedes the client ACK; **C2** `save_vote` writes directly, deliberately bypassing the batcher; **C3** `stop_group` follows a fixed order (stop supervisors → drain in-flight writes → `raft.shutdown()` → `log_storage.close()`), and `IoBatcher::close()` guarantees drain + final fsync + every completion answered exactly once, with a 30s deadline returning `Err` instead of panicking; **C4** under kill -9, entries whose `io_completed(Ok)` fired must survive; **C5** the above hold only under Strict; **C6** a snapshot is complete or nonexistent (poisoned builder + `rr_snapshot_build_failures_total`); **C7** an ingestor message whose offset→request_id mapping fails is fail-closed — never proposed, never offset-committed, supervisor exits.

## A-0012 — 成员变更 quorum 守卫 / Membership Quorum Guard

openraft 保证 membership 变更的正确性，但不管可用性——把 voter 缩到 1 是合法但脆弱的 SPOF，缩到 0 等于拆组。决策：`MIN_VOTERS_AFTER_REMOVAL = 2` 从 rr-admin 上移到 rr-core 作为全仓唯一定义；服务端在 `AdminServiceImpl::change_membership` 内强制（空集合**无条件拒绝**，`force` 不能绕过 n→0）；低于下限返回 `RaftError::UnsafeMembershipChange` → `failed_precondition`；`--force` 逃生（proto3 wire-additive）且**强制产生 warn 级审计事件**（含完整目标 voter 集合）。守卫只约束目标集合大小，不限制扩容；滚动升级期间旧 server 无守卫，不要依赖。

---

openraft keeps membership changes correct but says nothing about availability — shrinking to 1 voter is legal but a fragile SPOF, and shrinking to 0 tears the group down. Decision: move `MIN_VOTERS_AFTER_REMOVAL = 2` from rr-admin up to rr-core as the single repo-wide definition; enforce server-side in `AdminServiceImpl::change_membership` (empty set **unconditionally rejected** — `force` cannot bypass n→0); below the floor returns `RaftError::UnsafeMembershipChange` → `failed_precondition`; a `--force` escape hatch (proto3 wire-additive) **mandatorily emits a warn-level audit event** with the full target voter set. The guard constrains only the target size, never growth; during rolling upgrades old servers have no guard — do not rely on it.

## A-0013 — 删除 `GroupMeta.leader` / Remove `GroupMeta.leader`

`GroupMeta` 是配置面静态元数据，而 leader 是 raft 运行时状态——把后者塞进路由表必然过期、误导客户端。决策：删除该字段及其唯一消费链（`ShardRouter::get_leader*`、`RouterError::NoLeader`、对应 metric 与 gRPC 映射），leader 发现归位到既有的 client redirect 缓存（`ForwardToLeader` → `leader_info` → 缓存重试）；admin 观测面展示 leader 的正确数据源是 raft metrics 的 `current_leader`。源码核实：etcd 从不持久化该字段（读取时由 `replicas[0]` 现场派生）、proto 中无 `GroupMeta` 消息——删除对存量数据与 wire 零影响，无迁移。

---

`GroupMeta` is config-plane static metadata, while leader is raft runtime state — embedding the latter in the routing table is guaranteed to go stale and mislead clients. Decision: delete the field and its entire consumption chain (`ShardRouter::get_leader*`, `RouterError::NoLeader`, the associated metric and gRPC mapping); leader discovery returns to the existing client redirect cache (`ForwardToLeader` → `leader_info` → cached retry); the correct data source for leader observability on the admin surface is raft metrics' `current_leader`. Verified against source: etcd never persisted the field (it was derived on read from `replicas[0]`) and no proto message carries `GroupMeta` — removal has zero impact on stored data and wire protocols; no migration needed.

## A-0014 — TCP transport 安全门 / TCP Transport Security Gate

实验性 TCP raft transport（`RRAF` 二进制帧）**没有 TLS、没有任何对端认证、没有限流**，且它在 rr-server 二进制的 `start_node` 里绑定——完全在 `RrNode` 生命周期之外，原有 5 条启动门根本感知不到它的存在。决策：`NodeConfig` 增加 `raft_transport` 描述符（单一事实源，与 `main.rs` 的地址派生共享同一逻辑），新增两条门：**G6** 选 TCP 就必须显式 `allow_insecure`（gRPC 面 TLS 合规不能掩盖 TCP 裸奔）；**G7** bind 地址必须 loopback 且**不可被 `allow_insecure` 绕过**——这是"allow_insecure 短路一切"语义的首次有意例外。记录在案的残余缺口：库用户直接 `TcpRaftServer::bind` 可绕过门禁，rustdoc 已加安全警示。

---

The experimental TCP raft transport (`RRAF` binary frames) has **no TLS, no peer authentication of any kind, no rate limiting**, and it is bound inside the rr-server binary's `start_node` — entirely outside the `RrNode` lifecycle, invisible to the original five startup gates. Decision: add a `raft_transport` descriptor to `NodeConfig` (single source of truth, sharing the same address-derivation logic as `main.rs`) and two new gates: **G6** choosing TCP always requires explicit `allow_insecure` (a compliant gRPC TLS face must not mask a plaintext TCP port); **G7** the bind address must be loopback and **cannot be bypassed by `allow_insecure`** — the first deliberate exception to the "allow_insecure short-circuits everything" semantics. Recorded residual gap: library users calling `TcpRaftServer::bind` directly bypass the gates; the rustdoc carries a security warning.

## 相关 / Related

- 启动安全门与成员守卫的运维视角 / Operator view of the gates & membership guard: [../deployment/security.md](../deployment/security.md)
- 持久化契约的恢复语义 / Recovery semantics of the durability contracts: [durability.md](./durability.md)
- 幂等与 ingestor 的使用文档 / Usage guides for idempotency & ingestor: [../guide/idempotency.md](../guide/idempotency.md)、[../guide/kafka-input.md](../guide/kafka-input.md)
