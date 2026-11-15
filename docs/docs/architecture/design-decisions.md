---
sidebar_position: 8
---

# 设计决策记录（ADR 索引） / Design Decisions (ADR Index)

:::info 文档性质 / About This Page
Rr 的架构决策以 **ADR A-0001 … A-0019** 编号，代码注释通过 `ADR A-` 引用，可用
`grep -rn "ADR A-" crates/` 查找。完整 ADR 位于 `.sisyphus/adr/`，不由 Docusaurus 发布。
本页提炼代码引用与 ADR；如与代码注释不一致，以代码注释为准。表中列出决策的代码或设计锚点。

Rr numbers architecture decisions **ADR A-0001 … A-0019** and references them from code comments as
`ADR A-`; locate them with `grep -rn "ADR A-" crates/`. Full ADRs live in `.sisyphus/adr/` and are not
published by Docusaurus. This page distils the code references and ADRs; code comments win if they disagree.
The table lists each decision's code or design anchor.
:::

## 总表 / Index

| ADR | 主题 / Topic | 状态 / Status | 决策要点 / Decision | 代码或设计锚点 / Code or Design Anchors |
| :--- | :--- | :--- | :--- | :--- |
| A-0001 | `CommandIngestor` + Kafka | implemented | MQ 无关 trait；leader 独占消费，follower 经 Raft 复制。<br />MQ-agnostic trait; leader-only consumption, followers fed by Raft. | `rr-core/src/ingestor.rs:66`, `rr-raft/src/ingestor_runner.rs`, `rr-ingestor-kafka/` |
| A-0002 | 快照一致性与恢复 / Snapshot consistency and recovery | implemented | 原子快照、apply 完成才返回、restore 全量替换；明确持久化语义。<br />Atomic snapshots, completed applies, wholesale restore, and explicit durability semantics. | `rr-core/src/service.rs:131`, `rr-core/src/state_machine.rs:99-130`, `rr-storage/src/rocks.rs:34` |
| A-0003 | 框架幂等 / Framework idempotency | implemented | 按 `(request_type, request_id)` 去重；watermark + 有界 LRU；按请求 opt-in。<br />Dedup by `(request_type, request_id)` with watermark + bounded LRU; per-request opt-in. | `rr-raft/src/idempotency.rs:1-49`, `rr-core/src/idempotency.rs` |
| A-0004 | etcd 配置 schema / etcd config schema | implemented | typed prefix + JSON；`ConfigProvider` 只读。<br />Typed prefixes + JSON; read-only `ConfigProvider`. | `rr-config-etcd/src/lib.rs:5-7,105-121`；无代码内 ADR 注释。<br />`rr-config-etcd/src/lib.rs:5-7,105-121`; no in-code ADR comment. |
| A-0005 | 按调用方限流 / Per-caller rate limiting | implemented | token 指纹独立桶；未认证请求和调用方表溢出后的新调用方使用对应 `admin_rps` / `client_rps` 全局桶，启用 per-caller 时必须配置。<br />Token-fingerprint buckets; unauthenticated requests and new callers after caller-map overflow use the matching `admin_rps` / `client_rps` global bucket, which is required when per-caller limiting is enabled. | `rr-network/src/rate_limit.rs`, `rr-server/src/config_file.rs:115,155` |
| A-0006 | RocksDB 按列族调参 / Per-CF RocksDB tuning | implemented | `raft_log_cf` / `state_machine_cf` 可选覆盖；`metadata` 不开放。<br />Optional `raft_log_cf` / `state_machine_cf` overrides; no `metadata` override. | `rr-storage/src/rocks.rs:19-26,177-180,253`；无代码内 ADR 注释。<br />`rr-storage/src/rocks.rs:19-26,177-180,253`; no in-code ADR comment. |
| A-0007 | Output / Outbox | implemented | apply 返回 outputs；leader-only runner 无限重试；outbox 随快照复制。<br />Apply returns outputs; leader-only runner retries forever; outbox replicates in snapshots. | `rr-core/src/service.rs:149-155`, `rr-raft/src/output_runner.rs:3` |
| A-0008 | 快照策略 / Snapshot strategy | implemented | `LogsSinceLast(N)`，默认 50_000；快照是框架操作而非业务命令。<br />`LogsSinceLast(N)`, default 50_000; snapshots are framework operations, not business commands. | `rr-raft/src/lib.rs:11`, `rr-raft/src/raft_node.rs:74`, `rr-server/src/config.rs:115-118` |
| A-0009 | 读写 RPC 与锁 / Read/write RPCs and locking | implemented | `Request` 与 `ReadLocal` 分离；apply/stale read 共享读锁，lifecycle 使用写锁。<br />Separate `Request` and `ReadLocal`; apply/stale reads share read locks, lifecycle uses write locks. | `rr-client/src/lib.rs:377` |
| A-0010 | WAL 日志后端 / WAL log backend | accepted（暂缓）<br />accepted (deferred) | 自研分段 WAL；先证明 LSM 是瓶颈；作为第三后端，不换默认。<br />Custom segmented WAL only after profiling proves LSM is the bottleneck; third backend, default unchanged. | 无实现；计划见 `.sisyphus/plans/rr-wal-impl.md`。<br />No implementation; plan: `.sisyphus/plans/rr-wal-impl.md`. |
| A-0011 | 关停持久化 C1–C7 / Shutdown durability C1–C7 | implemented | fsync 先于 ACK；vote 直连；固定关停顺序；快照完整或不存在；offset 映射 fail-closed。<br />fsync before ACK; direct votes; fixed shutdown; complete-or-absent snapshots; fail-closed offset mapping. | `rr-raft/src/io_batcher.rs:36-62,277`, `rr-raft/src/rocks_log_storage.rs:12,137`, `rr-raft/src/group_manager.rs:465` |
| A-0012 | 成员变更 quorum 守卫 / Membership quorum guard | implemented | `MIN_VOTERS_AFTER_REMOVAL = 2`；`--force` 可缩到 1 但必须审计，n→0 永不接受。<br />`MIN_VOTERS_AFTER_REMOVAL = 2`; audited `--force` can shrink to 1, never to 0. | `rr-core/src/membership.rs:19`, `rr-server/src/admin.rs:582-643` |
| A-0013 | 删除 `GroupMeta.leader` / Remove `GroupMeta.leader` | implemented | leader 属于 Raft 运行时，不进入配置路由表；客户端通过 redirect 发现。<br />Leader is Raft runtime state, not routing configuration; clients discover it via redirects. | `rr-core/src/types.rs:27,92-104`, `rr-server/src/dispatch.rs:201`, `rr-router/src/lib.rs:21` |
| A-0014 | TCP transport 安全门 / TCP transport security gate | implemented | TCP 必须 `allow_insecure`；loopback 限制不可绕过。<br />TCP requires `allow_insecure`; loopback-only binding cannot be bypassed. | `rr-server/src/node.rs:34-69`, `rr-server/src/config.rs:29-45`, `rr-network/src/tcp_raft_server.rs:1-15` |
| A-0015 | 日志可见性契约 / Log visibility contract | implemented | append 提交即可见：pending overlay + flush barrier；截断和清理先 drain。<br />Submitted appends are visible through a pending overlay + flush barrier; truncation and purge drain first. | `rr-raft/src/rocks_log_storage.rs:12-40`, `rr-raft/src/io_batcher.rs:412-434` |
| A-0016 | 生产 durable log 接线 / Production durable-log wiring | implemented | 日志后端与 network factory 正交；`--storage rocks` 配置每组 RocksDB。<br />Log storage is orthogonal to network factory; `--storage rocks` configures per-group RocksDB. | `rr-raft/src/group_manager.rs:108,192`, `rr-server/src/main.rs:487-498` |
| A-0017 | `Service` Bytes 化 / `Service` Bytes migration | drafted | 计划以 `Bytes` 替代 `Vec<u8>` 消除 apply 拷贝；当前公开签名仍为 `Vec<u8>`。<br />Proposes `Bytes` instead of `Vec<u8>` to remove apply copies; current public signatures still use `Vec<u8>`. | `rr-core/src/service.rs:136-237`, `rr-raft/src/type_config.rs:8-10` |
| A-0018 | Apply 错误契约 / Apply error contract | drafted | 当前 apply 错误为 group-fatal；入口校验已落地，error-as-applied-response 仍待评审。<br />Apply errors remain group-fatal; ingress validation exists, while error-as-applied-response remains under review. | `rr-raft/src/service_adapter.rs:20-40`, `rr-core/src/ingress.rs` |
| A-0019 | G2 性能决策 / G2 performance decisions | accepted | `ordered` 旋钮延后；多组扇出复测已完成；单组 100k 仍是目标而非默认能力。<br />`ordered` knob deferred; multi-group fanout retested; single-group 100k remains aspirational, not a default capability. | `.sisyphus/adr/A-0019-g2-decisions.md`, `docs/perf/roadmap-100k.md` |

> **编号与状态**：A-0001 至 A-0019 均存在于 `.sisyphus/adr/`。A-0004、A-0006、A-0010、
> A-0017、A-0019 没有代码内 `ADR A-` 注释，表中锚点指向其模块、代码或计划。A-0010 已 accepted 但实现暂缓；
> A-0017、A-0018 为 drafted；A-0019 为 accepted。
>
> **Numbering and status**: A-0001 through A-0019 exist in `.sisyphus/adr/`. A-0004, A-0006, A-0010, A-0017,
> and A-0019 have no in-code `ADR A-` comment, so the table points to their modules, code, or plans. A-0010 is
> accepted but deferred; A-0017 and A-0018 are drafted; A-0019 is accepted.

## A-0001 — CommandIngestor 抽象 / CommandIngestor Abstraction

rr-core 定义 MQ 无关的 `CommandIngestor`（`poll` / `seek` / `commit_applied`），关联 `Offset` 经 bincode 擦除为 `DynCommandIngestor`；`rr-ingestor-kafka` 是参考实现，NATS / Pulsar / RocketMQ 推迟。每个 `(group_id, consumer)` 同时只有一个 ingestor，leader 当选启动、下台停止；`poll` 返回单调 offset，只有 `commit_applied` 可提交上游位点。Kafka 约定每组一个单 partition topic：`input_{group_id}`。

rr-core defines an MQ-agnostic `CommandIngestor` (`poll` / `seek` / `commit_applied`), with `Offset` bincode-erased into `DynCommandIngestor`; `rr-ingestor-kafka` is the reference implementation, while NATS / Pulsar / RocketMQ are deferred. Exactly one ingestor exists per `(group_id, consumer)`, started on election and stopped on step-down. `poll` returns monotonic offsets, and only `commit_applied` may commit upstream. Kafka uses one single-partition topic per group: `input_{group_id}`.

## A-0002 — 快照一致性与灾难恢复 / Snapshot Consistency and Disaster Recovery

`snapshot()` 必须捕获原子一致视图；`apply()` 返回前必须完成状态更新，禁止 fire-and-forget 写任务；`restore()` 全量替换状态。`Balanced` / `Fast` 改名为 `ReplicatedOnly` / `TestOnly`，旧别名在 v0.3.0 移除。当 `last_applied < committed` 且日志已清理时，恢复路径是 `node-reset` 后通过集群快照追赶。

`snapshot()` must capture an atomically consistent view; `apply()` must finish state updates before returning, with no fire-and-forget writes; `restore()` replaces state wholesale. `Balanced` / `Fast` became `ReplicatedOnly` / `TestOnly`, and old aliases were removed in v0.3.0. When `last_applied < committed` and the log is purged, recovery requires `node-reset` followed by cluster snapshot transfer.

## A-0003 — 框架幂等层 / Framework Idempotency Layer

客户端按请求传入 `IdempotencyKey { request_type, request_id, client_id }`。去重身份为 `(request_type, request_id)`，`client_id` 仅用于诊断与碰撞检测。每个 request type 的 watermark 和有界 LRU 缓存产生 New / Replay / Expired / Collision；Replay 和 Collision 返回缓存响应，Expired 要求客户端换更大 ID。不带 key 的请求走原路径。

Clients opt in with `IdempotencyKey { request_type, request_id, client_id }`. Deduplication uses `(request_type, request_id)`; `client_id` is only for diagnostics and collision detection. A per-request-type watermark and bounded LRU produce New / Replay / Expired / Collision. Replay and Collision return cached responses; Expired requires a larger ID. Unkeyed requests use the original path.

## A-0004 — etcd 配置 schema / etcd Config Schema

etcd v3 布局为 `/rr/{cluster_id}/nodes/{node_id}`、`/rr/{cluster_id}/partitions/{partition_id}` 和 `/rr/{cluster_id}/groups/{group_id}/replicas`，value 使用 JSON，groups value 为 `Vec<NodeId>`。空 `cluster_id` 默认为 `"default"`；`ConfigProvider` 只读，写入仅用于测试准备。代码无 ADR 注释，实现锚点是 `rr-config-etcd` 模块。

The etcd v3 layout is `/rr/{cluster_id}/nodes/{node_id}`, `/rr/{cluster_id}/partitions/{partition_id}`, and `/rr/{cluster_id}/groups/{group_id}/replicas`. Values are JSON, with groups encoded as `Vec<NodeId>`. A blank `cluster_id` defaults to `"default"`; `ConfigProvider` is read-only, and writes are limited to test setup. There is no in-code ADR comment; the anchor is the `rr-config-etcd` module.

## A-0005 — 按调用方限流 / Per-Caller Rate Limiting

在 per-service 令牌桶上增加按 bearer token siphash 指纹划分的 per-caller 桶。未认证请求和调用方表已满后的新调用方请求使用对应的 `admin_rps` / `client_rps` 全局桶；设置 `per_caller_*_rps` 时必须同时配置对应全局 rps，否则配置校验拒绝启动。桶存于 `RwLock<HashMap>`，空闲 5 分钟逐出；拒绝事件携带 `caller_token_fp` 进入审计流。

Per-caller token buckets, keyed by bearer-token siphash fingerprints, sit alongside per-service buckets. Unauthenticated requests and requests from new callers after the caller map is full use the matching `admin_rps` / `client_rps` global bucket. Setting `per_caller_*_rps` requires the corresponding global rps; otherwise configuration validation rejects startup. Buckets live in `RwLock<HashMap>` and expire after five idle minutes. Rejections carry `caller_token_fp` in the audit stream.

## A-0006 — RocksDB 按列族调参 / Per-CF RocksDB Options Tuning

`RocksConfig` 为 `raft_log_cf` / `state_machine_cf` 提供可选的 `write_buffer_size_mb`、`block_cache_size_mb` 和 `compression` 覆盖。`metadata` 不开放覆盖。推荐起始值只写入文档，不设为代码默认值；实现位于 `rr-storage/src/rocks.rs` 的 `build_cf_options`。

`RocksConfig` provides optional `write_buffer_size_mb`, `block_cache_size_mb`, and `compression` overrides for `raft_log_cf` / `state_machine_cf`. `metadata` has no override. Suggested starting values remain documentation rather than code defaults; the implementation is `build_cf_options` in `rr-storage/src/rocks.rs`.

## A-0007 — Output/Outbox 机制 / Output / Outbox Mechanism

`Service::handle_with_outputs` 返回 `HandleResult { response, outputs }`，框架为 output 分配单调 `seq` 和幂等键。leader-only runner 对失败发送从 100ms 起指数退避、30s 封顶并无限重试，不静默丢弃；`sent_watermark` 仅在确认后推进。outbox 随快照复制，新 leader 从 watermark 续发；`handle` 保留默认委托以兼容既有实现。

`Service::handle_with_outputs` returns `HandleResult { response, outputs }`, and the framework assigns each output a monotonic `seq` and idempotency key. A leader-only runner retries failed sends forever with exponential backoff from 100ms, capped at 30s, never silently dropping them. `sent_watermark` advances only after confirmation. Outbox state replicates in snapshots so a new leader resumes from the watermark; `handle` retains its default delegation for compatibility.

## A-0008 — 快照策略 / Snapshot Strategy

快照在 apply 路径内由 `SnapshotPolicy::LogsSinceLast(N)` 触发，默认 `N = 50_000`，在 100k ops/s 目标下约每 500ms 一次。N 权衡快照开销与恢复重放长度，可通过 `NodeConfig`、TOML 和 CLI 的 `snapshot_logs_since_last` 配置。快照包含用户状态及 ingestor offset、幂等 watermark、outbox 等框架状态。

Snapshots trigger inside apply through `SnapshotPolicy::LogsSinceLast(N)`, defaulting to `N = 50_000`, approximately every 500ms at the 100k ops/s target. N trades snapshot cost against recovery replay length and is exposed as `snapshot_logs_since_last` in `NodeConfig`, TOML, and CLI. Snapshots contain user state plus framework state: ingestor offsets, idempotency watermarks, and outbox.

## A-0009 — 读写 RPC 分离与状态机锁 / Read/Write RPC Separation & State-Machine Locking

原 `ClientRequest.read_only: bool` 可能把写请求静默降级为 stale read。协议改为独立的 `Request` 和 `ReadLocal`，后续增加基于 ReadIndex 的 `ReadLinearizable`。锁模型为 `Arc<TokioRwLock<Service>>`：apply 和 stale read 持读锁，`on_leader_elected` / `on_step_down` / `shutdown` / `restore` 持写锁。

The original `ClientRequest.read_only: bool` could silently downgrade a write to a stale read. The protocol now separates `Request` and `ReadLocal`, with ReadIndex-based `ReadLinearizable` added later. Locking uses `Arc<TokioRwLock<Service>>`: apply and stale reads take read locks, while `on_leader_elected` / `on_step_down` / `shutdown` / `restore` take write locks.

## A-0010 — WAL 日志存储后端 / WAL Log-Storage Backend

决策草案是以 `std::fs` 实现分段 WAL：magic + CRC32C 帧、64 MiB 段滚动、vote/last_purged sidecar 原子改名，状态机与快照仍使用 RocksDB。**实现暂缓**：决策时的性能数据来自 debug build，瓶颈指向 CPU 与 gRPC RTT；须先以 release profile 证明 LSM 是主瓶颈。落地后作为 `StorageBackend::Wal` 与 Mem/Rocks 并存，默认不变。

The proposal is a segmented WAL built on `std::fs`: magic + CRC32C frames, 64 MiB segment rollover, and atomically renamed vote/last_purged sidecars, while state machines and snapshots remain on RocksDB. **Implementation is deferred**: performance data at decision time came from debug builds and pointed to CPU and gRPC RTT; a release profile must first prove LSM is the main bottleneck. If implemented, `StorageBackend::Wal` will coexist with Mem/Rocks without changing the default.

## A-0011 — 关停持久化契约 / Shutdown Durability Contract (C1–C7)

**C1**：Strict 下 fsync 先于 client ACK；**C2**：`save_vote` 直连存储；**C3**：`stop_group` 按停 supervisor → drain → `raft.shutdown()` → `log_storage.close()` 执行，`IoBatcher::close()` 保证排空、final fsync 和 completion 恰好应答一次，30s 超时返回 `Err`；**C4**：`kill -9` 后，已触发 `io_completed(Ok)` 的 entry 必须幸存；**C5**：这些保证仅属于 Strict；**C6**：快照完整或不存在，失败计入 `rr_snapshot_build_failures_total`；**C7**：offset→request_id 映射失败时不 propose、不提交 offset，supervisor 退出。

**C1**: under Strict, fsync precedes client ACK; **C2**: `save_vote` writes directly; **C3**: `stop_group` stops supervisors → drains → `raft.shutdown()` → `log_storage.close()`, while `IoBatcher::close()` drains, performs a final fsync, and answers every completion exactly once, returning `Err` after 30s; **C4**: after `kill -9`, entries with `io_completed(Ok)` must survive; **C5**: these guarantees apply only to Strict; **C6**: snapshots are complete or absent, with failures counted by `rr_snapshot_build_failures_total`; **C7**: offset→request_id mapping failure performs no proposal or offset commit and exits the supervisor.

## A-0012 — 成员变更 quorum 守卫 / Membership Quorum Guard

`MIN_VOTERS_AFTER_REMOVAL = 2` 在 rr-core 统一定义，并由 `AdminServiceImpl::change_membership` 强制。低于下限返回 `RaftError::UnsafeMembershipChange` → `failed_precondition`；`--force` 可缩到 1，但必须产生包含目标 voter 集合的 warn 级审计。空集合无条件拒绝，`force` 不能绕过。守卫不限制扩容；滚动升级期间旧 server 可能没有该守卫。

`MIN_VOTERS_AFTER_REMOVAL = 2` is defined once in rr-core and enforced by `AdminServiceImpl::change_membership`. Lower targets return `RaftError::UnsafeMembershipChange` → `failed_precondition`; `--force` may shrink to one but must emit a warn-level audit containing the full target voter set. Empty targets are always rejected, even with `force`. The guard does not limit growth; old servers may lack it during rolling upgrades.

## A-0013 — 删除 `GroupMeta.leader` / Remove `GroupMeta.leader`

leader 是 Raft 运行时状态，不属于静态配置。决策删除 `GroupMeta.leader` 及 `ShardRouter::get_leader*`、`RouterError::NoLeader`、关联 metric 和 gRPC 映射；leader 发现使用 `ForwardToLeader` → `leader_info` → 客户端缓存，admin 观测使用 raft metrics 的 `current_leader`。etcd 原本不持久化该字段，proto 也无 `GroupMeta` 消息，因此无需数据或 wire 迁移。

Leader is Raft runtime state, not static configuration. The decision removed `GroupMeta.leader`, `ShardRouter::get_leader*`, `RouterError::NoLeader`, associated metrics, and gRPC mappings. Discovery uses `ForwardToLeader` → `leader_info` → client cache, while admin observability uses raft metrics' `current_leader`. Etcd never persisted the field and proto has no `GroupMeta` message, so no data or wire migration is required.

## A-0014 — TCP transport 安全门 / TCP Transport Security Gate

实验性 TCP transport 使用 `RRAF` 帧，无 TLS、对端认证或限流，且在 `start_node` 绑定，不受原有五条启动门覆盖。`NodeConfig.raft_transport` 作为描述符；G6 要求 TCP 显式 `allow_insecure`，G7 强制 loopback 且不可被 `allow_insecure` 绕过。残余缺口：库用户直接调用 `TcpRaftServer::bind` 可绕过门禁，rustdoc 已记录警告。

The experimental TCP transport uses `RRAF` frames without TLS, peer authentication, or rate limiting, and binds in `start_node`, outside the original five startup gates. `NodeConfig.raft_transport` describes the transport; G6 requires explicit `allow_insecure` for TCP, and G7 mandates loopback without an `allow_insecure` bypass. Residual gap: library users calling `TcpRaftServer::bind` directly bypass the gates, as documented by a rustdoc warning.

## A-0015 — Raft 日志可见性契约 / Raft Log Visibility Contract

openraft 在 append 提交后、flush 前即可读取日志，因此异步入队而不提供可见性会跳过 apply。`RocksLogStorage` 使用 pending overlay：`append` 先写入覆盖层，`try_get_log_entries` 在扫描 RocksDB 前克隆覆盖层并合并，flush 后精确摘除。`truncate_after` / `purge` 先等待 `IoBatcher::flush_barrier()`，再清理覆盖层并删除，避免已截断 entry 复活。该设计保留组提交，ADR 记录的既有实测增幅为约 26% TPS。

Openraft can read a log after append submission but before flush, so enqueueing without visibility can skip applies. `RocksLogStorage` uses a pending overlay: `append` stages entries, `try_get_log_entries` clones the overlay before scanning RocksDB and merges the results, and a post-flush hook removes entries. `truncate_after` / `purge` await `IoBatcher::flush_barrier()` before clearing the overlay and deleting, preventing truncated entries from reappearing. This preserves group commit, whose previously measured gain was about 26% TPS.

## A-0016 — 生产二进制接线 durable log / Durable Log Wiring in Production Binaries

`RaftGroupManager::with_rocks_base_path` 将日志后端与 network factory 解耦：设置 base path 时，`start_group` 在 `{base_path}/{group_id}` 使用 Strict `RocksLogStorage`，否则使用内存 `RrLogStorage`。stock `rr-server --storage rocks` 设置 `{data_dir}/raft-log`，gRPC 与 TCP 路径一致；`start_group_rocks` 保留为兼容 shim。旧数据目录首启自动创建 `raft-log`，无需迁移。

`RaftGroupManager::with_rocks_base_path` decouples log storage from the network factory: with a base path, `start_group` uses Strict `RocksLogStorage` at `{base_path}/{group_id}`; otherwise it uses in-memory `RrLogStorage`. Stock `rr-server --storage rocks` sets `{data_dir}/raft-log` for both gRPC and TCP. `start_group_rocks` remains a compatibility shim. Existing data directories create `raft-log` on first startup without migration.

## A-0017 — `Service` Bytes 化 / `Service` Bytes Migration

该草案计划将 `Service` 请求/响应及 `RrAppData` 从 `Vec<u8>` 迁到 `bytes::Bytes`，减少 apply 路径强制拷贝。2026-09-22 基线采样中分配器约占 leader CPU 13%、拷贝约占 5%。这是公开 trait 的破坏性 API 变更，需要 golden corpus 验证 bincode 兼容；当前代码仍使用 `Vec<u8>`，状态保持 drafted。

This draft proposes migrating `Service` request/response types and `RrAppData` from `Vec<u8>` to `bytes::Bytes` to avoid forced apply-path copies. Baseline sampling on 2026-09-22 attributed about 13% of leader CPU to the allocator and 5% to copying. It is a breaking public-trait change requiring a golden corpus for bincode compatibility; current code still uses `Vec<u8>`, and the status remains drafted.

## A-0018 — Apply 期错误契约 / Apply Error Contract

当前 decode 或 `Service::handle` 错误会使 openraft SM Worker 退出并令 Raft 组进入 Fatal；rocks 重启可能重放同一毒条目。2026-09-23 已增加 `rr_core::ingress::validate_client_payload` 入口校验，关闭客户端可达的畸形 house-magic 路径。草案短期维持 group-fatal 并补可观测性与恢复手册，中期评审确定性的 error-as-applied-response；依赖外部状态的错误不能直接转为已应用响应。

Currently, decode or `Service::handle` errors terminate the openraft SM Worker and make the Raft group Fatal; a rocks restart can replay the same poison entry. On 2026-09-23, `rr_core::ingress::validate_client_payload` added ingress validation for client-reachable malformed house-magic payloads. The draft keeps group-fatal semantics short term while adding observability and recovery procedures, and reviews deterministic error-as-applied-response later. Errors dependent on external state cannot simply become applied responses.

## A-0019 — G2 决策点 / G2 Decisions

2026-11-04 裁定：`BatchConfig::ordered` 旋钮延后至 v0.3.0 评审，不在未论证 deferred/idempotency 交互前放松顺序。扇出后的多组复测显示固定三节点上聚合吞吐仍不随组数扩展，per-group Mutex 不是观测到的瓶颈。单组 100k 保持追求目标：121.2k TPS 来自 opt-in batch64 + fanout4，默认配置约 30–34k TPS，不宣布默认达成。

On 2026-11-04, the `BatchConfig::ordered` knob was deferred to v0.3.0 review rather than relaxing ordering before analyzing deferred/idempotency interactions. Multi-group retesting with fanout showed that aggregate throughput still did not scale with group count on a fixed three-node topology, and the per-group Mutex was not the observed bottleneck. Single-group 100k remains aspirational: 121.2k TPS required opt-in batch64 + fanout4, while default configuration measured about 30–34k TPS.

## 相关 / Related

- [启动安全门与成员守卫 / Startup gates and membership guard](../deployment/security.md)
- [持久化恢复语义 / Durability and recovery](./durability.md)
- [幂等指南 / Idempotency guide](../guide/idempotency.md)
- [ingestor 使用指南 / Ingestor guide](../guide/kafka-input.md)
