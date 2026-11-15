---
sidebar_position: 8
---

# 与 openraft 的关系 / Relationship with openraft

Rr 不重新实现 Raft 算法，而是在 [openraft](https://github.com/datafuselabs/openraft) 之上提供多共识组框架。

Rr does not reimplement Raft; it provides a multi-group consensus framework on top of [openraft](https://github.com/datafuselabs/openraft).

## openraft 提供什么 / What openraft Provides

openraft 是单共识组的 Raft 库，实现 [Raft 论文](https://raft.github.io/raft.pdf) 及其扩展：

openraft is a single-group Raft library implementing the [Raft paper](https://raft.github.io/raft.pdf) and its extensions:

- 领导者选举、日志复制和安全性机制。<br />Leader election, log replication, and safety machinery.
- 联合共识成员变更。<br />Joint-consensus membership changes.
- 快照与日志压缩原语。<br />Snapshot and log-compaction primitives.
- ReadIndex 等线性一致读原语。<br />Linearizable-read primitives such as ReadIndex.
- 由使用方实现的 `RaftLogStorage`、`RaftStateMachine`、`RaftNetwork` / `RaftNetworkFactory` trait。<br />The `RaftLogStorage`, `RaftStateMachine`, and `RaftNetwork` / `RaftNetworkFactory` traits implemented by the consumer.

openraft 提供一个组的引擎；存储、网络和部署形态由使用方补齐。

openraft supplies one group's engine; storage, networking, and deployment remain the consumer's responsibility.

## Rr 增加的能力 / What Rr Adds

| 能力 / Capability | 行为 / Behavior | 位置 / Location |
| :--- | :--- | :--- |
| **多组生命周期 / Multi-group lifecycle** | `RaftGroupManager` 以 `DashMap<GroupId, Arc<Mutex<GroupState>>>` 管理本节点多个组的启动、停止和初始化。<br />`RaftGroupManager` starts, stops, and initializes local groups through `DashMap<GroupId, Arc<Mutex<GroupState>>>`. | `crates/rr-raft/src/group_manager.rs` |
| **成员编排 / Membership orchestration** | `add_learner` / `change_membership` 使用联合共识；`MIN_VOTERS_AFTER_REMOVAL = 2` 限制危险缩容；legacy `rr-server` 默认自动提升 learner（`auto_promote_learners = true`），可通过 TOML `cluster.auto_promote_learners` 或 `NodeConfig::with_auto_promote_learners` 配置；managed formation 强制关闭自动提升。<br />Joint-consensus `add_learner` / `change_membership`; `MIN_VOTERS_AFTER_REMOVAL = 2` guards shrinking; legacy `rr-server` auto-promotes learners by default (`auto_promote_learners = true`), configured through TOML `cluster.auto_promote_learners` or `NodeConfig::with_auto_promote_learners`; managed formation forcibly disables auto-promotion. | `rr-raft` / `rr-core` |
| **分片路由 / Shard routing** | `RoutingTable` 维护 `PartitionId → GroupId` 与 `NodeId → addr`；`ShardRouter` 每次请求经 `ConfigProvider` 取表，无本地缓存。<br />`RoutingTable` maps `PartitionId → GroupId` and `NodeId → addr`; `ShardRouter` fetches it through `ConfigProvider` per request, without a local cache. | `rr-router` |
| **网络 / Networking** | `RaftService`（3 RPC）、`ClientService`（4 RPC）、`AdminService`（16 RPC）；连接池；实验性明文 loopback TCP（`RRAF` 帧）。<br />`RaftService` (3 RPCs), `ClientService` (4), `AdminService` (16); connection pooling; experimental plaintext loopback-only TCP with `RRAF` framing. | `rr-network` |
| **客户端 SDK / Client SDK** | 基于 `ClientResponse.leader_info` 的 leader 重定向、指数退避重试和无 TTL 的 leader 缓存。<br />Leader redirects via `ClientResponse.leader_info`, exponential-backoff retries, and a leader cache without TTL. | `rr-client` |
| **框架幂等 / Framework idempotency** | `FramedEntry{version, offset, idem_key, payload}`、`IdempotencyLayer`（LRU + 水位线）、`send_request_idempotent` 和 `RrError::IdempotencyExpired`。<br />`FramedEntry{version, offset, idem_key, payload}`, `IdempotencyLayer` (LRU + watermark), `send_request_idempotent`, and `RrError::IdempotencyExpired`. | `rr-core` / `rr-client` |
| **接入与输出 / Ingest and egress** | Kafka at-least-once 摄入结合框架去重实现 exactly-once apply；outbox 提供 at-least-once 输出。<br />Kafka at-least-once ingest plus framework dedup gives exactly-once apply; the outbox provides at-least-once delivery. | `rr-ingestor-kafka` / `rr-core` |
| **配置提供者 / Config providers** | `MemConfigProvider` 可写；`EtcdConfigProvider` 只读，Rr 不回写路由表到 etcd。<br />`MemConfigProvider` is writable; `EtcdConfigProvider` is read-only, and Rr never writes routing tables back to etcd. | `rr-config` / `rr-config-etcd` |
| **安全 / Security** | TLS/mTLS（RaftService 强制 mTLS）、分离的 admin/client bearer token、按服务与调用方限流、G1–G7 启动安全门。<br />TLS/mTLS (mandatory mTLS for RaftService), separate admin/client bearer tokens, per-service/per-caller rate limits, and G1–G7 startup gates. | `rr-server` |
| **存储 / Storage** | 默认 `mem` 全内存；`--storage rocks` 持久化 Raft 日志和 `sm-snapshot.bin`，快照写入 fail-closed。<br />Default `mem` is memory-only; `--storage rocks` persists Raft logs and `sm-snapshot.bin`, with fail-closed snapshot writes. | `rr-storage` |
| **运维工具 / Operations** | `rr-admin` CLI + REPL、DrainNode/退役、OnlineBackup 快照流、离线 tar.gz + SHA-256 备份、node-reset、metrics 和 tracing。<br />`rr-admin` CLI + REPL, DrainNode/decommission, OnlineBackup snapshot streaming, offline tar.gz + SHA-256 backups, node-reset, metrics, and tracing. | `rr-admin` |

## 封装方式 / How Rr Wraps openraft

- **类型配置 / Type configuration**：`RrTypeConfig` 通过 `openraft::declare_raft_types!` 绑定 Rr 的请求与响应类型。<br />`RrTypeConfig` uses `openraft::declare_raft_types!` to bind Rr's request and response types.
- **节点封装 / Node wrapper**：每组一个 `RaftNode`，持有 `openraft::Raft<RrTypeConfig, RrStateMachine>`。<br />Each group has a `RaftNode` holding `openraft::Raft<RrTypeConfig, RrStateMachine>`.
- **存储适配 / Storage adapters**：`RrLogStorage` 实现 `RaftLogStorage<RrTypeConfig>`；`RocksLogStorage` 是 `rocks` feature 下的 RocksDB 实现。<br />`RrLogStorage` implements `RaftLogStorage<RrTypeConfig>`; `RocksLogStorage` is the RocksDB implementation behind `rocks`.
- **状态机适配 / State-machine adapter**：`RrStateMachine` 把用户的 `Service` / `StateMachine` 接入 openraft 的 apply、snapshot 和 restore。<br />`RrStateMachine` bridges user `Service` / `StateMachine` implementations to openraft's apply, snapshot, and restore interfaces.
- **网络适配 / Network adapters**：`GrpcNetworkFactory` 和实验性 `TcpRaftNetworkFactory` 实现 `RaftNetworkFactory`；快照通过 gRPC 流式 RPC 传输。<br />`GrpcNetworkFactory` and experimental `TcpRaftNetworkFactory` implement `RaftNetworkFactory`; snapshots use streaming gRPC.
- **组管理 / Group management**：`RaftGroupManager` 持有所有 `RaftNode`，提供写入、读取、成员变更和快照触发等组级 API。<br />`RaftGroupManager` owns all `RaftNode`s and exposes group-level write, read, membership, and snapshot APIs.

## 版本锁定 / Version Pinning

根 `Cargo.toml` 将 openraft 精确锁定为 `= 0.10.0-alpha.34`。pre-1.0 alpha 的相邻版本可能改变 API，精确锁定避免 `cargo update` 隐式升级；升级必须经过兼容性检查和测试。Rr 升级时以 [reference/compatibility](../reference/compatibility.md) 为准。

The root `Cargo.toml` pins openraft to `= 0.10.0-alpha.34`. APIs can change between pre-1.0 alpha releases, so the exact pin prevents implicit upgrades through `cargo update`; upgrades require compatibility checks and tests. When upgrading Rr, consult [reference/compatibility](../reference/compatibility.md).

## Vendored 补丁台账 / Vendored Patch Ledger

`vendor/openraft` 经 `[patch.crates-io]` 生效，当前记录 5 个本地补丁。#3/#4/#5 直接维护 INV-1：已 ACK 条目不丢失。升级 openraft 时必须逐条核对；上游状态栏的时间点是 **2026-11-04**。

`vendor/openraft` is activated through `[patch.crates-io]` and currently records five local patches. #3/#4/#5 directly uphold INV-1: no loss of ACKed entries. Reconcile every row when upgrading openraft; the upstream-status column is dated **2026-11-04**.

| 补丁 / Patch | 提交 / Commit | 问题与影响 / Problem and Impact | 上游状态 / Upstream Status |
| :--- | :--- | :--- | :--- |
| #1 快照清理竞态 / Snapshot-purge race | `9134d87` | 排队的 `PurgeLog` 可先清掉 `run_save_committed_and_apply` 要 unwrap 的 index，导致 RaftCore panic 和多数派丢失；16k/s×1KiB 下约 2.5 分钟在 2/3 节点复现。<br />A queued `PurgeLog` can remove an index before `run_save_committed_and_apply` unwraps it, panicking RaftCore and losing quorum; reproduced on 2/3 nodes in ~2.5 min at 16k/s×1KiB. | 补丁时点上游 main 未修；alpha.34 是当时最新 release。<br />Unfixed on upstream main at patch time; alpha.34 was the latest release then. |
| #2 vote-term 采纳 / Vote-term adoption | `9fa8347`；上游测试 `985b594`。<br />`9fa8347`; upstream test `985b594`. | `handle_vote_req` 的日志新鲜度拒绝分支不采纳更高 term，可能使集群 term 在过载扰动后不再收敛。<br />The log-recency rejection branch of `handle_vote_req` did not adopt a higher term, which could prevent cluster terms from reconverging after overload churn. | 未核对更新后的上游状态。<br />Not rechecked against newer upstream. |
| #3 become-leader flush 门控 / Become-leader flush gating | `bd698d2` | become-leader 的 `UpdateIOProgress` 未等待真实 flush，破坏 INV-1。<br />`UpdateIOProgress` on become-leader was not gated on a real flush, violating INV-1. | 未核对更新后的上游状态。<br />Not rechecked against newer upstream. |
| #4/#5 committed-entry 擦除守卫 / Committed-entry erasure guards | `0582499` | 短日志节点当选后，follower 可采纳其 committed 游标并物理擦除约 104 万条已提交且已 ACK 的条目；soak-nightly 实证。<br />After a short-log node won an election, followers could adopt its committed cursor and physically erase ~1.04M committed, ACKed entries; observed in soak-nightly. | 未核对更新后的上游状态。<br />Not rechecked against newer upstream. |

升级清单 / Upgrade checklist:

1. 核对上游 changelog 和 commit；已有等价修复的补丁可删除。<br />Check upstream changelogs and commits; drop patches with equivalent upstream fixes.
2. 移植其余补丁，并运行 `just gate-crash`、`just soak-smoke`、`io_flush_tracking` 单测和全量 `cargo test`。<br />Port the remaining patches, then run `just gate-crash`, `just soak-smoke`, the `io_flush_tracking` unit tests, and full `cargo test`.
3. 更新根 `Cargo.toml` 的精确版本和本台账。<br />Update the exact pin in the root `Cargo.toml` and this ledger.

## 何时直接使用 openraft / When to Use openraft Directly

以下场景可直接使用 openraft：

Use openraft directly when:

- 单个共识组已足够，不需要分片。<br />One consensus group is sufficient and no sharding is needed.
- 需要最小依赖树，不接受 Rr 引入的 tonic、DashMap、metrics 等依赖。<br />You need a minimal dependency tree without Rr's tonic, DashMap, metrics, and related dependencies.
- 已有自研存储或传输层，需要完全控制这些边界。<br />You already have custom storage or transport and need full control of those boundaries.
- 需要持续跟踪 openraft 最新 API，而不是固定 alpha 版本。<br />You need to track the latest openraft APIs rather than a fixed alpha release.

构建需要运维工具链的多分片强一致服务时，Rr 用部分底层灵活性换取集成后的多组框架。更多选型说明见 [FAQ](../faq.md)。

For a sharded, strongly consistent service that needs an operational toolchain, Rr trades some low-level flexibility for an integrated multi-group framework. See the [FAQ](../faq.md) for selection guidance.
