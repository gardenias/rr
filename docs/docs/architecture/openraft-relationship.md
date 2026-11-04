---
sidebar_position: 8
---

# 与 openraft 的关系 / Relationship with openraft

rr 不是 Raft 算法的重新实现，而是在 [openraft](https://github.com/datafuselabs/openraft) 之上构建的**多共识组框架**。理解两者的分工，是评估"rr 是否适合你的项目"的关键。

## openraft 提供什么

openraft 是一个**单共识组（single-group）** 的 Raft 共识库，忠实实现了 [Raft 论文](https://raft.github.io/raft.pdf)（含扩展）：

- 完整的领导者选举、日志复制、安全性证明级实现；
- **联合共识（joint consensus）** 成员变更；
- 快照（snapshot）与日志压缩的原语；
- ReadIndex 等线性一致读原语；
- 一组由**你自己实现**的核心 trait：`RaftLogStorage`（日志存储）、`RaftStateMachine`（状态机）、`RaftNetwork` / `RaftNetworkFactory`（节点间传输）。

换句话说，openraft 给你的是**一个**共识组的引擎，但存储、网络、部署形态都由你补齐。

## rr 在 openraft 之上增加了什么

生产级分片系统需要的远不止一个共识组。rr 补齐的能力：

| 能力 | 说明 | 位置 |
| :--- | :--- | :--- |
| **多共识组生命周期** | `RaftGroupManager` 以 `DashMap<GroupId, Arc<Mutex<GroupState>>>` 管理单节点上的多个共识组：启动 / 停止 / 初始化 | `crates/rr-raft/src/group_manager.rs` |
| **动态成员编排** | `add_learner` / `change_membership`（联合共识）；`MIN_VOTERS_AFTER_REMOVAL = 2` 成员安全护栏（`rr-core/src/membership.rs`）；learner 自动提升（`learner_promoter.rs`，stock `rr-server` 默认开启，可用 `--no-*` 覆盖） | `rr-raft` / `rr-core` |
| **表格式分片路由** | `RoutingTable` 维护 `PartitionId → GroupId` 与 `NodeId → addr` 映射（`rr-core/src/types.rs`）；`ShardRouter` 每请求经 `ConfigProvider` 取表查询，无本地缓存 | `rr-router` |
| **gRPC 网络层** | `RaftService`（3 RPC）、`ClientService`（4 RPC：Request / ReadLocal / ReadLinearizable / GetClusterInfo）、`AdminService`（15 RPC）；连接池；实验性 TCP 传输（`RRAF` 帧格式，明文，仅限 loopback） | `rr-network` |
| **客户端 SDK** | 基于 `ClientResponse.leader_info` 的自动 leader 重定向、指数退避重试、无 TTL 的 leader 缓存 | `rr-client` |
| **框架级幂等** | `FramedEntry{version, offset, idem_key, payload}` + `IdempotencyLayer`（LRU + 水位线），`send_request_idempotent`，`RrError::IdempotencyExpired` | `rr-core` / `rr-client` |
| **数据接入 / 输出** | Kafka ingestor（at-least-once + 框架去重 = exactly-once apply）；输出 outbox（at-least-once 投递） | `rr-ingestor-kafka` / `rr-core` |
| **配置提供者** | `MemConfigProvider`（内存，可写）；`EtcdConfigProvider`（**只读**——rr 不会把路由表写回 etcd） | `rr-config` / `rr-config-etcd` |
| **安全** | TLS/mTLS（RaftService 强制 mTLS）、bearer token（admin/client 分离）、按服务/按调用方限流、G1–G7 启动安全门 | `rr-server` |
| **存储** | `MemStorage`；RocksDB（`rocks` feature）：stock `rr-server --storage rocks` 持久化 Raft 日志（ADR A-0016）与状态机快照 `sm-snapshot.bin`（T1，fail-closed）；默认 `mem` 为全内存 | `rr-storage` |
| **运维工具** | `rr-admin` CLI + REPL、DrainNode/退役、OnlineBackup（tar.gz + sha256）、node-reset、metrics + tracing | `rr-admin` |

## rr 如何封装 openraft（技术细节)

- **类型配置**：`RrTypeConfig` 通过 `openraft::declare_raft_types!` 声明（`crates/rr-raft/src/type_config.rs`），把 rr 的请求/响应类型绑定到 openraft 的泛型上。
- **节点封装**：每个共识组是一个 `RaftNode`，内部持有 `openraft::Raft<RrTypeConfig, RrStateMachine>`（`crates/rr-raft/src/raft_node.rs`）。
- **存储适配**：`RrLogStorage` 实现 openraft 的 `RaftLogStorage<RrTypeConfig>`（`crates/rr-raft/src/log_storage.rs`）；`RocksLogStorage` 是 `rocks` feature 下的 RocksDB 实现。
- **状态机适配**：`RrStateMachine` 实现 `RaftStateMachine<RrTypeConfig>`（`crates/rr-raft/src/sm_adapter.rs`），把用户的 `Service` / `StateMachine` trait 桥接到 openraft 的 apply/snapshot/restore 接口上。
- **网络适配**：`GrpcNetworkFactory`（生产）与 `TcpRaftNetworkFactory`（实验）实现 openraft 的 `RaftNetworkFactory`；快照通过 gRPC 流式 RPC 传输。
- **组管理**：`RaftGroupManager` 持有全部 `RaftNode`，对外提供组级 API（写、本地读、线性一致读、成员变更、快照触发等）。

## 版本锁定策略

rr 将 openraft **精确锁定**在 `= 0.10.0-alpha.34`（见根 `Cargo.toml`）。原因很直接：openraft 目前处于 pre-1.0 alpha 阶段，相邻版本之间 API 可能变动。精确锁定保证了 rr 构建的可复现性，也让 openraft 升级成为一个**显式、经过测试的动作**，而不是 `cargo update` 的副作用。升级 rr 版本时，openraft 的兼容区间以 [reference/compatibility](../reference/compatibility.md) 为准。

## Vendored 补丁台账

rr 的 `vendor/openraft`（经 `[patch.crates-io]` 生效）携带 5 个本地补丁。其中 #3/#4/#5 直接承载
INV-1（已 ACK 条目不丢失），任何 openraft 升级都必须逐条核对本台账。

| 补丁 | 提交 | 问题 | 承载 | 上游状态（截至 2026-11-04） |
| :--- | :--- | :--- | :--- | :--- |
| #1 快照清理竞态 panic | `9134d87` | `run_save_committed_and_apply` unwrap 的 log index 可能被排队的 `PurgeLog` 抢先清掉 → `None` → RaftCore panic → 法定人数丢失（16k/s×1KiB 下约 2.5 分钟可复现 2/3 节点） | 进程存活/可用性 | 补丁时点上游 main 未修；alpha.34 已是当时最新 release |
| #2 vote-term 采纳 | `9fa8347`（语义钉入上游测试 `985b594`） | `handle_vote_req` 日志新鲜度拒绝分支提前返回、不采纳更高 term → 过载扰动后集群 term 永不收敛（perf-lab 永久 wedge 的根因之一） | 选举活性 | 未核对上游最新状态 |
| #3 become-leader flush 门控 | `bd698d2` | become-leader 的 `UpdateIOProgress` 未经真实 flush 门控（W9 INV-1 根因） | INV-1 | 未核对上游最新状态 |
| #4/#5 committed-entry 擦除守卫 | `0582499` | 短日志节点赢得选举 → follower 采纳其 committed 游标 → 约 104 万条已提交且已 ACK 的条目被物理擦除（soak-nightly 实证，`LogIOId` 的 (vote, log_id) 字典序与 #2 共同促成） | INV-1 | 未核对上游最新状态 |

**升级 openraft 时的核对清单**：

1. 逐条核对上游 changelog/commit 是否已包含等价修复；已包含的补丁直接删除。
2. 未包含的补丁必须移植，移植后强制跑：INV-1 相关套件（`just gate-crash` + `just soak-smoke` +
   `io_flush_tracking` 单测）与全量 `cargo test`。
3. 更新根 `Cargo.toml` 的精确版本锁定与本台账。

## 何时应该直接使用 openraft

以下情况直接用 openraft 可能更合适：

- **单个共识组就够了**——没有分片需求，多组管理是纯开销；
- **想要最小依赖树**——rr 会带入 gRPC（tonic）、DashMap、metrics 等一整套依赖；
- **想完全掌控存储与网络层**——例如已有自研存储引擎或传输协议，rr 的适配层反而限制你；
- **想跟踪 openraft 最新 API**——rr 锁定在固定的 alpha 版本。

反之，如果你要构建的是**多分片、强一致、需要运维工具链**的复制服务，rr 用一部分灵活性换来了一个完整、集成、经过测试的技术栈。更多选型讨论见 [FAQ](../faq.md)。

---

# Relationship with openraft

rr is not a re-implementation of the Raft algorithm. It is a **multi-group consensus framework** built on top of [openraft](https://github.com/datafuselabs/openraft). Understanding the split of responsibilities between the two is key to evaluating whether rr fits your project.

## What openraft provides

openraft is a **single-group** Raft consensus library, faithfully implementing the [Raft paper](https://raft.github.io/raft.pdf) (plus extensions):

- Complete leader election, log replication, and safety machinery;
- **Joint consensus** membership changes;
- Snapshot and log-compaction primitives;
- Linearizable-read primitives such as ReadIndex;
- A set of core traits that **you implement yourself**: `RaftLogStorage` (log storage), `RaftStateMachine` (state machine), `RaftNetwork` / `RaftNetworkFactory` (inter-node transport).

In other words, openraft gives you the engine for **one** consensus group; storage, networking, and deployment shape are yours to build.

## What rr adds on top of openraft

A production sharded system needs far more than one consensus group. rr fills the gap:

| Capability | Description | Location |
| :--- | :--- | :--- |
| **Multi-group lifecycle** | `RaftGroupManager` manages many groups per node via `DashMap<GroupId, Arc<Mutex<GroupState>>>`: start / stop / init | `crates/rr-raft/src/group_manager.rs` |
| **Membership orchestration** | `add_learner` / `change_membership` (joint consensus); `MIN_VOTERS_AFTER_REMOVAL = 2` safety guard (`rr-core/src/membership.rs`); learner auto-promotion (`learner_promoter.rs`, wired into stock `rr-server` by default, overridable via `--no-*` flags) | `rr-raft` / `rr-core` |
| **Table-based shard routing** | `RoutingTable` maps `PartitionId → GroupId` and `NodeId → addr` (`rr-core/src/types.rs`); `ShardRouter` fetches the table through `ConfigProvider` on every request — no local cache | `rr-router` |
| **gRPC networking** | `RaftService` (3 RPCs), `ClientService` (4 RPCs: Request / ReadLocal / ReadLinearizable / GetClusterInfo), `AdminService` (14 RPCs); connection pooling; experimental TCP transport (`RRAF` framing, plaintext, loopback-only) | `rr-network` |
| **Client SDK** | Automatic leader redirect via `ClientResponse.leader_info`, exponential-backoff retry, leader cache without TTL | `rr-client` |
| **Framework idempotency** | `FramedEntry{version, offset, idem_key, payload}` + `IdempotencyLayer` (LRU + watermark), `send_request_idempotent`, `RrError::IdempotencyExpired` | `rr-core` / `rr-client` |
| **Ingest / egress** | Kafka ingestor (at-least-once + framework dedup = exactly-once apply); output outbox (at-least-once delivery) | `rr-ingestor-kafka` / `rr-core` |
| **Config providers** | `MemConfigProvider` (in-memory, writable); `EtcdConfigProvider` (**read-only** — rr never writes the routing table back to etcd) | `rr-config` / `rr-config-etcd` |
| **Security** | TLS/mTLS (RaftService requires mTLS), bearer tokens (admin/client separated), per-service/per-caller rate limiting, G1–G7 startup safety gates | `rr-server` |
| **Storage** | `MemStorage`; RocksDB (`rocks` feature): the stock `rr-server --storage rocks` persists the Raft log (ADR A-0016) and the state-machine snapshot `sm-snapshot.bin` (T1, fail-closed); the default `mem` is fully in-memory | `rr-storage` |
| **Ops tooling** | `rr-admin` CLI + REPL, DrainNode/decommission, OnlineBackup (tar.gz + sha256), node-reset, metrics + tracing | `rr-admin` |

## How rr wraps openraft (technical view)

- **Type configuration**: `RrTypeConfig` is declared via `openraft::declare_raft_types!` (`crates/rr-raft/src/type_config.rs`), binding rr's request/response types to openraft's generics.
- **Node wrapper**: each consensus group is a `RaftNode` holding an `openraft::Raft<RrTypeConfig, RrStateMachine>` (`crates/rr-raft/src/raft_node.rs`).
- **Storage adapters**: `RrLogStorage` implements openraft's `RaftLogStorage<RrTypeConfig>` (`crates/rr-raft/src/log_storage.rs`); `RocksLogStorage` is the RocksDB implementation behind the `rocks` feature.
- **State-machine adapter**: `RrStateMachine` implements `RaftStateMachine<RrTypeConfig>` (`crates/rr-raft/src/sm_adapter.rs`), bridging the user's `Service` / `StateMachine` traits to openraft's apply/snapshot/restore interface.
- **Network adapters**: `GrpcNetworkFactory` (production) and `TcpRaftNetworkFactory` (experimental) implement openraft's `RaftNetworkFactory`; snapshots ship over a gRPC streaming RPC.
- **Group management**: `RaftGroupManager` owns all `RaftNode`s and exposes group-level APIs (write, local read, linearizable read, membership change, snapshot trigger, ...).

## Version pinning policy

rr pins openraft to the **exact** version `= 0.10.0-alpha.34` (see the root `Cargo.toml`). The reason is straightforward: openraft is pre-1.0 alpha, and its API may shift between releases. An exact pin keeps rr builds reproducible and turns an openraft upgrade into a **deliberate, tested action** rather than a side effect of `cargo update`. When upgrading rr, check [reference/compatibility](../reference/compatibility.md) for the supported openraft range.

## Vendored patch ledger

rr's `vendor/openraft` (activated via `[patch.crates-io]`) carries 5 local patches. Patches #3/#4/#5 directly
uphold INV-1 (no loss of ACKed entries); any openraft upgrade must reconcile this ledger row by row.

| Patch | Commit | Problem | Upholds | Upstream status (as of 2026-11-04) |
| :--- | :--- | :--- | :--- | :--- |
| #1 snapshot-purge race panic | `9134d87` | a log index unwrapped by `run_save_committed_and_apply` can be purged first by a queued `PurgeLog` → `None` → RaftCore panic → quorum loss (reproduced on 2/3 nodes within ~2.5 min at 16k/s×1KiB) | process liveness | unfixed on upstream main at patch time; alpha.34 was the latest release then |
| #2 vote-term adoption | `9fa8347` (semantics pinned into upstream tests by `985b594`) | the log-recency reject branch of `handle_vote_req` returned early without adopting a strictly higher term → cluster terms never reconverge after overload churn (one root cause of the permanent perf-lab wedge) | election liveness | not re-checked against current upstream |
| #3 become-leader flush gating | `bd698d2` | `UpdateIOProgress` on become-leader was not gated on the real flush (W9 INV-1 root cause) | INV-1 | not re-checked against current upstream |
| #4/#5 committed-entry erasure guards | `0582499` | a short-log node winning an election made followers adopt its committed cursor → ~1.04M committed, client-ACKed entries physically erased (soak-nightly evidence; enabled by `LogIOId` (vote, log_id) lexicographic ordering together with #2) | INV-1 | not re-checked against current upstream |

**Checklist for upgrading openraft**:

1. Reconcile each row against the upstream changelog/commits; drop patches whose fix has landed upstream.
2. Port every remaining patch, then run the INV-1 suites (`just gate-crash` + `just soak-smoke` +
   the `io_flush_tracking` unit tests) plus a full `cargo test`.
3. Update the exact version pin in the root `Cargo.toml` and this ledger.

## When to use openraft directly

Dropping down to raw openraft is the better call when:

- **A single consensus group suffices** — no sharding need, so multi-group management is pure overhead;
- **You want a minimal dependency tree** — rr pulls in gRPC (tonic), DashMap, metrics, and more;
- **You want full control of storage and networking** — e.g. you already have your own storage engine or transport, and rr's adapter layers would constrain you;
- **You want to track the latest openraft API** — rr is pinned to a fixed alpha version.

Conversely, if you are building a **sharded, strongly-consistent replicated service that needs an operational toolchain**, rr trades some flexibility for a complete, integrated, tested stack. See the [FAQ](../faq.md) for more selection guidance.
