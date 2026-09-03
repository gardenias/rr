---
sidebar_position: 2
title: FAQ
---

# 常见问题 / Frequently Asked Questions

## 定位 / Positioning

### rr 是一个数据库吗？

不是。rr 是一个**框架/库**，用来构建强一致、分片、高可用的复制服务——它不是一个开箱即用的数据库。你把 rr 嵌入自己的二进制，实现 `Service` / `StateMachine` trait 来表达业务逻辑（参考 `examples/kv-store`、`examples/counter`）。stock `rr-server` 二进制自带的只是一个冒烟测试用的 `EchoStateMachine`。如果你需要的是 SQL、事务或二级索引，应该选择一个真正的数据库。

### 什么时候不该用 rr？

- 你需要 SQL、事务、二级索引——rr 是共识框架，不是数据库；
- 你需要 AP 语义，或者希望"加副本提升写吞吐"——Raft 是 CP，且**单个共识组的写入不会随副本数扩展**；multi-raft 的扩展方式是跨共识组**分片**，不是加副本；
- 一个 etcd / rqlite / consul 已经能满足你——它们是成品，直接用就好；
- 你需要跨地域低延迟写入——quorum 往返延迟是硬约束。

### rr 和 etcd / rqlite / consul 的区别是什么？

那些是**成品系统**，各自带有固定的数据模型（KV、SQL 等）。rr 面向的场景是"你在构建**自己的**复制服务"：你定义状态机、请求格式和业务语义，rr 提供共识、分片路由、成员编排、客户端重定向和运维工具链。如果你想要的是它们的数据模型，直接用它们；如果你要造自己的，rr 是对的层次。

### 我已经在用 openraft 了，rr 还能给我什么？

openraft 给你**一个**共识组的引擎，存储、网络、部署都要自己实现。rr 在此之上补齐了多分片生产系统需要的其余部分：多共识组生命周期管理（`RaftGroupManager`）、表格式分片路由、gRPC 网络层与连接池、客户端 SDK（leader 重定向/重试/缓存）、框架级幂等、Kafka 接入、安全门与 TLS/mTLS、rr-admin 运维工具。详见 [与 openraft 的关系](architecture/openraft-relationship.md)。

## 一致性 / Consistency

### multi-raft 会让写入更快吗？

不会——至少不是"加副本"意义上的更快。Raft 组的每次写入都需要 quorum 确认，**给单个组加副本只会增加写延迟**，不会提升吞吐。multi-raft 的扩展性来自**分片**：不同 partition 落在不同共识组上，写入按组并行，聚合吞吐随组数增长。单个 key 分片的写延迟始终由该组的 quorum 往返决定。

### `read` 和 `read_linearizable` 有什么区别？

`read`（ReadLocal）直接读本地状态机副本，绕过共识，**可能过期**，但延迟最低且可以由 follower 服务。`read_linearizable`（ReadLinearizable）走 openraft ReadIndex：leader 先向 quorum 确认自己仍是合法 leader，等待本地 apply 追平后再读，保证线性一致（Raft 论文 §6.4），代价是一次 quorum 往返。完整对比见 [读一致性模型](guide/read-consistency.md)。

### 为什么 follower 不直接转发写请求，而是返回 leader_info？

这是刻意设计：服务端不转发，返回 `ClientResponse{success=false, leader_info}`，由客户端 SDK 更新 leader 缓存并重定向。好处是热点路径简单（无服务端转发跳数），且客户端始终学到真实的 leader 地址，后续请求直达。手写 gRPC 客户端时注意：重定向是通过响应里的 `leader_info` 字段表达的，**不是 gRPC 错误状态**。

### 客户端怎么找到 leader？

靠**重定向**，不是靠服务发现。客户端从 bootstrap 地址中任选一个发送请求；若该节点不是 leader，响应里的 `leader_info` 会给出 leader 的 `NodeId` 和地址，SDK 把它写入无 TTL 的 `HashMap<GroupId, (NodeId, String)>` 缓存并重试。`GetClusterInfo` RPC 目前是**服务端占位实现**（`leader_node_id = 0`、空副本列表，见 `crates/rr-server/src/dispatch.rs:191`），不能用于 leader 发现。leader 宕机时缓存会失效：旧 leader 拒绝服务或不可达，SDK 清缓存后重新走 bootstrap 轮询。

## 运维 / Operations

### 为什么在 stock rr-server 上 `get` 会把我的 key 原样返回？

因为 stock 二进制内置的是 `EchoStateMachine`（`crates/rr-server/src/builtin_service.rs`）——一个把请求 payload 原样回显的冒烟测试服务，快照为空、restore 是 no-op。它存在的意义是验证集群管线是否打通，**不是真实业务**。要跑真实 KV 语义，请参考 `examples/kv-store` 自行构建二进制。

### 为什么 REPL 里 `put` 打印了 OK，值却没写进去？

REPL 的 OK 只表示**传输层成功**（RPC 完成），不等于写被提交。常见原因：请求落到了 follower 上——写只能由 leader 处理，follower 会返回 `success=false` 加 `leader_info`；或者你随后用 `read`（本地读）立刻验证，读到了尚未 apply 的副本。排查方法：用 `rr-admin` 的 `get-group-status` 看 `last_log_index` 是否增长，并用 `read_linearizable` 读回验证。

### 第一次启动为什么报 TLS 错误？

rr-server 启动时执行 **G1–G7 安全检查**（`crates/rr-server/src/node.rs` 的 `validate_security_invariants`）：RaftService 必须有 TLS 且强制 mTLS（它没有 token 通道）、AdminService 必须有 TLS + admin_token、配置了 bearer token 就必须有 TLS 等。任何违规都会拒绝启动并一次性列出全部问题。开发环境可以用 `--allow-insecure` / `RR_ALLOW_INSECURE=1` / `[node].allow_insecure=true` 显式跳过 G1–G6——但 **G7（TCP raft 传输必须绑定 loopback）在任何模式下都不可绕过**，因为 `RRAF` 帧协议是明文且无任何对端认证。生产部署见 [TLS 配置](deployment/tls.md)。

### 我应该部署几个节点？

**3 或 5 个**（每个共识组）。quorum 是多数派：3 节点容忍 1 个故障，5 节点容忍 2 个。**不要部署 2 节点**——2 的多数派仍是 2，任何一个节点故障都会失去 quorum，可用性反而低于单节点。偶数节点（4、6）增加了 quorum 规模却不增加容错能力，属于纯浪费。

### 如何安全地下线一个节点？

用 `DrainNode` / decommission 流程（rr-admin）。成员变更有安全护栏：`MIN_VOTERS_AFTER_REMOVAL = 2`（`crates/rr-core/src/membership.rs`）——任何会把 voter 集合缩到 2 以下的 `change_membership` 都会被拒绝；缩到 0（拆组）**永远不被允许**，即使加 `force`，拆组请走 `node-reset`。`force` 逃生舱存在，但调用会被审计记录（ADR A-0012）。

### rr 能跨地域部署吗？

可以运行，但要清楚代价：每次写都需要组内多数派的往返确认，**跨地域 RTT 直接成为写延迟下限**；线性一致读的 ReadIndex 同理。如果业务要求跨地域低延迟写入，Raft（以及任何强一致共识）都不是合适的工具——这正是"什么时候不该用 rr"里列的场景。

## 成熟度 / Maturity

### 我能在生产环境用 RocksDB 持久化吗？

目前**不能通过 stock 二进制获得**。`rocks` feature 提供 `RocksLogStorage`，但它只在集成测试中接线；stock `rr-server` 始终使用内存 `RrLogStorage`。此外快照也只存在于内存中，**重启恢复 = 日志重放**（在内存日志下意味着状态不跨重启保留）。RocksDB 接入二进制在路线图上，但如果你今天就要持久化，需要以库方式嵌入 rr 并自行接线 `start_group_rocks`——且要意识到这条路径的测试覆盖还不如内存路径充分。

### openraft 稳定吗？

openraft 目前处于 pre-1.0 阶段（rr 使用的是 `0.10.0-alpha.34`），其 API 在版本之间可能变动。rr 通过**精确版本锁定**（`= 0.10.0-alpha.34`）把这种不确定性挡在框架边界上：升级 openraft 是 rr 版本发布中的显式动作，而非用户侧的风险。详见 [与 openraft 的关系](architecture/openraft-relationship.md) 和 [兼容性承诺](reference/compatibility.md)。

### rr 的测试覆盖到什么程度？

当前是：单元测试 + 集成测试（`tests/integration`）+ 模糊测试目标（`fuzz/`）。**还没有** Jepsen 或混沌工程测试；`tools/rr-perf-bench` 基准工具存在，但没有发布过官方基线数据。工作区版本为 0.2.0，这意味着 API 与行为仍可能调整。把 rr 当作"核心路径经过测试、但尚未经过形式化一致性验证"的框架来评估是准确的。

### learner 会自动提升为 voter 吗？

组件存在（`crates/rr-raft/src/learner_promoter.rs`），但**尚未接入任何二进制或示例**。当前需要手动编排：`AddLearner` 添加 learner，确认其日志追平后，用 `ChangeMembership` 将其提升为 voter（受 `MIN_VOTERS_AFTER_REMOVAL` 护栏约束）。自动提升接线是路线图项。

---

# Frequently Asked Questions

## Positioning

### Is rr a database?

No. rr is a **framework/library** for building strongly-consistent, sharded, highly-available replicated services — it is not a turnkey database. You embed rr in your own binary and implement the `Service` / `StateMachine` traits to express your business logic (see `examples/kv-store`, `examples/counter`). The stock `rr-server` binary only ships an `EchoStateMachine` for smoke testing. If you need SQL, transactions, or secondary indexes, choose a real database.

### When should I NOT use rr?

- You need SQL, transactions, or secondary indexes — rr is a consensus framework, not a database;
- You need AP semantics, or you expect "more replicas = more write throughput" — Raft is CP, and **a single group's writes do not scale with replica count**; multi-raft scales by **sharding across groups**, not by adding replicas;
- A single etcd / rqlite / consul already covers you — they are finished products, just use them;
- You need low-latency writes across geographic regions — quorum round-trips are a hard floor.

### How does rr differ from etcd / rqlite / consul?

Those are **finished products**, each with its own fixed data model (KV, SQL, ...). rr targets the case where you are building **your own** replicated service: you define the state machine, request format, and business semantics, and rr supplies consensus, shard routing, membership orchestration, client redirect, and the operational toolchain. If their data model fits you, use them; if you are building your own, rr is the right layer.

### I already use openraft — what does rr add?

openraft gives you the engine for **one** consensus group; storage, networking, and deployment are yours to build. On top of it, rr adds everything else a sharded production system needs: multi-group lifecycle management (`RaftGroupManager`), table-based shard routing, a gRPC networking layer with connection pooling, a client SDK (leader redirect/retry/cache), framework-level idempotency, Kafka ingest, safety gates with TLS/mTLS, and the rr-admin ops tooling. See [Relationship with openraft](architecture/openraft-relationship.md).

## Consistency

### Does multi-raft make writes faster?

No — at least not in the "add replicas" sense. Every write to a Raft group needs quorum confirmation, so **adding replicas to a single group only increases write latency**, never throughput. Multi-raft scalability comes from **sharding**: different partitions land on different groups, writes proceed in parallel across groups, and aggregate throughput grows with the number of groups. Write latency for a single key's shard is always bounded by that group's quorum round-trip.

### What is the difference between `read` and `read_linearizable`?

`read` (ReadLocal) reads the local state-machine replica directly, bypassing consensus — **possibly stale**, but with the lowest latency and servable by followers. `read_linearizable` (ReadLinearizable) goes through openraft's ReadIndex: the leader confirms with a quorum that it is still the legitimate leader, waits for local apply to catch up, then reads — linearizable per §6.4 of the Raft paper, at the cost of one quorum round-trip. Full comparison in [Read Consistency Model](guide/read-consistency.md).

### Why does a follower return leader_info instead of forwarding my write?

That is deliberate: the server never forwards; it returns `ClientResponse{success=false, leader_info}`, and the client SDK updates its leader cache and redirects. This keeps the hot path simple (no server-side forwarding hops) and ensures the client always learns the real leader address, so subsequent requests go direct. If you hand-roll a gRPC client, note that redirects are expressed via the `leader_info` field in the response, **not** as gRPC error statuses.

### How do clients find the leader?

By **redirect**, not by service discovery. The client sends its request to any bootstrap address; if that node is not the leader, the response's `leader_info` carries the leader's `NodeId` and address, and the SDK stores it in a TTL-less `HashMap<GroupId, (NodeId, String)>` cache and retries. The `GetClusterInfo` RPC is currently a **server-side placeholder** (`leader_node_id = 0`, empty replica list — see `crates/rr-server/src/dispatch.rs:191`) and cannot be used for leader discovery. When a leader dies, the cache self-heals: the old leader refuses or is unreachable, the SDK evicts the entry, and falls back to bootstrap round-robin.

## Operations

### Why does `get` echo my key back on the stock rr-server?

Because the stock binary embeds `EchoStateMachine` (`crates/rr-server/src/builtin_service.rs`) — a smoke-test service that returns the request payload verbatim, with empty snapshots and a no-op restore. It exists to prove the cluster plumbing works, **not** to serve real workloads. For real KV semantics, build your own binary modeled on `examples/kv-store`.

### Why did `put` print OK in the REPL but the value isn't there?

The REPL's OK only means **transport success** (the RPC completed), not that the write was committed. Common causes: the request landed on a follower — only the leader serves writes, and followers answer `success=false` with `leader_info`; or you verified with `read` (a local read) before the replica applied the entry. To diagnose: check `last_log_index` growth via `rr-admin`'s `get-group-status`, and read back with `read_linearizable`.

### Why do I get TLS errors on first launch?

rr-server runs **G1–G7 startup safety gates** (`validate_security_invariants` in `crates/rr-server/src/node.rs`): RaftService requires TLS with mTLS (it has no token path), AdminService requires TLS + admin_token, bearer tokens require TLS, and so on. Any violation refuses startup, listing all problems at once. In development you can explicitly skip G1–G6 with `--allow-insecure` / `RR_ALLOW_INSECURE=1` / `[node].allow_insecure=true` — but **G7 (the TCP raft transport must bind to loopback) cannot be bypassed in any mode**, because the `RRAF` frame protocol is plaintext with no peer authentication. For production, see [TLS configuration](deployment/tls.md).

### How many nodes should I run?

**3 or 5** (per consensus group). A quorum is a majority: 3 nodes tolerate 1 failure, 5 tolerate 2. **Do not run 2 nodes** — a majority of 2 is still 2, so any single failure loses quorum, giving you worse availability than a single node. Even numbers (4, 6) enlarge the quorum without adding fault tolerance — pure waste.

### How do I safely decommission a node?

Use the `DrainNode` / decommission flow (rr-admin). Membership changes are guarded: `MIN_VOTERS_AFTER_REMOVAL = 2` (`crates/rr-core/src/membership.rs`) — any `change_membership` that would shrink the voter set below 2 is rejected; shrinking to 0 (group teardown) is **never allowed**, even with `force` — use `node-reset` for teardown. A `force` escape hatch exists, but invocations are audit-logged (ADR A-0012).

### Does rr work across regions?

It runs, but be clear about the cost: every write requires a round-trip confirmation from a majority of the group, so **cross-region RTT becomes the floor of write latency**; the same applies to ReadIndex for linearizable reads. If your business demands low-latency writes across regions, Raft (like any strong-consensus protocol) is the wrong tool — which is exactly why this case is listed under "when NOT to use rr".

## Maturity

### Can I use RocksDB persistence in production?

Not via the stock binary **today**. The `rocks` feature provides `RocksLogStorage`, but it is only wired in integration tests; the stock `rr-server` always uses the in-memory `RrLogStorage`. Snapshots are also in-memory only, and **restart recovery = log replay** (with in-memory logs, state does not survive restarts). Wiring RocksDB into the binary is on the roadmap; if you need persistence today, embed rr as a library and wire `start_group_rocks` yourself — aware that this path's test coverage is not yet as deep as the in-memory path.

### Is openraft stable?

openraft is pre-1.0 (rr uses `0.10.0-alpha.34`), and its API may shift between releases. rr contains that uncertainty at the framework boundary through an **exact version pin** (`= 0.10.0-alpha.34`): upgrading openraft is an explicit step in an rr release, not a risk on your side. See [Relationship with openraft](architecture/openraft-relationship.md) and the [compatibility commitments](reference/compatibility.md).

### How well tested is rr?

Currently: unit tests + integration tests (`tests/integration`) + fuzz targets (`fuzz/`). There is **no** Jepsen or chaos testing yet; the `tools/rr-perf-bench` benchmark tool exists but no official baselines have been published. The workspace is at version 0.2.0, which means APIs and behavior can still change. The accurate mental model is "core paths are tested, but the framework has not yet undergone formal consistency verification".

### Are learners automatically promoted to voters?

The component exists (`crates/rr-raft/src/learner_promoter.rs`) but is **not wired into any binary or example yet**. Today you orchestrate promotion manually: `AddLearner` to add a learner, confirm its log has caught up, then promote it to voter via `ChangeMembership` (subject to the `MIN_VOTERS_AFTER_REMOVAL` guard). Wiring up auto-promotion is a roadmap item.
