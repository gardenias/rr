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

靠**重定向**，不是靠服务发现。客户端从 bootstrap 地址中任选一个发送请求；若该节点不是 leader，响应里的 `leader_info` 会给出 leader 的 `NodeId` 和地址，SDK 把它写入无 TTL 的 `HashMap<GroupId, (NodeId, String)>` 缓存并重试。`GetClusterInfo` RPC 返回真实的 raft 运行时拓扑（leader 与副本来自 `metrics`，选举中 leader 显示为 0；见 `crates/rr-server/src/dispatch.rs`），但 SDK 的 leader 发现仍走重定向缓存而非它。leader 宕机时缓存会失效：旧 leader 拒绝服务或不可达，SDK 清缓存后重新走 bootstrap 轮询。

## 运维 / Operations

### 为什么在 stock rr-server 上 `get` 会把我的 key 原样返回？

因为 stock 二进制内置的是 `EchoStateMachine`（`crates/rr-server/src/builtin_service.rs`）——一个把请求 payload 原样回显的冒烟测试服务，快照为空、restore 是 no-op。它存在的意义是验证集群管线是否打通，**不是真实业务**。要跑真实 KV 语义，请参考 `examples/kv-store` 自行构建二进制。

### 为什么 REPL 里 `put` 打印了 OK，值却没写进去？

现行 REPL 只在服务端返回 `ClientResponse.success=true` 时才打印 OK（`crates/rr-admin/src/main.rs` 的 `kv_call` / `kv_failure_detail`）：写请求落到 follower 时，REPL 会跟随 `leader_info` 重定向，并在当前节点断连或没有 leader 提示时尝试启动时发现的组成员；全部尝试后仍未成功则打印 `Error: request rejected ...`（不会打印 OK）。因此"OK 但值没写进去"在当前版本理论上不应出现；如果你写完立刻读不到值，更可能是随后用 `read`（本地读）在尚未 apply 的副本上验证。排查方法：用 `rr-admin` 的 `group status` 看 `last_log_index` 是否增长，并用 `read_linearizable` 读回验证。

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

可以。stock `rr-server --storage rocks`（需以 `rocks` feature 构建）持久化全部关键状态：Raft 日志落在 `{data_dir}/raft-log/{group_id}`（ADR A-0016），状态机快照与成员 sidecar 以原子写（write → fsync → rename）落在同目录的 `sm-snapshot.bin` / `sm-membership.bin`（T1/R-P0）。快照持久化是 **fail-closed** 的：写不进盘的快照会中止 build/install，OpenRaft 永远不会 purge 掉没有持久快照兜底的日志前缀；存在但损坏的快照文件会让节点启动时响亮失败，而不是以空状态机悄悄重启。默认 `mem` 后端仍是全内存、不跨重启保留。rocks 路径有单元测试、rocks-gated 集成测试与滚动重启验收测试覆盖，但测试深度仍不及 mem 路径，生产采用前建议按 [存储引擎指南](guide/storage-engines.md) 自行验证。

### openraft 稳定吗？

openraft 目前处于 pre-1.0 阶段（rr 使用的是 `0.10.0-alpha.34`），其 API 在版本之间可能变动。rr 通过**精确版本锁定**（`= 0.10.0-alpha.34`）把这种不确定性挡在框架边界上：升级 openraft 是 rr 版本发布中的显式动作，而非用户侧的风险。详见 [与 openraft 的关系](architecture/openraft-relationship.md) 和 [兼容性承诺](reference/compatibility.md)。

### rr 的测试覆盖到什么程度？

单元测试 + 集成测试（`tests/integration`，含 4 个在默认测试任务中运行的混沌/故障注入测试）+ 进程级 crash harness（`tests/crash`，真 SIGKILL/SIGTERM 三节点集群 + ack-log oracle）+ 模糊测试目标（`fuzz/`，含提交入仓的种子语料）+ proptest 属性测试 + porcupine 风格线性一致检查器（`tests/integration/src/linearizability.rs`）。另有 `scripts/soak.sh` 数小时级稳定性 harness 与已发布的官方性能基线（`docs/perf/baselines/v0.2.0-baseline.md`）。形式化方面，`formal/tla/` 携带 4 份 TLA+ 规约（MembershipGuard / OffsetExactlyOnce / OutputWatermark / RoutingTableSwitch），TLC 小模型实测零反例。工作区版本为 0.2.0，API 与行为仍可能调整。

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

By **redirect**, not by service discovery. The client sends its request to any bootstrap address; if that node is not the leader, the response's `leader_info` carries the leader's `NodeId` and address, and the SDK stores it in a TTL-less `HashMap<GroupId, (NodeId, String)>` cache and retries. The `GetClusterInfo` RPC returns real raft-runtime topology (leader and replicas from `metrics`; the leader reads 0 mid-election — see `crates/rr-server/src/dispatch.rs`), but the SDK still discovers leaders via the redirect cache rather than this RPC. When a leader dies, the cache self-heals: the old leader refuses or is unreachable, the SDK evicts the entry, and falls back to bootstrap round-robin.

## Operations

### Why does `get` echo my key back on the stock rr-server?

Because the stock binary embeds `EchoStateMachine` (`crates/rr-server/src/builtin_service.rs`) — a smoke-test service that returns the request payload verbatim, with empty snapshots and a no-op restore. It exists to prove the cluster plumbing works, **not** to serve real workloads. For real KV semantics, build your own binary modeled on `examples/kv-store`.

### Why did `put` print OK in the REPL but the value isn't there?

The current REPL only prints OK when the server answers `ClientResponse.success=true` (see `kv_call` / `kv_failure_detail` in `crates/rr-admin/src/main.rs`): when a write lands on a follower, the REPL follows `leader_info` redirects and tries the members discovered at startup after disconnects or missing leader hints; if every attempt fails it prints `Error: request rejected ...` — never OK. So "OK but the value was not written" should not occur in the current version; if a value is missing right after a write, the more likely cause is verifying with `read` (a local read) on a replica that has not applied the entry yet. To diagnose: check `last_log_index` growth via `rr-admin`'s `group status`, and read back with `read_linearizable`.

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

Yes. The stock `rr-server --storage rocks` (built with the `rocks` feature) persists all critical state: the Raft log lives at `{data_dir}/raft-log/{group_id}` (ADR A-0016), and the state-machine snapshot plus the membership sidecar are written atomically (write → fsync → rename) to `sm-snapshot.bin` / `sm-membership.bin` in the same directory (T1/R-P0). Snapshot persistence is **fail-closed**: a snapshot that cannot be written aborts the build/install, so OpenRaft never purges a log prefix that has no durable snapshot behind it; a present-but-corrupt snapshot file fails startup loudly instead of restarting empty. The default `mem` backend remains fully in-memory and does not survive restarts. The rocks path is covered by unit tests, rocks-gated integration tests, and a rolling-restart acceptance test, but its test depth still trails the mem path — validate against the [storage engine guide](guide/storage-engines.md) before production adoption.

### Is openraft stable?

openraft is pre-1.0 (rr uses `0.10.0-alpha.34`), and its API may shift between releases. rr contains that uncertainty at the framework boundary through an **exact version pin** (`= 0.10.0-alpha.34`): upgrading openraft is an explicit step in an rr release, not a risk on your side. See [Relationship with openraft](architecture/openraft-relationship.md) and the [compatibility commitments](reference/compatibility.md).

### How well tested is rr?

Unit tests + integration tests (`tests/integration`, including four chaos/fault-injection tests running in the default test job) + a process-level crash harness (`tests/crash`: real SIGKILL/SIGTERM on a 3-node cluster with an ack-log oracle) + fuzz targets (`fuzz/`, with a committed seed corpus) + proptest property tests + a porcupine-style linearizability checker (`tests/integration/src/linearizability.rs`). There is also `scripts/soak.sh` for hours-long stability runs and a published official performance baseline (`docs/perf/baselines/v0.2.0-baseline.md`). On the formal side, `formal/tla/` carries four TLA+ specifications (MembershipGuard / OffsetExactlyOnce / OutputWatermark / RoutingTableSwitch), all model-checked with TLC at small scale with zero counterexamples. The workspace is at version 0.2.0, so APIs and behavior can still change.

### Are learners automatically promoted to voters?

The component exists (`crates/rr-raft/src/learner_promoter.rs`) but is **not wired into any binary or example yet**. Today you orchestrate promotion manually: `AddLearner` to add a learner, confirm its log has caught up, then promote it to voter via `ChangeMembership` (subject to the `MIN_VOTERS_AFTER_REMOVAL` guard). Wiring up auto-promotion is a roadmap item.
