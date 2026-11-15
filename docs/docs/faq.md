---
sidebar_position: 2
title: 常见问题 / FAQ
---

# 常见问题 / Frequently Asked Questions

## 定位 / Positioning

### Rr 是数据库吗？ / Is Rr a database?

不是。Rr 是用于构建强一致、分片复制服务的框架和库，不是开箱即用的数据库。你需要在自己的二进制中实现
`Service` / `StateMachine` trait；`examples/kv-store` 和 `examples/counter` 展示了接入方式。标准
`rr-server` 只内置冒烟测试用的 `EchoStateMachine`。需要 SQL、事务或二级索引时，应选择数据库。

No. Rr is a framework and library for building strongly consistent, sharded replicated services, not a turnkey
database. Implement the `Service` / `StateMachine` traits in your own binary; see `examples/kv-store` and
`examples/counter`. The stock `rr-server` includes only an `EchoStateMachine` for smoke tests. Choose a database
if you need SQL, transactions, or secondary indexes.

### 什么时候不该用 Rr？ / When should I not use Rr?

- 你需要 SQL、事务、二级索引或现成的数据模型。<br />
  You need SQL, transactions, secondary indexes, or a ready-made data model.
- 你需要 AP 语义，或希望通过增加副本提升写吞吐；Raft 是 CP，写扩展依靠跨共识组分片。<br />
  You need AP semantics or expect replicas to scale writes; Raft is CP, and write scaling comes from sharding
  across consensus groups.
- etcd、rqlite 或 consul 已经满足需求。<br />
  etcd, rqlite, or consul already meets your needs.
- 你要求跨地域低延迟写入；多数派往返延迟无法消除。<br />
  You require low-latency cross-region writes; quorum round trips cannot be removed.

### Rr 与 etcd、rqlite、consul 有什么区别？ / How does Rr differ from etcd, rqlite, or consul?

它们是带有固定数据模型的成品系统。Rr 面向需要自己定义状态机、请求格式和业务语义的服务，提供共识、
分片路由、成员编排、客户端重定向和运维工具。现有系统的数据模型已经适合时，直接使用现有系统。

They are finished systems with fixed data models. Rr is for services that define their own state machines,
request formats, and business semantics; it supplies consensus, shard routing, membership orchestration, client
redirection, and operational tooling. If an existing system's data model fits, use that system.

### 已经使用 openraft，Rr 还能提供什么？ / What does Rr add if I already use openraft?

openraft 提供单个共识组的引擎，存储、网络和部署仍由应用实现。Rr 在其上提供多共识组生命周期管理
（`RaftGroupManager`）、表格式分片路由、gRPC 网络与连接池、客户端 SDK、框架级幂等、Kafka 接入、
TLS/mTLS 安全门和 `rr-admin`。详见 [与 openraft 的关系](architecture/openraft-relationship.md)。

openraft provides the engine for one consensus group, while the application implements storage, networking, and
deployment. Rr adds multi-group lifecycle management (`RaftGroupManager`), table-based shard routing, gRPC
networking and connection pooling, a client SDK, framework-level idempotency, Kafka ingestion, TLS/mTLS safety
gates, and `rr-admin`. See [Relationship with openraft](architecture/openraft-relationship.md).

## 一致性与扩展 / Consistency and scaling

### multi-raft 会让写入更快吗？ / Does multi-raft make writes faster?

增加单个 Raft 组的副本不会提升写吞吐，通常还会增加多数派确认延迟。multi-raft 通过把不同 partition
分配到不同组来并行写入；单个 key 的写延迟仍由所属组的多数派往返决定。在固定宿主机上增加组数也不会
提升聚合吞吐，跨机扩展需要各组的副本集合不相交。实测条件见 [容量规划](deployment/capacity-planning.md)。

Adding replicas to one Raft group does not increase write throughput and usually adds quorum latency. Multi-raft
parallelizes writes by assigning different partitions to different groups; a key's write latency still depends on
its group's quorum round trip. Adding groups on a fixed host does not increase aggregate throughput either;
scaling across hosts requires disjoint replica sets. See [Capacity Planning](deployment/capacity-planning.md) for
measurements.

### `read` 和 `read_linearizable` 有什么区别？ / How do `read` and `read_linearizable` differ?

`read`（ReadLocal）直接读取本地状态机，绕过共识，可能返回旧数据，但延迟最低，follower 也能服务。
`read_linearizable`（ReadLinearizable）使用 openraft ReadIndex：leader 先向多数派确认自身地位，等待本地
apply 追平后再读，因此提供线性一致读，代价是多数派往返。详见 [读一致性模型](guide/read-consistency.md)。

`read` (ReadLocal) reads the local state machine without consensus. It may return stale data, but has the lowest
latency and can be served by followers. `read_linearizable` (ReadLinearizable) uses openraft ReadIndex: the leader
confirms its leadership with a quorum, waits for local apply to catch up, and then reads. This is linearizable at
the cost of a quorum round trip. See [Read Consistency](guide/read-consistency.md).

### follower 为什么不转发写请求？ / Why do followers not forward writes?

服务端返回 `ClientResponse{success=false, leader_info}`，由客户端 SDK 更新 leader 缓存并重试。这样省去
服务端转发跳数，也让客户端后续直连 leader。自行实现 gRPC 客户端时，应读取响应中的 `leader_info`，
不要等待 gRPC 错误状态来表示重定向。

The server returns `ClientResponse{success=false, leader_info}`, and the client SDK updates its leader cache and
retries. This avoids a server-side forwarding hop and lets later requests go directly to the leader. A custom gRPC
client must read `leader_info` in the response; redirection is not reported as a gRPC error status.

### 客户端如何找到 leader？ / How do clients find the leader?

客户端先向任一 bootstrap 地址发送请求，再根据响应中的 `leader_info` 缓存 leader 地址并重试。旧 leader
失效后，SDK 清除缓存并重新遍历 bootstrap 地址。`GetClusterInfo` 可以返回运行时拓扑，但 SDK 的 leader
发现依赖重定向，不依赖该 RPC。

The client sends a request to any bootstrap address, then caches and retries against the leader reported in
`leader_info`. When the old leader fails, the SDK evicts the cache entry and returns to the bootstrap addresses.
`GetClusterInfo` can report runtime topology, but SDK leader discovery uses redirection rather than this RPC.

## 运维 / Operations

### 为什么标准 rr-server 的 `get` 会回显 key？ / Why does `get` echo the key on stock rr-server?

标准二进制的 `EchoStateMachine` 会原样返回请求 payload，快照为空，restore 为 no-op。它只用于验证集群
管线，不提供 KV 语义。真实 KV 示例见 `examples/kv-store`，实现位于
`crates/rr-server/src/builtin_service.rs`。

The stock binary's `EchoStateMachine` returns the request payload verbatim, with empty snapshots and a no-op
restore. It only verifies cluster plumbing and does not provide KV semantics. See `examples/kv-store` for a real
KV example and `crates/rr-server/src/builtin_service.rs` for the echo implementation.

### 为什么 REPL 的 `put` 打印 OK，值却读不到？ / Why did REPL `put` print OK while the value is missing?

当前 REPL 只在服务端返回 `ClientResponse.success=true` 时打印 OK，并会跟随 `leader_info` 重定向；所有
尝试失败时会打印错误。因此当前实现不应出现“失败后仍打印 OK”。更常见的原因是写入后立刻在尚未 apply
的副本上使用可能过期的 `read`。可用 `rr-admin` 的 `group status` 检查 `last_log_index`，再用
`read_linearizable` 读回。

The current REPL prints OK only for `ClientResponse.success=true` and follows `leader_info` redirects; failed
attempts produce an error. A failed write should therefore not print OK. A more likely cause is an immediate,
potentially stale `read` from a replica that has not applied the write. Check `last_log_index` with `rr-admin`
`group status`, then read back with `read_linearizable`.

### 第一次启动为什么报 TLS 错误？ / Why does the first launch report TLS errors?

`rr-server` 启动时执行 G1–G7 安全检查：RaftService 需要 TLS 和 mTLS，AdminService 需要 TLS 和
`admin_token`，bearer token 也必须配合 TLS。开发环境可用 `--allow-insecure`、
`RR_ALLOW_INSECURE=1` 或 `[node].allow_insecure=true` 跳过 G1–G6；G7 不可绕过，实验性的明文 TCP
Raft 传输必须绑定 loopback。生产配置见 [TLS 配置](deployment/tls.md)。

`rr-server` runs G1–G7 startup checks: RaftService requires TLS and mTLS, AdminService requires TLS and
`admin_token`, and bearer tokens require TLS. For development, `--allow-insecure`, `RR_ALLOW_INSECURE=1`, or
`[node].allow_insecure=true` can skip G1–G6. G7 cannot be bypassed: the experimental plaintext TCP Raft transport
must bind to loopback. See [TLS Configuration](deployment/tls.md) for production setup.

### 应该部署几个节点？ / How many nodes should I run?

每个共识组通常部署 **3 或 5 个节点**：3 节点容忍 1 个故障，5 节点容忍 2 个。不要部署 2 节点，因为
多数派仍是 2，任一故障都会失去 quorum。4、6 等偶数会增加多数派规模，却不比前一个奇数规模多容忍
一次故障。

Run **3 or 5 nodes per consensus group**: three tolerate one failure, and five tolerate two. Do not run two nodes;
their quorum is still two, so either failure removes availability. Even sizes such as four or six increase the
quorum without tolerating one more failure than the preceding odd size.

### 如何安全下线节点？ / How do I safely decommission a node?

使用 `rr-admin` 的 `DrainNode` / decommission 流程。默认情况下，会使 voter 少于
`MIN_VOTERS_AFTER_REMOVAL = 2` 的 `change_membership` 会被拒绝；`force` 可以绕过这一限制，但会记录
审计日志。缩到 0 属于拆组，即使使用 `force` 也不允许，应改用 `node-reset`。实现见
`crates/rr-core/src/membership.rs`。

Use the `rr-admin` `DrainNode` / decommission flow. By default, `change_membership` rejects a target with fewer
than `MIN_VOTERS_AFTER_REMOVAL = 2` voters. `force` can override this guard and is audit-logged. Shrinking to
zero tears down the group and is never allowed, even with `force`; use `node-reset` instead. See
`crates/rr-core/src/membership.rs`.

### Rr 能跨地域部署吗？ / Can Rr run across regions?

可以，但每次写入和线性一致读都需要多数派往返，跨地域 RTT 会直接成为延迟下限。如果业务要求跨地域
低延迟写入，强一致共识并不合适。

Yes, but every write and linearizable read requires a quorum round trip, so cross-region RTT becomes a latency
floor. Strongly consistent consensus is not a fit when the business requires low-latency writes across regions.

## 成熟度 / Maturity

### RocksDB 持久化能用于生产吗？ / Can RocksDB persistence be used in production?

可以使用，但应针对业务和部署自行验收。以 `rocks` feature 构建后，`rr-server --storage rocks` 会持久化
Raft 日志、状态机快照和成员 sidecar；快照写入采用原子替换，失败时会中止 snapshot build/install，损坏的
快照会导致启动失败。默认 `mem` 后端仍是全内存，重启后不保留数据。RocksDB 路径有单元、集成和滚动重启
测试，但覆盖深度仍不及 mem 路径。详见 [存储引擎指南](guide/storage-engines.md)。

It is available, but validate it against your own workload and deployment. When built with the `rocks` feature,
`rr-server --storage rocks` persists Raft logs, state-machine snapshots, and the membership sidecar. Snapshots use
atomic replacement; write failures abort snapshot build/install, and a corrupt snapshot fails startup. The default
`mem` backend remains in-memory and does not survive restarts. The RocksDB path has unit, integration, and
rolling-restart tests, but less coverage depth than the mem path. See the [Storage Engine Guide](guide/storage-engines.md).

### openraft 稳定吗？ / Is openraft stable?

openraft 仍处于 pre-1.0 阶段，API 可能变化。Rr 将依赖精确锁定为 `= 0.10.0-alpha.34`，openraft 升级由
Rr 版本显式完成；这不代表 Rr 自身 API 已稳定。详见 [与 openraft 的关系](architecture/openraft-relationship.md)
和 [兼容性说明](reference/compatibility.md)。

openraft is pre-1.0 and its API may change. Rr pins it exactly to `= 0.10.0-alpha.34`, with upgrades handled
explicitly in Rr releases; this does not make Rr's own API stable. See [Relationship with openraft](architecture/openraft-relationship.md)
and the [compatibility policy](reference/compatibility.md).

### Rr 的测试覆盖到什么程度？ / How well tested is Rr?

仓库包含单元测试、`tests/integration` 集成测试（默认测试任务含 4 个混沌/故障注入测试）、
`tests/crash` 进程级 SIGKILL/SIGTERM 测试、`fuzz/` 模糊测试与种子语料、proptest 属性测试，以及
`tests/integration/src/linearizability.rs` 的 porcupine 风格线性一致检查。`scripts/soak.sh` 支持数小时
稳定性运行。`formal/tla/` 有 4 份 TLA+ 规约，仓库记录的小模型 TLC 检查均为零反例。测试覆盖不代表
所有部署场景都已验证。

The repository includes unit tests, integration tests in `tests/integration` (including four chaos/fault-injection
tests in the default test job), process-level SIGKILL/SIGTERM tests in `tests/crash`, fuzz targets and seed corpora
in `fuzz/`, proptest property tests, and a porcupine-style linearizability checker in
`tests/integration/src/linearizability.rs`. `scripts/soak.sh` supports hours-long stability runs. `formal/tla/`
contains four TLA+ specifications, with zero counterexamples recorded for small-model TLC checks. Test coverage
does not validate every deployment scenario.

### learner 会自动提升为 voter 吗？ / Are learners automatically promoted to voters?

取决于启动模式。legacy 模式下 `NodeConfig.auto_promote_learners` 默认为 `true`，`RrNode` 会为本节点托管的组
启动 learner promoter；只有持续追平且通过成员变更安全检查的 learner 才会提升为 voter。可通过 TOML
`cluster.auto_promote_learners` 或 `NodeConfig` 控制 legacy 行为；关闭后可手动执行 `AddLearner` 和
`ChangeMembership`。

managed formation P1 不自动提升 learner：`RrNode::new_with_formation` 会强制关闭该开关，由
convergence / adoption 流程显式处理。

In legacy mode, `NodeConfig.auto_promote_learners` defaults to `true`, and `RrNode` starts a promoter for each
hosted group. Only learners that remain caught up and pass membership safety checks become voters. Control legacy
behavior through TOML `cluster.auto_promote_learners` or `NodeConfig`; when disabled, use `AddLearner` and
`ChangeMembership` manually.

Managed formation P1 does not automatically promote learners: `RrNode::new_with_formation` forces the flag off,
and the convergence / adoption flow handles them explicitly.
