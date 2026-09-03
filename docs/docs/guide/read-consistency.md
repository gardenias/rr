---
sidebar_position: 7
---

# 读一致性模型 / Read Consistency Model

rr 提供三个明确区分的请求级别：**写（始终线性一致）**、**本地读（可能过期）**、**线性一致读（ReadIndex）**。它们是不同的 gRPC 方法，而不是同一个 RPC 上的开关——这是刻意的安全设计（见下文 ADR A-0009）。

## 三个级别对比

| 级别 | 客户端 API | gRPC 方法 | 机制 | 一致性保证 | 延迟成本 | 典型用途 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **写** | `send_request` / `send_request_idempotent` | `Request` | 提交到 leader → Raft 复制 → 法定人数提交 → 串行 apply | 线性一致 | 一次 quorum 复制往返 | 所有状态变更 |
| **本地读** | `read` | `ReadLocal` | 直接读本节点状态机副本（`StateMachineReader`），绕过共识 | 可能**过期**（stale） | 最低，本地读取 | 延迟不敏感的读、缓存、监控展示 |
| **线性一致读** | `read_linearizable` | `ReadLinearizable` | leader 执行 ReadIndex 确认提交位点，等待本地 apply 追平后读取 | 线性一致（Raft 论文 §6.4） | 一次 quorum 往返（心跳确认） | 读己之写、基于最新值做决策 |

## 写：始终走 leader

写请求只能由共识组的 leader 处理。如果请求落到 follower 上，服务端**不做转发**，而是返回 `ClientResponse{success=false, leader_info}`，由 `rr-client` SDK 自动更新 leader 缓存并重试到 leader 地址。服务端不转发是刻意的：它让热点路径保持简单，也让客户端始终知道真正的 leader 是谁。

```rust
// 写：自动 leader 重定向 + 指数退避重试
let resp = client.send_request(group_id, payload).await?;

// 幂等写：携带 IdempotencyKey，框架层去重
let resp = client.send_request_idempotent(group_id, payload, key).await?;
```

## 本地读（ReadLocal）：最快，可能过期

`client.read(group_id, payload)`（`crates/rr-client/src/lib.rs:330`）把请求发给任意可达节点，该节点通过 `StateMachineReader::read`（`crates/rr-core/src/state_machine.rs:38`）直接读取**本地状态机副本**，完全绕过 Raft 共识。

- **可以在 follower 上服务**——这正是它的价值：读负载可以摊到所有副本上；
- **可能读到旧值**：follower 的 apply 落后于 leader 的 commit，落后的窗口取决于复制延迟；
- **没有 leader 重定向**：SDK 在 bootstrap 地址间轮询重试，因为它不需要找到 leader。

```rust
// 本地读：最低延迟，允许轻微过期
let value = client.read(group_id, payload).await?;
```

## 线性一致读（ReadLinearizable）：不过期，更贵

`client.read_linearizable(group_id, payload)`（`crates/rr-client/src/lib.rs:391`）的服务端流程（`crates/rr-raft/src/group_manager.rs:857` 的 `linearizable_read`）：

1. 被联系的副本调用 openraft 的 `get_read_linearizer(ReadPolicy::ReadIndex)`；
2. **如果它是 leader**：ReadIndex 向 quorum 确认自己的 commit 位点仍然有效（确认自己仍是合法 leader），然后 `await_ready` 等待本地状态机 apply 到该位点，最后执行本地读返回；
3. **如果它是 follower**：openraft 返回 `ForwardToLeader`；rr 服务端把它压平成 `ReadLocalResponse.leader_info` 重定向（与写路径相同），SDK 自动改拨 leader 地址重试。

这满足 Raft 论文 §6.4 的线性一致读语义，代价是一次 quorum 往返。

```rust
// 线性一致读：保证读到最新已提交状态
let value = client.read_linearizable(group_id, payload).await?;
```

## 为什么是独立的 RPC，而不是一个布尔开关（ADR A-0009)

早期设计用 `ClientRequest.read_only: bool` 区分读写。问题是：调用方一旦把写请求错标成 `read_only=true`，服务端会把它路由到本地读路径——**不报错、不修改状态、数据静默丢失**。这比返回错误危险得多。

因此 ADR A-0009 决定：**写、本地读、线性一致读是三个独立的 gRPC 方法**，由协议层强制语义分离。一致性级别不可能因为一个布尔参数被静默降级；直接手写 gRPC 的客户端也无法犯这个错误。

## 实践建议

- **默认用 `read`**：只要业务能容忍轻微过期（缓存、列表展示、监控），本地读的延迟和负载分布最优；
- **必须不过期时用 `read_linearizable`**：读己之写（写后立刻读）、基于最新状态做决策（如抢锁前检查）、结算类逻辑；
- **写永远走 `send_request` / `send_request_idempotent`**——不要试图用读路径"确认"写结果，需要确认就用线性一致读；
- **重试是 SDK 内置的**：leader 重定向和指数退避（`retry_delay_ms * 2^attempt`）由 `rr-client` 处理，业务层通常只需配置 `RetryConfig`；
- 注意 `GetClusterInfo` 目前返回的是**占位数据**（`leader_node_id = 0`、空副本列表），leader 发现的实际机制是上面的 `leader_info` 重定向，不要依赖 `GetClusterInfo` 做路由。

---

# Read Consistency Model

rr offers three explicitly separated request levels: **writes (always linearizable)**, **local reads (possibly stale)**, and **linearizable reads (ReadIndex)**. These are distinct gRPC methods, not a flag on a single RPC — a deliberate safety design (see ADR A-0009 below).

## The three levels at a glance

| Level | Client API | gRPC method | Mechanism | Guarantee | Latency cost | Typical use |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Write** | `send_request` / `send_request_idempotent` | `Request` | Submit to leader → Raft replication → quorum commit → serial apply | Linearizable | One quorum replication round-trip | All state mutations |
| **Local read** | `read` | `ReadLocal` | Reads the node's local state-machine replica (`StateMachineReader`), bypassing consensus | May be **stale** | Lowest — local read | Latency-insensitive reads, caches, dashboards |
| **Linearizable read** | `read_linearizable` | `ReadLinearizable` | Leader runs ReadIndex to confirm its commit point, waits for local apply to catch up, then reads | Linearizable (Raft paper §6.4) | One quorum round-trip (heartbeat confirmation) | Read-your-writes, decisions on latest state |

## Writes: always to the leader

Only the group's leader can serve writes. If a write lands on a follower, the server **does not forward it** — it returns `ClientResponse{success=false, leader_info}`, and the `rr-client` SDK updates its leader cache and retries against the leader address. The no-forwarding rule is deliberate: it keeps the hot path simple and ensures the client always learns who the real leader is.

```rust
// Write: automatic leader redirect + exponential-backoff retry
let resp = client.send_request(group_id, payload).await?;

// Idempotent write: carries an IdempotencyKey, deduplicated by the framework
let resp = client.send_request_idempotent(group_id, payload, key).await?;
```

## Local read (ReadLocal): fastest, possibly stale

`client.read(group_id, payload)` (`crates/rr-client/src/lib.rs:330`) sends the request to any reachable node, which reads its **local state-machine replica** directly via `StateMachineReader::read` (`crates/rr-core/src/state_machine.rs:38`), completely bypassing Raft consensus.

- **Followers can serve it** — that is the point: read load spreads across all replicas;
- **Values may be stale**: a follower's apply lags the leader's commit; the staleness window is bounded by replication lag;
- **No leader redirect**: the SDK round-robins bootstrap addresses on retry, because it does not need the leader.

```rust
// Local read: lowest latency, tolerates slight staleness
let value = client.read(group_id, payload).await?;
```

## Linearizable read (ReadLinearizable): never stale, more expensive

The server-side flow of `client.read_linearizable(group_id, payload)` (`crates/rr-client/src/lib.rs:391`) is `linearizable_read` at `crates/rr-raft/src/group_manager.rs:857`:

1. The contacted replica calls openraft's `get_read_linearizer(ReadPolicy::ReadIndex)`;
2. **If it is the leader**: ReadIndex confirms with a quorum that its commit point is still valid (i.e. it is still the legitimate leader), then `await_ready` waits until the local state machine has applied through that point, and finally serves the read locally;
3. **If it is a follower**: openraft returns `ForwardToLeader`; the rr server flattens this into a `ReadLocalResponse.leader_info` redirect (same shape as the write path), and the SDK automatically dials the leader and retries.

This satisfies the linearizable-read semantics of §6.4 of the Raft paper, at the cost of one quorum round-trip.

```rust
// Linearizable read: guaranteed to see the latest committed state
let value = client.read_linearizable(group_id, payload).await?;
```

## Why separate RPCs instead of a boolean flag (ADR A-0009)

An early design distinguished reads from writes with `ClientRequest.read_only: bool`. The problem: if a caller mislabeled a write as `read_only=true`, the server would route it to the local-read path — **no error, no state change, silent data loss**. That is far more dangerous than returning an error.

ADR A-0009 therefore decided: **write, local read, and linearizable read are three independent gRPC methods**, with semantics enforced at the protocol layer. The consistency level can never be silently downgraded by a boolean parameter, and even hand-rolled gRPC clients cannot make that mistake.

## Practical guidance

- **Default to `read`**: whenever the business can tolerate slight staleness (caches, listings, monitoring), local reads have the best latency and load distribution;
- **Use `read_linearizable` when staleness is unacceptable**: read-your-writes (reading right after writing), decisions based on the latest state (e.g. checking before acquiring a lock), settlement-style logic;
- **Always write via `send_request` / `send_request_idempotent`** — don't try to "confirm" a write through the read path; if you need confirmation, use a linearizable read;
- **Retries are built into the SDK**: leader redirects and exponential backoff (`retry_delay_ms * 2^attempt`) are handled by `rr-client`; the business layer usually only tunes `RetryConfig`;
- Note that `GetClusterInfo` currently returns **placeholder data** (`leader_node_id = 0`, empty replica list). The real leader-discovery mechanism is the `leader_info` redirect described above — do not rely on `GetClusterInfo` for routing.
