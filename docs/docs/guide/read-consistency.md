---
sidebar_position: 7
---

# 读一致性模型 / Read Consistency Model

Rr 将写、本地读和线性一致读定义为三个独立 gRPC 方法，而不是同一 RPC 的开关。
Rr defines writes, local reads, and linearizable reads as three separate gRPC methods rather than a flag on one RPC.

## 请求路径对比 / Request Paths

| 级别 / Level | 客户端 API / Client API | gRPC | 机制 / Mechanism | 保证与成本 / Guarantee and Cost |
| :--- | :--- | :--- | :--- | :--- |
| 写 / Write | `send_request`, `send_request_idempotent` | `Request` | leader 提交、quorum 复制、串行 apply / leader proposal, quorum replication, serial apply | 线性一致；一次复制往返 / Linearizable; one replication round trip |
| 本地读 / Local read | `read` | `ReadLocal` | 直接读取本地状态机 / Read the local state machine | 可能过期；无共识往返 / May be stale; no consensus round trip |
| 线性一致读 / Linearizable read | `read_linearizable` | `ReadLinearizable` | leader 执行 ReadIndex，等待 apply 后读取 / ReadIndex, wait for apply, then read | 线性一致；一次 quorum 确认 / Linearizable; one quorum confirmation |

## 写请求 / Writes

写只能由 leader 处理。服务端不转发 follower 收到的写，而是返回 `success=false` 和 `leader_info`；SDK 更新 leader 缓存并重试。
Only the leader handles writes. The server does not proxy a write received by a follower; it returns `success=false` with `leader_info`, and the SDK updates its leader cache and retries.

```rust
let response = client.send_request(group_id, payload).await?;

// 幂等写在重试时由框架去重 / Idempotent writes are deduplicated across retries.
let response = client.send_request_idempotent(group_id, payload, key).await?;
```

成功响应表示请求已提交并应用；超时或错误响应的结果可能不确定，需要按业务键查询或幂等重试。
A success response means the request was committed and applied. After a timeout or error, the outcome may be unknown; query by business key or retry idempotently.

## 本地读 / Local Reads

`client.read(group_id, payload)` 可发往任意副本，通过 `StateMachineReader::read` 读取本地状态，不经过 Raft。
`client.read(group_id, payload)` may contact any replica and reads local state through `StateMachineReader::read` without Raft consensus.

- follower 可以应答，读负载可分布到各副本。/ Followers can answer, distributing read load.
- follower 的 apply 可能落后，因此值可能过期。/ Follower apply may lag, so values may be stale.
- 该路径不需要 leader 重定向；重试会尝试其他引导地址。/ No leader redirect is needed; retries try other bootstrap addresses.

```rust
let value = client.read(group_id, payload).await?;
```

## 线性一致读 / Linearizable Reads

`client.read_linearizable(group_id, payload)` 在 leader 上通过 ReadIndex 确认领导权和提交位点，等待本地 apply 追平后读取。follower 返回 leader 提示，SDK 自动重试。
`client.read_linearizable(group_id, payload)` uses ReadIndex on the leader to confirm leadership and the commit point, waits for local apply, then reads. A follower returns a leader hint and the SDK retries.

```rust
let value = client.read_linearizable(group_id, payload).await?;
```

该路径满足线性一致读，但增加一次 quorum 确认和必要的 apply 等待。
This path provides linearizable reads at the cost of a quorum confirmation and any required apply wait.

## 为什么不用布尔开关 / Why Not a Boolean Flag

早期 `ClientRequest.read_only` 允许把写误标为只读，导致不修改状态却不报错。独立 RPC 在协议层阻止这种静默语义降级；ADR A-0009 记录了该决定。
The earlier `ClientRequest.read_only` flag could misclassify a write as a read, returning without an error or state change. Separate RPCs prevent that silent downgrade at the protocol layer; ADR A-0009 records the decision.

## 选择与错误处理 / Selection and Error Handling

可容忍过期时使用 `read`；读己之写或依赖最新状态做决策时使用 `read_linearizable`；状态变更只使用写 API。
Use `read` when staleness is acceptable, `read_linearizable` for read-your-writes or decisions based on current state, and write APIs for all mutations.

leader 重定向和带抖动的指数退避由 SDK 处理；应用应处理重试耗尽、确定性 gRPC 拒绝和幂等过期等错误。`GetClusterInfo` 可查看运行时拓扑，选举期间 `leader_node_id` 为 0，但 SDK 的写路由不依赖它。
The SDK handles leader redirects and jittered exponential backoff. Applications must still handle exhausted retries, deterministic gRPC rejections, and idempotency expiry. `GetClusterInfo` exposes runtime topology with `leader_node_id = 0` during elections, but SDK write routing does not depend on it.
