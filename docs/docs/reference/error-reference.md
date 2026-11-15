---
sidebar_position: 4
---

# 错误参考 / Error Reference

框架公共操作通常返回 `Result<T, RrError>`。`RrError` 在 `rr-core` 中通过 `thiserror` 定义，
按组件包装错误；各错误枚举均为 `#[non_exhaustive]`，匹配时应保留通配分支。
Framework operations generally return `Result<T, RrError>`. Defined in `rr-core` with `thiserror`,
`RrError` wraps component-specific errors. The error enums are `#[non_exhaustive]`; include a wildcard
arm when matching them.

## `RrError` 变体 / `RrError` Variants

| 变体 / Variant | 含义 / Meaning |
| :--- | :--- |
| `Raft(RaftError)` | Raft 共识错误 / Raft consensus errors |
| `Storage(StorageError)` | 存储 I/O、损坏或缺失 / Storage I/O, corruption, or missing keys |
| `Config(ConfigError)` | 配置提供者错误 / Config-provider errors |
| `Lifecycle(LifecycleError)` | 集群 formation 与生命周期协调错误 / Formation and lifecycle-coordination errors |
| `Router(RouterError)` | 路由与分区查找错误 / Routing and partition-lookup errors |
| `Network(NetworkError)` | 连接、RPC 与超时错误 / Connection, RPC, and timeout errors |
| `Codec(CodecError)` | 序列化与反序列化错误 / Serialization and deserialization errors |
| `Application(String)` | 应用或状态机错误 / Application or state-machine errors |
| `IdempotencyExpired { .. }` | 幂等记录已过期并逐出 / Idempotency record expired and evicted |
| `IdempotencyNamespaceLimitExceeded { .. }` | 新 `request_type` 超出组级命名空间上限 / New `request_type` exceeds the per-group namespace cap |

## 领域错误 / Domain Errors

### `RaftError`

| 变体 / Variant | 场景与处理 / When and how to handle |
| :--- | :--- |
| `NotLeader { leader, leader_addr }` | 当前节点不是 Leader；按已知 ID/地址重定向，未知时重试发现 Leader。 / Not leader; redirect to the known ID/address or retry leader discovery. |
| `GroupNotFound { group_id }` | 组不存在；检查路由表和节点成员。 / Group missing; check the routing table and node membership. |
| `ProposalFailed { reason }` | 提案未提交；按 `reason` 决定是否重试。 / Proposal did not commit; use `reason` to decide whether to retry. |
| `Timeout` | Raft 操作超时；通常可按指数退避重试。 / Raft operation timed out; exponential-backoff retry is usually appropriate. |
| `InitializationFailed { reason }` | Raft 组初始化失败；修复配置或存储原因。 / Group initialization failed; fix the configuration or storage cause. |
| `UnsafeMembershipChange { target_voters, min }` | 目标 voter 集低于下限；仅在灾难恢复时显式使用 `force`。 / Target voter set is below the minimum; use explicit `force` only for disaster recovery. |
| `FinalVoterRemoval` | 最后 voter 不可通过普通成员变更移除。 / Ordinary membership changes cannot remove the final voter. |

### `StorageError`

| 变体 / Variant | 场景与处理 / When and how to handle |
| :--- | :--- |
| `Io(std::io::Error)` | 底层 I/O 失败；检查空间、权限与介质，通常需要运维介入。 / Underlying I/O failure; check space, permissions, and media. Usually requires operator action. |
| `Corruption(String)` | 完整性或解码检查失败；按恢复流程从快照或副本恢复。 / Integrity or decoding failed; recover from a snapshot or replica. |
| `NotFound { key }` | 指定键不存在；确认这是否符合应用预期。 / Key absent; confirm whether the application expects that state. |

### `ConfigError`

| 变体 / Variant | 场景与处理 / When and how to handle |
| :--- | :--- |
| `ProviderUnavailable(String)` | 配置后端不可达；检查网络与后端状态。 / Provider unavailable; check connectivity and backend health. |
| `InvalidConfig(String)` | 配置格式或内容非法；修正后重载。 / Configuration malformed or invalid; correct it before reloading. |
| `WatchDisconnected` | watch 中断；重新获取路由表并重新订阅。 / Watch disconnected; re-fetch the table and subscribe again. |

### `LifecycleError`

| 变体 / Variant | 含义 / Meaning |
| :--- | :--- |
| `Unavailable(String)` | lifecycle store 无法给出权威结果 / Lifecycle store cannot provide an authoritative answer |
| `CorruptData(String)` | 持久化生命周期数据损坏 / Persisted lifecycle data is corrupt |
| `UnsupportedSchema { schema, version }` | schema 版本不受支持 / Schema version is unsupported |
| `InvalidTransition(String)` | 状态迁移或不可变字段更新非法 / Invalid state transition or immutable-field update |
| `IdentityMismatch` / `SeedMismatch` | 集群身份或 seed 与授权记录不符 / Cluster identity or seed differs from the authorized record |
| `InvalidSeed(String)` | seed 违反 genesis 约束 / Seed violates genesis invariants |

### `RouterError`、`NetworkError` 与 `CodecError` / `RouterError`, `NetworkError`, and `CodecError`

| 变体 / Variant | 场景与处理 / When and how to handle |
| :--- | :--- |
| `PartitionNotFound { partition_id }` | 路由表无此分区；刷新并核对路由表。 / Partition absent; refresh and verify the routing table. |
| `ConnectionFailed { addr, reason }` | 连接失败；检查目标存活与网络可达性。 / Connection failed; check target liveness and reachability. |
| `RpcFailed(String)` | RPC 失败；检查服务与协议兼容性。 / RPC failed; check service health and protocol compatibility. |
| `RpcRejected { code, message }` | 确定性 gRPC 拒绝；保留状态码供分类，不要盲目重试。 / Deterministic gRPC rejection; classify by status code instead of blindly retrying. |
| `NetworkError::Timeout` | 网络超时；检查延迟与超时配置。 / Network timeout; inspect latency and timeout configuration. |
| `NetworkError::InvalidConfig(String)` | 客户端配置非法，在连接前失败。 / Invalid client configuration, rejected before connecting. |
| `SerializeFailed(String)` / `DeserializeFailed(String)` | 编解码失败；检查数据结构与格式版本。 / Codec failure; check the data structure and format version. |

## 传播与处理 / Propagation and Handling

底层错误通过 `From` 转换包装进 `RrError`，可用 `?` 传播，用 `match` 分类处理。
Lower-level errors use `From` conversions into `RrError`; propagate them with `?` or classify them with
`match`.

- `NotLeader` 与网络超时通常瞬时；`RrClient` 会处理 Leader 重定向、路由刷新和有限重试。
  原始 gRPC 调用方必须自行处理 `leader_info`。
  `NotLeader` and network timeouts are usually transient. `RrClient` handles leader redirects, route
  refreshes, and bounded retries; raw gRPC callers must process `leader_info` themselves.
- `RpcRejected`、非法配置与成员安全拒绝通常是确定性错误，重试不会修复。
  `RpcRejected`, invalid configuration, and membership-safety rejections are deterministic; retrying does
  not fix them.
- `IdempotencyExpired` 应使用大于返回 watermark 的新 `request_id`；命名空间超限则更换新 ID 无效，
  应复用已跟踪的 `request_type` 或调整容量。
  For `IdempotencyExpired`, use a fresh `request_id` above the returned watermark. For a namespace-limit
  rejection, changing the ID does not help; reuse a tracked `request_type` or adjust capacity.
- 存储 I/O 与数据损坏通常需要告警和运维介入。日志保留足够上下文与 Debug 信息，但不要记录密钥。
  Storage I/O and corruption usually need alerts and operator action. Keep useful context and Debug
  output in logs, but never log secrets.

:::warning Apply 错误不是普通业务响应 / Apply errors are not ordinary business responses
在 apply 路径返回 `Err` 会按 apply 错误契约使 Raft 组进入 fatal 路径，不能用它表达可预期的客户端
拒绝。框架支持的拒绝应使用类型化 outcome；业务校验应在 propose 前完成。
Returning `Err` from apply follows the apply-error contract and can take the Raft group down a fatal
path; do not use it for expected client rejections. Use typed outcomes for framework-supported
rejections and perform business validation before proposing.
:::
