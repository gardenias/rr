---
sidebar_position: 4
---

# 错误参考 / Error Reference

## 概述 / Overview

在 `rr` 框架中，所有的操作都返回 `Result<T, RrError>`。`RrError` 是在 `rr-core` 中定义的统一错误枚举，利用 `thiserror` 库实现。这种设计确保了分布式系统中的各种错误（如 Raft 共识、存储、网络等）都能被正确捕获、传播并以类型安全的方式处理。

错误变体根据其所属的组件或领域进行分组，方便开发者根据错误类型采取相应的处理策略。

---

In the `rr` framework, all operations return `Result<T, RrError>`. `RrError` is a unified error enum defined in `rr-core`, implemented using the `thiserror` crate. This design ensures that various errors in the distributed system—such as Raft consensus, storage, and networking—are correctly captured, propagated, and handled in a type-safe manner.

Error variants are grouped by their respective components or domains, allowing developers to take appropriate handling strategies based on the error type.

## RrError 变体 / RrError Variants

`RrError` 包含以下主要类别：

---

`RrError` contains the following main categories:

| 变体 / Variant | 描述 / Description |
| :--- | :--- |
| `Raft(RaftError)` | Raft 共识层相关的错误，如领导者变更或提案失败。 / Errors related to the Raft consensus layer, such as leader changes or proposal failures. |
| `Storage(StorageError)` | 存储后端错误，包括 IO 故障或数据损坏。 / Storage backend errors, including IO failures or data corruption. |
| `Config(ConfigError)` | 配置管理错误，如配置提供程序不可用。 / Configuration management errors, such as configuration provider unavailability. |
| `Router(RouterError)` | 路由和分区查找错误。 / Errors during routing and partition lookup. |
| `Network(NetworkError)` | 网络通信错误，包括 RPC 失败和超时。 / Network communication errors, including RPC failures and timeouts. |
| `Codec(CodecError)` | 数据序列化或反序列化失败。 / Failures in data serialization or deserialization. |
| `Application(String)` | 用户定义的业务逻辑错误。 / User-defined business logic errors. |

### Raft 错误 / RaftError

| 变体 / Variant | 发生场景 / When it occurs | 处理建议 / How to handle it |
| :--- | :--- | :--- |
| `NotLeader { leader }` | 向非领导者节点发送写请求时。 / Sending a write request to a non-leader node. | 客户端应重定向请求到 `leader` 字段指定的节点。 / Client should redirect the request to the node specified in the `leader` field. |
| `GroupNotFound { group_id }` | 指定的 Raft 组在当前节点上不存在。 / The specified Raft group does not exist on the current node. | 检查路由表配置或确保节点已加入该组。 / Check routing table config or ensure the node has joined the group. |
| `ProposalFailed { reason }` | Raft 提案由于冲突或其他原因无法提交。 / Raft proposal cannot be committed due to conflicts or other reasons. | 根据 `reason` 判断是否重试。 / Decide whether to retry based on the `reason`. |
| `Timeout` | Raft 操作（如选举或提案提交）超时。 / Raft operations (e.g., election or proposal commit) timed out. | 通常是暂时的，建议进行指数退避重试。 / Usually transient; exponential backoff retry is recommended. |

### 存储错误 / StorageError

| 变体 / Variant | 发生场景 / When it occurs | 处理建议 / How to handle it |
| :--- | :--- | :--- |
| `Io(std::io::Error)` | 底层磁盘 IO 操作失败。 / Underlying disk IO operation failed. | 检查磁盘空间、权限或硬件状态。通常是致命错误。 / Check disk space, permissions, or hardware status. Usually fatal. |
| `Corruption(String)` | 检测到存储数据损坏。 / Detected storage data corruption. | 可能需要从快照恢复或重新同步节点。 / May require restoring from a snapshot or resyncing the node. |
| `NotFound { key }` | 在存储中找不到指定的键。 / The specified key was not found in storage. | 检查逻辑是否存在预期之外的缺失。 / Check if there is an unexpected absence in logic. |

### 配置错误 / ConfigError

| 变体 / Variant | 发生场景 / When it occurs | 处理建议 / How to handle it |
| :--- | :--- | :--- |
| `ProviderUnavailable(String)` | 无法连接到配置中心或元数据服务。 / Unable to connect to the configuration center or metadata service. | 检查配置提供程序（如 Mem, Etcd）的网络连接。 / Check network connectivity for the ConfigProvider (e.g., Mem, Etcd). |
| `InvalidConfig(String)` | 加载的配置文件格式错误或参数非法。 / Loaded config file has a wrong format or invalid parameters. | 校验配置内容。 / Validate the configuration content. |
| `WatchDisconnected` | 配置监听连接中断。 / Configuration watch connection was interrupted. | `rr` 会尝试重新建立监听，建议关注日志。 / `rr` will attempt to re-establish the watch; monitor logs. |

### 路由错误 / RouterError

| 变体 / Variant | 发生场景 / When it occurs | 处理建议 / How to handle it |
| :--- | :--- | :--- |
| `PartitionNotFound { id }` | 路由表中找不到指定的分区 ID。 / Specified partition ID not found in the routing table. | 确保客户端持有最新的路由表。 / Ensure the client has the latest routing table. |
| `GroupNotFound { id }` | 路由表中存在分区但找不到对应的 Raft 组。 / Partition exists in the routing table but no corresponding Raft group is found. | 同上。 / Same as above. |

### 网络错误 / NetworkError

| 变体 / Variant | 发生场景 / When it occurs | 处理建议 / How to handle it |
| :--- | :--- | :--- |
| `ConnectionFailed { addr, reason }` | 无法建立到指定地址的 TCP 连接。 / Failed to establish TCP connection to the specified address. | 检查目标节点存活状态和网络可达性。 / Check target node liveness and network accessibility. |
| `RpcFailed(String)` | gRPC 调用过程出错。 / Error during gRPC call. | 检查服务状态和 protobuf 兼容性。 / Check service status and protobuf compatibility. |
| `Timeout` | 网络请求在规定时间内未返回。 / Network request did not return within the specified time. | 增加超时时间或进行重试。 / Increase timeout or retry. |

### 编解码错误 / CodecError

| 变体 / Variant | 发生场景 / When it occurs | 处理建议 / How to handle it |
| :--- | :--- | :--- |
| `SerializeFailed(String)` | 将结构体序列化为字节流失败。 / Failed to serialize struct to byte stream. | 检查数据结构是否符合序列化要求。 / Check if data structures meet serialization requirements. |
| `DeserializeFailed(String)` | 从字节流还原结构体失败。 / Failed to restore struct from byte stream. | 检查数据版本是否匹配。 / Check for data version mismatch. |

## 错误传播 / Error Propagation

`rr` 内部采用层级化的错误处理机制。底层产生的特定错误（如 `StorageError`）会被封装进 `RrError::Storage` 变体中向上传播。

- **Storage → Raft**: 状态机应用日志时的存储故障会转化为 `RrError`。
- **Network → Client**: 远程调用失败会转化为 `NetworkError`。
- **Router → Client**: 路由查找失败会转化为 `RouterError`。

开发者可以利用 Rust 的 `?` 运算符方便地传播错误，或通过 `match` 语句对特定错误进行细粒度处理。

---

`rr` uses a hierarchical error handling mechanism internally. Specific errors generated at lower levels (e.g., `StorageError`) are wrapped into the `RrError::Storage` variant and propagated upwards.

- **Storage → Raft**: Storage failures when the state machine applies logs are converted to `RrError`.
- **Network → Client**: Remote call failures are converted to `NetworkError`.
- **Router → Client**: Routing lookup failures are converted to `RouterError`.

Developers can use Rust's `?` operator for easy error propagation or use `match` statements for fine-grained handling of specific errors.

## 常见错误场景 / Common Error Scenarios

### 非领导者 / NotLeader (ForwardToLeader)

当写请求被发送到非领导者节点时，该节点会返回 `NotLeader` 错误，并（如果知道的话）附带当前领导者的地址。

- **RrClient**: 客户端 SDK 会自动捕获此错误，更新其缓存的领导者信息并重试。
- **原始 gRPC**: 如果你直接调用 gRPC 接口，需要手动解析此错误并重定向请求。

---

When a write request is sent to a non-leader node, the node returns a `NotLeader` error, including the current leader's address if known.

- **RrClient**: The client SDK automatically captures this error, updates its cached leader info, and retries.
- **Raw gRPC**: If you are calling the gRPC interface directly, you need to manually parse this error and redirect the request.

### 网络超时与闪断 / Network Issues

分布式系统中，网络抖动是常态。`rr-network` 会产生超时或连接失败错误。

- **处理**: 建议在应用层实现带有抖动（jitter）的指数退避重试策略。

---

Network jitters are common in distributed systems. `rr-network` generates timeout or connection failure errors.

- **Handling**: It is recommended to implement an exponential backoff retry strategy with jitter at the application layer.

### 存储空间耗尽 / Storage Errors

`StorageError::Io` 通常意味着底层物理环境出了问题。

- **后果**: 这种错误通常是致命的，会导致 Raft 组停止服务。
- **调查**: 检查磁盘空间、文件系统权限或物理介质健康状况。

---

`StorageError::Io` usually means something is wrong with the underlying physical environment.

- **Consequences**: This type of error is usually fatal and causes the Raft group to stop serving.
- **Investigation**: Check disk space, file system permissions, or physical media health.

## 错误处理最佳实践 / Error Handling Best Practices

1. **优先使用 RrClient**: 官方 SDK 已经内置了领导者重定向、路由表刷新和自动重试逻辑，能屏蔽大部分瞬时错误。
2. **区分错误性质**: `NotLeader` 和网络超时通常是瞬时的（Transient），应当重试；`StorageError` 或 `InvalidConfig` 通常是永久的（Permanent），需要人工介入或报警。
3. **记录上下文**: 在捕获 `RrError` 并记录日志时，保留完整的 Debug 打印，以便追踪错误根源。
4. **状态机安全**: 在 `Service` 实现中，如果 `handle` 返回错误，该操作将不会记录在 Raft 日志中（对于读请求）或可能导致提案失败，务必确保错误处理逻辑不会破坏状态机的一致性。

---

1. **Prefer RrClient**: The official SDK has built-in leader redirection, routing table refreshing, and automatic retry logic, shielding most transient errors.
2. **Distinguish Error Nature**: `NotLeader` and network timeouts are usually transient and should be retried; `StorageError` or `InvalidConfig` are usually permanent and require manual intervention or alerting.
3. **Log Context**: When capturing `RrError` and logging it, keep the full Debug output to track the root cause.
4. **State Machine Safety**: In `Service` implementations, if `handle` returns an error, the operation won't be recorded in Raft logs (for read requests) or might cause proposal failure. Ensure error handling logic does not break state machine consistency.
