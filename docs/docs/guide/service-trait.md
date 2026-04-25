---
sidebar_position: 2
---

# Service Trait

## 什么是 Service Trait / What is the Service Trait

`Service` 是 rr 框架提供的高层、用户友好的 API，用于实现业务逻辑。它抽象了 Raft 日志条目的复杂性，让开发者专注于处理业务请求。

在内部，框架会将你的 `Service` 包装在 `ServiceStateMachineAdapter` 中，从而与底层的 Raft 状态机（openraft）对接。

---

`Service` is a high-level, user-friendly API provided by the rr framework for implementing business logic. It abstracts away the complexity of Raft log entries, allowing developers to focus on processing business requests.

Internally, the framework wraps your `Service` in a `ServiceStateMachineAdapter` to bridge it with the underlying Raft state machine (openraft).

## Service 与 StateMachine 的对比 / Service vs StateMachine

| 特性 / Feature | Service | StateMachine |
| :--- | :--- | :--- |
| **抽象级别 / Abstraction level** | 高（业务逻辑）/ High (business logic) | 低（Raft 内部）/ Low (Raft internals) |
| **条目格式 / Entry format** | 原始字节（自定义编码）/ Raw bytes (your encoding) | 带有元数据的日志条目 / LogEntry with metadata |
| **快照 / Snapshot** | 简单的序列化与反序列化 / Simple serialize/deserialize | 复杂的日志管理 / Complex log management |
| **使用场景 / Use when** | 大多数应用程序 / Most applications | 需要完全控制 Raft 日志 / Need full Raft log control |

## 安全不变量 / Safety Invariants (MUST uphold)

实现 `Service` 时必须遵守 **3 条安全不变量**——违反任何一条都会导致**静默
数据损坏**（snapshot 不一致、follower 与 leader 偏离），且没有运行时检查
能在数据写到生产之前 catch 住。完整契约（含 OpenRaft 源码引用）见 trait 的
[rustdoc](https://docs.rs/rr-core/latest/rr_core/service/trait.Service.html)
和 [ADR A-0002](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md)。

---

Implementing `Service` requires upholding **3 safety invariants** — violating
any of them causes **silent data corruption** (inconsistent snapshots,
followers diverging from the leader), with no runtime check that catches
it before the data ships. See the trait
[rustdoc](https://docs.rs/rr-core/latest/rr_core/service/trait.Service.html)
and [ADR A-0002](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md)
for the full contract with OpenRaft source citations.

| # | Invariant | Failure mode |
| :- | :--- | :--- |
| 1 | `snapshot()` captures an atomically-consistent view (single lock or immutable clone) | Snapshot bytes mix old + new state; followers receive corrupt snapshot |
| 2 | `handle()` completes all state mutations synchronously before returning | Subsequent `snapshot()` reads stale state; followers diverge |
| 3 | `restore()` completely replaces state (no merging into existing data) | Restored node retains pre-restore data, drifts from leader's authoritative state |

### 反例与正例 / Anti-pattern and Correct Pattern (Invariant 1)

下面是最常见的违反 Invariant 1 的写法 vs 正确写法。两个 lock 之间出现的 gap
window 让 `apply()` 可以提交一个更新到 `map_b` 但 `map_a` 还没——`snapshot()`
拿到的两半就不对应同一个时刻。

---

The most common violation of Invariant 1 vs. the correct fix. The gap
window between two `read()` calls lets `apply()` commit an update to
`map_b` while `map_a` is unchanged — `snapshot()` returns two halves that
don't correspond to the same moment.

```rust
// ❌ 错误 / Incorrect — gap window between lock acquisitions
async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
    let a = self.map_a.read().await;
    let snap_a = a.clone();
    drop(a);                                // gap window opens here
    let b = self.map_b.read().await;        // b may already reflect a write
    let snap_b = b.clone();
    bincode::serde::encode_to_vec(&(snap_a, snap_b), bincode::config::standard())
        .map_err(|e| RrError::Codec(/* ... */))
}

// ✅ 正确 / Correct — single lock covers all fields
async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
    let state = self.state.read().await;    // covers ALL fields
    bincode::serde::encode_to_vec(&*state, bincode::config::standard())
        .map_err(|e| RrError::Codec(/* ... */))
}
```

## Service Trait 定义 / Service Trait Definition

以下是 `Service` trait 的核心方法（定义在 `crates/rr-core/src/service.rs`）：

```rust
#[async_trait]
pub trait Service: Send + Sync {
    /// 处理进入的客户端请求 / Handle an incoming client request.
    ///
    /// 框架在调用此方法前，会先通过 Raft 共识对请求字节进行提案。
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError>;

    /// 返回服务的名称（用于标识和日志记录）/ Return the name of this service.
    fn name(&self) -> &str;

    /// 为 Raft 快照创建服务状态的快照 / Create a snapshot of the service state.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// 从快照恢复服务状态 / Restore service state from a snapshot.
    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError>;
}
```

## 完整的 KvService 示例 / Complete KvService Example

以下是一个典型的 `KvService` 实现（参考自 `examples/kv-store/src/service.rs`）：

```rust
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use bincode::config::standard;
use tokio::sync::RwLock;
use rr_core::error::{CodecError, RrError};
use rr_core::service::Service;

#[derive(Clone)]
pub struct KvService {
    store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl Service for KvService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        // 解码请求 / Decode request
        let (req, _): (KvRequest, _) = bincode::serde::decode_from_slice(&request, standard())
            .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;

        let response = match req {
            KvRequest::Put { key, value } => {
                self.store.write().await.insert(key, value);
                KvResponse::none()
            }
            KvRequest::Get { key } => {
                let guard = self.store.read().await;
                KvResponse {
                    value: guard.get(&key).cloned(),
                }
            }
            KvRequest::Delete { key } => {
                self.store.write().await.remove(&key);
                KvResponse::none()
            }
        };

        // 编码响应 / Encode response
        let encoded = bincode::serde::encode_to_vec(&response, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))?;
        Ok(encoded)
    }

    fn name(&self) -> &str {
        "kv-store"
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let guard = self.store.read().await;
        bincode::serde::encode_to_vec(&*guard, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        let (restored, _): (HashMap<String, Vec<u8>>, _) =
            bincode::serde::decode_from_slice(data, standard())
                .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;
        *self.store.write().await = restored;
        Ok(())
    }
}
```

## 服务状态机适配器 / ServiceStateMachineAdapter

`ServiceStateMachineAdapter<S: Service>` 负责将你的 `Service` 包装起来。

- 它实现了 openraft 的状态机接口。
- 将 `apply()` 委托给 `Service::handle()`。
- 将 `snapshot()` 委托给 `Service::snapshot()`。
- 将 `restore()` 委托给 `Service::restore()`。
- 内部自动处理 `last_applied` 索引的追踪。

---

`ServiceStateMachineAdapter<S: Service>` wraps your `Service`.

- It implements openraft's state machine interface.
- Delegates `apply()` to `Service::handle()`.
- Delegates `snapshot()` to `Service::snapshot()`.
- Delegates `restore()` to `Service::restore()`.
- Internally handles `last_applied` index tracking automatically.

## 选择指南 / When to Use Service vs StateMachine

选择哪种方式取决于你的需求：

- **使用 `Service` 当：** 你只需要简单的请求/响应语义，不需要访问 Raft 日志的元数据（如 Index 或 Term）。
- **直接使用 `StateMachine` 当：** 你在 `apply()` 时需要访问日志的 Index/Term，或者你需要自定义快照格式，亦或是在构建高级的复制逻辑。

---

The choice depends on your requirements:

- **Use `Service` when:** you want simple request/response semantics and don't need access to log metadata (like Index or Term).
- **Use `StateMachine` directly when:** you need access to the log Index/Term in `apply()`, need custom snapshot formats, or are building advanced replication logic.

## 请求/响应编码 / Request/Response Encoding

关于序列化的建议：

- 请求和响应都是 `Vec<u8>`，你可以使用任何编码方式。
- 推荐：Rust 之间的通信使用 `bincode` v2，跨语言通信使用 `serde_json`。
- `kv-store` 示例中展示了使用 `bincode` 进行编解码的标准模式。

---

Notes about serialization:

- Both requests and responses are `Vec<u8>`, any encoding works.
- Recommended: `bincode` v2 for Rust-to-Rust communication, and `serde_json` for cross-language scenarios.
- The `kv-store` example demonstrates the standard pattern of using `bincode` for encoding and decoding.
