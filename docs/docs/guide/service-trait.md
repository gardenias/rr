---
sidebar_position: 2
---

# Service 特性 / Service Trait

`Service` 是业务请求处理的高层接口。框架用 `ServiceStateMachineAdapter` 将它接入 Raft，多数应用不需要直接实现底层状态机。
`Service` is the high-level interface for business request handling. `ServiceStateMachineAdapter` connects it to Raft, so most applications do not implement the low-level state machine directly.

## 与 StateMachine 的分工 / Service versus StateMachine

| 项目 / Aspect | `Service` | `StateMachine` |
| :--- | :--- | :--- |
| 请求 / Request | 已提交请求的业务字节 / Business bytes for a committed request | 原始日志字节，可选 `ApplyContext` / Raw entry bytes with optional `ApplyContext` |
| 进度 / Progress | 适配器跟踪 `last_applied` / Adapter tracks `last_applied` | 实现自行跟踪 / Implementation tracks it |
| 适用 / Use | 常规请求、快照和恢复 / Ordinary requests, snapshots, and restoration | 自定义帧格式或底层控制 / Custom framing or low-level control |

## 必须满足的安全条件 / Required Safety Invariants

违反以下任一条件都可能导致快照不一致或副本状态分叉。完整契约见当前仓库的 [`Service` 源码与 rustdoc 注释](https://github.com/gardenias/rr/blob/main/crates/rr-core/src/service.rs) 和 [ADR A-0002](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md)。
Violating any invariant below can cause inconsistent snapshots or diverging replicas. See the [`Service` source and rustdoc comments](https://github.com/gardenias/rr/blob/main/crates/rr-core/src/service.rs) in this repository and [ADR A-0002](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md) for the full contract.

| 条件 / Invariant | 错误后果 / Failure |
| :--- | :--- |
| `snapshot()` 用单个锁或不可变结构捕获完整原子视图 / Capture one atomic view with one lock or an immutable structure | 快照混合不同时刻的状态 / Snapshot mixes different points in time |
| `handle()` 返回前完成所有状态修改 / Finish every mutation before `handle()` returns | 随后的快照漏掉已应用修改 / A later snapshot misses applied mutations |
| `restore()` 完整替换旧状态 / Replace all old state in `restore()` | 恢复后残留旧数据 / Pre-restore data survives |

错误写法是在两个读锁之间释放第一个锁，再读取另一部分状态；这样得到的两个字段不属于同一时刻。
A broken implementation releases one read lock before acquiring another for a different field, producing fields from different points in time.

```rust
// 错误 / Incorrect: two independent lock windows.
let a = self.map_a.read().await.clone();
let b = self.map_b.read().await.clone();

// 正确 / Correct: one lock covers every snapshotted field.
let state = self.state.read().await;
let bytes = bincode::serde::encode_to_vec(&*state, bincode::config::standard())?;
```

## 接口与默认方法 / Interface and Default Methods

必需方法如下：
The required methods are:

```rust
#[async_trait]
pub trait Service: Send + Sync {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError>;
    fn name(&self) -> &str;
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;
    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError>;
}
```

默认方法按需覆盖：
Override default methods only when needed:

- `handle_with_outputs`：apply 后以至少一次语义产生输出。/ Emit outputs with at-least-once delivery after apply.
- `handle_with_ctx` / `handle_with_outputs_ctx`：接收真实 `ApplyContext { log_index, log_term }`；后者同时支持输出。/ Receive the real apply context; the outputs variant also emits outputs.
- `handle_deferred`：两阶段 apply 可延迟客户端响应；不与框架幂等键层组合。/ Two-phase apply may defer a client response; it does not compose with framework idempotency keys.
- `on_leader_elected` / `on_step_down`：管理仅 leader 使用的资源。/ Manage leader-only resources.
- `shutdown`：组停止前释放资源。/ Release resources before the group stops.

## KV 实现示例 / KV Implementation

下面模式与 `examples/kv-store/src/service.rs` 一致：单个 `RwLock` 同时保护业务状态和快照视图。
This follows `examples/kv-store/src/service.rs`: one `RwLock` protects both business state and the snapshot view.

```rust
use std::{collections::HashMap, sync::Arc};
use async_trait::async_trait;
use bincode::config::standard;
use rr_core::error::{CodecError, RrError};
use rr_core::service::Service;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct KvService {
    store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl Service for KvService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        let (request, _): (KvRequest, _) =
            bincode::serde::decode_from_slice(&request, standard())
                .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;

        let response = match request {
            KvRequest::Put { key, value } => {
                self.store.write().await.insert(key, value);
                KvResponse { value: None }
            }
            KvRequest::Get { key } => KvResponse {
                value: self.store.read().await.get(&key).cloned(),
            },
            KvRequest::Delete { key } => {
                self.store.write().await.remove(&key);
                KvResponse { value: None }
            }
        };

        bincode::serde::encode_to_vec(&response, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    fn name(&self) -> &str {
        "kv-store"
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let state = self.store.read().await;
        bincode::serde::encode_to_vec(&*state, standard())
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

不要在 `handle()` 中用 `tokio::spawn` 延迟实际写入；如果解码或业务校验失败，应返回错误，不要推进状态。
Do not defer the actual mutation with `tokio::spawn` in `handle()`. Return an error on decode or validation failure without advancing state.

## 编码与读取 / Encoding and Reads

请求、响应和快照都是字节，编码由应用选择。`kv-store` 使用 bincode v2；跨语言协议需要双方共享且版本化的 schema，不能依赖 Rust 私有类型布局。
Requests, responses, and snapshots are bytes chosen by the application. `kv-store` uses bincode v2; cross-language protocols need a shared, versioned schema rather than Rust-specific type layouts.

读 API 不会替应用决定一致性。本地读与 ReadIndex 的区别见 [读一致性模型](read-consistency.md)；重试写需要业务键或 [框架幂等](idempotency.md)。
Read APIs do not choose consistency for the application. See [Read Consistency](read-consistency.md) for local reads versus ReadIndex and [Framework Idempotency](idempotency.md) for retried writes.
