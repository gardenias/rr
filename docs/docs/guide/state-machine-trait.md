---
sidebar_position: 3
---

# StateMachine Trait / StateMachine 特性

## 什么是 StateMachine Trait / What is the StateMachine Trait

`StateMachine` 是 `rr` 框架中用于实现底层状态机的核心接口。它直接处理来自 Raft 日志的原始字节条目（包含索引和任期信息）。

如果你需要对状态机的行为进行细粒度控制，或者不希望使用更高层级的 `Service` 抽象，可以直接实现此 Trait。它允许你完全自主地管理序列化、反序列化以及状态转换逻辑。

---

The `StateMachine` trait is the core low-level interface for implementing state machines in the `rr` framework. It provides direct access to raw byte entries from the Raft log, including their associated index and term.

Use this trait when you need fine-grained control over state machine behavior or prefer not to use the higher-level `Service` abstraction. It grants you full autonomy over serialization, deserialization, and state transition logic.

## StateMachine Trait 方法 / StateMachine Trait Methods

以下是 `StateMachine` Trait 的核心定义（位于 `crates/rr-core/src/state_machine.rs`）：

---

Here is the core definition of the `StateMachine` trait (found in `crates/rr-core/src/state_machine.rs`):

```rust
#[async_trait]
pub trait StateMachine: Send + Sync {
    /// 应用一个已提交的 Raft 日志条目到状态机。
    /// Apply a committed Raft log entry to the state machine.
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError>;

    /// 创建当前状态的一致性快照。
    /// Create a consistent snapshot of the current state.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// 从快照恢复状态。
    /// Restore state from a snapshot.
    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError>;

    /// 返回最后一次应用的日志条目的索引。
    /// Return the index of the last applied log entry, if any.
    async fn last_applied(&self) -> Option<LogIndex>;
}
```

- **apply**: 接收原始字节，执行状态变更，并返回发送给客户端的响应字节。
- **snapshot**: 将当前内存状态序列化为 `Vec<u8>`。
- **restore**: 接收字节流并重建内部状态。
- **last_applied**: 用于 Raft 恢复时确定应用进度。

---

- **apply**: Receives raw bytes, performs state transitions, and returns response bytes for the client.
- **snapshot**: Serializes the current in-memory state into a `Vec<u8>`.
- **restore**: Rebuilds the internal state from a provided byte stream.
- **last_applied**: Used during Raft recovery to determine the application progress.

## RrStateMachine 如何包装你的状态机 / How RrStateMachine Wraps Your StateMachine

在 `rr-raft` 内部，`RrStateMachine` 充当了你的 `StateMachine` 实现与 `openraft` 的 `RaftStateMachine` 接口之间的桥梁。

---

Internally within `rr-raft`, `RrStateMachine` acts as a bridge between your `StateMachine` implementation and openraft's `RaftStateMachine` interface.

- **适配器模式**: `RrStateMachine` 实现了 `openraft` 所需的复杂接口，而将核心逻辑委托给你的 `StateMachine`。
- **快照管理**: 它管理 `RrSnapshotBuilder`，处理大快照的分块传输逻辑。
- **元数据跟踪**: 自动处理 `last_applied` 索引的跟踪以及成员变更（Membership）日志的应用。

---

- **Adapter Pattern**: `RrStateMachine` implements the complex interfaces required by `openraft` while delegating core logic to your `StateMachine`.
- **Snapshot Management**: It manages the `RrSnapshotBuilder` and handles chunked transfer logic for large snapshots.
- **Metadata Tracking**: It automatically tracks the `last_applied` index and handles the application of membership change logs.

## 完整的 KvStateMachine 示例 / Complete KvStateMachine Example

以下是 `examples/kv-store/src/state_machine.rs` 中一个完整的实现示例。它展示了如何处理自定义的请求和响应类型，并实现快照逻辑。

---

Below is a complete implementation example from `examples/kv-store/src/state_machine.rs`. It demonstrates how to handle custom request and response types and implement snapshot logic.

```rust
use std::collections::HashMap;
use async_trait::async_trait;
use bincode::config::standard;
use rr_core::error::{CodecError, RrError};
use rr_core::state_machine::StateMachine;
use rr_core::types::LogIndex;

pub struct KvStateMachine {
    store: HashMap<String, Vec<u8>>,
    last_applied: Option<LogIndex>,
}

#[async_trait]
impl StateMachine for KvStateMachine {
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
        // 在此示例中，entry 包含 8 字节索引和序列化的 payload
        // In this example, entry contains an 8-byte index and serialized payload
        if entry.len() < 8 {
            return Err(RrError::Codec(CodecError::DeserializeFailed("Too short".into())));
        }
        let (index_bytes, payload) = entry.split_at(8);
        let index = u64::from_le_bytes(index_bytes.try_into().unwrap());

        let (req, _): (KvRequest, _) = bincode::serde::decode_from_slice(payload, standard())
            .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;

        let response = match req {
            KvRequest::Put { key, value } => {
                self.store.insert(key, value);
                KvResponse::none()
            }
            KvRequest::Get { key } => {
                KvResponse { value: self.store.get(&key).cloned() }
            }
            KvRequest::Delete { key } => {
                self.store.remove(&key);
                KvResponse::none()
            }
        };

        self.last_applied = Some(index);

        bincode::serde::encode_to_vec(&response, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        bincode::serde::encode_to_vec(&self.store, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError> {
        let (restored, _): (HashMap<String, Vec<u8>>, _) =
            bincode::serde::decode_from_slice(snapshot, standard())
                .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;
        self.store = restored;
        Ok(())
    }

    async fn last_applied(&self) -> Option<LogIndex> {
        self.last_applied
    }
}
```

## 快照生命周期 / Snapshot Lifecycle

`rr` 遵循标准的 Raft 快照流程：

1. **触发**: 当已提交日志数量超过 `max_in_memory_logs` 配置时，`openraft` 触发快照创建。
2. **构建**: `RrSnapshotBuilder::build_snapshot()` 调用你的 `StateMachine::snapshot()` 方法。
3. **编码**: 状态机返回序列化后的 `Vec<u8>` 字节流。
4. **传输**: 如果有节点进度落后太远，Leader 通过 `InstallSnapshot` RPC 将快照发送给 Follower。
5. **恢复**: Follower 接收完整快照后，通过 `RrStateMachine::install_snapshot()` 调用你的 `StateMachine::restore()`。

---

`rr` follows the standard Raft snapshot workflow:

1. **Trigger**: When the number of committed logs exceeds the `max_in_memory_logs` configuration, `openraft` triggers snapshot creation.
2. **Build**: `RrSnapshotBuilder::build_snapshot()` calls your `StateMachine::snapshot()` method.
3. **Encode**: The state machine returns a serialized `Vec<u8>` byte stream.
4. **Transfer**: If a node falls too far behind, the Leader sends the snapshot to the Follower via `InstallSnapshot` RPCs.
5. **Restore**: After receiving the full snapshot, the Follower calls your `StateMachine::restore()` via `RrStateMachine::install_snapshot()`.

## 序列化模式 / Serialization Patterns

虽然状态快照可以是任何 `Vec<u8>`，但我们建议在 `rr` 生态中使用 `bincode` v2。

---

While state snapshots can be any `Vec<u8>`, we recommend using `bincode` v2 within the `rr` ecosystem.

```rust
// 推荐的序列化模式 / Recommended serialization pattern
let encoded = bincode::serde::encode_to_vec(&state, bincode::config::standard())?;

// 推荐的反序列化模式 / Recommended deserialization pattern
let (decoded, _) = bincode::serde::decode_from_slice(data, bincode::config::standard())?;
```

使用 `bincode::config::standard()` 可以确保跨平台的兼容性和极致的性能。

---

Using `bincode::config::standard()` ensures cross-platform compatibility and peak performance.
