---
sidebar_position: 3
---

# StateMachine 特性 / StateMachine Trait

`StateMachine` 直接处理已提交日志的字节并自行维护 `last_applied`。需要自定义帧格式、快照或底层控制时使用；常规业务优先实现 [Service](service-trait.md)。
`StateMachine` applies committed entry bytes and tracks `last_applied` itself. Use it for custom framing, snapshots, or low-level control; ordinary business logic should implement [Service](service-trait.md).

## 安全条件 / Safety Invariants

除 Service 的三项条件外，底层实现还必须正确维护应用进度。完整契约见当前仓库的 [`StateMachine` 源码与 rustdoc 注释](https://github.com/gardenias/rr/blob/main/crates/rr-core/src/state_machine.rs) 和 [ADR A-0002](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md)。
In addition to the Service invariants, low-level implementations must maintain apply progress correctly. See the [`StateMachine` source and rustdoc comments](https://github.com/gardenias/rr/blob/main/crates/rr-core/src/state_machine.rs) in this repository and [ADR A-0002](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md).

| 条件 / Invariant | 错误后果 / Failure |
| :--- | :--- |
| `snapshot()` 捕获完整原子视图，包括 `last_applied` / Capture an atomic view including `last_applied` | 恢复点与业务状态不一致 / Restore point disagrees with state |
| `apply()` 返回前完成修改并更新 `last_applied` / Finish mutations and update `last_applied` before returning | 恢复时重放错误区间 / Recovery replays the wrong range |
| `restore()` 替换全部状态并恢复快照进度 / Replace all state and restore snapshot progress | 旧数据或旧进度残留 / Old state or progress survives |

`apply()` 的 `entry` 是提案负载；它不会自动携带可信的 Raft index/term。需要真实日志上下文时覆盖 `apply_with_ctx(entry, ApplyContext)`，不要把客户端伪造的元数据当作 fencing 依据。
The `entry` passed to `apply()` is the proposed payload; it does not automatically carry trusted Raft index or term metadata. Override `apply_with_ctx(entry, ApplyContext)` for real log context instead of trusting client-supplied metadata.

## 必需接口 / Required Interface

```rust
#[async_trait]
pub trait StateMachine: Send + Sync {
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError>;
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;
    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError>;
    async fn last_applied(&self) -> Option<LogIndex>;
}
```

`apply_with_ctx` 和 `apply_deferred` 有默认实现。后者可延迟客户端响应；默认路径依次委托到 `apply_with_ctx` 和 `apply`。
`apply_with_ctx` and `apply_deferred` have defaults. The latter may defer a client response; the default path delegates through `apply_with_ctx` to `apply`.

## 自定义帧示例 / Custom Frame Example

`examples/kv-store/src/state_machine.rs` 演示了自定义的 `index || request` 帧。下面的快照同时包含业务和进度，避免只恢复 KV map 而丢失 `last_applied`。
`examples/kv-store/src/state_machine.rs` demonstrates a custom `index || request` frame. This snapshot includes both business state and progress, avoiding a KV-only restore that loses `last_applied`.

```rust
use std::collections::HashMap;
use async_trait::async_trait;
use bincode::config::standard;
use rr_core::error::{CodecError, RrError};
use rr_core::{state_machine::StateMachine, types::LogIndex};

pub struct KvStateMachine {
    store: HashMap<String, Vec<u8>>,
    last_applied: Option<LogIndex>,
}

#[async_trait]
impl StateMachine for KvStateMachine {
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
        if entry.len() < 8 {
            return Err(RrError::Codec(CodecError::DeserializeFailed(
                "entry too short".into(),
            )));
        }
        let (index, payload) = entry.split_at(8);
        let index = LogIndex::from_le_bytes(index.try_into().unwrap());
        let (request, _): (KvRequest, _) =
            bincode::serde::decode_from_slice(payload, standard())
                .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;

        let response = match request {
            KvRequest::Put { key, value } => {
                self.store.insert(key, value);
                KvResponse { value: None }
            }
            KvRequest::Get { key } => KvResponse {
                value: self.store.get(&key).cloned(),
            },
            KvRequest::Delete { key } => {
                self.store.remove(&key);
                KvResponse { value: None }
            }
        };
        self.last_applied = Some(index);
        bincode::serde::encode_to_vec(&response, standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        bincode::serde::encode_to_vec(&(self.last_applied, &self.store), standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))
    }

    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError> {
        let ((last_applied, store), _): ((Option<LogIndex>, HashMap<String, Vec<u8>>), _) =
            bincode::serde::decode_from_slice(snapshot, standard())
                .map_err(|e| RrError::Codec(CodecError::DeserializeFailed(e.to_string())))?;
        self.last_applied = last_applied;
        self.store = store;
        Ok(())
    }

    async fn last_applied(&self) -> Option<LogIndex> {
        self.last_applied
    }
}
```

该帧格式是示例的私有约定，不是框架通用格式。采用它时，提案方必须显式生成相同格式；需要框架提供的真实 index 时改用 `apply_with_ctx`。
This frame is the example's private convention, not a framework-wide format. Producers must encode it explicitly; use `apply_with_ctx` when the framework's real index is required.

## 适配器与快照 / Adapter and Snapshots

`RrStateMachine` 将底层实现接到 OpenRaft，并处理快照构建、传输和安装。实现仍负责业务字节、原子性和 `last_applied`。
`RrStateMachine` connects the implementation to OpenRaft and handles snapshot building, transfer, and installation. The implementation still owns business bytes, atomicity, and `last_applied`.

默认在距上个快照 50,000 条日志后触发快照，可用 `NodeConfig::with_snapshot_logs_since_last` 调整。落后过多的 follower 通过 `InstallSnapshot` 接收快照，随后调用 `restore()`。
Snapshots default to every 50,000 entries since the previous snapshot and can be tuned with `NodeConfig::with_snapshot_logs_since_last`. A lagging follower receives one through `InstallSnapshot`, followed by `restore()`.

序列化格式必须同时版本化业务状态和恢复进度。bincode v2 是仓库示例的选择，不是跨语言兼容性的保证。
Version the serialization format for both business state and recovery progress. The repository examples use bincode v2; that alone does not guarantee cross-language compatibility.
