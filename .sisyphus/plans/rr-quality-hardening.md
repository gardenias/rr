# rr Quality Hardening Plan

> 来源：`.sisyphus/evidence/rr-framework-review-2026-05-08.md`
> 目标：修复所有生产可靠性问题，提升吞吐量，完善 API。

## 状态

| 阶段 | 内容 | 状态 |
|---|---|---|
| P0 | Lock poisoning + RetryConfig 溢出 | 🚧 进行中 |
| P1 | Ingestor batch write + Service lifecycle hooks | ⏳ 待开始 |
| P2 | GroupManager RwLock 合并 + Proto 错误码 | ⏳ 待开始 |
| P3 | Rate limiter GC + Backup checksum | ⏳ 待开始 |

---

## Phase P0 — 立即修复（生产崩溃风险）

### P0-1: Lock Poisoning

**文件**: `crates/rr-raft/src/service_adapter.rs:105,111,149,159`
**文件**: `crates/rr-network/src/rate_limit.rs:231,235,245`

```rust
// Before:
.expect("...lock poisoned")
.unwrap()

// After:
.unwrap_or_else(|poisoned| poisoned.into_inner())
```

### P0-2: RetryConfig 指数退避溢出

**文件**: `crates/rr-client/src/lib.rs:435,484,485,513,514,558,559,609,610`

```rust
// Before:
let delay_ms = self.retry_config.retry_delay_ms * 2u64.pow(attempt);

// After:
let delay_ms = self.retry_config.retry_delay_ms
    .saturating_mul(2u64.saturating_pow(attempt.min(20)));
```

---

## Phase P1 — 高优先级改进（吞吐量 + API 完整性）

### P1-1: Ingestor Batch Write

目标：`ingestor_runner` 将 batch 内所有命令通过一次 `raft.client_write_many()` 提交，而非逐条提交。

涉及：
- `RaftGroupManager::write_batch(group_id, Vec<Vec<u8>>) -> Vec<Result<Vec<u8>>>`
- `ingestor_runner.rs` poll loop 改为批量提交
- openraft 0.10 有 `client_write_many()` 支持此模式

预期效益：吞吐 1000 → 10000+ msgs/sec/group（取决于 Raft batch size）

### P1-2: Service Trait Lifecycle Hooks

新增可选生命周期方法（默认 no-op，向后兼容）：

```rust
pub trait Service: Send + Sync + 'static {
    // ... existing methods ...

    /// Called when this node becomes the group leader.
    async fn on_leader_elected(&mut self, group_id: GroupId) -> Result<(), RrError> { Ok(()) }

    /// Called when this node steps down from leadership.
    async fn on_step_down(&mut self, group_id: GroupId) { }

    /// Called before the group is stopped. Last chance to flush/cleanup.
    async fn shutdown(&mut self) -> Result<(), RrError> { Ok(()) }
}
```

`RaftGroupManager` 内部监听 metrics 变化，自动触发这些钩子（不需要用户手动监听）。

---

## Phase P2 — 中期重构

### P2-1: GroupManager State 合并

将 7 个独立 `Arc<RwLock<HashMap<GroupId, ...>>>` 合并：

```rust
struct GroupState {
    node: RaftNode,
    ingestor_handle: Option<IngestorHandle>,
    metrics_ticker: Option<MetricsTickerHandle>,
    ingestor_offset_control: Option<Arc<dyn IngestorOffsetControl>>,
    read_handle: Option<Arc<dyn StateMachineReader>>,
    learner_promoter: Option<LearnerPromoterHandle>,
}

// Single lock:
groups: Arc<DashMap<GroupId, Arc<Mutex<GroupState>>>>,
```

减少 `stop_group()` 从 6 次写锁获取到 1 次。

### P2-2: Proto 错误码 gRPC-idiomatic

Admin RPC 改用 gRPC Status 而非 `bool success + string message`：
- `NOT_FOUND` → group 不存在
- `PERMISSION_DENIED` → auth 失败
- `RESOURCE_EXHAUSTED` → rate limit
- `ALREADY_EXISTS` → group 已存在

Note: 这是 breaking change，需要同步更新 Java SDK。

---

## Phase P3 — 小修复

### P3-1: Rate Limiter 后台 GC

```rust
// 启动时 spawn 后台 cleanup task：
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        let mut guard = entries.write().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        guard.retain(|_, entry| now.duration_since(entry.last_seen) <= ttl);
    }
});
```

### P3-2: Backup Checksum

```rust
// backup metadata 增加字段：
pub sha256: String,  // hex-encoded SHA256 of archive bytes

// restore 时验证：
let actual = sha256_of_file(&input)?;
if actual != meta.sha256 { bail!("checksum mismatch"); }
```

---

## 工作顺序

```
P0-1 (lock poisoning) → P0-2 (overflow) → commit "fix(reliability): fix lock poisoning..."
P1-1 (ingestor batch) → commit "feat(raft,ingestor): batch write..."
P1-2 (service hooks)  → commit "feat(core): add Service lifecycle hooks..."
P2-1 (GM state merge) → commit "refactor(raft): merge GroupManager state..."
P2-2 (proto errors)   → commit "feat(network): gRPC-idiomatic error codes..."
P3-1 (rate GC)        → commit "fix(network): background GC for per-caller limiter..."
P3-2 (backup checksum)→ commit "fix(admin): add SHA256 checksum to backup..."
```
