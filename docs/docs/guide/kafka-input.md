---
title: Kafka 输入 / Kafka Input (CommandIngestor)
description: Drive an rr Raft group from a Kafka topic with leader-exclusive consumption and exactly-once apply
---

# Kafka 输入 / Kafka Input

自 v0.2.0 起，`rr-ingestor-kafka` 可将 Kafka topic 作为 Raft 组的写输入。只有当前 leader 消费；follower 通过 Raft 日志接收命令。结合 [框架幂等](idempotency.md)，Kafka 至少一次投递在用户 `Service::handle()` 处变为恰好一次 apply。迁移见 [v0.1 → v0.2 指南](../deployment/v0.1-to-v0.2-migration.md)，设计见 [ADR A-0001](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0001-command-ingestor-abstraction.md)。
Since v0.2.0, `rr-ingestor-kafka` can use a Kafka topic as a Raft group's write input. Only the current leader consumes; followers receive commands through Raft replication. With [framework idempotency](idempotency.md), Kafka's at-least-once delivery becomes exactly-once apply at `Service::handle()`. See the [migration guide](../deployment/v0.1-to-v0.2-migration.md) and [ADR A-0001](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0001-command-ingestor-abstraction.md).

## Topic 规划 / Topic Layout

默认每组使用 `input_{group_id}`，topic 名可配置。每组一个 topic，且必须恰好一个 partition：
By default, each group uses `input_{group_id}`; the pattern is configurable. Use one topic per group with exactly one partition:

```text
Raft Group 0 ↔ topic input_0 (partition 0)
Raft Group 1 ↔ topic input_1 (partition 0)
Raft Group N ↔ topic input_N (partition 0)
```

框架用 `assign()` 而非 `subscribe()` 固定 partition，避免 consumer-group rebalance 在持有领导权时移动消费。多 partition 会破坏 leader 独占消费模型。
The framework uses `assign()`, not `subscribe()`, to pin the partition so consumer-group rebalances cannot move consumption during leadership. Multiple partitions violate leader-exclusive consumption.

## 生命周期 / Lifecycle

```text
Group G becomes leader
    │
    ├─ Read last_applied_offset from the framed snapshot
    ├─ seek(last_applied_offset + 1)
    └─ Poll loop:
       poll() → frame offset/payload/idempotency key
              → Raft client_write() → commit + apply

Step down
    │
    ├─ Cancel the poll loop
    └─ Drop the ingestor
```

`client_write` 等待 commit 和 apply，因此消费速度受 Raft 提交吞吐限制。
`client_write` waits for commit and apply, so Raft throughput naturally limits consumption.

`seek` 失败时 supervisor 退出，不会退回到自由 poll；否则 `auto.offset.reset = "earliest"` 可能从 0 静默重放。此时增加 `rr_ingestor_seed_failures_total{group_id}` 并记录结构化错误。
If `seek` fails, the supervisor exits instead of falling through to polling; otherwise `auto.offset.reset = "earliest"` could silently replay from zero. It increments `rr_ingestor_seed_failures_total{group_id}` and logs a structured error.

## 构建前提 / Build Prerequisites

默认 feature 只包含内存 `MockKafkaIngestor` 和公共配置，无本地 Kafka 依赖。真实后端需要 `rdkafka` feature 和 `cmake`。
Default features include only the in-memory `MockKafkaIngestor` and public configuration, with no native Kafka dependency. The real backend requires the `rdkafka` feature and `cmake`.

```toml
[dependencies]
rr-ingestor-kafka = { path = "../rr/crates/rr-ingestor-kafka", features = ["rdkafka"] }
```

```bash
# Ubuntu
sudo apt-get install cmake

# macOS
brew install cmake
```

启用后，rdkafka 使用 `cmake-build` 编译捆绑的 librdkafka。
With this feature, rdkafka uses `cmake-build` to compile the bundled librdkafka.

## 接入 Raft 组 / Attach to a Raft Group

推荐使用 `attach_ingestor_with_auto_idempotency`，以关闭 leader 切换造成的重复 apply 窗口：
Use `attach_ingestor_with_auto_idempotency` to close the duplicate-apply window during leadership changes:

```rust
use rr_ingestor_kafka::{kafka_offset_to_request_id, KafkaIngestor, KafkaIngestorConfig};
use rr_raft::ingestor_runner::{AutoIdempotency, OffsetMapError};

let cfg = KafkaIngestorConfig {
    bootstrap_servers: vec!["broker1:9092".into()],
    topic_pattern: "input_{group_id}".to_string(),
    max_batch_size: 100,
    poll_timeout: std::time::Duration::from_millis(200),
    ..Default::default()
};
let ingestor = KafkaIngestor::new(group_id, cfg.clone()).await?;

mgr.attach_ingestor_with_auto_idempotency(
    group_id,
    Box::new(ingestor),
    AutoIdempotency {
        request_type: cfg.topic_for(group_id),
        offset_to_request_id: Box::new(|bytes| {
            kafka_offset_to_request_id(bytes).map_err(OffsetMapError::new)
        }),
    },
).await?;
```

只有 `Service::handle()` 自身已按业务键幂等时，才考虑裸 `attach_ingestor`。
Consider bare `attach_ingestor` only when `Service::handle()` is already idempotent by business key.

## TLS 与 SASL / TLS and SASL

`KafkaSecurityConfig` 映射到 rdkafka 的 `security.protocol`、`ssl.*` 和 `sasl.*`。密码通过 `*_env` 指定的环境变量读取，不写入配置文件。
`KafkaSecurityConfig` maps to rdkafka's `security.protocol`, `ssl.*`, and `sasl.*` properties. Passwords are read from environment variables named by `*_env`, not stored in configuration files.

```rust
use rr_ingestor_kafka::{KafkaIngestorConfig, KafkaSecurityConfig, SecurityProtocol};

let cfg = KafkaIngestorConfig {
    bootstrap_servers: vec!["broker.prod:9094".into()],
    security: Some(KafkaSecurityConfig {
        protocol: SecurityProtocol::SaslSsl,
        ssl_ca_path: Some("/etc/ssl/kafka/ca.pem".into()),
        ssl_cert_path: None,
        ssl_key_path: None,
        ssl_key_password_env: None,
        sasl_mechanism: Some("SCRAM-SHA-256".into()),
        sasl_username: Some("rr-prod".into()),
        sasl_password_env: Some("KAFKA_PASSWORD".into()),
    }),
    ..Default::default()
};
```

环境变量缺失或为空时，`KafkaIngestor::new` 以 `KafkaIngestorError::MissingSecret` 立即失败。
A missing or empty environment variable fails `KafkaIngestor::new` with `KafkaIngestorError::MissingSecret`.

## 自定义 Ingestor / Custom Ingestors

其他数据源实现 `CommandIngestor` 即可接入：
Other sources integrate by implementing `CommandIngestor`:

```rust
use async_trait::async_trait;
use rr_core::error::RrError;
use rr_core::ingestor::{CommandIngestor, IngressCommand};

#[async_trait]
impl CommandIngestor for MyIngestor {
    type Offset = i64;

    async fn seek(&mut self, offset: Option<i64>) -> Result<(), RrError> { /* ... */ }
    async fn poll(
        &mut self,
        max_batch_size: usize,
    ) -> Result<Vec<IngressCommand<i64>>, RrError> { /* ... */ }
    async fn commit_applied(&mut self, offset: i64) -> Result<(), RrError> { /* ... */ }
}
```

实现必须满足：
Implementations must satisfy:

1. 每个 `(group_id, source)` 同时只有一个 ingestor；框架在成为 leader 时创建，step-down 时销毁。/ One ingestor per `(group_id, source)`; the framework creates it on promotion and drops it on step-down.
2. `poll()` 的 offset 严格递增，允许空洞但不允许回退。/ Poll offsets strictly increase; gaps are allowed, regressions are not.
3. 相同 `seek(O)` 可重复调用且效果一致。/ Repeating the same `seek(O)` has the same effect.
4. `poll()` 和 `seek()` 不得提交源端 offset，只有 `commit_applied()` 可以。/ Neither `poll()` nor `seek()` commits source offsets; only `commit_applied()` may.

## 指标 / Metrics

| 指标 / Metric | 类型 / Type | 触发 / Trigger |
| :--- | :--- | :--- |
| `rr_ingestor_seek_total` | counter | leader 提升时调用 `seek()` / `seek()` on leader promotion |
| `rr_ingestor_poll_duration_seconds` | histogram | 每次 `poll()` / Each poll |
| `rr_ingestor_seed_failures_total` | counter | seek 失败，supervisor 退出 / Seek failure and supervisor exit |
| `rr_ingestor_step_down_drops_total` | counter | step-down 丢弃批次尾部；下任 leader 重拉并由幂等去重 / Batch tail dropped on step-down, then refetched and deduplicated |

将 step-down drop 与 `rr_idempotency_replays_total` 一起观察，可区分重拉和实际去重。
Observe step-down drops with `rr_idempotency_replays_total` to correlate refetches with deduplication.

## 故障与处理 / Failures and Responses

| 症状 / Symptom | 原因 / Cause | 处理 / Action |
| :--- | :--- | :--- |
| `rr_ingestor_seed_failures_total > 0` | 持久 offset 损坏或提升时 broker 不可达 / Corrupt offset or unreachable broker | 检查 broker；必要时用 `rr-admin group reset-ingestor-offset --group <G> --offset <N>` 覆盖，`-1` 清除 / Check the broker; override with the Admin command if needed, using `-1` to clear |
| 持续出现 `ingestor poll failed; backing off` / Repeated poll warnings | broker 中断 / Broker outage | 恢复 broker；退避上限 30 秒，共识本身不受影响 / Restore the broker; backoff caps at 30 seconds and consensus continues |
| 自动幂等下 collision 增加 / Collisions with auto-idempotency | 多条流复用 `request_type` / Streams share a request type | 确保 `topic_for(group_id)` 对流唯一 / Make `topic_for(group_id)` unique per stream |
| out-of-range seek 警告 / Out-of-range seek warning | retention 已裁过持久 offset / Retention trimmed past the persisted offset | 检查保留策略与消费 lag；watermark 查询失败会跳过范围校验并记录日志 / Inspect retention and lag; watermark failure skips range validation with a log |

新 leader seek 前会刷新过旧快照；残余重拉仍由自动幂等去重。
A new leader refreshes a stale snapshot before seeking; any residual refetch is still deduplicated by auto-idempotency.

## 边界与参考 / Limits and References

当前不内置 Pulsar、NATS 或 RocketMQ 后端，也不支持单组多 partition fan-out。需要分片时使用多个 Raft 组，每组独立 topic。
No Pulsar, NATS, or RocketMQ backend is bundled, and per-group multi-partition fan-out is unsupported. Shard with multiple Raft groups and one topic per group.

- [`CommandIngestor` 源码与 rustdoc 注释 / source and rustdoc comments](https://github.com/gardenias/rr/blob/main/crates/rr-core/src/ingestor.rs)
- [框架幂等 / Framework Idempotency](idempotency.md)
- [Service Trait 指南 / Service Trait Guide](service-trait.md)
- [ADR A-0001](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0001-command-ingestor-abstraction.md)
