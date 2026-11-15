---
title: 框架幂等 / Framework Idempotency
description: Exactly-once apply on top of at-least-once delivery via (request_type, request_id) keys
---

# 框架幂等 / Framework Idempotency

自 v0.2.0 起，客户端可按请求附加 `IdempotencyKey`，把至少一次重试转换为用户 `Service::handle()` 的恰好一次 apply。无键请求保持原路径。迁移见 [v0.1 → v0.2 指南](../deployment/v0.1-to-v0.2-migration.md)，设计见 [ADR A-0003](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0003-framework-idempotency-layer.md)。
Since v0.2.0, clients can attach an `IdempotencyKey` per request to turn at-least-once retries into exactly-once apply at `Service::handle()`. Unkeyed requests keep the existing path. See the [migration guide](../deployment/v0.1-to-v0.2-migration.md) and [ADR A-0003](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0003-framework-idempotency-layer.md).

## 为什么需要去重 / Why Deduplication Is Needed

写可能已在 leader 提交，但响应因网络或 leader 切换而丢失。SDK 重试后，无键请求可能再次执行 `Service::handle()`。Kafka 在 leader 切换后也可能重新拉取消息。
A write may commit on the leader before its response is lost to a network failure or leadership change. An SDK retry can then run an unkeyed `Service::handle()` again. Kafka ingestion can likewise refetch messages after leadership changes.

幂等层在 apply 前去重并缓存响应，覆盖这两条重复路径。
The idempotency layer deduplicates before apply and caches responses for both replay paths.

## 请求身份 / Request Identity

```rust
pub struct IdempotencyKey {
    pub request_type: String,        // 命名空间 / namespace
    pub request_id: u64,             // 命名空间内单调 ID / monotonic ID within the namespace
    pub client_id: Option<String>,   // 仅诊断，不参与去重 / diagnostic only; not part of dedup
}
```

去重身份只有 `(request_type, request_id)`。相同键返回缓存响应；不同 `client_id` 复用同一键仍返回该响应，并记录 `op="idempotency_collision"` 审计警告。
Deduplication uses only `(request_type, request_id)`. The same key returns its cached response; reuse with another `client_id` still returns that response and records an `op="idempotency_collision"` audit warning.

每个 `request_type` 独立维护最大已应用 `request_id` 水位。缓存已淘汰且不高于水位的旧请求返回 `Expired`，不会静默重放。
Each `request_type` has an independent high-watermark for the largest applied `request_id`. An evicted request at or below that watermark returns `Expired` instead of replaying silently.

每个命名空间的首个请求即使 `request_id == 0` 也为 `New`，因此 Kafka offset 0 可正常使用。
The first request in a namespace is `New` even when `request_id == 0`, so Kafka offset zero is valid.

## Apply 决策 / Apply Decisions

| 结果 / Outcome | 条件 / Condition | 行为 / Behavior |
| :--- | :--- | :--- |
| `New` | 缓存未命中，且无水位或 ID 高于水位 / Cache miss with no watermark or an ID above it | 执行 `Service::handle()` 并缓存响应 / Run the handler and cache the response |
| `Replay(bytes)` | 缓存命中且 `client_id` 相同 / Cache hit with the same client ID | 返回缓存，不执行 handler / Return cached bytes without running the handler |
| `Collision` | 缓存命中但 `client_id` 不同 / Cache hit with another client ID | 返回缓存并记录警告 / Return cached bytes and log a warning |
| `Expired` | 缓存未命中且 ID 不高于水位 / Cache miss with an ID at or below the watermark | 返回 `RrError::IdempotencyExpired` / Return an expiry error |

## 客户端用法 / Client Usage

```rust
use rr_client::RrClient;
use rr_core::idempotency::IdempotencyKey;

let client = RrClient::connect(vec!["node1:50051".to_string()]).await?;

// 无键：重试可能重复 apply / Unkeyed: retries may apply twice.
let response = client.send_request(group_id, payload.clone()).await?;

// 有键：重试返回原响应 / Keyed: retries return the original response.
let key = IdempotencyKey::new("transfer", request_id_from_app);
let response = client.send_request_idempotent(group_id, payload, key).await?;
```

默认每组缓存 10,000 条。响应淘汰且水位已越过该 ID 时，客户端收到 `IdempotencyExpired`；不要盲目用同一 ID 继续重试。
The default cache holds 10,000 entries per group. Once a response is evicted and the watermark has advanced, the client receives `IdempotencyExpired`; do not keep retrying blindly with the same ID.

`request_id` 必须在 `request_type` 内单调分配。可持久化每进程计数器、使用外部协调器，或在 Kafka 输入中直接使用 offset。进程重启后，未持久化计数器可能重用键并返回旧响应。
Allocate `request_id` monotonically within each `request_type`, using a persisted process counter, an external coordinator, or Kafka offsets. An unpersisted counter can reuse keys after restart and return old responses.

## Kafka 自动幂等 / Kafka Auto-Idempotency

框架可从 Kafka offset 自动构造键：
The framework can derive keys from Kafka offsets:

```rust
use rr_ingestor_kafka::{kafka_offset_to_request_id, KafkaIngestor, KafkaIngestorConfig};
use rr_raft::ingestor_runner::{AutoIdempotency, OffsetMapError};

let cfg = KafkaIngestorConfig {
    bootstrap_servers: vec!["broker1:9092".into()],
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

`request_type` 必须对每条输入流唯一，通常使用 topic 名。offset 映射必须单调；`kafka_offset_to_request_id` 对解码错误和负 sentinel fail-closed，supervisor 退出而不会提交该消息。
`request_type` must be unique per input stream, normally using the topic name. Offset mapping must be monotonic. `kafka_offset_to_request_id` fails closed on decode errors and negative sentinel offsets; the supervisor exits without proposing the offending message.

step-down、快照 seek 落后或网络重试造成的重复拉取，都会由同一键去重。
Refetches caused by step-down, snapshot seek lag, or network retries are deduplicated by the same key.

## 持久化与容量 / Persistence and Capacity

快照的 `framework_state.idempotency` 同时保存水位和 LRU 缓存。leader 切换与节点重启通过日志重放和快照恢复延续去重状态。
The snapshot's `framework_state.idempotency` stores both watermarks and the LRU cache. Leadership changes and restarts preserve deduplication through log replay and snapshot restoration.

可用 `ServiceStateMachineAdapter::with_idempotency_capacity` 调整每组容量。缓存应覆盖最长预期重试窗口，否则正常重试会变为 `IdempotencyExpired`。
Tune per-group capacity with `ServiceStateMachineAdapter::with_idempotency_capacity`. The cache should outlive the longest expected retry window, or valid retries will expire.

## 指标 / Metrics

所有指标带 `group_id` 标签：
All metrics carry a `group_id` label:

| 指标 / Metric | 类型 / Type | 触发 / Trigger |
| :--- | :--- | :--- |
| `rr_idempotency_replays_total` | counter | 同 client 的缓存命中 / Cache hit from the same client |
| `rr_idempotency_collisions_total` | counter | 不同 client 复用键 / Key reused by another client |
| `rr_idempotency_expired_total` | counter | 缓存未命中且 ID 低于水位 / Cache miss below the watermark |
| `rr_idempotency_cache_size` | gauge | apply 后的缓存长度 / Cache length after apply |

非零 collision 通常表示不同客户端的 ID 分配发生冲突，应检查协调逻辑。
A nonzero collision count usually indicates conflicting ID allocation between clients; inspect coordination.

## 不适用场景 / When Not to Use

只读请求无需写去重。可接受至少一次语义的遥测等写入可使用无键路径，避免缓存开销。业务身份超过 `(type, id)` 时，应在 `Service::handle()` 内实现业务去重。
Reads do not need write deduplication. Telemetry and other writes that tolerate at-least-once semantics can use the unkeyed path to avoid cache overhead. If business identity requires more than `(type, id)`, implement deduplication in `Service::handle()`.

## 相关文档 / Related Documentation

- [Service Trait 指南 / Service Trait Guide](service-trait.md)
- [Kafka 输入 / Kafka Input](kafka-input.md)
- [客户端 SDK / Client SDK](client-sdk.md)
- [ADR A-0003](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0003-framework-idempotency-layer.md)
