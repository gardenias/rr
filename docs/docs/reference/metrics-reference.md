---
sidebar_position: 7
---

# Metrics 参考 / Metrics Reference

`rr` 框架通过 `metrics` crate（v0.24）内部记录指标，并通过 `rr-server` 中的 Prometheus exporter 对外暴露。
通过 `NodeConfig::with_metrics_port(port)` 启用后，指标将以 Prometheus 文本格式在
`http://<host>:<metrics_port>/metrics` 上提供。

---

The `rr` framework records metrics internally using the `metrics` crate (v0.24) and exposes them
via the Prometheus exporter in `rr-server`.
After enabling with `NodeConfig::with_metrics_port(port)`, metrics are served in Prometheus text
format at `http://<host>:<metrics_port>/metrics`.

## 启用 Metrics 端口 / Enabling the Metrics Port

```rust
let config = NodeConfig::new(node_id, listen_addr, storage, config_provider, sm_factory)
    .with_metrics_port(62001); // Prometheus HTTP 端口 / Prometheus HTTP port
```

验证端点 / Verify the endpoint:

```bash
curl http://127.0.0.1:62001/metrics
```

---

## 命名惯例 / Naming Conventions

rr 当前混用两种命名风格，将来版本会统一：

rr currently mixes two naming styles; future versions will unify them:

- **`rr_` 前缀 + 下划线**（Prometheus 推荐）/ `rr_` prefix + underscores (Prometheus idiomatic): `rr_proposal_duration_seconds`, `rr_raft_current_term`, `rr_channel_reconnects_total`
- **点分隔**（老式）/ Dot-separated (legacy): `router.route.not_found`, `node.started`, `raft.group.start`

Prometheus 会把点自动转换为下划线，所以在 Prometheus 查询时两种都写成 `router_route_not_found` 等。但为了**稳定的指标契约**，新代码应使用 `rr_` 前缀。

Prometheus auto-converts dots to underscores, so either style queries as `router_route_not_found` etc. in Prometheus. For a **stable metric contract**, new code should use the `rr_` prefix style.

---

## 指标列表 / Metric Inventory

所有指标均通过 `metrics` crate 在运行时由 Prometheus exporter 收集。

All metrics are collected at runtime via the `metrics` crate and scraped by the Prometheus exporter.

### Histograms (直方图)

所有 histogram 以原生 Prometheus histogram（`_bucket` 序列）导出，可直接用于
`histogram_quantile()` 与跨实例聚合；bucket 边界在 `rr-server/src/node.rs` 的
recorder 安装处配置（`*_seconds` 使用统一的延迟 bucket 组）。

All histograms are exported as native Prometheus histograms (`_bucket` series), ready for
`histogram_quantile()` and cross-instance aggregation; bucket boundaries are configured where
the recorder is installed in `rr-server/src/node.rs` (all `*_seconds` metrics share one
latency bucket set).

---

#### `rr_proposal_duration_seconds`

记录一次 Raft `client_write` 的耗时（秒）：从提交提案到 quorum 提交并 apply
完成。`source` 标签区分调用路径：`client` = dispatch 的客户端单条写（含
`group_manager.write()`）；`batch` = ingestor 的 `write_batch()`（一次往返约
100 条命令，形状不同，勿与 client 混算均值）。leader 转向（ForwardToLeader）
不计入——请求从未到达 raft。

---

Records one Raft `client_write` duration in seconds: from proposal submission to
quorum commit + apply. The `source` label distinguishes call paths: `client` =
single client writes via dispatch (incl. `group_manager.write()`); `batch` =
ingestor `write_batch()` (~100 commands per round trip — a different shape; do not
mix its mean with client writes). Leader redirects (ForwardToLeader) are not
recorded — the request never reached raft.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | `group_id`, `source` (`client` \| `batch`) |
| 记录位置 / Recorded in | `rr-server/src/dispatch.rs` — `submit_write()`；`rr-raft/src/group_manager.rs` — `write()`, `write_batch()` |

---

#### `rr_replication_batch_entries`

记录每次 AppendEntries RPC 中携带的日志条目数量，用于观测批量复制效率。

---

Records the number of log entries per AppendEntries RPC, for observing replication batch efficiency.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/network_factory.rs` — `append_entries()` |

---

#### `rr_raft_append_entries_rtt_seconds`

leader→follower 的 AppendEntries RPC 往返耗时（秒）。`kind` 标签区分心跳
（`heartbeat`，空 entries）与真实复制流量（`append`）——前者是空闲期的网络
基线，后者是复制同步延迟。混沌场景（延迟/丢包注入）下最先抬升的曲线。

---

Leader→follower AppendEntries RPC round-trip time in seconds. The `kind` label splits
heartbeats (`heartbeat`, empty entries) from real replication traffic (`append`) — the
former is the idle network baseline, the latter is replication sync latency. This is the
first curve to rise under chaos (latency/packet-loss injection).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | `group_id`, `follower_node_id`, `kind` (`heartbeat` \| `append`) |
| 记录位置 / Recorded in | `rr-raft/src/network_factory.rs` — `append_entries()` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_server_request_duration_seconds`

服务端客户端协议边界（dispatch）的单请求耗时（秒）。`write` = propose→quorum
commit；`read` = 本地读；`lin_read` = 线性一致读。Kafka ingestor 路径不经过
此边界（直接调 `write_batch`）。

---

Per-request latency at the server's client-protocol boundary (dispatch), in seconds.
`write` = propose→quorum commit; `read` = local read; `lin_read` = linearizable read.
The Kafka ingestor path does NOT cross this boundary (it calls `write_batch` directly).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | `group_id`, `op` (`write` \| `read` \| `lin_read`) |
| 记录位置 / Recorded in | `rr-server/src/dispatch.rs` — `submit_write()` / `submit_read()` / `submit_linearizable_read()` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_raft_apply_duration_seconds`

状态机 apply 段耗时（秒）：幂等层 + 业务 `Service::handle`。每个节点对每条已提交
日志记录一次。是全链路延迟的最内层段：`proposal ≈ replication + apply`。

---

State-machine apply segment duration in seconds: idempotency layer + business
`Service::handle`. Recorded on every node for each committed entry. The innermost
segment of the write latency chain: `proposal ≈ replication + apply`.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/service_adapter.rs` — `apply()` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_ingestor_poll_duration_seconds`

Kafka ingestor 单次 poll 的耗时（秒），含批聚合等待。用于观察摄取循环的健康度。

---

Duration of one ingestor `poll()` call (seconds), including batch-aggregation
wait. Health signal for the ingestion loop.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/ingestor_runner.rs` — poll loop |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_output_send_duration_seconds`

outbox 单条 output 的投递耗时（秒），按投递目标类型分标签。

---

Per-output delivery latency (seconds), labelled by delivery target type.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | `group_id`, `target` (`raft_write` \| `kafka_publish`) |
| 记录位置 / Recorded in | `rr-raft/src/output_runner.rs` — send loop |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_config_etcd_probe_duration_seconds`

etcd 前缀指纹探针（revision 缓存的每调用校验）耗时（秒）。

---

Latency of the etcd prefix-fingerprint probe guarding the revision cache
(seconds).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-config-etcd/src/lib.rs` — `probe_revision()` |
| 可用版本 / Available since | Unreleased (v0.3.0) |

---

### Gauges (仪表盘)

#### `rr_raft_is_leader`

标识当前节点是否为该 Raft 组的 Leader：1 表示是，0 表示否。

---

Indicates whether this node is the leader for the Raft group: 1 = leader, 0 = not leader.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Gauge |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/group_manager.rs` — `write()` |

---

#### `rr_raft_current_term`

当前 Raft 任期编号。

---

Current Raft term number.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Gauge |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/group_manager.rs` — `write()` |

---

#### `rr_raft_last_log_index`

最新日志条目的索引。

---

Index of the most recent log entry.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Gauge |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/group_manager.rs` — `write()` 和 `rr-raft/src/metrics_ticker.rs` — 周期性发射 / `write()` and periodic ticker |

---

#### `rr_raft_replication_lag`

每个 follower 相对 leader 的 log index 差距（`leader.last_log_index -
follower.matched.index`）。**仅在 leader 上发射**——follower 不会发射
该指标，避免 Prometheus 误以为"lag = 0"。告警时务必 predicate 在
`rr_raft_is_leader{group_id} == 1`。

---

For each follower, the gap between leader's `last_log_index` and the
follower's matched index. **Emitted only on the leader** —
followers do NOT emit this gauge to avoid Prometheus reading the
absence as "lag is 0". Always predicate alerts on
`rr_raft_is_leader{group_id} == 1`.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Gauge |
| 标签 / Labels | `group_id`, `follower_node_id` |
| 记录位置 / Recorded in | `rr-raft/src/metrics_ticker.rs` — 5s 周期性发射 / 5s periodic ticker |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_idempotency_cache_size`

幂等层 LRU 响应缓存的当前条数。接近容量上限时重放命中会退化为 `Expired`。

---

Current occupancy of the idempotency layer's LRU response cache. Near the cap,
replays degrade into `Expired`.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Gauge |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/service_adapter.rs` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_output_pending`

outbox 待投递条数（watermark 之后）。持续增长 = 投递卡滞。

---

Outbox backlog depth (entries past the watermark). Sustained growth means
delivery is stuck.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Gauge |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/output_runner.rs` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_security_insecure_mode`

`allow_insecure=true` 启动时置 1。生产环境应配置告警（`== 1` 即报警）。

---

Set to 1 when the node runs with `allow_insecure=true`. Alert on `== 1` in
production deployments.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Gauge |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-server/src/node.rs` — startup |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_raft_pending_deferred`

deferred（两阶段）apply 当前挂起的客户端响应数；有上限（10 000），溢出见
`rr_raft_pending_deferred_overflow_total`。

---

Currently parked deferred (two-phase) client responses; bounded (10 000) —
overflow is counted by `rr_raft_pending_deferred_overflow_total`.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Gauge |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/sm_adapter.rs` |
| 可用版本 / Available since | Unreleased (v0.3.0) |

---

### Counters (计数器)

#### `rr_snapshot_installs_total`

快照安装（状态机恢复）的总次数，通常发生在节点落后后通过快照追赶时。

---

Total number of snapshot installs (state machine restores), typically occurring when
a node catches up via snapshot after falling behind.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/service_adapter.rs` — `restore()` |

---

#### `rr_channel_reconnects_total`

因 gRPC 连接不可用而主动驱逐并重连的次数，用于监控连接健康状况。

---

Total number of channel evictions due to gRPC `UNAVAILABLE` errors, indicating
how often the connection pool detects and recovers from stale connections.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `node_id` |
| 记录位置 / Recorded in | `rr-raft/src/network_factory.rs` — `append_entries()`, `vote()`, `install_snapshot()` |

---

#### `rr_server_requests_total`

服务端客户端协议边界的请求总数，`op` 区分操作类型，`outcome` 区分结果
（`redirect` = 非 leader，返回 leader 地址给客户端）。`rate()` 即按操作
类型的 TPS。

---

Total requests at the server's client-protocol boundary, with `op` for the operation
kind and `outcome` for the result (`redirect` = not leader; the client gets the leader
address when known, or a retriable `Unavailable` when the node is mid-election and
cannot name one). `rate()` gives TPS by operation type.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id`, `op` (`write` \| `read` \| `lin_read`), `outcome` (`success` \| `redirect` \| `error` \| `idempotency_expired`) |
| 记录位置 / Recorded in | `rr-server/src/dispatch.rs` — `submit_write()` / `submit_read()` / `submit_linearizable_read()` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_ingestor_commands_committed_total`

Kafka ingestor 路径经 Raft 持久化提交的命令总数（按批内命令数递增）。
`rate()` 即 Kafka 链路 TPS，可与 producer 发送速率、应用层计数器三方对账。

---

Total commands durably committed via Raft on the Kafka ingestor path (incremented by
batch command count). `rate()` is the Kafka-path TPS, cross-checkable against the
producer's send rate and application-level counters.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/ingestor_runner.rs` — poll/write loop |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_raft_applied_entries_total`

状态机成功 apply 的日志条目总数，每个节点各自发射。`rate()` 即精确的集群写入
TPS——不受 InstallSnapshot 阶跃影响，换主时无 series 断档（新 leader 继续发射）。
集群口径用 `max by (group_id) (rate(...[1m]))`。

---

Total log entries successfully applied by the state machine, emitted independently on
every node. `rate()` is the exact cluster write TPS — immune to InstallSnapshot steps
and leader-switch series gaps (the new leader keeps emitting). For a cluster-level
value use `max by (group_id) (rate(...[1m]))`.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/service_adapter.rs` — `apply()` |
| 可用版本 / Available since | v0.2.0 |

---

#### `node.started`

节点启动完成（所有子系统 wired up，gRPC 服务已绑定）时 +1。

---

Incremented by 1 when the node finishes startup (all subsystems wired, gRPC servers bound).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-server/src/node.rs` — `start()` |

---

#### `node.shutdown`

节点优雅关闭开始时 +1。

---

Incremented by 1 when graceful shutdown begins on the node.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-server/src/node.rs` — `shutdown()` |

---

#### `node.group.started`

`RrNode` 响应 `RoutingTableChange::PartitionAdded` / `GroupMembershipChanged` 事件成功启动一个本地 Raft group 时 +1。

---

Incremented by 1 when `RrNode` successfully starts a local Raft group in response to a
`RoutingTableChange::PartitionAdded` or `GroupMembershipChanged` event.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-server/src/node.rs` — config watch loop |

---

#### `raft.group.start`

`RaftGroupManager::start_group()` 或 `init_cluster()` 成功启动一个 Raft group 时 +1。区别于 `node.group.started`：此指标在较低层（GroupManager 本身）发出，无论触发方是 config watch 还是直接调用。

---

Incremented by 1 whenever `RaftGroupManager::start_group()` or `init_cluster()` successfully
starts a Raft group. Emitted at a lower layer (the GroupManager itself) than `node.group.started`
— regardless of whether the caller is the config-watch loop or a direct API call.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `node_id` |
| 记录位置 / Recorded in | `rr-raft/src/group_manager.rs` — `start_group()`, `init_cluster()` |

---

#### `raft.group.stop`

`RaftGroupManager::stop_group()` 优雅停止一个 Raft group 时 +1。

---

Incremented by 1 when `RaftGroupManager::stop_group()` gracefully stops a Raft group.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `node_id` |
| 记录位置 / Recorded in | `rr-raft/src/group_manager.rs` — `stop_group()` |

---

#### `router.route.not_found`

客户端请求携带的 `partition_id` 在 `ShardRouter` 的路由表中找不到时 +1。通常表示路由表陈旧或客户端使用了错误的 partition。

---

Incremented by 1 when an inbound client request carries a `partition_id` that does not
exist in `ShardRouter`'s routing table. Usually indicates a stale routing table or a
client using a wrong partition.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-router/src/router.rs` — `route()` |

---

#### `rr_idempotency_replays_total` / `rr_idempotency_expired_total` / `rr_idempotency_collisions_total`

幂等层判定计数：重放命中（返回缓存响应）/ 因 watermark 过期被拒（`IdempotencyExpired`）/
键冲突（同键不同负载，响亮拒绝）。持续增长碰撞率 = 客户端键生成有 bug。

---

Idempotency-layer decisions: replay hits (cached response served) / requests
refused as expired below the watermark (`IdempotencyExpired`) / key collisions
(same key, different payload — loudly rejected). A growing collision rate means
a client key-generation bug.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/service_adapter.rs` — keyed apply path |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_learner_promotions_total` / `rr_learner_promotion_failures_total`

learner 自动提升为 voter 的成功/失败计数（`learner_promoter`，仅 leader 发射）。

---

Learner auto-promotion successes/failures (`learner_promoter`; leader-only).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/learner_promoter.rs` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_snapshot_build_failures_total`

快照构建失败计数（fail-closed poisoned-builder 路径，ADR A-0011）。出现即查日志。

---

Snapshot build failures (the fail-closed poisoned-builder path, ADR A-0011).
Investigate logs when nonzero.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-raft/src/sm_adapter.rs` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_framing_decode_failures_total`

字节信封解码失败计数，按 `kind` 分（`framed_entry_body` / `framed_snapshot_*` /
`apply_outcome_*` / `batch_*`）。非零 = 有不兼容或损坏的写入方。

---

Envelope decode failures by `kind` (`framed_entry_body` / `framed_snapshot_*` /
`apply_outcome_*` / `batch_*`). Nonzero means an incompatible or corrupt writer.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `kind` |
| 记录位置 / Recorded in | `rr-core/src/{framed_entry,framed_snapshot,idempotency,batch}.rs` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_ingestor_seed_failures_total` / `rr_ingestor_seek_total` / `rr_ingestor_offset_map_failures_total` / `rr_ingestor_step_down_drops_total` / `rr_ingestor_seed_snapshot_refreshes_total`

ingestor 生命周期计数：种子 offset 失败（fail-closed，致命）/ seek 次数 /
offset→request_id 映射失败（fail-closed）/ 下台丢弃的批尾命令 / 种子快照刷新次数。

---

Ingestor lifecycle counters: seed-offset failures (fail-closed, fatal) / seeks /
offset→request-id mapping failures (fail-closed) / batch-tail commands dropped
on step-down / seed snapshot refreshes.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/ingestor_runner.rs` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_ingestor_seek_below_watermark_total` / `rr_ingestor_seek_above_watermark_total`

Kafka seek 目标超出 broker 水位线的计数（below = 数据已被保留期删除；above =
持久 offset 超过 topic 末尾，fail-closed 报错——疑似 topic 重建）。

---

Seek target outside broker watermarks (below = retention already deleted data;
above = persisted offset past the topic end, fail-closed — likely a recreated
topic).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-ingestor-kafka/src/backend.rs` — `seek()` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_ingestor_encode_failures_total`

ingestor 消息编码为 FramedEntry 失败的计数（fail-closed：supervisor 退出，
不推进 offset）。非零需要人工介入。

---

Ingestor message→FramedEntry encode failures (fail-closed: the supervisor exits
without advancing the offset). Nonzero requires operator attention.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/ingestor_runner.rs` |
| 可用版本 / Available since | Unreleased (v0.3.0) |

---

#### `rr_output_send_total` / `rr_output_send_failures_total`

outbox 投递成功/失败计数，按目标类型分。失败会持续退避重试（at-least-once）。

---

Outbox delivery successes/failures by target type. Failures retry with backoff
(at-least-once).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id`, `target` (`raft_write` \| `kafka_publish`) |
| 记录位置 / Recorded in | `rr-raft/src/output_runner.rs` |
| 可用版本 / Available since | v0.2.0 |

---

#### `rr_raft_pending_deferred_overflow_total` / `rr_raft_pending_deferred_duplicate_total`

deferred 挂起注册表异常：溢出（新写被拒，客户端收到错误）/ 重复 correlation id
（旧 responder 被覆盖，其客户端收到错误）。两者都应罕见，非零即服务实现有 bug。

---

Deferred-registry anomalies: overflow (new write refused, client gets an error)
/ duplicate correlation id (old responder displaced; its client gets an error).
Both should be rare — nonzero means a `Service` implementation bug.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/sm_adapter.rs` |
| 可用版本 / Available since | Unreleased (v0.3.0) |

---

#### `rr_storage_wal_flush_errors_total`

`ReplicatedOnly` 组提交任务的后台 WAL fsync 失败计数。持续非零 = 磁盘子系统故障。

---

Background WAL-fsync failures of the `ReplicatedOnly` group-commit task.
Sustained nonzero means disk-subsystem failure.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-storage/src/rocks.rs` — group-commit task |
| 可用版本 / Available since | Unreleased (v0.3.0) |

---

#### `rr_config_mem_watch_lagged_total`

`MemConfigProvider`（dev/test 后端）watch 溢出丢弃的事件数。生产后端（etcd）是
背压而非丢弃；此计数在 dev 集群非零 = 消费端滞后。

---

Events dropped on lag by the `MemConfigProvider` (dev/test backend) watch
broadcast. The production etcd backend back-pressures instead of dropping; a
nonzero value in dev clusters means a lagging consumer.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-config/src/mem.rs` |
| 可用版本 / Available since | Unreleased (v0.3.0) |

---

#### `rr_config_etcd_watch_reconnects_total{domain}` / `rr_config_etcd_watch_events_total{domain}` / `rr_config_etcd_watch_compactions_total`

etcd watch 通道健康度：重连次数（按 domain: nodes/partitions/groups）/ 收到的事件 /
因 compaction 触发的全量对账次数。

---

etcd watch-channel health: reconnects (by domain: nodes/partitions/groups) /
events received / full reconciliations triggered by compaction.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `domain`（仅前两个 / first two） |
| 记录位置 / Recorded in | `rr-config-etcd/src/lib.rs` — watch loop |
| 可用版本 / Available since | Unreleased (v0.3.0) |

---

#### `rr_config_etcd_stale_serves_total` / `rr_config_etcd_cache_hits_total` / `rr_config_etcd_cache_misses_total` / `rr_config_etcd_rescans_total` / `rr_config_etcd_bad_keys_total` / `rr_config_etcd_fabricated_groups_total`

etcd revision 缓存行为：探针失败时用缓存应答（stale serve，伴随 warn）/ 缓存命中 /
未命中 / 全量重扫 / 跳过的不良 key / 缺 replicas 时编造的空组（均伴随日志）。

---

etcd revision-cache behaviour: stale serves on probe failure (with a warn) /
cache hits / misses / full rescans / skipped malformed keys / fabricated empty
groups on a missing replicas key (all accompanied by logs).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-config-etcd/src/lib.rs` |
| 可用版本 / Available since | Unreleased (v0.3.0) |

---

### Client-side (rr-client, emitted by applications that embed the SDK)

#### `rr_client_request_duration_seconds`

`RrClient::send_request` 的端到端耗时（含所有重试）。`outcome` 标签区分成功与最终失败。

End-to-end latency of `RrClient::send_request` including all retry attempts. The `outcome` label distinguishes success from terminal failure.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | `group_id`, `outcome` (`success` \| `failure`) |
| 记录位置 / Recorded in | `rr-client/src/lib.rs` — `RrClient::send_request` |

---

#### `rr_client_retries_total`

客户端重试次数。第 0 次尝试不计；只有第 1 次及之后的尝试才递增。

Client retry attempts. The initial attempt is not counted; only attempts ≥ 1 increment.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-client/src/lib.rs` — `RrClient::send_request` |

---

#### `rr_client_leader_cache_hits_total` / `rr_client_leader_cache_misses_total`

客户端 leader cache 的命中/失效次数。失效意味着必须 fallback 到 bootstrap 地址轮询。两者之比反映路由稳定性；缓存命中率持续低（`<80%`）说明 leader 频繁切换或集群不稳。

Hit / miss counts for the client's leader cache. A miss means fallback to round-robin through bootstrap addresses. The ratio reflects routing stability; a sustained low hit rate (`<80%`) signals frequent leader changes or cluster instability.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-client/src/lib.rs` — `RrClient::send_request` |

---

#### `rr_client_batch_flush_total` / `rr_client_batch_entries` / `rr_client_batch_buffer_rejected_total`

客户端批处理（opt-in，默认关）：flush 次数（`trigger` = `size` \| `bytes` \|
`linger` \| `immediate`）/ 每批条数（histogram）/ 缓冲拒绝计数（`limit` =
`entries` \| `bytes`，背压 fail-fast 信号）。

---

Client write batching (opt-in, off by default): flush count (`trigger` =
`size` \| `bytes` \| `linger` \| `immediate`) / entries per batch (histogram) /
buffer rejections (`limit` = `entries` \| `bytes` — the fail-fast backpressure
signal).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter / Histogram / Counter |
| 标签 / Labels | `partition_id`（+ `trigger` / `limit`） |
| 记录位置 / Recorded in | `rr-client/src/batch.rs` |
| 可用版本 / Available since | Unreleased (v0.3.0) |

---

## 全链路延迟分解 / Write-Path Latency Breakdown

一个写请求从客户端发起到交付结果的完整链路，每一段都有对应指标：

The full write-request chain from client send to response, with the metric covering
each segment:

```
client t0 ──network──▶ dispatch t1 ──route/queue──▶ client_write ──quorum──▶ apply ──▶ response
│                      │                              │                     │
│ rr_client_request_   │ rr_server_request_            │ rr_proposal_        │ rr_raft_apply_
│ duration_seconds     │ duration_seconds{op="write"}  │ duration_seconds    │ duration_seconds
│ (SDK 侧 / SDK-side)   │ (dispatch 边界 / boundary)     │ (纯 client_write)    │ (状态机 / SM)
│
└── client_total ≈ network + server_total
```

组合关系 / Composition:

- `server_total ≈ routing_encode + proposal`
- `proposal ≈ replication_quorum + apply`
- `client_total ≈ network + server_total`

**分位数不可相减，均值可组合** / Quantiles do not subtract; means do:

- 构成分析用均值规则 / Use means for composition: `rr:server_request_seconds:mean_1m`,
  `rr:proposal_seconds:mean_1m`, `rr:apply_seconds:mean_1m`（replication/routing 段由减法得出 /
  replication and routing segments derive by subtraction）
- 尾部分析用各段自身分位数 / Use per-segment quantiles for tails:
  `rr:server_request_duration_seconds:p99_1m`, `rr:proposal_duration_seconds:p99_1m`,
  `rr:apply_duration_seconds:p99_1m`, `rr:append_entries_rtt_seconds:p99_1m`

**窗口口径** / Window semantics: 延迟分位数规则一律使用 1m 窗口——分位数只有绑定当前
工况才有意义；长拖尾窗口（如 5m）会在负载停止后重播历史最坏样本（假平台），并在负载
爬升时混合工况。/ Latency quantile rules use 1m windows by design — a percentile is only
meaningful bound to the current regime; long trailing windows (e.g. 5m) replay worst-case
history after load stops (stale plateaus) and mix regimes while load ramps.

**读路径** / Read path: `read`（本地读）与 `lin_read`（线性一致读）不经过 raft 复制，
`rr_server_request_duration_seconds{op}` 的 total 即全链路。/ `read` (local) and
`lin_read` (linearizable) skip raft replication; the `rr_server_request_duration_seconds{op}`
total IS the full chain.

---

## 规划中的指标 / Planned Metrics

原 Phase 3 观测性计划已全部落地：/ The original Phase 3 observability plan has fully landed:

- `rr_raft_replication_lag` — 已实现，见上文 Gauge 表 / Implemented, see the gauge table above (since v0.2.0)
- W3C trace context 传播 — 已实现（`rr-network::tracing` 的 `extract_/attach_traceparent` 助手 + `examples/otel-client` 集成示例）/ Shipped in v0.2.0 via `rr-network::tracing` helpers and the `examples/otel-client` integration example。OpenTelemetry SDK (OTLP exporter) 按设计不内置，由用户自带 / The OTel SDK itself is deliberately not bundled — bring your own OTLP stack.

---

## Prometheus 集成示例 / Prometheus Integration Example

在 `prometheus.yml` 中配置抓取任务：

---

Configure a scrape job in `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'rr-cluster'
    static_configs:
      - targets:
          - '127.0.0.1:62001'  # 节点 1 / Node 1
          - '127.0.0.1:62002'  # 节点 2 / Node 2
          - '127.0.0.1:62003'  # 节点 3 / Node 3
    scrape_interval: 15s
```

## 有用的查询示例 / Useful Query Examples

```promql
# 每秒提案吞吐率 / Proposals per second per group
sum by (group_id) (rate(rr_proposal_duration_seconds_count[1m]))

# p99 提案延迟 / p99 proposal latency
histogram_quantile(0.99, sum by (le, group_id) (rate(rr_proposal_duration_seconds_bucket[5m])))

# 当前集群所有 leader / All current leaders in the cluster
rr_raft_is_leader == 1

# 路由失败速率 / Routing lookup failures per second
rate(router_route_not_found[1m])

# 连接池不稳定性 / Connection-pool churn
sum(rate(rr_channel_reconnects_total[5m])) by (node_id)

# 集群写入 TPS（精确：apply 计数）/ Cluster write TPS (exact: applied entries)
rr:applied_entries_rate_1m

# 按操作类型的请求 TPS / Request TPS by op
rr:server_request_rate_1m:by_op

# Kafka 链路 TPS / Kafka-path TPS
rr:ingestor_commands_rate_1m

# p99 客户端请求延迟（按操作类型，1m 窗口）/ p99 client request latency by op (1m window)
rr:server_request_duration_seconds:p99_1m

# p99 leader→follower 同步 RTT / p99 leader→follower AppendEntries RTT
rr:append_entries_rtt_seconds:p99_1m

# 写延迟构成（均值堆叠）/ Write latency budget (stacked means)
rr:apply_seconds:mean_1m
rr:proposal_seconds:mean_1m - on(group_id) rr:apply_seconds:mean_1m            # replication 段
rr:server_request_seconds:mean_1m{op="write"} - on(group_id) rr:proposal_seconds:mean_1m  # routing/encode 段
```
