---
sidebar_position: 7
---

# 指标参考 / Metrics Reference

Rr 使用 `metrics` crate（v0.24）记录指标，由 `rr-server` 的 Prometheus exporter 暴露。通过
`NodeConfig::with_metrics_port(port)` 启用后，端点为 `http://<host>:<metrics_port>/metrics`。
Rr records metrics with the `metrics` crate (v0.24) and exposes them through `rr-server`'s Prometheus
exporter. After `NodeConfig::with_metrics_port(port)`, scrape `http://<host>:<metrics_port>/metrics`.

## 启用与验证 / Enable and Verify

```rust
let config = NodeConfig::new(node_id, listen_addr, storage, config_provider, sm_factory)
    .with_metrics_port(62001); // Prometheus HTTP 端口 / Prometheus HTTP port
```

```bash
curl http://127.0.0.1:62001/metrics
```

## Console 实时与历史 / Console Live and History

Console BFF 抓取上述逐节点 `/metrics`，将实时序列保存在有界内存中；重启会清空数据，时间窗只筛选已留存数据，
不会回填 Console 启动前的历史。Prometheus 不是实时功能的必需依赖；成对配置 `--prometheus-url` 与
`--prometheus-cluster` 后，Console 才可查询固定 raw-family 历史模板，不开放任意 PromQL。详见
[Cluster Admin Console › 留存、新鲜度与规模](../operations/cluster-admin-console.md#留存新鲜度与规模--retention-freshness-and-scale)
中的留存语义。
The Console BFF scrapes the per-node `/metrics` endpoints above and retains live series in bounded memory. Restarting
clears this data; time windows only filter retained observations and never backfill history from before Console startup.
Prometheus is not required for live data. Pairing `--prometheus-url` with `--prometheus-cluster` enables fixed raw-family
history templates, not arbitrary PromQL. See [Cluster Admin Console › Retention, Freshness, and
Scale](../operations/cluster-admin-console.md#留存新鲜度与规模--retention-freshness-and-scale).

## 命名惯例 / Naming Conventions

Rr 当前混用两种命名风格，未来版本会统一 / Rr currently mixes two naming styles; future versions will
unify them:

- `rr_` 前缀 + 下划线（Prometheus 推荐）/ `rr_` prefix with underscores (recommended by Prometheus):<br />
  `rr_proposal_duration_seconds`, `rr_raft_current_term`, `rr_channel_reconnects_total`
- 点分隔（legacy）/ Dot-separated legacy names:<br />
  `router.route.not_found`, `node.started`, `raft.group.start`

Prometheus 会把点转换为下划线，例如 `router.route.not_found` 查询为 `router_route_not_found`。
为保持稳定的指标契约，新代码应使用 `rr_` 前缀。
Prometheus converts dots to underscores, so `router.route.not_found` is queried as
`router_route_not_found`. New code should use the `rr_` prefix for a stable metric contract.

## 服务端指标 / Server-Side Metrics

所有指标由 Prometheus exporter 在 scrape 时收集。histogram 以原生 Prometheus histogram（`_bucket`
序列）导出，可用于 `histogram_quantile()` 与跨实例聚合；bucket 边界在 `rr-server/src/node.rs` 的
recorder 安装处配置，`*_seconds` 共用延迟 bucket 组。
The Prometheus exporter collects all metrics at scrape time. Histograms are exported as native Prometheus
histograms (`_bucket` series) for `histogram_quantile()` and cross-instance aggregation. Bucket boundaries
are configured where the recorder is installed in `rr-server/src/node.rs`; `*_seconds` metrics share one
latency bucket set.

### 直方图 / Histograms

#### `rr_proposal_duration_seconds`

`client_write` 从提案到 quorum commit + apply 的秒数；Leader 重定向不计。`source=client` 为 dispatch
单写，`batch` 为 ingestor `write_batch`（约 100 条/往返），勿混算均值。
Proposal-to-commit-and-apply seconds; redirects excluded. `client` is single dispatch writes; `batch` is
ingestor batches (~100 commands/round trip), so do not mix their means.

- **标签 / Labels**：`group_id`, `source` (`client` | `batch`)
- **位置 / Recorded in**：`rr-server/src/dispatch.rs` `submit_write()`；`rr-raft/src/group_manager.rs`
  `write()`, `write_batch()`

#### `rr_replication_batch_entries`

每次 AppendEntries 携带的日志条目数，用于观测批量复制效率。
Log entries per AppendEntries RPC, for observing replication batch efficiency.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/network_factory.rs` `append_entries()`

#### `rr_raft_append_entries_rtt_seconds`

Leader→follower AppendEntries RTT；`heartbeat` 为空 entries，`append` 为真实复制流量。
Leader→follower RTT; `heartbeat` has no entries and `append` carries replication traffic.

- **标签 / Labels**：`group_id`, `follower_node_id`, `kind` (`heartbeat` | `append`)
- **位置 / Recorded in**：`rr-raft/src/network_factory.rs` `append_entries()`
- **版本 / Since**：v0.2.0

#### `rr_server_request_duration_seconds`

dispatch 边界单请求秒数；`write` = propose→quorum commit，`read` = 本地读，`lin_read` = 线性一致读；
Kafka ingestor 不经过此边界。
Dispatch-boundary request seconds. Kafka ingestor bypasses this boundary and calls `write_batch` directly.

- **标签 / Labels**：`group_id`, `op` (`write` | `read` | `lin_read`)
- **位置 / Recorded in**：`rr-server/src/dispatch.rs` `submit_write()` / `submit_read()` /
  `submit_linearizable_read()`
- **版本 / Since**：v0.2.0

#### `rr_raft_apply_duration_seconds`

状态机 apply 秒数：幂等层 + `Service::handle`，每个节点逐条记录；是写延迟链的最内层。
State-machine apply seconds: idempotency + `Service::handle`, recorded per entry on every node. This is the
innermost write-latency segment.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/service_adapter.rs` `apply()`
- **版本 / Since**：v0.2.0

#### `rr_ingestor_poll_duration_seconds`

ingestor 单次 poll 秒数，含批聚合等待。
One ingestor poll in seconds, including batch-aggregation wait.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/ingestor_runner.rs` poll loop
- **版本 / Since**：v0.2.0

#### `rr_output_send_duration_seconds`

outbox 单条 output 投递秒数，按目标类型分标签。
Per-output outbox delivery seconds, labelled by target type.

- **标签 / Labels**：`group_id`, `target` (`raft_write` | `kafka_publish`)
- **位置 / Recorded in**：`rr-raft/src/output_runner.rs` send loop
- **版本 / Since**：v0.2.0

#### `rr_config_etcd_probe_duration_seconds`

etcd 前缀指纹探针秒数，用于校验 revision 缓存。
Etcd prefix-fingerprint probe seconds for revision-cache validation.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-config-etcd/src/lib.rs` `probe_revision()`
- **版本 / Since**：v1.0.0

#### `rr_formation_duration_seconds`

formation reconcile 墙钟秒数，成功失败均记录；首次 genesis reconcile 即集群成形时间。
Formation-reconcile wall seconds on success and failure; the first genesis reconcile is cluster formation
time.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-server/src/formation.rs` `FormationSupervisor::reconcile()`
- **版本 / Since**：v1.0.0

#### `rr_snapshot_transfer_duration_seconds`

`send` 为开始流式发送到对端确认的成功传输时间；`recv` 为 fail-closed 落盘 + 状态机恢复时间，启动时本地
SnapshotStore 重建也计入（与 `rr_snapshot_installs_total` 同口径）。
`send` covers successful streaming through acknowledgement; `recv` covers fail-closed persistence plus
restore and includes startup rebuilds from the local SnapshotStore, matching `rr_snapshot_installs_total`.

- **标签 / Labels**：`direction` (`send` | `recv`), `group_id`
- **位置 / Recorded in**：`send`: `rr-raft/src/network_factory.rs` `full_snapshot()`; `recv`: `rr-raft/src/sm_adapter.rs` `install_snapshot()`
- **版本 / Since**：v1.0.0

### 仪表盘 / Gauges

#### `rr_raft_is_leader`

当前节点是否为该组 Leader：1 是，0 否。
Whether this node leads the group: 1 yes, 0 no.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/group_manager.rs` `write()`

#### `rr_raft_current_term`

当前 Raft 任期编号。
Current Raft term number.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/group_manager.rs` `write()`

#### `rr_raft_last_log_index`

最新日志条目索引。
Index of the latest log entry.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/group_manager.rs` `write()` 与 `rr-raft/src/metrics_ticker.rs`
  周期发射 / `rr-raft/src/group_manager.rs` `write()` and the periodic ticker in
  `rr-raft/src/metrics_ticker.rs`

#### `rr_raft_replication_lag`

`leader.last_log_index - follower.matched.index`，仅 Leader 发射；follower 不发射，避免把缺席误读为
lag=0。告警必须同时谓词 `rr_raft_is_leader{group_id} == 1`。
Leader-minus-follower matched index, emitted only on leaders so absence is not mistaken for zero lag.
Predicate alerts on `rr_raft_is_leader{group_id} == 1`.

- **标签 / Labels**：`group_id`, `follower_node_id`
- **位置 / Recorded in**：`rr-raft/src/metrics_ticker.rs`，5s 周期 / `rr-raft/src/metrics_ticker.rs`, 5s ticker
- **版本 / Since**：v0.2.0

#### `rr_idempotency_cache_size`

幂等 LRU 当前条数；接近容量上限时重放会退化为 `Expired`。
Current idempotency LRU occupancy; near the cap, replays degrade to `Expired`.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/service_adapter.rs`
- **版本 / Since**：v0.2.0

#### `rr_output_pending`

watermark 后的 outbox 待投递条数；持续增长表示投递卡滞。
Outbox entries past the watermark; sustained growth means delivery is stuck.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/output_runner.rs`
- **版本 / Since**：v0.2.0

#### `rr_security_insecure_mode`

`allow_insecure=true` 时为 1；生产环境应对 `== 1` 告警。
Set to 1 with `allow_insecure=true`; alert on `== 1` in production.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-server/src/node.rs` startup
- **版本 / Since**：v0.2.0

#### `rr_raft_pending_deferred`

当前挂起的 deferred 客户端响应数，上限 10 000；溢出见
`rr_raft_pending_deferred_overflow_total`。

Currently parked deferred responses, capped at 10,000; see `rr_raft_pending_deferred_overflow_total` for
overflow.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/sm_adapter.rs`
- **版本 / Since**：v1.0.0

#### `rr_formation_state`

每组 formation 状态：0 = Forming（已初始化、未到激活条件），1 = Active；加载/创建 lifecycle 记录时发布，
Forming→Active 时更新。
Per-group state: 0 = Forming (initialized but not active), 1 = Active; published on lifecycle load/create
and updated on Forming→Active.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-server/src/formation.rs` `FormationSupervisor`
- **版本 / Since**：v1.0.0

### 计数器 / Counters

#### `rr_snapshot_installs_total`

快照安装/状态机恢复总次数，通常用于节点落后后的快照追赶。
Snapshot installs/state-machine restores, typically for catch-up after falling behind.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/service_adapter.rs` `restore()`

#### `rr_snapshot_transfer_bytes`

`send` 统计交给传输层的字节，含取消/超时前已发送部分；`recv` 统计成功安装载荷，含启动时本地
SnapshotStore 重建（与 `rr_snapshot_installs_total` 同口径）。
`send` counts bytes handed to transport, including partial sends on cancellation/timeout; `recv` counts
successfully installed payload, including startup rebuilds, matching `rr_snapshot_installs_total`.

- **标签 / Labels**：`direction` (`send` | `recv`), `group_id`
- **位置 / Recorded in**：`send`: `rr-raft/src/network_factory.rs` `full_snapshot()`; `recv`: `rr-raft/src/sm_adapter.rs` `install_snapshot()`
- **版本 / Since**：v1.0.0

#### `rr_formation_cas_total`

lifecycle store CAS：`applied` 成功，`conflict` 竞争失败后重载重试；持续不收敛表示 seed/roster 不一致或
store 分区。
CAS outcomes: `applied` succeeded; `conflict` reloads and retries. Persistent conflicts indicate divergent
seeds/rosters or a partitioned store.

- **标签 / Labels**：`result` (`applied` | `conflict`)
- **位置 / Recorded in**：`rr-server/src/formation.rs` 的 `create_if_absent` 和 `compare_and_swap` 调用点 /
  `create_if_absent` and `compare_and_swap` call sites in `rr-server/src/formation.rs`
- **版本 / Since**：v1.0.0

#### `rr_admin_membership_changes_total` / `rr_admin_transfer_leader_total` / `rr_admin_snapshot_trigger_total`

Admin 操作结果，每 RPC 恰好一次，含校验/前置条件拒绝。membership 的 `op` 为
`change_membership` | `remove_group_member` | `add_learner`；`result` 均为 `ok` | `error`。
Exactly one outcome per Admin RPC, including validation/precondition errors. Membership uses the listed
`op`; all use `result=ok|error`.

- **标签 / Labels**：membership 为 `op`, `result`；其余两个为 `result` / Membership has `op` and
  `result`; the other two have `result`
- **位置 / Recorded in**：`rr-server/src/admin.rs` RPC wrappers
- **版本 / Since**：v1.0.0

#### `rr_channel_reconnects_total`

gRPC `UNAVAILABLE` 后主动连接驱逐与重连次数。
Channel evictions and reconnects after gRPC `UNAVAILABLE`.

- **标签 / Labels**：`node_id`
- **位置 / Recorded in**：`rr-raft/src/network_factory.rs` `append_entries()`, `vote()`,
  `install_snapshot()`

#### `rr_server_requests_total`

dispatch 边界请求数；`rate()` 为按 op 的 TPS。`redirect` 为非 Leader；`idempotency_expired` /
`idempotency_namespace_limit` 为 keyed write 类型化拒绝，条目正常 apply，不走 group-fatal apply error。
Dispatch requests; `rate()` gives TPS by op. Typed idempotency rejections apply cleanly and never become
group-fatal apply errors.

- **标签 / Labels**：`group_id`, `op` (`write` | `read` | `lin_read`), `outcome` (`success` | `redirect` |
  `error` | `idempotency_expired` | `idempotency_namespace_limit`)
- **位置 / Recorded in**：`rr-server/src/dispatch.rs` `submit_write()` / `submit_read()` /
  `submit_linearizable_read()`
- **版本 / Since**：v0.2.0

#### `rr_ingestor_commands_committed_total`

Kafka ingestor 经 Raft 提交的命令数，按批内命令数递增；`rate()` 为 Kafka TPS，可与 producer 与应用计数
对账。
Commands committed through Raft by the Kafka ingestor, incremented per batched command; `rate()` is
Kafka-path TPS and can be reconciled with producer and application counters.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/ingestor_runner.rs` poll/write loop
- **版本 / Since**：v0.2.0

#### `rr_raft_applied_entries_total`

各节点成功 apply 的日志条目数；`rate()` 为精确集群写入 TPS，不受快照阶跃或换主 series 断档影响。
集群口径用 `max by (group_id) (rate(...[1m]))`。
Successfully applied entries per node. Its rate is exact cluster write TPS, unaffected by snapshot steps or
leader-switch series gaps; use `max by (group_id) (rate(...[1m]))` cluster-wide.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/service_adapter.rs` `apply()`
- **版本 / Since**：v0.2.0

#### `node.started`

节点子系统接线且 gRPC 绑定完成后 +1。
+1 after node wiring and gRPC binding complete.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-server/src/node.rs` `start()`

#### `node.shutdown`

节点优雅关闭开始时 +1。
+1 when graceful node shutdown begins.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-server/src/node.rs` `shutdown()`

#### `node.group.started`

仅在 `RrNode::start()` 初始路由表循环成功启动本地组时 +1；watch 后续启动不重复发射。
+1 only for local groups started by the initial routing-table loop; later watch-loop starts do not emit it.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-server/src/node.rs` `start()` initial routing-table loop

#### `raft.group.start`

`RaftGroupManager::start_group()` 成功时 +1，无论调用方；`init_cluster()` 不发射。
+1 on every successful `start_group()`, regardless of caller; `init_cluster()` does not emit.

- **标签 / Labels**：`node_id`
- **位置 / Recorded in**：`rr-raft/src/group_manager.rs` `start_group()`

#### `raft.group.stop`

`RaftGroupManager::stop_group()` 优雅停止组时 +1。
+1 when `stop_group()` gracefully stops a group.

- **标签 / Labels**：`node_id`
- **位置 / Recorded in**：`rr-raft/src/group_manager.rs` `stop_group()`

#### `router.route.not_found`

请求的 `partition_id` 不在路由表时 +1，通常表示路由表陈旧或客户端 partition 错误。
+1 when a request's `partition_id` is absent, usually indicating a stale table or wrong client partition.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-router/src/router.rs` `route()`

#### `rr_idempotency_replays_total` / `rr_idempotency_expired_total` / `rr_idempotency_collisions_total`

分别为重放命中、低于 watermark 且已逐出而拒绝、同键但 `client_id` 不同。collision 不比较 payload，也不拒绝
请求，而是记录 `rr::audit` 警告并返回首次缓存的成功响应；持续增长表示不同客户端的 ID 分配冲突。
Replay hits, expired rejections below the watermark, and cache hits where the same key arrives with another
`client_id`. A collision does not compare payloads or reject the request; it logs an `rr::audit` warning and
returns the first cached successful response. Growth indicates conflicting ID allocation across clients.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/service_adapter.rs` keyed apply path
- **版本 / Since**：v0.2.0

#### `rr_idempotency_namespace_rejects_total`

组已达 `MAX_IDEMPO_NAMESPACES = 1024` 时拒绝新 `request_type`；作为 apply outcome 干净应用，客户端收到
`NamespaceLimitError`。换新 ID 无效，应复用已有 namespace、提高上限或回收；dispatch 同时记录
`rr_server_requests_total{outcome="idempotency_namespace_limit"}` 并写 `rr::audit` warn。
Rejects a new `request_type` at the 1024-namespace cap as a clean apply outcome and typed
`NamespaceLimitError`. Fresh IDs do not help; reuse, raise, or retire namespaces. Also emits the dispatch
outcome and an audit warning.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/service_adapter.rs` `apply_keyed()`
- **版本 / Since**：v1.0.0

#### `rr_learner_promotions_total` / `rr_learner_promotion_failures_total`

learner 自动提升为 voter 的成功/失败计数，仅 Leader 发射。
Learner auto-promotion successes/failures, emitted only by leaders.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/learner_promoter.rs`
- **版本 / Since**：v0.2.0

#### `rr_snapshot_build_failures_total`

快照构建失败（fail-closed poisoned-builder，ADR A-0011）；非零即查日志。
Snapshot build failures on the fail-closed poisoned-builder path; investigate logs when nonzero.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-raft/src/sm_adapter.rs`
- **版本 / Since**：v0.2.0

#### `rr_framing_decode_failures_total`

信封解码失败；`kind` 为 `framed_entry_body` / `framed_entry_future_version` / `framed_snapshot_body` /
`framed_snapshot_future_version` / `apply_outcome_too_short` / `apply_outcome_bad_tag` /
`apply_outcome_future_version` / `apply_outcome_body`。非零表示写入方不兼容或损坏。无 `batch_*` kind；
批帧在 ingress 无分配校验，不计入此指标。
Envelope decode failures by the listed `kind`; nonzero means an incompatible or corrupt writer. No
`batch_*` kinds exist; ingress validates batch frames without allocation and does not count them here.

- **标签 / Labels**：`kind`
- **位置 / Recorded in**：`rr-core/src/{framed_entry,framed_snapshot,idempotency}.rs`
- **版本 / Since**：v0.2.0

#### `rr_ingestor_seed_failures_total` / `rr_ingestor_seek_total` / `rr_ingestor_offset_map_failures_total` / `rr_ingestor_step_down_drops_total` / `rr_ingestor_seed_snapshot_refreshes_total`

依次为 seed offset 失败（fail-closed、致命）、seek、offset→request_id 映射失败（fail-closed）、下台丢弃
批尾命令、seed 快照刷新。
Fatal fail-closed seed failures, seeks, fail-closed offset-mapping failures, batch-tail commands dropped on
step-down, and seed-snapshot refreshes.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/ingestor_runner.rs`
- **版本 / Since**：v0.2.0

#### `rr_ingestor_seek_below_watermark_total` / `rr_ingestor_seek_above_watermark_total`

seek 低于/高于 broker 水位；below 表示保留期已删数据，above 表示持久 offset 超过 topic 末尾并
fail-closed，疑似 topic 重建。
Seek below/above broker watermarks. Below means retention deleted data; above fails closed because the
persisted offset is past the topic end, suggesting topic recreation.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-ingestor-kafka/src/backend.rs` `seek()`
- **版本 / Since**：v0.2.0

#### `rr_ingestor_encode_failures_total`

消息编码为 FramedEntry 失败；fail-closed，supervisor 退出且不推进 offset，需人工介入。
Message-to-FramedEntry encoding failed; the supervisor fails closed, exits without advancing the offset,
and needs operator attention.

- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-raft/src/ingestor_runner.rs`
- **版本 / Since**：v1.0.0

#### `rr_output_send_total` / `rr_output_send_failures_total`

outbox 投递成功/失败；失败按退避持续重试（at-least-once）。
Outbox delivery successes/failures; failures retry with backoff (at-least-once).

- **标签 / Labels**：`group_id`, `target` (`raft_write` | `kafka_publish`)
- **位置 / Recorded in**：`rr-raft/src/output_runner.rs`
- **版本 / Since**：v0.2.0

#### `rr_raft_pending_deferred_overflow_total` / `rr_raft_pending_deferred_duplicate_total`

deferred 注册表溢出（新写被拒）/ correlation ID 重复（旧 responder 被覆盖，其客户端收到错误）；
非零表示 Service 实现 bug。
Deferred-registry overflow rejects new writes; duplicate correlation IDs displace the old responder and
error its client. Nonzero indicates a Service implementation bug.

- **标签 / Labels**：—；组上下文见 `rr_raft_pending_deferred` / No labels; see `rr_raft_pending_deferred`
  for per-group depth
- **位置 / Recorded in**：`rr-raft/src/sm_adapter.rs`
- **版本 / Since**：v1.0.0

#### `rr_deferred_pending_via_non_deferred_total`

`DeferredApply::Pending` 到达非 deferred `into_response()` 的防御计数；表示 Service 接线错误
（R-2/A-0033），不是客户端故障。框架映射为空成功响应以保持 apply 循环，并记录 warn；该客户端会收到
空成功响应。与注册表容量/重复异常指标不同，本指标守护错误路径。
Defensive count of `Pending` reaching non-deferred `into_response()`, indicating incorrect Service wiring,
not a client fault. The framework returns an empty successful response to keep apply alive and logs a
warning; that client receives the empty success. Unlike registry capacity/duplication metrics, this guards a
wrong-path defense.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-core/src/deferred.rs` `DeferredApply::into_response()`
- **版本 / Since**：v1.0.0

#### `rr_storage_wal_flush_errors_total`

`ReplicatedOnly` 组提交任务的后台 WAL fsync 失败；持续非零表示磁盘子系统故障。
Background WAL-fsync failures in the `ReplicatedOnly` group-commit task; sustained nonzero indicates disk
failure.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-storage/src/rocks.rs` group-commit task
- **版本 / Since**：v1.0.0

#### `rr_config_mem_watch_lagged_total`

`MemConfigProvider`（dev/test）watch 因滞后丢弃的事件数；生产 etcd 使用背压而非丢弃。
Events dropped by lagging `MemConfigProvider` (dev/test) watches; production etcd applies backpressure
instead.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-config/src/mem.rs`
- **版本 / Since**：v1.0.0

#### `rr_config_etcd_watch_reconnects_total{domain}` / `rr_config_etcd_watch_events_total{domain}` / `rr_config_etcd_watch_compactions_total`

etcd watch 重连、收到事件、compaction 触发全量对账次数；domain 为 nodes/partitions/groups。
Etcd watch reconnects, received events, and compaction-triggered full reconciliations; domains are
nodes/partitions/groups.

- **标签 / Labels**：仅前两个有 `domain` / Only the first two have `domain`
- **位置 / Recorded in**：`rr-config-etcd/src/lib.rs` watch loop
- **版本 / Since**：v1.0.0

#### `rr_config_etcd_stale_serves_total` / `rr_config_etcd_cache_hits_total` / `rr_config_etcd_cache_misses_total` / `rr_config_etcd_rescans_total` / `rr_config_etcd_bad_keys_total` / `rr_config_etcd_fabricated_groups_total`

探针失败时缓存应答（stale serve + warn）、缓存命中/未命中、全量重扫、跳过不良 key、缺 replicas 时编造
空组；均伴随日志。
Stale cached serves on probe failure (with warning), cache hits/misses, full rescans, skipped malformed
keys, and fabricated empty groups when replicas are missing; all are logged.

- **标签 / Labels**：—
- **位置 / Recorded in**：`rr-config-etcd/src/lib.rs`
- **版本 / Since**：v1.0.0

### 节点资源 / Node Resources

节点仅在启用 metrics 端口后启动资源采样器，每 5 秒采样一次。平台或 cgroup 不支持的值不会发射；
缺席不是零。文件系统口径取包含 boot journal 目录的最长前缀挂载点；未配置 journal 目录时使用进程当前目录。
The resource sampler starts only when the metrics port is enabled and samples every five seconds. Values unsupported by
the platform or cgroup are omitted, never exported as zero. Filesystem metrics select the longest-prefix mount
containing the boot-journal directory, or the process working directory when no journal directory is configured.

#### `rr_node_cpu_usage_ratio`

CPU 使用率。`process` 是进程 CPU 百分比除以 100 和 host 逻辑核数，1.0 约等于占满全部 host 核；
`host` 是整机使用率，0.0–1.0。两种 scope 不可直接互相替代。
CPU utilization. `process` divides process CPU percentage by 100 and the host logical-core count, so 1.0 approximates
all host cores; `host` is whole-host utilization from 0.0 to 1.0. The scopes are not interchangeable.

- **类型 / Type**：Gauge
- **标签 / Labels**：`scope` (`process` | `host`)
- **位置 / Recorded in**：`rr-server/src/resources.rs`
- **版本 / Since**：v1.0.0

#### `rr_node_cpu_cores`

host 可见的逻辑 CPU 核数；不是进程实际使用量，也不是 cgroup 配额。
Logical CPU cores visible on the host; this is neither process usage nor the cgroup quota.

- **类型 / Type**：Gauge
- **标签 / Labels**：`scope` (`host`)
- **位置 / Recorded in**：`rr-server/src/resources.rs`
- **版本 / Since**：v1.0.0

#### `rr_node_cpu_limit_cores`

Linux cgroup v1/v2 CPU quota 换算的核数（quota/period）。无配额、非 Linux 或无法读取时不发射。
CPU cores derived from the Linux cgroup v1/v2 quota divided by its period. Omitted when unlimited, off Linux, or
unreadable.

- **类型 / Type**：Gauge
- **标签 / Labels**：`scope` (`cgroup`)
- **位置 / Recorded in**：`rr-server/src/resources.rs`
- **版本 / Since**：v1.0.0

#### `rr_node_memory_bytes`

内存字节数。有效组合为 `process/used`、`host/total`、`host/used`、`cgroup/limit` 和 `cgroup/used`；
cgroup 值不可用时缺席。比较用量与限制时必须保持同一 `scope`。
Memory in bytes. Valid combinations are `process/used`, `host/total`, `host/used`, `cgroup/limit`, and `cgroup/used`;
unavailable cgroup values are omitted. Compare usage and limits only within the same `scope`.

- **类型 / Type**：Gauge
- **标签 / Labels**：`scope` (`process` | `host` | `cgroup`), `kind` (`used` | `total` | `limit`)
- **位置 / Recorded in**：`rr-server/src/resources.rs`
- **版本 / Since**：v1.0.0

#### `rr_node_network_bytes_total`

host 全部网络接口累计 RX/TX 字节的单调计数；采样器每次发布差值，源计数器回退时从当前值重新累计，
不产生负增量。用 `rate()` 或 `increase()` 计算吞吐；首个样本包含接口已有累计值，不代表进程生命周期流量。
Monotonic received/transmitted bytes across all host interfaces. The sampler publishes deltas and rebases after a source
counter reset instead of emitting a negative increment. Use `rate()` or `increase()` for throughput; the first sample
includes the interfaces' existing totals and is not process-lifetime traffic.

- **类型 / Type**：Counter
- **标签 / Labels**：`scope` (`host`), `kind` (`rx` | `tx`)
- **位置 / Recorded in**：`rr-server/src/resources.rs`
- **版本 / Since**：v1.0.0

#### `rr_node_filesystem_bytes`

所选挂载点的总容量和已用字节。`mount` 是实际挂载点，不是每组 RocksDB 用量；本指标不能分解为组级磁盘占用。
Total and used bytes on the selected mount. `mount` is the actual mount point, not per-group RocksDB usage; this metric
cannot be attributed to individual groups.

- **类型 / Type**：Gauge
- **标签 / Labels**：`scope` (`host`), `kind` (`total` | `used`), `mount`
- **位置 / Recorded in**：`rr-server/src/resources.rs`
- **版本 / Since**：v1.0.0

## 客户端指标 / Client-Side Metrics

由嵌入 `rr-client` SDK 的应用发射 / Emitted by applications embedding the `rr-client` SDK.

#### `rr_client_request_duration_seconds`

`RrClient::send_request` 端到端秒数，含全部重试；`outcome` 区分成功、最终失败与幂等拒绝。
End-to-end `send_request` seconds, including retries; `outcome` distinguishes success, terminal failure,
and idempotency rejection.

- **类型 / Type**：Histogram
- **标签 / Labels**：`group_id`, `outcome` (`success` | `failure` | `idempotency_expired` |
  `idempotency_namespace_limit`)
- **位置 / Recorded in**：`rr-client/src/lib.rs` `RrClient::send_request`

#### `rr_client_retries_total`

客户端重试次数；首次尝试不计，仅从第 1 次重试起递增。
Retry attempts; the initial attempt is excluded.

- **类型 / Type**：Counter
- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-client/src/lib.rs` `RrClient::send_request`

#### `rr_client_leader_cache_hits_total` / `rr_client_leader_cache_misses_total`

Leader cache 命中/失效；失效后回退 bootstrap 轮询。持续命中率 `<80%` 表示 Leader 频繁切换或集群不稳。
Leader-cache hits/misses; misses fall back to bootstrap round-robin. A sustained hit rate `<80%` indicates
frequent leader changes or instability.

- **类型 / Type**：Counter
- **标签 / Labels**：`group_id`
- **位置 / Recorded in**：`rr-client/src/lib.rs` `RrClient::send_request`

#### `rr_client_batch_flush_total` / `rr_client_batch_entries` / `rr_client_batch_buffer_rejected_total`

opt-in 批处理（默认关）：flush 的 `trigger` 为 `size` | `bytes` | `linger` | `immediate`；每批条数；
缓冲拒绝的 `limit` 为 `entries` | `bytes`，是 fail-fast 背压信号。
Opt-in batching, off by default: flushes, entries per batch, and fail-fast backpressure rejections with the
listed `trigger` and `limit` values.

- **类型 / Type**：Counter / Histogram / Counter
- **标签 / Labels**：flush 为 `partition_id` + `trigger`；rejected 仅 `limit`，无 `partition_id` /
  Flush has `partition_id` and `trigger`; rejected has only `limit` and no `partition_id`
- **位置 / Recorded in**：`rr-client/src/batch.rs`
- **版本 / Since**：v1.0.0

## 写链路延迟证据 / Write-Path Latency Evidence

以下指标描述不同边界的样本，应并排分析，而不是拼成互斥的延迟预算：
These metrics observe samples at different boundaries. Analyze them side by side, not as a partition of one latency
budget:

- `rr_client_request_duration_seconds`：SDK 端到端请求，含重试。/ SDK end-to-end requests, including retries.
- `rr_server_request_duration_seconds{op="write"}`：dispatch 边界写入。/ Writes at the dispatch boundary.
- `rr_proposal_duration_seconds`：client/batch 提案到 commit + apply。/ Client/batch proposals through commit and
  apply.
- `rr_raft_apply_duration_seconds`：每个副本的状态机 apply。/ State-machine apply on every replica.
- `rr_raft_append_entries_rtt_seconds`：Leader→follower heartbeat/append RTT。/ Leader-to-follower heartbeat and
  append RTT.

:::warning 禁止未对齐样本相减 / Never Subtract Unaligned Cohorts
当前 server histogram 没有 `outcome`，proposal 还区分 `client`/`batch`，apply 又在每个副本逐条记录。
失败、redirect、重试和 batch 会使样本集合不同；相同 `group_id`、Leader 和时间窗也不能证明对齐。
因此 **server、proposal、apply 的均值不得相减或堆叠，分位数也不得相减、相加或堆叠**。Console 将此状态标为
`UNALIGNED_COHORTS`，独立展示各段均值和分位数。

The current server histogram has no `outcome`; proposal distinguishes `client` and `batch`; apply is recorded per entry
on every replica. Failures, redirects, retries, and batches select different samples. Matching `group_id`, leader, and
time window does not prove alignment. **Never subtract or stack server/proposal/apply means, and never subtract, add,
or stack their quantiles.** Console reports `UNALIGNED_COHORTS` and presents each segment independently.

只有未来版本提供对齐的 `outcome`/`cohort` 标签，并验证所选 server/proposal/apply 样本计数一致后，才可重新评估
均值构成；分位数仍永远不可做构成减法。
Mean composition may be reconsidered only after a future version supplies aligned `outcome`/`cohort` labels and equal
selected sample counts are verified across server, proposal, and apply. Quantile composition remains invalid.
:::

尾部分析使用各段自身的 `rr:server_request_duration_seconds:p99_1m`、`rr:proposal_duration_seconds:p99_1m`、
`rr:apply_duration_seconds:p99_1m` 和 `rr:append_entries_rtt_seconds:p99_1m`。P99 必须先按 `le` 和所需维度聚合
histogram bucket，再调用 `histogram_quantile()`；禁止平均节点或副本的 P99。
For tail analysis, use each segment's own `rr:server_request_duration_seconds:p99_1m`,
`rr:proposal_duration_seconds:p99_1m`, `rr:apply_duration_seconds:p99_1m`, and
`rr:append_entries_rtt_seconds:p99_1m`. Compute P99 by aggregating histogram buckets by `le` and the required
dimensions before `histogram_quantile()`; never average per-node or per-replica P99 values.

`deploy/observability/prometheus/rr-recording-rules.yml` 中预计算的 `rr:*:p99_1m` recording rules 使用 1m
窗口；直接查询原始 histogram 时可选其他窗口，下方 `histogram_quantile()` 示例使用 5m。长拖尾窗口会在负载
停止后保留历史最坏样本，并在爬升时混合工况；需要当前工况时优先使用 1m 规则。`read` 与 `lin_read` 不经过
Raft 复制，`rr_server_request_duration_seconds{op}` 的 total 即该 dispatch 边界的全链路。
The precomputed `rr:*:p99_1m` recording rules in
`deploy/observability/prometheus/rr-recording-rules.yml` use a 1m window. Direct queries against raw
histograms may choose other windows; the `histogram_quantile()` example below uses 5m. Long trailing windows
retain historical worst samples after load stops and mix regimes during ramp-up, so prefer the 1m rules for
the current regime. `read` and `lin_read` skip Raft replication, so the total from
`rr_server_request_duration_seconds{op}` is the full chain at that dispatch boundary.

## 链路追踪 / Tracing

原 Phase 3 观测性计划已全部落地：`rr_raft_replication_lag` 自 v0.2.0 可用；W3C trace context 传播自
v0.2.0 由 `rr-network::tracing` 的 `extract_/attach_traceparent` 助手和 `examples/otel-client` 示例提供。
OpenTelemetry SDK（OTLP exporter）按设计不内置，由用户自带。
The original Phase 3 observability plan has landed: `rr_raft_replication_lag` is available since v0.2.0,
and W3C trace-context propagation shipped in v0.2.0 through `rr-network::tracing`
`extract_/attach_traceparent` helpers and `examples/otel-client`. The OpenTelemetry SDK and OTLP exporter
are deliberately not bundled; bring your own OTLP stack.

## Prometheus 配置 / Prometheus Configuration

每个 target 必须带稳定且唯一的 `node_id`，并用精确的集群标签隔离同名 group。不要使用 `instance` 反推
node ID。Console 历史直接使用原始 histogram/counter/gauge family，并为每次查询强制精确集群 matcher 和节点
映射校验；它不消费 recording rules。供其他查询使用的 recording rules 也应保留集群和节点标签。
Every target needs a stable, unique `node_id` and an exact cluster label to isolate same-named groups. Never infer node
IDs from `instance`. Console history queries raw histogram/counter/gauge families directly and enforces the exact
cluster matcher and node mapping on every query; it does not consume recording rules. Recording rules used by other
queries should also preserve cluster and node labels.

```yaml
scrape_configs:
  - job_name: 'rr-cluster'
    static_configs:
      - targets: ['127.0.0.1:62001']
        labels: {cluster_id: 'prod-rr', node_id: '1'}
      - targets: ['127.0.0.1:62002']
        labels: {cluster_id: 'prod-rr', node_id: '2'}
      - targets: ['127.0.0.1:62003']
        labels: {cluster_id: 'prod-rr', node_id: '3'}
    scrape_interval: 15s
```

## 查询示例 / Query Examples

```promql
# 每组每秒提案数 / Proposals per second per group
sum by (group_id) (rate(rr_proposal_duration_seconds_count[1m]))

# p99 提案延迟 / p99 proposal latency
histogram_quantile(0.99, sum by (le, group_id) (rate(rr_proposal_duration_seconds_bucket[5m])))

# 当前全部 Leader / All current leaders
rr_raft_is_leader == 1

# 每秒路由失败 / Routing lookup failures per second
rate(router_route_not_found[1m])

# 连接池抖动 / Connection-pool churn
sum(rate(rr_channel_reconnects_total[5m])) by (node_id)

# 集群写入 TPS（apply 精确口径）/ Cluster write TPS (exact applied entries)
rr:applied_entries_rate_1m

# 按操作类型的请求 TPS / Request TPS by op
rr:server_request_rate_1m:by_op

# Kafka 链路 TPS / Kafka-path TPS
rr:ingestor_commands_rate_1m

# p99 客户端请求延迟（按 op，1m）/ p99 client request latency by op (1m)
rr:server_request_duration_seconds:p99_1m

# p99 Leader→follower AppendEntries RTT
rr:append_entries_rtt_seconds:p99_1m

# 各段均值独立比较；不要相减或堆叠 / Compare segment means independently; never subtract or stack
rr:apply_seconds:mean_1m
rr:proposal_seconds:mean_1m
rr:server_request_seconds:mean_1m{op="write"}
```
