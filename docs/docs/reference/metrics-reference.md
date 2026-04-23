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

#### `rr_proposal_duration_seconds`

记录从客户端提交提案到该提案被 Raft 提交的耗时（秒）。

---

Records the time in seconds from client proposal submission to Raft commit.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Histogram |
| 标签 / Labels | `group_id` |
| 记录位置 / Recorded in | `rr-raft/src/group_manager.rs` — `write()` |

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
| 记录位置 / Recorded in | `rr-raft/src/group_manager.rs` — `write()` |

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
| 标签 / Labels | `group_id` (目前硬编码为 `"unknown"` / currently hard-coded to `"unknown"`) |
| 记录位置 / Recorded in | `rr-raft/src/service_adapter.rs` — `restore()` |
| 已知问题 / Known Issue | `group_id` 标签目前因为 `service_adapter` 无法访问所在 group 的 ID 而硬编码。Phase 3 将通过把 group_id 注入 adapter 修复此问题 / The `group_id` label is hard-coded because `service_adapter` lacks access to its owning group's ID. Phase 3 will fix this by threading `group_id` into the adapter. |

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

#### `node.started`

节点启动完成（所有子系统 wired up，gRPC 服务已绑定）时 +1。

---

Incremented by 1 when the node finishes startup (all subsystems wired, gRPC servers bound).

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-server/src/node.rs` — `serve()` |

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

#### `router.get_leader_addr.success`

`ShardRouter::get_leader_addr()` 成功解析到一个 leader 地址时 +1。与 `router.route.not_found` 配对，可用于计算路由查找成功率。

---

Incremented by 1 when `ShardRouter::get_leader_addr()` successfully resolves a leader
address for a group. Pair with `router.route.not_found` to compute routing-lookup
success rate.

| 属性 / Attribute | 值 / Value |
| :--- | :--- |
| 类型 / Type | Counter |
| 标签 / Labels | — |
| 记录位置 / Recorded in | `rr-router/src/router.rs` — `get_leader_addr()` |

---

## 规划中的指标 / Planned Metrics

Phase 3 计划新增以下指标（详见 `.sisyphus/plans/rr-phase3-prodready.md` T28、T29）：

Phase 3 plans to add the following metrics (see `.sisyphus/plans/rr-phase3-prodready.md` T28, T29):

- `rr_client_request_duration_seconds` — 客户端 SDK 端到端请求延迟（含重试）/ End-to-end client SDK request latency (incl. retries)
- `rr_client_retries_total` — 客户端重试次数 / Client retry attempts
- `rr_client_leader_cache_hits_total` / `_misses_total` — leader 缓存命中率 / Leader cache hit rate
- `rr_raft_replication_lag` — 每个 follower 相对 leader 的 `commit_index` 差距 / Per-follower commit_index lag behind leader

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

# 路由失败率 / Routing failure rate
rate(router_route_not_found[1m]) / (rate(router_route_not_found[1m]) + rate(router_get_leader_addr_success[1m]))

# 连接池不稳定性 / Connection-pool churn
sum(rate(rr_channel_reconnects_total[5m])) by (node_id)
```
