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

## 指标列表 / Metric Inventory

所有指标均以 `rr_` 为前缀，并携带 `group_id` 标签（如适用）。

---

All metrics are prefixed with `rr_` and carry a `group_id` label where applicable.

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
