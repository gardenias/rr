---
sidebar_position: 3
---

# Leader 反复切换手册 / Runbook: Leader Flapping

## 症状 / Symptoms

客户端频繁收到 `ForwardToLeader`，延迟上升，日志出现连续 vote/election，`rr_raft_current_term` 快速增加。

Clients repeatedly receive `ForwardToLeader`, latency rises, logs show continuous votes/elections, and
`rr_raft_current_term` increases rapidly.

```text
INFO openraft::engine::handler::vote_handler: vote is changing from T5-N1:committed to T6-N2:uncommitted
INFO openraft::engine::handler::server_state_handler: become leader id=2
INFO openraft::engine::handler::vote_handler: vote is changing from T6-N2:committed to T7-N3:uncommitted
```

## 检测 / Detection

```promql
increase(rr_raft_current_term[5m]) > 10
changes(rr_raft_is_leader[5m]) > 2
```

`rr_raft_is_leader` 只有 `group_id` 标签，节点身份来自 scrape target 的 `instance` 等标签。按实例查看哪些节点在过去 15
分钟成为过 leader。

`rr_raft_is_leader` has only the `group_id` metric label; node identity comes from scrape-target labels such as
`instance`. Inspect by instance to see which nodes became leader during the last 15 minutes.

```bash
rr-admin --addr rr-0:52001 --output json group status --group "$GROUP" | jq .
```

安全模式下，为 `rr-admin` 添加 `--token-env RR_ADMIN_TOKEN` 和三个 TLS 参数。

In secure mode, add `--token-env RR_ADMIN_TOKEN` and all three TLS flags to `rr-admin`.

## 立即处置 / Immediate Mitigation

若 `leader_node_id` 为 `0`、`null` 或持续变化，先停止非必要管理操作。完全没有 leader 时，同时按
[网络分区](./network-partition.md) 检查连通性。

If `leader_node_id` is `0`, `null`, or keeps changing, stop nonessential administration. When no leader exists at all,
also follow [Network Partition](./network-partition.md).

不要循环执行 leader transfer，它不是“锁定 leader”。只有在确认目标节点是 voter、磁盘/CPU/网络健康时，才在目标节点自己的
Admin API 上触发一次选举：

Do not loop leader transfers; they do not pin leadership. Only after confirming that the target is a voter with healthy
disk, CPU, and network, trigger one election through the target node's own Admin API:

```bash
rr-admin --addr rr-N3:52001 group transfer-leader --group "$GROUP" --to 3
sleep 10
rr-admin --addr rr-N3:52001 --output json group status --group "$GROUP" | \
  jq '{leader: .leader_node_id, term: .current_term}'
```

若仍 flap，转入根因处理。当前选举超时不支持配置，不要寻找或新增不存在的 `election_timeout_*` 配置项。

If flapping continues, address the root cause. Election timeouts are not currently configurable; do not look for or
invent `election_timeout_*` settings.

## 根因分析 / Root-Cause Investigation

### 网络抖动 / Network Flakiness

证据包括 `rr_channel_reconnects_total` 增长、主机网络错误或丢包。先修复链路或隔离故障节点，流程见
[网络分区](./network-partition.md)。

Evidence includes rising `rr_channel_reconnects_total`, host network errors, or packet loss. Repair the link or isolate
the faulty node using [Network Partition](./network-partition.md).

### 磁盘或 I/O 过慢 / Slow Disk or I/O

检查 proposal p99、append RTT、主机 iowait 和 RocksDB 日志。p99 超过 250ms 是异常信号，但不能单独证明磁盘是根因，网络和
CPU 也会产生同样症状。

Inspect proposal p99, append RTT, host iowait, and RocksDB logs. Proposal p99 above 250ms is a warning signal but does
not prove a disk root cause; network and CPU problems can look identical.

```promql
histogram_quantile(0.99, sum by (le, group_id) (rate(rr_proposal_duration_seconds_bucket[5m])))
histogram_quantile(0.99, sum by (le, group_id, follower_node_id) (rate(rr_raft_append_entries_rtt_seconds_bucket[5m])))
```

修复慢盘、其他租户 I/O 或写放大，长期可使用 NVMe。不要把 `durability` 降为 `replicated_only` 作为处置：Raft 日志仍固定使
用 Strict，而且降级会削弱状态机侧持久化。

Fix slow disks, competing I/O, or write amplification; use NVMe where appropriate. Do not lower `durability` to
`replicated_only` as mitigation: Raft logs remain Strict, and the change weakens state-machine-side persistence.

### CPU 抢占 / CPU Starvation

检查 `node_cpu_seconds_total`、steal/iowait 和 `pidstat` 调度延迟。限制同机其他负载，并在 Kubernetes 为 CPU/内存设置匹配
的 requests/limits；只设 CPU request 不等于 Guaranteed QoS。

Inspect `node_cpu_seconds_total`, steal/iowait, and scheduler latency with `pidstat`. Limit competing workloads and set
matching Kubernetes CPU/memory requests and limits; a CPU request alone does not provide Guaranteed QoS.

### 配置抖动 / Configuration Oscillation

检查 ConfigProvider 是否反复广播 `GroupMembershipChanged`、`NodeAdded` 或 `NodeRemoved`，导致 raft group 反复 start/stop
。路由表不保存 leader，leader flap 不可能由“leader 写回路由表”造成。

Check whether the ConfigProvider repeatedly emits `GroupMembershipChanged`, `NodeAdded`, or `NodeRemoved`, causing
repeated group start/stop. The routing table does not store leadership, so flapping cannot be caused by writing leader
information back to it.

### 时钟与调度 / Clock and Scheduling

rr 选举使用 monotonic `Instant`，wall-clock 跳变不会直接改变超时。但时钟修正可能触发外部调度器或监控行为，仍需检查
NTP/chrony 和进程重启记录。

rr elections use monotonic `Instant`, so wall-clock jumps do not directly change timeouts. Clock corrections can still
affect external schedulers or monitoring, so inspect NTP/chrony and process-restart records.

## 恢复 / Recovery

连续 5 分钟无新 election，term 稳定，proposal 延迟回到基线，所有 voter 能观察同一 leader，客户端重定向恢复到正常水平。

Recovery means no new election for five minutes, a stable term, proposal latency back at baseline, all voters observing
the same leader, and normal client redirect rates.

## 预防 / Prevention

- 对 term 增长和 leader changes 告警，保留节点维度证据。<br />
  Alert on term growth and leader changes, retaining per-node evidence.
- 保持磁盘、CPU 和网络余量，避免共享资源抢占。<br />
  Maintain disk, CPU, and network headroom and avoid shared-resource contention.
- 当前心跳 50ms、选举超时 150–300ms，按此限制部署网络，不要用于高延迟跨地域链路。<br />
  With the current 50ms heartbeat and 150–300ms election timeout, constrain deployment networks accordingly and avoid
  high-latency cross-region links.
- 使用稳定 NTP/chrony，并监控进程重启与时钟同步事件。<br />
  Use stable NTP/chrony and monitor process restarts and clock synchronization.

## 相关 / Related

- [无 leader 或分区 / No leader or partition](./network-partition.md)
- [磁盘压力 / Disk pressure](./disk-full.md)
- [选举机制 / Election mechanics](../../architecture/c4/dynamic-leader-election.md)
