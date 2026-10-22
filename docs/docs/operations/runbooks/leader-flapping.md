---
sidebar_position: 3
---

# Runbook — Leader 反复切换 / Leader Flapping

## Symptom / 症状

某个 Raft group 的 leader 短时间内改变多次。症状：

- 客户端看到频繁的 `ForwardToLeader` 重定向；延迟飙升
- 节点日志里大量 openraft vote / election 消息
- Prometheus: `rr_raft_current_term` 快速递增（每分钟 > 3 次）

```
INFO openraft::engine::handler::vote_handler: vote is changing from T5-N1:committed to T6-N2:uncommitted
INFO openraft::engine::handler::server_state_handler: become leader id=2
INFO openraft::engine::handler::vote_handler: vote is changing from T6-N2:committed to T7-N3:uncommitted
...
```

## Detection / 检测

```promql
# PromQL — 任一 group 5 分钟内 term 增长超过 10
increase(rr_raft_current_term[5m]) > 10

# PromQL — 每分钟 leader 变更 > 1 次
rate(changes(rr_raft_is_leader[1m])[5m:]) > 1
```

## Immediate Mitigation / 立即处置

### Step 1 — 确认当前有 leader / Confirm there IS a leader right now

```bash
rr-admin --addr rr-0:52001 group status --group $GROUP --output json | jq .leader_node_id
```

如果返回 `0` 或空，说明此刻没有 leader — 这是比 flapping 更严重的问题（完全无法提交写），转到 [network-partition.md](./network-partition.md)。

If this returns `0` or empty, there is NO current leader — a worse state
than flapping. Go to [network-partition.md](./network-partition.md).

### Step 2 — 识别 flap 参与的节点 / Identify the flapping nodes

查 Prometheus `rr_raft_is_leader` 过去 15 分钟的时序，看哪些 node_id 出现过 `1`。

### Step 3 — 尝试人工稳定 / Attempt manual stabilization

选择一个**磁盘 I/O 最健康、CPU 最低、网络最稳定**的节点作为目标 leader，然后：

```bash
# 锁定 leader 在 N3 上 / Pin leader to N3
rr-admin --addr rr-N3:52001 group transfer-leader --group $GROUP --to 3
sleep 10
rr-admin --addr rr-0:52001 group status --group $GROUP --output json | jq '{leader: .leader_node_id, term: .current_term}'
```

如果 10 秒内 leader 稳定在 3 且 `current_term` 不再递增 → 进入根因分析。
如果继续 flap → 可能需要短期让 `election_timeout_*` 增大（配置变更 + 滚动重启）。

## Root-Cause Investigation / 根因分析

按可能性排序：

### 1. 网络抖动 / Network flakiness

**证据**: `rr_channel_reconnects_total` 快速增长；Prometheus `node_network_transmit_errs_total` 非 0；`ping` 到 peer 有丢包。

**Fix**: 短期隔离问题节点（见 [network-partition.md](./network-partition.md)）；长期修复网络。

### 2. 磁盘慢到触发选举超时 / Disk too slow to hold election timeout

**证据**: `rr_proposal_duration_seconds` 的 p99 接近或超过 `election_timeout_min`（默认 299ms）；`node_disk_io_time_seconds_total` 高。

```promql
histogram_quantile(0.99, sum by (le) (rate(rr_proposal_duration_seconds_bucket[5m])))
```

如果 p99 > 250ms，磁盘是瓶颈。磁盘 fsync 阻塞 Raft heartbeat tick，引发被动选举。

**Fix**:
- 临时：把 `durability` 从 `strict` 降到 `replicated_only`（**注意**：本机不再 fsync，违反 Raft 耐久性合约，仅靠多副本冗余兜底；旧别名 `balanced` 已在 v0.3.0 移除，配置会硬报错）。只用于已知非关键数据 + replicated clusters + 有完整备份 + 立即计划磁盘升级。
- 长期：换更快的磁盘 (NVMe)，或减少写入速率。

### 3. CPU 抢占 / CPU starvation

**证据**: `node_cpu_seconds_total{mode="iowait|steal"}` 高；`rr-server` 进程 `pidstat` 显示 scheduler latency 高。

**Fix**: 限制同机其他租户的 CPU 消耗；在 K8s 设置 `resources.requests.cpu` 确保 guaranteed QoS。

### 4. 配置抖动 / Config oscillation

**证据**: ConfigProvider 反复广播 `RoutingTableChange::GroupMembershipChanged`（或 `NodeAdded`/`NodeRemoved` 抖动）事件，各节点的 watch loop 不断 start/stop raft group，leader 随之反复上下台。

**Fix**: 检查 ConfigProvider 实现是否在配置面反复改写 membership 或节点地址（形成抖动回路）。注意路由表**不包含** leader 信息（ADR A-0013：leader 是 raft 运行时状态，客户端经 redirect 发现），所以 leader 抖动不可能来自路由表写回——它只会来自 membership/节点事件的反复变更。`MemConfigProvider` 不会自发产生这类事件；自定义实现可能有 bug。

### 5. 时钟跳变 / Clock jumps

**证据**: `ntpstat` 显示 unsynchronized；`journalctl` 有 "Clock synchronized" 条目恰好与 flap 开始时间吻合。

**Fix**: rr 使用 `Instant::now()`（monotonic），不受 wall-clock 跳变影响。但如果监控器/调度器受 wall-clock 影响重调度 rr 进程，仍可能引发问题。部署 NTP / chrony，禁用激进的时钟同步（`makestep` 阈值放大）。

## Recovery / 恢复

- 确认 `rr_raft_current_term` 停止增长（过去 5 分钟没新 election）
- `rr_proposal_duration_seconds` p99 回到 baseline
- 客户端 ForwardToLeader 计数（如果暴露）归零

## Prevention / 预防

- **告警** — term 5 分钟增长 > 5 pages（不只是 warn）
- **容量 headroom** — 磁盘 fsync 延迟 < `election_timeout_min / 3`
- **绑定 CPU** — 生产环境给 rr-server 独占 CPU core 或用 K8s `cpuManagerPolicy: static`
- **NTP** — 部署稳定的 NTP 源（chrony or ntpd），每节点监控 offset

## 相关 / Related

- [network-partition.md](./network-partition.md) — 如果 leader 完全没了
- [disk-full.md](./disk-full.md) — 磁盘压力也引起 flap
- [../../architecture/c4/dynamic-leader-election.md](../../architecture/c4/dynamic-leader-election.md) — 选举机制细节
