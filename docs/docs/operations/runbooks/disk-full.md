---
sidebar_position: 2
---

# Runbook — 节点磁盘满 / Node Disk Full

## Symptom / 症状

一个或多个 rr-server 节点的日志开始输出 RocksDB I/O 错误，客户端写入开始超时或失败。常见日志条目：

One or more rr-server nodes start logging RocksDB I/O errors and client
writes time out or fail. Typical log lines:

```
ERROR rr_raft::group_manager: client_write failed: storage error: io error: No space left on device (os error 28)
ERROR openraft::storage: apply_to_state_machine failed: storage error: Io(...)
```

典型指标 / Typical metrics:

- `rr_proposal_duration_seconds` p99 飙升
- `rr_raft_last_log_index` 停止增长（该节点）
- Prometheus `up{job="rr"}` 开始翻 `0`

## Detection / 检测

自动告警应基于：

Automated alerts should fire on:

```promql
# PromQL — 节点可用磁盘 < 10% 持续 5 分钟
node_filesystem_avail_bytes{mountpoint="/var/lib/rr"}
  / node_filesystem_size_bytes{mountpoint="/var/lib/rr"} < 0.1

# 或应用层：Raft 组 last_log_index 20 分钟没增长
increase(rr_raft_last_log_index[20m]) == 0
```

手动确认 / Manual confirm:

```bash
# SSH 到涉事节点 / SSH to the affected node
ssh rr-N
df -h /var/lib/rr
du -sh /var/lib/rr/data/* | sort -h | tail -5
```

## Immediate Mitigation / 立即处置

**Rules of engagement**:
- 如果集群仍有 quorum（≥ ⌈N/2⌉+1 节点健康），优先保持 quorum，不要动健康节点。
- 不要 `rm -rf` 任何 RocksDB 文件 —— 即使是 `.log` 或 `.sst`。RocksDB 对自己的文件布局有严格依赖。

### Option A — 放逐受影响节点（推荐）/ Evacuate the affected node (preferred)

```bash
# 1. 把该节点上的所有 leader 转走 / Transfer all leaderships off the node
for gid in $(rr-admin --addr OTHER_NODE:52001 node status --output json | jq -r '.group_ids[]'); do
  # Find alternative voter
  rr-admin --addr OTHER_NODE:52001 group transfer-leader --group $gid --to OTHER_VOTER_ID
done

# 2. 停止该节点 / Stop the node
systemctl stop rr-server  # or kubectl delete pod rr-N

# 3. 给它更大的盘（或清理非 rr 的占用）/ Grow the disk or clean non-rr occupants
# (cloud: resize volume; on-prem: delete unrelated files)

# 4. 启动节点 / Start again
systemctl start rr-server
```

Cluster continues serving while the node is down (as long as quorum is
maintained).

### Option B — 受影响节点是最后一个 quorum 成员 / Affected node is the last quorum member

Then you CANNOT take it offline without losing availability. Options:

- **Quick win**: 识别并删除**非 rr** 的占用（`/var/log/*`、`core.*` 等）在同一 filesystem 上。
- **Medium**: 在另一个 mountpoint 挂卷，然后 `rr-admin backup` → shutdown → 把 data_dir 拷到新卷 → 修改 config 的 `data_dir` → 重启。
- **Risky last resort**: 在 RocksDB 相对较小的 group 上 `rr-admin group snapshot --group G` 触发日志压缩释放空间。**先在非 quorum 成员上试**。

## Root-Cause Investigation / 根因分析

按可能性从高到低：

In order of likelihood:

1. **log growth outpacing snapshots** — 检查 `rr_raft_last_log_index` 与 `rr_snapshot_installs_total` 的比率。正常应每 `max_in_memory_logs` 条目一次快照。如果 snapshot 间隔过大，`openraft` 的 snapshot policy 可能配置太保守。
2. **compaction backlog** — 在 RocksDB `LOG` (`/var/lib/rr/data/LOG`) 里 grep `compaction`；若看到 "Slowdown" 或 "Stop" 条目，说明 L0 堆积。需要调高 `max_background_jobs` 或 `level_zero_stop_trigger`。
3. **runaway write rate** — Prometheus `rate(rr_proposal_duration_seconds_count[5m])` 是否比平时高 10×？若是，上游问题（应用 bug、流量激增），不单纯是 rr 问题。
4. **非 rr 租户** — 同一 volume 上的其他进程（日志、core dump、其他数据库）。`ncdu /var/lib/rr/..`。

---

1. **log growth outpacing snapshots** — check `rr_raft_last_log_index`
   vs `rr_snapshot_installs_total`. Normal behavior: snapshot every
   `max_in_memory_logs` entries. If the gap is too large, openraft's
   snapshot policy is set too conservatively.
2. **compaction backlog** — grep `compaction` in RocksDB's `LOG` at
   `/var/lib/rr/data/LOG`. "Slowdown" or "Stop" entries indicate L0
   pile-up. Raise `max_background_jobs` or
   `level_zero_stop_trigger`.
3. **runaway write rate** — is
   `rate(rr_proposal_duration_seconds_count[5m])` 10× normal? If so,
   the upstream application is the cause.
4. **non-rr tenant** — other processes on the same volume (logs, core
   dumps, another database). `ncdu /var/lib/rr/..`.

## Recovery / 恢复

在解除磁盘压力后：

After relieving disk pressure:

1. **确认节点重新加入 quorum** — `rr-admin node status` 返回它所在的所有 group_ids 并且 leader 与其他节点一致。
2. **等待追赶完成** — `rr_raft_last_log_index` 与 leader 上的差距应持续收敛；根据磁盘性能，1GB 状态大约 1-5 分钟。
3. **如果该节点落后过远**，leader 会触发 `InstallSnapshot`；`rr_snapshot_installs_total{node_id=<N>}` 会 +1。等待完成。

## Prevention / 预防

- **告警** — 设置磁盘 <20% 的警告、<10% 的 ticket/page。
- **容量预留** — RocksDB 在 compaction 期间可能瞬时占用 2× 数据大小；确保 overhead ≥ 100%。
- **配置 tuning** — 在 `RocksConfig` 里合理设置 `max_background_jobs`（≥4）与 `level_zero_stop_trigger`。
- **专用 volume** — `/var/lib/rr/data` 独占一个 volume，避免和日志、swap 抢空间。
- **定期 `rr-admin group snapshot`** — 对写密集的 group 定时（每小时或每万条）触发 snapshot 以 cap 日志增长。

## 相关 / Related

- [leader-flapping.md](./leader-flapping.md) — 磁盘压力也是 leader 不稳定的常见诱因
- [../../reference/metrics-reference.md](../../reference/metrics-reference.md) — 监控指标
- [../deployment.md](../../deployment/deployment.md) — 生产部署建议
