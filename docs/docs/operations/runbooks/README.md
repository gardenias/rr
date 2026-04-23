---
sidebar_position: 1
---

# Operational Runbooks / 运维手册索引

本目录收录 rr 生产运维场景的标准响应流程。每篇按 **症状 → 检测 → 立即处置 → 根因分析 → 恢复 → 预防** 格式组织，供事故响应时直接跟读。

---

This directory collects standardized response procedures for rr production
scenarios. Each runbook follows a **Symptom → Detection → Immediate
mitigation → Root-cause investigation → Recovery → Prevention** structure
intended to be followed top-to-bottom during an incident.

## 索引 / Index

| Runbook | 触发场景 / Trigger |
| :--- | :--- |
| [Disk Full](./disk-full.md) | 任一节点的 RocksDB 数据目录磁盘耗尽 / Any node's RocksDB data dir runs out of disk space |
| [Leader Flapping](./leader-flapping.md) | 某个 Raft group 的 leader 反复改变 / A Raft group's leader changes repeatedly |
| [Network Partition](./network-partition.md) | 集群网络分区（部分节点互通失败）/ Cluster network partition (nodes can't reach a subset) |
| [Snapshot Corruption](./snapshot-corruption.md) | `InstallSnapshot` 失败，`bincode` decode errors / InstallSnapshot fails with bincode decode errors |
| [ConfigProvider Unreachable](./configprovider-unreachable.md) | 外部 ConfigProvider（etcd 等）不可达 / External ConfigProvider (etcd etc.) is unreachable |

## 通用前置步骤 / Common Prework

**事故响应开始时**：

1. **记时** — `date -u +"%Y-%m-%dT%H:%M:%SZ"`；所有后续行动以此为起点。
2. **打开事故频道** — Slack/Discord 专用频道，消息带 `[rr-incident-<时间戳>]` 前缀。
3. **snapshot 当前状态** —
   ```bash
   for node in rr-0 rr-1 rr-2; do
     rr-admin --addr ${node}:52001 node status --output json > status-${node}.json
     rr-admin --addr ${node}:52001 metrics --output json > metrics-${node}.json
   done
   ```
4. **确认 rr 版本** — `rr-server version`；记录在事故 log。

---

**At the start of incident response**:

1. **Note the time** — `date -u +"%Y-%m-%dT%H:%M:%SZ"` is your T0.
2. **Open incident channel** — Slack/Discord channel with messages
   prefixed `[rr-incident-<timestamp>]`.
3. **Snapshot current state** with the `rr-admin` calls above.
4. **Confirm the rr version** on every node — version skew can itself be
   the cause.

## 快捷命令速查 / Quick-reference commands

```bash
# 节点状态 / Node status
rr-admin --addr 127.0.0.1:52001 node status

# 所有 Raft 组状态 / All Raft group status (loop over known groups)
for gid in 1 2 3; do rr-admin --addr 127.0.0.1:52001 group status --group $gid; done

# 查看连接的 leader 是哪个 / Find the leader for group 1
rr-admin --addr 127.0.0.1:52001 group status --group 1 --output json | jq .leader_node_id

# 转移 leader 到另一节点 / Transfer leader
rr-admin --addr 127.0.0.1:52001 group transfer-leader --group 1 --to 3

# 触发快照 / Trigger snapshot
rr-admin --addr 127.0.0.1:52001 group snapshot --group 1

# 备份 data_dir（节点必须已停）/ Backup data_dir (node must be stopped)
rr-admin backup --data-dir /var/lib/rr/data --output /tmp/rr-backup-$(date +%F).tar.gz --node-id 1

# Prometheus 查询示例 / Prometheus queries
# 当前各组 leader / Current leaders
# rr_raft_is_leader == 1
# p99 propose 延迟 / p99 propose latency
# histogram_quantile(0.99, sum by (le, group_id) (rate(rr_proposal_duration_seconds_bucket[5m])))
```

## 相关 / Related

- 架构 / Architecture: [../../architecture/c4/README.md](../../architecture/c4/README.md)
- 指标 / Metrics: [../../reference/metrics-reference.md](../../reference/metrics-reference.md)
- 错误码 / Error reference: [../../reference/error-reference.md](../../reference/error-reference.md)
