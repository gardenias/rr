---
sidebar_position: 1
---

# 运维手册索引 / Operational Runbooks

每篇按症状、检测、立即处置、根因、恢复和预防组织。先保存证据，再执行变更；涉及成员或数据目录的命令必须先满足该页前提。

Each runbook follows symptoms, detection, mitigation, root cause, recovery, and prevention. Preserve evidence before
making changes; membership or data-directory commands require the prerequisites stated on that page.

## 索引 / Index

| 手册 / Runbook | 触发场景 / Trigger |
| :--- | :--- |
| [Disk Full](./disk-full.md) | RocksDB 数据目录磁盘耗尽 / RocksDB data directory runs out of disk |
| [Leader Flapping](./leader-flapping.md) | Raft group leader 反复改变 / A Raft group's leader changes repeatedly |
| [Network Partition](./network-partition.md) | 部分节点无法互通 / Some nodes cannot reach each other |
| [Snapshot Corruption](./snapshot-corruption.md) | 快照启动加载或 InstallSnapshot 解码/应用失败 / Snapshot startup load or InstallSnapshot decode/apply fails |
| [ConfigProvider Unreachable](./configprovider-unreachable.md) | 外部 ConfigProvider 或 managed lifecycle store 不可达 / External ConfigProvider or managed lifecycle store is unreachable |

## 通用前置步骤 / Common Prework

1. 记录 UTC 起点，并打开带 `[rr-incident-<时间戳>]` 前缀的事故频道。<br />
   Record the UTC start time and open an incident channel using the `[rr-incident-<timestamp>]` prefix.
2. 保存每个节点的状态与指标，不要在未保存前重启。<br />
   Capture status and metrics from every node before restarting anything.
3. 在每个节点执行 `rr-server version`，记录版本差异。<br />
   Run `rr-server version` on every node and record skew.
4. 安全模式下，所有 Admin 命令额外提供 `--token-env RR_ADMIN_TOKEN` 和三个 TLS 参数；下文为可读性省略这些公共参数。<br />
   In secure mode, add `--token-env RR_ADMIN_TOKEN` and all three TLS flags to every Admin command; later examples omit
   these common flags for readability.

```bash
date -u +"%Y-%m-%dT%H:%M:%SZ"
for node in rr-0 rr-1 rr-2; do
  rr-admin --addr ${node}:52001 --output json node status > status-${node}.json
  rr-admin --addr ${node}:52001 --output json metrics > metrics-${node}.json
done
```

## 常用命令 / Quick-Reference Commands

```bash
rr-admin --addr 127.0.0.1:52001 node status

for gid in 1 2 3; do
  rr-admin --addr 127.0.0.1:52001 group status --group "$gid"
done

rr-admin --addr 127.0.0.1:52001 --output json group status --group 1 | jq .leader_node_id

# transfer-leader 必须调用目标节点自己的 Admin API / call the target node's own Admin API
rr-admin --addr TARGET_NODE:52001 group transfer-leader --group 1 --to 3

rr-admin --addr 127.0.0.1:52001 group snapshot --group 1
rr-admin --addr 127.0.0.1:52001 group snapshot-status --group 1

# 仅停机后执行 / only after the node has stopped
rr-admin backup --data-dir /var/lib/rr/data --output /tmp/rr-backup-$(date +%F).tar.gz --node-id 1
```

```promql
rr_raft_is_leader == 1
histogram_quantile(0.99, sum by (le, group_id) (rate(rr_proposal_duration_seconds_bucket[5m])))
```

## 相关 / Related

- [架构 / Architecture](../../architecture/c4/README.md)
- [指标 / Metrics](../../reference/metrics-reference.md)
- [错误码 / Error reference](../../reference/error-reference.md)
