---
sidebar_position: 2
---

# 节点磁盘满手册 / Runbook: Node Disk Full

## 症状 / Symptoms

RocksDB 报 `No space left on device`，写入超时或失败，proposal 延迟升高。进程仍可能健康，因此不能只依赖 Prometheus `up`
。

RocksDB reports `No space left on device`, writes time out or fail, and proposal latency rises. The process can remain
alive, so Prometheus `up` alone is insufficient.

```text
ERROR rr_raft::group_manager: client_write failed: storage error: io error: No space left on device (os error 28)
ERROR openraft::storage: apply_to_state_machine failed: storage error: Io(...)
```

## 检测 / Detection

```promql
node_filesystem_avail_bytes{mountpoint="/var/lib/rr"}
  / node_filesystem_size_bytes{mountpoint="/var/lib/rr"} < 0.1

# 仅在有持续写入时成立 / meaningful only during sustained writes
increase(rr_raft_last_log_index[20m]) == 0
```

```bash
df -h /var/lib/rr
du -sh /var/lib/rr/data/* | sort -h
rr-admin --addr rr-N:52001 group list
```

安全模式下，为 `rr-admin` 添加 `--token-env RR_ADMIN_TOKEN` 和三个 TLS 参数。

In secure mode, add `--token-env RR_ADMIN_TOKEN` and all three TLS flags to `rr-admin`.

## 立即处置 / Immediate Mitigation

多数派数量为 `floor(N/2)+1`。只要仍有健康多数派，优先保持它，不要重启健康节点。不要删除任何 RocksDB `.log`、`.sst`、
`MANIFEST` 或其他文件。

A majority is `floor(N/2)+1`. Preserve a healthy majority and do not restart healthy nodes. Never delete RocksDB `.log`,
`.sst`, `MANIFEST`, or other database files.

### 可离线受影响节点 / When the Affected Node Can Go Offline

1. 在受影响节点自己的 Admin API 上 drain 所有组。<br />
   Drain every group through the affected node's own Admin API.
2. 优雅停止节点，保留数据目录和 PVC。<br />
   Stop the node gracefully while retaining its data directory and PVC.
3. 扩容文件系统，或只清理由其他服务产生的已知文件。<br />
   Grow the filesystem, or remove only known files owned by other services.
4. 启动节点并检查追平。<br />
   Start the node and verify catch-up.

```bash
rr-admin --addr rr-N:52001 node drain --node-id N
systemctl stop rr-server
# 扩容磁盘或清理非 rr 文件 / grow the disk or clean non-rr files
systemctl start rr-server
```

Kubernetes 不要直接 `kubectl delete pod` 当作停机扩容：控制器会立即重建 Pod。先使用能保留 PVC 且阻止重建的运维流程，再扩
卷或挂载离线任务。

On Kubernetes, do not use `kubectl delete pod` as a disk-resize procedure; the controller recreates it immediately. Use
an outage procedure that preserves the PVC while preventing recreation, then resize the volume or attach an offline
task.

### 不能离线或已无 quorum / When It Cannot Go Offline or Quorum Is Lost

优先在线扩卷、挂载额外空间，或删除同一文件系统上确认属于其他服务的日志/core dump。不要为“释放空间”触发新快照：构建快照本
身需要临时空间，且不能立即解决 RocksDB compaction 占用。

Prefer online volume growth, additional mounted capacity, or removal of verified non-rr logs/core dumps on the same
filesystem. Do not trigger a fresh snapshot to reclaim emergency space: snapshot construction needs temporary space and
does not immediately solve RocksDB compaction usage.

若整目录迁移不可避免，先 drain、正常停止，再离线 `rr-admin backup`，恢复到新卷上的空目录，更新 `data_dir` 后启动。不要对
运行中的目录做普通文件复制。

If whole-directory migration is unavoidable, drain and stop gracefully, create an offline `rr-admin backup`, restore
into an empty directory on the new volume, update `data_dir`, then start. Never copy a live data directory with ordinary
file commands.

## 根因分析 / Root-Cause Investigation

1. **日志增长快于快照。** 用 `group snapshot-status` 检查 `last_snapshot_index` 是否推进，并监测
   `rr_snapshot_build_failures_total`。默认每 50,000 条日志触发一次，阈值由 `snapshot_logs_since_last` 控制。<br />
   **Log growth outpacing snapshots.** Check whether `last_snapshot_index` advances and monitor
   `rr_snapshot_build_failures_total`. The default trigger is every 50,000 entries, controlled by
   `snapshot_logs_since_last`.
2. **RocksDB compaction 积压。** 检查各组实际 RocksDB 目录下的 `LOG` 是否有 `Slowdown`、`Stop`、写失败或重复
   compaction。<br />
   **Compaction backlog.** Inspect each group's actual RocksDB `LOG` for `Slowdown`, `Stop`, write failures, or repeated
   compaction.
3. **写入激增。** 比较 `rate(rr_proposal_duration_seconds_count[5m])` 与正常基线，排查上游流量或重试风暴。<br />
   **Runaway writes.** Compare proposal rate with baseline and inspect upstream traffic or retry storms.
4. **共享文件系统。** 检查日志、core dump、其他数据库和备份是否写入同一卷。<br />
   **Other tenants.** Check whether logs, core dumps, other databases, or backups share the volume.

## 恢复 / Recovery

1. 确认节点重新加入所有预期 group，leader 与其他节点一致。<br />
   Confirm that the node rejoins every expected group and observes the same leader.
2. 观察 leader 上的 `rr_raft_replication_lag{follower_node_id="N"}` 持续下降，或在暂停写入时比较各节点
   `last_log_index`。<br />
   Watch the leader's `rr_raft_replication_lag{follower_node_id="N"}` decline, or compare `last_log_index` across nodes
   while writes are paused.
3. 若 leader 使用 InstallSnapshot，等待对应接收指标和日志确认完成；恢复时间取决于状态大小、磁盘和网络，不使用固定估算。<br />
   If the leader sends InstallSnapshot, wait for receiver metrics and logs to confirm completion. Recovery time depends
   on state size, disk, and network; do not assume a fixed estimate.

```bash
rr-admin --addr rr-N:52001 node status
rr-admin --addr rr-N:52001 group snapshot-status --group 1
```

## 预防 / Prevention

- 磁盘低于 20% 警告，低于 10% 分页告警；同时监测 inodes。<br />
  Warn below 20% free disk and page below 10%; monitor inodes too.
- 为 RocksDB compaction 和快照临时文件预留空间，生产数据使用专用卷。<br />
  Reserve headroom for RocksDB compaction and snapshot temporary files, using a dedicated production volume.
- 根据恢复时间目标设置 `snapshot_logs_since_last`，不要依赖定时手工快照控制正常日志增长。<br />
  Tune `snapshot_logs_since_last` to recovery objectives rather than relying on manual snapshots for normal log growth.
- 备份输出必须放在 `data_dir` 外的独立卷。<br />
  Keep backup output outside `data_dir`, on a separate volume.

## 相关 / Related

- [Leader 不稳定 / Leader instability](./leader-flapping.md)
- [指标 / Metrics](../../reference/metrics-reference.md)
- [部署建议 / Deployment guidance](../../deployment/deployment.md)
