---
sidebar_position: 5
---

# 快照损坏手册 / Runbook: Snapshot Corruption

## 症状 / Symptoms

Follower 的 InstallSnapshot 解码/恢复失败，或 Rocks 节点启动时无法读取 `sm-snapshot.bin`。持久化快照损坏会 fail-closed，
节点不会从空状态静默启动。

A follower's InstallSnapshot decode/restore fails, or a Rocks node cannot read `sm-snapshot.bin` at startup. A corrupt
persisted snapshot fails closed; the node never silently starts from empty state.

```text
ERROR rr_raft::sm_adapter: install_snapshot: restore() returned error: codec error: deserialize failed: ...
ERROR rr_raft::sm_adapter: install_snapshot: state machine snapshot bytes failed bincode decode: ...
ERROR snapshot store: failed to decode .../sm-snapshot.bin: ...
```

失败的安装不增加成功计数 `rr_snapshot_installs_total`，follower 可能持续落后。

Failed installs do not increment the success counter `rr_snapshot_installs_total`, and the follower can remain behind.

## 检测 / Detection

```promql
rr_raft_replication_lag > 10000
increase(rr_snapshot_build_failures_total[5m]) > 0
increase(rr_framing_decode_failures_total[5m]) > 0
```

`rr_raft_replication_lag` 只在 leader 上发射。逐节点查询实际状态和快照元数据：

`rr_raft_replication_lag` is emitted only by the leader. Query actual state and snapshot metadata on each node:

```bash
for node in rr-0 rr-1 rr-2; do
  rr-admin --addr ${node}:52001 --output json group status --group "$GROUP"
  rr-admin --addr ${node}:52001 --output json group snapshot-status --group "$GROUP"
done
```

安全模式下，为 `rr-admin` 添加 `--token-env RR_ADMIN_TOKEN` 和三个 TLS 参数。

In secure mode, add `--token-env RR_ADMIN_TOKEN` and all three TLS flags to `rr-admin`.

## 立即处置 / Immediate Mitigation

多数派数量为 `floor(N/2)+1`。先确认健康 voter 仍占多数；若已失去 quorum，停止所有清盘/重置操作，保存证据并进入灾难恢复，
不要继续重置节点。

A majority is `floor(N/2)+1`. First confirm that healthy voters still form a majority. If quorum is already lost, stop
all wipe/reset work, preserve evidence, and enter disaster recovery; do not reset more nodes.

### 判断发送端或接收端 / Distinguish Sender from Receiver

- 同一快照到多个 follower 都失败，优先检查 leader 的快照构建、应用 `Service::snapshot()` 和版本兼容。<br />
  If the same snapshot fails on multiple followers, inspect leader snapshot construction, application
  `Service::snapshot()`, and version compatibility first.
- 只有一个 follower 失败，优先检查该节点本地磁盘、已有快照、应用恢复逻辑和版本；不要仅凭这一点归因于网络损坏。<br />
  If only one follower fails, inspect its local disk, existing snapshot, application restore logic, and version; do not
  attribute it to network corruption on that evidence alone.

### 发送端持续产生坏快照 / Sender Repeatedly Produces Bad Snapshots

暂停非必要写入，选择健康 voter，并在目标节点自己的 Admin API 上触发一次 leader transfer。保存失败日志和快照证据后，再在
新 leader 上触发快照并观察其他 follower。

Pause nonessential writes, choose a healthy voter, and trigger one leadership transfer through that target's own Admin
API. Preserve failed logs and snapshot evidence, then trigger a snapshot on the new leader and observe other followers.

```bash
rr-admin --addr HEALTHY_TARGET:52001 group transfer-leader --group "$GROUP" --to TARGET_ID
rr-admin --addr NEW_LEADER:52001 group snapshot --group "$GROUP"
```

若新 leader 仍产生无法恢复的快照，停止该组写入并修复应用快照实现；不要反复清空 follower。

If the new leader also produces unrestorable snapshots, stop writes to that group and fix the application's snapshot
implementation; do not repeatedly wipe followers.

### 单节点本地恢复 / Single-Node Local Recovery

以下 destructive 流程只适用于支持动态成员替换的 legacy 集群。Managed P1 不支持副本替换，不要执行这套 reset/rejoin 步骤。

The following destructive flow is only for legacy clusters that support dynamic membership replacement. Managed P1 does
not support replica replacement; do not run this reset/rejoin sequence there.

1. 确认其他健康节点有多数派，并从健康节点预检、退役受损节点。<br />
   Confirm a healthy majority elsewhere, then dry-run and decommission the damaged node from a healthy member.
2. 正常停止受损节点，为取证尽量做离线备份。<br />
   Stop the damaged node gracefully and attempt an offline backup for forensics.
3. 核对 `data_dir` 后使用带预检的 `node-reset --force`；不要手工 `rm -rf`。<br />
   Verify `data_dir` and use the guarded `node-reset --force`; never substitute manual `rm -rf`.
4. 按原节点身份启动空节点，在当前 leader 上重新添加 learner。<br />
   Start the empty node with its original identity and re-add it as a learner through the current leader.
5. 等待追平。默认自动提升开启时无需手工 promote；关闭时设置完整目标 voter 集合。<br />
   Wait for catch-up. Default auto-promotion needs no manual promotion; if disabled, set the complete target voter list.

```bash
rr-admin --addr HEALTHY_NODE:52001 node decommission --node-id 3 --dry-run
rr-admin --addr HEALTHY_NODE:52001 node decommission --node-id 3
# 在受损节点正常停止 rr-server 后 / after gracefully stopping the damaged node:
rr-admin backup --data-dir /var/lib/rr/data --output /var/backups/rr/node3-corrupt.tar.gz --node-id 3
rr-admin node-reset --data-dir /var/lib/rr/data --force
# 启动空节点后，在当前 leader 上 / after starting it empty, on the current leader:
rr-admin --addr CURRENT_LEADER:52001 group add-learner --group 1 --node-id 3 --addr rr-2:51001
rr-admin --addr CURRENT_LEADER:52001 group change-membership --group 1 --voters 1,2,3
```

`node-reset` 会删除整个 `data_dir`，`--force` 就是最终确认，没有交互提示。路径错误、节点未停止或 quorum 不明确时都不要执
行。

`node-reset` deletes the entire `data_dir`; `--force` is the final confirmation and there is no interactive prompt.
Never run it with an uncertain path, a live node, or unclear quorum.

## 根因分析 / Root-Cause Investigation

1. **应用快照实现。** 检查 `snapshot()` 是否持有合适读锁、`restore()` 是否替换而非重复合并旧状态、新增字段是否兼容旧快
   照。<br />
   **Application snapshot implementation.** Check snapshot locking, whether `restore()` replaces rather than repeatedly
   merges old state, and whether new fields remain backward-compatible.
2. **磁盘或文件损坏。** 检查内核 I/O 日志、SMART 和文件系统错误。原子写能避免撕裂写入，但不能修复介质 bit rot。<br />
   **Disk or file corruption.** Inspect kernel I/O logs, SMART, and filesystem errors. Atomic writes prevent torn writes
   but cannot repair media bit rot.
3. **版本或 schema 不匹配。** 核对所有节点实际二进制和快照 envelope 版本；带正确 magic 但 body 损坏或版本过高会被拒
   绝。<br />
   **Version or schema drift.** Check actual binaries and snapshot envelope versions on every node. Correct magic with a
   malformed body or a newer version is rejected.
4. **传输问题。** 只有存在明确链路/驱动证据时按此项排查；TLS 和 TCP checksum 不能修复加密前已损坏的应用字节。<br />
   **Transport corruption.** Investigate this only with concrete link/driver evidence; TLS and TCP checksums cannot
   repair application bytes corrupted before encryption.

## 恢复验证 / Recovery Verification

确认目标节点已回到预期 voter/learner 集合、复制 lag 下降、快照状态推进，并写读一个已知探针。`rr_snapshot_installs_total`
是历史累计计数，不同节点不需要收敛到相同值。

Confirm expected voter/learner membership, decreasing replication lag, advancing snapshot state, and a successful known
write/read probe. `rr_snapshot_installs_total` is a historical cumulative counter and need not converge to the same
value on every node.

```bash
rr-admin --addr CURRENT_LEADER:52001 group membership --group 1
rr-admin --addr RECOVERED_NODE:52001 group snapshot-status --group 1
printf 'put recovery:probe ok\nget recovery:probe\n' | rr-admin repl --addr CLIENT_ADDR:51001 --group 1
```

## 预防 / Prevention

- 生产启用 TLS + mTLS 和 Strict 持久化，监测快照构建/解码失败。<br />
  Use TLS + mTLS and Strict persistence in production, and monitor snapshot build/decode failures.
- 对自定义 Service 做 snapshot → restore → 状态比较测试，覆盖旧版本样本。<br />
  Test custom Service snapshot → restore → state comparison, including older-version fixtures.
- 按兼容矩阵升级，不在未验证的版本组合间传输快照。<br />
  Upgrade within the compatibility matrix and avoid snapshot transfer between untested version combinations.
- 监控磁盘健康和空间，定期验证备份可读取。<br />
  Monitor disk health and space, and verify backup readability regularly.

## 相关 / Related

- [快照传输 / Snapshot transfer](../../architecture/c4/dynamic-snapshot-transfer.md)
- [Service 契约 / Service contract](../../guide/service-trait.md)
- [磁盘压力 / Disk pressure](./disk-full.md)
