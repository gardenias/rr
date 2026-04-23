---
sidebar_position: 5
---

# Runbook — 快照损坏 / Snapshot Corruption

## Symptom / 症状

`InstallSnapshot` RPC 在 follower 侧解码或应用失败。典型日志条目：

`InstallSnapshot` RPC fails on the follower side during decode or apply:

```
ERROR rr_raft::sm_adapter: install_snapshot: restore() returned error: codec error: deserialize failed: ...
ERROR rr_raft::sm_adapter: install_snapshot: state machine snapshot bytes failed bincode decode: ...
ERROR rr_raft::service_adapter: restore propagated application error: ...
```

以及 `rr_snapshot_installs_total` **不增长**（失败不计数）+ follower 的 `rr_raft_last_log_index` 卡在某个位置不动。

Plus `rr_snapshot_installs_total` does **not** increment (the failure
counter is not counted) and the follower's `rr_raft_last_log_index`
is stuck.

## Detection / 检测

```promql
# PromQL — 某节点 follower last_applied 与 leader commit 差距 > 10000 持续 5 分钟
(
  max by (group_id) (rr_raft_last_log_index{is_leader="1"})
  - min by (group_id) (rr_raft_last_log_index{is_leader="0"})
) > 10000
```

手动 / Manual:

```bash
for nid in 1 2 3; do
  rr-admin --addr rr-$((nid-1)):52001 group status --group $GROUP --output json | \
    jq "{node: $nid, is_leader: .is_leader, last: .last_log_index}"
done
```

## Immediate Mitigation / 立即处置

### Step 1 — 确认 quorum 不受影响 / Confirm quorum is not impacted

只要健康节点数 ≥ ⌈N/2⌉ + 1，集群仍可读写。继续后续步骤而不必紧急响应。

As long as healthy nodes ≥ majority, the cluster keeps serving. Proceed
without emergency urgency.

### Step 2 — 识别是 sender 还是 receiver 问题 / Sender vs receiver bug

**Sender 问题** (snapshot 本身 bad):
- 同一个 snapshot 发给多个 follower，**全部** 失败
- Leader 上 `Service::snapshot()` 可能返回了损坏数据

**Receiver 问题** (网络传输过程损坏):
- 只有**某一个** follower 失败，其他 follower 的 `rr_snapshot_installs_total` 正常递增
- 通常伴随 `rr_channel_reconnects_total` 升高

### Step 3 — Sender bug 处置

1. 停止接受客户端写入 (gateway / load balancer 把流量切走)
2. `rr-admin group transfer-leader` 到另一节点
3. 在原 leader 上跑 backup → 留证 → `rr-admin group snapshot` 强制新 snapshot
4. 如果新 snapshot 能传递成功 → 问题是 flaky bit（磁盘 / 内存），本 runbook 的 "Root Cause" 段继续
5. 如果新 snapshot 仍失败 → **你的 `Service::snapshot()` 实现有 bug**，见下

### Step 4 — Receiver bug 处置

1. 停止涉事 follower 节点
2. 清理它的 data_dir：`rr-admin backup --data-dir ...` 备存，然后 `rm -rf /var/lib/rr/data/*`（**确认集群有 quorum 再做**）
3. 重启节点。它会作为 fresh follower 请求完整 snapshot 并应用
4. 观察 `rr_snapshot_installs_total{node_id=<N>}` +1

## Root-Cause Investigation / 根因分析

按可能性：

### 1. 应用层 `Service::snapshot()` / `restore()` bug / Application Service bug

**证据**: 自定义 Service 实现；错误 message 含业务字段名。

**Fix**: 修应用代码。常见 bug:
- `snapshot()` 里读数据不用 read-lock，快照拍到不一致状态
- `restore()` 没 **清空** 现有状态就 merge
- 新字段加了但没 `#[serde(default)]` 导致旧 snapshot 解码失败

这是最常见的根因，因为 rr 框架本身的 bincode + snapshot 流程很稳定。

### 2. 磁盘 bit rot / Disk bit rot

**证据**: `dmesg` 有 I/O error；smartctl 显示 pending sectors；校验和失败。

**Fix**: 换磁盘；依靠 Raft replication 从其他副本拉回。

### 3. rr-raft ↔ rr-network 的本地 proto 不同步 / Local proto drift

**证据**: 刚升级 rr-raft 而 rr-network 版本不一致，或反之。日志 `unexpected field number in proto decode`。

**Fix**: 确保所有节点使用相同版本。rr 的内部 proto 在 rr-raft 和 rr-network 里被重复定义（故意打破循环依赖），如果两处 drift 会 decode error。

### 4. 网络中途损坏 / Corruption in transit

**证据**: TLS 未启用 + 走过 lossy link；或 TCP checksum offloading bug（`ethtool -K <nic> rx off tx off`）。

**Fix**: 启用 TLS（TLS 里的 MAC 会 fail-fast 而不是 silently pass corrupted data 给应用层）。

### 5. 磁盘写未 fsync / Writes not fsync'd

**证据**: `rocks.rs::DurabilityPolicy != Strict`；crash 前的 snapshot 没完整落盘。

**Fix**: 配置 `durability = "strict"`（Phase 3 默认）。

## Recovery / 恢复

按 Step 3 或 Step 4 的路径完成后：

After the recovery path completes:

1. 确认 `rr_snapshot_installs_total` 所有节点最终收敛到相同值（可能相差 1-2 表示传输窗口）
2. `rr_raft_last_log_index` 所有节点一致
3. 跑 sanity check：写一条 known test payload，确认 leader 返回 `Ok(response)`；read-back 验证

## Prevention / 预防

- **TLS on** — 保护数据完整性 end-to-end
- **`DurabilityPolicy::Strict`** — 默认；不要改
- **Service trait 测试** — 对自定义 Service 跑 property test：随机写入后 snapshot → restore → 比对所有键值
- **版本对齐** — 升级时跨节点版本差距 ≤ 1 个 patch
- **磁盘健康监控** — SMART attrs (`smart_device_health_ok`) 导出到 Prometheus
- **Raft 定期 checkpoint** — 通过 `rr-admin group snapshot` 定时强制以 cap error blast radius

## 相关 / Related

- [../../architecture/c4/dynamic-snapshot-transfer.md](../../architecture/c4/dynamic-snapshot-transfer.md) — snapshot 传输机制
- [../../guide/service-trait.md](../../guide/service-trait.md) — Service::snapshot/restore 契约
- [disk-full.md](./disk-full.md) — 磁盘压力也会导致 snapshot 失败
