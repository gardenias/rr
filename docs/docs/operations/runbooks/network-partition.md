---
sidebar_position: 4
---

# Runbook — 网络分区 / Network Partition

## Symptom / 症状

部分 rr 节点相互无法通信，导致集群分裂成两个或多个子集。症状：

- 一侧客户端成功写入；另一侧报 `RaftError::NotLeader` 或超时
- `rr_channel_reconnects_total` 在多个节点上爆增
- 节点日志：
  ```
  WARN rr_raft::network_factory: failed to append_entries to node 3: transport error: ... Connection refused
  ```
- Prometheus `up{job="rr"}` 部分节点显示 `0`（监控器视角）

**重要事实 / Important**: Raft 的设计能**安全地处理分区** —— 多数方继续工作，少数方只读/不写。**真正的问题是 split-brain 风险（不会发生于 rr，因为 Raft 保证最多一个 leader per term），以及长期 availability 降级。**

## Detection / 检测

```promql
# PromQL — 某节点 5 分钟内 reconnect > 30 次（持续断连）
increase(rr_channel_reconnects_total[5m]) > 30

# PromQL — 某节点 node_id 视角看不到其他节点
# （需要 blackbox_exporter 或 dedicated probe）
probe_success{job="rr-peer-probe"} == 0

# 应用层 / Application-level
# A 端客户端写入率为 0 而 B 端仍在写 → 分区高度可疑
```

手动确认 / Manual confirm:

```bash
# 从节点 A 测试能否连到其他节点的 Raft port / From node A, can reach peers?
for peer in rr-0 rr-1 rr-2; do
  echo "--- $peer ---"
  nc -zv $peer 51001 2>&1 | head -1
done
```

## Immediate Mitigation / 立即处置

### Step 1 — 映射分区拓扑 / Map the partition topology

**从每个节点 ssh 过去运行 probe**，画出可达矩阵：

```
     rr-0  rr-1  rr-2  rr-3  rr-4
rr-0  -    ✓     ✓     ✗     ✗
rr-1  ✓    -     ✓     ✗     ✗
rr-2  ✓    ✓     -     ✗     ✗
rr-3  ✗    ✗     ✗     -     ✓
rr-4  ✗    ✗     ✗     ✓     -
```

这个矩阵告诉你哪一侧有 quorum（此例 3/5 → {0,1,2} 侧仍可写，{3,4} 只读）。

### Step 2 — 警告客户端写入将倾斜 / Warn consumers that writes will skew

如果你的应用（网关等）能识别 leader 地址，让它们只连接 quorum 侧。否则客户端会经历超时（因为它们缓存的 leader 可能在 minority 侧）。

### Step 3 — **不要手动 `rr-admin change-membership`** 除非确定

人为改变 membership 在分区期间是**最危险的操作**。若你在 minority 侧改成员去排除 majority，你会造成 split-brain（两侧都觉得自己有 quorum）。**仅当确信另一侧永久下线时才改 membership**（例如 DC 失火全毁）。

## Root-Cause Investigation / 根因分析

### 1. 物理网络故障 / Physical network failure

**证据**: 云厂商状态页报 AZ / region 事件；`ip route` 异常；switch/router 监控告警。

**Fix**: 等网络恢复；修 NIC / cable / peering。

### 2. 防火墙规则变更 / Firewall rule change

**证据**: 最近有部署/配置变更；`iptables -L -n -v` 或 `nft list ruleset` 显示新规则；security group/NetworkPolicy 改过。

**Fix**: 回滚；或在所有节点打开 51001（Client+Raft）、52001（Admin，仅从 ops 网段）。

### 3. K8s NetworkPolicy / Istio 策略 / K8s NetworkPolicy or Istio policies

**证据**: `kubectl describe networkpolicy`、Istio AuthorizationPolicy 最近被改。

**Fix**: 回滚 policy；确保 rr StatefulSet 的 pods 能互相 egress 51001。

### 4. MTU 不匹配 / MTU mismatch

**证据**: 小包能过，大包（AppendEntries with batch）丢；`ping -s 8000 peer` 失败而 `ping peer` 成功；新 VLAN / VPN 隧道最近上线。

**Fix**: 在 NIC 上降 MTU 到 1500 或 enable path MTU discovery。

### 5. TLS 证书过期 / Expired TLS certs

**证据**: 日志 `certificate expired`、`UnknownIssuer`；`openssl x509 -in cert.pem -noout -enddate` 显示 past-due。

**Fix**: 立刻 rotate 证书（见 [tls.md](../../deployment/tls.md#证书轮换--certificate-rotation)）。

### 6. DNS 解析失败 / DNS failures

**证据**: `nslookup peer.domain` 从节点内部失败；`CoreDNS` 日志错误。

**Fix**: 修 CoreDNS；临时用 IP 直连。

## Recovery / 恢复

### 分区自动愈合后 / After partition heals on its own

1. **不要重启节点** — Raft 会自动恢复。旧 minority 侧的节点会作为 follower 重新加入。
2. **等 catch-up 完成** — `rr_raft_last_log_index` 在所有节点收敛到同一值。慢节点可能要几分钟或触发 `InstallSnapshot`。
3. **验证 quorum 健康** — `rr-admin group status --group $GID` 显示所有 voters 均可见。
4. **如果 minority 侧有节点写入被应用** — 在 Raft 里这**不可能发生**（minority 无 leader），但如果曾 temporary 被误认为 leader（例如 GC pause 延迟了 step-down），该 writes 会被新的 log entries 覆盖。这是 Raft 的正确行为。

### 需要手动干预的情况 / When manual intervention is needed

- **Minority 侧节点**的 cert 全过期、配置全坏了 → `rr-admin backup` 导出后 wipe → rebuild 从 majority 侧 replicate。
- **分区后两侧都认为自己 quorum**（rr 不应该导致；开源 etcd 有过类似 bug 场景）→ 证据收集 → 停止写入两侧 → 选择一侧为 source of truth → 另一侧做 restore。

## Prevention / 预防

- **跨 AZ 部署** — 每个 rr 节点在独立 availability zone。3 AZ × 3 节点最小化单 AZ 故障影响。
- **Quorum size** — 5 节点容忍 2 宕 > 3 节点容忍 1 宕。生产优先 5+。
- **NetworkPolicy 测试** — 每个 NetworkPolicy 变更都在 staging 的活 rr 集群跑一遍。
- **TLS 证书监控** — Prometheus `blackbox_exporter` 的 `probe_ssl_earliest_cert_expiry` 告警 7 天前。
- **Chaos testing** — 定期注入分区（rr 的集成测试套件 `test_leader_isolation_and_recovery` 就是这种场景；生产可用 [Chaos Mesh](https://chaos-mesh.org/) 复现）。

## 相关 / Related

- [leader-flapping.md](./leader-flapping.md) — 间歇性网络问题的症状
- [tls.md](../../deployment/tls.md) — 证书 rotation
- [../../architecture/c4/deployment.md](../../architecture/c4/deployment.md) — 多 AZ 部署推荐
