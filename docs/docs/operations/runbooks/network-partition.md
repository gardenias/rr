---
sidebar_position: 4
---

# 网络分区手册 / Runbook: Network Partition

## 症状 / Symptoms

部分节点无法互通，一侧可提交写入，另一侧报 `NotLeader` 或超时；`rr_channel_reconnects_total` 增长，监控视角中的部分节点
不可达。

Some nodes cannot communicate. One side may commit writes while the other returns `NotLeader` or times out;
`rr_channel_reconnects_total` rises and some nodes become unreachable from monitoring.

```text
WARN rr_raft::network_factory: failed to append_entries to node 3: transport error: ... Connection refused
```

多数派可继续提交，少数派不能提交新写。少数派的本地读可能返回旧数据，不应把它描述为正常只读服务。Raft 防止同一 term 出现
两个合法 leader，但分区仍会造成可用性下降和客户端路由问题。

A majority can continue committing; a minority cannot commit new writes. Local reads on the minority can return stale
data and must not be presented as normal read-only service. Raft prevents two legitimate leaders in one term, but a
partition still degrades availability and client routing.

## 检测 / Detection

```promql
increase(rr_channel_reconnects_total[5m]) > 30
probe_success{job="rr-peer-probe"} == 0
```

从每个节点向其他节点探测实际配置的 Raft 地址。TCP 成功只证明端口可连，不证明 TLS、认证或 Raft RPC 正常。

Probe the configured Raft addresses from every node. TCP success proves only port reachability, not TLS, authentication,
or healthy Raft RPCs.

```bash
for peer in rr-0 rr-1 rr-2; do
  nc -zv "$peer" 51001
done
rr-admin --addr rr-0:52001 group status --group "$GROUP"
```

安全模式下，为 `rr-admin` 添加 `--token-env RR_ADMIN_TOKEN` 和三个 TLS 参数。

In secure mode, add `--token-env RR_ADMIN_TOKEN` and all three TLS flags to `rr-admin`.

## 立即处置 / Immediate Mitigation

### 绘制可达矩阵 / Map the Reachability Matrix

从每个节点执行探测，记录方向性；A→B 成功不代表 B→A 成功。根据实际 voter 集合判断哪一侧有多数派，而不是根据进程是否存活。

Probe from every node and record directionality; A→B success does not imply B→A. Determine which side has a majority
from the actual voter set, not from process liveness.

```text
     rr-0  rr-1  rr-2  rr-3  rr-4
rr-0  -     yes   yes   no    no
rr-1  yes   -     yes   no    no
rr-2  yes   yes   -     no    no
rr-3  no    no    no    -     yes
rr-4  no    no    no    yes   -
```

### 引导客户端 / Steer Clients

让客户端连接多数派侧的健康地址，并清理失效的本地连接。不要通过向少数派重复写入来“测试恢复”，这只会增加超时和重试压力。

Point clients at healthy majority-side addresses and clear dead local connections. Do not repeatedly write to the
minority as a recovery probe; it only adds timeouts and retry pressure.

### 冻结成员变更 / Freeze Membership Changes

分区期间不要用 `change-membership` 排除暂时不可达节点。少数派上的变更无法提交，不会因此自动产生双主；多数派上排除节点则
会真实改变 quorum，必须只用于已确认永久损失且剩余 voter 仍满足多数派的灾难场景。

Do not use `change-membership` to evict temporarily unreachable nodes during a partition. A change on the minority
cannot commit and does not automatically create split-brain; a change on the majority really changes quorum and belongs
only to confirmed permanent-loss disaster recovery where remaining voters still form a majority.

不要对少数派使用 `--force`，也不要把 force 当成绕过网络分区的工具。

Never use `--force` on the minority or treat it as a way to bypass a network partition.

## 根因分析 / Root-Cause Investigation

1. **物理或云网络故障。** 检查云事件、路由、NIC、交换机和 peering。<br />
   **Physical or cloud-network failure.** Inspect cloud incidents, routes, NICs, switches, and peering.
2. **防火墙变更。** 检查最近部署、security group、`iptables`/`nft`，只开放实际配置的 Client/Raft 端口；Admin 只向运维网段
   开放。<br />
   **Firewall change.** Inspect recent deployments, security groups, and packet filters. Open only configured Client/Raft
   ports; restrict Admin to operations networks.
3. **Kubernetes 策略。** 检查 NetworkPolicy、Istio AuthorizationPolicy 和 egress 是否允许 StatefulSet Pod 互通。<br />
   **Kubernetes policy.** Check NetworkPolicy, Istio AuthorizationPolicy, and egress for StatefulSet peer traffic.
4. **MTU 不匹配。** 小包可用而大 AppendEntries 失败时，检查隧道/VLAN MTU 和 path MTU discovery，不要盲目改成固定值。<br />
   **MTU mismatch.** If small packets work but large AppendEntries fail, inspect tunnel/VLAN MTU and path MTU discovery
   instead of blindly forcing a value.
5. **TLS 证书。** 检查 `certificate expired`、`UnknownIssuer` 和 SAN；见
   [TLS 轮换](../../deployment/tls.md#证书轮换--certificate-rotation)。<br />
   **TLS certificates.** Inspect expiry, issuer, and SAN errors and follow
   [TLS rotation](../../deployment/tls.md#证书轮换--certificate-rotation).
6. **DNS 故障。** 从 Pod/节点内部检查解析和 CoreDNS；临时 IP 直连也必须与证书 SAN 或显式 TLS domain 匹配。<br />
   **DNS failures.** Test resolution from inside the Pod/node and inspect CoreDNS. Temporary IP dialing must still match
   certificate SANs or an explicit TLS domain.

## 恢复 / Recovery

网络恢复后通常无需重启。确认连接重建、leader 唯一且稳定、复制 lag 下降；落后过多时等待 InstallSnapshot 完成。写入期间各
节点 `last_log_index` 不要求每一时刻完全相等，应在暂停写入后收敛或持续增长时保持有限 lag。

After the network heals, a restart is usually unnecessary. Confirm rebuilt connections, one stable leader, and
decreasing replication lag; wait for InstallSnapshot if a node is too far behind. During active writes, `last_log_index`
need not be identical at every instant; it should converge when writes pause or remain within bounded lag during growth.

```bash
rr-admin --addr rr-0:52001 group membership --group "$GROUP"
rr-admin --addr rr-0:52001 group status --group "$GROUP"
```

```promql
rr_raft_replication_lag
```

若少数派节点数据损坏，先在多数派侧确认集群健康，再按 [快照损坏手册](./snapshot-corruption.md) 做单节点恢复。若怀疑实现缺
陷导致两侧都提交冲突写，立即停止两侧写入、保存证据并升级事故；不要自行选择一侧覆盖另一侧。

If a minority node's data is corrupt, first verify majority-side health, then follow
[Snapshot Corruption](./snapshot-corruption.md) for single-node recovery. If an implementation defect is suspected of
allowing conflicting commits on both sides, stop writes on both sides, preserve evidence, and escalate; do not choose
one side to overwrite the other ad hoc.

## 预防 / Prevention

- 跨 AZ 分布 voter，使任一 AZ 故障不夺走多数派。<br />
  Spread voters across AZs so one AZ loss cannot remove the majority.
- 按容错目标选择 3 或 5 个 voter；5 个可容忍 2 个故障，但成本和延迟更高。<br />
  Choose three or five voters from fault-tolerance goals; five tolerate two failures at higher cost and latency.
- 在 staging 验证 NetworkPolicy、防火墙和 TLS 变更。<br />
  Validate NetworkPolicy, firewall, and TLS changes in staging.
- 用 `probe_ssl_earliest_cert_expiry` 等证书指标提前告警。<br />
  Alert early with certificate metrics such as `probe_ssl_earliest_cert_expiry`.
- 定期做分区演练；集成测试 `test_leader_isolation_and_recovery` 覆盖同类场景，生产可使用
  [Chaos Mesh](https://chaos-mesh.org/)。<br />
  Rehearse partitions regularly; `test_leader_isolation_and_recovery` covers a similar scenario, and
  [Chaos Mesh](https://chaos-mesh.org/) can be used in production-like environments.

## 相关 / Related

- [Leader 不稳定 / Leader instability](./leader-flapping.md)
- [证书轮换 / Certificate rotation](../../deployment/tls.md)
- [多 AZ 部署 / Multi-AZ deployment](../../architecture/c4/deployment.md)
