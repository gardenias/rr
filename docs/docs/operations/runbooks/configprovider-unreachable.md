---
sidebar_position: 6
---

# ConfigProvider 不可达手册 / Runbook: ConfigProvider Unreachable

适用于使用外部 ConfigProvider（etcd、Consul、自定义）的 legacy 集群。Managed formation 还依赖 etcd lifecycle store，但它
不是 routing-table ConfigProvider；两者影响不同。默认 `MemConfigProvider` 是进程内实现，不存在网络不可达。

Use this runbook for legacy clusters with an external ConfigProvider (etcd, Consul, or custom). Managed formation also
depends on an etcd lifecycle store, which is not the routing-table ConfigProvider; their failure modes differ. The
default `MemConfigProvider` is in-process and has no network outage mode.

## 症状 / Symptoms

外部 ConfigProvider 故障时，日志出现 provider timeout、watch 断开或 stale cache 警告。已有路由缓存的节点可继续按最后已知
路由服务，但不能安全依赖新配置传播。

An external ConfigProvider outage produces provider timeouts, watch disconnects, or stale-cache warnings. Nodes with a
cached routing table can continue using the last known routes, but new configuration propagation cannot be assumed safe.

Managed lifecycle store 故障时，已初始化节点可继续本地 Raft 读写，但 `/ready` 为 503 degraded；空节点不会初始化，
formation/membership 操作可能被拒绝。

During a managed lifecycle-store outage, initialized nodes can continue local Raft reads/writes, but `/ready` returns
503 degraded. Pristine nodes do not initialize, and formation/membership operations can be rejected.

## 检测 / Detection

```promql
increase(rr_config_etcd_stale_serves_total[5m]) > 0
rate(rr_config_etcd_watch_reconnects_total[5m]) > 0
etcd_server_has_leader == 0
```

前两项只适用于 `rr-config-etcd`；其他实现需要各自指标。逐节点检查 provider 连通和 rr readiness：

The first two queries apply only to `rr-config-etcd`; other implementations need their own metrics. Check provider
connectivity and rr readiness from every node:

```bash
etcdctl --endpoints=http://etcd:2379 endpoint status
curl -fsS http://rr-0:9100/ready
rr-admin --addr rr-0:52001 formation status
```

安全模式下，为 `rr-admin` 添加 `--token-env RR_ADMIN_TOKEN` 和三个 TLS 参数。`formation status` 不适用于未启用 managed
formation 的 legacy 集群。

In secure mode, add `--token-env RR_ADMIN_TOKEN` and all three TLS flags to `rr-admin`. `formation status` does not
apply to a legacy cluster without managed formation.

## 立即处置 / Immediate Mitigation

先恢复配置服务，不要改 rr 拓扑。确认现有读写是否仍可工作：

Restore the configuration service first; do not change rr topology. Check whether existing reads/writes still work:

```bash
rr-admin --addr rr-0:52001 node status
printf 'put smoke now\nget smoke\n' | rr-admin repl --addr rr-0:51001 --group 1
```

`repl` 使用 ClientService 端口，不是 Admin 端口。若现有写入也失败，转 [网络分区](./network-partition.md) 或
[Leader 反复切换](./leader-flapping.md)。

`repl` targets the ClientService port, not Admin. If existing writes also fail, switch to
[Network Partition](./network-partition.md) or [Leader Flapping](./leader-flapping.md).

冻结路由表、节点增删和 partition 变更，直到 source of truth 与所有缓存一致。`transfer-leader` 不写 leader 到
ConfigProvider，不需要仅因 provider outage 禁止；若 managed lifecycle 不是 Active，成员操作仍会被拒绝。

Freeze routing-table, node-membership, and partition changes until the source of truth and every cache agree.
`transfer-leader` does not write leadership to the ConfigProvider and need not be prohibited solely for a provider
outage. Membership operations can still be rejected while managed lifecycle is not Active.

按实现恢复服务：

Restore the service according to its implementation:

- etcd：检查 `endpoint health`、leader、磁盘和网络。<br />
  For etcd, inspect endpoint health, leader, disk, and network.
- Consul：检查 `consul operator raft list-peers` 和服务日志。<br />
  For Consul, inspect Raft peers and service logs.
- 自定义实现：检查健康端点、日志、认证和 watch 重连行为。<br />
  For custom providers, inspect health, logs, authentication, and watch-reconnect behavior.

不要例行重启 rr。只有确认自定义 provider 的 watch 不会在断流后恢复时，才按其实现要求进行受控重启。

Do not restart rr routinely. Use a controlled restart only when the custom provider is known not to recover its watch
after a disconnect.

## 根因分析 / Root-Cause Investigation

1. **服务不可用。** 按 etcd、Consul 或自定义服务的 playbook 处理。<br />
   **Service down.** Follow the provider's own service playbook.
2. **网络中断。** 从每个 rr 节点测试，而不是只从运维机测试。见 [网络分区](./network-partition.md)。<br />
   **Network path broken.** Test from every rr node, not only an operator workstation. See
   [Network Partition](./network-partition.md).
3. **认证或授权。** 检查证书有效期、token 轮换和 ACL 变更。<br />
   **Authentication or authorization.** Check certificate expiry, token rotation, and ACL changes.
4. **数据损坏。** 从 provider 备份恢复路由数据，或根据已核验拓扑重建；不要用 rr 节点数据目录代替 ConfigProvider 备份。<br />
   **Corrupt provider data.** Restore routing data from the provider backup or rebuild it from verified topology; an rr
   node data directory is not a ConfigProvider backup.
5. **自定义实现缺陷。** 检查 watch 提前结束、重连、typed error 和 panic；故障应返回 `ProviderUnavailable` 等错误，以便使用
   缓存，而不是静默返回空路由。<br />
   **Custom implementation bug.** Inspect early watch termination, reconnects, typed errors, and panics. Failures should
   return errors such as `ProviderUnavailable` so cached routing can be used, never an empty route silently.

## 恢复 / Recovery

1. 等 provider 健康、watch 恢复、日志不再出现 stale/provider error。<br />
   Wait for provider health, watch recovery, and the disappearance of stale/provider errors.
2. 对比 rr 路由缓存和 source of truth。<br />
   Compare rr's routing view with the source of truth.
3. Managed 集群确认 `formation status` 为 Active，所有节点 `/ready` 恢复。<br />
   For managed clusters, confirm Active formation and restored `/ready` on every node.
4. 仅重试明确失败或尚未执行的配置操作；不要盲目重放可能已成功的写。<br />
   Retry only operations known to have failed or never run; do not blindly replay configuration writes that may have
   succeeded.

```bash
rr-admin --addr rr-0:52001 --output json config show > /tmp/rr-routing.json
rr-admin --addr rr-0:52001 formation status
```

## 预防 / Prevention

- ConfigProvider 和 lifecycle store 使用高可用部署，例如跨 AZ 的 3+ 节点 etcd。<br />
  Run ConfigProvider and lifecycle store in high-availability form, such as 3+ etcd nodes across AZs.
- 设置有界的连接和读取超时，并监测 stale serve、watch reconnect 和 readiness。<br />
  Use bounded connection/read timeouts and monitor stale serves, watch reconnects, and readiness.
- 监控系统独立于 rr 的 ConfigProvider，避免级联故障。<br />
  Keep monitoring independent of rr's ConfigProvider to avoid cascading failure.
- 定期备份 provider 数据，并演练 outage、恢复和缓存对账。<br />
  Back up provider data regularly and rehearse outage, recovery, and cache reconciliation.

## 相关 / Related

- [ShardRouter + ConfigProvider 架构 / architecture](../../architecture/c4/components-router.md)
- [配置传播 / Configuration propagation](../../architecture/c4/dynamic-config-update.md)
- [自定义实现 / Custom implementations](../../guide/config-provider.md)
