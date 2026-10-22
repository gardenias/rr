---
sidebar_position: 6
---

# Runbook — ConfigProvider 不可达 / ConfigProvider Unreachable

**适用范围**: 使用**外部** ConfigProvider（etcd、Consul、自定义）的生产集群。
使用默认 `MemConfigProvider` 的开发/测试集群不受此 runbook 影响（因为 config 是进程内的，不会 unreachable）。

---

**Scope**: Production clusters using an **external** ConfigProvider (etcd,
Consul, or a custom implementation). Dev/test clusters using the default
`MemConfigProvider` are immune (config is in-process and can't be
unreachable).

## Symptom / 症状

rr-server 日志报 ConfigProvider 错误，但 Raft 本身仍在运行。典型日志：

```
ERROR rr_server::node: watch: config change stream ended
WARN rr_server::node: config provider get_routing_table failed: provider unavailable: etcd connection timed out
WARN rr_router::router: routing table stale; using last-known cached copy
```

**关键事实**: rr 设计**能**容忍 ConfigProvider 短暂不可用。`ShardRouter` 缓存了上一次成功读到的 routing table 快照，所以**客户端请求仍然路由正确**。只有当你需要**变更** routing table（添加节点、迁移分区、调整成员）时，才会真正卡住。

**Critical**: rr is designed to tolerate brief ConfigProvider
unavailability. `ShardRouter` caches the last successfully-read routing
table snapshot, so **client requests continue to route correctly**. The
impact only surfaces when you need to **change** the routing table (add a
node, migrate a partition, adjust membership).

## Detection / 检测

```promql
# PromQL — etcd ConfigProvider 在 etcd 不可达时服务陈旧缓存（可用性退化信号）
increase(rr_config_etcd_stale_serves_total[5m]) > 0

# watch 重连频率（持续非零说明连接面不稳定）
rate(rr_config_etcd_watch_reconnects_total[5m]) > 0

# 或：如果你的 ConfigProvider 是 etcd
etcd_server_has_leader == 0
```

手动 / Manual:

```bash
# 测试从 rr 节点能否连到 ConfigProvider
for node in rr-0 rr-1 rr-2; do
  ssh $node "etcdctl --endpoints=http://etcd:2379 endpoint status"
done
```

## Immediate Mitigation / 立即处置

**哲学**: 由于 rr 能用 cached routing table 继续服务，你的首要目标是**恢复 ConfigProvider 本身**，而不是紧急改 rr 集群。

**Philosophy**: since rr continues to serve with a cached routing table,
your priority is restoring the **ConfigProvider itself**, NOT emergency
changes to the rr cluster.

### Step 1 — 确认 rr 集群仍健康 / Confirm rr cluster is still serving

```bash
# 读应该工作 / Reads should work
rr-admin --addr rr-0:52001 node status

# 写应该工作（到现有 partition）/ Writes should work (to existing partitions)
echo "smoke" | rr-admin --addr rr-0:52001 repl --group 1 <<< "put key=smoke value=now"
```

如果写入也失败 → 这是更严重的故障，转到 [network-partition.md](./network-partition.md) 或 [leader-flapping.md](./leader-flapping.md)。

### Step 2 — 诊断 ConfigProvider 本身 / Diagnose the ConfigProvider itself

跳出 rr 的视角，按你的 ConfigProvider 类型走：

- **etcd**: `etcdctl endpoint health`、查看 etcd leader、磁盘 I/O、网络
- **Consul**: `consul operator raft list-peers`
- **自定义**: 查该服务的健康端点、日志

### Step 3 — 等待 ConfigProvider 恢复 / Wait for the ConfigProvider to recover

rr 会**自动** resume 一旦 ConfigProvider 可达。`watch_changes` stream 会重连，新的 `RoutingTableChange` 事件会重新流入。通常不需要重启 rr 节点。

rr will **automatically** resume once the ConfigProvider is reachable.
The `watch_changes` stream reconnects; new `RoutingTableChange` events
flow back in. Usually **no rr-side restart** is needed.

### Step 4 — 冻结 membership 变更 / Freeze membership changes

在 ConfigProvider 不稳期间，**不要**运行：
- `rr-admin group transfer-leader`（会写回 leader 信息到 ConfigProvider）
- 添加 / 移除节点
- 创建 / 删除 partition

这些在 ConfigProvider 不可达时会失败，可能让路由表处于半更新状态。

**During ConfigProvider instability, do NOT run**:
- `rr-admin group transfer-leader` (writes back to ConfigProvider)
- Adding / removing nodes
- Creating / deleting partitions

These can leave the routing table in a half-updated state.

## Root-Cause Investigation / 根因分析

走完 ConfigProvider 侧的标准故障诊断后，确认根因：

### 1. ConfigProvider 本身宕机 / ConfigProvider is down

etcd / Consul / 自定义服务的标准故障场景。按那个服务的 playbook 处理。

### 2. 网络路径中断 / Network path broken

rr 节点 → ConfigProvider 的路径坏了，但双方都健康。见 [network-partition.md](./network-partition.md) 第 3、4 项。

### 3. 认证 / 授权问题 / Auth / authz problems

证书过期、token 换了、ACL 改了。

```
ERROR rr_config: etcd auth failed: permission denied
```

**Fix**: rotate credentials；确保 ConfigProvider client config 的 key/token 与 server 期望一致。

### 4. ConfigProvider 数据损坏 / ConfigProvider data corrupted

路由表存储里的数据 decode 失败。

```
ERROR rr_config: decode RoutingTable failed: bincode error ...
```

**Fix**: 从备份恢复 ConfigProvider 数据。如果没备份，可手动重建 routing table（需要 cluster topology 记录）。

### 5. 自定义 ConfigProvider 实现 bug / Custom impl bug

watch stream 提前关闭、没处理 reconnect 等。

**Fix**: 修你的 impl。标准合约：
- `watch_changes` 返回的 stream 必须在 connection drop 时自动重连
- `get_routing_table` 在故障期间返回 `RrError::Config(ConfigError::ProviderUnavailable(...))`，rr 会用缓存
- 不要 panic；只返回 typed errors

## Recovery / 恢复

ConfigProvider 恢复后：

1. 等 `rr-admin node status` 显示所有节点再次可读 routing table（rr 日志无 provider error）
2. 验证 `rr-router` 的缓存和 source of truth 一致：
   ```bash
   rr-admin --addr rr-0:52001 node status --output json > /tmp/rr.json
   # 和你的 ConfigProvider 的 dump 比较
   ```
3. 如果期间有 admin 操作积压（被阻塞），此时可以**按原顺序**重试。

## Prevention / 预防

- **ConfigProvider 高可用部署** — 比如 etcd 3+ 节点跨 AZ
- **短 `get_routing_table` 超时** — 失败快，落回 cache 快；默认 30s 太长，建议 3-5s
- **监控 ConfigProvider 独立性** — 用**不同**监控系统（Prometheus 应独立于 rr 的 ConfigProvider），避免 cascading failure
- **定期演练** — 每季度故意让 ConfigProvider 下线 10 分钟，验证 rr 仍服务
- **备份 routing table** — 每天定时 dump ConfigProvider 数据到对象存储

## 相关 / Related

- [../../architecture/c4/components-router.md](../../architecture/c4/components-router.md) — ShardRouter + ConfigProvider 架构
- [../../architecture/c4/dynamic-config-update.md](../../architecture/c4/dynamic-config-update.md) — 配置变更传播流程
- [../../guide/config-provider.md](../../guide/config-provider.md) — 自定义 ConfigProvider 实现
