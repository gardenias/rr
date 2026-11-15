---
sidebar_position: 1
---

# 部署指南 / Deployment Guide

## 生产构建 / Production Build

在仓库根目录构建服务端、管理工具和带 RocksDB 的 KV 示例。`rr-server` 内置的是 Echo 冒烟服务；真实业务应嵌入 Rr crates，
或参考 `examples/kv-store` 构建自己的二进制。

Build the server, admin CLI, and RocksDB-enabled KV example from the repository root. The stock `rr-server` contains an
Echo smoke service; embed the Rr crates for real application logic, using `examples/kv-store` as the reference.

```bash
cargo build --release -p rr-server -p rr-admin --features rr-server/rocks
cargo build --release -p kv-store --features rocks
```

二进制位于 `target/release/`。`kv-store --data-dir` 只在带 `rocks` 特性的构建中可用。

Binaries are written to `target/release/`. `kv-store --data-dir` requires a build with the `rocks` feature.

## 节点配置 / Node Configuration

生产节点使用 `rocks` 和 `strict`，为数据、Admin、指标分别设置地址。启用 Admin 时必须同时配置 TLS、mTLS 和 Admin token；
以下片段不包含完整 TLS 字段，按 [TLS 指南](./tls.md) 补齐后再启动。

Use `rocks` with `strict` in production and configure data, Admin, and metrics addresses explicitly. An enabled Admin
service also requires TLS, mTLS, and an Admin token. Complete the TLS fields in the following fragment with the
[TLS guide](./tls.md) before starting the node.

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"
data_dir = "/var/lib/rr/data"
admin_port = 52001
admin_bind_host = "0.0.0.0"
metrics_port = 9100
metrics_bind_host = "0.0.0.0"

[storage]
backend = "rocks"
durability = "strict"

[auth]
admin_token_env = "RR_ADMIN_TOKEN"
```

Admin 和未认证的指标端点只向运维网络开放；不要把它们直接暴露到公网。配置优先级为 CLI > 环境变量 > TOML > 默认值。

Expose Admin and the unauthenticated metrics endpoint only to the operations network, never directly to the public
internet. Configuration precedence is CLI > environment > TOML > defaults.

## 启动三节点 KV 集群 / Start a Three-Node KV Cluster

这是 `kv-store` 的手动组建流程，不是通用的在线加节点流程。三个节点必须使用相同的完整花名册，格式为 `id@advertised_addr`
，且必须包含本节点。只在节点 1 上使用一次 `--bootstrap`；它会直接把花名册中的三个节点初始化为 voter。其他节点不要逐个“加
入”，也不要再执行 bootstrap。

This is the `kv-store` manual formation flow, not a generic online node-join flow. All nodes must use the same complete
`id@advertised_addr` roster, including themselves. Use `--bootstrap` once, on node 1 only; it initializes all three
roster entries as voters. Do not bootstrap the other nodes or treat them as incremental joins.

前提：三个节点已配置符合 SAN 的证书，且环境中均有非空的 `RR_ADMIN_TOKEN` 和 `RR_CLIENT_TOKEN`。`kv-store` 的 Admin 端口
始终是 `--port + 1000`。

Prerequisites: every node has a certificate with the correct SANs, and non-empty `RR_ADMIN_TOKEN` and `RR_CLIENT_TOKEN`
are present in its environment. The `kv-store` Admin port is always `--port + 1000`.

```bash
# 节点 1 / node 1
./target/release/kv-store --node-id 1 --listen-addr 0.0.0.0 --port 51001 \
  --data-dir /var/lib/rr/data --bootstrap \
  --peers 1@192.168.1.1:51001,2@192.168.1.2:51001,3@192.168.1.3:51001 \
  --tls-cert /etc/rr/tls/node1.pem --tls-key /etc/rr/tls/node1.key --tls-ca /etc/rr/tls/ca.pem

# 节点 2 / node 2
./target/release/kv-store --node-id 2 --listen-addr 0.0.0.0 --port 51001 \
  --data-dir /var/lib/rr/data \
  --peers 1@192.168.1.1:51001,2@192.168.1.2:51001,3@192.168.1.3:51001 \
  --tls-cert /etc/rr/tls/node2.pem --tls-key /etc/rr/tls/node2.key --tls-ca /etc/rr/tls/ca.pem

# 节点 3 / node 3
./target/release/kv-store --node-id 3 --listen-addr 0.0.0.0 --port 51001 \
  --data-dir /var/lib/rr/data \
  --peers 1@192.168.1.1:51001,2@192.168.1.2:51001,3@192.168.1.3:51001 \
  --tls-cert /etc/rr/tls/node3.pem --tls-key /etc/rr/tls/node3.key --tls-ca /etc/rr/tls/ca.pem
```

`--listen-addr` 只决定绑定地址，其他节点拨入的地址来自花名册。不要复用 `rr-server --peers` 的旧语义：stock server 的该参
数仅用于推导本地初始路由表，并按列表顺序猜测节点 ID；多节点应使用显式托管花名册或外部 ConfigProvider。

`--listen-addr` controls binding only; peers dial the advertised roster addresses. Do not carry this syntax over to
`rr-server --peers`: the stock server uses that option only to size its local initial routing table and guesses IDs by
list order. Use an explicit managed roster or an external ConfigProvider for real multi-node topologies.

托管 Genesis 改用相同的 `cluster.name`、`cluster.nodes` 和 `[cluster.formation]`，不设置 `bootstrap`，也不使用旧 `peers`
列表。已有集群必须使用 `adopt`，不能用 Genesis 重建。完整流程见 [Kubernetes 部署](./kubernetes.md) 和
[升级指南](./upgrading.md)。

Managed Genesis instead uses identical `cluster.name`, `cluster.nodes`, and `[cluster.formation]` settings, with neither
`bootstrap` nor the legacy `peers` list. Existing clusters must use `adopt`, never a fresh Genesis. See
[Kubernetes Deployment](./kubernetes.md) and [Upgrading](./upgrading.md).

验证 leader、日志推进和成员关系；安全模式下 `rr-admin` 必须显式携带 TLS 和 token 环境变量名。

Verify the leader, log progress, and membership. In secure mode, pass TLS paths and the token environment-variable name
to `rr-admin` explicitly.

```bash
rr-admin --addr 192.168.1.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.pem --tls-cert /etc/rr/tls/client.pem --tls-key /etc/rr/tls/client.key \
  group status --group 1
rr-admin --addr 192.168.1.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.pem --tls-cert /etc/rr/tls/client.pem --tls-key /etc/rr/tls/client.key \
  group membership --group 1
```

若节点一直未选出 leader，先检查三个地址和端口是否互通、花名册是否逐字一致、证书 SAN 与 CA 是否匹配，再检查是否误在多个节
点执行了 bootstrap。

If no leader emerges, check address/port reachability, byte-for-byte roster agreement, certificate SANs and CA trust,
then check whether more than one node was bootstrapped.

## 快照与持久化 / Snapshots and Persistence

快照按 `SnapshotPolicy::LogsSinceLast(N)` 自动构建，默认阈值为 50,000 条日志，可用 `--snapshot-logs-since-last` 或
`RR_SNAPSHOT_LOGS_SINCE_LAST` 调整。Rocks 模式会把每组快照原子写入 `{data_dir}/raft-log/{group_id}/sm-snapshot.bin`，重
启后先恢复快照，再重放剩余日志。Mem 模式的日志和快照都只在内存中，进程重启即丢失。

Snapshots use `SnapshotPolicy::LogsSinceLast(N)`, with a default threshold of 50,000 entries; override it with
`--snapshot-logs-since-last` or `RR_SNAPSHOT_LOGS_SINCE_LAST`. In Rocks mode, each group's snapshot is atomically
persisted to `{data_dir}/raft-log/{group_id}/sm-snapshot.bin`; startup restores it before replaying the remaining log.
In Mem mode, both log and snapshot are memory-only and lost on restart.

框架持久化的是 `Service::snapshot()` 产生的字节，不替代业务级备份。`kv-store` 的业务 map 虽在内存中维护，但 Rocks 模式可
通过已持久化快照和日志重建。

The framework persists the bytes returned by `Service::snapshot()`; this is not a substitute for business-level backups.
Although `kv-store` keeps its working map in memory, Rocks mode can rebuild it from the persisted snapshot and log.

## 备份与恢复 / Backup and Restore

整目录备份必须离线执行。先 drain、正常停止节点，再备份；输出不能位于 `data_dir` 内，也不能覆盖已有文件。

Whole-directory backup is offline only. Drain and stop the node gracefully before archiving. The output must be outside
`data_dir` and must not already exist.

```bash
mkdir -p /var/backups/rr
rr-admin --addr 127.0.0.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.pem --tls-cert /etc/rr/tls/client.pem --tls-key /etc/rr/tls/client.key \
  node drain --node-id 1
# 正常停止 rr-server / stop rr-server gracefully, then run:
rr-admin backup --data-dir /var/lib/rr/data --output /var/backups/rr/node1-$(date +%F).tar.gz --node-id 1
rr-admin inspect-backup --input /var/backups/rr/node1-$(date +%F).tar.gz
```

恢复时节点必须停止，目标目录必须为空或不存在。工具先解包到同文件系统的临时目录并校验 SHA-256，再原子改名；不要手工覆盖非
空目录。

For restore, stop the node and use an empty or absent target directory. The tool verifies SHA-256 in a sibling temporary
directory before an atomic rename; never overwrite a non-empty directory manually.

```bash
rr-admin restore --input /var/backups/rr/node1-2026-11-14.tar.gz --data-dir /var/lib/rr/data
```

恢复后启动该节点并检查快照、日志追平和成员关系。不要把单节点旧备份当成整集群时间点恢复，也不要在仍有 quorum 时同时回滚多
个节点；多数派状态丢失需要单独的灾难恢复方案。

After restore, start the node and verify snapshots, log catch-up, and membership. A single node's old archive is not a
cluster-wide point-in-time restore. Do not roll back multiple nodes while a healthy quorum exists; majority state loss
requires a separate disaster-recovery procedure.

运行中可按组导出快照，但它不是 `rr-admin restore` 可读取的 tar 包，恢复需要应用自己的导入流程：

A running node can export one group's snapshot. This binary snapshot is not a tar archive accepted by
`rr-admin restore`; importing it requires an application-specific procedure.

```bash
rr-admin --addr 192.168.1.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.pem --tls-cert /etc/rr/tls/client.pem --tls-key /etc/rr/tls/client.key \
  group online-backup --group 1 --output /var/backups/rr/group-1.snapshot --force-fresh
```

## 添加节点 / Add a Node

以下流程只适用于支持动态成员的 legacy 部署。先让 ConfigProvider 包含新节点地址并启动一个空节点，再在当前 leader 上添加
learner。默认 `auto_promote_learners = true` 会在追平后提升 voter；若显式关闭，则手工指定完整目标 voter 集合。

This flow applies only to legacy deployments that support dynamic membership. Add the new node's address to the
ConfigProvider and start the empty node, then add a learner on the current leader. By default
`auto_promote_learners = true` promotes a caught-up learner; if disabled, set the complete target voter list manually.

```bash
rr-admin --addr 192.168.1.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.pem --tls-cert /etc/rr/tls/client.pem --tls-key /etc/rr/tls/client.key \
  group add-learner --group 1 --node-id 4 --addr 192.168.1.4:51001
rr-admin --addr 192.168.1.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.pem --tls-cert /etc/rr/tls/client.pem --tls-key /etc/rr/tls/client.key \
  group change-membership --group 1 --voters 1,2,3,4
```

验证 `group membership`、路由表和 `/ready`。Managed formation P1 不支持动态扩容或副本替换；不要通过修改 SeedSpec 或
`replicaCount` 模拟加节点。

Verify `group membership`, the routing table, and `/ready`. Managed formation P1 does not support dynamic expansion or
replica replacement; do not emulate node addition by changing SeedSpec or `replicaCount`.

## 移除节点 / Remove a Node

先预检，再从所有组移除节点，最后更新路由配置并停止进程。不要在改 Raft membership 之前先从 ConfigProvider 删除地址，否则
编排工具可能无法拨到目标或 leader。

Run a dry run, remove the node from every group, then update routing configuration and stop the process. Do not delete
its address from the ConfigProvider before the Raft change; the orchestrator may still need to reach the target or
leader.

```bash
rr-admin --addr 192.168.1.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.pem --tls-cert /etc/rr/tls/client.pem --tls-key /etc/rr/tls/client.key \
  node decommission --node-id 3 --dry-run
rr-admin --addr 192.168.1.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.pem --tls-cert /etc/rr/tls/client.pem --tls-key /etc/rr/tls/client.key \
  node decommission --node-id 3
```

命令失败或报告 coverage gap 时不要停节点，应在承载对应组的节点或 leader 上重跑。服务端默认拒绝缩到少于 2 个 voter；
`--force` 仅限灾难恢复。Managed P1 不支持此运行时缩容流程。

If the command fails or reports a coverage gap, do not stop the node; rerun it against a node hosting that group or its
leader. The server rejects a result with fewer than two voters by default; `--force` is reserved for disaster recovery.
Managed P1 does not support this runtime shrink flow.

## 监控与网络 / Monitoring and Network

使用 `RUST_LOG` 控制结构化日志，通过 `rr-admin group status`、`group snapshot-status` 和 Prometheus 指标观察 leader、日
志与快照。指标语义见 [指标参考](../reference/metrics-reference.md)，持久化细节见
[持久化与恢复](../architecture/durability.md)。

Use `RUST_LOG` for structured logs and inspect leader, log, and snapshot state through `rr-admin group status`,
`group snapshot-status`, and Prometheus. See [Metrics Reference](../reference/metrics-reference.md) and
[Durability & Recovery](../architecture/durability.md).

节点间必须能访问配置的 Client/Raft gRPC 地址；Admin 和 metrics 使用独立端口。保持低延迟、稳定拓扑，并监控
`rr_channel_reconnects_total`。当前心跳为 50ms、选举超时为 150–300ms，高延迟链路会导致反复选举。

Peers must reach the configured Client/Raft gRPC addresses; Admin and metrics use separate ports. Keep latency low and
topology stable, and monitor `rr_channel_reconnects_total`. The current heartbeat is 50ms and election timeout is 150–
300ms; high-latency links can cause repeated elections.
