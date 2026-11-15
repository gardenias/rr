---
title: 升级 rr / Upgrading rr
sidebar_label: 升级 / Upgrading
---

# 升级 rr / Upgrading rr

先确认目标版本的迁移说明、磁盘格式和回退边界，再逐节点操作。跨 minor 升级不要只替换二进制；本页重点覆盖生产安全门和旧集
群收养。

Read the target version's migration notes, disk-format changes, and rollback limits before touching a node. A minor
upgrade is not always a binary-only swap; this page focuses on production safety gates and legacy-cluster adoption.

## 生产安全门 / Production Safety Gates

当前 `RrNode::new()` 在绑定 socket 前执行 G1–G7：Raft 必须 TLS + mTLS；bearer token 必须配 TLS；Admin 必须 TLS +
`admin_token`；明文 TCP Raft 必须显式 insecure opt-in 且只能绑定 loopback。只有 G1–G6 可被 `allow_insecure` 绕过，G7 始
终执行。

Current `RrNode::new()` enforces G1–G7 before binding sockets: Raft requires TLS + mTLS; bearer tokens require TLS;
Admin requires TLS + `admin_token`; plaintext TCP Raft requires an explicit insecure opt-in and a loopback bind. Only G1
–G6 can be bypassed by `allow_insecure`; G7 always applies.

开发、CI 和 examples 可通过 `--allow-insecure`、`RR_ALLOW_INSECURE=1` 或 `[node].allow_insecure = true` 临时绕过。生产不
要启用。启用后节点写 `warn!`，并暴露 `rr_security_insecure_mode = 1`。

Development, CI, and examples can temporarily bypass gates with `--allow-insecure`, `RR_ALLOW_INSECURE=1`, or
`[node].allow_insecure = true`. Never enable it in production. Startup emits a `warn!` and exposes
`rr_security_insecure_mode = 1`.

## 从 plaintext v0.1.x 升级到 strict v0.2.x / Upgrading Plaintext v0.1.x to Strict v0.2.x

:::warning 明文与 TLS 不能互通 / Plaintext and TLS Do Not Interoperate
不要假设这是无停机滚动升级。未启用 TLS 的节点无法与只接受 TLS 的节点完成 Raft RPC；`allow_insecure` 只绕过配置校验，不提
供协议兼容或自动回退。第二阶段应安排维护窗口并暂停写流量，或先部署经过验证的协议兼容代理。

Do not assume a zero-downtime rolling upgrade. A plaintext node cannot complete Raft RPCs with a TLS-only peer;
`allow_insecure` bypasses configuration validation but provides neither protocol compatibility nor fallback. Use a
maintenance window with writes paused for phase 2, or deploy a separately validated protocol-compatibility proxy.
:::

### 阶段 1：部署新二进制，保留明文 / Phase 1: Deploy the Binary, Retain Plaintext

逐节点 drain、替换二进制、启动、等待追平。暂时保留 `allow_insecure`，并确认未改变 Raft 监听协议。

Drain, replace, start, and wait for catch-up one node at a time. Keep `allow_insecure` temporarily and do not change the
Raft transport protocol yet.

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"
admin_port = 52001
metrics_port = 9100
allow_insecure = true
```

验证每个节点 `/ready` 恢复、leader 稳定、日志追平，且 `rr_security_insecure_mode == 1`。若 `/ready` 持续 503 或日志出现
`production safety invariants violated`，停止后续节点并修复或回退当前节点。

Verify `/ready`, a stable leader, log catch-up, and `rr_security_insecure_mode == 1` on every node. If `/ready` remains
503 or logs contain `production safety invariants violated`, stop the rollout and repair or roll back the current node.

### 阶段 2：切换 TLS 与认证 / Phase 2: Cut Over TLS and Authentication

准备节点证书、私钥、共享 CA 和通过环境注入的 token。`scripts/gen-dev-certs.sh` 只能生成开发证书，不能用于生产。

Prepare node certificates, keys, a shared CA, and environment-injected tokens. `scripts/gen-dev-certs.sh` creates
development certificates only.

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"
admin_port = 52001
metrics_port = 9100
allow_insecure = true

[tls]
cert_path = "/etc/rr/cert.pem"
key_path = "/etc/rr/key.pem"
ca_path = "/etc/rr/ca.pem"
require_client_cert = true

[auth]
admin_token_env = "RR_ADMIN_TOKEN"
client_token_env = "RR_CLIENT_TOKEN"
```

在维护窗口内暂停写入，按变更方案切换所有节点。每个节点启动后验证证书链和 mTLS；只检查 TCP 连通不够。

Pause writes during the maintenance window and cut over every node according to the change plan. After each start,
verify both the certificate chain and mTLS; TCP connectivity alone is insufficient.

```bash
openssl s_client -connect node1:51001 -servername node1.rr.internal \
  -CAfile /etc/rr/ca.pem -cert /etc/rr/client.pem -key /etc/rr/client.key \
  -showcerts < /dev/null
```

恢复流量前，确认所有节点能相互复制、leader 稳定、完整 voter 集合一致。不要因 `allow_insecure=true` 仍开启就跳过这些检查
。

Before resuming traffic, verify inter-node replication, a stable leader, and the complete voter set. Do not skip these
checks merely because `allow_insecure=true` remains enabled.

### 阶段 3：移除 insecure 开关 / Phase 3: Remove the Insecure Override

所有节点已经使用 TLS + mTLS 后，再逐节点移除配置并滚动重启。

After every node is on TLS + mTLS, remove the override and rolling-restart one node at a time.

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"
```

启动会执行完整校验，缺失配置会 fail-fast。验证 `rr_security_insecure_mode == 0`、`/ready` 和审计日志；不要仅因进程启动就
继续下一节点。

Startup now runs full validation and fails fast on missing configuration. Verify `rr_security_insecure_mode == 0`,
`/ready`, and audit logs before proceeding to the next node.

| 阶段 / Phase | `allow_insecure` | TLS | 认证 / Auth | 说明 / Notes |
|---|---|---|---|---|
| 1 | `true` | 可选 / optional | 可选 / optional | 部署新二进制，保持明文 / Deploy the binary, retain plaintext |
| 2 | `true` | 启用 / enabled | 启用 / enabled | 维护窗口内切换 / Cut over in a maintenance window |
| 3 | 未设置或 `false` / unset or `false` | 启用 / enabled | 启用 / enabled | 完整安全门生效 / Full gates active |

## 收养旧集群到托管生命周期 / Adopt a Legacy Cluster into Managed Formation

Genesis 只用于全新的空节点，不能收养已有 Raft 状态。旧集群必须使用 `adopt`，并区分“记录现有状态”和“后续成员收敛”。Helm
和 raw Kubernetes 都必须显式选择 formation-capable 镜像；rr 0.2.0 会拒绝新配置字段。

Genesis is only for fresh pristine nodes and cannot adopt existing Raft state. Use `adopt` for an existing group,
separating lifecycle adoption from subsequent membership convergence. Both Helm and raw Kubernetes require an explicit
formation-capable image; rr 0.2.0 rejects the new configuration fields.

1. **准备相同花名册。** 所有节点使用相同的 `cluster.name`、完整
   `cluster.nodes = ["id@advertised_addr", ...]`、`mode = "adopt"` 和 etcd endpoints；移除 `bootstrap` 与旧 `peers`。花
   名册是 SeedSpec 和初始路由，不是当前 Raft membership。Helm 必须设置非空、非 0.2.0 的 `image.tag`，raw manifests 必须
   替换 `REPLACE_WITH_FORMATION_CAPABLE_TAG`。<br />
   **Prepare one roster.** Every node uses identical `cluster.name`, complete
   `cluster.nodes = ["id@advertised_addr", ...]`, `mode = "adopt"`, and etcd endpoints; remove `bootstrap` and legacy
   `peers`. The roster is SeedSpec and initial routing, not current Raft membership. Helm requires a non-empty, non-0.2.0
   `image.tag`; raw manifests require replacing `REPLACE_WITH_FORMATION_CAPABLE_TAG`.

2. **先收养已初始化节点。** 只启动本地所有托管 group 均已初始化的旧节点。第一个节点通过 CAS 创建
   `LegacyAdopted + Active` 记录，其余节点加载同一记录。混合 initialized/pristine 本地 group 或全 pristine 的首次收养会失败，
   不会创建 lifecycle 记录。<br />
   **Adopt initialized nodes first.** Start only legacy nodes whose local managed groups are all initialized. The first
   node creates a `LegacyAdopted + Active` record through CAS and the others load it. Mixed initialized/pristine local
   groups or an all-pristine first adoption fails without creating a lifecycle record.

   当前 convergence Job 的 token 参数有已知缺口；在执行第二阶段前必须先处理 [Kubernetes 页的安全警告](./kubernetes.md)。

   The current convergence Job has a known token-flag gap; resolve the
   [security warning on the Kubernetes page](./kubernetes.md) before phase 2.

   Helm 第一阶段不渲染 Job：

   Helm phase 1 renders no Job:

   ```bash
   helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
     --set image.tag=<formation-capable-tag> \
     --set tls.enabled=true --set tls.requireClientCert=true --set tls.existingSecret=rr-tls \
     --set cluster.formation.mode=adopt \
     --set cluster.formation.adoptionInitializedReplicas=1 --wait
   kubectl wait --for=condition=ready pod/rr-0 --timeout=300s
   ```

   Raw 第一阶段只启动 ordinal 0，但 SeedSpec 仍含全部三个节点：

   Raw phase 1 starts only ordinal 0 while SeedSpec still contains all three nodes:

   ```bash
   kubectl apply -f deploy/k8s/statefulset-adopt.yaml
   kubectl wait --for=condition=ready pod/rr-0 --timeout=300s
   ```

3. **再启动空节点。** 只有第一阶段 readiness 成功后才能继续。新节点显示 `awaiting_replication` 且 `/ready` 为 503；Active
   lifecycle 不会调用 initialize，也不会自动添加 learner 或 voter。<br />
   **Then start pristine nodes.** Continue only after phase-1 readiness succeeds. New nodes report `awaiting_replication`
   and `/ready` returns 503. An Active lifecycle neither initializes them nor adds learners or voters automatically.

   ```bash
   helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
     --set image.tag=<formation-capable-tag> \
     --set tls.enabled=true --set tls.requireClientCert=true --set tls.existingSecret=rr-tls \
     --set cluster.formation.mode=adopt \
     --set cluster.formation.adoptionInitializedReplicas=3 --wait
   ```

   Raw 第二阶段先启动空节点，再运行 Job：

   Raw phase 2 starts pristine nodes before running the Job:

   ```bash
   kubectl scale statefulset rr --replicas=3
   kubectl apply -f deploy/k8s/cluster-form-job.yaml
   kubectl wait --for=condition=complete job/rr-cluster-form --timeout=300s
   ```

4. **显式收敛并验证。** 第二阶段 Job 在当前 leader 上逐个执行 `rr-admin group add-learner`，等待追平后设置完整 voter
   集合。验证 ACK 数据、partition → group 路由、完整 voter 和 `/ready`。只有新 Job 完成且检查通过后，才能清理旧
   revision 的 completed Jobs。<br />
   **Converge and verify.** The phase-2 Job runs `rr-admin group add-learner` on the current leader, waits for catch-up,
   and sets the complete voter list. Verify acknowledged data, partition → group routing, full voter membership, and
   `/ready`. Clean old completed Jobs only after the new Job and all checks pass.

5. **保持 P1 边界。** 不要通过修改 `replicaCount` 或 SeedSpec 扩缩容。P1 没有动态成员扩缩、副本替换或 resharding；改变
   花名册会改变 seed identity，而不是更新当前 membership。<br />
   **Preserve the P1 boundary.** Do not scale by changing `replicaCount` or SeedSpec. P1 has no dynamic membership
   scaling, replica replacement, or resharding; changing the roster changes seed identity, not current membership.

lifecycle store 故障时，已初始化节点可依靠本地 Raft 状态继续读写，partition 路由不变，但 readiness 为 degraded（HTTP 503
）；空节点 fail closed。store 恢复后 reconcile 继续，不会把故障窗口视为重新组建集群的授权。未配置 formation 时，legacy
bootstrap、peers、Admin learner 和手工 membership 行为不变；fresh Genesis 不运行 formation Job。

During a lifecycle-store outage, initialized nodes can continue Raft reads/writes from local state with unchanged
partition routing, but readiness is degraded (HTTP 503); pristine nodes fail closed. Reconciliation resumes after
recovery and never treats the outage as authorization to form a new cluster. Without formation configuration, legacy
bootstrap, peers, Admin learner, and manual membership behavior remain unchanged. Fresh Genesis does not run the
formation Job.

## 库用户 / Library Users

直接通过 `rr-server` crate 构建 `RrNode` 时，开发环境可显式调用 `.with_allow_insecure(true)`。生产代码不要保留此开关，应
按 [TLS 配置指南](./tls.md) 和 [审计日志指南](./audit-log.md) 配置。

When constructing `RrNode` through the `rr-server` crate, development code can explicitly call
`.with_allow_insecure(true)`. Do not retain it in production; follow the [TLS guide](./tls.md) and
[audit-log guide](./audit-log.md).

```rust
let config = NodeConfig::new(node_id, addr, storage, provider, factory)
    .with_raft_lookup(lookup)
    .with_client_dispatch(dispatch)
    .with_allow_insecure(true);
let node = Arc::new(RrNode::new(config)?);
```

## 回退 / Rollback

先停止有问题的节点，恢复与旧二进制兼容的配置，再启动并等待 `/ready`。不要假定所有回退都无数据损失：一旦新版本写入旧版本
无法识别的快照或日志格式，直接换回旧二进制可能失败。回退前核对目标版本迁移页，并保留升级前离线备份；若出现快照解码错误，
按 [快照损坏手册](../operations/runbooks/snapshot-corruption.md) 处理，不要删除整个数据目录。

Stop the affected node, restore configuration compatible with the old binary, then start it and wait for `/ready`. Never
assume rollback is lossless: after a newer version writes a snapshot or log format the old binary cannot decode, a
binary swap can fail. Check the target migration page and retain pre-upgrade offline backups. On snapshot decode errors,
follow the [Snapshot Corruption runbook](../operations/runbooks/snapshot-corruption.md) instead of deleting the whole
data directory.

## 变更记录 / Changelog

| Date | Change | Trigger |
|---|---|---|
| 2026-04-26 | Initial draft | Wave 1 — F2.1+F2.2 enforce + `allow_insecure` opt-in shipped |
