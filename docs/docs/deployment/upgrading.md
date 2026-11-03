---
title: Upgrading rr
sidebar_label: Upgrading
---

# 升级 rr / Upgrading rr

本文档描述跨主版本（minor）升级的步骤，重点关注**生产安全门禁**的滚动启用。

This guide covers cross-minor upgrade procedures with a focus on rolling out
production safety gates without downtime.

---

## 概览 / Overview

从 rr 的早期 minor 版本（含 `v0.2.x`）开始，框架在 `RrNode::new()` 默认强制 5 条
**生产安全不变量** / Starting from early minor versions of rr (including
`v0.2.x` and onward), the framework enforces five **production safety
invariants** in `RrNode::new()` by default:

1. `RaftService` 启用 → 必须有 TLS / `RaftService` enabled → TLS required.
2. `RaftService` + TLS → 必须 `tls.require_client_cert = true`（mTLS）/ ...
   must use mTLS.
3. Bearer-token auth (`admin_token` / `client_token`) → 必须 TLS / requires TLS.
4. `AdminService` 启用（`admin_port = Some`）→ 必须 TLS。
5. `AdminService` 启用 → 必须 `admin_token` 已配置（不允许任意 caller transfer
   leadership 或 trigger snapshot）。

**Escape hatch**: `--allow-insecure` (CLI) / `RR_ALLOW_INSECURE=1` (env) /
`[node].allow_insecure = true` (TOML)。仅限 dev / CI / examples，**绝不**用于
生产。启用时节点会发结构化 `tracing::warn!` 并设 Prometheus gauge
`rr_security_insecure_mode = 1` 以便告警。

---

## 从 plaintext v0.1.x 升级到 strict v0.2.x

### 风险分析 / Risk Profile

**唯一可能丢 quorum 的步骤是把所有节点的 plaintext gRPC 翻成 TLS** —— 在切换
窗口里，老 plaintext 节点和新 TLS 节点无法通信。其它步骤都是滚动安全的。

### 三阶段流程 / Three-Phase Procedure

#### Phase 1 — Deploy strict binary, retain plaintext

每个节点的 TOML 加 `allow_insecure = true`：

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"
admin_port  = 52001
metrics_port = 9100
allow_insecure = true       # TEMPORARY — remove in Phase 3.
```

逐节点滚动重启：drain → swap binary → start → wait for caught-up → next。
新 binary 起来后会按上面的 5 条做 validation；`allow_insecure=true` 全部 bypass
并发出明显的 `INSECURE MODE ACTIVE` warning（这是预期的，提醒你 Phase 1 还没结束）。

监控：`rr_security_insecure_mode == 1` 应在所有节点都为 1。

**Stop conditions**：任何节点 `/ready` 返回 503 超过 60 秒，或日志出现
`production safety invariants violated` —— 立即回滚那个节点，diff 配置。

#### Phase 2 — Roll TLS + auth in

为每个节点准备：

- TLS cert + key（用 `scripts/gen-dev-certs.sh` 或你的 PKI 颁发）
- 共享 CA bundle
- `admin_token` 通过 env var 注入（不要写进 TOML）

更新每个节点的 TOML：

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"
admin_port  = 52001
metrics_port = 9100
allow_insecure = true       # STILL on — keep until Phase 3.

[tls]
cert_path = "/etc/rr/cert.pem"
key_path  = "/etc/rr/key.pem"
ca_path   = "/etc/rr/ca.pem"
require_client_cert = true

[auth]
admin_token_env  = "RR_ADMIN_TOKEN"
client_token_env = "RR_CLIENT_TOKEN"
```

注意：因为 `allow_insecure=true` 还在，校验仍然 bypass。但**此时所有节点都
serving TLS**，TLS 链已经建立。验证：

```bash
openssl s_client -connect node1:51001 -CAfile /etc/rr/ca.pem -showcerts < /dev/null
```

应见所有节点的 cert chain。

#### Phase 3 — Remove `allow_insecure`

最后一轮滚动重启，每个 TOML 删掉 `allow_insecure`：

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"
# allow_insecure removed — strict mode active.
```

新启动会跑完整 validation；任何配置缺失会 fail-fast。`rr_security_insecure_mode`
gauge 现在应该是 `0`。Audit log 不再出现 `INSECURE MODE ACTIVE`。

---

## 收养旧集群到托管生命周期 / Adopt a Legacy Cluster into Managed Formation

Genesis 只用于全新的空节点，不能用来收养已有 Raft 状态。旧集群升级必须选择 `adopt`，并严格区分
“记录现有状态”和“后续成员收敛”两个阶段。

Genesis is only for fresh pristine nodes. Use `adopt` for an existing Raft group, and keep lifecycle adoption
separate from subsequent membership convergence. A formation-capable image override is mandatory for both Helm
and raw Kubernetes; rr 0.2.0 rejects the new configuration fields and must not be deployed.

1. **准备镜像与相同花名册 / Prepare the image and one roster**：必须显式选择包含 managed formation 的
   镜像；rr 0.2.0 的 strict parser 会拒绝新字段，不能使用。Helm 必须设置非空、非 0.2.0 的
   `image.tag`，raw manifests 必须替换 `REPLACE_WITH_FORMATION_CAPABLE_TAG`。给所有节点配置相同的
   `cluster.name`、完整 `cluster.nodes = ["id@advertised_addr", ...]`、`mode = "adopt"` 和 etcd
   endpoints；移除 `bootstrap` 与 `peers`。花名册是 SeedSpec 和初始路由，不是当前 Raft membership。
2. **先收养已初始化节点 / Adopt initialized nodes first**：只启动本地所有托管 group 都已初始化的旧节点。
   第一个节点通过 CAS 创建 `LegacyAdopted + Active` 记录，其余节点加载同一记录。确认原 voter 集合、
   ACK 数据和分区路由均未改变。混合 initialized/pristine 本地 group 或全 pristine 的首次收养会失败，
   不会创建生命周期记录。以下命令以 `rr-0` 为唯一 initialized voter；不得依赖 `Parallel` 决定启动顺序。

   Helm phase 1（此阶段不渲染 Job）：

   ```bash
   helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
     --set image.tag=<formation-capable-tag> \
     --set cluster.formation.mode=adopt \
     --set cluster.formation.adoptionInitializedReplicas=1 --wait
   kubectl wait --for=condition=ready pod/rr-0 --timeout=300s
   ```

   Raw phase 1（专用 manifest 只有 ordinal 0，但 SeedSpec 仍是全部三个节点）：

   ```bash
   kubectl apply -f deploy/k8s/statefulset-adopt.yaml
   kubectl wait --for=condition=ready pod/rr-0 --timeout=300s
   ```

3. **再启动空节点 / Then start pristine nodes**：只有阶段 1 readiness 成功后才能执行阶段 2。新节点显示
   `awaiting_replication` 且 `/ready` 为 503；Active 生命周期下不会调用 initialize，也不会自动添加
   learner 或 voter。

   Helm phase 2（渲染 Job 并由 `--wait` 等待）：

   ```bash
   helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
     --set image.tag=<formation-capable-tag> \
     --set cluster.formation.mode=adopt \
     --set cluster.formation.adoptionInitializedReplicas=3 --wait
   ```

   Raw phase 2（先启动空节点，再运行 Job）：

   ```bash
   kubectl scale statefulset rr --replicas=3
   kubectl apply -f deploy/k8s/cluster-form-job.yaml
   kubectl wait --for=condition=complete job/rr-cluster-form --timeout=300s
   ```

4. **显式收敛成员 / Converge membership explicitly**：阶段 2 的既有 cluster-form Job 在当前 leader 上逐个
   `rr-admin group add-learner`，等待追平后执行一次完整 voter 集合的 `change-membership`。Helm `adopt`
   Job 是普通 resource 而不是 hook；阶段 1 不渲染它，阶段 2 才渲染。Job 名包含 `.Release.Revision`，
   升级会创建新 Job，不会修改已完成 Job 的 immutable pod template。验证 ACK 数据、partition → group
   路由、全量 voter 和 `/ready`。只有新 Job 完成且这些检查通过后，才能手工清理旧 revision 的
   completed Jobs；不要删除当前或仍在运行的 Job。
5. **保持 P1 边界 / Preserve the P1 boundary**：不要通过修改 `replicaCount` 或 SeedSpec 来扩容。
   P1 没有动态成员扩缩、副本替换或 resharding；改变花名册会改变 seed identity，而不是更新当前 membership。

lifecycle store 故障时，已初始化节点可依靠本地 Raft 状态继续读写，partition 路由不变，但 readiness
变为 degraded（HTTP 503）；pristine 节点 fail closed，不会自行初始化。store 恢复后 reconcile 会继续，
不会把故障窗口当成重新组建集群的授权。

Without formation configuration, the legacy bootstrap, peers, Admin learner, and manual membership behavior is
unchanged. Fresh Genesis does not render/run the ordinary formation job; that job remains available for legacy
adoption and manual recovery only.

---

## Quick Reference

| 阶段 / Phase | `allow_insecure` | TLS | Auth | Notes |
|---|---|---|---|---|
| 1 | `true` | optional | optional | Deploy strict binary, no TLS / auth needed yet |
| 2 | `true` | enabled | enabled | TLS + auth wired, gates still bypassed |
| 3 | unset/`false` | enabled | enabled | Strict mode active, gauge = 0 |

---

## Library users

如果你直接通过 `rr-server` crate 库（例如 `examples/counter`）构建 `RrNode`，
需要在 `NodeConfig` 上调用 `.with_allow_insecure(true)` 才能在 dev 模式跑：

```rust
let config = NodeConfig::new(node_id, addr, storage, provider, factory)
    .with_raft_lookup(lookup)
    .with_client_dispatch(dispatch)
    .with_allow_insecure(true);   // DEV / EXAMPLES ONLY
let node = Arc::new(RrNode::new(config)?);
```

`examples/counter` 和 `examples/kv-store` 已经这样做。生产代码请用 [TLS 配置
指南](./tls.md) + [审计日志指南](./audit-log.md) 把 `allow_insecure` 拿掉。

---

## Rollback

任何阶段失败，回滚步骤：

1. 把出问题节点的 binary 回退到上一版。
2. 配置回到上一版（git checkout 即可）。
3. 重启该节点。
4. 等 `/ready` 200。

因为 v0.1 → v0.2 的 wire / disk 兼容（见 F4 review），rollback 不丢数据。

---

## Changelog

| Date | Change | Trigger |
|---|---|---|
| 2026-04-26 | Initial draft | Wave 1 — F2.1+F2.2 enforce + `allow_insecure` opt-in shipped |
