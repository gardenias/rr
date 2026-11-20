---
sidebar_position: 3
---

# Kubernetes 部署 / Kubernetes Deployment

原始 manifests（`deploy/k8s/`）用于快速试用，Helm chart（`deploy/helm/rr/`）用于需要参数化管理的部署。两者都使用 managed
formation，不是旧 bootstrap/peers 流程。

Use raw manifests (`deploy/k8s/`) for quick trials and the Helm chart (`deploy/helm/rr/`) for parameterized deployments.
Both use managed formation, not the legacy bootstrap/peers flow.

## 前置条件 / Prerequisites

- Kubernetes 1.27+，以及支持 `ReadWriteOnce` 的 StorageClass。<br />
  Kubernetes 1.27+ and a StorageClass supporting `ReadWriteOnce`.
- 每个 Pod 可访问的 etcd v3。Raw manifests 默认使用 `http://etcd:2379`；Helm 默认使用 `http://rr-etcd:2379`。<br />
  An etcd v3 endpoint reachable from every Pod. Raw manifests default to `http://etcd:2379`; Helm defaults to
  `http://rr-etcd:2379`.
- 默认镜像为支持 managed formation 的 Rr 1.0.0。Rr 0.2.0 的严格解析器拒绝 `[cluster.formation]` 和新 cluster
  字段，不能使用；覆盖镜像 tag 时必须选择其他兼容版本。<br />
  The default image is Rr 1.0.0 with managed formation support. Rr 0.2.0 rejects `[cluster.formation]` and the new
  cluster fields; when overriding the image tag, select another compatible release.
- 默认安全配置需要 `rr-tls` 和 `rr-admin-token` 两个 Secret。证书 SAN 必须覆盖实际拨入的 Pod/Service DNS 名。<br />
  The default secure configuration requires `rr-tls` and `rr-admin-token` Secrets. Certificate SANs must cover the
  Pod/Service DNS names clients actually dial.

```bash
kubectl create secret generic rr-tls \
  --from-file=tls.crt=node.pem --from-file=tls.key=node.key --from-file=ca.crt=ca.pem
kubectl create secret generic rr-admin-token \
  --from-literal=token="$(openssl rand -hex 32)"
```

## 为什么不按 Pod 编排 `--bootstrap` / Why Not Bootstrap Individual Pods

`--bootstrap` 是手动/legacy 流程中的一次性初始化动作，不是 StatefulSet 的稳态配置。Kubernetes 部署统一使用 managed
formation：所有 Pod 获得相同的 `cluster.name`、完整 `cluster.nodes` 和 `[cluster.formation]`，只有节点 ID 随 ordinal 改变；
不要按 ordinal 添加 `--bootstrap`，也不要生成逐节点递增的 `peers` 列表。

`--bootstrap` is a one-time initialization action in the manual/legacy flow, not steady-state StatefulSet configuration.
Kubernetes deployments use managed formation instead: every Pod receives the same `cluster.name`, complete `cluster.nodes`,
and `[cluster.formation]`; only the node ID varies by ordinal. Do not add `--bootstrap` for a selected ordinal or generate
prefix-based `peers` lists.

全新集群使用 `genesis` 和 `Parallel` 启动。etcd 中的 lifecycle CAS 授权一份共享初始花名册，避免并发 Pod 各自建立不同集群；
`Active` 记录下已经初始化的节点走本地恢复。正常重启和镜像滚动升级保持相同花名册及 `genesis` 配置，不切换模式，不重新
bootstrap，也不运行 formation Job。旧 bootstrap/peers 集群接入 managed formation 时必须使用 `adopt`，不能重新 Genesis。

A fresh cluster uses `genesis` with `Parallel` startup. The lifecycle CAS in etcd authorizes one shared initial roster instead
of allowing concurrent Pods to form divergent clusters. Under an `Active` record, initialized nodes recover from local state.
Keep the same roster and `genesis` configuration for normal restarts and rolling image upgrades: do not switch modes, bootstrap
again, or run the formation Job. A legacy bootstrap/peers cluster must use `adopt` when moving to managed formation; never run
a fresh Genesis for it.

## 选项 A：原始 manifests / Option A: Raw Manifests

Manifests 默认固定 `ghcr.io/gardenias/rr:1.0.0`。Genesis 使用 `statefulset.yaml`；收养还使用
`statefulset-adopt.yaml` 和第二阶段的 `cluster-form-job.yaml`。

Manifests pin `ghcr.io/gardenias/rr:1.0.0` by default. Genesis uses `statefulset.yaml`; adoption also uses
`statefulset-adopt.yaml` and, in phase 2, `cluster-form-job.yaml`.

```bash
kubectl apply -f deploy/k8s/statefulset.yaml
```

该文件创建 3 副本、`Parallel` 启动的 `StatefulSet rr`，headless `Service rr`、客户端 `Service rr-client`、统一
`ConfigMap rr-config`、`minAvailable=2` 的 PDB，以及每 Pod 20Gi PVC。只有节点 ID 随 ordinal 改变，不生成 bootstrap 标志
或前缀 peers。

It creates a three-replica `StatefulSet rr` with `Parallel` startup, headless `Service rr`, client `Service rr-client`,
a homogeneous `ConfigMap rr-config`, a PDB with `minAvailable=2`, and a 20Gi PVC per Pod. Only the node ID varies by
ordinal; no bootstrap flag or prefix peers list is generated.

全新 Genesis 不要 apply `cluster-form-job.yaml`；lifecycle CAS 已授权完整花名册初始化。该 Job 只用于旧集群收养或人工恢复
。旧集群必须按 [有序收养流程](./upgrading.md) 先启动已初始化节点，再启动空节点并显式收敛成员。

For fresh Genesis, do not apply `cluster-form-job.yaml`; the lifecycle CAS already authorizes full-roster
initialization. The Job is only for legacy adoption or manual recovery. Existing clusters must follow the
[ordered adoption procedure](./upgrading.md): initialized nodes first, pristine nodes second, then explicit membership
convergence.

## 选项 B：Helm chart / Option B: Helm Chart

先创建上述 Secret，再安装。默认 `tls.enabled=false` 与 `security.allowInsecure=false` 的组合会被 chart 拒绝，因此安全安
装必须显式启用 TLS 和 mTLS。

Create the Secrets first, then install. The chart rejects the default combination of `tls.enabled=false` and
`security.allowInsecure=false`, so a secure installation must enable TLS and mTLS explicitly.

```bash
helm install rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=1.0.0 \
  --set tls.enabled=true --set tls.requireClientCert=true \
  --set tls.existingSecret=rr-tls --wait
```

升级时固定同一花名册和 formation values，不要只更新镜像后假设扩缩容会自动发生。

For upgrades, keep the same roster and formation values; changing the image alone never implies automatic scaling.

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=1.0.0 \
  --set tls.enabled=true --set tls.requireClientCert=true \
  --set tls.existingSecret=rr-tls --wait
```

### 两阶段旧集群收养 / Two-Phase Legacy Adoption

`Parallel` 只并发启动 Pod，不保证 initialized adopters 先于 pristine ordinals。以下示例以 `rr-0` 为唯一 initialized
legacy voter，`rr-1`/`rr-2` 为空节点；若有多个已初始化节点，第一阶段使用它们连续 ordinal 的数量。两阶段必须使用相同
chart values 和完整三节点 SeedSpec。

`Parallel` starts Pods concurrently but does not order initialized adopters before pristine ordinals. This example
starts one initialized legacy voter (`rr-0`) first, while `rr-1` and `rr-2` remain absent. If more initialized nodes
exist, phase 1 uses their contiguous ordinal count. Both phases must use identical chart values and the same complete
three-node SeedSpec.

:::info convergence Job 认证 / Convergence Job Authentication
raw/Helm Job 模板注入 `RR_ADMIN_TOKEN`，并在该变量非空时为每个 `rr-admin` 调用传入
`--token-env RR_ADMIN_TOKEN`。无 token 的显式 insecure 本地流程仍可不设置该变量；Genesis 不运行此 Job。

The raw/Helm Job templates inject `RR_ADMIN_TOKEN` and pass `--token-env RR_ADMIN_TOKEN` to every `rr-admin` call when
it is non-empty. Explicit insecure local flows without a token may leave it unset. Genesis does not run this Job.
:::

Helm 第一阶段只启动 initialized adopter，不渲染 convergence Job：

Helm phase 1 starts only initialized adopters and renders no convergence Job:

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=1.0.0 \
  --set tls.enabled=true --set tls.requireClientCert=true --set tls.existingSecret=rr-tls \
  --set cluster.formation.mode=adopt \
  --set cluster.formation.adoptionInitializedReplicas=1 --wait
kubectl wait --for=condition=ready pod/rr-0 --timeout=300s
```

确认上一阶段成功后，Helm 第二阶段启动完整花名册和 convergence Job：

After phase 1 succeeds, Helm phase 2 starts the full roster and the convergence Job:

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=1.0.0 \
  --set tls.enabled=true --set tls.requireClientCert=true --set tls.existingSecret=rr-tls \
  --set cluster.formation.mode=adopt \
  --set cluster.formation.adoptionInitializedReplicas=3 --wait
kubectl logs job/rr-cluster-form-<phase-2-revision> -f
```

Raw 第一阶段使用专用 manifest，固定 `replicas: 1` 和 `mode = "adopt"`，但保留完整三节点 SeedSpec：

Raw phase 1 uses a dedicated manifest with `replicas: 1` and `mode = "adopt"`, while retaining the complete three-node
SeedSpec:

```bash
kubectl apply -f deploy/k8s/statefulset-adopt.yaml
kubectl wait --for=condition=ready pod/rr-0 --timeout=300s
```

仅在 `rr-0` ready 后执行 Raw 第二阶段：

Run raw phase 2 only after `rr-0` is ready:

```bash
kubectl scale statefulset rr --replicas=3
kubectl apply -f deploy/k8s/cluster-form-job.yaml
kubectl wait --for=condition=complete job/rr-cluster-form --timeout=300s
kubectl logs job/rr-cluster-form -f
```

`image.tag` 默认为 `1.0.0`；为空或为 `0.2.0` 时 chart 拒绝渲染。`adopt` Job 是普通 Helm resource，只在第二阶段
渲染，因此第二阶段 `--wait` 会等待它完成；Genesis 和收养第一阶段不渲染 Job。Job 名包含 `.Release.Revision`，升级会创建
新 Job，而不是修改已完成 Job 的不可变 Pod template。

The image tag defaults to `1.0.0`; the chart rejects an empty or `0.2.0` tag. The adopt Job is an ordinary Helm
resource rendered only in phase 2, so phase-2 `--wait` includes its completion. Genesis and adoption phase 1 render no
Job. Job names include `.Release.Revision`, so upgrades create new Jobs rather than mutating completed immutable Pod
templates.

只有新 revision 的 convergence Job 完成、完整 voter 集合和 `/ready` 均验证通过后，才能手工删除旧 completed Jobs；不要删
除当前或仍在运行的 Job。

Delete old completed Jobs manually only after the replacement Job completes and the new revision's full voter membership
and `/ready` checks pass. Never delete the current or a running Job.

### 关键 values / Important Values

| Key | Default | 说明 / Description |
| :--- | :--- | :--- |
| `image.repository` | `ghcr.io/gardenias/rr` | 镜像仓库；fork 后改成自己的命名空间 / Image repository; override for a fork |
| `image.tag` | `"1.0.0"` | formation-capable tag；拒绝空值和 `0.2.0` / Formation-capable tag; empty and `0.2.0` rejected |
| `replicaCount` | `3` | 固定初始花名册大小，不是运行时扩容开关 / Fixed initial roster size, not a runtime scaling knob |
| `cluster.name` | `"rr"` | 共享 SeedSpec 集群名 / Shared SeedSpec cluster name |
| `cluster.formation.mode` | `"genesis"` | 空节点用 `genesis`，旧节点用 `adopt` / `genesis` for pristine nodes; `adopt` for legacy nodes |
| `cluster.formation.adoptionInitializedReplicas` | `null` | 第一阶段已初始化 ordinal 数；第二阶段设为 `replicaCount` / Initialized ordinal count in phase 1; `replicaCount` in phase 2 |
| `cluster.formation.etcdEndpoints` | `["http://rr-etcd:2379"]` | 线性一致 lifecycle store endpoints / Linearizable lifecycle-store endpoints |
| `storage.backend` | `"rocks"` | `"mem"` 或 `"rocks"` / `"mem"` or `"rocks"` |
| `storage.durability` | `"strict"` | `"strict"` / `"replicated_only"` / `"test_only"` |
| `storage.dataSize` | `"20Gi"` | 每 Pod PVC 大小 / Per-Pod PVC size |
| `storage.storageClassName` | `""`（集群默认 / cluster default） | 按需指定存储类 / Set a specific class when required |
| `auth.adminTokenSecret.name` | `rr-admin-token` | 保存 Admin bearer token 的 Secret / Secret holding the Admin bearer token |
| `tls.enabled` | `false` | 启用 gRPC TLS / Enable TLS on gRPC |
| `tls.existingSecret` | `""` | 含 `tls.crt`/`tls.key`/`ca.crt` 的 Secret / Secret containing `tls.crt`/`tls.key`/`ca.crt` |
| `tls.requireClientCert` | `false` | 启用 mTLS / Enable mTLS |
| `security.allowInsecure` | `false` | 仅开发；与 `tls.enabled=false` 同用时 chart 拒绝渲染 / Dev only; chart rejects it with `tls.enabled=false` |
| `resources.*` | 500m / 512Mi request | 按负载调整 / Adjust for the workload |

### 使用 cert-manager 配置 TLS / TLS Setup with cert-manager

按实际 namespace 和完整 Pod DNS 调整名称。证书中的 `commonName` 不能替代 SAN 匹配。

Adjust names for the actual namespace and complete Pod DNS names. `commonName` does not replace SAN matching.

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: rr-tls
spec:
  secretName: rr-tls
  commonName: rr.default.svc.cluster.local
  dnsNames:
    - rr.default.svc.cluster.local
    - rr-0.rr.default.svc.cluster.local
    - rr-1.rr.default.svc.cluster.local
    - rr-2.rr.default.svc.cluster.local
  issuerRef:
    name: my-ca-issuer
    kind: ClusterIssuer
```

## 操作 / Operations

### Cluster Admin Console（自管） / Cluster Admin Console (Self-Managed)

当前 raw manifests 和 Helm chart 不会自动部署 Console：其中没有 Console `Deployment`、`Service` 或对应 Helm
values。需要浏览器 UI 时，应自行将 `rr-admin console` 作为独立工作负载和 Service 部署，不要把它加入 Raft
`StatefulSet` 或当作 formation/控制面组件。Console 故障不影响 Raft 节点，受控写操作仍由 Admin CLI 执行。

The current raw manifests and Helm chart do not deploy the Console automatically: they contain no Console `Deployment`,
`Service`, or corresponding Helm values. To use the browser UI, operate `rr-admin console` as a separate workload and
Service yourself. Do not add it to the Raft `StatefulSet` or treat it as a formation/control-plane component. A Console
outage does not affect Raft nodes, and controlled write operations remain the responsibility of the Admin CLI.

自管部署至少需要满足以下要求：

A self-managed deployment must account for the following:

- 同时携带 `rr-admin` 二进制和前端静态导出 `console/out`，并用 `--static-dir` 指向实际部署目录；静态文件由
  Console BFF 同源托管，不需要独立 Node.js 生产服务。<br />Ship both the `rr-admin` binary and the `console/out`
  static export, and point `--static-dir` at the deployed directory. The Console BFF serves these files from the same
  origin; no standalone Node.js production service is required.
- 一个 Console 可通过 `/connections/` 管理多个目标集群，但最多 8 个显式连接的 runtime 同时采集。将
  `--state-dir` 指向独立持久卷，挂载为 Console 用户独占读写并备份其中的 `connections.json`；profile 会恢复，
  runtime、session、缓存和趋势在重启后不会自动恢复。<br />One Console manages multiple target clusters through
  `/connections/`, with at most eight explicitly connected runtimes collecting at once. Point `--state-dir` at a
  dedicated persistent volume writable only by the Console user and back up its `connections.json`. Profiles survive,
  while runtimes, sessions, caches, and trends do not resume automatically after restart.
- 容器通常没有桌面 OS keychain。可显式设置 `--keychain-disable`，使进程完全不访问 keychain、UI 禁用 Remember，
  所有连接秘密只留内存并在重启后重新输入；生产默认仍是不传该参数的 keychain-enabled 行为。<br />Containers usually
  have no desktop OS keychain. Explicit `--keychain-disable` prevents all keychain access, disables Remember in the UI,
  and keeps connection secrets memory-only so they must be re-entered after restart. The production default remains
  keychain-enabled when the flag is omitted.
- 打通 Console Pod 到各目标群节点 Admin gRPC（仓库 manifests 使用 52001，生产使用 TLS/mTLS 和 Admin token）以及
  observability HTTP（仓库 manifests 使用 9100，抓取 `/health`、`/ready` 和 `/metrics`）的网络路径，并用
  NetworkPolicy 限制访问来源。完整健康与实时指标还要求每节点在 `[node]` 中配置完整的
  `advertise_addr`、`admin_advertise_addr`、`observability_advertise_addr` 和 `environment`；当前 raw/Helm
  配置未提供这些描述符字段，需要自管部署自行补充，否则相关视图按能力降级。字段语义见
  [NodeDirectory 自注册](../guide/config-provider.md#节点自注册--node-self-registration)。Prometheus 仅用于可选
  历史查询，不是 Console 的必需依赖。<br />Allow network paths from the Console Pod to each node's Admin gRPC endpoint
  (port 52001 in the repository manifests, with TLS/mTLS and the Admin token in production) and observability HTTP
  endpoint (port 9100 in the repository manifests, for `/health`, `/ready`, and `/metrics`), restricting clients with
  NetworkPolicy. Full health and live metrics also require every node to configure `advertise_addr`,
  `admin_advertise_addr`, `observability_advertise_addr`, and `environment` in `[node]`. The current raw/Helm
  configuration omits these descriptor fields; a self-managed deployment must supply them or accept capability
  degradation. See [NodeDirectory self-registration](../guide/config-provider.md#节点自注册--node-self-registration).
  Prometheus is optional for historical queries and is not required by the Console.
- 使用专用 Secret 提供 Console HTTPS 服务证书和私钥和 viewer token（通过 `RR_CONSOLE_TOKEN` 注入），并与节点
  `rr-tls`、`rr-admin-token` 分开管理；BFF→Admin 的客户端 TLS 凭证按手册单独配置。非 loopback 监听必须同时
  启用 Console HTTPS 和 viewer token。<br />Use dedicated Secrets for the Console HTTPS serving certificate/private key
  and viewer token (injected through `RR_CONSOLE_TOKEN`), managed separately from the node `rr-tls` and
  `rr-admin-token` Secrets. Configure BFF-to-Admin client TLS credentials separately as described in the manual.
  Non-loopback listeners require both Console HTTPS and a viewer token.
- 只通过受控 Service/Ingress 暴露 Console HTTPS；不要为了 Console 把节点 Admin 或 metrics 端点暴露到公网。<br />
  Expose only Console HTTPS through a controlled Service/Ingress; never expose node Admin or metrics endpoints publicly
  to support the Console.

完整启动参数、静态构建顺序、发现模式和安全边界见
[Cluster Admin Console 手册](../operations/cluster-admin-console.md)。
See the [Cluster Admin Console manual](../operations/cluster-admin-console.md) for complete startup flags, the static
build sequence, discovery modes, and security boundaries.

### 查看集群状态 / View Cluster Status

默认安全配置下，Pod 内的 `rr-admin` 也要显式加载 TLS 文件和 Admin token：

With the default secure configuration, in-Pod `rr-admin` also needs explicit TLS paths and the Admin token:

```bash
kubectl exec -it rr-0 -- /usr/local/bin/rr-admin \
  --addr 127.0.0.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.crt --tls-cert /etc/rr/tls/tls.crt --tls-key /etc/rr/tls/tls.key \
  node status
```

### 在线快照备份 / Online Snapshot Backup

对运行中的节点使用 `group online-backup`。这里生成的是单组快照字节，不是 `rr-admin restore` 接受的整目录 tar 包；不要在
运行中的 Pod 上执行离线 `rr-admin backup`。

Use `group online-backup` against a running node. It writes one group's snapshot bytes, not the whole-directory tar
archive accepted by `rr-admin restore`. Never run the offline `rr-admin backup` against a live Pod.

```bash
kubectl exec rr-2 -- /usr/local/bin/rr-admin \
  --addr 127.0.0.1:52001 --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/tls/ca.crt --tls-cert /etc/rr/tls/tls.crt --tls-key /etc/rr/tls/tls.key \
  group online-backup --group 1 --output /tmp/group-1.snapshot --force-fresh
kubectl cp rr-2:/tmp/group-1.snapshot ./group-1-$(date +%F).snapshot
```

整目录离线备份必须先 drain 并停止该节点，再通过挂载同一 PVC 的离线任务或存储卷快照执行；通用命令见
[部署指南](./deployment.md)。

A whole-directory offline backup requires draining and stopping the node first, then using an offline workload mounted
with the same PVC or a storage-volume snapshot. See the [Deployment Guide](./deployment.md) for the generic command
sequence.

### P1 扩容与替换边界 / P1 Scaling and Replacement Boundary

不要通过 `helm upgrade --set replicaCount=5` 或 `kubectl scale` 扩容。完整花名册属于 SeedSpec identity，改变它不会扩大
Raft membership，反而会让现有 lifecycle 记录拒绝不匹配的节点。P1 不支持动态成员扩缩容、副本替换或 resharding；
adopt-only formation Job 只负责初始旧集群收敛，不能复用为日常扩容 Job。

Do not scale an existing managed cluster with `helm upgrade --set replicaCount=5` or `kubectl scale`. The complete roster
belongs to the SeedSpec identity; changing it does not expand Raft membership and makes the existing lifecycle record reject
mismatched nodes. P1 supports neither dynamic membership scaling, replica replacement, nor resharding. The adopt-only formation
Job performs initial legacy convergence and is not a routine scaling Job.

当前应按以下边界规划容量 / Plan capacity within these boundaries:

- 全新集群可在首次 Genesis 前把 `replicaCount` 和完整花名册规划为目标规模；这不能用于已经初始化的集群。<br />
  For a fresh cluster, set `replicaCount` and the complete roster to the target size before the first Genesis; this is
  not valid for an initialized cluster.
- 已运行集群可以垂直增加 CPU、内存等资源；磁盘扩容另按存储运维流程处理。<br />
  A running cluster can scale vertically with more CPU, memory, and similar resources; handle disk growth through the
  storage operations procedure.
- 必须水平扩展时，新建目标规模的集群并执行应用层数据迁移与流量切换；当前没有内置的整集群在线迁移能力。<br />
  If horizontal expansion is mandatory, create a cluster at the target size and perform application-level data migration
  and traffic cutover; built-in online whole-cluster migration is not currently available.
- `add-learner` + `change-membership` 的手工加节点流程只适用于支持动态成员的 legacy 部署，见
  [一般部署指南](./deployment.md)；不要将它套用到 Managed P1。<br />
  The manual `add-learner` + `change-membership` flow applies only to legacy deployments with dynamic membership support;
  see the [generic deployment guide](./deployment.md). Do not apply it to Managed P1.

增加同一 Raft 组的 voter 主要改变容错和多数派延迟，不会提升该组的写入吞吐。需要水平写扩展时，应使用跨机且副本集合
不相交的多个 Raft 组；Managed P1 仍不支持在线加组或已有 partition 的 resharding。详见
[容量规划](./capacity-planning.md)。

Adding voters to one Raft group changes fault tolerance and quorum latency, not its write throughput. Horizontal write scaling
requires multiple Raft groups on disjoint cross-host replica sets. Managed P1 still supports neither online group addition nor
resharding of existing partitions. See [Capacity Planning](./capacity-planning.md).

### Readiness 与 lifecycle store 故障 / Readiness and Lifecycle-Store Outage

`/ready` 要求所有托管 group formed 且观察到 leader。空节点处于 `awaiting_replication` 时返回 503。store outage 期间，已
初始化节点可继续 Raft 读写并保持 partition 路由，但 `/ready` 返回 503 degraded；空节点不会初始化。`/health` 只表示进程存
活，不代表 formation 或 quorum 就绪。恢复后先检查 `rr-admin formation status`，不要重新 Genesis。

`/ready` requires every managed group to be formed and a leader to be observed. A pristine node in
`awaiting_replication` returns 503. During a store outage, initialized nodes can continue Raft reads/writes and retain
partition routing, but `/ready` returns 503 degraded; pristine nodes do not initialize. `/health` reports process
liveness only, not formation or quorum readiness. After recovery, inspect `rr-admin formation status`; never rerun
Genesis as a repair.

## 相关 / Related

- [一般部署指南](./deployment.md)<br />[Generic deployment](./deployment.md)
- [TLS 配置](./tls.md)<br />[TLS setup](./tls.md)
- [C4 部署图](../architecture/c4/deployment.md)<br />[C4 deployment diagram](../architecture/c4/deployment.md)
