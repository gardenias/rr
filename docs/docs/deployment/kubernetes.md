---
sidebar_position: 3
---

# Kubernetes 部署 / Kubernetes Deployment

原始 manifests（`deploy/k8s/`）用于快速试用，Helm chart（`deploy/helm/rr/`）用于需要参数化管理的部署。两者都使用 managed
formation，不是旧 bootstrap/peers 流程。

Use raw manifests (`deploy/k8s/`) for quick trials and the Helm chart (`deploy/helm/rr/`) for parameterized deployments.
Both use managed formation, not the legacy bootstrap/peers flow.

## 前置条件 / Prerequisites

- Kubernetes 1.27+，以及支持 `ReadWriteOnce` 的 StorageClass。
- 每个 Pod 可访问的 etcd v3。Raw manifests 默认使用 `http://etcd:2379`；Helm 默认使用 `http://rr-etcd:2379`。
- 显式指定支持 managed formation 的镜像。rr 0.2.0 的严格解析器拒绝 `[cluster.formation]` 和新 cluster 字段，不能使用，也
  不能保留 `REPLACE_WITH_FORMATION_CAPABLE_TAG`。
- 默认安全配置需要 `rr-tls` 和 `rr-admin-token` 两个 Secret。证书 SAN 必须覆盖实际拨入的 Pod/Service DNS 名。

Requirements:

- Kubernetes 1.27+ and a StorageClass supporting `ReadWriteOnce`.
- An etcd v3 endpoint reachable from every Pod. Raw manifests default to `http://etcd:2379`; Helm defaults to
  `http://rr-etcd:2379`.
- An explicit formation-capable image. rr 0.2.0 rejects `[cluster.formation]` and the new cluster fields; neither that
  release nor `REPLACE_WITH_FORMATION_CAPABLE_TAG` is valid.
- The default secure configuration requires `rr-tls` and `rr-admin-token` Secrets. Certificate SANs must cover the
  Pod/Service DNS names clients actually dial.

```bash
kubectl create secret generic rr-tls \
  --from-file=tls.crt=node.pem --from-file=tls.key=node.key --from-file=ca.crt=ca.pem
kubectl create secret generic rr-admin-token \
  --from-literal=token="$(openssl rand -hex 32)"
```

## 选项 A：原始 manifests / Option A: Raw Manifests

替换所有待 apply 文件中的镜像占位符。Genesis 使用 `statefulset.yaml`；收养还使用 `statefulset-adopt.yaml` 和第二阶段的
`cluster-form-job.yaml`。

Replace the image placeholder in every manifest being applied. Genesis uses `statefulset.yaml`; adoption also uses
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
  --set image.tag=<formation-capable-tag> \
  --set tls.enabled=true --set tls.requireClientCert=true \
  --set tls.existingSecret=rr-tls --wait
```

升级时固定同一花名册和 formation values，不要只更新镜像后假设扩缩容会自动发生。

For upgrades, keep the same roster and formation values; changing the image alone never implies automatic scaling.

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=<formation-capable-tag> \
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

:::warning 当前 convergence Job 的认证参数 / Authentication in the current convergence Job
当前仓库的 raw/Helm Job 模板注入了 `RR_ADMIN_TOKEN`，但 `rr-admin` 调用未传 `--token-env RR_ADMIN_TOKEN`；默认安全 Admin
会拒绝这些调用。执行收养前，应使用已修正此问题的模板，或在 Job 的每次 `rr-admin` 调用中显式加入该参数。Genesis 不运行此
Job，不受影响。

The current raw/Helm Job templates inject `RR_ADMIN_TOKEN` but omit `--token-env RR_ADMIN_TOKEN` from `rr-admin` calls,
so the default secure Admin service rejects them. Before adoption, use a template that fixes this or add the flag to
every `rr-admin` invocation in the Job. Genesis does not run this Job and is unaffected.
:::

Helm 第一阶段只启动 initialized adopter，不渲染 convergence Job：

Helm phase 1 starts only initialized adopters and renders no convergence Job:

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=<formation-capable-tag> \
  --set tls.enabled=true --set tls.requireClientCert=true --set tls.existingSecret=rr-tls \
  --set cluster.formation.mode=adopt \
  --set cluster.formation.adoptionInitializedReplicas=1 --wait
kubectl wait --for=condition=ready pod/rr-0 --timeout=300s
```

确认上一阶段成功后，Helm 第二阶段启动完整花名册和 convergence Job：

After phase 1 succeeds, Helm phase 2 starts the full roster and the convergence Job:

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=<formation-capable-tag> \
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

`image.tag` 为空或为 `0.2.0` 时 chart 拒绝渲染。`adopt` Job 是普通 Helm resource，只在第二阶段渲染，因此第二阶段
`--wait` 会等待它完成；Genesis 和收养第一阶段不渲染 Job。Job 名包含 `.Release.Revision`，升级会创建新 Job，而不是修改已
完成 Job 的不可变 Pod template。

The chart rejects an empty or `0.2.0` image tag. The adopt Job is an ordinary Helm resource rendered only in phase 2, so
phase-2 `--wait` includes its completion. Genesis and adoption phase 1 render no Job. Job names include
`.Release.Revision`, so upgrades create new Jobs rather than mutating completed immutable Pod templates.

只有新 revision 的 convergence Job 完成、完整 voter 集合和 `/ready` 均验证通过后，才能手工删除旧 completed Jobs；不要删
除当前或仍在运行的 Job。

Delete old completed Jobs manually only after the replacement Job completes and the new revision's full voter membership
and `/ready` checks pass. Never delete the current or a running Job.

### 关键 values / Important Values

| Key | Default | 说明 / Description |
| :--- | :--- | :--- |
| `image.repository` | `ghcr.io/gardenias/rr` | 镜像仓库；fork 后改成自己的命名空间 / Image repository; override for a fork |
| `image.tag` | `""`（渲染失败 / render fails） | 必填 formation-capable tag；拒绝 `0.2.0` / Mandatory formation-capable tag; `0.2.0` rejected |
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

不要通过 `helm upgrade --set replicaCount=5` 扩容。完整花名册属于 SeedSpec identity，改变它不会扩大 Raft membership，反
而会让现有 lifecycle 记录拒绝不匹配的节点。P1 不支持动态成员扩容、副本替换或 resharding；adopt-only formation Job 只负责
初始旧集群收敛。

Do not scale an existing managed cluster by changing `replicaCount`. The complete roster belongs to the SeedSpec
identity; changing it does not expand Raft membership and makes the existing lifecycle record reject mismatched nodes.
P1 supports neither dynamic expansion, replica replacement, nor resharding. The adopt-only formation Job performs
initial legacy convergence, not ongoing automatic scaling.

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

- [一般部署指南 / Generic deployment](./deployment.md)
- [TLS 配置 / TLS setup](./tls.md)
- [C4 部署图 / C4 deployment diagram](../architecture/c4/deployment.md)
