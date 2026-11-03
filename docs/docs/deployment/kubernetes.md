---
sidebar_position: 3
---

# Kubernetes 部署 / Kubernetes Deployment

rr 提供两种 Kubernetes 部署方式：**原始 manifests**（`deploy/k8s/`）用于快速试用，和 **Helm chart**（`deploy/helm/rr/`）用于生产。

---

rr ships two Kubernetes deployment paths: **raw manifests** (`deploy/k8s/`) for quick trials, and a **Helm chart** (`deploy/helm/rr/`) for production.

## 前置条件 / Prerequisites

- Kubernetes 1.27+
- A formation-capable rr image. **rr 0.2.0 cannot be used**: its strict parser rejects
  `[cluster.formation]` and the new cluster fields. An explicit image override is mandatory; no compatible
  released version is implied by these manifests.
- A StorageClass that supports `ReadWriteOnce` (default in most clusters)
- An etcd v3 endpoint reachable from every pod. The raw manifests expect a Service named `etcd` on port
  `2379`; Helm defaults to `http://rr-etcd:2379` and is configurable through `cluster.formation.etcdEndpoints`.

---

## 选项 A：原始 manifests / Option A: Raw manifests

先在使用的 raw manifests 中把 `REPLACE_WITH_FORMATION_CAPABLE_TAG` 替换为包含 managed formation 的镜像
tag：Genesis 使用 `statefulset.yaml`，收养第一阶段使用 `statefulset-adopt.yaml`，第二阶段还使用
`cluster-form-job.yaml`。不得保留占位符或使用 0.2.0，然后再 apply：

Replace `REPLACE_WITH_FORMATION_CAPABLE_TAG` in every raw manifest being applied with a formation-capable
image tag. The placeholder and rr 0.2.0 are both invalid for this configuration.

```bash
kubectl apply -f deploy/k8s/statefulset.yaml
```

这会部署：
- `StatefulSet rr`（3 副本）with `Parallel` 启动，避免 Genesis 在首个 pod readiness 前等待 quorum 的死锁
- Headless `Service rr`（pod DNS: `rr-0.rr`, `rr-1.rr`, `rr-2.rr`）
- ClusterIP `Service rr-client`（客户端连接入口）
- `ConfigMap rr-config`（所有 pod 相同的完整 advertised roster 和 Genesis formation settings）
- `PodDisruptionBudget rr`（minAvailable=2）
- `PersistentVolumeClaim` 每个 pod 20Gi

This deploys a three-replica parallel StatefulSet, headless/client Services, one homogeneous managed config,
a PodDisruptionBudget, and per-pod PVCs. Only the numeric node ID differs per ordinal; no bootstrap flag or
prefix peers list is derived.

**全新 Genesis / Fresh Genesis**：所有空 pod 并行启动，lifecycle CAS 授权完整 roster 初始化。不要 apply
`cluster-form-job.yaml`；该 Job 只用于旧集群收养或人工恢复。旧集群必须按
[有序收养流程](./upgrading.md)
先启动已初始化节点，再启动 pristine 节点并显式收敛成员。

---

## 选项 B：Helm chart / Option B: Helm chart

安装 / Install（替换两个占位符 / replace both placeholders）:

```bash
helm install rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=<formation-capable-tag> --wait
```

升级 / Upgrade:

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=<formation-capable-tag> --wait
```

### 两阶段旧集群收养 / Two-phase legacy adoption

`Parallel` 只能并发启动 pod，不能保证 initialized adopters 先于 pristine ordinals 完成收养。以下示例中
`rr-0` 是唯一 initialized legacy voter，`rr-1`/`rr-2` 是 pristine；有其他 initialized 节点时，把第一阶段的
数量改为它们的连续 ordinal 数量。两阶段必须使用相同 chart values 和完整三节点 SeedSpec。

`Parallel` alone does not order adoption. This example starts one initialized legacy voter (`rr-0`) first;
`rr-1` and `rr-2` remain absent until the adopted record is Active and `rr-0` is ready.

**Helm 第一阶段 / phase 1** — 只启动 initialized adopter，不渲染 convergence Job：

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=<formation-capable-tag> \
  --set cluster.formation.mode=adopt \
  --set cluster.formation.adoptionInitializedReplicas=1 --wait
kubectl wait --for=condition=ready pod/rr-0 --timeout=300s
```

**Helm 第二阶段 / phase 2** — 确认上一命令成功后，启动完整 roster 和 convergence Job：

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version <chart-version> \
  --set image.tag=<formation-capable-tag> \
  --set cluster.formation.mode=adopt \
  --set cluster.formation.adoptionInitializedReplicas=3 --wait
kubectl logs job/rr-cluster-form-<phase-2-revision> -f
```

**Raw 第一阶段 / phase 1** — 专用 manifest 固定 `replicas: 1`、`mode = "adopt"`，但保留三节点 SeedSpec：

```bash
kubectl apply -f deploy/k8s/statefulset-adopt.yaml
kubectl wait --for=condition=ready pod/rr-0 --timeout=300s
```

**Raw 第二阶段 / phase 2** — 仅在 `rr-0` ready 后启动 pristine 节点并显式收敛：

```bash
kubectl scale statefulset rr --replicas=3
kubectl apply -f deploy/k8s/cluster-form-job.yaml
kubectl wait --for=condition=complete job/rr-cluster-form --timeout=300s
kubectl logs job/rr-cluster-form -f
```

`image.tag` 为空或 `0.2.0` 时 chart 会直接拒绝渲染。`adopt` 的 convergence Job 是普通 Helm resource，
不再使用 post-install/post-upgrade hook；它只在第二阶段渲染，因此第二阶段 `--wait` 会等待 Job 完成。
Genesis 和收养第一阶段都不渲染该 Job。

The chart rejects an empty or 0.2.0 image tag. The adopt Job is an ordinary resource rendered only in phase 2,
so phase-2 `--wait` includes its completion. Genesis and adoption phase 1 render no such Job. Job names include
`.Release.Revision`, so upgrades create new Jobs instead of mutating completed immutable pod templates.

旧 revision 的已完成 Job 不会自动清理。只有在新 revision 的 convergence Job 完成、全量 voter 和 `/ready`
验证通过后，才可手工删除旧 completed Jobs；不要删除当前或仍在运行的 Job。

Older completed Jobs are not deleted automatically. Remove them manually only after the replacement Job has
completed and the new revision's membership/readiness checks pass; never delete the current or a running Job.

### 关键 values / Important values

| Key | Default | 说明 / Description |
| :--- | :--- | :--- |
| `image.repository` | `ghcr.io/gardenias/rr` | 镜像仓库 / Image repo (override to your own fork's GHCR namespace if you fork) |
| `image.tag` | `""` (render fails) | Mandatory formation-capable tag; `0.2.0` is rejected |
| `replicaCount` | `3` | Fixed initial roster size; not a P1 runtime scaling knob |
| `cluster.name` | `"rr"` | Shared SeedSpec cluster name |
| `cluster.formation.mode` | `"genesis"` | `genesis` for fresh nodes; `adopt` for initialized legacy nodes |
| `cluster.formation.adoptionInitializedReplicas` | `null` | Phase-1 initialized ordinal count; set to `replicaCount` for phase 2 |
| `cluster.formation.etcdEndpoints` | `["http://rr-etcd:2379"]` | Linearizable lifecycle store endpoints |
| `storage.backend` | `"rocks"` | `"mem"` or `"rocks"` |
| `storage.durability` | `"strict"` | `"strict"` / `"balanced"` / `"fast"` |
| `storage.dataSize` | `"20Gi"` | Per-pod PVC size |
| `storage.storageClassName` | `""` (cluster default) | Specify for local-ssd / fast-class |
| `auth.adminTokenSecret.name` | `rr-admin-token` | Secret holding admin bearer token |
| `tls.enabled` | `false` | Enable TLS on gRPC |
| `tls.existingSecret` | `""` | TLS secret name (tls.crt/tls.key/ca.crt) |
| `tls.requireClientCert` | `false` | mTLS mode |
| `security.allowInsecure` | `false` | DEV ONLY:渲染 `RR_ALLOW_INSECURE=true` 绕过 rr-server 启动安全 gate;与 `tls.enabled=false` 组合时模板直接 `fail` 拒绝渲染 / DEV ONLY — renders `RR_ALLOW_INSECURE=true`, bypassing startup security gates; the chart refuses to render when combined with `tls.enabled=false` |
| `resources.*` | 500m / 512Mi request | Adjust for workload |

### TLS setup with cert-manager

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

然后 / then:

```bash
helm install rr oci://ghcr.io/gardenias/charts/rr \
  --set tls.enabled=true \
  --set tls.existingSecret=rr-tls \
  --set tls.requireClientCert=true
```

### Admin token secret

```bash
kubectl create secret generic rr-admin-token \
  --from-literal=token="$(openssl rand -hex 32)"
```

## 操作 / Operations

### 查看集群状态 / View cluster status

```bash
kubectl exec -it rr-0 -- /usr/local/bin/rr-admin \
  --addr 127.0.0.1:52001 node status
```

### 备份 / Backup

```bash
kubectl exec rr-2 -- /usr/local/bin/rr-admin \
  backup --data-dir /var/lib/rr/data --output /tmp/backup.tar.gz --node-id 3
kubectl cp rr-2:/tmp/backup.tar.gz ./backup-$(date +%F).tar.gz
```

### P1 扩容与替换边界 / P1 Scaling and Replacement Boundary

不要在现有集群上通过 `helm upgrade --set replicaCount=5` 扩容。完整 roster 是 SeedSpec identity 的一部分，
改变它不会让 Raft membership 自动扩大，反而会使现有生命周期记录拒绝不匹配的节点。P1 不支持动态
membership expansion、replica replacement 或 resharding。

Do not scale an existing managed cluster by changing `replicaCount`. SeedSpec is not current Raft membership;
P1 has no dynamic expansion, replacement, or resharding. The adopt-only formation job performs initial legacy
convergence, not ongoing automatic scaling.

### Readiness 与 lifecycle store 故障 / Readiness and Lifecycle-Store Outage

`/ready` 要求所有托管 group formed 且观察到 leader。pristine 节点在 `awaiting_replication` 时返回 503。
store outage 期间，已初始化节点可继续 Raft 读写并保持 partition 路由，但 `/ready` 仍返回 503 degraded；
pristine 节点不会初始化。`/health` 仅表示进程存活性，不代表 formation 或 quorum 已就绪。

## 相关 / Related

- 一般部署指南 / Generic deployment: [deployment.md](./deployment.md)
- TLS 配置 / TLS setup: [tls.md](./tls.md)
- C4 部署图 / C4 deployment diagram: [../architecture/c4/deployment.md](../architecture/c4/deployment.md)
