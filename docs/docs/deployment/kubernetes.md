---
sidebar_position: 3
---

# Kubernetes 部署 / Kubernetes Deployment

rr 提供两种 Kubernetes 部署方式：**原始 manifests**（`deploy/k8s/`）用于快速试用，和 **Helm chart**（`deploy/helm/rr/`）用于生产。

---

rr ships two Kubernetes deployment paths: **raw manifests** (`deploy/k8s/`) for quick trials, and a **Helm chart** (`deploy/helm/rr/`) for production.

## 前置条件 / Prerequisites

- Kubernetes 1.27+
- Docker image published to `ghcr.io/gardenias/rr:<version>` (or a local registry / your own fork's GHCR namespace)
- A StorageClass that supports `ReadWriteOnce` (default in most clusters)

---

## 选项 A：原始 manifests / Option A: Raw manifests

```bash
kubectl apply -f deploy/k8s/statefulset.yaml
```

这会部署：
- `StatefulSet rr`（3 副本）with `OrderedReady` 启动
- Headless `Service rr`（pod DNS: `rr-0.rr`, `rr-1.rr`, `rr-2.rr`）
- ClusterIP `Service rr-client`（客户端连接入口）
- `ConfigMap rr-config`（config.toml 模板）
- `PodDisruptionBudget rr`（minAvailable=2）
- `PersistentVolumeClaim` 每个 pod 20Gi

This deploys: StatefulSet (3 replicas, OrderedReady), headless Service, ClusterIP service for clients, ConfigMap with config template, PodDisruptionBudget (minAvailable=2), per-pod 20Gi PVC.

**初始化流程 / Bootstrap flow**：
- `rr-0` 以 `bootstrap=true` 启动（single-member cluster）
- `rr-1` 启动后连接到 `rr-0.rr:51001` 作为 peer
- `rr-2` 启动后连接到 `rr-0.rr:51001, rr-1.rr:51001`
- initContainer `render-config` 根据 pod ordinal 渲染 peers 列表

---

## 选项 B：Helm chart / Option B: Helm chart

安装 / Install:

```bash
helm install rr oci://ghcr.io/gardenias/charts/rr --version 0.1.0
```

升级 / Upgrade:

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --version 0.2.0
```

### 关键 values / Important values

| Key | Default | 说明 / Description |
| :--- | :--- | :--- |
| `image.repository` | `ghcr.io/gardenias/rr` | 镜像仓库 / Image repo (override to your own fork's GHCR namespace if you fork) |
| `image.tag` | (Chart.appVersion) | Image tag |
| `replicaCount` | `3` | Must be odd; 3/5/7 typical |
| `storage.backend` | `"rocks"` | `"mem"` or `"rocks"` |
| `storage.durability` | `"strict"` | `"strict"` / `"balanced"` / `"fast"` |
| `storage.dataSize` | `"20Gi"` | Per-pod PVC size |
| `storage.storageClassName` | `""` (cluster default) | Specify for local-ssd / fast-class |
| `auth.adminTokenSecret.name` | `rr-admin-token` | Secret holding admin bearer token |
| `tls.enabled` | `false` | Enable TLS on gRPC |
| `tls.existingSecret` | `""` | TLS secret name (tls.crt/tls.key/ca.crt) |
| `tls.requireClientCert` | `false` | mTLS mode |
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

### 扩容 / Scale up (3 → 5)

```bash
helm upgrade rr oci://ghcr.io/gardenias/charts/rr --set replicaCount=5
```

新节点 rr-3 / rr-4 会作为 learner 加入，然后由 leader 通过 `ChangeMembership` 提升为 voter（目前需要手动通过 rr-admin 触发；自动化在 Phase 3.1 roadmap）。

---

New nodes rr-3 / rr-4 join as learners; promotion to voter via `ChangeMembership` is currently manual via rr-admin (auto-promotion is on the Phase 3.1 roadmap).

## 相关 / Related

- 一般部署指南 / Generic deployment: [deployment.md](./deployment.md)
- TLS 配置 / TLS setup: [tls.md](./tls.md)
- C4 部署图 / C4 deployment diagram: [../architecture/c4/deployment.md](../architecture/c4/deployment.md)
