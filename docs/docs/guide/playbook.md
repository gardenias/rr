---
sidebar_position: 2
---

# 运维手册 / Operations Playbook

本手册使用仓库根目录的 `just` recipe 执行本地集群流程。后台 PID 和日志位于 `target/just/`；recipe 只管理自己启动的进程和容器。
This playbook uses the repository's `just` recipes for local cluster workflows. Background PIDs and logs live under `target/just/`; recipes manage only the processes and containers they start.

## 1. 环境检查 / Environment Check

按任务安装 Rust、Python 3、Java、Node/npm、Helm 和 Docker/Colima。`doctor` 只检查，不安装软件。
Install the tools required by your task: Rust, Python 3, Java, Node/npm, Helm, and Docker or Colima. `doctor` checks without installing anything.

```bash
just doctor
just --list
```

Docker daemon 不在默认 socket 时，设置 `RR_DOCKER_HOST`：
If the Docker daemon is not on the default socket, set `RR_DOCKER_HOST`:

```bash
export RR_DOCKER_HOST=unix:///Users/$USER/.colima/default/docker.sock
```

容器 recipe 只需要可访问的 Docker API 和已在本机的镜像，不会隐式拉取镜像。
Container recipes need only a reachable Docker API and images already present locally; they do not pull images implicitly.

## 2. 冒烟与开发检查 / Smoke and Development Checks

运行单进程演示：
Run the in-process demo:

```bash
cargo run -p kv-store
```

它创建单副本内存 Raft 组，执行少量读写后退出；不验证持久化或多节点复制。
It creates a single-replica in-memory Raft group, performs a few reads and writes, and exits. It does not verify persistence or multi-node replication.

常用开发检查：
Common development checks:

```bash
just build
just test
just test-one membership_guard
just check
```

`just check` 包含格式、invariant registry 和 workspace check。迭代时可用 `test-one` 缩小范围，集成前运行完整测试。
`just check` covers formatting, the invariant registry, and workspace checking. Use `test-one` while iterating and the full suite before integration.

## 3. 手工三节点 KV 集群 / Manual Three-Node KV Cluster

该流程不依赖生命周期存储，所有节点使用同一份 `id@addr` 花名册，状态保存在内存。
This path uses no lifecycle store. Every node receives the same `id@addr` roster, and state remains in memory.

```bash
just kv-cluster-up
just kv-cluster-status
just kv-cluster-put
```

默认客户端端口为 53101–53103，日志为 `target/just/logs/kv-node{1,2,3}.log`。确认各节点有相同 `leader_node_id`，写入后 `last_log_index` 均推进；follower 写入应由客户端重定向。
Default client ports are 53101–53103 and logs are `target/just/logs/kv-node{1,2,3}.log`. Confirm a common `leader_node_id` and advancing `last_log_index` on every node after writes; follower writes should be redirected by the client.

停止 recipe 启动的节点：
Stop the nodes started by the recipe:

```bash
just kv-cluster-down
```

若端口冲突，检查 Docker  wildcard 端口映射等已有监听，并换用空闲基端口：
If ports conflict, check existing listeners such as Docker wildcard port mappings and select a free base port:

```bash
RR_KV_BASE_PORT=54101 just kv-cluster-up
```

## 4. 同构 Genesis 集群 / Homogeneous Genesis Cluster

所有节点使用相同完整花名册和生命周期配置，没有专用 `--bootstrap` 节点。前提是本地已有 `quay.io/coreos/etcd:v3.5.13`，etcd 可达且端口空闲。
Every node uses the same complete roster and lifecycle settings, with no special `--bootstrap` node. The local `quay.io/coreos/etcd:v3.5.13` image, a reachable etcd endpoint, and free ports are required.

```bash
just genesis-up
just genesis-status
```

Genesis 通过线性一致的生命周期记录授权组建。内存节点重启后不会重放 seed；若组已 Active，它等待幸存成员复制。Rocks 节点会恢复本地身份和 Raft 状态。
Genesis is authorized through a linearizable lifecycle record. A restarted in-memory node does not replay the seed; if the group is already Active, it waits for replication from survivors. Rocks-backed nodes restore local identity and Raft state.

停止本流程创建的节点和 etcd 容器：
Stop the nodes and etcd container created by this workflow:

```bash
just genesis-down
just etcd-down
```

覆盖默认端口或集群名：
Override the default ports or cluster name:

```bash
RR_GENESIS_BASE_PORT=54201 RR_CLUSTER_NAME=rr-dev just genesis-up
```

同名记录只允许重启同一集群；不同花名册会被拒绝，不会另建一套配置。
Reusing a name is valid only for restarting the same cluster; a different roster is rejected rather than forming another configuration.

## 5. 收养旧集群 / Legacy Adoption

只对已初始化的旧集群使用 adopt，顺序如下：
Use adoption only for an existing initialized cluster, in this order:

1. 先以 adopt 模式启动已初始化节点。/ Start initialized nodes in adopt mode.
2. 确认记录为 `LegacyAdopted` 且组为 Active。/ Verify `LegacyAdopted` and Active groups.
3. 再启动空节点。/ Start pristine nodes.
4. 通过既有 Job 或 `add-learner` + `change-membership` 收敛成员。/ Converge membership through the existing job or explicit learner and membership operations.

收养不会自行初始化或扩容，正常流程不要清理数据目录。Kubernetes 阶段见 [升级指南](../deployment/upgrading.md)。
Adoption never initializes or expands membership by itself. Do not delete data directories during normal adoption. See [Upgrading](../deployment/upgrading.md) for the Kubernetes phases.

## 6. Rocks 持久化重启检查 / Durable Rocks Restart Checks

运行库级测试：
Run the library-level test:

```bash
cargo test -p integration --features rocks --lib homogeneous_formation -- --nocapture
```

进程级测试需要真实 etcd 和 `kv-store`，使用与本地镜像相同的 Docker daemon：
The process-level test needs real etcd and `kv-store` processes, using the Docker daemon that owns the local image:

```bash
DOCKER_HOST="$RR_DOCKER_HOST" RR_TEST_ETCD=1 \
  cargo test -p kv-store --features rocks --test homogeneous_process -- --include-ignored --nocapture
```

Rocks 模式持久化 Raft 日志、快照和框架身份；示例的业务 KV map 仍在内存，不能视为生产存储实现。
Rocks mode persists Raft logs, snapshots, and framework identity. The example's business KV map remains in memory and is not a production storage implementation.

## 7. 故障转移与恢复 / Failover and Recovery

三节点运行时，记录 leader，停止其进程，再检查状态：
With three nodes running, identify and stop the leader, then check status:

```bash
just kv-cluster-status
```

recipe 启动的进程 PID 位于 `target/just/pids/`。多数节点存活时应选出新 leader，已提交数据仍可读取。
PIDs for recipe-started processes are under `target/just/pids/`. A surviving majority should elect a new leader and preserve committed data.

内存节点重启会丢失本地日志和投票状态，需由幸存副本重新复制；全部重启不会保留写入。持久恢复使用 `rocks`。
Restarting an in-memory node loses its local log and vote, so survivors replicate state again. Restarting all nodes loses writes. Use `rocks` for durable recovery.

## 8. 管理与可观测性 / Admin and Observability

`kv-store` 的 Admin 端口为主端口加 1000。默认基端口下：
`kv-store` serves Admin on the main port plus 1000. With the default base port:

```bash
target/debug/rr-admin --addr 127.0.0.1:54101 --output json group status --group 1
target/debug/rr-admin --addr 127.0.0.1:54101 --output json group membership --group 1
```

替换为实际基端口加 1000。组处于 Forming 时，托管组建会拒绝成员变更；Active 后恢复常规管理操作。
Replace 54101 with the selected base port plus 1000. Managed formation rejects membership changes while a group is Forming; normal operations resume once Active.

健康与指标端点需要配置 metrics 端口。生命周期存储中断时，空节点不就绪；已初始化节点可继续 Raft 流量，但 readiness 为 degraded。
Health and metrics endpoints require a configured metrics port. During a lifecycle-store outage, pristine nodes stay unready; initialized nodes may continue Raft traffic with degraded readiness.

## 9. 验收与质量门禁 / Acceptance and Quality Gates

```bash
just test-etcd
just helm-test
```

`test-etcd` 需要本地 etcd 镜像，运行生命周期 CAS 和进程级组建测试；`helm-test` 需要 Helm，检查 Genesis/adoption 渲染。被跳过的测试不算通过，失败详情见 Cargo 输出或 `target/just/logs/`。
`test-etcd` requires the local etcd image and runs lifecycle CAS plus process-level formation suites. `helm-test` requires Helm and checks Genesis/adoption rendering. Skipped tests are not passes; inspect Cargo output or `target/just/logs/` for failures.

本地完整门禁：
Local quality gates:

```bash
just nightly
just bench-gate
just soak-smoke
```

`nightly` 运行 invariant、workspace、crash、fuzz 和 formal 检查，并将日志写入 `target/quality-runs/<timestamp>-nightly/`。`bench-gate` 需要先用 `just bench-save-baseline` 创建基线；`soak-smoke` 需要本地 `rr-lab:dev` 镜像。
`nightly` runs invariant, workspace, crash, fuzz, and formal checks, archiving logs under `target/quality-runs/<timestamp>-nightly/`. `bench-gate` requires a baseline from `just bench-save-baseline`; `soak-smoke` requires the local `rr-lab:dev` image.

## 10. Java SDK / Java SDK

SDK 位于 `sdks/java`，使用自带 Gradle wrapper：
The SDK lives in `sdks/java` and uses its Gradle wrapper:

```bash
just java-build
just java-test
just java-integration-test
just java-javadoc
just java-publish-local
```

集成测试遵循 `RR_TEST_DOCKER=1`，需要可用 Docker daemon。`java-publish-local` 只写入 Maven Local。
Integration tests use `RR_TEST_DOCKER=1` and require a reachable Docker daemon. `java-publish-local` writes only to Maven Local.

API 示例见 [Java SDK README](https://github.com/gardenias/rr/blob/main/sdks/java/README.md)。
See the [Java SDK README](https://github.com/gardenias/rr/blob/main/sdks/java/README.md) for API examples.

## 11. 故障排查 / Troubleshooting

**生命周期存储不可用 / Lifecycle store unavailable**

空节点启动失败或保持不就绪；已初始化节点可恢复本地 Raft，但存储恢复前 readiness 保持 degraded。
Pristine nodes fail closed or stay unready. Initialized nodes may recover local Raft, but readiness remains degraded until the store returns.

**同名集群使用不同 seed / Different seed under the same cluster name**

组建会拒绝不匹配的花名册。新部署使用新集群名，否则恢复原 SeedSpec。
Formation rejects the mismatched roster. Use a new name for a new deployment or restore the original SeedSpec.

**成员操作返回 `FailedPrecondition` / Membership operation returns `FailedPrecondition`**

组仍在 Forming 或生命周期状态不可用。先检查：
The group is still Forming or lifecycle state is unavailable. Check first:

```bash
rr-admin --addr <admin-addr> formation status
```

`origin=Untracked` 表示旧集群没有生命周期记录，应按收养流程处理，不要重新 bootstrap。组长期 Forming 时，检查所有节点的 etcd 连通性、相同 seed，以及 `rr_formation_cas_total{result="conflict"}` 和 WARN 日志；Active 后再重试成员变更。
`origin=Untracked` means the legacy cluster has no lifecycle record; adopt it instead of re-bootstrapping. For a group stuck Forming, check etcd reachability, identical seeds, `rr_formation_cas_total{result="conflict"}`, and WARN logs. Retry membership changes after Active.

**命令中断后仍有后台进程 / Background processes remain after interruption**

需要保留故障证据时先查看日志，再清理 recipe 的 PID 状态和进程：
Inspect logs first if you need failure evidence, then clean up recipe processes and PID state:

```bash
just clean-target-just
```

## 12. 边界 / Boundaries

本手册不覆盖动态副本替换、resharding、分区拆分/合并或多数状态丢失后的灾难恢复。SeedSpec 只描述初始组建；组 Active 后，当前 Raft membership 才是运行时成员依据。
This playbook does not cover dynamic replica replacement, resharding, partition split/merge, or disaster recovery after majority state loss. SeedSpec describes initial formation; once Active, current Raft membership is authoritative.
