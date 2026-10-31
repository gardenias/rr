# 同构集群生命周期与统一机器组设计

- 日期：2026-10-31
- 状态：已与需求方确认设计边界，进入实施
- 范围：rr-core、rr-config、rr-config-etcd、rr-raft、rr-server、rr-admin，以及 kv-store 参考实现

## 1. 背景与目标

集群需要支持同构部署：所有机器使用相同的集群花名册和分片配置，不因承担首次初始化职责而使用不同角色参数。
Raft leader 仍由选举产生，初始化职责不构成永久节点角色。

运行中增删机器的主要目的是更换部署，例如把副本迁移到配置更好的物理机器。所有机器组成一个统一机器组，
共同承担全部 Raft groups；不同 groups 不使用交叉机器子集。

本设计解决四个问题：

1. 新集群无需外部一次性初始化命令即可自动形成。
2. 已初始化节点重启时恢复状态，不重放初始花名册。
3. 新机器通过 learner 和 Raft 复制加入所有既有 groups，替换旧机器副本。
4. 旧机器只有在全部 groups 安全移除后才能退役，且不能通过旧配置自动复活。

## 2. 产品边界

### 2.1 目标

- 全局统一机器 roster；所有 groups 使用同一组机器。
- `partition → group` 路由在机器更换过程中保持不变。
- 新机器加入所有既有 groups，逐组完成 learner 同步、提升和旧副本移除。
- 初始化、重启、扩容、缩容和更换机器均可重入，并能在故障后恢复。
- 配置变更与 Raft 实际 membership 最终一致，过程状态可查询。
- 客户端在副本更换期间继续通过 leader redirect 读写。

### 2.2 非目标

- 不支持不同 groups 使用不同机器子集。
- 不支持 `partition → group` 在线迁移。
- 不支持真正 resharding、partition split/merge 或跨 group 历史状态搬迁。
- 不承诺跨多个 Raft groups 的全局原子 membership 事务。
- 不把 `PartitionConfig::Range` 的实现纳入本工作。

## 3. 核心模型

### 3.1 两层路由

```text
PartitionId → GroupId                    分片路由，机器更换时不变
GroupId     → 当前 Raft voters/learners  共识事实，逐组独立变化
NodeId      → NodeAddr                   全局机器地址册
```

所有 groups 共享同一份期望机器集合。现有 `GroupMeta.replicas` 可以继续按组序列化，但控制面必须校验它们一致，
不能把统一机器组退化为任意交叉放置。

### 3.2 三类状态职责

**SeedSpec** 只描述首次建群：

- cluster name、初始 nodes、初始 groups、统一初始 voters、初始 partitions；
- 规范化排序后计算 seed hash；
- 永远不作为当前 membership 在重启时回放。

**Raft membership** 是运行时共识事实：

- 当前 voters、learners、joint/uniform 状态以 Raft 日志和快照为准；
- 既有 group 的新副本只能通过 learner 和日志/快照复制加入；
- 运行中增删节点不得调用 `initialize`。

**Topology/LifecycleStore** 是控制面事实：

- `cluster_uuid`、`topology_revision`、`seed_hash`；
- 每 group 的 `bootstrap_epoch` 和 formation 状态；
- 全局期望机器集合；
- 节点的 `Joining / Active / Retiring / Retired` 状态和 incarnation；
- `operation_id`、操作类型、目标 revision 和每 group 执行进度。

生命周期判断需要线性化读取与 CAS。普通路由 watch、stale cache 或本地 pristine 状态不能作为建群授权。
生产后端可复用 etcd 的事务能力；内存后端仅用于单控制面测试和开发，不宣称提供跨进程协调。

## 4. 关键不变量

1. 同一 `(cluster_uuid, group_id, bootstrap_epoch)` 只能使用一份完全相同的初始 roster。
2. 已存在 group 不得因任何节点本地 pristine 而重新 initialize。
3. 既有 group 的新机器只能经 learner 同步加入。
4. 机器集合统一不等于 membership 全局原子；每个 group 独立提交并记录进度。
5. 自动提升只允许作用于 `Joining` 且仍在期望集合中的 learner。
6. `Retiring / Retired` 节点不得被自动提升、重新 initialize 或使用旧 roster 重建集群。
7. 被移除节点最终既不在 voters，也不在 learners/nodes；`retain=true` 不能表示完成退役。
8. 删除提交前不得停止仍被实际 quorum 需要的本地 group。
9. 所有 groups 完成前，整台机器的加入或退出不得报告成功。
10. `partition → group` 在节点加入、替换、删除期间保持不变。
11. 任何 `Blocked / Failed / Unknown / AbortedQuorumLoss` 都必须使运维命令返回非成功状态。
12. `force` 只能绕过明确标识的本地策略，不得被描述为绕过 Raft quorum。

## 5. 产品工作流

### 5.1 首次同构自动建群

1. 所有节点读取相同 SeedSpec。
2. 本地已初始化的 group 直接恢复，不执行 initialize。
3. pristine 节点线性化读取生命周期记录：
   - 记录不存在时，通过 CAS 创建 `cluster_uuid`、seed hash 和 bootstrap epoch；
   - CAS 失败者重读；seed hash 不同则拒绝建群；
   - group 已存在或已 Active 时不得走首次建群。
4. 获得 formation 授权的初始副本使用相同 `BTreeMap<NodeId, BasicNode>` 调用 initialize。
5. `NotAllowed` 仅作为并发或重试信号，必须重新读取实际状态，不能单独作为配置一致性的证明。
6. 各 group 独立选举 leader；迟到的初始节点在 group Active 后通过复制接管，不再回放 seed。
7. readiness 区分进程已监听、group 已形成和集群已有可服务 leader/quorum。

### 5.2 普通重启

- Rocks 节点存在日志、vote 或快照状态时，恢复本地 Raft 状态，绝不 initialize。
- Mem 节点重启后虽然本地 pristine，但生命周期记录显示 group 已存在，因此按复制恢复处理，不重新建群。
- 本地 cluster identity 与控制面 `cluster_uuid` 不一致时 fail-closed。
- 初始 SeedSpec 不得覆盖运行期间已经形成的机器集合。

### 5.3 新增机器

以新增 N4 为例：

1. 注册 N4 的地址、incarnation 和 `Joining` 状态。
2. N4 启动所有既有 groups 的空实例，但不 initialize。
3. 每个 group 的 leader 分别执行 `add_learner(N4)`。
4. N4 通过日志或 snapshot 同步各 group 的完整状态。
5. 每个 group 独立等待追平，达到 committed/applied 和复制新鲜度要求后提升 N4。
6. 所有 groups 完成后，N4 标记为 `Active`；任一 group 未完成则整机操作保持 Pending。

节点地址注册本身不直接产生 membership 副作用。统一机器组语义由生命周期编排器驱动，而不是由每个节点收到
`NodeAdded` 后各自发起阻塞调用。

### 5.4 更换机器

以 `{N1,N2,N3}` 中用 N4 替换 N3 为例：

1. 按新增流程让 N4 加入所有 groups 并追平。
2. 每个 group 独立提交目标 voter 集 `{N1,N2,N4}`。
3. 若 N3 是某组 leader，先向健康、追平且保留的 voter 移交 leadership。
4. 确认各组提交后，真正移除 N3 的 voter/learner 记录。
5. 所有 groups 完成后，将 N3 标记为 `Retired` 并停止其本地 groups 和进程。
6. 全部过程中 `partition → group` 不变，客户端写不需要跨组迁移协议。

默认保持原 replication factor，避免把机器更换变成所有 groups 的永久 voter 扩组。

### 5.5 减少机器

1. 将目标标记为 `Retiring`，阻止自动提升和重新加入。
2. 枚举目标参与的全部 groups。
3. 如果目标集合不满足预期容错能力，先添加并追平替代副本。
4. 必要时安全转移 leadership。
5. 逐组提交 membership 删除并确认剩余实际 quorum。
6. 确认目标不在 voters、learners/nodes 后，再停止本地 group。
7. 全部 groups 完成后写全局 `Retired` tombstone。
8. 复用相同 node ID 必须显式创建新 incarnation 和 join grant，不能隐式清除 tombstone。

### 5.6 状态丢失恢复

- 单 follower wiped：识别为既有节点的状态丢失，进入 recovery/quarantine，经 learner 复制恢复。
- 多数节点 wiped 或 group 已失去 quorum：不得自动从空状态重建，进入显式灾难恢复。
- 生命周期存储不可用：pristine 节点 fail-closed，不得利用 stale cache 判定“集群不存在”。

## 6. 控制面与配置传播

### 6.1 每 group 串行协调

Admin、自动提升和配置 reconciliation 必须进入同一个 leader-only 控制面：

- follower 不直接提交 membership 变更；
- leader 切换后重新读取实际 membership 并恢复 operation；
- 操作携带 `operation_id` 和目标 topology revision；
- 自动任务和人工操作串行，避免基于陈旧快照做全量替换；
- 多 group 通过持久操作记录最终收敛，不提供虚假的全局事务。

### 6.2 版本化配置契约

配置传播采用：

```text
VersionedRoutingTable { revision, table }
ConfigWatchItem::{Snapshot, Delta, ResyncRequired}
```

要求：

- 初始快照和 watch 起点之间无丢失窗口；
- Mem lag、Etcd compaction、stream 结束和解析失败都进入全量 resync；
- 消费者只应用单调递增 revision；
- Mem 与 Etcd 对相同操作提供一致的最终表和生命周期语义；
- watch 只负责唤醒 reconciliation，不能替代全量状态对账。

### 6.3 有界 learner 状态机

`add_learner` 提交与追平等待分离：

- 提交使用非阻塞模式；
- 等待有 deadline、取消和可查询状态；
- 追平要求同 leader term 内持续满足 lag、近期复制进展以及 committed/applied 水位；
- 超时保持 learner，不自动提升，不阻塞其他配置事件；
- 地址变化必须显式拒绝或更新，不得静默成功但沿用旧地址。

## 7. 运维契约

- 操作状态统一为 `Pending / Done / Blocked / Failed / Unknown`。
- 所有长操作支持按 `operation_id` 查询和安全重试。
- decommission dry-run 列出全部 groups、目标 voters、替代副本要求和风险。
- 部分 group 成功不得被描述为整机成功。
- leadership drain 只有观察到明确的其他 leader ID 才算成功；`None` 或仅触发选举不算成功。
- 生产默认目标至少 3 voters；2 voters 可用但不具备单节点故障容忍度，必须显式提示。

## 8. 兼容与迁移

- 已初始化集群升级后不得重新 initialize。
- 现有单 voter bootstrap + formation job 集群通过 learner/membership 收敛到统一 roster。
- 原 `NodeAdded` 对全部 groups 的语义可以保留在产品层，但实现必须改为统一编排、按组非阻塞执行。
- 初始 roster、当前 membership 和节点生命周期不得继续混用同一个布尔 `--bootstrap` 语义。
- 在自动 formation 验收完成前，保留现有人工路径作为兼容和灾难恢复入口。

## 9. 验收标准

### 9.1 首次启动与重启

- 1/3/5 节点并发同构启动，每 group 最终只有一个 leader，初始 voters 等于 SeedSpec。
- 相同 roster 顺序不同可正常建群；实际 node ID/地址或 seed hash 不同必须拒绝。
- Mem/Rocks 滚动重启不重新 initialize，不回放旧 roster，已 ACK 数据保留。
- 既有 group 的新节点和 wiped follower 从未调用 initialize。

### 9.2 动态机器生命周期

- 真实新增节点加入所有 groups，先 learner 后 voter，未授权或失联 learner 不提升。
- 替换机器时持续 keyed 写和 linearizable read，所有已 ACK 写保留。
- 删除 leader/follower 后节点不残留 learner，等待多个提升窗口也不会复活。
- 一组成功、一组 Blocked/Failed 时整机和 CLI 不报告成功，可按 operation ID 续跑。
- 在 learner 添加、同步、提升、joint/uniform、旧副本停止各阶段注入 leader/协调器故障，最终收敛。

### 9.3 配置后端

- 独立 Mem provider 与真实 Etcd 运行同一套一致性测试，不使用共享 Arc 掩盖跨节点传播。
- 覆盖初始 snapshot 并发写、lag、断连、compaction、重订阅、陈旧/重复事件和全量对账。
- 节点变更只影响 Raft membership 和统一机器集合，不改变任何 partition 路由。

### 9.4 部署

- 覆盖安装、旧版本升级、滚动重启、3→4、4→3 和机器替换。
- scale-down 在全部 groups 安全移除前不得删除 pod/机器。
- 自动 formation 稳定后可移除普通 formation job 的运行时职责，但保留灾难恢复文档。

## 10. 实施阶段

1. **P0 安全基线**：删除防复活、真正移除 learner、decommission 结果契约、配置对账基础。
2. **P1 同构 formation**：版本化 SeedSpec、生命周期授权、Mem/Rocks 重启和旧集群升级。
3. **P2 动态机器组**：统一 per-group 编排、有界 learner 状态机、真实新增/删除与故障恢复。
4. **P3 副本替换与部署**：保持 partition 路由不变，完成机器更换和 scale-down 验收。
5. **后续独立项目**：真正 resharding；不属于本规范的交付范围。
