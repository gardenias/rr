# Rr Cluster Admin Console 设计

- 日期：2026-11-15
- 状态：设计阶段交付物，待 owner 批准；不包含产品代码实现
- 范围：框架自带的集群管理后台、节点发现、采集聚合、只读观测 UI，以及后续受控操作边界
- UI 基线：`my-biz/docs/superpowers/specs/2026-10-22-cex-admin-ui-design.md` v4 与
  `my-biz/docs/superpowers/specs/ui-mockups/00-design-system.html`
- 品牌基线：仓库根目录 `.impeccable.md`

## 1. 产品意图

为所有基于 Rr 构建的应用提供一个可独立部署、默认可发现集群的管理后台。使用方只需提供任一
AdminService 种子地址，或现有 discovery/etcd 配置，就能看到一个一致的集群视图，而不必逐台拼接
`rr-admin` 输出、Prometheus 指标和节点日志。

核心用户是 SRE、后端工程师和分布式系统维护者。核心任务不是“看一张漂亮仪表盘”，而是回答：

1. 集群现在是否能工作，哪些节点或组需要处理？
2. 故障发生在哪一层：机器、网络、Raft 选举、复制、apply、快照、路由还是存储？
3. 当前延迟和吞吐是否异常，异常集中在哪个节点、组或副本？
4. 节点最近是否重启、是否正在滚动升级、各副本是否运行兼容版本？
5. 页面上的数据来自哪里、何时采集、是否完整，能否据此安全地下结论？

### 1.1 目标

- 一个命令启动，支持 `--peers` 和 discovery 两种接入方式。
- 覆盖节点资源、节点与 Raft 拓扑、组内角色/term/复制延迟、吞吐/延迟、快照、重启和版本历史。
- 同时呈现控制面期望拓扑、Raft 运行时状态和控制台观测状态，不混为一个“健康”字段。
- 每个聚合视图都有来源、采样时间、新鲜度和缺失原因。
- 前端与 Rr 品牌一致，复用指定设计系统的布局、组件、动效和无障碍规则，仅替换颜色及领域组件。
- 控制台自身不能成为集群控制面，也不能因浏览器数量增加而线性放大节点采集压力。

### 1.2 非目标与反目标

- P0 只做只读观测，不提供重启进程、修改 membership、强制选主或 reset ingestor offset。
- 不替代 Prometheus 的长期存储、Grafana 的自由分析、OpenTelemetry trace 或集中日志系统。
- 不用端口加减规则猜测 Admin/Metrics 地址，不把 `client_addr` 当成 `admin_addr`。
- 不把控制台无法连接多数节点直接表述为“集群失去 quorum”；那只是控制台视角的风险。
- 不平均各节点 P99，不把 follower 缺失的 replication lag 当成 0，不显示废弃的
  `snapshot_in_progress=false` 作为真实进度。
- 不为填满界面而显示无来源的 CPU、磁盘、重启原因、历史版本或告警。
- P0 不多集群管理、不内置通知渠道、不开放任意 PromQL 代理。

## 2. 当前能力与必须补齐的契约

### 2.1 已确认可复用

- `AdminService` 已有节点状态、组列表/状态、成员、快照 index/term、路由和 formation 查询。
- 已有受控 RPC：TriggerSnapshot、TransferLeader、DrainNode、AddLearner、ChangeMembership、
  RemoveGroupMember、OnlineBackup 等；它们是 P1 操作中心的后端基础，不在 P0 开放。
- `NodeStatus` 当前提供 `node_id`、`listen_addr`、`group_ids`、`uptime_secs`。
- `GroupStatus/GroupSummary` 当前提供 leader、term 和 last log index。
- `GetGroupMembership` 提供 voters、learners 及地址；`GetFormationStatus` 提供集群 UUID、
  topology/store revision 和 formation 状态。
- `/health`、`/ready` 和 Prometheus `/metrics` 已存在；真实性能数据必须走 `/metrics`，
  `GetMetricsSummary` 已废弃且服务端返回 unimplemented。
- 已有 proposal/server/apply/AppendEntries RTT histogram、replication lag、snapshot transfer、
  request/apply/ingestor counter、security gauge、生命周期相关计数器。
- `ConfigProvider` 和 etcd provider 可读取/订阅路由表；节点、分区和组是配置面的不同事实域。

### 2.2 必须新增，不能用 mock 数据代替

1. **节点目录与完整描述符**：node ID、client/admin/observability 地址、advertise 地址、环境和可选
   region/zone/rack 标签。现有 `NodeAddr` 只有一个地址，不能支撑自动发现三个端口角色。P0 新增独立
   `NodeDirectory` 读写契约，由节点进程在启动后发布带 lease/revision 的目录记录并周期续租；它不复用
   只读 `ConfigProvider`，也不把 managed formation 的 LifecycleStore 误当成可写节点地址册。部署系统也
   可以 provision 同一 schema。没有目录记录的旧集群继续使用显式 Admin seed。
2. **构建信息**：Rr 版本、应用版本、git commit、dirty、build profile、协议兼容版本、boot ID、
   process start time 和配置指纹。应用版本与框架版本必须分列。兼容性由 `rr-core` 中的版本化规则
   计算，不由 UI 猜测版本号范围。
3. **节点资源采样**：CPU 核数/限制与使用率、内存总量/限制与使用量、网络收发字节率、磁盘容量/
   使用量及可选 IO 指标，并标明 `process/cgroup/host` 口径。
4. **完整副本状态**：ServerState、commit/applied/matched index、leader 观察时间、最后心跳、
   AppendEntries RTT 和 snapshot 状态。leader-only 数据必须保留来源谓词。
5. **本地启动日志**：每次启动追加 boot record，优雅关闭补写 shutdown record；异常终止只能标记
   `abnormal_or_uncertain`，不能伪造崩溃原因。RocksDB 节点默认放在 data dir；`storage=mem` 或无
   data dir 的嵌入节点默认只保留当前 boot 并标记 `VOLATILE`。需要持久历史时必须显式配置独立
   journal dir。每节点最多保留 100 个 boot，查询分页，概要轮询只取最新记录。
6. **真实快照任务状态**：若需要“构建中/传输中/进度百分比”，必须新增运行时任务事件和进度，不能
   继续使用已废弃的 bool 占位。P0 不承诺这些任务态；现有 build failure 无 group 标签时只能显示
   节点级失败，per-group 单元格为 `UNSUPPORTED`。
7. **完整集群拓扑 RPC**：当前 `RoutingTableSnapshot` 响应只含 partition routes；Console 需要节点
   地址册、组副本、partition 映射和 revision 的稳定分页快照。

新增 RPC/字段必须向后兼容：旧节点返回 `UNIMPLEMENTED` 或缺少可选能力时，页面显示
`UNSUPPORTED` 和具体原因，而不是显示零值。旧 `GroupStatus` 以 `leader_node_id=0` 表示无 leader，
兼容层必须转换为 `None/UNKNOWN`，不能当成真实 node 0。

### 2.3 版本兼容规则

BuildInfo 同时携带 Admin API 与 Raft wire 的 `major/minor`；Raft 另带 `min_peer_minor`。权威规则由
`rr-core` 的版本化兼容表提供：Admin 按 major + capabilities 协商；同组任意两个 Raft 副本只有在
major 相同，且双方 minor 都满足对方 `min_peer_minor` 时才判定 wire-compatible。App/Rr 版本号和
git commit 只用于展示与审计，不参与字符串比较。Console 显示规则结果和具体不兼容副本对，不自行
声明“可升级”或“升级完成”。

## 3. 建设方式与取舍

### 方案 A：独立 Console BFF + 内嵌静态前端（采用）

`rr-admin console` 启动一个 Rust 聚合进程，后端提供同源 JSON/SSE API 并托管构建后的静态前端。
BFF 统一发现节点、连接 AdminService、抓取 `/metrics`、规范化数据并维护短周期缓存。

优点是一个入口、无浏览器跨域、节点凭证不进入浏览器、集群视角一致，并且多个浏览器共享采集任务。
代价是需要新增 Console BFF 和少量节点描述/历史契约。

### 方案 B：每个节点内嵌一套 UI

部署入口多，单节点实现看似直接，但浏览器需要访问所有节点，凭证和 CORS 复杂；不同节点可能给出
不同拓扑视图，控制台掉线时也无法解释整集群。因此不采用。

### 方案 C：只生成 Grafana/Prometheus 配置

适合自由指标分析，但无法完整覆盖 Raft 副本证据、启动历史、受控操作和产品化诊断流程。Console
会保留“在 Grafana 中查看”链接能力，而不是把它作为唯一界面。

## 4. 总体架构

```text
Browser ──HTTPS──▶ rr-admin console / Console BFF
                      ├─ static App Router export (React + shadcn/ui)
                      ├─ Discovery: trusted seed peers | NodeDirectory/etcd
                      ├─ bounded scheduler + per-node circuit breaker
                      ├─ AdminService clients (TLS/mTLS + admin token)
                      ├─ HTTP /health /ready /metrics scraper
                      ├─ normalizer + freshness/source metadata
                      ├─ in-memory live cache + bounded time-series ring
                      └─ optional external Prometheus for retained history

Rr application node ─ AdminService: status/topology/group/boot/build
                    ─ HTTP observability: health/ready/metrics
                    ─ local boot journal: start/shutdown/version
```

P0 的 Console 不写入 Raft/membership，也不要求自己的数据库；节点目录发布是节点运行时的独立注册
行为。实时聚合保存于内存；长时间性能历史通过可选 `--prometheus-url` 查询。未配置时，趋势图只显示
Console 启动后的留存，并明确标注“Console 内存留存”。节点重启历史来自节点自己的持久 boot
journal，因此不依赖 Console 一直在线。

前端采用 React 19、TypeScript strict、Next.js App Router 静态导出、Tailwind CSS v4、shadcn/ui、
TanStack Table 和 Recharts。构建产物由 Rust BFF 同源托管；不引入独立 Node.js 生产服务。拓扑图采用
可键盘访问的 SVG 图组件，图只是关系的可视化，表格始终是等价替代视图。

## 5. 启动、发现与兼容流程

### 5.1 命令形态

```bash
# 种子模式：地址明确指向 AdminService，不做端口推导
rr-admin console \
  --listen 127.0.0.1:8080 \
  --peers 1@10.0.0.11:52001,2@10.0.0.12:52001,3@10.0.0.13:52001 \
  --token-env RR_ADMIN_TOKEN

# discovery 模式：复用集群配置面
rr-admin console \
  --listen 127.0.0.1:8080 \
  --discovery etcd://10.0.0.2:2379,10.0.0.3:2379 \
  --cluster-name prod-rr \
  --token-env RR_ADMIN_TOKEN

# 可选长期历史：必须绑定唯一集群标签，Prometheus target 还必须带 node_id
rr-admin console \
  --peers ... \
  --prometheus-url http://127.0.0.1:9090 \
  --prometheus-cluster cluster_id=prod-rr

# 对外监听时：P0 使用单一 viewer 凭证，环境变量提供，不进入 argv
rr-admin console --listen 0.0.0.0:8443 --peers ... \
  --console-token-env RR_CONSOLE_TOKEN --tls-cert ... --tls-key ...
```

Console 的 `--peers` 是“AdminService 种子”，与 `rr-server serve --peers` 的本地 bootstrap 路由表参数
是不同语义，文档和 help 必须明确区分。`--discovery` 与 `--peers` 可二选一；同时提供时 discovery
为 roster 权威，peers 仅作连通性种子，UI 显示这一优先级。`--cluster-name` 在 etcd 模式必填。

TLS、mTLS、etcd 凭证继续通过现有配置/环境变量惯例传入；token 不允许直接放在 argv。非 loopback
监听必须启用 Console 自身认证和 HTTPS。`--allow-insecure` 只允许 loopback，非 loopback 与其组合
必须拒绝启动；P0 不提供远程开发绕过开关。P0 登录采用 `RR_CONSOLE_TOKEN` 的单一 viewer 凭证，
换取短期 HttpOnly session cookie；本地用户体系和 OIDC/SSO 留到后续，不在首版隐式支持。

历史 Prometheus 必须配置精确且唯一的 `--prometheus-cluster label=value` matcher，所有查询都附带
该 matcher 和受控 node/group 过滤。scrape target 必须附加稳定的 `node_id` 标签；缺标签或一对多
映射时历史功能显示 `UNMAPPED`，禁止按 instance 字符串猜 node ID。使用的 recording rules 必须保留
cluster/node 标签，否则 Console 改用原始 histogram/counter；绝不直接复用会跨集群聚合的旧规则。

### 5.2 发现算法

1. 校验监听安全、TLS/token 配置和 discovery 参数，任何一项不完整都 fail fast。
2. 显式 peers 是 operator 对“这些 Admin seed 属于同一集群”的信任断言；seed 的 Admin 地址即使
   节点不支持新描述符也仍然有效，继续提供旧能力。通过该已认证 Admin 通道返回的本机其他端点
   才可继承信任。
3. discovery 模式读取独立 NodeDirectory 的 lease 记录和 ConfigProvider 路由/组信息。节点进程或
   provisioner 负责发布目录；只有目录后端凭证所保护的记录才视为可信，过期 lease 不转发凭证。
4. 并发访问 `GetNodeStatus/GetBuildInfo/GetNodeDescriptor`，校验 node ID、cluster UUID 和目录
   revision。任何转发 Admin token 的新地址都必须来自显式 seed、受信目录或已认证本机描述符，
   且 TLS 身份校验通过；普通 metrics 标签、未认证 HTTP 响应不能提升地址信任。
5. 非 seed 节点只使用描述符中明确的 admin/observability 地址；旧节点缺地址时保留节点但标记
   `ENDPOINTS_UNAVAILABLE`，不猜端口。显式 seed 本身不因缺少新 RPC 而被丢弃。
6. 订阅目录和配置变化，并周期全量对账，处理 lease 过期、watch 断开、compaction、节点加入和
   地址变化。按能力探测建立 feature matrix；旧节点只降级无来源模块。
7. 节点不可达时保留最后一次快照，标记 stale、last successful contact 和错误，不把节点从拓扑删除。
8. 非空 cluster UUID 冲突时隔离来源并停止聚合。全部节点为 Untracked 时，显式 peers 或受信
   discovery namespace 就是 operator 的同群断言，Console 以配置的 cluster name 或 `untracked`
   作为展示身份并持续警告；空与非空 UUID 混合时不得合并，要求先完成集群收养/身份修复。

### 5.3 采集调度

- 节点/组概要默认 5s，资源和 Prometheus 默认 5–15s，boot/build 默认 30s；详细组状态按需采集。
- 所有请求有连接和总超时、有限并发、每节点熔断和指数退避；恢复时逐步回到正常周期。
- BFF 按集群单飞采集，浏览器只读缓存或订阅 SSE；浏览器数量不增加节点 RPC 数。
- 大集群只周期采集概要，副本矩阵按选中的 group 加载；列表和拓扑数据必须分页/限量。
- 指标响应有 body 上限和解析上限；不转发用户提供的任意 PromQL，只允许固定模板和受控参数。

## 6. 统一数据语义

### 6.1 响应元数据

每个 Console API 响应都带统一元数据；单资源错误也返回相同结构：

```text
meta {
  schema_version, correlation_id, cluster_id, generated_at,
  partial, time_window, retention_source,
  sources[{node_id, endpoint_role, observed_at, age_ms, status, error_code}],
  capabilities[{feature, status, reason}]
}
```

页面至少显示 generated_at、最老来源年龄、partial 和 correlation ID。详情面板可查看逐节点来源及
错误。stale 数据保留但使用 stale badge；未知字段显示 `—`，不显示 0。

### 6.2 三层事实模型

- **期望状态**：ConfigProvider 的 roster、partition→group、replicas 和 topology revision。
- **运行时状态**：各节点报告的 leader、term、membership、log/snapshot index 和角色。
- **观测状态**：Console 能否连接节点、最后一次成功采集、指标是否缺失、节点是否 stale。

三组状态并列呈现。例如“配置中有 3 个 voter；Console 可达 2 个；节点 1 与 2 都观察 leader=1”是
一条完整证据，不能只压成绿色“健康”。

跨节点轮询不是原子快照。leader 冲突只在**相同 term**、样本处于同一允许时间窗且超过选举/切换
宽限期后仍报告不同 leader 时成立；不同 term 属于正常演进。membership 只比较携带相同 log ID/
revision 的视图，否则显示 `SYNCING`，不能报冲突。所有冲突告警保留各来源的 observed_at，禁止把
采集时差当成 Raft 脑裂。

### 6.3 健康词表

| 层级 | 状态 | 含义 |
|---|---|---|
| Node | `ONLINE` | Admin 与必要数据源新鲜可用 |
| Node | `DEGRADED` | 进程可达，但 metrics/ready/部分组或资源数据异常 |
| Node | `UNREACHABLE` | Console 当前无法连接；不等于进程已停止 |
| Node | `UNKNOWN` | 从未获得可信状态 |
| Group | `HEALTHY` | 新鲜观察中 leader 一致、voter 可用性和 lag 未越阈值 |
| Group | `DEGRADED` | 有 leader，但存在副本滞后、不可达或指标缺失 |
| Group | `LEADERLESS_OBSERVED` | 所有新鲜可达副本都未观察到 leader |
| Group | `QUORUM_AT_RISK` | Console 可达 voter 不足多数；明确是控制台视角 |
| Group | `CONFLICTING_VIEWS` | 同 term、同时间窗且超过切换宽限期的 leader 证据冲突 |
| Group | `SYNCING` | membership/term 处于不同 revision，暂不能原子比较 |
| Group | `FORMING` / `UNTRACKED` | 来自 formation 的真实状态 |
| Any | `STALE` / `UNSUPPORTED` | 数据过期或节点版本不支持 |

告警阈值必须有默认值、可配置项和来源；页面提供阈值说明，不把经验值当作 Raft 协议保证。

## 7. 信息架构与全局壳

```text
集群 CLUSTER
  总览 /overview
  节点 /nodes
  拓扑 /topology
  Raft 组 /groups
  分区与路由（拓扑页第三 Tab）

可观测 OBSERVE
  性能 /performance
  快照与存储 /snapshots
  生命周期 /lifecycle
  事件与告警 /events

系统 SYSTEM
  数据源 /sources
  操作中心 /actions（P1，未实现时完全隐藏）
```

沿用指定设计系统：232px 固定侧栏、52px sticky 顶栏、单标题铁律、通铺面板、24px 文本留白、
38px 密表行高、480/560px 详情抽屉、Seg 切换状态/模式、下划线 Tab 切换内容 pane。

顶栏右侧依次是全局搜索、告警数、环境 badge、Console 数据面状态、刷新周期和最后更新时间。描述条
展示当前集群、时间窗口和统一新鲜度。`⌘K` 可搜索节点、组、partition、boot ID 和 git commit。

所有页面保留原有筛选、排序、密度和列显隐；URL 保存时间窗口与筛选，方便事故期间分享同一视图。
自动刷新可暂停；切到后台标签页时降低刷新频率，回到前台立即对账。

## 8. 页面规格

### 8.1 集群总览 `/overview`

**目的**：10 秒内确定是否需要介入，并一键进入故障对象。

- 顶部异常条只显示最高优先级问题：leaderless、quorum risk、版本不兼容、快照失败、资源耗尽或
  数据源大面积 stale；无异常时不展示虚假“全部绝对正常”，而显示观测覆盖率。
- 概要卡：在线/总节点、有 leader/总组、quorum risk 组、写入 TPS、写 P99、活跃告警。卡片显示
  分母、来源新鲜度和点击下钻。
- 主区左侧为压缩 Raft 拓扑，右侧为实时事件；下方为吞吐/P99 趋势、最滞后副本 Top 10、资源最热
  节点 Top 10、最近重启/版本变化。
- 默认实时窗口 15m；没有历史源时只绘制已有留存，不回填平线。

### 8.2 节点 `/nodes`

**列表列**：Node、地址/位置、状态、Leader 组数/本地组数、CPU（使用率 + 核数/限制）、内存
（使用量 + 总量/限制）、网络 RX/TX、磁盘、版本、uptime、最后联系。

支持状态、zone、版本、角色和文本过滤；数字右对齐等宽字体；不可获得的数据显示 `—` 和原因。
点击行打开 560px 抽屉，Tab 为：

1. **概要**：ready/liveness、node ID、地址角色、环境标签、配置指纹、boot ID、启动时间。
2. **资源**：CPU/内存/网络/磁盘趋势和 process/cgroup/host 口径；显示限值与采样来源。
3. **Raft**：本节点所有组的角色、term、last/commit/applied、lag、leader 和状态。
4. **配置**：仅 allowlist 的非敏感配置，以及与文档化 built-in default 和其他节点配置的 diff；不虚构
   “集群默认配置”权威，也不任选 seed 当基准。token、密钥、证书私钥和凭证永不输出。
5. **重启与版本**：当前及历史 boot record、shutdown 状态、应用/Rr/git 版本。

### 8.3 拓扑 `/topology`

三个内容 Tab，不用颜色表达类别：

- **节点拓扑**：按 region/zone/rack 分组；节点显示在线状态、资源热点和版本。边只表示已观测的
  Raft 通信关系，标签为“近 5m 有 AppendEntries”，不是物理网络断言。多组共享边聚合为组计数。
- **Raft 拓扑**：每组以 leader→voter/follower/learner 的关系呈现；箭头是复制方向，线宽不映射流量，
  选中边显示组、RTT、lag 和采样时间。大量组先进入虚拟化组列表，再绘制选中组，避免“毛线团”。
- **分区与路由**：partition→group→replicas 表，标出期望路由与实际 leader、配置 revision、
  route not found 和 stale route；P0 不提供迁移按钮。

图形支持缩放、适配、拖动、键盘选择和“切换为表格”；导出 SVG/PNG 只作为后续增强，不阻塞 P0。

### 8.4 Raft 组 `/groups`

**列表列**：Group、formation、观测健康、Leader、Term、Voters 可达/总数、Learners、最大 lag entries、
最大 append RTT P99、last log、snapshot index、TPS、来源数。

组详情是核心事故页：

1. **状态证据条**：配置 membership、当前 leader 观察、term、quorum 视角、最后一致采样时间。
2. **副本矩阵**：Node、ServerState、Leader/Follower/Learner、Term、Last/Commit/Applied/Matched、
   Lag entries、Heartbeat age、Append RTT（heartbeat/append 分开）、Snapshot、来源及新鲜度。
3. **复制趋势**：每组 TPS、各 follower lag 与 RTT；leader 切换处画事件标记，不能跨换主硬连线。
4. **成员与选举时间线**：membership revision、leader/term 变化和 learner promotion；轮询推导的事件
   标记 `observed`，只有节点明确事件才标记精确发生时间。
5. **关联视图**：partition、节点、快照和性能下钻。

leader 的 matched/lag 只能来自 leader；其他节点未报告时显示“非 Leader 来源，无此数据”。

### 8.5 性能 `/performance`

- 顶部筛选：实时/历史、15m/1h/6h/24h、group/node/op/source；历史范围超过本地留存时要求
  Prometheus，或明确截断并标识。
- **吞吐**：write/read/linearizable read TPS、精确 applied entries TPS、ingestor commands TPS、
  redirect/error/idempotency rejection 比率。
- **延迟**：各 op P50/P95/P99；P99 从聚合 histogram bucket 计算，禁止平均节点分位数。
- **写延迟分析**：P0 分开展示 server total、proposal、apply 的均值和分位数，不做均值堆叠或差值。
  当前 server histogram 没有 outcome，proposal/apply 也缺少贯穿请求路径的 cohort/source；失败、
  redirect 和 batch 会使样本集合不同，同 group/leader/window 仍不足以安全相减。只有新增对齐的
  outcome/cohort 标签，并证明所选 server/proposal/apply 样本计数一致后，才开放均值构成；否则显示
  `UNALIGNED_COHORTS`。分位数永远不做堆叠减法。
- **复制**：AppendEntries heartbeat/append RTT、batch entries、replication lag、channel reconnects。
- **热点**：按 group/node 的延迟和吞吐表、橙色强度热力图；支持点击下钻并保留时间窗口。
- **背压与错误**：output pending、deferred pending/overflow、idempotency、storage/WAL、etcd stale、
  snapshot build failures 等真实指标。

### 8.6 快照与存储 `/snapshots`

- 概要：有快照组数、构建失败、安装次数、传输字节率、P95/P99 传输时间、磁盘风险节点。
- 密表按 Group × Node 展示 last snapshot index/term、last log 与 snapshot 距离、安装次数、发送/
  接收字节、最近传输时间、磁盘使用、来源和年龄。当前 build failure 没有 group 标签，因此只在
  节点级汇总显示，Group × Node 的失败单元格为 `UNSUPPORTED`，直到新增带组标签的事件。
- 组详情显示 send/recv 指标和由计数器推导的 `observed` 安装/追赶事件；P0 不声称重建精确
  snapshot→catch-up→apply 时间线。只有新增真实任务事件后才显示“构建中/传输中/进度百分比”；
  现有 bool 占位不出现在 UI。
- 存储面板展示 RocksDB/WAL/磁盘相关真实指标；没有目录级容量时只显示节点总量，不伪造每组占用。
- P1 可在此接入 TriggerSnapshot 和 OnlineBackup，但必须受权限、确认、审计和下载授权约束。

### 8.7 生命周期 `/lifecycle`

- **当前状态**：节点 uptime、process start、boot ID、ready phase、当前版本和 config hash。
- **重启时间线**：按节点堆叠 start/shutdown/unreachable/observed gap；区分优雅关闭、异常或不确定
  结束、Console 无法观测。不能仅凭掉线判定 crash。
- **重启历史表**：Node、Boot ID、Started、Previous shutdown、Previous version、Current version、
  Duration、记录来源。内存存储/无 journal dir 的节点只展示当前 boot 和 `VOLATILE`，不承诺历史。
- **版本矩阵**：Node × App version × Rr version × git commit × protocol × config hash，突出混合版本和
  不兼容组合；dirty build 永远显示警示。
- **版本历史**：每次 boot 的应用和框架版本、commit、开始/结束时间；支持按 commit 搜索和复制。
- 滚动升级期间显示“版本分布”，但不自动推断升级完成，也不承诺跨版本兼容，除非协议规则明确通过。

### 8.8 事件与告警 `/events`

事件类型包括节点不可达、ready 失败、leaderless/conflict、quorum risk、lag/RTT 越阈值、term/leader
变化、membership/formation 变化、快照失败、资源压力、版本变化、重启和采集能力缺失。

每条事件包含严重度、对象、首次/最近发生、持续时长、当前状态、证据来源、关联指标和 runbook 链接。
告警是 Console 基于阈值生成的观测事件，不冒充节点已持久化的事实。P0 提供页内确认/静默到本地
偏好，不承诺邮件、Slack 或 PagerDuty 投递。

### 8.9 数据源 `/sources`

这是排查“为什么页面不对”的一等页面：

- 当前启动方式、discovery/cluster 名称、topology revision、Console 版本和只读模式。
- 每节点 admin/observability 地址、TLS/mTLS 状态、能力矩阵、最后成功/失败、延迟、熔断状态和
  脱敏错误。
- Prometheus 连接、历史留存来源和查询错误。
- 缺失指标/旧版本节点的具体影响与升级建议。
- 提供“立即对账”只读刷新，不展示 token、环境变量值、私钥或完整敏感配置。

## 9. 视觉系统

保留参考规范的“精密仪器”密度、磨砂深色、凹陷输入、浮起选中、单标题、动作回声和危险门槛。
概念改为 **「共识控制平面」**：冷静、可验证、数字和证据优先，不做科幻大屏。

```css
:root {
  --bg-0: #0b0e13;
  --bg-1: rgb(255 255 255 / 0.03);
  --bg-2: #171b22;
  --bg-3: rgb(255 255 255 / 0.05);
  --line-1: rgb(255 255 255 / 0.06);
  --line-2: rgb(255 255 255 / 0.10);
  --tx-1: #f4f4f5; --tx-2: #d4d4d8; --tx-3: #a1a1aa;
  --tx-4: #71717a; --tx-5: #52525b;
  --acc: #f97316;
  --acc-strong: #ea580c;
  --acc-tint: rgb(249 115 22 / 0.12);
  --acc-ring: rgb(249 115 22 / 0.35);
  --ok: #34d399; --ok-tint: rgb(52 211 153 / 0.12);
  --bad: #fb7185; --bad-tint: rgb(251 113 133 / 0.12);
  --sys: #a78bfa; --sys-tint: rgb(167 139 250 / 0.12);
}
```

- Rust-orange 只承担主行动、当前选择、待决/警示；emerald/rose/violet/zinc 语义保持参考规范。
- body 光晕同步改为低透明度 Rust-orange；不使用多彩渐变、霓虹拓扑或装饰性粒子。
- 字体保持 Inter + Noto Sans SC + JetBrains Mono，实现时使用项目自托管资源。
- 多系列图表使用橙色突出当前对象、灰阶及线型区分比较对象，配合直接标注；不给节点/组分配彩虹色。
- 状态永远有文字或图标；leader 用符号和文字，不只靠橙色；危险/异常不只靠红色。
- 新增领域组件：`HealthBadge`、`FreshnessBadge`、`SourceEvidence`、`ReplicaMatrix`、
  `ConsensusGraph`、`MetricChart`、`BootTimeline`、`VersionMatrix`、`CapabilityNotice`。

## 10. Console API 轮廓

P0 API 均为同源只读，路径版本化为 `/api/v1/console`：

```text
GET /summary?window=15m
GET /nodes?cursor=&page_size=&filter=
GET /nodes/{nodeId}
GET /nodes/{nodeId}/boots?cursor=&page_size=
GET /topology?view=nodes|raft|routes&group_id=
GET /groups?cursor=&page_size=&filter=
GET /groups/{groupId}
GET /groups/{groupId}/replicas
GET /metrics/performance?window=&group_id=&node_id=&op=
GET /snapshots?cursor=&group_id=&node_id=
GET /lifecycle?view=current|restarts|versions&cursor=&page_size=
GET /events?cursor=&page_size=&severity=&state=
GET /sources
POST /sources/reconcile
GET /stream                 # SSE：概要、事件和新鲜度；断线后 GET 全量对账
```

列表统一 cursor 分页，默认 50、最大 200；boot journal 每节点最多 100 条，周期概要只读最新一条，
历史列表按需查询。时间统一 RFC 3339 UTC，字节/秒/条目使用明确单位字段或稳定字段名。指标接口按
固定查询模板输出 series + timestamp + value + source，不开放任意查询语言。所有响应带第 6.1 节元
数据；成功、部分成功、旧节点不支持、上游超时和完全不可达都有契约测试。

## 11. 安全与操作边界

- Admin token 只存在 BFF 进程内；浏览器不直接持有节点 AdminService 凭证。
- Console 默认 loopback；对外监听强制 HTTPS 和认证，`--allow-insecure` 仅限 loopback。P0 采用
  `RR_CONSOLE_TOKEN` 单 viewer 凭证登录，BFF 换取短期 Secure/HttpOnly/SameSite=Strict session
  cookie；session 只存内存并有过期/撤销。POST reconcile 使用 CSRF token 和 Origin 校验。
- bearer token 不写进 localStorage、URL、静态 HTML 或日志；认证失败不泄露凭证是否接近正确。
- P0 所有用户只有 viewer 能力；P1 再引入 viewer/operator/admin、SSO 和写操作授权。
- `/metrics` 当前可能未认证，BFF 不应把原始指标或内部地址无保护地公开到公网；生产部署仍需网络
  策略、TLS 或可信内网边界。
- 配置输出采用字段 allowlist，而非仅对已知 secret 做黑名单掩码。
- P1 写操作必须带原因、对象确认、权限检查、审计和持久 action ID；UNKNOWN 时先查询，禁止盲目重试。
- P1 不提供“远程重启进程”按钮，除非存在独立、受认证的 supervisor；DrainNode 不等于进程重启。
- 所有危险操作后果摘要必须描述 quorum、副本和可用性影响，force 不表示绕过 Raft 协议。

## 12. 性能、规模与可用性预算

- 初始目标：50 节点、500 组的概要视图；节点和组表格虚拟化/分页，拓扑只绘制受控规模或选中组。
- 缓存命中时，概要/列表 API 在目标规模下的服务端 P95 ≤ 300ms；按需组详情在上游正常时 P95
  ≤ 2s，慢节点按自己的超时降级，不拖住其他来源。
- 默认周期内，每节点常规 Admin 请求折算 ≤ 1 RPC/s，Prometheus scrape ≤ 0.2 次/s；按需详情另计，
  20 个并发浏览器不得增加该上游采集频率。
- 单次 metrics body ≤ 8MiB、输入 series ≤ 100,000/节点；规范化活跃 series ≤ 20,000/集群，内存
  环形缓冲 ≤ 2,000,000 点，超限拒绝/降采样并产生明确事件，不允许无界增长。
- 50 节点/500 组目标下 BFF 常驻内存预算 ≤ 512MiB，浏览器详细节点内存预算 ≤ 250MiB；超限即
  降低留存/图形点数，而不是牺牲节点服务。
- BFF 不为每个浏览器创建采集循环；所有上游调用有超时、并发、取消和每节点熔断。概要刷新不执行
  N×G 全量副本深查，完整副本矩阵按组加载，大集群采用分页或服务端聚合 RPC。
- 浏览器首屏只加载概要；性能图、拓扑图和详情按需加载。SSE 只推变化/概要，断线指数退避并全量
  对账；事件数量有界，旧数据按环形容量淘汰。
- Console 故障不影响 Rr 节点；Console 冷启动无缓存时展示分源骨架和部分结果。外部 Prometheus
  不可用时回退到实时缓存并明确历史缺失，不伪造历史曲线。

## 13. 空态、错误与降级

- 初次发现中：显示分阶段进度和已成功节点，不让整个页面一直 skeleton。
- 部分节点失败：保留其他节点数据，顶部显示 `PARTIAL` 和失败数量，可下钻到 Sources。
- 旧节点：页面级功能保留，缺失卡片显示 `UNSUPPORTED`、需要的最低能力和关联节点。
- 无快照：显示“尚未观察到快照”及 last log，不显示进度 0%。
- 无重启历史：区分“节点版本不支持”“本地 journal 无记录”“节点不可达”。
- 无告警：显示当前观测覆盖率；若数据源不完整，不能显示“集群完全健康”。
- 图组件失败或数据过多：自动切换等价表格，不白屏。
- 所有错误包含 correlation ID、来源和可执行恢复建议；敏感内部错误只留在服务端日志。

## 14. 响应式与无障碍

- 桌面优先，1440×900 为主验收视口；1280px 下不出现页面级横向滚动，密表自身可横向滚动。
- 小屏侧栏折叠为抽屉，概要卡改为单列，拓扑默认表格，详情抽屉全宽；不承诺手机端复杂图编辑。
- 所有交互可键盘完成，focus-visible 使用 acc-ring；Dialog/Drawer 焦点圈禁、Esc 关闭并恢复焦点。
- 图表提供摘要和数据表；拓扑提供等价表格；排序、筛选、freshness、partial 状态读屏可达。
- 正文对比度至少 4.5:1，辅助信息至少 3:1；橙色按钮文字使用深色并验证对比度。
- 动效保持 120–240ms，自动刷新只使用细进度提示；完整支持 `prefers-reduced-motion`。

## 15. 分阶段交付

### P0：自包含只读控制台

- Console BFF、静态前端、seed/discovery、节点描述符和能力协商。
- 总览、节点、拓扑、组、性能、快照、生命周期、事件和数据源九个视图。
- 节点资源/构建/boot journal 契约、真实数据来源与降级态。
- 实时内存趋势；可选 Prometheus 历史；无写操作。

### P1：受控运维

- TriggerSnapshot、TransferLeader、DrainNode、OnlineBackup 和已有安全 membership 流程。
- 操作中心、RBAC、持久 action/audit 记录、UNKNOWN 恢复和审批策略。
- 仅在独立 supervisor 存在时设计进程重启；不绕过现有 decommission/quorum guard。

### P2：深度诊断与规模增强

- 外部日志/trace 链接、告警通知、长期 Console 事件存储、多集群和更大规模聚合。
- 这些是增强，不在首版引入自建 TSDB、日志平台或新的分布式控制面。

## 16. 验收标准

### 契约与正确性

- 仅用两个 Admin seed 地址即可发现并展示全部已注册节点，不发生端口猜测；旧 seed 不支持新
  RPC 时原有能力仍可用。
- 仅用 etcd discovery + cluster name，可通过新 NodeDirectory 完成同样流程；覆盖 lease 过期、
  provisioned descriptor、watch 断开后全量对账，以及 managed formation 节点目录发布。
- 非空 UUID 冲突、Untracked 同群断言、空/非空 UUID 混合、节点 ID 冲突、不可信地址替换和
  stale cache 都有明确测试，不合并错误集群，不向未受信地址转发 token。
- 节点掉线、leader 切换、副本滞后、无 leader、部分指标缺失和恢复均按第 6 节语义呈现；非原子
  轮询下的正常 term/membership 变化不得误报 CONFLICTING_VIEWS。
- 重启历史能区分优雅关闭与异常/不确定结束；版本历史同时包含 App/Rr/git commit；mem/无目录
  节点正确显示 VOLATILE，历史接口验证分页与 100 条保留上限。
- P99 使用 histogram 聚合；P0 不做延迟差值，未来构成图必须校验 outcome/cohort 和样本计数对齐。
  lag 缺失误判、P99 平均、snapshot stub 和 UNKNOWN 重试有回归测试。
- 两套同名 group 的集群共享 Prometheus 时，cluster matcher 保证历史不串群；缺失/冲突 node_id
  标签返回 UNMAPPED，不使用 instance 猜测。

### UI 与端到端

- 在真实 3 节点集群上验证发现、节点资源、拓扑、组副本、性能、快照和生命周期完整流程。
- 杀掉一个节点、制造 leader 变化并恢复，Console 显示部分失败、事件和恢复，不刷新整页白屏。
- 1440×900 与 1280×800 下无页面横向溢出；键盘、读屏、reduced motion 和图表替代表格通过检查。
- Playwright 覆盖概要→节点→组→快照→生命周期下钻、Sources 错误下钻、暂停刷新和 SSE 重连。
- console 无 error；无凭证进入 URL、HTML、浏览器持久存储或前端日志。非 loopback 无认证/TLS
  和 `--allow-insecure` 公网绑定均拒绝启动；session 过期/撤销、CSRF 和 Origin 检查通过测试。

### 性能与故障

- 以 50 个真实模拟上游进程、每节点 500 组概要及代表性 metrics 响应运行实际 BFF scheduler/
  parser，验证第 12 节 API 延迟、上游 QPS、BFF/浏览器内存、series/样本上限；不能只测静态 JSON
  前端渲染，也不以全量 N×G 图渲染作为验收方式。
- 20 个并发浏览器打开时节点采集 QPS 保持第 12 节预算，不随浏览器数线性增加。
- 对慢节点、超大 metrics 响应、Prometheus 超时和 discovery 中断验证超时、取消、熔断和降级。
- Console 停止和重启不影响集群，节点继续服务；未配置历史源时明确只保留本次进程数据。

## 17. 设计决策与留给实施者的事项

本设计已决定：采用独立聚合 BFF、静态前端内嵌、P0 只读、Rust-orange 品牌色、独立 NodeDirectory、
节点本地 boot journal、mem 节点默认 VOLATILE、显式 seed/discovery namespace 对 Untracked 的同群
断言、单 viewer token 登录、公网不允许 insecure、无端口推导、无任意 PromQL，以及外部 Prometheus
强制 cluster/node 标签隔离。无外部历史源时只做透明留存。

实施者在不改变世界语义的前提下可决定：Rust HTTP 静态托管的具体 crate、资源采样在不同 OS/
容器内的实现、图布局算法、前端构建目录和具体组件拆分。新增依赖必须在实现计划中列出并审查，
不能因为常见就假定项目已经具备。

后续 mockup 至少覆盖：总览、节点列表+抽屉、Raft 组详情、拓扑、性能、快照、生命周期和数据源
降级态；mockup 数据必须标记为仿真，不得反过来成为无来源产品指标的依据。
