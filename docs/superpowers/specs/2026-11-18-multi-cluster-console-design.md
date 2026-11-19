# Multi-cluster Rr Console Design

日期：2026-11-18
状态：已实现并完成本地验收（2026-11-19）；Tasks 1–7 独立复审通过，Task 8 正式/专项复审与 fix round 1 已完成

## 1. 意图

将 `rr-admin console` 从启动时绑定一个集群的只读观察器，改造成类似数据库客户端的多集群客户端：
Console 可以独立启动，用户保存多个集群连接，连接后查看目标集群，并在页面内切换集群。

首版继续保持现有只读观察能力，不新增转移 leader、修改 membership 等集群写操作；连接管理本身的
CRUD、测试、连接和断开属于本设计范围。

## 2. 目标与非目标

### 目标

- `rr-admin console --listen 127.0.0.1:8080` 无需 peers/discovery 即可启动。
- 保存、编辑、测试、连接、断开和删除多个集群连接。
- 每个已连接集群拥有独立的上游客户端、发现、调度、缓存、Prometheus 配置和生命周期。
- 页面通过明确的 connection ID 查看集群；刷新、bookmark 和多标签页不会串集群。
- 非敏感连接配置可持久化；秘密不进入浏览器持久存储、URL、日志或 API 响应。
- 保留现有 Rust BFF 同源托管 Next.js 静态导出的生产架构。
- 保留旧启动参数作为兼容入口。

### 非目标

- 不引入 Electron/Tauri 桌面壳或独立 Node.js 生产服务。
- 不扩展为多租户、多用户权限平台；继续采用当前单 operator viewer 认证模型。
- 不在本轮新增 Raft/membership 写操作。
- 不让浏览器直接访问 AdminService gRPC，也不在浏览器保存 Admin token。

### 反目标

- 不使用后端全局“当前集群”并在切换时替换唯一 Scheduler。
- 不要求每个集群部署一个 Console 进程。
- 不在切换后短暂展示上一个集群的数据。
- 不让所有已保存连接在后台持续轮询。

## 3. 已考虑的方案

### A. 全局可变当前集群

后端只有一个 Scheduler，切换时替换其目标。改动较小，但多标签页、多浏览器、SSE、在途请求和缓存均可能
串集群，不采用。

### B. 每集群一个 Console 进程

前端保存多个 BFF 地址。后端改造较少，但端口、部署、认证和跨域复杂，不是单一客户端体验，不采用。

### C. 单进程多连接（采用）

一个 `ConnectionManager` 管理持久化 profile 和按需创建的 runtime。每个 runtime 包含现有单集群采集栈；
所有集群数据 API 显式携带 connection ID。该方案复用现有实现，同时提供严格隔离和多标签页能力。

## 4. 领域模型

### ConnectionProfile

持久化的用户配置，至少包含：

- 稳定随机 `id`、用户可读 `name`、可选环境标签。
- Admin seeds 与/或 discovery 配置，以及 discovery cluster namespace。
- Admin TLS 文件路径和秘密引用：可选 CA、可选且成对的 cert/key、可选 domain，支持 CA-only TLS 与 mTLS；
  旧 CLI 的 all-or-nothing 参数继续作为兼容入口。
- Discovery TLS、认证和秘密引用。
- 可选 Prometheus 地址与精确 cluster matcher、轮询间隔。
- 创建/更新时间，以及首次连接后观察到的可选集群身份。

API DTO 必须脱敏，只暴露 `credentials_configured` 等布尔状态，不返回 token、密码或私钥内容。

### ConnectionRuntime

连接后按需创建，包含：

- 该连接的 `TonicUpstream`、`TrustedDiscovery`、`Scheduler`、`ConsoleCache`。
- 该连接的 Prometheus history 配置和 summary projection。
- 后台任务 shutdown 句柄、连接状态、最后成功时间与脱敏错误。

runtime 不持久化。每个 runtime 独占缓存后，现有缓存键无需额外加入 connection ID。

### ActiveConnection

浏览器当前页面正在查看的 connection ID。它不是后端全局状态；URL 是唯一权威来源。localStorage 可保存最近
连接 ID 用于连接管理页高亮或默认选中，但不得自动连接或覆盖 URL。

`/`、无 connection 的集群页面、recent ID 已删除或已断开时都进入 `/connections/`。选择 DISCONNECTED
profile 只选中它，不自动 Connect。多标签页的 recent ID 可以共享，但 storage 事件不得改变其他标签页当前
URL 或连接。

ConnectionManager 使用 `ManagedConnection { profile, runtime, status, last_error, operation_generation }`
保存完整状态，而不是把状态只放在可缺失的 runtime 内。按 ID 串行化 connect/disconnect/edit/delete，不在全局
map 锁内 await；generation 或取消令牌使迟到的初始化结果失效。活跃上限默认 8，包含 CONNECTING，重复
connect/disconnect 幂等。

## 5. 启动与兼容

无集群参数启动时只初始化监听、安全、静态页面、viewer 认证、profile store 和空 ConnectionManager，不连接
任何 AdminService、etcd 或 Prometheus。

现有 `--peers`、`--discovery`、全局 Admin TLS/token 和 Prometheus 参数保留。提供集群来源时，将其转换为
一个**进程内临时启动连接**，不自动写入 profile store，也不承诺 bookmark 跨重启恢复。新 API 可对它执行管理
操作；旧无作用域 API 永远固定其 ID，断开或删除后返回明确错误，绝不选择其他连接。新 UI 始终使用连接作用域
API，无启动连接时旧 API 返回 `CONNECTION_REQUIRED`。

无集群来源但提供 Prometheus/Admin TLS/token 等启动参数时必须拒绝启动，不能静默忽略。兼容路由覆盖现有完整
清单，包括 node 详情/boots、group 详情/replicas、snapshots、lifecycle、events、sources/reconcile 和额外
`/stream` 别名。

## 6. 连接生命周期

- **Test candidate**：验证候选配置的地址、TLS、认证和基础 RPC；设置独立总 deadline 和并发上限，不创建
  Scheduler、Prometheus history、watch 或长期任务，不保存配置。
- **Save**：原子持久化非敏感 profile，不自动轮询。
- **Connect**：懒创建并启动 runtime；重复连接应幂等，不创建重复任务。
- **Switch**：仅改变前端 connection ID；旧 runtime 可保持，便于快速切回。
- **Disconnect**：先原子移除 runtime 并拒绝新请求，再广播 runtime shutdown、结束 SSE、取消并等待 scheduler
  及所有派生任务，最后释放 runtime；保留 profile，重复断开幂等。已捕获 Arc 的普通在途 HTTP 请求允许完成，
  但不得启动新的后台采集。
- **Delete**：先 disconnect，再删除 profile 和秘密引用。
- **Edit**：要求目标处于断开状态，避免运行配置与持久配置不一致。

ConnectionManager 需要限制同时活跃 runtime 数，防止无限后台采集。首版不引入复杂空闲回收；只有显式连接
的 runtime 采集，断开、删除和进程退出必须停止任务。

连接状态与集群健康分开：连接可以是 CONNECTING/CONNECTED/DEGRADED/UNREACHABLE/DISCONNECTED，集群节点和
Raft 组继续使用现有健康词表。

## 7. API 契约

连接管理：

```text
GET    /api/v1/console/connections
POST   /api/v1/console/connections
POST   /api/v1/console/connections/test
GET    /api/v1/console/connections/{id}
PATCH  /api/v1/console/connections/{id}
DELETE /api/v1/console/connections/{id}
POST   /api/v1/console/connections/{id}/test
POST   /api/v1/console/connections/{id}/connect
POST   /api/v1/console/connections/{id}/disconnect
```

集群资源改为：

```text
/api/v1/console/connections/{id}/summary
/api/v1/console/connections/{id}/nodes
/api/v1/console/connections/{id}/groups
/api/v1/console/connections/{id}/topology
/api/v1/console/connections/{id}/metrics/performance
/api/v1/console/connections/{id}/stream
```

其余现有 nodes/groups/snapshots/lifecycle/events/sources 资源采用同样作用域。`AppState` 持有
ConnectionManager，handler 从路径解析 ID 并获取对应 runtime。不存在、未连接和连接失败必须返回明确错误，
不得回退到默认集群。约定 `404 CONNECTION_NOT_FOUND`、`409 CONNECTION_NOT_CONNECTED/CONNECTION_ACTIVE`、
`400 CONNECTION_REQUIRED`、`429 ACTIVE_CONNECTION_LIMIT` 和 `409 CREDENTIALS_REQUIRED`。

无 runtime 的认证、连接管理和统一错误使用 neutral meta：`cluster_id: null`、`retention_source: NONE`、空
sources/capabilities，并保留新 correlation ID 和时间；`cluster_id` 契约改为 nullable。能取得 runtime 的
数据错误继续使用该 runtime 的 cache meta。

viewer 登录继续使用现有 `/auth/*`、HttpOnly session 和 CSRF。连接 CRUD、connect/disconnect 和 reconcile
等 POST/PATCH/DELETE 均使用同源 Origin 与 CSRF 防护。秘密值不能写入 access log、错误响应或 OpenAPI 示例。

## 8. 配置与秘密持久化

非敏感 profile 保存在运行 Console 的机器用户配置目录，使用原子替换和 `0600` 权限；提供 `--state-dir`
用于测试与部署覆盖。具体目录选择复用 Rust 平台配置目录惯例，不将文件放在仓库中。

Admin token 和 etcd 密码默认仅保留在进程内存。用户选择“记住凭证”时使用操作系统钥匙串，profile 只存秘密
引用；不支持钥匙串时保持会话内凭证并在界面明确说明。TLS CA、证书和私钥以 Console 主机上的文件路径保存，
不上传或复制私钥内容到 profile。

秘密字段均为 write-only；PATCH 中“省略=保留、null=清除、非空值=替换”。会话秘密由 manager 的内存 store
持有，断开后仍可用于重连，edit/delete/进程退出时清除。`credentials_configured` 表示当前进程能够取得可用
秘密；列表还暴露 keychain 可用能力和凭证来源 memory/keychain。进程重启后所有 runtime 默认 DISCONNECTED，
内存凭证要求重新输入。

隔离测试和没有 OS keychain 的 headless 部署可显式传入进程级 `--keychain-disable`。该模式完全不调用
keychain（包括 delete 清理），所有秘密仅保留在内存，API 报告 `keychain_available=false`，UI 禁用 Remember。
生产默认不传此参数，保持 keychain-enabled；已有 keychain-only 凭证在该模式下需重新输入。

profile 文件包含格式版本；损坏或部分写入必须报错，不得当成空列表覆盖。删除 profile 时删除其 keychain 项；
文件与钥匙串不假装具备原子事务，失败返回明确错误。Prometheus URL 拒绝 userinfo/query/fragment。所有
candidate/connect/discovery 错误使用公开 code 与固定安全 message，底层错误只留内部；含秘密的配置类型不得
派生未脱敏 Debug。

## 9. 前端体验

### 连接首页

无 URL connection 时进入连接管理页，而不是直接打开 Overview。静态路由使用 `/connections/` 和
`/connections/new/`；编辑使用管理页 dialog 或 `?edit=<id>`，不使用需要预生成参数的动态路由。这些页面加入
登录回跳白名单，默认登录 fallback 改为 `/connections/`。

页面提供 New Connection，并列出名称、环境、连接状态、地址摘要、持久化的可选 last_connected_at 和脱敏错误。
节点摘要只随活跃 runtime 的列表 DTO 返回，断开显示 `—`，不为 saved profiles 发 N 个 summary 请求。每项支持
Connect、Disconnect、Edit、Test、Delete；Connected 时 Edit 禁用并提示先 Disconnect。

### 新建与编辑

表单分组为基本信息、Admin seeds 或 etcd discovery、AdminService 安全、Discovery 安全和高级设置。提供
Test Connection、Save、Save & Connect。Test 的失败展示可操作的脱敏诊断；失败后仍允许保存配置。Save &
Connect 失败时保留已保存 profile。seeds 与 discovery 可独立填写并同时存在，但至少提供一种来源。前端 API
helper 强制接收 connection ID，并统一处理嵌套 DTO、CSRF、401 回跳和 DELETE 204。

### 集群页面与切换

集群页面使用：

```text
/overview/?connection=<id>
/groups/?connection=<id>
/nodes/?connection=<id>
```

Shell 顶部提供明确的当前连接选择器和状态。切换保持当前主页面，但只保留 `window` 及内存中的
pause/refresh interval，清除 node/group 选择、tab、搜索、分页 cursor 和旧数据。普通导航、筛选和登录回跳
必须 round-trip connection；切换连接使用另一套白名单 URL 构造，不能复制整个 search。

切换必须关闭旧 SSE、取消 reconnect timer、Abort 旧请求和 resync/详情请求，并忽略旧 generation 的回调。
ResourceState 和共享 summary 记录所属 connection，render 只接受匹配当前 ID 的数据，避免 effect cleanup 前
短暂显示旧集群；同一连接内仍保留“错误时展示最后成功数据”的现有行为。不要 key 整个 RefreshProvider，以免
重置 pause/interval。缺失、未知、断开或删除的 ID 不挂载数据请求和 SSE；NOT_FOUND/NOT_CONNECTED 停止轮询与
重连，临时 UNREACHABLE 才继续重试。

所有导航 Link、全局搜索、登录回跳和 query-state 序列化必须保留 connection 参数。事件 acknowledge/silence
等现有本地偏好按 connection ID 分区，避免相同 event ID 跨集群共享；这与 recent ID 规则不冲突。

## 10. 后端改造边界

- `ConsoleArgs` 取消 peers/discovery 必填约束，增加状态目录配置。
- 新增 profile store、秘密引用和 ConnectionManager；不在全局复用 Admin TLS/token。
- 将 `connect_discovery` 和 scheduler 构建改为按 profile 调用。
- 将 `AppState.scheduler` 与全局 `summary_projection` 从单例状态移除。
- 现有 `Scheduler`、`ConsoleCache`、`TonicUpstream` 和投影逻辑尽量保持为单 runtime 内实现。
- 错误中间件不能再依赖唯一 scheduler/cache 生成 meta；控制面错误使用连接无关的脱敏 meta。

## 11. 前端改造边界

- 新增连接类型、连接 API、ConnectionContext 和连接管理页面/表单。
- `buildConsoleUrl`、资源 hooks、SSE 和 reconcile 全部使用当前 connection ID。
- ConsoleShell 增加连接选择、状态和连接管理入口。
- 路由 query 状态、登录回跳、内部链接统一保留 connection。
- 保留现有静态导出，不使用需要预生成动态参数的动态 App Router 路径。

## 12. 验收证据

### 后端与契约

- 无 peers/discovery 启动成功；旧启动参数保持可用。
- CRUD、原子持久化、重启恢复、秘密脱敏、CSRF/Origin 和错误路径有测试。
- Test 不遗留后台任务；重复 connect/disconnect 幂等；断开/删除/退出停止采集。
- 活跃连接上限和无效/未连接 ID 有明确结果。
- OpenAPI 与实现一致，兼容模式有契约测试。

### 隔离与并发

- 两个集群使用相同 node/group ID 时，所有资源和 SSE 均只返回目标连接数据。
- 并发连接、切换、断开和在途请求不会串数据或创建重复 runtime。
- 两个浏览器标签页可同时查看不同集群。

### 前端与端到端

- 从空状态新建、测试、保存并连接，查看 Overview，切换到另一集群并保持主页面。
- 刷新和 bookmark 恢复正确连接；所有内部导航不丢 connection。
- 切换后旧 SSE 关闭、旧请求/resync/详情请求失效，cursor 和事件偏好正确隔离；用延迟响应与晚到 SSE 证明
  页面从未短暂展示旧集群数据。
- 同一 browser context 的两个 Page 分别保持 A/B 连接和 SSE；另一标签页 disconnect/delete 当前连接时行为明确。
- 通过真实点击 Shell、全局搜索和钻取链接验证 connection 保留，并覆盖登录回跳。
- 覆盖连接失败、Test 失败后 Save、Save & Connect 失败、未连接、断开、删除、凭证重启恢复和移动端/键盘可用性。
- 运行 rr-admin Rust 测试、前端 typecheck/lint/unit/build、mock E2E 和双集群真实 E2E。真实 harness 必须启动
  两个独立集群、一个无 peers/discovery 的 Console 和独立 state-dir，通过 UI 建立两个连接。

## 13. 实施顺序

1. 定义连接 DTO、错误和 OpenAPI。
2. 实现 profile 持久化、秘密边界和 ConnectionManager。
3. 按连接构造现有采集栈并接入作用域 API。
4. 实现连接管理 UI 与前端 connection 状态。
5. 改造所有数据请求、SSE、导航和切换清理。
6. 完成兼容、文档和多层验收。
