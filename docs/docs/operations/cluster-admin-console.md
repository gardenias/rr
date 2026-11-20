---
sidebar_position: 2
---

# 集群管理控制台 / Cluster Admin Console

`rr-admin console` 启动同源只读 Console BFF：浏览器访问静态前端、JSON 和 SSE，BFF 持有节点 Admin
凭证并共享上游采集。Console 不是 Raft 控制面，停止 Console 不会影响节点服务。
`rr-admin console` starts a same-origin, read-only Console BFF. The browser receives the static frontend, JSON, and
SSE; the BFF holds node Admin credentials and shares upstream collection. The Console is not a Raft control plane, and
stopping it does not stop node service.

:::info 数据路径 / Data Paths
Console 支持显式 Admin seed 和 etcd 受信发现。实时性能源自逐节点 `/metrics` 抓取并保存在
`CONSOLE_MEMORY`；成对配置 `--prometheus-url` 与 `--prometheus-cluster` 后，还可查询固定 raw-family
历史模板。旧节点、缺失端点、空数据和上游错误仍按能力降级，不会补零或伪造历史。
Console supports explicit Admin seeds and trusted etcd discovery. Live performance comes from per-node `/metrics`
scrapes retained in `CONSOLE_MEMORY`. Pairing `--prometheus-url` with `--prometheus-cluster` also enables fixed
raw-family history templates. Legacy nodes, missing endpoints, empty data, and upstream errors still degrade by
capability without zero-filling or fabricating history.
:::

## 构建与启动 / Build and Startup

### 静态导出顺序 / Static Export Order

Rust 编译和普通 debug 构建不需要 Node.js，也不要求已有前端导出。完整浏览器 UI 需要在启动 Console 前生成
`crates/rr-admin/console/out`；缺少该目录时 BFF 仍提供 JSON/SSE 和一个说明性 HTML fallback。
Rust compilation and ordinary debug builds need neither Node.js nor an existing frontend export. The full browser UI
requires `crates/rr-admin/console/out` to be generated before Console startup. Without it, the BFF still serves JSON/SSE
and an explanatory HTML fallback.

```bash
pnpm --dir crates/rr-admin/console install --frozen-lockfile
pnpm --dir crates/rr-admin/console run build
cargo build --release -p rr-admin
```

源码树内的本地部署可使用 `just console-up`：它构建静态导出和 debug `rr-admin`，从 `RR_CONSOLE_TOKEN` 读取
viewer token，将 profile 存入 `target/just/console-state`，并把 PID/日志写入 `target/just/`。默认监听
`127.0.0.1:8080`，可用 `RR_CONSOLE_LISTEN` 覆盖；`just console-up <seed-roster>` 可附带一个临时启动连接，
`just console-down` 只停止该 recipe 管理的进程。
For local source-tree deployment, `just console-up` builds the static export and debug `rr-admin`, reads the viewer
token from `RR_CONSOLE_TOKEN`, stores profiles in `target/just/console-state`, and writes PID/log files under
`target/just/`. It listens on `127.0.0.1:8080` unless `RR_CONSOLE_LISTEN` is set. Use `just console-up <seed-roster>`
for an ephemeral startup connection and `just console-down` to stop only the recipe-managed process.

部署时同时携带二进制和静态导出目录，或用 `--static-dir` 指向导出位置。默认目录是源码树中的
`crates/rr-admin/console/out`，不是独立 Node.js 生产服务。
Ship the static export with the binary, or point `--static-dir` at its deployed location. The default is
`crates/rr-admin/console/out` in the source tree; no standalone Node.js production server is used.

### 独立多连接模式 / Standalone Multi-Connection Mode

默认启动不需要 peers、discovery、Admin TLS/token 或 Prometheus 参数；无来源启动只初始化监听器、viewer 认证、
静态页面、profile store 和空 `ConnectionManager`，不会连接或轮询任何集群。进程级 `--poll-interval-secs` 仍属于
legacy 启动连接；保存连接的轮询间隔在表单 Advanced settings 中按连接设置。

The default startup needs no peers, discovery, Admin TLS/token, or Prometheus options. It initializes only the listener,
viewer authentication, static assets, profile store, and an empty `ConnectionManager`; no cluster is contacted or polled.
Process-level `--poll-interval-secs` remains a legacy-startup option. Saved connections use their own Advanced-settings
polling interval.

```bash
export RR_CONSOLE_TOKEN='<viewer-token-from-your-secret-store>'

./target/release/rr-admin console \
  --listen 127.0.0.1:8080 \
  --console-token-env RR_CONSOLE_TOKEN \
  --state-dir /var/lib/rr-console \
  --static-dir /opt/rr/console
```

打开 `/connections/`。每个 profile 可配置名称、环境、一个或多个 Admin seeds、可选 etcd discovery、各自的
Admin/discovery TLS 和凭证，以及可选 Prometheus 精确 matcher。Test candidate 有 10 秒总 deadline、最多 4 个并发，
不保存配置且不创建 scheduler/watch/history 长任务；Save 不自动采集；Connect 才懒创建独立 runtime。Save & Connect
失败时保留已保存 profile。Edit 要求先 Disconnect；Disconnect 保留 profile 并终止该 runtime 的任务和 SSE；Delete
先 Disconnect 再删除 profile 与 keychain entry。重复 Connect/Disconnect 幂等，同时活跃 runtime（含 Connecting）
最多 8 个，不做空闲自动回收。

Open `/connections/`. Each profile has a name, environment, one or more Admin seeds, optional etcd discovery, its own
Admin/discovery TLS and credentials, and optional Prometheus exact matcher. Candidate tests have a ten-second overall
deadline and four-way concurrency bound; they neither save the candidate nor create scheduler/watch/history tasks. Save
starts no collection, while Connect lazily creates an isolated runtime. Save & Connect retains the profile if connecting
fails. Edit requires Disconnect. Disconnect retains the profile but stops runtime tasks and SSE; Delete disconnects before
removing the profile and keychain entries. Connect/Disconnect are idempotent, at most eight runtimes including CONNECTING
may be active, and idle runtimes are not automatically reclaimed.

CONNECTED、DEGRADED 和 UNREACHABLE 的 PROFILE/STARTUP 项都显示明显的 Open，可直接进入对应 Overview。状态由
runtime cache 投影：全部 Admin seeds 失效时为 UNREACHABLE；部分或陈旧证据为 DEGRADED；恢复且证据完整时回到
CONNECTED。这些状态都保留有效 runtime 的 scoped API/SSE，不会因 UNREACHABLE 返回 CONNECTION_NOT_CONNECTED。

CONNECTED, DEGRADED, and UNREACHABLE PROFILE/STARTUP entries expose a clear Open action into that connection's
Overview. Runtime cache evidence projects the public state: all failed Admin seeds produce UNREACHABLE, partial or stale
evidence produces DEGRADED, and complete recovery returns to CONNECTED. Every state retains the live runtime for scoped
API/SSE access; UNREACHABLE does not turn into CONNECTION_NOT_CONNECTED.

每个 runtime 独占 upstream、discovery、scheduler、cache、Prometheus history 配置和生命周期。所有集群数据与 SSE 都位于
`/api/v1/console/connections/{id}/...`，不存在、未连接和连接失败不会回退到默认集群。浏览器 URL 中的
`?connection=<id>` 是当前页面唯一权威；`/` 或缺失/未知/已断开的 ID 进入管理页。切换只保留主页面和 `window`，
取消旧请求/SSE 并清空选择、tab、搜索、cursor；刷新、bookmark 和同一 browser context 的多个 Page 可分别保持
不同连接。recent ID 的 localStorage 不覆盖 URL，也不会自动 Connect。

Each runtime owns its upstream, discovery, scheduler, cache, Prometheus history configuration, and lifecycle. All cluster
data and SSE live under `/api/v1/console/connections/{id}/...`; unknown, disconnected, or failed IDs never fall back to a
default cluster. The URL's `?connection=<id>` is authoritative for each page. `/` and missing, unknown, or disconnected IDs
lead to the manager. Switching retains only the main route and `window`, aborts old requests/SSE, and clears selection,
tabs, search, and cursors. Refresh, bookmarks, and Pages in one browser context can retain different connections. A recent
ID in localStorage neither overrides the URL nor auto-connects.

### Profile 与秘密持久化 / Profile and Secret Persistence

非敏感 profile 默认写入平台配置目录的 `rr/console/connections.json`；`--state-dir PATH` 改为
`PATH/connections.json`。文件带格式版本，使用同目录临时文件、flush、`0600` 和原子替换；损坏、未知版本或读取失败
会报错，不会按空列表覆盖。部署需让 Console 运行用户独占该目录并纳入配置备份；内存 runtime、缓存和趋势不持久化。

Non-sensitive profiles default to `rr/console/connections.json` under the platform configuration directory;
`--state-dir PATH` selects `PATH/connections.json`. The versioned file uses a sibling temporary file, flush, mode `0600`,
and atomic replacement. Corruption, unknown versions, and read failures are reported rather than overwritten as an empty
store. Give the Console user exclusive access and back up this configuration as appropriate; runtimes, caches, and trends
are not persisted.

Admin token 和 discovery password 是 write-only，省略=保留、null=清除、非空值=替换。默认仅保存于进程内存，断开仍可
重连，edit/delete/退出时清除；只有勾选 Remember 才使用 OS keychain，profile 只保存 secret reference。API 仅返回
`credentials_configured`、`credential_source` 和 `keychain_available`，不会返回秘密或私钥内容。TLS CA/cert/key 是
Console 主机文件路径，浏览器不上传文件内容。Console 重启后所有 profile 均为 Disconnected；keychain 凭证可重连，
内存凭证必须重新输入。Prometheus URL 拒绝 userinfo/query/fragment，公开连接错误使用固定脱敏 message 和 correlation ID。

Admin tokens and discovery passwords are write-only: omitted keeps, null clears, and a non-empty value replaces. They
remain in process memory by default, survive disconnect for reconnection, and are cleared on edit/delete/exit. Only the
explicit Remember option uses the OS keychain; profiles contain secret references, not secret values. APIs expose only
`credentials_configured`, `credential_source`, and `keychain_available`. TLS CA/certificate/key fields are host paths;
the browser does not upload their contents. After a restart every profile is Disconnected; keychain credentials can be
reused, while memory credentials must be re-entered. Prometheus URLs reject userinfo, query, and fragments. Public
connection failures use fixed sanitized messages and correlation IDs.

隔离测试或 headless 部署可显式使用进程级 `--keychain-disable`，完全禁止该进程的 OS keychain get/set/delete，
凭证仅保留在内存且 API 返回 `keychain_available=false`。已有 profile 若只有 keychain 凭证，需要重新输入；
重启后内存凭证仍需重新输入。未传参数时生产行为不变，默认启用 keychain 能力探测。

Isolated tests and headless deployments can explicitly set the process-level `--keychain-disable` flag. It disables all
OS keychain get/set/delete operations for that process, keeps credentials memory-only, and makes the API report
`keychain_available=false`. Re-enter credentials for an existing profile whose secrets exist only in the keychain;
memory credentials also need re-entry after restart. Omitting the flag preserves the production default of probing and
using the OS keychain.

### 兼容：临时启动连接 / Compatibility: Ephemeral Startup Connection

`--peers` 的每项是 `node_id@AdminService_host:port`，不是 client/bootstrap 地址。不要套用 `rr-server serve
--peers` 的语义，也不要推导端口。
Each `--peers` entry is `node_id@AdminService_host:port`, not a client/bootstrap address. Do not apply `rr-server serve
--peers` semantics or derive ports.

```bash
export RR_ADMIN_TOKEN='<admin-token-from-your-secret-store>'

./target/release/rr-admin \
  --token-env RR_ADMIN_TOKEN \
  console \
  --listen 127.0.0.1:8080 \
  --peers 1@127.0.0.1:52001,2@127.0.0.1:52002,3@127.0.0.1:52003
```

仅为明文本地 AdminService 省略 `--token-env`。打开 `http://127.0.0.1:8080`。默认概要轮询为 5 秒，可用
`--poll-interval-secs 1..=300` 调整；多个浏览器共享该调度，不增加逐浏览器采集循环。
Omit `--token-env` only for a plaintext local AdminService that does not require authentication. Open
`http://127.0.0.1:8080`. Summary polling defaults to five seconds and can be changed with
`--poll-interval-secs 1..=300`; browsers share this scheduler rather than creating per-browser collection loops.

### 非 loopback 安全启动 / Secure Non-Loopback Startup

非 loopback 监听必须同时启用 Console HTTPS 和 viewer token。`--allow-insecure` 只接受 loopback，不能作为远程
开发绕过开关。token 只能经环境变量名引用，不能放入 argv。
A non-loopback listener requires both Console HTTPS and a viewer token. `--allow-insecure` is accepted only for
loopback and is not a remote-development bypass. Tokens are referenced by environment-variable name and never placed
in argv.

```bash
export RR_ADMIN_TOKEN='<admin-token-from-your-secret-store>'
export RR_CONSOLE_TOKEN='<viewer-token-from-your-secret-store>'

./target/release/rr-admin \
  --token-env RR_ADMIN_TOKEN \
  --tls-ca /etc/rr/admin-ca.pem \
  --tls-cert /etc/rr/admin-client.pem \
  --tls-key /etc/rr/admin-client.key \
  console \
  --listen 0.0.0.0:8443 \
  --peers 1@rr-1.internal:52001,2@rr-2.internal:52001 \
  --console-token-env RR_CONSOLE_TOKEN \
  --tls-cert /etc/rr/console.pem \
  --tls-key /etc/rr/console.key
```

全局 `--token-env`、`--tls-ca`、`--tls-cert`、`--tls-key` 和可选 `--tls-domain` 位于 `console` **之前**，用于
BFF→AdminService；子命令后的 `--tls-cert`/`--tls-key` 位于 `console` **之后**，用于 Browser→Console HTTPS。
The global `--token-env`, `--tls-ca`, `--tls-cert`, `--tls-key`, and optional `--tls-domain` appear **before**
`console` and configure BFF-to-AdminService connections. The subcommand's `--tls-cert` and `--tls-key` appear **after**
`console` and configure browser-to-Console HTTPS.

- loopback 的 viewer 认证和 HTTPS 可选；设置 `RR_CONSOLE_TOKEN` 即启用登录。<br />Viewer authentication and HTTPS
  are optional on loopback; setting `RR_CONSOLE_TOKEN` enables login.
- viewer 登录换取 15 分钟内存 session；cookie 为 HttpOnly、SameSite=Strict，HTTPS 时增加 Secure。<br />Viewer
  login issues a 15-minute in-memory session; cookies are HttpOnly and SameSite=Strict, with Secure under HTTPS.
- session 不持久化，Console 重启会撤销现有 session；连接 CRUD、test/connect/disconnect 和 reconcile 等
  POST/PATCH/DELETE 都检查 CSRF token 和同源 Origin。<br />Sessions are not persisted, so a Console restart revokes them.
  Connection CRUD, test/connect/disconnect, reconcile, and other POST/PATCH/DELETE mutations check CSRF and same-origin
  Origin.
- 节点 Admin token 只在 BFF 内；不要写入 URL、HTML、localStorage 或日志。<br />Node Admin tokens remain inside the
  BFF and must never appear in URLs, HTML, localStorage, or logs.
- 节点 `/metrics` 当前可能未认证；用网络策略、TLS 或可信内网保护它，不由 Console 暴露原始指标代理。<br />Node
  `/metrics` may be unauthenticated; protect it with network policy, TLS, or a trusted network boundary. The Console
  does not proxy raw metrics.

## 发现与信任 / Discovery and Trust

### Seed 与 discovery 模式 / Seed and Discovery Modes

显式 seed 是 operator 对这些 Admin 地址及其同群关系的信任断言。seed 地址即使来自旧节点也保留，缺少新增 RPC
只降级相应模块；经认证的集群拓扑和本机描述符可以补充其他受信节点，但不能用未认证地址替换 seed。
Explicit seeds are the operator's trust assertion for those Admin addresses and their cluster membership. Seeds remain
usable on older nodes, and missing new RPCs degrade only the affected modules. Authenticated cluster topology and local
descriptors can add other trusted nodes, but an unauthenticated address cannot replace a seed.

discovery 模式连接同一 etcd namespace 下的 `NodeDirectory` 和 `ConfigProvider`。生产环境应同时使用 CA
验证、mTLS（若 etcd 要求）和用户名/密码认证：
Discovery mode connects to the `NodeDirectory` and `ConfigProvider` in the same etcd namespace. In production, use CA
verification, mTLS when required by etcd, and username/password authentication:

```bash
export RR_ADMIN_TOKEN='<admin-token-from-your-secret-store>'
export RR_CONSOLE_ETCD_PASSWORD='<etcd-password-from-your-secret-store>'

./target/release/rr-admin \
  --token-env RR_ADMIN_TOKEN \
  console \
  --listen 127.0.0.1:8080 \
  --discovery etcd://etcd-1.internal:2379,etcd-2.internal:2379 \
  --cluster-name prod-rr \
  --discovery-tls-ca /etc/rr/etcd-ca.pem \
  --discovery-tls-cert /etc/rr/etcd-console.pem \
  --discovery-tls-key /etc/rr/etcd-console.key \
  --discovery-tls-domain etcd.internal \
  --discovery-username rr-console \
  --discovery-password-env RR_CONSOLE_ETCD_PASSWORD
```

`--discovery` 只接受 `etcd://host:port[,host:port...]`，并强制要求非空 `--cluster-name`。同时提供 peers 时，
目录决定非 seed roster，显式 seed 仅作保留的连通性和信任起点；目录不会覆盖同 ID 的显式 seed。
`--discovery` accepts only `etcd://host:port[,host:port...]` and requires a non-empty `--cluster-name`. When peers are
also present, the directory controls the non-seed roster while explicit seeds remain connectivity and trust anchors;
a directory record does not override an explicit seed with the same node ID.

安全参数规则如下；错误组合或未设置/为空的密码环境变量会在启动时失败：
Discovery security is validated at startup, including invalid combinations and missing or empty password variables:

- 必须提供 `--discovery-tls-ca`，或显式选择 `--discovery-allow-insecure`；二者互斥。<br />Require either
  `--discovery-tls-ca` or explicit `--discovery-allow-insecure`; they are mutually exclusive.
- `--discovery-tls-cert` 与 `--discovery-tls-key` 必须成对，并同时提供 CA；`--discovery-tls-domain` 也要求 CA。<br />
  `--discovery-tls-cert` and `--discovery-tls-key` are a pair and require the CA; `--discovery-tls-domain` also
  requires the CA.
- `--discovery-username` 与 `--discovery-password-env` 必须成对且非空，并且认证必须基于 CA TLS。密码只从指定
  环境变量读取，不能放入 argv。<br />`--discovery-username` and `--discovery-password-env` are a non-empty pair and
  authentication requires CA-backed TLS. The password is read only from the named environment variable, never argv.

本地无认证明文必须显式 opt-in，不能省略安全配置而隐式使用：
Unauthenticated local plaintext requires an explicit opt-in; omitting security configuration does not enable it
implicitly:

```bash
./target/release/rr-admin console \
  --listen 127.0.0.1:8080 \
  --discovery etcd://127.0.0.1:2379 \
  --cluster-name rr-local \
  --discovery-allow-insecure
```

`--discovery-allow-insecure` 仅用于 legacy 本地 etcd，不能与 CA 或用户名/密码组合。它与 Browser→Console 的
`--allow-insecure` 是不同开关，也不替代节点 AdminService 的 TLS/token。
`--discovery-allow-insecure` is for legacy local etcd only and cannot be combined with CA or username/password
authentication. It is distinct from the browser-to-Console `--allow-insecure` flag and does not replace AdminService
TLS or tokens.

### NodeDirectory 契约 / NodeDirectory Contract

Console discovery 使用 `/rr/{cluster}/directory/nodes/{node_id}` 的 leased JSON 记录，默认 TTL 15 秒、节点每
5 秒 heartbeat；完整描述符、外部 provisioning 和冲突语义见
[ConfigProvider › NodeDirectory](../guide/config-provider.md#节点目录--nodedirectory)。
Console discovery consumes leased JSON records at `/rr/{cluster}/directory/nodes/{node_id}` with a 15-second default
TTL and five-second node heartbeats. See [ConfigProvider ›
NodeDirectory](../guide/config-provider.md#节点目录--nodedirectory) for complete descriptors, external provisioning,
and conflict semantics.

实现遵循以下信任边界：
The implementation enforces these trust boundaries:

- discovery 是 roster 权威；若同时提供 peers，peers 仅作连通性种子，且 `--cluster-name` 必填。<br />Discovery is
  the roster authority. When peers are also present, they are connectivity seeds only, and `--cluster-name` is required.
- 只信任显式 seed、受信目录记录，或经认证本机描述符返回的地址；普通 metrics 标签和未认证 HTTP 响应不能提升
  地址信任。<br />Trust only explicit seeds, authenticated directory records, or addresses returned by an authenticated
  local descriptor. Metrics labels and unauthenticated HTTP responses cannot promote address trust.
- 校验 node ID、cluster UUID、目录 revision 和 TLS 身份后才可转发 Admin token；冲突的非空 UUID 以及空与非空
  UUID 混合必须隔离，不得合并。<br />Verify node ID, cluster UUID, directory revision, and TLS identity before forwarding
  an Admin token. Conflicting non-empty UUIDs and mixed empty/non-empty UUIDs must be quarantined, never merged.
- 非 seed 节点只使用显式 admin/observability 地址；缺失时标记 `ENDPOINTS_UNAVAILABLE`，不做端口加减。<br />
  Non-seed nodes use only explicit Admin/observability addresses. Missing endpoints produce `ENDPOINTS_UNAVAILABLE`;
  port arithmetic is forbidden.
- lease 过期、watch 断开或 compaction 需要全量对账。非 seed 若 15 秒未通过权威对账或认证拓扑刷新信任，
  立即从可连接 roster 移除并暂停 Admin token 转发；显式 seed 不过期。不可达节点保留最后快照并标 stale，
  不直接从拓扑删除。<br />Lease expiry, watch interruption, and compaction require full reconciliation. A non-seed
  that has not refreshed trust through authoritative reconciliation or authenticated topology for 15 seconds is removed
  from the dialable roster and Admin-token forwarding stops; explicit seeds do not expire. Unreachable nodes retain
  their last snapshot with stale evidence instead of disappearing from topology.

## Console HTTP API 与兼容路由 / Console HTTP API and Compatibility Routes

机器可读契约以 `crates/rr-admin/openapi/console-v1.json` 为准，server base 为 `/api/v1/console`。连接控制面如下；
POST/PATCH/DELETE 都要求同源 `Origin`，启用 viewer 认证时还要求 `X-Console-CSRF`。登录是既有公开例外，成功后
再获取 session CSRF。连接响应使用 neutral meta：`cluster_id: null`、`retention_source: NONE`、空 sources/capabilities。

The machine-readable contract is `crates/rr-admin/openapi/console-v1.json`, with server base `/api/v1/console`.
Connection control routes are listed below. Every POST/PATCH/DELETE requires a same-origin `Origin` and, when viewer
authentication is enabled, `X-Console-CSRF`; the established public login flow is the exception. Control responses use
neutral meta: null `cluster_id`, `NONE` retention, and empty sources/capabilities.

| 方法与路径 / Method and Path | 行为 / Behavior |
| :--- | :--- |
| `GET /connections` | 脱敏 profile/runtime 列表和 keychain 能力；不逐连接请求 summary / Redacted list and keychain capability; no per-connection summary fan-out |
| `POST /connections` | 保存 DISCONNECTED profile，返回 201 / Save a DISCONNECTED profile; 201 |
| `POST /connections/test` | 测试未保存 candidate；可选 existing ID 仅在 BFF 内解析 Keep 秘密 / Test an unsaved candidate; optional existing ID resolves Keep secrets only inside the BFF |
| `GET/PATCH/DELETE /connections/{id}` | 读取、断开状态编辑、断开并删除 / Read, edit while disconnected, or disconnect and delete |
| `POST /connections/{id}/test` | 测试已保存配置 / Test a saved profile |
| `POST /connections/{id}/connect` | 幂等懒启动 runtime / Idempotently start the runtime lazily |
| `POST /connections/{id}/disconnect` | 幂等停止 runtime，保留 profile / Idempotently stop the runtime and retain the profile |

新 UI 的全部数据面必须带 connection ID：`/connections/{id}/summary`、`nodes`、`nodes/{node}`、
`nodes/{node}/boots`、`groups`、`groups/{group}`、`groups/{group}/replicas`、`topology`、
`metrics/performance`、`snapshots`、`lifecycle`、`events`、`sources`、`sources/reconcile` 和 `stream`。
资源错误使用该 runtime 的 cache meta；普通已捕获的在途读取可完成，但断开后新请求返回
`CONNECTION_NOT_CONNECTED`，SSE 关闭，不启动新采集。

Every data-plane route used by the new UI is connection-scoped: `/connections/{id}/summary`, `nodes`, `nodes/{node}`,
`nodes/{node}/boots`, `groups`, `groups/{group}`, `groups/{group}/replicas`, `topology`, `metrics/performance`,
`snapshots`, `lifecycle`, `events`, `sources`, `sources/reconcile`, and `stream`. Resource errors retain that runtime's
cache meta. Already-captured ordinary reads may finish, but after disconnect new requests return
`CONNECTION_NOT_CONNECTED`, SSE closes, and no new collection starts.

旧无作用域资源路由和 `/stream` 别名只映射到本次进程由 `--peers`/`--discovery` 创建的固定临时启动连接；它们绝不
选择其他 profile。无启动连接时返回 `400 CONNECTION_REQUIRED`；启动连接被断开或删除后仍返回明确错误，bookmark
不承诺跨重启保留。新 UI 不使用这些兼容路由。

Legacy unscoped resources and the `/stream` alias map only to the fixed ephemeral startup connection created by
`--peers`/`--discovery`; they never select another profile. Without a startup connection they return
`400 CONNECTION_REQUIRED`. Disconnecting or deleting that connection leaves an explicit error rather than retargeting
the route, and its bookmark is not promised across restarts. The new UI never uses these compatibility routes.

常见控制错误包括 `404 CONNECTION_NOT_FOUND`、`409 CONNECTION_NOT_CONNECTED/CONNECTION_ACTIVE`、
`400 CONNECTION_REQUIRED/INVALID_CONNECTION_PROFILE`、`409 CREDENTIALS_REQUIRED`、
`429 ACTIVE_CONNECTION_LIMIT` 和 `502 CONNECTION_TEST_FAILED/CONNECTION_INITIALIZATION_FAILED`。错误响应不含秘密，
用 correlation ID 关联内部详细日志。

Common control errors include `404 CONNECTION_NOT_FOUND`, `409 CONNECTION_NOT_CONNECTED/CONNECTION_ACTIVE`,
`400 CONNECTION_REQUIRED/INVALID_CONNECTION_PROFILE`, `409 CREDENTIALS_REQUIRED`, `429 ACTIVE_CONNECTION_LIMIT`, and
`502 CONNECTION_TEST_FAILED/CONNECTION_INITIALIZATION_FAILED`. Error bodies contain no secrets; use the correlation ID
to find detailed internal logs.

## 健康、Readiness 与 Quorum / Health, Readiness, and Quorum

### 节点与 Console 探针 / Node and Console Probes

节点 `/health` 是 liveness：HTTP 服务仍能响应就返回 200，不证明 Raft 可服务。`/ready` 是流量准入：
`ready=true` 返回 200，否则返回 503 和 `ready/reason/since_unix_secs/phase/detail` JSON。legacy 节点在
`start()` 成功后 ready；managed 节点还要求所有 hosted group 已 formed 且观察到明确 leader。
A node's `/health` endpoint is liveness: a responsive HTTP service returns 200 without proving Raft serviceability.
`/ready` gates traffic: `ready=true` returns 200; otherwise it returns 503 with
`ready/reason/since_unix_secs/phase/detail` JSON. Legacy nodes become ready after `start()` succeeds. Managed nodes also
require every hosted group to be formed and have an explicitly observed leader.

managed lifecycle store 不可用时可凭已初始化本地状态继续 Raft，但 `/ready` 以 `phase=degraded` 返回 503；
`forming`、`awaiting_replication`、`waiting_for_leader` 和 `failed` 同样不是 ready。liveness 与 readiness 必须
分开告警，不能把 ready=false 当成进程死亡。
A managed node can continue Raft from initialized local state while its lifecycle store is unavailable, but `/ready`
returns 503 with `phase=degraded`. `forming`, `awaiting_replication`, `waiting_for_leader`, and `failed` are also not
ready. Alert on liveness and readiness independently; ready=false does not mean the process is dead.

Console 从受信 observability origin 分别抓取 `/health` 和 `/ready`，保留 `liveness`、`ready` 和 `ready_phase`。
例如 live=true、ready=false 表示进程存活但不应接流量。ready HTTP 503 会记录真实 false 和 phase；连接失败、
非法 JSON 或超过 16 KiB 的 ready body 则记录来源错误和未知值。metrics/probe 错误使节点显示 `DEGRADED`，
Admin 已不可达时则显示 `STALE`/`UNREACHABLE`，不会用一次 health 200 掩盖其他故障。
Console fetches `/health` and `/ready` separately from the trusted observability origin and retains `liveness`,
`ready`, and `ready_phase`. For example, live=true with ready=false means the process is alive but should not receive
traffic. A ready HTTP 503 preserves the real false value and phase. Connection failures, invalid JSON, or a ready body
over 16 KiB produce source errors and unknown values. Metrics/probe errors mark the node `DEGRADED`; an unreachable
Admin source instead produces `STALE`/`UNREACHABLE`. A single health 200 never masks other failures.

### 期望 Voter 与 quorum / Expected Voters and Quorum

期望 voter 集来自受信 ConfigProvider 或认证拓扑的 replica IDs；判定 `HEALTHY` 时还要求实际 membership
voters 与该集合一致。fresh learner 或其他不在期望集合中的节点不计入 quorum 分子，不能靠新增 learner 让
`QUORUM_AT_RISK` 消失。
Expected voters come from replica IDs in trusted ConfigProvider or authenticated topology; `HEALTHY` additionally
requires the observed membership voters to equal that set. Fresh learners and other nodes outside the expected set do
not count toward the quorum numerator, so adding a learner cannot clear `QUORUM_AT_RISK`.

对 N 个期望 voter，多数为 `floor(N/2)+1`。Console 只统计新鲜可达且属于期望集合的 voter；低于多数显示
`QUORUM_AT_RISK`，全部期望 voter 可达但证据仍不完整则为 `DEGRADED`。期望集合为空时不计算多数，缺失
membership 或 leader lag 证据也不会显示健康。
For N expected voters, a majority is `floor(N/2)+1`. Console counts only fresh, reachable voters in that set. Below a
majority it reports `QUORUM_AT_RISK`; even when all expected voters are reachable, incomplete evidence produces
`DEGRADED`. No majority is calculated for an empty expected set, and missing membership or leader-lag evidence never
becomes healthy.

`HEALTHY` 还需要一致的新鲜 leader/term 观察、精确 membership 和 leader 提供的完整 follower lag 证据。
`QUORUM_AT_RISK` 始终只是 Console 的观测视角，不等价于已证明 Raft 失去 quorum。
`HEALTHY` also requires consistent fresh leader/term observations, exact membership, and complete follower-lag evidence
from the leader. `QUORUM_AT_RISK` remains the Console's observational perspective, not proof that Raft has lost quorum.

## Boot Journal 与重置 / Boot Journal and Reset

`rr-server` 的 RocksDB 模式默认写入 `{data_dir}/boot-journal/boot-journal.json`。`storage=mem` 默认只有当前
boot，状态为 `VOLATILE`；需要持久历史时显式设置 `[node].boot_journal_dir` 或 `--boot-journal-dir`。
In `rr-server` RocksDB mode, the journal defaults to `{data_dir}/boot-journal/boot-journal.json`. `storage=mem` nodes
retain only the current boot as `VOLATILE` unless `[node].boot_journal_dir` or `--boot-journal-dir` is explicitly set.

```toml
[node]
boot_journal_dir = "/var/lib/rr-boot-history/node-1"
```

- 每次启动追加 boot ID、启动时间和 App/Rr/git/build 信息；优雅关闭补写结束时间和 `GRACEFUL`。<br />Every start
  appends a boot ID, start time, and App/Rr/git/build information. Graceful shutdown adds an end time and `GRACEFUL`.
- 下次启动发现仍为 `RUNNING` 的旧记录时，将其改为 `ABNORMAL_OR_UNCERTAIN`，结束时间保持未知；不能据此断言
  crash 原因。<br />A previous record still `RUNNING` at the next startup becomes `ABNORMAL_OR_UNCERTAIN` with no invented
  end time; this does not prove a crash reason.
- 持久 journal 每节点最多 100 个 boot，按新到旧分页；默认每页 50，最大 200。<br />A persistent journal retains at
  most 100 boots per node, newest first. Pages default to 50 and are capped at 200.
- 复用同一目录会延续历史。换成空目录、删除 journal，或重置包含默认 journal 的整个 data dir，都会重置可见
  历史；需要跨数据重置保留历史时使用独立 journal dir。<br />Reusing the same directory continues history. An empty
  replacement directory, journal deletion, or resetting a data dir that contains the default journal resets visible
  history. Use an independent journal dir when history must survive a data reset.
- journal 损坏、版本不支持或持久化失败会响亮报错；不要以伪造空历史替代恢复。<br />Corrupt, unsupported-version,
  or unwritable journals fail loudly; do not replace recovery with fabricated empty history.

### 有界按需历史 / Bounded On-Demand History

节点 boot 列表按 cursor 请求，单页 1–200，不会随概要轮询拉取全部 100 条。Lifecycle 首页每节点最多读取
`min(page_size+1,100)` 条，后续 cursor 页最多 100 条；跨节点 fan-out 上限为 8，handler deadline 为 2 秒。
Node boot lists use cursors with 1–200 records per page and do not fetch all 100 records during summary polling. A
Lifecycle first page fetches at most `min(page_size+1,100)` recent records per node and subsequent cursor pages at most
100; cross-node fan-out is capped at eight with a two-second handler deadline.

相同 boot page 请求共享 single-flight，boot page/flight 状态以 256 个 key、30 秒 LRU/TTL 为界。组详情也按
每节点 256 个 key、30 秒为界，失败缓存仅 1 秒，组级按需并发上限为 16。信任过期或地址/boot 改变后不会
复用旧身份的历史 key，新请求必须先重新建立信任；缓存不是绕过凭证到期的通道。
Identical boot-page requests share single-flight execution. Boot page/flight state is bounded to 256 keys with a
30-second LRU/TTL. Group details are bounded per node to 256 keys for 30 seconds, failure caching lasts one second, and
on-demand group concurrency is capped at 16. After trust expiry or an address/boot change, history keys from the old
identity are not reused; new requests require renewed trust. Cache entries never bypass credential expiry.

## 留存、新鲜度与规模 / Retention, Freshness, and Scale

### 实时 `CONSOLE_MEMORY` / Live `CONSOLE_MEMORY`

实时性能来自受信描述符的 `observability_addr`，BFF 抓取 `http://<observability_addr>/metrics` 后按受信 peer
node ID 归属样本；metrics 响应内的 `node_id` 标签不能替换该身份。当前节点抓取使用 HTTP，描述符不接受 URL
scheme，因此生产环境必须用可信内网或网络策略保护该端点。
Live performance comes from the trusted descriptor's `observability_addr`. The BFF scrapes
`http://<observability_addr>/metrics` and assigns samples to the trusted peer node ID; a `node_id` label in the metrics
response cannot replace that identity. Node scraping currently uses HTTP and descriptors do not accept a URL scheme, so
protect this endpoint with a trusted network or network policy in production.

成功的实时序列以 `retention_source=CONSOLE_MEMORY` 返回。每个活跃连接拥有独立有界缓存：事件环形缓冲最多
512 条，观测点最多 20,000 个；最多 8 个活跃 runtime 时，容量按 runtime 分别计算。已保存但断开的 profile 不占
采集缓存。没有固定时长保证，容量耗尽时淘汰最旧记录，重启全部清空。15m/1h/6h/24h 窗口只筛选已留存数据，
不会回填 Console 启动前的平线。没有可用实时序列时，该响应为 `UNSUPPORTED` + `NONE`，不等于整个性能功能不存在。
Successful live series return `retention_source=CONSOLE_MEMORY`. Every active connection has an isolated bounded cache
of at most 512 events and 20,000 observation points; with at most eight active runtimes, these limits apply per runtime.
Saved but disconnected profiles use no collection cache. There is no fixed duration guarantee; oldest records are evicted
at capacity and all data is lost on restart. The 15m/1h/6h/24h windows filter existing retention and never backfill a
flat line from before Console startup. If no live series is available, that response is `UNSUPPORTED` with `NONE`; it does
not mean the entire performance feature is absent.

未配置历史时默认读取实时缓存；显式 `source=CONSOLE_MEMORY` 始终选择实时缓存。配置历史后，省略 `source`
默认选择 Prometheus，需要实时数据时必须明确选择 `CONSOLE_MEMORY`。
Without history configuration, performance defaults to the live cache; explicit `source=CONSOLE_MEMORY` always selects
it. Once history is configured, omitting `source` defaults to Prometheus, so select `CONSOLE_MEMORY` explicitly for live
data.

节点在最后成功采集超过三倍轮询间隔后视为 stale（默认 15 秒）。stale/partial 数据保留来源、采样时间、错误和
correlation ID；不可达不等于进程停止，`QUORUM_AT_RISK` 也只是 Console 可达性视角。
A node becomes stale after three polling intervals without successful collection (15 seconds by default). Stale/partial
data retains source, observation time, error, and correlation ID. Unreachable does not mean stopped, and
`QUORUM_AT_RISK` is only the Console's reachability perspective.

### Prometheus 历史 / Prometheus History

保存连接在 Advanced settings 中分别配置 Prometheus URL 与精确 matcher；不同连接可使用不同历史后端。以下两个
CLI 参数是 legacy 临时启动连接的等价配置，必须成对提供，否则启动失败：
Saved connections configure their Prometheus URL and exact matcher independently in Advanced settings, so different
connections may use different history backends. These two CLI flags are the equivalent legacy-startup configuration and
are mandatory together; providing only one fails startup:

```bash
./target/release/rr-admin \
  --token-env RR_ADMIN_TOKEN \
  console \
  --listen 127.0.0.1:8080 \
  --peers 1@127.0.0.1:52001,2@127.0.0.1:52002 \
  --prometheus-url http://127.0.0.1:9090 \
  --prometheus-cluster cluster_id=prod-rr
```

- `--prometheus-url` 必须是带 host 的绝对 HTTP 或 HTTPS URL。<br />`--prometheus-url` must be an absolute HTTP or
  HTTPS URL with a host.
- `--prometheus-cluster` 必须恰好是一个非空 `label=value` 精确 matcher；label 使用 Prometheus 标识符规则，
  value 不能为空且不能再含 `=`。<br />`--prometheus-cluster` must be exactly one non-empty `label=value` matcher.
  The label follows Prometheus identifier rules; the value is non-empty and cannot contain another `=`.
- 显式 `source=PROMETHEUS` 但未配置历史时返回 `UNSUPPORTED` + `NONE`；实时缓存仍可通过
  `source=CONSOLE_MEMORY` 使用。<br />Explicit `source=PROMETHEUS` without history configuration returns `UNSUPPORTED`
  with `NONE`; live data remains available through `source=CONSOLE_MEMORY`.

### Raw-family 隔离与错误 / Raw-Family Isolation and Errors

历史查询使用固定 raw histogram/counter/gauge family 模板，不开放任意 PromQL，也不依赖 recording rules。
每个查询都附加精确集群 matcher 和受控 node/group 过滤。histogram 在 `histogram_quantile()` 前按 `le`、
`node_id`、`instance`、集群标签及原 family 维度聚合，因此不会跨集群/节点合并 bucket 或平均节点 P99。
History uses fixed raw histogram/counter/gauge family templates, not arbitrary PromQL or recording rules. Every query
includes the exact cluster matcher and controlled node/group filters. Histograms aggregate by `le`, `node_id`,
`instance`, the cluster label, and family dimensions before `histogram_quantile()`, so buckets are not merged across
clusters/nodes and node P99 values are never averaged.

每个返回序列还会再次校验集群标签。缺失、非法或为 0 的 `node_id`，以及同一 `node_id` 映射多个 `instance`，
都会产生 `UNMAPPED` 并扣留历史序列；不会按 `instance` 猜节点。集群不匹配或其他查询错误显示 `PARTIAL` 和
具体能力错误，不会静默回退成实时曲线；需要实时数据时显式切换 `CONSOLE_MEMORY`。
Every returned series is checked again for the configured cluster label. A missing, invalid, or zero `node_id`, or one
`node_id` mapped to multiple `instance` values, produces `UNMAPPED` and withholds history series; node identity is never
inferred from `instance`. Cluster mismatches and other query errors produce `PARTIAL` with explicit capability errors.
There is no silent fallback to a live-looking curve; select `CONSOLE_MEMORY` explicitly when live data is wanted.

每个 target 都应配置稳定唯一的 `node_id` 和同一精确集群标签，示例见
[Metrics Reference › Prometheus
Configuration](../reference/metrics-reference.md#prometheus-配置--prometheus-configuration)。实时抓取和历史响应
均限制为 8 MiB、100,000 个输入 series，超限显示错误而非截断成可信完整数据。
Configure every target with a stable, unique `node_id` and the same exact cluster label, as shown in [Metrics Reference
› Prometheus Configuration](../reference/metrics-reference.md#prometheus-配置--prometheus-configuration). Live scrapes
and history responses are limited to 8 MiB and 100,000 input series; exceeding either limit produces an error rather
than a truncated response presented as complete.

固定历史当前覆盖 server/proposal/apply/AppendEntries、请求与 apply/ingestor 吞吐，以及 snapshot 传输/安装。
replication lag 历史仍为 `UNSUPPORTED`，实时 lag 只接受同次 scrape 的 leader 样本，follower 缺席不补零。
节点资源等其他 family 不在历史模板中，可用实时缓存或直接使用 Prometheus/Grafana 查询。
Fixed history currently covers server/proposal/apply/AppendEntries, request and apply/ingestor throughput, and snapshot
transfer/install families. Replication-lag history remains `UNSUPPORTED`; live lag accepts only leader samples from the
same scrape and never zero-fills follower absence. Other families, including node resources, are not in the history
templates; use live retention or query Prometheus/Grafana directly for them.

## 指标解释与 P0 不支持项 / Metric Interpretation and P0 Unsupported Behavior

- 未知、旧节点未实现或平台不支持的值显示 `—`/`UNSUPPORTED`，绝不替换为 0。<br />Unknown, old-node-unimplemented,
  or platform-unsupported values are `—`/`UNSUPPORTED`, never zero.
- follower 缺失 leader-only matched/lag 不是 lag=0；legacy `leader_node_id=0` 归一化为无 leader/未知。<br />Missing
  leader-only matched/lag on followers is not zero lag; legacy `leader_node_id=0` normalizes to no leader/unknown.
- P99 从聚合 histogram bucket 计算，禁止平均节点 P99。server、proposal、apply 的 outcome/cohort 当前未对齐，
  均值和分位数都不得相减或堆叠，状态为 `UNALIGNED_COHORTS`。详见
  [写链路延迟证据](../reference/metrics-reference.md#写链路延迟证据--write-path-latency-evidence)。<br />P99 comes
  from aggregated histogram buckets, never averaged node quantiles. Server, proposal, and apply cohorts are not aligned;
  neither means nor quantiles may be subtracted or stacked. The state is `UNALIGNED_COHORTS`; see [Write-Path Latency
  Evidence](../reference/metrics-reference.md#写链路延迟证据--write-path-latency-evidence).
- 快照构建失败没有 group 标签，只能归到节点；Group × Node 失败单元格为 `UNSUPPORTED`。废弃的
  `snapshot_in_progress=false` 不表示进度，无快照也不显示 0%。<br />Snapshot build failures have no group label and
  remain node-level. Group × Node failure cells are `UNSUPPORTED`; the deprecated `snapshot_in_progress=false` is not
  progress, and no snapshot does not mean 0%.
- 节点资源指标按 `process`/`host`/`cgroup` 分 scope，网络 counter reset 安全处理，缺席不补零。<br />Node resource
  metrics keep `process`/`host`/`cgroup` scopes distinct, handle network counter resets safely, and omit unsupported
  values.
- P0 不提供 membership 修改、强制选主、TriggerSnapshot、DrainNode、offset reset 或进程重启按钮；唯一的集群
  reconcile POST 也是只读刷新，不执行这些控制操作。<br />P0 exposes no membership changes, forced leadership,
  TriggerSnapshot, DrainNode, offset reset, or process-restart actions. The cluster reconcile POST is a read-only
  refresh and performs none of those control operations.
- P0 不含任意 PromQL 代理、通知投递、多用户权限体系或 OIDC/SSO。多集群仅表示单 operator 的多个只读连接，
  所有 Console 用户仍只有 viewer 能力。<br />P0 includes no arbitrary PromQL proxy, notification delivery, multi-user
  authorization system, or OIDC/SSO. Multi-cluster means multiple read-only connections for one operator; every Console
  user remains viewer-only.

排查页面数据时先打开 **Sources**：检查启动模式、逐节点 Admin/observability 地址、最后成功/失败、能力矩阵、
留存来源和 correlation ID，再进入节点、组或生命周期证据。不要把采集缺失误判为 Raft 故障。
When investigating page data, open **Sources** first. Check startup mode, per-node Admin/observability addresses, last
success/failure, capability matrix, retention source, and correlation ID before inspecting node, group, or lifecycle
evidence. Do not mistake missing collection for a Raft failure.
