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

部署时同时携带二进制和静态导出目录，或用 `--static-dir` 指向导出位置。默认目录是源码树中的
`crates/rr-admin/console/out`，不是独立 Node.js 生产服务。
Ship the static export with the binary, or point `--static-dir` at its deployed location. The default is
`crates/rr-admin/console/out` in the source tree; no standalone Node.js production server is used.

### 本地 seed 模式 / Local Seed Mode

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

- loopback 的 viewer 认证和 HTTPS 可选；设置 `RR_CONSOLE_TOKEN` 即启用登录。/ Viewer authentication and HTTPS
  are optional on loopback; setting `RR_CONSOLE_TOKEN` enables login.
- viewer 登录换取 15 分钟内存 session；cookie 为 HttpOnly、SameSite=Strict，HTTPS 时增加 Secure。/ Viewer
  login issues a 15-minute in-memory session; cookies are HttpOnly and SameSite=Strict, with Secure under HTTPS.
- session 不持久化，Console 重启会撤销现有 session；reconcile POST 还检查 CSRF token 和同源 Origin。/ Sessions
  are not persisted, so a Console restart revokes them. Reconcile POST also checks the CSRF token and same-origin
  Origin.
- 节点 Admin token 只在 BFF 内；不要写入 URL、HTML、localStorage 或日志。/ Node Admin tokens remain inside the
  BFF and must never appear in URLs, HTML, localStorage, or logs.
- 节点 `/metrics` 当前可能未认证；用网络策略、TLS 或可信内网保护它，不由 Console 暴露原始指标代理。/ Node
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

- 必须提供 `--discovery-tls-ca`，或显式选择 `--discovery-allow-insecure`；二者互斥。/ Require either
  `--discovery-tls-ca` or explicit `--discovery-allow-insecure`; they are mutually exclusive.
- `--discovery-tls-cert` 与 `--discovery-tls-key` 必须成对，并同时提供 CA；`--discovery-tls-domain` 也要求 CA。/
  `--discovery-tls-cert` and `--discovery-tls-key` are a pair and require the CA; `--discovery-tls-domain` also
  requires the CA.
- `--discovery-username` 与 `--discovery-password-env` 必须成对且非空，并且认证必须基于 CA TLS。密码只从指定
  环境变量读取，不能放入 argv。/ `--discovery-username` and `--discovery-password-env` are a non-empty pair and
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

- discovery 是 roster 权威；若同时提供 peers，peers 仅作连通性种子，且 `--cluster-name` 必填。/ Discovery is
  the roster authority. When peers are also present, they are connectivity seeds only, and `--cluster-name` is required.
- 只信任显式 seed、受信目录记录，或经认证本机描述符返回的地址；普通 metrics 标签和未认证 HTTP 响应不能提升
  地址信任。/ Trust only explicit seeds, authenticated directory records, or addresses returned by an authenticated
  local descriptor. Metrics labels and unauthenticated HTTP responses cannot promote address trust.
- 校验 node ID、cluster UUID、目录 revision 和 TLS 身份后才可转发 Admin token；冲突的非空 UUID 以及空/非空
  UUID 混合必须隔离，不得合并。/ Verify node ID, cluster UUID, directory revision, and TLS identity before forwarding
  an Admin token. Conflicting non-empty UUIDs and mixed empty/non-empty UUIDs must be quarantined, never merged.
- 非 seed 节点只使用显式 admin/observability 地址；缺失时标记 `ENDPOINTS_UNAVAILABLE`，不做端口加减。/
  Non-seed nodes use only explicit Admin/observability addresses. Missing endpoints produce `ENDPOINTS_UNAVAILABLE`;
  port arithmetic is forbidden.
- lease 过期、watch 断开或 compaction 需要全量对账。非 seed 若 15 秒未通过权威对账/认证拓扑刷新信任，
  立即从可连接 roster 移除并暂停 Admin token 转发；显式 seed 不过期。不可达节点保留最后快照并标 stale，
  不直接从拓扑删除。/ Lease expiry, watch interruption, and compaction require full reconciliation. A non-seed
  that has not refreshed trust through authoritative reconciliation or authenticated topology for 15 seconds is removed
  from the dialable roster and Admin-token forwarding stops; explicit seeds do not expire. Unreachable nodes retain
  their last snapshot with stale evidence instead of disappearing from topology.

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

- 每次启动追加 boot ID、启动时间和 App/Rr/git/build 信息；优雅关闭补写结束时间和 `GRACEFUL`。/ Every start
  appends a boot ID, start time, and App/Rr/git/build information. Graceful shutdown adds an end time and `GRACEFUL`.
- 下次启动发现仍为 `RUNNING` 的旧记录时，将其改为 `ABNORMAL_OR_UNCERTAIN`，结束时间保持未知；不能据此断言
  crash 原因。/ A previous record still `RUNNING` at the next startup becomes `ABNORMAL_OR_UNCERTAIN` with no invented
  end time; this does not prove a crash reason.
- 持久 journal 每节点最多 100 个 boot，按新到旧分页；默认每页 50，最大 200。/ A persistent journal retains at
  most 100 boots per node, newest first. Pages default to 50 and are capped at 200.
- 复用同一目录会延续历史。换成空目录、删除 journal，或重置包含默认 journal 的整个 data dir，都会重置可见
  历史；需要跨数据重置保留历史时使用独立 journal dir。/ Reusing the same directory continues history. An empty
  replacement directory, journal deletion, or resetting a data dir that contains the default journal resets visible
  history. Use an independent journal dir when history must survive a data reset.
- journal 损坏、版本不支持或持久化失败会响亮报错；不要以伪造空历史替代恢复。/ Corrupt, unsupported-version,
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

成功的实时序列以 `retention_source=CONSOLE_MEMORY` 返回。事件环形缓冲最多 512 条，观测点最多 20,000 个；
没有固定时长保证，容量耗尽时淘汰最旧记录，重启全部清空。15m/1h/6h/24h 窗口只筛选已留存数据，不会回填
Console 启动前的平线。没有可用实时序列时，该响应为 `UNSUPPORTED` + `NONE`，不等于整个性能功能不存在。
Successful live series return `retention_source=CONSOLE_MEMORY`. At most 512 events and 20,000 observation points are
retained in bounded rings. There is no fixed duration guarantee; oldest records are evicted at capacity and all data is
lost on restart. The 15m/1h/6h/24h windows filter existing retention and never backfill a flat line from before Console
startup. If no live series is available, that response is `UNSUPPORTED` with `NONE`; it does not mean the entire
performance feature is absent.

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

以下两个参数必须成对提供，否则启动失败：
These two flags are mandatory together; providing only one fails startup:

```bash
./target/release/rr-admin \
  --token-env RR_ADMIN_TOKEN \
  console \
  --listen 127.0.0.1:8080 \
  --peers 1@127.0.0.1:52001,2@127.0.0.1:52002 \
  --prometheus-url http://127.0.0.1:9090 \
  --prometheus-cluster cluster_id=prod-rr
```

- `--prometheus-url` 必须是带 host 的绝对 HTTP 或 HTTPS URL。/ `--prometheus-url` must be an absolute HTTP or
  HTTPS URL with a host.
- `--prometheus-cluster` 必须恰好是一个非空 `label=value` 精确 matcher；label 使用 Prometheus 标识符规则，
  value 不能为空且不能再含 `=`。/ `--prometheus-cluster` must be exactly one non-empty `label=value` matcher.
  The label follows Prometheus identifier rules; the value is non-empty and cannot contain another `=`.
- 显式 `source=PROMETHEUS` 但未配置历史时返回 `UNSUPPORTED` + `NONE`；实时缓存仍可通过
  `source=CONSOLE_MEMORY` 使用。/ Explicit `source=PROMETHEUS` without history configuration returns `UNSUPPORTED`
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

- 未知、旧节点未实现或平台不支持的值显示 `—`/`UNSUPPORTED`，绝不替换为 0。/ Unknown, old-node-unimplemented,
  or platform-unsupported values are `—`/`UNSUPPORTED`, never zero.
- follower 缺失 leader-only matched/lag 不是 lag=0；legacy `leader_node_id=0` 归一化为无 leader/未知。/ Missing
  leader-only matched/lag on followers is not zero lag; legacy `leader_node_id=0` normalizes to no leader/unknown.
- P99 从聚合 histogram bucket 计算，禁止平均节点 P99。server、proposal、apply 的 outcome/cohort 当前未对齐，
  均值和分位数都不得相减或堆叠，状态为 `UNALIGNED_COHORTS`。详见
  [写链路延迟证据](../reference/metrics-reference.md#写链路延迟证据--write-path-latency-evidence)。/ P99 comes
  from aggregated histogram buckets, never averaged node quantiles. Server, proposal, and apply cohorts are not aligned;
  neither means nor quantiles may be subtracted or stacked. The state is `UNALIGNED_COHORTS`; see [Write-Path Latency
  Evidence](../reference/metrics-reference.md#写链路延迟证据--write-path-latency-evidence).
- 快照构建失败没有 group 标签，只能归到节点；Group × Node 失败单元格为 `UNSUPPORTED`。废弃的
  `snapshot_in_progress=false` 不表示进度，无快照也不显示 0%。/ Snapshot build failures have no group label and
  remain node-level. Group × Node failure cells are `UNSUPPORTED`; the deprecated `snapshot_in_progress=false` is not
  progress, and no snapshot does not mean 0%.
- 节点资源指标按 `process`/`host`/`cgroup` 分 scope，网络 counter reset 安全处理，缺席不补零。/ Node resource
  metrics keep `process`/`host`/`cgroup` scopes distinct, handle network counter resets safely, and omit unsupported
  values.
- P0 不提供 membership 修改、强制选主、TriggerSnapshot、DrainNode、offset reset 或进程重启按钮；唯一的集群
  reconcile POST 也是只读刷新，不执行这些控制操作。/ P0 exposes no membership changes, forced leadership,
  TriggerSnapshot, DrainNode, offset reset, or process-restart actions. The cluster reconcile POST is a read-only
  refresh and performs none of those control operations.
- P0 不含任意 PromQL 代理、通知投递、多集群、本地用户体系或 OIDC/SSO。所有 Console 用户只有 viewer 能力。/
  P0 includes no arbitrary PromQL proxy, notification delivery, multi-cluster management, local user database, or
  OIDC/SSO. Every Console user has viewer-only capability.

排查页面数据时先打开 **Sources**：检查启动模式、逐节点 Admin/observability 地址、最后成功/失败、能力矩阵、
留存来源和 correlation ID，再进入节点、组或生命周期证据。不要把采集缺失误判为 Raft 故障。
When investigating page data, open **Sources** first. Check startup mode, per-node Admin/observability addresses, last
success/failure, capability matrix, retention source, and correlation ID before inspecting node, group, or lifecycle
evidence. Do not mistake missing collection for a Raft failure.
