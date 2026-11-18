---
sidebar_position: 5
---

# 安全模型 / Security Model

安全层包括启动安全门、TLS/mTLS、bearer token、速率限制和审计日志。证书细节见 [TLS 指南](./tls.md)，事件字段见
[审计日志](./audit-log.md)。

The security layer consists of startup gates, TLS/mTLS, bearer tokens, rate limiting, and audit logging. See the
[TLS guide](./tls.md) for certificates and [Audit Log](./audit-log.md) for event fields.

## 生产清单 / Production Checklist

| 项 / Item | 生产要求 / Production Requirement | 执行点 / Enforcement |
| :--- | :--- | :--- |
| 启动安全门 G1–G7 / Startup gates | 全部通过，不使用 `--allow-insecure` / All pass; no `--allow-insecure` | `RrNode::new` → `validate_security_invariants` |
| 节点间 Raft RPC / Inter-node Raft | TLS + `require_client_cert = true` / TLS + mTLS | G1 / G2 |
| Admin / Client 服务 / services | TLS；Admin 必须独立 token，Client token 按需配置 / TLS; Admin token required, Client token optional | G3 / G4 / G5 |
| Browser → Console | 非 loopback 必须 HTTPS + viewer token；节点 Admin token 只留在 BFF，从不下发浏览器 / Non-loopback requires HTTPS + viewer token; node Admin tokens stay in the BFF and are never sent to browsers | `ConsoleArgs::validate`；BFF 凭证隔离 / BFF credential isolation |
| Console viewer session | 15 分钟内存 session；HttpOnly、SameSite=Strict，HTTPS 时 Secure；重启即失效 / 15-minute in-memory session; HttpOnly, SameSite=Strict, Secure under HTTPS; restart revokes it | `AuthState` |
| Console reconcile POST | 仅只读刷新；必须同源 Origin，启用 viewer 认证时还须有效 `X-Console-CSRF` / Read-only refresh only; same-origin Origin required, plus valid `X-Console-CSRF` when viewer auth is enabled | `http::reconcile` |
| 速率限制 / Rate limiting | Admin 可从 10–100 rps 起步，按调用方隔离 / Start Admin around 10–100 rps and isolate callers as needed | 默认不限流，需显式配置 / Unlimited by default |
| 审计日志 / Audit log | `RUST_LOG="warn,rr::audit=info"`，路由到独立 append-only sink / Route to a separate append-only sink | 运维配置 / Operator configuration |
| TCP Raft transport | 不用于生产 / Not for production | G6 / G7 |
| 成员变更 / Membership | 不常规使用 `--force`；缩容后至少 2 voter / No routine `--force`; keep at least two voters | 服务端守卫 / Server-side guard |

## 启动安全门 G1–G7 / Startup Safety Gates

`RrNode::new` 在绑定 socket 前调用 `validate_security_invariants`。G7 无条件先执行；当 `allow_insecure = false` 时再聚合
检查 G1–G6，一次列出所有违规。

`RrNode::new` calls `validate_security_invariants` before binding sockets. G7 always runs first; with
`allow_insecure = false`, G1–G6 are then aggregated so all violations appear together.

| Gate | 检查内容 / Check | `allow_insecure` 可绕过 / Bypassable |
| :--- | :--- | :--- |
| **G1** | RaftService 未配置 TLS / RaftService without TLS | 是 / Yes |
| **G2** | Raft TLS 未要求客户端证书；Raft 没有 bearer-token 通道，mTLS 是唯一对端身份检查 / Raft TLS without mTLS; mTLS is the only Raft peer-identity check | 是 / Yes |
| **G3** | 配置 bearer token 但未启用 TLS，token 会进入明文 metadata / Bearer tokens without TLS expose them in plaintext metadata | 是 / Yes |
| **G4** | AdminService 未启用 TLS / AdminService without TLS | 是 / Yes |
| **G5** | AdminService 未配置 `admin_token` / AdminService without `admin_token` | 是 / Yes |
| **G6** | `raft_transport = tcp` 没有 TLS、对端认证或限流，必须显式 opt-in / TCP Raft has no TLS, peer authentication, or rate limiting and requires opt-in | 必须设置 / Required |
| **G7** | TCP Raft bind 必须解析为 loopback；`0.0.0.0`、`::` 和无法解析的地址均拒绝 / TCP Raft bind must resolve to loopback; wildcard and unparsable addresses fail closed | 否，始终执行 / No, always enforced |

### insecure 开关 / The Insecure Override

仅在本地开发、examples、CI 中使用 `--allow-insecure`、`RR_ALLOW_INSECURE=1` 或 `[node].allow_insecure = true`。启用后启
动日志列出被绕过的门，指标端口暴露 `rr_security_insecure_mode = 1`；安全模式为 0。生产应对该指标告警。G7 不可绕过。

Use `--allow-insecure`, `RR_ALLOW_INSECURE=1`, or `[node].allow_insecure = true` only for local development, examples,
and CI. Startup logs list bypassed gates, and the metrics endpoint exposes `rr_security_insecure_mode = 1` (`0` in
secure mode). Alert on this metric in production. G7 cannot be bypassed.

## 传输安全与认证 / Transport Security and Authentication

三个 gRPC 面的认证方式不同：

The three gRPC surfaces deliberately use different authentication models:

| 服务 / Service | 加密 / Encryption | 认证 / Authentication |
| :--- | :--- | :--- |
| `RaftService` | TLS | 仅 mTLS；无 bearer-token 路径 / mTLS only; no bearer-token path |
| `ClientService` | TLS | `client_token` 可选；未设即开放，仅限 dev / Optional `client_token`; unset means open, dev only |
| `AdminService` | TLS | 生产必须 `admin_token` / `admin_token` required in production |

Admin 与 Client 使用独立 token，分别发给运维和应用。token 比较使用常数时间比较。拦截器顺序固定为认证先于限流，因此认证失
败不会消耗业务限流配额；TLS 证书失败更在 gRPC 拦截器之前的握手阶段被拒绝。

Admin and Client use separate tokens for operators and applications. Token comparison is constant-time. Authentication
runs before rate limiting, so rejected credentials do not consume application rate-limit quota; certificate failures are
rejected even earlier during the TLS handshake.

TOML 的 `[auth]` 只保存环境变量名，不保存 secret：

The TOML `[auth]` section stores environment-variable names, never the secrets themselves:

```toml
[auth]
admin_token_env = "RR_ADMIN_TOKEN"
client_token_env = "RR_CLIENT_TOKEN"
```

`rr-admin` 用 `--token-env RR_ADMIN_TOKEN` 读取 Admin token，REPL 另用 `--client-token-env RR_CLIENT_TOKEN`。证书字段和
轮换见 [TLS 指南](./tls.md)。

`rr-admin` reads the Admin token through `--token-env RR_ADMIN_TOKEN`; the REPL uses
`--client-token-env RR_CLIENT_TOKEN` separately. See the [TLS guide](./tls.md) for certificate fields and rotation.

### Cluster Admin Console 的两条边界 / Two Cluster Admin Console Boundaries

Browser→Console 与 Console BFF→AdminService 是两条独立的 TLS 和认证边界。前者加密浏览器到同源 Console 的
HTTP/JSON/SSE，并用 viewer token 登录；后者加密 BFF 到节点 AdminService 的 gRPC，并使用节点 Admin token。
一条边界已启用 TLS 不代表另一条也已安全。

Browser→Console and Console BFF→AdminService are independent TLS and authentication boundaries. The first protects
same-origin Console HTTP/JSON/SSE with HTTPS and viewer-token login; the second protects BFF-to-node AdminService gRPC
with TLS and the node Admin token. TLS on one boundary does not secure the other.

| 连接 / Connection | 传输 / Transport | 认证与凭证位置 / Authentication and Credential Location |
| :--- | :--- | :--- |
| Browser → Console BFF | Console 自身 HTTPS；非 loopback 必须启用 / Console HTTPS; required for non-loopback | viewer token 只用于登录，换取 15 分钟内存 session；不是节点 Admin token / Viewer token login issues a 15-minute in-memory session; it is not the node Admin token |
| Console BFF → AdminService | gRPC TLS，按节点要求使用 mTLS / gRPC TLS, with mTLS as required by nodes | 节点 Admin bearer token 只由 BFF 持有并附加到上游 RPC，不下发浏览器 / The BFF alone holds the node Admin bearer token and attaches it to upstream RPCs; browsers never receive it |

session 不持久化，Console 重启会撤销全部 session。cookie 为 HttpOnly、SameSite=Strict，HTTPS 时增加 Secure。
`POST /api/v1/console/sources/reconcile` 虽只触发只读刷新，仍要求 `Origin` 与当前 scheme/host 完全一致；启用
viewer 认证时还要求与 session 匹配的 `X-Console-CSRF`。这些检查不增加任何集群写能力。

Sessions are not persisted, and a Console restart revokes all of them. Cookies are HttpOnly and SameSite=Strict, with
Secure under HTTPS. Although `POST /api/v1/console/sources/reconcile` only triggers a read-only refresh, it requires an
`Origin` exactly matching the current scheme/host; with viewer authentication enabled, it also requires an
`X-Console-CSRF` matching the session. These checks add no cluster write capability.

完整启动参数见 [TLS 指南](./tls.md#cluster-admin-console-的两条-tls-边界--two-cluster-admin-console-tls-boundaries)，
Console 行为见 [集群管理控制台](../operations/cluster-admin-console.md)。

See the [TLS guide](./tls.md#cluster-admin-console-的两条-tls-边界--two-cluster-admin-console-tls-boundaries) for
startup flags and the [Cluster Admin Console guide](../operations/cluster-admin-console.md) for Console behavior.

## 速率限制 / Rate Limiting

限流只作用于 AdminService 与 ClientService，RaftService 永不设限。配额耗尽返回 gRPC `RESOURCE_EXHAUSTED`（code 8）。

Rate limits apply only to AdminService and ClientService, never RaftService. Exhaustion returns gRPC
`RESOURCE_EXHAUSTED` (code 8).

| 配置 / Knob | CLI / Env | 默认 / Default |
| :--- | :--- | :--- |
| `admin_rps` | `--admin-rps` / `RR_ADMIN_RPS` | 不限 / Unlimited |
| `client_rps` | `--client-rps` / `RR_CLIENT_RPS` | 不限 / Unlimited |
| `burst` | `[rate_limit].burst` | `max(rps, 10)` |
| `per_caller_admin_rps` | `--per-caller-admin-rps` / `RR_PER_CALLER_ADMIN_RPS` | 关闭 / Disabled |
| `per_caller_client_rps` | `--per-caller-client-rps` / `RR_PER_CALLER_CLIENT_RPS` | 关闭 / Disabled |
| `per_caller_burst` | `[rate_limit].per_caller_burst` | `max(per_caller_rps, 10)` |

设置 `per_caller_*` 后，已认证调用方按 bearer token 的 siphash 指纹获得独立桶。未认证请求和调用方表已满时的溢出请求使用
对应全局桶，因此必须同时配置 `admin_rps` 或 `client_rps`，否则配置校验拒绝启动。空闲超过 300 秒的调用方条目会清理，拒绝
事件写 `op="rate_limit", result="rejected"` 审计日志。

With `per_caller_*` set, authenticated callers receive individual buckets keyed by a siphash fingerprint of the bearer
token. Unauthenticated requests and callers overflowing the per-caller map use the matching global bucket, so
`admin_rps` or `client_rps` is also required; validation rejects the configuration otherwise. Caller entries idle for
more than 300 seconds are evicted, and rejects emit `op="rate_limit", result="rejected"` audit events.

## 审计日志 / Audit Logging

认证拒绝、限流拒绝和 AdminService RPC 生命周期事件写入专用 tracing target `rr::audit`。强制成员变更额外写 warn 级
`result="force_accepted"`，ingestor offset 人工覆盖也写 warn 级事件，`DrainNode` 按组记录领导权转移。

Authentication rejects, rate-limit rejects, and AdminService RPC lifecycle events use the dedicated tracing target
`rr::audit`. Forced membership changes add a warn-level `result="force_accepted"` event, manual ingestor-offset
overrides are warn-level, and `DrainNode` records leadership transfers per group.

使用 `RUST_LOG="warn,rr::audit=info"` 同时保留 warn 拒绝事件和 info RPC 事件。schema、sink 和限制见
[审计日志指南](./audit-log.md)。

Use `RUST_LOG="warn,rr::audit=info"` to retain both warn-level rejects and info-level RPC events. See the
[audit-log guide](./audit-log.md) for schema, sinks, and limitations.

## 成员变更护栏 / Membership Safety Guard

`MIN_VOTERS_AFTER_REMOVAL = 2` 由服务端和 `rr-admin node decommission` 预检共享。openraft joint consensus 保证成员变更的
一致性，但不保证变更后的可用性；该护栏阻止常规操作缩成单点。

`MIN_VOTERS_AFTER_REMOVAL = 2` is shared by the server and the `rr-admin node decommission` precheck. Openraft joint
consensus keeps membership changes consistent but does not guarantee availability afterward; this guard prevents routine
changes from creating a single point of failure.

| 变更 / Change | 无 force / No Force | 有 force / With Force |
| :--- | :--- | :--- |
| 3→2 | 允许 / Allowed | 允许 / Allowed |
| 3→1 / 2→1 | `failed_precondition` | 允许并写 warn 审计 / Allowed with warn audit |
| 1→1 换 voter / voter swap | 拒绝 / Rejected | 允许并写 warn 审计 / Allowed with warn audit |
| n→0 | `invalid_argument` | 仍拒绝；不能绕过 / Still rejected; not bypassable |
| 2→3 等扩容 / growth | 允许 / Allowed | 允许 / Allowed |

只有运行新二进制的 server 才执行新护栏。滚动升级期间不要依赖它：旧 server 的 proto3 解码会忽略未知 `force` 字段。不要把
`--force` 当作常规缩容工具。

Only servers running the new binary enforce the new guard. Do not rely on it during a rolling upgrade: an old server's
proto3 decoding ignores the unknown `force` field. Never use `--force` for routine shrink operations.

## 实验性 TCP Raft / Experimental TCP Raft Transport

`RaftTransport::Tcp`（`--raft-transport tcp`）只承载 AppendEntries/Vote，InstallSnapshot 始终回退 gRPC，因此 gRPC
TLS/mTLS 门仍适用。TCP frame 协议没有 TLS、对端认证或限流，任何能到达端口的人都可注入帧。

`RaftTransport::Tcp` (`--raft-transport tcp`) carries only AppendEntries/Vote; InstallSnapshot always falls back to gRPC
, so gRPC TLS/mTLS gates still apply. The TCP frame protocol has no TLS, peer authentication, or rate limiting; anyone
who can reach the port can inject frames.

因此必须显式 `--allow-insecure`，且 bind 只能为 loopback。TCP 端口默认从 gRPC 端口 +10000 派生，可用 `--raft-tcp-port` /
`RR_RAFT_TCP_PORT` 覆盖。库用户直接调用 `TcpRaftServer::bind` 会绕过这些启动门，不要绑定非 loopback。生产使用默认 gRPC
transport。

It therefore requires explicit `--allow-insecure` and a loopback bind. The TCP port defaults to gRPC port +10000 and can
be overridden with `--raft-tcp-port` / `RR_RAFT_TCP_PORT`. Library users calling `TcpRaftServer::bind` directly bypass
the startup gates; never bind it off-loopback. Use the default gRPC transport in production.

## 本地开发 / Insecure Local Development

本地可使用明文 + `--allow-insecure`，或生成开发证书演练真实 TLS：

For local work, use either plaintext + `--allow-insecure` or generate development certificates to rehearse real TLS:

```bash
./scripts/gen-dev-certs.sh dev-certs 3
```

脚本生成自签名开发 CA（RSA-4096，10 年）和每节点 cert/key（RSA-2048，1 年），SAN 覆盖 `localhost`、`127.0.0.1`、`::1`、
`rr-N`、`nodeN.rr.local`，key 权限为 600。

The script creates a self-signed development CA (RSA-4096, ten years) and per-node cert/key pairs (RSA-2048, one year),
with SANs for `localhost`, `127.0.0.1`, `::1`, `rr-N`, and `nodeN.rr.local`; keys use mode 600.

> **这些证书不得用于生产。** 私钥不加密，SAN 只覆盖本地主机名。完整启动命令见 [TLS 指南](./tls.md)。
>
> **Never use these certificates in production.** Private keys are unencrypted and SANs cover local hostnames only. See
> the [TLS guide](./tls.md) for full launch commands.

## 相关 / Related

- [TLS 配置与轮换 / TLS configuration and rotation](./tls.md)
- [审计事件与 sink / Audit events and sinks](./audit-log.md)
- [整体部署 / Full deployment](./deployment.md)
- [ADR 索引 / ADR index](../architecture/design-decisions.md)
