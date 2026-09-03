---
sidebar_position: 5
---

# 安全模型 / Security Model

rr 的安全层由五部分组成：**启动安全门**（不安全配置直接拒绝启动）、**传输加密**（TLS/mTLS）、**bearer token 认证**、**速率限制**、**审计日志**。本页给出整体模型与生产清单。TLS 证书配置的细节见 [tls.md](./tls.md)，审计事件 schema 与路由见 [audit-log.md](./audit-log.md)——本页不重复这些内容。

---

rr's security layer has five parts: **startup safety gates** (insecure configs refuse to boot), **transport encryption** (TLS/mTLS), **bearer-token auth**, **rate limiting**, and **audit logging**. This page gives the overall model and a production checklist. See [tls.md](./tls.md) for certificate configuration and [audit-log.md](./audit-log.md) for the audit event schema — those details are not repeated here.

## 生产清单 / Production checklist

| 项 / Item | 生产要求 / Production requirement | 执行点 / Enforced by |
| :--- | :--- | :--- |
| 启动安全门 G1–G7 | 全部通过；不要使用 `--allow-insecure` | `RrNode::new` → `validate_security_invariants` |
| 节点间 Raft RPC | TLS **且** `require_client_cert = true`（mTLS） | G1 / G2（硬性拒绝启动） |
| Admin / Client 服务 | TLS + 独立的 `admin_token`（可选 `client_token`） | G3 / G4 / G5 |
| 速率限制 | admin 建议 10–100 rps；按需开启 per-caller 模式 | 运维配置（默认不限流） |
| 审计日志 | `RUST_LOG="warn,rr::audit=info"`，路由到独立 append-only sink | 运维配置 |
| TCP raft transport | **不要用于生产**（明文、无认证） | G6 / G7 |
| 成员变更 | 不要常规使用 `--force`；缩容后 voter ≥ 2 | 服务端 quorum 守卫（ADR A-0012） |

---

| Item | Production requirement | Enforced by |
| :--- | :--- | :--- |
| Startup gates G1–G7 | All pass; do not use `--allow-insecure` | `RrNode::new` → `validate_security_invariants` |
| Inter-node Raft RPC | TLS **and** `require_client_cert = true` (mTLS) | G1 / G2 (hard startup refusal) |
| Admin / Client services | TLS + a dedicated `admin_token` (optional `client_token`) | G3 / G4 / G5 |
| Rate limiting | 10–100 rps suggested for admin; enable per-caller mode as needed | Operator config (unlimited by default) |
| Audit log | `RUST_LOG="warn,rr::audit=info"` routed to a separate append-only sink | Operator config |
| TCP raft transport | **Not for production** (plaintext, no auth) | G6 / G7 |
| Membership changes | No routine `--force`; keep ≥ 2 voters after removal | Server-side quorum guard (ADR A-0012) |

## 启动安全门 G1–G7 / Startup safety gates

`RrNode::new` 在任何 socket 绑定之前执行 `validate_security_invariants`（`crates/rr-server/src/node.rs:45`）。校验分两阶段（ADR A-0014）：**G7 无条件先执行**，随后若 `allow_insecure = false` 再聚合检查 G1–G6——所有违规一次性列出，方便运维一次修完。

---

`RrNode::new` runs `validate_security_invariants` before any socket binds (`crates/rr-server/src/node.rs:45`). Evaluation is two-phase (ADR A-0014): **G7 runs unconditionally first**, then, unless `allow_insecure` is set, G1–G6 are evaluated and *all* violations are aggregated into a single error so operators fix the config in one pass.

| Gate | 检查内容 / Check | `allow_insecure` 可绕过？ / Bypassable? |
| :--- | :--- | :--- |
| **G1** | `RaftService` 已启用但未配置 TLS——节点间 RPC 必须通过 TLS 建立对端身份 / RaftService enabled without TLS | ✅ |
| **G2** | RaftService 有 TLS 但 `require_client_cert = false`——RaftService 没有 bearer-token 通道，**mTLS 是唯一的对端身份检查** / TLS without mTLS; mTLS is the ONLY peer-identity check for RaftService | ✅ |
| **G3** | 配置了 bearer token（admin 或 client）但未启用 TLS——token 会以明文出现在 gRPC metadata 里 / Bearer tokens configured without TLS (tokens would travel in plaintext) | ✅ |
| **G4** | `AdminService` 已启用但无 TLS——leader 转移、快照触发等能力暴露在明文端口上即 DoS 向量 / AdminService without TLS | ✅ |
| **G5** | `AdminService` 已启用但未设 `admin_token`——任何能到达 admin 端口的人都能转移 leader / AdminService without `admin_token` | ✅ |
| **G6** | `raft_transport = tcp`——TCP frame 协议**没有 TLS 也没有任何对端认证**，即使 gRPC 面 TLS/mTLS 完全合规也必须显式 opt-in / TCP raft transport always requires explicit opt-in | 必须显式设置 / Required |
| **G7** | TCP raft 的 bind 地址必须解析为 **loopback**（`127.0.0.0/8` 或 `::1`；`0.0.0.0`/`::` 是 unspecified，拒绝）。无法解析的地址按 fail-closed 处理 / TCP bind address must be loopback; unparsable = fail closed | ❌ **硬性门，任何模式下都执行** / Hard gate, always enforced |

### `--allow-insecure` 逃生开关 / The `allow_insecure` escape hatch

仅在**本地开发、examples、CI** 中使用。三种等效打开方式：`--allow-insecure`（CLI）、`RR_ALLOW_INSECURE=1`（环境变量）、`[node].allow_insecure = true`（TOML）。

开启后：

- 启动时发出一条多行 `warn!`，列出每个被绕过的门和绑定的 `listen_addr`（`node.rs:216-231`）；
- 当配置了 `metrics_port` 时，暴露 Prometheus gauge **`rr_security_insecure_mode = 1`**（安全模式为 `0`，`node.rs:238`）——生产监控应对 `rr_security_insecure_mode == 1` 告警；
- **G7 不被绕过**：明文 TCP raft 监听在任何模式下都不允许绑定非 loopback 地址。

---

Use it **only for local dev, examples, and CI**. Three equivalent switches: `--allow-insecure` (CLI), `RR_ALLOW_INSECURE=1` (env), `[node].allow_insecure = true` (TOML).

When enabled:

- A multi-line `warn!` at startup lists every bypassed gate and the bound `listen_addr` (`node.rs:216-231`);
- When `metrics_port` is configured, a Prometheus gauge **`rr_security_insecure_mode = 1`** is exposed (`0` in secure mode, `node.rs:238`) — production monitoring should alert on `rr_security_insecure_mode == 1`;
- **G7 is not bypassed**: a plaintext TCP raft listener may never bind off-loopback, in any mode.

## 传输安全与认证 / Transport security & authentication

三个 gRPC 面的安全模型**不对称**，这是有意设计：

| 服务 / Service | 加密 / Encryption | 认证 / Authentication |
| :--- | :--- | :--- |
| `RaftService`（节点间 AppendEntries / Vote / InstallSnapshot） | TLS | **仅 mTLS**（`require_client_cert = true`）；没有 bearer-token 通道 |
| `ClientService`（应用读写） | TLS | `client_token`（可选；未设 = 开放，仅限 dev） |
| `AdminService`（运维操作，建议独立端口） | TLS | `admin_token`（生产**必须**设置，见 G5） |

Admin 与 Client 使用**两个独立 token**（`AuthConfig { admin_token, client_token }`，`crates/rr-network/src/auth.rs:41`）：admin token 发给 ops/SRE 角色，client token 发给应用流量，泄漏半径互不影响。token 比较使用常数时间比较（`constant_time_eq`）以缓解计时攻击。

拦截器链顺序固定为 **认证先于限流**——`ChainedInterceptor { first: BearerInterceptor, second: RateLimitInterceptor }`（`node.rs:484-488` client 面、`:556-560` admin 面）。因此未认证/无效证书的调用**不消耗**限流配额：垃圾流量无法耗尽你的 admin 配额。

Token 绝不写入配置文件：TOML 的 `[auth]` 段只存**环境变量名**（`admin_token_env = "RR_ADMIN_TOKEN"` / `client_token_env = "RR_CLIENT_TOKEN"`），真正的 secret 从环境或 secrets manager 注入。

TLS 证书字段、对称 mTLS 语义、证书轮换流程见 [tls.md](./tls.md)。

---

The three gRPC surfaces have deliberately **asymmetric** security models:

| Service | Encryption | Authentication |
| :--- | :--- | :--- |
| `RaftService` (inter-node AppendEntries / Vote / InstallSnapshot) | TLS | **mTLS only** (`require_client_cert = true`); no bearer-token path |
| `ClientService` (application reads/writes) | TLS | `client_token` (optional; unset = open, dev only) |
| `AdminService` (ops; separate port recommended) | TLS | `admin_token` (**required** in production, see G5) |

Admin and Client use **two independent tokens** (`AuthConfig { admin_token, client_token }`, `crates/rr-network/src/auth.rs:41`): the admin token goes to ops/SRE, the client token to application traffic, so a leak of one does not compromise the other. Token comparison is constant-time (`constant_time_eq`) to mitigate timing attacks.

The interceptor chain is fixed as **auth before rate limiting** — `ChainedInterceptor { first: BearerInterceptor, second: RateLimitInterceptor }` (`node.rs:484-488` for client, `:556-560` for admin). Unauthenticated or bad-cert callers therefore do **not** consume rate-limit quota: garbage traffic cannot exhaust your admin budget.

Tokens never live in the config file: the TOML `[auth]` section stores **environment variable names** (`admin_token_env = "RR_ADMIN_TOKEN"` / `client_token_env = "RR_CLIENT_TOKEN"`); the actual secrets come from the environment or a secrets manager.

For certificate fields, symmetric mTLS semantics, and rotation, see [tls.md](./tls.md).

## 速率限制 / Rate limiting

`crates/rr-network/src/rate_limit.rs` 实现了基于 `tokio::sync::Semaphore` + 后台补充任务的令牌桶。**RaftService 永不设限**——给 Raft RPC 节流会触发选举超时、破坏集群稳定性；限流只作用于 AdminService 与 ClientService。配额耗尽时客户端收到 gRPC `RESOURCE_EXHAUSTED`（code 8）。

---

`crates/rr-network/src/rate_limit.rs` implements a token bucket on `tokio::sync::Semaphore` with a background refill task. **RaftService is never rate-limited** — throttling Raft RPCs causes election timeouts and destabilizes consensus; limits apply only to AdminService and ClientService. On exhaustion the client gets gRPC `RESOURCE_EXHAUSTED` (code 8).

| 配置 / Knob | CLI / Env | 默认 / Default |
| :--- | :--- | :--- |
| `admin_rps` | `--admin-rps` / `RR_ADMIN_RPS` | `None`（不限 / unlimited） |
| `client_rps` | `--client-rps` / `RR_CLIENT_RPS` | `None` |
| `burst` | `[rate_limit].burst` | `max(rps, 10)` |
| `per_caller_admin_rps` | `--per-caller-admin-rps` / `RR_PER_CALLER_ADMIN_RPS` | `None` |
| `per_caller_client_rps` | `--per-caller-client-rps` / `RR_PER_CALLER_CLIENT_RPS` | `None` |
| `per_caller_burst` | `[rate_limit].per_caller_burst` | `max(per_caller_rps, 10)` |

**Per-caller 模式（ADR A-0005）**：设置 `per_caller_*` 后，每个已认证调用方（以 bearer token 的 siphash 指纹 `caller_token_fp` 标识）获得独立令牌桶，解决"noisy neighbour"问题；此时该服务的全局 `admin_rps`/`client_rps` 被忽略。未认证请求回落到全局桶。空闲超过 300s 的调用方条目会被自动清理。每次拒绝都会写一条 `op="rate_limit", result="rejected"` 的审计事件。

**Per-caller mode (ADR A-0005)**: with `per_caller_*` set, each authenticated caller (identified by the siphash fingerprint `caller_token_fp` of its bearer token) gets its own bucket, isolating noisy neighbours; the service's global `admin_rps`/`client_rps` is then ignored. Unauthenticated requests fall through to the global bucket. Caller entries idle for more than 300s are evicted. Every reject emits an `op="rate_limit", result="rejected"` audit event.

## 审计日志 / Audit logging

所有认证拒绝（`op="auth"`）、限流拒绝（`op="rate_limit"`）、AdminService RPC 生命周期（`started` / `succeeded` / `failed`）都发射到专用 tracing target **`rr::audit`**（`crates/rr-network/src/audit.rs:32`）。值得注意的审计点：

- **成员变更 `force` 放行**：`ChangeMembership` 被 `--force` 强制绕过 quorum 守卫时，额外产生一条 **warn 级** `result="force_accepted"` 事件，携带完整的 target voter 集合（`crates/rr-server/src/admin.rs:609-624`）——所有被强制绕过的守卫都可以在审计流中 grep 出来；
- **`reset_ingestor_offset`**：ingestor offset 的人工覆盖以 warn 级记录（`admin.rs:698-707`）；
- **`DrainNode`**：编排式关停的领导权转移逐组记录。

事件 schema、`EnvFilter` 路由（`RUST_LOG="warn,rr::audit=info"`）、保留期建议与已知限制见 [audit-log.md](./audit-log.md)。

---

All auth rejects (`op="auth"`), rate-limit rejects (`op="rate_limit"`), and AdminService RPC lifecycle events (`started` / `succeeded` / `failed`) are emitted on the dedicated tracing target **`rr::audit`** (`crates/rr-network/src/audit.rs:32`). Notable audit points:

- **Membership `force` overrides**: when `ChangeMembership` bypasses the quorum guard via `--force`, an extra **warn-level** `result="force_accepted"` event carries the exact target voter set (`crates/rr-server/src/admin.rs:609-624`) — every forced override is greppable in the audit stream;
- **`reset_ingestor_offset`**: manual ingestor-offset overrides are logged at warn level (`admin.rs:698-707`);
- **`DrainNode`**: orchestrated-shutdown leadership transfers are logged per group.

For the event schema, `EnvFilter` routing (`RUST_LOG="warn,rr::audit=info"`), retention guidance, and known limitations, see [audit-log.md](./audit-log.md).

## 成员变更安全护栏（ADR A-0012）/ Membership safety guard

`change_membership` 的服务端守卫常量为 **`MIN_VOTERS_AFTER_REMOVAL = 2`**（`crates/rr-core/src/membership.rs:19`），由 `AdminServiceImpl::change_membership`（`admin.rs:560-644`）与 `rr-admin node decommission` 的 client 侧预检共享同一定义。openraft 的 joint consensus 保证变更*正确*（不脑裂），但不保证*可用性*——这个守卫挡的是"可用性自杀"。

判定真值表：

| 变更 / Change | 无 force / No force | 有 force / With force |
| :--- | :--- | :--- |
| 3→2（= 下限） | ✅ 允许 | ✅ |
| 3→1 / 2→1（结果 SPOF） | ❌ `failed_precondition` | ✅ + warn 级审计 |
| 1→1（单节点换 voter） | ❌ | ✅ + warn 级审计 |
| n→0（拆组） | ❌ `invalid_argument` | ❌ **`force` 不能绕过**——teardown 走 `node-reset` |
| 2→3 等扩容 | ✅ | ✅（守卫只约束下限） |

注意：守卫只在运行新二进制的 server 上生效——滚动升级期间不要依赖它（proto3 会静默丢弃旧 server 不认识的 `force` 字段）。

---

The server-side guard constant is **`MIN_VOTERS_AFTER_REMOVAL = 2`** (`crates/rr-core/src/membership.rs:19`), shared between `AdminServiceImpl::change_membership` (`admin.rs:560-644`) and the client-side precheck in `rr-admin node decommission`. openraft's joint consensus keeps membership changes *correct* (no split-brain) but says nothing about *availability* — this guard blocks "availability suicide".

Truth table:

| Change | No force | With force |
| :--- | :--- | :--- |
| 3→2 (at the floor) | ✅ allowed | ✅ |
| 3→1 / 2→1 (resulting SPOF) | ❌ `failed_precondition` | ✅ + warn-level audit |
| 1→1 (single-node voter swap) | ❌ | ✅ + warn-level audit |
| n→0 (teardown) | ❌ `invalid_argument` | ❌ **`force` cannot bypass** — use `node-reset` for teardown |
| 2→3 and other growth | ✅ | ✅ (only the floor is guarded) |

Caveat: the guard is active only on servers running the new binary — do not rely on it during a rolling upgrade (proto3 silently drops the unknown `force` field on old servers).

## 实验性 TCP raft transport / Experimental TCP raft transport

`RaftTransport::Tcp`（`--raft-transport tcp`）是一个轻量二进制 frame 协议（`RRAF` magic + version，26 字节头），只承载 AppendEntries/Vote；**InstallSnapshot 始终回退 gRPC**（`crates/rr-raft/src/tcp_transport.rs:1-4`），因此 TCP 模式下 gRPC `RaftService` 及其 TLS/mTLS 门仍然在线。

该协议**没有 TLS、没有任何形式的对端认证、没有限流**（`crates/rr-network/src/tcp_raft_server.rs:1-15`）——任何能到达端口的客户端都能注入 Vote/AppendEntries 帧、干扰选举与复制。因此（ADR A-0014）：

- **G6**：即使 gRPC 面 TLS/mTLS 完全合规，也必须显式 `--allow-insecure`；
- **G7**：bind 地址必须是 loopback，且**不可绕过**；TCP 端口默认由 gRPC 端口 **+10000** 派生（可用 `--raft-tcp-port` / `RR_RAFT_TCP_PORT` 覆盖）；
- 库用户直接调用 `TcpRaftServer::bind` 会绕过这些门——**永远不要把它绑到非 loopback 地址**。

在 v0.2.0 中它是 dev/benchmark-only 特性，生产请使用默认的 gRPC transport。

---

`RaftTransport::Tcp` (`--raft-transport tcp`) is a lightweight binary frame protocol (`RRAF` magic + version, 26-byte header) carrying only AppendEntries/Vote; **InstallSnapshot always falls back to gRPC** (`crates/rr-raft/src/tcp_transport.rs:1-4`), so the gRPC `RaftService` and its TLS/mTLS gates stay in force under TCP mode.

The frame protocol has **no TLS, no peer authentication of any kind, and no rate limiting** (`crates/rr-network/src/tcp_raft_server.rs:1-15`) — any client that can reach the port can inject Vote/AppendEntries frames and interfere with elections and replication. Hence (ADR A-0014):

- **G6**: explicit `--allow-insecure` is required even when the gRPC face is fully TLS/mTLS compliant;
- **G7**: the bind address must be loopback and **cannot be bypassed**; the TCP port defaults to the gRPC port **+10000** (override with `--raft-tcp-port` / `RR_RAFT_TCP_PORT`);
- Library users calling `TcpRaftServer::bind` directly bypass these gates — **never bind it to a non-loopback address**.

In v0.2.0 it is a dev/benchmark-only transport; use the default gRPC transport in production.

## 本地开发（insecure）/ Insecure local development

本地起集群的两种常见姿势：

1. **明文 + `--allow-insecure`**：examples 与 CI 的默认路径。启动日志会出现 `INSECURE MODE ACTIVE` 警告，metrics 端口上 `rr_security_insecure_mode = 1`。
2. **开发证书 + 真实 TLS**（推荐用于演练 TLS 配置）：仓库根目录的 `scripts/gen-dev-certs.sh` 生成自签名开发 CA（RSA-4096，10 年）与每节点 cert/key 对（RSA-2048，1 年），SAN 覆盖 `localhost`、`127.0.0.1`、`::1`、`rr-N`、`nodeN.rr.local`，key 文件权限 600：

```bash
./scripts/gen-dev-certs.sh dev-certs 3
```

> **这些证书不得用于生产**：私钥不加密、SAN 只覆盖本地主机名。完整启动命令示例与字段说明见 [tls.md](./tls.md)。

---

Two common ways to bring up a local cluster:

1. **Plaintext + `--allow-insecure`**: the default path for examples and CI. Expect an `INSECURE MODE ACTIVE` warning at startup and `rr_security_insecure_mode = 1` on the metrics port.
2. **Dev certs + real TLS** (recommended for rehearsing TLS configuration): `scripts/gen-dev-certs.sh` at the repo root generates a self-signed dev CA (RSA-4096, 10 years) plus per-node cert/key pairs (RSA-2048, 1 year) with SANs for `localhost`, `127.0.0.1`, `::1`, `rr-N`, `nodeN.rr.local`; key files are chmod 600:

```bash
./scripts/gen-dev-certs.sh dev-certs 3
```

> **Never use these certificates in production**: private keys are unencrypted and SANs only cover local hostnames. Full launch examples and field semantics: [tls.md](./tls.md).

## 相关 / Related

- TLS 配置与轮换 / TLS configuration & rotation: [tls.md](./tls.md)
- 审计事件 schema 与 sink 路由 / Audit schema & sink routing: [audit-log.md](./audit-log.md)
- 整体部署 / Full deployment guide: [deployment.md](./deployment.md)
- ADR 索引（A-0012 / A-0014 等）/ ADR index (A-0012, A-0014, …): [../architecture/design-decisions.md](../architecture/design-decisions.md)
