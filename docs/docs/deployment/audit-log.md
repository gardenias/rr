---
sidebar_position: 4
---

# 审计日志 / Audit Log

rr 的每个 AdminService RPC 都会在专用 tracing target `rr::audit` 上发射一条 `info!` 事件，包含调用者 peer 地址、操作名、参数、结果。这些事件与普通应用日志一起通过 `tracing` 流出，但可以用 `EnvFilter` 独立路由，写入不可篡改的审计日志 sink（例如只读文件、syslog、云端 immutable log）。

---

Every AdminService RPC in rr emits an `info!` event on the dedicated
tracing target `rr::audit` including the caller's peer address, operation
name, arguments, and result. These flow through `tracing` alongside
application logs but can be routed independently via `EnvFilter` into an
append-only audit sink (e.g. read-only file, syslog, cloud immutable log).

## 事件 schema / Event schema

| Field | Type | Example |
| :--- | :--- | :--- |
| `target` | always `rr::audit` | — |
| `op` | Admin RPC method | `TriggerSnapshot`, `TransferLeader`, `GetNodeStatus`, `GetGroupStatus`, `GetMetricsSummary` |
| `peer` | caller socket addr (from tonic) | `10.0.1.23:51234` or `unknown` |
| `node_id` | local node id serving the RPC | `1` |
| `group_id` | for group-scoped operations | `7` |
| `target_node_id` | for `TransferLeader` only | `3` |
| `success` | on mutation result | `true` / `false` |
| `message` | on mutation result | free-form response |

Mutation operations (`TriggerSnapshot`, `TransferLeader`) emit **two**
events — one at dispatch ("admin rpc — mutation") and one at completion
("admin rpc result"). Read-only operations emit one event.

## 配置独立的审计 sink / Routing audit events to a dedicated sink

### Option A — JSON 到 stdout，由 journald / Docker 捕获

Since rr-server uses `tracing-subscriber::fmt`, enable JSON output via the
default log level. Audit events mix with app events but are distinguishable
by `target`:

```bash
RUST_LOG="rr::audit=info,info" rr-server serve --config ...
```

Downstream (Fluent Bit, Vector, journald) can grep `rr::audit` to split
the stream.

### Option B — 独立文件 / Dedicated file

用自定义 `tracing-subscriber` layer（需要 fork rr-server 或 embed rr as library），把 `target="rr::audit"` 的 events 写到独立 file，并加上 `append-only` / `immutable` 文件系统标志（e.g. `chattr +a`）。

Use a custom `tracing-subscriber` layer (requires forking rr-server or
embedding rr as a library) to split `target="rr::audit"` events into a
dedicated file, which you then protect with filesystem `append-only` /
`immutable` attributes (e.g. `chattr +a`).

### Option C — Loki / CloudWatch / DataDog

将所有 `tracing` 事件转发到这些系统，并在这些系统里做 `target:"rr::audit"` 的持久化+查询。大多数云端观测栈可以这样处理。

Forward all `tracing` events into these systems and filter on
`target:"rr::audit"` downstream. Works out-of-the-box with most cloud
observability stacks.

## 保留政策建议 / Retention recommendations

| 合规要求 / Compliance | 保留期 / Retention |
| :--- | :--- |
| SOC2 | ≥ 1 year |
| PCI-DSS | ≥ 1 year (3 months hot + 9 months cold) |
| HIPAA | ≥ 6 years |
| 一般生产 / General production | ≥ 90 days |

设置 logrotate / cloud log policy 以匹配你的合规要求。

## 威胁模型 / Threat model

审计日志**检测**未授权变更，而**不能阻止**它们。防御层次：

Audit logs **detect** unauthorized changes; they do not **prevent** them.
Layered defenses:

1. **TLS + mTLS**（[tls.md](./tls.md)）— 防止冒名顶替
2. **Bearer-token auth**（`[auth]` config）— 要求有效凭证
3. **网络隔离** — admin_port 只暴露给 ops VPN / bastion
4. **审计日志** — 检测 + 事后取证（本文档）
5. **immutable log sink** — 防止攻击者抹掉痕迹

## 已知限制 / Known limitations

- 审计事件目前**不含调用者身份**（除 peer IP）。Phase 3 的 auth interceptor 目前只校验 token 不 forward 身份到 handler。未来计划在 gRPC metadata extension 里透传 subject claim，让 audit 事件携带 `caller_subject` 字段。
- 审计事件**与应用日志共享 subscriber**——如果 ops 团队配置了 `RUST_LOG=warn` 则 audit info 会被过滤掉。**建议用独立的 EnvFilter**: `RUST_LOG="warn,rr::audit=info"`.

---

- Audit events currently **do not include caller identity** beyond the
  peer IP. The Phase 3 auth interceptor validates tokens but does not
  forward identity to handlers. Future work: propagate subject via gRPC
  metadata extension into a `caller_subject` field.
- Audit events **share the subscriber with app logs** — if operators set
  `RUST_LOG=warn` the audit info level is filtered out. Recommended:
  `RUST_LOG="warn,rr::audit=info"`.

## 相关 / Related

- [tls.md](./tls.md) — 加密传输
- [../operations/runbooks/](../operations/runbooks/) — 事故响应
- [../reference/compatibility.md](../reference/compatibility.md) — API 稳定性承诺（审计 schema 也适用）
