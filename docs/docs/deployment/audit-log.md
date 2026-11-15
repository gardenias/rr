---
sidebar_position: 4
---

# 审计日志 / Audit Log

AdminService、认证和限流事件写入专用 tracing target `rr::audit`。事件与应用日志共享 subscriber，但可用 `EnvFilter` 保留
并路由到独立 append-only sink。

AdminService, authentication, and rate-limit events use the dedicated tracing target `rr::audit`. They share the
application subscriber, but `EnvFilter` can retain them for routing into a separate append-only sink.

## 事件字段 / Event Schema

| Field | 类型 / Type | 示例或用途 / Example or Use |
| :--- | :--- | :--- |
| `target` | 固定值 / constant | `rr::audit` |
| `op` | 操作 / operation | `TriggerSnapshot`、`TransferLeader`、`auth`、`rate_limit` |
| `service` | gRPC 面 / surface | `admin`、`client`、`raft` |
| `peer` | 调用方 socket 地址 / caller socket address | `10.0.1.23:51234` / `unknown` |
| `node_id` | 处理请求的本地节点 / local serving node | `1` |
| `group_id` | 组级操作 / group-scoped operation | `7` |
| `target_node_id` | 目标节点 / target node | `3` |
| `result` | 生命周期 / lifecycle | `started`、`succeeded`、`failed`、`rejected`、`force_accepted` |
| `success` | 操作结果 / operation outcome | `true` / `false` |
| `reason` | 拒绝或失败原因 / rejection or failure reason | `missing_credential`、`invalid_credential` |
| `message` | 结果说明 / result detail | 自由文本 / free-form text |

变更操作通常在分发时写 `started`，完成时写 `succeeded` 或 `failed`；查询操作的事件数量取决于具体 RPC。认证和限流拦截器对
每个被拒绝请求写一条 `rejected` 事件。

Mutation operations normally emit `started` at dispatch and `succeeded` or `failed` at completion; the number of events
for query RPCs varies by handler. Authentication and rate-limit interceptors emit one `rejected` event per blocked
request.

## 路由到独立 sink / Route to a Dedicated Sink

### 标准输出与日志采集器 / Standard Output and a Log Collector

`rr-server` 默认使用 compact tracing 文本输出，不是 JSON。用下面的过滤器同时保留普通 warn 和审计 info/warn，再由 Fluent
Bit、Vector、journald 等按 `rr::audit` target 分流。

`rr-server` uses compact tracing text by default, not JSON. This filter retains normal warnings plus info/warn audit
events; Fluent Bit, Vector, journald, or another collector can then split on the `rr::audit` target.

```bash
RUST_LOG="warn,rr::audit=info" rr-server serve --config /etc/rr/config.toml
```

### 独立文件 / Dedicated File

嵌入 Rr 或 fork `rr-server` 时，可增加自定义 `tracing-subscriber` layer，把 `target="rr::audit"` 写到独立文件，再由平台
设置 append-only/immutable 属性。默认二进制的 stdout 配置本身不会创建独立审计文件。

When embedding Rr or forking `rr-server`, add a custom `tracing-subscriber` layer for `target="rr::audit"` and protect
the resulting file with platform append-only/immutable attributes. The stock binary's stdout configuration does not
create a separate audit file.

### 日志平台 / Loki, CloudWatch, or Datadog

把 tracing 输出转发到日志平台，再按 `target:"rr::audit"` 持久化和查询。传输、索引和不可篡改保留策略由该平台负责，Rr 不直
接提供这些 sink。

Forward tracing output to the platform and filter on `target:"rr::audit"` for persistence and queries. Transport,
indexing, and immutable retention belong to that platform; Rr does not provide these sinks directly.

## 保留策略 / Retention Policy

按组织政策、适用法规和取证需求确定保留期，不要把通用模板当作合规保证。至少明确热/冷存储时长、访问控制、删除流程和恢复演
练，并用 logrotate 或云端保留策略执行。

Set retention from organizational policy, applicable regulations, and forensic needs; a generic table is not a
compliance guarantee. Define hot/cold retention, access control, deletion, and restore drills, then enforce them through
logrotate or cloud logging policies.

## 威胁模型 / Threat Model

审计日志用于检测和取证，不能阻止未授权变更。生产至少组合以下控制：

Audit logs support detection and forensics; they do not prevent unauthorized changes. Combine at least:

1. TLS + mTLS，见 [TLS 指南](./tls.md)，验证对端身份。<br />
   TLS + mTLS, described in the [TLS guide](./tls.md), verifies peers.
2. Bearer-token 认证，使用 `[auth]` 引用的环境变量。<br />
   Bearer-token authentication uses environment variables referenced by `[auth]`.
3. 网络隔离，Admin 端口只暴露给运维 VPN 或 bastion。<br />
   Network isolation exposes Admin only to an operations VPN or bastion.
4. 独立审计 sink，降低攻击者抹除痕迹的能力。<br />
   A separate audit sink reduces an attacker's ability to erase evidence.

## 调用者身份 / Caller Identity

认证拒绝以及带调用方指纹的限流拒绝可包含 `caller_token_fp`：基于 siphash 的 16 位十六进制、非密码学指纹，用于关联同一
token 的多次事件，不暴露 token 本身。`peer` 仍提供网络层地址。

Authentication rejects and per-caller rate-limit rejects can include `caller_token_fp`: a non-cryptographic,
16-hex-character siphash fingerprint that correlates events for one token without exposing it. `peer` still provides the
network address.

```json
{"op":"auth","service":"client","peer":"10.0.1.23:51234","caller_token_fp":"7f3e9a02b1c4d5e6","result":"rejected","reason":"invalid_credential"}
```

调查撞库或 spray 时可按该字段聚合，但它不是可验证的用户身份，也不能跨不同哈希键当作永久标识。当前没有完整传播 mTLS CN、
JWT subject 等 subject claims。

For credential-stuffing or spray investigations, group by this field, but do not treat it as authenticated user identity
or a permanent identifier across different hash keys. Full subject-claim propagation, such as mTLS CN or JWT subject, is
not currently available.

## 已知限制 / Known Limitations

- 认证/限流拒绝通常为 `warn!`，Admin RPC 为 `info!`；`RUST_LOG=warn` 会过滤 Admin info 事件。使用
  `RUST_LOG="warn,rr::audit=info"`。<br />
  Auth/rate-limit rejects are usually `warn!`, while Admin RPCs are `info!`; `RUST_LOG=warn` drops Admin info events.
  Use `RUST_LOG="warn,rr::audit=info"`.
- 默认审计事件与应用日志共享 subscriber 和输出；独立文件需要自定义 layer 或下游采集。<br />
  By default, audit and application events share the subscriber and output; separate files require a custom layer or
  downstream collection.
- stdout 被丢弃、进程崩溃或 sink 不可达时，Rr 不提供独立持久化补发队列。<br />
  Rr has no separate durable replay queue if stdout is lost, the process crashes, or the sink is unreachable.

## 相关 / Related

- [加密传输 / Encrypted transport](./tls.md)
- [事故响应 / Incident response](../operations/runbooks/)
- [API 稳定性 / API stability](../reference/compatibility.md)
