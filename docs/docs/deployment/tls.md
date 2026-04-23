---
sidebar_position: 2
---

# TLS — 加密 gRPC 流量 / Encrypting gRPC Traffic

rr 通过 rustls 在所有 gRPC 端点（Client / Raft / Admin）上支持 TLS 和可选的 mTLS。默认**不启用**（便于 dev），生产必须启用。

---

rr supports TLS and optional mTLS on every gRPC endpoint (Client / Raft / Admin) via rustls. TLS is **off by default** (to keep the dev loop simple); production deployments MUST enable it.

## 本地开发：生成自签名证书 / Local dev: generate self-signed certs

```bash
./scripts/gen-dev-certs.sh dev-certs 3
```

This creates `dev-certs/ca.pem`, `dev-certs/ca.key`, and per-node pairs `dev-certs/node{1,2,3}.pem` + `.key`, all signed by a single dev CA with SANs for `localhost`, `127.0.0.1`, `rr-N`, and `nodeN.rr.local`.

> **不要在生产中使用这些证书。** 密钥不加密、保护期 1 年、CA 10 年、SAN 仅覆盖本地开发主机名。
>
> **Do not use these in production.** Keys are unencrypted; validity is 1 year (node) / 10 years (CA); SANs only cover local-dev hostnames.

## 启动 rr-server with TLS / Starting rr-server with TLS

### Via CLI flags

```bash
rr-server serve \
  --node-id 1 --listen-addr 127.0.0.1:51001 \
  --tls-cert dev-certs/node1.pem \
  --tls-key  dev-certs/node1.key \
  --tls-ca   dev-certs/ca.pem \
  --tls-domain node1.rr.local \
  --tls-require-client-cert \
  --bootstrap
```

### Via config file

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"

[tls]
cert_path           = "/etc/rr/tls/node1.pem"
key_path            = "/etc/rr/tls/node1.key"
ca_path             = "/etc/rr/tls/ca.pem"
require_client_cert = true
domain_name         = "node1.rr.internal"
```

Then: `rr-server serve --config /etc/rr/node1.toml`.

### Via env vars

```bash
export RR_TLS_CERT=/etc/rr/tls/node1.pem
export RR_TLS_KEY=/etc/rr/tls/node1.key
export RR_TLS_CA=/etc/rr/tls/ca.pem
export RR_TLS_REQUIRE_CLIENT_CERT=true
export RR_TLS_DOMAIN=node1.rr.internal
rr-server serve --config /etc/rr/node1.toml
```

CLI flags > env vars > config file values > built-in defaults.

## 字段含义 / Field Semantics

| Field | 含义 / Meaning | Required |
| :--- | :--- | :--- |
| `cert_path` / `--tls-cert` / `RR_TLS_CERT` | PEM-encoded server cert; also used as client identity when this node dials peers | Yes (if TLS enabled) |
| `key_path` / `--tls-key` / `RR_TLS_KEY` | Matching private key | Yes (if TLS enabled) |
| `ca_path` / `--tls-ca` / `RR_TLS_CA` | PEM CA bundle used to verify peer certs. Required for mTLS. | Iff `require_client_cert` or custom CA |
| `require_client_cert` / `--tls-require-client-cert` | Server rejects connections without a valid client cert (mTLS). | No (default false) |
| `domain_name` / `--tls-domain` | SNI name expected in the peer's cert when this node dials others | Recommended |

## TLS 模式 / Modes

| 模式 / Mode | Settings | Trust model |
| :--- | :--- | :--- |
| **Plaintext (dev only)** | No TLS fields set | None — ⚠️ never use in production |
| **Server TLS only** | `cert_path` + `key_path` set | Clients verify server (like HTTPS); server does not verify clients |
| **mTLS** | + `ca_path` + `require_client_cert = true` | Mutual — server also rejects unknown clients |

**生产建议 / Production recommendation**: mTLS with a per-deployment CA. All nodes + client applications get certs signed by the same CA. The CA private key is kept offline.

## 对称 TLS / Symmetric TLS Between Peers

rr 中节点间的 Raft RPC 是**对称**的 —— 每个节点既是服务端也是客户端。因此：

- 同一套 `cert_path` + `key_path` 被**服务端**用于接受 inbound TLS，也被 **ChannelPool** 用作 outbound client identity（发给对等节点做 mTLS 认证）。
- `ca_path` 在服务端用于验证客户端证书，在客户端用于验证 peer 服务端证书。

这意味着**每个节点只需要一对 cert+key**。多个节点可以共用一个 cert（SAN 必须覆盖所有主机名），或每节点独立 cert（推荐）。

---

In rr, inter-node Raft RPCs are **symmetric** — every node is both server and client. So:

- The same `cert_path` + `key_path` pair is used **both** by the inbound server to accept TLS connections **and** by the outbound `ChannelPool` as the client identity (for mTLS auth to peers).
- `ca_path` is used on the server to verify client certs and on the client to verify peer server certs.

This means **each node only needs one cert+key pair**. You may share a single cert across all nodes (SANs must cover every hostname), or give each node its own (recommended for blast-radius control).

## 故障排查 / Troubleshooting

### "unable to locate CA in the native certificate store"

你没有设置 `ca_path` 而 peer 的 cert 不是 WebPKI-信任的。**解决方案**：用 `--tls-ca` 或 `[tls].ca_path` 指定你的内部 CA。

You didn't set `ca_path` and the peer's cert isn't trusted by WebPKI. **Fix**: point `--tls-ca` / `[tls].ca_path` at your internal CA.

### "CertificateRequired" from client

客户端没有发 cert 但服务端设了 `require_client_cert = true`。**解决方案**：要么在 client 里也配 cert+key+ca，要么关闭 `require_client_cert`。

Client didn't send a cert but server has `require_client_cert = true`. **Fix**: either configure a client-side cert+key+ca, or disable `require_client_cert`.

### "HandshakeFailure" / "UnknownIssuer"

Server 的 cert 由一个 client 不信任的 CA 签发。**解决方案**：确认客户端的 `ca_path` 指向对应的 CA bundle。

Server's cert is signed by a CA the client doesn't trust. **Fix**: point client's `ca_path` at the correct CA bundle.

### "Could not automatically determine the process-level CryptoProvider"

rustls 0.23 要求显式选 crypto provider。rr-server 二进制已经在 `main()` 里调用 `rustls::crypto::ring::default_provider().install_default()`，所以**不应该**触发此错误。如果你把 rr-server 作为库嵌入到自己的二进制，需要在 `main()` 最早处加同样一行。

rustls 0.23 requires picking a crypto provider. The `rr-server` binary calls `rustls::crypto::ring::default_provider().install_default()` in `main()`, so you **shouldn't** hit this. If you embed rr-server as a library in your own binary, add the same line at the top of your `main()`.

## 证书轮换 / Certificate Rotation

目前 rr **不支持热轮换**：更新 cert/key 需要重启进程。滚动重启的流程：

rr does **not** currently support hot rotation: updating a cert/key requires restarting the process. Rolling-restart recipe:

1. 同时更新所有节点的 CA bundle（`ca_path`）—— 这是**先决条件**，否则你更新后的 peer 拿新 cert 来握手会失败。
2. 一次重启一个节点：用新 cert+key 启动，确认它重新加入集群、成为 follower、稳定。
3. 全部重启完毕后，旧 cert 可以删除。

---

1. Update every node's CA bundle (`ca_path`) first — **prerequisite**, otherwise peers with updated certs will fail TLS handshake to nodes still using the old trust store.
2. Restart one node at a time: bring it up with the new cert+key, verify it rejoins, becomes follower, stays stable.
3. Once all nodes are rotated, old certs can be deleted.

**规划中 / Planned**: hot-reload via `SIGHUP` or `rr-admin tls reload` (tracked in Phase 3 roadmap).

## 相关 / Related

- 整体部署 / Full deployment guide: [deployment.md](./deployment.md)
- C4 Components (TLS 绑点 / TLS touch-points): [../architecture/c4/components-network.md](../architecture/c4/components-network.md)
