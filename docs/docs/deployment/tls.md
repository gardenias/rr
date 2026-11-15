---
sidebar_position: 2
---

# TLS：加密 gRPC 流量 / TLS: Encrypting gRPC Traffic

Rr 通过 rustls 在 Client、Raft、Admin gRPC 端点支持 TLS 和可选 mTLS。默认不启用 TLS；生产 Raft 必须启用 TLS + mTLS。

Rr supports TLS and optional mTLS on Client, Raft, and Admin gRPC endpoints through rustls. TLS is off by default;
production Raft requires TLS + mTLS.

## 本地开发证书 / Local Development Certificates

```bash
./scripts/gen-dev-certs.sh dev-certs 3
```

生成 `dev-certs/ca.pem`、`ca.key` 和每节点 `node{1,2,3}.pem` + `.key`。SAN 覆盖 `localhost`、`127.0.0.1`、`::1`、`rr-N`
和 `nodeN.rr.local`。

This creates `dev-certs/ca.pem`, `ca.key`, and per-node `node{1,2,3}.pem` + `.key`. SANs cover `localhost`, `127.0.0.1`,
`::1`, `rr-N`, and `nodeN.rr.local`.

> **不要在生产中使用这些证书。** 私钥不加密，节点证书有效期 1 年、CA 10 年，SAN 只覆盖本地开发主机名。
>
> **Do not use these certificates in production.** Private keys are unencrypted, node certificates last one year and the
> CA ten years, and SANs cover local development hostnames only.

## 启动 rr-server / Start rr-server with TLS

### CLI 参数 / CLI Flags

```bash
rr-server serve \
  --node-id 1 --listen-addr 127.0.0.1:51001 \
  --tls-cert dev-certs/node1.pem \
  --tls-key dev-certs/node1.key \
  --tls-ca dev-certs/ca.pem \
  --tls-domain node1.rr.local \
  --tls-require-client-cert \
  --bootstrap
```

### 配置文件 / Configuration File

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"

[tls]
cert_path = "/etc/rr/tls/node1.pem"
key_path = "/etc/rr/tls/node1.key"
ca_path = "/etc/rr/tls/ca.pem"
require_client_cert = true
domain_name = "node1.rr.internal"
```

```bash
rr-server serve --config /etc/rr/node1.toml
```

### 环境变量 / Environment Variables

```bash
export RR_TLS_CERT=/etc/rr/tls/node1.pem
export RR_TLS_KEY=/etc/rr/tls/node1.key
export RR_TLS_CA=/etc/rr/tls/ca.pem
export RR_TLS_REQUIRE_CLIENT_CERT=true
export RR_TLS_DOMAIN=node1.rr.internal
rr-server serve --config /etc/rr/node1.toml
```

优先级为 CLI > 环境变量 > 配置文件 > 内置默认值。

Precedence is CLI > environment > configuration file > built-in defaults.

## 字段含义 / Field Semantics

| Field | 含义 / Meaning | 必需条件 / Required When |
| :--- | :--- | :--- |
| `cert_path` / `--tls-cert` / `RR_TLS_CERT` | PEM 服务端证书，也用作拨入 peer 时的客户端身份 / PEM server certificate and outbound client identity | 启用 TLS / TLS enabled |
| `key_path` / `--tls-key` / `RR_TLS_KEY` | 匹配的私钥 / Matching private key | 启用 TLS / TLS enabled |
| `ca_path` / `--tls-ca` / `RR_TLS_CA` | 验证对端证书的 PEM CA bundle / PEM CA bundle for peer verification | mTLS 或私有 CA / mTLS or a private CA |
| `require_client_cert` / `--tls-require-client-cert` | 拒绝无有效客户端证书的连接 / Reject clients without a valid certificate | 生产 Raft 必须为 true / Required for production Raft |
| `domain_name` / `--tls-domain` | 拨入其他节点时使用的 SNI/证书名称覆盖 / SNI and certificate-name override when dialing peers | 地址与 SAN 不一致时 / When dial address differs from SAN |

## TLS 模式 / TLS Modes

| 模式 / Mode | 设置 / Settings | 信任模型 / Trust Model |
| :--- | :--- | :--- |
| 明文，仅开发 / Plaintext, dev only | 不设置 TLS / No TLS fields | 无身份或加密；禁止生产 / No identity or encryption; never production |
| 仅服务端 TLS / Server TLS only | `cert_path` + `key_path` | 客户端验证服务端，服务端不验证客户端 / Client verifies server; server does not verify client |
| mTLS | 再加 `ca_path` + `require_client_cert = true` / Add both | 双向验证 / Mutual verification |

生产使用部署专用 CA 和每节点独立证书，CA 私钥离线保存。Client/Admin 应用也需要签发证书和对应 token；`rr-admin` 的三个
TLS 参数必须同时提供。

Use a deployment-specific CA and distinct node certificates in production, with the CA private key offline. Client/Admin
applications also need certificates and appropriate tokens; all three `rr-admin` TLS flags must be supplied together.

## 节点间对称 TLS / Symmetric TLS Between Peers

每个节点既是服务端也是客户端。同一 `cert_path` + `key_path` 用于 inbound TLS 和 ChannelPool outbound 客户端身份；
`ca_path` 同时用于验证 inbound 客户端和 outbound peer 服务端。因此每节点只需一对 cert/key，SAN 必须覆盖其他节点实际使用
的 DNS 名或 `--tls-domain` 覆盖名。

Every node is both server and client. The same `cert_path` + `key_path` pair serves inbound TLS and identifies the
outbound ChannelPool client; `ca_path` verifies both inbound clients and outbound peer servers. Each node therefore
needs one cert/key pair, whose SANs must cover the DNS names peers actually dial or the `--tls-domain` override.

## 故障排查 / Troubleshooting

### 找不到 CA：`unable to locate CA in the native certificate store`<br />CA Not Found: `unable to locate CA in the native certificate store`

对端证书不由当前信任库信任。用 `--tls-ca` 或 `[tls].ca_path` 指向签发它的内部 CA，并确认 bundle 可读。

The peer certificate is not trusted by the current roots. Point `--tls-ca` or `[tls].ca_path` at the issuing internal CA
and confirm the bundle is readable.

### 客户端证书缺失：`CertificateRequired`<br />Missing Client Certificate: `CertificateRequired`

服务端启用了 `require_client_cert = true`，但客户端未提供证书。为客户端配置 cert + key + CA；不要为了消除生产错误而关闭
mTLS。

The server requires a client certificate but the client did not present one. Configure client cert + key + CA; do not
disable mTLS to hide a production error.

### 握手或签发者错误：`HandshakeFailure` / `UnknownIssuer`<br />Handshake or Issuer Errors: `HandshakeFailure` / `UnknownIssuer`

确认客户端 CA bundle 包含实际签发 CA、证书未过期，且拨入名称匹配 SAN。可使用 `openssl s_client` 同时带 CA 和客户端证书验
证。

Confirm that the client CA bundle contains the actual issuer, the certificate is unexpired, and the dial name matches a
SAN. Test with `openssl s_client` using both the CA and client certificate.

### 无法选择 CryptoProvider：`Could not automatically determine the process-level CryptoProvider`<br />Cannot Select CryptoProvider: `Could not automatically determine the process-level CryptoProvider`

`rr-server` 二进制已在 `main()` 安装 ring provider。如果把 rr-server 作为库嵌入自己的二进制，在 `main()` 最早处安装同一
个 provider：

The `rr-server` binary installs the ring provider in `main()`. If embedding rr-server as a library, install the same
provider at the start of your own `main()`:

```rust
let _ = rustls::crypto::ring::default_provider().install_default();
```

## 证书轮换 / Certificate Rotation

当前不支持热加载，更新 cert/key 或 CA bundle 后必须重启。每次只重启一个节点，等待 `/ready`、leader 和复制追平后再继续。

Hot reload is not supported; restarting is required after changing cert/key or CA bundle. Restart one node at a time and
wait for `/ready`, stable leadership, and replication catch-up before continuing.

如果只是用同一 CA 换发节点证书，直接逐节点替换 cert/key 并滚动重启。其他节点已信任该 CA，不需要先换 CA。

When reissuing node certificates under the same CA, replace each cert/key and rolling-restart directly. Peers already
trust that CA, so no CA transition is needed.

如果更换 CA，必须先让所有节点同时信任旧、新 CA，再切换身份证书：

When changing CAs, every node must trust both old and new CAs before any identity certificate changes:

1. 在所有节点部署包含旧、新 CA 的 bundle，逐节点重启以加载它。只覆盖文件而不重启不会生效。<br />
   Deploy a bundle containing both CAs everywhere and rolling-restart to load it. Replacing the file without restarting
   has no effect.
2. 逐节点部署新 CA 签发的 cert/key 并重启，验证 mTLS、成员和追平。<br />
   Deploy certificates signed by the new CA and restart one node at a time, verifying mTLS, membership, and catch-up.
3. 全部节点切换完成后，如需移除旧 CA，再从 bundle 删除并做第三轮滚动重启。旧私钥按 secret-management 流程安全删除。<br />
   After every node has switched, optionally remove the old CA and perform a third rolling restart. Delete old private
   keys through your secret-management process.

若握手失败，停止后续轮换，恢复该节点上一份可用配置并验证集群；不要同时重启其余节点。

If a handshake fails, stop the rotation, restore that node's last working configuration, and verify cluster health; do
not restart the remaining nodes.

## 相关 / Related

- [整体部署 / Full deployment](./deployment.md)
- [C4 TLS 绑点 / C4 TLS touch-points](../architecture/c4/components-network.md)
