# rr-client

Async Rust client SDK for the [Rr distributed Multi-Raft consensus framework](https://github.com/gardenias/rr).

Features:

- Automatic leader-redirect with per-group leader cache.
- Exponential-backoff retry on transport errors.
- Framework-level idempotency: send a request multiple times safely with `send_request_idempotent` — duplicates are absorbed, you get the cached response.
- TLS / mTLS via [`rr_network::tls::TlsConfig`].
- Bearer-token authentication via the `authorization: Bearer <token>` metadata header.
- Built-in metrics (`metrics` crate, prefixed `rr_client_*`).
- `tracing::instrument` spans on every public method.

## Install

```toml
[dependencies]
rr-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Quick start (dev / loopback, no TLS, no auth)

```rust,no_run
use rr_client::RrClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RrClient::connect(vec!["127.0.0.1:51001".into()]).await?;

    let payload = b"hello".to_vec();
    let response = client.send_request(/* group_id */ 1, payload).await?;
    println!("got {} bytes", response.len());

    Ok(())
}
```

## Production (TLS + bearer auth)

```rust,no_run
use rr_client::RrClientBuilder;
use rr_network::tls::TlsConfig;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tls = TlsConfig {
        cert_path: "/etc/rr/client.crt".into(),
        key_path: "/etc/rr/client.key".into(),
        ca_path: Some("/etc/rr/ca.crt".into()),
        require_client_cert: false, // ignored on client side
        domain_name: Some("rr.example.com".into()),
    };

    let token = env::var("RR_CLIENT_TOKEN")?;

    let client = RrClientBuilder::new(vec![
        "rr-1.example.com:51001".into(),
        "rr-2.example.com:51001".into(),
        "rr-3.example.com:51001".into(),
    ])
    .with_tls(tls)
    .with_bearer_token(token)
    .with_strict_security(true) // reject token-without-tls at build time
    .build()
    .await?;

    let response = client.send_request(1, b"hello".to_vec()).await?;
    println!("got {} bytes", response.len());

    Ok(())
}
```

## Idempotent writes

When the network is flaky and you can't tell whether a request was applied, `send_request_idempotent` makes retries safe:

```rust,no_run
use rr_client::RrClient;
use rr_core::idempotency::IdempotencyKey;

# async fn run(client: RrClient) -> Result<(), Box<dyn std::error::Error>> {
let key = IdempotencyKey::new("create_order".into(), 42);
let payload = b"order #42 contents".to_vec();

// Safe to call any number of times. The framework deduplicates by
// (request_type, request_id) and returns the original cached response.
let response = client.send_request_idempotent(1, payload, key).await?;
# Ok(())
# }
```

If `request_id` falls below the framework watermark and has been evicted from the LRU cache, the call returns `RrError::IdempotencyExpired { request_type, request_id, watermark }` — pick a new `request_id` strictly greater than `watermark` and retry.

## Cluster discovery

```rust,no_run
# async fn run(client: rr_client::RrClient) -> Result<(), Box<dyn std::error::Error>> {
let info = client.get_cluster_info().await?;
for group in info.groups {
    println!("group {} leader={} replicas={:?}",
        group.group_id, group.leader_node_id, group.replica_node_ids);
}
# Ok(())
# }
```

## Metrics

The SDK emits the following [`metrics`](https://crates.io/crates/metrics) entries — install any compatible exporter (e.g. `metrics-exporter-prometheus`) to scrape them:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rr_client_request_duration_seconds` | histogram | `group_id`, `outcome={success,failure,idempotency_expired}` | End-to-end request latency including retries |
| `rr_client_retries_total` | counter | `group_id` | Number of retry attempts |
| `rr_client_leader_cache_hits_total` | counter | `group_id` | Cached leader was used |
| `rr_client_leader_cache_misses_total` | counter | `group_id` | Cache miss → bootstrap address used |

## Configuration knobs

- `RetryConfig { max_retries: u32, retry_delay_ms: u64 }` — default `max_retries=3, retry_delay_ms=100`. Backoff is `retry_delay_ms * 2^attempt`.
- `RrClientBuilder::with_strict_security(true)` — reject `with_bearer_token` without `with_tls` at `build()` time. Default is warn-only for parity with `rr-server`'s `allow_insecure` opt-in.

## Security notes

- Bearer tokens travel **in plaintext** inside the gRPC metadata. **Pair them with TLS** in production.
- The SDK never logs tokens. Audit-log events on the server side carry only an 8-hex `caller_token_fp` siphash fingerprint for correlation, never the token itself.
- For mTLS, populate `TlsConfig.cert_path` + `key_path` and serve them as the SDK's identity. The server-side mTLS check is configured separately.

## License

MIT — see the repository [`LICENSE`](https://github.com/gardenias/rr/blob/main/LICENSE).

## Links

- Main project: <https://github.com/gardenias/rr>
- Documentation site: <https://gardenias.github.io/rr/>
- Compatibility & semver policy: see the [`compatibility`](https://github.com/gardenias/rr/blob/main/docs/docs/reference/compatibility.md) page.
