# rr-client-java

Async Java SDK for the [rr distributed Multi-Raft consensus framework](https://github.com/gardenias/rr).

Features:

- Automatic leader-redirect with per-group leader cache.
- Exponential-backoff retry on transport errors.
- Framework-level idempotency with `sendRequestIdempotent(...)`.
- TLS / mTLS client configuration via `TlsConfig`.
- Bearer-token auth via gRPC metadata header `authorization: Bearer <token>`.
- Built-in Micrometer metrics (`rr.client.*`).

## Install

Gradle (GitHub Packages):

```kotlin
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/gardenias/rr")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("io.github.gardenias:rr-client:0.2.0")
}
```

## Quick start (dev / loopback, no TLS, no auth)

```java
import io.github.gardenias.rr.client.RrClient;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class QuickStart {
    public static void main(String[] args) {
        RrClient client = RrClient.connect(List.of("127.0.0.1:51001")).join();
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] response = client.sendRequest(1L, payload).join();
        System.out.println("got " + response.length + " bytes");
        client.close();
    }
}
```

## Production (TLS + bearer auth)

```java
import io.github.gardenias.rr.client.*;

import java.nio.file.Path;
import java.util.List;

public class ProdExample {
    public static void main(String[] args) {
        TlsConfig tls = new TlsConfig(
                Path.of("/etc/rr/client.crt"),
                Path.of("/etc/rr/client.key"),
                Path.of("/etc/rr/ca.crt"),
                false,
                "rr.example.com"
        );

        String token = System.getenv("RR_CLIENT_TOKEN");

        RrClient client = RrClientBuilder.newBuilder(List.of(
                        "rr-1.example.com:51001",
                        "rr-2.example.com:51001",
                        "rr-3.example.com:51001"
                ))
                .withTls(tls)
                .withBearerToken(token)
                .withStrictSecurity(true)
                .build()
                .join();

        byte[] response = client.sendRequest(1L, "hello".getBytes()).join();
        System.out.println("got " + response.length + " bytes");
        client.close();
    }
}
```

> Note: Java cannot define a static method literally named `new(...)` on `RrClientBuilder`
> because `new` is a language keyword. The SDK provides `RrClientBuilder.newBuilder(...)`.

## Idempotency

When the network is flaky and you cannot tell whether a request was applied,
`sendRequestIdempotent(...)` makes retries safe:

```java
import io.github.gardenias.rr.client.IdempotencyKey;

IdempotencyKey key = IdempotencyKey.of("create_order", 42L);
byte[] response = client.sendRequestIdempotent(1L, payload, key).join();
```

If `request_id` is below the framework watermark and evicted from the LRU cache,
the call completes exceptionally with `IdempotencyExpiredException`.

## Cluster discovery

```java
rr.client.Client.ClusterInfo info = client.getClusterInfo().join();
for (rr.client.Client.GroupInfo group : info.getGroupsList()) {
    System.out.printf(
            "group %s leader=%s replicas=%s%n",
            Long.toUnsignedString(group.getGroupId()),
            Long.toUnsignedString(group.getLeaderNodeId()),
            group.getReplicaNodeIdsList());
}
```

## Metrics

The SDK emits Micrometer meters:

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `rr.client.request.duration` | timer | `group_id`, `outcome={success,failure,idempotency_expired}` | End-to-end request latency including retries |
| `rr.client.retries` | counter | `group_id` | Number of retry attempts |
| `rr.client.leader_cache.hits` | counter | `group_id` | Cached leader used |
| `rr.client.leader_cache.misses` | counter | `group_id` | Cache miss, bootstrap address used |

These dot-separated names map naturally to underscore form in Prometheus exporters.

## Security notes

- Bearer tokens travel in plaintext in gRPC metadata. Pair bearer auth with TLS in production.
- `withStrictSecurity(true)` rejects bearer-token-without-TLS configuration at build time.
- `TlsConfig.domainName` sets gRPC override authority / TLS SNI host for certificate verification.

## License

MIT — see the repository [LICENSE](https://github.com/gardenias/rr/blob/main/LICENSE).

## Links

- Main project: <https://github.com/gardenias/rr>
- Documentation site: <https://rr-framework.github.io>
- Compatibility & semver policy: <https://github.com/gardenias/rr/blob/main/docs/docs/reference/compatibility.md>
- **Reactive usage guide / 响应式使用手册**: [`docs/reactive-guide.md`](./docs/reactive-guide.md) — `CompletableFuture` patterns, error handling, batching, timeouts, Reactor/RxJava interop
