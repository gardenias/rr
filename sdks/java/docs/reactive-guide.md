# rr-client-java — 响应式使用手册 / Reactive Usage Guide

> 面向 `io.github.gardenias:rr-client` `0.2.0+`。
> Targets `io.github.gardenias:rr-client` `0.2.0+`.

本手册介绍如何用 `RrClient` 的**响应式 / 非阻塞**接口发起请求并处理返回。所有响应式方法立即返回 `CompletableFuture<T>`，底层 I/O 由 gRPC + Netty 事件循环驱动，等待响应期间**不占用线程**。

---

This guide explains how to issue requests and consume responses with the **reactive / non-blocking** API of `RrClient`. All reactive methods return a `CompletableFuture<T>` immediately; the underlying I/O is driven by the gRPC + Netty event loop, so **no thread is blocked while waiting for the response**.

---

## 目录 / Table of Contents

1. [概述 / Overview](#1-概述--overview)
2. [快速开始 / Quick Start](#2-快速开始--quick-start)
3. [响应式 vs 阻塞 / Reactive vs Blocking](#3-响应式-vs-阻塞--reactive-vs-blocking)
4. [`CompletableFuture` 组合子 / Combinators](#4-completablefuture-组合子--combinators)
5. [错误处理 / Error Handling](#5-错误处理--error-handling)
6. [幂等响应式调用 / Idempotent Reactive Calls](#6-幂等响应式调用--idempotent-reactive-calls)
7. [并发批量请求 / Concurrent Batch Requests](#7-并发批量请求--concurrent-batch-requests)
8. [取消与超时 / Cancellation & Timeout](#8-取消与超时--cancellation--timeout)
9. [Reactor / RxJava 互操作 / Interop](#9-reactor--rxjava-互操作--interop)
10. [集群信息响应式查询 / Cluster Info](#10-集群信息响应式查询--cluster-info)
11. [常见坑 / Common Pitfalls](#11-常见坑--common-pitfalls)
12. [参考 / References](#12-参考--references)

---

## 1. 概述 / Overview

`RrClient` 暴露两套并存的接口：

| 风格 | 方法签名 | 阻塞行为 |
|---|---|---|
| **响应式 / Reactive** | `CompletableFuture<byte[]> sendRequest(long, byte[])` | 立即返回 future；不占线程 |
| **阻塞 / Blocking** | `byte[] sendRequestSync(long, byte[])` | 调用线程一直阻塞到响应到达 |

**响应式接口的优势**：

- **高吞吐**：单线程可同时挂起数千个 in-flight 请求
- **可组合**：用 `thenApply` / `thenCompose` / `allOf` 链式编排
- **可取消**：`future.cancel(true)` 会取消底层 gRPC call
- **响应式生态**：可平滑接入 Reactor `Mono` / RxJava `Single` / Project Loom 虚拟线程

---

`RrClient` exposes two coexisting interfaces:

| Style | Signature | Blocking |
|---|---|---|
| **Reactive** | `CompletableFuture<byte[]> sendRequest(long, byte[])` | Returns immediately; no thread held |
| **Blocking** | `byte[] sendRequestSync(long, byte[])` | Caller thread blocked until response |

**Why reactive**:

- **High throughput** — a single thread can hold thousands of in-flight requests
- **Composable** — chain via `thenApply` / `thenCompose` / `allOf`
- **Cancellable** — `future.cancel(true)` cancels the underlying gRPC call
- **Plays well with reactive stacks** — Reactor `Mono`, RxJava `Single`, Project Loom virtual threads

---

## 2. 快速开始 / Quick Start

最小化的响应式请求示例（开发环境，无 TLS、无 auth）：

```java
import io.github.gardenias.rr.client.RrClient;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ReactiveQuickStart {
    public static void main(String[] args) throws Exception {
        // 1. 建立连接（也是响应式的）
        CompletableFuture<RrClient> clientFuture =
                RrClient.connect(List.of("127.0.0.1:51001"));

        // 2. 链式发请求 + 处理响应
        CompletableFuture<Void> done = clientFuture
                .thenCompose(client -> {
                    byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
                    return client.sendRequest(1L, payload)
                                 .thenAccept(resp -> System.out.println(
                                         "got " + resp.length + " bytes"))
                                 .whenComplete((__, ex) -> client.close());
                })
                .exceptionally(ex -> {
                    System.err.println("request failed: " + ex.getMessage());
                    return null;
                });

        // 3. main 不退出 —— 等链路完成
        done.join();
    }
}
```

---

Minimal reactive request (dev environment, no TLS, no auth):

```java
import io.github.gardenias.rr.client.RrClient;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ReactiveQuickStart {
    public static void main(String[] args) throws Exception {
        // 1. Connect reactively
        CompletableFuture<RrClient> clientFuture =
                RrClient.connect(List.of("127.0.0.1:51001"));

        // 2. Chain request + response handling
        CompletableFuture<Void> done = clientFuture
                .thenCompose(client -> {
                    byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
                    return client.sendRequest(1L, payload)
                                 .thenAccept(resp -> System.out.println(
                                         "got " + resp.length + " bytes"))
                                 .whenComplete((__, ex) -> client.close());
                })
                .exceptionally(ex -> {
                    System.err.println("request failed: " + ex.getMessage());
                    return null;
                });

        // 3. Keep main alive until the chain completes
        done.join();
    }
}
```

---

## 3. 响应式 vs 阻塞 / Reactive vs Blocking

`RrClient` 提供完全对称的两套方法。**底层 gRPC 调用完全一致**——区别只在最后是否阻塞调用线程。

| 响应式 / Reactive | 阻塞 / Blocking | 用途 / Purpose |
|---|---|---|
| `sendRequest(groupId, payload)` | `sendRequestSync(groupId, payload)` | 强一致写 / Linearizable write |
| `sendRequestIdempotent(groupId, payload, key)` | `sendRequestIdempotentSync(groupId, payload, key)` | 幂等写 / Idempotent write |
| `sendRequestWithPartition(partitionId, payload)` | — | 别名 / Alias |
| `read(groupId, payload)` | `readSync(groupId, payload)` | 陈旧读 / Stale read |
| `readLinearizable(groupId, payload)` | `readLinearizableSync(groupId, payload)` | 线性一致读 / Linearizable read |
| `getClusterInfo()` | `getClusterInfoSync()` | 集群拓扑 / Cluster topology |

**何时选哪个 / When to choose which**：

- **响应式 / Reactive** — Web 服务器（Netty、Vert.x、Spring WebFlux）、批量并发请求、和其它 async API 组合
- **阻塞 / Blocking** — 命令行工具、Servlet（Tomcat 默认 thread-per-request）、简单顺序脚本

> 阻塞方法内部就是包装了 `.get()`：
> The blocking methods are simply wrappers around `.get()`:
> ```java
> public byte[] sendRequestSync(long groupId, byte[] payload) {
>     return blockOn(sendRequest(groupId, payload));  // unwraps to RrClientException
> }
> ```

---

## 4. `CompletableFuture` 组合子 / Combinators

### 4.1 转换响应 / Transform the response

`thenApply` — 同步转换值（在 Netty I/O 线程上执行，**只能跑廉价的纯计算**）：

`thenApply` — synchronous value transform (runs on the Netty I/O thread, **only cheap pure work**):

```java
// 把响应字节数组解析为 String
// Parse response bytes into a String
CompletableFuture<String> parsed = client.sendRequest(1L, payload)
        .thenApply(bytes -> new String(bytes, StandardCharsets.UTF_8));
```

`thenApplyAsync` — 把转换调度到 `ForkJoinPool.commonPool()`（**重 CPU 工作用这个**）：

`thenApplyAsync` — dispatch the transform onto `ForkJoinPool.commonPool()` (**use this for CPU-heavy work**):

```java
CompletableFuture<Order> order = client.sendRequest(1L, payload)
        .thenApplyAsync(bytes -> parseOrderProtobuf(bytes));  // 释放 Netty 线程 / frees Netty thread
```

### 4.2 链接下一个异步调用 / Chain the next async call

`thenCompose` —— 类似 `flatMap`，等前一个 future 完成后**触发新的异步调用**：

`thenCompose` — flatMap-like; after the previous future completes, **trigger a new async call**:

```java
// 先查集群信息，再向 leader 发请求
// First fetch cluster info, then send a request to the leader
CompletableFuture<byte[]> chained = client.getClusterInfo()
        .thenCompose(info -> {
            long groupId = info.getGroups(0).getGroupId();
            return client.sendRequest(groupId, payload);
        });
```

### 4.3 副作用 / Side effects

| 组合子 / Combinator | 何时触发 / Fires when | 拿到什么 / Receives |
|---|---|---|
| `thenAccept` | 成功 / success | `T result` |
| `thenRun` | 成功 / success | nothing |
| `whenComplete` | 成功**或**失败 / success **or** failure | `(T result, Throwable ex)` |
| `handle` | 成功**或**失败，可转换 / success **or** failure, transforms | `(T result, Throwable ex) -> R` |

```java
client.sendRequest(1L, payload)
      .thenAccept(resp -> log.info("ok, {} bytes", resp.length))
      .whenComplete((result, ex) -> meterRegistry.counter(
              "app.requests",
              "outcome", ex == null ? "ok" : "fail").increment());
```

### 4.4 错误恢复 / Recovery

`exceptionally` — 失败时给个 fallback：

`exceptionally` — fallback when the future fails:

```java
CompletableFuture<byte[]> safe = client.sendRequest(1L, payload)
        .exceptionally(ex -> {
            log.warn("request failed, returning empty payload", ex);
            return new byte[0];
        });
```

`exceptionallyCompose` (Java 12+) — 失败时**异步**重试或降级：

`exceptionallyCompose` (Java 12+) — **async** retry or degrade on failure:

```java
CompletableFuture<byte[]> withFallback = client.sendRequest(1L, payload)
        .exceptionallyCompose(ex -> {
            if (ex.getCause() instanceof NetworkException) {
                // 主集群挂了 → 走读副本
                // Primary cluster down → fall back to a read replica
                return client.read(1L, payload);
            }
            return CompletableFuture.failedFuture(ex);
        });
```

---

## 5. 错误处理 / Error Handling

**SDK 抛出的所有异常都包装在 `CompletableFuture` 的 failure 路径里**。在 `exceptionally` / `handle` / `whenComplete` 里 unwrap：

**All SDK exceptions are delivered through the future's failure path.** Unwrap them in `exceptionally` / `handle` / `whenComplete`:

```java
client.sendRequestIdempotent(1L, payload, IdempotencyKey.of("create_order", 42L))
      .whenComplete((resp, ex) -> {
          if (ex == null) {
              process(resp);
              return;
          }
          // 注意：CompletableFuture 把异常包成 CompletionException
          // Note: CompletableFuture wraps exceptions in CompletionException
          Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;

          if (cause instanceof IdempotencyExpiredException iee) {
              log.error("idempotency expired: request_id={} watermark={}",
                      Long.toUnsignedString(iee.requestId()),
                      Long.toUnsignedString(iee.watermark()));
              // 业务层应当生成新的 request_id 重试
              // Application should generate a fresh request_id and retry
          } else if (cause instanceof RpcFailedException rpc) {
              log.error("rpc failed at {}: {}", rpc.addr(), rpc.getMessage());
          } else if (cause instanceof NetworkException ne) {
              log.error("network error at {}: {}", ne.addr(), ne.getMessage());
          } else {
              log.error("unknown error", cause);
          }
      });
```

### 异常层级 / Exception Hierarchy

```
RuntimeException
└── RrClientException                     // 基类 / base
    ├── IdempotencyExpiredException       // request_id 已被驱逐 / request_id evicted
    └── NetworkException                  // 连接 / TLS / 通用网络错误 / connect / TLS / generic
        └── RpcFailedException            // gRPC 调用失败 / gRPC call failed
```

**关键点 / Key points**：

- `IdempotencyExpiredException` **不会自动重试**——业务必须用新 `request_id`
- `IdempotencyExpiredException` is **not auto-retried** — the caller must use a new `request_id`
- `NetworkException` / `RpcFailedException` 在 SDK 内**已按 `RetryConfig` 自动重试** `maxRetries` 次后才抛出
- `NetworkException` / `RpcFailedException` are **already auto-retried** up to `RetryConfig.maxRetries` before surfacing
- 默认 `RetryConfig`：3 retries × 100ms exponential backoff
- Default `RetryConfig`: 3 retries × 100ms exponential backoff

---

## 6. 幂等响应式调用 / Idempotent Reactive Calls

网络抖动时无法确认请求是否落库，用 `sendRequestIdempotent` 让重试安全：

When the network flaps and you cannot tell if a request was applied, `sendRequestIdempotent` makes retries safe:

```java
import io.github.gardenias.rr.client.IdempotencyKey;

IdempotencyKey key = IdempotencyKey.of("create_order", 42L);

CompletableFuture<OrderResult> future = client
        .sendRequestIdempotent(1L, payload, key)
        .thenApply(bytes -> OrderResult.parseFrom(bytes))
        .exceptionallyCompose(ex -> {
            Throwable cause = unwrap(ex);
            if (cause instanceof IdempotencyExpiredException) {
                // request_id 已被驱逐 → 生成新 key 重试
                // request_id evicted → generate a new key and retry
                IdempotencyKey freshKey = IdempotencyKey.of(
                        "create_order", System.currentTimeMillis());
                return client.sendRequestIdempotent(1L, payload, freshKey)
                             .thenApply(bytes -> OrderResult.parseFrom(bytes));
            }
            return CompletableFuture.failedFuture(cause);
        });
```

**幂等键设计建议 / Idempotency key design tips**：

- `requestType` 标识**操作类型**（"create_order"、"transfer"）
- `requestType` identifies the **operation type** ("create_order", "transfer")
- `requestId` 在 (clientId, requestType) 内**单调递增、唯一**（雪花 ID / UUID `.getMostSignificantBits()` / 业务 ID）
- `requestId` is **monotonic & unique** within (clientId, requestType) — snowflake / UUID `.getMostSignificantBits()` / business ID
- 线缆上是 `uint64`；Java `long` 高位会显示为负数，**展示请用** `Long.toUnsignedString(...)`
- Wire format is `uint64`; high values appear as negative `long` in Java — **display via** `Long.toUnsignedString(...)`

---

## 7. 并发批量请求 / Concurrent Batch Requests

响应式接口的杀手特性——并发挂起 N 个请求，单线程编排：

The killer feature of the reactive API — fan out N concurrent requests and join them on a single thread:

### 7.1 等所有完成 / Wait for all

```java
import java.util.concurrent.CompletableFuture;
import java.util.List;
import java.util.stream.Collectors;

List<byte[]> payloads = /* N 个 payload / N payloads */;

List<CompletableFuture<byte[]>> futures = payloads.stream()
        .map(p -> client.sendRequest(1L, p))
        .toList();

CompletableFuture<List<byte[]>> allDone = CompletableFuture
        .allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(__ -> futures.stream()
                .map(CompletableFuture::join)  // 此处已全部完成，join 不会阻塞 / all done by now
                .collect(Collectors.toList()));

allDone.thenAccept(results -> log.info("batch ok, {} responses", results.size()));
```

### 7.2 任一完成即返回 / First-to-finish

```java
// 同时向 3 个 group 发请求，第一个响应到达即用
// Fire to 3 groups concurrently, use whichever returns first
CompletableFuture<Object> firstWin = CompletableFuture.anyOf(
        client.sendRequest(1L, payload),
        client.sendRequest(2L, payload),
        client.sendRequest(3L, payload));

firstWin.thenAccept(resp -> log.info("first reply: {} bytes", ((byte[]) resp).length));
```

### 7.3 部分失败容忍 / Partial-failure tolerance

`allOf` 一旦有任一 future 失败，整体就 fail。如果要容忍部分失败，对每个 future 单独 wrap：

`allOf` fails fast if any future fails. To tolerate partial failures, wrap each future individually:

```java
record Outcome(byte[] result, Throwable error) {}

List<CompletableFuture<Outcome>> tolerant = payloads.stream()
        .map(p -> client.sendRequest(1L, p)
                        .<Outcome>handle((r, ex) -> new Outcome(r, ex)))
        .toList();

CompletableFuture.allOf(tolerant.toArray(CompletableFuture[]::new))
        .thenAccept(__ -> {
            long ok = tolerant.stream().map(CompletableFuture::join)
                    .filter(o -> o.error() == null).count();
            log.info("{}/{} succeeded", ok, tolerant.size());
        });
```

---

## 8. 取消与超时 / Cancellation & Timeout

### 8.1 取消 / Cancellation

`RrClient.sendRequest` 返回的 future **支持级联取消** —— `cancel(true)` 会取消底层 gRPC call（见 `fromListenableFuture` 实现）：

The future returned by `RrClient.sendRequest` **supports cascading cancellation** — `cancel(true)` cancels the underlying gRPC call (see `fromListenableFuture`):

```java
CompletableFuture<byte[]> future = client.sendRequest(1L, payload);

// 用户点了"取消"
// User clicked cancel
future.cancel(true);
```

### 8.2 超时 / Timeout (Java 9+)

```java
import java.util.concurrent.TimeUnit;

// 硬超时：超过 2s 直接 fail
// Hard timeout: fail with TimeoutException after 2s
client.sendRequest(1L, payload)
      .orTimeout(2, TimeUnit.SECONDS)
      .exceptionally(ex -> {
          if (ex.getCause() instanceof java.util.concurrent.TimeoutException) {
              log.warn("request timed out");
          }
          return new byte[0];
      });

// 软超时：超时给默认值，不抛异常
// Soft timeout: return a default value on timeout, no exception
client.sendRequest(1L, payload)
      .completeOnTimeout(new byte[0], 2, TimeUnit.SECONDS)
      .thenAccept(resp -> /* ... */);
```

> ⚠️ `orTimeout` 只**信号**层超时——底层 gRPC call 仍然在跑直到完成或被取消。需要真正释放资源，再 `.whenComplete((r, ex) -> future.cancel(true))`。
> ⚠️ `orTimeout` is **signal-level only** — the underlying gRPC call keeps running until completion or explicit cancellation. To free resources, also `.whenComplete((r, ex) -> future.cancel(true))`.

---

## 9. Reactor / RxJava 互操作 / Interop

### 9.1 Project Reactor (`Mono`)

```java
import reactor.core.publisher.Mono;

Mono<byte[]> mono = Mono.fromFuture(() -> client.sendRequest(1L, payload));

mono.timeout(Duration.ofSeconds(2))
    .retry(3)
    .subscribe(
            resp -> log.info("got {} bytes", resp.length),
            ex   -> log.error("failed", ex));
```

> `Mono.fromFuture(Supplier)` 比 `Mono.fromFuture(future)` 更好——前者**订阅时**才触发请求，符合响应式语义。
> Prefer `Mono.fromFuture(Supplier)` over `Mono.fromFuture(future)` — the former triggers the request **on subscription**, matching reactive semantics.

### 9.2 RxJava 3 (`Single`)

```java
import io.reactivex.rxjava3.core.Single;

Single<byte[]> single = Single.defer(() ->
        Single.fromFuture(client.sendRequest(1L, payload)));

single.timeout(2, TimeUnit.SECONDS)
      .retry(3)
      .subscribe(
              resp -> log.info("got {} bytes", resp.length),
              ex   -> log.error("failed", ex));
```

### 9.3 Project Loom (Java 21+ 虚拟线程 / virtual threads)

虚拟线程下，阻塞接口**也变成**轻量级——可以直接用 `sendRequestSync`：

Under virtual threads, the blocking API **also becomes** lightweight — feel free to call `sendRequestSync`:

```java
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    for (int i = 0; i < 10_000; i++) {
        executor.submit(() -> {
            byte[] resp = client.sendRequestSync(1L, payload);  // 看似阻塞，实际挂起虚拟线程
            // looks blocking, actually just parks the virtual thread
            process(resp);
        });
    }
}
```

但如果团队约定**避免线程语义混用**，仍然推荐响应式接口 + `.thenApply(this::process)`。

That said, if the team prefers consistent thread semantics, stick with the reactive API + `.thenApply(this::process)`.

---

## 10. 集群信息响应式查询 / Cluster Info Reactive Query

```java
import rr.client.Client;

client.getClusterInfo()
      .thenAccept(info -> {
          for (Client.GroupInfo group : info.getGroupsList()) {
              System.out.printf("group %s leader=%s replicas=%s%n",
                      Long.toUnsignedString(group.getGroupId()),
                      Long.toUnsignedString(group.getLeaderNodeId()),
                      group.getReplicaNodeIdsList());
          }
      })
      .exceptionally(ex -> {
          log.error("cluster discovery failed", ex);
          return null;
      });
```

`getClusterInfo()` 内部会**依次尝试**所有 bootstrap 地址（见 `clusterInfoAsync`），第一个成功的就返回。**业务无需自己写故障转移**。

`getClusterInfo()` **walks through all bootstrap addresses internally** (see `clusterInfoAsync`) and returns on the first success. **No application-level failover needed**.

---

## 11. 常见坑 / Common Pitfalls

### 11.1 `thenApply` 里跑阻塞 IO — 锁死 Netty / Blocking I/O inside `thenApply` — stalls Netty

❌ **坏 / Bad**：

```java
client.sendRequest(1L, payload)
      .thenApply(bytes -> {
          // 在 Netty I/O 线程上跑 JDBC —— 锁死整个 channel！
          // JDBC on Netty I/O thread — stalls the whole channel!
          return jdbcTemplate.queryForObject("...", String.class);
      });
```

✅ **好 / Good**：

```java
client.sendRequest(1L, payload)
      .thenApplyAsync(bytes ->
              jdbcTemplate.queryForObject("...", String.class),
              ioBoundExecutor);  // 自带的 IO 线程池 / dedicated I/O pool
```

### 11.2 忘了 unwrap `CompletionException` / Forgetting to unwrap `CompletionException`

```java
.exceptionally(ex -> {
    // ex 是 CompletionException，真实异常在 .getCause()
    // ex is CompletionException, real exception is in .getCause()
    Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
    if (cause instanceof IdempotencyExpiredException) { /* ... */ }
    return fallback();
});
```

### 11.3 在响应式链里 `.get()` / Calling `.get()` inside a reactive chain

❌ **坏 / Bad**：

```java
client.sendRequest(1L, p1)
      .thenApply(r1 -> {
          // ⛔ 在 Netty 线程上同步等下一个请求 —— 死锁风险！
          // ⛔ Synchronously waits on Netty thread — deadlock risk!
          return client.sendRequest(1L, r1).get();
      });
```

✅ **好 / Good**：

```java
client.sendRequest(1L, p1)
      .thenCompose(r1 -> client.sendRequest(1L, r1));  // 异步链式 / proper chaining
```

### 11.4 没关 client / Not closing the client

`RrClient` 实现了 `AutoCloseable`。**长生命周期应用**用单例 + JVM shutdown hook；**短脚本**用 try-with-resources：

`RrClient` implements `AutoCloseable`. For **long-lived apps**, use a singleton + JVM shutdown hook; for **short scripts**, try-with-resources:

```java
try (RrClient client = RrClient.connect(addrs).join()) {
    client.sendRequest(1L, payload).join();
}  // 自动关闭 / auto-closed
```

### 11.5 把 `.join()` 当 `.get()` 用，丢异常类型 / Using `.join()` and losing the typed exception

`.join()` 把 checked exception 包成 `CompletionException`。如果你需要 typed exception，用 `.get()` 并处理 `ExecutionException`，或者在链里 `.exceptionally(...)`。

`.join()` wraps checked exceptions in `CompletionException`. If you need the typed exception, use `.get()` and handle `ExecutionException`, or stay reactive with `.exceptionally(...)`.

---

## 12. 参考 / References

- **主 README / Main README**: [`../README.md`](../README.md)
- **SDK 源码 / SDK source**: [`../src/main/java/io/github/gardenias/rr/client/`](../src/main/java/io/github/gardenias/rr/client/)
- **`RrClient.java` Javadoc**: 类文件顶部注释列出了完整的响应式 / 阻塞接口分组 / class-level Javadoc enumerates the full reactive / blocking method split
- **`CompletableFuture` 官方文档 / official Javadoc**: <https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/CompletableFuture.html>
- **Project Reactor**: <https://projectreactor.io/docs>
- **gRPC Java**: <https://grpc.io/docs/languages/java/>
- **兼容性 / Compatibility policy**: <https://github.com/gardenias/rr/blob/main/docs/docs/reference/compatibility.md>
