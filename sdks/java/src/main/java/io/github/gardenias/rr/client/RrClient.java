package io.github.gardenias.rr.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.StatusException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import rr.client.Client;
import rr.client.ClientServiceGrpc;

/**
 * Java client for rr distributed Multi-Raft clusters.
 *
 * <p>Provides three calling styles:
 *
 * <ul>
 *   <li><b>Reactive</b> ({@code sendRequest}, {@code read}, {@code getClusterInfo}) — returns
 *       {@link CompletableFuture} immediately; the gRPC I/O is driven by Netty's event loop so no
 *       thread is blocked while waiting for the response. Chain callbacks with
 *       {@code .thenAccept()} / {@code .thenApply()}.
 *   <li><b>Blocking</b> ({@code sendRequestSync}, {@code readSync}, {@code getClusterInfoSync}) —
 *       blocks the calling thread until the response arrives. Suitable for simple sequential
 *       programs or contexts where threading overhead does not matter.
 * </ul>
 */
public final class RrClient implements AutoCloseable {
    private final List<String> bootstrapAddrs;
    private final RetryConfig retryConfig;
    private final TlsConfig tlsConfig;
    private final String bearerToken;
    private final MeterRegistry meterRegistry;
    private final RrClientBuilder.ChannelFactory channelFactory;

    private final LeaderCache leaderCache = new LeaderCache();
    private final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ClientInterceptor bearerTokenInterceptor;

    RrClient(
            List<String> bootstrapAddrs,
            RetryConfig retryConfig,
            TlsConfig tlsConfig,
            String bearerToken,
            MeterRegistry meterRegistry,
            RrClientBuilder.ChannelFactory channelFactory) {
        this.bootstrapAddrs = List.copyOf(bootstrapAddrs);
        this.retryConfig = Objects.requireNonNull(retryConfig, "retryConfig");
        this.tlsConfig = tlsConfig;
        this.bearerToken = bearerToken;
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.channelFactory = Objects.requireNonNull(channelFactory, "channelFactory");
        this.bearerTokenInterceptor = bearerToken == null ? null : new BearerTokenInterceptor(bearerToken);
    }

    public static CompletableFuture<RrClient> connect(List<String> addrs) {
        return RrClientBuilder.newBuilder(addrs).build();
    }

    public static CompletableFuture<RrClient> connectWithConfig(List<String> addrs, RetryConfig retryConfig) {
        return RrClientBuilder.newBuilder(addrs).withRetryConfig(retryConfig).build();
    }

    // ── Reactive interface (non-blocking, callback-driven) ────────────────────

    /**
     * Reactive write: sends the request via Netty's non-blocking I/O and returns a
     * {@link CompletableFuture} immediately. No thread is blocked while waiting for the response.
     *
     * <p>Use callbacks to handle the result without blocking:
     * <pre>{@code
     * client.sendRequest(groupId, payload)
     *       .thenAccept(resp -> handleResponse(resp))
     *       .exceptionally(ex -> { handleError(ex); return null; });
     * }</pre>
     */
    public CompletableFuture<byte[]> sendRequest(long groupId, byte[] payload) {
        Timer.Sample sample = Timer.start(meterRegistry);
        return sendRequestAsync(groupId, payload, null, 0, 0)
                .whenComplete((result, ex) ->
                        sample.stop(requestTimer(groupId, timerOutcome(ex))));
    }

    /** Reactive idempotent write. See {@link #sendRequest} for the reactive usage pattern. */
    public CompletableFuture<byte[]> sendRequestIdempotent(
            long groupId, byte[] payload, IdempotencyKey key) {
        Objects.requireNonNull(key, "key");
        Timer.Sample sample = Timer.start(meterRegistry);
        return sendRequestAsync(groupId, payload, key, 0, 0)
                .whenComplete((result, ex) ->
                        sample.stop(requestTimer(groupId, timerOutcome(ex))));
    }

    /** Alias: routes by partition ID (same as group ID in single-group deployments). */
    public CompletableFuture<byte[]> sendRequestWithPartition(long partitionId, byte[] payload) {
        return sendRequest(partitionId, payload);
    }

    /**
     * Reactive stale read: routes to any replica without Raft consensus.
     * No leader redirect — any node serves the request. Returns a {@link CompletableFuture}
     * immediately; no thread is blocked during I/O.
     */
    public CompletableFuture<byte[]> read(long groupId, byte[] payload) {
        Timer.Sample sample = Timer.start(meterRegistry);
        return readAsync(groupId, payload, 0, 0)
                .whenComplete((result, ex) ->
                        sample.stop(requestTimer(groupId, timerOutcome(ex))));
    }

    /** Reactive cluster-info fetch. Returns a {@link CompletableFuture} immediately. */
    public CompletableFuture<Client.ClusterInfo> getClusterInfo() {
        return clusterInfoAsync(0);
    }

    // ── Blocking interface ────────────────────────────────────────────────────

    /**
     * Blocking write: blocks the calling thread until the response arrives.
     *
     * <p>Equivalent to {@code sendRequest(groupId, payload).get()}, but wraps
     * checked exceptions into {@link RrClientException} subclasses.
     *
     * @throws RrClientException on transport error, max retries exhausted, or idempotency expiry
     */
    public byte[] sendRequestSync(long groupId, byte[] payload) {
        return blockOn(sendRequest(groupId, payload));
    }

    /** Blocking idempotent write. See {@link #sendRequestSync}. */
    public byte[] sendRequestIdempotentSync(long groupId, byte[] payload, IdempotencyKey key) {
        return blockOn(sendRequestIdempotent(groupId, payload, key));
    }

    /** Blocking stale read. See {@link #sendRequestSync}. */
    public byte[] readSync(long groupId, byte[] payload) {
        return blockOn(read(groupId, payload));
    }

    /** Blocking cluster-info fetch. See {@link #sendRequestSync}. */
    public Client.ClusterInfo getClusterInfoSync() {
        return blockOn(getClusterInfo());
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public List<String> bootstrapAddrs() {
        return bootstrapAddrs;
    }

    public RetryConfig retryConfig() {
        return retryConfig;
    }

    TlsConfig tlsConfig() {
        return tlsConfig;
    }

    String bearerToken() {
        return bearerToken;
    }

    void updateLeaderCache(long groupId, long nodeId, String addr) {
        leaderCache.update(groupId, nodeId, addr);
    }

    Optional<LeaderCache.LeaderInfo> getCachedLeader(long groupId) {
        return leaderCache.get(groupId);
    }

    void removeCachedLeader(long groupId) {
        leaderCache.remove(groupId);
    }

    // ── Internal async helpers ────────────────────────────────────────────────

    /**
     * Core async write with retry. Uses gRPC FutureStub so the underlying I/O is
     * handled by Netty's event loop — no thread is blocked.
     */
    private CompletableFuture<byte[]> sendRequestAsync(
            long groupId, byte[] payload, IdempotencyKey idemKey,
            int attempt, int bootstrapIndex) {

        int maxAttempts = retryConfig.maxRetries() + 1;
        if (attempt >= maxAttempts) {
            return CompletableFuture.failedFuture(
                    new RpcFailedException("<exhausted>", "max retries exhausted"));
        }

        if (attempt > 0) {
            retriesCounter(groupId).increment();
        }

        final String addr = chooseAddr(groupId, bootstrapIndex);
        final int nextBootstrapIndex = bootstrapIndex + 1;

        Client.ClientRequest.Builder reqBuilder = Client.ClientRequest.newBuilder()
                .setPartitionId(groupId)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload));
        if (idemKey != null) {
            Client.IdempotencyKey.Builder ikb = Client.IdempotencyKey.newBuilder()
                    .setRequestType(idemKey.requestType())
                    .setRequestId(idemKey.requestId());
            if (idemKey.clientId() != null) {
                ikb.setClientId(idemKey.clientId());
            }
            reqBuilder.setIdemKey(ikb.build());
        }

        // FutureStub: gRPC sends the request via Netty, returns a ListenableFuture
        // that completes on Netty's I/O thread — no blocking involved.
        ListenableFuture<Client.ClientResponse> grpcFuture =
                ClientServiceGrpc.newFutureStub(channelFor(addr)).request(reqBuilder.build());

        return fromListenableFuture(grpcFuture)
                .thenCompose(response -> {
                    if (response.hasIdemError()) {
                        Client.IdempotencyError e = response.getIdemError();
                        return CompletableFuture.failedFuture(new IdempotencyExpiredException(
                                e.getRequestType(), e.getRequestId(), e.getWatermark()));
                    }
                    if (response.hasLeaderInfo()) {
                        Client.LeaderInfo li = response.getLeaderInfo();
                        updateLeaderCache(groupId, li.getNodeId(), li.getAddr());
                        if (!response.getSuccess()) {
                            return asyncDelay(attempt).thenCompose(__ ->
                                    sendRequestAsync(groupId, payload, idemKey,
                                            attempt + 1, nextBootstrapIndex));
                        }
                    }
                    return CompletableFuture.completedFuture(
                            response.getPayload().toByteArray());
                })
                .exceptionallyCompose(ex -> {
                    if (ex instanceof IdempotencyExpiredException) {
                        return CompletableFuture.failedFuture(ex);
                    }
                    removeCachedLeader(groupId);
                    if (attempt + 1 >= maxAttempts) {
                        RuntimeException err = ex instanceof RrClientException
                                ? (RrClientException) ex
                                : new NetworkException(addr, "connection failed", ex);
                        return CompletableFuture.failedFuture(err);
                    }
                    return asyncDelay(attempt).thenCompose(__ ->
                            sendRequestAsync(groupId, payload, idemKey,
                                    attempt + 1, nextBootstrapIndex));
                });
    }

    private CompletableFuture<byte[]> readAsync(
            long groupId, byte[] payload, int attempt, int bootstrapIndex) {

        int maxAttempts = retryConfig.maxRetries() + 1;
        if (attempt >= maxAttempts) {
            return CompletableFuture.failedFuture(
                    new RpcFailedException("<exhausted>", "max retries exhausted"));
        }

        final String addr = bootstrapAddrs.get(bootstrapIndex % bootstrapAddrs.size());
        final int nextBootstrapIndex = bootstrapIndex + 1;

        Client.ReadLocalRequest req = Client.ReadLocalRequest.newBuilder()
                .setPartitionId(groupId)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .build();

        ListenableFuture<Client.ReadLocalResponse> grpcFuture =
                ClientServiceGrpc.newFutureStub(channelFor(addr)).readLocal(req);

        return fromListenableFuture(grpcFuture)
                .thenApply(resp -> resp.getPayload().toByteArray())
                .exceptionallyCompose(ex -> {
                    if (attempt + 1 >= maxAttempts) {
                        RuntimeException err = ex instanceof RrClientException
                                ? (RrClientException) ex
                                : new NetworkException(addr, "read failed", ex);
                        return CompletableFuture.failedFuture(err);
                    }
                    return asyncDelay(attempt).thenCompose(__ ->
                            readAsync(groupId, payload, attempt + 1, nextBootstrapIndex));
                });
    }

    private CompletableFuture<Client.ClusterInfo> clusterInfoAsync(int bootstrapIndex) {
        if (bootstrapIndex >= bootstrapAddrs.size()) {
            return CompletableFuture.failedFuture(
                    new NetworkException("<all>", "no reachable bootstrap nodes"));
        }
        final String addr = bootstrapAddrs.get(bootstrapIndex);
        ListenableFuture<Client.ClusterInfo> grpcFuture =
                ClientServiceGrpc.newFutureStub(channelFor(addr))
                        .getClusterInfo(Client.Empty.getDefaultInstance());
        return fromListenableFuture(grpcFuture)
                .exceptionallyCompose(ex ->
                        clusterInfoAsync(bootstrapIndex + 1));
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    /**
     * Converts a gRPC {@link ListenableFuture} (backed by Netty's event loop) into a
     * {@link CompletableFuture} without occupying any thread while waiting.
     * The callback fires on Netty's I/O thread when the response arrives.
     */
    private static <T> CompletableFuture<T> fromListenableFuture(ListenableFuture<T> lf) {
        CompletableFuture<T> cf = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                lf.cancel(mayInterruptIfRunning);
                return super.cancel(mayInterruptIfRunning);
            }
        };
        // Runnable::run is a direct executor: runs on the thread that calls run(),
        // which is Netty's I/O thread — no extra thread switch, no blocking.
        lf.addListener(() -> {
            try {
                cf.complete(lf.get());
            } catch (ExecutionException e) {
                cf.completeExceptionally(e.getCause());
            } catch (Exception e) {
                cf.completeExceptionally(e);
            }
        }, Runnable::run);
        return cf;
    }

    /** Non-blocking delay using a JDK scheduled executor — no thread is held during the wait. */
    private CompletableFuture<Void> asyncDelay(int attempt) {
        long multiplier = 1L << Math.min(attempt, 20);
        long delayMs = retryConfig.retryDelayMs() * multiplier;
        if (delayMs <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(
                () -> { },
                CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS));
    }

    private String chooseAddr(long groupId, int bootstrapIndex) {
        Optional<LeaderCache.LeaderInfo> cached = getCachedLeader(groupId);
        if (cached.isPresent()) {
            leaderCacheHits(groupId).increment();
            return cached.get().addr();
        }
        leaderCacheMisses(groupId).increment();
        return bootstrapAddrs.get(bootstrapIndex % bootstrapAddrs.size());
    }

    private static <T> T blockOn(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RrClientException) throw (RrClientException) cause;
            throw new NetworkException("<unknown>", "request failed", cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NetworkException("<unknown>", "interrupted", e);
        }
    }

    private static String timerOutcome(Throwable ex) {
        if (ex == null) return "success";
        if (ex instanceof IdempotencyExpiredException) return "idempotency_expired";
        return "failure";
    }

    private ManagedChannel channelFor(String addr) {
        return channels.computeIfAbsent(
                addr, a -> channelFactory.createChannel(a, tlsConfig, bearerTokenInterceptor));
    }

    private Timer requestTimer(long groupId, String outcome) {
        return Timer.builder("rr.client.request.duration")
                .tag("group_id", Long.toString(groupId))
                .tag("outcome", outcome)
                .register(meterRegistry);
    }

    private Counter retriesCounter(long groupId) {
        return Counter.builder("rr.client.retries")
                .tag("group_id", Long.toString(groupId))
                .register(meterRegistry);
    }

    private Counter leaderCacheHits(long groupId) {
        return Counter.builder("rr.client.leader_cache.hits")
                .tag("group_id", Long.toString(groupId))
                .register(meterRegistry);
    }

    private Counter leaderCacheMisses(long groupId) {
        return Counter.builder("rr.client.leader_cache.misses")
                .tag("group_id", Long.toString(groupId))
                .register(meterRegistry);
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
            try {
                channel.awaitTermination(
                        Duration.ofSeconds(1).toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
    }
}
