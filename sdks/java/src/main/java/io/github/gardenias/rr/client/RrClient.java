package io.github.gardenias.rr.client;

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
import rr.client.Client;
import rr.client.ClientServiceGrpc;

/** Async Java SDK client for rr distributed Multi-Raft clusters. */
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

    public CompletableFuture<byte[]> sendRequest(long groupId, byte[] payload) {
        return CompletableFuture.supplyAsync(() -> sendRequestSync(groupId, payload, null));
    }

    public CompletableFuture<byte[]> sendRequestIdempotent(long groupId, byte[] payload, IdempotencyKey key) {
        Objects.requireNonNull(key, "key");
        return CompletableFuture.supplyAsync(() -> sendRequestSync(groupId, payload, key));
    }

    public CompletableFuture<byte[]> sendRequestWithPartition(long partitionId, byte[] payload) {
        return sendRequest(partitionId, payload);
    }

    public CompletableFuture<Client.ClusterInfo> getClusterInfo() {
        return CompletableFuture.supplyAsync(this::getClusterInfoSync);
    }

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

    private byte[] sendRequestSync(long groupId, byte[] payload, IdempotencyKey idemKey) {
        int maxAttempts = retryConfig.maxRetries() + 1;
        int bootstrapIndex = 0;
        RuntimeException lastError = null;
        Timer.Sample timerSample = Timer.start(meterRegistry);

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (attempt > 0) {
                retriesCounter(groupId).increment();
            }

            String addr;
            Optional<LeaderCache.LeaderInfo> cachedLeader = getCachedLeader(groupId);
            if (cachedLeader.isPresent()) {
                leaderCacheHits(groupId).increment();
                addr = cachedLeader.get().addr();
            } else {
                leaderCacheMisses(groupId).increment();
                addr = bootstrapAddrs.get(bootstrapIndex % bootstrapAddrs.size());
                bootstrapIndex++;
            }

            try {
                ClientServiceGrpc.ClientServiceBlockingV2Stub stub =
                        ClientServiceGrpc.newBlockingV2Stub(channelFor(addr));

                Client.ClientRequest.Builder request = Client.ClientRequest.newBuilder()
                        .setPartitionId(groupId)
                        .setPayload(com.google.protobuf.ByteString.copyFrom(payload));
                if (idemKey != null) {
                    Client.IdempotencyKey.Builder idemProto = Client.IdempotencyKey.newBuilder()
                            .setRequestType(idemKey.requestType())
                            .setRequestId(idemKey.requestId());
                    if (idemKey.clientId() != null) {
                        idemProto.setClientId(idemKey.clientId());
                    }
                    request.setIdemKey(idemProto.build());
                }

                Client.ClientResponse response = stub.request(request.build());

                if (response.hasIdemError()) {
                    Client.IdempotencyError error = response.getIdemError();
                    timerSample.stop(requestTimer(groupId, "idempotency_expired"));
                    throw new IdempotencyExpiredException(
                            error.getRequestType(),
                            error.getRequestId(),
                            error.getWatermark());
                }

                if (response.hasLeaderInfo()) {
                    Client.LeaderInfo leaderInfo = response.getLeaderInfo();
                    updateLeaderCache(groupId, leaderInfo.getNodeId(), leaderInfo.getAddr());
                    if (!response.getSuccess() && attempt + 1 < maxAttempts) {
                        sleepBackoff(attempt);
                        continue;
                    }
                }

                timerSample.stop(requestTimer(groupId, "success"));
                return response.getPayload().toByteArray();
            } catch (IdempotencyExpiredException e) {
                throw e;
            } catch (StatusException e) {
                removeCachedLeader(groupId);
                String message = e.getStatus().getDescription() == null
                        ? e.getStatus().toString()
                        : e.getStatus().getDescription();
                lastError = new RpcFailedException(addr, message, e);
                if (attempt + 1 < maxAttempts) {
                    sleepBackoff(attempt);
                }
            } catch (RuntimeException e) {
                removeCachedLeader(groupId);
                lastError = e instanceof RrClientException
                        ? e
                        : new NetworkException(addr, "connection failed", e);
                if (attempt + 1 < maxAttempts) {
                    sleepBackoff(attempt);
                }
            }
        }

        timerSample.stop(requestTimer(groupId, "failure"));
        if (lastError != null) {
            throw lastError;
        }
        throw new RpcFailedException("<unknown>", "max retries exhausted");
    }

    private Client.ClusterInfo getClusterInfoSync() {
        RuntimeException lastError = null;
        for (String addr : bootstrapAddrs) {
            try {
                ClientServiceGrpc.ClientServiceBlockingV2Stub stub =
                        ClientServiceGrpc.newBlockingV2Stub(channelFor(addr));
                return stub.getClusterInfo(Client.Empty.getDefaultInstance());
            } catch (StatusException e) {
                String message = e.getStatus().getDescription() == null
                        ? e.getStatus().toString()
                        : e.getStatus().getDescription();
                lastError = new RpcFailedException(addr, message, e);
            } catch (RuntimeException e) {
                lastError = e instanceof RrClientException
                        ? e
                        : new NetworkException(addr, "connection failed", e);
            }
        }

        if (lastError != null) {
            throw lastError;
        }
        throw new NetworkException("<all>", "no reachable bootstrap nodes");
    }

    private ManagedChannel channelFor(String addr) {
        return channels.computeIfAbsent(addr, a -> channelFactory.createChannel(a, tlsConfig, bearerTokenInterceptor));
    }

    private void sleepBackoff(int attempt) {
        long delayMs = retryConfig.retryDelayMs() * (1L << attempt);
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NetworkException("<sleep>", "retry interrupted", e);
        }
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
                channel.awaitTermination(Duration.ofSeconds(1).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
    }
}
