package io.github.gardenias.rr.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rr.client.Client;
import rr.client.ClientServiceGrpc;

/**
 * Unit tests for {@link RrClient#readLinearizable}: two in-process gRPC servers play the
 * roles of follower (answers with a {@code leader_info} redirect) and leader (serves the
 * read), exercising the redirect semantics against the real client channel stack.
 */
class RrClientReadLinearizableTest {

    private String followerName;
    private String leaderName;
    private ScriptableClientService followerService;
    private ScriptableClientService leaderService;
    private Server followerServer;
    private Server leaderServer;
    private RrClient client;

    @BeforeEach
    void setUp() throws Exception {
        followerName = InProcessServerBuilder.generateName();
        leaderName = InProcessServerBuilder.generateName();
        followerService = new ScriptableClientService();
        leaderService = new ScriptableClientService();
        followerServer = InProcessServerBuilder.forName(followerName)
                .directExecutor()
                .addService(followerService)
                .build()
                .start();
        leaderServer = InProcessServerBuilder.forName(leaderName)
                .directExecutor()
                .addService(leaderService)
                .build()
                .start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        followerServer.shutdownNow();
        leaderServer.shutdownNow();
        followerServer.awaitTermination(1, TimeUnit.SECONDS);
        leaderServer.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    void servesPayloadDirectlyFromLeader() {
        byte[] expected = "fresh-value".getBytes(StandardCharsets.UTF_8);
        leaderService.enqueue(req -> payloadResponse(expected));
        RrClient c = newClient(List.of(leaderName), new RetryConfig(2, 10));

        byte[] result = c.readLinearizable(1L, "get k".getBytes(StandardCharsets.UTF_8)).join();

        assertThat(result).isEqualTo(expected);
        assertThat(leaderService.requests.get()).isEqualTo(1);
        assertThat(leaderService.seen.peek().getPartitionId()).isEqualTo(1L);
        assertThat(leaderService.seen.peek().getPayload().toByteArray())
                .isEqualTo("get k".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void followsLeaderInfoRedirectToLeader() {
        followerService.enqueue(req -> redirectResponse(2L, leaderName));
        leaderService.enqueue(req -> payloadResponse("v".getBytes(StandardCharsets.UTF_8)));
        RrClient c = newClient(List.of(followerName), new RetryConfig(2, 10));

        byte[] result = c.readLinearizable(1L, new byte[]{1}).join();

        assertThat(result).isEqualTo("v".getBytes(StandardCharsets.UTF_8));
        assertThat(followerService.requests.get()).isEqualTo(1);
        assertThat(leaderService.requests.get()).isEqualTo(1);
    }

    @Test
    void leaderInfoTakesPrecedenceOverPayload() {
        // Proto contract: a present leader_info is a redirect regardless of payload.
        followerService.enqueue(req -> Client.ReadLocalResponse.newBuilder()
                .setPayload(ByteString.copyFrom("stale".getBytes(StandardCharsets.UTF_8)))
                .setLeaderInfo(Client.LeaderInfo.newBuilder().setNodeId(2L).setAddr(leaderName))
                .build());
        leaderService.enqueue(req -> payloadResponse("fresh".getBytes(StandardCharsets.UTF_8)));
        RrClient c = newClient(List.of(followerName), new RetryConfig(1, 10));

        byte[] result = c.readLinearizable(1L, new byte[]{1}).join();

        assertThat(result).isEqualTo("fresh".getBytes(StandardCharsets.UTF_8));
        assertThat(leaderService.requests.get()).isEqualTo(1);
    }

    @Test
    void exhaustsRetryBudgetWhenRedirectLoops() {
        followerService.enqueueAlways(req -> redirectResponse(2L, leaderName));
        leaderService.enqueueAlways(req -> redirectResponse(1L, followerName));
        RrClient c = newClient(List.of(followerName), new RetryConfig(2, 1));

        assertThatThrownBy(() -> c.readLinearizable(1L, new byte[]{1}).join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(RpcFailedException.class)
                .hasMessageContaining("follower redirected to leader");
        // maxRetries=2 → 3 attempts total, shared across both nodes.
        assertThat(followerService.requests.get() + leaderService.requests.get()).isEqualTo(3);
    }

    @Test
    void retriesOnTransportErrorThenSucceedsOnNextBootstrapAddr() {
        // First bootstrap addr has no server behind it → UNAVAILABLE → backoff → next addr.
        leaderService.enqueueAlways(req -> payloadResponse("v".getBytes(StandardCharsets.UTF_8)));
        RrClient c = newClient(List.of("no-such-server", leaderName), new RetryConfig(1, 1));

        byte[] result = c.readLinearizable(1L, new byte[]{1}).join();

        assertThat(result).isEqualTo("v".getBytes(StandardCharsets.UTF_8));
        assertThat(leaderService.requests.get()).isEqualTo(1);
    }

    @Test
    void surfacesNetworkExceptionWhenAllAttemptsFail() {
        RrClient c = newClient(List.of("no-such-server"), new RetryConfig(1, 1));

        assertThatThrownBy(() -> c.readLinearizable(1L, new byte[]{1}).join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(NetworkException.class)
                .hasMessageContaining("linearizable read failed");
    }

    @Test
    void syncVariantReturnsPayload() {
        leaderService.enqueueAlways(req -> payloadResponse("sync-v".getBytes(StandardCharsets.UTF_8)));
        RrClient c = newClient(List.of(leaderName), new RetryConfig(1, 10));

        assertThat(c.readLinearizableSync(1L, new byte[]{1}))
                .isEqualTo("sync-v".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void syncVariantThrowsRrClientExceptionOnFailure() {
        RrClient c = newClient(List.of("no-such-server"), new RetryConfig(0, 1));

        assertThatThrownBy(() -> c.readLinearizableSync(1L, new byte[]{1}))
                .isInstanceOf(RrClientException.class);
    }

    private RrClient newClient(List<String> addrs, RetryConfig retryConfig) {
        client = RrClientBuilder.newBuilder(addrs)
                .withRetryConfig(retryConfig)
                .buildWithFactory((addr, tlsConfig, interceptor) -> {
                    InProcessChannelBuilder builder =
                            InProcessChannelBuilder.forName(addr).directExecutor();
                    if (interceptor != null) {
                        builder.intercept(interceptor);
                    }
                    return builder.build();
                })
                .join();
        return client;
    }

    private static Client.ReadLocalResponse payloadResponse(byte[] payload) {
        return Client.ReadLocalResponse.newBuilder()
                .setPayload(ByteString.copyFrom(payload))
                .build();
    }

    private static Client.ReadLocalResponse redirectResponse(long nodeId, String addr) {
        return Client.ReadLocalResponse.newBuilder()
                .setLeaderInfo(Client.LeaderInfo.newBuilder().setNodeId(nodeId).setAddr(addr))
                .build();
    }

    private static final class ScriptableClientService extends ClientServiceGrpc.ClientServiceImplBase {
        private final Queue<Function<Client.ReadLocalRequest, Client.ReadLocalResponse>> script =
                new ConcurrentLinkedQueue<>();
        private final Queue<Client.ReadLocalRequest> seen = new ConcurrentLinkedQueue<>();
        private final AtomicInteger requests = new AtomicInteger();
        private volatile Function<Client.ReadLocalRequest, Client.ReadLocalResponse> fallback;

        void enqueue(Function<Client.ReadLocalRequest, Client.ReadLocalResponse> behavior) {
            script.add(behavior);
        }

        void enqueueAlways(Function<Client.ReadLocalRequest, Client.ReadLocalResponse> behavior) {
            fallback = behavior;
        }

        @Override
        public void readLinearizable(
                Client.ReadLocalRequest request,
                StreamObserver<Client.ReadLocalResponse> responseObserver) {
            requests.incrementAndGet();
            seen.add(request);
            Function<Client.ReadLocalRequest, Client.ReadLocalResponse> behavior = script.poll();
            if (behavior == null) {
                behavior = fallback;
            }
            if (behavior == null) {
                responseObserver.onError(
                        Status.INTERNAL.withDescription("no scripted behavior").asRuntimeException());
                return;
            }
            responseObserver.onNext(behavior.apply(request));
            responseObserver.onCompleted();
        }
    }
}
