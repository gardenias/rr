package io.github.gardenias.rr.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rr.client.Client;
import rr.client.ClientServiceGrpc;

/**
 * Write-path regression tests (Rust-parity):
 *
 * <ul>
 *   <li>a {@code success=false} response must NEVER be acknowledged as applied — the
 *       leader-unknown window (no {@code leader_info}) retries via bootstrap and exhausts
 *       into {@link NotLeaderException} with no leader hint;
 *   <li>a redirect ({@code leader_info}) updates the cache and converges on the leader;
 *   <li>redirect exhaustion keeps the NotLeader diagnostics instead of a bare
 *       "max retries exhausted";
 *   <li>{@code ns_limit_error} maps to {@link NamespaceLimitException} without retries;
 *   <li>deterministic gRPC statuses (INVALID_ARGUMENT etc.) fail fast;
 *   <li>{@code withRequestTimeout} bounds a never-answered call.
 * </ul>
 */
class RrClientWriteRedirectTest {

    private Server server;
    private String serverName;
    private ScriptableClientService service;

    @BeforeEach
    void setUp() throws Exception {
        serverName = InProcessServerBuilder.generateName();
        service = new ScriptableClientService();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(service)
                .build()
                .start();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdownNow();
        server.awaitTermination(1, TimeUnit.SECONDS);
    }

    private RrClient newClient(RetryConfig retryConfig) {
        return newClientBuilder(retryConfig).buildWithFactory(this::inProcessChannel).join();
    }

    private RrClientBuilder newClientBuilder(RetryConfig retryConfig) {
        return RrClientBuilder.newBuilder(List.of(serverName)).withRetryConfig(retryConfig);
    }

    private io.grpc.ManagedChannel inProcessChannel(
            String addr, TlsConfig tlsConfig, io.grpc.ClientInterceptor interceptor) {
        InProcessChannelBuilder builder = InProcessChannelBuilder.forName(addr).directExecutor();
        if (interceptor != null) {
            builder.intercept(interceptor);
        }
        return builder.build();
    }

    @Test
    void leaderUnknownWindowNeverFakeSucceeds() {
        service.mode = Mode.LEADER_UNKNOWN;
        try (RrClient client = newClient(new RetryConfig(2, 1))) {
            assertThatThrownBy(() -> client.sendRequest(1L, new byte[] {1}).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(NotLeaderException.class)
                    .satisfies(ex -> {
                        NotLeaderException nle = (NotLeaderException) ex;
                        assertThat(nle.leaderNodeId()).isNull();
                        assertThat(nle.leaderAddr()).isNull();
                    });
            assertThat(service.requests.get())
                    .as("maxAttempts = maxRetries + 1 = 3 bootstrap polls, never a fake success")
                    .isEqualTo(3);
        }
    }

    @Test
    void redirectChainConvergesOnLeader() {
        service.mode = Mode.REDIRECT_THEN_SUCCESS;
        try (RrClient client = newClient(new RetryConfig(3, 1))) {
            byte[] result = client.sendRequest(1L, new byte[] {1}).join();
            assertThat(result).isEqualTo(new byte[] {42});
            assertThat(service.requests.get()).isEqualTo(2);
            assertThat(client.getCachedLeader(1L)).isPresent();
        }
    }

    @Test
    void redirectExhaustionKeepsNotLeaderDiagnostics() {
        service.mode = Mode.REDIRECT_FOREVER;
        try (RrClient client = newClient(new RetryConfig(2, 1))) {
            assertThatThrownBy(() -> client.sendRequest(1L, new byte[] {1}).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(NotLeaderException.class)
                    .satisfies(ex -> {
                        NotLeaderException nle = (NotLeaderException) ex;
                        assertThat(nle.leaderNodeId()).isEqualTo(7L);
                        assertThat(nle.leaderAddr()).isEqualTo(serverName);
                    });
            assertThat(service.requests.get()).isEqualTo(3);
        }
    }

    @Test
    void namespaceLimitErrorIsTypedAndTerminal() {
        service.mode = Mode.NAMESPACE_LIMIT;
        try (RrClient client = newClient(new RetryConfig(5, 1))) {
            assertThatThrownBy(() -> client
                            .sendRequestIdempotent(1L, new byte[] {1}, IdempotencyKey.of("ns-x", 1L))
                            .join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(NamespaceLimitException.class)
                    .satisfies(ex -> {
                        NamespaceLimitException nle = (NamespaceLimitException) ex;
                        assertThat(nle.requestType()).isEqualTo("ns-x");
                        assertThat(nle.max()).isEqualTo(1024L);
                    });
            assertThat(service.requests.get())
                    .as("a deterministic namespace-cap rejection must not be retried")
                    .isEqualTo(1);
        }
    }

    @Test
    void deterministicStatusIsNotRetried() {
        service.mode = Mode.INVALID_ARGUMENT;
        try (RrClient client = newClient(new RetryConfig(5, 1))) {
            assertThatThrownBy(() -> client.sendRequest(1L, new byte[] {1}).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(RpcFailedException.class)
                    .satisfies(ex -> assertThat(ex.getMessage()).contains("INVALID_ARGUMENT"));
            assertThat(service.requests.get())
                    .as("deterministic gRPC rejections fail fast instead of burning the budget")
                    .isEqualTo(1);
        }
    }

    @Test
    void requestTimeoutBoundsParkedCall() {
        service.mode = Mode.NEVER_RESPOND;
        RetryConfig retryConfig = new RetryConfig(1, 1);
        try (RrClient client = newClientBuilder(retryConfig)
                .withRequestTimeout(Duration.ofMillis(50))
                .buildWithFactory(this::inProcessChannel)
                .join()) {
            long started = System.nanoTime();
            assertThatThrownBy(() -> client.sendRequest(1L, new byte[] {1}).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(NetworkException.class);
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;
            assertThat(elapsedMs)
                    .as("2 attempts x 50ms deadline must complete well under the parked-forever shape")
                    .isLessThan(30_000L);
        }
    }

    private enum Mode {
        LEADER_UNKNOWN,
        REDIRECT_THEN_SUCCESS,
        REDIRECT_FOREVER,
        NAMESPACE_LIMIT,
        INVALID_ARGUMENT,
        NEVER_RESPOND,
    }

    private final class ScriptableClientService extends ClientServiceGrpc.ClientServiceImplBase {
        private final AtomicInteger requests = new AtomicInteger();
        private volatile Mode mode = Mode.LEADER_UNKNOWN;

        @Override
        public void request(
                Client.ClientRequest request, StreamObserver<Client.ClientResponse> responseObserver) {
            int hit = requests.incrementAndGet();
            switch (mode) {
                case LEADER_UNKNOWN -> {
                    responseObserver.onNext(Client.ClientResponse.newBuilder()
                            .setSuccess(false)
                            .build());
                    responseObserver.onCompleted();
                }
                case REDIRECT_THEN_SUCCESS -> {
                    if (hit == 1) {
                        responseObserver.onNext(Client.ClientResponse.newBuilder()
                                .setSuccess(false)
                                .setLeaderInfo(Client.LeaderInfo.newBuilder()
                                        .setNodeId(7L)
                                        .setAddr(serverName))
                                .build());
                    } else {
                        responseObserver.onNext(Client.ClientResponse.newBuilder()
                                .setSuccess(true)
                                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[] {42}))
                                .build());
                    }
                    responseObserver.onCompleted();
                }
                case REDIRECT_FOREVER -> {
                    responseObserver.onNext(Client.ClientResponse.newBuilder()
                            .setSuccess(false)
                            .setLeaderInfo(Client.LeaderInfo.newBuilder()
                                    .setNodeId(7L)
                                    .setAddr(serverName))
                            .build());
                    responseObserver.onCompleted();
                }
                case NAMESPACE_LIMIT -> {
                    responseObserver.onNext(Client.ClientResponse.newBuilder()
                            .setSuccess(false)
                            .setNsLimitError(Client.NamespaceLimitError.newBuilder()
                                    .setRequestType("ns-x")
                                    .setRequestId(1L)
                                    .setNamespaces(1024L)
                                    .setMax(1024L))
                            .build());
                    responseObserver.onCompleted();
                }
                case INVALID_ARGUMENT -> responseObserver.onError(
                        Status.INVALID_ARGUMENT.withDescription("bad payload").asException());
                case NEVER_RESPOND -> {
                    // Intentionally never calls onNext/onCompleted: only the
                    // client-side deadline can complete this call.
                }
            }
        }
    }
}
