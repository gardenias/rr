package io.github.gardenias.rr.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
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
 * Regression test: a server-side idempotency expiry must surface as
 * {@link IdempotencyExpiredException} without consuming retries (failures produced inside a
 * {@code thenCompose} stage reach {@code exceptionallyCompose} wrapped in a
 * {@link CompletionException}; the retry guard must unwrap before classifying).
 */
class RrClientIdempotencyTest {

    private Server server;
    private ExpiringClientService service;
    private RrClient client;

    @BeforeEach
    void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        service = new ExpiringClientService();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(service)
                .build()
                .start();

        client = RrClientBuilder.newBuilder(List.of(serverName))
                .withRetryConfig(new RetryConfig(3, 1))
                .buildWithFactory((addr, tlsConfig, interceptor) -> {
                    InProcessChannelBuilder builder =
                            InProcessChannelBuilder.forName(addr).directExecutor();
                    if (interceptor != null) {
                        builder.intercept(interceptor);
                    }
                    return builder.build();
                })
                .join();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        server.shutdownNow();
        server.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    void idemErrorSurfacesTypedExceptionWithoutRetry() {
        assertThatThrownBy(() -> client
                        .sendRequestIdempotent(1L, new byte[]{1}, IdempotencyKey.of("create_order", 42L))
                        .join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(IdempotencyExpiredException.class)
                .satisfies(ex -> {
                    IdempotencyExpiredException iee = (IdempotencyExpiredException) ex;
                    assertThat(iee.requestType()).isEqualTo("create_order");
                    assertThat(iee.requestId()).isEqualTo(42L);
                    assertThat(iee.watermark()).isEqualTo(100L);
                });
        assertThat(service.requests.get())
                .as("idempotency expiry is terminal — no automatic retries")
                .isEqualTo(1);
    }

    @Test
    void syncVariantThrowsIdempotencyExpiredException() {
        assertThatThrownBy(() -> client.sendRequestIdempotentSync(
                        1L, new byte[]{1}, IdempotencyKey.of("create_order", 42L)))
                .isInstanceOf(IdempotencyExpiredException.class);
        assertThat(service.requests.get()).isEqualTo(1);
    }

    private static final class ExpiringClientService extends ClientServiceGrpc.ClientServiceImplBase {
        private final AtomicInteger requests = new AtomicInteger();

        @Override
        public void request(
                Client.ClientRequest request, StreamObserver<Client.ClientResponse> responseObserver) {
            requests.incrementAndGet();
            Client.IdempotencyError error = Client.IdempotencyError.newBuilder()
                    .setRequestType("create_order")
                    .setRequestId(42L)
                    .setWatermark(100L)
                    .build();
            responseObserver.onNext(Client.ClientResponse.newBuilder()
                    .setSuccess(false)
                    .setIdemError(error)
                    .build());
            responseObserver.onCompleted();
        }
    }
}
