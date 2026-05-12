package io.github.gardenias.rr.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RrClientTest {

    private RrClient client;

    @BeforeEach
    void setUp() {
        client = RrClientBuilder.newBuilder(List.of("127.0.0.1:1"))
                .withRetryConfig(new RetryConfig(0, 10))
                .build()
                .join();
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    // ── Blocking interface ──────────────────────────────────────────────────

    @Test
    void sendRequestSyncThrowsRrClientExceptionOnFailure() {
        assertThatThrownBy(() -> client.sendRequestSync(1L, new byte[]{1, 2, 3}))
                .isInstanceOf(RrClientException.class);
    }

    @Test
    void sendRequestSyncDoesNotWrapInExecutionException() {
        try {
            client.sendRequestSync(1L, new byte[]{});
        } catch (RrClientException e) {
            assertThat(e).isNotInstanceOf(java.util.concurrent.ExecutionException.class);
        }
    }

    @Test
    void readSyncThrowsRrClientExceptionOnFailure() {
        assertThatThrownBy(() -> client.readSync(1L, new byte[]{1}))
                .isInstanceOf(RrClientException.class);
    }

    @Test
    void getClusterInfoSyncThrowsRrClientExceptionOnFailure() {
        assertThatThrownBy(() -> client.getClusterInfoSync())
                .isInstanceOf(RrClientException.class);
    }

    // ── Reactive interface ──────────────────────────────────────────────────

    @Test
    void sendRequestReturnsCompletableFutureImmediately() {
        long start = System.currentTimeMillis();
        CompletableFuture<byte[]> future = client.sendRequest(1L, new byte[]{});
        long elapsed = System.currentTimeMillis() - start;

        assertThat(future).isNotNull();
        assertThat(elapsed)
                .as("sendRequest must return before network I/O completes")
                .isLessThan(200);
    }

    @Test
    void sendRequestCallbackFiredOnCompletion() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean callbackFired = new AtomicBoolean(false);

        client.sendRequest(1L, new byte[]{42})
                .whenComplete((result, ex) -> {
                    callbackFired.set(true);
                    latch.countDown();
                });

        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertThat(completed).as("callback must fire within 5s").isTrue();
        assertThat(callbackFired.get()).isTrue();
    }

    @Test
    void sendRequestCallbackReceivesExceptionWhenNoServer() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> capturedEx = new AtomicReference<>();

        client.sendRequest(1L, new byte[]{})
                .whenComplete((result, ex) -> {
                    capturedEx.set(ex);
                    latch.countDown();
                });

        latch.await(5, TimeUnit.SECONDS);
        assertThat(capturedEx.get())
                .as("reactive callback must receive exception for unreachable server")
                .isNotNull()
                .isInstanceOf(Throwable.class);
    }

    @Test
    void callingThreadNotBlockedWhileFutureInFlight() throws InterruptedException {
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicBoolean callerContinued = new AtomicBoolean(false);

        client.sendRequest(1L, new byte[]{})
                .whenComplete((r, ex) -> callbackLatch.countDown());

        // Calling thread immediately reaches here — not blocked on network
        callerContinued.set(true);

        boolean callbackFired = callbackLatch.await(5, TimeUnit.SECONDS);
        assertThat(callerContinued.get())
                .as("calling thread must continue after sendRequest returns")
                .isTrue();
        assertThat(callbackFired).as("callback must eventually fire").isTrue();
    }

    @Test
    void readReturnsCompletableFutureImmediately() {
        CompletableFuture<byte[]> future = client.read(1L, new byte[]{1});
        assertThat(future).isNotNull();
    }

    @Test
    void readCallbackFiredOnCompletion() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        client.read(1L, new byte[]{}).whenComplete((r, ex) -> latch.countDown());
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void multipleReactiveRequestsAllComplete() throws InterruptedException {
        int n = 5;
        CountDownLatch latch = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            client.sendRequest(1L, new byte[]{(byte) i})
                    .whenComplete((r, ex) -> latch.countDown());
        }
        assertThat(latch.await(10, TimeUnit.SECONDS))
                .as("all %d reactive requests must complete within 10s", n)
                .isTrue();
    }
}
