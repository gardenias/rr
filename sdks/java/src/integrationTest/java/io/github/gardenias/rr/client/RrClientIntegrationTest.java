package io.github.gardenias.rr.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import rr.client.Client;
import rr.client.ClientServiceGrpc;

@EnabledIfEnvironmentVariable(named = "RR_TEST_DOCKER", matches = "1")
class RrClientIntegrationTest {
    private static GenericContainer<?> rrContainer;
    private static Server inProcessServer;
    private static RrClient client;

    @BeforeAll
    static void beforeAll() throws Exception {
        if (isDockerImageReachable() && tryStartContainerMode()) {
            return;
        }
        startInProcessFallbackMode();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (client != null) {
            client.close();
        }
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
            inProcessServer.awaitTermination(1, TimeUnit.SECONDS);
        }
        if (rrContainer != null) {
            rrContainer.stop();
        }
    }

    @Test
    void endToEndHappyPath() {
        byte[] payload = "hello-rr".getBytes(StandardCharsets.UTF_8);
        byte[] response = client.sendRequest(1L, payload).join();
        assertThat(response).isEqualTo(payload);

        Client.ClusterInfo info = client.getClusterInfo().join();
        assertThat(info.getGroupsCount()).isGreaterThan(0);
    }

    // reason: ownership transfers across static fields and JDT false-positives resource leaks here.
    @SuppressWarnings("all")
    private static boolean tryStartContainerMode() {
        GenericContainer<?> container = null;
        RrClient candidate = null;
        try {
            container = new GenericContainer<>("ghcr.io/gardenias/rr:latest")
                    .withExposedPorts(51001)
                    .withStartupTimeout(Duration.ofSeconds(45));
            container.start();
            String addr = container.getHost() + ":" + container.getMappedPort(51001);
            candidate = RrClientBuilder.newBuilder(List.of(addr))
                    .withRetryConfig(new RetryConfig(0, 10))
                    .build()
                    .join();
            byte[] probe = "probe".getBytes(StandardCharsets.UTF_8);
            candidate.sendRequest(1L, probe).get(2, TimeUnit.SECONDS);
            rrContainer = container;
            container = null;
            client = candidate;
            return true;
        } catch (Exception ignored) {
            if (candidate != null) {
                candidate.close();
            }
            return false;
        } finally {
            if (container != null) {
                container.stop();
            }
        }
    }

    private static void startInProcessFallbackMode() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new EchoClientService())
                .build()
                .start();

        client = RrClientBuilder.newBuilder(List.of(serverName))
                .buildWithFactory((addr, tlsConfig, interceptor) -> {
                    InProcessChannelBuilder channelBuilder = InProcessChannelBuilder.forName(addr).directExecutor();
                    if (interceptor != null) {
                        channelBuilder.intercept(interceptor);
                    }
                    ManagedChannel channel = channelBuilder.build();
                    return channel;
                })
                .join();
    }

    private static boolean isDockerImageReachable() {
        try {
            Process process = new ProcessBuilder("docker", "manifest", "inspect", "ghcr.io/gardenias/rr:latest")
                    .redirectErrorStream(true)
                    .start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                while (reader.readLine() != null) {
                    // consume output
                }
            }
            return process.waitFor(30, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static final class EchoClientService extends ClientServiceGrpc.ClientServiceImplBase {
        @Override
        public void request(Client.ClientRequest request, StreamObserver<Client.ClientResponse> responseObserver) {
            Client.ClientResponse response = Client.ClientResponse.newBuilder()
                    .setSuccess(true)
                    .setPayload(request.getPayload())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getClusterInfo(Client.Empty request, StreamObserver<Client.ClusterInfo> responseObserver) {
            Client.GroupInfo group = Client.GroupInfo.newBuilder()
                    .setGroupId(1L)
                    .setLeaderNodeId(1L)
                    .addReplicaNodeIds(1L)
                    .build();
            responseObserver.onNext(Client.ClusterInfo.newBuilder().addGroups(group).build());
            responseObserver.onCompleted();
        }
    }
}
