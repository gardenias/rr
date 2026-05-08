package io.github.gardenias.rr.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class RrClientBuilderTest {

    @Test
    void defaults() {
        RrClient client = RrClientBuilder.newBuilder(List.of("127.0.0.1:51001")).build().join();
        try {
            assertThat(client.bootstrapAddrs()).containsExactly("127.0.0.1:51001");
            assertThat(client.retryConfig().maxRetries()).isEqualTo(3);
            assertThat(client.retryConfig().retryDelayMs()).isEqualTo(100);
            assertThat(client.tlsConfig()).isNull();
            assertThat(client.bearerToken()).isNull();
        } finally {
            client.close();
        }
    }

    @Test
    void withBearerTokenStoresToken() {
        RrClient client = RrClientBuilder.newBuilder(List.of("127.0.0.1:51001"))
                .withBearerToken("secret-test-token")
                .build()
                .join();
        try {
            assertThat(client.bearerToken()).isEqualTo("secret-test-token");
        } finally {
            client.close();
        }
    }

    @Test
    void strictRejectsTokenWithoutTls() {
        assertThatThrownBy(() -> RrClientBuilder.newBuilder(List.of("127.0.0.1:51001"))
                        .withBearerToken("secret-test-token")
                        .withStrictSecurity(true)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "strict security: bearer token set without TLS — enable withTls() or disable withStrictSecurity(false) for dev");
    }

    @Test
    void withRetryConfigOverridesDefault() {
        RetryConfig retryConfig = new RetryConfig(7, 250);
        RrClient client = RrClientBuilder.newBuilder(List.of("127.0.0.1:51001"))
                .withRetryConfig(retryConfig)
                .build()
                .join();
        try {
            assertThat(client.retryConfig().maxRetries()).isEqualTo(7);
            assertThat(client.retryConfig().retryDelayMs()).isEqualTo(250);
        } finally {
            client.close();
        }
    }
    @Test
    void readMethodFailsWithoutServer() {
        RrClient client = RrClientBuilder.newBuilder(List.of("127.0.0.1:1")).build().join();
        try {
            assertThatThrownBy(() -> client.read(1L, new byte[]{1}).join())
                    .hasCauseInstanceOf(RrClientException.class);
        } finally {
            client.close();
        }
    }

    @Test
    void readLinearizableMethodFailsWithoutServer() {
        RrClient client = RrClientBuilder.newBuilder(List.of("127.0.0.1:1")).build().join();
        try {
            assertThatThrownBy(() -> client.readLinearizable(1L, new byte[]{1}).join())
                    .hasCauseInstanceOf(RrClientException.class);
        } finally {
            client.close();
        }
    }
}
