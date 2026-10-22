package io.github.gardenias.rr.client;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builder for {@link RrClient}. */
public final class RrClientBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(RrClientBuilder.class);
    private static final String TOKEN_WITHOUT_TLS_WARNING =
            "rr-client: bearer token configured without TLS — token will travel in plaintext. "
                    + "This is acceptable only for loopback / mTLS-elsewhere setups. "
                    + "In production, attach with_tls(...) or enable with_strict_security(true).";

    private final List<String> bootstrapAddrs;
    private RetryConfig retryConfig = RetryConfig.defaultConfig();
    private TlsConfig tlsConfig;
    private String bearerToken;
    private boolean strictSecurity;
    private long requestTimeoutMs;
    private MeterRegistry meterRegistry = Metrics.globalRegistry;
    private ChannelFactory channelFactory = new NettyChannelFactory();

    private RrClientBuilder(List<String> addrs) {
        this.bootstrapAddrs = List.copyOf(addrs);
    }

    /**
     * Java keyword constraints prevent a static method literally named {@code new}; use this factory.
     */
    public static RrClientBuilder newBuilder(List<String> addrs) {
        Objects.requireNonNull(addrs, "addrs");
        return new RrClientBuilder(new ArrayList<>(addrs));
    }

    public static RrClientBuilder builder(List<String> addrs) {
        return newBuilder(addrs);
    }

    public RrClientBuilder withTls(TlsConfig tlsConfig) {
        this.tlsConfig = Objects.requireNonNull(tlsConfig, "tlsConfig");
        return this;
    }

    public RrClientBuilder withBearerToken(String token) {
        this.bearerToken = Objects.requireNonNull(token, "token");
        return this;
    }

    public RrClientBuilder withRetryConfig(RetryConfig retryConfig) {
        this.retryConfig = Objects.requireNonNull(retryConfig, "retryConfig");
        return this;
    }

    public RrClientBuilder withStrictSecurity(boolean strictSecurity) {
        this.strictSecurity = strictSecurity;
        return this;
    }

    /**
     * Bounds a single gRPC attempt (mirrors the Rust {@code rr-client}
     * {@code with_request_timeout}). A deadline expiry maps to a retryable
     * failure inside the normal retry budget — the intended use is bounding
     * waits that can otherwise park forever (e.g. deferred two-phase apply).
     * Zero (the default) means unbounded.
     */
    public RrClientBuilder withRequestTimeout(java.time.Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long ms = timeout.toMillis();
        if (ms <= 0) {
            throw new IllegalArgumentException("request timeout must be at least 1 ms");
        }
        this.requestTimeoutMs = ms;
        return this;
    }

    public RrClientBuilder withMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        return this;
    }

    CompletableFuture<RrClient> buildWithFactory(ChannelFactory customFactory) {
        this.channelFactory = Objects.requireNonNull(customFactory, "customFactory");
        return build();
    }

    public CompletableFuture<RrClient> build() {
        if (bootstrapAddrs.isEmpty()) {
            throw new IllegalStateException("no bootstrap addresses provided");
        }

        boolean tokenWithoutTls = bearerToken != null && tlsConfig == null;
        if (tokenWithoutTls) {
            if (strictSecurity) {
                throw new IllegalStateException(
                        "strict security: bearer token set without TLS — enable withTls() or disable withStrictSecurity(false) for dev");
            }
            LOGGER.warn(TOKEN_WITHOUT_TLS_WARNING);
        }

        return CompletableFuture.completedFuture(
                new RrClient(
                        bootstrapAddrs,
                        retryConfig,
                        tlsConfig,
                        bearerToken,
                        requestTimeoutMs,
                        meterRegistry,
                        channelFactory));
    }

    interface ChannelFactory {
        ManagedChannel createChannel(String addr, TlsConfig tlsConfig, ClientInterceptor interceptor);
    }

    static final class NettyChannelFactory implements ChannelFactory {
        @Override
        public ManagedChannel createChannel(String addr, TlsConfig tlsConfig, ClientInterceptor interceptor) {
            NettyChannelBuilder builder = NettyChannelBuilder.forTarget(addr);

            if (tlsConfig != null) {
                builder.useTransportSecurity();
                try {
                    SslContext sslContext = buildSslContext(tlsConfig);
                    builder.sslContext(sslContext);
                } catch (Exception e) {
                    throw new NetworkException(addr, "failed to configure TLS", e);
                }
                if (tlsConfig.domainName() != null && !tlsConfig.domainName().isBlank()) {
                    builder.overrideAuthority(tlsConfig.domainName());
                }
            } else {
                builder.usePlaintext();
            }

            if (interceptor != null) {
                builder.intercept(interceptor);
            }
            return builder.build();
        }

        private SslContext buildSslContext(TlsConfig tlsConfig) throws Exception {
            var sslBuilder = GrpcSslContexts.forClient()
                    .keyManager(tlsConfig.certPath().toFile(), tlsConfig.keyPath().toFile());
            if (tlsConfig.caPath() != null) {
                sslBuilder.trustManager(tlsConfig.caPath().toFile());
            }
            return sslBuilder.build();
        }
    }
}
