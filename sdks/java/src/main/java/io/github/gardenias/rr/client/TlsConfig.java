package io.github.gardenias.rr.client;

import java.nio.file.Path;
import java.util.Objects;

/**
 * TLS configuration mirroring rr-network's TlsConfig.
 *
 * <p>{@code domainName} changes the TLS SNI/authority used by gRPC via {@code overrideAuthority}.
 */
public final class TlsConfig {
    private final Path certPath;
    private final Path keyPath;
    private final Path caPath;
    private final boolean requireClientCert;
    private final String domainName;

    public TlsConfig(Path certPath, Path keyPath, Path caPath, boolean requireClientCert, String domainName) {
        this.certPath = Objects.requireNonNull(certPath, "certPath");
        this.keyPath = Objects.requireNonNull(keyPath, "keyPath");
        this.caPath = caPath;
        this.requireClientCert = requireClientCert;
        this.domainName = domainName;
    }

    public Path certPath() {
        return certPath;
    }

    public Path keyPath() {
        return keyPath;
    }

    public Path caPath() {
        return caPath;
    }

    public boolean requireClientCert() {
        return requireClientCert;
    }

    public String domainName() {
        return domainName;
    }
}
