package io.github.gardenias.rr.client;

/** Retry policy for transient transport failures and leader redirects. */
public final class RetryConfig {
    private final int maxRetries;
    private final long retryDelayMs;

    public RetryConfig(int maxRetries, long retryDelayMs) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        if (retryDelayMs < 0) {
            throw new IllegalArgumentException("retryDelayMs must be >= 0");
        }
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    public static RetryConfig defaultConfig() {
        return new RetryConfig(3, 100);
    }

    public int maxRetries() {
        return maxRetries;
    }

    public long retryDelayMs() {
        return retryDelayMs;
    }
}
