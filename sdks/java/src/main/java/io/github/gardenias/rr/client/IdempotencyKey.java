package io.github.gardenias.rr.client;

import java.util.Objects;

/** Framework-level idempotency key. */
public final class IdempotencyKey {
    private final String requestType;
    private final long requestId;
    private final String clientId;

    /**
     * @param requestId Wire is uint64; values &gt;= 2^63 will be observed as negative {@code long} but
     *                  the bit pattern is preserved through serialization. Use
     *                  {@code Long.toUnsignedString(requestId)} for display.
     */
    public IdempotencyKey(String requestType, long requestId, String clientId) {
        this.requestType = Objects.requireNonNull(requestType, "requestType");
        this.requestId = requestId;
        this.clientId = clientId;
    }

    public static IdempotencyKey of(String requestType, long requestId) {
        return new IdempotencyKey(requestType, requestId, null);
    }

    public String requestType() {
        return requestType;
    }

    /**
     * Wire is uint64; values &gt;= 2^63 will be observed as negative {@code long} but
     * the bit pattern is preserved through serialization. Use
     * {@code Long.toUnsignedString(requestId)} for display.
     */
    public long requestId() {
        return requestId;
    }

    public String clientId() {
        return clientId;
    }
}
