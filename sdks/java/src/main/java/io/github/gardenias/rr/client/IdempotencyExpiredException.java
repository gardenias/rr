package io.github.gardenias.rr.client;

/** Thrown when request_id is below framework watermark and already evicted. */
public final class IdempotencyExpiredException extends RrClientException {
    private final String requestType;
    private final long requestId;
    private final long watermark;

    /**
     * @param requestId Wire is uint64; values &gt;= 2^63 will be observed as negative {@code long} but
     *                  the bit pattern is preserved through serialization. Use
     *                  {@code Long.toUnsignedString(requestId)} for display.
     * @param watermark Wire is uint64; values &gt;= 2^63 will be observed as negative {@code long} but
     *                  the bit pattern is preserved through serialization. Use
     *                  {@code Long.toUnsignedString(watermark)} for display.
     */
    public IdempotencyExpiredException(String requestType, long requestId, long watermark) {
        super(
                "idempotency key expired: request_type="
                        + requestType
                        + ", request_id="
                        + Long.toUnsignedString(requestId)
                        + ", watermark="
                        + Long.toUnsignedString(watermark));
        this.requestType = requestType;
        this.requestId = requestId;
        this.watermark = watermark;
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

    /**
     * Wire is uint64; values &gt;= 2^63 will be observed as negative {@code long} but
     * the bit pattern is preserved through serialization. Use
     * {@code Long.toUnsignedString(watermark)} for display.
     */
    public long watermark() {
        return watermark;
    }
}
