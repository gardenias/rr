package io.github.gardenias.rr.client;

/**
 * Thrown when the request's {@code request_type} would open a NEW idempotency
 * namespace beyond the per-group cap. This is a deterministic rejection:
 * picking a fresh {@code request_id} does NOT help — every ID in the rejected
 * namespace is refused while the group tracks {@code max} namespaces.
 * Resubmit under an already-tracked {@code request_type} (or ask operators
 * about the cap).
 */
public final class NamespaceLimitException extends RrClientException {
    private final String requestType;
    private final long requestId;
    private final long namespaces;
    private final long max;

    /**
     * @param requestId  Wire is uint64; values &gt;= 2^63 will be observed as negative
     *                   {@code long} but the bit pattern is preserved. Use
     *                   {@code Long.toUnsignedString(requestId)} for display.
     * @param namespaces Wire is uint64; see {@code requestId}.
     * @param max        Wire is uint64; see {@code requestId}.
     */
    public NamespaceLimitException(String requestType, long requestId, long namespaces, long max) {
        super(
                "idempotency namespace limit exceeded: request_type="
                        + requestType
                        + ", request_id="
                        + Long.toUnsignedString(requestId)
                        + ", namespaces="
                        + Long.toUnsignedString(namespaces)
                        + ", max="
                        + Long.toUnsignedString(max));
        this.requestType = requestType;
        this.requestId = requestId;
        this.namespaces = namespaces;
        this.max = max;
    }

    public String requestType() {
        return requestType;
    }

    /** Wire is uint64; use {@code Long.toUnsignedString(requestId)} for display. */
    public long requestId() {
        return requestId;
    }

    /** Namespaces currently tracked on the group. Wire is uint64; see {@link #requestId()}. */
    public long namespaces() {
        return namespaces;
    }

    /** The configured cap. Wire is uint64; see {@link #requestId()}. */
    public long max() {
        return max;
    }
}
