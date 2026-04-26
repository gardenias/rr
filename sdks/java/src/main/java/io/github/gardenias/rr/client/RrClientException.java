package io.github.gardenias.rr.client;

/** Base unchecked exception for rr Java SDK client failures. */
public class RrClientException extends RuntimeException {
    public RrClientException(String message) {
        super(message);
    }

    public RrClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
