package io.github.gardenias.rr.client;

/** Network-level failures (connectivity, TLS wiring, etc.). */
public class NetworkException extends RrClientException {
    private final String addr;

    public NetworkException(String addr, String message) {
        super(message);
        this.addr = addr;
    }

    public NetworkException(String addr, String message, Throwable cause) {
        super(message, cause);
        this.addr = addr;
    }

    public String addr() {
        return addr;
    }
}
