package io.github.gardenias.rr.client;

/** RPC call failed with a transport or remote status error. */
public class RpcFailedException extends NetworkException {
    public RpcFailedException(String addr, String message) {
        super(addr, message);
    }

    public RpcFailedException(String addr, String message, Throwable cause) {
        super(addr, message, cause);
    }
}
