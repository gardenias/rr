package io.github.gardenias.rr.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import org.junit.jupiter.api.Test;
import rr.client.Client;
import rr.client.ClientServiceGrpc;

class BearerTokenInterceptorTest {

    @Test
    void headerAttachedWhenTokenSet() {
        Channel channel = mock(Channel.class);
        // reason: Mockito cannot infer nested generic ClientCall type at runtime.
        @SuppressWarnings("unchecked")
        ClientCall<Client.Empty, Client.ClusterInfo> delegate = mock(ClientCall.class);
        when(channel.newCall(eq(ClientServiceGrpc.getGetClusterInfoMethod()), eq(CallOptions.DEFAULT)))
                .thenReturn(delegate);

        BearerTokenInterceptor interceptor = new BearerTokenInterceptor("abc123");
        ClientCall<Client.Empty, Client.ClusterInfo> call =
                interceptor.interceptCall(ClientServiceGrpc.getGetClusterInfoMethod(), CallOptions.DEFAULT, channel);

        Metadata headers = new Metadata();
        call.start(new ClientCall.Listener<>() {}, headers);

        assertThat(headers.get(BearerTokenInterceptor.AUTHORIZATION)).isEqualTo("Bearer abc123");
        verify(delegate).start(any(), same(headers));
    }

    @Test
    void headerOmittedWhenNull() {
        Channel channel = mock(Channel.class);
        // reason: Mockito cannot infer nested generic ClientCall type at runtime.
        @SuppressWarnings("unchecked")
        ClientCall<Client.Empty, Client.ClusterInfo> delegate = mock(ClientCall.class);
        when(channel.newCall(eq(ClientServiceGrpc.getGetClusterInfoMethod()), eq(CallOptions.DEFAULT)))
                .thenReturn(delegate);

        BearerTokenInterceptor interceptor = new BearerTokenInterceptor(null);
        ClientCall<Client.Empty, Client.ClusterInfo> call =
                interceptor.interceptCall(ClientServiceGrpc.getGetClusterInfoMethod(), CallOptions.DEFAULT, channel);

        Metadata headers = new Metadata();
        call.start(new ClientCall.Listener<>() {}, headers);

        assertThat(headers.get(BearerTokenInterceptor.AUTHORIZATION)).isNull();
        verify(delegate).start(any(), same(headers));
    }
}
