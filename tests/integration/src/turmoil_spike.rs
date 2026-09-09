//! R-3 spike evidence: tonic 0.14 gRPC over turmoil 0.7 simulated network.
//!
//! This test answers the load-bearing feasibility question of the turmoil
//! spike (`.sisyphus/plans/rr-reliability-program.md`, R-3): can a production
//! tonic server and client communicate over turmoil's simulated TCP?
//!
//! - server: `tonic::transport::Server::serve_with_incoming` over a
//!   `turmoil::net::TcpListener` (stream of `TokioIo<TcpStream>`);
//! - client: `Endpoint::connect_with_connector` with a connector that dials
//!   `turmoil::net::TcpStream` and wraps it in `hyper_util::rt::TokioIo`.
//!
//! If both directions work, the R-3 pilot is a mechanical refactor
//! (StreamConnector seam + per-host ClusterBuilder); if either fails, the
//! whole turmoil path is dead regardless of refactor cost. Verdict and
//! measurements: `.sisyphus/evidence/rr-r3-turmoil-spike-2026-09-09.md`.
//!
//! The service under test is a minimal hand-rolled unary gRPC echo (same
//! prost/tonic versions as the production stack), not an rr service — the
//! spike isolates transport feasibility from rr wiring.

#[cfg(test)]
mod turmoil_tonic_spike {
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    /// `rr.spike.Echo` unary request/response (prost, matching production).
    #[derive(Clone, prost::Message, PartialEq)]
    struct EchoReq {
        #[prost(bytes = "vec", tag = "1")]
        payload: Vec<u8>,
    }

    #[derive(Clone, prost::Message, PartialEq)]
    struct EchoResp {
        #[prost(bytes = "vec", tag = "1")]
        payload: Vec<u8>,
    }

    // ── Hand-rolled server, mirroring tonic-prost generated code ────────────

    #[derive(Clone)]
    struct EchoImpl;

    mod echo_grpc {
        use super::*;

        pub const SERVICE_NAME: &str = "rr.spike.Echo";
        pub const ECHO_PATH: &str = "/rr.spike.Echo/Echo";

        /// The stateless echo handler; generated-code shape keeps the `Arc`
        /// field for parity with real services.
        pub struct EchoUnary<T>(#[allow(dead_code)] pub Arc<T>);

        impl tonic::server::UnaryService<EchoReq> for EchoUnary<EchoImpl> {
            type Response = EchoResp;
            type Future = std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<tonic::Response<EchoResp>, tonic::Status>,
                        > + Send,
                >,
            >;

            fn call(&mut self, request: tonic::Request<EchoReq>) -> Self::Future {
                Box::pin(async move {
                    let payload = request.into_inner().payload;
                    Ok(tonic::Response::new(EchoResp { payload }))
                })
            }
        }

        /// Path-dispatching service, the shape `tonic_prost_build` generates.
        #[derive(Clone)]
        pub struct EchoServer {
            inner: Arc<EchoImpl>,
        }

        impl EchoServer {
            pub fn new(inner: Arc<EchoImpl>) -> Self {
                Self { inner }
            }
        }

        impl<B> tower_service::Service<http::Request<B>> for EchoServer
        where
            B: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
            B::Error: Into<tonic::codegen::StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::Body>;
            type Error = Infallible;
            type Future = std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
            >;

            fn poll_ready(
                &mut self,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }

            fn call(&mut self, req: http::Request<B>) -> Self::Future {
                match req.uri().path() {
                    ECHO_PATH => {
                        let inner = Arc::clone(&self.inner);
                        Box::pin(async move {
                            let codec = tonic_prost::ProstCodec::default();
                            let mut grpc = tonic::server::Grpc::new(codec);
                            let res = grpc.unary(EchoUnary(inner), req).await;
                            Ok(res)
                        })
                    }
                    _ => Box::pin(async move {
                        Ok(http::Response::builder()
                            .status(404)
                            .header("grpc-status", "12")
                            .body(tonic::body::Body::default())
                            .unwrap())
                    }),
                }
            }
        }

        impl tonic::server::NamedService for EchoServer {
            const NAME: &'static str = SERVICE_NAME;
        }
    }

    // ── turmile listener → tonic incoming stream ─────────────────────────────

    /// Server-side IO wrapper: tonic's `serve_with_incoming` requires
    /// `Connected` (no blanket impl), while turmoil's stream already speaks
    /// tokio `AsyncRead`/`AsyncWrite`. `TokioIo` would be the wrong direction
    /// here (it grants hyper traits to tokio types, not vice versa).
    struct SimIo(turmoil::net::TcpStream);

    impl tonic::transport::server::Connected for SimIo {
        type ConnectInfo = ();
        fn connect_info(&self) -> Self::ConnectInfo {}
    }

    impl tokio::io::AsyncRead for SimIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for SimIo {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            data: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.0).poll_write(cx, data)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.0).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
        }
        fn poll_write_vectored(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
        }
        fn is_write_vectored(&self) -> bool {
            self.0.is_write_vectored()
        }
    }

    fn turmoil_incoming(
        listener: turmoil::net::TcpListener,
    ) -> impl futures_core::Stream<Item = std::io::Result<SimIo>> {
        async_stream::stream! {
            loop {
                match listener.accept().await {
                    Ok((stream, _peer)) => yield Ok(SimIo(stream)),
                    Err(e) => yield Err(e),
                }
            }
        }
    }

    // ── client connector dialing through the simulated network ──────────────

    #[derive(Clone, Default)]
    struct TurmoilConnector;

    impl tower_service::Service<tonic::codegen::http::Uri> for TurmoilConnector {
        type Response = hyper_util::rt::TokioIo<turmoil::net::TcpStream>;
        type Error = std::io::Error;
        type Future = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
        >;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, uri: tonic::codegen::http::Uri) -> Self::Future {
            Box::pin(async move {
                let host = uri.host().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "uri has no host")
                })?;
                let port = uri.port_u16().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "uri has no port")
                })?;
                let addr: SocketAddr = format!("{host}:{port}")
                    .parse()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                let stream = turmoil::net::TcpStream::connect(addr).await?;
                Ok(hyper_util::rt::TokioIo::new(stream))
            })
        }
    }

    // ── client call (hand-rolled, mirrors rr-raft's RaftGrpcClient) ─────────

    async fn echo_once(channel: tonic::transport::Channel, payload: Vec<u8>) -> Vec<u8> {
        let mut grpc = tonic::client::Grpc::new(channel);
        grpc.ready().await.expect("channel ready");
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(echo_grpc::ECHO_PATH);
        let codec = tonic_prost::ProstCodec::default();
        let resp: tonic::Response<EchoResp> = grpc
            .unary(tonic::Request::new(EchoReq { payload }), path, codec)
            .await
            .expect("unary echo over sim network");
        resp.into_inner().payload
    }

    /// The spike: one server host + one client host in a single `Sim`,
    /// unary echo round-trip through turmoil's simulated TCP. `sim.run`
    /// drives the simulated clock — the whole run takes milliseconds of wall
    /// time regardless of the timers involved.
    #[test]
    fn tonic_echo_over_turmoil_sim() {
        let payload = b"spike-payload".to_vec();

        let mut sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(30))
            .build();

        sim.host("server", || async {
            let listener = turmoil::net::TcpListener::bind("0.0.0.0:9000").await?;
            tonic::transport::Server::builder()
                .add_service(echo_grpc::EchoServer::new(Arc::new(EchoImpl)))
                .serve_with_incoming(turmoil_incoming(listener))
                .await?;
            Ok(())
        });

        let expected = payload.clone();
        sim.host("client", move || {
            let payload = payload.clone();
            let expected = expected.clone();
            async move {
                let endpoint = tonic::transport::Endpoint::from_static("http://server:9000");
                let channel = endpoint.connect_with_connector(TurmoilConnector).await?;
                for _ in 0..3 {
                    let got = echo_once(channel.clone(), payload.clone()).await;
                    assert_eq!(got, expected, "echo round-trip payload mismatch");
                }
                Ok(())
            }
        });

        sim.run().expect("simulation completes without deadlock");
    }
}
