mod auth;
mod cache;
mod config;
mod discovery;
mod http;
mod metrics;
mod model;
mod pagination;
mod scheduler;
mod upstream;

use anyhow::{Context, Result};
pub(crate) use config::ConsoleArgs;
use config::ConsoleConfig;
use etcd_client::{Certificate, ConnectOptions, Identity, TlsOptions};
use rr_config_etcd::{EtcdConfigProvider, EtcdNodeDirectory};
use rr_network::tls::TlsConfig;
use rustls::ServerConfig;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{watch, Semaphore};
use tokio_rustls::TlsAcceptor;

use auth::AuthState;
use cache::ConsoleCache;
use discovery::TrustedDiscovery;
use scheduler::Scheduler;
use upstream::TonicUpstream;

pub(crate) async fn run(
    args: ConsoleArgs,
    admin_tls: Option<TlsConfig>,
    admin_token: Option<Arc<str>>,
) -> Result<()> {
    let config = args.validate()?;
    let discovery = connect_discovery(&config).await?;
    let tls = load_tls(&config)?;
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("bind Console listener {}", config.listen))?;

    let cache = ConsoleCache::production(config.cluster_name.clone(), config.poll_interval);
    let upstream = Arc::new(TonicUpstream::new(admin_tls, admin_token));
    let scheduler = build_scheduler(&config, cache, upstream, discovery);
    if let Some(prometheus) = config.prometheus.clone() {
        scheduler.configure_history(prometheus)?;
    }
    let auth = AuthState::new(
        config.viewer_token.clone(),
        Duration::from_secs(15 * 60),
        config.using_tls(),
    );
    let app = http::router(http::AppState::new(
        scheduler.clone(),
        auth,
        config.static_dir.clone(),
        config.using_tls(),
    ));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let scheduler_task = tokio::spawn(scheduler.clone().run(shutdown_rx.clone()));
    tracing::info!(listen = %config.listen, https = config.using_tls(), "Console listening");

    let result = if let Some(acceptor) = tls {
        serve_tls(listener, acceptor, app, shutdown_rx).await
    } else {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal(shutdown_rx))
            .await
            .context("Console HTTP server")
    };
    let _ = shutdown_tx.send(true);
    scheduler_task.abort();
    let _ = scheduler_task.await;
    result
}

async fn connect_discovery(config: &ConsoleConfig) -> Result<Option<Arc<TrustedDiscovery>>> {
    if config.discovery.is_none() {
        return Ok(None);
    }
    let cluster_name = config
        .cluster_name
        .clone()
        .context("validated discovery configuration is missing its cluster name")?;
    anyhow::ensure!(
        !config.discovery_endpoints.is_empty(),
        "validated discovery configuration has no etcd endpoints"
    );
    let options = discovery_connect_options(config, |name| std::env::var(name).ok())?;
    let directory = EtcdNodeDirectory::new_with_options(
        config.discovery_endpoints.clone(),
        cluster_name.clone(),
        options.clone(),
    )
    .await
    .context("connect trusted Console NodeDirectory")?;
    let config_provider = EtcdConfigProvider::new_with_options(
        config.discovery_endpoints.clone(),
        cluster_name,
        options,
    )
    .await
    .context("connect trusted Console ConfigProvider")?;
    Ok(Some(Arc::new(TrustedDiscovery::new(
        Arc::new(directory),
        Arc::new(config_provider),
    ))))
}

fn discovery_connect_options(
    config: &ConsoleConfig,
    env: impl Fn(&str) -> Option<String>,
) -> Result<ConnectOptions> {
    let security = &config.discovery_security;
    let transport = &config.discovery_transport;
    let mut options = ConnectOptions::new()
        .with_connect_timeout(transport.connect_timeout)
        .with_timeout(transport.request_timeout)
        .with_keep_alive(transport.keep_alive_interval, transport.keep_alive_timeout)
        .with_tcp_keepalive(transport.tcp_keepalive);
    if let Some(ca_path) = &security.tls_ca {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let ca = std::fs::read(ca_path)
            .with_context(|| format!("read etcd discovery CA {}", ca_path.display()))?;
        let mut tls = TlsOptions::new().ca_certificate(Certificate::from_pem(ca));
        if let Some(domain) = &security.tls_domain {
            tls = tls.domain_name(domain.clone());
        }
        if let (Some(cert_path), Some(key_path)) = (&security.tls_cert, &security.tls_key) {
            let cert = std::fs::read(cert_path).with_context(|| {
                format!("read etcd discovery certificate {}", cert_path.display())
            })?;
            let key = std::fs::read(key_path).with_context(|| {
                format!("read etcd discovery private key {}", key_path.display())
            })?;
            tls = tls.identity(Identity::from_pem(cert, key));
        }
        options = options.with_tls(tls);
    }
    if let (Some(username), Some(password_env)) = (&security.username, &security.password_env) {
        let password = env(password_env)
            .filter(|password| !password.is_empty())
            .with_context(|| {
                format!("read non-empty etcd discovery password from ${password_env}")
            })?;
        options = options.with_user(username.clone(), password);
    }
    Ok(options)
}

fn build_scheduler(
    config: &ConsoleConfig,
    cache: Arc<ConsoleCache>,
    upstream: Arc<dyn upstream::Upstream>,
    discovery: Option<Arc<TrustedDiscovery>>,
) -> Arc<Scheduler> {
    cache.register_peers(&config.peers);
    let scheduler = Scheduler::new(
        upstream,
        cache,
        config.peers.clone(),
        config.poll_interval,
        Duration::from_secs(4),
        16,
    );
    let scheduler = match discovery {
        Some(discovery) => scheduler.with_discovery(discovery),
        None => scheduler,
    };
    Arc::new(scheduler)
}

async fn shutdown_signal(mut shutdown: watch::Receiver<bool>) {
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::warn!(%error, "failed to install Console shutdown signal");
            }
        }
        result = shutdown.changed() => {
            let _ = result;
        }
    }
}

fn load_tls(config: &ConsoleConfig) -> Result<Option<TlsAcceptor>> {
    let (Some(cert_path), Some(key_path)) = (&config.tls_cert, &config.tls_key) else {
        return Ok(None);
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs = rustls_pemfile::certs(&mut BufReader::new(
        File::open(cert_path).with_context(|| format!("open {}", cert_path.display()))?,
    ))
    .collect::<std::result::Result<Vec<_>, _>>()
    .with_context(|| format!("parse certificates from {}", cert_path.display()))?;
    anyhow::ensure!(
        !certs.is_empty(),
        "{} contains no certificates",
        cert_path.display()
    );
    let key = rustls_pemfile::private_key(&mut BufReader::new(
        File::open(key_path).with_context(|| format!("open {}", key_path.display()))?,
    ))
    .with_context(|| format!("parse private key from {}", key_path.display()))?
    .with_context(|| format!("{} contains no private key", key_path.display()))?;
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("configure Console TLS certificate")?;
    Ok(Some(TlsAcceptor::from(Arc::new(server))))
}

const TLS_MAX_CONNECTIONS: usize = 256;
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const TLS_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const TLS_HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const TLS_HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

async fn accept_tls_with_deadline(
    acceptor: TlsAcceptor,
    stream: tokio::net::TcpStream,
    timeout: Duration,
) -> Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>> {
    match tokio::time::timeout(timeout, acceptor.accept(stream)).await {
        Ok(result) => result.map_err(anyhow::Error::from),
        Err(_) => Err(anyhow::anyhow!(
            "Console TLS handshake timed out after {timeout:?}"
        )),
    }
}

async fn serve_tls(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    app: axum::Router,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
    use tower::Service;

    let connection_permits = Arc::new(Semaphore::new(TLS_MAX_CONNECTIONS));
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => accepted.context("accept Console TLS connection")?,
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
        };
        let permit = match connection_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                tracing::warn!("Console TLS connection limit reached; dropping accepted socket");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        let mut connection_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let connection = async {
                let stream =
                    accept_tls_with_deadline(acceptor, stream, TLS_HANDSHAKE_TIMEOUT).await?;
                let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                    let mut app = app.clone();
                    async move {
                        let request = request.map(axum::body::Body::new);
                        let response = app.call(request).await?;
                        Ok::<_, std::convert::Infallible>(response)
                    }
                });
                let mut builder =
                    hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                builder
                    .http1()
                    .timer(TokioTimer::new())
                    .header_read_timeout(TLS_HEADER_READ_TIMEOUT);
                builder
                    .http2()
                    .timer(TokioTimer::new())
                    .keep_alive_interval(TLS_HTTP2_KEEP_ALIVE_INTERVAL)
                    .keep_alive_timeout(TLS_HTTP2_KEEP_ALIVE_TIMEOUT);
                builder
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                Ok::<(), anyhow::Error>(())
            };
            tokio::select! {
                result = connection => {
                    if let Err(error) = result {
                        tracing::debug!(%error, "Console TLS connection ended");
                    }
                }
                result = connection_shutdown.changed() => {
                    let _ = result;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::console::config::{DiscoverySecurityConfig, SeedPeer};
    use crate::console::discovery::tests::{MockConfig, MockDirectory};
    use rr_core::directory::NodeDirectoryPage;
    use rr_core::{NodeDirectory, RoutingTable};
    use std::path::PathBuf;

    fn dev_cert(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../dev-certs")
            .join(name)
    }

    fn startup_config(discovery: bool) -> ConsoleConfig {
        ConsoleConfig {
            listen: "127.0.0.1:8080".parse().unwrap(),
            peers: if discovery {
                vec![]
            } else {
                vec![SeedPeer {
                    node_id: 1,
                    admin_addr: "127.0.0.1:52001".to_string(),
                }]
            },
            discovery: discovery.then(|| "etcd://127.0.0.1:2379".to_string()),
            discovery_endpoints: if discovery {
                vec!["127.0.0.1:2379".to_string()]
            } else {
                vec![]
            },
            discovery_security: DiscoverySecurityConfig::default(),
            discovery_transport: crate::console::config::DiscoveryTransportConfig::default(),
            cluster_name: discovery.then(|| "test-cluster".to_string()),
            viewer_token: None,
            tls_cert: None,
            tls_key: None,
            poll_interval: Duration::from_secs(5),
            prometheus: None,
            static_dir: PathBuf::from("unused"),
        }
    }

    #[tokio::test]
    async fn tls_handshake_times_out_when_the_peer_stalls() {
        let mut config = startup_config(false);
        config.tls_cert = Some(dev_cert("node1.pem"));
        config.tls_key = Some(dev_cert("node1.key"));
        let acceptor = load_tls(&config).unwrap().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(address).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();

        let started = std::time::Instant::now();
        let result = accept_tls_with_deadline(acceptor, stream, Duration::from_millis(25)).await;

        assert!(
            result.is_err(),
            "a stalled TLS handshake must not remain pending"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(client);
    }

    #[tokio::test]
    async fn legacy_seed_startup_remains_explicit_and_does_not_connect_discovery() {
        let config = startup_config(false);
        let discovery = connect_discovery(&config).await.unwrap();
        assert!(discovery.is_none());
        let cache = ConsoleCache::production(None, config.poll_interval);
        let scheduler = build_scheduler(
            &config,
            cache,
            Arc::new(TonicUpstream::new(None, None)),
            discovery,
        );
        let view = scheduler.cache.view();
        assert_eq!(view.source_mode, "EXPLICIT_ADMIN_SEEDS");
        assert_eq!(view.nodes.len(), 1);
        assert_eq!(view.nodes[0].peer.admin_addr, "127.0.0.1:52001");
    }

    #[test]
    fn discovery_connect_options_apply_bounded_transport_timeouts() {
        let options = discovery_connect_options(&startup_config(true), |_| None).unwrap();
        let effective = format!("{options:?}");
        for expected in [
            "connect_timeout: Some(3s)",
            "timeout: Some(5s)",
            "keep_alive: Some((10s, 5s))",
            "tcp_keepalive: Some(30s)",
        ] {
            assert!(
                effective.contains(expected),
                "missing {expected}: {effective}"
            );
        }
    }

    #[tokio::test]
    async fn discovery_startup_surfaces_node_directory_connection_errors() {
        let mut config = startup_config(true);
        config.discovery_endpoints = vec!["://bad-endpoint".to_string()];
        let error = match connect_discovery(&config).await {
            Err(error) => error,
            Ok(_) => panic!("invalid etcd endpoint unexpectedly connected"),
        };
        assert_eq!(error.to_string(), "connect trusted Console NodeDirectory");
    }

    #[tokio::test]
    async fn secured_discovery_connects_both_clients_with_tls_and_auth() {
        use http_body_util::{BodyExt, StreamBody};
        use hyper::body::{Bytes, Frame};
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};

        let password_env = "RR_CONSOLE_TEST_ETCD_PASSWORD";
        let password = "etcd-secret-not-for-logs";
        std::env::set_var(password_env, password);
        let _ = rustls::crypto::ring::default_provider().install_default();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut config = startup_config(true);
        config.discovery_endpoints = vec![address.to_string()];
        config.discovery_security = DiscoverySecurityConfig {
            tls_ca: Some(dev_cert("ca.pem")),
            tls_cert: Some(dev_cert("node1.pem")),
            tls_key: Some(dev_cert("node1.key")),
            tls_domain: Some("localhost".to_string()),
            username: Some("console".to_string()),
            password_env: Some(password_env.to_string()),
        };
        let mut roots = rustls::RootCertStore::empty();
        for ca in
            rustls_pemfile::certs(&mut BufReader::new(File::open(dev_cert("ca.pem")).unwrap()))
        {
            roots.add(ca.unwrap()).unwrap();
        }
        let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let certs = rustls_pemfile::certs(&mut BufReader::new(
            File::open(dev_cert("node1.pem")).unwrap(),
        ))
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
        let key = rustls_pemfile::private_key(&mut BufReader::new(
            File::open(dev_cert("node1.key")).unwrap(),
        ))
        .unwrap()
        .unwrap();
        let mut server_config = ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .unwrap();
        server_config.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let (observed_tx, mut observed_rx) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let stream = acceptor.accept(stream).await.unwrap();
                let observed_tx = observed_tx.clone();
                let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                    let observed_tx = observed_tx.clone();
                    async move {
                        let (parts, body) = request.into_parts();
                        let body = body.collect().await.unwrap().to_bytes();
                        observed_tx
                            .send((parts.uri.path().to_string(), parts.headers, body))
                            .unwrap();
                        let response = if parts.uri.path() == "/etcdserverpb.Auth/Authenticate" {
                            let mut message = vec![0x12, 10];
                            message.extend_from_slice(b"test-token");
                            let mut frame = vec![0, 0, 0, 0, message.len() as u8];
                            frame.extend_from_slice(&message);
                            frame
                        } else {
                            vec![0, 0, 0, 0, 0]
                        };
                        let mut trailers = hyper::http::HeaderMap::new();
                        trailers.insert("grpc-status", "0".parse().unwrap());
                        let frames = futures::stream::iter([
                            Ok::<_, std::convert::Infallible>(Frame::data(Bytes::from(response))),
                            Ok(Frame::trailers(trailers)),
                        ]);
                        Ok::<_, std::convert::Infallible>(
                            hyper::Response::builder()
                                .header("content-type", "application/grpc")
                                .body(StreamBody::new(frames))
                                .unwrap(),
                        )
                    }
                });
                tokio::spawn(async move {
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        let result = tokio::time::timeout(Duration::from_secs(5), connect_discovery(&config)).await;
        std::env::remove_var(password_env);
        server.abort();
        let discovery = result
            .expect("secured discovery constructors timed out")
            .expect("secured discovery should connect both clients");
        assert!(discovery.is_some());

        let mut authenticate_requests = 0;
        let mut authenticated_status_requests = 0;
        while let Ok((path, headers, body)) = observed_rx.try_recv() {
            if path == "/etcdserverpb.Auth/Authenticate" {
                authenticate_requests += 1;
                assert!(body
                    .windows(password.len())
                    .any(|window| window == password.as_bytes()));
            }
            if path == "/etcdserverpb.Maintenance/Status"
                && headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    == Some("test-token")
            {
                authenticated_status_requests += 1;
            }
        }
        assert_eq!(authenticate_requests, 2);
        assert_eq!(authenticated_status_requests, 2);
    }

    #[tokio::test]
    async fn discovery_startup_attaches_the_trusted_directory_and_config_provider() {
        let config = startup_config(true);
        let directory: Arc<dyn NodeDirectory> =
            Arc::new(MockDirectory::new(vec![NodeDirectoryPage {
                records: vec![],
                next_cursor: None,
                revision: 1,
            }]));
        let discovery = Arc::new(TrustedDiscovery::new(
            directory,
            Arc::new(MockConfig::new(RoutingTable::default())),
        ));
        let cache = ConsoleCache::production(config.cluster_name.clone(), config.poll_interval);
        let scheduler = build_scheduler(
            &config,
            cache,
            Arc::new(TonicUpstream::new(None, None)),
            Some(discovery),
        );
        let view = scheduler.cache.view();
        assert_eq!(view.source_mode, "TRUSTED_DIRECTORY_AND_CONFIG");
        assert_eq!(view.cluster_id, "test-cluster");
        assert!(view.nodes.is_empty());
    }
}
