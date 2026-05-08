//! End-to-end tests proving TLS + mTLS + bearer auth compose
//! correctly across the AdminService gRPC boundary.
//!
//! What this exercises:
//! - Real `rcgen`-issued CA / server cert / client cert PEMs.
//! - A real `tonic::transport::Server` bound to `127.0.0.1:0` with
//!   `ServerTlsConfig::client_auth_optional(false)` (mTLS) and a
//!   chained `BearerInterceptor` carrying the admin token.
//! - A real `AdminServiceClient` connecting over a `Channel` whose
//!   `ClientTlsConfig` carries the CA + a client identity.
//!
//! Test matrix:
//! | scenario | TLS verify | bearer | expected |
//! |---|---|---|---|
//! | valid CA + valid cert + correct token | OK | OK | `Ok(NodeStatus)` |
//! | valid CA + valid cert + wrong token | OK | reject | `Unauthenticated` |
//! | valid CA + valid cert + no token | OK | reject | `Unauthenticated` |
//! | valid CA + NO client cert (server requires) | reject | n/a | TLS handshake error |
//!
//! All four assertions sit in one async test function so the
//! cert-issuance + server-spawn cost is paid once. Sub-test failures
//! report which scenario tripped via `assert!(…, "scenario: …")`.

use std::sync::Arc;
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, Ia5String, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use rr_network::audit::ServiceLabel;
use rr_network::auth::BearerInterceptor;
use rr_network::proto::admin::admin_service_client::AdminServiceClient;
use rr_network::proto::admin::admin_service_server::AdminServiceServer;
use rr_network::proto::admin::Empty;
use rr_network::tls::TlsConfig;
use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};
use rr_server::admin::AdminServiceImpl;
use tempfile::TempDir;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, Server};

const ADMIN_TOKEN: &str = "test-admin-token-correct-horse-battery-staple";

struct CertBundle {
    server_cert_pem: String,
    server_key_pem: String,
    ca_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn generate_certs() -> CertBundle {
    let mut ca_params = CertificateParams::new(vec!["rr-test-ca".to_string()]).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "rr-test-ca");
        dn
    };
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
    ];
    let ca_kp = KeyPair::generate().expect("ca key");
    let ca_cert = ca_params.self_signed(&ca_kp).expect("ca self-sign");

    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
    server_params.subject_alt_names = vec![
        SanType::DnsName(Ia5String::try_from("localhost".to_string()).unwrap()),
        SanType::IpAddress("127.0.0.1".parse().unwrap()),
    ];
    server_params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "rr-test-server");
        dn
    };
    let server_kp = KeyPair::generate().expect("server key");
    let server_cert = server_params
        .signed_by(&server_kp, &ca_cert, &ca_kp)
        .expect("server cert");

    let mut client_params = CertificateParams::new(Vec::new()).expect("client params");
    client_params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "rr-test-client");
        dn
    };
    let client_kp = KeyPair::generate().expect("client key");
    let client_cert = client_params
        .signed_by(&client_kp, &ca_cert, &ca_kp)
        .expect("client cert");

    CertBundle {
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_kp.serialize_pem(),
        ca_pem: ca_cert.pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_kp.serialize_pem(),
    }
}

fn write_pem_files(
    dir: &std::path::Path,
    certs: &CertBundle,
) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    use std::fs::write;
    let server_cert = dir.join("server.pem");
    let server_key = dir.join("server.key");
    let ca = dir.join("ca.pem");
    write(&server_cert, &certs.server_cert_pem).unwrap();
    write(&server_key, &certs.server_key_pem).unwrap();
    write(&ca, &certs.ca_pem).unwrap();
    (server_cert, server_key, ca)
}

fn noop_factory() -> StateMachineFactory {
    use async_trait::async_trait;
    use rr_core::error::RrError;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;

    struct NoopSM;
    #[async_trait]
    impl StateMachine for NoopSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }
        async fn restore(&mut self, _: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }
    Box::new(|_gid| Box::new(NoopSM))
}

async fn spawn_tls_admin_server(
    addr: std::net::SocketAddr,
    tls_dir: &std::path::Path,
) -> tokio::task::JoinHandle<()> {
    let (server_cert, server_key, ca) = {
        let mut p = tls_dir.to_path_buf();
        (
            {
                p.push("server.pem");
                p.clone()
            },
            {
                p.pop();
                p.push("server.key");
                p.clone()
            },
            {
                p.pop();
                p.push("ca.pem");
                p
            },
        )
    };
    let tls = TlsConfig {
        cert_path: server_cert,
        key_path: server_key,
        ca_path: Some(ca),
        require_client_cert: true,
        domain_name: None,
    };
    let server_tls = tls.build_server_tls().expect("server tls");

    let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
    let admin = AdminServiceImpl::new(1, addr.to_string(), mgr);

    let auth = BearerInterceptor::new(ServiceLabel::Admin, ADMIN_TOKEN.to_string());
    let svc = AdminServiceServer::with_interceptor(admin, auth);

    tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)
            .expect("server tls config apply")
            .add_service(svc)
            .serve(addr)
            .await
            .expect("admin server failed");
    })
}

fn build_client_channel_with_token(
    addr: &std::net::SocketAddr,
    certs: &CertBundle,
    include_client_cert: bool,
) -> Result<ClientTlsConfig, String> {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(certs.ca_pem.as_bytes()))
        .domain_name("localhost");
    if include_client_cert {
        tls = tls.identity(Identity::from_pem(
            certs.client_cert_pem.as_bytes(),
            certs.client_key_pem.as_bytes(),
        ));
    }
    let _ = addr;
    Ok(tls)
}

async fn connect(
    addr: &std::net::SocketAddr,
    tls: ClientTlsConfig,
) -> Result<Channel, tonic::transport::Error> {
    let endpoint = format!("https://{addr}");
    Channel::from_shared(endpoint)
        .unwrap()
        .tls_config(tls)?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5))
        .connect()
        .await
}

#[tokio::test]
async fn tls_mtls_bearer_auth_e2e_matrix() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let tmp = TempDir::new().unwrap();
    let certs = generate_certs();
    let (_server_cert_path, _server_key_path, _ca_path) = write_pem_files(tmp.path(), &certs);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let _server_handle = spawn_tls_admin_server(addr, tmp.path()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let valid_tls = build_client_channel_with_token(&addr, &certs, true).unwrap();
    let chan = connect(&addr, valid_tls.clone())
        .await
        .expect("scenario A: TLS handshake should succeed");
    let mut client = AdminServiceClient::with_interceptor(chan, |mut req: tonic::Request<()>| {
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {ADMIN_TOKEN}").parse().unwrap(),
        );
        Ok(req)
    });
    let resp = client.get_node_status(Empty {}).await;
    assert!(
        resp.is_ok(),
        "scenario A (valid TLS + valid token) should succeed; got {:?}",
        resp.err()
    );

    let chan = connect(&addr, valid_tls.clone())
        .await
        .expect("scenario B: TLS handshake should succeed");
    let mut client = AdminServiceClient::with_interceptor(chan, |mut req: tonic::Request<()>| {
        req.metadata_mut().insert(
            "authorization",
            "Bearer wrong-token-not-the-real-one".parse().unwrap(),
        );
        Ok(req)
    });
    let resp = client.get_node_status(Empty {}).await;
    let err = match resp {
        Ok(_) => panic!("scenario B (wrong token) must fail"),
        Err(e) => e,
    };
    assert_eq!(
        err.code(),
        tonic::Code::Unauthenticated,
        "scenario B should be Unauthenticated, got {err:?}",
    );

    let chan = connect(&addr, valid_tls.clone())
        .await
        .expect("scenario C: TLS handshake should succeed");
    let mut client = AdminServiceClient::new(chan);
    let resp = client.get_node_status(Empty {}).await;
    let err = match resp {
        Ok(_) => panic!("scenario C (no token) must fail"),
        Err(e) => e,
    };
    assert_eq!(
        err.code(),
        tonic::Code::Unauthenticated,
        "scenario C should be Unauthenticated, got {err:?}",
    );

    let no_cert_tls = build_client_channel_with_token(&addr, &certs, false).unwrap();
    let connect_result = connect(&addr, no_cert_tls).await;
    let scenario_d_failed = match connect_result {
        Err(_) => true,
        Ok(chan) => {
            let mut client =
                AdminServiceClient::with_interceptor(chan, |mut req: tonic::Request<()>| {
                    req.metadata_mut().insert(
                        "authorization",
                        format!("Bearer {ADMIN_TOKEN}").parse().unwrap(),
                    );
                    Ok(req)
                });
            client.get_node_status(Empty {}).await.is_err()
        }
    };
    assert!(
        scenario_d_failed,
        "scenario D: TLS without client cert (mTLS required) must fail at handshake or RPC time; \
         both succeeded which means mTLS is NOT being enforced"
    );
}
