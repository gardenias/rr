//! TLS configuration for gRPC server and client channels.
//!
//! **Security notes**:
//! - Certificates and keys are loaded from PEM files at runtime.
//! - Private key material is kept only in-memory for the lifetime of the
//!   server / channel; it is never written to disk by this module.
//! - A missing `ca_path` on a server-side `TlsConfig` disables client
//!   certificate verification entirely; setting [`TlsConfig::require_client_cert`]
//!   without a CA is a configuration error and will be rejected.

use std::fs;
use std::path::{Path, PathBuf};

use rr_core::error::NetworkError;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// PEM file locations + policy for one TLS endpoint (either server or client).
///
/// Used by:
/// - `rr-server` to configure [`tonic::transport::Server::tls_config`]
/// - `rr-raft::network_factory::ChannelPool` to attach [`ClientTlsConfig`] to outbound channels
/// - `rr-client::RrClient` to attach TLS to its connection
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the PEM-encoded certificate presented by this endpoint.
    pub cert_path: PathBuf,
    /// Path to the PEM-encoded private key for `cert_path`.
    pub key_path: PathBuf,
    /// Optional path to the PEM-encoded CA bundle used to verify peers.
    /// - On the server: required to enable client-cert verification (mTLS).
    /// - On the client: required to verify the server's certificate when
    ///   `domain_name` is not a WebPKI-trusted CA.
    pub ca_path: Option<PathBuf>,
    /// If true, the server rejects connections without a valid client certificate
    /// (mTLS mode). Requires `ca_path` to be set. Ignored on client-side configs.
    pub require_client_cert: bool,
    /// The DNS name (SNI) expected in the server's certificate when this config
    /// is used on the client side. Ignored on server-side configs.
    pub domain_name: Option<String>,
}

impl TlsConfig {
    /// Build a `tonic::transport::ServerTlsConfig` for configuring a server.
    ///
    /// Returns an error if cert/key/CA files cannot be read, or if
    /// `require_client_cert = true` is set without a `ca_path`.
    pub fn build_server_tls(&self) -> Result<ServerTlsConfig, NetworkError> {
        let cert_pem = read_pem(&self.cert_path)?;
        let key_pem = read_pem(&self.key_path)?;
        let identity = Identity::from_pem(cert_pem, key_pem);

        let mut cfg = ServerTlsConfig::new().identity(identity);

        if let Some(ca) = &self.ca_path {
            let ca_pem = read_pem(ca)?;
            cfg = cfg.client_ca_root(Certificate::from_pem(ca_pem));
            if self.require_client_cert {
                cfg = cfg.client_auth_optional(false);
            } else {
                cfg = cfg.client_auth_optional(true);
            }
        } else if self.require_client_cert {
            return Err(NetworkError::ConnectionFailed {
                addr: String::new(),
                reason:
                    "TlsConfig.require_client_cert = true requires ca_path to verify client certs"
                        .to_string(),
            });
        }

        Ok(cfg)
    }

    /// Build a `tonic::transport::ClientTlsConfig` for configuring a client.
    ///
    /// Returns an error if CA / cert / key files cannot be read.
    pub fn build_client_tls(&self) -> Result<ClientTlsConfig, NetworkError> {
        let mut cfg = ClientTlsConfig::new();
        if let Some(ca) = &self.ca_path {
            let ca_pem = read_pem(ca)?;
            cfg = cfg.ca_certificate(Certificate::from_pem(ca_pem));
        }
        if let Some(name) = &self.domain_name {
            cfg = cfg.domain_name(name.clone());
        }

        let cert_pem = read_pem(&self.cert_path)?;
        let key_pem = read_pem(&self.key_path)?;
        cfg = cfg.identity(Identity::from_pem(cert_pem, key_pem));

        Ok(cfg)
    }
}

fn read_pem(path: &Path) -> Result<Vec<u8>, NetworkError> {
    fs::read(path).map_err(|e| NetworkError::ConnectionFailed {
        addr: String::new(),
        reason: format!("failed to read PEM at {}: {}", path.display(), e),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n";

    fn write_file(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn require_client_cert_without_ca_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let cert = write_file(tmp.path(), "cert.pem", TEST_CA_PEM);
        let key = write_file(tmp.path(), "key.pem", TEST_CA_PEM);
        let cfg = TlsConfig {
            cert_path: cert,
            key_path: key,
            ca_path: None,
            require_client_cert: true,
            domain_name: None,
        };
        let err = cfg.build_server_tls().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("require_client_cert") || msg.contains("ca_path"));
    }

    #[test]
    fn missing_cert_file_errors_clearly() {
        let cfg = TlsConfig {
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            ca_path: None,
            require_client_cert: false,
            domain_name: None,
        };
        let err = cfg.build_server_tls().unwrap_err();
        assert!(err.to_string().contains("failed to read PEM"));
    }

    #[test]
    fn client_tls_requires_cert_and_key_paths() {
        let tmp = TempDir::new().unwrap();
        let cfg = TlsConfig {
            cert_path: tmp.path().join("missing.pem"),
            key_path: tmp.path().join("missing.pem"),
            ca_path: None,
            require_client_cert: false,
            domain_name: Some("example.com".to_string()),
        };
        assert!(cfg.build_client_tls().is_err());
    }
}
