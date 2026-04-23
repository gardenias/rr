//! Bearer-token authentication for rr gRPC services.
//!
//! # Model
//!
//! Bearer tokens are shared secrets carried in the `authorization` metadata
//! header of each gRPC request, formatted `Bearer <token>`. Two independent
//! tokens are supported:
//!
//! - [`AuthConfig::admin_token`] — if set, all AdminService RPCs require
//!   this token. Intended for the operator / SRE role.
//! - [`AuthConfig::client_token`] — if set, all ClientService RPCs require
//!   this token. Intended for application / end-user traffic. `None` means
//!   the ClientService is open (useful in dev; not recommended for prod).
//!
//! RaftService (inter-node) is authenticated via **mTLS** only — this module
//! does NOT install an interceptor on it, because peer identity is already
//! established by TLS.
//!
//! # Security notes
//!
//! - Tokens must be kept secret: load them from env vars or a secrets
//!   manager, never commit them to git.
//! - Token comparison is done in constant time via [`constant_time_eq`] to
//!   mitigate timing attacks.
//! - Tokens travel over the wire in plaintext inside the gRPC metadata;
//!   **TLS must be enabled** to prevent interception. Bearer-token auth
//!   without TLS is equivalent to no auth.

use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Bearer-token authentication policy for rr gRPC services.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Required token for AdminService calls. `None` disables admin auth
    /// (open — dev only).
    pub admin_token: Option<String>,
    /// Required token for ClientService calls. `None` disables client auth
    /// (open — dev only).
    pub client_token: Option<String>,
}

impl AuthConfig {
    /// Build an interceptor that validates AdminService requests.
    ///
    /// When `admin_token` is `None`, returns an interceptor that allows
    /// everything (dev mode). When set, the interceptor rejects any request
    /// missing a valid `Authorization: Bearer <token>` header.
    pub fn admin_interceptor(&self) -> BearerInterceptor {
        BearerInterceptor {
            expected: self.admin_token.clone(),
        }
    }

    /// Build an interceptor that validates ClientService requests.
    pub fn client_interceptor(&self) -> BearerInterceptor {
        BearerInterceptor {
            expected: self.client_token.clone(),
        }
    }

    /// Whether admin auth is enforced.
    pub fn admin_required(&self) -> bool {
        self.admin_token.is_some()
    }

    /// Whether client auth is enforced.
    pub fn client_required(&self) -> bool {
        self.client_token.is_some()
    }
}

/// A tonic interceptor that validates a bearer token in the `authorization`
/// metadata header. `expected = None` = pass-through.
#[derive(Debug, Clone)]
pub struct BearerInterceptor {
    expected: Option<String>,
}

impl BearerInterceptor {
    /// Create a new interceptor requiring the given token.
    pub fn new(expected_token: String) -> Self {
        Self {
            expected: Some(expected_token),
        }
    }

    /// Create a pass-through interceptor (no auth check).
    pub fn disabled() -> Self {
        Self { expected: None }
    }
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let Some(expected) = &self.expected else {
            return Ok(request);
        };

        let header = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;

        let header_str = header
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is not valid ASCII"))?;

        let token = header_str
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("authorization must be 'Bearer <token>'"))?;

        if !constant_time_eq(token.as_bytes(), expected.as_bytes()) {
            return Err(Status::unauthenticated("invalid token"));
        }

        Ok(request)
    }
}

/// Attach a bearer token to an outbound gRPC request's metadata.
///
/// Use on the client side (rr-admin, rr-client) to authenticate to a server
/// that requires bearer auth.
pub fn attach_bearer_token<T>(request: &mut Request<T>, token: &str) -> Result<(), Status> {
    let header: MetadataValue<_> = format!("Bearer {token}")
        .parse()
        .map_err(|_| Status::invalid_argument("token is not valid ASCII for a metadata header"))?;
    request.metadata_mut().insert("authorization", header);
    Ok(())
}

/// Constant-time byte-slice equality.
///
/// Prevents token length / prefix leaks through timing side channels.
/// See RFC 8628 §7.1 and OWASP ASVS V2.2.1.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req_with_auth(header: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(h) = header {
            req.metadata_mut()
                .insert("authorization", h.parse().unwrap());
        }
        req
    }

    #[test]
    fn disabled_interceptor_allows_missing_auth() {
        let mut interceptor = BearerInterceptor::disabled();
        assert!(interceptor.call(req_with_auth(None)).is_ok());
    }

    #[test]
    fn enabled_interceptor_rejects_missing_auth() {
        let mut interceptor = BearerInterceptor::new("secret".into());
        let err = interceptor.call(req_with_auth(None)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn enabled_interceptor_rejects_wrong_token() {
        let mut interceptor = BearerInterceptor::new("secret".into());
        let err = interceptor
            .call(req_with_auth(Some("Bearer wrong")))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn enabled_interceptor_rejects_bad_scheme() {
        let mut interceptor = BearerInterceptor::new("secret".into());
        let err = interceptor
            .call(req_with_auth(Some("Basic dXNlcjpwYXNz")))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn enabled_interceptor_accepts_correct_token() {
        let mut interceptor = BearerInterceptor::new("secret".into());
        assert!(interceptor.call(req_with_auth(Some("Bearer secret"))).is_ok());
    }

    #[test]
    fn constant_time_eq_distinguishes_different_lengths() {
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"x"));
    }

    #[test]
    fn constant_time_eq_matches_exact() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
    }

    #[test]
    fn auth_config_admin_required_when_token_set() {
        let cfg = AuthConfig {
            admin_token: Some("x".into()),
            client_token: None,
        };
        assert!(cfg.admin_required());
        assert!(!cfg.client_required());
    }

    #[test]
    fn attach_bearer_token_sets_header() {
        let mut req = Request::new(());
        attach_bearer_token(&mut req, "hello").unwrap();
        assert_eq!(
            req.metadata().get("authorization").unwrap().to_str().unwrap(),
            "Bearer hello"
        );
    }
}
