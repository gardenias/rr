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
//!   without TLS is equivalent to no auth — `RrNode` enforces this at
//!   startup unless `allow_insecure = true`.
//! - Every reject is emitted to the audit log
//!   ([`crate::audit::AUDIT_TARGET`]) with peer address + reason so
//!   operators can detect spray attacks and credential rotation needs.

use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

use crate::audit::{ServiceLabel, AUDIT_TARGET};

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
            service: ServiceLabel::Admin,
        }
    }

    /// Build an interceptor that validates ClientService requests.
    pub fn client_interceptor(&self) -> BearerInterceptor {
        BearerInterceptor {
            expected: self.client_token.clone(),
            service: ServiceLabel::Client,
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
///
/// On reject, emits a structured audit-log event at
/// [`crate::audit::AUDIT_TARGET`] with `op="auth"` plus peer / service /
/// reason so SOC tooling can alert on credential abuse.
#[derive(Debug, Clone)]
pub struct BearerInterceptor {
    expected: Option<String>,
    service: ServiceLabel,
}

impl BearerInterceptor {
    /// Create a new interceptor requiring the given token, tagged with
    /// the service it protects (used in audit events).
    pub fn new(service: ServiceLabel, expected_token: String) -> Self {
        Self {
            expected: Some(expected_token),
            service,
        }
    }

    /// Create a pass-through interceptor (no auth check), still tagged
    /// with a service label for any reject events emitted by other
    /// layers in the chain.
    pub fn disabled(service: ServiceLabel) -> Self {
        Self {
            expected: None,
            service,
        }
    }
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let Some(expected) = &self.expected else {
            return Ok(request);
        };

        let peer = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let presented_token = request
            .metadata()
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(strip_bearer_scheme);

        let outcome = (|| -> Result<(), &'static str> {
            let token = presented_token.ok_or("missing_credential")?;
            if !constant_time_eq(token.as_bytes(), expected.as_bytes()) {
                return Err("invalid_credential");
            }
            Ok(())
        })();

        match outcome {
            Ok(()) => Ok(request),
            Err(reason) => {
                let fingerprint = presented_token
                    .map(token_fingerprint)
                    .unwrap_or_else(|| "none".to_string());
                tracing::warn!(
                    target: AUDIT_TARGET,
                    op = "auth",
                    service = self.service.as_str(),
                    peer = %peer,
                    caller_token_fp = %fingerprint,
                    result = "rejected",
                    reason,
                    "auth rejected"
                );
                Err(Status::unauthenticated("authentication failed"))
            }
        }
    }
}

/// Strip the `Bearer` auth scheme from an `Authorization` header value,
/// returning the token verbatim.
///
/// The scheme is matched **case-insensitively** per RFC 7235 §2.1
/// ("`auth-scheme` is case-insensitive"), so `bearer x` and `BEARER x`
/// are accepted alongside `Bearer x`. The token itself is NOT
/// case-folded — credential comparison stays exact (and constant-time,
/// see [`constant_time_eq`]). An empty token yields `None` (missing
/// credential, not an invalid one).
pub(crate) fn strip_bearer_scheme(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty() {
        Some(token)
    } else {
        None
    }
}

/// Compute a short, **non-cryptographic** fingerprint of a presented
/// bearer token, suitable for audit-log correlation across reject
/// events from the same caller.
///
/// The output is the lower 16 hex characters of a `DefaultHasher`
/// (siphash-1-3) over the token bytes — collision-resistant enough
/// to discriminate attackers within a session without leaking the
/// token itself. NOT a security primitive: do NOT use this for
/// authentication, only for grouping log lines. Note that
/// `DefaultHasher`'s algorithm is not specified, so fingerprints may
/// change across Rust toolchain upgrades — correlate only within a
/// single deployment generation.
///
/// Empty input returns `"empty"` so log lines stay greppable.
pub fn token_fingerprint(token: &str) -> String {
    if token.is_empty() {
        return "empty".to_string();
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    token.as_bytes().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
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

/// Constant-time byte-slice equality, backed by the vetted [`subtle`]
/// crate.
///
/// Prevents token *content* leaks through timing side channels. Note that
/// (like every slice comparison) this returns early on length mismatch —
/// a length oracle is considered acceptable: tokens are fixed-length in
/// practice and length is not the secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
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
        let mut interceptor = BearerInterceptor::disabled(ServiceLabel::Admin);
        assert!(interceptor.call(req_with_auth(None)).is_ok());
    }

    #[test]
    fn enabled_interceptor_rejects_missing_auth() {
        let mut interceptor = BearerInterceptor::new(ServiceLabel::Admin, "secret".into());
        let err = interceptor.call(req_with_auth(None)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn enabled_interceptor_rejects_wrong_token() {
        let mut interceptor = BearerInterceptor::new(ServiceLabel::Admin, "secret".into());
        let err = interceptor
            .call(req_with_auth(Some("Bearer wrong")))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn enabled_interceptor_rejects_bad_scheme() {
        let mut interceptor = BearerInterceptor::new(ServiceLabel::Admin, "secret".into());
        let err = interceptor
            .call(req_with_auth(Some("Basic dXNlcjpwYXNz")))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn enabled_interceptor_accepts_correct_token() {
        let mut interceptor = BearerInterceptor::new(ServiceLabel::Admin, "secret".into());
        assert!(interceptor
            .call(req_with_auth(Some("Bearer secret")))
            .is_ok());
    }

    #[test]
    fn bearer_scheme_is_case_insensitive_per_rfc7235() {
        // RFC 7235 §2.1: auth-scheme is case-insensitive. Any casing of
        // "Bearer" must authenticate; the token comparison stays exact.
        let mut interceptor = BearerInterceptor::new(ServiceLabel::Admin, "secret".into());
        assert!(interceptor
            .call(req_with_auth(Some("bearer secret")))
            .is_ok());
        assert!(interceptor
            .call(req_with_auth(Some("BEARER secret")))
            .is_ok());
        assert!(interceptor
            .call(req_with_auth(Some("BeArEr secret")))
            .is_ok());
        // Token casing is NOT folded — a differently-cased token fails.
        let err = interceptor
            .call(req_with_auth(Some("bearer Secret")))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        // Wrong scheme still rejected regardless of casing.
        let err = interceptor
            .call(req_with_auth(Some("basic secret")))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        // "Bearer" with no token at all → missing credential reject.
        let err = interceptor.call(req_with_auth(Some("Bearer"))).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        let err = interceptor
            .call(req_with_auth(Some("Bearer ")))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn rejected_message_does_not_distinguish_failure_modes() {
        // Information-leak hardening: external clients see one generic
        // message regardless of which auth check tripped. Detail goes
        // only to the audit log (verified separately via tracing test).
        let mut interceptor = BearerInterceptor::new(ServiceLabel::Admin, "secret".into());
        let missing_msg = interceptor.call(req_with_auth(None)).unwrap_err();
        let scheme_msg = interceptor
            .call(req_with_auth(Some("Basic abc")))
            .unwrap_err();
        let invalid_msg = interceptor
            .call(req_with_auth(Some("Bearer wrong")))
            .unwrap_err();
        assert_eq!(missing_msg.message(), "authentication failed");
        assert_eq!(scheme_msg.message(), "authentication failed");
        assert_eq!(invalid_msg.message(), "authentication failed");
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
    fn token_fingerprint_is_stable_across_calls() {
        let a = token_fingerprint("alice-secret");
        let b = token_fingerprint("alice-secret");
        assert_eq!(a, b);
        assert_eq!(a.len(), 16);
    }

    #[test]
    fn token_fingerprint_distinguishes_distinct_tokens() {
        let a = token_fingerprint("alice-secret");
        let b = token_fingerprint("bob-secret");
        assert_ne!(a, b);
    }

    #[test]
    fn token_fingerprint_empty_returns_sentinel() {
        assert_eq!(token_fingerprint(""), "empty");
    }

    #[test]
    fn token_fingerprint_does_not_reveal_token_content() {
        let secret = "supersecret-1234567890";
        let fp = token_fingerprint(secret);
        assert!(
            !fp.contains("super") && !fp.contains("secret") && !fp.contains("123"),
            "fingerprint must not embed any prefix of the token"
        );
    }

    #[test]
    fn attach_bearer_token_sets_header() {
        let mut req = Request::new(());
        attach_bearer_token(&mut req, "hello").unwrap();
        assert_eq!(
            req.metadata()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer hello"
        );
    }
}
