//! Audit log primitives shared across rr gRPC services.
//!
//! All authentication / authorization / rate-limit decisions are emitted
//! to the `tracing` target [`AUDIT_TARGET`], so operators can route audit
//! traffic to a forensic sink (Loki, file, syslog) separately from
//! operational logs via `EnvFilter`:
//!
//! ```text
//! RUST_LOG="info,rr::audit=info"
//! ```
//!
//! # Audit event schema (informal)
//!
//! Every audit event MUST include:
//! - `op`        — short operation label (e.g. `"auth"`, `"rate_limit"`,
//!   `"TransferLeader"`).
//! - `service`   — which gRPC service the event came from
//!   ([`ServiceLabel`]).
//! - `peer`      — remote address (`SocketAddr` as string) or
//!   `"unknown"` if not recoverable.
//! - `result`    — `"accepted"` or `"rejected"` for fast filtering.
//! - `reason`    — short human-readable reason on reject (free-form,
//!   already sanitized — must not include token material).
//!
//! Mutation operations (`TransferLeader`, `TriggerSnapshot`,
//! `node-reset`, etc.) emit a **before** event with `result =
//! "started"` and an **after** event with `result =
//! "succeeded"|"failed"` so an investigator can pair them.

/// Tracing target for audit events. Matches the conventional
/// `rr::audit` target documented in `docs/docs/deployment/audit-log.md`.
pub const AUDIT_TARGET: &str = "rr::audit";

/// Identifies which rr gRPC surface produced an audit event.
///
/// Used as the `service` field on every audit log line so operators
/// can filter by surface (e.g. `service="admin"` to scope alerting to
/// management traffic).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLabel {
    /// `AdminService` (management RPCs — TransferLeader, TriggerSnapshot, …).
    Admin,
    /// `ClientService` (application data-plane writes / reads).
    Client,
    /// `RaftService` (inter-node consensus RPCs — never bearer-auth'd,
    /// always mTLS in production).
    Raft,
}

impl ServiceLabel {
    /// Stable string form for use as a structured field value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Client => "client",
            Self::Raft => "raft",
        }
    }
}

impl std::fmt::Display for ServiceLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_label_string_form_is_stable() {
        assert_eq!(ServiceLabel::Admin.as_str(), "admin");
        assert_eq!(ServiceLabel::Client.as_str(), "client");
        assert_eq!(ServiceLabel::Raft.as_str(), "raft");
    }

    #[test]
    fn audit_target_is_documented_value() {
        // This constant is part of the public observability contract:
        // changing it would silently break ops dashboards. Pin it here.
        assert_eq!(AUDIT_TARGET, "rr::audit");
    }

    #[test]
    fn service_label_display_matches_as_str() {
        assert_eq!(format!("{}", ServiceLabel::Admin), "admin");
        assert_eq!(format!("{}", ServiceLabel::Client), "client");
        assert_eq!(format!("{}", ServiceLabel::Raft), "raft");
    }
}
