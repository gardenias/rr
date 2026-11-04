//! Conversions between rr domain errors and gRPC status codes.
//!
//! Because neither `RrError` nor `tonic::Status` are defined in this crate,
//! the orphan rule prevents `From` impls. Instead, we provide the [`IntoStatus`]
//! trait and implement it for all rr error types.
//!
//! # Information-leak hardening
//!
//! Most internal error detail (peer addresses, file paths from
//! `io::Error`, openraft / bincode internal strings, storage corruption
//! diagnostics) is **not** safe to return to an untrusted client — it
//! leaks topology, layout, and version metadata that helps attackers
//! reconnoiter the system. So this module follows a uniform rule:
//!
//! * **Client message**: a short, action-oriented sentence with the
//!   correct gRPC code. Never includes peer addresses, file paths,
//!   library-internal strings, or stack traces.
//! * **Server log**: the full detail goes to `tracing::warn!` / `error!`
//!   under the active span (so existing `#[instrument]` annotations on
//!   handlers carry caller / group / payload metadata for correlation).
//!
//! Exceptions where it IS safe to echo input back:
//!
//! * `StorageError::NotFound { key }` — the key was user-supplied.
//! * `RaftError::GroupNotFound { group_id }` — the group_id was
//!   user-supplied.
//! * `RouterError::*` — partition / group IDs are user-supplied.
//! * `RaftError::UnsafeMembershipChange` — the target voter count was
//!   user-supplied and the policy minimum is public (the voter list is
//!   already exposed via `GetGroupMembership`).
//! * `RrError::Application(msg)` — the user's own service produced it;
//!   it is the user's responsibility not to leak in their own message.
//! * `ConfigError::InvalidConfig(msg)` — messages are framework-authored
//!   validation strings that may echo user-supplied config values (the
//!   operator needs them verbatim, e.g. the G1–G7 startup gates). The
//!   authoring rule: never embed absolute file paths or secrets in these
//!   messages.

use rr_core::error::{
    CodecError, ConfigError, NetworkError, RaftError, RouterError, RrError, StorageError,
};
use tonic::Status;

/// Extension trait for converting rr error types into [`tonic::Status`].
pub trait IntoStatus {
    /// Convert this error into a gRPC status with an appropriate code.
    fn into_status(self) -> Status;
}

impl IntoStatus for RrError {
    fn into_status(self) -> Status {
        match self {
            RrError::Raft(e) => e.into_status(),
            RrError::Storage(e) => e.into_status(),
            RrError::Config(e) => e.into_status(),
            RrError::Router(e) => e.into_status(),
            RrError::Network(e) => e.into_status(),
            RrError::Codec(e) => e.into_status(),
            RrError::Application(msg) => Status::internal(msg),
            RrError::IdempotencyExpired {
                request_type,
                request_id,
                watermark,
            } => Status::failed_precondition(format!(
                "idempotency expired: request_type={request_type}, \
                 request_id={request_id}, watermark={watermark}"
            )),
            // `RrError` is `#[non_exhaustive]`; future variants map to a
            // sanitized internal error (no detail leak) until classified here.
            _ => Status::internal("internal error"),
        }
    }
}

impl IntoStatus for RaftError {
    fn into_status(self) -> Status {
        match self {
            RaftError::NotLeader {
                leader,
                leader_addr,
            } => {
                let id_msg = leader.map_or("no leader".to_string(), |id| format!("leader is {id}"));
                let addr_msg = leader_addr
                    .map(|a| format!(", addr={a}"))
                    .unwrap_or_default();
                Status::unavailable(format!("not leader: {id_msg}{addr_msg}"))
            }
            RaftError::GroupNotFound { group_id } => {
                Status::not_found(format!("raft group {group_id} not found"))
            }
            RaftError::ProposalFailed { reason } => {
                tracing::warn!(
                    detail = %reason,
                    "raft proposal failed (detail sanitized for client)"
                );
                Status::internal("raft proposal failed")
            }
            RaftError::Timeout => Status::deadline_exceeded("raft operation timed out"),
            RaftError::InitializationFailed { reason } => {
                tracing::error!(
                    detail = %reason,
                    "raft initialization failed (detail sanitized for client)"
                );
                Status::failed_precondition("raft initialization failed")
            }
            // Voter counts and the policy minimum are not sensitive:
            // `GetGroupMembership` already exposes the voter list. Reuse the
            // error's Display so the client message can never drift from it.
            e @ RaftError::UnsafeMembershipChange { .. } => {
                Status::failed_precondition(e.to_string())
            }
            e @ RaftError::FinalVoterRemoval => Status::invalid_argument(e.to_string()),
            _ => Status::internal("raft error"),
        }
    }
}

impl IntoStatus for StorageError {
    fn into_status(self) -> Status {
        match self {
            StorageError::NotFound { key } => Status::not_found(format!("key not found: {key}")),
            StorageError::Io(e) => {
                tracing::error!(
                    error = %e,
                    "storage I/O error (detail sanitized for client)"
                );
                Status::internal("storage I/O error")
            }
            StorageError::Corruption(msg) => {
                tracing::error!(
                    detail = %msg,
                    "storage corruption detected (detail sanitized for client)"
                );
                Status::data_loss("storage corruption")
            }
            _ => Status::internal("storage error"),
        }
    }
}

impl IntoStatus for ConfigError {
    fn into_status(self) -> Status {
        match self {
            ConfigError::ProviderUnavailable(msg) => {
                tracing::warn!(
                    detail = %msg,
                    "config provider unavailable (detail sanitized for client)"
                );
                Status::unavailable("config provider unavailable")
            }
            ConfigError::InvalidConfig(msg) => Status::invalid_argument(msg),
            ConfigError::WatchDisconnected => Status::unavailable("config watch disconnected"),
            _ => Status::internal("config error"),
        }
    }
}

impl IntoStatus for RouterError {
    fn into_status(self) -> Status {
        match self {
            RouterError::PartitionNotFound { partition_id } => {
                Status::not_found(format!("partition {partition_id} not found"))
            }
            // RouterError::GroupNotFound was removed in the 0.3.0 breaking
            // window: it had no constructor anywhere in the workspace.
            _ => Status::internal("router error"),
        }
    }
}

impl IntoStatus for NetworkError {
    fn into_status(self) -> Status {
        match self {
            NetworkError::ConnectionFailed { addr, reason } => {
                tracing::warn!(
                    peer_addr = %addr,
                    detail = %reason,
                    "network connection failed (detail sanitized for client)"
                );
                Status::unavailable("connection failed")
            }
            NetworkError::RpcFailed(msg) => {
                tracing::warn!(
                    detail = %msg,
                    "rpc failed (detail sanitized for client)"
                );
                Status::internal("rpc failed")
            }
            NetworkError::Timeout => Status::deadline_exceeded("network timeout"),
            _ => Status::internal("network error"),
        }
    }
}

impl IntoStatus for CodecError {
    fn into_status(self) -> Status {
        match self {
            CodecError::SerializeFailed(msg) => {
                tracing::error!(
                    detail = %msg,
                    "serialize failed (detail sanitized for client)"
                );
                Status::internal("serialize failed")
            }
            CodecError::DeserializeFailed(msg) => {
                tracing::error!(
                    detail = %msg,
                    "deserialize failed (detail sanitized for client)"
                );
                Status::internal("deserialize failed")
            }
            _ => Status::internal("codec error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_leader_maps_to_unavailable() {
        let err = RrError::Raft(RaftError::NotLeader {
            leader: Some(3),
            leader_addr: Some("127.0.0.1:9001".to_string()),
        });
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_not_leader_no_leader_maps_to_unavailable() {
        let err = RrError::Raft(RaftError::NotLeader {
            leader: None,
            leader_addr: None,
        });
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("no leader"));
    }

    #[test]
    fn test_storage_not_found_maps_to_not_found() {
        let err = RrError::Storage(StorageError::NotFound {
            key: "mykey".to_string(),
        });
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_network_timeout_maps_to_deadline_exceeded() {
        let err = RrError::Network(NetworkError::Timeout);
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    }

    #[test]
    fn test_raft_group_not_found_maps_to_not_found() {
        let err = RrError::Raft(RaftError::GroupNotFound { group_id: 99 });
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_raft_timeout_maps_to_deadline_exceeded() {
        let err = RrError::Raft(RaftError::Timeout);
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    }

    #[test]
    fn test_unsafe_membership_change_maps_to_failed_precondition() {
        let err = RrError::Raft(RaftError::UnsafeMembershipChange {
            target_voters: 1,
            min: 2,
        });
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let msg = status.message();
        assert!(
            msg.contains('1') && msg.contains('2'),
            "client message must name the target voter count and the minimum; got: {msg}"
        );
    }

    #[test]
    fn test_config_invalid_maps_to_invalid_argument() {
        let err = RrError::Config(ConfigError::InvalidConfig("bad".to_string()));
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_storage_corruption_maps_to_data_loss() {
        let err = RrError::Storage(StorageError::Corruption("corrupt".to_string()));
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn test_codec_error_maps_to_internal() {
        let err = RrError::Codec(CodecError::SerializeFailed("oops".to_string()));
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_application_error_maps_to_internal() {
        let err = RrError::Application("app error".to_string());
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("app error"));
    }

    #[test]
    fn test_network_connection_failed_maps_to_unavailable() {
        let err = RrError::Network(NetworkError::ConnectionFailed {
            addr: "127.0.0.1:5000".to_string(),
            reason: "refused".to_string(),
        });
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_internal_errors_dont_leak_peer_addrs() {
        let err = RrError::Network(NetworkError::ConnectionFailed {
            addr: "10.0.0.42:51001".to_string(),
            reason: "secret reason".to_string(),
        });
        let status = err.into_status();
        let msg = status.message();
        assert!(
            !msg.contains("10.0.0.42") && !msg.contains("secret reason"),
            "client-facing message must not echo peer addr or internal reason; got: {msg}"
        );
    }

    #[test]
    fn test_codec_errors_dont_leak_internal_detail() {
        let err = RrError::Codec(CodecError::DeserializeFailed(
            "bincode: invalid varint at offset 42".to_string(),
        ));
        let status = err.into_status();
        let msg = status.message();
        assert!(
            !msg.contains("bincode") && !msg.contains("offset"),
            "client-facing message must not echo bincode internals; got: {msg}"
        );
    }

    #[test]
    fn test_storage_io_errors_dont_leak_paths() {
        let err = RrError::Storage(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "/etc/rr/secrets.toml: permission denied",
        )));
        let status = err.into_status();
        let msg = status.message();
        assert!(
            !msg.contains("/etc/rr") && !msg.contains("secrets"),
            "client-facing message must not echo file paths; got: {msg}"
        );
    }
}
