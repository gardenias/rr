//! Error types for the rr distributed framework.

use crate::types::{GroupId, NodeId, PartitionId};

/// Top-level error type of the rr framework, wrapping the domain-specific errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RrError {
    /// Raft consensus error.
    #[error("raft error: {0}")]
    Raft(#[from] RaftError),
    /// Storage engine error.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    /// Configuration provider error.
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    /// Routing error.
    #[error("router error: {0}")]
    Router(#[from] RouterError),
    /// Network transport error.
    #[error("network error: {0}")]
    Network(#[from] NetworkError),
    /// Serialization or deserialization error.
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    /// Error raised by application code (Service / StateMachine logic).
    #[error("application error: {0}")]
    Application(String),
    /// The request's idempotency record has expired and been evicted; retry
    /// with a fresh `request_id` greater than the returned watermark.
    #[error("idempotency expired: request_type={request_type}, request_id={request_id}, watermark={watermark}")]
    IdempotencyExpired {
        /// Request type of the rejected request.
        request_type: String,
        /// Request ID that fell below the dedup watermark.
        request_id: u64,
        /// Current dedup watermark; IDs at or below it are no longer tracked.
        watermark: u64,
    },
    /// A keyed request tried to open a NEW idempotency namespace
    /// (`request_type`) beyond the per-group cap
    /// (`rr_raft::idempotency::MAX_IDEMPO_NAMESPACES`). Fail-loud guard
    /// against unbounded watermark-map growth from client-controlled
    /// `request_type` values; already-tracked namespaces are unaffected.
    #[error("idempotency namespace limit exceeded: refusing new request_type '{request_type}' ({namespaces} namespaces tracked, max {max})")]
    IdempotencyNamespaceLimitExceeded {
        /// The rejected namespace.
        request_type: String,
        /// Namespaces currently tracked on this group.
        namespaces: usize,
        /// The configured cap.
        max: usize,
    },
}

/// Errors raised by the Raft consensus layer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RaftError {
    /// The node that received the request is not the group's leader.
    #[error("not leader, current leader: id={leader:?}, addr={leader_addr:?}")]
    NotLeader {
        /// Node ID of the current leader, if known.
        leader: Option<NodeId>,
        /// Address of the current leader, if known.
        leader_addr: Option<String>,
    },
    /// The requested Raft group does not exist.
    #[error("group not found: {group_id}")]
    GroupNotFound {
        /// ID of the missing group.
        group_id: GroupId,
    },
    /// A Raft proposal failed to commit.
    #[error("proposal failed: {reason}")]
    ProposalFailed {
        /// Why the proposal failed.
        reason: String,
    },
    /// A Raft operation timed out.
    #[error("raft timeout")]
    Timeout,
    /// Raft group initialization failed.
    #[error("raft initialization failed: {reason}")]
    InitializationFailed {
        /// Why initialization failed.
        reason: String,
    },
    /// Membership change rejected because the remaining voter set would drop
    /// below the required minimum; re-run with force to override.
    #[error(
        "membership change rejected: target voter set has {target_voters} voter(s), \
             below minimum {min}; re-run with force to override"
    )]
    UnsafeMembershipChange {
        /// Number of voters that would remain after the change.
        target_voters: usize,
        /// Minimum required voter count (see `MIN_VOTERS_AFTER_REMOVAL`).
        min: usize,
    },
    /// Membership change rejected because it would remove the final voter.
    #[error("cannot remove the final voter")]
    FinalVoterRemoval,
}

/// Errors raised by a storage engine backend.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// Underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Persisted data failed integrity or decode checks.
    #[error("data corruption: {0}")]
    Corruption(String),
    /// The requested key was not found.
    #[error("not found: {key}")]
    NotFound {
        /// The missing key.
        key: String,
    },
}

/// Errors raised by a configuration provider.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The configuration backend could not be reached.
    #[error("provider unavailable: {0}")]
    ProviderUnavailable(String),
    /// The provider returned malformed configuration data.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    /// The change-watch stream disconnected; re-fetch and re-subscribe.
    #[error("watch disconnected")]
    WatchDisconnected,
}

/// Errors raised while routing a request to a Raft group.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RouterError {
    /// No routing table entry exists for the partition.
    #[error("partition not found: {partition_id}")]
    PartitionNotFound {
        /// ID of the missing partition.
        partition_id: PartitionId,
    },
}

/// Errors raised by the network transport layer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NetworkError {
    /// Failed to establish a connection.
    #[error("connection failed to {addr}: {reason}")]
    ConnectionFailed {
        /// Address that could not be reached.
        addr: String,
        /// Why the connection failed.
        reason: String,
    },
    /// An established RPC call failed.
    #[error("rpc failed: {0}")]
    RpcFailed(String),
    /// An established RPC call was rejected with a deterministic gRPC
    /// status code — retrying can never converge (auth, validation,
    /// quota, …), so the code is preserved for the caller to classify.
    #[error("rpc rejected ({code}): {message}")]
    RpcRejected {
        /// The gRPC status code (e.g. `Unauthenticated`).
        code: String,
        /// Server-supplied (already sanitized) message.
        message: String,
    },
    /// A network operation timed out.
    #[error("network timeout")]
    Timeout,
    /// A configuration value is invalid (e.g. a client-side knob that would
    /// wedge or misbehave); raised before any connection is attempted.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

/// Errors raised while encoding or decoding payloads.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CodecError {
    /// A value could not be serialized.
    #[error("serialize failed: {0}")]
    SerializeFailed(String),
    /// Bytes could not be deserialized into the target type.
    #[error("deserialize failed: {0}")]
    DeserializeFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rr_error_display() {
        let e = RrError::Application("test error".to_string());
        assert_eq!(e.to_string(), "application error: test error");
    }

    #[test]
    fn test_storage_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let storage_err: StorageError = io_err.into();
        assert!(storage_err.to_string().contains("io error"));
    }

    #[test]
    fn test_rr_error_from_storage() {
        let storage_err = StorageError::NotFound {
            key: "mykey".to_string(),
        };
        let rr_err: RrError = storage_err.into();
        assert!(rr_err.to_string().contains("not found"));
    }

    #[test]
    fn test_raft_error_not_leader() {
        let e = RaftError::NotLeader {
            leader: Some(3),
            leader_addr: Some("127.0.0.1:9001".to_string()),
        };
        assert!(e.to_string().contains("not leader"));
        assert!(e.to_string().contains("127.0.0.1:9001"));
    }
}
