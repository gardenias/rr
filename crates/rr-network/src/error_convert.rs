//! Conversions between rr domain errors and gRPC status codes.
//!
//! Because neither `RrError` nor `tonic::Status` are defined in this crate,
//! the orphan rule prevents `From` impls. Instead, we provide the [`IntoStatus`]
//! trait and implement it for all rr error types.

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
        }
    }
}

impl IntoStatus for RaftError {
    fn into_status(self) -> Status {
        match self {
            RaftError::NotLeader { leader } => {
                let msg = leader.map_or("no leader".to_string(), |id| format!("leader is {id}"));
                Status::unavailable(format!("not leader: {msg}"))
            }
            RaftError::GroupNotFound { group_id } => {
                Status::not_found(format!("raft group {group_id} not found"))
            }
            RaftError::ProposalFailed { reason } => Status::internal(reason),
            RaftError::Timeout => Status::deadline_exceeded("raft operation timed out"),
            RaftError::InitializationFailed { reason } => {
                Status::failed_precondition(format!("raft initialization failed: {reason}"))
            }
        }
    }
}

impl IntoStatus for StorageError {
    fn into_status(self) -> Status {
        match self {
            StorageError::NotFound { key } => Status::not_found(format!("key not found: {key}")),
            StorageError::Io(e) => Status::internal(e.to_string()),
            StorageError::Corruption(msg) => Status::data_loss(msg),
        }
    }
}

impl IntoStatus for ConfigError {
    fn into_status(self) -> Status {
        match self {
            ConfigError::ProviderUnavailable(msg) => Status::unavailable(msg),
            ConfigError::InvalidConfig(msg) => Status::invalid_argument(msg),
            ConfigError::WatchDisconnected => Status::unavailable("config watch disconnected"),
        }
    }
}

impl IntoStatus for RouterError {
    fn into_status(self) -> Status {
        match self {
            RouterError::PartitionNotFound { partition_id } => {
                Status::not_found(format!("partition {partition_id} not found"))
            }
            RouterError::GroupNotFound { group_id } => {
                Status::not_found(format!("group {group_id} not found"))
            }
            RouterError::NoLeader { group_id } => {
                Status::unavailable(format!("no leader for group {group_id}"))
            }
        }
    }
}

impl IntoStatus for NetworkError {
    fn into_status(self) -> Status {
        match self {
            NetworkError::ConnectionFailed { addr, reason } => {
                Status::unavailable(format!("connection to {addr} failed: {reason}"))
            }
            NetworkError::RpcFailed(msg) => Status::internal(msg),
            NetworkError::Timeout => Status::deadline_exceeded("network timeout"),
        }
    }
}

impl IntoStatus for CodecError {
    fn into_status(self) -> Status {
        match self {
            CodecError::SerializeFailed(msg) => {
                Status::internal(format!("serialize failed: {msg}"))
            }
            CodecError::DeserializeFailed(msg) => {
                Status::internal(format!("deserialize failed: {msg}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_leader_maps_to_unavailable() {
        let err = RrError::Raft(RaftError::NotLeader { leader: Some(3) });
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_not_leader_no_leader_maps_to_unavailable() {
        let err = RrError::Raft(RaftError::NotLeader { leader: None });
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
    fn test_router_no_leader_maps_to_unavailable() {
        let err = RrError::Router(RouterError::NoLeader { group_id: 5 });
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
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
}
