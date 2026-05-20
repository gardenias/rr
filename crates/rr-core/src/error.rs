//! Error types for the rr distributed framework.

#[derive(Debug, thiserror::Error)]
pub enum RrError {
    #[error("raft error: {0}")]
    Raft(#[from] RaftError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("router error: {0}")]
    Router(#[from] RouterError),
    #[error("network error: {0}")]
    Network(#[from] NetworkError),
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("application error: {0}")]
    Application(String),
    #[error("idempotency expired: request_type={request_type}, request_id={request_id}, watermark={watermark}")]
    IdempotencyExpired {
        request_type: String,
        request_id: u64,
        watermark: u64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("not leader, current leader: id={leader:?}, addr={leader_addr:?}")]
    NotLeader {
        leader: Option<u64>,
        leader_addr: Option<String>,
    },
    #[error("group not found: {group_id}")]
    GroupNotFound { group_id: u64 },
    #[error("proposal failed: {reason}")]
    ProposalFailed { reason: String },
    #[error("raft timeout")]
    Timeout,
    #[error("raft initialization failed: {reason}")]
    InitializationFailed { reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("data corruption: {0}")]
    Corruption(String),
    #[error("not found: {key}")]
    NotFound { key: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("watch disconnected")]
    WatchDisconnected,
}

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("partition not found: {partition_id}")]
    PartitionNotFound { partition_id: u64 },
    #[error("group not found: {group_id}")]
    GroupNotFound { group_id: u64 },
    #[error("no leader for group: {group_id}")]
    NoLeader { group_id: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("connection failed to {addr}: {reason}")]
    ConnectionFailed { addr: String, reason: String },
    #[error("rpc failed: {0}")]
    RpcFailed(String),
    #[error("network timeout")]
    Timeout,
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("serialize failed: {0}")]
    SerializeFailed(String),
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
