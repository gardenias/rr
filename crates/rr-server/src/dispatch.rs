//! Production implementation of [`ClientDispatch`] for routing client writes.
//!
//! Threads idempotency keys end-to-end: when the gRPC layer surfaces an
//! `IdempotencyKey`, the dispatch wraps the user payload as a
//! [`FramedEntry`] so the state-machine adapter's idempotency layer can
//! act on it; on the response side it decodes the
//! [`ApplyOutcome`] envelope and surfaces an Expired result via
//! [`ClientWriteResult::idem_error`]. Unkeyed requests pass through
//! verbatim for v0.2.x wire compatibility.

use std::sync::Arc;

use async_trait::async_trait;
use openraft::BasicNode;
use rr_core::error::{RaftError, RrError};
use rr_core::framed_entry::FramedEntry;
use rr_core::idempotency::{ApplyOutcome, IdempotencyKey};
use rr_core::types::GroupId;
use rr_network::client_service::{ClientDispatch, ClientWriteResult, LinearizableReadResult};
use rr_network::proto::client::GroupInfo;
use rr_raft::group_manager::RaftGroupManager;
use rr_router::router::ShardRouter;
use tracing::instrument;

/// Production implementation of [`ClientDispatch`].
///
/// Routes client writes to the correct Raft group via [`ShardRouter`],
/// then submits to openraft. Translates `ForwardToLeader` errors into
/// redirect responses.
pub struct ClientDispatchImpl {
    group_manager: Arc<RaftGroupManager>,
    shard_router: Arc<ShardRouter>,
}

impl ClientDispatchImpl {
    /// Create a new `ClientDispatchImpl`.
    pub fn new(group_manager: Arc<RaftGroupManager>, shard_router: Arc<ShardRouter>) -> Self {
        Self {
            group_manager,
            shard_router,
        }
    }
}

#[async_trait]
impl ClientDispatch for ClientDispatchImpl {
    #[instrument(skip(self, payload, idem_key), fields(payload_len = payload.len(), keyed = idem_key.is_some()))]
    async fn submit_write(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
        idem_key: Option<IdempotencyKey>,
    ) -> Result<ClientWriteResult, tonic::Status> {
        let start = std::time::Instant::now();
        let group_id: GroupId = self.shard_router.route(partition_id).await.map_err(|e| {
            tonic::Status::not_found(format!("partition {partition_id} not found: {e}"))
        })?;

        let (raft, _guard) = self
            .group_manager
            .get_raft_for_write(group_id)
            .await
            .ok_or_else(|| tonic::Status::not_found(format!("group {group_id} not found")))?;

        let keyed = idem_key.is_some();
        let entry_bytes = if let Some(key) = idem_key {
            FramedEntry::from_payload(payload)
                .with_idem_key(key)
                .encode()
                .map_err(|e| {
                    tracing::error!(group_id, error = %e, "framed entry encode failed");
                    tonic::Status::internal("internal encode error")
                })?
        } else {
            payload
        };

        let raft_started = std::time::Instant::now();
        let mut proposed = true;
        let result = match raft.client_write(entry_bytes.into()).await {
            Ok(resp) => {
                if keyed {
                    match ApplyOutcome::decode(resp.data.as_ref()) {
                        Ok(ApplyOutcome::Success(bytes)) => Ok(ClientWriteResult {
                            success: true,
                            payload: bytes,
                            leader_node_id: None,
                            leader_addr: None,
                            idem_error: None,
                        }),
                        Ok(ApplyOutcome::IdempotencyExpired(err)) => Ok(ClientWriteResult {
                            success: false,
                            payload: vec![],
                            leader_node_id: None,
                            leader_addr: None,
                            idem_error: Some(err),
                        }),
                        Err(e) => {
                            tracing::error!(
                                group_id,
                                error = %e,
                                "apply outcome decode failed (keyed request, expected wrapped response)"
                            );
                            Err(tonic::Status::internal("internal decode error"))
                        }
                    }
                } else {
                    Ok(ClientWriteResult {
                        success: true,
                        payload: resp.data.into(),
                        leader_node_id: None,
                        leader_addr: None,
                        idem_error: None,
                    })
                }
            }
            Err(ref e) => {
                if let Some(fwd) = e.forward_to_leader() {
                    proposed = false;
                    Ok(ClientWriteResult {
                        success: false,
                        payload: vec![],
                        leader_node_id: fwd.leader_id,
                        leader_addr: fwd.leader_node.as_ref().map(|n: &BasicNode| n.addr.clone()),
                        idem_error: None,
                    })
                } else {
                    tracing::error!(
                        partition_id,
                        group_id,
                        error = %e,
                        "client_write failed (detail sanitized for client)"
                    );
                    Err(tonic::Status::internal("client write failed"))
                }
            }
        };
        // Raft propose→commit+apply round trip for the client write path —
        // the quorum segment of the server-side latency budget. Redirects
        // never reached raft, so they are excluded from this histogram.
        if proposed {
            metrics::histogram!(
                "rr_proposal_duration_seconds",
                "group_id" => group_id.to_string(),
                "source" => "client"
            )
            .record(raft_started.elapsed().as_secs_f64());
        }
        let outcome = match &result {
            Ok(r) if r.idem_error.is_some() => "idempotency_expired",
            Ok(r) if r.success => "success",
            Ok(_) => "redirect",
            Err(_) => "error",
        };
        record_request_metrics(group_id, "write", start, outcome);
        result
    }

    #[instrument(skip(self, payload), fields(payload_len = payload.len()))]
    async fn submit_read(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status> {
        let start = std::time::Instant::now();
        let group_id: GroupId = self.shard_router.route(partition_id).await.map_err(|e| {
            tonic::Status::not_found(format!("partition {partition_id} not found: {e}"))
        })?;
        let result = self
            .group_manager
            .local_read(group_id, payload)
            .await
            .map_err(|e| {
                tracing::error!(
                    partition_id,
                    group_id,
                    error = %e,
                    "local_read failed (detail sanitized for client)"
                );
                tonic::Status::internal("local read failed")
            });
        let outcome = if result.is_ok() { "success" } else { "error" };
        record_request_metrics(group_id, "read", start, outcome);
        result
    }

    #[instrument(skip(self, payload), fields(payload_len = payload.len()))]
    async fn submit_linearizable_read(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
    ) -> Result<LinearizableReadResult, tonic::Status> {
        let start = std::time::Instant::now();
        let group_id: GroupId = self.shard_router.route(partition_id).await.map_err(|e| {
            tonic::Status::not_found(format!("partition {partition_id} not found: {e}"))
        })?;
        let result = match self
            .group_manager
            .linearizable_read(group_id, payload)
            .await
        {
            Ok(bytes) => Ok(LinearizableReadResult {
                payload: bytes,
                leader_node_id: None,
                leader_addr: None,
            }),
            Err(RrError::Raft(RaftError::NotLeader {
                leader,
                leader_addr,
            })) => Ok(LinearizableReadResult {
                payload: vec![],
                leader_node_id: leader,
                leader_addr,
            }),
            Err(e) => {
                tracing::error!(
                    partition_id,
                    group_id,
                    error = %e,
                    "linearizable_read failed (detail sanitized for client)"
                );
                Err(tonic::Status::internal("linearizable read failed"))
            }
        };
        let outcome = match &result {
            Ok(r) if r.leader_node_id.is_some() => "redirect",
            Ok(_) => "success",
            Err(_) => "error",
        };
        record_request_metrics(group_id, "lin_read", start, outcome);
        result
    }

    #[instrument(skip(self))]
    async fn get_cluster_info(&self) -> Result<Vec<GroupInfo>, tonic::Status> {
        let group_ids = self.group_manager.group_ids().await;
        let groups = group_ids
            .into_iter()
            .map(|group_id| GroupInfo {
                group_id,
                // Placeholder: `0` carries no information. If a real value is
                // ever provided here, it MUST be sourced from the raft runtime
                // (`metrics.current_leader`, as `admin.rs` does) — the
                // routing-table `GroupMeta.leader` field was removed by
                // ADR A-0013 and must NOT be reintroduced.
                leader_node_id: 0,
                replica_node_ids: vec![],
            })
            .collect();
        Ok(groups)
    }
}

/// Record per-request Prometheus metrics at the client-protocol boundary:
/// `rr_server_requests_total{group_id, op, outcome}` (counter) and
/// `rr_server_request_duration_seconds{group_id, op}` (histogram).
/// `op` is one of `write|read|lin_read`; `outcome` is
/// `success|redirect|error|idempotency_expired`.
fn record_request_metrics(
    group_id: GroupId,
    op: &'static str,
    start: std::time::Instant,
    outcome: &'static str,
) {
    let gid = group_id.to_string();
    metrics::counter!(
        "rr_server_requests_total",
        "group_id" => gid.clone(),
        "op" => op,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        "rr_server_request_duration_seconds",
        "group_id" => gid,
        "op" => op
    )
    .record(start.elapsed().as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use async_trait::async_trait;
    use rr_config::mem::MemConfigProvider;
    use rr_core::error::RrError;
    use rr_core::state_machine::StateMachine;
    use rr_core::types::LogIndex;
    use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};

    struct NoopSM;

    #[async_trait]
    impl StateMachine for NoopSM {
        async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
            Ok(entry.to_vec())
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }
        async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
        async fn last_applied(&self) -> Option<LogIndex> {
            None
        }
    }

    fn noop_factory() -> StateMachineFactory {
        Box::new(|_group_id| Box::new(NoopSM))
    }

    #[test]
    fn test_dispatch_new() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let config = Arc::new(MemConfigProvider::default());
        let router = Arc::new(ShardRouter::new(config));
        let _dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);
    }

    #[tokio::test]
    async fn test_dispatch_unknown_partition() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let config = Arc::new(MemConfigProvider::default());
        let router = Arc::new(ShardRouter::new(config));
        let dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);

        let result = dispatch.submit_write(999, vec![1, 2, 3], None).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_dispatch_get_cluster_info_empty() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let config = Arc::new(MemConfigProvider::default());
        let router = Arc::new(ShardRouter::new(config));
        let dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);

        let result = dispatch.get_cluster_info().await;
        assert!(result.is_ok());
        let groups = result.unwrap();
        assert!(groups.is_empty());
    }
}
