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
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::BasicNode;
use rr_core::error::{RaftError, RouterError, RrError};
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
    /// Interned `group_id` metric labels: one bounded entry per group id,
    /// so the per-request metrics path allocates nothing.
    group_labels: dashmap::DashMap<GroupId, &'static str>,
}

impl ClientDispatchImpl {
    /// Create a new `ClientDispatchImpl`.
    pub fn new(group_manager: Arc<RaftGroupManager>, shard_router: Arc<ShardRouter>) -> Self {
        Self {
            group_manager,
            shard_router,
            group_labels: dashmap::DashMap::new(),
        }
    }

    /// Intern the `group_id` label, leaking a few bytes once per distinct
    /// group id instead of allocating a `String` on every request.
    fn group_label(&self, group_id: GroupId) -> &'static str {
        if let Some(label) = self.group_labels.get(&group_id) {
            return *label;
        }
        let leaked: &'static str = Box::leak(group_id.to_string().into_boxed_str());
        *self.group_labels.entry(group_id).or_insert(leaked)
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
        let group_id: GroupId = self
            .shard_router
            .route(partition_id)
            .await
            .map_err(|e| route_error_to_status(partition_id, e))?;

        let (raft, _guard) = self
            .group_manager
            .get_raft_for_write(group_id)
            .await
            .ok_or_else(|| tonic::Status::not_found(format!("group {group_id} not found")))?;

        let keyed = idem_key.is_some();
        let group_label = self.group_label(group_id);
        let entry_bytes = if let Some(key) = idem_key {
            FramedEntry::from_payload(payload)
                .with_idem_key(key)
                .encode()
                .map_err(|e| {
                    tracing::error!(group_id, error = %e, "framed entry encode failed");
                    tonic::Status::internal("internal encode error")
                })?
        } else {
            // Log-poison guard: the apply path interprets any payload with a
            // house magic prefix as that envelope, and a malformed or
            // future-version frame is fatal to the whole Raft group at apply
            // time. Reject it deterministically here — identical on every
            // node — instead of committing it to the log.
            if let Err(e) = rr_core::ingress::validate_client_payload(&payload) {
                tracing::warn!(
                    partition_id,
                    group_id,
                    error = %e,
                    "rejecting malformed client payload before propose (detail sanitized for client)"
                );
                record_request_metrics(group_label, "write", start, "error");
                return Err(tonic::Status::invalid_argument(
                    "payload rejected by ingress validation",
                ));
            }
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
                            ns_limit_error: None,
                        }),
                        Ok(ApplyOutcome::IdempotencyExpired(err)) => Ok(ClientWriteResult {
                            success: false,
                            payload: vec![],
                            leader_node_id: None,
                            leader_addr: None,
                            idem_error: Some(err),
                            ns_limit_error: None,
                        }),
                        Ok(ApplyOutcome::IdempotencyNamespaceLimitExceeded(err)) => {
                            Ok(ClientWriteResult {
                                success: false,
                                payload: vec![],
                                leader_node_id: None,
                                leader_addr: None,
                                idem_error: None,
                                ns_limit_error: Some(err),
                            })
                        }
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
                        ns_limit_error: None,
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
                        ns_limit_error: None,
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
                "group_id" => group_label,
                "source" => "client"
            )
            .record(raft_started.elapsed().as_secs_f64());
        }
        let outcome = match &result {
            Ok(r) if r.idem_error.is_some() => "idempotency_expired",
            Ok(r) if r.ns_limit_error.is_some() => "idempotency_namespace_limit",
            Ok(r) if r.success => "success",
            Ok(_) => "redirect",
            Err(_) => "error",
        };
        record_request_metrics(group_label, "write", start, outcome);
        result
    }

    #[instrument(skip(self, payload), fields(payload_len = payload.len()))]
    async fn submit_read(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, tonic::Status> {
        let start = std::time::Instant::now();
        let group_id: GroupId = self
            .shard_router
            .route(partition_id)
            .await
            .map_err(|e| route_error_to_status(partition_id, e))?;
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
        record_request_metrics(self.group_label(group_id), "read", start, outcome);
        result
    }

    #[instrument(skip(self, payload), fields(payload_len = payload.len()))]
    async fn submit_linearizable_read(
        &self,
        partition_id: u64,
        payload: Vec<u8>,
    ) -> Result<LinearizableReadResult, tonic::Status> {
        let start = std::time::Instant::now();
        let group_id: GroupId = self
            .shard_router
            .route(partition_id)
            .await
            .map_err(|e| route_error_to_status(partition_id, e))?;
        let result = match self
            .group_manager
            .linearizable_read(group_id, payload)
            .await
        {
            Ok(bytes) => Ok(LinearizableReadResult::Served(bytes)),
            Err(RrError::Raft(RaftError::NotLeader {
                leader,
                leader_addr,
            })) => {
                // Not the leader: there is no read result, so there is
                // nothing to put in a payload — return the bare redirect
                // signal. When the follower is mid-election both fields are
                // `None` and the gRPC layer maps that to a retriable
                // Unavailable; fabricating an empty payload here would let
                // the client acknowledge bytes that never went through
                // consensus (soak-nightly run 38045591646 follow-up).
                Ok(LinearizableReadResult::NotLeader {
                    leader_node_id: leader,
                    leader_addr,
                })
            }
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
            Ok(LinearizableReadResult::NotLeader { .. }) => "redirect",
            Ok(LinearizableReadResult::Served(_)) => "success",
            Err(_) => "error",
        };
        record_request_metrics(self.group_label(group_id), "lin_read", start, outcome);
        result
    }

    #[instrument(skip(self))]
    async fn get_cluster_info(&self) -> Result<Vec<GroupInfo>, tonic::Status> {
        let mut group_ids = self.group_manager.group_ids().await;
        group_ids.sort_unstable();
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            // A group whose raft instance is gone (mid-teardown) is skipped
            // rather than failing the whole RPC.
            let Some(raft) = self.group_manager.get_raft(group_id).await else {
                continue;
            };
            let metrics = raft.metrics().borrow_watched().clone();
            // Topology comes from the raft runtime (`metrics`), never from
            // the routing table — ADR A-0013 removed `GroupMeta.leader` and
            // it must NOT be reintroduced. `leader_node_id = 0` means "no
            // known leader right now" (e.g. mid-election); 0 is not a valid
            // NodeId. Replicas are the full membership (voters + learners).
            let replica_node_ids = metrics
                .membership_config
                .membership()
                .nodes()
                .map(|(id, _)| *id)
                .collect();
            groups.push(GroupInfo {
                group_id,
                leader_node_id: metrics.current_leader.unwrap_or(0),
                replica_node_ids,
            });
        }
        Ok(groups)
    }
}

/// Map a routing failure to a client-facing gRPC status. Only a genuinely
/// unknown partition is `not_found`; config-provider and other internal
/// failures are sanitized to `unavailable` — the raw error text would leak
/// provider internals to clients (F2.5), and mislabelling a transient
/// provider outage as NotFound breaks client retry semantics.
fn route_error_to_status(partition_id: u64, err: RrError) -> tonic::Status {
    match err {
        RrError::Router(RouterError::PartitionNotFound { .. }) => {
            tonic::Status::not_found(format!("partition {partition_id} not found"))
        }
        e => {
            tracing::error!(
                partition_id,
                error = %e,
                "routing failed (detail sanitized for client)"
            );
            tonic::Status::unavailable("routing temporarily unavailable")
        }
    }
}

/// Record per-request Prometheus metrics at the client-protocol boundary:
/// `rr_server_requests_total{group_id, op, outcome}` (counter) and
/// `rr_server_request_duration_seconds{group_id, op}` (histogram).
/// `op` is one of `write|read|lin_read`; `outcome` is
/// `success|redirect|error|idempotency_expired|idempotency_namespace_limit`.
/// `group_label` is an interned
/// `&'static str` (see [`ClientDispatchImpl::group_label`]) so recording
/// allocates nothing per request.
fn record_request_metrics(
    group_label: &'static str,
    op: &'static str,
    start: std::time::Instant,
    outcome: &'static str,
) {
    metrics::counter!(
        "rr_server_requests_total",
        "group_id" => group_label,
        "op" => op,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        "rr_server_request_duration_seconds",
        "group_id" => group_label,
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
    use rr_core::types::{LogIndex, PartitionConfig};
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

    /// A config provider whose routing lookup always fails (simulates an
    /// etcd outage) — used to pin the sanitized `unavailable` mapping.
    struct FailingConfigProvider;

    #[async_trait]
    impl rr_core::config::ConfigProvider for FailingConfigProvider {
        async fn get_routing_table(
            &self,
        ) -> Result<rr_core::types::RoutingTable, rr_core::error::ConfigError> {
            Err(rr_core::error::ConfigError::ProviderUnavailable(
                "simulated provider outage with internal detail".to_string(),
            ))
        }
        async fn watch_changes(
            &self,
        ) -> Result<
            std::pin::Pin<
                Box<dyn futures::Stream<Item = rr_core::config::RoutingTableChange> + Send>,
            >,
            rr_core::error::ConfigError,
        > {
            Ok(Box::pin(futures::stream::empty()))
        }
        async fn get_node_addr(
            &self,
            _node_id: rr_core::types::NodeId,
        ) -> Result<rr_core::types::NodeAddr, rr_core::error::ConfigError> {
            Err(rr_core::error::ConfigError::ProviderUnavailable(
                "simulated provider outage".to_string(),
            ))
        }
    }

    // Routing failures must not leak provider internals nor mislabel a
    // provider outage as NotFound (breaks client retry semantics): only a
    // genuinely unknown partition is NotFound, everything else is a
    // sanitized Unavailable.
    #[tokio::test]
    async fn test_dispatch_provider_error_is_unavailable_and_sanitized() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        let router = Arc::new(ShardRouter::new(Arc::new(FailingConfigProvider)));
        let dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);

        let err = dispatch.submit_write(1, vec![1], None).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(
            !err.message().contains("simulated provider outage"),
            "provider internals must not leak to the client: {}",
            err.message()
        );

        let err = dispatch.submit_read(1, vec![1]).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(!err.message().contains("simulated provider outage"));

        let err = dispatch
            .submit_linearizable_read(1, vec![1])
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(!err.message().contains("simulated provider outage"));
    }

    // P0-1 regression: an unkeyed payload that collides with a house magic
    // but is malformed must be rejected with InvalidArgument before propose —
    // a committed poison frame is fatal to the whole group at apply time.
    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[tokio::test]
    async fn test_dispatch_rejects_malformed_house_magic_payload() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        mgr.start_group(1).await.unwrap();
        let config = Arc::new(MemConfigProvider::default());
        config
            .add_partition(7, PartitionConfig::SingleId(7), 1, vec![1])
            .await;
        let router = Arc::new(ShardRouter::new(config));
        let dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);

        // The group was never initialized, so reaching propose would yield a
        // redirect — an InvalidArgument here proves rejection before propose.
        let mut poison = Vec::from(rr_core::batch::BATCH_FRAME_MAGIC);
        poison.extend_from_slice(b"not-a-batch-frame");
        let err = dispatch.submit_write(7, poison, None).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let mut poison = Vec::from(rr_core::framed_entry::FRAMED_ENTRY_MAGIC);
        poison.extend_from_slice(b"not bincode");
        let err = dispatch.submit_write(7, poison, None).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // Keyed payloads are wrapped in a well-formed FramedEntry by dispatch
    // itself, so user bytes that merely start with a house magic are
    // legitimate application data and must NOT be ingress-rejected.
    // INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    #[tokio::test]
    async fn test_dispatch_keyed_payload_bypasses_ingress_validation() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        mgr.start_group(1).await.unwrap();
        let config = Arc::new(MemConfigProvider::default());
        config
            .add_partition(7, PartitionConfig::SingleId(7), 1, vec![1])
            .await;
        let router = Arc::new(ShardRouter::new(config));
        let dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);

        let key = rr_core::idempotency::IdempotencyKey {
            request_type: "t".to_string(),
            request_id: 1,
            client_id: Some("c".to_string()),
        };
        let mut payload = Vec::from(rr_core::batch::BATCH_FRAME_MAGIC);
        payload.extend_from_slice(b"user bytes that happen to carry the magic");
        let result = dispatch.submit_write(7, payload, Some(key)).await;
        match result {
            // No leader was initialized: a redirect proves the request was
            // framed and proposed rather than rejected at ingress.
            Ok(r) => assert!(!r.success),
            Err(s) => assert_ne!(s.code(), tonic::Code::InvalidArgument),
        }
    }

    // Regression (soak-nightly run 38045591646 follow-up): a group whose
    // raft instance is up but has no known leader (never initialized /
    // mid-election) must surface the bare NotLeader signal — dispatch must
    // NOT fabricate an empty-payload success, which the client would
    // acknowledge as bytes read without any consensus check.
    // INV-2: linearizable reads reflect all writes committed before the read was issued; followers redirect to the leader.
    #[tokio::test]
    async fn test_dispatch_linearizable_read_leader_unknown_is_not_served() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        mgr.start_group(1).await.unwrap();
        let config = Arc::new(MemConfigProvider::default());
        config
            .add_partition(7, PartitionConfig::SingleId(7), 1, vec![1])
            .await;
        let router = Arc::new(ShardRouter::new(config));
        let dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);

        // The group was never initialized, so no leader exists anywhere:
        // openraft answers ForwardToLeader::empty() → NotLeader{None, None}.
        let result = dispatch.submit_linearizable_read(7, vec![1]).await.unwrap();
        assert!(
            matches!(
                result,
                LinearizableReadResult::NotLeader {
                    leader_node_id: None,
                    leader_addr: None
                }
            ),
            "leader-unknown read must surface bare NotLeader, got: {result:?}"
        );
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

    #[tokio::test]
    async fn test_dispatch_get_cluster_info_reports_runtime_topology() {
        let mgr = Arc::new(RaftGroupManager::new(1, noop_factory()));
        mgr.start_group(1).await.unwrap();
        let mut members = std::collections::BTreeMap::new();
        members.insert(1u64, openraft::BasicNode::default());
        mgr.init_cluster(1, members).await.unwrap();

        // Wait for single-node self-election.
        let raft = mgr.get_raft(1).await.unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let metrics = raft.metrics().borrow_watched().clone();
            if metrics.current_leader == Some(1) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "single-node self-election timed out"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let config = Arc::new(MemConfigProvider::default());
        let router = Arc::new(ShardRouter::new(config));
        let dispatch = ClientDispatchImpl::new(Arc::clone(&mgr), router);

        let groups = dispatch.get_cluster_info().await.unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group_id, 1);
        assert_eq!(groups[0].leader_node_id, 1);
        assert_eq!(groups[0].replica_node_ids, vec![1]);
    }
}
