//! High-level Service → StateMachine bridge adapter.
//!
//! [`ServiceStateMachineAdapter`] wraps a user's [`Service`]
//! implementation to fulfill the framework's internal
//! [`StateMachine`] trait.
//!
//! # Snapshot framing
//!
//! From v0.3.0 onward, the adapter wraps the user's
//! [`Service::snapshot`] bytes in a [`FramedSnapshot`] envelope so the
//! framework can persist its own state (idempotency layer, command
//! ingestor offsets) alongside the user's. On `restore()` the adapter
//! detects the envelope by its magic prefix; bytes lacking the prefix
//! are treated as legacy v0.2.x raw user snapshots and migrated
//! transparently. After the next `snapshot()` call the on-disk format
//! is the new framed envelope.
//!
//! `Service::handle` / `Service::snapshot` / `Service::restore` see
//! only the user's raw bytes — the framing is invisible to them.

use async_trait::async_trait;

use rr_core::error::RrError;
use rr_core::framed_snapshot::FramedSnapshot;
use rr_core::service::Service;
use rr_core::state_machine::StateMachine;
use rr_core::types::{GroupId, LogIndex};

/// Adapter that bridges a high-level [`Service`] to the low-level [`StateMachine`] trait.
///
/// The adapter delegates all operations directly to the wrapped service and
/// tracks how many entries have been applied via an internal counter.
///
/// The `group_id` is captured at construction so the framework can label
/// per-group metrics correctly (e.g. `rr_snapshot_installs_total{group_id="…"}`).
/// Construct via the state-machine factory closure passed to
/// [`crate::group_manager::RaftGroupManager`]:
///
/// ```ignore
/// Box::new(|group_id| Box::new(ServiceStateMachineAdapter::new(group_id, MyService::new())))
/// ```
pub struct ServiceStateMachineAdapter<S: Service> {
    group_id: GroupId,
    service: S,
    applied_count: u64,
}

impl<S: Service> ServiceStateMachineAdapter<S> {
    /// Create a new adapter wrapping the given service for `group_id`.
    pub fn new(group_id: GroupId, service: S) -> Self {
        Self {
            group_id,
            service,
            applied_count: 0,
        }
    }
}

#[async_trait]
impl<S: Service> StateMachine for ServiceStateMachineAdapter<S> {
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
        let result = self.service.handle(entry.to_vec()).await?;
        self.applied_count += 1;
        Ok(result)
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let user_snapshot = self.service.snapshot().await?;
        FramedSnapshot::new(user_snapshot).encode()
    }

    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError> {
        let user_bytes: Vec<u8> = match FramedSnapshot::decode(snapshot)? {
            Some(framed) => {
                tracing::debug!(
                    group_id = self.group_id,
                    version = framed.version,
                    has_idempotency = framed.framework_state.idempotency.is_some(),
                    has_ingestor = framed.framework_state.ingestor.is_some(),
                    user_bytes = framed.user_snapshot.len(),
                    "restoring from framed snapshot envelope"
                );
                framed.user_snapshot
            }
            None => {
                tracing::info!(
                    group_id = self.group_id,
                    snapshot_bytes = snapshot.len(),
                    "restoring from legacy (pre-framed) snapshot — auto-migrating; \
                     next snapshot() emits the new envelope"
                );
                snapshot.to_vec()
            }
        };
        self.service.restore(&user_bytes).await?;
        metrics::counter!(
            "rr_snapshot_installs_total",
            "group_id" => self.group_id.to_string()
        )
        .increment(1);
        Ok(())
    }

    async fn last_applied(&self) -> Option<LogIndex> {
        if self.applied_count == 0 {
            None
        } else {
            Some(self.applied_count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Mock service that records calls and returns configurable responses.
    struct MockService {
        calls: Arc<Mutex<Vec<Vec<u8>>>>,
        response: Vec<u8>,
        error: Option<String>,
        snapshot_data: Arc<Mutex<Vec<u8>>>,
    }

    impl MockService {
        fn new(response: Vec<u8>) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                response,
                error: None,
                snapshot_data: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_error(mut self, msg: &str) -> Self {
            self.error = Some(msg.to_string());
            self
        }

        fn with_snapshot(self, data: Vec<u8>) -> Self {
            // Block on setting isn't needed — we just replace the Arc
            Self {
                snapshot_data: Arc::new(Mutex::new(data)),
                ..self
            }
        }
    }

    #[async_trait]
    impl Service for MockService {
        async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
            if let Some(ref msg) = self.error {
                return Err(RrError::Application(msg.clone()));
            }
            self.calls.lock().await.push(request);
            Ok(self.response.clone())
        }

        fn name(&self) -> &str {
            "mock"
        }

        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(self.snapshot_data.lock().await.clone())
        }

        async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
            *self.snapshot_data.lock().await = data.to_vec();
            Ok(())
        }
    }

    #[tokio::test]
    async fn apply_passes_bytes_unchanged() {
        let svc = MockService::new(b"response-42".to_vec());
        let calls = Arc::clone(&svc.calls);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let input = b"request-data";
        let result = adapter.apply(input).await.unwrap();

        assert_eq!(result, b"response-42");
        let recorded = calls.lock().await;
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], input.to_vec());
    }

    #[tokio::test]
    async fn snapshot_emits_framed_envelope_carrying_user_bytes() {
        let svc = MockService::new(vec![]).with_snapshot(b"snap-state".to_vec());
        let adapter = ServiceStateMachineAdapter::new(7, svc);

        let encoded = adapter.snapshot().await.unwrap();
        assert!(
            encoded.starts_with(&rr_core::framed_snapshot::FRAMED_SNAPSHOT_MAGIC),
            "snapshot bytes should start with the framed-snapshot magic prefix; \
             got prefix {:?}",
            &encoded[..encoded.len().min(8)]
        );
        let framed = rr_core::framed_snapshot::FramedSnapshot::decode(&encoded)
            .expect("decode")
            .expect("framed envelope present");
        assert_eq!(framed.user_snapshot, b"snap-state");
        assert!(framed.framework_state.idempotency.is_none());
        assert!(framed.framework_state.ingestor.is_none());
    }

    #[tokio::test]
    async fn restore_unwraps_framed_envelope_to_service() {
        let svc = MockService::new(vec![]);
        let snapshot_data = Arc::clone(&svc.snapshot_data);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let envelope = rr_core::framed_snapshot::FramedSnapshot::new(b"framed-state".to_vec())
            .encode()
            .unwrap();
        adapter.restore(&envelope).await.unwrap();

        let data = snapshot_data.lock().await;
        assert_eq!(*data, b"framed-state");
    }

    #[tokio::test]
    async fn restore_legacy_unframed_bytes_passes_through_as_user_snapshot() {
        let svc = MockService::new(vec![]);
        let snapshot_data = Arc::clone(&svc.snapshot_data);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let legacy = b"raw v0.2.x snapshot bytes".to_vec();
        adapter.restore(&legacy).await.unwrap();

        let data = snapshot_data.lock().await;
        assert_eq!(*data, legacy);
    }

    #[tokio::test]
    async fn snapshot_then_restore_roundtrip_through_adapter() {
        let svc1 = MockService::new(vec![]).with_snapshot(b"persistent-state".to_vec());
        let adapter1 = ServiceStateMachineAdapter::new(7, svc1);
        let bytes = adapter1.snapshot().await.unwrap();

        let svc2 = MockService::new(vec![]);
        let svc2_data = Arc::clone(&svc2.snapshot_data);
        let mut adapter2 = ServiceStateMachineAdapter::new(7, svc2);
        adapter2.restore(&bytes).await.unwrap();

        assert_eq!(*svc2_data.lock().await, b"persistent-state");
    }

    #[tokio::test]
    async fn restore_malformed_framed_bytes_errors() {
        let svc = MockService::new(vec![]);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let mut bad = Vec::from(rr_core::framed_snapshot::FRAMED_SNAPSHOT_MAGIC);
        bad.extend_from_slice(b"not bincode");
        let err = adapter.restore(&bad).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("framed snapshot decode failed") || msg.contains("decode"),
            "expected decode error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn error_propagation_from_handle() {
        let svc = MockService::new(vec![]).with_error("service failed");
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let result = adapter.apply(b"anything").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("service failed"));
    }

    #[tokio::test]
    async fn last_applied_tracking() {
        let svc = MockService::new(b"ok".to_vec());
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        assert_eq!(adapter.last_applied().await, None);

        adapter.apply(b"a").await.unwrap();
        assert_eq!(adapter.last_applied().await, Some(1));

        adapter.apply(b"b").await.unwrap();
        assert_eq!(adapter.last_applied().await, Some(2));

        adapter.apply(b"c").await.unwrap();
        assert_eq!(adapter.last_applied().await, Some(3));
    }

    #[tokio::test]
    async fn failed_apply_does_not_increment_counter() {
        let svc = MockService::new(vec![]).with_error("boom");
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let _ = adapter.apply(b"data").await;
        assert_eq!(adapter.last_applied().await, None);
    }

    #[tokio::test]
    async fn restore_records_snapshot_install_metric() {
        let svc = MockService::new(vec![]);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        adapter.restore(b"snapshot-data").await.unwrap();
    }

    #[tokio::test]
    async fn group_id_is_preserved_for_metric_label() {
        let svc = MockService::new(vec![]);
        let adapter = ServiceStateMachineAdapter::new(42, svc);
        assert_eq!(adapter.group_id, 42);
    }
}
