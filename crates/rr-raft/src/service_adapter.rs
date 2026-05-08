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
//! # Per-entry framing + idempotency
//!
//! `apply` decodes each entry as a [`FramedEntry`]. If the envelope
//! carries an `IdempotencyKey`, the adapter consults its
//! [`IdempotencyLayer`] before invoking `Service::handle`:
//!
//! - **Replay** / **Collision** → returns the cached response, never
//!   calls the user service.
//! - **Expired** → returns an [`ApplyOutcome::IdempotencyExpired`]
//!   wrapping the watermark info; the dispatch layer surfaces it as
//!   `ClientResponse.idem_error`.
//! - **New** → calls the user service, records the response, and wraps
//!   it as [`ApplyOutcome::Success`] so the dispatch layer can decode
//!   it back to raw bytes.
//!
//! Entries without an `IdempotencyKey` (legacy v0.2.x payloads or
//! opt-out new clients) skip the layer entirely and return raw
//! response bytes — preserving wire compatibility.
//!
//! `Service::handle` / `Service::snapshot` / `Service::restore` see
//! only the user's raw bytes — the framing is invisible to them.

use async_trait::async_trait;
use std::sync::{Arc, RwLock};
use tokio::sync::RwLock as TokioRwLock;

use rr_core::error::RrError;
use rr_core::framed_entry::FramedEntry;
use rr_core::framed_snapshot::{FramedSnapshot, IngestorState};
use rr_core::idempotency::{ApplyOutcome, IdempotencyDecision};
use rr_core::service::Service;
use rr_core::state_machine::{IngestorOffsetControl, StateMachine, StateMachineReader};
use rr_core::types::{GroupId, LogIndex};

use crate::idempotency::{IdempotencyLayer, DEFAULT_IDEMPOTENCY_CACHE_SIZE};

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
    service: Arc<TokioRwLock<S>>,
    applied_count: u64,
    idempotency: IdempotencyLayer,
    /// Most recent ingestor offset observed via [`FramedEntry::offset`].
    /// Bytes are bincode-encoded `CommandIngestor::Offset` (opaque to
    /// the framework). Captured into [`IngestorState`] on `snapshot()`
    /// so a new leader can call `ingestor.seek()` after restore.
    /// `None` if no ingestor-sourced entry has been applied yet on
    /// this adapter instance.
    latest_ingestor_offset: Arc<RwLock<Option<Vec<u8>>>>,
    ingestor_offset_control: Arc<dyn IngestorOffsetControl>,
}

struct ServiceReadHandle<S: Service> {
    service: Arc<TokioRwLock<S>>,
}

#[async_trait]
impl<S: Service + 'static> StateMachineReader for ServiceReadHandle<S> {
    async fn read(&self, payload: Vec<u8>) -> Result<Vec<u8>, RrError> {
        self.service.read().await.handle(payload).await
    }
}

struct SharedIngestorOffsetControl {
    inner: Arc<RwLock<Option<Vec<u8>>>>,
}

impl SharedIngestorOffsetControl {
    fn new(inner: Arc<RwLock<Option<Vec<u8>>>>) -> Self {
        Self { inner }
    }
}

impl IngestorOffsetControl for SharedIngestorOffsetControl {
    fn set_offset(&self, offset: Option<Vec<u8>>) {
        *self.inner.write().expect("latest_ingestor_offset lock poisoned") = offset;
    }

    fn get_offset(&self) -> Option<Vec<u8>> {
        self.inner
            .read()
            .expect("latest_ingestor_offset lock poisoned")
            .clone()
    }
}

impl<S: Service> ServiceStateMachineAdapter<S> {
    /// Create a new adapter wrapping the given service for `group_id`,
    /// using the framework default idempotency cache capacity
    /// ([`DEFAULT_IDEMPOTENCY_CACHE_SIZE`]).
    pub fn new(group_id: GroupId, service: S) -> Self {
        Self::with_idempotency_capacity(group_id, service, DEFAULT_IDEMPOTENCY_CACHE_SIZE)
    }

    /// Create an adapter with an explicit idempotency cache capacity.
    /// Use this in tests or when memory constraints differ from the
    /// default 10 000 entries.
    pub fn with_idempotency_capacity(group_id: GroupId, service: S, capacity: usize) -> Self {
        let latest_ingestor_offset = Arc::new(RwLock::new(None));
        let ingestor_offset_control = Arc::new(SharedIngestorOffsetControl::new(
            latest_ingestor_offset.clone(),
        )) as Arc<dyn IngestorOffsetControl>;
        Self {
            group_id,
            service: Arc::new(TokioRwLock::new(service)),
            applied_count: 0,
            idempotency: IdempotencyLayer::new(capacity),
            latest_ingestor_offset,
            ingestor_offset_control,
        }
    }

    /// Latest ingestor offset (bincode-encoded `CommandIngestor::Offset`)
    /// observed via [`FramedEntry::offset`]. Used by the framework's
    /// ingestor lifecycle hook on leader promotion to seed
    /// `ingestor.seek()` without needing to decode the full snapshot.
    pub fn latest_ingestor_offset(&self) -> Option<Vec<u8>> {
        self.latest_ingestor_offset
            .read()
            .expect("latest_ingestor_offset lock poisoned")
            .clone()
    }

    /// Override the persisted ingestor offset bytes tracked by this adapter.
    /// `None` clears/unsets the current value.
    pub fn set_latest_ingestor_offset(&self, offset: Option<Vec<u8>>) {
        *self
            .latest_ingestor_offset
            .write()
            .expect("latest_ingestor_offset lock poisoned") = offset;
    }

    async fn apply_keyed(
        &mut self,
        key: rr_core::idempotency::IdempotencyKey,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RrError> {
        let group_id_label = self.group_id.to_string();
        match self.idempotency.decide(&key) {
            IdempotencyDecision::Replay(cached) => {
                metrics::counter!(
                    "rr_idempotency_replays_total",
                    "group_id" => group_id_label
                )
                .increment(1);
                ApplyOutcome::Success(cached).encode()
            }
            IdempotencyDecision::Expired {
                request_type,
                request_id,
                watermark,
            } => {
                metrics::counter!(
                    "rr_idempotency_expired_total",
                    "group_id" => group_id_label
                )
                .increment(1);
                ApplyOutcome::IdempotencyExpired(rr_core::idempotency::IdempotencyError {
                    request_type,
                    request_id,
                    watermark,
                })
                .encode()
            }
            IdempotencyDecision::Collision {
                cached_client_id,
                incoming_client_id,
                cached_response,
            } => {
                metrics::counter!(
                    "rr_idempotency_collisions_total",
                    "group_id" => group_id_label
                )
                .increment(1);
                tracing::warn!(
                    target: "rr::audit",
                    op = "idempotency_collision",
                    group_id = self.group_id,
                    request_type = %key.request_type,
                    request_id = key.request_id,
                    cached_client_id = ?cached_client_id,
                    incoming_client_id = ?incoming_client_id,
                    "idempotency key reused with different client_id"
                );
                ApplyOutcome::Success(cached_response).encode()
            }
            IdempotencyDecision::New => {
                let response = self.service.read().await.handle(payload).await?;
                self.idempotency.record(&key, response.clone());
                metrics::gauge!(
                    "rr_idempotency_cache_size",
                    "group_id" => group_id_label
                )
                .set(self.idempotency.cache_len() as f64);
                ApplyOutcome::Success(response).encode()
            }
        }
    }
}

#[async_trait]
impl<S: Service + 'static> StateMachine for ServiceStateMachineAdapter<S> {
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
        let (offset, idem_key, payload) = match FramedEntry::decode(entry)? {
            Some(framed) => (framed.offset, framed.idem_key, framed.payload),
            None => (None, None, entry.to_vec()),
        };

        let result = match idem_key {
            None => self.service.read().await.handle(payload).await,
            Some(key) => self.apply_keyed(key, payload).await,
        };

        if result.is_ok() {
            self.applied_count += 1;
            if let Some(off) = offset {
                self.set_latest_ingestor_offset(Some(off));
            }
        }
        result
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let user_snapshot = self.service.read().await.snapshot().await?;
        let mut framed = FramedSnapshot::new(user_snapshot);
        if !self.idempotency.is_empty() {
            framed.framework_state.idempotency = Some(self.idempotency.capture()?);
        }
        let latest_ingestor_offset = self.latest_ingestor_offset();
        if latest_ingestor_offset.is_some() {
            framed.framework_state.ingestor = Some(IngestorState {
                last_applied_offset: latest_ingestor_offset,
            });
        }
        framed.encode()
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
                if let Some(idem_state) = framed.framework_state.idempotency {
                    self.idempotency.restore(idem_state)?;
                }
                if let Some(ing_state) = framed.framework_state.ingestor {
                    self.set_latest_ingestor_offset(ing_state.last_applied_offset);
                }
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
        self.service.write().await.restore(&user_bytes).await?;
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

    fn ingestor_offset_control(&self) -> Option<Arc<dyn IngestorOffsetControl>> {
        Some(self.ingestor_offset_control.clone())
    }

    fn read_handle(&self) -> Option<Arc<dyn StateMachineReader>> {
        Some(Arc::new(ServiceReadHandle {
            service: Arc::clone(&self.service),
        }))
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

    fn keyed_entry(payload: &[u8], key: rr_core::idempotency::IdempotencyKey) -> Vec<u8> {
        rr_core::framed_entry::FramedEntry::from_payload(payload.to_vec())
            .with_idem_key(key)
            .encode()
            .unwrap()
    }

    fn unkeyed_entry(payload: &[u8]) -> Vec<u8> {
        rr_core::framed_entry::FramedEntry::from_payload(payload.to_vec())
            .encode()
            .unwrap()
    }

    #[tokio::test]
    async fn apply_unkeyed_framed_entry_returns_raw_user_bytes() {
        let svc = MockService::new(b"resp".to_vec());
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let entry = unkeyed_entry(b"req");
        let result = adapter.apply(&entry).await.unwrap();

        assert_eq!(
            result, b"resp",
            "unkeyed framed entries must return raw service bytes (not ApplyOutcome-wrapped)"
        );
    }

    #[tokio::test]
    async fn apply_keyed_first_call_records_and_wraps_success() {
        let svc = MockService::new(b"resp1".to_vec());
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let key = rr_core::idempotency::IdempotencyKey::new("transfer", 42);
        let entry = keyed_entry(b"req", key);
        let bytes = adapter.apply(&entry).await.unwrap();

        match rr_core::idempotency::ApplyOutcome::decode(&bytes).unwrap() {
            rr_core::idempotency::ApplyOutcome::Success(payload) => {
                assert_eq!(payload, b"resp1");
            }
            other => panic!("expected Success, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_keyed_replay_returns_cached_without_calling_service() {
        let svc = MockService::new(b"first-response".to_vec());
        let calls = Arc::clone(&svc.calls);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let key = rr_core::idempotency::IdempotencyKey::new("transfer", 42);
        let entry = keyed_entry(b"req", key);

        let first = adapter.apply(&entry).await.unwrap();
        let second = adapter.apply(&entry).await.unwrap();

        assert_eq!(
            first, second,
            "second apply with same key must return cached response"
        );
        assert_eq!(
            calls.lock().await.len(),
            1,
            "service.handle must run exactly once across two keyed retries"
        );
    }

    #[tokio::test]
    async fn apply_keyed_expired_returns_idempotency_expired_outcome() {
        let svc = MockService::new(b"resp".to_vec());
        let mut adapter = ServiceStateMachineAdapter::with_idempotency_capacity(7, svc, 2);

        let k1 = rr_core::idempotency::IdempotencyKey::new("transfer", 1);
        let k2 = rr_core::idempotency::IdempotencyKey::new("transfer", 2);
        let k3 = rr_core::idempotency::IdempotencyKey::new("transfer", 3);
        adapter.apply(&keyed_entry(b"a", k1.clone())).await.unwrap();
        adapter.apply(&keyed_entry(b"b", k2)).await.unwrap();
        adapter.apply(&keyed_entry(b"c", k3)).await.unwrap();

        let bytes = adapter.apply(&keyed_entry(b"a-retry", k1)).await.unwrap();
        match rr_core::idempotency::ApplyOutcome::decode(&bytes).unwrap() {
            rr_core::idempotency::ApplyOutcome::IdempotencyExpired(err) => {
                assert_eq!(err.request_type, "transfer");
                assert_eq!(err.request_id, 1);
                assert_eq!(err.watermark, 3);
            }
            other => panic!("expected IdempotencyExpired, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn snapshot_restore_preserves_idempotency_replay_behaviour() {
        let svc1 = MockService::new(b"first-response".to_vec());
        let mut adapter1 = ServiceStateMachineAdapter::new(7, svc1);
        let key = rr_core::idempotency::IdempotencyKey::new("transfer", 42);
        adapter1.apply(&keyed_entry(b"req", key.clone())).await.unwrap();
        let snapshot_bytes = adapter1.snapshot().await.unwrap();

        let svc2 = MockService::new(b"second-response".to_vec());
        let calls2 = Arc::clone(&svc2.calls);
        let mut adapter2 = ServiceStateMachineAdapter::new(7, svc2);
        adapter2.restore(&snapshot_bytes).await.unwrap();

        let bytes = adapter2.apply(&keyed_entry(b"req-retry", key)).await.unwrap();
        match rr_core::idempotency::ApplyOutcome::decode(&bytes).unwrap() {
            rr_core::idempotency::ApplyOutcome::Success(payload) => {
                assert_eq!(
                    payload, b"first-response",
                    "post-restore replay must return the ORIGINAL adapter's cached response"
                );
            }
            other => panic!("expected Success, got {other:?}"),
        }
        assert_eq!(
            calls2.lock().await.len(),
            0,
            "post-restore replay must not invoke the new service.handle"
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

    fn entry_with_offset(payload: &[u8], offset: Vec<u8>) -> Vec<u8> {
        rr_core::framed_entry::FramedEntry::from_payload(payload.to_vec())
            .with_offset(offset)
            .encode()
            .unwrap()
    }

    #[tokio::test]
    async fn apply_records_ingestor_offset_when_framed_entry_carries_one() {
        let svc = MockService::new(b"ok".to_vec());
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);
        assert!(adapter.latest_ingestor_offset().is_none());

        adapter
            .apply(&entry_with_offset(b"req", vec![0, 0, 0, 1]))
            .await
            .unwrap();
        assert_eq!(adapter.latest_ingestor_offset(), Some(vec![0u8, 0, 0, 1]));

        adapter
            .apply(&entry_with_offset(b"req", vec![0, 0, 0, 2]))
            .await
            .unwrap();
        assert_eq!(adapter.latest_ingestor_offset(), Some(vec![0u8, 0, 0, 2]));
    }

    #[tokio::test]
    async fn failed_apply_does_not_advance_ingestor_offset() {
        let svc = MockService::new(vec![]).with_error("boom");
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let _ = adapter
            .apply(&entry_with_offset(b"req", vec![0, 0, 0, 9]))
            .await;
        assert!(
            adapter.latest_ingestor_offset().is_none(),
            "failed apply must not commit the offset"
        );
    }

    #[tokio::test]
    async fn unkeyed_unframed_apply_leaves_offset_untouched() {
        let svc = MockService::new(b"ok".to_vec());
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        adapter
            .apply(&entry_with_offset(b"req", vec![0, 0, 0, 5]))
            .await
            .unwrap();
        adapter.apply(b"raw v0.2.x bytes").await.unwrap();

        assert_eq!(
            adapter.latest_ingestor_offset(),
            Some(vec![0u8, 0, 0, 5]),
            "raw legacy entries must not clobber the latest offset"
        );
    }

    #[tokio::test]
    async fn snapshot_emits_ingestor_state_when_offset_present() {
        let svc = MockService::new(b"ok".to_vec()).with_snapshot(b"user-snap".to_vec());
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);
        adapter
            .apply(&entry_with_offset(b"req", vec![0, 0, 0, 42]))
            .await
            .unwrap();

        let bytes = adapter.snapshot().await.unwrap();
        let framed = rr_core::framed_snapshot::FramedSnapshot::decode(&bytes)
            .unwrap()
            .unwrap();
        let ingestor = framed
            .framework_state
            .ingestor
            .expect("ingestor state should be populated after offset apply");
        assert_eq!(ingestor.last_applied_offset, Some(vec![0u8, 0, 0, 42]));
    }

    #[tokio::test]
    async fn snapshot_omits_ingestor_state_when_no_offset_seen() {
        let svc = MockService::new(b"ok".to_vec());
        let adapter = ServiceStateMachineAdapter::new(7, svc);
        let bytes = adapter.snapshot().await.unwrap();
        let framed = rr_core::framed_snapshot::FramedSnapshot::decode(&bytes)
            .unwrap()
            .unwrap();
        assert!(
            framed.framework_state.ingestor.is_none(),
            "ingestor state must be None when no offset has been seen"
        );
    }

    #[tokio::test]
    async fn restore_rehydrates_ingestor_offset_for_lifecycle_hook() {
        let svc1 = MockService::new(b"ok".to_vec()).with_snapshot(b"state".to_vec());
        let mut adapter1 = ServiceStateMachineAdapter::new(7, svc1);
        adapter1
            .apply(&entry_with_offset(b"req", vec![0, 0, 0, 99]))
            .await
            .unwrap();
        let snap_bytes = adapter1.snapshot().await.unwrap();

        let svc2 = MockService::new(vec![]);
        let mut adapter2 = ServiceStateMachineAdapter::new(7, svc2);
        adapter2.restore(&snap_bytes).await.unwrap();

        assert_eq!(
            adapter2.latest_ingestor_offset(),
            Some(vec![0u8, 0, 0, 99]),
            "restored adapter must surface the persisted offset so the runner can seek"
        );
    }
}
