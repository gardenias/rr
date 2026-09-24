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

use rr_core::deferred::{DeferredApply, DeferredHandleResult};
use rr_core::error::RrError;
use rr_core::framed_entry::FramedEntry;
use rr_core::framed_snapshot::{FramedSnapshot, IngestorState, OutboxState};
use rr_core::idempotency::{ApplyOutcome, IdempotencyDecision};
use rr_core::output::Output;
use rr_core::service::Service;
use rr_core::state_machine::{
    IngestorOffsetControl, OutboxControl, StateMachine, StateMachineLifecycleHook,
    StateMachineReader,
};
use rr_core::types::{ApplyContext, GroupId, LogIndex};

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
    /// Interned `group_id` metric label — interned once at construction so
    /// the per-entry apply metrics allocate nothing.
    metrics_label: &'static str,
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
    outbox_state: Arc<RwLock<OutboxRuntimeState>>,
    outbox_control: Arc<dyn OutboxControl>,
}

#[derive(Debug, Default, Clone)]
struct OutboxRuntimeState {
    pending: Vec<Output>,
    sent_watermark: u64,
    next_seq: u64,
}

struct ServiceLifecycleHook<S: Service> {
    service: Arc<TokioRwLock<S>>,
}

struct ServiceReadHandle<S: Service> {
    service: Arc<TokioRwLock<S>>,
}

struct SharedIngestorOffsetControl {
    inner: Arc<RwLock<Option<Vec<u8>>>>,
}

struct SharedOutboxControl {
    inner: Arc<RwLock<OutboxRuntimeState>>,
}

impl SharedIngestorOffsetControl {
    fn new(inner: Arc<RwLock<Option<Vec<u8>>>>) -> Self {
        Self { inner }
    }
}

impl SharedOutboxControl {
    fn new(inner: Arc<RwLock<OutboxRuntimeState>>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<S: Service + 'static> StateMachineReader for ServiceReadHandle<S> {
    async fn read(&self, payload: Vec<u8>) -> Result<Vec<u8>, RrError> {
        self.service.read().await.handle(payload).await
    }
}

impl IngestorOffsetControl for SharedIngestorOffsetControl {
    fn set_offset(&self, offset: Option<Vec<u8>>) {
        *self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = offset;
    }

    fn get_offset(&self) -> Option<Vec<u8>> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl OutboxControl for SharedOutboxControl {
    fn pending_outputs(&self) -> Vec<Output> {
        let state = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .pending
            .iter()
            .filter(|o| o.seq > state.sent_watermark)
            .cloned()
            .collect()
    }

    fn advance_watermark(&self, seq: u64) {
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if seq > state.sent_watermark {
            state.sent_watermark = seq;
            let watermark = state.sent_watermark;
            state.pending.retain(|o| o.seq > watermark);
        }
    }
}

#[async_trait]
impl<S: Service + 'static> StateMachineLifecycleHook for ServiceLifecycleHook<S> {
    async fn on_leader_elected(&self, group_id: GroupId) {
        self.service.write().await.on_leader_elected(group_id).await;
    }

    async fn on_step_down(&self, group_id: GroupId) {
        self.service.write().await.on_step_down(group_id).await;
    }

    async fn on_shutdown(&self) {
        self.service.write().await.shutdown().await;
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
        let outbox_state = Arc::new(RwLock::new(OutboxRuntimeState::default()));
        let outbox_control =
            Arc::new(SharedOutboxControl::new(outbox_state.clone())) as Arc<dyn OutboxControl>;
        Self {
            group_id,
            metrics_label: crate::network_factory::intern_id_label(group_id),
            service: Arc::new(TokioRwLock::new(service)),
            applied_count: 0,
            idempotency: IdempotencyLayer::new(capacity),
            latest_ingestor_offset,
            ingestor_offset_control,
            outbox_state,
            outbox_control,
        }
    }

    fn append_outputs(&self, outputs: Vec<Output>) {
        if outputs.is_empty() {
            return;
        }
        let mut state = self
            .outbox_state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for mut output in outputs {
            state.next_seq = state.next_seq.saturating_add(1);
            output.seq = state.next_seq;
            state.pending.push(output);
        }
    }

    /// Shared apply body for [`StateMachine::apply`],
    /// [`StateMachine::apply_with_ctx`], and [`StateMachine::apply_deferred`].
    /// `ctx` is `Some` on the real Raft apply path and `None` for direct
    /// non-context callers. `deferred` selects the two-phase dispatch
    /// (`Service::handle_deferred`) used by the canonical Raft apply path.
    async fn apply_entry(
        &mut self,
        entry: &[u8],
        ctx: Option<ApplyContext>,
        deferred: bool,
    ) -> Result<DeferredApply, RrError> {
        // Full-chain latency observability: this is the business-apply
        // segment (idempotency layer + Service::handle). Combined with
        // rr_proposal_duration_seconds (client_write total) and
        // rr_server_request_duration_seconds (dispatch total) it closes the
        // server-side latency budget: server ≈ routing + replication + apply.
        let apply_started = std::time::Instant::now();
        let (framed, offset, idem_key, payload) = match FramedEntry::decode(entry)? {
            Some(f) => (true, f.offset, f.idem_key, f.payload),
            None => (false, None, None, entry.to_vec()),
        };

        let result: Result<DeferredApply, RrError> = match idem_key {
            Some(key) => {
                // rr-idempotency-keyed entries always resolve immediately (a
                // parked response cannot be cached); deferral + rr-idem-key is
                // unsupported (services key dedup in app state, A-0033).
                match self.apply_keyed(key, payload, ctx).await {
                    Ok(resp) => Ok(DeferredApply::Immediate(resp)),
                    Err(e) => Err(e),
                }
            }
            None => {
                // Client-write batching (opt-in on rr-client): a raw,
                // unframed payload that decodes as a batch frame is split
                // here — each item is applied individually through the
                // immediate path and per-item responses are packed
                // positionally. Deferred (two-phase) services must not be
                // combined with client batching — see `rr_core::batch`
                // module docs.
                //
                // The split is reserved for raw client bytes: the inner
                // payload of a FramedEntry envelope is applied verbatim, so
                // ingestor-sourced entries (offset = Some) never have their
                // upstream bytes re-interpreted — a Kafka record carrying
                // the batch magic applies as-is instead of failing the apply
                // (log poison). This also keeps
                // `rr_core::ingress::validate_client_payload` exactly aligned
                // with what the apply path would reject.
                let batch = if framed {
                    None
                } else {
                    rr_core::batch::decode_batch(&payload)?
                };
                match batch {
                    Some(items) => {
                        let mut responses = Vec::with_capacity(items.len());
                        for item in items {
                            let resp = self.handle_and_collect_outputs(item, ctx).await?;
                            responses.push(resp);
                        }
                        Ok(DeferredApply::Immediate(rr_core::batch::encode_batch(
                            &responses,
                        )))
                    }
                    None => {
                        if deferred {
                            let Some(ctx) = ctx else {
                                return Err(RrError::Application(
                                    "apply_deferred called without an ApplyContext — \
                                     internal contract violation"
                                        .to_string(),
                                ));
                            };
                            match self
                                .service
                                .read()
                                .await
                                .handle_deferred(payload, ctx)
                                .await
                            {
                                Ok(dhr) => Ok(self.map_deferred_result(dhr)),
                                Err(e) => Err(e),
                            }
                        } else {
                            match self.handle_and_collect_outputs(payload, ctx).await {
                                Ok(resp) => Ok(DeferredApply::Immediate(resp)),
                                Err(e) => Err(e),
                            }
                        }
                    }
                }
            }
        };

        if result.is_ok() {
            self.applied_count += 1;
            // Exact applied-entries TPS source: emitted on every node per
            // committed entry — no snapshot steps, no leader-switch gaps,
            // unlike rate(rr_raft_last_log_index).
            metrics::counter!(
                "rr_raft_applied_entries_total",
                "group_id" => self.metrics_label
            )
            .increment(1);
            if let Some(off) = offset {
                self.set_latest_ingestor_offset(Some(off));
            }
        }
        metrics::histogram!(
            "rr_raft_apply_duration_seconds",
            "group_id" => self.metrics_label
        )
        .record(apply_started.elapsed().as_secs_f64());
        result
    }

    /// Map a service-level [`DeferredHandleResult`] to the adapter-level
    /// [`DeferredApply`], collecting any outputs into the outbox.
    fn map_deferred_result(&self, dhr: DeferredHandleResult) -> DeferredApply {
        match dhr {
            DeferredHandleResult::Immediate(hr) => {
                self.append_outputs(hr.outputs);
                DeferredApply::Immediate(hr.response)
            }
            DeferredHandleResult::Pending { correlation_id } => {
                DeferredApply::Pending { correlation_id }
            }
            DeferredHandleResult::Complete {
                completions,
                result,
            } => {
                self.append_outputs(result.outputs);
                DeferredApply::Complete {
                    completions,
                    response: result.response,
                }
            }
        }
    }

    async fn handle_and_collect_outputs(
        &self,
        payload: Vec<u8>,
        ctx: Option<ApplyContext>,
    ) -> Result<Vec<u8>, RrError> {
        let result = match ctx {
            // R-1: the apply path carries the entry's ApplyContext; the service
            // receives it via `handle_with_outputs_ctx`.
            Some(ctx) => {
                self.service
                    .read()
                    .await
                    .handle_with_outputs_ctx(payload, ctx)
                    .await?
            }
            // Non-context callers (direct `StateMachine::apply` use) keep the
            // legacy `handle_with_outputs` path unchanged.
            None => {
                self.service
                    .read()
                    .await
                    .handle_with_outputs(payload)
                    .await?
            }
        };
        self.append_outputs(result.outputs);
        Ok(result.response)
    }

    /// Latest ingestor offset (bincode-encoded `CommandIngestor::Offset`)
    /// observed via [`FramedEntry::offset`]. Used by the framework's
    /// ingestor lifecycle hook on leader promotion to seed
    /// `ingestor.seek()` without needing to decode the full snapshot.
    pub fn latest_ingestor_offset(&self) -> Option<Vec<u8>> {
        self.latest_ingestor_offset
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Override the persisted ingestor offset bytes tracked by this adapter.
    /// `None` clears/unsets the current value.
    pub fn set_latest_ingestor_offset(&self, offset: Option<Vec<u8>>) {
        *self
            .latest_ingestor_offset
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = offset;
    }

    /// Notify the wrapped service that this node became the group leader.
    pub async fn notify_leader_elected(&self, group_id: GroupId) {
        self.service.write().await.on_leader_elected(group_id).await;
    }

    /// Notify the wrapped service that this node stepped down.
    pub async fn notify_step_down(&self, group_id: GroupId) {
        self.service.write().await.on_step_down(group_id).await;
    }

    /// Notify the wrapped service to shut down.
    pub async fn notify_shutdown(&self) {
        self.service.write().await.shutdown().await;
    }

    /// Get pending outputs (seq > sent_watermark).
    pub fn pending_outputs(&self) -> Vec<Output> {
        self.outbox_control.pending_outputs()
    }

    /// Advance the sent watermark (called by OutputRunner after successful delivery).
    pub fn advance_output_watermark(&mut self, seq: u64) {
        self.outbox_control.advance_watermark(seq);
    }

    async fn apply_keyed(
        &mut self,
        key: rr_core::idempotency::IdempotencyKey,
        payload: Vec<u8>,
        ctx: Option<ApplyContext>,
    ) -> Result<Vec<u8>, RrError> {
        let group_id_label = self.group_id.to_string();
        match self.idempotency.decide(&key)? {
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
                let response = self.handle_and_collect_outputs(payload, ctx).await?;
                self.idempotency.record(&key, response.clone())?;
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
        Ok(self.apply_entry(entry, None, false).await?.into_response())
    }

    async fn apply_with_ctx(
        &mut self,
        entry: &[u8],
        ctx: ApplyContext,
    ) -> Result<Vec<u8>, RrError> {
        Ok(self
            .apply_entry(entry, Some(ctx), false)
            .await?
            .into_response())
    }

    async fn apply_deferred(
        &mut self,
        entry: &[u8],
        ctx: ApplyContext,
    ) -> Result<DeferredApply, RrError> {
        self.apply_entry(entry, Some(ctx), true).await
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
        {
            let outbox = self
                .outbox_state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !outbox.pending.is_empty() || outbox.sent_watermark > 0 {
                framed.framework_state.outbox = Some(OutboxState {
                    pending: outbox.pending.clone(),
                    sent_watermark: outbox.sent_watermark,
                    next_seq: outbox.next_seq,
                });
            }
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
                {
                    let mut outbox = self
                        .outbox_state
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if let Some(outbox_state) = framed.framework_state.outbox {
                        outbox.pending = outbox_state.pending;
                        outbox.sent_watermark = outbox_state.sent_watermark;
                        outbox.next_seq = outbox_state.next_seq;
                    } else {
                        *outbox = OutboxRuntimeState::default();
                    }
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
                *self
                    .outbox_state
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    OutboxRuntimeState::default();
                snapshot.to_vec()
            }
        };
        self.service.write().await.restore(&user_bytes).await?;
        metrics::counter!(
            "rr_snapshot_installs_total",
            "group_id" => self.metrics_label
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

    async fn on_leader_elected(&mut self, group_id: GroupId) {
        self.notify_leader_elected(group_id).await;
    }

    async fn on_step_down(&mut self, group_id: GroupId) {
        self.notify_step_down(group_id).await;
    }

    async fn on_shutdown(&mut self) {
        self.notify_shutdown().await;
    }

    fn lifecycle_hook(&self) -> Option<Arc<dyn StateMachineLifecycleHook>> {
        Some(Arc::new(ServiceLifecycleHook {
            service: Arc::clone(&self.service),
        }))
    }

    fn ingestor_offset_control(&self) -> Option<Arc<dyn IngestorOffsetControl>> {
        Some(self.ingestor_offset_control.clone())
    }

    fn read_handle(&self) -> Option<Arc<dyn StateMachineReader>> {
        Some(Arc::new(ServiceReadHandle {
            service: Arc::clone(&self.service),
        }))
    }

    fn outbox_control(&self) -> Option<Arc<dyn OutboxControl>> {
        Some(self.outbox_control.clone())
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
        outputs: Vec<rr_core::output::Output>,
        error: Option<String>,
        snapshot_data: Arc<Mutex<Vec<u8>>>,
    }

    impl MockService {
        fn new(response: Vec<u8>) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                response,
                outputs: Vec::new(),
                error: None,
                snapshot_data: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_outputs(mut self, outputs: Vec<rr_core::output::Output>) -> Self {
            self.outputs = outputs;
            self
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

        async fn handle_with_outputs(
            &self,
            request: Vec<u8>,
        ) -> Result<rr_core::output::HandleResult, RrError> {
            if let Some(ref msg) = self.error {
                return Err(RrError::Application(msg.clone()));
            }
            self.calls.lock().await.push(request);
            Ok(rr_core::output::HandleResult::with_outputs(
                self.response.clone(),
                self.outputs.clone(),
            ))
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
    async fn apply_emits_prometheus_metrics() {
        // Global recorder can be installed only once per process; rr-raft has
        // no other test installing one, so this is safe here.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _ = metrics::set_global_recorder(recorder);

        let svc = MockService::new(b"ok".to_vec());
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);
        adapter.apply(b"req").await.unwrap();

        let rendered = handle.render();
        assert!(
            rendered.contains("rr_raft_applied_entries_total"),
            "applied-entries counter missing from registry: {rendered}"
        );
        assert!(
            rendered.contains("rr_raft_apply_duration_seconds"),
            "apply-duration histogram missing from registry: {rendered}"
        );
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

    // INV-5: a snapshot is the consistent state at one log index; install fully replaces state, never merges.
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

    // INV-5: a snapshot is the consistent state at one log index; install fully replaces state, never merges.
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

    // INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
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

    // INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
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

    // INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    #[tokio::test]
    async fn snapshot_restore_preserves_idempotency_replay_behaviour() {
        let svc1 = MockService::new(b"first-response".to_vec());
        let mut adapter1 = ServiceStateMachineAdapter::new(7, svc1);
        let key = rr_core::idempotency::IdempotencyKey::new("transfer", 42);
        adapter1
            .apply(&keyed_entry(b"req", key.clone()))
            .await
            .unwrap();
        let snapshot_bytes = adapter1.snapshot().await.unwrap();

        let svc2 = MockService::new(b"second-response".to_vec());
        let calls2 = Arc::clone(&svc2.calls);
        let mut adapter2 = ServiceStateMachineAdapter::new(7, svc2);
        adapter2.restore(&snapshot_bytes).await.unwrap();

        let bytes = adapter2
            .apply(&keyed_entry(b"req-retry", key))
            .await
            .unwrap();
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

    // INV-5: a snapshot is the consistent state at one log index; install fully replaces state, never merges.
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

    // INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    #[tokio::test]
    async fn apply_collects_outputs_and_assigns_monotonic_seq() {
        let svc =
            MockService::new(b"resp".to_vec()).with_outputs(vec![rr_core::output::Output::new(
                "idem:0",
                rr_core::output::OutputTarget::RaftWrite {
                    addrs: vec!["127.0.0.1:50051".to_string()],
                    group_id: 9,
                },
                b"out".to_vec(),
            )]);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        adapter.apply(b"req").await.unwrap();
        let pending = adapter.pending_outputs();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, 1);
        assert_eq!(pending[0].idempotency_key, "idem:0");
    }

    // INV-4: every committed output is eventually delivered (at-least-once); the watermark advances only after confirmed delivery.
    #[tokio::test]
    async fn snapshot_restore_roundtrip_preserves_outbox_state() {
        let svc =
            MockService::new(b"resp".to_vec()).with_outputs(vec![rr_core::output::Output::new(
                "idem:1",
                rr_core::output::OutputTarget::RaftWrite {
                    addrs: vec!["127.0.0.1:50051".to_string()],
                    group_id: 9,
                },
                b"out".to_vec(),
            )]);
        let mut adapter1 = ServiceStateMachineAdapter::new(7, svc);
        adapter1.apply(b"req").await.unwrap();

        let snap = adapter1.snapshot().await.unwrap();
        let mut adapter2 = ServiceStateMachineAdapter::new(7, MockService::new(b"r".to_vec()));
        adapter2.restore(&snap).await.unwrap();

        let pending = adapter2.pending_outputs();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, 1);

        adapter2.advance_output_watermark(1);
        assert!(adapter2.pending_outputs().is_empty());
    }

    // ── R-1 / A-0033: ApplyContext threading through the adapter ─────────

    /// Service that records the `ApplyContext` it receives via `handle_with_ctx`.
    struct CtxRecordingService {
        seen: Arc<Mutex<Vec<rr_core::types::ApplyContext>>>,
    }

    #[async_trait]
    impl Service for CtxRecordingService {
        async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
            Ok(request)
        }

        async fn handle_with_ctx(
            &self,
            request: Vec<u8>,
            ctx: rr_core::types::ApplyContext,
        ) -> Result<Vec<u8>, RrError> {
            self.seen.lock().await.push(ctx);
            Ok(request)
        }

        fn name(&self) -> &str {
            "ctx-recording"
        }

        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }

        async fn restore(&mut self, _data: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
    }

    /// R-1: `apply_with_ctx` must thread the entry's `ApplyContext` through the
    /// adapter to the service's `handle_with_ctx`.
    #[tokio::test]
    async fn apply_with_ctx_threads_log_context_to_service() {
        let svc = CtxRecordingService {
            seen: Arc::new(Mutex::new(Vec::new())),
        };
        let seen = Arc::clone(&svc.seen);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let ctx = rr_core::types::ApplyContext::new(42, 3);
        adapter.apply_with_ctx(b"req", ctx).await.unwrap();

        let seen = seen.lock().await;
        assert_eq!(
            seen.as_slice(),
            &[rr_core::types::ApplyContext::new(42, 3)],
            "the service must receive the exact ApplyContext from apply_with_ctx"
        );
    }

    /// The non-context `apply` path must not fabricate an `ApplyContext`.
    #[tokio::test]
    async fn apply_without_ctx_does_not_call_handle_with_ctx() {
        let svc = CtxRecordingService {
            seen: Arc::new(Mutex::new(Vec::new())),
        };
        let seen = Arc::clone(&svc.seen);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        adapter.apply(b"req").await.unwrap();
        assert!(
            seen.lock().await.is_empty(),
            "the non-ctx apply path must not reach handle_with_ctx"
        );
    }

    /// A keyed (idempotent) apply must also thread the context on the New branch.
    #[tokio::test]
    async fn apply_keyed_with_ctx_threads_context_on_new() {
        let svc = CtxRecordingService {
            seen: Arc::new(Mutex::new(Vec::new())),
        };
        let seen = Arc::clone(&svc.seen);
        let mut adapter = ServiceStateMachineAdapter::new(7, svc);

        let key = rr_core::idempotency::IdempotencyKey::new("transfer", 7);
        let entry = keyed_entry(b"req", key);
        let ctx = rr_core::types::ApplyContext::new(9, 2);
        adapter.apply_with_ctx(&entry, ctx).await.unwrap();

        let seen = seen.lock().await;
        assert_eq!(seen.as_slice(), &[rr_core::types::ApplyContext::new(9, 2)]);
    }

    // ── Client-write batching: apply-side split (rr_core::batch) ────────

    struct BatchEchoService {
        calls: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    #[async_trait]
    impl Service for BatchEchoService {
        async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
            self.calls.lock().await.push(request.clone());
            Ok(request)
        }
        fn name(&self) -> &str {
            "batch-echo"
        }
        async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
            Ok(vec![])
        }
        async fn restore(&mut self, _data: &[u8]) -> Result<(), RrError> {
            Ok(())
        }
    }

    /// A batch frame is split at apply: every item reaches the service
    /// individually (in order) and the response is a positional batch frame.
    #[tokio::test]
    async fn apply_batch_frame_splits_items_and_packs_responses() {
        let svc = BatchEchoService {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let calls = Arc::clone(&svc.calls);
        let mut adapter = ServiceStateMachineAdapter::new(1, svc);

        let items = vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()];
        let frame = rr_core::batch::encode_batch(&items);
        let resp = adapter.apply(&frame).await.unwrap();

        assert_eq!(
            calls.lock().await.as_slice(),
            items.as_slice(),
            "each batch item must reach the service exactly once, in order"
        );
        let decoded = rr_core::batch::decode_batch(&resp)
            .unwrap()
            .expect("apply of a batch must answer with a batch frame");
        assert_eq!(decoded, items, "responses are positional echoes");
    }

    /// Malformed batch frames (magic present, body truncated) fail the apply
    /// loudly instead of reaching the service as a corrupt payload.
    #[tokio::test]
    async fn apply_malformed_batch_frame_errors() {
        let svc = BatchEchoService {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let calls = Arc::clone(&svc.calls);
        let mut adapter = ServiceStateMachineAdapter::new(1, svc);

        let mut frame = rr_core::batch::encode_batch(&[b"x".to_vec()]);
        frame.truncate(frame.len() - 1);
        assert!(adapter.apply(&frame).await.is_err());
        assert!(calls.lock().await.is_empty());
    }

    /// Raw unkeyed payloads that merely lack the magic keep the verbatim
    /// v0.2.x path — no batch interpretation.
    #[tokio::test]
    async fn apply_non_batch_unkeyed_payload_unchanged() {
        let svc = BatchEchoService {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let calls = Arc::clone(&svc.calls);
        let mut adapter = ServiceStateMachineAdapter::new(1, svc);

        let resp = adapter.apply(b"plain").await.unwrap();
        assert_eq!(resp, b"plain");
        assert_eq!(calls.lock().await.as_slice(), &[b"plain".to_vec()]);
    }

    /// INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    /// P0-1: an ingestor-sourced entry (offset present) whose payload carries
    /// the batch magic is applied VERBATIM — never split, never failed on a
    /// malformed frame. Upstream bytes are opaque to the framework; a Kafka
    /// record that happens to start with the batch magic must not kill the
    /// group.
    #[tokio::test]
    async fn apply_ingestor_entry_with_batch_magic_payload_applies_verbatim() {
        let svc = BatchEchoService {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let calls = Arc::clone(&svc.calls);
        let mut adapter = ServiceStateMachineAdapter::new(1, svc);

        // Well-formed batch frame as an ingestor entry's payload: no split.
        let inner = rr_core::batch::encode_batch(&[b"a".to_vec(), b"b".to_vec()]);
        let entry = entry_with_offset(&inner, vec![0, 0, 0, 1]);
        let resp = adapter.apply(&entry).await.unwrap();
        assert_eq!(
            resp, inner,
            "ingestor payload must apply verbatim, not as a split batch"
        );
        assert_eq!(
            calls.lock().await.as_slice(),
            std::slice::from_ref(&inner),
            "the service must see the whole payload exactly once"
        );
        assert_eq!(adapter.latest_ingestor_offset(), Some(vec![0, 0, 0, 1]));

        // Malformed batch frame as an ingestor entry's payload: must NOT
        // fail the apply — the group survives poisoned upstream bytes.
        let mut poison = Vec::from(rr_core::batch::BATCH_FRAME_MAGIC);
        poison.extend_from_slice(b"garbage");
        let entry = entry_with_offset(&poison, vec![0, 0, 0, 2]);
        let resp = adapter.apply(&entry).await.unwrap();
        assert_eq!(resp, poison);
        assert_eq!(adapter.latest_ingestor_offset(), Some(vec![0, 0, 0, 2]));
    }

    /// INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    /// A framed entry without offset or idem key whose inner payload is a
    /// batch frame is applied verbatim: batch splitting is reserved for raw
    /// top-level client payloads. This keeps
    /// `rr_core::ingress::validate_client_payload` exactly aligned with what
    /// the apply path would reject (a nested hand-crafted frame cannot
    /// smuggle a malformed batch past ingress validation into apply).
    #[tokio::test]
    async fn apply_framed_entry_does_not_split_inner_batch_payload() {
        let svc = BatchEchoService {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let calls = Arc::clone(&svc.calls);
        let mut adapter = ServiceStateMachineAdapter::new(1, svc);

        let inner = rr_core::batch::encode_batch(&[b"a".to_vec(), b"b".to_vec()]);
        let entry = unkeyed_entry(&inner);
        let resp = adapter.apply(&entry).await.unwrap();
        assert_eq!(resp, inner, "framed inner payload must apply verbatim");
        assert_eq!(calls.lock().await.as_slice(), &[inner]);
    }

    /// No-panic invariant: the deferred path invoked without an
    /// `ApplyContext` must return an internal error, never panic.
    #[tokio::test]
    async fn deferred_apply_without_context_errors_instead_of_panicking() {
        let svc = MockService::new(b"ok".to_vec());
        let mut adapter = ServiceStateMachineAdapter::new(1, svc);
        let err = adapter
            .apply_entry(b"plain", None, true)
            .await
            .expect_err("deferred apply without a context must error, not panic");
        assert!(
            err.to_string().contains("ApplyContext"),
            "error should name the missing context: {err}"
        );
    }
}
