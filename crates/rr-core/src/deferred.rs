//! Deferred (two-phase) apply responses — R-2 / A-0033.
//!
//! # What this is
//!
//! Normally a `Service::handle` returns the response bytes synchronously and
//! the framework answers the client's `client_write` as soon as the entry is
//! applied. **Deferred response** lets an apply instead return
//! [`DeferredResponse::Pending`], holding the client's response under a
//! [`CorrelationId`]; a *later* entry's apply returns
//! [`DeferredResponse::Complete`] to deliver the held bytes. This is the
//! framework capability behind intent-sourcing / two-phase apply (A-0033):
//! phase 1 (e.g. `place_order_intent`) records a deterministic intent and
//! pends the client; a leader-only effector then executes the external side
//! effect and proposes a feedback command whose apply completes the response.
//!
//! # Semantics contract (2026-09-17 narrowing)
//!
//! Deferred response is **opt-in per request** and exists to serve "wait for
//! the result" (`respMode=full`) semantics. The `ack` style — return as soon
//! as phase-1 commits, deliver the outcome later via the outbox/event stream —
//! does **not** use it. The response contract therefore keeps both shapes:
//! `Immediate` for synchronous/ack handling and `Pending`/`Complete` for the
//! deferred/full path.
//!
//! # Failure semantics
//!
//! The pending responder lives only in the serving node's memory. On leader
//! change, restart, or if no `Complete` ever arrives, the held `client_write`
//! is answered by the client's own timeout (dropping the responder errors the
//! wait), and the client retries — at-least-once execution paired with
//! business-key idempotency is the authoritative exactly-once guarantee, not
//! the deferred channel.

use serde::{Deserialize, Serialize};

/// Links a [`DeferredResponse::Pending`] apply to the later apply that
/// completes it. For intent-sourcing this is the phase-1 entry's Raft log
/// index (see `ApplyContext::log_index`), which the feedback command then
/// references as its `intent_seq`.
pub type CorrelationId = u64;

/// One deferred response completed by a [`DeferredResponse::Complete`] apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedResponse {
    /// The `correlation_id` of the `Pending` apply being completed.
    pub correlation_id: CorrelationId,
    /// The response bytes to deliver to the held client.
    pub response: Vec<u8>,
}

/// The effect of applying one committed entry, returned by
/// [`Service::handle_deferred`](crate::service::Service::handle_deferred).
#[derive(Debug, Clone)]
pub enum DeferredHandleResult {
    /// Respond to this entry's client immediately (response + outputs).
    Immediate(crate::output::HandleResult),
    /// Hold this entry's client response under `correlation_id`. The framework
    /// parks the client's responder; no immediate response is sent. A later
    /// `Complete` carrying the same `correlation_id` delivers the response.
    Pending {
        /// The id the later `Complete` will reference.
        correlation_id: CorrelationId,
    },
    /// Complete previously-parked responses, then respond to this entry's own
    /// client (e.g. the leader-side effector that proposed the feedback
    /// command) with `result`.
    Complete {
        /// The parked responses to deliver now.
        completions: Vec<CompletedResponse>,
        /// This entry's own response + outputs.
        result: crate::output::HandleResult,
    },
}

/// The state-machine-level view of a deferred apply effect, surfaced by
/// [`StateMachine::apply_deferred`](crate::state_machine::StateMachine::apply_deferred)
/// to the Raft layer (`RrStateMachine`), which owns the client responders.
/// Unlike [`DeferredHandleResult`] this carries plain response bytes (outputs
/// have already been collected by the adapter).
#[derive(Debug, Clone, PartialEq)]
pub enum DeferredApply {
    /// Respond to this entry's client immediately.
    Immediate(Vec<u8>),
    /// Park this entry's client responder under `correlation_id`.
    Pending {
        /// The id the later `Complete` will reference.
        correlation_id: CorrelationId,
    },
    /// Complete parked responses, then respond to this entry's own client.
    Complete {
        /// The parked responses to deliver now.
        completions: Vec<CompletedResponse>,
        /// This entry's own response bytes.
        response: Vec<u8>,
    },
}

impl DeferredApply {
    /// Extract this entry's own immediate response bytes.
    ///
    /// Only the non-deferred `apply` / `apply_with_ctx` entry points use this;
    /// they never dispatch through the two-phase path, so `Pending` cannot
    /// occur there. `Pending` maps to empty bytes defensively (it would
    /// indicate a service driving deferral through a non-deferred caller);
    /// that defensive hit is loud — `tracing::warn!` plus a metrics counter —
    /// because it signals a programming error in service wiring, not a
    /// client fault.
    pub fn into_response(self) -> Vec<u8> {
        match self {
            DeferredApply::Immediate(bytes) => bytes,
            DeferredApply::Pending { correlation_id } => {
                // R-2 / A-0033: the non-deferred apply path never parks a
                // responder, so a Pending here means a deferring service
                // answered a caller that cannot receive a later Complete.
                // Keep the defensive empty-bytes mapping (an error here would
                // wedge the apply loop), but stay observable: warn + counter.
                tracing::warn!(
                    correlation_id,
                    "DeferredApply::Pending reached the non-deferred response path; \
                     a deferring service is answering a caller that never parked a \
                     deferred responder (programming-error signal — check Service \
                     wiring; the client receives an empty success response)"
                );
                metrics::counter!("rr_deferred_pending_via_non_deferred_total").increment(1);
                Vec::new()
            }
            DeferredApply::Complete { response, .. } => response,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    // ── into_response variant conversions ─────────────────────────────

    #[test]
    fn into_response_immediate_returns_bytes() {
        let bytes = DeferredApply::Immediate(b"ok".to_vec()).into_response();
        assert_eq!(bytes, b"ok".to_vec());
    }

    // R-2 / A-0033: Complete answers THIS entry's own client with `response`;
    // the parked `completions` are delivered by the raft layer, not by
    // into_response — extracting the own response must not consume them.
    #[test]
    fn into_response_complete_returns_own_response_ignoring_completions() {
        let apply = DeferredApply::Complete {
            completions: vec![CompletedResponse {
                correlation_id: 9,
                response: b"parked".to_vec(),
            }],
            response: b"own".to_vec(),
        };
        assert_eq!(apply.into_response(), b"own".to_vec());
    }

    // R-2 / A-0033: Pending through the non-deferred path is a wiring error;
    // the defensive contract is "empty success bytes", never a panic — the
    // apply loop must keep making progress for the other entries' clients.
    #[test]
    fn into_response_pending_maps_to_empty_bytes() {
        let bytes = DeferredApply::Pending { correlation_id: 1 }.into_response();
        assert!(
            bytes.is_empty(),
            "Pending must defensively map to empty bytes"
        );
    }

    // CompletedResponse crosses the adapter boundary as plain data; pin the
    // bincode (de)serialization contract the deferred channel relies on.
    #[test]
    fn completed_response_bincode_roundtrip() {
        let completed = CompletedResponse {
            correlation_id: 42,
            response: b"done".to_vec(),
        };
        let encoded =
            bincode::serde::encode_to_vec(&completed, bincode::config::standard()).unwrap();
        let (decoded, n): (CompletedResponse, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(n, encoded.len());
        assert_eq!(decoded, completed);
    }

    // ── Pending defensive-path observability ──────────────────────────

    /// Minimal `tracing::Subscriber` that captures WARN-level event messages
    /// into a shared buffer. Hand-rolled (instead of pulling
    /// tracing-subscriber into dev-dependencies) because the only assertion
    /// needed here is "a warn fired on the defensive path".
    #[derive(Clone, Default)]
    struct WarnCapture {
        messages: Arc<Mutex<Vec<String>>>,
    }

    impl tracing::Subscriber for WarnCapture {
        fn enabled(&self, meta: &tracing::Metadata<'_>) -> bool {
            meta.level() <= &tracing::Level::WARN
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let mut visitor = MessageVisitor(None);
            event.record(&mut visitor);
            if let Some(msg) = visitor.0 {
                self.messages.lock().unwrap().push(msg);
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[derive(Default)]
    struct MessageVisitor(Option<String>);

    impl tracing::field::Visit for MessageVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = Some(format!("{value:?}"));
            }
        }
    }

    // R-2 / A-0033: the silent empty-success mapping must stay loud — the
    // warn is the operator-facing signal that a service is deferring
    // responses on a caller that never parked a responder.
    #[test]
    fn pending_via_non_deferred_path_emits_warn() {
        let capture = WarnCapture::default();
        {
            let _guard = tracing::subscriber::set_default(capture.clone());
            let bytes = DeferredApply::Pending { correlation_id: 42 }.into_response();
            assert!(
                bytes.is_empty(),
                "defensive mapping must stay empty-bytes while the warn fires"
            );
        }
        let messages = capture.messages.lock().unwrap();
        assert!(
            messages
                .iter()
                .any(|m| m.contains("non-deferred response path")),
            "expected a warn on the defensive Pending path, got: {messages:?}"
        );
    }

    /// Minimal `metrics::Recorder` that counts counter increments and
    /// remembers the registered counter key names.
    struct CountingRecorder {
        hits: Arc<AtomicU64>,
        seen_counter_keys: Arc<Mutex<Vec<String>>>,
    }

    impl metrics::Recorder for CountingRecorder {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn register_counter(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            self.seen_counter_keys
                .lock()
                .unwrap()
                .push(key.name().to_string());
            metrics::Counter::from_arc(Arc::new(CountingCounter {
                hits: Arc::clone(&self.hits),
            }))
        }
        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }
        fn register_histogram(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    struct CountingCounter {
        hits: Arc<AtomicU64>,
    }

    impl metrics::CounterFn for CountingCounter {
        fn increment(&self, value: u64) {
            self.hits.fetch_add(value, Ordering::SeqCst);
        }
        fn absolute(&self, value: u64) {
            self.hits.store(value, Ordering::SeqCst);
        }
    }

    // R-2 / A-0033: metrics is the dashboard-side tripwire for the same
    // wiring error the warn reports on the log side.
    #[test]
    fn pending_via_non_deferred_path_increments_counter() {
        let hits = Arc::new(AtomicU64::new(0));
        let seen_keys = Arc::new(Mutex::new(Vec::new()));
        let recorder = CountingRecorder {
            hits: Arc::clone(&hits),
            seen_counter_keys: Arc::clone(&seen_keys),
        };
        metrics::with_local_recorder(&recorder, || {
            DeferredApply::Pending { correlation_id: 7 }.into_response();
        });
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "defensive Pending path must increment its counter exactly once"
        );
        let keys = seen_keys.lock().unwrap();
        assert!(
            keys.iter()
                .any(|k| k == "rr_deferred_pending_via_non_deferred_total"),
            "counter key must be rr_deferred_pending_via_non_deferred_total, saw: {keys:?}"
        );
    }
}
