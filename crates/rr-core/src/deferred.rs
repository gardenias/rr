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
    /// indicate a service driving deferral through a non-deferred caller).
    pub fn into_response(self) -> Vec<u8> {
        match self {
            DeferredApply::Immediate(bytes) => bytes,
            DeferredApply::Pending { .. } => Vec::new(),
            DeferredApply::Complete { response, .. } => response,
        }
    }
}
