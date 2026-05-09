//! [`StateMachine`] — low-level Raft state-machine trait.
//!
//! Implement [`StateMachine`] directly when you need full control over the
//! framework integration: custom snapshot framing, log-index-aware apply,
//! or interleaving with non-Raft state. Most users should prefer
//! [`crate::service::Service`] (a higher-level trait) and let
//! `rr_raft::ServiceStateMachineAdapter` bridge it for them.
//!
//! See the [`StateMachine`] trait rustdoc for the **mandatory** safety
//! invariants every implementation must uphold.

use async_trait::async_trait;
use std::sync::Arc;

use crate::error::RrError;
use crate::output::Output;
use crate::types::{GroupId, LogIndex};

/// Optional runtime control surface for state machines that persist
/// framework-level ingestor progress.
///
/// Implementations may expose this through
/// [`StateMachine::ingestor_offset_control`] so operators can override
/// a bad/corrupt persisted ingestor offset during disaster recovery.
pub trait IngestorOffsetControl: Send + Sync {
    /// Set the latest ingestor offset bytes.
    ///
    /// `None` means clear/unset the persisted offset.
    fn set_offset(&self, offset: Option<Vec<u8>>);

    /// Read the latest ingestor offset bytes.
    fn get_offset(&self) -> Option<Vec<u8>>;
}

/// Read-only handle for serving follower reads directly from a local state
/// machine replica.
#[async_trait]
pub trait StateMachineReader: Send + Sync {
    /// Read from local state machine replica.
    async fn read(&self, payload: Vec<u8>) -> Result<Vec<u8>, RrError>;
}

/// Optional runtime control surface for state machines that persist
/// framework-level outbox progress.
pub trait OutboxControl: Send + Sync {
    /// Read outputs whose `seq` is newer than the sent watermark.
    fn pending_outputs(&self) -> Vec<Output>;

    /// Advance sent watermark and allow implementation to GC older entries.
    fn advance_watermark(&self, seq: u64);
}

/// Optional lifecycle callback surface the group manager can invoke on
/// leadership transitions and shutdown.
#[async_trait]
pub trait StateMachineLifecycleHook: Send + Sync {
    /// Called when this node becomes group leader.
    async fn on_leader_elected(&self, _group_id: GroupId) {}

    /// Called when this node steps down from leadership.
    async fn on_step_down(&self, _group_id: GroupId) {}

    /// Called before the group is stopped.
    async fn on_shutdown(&self) {}
}

/// Low-level trait for implementing a Raft-backed state machine.
///
/// Implementations receive raw byte entries from the Raft log and must
/// maintain their own serialization/deserialization. The framework handles
/// the Raft consensus — the state machine only needs to apply entries.
///
/// # Safety Invariants (MUST uphold)
///
/// The same three invariants as [`crate::service::Service`] apply, mapped
/// onto the lower-level trait shape. Violating them produces **silent data
/// corruption** with no runtime check.
///
/// ## Invariant 1 — `snapshot()` captures an atomically-consistent view
///
/// OpenRaft calls [`snapshot`](Self::snapshot) in a spawned task that runs
/// **concurrently** with [`apply`](Self::apply). The implementer MUST hold
/// a single lock or use immutable structures so the bytes reflect a
/// point-in-time view of the entire state machine.
///
/// See `docs/docs/guide/state-machine-trait.md` for worked examples.
///
/// ## Invariant 2 — `apply()` completes mutations before returning
///
/// After [`apply`](Self::apply) returns, [`snapshot`](Self::snapshot) may
/// be invoked immediately. Every mutation triggered by the entry must be
/// persisted to `self` (or to whatever backing store the snapshot reads
/// from) **before** returning. No `tokio::spawn` of write tasks; no
/// channels-buffered writes that haven't drained.
///
/// Additionally, `apply` MUST update `last_applied` — the value
/// returned by [`last_applied`](Self::last_applied) must reflect the index
/// of the entry just applied (or higher). This is essential for OpenRaft's
/// recovery flow; see ADR A-0002 §Contract 2.
///
/// ## Invariant 3 — `restore()` completely replaces state
///
/// When [`restore`](Self::restore) is called, every prior state must be
/// discarded and rebuilt from the snapshot bytes. After `restore`,
/// [`last_applied`](Self::last_applied) must reflect the snapshot's
/// `last_log_id`, not the value before restore.
///
/// # Example
/// ```rust,ignore
/// struct KvStateMachine {
///     store: HashMap<String, String>,
///     last_applied: Option<LogIndex>,
/// }
///
/// #[async_trait::async_trait]
/// impl StateMachine for KvStateMachine {
///     async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
///         // decode entry, update store synchronously, set self.last_applied,
///         // then return — no spawned writes.
///     }
///     // ...
/// }
/// ```
///
/// # See also
///
/// - [`crate::service::Service`] — higher-level trait that's easier to get
///   right; the same invariants apply but the framework manages
///   `last_applied` for you.
/// - [ADR A-0002](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md)
///   for the OpenRaft source citations behind these invariants.
#[async_trait]
pub trait StateMachine: Send + Sync {
    /// Apply a committed Raft log entry to the state machine.
    ///
    /// `entry` is the raw bytes payload proposed by the client.
    /// Returns the response bytes to be sent back to the client.
    ///
    /// **Must satisfy [Invariant 2](Self#invariant-2--apply-completes-mutations-before-returning)**:
    /// every mutation must be persisted to `self` synchronously before
    /// returning, and `self.last_applied` must reflect the applied index.
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError>;

    /// Create a consistent snapshot of the current state.
    ///
    /// **Must satisfy [Invariant 1](Self#invariant-1--snapshot-captures-an-atomically-consistent-view)**:
    /// the bytes must reflect a single point-in-time view; take one lock
    /// (or one structural clone) and hold it until serialization completes.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// Restore state from a snapshot.
    ///
    /// **Must satisfy [Invariant 3](Self#invariant-3--restore-completely-replaces-state)**:
    /// discard all existing state, rebuild from the bytes, and update
    /// `last_applied` to match the snapshot's `last_log_id`.
    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError>;

    /// Return the index of the last applied log entry, if any.
    async fn last_applied(&self) -> Option<LogIndex>;

    /// Called when this node becomes the group leader.
    async fn on_leader_elected(&mut self, _group_id: GroupId) {}

    /// Called when this node steps down from leadership.
    async fn on_step_down(&mut self, _group_id: GroupId) {}

    /// Called before the group is stopped.
    async fn on_shutdown(&mut self) {}

    /// Optional lifecycle hook handle for out-of-band lifecycle notifications.
    ///
    /// Most state machines can keep the default `None` implementation.
    fn lifecycle_hook(&self) -> Option<Arc<dyn StateMachineLifecycleHook>> {
        None
    }

    /// Optional control surface for overriding persisted ingestor offset.
    ///
    /// Most state machines don't track ingestor offsets and should keep
    /// the default `None` implementation.
    fn ingestor_offset_control(&self) -> Option<Arc<dyn IngestorOffsetControl>> {
        None
    }

    /// Optional read handle for local follower reads.
    ///
    /// Most state machines can keep the default `None` implementation.
    fn read_handle(&self) -> Option<Arc<dyn StateMachineReader>> {
        None
    }

    /// Optional control surface for output outbox access.
    ///
    /// Most state machines don't track outbox outputs and should keep
    /// the default `None` implementation.
    fn outbox_control(&self) -> Option<Arc<dyn OutboxControl>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_machine_is_object_safe() {
        let _: Option<Box<dyn StateMachine>> = None;
    }
}
