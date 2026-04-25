//! [`Service`] — high-level user-facing trait for distributed state.
//!
//! Implement [`Service`] when you have business logic that should run inside
//! a Raft-replicated state machine but you don't want to deal with the raw
//! state-machine plumbing. The framework wraps `Service` in
//! `rr_raft::ServiceStateMachineAdapter` to bridge it onto OpenRaft's
//! `RaftStateMachine` interface.
//!
//! See the [`Service`] trait rustdoc for the **mandatory** safety invariants
//! every implementation must uphold to avoid silent data corruption. The
//! invariants are also walked through with worked examples in
//! `docs/docs/guide/service-trait.md`.

use async_trait::async_trait;

use crate::error::RrError;

/// High-level trait for implementing a distributed service.
///
/// Users implementing this trait do not need to be aware of Raft internals.
/// The framework wraps the `Service` in a `ServiceStateMachineAdapter` to
/// bridge it with the Raft state machine.
///
/// # Safety Invariants (MUST uphold)
///
/// `Service` runs inside the OpenRaft state-machine task, which has specific
/// concurrency contracts. Violating any of the three invariants below
/// produces **silent data corruption** — your snapshots will be inconsistent,
/// followers will diverge from the leader, and there is no runtime check that
/// catches it before it ships.
///
/// ## Invariant 1 — `snapshot()` must capture an atomically-consistent view
///
/// The framework calls [`snapshot`](Self::snapshot) inside a tokio task that
/// runs **concurrently** with [`handle`](Self::handle) calls on the same
/// instance (per OpenRaft's `RaftSnapshotBuilder` contract). You MUST either
///
/// - hold a single read-lock that covers every field included in the snapshot
///   for its entire duration, OR
/// - take an immutable structural snapshot (e.g. `Arc<im::HashMap>` clone) up
///   front and serialize that.
///
/// ### ❌ Incorrect — silent corruption
/// ```rust,ignore
/// async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
///     let a = self.map_a.read().await;
///     let snap_a = a.clone();
///     drop(a);                                  // gap window opens here
///     let b = self.map_b.read().await;          // b may already reflect a write
///     let snap_b = b.clone();                   // (a, b) no longer correlated
///     bincode::serde::encode_to_vec(&(snap_a, snap_b), bincode::config::standard())
///         .map_err(|e| /* ... */)
/// }
/// ```
///
/// ### ✅ Correct — single lock
/// ```rust,ignore
/// async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
///     let state = self.state.read().await;     // covers ALL fields
///     bincode::serde::encode_to_vec(&*state, bincode::config::standard())
///         .map_err(|e| /* ... */)
/// }
/// ```
///
/// ## Invariant 2 — `handle()` must complete state updates before returning
///
/// After [`handle`](Self::handle) returns, the framework may call
/// [`snapshot`](Self::snapshot) immediately. Any state change initiated by
/// `handle()` MUST be visible to subsequent `snapshot()` calls on the same
/// instance.
///
/// ### ❌ Forbidden — fire-and-forget mutations
/// ```rust,ignore
/// async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
///     let state = Arc::clone(&self.state);
///     tokio::spawn(async move {
///         state.write().await.apply_change();   // races with future snapshot()
///     });
///     Ok(response)
/// }
/// ```
///
/// This pattern leaks mutations outside the apply window. Future snapshots
/// will be inconsistent or stale; followers receiving such a snapshot will
/// silently diverge. Always update synchronously before returning from
/// `handle()`.
///
/// ## Invariant 3 — `restore()` completely replaces state
///
/// When the framework calls [`restore`](Self::restore), the service MUST
/// discard all existing in-memory state and reconstruct from the provided
/// bytes. After `restore()` returns, no subsequent operation may reference
/// any pre-restore data.
///
/// ### ❌ Incorrect — partial restore
/// ```rust,ignore
/// async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
///     let restored = decode(data)?;
///     for (k, v) in restored { self.map.insert(k, v); }   // merges, doesn't replace
///     Ok(())
/// }
/// ```
///
/// ### ✅ Correct — full replacement
/// ```rust,ignore
/// async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
///     let restored = decode(data)?;
///     self.map = restored;                                // wholesale swap
///     Ok(())
/// }
/// ```
///
/// # Example
/// ```rust,ignore
/// struct KvService { store: Arc<RwLock<HashMap<String, String>>> }
///
/// #[async_trait::async_trait]
/// impl Service for KvService {
///     async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
///         // decode request, perform operation under a single lock, encode response
///     }
///     fn name(&self) -> &str { "kv" }
///     // ...
/// }
/// ```
///
/// # See also
///
/// - [ADR A-0002 §D1](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md)
///   for the OpenRaft source citations behind these invariants.
/// - `docs/docs/guide/service-trait.md` for worked examples in both
///   English and Chinese.
#[async_trait]
pub trait Service: Send + Sync {
    /// Handle an incoming client request.
    ///
    /// The framework will propose the request bytes through Raft consensus
    /// before calling this method on the leader. Returns response bytes.
    ///
    /// **Must satisfy [Invariant 2](Self#invariant-2--handle-must-complete-state-updates-before-returning)**:
    /// every state mutation must be persisted to `self` before this method
    /// returns. No `tokio::spawn` of write tasks; no detached background work.
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError>;

    /// Return the name of this service (used for identification and logging).
    fn name(&self) -> &str;

    /// Create a snapshot of the service state for Raft snapshot installation.
    ///
    /// **Must satisfy [Invariant 1](Self#invariant-1--snapshot-must-capture-an-atomically-consistent-view)**:
    /// the bytes returned must reflect a single point-in-time view of every
    /// field in the snapshot. Take one lock (or one structural clone) and
    /// hold it until serialization completes.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// Restore service state from a snapshot.
    ///
    /// **Must satisfy [Invariant 3](Self#invariant-3--restore-completely-replaces-state)**:
    /// existing state must be discarded, not merged. After this method
    /// returns, the service must behave exactly as if the snapshot bytes
    /// were the only history it had ever seen.
    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_is_object_safe() {
        let _: Option<Box<dyn Service>> = None;
    }
}
