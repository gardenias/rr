//! StateMachine trait for user-defined state machines backed by Raft consensus.

use async_trait::async_trait;

use crate::error::RrError;
use crate::types::LogIndex;

/// Low-level trait for implementing a Raft-backed state machine.
///
/// Implementations receive raw byte entries from the Raft log and must
/// maintain their own serialization/deserialization. The framework handles
/// the Raft consensus — the state machine only needs to apply entries.
///
/// # Example
/// ```rust,ignore
/// struct KvStateMachine { store: HashMap<String, String> }
///
/// #[async_trait::async_trait]
/// impl StateMachine for KvStateMachine {
///     async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
///         // decode entry, update store, return response
///     }
///     // ...
/// }
/// ```
#[async_trait]
pub trait StateMachine: Send + Sync {
    /// Apply a committed Raft log entry to the state machine.
    ///
    /// `entry` is the raw bytes payload proposed by the client.
    /// Returns the response bytes to be sent back to the client.
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError>;

    /// Create a consistent snapshot of the current state.
    ///
    /// Returns serialized snapshot bytes.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// Restore state from a snapshot.
    ///
    /// Called when installing a snapshot received from the leader.
    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError>;

    /// Return the index of the last applied log entry, if any.
    async fn last_applied(&self) -> Option<LogIndex>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verify StateMachine is object-safe
    #[test]
    fn test_state_machine_is_object_safe() {
        let _: Option<Box<dyn StateMachine>> = None;
    }
}
