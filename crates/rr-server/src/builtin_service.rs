//! Built-in state machines that ship with the `rr-server` binary.
//!
//! These exist so the generic `rr-server` binary can start a smoke-testable
//! cluster out of the box — without a custom [`Service`] implementation.
//! Users who want real business logic should model their own binary on
//! `examples/kv-store` or `examples/counter`, which use the same crates
//! as rr-server but embed their own [`Service`] implementation.
//!
//! [`Service`]: rr_core::service::Service

use async_trait::async_trait;
use rr_core::error::RrError;
use rr_core::state_machine::{StateMachine, StateMachineReader};
use rr_core::types::LogIndex;
use std::sync::Arc;

/// A minimal state machine that echoes the committed entry back as the response.
///
/// Useful for smoke tests and for verifying the plumbing of a brand-new cluster.
/// Snapshots contain no state (empty byte slice); restore is a no-op. Do not
/// rely on this for real workloads.
pub struct EchoStateMachine {
    last_applied: Option<LogIndex>,
}

impl EchoStateMachine {
    pub fn new() -> Self {
        Self { last_applied: None }
    }
}

impl Default for EchoStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

/// Read handle that echoes the read payload back, mirroring `apply`'s echo
/// semantics. Lets the stock `rr-server` binary serve `ReadLocal` /
/// `ReadLinearizable` for smoke tests and benchmarks instead of failing with
/// "state machine does not support local read".
struct EchoReader;

#[async_trait]
impl StateMachineReader for EchoReader {
    async fn read(&self, payload: Vec<u8>) -> Result<Vec<u8>, RrError> {
        Ok(payload)
    }
}

#[async_trait]
impl StateMachine for EchoStateMachine {
    async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
        Ok(entry.to_vec())
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        Ok(Vec::new())
    }

    async fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RrError> {
        Ok(())
    }

    async fn last_applied(&self) -> Option<LogIndex> {
        self.last_applied
    }

    fn read_handle(&self) -> Option<Arc<dyn StateMachineReader>> {
        Some(Arc::new(EchoReader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo_apply_returns_input_verbatim() {
        let mut sm = EchoStateMachine::new();
        let out = sm.apply(b"hello").await.unwrap();
        assert_eq!(out, b"hello".to_vec());
    }

    #[tokio::test]
    async fn echo_snapshot_is_empty() {
        let sm = EchoStateMachine::new();
        let snap = sm.snapshot().await.unwrap();
        assert!(snap.is_empty());
    }

    #[tokio::test]
    async fn echo_restore_is_noop() {
        let mut sm = EchoStateMachine::new();
        sm.restore(b"anything").await.unwrap();
        assert_eq!(sm.apply(b"x").await.unwrap(), b"x".to_vec());
    }

    #[tokio::test]
    async fn echo_read_handle_echoes_payload() {
        let sm = EchoStateMachine::new();
        let reader = sm.read_handle().expect("echo SM must expose a read handle");
        assert_eq!(
            reader.read(b"ping".to_vec()).await.unwrap(),
            b"ping".to_vec()
        );
    }
}
