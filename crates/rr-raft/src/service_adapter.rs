//! High-level Service → StateMachine bridge adapter.
//!
//! [`ServiceStateMachineAdapter`] wraps a user's [`Service`](rr_core::service::Service)
//! implementation to fulfill the framework's internal
//! [`StateMachine`](rr_core::state_machine::StateMachine) trait. It is a thin
//! pass-through: no interpretation of entry bytes occurs.

use async_trait::async_trait;

use rr_core::error::RrError;
use rr_core::service::Service;
use rr_core::state_machine::StateMachine;
use rr_core::types::LogIndex;

/// Adapter that bridges a high-level [`Service`] to the low-level [`StateMachine`] trait.
///
/// The adapter delegates all operations directly to the wrapped service and
/// tracks how many entries have been applied via an internal counter.
pub struct ServiceStateMachineAdapter<S: Service> {
    service: S,
    applied_count: u64,
}

impl<S: Service> ServiceStateMachineAdapter<S> {
    /// Create a new adapter wrapping the given service.
    pub fn new(service: S) -> Self {
        Self {
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
        self.service.snapshot().await
    }

    async fn restore(&mut self, snapshot: &[u8]) -> Result<(), RrError> {
        self.service.restore(snapshot).await
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
        let mut adapter = ServiceStateMachineAdapter::new(svc);

        let input = b"request-data";
        let result = adapter.apply(input).await.unwrap();

        assert_eq!(result, b"response-42");
        let recorded = calls.lock().await;
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], input.to_vec());
    }

    #[tokio::test]
    async fn snapshot_delegates_to_service() {
        let svc = MockService::new(vec![]).with_snapshot(b"snap-state".to_vec());
        let adapter = ServiceStateMachineAdapter::new(svc);

        let snap = adapter.snapshot().await.unwrap();
        assert_eq!(snap, b"snap-state");
    }

    #[tokio::test]
    async fn restore_delegates_to_service() {
        let svc = MockService::new(vec![]);
        let snapshot_data = Arc::clone(&svc.snapshot_data);
        let mut adapter = ServiceStateMachineAdapter::new(svc);

        adapter.restore(b"restored-state").await.unwrap();

        let data = snapshot_data.lock().await;
        assert_eq!(*data, b"restored-state");
    }

    #[tokio::test]
    async fn error_propagation_from_handle() {
        let svc = MockService::new(vec![]).with_error("service failed");
        let mut adapter = ServiceStateMachineAdapter::new(svc);

        let result = adapter.apply(b"anything").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("service failed"));
    }

    #[tokio::test]
    async fn last_applied_tracking() {
        let svc = MockService::new(b"ok".to_vec());
        let mut adapter = ServiceStateMachineAdapter::new(svc);

        // Initially None
        assert_eq!(adapter.last_applied().await, None);

        // After 1 apply → Some(1)
        adapter.apply(b"a").await.unwrap();
        assert_eq!(adapter.last_applied().await, Some(1));

        // After 2 applies → Some(2)
        adapter.apply(b"b").await.unwrap();
        assert_eq!(adapter.last_applied().await, Some(2));

        // After 3 applies → Some(3)
        adapter.apply(b"c").await.unwrap();
        assert_eq!(adapter.last_applied().await, Some(3));
    }

    #[tokio::test]
    async fn failed_apply_does_not_increment_counter() {
        let svc = MockService::new(vec![]).with_error("boom");
        let mut adapter = ServiceStateMachineAdapter::new(svc);

        let _ = adapter.apply(b"data").await;
        assert_eq!(adapter.last_applied().await, None);
    }
}
