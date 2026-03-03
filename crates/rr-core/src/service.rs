//! Service trait for high-level user-facing service implementations.

use async_trait::async_trait;

use crate::error::RrError;

/// High-level trait for implementing a distributed service.
///
/// Users implementing this trait do not need to be aware of Raft internals.
/// The framework wraps the Service in a `ServiceStateMachineAdapter` to
/// bridge it with the Raft state machine.
///
/// # Example
/// ```rust,ignore
/// struct KvService { store: Arc<RwLock<HashMap<String, String>>> }
///
/// #[async_trait::async_trait]
/// impl Service for KvService {
///     async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
///         // decode request, perform operation, encode response
///     }
///     fn name(&self) -> &str { "kv" }
///     // ...
/// }
/// ```
#[async_trait]
pub trait Service: Send + Sync {
    /// Handle an incoming client request.
    ///
    /// The framework will propose the request bytes through Raft consensus
    /// before calling this method on the leader. Returns response bytes.
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError>;

    /// Return the name of this service (used for identification and logging).
    fn name(&self) -> &str;

    /// Create a snapshot of the service state for Raft snapshot installation.
    async fn snapshot(&self) -> Result<Vec<u8>, RrError>;

    /// Restore service state from a snapshot.
    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verify Service is object-safe
    #[test]
    fn test_service_is_object_safe() {
        let _: Option<Box<dyn Service>> = None;
    }
}
