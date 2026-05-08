//! In-memory [`CommandIngestor`] for testing without a live Kafka
//! broker.
//!
//! `MockKafkaIngestor` exposes the same `CommandIngestor` surface as
//! the real `KafkaIngestor`, so end-to-end framework tests can
//! exercise the lifecycle (seek + poll + commit) without standing up
//! testcontainers. Use the real backend (`crate::KafkaIngestor`)
//! gated behind the `rdkafka` feature for production.

use async_trait::async_trait;
use rr_core::error::RrError;
use rr_core::ingestor::{CommandIngestor, IngressCommand};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Test-only in-memory ingestor with [`i64`] offsets — same offset
/// type the real Kafka backend uses, so tests written against this
/// mock can be ported to the real backend without changing offset
/// types.
pub struct MockKafkaIngestor {
    inner: Arc<Mutex<MockState>>,
}

struct MockState {
    queue: VecDeque<IngressCommand<i64>>,
    last_seek: Option<Option<i64>>,
    commits: Vec<i64>,
    fail_next_poll: bool,
}

/// Cloneable handle for a test to push messages into the mock and
/// inspect what `commit_applied` / `seek` saw.
#[derive(Clone)]
pub struct MockKafkaHandle {
    inner: Arc<Mutex<MockState>>,
}

impl MockKafkaIngestor {
    /// Construct an empty mock + a handle the test uses to drive it.
    pub fn new() -> (Self, MockKafkaHandle) {
        let state = Arc::new(Mutex::new(MockState {
            queue: VecDeque::new(),
            last_seek: None,
            commits: Vec::new(),
            fail_next_poll: false,
        }));
        (
            Self {
                inner: Arc::clone(&state),
            },
            MockKafkaHandle { inner: state },
        )
    }
}

impl Default for MockKafkaIngestor {
    fn default() -> Self {
        Self::new().0
    }
}

impl MockKafkaHandle {
    /// Append a command to the mock's queue.
    pub async fn push(&self, offset: i64, payload: Vec<u8>) {
        self.inner
            .lock()
            .await
            .queue
            .push_back(IngressCommand { offset, payload });
    }

    /// Snapshot the most recent [`CommandIngestor::seek`] argument.
    pub async fn last_seek(&self) -> Option<Option<i64>> {
        self.inner.lock().await.last_seek
    }

    /// Snapshot the offsets that have been passed to
    /// [`CommandIngestor::commit_applied`].
    pub async fn commits(&self) -> Vec<i64> {
        self.inner.lock().await.commits.clone()
    }

    /// Force the next [`CommandIngestor::poll`] call to error.
    pub async fn fail_next_poll(&self) {
        self.inner.lock().await.fail_next_poll = true;
    }
}

#[async_trait]
impl CommandIngestor for MockKafkaIngestor {
    type Offset = i64;

    async fn seek(&mut self, offset: Option<i64>) -> Result<(), RrError> {
        let mut g = self.inner.lock().await;
        g.last_seek = Some(offset);
        if let Some(o) = offset {
            while g.queue.front().map(|c| c.offset <= o).unwrap_or(false) {
                g.queue.pop_front();
            }
        }
        Ok(())
    }

    async fn poll(&mut self, max_batch_size: usize) -> Result<Vec<IngressCommand<i64>>, RrError> {
        let mut g = self.inner.lock().await;
        if g.fail_next_poll {
            g.fail_next_poll = false;
            return Err(RrError::Application(
                "mock kafka: simulated poll failure".to_string(),
            ));
        }
        let n = g.queue.len().min(max_batch_size);
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(cmd) = g.queue.pop_front() {
                out.push(cmd);
            }
        }
        Ok(out)
    }

    async fn commit_applied(&mut self, offset: i64) -> Result<(), RrError> {
        self.inner.lock().await.commits.push(offset);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn poll_returns_pushed_messages_in_order() {
        let (mut ing, h) = MockKafkaIngestor::new();
        h.push(10, b"a".to_vec()).await;
        h.push(11, b"b".to_vec()).await;
        let batch = ing.poll(10).await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].offset, 10);
        assert_eq!(batch[0].payload, b"a");
        assert_eq!(batch[1].offset, 11);
    }

    #[tokio::test]
    async fn poll_respects_max_batch_size() {
        let (mut ing, h) = MockKafkaIngestor::new();
        for i in 0..5 {
            h.push(i, vec![i as u8]).await;
        }
        let batch = ing.poll(2).await.unwrap();
        assert_eq!(batch.len(), 2);
        let next = ing.poll(2).await.unwrap();
        assert_eq!(next.len(), 2);
        let last = ing.poll(2).await.unwrap();
        assert_eq!(last.len(), 1);
    }

    #[tokio::test]
    async fn poll_returns_empty_when_drained() {
        let (mut ing, _h) = MockKafkaIngestor::new();
        let batch = ing.poll(10).await.unwrap();
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn seek_records_offset_and_drops_earlier_messages() {
        let (mut ing, h) = MockKafkaIngestor::new();
        for i in 0..10 {
            h.push(i, vec![]).await;
        }
        ing.seek(Some(5)).await.unwrap();
        assert_eq!(h.last_seek().await, Some(Some(5)));
        let batch = ing.poll(20).await.unwrap();
        assert_eq!(batch.len(), 4);
        assert_eq!(batch[0].offset, 6);
    }

    #[tokio::test]
    async fn seek_with_none_keeps_all_messages() {
        let (mut ing, h) = MockKafkaIngestor::new();
        for i in 0..3 {
            h.push(i, vec![]).await;
        }
        ing.seek(None).await.unwrap();
        let batch = ing.poll(20).await.unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn commit_applied_is_recorded() {
        let (mut ing, h) = MockKafkaIngestor::new();
        ing.commit_applied(42).await.unwrap();
        ing.commit_applied(43).await.unwrap();
        assert_eq!(h.commits().await, vec![42, 43]);
    }

    #[tokio::test]
    async fn fail_next_poll_returns_err_once() {
        let (mut ing, h) = MockKafkaIngestor::new();
        h.push(0, b"x".to_vec()).await;
        h.fail_next_poll().await;
        let err = ing.poll(10).await.unwrap_err();
        assert!(err.to_string().contains("simulated"));
        let batch = ing.poll(10).await.unwrap();
        assert_eq!(batch.len(), 1, "next poll should succeed");
    }
}
