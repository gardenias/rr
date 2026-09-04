//! Pluggable command-source abstraction.
//!
//! Implements [ADR
//! A-0001](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0001-command-ingestor-abstraction.md):
//! a [`CommandIngestor`] feeds commands into the leader of a Raft group
//! from an external source (Kafka topic, file, etc). Concrete
//! implementations live in their own crates (e.g. `rr-ingestor-kafka`)
//! to avoid polluting `rr-core` with transport dependencies.
//!
//! # Why two traits
//!
//! [`CommandIngestor`] uses an associated type [`CommandIngestor::Offset`]
//! so implementers get the typed offset that fits their backend
//! (Kafka's `i64`, a future Pulsar implementation's `MessageId`, …).
//! That gives users a clean API but is **not object-safe**: traits with
//! method-position associated types can't be `Box<dyn Trait>`'d.
//!
//! [`DynCommandIngestor`] is the framework-internal type-erased
//! companion: every offset operation goes through `Vec<u8>` (bincode
//! encoding/decoding of the implementer's `Offset`). It IS object-safe
//! and is the type the framework actually stores
//! (`Box<dyn DynCommandIngestor>`). A blanket impl wraps any
//! [`CommandIngestor`] into a `DynCommandIngestor` automatically.
//!
//! Cost: one bincode encode + decode per offset operation. This is
//! amortised across the cost of a Kafka poll or a network RPC, so
//! negligible.
//!
//! # Lifecycle
//!
//! - **On leader promotion**, the framework calls
//!   [`DynCommandIngestor::seek_bytes`] with the bytes recorded in the most
//!   recent snapshot's `IngestorState.last_applied_offset` (or `None`
//!   for a fresh group). The ingestor resumes from that point.
//! - **On step-down**, the framework drops the ingestor instance —
//!   implementations should keep `seek` cheap and `poll` cancel-safe.

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

use crate::error::RrError;

/// One command read from the source, ready to be proposed to Raft.
#[derive(Debug, Clone)]
pub struct IngressCommand<O> {
    /// Source-specific offset of this command. The framework persists
    /// the bincode-encoded form alongside each Raft log entry so it
    /// can resume on leader transition without re-applying.
    pub offset: O,
    /// Raw command bytes. The user `Service` decides what these mean.
    pub payload: Vec<u8>,
}

/// User-facing command source.
///
/// Implement this to expose a Kafka topic, file, or any other
/// command stream as input for an rr Raft group. Use `Offset = i64`
/// for Kafka, `Offset = String` for HTTP cursor APIs, etc.
///
/// **Safety**: implementations must be cancel-safe — the framework
/// drops the ingestor on leader step-down, which awaits any in-flight
/// `poll` future for `Drop`. `poll` should be a clean `tokio::select!`
/// over a stream + cancellation token, not a blocking syscall.
#[async_trait]
pub trait CommandIngestor: Send + Sync + 'static {
    /// Backend-specific offset type. Must round-trip through bincode
    /// because the framework persists offsets opaquely as bytes.
    type Offset: Serialize + DeserializeOwned + Ord + Copy + Debug + Send + Sync + 'static;

    /// Reposition the source so the next `poll` returns the command
    /// AFTER the supplied offset. `None` means "start from the
    /// beginning" (fresh group, no persisted offset).
    async fn seek(&mut self, offset: Option<Self::Offset>) -> Result<(), RrError>;

    /// Read the next batch of commands. Returns up to `max_batch_size`
    /// items; an empty vector signals "no commands available right
    /// now, retry later". Errors propagate to the framework's poll
    /// loop, which applies exponential backoff.
    async fn poll(
        &mut self,
        max_batch_size: usize,
    ) -> Result<Vec<IngressCommand<Self::Offset>>, RrError>;

    /// Hook called after the framework has successfully applied
    /// `offset` (and any earlier offsets) to the state machine.
    /// Implementations may use this to commit checkpoints to the
    /// upstream system (e.g. Kafka `__consumer_offsets`). The Raft log
    /// is the source of truth, so failures here are non-fatal — the
    /// framework will simply re-emit later.
    async fn commit_applied(&mut self, offset: Self::Offset) -> Result<(), RrError>;
}

/// Framework-internal type-erased ingestor.
///
/// All offset operations are bincode-encoded `Vec<u8>` so the framework
/// can store ingestors of different `Offset` types together
/// (`Box<dyn DynCommandIngestor>`) — `CommandIngestor` itself is not
/// object-safe.
///
/// **Don't implement this manually.** A blanket impl wraps any
/// [`CommandIngestor`] into a `DynCommandIngestor` for free.
#[async_trait]
pub trait DynCommandIngestor: Send + Sync {
    /// Reposition. `None` means "from beginning". Bytes are
    /// bincode-encoded `Self::Offset`.
    async fn seek_bytes(&mut self, offset: Option<&[u8]>) -> Result<(), RrError>;

    /// Read up to `max_batch_size` commands; offsets in the response
    /// are bincode-encoded.
    async fn poll_bytes(
        &mut self,
        max_batch_size: usize,
    ) -> Result<Vec<DynIngressCommand>, RrError>;

    /// Hook for upstream checkpoint commits. Bytes are bincode-encoded.
    async fn commit_applied_bytes(&mut self, offset: &[u8]) -> Result<(), RrError>;
}

/// Type-erased counterpart of [`IngressCommand`].
#[derive(Debug, Clone)]
pub struct DynIngressCommand {
    /// Bincode-encoded offset.
    pub offset: Vec<u8>,
    /// Raw command payload.
    pub payload: Vec<u8>,
}

#[async_trait]
impl<I: CommandIngestor> DynCommandIngestor for I {
    async fn seek_bytes(&mut self, offset: Option<&[u8]>) -> Result<(), RrError> {
        let typed: Option<I::Offset> = match offset {
            Some(bytes) => {
                let (decoded, _) =
                    bincode::serde::decode_from_slice(bytes, bincode::config::standard()).map_err(
                        |e| {
                            RrError::Codec(crate::error::CodecError::DeserializeFailed(format!(
                                "ingestor offset decode failed: {e}"
                            )))
                        },
                    )?;
                Some(decoded)
            }
            None => None,
        };
        CommandIngestor::seek(self, typed).await
    }

    async fn poll_bytes(
        &mut self,
        max_batch_size: usize,
    ) -> Result<Vec<DynIngressCommand>, RrError> {
        let batch = CommandIngestor::poll(self, max_batch_size).await?;
        let mut out = Vec::with_capacity(batch.len());
        for cmd in batch {
            let offset_bytes =
                bincode::serde::encode_to_vec(cmd.offset, bincode::config::standard()).map_err(
                    |e| {
                        RrError::Codec(crate::error::CodecError::SerializeFailed(format!(
                            "ingestor offset encode failed: {e}"
                        )))
                    },
                )?;
            out.push(DynIngressCommand {
                offset: offset_bytes,
                payload: cmd.payload,
            });
        }
        Ok(out)
    }

    async fn commit_applied_bytes(&mut self, offset: &[u8]) -> Result<(), RrError> {
        let (typed, _): (I::Offset, _) =
            bincode::serde::decode_from_slice(offset, bincode::config::standard()).map_err(
                |e| {
                    RrError::Codec(crate::error::CodecError::DeserializeFailed(format!(
                        "ingestor offset decode failed: {e}"
                    )))
                },
            )?;
        CommandIngestor::commit_applied(self, typed).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CountingIngestor {
        next_offset: u64,
        seek_history: Vec<Option<u64>>,
        commit_history: Vec<u64>,
    }

    #[async_trait]
    impl CommandIngestor for CountingIngestor {
        type Offset = u64;

        async fn seek(&mut self, offset: Option<u64>) -> Result<(), RrError> {
            self.seek_history.push(offset);
            self.next_offset = offset.map(|o| o + 1).unwrap_or(0);
            Ok(())
        }

        async fn poll(
            &mut self,
            max_batch_size: usize,
        ) -> Result<Vec<IngressCommand<u64>>, RrError> {
            let mut batch = Vec::new();
            for _ in 0..max_batch_size {
                batch.push(IngressCommand {
                    offset: self.next_offset,
                    payload: format!("cmd-{}", self.next_offset).into_bytes(),
                });
                self.next_offset += 1;
            }
            Ok(batch)
        }

        async fn commit_applied(&mut self, offset: u64) -> Result<(), RrError> {
            self.commit_history.push(offset);
            Ok(())
        }
    }

    #[tokio::test]
    async fn dyn_seek_decodes_typed_offset() {
        let mut ingestor: Box<dyn DynCommandIngestor> = Box::new(CountingIngestor {
            next_offset: 0,
            seek_history: Vec::new(),
            commit_history: Vec::new(),
        });
        let typed_offset: u64 = 42;
        let bytes =
            bincode::serde::encode_to_vec(typed_offset, bincode::config::standard()).unwrap();
        ingestor.seek_bytes(Some(&bytes)).await.unwrap();
        let next = ingestor.poll_bytes(1).await.unwrap();
        let (decoded, _): (u64, _) =
            bincode::serde::decode_from_slice(&next[0].offset, bincode::config::standard())
                .unwrap();
        assert_eq!(decoded, 43);
    }

    #[tokio::test]
    async fn dyn_seek_with_none_starts_from_beginning() {
        let mut ingestor: Box<dyn DynCommandIngestor> = Box::new(CountingIngestor {
            next_offset: 1000,
            seek_history: Vec::new(),
            commit_history: Vec::new(),
        });
        ingestor.seek_bytes(None).await.unwrap();
        let next = ingestor.poll_bytes(1).await.unwrap();
        let (decoded, _): (u64, _) =
            bincode::serde::decode_from_slice(&next[0].offset, bincode::config::standard())
                .unwrap();
        assert_eq!(decoded, 0);
    }

    #[tokio::test]
    async fn dyn_poll_round_trips_offsets() {
        let mut ingestor: Box<dyn DynCommandIngestor> = Box::new(CountingIngestor {
            next_offset: 100,
            seek_history: Vec::new(),
            commit_history: Vec::new(),
        });
        let batch = ingestor.poll_bytes(3).await.unwrap();
        assert_eq!(batch.len(), 3);
        for (i, cmd) in batch.iter().enumerate() {
            let (decoded, _): (u64, _) =
                bincode::serde::decode_from_slice(&cmd.offset, bincode::config::standard())
                    .unwrap();
            assert_eq!(decoded, 100 + i as u64);
            assert_eq!(cmd.payload, format!("cmd-{}", 100 + i).into_bytes());
        }
    }

    #[tokio::test]
    async fn dyn_commit_applied_decodes_offset() {
        let mut ingestor: Box<dyn DynCommandIngestor> = Box::new(CountingIngestor {
            next_offset: 0,
            seek_history: Vec::new(),
            commit_history: Vec::new(),
        });
        let typed_offset: u64 = 999;
        let bytes =
            bincode::serde::encode_to_vec(typed_offset, bincode::config::standard()).unwrap();
        ingestor.commit_applied_bytes(&bytes).await.unwrap();
    }

    #[tokio::test]
    async fn dyn_seek_rejects_invalid_offset_bytes() {
        let mut ingestor: Box<dyn DynCommandIngestor> = Box::new(CountingIngestor {
            next_offset: 0,
            seek_history: Vec::new(),
            commit_history: Vec::new(),
        });
        // bincode varint marker 251 declares the next 2 bytes are a u16
        // length prefix; supplying only 1 trailing byte truncates the
        // payload mid-prefix so decode unambiguously fails.
        let truncated_varint = [251u8, 0u8];
        let result = ingestor.seek_bytes(Some(&truncated_varint)).await;
        assert!(
            result.is_err(),
            "decode of truncated varint should fail; got Ok"
        );
    }
}
