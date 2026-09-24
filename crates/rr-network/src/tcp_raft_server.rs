//! TCP server for Raft AppendEntries/Vote with lightweight binary framing.
//!
//! # Security
//!
//! The `RRAF` frame protocol is **plaintext, with no TLS, no peer
//! authentication, and no rate limiting** — any client that can reach the
//! port can inject Vote/AppendEntries frames and interfere with leader
//! election and replication. This transport is meant for development and
//! trusted networks only.
//!
//! Two bounded-resource mitigations apply regardless:
//!
//! - the wire-controlled frame `payload_len` is validated against
//!   [`RAFT_MAX_DECODING_MESSAGE_SIZE`] **before** any byte is read or
//!   allocated, and the offending connection is closed with a WARN;
//! - per-connection tasks live in a [`JoinSet`] owned by `serve`, so
//!   server shutdown aborts every open connection instead of leaking it.
//!
//! In the standard `rr-server` path (`NodeConfig::raft_transport =
//! RaftTransport::Tcp`) startup gates G6/G7 (ADR A-0014) require an explicit
//! `allow_insecure` opt-in and force the bind address to be loopback.
//! Library users who bind [`TcpRaftServer`] directly bypass those gates:
//! **never bind it to a non-loopback address.**

use std::io;
use std::sync::Arc;

use rr_core::types::GroupId;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::raft_service::RaftGroupLookup;
use crate::RAFT_MAX_DECODING_MESSAGE_SIZE;

const FRAME_MAGIC: [u8; 4] = [0x52, 0x52, 0x41, 0x46]; // "RRAF"
const FRAME_VERSION: u8 = 0x01;
const FRAME_HEADER_LEN: usize = 26;

const MSG_AE_REQ: u8 = 0x01;
const MSG_AE_RESP: u8 = 0x02;
const MSG_VOTE_REQ: u8 = 0x03;
const MSG_VOTE_RESP: u8 = 0x04;

/// A frame whose declared `payload_len` exceeds
/// [`RAFT_MAX_DECODING_MESSAGE_SIZE`]. Carried as the inner error of an
/// [`io::Error`] so the per-connection task can downgrade ordinary
/// disconnects to DEBUG but log this (likely hostile or corrupted peer)
/// at WARN.
#[derive(Debug)]
struct FrameTooLarge {
    declared_len: usize,
    cap: usize,
}

impl std::fmt::Display for FrameTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "frame payload_len {} exceeds cap {} (closing connection)",
            self.declared_len, self.cap
        )
    }
}

impl std::error::Error for FrameTooLarge {}

fn is_frame_too_large(e: &io::Error) -> bool {
    e.get_ref()
        .and_then(|inner| inner.downcast_ref::<FrameTooLarge>())
        .is_some()
}

#[derive(Debug)]
struct DecodedFrame {
    msg_type: u8,
    request_id: u64,
    group_id: u64,
    payload: Vec<u8>,
}

/// Fit a payload length into the frame's u32 length field. Returns an
/// `InvalidInput` error instead of truncating via `as u32`.
fn frame_payload_len(payload_len: usize) -> io::Result<u32> {
    u32::try_from(payload_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("payload of {payload_len} bytes exceeds the u32 frame length field"),
        )
    })
}

fn encode_frame(
    msg_type: u8,
    request_id: u64,
    group_id: u64,
    payload: &[u8],
) -> io::Result<Vec<u8>> {
    let payload_len = frame_payload_len(payload.len())?;
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    out.extend_from_slice(&FRAME_MAGIC);
    out.push(FRAME_VERSION);
    out.push(msg_type);
    out.extend_from_slice(&request_id.to_be_bytes());
    out.extend_from_slice(&group_id.to_be_bytes());
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

async fn read_frame<R>(reader: &mut R) -> io::Result<DecodedFrame>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut header = [0u8; FRAME_HEADER_LEN];
    reader.read_exact(&mut header).await?;

    if header[0..4] != FRAME_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid frame magic",
        ));
    }
    if header[4] != FRAME_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported frame version: {}", header[4]),
        ));
    }

    let request_id = u64::from_be_bytes(
        header[6..14]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid request_id"))?,
    );
    let group_id = u64::from_be_bytes(
        header[14..22]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid group_id"))?,
    );
    let payload_len = u32::from_be_bytes(
        header[22..26]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid payload_len"))?,
    ) as usize;

    // The length prefix is wire-controlled: validate against the cap
    // BEFORE allocating or reading a single payload byte.
    if payload_len > RAFT_MAX_DECODING_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            FrameTooLarge {
                declared_len: payload_len,
                cap: RAFT_MAX_DECODING_MESSAGE_SIZE,
            },
        ));
    }

    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await?;
    }

    Ok(DecodedFrame {
        msg_type: header[5],
        request_id,
        group_id,
        payload,
    })
}

/// Server-side TCP listener for Raft append/vote traffic.
///
/// # Security
///
/// Plaintext framing, no TLS, no peer authentication (see the module-level
/// docs). Bind loopback only; the `rr-server` startup gates G6/G7
/// (ADR A-0014) enforce this for the standard `RaftTransport::Tcp` path,
/// but a direct [`TcpRaftServer::bind`] bypasses them.
pub struct TcpRaftServer {
    listener: TcpListener,
    groups: Arc<dyn RaftGroupLookup>,
}

impl TcpRaftServer {
    /// Bind the listener. `addr` must be a loopback address — this
    /// transport has no TLS and no peer authentication (ADR A-0014 G7).
    pub async fn bind(addr: &str, groups: Arc<dyn RaftGroupLookup>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, groups })
    }

    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn serve(self) -> io::Result<()> {
        // Every per-connection task is tracked in this JoinSet. Dropping
        // the JoinSet aborts all live tasks, so shutdown — the embedding
        // binary aborts this `serve` task on SIGTERM — cannot leak
        // connections, and neither can an `accept` error return.
        let mut connections: JoinSet<()> = JoinSet::new();
        loop {
            let (stream, peer_addr) = self.listener.accept().await?;
            let groups = Arc::clone(&self.groups);
            connections.spawn(async move {
                if let Err(e) = handle_connection(stream, groups).await {
                    if is_frame_too_large(&e) {
                        tracing::warn!(peer = %peer_addr, error = %e, "tcp raft connection closed (frame cap)");
                    } else {
                        tracing::debug!(peer = %peer_addr, error = %e, "tcp raft connection closed");
                    }
                }
            });
            // Reap finished tasks so the JoinSet stays bounded under
            // connection churn.
            while let Some(outcome) = connections.try_join_next() {
                if let Err(e) = outcome {
                    tracing::warn!(error = %e, "tcp raft connection task failed");
                }
            }
        }
    }
}

async fn handle_connection(stream: TcpStream, groups: Arc<dyn RaftGroupLookup>) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let (mut reader, mut writer) = stream.into_split();

    loop {
        let frame = read_frame(&mut reader).await?;
        match dispatch_frame(Arc::clone(&groups), frame).await {
            Ok(resp_bytes) => {
                writer.write_all(&resp_bytes).await?;
                writer.flush().await?;
            }
            Err(status) => {
                return Err(io::Error::other(format!(
                    "tcp raft dispatch error: [{:?}] {}",
                    status.code(),
                    status.message()
                )));
            }
        }
    }
}

async fn dispatch_frame(
    groups: Arc<dyn RaftGroupLookup>,
    frame: DecodedFrame,
) -> Result<Vec<u8>, tonic::Status> {
    let group_id: GroupId = frame.group_id;
    match frame.msg_type {
        MSG_AE_REQ => {
            let payload = groups.append_entries(group_id, frame.payload).await?;
            encode_frame(MSG_AE_RESP, frame.request_id, frame.group_id, &payload).map_err(|e| {
                tracing::error!(group_id, error = %e, "tcp raft: append_entries response encode failed");
                tonic::Status::internal("response encode failed")
            })
        }
        MSG_VOTE_REQ => {
            let payload = groups.vote(group_id, frame.payload).await?;
            encode_frame(MSG_VOTE_RESP, frame.request_id, frame.group_id, &payload).map_err(|e| {
                tracing::error!(group_id, error = %e, "tcp raft: vote response encode failed");
                tonic::Status::internal("response encode failed")
            })
        }
        _ => Err(tonic::Status::invalid_argument(
            "unsupported tcp raft msg_type",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn frame_header(msg_type: u8, request_id: u64, group_id: u64, payload_len: u32) -> Vec<u8> {
        let mut header = Vec::with_capacity(FRAME_HEADER_LEN);
        header.extend_from_slice(&FRAME_MAGIC);
        header.push(FRAME_VERSION);
        header.push(msg_type);
        header.extend_from_slice(&request_id.to_be_bytes());
        header.extend_from_slice(&group_id.to_be_bytes());
        header.extend_from_slice(&payload_len.to_be_bytes());
        header
    }

    #[tokio::test]
    async fn read_frame_roundtrips_encoded_frame() {
        let payload = b"hello-raft".to_vec();
        let bytes = encode_frame(MSG_AE_REQ, 42, 7, &payload).unwrap();
        let (mut tx, mut rx) = tokio::io::duplex(4096);
        tx.write_all(&bytes).await.unwrap();
        drop(tx);

        let frame = read_frame(&mut rx).await.unwrap();
        assert_eq!(frame.msg_type, MSG_AE_REQ);
        assert_eq!(frame.request_id, 42);
        assert_eq!(frame.group_id, 7);
        assert_eq!(frame.payload, payload);
    }

    #[tokio::test]
    async fn read_frame_rejects_payload_len_above_cap_before_reading() {
        // Regression: the declared length is wire-controlled; an unbounded
        // `vec![0u8; payload_len]` lets any peer force a 4 GiB allocation
        // without sending a single payload byte. The cap must trip first.
        let over_cap = RAFT_MAX_DECODING_MESSAGE_SIZE as u32 + 1;
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&frame_header(MSG_AE_REQ, 1, 7, over_cap))
            .await
            .unwrap();
        drop(tx);

        let err = read_frame(&mut rx).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            is_frame_too_large(&err),
            "expected typed FrameTooLarge payload, got: {err}"
        );
    }

    #[tokio::test]
    async fn read_frame_accepts_payload_len_at_cap_boundary() {
        // A declared len exactly at the cap passes validation; the read
        // itself then fails on EOF (no payload sent) — the point of the
        // test is that the cap check does not fire at the boundary.
        let at_cap = RAFT_MAX_DECODING_MESSAGE_SIZE as u32;
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&frame_header(MSG_VOTE_REQ, 1, 7, at_cap))
            .await
            .unwrap();
        drop(tx);

        let err = read_frame(&mut rx).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(!is_frame_too_large(&err));
    }

    #[test]
    fn encode_frame_rejects_payload_exceeding_u32_length_field() {
        // Regression: `payload.len() as u32` silently truncated; a 4 GiB+1
        // response would announce a 1-byte payload and corrupt the stream.
        assert!(frame_payload_len(usize::try_from(u32::MAX).unwrap()).is_ok());
        let err = frame_payload_len(usize::try_from(u32::MAX).unwrap() + 1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn frame_too_large_display_names_len_and_cap() {
        let err = FrameTooLarge {
            declared_len: 42,
            cap: 10,
        };
        let msg = err.to_string();
        assert!(msg.contains("42") && msg.contains("10"), "got: {msg}");
    }
}
