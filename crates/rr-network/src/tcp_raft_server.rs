//! TCP server for Raft AppendEntries/Vote with lightweight binary framing.

use std::io;
use std::sync::Arc;

use rr_core::types::GroupId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};


use crate::raft_service::RaftGroupLookup;

const FRAME_MAGIC: [u8; 4] = [0x52, 0x52, 0x41, 0x46]; // "RRAF"
const FRAME_VERSION: u8 = 0x01;
const FRAME_HEADER_LEN: usize = 26;

const MSG_AE_REQ: u8 = 0x01;
const MSG_AE_RESP: u8 = 0x02;
const MSG_VOTE_REQ: u8 = 0x03;
const MSG_VOTE_RESP: u8 = 0x04;

#[derive(Debug)]
struct DecodedFrame {
    msg_type: u8,
    request_id: u64,
    group_id: u64,
    payload: Vec<u8>,
}

fn encode_frame(msg_type: u8, request_id: u64, group_id: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    out.extend_from_slice(&FRAME_MAGIC);
    out.push(FRAME_VERSION);
    out.push(msg_type);
    out.extend_from_slice(&request_id.to_be_bytes());
    out.extend_from_slice(&group_id.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
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
pub struct TcpRaftServer {
    listener: TcpListener,
    groups: Arc<dyn RaftGroupLookup>,
}

impl TcpRaftServer {
    pub async fn bind(addr: &str, groups: Arc<dyn RaftGroupLookup>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, groups })
    }

    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn serve(self) -> io::Result<()> {
        loop {
            let (stream, peer_addr) = self.listener.accept().await?;
            let groups = Arc::clone(&self.groups);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, groups).await {
                    tracing::debug!(peer = %peer_addr, error = %e, "tcp raft connection closed");
                }
            });
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    groups: Arc<dyn RaftGroupLookup>,
) -> io::Result<()> {
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
            Ok(encode_frame(
                MSG_AE_RESP,
                frame.request_id,
                frame.group_id,
                &payload,
            ))
        }
        MSG_VOTE_REQ => {
            let payload = groups.vote(group_id, frame.payload).await?;
            Ok(encode_frame(
                MSG_VOTE_RESP,
                frame.request_id,
                frame.group_id,
                &payload,
            ))
        }
        _ => Err(tonic::Status::invalid_argument("unsupported tcp raft msg_type")),
    }
}
