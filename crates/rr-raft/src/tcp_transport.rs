//! Lightweight binary TCP transport for Raft AppendEntries/Vote.
//!
//! `InstallSnapshot` intentionally keeps using gRPC via `GrpcRaftNetwork`
//! fallback to avoid re-implementing streaming snapshot transfer.

use std::io;
use std::io::BufReader;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::BasicNode;
use rr_core::types::NodeId;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::network_factory::{ChannelPoolLike, GrpcNetworkFactory, GrpcRaftNetwork, TlsPaths};
use crate::type_config::RrTypeConfig;

const FRAME_MAGIC: [u8; 4] = [0x52, 0x52, 0x41, 0x46]; // "RRAF"
const FRAME_VERSION: u8 = 0x01;
const FRAME_HEADER_LEN: usize = 26;

const MSG_AE_REQ: u8 = 0x01;
const MSG_AE_RESP: u8 = 0x02;
const MSG_VOTE_REQ: u8 = 0x03;
const MSG_VOTE_RESP: u8 = 0x04;

fn to_unreachable<E: std::fmt::Display>(e: E) -> RPCError<RrTypeConfig> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other(e.to_string())))
}

#[derive(Debug)]
struct DecodedFrame {
    msg_type: u8,
    request_id: u64,
    _group_id: u64,
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
    R: AsyncRead + Unpin,
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

    let msg_type = header[5];
    let request_id = u64::from_be_bytes(header[6..14].try_into().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "invalid request_id field")
    })?);
    let group_id = u64::from_be_bytes(
        header[14..22]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid group_id field"))?,
    );
    let payload_len = u32::from_be_bytes(
        header[22..26]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid payload_len field"))?,
    ) as usize;

    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await?;
    }

    Ok(DecodedFrame {
        msg_type,
        request_id,
        _group_id: group_id,
        payload,
    })
}

#[derive(Debug)]
struct TcpResponseFrame {
    msg_type: u8,
    payload: Vec<u8>,
}

trait AsyncIo: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncIo for T {}

type BoxedIo = Box<dyn AsyncIo + Send + Unpin>;

/// Single TCP connection with background reader/writer tasks.
pub struct TcpConn {
    writer_tx: mpsc::Sender<(u64, Vec<u8>)>,
    pending: Arc<DashMap<u64, oneshot::Sender<TcpResponseFrame>>>,
    _reader: JoinHandle<()>,
    _writer: JoinHandle<()>,
}

impl TcpConn {
    async fn connect(
        addr: &str,
        tls: Option<Arc<TlsPaths>>,
        on_disconnect: Arc<dyn Fn() + Send + Sync>,
    ) -> io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr).await?;
        tcp_stream.set_nodelay(true)?;

        let io: BoxedIo = if let Some(tls_paths) = tls {
            let config = build_tcp_client_tls_config(&tls_paths)?;
            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
            let server_name = derive_server_name(addr, &tls_paths)?;
            let tls_stream = connector.connect(server_name, tcp_stream).await.map_err(|e| {
                io::Error::new(io::ErrorKind::ConnectionAborted, format!("tls connect: {e}"))
            })?;
            Box::new(tls_stream)
        } else {
            Box::new(tcp_stream)
        };

        let (mut reader, mut writer) = tokio::io::split(io);
        let (writer_tx, mut writer_rx) = mpsc::channel::<(u64, Vec<u8>)>(256);
        let pending: Arc<DashMap<u64, oneshot::Sender<TcpResponseFrame>>> =
            Arc::new(DashMap::new());

        let pending_for_reader = Arc::clone(&pending);
        let on_disconnect_reader = Arc::clone(&on_disconnect);
        let reader_task = tokio::spawn(async move {
            loop {
                match read_frame(&mut reader).await {
                    Ok(frame) => {
                        if let Some((_, tx)) = pending_for_reader.remove(&frame.request_id) {
                            let _ = tx.send(TcpResponseFrame {
                                msg_type: frame.msg_type,
                                payload: frame.payload,
                            });
                        }
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "tcp raft reader loop ended");
                        let keys: Vec<u64> = pending_for_reader.iter().map(|e| *e.key()).collect();
                        for key in keys {
                            pending_for_reader.remove(&key);
                        }
                        (on_disconnect_reader)();
                        break;
                    }
                }
            }
        });

        let pending_for_writer = Arc::clone(&pending);
        let on_disconnect_writer = Arc::clone(&on_disconnect);
        let writer_task = tokio::spawn(async move {
            while let Some((request_id, frame_bytes)) = writer_rx.recv().await {
                if let Err(e) = writer.write_all(&frame_bytes).await {
                    tracing::debug!(error = %e, request_id, "tcp raft writer write failed");
                    let keys: Vec<u64> = pending_for_writer.iter().map(|e| *e.key()).collect();
                    for key in keys {
                        pending_for_writer.remove(&key);
                    }
                    (on_disconnect_writer)();
                    break;
                }
            }
        });

        Ok(Self {
            writer_tx,
            pending,
            _reader: reader_task,
            _writer: writer_task,
        })
    }

    async fn request(&self, request_id: u64, frame_bytes: Vec<u8>) -> io::Result<TcpResponseFrame> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert(request_id, tx);

        if let Err(e) = self.writer_tx.send((request_id, frame_bytes)).await {
            self.pending.remove(&request_id);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("tcp writer channel closed: {e}"),
            ));
        }

        rx.await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                format!("tcp response wait failed: {e}"),
            )
        })
    }
}

/// Shared TCP connection pool across Raft groups.
#[derive(Clone)]
pub struct TcpConnectionPool {
    conns: Arc<DashMap<(NodeId, u8), Arc<TcpConn>>>,
    max_per_node: u8,
}

impl Default for TcpConnectionPool {
    fn default() -> Self {
        Self::new(8)
    }
}

impl TcpConnectionPool {
    pub fn new(max_per_node: u8) -> Self {
        Self {
            conns: Arc::new(DashMap::new()),
            max_per_node: max_per_node.max(1),
        }
    }

    pub fn max_per_node(&self) -> u8 {
        self.max_per_node
    }

    pub fn remove_conn(&self, node_id: NodeId, slot: u8) {
        self.conns.remove(&(node_id, slot));
    }

    async fn get_or_connect(
        &self,
        node_id: NodeId,
        slot: u8,
        addr: &str,
        tls: Option<Arc<TlsPaths>>,
    ) -> io::Result<Arc<TcpConn>> {
        if let Some(existing) = self.conns.get(&(node_id, slot)) {
            return Ok(existing.clone());
        }

        let pool = self.clone();
        let on_disconnect = Arc::new(move || {
            pool.remove_conn(node_id, slot);
        });

        let conn = Arc::new(TcpConn::connect(addr, tls, on_disconnect).await?);
        let entry = self
            .conns
            .entry((node_id, slot))
            .or_insert_with(|| Arc::clone(&conn));
        Ok(entry.clone())
    }
}

/// Factory creating TCP-backed Raft networks for one group.
#[derive(Clone)]
pub struct TcpRaftNetworkFactory {
    tcp_pool: Arc<TcpConnectionPool>,
    grpc_channel_pool: Arc<dyn ChannelPoolLike>,
    group_id: u64,
    port_offset: u16,
    tls: Option<TlsPaths>,
}

impl TcpRaftNetworkFactory {
    pub fn new(
        tcp_pool: Arc<TcpConnectionPool>,
        grpc_channel_pool: Arc<dyn ChannelPoolLike>,
        group_id: u64,
        port_offset: u16,
        tls: Option<TlsPaths>,
    ) -> Self {
        Self {
            tcp_pool,
            grpc_channel_pool,
            group_id,
            port_offset,
            tls,
        }
    }
}

impl RaftNetworkFactory<RrTypeConfig> for TcpRaftNetworkFactory {
    type Network = TcpRaftNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let target_tcp_addr = derive_tcp_addr(&node.addr, self.port_offset).unwrap_or_else(|e| {
            tracing::warn!(
                target,
                grpc_addr = %node.addr,
                port_offset = self.port_offset,
                error = %e,
                "failed to derive tcp raft addr; fallback to grpc addr"
            );
            node.addr.clone()
        });

        let mut grpc_factory =
            GrpcNetworkFactory::new(Arc::clone(&self.grpc_channel_pool), self.group_id);
        let grpc = grpc_factory.new_client(target, node).await;

        TcpRaftNetwork {
            target,
            target_addr: node.addr.clone(),
            target_tcp_addr,
            group_id: self.group_id,
            tcp_pool: Arc::clone(&self.tcp_pool),
            tls: self.tls.clone().map(Arc::new),
            request_seq: Arc::new(AtomicU64::new(1)),
            grpc,
        }
    }
}

/// TCP-backed Raft network (AppendEntries/Vote over TCP, Snapshot over gRPC).
pub struct TcpRaftNetwork {
    target: u64,
    target_addr: String,
    target_tcp_addr: String,
    group_id: u64,
    tcp_pool: Arc<TcpConnectionPool>,
    tls: Option<Arc<TlsPaths>>,
    request_seq: Arc<AtomicU64>,
    grpc: GrpcRaftNetwork,
}

impl TcpRaftNetwork {
    async fn rpc_roundtrip(
        &self,
        req_type: u8,
        resp_type: u8,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RPCError<RrTypeConfig>> {
        let request_id = self.request_seq.fetch_add(1, Ordering::Relaxed);
        let slot = (request_id % self.tcp_pool.max_per_node() as u64) as u8;

        let conn = self
            .tcp_pool
            .get_or_connect(self.target, slot, &self.target_tcp_addr, self.tls.clone())
            .await
            .map_err(to_unreachable)?;

        let frame = encode_frame(req_type, request_id, self.group_id, &payload);
        let response = conn.request(request_id, frame).await.map_err(|e| {
            self.tcp_pool.remove_conn(self.target, slot);
            to_unreachable(e)
        })?;

        if response.msg_type != resp_type {
            return Err(to_unreachable(format!(
                "unexpected tcp raft response type: expected={resp_type:#x}, got={:#x}",
                response.msg_type
            )));
        }

        Ok(response.payload)
    }
}

impl RaftNetworkV2<RrTypeConfig> for TcpRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RrTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<RrTypeConfig>, RPCError<RrTypeConfig>> {
        let payload = bincode::serde::encode_to_vec(&rpc, bincode::config::standard())
            .map_err(to_unreachable)?;
        let resp_payload = self
            .rpc_roundtrip(MSG_AE_REQ, MSG_AE_RESP, payload)
            .await
            .map_err(|e| {
                tracing::debug!(
                    target = self.target,
                    grpc_addr = %self.target_addr,
                    tcp_addr = %self.target_tcp_addr,
                    error = %e,
                    "tcp append_entries failed"
                );
                e
            })?;

        let (resp, _): (AppendEntriesResponse<RrTypeConfig>, _) =
            bincode::serde::decode_from_slice(&resp_payload, bincode::config::standard())
                .map_err(to_unreachable)?;
        Ok(resp)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RrTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RrTypeConfig>, RPCError<RrTypeConfig>> {
        let payload = bincode::serde::encode_to_vec(&rpc, bincode::config::standard())
            .map_err(to_unreachable)?;
        let resp_payload = self
            .rpc_roundtrip(MSG_VOTE_REQ, MSG_VOTE_RESP, payload)
            .await
            .map_err(|e| {
                tracing::debug!(
                    target = self.target,
                    grpc_addr = %self.target_addr,
                    tcp_addr = %self.target_tcp_addr,
                    error = %e,
                    "tcp vote failed"
                );
                e
            })?;

        let (resp, _): (VoteResponse<RrTypeConfig>, _) =
            bincode::serde::decode_from_slice(&resp_payload, bincode::config::standard())
                .map_err(to_unreachable)?;
        Ok(resp)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<RrTypeConfig>,
        snapshot: SnapshotOf<RrTypeConfig>,
        cancel: impl std::future::Future<Output = ReplicationClosed>
            + openraft::OptionalSend
            + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<RrTypeConfig>, StreamingError<RrTypeConfig>> {
        self.grpc.full_snapshot(vote, snapshot, cancel, option).await
    }
}

fn derive_tcp_addr(grpc_addr: &str, port_offset: u16) -> io::Result<String> {
    let (host, grpc_port) = split_host_port(grpc_addr)?;
    let tcp_port = grpc_port
        .checked_add(port_offset)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "port overflow"))?;
    Ok(format!("{host}:{tcp_port}"))
}

fn split_host_port(addr: &str) -> io::Result<(String, u16)> {
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "address missing port"))?;
    let port = port.parse::<u16>().map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("invalid port in addr: {e}"))
    })?;
    Ok((host.to_string(), port))
}

fn derive_server_name(
    addr: &str,
    tls: &TlsPaths,
) -> io::Result<rustls::pki_types::ServerName<'static>> {
    let domain = if let Some(domain) = &tls.domain_name {
        domain.clone()
    } else {
        let (host, _) = split_host_port(addr)?;
        host.trim_start_matches('[')
            .trim_end_matches(']')
            .to_string()
    };
    rustls::pki_types::ServerName::try_from(domain)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid SNI: {e}")))
}

fn build_tcp_client_tls_config(tls: &TlsPaths) -> io::Result<rustls::ClientConfig> {
    let cert_chain = load_cert_chain(&tls.cert_path)?;
    let key = load_private_key(&tls.key_path)?;

    let mut roots = rustls::RootCertStore::empty();
    if let Some(ca) = &tls.ca_path {
        let ca_certs = load_cert_chain(ca)?;
        let (_added, rejected) = roots.add_parsable_certificates(ca_certs);
        if rejected > 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "failed to parse one or more CA certs",
            ));
        }
    }

    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(cert_chain, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls config: {e}")))
}

fn load_cert_chain(path: &Path) -> io::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader).collect()
}

fn load_private_key(path: &Path) -> io::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key found in {}", path.display()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_roundtrip_works() {
        let payload = b"hello".to_vec();
        let bytes = encode_frame(MSG_AE_REQ, 7, 9, &payload);

        let (mut a, mut b) = tokio::io::duplex(1024);
        let send = tokio::spawn(async move { a.write_all(&bytes).await.unwrap() });
        let recv = tokio::spawn(async move { read_frame(&mut b).await.unwrap() });

        send.await.unwrap();
        let frame = recv.await.unwrap();
        assert_eq!(frame.msg_type, MSG_AE_REQ);
        assert_eq!(frame.request_id, 7);
        assert_eq!(frame._group_id, 9);
        assert_eq!(frame.payload, payload);
    }

    #[test]
    fn derive_tcp_addr_adds_offset() {
        let addr = derive_tcp_addr("127.0.0.1:51001", 4).unwrap();
        assert_eq!(addr, "127.0.0.1:51005");
    }
}
