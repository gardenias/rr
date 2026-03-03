//! KV Store demo — single-node `rr` distributed framework example.
//!
//! Demonstrates:
//! 1. Building a node using `RrNode` with `MemStorage` + `MemConfigProvider`.
//! 2. Wrapping `KvService` in `ServiceStateMachineAdapter` as the state machine.
//! 3. Starting the node, initializing a single-member Raft group, proposing
//!    Put/Get/Delete operations, and shutting down gracefully.

use std::sync::Arc;

use anyhow::Result;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::storage::StorageEngine;
use rr_core::types::PartitionConfig;
use rr_raft::service_adapter::ServiceStateMachineAdapter;
use rr_server::config::NodeConfig;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use kv_store::service::KvService;
use kv_store::types::{KvRequest, KvResponse};

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // ── 1. Initialise tracing ─────────────────────────────────────────────────
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .try_init();

    info!("=== rr KV-Store demo starting ===");

    // ── 2. Build config provider ──────────────────────────────────────────────
    //
    // Partition 1  →  group 1  →  replica [node 1]
    let config_provider = MemConfigProvider::new();
    config_provider
        .add_partition(1, PartitionConfig::SingleId(1), 1, vec![1])
        .await;
    let config_provider: Arc<dyn ConfigProvider> = Arc::new(config_provider);

    // ── 3. Build storage engine ───────────────────────────────────────────────
    let storage_engine: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());

    // ── 4. Build state machine factory ───────────────────────────────────────
    //
    // Each Raft group gets its own `KvService` wrapped in
    // `ServiceStateMachineAdapter`.
    let sm_factory = Box::new(|_group_id| -> Box<dyn rr_core::state_machine::StateMachine> {
        Box::new(ServiceStateMachineAdapter::new(KvService::new()))
    });

    // ── 5. Build NodeConfig and RrNode ────────────────────────────────────────
    let config = NodeConfig::new(
        1,                          // node_id
        "127.0.0.1:0".to_string(), // port 0 = OS picks one (no gRPC in this demo)
        storage_engine,
        config_provider,
        sm_factory,
    );

    let node = RrNode::new(config)?;

    // ── 6. Start the node ─────────────────────────────────────────────────────
    //
    // `start()` reads the routing table and spawns Raft group 1.
    node.start().await?;
    info!("Node started — group count: {}", node.group_manager().group_count().await);

    // ── 7. Initialize the single-member Raft group ────────────────────────────
    //
    // In a single-node cluster the member must initialize itself as the sole
    // voter so that it immediately becomes leader.
    let group_id: u64 = 1;
    let raft = node
        .group_manager()
        .get_raft(group_id)
        .await
        .expect("group 1 must exist after start()");

    use openraft::BasicNode;
    use std::collections::BTreeMap;

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());

    raft.initialize(members).await.map_err(|e| anyhow::anyhow!("Raft init failed: {e}"))?;
    info!("Raft group {group_id} initialized");

    // Wait a moment for leader election (single-node, should be instant)
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // ── 8. Propose operations via client request bytes ────────────────────────
    //
    // We encode a `KvRequest` with bincode, propose it through Raft, and
    // decode the `KvResponse` from the response bytes.

    demo_put(&raft, "hello", b"world").await?;
    demo_get(&raft, "hello").await?;
    demo_put(&raft, "counter", b"1").await?;
    demo_delete(&raft, "hello").await?;
    demo_get(&raft, "hello").await?; // should be None after delete
    demo_get(&raft, "counter").await?;

    // ── 9. Graceful shutdown ──────────────────────────────────────────────────
    node.shutdown().await?;
    info!("=== rr KV-Store demo complete ===");

    Ok(())
}

// ── Demo helpers ──────────────────────────────────────────────────────────────

type RaftInstance = openraft::Raft<rr_raft::type_config::RrTypeConfig>;

async fn propose(
    raft: &RaftInstance,
    req: &KvRequest,
) -> Result<KvResponse> {
    use bincode::config::standard;

    // Encode request bytes
    let payload = bincode::serde::encode_to_vec(req, standard())
        .map_err(|e| anyhow::anyhow!("encode failed: {e}"))?;

    // Propose through Raft — blocks until committed + applied
    let response = raft
        .client_write(payload)
        .await
        .map_err(|e| anyhow::anyhow!("client_write failed: {e}"))?;

    // response.data is the response bytes from the state machine
    let resp_bytes = &response.data;

    // Decode response
    let (resp, _): (KvResponse, _) =
        bincode::serde::decode_from_slice(resp_bytes, standard())
            .map_err(|e| anyhow::anyhow!("decode failed: {e}"))?;

    Ok(resp)
}

async fn demo_put(raft: &RaftInstance, key: &str, value: &[u8]) -> Result<()> {
    let req = KvRequest::Put { key: key.to_string(), value: value.to_vec() };
    propose(raft, &req).await?;
    info!("PUT {key} = {:?}", String::from_utf8_lossy(value));
    Ok(())
}

async fn demo_get(raft: &RaftInstance, key: &str) -> Result<()> {
    let req = KvRequest::Get { key: key.to_string() };
    let resp = propose(raft, &req).await?;
    match &resp.value {
        Some(v) => info!("GET {key} = {:?}", String::from_utf8_lossy(v)),
        None => info!("GET {key} = <not found>"),
    }
    Ok(())
}

async fn demo_delete(raft: &RaftInstance, key: &str) -> Result<()> {
    let req = KvRequest::Delete { key: key.to_string() };
    propose(raft, &req).await?;
    info!("DEL {key}");
    Ok(())
}
