//! Client-write batching end-to-end (opt-in `RrClientBuilder::with_batching`).
//!
//! Exercises the full path: rr-client batcher → one gRPC ClientRequest per
//! flush → dispatch → one Raft entry → apply-side split (rr_core::batch) →
//! per-item echo responses resolved positionally.
//!
//! The cluster here is built on `ServiceStateMachineAdapter` (the canonical
//! Service path) rather than the harness's plain `EchoSM`: the apply-side
//! batch split lives in the Service adapter, and a plain echo state machine
//! would "pass" by echoing the unsplit frame — a false positive.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use rr_client::{BatchConfig, RetryConfig, RrClientBuilder};
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::error::RrError;
use rr_core::service::Service;
use rr_core::storage::StorageEngine;
use rr_core::types::{NodeAddr, NodeId, PartitionConfig};
use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};
use rr_raft::network_factory::{ChannelPool, ChannelPoolLike};
use rr_raft::service_adapter::ServiceStateMachineAdapter;
use rr_router::router::ShardRouter;
use rr_server::config::NodeConfig;
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;

/// Echo service: the response is the request bytes — per item, so a
/// batch-frame apply proves the split by returning distinguishable echoes.
struct EchoService;

#[async_trait::async_trait]
impl Service for EchoService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        Ok(request)
    }
    fn name(&self) -> &str {
        "batch-e2e-echo"
    }
    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        Ok(vec![])
    }
    async fn restore(&mut self, _data: &[u8]) -> Result<(), RrError> {
        Ok(())
    }
}

fn service_factory() -> StateMachineFactory {
    Box::new(|group_id| Box::new(ServiceStateMachineAdapter::new(group_id, EchoService)))
}

/// Three real-gRPC nodes with group 1 spanning all of them — mirrors
/// `ClusterBuilder::build_real_network` but wires the Service adapter.
async fn build_service_cluster() -> (Vec<Arc<RrNode>>, Arc<MemConfigProvider>) {
    let provider = Arc::new(MemConfigProvider::new());
    let node_ids: Vec<NodeId> = vec![1, 2, 3];

    provider
        .add_partition(1, PartitionConfig::SingleId(1), 1, node_ids.clone())
        .await;

    let mut node_addrs: Vec<(NodeId, String)> = Vec::new();
    for &node_id in &node_ids {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // release port; small TOCTOU window is acceptable in tests
        let addr = format!("127.0.0.1:{port}");
        provider
            .add_node(node_id, NodeAddr { addr: addr.clone() })
            .await;
        node_addrs.push((node_id, addr));
    }

    let pool: Arc<dyn ChannelPoolLike> = Arc::new(ChannelPool::new());
    let mut nodes: Vec<Arc<RrNode>> = Vec::new();
    for (node_id, addr) in &node_addrs {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider_dyn: Arc<dyn ConfigProvider> = Arc::clone(&provider) as _;
        let mgr = Arc::new(RaftGroupManager::new_with_network(
            *node_id,
            service_factory(),
            Arc::clone(&pool),
        ));
        let router = Arc::new(ShardRouter::new(Arc::clone(&provider_dyn)));
        let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
        let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));
        let config = NodeConfig::new(
            *node_id,
            addr.clone(),
            storage,
            provider_dyn,
            service_factory(), // ignored once group_manager is pre-built
        )
        .with_group_manager(Arc::clone(&mgr))
        .with_raft_lookup(lookup)
        .with_client_dispatch(dispatch)
        .with_allow_insecure(true);
        let node = Arc::new(RrNode::new(config).unwrap());
        node.start().await.unwrap();
        nodes.push(node);
    }

    let mut members = BTreeMap::new();
    for (node_id, addr) in &node_addrs {
        members.insert(*node_id, BasicNode { addr: addr.clone() });
    }
    let raft = nodes[0]
        .group_manager()
        .get_raft(1)
        .await
        .expect("node 1 hosts group 1");
    let _ = raft.initialize(members).await;

    // Let gRPC servers finish coming up before clients dial.
    tokio::time::sleep(Duration::from_millis(100)).await;
    (nodes, provider)
}

async fn batch_client(
    provider: &MemConfigProvider,
    max_entries: usize,
    linger: Duration,
) -> rr_client::RrClient {
    let addrs: Vec<String> = provider
        .get_routing_table()
        .await
        .unwrap()
        .nodes
        .values()
        .map(|n| n.addr.clone())
        .collect();
    RrClientBuilder::new(addrs)
        .with_retry_config(RetryConfig {
            // Fresh-leader lease window: alpha.34 fast-fails proposals until
            // the lease establishes; give redirects enough attempts (same
            // budget as the leader-redirect harness).
            max_retries: 10,
            retry_delay_ms: 100,
        })
        .with_batching(BatchConfig {
            max_entries,
            linger,
            ..BatchConfig::default()
        })
        .build()
        .await
        .unwrap()
}

/// Eight concurrent unkeyed writes with `max_entries = 8` must share at
/// least one flush; each waiter resolves with ITS OWN echoed payload
/// (positional response mapping), proving both the apply-side split and the
/// client-side fan-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_batched_writes_commit_and_resolve_per_item() {
    let (nodes, provider) = build_service_cluster().await;
    crate::wait_for_cluster_ready(&nodes, 1, 8_000)
        .await
        .unwrap();
    let client = batch_client(&provider, 8, Duration::from_millis(50)).await;

    let mut handles = Vec::new();
    for i in 0..8u8 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.send_request(1, vec![0xA0, i]).await
        }));
    }

    let mut got = Vec::new();
    for h in handles {
        got.push(h.await.expect("task joins").expect("write resolves"));
    }
    let mut got: Vec<u8> = got.into_iter().map(|v| v[1]).collect();
    got.sort_unstable();
    assert_eq!(
        got,
        (0..8).collect::<Vec<u8>>(),
        "every batched write resolves with its own response"
    );

    crate::shutdown_all(&nodes).await;
}

/// Below the size threshold, the linger timer must still flush — a lone
/// write resolves without waiting for a full batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_batch_linger_flushes_below_threshold() {
    let (nodes, provider) = build_service_cluster().await;
    crate::wait_for_cluster_ready(&nodes, 1, 8_000)
        .await
        .unwrap();
    let client = batch_client(&provider, 1000, Duration::from_millis(20)).await;

    let started = std::time::Instant::now();
    let resp = tokio::time::timeout(
        Duration::from_secs(5),
        client.send_request(1, b"lone".to_vec()),
    )
    .await
    .expect("linger flush must resolve the write well before any size trigger")
    .unwrap();
    assert_eq!(resp, b"lone", "EchoService must echo the lone write");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "linger flush latency sanity"
    );

    crate::shutdown_all(&nodes).await;
}

/// With batching enabled, keyed (idempotent) writes still go through the
/// direct framework idempotency path: replaying the same key must return
/// the cached response without a second apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_keyed_writes_bypass_batching_and_stay_idempotent() {
    let (nodes, provider) = build_service_cluster().await;
    crate::wait_for_cluster_ready(&nodes, 1, 8_000)
        .await
        .unwrap();
    let client = batch_client(&provider, 4, Duration::from_millis(20)).await;

    let key = rr_core::idempotency::IdempotencyKey::new("batch-e2e", 1);
    let first = client
        .send_request_idempotent(1, b"keyed".to_vec(), key.clone())
        .await
        .unwrap();
    let replay = client
        .send_request_idempotent(1, b"keyed".to_vec(), key)
        .await
        .unwrap();
    assert_eq!(first, b"keyed");
    assert_eq!(replay, first, "keyed replay returns the cached response");

    crate::shutdown_all(&nodes).await;
}
