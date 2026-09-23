//! End-to-end test against a real etcd v3 instance via testcontainers.
//!
//! Run manually:
//! ```bash
//! RR_TEST_ETCD=1 cargo test -p rr-config-etcd --test e2e_real_etcd -- --include-ignored
//! ```

use std::time::Duration;

use etcd_client::Client;
use rr_config_etcd::{ConfigProvider, EtcdConfigProvider, RoutingTableChange};
use serde_json::json;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio_stream::StreamExt;

fn live_etcd_enabled() -> bool {
    matches!(std::env::var("RR_TEST_ETCD").as_deref(), Ok("1"))
}

#[tokio::test]
#[ignore = "requires Docker + RR_TEST_ETCD=1"]
async fn routing_table_and_watch_work_end_to_end() {
    if !live_etcd_enabled() {
        eprintln!("RR_TEST_ETCD != 1; skipping live etcd e2e test");
        return;
    }

    let container = GenericImage::new("quay.io/coreos/etcd", "v3.5.13")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("ready to serve client requests"))
        .with_cmd(vec![
            "--name=node1",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://0.0.0.0:2379",
            "--listen-peer-urls=http://0.0.0.0:2380",
            "--initial-advertise-peer-urls=http://0.0.0.0:2380",
            "--initial-cluster=node1=http://0.0.0.0:2380",
            "--initial-cluster-state=new",
        ])
        .start()
        .await
        .expect("etcd container should start");

    let host = container
        .get_host()
        .await
        .expect("container host should be available")
        .to_string();
    let port = container
        .get_host_port_ipv4(2379.tcp())
        .await
        .expect("mapped etcd client port should be available");

    let endpoint = format!("http://{host}:{port}");
    let cluster_id = "e2e-cluster";

    let mut setup_client = Client::connect([endpoint.as_str()], None)
        .await
        .expect("setup etcd client should connect");

    setup_client
        .put(
            format!("/rr/{cluster_id}/nodes/1"),
            serde_json::to_vec(&json!({ "addr": "127.0.0.1:5001" })).unwrap(),
            None,
        )
        .await
        .expect("seed node key should succeed");

    setup_client
        .put(
            format!("/rr/{cluster_id}/partitions/10"),
            serde_json::to_vec(&json!({
                "config": { "SingleId": 10 },
                "group_id": 1
            }))
            .unwrap(),
            None,
        )
        .await
        .expect("seed partition key should succeed");

    setup_client
        .put(
            format!("/rr/{cluster_id}/groups/1/replicas"),
            serde_json::to_vec(&vec![1_u64, 2, 3]).unwrap(),
            None,
        )
        .await
        .expect("seed group key should succeed");

    let provider = EtcdConfigProvider::new(vec![endpoint.clone()], cluster_id.to_string())
        .await
        .expect("provider should construct");

    let table = provider
        .get_routing_table()
        .await
        .expect("routing table should load");
    assert_eq!(table.nodes.len(), 1);
    assert_eq!(table.partitions.len(), 1);
    assert_eq!(table.groups.len(), 1);
    assert_eq!(
        table
            .nodes
            .get(&1)
            .map(|n| n.addr.as_str())
            .expect("node 1 should exist"),
        "127.0.0.1:5001"
    );

    let mut changes = provider
        .watch_changes()
        .await
        .expect("watch stream should initialize");

    tokio::time::sleep(Duration::from_millis(200)).await;

    setup_client
        .put(
            format!("/rr/{cluster_id}/partitions/10"),
            serde_json::to_vec(&json!({
                "config": { "Range": { "start": 10, "end": 20 } },
                "group_id": 1
            }))
            .unwrap(),
            None,
        )
        .await
        .expect("partition update should succeed");

    let event = tokio::time::timeout(Duration::from_secs(5), changes.next())
        .await
        .expect("watch event should arrive before timeout")
        .expect("watch stream should yield one event");

    assert!(matches!(
        event,
        RoutingTableChange::PartitionUpdated {
            partition_id: 10,
            group_id: 1,
            ..
        }
    ));
}

/// The revision-probe cache must serve identical snapshots for an unchanged
/// table and must observe a write on the very next call (INV-6: etcd stays
/// the single source of truth — a stale cached table is never served after
/// the store revision has moved).
#[tokio::test]
#[ignore = "requires Docker + RR_TEST_ETCD=1"]
async fn routing_table_snapshot_tracks_revision() {
    if !live_etcd_enabled() {
        eprintln!("RR_TEST_ETCD != 1; skipping live etcd e2e test");
        return;
    }

    let container = GenericImage::new("quay.io/coreos/etcd", "v3.5.13")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("ready to serve client requests"))
        .with_cmd(vec![
            "--name=node1",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://0.0.0.0:2379",
            "--listen-peer-urls=http://0.0.0.0:2380",
            "--initial-advertise-peer-urls=http://0.0.0.0:2380",
            "--initial-cluster=node1=http://0.0.0.0:2380",
            "--initial-cluster-state=new",
        ])
        .start()
        .await
        .expect("etcd container should start");

    let host = container
        .get_host()
        .await
        .expect("container host should be available")
        .to_string();
    let port = container
        .get_host_port_ipv4(2379.tcp())
        .await
        .expect("mapped etcd client port should be available");

    let endpoint = format!("http://{host}:{port}");
    let cluster_id = "e2e-snapshot-cache";

    let mut setup_client = Client::connect([endpoint.as_str()], None)
        .await
        .expect("setup etcd client should connect");
    setup_client
        .put(
            format!("/rr/{cluster_id}/partitions/10"),
            serde_json::to_vec(&json!({
                "config": { "SingleId": 10 },
                "group_id": 1
            }))
            .unwrap(),
            None,
        )
        .await
        .expect("seed partition key should succeed");

    let provider = EtcdConfigProvider::new(vec![endpoint], cluster_id.to_string())
        .await
        .expect("provider should construct");

    let first = provider
        .routing_table_snapshot()
        .await
        .expect("snapshot should load");
    assert_eq!(first.partitions.len(), 1);

    // Unchanged revision: same Arc is served (no re-scan).
    let second = provider
        .routing_table_snapshot()
        .await
        .expect("cached snapshot should be served");
    assert!(
        std::sync::Arc::ptr_eq(&first, &second),
        "unchanged revision must serve the cached Arc"
    );

    // A write moves the revision: the next snapshot must reflect it.
    setup_client
        .put(
            format!("/rr/{cluster_id}/partitions/11"),
            serde_json::to_vec(&json!({
                "config": { "SingleId": 11 },
                "group_id": 1
            }))
            .unwrap(),
            None,
        )
        .await
        .expect("second partition write should succeed");

    let third = provider
        .routing_table_snapshot()
        .await
        .expect("post-write snapshot should load");
    assert_eq!(
        third.partitions.len(),
        2,
        "snapshot must observe the write that moved the revision"
    );
    assert!(!std::sync::Arc::ptr_eq(&first, &third));
}
