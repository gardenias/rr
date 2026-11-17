//! End-to-end test against a real etcd v3 instance via testcontainers.
//!
//! Run manually:
//! ```bash
//! RR_TEST_ETCD=1 cargo test -p rr-config-etcd --test e2e_real_etcd -- --include-ignored
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use etcd_client::Client;
use rr_config_etcd::{
    ConfigProvider, EtcdConfigProvider, EtcdLifecycleStore, EtcdNodeDirectory, RoutingTableChange,
};
use rr_core::{
    CasOutcome, ClusterLifecycle, ClusterOrigin, DirectoryError, FormationState, GroupFormation,
    LifecycleStore, NodeDescriptor, NodeDirectory, NodeDirectoryChange,
};
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
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_cmd(vec![
            "/usr/local/bin/etcd",
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
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_cmd(vec![
            "/usr/local/bin/etcd",
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

#[tokio::test]
#[ignore = "requires Docker + RR_TEST_ETCD=1"]
async fn lifecycle_store_enforces_cas_end_to_end() {
    if !live_etcd_enabled() {
        eprintln!("RR_TEST_ETCD != 1; skipping live etcd lifecycle test");
        return;
    }

    let container = GenericImage::new("quay.io/coreos/etcd", "v3.5.13")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_cmd(vec![
            "/usr/local/bin/etcd",
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
        .expect("etcd container should start from the existing local image");

    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(2379.tcp()).await.unwrap();
    let endpoint = format!("http://{host}:{port}");
    let cluster_name = "lifecycle-e2e";
    let initial = ClusterLifecycle {
        version: 1,
        cluster_uuid: "12345678-1234-1234-1234-123456789abc".parse().unwrap(),
        topology_revision: 1,
        seed_hash: [7; 32],
        origin: ClusterOrigin::Genesis,
        groups: BTreeMap::from([(
            1,
            GroupFormation {
                bootstrap_epoch: 1,
                state: FormationState::Forming,
            },
        )]),
    };

    let mut stores = Vec::new();
    for _ in 0..8 {
        stores.push(
            EtcdLifecycleStore::new(vec![endpoint.clone()], cluster_name.to_string())
                .await
                .unwrap(),
        );
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(stores.len()));
    let mut tasks = Vec::new();
    for store in stores.iter().cloned() {
        let barrier = Arc::clone(&barrier);
        let initial = initial.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.create_if_absent(initial).await.unwrap()
        }));
    }
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.unwrap());
    }
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == CasOutcome::Applied)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == CasOutcome::Conflict)
            .count(),
        7
    );

    let stale = stores[0].load_linearizable().await.unwrap().unwrap();
    let mut active = stale.record.clone();
    active.groups.get_mut(&1).unwrap().state = FormationState::Active;
    assert_eq!(
        stores[1]
            .compare_and_swap(stale.revision, active)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    assert_eq!(
        stores[2]
            .compare_and_swap(stale.revision, initial)
            .await
            .unwrap(),
        CasOutcome::Conflict
    );

    let fresh = stores[2].load_linearizable().await.unwrap().unwrap();
    assert!(fresh.revision > stale.revision);
    assert_eq!(fresh.record.groups[&1].state, FormationState::Active);
    let mut progressed = fresh.record.clone();
    progressed.topology_revision = 2;
    assert_eq!(
        stores[2]
            .compare_and_swap(fresh.revision, progressed)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    let persisted = stores[0].load_linearizable().await.unwrap().unwrap();
    assert_eq!(persisted.record.groups[&1].state, FormationState::Active);
    assert_eq!(persisted.record.topology_revision, 2);

    let mut raw_client = Client::connect([endpoint.as_str()], None).await.unwrap();
    raw_client
        .put(
            "/rr/lifecycle-malformed/lifecycle/formation",
            b"{".as_slice(),
            None,
        )
        .await
        .unwrap();
    let malformed_store =
        EtcdLifecycleStore::new(vec![endpoint], "lifecycle-malformed".to_string())
            .await
            .unwrap();
    assert!(malformed_store.load_linearizable().await.is_err());
}

fn directory_descriptor(node_id: u64) -> NodeDescriptor {
    NodeDescriptor {
        node_id,
        client_addr: format!("10.0.0.{node_id}:5000"),
        admin_addr: format!("10.0.0.{node_id}:5001"),
        observability_addr: format!("10.0.0.{node_id}:5002"),
        advertise_addr: format!("10.0.0.{node_id}:6000"),
        environment: "e2e".into(),
        region: None,
        zone: None,
        rack: None,
        boot_id: format!("boot-{node_id}"),
    }
}

async fn start_directory_etcd() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("quay.io/coreos/etcd", "v3.5.13")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_cmd(vec![
            "/usr/local/bin/etcd",
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
        .expect("etcd container should start from the existing local image");
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(2379.tcp()).await.unwrap();
    (container, format!("http://{host}:{port}"))
}

#[tokio::test]
#[ignore = "requires Docker + RR_TEST_ETCD=1"]
async fn node_directory_lease_heartbeat_revoke_and_expiry_end_to_end() {
    if !live_etcd_enabled() {
        eprintln!("RR_TEST_ETCD != 1; skipping live etcd directory test");
        return;
    }
    let (_container, endpoint) = start_directory_etcd().await;
    let directory = EtcdNodeDirectory::new(vec![endpoint.clone()], "directory-lease".into())
        .await
        .unwrap();

    let registration = directory.register(directory_descriptor(1)).await.unwrap();
    let before = directory.get(1).await.unwrap().unwrap();
    assert_eq!(before, *registration.record());
    registration.heartbeat().await.unwrap();
    let after = directory.get(1).await.unwrap().unwrap();
    assert_eq!(
        after.revision, before.revision,
        "heartbeat rewrote the value"
    );

    assert!(matches!(
        directory.register(directory_descriptor(1)).await,
        Err(DirectoryError::AlreadyExists { node_id: 1 })
    ));
    registration.revoke().await.unwrap();
    assert!(directory.get(1).await.unwrap().is_none());

    let replacement = directory.register(directory_descriptor(1)).await.unwrap();
    assert!(replacement.record().revision > before.revision);
    replacement.revoke().await.unwrap();

    let expiring = EtcdNodeDirectory::new(vec![endpoint], "directory-expiry".into())
        .await
        .unwrap()
        .with_lease_ttl(Duration::from_secs(1));
    let _expired = expiring.register(directory_descriptor(2)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(expiring.get(2).await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires Docker + RR_TEST_ETCD=1"]
async fn node_directory_watch_observes_upsert_and_lease_deletion() {
    if !live_etcd_enabled() {
        eprintln!("RR_TEST_ETCD != 1; skipping live etcd directory watch test");
        return;
    }
    let (_container, endpoint) = start_directory_etcd().await;
    let directory = EtcdNodeDirectory::new(vec![endpoint], "directory-watch".into())
        .await
        .unwrap();
    let mut changes = directory.watch().await.unwrap();
    let registration = directory.register(directory_descriptor(7)).await.unwrap();

    let upsert = tokio::time::timeout(Duration::from_secs(5), changes.next())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        upsert,
        NodeDirectoryChange::Upsert(ref record) if record.descriptor.node_id == 7
    ));

    registration.heartbeat().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), changes.next())
            .await
            .is_err()
    );

    registration.revoke().await.unwrap();
    let removal = tokio::time::timeout(Duration::from_secs(5), changes.next())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        removal,
        NodeDirectoryChange::Remove { node_id: 7, .. }
    ));
}

#[tokio::test]
#[ignore = "requires Docker + RR_TEST_ETCD=1"]
async fn node_directory_cursor_pins_pages_to_one_revision() {
    if !live_etcd_enabled() {
        eprintln!("RR_TEST_ETCD != 1; skipping live etcd directory pagination test");
        return;
    }
    let (_container, endpoint) = start_directory_etcd().await;
    let directory = EtcdNodeDirectory::new(vec![endpoint], "directory-pages".into())
        .await
        .unwrap();
    let mut registrations = Vec::new();
    for node_id in [1, 10, 2] {
        registrations.push(
            directory
                .register(directory_descriptor(node_id))
                .await
                .unwrap(),
        );
    }

    let first = directory.list(None, Some(2)).await.unwrap();
    let ids: Vec<_> = first
        .records
        .iter()
        .map(|record| record.descriptor.node_id)
        .collect();
    assert_eq!(ids, vec![1, 10]);
    let cursor = first.next_cursor.expect("second page should exist");

    registrations.push(directory.register(directory_descriptor(3)).await.unwrap());
    let second = directory.list(Some(&cursor), Some(2)).await.unwrap();
    let ids: Vec<_> = second
        .records
        .iter()
        .map(|record| record.descriptor.node_id)
        .collect();
    assert_eq!(ids, vec![2], "snapshot page included a newer record");
    assert_eq!(second.revision, first.revision);
    assert!(second.next_cursor.is_none());

    let fresh = directory.list(None, Some(2)).await.unwrap();
    let ids: Vec<_> = fresh
        .records
        .iter()
        .map(|record| record.descriptor.node_id)
        .collect();
    assert_eq!(ids, vec![1, 10]);
    assert!(fresh.revision > first.revision);
}
