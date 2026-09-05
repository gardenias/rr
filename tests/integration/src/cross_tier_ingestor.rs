//! Cross-tier integration tests proving Phase 4 Tier 1 (FramedSnapshot)
//! and Tier 3 (CommandIngestor) compose correctly:
//!
//! - Ingestor-pushed commands flow through the supervisor → raft →
//!   adapter → user `Service`.
//! - The applied offset is captured into the framed snapshot.
//! - Snapshot → restore → fresh ingestor sees the persisted offset.
//!
//! These tests use `MockKafkaIngestor` so they run on every CI host
//! without Docker / Kafka. The wire path exercised is identical to
//! the rdkafka backend's — only the source of bytes differs.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::BasicNode;
use tokio::sync::Mutex;

use rr_core::error::RrError;
use rr_core::framed_snapshot::FramedSnapshot;
use rr_core::ingestor::DynCommandIngestor;
use rr_core::service::Service;
use rr_core::state_machine::StateMachine;
use rr_ingestor_kafka::{kafka_offset_to_request_id, MockKafkaIngestor};
use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};
use rr_raft::ingestor_runner::{AutoIdempotency, OffsetMapError};
use rr_raft::ServiceStateMachineAdapter;

/// Service that records the order in which payloads were applied.
/// Used to assert exactly-once apply across the pipeline.
#[derive(Clone, Default)]
struct RecordingService {
    applied: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl Service for RecordingService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        self.applied.lock().await.push(request.clone());
        Ok(request)
    }

    fn name(&self) -> &str {
        "recording"
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        let guard = self.applied.lock().await;
        Ok(
            bincode::serde::encode_to_vec(&*guard, bincode::config::standard())
                .map_err(|e| RrError::Application(e.to_string()))?,
        )
    }

    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        let (decoded, _): (Vec<Vec<u8>>, _) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| RrError::Application(e.to_string()))?;
        *self.applied.lock().await = decoded;
        Ok(())
    }
}

fn factory_from(svc: RecordingService) -> StateMachineFactory {
    Box::new(move |gid| Box::new(ServiceStateMachineAdapter::new(gid, svc.clone())))
}

async fn make_single_node_leader(svc: RecordingService, group_id: u64) -> Arc<RaftGroupManager> {
    let mgr = Arc::new(RaftGroupManager::new(1, factory_from(svc)));
    mgr.start_group(group_id).await.unwrap();
    let mut members = BTreeMap::new();
    members.insert(
        1u64,
        BasicNode {
            addr: String::new(),
        },
    );
    mgr.init_cluster(group_id, members).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let raft = mgr.get_raft(group_id).await.unwrap();
    assert!(
        raft.metrics().borrow_watched().current_leader.is_some(),
        "single-node cluster failed to elect a leader"
    );
    mgr
}

async fn wait_for_applied(svc: &RecordingService, expected: usize, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let n = svc.applied.lock().await.len();
        if n >= expected {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for {expected} applied entries, only saw {n}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn ingestor_pushed_commands_apply_through_full_stack() {
    let svc = RecordingService::default();
    let applied_handle = svc.applied.clone();
    let mgr = make_single_node_leader(svc, 1).await;

    let (ingestor, h) = MockKafkaIngestor::new();
    for i in 0..5_i64 {
        h.push(i, format!("cmd-{i}").into_bytes()).await;
    }

    let dyn_ingestor: Box<dyn DynCommandIngestor> = Box::new(ingestor);
    mgr.attach_ingestor(1, dyn_ingestor).await.unwrap();

    wait_for_applied(
        &RecordingService {
            applied: applied_handle.clone(),
        },
        5,
        Duration::from_secs(5),
    )
    .await;

    let recorded = applied_handle.lock().await.clone();
    assert_eq!(recorded.len(), 5, "expected exactly 5 applied entries");
    for (i, payload) in recorded.iter().enumerate() {
        assert_eq!(*payload, format!("cmd-{i}").into_bytes());
    }
    mgr.stop_ingestor(1).await.unwrap();
    mgr.stop_group(1).await.unwrap();
}

#[tokio::test]
async fn snapshot_after_ingestor_apply_carries_offset_into_framework_state() {
    let svc = RecordingService::default();
    let applied_handle = svc.applied.clone();
    let mgr = make_single_node_leader(svc, 2).await;

    let (ingestor, h) = MockKafkaIngestor::new();
    for i in 100..103_i64 {
        h.push(i, format!("kafka-{i}").into_bytes()).await;
    }
    let dyn_ingestor: Box<dyn DynCommandIngestor> = Box::new(ingestor);
    mgr.attach_ingestor(2, dyn_ingestor).await.unwrap();

    wait_for_applied(
        &RecordingService {
            applied: applied_handle.clone(),
        },
        3,
        Duration::from_secs(5),
    )
    .await;

    mgr.stop_ingestor(2).await.unwrap();
    let raft = mgr.get_raft(2).await.unwrap();
    raft.trigger()
        .snapshot()
        .await
        .expect("trigger snapshot should succeed");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut snap_bytes = None;
    while std::time::Instant::now() < deadline {
        if let Some(bytes) = mgr.fetch_snapshot_bytes(2).await.unwrap() {
            snap_bytes = Some(bytes);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let snap_bytes = snap_bytes.expect("snapshot did not materialise within 5s");
    let framed = FramedSnapshot::decode(&snap_bytes).unwrap().unwrap();

    let ingestor_state = framed
        .framework_state
        .ingestor
        .expect("ingestor state must be persisted alongside applied entries");
    let last = ingestor_state
        .last_applied_offset
        .expect("last_applied_offset must be Some after at least one apply");
    let (decoded, _): (i64, _) =
        bincode::serde::decode_from_slice(&last, bincode::config::standard()).unwrap();
    assert_eq!(
        decoded, 102,
        "framework_state.ingestor.last_applied_offset must equal the highest applied Kafka offset"
    );

    mgr.stop_group(2).await.unwrap();
}

#[tokio::test]
async fn auto_idempotency_dedups_replayed_kafka_offsets() {
    let svc = RecordingService::default();
    let applied_handle = svc.applied.clone();
    let mgr = make_single_node_leader(svc, 4).await;

    let (ingestor, h) = MockKafkaIngestor::new();
    for i in 0..3_i64 {
        h.push(i, format!("evt-{i}").into_bytes()).await;
    }

    let auto_idem = AutoIdempotency {
        request_type: "test_topic_4".into(),
        offset_to_request_id: Box::new(|b| {
            kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
        }),
    };
    let dyn_ingestor: Box<dyn DynCommandIngestor> = Box::new(ingestor);
    mgr.attach_ingestor_with_auto_idempotency(4, dyn_ingestor, auto_idem)
        .await
        .unwrap();

    wait_for_applied(
        &RecordingService {
            applied: applied_handle.clone(),
        },
        3,
        Duration::from_secs(5),
    )
    .await;

    mgr.stop_ingestor(4).await.unwrap();

    let (replay_ingestor, h2) = MockKafkaIngestor::new();
    for i in 0..3_i64 {
        h2.push(i, format!("evt-{i}").into_bytes()).await;
    }
    let replay_auto_idem = AutoIdempotency {
        request_type: "test_topic_4".into(),
        offset_to_request_id: Box::new(|b| {
            kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
        }),
    };
    let dyn_replay: Box<dyn DynCommandIngestor> = Box::new(replay_ingestor);
    mgr.attach_ingestor_with_auto_idempotency(4, dyn_replay, replay_auto_idem)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;

    let recorded = applied_handle.lock().await.clone();
    assert_eq!(
        recorded.len(),
        3,
        "auto-idempotency must dedup replayed offsets — expected 3 applied total, got {}: {:?}",
        recorded.len(),
        recorded
    );

    mgr.stop_ingestor(4).await.unwrap();
    mgr.stop_group(4).await.unwrap();
}

#[tokio::test]
async fn restored_adapter_seeds_new_ingestor_with_persisted_offset() {
    let mut adapter = ServiceStateMachineAdapter::new(3, RecordingService::default());
    let entry = rr_core::framed_entry::FramedEntry::from_payload(b"x".to_vec())
        .with_offset(bincode::serde::encode_to_vec(42_i64, bincode::config::standard()).unwrap())
        .encode()
        .unwrap();
    adapter.apply(&entry).await.unwrap();
    let snap = adapter.snapshot().await.unwrap();

    let mut adapter2 = ServiceStateMachineAdapter::new(3, RecordingService::default());
    adapter2.restore(&snap).await.unwrap();
    let raw = adapter2
        .latest_ingestor_offset()
        .expect("restored adapter must surface the persisted offset");
    let (decoded, _): (i64, _) =
        bincode::serde::decode_from_slice(&raw, bincode::config::standard()).unwrap();
    assert_eq!(decoded, 42);

    let (mut ingestor, _h) = MockKafkaIngestor::new();
    let raw_offset = adapter2.latest_ingestor_offset().unwrap();
    let dyn_ing: &mut dyn DynCommandIngestor = &mut ingestor;
    dyn_ing
        .seek_bytes(Some(&raw_offset))
        .await
        .expect("the persisted bytes must round-trip back through DynCommandIngestor::seek_bytes");
}

/// INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
/// T-003 — Ingestor restart with overlapping offsets must not lose or duplicate
/// messages. Simulates leader-switch recovery where the new ingestor re-seeks
/// from last_applied_offset and replays a partially-overlapping stream.
#[tokio::test]
async fn test_ingestor_survives_leader_switch_no_message_loss() {
    let svc = RecordingService::default();
    let applied_handle = svc.applied.clone();
    let mgr = make_single_node_leader(svc, 5).await;

    // Phase 1 — ingest offsets 0..5
    let (ingestor, h) = MockKafkaIngestor::new();
    for i in 0..5_i64 {
        h.push(i, format!("evt-{i}").into_bytes()).await;
    }

    let auto_idem = AutoIdempotency {
        request_type: "test_topic_5".into(),
        offset_to_request_id: Box::new(|b| {
            kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
        }),
    };
    let dyn_ingestor: Box<dyn DynCommandIngestor> = Box::new(ingestor);
    mgr.attach_ingestor_with_auto_idempotency(5, dyn_ingestor, auto_idem)
        .await
        .unwrap();

    wait_for_applied(
        &RecordingService {
            applied: applied_handle.clone(),
        },
        5,
        Duration::from_secs(5),
    )
    .await;

    // Stop ingestor (simulates leader step-down / ingestor teardown)
    mgr.stop_ingestor(5).await.unwrap();

    // Phase 2 — new ingestor with overlapping stream: offsets 0..10
    // Offsets 0..4 are duplicates, 5..9 are new. Auto-idempotency must dedup
    // the overlap so the service sees exactly 10 unique entries total.
    let (replay_ingestor, h2) = MockKafkaIngestor::new();
    for i in 0..10_i64 {
        h2.push(i, format!("evt-{i}").into_bytes()).await;
    }

    let replay_auto_idem = AutoIdempotency {
        request_type: "test_topic_5".into(),
        offset_to_request_id: Box::new(|b| {
            kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
        }),
    };
    let dyn_replay: Box<dyn DynCommandIngestor> = Box::new(replay_ingestor);
    mgr.attach_ingestor_with_auto_idempotency(5, dyn_replay, replay_auto_idem)
        .await
        .unwrap();

    wait_for_applied(
        &RecordingService {
            applied: applied_handle.clone(),
        },
        10,
        Duration::from_secs(5),
    )
    .await;

    let recorded = applied_handle.lock().await.clone();
    assert_eq!(
        recorded.len(),
        10,
        "auto-idempotency must dedup replayed offsets — expected 10 applied total, got {}: {:?}",
        recorded.len(),
        recorded
    );

    // Verify ordering: first 5 should be evt-0..evt-4, next 5 evt-5..evt-9
    for (i, payload) in recorded.iter().enumerate() {
        let expected = format!("evt-{i}").into_bytes();
        assert_eq!(
            *payload, expected,
            "message at index {i} must match expected payload"
        );
    }

    mgr.stop_ingestor(5).await.unwrap();
    mgr.stop_group(5).await.unwrap();
}
