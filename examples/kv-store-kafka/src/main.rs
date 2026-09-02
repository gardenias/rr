//! kv-store-kafka — minimal example showing how to wire a Kafka-style
//! command stream into an rr Raft group with auto-idempotency.
//!
//! Default build uses the in-memory [`MockKafkaIngestor`] so this
//! compiles and runs without librdkafka / cmake. To swap to a real
//! broker, build with `--features rdkafka` and replace
//! [`build_ingestor`] with a `KafkaIngestor` constructor — see
//! `docs/docs/guide/kafka-input.md` for the full wiring.
//!
//! Run:
//! ```bash
//! cargo run -p kv-store-kafka
//! ```
//!
//! Expected output: 5 KV-Put commands flow through MockKafkaIngestor
//! → supervisor → raft → state machine; the auto-idempotency layer
//! ensures `Service::handle()` runs at most once per logical Kafka
//! offset even when the example replays the same offsets.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use openraft::BasicNode;
use std::collections::BTreeMap;

use kv_store::types::KvRequest;
use kv_store::KvService;
use rr_core::ingestor::DynCommandIngestor;
use rr_ingestor_kafka::{kafka_offset_to_request_id, MockKafkaHandle, MockKafkaIngestor};
use rr_raft::group_manager::{RaftGroupManager, StateMachineFactory};
use rr_raft::ingestor_runner::{AutoIdempotency, OffsetMapError};
use rr_raft::ServiceStateMachineAdapter;

const GROUP_ID: u64 = 1;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,kv_store_kafka=debug".into()),
        )
        .init();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run())
}

async fn run() -> Result<()> {
    let svc = KvService::new();
    let factory: StateMachineFactory = {
        let svc = svc.clone();
        Box::new(move |gid| Box::new(ServiceStateMachineAdapter::new(gid, svc.clone())))
    };
    let mgr = Arc::new(RaftGroupManager::new(1, factory));
    mgr.start_group(GROUP_ID).await?;

    let mut members = BTreeMap::new();
    members.insert(
        1u64,
        BasicNode {
            addr: String::new(),
        },
    );
    mgr.init_cluster(GROUP_ID, members).await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let (ingestor, handle) = build_ingestor().await;

    let auto_idem = AutoIdempotency {
        request_type: format!("input_{GROUP_ID}"),
        offset_to_request_id: Box::new(|b| {
            kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
        }),
    };
    mgr.attach_ingestor_with_auto_idempotency(GROUP_ID, ingestor, auto_idem)
        .await?;

    seed_messages(&handle, 5).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    tracing::info!("---- replaying same 5 offsets to demonstrate dedup ----");
    let (replay, replay_handle) = MockKafkaIngestor::new();
    seed_messages(&replay_handle, 5).await?;
    let replay_dyn: Box<dyn DynCommandIngestor> = Box::new(replay);
    mgr.stop_ingestor(GROUP_ID).await?;
    mgr.attach_ingestor_with_auto_idempotency(
        GROUP_ID,
        replay_dyn,
        AutoIdempotency {
            request_type: format!("input_{GROUP_ID}"),
            offset_to_request_id: Box::new(|b| {
                kafka_offset_to_request_id(b).map_err(OffsetMapError::new)
            }),
        },
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    tracing::info!("done; check rr_idempotency_replays_total to see the dedup at work");
    mgr.stop_ingestor(GROUP_ID).await?;
    mgr.stop_group(GROUP_ID).await?;
    Ok(())
}

async fn build_ingestor() -> (Box<dyn DynCommandIngestor>, MockKafkaHandle) {
    let (ing, h) = MockKafkaIngestor::new();
    (Box::new(ing), h)
}

async fn seed_messages(handle: &MockKafkaHandle, n: i64) -> Result<()> {
    for i in 0..n {
        let req = KvRequest::Put {
            key: format!("kafka-key-{i}"),
            value: format!("kafka-val-{i}").into_bytes(),
        };
        let payload = bincode::serde::encode_to_vec(&req, bincode::config::standard())?;
        handle.push(i, payload).await;
        tracing::debug!(offset = i, "seeded mock kafka message");
    }
    Ok(())
}
