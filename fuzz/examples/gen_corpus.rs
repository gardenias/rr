//! Seed-corpus generator for the fuzz targets.
//!
//! Regenerate the committed corpus after any wire-format change:
//!
//! ```bash
//! cargo run --example gen_corpus          # from the fuzz/ directory
//! ```
//!
//! Seeds are produced exclusively through the crates' *real* encode paths
//! (`FramedEntry::encode`, `FramedSnapshot::encode`, `ApplyOutcome::encode`,
//! `prost::Message::encode_to_vec`) so they are guaranteed to be valid
//! current-format frames. Each target also gets a truncated sample and
//! (for the versioned formats) a future-version sample so the rejection
//! paths are covered from the first run.
//!
//! Layout follows the cargo-fuzz default convention:
//! `fuzz/corpus/<target>/<seed-file>`.

use std::fs;
use std::path::{Path, PathBuf};

use prost::Message;
use rr_core::framed_entry::FramedEntry;
use rr_core::framed_snapshot::{FramedSnapshot, FrameworkState, IdempotencyState, IngestorState, OutboxState};
use rr_core::idempotency::{ApplyOutcome, IdempotencyError, IdempotencyKey};
use rr_core::output::{Output, OutputTarget};

fn corpus_dir(target: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("corpus")
        .join(target)
}

fn write_seed(target: &str, name: &str, bytes: &[u8]) {
    let dir = corpus_dir(target);
    fs::create_dir_all(&dir).expect("create corpus dir");
    let path = dir.join(name);
    fs::write(&path, bytes).expect("write seed");
    println!("wrote {:>6} bytes  {}", bytes.len(), path.display());
}

/// Truncate to just past the header so the magic/tag is intact but the
/// body is cut off mid-decode.
fn truncated(bytes: &[u8], keep: usize) -> Vec<u8> {
    bytes[..keep.min(bytes.len())].to_vec()
}

fn framed_entry_seeds() -> Vec<(&'static str, Vec<u8>)> {
    let mut seeds: Vec<(&'static str, Vec<u8>)> = Vec::new();

    let encode = |e: &FramedEntry| e.encode().expect("encode framed entry");

    seeds.push(("entry_empty.bin", encode(&FramedEntry::from_payload(vec![]))));
    seeds.push(("entry_hello.bin", encode(&FramedEntry::from_payload(b"hello".to_vec()))));
    seeds.push((
        "entry_idem.bin",
        encode(&FramedEntry::from_payload(b"value".to_vec())
            .with_idem_key(IdempotencyKey::new("transfer", 42))),
    ));
    seeds.push((
        "entry_idem_client.bin",
        encode(&FramedEntry::from_payload(b"value".to_vec())
            .with_idem_key(IdempotencyKey::with_client_id("transfer", 42, "alice"))),
    ));
    seeds.push((
        "entry_offset.bin",
        encode(&FramedEntry::from_payload(b"x".to_vec())
            .with_offset(vec![0, 1, 2, 3, 4, 5, 6, 7])),
    ));
    seeds.push((
        "entry_full.bin",
        encode(&FramedEntry::from_payload(b"user payload".to_vec())
            .with_offset(vec![9, 8, 7])
            .with_idem_key(IdempotencyKey::with_client_id("create_user", 7, "client-1"))),
    ));
    // Legacy unframed bytes: decode accepts them as `Ok(None)`.
    seeds.push(("entry_legacy.bin", b"raw user payload from v0.2.x".to_vec()));

    // Rejection paths: truncated body, and a version newer than this binary.
    let valid = seeds[5].1.clone(); // entry_full.bin
    seeds.push(("entry_truncated.bin", truncated(&valid, 12)));
    let future = FramedEntry {
        version: 2, // current is 1; must decode to a loud Err
        ..FramedEntry::from_payload(b"hello".to_vec())
    };
    seeds.push(("entry_future_version.bin", encode(&future)));

    seeds
}

fn framed_snapshot_seeds() -> Vec<(&'static str, Vec<u8>)> {
    let mut seeds: Vec<(&'static str, Vec<u8>)> = Vec::new();

    let encode = |s: &FramedSnapshot| s.encode().expect("encode framed snapshot");

    seeds.push(("snap_empty.bin", encode(&FramedSnapshot::new(vec![]))));
    seeds.push(("snap_user.bin", encode(&FramedSnapshot::new(b"some user state".to_vec()))));

    let mut with_ingestor = FramedSnapshot::new(b"user".to_vec());
    with_ingestor.framework_state.ingestor = Some(IngestorState {
        last_applied_offset: Some(vec![0, 1, 2, 3, 4, 5, 6, 7]),
    });
    seeds.push(("snap_ingestor.bin", encode(&with_ingestor)));

    let mut with_idem = FramedSnapshot::new(b"user".to_vec());
    with_idem.framework_state.idempotency = Some(IdempotencyState {
        data: b"opaque-layer-blob".to_vec(),
    });
    seeds.push(("snap_idempotency.bin", encode(&with_idem)));

    let mut with_outbox = FramedSnapshot::new(b"user".to_vec());
    with_outbox.framework_state.outbox = Some(OutboxState {
        pending: vec![Output::new(
            "req-1:0",
            OutputTarget::RaftWrite {
                addrs: vec!["127.0.0.1:50051".to_string()],
                group_id: 7,
            },
            b"payload".to_vec(),
        )],
        sent_watermark: 0,
        next_seq: 1,
    });
    seeds.push(("snap_outbox.bin", encode(&with_outbox)));

    // Legacy unframed snapshot bytes: decode accepts them as `Ok(None)`.
    seeds.push(("snap_legacy.bin", b"raw user snapshot bytes from v0.2.x".to_vec()));

    // Rejection paths.
    let valid = seeds[4].1.clone(); // snap_outbox.bin
    seeds.push(("snap_truncated.bin", truncated(&valid, 16)));
    let future = FramedSnapshot {
        version: 2, // current is 1; must decode to a loud Err
        framework_state: FrameworkState::default(),
        user_snapshot: b"hello".to_vec(),
    };
    seeds.push(("snap_future_version.bin", encode(&future)));

    seeds
}

fn apply_outcome_seeds() -> Vec<(&'static str, Vec<u8>)> {
    let mut seeds: Vec<(&'static str, Vec<u8>)> = Vec::new();

    let encode = |o: &ApplyOutcome| o.encode().expect("encode apply outcome");

    seeds.push(("outcome_success_empty.bin", encode(&ApplyOutcome::Success(vec![]))));
    seeds.push(("outcome_success.bin", encode(&ApplyOutcome::Success(b"response bytes".to_vec()))));
    seeds.push((
        "outcome_expired.bin",
        encode(&ApplyOutcome::IdempotencyExpired(IdempotencyError {
            request_type: "transfer".to_string(),
            request_id: 100,
            watermark: 200,
        })),
    ));
    seeds.push((
        "outcome_expired_edge.bin",
        encode(&ApplyOutcome::IdempotencyExpired(IdempotencyError {
            request_type: String::new(),
            request_id: 0,
            watermark: u64::MAX,
        })),
    ));

    // Rejection paths: header cut short, and a version newer than this
    // binary (tag + version live at bytes 0..=1).
    let valid = seeds[1].1.clone(); // outcome_success.bin
    seeds.push(("outcome_truncated.bin", truncated(&valid, 1)));
    let mut future = valid.clone();
    future[1] = 2; // current is 1; must decode to a loud Err
    seeds.push(("outcome_future_version.bin", future));

    seeds
}

fn client_request_proto_seeds() -> Vec<(&'static str, Vec<u8>)> {
    use rr_network::proto::client;

    let mut seeds: Vec<(&'static str, Vec<u8>)> = Vec::new();

    seeds.push((
        "req_minimal.bin",
        client::ClientRequest {
            partition_id: 1,
            payload: b"ping".to_vec(),
            idem_key: None,
        }
        .encode_to_vec(),
    ));
    seeds.push((
        "req_idem.bin",
        client::ClientRequest {
            partition_id: 3,
            payload: b"set foo=bar".to_vec(),
            idem_key: Some(client::IdempotencyKey {
                request_type: "transfer".to_string(),
                request_id: 42,
                client_id: None,
            }),
        }
        .encode_to_vec(),
    ));
    seeds.push((
        "req_idem_client.bin",
        client::ClientRequest {
            partition_id: 3,
            payload: b"set foo=bar".to_vec(),
            idem_key: Some(client::IdempotencyKey {
                request_type: "transfer".to_string(),
                request_id: 42,
                client_id: Some("alice".to_string()),
            }),
        }
        .encode_to_vec(),
    ));
    seeds.push((
        "req_large_payload.bin",
        client::ClientRequest {
            partition_id: u64::MAX,
            payload: vec![0xAB; 64],
            idem_key: None,
        }
        .encode_to_vec(),
    ));

    // Rejection path: length-delimited field cut off mid-value.
    let valid = seeds[2].1.clone(); // req_idem_client.bin
    seeds.push(("req_truncated.bin", truncated(&valid, valid.len() / 2)));

    seeds
}

fn main() {
    let entry = framed_entry_seeds();
    let snapshot = framed_snapshot_seeds();
    let outcome = apply_outcome_seeds();
    let proto = client_request_proto_seeds();

    // The roundtrip targets share the input format with their decode
    // counterpart, so they get identical seeds.
    for (target, seeds) in [
        ("fuzz_framed_entry_decode", &entry),
        ("fuzz_framed_entry_roundtrip", &entry),
        ("fuzz_framed_snapshot_decode", &snapshot),
        ("fuzz_apply_outcome_decode", &outcome),
        ("fuzz_apply_outcome_roundtrip", &outcome),
        ("fuzz_client_request_proto", &proto),
    ] {
        for (name, bytes) in seeds {
            write_seed(target, name, bytes);
        }
    }
}
