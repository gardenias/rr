//! Golden corpus for the framed wire formats (reliability program R-6,
//! proptest item 4; mitigation for review P2 "bincode 未精确锁定").
//!
//! A small set of fixed, known-good byte samples is committed under
//! `crates/rr-core/tests/golden/`. This test asserts that the CURRENT code
//!
//! 1. still decodes each sample to exactly the expected value, and
//! 2. re-encodes the expected value to the sample **byte-for-byte**.
//!
//! Direction (2) is what catches silent format drift: a bincode upgrade or
//! a field-order change that keeps decode working but changes the emitted
//! bytes turns this test red before such a build can be written to disk.
//!
//! Regeneration recipe (only ever needed when intentionally bumping the wire
//! format): every sample is constructed in code in [`golden_samples`] — write
//! `encode()` of each value to the file named beside it. The construction in
//! this file IS the authoritative spec of the corpus.

use std::fs;

use rr_core::framed_entry::{FramedEntry, FRAMED_ENTRY_MAGIC, FRAMED_ENTRY_VERSION};
use rr_core::framed_snapshot::{
    FramedSnapshot, FrameworkState, IdempotencyState, IngestorState, OutboxState,
    FRAMED_SNAPSHOT_MAGIC, FRAMED_SNAPSHOT_VERSION,
};
use rr_core::idempotency::{
    ApplyOutcome, IdempotencyError, IdempotencyKey, APPLY_OUTCOME_TAG, APPLY_OUTCOME_VERSION,
};
use rr_core::output::{Output, OutputTarget};

const GOLDEN_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");

/// One corpus sample: the file name under `tests/golden/` plus the exact
/// value the file must contain.
enum Golden {
    Entry(FramedEntry),
    Snapshot(FramedSnapshot),
    Outcome(ApplyOutcome),
}

impl Golden {
    fn encode(&self) -> Vec<u8> {
        match self {
            Golden::Entry(e) => e.encode().expect("golden entry encode"),
            Golden::Snapshot(s) => s.encode().expect("golden snapshot encode"),
            Golden::Outcome(o) => o.encode().expect("golden outcome encode"),
        }
    }
}

/// The corpus. Fixed values only — no randomness, no timestamps — so the
/// committed bytes are reproducible forever.
fn golden_samples() -> Vec<(&'static str, Golden)> {
    vec![
        (
            "framed_entry_plain.bin",
            Golden::Entry(FramedEntry::from_payload(
                b"golden: plain entry payload".to_vec(),
            )),
        ),
        (
            "framed_entry_full.bin",
            Golden::Entry(
                FramedEntry::from_payload(b"golden: keyed+offset entry payload".to_vec())
                    .with_idem_key(IdempotencyKey::with_client_id(
                        "transfer",
                        42,
                        "golden-client",
                    ))
                    .with_offset(vec![0xde, 0xad, 0xbe, 0xef, 0, 0, 0, 7]),
            ),
        ),
        (
            "framed_snapshot_minimal.bin",
            Golden::Snapshot(FramedSnapshot::new(
                b"golden: minimal user snapshot".to_vec(),
            )),
        ),
        (
            "framed_snapshot_full.bin",
            Golden::Snapshot(FramedSnapshot {
                version: FRAMED_SNAPSHOT_VERSION,
                framework_state: FrameworkState {
                    idempotency: Some(IdempotencyState {
                        data: vec![1, 1, 2, 3, 5, 8, 13],
                    }),
                    ingestor: Some(IngestorState {
                        last_applied_offset: Some(vec![0, 0, 0, 0, 0, 0, 0, 9]),
                    }),
                    outbox: Some(OutboxState {
                        pending: vec![Output::new(
                            "golden-req:0",
                            OutputTarget::RaftWrite {
                                addrs: vec!["127.0.0.1:50051".to_string()],
                                group_id: 3,
                            },
                            b"golden outbox payload".to_vec(),
                        )],
                        sent_watermark: 11,
                        next_seq: 12,
                    }),
                },
                user_snapshot: b"golden: full user snapshot".to_vec(),
            }),
        ),
        (
            "apply_outcome_success.bin",
            Golden::Outcome(ApplyOutcome::Success(
                b"golden: apply success response".to_vec(),
            )),
        ),
        (
            "apply_outcome_expired.bin",
            Golden::Outcome(ApplyOutcome::IdempotencyExpired(IdempotencyError {
                request_type: "transfer".to_string(),
                request_id: 42,
                watermark: 77,
            })),
        ),
    ]
}

fn assert_entry_matches(decoded: &FramedEntry, expected: &FramedEntry, file: &str) {
    assert_eq!(decoded.version, expected.version, "{file}: version");
    assert_eq!(decoded.offset, expected.offset, "{file}: offset");
    assert_eq!(decoded.idem_key, expected.idem_key, "{file}: idem_key");
    assert_eq!(decoded.payload, expected.payload, "{file}: payload");
}

fn assert_snapshot_matches(decoded: &FramedSnapshot, expected: &FramedSnapshot, file: &str) {
    assert_eq!(decoded.version, expected.version, "{file}: version");
    assert_eq!(
        decoded.user_snapshot, expected.user_snapshot,
        "{file}: user_snapshot"
    );
    let d = &decoded.framework_state;
    let e = &expected.framework_state;
    assert_eq!(
        d.idempotency.as_ref().map(|s| &s.data),
        e.idempotency.as_ref().map(|s| &s.data),
        "{file}: framework_state.idempotency"
    );
    assert_eq!(
        d.ingestor.as_ref().map(|s| &s.last_applied_offset),
        e.ingestor.as_ref().map(|s| &s.last_applied_offset),
        "{file}: framework_state.ingestor"
    );
    match (&d.outbox, &e.outbox) {
        (None, None) => {}
        (Some(d_out), Some(e_out)) => {
            assert_eq!(
                d_out.sent_watermark, e_out.sent_watermark,
                "{file}: outbox.sent_watermark"
            );
            assert_eq!(d_out.next_seq, e_out.next_seq, "{file}: outbox.next_seq");
            assert_eq!(
                d_out.pending.len(),
                e_out.pending.len(),
                "{file}: outbox.pending len"
            );
            for (d_o, e_o) in d_out.pending.iter().zip(e_out.pending.iter()) {
                assert_eq!(d_o.seq, e_o.seq, "{file}: outbox seq");
                assert_eq!(
                    d_o.idempotency_key, e_o.idempotency_key,
                    "{file}: outbox idempotency_key"
                );
                assert_eq!(d_o.payload, e_o.payload, "{file}: outbox payload");
                match (&d_o.target, &e_o.target) {
                    (
                        OutputTarget::RaftWrite {
                            addrs: d_a,
                            group_id: d_g,
                        },
                        OutputTarget::RaftWrite {
                            addrs: e_a,
                            group_id: e_g,
                        },
                    ) => {
                        assert_eq!(d_a, e_a, "{file}: outbox target addrs");
                        assert_eq!(d_g, e_g, "{file}: outbox target group_id");
                    }
                    _ => panic!("{file}: outbox target mismatch"),
                }
            }
        }
        _ => panic!("{file}: outbox presence mismatch"),
    }
}

fn assert_outcome_matches(decoded: &ApplyOutcome, expected: &ApplyOutcome, file: &str) {
    match (decoded, expected) {
        (ApplyOutcome::Success(d), ApplyOutcome::Success(e)) => {
            assert_eq!(d, e, "{file}: success payload");
        }
        (ApplyOutcome::IdempotencyExpired(d), ApplyOutcome::IdempotencyExpired(e)) => {
            assert_eq!(d, e, "{file}: idempotency error");
        }
        _ => panic!("{file}: apply outcome variant mismatch"),
    }
}

#[test]
fn golden_corpus_decodes_and_reencodes_byte_identical() {
    let samples = golden_samples();
    assert_eq!(samples.len(), 6, "the corpus must stay small and fixed");

    for (file, expected) in &samples {
        let path = format!("{GOLDEN_DIR}/{file}");
        let bytes = fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));

        // (1) Decode to exactly the expected value, with the right envelope
        //     headers.
        match expected {
            Golden::Entry(e) => {
                assert!(
                    bytes.starts_with(&FRAMED_ENTRY_MAGIC),
                    "{file}: missing entry magic"
                );
                assert_eq!(e.version, FRAMED_ENTRY_VERSION, "{file}: spec version");
                let decoded = FramedEntry::decode(&bytes)
                    .unwrap_or_else(|err| panic!("{file}: decode failed: {err}"))
                    .unwrap_or_else(|| panic!("{file}: golden bytes must decode to Some"));
                assert_entry_matches(&decoded, e, file);
            }
            Golden::Snapshot(s) => {
                assert!(
                    bytes.starts_with(&FRAMED_SNAPSHOT_MAGIC),
                    "{file}: missing snapshot magic"
                );
                assert_eq!(s.version, FRAMED_SNAPSHOT_VERSION, "{file}: spec version");
                let decoded = FramedSnapshot::decode(&bytes)
                    .unwrap_or_else(|err| panic!("{file}: decode failed: {err}"))
                    .unwrap_or_else(|| panic!("{file}: golden bytes must decode to Some"));
                assert_snapshot_matches(&decoded, s, file);
            }
            Golden::Outcome(o) => {
                assert_eq!(
                    bytes[0], APPLY_OUTCOME_TAG,
                    "{file}: missing apply-outcome tag"
                );
                assert_eq!(
                    bytes[1], APPLY_OUTCOME_VERSION,
                    "{file}: apply-outcome version"
                );
                let decoded = ApplyOutcome::decode(&bytes)
                    .unwrap_or_else(|err| panic!("{file}: decode failed: {err}"));
                assert_outcome_matches(&decoded, o, file);
            }
        }

        // (2) Re-encode byte-for-byte identical — the bincode drift tripwire.
        let reencoded = expected.encode();
        assert_eq!(
            reencoded, bytes,
            "{file}: re-encode drifted from the committed golden bytes \
             (bincode format change? intentional wire-format bump requires \
             regenerating the corpus deliberately)"
        );
    }
}
