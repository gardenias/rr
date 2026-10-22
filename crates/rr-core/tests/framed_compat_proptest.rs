//! Bidirectional framed-codec compatibility matrix + arbitrary-bytes
//! robustness (reliability program R-6, proptest item 4).
//!
//! Promises under test (the forward-compat commitment in
//! `docs/docs/reference/compatibility.md` and the CHANGELOG):
//!
//! - bytes carrying `version <= CURRENT` MUST still decode — the
//!   forward-compat promise to older on-disk logs/snapshots;
//! - bytes carrying `version > CURRENT` MUST fail with a clear, typed
//!   `RrError::Codec(CodecError::DeserializeFailed)` naming the version —
//!   never a panic, never a silent misread;
//! - bytes WITHOUT the magic prefix are legacy payloads: `Ok(None)`, never
//!   `Err`, never a misparse;
//! - `decode` never panics on arbitrary bytes.
//!
//! The matrix drives the real public API (`FramedEntry` / `FramedSnapshot` /
//! `ApplyOutcome` encode+decode and their `#[doc(hidden)]` magic/version
//! constants) — no reimplementation of the wire format in the test.

use proptest::prelude::*;

use rr_core::error::{CodecError, RrError};
use rr_core::framed_entry::{FramedEntry, FRAMED_ENTRY_MAGIC, FRAMED_ENTRY_VERSION};
use rr_core::framed_snapshot::{
    FramedSnapshot, FrameworkState, IdempotencyState, IngestorState, OutboxState,
    FRAMED_SNAPSHOT_MAGIC, FRAMED_SNAPSHOT_VERSION,
};
use rr_core::idempotency::{
    ApplyOutcome, IdempotencyError, IdempotencyKey, APPLY_OUTCOME_TAG, APPLY_OUTCOME_VERSION,
};
use rr_core::output::{Output, OutputTarget};

/// The designed version axis: {0, 1, current, current+1, u8::MAX}.
/// (`current` is duplicated deliberately when `current == 1`.)
fn version_strategy(current: u8) -> impl Strategy<Value = u8> {
    prop::sample::select(vec![0u8, 1, current, current.saturating_add(1), u8::MAX])
}

fn small_bytes(max: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..=max)
}

fn idem_key_strategy() -> impl Strategy<Value = Option<IdempotencyKey>> {
    prop::option::of(
        (
            prop::sample::select(vec!["transfer", "create_user", "t"]),
            any::<u64>(),
            prop::option::of(prop::sample::select(vec!["client-a", "client-b"])),
        )
            .prop_map(|(rt, id, client)| match client {
                None => IdempotencyKey::new(rt, id),
                Some(c) => IdempotencyKey::with_client_id(rt, id, c),
            }),
    )
}

fn framework_state_strategy() -> impl Strategy<Value = FrameworkState> {
    (
        prop::option::of(small_bytes(24)),
        prop::option::of(prop::option::of(small_bytes(8))),
        prop::option::of((
            prop::collection::vec(small_bytes(12), 0..=2),
            any::<u64>(),
            any::<u64>(),
        )),
    )
        .prop_map(|(idem, ingestor, outbox)| {
            let outbox = outbox.map(|(payloads, sent_watermark, next_seq)| OutboxState {
                pending: payloads
                    .into_iter()
                    .enumerate()
                    .map(|(i, p)| {
                        Output::new(
                            format!("golden-req:{i}"),
                            OutputTarget::RaftWrite {
                                addrs: vec!["127.0.0.1:50051".to_string()],
                                group_id: 7,
                            },
                            p,
                        )
                    })
                    .collect(),
                sent_watermark,
                next_seq,
            });
            FrameworkState {
                idempotency: idem.map(|data| IdempotencyState { data }),
                ingestor: ingestor.map(|last_applied_offset| IngestorState {
                    last_applied_offset,
                }),
                outbox,
            }
        })
}

fn outcome_strategy() -> impl Strategy<Value = ApplyOutcome> {
    prop_oneof![
        2 => small_bytes(48).prop_map(ApplyOutcome::Success),
        1 => (
            prop::sample::select(vec!["transfer", "create_user"]),
            any::<u64>(),
            any::<u64>(),
        )
            .prop_map(|(rt, request_id, watermark)| {
                ApplyOutcome::IdempotencyExpired(IdempotencyError {
                    request_type: rt.to_string(),
                    request_id,
                    watermark,
                })
            }),
    ]
}

/// The version gate must reject future versions with a typed
/// `Codec::DeserializeFailed` whose message names the offending version.
fn assert_version_gate_error(err: &RrError, version: u8) -> Result<(), TestCaseError> {
    match err {
        RrError::Codec(CodecError::DeserializeFailed(msg)) => {
            prop_assert!(
                msg.contains("version"),
                "error must name the version gate, got: {}",
                msg
            );
            prop_assert!(
                msg.contains(&version.to_string()),
                "error must name the offending version {}, got: {}",
                version,
                msg
            );
            Ok(())
        }
        other => Err(TestCaseError::fail(format!(
            "future version {version} must produce Codec::DeserializeFailed, got {other:?}"
        ))),
    }
}

fn assert_entry_eq(a: &FramedEntry, b: &FramedEntry) -> Result<(), TestCaseError> {
    prop_assert_eq!(a.version, b.version);
    prop_assert_eq!(&a.offset, &b.offset);
    prop_assert_eq!(&a.idem_key, &b.idem_key);
    prop_assert_eq!(&a.payload, &b.payload);
    Ok(())
}

fn assert_snapshot_eq(a: &FramedSnapshot, b: &FramedSnapshot) -> Result<(), TestCaseError> {
    prop_assert_eq!(a.version, b.version);
    prop_assert_eq!(&a.user_snapshot, &b.user_snapshot);
    prop_assert_eq!(
        a.framework_state.idempotency.as_ref().map(|s| &s.data),
        b.framework_state.idempotency.as_ref().map(|s| &s.data),
    );
    prop_assert_eq!(
        a.framework_state
            .ingestor
            .as_ref()
            .map(|s| &s.last_applied_offset),
        b.framework_state
            .ingestor
            .as_ref()
            .map(|s| &s.last_applied_offset),
    );
    match (&a.framework_state.outbox, &b.framework_state.outbox) {
        (None, None) => {}
        (Some(x), Some(y)) => {
            prop_assert_eq!(x.sent_watermark, y.sent_watermark);
            prop_assert_eq!(x.next_seq, y.next_seq);
            prop_assert_eq!(x.pending.len(), y.pending.len());
            for (o1, o2) in x.pending.iter().zip(y.pending.iter()) {
                prop_assert_eq!(&o1.idempotency_key, &o2.idempotency_key);
                prop_assert_eq!(&o1.payload, &o2.payload);
                match (&o1.target, &o2.target) {
                    (
                        OutputTarget::RaftWrite {
                            addrs: a1,
                            group_id: g1,
                        },
                        OutputTarget::RaftWrite {
                            addrs: a2,
                            group_id: g2,
                        },
                    ) => {
                        prop_assert_eq!(a1, a2);
                        prop_assert_eq!(g1, g2);
                    }
                    _ => {
                        return Err(TestCaseError::fail("outbox target mismatch"));
                    }
                }
            }
        }
        _ => return Err(TestCaseError::fail("outbox presence mismatch")),
    }
    Ok(())
}

fn assert_outcome_eq(a: &ApplyOutcome, b: &ApplyOutcome) -> Result<(), TestCaseError> {
    match (a, b) {
        (ApplyOutcome::Success(x), ApplyOutcome::Success(y)) => prop_assert_eq!(x, y),
        (ApplyOutcome::IdempotencyExpired(x), ApplyOutcome::IdempotencyExpired(y)) => {
            prop_assert_eq!(x, y)
        }
        _ => return Err(TestCaseError::fail("apply outcome variant mismatch")),
    }
    Ok(())
}

proptest! {
    // Bounded case count: these run in the default CI test job (R-6 budget).
    // Pure CPU bincode work — the default 256 cases complete in well under a second.
    //
    // R-6 review item L7a: persist failing seeds into the git-tracked
    // `crates/rr-core/proptest-regressions/` so a failure reproduces across
    // runs. `SourceParallel` cannot find a lib.rs/main.rs ancestor above
    // `tests/` (it would degrade to an extension-swapped sibling file with a
    // warning), so pin the absolute path via CARGO_MANIFEST_DIR.
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: Some(Box::new(
            proptest::test_runner::FileFailurePersistence::Direct(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/proptest-regressions/framed_compat_proptest.txt"
            )),
        )),
        ..ProptestConfig::default()
    })]

    #[test]
    fn proptest_framed_entry_version_matrix(
        version in version_strategy(FRAMED_ENTRY_VERSION),
        payload in small_bytes(48),
        offset in prop::option::of(small_bytes(16)),
        idem_key in idem_key_strategy(),
    ) {
        // Encode never validates the version, so any version can be put on
        // the wire — that is exactly what the matrix needs.
        let entry = FramedEntry {
            version,
            offset,
            idem_key,
            payload,
        };
        let bytes = entry.encode().expect("encode must succeed for any version");
        prop_assert!(bytes.starts_with(&FRAMED_ENTRY_MAGIC));

        match FramedEntry::decode(&bytes) {
            Ok(Some(decoded)) => {
                prop_assert!(
                    version <= FRAMED_ENTRY_VERSION,
                    "future version {} must NOT decode (silent misread)",
                    version
                );
                assert_entry_eq(&decoded, &entry)?;
            }
            Ok(None) => {
                return Err(TestCaseError::fail(
                    "magic-prefixed bytes must never fall back to the legacy path",
                ));
            }
            Err(e) => {
                prop_assert!(
                    version > FRAMED_ENTRY_VERSION,
                    "version {} <= current ({}) must decode, got {}",
                    version,
                    FRAMED_ENTRY_VERSION,
                    e
                );
                assert_version_gate_error(&e, version)?;
            }
        }
    }

    #[test]
    fn proptest_framed_snapshot_version_matrix(
        version in version_strategy(FRAMED_SNAPSHOT_VERSION),
        user_snapshot in small_bytes(48),
        framework_state in framework_state_strategy(),
    ) {
        let snapshot = FramedSnapshot {
            version,
            framework_state,
            user_snapshot,
        };
        let bytes = snapshot.encode().expect("encode must succeed for any version");
        prop_assert!(bytes.starts_with(&FRAMED_SNAPSHOT_MAGIC));

        match FramedSnapshot::decode(&bytes) {
            Ok(Some(decoded)) => {
                prop_assert!(
                    version <= FRAMED_SNAPSHOT_VERSION,
                    "future version {} must NOT decode (silent misread)",
                    version
                );
                assert_snapshot_eq(&decoded, &snapshot)?;
            }
            Ok(None) => {
                return Err(TestCaseError::fail(
                    "magic-prefixed bytes must never fall back to the legacy path",
                ));
            }
            Err(e) => {
                prop_assert!(
                    version > FRAMED_SNAPSHOT_VERSION,
                    "version {} <= current ({}) must decode, got {}",
                    version,
                    FRAMED_SNAPSHOT_VERSION,
                    e
                );
                assert_version_gate_error(&e, version)?;
            }
        }
    }

    #[test]
    fn proptest_apply_outcome_version_matrix(
        version in version_strategy(APPLY_OUTCOME_VERSION),
        outcome in outcome_strategy(),
    ) {
        let mut bytes = outcome.encode().expect("encode must succeed");
        prop_assert_eq!(bytes[0], APPLY_OUTCOME_TAG);
        // The apply-outcome header carries the version as a raw byte —
        // rewrite it directly to put any matrix version on the wire.
        bytes[1] = version;

        match ApplyOutcome::decode(&bytes) {
            Ok(decoded) => {
                prop_assert!(
                    version <= APPLY_OUTCOME_VERSION,
                    "future version {} must NOT decode (silent misread)",
                    version
                );
                assert_outcome_eq(&decoded, &outcome)?;
            }
            Err(e) => {
                prop_assert!(
                    version > APPLY_OUTCOME_VERSION,
                    "version {} <= current ({}) must decode, got {}",
                    version,
                    APPLY_OUTCOME_VERSION,
                    e
                );
                assert_version_gate_error(&e, version)?;
            }
        }
    }

    #[test]
    fn proptest_apply_outcome_bad_tag_is_typed_error(
        tag in any::<u8>(),
        outcome in outcome_strategy(),
    ) {
        let mut bytes = outcome.encode().expect("encode must succeed");
        bytes[0] = tag;
        let decoded = ApplyOutcome::decode(&bytes);
        if tag == APPLY_OUTCOME_TAG {
            prop_assert!(decoded.is_ok(), "correct tag must decode");
        } else {
            match decoded {
                Err(RrError::Codec(CodecError::DeserializeFailed(msg))) => {
                    prop_assert!(msg.contains("tag"), "tag error must be named, got: {}", msg);
                }
                other => {
                    return Err(TestCaseError::fail(format!(
                        "bad tag {tag:#x} must be a typed DeserializeFailed, got {other:?}"
                    )));
                }
            }
        }
    }

    #[test]
    fn proptest_apply_outcome_truncated_is_typed_error(len in 0usize..2) {
        let bytes = vec![APPLY_OUTCOME_TAG; len];
        match ApplyOutcome::decode(&bytes) {
            Err(RrError::Codec(CodecError::DeserializeFailed(_))) => {}
            other => {
                return Err(TestCaseError::fail(format!(
                    "header shorter than 2 bytes must be a typed error, got {other:?}"
                )));
            }
        }
    }

    #[test]
    fn proptest_framed_entry_decode_arbitrary_bytes(
        data in prop::collection::vec(any::<u8>(), 0..72),
    ) {
        // Never panics (proptest fails the case on panic).
        let res = FramedEntry::decode(&data);
        if data.len() < FRAMED_ENTRY_MAGIC.len() || data[..FRAMED_ENTRY_MAGIC.len()] != FRAMED_ENTRY_MAGIC {
            prop_assert!(
                matches!(res, Ok(None)),
                "non-magic bytes are legacy payloads and must decode to Ok(None), got {:?}",
                res
            );
        } else {
            match res {
                Ok(Some(f)) => prop_assert!(
                    f.version <= FRAMED_ENTRY_VERSION,
                    "a future-version envelope must never decode successfully"
                ),
                Ok(None) => {
                    return Err(TestCaseError::fail(
                        "magic-prefixed bytes must never fall back to the legacy path",
                    ));
                }
                Err(_) => {}
            }
        }
    }

    #[test]
    fn proptest_framed_snapshot_decode_arbitrary_bytes(
        data in prop::collection::vec(any::<u8>(), 0..72),
    ) {
        let res = FramedSnapshot::decode(&data);
        if data.len() < FRAMED_SNAPSHOT_MAGIC.len()
            || data[..FRAMED_SNAPSHOT_MAGIC.len()] != FRAMED_SNAPSHOT_MAGIC
        {
            prop_assert!(
                matches!(res, Ok(None)),
                "non-magic bytes are legacy payloads and must decode to Ok(None), got {:?}",
                res
            );
        } else {
            match res {
                Ok(Some(f)) => prop_assert!(
                    f.version <= FRAMED_SNAPSHOT_VERSION,
                    "a future-version envelope must never decode successfully"
                ),
                Ok(None) => {
                    return Err(TestCaseError::fail(
                        "magic-prefixed bytes must never fall back to the legacy path",
                    ));
                }
                Err(_) => {}
            }
        }
    }
}
