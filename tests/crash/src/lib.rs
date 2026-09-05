//! `crash-tests` — process-level crash-recovery harness for rr (reliability
//! program R-4, `.sisyphus/plans/rr-reliability-program.md`).
//!
//! Unlike the in-process crash test in `tests/integration`
//! (`test_crash_during_write_preserves_acked_entries`, which simulates a
//! crash by dropping a `RaftGroupManager`), this harness spawns **real node
//! processes** and kills them with **real SIGKILL/SIGTERM**, so the kernel —
//! not a `Drop` impl — decides what survives. Each node runs
//! `rr-crash-node` (see `src/bin/rr-crash-node.rs`): a server assembled from
//! the same production components as `rr-server` (`RaftNode` +
//! `RocksLogStorage` + `IoBatcher` under `DurabilityPolicy::Strict`,
//! `ServiceStateMachineAdapter` idempotency, production gRPC service impls),
//! differing only in the built-in state machine.
//!
//! ## Why not the stock `rr-server` binary?
//!
//! Two discovered gaps make the stock binary unusable for this harness
//! (reported to the R-4 track; both are out of scope for the test crate):
//!
//! 1. Stock `rr-server` registers the raw `EchoStateMachine`
//!    (`crates/rr-server/src/builtin_service.rs`) as its
//!    (`FramedEntry` + `IdempotencyKey`) therefore echoes framed bytes back
//!    and fails `ApplyOutcome::decode` in the dispatch layer
//!    (`Status::internal("internal decode error")`): the idempotency layer
//!    is never engaged through the production binary.
//! 2. `RrNode::start` starts groups via `RaftGroupManager::start_group`,
//!    which hardcodes in-memory `RrLogStorage`; the `--storage rocks`
//!    `RocksStorage` built in `main.rs` is never wired into the Raft log
//!    path, and `RaftGroupManager` has no constructor combining a network
//!    factory with `rocks_base_path`. So a stock `rr-server` restart today
//!    loses the Raft log regardless of `--storage`.
//!
//! `rr-crash-node` wires `RaftNode::new_with_rocks_factory` (RocksDB log,
//! strict durability) + `GrpcNetworkFactory` directly — the exact storage
//! path INV-1 makes claims about — and wraps its service in the production
//! idempotency adapter.
//!
//! ## The oracle
//!
//! After every kill/restart round the harness replays the **ack log** (every
//! client-observed `Ok`, fdatasynced at ack time) by re-issuing each keyed
//! write with a **different probe payload**. Because the idempotency layer
//! short-circuits known keys, a surviving entry returns the *original*
//! payload (from the cache); a lost entry would be freshly applied and return
//! the probe payload instead. This proves, purely from client-observable
//! responses: log survival + no duplicate state-machine apply + idempotency
//! surviving restart — the durability face of exactly-once, strictly
//! stronger than comparing log indices.
//!
//! ## Failure reproducibility
//!
//! The kill schedule is drawn up-front from a seeded RNG
//! (`StdRng::seed_from_u64`; stable for the lockfile-pinned rand 0.8). The
//! seed is printed to the test log and recorded in `fault-log.jsonl`;
//! `CRASH_SEED=<n>` replays an identical `(round, node, planned_offset_ms)`
//! kill sequence.
//!
//! ## Environment knobs
//!
//! | var | default | meaning |
//! |---|---|---|
//! | `CRASH_ROUNDS` | `3` | kill/restart rounds in scenario B (CI uses 10) |
//! | `CRASH_SEED` | entropy | RNG seed for the kill schedule |
//! | `CRASH_PORT_BASE` | probe | port layout base (see [`cluster`]) |
//! | `CRASH_WRITE_RPS` | `50` | driver write rate (writes/s) |
//! | `CRASH_WARMUP_WRITES` | `30` | acked writes required before round 1 |
//! | `CRASH_RUN_DIR` | `target/crash-tests/<scenario>` | logs + journals |
//! | `RR_CRASH_NODE_BIN` | autodetect | path to the `rr-crash-node` binary |
//!

pub mod cluster;
pub mod driver;
pub mod journal;
pub mod oracle;

#[cfg(test)]
mod scenarios;

/// Idempotency `request_type` used for all driver writes. One namespace for
/// the whole run; `request_id` is the dedup identity within it.
pub const REQUEST_TYPE: &str = "crash-write";

/// `client_id` shared by the driver and the oracle. Keeping it constant
/// means an oracle probe of a surviving key hits `IdempotencyDecision::Replay`
/// rather than `Collision` (both return the cached response; Replay is the
/// clean, audit-warning-free path).
pub const CLIENT_ID: &str = "crash-driver";

/// Deterministic payload for the driver's `request_id`-th write.
///
/// The whole run is reproducible from the request id alone: the ack log only
/// needs to store a hash, and the oracle regenerates the expected bytes.
pub fn payload_for(request_id: u64) -> Vec<u8> {
    format!("crash-value-{request_id:08}-{}", "v".repeat(24)).into_bytes()
}

/// Probe payload the oracle sends under the *same* idempotency key as
/// `request_id`. Deliberately different bytes from [`payload_for`]: if the
/// response comes back as the probe payload, the write was freshly applied —
/// i.e. the original entry was lost. If it comes back as the original
/// payload, the idempotency layer served it from the cache, proving the
/// original entry (and the layer state rebuilt from it) survived.
pub fn probe_payload_for(request_id: u64) -> Vec<u8> {
    format!("crash-probe-{request_id:08}-{}", "p".repeat(24)).into_bytes()
}

/// FNV-1a 64-bit hash, hex-encoded. Chosen over `DefaultHasher` because it is
/// stable across Rust versions and processes — the ack log can be inspected
/// offline and hashes recomputed by hand. Not cryptographic; the threat model
/// here is test-oracle bookkeeping, not adversarial input.
pub fn payload_hash(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_and_probe_are_deterministic_and_distinct() {
        assert_eq!(payload_for(7), payload_for(7));
        assert_eq!(probe_payload_for(7), probe_payload_for(7));
        assert_ne!(
            payload_for(7),
            probe_payload_for(7),
            "oracle relies on probe != original"
        );
        assert_ne!(payload_for(7), payload_for(8));
    }

    #[test]
    fn payload_hash_is_stable() {
        // FNV-1a64 of b"" and b"a" are well-known constants.
        assert_eq!(payload_hash(b""), "cbf29ce484222325");
        assert_eq!(payload_hash(b"a"), "af63dc4c8601ec8c");
    }
}
