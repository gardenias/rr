//! Deterministic fault-schedule record/replay for chaos tests (R-3
//! alternative, `.sisyphus/plans/rr-reliability-program.md`).
//!
//! The turmoil spike (`.sisyphus/evidence/rr-r3-turmoil-spike-2026-09-09.md`)
//! proved feasibility but the full pilot exceeds the timebox, so the replay
//! value is landed here instead: fault schedules are **generated from an
//! explicit seed** (or replayed verbatim from a recorded log), and every
//! event actually applied is appended to a JSONL fault log.
//!
//! # Reproducing a failure
//!
//! Every schedule run prints its seed and log path to the test log:
//!
//! ```text
//! [fault-schedule] seed=18442198347553230518 log=target/fault-logs/e5-….jsonl
//! ```
//!
//! - **Same schedule**: `RR_FAULT_SEED=<n>` regenerates it from the seed.
//! - **Exact replay** (including any hand edits): `RR_FAULT_REPLAY=<path>`
//!   re-applies the recorded events at their recorded offsets, ignoring the
//!   seed entirely.
//!
//! # Schema
//!
//! One JSON object per line: `{ts_ms, offset_ms, node, kind, value, seed}`.
//! Same family as the crash harness's `fault-log.jsonl` (tests/crash) and
//! the R-8 soak fault log — one replay-facility lineage.
//!
//! # Randomness discipline
//!
//! All choices are drawn from a `StdRng` seeded at construction. The apply
//! loop adds no randomness of its own, so plan == record == replay. (The
//! pre-existing per-call `rand::random` in packet-loss evaluation is a
//! separate, unchanged code path — schedules never depend on its draws.)

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use rr_core::types::NodeId;

use crate::fault_injection::FaultInjector;

/// The fault primitives a schedule can apply. Mirrors the
/// [`FaultInjector`] API surface (isolate/restore/loss/latency/clear).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FaultKind {
    Isolate,
    Restore,
    PacketLoss { probability: f64 },
    Latency { delay_ms: u64 },
    ClearAll,
}

/// One scheduled (and later, applied) fault event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultEvent {
    /// Wall-clock time the apply loop actually fired (ms since epoch).
    /// Informational on replay (the offset sequence is authoritative).
    pub ts_ms: u64,
    /// Milliseconds after schedule start at which to apply.
    pub offset_ms: u64,
    /// Target node; `None` for cluster-wide kinds (ClearAll).
    pub node: Option<NodeId>,
    #[serde(flatten)]
    pub kind: FaultKind,
    /// The seed that generated this event (0 for replayed/hand-written logs).
    pub seed: u64,
}

/// A deterministically generated (or replayed) fault schedule.
pub struct FaultSchedule {
    pub seed: u64,
    pub events: Vec<FaultEvent>,
}

/// Knobs for schedule generation. Durations are milliseconds.
pub struct ScheduleConfig {
    /// Number of isolate/restore rounds to generate.
    pub rounds: u32,
    /// Fault duration range per round (isolate stays active this long).
    pub isolate_ms: (u64, u64),
    /// Heal-gap range between rounds.
    pub gap_ms: (u64, u64),
    /// Candidate target nodes (e.g. followers only — the caller chooses the
    /// pool, so a test can guarantee the leader is never isolated).
    pub pool: Vec<NodeId>,
}

impl FaultSchedule {
    /// Generate a schedule from `seed`: `rounds` rounds of
    /// `isolate(pool[i])` … `restore` … gap, with targets drawn round-robin
    /// from a seeded shuffle and durations from the configured ranges.
    ///
    /// Round-robin over a shuffled pool (rather than independent draws) keeps
    /// every node equally exposed across a run while staying seed-deterministic.
    pub fn generate(seed: u64, cfg: &ScheduleConfig) -> Self {
        use rand::seq::SliceRandom;
        use rand::{Rng, SeedableRng};

        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut pool = cfg.pool.clone();
        pool.shuffle(&mut rng);

        let mut events = Vec::with_capacity(cfg.rounds as usize * 2);
        let mut t = 0u64;
        for round in 0..cfg.rounds {
            let node = pool[(round as usize) % pool.len()];
            let isolate_for =
                rng.gen_range(cfg.isolate_ms.0..=cfg.isolate_ms.1.max(cfg.isolate_ms.0));
            let gap = rng.gen_range(cfg.gap_ms.0..=cfg.gap_ms.1.max(cfg.gap_ms.0));
            events.push(FaultEvent {
                ts_ms: 0,
                offset_ms: t,
                node: Some(node),
                kind: FaultKind::Isolate,
                seed,
            });
            t += isolate_for;
            events.push(FaultEvent {
                ts_ms: 0,
                offset_ms: t,
                node: Some(node),
                kind: FaultKind::Restore,
                seed,
            });
            t += gap;
        }
        Self { seed, events }
    }

    /// Load a schedule from a recorded fault log (replay). Offsets, targets
    /// and kinds are taken verbatim; `ts_ms` is ignored on replay.
    pub fn from_replay_file(path: &Path) -> std::io::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut events = Vec::new();
        let mut seed = 0u64;
        for (lineno, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let ev: FaultEvent = serde_json::from_str(line).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{}:{}: {e}", path.display(), lineno + 1),
                )
            })?;
            if seed == 0 && ev.seed != 0 {
                seed = ev.seed;
            }
            events.push(ev);
        }
        Ok(Self { seed, events })
    }

    /// Total scheduled duration (offset of the last event).
    pub fn duration_ms(&self) -> u64 {
        self.events.last().map(|e| e.offset_ms).unwrap_or(0)
    }
}

/// Where fault logs land. `target/fault-logs/<hint>-<millis>.jsonl` — outside
/// the package dir so CI can archive the whole folder on failure.
pub fn fault_log_path(hint: &str) -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/fault-logs");
    let _ = std::fs::create_dir_all(&base);
    base.join(format!("{hint}-{}.jsonl", now_ms()))
}

/// Resolve the schedule for a test run: replay file wins, then a pinned
/// seed, then entropy. Always records applied events to `log_path`.
///
/// Returns `(schedule, log_path)`.
pub fn resolve_schedule(
    hint: &str,
    generated: impl FnOnce(u64) -> FaultSchedule,
) -> (FaultSchedule, PathBuf) {
    if let Ok(path) = std::env::var("RR_FAULT_REPLAY") {
        let schedule = FaultSchedule::from_replay_file(Path::new(&path))
            .unwrap_or_else(|e| panic!("RR_FAULT_REPLAY={path}: {e}"));
        // Replay re-records into a FRESH log: the source file is a forensic
        // artifact and must not be truncated by the replay run.
        let log_path = fault_log_path(&format!("{hint}-replay"));
        eprintln!(
            "[fault-schedule] REPLAY {path} ({} events, {}ms) → re-record {}",
            schedule.events.len(),
            schedule.duration_ms(),
            log_path.display(),
        );
        return (schedule, log_path);
    }
    let seed = std::env::var("RR_FAULT_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or_else(rand::random);
    let schedule = generated(seed);
    let log_path = fault_log_path(hint);
    eprintln!(
        "[fault-schedule] seed={seed} log={} (replay: RR_FAULT_REPLAY={} | reseed: RR_FAULT_SEED={seed})",
        log_path.display(),
        log_path.display(),
    );
    (schedule, log_path)
}

/// Apply a schedule against an injector, recording every fired event to the
/// JSONL log (fdatasync per record — the log is the forensic artifact).
///
/// Runs the events on the calling task (sequential sleeps); spawn it if the
/// test needs to do other work concurrently.
pub async fn apply_schedule(schedule: &FaultSchedule, injector: &FaultInjector, log_path: &Path) {
    let started = std::time::Instant::now();
    let mut log = LogWriter::create(log_path);
    for event in &schedule.events {
        let target_ms = event.offset_ms;
        let elapsed = started.elapsed().as_millis() as u64;
        if target_ms > elapsed {
            tokio::time::sleep(std::time::Duration::from_millis(target_ms - elapsed)).await;
        }
        apply_one(injector, event);
        log.record(event);
    }
}

fn apply_one(injector: &FaultInjector, event: &FaultEvent) {
    match event.kind {
        FaultKind::Isolate => injector.isolate(event.node.expect("isolate needs a node")),
        FaultKind::Restore => injector.restore(event.node.expect("restore needs a node")),
        FaultKind::PacketLoss { probability } => {
            injector.set_packet_loss(event.node.expect("loss needs a node"), probability)
        }
        FaultKind::Latency { delay_ms } => {
            injector.set_latency(event.node.expect("latency needs a node"), delay_ms)
        }
        FaultKind::ClearAll => injector.clear_all(),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Append-only JSONL writer with fdatasync-per-record (same discipline as
/// the crash harness's journal: the record is the forensic ground truth).
struct LogWriter {
    file: std::fs::File,
}

impl LogWriter {
    fn create(path: &Path) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .unwrap_or_else(|e| panic!("create fault log {}: {e}", path.display()));
        Self { file }
    }

    fn record(&mut self, event: &FaultEvent) {
        use std::io::Write;
        let mut rec = event.clone();
        rec.ts_ms = now_ms();
        let mut line = serde_json::to_vec(&rec).expect("fault event serializes");
        line.push(b'\n');
        self.file.write_all(&line).expect("fault log write");
        self.file.sync_data().expect("fault log fdatasync");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> Vec<NodeId> {
        vec![1, 2, 3]
    }

    fn cfg() -> ScheduleConfig {
        ScheduleConfig {
            rounds: 6,
            isolate_ms: (100, 200),
            gap_ms: (50, 100),
            pool: pool(),
        }
    }

    #[test]
    fn same_seed_same_schedule() {
        let a = FaultSchedule::generate(42, &cfg());
        let b = FaultSchedule::generate(42, &cfg());
        assert_eq!(a.events.len(), b.events.len());
        for (x, y) in a.events.iter().zip(&b.events) {
            assert_eq!(x.offset_ms, y.offset_ms);
            assert_eq!(x.node, y.node);
            assert_eq!(x.kind, y.kind);
        }
    }

    #[test]
    fn different_seeds_usually_differ() {
        let a = FaultSchedule::generate(1, &cfg());
        let b = FaultSchedule::generate(2, &cfg());
        let same = a
            .events
            .iter()
            .zip(&b.events)
            .all(|(x, y)| x.node == y.node && x.offset_ms == y.offset_ms);
        assert!(!same, "distinct seeds should produce distinct schedules");
    }

    #[test]
    fn round_robin_covers_pool() {
        let s = FaultSchedule::generate(7, &cfg());
        let isolated: std::collections::HashSet<_> = s
            .events
            .iter()
            .filter(|e| e.kind == FaultKind::Isolate)
            .map(|e| e.node.unwrap())
            .collect();
        assert_eq!(isolated.len(), pool().len(), "6 rounds over 3 nodes");
    }

    #[test]
    fn schedule_offsets_monotonic() {
        let s = FaultSchedule::generate(99, &cfg());
        for w in s.events.windows(2) {
            assert!(w[1].offset_ms >= w[0].offset_ms);
        }
    }

    #[test]
    fn replay_roundtrip_preserves_schedule() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("fault-log.jsonl");
        let original = FaultSchedule::generate(4242, &cfg());

        // Write the log the way the apply loop does.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let injector = FaultInjector::new();
            apply_schedule(&original, &injector, &path).await;
        });

        let replayed = FaultSchedule::from_replay_file(&path).unwrap();
        assert_eq!(replayed.seed, 4242);
        assert_eq!(replayed.events.len(), original.events.len());
        for (x, y) in original.events.iter().zip(&replayed.events) {
            assert_eq!(x.offset_ms, y.offset_ms);
            assert_eq!(x.node, y.node);
            assert_eq!(x.kind, y.kind);
        }
    }

    #[tokio::test]
    async fn apply_schedule_drives_injector() {
        let injector = FaultInjector::new();
        let schedule = FaultSchedule {
            seed: 1,
            events: vec![
                FaultEvent {
                    ts_ms: 0,
                    offset_ms: 0,
                    node: Some(2),
                    kind: FaultKind::Isolate,
                    seed: 1,
                },
                FaultEvent {
                    ts_ms: 0,
                    offset_ms: 10,
                    node: Some(2),
                    kind: FaultKind::Restore,
                    seed: 1,
                },
            ],
        };
        let tmp = tempfile::TempDir::new().unwrap();
        apply_schedule(&schedule, &injector, &tmp.path().join("log.jsonl")).await;
        assert!(!injector.is_isolated(2), "restore must have been applied");

        // The log is replayable.
        let replayed = FaultSchedule::from_replay_file(&tmp.path().join("log.jsonl")).unwrap();
        assert_eq!(replayed.events.len(), 2);
    }
}
