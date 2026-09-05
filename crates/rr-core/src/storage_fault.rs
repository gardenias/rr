//! Fault-injecting [`StorageEngine`] wrapper (reliability program R-4, level
//! L1).
//!
//! [`FaultInjectingStorage`] wraps any concrete [`StorageEngine`] and
//! delegates every call to it, optionally injecting one of four
//! **independently programmable** storage faults first:
//!
//! | Mode | Simulates | Effect when it fires |
//! |------|-----------|----------------------|
//! | `fail_flush` | failed fsync / flush to durable media | [`StorageEngine::flush`] returns `Err(StorageError::Io(..))` |
//! | `torn_write` | crash mid-`write_batch` (partial apply) | only a prefix of the batch's ops is applied to the inner engine, then `Err(StorageError::Io(..))` is returned |
//! | `enospc` | full disk | the write paths ([`StorageEngine::put`], [`StorageEngine::write_batch`], [`StorageEngine::delete`]) return an ENOSPC-class `Err(StorageError::Io(..))` ([`std::io::ErrorKind::StorageFull`]) |
//! | `latency` | slow disk | a configured delay is injected before **every** method call |
//!
//! # Test-only
//!
//! This wrapper exists for tests (unit, proptest, integration) and is gated
//! behind the crate feature `test-utils`. It must never be compiled into a
//! production server build.
//!
//! # Determinism
//!
//! All randomness (`Probability` triggers, `TornPrefix::Random`) is drawn from
//! a [SplitMix64](https://prng.di.unimi.it/splitmix64.c) generator seeded at
//! construction ([`FaultConfig::seed`]). For a fixed seed **and a fixed
//! sequence of wrapper calls**, the fired-fault sequence is identical on every
//! run and every platform (the generator is pure integer arithmetic — no
//! floats, no OS entropy, no time). Arm/disarm calls do not consume randomness
//! and do not reset the generator or the per-method call counters.
//!
//! # Composition order
//!
//! When several modes are armed at once they compose in a fixed, documented
//! order: `latency` (delay) always comes first; on the write paths an armed
//! `enospc` takes precedence over an armed `torn_write` (a full disk reports
//! failure without applying anything). Reads (`get`/`scan`) and `create_cf`
//! are only ever affected by `latency`.
//!
//! # Example
//!
//! ```ignore
//! let engine = MemStorage::new();
//! let storage = FaultInjectingStorage::new(
//!     engine,
//!     FaultConfig::seeded(42).with_enospc(Trigger::EveryCall),
//! );
//! // Every write now fails with ENOSPC until disarmed:
//! assert!(storage.put("state_machine", b"k", b"v").await.is_err());
//! storage.disarm_all();
//! storage.put("state_machine", b"k", b"v").await.unwrap();
//! ```

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::error::StorageError;
use crate::storage::{BatchOp, StorageEngine};

// ── Fault configuration ──────────────────────────────────────────────────────

/// When an armed fault fires.
///
/// The per-method call counters used by [`Trigger::AfterCalls`] start at
/// construction and are never reset by arm/disarm calls, so "the first `n`
/// calls" always refers to the wrapper's lifetime, not to the arming moment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Trigger {
    /// Fire on every call to the faulted method.
    EveryCall,
    /// The first `n` calls behave normally; every call after that faults.
    /// `AfterCalls(0)` is equivalent to [`Trigger::EveryCall`].
    AfterCalls(u64),
    /// Fire on each call with probability `p`, drawn from the wrapper's
    /// seeded generator (deterministic for a given seed and call sequence).
    /// Values outside `[0.0, 1.0]` are clamped.
    Probability(f64),
}

/// How many leading ops a fired `torn_write` applies before reporting
/// failure.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TornPrefix {
    /// Apply exactly the first `k` ops of the batch (clamped to the batch
    /// length), then fail. `Fixed(0)` fails without applying anything.
    Fixed(usize),
    /// Draw the prefix length uniformly from `[0, ops.len()]` from the
    /// seeded generator — deterministic for a given seed and call sequence.
    Random,
}

/// Configuration of the `torn_write` fault mode.
///
/// Mirrors — deliberately, as an *explicit, configurable* fault — the
/// partial-apply semantics `MemStorage::write_batch`
/// (`crates/rr-storage/src/mem.rs`) exhibits when it errors mid-loop: ops
/// applied before the failure stay applied and are NOT rolled back. A real
/// crash inside `RocksDB::write_opt` can leave the same observable state
/// (WAL replay applies a prefix of the batch).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TornWrite {
    /// When the fault fires.
    pub trigger: Trigger,
    /// How many leading ops get applied before the error is returned.
    pub prefix: TornPrefix,
}

/// Seed-programmable fault configuration for [`FaultInjectingStorage`].
///
/// All modes default to disarmed. `Default` uses seed `0`; prefer
/// [`FaultConfig::seeded`] so a failing run can be replayed exactly.
#[derive(Debug, Clone, Default)]
pub struct FaultConfig {
    /// Seed for the internal SplitMix64 generator. See the module-level
    /// determinism guarantee.
    pub seed: u64,
    /// Armed: `flush()` fails with [`StorageError::Io`] (failed fsync).
    pub fail_flush: Option<Trigger>,
    /// Armed: `write_batch` applies only a prefix of its ops, then fails.
    pub torn_write: Option<TornWrite>,
    /// Armed: write paths (`put`/`write_batch`/`delete`) fail with an
    /// ENOSPC-class [`StorageError::Io`].
    pub enospc: Option<Trigger>,
    /// Armed: delay injected before every method call (slow disk).
    pub latency: Option<Duration>,
}

impl FaultConfig {
    /// An all-disarmed config with the given seed.
    pub fn seeded(seed: u64) -> Self {
        Self {
            seed,
            ..Default::default()
        }
    }

    /// Builder: arm `fail_flush`.
    pub fn with_fail_flush(mut self, trigger: Trigger) -> Self {
        self.fail_flush = Some(trigger);
        self
    }

    /// Builder: arm `torn_write`.
    pub fn with_torn_write(mut self, torn_write: TornWrite) -> Self {
        self.torn_write = Some(torn_write);
        self
    }

    /// Builder: arm `enospc`.
    pub fn with_enospc(mut self, trigger: Trigger) -> Self {
        self.enospc = Some(trigger);
        self
    }

    /// Builder: arm `latency`.
    pub fn with_latency(mut self, delay: Duration) -> Self {
        self.latency = Some(delay);
        self
    }
}

// ── Seeded RNG (SplitMix64) ─────────────────────────────────────────────────

/// SplitMix64: 64-bit state, public-domain, platform-independent. Chosen over
/// pulling in `rand` so this test-only module adds no dependency surface to
/// `rr-core`; the determinism guarantee is obvious from ~10 lines of integer
/// arithmetic.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform double in `[0, 1)` (top 53 bits, IEEE-754 — the standard
    /// `next_double` construction).
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

// ── Internal state ───────────────────────────────────────────────────────────

/// The wrapper methods, used to index the per-method call counters that drive
/// [`Trigger::AfterCalls`].
#[derive(Debug, Clone, Copy)]
enum Method {
    Put,
    Get,
    Delete,
    Scan,
    WriteBatch,
    Flush,
    CreateCf,
}

impl Method {
    const COUNT: usize = 7;
}

impl From<Method> for usize {
    fn from(m: Method) -> usize {
        match m {
            Method::Put => 0,
            Method::Get => 1,
            Method::Delete => 2,
            Method::Scan => 3,
            Method::WriteBatch => 4,
            Method::Flush => 5,
            Method::CreateCf => 6,
        }
    }
}

struct FaultState {
    config: FaultConfig,
    rng: SplitMix64,
    counts: [u64; Method::COUNT],
}

impl FaultState {
    /// Evaluate `trigger` against `method`'s (already bumped) call count,
    /// drawing from the seeded generator for probability triggers.
    fn fires(&mut self, method: Method, trigger: Trigger) -> bool {
        match trigger {
            Trigger::EveryCall => true,
            Trigger::AfterCalls(n) => self.counts[usize::from(method)] > n,
            Trigger::Probability(p) => self.rng.next_f64() < p.clamp(0.0, 1.0),
        }
    }
}

// ── The wrapper ──────────────────────────────────────────────────────────────

/// A fault-injecting [`StorageEngine`] wrapper — see the module docs for the
/// four fault modes, the composition order, and the seeded-determinism
/// guarantee.
///
/// The wrapper is `Send + Sync` (fault state lives behind a `Mutex`, held
/// only for synchronous decision-making — never across an `.await`), so an
/// `Arc<FaultInjectingStorage<S>>` coerces to `Arc<dyn StorageEngine>` and
/// slots into any engine-typed consumer, including `rr_raft`'s `IoBatcher`
/// group-commit worker.
pub struct FaultInjectingStorage<S: StorageEngine> {
    inner: S,
    state: Arc<Mutex<FaultState>>,
}

impl<S: StorageEngine> FaultInjectingStorage<S> {
    /// Wrap `inner`. All faults start as configured in `config` (all disarmed
    /// for [`FaultConfig::seeded`]); the seed initialises the generator.
    pub fn new(inner: S, config: FaultConfig) -> Self {
        let seed = config.seed;
        Self {
            inner,
            state: Arc::new(Mutex::new(FaultState {
                config,
                rng: SplitMix64(seed),
                counts: [0; Method::COUNT],
            })),
        }
    }

    /// The wrapped engine, for white-box assertions in tests.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// A snapshot of the current fault configuration.
    pub fn config(&self) -> FaultConfig {
        self.state.lock().unwrap().config.clone()
    }

    /// Arm `fail_flush` at runtime (failed fsync on `flush()`).
    pub fn arm_fail_flush(&self, trigger: Trigger) {
        self.state.lock().unwrap().config.fail_flush = Some(trigger);
    }

    /// Arm `torn_write` at runtime (partial-apply + error on `write_batch`).
    pub fn arm_torn_write(&self, torn_write: TornWrite) {
        self.state.lock().unwrap().config.torn_write = Some(torn_write);
    }

    /// Arm `enospc` at runtime (full disk on the write paths).
    pub fn arm_enospc(&self, trigger: Trigger) {
        self.state.lock().unwrap().config.enospc = Some(trigger);
    }

    /// Arm `latency` at runtime (delay before every method).
    pub fn arm_latency(&self, delay: Duration) {
        self.state.lock().unwrap().config.latency = Some(delay);
    }

    /// Disarm every fault mode. Does not reset the call counters or the
    /// generator, so determinism is preserved across arm/disarm cycles.
    pub fn disarm_all(&self) {
        let mut state = self.state.lock().unwrap();
        state.config.fail_flush = None;
        state.config.torn_write = None;
        state.config.enospc = None;
        state.config.latency = None;
    }

    /// Bump `method`'s call counter (counters run from construction,
    /// independent of arming) and return a snapshot of the armed config.
    fn note_call(&self, method: Method) -> FaultConfig {
        let mut state = self.state.lock().unwrap();
        state.counts[usize::from(method)] += 1;
        state.config.clone()
    }

    /// Evaluate an armed trigger for a call that was just counted.
    fn fires(&self, method: Method, trigger: Option<Trigger>) -> bool {
        match trigger {
            None => false,
            Some(trigger) => self.state.lock().unwrap().fires(method, trigger),
        }
    }

    /// Sleep for the armed `latency` delay, if any (slow disk). Applies to
    /// every wrapper method.
    async fn inject_latency(&self, config: &FaultConfig) {
        if let Some(delay) = config.latency {
            tokio::time::sleep(delay).await;
        }
    }
}

fn fail_flush_error() -> StorageError {
    StorageError::Io(std::io::Error::other(
        "injected fault: flush failed (simulated fsync failure)",
    ))
}

fn torn_write_error() -> StorageError {
    StorageError::Io(std::io::Error::other(
        "injected fault: write_batch torn (crash mid-batch, partial apply not rolled back)",
    ))
}

fn enospc_error() -> StorageError {
    StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::StorageFull,
        "injected fault: no space left on device (ENOSPC)",
    ))
}

#[async_trait]
impl<S: StorageEngine> StorageEngine for FaultInjectingStorage<S> {
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let config = self.note_call(Method::Put);
        self.inject_latency(&config).await;
        if self.fires(Method::Put, config.enospc) {
            return Err(enospc_error());
        }
        self.inner.put(cf, key, value).await
    }

    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let config = self.note_call(Method::Get);
        self.inject_latency(&config).await;
        self.inner.get(cf, key).await
    }

    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), StorageError> {
        let config = self.note_call(Method::Delete);
        self.inject_latency(&config).await;
        if self.fires(Method::Delete, config.enospc) {
            return Err(enospc_error());
        }
        self.inner.delete(cf, key).await
    }

    async fn scan(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let config = self.note_call(Method::Scan);
        self.inject_latency(&config).await;
        self.inner.scan(cf, start, end).await
    }

    async fn write_batch(&self, ops: Vec<BatchOp>) -> Result<(), StorageError> {
        let config = self.note_call(Method::WriteBatch);
        self.inject_latency(&config).await;
        // Fixed composition order (module docs): a full disk fails the batch
        // without applying anything; only if it doesn't fire may the batch
        // tear.
        if self.fires(Method::WriteBatch, config.enospc) {
            return Err(enospc_error());
        }
        if let Some(torn) = config.torn_write {
            if self.fires(Method::WriteBatch, Some(torn.trigger)) {
                let prefix_len = match torn.prefix {
                    TornPrefix::Fixed(k) => k.min(ops.len()),
                    TornPrefix::Random => {
                        let draw = self.state.lock().unwrap().rng.next_u64();
                        (draw % (ops.len() as u64 + 1)) as usize
                    }
                };
                // Partial apply, NOT rolled back — the torn-write semantics
                // this fault exists to make explicit and configurable.
                let head = ops[..prefix_len].to_vec();
                if !head.is_empty() {
                    self.inner.write_batch(head).await?;
                }
                return Err(torn_write_error());
            }
        }
        self.inner.write_batch(ops).await
    }

    async fn flush(&self) -> Result<(), StorageError> {
        let config = self.note_call(Method::Flush);
        self.inject_latency(&config).await;
        if self.fires(Method::Flush, config.fail_flush) {
            return Err(fail_flush_error());
        }
        self.inner.flush().await
    }

    async fn create_cf(&self, name: &str) -> Result<(), StorageError> {
        let config = self.note_call(Method::CreateCf);
        self.inject_latency(&config).await;
        self.inner.create_cf(name).await
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Minimal in-memory engine for wrapper unit tests (rr-core deliberately
    /// carries no concrete storage backend).
    type KvMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

    #[derive(Default)]
    struct MockStorage {
        data: Mutex<KvMap>,
        flushes: Mutex<u64>,
    }

    #[async_trait]
    impl StorageEngine for MockStorage {
        async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
            self.data
                .lock()
                .unwrap()
                .insert((cf.to_string(), key.to_vec()), value.to_vec());
            Ok(())
        }

        async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .get(&(cf.to_string(), key.to_vec()))
                .cloned())
        }

        async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), StorageError> {
            self.data
                .lock()
                .unwrap()
                .remove(&(cf.to_string(), key.to_vec()));
            Ok(())
        }

        async fn scan(
            &self,
            cf: &str,
            start: &[u8],
            end: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .range((cf.to_string(), start.to_vec())..(cf.to_string(), end.to_vec()))
                .map(|((_, k), v)| (k.clone(), v.clone()))
                .collect())
        }

        async fn write_batch(&self, ops: Vec<BatchOp>) -> Result<(), StorageError> {
            for op in ops {
                match op {
                    BatchOp::Put { cf, key, value } => self.put(&cf, &key, &value).await?,
                    BatchOp::Delete { cf, key } => self.delete(&cf, &key).await?,
                }
            }
            Ok(())
        }

        async fn flush(&self) -> Result<(), StorageError> {
            *self.flushes.lock().unwrap() += 1;
            Ok(())
        }

        async fn create_cf(&self, _name: &str) -> Result<(), StorageError> {
            Ok(())
        }
    }

    fn put_op(i: u64) -> BatchOp {
        BatchOp::Put {
            cf: "state_machine".to_string(),
            key: i.to_be_bytes().to_vec(),
            value: format!("val-{i}").into_bytes(),
        }
    }

    #[tokio::test]
    async fn disarmed_wrapper_delegates_everything() {
        let storage = FaultInjectingStorage::new(MockStorage::default(), FaultConfig::seeded(7));
        storage.create_cf("state_machine").await.unwrap();
        storage.put("state_machine", b"k", b"v").await.unwrap();
        assert_eq!(
            storage.get("state_machine", b"k").await.unwrap(),
            Some(b"v".to_vec())
        );
        storage
            .write_batch(vec![put_op(1), put_op(2)])
            .await
            .unwrap();
        assert_eq!(
            storage
                .scan("state_machine", &1u64.to_be_bytes(), &3u64.to_be_bytes())
                .await
                .unwrap()
                .len(),
            2
        );
        storage.delete("state_machine", b"k").await.unwrap();
        assert_eq!(storage.get("state_machine", b"k").await.unwrap(), None);
        storage.flush().await.unwrap();
        assert_eq!(*storage.inner().flushes.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn fail_flush_errors_then_recovers_after_disarm() {
        let storage = FaultInjectingStorage::new(
            MockStorage::default(),
            FaultConfig::seeded(1).with_fail_flush(Trigger::EveryCall),
        );
        let err = storage.flush().await.unwrap_err();
        assert!(
            matches!(err, StorageError::Io(_)),
            "fail_flush must surface StorageError::Io, got {err:?}"
        );
        // Writes are unaffected by fail_flush, and disarming restores flush.
        storage.put("state_machine", b"k", b"v").await.unwrap();
        storage.disarm_all();
        storage.flush().await.unwrap();
    }

    #[tokio::test]
    async fn fail_flush_after_calls_lets_first_calls_pass() {
        let storage = FaultInjectingStorage::new(
            MockStorage::default(),
            FaultConfig::seeded(1).with_fail_flush(Trigger::AfterCalls(2)),
        );
        storage.flush().await.unwrap();
        storage.flush().await.unwrap();
        assert!(storage.flush().await.is_err(), "third flush must fault");
        assert!(storage.flush().await.is_err(), "and keeps faulting");
    }

    #[tokio::test]
    async fn enospc_hits_all_write_paths_with_storage_full() {
        let storage = FaultInjectingStorage::new(
            MockStorage::default(),
            FaultConfig::seeded(3).with_enospc(Trigger::EveryCall),
        );
        for result in [
            storage.put("state_machine", b"k", b"v").await,
            storage.write_batch(vec![put_op(1)]).await,
            storage.delete("state_machine", b"k").await,
        ] {
            match result {
                Err(StorageError::Io(e)) => assert_eq!(
                    e.kind(),
                    std::io::ErrorKind::StorageFull,
                    "ENOSPC faults must be StorageFull-class: {e}"
                ),
                other => panic!("write path must fail under ENOSPC, got {other:?}"),
            }
        }
        // Reads are not write paths: unaffected.
        assert_eq!(storage.get("state_machine", b"k").await.unwrap(), None);
        // Nothing was applied.
        storage.disarm_all();
        assert_eq!(storage.get("state_machine", b"k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn torn_write_applies_prefix_then_errors() {
        let storage = FaultInjectingStorage::new(
            MockStorage::default(),
            FaultConfig::seeded(5).with_torn_write(TornWrite {
                trigger: Trigger::EveryCall,
                prefix: TornPrefix::Fixed(2),
            }),
        );
        let err = storage
            .write_batch(vec![put_op(0), put_op(1), put_op(2)])
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::Io(_)));
        // Honest post-state: the prefix (ops 0,1) is applied and NOT rolled
        // back; the suffix (op 2) is absent.
        assert_eq!(
            storage
                .get("state_machine", &0u64.to_be_bytes())
                .await
                .unwrap(),
            Some(b"val-0".to_vec())
        );
        assert_eq!(
            storage
                .get("state_machine", &1u64.to_be_bytes())
                .await
                .unwrap(),
            Some(b"val-1".to_vec())
        );
        assert_eq!(
            storage
                .get("state_machine", &2u64.to_be_bytes())
                .await
                .unwrap(),
            None
        );
        // The wrapper keeps serving: a disarmed retry lands the full batch.
        storage.disarm_all();
        storage
            .write_batch(vec![put_op(0), put_op(1), put_op(2)])
            .await
            .unwrap();
        assert_eq!(
            storage
                .get("state_machine", &2u64.to_be_bytes())
                .await
                .unwrap(),
            Some(b"val-2".to_vec())
        );
    }

    #[tokio::test]
    async fn torn_write_random_prefix_stays_in_bounds() {
        let storage = FaultInjectingStorage::new(
            MockStorage::default(),
            FaultConfig::seeded(11).with_torn_write(TornWrite {
                trigger: Trigger::EveryCall,
                prefix: TornPrefix::Random,
            }),
        );
        for round in 0..20u64 {
            let base = round * 10;
            let ops: Vec<BatchOp> = (base..base + 5).map(put_op).collect();
            assert!(storage.write_batch(ops).await.is_err());
            let applied = storage
                .scan(
                    "state_machine",
                    &base.to_be_bytes(),
                    &(base + 5).to_be_bytes(),
                )
                .await
                .unwrap();
            // Prefix property: the applied keys are exactly 0..=k of the
            // batch, no gaps, never more than the batch length.
            for (offset, (k, _)) in applied.iter().enumerate() {
                assert_eq!(k.as_slice(), (base + offset as u64).to_be_bytes());
            }
        }
    }

    #[tokio::test]
    async fn probability_triggers_are_deterministic_per_seed() {
        async fn fired_pattern(seed: u64) -> Vec<bool> {
            let storage = FaultInjectingStorage::new(
                MockStorage::default(),
                FaultConfig::seeded(seed).with_fail_flush(Trigger::Probability(0.5)),
            );
            let mut pattern = Vec::new();
            for _ in 0..64 {
                pattern.push(storage.flush().await.is_err());
            }
            pattern
        }
        // Same seed, same call sequence → identical fault sequence.
        assert_eq!(fired_pattern(42).await, fired_pattern(42).await);
        // Sanity: at p=0.5 over 64 draws the pattern is neither all-fault
        // nor all-pass (overwhelmingly likely; pinned by the fixed seed).
        let pattern = fired_pattern(42).await;
        assert!(pattern.iter().any(|&f| f) && pattern.iter().any(|&f| !f));
    }

    #[tokio::test]
    async fn latency_delays_calls_without_breaking_them() {
        let delay = Duration::from_millis(20);
        let storage = FaultInjectingStorage::new(
            MockStorage::default(),
            FaultConfig::seeded(9).with_latency(delay),
        );
        let start = std::time::Instant::now();
        storage.put("state_machine", b"k", b"v").await.unwrap();
        // tokio's timer resolution can land a touch early; allow 2 ms slack.
        assert!(
            start.elapsed() + Duration::from_millis(2) >= delay,
            "latency fault must actually delay the call"
        );
        assert_eq!(
            storage.get("state_machine", b"k").await.unwrap(),
            Some(b"v".to_vec())
        );
    }

    #[tokio::test]
    async fn enospc_takes_precedence_over_torn_write() {
        let storage = FaultInjectingStorage::new(
            MockStorage::default(),
            FaultConfig::seeded(13)
                .with_enospc(Trigger::EveryCall)
                .with_torn_write(TornWrite {
                    trigger: Trigger::EveryCall,
                    prefix: TornPrefix::Fixed(1),
                }),
        );
        assert!(storage
            .write_batch(vec![put_op(0), put_op(1)])
            .await
            .is_err());
        // Documented composition: ENOSPC fails without applying anything —
        // no prefix may leak through.
        assert_eq!(
            storage
                .get("state_machine", &0u64.to_be_bytes())
                .await
                .unwrap(),
            None
        );
    }
}
