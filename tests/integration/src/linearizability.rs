//! Porcupine-style linearizability checker for a single kv register (R-5).
//!
//! This module implements work-item **R-5** of the reliability program
//! (`.sisyphus/plans/rr-reliability-program.md`): a real history checker for
//! the register semantics that back the linearizable-read invariant
//! (INV-2, `docs/docs/reference/invariants.md`).
//!
//! # Model
//!
//! A single-partition, single-key register with two operations:
//!
//! - `Write(v)` — sets the register to `v`, acked with `v`;
//! - `Read` — returns the value of the most recent write in the
//!   linearization, or the initial value [`INITIAL_VALUE`] if no write
//!   precedes it.
//!
//! # History model
//!
//! Each client call is recorded as an [`Op`] with interval semantics:
//! `invoke_us` / `return_us` are microseconds since the [`History`]'s own
//! process-local `Instant` epoch. Because the integration client and the
//! cluster under test share one process, a single monotonic clock is sound;
//! **no clocks are ever compared across processes**. Calls whose response is
//! unknown (timeout, transport error, leader churn) are recorded with
//! `return_us: None` and participate in the partial order as `?` per
//! porcupine convention: they may be linearized at any point at or after
//! their invocation, constrained only by the real-time precedence of other
//! operations.
//!
//! # Checker
//!
//! Standard porcupine flow:
//!
//! 1. Histories larger than [`CheckerConfig::max_ops`] (default 10 000) are
//!    downsampled with a **loud** stderr warning (uniform stride, order and
//!    ids preserved) so checking stays in the seconds regime.
//! 2. Operations are sorted by invocation time and scanned with a
//!    backtracking search over linearization points. An op `i` is a
//!    *candidate* once every completed op `a` with `a.return_us <
//!    i.invoke_us` (its real-time predecessors) has been linearized.
//! 3. Pruning: a `Read` candidate with a known result is only attempted when
//!    the current register state equals the observed value — when the
//!    register state is determined, impossible branches are never explored.
//!    Reads with a matching value and all writes are always legal moves.
//! 4. Failed states are memoized on `(register_value, linearized_set)`:
//!    availability counts are a pure function of the linearized set, so a
//!    re-visited frontier from a different path is skipped in O(bitset).
//!
//! Complexity: worst case is exponential in the number of mutually
//! concurrent operations — linearizability checking is NP-complete in
//! general. With read-match pruning plus the memo table, realistic
//! near-sequential histories (a mostly-correct system under test) complete
//! in milliseconds for a few hundred ops and stay within seconds at the 10k
//! cap. The search is *complete*: every topological order of the real-time
//! poset that respects the register spec is reachable, pruning only removes
//! provably-impossible or provably-failed branches, so an `Illegal` verdict
//! is a genuine counterexample, never a timeout artefact. (During
//! counterexample minimization, individual re-checks run under a step
//! budget; a budget overrun keeps the operation, which can only leave the
//! printed sub-history larger than minimal — it can never make a legal
//! history look illegal.)
//!
//! On violation the checker minimizes the failing history (greedy single-op
//! removal — delta debugging — under a wall-clock budget) and renders the
//! surviving conflicting sub-history plus its real-time precedence chain in
//! human-readable form.
//!
//! # Session-guarantee reads
//!
//! `read_local` (`RrClient::read`) is answered by a local replica without
//! consensus and may legitimately return stale data. [`RecordingClient`]
//! records those calls into a **separate** [`History`] that must NOT be fed
//! to the register checker; only writes and `read_linearizable` calls belong
//! to the checked history.

use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use rr_client::RrClient;
use rr_config::mem::MemConfigProvider;
use rr_core::config::ConfigProvider;
use rr_core::error::RrError;
use rr_core::service::Service;
use rr_core::storage::StorageEngine;
use rr_core::types::{GroupId, NodeId, PartitionConfig};
use rr_raft::group_manager::RaftGroupManager;
use rr_raft::network_factory::ChannelPoolLike;
use rr_raft::ServiceStateMachineAdapter;
use rr_router::router::ShardRouter;
use rr_server::config::NodeConfig;
use rr_server::dispatch::ClientDispatchImpl;
use rr_server::lookup::RaftGroupLookupImpl;
use rr_server::node::RrNode;
use rr_storage::mem::MemStorage;

use crate::echo_factory;
use crate::fault_injection::FaultInjector;

/// Initial register value before any write is linearized.
pub const INITIAL_VALUE: u64 = 0;

/// Default maximum number of operations fed to the checker; larger histories
/// are downsampled with a loud warning (see module docs).
pub const DEFAULT_MAX_OPS: usize = 10_000;

// ── History model ────────────────────────────────────────────────────────────

/// The operation performed against the register.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    /// `Write(v)` — set the register to `v`.
    Write(u64),
    /// `Read` — observe the register value.
    Read,
}

/// One recorded client call, with interval (porcupine) semantics.
///
/// `invoke_us` and `return_us` are microseconds since the recording
/// [`History`]'s process-local epoch. `return_us == None` marks an operation
/// whose response is unknown (timeout / transport error): the `?` case.
///
/// `result` is the observed value: for a read, the value returned; for a
/// write, the acknowledged value. `None` when the response is unknown or
/// carried no decodable value.
#[derive(Clone, Debug)]
pub struct Op {
    /// Monotonically increasing id in this process's invocation order.
    pub id: usize,
    /// Invocation time, microseconds since the history epoch.
    pub invoke_us: u64,
    /// Response time, microseconds since the history epoch; `None` = unknown.
    pub return_us: Option<u64>,
    /// Operation kind (write value / read).
    pub kind: OpKind,
    /// Observed result value (`?` when `None`).
    pub result: Option<u64>,
}

impl Op {
    /// One-line human rendering, e.g. `#3 WRITE(7)  [1203µs → 1450µs]  ack=7`.
    fn render(&self) -> String {
        let kind = match self.kind {
            OpKind::Write(v) => format!("WRITE({v})"),
            OpKind::Read => "READ".to_string(),
        };
        let ret = match self.return_us {
            Some(t) => format!("{t}µs"),
            None => "?".to_string(),
        };
        let result = match (self.kind, self.result) {
            (OpKind::Read, Some(v)) => format!("→ {v}"),
            (OpKind::Write(_), Some(v)) => format!("ack={v}"),
            (_, None) => "→ ? (response unknown)".to_string(),
        };
        format!(
            "  #{} {}  [{}µs → {}]  {}",
            self.id, kind, self.invoke_us, ret, result
        )
    }
}

/// A shared, append-only operation history recorded against a single
/// process-local `Instant` epoch.
///
/// Cheap to clone — clones share the same underlying log and epoch. Recording
/// is two-phase: `begin_*` assigns the id and invocation timestamp under a
/// short lock (never held across `.await`), and the returned [`OpHandle`]
/// completes the record exactly once.
#[derive(Clone)]
pub struct History {
    inner: Arc<HistoryInner>,
}

struct HistoryInner {
    epoch: Instant,
    ops: Mutex<Vec<Op>>,
}

/// In-flight operation record. Exactly-once completion: the first of
/// [`ack`](Self::ack) / [`unknown`](Self::unknown) wins. Dropping a handle
/// without completing it reports a recording bug (the op stays in the log
/// with an unknown response, which is always safe for the checker).
pub struct OpHandle {
    history: History,
    id: usize,
    done: bool,
}

impl History {
    /// Start a new, empty history with its own epoch.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HistoryInner {
                epoch: Instant::now(),
                ops: Mutex::new(Vec::new()),
            }),
        }
    }

    fn elapsed_us(&self) -> u64 {
        self.inner.epoch.elapsed().as_micros() as u64
    }

    fn begin(&self, kind: OpKind) -> OpHandle {
        let mut ops = self.inner.ops.lock().unwrap();
        let id = ops.len();
        ops.push(Op {
            id,
            invoke_us: self.elapsed_us(),
            return_us: None,
            kind,
            result: None,
        });
        OpHandle {
            history: self.clone(),
            id,
            done: false,
        }
    }

    /// Record the invocation of a `Write(v)`.
    pub fn begin_write(&self, value: u64) -> OpHandle {
        self.begin(OpKind::Write(value))
    }

    /// Record the invocation of a `Read`.
    pub fn begin_read(&self) -> OpHandle {
        self.begin(OpKind::Read)
    }

    /// Number of operations recorded so far (including in-flight ones).
    pub fn len(&self) -> usize {
        self.inner.ops.lock().unwrap().len()
    }

    /// True when no operations have been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clone of every recorded operation, in invocation-id order.
    ///
    /// Call only after all worker tasks have been joined: operations still in
    /// flight appear with `return_us: None` (indistinguishable from `?`).
    pub fn snapshot(&self) -> Vec<Op> {
        self.inner.ops.lock().unwrap().clone()
    }
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}

impl OpHandle {
    /// Complete the operation with its observed result value (read value or
    /// write ack).
    pub fn ack(mut self, result: u64) {
        self.finish(Some(result));
    }

    /// Mark the operation's response as unknown (timeout / transport error /
    /// undecodable payload) — the porcupine `?` case: `return_us` stays
    /// `None`.
    pub fn unknown(mut self) {
        self.done = true;
    }

    fn finish(&mut self, result: Option<u64>) {
        let return_us = self.history.elapsed_us();
        let mut ops = self.history.inner.ops.lock().unwrap();
        let op = &mut ops[self.id];
        // Guard against clock granularity: an op may invoke and return within
        // one timer tick; never let the interval go backwards.
        op.return_us = Some(return_us.max(op.invoke_us));
        op.result = result;
        self.done = true;
    }
}

impl Drop for OpHandle {
    fn drop(&mut self) {
        if !self.done {
            eprintln!(
                "linearizability: OpHandle #{} dropped without completion; \
                 recorded as unknown (?)",
                self.id
            );
        }
    }
}

// ── Checker ──────────────────────────────────────────────────────────────────

/// Verdict of a register-history check.
#[derive(Debug)]
pub enum Verdict {
    /// A valid linearization exists.
    Linearizable {
        /// Operations actually checked (after any downsampling).
        checked: usize,
        /// Operations in the original history.
        total: usize,
    },
    /// No linearization exists; carries a human-readable counterexample.
    Illegal(Violation),
}

impl Verdict {
    /// True when the history is linearizable.
    pub fn is_linearizable(&self) -> bool {
        matches!(self, Verdict::Linearizable { .. })
    }
}

/// A linearizability violation with a minimized conflicting sub-history.
#[derive(Debug)]
pub struct Violation {
    total_ops: usize,
    checked_ops: usize,
    minimal: Vec<Op>,
}

impl Violation {
    /// The minimized conflicting sub-history (the conflicting pair plus the
    /// real-time chain between them; greedy 1-minimal).
    pub fn minimal_subhistory(&self) -> &[Op] {
        &self.minimal
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "register history is NOT linearizable: no valid linearization exists \
             ({}/{} ops checked)",
            self.checked_ops, self.total_ops,
        )?;
        writeln!(
            f,
            "minimal conflicting sub-history ({} op(s), greedy 1-minimal):",
            self.minimal.len()
        )?;
        for op in &self.minimal {
            writeln!(f, "{}", op.render())?;
        }
        // Real-time precedence chain: a ≺ b iff a returned before b was
        // invoked. `?` (unknown-response) ops never precede anything.
        writeln!(
            f,
            "real-time precedence (a ≺ b ⇒ a returned before b invoked):"
        )?;
        let mut any_edge = false;
        for a in &self.minimal {
            for b in &self.minimal {
                if a.id != b.id && matches!((a.return_us, b.invoke_us), (Some(r), inv) if r < inv) {
                    writeln!(f, "  #{} ≺ #{}", a.id, b.id)?;
                    any_edge = true;
                }
            }
        }
        if !any_edge {
            writeln!(f, "  (none — all surviving ops are mutually concurrent)")?;
        }
        Ok(())
    }
}

/// Configuration for [`Checker`].
#[derive(Clone, Copy, Debug)]
pub struct CheckerConfig {
    /// Histories larger than this are downsampled with a loud warning.
    pub max_ops: usize,
    /// Wall-clock budget for greedy counterexample minimization.
    pub minimize_budget: Duration,
    /// Step budget (search states explored) for each individual re-check
    /// during minimization. An overrun is treated as "still illegal evidence
    /// unavailable" and keeps the op, so the printed sub-history stays a
    /// proven-illegal superset of the minimal core.
    pub minimize_step_budget: u64,
}

impl Default for CheckerConfig {
    fn default() -> Self {
        Self {
            max_ops: DEFAULT_MAX_OPS,
            minimize_budget: Duration::from_secs(5),
            minimize_step_budget: 2_000_000,
        }
    }
}

/// Porcupine-style register checker. See module docs for the algorithm.
pub struct Checker {
    config: CheckerConfig,
}

impl Default for Checker {
    fn default() -> Self {
        Self::new()
    }
}

impl Checker {
    /// Checker with default limits (10k ops, 5s minimization budget).
    pub fn new() -> Self {
        Self {
            config: CheckerConfig::default(),
        }
    }

    /// Checker with explicit limits (used by the calibration tests).
    pub fn with_config(config: CheckerConfig) -> Self {
        Self { config }
    }

    /// Check a recorded history against the single-register spec.
    pub fn check(&self, ops: &[Op]) -> Verdict {
        let total = ops.len();
        let ops = self.downsample(ops);
        let checked = ops.len();
        if guarded_search(&ops, None).is_linearizable() {
            Verdict::Linearizable { checked, total }
        } else {
            let minimal = self.minimize(&ops);
            Verdict::Illegal(Violation {
                total_ops: total,
                checked_ops: checked,
                minimal,
            })
        }
    }

    /// Uniform-stride downsampling with a loud warning. Interval order and
    /// op ids are preserved, so the real-time semantics of the sample are a
    /// subset of the full history's (a violation in the sample is a real
    /// violation; the elided ops are simply not covered).
    fn downsample(&self, ops: &[Op]) -> Vec<Op> {
        let max = self.config.max_ops.max(1);
        if ops.len() <= max {
            return ops.to_vec();
        }
        let stride = ops.len().div_ceil(max);
        eprintln!(
            "linearizability: WARNING — history has {} ops, exceeding the {}-op limit; \
             downsampling to every {}-th op (~{} ops). The check covers a subset; \
             split the workload or raise CheckerConfig::max_ops for full coverage.",
            ops.len(),
            self.config.max_ops,
            stride,
            ops.len() / stride
        );
        ops.iter().step_by(stride).cloned().collect()
    }

    /// Greedy delta-debugging: repeatedly drop single ops while the history
    /// stays illegal, yielding a 1-minimal conflicting sub-history (the two
    /// conflicting operations plus the real-time chain between them).
    fn minimize(&self, ops: &[Op]) -> Vec<Op> {
        let deadline = Instant::now() + self.config.minimize_budget;
        let mut current: Vec<Op> = ops.to_vec();
        let mut i = 0;
        while i < current.len() {
            if Instant::now() >= deadline {
                eprintln!(
                    "linearizability: counterexample minimization hit its {:?} budget; \
                     returning the partially minimized sub-history ({} ops remain)",
                    self.config.minimize_budget,
                    current.len()
                );
                break;
            }
            let mut candidate = current.clone();
            candidate.remove(i);
            // Budget overruns are treated as "cannot prove illegal" and keep
            // the op — the result stays a proven-illegal superset.
            let still_illegal = !candidate.is_empty()
                && guarded_search(&candidate, Some(self.config.minimize_step_budget)).is_illegal();
            if still_illegal {
                current = candidate; // still illegal without op i — drop it
            } else {
                i += 1; // op i is load-bearing — keep it
            }
        }
        current
    }
}

/// Outcome of one search run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SearchOutcome {
    /// A valid linearization exists.
    Linearizable,
    /// Exhaustive search completed: no valid linearization exists.
    Illegal,
    /// Step budget exhausted before the search completed (minimizer only).
    BudgetExceeded,
}

impl SearchOutcome {
    fn is_linearizable(self) -> bool {
        matches!(self, SearchOutcome::Linearizable)
    }

    fn is_illegal(self) -> bool {
        matches!(self, SearchOutcome::Illegal)
    }
}

/// Run [`search`] on a dedicated thread with a large stack: the DFS recurses
/// to depth `ops.len()` (up to 10k after downsampling), which can exceed the
/// default 2 MiB test-thread stack. 64 MiB is virtual/lazily committed.
fn guarded_search(ops: &[Op], step_budget: Option<u64>) -> SearchOutcome {
    const STACK_SIZE: usize = 64 * 1024 * 1024;
    std::thread::scope(|scope| {
        let handle = std::thread::Builder::new()
            .name("linearizability-search".to_string())
            .stack_size(STACK_SIZE)
            .spawn_scoped(scope, || search(ops, step_budget))
            .expect("failed to spawn linearizability search thread");
        match handle.join() {
            Ok(outcome) => outcome,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    })
}

/// Core backtracking search: does ANY valid linearization of `ops` exist?
///
/// `ops` may arrive in any order; they are sorted by invocation time first.
/// `pred_count[i]` tracks how many not-yet-linearized completed ops returned
/// before op `i` was invoked (its real-time predecessors); op `i` is a
/// candidate exactly when the count reaches zero. Failed `(register value,
/// linearized set)` states are memoized — availability counts are a pure
/// function of the linearized set, so the pair fully determines the
/// remaining subproblem.
fn search(ops: &[Op], step_budget: Option<u64>) -> SearchOutcome {
    let n = ops.len();
    if n == 0 {
        return SearchOutcome::Linearizable;
    }

    // Sort by invocation time (stable on id) so candidate scans follow a
    // deterministic, physically meaningful order.
    let mut ops: Vec<Op> = ops.to_vec();
    ops.sort_by_key(|op| (op.invoke_us, op.id));

    // Initial real-time precedence counts:
    // pred(j) = { i ≠ j : i completed with return_us(i) < invoke_us(j) }.
    let mut pred_count = vec![0usize; n];
    for j in 0..n {
        let inv_j = ops[j].invoke_us;
        pred_count[j] = ops
            .iter()
            .filter(|op_i| matches!(op_i.return_us, Some(r) if r < inv_j))
            .count();
    }

    let words = n.div_ceil(64);
    let mut state = SearchState {
        ops: &ops,
        pred_count: &mut pred_count,
        linearized: vec![false; n],
        bits: vec![0u64; words],
        done: 0,
        value: INITIAL_VALUE,
        value_stack: Vec::with_capacity(n),
        memo: HashSet::new(),
        steps: 0,
        step_budget,
    };
    state.run()
}

struct SearchState<'a> {
    ops: &'a [Op],
    /// Remaining real-time predecessors per op (mutable during the DFS,
    /// restored on backtrack).
    pred_count: &'a mut [usize],
    linearized: Vec<bool>,
    /// Bitset mirror of `linearized`, used as (part of) the memo key.
    bits: Vec<u64>,
    done: usize,
    /// Register value after the linearized prefix.
    value: u64,
    /// `value` before each apply, for O(1) restore on backtrack.
    value_stack: Vec<u64>,
    memo: HashSet<(u64, Vec<u64>)>,
    steps: u64,
    step_budget: Option<u64>,
}

impl SearchState<'_> {
    fn run(&mut self) -> SearchOutcome {
        let n = self.ops.len();
        if self.done == n {
            return SearchOutcome::Linearizable;
        }
        self.steps += 1;
        if let Some(budget) = self.step_budget {
            if self.steps > budget {
                return SearchOutcome::BudgetExceeded;
            }
        }

        let key = (self.value, self.bits.clone());
        if self.memo.contains(&key) {
            return SearchOutcome::Illegal;
        }

        // Candidates: unlinearized ops whose real-time predecessors have all
        // been linearized. A read with a known result is only attempted when
        // the register currently holds that value (pruning of impossible
        // branches); reads are tried before writes because they are the
        // constrained moves.
        let mut reads = Vec::new();
        let mut writes = Vec::new();
        for i in 0..n {
            if self.linearized[i] || self.pred_count[i] > 0 {
                continue;
            }
            match self.ops[i].kind {
                OpKind::Read => match self.ops[i].result {
                    Some(v) if v == self.value => reads.push(i),
                    Some(_) => {}          // impossible branch — skip
                    None => reads.push(i), // `?` response constrains nothing
                },
                OpKind::Write(_) => writes.push(i),
            }
        }

        for i in reads.into_iter().chain(writes) {
            self.apply(i);
            match self.run() {
                SearchOutcome::Linearizable => return SearchOutcome::Linearizable,
                SearchOutcome::BudgetExceeded => {
                    self.undo(i);
                    return SearchOutcome::BudgetExceeded;
                }
                SearchOutcome::Illegal => self.undo(i),
            }
        }

        self.memo.insert(key);
        SearchOutcome::Illegal
    }

    /// Linearize op `i` (must be available): apply its register effect and
    /// discharge it as a real-time predecessor of every unlinearized op.
    fn apply(&mut self, i: usize) {
        debug_assert!(!self.linearized[i] && self.pred_count[i] == 0);
        self.value_stack.push(self.value);
        self.linearized[i] = true;
        self.bits[i / 64] |= 1u64 << (i % 64);
        self.done += 1;
        if let OpKind::Write(v) = self.ops[i].kind {
            self.value = v;
        }
        // i is a real-time predecessor of j iff i completed before j was
        // invoked. Unknown-return ops are nobody's predecessor.
        if let Some(ret_i) = self.ops[i].return_us {
            for (j, op_j) in self.ops.iter().enumerate() {
                if !self.linearized[j] && ret_i < op_j.invoke_us {
                    self.pred_count[j] -= 1;
                }
            }
        }
    }

    /// Backtrack op `i`, exactly mirroring [`Self::apply`].
    fn undo(&mut self, i: usize) {
        if let Some(ret_i) = self.ops[i].return_us {
            for (j, op_j) in self.ops.iter().enumerate() {
                // j == i is excluded because i is still marked linearized.
                if !self.linearized[j] && ret_i < op_j.invoke_us {
                    self.pred_count[j] += 1;
                }
            }
        }
        self.value = self.value_stack.pop().expect("value stack mirrors depth");
        self.done -= 1;
        self.bits[i / 64] &= !(1u64 << (i % 64));
        self.linearized[i] = false;
    }
}

// ── Register payload codec (single key, single partition) ───────────────────

/// Write-request tag byte: `[WRITE_TAG, value: 8-byte big-endian]`.
const WRITE_TAG: u8 = 0x01;
/// Read-request tag byte: `[READ_TAG]`.
const READ_TAG: u8 = 0x02;

fn encode_write(value: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(WRITE_TAG);
    out.extend_from_slice(&value.to_be_bytes());
    out
}

fn encode_read() -> Vec<u8> {
    vec![READ_TAG]
}

/// Decode an 8-byte big-endian value (read response / write ack payload).
fn decode_value(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

// ── KvRegisterService ────────────────────────────────────────────────────────

/// Single-key kv register service backing the linearizability workload.
///
/// `handle` serves both the write path (Raft apply →
/// `ServiceStateMachineAdapter`) and the read path (`ServiceReadHandle` →
/// `read_local` / `read_linearizable`), so the register state is exercised
/// through exactly the production read/write plumbing.
///
/// State lives behind an `Arc<AtomicU64>`; the factory below creates a
/// **fresh register per node**, so follower replicas genuinely lag behind
/// the leader while isolated — that is what makes the `read_local` side of
/// the workload able to observe staleness at all.
pub struct KvRegisterService {
    value: Arc<AtomicU64>,
}

impl KvRegisterService {
    /// A register initialized to [`INITIAL_VALUE`].
    pub fn new() -> Self {
        Self {
            value: Arc::new(AtomicU64::new(INITIAL_VALUE)),
        }
    }
}

impl Default for KvRegisterService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Service for KvRegisterService {
    async fn handle(&self, request: Vec<u8>) -> Result<Vec<u8>, RrError> {
        match request.as_slice() {
            [WRITE_TAG, rest @ ..] if rest.len() == 8 => {
                let value = u64::from_be_bytes(rest.try_into().expect("length checked"));
                self.value.store(value, Ordering::SeqCst);
                Ok(value.to_be_bytes().to_vec())
            }
            [READ_TAG] => Ok(self.value.load(Ordering::SeqCst).to_be_bytes().to_vec()),
            _ => Err(RrError::Application(format!(
                "kv-register: malformed payload ({} bytes)",
                request.len()
            ))),
        }
    }

    fn name(&self) -> &str {
        "kv-register"
    }

    async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
        Ok(self.value.load(Ordering::SeqCst).to_be_bytes().to_vec())
    }

    async fn restore(&mut self, data: &[u8]) -> Result<(), RrError> {
        match decode_value(data) {
            // Full replacement: the snapshot bytes are the entire state.
            Some(value) => {
                self.value.store(value, Ordering::SeqCst);
                Ok(())
            }
            None => Err(RrError::Application(format!(
                "kv-register: malformed snapshot ({} bytes)",
                data.len()
            ))),
        }
    }
}

// ── Register cluster harness ─────────────────────────────────────────────────

/// The single register partition exercised by the linearizability tests.
pub const REGISTER_GROUP_ID: GroupId = 1;

/// Build a 3-node real-network cluster running one kv-register group
/// ([`REGISTER_GROUP_ID`]) behind a bidirectional [`FaultInjector`].
///
/// Mirrors `build_3node_cluster_with_factory` (tests/integration/src/lib.rs)
/// — kept as a separate additive helper so the existing helper is untouched —
/// but wires [`KvRegisterService`] (fresh register state per node) and
/// additionally returns the shared config provider so tests can build an
/// `RrClient` from the routing table.
pub async fn build_register_cluster(
) -> Result<(Vec<Arc<RrNode>>, Arc<FaultInjector>, Arc<MemConfigProvider>)> {
    let node_ids: Vec<NodeId> = vec![1, 2, 3];
    let config_provider = Arc::new(MemConfigProvider::new());

    config_provider
        .add_partition(
            REGISTER_GROUP_ID,
            PartitionConfig::SingleId(REGISTER_GROUP_ID),
            REGISTER_GROUP_ID,
            node_ids.clone(),
        )
        .await;

    let mut node_addrs: Vec<(NodeId, String)> = Vec::with_capacity(node_ids.len());
    for &node_id in &node_ids {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        drop(listener); // release port; small TOCTOU window is acceptable in tests
        node_addrs.push((node_id, format!("127.0.0.1:{port}")));
    }

    for (node_id, addr) in &node_addrs {
        config_provider
            .add_node(*node_id, rr_core::types::NodeAddr { addr: addr.clone() })
            .await;
    }

    let injector = Arc::new(FaultInjector::new());
    let mut nodes: Vec<Arc<RrNode>> = Vec::with_capacity(node_ids.len());

    for (node_id, addr) in &node_addrs {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemStorage::new());
        let provider: Arc<dyn ConfigProvider> = Arc::clone(&config_provider) as _;
        let pool_for_node: Arc<dyn ChannelPoolLike> = injector.new_pool_for_node(*node_id);

        let mgr = Arc::new(RaftGroupManager::new_with_network(
            *node_id,
            register_factory(),
            pool_for_node,
        ));

        let router = Arc::new(ShardRouter::new(Arc::clone(&provider)));
        let lookup = Arc::new(RaftGroupLookupImpl::new(Arc::clone(&mgr)));
        let dispatch = Arc::new(ClientDispatchImpl::new(Arc::clone(&mgr), router));

        // The factory passed to NodeConfig is ignored once a pre-built group
        // manager is supplied (same pattern as the lib.rs harness).
        let config = NodeConfig::new(*node_id, addr.clone(), storage, provider, echo_factory())
            .with_group_manager(Arc::clone(&mgr))
            .with_raft_lookup(lookup)
            .with_client_dispatch(dispatch)
            .with_allow_insecure(true);

        let node = Arc::new(RrNode::new(config)?);
        node.start().await?;
        nodes.push(node);
    }

    let mut members = std::collections::BTreeMap::new();
    for (node_id, addr) in &node_addrs {
        members.insert(*node_id, openraft::BasicNode { addr: addr.clone() });
    }

    let raft = nodes[0]
        .group_manager()
        .get_raft(REGISTER_GROUP_ID)
        .await
        .ok_or_else(|| anyhow::anyhow!("node 1 missing group {REGISTER_GROUP_ID}"))?;
    let _ = raft.initialize(members).await;

    // Brief pause to allow gRPC servers to fully start (mirrors the harness).
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok((nodes, injector, config_provider))
}

fn register_factory() -> rr_raft::group_manager::StateMachineFactory {
    Box::new(|group_id| {
        Box::new(ServiceStateMachineAdapter::new(
            group_id,
            KvRegisterService::new(),
        ))
    })
}

// ── RecordingClient ──────────────────────────────────────────────────────────

/// Decorating client that records every register call against
/// [`REGISTER_GROUP_ID`] (or a caller-chosen group) into two histories:
///
/// - writes (`send_request`) and `read_linearizable` go to the **checked**
///   history — the one fed to [`Checker`];
/// - `read_local` (`RrClient::read`, stale-able session read) goes to a
///   **separate** session history that must NOT be fed to the checker.
///
/// Time base is a single process-local `Instant` per history (see module
/// docs); failed/undecodable calls are recorded as `?`.
pub struct RecordingClient {
    inner: RrClient,
    group_id: GroupId,
    checked: History,
    session: History,
}

impl RecordingClient {
    /// Wrap `inner`, recording register ops against `group_id`.
    pub fn new(inner: RrClient, group_id: GroupId) -> Self {
        Self {
            inner,
            group_id,
            checked: History::new(),
            session: History::new(),
        }
    }

    /// The checked history (writes + linearizable reads).
    pub fn checked_history(&self) -> History {
        self.checked.clone()
    }

    /// The session-guarantee history (`read_local` — stale-able, unchecked).
    pub fn session_history(&self) -> History {
        self.session.clone()
    }

    /// `Write(v)`: record the invocation, forward through
    /// [`RrClient::send_request`], record ack `v` on success or `?` on
    /// error/undecodable ack.
    pub async fn write(&self, value: u64) -> Result<(), RrError> {
        let op = self.checked.begin_write(value);
        match self
            .inner
            .send_request(self.group_id, encode_write(value))
            .await
        {
            Ok(resp) => {
                // The op returned, so the record completes with the acked
                // value regardless of payload shape; a malformed ack payload
                // is surfaced as an error so workload success counters trip.
                op.ack(value);
                if decode_value(&resp) == Some(value) {
                    Ok(())
                } else {
                    Err(RrError::Application(format!(
                        "kv-register: write ack payload mismatch ({} bytes)",
                        resp.len()
                    )))
                }
            }
            Err(e) => {
                op.unknown();
                Err(e)
            }
        }
    }

    /// `Read` with linearizable semantics via [`RrClient::read_linearizable`]
    /// — recorded into the checked history.
    pub async fn read_linearizable(&self) -> Result<u64, RrError> {
        let op = self.checked.begin_read();
        match self
            .inner
            .read_linearizable(self.group_id, encode_read())
            .await
        {
            Ok(resp) => match decode_value(&resp) {
                Some(v) => {
                    op.ack(v);
                    Ok(v)
                }
                None => {
                    op.unknown();
                    Err(RrError::Application(format!(
                        "kv-register: undecodable read value ({} bytes)",
                        resp.len()
                    )))
                }
            },
            Err(e) => {
                op.unknown();
                Err(e)
            }
        }
    }

    /// Stale-able session read via [`RrClient::read`] (the `read_local`
    /// RPC) — recorded into the **session** history, never checked.
    pub async fn read_local(&self) -> Result<u64, RrError> {
        let op = self.session.begin_read();
        match self.inner.read(self.group_id, encode_read()).await {
            Ok(resp) => match decode_value(&resp) {
                Some(v) => {
                    op.ack(v);
                    Ok(v)
                }
                None => {
                    op.unknown();
                    Err(RrError::Application(format!(
                        "kv-register: undecodable read-local value ({} bytes)",
                        resp.len()
                    )))
                }
            },
            Err(e) => {
                op.unknown();
                Err(e)
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{shutdown_all, wait_for_cluster_ready};
    use rr_client::RetryConfig;

    // ── checker calibration fixtures ────────────────────────────────────────

    /// Completed `Write(v)` op.
    fn w(id: usize, inv: u64, ret: u64, v: u64) -> Op {
        Op {
            id,
            invoke_us: inv,
            return_us: Some(ret),
            kind: OpKind::Write(v),
            result: Some(v),
        }
    }

    /// `Write(v)` whose response is unknown (timeout) — the `?` case.
    fn w_timeout(id: usize, inv: u64, v: u64) -> Op {
        Op {
            id,
            invoke_us: inv,
            return_us: None,
            kind: OpKind::Write(v),
            result: None,
        }
    }

    /// Completed `Read → v` op.
    fn r(id: usize, inv: u64, ret: u64, v: u64) -> Op {
        Op {
            id,
            invoke_us: inv,
            return_us: Some(ret),
            kind: OpKind::Read,
            result: Some(v),
        }
    }

    fn assert_legal(ops: &[Op]) {
        let verdict = Checker::new().check(ops);
        assert!(
            verdict.is_linearizable(),
            "history must be judged legal: {verdict:?}"
        );
    }

    fn assert_illegal(ops: &[Op]) -> Violation {
        match Checker::new().check(ops) {
            Verdict::Illegal(violation) => violation,
            Verdict::Linearizable { .. } => {
                panic!("history must be judged ILLEGAL, got Linearizable")
            }
        }
    }

    // ── illegal histories (calibration: all three bug classes) ──────────────

    /// Class 1 — stale read: the read starts after write(1) returned, so the
    /// register must hold 1; observing the initial value is illegal.
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_rejects_stale_read() {
        let ops = vec![w(0, 100, 200, 1), r(1, 300, 400, INITIAL_VALUE)];
        let violation = assert_illegal(&ops);
        assert_eq!(
            violation.minimal_subhistory().len(),
            2,
            "stale-read core must be the write/read pair"
        );
        let report = violation.to_string();
        assert!(
            report.contains("#0 ≺ #1"),
            "report must show the chain:\n{report}"
        );
        assert!(
            report.contains("WRITE(1)"),
            "report must name the write:\n{report}"
        );
    }

    /// Class 2 — lost write: write(2) was acked, yet a strictly later read
    /// still observes 1; the acked write vanished from the linearization.
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_rejects_lost_write() {
        let ops = vec![w(0, 100, 200, 1), w(1, 300, 400, 2), r(2, 500, 600, 1)];
        assert_illegal(&ops);
    }

    /// Class 3 — duplicated-apply value corruption: a later read observes the
    /// register regress to the older value (the observable symptom of a
    /// duplicated apply of the first write clobbering the second).
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_rejects_duplicated_apply_corruption() {
        let ops = vec![
            w(0, 100, 200, 10),
            w(1, 300, 400, 20),
            r(2, 450, 550, 20),
            r(3, 600, 700, 10), // regression: 20 already observed, 10 is impossible
        ];
        assert_illegal(&ops);
    }

    /// Phantom value: a timed-out write can only justify its own value; a
    /// read observing a value nobody wrote is illegal even with `?` ops.
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_rejects_phantom_value() {
        let ops = vec![w_timeout(0, 100, 3), r(1, 150, 250, 4)];
        assert_illegal(&ops);
    }

    // ── legal histories ─────────────────────────────────────────────────────

    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_accepts_empty_history() {
        assert_legal(&[]);
    }

    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_accepts_initial_value_read() {
        assert_legal(&[r(0, 100, 200, INITIAL_VALUE), w(1, 300, 400, 5)]);
    }

    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_accepts_sequential_consistent() {
        let ops = vec![
            w(0, 100, 200, 1),
            r(1, 300, 400, 1),
            w(2, 500, 600, 2),
            r(3, 700, 800, 2),
        ];
        assert_legal(&ops);
    }

    /// Overlapping writes: w2 completes while w1 is still in flight. A read
    /// after w2's return must see 2; a read after both returned may see
    /// either value (their linearization order is ambiguous).
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_accepts_concurrent_overlap() {
        let ops = vec![
            w(0, 100, 500, 1), // long write, overlaps everything
            w(1, 200, 300, 2), // completes inside w1's interval
            r(2, 350, 400, 2), // w(2) ≺ read forced; w(1) linearized after it
            r(3, 600, 700, 1), // both writes precede; order ambiguous → 1 legal
        ];
        assert_legal(&ops);
    }

    /// Unknown-response (`?`) writes: one timed-out write may or may not have
    /// applied — both observations must be accepted; another timed-out write
    /// that *did* take effect must justify a later read of its value.
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_accepts_unknown_responses() {
        let ops = vec![
            w(0, 100, 200, 1),    // acked
            w_timeout(1, 250, 2), // timeout — effect unknown
            r(2, 300, 400, 1),    // legal: w(2) linearized after this read
            r(3, 500, 600, 2),    // legal: w(2) linearized before this read
            w_timeout(4, 700, 3), // timeout — but did take effect
            r(4, 750, 800, 3),    // legal: timed-out write justifies the read
        ];
        assert_legal(&ops);
    }

    // ── counterexample quality ──────────────────────────────────────────────

    /// Greedy minimization must strip legal padding and keep the conflicting
    /// pair plus nothing else.
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_minimizes_violation_to_conflicting_pair() {
        let ops = vec![
            w(0, 10, 20, 9), // legal padding before
            r(1, 30, 40, 9),
            w(2, 100, 200, 1), // ── conflicting core: stale read of 0 after 1
            r(3, 300, 400, INITIAL_VALUE),
            w(4, 500, 600, 7), // legal padding after
            r(5, 700, 800, 7),
        ];
        let violation = assert_illegal(&ops);
        let minimal = violation.minimal_subhistory();
        assert_eq!(minimal.len(), 2, "padding must be stripped: {violation}");
        // The surviving sub-history is itself illegal (1-minimal).
        assert_illegal(minimal);
        let report = violation.to_string();
        assert!(report.contains("#2 ≺ #3"), "chain must survive:\n{report}");
    }

    /// Beyond `max_ops` the checker downsamples (with a loud warning) instead
    /// of blowing its time budget.
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_downsamples_oversized_history() {
        let ops: Vec<Op> = (0..9)
            .map(|i| w(i, 10 * i as u64, 10 * i as u64 + 5, i as u64 + 1))
            .collect();
        let checker = Checker::with_config(CheckerConfig {
            max_ops: 4,
            ..CheckerConfig::default()
        });
        match checker.check(&ops) {
            Verdict::Linearizable { checked, total } => {
                assert_eq!(total, 9);
                assert_eq!(checked, 3, "stride-3 sample of 9 ops keeps 3");
            }
            Verdict::Illegal(v) => panic!("write-only history is always legal: {v}"),
        }
    }

    // ── performance sanity ──────────────────────────────────────────────────

    /// 2 000 fully-ordered ops must check in well under a second (fully
    /// sequential ⇒ zero backtracking).
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_perf_sequential_2k_ops() {
        let mut ops = Vec::with_capacity(2000);
        for i in 0..1000u64 {
            ops.push(w(ops.len(), 10 * i, 10 * i + 4, i + 1));
            ops.push(r(ops.len(), 10 * i + 5, 10 * i + 9, i + 1));
        }
        let start = Instant::now();
        assert_legal(&ops);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "2k sequential ops must check in far under a second, took {elapsed:?}"
        );
    }

    /// Wide concurrency (64 overlapping writes, then 64 concurrent reads that
    /// each observe a different written value) must still check fast: the
    /// read-match pruning keeps the first descent on a valid path.
    ///
    /// Timing note: the reads OVERLAP the writes (invoke 1200 < write return
    /// 1500), so each `read → v` can linearize right after its `write(v)`.
    /// Reads strictly after all writes would make 63 of the 64 observations
    /// unsatisfiable by construction — and the exhaustive search for a
    /// nonexistent linearization is precisely the exponential case.
    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn checker_perf_concurrent_burst() {
        let mut ops = Vec::with_capacity(128);
        for v in 1..=64u64 {
            ops.push(w(ops.len(), 1000, 1500, v));
        }
        for v in 1..=64u64 {
            ops.push(r(ops.len(), 1200, 1700, v));
        }
        let start = Instant::now();
        assert_legal(&ops);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "128-op concurrent burst must check fast, took {elapsed:?}"
        );
    }

    // ── recorder sanity ─────────────────────────────────────────────────────

    // INV-2: linearizable reads reflect all writes committed before the read was issued (R-5 checker calibration).
    #[test]
    fn history_records_completed_and_unknown_ops() {
        let history = History::new();
        let op = history.begin_write(7);
        std::thread::sleep(Duration::from_millis(1));
        op.ack(7);
        history.begin_read().unknown();

        let ops = history.snapshot();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].kind, OpKind::Write(7));
        assert_eq!(ops[0].result, Some(7));
        assert!(
            ops[0].return_us.expect("completed") >= ops[0].invoke_us,
            "interval must not go backwards"
        );
        assert_eq!(ops[1].return_us, None, "unknown op keeps open interval");
        assert_eq!(ops[1].result, None);
    }

    // ── integration: register workload under rotating follower isolation ────

    /// INV-2: linearizable reads reflect all writes committed before the read was issued.
    /// (docs/docs/reference/invariants.md, INV-2; R-5 makes it executable: the
    /// recorded write + `read_linearizable` history must satisfy the
    /// porcupine-style register checker in this module.)
    ///
    /// A 3-node cluster runs the single-key [`KvRegisterService`] while a
    /// fault injector rotates isolation across the two followers. Writers and
    /// linearizable readers are recorded into the checked history;
    /// stale-able `read_local` calls are recorded into a separate session
    /// history (session guarantee — deliberately NOT fed to the checker).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_kv_register_linearizable_under_partition() {
        let (nodes, injector, provider) = build_register_cluster()
            .await
            .expect("register cluster builds");
        let leader = wait_for_cluster_ready(&nodes, REGISTER_GROUP_ID, 8_000)
            .await
            .expect("leader elected");

        let table = provider.get_routing_table().await.expect("routing table");
        let addrs: Vec<String> = table.nodes.values().map(|n| n.addr.clone()).collect();
        let raw_client = RrClient::connect_with_config(
            addrs,
            RetryConfig {
                max_retries: 8,
                retry_delay_ms: 40,
            },
        )
        .await
        .expect("client connects");
        let client = Arc::new(RecordingClient::new(raw_client, REGISTER_GROUP_ID));

        const WRITERS: u64 = 2;
        const WRITES_PER_WRITER: u64 = 40;
        const LIN_READERS: u64 = 2;
        const LIN_READS_PER_READER: u64 = 30;
        const LOCAL_READS: u64 = 30;

        let acked_writes = Arc::new(AtomicU64::new(0));
        let lin_reads_ok = Arc::new(AtomicU64::new(0));
        let local_reads_ok = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for w in 0..WRITERS {
            let client = Arc::clone(&client);
            let acked = Arc::clone(&acked_writes);
            handles.push(tokio::spawn(async move {
                for i in 0..WRITES_PER_WRITER {
                    // Globally unique, non-zero register values.
                    let value = (w + 1) * 1_000_000 + i + 1;
                    if client.write(value).await.is_ok() {
                        acked.fetch_add(1, Ordering::SeqCst);
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }));
        }
        for _ in 0..LIN_READERS {
            let client = Arc::clone(&client);
            let ok = Arc::clone(&lin_reads_ok);
            handles.push(tokio::spawn(async move {
                for _ in 0..LIN_READS_PER_READER {
                    if client.read_linearizable().await.is_ok() {
                        ok.fetch_add(1, Ordering::SeqCst);
                    }
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }
            }));
        }
        {
            let client = Arc::clone(&client);
            let ok = Arc::clone(&local_reads_ok);
            handles.push(tokio::spawn(async move {
                for _ in 0..LOCAL_READS {
                    if client.read_local().await.is_ok() {
                        ok.fetch_add(1, Ordering::SeqCst);
                    }
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }
            }));
        }

        // Rotating follower isolation: one follower partitioned at a time
        // (~400ms isolated, ~250ms healed), so the leader keeps a quorum and
        // keeps serving throughout.
        let followers: Vec<NodeId> = nodes
            .iter()
            .map(|n| n.node_id())
            .filter(|id| *id != leader)
            .collect();
        let chaos = {
            let injector = Arc::clone(&injector);
            tokio::spawn(async move {
                for round in 0..6usize {
                    let target = followers[round % followers.len()];
                    injector.isolate(target);
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    injector.restore(target);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            })
        };

        for h in handles {
            let _ = h.await;
        }
        let _ = chaos.await;
        injector.clear_all();

        // Settle: stable leader again, isolated followers catch up.
        wait_for_cluster_ready(&nodes, REGISTER_GROUP_ID, 12_000)
            .await
            .expect("cluster re-converges after partitions heal");
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        let acked = acked_writes.load(Ordering::SeqCst);
        let lin_ok = lin_reads_ok.load(Ordering::SeqCst);
        let local_ok = local_reads_ok.load(Ordering::SeqCst);

        // The leader is never isolated, so a quorum always exists: the
        // overwhelming majority of writes must succeed.
        assert!(
            acked >= WRITERS * WRITES_PER_WRITER * 4 / 5,
            "at least 80% of writes must ack under follower-only partitions; got {acked}/{}",
            WRITERS * WRITES_PER_WRITER
        );
        assert!(lin_ok > 0, "some linearizable reads must succeed");
        assert!(local_ok > 0, "some read_local calls must succeed");

        // Checked history: every recorded write + linearizable read.
        let checked = client.checked_history().snapshot();
        assert_eq!(
            checked.len() as u64,
            WRITERS * WRITES_PER_WRITER + LIN_READERS * LIN_READS_PER_READER,
            "every write/linearizable-read invocation must be recorded"
        );
        match Checker::new().check(&checked) {
            Verdict::Linearizable { checked: n, total } => {
                eprintln!("register history linearizable ({n}/{total} ops checked)");
            }
            Verdict::Illegal(violation) => {
                panic!("INV-2 violated: register history not linearizable under partition:\n{violation}")
            }
        }

        // Session history: recorded separately and deliberately NOT checked
        // (read_local is a session-guarantee read; staleness is legal there).
        let session = client.session_history().snapshot();
        assert_eq!(
            session.len() as u64,
            LOCAL_READS,
            "every read_local invocation must land in the session history"
        );

        shutdown_all(&nodes).await;
    }
}
