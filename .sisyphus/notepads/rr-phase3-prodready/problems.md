# Problems — rr-phase3-prodready

## [2026-04-22] Problems Encountered and Resolved

### RocksDB durability silently broken

**Problem**: `RocksStorage::put/delete/write_batch` used `db.put_cf` without
a `WriteOptions` argument; no `set_sync(true)` anywhere. Result: committed
Raft log entries only in OS page cache, not on disk. Power loss →
lose "committed" entries → Raft durability contract violated.

**Resolution**: Added `DurabilityPolicy` enum + `durability` field on
`RocksConfig`. Default `Strict`. Switched to `put_cf_opt/delete_cf_opt/write_opt`
with per-call `WriteOptions` driven by the policy.

**Detection**: Phase 3 correctness audit (via parallel explore agent).
**Impact**: CRITICAL — this was a silent corruption risk.

### `RaftNode::new_*` panicked on init failure

**Problem**: `config.validate().expect(...)` + `RocksLogStorage::open().expect(...)`
meant a single RocksDB open failure (disk full, permission denied, corrupt db)
panicked the entire rr-server process instead of returning a typed error.

**Resolution**: Widened signatures to `Result<Self, RrError>`. Updated 27
call sites via `gitnexus_impact` — most just propagated via `?`, so the
change was mechanical.

**Detection**: Code review via `.unwrap()` / `.expect()` grep.
**Impact**: HIGH — production crashes instead of clean errors.

### Chaos tests `#[ignore]`d — isolation was one-directional

**Problem**: `test_leader_isolation_and_recovery` always timed out because
`FaultAwareChannelPool::isolate(N)` only blocked inbound connections to N.
The "isolated" leader could still send AppendEntries heartbeats, so
followers never hit election timeout.

**Resolution**: Refactored to per-node pools sharing a common
`Arc<FaultState>`. `isolate(N)` now blocks both directions (owner check
+ destination check). All 4 chaos tests now pass in ~3.5s total.

**Detection**: Running `cargo test -- --ignored` as part of Phase 3 T4.
**Impact**: MEDIUM — chaos tests hadn't been running in CI for a long time;
bidirectional-isolation bugs in real code could have been missed.

### rr-raft ↔ rr-network circular dep forced duplicate code

**Problem**: rr-network needs `ChannelPool` + `TlsConfig` types.
rr-raft needs to USE those types for outbound Raft RPCs, but rr-raft
can't depend on rr-network (would cycle: rr-network → rr-raft-types → rr-raft → rr-network).

**Resolution**: Intentional duplication. `rr_raft::network_factory::ChannelPool`
mirrors `rr_network::ChannelPool` (same logic, own file). `rr_raft::network_factory::TlsPaths`
mirrors `rr_network::TlsConfig`. rr-server's `main.rs` translates between
them. Documented via module-level doc comments explaining the cycle-break.

**Detection**: Expected from the existing ChannelPool pattern.
**Impact**: LOW — manageable; clearly-documented duplication.

### rustls 0.23 requires explicit CryptoProvider

**Problem**: First TLS smoke test panicked at runtime:
"Could not automatically determine the process-level CryptoProvider from
Rustls crate features." Even though tonic was compiled with `tls-ring`,
rustls 0.23 requires `install_default()` to be called exactly once.

**Resolution**: Added
`rustls::crypto::ring::default_provider().install_default()` in
`rr-server::main()`, gated only by the `serve` subcommand (not `version`).
Documented in `docs/docs/deployment/tls.md` for library embedders.

**Detection**: First end-to-end smoke test of `rr-server --tls-cert ...`.
**Impact**: LOW — caught quickly; but would have shipped broken without
the smoke test.

### Nightly LLVM `--branch` coverage crashes (SIGSEGV)

**Problem**: `cargo +nightly llvm-cov --branch` on rr crashed LLVM with
a segfault in the `llvm-cov report` tool (not in our code). Known issue
with the unstable flag combined with complex rustc-emitted coverage
regions.

**Resolution**: Not fixable at our level. Fell back to stable's region
coverage, which is finer-grained than line coverage and includes
conditional arms. Documented in the coverage report.

**Detection**: Running T29 follow-up coverage report.
**Impact**: LOW — we lose strict "branch" metric, but region coverage
is an effective substitute.

### Bogus clippy errors from newer stable rustc

**Problem**: `cargo clippy --workspace -- -D warnings` on newer stable
flagged 32 `io_other_error` warnings (`std::io::Error::new(ErrorKind::Other, ...)`
→ prefer `std::io::Error::other(...)`). These existed on `main` before
Phase 3 work — NOT introduced by us.

**Resolution**: Skipped. Pre-existing technical debt, not in scope.
Tracking as Phase 3.1 cleanup.

**Detection**: Running clippy after major edits.
**Impact**: LOW — hasn't broken CI (older toolchain?), but will once
CI's `stable` toolchain picks up the newer lint. Fix is mechanical
(`Error::new(ErrorKind::Other, e.to_string())` → `Error::other(e.to_string())`).

### `ClientService` gRPC status mismatch in integration tests

**Problem** (encountered during T14): integration tests check exit codes
and HTTP status; some tests used `tonic::Code::InvalidArgument` where
the actual code is now `ResourceExhausted` (post-rate-limit). Had to
update test expectations.

**Resolution**: Test updates inline with the code change.
**Impact**: NONE — caught and fixed in the same commit.

### MacOS filesystem case-insensitivity ambiguity

**Problem**: When writing `docs/docs/architecture/c4/context.md`, the
system reminder arrived as `CONTEXT.md` (uppercase) content. Initial
confusion about whether two files existed.

**Resolution**: Noted that MacOS default HFS+/APFS is case-insensitive
by default. Both references point to the same file.
**Impact**: NONE — cosmetic confusion only.

### Proptests under coverage = 200× slower

**Problem**: `cargo llvm-cov --workspace` with default proptest sample
size (256 per test × 4 proptests × per-test RocksDB open) would take
~15-20 min. Instrumentation slows everything ~2-3×, per-test setup
dominates.

**Resolution**: Pass `--skip proptest` to the test harness during
coverage runs. Documented in the coverage report.
**Impact**: LOW — trade coverage completeness for feedback loop time.
The "missing" coverage from proptests is on code paths that non-proptest
tests already exercise.

## [2026-04-23] Problems with the Coverage Report Pipeline

### Running coverage report twice redirects JSON to stderr

**Problem**: `cargo llvm-cov report --json --summary-only | python3 ...`
gave "Expecting value: line 1 column 1" — the JSON wasn't on stdout.

**Resolution**: Use `--output-path <file>` explicitly, then `python3` reads
from the file. `cargo llvm-cov` apparently mixes build progress and
JSON output on stdout in some modes.

### Ignore regex needs both crate-relative and absolute patterns

**Problem**: `--ignore-filename-regex 'tests/'` didn't exclude integration
tests because the path was `/Users/louis/Projects/rr/tests/integration/src/...`
and the regex anchored wrong.

**Resolution**: `(tests/|examples/|benches/|\.sisyphus/|proto\.rs)` with
alternation works. cargo-llvm-cov's internal regex also auto-adds
`/rustc/...` and cargo registry exclusions, so the "clean" regex is
simpler than you'd expect.
