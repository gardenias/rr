---
title: "WAL log-storage backend as a faster alternative to RocksDB"
status: accepted
created: 2026-05-13
updated: 2026-05-13
tags: [adr, architecture, storage, performance, raft]
---

# A-0010 — WAL Log-Storage Backend

## Context

The Raft log is currently persisted in RocksDB via `RocksLogStorage`
(`crates/rr-raft/src/rocks_log_storage.rs`), using two column families
(`raft_log` and `metadata`). `IoBatcher` (`crates/rr-raft/src/io_batcher.rs`)
batches concurrent `append` calls into a single `write_batch + fsync` to
amortise the durability cost (~26 % throughput improvement measured).

The Raft log workload is **append-only, sequential, time-ordered**. An
LSM-tree (RocksDB) gives us write amplification, compaction overhead,
and the WriteThread group-commit mutex
([RocksDB issue #14627](https://github.com/facebook/rocksdb/issues/14627))
— all of which we pay for capabilities (range scans, key updates,
deletes) the Raft log never uses.

A purpose-built segmented WAL eliminates the LSM overhead:

- Append is the entire write path (no rewrites, no compaction).
- Purge is "delete an old segment file"; truncate is "truncate the
  active segment at an offset".
- No WriteThread mutex; per-group append directly contends only on its
  own active segment.

The state machine and snapshots **stay on RocksDB** — only the log
moves. This isolates the change and keeps the existing snapshot path
untouched.

## Problem

Pick a WAL implementation that is:

1. **Compatible with openraft 0.10.0-alpha.19's `RaftLogStorage` trait**
   (same surface that `RocksLogStorage` implements today).
2. **Crash-safe** — every acked entry must survive process kill.
3. **Faster than `RocksLogStorage + IoBatcher`** under sequential
   append-only load (the actual demonstrated improvement is the
   acceptance criterion, not theoretical).
4. **Tractable to maintain** — code we can debug; bounded API surface.
5. **Not blocking on upstream** — we cannot wait for a third-party
   crate to add openraft 0.10 support.

## Options Considered

Background research consolidated in
`.sisyphus/evidence/wal-options-2026-05-13.md`.

### Option A — Adopt `raft-wal` v0.8 (NyxStudio-Labs)

| Pro | Con |
|---|---|
| Implements `RaftLogStorage` / `RaftLogReader` out of the box | Targets **openraft ^0.9**, we run 0.10.0-alpha.19 — would need a fork to port the binding |
| Well-shaped API (segments + CRC32C + parallel recovery) | 1 GitHub star, single contributor, 2-week development burst |
| Optional `io-uring` backend | Author explicitly states "not battle-tested in production" |
| Light dependencies | Adopting means we sign up for the openraft-binding port + ongoing maintenance |

### Option B — Wrap TiKV `raft-engine`

| Pro | Con |
|---|---|
| **Production-proven** at TiKV scale since v6.1 (2022) | API tied to `raft-rs` protobuf `Entry`; openraft uses `C::Entry` |
| Real benchmark wins (TpmC +4 %, p99.9 latency −22 %, write I/O −38 %) | Full adapter layer required: protobuf ↔ openraft type-config |
| 630★, active dev (TiKV team) | Heavy dependency surface (`raft-rs`, `kvproto` in dev-deps) |

### Option C — Sled / Fjall

Rejected. Sled doesn't reliably fsync (per Fjall maintainer); both are
LSM/Bw-tree KV stores — the same overhead we are escaping. Wrong data
structure for an append-only log.

### Option D — redb / heed

Rejected. Both are B-tree KV stores with ~2 ms per fsynced transaction
— two orders of magnitude slower than what we want for log append.

### Option E — Custom segmented log on `std::fs`

| Pro | Con |
|---|---|
| Zero new dependencies | We write and own ~600–1 000 LOC |
| API shape can be a 1:1 fit for openraft 0.10 | We own all bugs (segment rotation edge cases, crash recovery, partial-write handling) |
| Reuses existing `IoBatcher` for group commit | Initial test bar: must clear the 16 `RocksLogStorage` tests |
| Predictable, small attack surface | Same effort as Option A per librarian assessment, but we get the openraft 0.10 fit |

## Decision

**Option E — custom segmented log on `std::fs`.**

Rationale:

- **Version fit**: openraft 0.10.0-alpha.19 changed the trait surface
  (`truncate` → `truncate_after`, vote API split, etc.). Option A would
  need a fork-and-port to ship today; Option E lets us target 0.10
  natively from the start.
- **Effort parity**: per librarian's research, building custom is
  "same effort, same result" as adopting `raft-wal` plus the
  fork-maintenance overhead.
- **Maturity matters more than novelty**: 1★ third-party crate vs
  600–1 000 LOC of testable in-house code is a wash on risk; the
  in-house version is debuggable and has no external supply-chain
  risk.
- **Compose, don't replace**: the state machine and snapshots stay on
  RocksDB. `IoBatcher` is generalised (or paired with a WAL-specific
  batcher of the same shape) to drive segmented-file appends.

We will **defer the actual implementation** until we have a
release-build profile showing the LSM is the dominant cost. The
current performance numbers in `CONTEXT.md` are debug-build only and
identify CPU + gRPC RTT as the bottleneck. Sequencing:

1. **Profile-first**: run `rr-perf-bench --release` once
   `release-build` benchmarking lands. If storage *is* the bottleneck,
   proceed with implementation. If not, the ADR remains `accepted`
   until profile evidence changes.
2. **Implementation plan**: `.sisyphus/plans/rr-wal-impl.md` carries
   the concrete crate layout, file format, trait method-by-method
   plan, and the 16-test parity bar.
3. **Pluggable, not exclusive**: `StorageBackend::Wal` will be a third
   variant alongside `Mem` and `Rocks`. Existing deployments choose
   their backend; we do not flip the default until WAL has cleared a
   release-build chaos suite.

## Design Sketch (deferred implementation)

### Crate location

`crates/rr-raft/src/wal_log_storage.rs` — new module, sibling to
`rocks_log_storage.rs`. No new workspace crate. Feature flag
`feature = "wal"` on `rr-raft` for users who don't want the WAL code
compiled in.

### On-disk layout

```text
<data_dir>/rr-wal/
├── 00000000.seg         active segment (entries by Raft index)
├── 00000001.seg
├── ...
├── vote                 single small file, atomic rename on update
└── last_purged          single small file, atomic rename on update
```

Each segment:

```text
[ magic 8 bytes "\0rrWAL01" ]
[ segment_start_index u64 LE ]
[ entry_0: len u32 | crc32c u32 | bincode(RrEntry) bytes ]
[ entry_1: ... ]
...
```

Default segment size: 64 MiB. Roll over on size.

### Crash recovery

On open, scan each segment from the head:

- Validate magic + start_index.
- Walk entries. On the first entry whose CRC32C does not match its
  declared length, **truncate the file at that boundary**. This is
  the "torn write at the tail" recovery path documented in raft-wal's
  README and in Walrus's test suite.
- All earlier acked entries are preserved.

### Operations (mapping to `RaftLogStorage`)

| Trait method | WAL implementation |
|---|---|
| `get_log_state()` | Read first / last entry across all segments; read `last_purged` file. |
| `get_log_reader()` | Clone `Arc<Inner>` — reader shares state with writer. |
| `save_vote(vote)` | `serialize → temp file → fsync → rename → fsync dir`. |
| `append(entries, callback)` | Hand off `Vec<BatchOp::Put>` analogue to a `WalBatcher` (mirrors `IoBatcher`'s group-commit pattern); callback fires when fsync returns. |
| `truncate_after(idx)` | If the index falls inside the active segment, truncate the file at that byte offset. If older, delete younger segments + truncate. |
| `purge(idx)` | Delete every segment whose `start_index + entry_count <= idx`. Persist `last_purged = idx`. |
| `try_get_log_entries(range)` | Walk segments overlapping `range`, decode in order, return. |
| `read_vote()` | Read `vote` file, deserialize. |

### Batcher

Either generalise `IoBatcher` to take a `BatchWriter` trait (`fn write_batch(ops, callback)`) so both `RocksLogStorage` and `WalLogStorage` can plug in, **or** copy `IoBatcher` to `WalBatcher` with WAL-specific ops. Decision deferred to implementation.

### Test parity bar

Port all 16 `RocksLogStorage` tests (`crates/rr-raft/src/rocks_log_storage.rs` lines 316–627). Specifically:

- append + read
- vote save/read + persistence across reopen
- log persistence across reopen
- truncate_after (within segment + across segments)
- purge (mid-segment + segment-aligned)
- get_log_state (empty, after purge, after truncate)
- callback firing semantics
- concurrent appends
- empty entries
- truncate after async append

Plus new WAL-specific tests:

- Recovery from a deliberately-corrupted segment tail (torn write).
- Segment rotation at exactly the size threshold.
- Recovery from a process-kill mid-`append` (lossless for acked entries).

## Consequences

### Positive

- LSM overhead eliminated for the Raft log.
- Predictable code we own and can debug.
- No external version-pinning risk; ships natively against
  openraft 0.10.
- `StorageBackend::Wal` is opt-in; existing RocksDB deployments are
  unaffected.

### Negative

- ~600–1 000 LOC of new Rust, 16+ ported tests, plus 3 WAL-specific
  recovery tests.
- Operators have a third storage backend to learn (negligible — TOML
  enum value).
- Until a release-build profile confirms storage is the binding
  constraint, this ADR remains `accepted` (design locked) but the
  implementation does not land.

### Neutral

- State machine and snapshots stay on RocksDB. WAL only replaces the
  log column family.
- Existing chaos suite (`test_crash_during_write_preserves_acked_entries`,
  `test_concurrent_writes_under_chaos_linearizability`) becomes the
  primary acceptance gate for the WAL impl.

## Changelog

| Date | Change |
|------|--------|
| 2026-05-13 | Initial: decision matrix from librarian research (`bg_345e4dcc`); status `accepted` pending release-build profile confirming storage is the bottleneck. Implementation plan in `.sisyphus/plans/rr-wal-impl.md`. |
