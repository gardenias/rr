# rr WAL log-storage backend — implementation plan

> Companion to ADR A-0010. Carries the concrete task breakdown.
> Status: **deferred** until release-build profile confirms RocksDB
> LSM overhead is the dominant cost for the Raft log path.

## Pre-conditions (do these first, in order)

1. **Release-build perf baseline**. Run `cargo build --release -p rr-perf-bench` then `rr-perf-bench --scenario s1 --storage rocks --durability strict` and `--scenario s2 --concurrency 100 --durability replicated_only`. Capture TPS / p50 / p99 / p999.
   → **✅ DONE 2026-09-05**: full matrix archived in `docs/perf/baselines/v0.2.0-baseline.md`
   (repro: `scripts/perf-baseline.sh`; S1 + S2 sweep × strict/replicated_only/test_only/mem).
2. **Identify the actual bottleneck**. If CPU saturates before fsync queue depth, WAL is premature — finish IoBatcher tuning instead. Only proceed if storage shows up as the binding constraint (p99 fsync time dominates request latency, or `iostat` shows sustained high write-amp).
   → **❌ NOT MET 2026-09-05**: strict ≈ test_only ≈ mem (<2% at every concurrency level),
   per-process CPU peaks at ~3.6 cores at saturation. The binding constraint is the CPU path
   (codec / replication / apply), not fsync or the RocksDB LSM. **WAL stays deferred** —
   and IoBatcher tuning would be equally pointless (fsync is cold). Next perf lever requires
   CPU profiling of the hot path; revisit A-0010 only if a future profile shows storage-bound.
3. **Decide on `IoBatcher` reuse vs fork**. Read `crates/rr-raft/src/io_batcher.rs` end-to-end. Either:
   - Extract a `BatchWriter` trait so both `RocksLogStorage` and `WalLogStorage` plug in, or
   - Copy the file to `WalBatcher` and accept some duplication.

## Implementation tasks

### Phase 1 — module scaffolding

- [ ] `crates/rr-raft/Cargo.toml` — add `wal` feature flag (default: `[]`); add `crc32fast = "1"` under that feature.
- [ ] `crates/rr-raft/src/wal_log_storage.rs` (new file) — define `WalLogStorage` struct + `WalConfig` config struct.
- [ ] `crates/rr-raft/src/lib.rs` — gated `pub mod wal_log_storage`.
- [ ] On-disk layout constants: `WAL_MAGIC = b"\0rrWAL01"`, `DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024`.

### Phase 2 — segment writer

- [ ] `Segment` struct: `file: File`, `start_index: u64`, `next_index: u64`, `bytes_written: u64`.
- [ ] `Segment::open_for_append(path, start_index)` — create or open; on existing, scan to find `next_index` and validated `bytes_written`.
- [ ] `Segment::append(entries: &[(LogIndex, RrEntry)])` — serialize, write, return `bytes_written_delta`.
- [ ] `Segment::sync()` — `file.sync_data()`.
- [ ] Atomic rotate: when `bytes_written + payload >= DEFAULT_SEGMENT_SIZE`, close current, open `{next_segment_id}.seg`.

### Phase 3 — segment reader

- [ ] `Segment::open_for_read(path)` — validates magic, returns `start_index`.
- [ ] `Segment::scan_entries(start: u64, end: u64)` → iterator yielding `(LogIndex, RrEntry)`.
- [ ] `Segment::recover_tail()` — walks entries, finds first CRC mismatch, truncates file at that boundary, returns recovery report (entries kept, bytes truncated).

### Phase 4 — vote and `last_purged` files

- [ ] `WalLogStorage::save_vote(vote)` — write `tmp`, `fsync`, rename, `fsync(dir)`.
- [ ] `WalLogStorage::load_vote()` — read + bincode.
- [ ] Same pattern for `last_purged` u64.

### Phase 5 — `RaftLogStorage` impl

Implement, in order, exactly mirroring `RocksLogStorage`:

- [ ] `get_log_state()` — first segment's `start_index` (or `last_purged`), last segment's `next_index - 1`.
- [ ] `get_log_reader()` — clone `Arc<Inner>`.
- [ ] `save_vote()` — Phase 4.
- [ ] `append(entries, callback)` — submit to `WalBatcher`, callback on fsync return.
- [ ] `truncate_after(log_id)` — locate segment, truncate file mid-segment OR delete younger segments.
- [ ] `purge(log_id)` — delete segments whose entries are all `<= log_id`; persist new `last_purged`.

Implement `RaftLogReader`:

- [ ] `try_get_log_entries(range)` — walk overlapping segments.
- [ ] `read_vote()` — Phase 4.

### Phase 6 — batcher

- [ ] `WalBatcher` mirrors `IoBatcher`'s 3-phase accumulation (try_recv drain → 500 µs window → single batch).
- [ ] Single `Segment::append + sync` per batch; one callback per submission.

### Phase 7 — storage-backend wiring

- [ ] `crates/rr-server/src/config_file.rs` — `StorageBackend::Wal` variant; parse `"wal"` in TOML / CLI.
- [ ] `crates/rr-server/src/main.rs` — `build_storage` factory case for `Wal`.
- [ ] Decide: does WAL replace **only** the log, or also the state-machine KV? **Only the log**: keep `RocksStorage` for state machine + metadata; WAL backs `RaftLogStorage` only.

### Phase 8 — test parity

Port these 16 tests from `crates/rr-raft/src/rocks_log_storage.rs` (mod `tests`):

| # | Test name | Concerns |
|---|---|---|
| 1 | `test_rocks_append_and_read` | Basic append + scan |
| 2 | `test_rocks_vote_save_read` | Vote roundtrip |
| 3 | `test_rocks_truncate_after` | Mid-log truncate |
| 4 | `test_rocks_truncate_after_with_empty_log` | Edge: truncate empty |
| 5 | `test_rocks_purge` | Mid-log purge |
| 6 | `test_rocks_purge_then_log_state` | Purge updates state |
| 7 | `test_rocks_log_state_empty` | Empty log |
| 8 | `test_rocks_log_state_after_append` | Log state correctness |
| 9 | `test_rocks_vote_persistence` | Vote survives reopen |
| 10 | `test_rocks_log_persistence` | Log survives reopen |
| 11 | `test_rocks_append_callback_fires` | Callback semantics |
| 12 | `test_rocks_concurrent_appends` | Concurrent submitters |
| 13 | `test_rocks_batch_window_config` | Custom batcher window |
| 14 | `test_rocks_empty_entries_no_hang` | Edge: empty append |
| 15 | `test_rocks_truncate_after_async_append` | Truncate after pending append |
| 16 | `test_rocks_callback_on_write_error` | Error propagation |

Plus three new WAL-only tests:

- [ ] `test_wal_recovers_from_corrupt_tail` — write 100 entries, corrupt last 16 bytes, reopen, assert entries 0..99 present (or 0..98 if last entry is what got corrupted), no panic.
- [ ] `test_wal_rotates_segment_at_size_boundary` — write enough entries to trigger rotation, assert `00000000.seg` is read-only and `00000001.seg` is the active one.
- [ ] `test_wal_kill_mid_append_preserves_acked` — fork a process, write N entries with one fsync each, kill -9 mid-second-batch, reopen, assert all entries whose append-callback fired are present.

### Phase 9 — benchmark

- [ ] Extend `benches/benches/log_append_batched.rs` to compare RocksDB vs WAL backends. Acceptance: WAL ≥ 2× RocksDB on sustained sequential append + fsync, in release build.
- [ ] Document the result in `docs/research/2026-MM-DD-wal-vs-rocksdb-bench.md` per R1.

### Phase 10 — chaos parity

- [ ] Run `test_crash_during_write_preserves_acked_entries` against WAL.
- [ ] Run `test_concurrent_writes_under_chaos_linearizability` against WAL.
- [ ] Both must pass before the WAL backend is recommended for production.

### Phase 11 — docs

- [ ] `docs/docs/architecture/storage.md` — add WAL backend section, contrast with RocksDB.
- [ ] `docs/docs/deployment/deployment.md` — document the `storage.backend = "wal"` TOML choice and trade-offs.
- [ ] Update `docs/docs/reference/metrics-reference.md` if the WAL emits new metrics.

### Phase 12 — release readiness

- [ ] Soak test in a single-node cluster for 24+ hours.
- [ ] Soak test in a 3-node cluster under continuous load with fault injection.
- [ ] Default backend remains `Rocks` until soak + chaos + bench are all green.
- [ ] ADR A-0010 status → `implemented` with link to bench result doc.

## Acceptance criteria

1. All 16 ported `RocksLogStorage` tests pass.
2. All 3 WAL-only tests pass.
3. Both chaos tests pass against WAL.
4. Release-build benchmark shows ≥ 2× throughput improvement on sustained sequential append.
5. 24 h single-node soak shows zero data loss, zero panics.

## Risk register

| Risk | Mitigation |
|---|---|
| Torn write at segment boundary | CRC32C per entry; recovery truncates at first bad CRC. |
| `fsync(dir)` not portable | Use `cap-std` or guard with `cfg(unix)`; on Windows accept weaker guarantee or refuse to open WAL backend. |
| Segment rotation edge: append spans two segments | Force segment to be a multiple of the largest possible entry size, or hold the append fully in one segment by rotating before the entry. |
| Large in-flight batch lost on crash | Callback fires *after* fsync only; openraft contract preserved. |
| Process kill mid-`rename(vote.tmp, vote)` | rename is atomic on POSIX; on first open, prefer `vote.tmp` if it exists and parse-valid, else `vote`. |

## Out of scope (for now)

- `io-uring` backend — Linux-only; let the synchronous path land first.
- Per-segment compression — Raft entries are already user payloads, often pre-compressed.
- Multi-WAL striping across disks — single-disk is the dominant deployment shape.
