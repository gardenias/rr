---
title: "Snapshot consistency contract + disaster recovery path"
status: implemented
created: 2026-04-24
updated: 2026-09-04
tags: [adr, architecture, snapshot, recovery, openraft]
---

# A-0002 — Snapshot Consistency and Disaster Recovery

## Context

rr wraps OpenRaft with its own `Service` trait for user business logic. The
current design has three recovery-related mechanisms:

1. **OpenRaft automatic recovery**: `Raft::new()` internally reads log + state
   + snapshot and reconciles inconsistencies
2. **rr `DurabilityPolicy`** enum: `Strict` (fsync every write, default) /
   `Balanced` (WAL no fsync) / `Fast` (no WAL, test-only)
3. **rr snapshot streaming**: `InstallSnapshot` RPC splits snapshots into 64KB
   chunks, bincode-serialized `InstallSnapshotRequest` as metadata on final
   chunk

During design review, the question arose: **what are the exact guarantees and
failure modes of this recovery path**, and what should users know to use it
safely?

This ADR documents the **authoritative contracts** verified against OpenRaft
source code (`databendlabs/openraft` @ commit `30dc1f0c`) and specifies:

1. `Service` trait safety invariants users must uphold
2. Clarified `DurabilityPolicy` guidance
3. New `rr-admin node-reset` command for single-node disaster recovery

## Problem

Design discussion surfaced claims that needed verification:

1. ❌ Claimed: "OpenRaft guarantees `snapshot.meta.last_log_id` atomically
   corresponds to state in snapshot bytes"
2. ⚠️ Claimed: "On recovery, OpenRaft uses snapshot's `last_log_id` as replay
   anchor"
3. ❌ Claimed: "If `applied_state > log_state`, OpenRaft detects inconsistency
   as error"

Librarian verification against OpenRaft source **refuted claims 1 and 3**.
OpenRaft's actual contracts are more subtle, and rr's documentation must reflect
them correctly — otherwise framework users will misimplement `Service` and hit
silent data corruption.

## Background: OpenRaft's actual contracts (verified)

### Contract 1: Snapshot atomicity is the implementer's responsibility

OpenRaft does **not** guarantee snapshot consistency. Instead, it calls
`RaftSnapshotBuilder::build_snapshot()` in a **spawned tokio task** that runs
**concurrently** with `apply()`:

> Building snapshot is a read-only operation that runs in a spawned task. This
> parallelization depends on the [`RaftSnapshotBuilder`] implementation: The
> builder must:
> - hold a consistent view of the state machine that won't be affected by
>   further writes such as applying a log entry,
> - or it must be able to acquire a lock that prevents any write operations.

Source: [`openraft/src/core/sm/worker.rs#L233-L237`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/core/sm/worker.rs#L233-L237)

The `RaftSnapshotBuilder::build_snapshot()` doc comment itself only says:

> A snapshot has to contain state of all applied logs, including membership.
> Usually it is just a serialized state machine.

Source: [`raft_snapshot_builder.rs#L23-L32`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/storage/v2/raft_snapshot_builder.rs#L23-L32)

"has to contain" is a **requirement on the implementer**, not a runtime
guarantee from OpenRaft. Canonical example implementations use an explicit
lock:

```rust
// tests-turmoil/src/store.rs (OpenRaft reference impl)
async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
    let data = self.data.lock().unwrap();  // ← IMPLEMENTER holds lock
    let snapshot_data = serde_json::to_vec(&*data)?;
    let meta = SnapshotMeta {
        last_log_id: data.last_applied,     // ← IMPLEMENTER reads under lock
        last_membership: data.last_membership.clone(),
        snapshot_id: snapshot_idx.to_string(),
    };
    // ...
}
```

Source: [`tests-turmoil/src/store.rs#L165-L187`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/tests-turmoil/src/store.rs#L165-L187)

### Contract 2: Recovery flow is more nuanced than "use snapshot as anchor"

The actual recovery path in `StorageHelper::get_initial_state()`:

```
1. Read vote, log_state, applied_state, committed from storage
2. If committed < last_applied → committed = last_applied  (bump committed)
3. restore_from_snapshot():
     if snapshot.meta.last_log_id > last_applied:
       install_snapshot()  ← updates last_applied
4. Re-read applied_state (may have changed from step 3)
5. If last_applied < committed:
     Re-apply logs from last_applied.next_index() to committed.next_index()
     If start < last_purged_log_id.next_index() → ERROR (required logs purged)
6. If last_log_id < last_applied:
     Purge logs to last_applied  ← NOT an error, normal cleanup
7. If logs purged in step 6 but no snapshot → force-build a snapshot
```

Source: [`openraft/src/storage/helper.rs#L88-L225`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/storage/helper.rs#L88-L225)

The replay anchor is `applied_state().last_applied`, which **after snapshot
install** equals `snapshot.meta.last_log_id` — so the end result matches the
original claim, but the mechanism involves multiple steps.

### Contract 3: `applied_state > log_state` is NOT an error

This was the most important correction. Original claim was wrong.

Actual OpenRaft code:

```rust
// Clean up dirty state: snapshot is installed, but logs are not cleaned.
if last_log_id < last_applied {
    tracing::info!(
        "Clean the hole between last_log_id({}) and last_applied({}) by purging logs to {}",
        ...
    );
    self.log_store.purge(last_applied.clone().unwrap()).await?;
    last_log_id = last_applied.clone();
    last_purged_log_id = last_applied.clone();
}
```

Source: [`helper.rs#L153-L165`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/storage/helper.rs#L153-L165)

The comment "Clean up dirty state: snapshot is installed, but logs are not
cleaned" documents the expected scenario: a crash occurred between snapshot
installation and the log-purge that would normally follow. OpenRaft handles
this gracefully.

**The ACTUAL disaster** is different:

```rust
if start < last_purged_log_id.next_index() {
    return Err(StorageError::read_log_at_index(start, ...));
}
```

If `last_applied < committed` AND logs needed to replay `[last_applied+1 ..
committed]` are **purged** (not just gone from in-memory — purged from storage
with no snapshot covering them), OpenRaft returns a `StorageError`. This is the
unrecoverable single-node scenario.

### Contract 4: Durability is binary, not tunable

OpenRaft has no `DurabilityPolicy` enum. Its contract:

> An implementation has to guarantee data durability. The caller always assumes
> a completed writing is persistent. The raft correctness highly depends on a
> reliable store.

Source: [`getting-started.md#L192-L195`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/docs/getting_started/getting-started.md#L192-L195)

Per-method contracts:

- `save_vote()`: "The vote must be persisted on disk before returning"
- `append()`: "When the `callback` is called, the entries must be persisted on
  disk"
- `apply()`: implementer chooses one of three strategies (persistent SM /
  persistent snapshot only / transient SM with `save_committed()`)

Source: [`raft_log_storage.rs#L27-L32`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/storage/v2/raft_log_storage.rs#L27-L32),
[`raft_state_machine.rs#L70-L86`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/storage/v2/raft_state_machine.rs#L70-L86)

**Implication for rr**: `DurabilityPolicy::Balanced` (WAL-no-fsync) and
`DurabilityPolicy::Fast` (no-WAL) **violate** OpenRaft's durability contract.
Only `Strict` is safe.

### Contract 5: IO ordering between vote and log writes

OpenRaft requires strict serialization of vote and log IO:

> Vote and log I/O operations must be strictly ordered to prevent committed
> data loss. The term update must be persisted before any log entries from
> that term, ensuring that once a log entry is written, the corresponding term
> is also guaranteed to be durable.

Source: [`io_ordering.md#L67-L73`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/docs/protocol/io_ordering.md#L67-L73)

**Implication for rr**: `RocksLogStorage::save_vote()` and `append()` must be
serialised. A concurrent `append()` cannot overtake a pending `save_vote()`
from an older term. rr currently satisfies this because all IO goes through
`tokio::spawn_blocking` on a single state machine task, but this should be
documented as a contract rr-raft must uphold.

## Options for rr framework changes

### Problem 1: `Service` safety invariants are undocumented

Users implementing `Service::snapshot()` and `Service::apply()` today receive
no guidance on the concurrency contracts OpenRaft imposes. Misimplementations
produce silent data corruption.

#### Option 1A — Documentation-only

Add "Safety Invariants" section to `Service` trait rustdoc and a prose page in
the Docusaurus guide.

Pros: simple, no code changes

Cons: users may not read docs carefully; no compile-time enforcement

#### Option 1B — Documentation + runtime assertions

Add debug-mode assertions that catch common violations:
- Track `apply()` entry/exit; if a second `apply()` is seen before the first
  returns, panic in debug builds
- Assert `snapshot()` returns consistent `last_applied` (compare
  before/after)

Pros: catches misuse in CI

Cons: debug-only (production builds silent); complex to implement correctly;
false positives possible

#### Option 1C — Wrap `Service` in a framework mutex

Auto-lock around `apply()` and `snapshot()` calls at framework level.

Pros: correct-by-construction

Cons:
- Serialises snapshot building with apply, losing the concurrency benefit
  OpenRaft was designed for
- Imposes cost on users who correctly implemented internal locking

### Problem 2: `DurabilityPolicy` is misleading

Current `DurabilityPolicy::Balanced` and `::Fast` violate OpenRaft's contract
but don't warn the user.

#### Option 2A — Keep all three policies, add docs

Rustdoc warns loudly on Balanced/Fast; `NodeConfig::new()` accepts any.

Pros: flexibility for advanced users (e.g., ephemeral test clusters)

Cons: easy footgun; users following examples may use `Balanced` for perceived
performance

#### Option 2B — Keep three policies, `Strict` is the only default; make
non-strict opt-in via environment variable or feature flag

Requires setting `RR_ALLOW_UNSAFE_DURABILITY=1` or
`cargo build --features unsafe-durability` to construct non-Strict variants.

Pros: users can't accidentally use unsafe policy; explicit signal in config

Cons: runtime flag inspection adds startup complexity

#### Option 2C — Remove `Balanced` and `Fast`, Strict only

Breaking change. `Fast` mode is actively useful for test suites (speeds up
integration tests ~10x).

### Problem 3: Single-node disaster recovery is undocumented

When a single node's storage is corrupted (disk fault, accidental
`rm`, etc.), there's no framework-provided path to recover it from peers.

#### Option 3A — Manual runbook

Document steps: stop node, clear data dir, re-add as learner, wait for
snapshot install, promote to voter.

Pros: zero framework work

Cons: error-prone (multiple CLI commands, must be done in order); missing any
step leaves node in bad state

#### Option 3B — New `rr-admin node-reset <node-id>` command

Single CLI command that orchestrates the full recovery:

```bash
rr-admin --addr <healthy-peer> node-reset --node-id 42 \
         --data-dir /var/lib/rr \
         --wait-catch-up
```

Pros: one-shot recovery; less room for operator error

Cons: new admin surface to maintain

## Decision

### D1 — Document `Service` safety invariants (Option 1A)

Add prominent "Safety Invariants" section to the `Service` trait rustdoc:

```rust
/// # Safety Invariants (MUST uphold)
///
/// `Service` implementations run inside OpenRaft's state machine task, which
/// has specific concurrency contracts. Violating these invariants causes
/// silent data corruption.
///
/// ## 1. `snapshot()` must capture an atomically-consistent view
///
/// The framework calls `snapshot()` in a spawned task that runs **concurrently**
/// with `apply()` calls on the same service instance. You MUST either:
///
/// - Hold a read-lock covering all fields accessed, OR
/// - Use immutable data structures (`Arc<im::HashMap>`, etc.)
///
/// ### Incorrect (silent corruption):
/// ```rust,ignore
/// async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
///     let a = self.map_a.read().await;
///     drop(a);
///     let b = self.map_b.read().await;  // <-- b may be newer than a
///     bincode::encode(&(*a, *b))         // <-- inconsistent view
/// }
/// ```
///
/// ### Correct:
/// ```rust,ignore
/// async fn snapshot(&self) -> Result<Vec<u8>, RrError> {
///     let state = self.state.read().await;  // single lock covers everything
///     bincode::encode(&*state)
/// }
/// ```
///
/// ## 2. `apply()` must complete all state updates before returning
///
/// After `apply()` returns, `snapshot()` may be called immediately. Any state
/// change initiated by `apply()` MUST be visible to subsequent `snapshot()`
/// calls on the same instance.
///
/// ### Forbidden:
/// ```rust,ignore
/// async fn apply(&mut self, entry: &[u8]) -> Result<Vec<u8>, RrError> {
///     let state = self.state.clone();
///     tokio::spawn(async move {
///         state.write().await.apply_change();  // <-- fire-and-forget
///     });
///     Ok(response)
/// }
/// ```
///
/// This leaks mutations outside the apply() window. Future snapshots will be
/// inconsistent. Always update synchronously before returning from `apply()`.
///
/// ## 3. `restore()` completely replaces state
///
/// When the framework calls `restore()`, the service MUST discard all existing
/// in-memory state and reconstruct from the provided bytes. After `restore()`
/// returns, `apply()` MUST NOT reference any pre-restore state.
```

Adding the same invariants to the Docusaurus `guide/service-trait.md`
as "Correct implementation patterns" with worked examples.

### D2 — Clarify `DurabilityPolicy` semantics (hybrid of Option 2A and 2B)

- **Keep all three policies**, but:
- **Rename**: `Strict` → `Strict` (unchanged), `Balanced` → `ReplicatedOnly`,
  `Fast` → `TestOnly`. Breaking change via `#[deprecated]` path:

```rust
#[derive(Debug, Clone, Copy)]
pub enum DurabilityPolicy {
    /// fsync every write. Only safe option per OpenRaft's durability contract.
    /// REQUIRED for production deployments.
    Strict,

    /// WAL written but not fsynced. Data loss possible if entire quorum loses
    /// power simultaneously. ONLY SAFE when:
    /// - Replication factor >= 3
    /// - Nodes on independent power domains
    /// - External backup captures Raft log retention
    ///
    /// WARNING: Violates OpenRaft's durability contract. Use at your own risk.
    ReplicatedOnly,

    /// No WAL, no fsync. TESTS ONLY. Any crash loses committed data.
    TestOnly,

    #[deprecated(since = "0.3.0", note = "Renamed to ReplicatedOnly")]
    Balanced,

    #[deprecated(since = "0.3.0", note = "Renamed to TestOnly")]
    Fast,
}
```

- **Add log-warning on non-Strict**: When `RocksStorage::open()` is called with
  `ReplicatedOnly` or `TestOnly`, emit `warn!` log:
  > "DurabilityPolicy::ReplicatedOnly violates OpenRaft's durability contract.
  > This mode is only safe with replication + external backup."

### D3 — Add `rr-admin node-reset` command (Option 3B)

```bash
rr-admin --addr <healthy-peer> node-reset \
    --target-node-id 42 \
    --data-dir /var/lib/rr/node-42 \
    [--wait-catch-up]
    [--promote-after-catch-up]
```

Behaviour:

1. Verify node 42 is currently unreachable or explicitly drained (refuse if
   healthy — operator might mean to reset a live node by accident)
2. Issue `RemoveMember(42)` via AdminService to the healthy peer
3. SSH/local-execute cleanup: `rm -rf <data-dir>/*` (requires explicit
   operator confirmation prompt unless `--force`)
4. Re-add as learner: `AddMember(42, learner=true)`
5. If `--wait-catch-up`: poll `GetGroupStatus` on healthy peer until node 42's
   `last_log_id` reaches leader's `committed - tolerance`
6. If `--promote-after-catch-up`: issue `ChangeMembership` to make voter

Implementation touches:
- `rr-admin/src/backup.rs` (extend with reset command)
- `rr-network/proto/admin.proto` (add `ResetNode` RPC — optional; can also use
  existing `ChangeMembership` + operator script)
- `docs/docs/operations/runbooks/node-reset.md` (new runbook documenting
  fault scenarios)

## Consequences

### Positive

- **Correctness guarantees documented** — users can implement `Service`
  correctly with confidence
- **Durability misuse hard to trigger** — rename makes intent explicit; log
  warnings at node startup
- **Single-node disaster recovery becomes a single command** — operator error
  reduced
- **Alignment with OpenRaft upstream** — rr no longer makes claims contrary
  to OpenRaft's actual contracts

### Negative

- **Breaking change for existing users** on `DurabilityPolicy::Balanced` /
  `::Fast` — though deprecation alias preserves compilation for one minor
  version
- **More documentation surface** — `Service` trait docs grow significantly;
  long-form runbook adds to maintenance
- **`node-reset` command is new admin surface** — needs test coverage,
  potential security implications (remove-member is a destructive op)
- **Log-warning on non-Strict durability** — noisy for legitimate test
  deployments; balanced by clarity for production

### Neutral

- **`Service` trait signature unchanged** — no API breakage; safety invariants
  are documentation-only
- **Snapshot format unchanged** — recovery flow stays identical; this ADR is
  about *correctness documentation*, not structural changes

## Implementation dependencies

- Depends on: nothing (purely documentation + one CLI command + one enum
  rename)
- Enables: **A-0001** (framework state machine wrapper needs atomic snapshot
  capture guaranteed by D1); **A-0003** (idempotency state in snapshots needs
  same guarantee)
- Does not block: any current v0.2.x work

## Reference: Complete OpenRaft recovery sequence

For implementers and documentation writers, the authoritative recovery
sequence (from `StorageHelper::get_initial_state()`):

| Step | Condition | Action | Failure mode |
|------|-----------|--------|--------------|
| 1 | Always | Read `vote`, `log_state`, `applied_state`, `committed` | Storage IO error → boot fails |
| 2 | `committed < last_applied` | `committed = last_applied` | — |
| 3 | `snapshot.meta.last_log_id > last_applied` | `install_snapshot()` (updates last_applied) | `Service::restore()` panic → boot fails |
| 4 | `last_applied < committed` | Replay logs `[last_applied+1..committed]` | If `start < last_purged+1` → **StorageError: required logs purged** (unrecoverable single-node) |
| 5 | `last_log_id < last_applied` | Purge logs to `last_applied` | — (normal cleanup) |
| 6 | Logs purged but no snapshot exists | Force-build a snapshot | `Service::snapshot()` panic → boot fails |

## Citations

All OpenRaft citations from commit
`30dc1f0c6c68bd31c2d05546cdff6b8517951a62` (main branch, 2026-04-24).

- [`openraft/src/storage/helper.rs`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/storage/helper.rs) — `get_initial_state()` recovery flow
- [`openraft/src/core/sm/worker.rs`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/core/sm/worker.rs) — `build_snapshot()` spawning + concurrency contract
- [`openraft/src/storage/v2/raft_snapshot_builder.rs`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/storage/v2/raft_snapshot_builder.rs) — trait doc
- [`openraft/src/storage/v2/raft_log_storage.rs`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/storage/v2/raft_log_storage.rs) — durability + serialization contracts
- [`openraft/src/storage/v2/raft_state_machine.rs`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/storage/v2/raft_state_machine.rs) — three apply strategies
- [`openraft/src/docs/getting_started/getting-started.md`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/docs/getting_started/getting-started.md) — durability policy
- [`openraft/src/docs/protocol/io_ordering.md`](https://github.com/databendlabs/openraft/blob/30dc1f0c6c68bd31c2d05546cdff6b8517951a62/openraft/src/docs/protocol/io_ordering.md) — IO ordering

## Changelog

| Date | Change | Trigger |
|---|---|---|
| 2026-04-24 | Initial draft based on design discussion | Design discussion with Louis |
| 2026-04-24 | Corrected claims 1/3 after OpenRaft source verification (librarian task `bg_e072f2fd`) | Librarian returned authoritative contracts |
