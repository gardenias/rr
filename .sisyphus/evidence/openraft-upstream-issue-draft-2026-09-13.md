# Upstream issue draft — openraft (databendlabs/openraft)

> Two liveness defects found in 0.10.0-alpha.34 (also present on `main` as of
> 2026-09-12), reproduced and root-caused by the rr perf-lab. rr currently
> carries both as a vendored patch (`vendor/openraft` + `[patch.crates-io]`);
> we would love to drop it once upstream fixes land.

## Issue 1: RaftCore panics on snapshot-purge/apply race in `run_save_committed_and_apply`

**Location**: `openraft/src/core/raft_core.rs` (alpha.34: line 2422; main: line 2451)

```rust
let first = self.engine.state.get_log_id(already_applied.next_index()).unwrap();
```

**What happens**: `Command::SaveCommittedAndApply { already_applied, upto }` is
queued, then a `Command::PurgeLog` (snapshot completion) runs before it and
purges past `already_applied.next_index()`. `get_log_id` returns `None` and the
`unwrap()` panics the RaftCore task. Every subsequent `RaftMsg` send to the core
fails with `Fatal::Panicked`; with 2/3 nodes hit, the cluster loses quorum
**permanently** (it does not recover even after the load stops).

**Reproduction**: 3-node cluster, `SnapshotPolicy::LogsSinceLast(50_000)`,
sustained 16k writes/s × 1KiB on CPU-limited nodes (2 cores each, docker
cpuset). Panic on 2/3 nodes within ~2.5 minutes, 100% reproducible across runs.

**Suggested fix** (what our vendor patch does): when the first index is gone,
skip forward to the first present log id (purged entries are covered by the
installed snapshot, which the state machine has already applied); if the whole
range was purged, skip the apply entirely. We kept the panic only for
`next > last_log_id` (a genuine invariant violation).

## Issue 2: `handle_vote_req` recency rejection never adopts a higher term

**Location**: `openraft/src/engine/engine_impl.rs::handle_vote_req`

```rust
if req.last_log_id.as_ref() >= self.state.last_log_id() {
    // Ok
} else {
    // reject by last_log_id
    return VoteResponse::new(self.state.vote_ref(), ..., false);  // early return
}
let res = self.vote_handler().update_vote(&req.vote);             // never reached
```

**What happens**: when a RequestVote is rejected because the candidate's log is
behind, the function returns **before** `update_vote` — so the receiver never
adopts the higher term and never steps down. Standard Raft requires term
adoption on *any* higher-term message. Consequence observed in the lab: a node
whose term ran ahead (e.g. after network churn) campaigns forever while the
rest of the cluster stays at a lower term — terms never reconverge, and the
cluster is permanently split even though a healthy quorum pair exists.

**Suggested fix** (what our vendor patch does): in the recency-reject branch,
if `req.vote > local vote`, still call `update_vote` locally (adopt term +
step down) but keep answering `granted = false`. Safety is preserved because
the candidate is told it was rejected, and because the recency branch runs
first, no equal/lower-term replay can turn the local record into a real grant.

## Issue 3: become-leader `UpdateIOProgress { when: None }` marks unflushed log IO as flushed

**Location**: `openraft/src/engine/handler/vote_handler/mod.rs` (become-leader
path) + `openraft/src/core/raft_core.rs::run_update_io_progress` /
`handle_local_io`

```rust
// vote_handler (become leader):
self.state.accept_log_io(IOId::new_log_io(leader_vote.clone(), last_log_id.clone()));
self.output.push_command(Command::UpdateIOProgress { when: None, io_id: ... });

// raft_core::run_update_io_progress:
let notify = Notification::LocalIO { io_id: io_id.clone() };
self.tx_notification.send(notify).await.ok();   // at SUBMIT time

// raft_core::handle_local_io:
self.engine.state.log_progress_mut().try_flush(io_id.clone());   // treated as FLUSHED
... self.engine.replication_handler().update_local_progress(log_io_id.log_id);
```

**What happens**: `handle_local_io` treats every `Notification::LocalIO` as a
completed flush, but the become-leader `UpdateIOProgress` generates one without
any IO at all. With a `RaftLogStorage` whose `append` completes asynchronously
(callback fires after return — which the `IOFlushed` callback design explicitly
supports, cf. the comment "Submit IO request, do not wait for the response"),
a node elected while appends are still in flight gets its
`log_progress.flushed()` cursor advanced over entries no storage write has
covered. Two consequences:

1. The new leader's `update_local_progress` self-matches its unflushed tail, so
   a quorum can reach commit with the entry durable on fewer nodes than the
   quorum implies — potentially exactly one.
2. The inflated `flushed()` cursor also releases `Condition::IOFlushed`-gated
   `Respond` commands (e.g. AppendEntries replies) early. Because `IOId`
   ordering is vote-dominant, a later vote IO completion (`save_vote` is a
   fast direct write in most stores) can likewise jump the cursor past a
   queued log backlog — same effect from the follower side.

We observed the end-to-end result in the rr perf-lab (W9): an entry was ACKed
to the client while durable on one node only; a restart wave then let a stale
majority elect a leader without it and truncate the last copy. With
synchronous stores (`append` durable before return) the window is empty, which
is presumably why it has never bitten upstream.

**Suggested fix** (what our vendor patch does): gate the become-leader
`UpdateIOProgress` on the real flush of the previously accepted IO —
`when: prev_accepted.map(|io_id| Condition::IOFlushed { io_id })` where
`prev_accepted` is the return of `accept_log_io` — mirroring the existing
guard in `following_handler`'s duplicate-append path. Fresh starts
(`prev_accepted == None`) keep the immediate behavior. Note this makes the
command conditionally deferred, so `run_update_io_progress` must also use
`try_submit` instead of `submit` for the `submitted` cursor: the new leader's
blank append can execute first, and a plain `submit` would regress the
monotonic cursor (debug panic). Additionally, stores with deferred append
completion must order `save_vote` completion after previously accepted
appends (or the vote IO completion re-opens the same cursor jump); worth one
line in the `RaftLogStorage` contract docs either way.

## Context

Found by rr's production-shape perf matrix (3 nodes, mTLS, cpuset-limited,
durable log). Happy to provide full node logs / repro scripts, and to upstream
our patches if the approach looks right.
