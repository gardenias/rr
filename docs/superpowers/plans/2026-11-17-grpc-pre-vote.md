# gRPC Pre-Vote Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Pre-Vote a real remote RPC, prevent isolated nodes from inflating their term, and
clarify safe bootstrap and optional file-logging workflows.

**Architecture:** Add `PreVote` beside `Vote` in the existing Raft gRPC service and reuse the same
bincode `VoteRequest`/`VoteResponse` payloads. The client enforces the RPC TTL and propagates transport
failures; the server dispatches to `Raft::pre_vote`, never `Raft::vote`. `UNIMPLEMENTED` alone retains
openraft's rolling-upgrade fallback, while all real errors remain errors.

**Tech Stack:** Rust 2024, Tokio, tonic/prost, openraft, bincode, existing Cargo integration tests,
Markdown and Bash 3.2-compatible documentation examples.

**Spec:** The user's three requests in this session; no separate design spec exists.

## Global Constraints

- Preserve all unrelated uncommitted work; make no git commits and do not modify the running cluster.
- Do not change existing protobuf messages, field numbers, or bincode payload formats.
- Never translate connection, timeout, decode, lookup, or internal RPC failures into a granted vote.
- Only `UNIMPLEMENTED` may synthesize a grant, matching openraft's rolling-upgrade behavior.
- Pre-Vote must not persist a vote or change the remote term; never delegate it to `Raft::vote`.
- Use dynamic ports in tests; never use the live 51001–51003 or 52001–52003 endpoints.
- Documentation must distinguish kv-store's full-roster bootstrap from stock rr-server's single-voter
  bootstrap. Markdown lines stay at most 120 columns where practical.
- The optional README script uses file logs and exact child PIDs, works with macOS Bash 3.2, and never
  uses `pkill` or kills by port.

## Review Focus

- Mixed-version nodes: only an unrecognized `PreVote` method falls back; test this in Task 1.
- A fully isolated survivor: it must neither campaign nor increment its term; test this in Task 1.
- Malformed or unknown-group requests: return sanitized errors without granting; test this in Task 1.
- Restarting an initialized or durable node: docs must say to omit `--bootstrap`; verify in Task 2.
- Script interruption: Ctrl-C stops only the three spawned PIDs and preserves logs; verify in Task 2.

---

### Task 1: Real Pre-Vote Transport and Regression Tests

**Files:**
- Modify: `crates/rr-network/proto/raft.proto`
- Modify: `crates/rr-network/src/raft_service.rs`
- Modify: `crates/rr-raft/src/network_factory.rs`
- Modify: `crates/rr-raft/src/tcp_transport.rs`
- Modify: `crates/rr-server/src/lookup.rs`
- Modify: `tests/crash/src/bin/rr-crash-node.rs`
- Test: `examples/kv-store/tests/homogeneous_process.rs`

**Interfaces:**
- Consumes: openraft `RaftNetworkV2::pre_vote`, `Raft::pre_vote`, `VoteRequest`, `VoteResponse`,
  `RPCOption::soft_ttl`, and the existing `ChannelPoolLike`/`RaftGroupLookup` patterns.
- Produces: `rpc PreVote(VoteRequest) returns (VoteResponse)` and byte-oriented
  `RaftGroupLookup::pre_vote(group_id, payload)`. The trait's default returns `UNIMPLEMENTED`; all rr
  production/harness implementations override it. `TcpRaftNetwork::pre_vote` delegates to its existing
  gRPC client.

- [ ] **Step 1: Run GitNexus impact before editing**

Record status/freshness and method/file impact. An unresolved `UNKNOWN` is not evidence of safety;
confirm references with direct source search. Treat the consensus path as high risk.

- [ ] **Step 2: Add failing transport and process regression tests**

Add a connection-error test asserting `RPCError::Unreachable`, a real gRPC round-trip asserting a
remote rejection is preserved, and an `UNIMPLEMENTED` compatibility test. Add a three-process legacy
cluster test that stops the leader and the other peer, then verifies the isolated survivor's term stays
at its baseline for 1.5 seconds and it never becomes leader.

- [ ] **Step 3: Run the new tests before implementation**

Run the targeted rr-raft and kv-store tests. Expected: the connection test returns a synthetic grant,
the round-trip never reaches the peer, and the isolated survivor increments its term. Interface tests
that require the new RPC may fail at compile time at this stage; record that separately from runtime
failures.

- [ ] **Step 4: Implement the protocol and real client/server path**

Add the new RPC, hand-written client method, TTL/error handling, bounded response decoding, and the
single `UNIMPLEMENTED` fallback. Add service and production lookup dispatch to `Raft::pre_vote`, with
unknown-group `NotFound`, malformed-payload `InvalidArgument`, and sanitized internal errors. Add the
TCP delegation and crash-harness implementation without touching unrelated node code.

- [ ] **Step 5: Add service and lookup boundary tests**

Verify group/payload forwarding, unknown groups, malformed payloads, and that a valid Pre-Vote leaves
the receiver's term and persisted vote unchanged. Ensure no test treats a transport error as a grant.

- [ ] **Step 6: Run focused and package verification**

Run the new targeted tests, `cargo test -p rr-network raft_service::tests`,
`cargo test -p rr-raft network_factory::tests`, `cargo test -p rr-server lookup::tests`, and the new
process test. Then run the broader checks needed for changed crates and `cargo fmt --all -- --check`.

### Task 2: Bootstrap Guidance and Optional File-Logging Script

**Files:**
- Modify: `README.md`
- Modify: `examples/kv-store/README.md`
- Modify: `examples/kv-store/src/main.rs`
- Modify: `examples/counter/README.md`
- Modify: `docs/docs/guide/getting-started.md`
- Modify: `docs/docs/deployment/deployment.md`

**Interfaces:**
- Consumes: the manual/legacy and managed formation behavior implemented in kv-store and documented in
  the existing operations playbook.
- Produces: consistent user-facing bootstrap rules and an optional copy/paste Bash supervised startup
  example using `target/kv-store-demo/{logs,pids}` and default base port 56101.

- [ ] **Step 1: Document when bootstrap is required**

State that no-argument demo mode self-initializes; a persistent single-node manual service needs one
initial bootstrap; manual multi-node startup uses the same full roster everywhere and exactly one fresh
node bootstraps. Other nodes, Genesis, Adopt, normal restarts, and already initialized/durable nodes do
not use it. Repeating initialization being a no-op is not a restart or membership-change strategy.

- [ ] **Step 2: Distinguish stock rr-server semantics**

Where stock commands appear, explain that its bootstrap initializes only the local single-voter group
and differs from kv-store's full-roster initialization. Do not rewrite correct existing playbook,
upgrade, Kubernetes, or historical release documents.

- [ ] **Step 3: Add the optional README logging script**

Embed a foreground supervised Bash example that builds kv-store/rr-admin, starts nodes 1–3 on dynamic
base-port-derived addresses, redirects each node's stdout/stderr to its own log, records exact PIDs,
waits in the foreground, and on Ctrl-C signals/waits only those PIDs. Preserve logs and remove only the
script's PID files. Include log-tail and three-node status commands and an idle-base-port override.

- [ ] **Step 4: Verify documentation and script**

Run `git diff --check`, Markdown line-length checks on changed lines, `bash -n` on the extracted script,
and the docs build. Functionally run the script only on confirmed-free non-live ports; verify file logs,
consistent leader/membership, then interrupt it and confirm only its children exited.

### Task 3: Independent Review and Final Acceptance

**Files:**
- Review all Task 1–2 diffs and test evidence; no planned production files.

**Interfaces:**
- Consumes: both task reports, focused test results, and the complete diff.

- [ ] **Step 1: Run GitNexus change analysis**

Run `detect_changes` for the complete worktree. Do not treat partial/truncated output as clean; report
pre-existing unrelated changes separately from this task's files.

- [ ] **Step 2: Obtain one independent high-risk review**

Review correctness, rolling compatibility, error handling, distributed test quality, documentation
accuracy, script signal safety, and minimal scope. Do not rerun identical tests on unchanged code.

- [ ] **Step 3: Resolve findings and verify the deliverable**

Fix any confirmed Critical/Important findings through the implementer and scoped re-review. Run the
standard relevant build/tests after any change and report any unrelated pre-existing failures plainly.
