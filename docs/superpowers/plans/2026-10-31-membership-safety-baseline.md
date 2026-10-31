# Membership Safety Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make node removal permanent, prevent stale auto-promotion from restoring removed voters, and keep unified-machine-group configuration watches responsive.

**Architecture:** Add incremental OpenRaft membership primitives in `RaftGroupManager`, expose a typed single-node removal RPC, and make decommission use it for both voters and learner-only nodes. Keep the product's unified-machine-group semantics, but separate learner submission from catch-up waiting so an unreachable node cannot block the config watch.

**Tech Stack:** Rust 2021, Tokio, OpenRaft 0.10 alpha, Tonic/Prost 0.14, existing Cargo integration tests.

**Spec:** `docs/superpowers/specs/2026-10-31-homogeneous-cluster-lifecycle-design.md`

## Global Constraints

- All machines join all groups; this plan does not introduce per-group placement or resharding.
- Existing groups never call `initialize` during machine addition or removal.
- `partition → group` mappings remain unchanged.
- Removed nodes must be absent from both voters and learners/nodes.
- Auto-promotion remains enabled and must use incremental voter addition rather than stale full-set replacement.
- Match existing error conversion, audit logging, CLI and test conventions.
- Do not modify unrelated user changes under `sdks/java`.
- Do not commit or push unless the user explicitly requests it.
- Run GitNexus `impact` before editing each shared symbol and `detect_changes` before any requested commit.

## Review Focus

- Learner-only deletion: must remove the node rather than report `Skipped`; covered by Task 3's learner-only integration test.
- Unrelated learners: removing one node must not delete other joining learners; covered by Task 1's Batch semantics test.
- Unreachable learner: submission must return promptly and the config watch must continue processing later events; covered by Task 4.
- Quorum abort or partial completion: must produce a non-success CLI result; covered by Task 3's report tests.
- Concurrent promoter/removal: promotion must not re-add an older voter set; covered by Task 1's incremental API and Task 2's post-removal stability test.

---

### Task 1: Incremental Raft Membership Primitives

**Files:**
- Modify: `crates/rr-raft/src/group_manager.rs:346-428`
- Modify: `crates/rr-raft/src/learner_promoter.rs:180-375`
- Test: `tests/integration/src/membership_guard.rs`

**Interfaces:**
- Consumes: `openraft::ChangeMembers::{AddVoterIds, RemoveVoters, RemoveNodes, Batch}` and existing `RaftGroupManager::get_raft`.
- Produces:
  - `RaftGroupManager::promote_learners(group_id: GroupId, members: BTreeSet<NodeId>) -> Result<(), RrError>`
  - `RaftGroupManager::remove_node(group_id: GroupId, node_id: NodeId) -> Result<(), RrError>`
  - `RaftGroupManager::add_learner_nonblocking(group_id: GroupId, node_id: NodeId, addr: String) -> Result<(), RrError>`

- [ ] **Step 1: Write failing membership regression tests**

Add integration coverage proving that:
- `remove_node` on a voter removes it from voters and learners, while another learner remains present.
- `remove_node` also works for a learner-only node.
- `promote_learners` uses `AddVoterIds`, so a voter removed after promotion planning is not restored by the promotion call.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p integration --lib membership_guard -- --nocapture`
Expected: FAIL because the incremental manager APIs do not exist and removal currently retains learners.

- [ ] **Step 3: Implement the three manager methods**

Use `raft.change_membership(ChangeMembers::AddVoterIds(members), true)` for promotion.
For a current voter, use `RemoveVoters({node_id})` with `retain=false`; OpenRaft keeps the node entry through joint consensus and removes it during uniform flattening. For a learner-only node, use `RemoveNodes({node_id})`. Determine the role from current Raft metrics, and preserve unrelated learners.
Keep the existing blocking `add_learner` for compatibility; the new nonblocking method passes `false`.

- [ ] **Step 4: Switch auto-promotion to incremental addition**

Replace the live full-voter-set construction and `change_membership` call with `promote_learners(group_id, promotable)`. Retain the existing check that every planned learner still exists, and update comments/metrics only where behavior changed.

- [ ] **Step 5: Run focused verification**

Run: `cargo test -p integration --lib membership_guard -- --nocapture`
Run: `cargo test -p rr-raft learner_promoter -- --nocapture`
Expected: PASS; no full-set auto-promotion remains.

- [ ] **Step 6: Review checkpoint**

Inspect the diff and GitNexus impact results. Do not commit unless requested.

### Task 2: Typed Admin Single-Node Removal

**Files:**
- Modify: `crates/rr-network/proto/admin.proto:6-51,127-158`
- Modify: `crates/rr-server/src/admin.rs:534-784`
- Test: `crates/rr-server/src/admin.rs`
- Test: `tests/integration/src/membership_guard.rs`

**Interfaces:**
- Consumes: Task 1's `RaftGroupManager::remove_node`.
- Produces:
  - `rpc RemoveGroupMember(RemoveGroupMemberRequest) returns (Empty)`
  - `RemoveGroupMemberRequest { uint64 group_id = 1; uint64 node_id = 2; bool force = 3; }`

- [ ] **Step 1: Add failing server and end-to-end tests**

Cover unknown group, rejection when removing a voter would leave fewer than `MIN_VOTERS_AFTER_REMOVAL` without `force`, successful voter removal, and successful learner-only removal. Assert the final `GetGroupMembership` contains neither voter nor learner for the target.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rr-server admin::tests -- --nocapture`
Run: `cargo test -p integration --lib membership_guard -- --nocapture`
Expected: FAIL because `RemoveGroupMember` is not defined.

- [ ] **Step 3: Add the proto contract and regenerate through the existing build**

Add the RPC and request message without changing existing field numbers or `ChangeMembership` semantics. Update the proto comment so it no longer implies decommission must use retained learners.

- [ ] **Step 4: Implement the AdminService handler**

Mirror `ChangeMembership`'s group existence, guard, audit and error-mapping conventions. The handler calls `remove_node`; `force` only overrides the minimum-voter policy and cannot remove the final voter.

- [ ] **Step 5: Run focused verification**

Run: `cargo test -p rr-server admin::tests -- --nocapture`
Run: `cargo test -p integration --lib membership_guard -- --nocapture`
Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Inspect the generated-code diff and public API surface. Do not commit unless requested.

### Task 3: Complete and Truthful Decommission

**Files:**
- Modify: `crates/rr-admin/src/decommission.rs:1-466`
- Modify: `crates/rr-admin/src/main.rs:590-635`
- Test: `crates/rr-admin/src/decommission.rs:468-607`
- Test: `tests/integration/src/membership_guard.rs`

**Interfaces:**
- Consumes: Task 2's `AdminServiceClient::remove_group_member`.
- Produces: decommission removes voters and learner-only nodes and treats every incomplete outcome as unsuccessful.

- [ ] **Step 1: Write failing report and workflow tests**

Add assertions that `AbortedQuorumLoss` makes `has_failures()` true, learner-only targets are not skipped, confirmation checks both voters and learners, and partial completion cannot print an all-success message.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rr-admin decommission -- --nocapture`
Expected: FAIL on the current `Skipped`, `has_failures()` and confirmation behavior.

- [ ] **Step 3: Update decommission workflow**

Use `RemoveGroupMember` instead of full-set `ChangeMembership`. Skip only nodes absent from both voters and learners; learner-only targets require neither quorum pre-check nor leadership drain. Confirm both collections after mutation and update stale workflow comments.

- [ ] **Step 4: Align CLI result handling**

Include `AbortedQuorumLoss`, coverage gaps, unknown outcomes and per-group failures in the non-zero path. Print an incomplete/decommission-blocked summary rather than "target was not a voter" or "all eligible groups processed successfully".

- [ ] **Step 5: Add the post-removal stability integration test**

After removing an online voter, wait longer than the default three 5-second promotion stability ticks and assert the target remains absent from voters and learners. Keep condition-based polling for the mutation itself; the extended wait specifically guards against auto-promotion regression.

- [ ] **Step 6: Run focused verification**

Run: `cargo test -p rr-admin decommission -- --nocapture`
Run: `cargo test -p integration --lib membership_guard -- --nocapture`
Expected: PASS; removed nodes do not return.

- [ ] **Step 7: Review checkpoint**

Inspect report compatibility and error paths. Do not commit unless requested.

### Task 4: Nonblocking Unified-Group Watch Submission

**Files:**
- Modify: `crates/rr-server/src/node.rs:486-503`
- Test: `tests/integration/src/lib.rs:1110-1152`

**Interfaces:**
- Consumes: Task 1's `add_learner_nonblocking` and Task 2's removal semantics.
- Produces: `NodeAdded` still targets every local group, but returns after learner submission rather than waiting for catch-up.

- [ ] **Step 1: Strengthen the dynamic-node test**

Register an unreachable node, assert learner submission completes within a bounded deadline, then apply a subsequent config change and prove the watch processes it. Assert unrelated `partition → group` mappings remain unchanged.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p integration --lib test_dynamic_node_add -- --nocapture`
Expected: FAIL or exceed the bounded deadline because the current watch awaits blocking learner catch-up.

- [ ] **Step 3: Use nonblocking submission in the watch**

Call `add_learner_nonblocking` for every local group, preserving unified-machine-group semantics and existing audit/error logging. Do not initialize any existing group or wait for promotion in this task.

- [ ] **Step 4: Run focused and regression verification**

Run: `cargo test -p integration --lib test_dynamic_node_add -- --nocapture`
Run: `cargo test -p integration --lib membership_guard -- --nocapture`
Run: `cargo test -p rr-raft -p rr-server -p rr-admin`
Expected: PASS.

- [ ] **Step 5: Review checkpoint**

Run `cargo fmt --check`, inspect `git diff --check` and `git status --short`, and verify no generated logs or unrelated files were added. Do not commit unless requested.
