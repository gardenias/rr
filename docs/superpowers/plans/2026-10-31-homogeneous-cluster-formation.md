# Homogeneous Cluster Formation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a uniform machine set form a new cluster without a designated bootstrap node, recover Mem/Rocks restarts without replaying the seed, and adopt legacy clusters without re-initialization.

**Architecture:** Add a canonical `SeedSpec` and a separate linearizable `LifecycleStore` with Mem and Etcd backends. A single `RrNode` formation supervisor coordinates CAS-authorized genesis, local identity, recovery and legacy adoption; stock server and kv-store only assemble configuration. Dynamic machine membership and replica replacement remain P2.

**Tech Stack:** Rust 2021, Tokio, OpenRaft 0.10 alpha, etcd-client 0.14, serde/bincode, SHA-256, UUID, Tonic.

**Spec:** `docs/superpowers/specs/2026-10-31-homogeneous-cluster-lifecycle-design.md`

## Global Constraints

- All groups use the same initial machine set; no cross-group placement and no resharding.
- Existing groups never initialize during restart, adoption or later machine addition.
- `partition → group` remains unchanged.
- Genesis requires a linearizable lifecycle CAS and identical seed hash/roster; local pristine alone is never authorization.
- `NotAllowed` is not success until local state and actual Genesis membership have been re-read and verified.
- Mem recovery promises only recovery through surviving replicas; all-Mem data loss is fail-closed, not durable recovery.
- Legacy adoption never expands membership automatically; the existing formation job/manual learner path remains responsible.
- Legacy CLI/bootstrap behavior stays unchanged when formation is not configured.
- No dynamic Joining/Retiring operation engine, automatic replica replacement, versioned watch/resync protocol or disaster-recovery epoch in P1.
- Run GitNexus impact before shared edits and detect-changes before commits; no unrelated `sdks/java` changes.

## Review Focus

- Mixed local state during adoption: must not let a pristine node create a legacy record; covered by Task 4's adoption matrix tests.
- CAS conflict after another node used a different seed: must fail rather than adopt the winner; covered by Tasks 2 and 4.
- `NotAllowed` with a vote but no matching membership: must not mark Active; covered by Tasks 3 and 4.
- Store outage with pristine local state: zero initialize and not ready; covered by Tasks 4 and 6.
- Active group after Mem restart or a late initial node: recover by replication only; covered by Task 6.
- Existing manual cluster: behavior unchanged without formation config; covered by Tasks 5 and 7.

---

### Task 1: Canonical Seed and Lifecycle Contracts

**Files:**
- Create: `crates/rr-core/src/lifecycle.rs`
- Modify: `crates/rr-core/src/lib.rs`, `crates/rr-core/src/error.rs`, `crates/rr-core/Cargo.toml`
- Modify: `Cargo.toml`, `Cargo.lock`

**Interfaces:**
- Produces:
  - `SeedSpec { version: u32, cluster_name: String, nodes: BTreeMap<NodeId, NodeAddr>, groups: BTreeMap<GroupId, SeedGroup>, partitions: BTreeMap<PartitionId, SeedPartition> }`
  - `SeedGroup { voters: BTreeSet<NodeId> }`, `SeedPartition { config: PartitionConfig, group_id: GroupId }`
  - `SeedSpec::validate()`, `seed_hash()`, `initial_routing_table()`, `stock(cluster_name, nodes, group_count)`
  - `ClusterLifecycle`, `GroupFormation`, `FormationState::{Forming, Active}`, `ClusterOrigin::{Genesis, LegacyAdopted}`
  - `LifecycleStore::{load_linearizable, create_if_absent, compare_and_swap}` and `CasOutcome::{Applied, Conflict}`
  - `VersionedLifecycle { revision: u64, record: ClusterLifecycle }`

- [ ] **Step 1: Write failing schema and golden-hash tests**

Cover order-independent hashing, every semantic field changing the hash, a fixed v1 golden vector, empty/zero IDs, non-uniform voters, unknown groups, `Range`, invalid advertised addresses and stock `partition i → group i` generation. Add transition tests that Active cannot return to Forming and bootstrap epoch cannot change.

- [ ] **Step 2: Run RED**

Run: `cargo test -p rr-core lifecycle -- --nocapture`
Expected: FAIL because `lifecycle` types do not exist.

- [ ] **Step 3: Implement contracts and canonical hashing**

Use BTree collections for canonical ordering and domain-separated SHA-256 over `rr/seed-spec/v1\0` plus bincode-standard encoding. Add workspace `sha2 = "0.10"` and `uuid = { version = "1", features = ["v4", "serde"] }`; these capabilities are not currently present. Keep store revision separate from topology revision. Add `RrError::Lifecycle` and a typed `LifecycleError` that distinguishes unavailable, corrupt data, unsupported schema, invalid transition and identity/seed mismatch.

- [ ] **Step 4: Run GREEN**

Run: `cargo test -p rr-core lifecycle -- --nocapture`
Expected: PASS, including the golden hash.

### Task 2: Mem and Etcd Lifecycle Stores

**Files:**
- Create: `crates/rr-config/src/lifecycle.rs`, `crates/rr-config-etcd/src/lifecycle.rs`
- Modify: both crates' `src/lib.rs` and `rr-config-etcd/tests/e2e_real_etcd.rs`
- Test: `crates/rr-config/src/lifecycle.rs`, `crates/rr-config-etcd/src/lifecycle.rs`

**Interfaces:**
- Produces `MemLifecycleStore` and `EtcdLifecycleStore`, implementing Task 1's trait.
- Etcd key: `/rr/{cluster_name}/lifecycle/formation`.

- [ ] **Step 1: Write failing shared contract tests**

Test single create winner, stale CAS conflict, conflict-then-reread, Active persistence, malformed record rejection, independent Mem instances not coordinating, and backend unavailability not looking like `None`.

- [ ] **Step 2: Run RED**

Run: `cargo test -p rr-config lifecycle -- --nocapture`
Run: `cargo test -p rr-config-etcd lifecycle -- --nocapture`
Expected: FAIL because backends do not exist.

- [ ] **Step 3: Implement Mem CAS**

Store `Option<VersionedLifecycle>` behind one Tokio mutex; clones share one test control plane. Read/create/CAS are atomic under that lock and revisions increase monotonically.

- [ ] **Step 4: Implement Etcd CAS**

Use linearizable GET without serializable/cache, `Compare::version(key, Equal, 0)` for create and `Compare::mod_revision(key, Equal, expected)` for updates. Use strict JSON/schema validation and checked u64/i64 conversions. Reuse existing etcd connect options; never use the routing `RevisionCache`.

- [ ] **Step 5: Run GREEN and real Etcd contract**

Run both unit commands, then the real-etcd lifecycle test using the repository's existing testcontainers pattern. Expected: PASS; concurrent clients have one winner and stale writes never overwrite Active.

### Task 3: Formation-Aware Initialization and Local Identity

**Files:**
- Modify: `crates/rr-raft/src/group_manager.rs`
- Create: `crates/rr-server/src/identity.rs`
- Test: `tests/integration/src/membership_guard.rs`, `crates/rr-server/src/identity.rs`

**Interfaces:**
- Produces:
  - `InitializeOutcome::{Initialized, AlreadyInitialized}`
  - `RaftGroupManager::initialize_for_formation(group_id, members) -> Result<InitializeOutcome, RrError>`
  - `LocalClusterIdentity { version: u32, cluster_uuid: Uuid, node_id: NodeId }`
  - identity `load/store` using `StorageEngine` metadata CF key `rr/cluster-identity/v1`.

- [ ] **Step 1: Write failing initialization and identity tests**

Cover per-group lock re-entry behavior, `NotAllowed` mapped to `AlreadyInitialized`, actual re-read still required by the caller, UUID/node mismatch, corrupt identity, missing legacy identity and identity write failure.

- [ ] **Step 2: Run RED**

Run: `cargo test -p rr-raft initialize_for_formation -- --nocapture`
Run: `cargo test -p rr-server identity -- --nocapture`
Expected: FAIL because APIs do not exist.

- [ ] **Step 3: Implement minimal APIs**

Reuse the existing per-group membership lock. The manager returns the typed outcome but does not mark lifecycle Active. Identity helpers use versioned bincode and never silently replace a different UUID/node ID. Mem may lose identity on restart; the external lifecycle record remains authoritative.

- [ ] **Step 4: Run GREEN**

Run both focused commands and `cargo test -p integration --lib membership_guard -- --nocapture`.
Expected: PASS without changing legacy `init_cluster` behavior.

### Task 4: Single Formation Supervisor in RrNode

**Files:**
- Create: `crates/rr-server/src/formation.rs`
- Modify: `crates/rr-server/src/lib.rs`, `crates/rr-server/src/node.rs`, `crates/rr-server/src/health.rs`
- Test: `crates/rr-server/src/formation.rs`

**Interfaces:**
- Produces:
  - `FormationMode::{Genesis, AdoptLegacy}`
  - `FormationConfig { seed: SeedSpec, store: Arc<dyn LifecycleStore>, mode: FormationMode }`
  - `RrNode::new_with_formation(config: NodeConfig, formation: FormationConfig)`
  - `GroupFormationOutcome::{Initialized, Recovered, AwaitingReplication, AdoptedExisting}`

- [ ] **Step 1: Write failing decision-matrix tests**

Cover all-pristine Genesis, any-initialized Genesis rejection, all-initialized adoption, mixed-state adoption rejection, CAS seed conflict, UUID mismatch, Forming full-roster initialization, Active+pristine awaiting replication, late initial node, store outage pristine fail-closed, initialized recovery during outage, and `NotAllowed` with nonmatching membership not becoming Active.

- [ ] **Step 2: Run RED**

Run: `cargo test -p rr-server formation -- --nocapture`
Expected: FAIL because the supervisor does not exist.

- [ ] **Step 3: Implement the supervisor**

Use one code path for cluster CAS, local identity, initialize, actual-membership verification and Active CAS. Genesis Forming→Active requires actual voters and addresses equal to the seed; once Active, recovery uses current Raft membership and never replays or revalidates the seed roster. Adoption does not require the legacy single voter to equal the new seed. Only the node that actually initialized or verified an initialized Genesis group may mark it Active.

- [ ] **Step 4: Integrate without changing the legacy constructor path**

Keep `RrNode::new(config)` semantics. The formation constructor stores supervisor state and disables auto-promote and dynamic membership watch side effects in managed mode. Bind listeners before formation runs; on hard formation failure, startup returns error and readiness remains false. Run reentrant reconciliation in the background for readiness/recovery.

- [ ] **Step 5: Implement readiness phases**

Preserve existing JSON fields and add compatible phase/detail information. Managed readiness requires every hosted group to be formed and have an explicit observed leader. Store outage with initialized local state is degraded; pristine outage is not ready. Shutdown cancels formation before stopping groups.

- [ ] **Step 6: Run GREEN**

Run: `cargo test -p rr-server --lib`
Expected: PASS; legacy tests remain green.

### Task 5: Stock Server Configuration and Assembly

**Files:**
- Modify: `crates/rr-server/src/cli.rs`, `config_file.rs`, `main.rs`, `Cargo.toml`
- Test: `crates/rr-server/src/config_file.rs`, `main.rs`

**Interfaces:**
- Adds optional `[cluster] name`, `nodes = ["id@advertised_addr", ...]` and `[cluster.formation] mode = "genesis" | "adopt"`, `etcd_endpoints = [...]` plus corresponding CLI/env overrides.

- [ ] **Step 1: Write failing config tests**

Cover default legacy behavior, formation/bootstrap/peers conflicts, missing self, duplicate IDs, empty address, seed generation and bind-vs-advertised separation.

- [ ] **Step 2: Run RED**

Run: `cargo test -p rr-server config_file -- --nocapture`
Expected: FAIL on unknown formation fields.

- [ ] **Step 3: Assemble the managed path**

Build `SeedSpec::stock`, static routing, `EtcdLifecycleStore` and `new_with_formation`. Keep legacy peers parsing and manual bootstrap untouched. Managed mode does not start the old watch/promoter or run `bootstrap_hosted_groups`.

- [ ] **Step 4: Fix managed TCP ordering and cleanup**

Bind the TCP Raft listener before formation can run. Any listener/formation/start failure aborts the listener and shuts down groups/storage before returning the error.

- [ ] **Step 5: Run GREEN**

Run: `cargo test -p rr-server --lib`
Expected: PASS.

### Task 6: kv-store Reference and Mem/Rocks Recovery

**Files:**
- Modify: `examples/kv-store/src/main.rs`, `examples/kv-store/Cargo.toml`
- Create: `tests/integration/src/homogeneous_formation.rs`, `examples/kv-store/tests/homogeneous_process.rs`
- Modify: `tests/integration/src/lib.rs`, `tests/integration/Cargo.toml`

**Interfaces:**
- Adds `--formation genesis|adopt`, `--cluster-name`, `--etcd-endpoints`; existing full `--peers` roster becomes the SeedSpec. Legacy `--bootstrap` remains manual-only.

- [ ] **Step 1: Write failing in-process formation tests**

Cover concurrent 1/3/5-node genesis, roster permutation, conflicting seed without a second cluster, late initial node, Active+pristine never initializing, Mem rolling restart with ACK preservation, wiped follower recovery, store outage and unchanged partition routes. Use shared Mem lifecycle only for single-control-plane tests and independent routing providers.

- [ ] **Step 2: Write failing process tests**

Run three real kv-store processes with no `--bootstrap` and real Etcd. Assert one logical cluster, complete voters and KV reads/writes. Add Rocks restart using persistent directories and zero subsequent Genesis initialization evidence.

- [ ] **Step 3: Run RED**

Run: `cargo test -p integration --lib homogeneous_formation -- --nocapture`
Run: `cargo test -p kv-store --test homogeneous_process -- --nocapture`
Expected: FAIL because formation flags/wiring do not exist.

- [ ] **Step 4: Implement kv-store assembly**

Use the shared supervisor and static seed routing; do not call `initialize_with_retry` in managed mode. Enable `rr-storage/rocks` under `kv-store/rocks` so Rocks can persist framework identity while business KV semantics remain unchanged. Preserve demo and manual behavior.

- [ ] **Step 5: Run GREEN**

Run both test commands, `cargo test -p integration --features rocks --lib homogeneous_formation -- --nocapture`, and the Rocks process variant. Expected: PASS; no Mem all-wiped durability claim.

### Task 7: Legacy Adoption, Deployment and Final Verification

**Files:**
- Modify: `tests/integration/src/homogeneous_formation.rs`
- Modify: `deploy/helm/rr/values.yaml`, `deploy/helm/rr/templates/{configmap,statefulset,job-cluster-form}.yaml`
- Modify: `deploy/k8s/{statefulset,cluster-form-job}.yaml`, `deploy/compose/config/node*.toml`
- Modify: `docs/docs/{guide/getting-started,deployment/kubernetes,deployment/upgrading,guide/config-provider,reference/invariants}.md`, `README.md`, `CHANGELOG.md`

**Interfaces:**
- Preserves legacy formation job for adoption/manual recovery; fresh Genesis disables its ordinary formation responsibility.

- [ ] **Step 1: Add failing legacy adoption tests**

Start a legacy single-voter group with ACK data, adopt from initialized nodes, then start pristine nodes and prove zero initialize. Complete convergence through the existing learner/job path and verify ACK data and partition routes. Adoption from mixed/pristine-only local state must fail.

- [ ] **Step 2: Run RED**

Run: `cargo test -p integration --lib homogeneous_formation -- --nocapture`
Expected: FAIL on the new adoption scenarios.

- [ ] **Step 3: Update deployment and documentation**

Give all managed pods the same full advertised roster and formation settings; do not derive different bootstrap/peers per ordinal. Document fresh Genesis, ordered legacy adoption, store outage behavior, readiness and the fact that SeedSpec is not current membership. Keep job-based compatibility available.

- [ ] **Step 4: Run full verification**

Run: `cargo test`, relevant Rocks integration tests, real-etcd lifecycle tests, and `git diff --check`.
Expected: PASS with no unrelated file changes. Record any pre-existing formatting drift separately rather than rewriting the workspace.

- [ ] **Step 5: Review checkpoint**

Run GitNexus detect-changes with no partial/truncated result, review public API compatibility and generated deployment diffs, then present the local integration options. Do not push without explicit user choice.
