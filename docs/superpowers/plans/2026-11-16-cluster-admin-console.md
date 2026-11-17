# Rr Cluster Admin Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the approved P0 self-contained, read-only Rr cluster admin console with trusted discovery,
truthful node/Raft/resource/snapshot/lifecycle data, nine accessible UI views, and real-cluster verification.

**Architecture:** Rr nodes publish explicit endpoint descriptors and expose additive AdminService capabilities.
`rr-admin console` aggregates Admin and Prometheus data through one bounded Rust BFF and serves a static Next.js
export. The browser never receives node Admin credentials and never drives upstream polling loops.

**Tech Stack:** Rust 2021/Tokio/Tonic/Axum, etcd, Prometheus, React 19, Next.js App Router static export,
TypeScript strict, Tailwind CSS v4, shadcn/ui, TanStack Table, Recharts, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-15-cluster-admin-console-design.md`

## Global Constraints

- P0 is read-only: do not expose membership, snapshot-trigger, drain, reset, or process-restart mutations.
- Never infer Admin/observability ports; never forward Admin tokens to an untrusted discovered address.
- Preserve every existing RPC/field behavior, including legacy `leader_node_id=0` and the snapshot stub.
- Unknown values are absent/`UNSUPPORTED`, never zero. Do not average P99 or subtract unaligned latency cohorts.
- NodeDirectory is separate from ConfigProvider/LifecycleStore; lease heartbeat never rewrites descriptor JSON.
- Default page size 50, maximum 200; persistent journals retain at most 100 boots; mem nodes are VOLATILE.
- Non-loopback Console requires HTTPS + viewer authentication; `--allow-insecure` is loopback-only.
- Every metrics family added to code must be added to `docs/docs/reference/metrics-reference.md`.
- Frontend lives only in `crates/rr-admin/console/**`; BFF owns `crates/rr-admin/src/console/**` and OpenAPI.
- Do not modify existing user files outside the assigned scope, do not commit, and do not create unrelated tests,
  reports, scaffolding, or documentation. All new tests must follow the repository's existing test conventions.
- Docker images are local-only. Do not access external registries or enable hosted CI.

## Review Focus

- Stale/non-atomic Raft observations: normal term/membership transitions must not become conflict alerts (Task 2,
  Task 8 tests).
- Address replacement and legacy identity: untrusted endpoints and mixed empty/non-empty cluster UUIDs must be
  rejected before credential forwarding (Tasks 1, 3, 8 tests).
- Unsupported and reset metrics: missing series, counter reset, old-node UNIMPLEMENTED, and node-only snapshot
  failures must not become zero or per-group facts (Tasks 2, 3, 8 tests).
- Browser fan-out and oversized upstream data: 20 subscribers must not increase polling QPS; body/series/sample
  limits must stay enforced (Tasks 3, 8, 9 tests).
- Accessibility and degraded rendering: every graph/chart has a table, all nine routes remain usable with partial,
  stale, unsupported, and empty data (Tasks 4–7, 9 tests).

---

### Task 1: Shared Compatibility and Leased NodeDirectory

**Files:**
- Create: `crates/rr-core/src/compatibility.rs`, `crates/rr-core/src/directory.rs`
- Modify: `crates/rr-core/src/lib.rs`, `crates/rr-core/src/error.rs`
- Create: `crates/rr-config-etcd/src/directory.rs`
- Modify: `crates/rr-config-etcd/src/lib.rs`, `crates/rr-config-etcd/tests/e2e_real_etcd.rs`

**Interfaces:**
- Produces: `ProtocolVersion { major, minor, min_peer_minor }`, `raft_pair_compatible(a, b)`.
- Produces: `NodeDescriptor`, `NodeDirectoryRecord`, `NodeDirectoryPage`, `NodeDirectoryChange`.
- Produces: object-safe `NodeDirectory::{get,list,watch,register}` and
  `NodeDirectoryRegistration::{record,heartbeat,revoke}` as specified in the approved design.
- Produces: `EtcdNodeDirectory` using `/rr/{cluster}/directory/nodes/{node_id}`, 15s default TTL, stable
  revision cursor, lease-owned create/revoke, and no heartbeat PUT.

- [ ] **Step 1: Write failing core/schema tests** for asymmetric protocol minor boundaries, major mismatch,
  unknown versions, wildcard addresses, unknown JSON fields, node-ID mismatch, 50/200 page limits, and cursor
  stability.
- [ ] **Step 2: Run focused tests and confirm failure** with `cargo test -p rr-core directory compatibility`.
- [ ] **Step 3: Implement core and etcd contracts** with atomic register conflict handling, lease keep-alive,
  revoke/re-register, watch compaction resync, and schema validation. Do not change existing routing keys.
- [ ] **Step 4: Add real-etcd gated tests** for lease expiry, ownership conflict, revoke, watch deletion, stable
  pagination, and unchanged `mod_revision` after heartbeat.
- [ ] **Step 5: Run verification** with `cargo test -p rr-core -p rr-config-etcd`; run real-etcd tests only when
  the existing local `RR_TEST_ETCD=1` environment is available.

### Task 2: Node Runtime, Build/Boot/Resource Data, and Additive Admin RPCs

**Files:**
- Modify: `crates/rr-network/proto/admin.proto`
- Modify: `crates/rr-admin/src/decommission.rs` (generated trait stub only)
- Create: `crates/rr-server/src/{runtime.rs,boot_journal.rs,resources.rs}`
- Modify: `crates/rr-server/src/{admin.rs,node.rs,config.rs,cli.rs,config_file.rs,main.rs,lib.rs,Cargo.toml}`
- Modify: `crates/rr-server/build.rs` if build metadata is not otherwise available
- Test: existing inline `rr-server` tests and new focused module tests

**Interfaces:**
- Consumes: Task 1 protocol and directory contracts.
- Produces additive RPCs: `GetBuildInfo`, `GetNodeDescriptor`, `GetClusterTopology`,
  `GetGroupReplicaStatus`, `GetBootStatus`, `ListBootRecords`.
- Produces: shared boot identity/runtime used by journal, BuildInfo, and directory registration.
- Produces: explicit advertise/build/journal configuration; no wildcard advertisement.
- Produces: optional leader-only replication view; legacy Admin behavior remains unchanged.
- Produces resource families `rr_node_cpu_usage_ratio`, `rr_node_cpu_cores`, `rr_node_cpu_limit_cores`,
  `rr_node_memory_bytes`, `rr_node_network_bytes_total`, `rr_node_filesystem_bytes` with explicit scope/kind.

- [ ] **Step 1: Write failing tests** for no-leader `None`, follower without leader view, missing matched without
  lag, local/cluster commit separation, stable topology pages, expired cursor, journal abnormal recovery,
  VOLATILE mode, 100-boot retention, resource counter resets, and unsupported values omitted.
- [ ] **Step 2: Run tests and confirm additive APIs are absent** with
  `cargo test -p rr-server admin::tests boot_journal resources`.
- [ ] **Step 3: Implement runtime and boot journal** with one boot ID, deterministic secret-free config hash,
  atomic persistence, default Rocks journal path, explicit mem persistence opt-in, and graceful/abnormal states.
- [ ] **Step 4: Implement Admin mappings and topology snapshots** without changing old response shapes; update the
  manual Admin stub in `rr-admin` immediately after proto generation.
- [ ] **Step 5: Implement bounded resource sampling and directory lifecycle** with tracked tasks, explicit
  descriptors, heartbeat reconciliation, startup cleanup, and revoke-on-shutdown.
- [ ] **Step 6: Run verification** with `cargo test -p rr-network -p rr-server -p rr-admin` and
  `scripts/check-metrics-docs.sh` after the documentation task lands metric names.

### Task 3: Console CLI, Legacy Vertical Slice, Security, and Static Hosting

**Files:**
- Create: `crates/rr-admin/src/console.rs`, `crates/rr-admin/src/console/**`
- Modify: `crates/rr-admin/src/main.rs`, `crates/rr-admin/Cargo.toml`
- Create: `crates/rr-admin/openapi/console-v1.json`
- Test: `crates/rr-admin/src/console/**` inline tests, `crates/rr-admin/tests/console_process.rs`

**Interfaces:**
- Produces: `rr-admin console --listen ... [--peers ...|--discovery ... --cluster-name ...]`.
- Produces: same-origin `/api/v1/console/*`, `/stream`, static SPA fallback, and uniform `meta` with source,
  freshness, capabilities, partial, correlation ID, and retention.
- Produces: shared `Upstream` abstraction, `ConsoleCache`, single-flight scheduler, cursor pagination,
  bounded event/time-series rings, and legacy normalization `leader_node_id=0 → None`.
- Produces: loopback viewer optional; non-loopback HTTPS + `RR_CONSOLE_TOKEN`, in-memory expiring sessions,
  Secure/HttpOnly/SameSite=Strict cookies, CSRF + Origin checks for reconcile.

- [ ] **Step 1: Write failing CLI/API/security tests** for old command parsing, peer parsing, non-loopback
  rejection, session expiry, CSRF/Origin, API-vs-SPA 404, legacy UNIMPLEMENTED degradation, stale cache, and
  uniform partial errors.
- [ ] **Step 2: Run focused tests and confirm failure** with `cargo test -p rr-admin console`.
- [ ] **Step 3: Implement the seed-only vertical slice** using existing status/group/membership/snapshot/routing/
  formation RPCs and explicit observability addresses; keep all new Console dispatch before legacy `--addr` use.
- [ ] **Step 4: Implement scheduler/cache/SSE** with bounded concurrency, deadlines, cancellation, per-node
  circuit breaker, shared subscribers, and lag-triggered client resync.
- [ ] **Step 5: Implement auth and embedded static hosting** against `crates/rr-admin/console/out`; ordinary Rust
  debug builds must not require Node or a pre-existing export.
- [ ] **Step 6: Run verification** with `cargo test -p rr-admin` and the new process-level security tests.

### Task 4: Frontend Contract, Theme, Shell, and Shared Evidence Components

**Files:**
- Create: `crates/rr-admin/console/package.json`, lockfile, Next/TS/Tailwind/shadcn/Vitest configs
- Create: `crates/rr-admin/console/src/app/**`, `src/components/ui/**`, `src/components/console/**`, `src/lib/**`
- Create: `crates/rr-admin/console/e2e/mock/server.mjs`, shared fixtures and Playwright config

**Interfaces:**
- Consumes: Task 3 OpenAPI and `console/out` contract.
- Produces: typed API client, query-state helpers, formatting, freshness/source/capability models.
- Produces: shell/routes/login, `HealthBadge`, `FreshnessBadge`, `SourceEvidence`, `CapabilityNotice`,
  dense table, chart-with-table, drawer/dialog, empty/degraded states, and mock SSE server.
- Produces: Rust-orange tokens and reference dimensions exactly as the approved design specifies.

- [ ] **Step 1: Write failing unit/component tests** for absent versus zero, stale/partial evidence, legacy leader
  normalization display, keyboard drawer focus, reduced motion, and secret-free formatting.
- [ ] **Step 2: Run tests and confirm failure** with `npm --prefix crates/rr-admin/console run test:unit`.
- [ ] **Step 3: Implement the isolated static-export app** without changing Docusaurus files; use browser-side
  same-origin API/SSE only and keep details in drawers/query parameters.
- [ ] **Step 4: Run frontend gates** with `npm --prefix crates/rr-admin/console run typecheck`,
  `npm --prefix crates/rr-admin/console run lint`, `npm --prefix crates/rr-admin/console run test:unit`, and
  `npm --prefix crates/rr-admin/console run build`.
- [ ] **Step 5: Verify the shell in the actual static export** with Playwright at 1440×900 and 1280×800, including
  keyboard navigation, no page overflow, and no console errors.

### Task 5: Overview, Nodes, and Lifecycle Views

**Files:**
- Create/modify only: `console/src/app/(cluster)/{overview,nodes,lifecycle}/**`
- Create: feature-specific components under `console/src/components/console/{overview,nodes,lifecycle}/**`
- Test: corresponding unit specs and `console/e2e/mock/{overview,nodes,lifecycle}.spec.ts`

**Interfaces:**
- Consumes: Task 4 shared shell/components/API; does not edit shared files.
- Produces: overview drilldowns, node table + five detail tabs, restart timeline and version matrix.

- [ ] **Step 1: Add failing mock scenarios** for healthy/partial/stale/unsupported nodes, resource scopes,
  VOLATILE history, abnormal-or-uncertain boots, dirty builds, and incompatible replica versions.
- [ ] **Step 2: Implement the three views** with filters/pagination, evidence links, boot history pagination,
  node resource detail, and no invented restart/crash reasons.
- [ ] **Step 3: Run view unit/build/mock tests** and verify all drilldowns preserve query state.

### Task 6: Raft Groups and Three Topology Views

**Files:**
- Create/modify only: `console/src/app/(cluster)/{groups,topology}/**`
- Create: feature-specific components under `console/src/components/console/{groups,topology}/**`
- Test: corresponding unit specs and `console/e2e/mock/{groups,topology}.spec.ts`

**Interfaces:**
- Consumes: Task 4 shared shell/components/API; does not edit shared files.
- Produces: group list + replica evidence matrix and nodes/Raft/routes topology with equivalent tables.

- [ ] **Step 1: Add failing mock scenarios** for leaderless, syncing, same-term conflict, leader-only matched/lag,
  absent RTT, large-group fallback, and keyboard graph/table switching.
- [ ] **Step 2: Implement groups and topology** without atomic-poll conflict false positives, rainbow graph
  encoding, physical-network claims, or unbounded graph rendering.
- [ ] **Step 3: Run view unit/build/mock tests** and verify group/node/partition deep links and accessible tables.

### Task 7: Performance, Snapshots, Events, and Sources Views

**Files:**
- Create/modify only: `console/src/app/(cluster)/{performance,snapshots,events,sources}/**`
- Create: feature-specific components under `console/src/components/console/{performance,snapshots,events,sources}/**`
- Test: corresponding unit specs and `console/e2e/mock/{performance,snapshots,events,sources}.spec.ts`

**Interfaces:**
- Consumes: Task 4 shared shell/components/API; does not edit shared files.
- Produces: fixed-template performance analysis, snapshot/storage evidence, alert/event views, and source health.

- [ ] **Step 1: Add failing mock scenarios** for UNALIGNED_COHORTS, counter resets, no snapshot, node-only build
  failure, unsupported group cells, missing history, Prometheus mapping errors, and reconcile feedback.
- [ ] **Step 2: Implement the four views** with chart alternatives, exact retention labels, no latency subtraction,
  no snapshot bool progress, no fake crash/alert history, and sanitized source errors.
- [ ] **Step 3: Run view unit/build/mock tests** and verify filters, acknowledge/silence preferences, and source
  drilldowns.

### Task 8: Full BFF Discovery, New Capabilities, Metrics, and History Integration

**Files:**
- Modify only: `crates/rr-admin/src/console/**`, `crates/rr-admin/openapi/console-v1.json`
- Test: BFF inline/process tests; avoid Task 3 CLI/manifest files unless a missing dependency is reported.

**Interfaces:**
- Consumes: Tasks 1–3 contracts and legacy BFF.
- Produces: trusted NodeDirectory+ConfigProvider reconciliation, capability-aware projections for all nine APIs,
  bounded Prometheus text scraping, reset-safe rates, histogram quantiles, and fixed historical queries.
- Produces: mandatory `--prometheus-cluster label=value` and stable `node_id` target mapping; no instance guessing.

- [ ] **Step 1: Write failing integration tests** for UUID conflicts, Untracked assertions, lease expiry, untrusted
  address replacement, legacy seeds, body/series caps, leader-predicated lag, histogram aggregation, counter reset,
  snapshot distance, and multi-cluster Prometheus isolation.
- [ ] **Step 2: Implement discovery and capability integration** without changing the NodeDirectory schema or
  adding a second etcd implementation.
- [ ] **Step 3: Implement metrics/history projections** with raw-family fallback when recording rules drop labels,
  no arbitrary PromQL, no cross-cohort latency composition, and explicit unsupported/missing states.
- [ ] **Step 4: Run verification** with `cargo test -p rr-admin` and 20-subscriber upstream-QPS tests.

### Task 9: Real Three-Node E2E, Scale, and Regression Evidence

**Files:**
- Create: `crates/rr-admin/console/e2e/real/**`, shared mock security/accessibility specs
- Create: `crates/rr-admin/tests/console_scale.rs` if not already covered by Task 8
- Modify only test wiring explicitly needed by the new suites

**Interfaces:**
- Consumes: completed backend, BFF, and static frontend.
- Produces: separate-process RocksDB three-node harness, actual Console BFF, workload/snapshot controls,
  kill/restart/failover scenarios, and scale evidence under `target/console-e2e`.

- [ ] **Step 1: Implement the real harness** with explicit ports, guaranteed child cleanup, two Admin seeds,
  workload generation, persistent journals, and no external registry dependency.
- [ ] **Step 2: Run all nine real views** then kill a leader, verify partial/stale/events/new leader, restart with
  the same data dir, verify boot/version history and SSE recovery.
- [ ] **Step 3: Run mock accessibility/security suites** including axe per route, keyboard/focus/reduced-motion,
  session/storage/log token assertions, and responsive overflow checks.
- [ ] **Step 4: Run the real scheduler/parser scale gate** with 50 simulated upstream processes × 500 groups and
  assert the approved API/QPS/memory/series/sample budgets.
- [ ] **Step 5: Preserve only necessary test evidence** and remove generated reports/logs not needed for delivery.

### Task 10: Documentation, Deployment Labels, and Build Recipes

**Files:**
- Modify: `justfile`, `.dockerignore`, `docs/docs/reference/metrics-reference.md`,
  `docs/docs/guide/config-provider.md`, relevant configuration/reference docs
- Modify: `deploy/lab/prometheus.yml`, `deploy/perf/prometheus.yml` and explicit advertise/journal examples
- Create: focused Console user/operations documentation in the existing docs navigation

**Interfaces:**
- Consumes: final CLI/config/metric contracts from Tasks 1–9.
- Produces: `console-install/build/test` recipes, secure startup guidance, discovery trust model, retention limits,
  node_id target labels, and P0 unsupported capabilities.

- [ ] **Step 1: Document exact shipped behavior** including static export build ordering, debug builds without
  Node, non-loopback security, NodeDirectory provisioning, boot journal reset behavior, and Prometheus isolation.
- [ ] **Step 2: Reconcile metric documentation** with every added family and remove any implication that unaligned
  latency cohorts can be subtracted.
- [ ] **Step 3: Run verification** with `scripts/check-metrics-docs.sh`, `just docs-build`, and recipe smoke tests.

### Task 11: Final Independent Review and Delivery Verification

**Files:**
- Review only: complete diff and all evidence; no feature expansion.

- [ ] **Step 1: Run GitNexus impact/detect-changes** and inspect all HIGH/CRITICAL/UNKNOWN findings with direct
  references rather than waiving them.
- [ ] **Step 2: Run standard verification** with `cargo test --workspace`, frontend typecheck/lint/unit/build/mock,
  real three-node E2E, metrics-doc check, docs build, and `git diff --check`.
- [ ] **Step 3: Independently review correctness, security, compatibility, tests, performance, and simplicity**;
  fix only confirmed P0 defects and rerun affected checks.
- [ ] **Step 4: Report delivered files, verified commands/results, scale evidence, and any genuinely unavailable
  environment-dependent checks** without presenting unverified work as complete.
