# Phase 3: Production Readiness for rr Distributed Framework

## TL;DR

> **Quick Summary**: Bridge rr from "feature-complete tech demo" (Phase 2) to "deployable, operable, auditable product." Phase 2 delivered multi-node Raft, persistence, client SDK, snapshots, and CI. Phase 3 delivers deployability (binary + artifacts), security (TLS + auth), operational tooling (backup/restore, runbooks), correctness hardening (fsync, panic cleanup), and release engineering (LICENSE, CHANGELOG, crates.io).
>
> **Deliverables**:
> - Production `rr-server` binary (CLI + config file + signal handling)
> - TLS (rustls) + authentication on all gRPC services
> - Backup/restore tooling
> - Deployment artifacts (Dockerfile, Helm chart, systemd)
> - RocksDB fsync correctness fix (CRITICAL BUG)
> - Distributed tracing + client-side metrics
> - LICENSE, CHANGELOG, CONTRIBUTING, published to crates.io
> - Operational runbooks (≥5 critical scenarios)
>
> **Estimated Effort**: XL (30+ tasks across Tier 0 blockers + Tier 1 important + Tier 2 post-v1)
> **Parallel Execution**: YES - 5 waves
> **Critical Path**: T2→T9→T10→T11→T17→T19→T20→T25→F1

---

## Context

### Original Request
User asked: "当前项目达到生产可用的级别还有哪些工作要做" (What remains before this project reaches production-ready level?)

### Audit Findings (4 parallel explore agents + direct tooling)
1. **Testing** (medium-high gaps): 18/24 integration tests active, 4 chaos tests `#[ignore]`d, zero Jepsen, critical modules undertested (`lookup.rs`=0, `node.rs`=3, `rr-client`=1)
2. **Security** (critical gaps): no TLS, no auth on any RPC, no rate limiting, AdminService fully open
3. **Operations** (critical gaps): no deployment artifacts, no backup/restore, no file config, rr-server binary is 4-line stub
4. **Observability** (medium gaps): 8 of 14 metrics undocumented, no distributed tracing, no client-side metrics
5. **Correctness** (serious): RocksDB fsync not configured (breaks Raft durability), 322 unwrap/expect calls, `RaftNode::new()` panics on init failure
6. **Features** (medium gaps): "linearizable reads" advertised but not implemented, no ChangeMembership RPC, only MemConfigProvider exists
7. **Release engineering** (critical): LICENSE file missing (README claims MIT), no CHANGELOG, not on crates.io

### Phase 2 Baseline (completed)
- ✅ Multi-node Raft over gRPC
- ✅ RocksDB persistent storage (feature-gated)
- ✅ Dynamic config via ConfigProvider trait
- ✅ Client SDK with auto leader-redirect
- ✅ Snapshot streaming
- ✅ CI pipeline (13 jobs: build, test, clippy, fmt, coverage, bench, msrv, doc-test, audit, nightly, macOS, integration, chaos)
- ✅ 24 integration tests + fault injection framework (built but disabled)

---

## Work Objectives

### Core Objective
Transform rr from Phase 2's "feature-complete framework" into a v1.0-ready product that an ops team can deploy, secure, back up, monitor, and recover — without writing deployment infrastructure from scratch.

### Concrete Deliverables
- `rr-server` binary (`crates/rr-server/src/main.rs` — real impl, not stub)
- `LICENSE` file (MIT, matching README)
- `CHANGELOG.md` + semver policy document
- `CONTRIBUTING.md` + `CODE_OF_CONDUCT.md`
- `DurabilityPolicy` enum + RocksDB fsync (correctness fix)
- TLS (rustls) + token auth on gRPC
- Backup/restore via `rr-admin` CLI
- Dockerfile + docker-compose + K8s StatefulSet example + Helm chart
- `docs/docs/operations/runbooks/*.md` (≥5 scenarios)
- `docs/docs/reference/metrics-reference.md` (complete, 14 metrics)
- OpenTelemetry traceparent propagation
- `rr-client` metrics instrumentation
- Published to crates.io (v0.2.0 as pre-release)

### Definition of Done
- [ ] v0.2.0 published to crates.io with all core crates
- [ ] A fresh ops team can go from zero to 3-node TLS+auth cluster in <1 hour using published artifacts
- [ ] Chaos tests (all 4) un-ignored and passing in CI
- [ ] RocksDB durability verified (sync on, survives kill -9 + power loss simulation)
- [ ] Backup/restore round-trip tested (backup → wipe data dir → restore → verify)
- [ ] All 14 metrics documented and scrapable from Prometheus
- [ ] Security audit clean: no unauthenticated admin RPCs, no plaintext in production config
- [ ] 0 warnings under `cargo clippy --workspace -- -D warnings`
- [ ] All existing tests continue to pass

### Must Have
- No regression of Phase 2 functionality
- TDD for all new production code
- Bilingual (CN+EN) prose in new docs where appropriate (match project style)

### Must NOT Have (Guardrails)
- ❌ No `anyhow` in library crates (only examples/tests)
- ❌ No `println!`/`eprintln!` in library crates
- ❌ No new `.unwrap()` in hot paths
- ❌ No `tokio::spawn` without `JoinHandle` tracking
- ❌ No breaking changes to Phase 2 public APIs without semver justification
- ❌ No hardcoded secrets, addresses, or ports
- ❌ No "TODO/FIXME" comments — any deferred work gets a GitHub issue

---

## Execution Strategy

### Parallel Execution Waves

```
Wave A (Week 1 — Quick Wins, no architectural decisions):
├── T1: LICENSE file [quick]
├── T2: RocksDB fsync fix (DurabilityPolicy) [quick]
├── T3: Metrics docs completion [quick]
├── T4: Un-ignore chaos tests + stabilize [deep]
├── T5: CHANGELOG.md [quick]
├── T6: CONTRIBUTING.md [writing]
├── T7: CODE_OF_CONDUCT.md [quick]
├── T8: PR template [quick]

Wave B (Weeks 2–3 — Deployability):
├── T9: rr-server binary (CLI + signals + wiring) [deep]
├── T10: TOML config file loader [deep]
├── T11: RaftNode::new() returns Result (no panic) [quick]
├── T12: Dockerfile + docker-compose for multi-node [deep]
├── T13: K8s StatefulSet example + Helm chart [deep]
├── T14: HTTP /health endpoint (for K8s probes) [quick]

Wave C (Weeks 4–6 — Security):
├── T15: TLS (rustls) support in gRPC server/client [deep]
├── T16: Bearer token auth on AdminService [deep]
├── T17: Optional auth on ClientService [deep]
├── T18: Rate limiting (tonic Layer) [deep]
├── T19: Audit log (immutable admin-op log) [deep]

Wave D (Weeks 7–8 — Operations):
├── T20: Backup command (rr-admin backup) [deep]
├── T21: Restore command (rr-admin restore) [deep]
├── T22: Runbook: disk full [writing]
├── T23: Runbook: leader flapping [writing]
├── T24: Runbook: network partition [writing]
├── T25: Runbook: snapshot corruption [writing]
├── T26: Runbook: ConfigProvider unreachable [writing]

Wave E (Weeks 9–10 — Observability + Release):
├── T27: OpenTelemetry traceparent propagation [deep]
├── T28: rr-client metrics (latency, retries, cache hits) [deep]
├── T29: #[instrument] on hot paths [quick]
├── T30: Semver policy document [writing]
├── T31: crates.io publish workflow [deep]
├── T32: v0.2.0 release (pre-release) [unspecified-high]

Wave FINAL (Independent review, 4 parallel):
├── F1: Plan compliance audit [oracle]
├── F2: Security review [oracle]
├── F3: Manual smoke test via Docker [unspecified-high]
├── F4: Upgrade path from Phase 2 → v0.2.0 [deep]

Critical Path: T2→T9→T10→T11→T15→T20→T22→T31→F1
Parallel Speedup: ~60% faster than sequential
Max Concurrent: 8 (Wave A)
```

### Dependency Matrix

| Task | Depends On | Blocks |
|------|-----------|--------|
| T1 (LICENSE) | — | T31, T32 |
| T2 (fsync) | — | F2, F3 |
| T3 (metrics docs) | — | — |
| T4 (chaos tests) | — | F1 |
| T5 (CHANGELOG) | — | T31, T32 |
| T6 (CONTRIBUTING) | — | — |
| T7 (COC) | — | — |
| T8 (PR template) | — | — |
| T9 (rr-server) | T10 (config), T11 (Result) | T12, T13, F3 |
| T10 (TOML config) | — | T9 |
| T11 (Result) | — | T9 |
| T12 (Docker) | T9 | T13, F3 |
| T13 (K8s/Helm) | T9, T12 | F3 |
| T14 (/health) | T9 | T13 |
| T15 (TLS) | T10 | T16, T17, F2 |
| T16 (admin auth) | T15 | F2 |
| T17 (client auth) | T15 | F2 |
| T18 (rate limit) | T15 | F2 |
| T19 (audit log) | T16 | F2 |
| T20 (backup) | — | T21 |
| T21 (restore) | T20 | F3 |
| T22–T26 (runbooks) | T9 (so they can reference production artifacts) | — |
| T27 (tracing) | — | — |
| T28 (client metrics) | — | — |
| T29 (#[instrument]) | — | — |
| T30 (semver) | — | T31, T32 |
| T31 (publish workflow) | T1, T5, T30 | T32 |
| T32 (v0.2.0) | T31 + all Wave A-D | F1, F4 |

### Agent Dispatch Summary

- **Wave A**: 8 — T1→`quick`, T2→`quick`, T3→`quick`, T4→`deep`, T5→`quick`, T6→`writing`, T7→`quick`, T8→`quick`
- **Wave B**: 6 — T9→`deep`, T10→`deep`, T11→`quick`, T12→`deep`, T13→`deep`, T14→`quick`
- **Wave C**: 5 — T15→`deep`, T16→`deep`, T17→`deep`, T18→`deep`, T19→`deep`
- **Wave D**: 7 — T20→`deep`, T21→`deep`, T22–T26→`writing`
- **Wave E**: 6 — T27→`deep`, T28→`deep`, T29→`quick`, T30→`writing`, T31→`deep`, T32→`unspecified-high`
- **FINAL**: 4 — F1→`oracle`, F2→`oracle`, F3→`unspecified-high`, F4→`deep`

---

## Key Decisions Required from User (BEFORE starting blocked tasks)

| Task | Decision Needed | Recommended Default |
|------|-----------------|---------------------|
| T15 (TLS) | mTLS required or optional? Cert rotation mechanism? Self-signed OK for dev? | Optional mTLS, cert paths in config, self-signed template provided |
| T16 (admin auth) | Bearer token in plaintext file? JWT with HMAC? K8s ServiceAccount? | Bearer token via `RR_ADMIN_TOKEN` env or config file; future JWT as opt-in |
| T17 (client auth) | Shared secret for all clients, or per-app tokens? | Optional token (empty = open); operator enables via config flag |
| T20/T21 (backup format) | Raw snapshot export, or logical (KV dump)? Compressed? Encrypted? | Raw snapshot (portable across rr versions pinned by major); gzip-compressed; opt-in AES encryption |
| T12/T13 (deploy artifacts) | Docker Hub namespace? Helm chart repo (OCI?)? | `ghcr.io/<owner>/rr`; OCI Helm repo on GitHub Packages |

---

## TODOs

### Wave A — Week 1 Quick Wins

#### T1. Create LICENSE File

**What to do**:
- Create `LICENSE` in repo root with MIT license text
- Copyright holder: match README or ask user
- Match the README's existing claim: "This project is licensed under the MIT License"

**Must NOT do**: Don't change license to something else unilaterally.

**Agent**: `quick`, no skills needed
**Files**: `/LICENSE` (new)
**Acceptance**:
- [ ] `LICENSE` exists with MIT text
- [ ] GitHub auto-detects as MIT (visible in repo sidebar)

---

#### T2. Fix RocksDB fsync (DurabilityPolicy)

**What to do**:
- Add `pub enum DurabilityPolicy { Strict, Balanced, Fast }` to `rr-storage/src/rocks.rs`
- Wire to `RocksConfig`:
  - `Strict` → `opts.set_sync(true)` + `opts.set_wal_sync(true)` (default for prod)
  - `Balanced` → `set_wal_sync(true)` only (default for balanced workloads)
  - `Fast` → neither (for tests and non-critical workloads only)
- Default is `Strict`
- `RocksConfig::for_testing()` uses `Fast` (keep tests fast)
- Add doc comment warning about durability vs performance tradeoff
- Add a test: write → kill process mid-write → restart → verify committed entry present (can simulate via drop)

**Must NOT do**: Don't change default behavior for existing `for_testing()` callers.

**Agent**: `quick` with `systematic-debugging` skill
**Files**: `crates/rr-storage/src/rocks.rs`
**Acceptance**:
- [ ] `DurabilityPolicy::Strict` is default
- [ ] Test `test_rocksdb_fsync_on_commit` passes
- [ ] Docs: `docs/docs/guide/storage-engines.md` explains tradeoff
- [ ] No existing tests break

---

#### T3. Complete metrics-reference.md

**What to do**:
- Audit all `counter!/gauge!/histogram!` calls in `crates/`
- Update `docs/docs/reference/metrics-reference.md` with 8 undocumented metrics:
  - `node.group.started`, `node.started`, `node.shutdown`
  - `raft.group.start`, `raft.group.stop`
  - `router.route.not_found`, `router.get_leader_addr.success`
  - Plus `rr_snapshot_installs_total` label fix (currently hardcoded `"unknown"`)

**Agent**: `quick`
**Files**: `docs/docs/reference/metrics-reference.md`
**Acceptance**:
- [ ] All 14 emitted metrics documented with name, type, labels, location

---

#### T4. Un-ignore Chaos Tests

**What to do**:
- Run `cargo test --workspace -- --ignored` locally to verify all 4 chaos tests pass
- If flaky, investigate root cause (retry once is fine, but silent hangs are not)
- Remove `#[ignore]` attribute from the 4 chaos tests in `tests/integration/src/lib.rs`
- Move them to the default test suite (CI runs them in main `test` job, not only `chaos` job)
- If a test is genuinely slow, keep in `chaos` job with 15min timeout

**Agent**: `deep` with `systematic-debugging` skill
**Files**: `tests/integration/src/lib.rs`, possibly `.github/workflows/ci.yml`
**Acceptance**:
- [ ] All 4 chaos tests pass 10 consecutive runs
- [ ] `#[ignore]` removed (or clearly documented why kept)

---

#### T5. CHANGELOG.md

**What to do**:
- Create `/CHANGELOG.md` following [Keep a Changelog](https://keepachangelog.com/)
- Initial entries:
  - `## [Unreleased]` — for in-progress work
  - `## [0.1.0] — YYYY-MM-DD` — Phase 2 baseline (retroactive)

**Agent**: `quick`
**Files**: `/CHANGELOG.md`
**Acceptance**:
- [ ] Follows Keep a Changelog format
- [ ] [0.1.0] section lists Phase 2 deliverables

---

#### T6. CONTRIBUTING.md

**What to do**:
- Create `/CONTRIBUTING.md` covering:
  - Dev environment setup (Rust toolchain 1.82+, RocksDB system libs)
  - Running tests (`cargo test --workspace`, `--features rocks`)
  - Running benchmarks
  - Building docs (Docusaurus in `docs/`)
  - Code style (clippy clean, fmt clean)
  - PR process

**Agent**: `writing`
**Files**: `/CONTRIBUTING.md`
**Acceptance**:
- [ ] New contributor can go zero-to-tests-passing in <20 minutes

---

#### T7. CODE_OF_CONDUCT.md

**What to do**:
- Use Contributor Covenant v2.1 verbatim
- Add a contact email (ask user or use placeholder)

**Agent**: `quick`
**Files**: `/CODE_OF_CONDUCT.md`

---

#### T8. PR Template

**What to do**:
- Create `.github/pull_request_template.md` with sections:
  - Summary
  - Related issues
  - Testing performed
  - Checklist (tests pass, docs updated, CHANGELOG updated)

**Agent**: `quick`
**Files**: `.github/pull_request_template.md`

---

### Wave B — Weeks 2–3: Deployability

#### T9. rr-server Binary

**What to do**:
- Replace `crates/rr-server/src/main.rs` stub with:
  - `clap` derive CLI parsing
  - Subcommands: `serve` (primary), `version`
  - `serve` accepts `--config /path/to/config.toml` or individual flags (`--node-id`, `--listen-addr`, etc.)
  - Config loading order: CLI flag > env var > config file > default
  - Signal handlers: SIGTERM/SIGINT trigger graceful shutdown
  - Tracing subscriber initialization (format: `RR_LOG=info,rr_raft=debug`)
  - Metrics HTTP endpoint started (if configured)
  - Health endpoint (T14 blocker — may be stubbed initially)
- Document each CLI option in `docs/docs/reference/config-reference.md`

**Must NOT do**:
- Don't reimplement anything — wire existing `RrNode`, `MemConfigProvider`, `RocksStorage`
- Don't force a specific ConfigProvider — let user pick via config

**Agent**: `deep`
**Files**: `crates/rr-server/src/main.rs`, `crates/rr-server/src/cli.rs` (new), `crates/rr-server/Cargo.toml` (add clap, tokio signal)
**Acceptance**:
- [ ] `rr-server --version` works
- [ ] `rr-server serve --config config.toml` starts a working node
- [ ] `kill -TERM <pid>` triggers graceful shutdown within 10s
- [ ] Docs updated

---

#### T10. TOML Config File Support

**What to do**:
- Add `rr-server::config::ConfigFile` struct matching `NodeConfig` fields
- Use `serde` + `toml` crate
- Validation: fail fast on unknown keys (`#[serde(deny_unknown_fields)]`)
- Example config in `examples/config/single-node.toml`, `examples/config/3-node-cluster.toml`
- Env var overrides: `RR_NODE_ID`, `RR_LISTEN_ADDR`, etc.

**Agent**: `deep`
**Files**: `crates/rr-server/src/config_file.rs` (new), `examples/config/*.toml` (new), `docs/docs/reference/config-reference.md`
**Acceptance**:
- [ ] TOML config loads into `NodeConfig`
- [ ] Unknown keys produce clear error messages
- [ ] Env vars override file values

---

#### T11. RaftNode::new() Returns Result

**What to do**:
- Change `crates/rr-raft/src/raft_node.rs` line 60, 120, 124 from `.expect(...)` to `?`
- Return type: `Result<Self, RrError>` (already mostly there; audit all call paths)
- Update `RaftGroupManager::start_group()` to propagate the error
- Update tests

**Agent**: `quick` with `gitnexus-refactoring` skill (to check call sites)
**Files**: `crates/rr-raft/src/raft_node.rs`, `crates/rr-raft/src/group_manager.rs`
**Acceptance**:
- [ ] `RaftNode::new()` returns `Result`
- [ ] No `panic!`, `.unwrap()`, `.expect()` in `RaftNode::new()` non-test code
- [ ] `cargo test --workspace` passes
- [ ] `gitnexus_impact` confirms all callers handle the new Result

---

#### T12. Dockerfile + docker-compose

**What to do**:
- Multi-stage Dockerfile:
  - Stage 1: `rust:1.82-slim` for build
  - Stage 2: `debian:bookworm-slim` for runtime
  - Build with `--features rr-storage/rocks`
  - Non-root user (`rr:rr`)
  - `ENTRYPOINT ["/usr/local/bin/rr-server"]`
- `docker-compose.yml` for 3-node local cluster:
  - 3 services: `rr-1`, `rr-2`, `rr-3`
  - Volumes for data dirs
  - Network for peer comms
  - Healthchecks

**Agent**: `deep`
**Files**: `/Dockerfile`, `/docker-compose.yml`, `/.dockerignore`
**Acceptance**:
- [ ] `docker compose up` launches 3-node cluster
- [ ] Image <100MB
- [ ] Runs as non-root

---

#### T13. K8s StatefulSet + Helm Chart

**What to do**:
- K8s manifests: `deploy/k8s/statefulset.yaml`, `service.yaml`, `configmap.yaml`, `pdb.yaml`
- Helm chart: `deploy/helm/rr/` with `Chart.yaml`, `values.yaml`, templates
- Docs: `docs/docs/deployment/kubernetes.md`

**Agent**: `deep`
**Files**: `deploy/k8s/*.yaml`, `deploy/helm/rr/**`
**Acceptance**:
- [ ] `helm install rr ./deploy/helm/rr` deploys working 3-node cluster on kind/minikube
- [ ] Readiness/liveness probes use gRPC health

---

#### T14. HTTP /health Endpoint

**What to do**:
- Add `hyper`-based minimal HTTP server on `metrics_port` (or separate `health_port`)
- Routes: `/health` (liveness), `/ready` (readiness — requires at least one group with leader)
- Responds `200 OK` or `503 Service Unavailable` with JSON status body

**Agent**: `quick`
**Files**: `crates/rr-server/src/health.rs` (new), `crates/rr-server/src/node.rs`
**Acceptance**:
- [ ] `curl :9100/health` returns 200 when node is running
- [ ] `curl :9100/ready` returns 503 until first leader elected

---

### Wave C — Weeks 4–6: Security

**⚠️ Blocked by user decisions in the Key Decisions section above.**

#### T15. TLS via rustls
(details deferred until T15 decision made)

#### T16. Bearer Token Auth on AdminService
(deferred)

#### T17. Optional Client Auth
(deferred)

#### T18. Rate Limiting
- Implement via tonic `Layer` — `tower::limit::RateLimitLayer` or custom
- Configurable per-service: `admin_rps`, `client_rps`, `raft_rps`
- `raft_rps` default: unlimited (internal cluster traffic)

#### T19. Audit Log
- Separate `tracing` target: `rr::audit`
- Log every AdminService call: caller identity (once auth lands), timestamp, operation, result
- Supports JSON output via `tracing_subscriber::fmt::json()`

---

### Wave D — Weeks 7–8: Operations

**⚠️ T20/T21 blocked by backup format decision.**

#### T20. Backup Command
#### T21. Restore Command

#### T22–T26. Runbooks (one file each)
- `docs/docs/operations/runbooks/disk-full.md`
- `docs/docs/operations/runbooks/leader-flapping.md`
- `docs/docs/operations/runbooks/network-partition.md`
- `docs/docs/operations/runbooks/snapshot-corruption.md`
- `docs/docs/operations/runbooks/configprovider-unreachable.md`

Format: Symptom → Detection → Immediate mitigation → Root cause investigation → Recovery → Prevention.

---

### Wave E — Weeks 9–10: Observability + Release

#### T27. OpenTelemetry Tracing
- Add `tracing_opentelemetry` + `opentelemetry_otlp` dependencies
- tonic server interceptor: extract `traceparent` from metadata
- tonic client interceptor: inject `traceparent`
- Span the hot path: `ClientService::request` → `ClientDispatch::dispatch` → `raft.client_write` → `StateMachine::apply`

#### T28. Client-side Metrics
- In `rr-client`: histogram for request duration, counters for retries, gauge for leader cache size
- Expose `RrClient::metrics()` returning snapshot (or just use the global `metrics` registry)

#### T29. #[instrument] on Hot Paths
- Add to public async fns in `RaftGroupManager`, `GrpcRaftNetwork`, `ClientDispatchImpl`, `ShardRouter`
- Level: `debug` for hot, `info` for lifecycle

#### T30. Semver Policy Document
- `docs/docs/reference/compatibility.md`: API stability commitment, breaking change process, deprecation timeline

#### T31. crates.io Publish Workflow
- `.github/workflows/release.yml`: on tag push, publish crates in dependency order
- Requires `CARGO_REGISTRY_TOKEN` secret

#### T32. v0.2.0 Release
- Bump workspace version to 0.2.0
- Tag + push
- Verify published + docs.rs built

---

### Wave FINAL — Independent Review

#### F1. Plan Compliance Audit (oracle)
#### F2. Security Review (oracle)
#### F3. Manual Smoke Test via Docker
#### F4. Upgrade Path from Phase 2 → v0.2.0

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| RocksDB fsync on every commit slows perf significantly | HIGH | MEDIUM | `Balanced` policy as middle ground; benchmarks in docs |
| Chaos tests actually flaky, not just "marked ignore" | MEDIUM | MEDIUM | Fix root cause; if truly unfixable, document and keep in `chaos` job |
| TLS implementation breaks existing tests that use plaintext | HIGH | LOW | TLS opt-in via config; default off in dev |
| Auth implementation breaks existing integration tests | HIGH | LOW | Auth opt-in; default tokens empty |
| Backup format incompatible across rr versions | MEDIUM | HIGH | Document version pinning; bake version into backup manifest |
| Publishing to crates.io reveals unpublished deps issues | LOW | HIGH | Dry-run publish; fix any `publish = false` or path deps |

---

## Success Metrics

- **Deployment time**: Zero → 3-node TLS+auth cluster in <1 hour
- **Test coverage**: ≥80% on critical modules (`lookup.rs`, `node.rs`, `rr-client/lib.rs`)
- **Chaos test reliability**: 100/100 consecutive passes
- **Durability**: Verified fsync behavior on simulated power loss
- **Security**: All RPCs require auth in production config
- **Backup/restore**: 1GB cluster backup → restore round-trip <10 minutes
- **Docs completeness**: 100% of metrics documented; ≥5 runbooks published
- **Published**: v0.2.0 on crates.io, docs.rs rendering correctly
