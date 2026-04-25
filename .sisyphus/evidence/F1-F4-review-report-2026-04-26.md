# F1-F4 Independent Review Report — v0.2.0 Release Gate

**Date**: 2026-04-26
**Reviewer**: Sisyphus (Oracle-assisted)
**Scope**: Phase 3 plan compliance, security, Docker smoke test, upgrade path
**Verdict**: **CONDITIONAL PASS** — F1 ✅ / F2 ❌ (5 blockers) / F3 ⏸️ (blocked) / F4 ✅

---

## Executive Summary

| Review | Verdict | Key Finding |
|--------|---------|-------------|
| **F1** Plan Compliance | **PASS** (30/32 tasks) | T3 partial (snapshot metric label), T32 not started (version bump + tag) |
| **F2** Security | **FAIL** (5 blockers) | RaftService mTLS not enforced; bearer token on plaintext; audit gaps |
| **F3** Docker Smoke | **PARTIAL PASS** | Local 3-node test: 10/10 passed; Docker build blocked (GFW); membership CLI gap found |
| **F4** Upgrade Path | **PASS** | Wire+disk compatible; rolling upgrade safe; TLS enablement is only quorum-risk step |

**Bottom line**: v0.2.0 final is **blocked on F2 security fixes** (2 HIGH + 3 MEDIUM blockers). F1 and F4 are clear. F3 local test passed; Docker container test deferred (network).

---

## F1: Plan Compliance Audit

### Score: 30/32 complete, 1 partial, 1 not started

**Compliance Matrix Summary**:

| Wave | Tasks | Status |
|------|-------|--------|
| A (Quick Wins) | T1-T8 | ✅ All complete (T3 partial — see below) |
| B (Deployability) | T9-T14 | ✅ All complete |
| C (Security) | T15-T19 | ✅ All complete |
| D (Operations) | T20-T26 | ✅ All complete |
| E (Observability + Release) | T27-T32 | T27-T31 ✅, T32 ❌ |

**Issues**:

1. **T3 partial** — `rr_snapshot_installs_total` metric label still hardcoded `"group_id" => "unknown"` in `service_adapter.rs:48`. Docs are complete (17 metrics documented), but code-level label fix was missed. **One-line fix**.

2. **T32 not started** — All 9 crate `Cargo.toml` files still `version = "0.1.0"`. Only `v0.2.0-rc1` and `rc2` tags exist. CHANGELOG `[0.2.0]` section says "TBD on tag cut". **Mechanical work: version bump + date stamp + tag**.

3. **T27 scope delta** — Plan said `tracing_opentelemetry` + `opentelemetry_otlp` deps; team shipped W3C header propagation only (no OTLP SDK, ~12MB saved). This was a deliberate scope change documented in CONTEXT.md, **not a defect**. But CHANGELOG should note the BYO-SDK posture.

**Risks**:
- T31 crates.io publish is gated on `workflow_dispatch`, not auto-triggered by tag push. Needs documentation or config change.
- Workspace versions diverge from tags — verify Docker/Helm artifact labels are correct.
- CHANGELOG `[Unreleased]` doesn't reflect 8 post-rc2 commits.

---

## F2: Security Review

### Verdict: FAIL — 5 blockers before v0.2.0 final

**Finding Matrix**:

| # | Severity | Finding | Location |
|---|----------|---------|----------|
| F2.1 | **HIGH** | RaftService mTLS-only design not enforced — TLS without `require_client_cert` leaves Raft RPCs unauthenticated | `node.rs:319-358`, `tls.rs:54-69` |
| F2.2 | **HIGH** | Bearer token works on plaintext gRPC — token transmitted cleartext | `auth.rs:25-27`, `node.rs:319-327` |
| F2.3 | **MEDIUM** | Auth env var misconfiguration silently degrades to open (no fail-fast) | `config_file.rs:280-295` |
| F2.4 | **MEDIUM** | Audit log incomplete: `TransferLeader` no result event; auth/rate-limit rejections not logged | `admin.rs:114-194` |
| F2.5 | **MEDIUM** | gRPC errors expose internal state (peer addresses, storage keys, codec details) | `error_convert.rs`, `lookup.rs`, `dispatch.rs` |
| F2.6 | **MEDIUM** | `InstallSnapshot` no size limit — memory DoS from authenticated peer | `raft_service.rs:119-149` |
| F2.7 | **LOW** | `constant_time_eq` returns early on length mismatch | `auth.rs:138-146` |
| F2.8 | **LOW** | Auth error messages distinguish missing header / wrong scheme / invalid token | `auth.rs:101-115` |
| F2.9 | **LOW** | `traceparent`/`tracestate` validation too permissive, logged raw | `tracing.rs:44-63` |

**Blockers (must fix for v0.2.0)**:

1. **Enforce transport invariant**: `RaftService enabled` → require `tls_config.is_some() && require_client_cert == true`. Startup must fail otherwise.
2. **Require TLS for auth**: If `admin_token` or `client_token` is set, require TLS enabled. Otherwise fail startup.
3. **Fail-closed auth config**: If `admin_token_env` / `client_token_env` is configured but env var missing/empty, fail startup (not silent open).
4. **Complete audit trail**: All Admin RPCs need before+after events. Auth/rate-limit rejections should enter `rr::audit`.
5. **Sanitize external errors**: Client-facing gRPC messages must use generic text; details go to server logs only.

**Positive observations**:
- No bearer tokens found in any log output
- Admin/Client tokens are independent; RaftService has no bearer-token path
- Rate limiting architecture correct (Raft exempt, `RESOURCE_EXHAUSTED` on exhaustion)
- TLS stack is modern (rustls 0.23 + ring, no SSL fallback, no skip-verify)
- No hardcoded certs/keys; dev cert script properly marks dev-only
- Example configs don't contain secrets

---

## F3: Docker Smoke Test

### Verdict: PARTIAL PASS — local smoke test executed, Docker build blocked by network

**Docker build blocked**: Docker Hub (`registry-1.docker.io`) unreachable (GFW). Aliyun mirror also failed. Cannot build container image.

**Local smoke test executed instead** (same binary, tests core functionality):

**Tested — PASS** (10/10):

| # | Test | Result | Evidence |
|---|------|--------|----------|
| 1 | Single-node bootstrap | ✅ | Node 1 starts with `--bootstrap`, becomes leader of group 1 |
| 2 | Multi-node startup | ✅ | 3 nodes running, all report group 1 via `node status` |
| 3 | `/health` endpoint | ✅ | HTTP 200 on all 3 nodes |
| 4 | `/ready` endpoint | ✅ | HTTP 200 on all 3 nodes |
| 5 | `/metrics` endpoint | ✅ | Prometheus format: `node_group_started`, `raft_group_start`, `node_started` |
| 6 | Admin: `node status` | ✅ | Returns node_id, listen_addr, group_ids, uptime |
| 7 | Admin: `group status` | ✅ | Shows is_leader, current_term, last_log_index, leader_node_id |
| 8 | Admin: `trigger snapshot` | ✅ | `success=true message=snapshot triggered` |
| 9 | Admin: `transfer-leader` (self) | ✅ | `success=true message=election triggered on this node` |
| 10 | Admin: `transfer-leader` (non-member) | ✅ | Graceful failure: `success=false message=transfer to remote node 2 not supported` |

**Tested — WARNING**:
- Node 2 initial startup failed with `Address already in use` on first attempt, succeeded on retry. Possible race condition when starting multiple nodes simultaneously.
- Auth warning correctly emitted: `AdminService has NO auth — anyone reaching this port can transfer leadership...`

**Not testable — BLOCKED**:

| # | Test | Blocker | Mitigation |
|---|------|---------|------------|
| 1 | Multi-node Raft consensus | No CLI for `add-learner` / `change-membership` | Known limitation (CONTEXT.md); nodes start group but can't join Raft membership |
| 2 | Leader failover | Requires multi-member Raft group (blocked by #1) | Covered by existing integration tests + chaos tests (T4) |
| 3 | Write operations | EchoStateMachine has no CLI write path | Covered by integration tests |
| 4 | Backup/restore | Debug binary lacks `rocks` feature | Requires `cargo build --features rocks` (~15 min) or Docker build |
| 5 | Docker container isolation | Docker Hub unreachable | Configure mirror or VPN |

**Infrastructure verified (code review)**:
- `Dockerfile` ✅ (multi-stage, non-root UID 10001, `rr-server` + `rr-admin` binaries)
- `docker-compose.yml` ✅ (3-node cluster, per-node TOML configs, named volumes)
- `deploy/compose/config/node{1,2,3}.toml` ✅ (bootstrap=true on node1, peers configured)

**Findings for backlog**:
1. **No CLI for membership management** — `rr-admin` has no `add-learner` / `change-membership` / `promote-voter` commands. This makes cluster setup impossible without programmatic API access. This is the single biggest gap for ops usability.
2. **Startup race condition** — starting multiple nodes simultaneously can cause ephemeral port bind failures. Docker compose `depends_on` mitigates this in containers but local multi-node needs staggered startup.
3. **Debug build without `rocks`** — default `cargo build` doesn't include RocksDB support. Operators must explicitly use `--features rocks` for production builds.

**Remaining Docker test plan** (for when Docker Hub is reachable):
1. Configure Docker mirror in `~/.docker/daemon.json`
2. `docker compose build && docker compose up -d`
3. Full 3-node test: bootstrap → join → write → kill leader → failover → backup → restore

---

## F4: Upgrade Path — Phase 2 (v0.1.0) → v0.2.0

### Verdict: PASS — rolling upgrade safe

**Key findings**:
- **Wire compatible**: gRPC proto unchanged (generic `bytes payload`)
- **On-disk compatible**: RocksDB column families (`raft_log`, `state_machine`, `metadata`) unchanged
- **Rollback safe**: v0.1.0 binary reads v0.2.0-written data correctly

**Breaking Changes (18 items analyzed)**:

| Risk Level | Count | Items |
|------------|-------|-------|
| None/Low | 14 | Proto, disk format, additive config fields, new CLI, client metrics, K8s probes, etc. |
| Medium | 2 | fsync performance impact (~10× write latency); `rr-server` invocation migration |
| High (if misconfigured) | 2 | TLS half-enabled breaks peer communication; auth half-enabled blocks admin RPCs |

**Upgrade procedure** (rolling, zero-downtime):
1. Pre-upgrade backup of all nodes
2. Confirm Rust 1.88+ (MSRV bump)
3. Prepare TOML configs — TLS/auth sections **absent** during rolling phase
4. Upgrade followers first, one at a time (drain → replace binary → start → wait catch-up)
5. Upgrade leader last
6. Enable TLS in a **separate maintenance window** (only quorum-risk step)
7. Enable auth after TLS is universal
8. Enable rate limits last

**Critical risk**: TLS enablement is the **only** step that can lose quorum (plaintext↔TLS nodes can't communicate). Recommended: short stop-the-world window (5-10 min) for TLS flip.

**Performance caveat**: `DurabilityPolicy::Strict` (new default) enables WAL fsync. Expect ~10× higher write p99 vs v0.1.0's broken-but-fast behavior. This is the **correct** fix. Benchmark before production rollout.

---

## Consolidated Blocker List for v0.2.0 Final

| # | Source | Item | Effort |
|---|--------|------|--------|
| 1 | F2.1 | Enforce RaftService requires mTLS | ~2h |
| 2 | F2.2 | Require TLS when auth tokens configured | ~1h |
| 3 | F2.3 | Fail-closed on missing auth env vars | ~1h |
| 4 | F2.4 | Complete audit trail for all Admin RPCs | ~4h |
| 5 | F2.5 | Sanitize external gRPC error messages | ~4h |
| 6 | F1/T3 | Fix `rr_snapshot_installs_total` group_id label | ~15min |
| 7 | F1/T32 | Bump workspace versions to 0.2.0 + tag | ~1h |
| 8 | F3 | Docker container test (needs Docker mirror/VPN) | ~2h |

**Total estimated effort**: ~2 days (parallel execution possible on items 1-5).

---

## Changelog

| Date | Change | Trigger |
|------|--------|---------|
| 2026-04-26 | Initial report | T0.4 F1-F4 review execution |
