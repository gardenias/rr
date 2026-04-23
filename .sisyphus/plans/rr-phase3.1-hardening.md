# Phase 3.1: v0.2.0 Final + Post-Release Hardening

## TL;DR

> **Goal**: Close the remaining Phase 3 loose ends, cut `v0.2.0` final,
> then deliver a hardening pass (Phase 3.1 proper) covering deferred
> items from Phase 3 + new gaps surfaced by the coverage report.
>
> **Timeline**: Tier 0 (~3-4 days to v0.2.0 final) → Tier 1 (4-6 weeks).
>
> **Status**: Drafted 2026-04-23 after `/louis` handoff session.
>
> **Prerequisites**:
> - `v0.2.0-rc1` tagged, pushed (done).
> - `.sisyphus/CONTEXT.md` + notepads updated (done).
> - Coverage baseline: 76.6% lines / 79.3% regions / 77.9% functions.

---

## Tier 0 — Must Do Before `v0.2.0` Final

Non-negotiable gates on cutting a non-prerelease tag.

### T0.1 — Verify `v0.2.0-rc1` release workflow succeeded
- **What**: Open <https://github.com/gardenias/rr/actions>, confirm the
  `release.yml` run for tag `v0.2.0-rc1` finished green.
- **Common fix**: Repo Settings → Actions → General → Workflow permissions
  must be "Read and write permissions" for `GITHUB_TOKEN` to push to GHCR.
- **Effort**: ~10 min.
- **Blocker**: nothing.

### T0.2 — Three-in-one cleanup commit
Combine into a single commit `chore: Phase 3 loose ends cleanup`:
- Delete `rr-network/src/{client,server}.rs` (0% coverage, no callers).
  Grep confirmed unused — `RrGrpcClient::connect` and `RrGrpcServer` stubs
  from before real gRPC wiring landed.
- Update `.github/workflows/ci.yml` coverage job to pass
  `--features rr-storage/rocks` — current Codecov number misses RocksDB.
- Apply `cargo clippy --fix` for the 32 pre-existing `io_other_error`
  warnings (mechanical `std::io::Error::new(ErrorKind::Other, ...)` →
  `std::io::Error::other(...)`). Review the diff, commit.
- **Effort**: ~1-2h.
- **Blocker**: T0.1 (don't touch the release branch while CI is still
  flapping from rc1).

### T0.3 — `rr-server/src/health.rs` integration test
- **What**: In `tests/integration/src/lib.rs`, add a test that spawns an
  `RrNode` with `metrics_port` set, `curl`s `/health` + `/ready` + `/metrics`,
  verifies 200/503 + body contents.
- **Why**: Lifts coverage of `rr-server/src/health.rs` from 23.6% → ~90%.
  It's the biggest remaining gap for a module that's genuinely exercised
  in production.
- **Effort**: ~1h.
- **Blocker**: none.

### T0.4 — F1-F4 independent reviews (Phase 3 plan's FINAL wave)
Four parallel review tasks using specialized agents:
- **F1**: Plan compliance audit — `task(subagent_type="oracle", ...)` with
  `.sisyphus/plans/rr-phase3-prodready.md` as input. Verify every
  completed task actually landed with correct evidence.
- **F2**: Security review — oracle agent with the code + `audit-log.md` +
  `tls.md` + `auth.rs`. Look for timing attacks, secret leaks in logs,
  permissive defaults.
- **F3**: Manual Docker smoke test — human (or `unspecified-high`):
  `docker compose up`, verify 3-node cluster bootstraps, writes work,
  kill leader, verify failover, backup one node, restore to a 4th.
- **F4**: Phase 2 → v0.2.0 upgrade path — `deep` agent: given a running
  Phase 2 (v0.1.0) cluster with real data, produce step-by-step upgrade
  procedure. Test it end-to-end.
- **Effort**: ~1-2 days (parallel execution).
- **Blocker**: T0.2 + T0.3 complete (don't review a moving target).

### T0.5 — Tag `v0.2.0-rc2`
- **What**: `git tag -a v0.2.0-rc2 -m "..."` + push. Triggers the
  release workflow to publish Docker image + Helm chart with the
  8 commits that landed after rc1.
- **Effort**: ~10 min.
- **Blocker**: T0.1 green.

### T0.6 — Tag `v0.2.0` final + first crates.io publish
- **What**: After F1-F4 pass, `git tag -a v0.2.0` + push. Update
  CHANGELOG's `[Unreleased]` → `[0.2.0] — YYYY-MM-DD`.
  Trigger `workflow_dispatch` on `release.yml` with crates.io publish
  enabled (requires `CARGO_REGISTRY_TOKEN` secret configured).
- **Effort**: ~30 min.
- **Blocker**: F1-F4 all signed off.

### T0.7 — Release notes + announcement
- **What**: GitHub Release text generated from CHANGELOG `[0.2.0]`
  section. Announcement post (blog / Twitter / HN — optional).
- **Effort**: ~1h.

**Tier 0 total**: ~3-4 days of calendar time (sequential + review wait).

---

## Tier 1 — Phase 3.1 Hardening (Post v0.2.0)

Structured into three themes. Do in parallel where possible.

### Theme A — Observability completeness

#### A.1 — `rr_raft_replication_lag` gauge
- Per-follower `commit_index` lag behind leader.
- Recorded in `rr-raft::network_factory::GrpcRaftNetwork::append_entries`
  after each successful RPC (difference between leader's and follower's
  reported match_index).
- Labels: `group_id`, `follower_id`.
- **Effort**: 0.5 day.
- **Value**: core signal missing from the 18-metric dashboard.

#### A.2 — Forward caller identity to audit log
- Currently audit events have only `peer` (IP:port). Need:
  - Auth interceptor extracts subject from bearer token / mTLS CN.
  - Pushes into `tonic::Request::extensions_mut()` via a typed key.
  - AdminService handlers read it for the `caller_subject` audit field.
- **Effort**: 2 days.
- **Value**: SOC2/PCI-DSS compliance, not just forensics.

#### A.3 — Grafana dashboard + Prometheus alerts
- JSON dashboard based on current 18 metrics.
- Alert rules: term churn > 3/min, proposal p99 > 200ms,
  replication_lag > 10000, leader flaps > 1/min, cache hit < 80%.
- **Effort**: 1 day each = 2 days total.
- **Value**: ops-ready out of the box, cuts onboarding time.

#### A.4 — OpenTelemetry full-stack example
- `examples/otel-client/` — `tracing-opentelemetry` + OTLP exporter.
- Demo: client span → traceparent passthrough → server spans visible in
  Jaeger/Tempo. Proves our lightweight passthrough composes with the
  full SDK.
- **Effort**: 1 day.

### Theme B — Ops automation

#### B.1 — Online backup via snapshot-fetch RPC
- New `AdminService.FetchSnapshot(group_id) → stream bytes` RPC.
- Internally calls `raft.trigger().snapshot()` then streams the current
  snapshot via the existing InstallSnapshot-like chunked protocol.
- `rr-admin backup --online` invokes this; node stays running.
- Backward-compatible: offline backup remains.
- **Effort**: 1-2 weeks. Needs proto change + feature flag + test.
- **Value**: removes the biggest operational limitation of v0.2.0.

#### B.2 — Auto-promote learner → voter
- Add `change_membership_mode` to `AdminService.AddLearner` RPC.
- Mode `auto_promote_after_caughtup` polls the learner's
  `last_log_index` vs leader's; when within threshold, calls
  `change_membership`.
- **Effort**: 1 week.
- **Value**: removes a manual step from common scale-out workflow.

#### B.3 — `DecommissionNode` workflow
- New `AdminService.DecommissionNode(node_id)` RPC.
- Steps: transfer leadership off node for every group → wait for
  acknowledgement → remove from membership → remove from ConfigProvider.
- Idempotent, resumable.
- **Effort**: 3 days.

#### B.4 — `/ready` detailed status body
- Currently returns `"ready\n"` or `"starting\n"`. Expand body to JSON:
  per-group leader status, uptime, current_term, last applied index.
- Backward-compatible: keep 200/503 semantics; body is extra.
- **Effort**: 0.5 day.

### Theme C — Testing hardening

#### C.1 — TLS + auth end-to-end integration test
- Extend `tests/integration/src/lib.rs` with a variant of the 3-node
  cluster test that uses `dev-certs/` + bearer tokens.
- Verifies: mTLS handshake, bearer-token rejection, rate-limit kicks in.
- **Effort**: 1 day.

#### C.2 — Fuzz proto deserialization
- `cargo-fuzz` targets for `AppendEntriesRequest`, `VoteRequest`,
  `InstallSnapshotRequest`, `ClientRequest`.
- Run as nightly CI job, bail on any crash/hang.
- **Effort**: 1 week.

#### C.3 — Real network partition chaos (iptables)
- Current `FaultAwareChannelPool` intercepts at the application layer.
- Add a variant that uses iptables rules (Linux-only) for true TCP-level
  partition. Docker containers make this feasible.
- **Effort**: 1 week.

#### C.4 — Crash-during-write durability test
- Kill `rr-server -9` mid-write; verify RocksDB + WAL recovery with
  `DurabilityPolicy::Strict` never loses a committed entry.
- Compares `Strict` vs `Balanced` vs `Fast` to empirically validate the
  correctness contract.
- **Effort**: 1 week.

#### C.5 — Jepsen-style linearizability test
- Big task. Use [Jepsen](https://github.com/jepsen-io/jepsen) or
  [Maelstrom](https://github.com/jepsen-io/maelstrom) as harness.
- Targets: concurrent reads/writes across leader changes and partitions.
- Verifies: linearizability of the KV example.
- **Effort**: 3-4 weeks. Gold standard for distributed DB correctness.

**Tier 1 total**: ~6-8 weeks, 2-3 people in parallel.

---

## Tier 2 — Post-v1.0 Wishlist

Out of Phase 3.1 scope. Listed so they don't get lost.

### Ecosystem
- Multi-language client SDKs (Go → Python → Java). Proto-first design
  makes Go ~1 week.
- `contrib/rr-config-etcd` — separate crate, avoids rr-config depending
  on etcd client in core.

### Security
- Secret rotation without restart — bearer token + TLS cert hot reload
  via `SIGHUP` or `rr-admin auth reload`.
- Per-IP rate limiting (requires peer identity from TLS or auth).
- Auth failure metrics (`rr_auth_failures_total` labeled by service).

### Supply chain
- Docker image signing via cosign/sigstore.
- SBOM generation (CycloneDX / SPDX) in release workflow.
- `cargo-deny` gate in CI (license + advisory checks).

### Storage / Data
- Alt backends: sled, LMDB.
- Remote snapshot store (S3, GCS).
- Snapshot compression tuning (zstd vs current gzip).
- Cross-partition transactions (needs 2PC).
- Partition split/merge (currently static partition → group mapping).

### Performance
- Multi-node benchmarks in CI (currently single-node only).
- Benchmark results publication (track over time, alert on regression).
- Profile-guided optimization build variant.

### Docs
- Architecture walkthrough video.
- "Building a custom Service trait" tutorial.
- Comparison against TiKV / etcd in deployment guide.

---

## Quick Wins (≤1h each, could be a single PR)

For someone with a spare afternoon:

1. **Delete vestigial stubs** (`rr-network/src/{client,server}.rs`) + their exports in `lib.rs`.
2. **CI coverage `--features rr-storage/rocks`** one-line fix.
3. **`cargo clippy --fix`** for the 32 `io_other_error` warnings.
4. **Tag `v0.2.0-rc2`** to push current state to ghcr.io.
5. **Add `rr_raft_replication_lag` gauge** (Tier 1 A.1 — 0.5d counts as quick).
6. **`/ready` JSON body** (Tier 1 B.4 — 0.5d).

Grouping 1-3 into one `chore: Phase 3 cleanup` commit closes this phase's
loose ends.

---

## Execution Strategy

### Wave T0 — to v0.2.0 final (days)
```
T0.1 (verify rc1 CI) → T0.2 (cleanup) → T0.3 (health test) → T0.5 (rc2)
                                                                ↓
                                                              T0.4 (F1-F4 reviews)
                                                                ↓
                                                              T0.6 (v0.2.0)
                                                                ↓
                                                              T0.7 (announce)
```

### Wave T1 — parallel themes after v0.2.0 (weeks)
```
Theme A: A.1 → A.2 → (A.3 + A.4 parallel)
Theme B: B.1 (biggest, start first) + (B.2/B.3/B.4 parallel)
Theme C: C.1 (quick warm-up) → (C.2/C.3/C.4 parallel) → C.5 (final)
```

### Wave T2 — ad-hoc, as scope emerges.

---

## Dependency Matrix (Tier 0)

| Task | Depends on |
|------|-----------|
| T0.2 | T0.1 |
| T0.3 | — (can parallel with T0.2) |
| T0.4 | T0.2 + T0.3 |
| T0.5 | T0.1 |
| T0.6 | T0.4 (all four) + T0.5 |
| T0.7 | T0.6 |

---

## Success Criteria for Phase 3.1 Complete

- [ ] `v0.2.0` final tagged and published to crates.io, ghcr.io, Helm OCI.
- [ ] All 4 F1-F4 reviews signed off without blocker findings.
- [ ] Coverage ≥ 85% lines on all library crates (rr-core/config/router
      already >95%; storage/raft/network/server lift 5-10 pts via C.1).
- [ ] No `#[ignore]` tests; no `io_other_error` clippy warnings.
- [ ] `rr_raft_replication_lag` + caller-identity in audit + Grafana
      dashboard shipped.
- [ ] Online backup works; tested end-to-end.
- [ ] `DecommissionNode` + auto-promote-learner workflows documented and tested.
- [ ] TLS + auth + chaos integration test passes in CI.
- [ ] One Jepsen-style test passes (at minimum the KV linearizability one).

---

## Related Files

- Phase 3 plan (already complete): `.sisyphus/plans/rr-phase3-prodready.md`
- Top-level context: `.sisyphus/CONTEXT.md`
- Phase 3 notepads: `.sisyphus/notepads/rr-phase3-prodready/{decisions,learnings,problems}.md`
- Latest tag: `v0.2.0-rc1` (pushed to `origin/main`)
- Coverage baseline: 76.6% lines at `target/coverage/html/html/index.html`
