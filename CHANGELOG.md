# Changelog

All notable changes to the Rr distributed multi-Raft consensus framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed — 2026-11-14 IoBatcher serial-write window elimination

- **The group-commit batcher no longer waits a timed window by default**
  (`crates/rr-raft/src/io_batcher.rs`): `IoBatcherConfig::default().window`
  is now `Duration::ZERO`. The 500µs default charged every single-submission
  batch the full window (and tokio's ~0.5-1ms timer granularity made even a
  10µs window expensive), which dominated serial-write latency on the
  post-A-0016 durable raft-log path. Measured on this branch: S1
  rocks-strict serial writes **443 → 4831 TPS** (p50 2.2ms → 198µs,
  matching the in-memory path); S2 rocks-strict improves at every level
  (k=100 15.7k → 29.4k, k=200 21.0k → 30.9k ≈ mem, k=500 24.9k → 27.8k).
  The non-blocking drain still coalesces queued submissions under load
  (Kafka `linger.ms=0` rationale). A nonzero `window` remains an explicit
  tuning knob, and the timed wait now also ends early when a second
  submission arrives instead of waiting out the deadline. Root-cause and
  A/B evidence: 2026-11-14 investigation; storage-engines.md documents the
  replicated_only failure-semantics boundary (majority power loss can drop
  recently ACKed writes; production should keep Strict — the concurrency
  cost is only ~5%).

### Added — 2026-11-04 local quality gates

- **`just` quality-gate recipes + launchd scheduling** — local replacement for
  the disabled nightly CI workflows (`.github/workflows/` have been
  `disabled_manually` since 2026-10-20; nothing else ran the invariants lint,
  crash/fuzz/soak nightlies, formal TLC, or the bench regression gate). New
  `gates` recipe group: `gate-invariants`, `gate-test`, `gate-crash`,
  `gate-fuzz` (`RR_FUZZ_SECONDS`, default 300s/target), `gate-formal` (pinned
  tla2tools v1.7.4 fetched once into `target/tools/`), `bench-gate` /
  `bench-save-baseline` (criterion vs the saved `main` baseline through
  `scripts/bench-compare.sh`), and `soak-smoke` (short seeded soak, requires
  the local `rr-lab:dev` image). `just nightly` runs the full set through
  `scripts/quality-gate.sh`, which never stops at the first failure and
  archives per-step logs + `SUMMARY.md` under `target/quality-runs/<ts>-<suite>/`.
  `scripts/install-local-gates.sh install|status|uninstall` registers a
  launchd agent for an unattended daily run (default 03:47 local).
- **Vendored openraft patch ledger** —
  `docs/docs/architecture/openraft-relationship.md` now carries a per-patch
  table (commit, root cause, upheld invariant, upstream status) and an
  upgrade reconciliation checklist. Patches #3/#4/#5 directly uphold INV-1;
  upgrading openraft without reconciling the ledger can silently reintroduce
  committed-entry erasure.

### Fixed — 2026-11-04 crash harness oracle accounting

- **Crash-harness count-bounds oracle now accounts for the bootstrap
  writability probe** (`tests/crash`): `wait_writable` applies exactly one
  keyed probe write per cluster start outside the driver's sent/acked
  counters, so the true bound is `applied ∈ [acked+1, sent+1]`. Scenarios
  whose lanes drained cleanly (sent == acked — deterministic for
  scenario-a's seed) failed with a false-positive "applied > sent ceiling";
  scenarios with in-flight slack absorbed the probe and passed (scenario-b
  had 51 batch failures of slack). Arithmetic across all three scenarios
  confirmed the +1 probe (343=171+171+1; 1333=661+671+1; 2921=1461+1459+1).
  Not an engine defect: the keyed ack-log replay verified zero loss in every
  run. `wait_writable` now returns the applied probe count, stored on
  `CrashCluster::warmup_applies` and added to both oracle bounds.

### Security — 2026-11-04 review remediation (B-1..B-3)

- **Inter-node RPC payload decode is now bounded**: all eight bincode decode
  sites on the raft RPC path go through `bounded_decode` — the three
  `RaftService` handlers (`crates/rr-server/src/lookup.rs`: AppendEntries /
  Vote / InstallSnapshot metadata), the gRPC network factory's three
  response decodes and the TCP transport's two (`crates/rr-raft`), plus the
  snapshot-embedded idempotency state and the membership sidecar via
  `decode_snapshot_body`. This closes the same allocation-abort class the
  fuzz-nightly found for `FramedEntry` (9-byte input claiming GiBs) on the
  peer-facing surface. New tests pin the reject path for the
  container-carrying request type and wire-compat roundtrips for the
  fixed-size response types.
- **`NodeConfig::validate` enforces `grpc_max_frame_size ≤ MAX_ENTRY_BODY`**
  (and rejects 0) for library embedders too — previously only the binary
  config path checked the ADR A-0018 coupling, leaving embedders fail-open
  to log-poisoning their own groups. The same `validate` also rejects
  `grpc_max_concurrent_streams = Some(0)` (`must be >= 1`) — a behavior
  change for library embedders who previously passed `Some(0)`.
- **Admin error sanitization consistency**: online-backup snapshot fetch /
  trigger and get-routing-table no longer echo raw internal errors to the
  RPC client; details go to tracing, matching `error_convert`'s documented
  contract (whose exception list now records the `ConfigError::InvalidConfig`
  authoring rule: framework-authored, no absolute paths or secrets).

### Added — 2026-11-04 formation & admin observability (review C-1/C-2)

- **Formation lifecycle is now observable** — `rr_formation_cas_total{result}`
  (applied|conflict), per-group `rr_formation_state{group_id}` gauge
  (0=Forming, 1=Active), and `rr_formation_duration_seconds` histogram, plus
  info!/warn! lifecycle events (genesis/adoption start & complete, per-group
  Forming→Active, CAS conflict retries). The previously log-free, metric-free
  `formation.rs` is the P1 flagship — a forming cluster is no longer a black
  box.
- **`GetFormationStatus` Admin RPC + `rr-admin formation status`** (one-shot
  CLI with human/JSON output and REPL): origin (Genesis / LegacyAdopted /
  Untracked), aggregate state, cluster UUID, topology + store CAS revisions,
  and a per-group table. `Untracked` is a first-class answer for pre-formation
  clusters, with a pointer to the playbook's new Troubleshooting › Formation
  section (adoption link, stuck-Forming checklist, "lifecycle is not Active").
- **Snapshot transfer metrics on both sides** —
  `rr_snapshot_transfer_bytes{direction,group_id}` and
  `rr_snapshot_transfer_duration_seconds{direction,group_id}`. Send counts
  bytes actually handed to the transport (partial sends on cancel/timeout
  included; duration is success-only); recv counts installed payload bytes and
  install wall time (startup rebuilds from the local SnapshotStore included,
  same convention as `rr_snapshot_installs_total`). One send seam covers both
  transports (the experimental TCP transport delegates to gRPC for snapshots).
- **Admin operation counters** — `rr_admin_membership_changes_total{op,result}`
  (change_membership | remove_group_member | add_learner),
  `rr_admin_transfer_leader_total{result}`, and
  `rr_admin_snapshot_trigger_total{result}`, recorded exactly once per RPC
  including validation/failed-precondition rejections.

### Added — 2026-11-04 hot-path microbenchmarks (review D-2)

- Five criterion targets close the blind spots the 15% bench gate could not
  see: `dispatch_pipeline` (submit_write keyed/unkeyed + dispatch-level
  linearizable read through a single-node in-memory raft round trip),
  `shard_router_route` (64/256-partition hit/miss via the copy-on-write
  snapshot path), `batch_frame` (encode/decode/validate at 1/8/64/256 items),
  `read_index` (`linearizable_read` vs a `local_read` baseline on a
  single-node quorum), and `snapshot_build` (in-memory build at 1/4/16 MiB,
  durable SnapshotStore persist, FramedSnapshot encode/decode throughput).
  `scripts/bench-compare.sh` picks them up automatically — no explicit list.

### Changed — 2026-11-04 performance re-baseline (review D-1)

- **benchmarks.md re-sampled to 2026-11-04** (21 cluster runs, 0 errors, raw
  JSONs under `docs/perf/baselines/`): default-config S2 k=200 3-node
  30.7k TPS; best opt-in batch64+fanout4 **174.2k TPS** (batch64+fan1
  168.3k — the cross-batch serialization cost recorded on 2026-09-23 no
  longer shows on this HEAD). The multi-group note now carries the
  groups{1,2,4} × fanout{1,4} retest: same-host multi-group aggregate does
  not scale post-fanout and the per-group Mutex is not the binding
  constraint — fixed-node host CPU/scheduling is the ceiling; multi-host
  scaling with disjoint replica sets is unaffected (ADR A-0019 ②).
- **Two material regressions surfaced by the re-baseline** (pre-date this
  branch; recorded as follow-up work in the remediation plan):
  rocks-strict serial writes 5.9k → 421 TPS (-93%, fsync-independent,
  CPU far from saturated — likely pathological in the post-A-0016 durable
  raft-log path; bisect window d8ba158 / 6eb02d6 / bcdbdbc) and a
  replication-path-specific mem serial/low-concurrency dip of ~17-22%
  (single-node p50 unchanged). `v0.2.0-baseline.md` carries a
  non-comparability note: pre/post-A-0016 rocks numbers must not be
  compared.

### Added — 2026-11-04 decision records & release hygiene (review D-3/F-1/F-2)
- **ADR A-0019 (G2 rulings)**: the `BatchConfig::ordered` knob is deferred to
  the v0.3.0 kickoff (opt-out of cross-batch ordering needs an interaction
  analysis with deferred services first; batch64+fanout4 already measured
  121.2k TPS without weakening semantics); the post-fanout multi-group retest
  is folded into the D-1 re-baseline; "single-group 100k" stays an
  aspirational goal with its honest current status recorded.
- **`.sisyphus/plans/rr-v030-scope.md`** — one-page v0.3.0 scope note
  (candidates: A-0017 Bytes migration, the ordered-knob decision, version
  bumps incl. sdks/java sync, roadmap-100k leftovers).
- **`docs/docs/operations/release-checklist.md`** — pre-release hygiene
  checklist (workspace + sdks/java version sync, CHANGELOG cut, breaking-change
  migration notes, docs sweep, tag + release-notes).

### Fixed — 2026-11-04 documentation & template debt (review C-3, E-1..E-4)

- **`deploy/compose/docker-compose.yml` now exists** — the compose template
  intro.md and this changelog referenced was never committed. Three rr-server
  nodes + etcd with managed genesis formation, healthchecks,
  `depends_on: service_healthy` gating, loopback-only host publishes
  (admin 5200N / metrics 910N per the config contract), and a README.
  Smoke-verified: genesis CAS → Active, leader elected, `/ready` 200 ×3,
  `rr-admin formation status` Active, clean `down -v`.
- **Java SDK documented** — `docs/docs/guide/client-sdk.md` gains an install /
  quick-start / capability-matrix section with a code-verified 14-row
  Rust-vs-Java comparison and the gap list (client-side batching and channel
  fanout are Rust-only; parity tracked, not scheduled).
- **rr-admin connection errors carry prescriptions** — connect timeout,
  transport connect failure, REPL member-discovery failure, and reconnect
  failure now append the next steps (port role admin vs client, process
  liveness, TLS + admin token, playbook pointer) while preserving the anyhow
  cause chain (`Caused by:`) for the underlying transport error.
- **Observability docs calibrated** — metrics-reference.md now documents the
  previously missing `rr_deferred_pending_via_non_deferred_total` and
  `rr_idempotency_namespace_rejects_total` (plus the
  `idempotency_namespace_limit` outcome value) and every metric family added
  in this release; deploy/observability/README.md thresholds now agree with
  `rr-alerts.yml` (8000 not 45k; 1 not 0.5 elections/min; two missing critical
  alerts documented). New `scripts/check-metrics-docs.sh` bidirectional
  code↔docs drift gate, wired as `just metrics-check` and into the nightly
  quality-gate suite.
- **Baseline writebacks** — intro.md registers the formation subsystem;
  `.sisyphus/plans/rr-quality-hardening.md` and `rr-prod-perf.md` headers
  reconciled with actual state; the crash harness's stale gap-2 comment now
  reflects post-ADR-A-0016 facts; `examples/kv-store/README.md` added.
- **`require_active_group` sanitized** — the admin membership guard no longer
  interpolates the raw lifecycle-store error into the client-facing gRPC
  Status; detail goes to tracing (error_convert contract).

### Added — 2026-11-01 homogeneous cluster formation

- Added versioned `SeedSpec` and linearizable lifecycle state with Mem and etcd stores, local cluster identity,
  and managed `genesis` / `adopt` modes for stock `rr-server` and the kv-store example. Fresh Genesis
  concurrently initializes an identical full advertised roster; late or pristine nodes under an Active record
  wait for replication instead of initializing. Mem/Rocks rolling recovery and store-outage readiness are
  covered by integration tests.
- Legacy adoption records initialized local groups as `LegacyAdopted + Active` without changing Raft
  membership. Mixed or pristine-only initial adoption fails before the lifecycle CAS. The existing
  learner/change-membership job remains the explicit convergence path and preserves ACK data and routes.
- Helm, raw Kubernetes, and Compose now render identical managed rosters/settings on every node and start
  Genesis pods concurrently. The Helm formation job is an ordinary resource rendered only for `adopt`, so
  `--wait` can complete convergence; revision-specific names avoid immutable-template upgrade conflicts, and
  fresh Genesis does not run it. Helm requires an explicit formation-capable
  image tag and raw manifests carry a mandatory override placeholder because Rr 0.2.0 rejects the new fields.
  P1 documents no dynamic expansion, replica replacement, or resharding, and SeedSpec is not current membership.

### Added — 2026-10-22 INV-4 outbox failover e2e

- **Cluster-level outbox failover e2e suite** — closes the last INV-4 coverage
  gap (leader-change / restart redelivery had unit tests and a TLA model but
  never a cluster-level e2e; reliability program R-4 / R-8, design
  `.sisyphus/plans/rr-inv4-outbox-failover-e2e.md`). Shared harness
  `tests/integration/src/outbox_support.rs` (cluster builder, emitting
  `OutboxEchoService` overriding `handle_with_outputs_ctx`, shared-records
  `CapturingKafka`, poll-only waits — no fixed sleeps) plus three arm files:
  - `test_inv4_outbox_delivers_outputs_kafka` (INV-4): N=5 writes to a 3-node
    real-network group deliver as field-exact sink 5-tuples; a snapshot decode
    pins the mechanism premise — the sent watermark is leader-local memory
    (leader snapshot shows watermark == N with empty pending, a follower ==
    0 with all N outputs still pending). Registered in the INV-4 test list
    (check-invariants.sh enforced).
  - `test_inv4_outbox_redelivers_after_leader_failover_kafka` (INV-4): after a
    graceful leader kill the new leader re-delivers the whole pending outbox —
    no loss, re-delivery provably observed (duplicate keys present), and
    payload-exact effective-once after idempotency-key dedup. Registered in
    the INV-4 test list (check-invariants.sh enforced).
  - `test_inv4_outbox_no_delivery_without_attached_runner` (INV-4): negative
    control verified — a node elected leader without an attached output
    runner delivers nothing in the settle window, so the redelivery
    assertions above cannot pass vacuously. Registered in the INV-4 test
    list (check-invariants.sh enforced).
  - `test_inv4_outbox_raft_write_duplicates_absorbed_by_target_group`
    (INV-4): RaftWrite re-deliveries are absorbed by the TARGET group's
    framework idempotency layer (Replay decisions — the user service is never
    re-invoked): the target apply count stays pinned at 3N across the failover
    while its raft log still grows by the N re-delivered entries. Registered
    in the INV-4 test list (check-invariants.sh enforced).
  - `test_inv4_outbox_watermark_survives_snapshot_restart` (INV-4, rocks-gated,
    skipped in default-feature runs): the sent watermark survives snapshot
    recovery — a rocks-backed single node rebuilt on the same data_dir
    re-delivers exactly the M post-snapshot pending outputs (sink total
    exactly N+2M, zero ghost deliveries) and never the N snapshotted seqs.
    Registered in the INV-4 test list (check-invariants.sh enforced).
  Two documented design deviations (see the design doc's 落地记录): the
  RaftWrite arm pins the "full pending" premise on the followers BEFORE the
  kill (the post-election drain can outrun the test thread's 50ms poll), with
  target-group `last_log_index` growth as the second re-delivery proof; the
  rocks arm asserts the exact total N+2M instead of an upper bound, via a
  bare-manager local snapshot helper (`fetch_framed_snapshot` is
  `RrNode`-bound).

### Fixed — 2026-10-21 review-deferred sweep (P2 tail)

Closes every deferred item from the full-review remediation batch (same
day, earlier section). All items below were P2-level unless noted.

**rr-raft**:

- **Write-admission counting is now under the `GroupState` lock**
  (`get_raft_for_write`): `stop_group`'s drain provably observes every
  admitted write — a write entering after the group is removed is rejected
  loudly (`GroupNotFound`) instead of racing the drain uncounted
  (drain-semantics proof gap, review rr-raft P2).
- **Learner promoter correctness**: the promotion target voter set is built
  from a live membership re-read just before `change_membership` — a voter
  concurrently removed by an admin is no longer re-added from a stale
  metrics snapshot (the attempt aborts with an audit warn and replans next
  tick). A learner with NO replication record is no longer treated as
  caught-up (`matched_index` absence is `None`, not `0`).
- **ChannelPool address revalidation**: a cached-channel hit whose
  registered address no longer matches evicts and redials (node-id reuse /
  address change previously kept dialing the old address until error-driven
  eviction). Applies to both the shared `rr-network` pool and the raft
  network factory's local pool.

**rr-server / rr-admin**:

- **Boolean CLI precedence**: `--no-bootstrap`, `--no-allow-insecure`,
  `--no-require-client-cert` (clap-conflicting with their positive
  counterparts). Merge order is now CLI positive > CLI negative > file >
  default false — previously `||`-merged, so a `true` in the TOML could
  never be overridden from the CLI, contradicting the documented
  CLI-over-file precedence.
- **Bootstrap idempotency is typed**: `is_initialized()` pre-check plus
  `InitializeError::NotAllowed` matching replace the string matching on
  openraft error text (`"already"/"Initialized"`), which a vendored
  openraft wording change would have turned into a hard failure.
- **REPL admin channel follows kv leader redirects**: after a `leader_info`
  redirect moves the kv channel, a derived admin endpoint follows (same
  host, port + 1000) so `status`/`members` keep describing the node the
  session talks to; an explicit `--admin-addr` stays pinned.

**rr-core / rr-router**:

- **`output.rs` / `deferred.rs` test coverage** (the only two source files
  without unit tests): 13 tests covering Output construction/targets/
  roundtrip and all `DeferredApply::into_response` variants. The defensive
  `Pending` → empty-bytes mapping (a programming-error signal) now emits a
  `tracing::warn!` and counts `rr_deferred_pending_via_non_deferred_total`.
- **Read-path cost model**: `ShardRouter::routing_table()` delegates to
  `routing_table_snapshot()` + clone instead of a deep etcd pull.
- **ingestor doc**: the `Offset = String` example is gone (the `Copy`
  bound forbids it); the bound's persistence-path motivation is documented.

**Config plane / ingest**:

- **etcd compaction resync no longer blocks the snapshot path**
  (`rr-config-etcd`): resync collects the diff under `rescan_lock` but
  dispatches events after releasing it (FIFO order preserved by a separate
  dispatch lock) — a stalled watch consumer can no longer block
  `routing_table_snapshot`'s rescan. Zero event loss by construction.
- **Kafka offset decode rejects trailing bytes** (fail-closed, same
  `KafkaOffsetError` path as garbage bytes).

**Test infrastructure**:

- **E5 chaos linearizability test strengthened**: writer/reader/chaos task
  panics now propagate (no more `let _ = h.await`), the ack floor is 80%
  of attempts (was `>= 1`), and the checked history mixes linearizable
  reads with writes (a write-only history is vacuously legal). Leader
  exclusion from the fault pool is kept deliberately — documented in the
  test and invariants registry.
- **Seeded packet-loss injection**: the fault injector draws from an
  `RR_FAULT_SEED`-seeded RNG (same knob as the fault schedule), printing a
  replay hint when entropy-seeded.
- **proptest regression persistence**: all 7 proptest suites get explicit
  `failure_persistence` configs and tracked `proptest-regressions/`
  directories — a failing seed is now replayable across runs.
- **fuzz**: `fuzz_batch_frame_decode` asserts `decode_batch` ⇔
  `validate_batch_frame` agreement on arbitrary input.
- **crash scenario C**: kill timing is seed-randomized (recorded in the
  fault log) and the snapshot check verifies freshness (mtime after kill),
  not mere existence.
- **Renames for honesty**: `test_crash_during_write_preserves_acked_entries`
  → `test_sequenced_writes_preserved_across_restart` (it sequences all
  writes — the true power-cut face is tests/crash);
  `test_snapshot_catchup` → `test_snapshot_trigger_purge_keeps_cluster_operational`
  (no lagging node, no install path). The invariants registry records both
  renames; rocks-gated integration tests now carry a prominent "how to run"
  note in the crate docs.

### Deprecated — 2026-10-21 (0.3.0 breaking window)

- **`rr_core::codec::Codec` / `BincodeCodec`**: unbounded bincode decode on
  untrusted bytes is an OOM risk; the framework's own decode paths all go
  through `bounded_decode` / the frame codecs. Deprecated with a pointer to
  those; zero in-workspace consumers.
- **`RouterError::GroupNotFound` removed** (never constructed; the matching
  arm in rr-network was dead too), and error fields now use the
  `NodeId`/`GroupId`/`PartitionId` newtype aliases instead of bare `u64`
  (`NotLeader.leader`, `RaftError::GroupNotFound.group_id`,
  `RouterError::PartitionNotFound.partition_id`).

### Fixed — 2026-10-21 full-review remediation batch

A whole-project review (14 areas) found two correctness P0s, several
broken operational mechanisms, and a systematic docs drift after v0.2.0.
Everything found is fixed in this batch; the docs site was synced
end-to-end (`docs/`, README, invariants registry).

**Correctness (the two P0s)**:

- **Idempotency namespace cap was an apply-time error — a client-triggerable
  group-fatal crash loop** (`rr-raft`): the 1025th new `request_type` made
  apply return `Err`, and under the project's own A-0018 contract an apply
  error is group-fatal — replayed (and re-fatal) on every restart with a
  durable log. Same shape as the log-poison fixed 2026-09-23. The rejection
  is now an apply OUTCOME: new `ApplyOutcome::IdempotencyNamespaceLimitExceeded`
  variant, surfaced over a wire-additive optional proto field
  `NamespaceLimitError`; rr-client maps it to the typed
  `RrError::IdempotencyNamespaceLimitExceeded` (never retried, counted),
  the Java SDK throws `NamespaceLimitException`. Regression tests pin the
  outcome-not-error contract in the apply arm and both clients.
- **Java SDK write path fake success** (data-loss class; the Rust fix of
  2026-09-04 had never been ported): `success=false` with no `leaderInfo`
  returned empty bytes as if the write succeeded, so Java applications
  silently lost writes in the leader-unknown window. The write path now
  treats every `success=false` as retriable (redirect-cache update when
  `leaderInfo` is present, bootstrap fallback otherwise), fails with
  `NotLeaderException` on exhaustion, and only completes on
  `success=true`. `RrClientWriteRedirectTest` covers both windows.

**Storage**:

- **MemStorage: land the three fixes the 0.2.0 changelog had already
  announced** (`rr-storage`): no more derived `Default` (capacity is
  explicit), `write_batch` is atomic (all-or-nothing), and a reversed
  `scan` range returns empty instead of panicking. Three regression tests.

**Clients (Rust + Java)**:

- **Deterministic gRPC status codes are no longer retried to exhaustion**
  (both SDKs): `Unauthenticated`/`InvalidArgument`/etc. fail fast instead
  of burning the full backoff budget, and retry sleeps carry jitter.
  Java additionally gains a request-timeout API
  (`RrClientBuilder.requestTimeout`, per-stub deadlines) and a
  closed-client guard; metric registration is cached (was per-request).

**rr-raft**:

- **Ingestor poll-error backoff is now cancellable** — the sleep (up to
  30s) races the cancel channel, so `stop_group` during a Kafka outage
  returns promptly instead of stalling for the full backoff (regression
  test asserts stop < 1s mid-backoff).
- Supervisor/runner `stop()` join failures (`IngestorHandle`,
  `OutputHandle`) now log a warning instead of vanishing silently.
- `RocksLogStorage` preserves the underlying `io::ErrorKind` instead of
  flattening every failure to `Other`.
- `RaftNode::init_single_node` returns `RrError`
  (`RaftError::InitializationFailed`) instead of `Box<dyn Error>`.
- `IoBatcher::flush_barrier` rustdoc now states the no-timeout trade-off
  explicitly (a wedged worker hangs the barrier callers — deliberate,
  INV-1 over silent reordering).

**rr-server / rr-admin**:

- **`grpc_max_frame_size` startup validation**: a value above the apply
  decoder's hard bound (16 MiB) is rejected at startup — previously keyed
  writes replicated fine but failed apply-decode on every node
  (group-fatal, same A-0018 shape).
- **Startup-failure path now shuts down gracefully**: when the TCP raft
  bind or `bootstrap_hosted_groups` fails after `node.start()`, the node
  is `shutdown().await`ed before returning the error (raft groups reach
  log close instead of being dropped mid-flight).
- **decommission leader drain works against any contact node**
  (`rr-admin`): the drain re-dials the TARGET node's own admin endpoint
  and issues `DrainNode` there (TransferLeader/DrainNode are self-only
  server-side) instead of issuing `transfer_leader` to the contacted
  node (which the server only supports for itself).
- **`rr-admin node drain` exits non-zero** when any group reports
  `leadership_transferred=false` (matching decommission and the
  "failed admin mutations exit non-zero" contract).
- **Backup hardening**: `create_backup` refuses an `--output` inside
  `--data-dir` (the archive would contain itself); `sha256_of_dir` fails
  loudly on traversal errors instead of `filter_map`-skipping them; the
  module doc no longer claims online backup is "planned" (it shipped).
- **`--peers` help text is now honest**: only the entry COUNT sizes the
  bootstrap routing table (IDs guessed 1-based in list order); the
  address strings are never dialed — use an etcd ConfigProvider for real
  topologies.

**Config plane / ingest**:

- **etcd compaction resync no longer has an event-loss window**
  (`rr-config-etcd`): the rescan captures its revision up front and the
  watch resumes from it, so events between scan and watch-establish can
  no longer vanish permanently.
- `EtcdConfigProvider` rejects `PartitionConfig::Range` loudly instead of
  silently persisting a routing variant nothing honors.
- **Kafka `topic_pattern` must contain `{group_id}`** — without it,
  multiple groups would consume the same topic and apply each other's
  commands. `enable.auto.commit` is pinned off (offsets move only via
  `commit_applied` after apply) and `auto_commit_interval` is deprecated
  to a no-op.

**Network**:

- **Per-caller rate limiting can no longer be bypassed by rotating forged
  Bearer tokens**, and buckets are refilled lazily on access — a forged
  fingerprint no longer spawns a tokio task per entry. A configured
  `rps = 0` is now a startup validation error (never-refilling bucket).
- `ChannelPool` endpoints get connect timeout + TCP/HTTP2 keepalive
  tuning (previously a dead peer could park a dial indefinitely).

**Deploy / observability / scripts**:

- **Helm default install no longer CrashLoops**: `metrics_bind_host` is
  only rendered when `metrics.bindHost` is set (v0.2.0's strict parser
  rejects the key), and the chart ships a post-install cluster-form Job
  (add-learner + change-membership via rr-admin), a ServiceAccount, and
  NOTES.txt — an `helm install` now yields a formed 3-voter cluster
  instead of 1 voter + 2 idling nodes. Raw `deploy/k8s/` manifests get
  the same Job, pinned image tags, security contexts, and probe windows.
- **Replication-lag alert fires again** (`rr-alerts.yml`): the expression
  joined on `instance`, which the recording rule had aggregated away
  (always empty). Also corrected: idempotency-cache threshold (code cap
  is 10000, was alerting at 45000) and leader-flapping units
  (per-second rate → per-minute elections). New fail-closed alert
  coverage; Grafana dashboard variables fixed.
- **`scripts/bench-compare.sh` regression gate actually fires**: the
  criterion output parser split on `", "` but criterion prints
  space-separated columns — the gate could never trip (verified: a
  +16.5% regression now exits 1). Both separators are accepted.
- **`scripts/check-invariants.sh` parser fixes** (second bullet test
  names no longer dropped, `INV-1` no longer substring-matches future
  `INV-10`) and rr-client tests carry their `// INV-2:` back-links — the
  lint is green on the worktree again.
- `scripts/gen-dev-certs.sh` generates a CA with the critical `CA:TRUE`
  basic-constraints extension; `scripts/prod-perf.sh` no longer swallows
  benchmark failures; compose node configs publish admin/metrics on
  0.0.0.0 (local dev only) and the lab Prometheus/Grafana ports bind to
  loopback.
- **`examples/config/*.toml` parse against the current schema** and
  `counter --demo` now actually defaults to demo mode with no arguments.

**Tests**:

- **Crash harness**: zero-panic gate in teardown (any `panic` in a node
  log fails the scenario), the previously dead batch-lane count-bounds
  verification is wired in, and the INV-5 snapshot-digest test now
  compares real state (EchoSM snapshots carry their apply count — the
  old test compared three empty byte strings, which was vacuously true).

**Docs**: full-site sync of the post-v0.2.0 drift — FAQ (zh/en) no longer
claims durable raft logs "aren't wired into stock rr-server", the RC
banner, consumer guide, compatibility external links, Kubernetes
versions, `metrics-reference.md` (verified against emitted metrics),
getting-started, durability/storage-engines, admin-api `TransferLeader`
section (proto returns `Empty`; self-only transfer documented), README
feature list (TLS/auth/rate-limit/backup/linearizable reads/durable log)
and REPL/admin-port notes.

### Added — 2026-10-21 full-review remediation batch

- **Automatic learner promotion is wired into stock `rr-server`**:
  `NodeConfig::auto_promote_learners` (default `true`,
  `with_auto_promote_learners(false)` to opt out) starts the
  quorum-guarded `start_learner_promoter` on every locally hosted group —
  previously an orphan API no caller used. A metrics-recorder conflict
  now logs a warning instead of leaving `/metrics` silently empty.
- **Java SDK**: `requestTimeout` builder option (see above),
  `NotLeaderException`, `NamespaceLimitException`.

### Fixed — 2026-10-20 soak INV-1 defense-in-depth

- **Read-path fake success** (soak-nightly run 38045591646 follow-up):
  a follower mid-election answered `ReadLinearizable` with
  `NotLeader{None, None}`, which the dispatch layer flattened into
  `payload=[] + leader_info=None` — indistinguishable from a successful
  empty read, so SDKs acknowledged bytes that never went through
  consensus. `LinearizableReadResult` is now an enum
  (`Served(payload)` / `NotLeader { leader_node_id, leader_addr }`) so a
  not-leader read has no payload to fake; the gRPC layer maps
  leader-unknown to a retriable `UNAVAILABLE` ("leader unknown; retry via
  bootstrap"), and `RrClient::read_linearizable` falls back to bootstrap
  polling with backoff (exhaustion reports an `RpcFailed` carrying the
  Unavailable cause). The Java SDK already retries `UNAVAILABLE` via
  bootstrap, so it inherits the fix server-side.
- **`scripts/soak.sh` gates**: (i) convergence magnitude check — after
  the three nodes agree on `last_log_index`, a keyed-only unbatched run
  (`CLIENT_BATCHING=0`, `SOAK_UNKEYED_RATIO=0`) must additionally satisfy
  `last_log_index >= acked` (final `metrics.jsonl` record), so a cluster
  that converged by *erasing* committed entries now fails instead of
  passing (incident shape: converged at 392176 with 1,434,247 acked);
  other traffic shapes degrade to warn. (ii) availability SLO gate —
  more than `SOAK_ZERO_TPS_MAX_WINDOWS` (default 10 ≈ 50 min at the
  300s metrics interval) consecutive zero-`window_tps` records ending
  well before load end fails the run; the 101-minute silent wedge in
  rounds 10–11 of run 38045591646 would have tripped this.

### Added — 2026-09-23 full-review batch

- **Ingress log-poison guard** (`rr_core::ingress::validate_client_payload`,
  P0): unkeyed client payloads carrying a house magic prefix
  (`\0rrENT01` / `\0rrBAT01`) are fully validated at the dispatch boundary
  *before* propose; malformed or future-version frames are rejected with
  `INVALID_ARGUMENT`. Previously such bytes committed to the log and then
  failed the apply on every node — apply errors are group-fatal, so one
  crafted (or buggy) client write wedged the whole Raft group, and a
  durable log replayed the poison on every restart. The apply-side batch
  split is additionally gated on raw client bytes only: framed entries
  (ingestor-sourced, `offset = Some`) are never re-interpreted as batch
  frames. New ADR: `A-0018` (apply-error contract, drafted).
- **`StorageEngine::last_key_value`** (semver-minor, defaulted): reverse
  tail fetch with native implementations in `MemStorage` and
  `RocksStorage`; `RocksLogStorage::get_log_state` no longer materializes
  the entire log CF at startup (was hundreds of MB transient on large
  logs).
- **`EtcdConfigProvider`**: `new_with_options(endpoints, cluster_id,
  ConnectOptions)` (TLS/auth on the etcd plane), cluster-id charset
  validation, watch reconnect loop with revision resume + compaction
  reconcile, `WatchStreamGuard` (aborts watchers on drop), and a full
  metrics surface (`rr_config_etcd_*`: probe duration, cache hits/misses,
  rescans, stale serves, watch reconnects/events/compactions, bad keys,
  fabricated groups).
- **Batch codec hardening** (`rr-core`): `validate_batch_frame` (no-alloc
  structural validation), overflow-checked cursor arithmetic (32-bit
  safe), infallible slicing (no reachable `expect` on untrusted bytes),
  and `encode_batch` now honestly panics on >u32::MAX counts/lengths
  (previously silently truncated into a corrupt frame).
- **rr-client batching knobs**: `BatchConfig::max_buffered_bytes` (default
  32 MiB; backpressure is now byte-aware as well as entry-aware),
  `NetworkError::InvalidConfig` variant (rr-core), per-partition flush
  serialization, and `linger = 0`/sub-ms now flushes immediately
  (Kafka-aligned; previously it disabled the timer and could park a lone
  write forever).
- **rr-admin**: `--admin-addr` flag (default: `--addr` host with
  port+1000, matching the kv-store convention), 5s connect timeout, and a
  real `flock`/`fcntl` probe for the RocksDB `LOCK` file in `node-reset`.
- **soak/perf tooling**: `rr-perf-bench --soak-unkeyed-ratio` adds an
  unkeyed write lane to the soak oracle (unique keys, verified by
  linearizable Get), so `--client-batching` is actually exercised under
  soak; the batching flag now hard-errors on soak configurations that
  can't exercise it. `scripts/soak.sh` gains `CLIENT_BATCHING` /
  `SOAK_UNKEYED_RATIO` / `SOAK_PORT_BASE` env knobs.
- **fuzz**: new `fuzz_batch_frame_decode` / `fuzz_batch_frame_roundtrip`
  targets + committed seed corpus (valid/empty/truncated/future-version).
- **Java SDK**: `readLinearizable` / `readLinearizableSync` with
  Rust-parity redirect semantics (follow `leader_info` immediately without
  consuming a bootstrap slot), plus regression tests; fixed a latent bug
  where typed failures (e.g. `IdempotencyExpiredException`) crossed the
  CompletableFuture relay wrapped in `CompletionException` and were
  wrongly retried.
- **Idempotency namespace cap**: at most `MAX_IDEMPO_NAMESPACES` (1024)
  distinct `request_type` namespaces per group; a new namespace beyond
  the cap fails loudly with `RrError::IdempotencyNamespaceLimitExceeded`
  (the watermark map is never evicted and enters every snapshot — unbounded
  growth was a client-controlled memory amplifier).
- **Deferred-response registry bounds**: parked deferred responders are
  capped at 10 000 per group with `rr_raft_pending_deferred` gauge +
  overflow/duplicate counters; overflow refuses the new write loudly
  (previously unbounded, silently overwriting duplicates).

### Fixed — 2026-09-23 full-review batch

- **P0 — TCP raft transport cross-group response misdelivery**: request
  ids were allocated per-network-instance starting at 1 while the
  connection pool (and its `pending` map, keyed by request id alone) is
  shared node-wide across groups — two groups replicating to the same
  target could collide, overwrite each other's waiters, and deliver a
  response to the wrong group's replication stream (false `matched`
  advancement → failover data loss). Request ids are now pool-wide,
  `pending` is keyed by `(group_id, request_id)` with the response frame's
  group verified, and an insert collision loudly fails the displaced
  waiter.
- **P0 — `OutputTarget::RaftWrite` cross-group idempotency collision**:
  the dedup key was `"rr.output"` + the per-source-group outbox sequence,
  so two source groups writing one target group collided — the second
  group's outputs were silently dropped (or permanently `Expired`-locked
  after a restart). `request_type` is now namespaced per source group
  (`rr.output.g<N>`); watermark monotonicity is preserved.
- **P0 — `rr-admin node-reset` live-node guard was dead code**:
  `rocks_lock_held` opened the LOCK file for writing and always returned
  false (opening does not probe `fcntl`/`flock`), so `--force` could wipe
  a *running* node's data dir. Both lock namespaces are now probed
  (fail-closed on unexpected probe errors), with a regression test that
  holds the flock and asserts refusal.
- **P0 — Kafka `poll()` dropped its partial batch on error**: messages
  already consumed (position advanced) were discarded when a later poll
  item errored, and the supervisor retried without re-seeking — commands
  were silently skipped for the rest of the leader tenure. The partial
  batch is now delivered first; the error resurfaces on the next poll.
- **P0 — etcd config-plane watch was one-shot**: any stream error/cancel
  permanently killed a domain watcher (undetectably — no reconnect, no
  resume, no metric, discarded `JoinHandle`). Watchers now reconnect with
  backoff, resume from the last seen revision, reconcile on compaction,
  and are aborted via the returned stream guard.
- **P1 — watch-loop quorum guard bypass after provider pre-stripping**:
  `MemConfigProvider::remove_node` now strips the node from group replica
  lists (no dangling refs), which made the watch handler's shrink check
  against the provider table a tautology — `NodeRemoved` could shrink a
  group below quorum despite the A-0012 guard. The guard now measures the
  current voter set from the **Raft membership** (source of truth).
  `test_node_removed_quorum_guard` (INV-7) covers the regression.
- **P1 — membership sidecar is now atomic + fsynced**: `sm-membership.bin`
  was written with a bare `std::fs::write` (no tmp+rename, no fsync) while
  being fail-open on read — a torn write recreated the zero-voter wedge
  the sidecar exists to prevent. It now shares the SnapshotStore-grade
  `atomic_write_enveloped` helper (tmp → fsync → rename → loud dir fsync).
- **P1 — `stop_group` skipped the contractual log close on shutdown
  error**: `raft.shutdown()` failing (exactly the RaftCore-fatal worst
  case) returned before `log_storage.close()`, skipping the final flush.
  Close is now attempted unconditionally and both errors surface.
- **P1 — snapshot persist no longer stalls the apply loop**: the
  monotonic guard compared against an in-memory `last_persisted_index`
  instead of reading + bincode-decoding the entire previous snapshot per
  build, and SnapshotStore IO moved off the sm worker onto
  `spawn_blocking`.
- **P1 — RocksDB storage fixes**: the `ReplicatedOnly` group-commit task
  called `flush_wal(false)` (a no-op) and swallowed all errors — it now
  fsyncs (`flush_wal(true)`) and counts failures
  (`rr_storage_wal_flush_errors_total`); the block cache is one shared
  `Arc<Cache>` as documented (was per-CF + an unused default ≈ 3× the
  documented envelope; unnamed CFs previously fell back to the 8 MiB
  default); RocksDB ENOSPC-class errors now map to
  `io::ErrorKind::StorageFull` (previously flattened to `Other`,
  unclassifiable upstream); `flush()` flushes every framework CF;
  `MemStorage` divergences fixed (`Default` = `new()`, two-pass atomic
  `write_batch`, `scan` with start > end returns empty instead of
  panicking).
- **P1 — dispatch error sanitization**: route failures leaked
  config-provider internals (etcd endpoints, dial errors) to clients with
  a misleading `NotFound` code — now `PartitionNotFound` maps to
  `not_found` (id only) and everything else to sanitized `Unavailable`;
  an ambiguous redirect (leader id known, address unknown) now maps to
  `Unavailable` so clients retry instead of seeing an indistinguishable
  empty failure.
- **P1 — readiness truthfulness**: the gRPC and admin servers now bind
  their listeners synchronously in `start()` — a bind failure (port in
  use) fails startup instead of leaving `/ready` = 200 and health =
  SERVING on a dead listener.
- **P1 — rate limiter rework**: refill is elapsed-time batched (a
  per-tick single permit busy-looped a core for rps ≳ 1000); per-caller
  mode now requires/builds a global fallback bucket so unauthenticated
  requests are no longer silently unlimited; refill/GC tasks are tracked
  and aborted on drop.
- **P1 — network-layer hardening**: TCP raft frames cap the
  wire-controlled `payload_len` at `RAFT_MAX_DECODING_MESSAGE_SIZE`
  before allocating (was a 4 GiB-per-connection DoS vector), per-
  connection tasks are tracked `JoinSet`s (raft TCP + health HTTP),
  `InstallSnapshot` streams into one buffer with an overall timeout and
  terminal-`done` enforcement (was ≈2× memory, no deadline), and
  AppendEntries/Vote now honor `RPCOption::soft_ttl()` (a half-open
  connection can no longer wedge replication invisibly).
- **P1 — runner lifecycle races**: `attach_ingestor` /
  `attach_output_runner` / `start_metrics_ticker` / `start_learner_promoter`
  re-check group existence after acquiring the group lock (no more
  supervisors spawned onto a removed group); the ingestor parked arm no
  longer hot-spins when its cancel sender is dropped; the 30s snapshot
  refresh/seed waits and the output-runner backoff sleep are cancellable
  (group stop no longer stalls for tens of seconds); the output runner no
  longer tight-spins when `pending_outputs` errors while leader (classifies
  GroupGone vs OutboxUnsupported and parks); the learner promoter promotes
  all stable learners of a tick in ONE `change_membership` (previously the
  second promotion demoted the first).
- **P1 — `RaftGroupManager::write()` preserves the structured redirect**
  (`RaftError::NotLeader { leader, leader_addr }`) instead of stringifying
  it, so single writes populate `leader_info` for the SDK redirect cache
  (only the batch path had this).
- **P1 — rr-admin correctness**: REPL `put`/`delete` verify
  `success`/`leader_info` instead of printing `OK` for unapplied follower
  writes; failed admin mutations exit non-zero (automation no longer sees
  success); `decommission` drives leader-drain off the *target's*
  leadership regardless of the contacted node; backup hashes stream
  (no whole-file memory load), fsync the archive, and restore is
  tar-slip-safe (paths with `..`/root rejected) and verifies before
  swapping into place.
- **P1 — examples**: kv-store argument parsing exits with usage on
  missing/malformed/unknown flags (was: panic or silent warn),
  `--port > 64535` rejected (admin-port overflow), and `Increment` uses
  `checked_add` (was debug-panic/release-wrap).
- **P2 — misc**: `is_defensive()` env check cached (was per-log-op);
  `RocksLogStorage` failure path closes the batcher explicitly (no
  LOCK-hold on immediate retry); duplicate `start_group` fails cleanly
  before any storage open; `local_read` on an unknown group returns
  `GroupNotFound`; per-request traceparent extraction gated behind
  `enabled!(DEBUG)`; Bearer scheme match is case-insensitive (RFC 7235);
  ClientService/AdminService decoding caps documented against
  `BatchConfig.max_bytes`; the deferred-apply `expect` on the apply path
  is now a typed internal error.

### Changed — 2026-09-23 full-review batch

- **`DurabilityPolicy` deprecated aliases removed** (`Balanced` / `Fast`)
  on schedule for the v0.3.0 breaking window — use `ReplicatedOnly` /
  `TestOnly`.
- **`MemConfigProvider` semantics**: `remove_node` strips dangling group
  replicas (warn), `add_partition` with conflicting replicas overwrites
  (warn), `remove_partition` garbage-collects the emptied group, and watch
  lag drops are loud (`rr_config_mem_watch_lagged_total`).
- **`EtcdConfigProvider` availability**: a failed revision probe now
  serves the cached table with a warn (`rr_config_etcd_stale_serves_total`)
  instead of failing every routing lookup while etcd is unreachable; the
  cache is keyed on a prefix-scoped fingerprint (max `mod_revision` + key
  count) instead of the cluster-wide store revision, so unrelated writes
  in a shared etcd no longer invalidate it; rescans are single-flight.
- **Kafka ingestor fail-closed on seek-above-high-watermark**
  (`KafkaIngestorError::SeekAboveWatermark`): a persisted offset past the
  topic end indicates a wrong/recreated topic — previously it logged and
  continued, and `auto.offset.reset=earliest` then replayed the whole
  topic. Offset arithmetic is overflow-checked (`OffsetOverflow`).
- **rr-client unreleased batching semantics tightened**: linger ZERO =
  immediate flush; per-partition in-flight-1 flush ordering; strict
  response-count equality (mismatch fails all waiters loudly); per-waiter
  error-kind preservation; `BatchConfig::validate` and sub-ms
  `with_request_timeout` reject with `NetworkError::InvalidConfig`;
  reactive APIs preflight the runtime with a typed error instead of
  panicking; `RrClient::channel_pool()` deprecated (dead field).
- **CI/gates**: heavy jobs carry `timeout-minutes`; per-job cargo cache
  keys (feature sets no longer collide); the `external-broker` job
  actually sets `RR_TEST_ETCD=1` (it was green-by-construction, running
  nothing); nightly workflows (crash/fuzz/soak/formal) open a deduplicated
  GitHub issue on failure instead of only uploading artifacts;
  `bench-compare.sh` fails loudly on missing input; `release.yml` crate
  publishing fails hard with retries (and documents that crates.io builds
  don't carry the vendored openraft patch).

### Performance — 2026-09-23 full-review batch

- Zero per-request `String` on the etcd-backed routing hot path's failure
  modes, single-flight rescans, and prefix-fingerprint probes (see Changed).
- rr-client: the final retry attempt moves the payload instead of cloning
  per attempt, group-id metric labels are interned per client, and
  bootstrap addrs are borrowed instead of cloned per attempt.
- Rocks log startup: `get_log_state` reverse-seek (see
  `StorageEngine::last_key_value` above); snapshot persist moved off the
  sm worker (see Fixed).



- **Durable-artifact formats enveloped, no legacy fallback** (pre-adoption
  breaking change, 2026-09-23): `sm-snapshot.bin` (T1 `PersistentSnapshot`)
  and `sm-membership.bin` (A-0016 sidecar) now carry a magic + version
  envelope (`\0rrPST01` / `\0rrMBR01`, version 1). Files written by 0.2.x
  are NOT read by this build: a stale `sm-snapshot.bin` aborts startup
  fail-closed (the error names the recovery runbook — delete the file, the
  node rebuilds from the leader); a stale `sm-membership.bin` is skipped
  with a warn and membership is rebuilt from the raft log/snapshot as
  before. Decoders reject future schema versions loudly.

### Added

- **Opt-in client-side write batching** (`rr-client`, off by default):
  `RrClientBuilder::with_batching(BatchConfig)` enables Kafka-producer-style
  batching with `max_entries` / `max_bytes` / `linger` / `max_buffered`
  (backpressure) knobs. Unkeyed `send_request` writes accumulate per
  partition and flush as ONE Raft entry (`rr_core::batch` frame, magic
  `\0rrBAT01`); the apply side splits the frame and calls the service per
  item, packing responses positionally. Idempotency-keyed writes bypass the
  batcher. Measured (S2 k=200 mem, 64/batch, linger 1ms): **175.2k TPS vs
  ~29k unbatched (6×)**, p50 686µs. Trade-off: serial (S1) writes pay the
  linger (p50 161µs → 3.1ms) — keep batching off for latency-bound callers.
  Requires servers whose apply path splits batch frames (this release);
  against older servers batches fail loudly, never fake-success. Not
  compatible with deferred two-phase apply (A-0033) services.
- **`EtcdConfigProvider` revision-probe snapshot cache**: `get_routing_table`
  / `routing_table_snapshot` no longer reconnect AND full-prefix-scan per
  call — the provider holds one shared `Client` and serves snapshots from a
  revision-keyed cache, probing the store revision with a tiny keys-only
  serializable GET per call (quorum-fresh probes available via
  `with_linearizable_probes(true)`). etcd remains the single source of truth
  (INV-6); a table write is visible from the next probe onward.
- **`ConfigProvider::routing_table_snapshot()`** (semver-minor): new defaulted
  trait method returning `Arc<RoutingTable>`. `MemConfigProvider` switches to
  copy-on-write `Arc` snapshots and overrides it, so `ShardRouter::route` no
  longer deep-clones the routing table per request. Routing semantics are
  unchanged — every `route()` call still sees the provider's current table
  (INV-6: no caller-side cache). Custom `ConfigProvider` implementations keep
  working via the default method (which wraps `get_routing_table()`).
- **Opt-in `mimalloc` allocator features** (`rr-server/mimalloc`,
  `rr-perf-bench/mimalloc`, both off by default): A/B switch for the
  allocator. Note: measured on macOS arm64 it is consistently ~20% *slower*
  than the system allocator (25.4k/23.6k vs 30.1k/30.2k TPS, S2 k=200 mem) —
  keep off on macOS; re-evaluate on Linux.
- **Multi-group stock topology (`rr-server --groups N` / `[cluster].groups`)**:
  the stock server now hosts N Raft groups with partition i → group i and all
  peers as replicas (default 1 — behavior unchanged). Bootstrap initializes
  every hosted group (`bootstrap_hosted_groups`). `rr-perf-bench --groups N`
  drives the same topology and spreads S1/S2/S3 ops across partitions
  (round-robin by op sequence; key-sticky in the open-loop mix/wladder
  engine), so aggregate multi-raft scaling is measurable end-to-end. Soak
  stays single-group (keyed oracle). New config validation: `groups = 0` is
  rejected.

- **Crash scenario C — snapshot/purge under SIGKILL**
  (`scenario_c_snapshot_purge_kill`, INV-1/INV-5): `rr-crash-node` accepts
  `--snapshot-logs-since-last`; the scenario runs it at 100 so snapshot and
  log-purge cycles straddle both the kill and the 15s dead window, then
  verifies catch-up (durable snapshot + un-purged log tail compose, or
  snapshot install past the purge horizon), the `sm-snapshot.bin` durable
  artifact on the restarted node (T1), and a full ack-log oracle replay.
  Registered in the INV-1 / INV-5 test lists (check-invariants.sh enforced).

  `IoBatcherConfig::max_inflight_bytes` (default 64 MiB). Submissions
  acquire byte permits (keys + values) before enqueueing and the worker
  releases them only after the covering flush completes, so the
  accepted-but-unflushed tail is bounded in bytes — the entry-count channel
  bound alone was payload-blind (512 × 16 KiB ≈ unbounded GiB-scale backlog
  under a starved fsync path, the trigger chain of the 2026-09-17 perf-lab
  W9 flake: backlog → OOM-restart wave → restart demotion). Exhaustion
  back-pressures submitters, i.e. the Raft proposal pipeline itself.
  Budget is clamped into `[1, u32::MAX]` so a zero/oversized config cannot
  panic or wedge. New test `byte_budget_backpressures_submitters`
  (registered under INV-1).

- **Deferred (two-phase) response (R-2 / A-0033)**: an apply may now park the
  client response and have a later entry's apply deliver it — the framework
  capability behind intent-sourcing / two-phase apply. Per the 2026-09-17
  narrowing, this is **opt-in per request** and serves "wait for the result"
  (`respMode=full`) semantics; the `ack` style (return at phase-1 commit,
  outcome via the event stream) does not use it.
  - New `rr_core::deferred`: `DeferredHandleResult` (service-level:
  `Immediate` / `Pending{correlation_id}` / `Complete{completions, result}`)
  and `DeferredApply` (state-machine-level), with `CorrelationId = u64`
  (the phase-1 log index, via R-1 `ApplyContext`, is the natural value).
  - `Service::handle_deferred(request, ctx)` — the two-phase hook (default
  wraps `handle_with_outputs_ctx` in `Immediate`, backward compatible);
  `StateMachine::apply_deferred(entry, ctx)` (default wraps `apply_with_ctx`).
  - `RrStateMachine` owns the client responders: `Pending` parks the entry's
  responder in an in-memory per-node registry keyed by `correlation_id`;
  `Complete` delivers the parked bytes and answers the completing entry's own
  client. Parked responders are never snapshotted; dropping them (stop /
  restart) errors the held `client_write` so the client retries.
  - `rr-client`: new `RrClientBuilder::with_request_timeout(d)` — bounds a
  single gRPC attempt, mapping a never-completed deferred wait to a retryable
  `NetworkError::Timeout` ("超时映射"). Default stays unbounded (unchanged).
  - Tests: `group_manager` end-to-end (`Pending` parks the `client_write`,
  `Complete` delivers it); integration `test_deferred_response_client_timeout_mapping`
  (parked response maps to `NetworkError::Timeout`; immediate path unaffected).

- **`ApplyContext` threading (R-1 / A-0033)**: the real Raft log id of each
  applied entry (index + term) now flows from OpenRaft down to the user's
  `Service` / `StateMachine`, instead of being discarded at the `sm_adapter`
  boundary. This lets business logic key intent records on the true log index
  (retiring `outbox_seq` proxies) and use it as a fencing basis.
  - New `rr_core::types::ApplyContext { log_index, log_term }`.
  - `Service::handle_with_ctx(request, ctx)` — the apply-context hook for
  plain services (default delegates to `handle`, backward compatible);
  `Service::handle_with_outputs_ctx(request, ctx)` — the ctx counterpart the
  adapter calls on the apply path (default delegates to `handle_with_ctx`).
  - `StateMachine::apply_with_ctx(entry, ctx)` (default delegates to `apply`,
  backward compatible). `RrStateMachine::apply` builds the ctx from
  `entry.log_id`; `ServiceStateMachineAdapter::apply_with_ctx` threads it to
  the service on both the plain and idempotency-keyed paths.
  - Tests: `sm_adapter` (real log id reaches the SM), `service_adapter`
  (ctx reaches `handle_with_ctx`; the non-ctx path fabricates none; keyed path
  threads ctx).

- **Durable state-machine snapshots (T1 / R-P0)** — closes the restart
  data-loss wedge on the durable-log (rocks) path. `RrStateMachine` snapshots
  were previously memory-only: after snapshot → log purge → restart, the
  covered state was gone and the node silently rebuilt an empty state machine
  (and could serve that empty snapshot to lagging peers). Now, on the rocks
  path (`RaftGroupManager::with_rocks_base_path` / `rr-server --storage
  rocks`), every built or installed snapshot is persisted atomically
  (write → fsync → rename) to `{base_path}/{group_id}/sm-snapshot.bin` via the
  new `snapshot_store::SnapshotStore`, and `get_current_snapshot()` serves the
  persisted snapshot during the startup-recovery window so OpenRaft's
  `StorageHelper::restore_from_snapshot` re-installs it before replaying the
  un-purged log tail.
  - Persist is **fail-closed**: a snapshot that cannot be written aborts the
  build/install, so OpenRaft never purges a log prefix that has no durable
  snapshot behind it. A present-but-corrupt snapshot file fails node startup
  loudly instead of restarting empty.
  - `get_current_snapshot()` only serves the persisted snapshot while the
  state machine is still behind it (`last_applied < snapshot.last_log_id`);
  once caught up it re-captures live state, so follower catch-up and the
  ingestor-offset seeding path (`fetch_snapshot_bytes`) are unchanged.
  - New public API: `rr_raft::snapshot_store::SnapshotStore` and
    `RrStateMachine::with_snapshot_store` (wired automatically by
    `RaftNode::new_with_rocks_factory`).
  - Tests: `snapshot_store` unit tests (round-trip, atomic overwrite,
    monotonic guard, corrupt fail-closed, torn-tmp ignored); `sm_adapter`
    restart-recovery unit tests; and the acceptance test
    `group_manager::tests::test_restart_recovers_state_from_persisted_snapshot`
    (write → snapshot → purge the covered log → restart → SM recovered from
    disk, verified by an independent restore observer).

- **Durable Raft log in production binaries (ADR A-0016)**: the Raft log is
  no longer memory-only outside tests. `RaftGroupManager` gained
  `with_rocks_base_path(path)` (feature `rocks`): `start_group` now selects
  `RocksLogStorage` at `{base_path}/{group_id}` when a base path is set and
  the in-memory `RrLogStorage` otherwise — log storage is fully orthogonal
  to the configured network factory (gRPC / TCP / Noop).
  - `rr-server --storage rocks` now also persists the Raft log at
    `{data_dir}/raft-log/{group_id}` (both gRPC and TCP raft transports).
    Previously only the state-machine-side storage was durable — a
    full-cluster power loss could lose un-snapshotted committed entries.
    The mem backend keeps the in-memory log, unchanged; existing data
    directories need no migration (`raft-log/` is created on first start
    and is included in `rr-admin backup` archives of `data_dir`).
  - `kv-store` example: new `--data-dir <PATH>` flag (requires the new
    `rocks` cargo feature of the example) enabling the same durable Raft
    log; non-rocks builds fail fast with a rebuild hint.
- Tests (ADR A-0016): two `rr-raft` unit tests (unified log selection
  persists across manager restart; `start_group_rocks` compat shim), one
  rocks-gated integration e2e
  (`test_rocks_raft_log_rolling_restart_preserves_acked_writes` — 3-node
  replicated + RocksDB-log cluster, 50 acked writes, rolling restart of
  every node with quorum preserved, recovered nodes serve linearizable
  reads, byte-level log-DB verification). INV-1 registry updated.
- **`kv-store` example: new TLS/mTLS/token surface** (perf-lab M1-a):
  `--tls-cert/--tls-key/--tls-ca` (all-or-nothing) plus optional
  `--tls-domain` switch the node into secure mode — TLS on every gRPC
  endpoint, mandatory client certificates (no opt-out flag: mTLS is the only
  RaftService peer-identity check, gate G2), and enforced bearer tokens read
  from the `RR_ADMIN_TOKEN` / `RR_CLIENT_TOKEN` environment variables (same
  secrets-via-env convention as rr-server's `auth.*_token_env`). In secure
  mode the full `RrNode` G1–G7 gate set applies with no `allow_insecure`
  bypass; with no `--tls-*` flags the historical plaintext dev mode is
  unchanged.
- **perf-lab stack (`deploy/perf/`)** (rr-prod-perf plan, milestone M1): a
  production-shape docker-compose benchmark cluster — three `kv-store` nodes
  (rocks durable Raft log via `--data-dir`, TLS/mTLS/token secure mode,
  per-node `cpuset` + 2Gi memory limits, named data volumes), Prometheus +
  Grafana wired to `deploy/observability` recording rules/alerts/dashboard,
  plus profile-gated `bench`/`probe` containers running inside the cluster
  network. Ships with `gen-certs.sh` (CA + node certs with `kv-N` SANs +
  client cert; `certs/` gitignored), `.env.example` token/cpuset knobs,
  `selfcheck.sh` (executable G1–G7 controls: `RR_ALLOW_INSECURE` absence,
  token injection, all-ready, exactly-one-leader over mTLS, plaintext and
  certificate-less clients rejected, valid mTLS client accepted), and a
  README. `deploy/lab/`, `scripts/`, and the existing bench tooling are
  untouched.
- **`rr-perf-bench`: open-loop engine + TLS client** (rr-prod-perf plan,
  milestone M2, §5.2 items 1–5). New scenarios `--scenario mix` (fixed-rate
  open-loop with `--mix write:90,local:5,lin:5` operation blends) and
  `--scenario wladder` (W1 step-up rate ladder, `--ladder-rates` rungs of
  `--duration-secs` each on one continuous schedule), speaking the kv-store
  KV wire protocol against external clusters:
  - Planned-time scheduling (constant grid or `--poisson` arrivals) with
    coordinated-omission-corrected accounting: per-op reports carry true
    latency (scheduled → completion), service time (actual send →
    completion) and schedule lag (scheduled → actual send) side by side, so
    a driver that cannot keep up with its own plan is visible. All
    randomness (arrivals, op selection, keys) comes from seeded SplitMix64
    streams (`--seed`), same generator family as the fault-injection crate;
    same seed + flags reproduce the same offered-load trace.
  - `--key-space`/`--key-dist uniform|zipfian` (`--zipf-theta`, default
    0.99) key distributions with keys carried in written values;
    `--client-id` tags JSON meta for multi-container fan-out aggregation;
    `--timeseries-out` emits 1s-bucket JSONL (t/op/count/err/p50/p99, plus
    rung for wladder); `--max-in-flight` caps concurrency with honest
    backpressure (slip recorded as lag, never silent shedding).
  - TLS/mTLS/auth client: `--tls-ca/--tls-cert/--tls-key` (all-or-nothing
    trio, kv-store convention), `--tls-domain` SNI override, `--token-env
    VAR` reads the bearer token from the environment (fail-fast, secrets off
    argv). Applies to every scenario incl. soak; plaintext remains the
    default. The perf-lab `bench` container command now carries these flags.
  - Writes use the plain (unkeyed) path: keyed writes under open-loop
    concurrency would violate the idempotency layer's monotonic-per-
    namespace watermark contract by construction (concurrent in-flight IDs
    apply out of order → `Expired`); the keyed path stays measured by the
    soak scenario and the Kafka ingestor path.
  - Existing scenarios are byte-compatible: S1/S2/S3/soak code paths,
    flags, text output and the `--json-out` schema are unchanged (new
    `mix`/`wladder`/`client_id` keys appear only when the new scenarios
    ran); `scripts/perf-baseline.sh`, `scripts/soak.sh`, `deploy/lab/`
    remain untouched.
- **Benchmark matrix runner + server-side capture** (rr-prod-perf plan,
  milestone M3, §5.2 items 6–7 + §5.3):
  - `rr-perf-bench --prom-url <url>` embeds a server-side Prometheus
    summary of the exact measurement window into the JSON report
    (server/proposal/apply/append-RTT p50/p99, replication-lag max,
    ingestor committed rate — queried from the raw histogram/counter
    families; the `deploy/observability` recording rules are untouched).
    Scenario reports carry `started_unix_secs` for window alignment.
    Capture failures degrade fields to null, never fail a run. The soak
    scenario summary gains the same window fields.
  - `scripts/prod-perf.sh` (new): one-shot W1–W9 + W11 matrix over
    `deploy/perf/` — build, secure stack up, plaintext negative control
    (hard gate), per-tier raw JSON + 1s time series, per-tier ok/fail
    ledger, overlay restarts for W6 (Kafka ingest), W7 (snapshot
    pressure), W8 (insecure control), W11 (mem-log control), and a
    rendered `report.md` from `scripts/prod-perf-report.py` (embedded
    template, no new deps). `--smoke` shrinks every tier for
    self-validation; artifacts archive to `docs/perf/prod/<date>-<sha>/`.
  - `deploy/perf/` overlays: `docker-compose.kafka.yml` (redpanda + kv
    `--kafka-brokers` + profile-gated fixed-rate producer),
    `docker-compose.insecure.yml` (W8 control), `docker-compose.memlog.yml`
    (W11 control). Base stack gains `SNAPSHOT_LOGS_SINCE_LAST` env knob
    (default 50000, unchanged behavior) for W7. The W10 (Toxiproxy WAN)
    profile is deferred — the plan marks it optional.
  - `kv-store` example: `--snapshot-logs-since-last` passthrough (W7).
  - `rr-admin`: `--tls-ca/--tls-cert/--tls-key` (all-or-nothing),
    `--tls-domain`, `--token-env`/`--client-token-env` — the perf-lab's
    secure cluster is now operable (W9 issues a mid-soak
    `TransferLeader` over mTLS + admin token).
  - Soak oracle: ack-log records past the idempotency cache's LRU window
    (10k entries) are now **survival-verified** by the linearizable Get
    alone (the replay probe is skipped — it would re-apply as `New` and
    pollute the key) and counted separately as `survival_only`; the
    `verified` headline total is unchanged in meaning. Previously any
    soak longer than the cache window (e.g. 2h runs) failed the oracle
    on the oldest sampled records — a latent limit first exposed by the
    perf-lab's W9 smoke (16.7k acks). The crash-harness oracle
    (tests/crash) is unchanged.

### Performance

- **Zero-allocation metric labels on hot paths**: `group_id`/`node_id` label
  values were built with `to_string()` on every client write
  (`rr-server/dispatch.rs`), every AppendEntries RPC incl. heartbeats
  (`rr-raft/network_factory.rs`), and every applied entry
  (`rr-raft/service_adapter.rs`). They are now interned once per id as
  `&'static str`, removing that allocation stream entirely.
- **No per-request routing-table copy**: `ShardRouter::route` reads the
  provider's current table via the new `routing_table_snapshot()`; with
  `MemConfigProvider` the snapshot is an `Arc` clone instead of a full table
  clone on every request.

### Fixed

- **Build no longer requires a system `protoc`**: `rr-network`'s build script
  now sets `PROTOC` from `protoc-bin-vendored`, so CI runner images (which no
  longer ship `protobuf-compiler`) and fresh dev machines build the workspace
  with zero system protobuf install. This un-reds every CI job — `build`,
  `test`, `clippy`, `bench`, … all failed at `rr-network` codegen with
  "Could not find `protoc`".

- **CI runner image gaps beyond protoc — libsasl2, cargo args, advisories**:
  - `sasl2-sys` is now `vendored` (cyrus-sasl built from source) in
    `rr-ingestor-kafka` and `rr-lab-producer` — runners lack `libsasl2-dev`;
    same rationale as `ssl-vendored`. SASL client capability is unchanged.
  - doc-test job: dropped `--no-deps` from `cargo test --doc --workspace`
    (cargo 1.98 rejects the flag for `test`).
  - All workspace-building jobs install `protobuf-compiler` (apt; `brew` on
    macOS): third-party `etcd-client`'s build script compiles protos with the
    system protoc — it cannot use our vendored one. (`rr-network` itself
    stays vendored, so dev machines still need nothing.) The same step also
    installs `libcurl4-openssl-dev` + `zlib1g-dev` on ubuntu: librdkafka's
    cmake build (rdkafka-sys) compiles C sources needing curl/zlib headers,
    which runner images lack (macOS provides them via the Xcode SDK).
  - Dependency bumps clearing the 10 error-level `cargo audit` advisories:
    aws-lc-sys 0.38→0.45 (+aws-lc-rs 1.16.1→1.18.1), rustls
    0.23.37→0.23.45, rustls-webpki 0.103.9→0.103.15, h2 0.4.13→0.4.19,
    quinn-proto 0.11.14→0.11.18, crossbeam-epoch 0.9.18→0.9.21, anyhow
    1.0.102→1.0.104. `rand` deliberately stays at 0.8 — the crash harness's
    `CRASH_SEED` replay contract depends on a stable `StdRng` stream;
    remaining advisories are warning-level.

- **Watch-loop `NodeRemoved` now honors the membership quorum guard**
  (ADR A-0012 follow-up, closes the last unguarded entry of INV-7): the
  config-watch path applied `change_membership(group, replicas − nid)`
  unconditionally, so a transient config-provider glitch (or a hasty
  `remove_node`) could shrink a group below `MIN_VOTERS_AFTER_REMOVAL` —
  or to zero — with no operator involvement. The automatic path now shares
  the `membership_change_is_safe` predicate with the Admin RPC guard:
  unsafe removals are refused with a `warn!` audit line and the group is
  left unchanged. There is deliberately no `force` on this path — an
  automatic event carries no operator intent; a sub-quorum teardown must
  go through `change-membership --force` / `node-reset`. Per A-0012 Option
  B the guard stays out of the manager layer (no raft-membership vs
  routing-table divergence). New integration test
  `test_node_removed_quorum_guard` (negative control verified: without the
  guard the group is shrunk to 1 voter).

- **Integration-suite election flakes (test harness only)**: since the
  openraft alpha.34 migration, freshly elected leaders fast-fail
  lease-gated proposals with `ForwardToLeader::empty()` until their blank
  entry commits — but the `wait_for_*` helpers only observe
  `current_leader`, so one-shot writes right after an election raced lease
  establishment (`test_leader_isolation_and_recovery` /
  `test_minority_partition_blocks_writes` / `test_3node_leader_failover`
  flaked at ~1/3 under parallel load; attribution runs proved it
  independent of the W9 vendor patch). New harness helper
  `write_when_lease_ready` retries on `ForwardToLeader` until the lease is
  up; applied at all four post-election write sites. 15/15 full-suite
  rounds green afterwards (baseline 4/12 failed).

- **Vendored openraft patch #3 — become-leader flush-cursor inflation
  (W9 INV-1 root cause, 2026-09-21)**: the become-leader path emitted
  `Command::UpdateIOProgress { when: None }`, and `run_update_io_progress`
  reports `Notification::LocalIO` at submit time — which `handle_local_io`
  treats as *flushed* (`log_progress.try_flush` + leader
  `update_local_progress`). Harmless with synchronous storage
  (accepted == durable), but under Rr's async append + pending overlay
  (A-0015: submitted ⇒ visible) a node elected with an unflushed backlog
  had its `flushed()` cursor inflated over entries no fsync had covered:
  the new leader self-matched its unflushed tail, so a quorum could commit
  (and ACK) an entry that was durable on only one node — the exact W9
  anatomy (stale-majority election then truncated the last holder). The
  command is now gated on the real flush of the pre-election accepted IO
  (`when: Condition::IOFlushed { prev_accepted }`), mirroring the existing
  follower duplicate-append guard. Fresh-start behavior (`None`) is
  unchanged — a recovered on-disk log is already durable. Companion fix in
  `run_update_io_progress`: the gated command can run after the new
  leader's blank append, so its `submit()` is now `try_submit()` — a plain
  `submit` would regress the monotonic submitted cursor (debug panic) when
  the gate delays execution. Vendor regression test
  `test_become_leader_gates_io_progress_on_prior_accepted_io` (note:
  openraft's own test suite requires network-unavailable dev-deps to
  execute locally; the test is compile-reviewed and runs in any networked
  checkout).
- **`RocksLogStorage::save_vote` now drains the append backlog first**
  (W9 / INV-1, same root family): the vote write bypasses the IoBatcher
  (direct `put`), so a fast vote completion reached openraft's monotonic
  `flushed` cursor while log appends were still queued — and because `IOId`
  ordering is vote-dominant, the cursor jumped over the unflushed tail,
  releasing `Condition::IOFlushed`-gated AppendEntries responses for
  entries no fsync had covered. `save_vote` now awaits the batcher's
  `flush_barrier()` first, restoring "vote flushed ⟹ prior log IO
  flushed". New test `test_save_vote_waits_for_pending_appends`
  (registered under INV-1).
- **Vendored openraft patch #4 — committed-truncate fail-stop guard**
  (soak run 38045591646 defense in depth, 2026-10-19):
  `following_handler::truncate_logs` (the sole producer of
  `Command::TruncateLog`) previously guarded its floor with a
  debug-only assertion, so a replication stream that conflicted inside
  the committed segment would silently erase committed entries in
  release builds. It now asserts — before any in-memory state mutation —
  that `since > local_committed.index()`, with the vote/log context in
  the panic message. Under legal raft this can never fire (leader
  completeness plus the `has_log_id` committed mask keep every
  legitimate truncation strictly above committed; `install_full_snapshot`'s
  `truncate(committed.next_index())` sits exactly on the allowed boundary),
  so a trigger means election safety was already breached upstream and
  the node fails stop instead of widening replica divergence. New vendor
  unit tests in `following_handler/truncate_logs_test.rs` (truncate at /
  below committed panics; above committed unaffected).
- **Vendored openraft patch #5 — committed index regression guard**
  (soak run 38045591646 defense in depth, 2026-10-19): `LogId`/`LogIOId`
  ordering is lexicographic on `(vote/leader_id, index)`, so a commit
  reported by a higher-vote leader with a *shorter* log compared
  "greater" and was adopted by `RaftState::update_local_committed` —
  depressing the local committed cursor (apply ceiling) from index
  1434247 to 392176 and unmasking the committed suffix from
  `has_log_id`, which let conflict truncation physically erase ~1.04M
  committed, client-ACKed entries. The cursor's index now never moves
  backwards: an update whose index is below the current local committed
  index is rejected with a `warn!`, regardless of vote ordering.
  Legitimate paths are unaffected by construction: leader completeness
  guarantees a legally elected leader's commit carries an index ≥ the
  locally committed one (same-vote and same-index updates keep the
  original lexicographic comparison), and snapshot installation advances
  `apply_progress` directly, bypassing the guarded funnel. Complements
  vendor patch #4 (committed-truncate fail-stop guard): #5 keeps the
  cursor correct, #4 panics if anything still tries to cut into it.
  New vendor unit tests in `raft_state/tests/update_committed_test.rs`
  (regression rejected / same-vote progression / higher-vote
  higher-index advance / funnel semantics; negative control verified —
  both guard tests go red without the patch).

### Changed

- **`GetClusterInfo` returns real raft-runtime topology** (previously a
  documented placeholder): `leader_node_id` is now sourced from
  `raft.metrics()` (`current_leader`; `0` while no leader is known, e.g.
  mid-election — 0 is not a valid `NodeId`), and `replica_node_ids` carries
  the full live membership (voters + learners). Runtime-sourced per ADR
  A-0013 — the routing-table `GroupMeta.leader` must not be reintroduced.
  Groups whose raft instance is mid-teardown are skipped instead of failing
  the whole RPC. Client-side leader discovery is unchanged: the SDK still
  relies on the `leader_info` redirect cache.

- **`RaftGroupManager::start_group_rocks` is now a compatibility shim**
  (ADR A-0016): it asserts `rocks_base_path` is set (same error as before)
  and delegates to the unified `start_group`. Its network factory is no
  longer hard-coded to `NoopNetworkFactory` but taken from the manager's
  construction — no observable change for existing callers, since the only
  rocks-configured constructor (`new_with_rocks`) uses NoopNetwork. This
  unlocks replicated + durable groups via
  `new_with_network(...).with_rocks_base_path(...)`.

## [0.2.0] — 2026-09-10

First production-ready release. Rolls up everything from `v0.2.0-rc1` and
`v0.2.0-rc2`, the post-rc2 Phase 4 feature work (ADRs A-0005–A-0010), the
release-hardening batch (roadmap WS-1~WS-7, ADRs A-0011–A-0014), the
server-side latency-budget observability batch, the container-lab batch, and
the reliability program R-1~R-8 (incl. the ADR A-0015 log-visibility fix and
the R-2 TLA+ formalization). See
[`release-notes/v0.2.0.md`](./release-notes/v0.2.0.md) for the thematic
narrative, breaking-change list, and upgrade guide. Compare view:
[`v0.1.0...v0.2.0`](https://github.com/gardenias/rr/compare/v0.1.0...v0.2.0).

### Added

#### Security
- **TLS / mTLS** (`rr-network::tls::TlsConfig`): gRPC over rustls (tonic feature `tls-ring`), opt-in per node. `rr_raft::network_factory::TlsPaths` mirrors this on the outbound side; `ChannelPool::with_tls` dials over `https://` with client certs. New CLI flags `--tls-cert`, `--tls-key`, `--tls-ca`, `--tls-domain`, `--tls-require-client-cert`, mirrored env vars, and a `[tls]` config section.
- **Bearer-token auth** (`rr-network::auth`): `AuthConfig`, `BearerInterceptor`, `attach_bearer_token` helper. AdminService and ClientService each carry their own token. RaftService is intentionally authenticated **only** via mTLS. Constant-time comparison defeats timing attacks. Tokens are loaded from environment variables referenced by name in `[auth]` config — never stored in config files.
- **Per-service rate limiting** (`rr-network::rate_limit`): token-bucket limiter built on `tokio::sync::Semaphore` (zero new deps). `RateLimitConfig { admin_rps, client_rps, burst }`, `RateLimitInterceptor`, `RateLimiter`, `ChainedInterceptor`. **`RaftService` is structurally exempt** — throttling Raft destabilises consensus. On exhaustion returns gRPC `RESOURCE_EXHAUSTED`. New CLI flags `--admin-rps`, `--client-rps`, `--rate-limit-burst`; matching env vars; `[rate_limit]` config section.
- **Audit log** via dedicated tracing target `rr::audit`: every AdminService RPC emits a structured info-level event with `op`, `peer` (socket addr), `node_id`, and per-operation fields. Mutation RPCs emit both dispatch and result events for forensic pairing. Schema and routing recipes in [`docs/deployment/audit-log.md`](./docs/docs/deployment/audit-log.md).
- `scripts/gen-dev-certs.sh` — Self-signed CA + per-node certs generator for local dev. SANs cover `localhost`, `127.0.0.1`, `rr-N`, `nodeN.rr.local`.
- **Production safety validation in `RrNode::new`**: refuses to start when any of five gates is violated — RaftService without TLS, RaftService without mTLS, bearer-token auth without TLS, AdminService without TLS, or AdminService without `admin_token`. All violations are aggregated into a single error so operators fix the config in one pass. Closes F2.1 + F2.2 + F2.4 from the F1-F4 review.
- **`NodeConfig::allow_insecure` opt-in escape hatch** (CLI `--allow-insecure`, env `RR_ALLOW_INSECURE`, TOML `[node].allow_insecure`) for dev / examples / CI. When enabled, the node emits a structured `INSECURE MODE ACTIVE` warning at startup and sets the Prometheus gauge `rr_security_insecure_mode = 1` so monitoring can alert on accidental production deployment.
- **Audit log** now covers `BearerInterceptor` and `RateLimitInterceptor` rejections: per-reject events on `rr::audit` with `op="auth"` / `op="rate_limit"`, `service` (admin/client), `peer`, and `reason`. `TransferLeader` now emits paired before/after events matching `TriggerSnapshot`. New `rr_network::audit` module with `ServiceLabel` enum and `AUDIT_TARGET` constant. Schema documented in `docs/deployment/audit-log.md`.
- **Audit event pairing completed** (AdminService): `DrainNode` now emits a finished result event on the main path (previously only the no-op branch had one), and `reset_ingestor_offset` emits a dispatch event. Auditing per-request on the `ClientService` hot path was reviewed and deliberately rejected — the documented audit scope is AdminService.
- **`InstallSnapshot` byte cap**: server-side enforces a 4 GiB ceiling on the total streamed bytes (`MAX_INSTALL_SNAPSHOT_BYTES`) to block memory-exhaustion DoS from authenticated peers. Closes F2.6.
- **change-membership server-side quorum guard (ADR A-0012)**: requests whose target voter set is `< 2` are rejected with `RaftError::UnsafeMembershipChange` → `FAILED_PRECONDITION`; removing down to 0 voters is never permitted (use `node reset`). Escape hatch: `ChangeMembershipRequest.force` (wire-additive) + `rr-admin group change-membership --force`; forced acceptances emit a warn-level audit event. `MIN_VOTERS_AFTER_REMOVAL = 2` moved to `rr_core::membership` as the single definition. Rolling-upgrade note: pre-guard servers silently ignore the `force` field.
- **TCP raft transport security gate (ADR A-0014)**: new `NodeConfig::raft_transport` (`RaftTransport::Grpc | Tcp { bind_addr }`, default `Grpc`). G6: TCP transport requires explicit `allow_insecure` regardless of gRPC TLS state. G7: TCP bind address must be loopback — and is **not** bypassable by `allow_insecure`.

#### Deployability
- **Production `rr-server` binary** (`crates/rr-server/src/main.rs`): replaces the v0.1.0 4-line stub with a full clap CLI (`serve` / `version` subcommands), TOML config loader, env var overrides, SIGTERM/SIGINT graceful shutdown, and a built-in `EchoStateMachine` for smoke testing.
- **TOML config loader** (`rr-server::config_file`): sections `[node]`, `[storage]`, `[cluster]`, `[tls]`, `[auth]`, `[rate_limit]`. Strict schema (`deny_unknown_fields`) and layered precedence (CLI > env > file > defaults).
- **Three example configs** in `examples/config/`: `single-node.toml`, `3node-cluster-node{1,2,3}.toml`.
- **Multi-stage Dockerfile** (Debian slim, non-root `rr:rr` at UID 10001), `.dockerignore`, `docker-compose.yml` for a local 3-node cluster.
- **K8s deployment artefacts**: raw manifests at `deploy/k8s/statefulset.yaml` (StatefulSet + headless Service + client Service + PodDisruptionBudget), full Helm chart at `deploy/helm/rr/` with TLS / auth / storage knobs in `values.yaml`.
- **`.github/workflows/release.yml`**: publishes the Docker image to `ghcr.io/gardenias/rr` and the Helm chart (OCI) to `ghcr.io/gardenias/charts/rr` on every tag push, plus an optional crates.io publish step gated on `workflow_dispatch`.

#### Operations
- **Offline backup / restore** in `rr-admin`: new `backup` / `restore` / `inspect-backup` subcommands producing gzip-compressed tar archives containing a `metadata.json` descriptor + the node's `data_dir`. Format versioned (`format_version = 1`); restore refuses newer majors. 6 unit tests + e2e roundtrip verified.
- **Online backup**: `AdminService.OnlineBackup` streaming RPC + `rr-admin online-backup --group N --output PATH [--force-fresh]` — streams the latest committed snapshot of a Raft group from a **running** node (unlike `backup`, which requires the node stopped); `--force-fresh` triggers a fresh snapshot first for groups that have never produced one.
- **Five operational runbooks** under `docs/operations/runbooks/`: `disk-full`, `leader-flapping`, `network-partition`, `snapshot-corruption`, `configprovider-unreachable`. Each structured as Symptom → Detection → Immediate mitigation → Root cause → Recovery → Prevention.
- **`rr-admin` upgrading guide** (`docs/deployment/upgrading.md`) walks operators through the v0.2.x → strict three-phase rolling upgrade.

#### Framework & protocol features (ADRs A-0005–A-0010)

##### Linearizable client reads (ADR A-0009)

- **New gRPC RPC `ClientService.ReadLinearizable`** in
  `crates/rr-network/proto/client.proto` — separate RPC method (not a
  flag on `Request`) so the consistency level is enforced at the
  protocol layer and cannot be silently downgraded by a misconfigured
  client. On the leader the server runs openraft
  `ReadPolicy::ReadIndex`, awaits local apply, and serves from the
  state-machine adapter.
- **Leader-aware redirect**: `ReadLocalResponse` gains an
  `optional LeaderInfo leader_info` field (wire-additive). When the
  contacted node is a follower, openraft returns
  `LinearizableReadError::ForwardToLeader`;
  `RaftGroupManager::linearizable_read` surfaces this as the new
  extended `RaftError::NotLeader { leader, leader_addr }` variant; the
  dispatch layer translates that into a redirect response and the SDK
  retries against the leader's `addr`. Mirrors the existing write-path
  forward-to-leader mechanism. Without this, a follower-targeted read
  would error instead of redirect.
- **`RaftError::NotLeader`** extended with `leader_addr: Option<String>`
  (was previously `{ leader: Option<u64> }` only). Write path
  populates it from `forward.leader_node.addr`; consumers in
  `error_convert` print both id and addr in the `Status::unavailable`
  message.
- **`ClientDispatch::submit_linearizable_read` trait method**
  now returns `LinearizableReadResult { payload, leader_node_id,
  leader_addr }` (was `Result<Vec<u8>, Status>`) so non-leader
  responses carry redirect info without being a transport error.
- **SDK methods** (`crates/rr-client/src/lib.rs`):
  `RrClient::read_linearizable`,
  `RrClient::read_linearizable_blocking`,
  `RrClient::read_linearizable_reactive`. The async variant honours
  `leader_info` redirects: on receiving a non-empty `leader_info`, the
  next retry attempt dials the indicated leader address. Transport
  failures fall back to round-robin over bootstrap addresses (same as
  stale reads). Total attempts still capped by
  `RetryConfig::max_retries`.
- **Tests**: three new unit tests in `rr-network::client_service::tests`
  (`test_read_local_routes_to_submit_read`,
  `test_read_linearizable_routes_to_submit_linearizable_read`,
  `test_read_linearizable_surfaces_leader_redirect_to_proto`) verify
  the handler routing and that a follower dispatch populates
  `ReadLocalResponse.leader_info` correctly. A real-cluster integration
  test (3-node, calling against a known follower, asserting redirect
  succeeds) is on the follow-up list — the current coverage validates
  proto-plumbing but not openraft end-to-end behaviour.

##### Per-caller rate limiting wire-up (ADR A-0005)

- **CLI flags** (`crates/rr-server/src/cli.rs`): `--per-caller-admin-rps`,
  `--per-caller-client-rps`, `--per-caller-burst` with `RR_PER_CALLER_ADMIN_RPS`
  / `RR_PER_CALLER_CLIENT_RPS` / `RR_PER_CALLER_BURST` env vars.
- **TOML schema** (`crates/rr-server/src/config_file.rs`): `[rate_limit]`
  section gains `per_caller_admin_rps`, `per_caller_client_rps`,
  `per_caller_burst` fields. `RateLimitSection` keeps `deny_unknown_fields`.
- **Plumbing**: `ResolvedRateLimit` extended; CLI → file → defaults merge
  order preserved (CLI wins). `main.rs` now wires the resolved per-caller
  fields into `RateLimitConfig` (previously hard-coded to `None`).
- **Semantics** (already shipped in `crates/rr-network/src/rate_limit.rs`):
  when per-caller is set, each authenticated caller (identified by
  `caller_token_fp`) gets its own token bucket; unauthenticated requests
  fall through to the global bucket. `RateLimitInterceptor::with_per_caller`
  + `PerCallerLimiterMap` (`Arc<RwLock<HashMap>>` keyed on the bearer-token
  siphash fingerprint, 5-minute idle TTL eviction).
- **RaftService remains unconditionally unthrottled** (per-caller path
  shares the same structural guarantee as the global path).

##### Outbox / side-effect delivery (ADR A-0007)

- **`rr_core::output::KafkaProducer` trait + `NoopKafkaProducer` stub**: pluggable
  Kafka producer abstraction for `OutputTarget::KafkaPublish`. Implementations
  should set Kafka-side idempotence; the framework guarantees only at-least-once
  delivery so duplicate publishes may occur on leader transitions.
- **Real `rr_raft::output_runner` send loop** (replaces v0.3-class no-op
  placeholder): leader-only background task that drains the per-group outbox
  via `RaftGroupManager::pending_outputs` → dispatches each `Output` by
  target type (`OutputTarget::RaftWrite` via `RrClient::send_request_idempotent`,
  `OutputTarget::KafkaPublish` via the supplied `KafkaProducer`) → on success
  calls `advance_output_watermark` → on failure logs + exponential backoff
  (`INITIAL_SEND_BACKOFF = 100ms`, `MAX_SEND_BACKOFF = 30s`). Honors stop
  signal and openraft metrics-channel leadership changes. Outputs targeting
  an unconfigured delivery client retry forever (no silent drops).
- **`RaftGroupManager::attach_output_runner` extended** to accept
  `Option<Arc<dyn KafkaProducer>>` alongside `Option<Arc<RrClient>>`. Existing
  call sites adjusted.
- **New metrics**: `rr_output_send_total{group_id, target}`,
  `rr_output_send_failures_total{group_id, target}`,
  `rr_output_send_duration_seconds{group_id, target}` (histogram),
  `rr_output_pending{group_id}` (gauge).
- **5 new unit tests** in `output_runner` covering: noop producer error,
  successful KafkaPublish dispatch, failure propagation, RaftWrite-without-client
  error, OutputHandle stop semantics.

##### Snapshot strategy (ADR A-0008)

- **`rr_raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST` constant** = 50_000 (ADR-recommended
  default at 100k ops/s → ~500ms snapshot cadence).
- **Configurable snapshot trigger**: `RaftNode::new_with_factory` /
  `new_with_rocks_factory` now accept `snapshot_logs_since_last: u64` (was
  hardcoded `LogsSinceLast(50_000)`). `RaftGroupManager::with_snapshot_logs_since_last`
  builder propagates the value to all groups it creates. `NodeConfig` in rr-server
  exposes the matching field + `with_snapshot_logs_since_last` method.
- **CLI flag `--snapshot-logs-since-last`** + env `RR_SNAPSHOT_LOGS_SINCE_LAST`
  + TOML `[node].snapshot_logs_since_last`. Precedence: CLI > env > TOML > default.
  Zero values are rejected at startup (would disable snapshots).
- **4 new config_file unit tests** covering: default = framework constant, TOML
  override, CLI overrides TOML, zero rejection.
- **1 new raft_node test** verifying the parameter is plumbed all the way into
  openraft's `Config::snapshot_policy`.

##### Per-CF RocksDB tuning (ADR A-0006)

- *(Already landed in code by 2026-05-07; surfacing in CHANGELOG.)*
- **`CfOptions` struct** in `rr-storage::rocks` with optional `write_buffer_size_mb`,
  `block_cache_size_mb`, `compression` per CF.
- **`RocksConfig.raft_log_cf` / `state_machine_cf`** optional overrides;
  `metadata` CF intentionally not exposed (defaults always sufficient).
- **`build_cf_options`** helper merges DB-level defaults with per-CF overrides.
- Recommended values documented in ADR A-0006 §"Recommended defaults" (NOT
  hardcoded — operators tune via TOML / programmatic `RocksConfig`).

##### WAL log-storage backend design (ADR A-0010)

- **New ADR `A-0010-wal-log-storage-backend.md`** — `accepted` status.
  Decision matrix evaluating `raft-wal` crate, TiKV `raft-engine`,
  sled / fjall, redb / heed, and a custom segmented log on `std::fs`.
  Chosen path: **custom segmented log**, because openraft 0.10-alpha
  binding compatibility, the librarian's "same effort" assessment vs
  forking `raft-wal`, and supply-chain risk on the 1-star upstream.
- **Implementation deferred** until a release-build profile shows
  RocksDB LSM overhead is the binding constraint on the Raft log path
  (current numbers are debug-build, CPU- and gRPC-bound). State
  machine and snapshots stay on RocksDB; WAL replaces only the log.
- **Implementation plan `.sisyphus/plans/rr-wal-impl.md`** — 12 phases
  covering segment writer, segment reader, vote / `last_purged` files,
  `RaftLogStorage` trait impl method-by-method, batcher reuse vs
  fork decision, storage-backend wiring, test parity (16 ported tests
  + 3 WAL-only torn-write / rotation / kill tests), benchmark
  acceptance (≥ 2× RocksDB), chaos-suite parity, and 24 h soak
  before flipping the default backend.

#### Durability & safety contracts (release-hardening, ADRs A-0011–A-0014)

- **Shutdown durability contract (ADR A-0011)**: `IoBatcher` is now a tracked
  spawn with an explicit `close()` protocol (closing flag → drain → final fsync
  → answer all pending callbacks → join; idempotent, 30 s deadline).
  `RocksLogStorage::close` added; `RaftGroupManager::stop_group` now awaits the
  batcher flush before returning (signature unchanged). The per-append relay
  `tokio::spawn` on the write hot path was removed — completions are answered
  directly by the batch worker (durability + latency, one change).
- **Snapshot failure propagation**: `try_create_snapshot_builder(force)` —
  non-forced failures defer and retry, forced snapshots go through a poisoned
  builder so `build_snapshot()` returns `Err`. An empty snapshot with valid
  metadata can no longer be produced or propagated.

#### Observability
- **HTTP observability endpoints** (`rr-server::health`): single hyper server on `metrics_port` serving `GET /health` (liveness, always 200), `GET /ready` (200 once `start()` completes, 503 otherwise — driven by a shared `ReadinessFlag`), and `GET /metrics` (Prometheus). K8s manifests + Helm chart updated to use HTTP probes instead of TCP-on-gRPC-port.
- **W3C Trace Context propagation** (`rr-network::tracing`): `extract_traceparent`, `extract_tracestate`, `attach_traceparent`, `attach_tracestate` helpers. Inbound RPC handlers log the incoming traceparent on the tracing span for cross-service correlation. Does **not** bundle the OpenTelemetry SDK — users bring their own OTLP stack.
- **`rr-client` side metrics**: `rr_client_request_duration_seconds` (histogram, `outcome=success|failure`), `rr_client_retries_total` (counter), `rr_client_leader_cache_hits_total` / `_misses_total` (counters). All labelled with `group_id`.
- `#[instrument]` on `ClientDispatchImpl`, `RaftGroupLookupImpl`, and `RrClient::{send_request, send_request_with_partition, get_cluster_info}` for full span coverage on the hot paths.
- Complete metrics documentation for all 14 server-side + 4 client-side metrics (previously only 6 documented). See [`docs/reference/metrics-reference.md`](./docs/docs/reference/metrics-reference.md).
- **Request-level metrics at the client-protocol boundary** (`rr-server`
  dispatch): `rr_server_requests_total{group_id, op, outcome}` counter and
  `rr_server_request_duration_seconds{group_id, op}` histogram, where
  `op = write|read|lin_read` and
  `outcome = success|redirect|error|idempotency_expired`. The Kafka ingestor
  path bypasses this boundary (it calls `write_batch` directly).
- **State-machine apply observability** (`rr-raft` adapter):
  `rr_raft_apply_duration_seconds{group_id}` histogram (idempotency layer +
  `Service::handle`) and `rr_raft_applied_entries_total{group_id}` counter —
  an exact applied-entries TPS source with no snapshot steps or
  leader-switch gaps, unlike `rate(rr_raft_last_log_index)`.
- **Replication RTT histogram**
  `rr_raft_append_entries_rtt_seconds{group_id, follower_node_id, kind}`: the
  time-based companion to the entry-count `rr_raft_replication_lag` gauge;
  `kind` splits idle heartbeats from real append traffic (the first curve to
  rise under chaos injection).
- **Kafka-path commit counter**
  `rr_ingestor_commands_committed_total{group_id}`: commands durably
  committed via Raft, cross-checkable against the producer's send rate and
  the application-level soak counter.
- **`source` label on `rr_proposal_duration_seconds`** (`client` | `batch`):
  single client writes and ingestor `write_batch` (~100 commands per round
  trip) no longer share one unlabeled series. Leader redirects are excluded —
  the request never reached raft.
- **Recording rules + dashboard**: `rr-recording-rules.yml` rebuilt around
  the latency budget (request → proposal → apply; replication RTT; ingestor
  throughput); the Grafana cluster-overview dashboard gained matching panels;
  `metrics-reference.md` documents every new series with usage notes (incl.
  "do not mix client/batch means").
- Test `apply_emits_prometheus_metrics` (rr-raft) asserting the adapter
  emits its counter + histogram into a real Prometheus registry.

#### Reliability
- `DurabilityPolicy` enum in `rr-storage::rocks::RocksConfig` with `Strict` (fsync + WAL sync, **default**), `Balanced` (per-batch fsync), and `Fast` (no fsync — testing only) options.
- `RaftError::InitializationFailed { reason }` variant in `rr-core::error`.
- **Bidirectional fault isolation** for chaos tests: `FaultAwareChannelPool` + per-node pool wiring in `ClusterBuilder::with_fault_injector`. All 4 chaos tests (`test_leader_isolation_and_recovery`, `test_follower_isolation_and_catchup`, `test_packet_loss_convergence`, `test_latency_under_high_load`) now run in the default CI test job (no longer `#[ignore]`d).
- **Integration test for HTTP `/health`, `/ready`, `/metrics`** lifecycle (`tests/integration::test_health_endpoints_full_lifecycle`): drives all four routes via real reqwest HTTP/1 and asserts on status codes, response bodies, and Prometheus content-type. Lifts `rr-server::health` line coverage from ~24% baseline toward ~80%.

#### Reliability engineering & testing (program R-1~R-8)

- **不变量登记执法机制(R-1)**:`scripts/check-invariants.sh` 五项双向检查(INV 有登记必有
  代码引用、代码引用不得指向未登记 INV、每条 INV 至少一个关联测试、登记的测试名必须存在、
  被登记的测试必须携带 `INV-n` 标记);新 CI job `invariants-lint`(纯文本检查,阻塞式);
  PR 模板新增两条登记/命名检查项 + Testing Performed 的 INV 编号栏;CONTRIBUTING 新增
  "Invariant enforcement review" 评审四问;INV-1~8 的 52 条既有测试全部补上 `/// INV-n:` 反链;
  `invariants.md` 新增维护规则 5(登记触发元规则:能力性宣称 / guarantee 级措辞 / 带语义承诺的
  pub API,任一命中即触发登记义务)。
- **性能基线与回归门禁(R-7)**:
  - `rr-perf-bench` 新增 `--json-out <path>`:scenario 结束后输出机器可读 JSON
    (meta = git SHA / rustc / OS / arch / storage / durability / payload / 并发档 + S1/S2/S3
    快照),兼容 `--cluster-addrs` 外接集群模式(此时 storage/durability 记为 `external`)。
  - `scripts/perf-baseline.sh`:一键采集 release 基线矩阵(S1 rocks/strict + S2 sweep ×
    strict/replicated-only/test-only/mem,含 s2-strict 期间 CPU 采样);参数可经环境变量覆盖。
  - 首份基线存档 `docs/perf/baselines/v0.2.0-baseline.md` + `raw-*.json` 原始数据
    (M3 Pro / macOS 26.6.1 / rustc 1.98.0;3 节点 loopback,128B 写):**~34k TPS 饱和,
    strict ≈ test_only ≈ mem(各档差 <2%),饱和点单进程 CPU ~3.6 核**。
  - **ADR A-0010 前置判据已回答**:前置 1(release 基线)✅;前置 2(存储是绑定约束)❌
    不成立 —— CPU 路径(codec/replication/apply)是约束,fsync/LSM 不热,**WAL 维持
    deferred**,IoBatcher 调优亦无收益;下一个性能杠杆需 CPU profiling 立项。
  - CI `bench` job 升级:main 分支 push 时 `--save-baseline main` 并缓存 `target/criterion`;
    PR 上 restore 后 `--baseline main` 对比,`scripts/bench-compare.sh` 按 R-7 阈值裁决
    (p<0.05 且均值回归 >15% → error,5~15% → warning 注解)。**`continue-on-error` 非阻塞
    ≥2 周**攒噪声数据后再议转阻塞(理由写在 job 注释,防止噪声门禁自欺)。
- **proptest 扩展(R-6,新增约 0.85s 测试时长)**:
  - `rr-router` 路由表变迁(`proptest_router_transitions_route_consistency`,INV-6):随机
    add/remove/reassign/membership 序列,每步对全 partition 宇宙断言 `route()` 与独立模型一致。
  - `rr-raft` 幂等 LRU/watermark(`proptest_idempotency_lru_watermark_model_parity`,INV-3):
    对照无界 HashMap oracle + 同容量 twin LRU + capture/restore 变换;watermark 单调、
    Replay 返回原值、Expired 满足契约必要条件、四路分派与模型精确同构。
  - `rr-raft` IoBatcher 交错(INV-1,rocks-gated):随机并发 × window × max_entries ×
    两种 submit 路径;每个 submission 恰好应答一次、批内按 big-endian log-index 有序、
    写错误广播给整批 caller、`close()` 幂等且事后 fail-fast、无 hang(20s 死线)。
  - `rr-core` framed 双向兼容矩阵(7 个 proptest):version ∈ {0, 1, current, current+1,
    u8::MAX} × 随机 payload;低版本必须可解、高版本必须明确 typed Err、任意字节不 panic;
    配套 **golden corpus**(`crates/rr-core/tests/golden/` 6 个固定字节样本,断言解码等价 +
    重编码字节一致——bincode 升级的静默漂移绊线)。
- **fuzz 扩展(R-6)**:新增 `fuzz_framed_entry_roundtrip` / `fuzz_apply_outcome_roundtrip`
  (decode→encode→decode 幂等,比纯 decode 深一层);**seed corpus 43 个样本入仓**
  (`fuzz/corpus/<target>/`,覆盖 valid/legacy-unframed/truncated/future-version;生成器
  `fuzz/examples/gen_corpus.rs` 可重生成);新 workflow `.github/workflows/fuzz-nightly.yml`
  (每日 03:23 UTC + 手动触发,6 target 矩阵各 15 分钟,crash 自动 tmin 最小化并上传
  artifact;纯信息性,不进 PR 门禁)。顺带修复 `fuzz/Cargo.toml` 的潜在损坏:
  `prost = { workspace = true }` 在独立 crate 下无法解析,改为钉 `prost = "0.14.3"` 并注释。
- **进程级 crash-recovery harness(R-4,新 crate `tests/crash`)**:真 SIGKILL/SIGTERM
  3 节点 rocks/strict 集群;client 侧 ack-log(fdatasync 落盘)即 oracle  ground truth;
  seed 化 kill 计划(`CRASH_SEED` 精确复现),事件流水 `fault-log.jsonl`;oracle 用
  **同幂等键 + 不同探针负载**回放全部已 ACK 写——存活证明强于 log-index 对比
  (同时验证日志存活 + 状态机不重复 apply + 幂等层跨重启有效)。
  `scenario_a_graceful_sigterm`(优雅关闭契约:20s 内 0 码退出 + clean-shutdown 标记 +
  已 ACK 全存活)与 `scenario_b_sigkill`(N 轮随机 kill)。新 workflow
  `.github/workflows/crash-nightly.yml`(每日 02:41 UTC,10 轮;信息性,不阻塞 PR,
  失败上传 run 目录 artifacts)。
- **存储故障注入包装层(R-4)**:`rr-core` 新 feature `test-utils` 下
  `FaultInjectingStorage`(`storage_fault.rs`)——四种可编程故障(fail_flush /
  torn_write 前缀应用 / ENOSPC(StorageFull 类)/ latency 慢盘),SplitMix64 seed 化
  确定性,组合顺序文档化(enospc 优先于 torn_write);8 个 wrapper 单测 +
  `rr-raft::storage_fault_tests` 4 个组提交路径消费者测试(fail fsync 报错不 panic、
  torn write 不留半条 entry、ENOSPC 广播整批 caller、慢盘不误判丢失)。
  **前置改造**: `IoBatcher` 存储参数泛化为 `Arc<dyn StorageEngine>`(R-4 计划选项 a)。
- **线性一致检查器(R-5,porcupine 风格,`tests/integration/src/linearizability.rs`)**:
  单 partition 单 key register 模型;`History`/`OpHandle` 两阶段区间语义记录
  (单进程 `Instant` 时钟,不跨进程比钟;失败/超时记 `?` 按部分序处理);
  回溯搜索 + read-match 剪枝 + `(value, bitset)` 失败态记忆化;违规时贪心
  delta-debugging 输出 1-minimal 冲突子历史 + 实时序链。`RecordingClient`
  把写与 `read_linearizable` 记入受检历史、`read_local` 记入独立的 session 历史
  (不送检)。`KvRegisterService` + `build_register_cluster`(FaultInjector 轮转隔离
  follower)。既有 `test_concurrent_writes_under_chaos_linearizability` 升级为
  计数断言(回归兜底)+ 记录历史送检双轨。
- **R-3 确定性仿真 spike(turmoil)——前提验证通过,按计划转替代方案**:
  - 证据测试 `tests/integration/src/turmoil_spike.rs`:tonic 0.14 server
    (`serve_with_incoming` + `SimIo` newtype 解决 `Connected` 无 blanket impl)×
    turmoil listener,client(`connect_with_connector` + `TokioIo` 包 turmoil stream)×
    sim 网络,echo 往返全绿、模拟时钟墙钟 ~0s。该测试随默认 test job 常驻——
    tonic/turmoil 升级若破坏此路径,CI 自动浮现重评信号。
  - spike 报告:`.sisyphus/evidence/rr-r3-turmoil-spike-2026-09-09.md`(完整试点的
    剩余工作量实测:per-host ClusterBuilder + RrNode serving seam ≈ 3~4 人日,超
    2 人日时间盒;重评触发条件在报告末节)。
- **R-3 替代方案:fault schedule 记录/回放**(`tests/integration/src/fault_schedule.rs`):
  seed 化生成(StdRng + pool 轮转)+ JSONL fault log(fdatasync/条,与 R-4/R-8 同族
  schema)+ `RR_FAULT_SEED` 重生成 / `RR_FAULT_REPLAY` 逐条回放;E3b/E5 两个 chaos
  测试接入(生成/回放双模式实测);6 个单测。
- **R-8 soak 自动化**:
  - `rr-perf-bench --scenario soak`:定率 keyed 幂等写 + 周期性 `read_linearizable`
    探针 + fdatasync ack log(JSONL)+ metrics.jsonl 计数快照;`--oracle-only`
    独立回放;oracle = 探针 Put(幂等缓存)+ linearizable Get 读回原始值,大日志
    均匀步长采样(首尾 100 条必含)。5 个单测。
  - `scripts/soak.sh` 一键 harness:lab 3 节点 + seed 化故障循环(SIGKILL / pause /
    网络分区,指数间隔;ENOSPC 有意不进 smoke 集,理由见脚本头注释)+ 三件套
    oracle(无丢失 / 无 panic / lag 60s 收敛)+ 报告入 `docs/perf/soak/`。
  - `.github/workflows/soak-nightly.yml`:每日 04:17 UTC 2h smoke,信息性;
    24h release 级经 `workflow_dispatch`,0.3.0 起进入 release checklist。
- **`formal/tla/` TLA+ 形式化(R-2)**:Rr 自有扩展协议的 4 份 spec + TLC 小模型实测:
  - `MembershipGuard.tla`(INV-7/A-0012,11,545 态零反例):并发 change-membership
    任意交错下 n→0 无条件拒绝、低于 MinVoters 必经 force+先审计、审计完备。
  - `OffsetExactlyOnce.tla`(INV-3,558 态零反例):exactly-once apply、缓存响应归属
    真实消息、watermark=最大已 apply、无虚假 Expired、哨兵 fail-closed。
  - `OutputWatermark.tla`(INV-4/A-0007,280k 态零反例):无幻影推进、保序游标;
    leader 切换落在发送成功↔watermark 推进窗口 ⇒ 重复投递**显式合法**(承诺语义)。
  - `RoutingTableSwitch.tla`(INV-6,双 cfg):Mem 整表快照 / etcd 按 key 增量两种
    watch 语义下,路由由视图唯一决定、收敛后无旧路由、watch 最终收敛。
- **植入反例实证**(`formal/tla/bugs/`,不进 CI 绿集):`MembershipGuardBuggy`(去 guard
  = P0#7 修复前形态)被 TLC 3 步抓到 `QuorumLossOnlyWithAudit` 违反;`OffsetClampBug`
  (哨兵 clamp-to-0 = P0#6 原始缺陷)被抓 `ResponseBelongsToGenuine` 违反。
- **CI**:`.github/workflows/formal.yml`(每周日 05:29 UTC + dispatch,非阻塞,
  tla2tools v1.7.4 钉版,失败传 artifact)。
- **双向链接**:invariants.md 的 INV-3/4/6/7 各增"形式化(R-2)"条目;spec 头部注释
  含 spec↔实现 file↔INV 三向链接;运行手册与性质映射在 `formal/tla/README.md`。
- **Hardening test suites**: durability T1–T6 (incl. `test_stop_group_flush_preserves_acked_and_pending`),
  `membership_guard` e2e (quorum rejection + force path), `leader_redirect_failover`
  e2e (client reconverges after leader kill without `GroupMeta.leader`), G6/G7
  gate test matrix, `GroupMeta` serde shape guard tests.
- **Linearizable-read redirect integration test** (ADR A-0009 follow-up):
  `test_read_linearizable_redirect_from_follower` points the SDK at a known
  follower and asserts the read converges via the `leader_info` redirect.
  The integration-harness `EchoSM` gained a `read_handle` (mirrors the
  production `EchoStateMachine` reader).
- Two regression tests for the client fake-success fix (mock gRPC server:
  leader-unknown window no longer returns `Ok`, client converges once a
  leader is known) and two for the blocking-API preflight (current_thread /
  no runtime now error instead of panic).

#### Local lab & benchmarking tooling

- **`deploy/lab/` local lab**: 3-node containerized `kv-store` (real KV semantics) with Redpanda as a real Kafka input source, Prometheus scraping + provisioned Grafana dashboard, Toxiproxy override for fine-grained network faults, a 7-scenario chaos suite with scripted pass/fail assertions, and `soak.sh` for hours-long stability runs with sampled reports (`deploy/lab/reports/`).
- **`tools/rr-lab-producer`**: rate-limited Kafka command producer emitting bincode `KvRequest`s — `Increment lab:counter` (exactly-once assertion: final counter must equal the sent count) + sequenced `Put lab:seq:<i>` keys for spot verification. `acks=all` + producer idempotence; `PRODUCER-DONE` summary line for scripting.
- **`kv-store` example**: new flags `--listen-addr`, `--metrics-port`, `--kafka-brokers`/`--kafka-topic-prefix` (real `KafkaIngestor` wiring behind `--features rdkafka`); advertise address now comes from the roster entry so container hostnames work. New `KvRequest::Increment { key, delta }` (appended last — wire ordinals unchanged; counters stored as decimal UTF-8).
- **`rr-perf-bench`**: external-cluster mode (`--cluster-addrs`, skips spawn/bootstrap; spawn-only flags conflict-checked), `--progress-secs` heartbeat for long runs, sampled WARN logging of the first op failures.

#### Documentation
- **C4 architecture diagram suite** under `docs/architecture/c4/` — 13 C4-PlantUML diagrams (Context, Container, four Component levels, Deployment, five Dynamic flows).
- **Semver / API stability document** ([`docs/reference/compatibility.md`](./docs/docs/reference/compatibility.md)): explicit commitments per surface (Rust API, gRPC wire, on-disk, config, CLI, metrics), pre-1.0 Cargo convention, MSRV policy, deprecation process.
- **Bilingual consumer guide** ([`docs/guide/consumer-guide.md`](./docs/docs/guide/consumer-guide.md)) for downstream business projects: covers all three consumption modes (`git+tag`, `path`, `crates.io`) with full `Cargo.toml` templates, private-repo SSH setup, `net.git-fetch-with-cli` configuration for CI runners, and MSRV pitfalls.
- TLS configuration + troubleshooting guide ([`docs/deployment/tls.md`](./docs/docs/deployment/tls.md)).
- K8s deployment guide ([`docs/deployment/kubernetes.md`](./docs/docs/deployment/kubernetes.md)) with raw manifests + Helm + cert-manager TLS example.
- **`Service` and `StateMachine` trait safety invariants** (T0.1 of Phase 4 Tier 0): three rustdoc invariants documenting OpenRaft's concurrency contract — `snapshot()` atomic capture, `apply()` synchronous mutation, `restore()` complete replacement. Worked anti-pattern + correct-pattern examples. Cross-references to ADR A-0002 with OpenRaft source citations. Same content surfaces in `docs/guide/service-trait.md` and `docs/guide/state-machine-trait.md`.
- **Storage durability policy doc** (`docs/guide/storage-engines.md`): comparison table of all three policies, the v0.3.0 rename note, and the runtime behaviour (startup `tracing::warn!` when policy is non-Strict).

#### Project meta
- `LICENSE` (MIT) at repository root.
- `CHANGELOG.md` following Keep a Changelog format.
- `CONTRIBUTING.md` with development setup, testing, and PR guidelines.
- `CODE_OF_CONDUCT.md` (Contributor Covenant v2.1).
- `.github/pull_request_template.md`.
- **CI**: new `semver-checks.yml` workflow (git-tag baseline via
  `--baseline-root`; a blocking gate as of the `v0.2.0` tag).

### Changed

- **`RocksConfig::default()` now uses `DurabilityPolicy::Strict`** (fsync + WAL sync). This fixes a critical correctness issue where Raft durability guarantees were effectively broken — see [Fixed](#fixed) below. Roughly 10× slower on writes than the prior implicit "Fast" behaviour, but actually durable.
- `RocksConfig::for_testing()` uses `DurabilityPolicy::Fast` to keep test suites fast.
- `RocksStorage::put` / `delete` / `write_batch` now use `*_cf_opt` / `write_opt` variants with `WriteOptions` driven by the chosen `DurabilityPolicy`.
- **`RaftNode::new_with_factory` / `::new_single_node` / `::new_with_rocks_factory`** now return `Result<Self, RrError>` instead of panicking on config validation or RocksDB open failure. **Breaking change** for direct callers (all 27 internal call sites updated). Library users propagating via `?` need no change. Removes a class of production crashes where a node panicked on startup instead of surfacing a clean error.
- **MSRV bumped from 1.82 to 1.98** (1.88 at rc2, 1.98 for the final tag).
  The 1.88 step was required by transitive dependencies (`clap_lex ≥ 1.1.0`
  needs `edition2024` stable in 1.85; `validit 0.2.5` uses `let_chains`
  stable in 1.88). `rust-toolchain.toml`, workspace `rust-version`, the CI
  `msrv` job pin, and the Dockerfile builder all move together; the pinned
  toolchain file makes contributors auto-install the right toolchain on
  first invocation.
- `README.md` architecture diagram upgraded from a generic Mermaid graph to a proper C4 Container diagram with technology labels.
- All Mermaid diagrams in the docs site converted to C4-PlantUML for richer rendering. `@docusaurus/theme-mermaid` removed.
- Architecture documentation corrected to describe the actual **dual-server gRPC topology**: `RaftService` + `ClientService` + `Health` share `listen_addr`; `AdminService` runs on a separate `tonic::Server` bound to `admin_port` so admin traffic cannot starve consensus bandwidth. Replaces the prior inaccurate "three services on one tonic Server" claim that referenced a stub type that never shipped.
- CI: `clippy` job now runs with `--all-targets` so test code, benches, and binary main paths are also linted. Coverage job now includes `--features rr-storage/rocks` so RocksDB code paths show up in Codecov.

#### Security (breaking — gated by `allow_insecure = false` default)
- `BearerInterceptor::new` and `BearerInterceptor::disabled` now require a `ServiceLabel` argument so audit events identify which service rejected.
- `RateLimitInterceptor::disabled` and `RateLimitInterceptor::with_limiter` likewise require a `ServiceLabel`.
- `config_file::resolve_auth` now returns `Result<ResolvedAuth>` and fails closed when an `*_token_env` variable is unset or empty (was: silently disabled auth — a fail-open security anti-pattern). Closes F2.3.
- `IntoStatus` impls for `RaftError`, `StorageError`, `ConfigError`, `NetworkError`, and `CodecError` no longer leak peer addresses, openraft internals, bincode errors, file paths, or storage corruption strings to the gRPC client. Detail is logged at `warn!` / `error!` server-side; clients see one short generic message + the correct gRPC code. Closes F2.5.
- `RaftGroupLookupImpl` and `ClientDispatchImpl` similarly sanitize all internal errors before returning to the client.

#### Metrics (breaking)
- **`rr_snapshot_installs_total`** is now correctly labelled with `group_id` (was: hardcoded `"unknown"`). Required `ServiceStateMachineAdapter::new` to take `group_id` as its first argument; updated `examples/counter`, `examples/kv-store`, README, and getting-started guide. Closes F1/T3.

#### Storage (breaking — deprecation aliases preserve compilation)
- **`DurabilityPolicy` rename** (T0.2 of Phase 4 Tier 0):
  - `Balanced` → `ReplicatedOnly` (clearer intent — only safe with multi-node replication).
  - `Fast` → `TestOnly` (clearer intent — never use in production).
  - Old names remain as `#[deprecated(since = "0.2.0")]` aliases for one minor version. TOML / CLI also accept old strings (`"balanced"` / `"fast"`) but emit a deprecation warning at startup. Aliases are removed in v0.3.0 — migrate before then.
- `RocksStorage::open` now emits a `tracing::warn!` at startup when the policy is not `Strict` (violates OpenRaft's durability contract per ADR A-0002).

#### Dependencies / Toolchain
- **`openraft` migrated `0.10.0-alpha.19` → `0.10.0-alpha.34`**: adapts to the `SnapshotData` move (`RaftTypeConfig` → `RaftStateMachine` / `RaftSnapshotBuilder` / `RaftNetworkV2`), `snapshot_id` removal from `SnapshotMeta` (3-field wire layout preserved), and `Trigger::elect(pre_vote)`. Inherits upstream fixes including commit-safety #1802, snapshot membership divergence #1808, and RaftCore stalls #1810. New behavior: leader-lease-gated proposals fast-fail with `ForwardToLeader::empty()`; a leader removed from membership auto-transfers after 150 ms. Rolling-upgrade note: vote RPCs from pre-upgrade nodes fail to decode on upgraded nodes (bincode positional layout gained a trailing field) — upgrade the leader last and avoid elections mid-roll.

#### Release-hardening batch (breaking — landed before the v0.2.0 API freeze)

- **`GroupMeta.leader` removed (ADR A-0013)**: the volatile raft-runtime leader
  no longer lives in the static routing table. Also removed:
  `ShardRouter::get_leader` / `get_leader_addr`, `RouterError::NoLeader`, and the
  `router.get_leader_addr.success` metric. Leader discovery is exclusively the
  client SDK's redirect cache fed by `NotLeader { leader_addr }` / `leader_info`.
  etcd-stored routing data needs **no migration** (group values are `Vec<NodeId>`;
  `leader` was derived at read time); legacy JSON containing a `leader` key is
  silently ignored (serde guard tests added).
- **`OffsetToRequestId` and `kafka_offset_to_request_id` now return `Result`**:
  undecodable offset bytes and negative sentinels are fail-closed — the ingestor
  supervisor exits with an error instead of silently mapping to `0` (which broke
  the exactly-once guarantee). This closes the silent-degradation hole in the
  A-0001/A-0003 chain.
- **All public error enums are now `#[non_exhaustive]`**: `RrError`,
  `RaftError`, `StorageError`, `ConfigError`, `RouterError`, `NetworkError`,
  `CodecError`. (`BatchOp` and `ApplyOutcome` were reviewed and deliberately left
  exhaustive — a silently-ignored new variant would be a correctness hazard there.)
- All workspace crates now share `workspace.package.version = "0.2.0"`;
  Helm `Chart.yaml` and the Java SDK fallback version aligned.
- K8s raw manifests default to a configuration that passes the startup security
  gates (TLS Secret mount + required `admin_token`); Helm chart adds
  `security.allowInsecure` (default `false`; combined with `tls.enabled=false`
  the template `fail`s with remediation hints). Both gained pod anti-affinity.
- Five more public enums are `#[non_exhaustive]`: `ServiceLabel`
  (rr-network), `RaftTransport` (rr-server), and `SecurityProtocol` /
  `KafkaIngestorError` / `KafkaOffsetError` (rr-ingestor-kafka). Matches all
  live in the defining crates, so no downstream breakage; the
  framework-dispatched enums (`BatchOp`, `OutputTarget`,
  `IdempotencyDecision`, `RoutingTableChange`, `PartitionConfig`) were
  reviewed and deliberately left exhaustive — the compiler forcing match
  updates there is a correctness feature.
- **rr-client never fake-succeeds in the leader-unknown window**
  (correctness, P1): `send_request_inner` treats every `success=false`
  response as retryable — a write is acknowledged only after it was actually
  applied. A follower that knows the leader updates the redirect cache; one
  that doesn't clears the cache and the client falls back to bootstrap
  polling; when attempts run out the call returns
  `RaftError::NotLeader { leader: None, leader_addr: None }`. Previously the
  SDK returned `Ok` with an empty payload in this window (and on the final
  attempt of a redirect chain), silently acknowledging unapplied writes.
- **Blocking client API preflights its runtime**: the four `*_blocking`
  methods now return a typed error ("requires a multi-thread runtime" /
  "unsupported on current_thread") instead of panicking inside tokio when
  called on a `current_thread` runtime or outside any runtime.
- **`ResolvedConfig::resolve` returns typed `ConfigError`** (was
  `anyhow::Result`): `anyhow` no longer appears in any library crate's
  public signature. It remains in the `rr-server` / `rr-admin` binaries.
- **`token_fingerprint` widened to 64-bit (16-hex)**: the `caller_token_fp`
  field in auth-reject audit events changes format (was 8-hex). Constant-time
  token comparison is now backed by the vetted `subtle` crate.
- Wire-level `MAGIC` / `VERSION` constants (`APPLY_OUTCOME_*`,
  `FRAMED_ENTRY_*`, `FRAMED_SNAPSHOT_*`) are `#[doc(hidden)]` — the on-the-wire
  format commitment lives in `docs/reference/compatibility.md`, not in
  docs.rs constant entries.
- `ConfigProvider::watch_changes` stream semantics are now contractual in
  rustdoc: no initial snapshot (call `get_routing_table` first), no global
  ordering across domains, the in-memory backend **silently drops** events
  on lag while the etcd backend applies backpressure.

### Fixed

- **CRITICAL — RocksDB was not configured with fsync**, risking data loss on power failure. This violated Raft's durability contract: a power loss could lose committed log entries. Fixed by introducing explicit `DurabilityPolicy` with `Strict` as the default. **All v0.1.0 deployments should upgrade.**
- **`FaultAwareChannelPool` isolation was one-directional only** (blocked inbound but not outbound), which made `test_leader_isolation_and_recovery` unable to trigger leader re-election and forced 4 chaos tests to live behind `#[ignore]`. Fixed by refactoring to per-node pools sharing a common `FaultState`; isolation is now bidirectional and all chaos tests run in CI.
- CI `msrv` job toolchain pin (1.82) was inconsistent with `workspace.package.rust-version` (1.88) — would have failed `cargo check` on next push. Now both pinned (see the MSRV note under [Changed](#changed)).
- Various minor lints surfaced by enabling `--all-targets` in CI: `uninlined_format_args` in test code, `bool_assert_comparison` in a unit test, `manual_range_contains` across 8 integration-test sites, `if_same_then_else` in `rr-server::main`, `doc_overindented_list_items` in `rr-server::health`, `large_enum_variant` and `reversed_empty_ranges` annotated with `#[allow]` + rationale.
- Error sanitization (F2.5) plus full audit trail (F2.4): the network layer no longer surfaces internal state to attackers, and unauthorized rejects are now investigatable from the audit sink.
- **CI now actually runs the rocks persistence tests**:
  `--features rr-storage/rocks,integration/rocks` propagation fixed, and the
  three tests' bit-rot (never compiled in CI) repaired. The `chaos` job was
  renamed `external-broker` to match what it runs; the bench job now builds the
  rocks benches (`log_append_rocks`, `log_append_batched`, `log_tps`).
- **Dockerfile**: builder `rust:1.82` → `1.88` (below-MSRV image could not build
  the workspace at all); added `COPY tools ./tools` (missing workspace member
  broke the build); added `HEALTHCHECK` against `/ready` (curl installed in the
  runtime stage). `docker-compose.yml` gained healthchecks +
  `service_healthy` ordering; the insecure opt-in now lives only in compose env
  (`RR_ALLOW_INSECURE=true`, dev-only comment) instead of being buried in TOML.
- Grafana dashboard: removed the panel referencing the nonexistent
  `rr_auth_rejects_total` metric (implementing that metric is scheduled in the
  RC audit-pairing batch).
- **Docs version narrative unified to 0.2.0**: idempotency / kafka-input /
  metrics-reference / audit-log / durability / upgrading pages, the
  `client.proto` comments, and the `config_file.rs` rustdoc no longer
  describe shipped features as "v0.3.0 (in development)". The migration
  guide was renamed and rewritten as `v0.1 → v0.2` (sidebar + cross-links
  updated; docusaurus build verified).
- **`metrics-reference.md`**: dropped the stale "group_id hard-coded /
  Phase 3 will fix" known issue for `rr_snapshot_installs_total` (fixed by
  the adapter `group_id` parameter); removed implemented items from the
  Planned Metrics section.
- **`DurabilityPolicy` deprecation metadata aligned** with the compatibility
  policy: aliases are `#[deprecated(since = "0.2.0")]` and removed in
  v0.3.0 (was: `since = "0.3.0"` + "migrate before v0.4.0"). Code, CHANGELOG,
  compatibility.md (pre-1.0 semantics clarified), and docs all agree now.
- CI hygiene: fixed the pre-existing clippy failures in `examples/kv-store`,
  `examples/counter` (doc-overindented list items) and `tools/rr-lab-producer`
  (`manual_is_multiple_of`) so the workspace lint gate is green again.
- **Histograms now export as native Prometheus histograms**: the recorder in
  `rr-server/src/node.rs` sets explicit buckets (one shared latency set for
  all `*_seconds` metrics, one for `rr_replication_batch_entries`).
  Previously every histogram rendered as a Prometheus *summary* (`quantile`
  label), leaving all `histogram_quantile(... _bucket ...)` recording rules
  and dashboard panels without data.
- **6 处先前遗留的 rustfmt 漂移**(ingestor_runner / builtin_service / config_file / rocks /
  examples ×2 / rr-lab-producer / integration):`cargo fmt --all -- --check` 现全绿。
- **`rr-perf-bench` `Cluster::drop` 现在会回收子进程**:start_kill 后 best-effort `try_wait`
  轮询(每子进程 ~2s 预算,sync Drop 的合理近似),panic 路径不再泄漏 rr-server 进程;
  `port_base` 文档注明并行运行需传不同 `--port-base`。
- **ADR A-0015:异步 append 违反 openraft "submitted ⇒ visible" 契约(关键正确性)。**
  R-4 harness 首跑即发现(seed 4242424242,两个场景四个节点实例):`RocksLogStorage::append`
  入队即返回,而 openraft 的 apply/复制以 submit(非 flush)为门控 —— debug 构建下
  sm worker(`sm/worker.rs:211`)与复制流(`stream_state.rs:105`)断言杀死任务,
  节点 apply 冻结但 gRPC 假活;release 构建无断言,**静默跳过 apply**(副本状态机分叉)。
  修复:`RocksLogStorage` pending overlay(append 先 stage 再提交;读路径**先克隆区间
  overlay 快照、再扫 RocksDB、后合并** —— 快照序对"RocksDB 迭代器创建即钉视图 +
  post-flush hook 摘除"竞态完备);`IoBatcher` 新增 post-flush hook(错误路径同样摘除)
  与 `flush_barrier()`;`truncate_after`/`purge` 先过屏障再直连删除(消除被截断 entry
  复活的同根 race);Phase 3 聚合窗口闸门按 submission 计数修正。回归测试 4+4 个;
  终证:scenario A/B 复跑全绿(10 轮 SIGKILL,3266/3266 oracle,零 panic、零
  head-gap 告警)。
- **R-5 校准测试 `checker_perf_concurrent_burst` 的模型错误**:原构造让 64 个读全部
  晚于所有写返回(real-time 后继),其中 63 个观察值注定不可满足 —— 既判不合法(语义错),
  又触发指数穷举(性能陷阱)。修正为读/写区间交叠,恢复"广并发快检"本意。
- **`rr-client` dialed a fresh TCP/H2 connection per RPC** (`connect_to_addr` bypassed the pool): under concurrency ≥10 loopback ephemeral-port/backlog exhaustion failed ~98% of reads and destabilized leader heartbeats. Channels are now cached per address and evicted on `Code::Unavailable`; integration suite 49/49 green, 500-concurrency sweeps error-free.
- **Kafka ingestor poll starvation**: the poll `select!` raced `metrics_rx.changed()` — which fires on every replication-progress update — canceling in-flight polls forever once heartbeats began (deterministic stall, only visible with a real broker). The in-flight poll is no longer cancellable by metrics; leadership is re-checked between batch return and propose. Plus `BATCH_WRITE_TIMEOUT` (10s) around `client_write_many`, whose response stream can stall indefinitely across a leadership transition; a timeout now drops the batch step-down-style and re-seeds from the raft-log-authoritative offset.
- **Wedged H2 channels after black-holed links**: all three dial sites (rr-raft raft pool, rr-network pool, rr-client) now set HTTP/2 keepalive (10s/5s/idle) + TCP keepalive so dead connections are torn down and redialed instead of producing permanent heartbeat timeouts and post-partition split-brain liveness loss. (Raft safety held throughout — divergent entries were never committed.)
- **`EchoStateMachine` serves reads** via a new echoing `read_handle`, so the stock binary's `ReadLocal`/`ReadLinearizable` work; `submit_read` failures now log (sanitized) like the write path.
- **`v0.2.0-rc` doc/sample corrections**: quickstart rewritten around verified end-to-end paths; architecture/C4 docs corrected against the code (ShardRouter cache fabrication, ConfigProvider write methods, RPC inventories, snapshot persistence claims, deployment flags/metrics); new pages — positioning, openraft relationship, read consistency, FAQ, security, durability & recovery, ADR index.

### Removed

- **`rr_network::RrGrpcClient`** and **`rr_network::RrGrpcServer`** (along with the `client` and `server` modules in `rr-network`). These were Phase 2 scaffolding stubs that were never wired into the production startup path — `rr_server::node::RrNode::start()` always constructs services and calls `tonic::transport::Server::builder()` directly. The stubs had 0% production coverage and only appeared in documentation. **Breaking change** for any external code that imported them, though grep across the workspace found zero call sites; downstream impact is expected to be nil.

### Notes

- fuzz 端到端(`cargo fuzz run`)在作者环境因网络中断未能执行(cargo-fuzz 未安装、
  crates.io 不可达);6 个 target 经 libfuzzer-sys 同形 shim 编译并对全部 43 个种子
  逐字节回放验证通过。**首次有网的 nightly 运行是真正的端到端验收**。

## [0.2.0-rc2] — 2026-04-23

Pre-release of [0.2.0]. See the [0.2.0] section above for the full set
of changes; this entry is preserved so the `v0.2.0-rc2` git tag has a
corresponding marker in the changelog history. The rc2 tag was cut at
commit `dd5a058` and shipped Phase 3 Wave A through Wave E plus the
release candidate's first round of post-rc1 work.

## [0.1.0] — 2026-04 (Phase 2 baseline — retroactive)

Phase 2 established the core distributed framework capabilities. Not published to crates.io.

### Added
- **Core framework** (`rr-core`): `Service`, `StateMachine`, `StorageEngine`, `ConfigProvider` traits; `RrError` taxonomy; core types (`NodeId`, `GroupId`, `PartitionId`, `RoutingTable`).
- **Storage** (`rr-storage`): `MemStorage` (BTreeMap-backed) and `RocksStorage` (RocksDB-backed, feature-gated).
- **Configuration** (`rr-config`): `MemConfigProvider` with broadcast-based change notification.
- **Raft consensus** (`rr-raft`): `RaftGroupManager` (multi-group), `RaftNode` wrapping `openraft::Raft<RrTypeConfig>`, `RrLogStorage` and `RocksLogStorage`, `RrStateMachine` + `ServiceStateMachineAdapter` adapter stack, `GrpcNetworkFactory` for inter-node RPC.
- **Networking** (`rr-network`): `ClientService`, `RaftService`, `AdminService` proto definitions, `ChannelPool` for connection reuse. (Also shipped `RrGrpcServer` / `RrGrpcClient` scaffolding stubs that were never wired into production code; both have been removed in [0.2.0].)
- **Routing** (`rr-router`): `ShardRouter` for table-based partition → group → leader resolution.
- **Server orchestration** (`rr-server`): `RrNode` lifecycle (boot, config watch loop, graceful shutdown with timeouts); `ClientDispatchImpl`, `RaftGroupLookupImpl`; `NodeConfig`; metrics exporter (`metrics-exporter-prometheus`); gRPC health checks (`tonic-health`).
- **Client SDK** (`rr-client`): `RrClient` with auto leader-redirect, retry config, leader cache.
- **Admin CLI** (`rr-admin`): `rr-admin` binary with subcommands: `node status`, `group status`, `group snapshot`, `group transfer-leader`, `metrics`, `repl`.
- **Examples**: `examples/kv-store` (distributed KV), `examples/counter` (distributed counter).
- **Integration tests** (`tests/integration`): 24 tests with `ClusterBuilder` harness; real gRPC networking; fault injection framework (`FaultAwareChannelPool` with isolation, packet loss, latency).
- **Benchmarks** (`benches/`): `log_append`, `log_append_rocks`, `mem_storage_put_scan`, `codec_roundtrip` using criterion.
- **CI/CD** (`.github/workflows/ci.yml`): 13 jobs — build, build-rocks, test, test-rocks, test-defensive, clippy, fmt, coverage (→ Codecov), bench, msrv (1.82), doc-test, audit (cargo-audit), nightly, test-macos, integration, chaos.
- **Documentation** (Docusaurus site under `docs/`): getting started, service/state-machine traits, storage engines, config provider, client SDK, architecture overview, crate map, data flow, multi-Raft, networking, storage, deployment guide, API reference (config, proto, errors, metrics, admin-api, benchmarks).
- **Feature flags**: `rocks` (RocksDB storage + log storage), `defensive` (defensive mode for testing).

### Known Limitations (addressed in [0.2.0])
- `rr-server` binary was a stub (use `examples/kv-store` or `examples/counter`).
- No TLS on gRPC.
- No authentication on any RPC.
- No backup/restore tooling.
- No deployment artifacts (Dockerfile, K8s manifests, Helm chart).
- RocksDB fsync not explicitly configured.
- Only `MemConfigProvider` implementation (no etcd / file-backed alternatives).
- Chaos tests built but marked `#[ignore]`.
- No distributed tracing (W3C traceparent propagation).
- Not published to crates.io.

[Unreleased]: https://github.com/gardenias/rr/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/gardenias/rr/releases/tag/v0.2.0
[0.2.0-rc2]: https://github.com/gardenias/rr/releases/tag/v0.2.0-rc2
[0.1.0]: https://github.com/gardenias/rr/releases/tag/v0.1.0
