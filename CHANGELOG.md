# Changelog

All notable changes to the rr distributed multi-Raft consensus framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Changed

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
- **`formal/tla/` TLA+ 形式化(R-2)**:rr 自有扩展协议的 4 份 spec + TLC 小模型实测:
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
