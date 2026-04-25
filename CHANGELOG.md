# Changelog

All notable changes to the rr distributed multi-Raft consensus framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Post-`v0.2.0` security hardening + Phase 4 Tier 0 docs patches. No release tag yet.

### Added

#### Security
- **Production safety validation in `RrNode::new`**: refuses to start when any of five gates is violated — RaftService without TLS, RaftService without mTLS, bearer-token auth without TLS, AdminService without TLS, or AdminService without `admin_token`. All violations are aggregated into a single error so operators fix the config in one pass. Closes F2.1 + F2.2 + F2.4 from the F1-F4 review.
- **`NodeConfig::allow_insecure` opt-in escape hatch** (CLI `--allow-insecure`, env `RR_ALLOW_INSECURE`, TOML `[node].allow_insecure`) for dev / examples / CI. When enabled, the node emits a structured `INSECURE MODE ACTIVE` warning at startup and sets the Prometheus gauge `rr_security_insecure_mode = 1` so monitoring can alert on accidental production deployment.
- **Audit log** now covers `BearerInterceptor` and `RateLimitInterceptor` rejections: per-reject events on `rr::audit` with `op="auth"` / `op="rate_limit"`, `service` (admin/client), `peer`, and `reason`. `TransferLeader` now emits paired before/after events matching `TriggerSnapshot`. New `rr_network::audit` module with `ServiceLabel` enum and `AUDIT_TARGET` constant. Schema documented in `docs/deployment/audit-log.md`.
- **`InstallSnapshot` byte cap**: server-side enforces a 4 GiB ceiling on the total streamed bytes (`MAX_INSTALL_SNAPSHOT_BYTES`) to block memory-exhaustion DoS from authenticated peers. Closes F2.6.
- **`rr-admin` upgrading guide** (`docs/deployment/upgrading.md`) walks operators through the v0.2.x → strict three-phase rolling upgrade.

#### Documentation
- **`Service` and `StateMachine` trait safety invariants** (T0.1 of Phase 4 Tier 0): three rustdoc invariants documenting OpenRaft's concurrency contract — `snapshot()` atomic capture, `apply()` synchronous mutation, `restore()` complete replacement. Worked anti-pattern + correct-pattern examples. Cross-references to ADR A-0002 with OpenRaft source citations. Same content surfaces in `docs/guide/service-trait.md` and `docs/guide/state-machine-trait.md`.
- **Storage durability policy doc** (`docs/guide/storage-engines.md`): comparison table of all three policies, the v0.3.0 rename note, and the runtime behaviour (startup `tracing::warn!` when policy is non-Strict).

### Changed

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
  - Old names remain as `#[deprecated(since = "0.3.0")]` aliases for one minor version. TOML / CLI also accept old strings (`"balanced"` / `"fast"`) but emit a deprecation warning at startup. Migrate before v0.4.0.
- `RocksStorage::open` now emits a `tracing::warn!` at startup when the policy is not `Strict` (violates OpenRaft's durability contract per ADR A-0002).

### Fixed

- Error sanitization (F2.5) plus full audit trail (F2.4): the network layer no longer surfaces internal state to attackers, and unauthorized rejects are now investigatable from the audit sink.

## [0.2.0] — _TBD on tag cut_

First production-ready release. Rolls up everything from `v0.2.0-rc1` and
`v0.2.0-rc2` plus the post-rc2 cleanup that completed Phase 3. See
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

#### Deployability
- **Production `rr-server` binary** (`crates/rr-server/src/main.rs`): replaces the v0.1.0 4-line stub with a full clap CLI (`serve` / `version` subcommands), TOML config loader, env var overrides, SIGTERM/SIGINT graceful shutdown, and a built-in `EchoStateMachine` for smoke testing.
- **TOML config loader** (`rr-server::config_file`): sections `[node]`, `[storage]`, `[cluster]`, `[tls]`, `[auth]`, `[rate_limit]`. Strict schema (`deny_unknown_fields`) and layered precedence (CLI > env > file > defaults).
- **Three example configs** in `examples/config/`: `single-node.toml`, `3node-cluster-node{1,2,3}.toml`.
- **Multi-stage Dockerfile** (Debian slim, non-root `rr:rr` at UID 10001), `.dockerignore`, `docker-compose.yml` for a local 3-node cluster.
- **K8s deployment artefacts**: raw manifests at `deploy/k8s/statefulset.yaml` (StatefulSet + headless Service + client Service + PodDisruptionBudget), full Helm chart at `deploy/helm/rr/` with TLS / auth / storage knobs in `values.yaml`.
- **`.github/workflows/release.yml`**: publishes the Docker image to `ghcr.io/gardenias/rr` and the Helm chart (OCI) to `ghcr.io/gardenias/charts/rr` on every tag push, plus an optional crates.io publish step gated on `workflow_dispatch`.

#### Operations
- **Offline backup / restore** in `rr-admin`: new `backup` / `restore` / `inspect-backup` subcommands producing gzip-compressed tar archives containing a `metadata.json` descriptor + the node's `data_dir`. Format versioned (`format_version = 1`); restore refuses newer majors. 6 unit tests + e2e roundtrip verified.
- **Five operational runbooks** under `docs/operations/runbooks/`: `disk-full`, `leader-flapping`, `network-partition`, `snapshot-corruption`, `configprovider-unreachable`. Each structured as Symptom → Detection → Immediate mitigation → Root cause → Recovery → Prevention.

#### Observability
- **HTTP observability endpoints** (`rr-server::health`): single hyper server on `metrics_port` serving `GET /health` (liveness, always 200), `GET /ready` (200 once `start()` completes, 503 otherwise — driven by a shared `ReadinessFlag`), and `GET /metrics` (Prometheus). K8s manifests + Helm chart updated to use HTTP probes instead of TCP-on-gRPC-port.
- **W3C Trace Context propagation** (`rr-network::tracing`): `extract_traceparent`, `extract_tracestate`, `attach_traceparent`, `attach_tracestate` helpers. Inbound RPC handlers log the incoming traceparent on the tracing span for cross-service correlation. Does **not** bundle the OpenTelemetry SDK — users bring their own OTLP stack.
- **`rr-client` side metrics**: `rr_client_request_duration_seconds` (histogram, `outcome=success|failure`), `rr_client_retries_total` (counter), `rr_client_leader_cache_hits_total` / `_misses_total` (counters). All labelled with `group_id`.
- `#[instrument]` on `ClientDispatchImpl`, `RaftGroupLookupImpl`, and `RrClient::{send_request, send_request_with_partition, get_cluster_info}` for full span coverage on the hot paths.
- Complete metrics documentation for all 14 server-side + 4 client-side metrics (previously only 6 documented). See [`docs/reference/metrics-reference.md`](./docs/docs/reference/metrics-reference.md).

#### Reliability
- `DurabilityPolicy` enum in `rr-storage::rocks::RocksConfig` with `Strict` (fsync + WAL sync, **default**), `Balanced` (per-batch fsync), and `Fast` (no fsync — testing only) options.
- `RaftError::InitializationFailed { reason }` variant in `rr-core::error`.
- **Bidirectional fault isolation** for chaos tests: `FaultAwareChannelPool` + per-node pool wiring in `ClusterBuilder::with_fault_injector`. All 4 chaos tests (`test_leader_isolation_and_recovery`, `test_follower_isolation_and_catchup`, `test_packet_loss_convergence`, `test_latency_under_high_load`) now run in the default CI test job (no longer `#[ignore]`d).
- **Integration test for HTTP `/health`, `/ready`, `/metrics`** lifecycle (`tests/integration::test_health_endpoints_full_lifecycle`): drives all four routes via real reqwest HTTP/1 and asserts on status codes, response bodies, and Prometheus content-type. Lifts `rr-server::health` line coverage from ~24% baseline toward ~80%.

#### Documentation
- **C4 architecture diagram suite** under `docs/architecture/c4/` — 13 C4-PlantUML diagrams (Context, Container, four Component levels, Deployment, five Dynamic flows).
- **Semver / API stability document** ([`docs/reference/compatibility.md`](./docs/docs/reference/compatibility.md)): explicit commitments per surface (Rust API, gRPC wire, on-disk, config, CLI, metrics), pre-1.0 Cargo convention, MSRV policy, deprecation process.
- **Bilingual consumer guide** ([`docs/guide/consumer-guide.md`](./docs/docs/guide/consumer-guide.md)) for downstream business projects: covers all three consumption modes (`git+tag`, `path`, `crates.io`) with full `Cargo.toml` templates, private-repo SSH setup, `net.git-fetch-with-cli` configuration for CI runners, and MSRV pitfalls.
- TLS configuration + troubleshooting guide ([`docs/deployment/tls.md`](./docs/docs/deployment/tls.md)).
- K8s deployment guide ([`docs/deployment/kubernetes.md`](./docs/docs/deployment/kubernetes.md)) with raw manifests + Helm + cert-manager TLS example.

#### Project meta
- `LICENSE` (MIT) at repository root.
- `CHANGELOG.md` following Keep a Changelog format.
- `CONTRIBUTING.md` with development setup, testing, and PR guidelines.
- `CODE_OF_CONDUCT.md` (Contributor Covenant v2.1).
- `.github/pull_request_template.md`.

### Changed

- **`RocksConfig::default()` now uses `DurabilityPolicy::Strict`** (fsync + WAL sync). This fixes a critical correctness issue where Raft durability guarantees were effectively broken — see [Fixed](#fixed) below. Roughly 10× slower on writes than the prior implicit "Fast" behaviour, but actually durable.
- `RocksConfig::for_testing()` uses `DurabilityPolicy::Fast` to keep test suites fast.
- `RocksStorage::put` / `delete` / `write_batch` now use `*_cf_opt` / `write_opt` variants with `WriteOptions` driven by the chosen `DurabilityPolicy`.
- **`RaftNode::new_with_factory` / `::new_single_node` / `::new_with_rocks_factory`** now return `Result<Self, RrError>` instead of panicking on config validation or RocksDB open failure. **Breaking change** for direct callers (all 27 internal call sites updated). Library users propagating via `?` need no change. Removes a class of production crashes where a node panicked on startup instead of surfacing a clean error.
- **MSRV bumped from 1.82 to 1.88**. Required by transitive dependencies: `clap_lex ≥ 1.1.0` needs `edition2024` stable in 1.85; `validit 0.2.5` uses `let_chains` stable in 1.88. A pinned `rust-toolchain.toml` is included so contributors auto-install the right toolchain on first invocation.
- `README.md` architecture diagram upgraded from a generic Mermaid graph to a proper C4 Container diagram with technology labels.
- All Mermaid diagrams in the docs site converted to C4-PlantUML for richer rendering. `@docusaurus/theme-mermaid` removed.
- Architecture documentation corrected to describe the actual **dual-server gRPC topology**: `RaftService` + `ClientService` + `Health` share `listen_addr`; `AdminService` runs on a separate `tonic::Server` bound to `admin_port` so admin traffic cannot starve consensus bandwidth. Replaces the prior inaccurate "three services on one tonic Server" claim that referenced a stub type that never shipped.
- CI: `clippy` job now runs with `--all-targets` so test code, benches, and binary main paths are also linted. Coverage job now includes `--features rr-storage/rocks` so RocksDB code paths show up in Codecov.

### Fixed

- **CRITICAL — RocksDB was not configured with fsync**, risking data loss on power failure. This violated Raft's durability contract: a power loss could lose committed log entries. Fixed by introducing explicit `DurabilityPolicy` with `Strict` as the default. **All v0.1.0 deployments should upgrade.**
- **`FaultAwareChannelPool` isolation was one-directional only** (blocked inbound but not outbound), which made `test_leader_isolation_and_recovery` unable to trigger leader re-election and forced 4 chaos tests to live behind `#[ignore]`. Fixed by refactoring to per-node pools sharing a common `FaultState`; isolation is now bidirectional and all chaos tests run in CI.
- CI `msrv` job toolchain pin (1.82) was inconsistent with `workspace.package.rust-version` (1.88) — would have failed `cargo check` on next push. Now both pinned to 1.88.
- Various minor lints surfaced by enabling `--all-targets` in CI: `uninlined_format_args` in test code, `bool_assert_comparison` in a unit test, `manual_range_contains` across 8 integration-test sites, `if_same_then_else` in `rr-server::main`, `doc_overindented_list_items` in `rr-server::health`, `large_enum_variant` and `reversed_empty_ranges` annotated with `#[allow]` + rationale.

### Removed

- **`rr_network::RrGrpcClient`** and **`rr_network::RrGrpcServer`** (along with the `client` and `server` modules in `rr-network`). These were Phase 2 scaffolding stubs that were never wired into the production startup path — `rr_server::node::RrNode::start()` always constructs services and calls `tonic::transport::Server::builder()` directly. The stubs had 0% production coverage and only appeared in documentation. **Breaking change** for any external code that imported them, though grep across the workspace found zero call sites; downstream impact is expected to be nil.

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
