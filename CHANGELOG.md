# Changelog

All notable changes to the rr distributed multi-Raft consensus framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- `workspace.package.rust-version` bumped from `1.82` to `1.88`. This reflects the actual minimum needed to compile the transitive dependency graph (`clap_lex ≥ 1.1.0` requires `edition2024` stable in 1.85; `validit 0.2.5` uses `let_chains` stable in 1.88). Added `rust-toolchain.toml` pinning `channel = "1.88"` so contributors get the right toolchain automatically.

### Added
- `docs/docs/guide/consumer-guide.md` — bilingual guide for consuming rr from downstream business projects (git+tag, path, crates.io), with full `Cargo.toml` templates, the private-repo SSH setup, and `rust-version` pitfalls.

## [0.2.0-rc2] — 2026-04-23

Release candidate 2 — see tag `v0.2.0-rc2` for the exact commit. All items below are Phase 3 continuation work landed after `v0.2.0-rc1`.

### Added

- **HTTP observability endpoints** (`rr-server::health`): single hyper server on `metrics_port` serves `GET /health` (liveness), `GET /ready` (readiness — flipped at end of `start()`, cleared on `shutdown()`), and `GET /metrics` (Prometheus). K8s manifests + Helm chart updated to use HTTP probes instead of TCP-on-gRPC-port.
- **Semver / API stability document** (`docs/docs/reference/compatibility.md`): explicit commitments per surface (Rust API, gRPC wire, on-disk, config, CLI, metrics), pre-1.0 Cargo convention, MSRV policy, deprecation process.
- **Five operational runbooks** under `docs/docs/operations/runbooks/`: disk-full, leader-flapping, network-partition, snapshot-corruption, configprovider-unreachable. Each structured as Symptom → Detection → Immediate mitigation → Root cause → Recovery → Prevention.
- **`rr-client` side metrics**: `rr_client_request_duration_seconds` (histogram, `outcome=success|failure`), `rr_client_retries_total` (counter), `rr_client_leader_cache_hits_total` / `_misses_total` (counters). All labeled with `group_id`.
- **Audit log** via dedicated tracing target `rr::audit`: every AdminService RPC emits a structured info-level event with `op`, `peer` (socket addr), `node_id`, and per-operation fields. Mutation RPCs emit both dispatch and result events for forensic pairing. `docs/docs/deployment/audit-log.md` documents schema + routing patterns.
- **W3C traceparent propagation** (`rr-network::tracing`): `extract_traceparent` / `extract_tracestate` / `attach_traceparent` / `attach_tracestate` helpers. Inbound RPC handlers log the incoming traceparent on the tracing span for cross-service correlation. Does NOT bundle the OpenTelemetry SDK — users bring their own OTLP stack.
- `#[instrument]` on client-dispatch, raft-group-lookup, and `RrClient::{send_request, send_request_with_partition, get_cluster_info}` for complete span coverage on hot paths.
- **Per-service rate limiting** (`rr-network::rate_limit`): token-bucket limiter built on `tokio::sync::Semaphore` (zero new deps). `RateLimitConfig { admin_rps, client_rps, burst }`, `RateLimitInterceptor`, `RateLimiter`, `ChainedInterceptor`. Admin and Client services can be rate-limited; **RaftService intentionally never is** (throttling Raft destabilizes consensus). On exhaustion returns gRPC `RESOURCE_EXHAUSTED`. New CLI flags `--admin-rps`, `--client-rps`, `--rate-limit-burst` + `RR_ADMIN_RPS`, `RR_CLIENT_RPS`, `RR_RATE_LIMIT_BURST` env vars + `[rate_limit]` TOML section. Interceptor order: auth first (cheap fail-fast) then rate-limit (only counts authenticated requests).

### Added
- `LICENSE` file (MIT) at repository root.
- `CHANGELOG.md` following Keep a Changelog format.
- `CONTRIBUTING.md` with development setup, testing, and PR guidelines.
- `CODE_OF_CONDUCT.md` (Contributor Covenant v2.1).
- `.github/pull_request_template.md`.
- `docs/docs/architecture/c4/` — Complete C4 architecture suite (Context, Container, Component, Deployment, Dynamic diagrams).
- `DurabilityPolicy` enum in `rr-storage::rocks::RocksConfig` with `Strict` (fsync + WAL sync, default), `Balanced`, and `Fast` options.
- Complete metrics documentation for all 14 emitted metrics (previously 6 documented).
- `.sisyphus/plans/rr-phase3-prodready.md` — Phase 3 production-readiness roadmap.
- **Production `rr-server` binary**: `crates/rr-server/src/main.rs` replaces the 4-line stub with a full CLI (`serve` / `version` subcommands) + TOML config + env var overrides + SIGTERM/SIGINT graceful shutdown.
- TOML config file loader (`rr-server::config_file`) with sections `[node]`, `[storage]`, `[cluster]`, `[tls]`, `[auth]`; strict schema (`deny_unknown_fields`) and layered precedence (CLI > env > file > defaults).
- Built-in `EchoStateMachine` (`rr-server::builtin_service`) for smoke testing.
- Example configs: `examples/config/{single-node,3node-cluster-node{1,2,3}}.toml`.
- **TLS/mTLS support** (`rr-network::tls::TlsConfig`): gRPC over rustls, opt-in per node. `rr_raft::network_factory::TlsPaths` mirrors this on the outbound side; `ChannelPool::with_tls` dials over `https://` with client certs. New CLI flags `--tls-cert/--tls-key/--tls-ca/--tls-domain/--tls-require-client-cert` + `[tls]` config section.
- `scripts/gen-dev-certs.sh` — Self-signed CA + per-node certs generator for local dev.
- `docs/docs/deployment/tls.md` — TLS configuration + troubleshooting guide (bilingual).
- **Bearer-token auth** (`rr-network::auth`): `AuthConfig` + `BearerInterceptor` + `attach_bearer_token` helper. AdminService always separate token from ClientService; RaftService authenticated via mTLS only. Constant-time token comparison. Tokens loaded from env vars referenced by name in `[auth]` config (never stored in the config file itself).
- Integration-test bidirectional fault isolation: `FaultAwareChannelPool` + per-node pool wiring in `ClusterBuilder::with_fault_injector`. All 4 chaos tests (`test_leader_isolation_and_recovery`, `test_follower_isolation_and_catchup`, `test_packet_loss_convergence`, `test_latency_under_high_load`) now pass in the default CI test job (no longer `#[ignore]`d).
- `RaftError::InitializationFailed { reason }` variant in `rr-core::error`.
- **Offline backup / restore** in `rr-admin`: new `backup` / `restore` / `inspect-backup` subcommands producing gzip-compressed tar archives containing a `metadata.json` descriptor + the node's data_dir. Format versioned (`format_version = 1`); restore refuses newer majors. 6 unit tests + e2e roundtrip verified.
- **Deployment artifacts**: multi-stage `Dockerfile` (Debian slim, non-root `rr:rr` at UID 10001), `.dockerignore`, `docker-compose.yml` for local 3-node cluster, raw K8s manifests (`deploy/k8s/statefulset.yaml`) with StatefulSet + headless Service + client Service + PodDisruptionBudget, and a full Helm chart at `deploy/helm/rr/` with TLS + auth + storage knobs in `values.yaml`.
- `.github/workflows/release.yml`: publishes Docker image to `ghcr.io/gardenias/rr` and Helm chart (OCI) to `ghcr.io/gardenias/charts/rr` on tag push. Optional crates.io publish via `workflow_dispatch`.
- `docs/docs/deployment/kubernetes.md` — K8s deployment guide (raw manifests + Helm, with cert-manager TLS example).

### Changed
- `RocksConfig::default()` now defaults to `DurabilityPolicy::Strict` (fsync + WAL sync). This fixes a critical correctness issue where Raft durability guarantees were effectively broken.
- `RocksConfig::for_testing()` uses `DurabilityPolicy::Fast` to keep test suites fast.
- `README.md` architecture diagram upgraded from generic Mermaid graph to proper C4 Container diagram with technology labels.
- **`RaftNode::new_with_factory` / `::new_single_node` / `::new_with_rocks_factory` now return `Result<Self, RrError>`** instead of panicking on config validation or RocksDB open failure. Breaking change for direct callers (all 27 internal call sites updated); library users who propagate the error via `?` need no change. This removes a class of production crashes where a node would panic on startup instead of surfacing a clean error.
- `RocksStorage::put` / `delete` / `write_batch` now use `*_cf_opt` / `write_opt` variants with `WriteOptions` driven by `DurabilityPolicy`.

### Fixed
- RocksDB was not configured with fsync, risking data loss on power failure. This violated Raft's durability contract. Fixed by introducing explicit `DurabilityPolicy` with safe defaults.
- `FaultAwareChannelPool` isolation was one-directional only (blocked inbound but not outbound), which made `test_leader_isolation_and_recovery` unable to trigger leader re-election and forced 4 chaos tests to live behind `#[ignore]`. Fixed by refactoring to per-node pools sharing a common `FaultState`; isolation is now bidirectional and all chaos tests run in CI.

## [0.1.0] — 2026-04 (Phase 2 baseline — retroactive)

Phase 2 established the core distributed framework capabilities. Not published to crates.io.

### Added
- **Core framework** (`rr-core`): `Service`, `StateMachine`, `StorageEngine`, `ConfigProvider` traits; `RrError` taxonomy; core types (`NodeId`, `GroupId`, `PartitionId`, `RoutingTable`).
- **Storage** (`rr-storage`): `MemStorage` (BTreeMap-backed) and `RocksStorage` (RocksDB-backed, feature-gated).
- **Configuration** (`rr-config`): `MemConfigProvider` with broadcast-based change notification.
- **Raft consensus** (`rr-raft`): `RaftGroupManager` (multi-group), `RaftNode` wrapping `openraft::Raft<RrTypeConfig>`, `RrLogStorage` and `RocksLogStorage`, `RrStateMachine` + `ServiceStateMachineAdapter` adapter stack, `GrpcNetworkFactory` for inter-node RPC.
- **Networking** (`rr-network`): `RrGrpcServer` + `RrGrpcClient`, `ClientService`, `RaftService`, `AdminService` proto definitions, `ChannelPool` for connection reuse.
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

### Known Limitations (addressed in Phase 3)
- `rr-server` binary is a stub (use `examples/kv-store` or `examples/counter`).
- No TLS on gRPC.
- No authentication on any RPC.
- No backup/restore tooling.
- No deployment artifacts (Dockerfile, K8s manifests, Helm chart).
- RocksDB fsync not explicitly configured (fixed in Unreleased).
- Only `MemConfigProvider` implementation (no etcd / file-backed alternatives).
- Chaos tests built but marked `#[ignore]`.
- No distributed tracing (W3C traceparent propagation).
- Not published to crates.io.

[Unreleased]: https://github.com/gardenias/rr/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/gardenias/rr/releases/tag/v0.1.0
