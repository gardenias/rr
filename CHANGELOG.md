# Changelog

All notable changes to the rr distributed multi-Raft consensus framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
