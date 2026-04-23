# Decisions — rr-phase3-prodready

## [2026-04-22] Architecture Decisions Made This Phase

### Security

- **TLS implementation**: rustls via tonic's `tls-ring` feature (not `tls-aws-lc`).
  Reason: broader platform support, no clang native dep at build time.
- **mTLS is OPTIONAL**: `require_client_cert = false` by default. Enabling
  requires `ca_path` to be set (validated at build time).
- **TLS cert/key on both server and client**: symmetric mTLS. Each node uses
  the same pair for inbound server identity and outbound client identity.
- **Authentication scheme**: Bearer token (shared secret) over gRPC metadata.
  NOT JWT, NOT full OAuth — those are too heavy for v0.2.0.
- **Two independent tokens**: `admin_token` (AdminService) and `client_token`
  (ClientService, optional). RaftService uses mTLS only (no bearer token).
- **Tokens come from env vars**, referenced BY NAME in `[auth]` config. Raw
  tokens NEVER in the config file. This prevents accidental commits of
  secrets.
- **Constant-time token compare** (subtle — see `rr-network::auth::constant_time_eq`)
  to mitigate timing attacks (RFC 8628 §7.1).
- **Interceptor order**: auth → rate-limit. Invalid tokens don't consume
  rate-limit quota (so attackers with garbage tokens can't exhaust the bucket).

### Rate Limiting

- **Custom token bucket** on `tokio::sync::Semaphore` + background refill task.
  NOT `tower-governor`, NOT `tower::limit::RateLimit`. Reasons:
  (a) zero new deps;
  (b) `tower::limit::RateLimit` applies globally to a `Server::builder()`,
      breaking the per-service invariant;
  (c) governor adds a transitive dep tree we don't need.
- **RaftService is STRUCTURALLY not rate-limitable** — there is NO code path
  in `rr-server::node` that attaches a rate-limit interceptor to Raft. This
  is a safety invariant: throttling Raft heartbeats causes elections.
- **Exhaustion → `Status::resource_exhausted`**, NOT drop the connection
  or return 429 (gRPC has no 429; code 8 is the semantic equivalent).

### RocksDB / Storage

- **Default `DurabilityPolicy::Strict`** = `set_sync(true)` on WAL.
  This is a CORRECTNESS fix — without it Raft's durability contract was broken.
- `Balanced` policy exists (WAL written but not fsync'd) for operators who
  explicitly trade durability for throughput. Documented as unsafe without
  replication + backups.
- `Fast` policy = `disable_wal(true)`, for tests only. `RocksConfig::for_testing()`
  uses Fast.

### Backup / Restore

- **Offline only for v0.2.0**: node must be stopped before `rr-admin backup`.
  Online (via new snapshot-fetch RPC) is explicitly deferred to Phase 3.1
  to avoid protocol changes in the first release.
- **Format: `.tar.gz` containing `metadata.json` + `data/`** (node's data_dir
  verbatim). Simple, portable, no library lock-in.
- **`format_version = 1`**; restore REFUSES newer majors. This gives us
  room to evolve the format without breaking operators.
- **Restore refuses to clobber non-empty data_dir**. Forces operator to
  explicitly clean before restore.

### Observability

- **HTTP /health /ready /metrics on ONE port** (`metrics_port`), not three
  separate ports. Reduces firewall complexity for K8s probes.
- **`/ready` driven by `ReadinessFlag`** (Arc<AtomicBool>): set at end of
  `start()`, cleared at top of `shutdown()`. Accurate signal for "cluster
  ready to accept work", not just "TCP port open".
- **OpenTelemetry: BYO SDK**. rr-network provides W3C traceparent header
  extraction/injection only. Full OTLP/gRPC exporter (~12MB of deps) is
  NOT bundled. Users install `tracing-opentelemetry` + their own exporter.
- **Audit log via dedicated tracing target `rr::audit`**, not a separate
  sink. Operators route via `EnvFilter`. Reuses the existing `tracing`
  infrastructure — no new deps, no new sink to configure.

### rr-server Binary

- **Built-in EchoStateMachine** for smoke testing. Real apps fork
  `examples/kv-store` or `counter` rather than customizing rr-server.
  Documented in module-level docstring to prevent misuse in production.
- **CLI parser: clap derive**. TOML loader: `toml` + `serde(deny_unknown_fields)`.
  Merge precedence: CLI > env > file > defaults.
- **Signal handling**: SIGTERM/SIGINT both → graceful shutdown. On non-Unix,
  only Ctrl-C (tokio's only cross-platform signal primitive).

### Deployment

- **Docker images → `ghcr.io/gardenias/rr`** (GitHub Container Registry).
  Not Docker Hub. Reason: integrates with `GITHUB_TOKEN` out of the box,
  no separate account needed.
- **Helm chart → OCI at `ghcr.io/gardenias/charts/rr`**. Not Artifact Hub,
  not a git-based chart repo. OCI is the modern standard.
- **Multi-stage Dockerfile**: `rust:1.82-bookworm` build → `debian:bookworm-slim`
  runtime. Non-root UID 10001. `VOLUME /var/lib/rr/data`.
- **K8s: StatefulSet with `OrderedReady` policy**, init container renders
  config.toml from template based on pod ordinal (rr-0 → bootstrap=true,
  others → peers=[rr-0.rr, rr-1.rr, ...]).
- **K8s probes: HTTP on metrics port**, not TCP on gRPC. TCP doesn't
  distinguish "port open" from "ready to accept work".

### Testing

- **Chaos tests: removed `#[ignore]`, run in default CI test job**
  (was a dedicated chaos job running `--ignored`). After fixing
  bidirectional isolation, they pass in ~3.5s total.
- **Fault isolation must be bidirectional**: per-node `FaultAwareChannelPool`
  sharing a common `Arc<FaultState>`. `isolate(N)` blocks both "outbound
  from N" (via owner check on N's pool) and "inbound to N" (via destination
  check on every pool).
- **Integration tests use `OrderedReady` cluster bootstrap**, bootstrap node
  is node_id=1 always. Derived from pod ordinal in K8s.

### Release Engineering

- **Semver policy: Cargo convention pre-1.0** — `0.x.y → 0.(x+1).0` is
  breaking; patch never breaks. Documented in `compatibility.md`.
- **MSRV bumps are MINOR**, not patch. Target: `stable - 6 months`.
- **Deprecation cycle**: announce in one MINOR, remove next MAJOR. Minimum
  one full MINOR grace period.
- **crates.io publish: workflow_dispatch only**. First publish is gated by
  explicit operator action (requires `CARGO_REGISTRY_TOKEN` secret).

### Code Style / Quality (confirmed / reinforced from Phase 2)

- **Docstrings that become `--help` output are NOT optional** (clap `///`).
  They're user-facing UX.
- **Safety-critical docstrings are also NOT optional** — e.g.,
  `DurabilityPolicy::Strict` must document the correctness implication;
  `AuthConfig::admin_token` must document that unset = open admin.
- **`tonic::service::Interceptor` must be `Clone`** to use with
  `ServiceServer::with_interceptor`. Our AuthConfig/RateLimitConfig/
  BearerInterceptor all derive Clone.

## [2026-04-23] Coverage-Driven Cleanup Decisions

- **`cargo-llvm-cov --branch` is unusable on this codebase** (LLVM SIGSEGV
  on nightly). Stable's region coverage is the substitute — regions are
  finer than lines and include conditional arms.
- **Vestigial stubs should be deleted, not tested**:
  `rr-network/src/{client,server}.rs` are 0% covered AND unused in
  production (rr-client dials channels directly; rr-server uses tonic's
  `Server::builder()` directly). Slated for removal.
- **CI's `coverage` job misses RocksDB paths** — should pass
  `--features rr-storage/rocks`. Known gap; fix is one line.
