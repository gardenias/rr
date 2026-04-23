# Learnings — rr-phase3-prodready

## [2026-04-22] API / Library Facts (verified against actual source)

### New in Phase 3

1. **`rr_core::error::RaftError::InitializationFailed { reason: String }`** —
   new variant. Added so `RaftNode::new_*` can return typed errors instead
   of `.expect()`-panicking. `error_convert.rs` maps to
   `tonic::Status::failed_precondition`.

2. **`rr_storage::rocks::DurabilityPolicy`**:
   - `Strict` → `WriteOptions::set_sync(true)` + `Options::set_use_fsync(true)`
   - `Balanced` → neither (WAL still written, just not fsync'd)
   - `Fast` → `WriteOptions::disable_wal(true)` (tests only)
   - `RocksConfig` has a `durability` field (default: `Strict`).
   - `put/delete/write_batch` now use `*_cf_opt / write_opt` with the
     policy's `WriteOptions`.

3. **`rr_network::TlsConfig`** + `rr_raft::network_factory::TlsPaths` —
   two independent structs, **intentionally duplicated** to avoid circular
   crate dep. rr-raft can't depend on rr-network.

4. **`rr_network::auth`**:
   - `AuthConfig { admin_token, client_token }` — both `Option<String>`,
     both default `None`.
   - `BearerInterceptor` impls `tonic::service::Interceptor`, is `Clone`.
   - `attach_bearer_token(&mut request, token)` helper for client side.
   - `constant_time_eq` private helper prevents timing attacks.

5. **`rr_network::rate_limit`**:
   - `RateLimiter::new(rps, burst)` — **spawns tokio task** for refill;
     must be called from tokio runtime context.
   - `RateLimitInterceptor::disabled()` / `::with_limiter(RateLimiter)`.
   - `try_acquire()` calls `permit.forget()` so the Semaphore's permit
     count reflects remaining capacity.

6. **`rr_network::ChainedInterceptor<A, B>`** — simple 2-interceptor chain.
   Used by rr-server to stack auth (first, fail-fast) then rate-limit
   (second, only counts authed reqs). Both A and B must be `Interceptor + Clone`.

7. **`rr_network::tracing`** module:
   - `extract_traceparent(&request) -> Option<String>`
   - `extract_tracestate(&request) -> Option<String>`
   - `attach_traceparent(&mut request, str) -> Result<(), Status>`
   - Constants `TRACEPARENT_HEADER`, `TRACESTATE_HEADER`.

8. **`rr_server::health`**:
   - `ReadinessFlag` is `Arc<AtomicBool>`, `Clone`. Set by `RrNode::start()`
     at completion; cleared by `shutdown()`.
   - `spawn_http_server(addr, PrometheusHandle, ReadinessFlag, watch::Receiver<bool>)`
     returns `JoinHandle`. Shuts down on `changed()`.
   - Routes: `/health` (always 200), `/ready` (200 iff flag set, else 503),
     `/metrics` (text/plain Prometheus format).

9. **`rr_server::builtin_service::EchoStateMachine`** — no-op
   StateMachine for smoke tests. Apps fork `examples/*` for real logic.

10. **`rr_server::config_file::ResolvedConfig`** has 4 new sub-structs:
    `ResolvedTls`, `ResolvedAuth`, `ResolvedRateLimit`. Field
    `Option<T>` = disabled / default.

11. **`rr_admin::backup`**:
    - `create_backup(data_dir, output, node_id, comment) -> Result<BackupMetadata>`
    - `restore_backup(input, data_dir) -> Result<BackupMetadata>`
    - `read_metadata(input) -> Result<BackupMetadata>` — no full extract
    - `BACKUP_FORMAT_VERSION: u32 = 1`
    - Refuses: overwrite existing output, restore to non-empty data_dir,
      newer format_version.

12. **`rr_server::admin::AUDIT_TARGET: &str = "rr::audit"`** — tracing
    target for audit events. Route separately via EnvFilter.

### CLI flags added (Phase 3)

Under `rr-server serve`:
- `--tls-cert`, `--tls-key`, `--tls-ca`, `--tls-domain`,
  `--tls-require-client-cert` (+ env vars `RR_TLS_*`)
- `--admin-rps`, `--client-rps`, `--rate-limit-burst`
  (+ env vars `RR_ADMIN_RPS`, `RR_CLIENT_RPS`, `RR_RATE_LIMIT_BURST`)

Under `rr-admin`:
- `backup --data-dir PATH --output PATH [--node-id N] [--comment STR]`
- `restore --input PATH --data-dir PATH`
- `inspect-backup --input PATH`

### TOML config sections added (Phase 3)

```toml
[tls]
cert_path = "..."
key_path = "..."
ca_path = "..."                # optional
require_client_cert = false
domain_name = "..."            # optional

[auth]
admin_token_env = "RR_ADMIN_TOKEN"  # name of env var, not the token itself
client_token_env = "RR_CLIENT_TOKEN"

[rate_limit]
admin_rps = 50
client_rps = 1000
burst = 100
```

### Deployment / Release APIs

- **Dockerfile**: `rust:1.82-bookworm` → `debian:bookworm-slim`, UID 10001
  (`rr:rr`), `VOLUME /var/lib/rr/data`, ports 51001/52001/9100.
- **Helm values.yaml** key fields: `image.repository`, `replicaCount`,
  `storage.{backend,durability,dataSize}`, `auth.adminTokenSecret`,
  `tls.{enabled,existingSecret,requireClientCert}`.
- **Release workflow**: `.github/workflows/release.yml` triggers on
  `v*.*.*` tag. Docker job uses `docker/build-push-action@v5` with
  multi-arch buildx (linux/amd64 + linux/arm64). Helm job uses
  `azure/setup-helm@v4` + `helm push` with OCI.

## [2026-04-23] Coverage Measurement Learnings

- `cargo-llvm-cov` workflow: `clean → --no-report` (collect) →
  `report --html/--lcov/--json` (multi-format from single dataset).
- `--ignore-filename-regex` is powerful for excluding tests/examples/proto.
- `--branch` flag requires nightly and LLVM's branch coverage
  instrumentation; currently crashes LLVM on our codebase.
- Region coverage on stable is already finer than line coverage —
  each conditional arm is its own region.
- For Rust, function-coverage % is useful but misleading: monomorphic
  generic fns count per-instantiation, so a widely-used `.unwrap()`
  helper can show 100% even if only one call site hit it.

## [2026-04-22~23] Library / Tooling Gotchas

- **rustls 0.23 requires explicit `CryptoProvider::install_default()`**.
  Calling TLS before installing one panics: "Could not automatically
  determine the process-level CryptoProvider". rr-server calls
  `rustls::crypto::ring::default_provider().install_default()` early in
  `main()`. Anyone embedding rr as a library must do the same.
- **tonic 0.14's `with_interceptor` requires `Interceptor: Clone`**.
  Our interceptors derive Clone via `#[derive(Clone)]` on the struct.
  `ChainedInterceptor<A, B>` requires `A: Clone, B: Clone`.
- **`InterceptedService::new(InterceptedService::new(svc, A), B)` order**:
  outer B runs FIRST (B wraps an already-intercepted svc). So
  `ChainedInterceptor { first: auth, second: rate_limit }` means
  auth.call() runs first, then if Ok, rate_limit.call() runs. The
  "first"/"second" naming matches call order, not wrapper depth.
- **tonic `AsciiMetadataValue::parse`** is lenient — many unicode-ish
  strings parse OK. Use `\n\r` (strict CR/LF) or null bytes to test
  rejection paths.
- **`cargo-llvm-cov` installs slowly (~45s)** but is worth it. Don't
  confuse with `cargo-tarpaulin` (older, Linux-only tool).
- **Proptests are 200× slower under coverage instrumentation**. Pass
  `--skip proptest` to the test harness during coverage runs.
- **Backup tarball handling**: `tar::Builder` appends metadata header
  with `set_path / set_size / set_mode / set_mtime / set_cksum`. Skipping
  `set_cksum` produces silently corrupt tars that some tar readers
  accept and others reject.
- **hyper 1.x** moved from `hyper::server::conn::Http` to
  `hyper::server::conn::http1::Builder::new().serve_connection(io, service)`.
  Takes `TokioIo::new(stream)` to adapt a `TcpStream` to hyper's traits.
- **`--deny=warnings` in CI bit us** during the rustls crypto-provider
  saga — test binary compiled fine but runtime panic'd. No compiler
  warning caught it.

## [2026-04-22~23] Project Structure / Convention

- **Module-level `//!` docs serve double duty**:
  - Developer documentation for contributors
  - Ops-facing safety notes (e.g., "RaftService never rate-limited" in
    `rate_limit.rs`)
- **Clap's `///` on CLI struct fields becomes `--help` output** — NEVER
  treat these as pure dev docs.
- **TOML config comments also serve as safety notes** (e.g., the
  commented-out `[tls]` section in `examples/config/single-node.toml`
  includes warnings about when to enable TLS).
- **Per-phase notepads in `.sisyphus/notepads/<phase>/`** follow
  `{decisions, learnings, problems, issues}.md` structure. Each phase
  gets its own subdir.
