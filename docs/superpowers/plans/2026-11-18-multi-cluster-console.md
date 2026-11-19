# Multi-cluster Rr Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Turn `rr-admin console` into an independently started client that persists, connects, observes and switches among multiple rr clusters without sharing runtime state.

**Architecture:** Keep the same-origin Rust/Axum BFF and Next.js static export. Add persistent connection profiles and a `ConnectionManager` whose managed entries each own an optional single-cluster runtime; scope every cluster resource and SSE stream by connection ID, and make the browser URL authoritative for the selected connection.

**Tech Stack:** Rust 2021, Tokio, Axum 0.8, tonic, serde/serde_json, Next.js 16 static export, React 19, TypeScript strict, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-18-multi-cluster-console-design.md`

**Status:** Tasks 1–8 are independently reviewed and complete. Task 8 formal/specialist review fixes and local gates
are complete as of 2026-11-19.

## Global Constraints

- No AdminService, etcd or Prometheus connection is created when Console starts without a cluster source.
- Browser code never receives or persists Admin tokens, etcd passwords or private-key contents.
- All cluster data APIs use `/api/v1/console/connections/{id}/...`; no fallback to an arbitrary/default cluster.
- Only explicitly connected runtimes collect; disconnect/delete/shutdown stop scheduler, derived tasks and SSE.
- The selected connection is encoded as `?connection=<id>`; localStorage never overrides it.
- Keep Next.js `output: "export"`; no runtime dynamic route requiring `generateStaticParams`.
- Existing read-only cluster behavior remains unchanged apart from connection scoping; no new Raft write actions.
- Preserve legacy startup arguments through one ephemeral startup connection and fixed compatibility routes.
- Follow existing test style and sanitize all public upstream errors. Do not add speculative abstractions.
- Do not commit or push unless the user explicitly asks.

## Review Focus

- Concurrent connect/disconnect/delete and late initialization: exactly one live generation may survive; covered by Task 3 generation tests.
- Real clusters use overlapping node/group IDs and event object identities `(kind,node_id,group_id)`; their random event UUIDs are not forced to collide. Deterministic equal event IDs exercise preference isolation in unit/mock tests; covered by Tasks 4, 7 and 8.
- Missing/corrupt profile or unavailable keychain: explicit error, never overwrite with an empty store; covered by Task 2.
- URLs and upstream errors containing userinfo/secrets: rejected or redacted in responses/logs; covered by Tasks 2, 3 and 4.
- Delayed responses arriving after a switch: render only data tagged with the active connection; covered by Tasks 7 and 8.

---

### Task 1: Connection Contracts and Startup Validation

**Files:**
- Modify: `crates/rr-admin/src/console/config.rs`
- Modify: `crates/rr-admin/src/console/model.rs`
- Modify: `crates/rr-admin/openapi/console-v1.json`
- Test: inline tests in `config.rs` and existing contract tests

**Interfaces:**
- Produces: serializable `ConnectionId = String`, `ConnectionProfile`, redacted `ConnectionSummary`, `ConnectionStatus`, `ConnectionSource`, `TlsProfile`, `PrometheusProfile` and write-only secret input DTOs.
- Produces: neutral response meta with nullable `cluster_id`, `retention_source: NONE`, empty sources/capabilities.
- Consumes: existing `SeedPeer`, `DiscoverySecurityConfig`, `DiscoveryTransportConfig` and `PrometheusConfig` validation.

- [x] **Step 1: Write failing config/contract tests** proving no-source startup validates, no-source plus Admin/Prometheus startup options fails, profile DTOs omit secrets, and neutral meta has `cluster_id: null`.
- [x] **Step 2: Run focused tests to verify failure** with `cargo test -p rr-admin console::config` and the relevant model test.
- [x] **Step 3: Implement the DTOs and validation split** between process-only Console settings and per-connection settings; add `--state-dir`, keeping the existing listener security rules.
- [x] **Step 4: Update OpenAPI** with connection schemas, neutral meta, scoped paths and error codes `CONNECTION_NOT_FOUND`, `CONNECTION_NOT_CONNECTED`, `CONNECTION_ACTIVE`, `CONNECTION_REQUIRED`, `CREDENTIALS_REQUIRED`, `ACTIVE_CONNECTION_LIMIT`.
- [x] **Step 5: Run contract and config tests** with `cargo test -p rr-admin console` and the repository OpenAPI consistency check.

### Task 2: Atomic Profile Store and Secret Boundary

**Files:**
- Create: `crates/rr-admin/src/console/connection_store.rs`
- Modify: `crates/rr-admin/src/console.rs`
- Modify: `crates/rr-admin/Cargo.toml`
- Test: inline tests in `connection_store.rs`

**Interfaces:**
- Consumes: profile and secret DTOs from Task 1.
- Produces: `ConnectionStore::load(path)`, `save(&ConnectionProfile)`, `remove(id)`, `list()` with versioned JSON, atomic replace and `0600` files.
- Produces: `SecretStore` supporting memory/keychain lookup, replacement, clearing and availability/source reporting; secret inputs use omitted=keep, null=clear, value=replace.

- [x] **Step 1: Write failing store tests** for round trip, restart reload, duplicate IDs, corrupt JSON, non-current format version, atomic write failure preservation, file permissions and delete.
- [x] **Step 2: Write failing secret tests** for memory persistence across disconnect, keychain-unavailable behavior, PATCH keep/clear/replace and delete cleanup; no secret may appear in `Debug`, serialized profiles or errors.
- [x] **Step 3: Run tests to verify failure** with `cargo test -p rr-admin connection_store`.
- [x] **Step 4: Implement the smallest complete stores** using the platform config directory plus explicit `--state-dir`; add the OS keychain dependency only for this capability. Reject Prometheus URL userinfo/query/fragment.
- [x] **Step 5: Run store tests and lint** with `cargo test -p rr-admin connection_store` and `cargo clippy -p rr-admin --all-targets -- -D warnings`.

### Task 3: ConnectionManager and Runtime Lifecycle

**Files:**
- Create: `crates/rr-admin/src/console/connection_manager.rs`
- Modify: `crates/rr-admin/src/console.rs`
- Modify: `crates/rr-admin/src/console/scheduler.rs`
- Modify: `crates/rr-admin/src/console/upstream.rs`
- Test: inline manager/scheduler lifecycle tests

**Interfaces:**
- Consumes: `ConnectionStore`, `SecretStore`, `ConsoleConfig`, `TonicUpstream`, `Scheduler` and `ConsoleCache`.
- Produces: `ConnectionManager::{list,get,create,update,remove,test_candidate,test,connect,disconnect,shutdown_all}` and `resolve(id) -> Result<Arc<ConnectionRuntime>, PublicConnectionError>`.
- Produces: `ConnectionRuntime { scheduler, shutdown, generation }`; per-ID serialized operations and maximum 8 CONNECTING/CONNECTED runtimes.

- [x] **Step 1: Write failing lifecycle tests** for lazy connect, idempotent connect/disconnect, failed initialization, reconnect, edit-while-connected rejection, active limit, delete cleanup and shutdown-all.
- [x] **Step 2: Write failing race tests** for connect/disconnect/delete and late initialization using generations; assert no old task survives and no global map lock is held across await.
- [x] **Step 3: Write failing shutdown tests** showing scheduler derived tasks and runtime subscribers terminate; candidate test has a total deadline and creates no scheduler/watch/history task.
- [x] **Step 4: Run tests to verify failure** with `cargo test -p rr-admin connection_manager`.
- [x] **Step 5: Implement manager and tracked runtime shutdown** by extracting per-profile discovery/scheduler construction from `console.rs`; support CA-only TLS plus optional cert/key without changing legacy CLI semantics.
- [x] **Step 6: Implement bounded sanitized test/connect diagnostics**, retaining detailed upstream errors only internally; `last_connected_at` persists without starting saved profiles.
- [x] **Step 7: Run lifecycle tests** with `cargo test -p rr-admin console` and `cargo test -p rr-admin --test console_process`.

### Task 4: Scoped HTTP/SSE API and Legacy Compatibility

**Files:**
- Modify: `crates/rr-admin/src/console/http.rs`
- Modify: `crates/rr-admin/src/console.rs`
- Modify: `crates/rr-admin/src/main.rs`
- Modify: `crates/rr-admin/openapi/console-v1.json`
- Test: inline HTTP tests and `crates/rr-admin/tests/console_process.rs`

**Interfaces:**
- Consumes: `ConnectionManager::resolve` and mutation methods from Task 3.
- Produces: CRUD/test/connect/disconnect routes and all existing resources under `/connections/{id}/...`; `AppState` owns manager rather than one scheduler.
- Produces: fixed legacy mapping only to the ephemeral startup connection; no-source legacy routes return `CONNECTION_REQUIRED`.

- [x] **Step 1: Write failing HTTP tests** for every connection route, neutral auth/error meta, CSRF/Origin on every mutation, 401/404/409/429 paths and redacted responses.
- [x] **Step 2: Write failing scoped-resource tests** for summary, nodes/details/boots, groups/details/replicas, topology, performance, snapshots, lifecycle, events, sources/reconcile and stream; include two runtimes with identical node/group IDs.
- [x] **Step 3: Run HTTP tests to verify failure** with `cargo test -p rr-admin console::http`.
- [x] **Step 4: Refactor handlers to resolve their runtime** and move summary projection into the runtime; make disconnect end SSE and prevent new requests without canceling already captured ordinary reads.
- [x] **Step 5: Add ephemeral legacy startup adaptation** covering every old resource and `/stream`; reject missing/invalid startup-only cluster options explicitly.
- [x] **Step 6: Run HTTP/process/contract tests** with `cargo test -p rr-admin console`, `cargo test -p rr-admin --test console_process`, and the OpenAPI check.

### Task 5: Frontend Connection Client and URL Context

**Files:**
- Create: `crates/rr-admin/console/src/lib/connections.ts`
- Create: `crates/rr-admin/console/src/lib/connection-context.tsx`
- Modify: `crates/rr-admin/console/src/lib/contract.ts`
- Modify: `crates/rr-admin/console/src/lib/query-state.ts`
- Modify: `crates/rr-admin/console/src/lib/auth.ts`
- Test: corresponding Vitest files

**Interfaces:**
- Consumes: Task 1/4 HTTP DTOs and routes.
- Produces: typed CRUD/test/connect/disconnect client with nested bodies, CSRF on mutations, DELETE 204 and full-URL 401 return.
- Produces: `ConnectionProvider`, `useActiveConnection()`, `hrefWithConnection(path, params)` and `switchConnectionHref(path, currentSearch, id)`.

- [x] **Step 1: Write failing unit tests** for URL round trip, login allowlist/fallback, recent-ID non-override, switch whitelist (only `window`), unknown/deleted/disconnected IDs and mutation helpers.
- [x] **Step 2: Run tests to verify failure** with `pnpm --dir crates/rr-admin/console run test:unit -- connection`.
- [x] **Step 3: Implement typed client and provider** with static routes `/connections/` and `/connections/new/`; API functions require connection ID as a separate argument.
- [x] **Step 4: Run frontend unit/type checks** with `pnpm --dir crates/rr-admin/console run test:unit` and `pnpm --dir crates/rr-admin/console run typecheck`.

### Task 6: Connection Management UI

**Files:**
- Create: `crates/rr-admin/console/src/app/connections/page.tsx`
- Create: `crates/rr-admin/console/src/app/connections/new/page.tsx`
- Create: `crates/rr-admin/console/src/components/connections/connection-list.tsx`
- Create: `crates/rr-admin/console/src/components/connections/connection-form.tsx`
- Modify: `crates/rr-admin/console/src/app/page.tsx`
- Test: component Vitest and mock Playwright specs

**Interfaces:**
- Consumes: Task 5 provider/client and existing UI primitives.
- Produces: empty/list/error/loading states and New/Edit/Test/Save/Save & Connect/Connect/Disconnect/Delete flows.

- [x] **Step 1: Write failing component tests** for empty state, list actions, simultaneous seeds+discovery, validation, secret keep/clear/replace, keychain availability, test failure then save, failed save-and-connect persistence and connected edit lock.
- [x] **Step 2: Write failing mock E2E** for create/test/save/connect, disconnect/edit/delete, keyboard/mobile flows, and absence of secrets in URL/browser storage/network responses.
- [x] **Step 3: Run focused tests to verify failure** with the Vitest connection files and Playwright connection spec.
- [x] **Step 4: Implement accessible responsive pages and forms** using existing design tokens/components; active runtime node summaries come from the list DTO, never N summary requests.
- [x] **Step 5: Run unit, E2E, lint and build** with the standard package scripts.

### Task 7: Cluster Shell Switching and Data-plane Isolation

**Files:**
- Modify: `crates/rr-admin/console/src/lib/api.ts`
- Modify: `crates/rr-admin/console/src/lib/use-console-resource.ts`
- Modify: `crates/rr-admin/console/src/lib/use-console-stream.ts`
- Modify: `crates/rr-admin/console/src/lib/refresh-context.tsx`
- Modify: `crates/rr-admin/console/src/components/console/console-shell.tsx`
- Modify: all internal drilldown/navigation call sites and connection-local cursor/preference owners
- Test: existing affected Vitest files plus new switch isolation tests

**Interfaces:**
- Consumes: Task 5 connection provider/href builders and Task 4 scoped resources.
- Produces: connection-tagged resource/summary state and scoped request/SSE APIs; connection selector preserving the current main page.

- [x] **Step 1: Write failing delayed-switch tests** proving old data never renders, old requests/resync/details abort, old EventSource/reconnect timers close, late callbacks are ignored and pause/interval survive.
- [x] **Step 2: Write failing navigation tests** covering Shell, global search, every drilldown helper and login return; unknown/not-connected stops polling/reconnect, unreachable retains controlled retry.
- [x] **Step 3: Write failing local-state tests** for groups/events/snapshots/node cursors and per-connection event acknowledge/silence preferences.
- [x] **Step 4: Run focused tests to verify failure** with `pnpm --dir crates/rr-admin/console run test:unit`.
- [x] **Step 5: Implement scoped API, connection-tagged state and shell selector** without keying the entire RefreshProvider; preserve last-good data only within the same connection.
- [x] **Step 6: Update all internal links and local cursors/preferences**, then run unit, typecheck, lint, build and mock E2E.

### Task 8: Dual-cluster E2E, Documentation and Final Gates

**Files:**
- Modify: `crates/rr-admin/console/e2e/mock/server.mjs` and fixtures/specs
- Modify: `crates/rr-admin/console/e2e/real/harness.ts`, `global-setup.ts`, `cluster.spec.ts`
- Modify: `README.md`, `docs/docs/operations/cluster-admin-console.md`, `docs/docs/deployment/*`, relevant C4 docs and release notes
- Test: real dual-cluster Playwright and repository gates

**Interfaces:**
- Consumes: all previous tasks.
- Produces: one no-source Console with isolated state-dir managing two real clusters through the UI; updated public startup/API documentation.

- [x] **Step 1: Extend mock/real harnesses** so the real clusters use overlapping node/group IDs and event object identities `(kind,node_id,group_id)`, while unit/mock fixtures deterministically reuse event IDs for preference isolation. No test injects fake real-cluster events or copies UUIDs, and all helpers require a connection ID; the new UI never uses legacy routes.
- [x] **Step 2: Add deterministic dual-cluster E2E** for create/connect, every resource family, SSE, switch, refresh/bookmark, two Pages in one browser context, disconnect/delete and late-response isolation.
- [x] **Step 3: Run real E2E and observe the required behavior**, retaining targeted diagnostics for any failure.
- [x] **Step 4: Update startup, security, compatibility, API and architecture documentation** to match the delivered behavior; mark the design/plan status accurately.
- [x] **Step 5: Run final local gates** with `cargo test -p rr-admin`, frontend typecheck/lint/unit/build, mock and real E2E, OpenAPI/docs checks and `git diff --check`.
- [x] **Step 6: Run GitNexus detect-changes and one independent final review**, covering correctness, security, compatibility, concurrency, tests, performance/resource bounds and simplicity; address all introduced findings before completion. Formal and specialist reviews completed; fix round 1 addressed all seven findings, including untracked-aware CLI detect with an isolated temporary index.
