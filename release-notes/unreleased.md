# Unreleased release notes

## Multi-cluster Cluster Admin Console

`rr-admin console` is now an independent, single-operator read-only client for multiple Rr clusters.
It starts without peers, discovery, Admin credentials, or Prometheus and opens `/connections/` instead
of binding itself to one startup cluster.

- Test, save, edit, connect, disconnect, and delete connection profiles through the browser.
- Give every explicitly connected cluster its own upstream, discovery, scheduler, cache, Prometheus
  configuration, event/cursor state, and SSE stream. At most eight runtimes, including CONNECTING,
  may be active at once.
- Use `/api/v1/console/connections/{id}/...` for every cluster resource and stream. The browser URL's
  `?connection=<id>` is authoritative for navigation, refresh, bookmarks, and multiple Pages.
- Stop runtime tasks and SSE on disconnect, delete, and process shutdown. Saved profiles survive a
  restart in a disconnected state; runtimes, sessions, caches, and trends do not resume automatically.

The Console still performs no Raft administration writes. Membership changes, forced leadership,
snapshots, drains, and offset resets remain with the Admin CLI or another authorized client.

## Persistence and security

Non-sensitive profiles are stored as versioned `connections.json` with atomic replacement and mode
`0600`, under `--state-dir` or the platform configuration directory. Corrupt or unknown-version stores
fail loudly rather than being replaced with an empty list. Admin tokens and discovery passwords are
write-only and process-memory-only by default; explicit Remember uses the OS keychain. TLS fields are
paths on the Console host, not browser uploads.

For isolated tests and headless systems, `--keychain-disable` explicitly disables every OS keychain
operation, keeps all connection secrets in memory, disables Remember in the UI, and reports
`keychain_available=false`. Production remains keychain-enabled when the flag is omitted. Memory-only
secrets must be re-entered after a restart.

Connection CRUD, tests, connect/disconnect, delete, and reconcile mutations retain same-origin Origin
and session CSRF protection. APIs return redacted credential capabilities, never secret values.

## Compatibility

Existing `--peers`, `--discovery`, global Admin TLS/token, and process-level Prometheus options remain
available as one ephemeral startup connection. Legacy unscoped resource routes and `/stream` stay fixed
to that connection and never fall back to an arbitrary saved profile. Without a startup connection they
return `CONNECTION_REQUIRED`; disconnected and deleted IDs return explicit scoped errors.

The deployment model remains one `rr-admin` process serving both the connection-scoped API/SSE and the
Next.js static export from the same origin. No separate Node.js server, multi-user authorization system,
OIDC/RBAC integration, or Raft control plane is introduced.

## Local full-observation workflow

`kv-store` now accepts explicit descriptor addresses, environment and placement metadata, an application version, and a
persistent boot-journal directory. The new `kv-console-cluster-up` recipe combines these with Genesis, Rocks, per-node
metrics, and a seeded Console; `kv-console-cluster-down` stops the stack without deleting its data. `console-up` and
`console-down` also manage a standalone local Console, with `RR_CONSOLE_TOKEN` supplying the viewer credential.

## Acceptance evidence

The real Playwright harness starts two independent three-node RocksDB clusters through local seed-based
processes, with overlapping node/group IDs, plus one no-source Console with an isolated state directory
and `--keychain-disable`. It creates both profiles through the UI and verifies all resource families,
nine views, SSE isolation, switching, refresh/bookmarks, two Pages, delayed real responses, leader
failure/restart isolation, Console restart, disconnect, and persistent delete.

The harness uses local binaries and browser installations only. It neither downloads an etcd image nor
accesses the developer's real OS keychain. Exact final gate results and any residual limits are recorded
in `.superpowers/sdd/2026-11-18-multi-cluster-console/task-8-report.md`; this unreleased note is not a claim about real etcd discovery or managed Genesis,
which the seed-based acceptance run does not exercise.
