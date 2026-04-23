# Contributing to rr

Thanks for considering a contribution! rr is a distributed multi-Raft consensus
framework in Rust. This guide will get you from zero to a green `cargo test`
in under 20 minutes, and explains how to propose changes.

> New to rr? Start with the [architecture overview](./docs/docs/architecture/overview.md)
> and the [C4 diagrams](./docs/docs/architecture/c4/README.md) for a mental map.

## 1. Prerequisites

- **Rust toolchain**: 1.82 or newer (MSRV is checked in CI).
  ```bash
  rustup toolchain install 1.82
  rustup default stable
  rustup component add clippy rustfmt
  ```
- **System dependencies** (required only if you build with the `rocks` feature):
  - **Linux (Debian/Ubuntu)**: `sudo apt-get install -y libclang-dev cmake build-essential`
  - **macOS**: `brew install cmake` (LLVM ships with Xcode command-line tools)
- **Optional but recommended**:
  - `cargo-nextest` for faster test runs: `cargo install cargo-nextest`
  - `cargo-llvm-cov` for local coverage: `cargo install cargo-llvm-cov`
  - `cargo-audit` for dependency audits: `cargo install cargo-audit`

## 2. Clone & build

```bash
git clone https://github.com/OWNER/rr.git
cd rr
cargo build --workspace
```

The first build pulls ~600 crates and may take 5–10 minutes. Subsequent builds
are incremental.

## 3. Run the tests

### Standard suite (default features, in-memory storage):
```bash
cargo test --workspace
```

### With RocksDB (persistent storage):
```bash
cargo test --workspace --features rr-storage/rocks
```

### Integration tests only (real gRPC, multi-node clusters):
```bash
cargo test -p integration
```

### Chaos tests (network partitions, latency, packet loss):
```bash
cargo test --workspace -- --ignored
```

### Defensive store mode (catches misuse of the storage abstraction):
```bash
RR_STORE_DEFENSIVE=on cargo test --workspace
```

## 4. Run the examples

### Single-node demo
```bash
cargo run -p kv-store -- --demo
cargo run -p counter -- --demo
```

### 3-node local cluster (three terminals):
```bash
# Terminal 1
cargo run -p kv-store -- --node-id 1 --port 51001 --bootstrap
# Terminal 2
cargo run -p kv-store -- --node-id 2 --port 51002 --peers 127.0.0.1:51001
# Terminal 3
cargo run -p kv-store -- --node-id 3 --port 51003 --peers 127.0.0.1:51001,127.0.0.1:51002
```

## 5. Style & quality gates

All PRs must pass these gates — CI enforces them:

```bash
# Format (must pass)
cargo fmt --check

# Lint (must pass, warnings are errors)
RUSTFLAGS="--deny=warnings" cargo clippy --workspace -- -D warnings

# Doc tests
cargo test --doc --workspace --no-deps

# Dependency audit
cargo audit
```

### Code guidelines

- **No `anyhow` in library crates** — only `examples/` and `tests/` may use it.
  Library code returns typed errors from `rr-core::error`.
- **No `println!` / `eprintln!` in library crates** — use `tracing::{info, warn, debug}`
  with structured fields.
- **No `.unwrap()` / `.expect()` in hot paths** — return `Result` and let the
  caller decide. `.expect()` is acceptable for genuinely infallible operations
  (e.g., mutex poisoning before a panic).
- **No `tokio::spawn` without `JoinHandle` tracking** — all tasks must be
  owned so they can be joined on shutdown.
- **No circular dependencies between crates** — `rr-core` is the DAG root and
  must never depend on any other `rr-*` crate.
- **No hardcoded addresses, ports, or secrets** — everything through `NodeConfig`
  or equivalent.
- **No `TODO` / `FIXME` / `HACK` comments** — open a GitHub issue instead and
  reference it in a comment if necessary.

### Commit message style

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): short subject

Longer explanation if needed.
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`, `build`, `ci`.
Scopes are crate names (`rr-raft`, `rr-storage`, …) or area names (`docs`, `ci`).

## 6. Tests policy (TDD)

We practice test-driven development:

1. **RED** — write a failing test that expresses the desired behavior.
2. **GREEN** — write the minimum implementation to pass it.
3. **REFACTOR** — clean up, preserving green.

PRs that add features without tests will be asked to add them. If a test
genuinely cannot be added (e.g., external service dependency), explain why
in the PR description.

## 7. Building the docs

The documentation site uses [Docusaurus](https://docusaurus.io/) in `docs/`.

```bash
cd docs
yarn install
yarn start   # local preview at http://localhost:3000
yarn build   # static output in docs/build/
```

## 8. Opening a pull request

1. **Fork** the repo and create a topic branch: `git checkout -b feat/my-feature`.
2. **Implement** with tests. Keep commits small and focused.
3. **Update `CHANGELOG.md`** under the `[Unreleased]` section with a short
   entry describing what changed. Use the Added/Changed/Fixed/Security/
   Removed/Deprecated categories per Keep a Changelog.
4. **Push** and open a PR against `main`. Fill out the PR template.
5. **CI** will run build, test (default + rocks + defensive), clippy, fmt,
   coverage, benchmarks, MSRV, doc tests, cargo-audit, and chaos tests. All
   must pass.
6. **Review** — at least one maintainer must approve.

## 9. Reporting security issues

**Do not** open a public issue for security vulnerabilities. Email the
maintainer listed in `CODE_OF_CONDUCT.md` directly with details and, if
possible, a reproducer. We will acknowledge receipt within 72 hours and work
with you on disclosure timing.

## 10. Project structure

```
rr/
├── crates/
│   ├── rr-core/       # foundation traits + types
│   ├── rr-storage/    # Mem + RocksDB storage engines
│   ├── rr-config/     # ConfigProvider implementations
│   ├── rr-raft/       # openraft wrapper + multi-Raft mgmt
│   ├── rr-network/    # tonic/gRPC transport + protos
│   ├── rr-router/     # partition → group → leader resolver
│   ├── rr-server/     # RrNode orchestrator
│   ├── rr-client/     # client SDK
│   └── rr-admin/      # admin CLI
├── examples/
│   ├── kv-store/      # distributed KV example
│   └── counter/       # distributed counter example
├── tests/
│   └── integration/   # multi-node integration + chaos tests
├── benches/           # criterion benchmarks
├── docs/              # Docusaurus documentation site
└── .sisyphus/plans/   # multi-phase roadmap docs
```

## 11. Getting help

- Read the [Architecture Overview](./docs/docs/architecture/overview.md)
  and [C4 Diagrams](./docs/docs/architecture/c4/README.md).
- Search existing [GitHub issues](https://github.com/OWNER/rr/issues) before
  opening a new one.
- For design discussions, open a GitHub Discussion tagged `design` or
  `question`.

Thank you for contributing!
