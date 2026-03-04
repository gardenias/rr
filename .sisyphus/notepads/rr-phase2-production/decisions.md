# Decisions — rr-phase2-production

## [2026-03-03] Architecture Decisions
- RocksDB feature-gated: `--features rocks` (not required for default build)
- TDD methodology continues from Phase 1 (test-first)
- TiKV storage pattern: shared DB with key prefixes, NOT one CF per Raft group
- No anyhow in library crates (only examples/ and tests/)
- No println!/eprintln! in library crates — use tracing macros
- rr-core must remain DAG root with ZERO deps on other rr-* crates
- No tokio::spawn without JoinHandle tracking
- No global state / static singletons — all state owned by RrNode
- No hardcoded addresses or ports — everything from NodeConfig
- No blocking RocksDB calls on tokio runtime — must use spawn_blocking
- Client SDK in new rr-client crate (Wave 4)
- Multi-node test timeout: 30s per test, 60s for E2E smoke test
