# C4 Rustdoc Learnings

## Pre-existing doc warnings
- `rr-raft/src/service_adapter.rs`: redundant explicit link targets for `Service` and `StateMachine`
- `tests/integration/src/lib.rs:303`: unresolved link `node[0]` — needs `\[0\]` escaping
- `examples/kv-store/src/state_machine.rs:3`: unresolved link to `KvService` (cross-module ref)

These are outside the 4 target files and cannot be fixed per task constraints.

## Conventions observed
- Existing `//!` one-liners use the `//! rr-{crate} — Description` format
- All crates re-export key items from modules via `pub use`
- Code examples in existing `///` docs use `rust,ignore` for anything requiring full framework setup

## Doc style used
- Kept original one-liner as first line (not modified)
- Added `## Architecture` and `## Usage` sections per spec
- Used `rust,ignore` for examples with async/external deps
- Used `rust,no_run` for rr-storage since it can compile standalone with hidden `async fn` wrapper
- Intra-doc links like `[`ShardRouter::route`]` work for items within the same crate
- Cross-crate explicit links like `[`ConfigProvider`](rr_core::config::ConfigProvider)` needed for rr-router and rr-config
