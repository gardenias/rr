# C3 Counter Service — Learnings

## What was already done
- The counter example files (`Cargo.toml`, `src/main.rs`, `README.md`) and workspace member entry already existed from a prior pass.
- The implementation was complete with CounterService, CounterRequest/Response types, Service trait impl, demo mode, multi-node mode, and 9 unit tests.

## What needed fixing
- **CLI parsing**: Used manual arg parsing (same pattern as kv-store) which didn't support `--help`. Replaced with `clap` derive-based `Parser` to satisfy acceptance criteria (`--help` exits 0).
- **Cargo.toml**: Had `rr-client` dep (unnecessary, copied from kv-store). Replaced with `clap = { version = "4", features = ["derive"] }`.

## Patterns observed
- Service trait is `Send + Sync` (not `Clone`) — but examples derive `Clone` anyway since they use `Arc<RwLock<...>>` for shared state.
- `ServiceStateMachineAdapter<S: Service>` wraps a Service into a StateMachine — no Clone bound needed.
- kv-store uses manual arg parsing; counter now uses clap (improvement).
- bincode 2.0 API: `encode_to_vec(&val, standard())` and `decode_from_slice::<T>(&bytes, standard())`.
- Error mapping pattern: `RrError::Codec(CodecError::DeserializeFailed(e.to_string()))`.
