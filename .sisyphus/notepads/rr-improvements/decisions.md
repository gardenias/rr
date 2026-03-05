## [2026-03-05] Architectural Decisions

### D1: parking_lot::RwLock
- Use `parking_lot::RwLock` (NOT tokio::sync::RwLock) — locks never held across .await
- parking_lot API: no .read().expect() needed — returns guard directly (infallible)
- Pattern: replace `self.inner.read().expect("...")` → `self.inner.read()`
- Add parking_lot to rr-raft/Cargo.toml as a dependency

### A2: Criterion Benchmarks
- Create a NEW crate at `benches/` workspace root (not inside existing crates)
- New `benches/Cargo.toml` with `[[bench]]` sections
- Add to workspace members in root Cargo.toml
- criterion must be added as workspace dev-dependency

### A3: Proptest
- Tests go INSIDE the existing crates (`#[cfg(test)]` modules)
- proptest as workspace dev-dependency
- BincodeCodec tests: rr-storage or rr-raft (wherever codec is defined)
- MemStorage tests: rr-storage crate

### C3: Counter Example
- New crate: examples/counter/
- Add to workspace members
- Implements Service trait from rr-core
- Must be runnable: cargo run -p counter

### C4: Rustdoc
- Add //! crate-level docs to: rr-storage/lib.rs, rr-router/lib.rs, rr-config/lib.rs, rr-core/lib.rs
- cargo doc --workspace --no-deps must emit zero warnings with RUSTDOCFLAGS='-D warnings'

### A1: Coverage CI
- cargo-llvm-cov tool
- Coverage job runs AFTER test job (needs: [test])
- Upload to Codecov with CODECOV_TOKEN secret
- .codecov.yml: no hard failures initially
- README.md: add coverage badge

### General Guards
- No unwrap()/expect() in production paths (only in tests with context)
- Bilingual docs: Chinese paragraph → --- separator → English paragraph
- Section headers: ## 标题 / Title format
- New Docusaurus pages for features (Wave 4)
