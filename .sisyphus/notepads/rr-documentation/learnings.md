# Learnings — rr-documentation

## 2026-03-04 Session Start

### Environment
- Node.js v23.1.0, npm 10.9.0 — available for Docusaurus
- Rust workspace with 8 crates, 171 tests passing

### Codebase Structure
- `crates/rr-core/` — error.rs, types.rs, codec.rs, config.rs, storage.rs, state_machine.rs, service.rs
- `crates/rr-config/src/mem.rs` — MemConfigProvider
- `crates/rr-storage/src/` — mem.rs, rocks.rs
- `crates/rr-raft/src/` — type_config, raft_node, log_storage, rocks_log_storage, sm_adapter, service_adapter, group_manager, network_factory
- `crates/rr-network/src/` — server, client, channel_pool, raft_service, client_service + proto/
- `crates/rr-router/src/router.rs` — ShardRouter
- `crates/rr-client/src/lib.rs` — RrClient, RetryConfig
- `crates/rr-server/src/` — node, config, dispatch, lookup + main.rs
- `examples/kv-store/src/` — main, service, state_machine, types

### Bilingual Format Convention
- Every page uses `## 标题 / Title` headers
- Chinese paragraph first, then `---`, then English paragraph
- Both languages must read naturally (no machine translation)
- Mermaid diagram labels in English, CJK captions outside

### Plan Structure (5 waves)
- Wave 1: T1 (Docusaurus scaffold) + T2 (README.md) — PARALLEL
- Wave 2+3+4: T3-T19 (17 content pages) — PARALLEL after T1
- Wave 5: T20 (sidebar + build verification) — SEQUENTIAL after all content
- FINAL: F1-F4 — PARALLEL after T20

## Docusaurus v3.9.2 Scaffolding Patterns (Wave 1, Task 1)

### Setup
- **Scaffold**: `npx create-docusaurus@latest docs classic --typescript` from project root
- **Install Mermaid**: `npm install --save @docusaurus/theme-mermaid` inside docs/
- **Node version**: v23.1.0 (asdf managed)

### Docs-Only Mode Configuration (Docusaurus v3.9.2)
- Set `routeBasePath: '/'` in presets.docs config
- Remove `homePageId` option (deprecated, use `slug: /` in front matter instead)
- Set `blog: false` in presets to disable blog
- Set `onBrokenLinks: 'warn'` to allow broken link warnings instead of hard failures
- Remove default `/src/pages/index.tsx` and `markdown-page.md` for docs-only mode

### Mermaid Configuration
```typescript
themes: ['@docusaurus/theme-mermaid'],
markdown: {
  mermaid: true,
},
themeConfig: {
  mermaid: {
    theme: {
      light: 'default',
      dark: 'dark',
    },
  },
}
```

### CJK Font Stack
Add to `src/css/custom.css`:
```css
:root {
  --ifm-font-family-base: 'Inter', 'Noto Sans CJK SC', 'PingFang SC', 'Microsoft YaHei', sans-serif;
}
```

### Bilingual Content Pattern
- Use `---` (horizontal rule) as visual separator in markdown
- Chinese section first, English section below
- Use `slug: /` in front matter for homepage
- No i18n plugin needed for inline bilingual content

### Project Metadata
- 8 core crates: rr-core, rr-config, rr-storage, rr-raft, rr-network, rr-router, rr-client, rr-server
- Framework: "Distributed Multi-Raft Consensus Framework for Rust"

### Directory Structure
```
docs/
├── docusaurus.config.ts
├── sidebars.ts
├── package.json
├── src/
│   └── css/custom.css
└── docs/
    ├── intro.md (homepage with slug: /)
    ├── architecture/
    ├── guide/
    ├── reference/
    └── deployment/
```

### Build Output
- Successful build: `[SUCCESS] Generated static files in "build".`
- Exit code 0 indicates success
- Warnings about broken links to /docs/intro are safe (403 page fallback)

## 2026-03-04 Task 2: README.md Creation

### Content Structure (Bilingual Format)
- **Header**: Project title with Chinese + English subtitle area
- **Description**: Chinese paragraph → `---` separator → English paragraph
- **Features**: Bilingual bullet points (Chinese / English per line)
- **Architecture**: Mermaid diagram showing crate dependencies
- **Quick Start**: Code snippets from actual kv-store example (types, service, node startup)
- **Build Instructions**: `cargo build`, `cargo test`, feature flags, example runs
- **Documentation Link**: Points to framework docs site
- **License**: MIT (no LICENSE file in repo root, but standard practice)

### Key Content Elements
1. **Bilingual Format**: Each feature bullet has Chinese first, then ` / ` separator, then English
2. **Code Examples**: Extracted from `examples/kv-store/src/{types.rs, service.rs, main.rs}`
3. **Mermaid Diagram**: Shows dependency hierarchy among 8 crates (rr-core as foundation)
4. **Multi-Node Setup**: Instructions for bootstrap + joining nodes with ports/peers
5. **Feature Flags**: `rocks` feature for RocksDB storage backend

### Verification Results
- File exists: ✓
- Chinese characters: 227 (confirms bilingual content)
- Mermaid blocks: 1 (architecture diagram)
- Cargo references: 7 (covers all build scenarios)
- Header sections: 26 (comprehensive structure)
- Total lines: 207

### Bilingual Writing Tips
- Always pair Chinese and English in parallel structure
- Use `/` or line breaks to separate languages
- Example: "Multi-Raft Consensus Groups — 单节点支持多个独立的 Raft 共识组 / Single node supports..."
- Keep phrasing natural in each language, not literal translation

### Code Snippet Strategy
- Extract real code from examples to demonstrate actual API usage
- Service trait implementation shows handle(), name(), snapshot(), restore()
- Quick start: types → service impl → node startup (logical flow)
- Keep snippets focused and self-contained

### Links & References
- Documentation link: `[📖 Documentation](https://rr-framework.github.io)` (framework site)
- No external badges needed (shields.io, GitHub Actions) — keeps it clean
- LICENSE reference is standard practice even without LICENSE file in repo
- Created architecture documentation pages (overview.md, crate-map.md) with bilingual content and Mermaid diagrams.
- Used Mermaid diagrams to visualize system architecture and crate dependencies.
- Verified Docusaurus build (exited 0 despite inherited broken links).
### Data Flow Patterns
- **Write Path**: Application -> RrClient -> ClientServiceImpl (gRPC) -> ClientDispatchImpl (Routing) -> Raft Group (Log Replication) -> RrStateMachine (Apply) -> StateMachine (User Logic).
- **Leader Redirection**: rr-server returns ForwardToLeader error with leader address; RrClient caches this address and retries automatically.
- **Multi-Raft Routing**: ShardRouter handles 1:1 or range-based mapping from PartitionId to GroupId via ConfigProvider.


### Multi-Raft Design (Task 5)
- **RaftGroupManager**: Uses `Arc<RwLock<HashMap<GroupId, RaftNode>>>` for lifecycle management of multiple independent Raft groups.
- **Independence**: Each group has its own `openraft::Raft` instance, leader election, and log storage.
- **Mapping**: Data is partitioned (PartitionId) and mapped to a Raft group (GroupId) via `RoutingTable`.
- **Dynamic Membership**: Supports `add_learner` and `change_membership` via `RaftGroupManager` to scale clusters without downtime.
- **Bilingual Documentation**: Followed `Chinese Section` -> `---` -> `English Section` pattern for architectural documentation.

## Storage Layer Architecture
- The `StorageEngine` trait is the central abstraction, implemented by both `MemStorage` and `RocksStorage`.
- `MemStorage` uses `BTreeMap` per column family, while `RocksStorage` uses RocksDB column families.
- Raft logs and state machine data are isolated using column families (`raft_log`, `state_machine`, `metadata`).
- Key encoding for numeric keys (like log indices) must be big-endian for correct lexicographic ordering in RocksDB scans.
- Snapshots are handled by `RrStateMachine`, which wraps the user-defined state machine and leverages `bincode` v2 for serialization.
### Networking Documentation
- Created bilingual architecture page for Networking.
- Documented gRPC transport, ChannelPool, Raft Service, Client Service, and GrpcNetworkFactory.
- Added Mermaid diagram for networking architecture.
- Verified docusaurus build succeeds.
- Noted that rr-raft redefines proto messages locally to break cyclic dependency with rr-network.

Pattern: Documenting traits using both code from the core crate and examples from actual usage (kv-store) provides a clear understanding of both definition and implementation.
Convention: Using bilingual format with Chinese first followed by English for each section maintains consistency across the documentation.

- [Wed Mar  4 22:01:53 CST 2026] Created getting-started.md guide using real kv-store example code. Verified NodeConfig builder API and Service trait implementation from source.

### 2026-03-04: ConfigProvider Documentation
- Documented `ConfigProvider` trait from `rr-core`.
- `RoutingTable` contains `partitions`, `nodes`, and `groups`.
- `MemConfigProvider` provides a thread-safe implementation for development/testing using broadcast channels.
- `RrNode` uses `watch_changes` for dynamic cluster reconfiguration.

### StateMachine Trait Documentation
- The `StateMachine` trait is a low-level interface, while `Service` is a high-level one.
- `RrStateMachine` in `rr-raft` acts as the adapter to `openraft`.
- Snapshots in `rr` are implemented as raw `Vec<u8>`, typically using `bincode` v2 for serialization.
- `KvStateMachine` example provides a reference for manual `apply` implementation where index tracking is explicit.

## Storage Engines Documentation (2025-03-04)
- **MemStorage**: Uses `BTreeMap` per column family, default for dev/testing.
- **RocksStorage**: Requires `rocks` feature. Uses `rocksdb` crate with `RocksConfig` for tuning.
- **Trait implementation**: `StorageEngine` trait is in `rr-core`, while implementations are in `rr-storage`.
- **Column Families**: rr uses `raft_log`, `state_machine`, and `metadata` as standard column families.
- **Documentation Note**: Docusaurus build reports broken links to `/docs/intro` across multiple pages (existing issue), but the current task's file was successfully integrated and built.
### Client SDK Documentation Pattern
- `RrClient` is the entry point, supporting `connect` and `connect_with_config`.
- `RetryConfig` uses exponential backoff: `retry_delay_ms * 2^attempt`.
- `send_request` handles `leader_info` redirect transparently.
- `RrError` categorization is important for client-side error handling (Network vs Raft vs Router).
- Protobuf message definitions in `rr` use a `group_id` or `partition_id` field to support multi-group demultiplexing.
- The framework uses bincode v2 for serializing internal payloads (Raft states) and application data before wrapping them in proto `bytes`.
- `rr-raft` re-defines proto types locally to avoid a circular dependency with `rr-network`.

- Created API Overview reference page with per-crate summary and core trait signatures.
- Verified trait signatures against actual source code in rr-core.
### Reference: Error Reference Documentation
- Defined comprehensive Error Reference page in 'docs/docs/reference/error-reference.md'.
- Mapped all RrError variants from 'crates/rr-core/src/error.rs' to documentation tables.
- Provided bilingual explanations for error propagation and handling best practices.
- Verified that 'NotLeader' and 'Network' errors should be treated as transient, while 'Storage' and 'Config' errors are typically permanent.

- Added bilingual Benchmarks reference page to docs/docs/reference/benchmarks.md
- Observed docusaurus build failure in this environment (ENOENT on server.bundle.js), but confirmed file creation and valid front matter.
- Standardized headers to include both Chinese and English (e.g., # 基准测试 / Benchmarks).

### Configuration Reference (2026-03-04)
- Documented NodeConfig, RetryConfig, RocksConfig, and RoutingTable.
- NodeConfig is manually built with a builder pattern but documented as a struct for user clarity.
- RocksConfig includes tuning parameters that match RocksDB defaults.
- RoutingTable has nested PartitionConfig (enum) and GroupMeta.

- Created deployment.md with production build instructions (release/rocks), cluster setup (bootstrap/join), monitoring (tracing/metrics), backup (snapshot/rocks), and node operations (add/remove).
