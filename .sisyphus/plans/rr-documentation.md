# rr Framework Documentation

## TL;DR

> **Quick Summary**: Create a comprehensive bilingual (中英文混排) documentation site using Docusaurus for the `rr` distributed multi-Raft framework. Two main deliverables: Framework Architecture Description and User Manual, plus a root README.md.
> 
> **Deliverables**:
> - Docusaurus documentation site with 19 pages
> - Root `README.md` as GitHub project entry point
> - 6 architecture pages (framework description)
> - 6 user guide pages (user manual)
> - 5 reference pages (API, proto, config, errors, benchmarks)
> - 1 deployment page
> 
> **Estimated Effort**: Large
> **Parallel Execution**: YES — 5 waves
> **Critical Path**: T1 (scaffold) → T2-T7 (architecture) → T8-T13 (guide) → T14-T18 (reference+deploy) → F1-F4 (verification)

---

## Context

### Original Request
User requested two documentation deliverables:
1. **框架说明文档 (Framework Description)**: Architecture, design decisions, concepts, data flow, per-module deep dive
2. **使用手册 (User Manual)**: Getting started, configuration, running a cluster, client SDK usage, deployment

### Interview Summary
**Key Discussions**:
- Language: Bilingual single-page mixed (中英文交替, paragraph-level interleaving)
- Format: Docusaurus documentation site
- Target audience: Both contributors (architecture doc) and application developers (user manual)
- Architecture depth: Two layers — high-level overview + per-module deep dive
- Diagrams: Mermaid (Docusaurus native support)
- Root README.md: Yes, as GitHub project entry point
- Scope: Full — includes API reference, deployment/ops, benchmarks placeholder

**Research Findings**:
- Analyzed TiKV, OpenRaft, Axum, Actix-web documentation patterns
- OpenRaft pattern (in-code docs module) best for framework docs
- TiKV pattern (external doc site + repo README) aligns with Docusaurus approach
- Codebase has comprehensive rustdoc comments on all public traits/structs already
- No existing documentation files in the repo

### Metis Review
**Identified Gaps** (addressed):
- Bilingual content doubles page length → Use visual separators and collapsible blocks
- Mermaid may not render CJK well → Keep diagram labels in English, CJK captions outside
- Docusaurus build may fail with CJK content → Verify build after first bilingual page
- Benchmarks: no benchmark code exists → Placeholder page only
- Deployment: no Docker/K8s infra → Generic deployment guidance only

---

## Work Objectives

### Core Objective
Create a production-quality bilingual documentation site for the `rr` distributed multi-Raft framework, covering architecture internals for contributors and a step-by-step user manual for application developers.

### Concrete Deliverables
- `docs/` directory containing a Docusaurus site (19 markdown pages + config)
- Root `README.md` as GitHub project entry point
- Architecture docs: overview, crate map, data flow, multi-Raft design, storage, networking
- User guide: getting started, Service trait, StateMachine trait, config, storage engines, client SDK
- Reference: API overview, protobuf reference, config reference, error reference, benchmarks placeholder
- Deployment guide

### Definition of Done
- [ ] `cd docs && npm run build` exits 0
- [ ] All 19 `.md` doc files exist in `docs/docs/`
- [ ] Every `.md` file contains both Chinese and English content
- [ ] Root `README.md` exists with project intro and docs link
- [ ] At least 3 pages contain Mermaid diagrams
- [ ] Sidebar navigation correctly groups all pages
- [ ] `cargo doc --workspace --no-deps` still builds

### Must Have
- Bilingual content (中英文交替) on every page
- Mermaid architecture diagrams
- Getting-started guide based on kv-store example
- All 8 crates documented in architecture section
- Client SDK usage guide
- Working Docusaurus build

### Must NOT Have (Guardrails)
- No line-number references in docs (they go stale with code changes)
- No duplicating `cargo doc` content — link to it, provide conceptual overviews
- No inventing features or APIs that don't exist in the codebase
- No writing benchmark code — only a placeholder page explaining how to benchmark
- No creating Docker/K8s manifests — only generic deployment guidance
- No i18n plugin (paragraph-level interleaving requires inline approach)
- No machine-translated content — both languages should read naturally
- No excessive ASCII art — use Mermaid for all diagrams

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed. No exceptions.

### Test Decision
- **Infrastructure exists**: N/A (documentation project)
- **Automated tests**: None (documentation)
- **Framework**: N/A

### QA Policy
Every task MUST include agent-executed QA scenarios.
Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **Documentation site**: Use Bash to run `npm run build` and verify output
- **Content quality**: Use Bash to grep for bilingual markers, Mermaid blocks, etc.
- **Links**: Verify internal links work via build output (Docusaurus reports broken links)

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately — foundation):
├── Task 1: Docusaurus scaffold + config + Mermaid plugin + CJK CSS [quick]
├── Task 2: Root README.md [quick]

Wave 2 (After Wave 1 — architecture docs, MAX PARALLEL):
├── Task 3: Architecture overview + crate map (depends: 1) [writing]
├── Task 4: Data flow page (depends: 1) [writing]
├── Task 5: Multi-Raft design page (depends: 1) [writing]
├── Task 6: Storage layer page (depends: 1) [writing]
├── Task 7: Networking page (depends: 1) [writing]

Wave 3 (After Wave 1 — user guide, PARALLEL with Wave 2):
├── Task 8: Getting started page (depends: 1) [writing]
├── Task 9: Service trait page (depends: 1) [writing]
├── Task 10: StateMachine trait page (depends: 1) [writing]
├── Task 11: Config provider page (depends: 1) [writing]
├── Task 12: Storage engines page (depends: 1) [writing]
├── Task 13: Client SDK page (depends: 1) [writing]

Wave 4 (After Wave 1 — reference + deployment, PARALLEL with Waves 2-3):
├── Task 14: API overview page (depends: 1) [writing]
├── Task 15: Proto reference page (depends: 1) [writing]
├── Task 16: Config reference page (depends: 1) [writing]
├── Task 17: Error reference page (depends: 1) [writing]
├── Task 18: Benchmarks placeholder page (depends: 1) [writing]
├── Task 19: Deployment guide page (depends: 1) [writing]

Wave 5 (After ALL — sidebar + build verification):
├── Task 20: Final sidebars.js config + build verification (depends: ALL) [quick]

Wave FINAL (After ALL tasks — independent review, 4 parallel):
├── Task F1: Plan compliance audit (oracle)
├── Task F2: Content quality review (unspecified-high)
├── Task F3: Build + link verification QA (unspecified-high)
├── Task F4: Scope fidelity check (deep)

Critical Path: T1 → T3-T19 (parallel) → T20 → F1-F4
Parallel Speedup: ~75% faster than sequential
Max Concurrent: 17 (Waves 2+3+4 all parallel after T1)
```

### Dependency Matrix

| Task | Depends On | Blocks | Wave |
|------|-----------|--------|------|
| T1 | — | T3-T20 | 1 |
| T2 | — | — | 1 |
| T3 | T1 | T20 | 2 |
| T4 | T1 | T20 | 2 |
| T5 | T1 | T20 | 2 |
| T6 | T1 | T20 | 2 |
| T7 | T1 | T20 | 2 |
| T8 | T1 | T20 | 3 |
| T9 | T1 | T20 | 3 |
| T10 | T1 | T20 | 3 |
| T11 | T1 | T20 | 3 |
| T12 | T1 | T20 | 3 |
| T13 | T1 | T20 | 3 |
| T14 | T1 | T20 | 4 |
| T15 | T1 | T20 | 4 |
| T16 | T1 | T20 | 4 |
| T17 | T1 | T20 | 4 |
| T18 | T1 | T20 | 4 |
| T19 | T1 | T20 | 4 |
| T20 | T3-T19 | F1-F4 | 5 |
| F1-F4 | T20 | — | FINAL |

### Agent Dispatch Summary

- **Wave 1**: **2** — T1 → `quick`, T2 → `quick`
- **Wave 2**: **5** — T3-T7 → `writing`
- **Wave 3**: **6** — T8-T13 → `writing`
- **Wave 4**: **6** — T14-T19 → `writing`
- **Wave 5**: **1** — T20 → `quick`
- **FINAL**: **4** — F1 → `oracle`, F2 → `unspecified-high`, F3 → `unspecified-high`, F4 → `deep`

---

## TODOs

> Implementation tasks. EVERY task MUST have: Recommended Agent Profile + Parallelization info + QA Scenarios.
> **Bilingual Format Convention**: Every page uses `## 标题 / Title` headers, then Chinese paragraph, then `---`, then English paragraph.
> **A task WITHOUT QA Scenarios is INCOMPLETE. No exceptions.**

- [x] 1. Docusaurus Scaffold + Mermaid + CJK CSS

  **What to do**:
  - Run `npx create-docusaurus@latest docs classic --typescript` in project root
  - Configure `docusaurus.config.ts`:
    - `title: 'rr Framework'`
    - `tagline: 'Distributed Multi-Raft Consensus Framework for Rust'`
    - `url: 'https://rr-framework.github.io'` (placeholder)
    - Enable docs-only mode: `routeBasePath: '/'`, disable blog
    - Add `@docusaurus/theme-mermaid` to themes
    - Configure Mermaid: `mermaid: { theme: { light: 'default', dark: 'dark' } }`
    - Set `markdown: { mermaid: true }`
  - Add CJK font stack in `src/css/custom.css`:
    ```css
    :root {
      font-family: 'Inter', 'Noto Sans CJK SC', 'PingFang SC', sans-serif;
    }
    ```
  - Create directory structure:
    ```
    docs/docs/
    ├── architecture/
    ├── guide/
    ├── reference/
    └── deployment/
    ```
  - Create `docs/docs/intro.md` as landing page with bilingual welcome content
  - Verify: `cd docs && npm run build` exits 0

  **Must NOT do**:
  - Do NOT install i18n plugin — bilingual is inline
  - Do NOT customize beyond fonts and Mermaid — keep default theme
  - Do NOT add unneeded Docusaurus plugins

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Standard scaffolding with known tool, straightforward config
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - `frontend-ui-ux`: Not needed — using default Docusaurus theme

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T2)
  - **Parallel Group**: Wave 1 (with T2)
  - **Blocks**: T3-T20
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - Docusaurus docs-only mode: https://docusaurus.io/docs/docs-introduction#docs-only-mode
  - Mermaid plugin: https://docusaurus.io/docs/markdown-features/diagrams

  **Codebase References**:
  - `examples/kv-store/Cargo.toml` — project name/description for site metadata
  - `Cargo.toml` (workspace root) — workspace member list for site metadata

  **Acceptance Criteria**:
  - [ ] `docs/` directory exists with Docusaurus project
  - [ ] `cd docs && npm run build` exits 0
  - [ ] `docs/docs/intro.md` exists with bilingual content
  - [ ] `docs/docusaurus.config.ts` has Mermaid enabled
  - [ ] `docs/src/css/custom.css` has CJK font stack
  - [ ] Directory structure for all doc sections exists

  **QA Scenarios:**

  ```
  Scenario: Docusaurus builds successfully
    Tool: Bash
    Preconditions: Node.js installed, `docs/` directory created
    Steps:
      1. cd docs && npm run build
      2. Check exit code is 0
      3. Verify `docs/build/` directory exists
    Expected Result: Build completes with exit code 0, build/ directory created
    Failure Indicators: Non-zero exit code, error messages in output
    Evidence: .sisyphus/evidence/task-1-docusaurus-build.txt

  Scenario: Intro page has bilingual content
    Tool: Bash (grep)
    Preconditions: docs/docs/intro.md created
    Steps:
      1. python3 -c "import re; t=open('docs/docs/intro.md').read(); print(len(re.findall(r'[\u4e00-\u9fff]',t)))" # Chinese chars
      2. grep -c '[a-zA-Z]' docs/docs/intro.md (English chars)
    Expected Result: Both grep commands return > 0
    Failure Indicators: Either grep returns 0
    Evidence: .sisyphus/evidence/task-1-bilingual-check.txt
  ```

  **Commit**: YES (standalone)
  - Message: `docs: scaffold Docusaurus site with Mermaid + CJK support`
  - Files: `docs/`
  - Pre-commit: `cd docs && npm run build`

- [x] 2. Root README.md

  **What to do**:
  - Create `README.md` in project root with:
    - Project name and one-line description (bilingual)
    - Feature list (bilingual bullet points):
      - Multi-Raft consensus groups
      - Automatic sharding with partition routing
      - Pluggable storage backends (MemStorage, RocksDB)
      - gRPC networking with connection pooling
      - Dynamic partition/node management
      - Client SDK with auto leader-redirect and retry
      - Snapshot transfer between nodes
    - Architecture diagram (Mermaid — crate dependency overview)
    - Quick start code snippet (from kv-store example)
    - Links to documentation site, API docs (`cargo doc`), examples
    - Build instructions (`cargo build`, `cargo test`, feature flags)
    - License section

  **Must NOT do**:
  - Do NOT duplicate the full user guide — link to it
  - Do NOT include line numbers to source files
  - Do NOT invent APIs not in the codebase

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Single file creation with known content
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1)
  - **Parallel Group**: Wave 1 (with T1)
  - **Blocks**: None
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - OpenRaft README.md (193 lines) — badge layout, feature list, getting-started links
  - TiKV README.md (213 lines) — architecture diagram, quick start with multiple deployment options

  **Codebase References**:
  - `examples/kv-store/src/main.rs` — Quick start code to excerpt
  - `Cargo.toml` — workspace members list for feature overview
  - `crates/rr-core/src/lib.rs` — core traits list for feature bullets
  - `crates/rr-raft/Cargo.toml` — openraft version for mentioning in README

  **Acceptance Criteria**:
  - [ ] `README.md` exists at project root
  - [ ] Contains both Chinese and English content
  - [ ] Contains Mermaid architecture diagram
  - [ ] Contains link to docs site
  - [ ] Contains build instructions
  - [ ] Contains quick start code snippet

  **QA Scenarios:**

  ```
  Scenario: README exists and is bilingual
    Tool: Bash
    Preconditions: README.md created
    Steps:
      1. test -f README.md && echo 'EXISTS'
      2. python3 -c "import re; t=open('README.md').read(); print(len(re.findall(r'[\u4e00-\u9fff]',t)))"
      3. grep -c 'cargo' README.md
      4. grep -c 'mermaid' README.md
    Expected Result: File exists, Chinese chars > 0, 'cargo' mentioned, mermaid block present
    Failure Indicators: File missing, no Chinese, no build instructions
    Evidence: .sisyphus/evidence/task-2-readme-check.txt
  ```

  **Commit**: YES (standalone)
  - Message: `docs: add root README.md`
  - Files: `README.md`

---

- [x] 3. Architecture Overview + Crate Map Pages

  **What to do**:
  - Create `docs/docs/architecture/overview.md` — high-level architecture page:
    - What is rr: a distributed multi-Raft consensus framework for Rust
    - Design philosophy: modular crates, trait-based abstraction, pluggable backends
    - Mermaid diagram: overall system architecture (Client → Router → Raft Groups → Storage)
    - Key concepts: Node, Group, Partition, Leader, Follower
    - Comparison with other Raft implementations (brief)
  - Create `docs/docs/architecture/crate-map.md` — crate dependency map:
    - Mermaid diagram: crate dependency graph (rr-core at root, rr-server at top)
    - Per-crate table: name, purpose, key types, dependencies
    - Layer description: Foundation → Infrastructure → Orchestration
    - Feature flags table: `rocks` and its effect on each crate
  - Both pages fully bilingual (Chinese paragraph → English paragraph)

  **Must NOT do**:
  - Do NOT list every struct/method — link to cargo doc for API details
  - Do NOT reference line numbers in source files

  **Recommended Agent Profile**:
  - **Category**: `writing`
    - Reason: Documentation writing with technical architecture content
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T4-T7)
  - **Parallel Group**: Wave 2
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Pattern References**:
  - TiKV architecture overview: https://tikv.org/deep-dive/overview/
  - OpenRaft component docs: `openraft/src/docs/components/`

  **Codebase References**:
  - `Cargo.toml` — workspace members, crate list
  - `crates/rr-core/src/lib.rs` — re-exports showing core abstractions (types, traits, errors)
  - `crates/rr-server/src/lib.rs` — top-level orchestration exports
  - `crates/rr-raft/Cargo.toml` — dependency on openraft 0.9.21
  - `crates/rr-storage/Cargo.toml` — optional `rocks` feature definition

  **Acceptance Criteria**:
  - [ ] `docs/docs/architecture/overview.md` exists with bilingual content
  - [ ] `docs/docs/architecture/crate-map.md` exists with bilingual content
  - [ ] Both pages have Mermaid diagrams
  - [ ] `cd docs && npm run build` still exits 0

  **QA Scenarios:**

  ```
  Scenario: Architecture pages exist and are bilingual with diagrams
    Tool: Bash
    Preconditions: Docusaurus scaffold exists (T1)
    Steps:
      1. test -f docs/docs/architecture/overview.md && echo 'EXISTS'
      2. test -f docs/docs/architecture/crate-map.md && echo 'EXISTS'
      3. grep -c 'mermaid' docs/docs/architecture/overview.md
      4. grep -c 'mermaid' docs/docs/architecture/crate-map.md
      5. python3 -c "import re; t=open('docs/docs/architecture/overview.md').read(); print(len(re.findall(r'[\u4e00-\u9fff]',t)))"
      6. cd docs && npm run build
    Expected Result: Both files exist, both have mermaid blocks, Chinese content present, build succeeds
    Failure Indicators: Files missing, no diagrams, build fails
    Evidence: .sisyphus/evidence/task-3-architecture-pages.txt
  ```

  **Commit**: YES (groups with T4-T7)
  - Message: `docs: add architecture documentation (6 pages)`
  - Files: `docs/docs/architecture/*.md`

- [x] 4. Data Flow Page

  **What to do**:
  - Create `docs/docs/architecture/data-flow.md`:
    - Mermaid sequence diagram: complete write request lifecycle
      - Client → gRPC ClientRequest → ClientDispatchImpl → ShardRouter::route()
      - → RaftGroupManager::get_raft() → raft.client_write() → log replication
      - → RrStateMachine::apply() → user StateMachine::apply() → response
    - Mermaid sequence diagram: leader election flow
      - raft.initialize() → vote requests → majority → leader established
    - Read request path (if applicable)
    - Error handling flow: ForwardToLeader, redirect, retry
  - Fully bilingual

  **Must NOT do**:
  - Do NOT include implementation details beyond function signatures
  - Do NOT reference line numbers

  **Recommended Agent Profile**:
  - **Category**: `writing`
    - Reason: Technical writing with sequence diagrams
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T3, T5-T7)
  - **Parallel Group**: Wave 2
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-server/src/dispatch.rs` — ClientDispatchImpl::submit_write() flow
  - `crates/rr-router/src/router.rs` — ShardRouter::route() and get_leader_addr()
  - `crates/rr-raft/src/group_manager.rs` — RaftGroupManager::get_raft(), init_cluster()
  - `crates/rr-raft/src/sm_adapter.rs` — RrStateMachine::apply() state machine adapter
  - `crates/rr-network/src/client_service.rs` — ClientServiceImpl::request() gRPC handler
  - `crates/rr-client/src/lib.rs` — RrClient::send_request() retry/redirect logic

  **Acceptance Criteria**:
  - [ ] `docs/docs/architecture/data-flow.md` exists with bilingual content
  - [ ] Contains at least 2 Mermaid sequence diagrams
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: Data flow page has sequence diagrams
    Tool: Bash
    Preconditions: Docusaurus scaffold exists (T1)
    Steps:
      1. test -f docs/docs/architecture/data-flow.md && echo 'EXISTS'
      2. grep -c 'sequenceDiagram' docs/docs/architecture/data-flow.md
      3. python3 -c "import re; t=open('docs/docs/architecture/data-flow.md').read(); print(len(re.findall(r'[\u4e00-\u9fff]',t)))"
    Expected Result: File exists, ≥ 2 sequence diagrams, Chinese content present
    Failure Indicators: File missing, no diagrams
    Evidence: .sisyphus/evidence/task-4-data-flow.txt
  ```

  **Commit**: YES (groups with T3, T5-T7)

- [x] 5. Multi-Raft Design Page

  **What to do**:
  - Create `docs/docs/architecture/multi-raft.md`:
    - What is Multi-Raft: multiple independent Raft consensus groups per node
    - Why Multi-Raft: scalability, partition independence, failure isolation
    - Mermaid diagram: multiple Raft groups on a single node
    - RaftGroupManager: lifecycle management of groups (start_group, stop_group)
    - Partition-to-group mapping: PartitionConfig, GroupMeta, RoutingTable
    - openraft integration: RrTypeConfig, how openraft is wrapped
    - Dynamic membership: add_learner, change_membership
  - Fully bilingual

  **Must NOT do**:
  - Do NOT explain basic Raft consensus — link to Raft paper
  - Do NOT duplicate openraft documentation

  **Recommended Agent Profile**:
  - **Category**: `writing`
    - Reason: Technical writing about distributed systems design
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T3-T4, T6-T7)
  - **Parallel Group**: Wave 2
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-raft/src/group_manager.rs` — RaftGroupManager: start_group(), stop_group(), group_ids()
  - `crates/rr-raft/src/raft_node.rs` — RaftNode wrapping openraft::Raft
  - `crates/rr-raft/src/type_config.rs` — RrTypeConfig definition
  - `crates/rr-core/src/types.rs` — NodeId, GroupId, PartitionId, PartitionConfig, GroupMeta, RoutingTable
  - `crates/rr-core/src/config.rs` — ConfigProvider trait, RoutingTableChange enum

  **Acceptance Criteria**:
  - [ ] `docs/docs/architecture/multi-raft.md` exists with bilingual content
  - [ ] Contains Mermaid diagram showing multi-group architecture
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: Multi-Raft page exists with diagram
    Tool: Bash
    Steps:
      1. test -f docs/docs/architecture/multi-raft.md && echo 'EXISTS'
      2. grep -c 'mermaid' docs/docs/architecture/multi-raft.md
      3. python3 -c "import re; t=open('docs/docs/architecture/multi-raft.md').read(); print(len(re.findall(r'[\u4e00-\u9fff]',t)))"
    Expected Result: File exists, has mermaid block, Chinese present
    Evidence: .sisyphus/evidence/task-5-multi-raft.txt
  ```

  **Commit**: YES (groups with T3-T4, T6-T7)

- [x] 6. Storage Layer Page

  **What to do**:
  - Create `docs/docs/architecture/storage.md`:
    - StorageEngine trait: put, get, delete, scan, write_batch, flush, create_cf
    - Two implementations: MemStorage (default) and RocksStorage (feature-gated `rocks`)
    - RocksDB organization: column families (raft_log, state_machine, metadata)
    - Key encoding: big-endian u64 for lexicographic ordering
    - Serialization: bincode v2 for all Raft types
    - Raft log storage: RrLogStorage (in-memory) vs RocksLogStorage (persistent)
    - State machine storage: how StateMachine trait interacts with StorageEngine
    - Mermaid diagram: storage layer architecture
  - Fully bilingual

  **Must NOT do**:
  - Do NOT document RocksDB internals — link to RocksDB docs
  - Do NOT include bincode serialization code details

  **Recommended Agent Profile**:
  - **Category**: `writing`
    - Reason: Technical writing about storage architecture
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T3-T5, T7)
  - **Parallel Group**: Wave 2
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-core/src/storage.rs` — StorageEngine trait definition
  - `crates/rr-storage/src/mem.rs` — MemStorage implementation
  - `crates/rr-storage/src/rocks.rs` — RocksStorage, RocksConfig implementation
  - `crates/rr-raft/src/log_storage.rs` — RrLogStorage (in-memory Raft log)
  - `crates/rr-raft/src/rocks_log_storage.rs` — RocksLogStorage (persistent Raft log)
  - `crates/rr-raft/src/sm_adapter.rs` — RrStateMachine, snapshot handling

  **Acceptance Criteria**:
  - [ ] `docs/docs/architecture/storage.md` exists with bilingual content
  - [ ] Contains Mermaid diagram
  - [ ] Covers both MemStorage and RocksStorage
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: Storage page covers both backends
    Tool: Bash
    Steps:
      1. test -f docs/docs/architecture/storage.md && echo 'EXISTS'
      2. grep -c 'MemStorage' docs/docs/architecture/storage.md
      3. grep -c 'RocksStorage' docs/docs/architecture/storage.md
      4. grep -c 'mermaid' docs/docs/architecture/storage.md
    Expected Result: File exists, mentions both storage backends, has diagram
    Evidence: .sisyphus/evidence/task-6-storage.txt
  ```

  **Commit**: YES (groups with T3-T5, T7)

- [x] 7. Networking Page

  **What to do**:
  - Create `docs/docs/architecture/networking.md`:
    - gRPC transport layer: tonic-based, protobuf definitions
    - Raft RPCs: AppendEntries, Vote, InstallSnapshot — bincode payloads over proto
    - Client RPCs: ClientRequest/ClientResponse
    - GrpcNetworkFactory: implements openraft RaftNetworkFactory
    - ChannelPool: lock-free channel reuse via DashMap
    - RaftGroupLookup trait: dispatching incoming RPCs to correct Raft group
    - ClientDispatch trait: handling client write requests
    - RaftServiceImpl and ClientServiceImpl: gRPC service implementations
    - Mermaid diagram: networking architecture
  - Fully bilingual

  **Must NOT do**:
  - Do NOT include protobuf file contents verbatim — summarize
  - Do NOT document tonic/prost internals

  **Recommended Agent Profile**:
  - **Category**: `writing`
    - Reason: Technical writing about networking architecture
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T3-T6)
  - **Parallel Group**: Wave 2
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-network/src/raft_service.rs` — RaftGroupLookup trait, RaftServiceImpl
  - `crates/rr-network/src/client_service.rs` — ClientDispatch trait, ClientServiceImpl
  - `crates/rr-network/src/channel_pool.rs` — ChannelPool implementation
  - `crates/rr-raft/src/network_factory.rs` — GrpcNetworkFactory, GrpcRaftNetwork
  - `crates/rr-network/proto/` — protobuf definitions for Raft and Client RPCs
  - `crates/rr-server/src/lookup.rs` — RaftGroupLookupImpl
  - `crates/rr-server/src/dispatch.rs` — ClientDispatchImpl

  **Acceptance Criteria**:
  - [ ] `docs/docs/architecture/networking.md` exists with bilingual content
  - [ ] Contains Mermaid diagram
  - [ ] Covers gRPC, ChannelPool, both service traits
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: Networking page covers key components
    Tool: Bash
    Steps:
      1. test -f docs/docs/architecture/networking.md && echo 'EXISTS'
      2. grep -c 'ChannelPool' docs/docs/architecture/networking.md
      3. grep -c 'gRPC\|grpc\|tonic' docs/docs/architecture/networking.md
      4. grep -c 'mermaid' docs/docs/architecture/networking.md
    Expected Result: File exists, mentions ChannelPool, gRPC, has diagram
    Evidence: .sisyphus/evidence/task-7-networking.txt
  ```

  **Commit**: YES (groups with T3-T6)
  - Message: `docs: add architecture documentation (6 pages)`
  - Files: `docs/docs/architecture/*.md`
  - Pre-commit: `cd docs && npm run build`


- [x] 8. Getting Started Page

  **What to do**:
  - Create `docs/docs/guide/getting-started.md`:
    - Prerequisites: Rust toolchain, protobuf compiler (protoc)
    - Installation: add rr crates to Cargo.toml
    - Minimal example: single-node KV store (based on kv-store example)
    - Step-by-step walkthrough:
      1. Define request/response types (KvRequest, KvResponse)
      2. Implement the Service trait (or StateMachine trait)
      3. Create NodeConfig with state machine factory
      4. Start RrNode and initialize Raft cluster
      5. Submit writes through Raft
    - Running the example: `cargo run -p kv-store`
    - Multi-node mode: `--node-id`, `--port`, `--peers`, `--bootstrap` flags
    - Code snippets from actual kv-store example (NOT invented)
  - Fully bilingual

  **Must NOT do**:
  - Do NOT invent code examples — excerpt from actual kv-store
  - Do NOT reference line numbers

  **Recommended Agent Profile**:
  - **Category**: `writing`
    - Reason: Tutorial/guide writing with code excerpts
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T9-T13)
  - **Parallel Group**: Wave 3
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Pattern References**:
  - OpenRaft getting-started.md (518 lines) — step-by-step implementation tutorial

  **Codebase References**:
  - `examples/kv-store/src/main.rs` — Complete example including CLI args parsing, node config, startup
  - `examples/kv-store/src/service.rs` — KvService implementing Service trait
  - `examples/kv-store/src/state_machine.rs` — KvStateMachine implementing StateMachine trait
  - `examples/kv-store/src/types.rs` — KvRequest, KvResponse type definitions
  - `crates/rr-server/src/config.rs` — NodeConfig builder API

  **Acceptance Criteria**:
  - [ ] `docs/docs/guide/getting-started.md` exists with bilingual content
  - [ ] Contains code snippets from actual kv-store example
  - [ ] Contains step-by-step instructions
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: Getting started page has code examples
    Tool: Bash
    Steps:
      1. test -f docs/docs/guide/getting-started.md && echo 'EXISTS'
      2. grep -c 'rust' docs/docs/guide/getting-started.md  # code blocks
      3. grep -c 'cargo' docs/docs/guide/getting-started.md
      4. python3 -c "import re; t=open('docs/docs/guide/getting-started.md').read(); print(len(re.findall(r'[\u4e00-\u9fff]',t)))"
    Expected Result: File exists, has Rust code blocks, mentions cargo, bilingual
    Evidence: .sisyphus/evidence/task-8-getting-started.txt
  ```

  **Commit**: YES (groups with T9-T13)
  - Message: `docs: add user guide documentation (6 pages)`
  - Files: `docs/docs/guide/*.md`

- [x] 9. Service Trait Page

  **What to do**:
  - Create `docs/docs/guide/service-trait.md`:
    - What is the Service trait: high-level user-facing API
    - Difference between Service and StateMachine traits
    - Service trait methods: `handle`, `name`, `snapshot`, `restore`
    - ServiceStateMachineAdapter: bridging Service to openraft's StateMachine
    - Complete implementation example (from kv-store KvService)
    - When to use Service vs StateMachine (decision guide)
  - Fully bilingual

  **Must NOT do**:
  - Do NOT duplicate the Getting Started page content

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T8, T10-T13)
  - **Parallel Group**: Wave 3
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-core/src/service.rs` — Service trait definition
  - `crates/rr-core/src/state_machine.rs` — StateMachine trait for comparison
  - `crates/rr-raft/src/service_adapter.rs` — ServiceStateMachineAdapter bridging logic
  - `examples/kv-store/src/service.rs` — KvService implementation example

  **Acceptance Criteria**:
  - [ ] `docs/docs/guide/service-trait.md` exists with bilingual content
  - [ ] Covers both Service and StateMachine traits
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: Service trait page covers both traits
    Tool: Bash
    Steps:
      1. test -f docs/docs/guide/service-trait.md && echo 'EXISTS'
      2. grep -c 'Service' docs/docs/guide/service-trait.md
      3. grep -c 'StateMachine' docs/docs/guide/service-trait.md
    Expected Result: File exists, mentions both Service and StateMachine
    Evidence: .sisyphus/evidence/task-9-service-trait.txt
  ```

  **Commit**: YES (groups with T8, T10-T13)

- [x] 10. StateMachine Trait Page

  **What to do**:
  - Create `docs/docs/guide/state-machine-trait.md`:
    - What is the StateMachine trait: low-level Raft state machine interface
    - Methods: `apply`, `snapshot`, `restore`, `last_applied`
    - How it integrates with openraft via RrStateMachine adapter
    - Snapshot lifecycle: build → transfer → restore
    - Complete implementation example (from kv-store KvStateMachine)
    - Binary serialization: bincode v2 patterns
  - Fully bilingual

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T8-T9, T11-T13)
  - **Parallel Group**: Wave 3
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-core/src/state_machine.rs` — StateMachine trait definition
  - `crates/rr-raft/src/sm_adapter.rs` — RrStateMachine, RrSnapshotBuilder, snapshot handling
  - `examples/kv-store/src/state_machine.rs` — KvStateMachine implementation
  - `crates/rr-core/src/codec.rs` — BincodeCodec for serialization

  **Acceptance Criteria**:
  - [ ] `docs/docs/guide/state-machine-trait.md` exists with bilingual content
  - [ ] Covers apply, snapshot, restore methods
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: StateMachine page covers key methods
    Tool: Bash
    Steps:
      1. test -f docs/docs/guide/state-machine-trait.md && echo 'EXISTS'
      2. grep -c 'apply' docs/docs/guide/state-machine-trait.md
      3. grep -c 'snapshot' docs/docs/guide/state-machine-trait.md
    Expected Result: File exists, covers apply and snapshot methods
    Evidence: .sisyphus/evidence/task-10-state-machine.txt
  ```

  **Commit**: YES (groups with T8-T9, T11-T13)

- [x] 11. Config Provider Page

  **What to do**:
  - Create `docs/docs/guide/config-provider.md`:
    - ConfigProvider trait: get_routing_table, watch_changes, get_node_addr
    - RoutingTable: partitions, groups, nodes
    - RoutingTableChange enum: PartitionAdded, PartitionRemoved, NodeAdded, NodeRemoved, etc.
    - MemConfigProvider: in-memory implementation for testing/development
    - How to implement a custom ConfigProvider (e.g., etcd-backed, ZooKeeper-backed)
    - Dynamic reconfiguration: watch loop pattern in RrNode
  - Fully bilingual

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T8-T10, T12-T13)
  - **Parallel Group**: Wave 3
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-core/src/config.rs` — ConfigProvider trait, RoutingTableChange enum
  - `crates/rr-core/src/types.rs` — RoutingTable, PartitionConfig, GroupMeta
  - `crates/rr-config/src/mem.rs` — MemConfigProvider implementation
  - `crates/rr-server/src/node.rs` — Watch loop pattern (watch_changes stream handling)

  **Acceptance Criteria**:
  - [ ] `docs/docs/guide/config-provider.md` exists with bilingual content
  - [ ] Covers ConfigProvider trait and RoutingTableChange
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: Config page covers ConfigProvider trait
    Tool: Bash
    Steps:
      1. test -f docs/docs/guide/config-provider.md && echo 'EXISTS'
      2. grep -c 'ConfigProvider' docs/docs/guide/config-provider.md
      3. grep -c 'RoutingTableChange\|watch_changes' docs/docs/guide/config-provider.md
    Expected Result: File exists, covers ConfigProvider and dynamic changes
    Evidence: .sisyphus/evidence/task-11-config-provider.txt
  ```

  **Commit**: YES (groups with T8-T10, T12-T13)

- [x] 12. Storage Engines Page

  **What to do**:
  - Create `docs/docs/guide/storage-engines.md`:
    - StorageEngine trait overview (user perspective, not internals)
    - Using MemStorage: default, no setup needed, data lost on restart
    - Using RocksStorage: enable `rocks` feature, persistent, production-ready
    - RocksConfig: path, create_if_missing, other options
    - Feature flag: `--features rocks` for cargo build/test
    - How to implement a custom StorageEngine
    - Code examples for both MemStorage and RocksStorage usage
  - Fully bilingual

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T8-T11, T13)
  - **Parallel Group**: Wave 3
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-core/src/storage.rs` — StorageEngine trait
  - `crates/rr-storage/src/mem.rs` — MemStorage
  - `crates/rr-storage/src/rocks.rs` — RocksStorage, RocksConfig
  - `crates/rr-storage/Cargo.toml` — `rocks` feature definition

  **Acceptance Criteria**:
  - [ ] `docs/docs/guide/storage-engines.md` exists with bilingual content
  - [ ] Covers both MemStorage and RocksStorage
  - [ ] Documents `rocks` feature flag
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: Storage engines page covers both backends
    Tool: Bash
    Steps:
      1. test -f docs/docs/guide/storage-engines.md && echo 'EXISTS'
      2. grep -c 'MemStorage' docs/docs/guide/storage-engines.md
      3. grep -c 'RocksStorage\|RocksDB' docs/docs/guide/storage-engines.md
      4. grep -c 'rocks' docs/docs/guide/storage-engines.md
    Expected Result: Both backends mentioned, feature flag documented
    Evidence: .sisyphus/evidence/task-12-storage-engines.txt
  ```

  **Commit**: YES (groups with T8-T11, T13)

- [x] 13. Client SDK Page

  **What to do**:
  - Create `docs/docs/guide/client-sdk.md`:
    - RrClient: connecting to a cluster
    - Connection methods: `connect(addrs)`, `connect_with_config(addrs, retry_config)`
    - Sending requests: `send_request(group_id, payload)`, `send_request_with_partition(partition_id, payload)`
    - RetryConfig: max_retries, retry_delay_ms, defaults
    - Auto leader-redirect: how it works, leader cache
    - Cluster info: `get_cluster_info()`
    - Error handling: what errors can occur, how to handle them
    - Code example: connecting and sending KV operations
  - Fully bilingual

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T8-T12)
  - **Parallel Group**: Wave 3
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-client/src/lib.rs` — RrClient, RetryConfig, all client methods
  - `crates/rr-core/src/error.rs` — RrError, NetworkError for client error handling
  - `tests/integration/src/lib.rs` — test_client_write_read, test_client_redirect examples

  **Acceptance Criteria**:
  - [ ] `docs/docs/guide/client-sdk.md` exists with bilingual content
  - [ ] Covers RrClient, RetryConfig, auto-redirect
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**

  ```
  Scenario: Client SDK page covers key APIs
    Tool: Bash
    Steps:
      1. test -f docs/docs/guide/client-sdk.md && echo 'EXISTS'
      2. grep -c 'RrClient' docs/docs/guide/client-sdk.md
      3. grep -c 'RetryConfig' docs/docs/guide/client-sdk.md
      4. grep -c 'send_request' docs/docs/guide/client-sdk.md
    Expected Result: File exists, covers RrClient, RetryConfig, send methods
    Evidence: .sisyphus/evidence/task-13-client-sdk.txt
  ```

  **Commit**: YES (groups with T8-T12)
  - Message: `docs: add user guide documentation (6 pages)`
  - Files: `docs/docs/guide/*.md`
  - Pre-commit: `cd docs && npm run build`

- [x] 14. API Overview Page

  **What to do**:
  - Create `docs/docs/reference/api-overview.md`:
    - Per-crate summary table: crate name, description, key types, key traits
    - Link to `cargo doc` generated rustdoc for each crate
    - Core traits overview with method signatures:
      - StateMachine: apply, snapshot, restore, last_applied
      - Service: handle, name, snapshot, restore
      - StorageEngine: put, get, delete, scan, write_batch, flush, create_cf
      - ConfigProvider: get_routing_table, watch_changes, get_node_addr
    - Type aliases: NodeId, GroupId, PartitionId, LogIndex
    - Instructions for generating local rustdoc: `cargo doc --workspace --no-deps --open`
  - Fully bilingual

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T15-T19)
  - **Parallel Group**: Wave 4
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-core/src/lib.rs` — all re-exports (types, traits, errors)
  - `crates/rr-core/src/state_machine.rs` — StateMachine trait signatures
  - `crates/rr-core/src/service.rs` — Service trait signatures
  - `crates/rr-core/src/storage.rs` — StorageEngine trait signatures
  - `crates/rr-core/src/config.rs` — ConfigProvider trait signatures
  - All `crates/*/Cargo.toml` — crate descriptions

  **Acceptance Criteria**:
  - [ ] `docs/docs/reference/api-overview.md` exists with bilingual content
  - [ ] Contains per-crate summary table
  - [ ] Links to cargo doc
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**
  ```
  Scenario: API overview covers all crates
    Tool: Bash
    Steps:
      1. test -f docs/docs/reference/api-overview.md && echo 'EXISTS'
      2. grep -c 'rr-core\|rr-raft\|rr-server\|rr-client' docs/docs/reference/api-overview.md
      3. grep -c 'cargo doc' docs/docs/reference/api-overview.md
    Expected Result: File exists, mentions major crates, links to cargo doc
    Evidence: .sisyphus/evidence/task-14-api-overview.txt
  ```

  **Commit**: YES (groups with T15-T19)

- [x] 15. Proto Reference Page

  **What to do**:
  - Create `docs/docs/reference/proto-reference.md`:
    - Protobuf message definitions for Raft RPCs:
      - AppendEntriesRequest/Response, VoteRequest/Response
      - InstallSnapshotRequest (ProtoSnapshotChunk)
    - Protobuf message definitions for Client RPCs:
      - ClientRequest/ClientResponse
    - How bincode payloads are wrapped in proto messages
    - Proto file locations
  - Fully bilingual

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T14, T16-T19)
  - **Parallel Group**: Wave 4
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-network/proto/` — .proto files with message definitions
  - `crates/rr-network/src/lib.rs` — proto module re-exports
  - `crates/rr-raft/src/network_factory.rs` — how proto messages are constructed/parsed

  **Acceptance Criteria**:
  - [ ] `docs/docs/reference/proto-reference.md` exists with bilingual content
  - [ ] Covers both Raft and Client proto messages
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**
  ```
  Scenario: Proto reference covers both RPC types
    Tool: Bash
    Steps:
      1. test -f docs/docs/reference/proto-reference.md && echo 'EXISTS'
      2. grep -c 'AppendEntries\|Vote\|InstallSnapshot' docs/docs/reference/proto-reference.md
      3. grep -c 'ClientRequest\|ClientResponse' docs/docs/reference/proto-reference.md
    Expected Result: Both Raft and Client RPCs documented
    Evidence: .sisyphus/evidence/task-15-proto-reference.txt
  ```

  **Commit**: YES (groups with T14, T16-T19)

- [x] 16. Config Reference Page

  **What to do**:
  - Create `docs/docs/reference/config-reference.md`:
    - NodeConfig fields table: field name, type, required/optional, description
      - node_id, listen_addr, storage_engine, config_provider, state_machine_factory
      - raft_lookup, client_dispatch, group_manager
    - RetryConfig fields: max_retries (default: 3), retry_delay_ms (default: 100)
    - RocksConfig fields: path, create_if_missing, other RocksDB options
    - openraft Config parameters: heartbeat_interval, election_timeout_min/max
    - RoutingTable structure: partitions HashMap, nodes HashMap
  - Format as tables for quick reference
  - Fully bilingual

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T14-T15, T17-T19)
  - **Parallel Group**: Wave 4
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-server/src/config.rs` — NodeConfig struct and builder
  - `crates/rr-client/src/lib.rs` — RetryConfig with defaults
  - `crates/rr-storage/src/rocks.rs` — RocksConfig
  - `crates/rr-core/src/types.rs` — RoutingTable, PartitionConfig, GroupMeta

  **Acceptance Criteria**:
  - [ ] `docs/docs/reference/config-reference.md` exists with bilingual content
  - [ ] Contains configuration tables
  - [ ] Covers NodeConfig, RetryConfig, RocksConfig
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**
  ```
  Scenario: Config reference has tables
    Tool: Bash
    Steps:
      1. test -f docs/docs/reference/config-reference.md && echo 'EXISTS'
      2. grep -c 'NodeConfig' docs/docs/reference/config-reference.md
      3. grep -c 'RetryConfig' docs/docs/reference/config-reference.md
      4. grep -c '|' docs/docs/reference/config-reference.md  # markdown tables
    Expected Result: File exists, covers configs, has markdown tables
    Evidence: .sisyphus/evidence/task-16-config-reference.txt
  ```

  **Commit**: YES (groups with T14-T15, T17-T19)

- [x] 17. Error Reference Page

  **What to do**:
  - Create `docs/docs/reference/error-reference.md`:
    - RrError enum: all variants with description
      - RaftError, StorageError, ConfigError, RouterError, NetworkError, CodecError
    - Per-variant detail: when it occurs, how to handle it, example
    - Error propagation: which crate returns which errors
    - Common error scenarios and troubleshooting
  - Fully bilingual

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T14-T16, T18-T19)
  - **Parallel Group**: Wave 4
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `crates/rr-core/src/error.rs` — RrError enum, all error variants

  **Acceptance Criteria**:
  - [ ] `docs/docs/reference/error-reference.md` exists with bilingual content
  - [ ] Covers all RrError variants
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**
  ```
  Scenario: Error page covers all variants
    Tool: Bash
    Steps:
      1. test -f docs/docs/reference/error-reference.md && echo 'EXISTS'
      2. grep -c 'RrError' docs/docs/reference/error-reference.md
      3. grep -c 'RaftError\|StorageError\|ConfigError\|RouterError\|NetworkError' docs/docs/reference/error-reference.md
    Expected Result: File exists, covers RrError and its sub-errors
    Evidence: .sisyphus/evidence/task-17-error-reference.txt
  ```

  **Commit**: YES (groups with T14-T16, T18-T19)

- [x] 18. Benchmarks Placeholder Page

  **What to do**:
  - Create `docs/docs/reference/benchmarks.md`:
    - Current status: benchmarks not yet implemented
    - What to benchmark: write throughput, read latency, leader election time, snapshot transfer time
    - How to benchmark: guidance on using criterion.rs, tokio tracing
    - Expected performance characteristics based on architecture
    - Link to openraft benchmarks for reference
  - Fully bilingual

  **Must NOT do**:
  - Do NOT write actual benchmark code — placeholder only
  - Do NOT make up performance numbers

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T14-T17, T19)
  - **Parallel Group**: Wave 4
  - **Blocks**: T20
  - **Blocked By**: T1

  **Acceptance Criteria**:
  - [ ] `docs/docs/reference/benchmarks.md` exists with bilingual content
  - [ ] Does NOT contain made-up numbers
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**
  ```
  Scenario: Benchmarks page is placeholder
    Tool: Bash
    Steps:
      1. test -f docs/docs/reference/benchmarks.md && echo 'EXISTS'
      2. python3 -c "import re; t=open('docs/docs/reference/benchmarks.md').read(); print(len(re.findall(r'[\u4e00-\u9fff]',t)))"
    Expected Result: File exists, is bilingual, no fake numbers
    Evidence: .sisyphus/evidence/task-18-benchmarks.txt
  ```

  **Commit**: YES (groups with T14-T17, T19)

- [x] 19. Deployment Guide Page

  **What to do**:
  - Create `docs/docs/deployment/deployment.md`:
    - Building for production: `cargo build --release --features rocks`
    - Node configuration: setting up NodeConfig for production
    - Multi-node deployment: network configuration, listen addresses, peer discovery
    - Running a 3-node cluster: step-by-step with kv-store example
    - Monitoring: what metrics are exposed (openraft metrics, custom metrics)
    - Backup/restore: using RocksDB snapshots
    - Scaling: adding/removing nodes dynamically via ConfigProvider
    - Generic deployment guidance (not Docker/K8s specific since no infra exists)
  - Fully bilingual

  **Must NOT do**:
  - Do NOT create Dockerfile or K8s manifests
  - Do NOT assume specific deployment target

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T14-T18)
  - **Parallel Group**: Wave 4
  - **Blocks**: T20
  - **Blocked By**: T1

  **References**:

  **Codebase References**:
  - `examples/kv-store/src/main.rs` — Multi-node CLI args for deployment reference
  - `crates/rr-server/src/config.rs` — NodeConfig for production setup
  - `crates/rr-server/src/node.rs` — RrNode lifecycle for operations guidance
  - `crates/rr-storage/src/rocks.rs` — RocksConfig for production storage

  **Acceptance Criteria**:
  - [ ] `docs/docs/deployment/deployment.md` exists with bilingual content
  - [ ] Covers build, multi-node setup, monitoring, scaling
  - [ ] Does NOT contain Docker/K8s manifests
  - [ ] `cd docs && npm run build` exits 0

  **QA Scenarios:**
  ```
  Scenario: Deployment page covers production setup
    Tool: Bash
    Steps:
      1. test -f docs/docs/deployment/deployment.md && echo 'EXISTS'
      2. grep -c 'release' docs/docs/deployment/deployment.md
      3. grep -c 'cargo build' docs/docs/deployment/deployment.md
      4. grep -v 'Dockerfile\|docker\|kubernetes\|k8s' docs/docs/deployment/deployment.md | wc -l
    Expected Result: File exists, covers release build, no Docker/K8s content
    Evidence: .sisyphus/evidence/task-19-deployment.txt
  ```

  **Commit**: YES (groups with T14-T18)
  - Message: `docs: add reference + deployment documentation (6 pages)`
  - Files: `docs/docs/reference/*.md`, `docs/docs/deployment/*.md`
  - Pre-commit: `cd docs && npm run build`

- [x] 20. Final Sidebar Config + Build Verification

  **What to do**:
  - Update `docs/sidebars.js` (or `sidebars.ts`) to correctly reference all 19 pages:
    - Architecture section: overview, crate-map, data-flow, multi-raft, storage, networking
    - User Guide section: getting-started, service-trait, state-machine-trait, config-provider, storage-engines, client-sdk
    - Reference section: api-overview, proto-reference, config-reference, error-reference, benchmarks
    - Deployment section: deployment
  - Run `cd docs && npm run build` and verify:
    - Exit code 0
    - No broken links in output
    - All 19 pages accessible
  - Run `npm run serve` briefly to verify pages render
  - Verify `cargo doc --workspace --no-deps` still builds (docs don't break code)

  **Must NOT do**:
  - Do NOT modify any documentation content — only sidebar config

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Configuration file update + verification
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO (depends on ALL content tasks)
  - **Parallel Group**: Wave 5 (sequential, after all content)
  - **Blocks**: F1-F4
  - **Blocked By**: T3-T19

  **Acceptance Criteria**:
  - [ ] `sidebars.js` references all 19 pages
  - [ ] `cd docs && npm run build` exits 0 with no broken links
  - [ ] `cargo doc --workspace --no-deps` exits 0
  - [ ] All 4 sections visible in sidebar

  **QA Scenarios:**

  ```
  Scenario: Full build succeeds with all pages
    Tool: Bash
    Steps:
      1. cd docs && npm run build
      2. echo $?
      3. find docs/docs -name '*.md' | wc -l
      4. cargo doc --workspace --no-deps
    Expected Result: Build exit 0, 19 markdown files, cargo doc succeeds
    Failure Indicators: Build errors, missing pages, broken links
    Evidence: .sisyphus/evidence/task-20-final-build.txt

  Scenario: Sidebar has all sections
    Tool: Bash
    Steps:
      1. grep -c 'architecture\|guide\|reference\|deployment' docs/sidebars.js
    Expected Result: All 4 sections referenced in sidebar
    Evidence: .sisyphus/evidence/task-20-sidebar-check.txt
  ```

  **Commit**: YES (standalone)
  - Message: `docs: finalize sidebar navigation and build verification`
  - Files: `docs/sidebars.js`
  - Pre-commit: `cd docs && npm run build`

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Rejection → fix → re-run.

- [ ] F1. **Plan Compliance Audit** — `oracle`
  Read the plan end-to-end. For each "Must Have": verify implementation exists (check file exists, check content). For each "Must NOT Have": search docs for forbidden patterns (line numbers, invented APIs, Docker manifests). Check evidence files exist in `.sisyphus/evidence/`. Compare deliverables against plan.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [N/N] | VERDICT: APPROVE/REJECT`

- [ ] F2. **Content Quality Review** — `unspecified-high`
  Read every `.md` file in `docs/docs/`. Verify: both Chinese and English present per page, Mermaid diagrams render (syntax check), no broken internal links (build output), no placeholder text ("TODO", "TBD", "Lorem"), no machine-translation artifacts. Check bilingual format consistency.
  Output: `Pages [N/N bilingual] | Mermaid [N diagrams valid] | Links [N/N] | VERDICT`

- [ ] F3. **Build + Link Verification QA** — `unspecified-high`
  Run `cd docs && npm run build`. Verify exit code 0. Check build output for warnings. Run `npm run serve` and use Playwright to navigate every page. Verify sidebar matches all 19 pages. Take screenshots of key pages.
  Output: `Build [PASS/FAIL] | Pages [N/N accessible] | Sidebar [PASS/FAIL] | VERDICT`

- [ ] F4. **Scope Fidelity Check** — `deep`
  For each task: read "What to do", verify the file was created with correct content. Check every page matches the planned topic. Verify no pages were skipped. Check no scope creep (extra unplanned pages). Verify README.md links to docs.
  Output: `Tasks [N/N compliant] | Extra Pages [CLEAN/N] | README [PASS/FAIL] | VERDICT`

---

## Commit Strategy

- **After T1**: `docs: scaffold Docusaurus site with Mermaid + CJK support`
- **After T2**: `docs: add root README.md`
- **After Wave 2 (T3-T7)**: `docs: add architecture documentation (6 pages)`
- **After Wave 3 (T8-T13)**: `docs: add user guide documentation (6 pages)`
- **After Wave 4 (T14-T19)**: `docs: add reference + deployment documentation (6 pages)`
- **After T20**: `docs: finalize sidebar navigation and build verification`

---

## Success Criteria

### Verification Commands
```bash
cd docs && npm run build  # Expected: exit 0, no errors
find docs/docs -name "*.md" | wc -l  # Expected: 19
cat README.md | grep -c "docs"  # Expected: ≥ 1
cargo doc --workspace --no-deps  # Expected: exit 0
```

### Final Checklist
- [ ] Docusaurus site builds without errors
- [ ] All 19 documentation pages present
- [ ] Every page is bilingual (中英文混排)
- [ ] At least 3 pages have Mermaid diagrams
- [ ] Root README.md exists with docs link
- [ ] Sidebar navigation covers all sections
- [ ] No stale line-number references in docs
- [ ] No invented APIs or features documented
- [ ] `cargo doc --workspace --no-deps` still builds
