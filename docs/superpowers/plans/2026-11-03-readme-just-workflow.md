# README Playbook and just Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn README into a clear entry point, add a task-oriented deep playbook, and provide a tested `just` command surface for Rust, clusters, acceptance, docs, and Java SDK work.

**Architecture:** README links to a dedicated guide and `just --list`; `justfile` owns process PID/log management under `target/just/`; Java recipes delegate to the existing Gradle wrapper without editing SDK files.

**Tech Stack:** Markdown, Docusaurus, just, bash, Cargo, Gradle, Docker/Colima, Helm.

**Spec:** `docs/superpowers/specs/2026-11-03-readme-just-workflow-design.md`

## Global Constraints

- README stays concise; deep procedures live in `docs/docs/guide/playbook.md`.
- Existing Java wrapper local changes must not be modified.
- No external registry access in recipes; required local Etcd image remains explicit.
- Background recipes must create PID/log state under `target/just/` and clean only their own processes.
- Every public command must fail fast with a useful prerequisite error.
- Do not commit or merge unless the user chooses that after verification.

## Review Focus

- Long-running cluster recipes must not leak processes after failures; covered by Task 2 status/down tests.
- Etcd commands must work with both default Docker and explicit `DOCKER_HOST`; covered by Task 2 doctor/help text.
- Java recipes must preserve current wrapper state; covered by Task 3.
- README links must point at existing docs and just commands; covered by Task 4.
- Playbook commands must match just recipes rather than duplicating conflicting raw commands; covered by Task 3.

---

### Task 1: Core justfile and doctor

**Files:**
- Create: `justfile`

**Interfaces:**
- Produces grouped recipes: `doctor`, `build`, `test`, `test-one NAME`, `fmt`, `check`, `clean-target-just`, `docs-build`, `docs-serve`, `helm-test`.

- [ ] **Step 1: Create the justfile**

Define stable variable defaults for `RR_CLUSTER_NAME`, `RR_PORTS`, `RR_ETCD_ENDPOINTS`, `RR_DOCKER_HOST`, `RR_ETCD_IMAGE`, and the Java SDK directory. Use shell functions/helpers to require commands and avoid duplicated error text.

- [ ] **Step 2: Verify core commands**

Run: `just --list`
Run: `just doctor`
Run: `just docs-build` only if existing `docs/node_modules` permits it without installation.
Expected: recipes list correctly; missing tools produce named prerequisites.

### Task 2: Cluster, Genesis and Etcd recipes

**Files:**
- Modify: `justfile`

**Interfaces:**
- Produces `etcd-up/down`, `kv-cluster-up/down/status/put`, `genesis-up/down/status`, `test-etcd`.

- [ ] **Step 1: Add process lifecycle recipes**

Build binaries first, start processes with `nohup`, record PID/log paths, and wait on readiness/KV evidence. `down` sends SIGINT to recorded PIDs and removes only those PID files; it never deletes data directories.

- [ ] **Step 2: Add real Etcd acceptance recipe**

`test-etcd` requires the local Etcd image, uses `DOCKER_HOST` when set, and otherwise fails with instructions. It runs the rr-config-etcd and kv-store real suites without silently replacing Docker.

- [ ] **Step 3: Smoke the recipes**

Run `just --list`, `just doctor`, and safe failure cases such as `just kv-cluster-status` without a cluster. If a local cluster is running on the documented default ports, do not disturb it; use configurable ports for any live smoke.

### Task 3: Deep playbook and README entry

**Files:**
- Create: `docs/docs/guide/playbook.md`
- Modify: `README.md`
- Modify: `docs/sidebars.ts` only if the guide navigation requires an explicit entry.

**Interfaces:**
- README links to playbook and `just` commands; playbook uses the same recipe names.

- [ ] **Step 1: Write playbook sections**

Cover quick start, manual three-node cluster, homogeneous Genesis, legacy adoption, Rocks restart, failover, membership/admin inspection, real Etcd/Helm acceptance, Java SDK, security/TLS and troubleshooting. Each section states prerequisites, command, verification and known boundary.

- [ ] **Step 2: Update README**

Add concise playbook and just entry without duplicating procedures. Update stale quickstart text if it conflicts with managed formation.

- [ ] **Step 3: Validate docs**

Run Markdown line-width checks and Docusaurus build only with existing local dependencies; do not install docs packages as part of validation.

### Task 4: Java SDK recipes and final verification

**Files:**
- Modify: `justfile`

**Interfaces:**
- Produces `java-build`, `java-test`, `java-integration-test`, `java-javadoc`, `java-publish-local`, `java-clean`.

- [ ] **Step 1: Add Java recipes**

Use `./gradlew` from `sdks/java`; integration tests default to the existing `RR_TEST_DOCKER=1` convention and fail fast when Docker is unavailable. Publishing remains local-only.

- [ ] **Step 2: Run final verification**

Run: `just --list`, `just doctor`, `just test-one <stable test>`, `just java-test`, playbook Markdown checks, `git diff --check`, and `cargo test`.
Expected: all executed checks pass; any unavailable external prerequisite is reported, not faked.

- [ ] **Step 3: Review checkpoint**

Inspect final diff for Java files, logs and process leftovers. Present local integration options; no push unless chosen.
