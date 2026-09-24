# rr — Architecture Decision Records

> Design decisions for rr framework evolution, mixed-status.
> An ADR moves from `proposed` → `accepted` (execution plan exists) → `implemented`
> (shipped in code, referenced from CHANGELOG). See the index below for current status.

## Status legend

| Status | Meaning |
|---|---|
| `drafted` | Written up for review; not yet agreed |
| `proposed` | Design agreed in discussion; no implementation work started |
| `accepted` | Execution plan drafted, work in progress or scheduled |
| `implemented` | Shipped in a released version (referenced in `CHANGELOG.md`) |
| `superseded` | Replaced by a later ADR (see `superseded_by`) |
| `deprecated` | No longer pursued; kept for historical context |

## Index

| ID | Title | Status | Created |
|---|---|---|---|
| [A-0001](./A-0001-command-ingestor-abstraction.md) | `CommandIngestor` trait + `rr-ingestor-kafka` reference impl | implemented | 2026-04-24 |
| [A-0002](./A-0002-snapshot-consistency-and-recovery.md) | Snapshot consistency contract + disaster recovery path | implemented | 2026-04-24 |
| [A-0003](./A-0003-framework-idempotency-layer.md) | Framework-level idempotency via `(request_type, request_id)` | implemented | 2026-04-24 |
| [A-0004](./A-0004-etcd-config-schema.md) | etcd key schema for ConfigProvider routing state | implemented | 2026-04-27 |
| [A-0005](./A-0005-per-caller-rate-limiting.md) | Per-caller rate limiting for gRPC services | implemented | 2026-05-07 |
| [A-0006](./A-0006-rocksdb-cf-tuning.md) | Per-CF RocksDB options tuning for rr-storage | implemented | 2026-05-12 |
| [A-0007](./A-0007-output-outbox-mechanism.md) | Output/Outbox mechanism for guaranteed side-effect delivery | implemented | 2026-05-12 |
| [A-0008](./A-0008-snapshot-strategy.md) | Snapshot strategy — periodic auto-trigger via openraft SnapshotPolicy | implemented | 2026-05-12 |
| [A-0009](./A-0009-read-write-separation-and-sm-locking.md) | Read/write RPC separation and state machine locking model | implemented | 2026-05-09 |
| [A-0010](./A-0010-wal-log-storage-backend.md) | WAL log-storage backend as a faster alternative to RocksDB | accepted | 2026-05-13 |
| [A-0011](./A-0011-shutdown-durability-contract.md) | Shutdown durability contract: IoBatcher close 协议 + snapshot/offset 错误传播 | implemented | 2026-08-28 |
| [A-0012](./A-0012-membership-quorum-guard.md) | change-membership server-side quorum guard | implemented | 2026-08-28 |
| [A-0013](./A-0013-remove-groupmeta-leader.md) | 删除静态路由表中的 `GroupMeta.leader` 字段 | implemented | 2026-08-28 |
| [A-0014](./A-0014-tcp-transport-security-gate.md) | TCP raft transport 的 transport-aware 安全 gate | implemented | 2026-08-28 |
| [A-0015](./A-0015-log-visibility-contract.md) | Raft 日志可见性契约:append 提交即可见(pending overlay + flush barrier) | implemented | 2026-09-06 |
| [A-0016](./A-0016-durable-raft-log-wiring.md) | 生产二进制接线 durable raft log(log storage × network factory 正交化) | implemented | 2026-09-10 |
| [A-0017](./A-0017-service-bytes-migration.md) | Service trait Bytes 化 — 消除 apply 路径强制拷贝 | drafted | 2026-09-23 |
| [A-0018](./A-0018-apply-error-contract.md) | Apply 期错误契约:group-fatal vs error-as-applied-response | drafted | 2026-09-23 |

## Relationship

```
A-0001 (Kafka input)  ──┬──> enables event-sourcing use cases
                        │
A-0002 (snapshot)     ──┼──> required invariants for A-0001 recovery flow
                        │
A-0003 (idempotency)  ──┴──> complements A-0001: Kafka at-least-once + framework dedup
```

All three ADRs are **compatible and composable** — implementing any subset is
valid. Full value is realised when all three ship together:

- Kafka as durable command log (A-0001)
- Snapshot/offset recovery with clear contracts (A-0002)
- Framework dedup on top of Kafka's at-least-once (A-0003)

## Conventions

- **File naming**: `A-NNNN-<slug>.md` (4-digit zero-padded sequential number)
- **Language**: English (internal design documentation)
- **Format**: 5 sections — Context / Problem / Options / Decision / Consequences
- **Citations**: OpenRaft source citations must use commit-pinned URLs
- **Changelog**: Every ADR ends with a `## Changelog` section tracking revisions

## See also

- `.sisyphus/CONTEXT.md` — project onboarding, current phase, key decisions
- `.sisyphus/plans/*.md` — execution plans (how, not why)
- `.sisyphus/notepads/<phase>/decisions.md` — per-phase retrospective decisions
