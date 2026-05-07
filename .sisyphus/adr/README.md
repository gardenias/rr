# rr — Architecture Decision Records

> Forward-looking design decisions for rr framework evolution.
> These ADRs represent **proposed designs** that have been discussed and agreed
> upon in principle, but are **not yet implemented**. Each ADR will be promoted
> into an execution plan in `.sisyphus/plans/` when work starts.

## Status legend

| Status | Meaning |
|---|---|
| `proposed` | Design agreed in discussion; no implementation work started |
| `accepted` | Execution plan drafted, work in progress or scheduled |
| `implemented` | Shipped in a released version (referenced in `CHANGELOG.md`) |
| `superseded` | Replaced by a later ADR (see `superseded_by`) |
| `deprecated` | No longer pursued; kept for historical context |

## Index

| ID | Title | Status | Created |
|---|---|---|---|
| [A-0001](./A-0001-command-ingestor-abstraction.md) | `CommandIngestor` trait + `rr-ingestor-kafka` reference impl | proposed | 2026-04-24 |
| [A-0002](./A-0002-snapshot-consistency-and-recovery.md) | Snapshot consistency contract + disaster recovery path | proposed | 2026-04-24 |
| [A-0003](./A-0003-framework-idempotency-layer.md) | Framework-level idempotency via `(request_type, request_id)` | proposed | 2026-04-24 |
| [A-0004](./A-0004-etcd-config-schema.md) | etcd key schema for ConfigProvider routing state | proposed | 2026-04-27 |
| [A-0005](./A-0005-per-caller-rate-limiting.md) | Per-caller rate limiting for gRPC services | accepted | 2026-05-07 |
| [A-0006](./A-0006-rocksdb-cf-tuning.md) | Per-CF RocksDB options tuning for rr-storage | accepted | 2026-05-07 |

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
