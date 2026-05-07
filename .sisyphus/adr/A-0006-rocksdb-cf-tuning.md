---
title: "Per-CF RocksDB options tuning for rr-storage"
status: accepted
created: 2026-05-07
updated: 2026-05-07
tags: [adr, architecture, storage, rocksdb, performance]
---

# A-0006 — Per-Column-Family RocksDB Options Tuning

## Context

`rr-storage` (`crates/rr-storage/src/rocks.rs`) opens a single RocksDB instance
containing three column families:

| CF name | Purpose | Primary access pattern |
|---|---|---|
| `raft_log` | Raft log entries (Entry serialized via bincode) | Sequential writes, sequential reads during replay, random reads for `get_log_entries` |
| `metadata` | Raft metadata: `vote`, `last_purged_log_id` | Extremely rare reads/writes (only on leader election and log compaction) |
| `state_machine` | Application state (user-defined KV data) | Random reads, periodic writes on apply |

All three CFs are currently opened with a **single shared `Options` object** and
`ColumnFamilyDescriptor::new(*name, Options::default())` for per-CF descriptors:

```rust
// Current code (rocks.rs lines 253-256):
let cf_descriptors: Vec<ColumnFamilyDescriptor> = cfs
    .iter()
    .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
    .collect();
```

This means the DB-level tunings (`block_cache_size`, `write_buffer_size`,
`compression`, etc.) in `RocksConfig` are applied uniformly to all CFs. There is
no mechanism to give `raft_log` a larger write buffer or give `state_machine` a
separate block cache.

`RocksConfig` currently exposes:

```rust
pub struct RocksConfig {
    pub path: PathBuf,
    pub block_cache_size_mb: u64,      // shared LRU cache for all CFs
    pub write_buffer_size_mb: u64,     // per-memtable size
    pub max_write_buffer_number: u32,
    pub max_background_jobs: u32,
    pub compression: DBCompressionType,
    pub level_zero_slowdown_trigger: i32,
    pub level_zero_stop_trigger: i32,
    pub durability: DurabilityPolicy,
}
```

## Problem

`raft_log` and `state_machine` have opposite access patterns:

- **`raft_log`** is **write-heavy and sequential**. It benefits from a large
  `write_buffer_size` (more buffering before flush), fewer L0 compaction
  triggers, and potentially lighter compression (LZ4 is fine; Zstd is overkill
  for transient log data). After log compaction the data is purged anyway.
- **`state_machine`** is **read-heavy and random**. It benefits from a large
  block cache, bloom filters on SSTs, and potentially stronger compression
  (Zstd) since it's long-lived data.
- **`metadata`** writes only on election and compaction events (roughly once per
  minutes to hours). Default options are more than sufficient.

There is currently no way to express these different requirements without
forking the entire `Options` object and duplicating DB-level settings.

## Options

### Option 1 — Hardcoded per-CF defaults by CF name

Inside `RocksStorage::open()`, detect the CF name and apply different `Options`
depending on the name:

```rust
let opts_for_cf = |name: &str| match name {
    "raft_log"      => raft_log_options(),
    "state_machine" => state_machine_options(),
    _               => Options::default(),
};
```

**Pros:**
- Simple — no new public API.
- Sensible defaults ship automatically.

**Cons:**
- Operators cannot tune further without recompiling.
- "Magic" behaviour: the config file is misleading (users think they control
  tuning via `RocksConfig`, but raft_log has different defaults silently).
- No path to workload-specific tuning.

### Option 2 — Optional per-CF overrides in `RocksConfig`

Extend `RocksConfig` with optional `CfOptions` structs:

```rust
pub struct CfOptions {
    pub write_buffer_size_mb: Option<u64>,
    pub block_cache_size_mb: Option<u64>,
    pub compression: Option<DBCompressionType>,
    // future: bloom_filter_bits, max_write_buffer_number, ...
}

pub struct RocksConfig {
    // ... all existing fields unchanged ...

    /// Per-CF overrides. `None` means inherit global defaults.
    pub raft_log_cf: Option<CfOptions>,
    pub state_machine_cf: Option<CfOptions>,
}
```

`ColumnFamilyDescriptor` is built per CF: merge global defaults with optional
overrides. Backward compatible: `default()` and `for_testing()` have no overrides
and behave exactly as today.

**Pros:**
- Config-driven: operators tune in TOML/env without recompiling.
- Backward compatible: no override = same behaviour as today.
- Clear mental model: global config is the base, per-CF is an override layer.
- `metadata` CF stays at defaults (intentionally simple).

**Cons:**
- Slightly larger `RocksConfig` surface.
- Building per-CF `Options` requires merging logic (inherit global then apply
  overrides) — a few extra lines of code.

### Option 3 — Separate RocksDB instances per CF

Give `raft_log` and `state_machine` their own RocksDB instances with independent
configs:

**Pros:**
- Total independence: different paths, different block caches, different
  compaction settings.

**Cons:**
- Two DB instances = two LOCK files, two WAL directories, more complex backup/
  restore logic.
- Existing `RocksStorage` API assumes a single `DB` handle.
- Not worth the complexity for the current use case.

## Decision

**Option 2 — Optional per-CF overrides in `RocksConfig`**.

Rationale:
- Adds tuning power without breaking anything.
- Operators using TOML config can tune per workload without code changes.
- `metadata` CF intentionally excluded from the override surface — it is
  low-activity enough that defaults are always appropriate.
- A minimal first version exposes `write_buffer_size_mb`, `block_cache_size_mb`,
  and `compression`; additional fields can be added as needed.

### Recommended defaults to document (not hardcode)

These are suggested starting points for operators (in docs / rustdoc):

| CF | Setting | Suggested value | Rationale |
|---|---|---|---|
| `raft_log` | `write_buffer_size_mb` | 128–256 | More buffering before flush → fewer small SSTs |
| `raft_log` | `block_cache_size_mb` | 32 | Log is mostly write; small cache sufficient |
| `raft_log` | `compression` | `Lz4` (default) | Log entries are short-lived; Lz4 is fastest |
| `state_machine` | `block_cache_size_mb` | 512–1024 | Read-heavy; large cache → fewer disk reads |
| `state_machine` | `compression` | `Zstd` | Long-lived state benefits from better compression ratio |
| `state_machine` | `write_buffer_size_mb` | 32–64 | Not write-heavy; smaller is fine |

These are documented recommendations, not hardcoded behaviour.

### Implementation sketch

```rust
// In rocks.rs: build per-CF options
fn build_cf_options(base: &Options, overrides: Option<&CfOptions>) -> Options {
    let mut opts = Options::default();
    // Copy relevant DB-level settings from base
    // (block cache, compression, write_buffer from RocksConfig)
    // ...
    if let Some(ov) = overrides {
        if let Some(wb) = ov.write_buffer_size_mb {
            opts.set_write_buffer_size(wb as usize * 1024 * 1024);
        }
        if let Some(c) = ov.compression {
            opts.set_compression_type(c);
        }
        // block_cache requires BlockBasedOptions rebuild
    }
    opts
}

// In open():
let cf_descriptors: Vec<ColumnFamilyDescriptor> = cfs.iter().map(|name| {
    let cf_opts = match *name {
        "raft_log"      => build_cf_options(&opts, config.raft_log_cf.as_ref()),
        "state_machine" => build_cf_options(&opts, config.state_machine_cf.as_ref()),
        _               => Options::default(),
    };
    ColumnFamilyDescriptor::new(*name, cf_opts)
}).collect();
```

## Consequences

### Positive
- Operators can tune storage to their workload without recompiling.
- `raft_log` write performance can be improved significantly for write-heavy
  workloads (larger write buffer → fewer flushes → less L0 compaction pressure).
- `state_machine` read performance benefits from a dedicated block cache.
- No change to existing behaviour when overrides are absent.

### Negative
- `RocksConfig` gains two optional fields (`raft_log_cf`, `state_machine_cf`).
  Operators who don't need tuning see slightly more surface in documentation.
- The `build_cf_options` helper adds a few lines of `if let` merging logic.

### Neutral
- The `metadata` CF is intentionally left at defaults and not exposed in config.
- `RocksConfig::for_testing()` remains unchanged (no overrides).
- Backup and restore procedures are unchanged (same single RocksDB instance).
- Feature is gated behind `--features rocks` as before.

## Changelog

| Date | Author | Change |
|------|--------|--------|
| 2026-05-07 | session | Initial draft — design from storage exploration + user discussion |
