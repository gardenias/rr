//! RocksDB-backed [`StorageEngine`] implementation.
//!
//! Uses RocksDB for persistent, efficient key-value storage with column families.

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType, Direction, IteratorMode,
    Options, ReadOptions, WriteBatch, WriteOptions, DB,
};
use std::sync::Arc;
use std::time::Duration;

use rr_core::error::StorageError;
use rr_core::storage::{BatchOp, StorageEngine};

/// Per-column-family RocksDB option overrides.
///
/// `None` fields inherit from the global [`RocksConfig`] defaults.
#[derive(Debug, Clone, Default)]
pub struct CfOptions {
    /// Write buffer size in MiB. Overrides [`RocksConfig::write_buffer_size_mb`] for this CF.
    pub write_buffer_size_mb: Option<u64>,
    /// Block cache size in MiB. When set, this CF gets a **private** LRU
    /// cache of this size INSTEAD of a share of the global
    /// [`RocksConfig::block_cache_size_mb`] cache — the knob exists to
    /// isolate one CF's cache budget, and it adds to total memory rather
    /// than carving out of the shared cache.
    pub block_cache_size_mb: Option<u64>,
    /// Compression type. Overrides [`RocksConfig::compression`] for this CF.
    pub compression: Option<DBCompressionType>,
}

/// Durability policy controlling when RocksDB syncs writes to disk.
///
/// **Only [`Strict`](Self::Strict) upholds OpenRaft's durability contract.**
/// Every other policy violates the contract and is provided only for tests
/// or for users who explicitly accept the risk of losing committed entries
/// on power failure (and have replication + external backup as
/// compensating controls). See ADR A-0002 §Contract 4.
///
/// Names changed in v0.3.0 (breaking window): the deprecated `Balanced` and
/// `Fast` aliases were removed — use [`ReplicatedOnly`](Self::ReplicatedOnly)
/// and [`TestOnly`](Self::TestOnly).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityPolicy {
    /// **Production default.** Every committed write issues an `fsync` on
    /// the write-ahead log (WAL) before returning.
    ///
    /// This is the **only** mode that upholds Raft's durability contract.
    /// With any weaker policy, a node that has "committed" a log entry
    /// may lose it after a power failure — breaking the consensus
    /// invariant that committed entries survive node restarts.
    ///
    /// Performance: each write incurs an fsync on the WAL (typically
    /// 1–10 ms on NVMe, 10–100 ms on spinning disks). Batch writes amortize
    /// the cost across many ops. Use this unless you **deeply understand**
    /// the durability/performance tradeoff.
    Strict,
    /// WAL is written but NOT fsynced. A process crash still recovers
    /// (the WAL is on disk via OS buffers); a **kernel panic or power
    /// loss will lose the last ~seconds of writes**.
    ///
    /// Safe ONLY when ALL of these hold:
    /// - replication factor ≥ 3,
    /// - nodes on independent power domains,
    /// - external backup captures the Raft log retention.
    ///
    /// **Violates** OpenRaft's durability contract — Raft's safety
    /// guarantees do NOT hold under simultaneous quorum power loss.
    /// Performance: 10–100× faster than `Strict` on typical hardware.
    ReplicatedOnly,
    /// No sync and no WAL. Maximum throughput. **Total data loss on crash.**
    ///
    /// Only appropriate for tests and throwaway / cache workloads. Never
    /// use in production.
    TestOnly,
}

impl DurabilityPolicy {
    /// Returns true iff this policy upholds OpenRaft's durability contract
    /// (i.e. fsyncs every committed write). Only [`Strict`](Self::Strict)
    /// qualifies.
    pub fn is_strict(self) -> bool {
        matches!(self, DurabilityPolicy::Strict)
    }

    /// Apply this policy to an `Options` (controls SST sync behavior).
    fn apply_to_db_options(self, opts: &mut Options) {
        match self {
            DurabilityPolicy::Strict => {
                opts.set_use_fsync(true);
            }
            DurabilityPolicy::ReplicatedOnly | DurabilityPolicy::TestOnly => {
                // Default: fdatasync or OS-buffer only
            }
        }
    }

    /// Construct `WriteOptions` appropriate for this policy.
    /// This is called on every write; constructing is cheap.
    fn write_options(self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        match self {
            DurabilityPolicy::Strict => {
                opts.set_sync(true);
            }
            DurabilityPolicy::ReplicatedOnly => {
                // Default: WAL is written but not synced.
            }
            DurabilityPolicy::TestOnly => {
                opts.disable_wal(true);
            }
        }
        opts
    }

    /// Human-readable name for the policy, used in startup warnings and
    /// metrics labels.
    fn canonical_name(self) -> &'static str {
        match self {
            DurabilityPolicy::Strict => "strict",
            DurabilityPolicy::ReplicatedOnly => "replicated_only",
            DurabilityPolicy::TestOnly => "test_only",
        }
    }
}

/// Configuration for tuning a RocksDB instance.
///
/// Use [`RocksConfig::default()`] for production-ready values or
/// [`RocksConfig::for_testing()`] for minimal-resource test environments.
///
/// # Durability
///
/// The `durability` field controls the fsync behavior. The default is
/// [`DurabilityPolicy::Strict`], which is required for Raft's durability
/// contract. See [`DurabilityPolicy`] for details.
#[derive(Debug, Clone)]
pub struct RocksConfig {
    /// Size of the block cache in megabytes: ONE LRU cache built at `open()`
    /// and shared by every column family (including the DB-level default).
    /// A CF with [`CfOptions::block_cache_size_mb`] set gets a private cache
    /// ON TOP of this one instead of sharing.
    pub block_cache_size_mb: usize,
    /// Size of each memtable (write buffer) in megabytes.
    pub write_buffer_size_mb: usize,
    /// Maximum number of memtables before stalling writes.
    pub max_write_buffer_number: i32,
    /// Maximum number of background compaction/flush jobs.
    pub max_background_jobs: i32,
    /// Compression algorithm used for SST files.
    pub compression: DBCompressionType,
    /// Number of L0 files that triggers write slowdown.
    pub level_zero_slowdown_trigger: i32,
    /// Number of L0 files that triggers a full write stop.
    pub level_zero_stop_trigger: i32,
    /// Durability/fsync policy. Defaults to [`DurabilityPolicy::Strict`].
    ///
    /// **WARNING**: weakening this below `Strict` violates Raft's durability
    /// contract. Only do so with a full understanding of the consequences.
    pub durability: DurabilityPolicy,
    /// Per-CF tuning overrides for the `raft_log` column family.
    /// Recommended: larger write_buffer for write-heavy log append workload.
    pub raft_log_cf: Option<CfOptions>,
    /// Per-CF tuning overrides for the `state_machine` column family.
    /// Recommended: larger block_cache for read-heavy application state.
    pub state_machine_cf: Option<CfOptions>,
    /// Optional background WAL flush interval in milliseconds.
    pub group_commit_interval_ms: Option<u64>,
}

impl Default for RocksConfig {
    fn default() -> Self {
        Self {
            block_cache_size_mb: 256,
            write_buffer_size_mb: 64,
            max_write_buffer_number: 4,
            max_background_jobs: 4,
            compression: DBCompressionType::Lz4,
            level_zero_slowdown_trigger: 20,
            level_zero_stop_trigger: 36,
            durability: DurabilityPolicy::Strict,
            raft_log_cf: None,
            state_machine_cf: None,
            group_commit_interval_ms: None,
        }
    }
}

impl RocksConfig {
    /// Create a minimal-resource configuration suitable for tests.
    ///
    /// Uses a small block cache (8 MB), tiny write buffers (4 MB),
    /// a single background job, no compression, and
    /// [`DurabilityPolicy::TestOnly`] (no WAL, no fsync) to keep the test
    /// suite fast.
    pub fn for_testing() -> Self {
        Self {
            block_cache_size_mb: 8,
            write_buffer_size_mb: 4,
            max_write_buffer_number: 2,
            max_background_jobs: 1,
            compression: DBCompressionType::None,
            level_zero_slowdown_trigger: 20,
            level_zero_stop_trigger: 36,
            durability: DurabilityPolicy::TestOnly,
            raft_log_cf: None,
            state_machine_cf: None,
            group_commit_interval_ms: None,
        }
    }
}

/// Map a RocksDB error onto a `StorageError`, preserving the actionable
/// `ErrorKind`: a full disk surfaces as [`std::io::ErrorKind::StorageFull`]
/// (the same kind the fault-injecting storage injects for ENOSPC — see
/// rr-core `storage_fault.rs`), everything else as `Other`. RocksDB flattens
/// its status codes into the message text, so the ENOSPC classes are matched
/// textually.
fn map_rocks_err(e: rocksdb::Error) -> StorageError {
    map_rocks_err_msg(e.to_string())
}

fn map_rocks_err_msg(msg: String) -> StorageError {
    let kind = if msg.contains("No space left") || msg.contains("ENOSPC") {
        std::io::ErrorKind::StorageFull
    } else {
        std::io::ErrorKind::Other
    };
    StorageError::Io(std::io::Error::new(kind, msg))
}

/// A persistent storage engine backed by RocksDB.
///
/// Wraps a RocksDB instance with standard column families for Raft:
/// - `raft_log`: Log entries
/// - `state_machine`: KV data for state machine
/// - `metadata`: Raft metadata (voted_for, applied_index, etc.)
pub struct RocksStorage {
    db: Arc<DB>,
    durability: DurabilityPolicy,
    group_commit: Option<GroupCommitHandle>,
    /// Column families opened at `open()` plus any later `create_cf()`.
    /// Tracked because `DB::flush()` alone only flushes the `default` CF —
    /// [`StorageEngine::flush`] walks this list instead. `lock().unwrap()`
    /// is deliberate: writers are rare (`create_cf`), sections are tiny, and
    /// poisoning can only follow an internal panic — fail-closed.
    cf_names: std::sync::RwLock<Vec<String>>,
}

/// Handle for a background group-commit task.
pub struct GroupCommitHandle {
    _handle: tokio::task::JoinHandle<()>,
    cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for GroupCommitHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(());
        }
    }
}

fn build_cf_options(
    base_config: &RocksConfig,
    overrides: Option<&CfOptions>,
    shared_cache: &Cache,
) -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(base_config.write_buffer_size_mb * 1024 * 1024);
    opts.set_max_write_buffer_number(base_config.max_write_buffer_number);
    opts.set_max_background_jobs(base_config.max_background_jobs);
    opts.set_compression_type(base_config.compression);
    opts.set_level_zero_slowdown_writes_trigger(base_config.level_zero_slowdown_trigger);
    opts.set_level_zero_stop_writes_trigger(base_config.level_zero_stop_trigger);

    let mut bb_opts = BlockBasedOptions::default();
    match overrides.and_then(|o| o.block_cache_size_mb) {
        // Per-CF override: a PRIVATE cache for this CF (see
        // `CfOptions::block_cache_size_mb`), deliberately not the shared one.
        Some(mb) => bb_opts.set_block_cache(&Cache::new_lru_cache(mb as usize * 1024 * 1024)),
        None => bb_opts.set_block_cache(shared_cache),
    }
    opts.set_block_based_table_factory(&bb_opts);

    if let Some(overrides) = overrides {
        if let Some(write_buffer_size_mb) = overrides.write_buffer_size_mb {
            opts.set_write_buffer_size(write_buffer_size_mb as usize * 1024 * 1024);
        }
        if let Some(compression) = overrides.compression {
            opts.set_compression_type(compression);
        }
    }

    opts
}

impl RocksStorage {
    /// Spawn a background group-commit task that periodically flushes the WAL.
    ///
    /// The flush is `flush_wal(true)` (a real fsync: `flush_wal(false)` is a
    /// no-op with the default `manual_wal_flush=false`). Failures are loud:
    /// warn + the `rr_storage_wal_flush_errors_total` counter, and the task
    /// keeps running so a transient error does not silently end durability
    /// maintenance.
    pub fn spawn_group_commit(&self, interval_ms: u64) -> GroupCommitHandle {
        let db = Arc::clone(&self.db);
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = &mut cancel_rx => break,
                    _ = ticker.tick() => {
                        let result = tokio::task::spawn_blocking({
                            let db = Arc::clone(&db);
                            move || db.flush_wal(true)
                        })
                        .await;
                        match result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                metrics::counter!("rr_storage_wal_flush_errors_total").increment(1);
                                tracing::warn!(error = %e, "group-commit WAL flush failed");
                            }
                            Err(e) => {
                                metrics::counter!("rr_storage_wal_flush_errors_total").increment(1);
                                tracing::warn!(error = %e, "group-commit WAL flush task failed");
                            }
                        }
                    }
                }
            }
        });

        GroupCommitHandle {
            _handle: handle,
            cancel_tx: Some(cancel_tx),
        }
    }

    /// Open a RocksDB instance with custom column families.
    ///
    /// # Arguments
    /// * `path` - Directory path for the database
    /// * `cfs` - List of column family names to create/open
    /// * `config` - RocksDB tuning parameters (see [`RocksConfig`])
    ///
    /// # Returns
    /// - `Ok(RocksStorage)` on success
    /// - `Err(StorageError::Io)` if RocksDB operations fail
    pub fn open(path: &str, cfs: Vec<&str>, config: RocksConfig) -> Result<Self, StorageError> {
        tracing::info!("Opening RocksDB with config: {:?}", config);

        let durability = config.durability;
        if !durability.is_strict() {
            tracing::warn!(
                policy = durability.canonical_name(),
                "DurabilityPolicy is not Strict — RocksDB will skip WAL fsync. \
                 This violates OpenRaft's durability contract and is only safe \
                 with replication + external backup. See \
                 docs/docs/guide/storage-engines.md for details."
            );
        }

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_write_buffer_size(config.write_buffer_size_mb * 1024 * 1024);
        opts.set_max_write_buffer_number(config.max_write_buffer_number);
        opts.set_max_background_jobs(config.max_background_jobs);
        opts.set_compression_type(config.compression);
        opts.set_level_zero_slowdown_writes_trigger(config.level_zero_slowdown_trigger);
        opts.set_level_zero_stop_writes_trigger(config.level_zero_stop_trigger);
        durability.apply_to_db_options(&mut opts);

        // ONE block cache per database, shared by every column family
        // (rust-rocksdb's `Cache` is internally Arc-refcounted, so every
        // `set_block_cache` below references the same cache). Before this,
        // each CF built its own cache sized at the global knob — total cache
        // memory was ~(1 + #CFs) × the documented budget.
        let shared_cache = Cache::new_lru_cache(config.block_cache_size_mb * 1024 * 1024);
        let mut bb_opts = BlockBasedOptions::default();
        bb_opts.set_block_cache(&shared_cache);
        opts.set_block_based_table_factory(&bb_opts);

        let cf_descriptors: Vec<ColumnFamilyDescriptor> = cfs
            .iter()
            .map(|name| {
                let cf_opts = match *name {
                    "raft_log" => {
                        build_cf_options(&config, config.raft_log_cf.as_ref(), &shared_cache)
                    }
                    "state_machine" => {
                        build_cf_options(&config, config.state_machine_cf.as_ref(), &shared_cache)
                    }
                    _ => build_cf_options(&config, None, &shared_cache),
                };
                ColumnFamilyDescriptor::new(*name, cf_opts)
            })
            .collect();

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors).map_err(map_rocks_err)?;

        let mut storage = Self {
            db: Arc::new(db),
            durability,
            group_commit: None,
            cf_names: std::sync::RwLock::new(cfs.iter().map(|s| s.to_string()).collect()),
        };

        if let Some(interval_ms) = config.group_commit_interval_ms {
            if durability == DurabilityPolicy::ReplicatedOnly {
                let group_commit = storage.spawn_group_commit(interval_ms);
                storage.group_commit = Some(group_commit);
            } else {
                tracing::warn!(
                    interval_ms,
                    policy = durability.canonical_name(),
                    "group_commit_interval_ms is ignored: background WAL group-commit only \
                     applies to DurabilityPolicy::ReplicatedOnly (Strict fsyncs every write; \
                     TestOnly disables the WAL)"
                );
            }
        }

        Ok(storage)
    }

    /// Current durability policy in effect for all writes.
    pub fn durability_policy(&self) -> DurabilityPolicy {
        self.durability
    }

    /// Open a RocksDB instance with the framework's standard column families.
    ///
    /// Creates three column families if they don't exist:
    /// - `raft_log`
    /// - `state_machine`
    /// - `metadata`
    ///
    /// # Arguments
    /// * `path` - Directory path for the database
    ///
    /// # Returns
    /// - `Ok(RocksStorage)` on success
    /// - `Err(StorageError::Io)` if RocksDB operations fail
    pub fn open_default(path: &str) -> Result<Self, StorageError> {
        Self::open(
            path,
            vec!["raft_log", "state_machine", "metadata"],
            RocksConfig::default(),
        )
    }

    /// Open a RocksDB instance with standard column families and minimal-resource
    /// configuration suitable for testing.
    pub fn open_for_testing(path: &str) -> Result<Self, StorageError> {
        Self::open(
            path,
            vec!["raft_log", "state_machine", "metadata"],
            RocksConfig::for_testing(),
        )
    }
}

#[async_trait::async_trait]
impl StorageEngine for RocksStorage {
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let db = Arc::clone(&self.db);
        let durability = self.durability;
        let cf = cf.to_string();
        let key = key.to_vec();
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || {
            let handle = db.cf_handle(&cf).ok_or_else(|| StorageError::NotFound {
                key: format!("cf:{cf}"),
            })?;
            let write_opts = durability.write_options();
            db.put_cf_opt(&handle, &key, &value, &write_opts)
                .map_err(map_rocks_err)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let db = Arc::clone(&self.db);
        let cf = cf.to_string();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || {
            let handle = db.cf_handle(&cf).ok_or_else(|| StorageError::NotFound {
                key: format!("cf:{cf}"),
            })?;
            db.get_cf(&handle, &key).map_err(map_rocks_err)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), StorageError> {
        let db = Arc::clone(&self.db);
        let durability = self.durability;
        let cf = cf.to_string();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || {
            let handle = db.cf_handle(&cf).ok_or_else(|| StorageError::NotFound {
                key: format!("cf:{cf}"),
            })?;
            let write_opts = durability.write_options();
            db.delete_cf_opt(&handle, &key, &write_opts)
                .map_err(map_rocks_err)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn scan(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let db = Arc::clone(&self.db);
        let cf = cf.to_string();
        let start = start.to_vec();
        let end = end.to_vec();
        tokio::task::spawn_blocking(move || {
            let handle = db.cf_handle(&cf).ok_or_else(|| StorageError::NotFound {
                key: format!("cf:{cf}"),
            })?;
            let mut read_opts = ReadOptions::default();
            read_opts.set_iterate_upper_bound(end.clone());
            let iter = db.iterator_cf_opt(
                &handle,
                read_opts,
                IteratorMode::From(&start, Direction::Forward),
            );
            let mut results = Vec::new();
            for item in iter {
                let (k, v) = item.map_err(map_rocks_err)?;
                results.push((k.to_vec(), v.to_vec()));
            }
            Ok(results)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn last_key_value(&self, cf: &str) -> Result<Option<(Vec<u8>, Vec<u8>)>, StorageError> {
        let db = Arc::clone(&self.db);
        let cf = cf.to_string();
        tokio::task::spawn_blocking(move || {
            let handle = db.cf_handle(&cf).ok_or_else(|| StorageError::NotFound {
                key: format!("cf:{cf}"),
            })?;
            // Reverse seek: position at the end of the CF and read one item —
            // O(1)-ish instead of the trait default's full-CF scan.
            let mut iter = db.iterator_cf_opt(&handle, ReadOptions::default(), IteratorMode::End);
            match iter.next() {
                None => Ok(None),
                Some(item) => {
                    let (k, v) = item.map_err(map_rocks_err)?;
                    Ok(Some((k.to_vec(), v.to_vec())))
                }
            }
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn write_batch(&self, ops: Vec<BatchOp>) -> Result<(), StorageError> {
        let db = Arc::clone(&self.db);
        let durability = self.durability;
        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            for op in ops {
                match op {
                    BatchOp::Put { cf, key, value } => {
                        let handle = db.cf_handle(&cf).ok_or_else(|| StorageError::NotFound {
                            key: format!("cf:{cf}"),
                        })?;
                        batch.put_cf(&handle, &key, &value);
                    }
                    BatchOp::Delete { cf, key } => {
                        let handle = db.cf_handle(&cf).ok_or_else(|| StorageError::NotFound {
                            key: format!("cf:{cf}"),
                        })?;
                        batch.delete_cf(&handle, &key);
                    }
                }
            }
            let write_opts = durability.write_options();
            db.write_opt(batch, &write_opts).map_err(map_rocks_err)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn flush(&self) -> Result<(), StorageError> {
        let db = Arc::clone(&self.db);
        let names = self.cf_names.read().unwrap().clone();
        tokio::task::spawn_blocking(move || {
            // Flush every known CF's memtable — `DB::flush()` alone would
            // only flush the `default` CF (see the trait doc).
            for name in &names {
                let handle = db.cf_handle(name).ok_or_else(|| StorageError::NotFound {
                    key: format!("cf:{name}"),
                })?;
                db.flush_cf(&handle).map_err(map_rocks_err)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn create_cf(&self, name: &str) -> Result<(), StorageError> {
        let db = Arc::clone(&self.db);
        let name = name.to_string();
        let created = tokio::task::spawn_blocking({
            let name = name.clone();
            move || {
                let opts = Options::default();
                db.create_cf(&name, &opts).map_err(map_rocks_err)
            }
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        created.map(|()| self.cf_names.write().unwrap().push(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn open_test_db() -> (RocksStorage, TempDir) {
        let tmp = TempDir::new().unwrap();
        let storage =
            RocksStorage::open_for_testing(tmp.path().to_str().unwrap()).expect("should open");
        (storage, tmp)
    }

    #[test]
    fn test_open_default_creates_db() {
        let tmp = TempDir::new().unwrap();
        let storage =
            RocksStorage::open_default(tmp.path().to_str().unwrap()).expect("should open");
        drop(storage); // verify clean close
    }

    #[test]
    fn cf_options_default_is_none() {
        let opts = CfOptions::default();
        assert!(opts.write_buffer_size_mb.is_none());
        assert!(opts.block_cache_size_mb.is_none());
        assert!(opts.compression.is_none());
    }

    #[test]
    fn rocks_config_default_has_no_cf_overrides() {
        let config = RocksConfig::default();
        assert!(config.raft_log_cf.is_none());
        assert!(config.state_machine_cf.is_none());
        assert!(config.group_commit_interval_ms.is_none());
    }

    #[test]
    fn build_cf_options_without_overrides_inherits_global() {
        let config = RocksConfig::default();
        let shared_cache = Cache::new_lru_cache(config.block_cache_size_mb * 1024 * 1024);
        let _opts = build_cf_options(&config, None, &shared_cache);
    }

    #[test]
    fn map_rocks_err_msg_marks_enospc_as_storage_full() {
        for msg in [
            "IO error: No space left on device",
            "IO error: while appending to file: ENOSPC",
        ] {
            match map_rocks_err_msg(msg.to_string()) {
                StorageError::Io(e) => assert_eq!(
                    e.kind(),
                    std::io::ErrorKind::StorageFull,
                    "ENOSPC-class RocksDB errors must keep the StorageFull kind \
                     (FaultInjectingStorage's contract): {msg}"
                ),
                other => panic!("expected StorageError::Io, got {other:?}"),
            }
        }
        // Unrelated errors stay Other.
        match map_rocks_err_msg("Corruption: bad CRC".to_string()) {
            StorageError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::Other),
            other => panic!("expected StorageError::Io, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_last_key_value_reverse_fetch() {
        let (storage, _tmp) = open_test_db();
        assert_eq!(storage.last_key_value("raft_log").await.unwrap(), None);

        storage.put("raft_log", b"log_1", b"e1").await.unwrap();
        storage.put("raft_log", b"log_3", b"e3").await.unwrap();
        storage.put("raft_log", b"log_2", b"e2").await.unwrap();
        assert_eq!(
            storage.last_key_value("raft_log").await.unwrap(),
            Some((b"log_3".to_vec(), b"e3".to_vec()))
        );

        storage.delete("raft_log", b"log_3").await.unwrap();
        assert_eq!(
            storage.last_key_value("raft_log").await.unwrap(),
            Some((b"log_2".to_vec(), b"e2".to_vec()))
        );

        let err = storage.last_key_value("nonexistent_cf").await.unwrap_err();
        assert!(matches!(err, StorageError::NotFound { .. }));
    }

    // INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    #[tokio::test]
    async fn test_flush_covers_every_cf() {
        let (storage, _tmp) = open_test_db();
        storage.put("raft_log", b"k", b"v1").await.unwrap();
        storage.put("state_machine", b"k", b"v2").await.unwrap();
        storage.put("metadata", b"k", b"v3").await.unwrap();
        storage.create_cf("extra_cf").await.unwrap();
        storage.put("extra_cf", b"k", b"v4").await.unwrap();

        // Must not error on any tracked CF (a `DB::flush()`-only
        // implementation would leave non-default CF memtables unflushed).
        storage.flush().await.unwrap();

        // Data stays readable after the all-CF flush, including the
        // runtime-created CF the tracker learned about via create_cf.
        for (cf, want) in [
            ("raft_log", b"v1"),
            ("state_machine", b"v2"),
            ("metadata", b"v3"),
            ("extra_cf", b"v4"),
        ] {
            assert_eq!(
                storage.get(cf, b"k").await.unwrap().as_deref(),
                Some(want.as_slice())
            );
        }
    }

    #[tokio::test]
    async fn test_group_commit_interval_ignored_outside_replicated_only() {
        let tmp = TempDir::new().unwrap();
        let config = RocksConfig {
            durability: DurabilityPolicy::Strict,
            group_commit_interval_ms: Some(10),
            ..RocksConfig::for_testing()
        };
        let storage = RocksStorage::open(tmp.path().to_str().unwrap(), vec!["raft_log"], config)
            .expect("should open");
        assert!(
            storage.group_commit.is_none(),
            "group-commit task must not spawn under Strict (it would be a no-op fsync loop)"
        );

        let tmp2 = TempDir::new().unwrap();
        let config = RocksConfig {
            durability: DurabilityPolicy::ReplicatedOnly,
            group_commit_interval_ms: Some(10),
            ..RocksConfig::for_testing()
        };
        let storage = RocksStorage::open(tmp2.path().to_str().unwrap(), vec!["raft_log"], config)
            .expect("should open");
        assert!(
            storage.group_commit.is_some(),
            "group-commit task must spawn under ReplicatedOnly when configured"
        );
    }

    #[tokio::test]
    async fn test_put_and_get_roundtrip() {
        let (storage, _tmp) = open_test_db();
        storage
            .put("state_machine", b"key1", b"value1")
            .await
            .unwrap();
        let val = storage.get("state_machine", b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_get_missing_returns_none() {
        let (storage, _tmp) = open_test_db();
        let val = storage.get("state_machine", b"missing").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_delete_removes_key() {
        let (storage, _tmp) = open_test_db();
        storage
            .put("state_machine", b"key1", b"value1")
            .await
            .unwrap();
        storage.delete("state_machine", b"key1").await.unwrap();
        let val = storage.get("state_machine", b"key1").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_scan_range() {
        let (storage, _tmp) = open_test_db();
        storage.put("state_machine", b"b", b"2").await.unwrap();
        storage.put("state_machine", b"a", b"1").await.unwrap();
        storage.put("state_machine", b"c", b"3").await.unwrap();
        let results = storage.scan("state_machine", b"a", b"c").await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"a".to_vec());
        assert_eq!(results[1].0, b"b".to_vec());
    }

    #[tokio::test]
    async fn test_scan_empty_range() {
        let (storage, _tmp) = open_test_db();
        let results = storage.scan("state_machine", b"x", b"z").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_write_batch_puts() {
        let (storage, _tmp) = open_test_db();
        let ops = vec![
            BatchOp::Put {
                cf: "state_machine".into(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            BatchOp::Put {
                cf: "state_machine".into(),
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
            },
            BatchOp::Put {
                cf: "state_machine".into(),
                key: b"k3".to_vec(),
                value: b"v3".to_vec(),
            },
        ];
        storage.write_batch(ops).await.unwrap();
        assert_eq!(
            storage.get("state_machine", b"k1").await.unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            storage.get("state_machine", b"k2").await.unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            storage.get("state_machine", b"k3").await.unwrap(),
            Some(b"v3".to_vec())
        );
    }

    #[tokio::test]
    async fn test_write_batch_mixed_put_delete() {
        let (storage, _tmp) = open_test_db();
        storage
            .put("state_machine", b"existing", b"val")
            .await
            .unwrap();
        let ops = vec![
            BatchOp::Put {
                cf: "state_machine".into(),
                key: b"new_key".to_vec(),
                value: b"new_val".to_vec(),
            },
            BatchOp::Delete {
                cf: "state_machine".into(),
                key: b"existing".to_vec(),
            },
        ];
        storage.write_batch(ops).await.unwrap();
        assert_eq!(
            storage.get("state_machine", b"new_key").await.unwrap(),
            Some(b"new_val".to_vec())
        );
        assert_eq!(
            storage.get("state_machine", b"existing").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_flush_succeeds() {
        let (storage, _tmp) = open_test_db();
        storage.put("metadata", b"key", b"val").await.unwrap();
        storage.flush().await.unwrap();
        // After flush, data should still be readable
        assert_eq!(
            storage.get("metadata", b"key").await.unwrap(),
            Some(b"val".to_vec())
        );
    }

    #[tokio::test]
    async fn test_unknown_cf_returns_error() {
        let (storage, _tmp) = open_test_db();
        let result = storage.get("nonexistent_cf", b"key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_custom_cfs() {
        let tmp = TempDir::new().unwrap();
        let storage = RocksStorage::open(
            tmp.path().to_str().unwrap(),
            vec!["custom_cf"],
            RocksConfig::for_testing(),
        )
        .expect("should open with custom cfs");
        storage.put("custom_cf", b"key", b"val").await.unwrap();
        let val = storage.get("custom_cf", b"key").await.unwrap();
        assert_eq!(val, Some(b"val".to_vec()));
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        let tmp = TempDir::new().unwrap();
        let storage = Arc::new(
            RocksStorage::open_default(tmp.path().to_str().unwrap()).expect("should open"),
        );
        let mut handles = vec![];
        for i in 0..10u8 {
            let s = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                s.put("state_machine", &[i], &[i * 2]).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        for i in 0..10u8 {
            let val = storage.get("state_machine", &[i]).await.unwrap();
            assert_eq!(val, Some(vec![i * 2]));
        }
    }

    #[tokio::test]
    async fn test_raft_log_cf_operations() {
        let (storage, _tmp) = open_test_db();
        storage.put("raft_log", b"log_1", b"entry_1").await.unwrap();
        storage.put("raft_log", b"log_2", b"entry_2").await.unwrap();
        let results = storage.scan("raft_log", b"log_1", b"log_3").await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, b"entry_1".to_vec());
        assert_eq!(results[1].1, b"entry_2".to_vec());
    }

    #[tokio::test]
    async fn open_with_cf_overrides_succeeds() {
        let tmp = TempDir::new().unwrap();
        let config = RocksConfig {
            raft_log_cf: Some(CfOptions {
                write_buffer_size_mb: Some(16),
                ..CfOptions::default()
            }),
            ..RocksConfig::for_testing()
        };
        let storage = RocksStorage::open(
            tmp.path().to_str().unwrap(),
            vec!["raft_log", "state_machine", "metadata"],
            config,
        )
        .expect("should open with cf overrides");
        storage.put("raft_log", b"k", b"v").await.unwrap();
        assert_eq!(
            storage.get("raft_log", b"k").await.unwrap(),
            Some(b"v".to_vec())
        );
    }

    #[test]
    fn test_rocks_config_default_values() {
        let config = RocksConfig::default();
        assert_eq!(config.block_cache_size_mb, 256);
        assert_eq!(config.write_buffer_size_mb, 64);
        assert_eq!(config.max_write_buffer_number, 4);
        assert_eq!(config.max_background_jobs, 4);
        assert_eq!(config.level_zero_slowdown_trigger, 20);
        assert_eq!(config.level_zero_stop_trigger, 36);
        // Lz4 compression for production
        assert!(matches!(
            config.compression,
            rocksdb::DBCompressionType::Lz4
        ));
        assert!(config.group_commit_interval_ms.is_none());
    }

    #[test]
    fn test_rocks_config_for_testing() {
        let config = RocksConfig::for_testing();
        assert_eq!(config.block_cache_size_mb, 8);
        assert_eq!(config.write_buffer_size_mb, 4);
        assert_eq!(config.max_write_buffer_number, 2);
        assert_eq!(config.max_background_jobs, 1);
        assert!(matches!(
            config.compression,
            rocksdb::DBCompressionType::None
        ));
        // Smaller than default
        let default = RocksConfig::default();
        assert!(config.block_cache_size_mb < default.block_cache_size_mb);
        assert!(config.write_buffer_size_mb < default.write_buffer_size_mb);
        assert!(config.max_background_jobs < default.max_background_jobs);
        assert!(config.group_commit_interval_ms.is_none());
    }

    #[tokio::test]
    async fn test_rocks_open_with_custom_config() {
        let tmp = TempDir::new().unwrap();
        let config = RocksConfig {
            block_cache_size_mb: 16,
            write_buffer_size_mb: 8,
            max_write_buffer_number: 2,
            max_background_jobs: 2,
            compression: rocksdb::DBCompressionType::None,
            level_zero_slowdown_trigger: 10,
            level_zero_stop_trigger: 20,
            durability: DurabilityPolicy::ReplicatedOnly,
            raft_log_cf: None,
            state_machine_cf: None,
            group_commit_interval_ms: None,
        };
        let storage = RocksStorage::open(
            tmp.path().to_str().unwrap(),
            vec!["raft_log", "state_machine", "metadata"],
            config,
        )
        .expect("should open with custom config");
        assert_eq!(
            storage.durability_policy(),
            DurabilityPolicy::ReplicatedOnly
        );
        storage
            .put("state_machine", b"key1", b"value1")
            .await
            .unwrap();
        let val = storage.get("state_machine", b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_default_durability_is_strict() {
        let config = RocksConfig::default();
        assert_eq!(
            config.durability,
            DurabilityPolicy::Strict,
            "Production default must be Strict to uphold Raft durability"
        );
    }

    #[test]
    fn test_testing_durability_is_test_only() {
        let config = RocksConfig::for_testing();
        assert_eq!(config.durability, DurabilityPolicy::TestOnly);
    }

    #[test]
    fn test_canonical_names() {
        assert_eq!(DurabilityPolicy::Strict.canonical_name(), "strict");
        assert_eq!(
            DurabilityPolicy::ReplicatedOnly.canonical_name(),
            "replicated_only"
        );
        assert_eq!(DurabilityPolicy::TestOnly.canonical_name(), "test_only");
    }

    #[test]
    fn test_only_strict_is_strict() {
        assert!(DurabilityPolicy::Strict.is_strict());
        assert!(!DurabilityPolicy::ReplicatedOnly.is_strict());
        assert!(!DurabilityPolicy::TestOnly.is_strict());
    }

    #[tokio::test]
    async fn test_strict_durability_persists_across_reopen() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let strict_cfg = RocksConfig {
            durability: DurabilityPolicy::Strict,
            ..RocksConfig::for_testing()
        };
        {
            let storage = RocksStorage::open(
                &path,
                vec!["raft_log", "state_machine", "metadata"],
                strict_cfg.clone(),
            )
            .expect("open strict");
            storage
                .put("state_machine", b"durable_key", b"durable_value")
                .await
                .expect("put must succeed under Strict");
        }
        let reopened = RocksStorage::open(
            &path,
            vec!["raft_log", "state_machine", "metadata"],
            strict_cfg,
        )
        .expect("reopen");
        let value = reopened
            .get("state_machine", b"durable_key")
            .await
            .expect("get after reopen");
        assert_eq!(
            value,
            Some(b"durable_value".to_vec()),
            "Strict-policy write must survive reopen (baseline durability check)"
        );
    }

    #[tokio::test]
    async fn test_durability_policy_passes_through_open() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = RocksConfig::for_testing();
        cfg.durability = DurabilityPolicy::Strict;
        let storage =
            RocksStorage::open(tmp.path().to_str().unwrap(), vec!["raft_log"], cfg).unwrap();
        assert_eq!(storage.durability_policy(), DurabilityPolicy::Strict);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn proptest_rocks_put_get_roundtrip(key: Vec<u8>, value: Vec<u8>) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (storage, _tmp) = open_test_db();
                storage.put("state_machine", &key, &value).await.unwrap();
                let got = storage.get("state_machine", &key).await.unwrap();
                prop_assert_eq!(got, Some(value));
                Ok(())
            })?;
        }

        #[test]
        fn proptest_rocks_delete_removes_key(key: Vec<u8>, value: Vec<u8>) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (storage, _tmp) = open_test_db();
                storage.put("state_machine", &key, &value).await.unwrap();
                storage.delete("state_machine", &key).await.unwrap();
                let got = storage.get("state_machine", &key).await.unwrap();
                prop_assert_eq!(got, None);
                Ok(())
            })?;
        }

        #[test]
        fn proptest_rocks_last_write_wins(key: Vec<u8>, val1: Vec<u8>, val2: Vec<u8>) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (storage, _tmp) = open_test_db();
                storage.put("state_machine", &key, &val1).await.unwrap();
                storage.put("state_machine", &key, &val2).await.unwrap();
                let got = storage.get("state_machine", &key).await.unwrap();
                prop_assert_eq!(got, Some(val2));
                Ok(())
            })?;
        }

        #[test]
        fn proptest_rocks_get_missing_returns_none(key: Vec<u8>) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (storage, _tmp) = open_test_db();
                let got = storage.get("state_machine", &key).await.unwrap();
                prop_assert_eq!(got, None);
                Ok(())
            })?;
        }
    }
}
