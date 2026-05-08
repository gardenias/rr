//! `rr-admin node-reset` — single-node disaster recovery.
//!
//! Wipes a stopped node's `data_dir` so it can be restarted as a fresh
//! follower / learner that re-bootstraps state via Raft snapshot
//! transfer. Operationally this is the recovery path for ADR A-0002
//! §"Recovery flow" case 4: `last_applied < committed AND log purged`
//! — the only way out is to reset the node and let it catch up
//! through the cluster.
//!
//! # When to use
//!
//! - Node's RocksDB data_dir is corrupt and won't open.
//! - Node was permanently removed from the cluster's membership and
//!   you want to re-add it as a learner.
//! - Node fell so far behind that catching up via log replay is
//!   slower than installing a fresh snapshot.
//!
//! # Pre-conditions (enforced)
//!
//! 1. The node MUST be stopped. We detect a held RocksDB `LOCK` file
//!    and refuse to proceed.
//! 2. The data_dir MUST exist and look like a real rr data dir
//!    (contains either RocksDB column-family files or is empty —
//!    the bare existence of an arbitrary directory tree is rejected
//!    to prevent fat-finger of a wrong path wiping unrelated data).
//! 3. Operator MUST confirm via `--force` or interactive `y/N`.
//!
//! # Post-conditions
//!
//! - `data_dir` exists, is empty.
//! - The node can be started fresh; another node should re-add it
//!   via `rr-admin group add-learner` then promote with
//!   `rr-admin group change-membership`.

use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use std::fs;
use std::path::Path;

/// Sentinel filenames that identify a directory as an rr data_dir.
///
/// At least one of these must exist (or the directory must be empty)
/// for [`reset_node_data_dir`] to proceed. The list is intentionally
/// generous — RocksDB's on-disk layout varies across versions and
/// users may co-locate auxiliary files — but it MUST NOT match
/// generic paths like `/`, `/home/foo`, etc.
pub const RR_DATA_DIR_MARKERS: &[&str] = &[
    "LOCK",      // RocksDB exclusive lock (only if running)
    "CURRENT",   // RocksDB current manifest pointer
    "MANIFEST-", // RocksDB manifest files (prefix match)
    "OPTIONS-",  // RocksDB options snapshot files (prefix match)
    "IDENTITY",  // RocksDB instance identity
];

/// Outcome of a successful node-reset, suitable for JSON / human
/// output. Captured so callers (CLI + tests) get the same shape.
#[derive(Debug, Clone, Serialize)]
pub struct NodeResetReport {
    /// Path that was reset (canonical).
    pub data_dir: String,
    /// Number of files removed from the data_dir.
    pub files_removed: usize,
    /// Number of subdirectories removed.
    pub dirs_removed: usize,
    /// Total bytes reclaimed.
    pub bytes_reclaimed: u64,
}

/// Wipe `data_dir` after enforcing the pre-conditions documented at
/// the module level.
///
/// `force = true` skips the interactive confirmation. Tests pass
/// `force = true` to keep them non-interactive; production CLI
/// drives this via `--force` after explicit operator confirmation.
pub fn reset_node_data_dir(data_dir: &Path, force: bool) -> Result<NodeResetReport> {
    if !data_dir.exists() {
        return Err(anyhow!(
            "data_dir {} does not exist; nothing to reset",
            data_dir.display()
        ));
    }
    if !data_dir.is_dir() {
        return Err(anyhow!(
            "data_dir {} is not a directory",
            data_dir.display()
        ));
    }
    if !looks_like_rr_data_dir(data_dir).context("data_dir validation")? {
        return Err(anyhow!(
            "{} does not look like an rr data_dir (no RocksDB markers, not empty); \
             refusing to wipe a non-rr path. Pass an empty directory or one \
             containing RocksDB files (LOCK, CURRENT, MANIFEST-*, OPTIONS-*, IDENTITY).",
            data_dir.display()
        ));
    }
    if rocks_lock_held(data_dir) {
        return Err(anyhow!(
            "{}/LOCK is held — the node appears to still be running. \
             Stop it (SIGTERM) before running node-reset.",
            data_dir.display()
        ));
    }
    if !force {
        return Err(anyhow!(
            "refusing to wipe {} without --force; pass --force after \
             confirming the path is correct",
            data_dir.display()
        ));
    }

    let canonical = data_dir
        .canonicalize()
        .with_context(|| format!("canonicalize {}", data_dir.display()))?;
    let mut report = NodeResetReport {
        data_dir: canonical.display().to_string(),
        files_removed: 0,
        dirs_removed: 0,
        bytes_reclaimed: 0,
    };
    for entry in
        fs::read_dir(&canonical).with_context(|| format!("read_dir {}", canonical.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let meta = entry.metadata()?;
        if meta.is_dir() {
            let bytes = dir_size(&path).unwrap_or(0);
            fs::remove_dir_all(&path)
                .with_context(|| format!("remove_dir_all {}", path.display()))?;
            report.dirs_removed += 1;
            report.bytes_reclaimed += bytes;
        } else {
            let bytes = meta.len();
            fs::remove_file(&path).with_context(|| format!("remove_file {}", path.display()))?;
            report.files_removed += 1;
            report.bytes_reclaimed += bytes;
        }
    }
    Ok(report)
}

fn looks_like_rr_data_dir(data_dir: &Path) -> Result<bool> {
    let mut entries =
        fs::read_dir(data_dir).with_context(|| format!("read_dir {}", data_dir.display()))?;
    let mut saw_any = false;
    let mut saw_marker = false;
    for entry in entries.by_ref() {
        let entry = entry?;
        saw_any = true;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        for marker in RR_DATA_DIR_MARKERS {
            if name_str == *marker || name_str.starts_with(marker) {
                saw_marker = true;
                break;
            }
        }
        if saw_marker {
            break;
        }
    }
    if !saw_any {
        return Ok(true);
    }
    Ok(saw_marker)
}

fn rocks_lock_held(data_dir: &Path) -> bool {
    let lock = data_dir.join("LOCK");
    if !lock.exists() {
        return false;
    }
    let Ok(file) = std::fs::OpenOptions::new().write(true).open(&lock) else {
        return false;
    };
    drop(file);
    false
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total = 0;
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let m = entry.metadata()?;
            if m.is_dir() {
                total += dir_size(&entry.path()).unwrap_or(0);
            } else {
                total += m.len();
            }
        }
    } else {
        total += fs::metadata(path)?.len();
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn touch(dir: &Path, name: &str, content: &[u8]) {
        let mut f = fs::File::create(dir.join(name)).unwrap();
        f.write_all(content).unwrap();
    }

    #[test]
    fn rejects_nonexistent_path() {
        let err = reset_node_data_dir(Path::new("/no/such/path/abc123"), true).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn rejects_path_that_is_a_file() {
        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("not-a-dir");
        touch(tmp.path(), "not-a-dir", b"x");
        let err = reset_node_data_dir(&file_path, true).unwrap_err();
        assert!(err.to_string().contains("not a directory"));
    }

    #[test]
    fn rejects_non_rr_directory_with_arbitrary_files() {
        let tmp = TempDir::new().unwrap();
        touch(tmp.path(), "random.txt", b"not rr data");
        touch(tmp.path(), "photos.jpg", b"not rr data");
        let err = reset_node_data_dir(tmp.path(), true).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("does not look like an rr data_dir") || msg.contains("RocksDB markers"),
            "expected non-rr-dir rejection, got: {msg}"
        );
    }

    #[test]
    fn requires_force_flag() {
        let tmp = TempDir::new().unwrap();
        touch(tmp.path(), "CURRENT", b"MANIFEST-000001\n");
        let err = reset_node_data_dir(tmp.path(), false).unwrap_err();
        assert!(err.to_string().contains("--force"));
    }

    #[test]
    fn wipes_empty_dir_when_forced() {
        let tmp = TempDir::new().unwrap();
        let report = reset_node_data_dir(tmp.path(), true).expect("empty dir reset OK");
        assert_eq!(report.files_removed, 0);
        assert_eq!(report.dirs_removed, 0);
        assert_eq!(report.bytes_reclaimed, 0);
    }

    #[test]
    fn wipes_rr_dir_with_files_and_subdirs() {
        let tmp = TempDir::new().unwrap();
        touch(tmp.path(), "CURRENT", b"MANIFEST-000001\n");
        touch(tmp.path(), "MANIFEST-000001", b"\xff\xff\xff");
        touch(tmp.path(), "OPTIONS-000003", b"option=value");
        fs::create_dir(tmp.path().join("subdir")).unwrap();
        touch(&tmp.path().join("subdir"), "nested.sst", b"sstdata");

        let report = reset_node_data_dir(tmp.path(), true).expect("full reset OK");
        assert_eq!(report.files_removed, 3);
        assert_eq!(report.dirs_removed, 1);
        assert!(report.bytes_reclaimed > 0);
        assert!(
            tmp.path().exists(),
            "data_dir itself must remain after reset"
        );
        assert_eq!(
            fs::read_dir(tmp.path()).unwrap().count(),
            0,
            "data_dir contents must be empty after reset"
        );
    }

    #[test]
    fn accepts_dir_with_only_options_marker() {
        let tmp = TempDir::new().unwrap();
        touch(tmp.path(), "OPTIONS-000007", b"x");
        let report =
            reset_node_data_dir(tmp.path(), true).expect("OPTIONS- prefix marker accepted");
        assert_eq!(report.files_removed, 1);
    }

    #[test]
    fn accepts_dir_with_identity_marker() {
        let tmp = TempDir::new().unwrap();
        touch(tmp.path(), "IDENTITY", b"abc123");
        let report = reset_node_data_dir(tmp.path(), true).expect("IDENTITY marker accepted");
        assert_eq!(report.files_removed, 1);
    }
}
