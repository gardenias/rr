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
//! 1. The node MUST be stopped. We probe the RocksDB `LOCK` file for a
//!    held OS-level lock and refuse to proceed while one is held.
//!    On Unix this probes both `fcntl(F_SETLK)` (what RocksDB's
//!    PosixFileSystem uses) and `flock` lock namespaces; an unexpected
//!    probe error fails CLOSED (refuses) with a WARN log.
//!    On non-Unix platforms the probe is not implemented and only the
//!    existence of `LOCK` is checked — the operator is responsible for
//!    ensuring the node is stopped there.
//! 2. The data_dir MUST exist and look like a real rr data dir
//!    (contains either RocksDB column-family files or is empty —
//!    the bare existence of an arbitrary directory tree is rejected
//!    to prevent fat-finger of a wrong path wiping unrelated data).
//! 3. Operator MUST confirm via `--force` (there is no interactive
//!    prompt; the flag IS the confirmation).
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
/// `force = true` IS the operator confirmation (there is no interactive
/// prompt; the CLI help text and module docs describe the flag as such).
/// Tests pass `force = true` to keep them non-interactive.
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

/// Probe whether the RocksDB `LOCK` file in `data_dir` is currently
/// locked by another (presumably running) process.
///
/// RocksDB's PosixFileSystem takes an exclusive `fcntl(F_SETLK)` write
/// lock on `LOCK` for the lifetime of an open DB (falling back to
/// `flock` on systems without `fcntl`), so we probe BOTH lock
/// namespaces: a held lock in either one means the node is still
/// running (or crashed without closing the DB — same operational
/// meaning: do not wipe).
///
/// Both probes are non-destructive: on success WE acquire and
/// immediately release the lock by closing our own file description,
/// which never touches another process's lock.
///
/// Fail-closed: an unexpected probe error (e.g. `ENOLCK` on NFS)
/// returns `true` (with a WARN) — wiping is destructive, so ambiguity
/// must stop the operation, not silently allow it.
#[cfg(unix)]
fn rocks_lock_held(data_dir: &Path) -> bool {
    use std::os::unix::io::AsRawFd;

    let lock = data_dir.join("LOCK");
    // Read+write: fcntl(F_WRLCK) and flock(LOCK_EX) both require a
    // write-capable fd (EBADF otherwise on several platforms).
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock)
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return false,
        Err(e) => {
            tracing::warn!(
                "cannot open {} to probe the RocksDB lock ({e}); \
                 refusing to treat the node as stopped",
                lock.display()
            );
            return true;
        }
    };
    let fd = file.as_raw_fd();

    // Probe 1: POSIX record lock (fcntl), the primary RocksDB mechanism.
    let mut fl: libc::flock = unsafe { std::mem::zeroed() };
    fl.l_type = libc::F_WRLCK as libc::c_short;
    fl.l_whence = libc::SEEK_SET as libc::c_short;
    fl.l_start = 0;
    fl.l_len = 0; // to EOF
    let rc = unsafe { libc::fcntl(fd, libc::F_SETLK, &fl) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EACCES) | Some(libc::EAGAIN) => return true,
            _ => {
                tracing::warn!(
                    "fcntl lock probe on {} failed unexpectedly ({err}); \
                     refusing to treat the node as stopped",
                    lock.display()
                );
                return true;
            }
        }
    }
    // We acquired the fcntl lock; release it explicitly before probing
    // flock (dropping `file` would also release it, but be explicit).
    fl.l_type = libc::F_UNLCK as libc::c_short;
    unsafe { libc::fcntl(fd, libc::F_SETLK, &fl) };

    // Probe 2: BSD flock, RocksDB's documented fallback (ENOSYS systems).
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EWOULDBLOCK) | Some(libc::EACCES) => return true,
            _ => {
                tracing::warn!(
                    "flock probe on {} failed unexpectedly ({err}); \
                     refusing to treat the node as stopped",
                    lock.display()
                );
                return true;
            }
        }
    }
    false // both probes acquired the lock → nobody else holds it
}

/// Non-Unix fallback: there is no portable OS-lock probe on this
/// platform, so the live-node guard is NOT enforced here. Only the
/// existence fast-path is kept, and only in the negative: a missing
/// `LOCK` is conclusive, while a present one proves nothing (RocksDB
/// unlocks but does NOT delete `LOCK` on clean shutdown, so refusing
/// on existence alone would make `node-reset` unusable). The operator
/// MUST ensure the node is stopped before running `node-reset`.
#[cfg(not(unix))]
fn rocks_lock_held(data_dir: &Path) -> bool {
    let lock = data_dir.join("LOCK");
    if !lock.exists() {
        return false; // no LOCK file → nothing to probe
    }
    false // LOCK present but not probeable on this platform
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

    /// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    /// Regression: a LOCK file held by a live (flock'd) RocksDB process must
    /// make node-reset refuse the wipe. RocksDB leaves `LOCK` behind after a
    /// clean shutdown, so presence alone must NOT refuse — only a held lock.
    /// (Only the flock probe is exercised in-process: POSIX `fcntl` record
    /// locks are per-process, so a lock taken by this same process would not
    /// conflict with the probe — testing that path would require a fork.)
    #[cfg(unix)]
    #[test]
    fn refuses_when_lock_file_is_flock_held() {
        use std::os::unix::io::AsRawFd;

        let tmp = TempDir::new().unwrap();
        touch(tmp.path(), "CURRENT", b"MANIFEST-000001\n");
        touch(tmp.path(), "LOCK", b"");
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path().join("LOCK"))
            .unwrap();
        let rc = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(rc, 0, "test setup: flock must succeed");

        let err = reset_node_data_dir(tmp.path(), true).unwrap_err();
        assert!(
            err.to_string().contains("still be running"),
            "expected live-node refusal, got: {err}"
        );
        // The dir must be untouched.
        assert!(tmp.path().join("CURRENT").exists());

        unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_UN) };
    }

    /// Companion to the refusal test: a LOCK file left over from a clean
    /// shutdown (unlocked) must NOT block the reset.
    #[cfg(unix)]
    #[test]
    fn allows_when_lock_file_present_but_unlocked() {
        let tmp = TempDir::new().unwrap();
        touch(tmp.path(), "CURRENT", b"MANIFEST-000001\n");
        touch(tmp.path(), "LOCK", b"");
        let report = reset_node_data_dir(tmp.path(), true)
            .expect("stale-but-unlocked LOCK must not block reset");
        assert_eq!(report.files_removed, 2);
    }
}
