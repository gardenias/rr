//! Offline backup / restore of an `rr` node's data directory.
//!
//! # Format
//!
//! A backup is a gzip-compressed tar archive (`.tar.gz`) containing:
//!
//! ```text
//!   metadata.json          — backup descriptor (see [`BackupMetadata`])
//!   data/...               — the entire contents of the node's data_dir
//! ```
//!
//! # Required Operational Discipline
//!
//! This module does **offline** backup: the source node MUST be stopped
//! (`SIGTERM` → graceful shutdown) before `backup` runs, and the target
//! node's data_dir MUST be empty (or absent) before `restore` runs. This
//! guarantees RocksDB consistency without needing a Checkpoint API.
//!
//! Rolling-backup procedure in a 3-node cluster:
//! 1. Transfer leadership away from the target node
//!    (`rr-admin group transfer-leader`).
//! 2. Stop the target node (`SIGTERM`).
//! 3. Run `rr-admin backup --data-dir /var/lib/rr/data --output backup.tar.gz`.
//! 4. Start the target node again; it rejoins as a follower.
//! 5. Upload `backup.tar.gz` to long-term storage.
//!
//! Online / non-disruptive backup via snapshot-fetch RPC is planned for a
//! later milestone (Phase 3.1).

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Component, Path};
use tar::{Archive, Builder};

/// Backup format version. Increment when breaking changes land.
pub const BACKUP_FORMAT_VERSION: u32 = 1;

/// Descriptor written as `metadata.json` inside the tarball.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupMetadata {
    /// Backup format version. Reject restores from newer majors.
    pub format_version: u32,
    /// rr crate version at the moment of backup (e.g. `"0.1.0"`).
    pub rr_version: String,
    /// Optional: node_id the backup was taken from (for audit / confirmation).
    pub source_node_id: Option<u64>,
    /// UTC timestamp when the backup was produced.
    pub taken_at: DateTime<Utc>,
    /// Free-form comment (useful for human operators).
    #[serde(default)]
    pub comment: Option<String>,
    /// SHA-256 hex digest of all data files in the backup (sorted by relative
    /// path). Empty string for backups created before this field was added.
    #[serde(default)]
    pub data_sha256: String,
}

impl BackupMetadata {
    pub fn new(source_node_id: Option<u64>, comment: Option<String>) -> Self {
        Self {
            format_version: BACKUP_FORMAT_VERSION,
            rr_version: env!("CARGO_PKG_VERSION").to_string(),
            source_node_id,
            taken_at: Utc::now(),
            comment,
            data_sha256: String::new(),
        }
    }
}

/// Compute a deterministic SHA-256 digest over all files in `dir`.
///
/// Files are visited in sorted-path order; each file contributes its
/// relative path (UTF-8), a NUL separator, 8-byte big-endian length,
/// then its raw bytes. This ensures the digest changes whenever any
/// file is added, removed, renamed, or has its content modified.
fn sha256_of_dir(dir: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut paths: Vec<_> = walkdir::WalkDir::new(dir)
        .sort_by_file_name()
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect();
    paths.sort();
    for path in &paths {
        let rel = path
            .strip_prefix(dir)
            .context("path outside dir")?
            .to_string_lossy();
        hasher.update(rel.as_bytes());
        hasher.update(b"\0");
        // Stream the file through the hasher (SST files can be GiB-scale;
        // reading them whole would blow up rr-admin's RSS). The length
        // prefix comes from metadata, not the buffered content.
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let len = file
            .metadata()
            .with_context(|| format!("failed to stat {}", path.display()))?
            .len();
        hasher.update(len.to_be_bytes());
        let mut reader = BufReader::new(file);
        std::io::copy(&mut reader, &mut hasher)
            .with_context(|| format!("failed to read {}", path.display()))?;
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Create a `.tar.gz` backup of the node's data directory.
///
/// # Arguments
/// - `data_dir`: source directory (typically `/var/lib/rr/data`)
/// - `output`: path to the `.tar.gz` file to create
/// - `source_node_id`, `comment`: optional metadata
///
/// # Errors
/// - If `data_dir` doesn't exist or isn't a directory
/// - If `output`'s parent directory doesn't exist
/// - If the output file already exists (refuse to overwrite)
pub fn create_backup(
    data_dir: &Path,
    output: &Path,
    source_node_id: Option<u64>,
    comment: Option<String>,
) -> Result<BackupMetadata> {
    if !data_dir.exists() {
        return Err(anyhow!("data_dir does not exist: {}", data_dir.display()));
    }
    if !data_dir.is_dir() {
        return Err(anyhow!(
            "data_dir is not a directory: {}",
            data_dir.display()
        ));
    }
    if output.exists() {
        return Err(anyhow!(
            "output file already exists; refusing to overwrite: {}",
            output.display()
        ));
    }

    let mut metadata = BackupMetadata::new(source_node_id, comment);
    metadata.data_sha256 = sha256_of_dir(data_dir).context("failed to compute data checksum")?;
    let metadata_json =
        serde_json::to_vec_pretty(&metadata).context("failed to serialize backup metadata")?;

    let file = File::create(output)
        .with_context(|| format!("failed to create output file {}", output.display()))?;
    let gz = GzEncoder::new(BufWriter::new(file), Compression::default());
    let mut tar = Builder::new(gz);

    let mut meta_header = tar::Header::new_gnu();
    meta_header.set_path("metadata.json")?;
    meta_header.set_size(metadata_json.len() as u64);
    meta_header.set_mode(0o644);
    meta_header.set_mtime(metadata.taken_at.timestamp() as u64);
    meta_header.set_cksum();
    tar.append(&meta_header, &metadata_json[..])
        .context("failed to append metadata.json to archive")?;

    tar.append_dir_all("data", data_dir)
        .with_context(|| format!("failed to archive data_dir {}", data_dir.display()))?;

    let gz = tar.into_inner().context("failed to finalize tar archive")?;
    // gz.finish() hands back the BufWriter with buffered bytes still
    // pending; dropping it would swallow flush errors, and nothing ever
    // fsyncs. Do both explicitly so a successful return means the archive
    // is durably on disk.
    let mut writer = gz.finish().context("failed to finalize gzip stream")?;
    writer.flush().context("failed to flush backup output")?;
    writer
        .get_ref()
        .sync_all()
        .context("failed to fsync backup output")?;

    Ok(metadata)
}

/// Restore a previously-created `.tar.gz` backup into `data_dir`.
///
/// The archive is unpacked into a sibling temporary directory first and
/// the checksum is verified THERE; only on success is the verified
/// directory renamed into place. On any failure `data_dir` is left
/// untouched (as verified empty, or absent).
///
/// # Arguments
/// - `input`: the backup `.tar.gz` file
/// - `data_dir`: destination directory. MUST be empty (or absent).
///
/// # Errors
/// - If `input` doesn't exist
/// - If `data_dir` exists and is non-empty (refuse to clobber)
/// - If metadata.json is missing or format_version is incompatible
/// - If any tar entry escapes the data dir (tar-slip) or is not a
///   regular file/directory (symlinks/hardlinks could escape via
///   link targets; backups only ever contain files and dirs)
/// - If the post-unpack SHA-256 does not match the metadata
pub fn restore_backup(input: &Path, data_dir: &Path) -> Result<BackupMetadata> {
    if !input.exists() {
        return Err(anyhow!("backup file does not exist: {}", input.display()));
    }
    if data_dir.exists() {
        let empty = std::fs::read_dir(data_dir)
            .map(|mut it| it.next().is_none())
            .unwrap_or(true);
        if !empty {
            return Err(anyhow!(
                "data_dir exists and is non-empty; refusing to restore over existing data: {}",
                data_dir.display()
            ));
        }
    }

    let metadata = read_metadata(input)?;
    if metadata.format_version > BACKUP_FORMAT_VERSION {
        return Err(anyhow!(
            "backup format_version {} is newer than this tool supports (max {})",
            metadata.format_version,
            BACKUP_FORMAT_VERSION
        ));
    }

    // Unpack into a sibling temp dir, never directly into data_dir: a
    // checksum mismatch (or malicious archive) must leave data_dir
    // untouched. Sibling = same filesystem, so the final rename is atomic.
    let parent = data_dir
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let dir_name = data_dir
        .file_name()
        .ok_or_else(|| anyhow!("data_dir has no final component: {}", data_dir.display()))?;
    let tmp_dir = parent.join(format!(
        ".{}.rr-restore-tmp-{}",
        dir_name.to_string_lossy(),
        std::process::id()
    ));
    if tmp_dir.exists() {
        // Leftover from a crashed earlier restore by this tool; safe to clear.
        std::fs::remove_dir_all(&tmp_dir)
            .with_context(|| format!("failed to clear stale temp dir {}", tmp_dir.display()))?;
    }

    let unpack_result = unpack_and_verify(input, &tmp_dir, data_dir, &metadata);
    if let Err(e) = unpack_result {
        // Best-effort cleanup of our own temp dir; loud on failure.
        if let Err(cleanup) = std::fs::remove_dir_all(&tmp_dir) {
            tracing::warn!(
                "failed to remove temp dir {} after failed restore: {cleanup}",
                tmp_dir.display()
            );
        }
        return Err(e);
    }

    if data_dir.exists() {
        // Verified empty above; remove_dir only succeeds on empty dirs.
        std::fs::remove_dir(data_dir)
            .with_context(|| format!("failed to clear empty data_dir {}", data_dir.display()))?;
    }
    std::fs::rename(&tmp_dir, data_dir).with_context(|| {
        format!(
            "failed to move verified restore {} into place at {}",
            tmp_dir.display(),
            data_dir.display()
        )
    })?;

    Ok(metadata)
}

/// Unpack the `data/` subtree of `input` into `tmp_dir` and verify the
/// SHA-256 checksum against `metadata`. `data_dir` is used only for
/// error messages (it is the dir left untouched on failure).
fn unpack_and_verify(
    input: &Path,
    tmp_dir: &Path,
    data_dir: &Path,
    metadata: &BackupMetadata,
) -> Result<()> {
    std::fs::create_dir_all(tmp_dir)
        .with_context(|| format!("failed to create temp dir {}", tmp_dir.display()))?;

    let file = File::open(input)
        .with_context(|| format!("failed to open backup file {}", input.display()))?;
    let gz = GzDecoder::new(BufReader::new(file));
    let mut tar = Archive::new(gz);

    for entry in tar.entries().context("failed to read tar entries")? {
        let mut entry = entry.context("failed to read tar entry")?;
        let path = entry.path().context("invalid entry path")?.into_owned();
        if path == Path::new("metadata.json") {
            continue;
        }
        let Ok(rel) = path.strip_prefix("data") else {
            continue;
        };
        // Tar-slip guard: reject any entry that would escape the data dir.
        if rel.components().any(|c| {
            matches!(
                c,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        }) {
            return Err(anyhow!(
                "refusing to unpack tar entry with parent/root component \
                 (tar-slip protection): {} — {} was left untouched",
                path.display(),
                data_dir.display()
            ));
        }
        // Backups only contain regular files and directories; links could
        // escape the temp dir via their targets, so reject them outright.
        let entry_type = entry.header().entry_type();
        if !matches!(
            entry_type,
            tar::EntryType::Regular | tar::EntryType::Directory
        ) {
            return Err(anyhow!(
                "refusing to unpack tar entry of type {:?} (only files and dirs are \
                 restorable): {} — {} was left untouched",
                entry_type,
                path.display(),
                data_dir.display()
            ));
        }
        let dst = tmp_dir.join(rel);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to mkdir {}", parent.display()))?;
        }
        entry
            .unpack(&dst)
            .with_context(|| format!("failed to unpack {}", dst.display()))?;
    }

    if !metadata.data_sha256.is_empty() {
        let actual = sha256_of_dir(tmp_dir).context("failed to verify data checksum")?;
        if actual != metadata.data_sha256 {
            return Err(anyhow!(
                "backup integrity check failed: expected SHA-256 {}, got {} — \
                 {} was left untouched",
                metadata.data_sha256,
                actual,
                data_dir.display()
            ));
        }
    }
    Ok(())
}

/// Read just the `metadata.json` from a backup tarball without unpacking everything.
pub fn read_metadata(input: &Path) -> Result<BackupMetadata> {
    let file = File::open(input)
        .with_context(|| format!("failed to open backup file {}", input.display()))?;
    let gz = GzDecoder::new(BufReader::new(file));
    let mut tar = Archive::new(gz);

    for entry in tar.entries().context("failed to read tar entries")? {
        let mut entry = entry.context("failed to read tar entry")?;
        let path = entry.path().context("invalid entry path")?.into_owned();
        if path == Path::new("metadata.json") {
            let mut buf = Vec::new();
            std::io::copy(&mut entry, &mut buf).context("failed to read metadata.json")?;
            let meta: BackupMetadata =
                serde_json::from_slice(&buf).context("failed to parse metadata.json")?;
            return Ok(meta);
        }
    }
    Err(anyhow!("backup is missing metadata.json"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn populate_fake_data_dir(dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir.join("raft_log"))?;
        std::fs::create_dir_all(dir.join("state_machine"))?;
        let mut f = std::fs::File::create(dir.join("raft_log/000001.sst"))?;
        f.write_all(b"fake sst content")?;
        let mut f = std::fs::File::create(dir.join("state_machine/CURRENT"))?;
        f.write_all(b"MANIFEST-000001\n")?;
        let mut f = std::fs::File::create(dir.join("IDENTITY"))?;
        f.write_all(b"node-1")?;
        Ok(())
    }

    #[test]
    fn backup_then_restore_roundtrip() {
        let src = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let restore_dir = TempDir::new().unwrap();
        let backup_path = backup_dir.path().join("backup.tar.gz");

        populate_fake_data_dir(src.path()).unwrap();

        let meta = create_backup(src.path(), &backup_path, Some(42), Some("nightly".into()))
            .expect("backup should succeed");
        assert_eq!(meta.source_node_id, Some(42));
        assert_eq!(meta.format_version, BACKUP_FORMAT_VERSION);

        let restore_path = restore_dir.path().join("data");
        let restored_meta = restore_backup(&backup_path, &restore_path).unwrap();
        assert_eq!(restored_meta.source_node_id, Some(42));

        let identity = std::fs::read_to_string(restore_path.join("IDENTITY")).unwrap();
        assert_eq!(identity, "node-1");
        let sst = std::fs::read(restore_path.join("raft_log/000001.sst")).unwrap();
        assert_eq!(sst, b"fake sst content");
    }

    #[test]
    fn backup_refuses_overwrite() {
        let src = TempDir::new().unwrap();
        populate_fake_data_dir(src.path()).unwrap();
        let out_dir = TempDir::new().unwrap();
        let out = out_dir.path().join("backup.tar.gz");
        std::fs::write(&out, b"already here").unwrap();

        let err = create_backup(src.path(), &out, None, None).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn backup_errors_on_missing_data_dir() {
        let out_dir = TempDir::new().unwrap();
        let err = create_backup(
            Path::new("/nonexistent/path"),
            &out_dir.path().join("b.tar.gz"),
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn restore_refuses_nonempty_target() {
        let src = TempDir::new().unwrap();
        populate_fake_data_dir(src.path()).unwrap();
        let out_dir = TempDir::new().unwrap();
        let backup_path = out_dir.path().join("backup.tar.gz");
        create_backup(src.path(), &backup_path, None, None).unwrap();

        let dst = TempDir::new().unwrap();
        std::fs::write(dst.path().join("stale"), b"junk").unwrap();
        let err = restore_backup(&backup_path, dst.path()).unwrap_err();
        assert!(err.to_string().contains("non-empty"));
    }

    #[test]
    fn read_metadata_without_full_extract() {
        let src = TempDir::new().unwrap();
        populate_fake_data_dir(src.path()).unwrap();
        let out_dir = TempDir::new().unwrap();
        let backup_path = out_dir.path().join("backup.tar.gz");
        create_backup(src.path(), &backup_path, Some(7), Some("test".into())).unwrap();

        let meta = read_metadata(&backup_path).unwrap();
        assert_eq!(meta.source_node_id, Some(7));
        assert_eq!(meta.comment.as_deref(), Some("test"));
    }

    #[test]
    fn restore_rejects_newer_format_version() {
        let out_dir = TempDir::new().unwrap();
        let backup_path = out_dir.path().join("future.tar.gz");

        let file = File::create(&backup_path).unwrap();
        let gz = GzEncoder::new(file, Compression::default());
        let mut tar = Builder::new(gz);
        let future_meta = serde_json::json!({
            "format_version": BACKUP_FORMAT_VERSION + 99,
            "rr_version": "99.0.0",
            "source_node_id": null,
            "taken_at": "2099-01-01T00:00:00Z",
            "comment": null,
        });
        let data = serde_json::to_vec(&future_meta).unwrap();
        let mut h = tar::Header::new_gnu();
        h.set_path("metadata.json").unwrap();
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tar.append(&h, &data[..]).unwrap();
        let gz = tar.into_inner().unwrap();
        gz.finish().unwrap();

        let dst = TempDir::new().unwrap();
        let dst_path = dst.path().join("data");
        let err = restore_backup(&backup_path, &dst_path).unwrap_err();
        assert!(err.to_string().contains("newer than this tool supports"));
    }

    /// Build a tar.gz from raw (path, bytes) entries plus a metadata.json —
    /// used to craft archives `create_backup` itself would never produce.
    /// `evil_paths` are written into the header verbatim (bypassing the
    /// tar builder's own `..` rejection) to simulate malicious archives.
    fn build_raw_tarball(path: &Path, metadata: &BackupMetadata, entries: &[(&str, &[u8])]) {
        let file = File::create(path).unwrap();
        let gz = GzEncoder::new(file, Compression::default());
        let mut tar = Builder::new(gz);
        let meta_json = serde_json::to_vec(metadata).unwrap();
        let mut h = tar::Header::new_gnu();
        h.set_path("metadata.json").unwrap();
        h.set_size(meta_json.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tar.append(&h, &meta_json[..]).unwrap();
        for (name, bytes) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_size(bytes.len() as u64);
            h.set_mode(0o644);
            // Raw name-field write: set_path() itself refuses `..`.
            let name = name.as_bytes();
            h.as_mut_bytes()[..name.len()].copy_from_slice(name);
            h.set_cksum();
            tar.append(&h, &bytes[..]).unwrap();
        }
        let gz = tar.into_inner().unwrap();
        gz.finish().unwrap();
    }

    /// Tar-slip regression: an entry with a `..` component must be rejected
    /// and the target dir must be left untouched (nothing unpacked).
    #[test]
    fn restore_rejects_tar_slip_entry() {
        let out_dir = TempDir::new().unwrap();
        let backup_path = out_dir.path().join("evil.tar.gz");
        let meta = BackupMetadata::new(None, None); // no checksum field
        build_raw_tarball(&backup_path, &meta, &[("data/../evil.txt", b"pwned")]);

        let dst = TempDir::new().unwrap();
        let data_dir = dst.path().join("data");
        let err = restore_backup(&backup_path, &data_dir).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("tar-slip"),
            "expected tar-slip rejection, got: {msg}"
        );
        assert!(msg.contains("left untouched"), "got: {msg}");
        assert!(
            !dst.path().join("evil.txt").exists(),
            "slip payload must not escape the data dir"
        );
        assert!(!data_dir.exists(), "data_dir must be left untouched");
    }

    /// A symlink entry could escape via its link target; only regular files
    /// and directories are restorable.
    #[test]
    fn restore_rejects_symlink_entries() {
        let out_dir = TempDir::new().unwrap();
        let backup_path = out_dir.path().join("link.tar.gz");
        let meta = BackupMetadata::new(None, None);

        let file = File::create(&backup_path).unwrap();
        let gz = GzEncoder::new(file, Compression::default());
        let mut tar = Builder::new(gz);
        let meta_json = serde_json::to_vec(&meta).unwrap();
        let mut h = tar::Header::new_gnu();
        h.set_path("metadata.json").unwrap();
        h.set_size(meta_json.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tar.append(&h, &meta_json[..]).unwrap();
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Symlink);
        h.set_path("data/link").unwrap();
        h.set_link_name("../../etc/passwd").unwrap();
        h.set_size(0);
        h.set_mode(0o777);
        h.set_cksum();
        tar.append(&h, std::io::empty()).unwrap();
        let gz = tar.into_inner().unwrap();
        gz.finish().unwrap();

        let dst = TempDir::new().unwrap();
        let data_dir = dst.path().join("data");
        let err = restore_backup(&backup_path, &data_dir).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("only files and dirs"),
            "expected entry-type rejection, got: {msg}"
        );
        assert!(!data_dir.exists(), "data_dir must be left untouched");
    }

    /// Checksum-mismatch regression: the unpacked temp dir must be removed
    /// and the error must tell the operator the target dir was untouched.
    #[test]
    fn restore_checksum_mismatch_leaves_data_dir_untouched() {
        let out_dir = TempDir::new().unwrap();
        let backup_path = out_dir.path().join("bad.tar.gz");
        let mut meta = BackupMetadata::new(None, None);
        meta.data_sha256 = "0".repeat(64); // bogus digest
        build_raw_tarball(&backup_path, &meta, &[("data/IDENTITY", b"node-1")]);

        let dst = TempDir::new().unwrap();
        let data_dir = dst.path().join("data");
        let err = restore_backup(&backup_path, &data_dir).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("integrity check failed"),
            "expected checksum failure, got: {msg}"
        );
        assert!(msg.contains("left untouched"), "got: {msg}");
        assert!(!data_dir.exists(), "data_dir must be left untouched");
        assert_eq!(
            std::fs::read_dir(dst.path()).unwrap().count(),
            0,
            "temp dir must be cleaned up after a failed restore"
        );
    }

    /// A pre-existing EMPTY data_dir is accepted (rename replaces it).
    #[test]
    fn restore_into_existing_empty_dir() {
        let src = TempDir::new().unwrap();
        populate_fake_data_dir(src.path()).unwrap();
        let out_dir = TempDir::new().unwrap();
        let backup_path = out_dir.path().join("backup.tar.gz");
        create_backup(src.path(), &backup_path, None, None).unwrap();

        let dst = TempDir::new().unwrap();
        let data_dir = dst.path().join("data");
        std::fs::create_dir(&data_dir).unwrap();
        restore_backup(&backup_path, &data_dir).unwrap();
        assert_eq!(
            std::fs::read_to_string(data_dir.join("IDENTITY")).unwrap(),
            "node-1"
        );
    }
}
