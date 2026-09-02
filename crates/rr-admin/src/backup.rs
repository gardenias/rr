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
use std::io::{BufReader, BufWriter};
use std::path::Path;
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
        let bytes =
            std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
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
    gz.finish().context("failed to finalize gzip stream")?;

    Ok(metadata)
}

/// Restore a previously-created `.tar.gz` backup into `data_dir`.
///
/// # Arguments
/// - `input`: the backup `.tar.gz` file
/// - `data_dir`: destination directory. MUST be empty (or absent).
///
/// # Errors
/// - If `input` doesn't exist
/// - If `data_dir` exists and is non-empty (refuse to clobber)
/// - If metadata.json is missing or format_version is incompatible
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
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data_dir {}", data_dir.display()))?;

    let metadata = read_metadata(input)?;
    if metadata.format_version > BACKUP_FORMAT_VERSION {
        return Err(anyhow!(
            "backup format_version {} is newer than this tool supports (max {})",
            metadata.format_version,
            BACKUP_FORMAT_VERSION
        ));
    }

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
        let dst = data_dir.join(rel);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to mkdir {}", parent.display()))?;
        }
        entry
            .unpack(&dst)
            .with_context(|| format!("failed to unpack {}", dst.display()))?;
    }

    if !metadata.data_sha256.is_empty() {
        let actual = sha256_of_dir(data_dir).context("failed to verify data checksum")?;
        if actual != metadata.data_sha256 {
            return Err(anyhow!(
                "backup integrity check failed: expected SHA-256 {}, got {}",
                metadata.data_sha256,
                actual
            ));
        }
    }

    Ok(metadata)
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
}
