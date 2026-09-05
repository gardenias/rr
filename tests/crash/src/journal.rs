//! Append-only JSONL journals: the **ack log** (oracle ground truth) and the
//! **fault log** (kill/restart event record for replay and forensics).
//!
//! Both follow the same durability discipline as the contract under test:
//! every record is written, flushed, and `fdatasync`'d before the writer
//! proceeds. The ack log's fsync-before-continue mirrors the server's
//! fsync-before-ACK (WS-4/INV-1): an entry present in the ack log was
//! client-observably acknowledged, full stop.
//!
//! The files live under `target/crash-tests/<scenario>/` (not a `TempDir`)
//! so they survive a failed test run for CI artifact upload and local
//! forensics.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Milliseconds since the Unix epoch, for log correlation across processes.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// One client-observed acknowledged write.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckRecord {
    pub request_id: u64,
    /// Idempotency `request_type` namespace ([`crate::REQUEST_TYPE`]).
    pub key: String,
    /// FNV-1a64 hex of the payload the server echoed for this write
    /// ([`crate::payload_hash`]).
    pub payload_hash: String,
    pub ts_ms: u64,
}

/// One fault-injection or lifecycle event. `planned_offset_ms` is drawn from
/// the seeded RNG before the round starts; replaying with the same
/// `CRASH_SEED` reproduces the same `(round, node, planned_offset_ms)`
/// sequence regardless of wall-clock jitter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FaultEvent {
    pub ts_ms: u64,
    /// `plan` | `sigkill` | `sigterm` | `restart` | `rejoined` | `note`.
    pub event: String,
    pub seed: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub round: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub planned_offset_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// An append-only JSONL journal with fdatasync-after-every-record.
pub struct Journal {
    file: File,
    path: PathBuf,
}

impl Journal {
    /// Create (truncating) or replace the journal at `path`.
    pub fn create(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create journal dir {}", parent.display()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .with_context(|| format!("create journal {}", path.display()))?;
        Ok(Self { file, path })
    }

    /// Append one record and fdatasync. This is intentionally synchronous:
    /// the caller (driver hot loop) must not observe an ack before the
    /// ground-truth record is on stable storage.
    pub fn append<T: Serialize>(&mut self, record: &T) -> Result<()> {
        let mut line = serde_json::to_vec(record).context("serialize journal record")?;
        line.push(b'\n');
        self.file
            .write_all(&line)
            .with_context(|| format!("append to {}", self.path.display()))?;
        // fdatasync(2): data + minimal metadata, matching the ack-boundary
        // contract (full fsync would also flush unrelated inode metadata).
        self.file
            .sync_data()
            .with_context(|| format!("fdatasync {}", self.path.display()))?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read back every record in append order.
    pub fn read_all<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e).with_context(|| format!("open {}", path.display())),
        };
        let mut out = Vec::new();
        for (lineno, line) in BufReader::new(file).lines().enumerate() {
            let line = line.with_context(|| format!("read {}:{}", path.display(), lineno + 1))?;
            if line.trim().is_empty() {
                continue;
            }
            out.push(
                serde_json::from_str(&line)
                    .with_context(|| format!("parse {}:{}", path.display(), lineno + 1))?,
            );
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_log_roundtrip_preserves_order_and_fields() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("ack-log.jsonl");
        {
            let mut log = Journal::create(&path).unwrap();
            for i in 1..=5u64 {
                log.append(&AckRecord {
                    request_id: i,
                    key: crate::REQUEST_TYPE.to_string(),
                    payload_hash: crate::payload_hash(&crate::payload_for(i)),
                    ts_ms: now_ms(),
                })
                .unwrap();
            }
        } // close the writer before reading back
        let records: Vec<AckRecord> = Journal::read_all(&path).unwrap();
        assert_eq!(records.len(), 5);
        for (i, r) in records.iter().enumerate() {
            let id = i as u64 + 1;
            assert_eq!(r.request_id, id);
            assert_eq!(r.payload_hash, crate::payload_hash(&crate::payload_for(id)));
        }
    }

    #[test]
    fn fault_log_roundtrip_skips_absent_optionals() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("fault-log.jsonl");
        {
            let mut log = Journal::create(&path).unwrap();
            log.append(&FaultEvent {
                ts_ms: now_ms(),
                event: "plan".to_string(),
                seed: 42,
                node: None,
                round: None,
                planned_offset_ms: None,
                detail: Some("rounds=3".to_string()),
            })
            .unwrap();
            log.append(&FaultEvent {
                ts_ms: now_ms(),
                event: "sigkill".to_string(),
                seed: 42,
                node: Some(2),
                round: Some(1),
                planned_offset_ms: Some(1234),
                detail: None,
            })
            .unwrap();
        }
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.lines().next().unwrap().contains("planned_offset_ms"));
        let events: Vec<FaultEvent> = Journal::read_all(&path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].node, Some(2));
        assert_eq!(events[1].planned_offset_ms, Some(1234));
    }

    #[test]
    fn read_all_missing_file_is_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        let records: Vec<AckRecord> = Journal::read_all(&tmp.path().join("nope.jsonl")).unwrap();
        assert!(records.is_empty());
    }
}
