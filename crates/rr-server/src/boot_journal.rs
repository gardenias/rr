use std::fmt::{Display, Formatter};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::runtime::BuildSnapshot;

pub const MAX_BOOT_RECORDS: usize = 100;
pub const DEFAULT_BOOT_PAGE_SIZE: usize = 50;
pub const MAX_BOOT_PAGE_SIZE: usize = 200;
const JOURNAL_FILE_NAME: &str = "boot-journal.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ShutdownState {
    Running,
    Graceful,
    AbnormalOrUncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootPersistence {
    Volatile,
    Persistent,
}

impl BootPersistence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Volatile => "VOLATILE",
            Self::Persistent => "PERSISTENT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootRecord {
    pub boot_id: String,
    pub started_at_unix_ms: u64,
    pub ended_at_unix_ms: Option<u64>,
    pub shutdown_state: ShutdownState,
    pub build: BuildSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootRecordPage {
    pub records: Vec<BootRecord>,
    pub next_cursor: Option<String>,
}

#[derive(Debug)]
pub struct BootJournalError(String);

impl Display for BootJournalError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for BootJournalError {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalFile {
    version: u32,
    records: Vec<BootRecord>,
}

#[derive(Debug)]
struct Inner {
    path: Option<PathBuf>,
    records: Vec<BootRecord>,
}

#[derive(Debug, Clone)]
pub struct BootJournal {
    inner: Arc<Mutex<Inner>>,
}

impl BootJournal {
    pub fn open(
        directory: Option<&Path>,
        boot_id: String,
        started_at_unix_ms: u64,
        build: BuildSnapshot,
    ) -> Result<Self, BootJournalError> {
        let path = directory.map(|directory| directory.join(JOURNAL_FILE_NAME));
        let mut records = match &path {
            Some(path) if path.exists() => {
                let bytes = fs::read(path).map_err(|error| {
                    BootJournalError(format!("read boot journal {}: {error}", path.display()))
                })?;
                let journal: JournalFile = serde_json::from_slice(&bytes).map_err(|error| {
                    BootJournalError(format!("decode boot journal {}: {error}", path.display()))
                })?;
                if journal.version != 1 {
                    return Err(BootJournalError(format!(
                        "unsupported boot journal version {} in {}",
                        journal.version,
                        path.display()
                    )));
                }
                journal.records
            }
            _ => Vec::new(),
        };
        for record in &mut records {
            if record.shutdown_state == ShutdownState::Running {
                record.shutdown_state = ShutdownState::AbnormalOrUncertain;
                record.ended_at_unix_ms = None;
            }
        }
        records.push(BootRecord {
            boot_id,
            started_at_unix_ms,
            ended_at_unix_ms: None,
            shutdown_state: ShutdownState::Running,
            build,
        });
        if records.len() > MAX_BOOT_RECORDS {
            records.drain(..records.len() - MAX_BOOT_RECORDS);
        }
        let journal = Self {
            inner: Arc::new(Mutex::new(Inner { path, records })),
        };
        journal.persist()?;
        Ok(journal)
    }

    pub fn persistence(&self) -> BootPersistence {
        let inner = self.lock();
        if inner.path.is_some() {
            BootPersistence::Persistent
        } else {
            BootPersistence::Volatile
        }
    }

    pub fn current(&self) -> BootRecord {
        self.lock()
            .records
            .last()
            .expect("a boot journal always contains its current boot")
            .clone()
    }

    pub fn list(
        &self,
        cursor: Option<&str>,
        page_size: Option<usize>,
    ) -> Result<BootRecordPage, BootJournalError> {
        let page_size = page_size.unwrap_or(DEFAULT_BOOT_PAGE_SIZE);
        if page_size == 0 || page_size > MAX_BOOT_PAGE_SIZE {
            return Err(BootJournalError(format!(
                "invalid boot page size {page_size}; expected 1..={MAX_BOOT_PAGE_SIZE}"
            )));
        }
        let offset = match cursor {
            Some(cursor) => cursor
                .parse::<usize>()
                .map_err(|_| BootJournalError(format!("invalid boot journal cursor {cursor:?}")))?,
            None => 0,
        };
        let inner = self.lock();
        if offset > inner.records.len() {
            return Err(BootJournalError(
                "boot journal cursor is outside the retained history".into(),
            ));
        }
        let records: Vec<_> = inner
            .records
            .iter()
            .rev()
            .skip(offset)
            .take(page_size)
            .cloned()
            .collect();
        let next_offset = offset + records.len();
        let next_cursor = (next_offset < inner.records.len()).then(|| next_offset.to_string());
        Ok(BootRecordPage {
            records,
            next_cursor,
        })
    }

    pub fn graceful_shutdown(&self) -> Result<(), BootJournalError> {
        {
            let mut inner = self.lock();
            let current = inner
                .records
                .last_mut()
                .expect("a boot journal always contains its current boot");
            if current.shutdown_state == ShutdownState::Graceful {
                return Ok(());
            }
            current.shutdown_state = ShutdownState::Graceful;
            current.ended_at_unix_ms = Some(unix_time_millis());
        }
        self.persist()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn persist(&self) -> Result<(), BootJournalError> {
        let inner = self.lock();
        let Some(path) = &inner.path else {
            return Ok(());
        };
        let directory = path
            .parent()
            .expect("journal path always has a parent directory");
        fs::create_dir_all(directory).map_err(|error| {
            BootJournalError(format!(
                "create boot journal directory {}: {error}",
                directory.display()
            ))
        })?;
        let temporary = directory.join(format!("{JOURNAL_FILE_NAME}.tmp"));
        let write_result = (|| -> Result<(), BootJournalError> {
            let mut file = File::create(&temporary).map_err(|error| {
                BootJournalError(format!("create {}: {error}", temporary.display()))
            })?;
            serde_json::to_writer_pretty(
                &mut file,
                &JournalFile {
                    version: 1,
                    records: inner.records.clone(),
                },
            )
            .map_err(|error| BootJournalError(format!("encode boot journal: {error}")))?;
            file.write_all(b"\n")
                .and_then(|()| file.sync_all())
                .map_err(|error| {
                    BootJournalError(format!("flush {}: {error}", temporary.display()))
                })?;
            fs::rename(&temporary, path).map_err(|error| {
                BootJournalError(format!(
                    "replace boot journal {} with {}: {error}",
                    path.display(),
                    temporary.display()
                ))
            })?;
            #[cfg(unix)]
            File::open(directory)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    BootJournalError(format!(
                        "sync boot journal directory {}: {error}",
                        directory.display()
                    ))
                })?;
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        write_result
    }
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::BuildSnapshot;
    use rr_core::ProtocolVersion;

    fn build(version: &str) -> BuildSnapshot {
        BuildSnapshot {
            rr_version: "0.2.0".into(),
            app_version: version.into(),
            git_commit: "abc123".into(),
            git_dirty: false,
            build_profile: "debug".into(),
            admin_protocol: ProtocolVersion {
                major: 1,
                minor: 0,
                min_peer_minor: 0,
            },
            raft_protocol: ProtocolVersion {
                major: 1,
                minor: 0,
                min_peer_minor: 0,
            },
            config_fingerprint: "deadbeef".into(),
        }
    }

    fn open_journal(dir: Option<&std::path::Path>, boot: &str) -> BootJournal {
        BootJournal::open(dir, boot.into(), 1_700_000_000_000, build(boot)).unwrap()
    }

    #[test]
    fn interrupted_boot_is_abnormal_or_uncertain_without_fake_end_time() {
        let temp = tempfile::tempdir().unwrap();
        let first = open_journal(Some(temp.path()), "first");
        let first_id = first.current().boot_id.clone();
        drop(first);

        let second = open_journal(Some(temp.path()), "second");
        let records = second.list(None, Some(10)).unwrap().records;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].boot_id, "second");
        assert_eq!(records[0].shutdown_state, ShutdownState::Running);
        let recovered = records
            .iter()
            .find(|record| record.boot_id == first_id)
            .unwrap();
        assert_eq!(recovered.shutdown_state, ShutdownState::AbnormalOrUncertain);
        assert_eq!(recovered.ended_at_unix_ms, None);
    }

    #[test]
    fn volatile_journal_keeps_only_current_boot() {
        let journal = open_journal(None, "volatile");
        assert_eq!(journal.persistence(), BootPersistence::Volatile);
        assert_eq!(journal.list(None, None).unwrap().records.len(), 1);
    }

    #[test]
    fn persistent_journal_retains_at_most_100_boots_and_paginates() {
        let temp = tempfile::tempdir().unwrap();
        for index in 0..102 {
            let journal = open_journal(Some(temp.path()), &format!("boot-{index}"));
            journal.graceful_shutdown().unwrap();
        }
        let journal = open_journal(Some(temp.path()), "current");
        let first = journal.list(None, Some(60)).unwrap();
        assert_eq!(first.records.len(), 60);
        assert_eq!(first.records[0].boot_id, "current");
        assert!(first.next_cursor.is_some());
        let second = journal
            .list(first.next_cursor.as_deref(), Some(60))
            .unwrap();
        assert_eq!(second.records.len(), 40);
        assert!(second.next_cursor.is_none());
        assert_eq!(second.records.last().unwrap().boot_id, "boot-3");
        assert!(!temp.path().join("boot-journal.json.tmp").exists());
    }

    #[test]
    fn graceful_shutdown_is_persisted() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_journal(Some(temp.path()), "graceful");
        journal.graceful_shutdown().unwrap();
        drop(journal);

        let next = open_journal(Some(temp.path()), "next");
        let previous = &next.list(None, None).unwrap().records[1];
        assert_eq!(previous.boot_id, "graceful");
        assert_eq!(previous.shutdown_state, ShutdownState::Graceful);
        assert!(previous.ended_at_unix_ms.is_some());
    }
}
