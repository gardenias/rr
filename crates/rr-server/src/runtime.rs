use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rr_core::error::{ConfigError, RrError};
use rr_core::{NodeDescriptor, NodeDirectory, NodeDirectoryRegistration, NodeId, ProtocolVersion};
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Mutex};
use tracing::warn;

use crate::boot_journal::{BootJournal, BootPersistence};

const DIRECTORY_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(10);
const DIRECTORY_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const DIRECTORY_STARTUP_RETRY_INTERVAL: Duration = Duration::from_secs(1);

mod protocol_version_serde {
    use rr_core::ProtocolVersion;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Representation {
        major: u32,
        minor: u32,
        min_peer_minor: u32,
    }

    pub fn serialize<S>(version: &ProtocolVersion, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Representation {
            major: version.major,
            minor: version.minor,
            min_peer_minor: version.min_peer_minor,
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ProtocolVersion, D::Error>
    where
        D: Deserializer<'de>,
    {
        let representation = Representation::deserialize(deserializer)?;
        Ok(ProtocolVersion {
            major: representation.major,
            minor: representation.minor,
            min_peer_minor: representation.min_peer_minor,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildSnapshot {
    pub rr_version: String,
    pub app_version: String,
    pub git_commit: String,
    pub git_dirty: bool,
    pub build_profile: String,
    #[serde(with = "protocol_version_serde")]
    pub admin_protocol: ProtocolVersion,
    #[serde(with = "protocol_version_serde")]
    pub raft_protocol: ProtocolVersion,
    pub config_fingerprint: String,
}

impl BuildSnapshot {
    pub fn current(app_version: String, config_fingerprint: String) -> Self {
        Self {
            rr_version: env!("CARGO_PKG_VERSION").to_string(),
            app_version,
            git_commit: option_env!("RR_BUILD_GIT_COMMIT")
                .unwrap_or("unknown")
                .to_string(),
            git_dirty: option_env!("RR_BUILD_GIT_DIRTY") == Some("true"),
            build_profile: option_env!("RR_BUILD_PROFILE")
                .unwrap_or("unknown")
                .to_string(),
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
            config_fingerprint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDescriptorConfig {
    pub client_addr: String,
    pub admin_addr: String,
    pub observability_addr: String,
    pub advertise_addr: String,
    pub environment: String,
    pub region: Option<String>,
    pub zone: Option<String>,
    pub rack: Option<String>,
}

impl NodeDescriptorConfig {
    fn with_boot_id(self, node_id: NodeId, boot_id: String) -> NodeDescriptor {
        NodeDescriptor {
            node_id,
            client_addr: self.client_addr,
            admin_addr: self.admin_addr,
            observability_addr: self.observability_addr,
            advertise_addr: self.advertise_addr,
            environment: self.environment,
            region: self.region,
            zone: self.zone,
            rack: self.rack,
            boot_id,
        }
    }
}

#[derive(Clone)]
pub struct NodeRuntimeConfig {
    pub node_id: NodeId,
    pub app_version: String,
    pub config_fingerprint: String,
    pub descriptor: Option<NodeDescriptorConfig>,
    pub journal_dir: Option<PathBuf>,
    pub directory: Option<Arc<dyn NodeDirectory>>,
}

impl NodeRuntimeConfig {
    pub fn new(node_id: NodeId, config_fingerprint: String) -> Self {
        Self {
            node_id,
            app_version: env!("CARGO_PKG_VERSION").to_string(),
            config_fingerprint,
            descriptor: None,
            journal_dir: None,
            directory: None,
        }
    }
}

pub struct NodeRuntime {
    boot_id: String,
    process_start_unix_ms: u64,
    build: BuildSnapshot,
    journal: BootJournal,
    descriptor: Option<NodeDescriptor>,
    directory: Option<Arc<DirectoryLifecycle>>,
}

impl NodeRuntime {
    pub fn new(config: NodeRuntimeConfig) -> Result<Self, RrError> {
        let boot_id = uuid::Uuid::new_v4().hyphenated().to_string();
        let process_start_unix_ms = unix_time_millis();
        let build = BuildSnapshot::current(config.app_version, config.config_fingerprint);
        let descriptor = config
            .descriptor
            .map(|descriptor| descriptor.with_boot_id(config.node_id, boot_id.clone()));
        if let Some(descriptor) = &descriptor {
            descriptor.validate()?;
        }
        if config.directory.is_some() && descriptor.is_none() {
            return Err(ConfigError::InvalidConfig(
                "node directory publication requires an explicit node descriptor".into(),
            )
            .into());
        }
        let journal = BootJournal::open(
            config.journal_dir.as_deref(),
            boot_id.clone(),
            process_start_unix_ms,
            build.clone(),
        )
        .map_err(|error| ConfigError::InvalidConfig(error.to_string()))?;
        let directory = match (config.directory, descriptor.clone()) {
            (Some(directory), Some(descriptor)) => {
                Some(Arc::new(DirectoryLifecycle::new(directory, descriptor)))
            }
            _ => None,
        };
        Ok(Self {
            boot_id,
            process_start_unix_ms,
            build,
            journal,
            descriptor,
            directory,
        })
    }

    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    pub fn process_start_unix_ms(&self) -> u64 {
        self.process_start_unix_ms
    }

    pub fn build(&self) -> &BuildSnapshot {
        &self.build
    }

    pub fn journal(&self) -> &BootJournal {
        &self.journal
    }

    pub fn descriptor(&self) -> Option<&NodeDescriptor> {
        self.descriptor.as_ref()
    }

    pub fn directory(&self) -> Option<Arc<dyn NodeDirectory>> {
        self.directory
            .as_ref()
            .map(|directory| Arc::clone(&directory.directory))
    }

    pub fn directory_revision(&self) -> u64 {
        self.directory
            .as_ref()
            .map(|directory| directory.revision.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    pub fn persistence(&self) -> BootPersistence {
        self.journal.persistence()
    }

    pub async fn start_directory(
        self: &Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), RrError> {
        match &self.directory {
            Some(directory) => directory.start(shutdown).await,
            None => Ok(()),
        }
    }

    pub async fn stop_directory(&self) {
        if let Some(directory) = &self.directory {
            directory.stop().await;
        }
    }

    pub fn graceful_shutdown(&self) -> Result<(), RrError> {
        self.journal
            .graceful_shutdown()
            .map_err(|error| ConfigError::InvalidConfig(error.to_string()).into())
    }
}

struct DirectoryLifecycle {
    directory: Arc<dyn NodeDirectory>,
    descriptor: NodeDescriptor,
    revision: AtomicU64,
    registration: Mutex<Option<Box<dyn NodeDirectoryRegistration>>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl DirectoryLifecycle {
    fn new(directory: Arc<dyn NodeDirectory>, descriptor: NodeDescriptor) -> Self {
        Self {
            directory,
            descriptor,
            revision: AtomicU64::new(0),
            registration: Mutex::new(None),
            task: Mutex::new(None),
        }
    }

    async fn start(self: &Arc<Self>, shutdown: watch::Receiver<bool>) -> Result<(), RrError> {
        let registration = tokio::time::timeout(DIRECTORY_STARTUP_TIMEOUT, async {
            let mut reported_conflict = false;
            loop {
                match tokio::time::timeout(
                    DIRECTORY_REGISTRATION_TIMEOUT,
                    self.directory.register(self.descriptor.clone()),
                )
                .await
                {
                    Ok(Ok(registration)) => break Ok(registration),
                    Ok(Err(error @ rr_core::DirectoryError::AlreadyExists { .. })) => {
                        if !reported_conflict {
                            warn!(%error, "node directory lease is still present; waiting for expiry before startup registration");
                            reported_conflict = true;
                        }
                        tokio::time::sleep(DIRECTORY_STARTUP_RETRY_INTERVAL).await;
                    }
                    Ok(Err(error)) => break Err(RrError::from(error)),
                    Err(_) => {
                        break Err(ConfigError::InvalidConfig(
                            "node directory registration timed out".into(),
                        )
                        .into())
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            ConfigError::InvalidConfig(
                "node directory startup registration timed out while waiting for lease expiry"
                    .into(),
            )
        })??;
        self.revision
            .store(registration.record().revision, Ordering::Release);
        *self.registration.lock().await = Some(registration);

        let lifecycle = Arc::clone(self);
        let task = tokio::spawn(async move {
            lifecycle.run(shutdown).await;
        });
        *self.task.lock().await = Some(task);
        Ok(())
    }

    async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = interval.tick() => self.reconcile().await,
            }
        }
    }

    async fn reconcile(&self) {
        let heartbeat = {
            let registration = self.registration.lock().await;
            match registration.as_ref() {
                Some(registration) => Some(
                    tokio::time::timeout(Duration::from_secs(10), registration.heartbeat()).await,
                ),
                None => None,
            }
        };
        if let Some(outcome) = heartbeat {
            match outcome {
                Ok(Ok(())) => return,
                Ok(Err(error)) => warn!(%error, "node directory heartbeat failed"),
                Err(_) => warn!("node directory heartbeat timed out"),
            }
            let mut registration = self.registration.lock().await;
            if let Some(registration) = registration.take() {
                match tokio::time::timeout(Duration::from_secs(10), registration.revoke()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        warn!(%error, "failed to revoke stale node directory registration")
                    }
                    Err(_) => warn!("stale node directory revocation timed out"),
                }
            }
            self.revision.store(0, Ordering::Release);
        }

        match tokio::time::timeout(
            Duration::from_secs(10),
            self.directory.register(self.descriptor.clone()),
        )
        .await
        {
            Ok(Ok(registration)) => {
                self.revision
                    .store(registration.record().revision, Ordering::Release);
                *self.registration.lock().await = Some(registration);
            }
            Ok(Err(error)) => warn!(%error, "node directory re-registration failed"),
            Err(_) => warn!("node directory re-registration timed out"),
        }
    }

    async fn stop(&self) {
        if let Some(mut handle) = self.task.lock().await.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => warn!(%error, "node directory task panicked during shutdown"),
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }
        let registration = self.registration.lock().await.take();
        if let Some(registration) = registration {
            match tokio::time::timeout(Duration::from_secs(10), registration.revoke()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(%error, "node directory revoke failed during shutdown"),
                Err(_) => warn!("node directory revoke timed out during shutdown"),
            }
        }
        self.revision.store(0, Ordering::Release);
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
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[derive(Default)]
    struct FakeDirectoryState {
        registration_attempts: AtomicUsize,
        registration_conflicts: AtomicUsize,
        registrations: AtomicUsize,
        heartbeats: AtomicUsize,
        revocations: AtomicUsize,
        failed_heartbeats: AtomicUsize,
        descriptors: Mutex<Vec<NodeDescriptor>>,
    }

    struct FakeDirectory {
        state: Arc<FakeDirectoryState>,
    }

    struct FakeRegistration {
        record: rr_core::NodeDirectoryRecord,
        state: Arc<FakeDirectoryState>,
    }

    #[async_trait::async_trait]
    impl NodeDirectoryRegistration for FakeRegistration {
        fn record(&self) -> &rr_core::NodeDirectoryRecord {
            &self.record
        }

        async fn heartbeat(&self) -> Result<(), rr_core::DirectoryError> {
            self.state.heartbeats.fetch_add(1, AtomicOrdering::SeqCst);
            if self
                .state
                .failed_heartbeats
                .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |failures| {
                    Some(failures.saturating_sub(1))
                })
                .unwrap_or(0)
                > 0
            {
                return Err(rr_core::DirectoryError::LeaseExpired { node_id: 7 });
            }
            Ok(())
        }

        async fn revoke(&self) -> Result<(), rr_core::DirectoryError> {
            self.state.revocations.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl NodeDirectory for FakeDirectory {
        async fn get(
            &self,
            _node_id: NodeId,
        ) -> Result<Option<rr_core::NodeDirectoryRecord>, rr_core::DirectoryError> {
            Ok(None)
        }

        async fn list(
            &self,
            _cursor: Option<&str>,
            _page_size: Option<usize>,
        ) -> Result<rr_core::NodeDirectoryPage, rr_core::DirectoryError> {
            Ok(rr_core::NodeDirectoryPage {
                records: Vec::new(),
                next_cursor: None,
                revision: 1,
            })
        }

        async fn watch(
            &self,
        ) -> Result<
            std::pin::Pin<Box<dyn futures::Stream<Item = rr_core::NodeDirectoryChange> + Send>>,
            rr_core::DirectoryError,
        > {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn register(
            &self,
            descriptor: NodeDescriptor,
        ) -> Result<Box<dyn NodeDirectoryRegistration>, rr_core::DirectoryError> {
            self.state
                .registration_attempts
                .fetch_add(1, AtomicOrdering::SeqCst);
            if self
                .state
                .registration_conflicts
                .fetch_update(
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                    |conflicts| Some(conflicts.saturating_sub(1)),
                )
                .unwrap_or(0)
                > 0
            {
                return Err(rr_core::DirectoryError::AlreadyExists { node_id: 7 });
            }
            let registration = self
                .state
                .registrations
                .fetch_add(1, AtomicOrdering::SeqCst)
                + 1;
            self.state.descriptors.lock().await.push(descriptor.clone());
            Ok(Box::new(FakeRegistration {
                record: rr_core::NodeDirectoryRecord {
                    descriptor,
                    revision: registration as u64,
                    lease_id: registration as i64,
                },
                state: Arc::clone(&self.state),
            }))
        }
    }

    fn directory_runtime(state: Arc<FakeDirectoryState>) -> Arc<NodeRuntime> {
        Arc::new(
            NodeRuntime::new(NodeRuntimeConfig {
                node_id: 7,
                app_version: "app".into(),
                config_fingerprint: "fingerprint".into(),
                descriptor: Some(NodeDescriptorConfig {
                    client_addr: "node-7:5000".into(),
                    admin_addr: "node-7:5001".into(),
                    observability_addr: "node-7:5002".into(),
                    advertise_addr: "node-7:5000".into(),
                    environment: "test".into(),
                    region: None,
                    zone: None,
                    rack: None,
                }),
                journal_dir: None,
                directory: Some(Arc::new(FakeDirectory { state })),
            })
            .unwrap(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn directory_start_waits_for_crashed_boot_lease_to_expire() {
        let state = Arc::new(FakeDirectoryState::default());
        state
            .registration_conflicts
            .store(16, AtomicOrdering::SeqCst);
        let runtime = directory_runtime(Arc::clone(&state));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let started = tokio::time::Instant::now();

        runtime.start_directory(shutdown_rx).await.unwrap();

        assert!(started.elapsed() >= Duration::from_secs(16));
        assert_eq!(state.registration_attempts.load(AtomicOrdering::SeqCst), 17);
        assert_eq!(state.registrations.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(runtime.directory_revision(), 1);
        runtime.stop_directory().await;
    }

    #[tokio::test(start_paused = true)]
    async fn directory_start_bounds_wait_for_another_live_boot() {
        let state = Arc::new(FakeDirectoryState::default());
        state
            .registration_conflicts
            .store(usize::MAX, AtomicOrdering::SeqCst);
        let runtime = directory_runtime(Arc::clone(&state));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let started = tokio::time::Instant::now();

        let error = runtime.start_directory(shutdown_rx).await.unwrap_err();

        assert!(error.to_string().contains("timed out"), "got: {error}");
        assert!(started.elapsed() >= Duration::from_secs(30));
        assert!(started.elapsed() < Duration::from_secs(31));
        assert_eq!(state.registrations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(runtime.directory_revision(), 0);
        runtime.stop_directory().await;
    }

    #[tokio::test]
    async fn directory_heartbeat_reconciles_and_shutdown_revokes() {
        let state = Arc::new(FakeDirectoryState::default());
        let runtime = directory_runtime(Arc::clone(&state));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        runtime.start_directory(shutdown_rx).await.unwrap();
        assert_eq!(state.registrations.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(runtime.directory_revision(), 1);
        let directory = runtime.directory.as_ref().unwrap();
        directory.reconcile().await;
        assert_eq!(state.heartbeats.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(state.registrations.load(AtomicOrdering::SeqCst), 1);

        state.failed_heartbeats.store(1, AtomicOrdering::SeqCst);
        directory.reconcile().await;
        assert_eq!(state.registrations.load(AtomicOrdering::SeqCst), 2);
        assert_eq!(runtime.directory_revision(), 2);
        runtime.stop_directory().await;
        assert!(state.revocations.load(AtomicOrdering::SeqCst) >= 2);
        assert_eq!(runtime.directory_revision(), 0);
        assert!(state
            .descriptors
            .lock()
            .await
            .iter()
            .all(|descriptor| descriptor.boot_id == runtime.boot_id()));
    }

    #[tokio::test]
    async fn directory_stop_consumes_completed_task_without_double_await() {
        let state = Arc::new(FakeDirectoryState::default());
        let runtime = directory_runtime(Arc::clone(&state));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        runtime.start_directory(shutdown_rx).await.unwrap();
        shutdown_tx.send(true).unwrap();

        runtime.stop_directory().await;

        assert_eq!(state.revocations.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(runtime.directory_revision(), 0);
        assert!(runtime
            .directory
            .as_ref()
            .unwrap()
            .task
            .lock()
            .await
            .is_none());
    }

    #[test]
    fn boot_identity_is_shared_by_journal_build_and_descriptor() {
        let runtime = NodeRuntime::new(NodeRuntimeConfig {
            node_id: 7,
            app_version: "app-1.2.3".into(),
            config_fingerprint: "fingerprint".into(),
            descriptor: Some(NodeDescriptorConfig {
                client_addr: "node-7:5000".into(),
                admin_addr: "node-7:5001".into(),
                observability_addr: "node-7:5002".into(),
                advertise_addr: "node-7:5000".into(),
                environment: "test".into(),
                region: None,
                zone: None,
                rack: None,
            }),
            journal_dir: None,
            directory: None,
        })
        .unwrap();
        assert_eq!(runtime.persistence(), BootPersistence::Volatile);
        assert_eq!(runtime.journal().current().boot_id, runtime.boot_id());
        assert_eq!(runtime.journal().current().build, *runtime.build());
        assert_eq!(runtime.descriptor().unwrap().boot_id, runtime.boot_id());
        assert_eq!(runtime.build().app_version, "app-1.2.3");
    }

    #[test]
    fn protocol_build_version_preserves_asymmetric_compatibility_inputs() {
        let version = ProtocolVersion {
            major: 1,
            minor: 5,
            min_peer_minor: 3,
        };
        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 5);
        assert_eq!(version.min_peer_minor, 3);
    }
}
