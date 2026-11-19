use super::cache::CacheView;
use super::config::parse_discovery_endpoints;
use super::connection_store::{ConnectionStore, SecretKind, SecretSource, SecretStore};
use super::model::{
    rfc3339, ConnectionId, ConnectionProfile, ConnectionSecrets, ConnectionSource,
    ConnectionStatus, ConnectionSummary, SecretInput, TlsProfile,
};
use super::scheduler::Scheduler;
use super::upstream::{TonicUpstream, Upstream};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};
use tokio::sync::{watch, Mutex as AsyncMutex, Semaphore};
use tokio::task::JoinHandle;

const MAX_ACTIVE_CONNECTIONS: usize = 8;
const MAX_CANDIDATE_TESTS: usize = 4;
#[cfg(not(test))]
const CANDIDATE_TEST_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const CANDIDATE_TEST_TIMEOUT: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const CONNECTION_INIT_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(test)]
const CONNECTION_INIT_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PublicConnectionError {
    pub code: &'static str,
    pub message: &'static str,
}

impl PublicConnectionError {
    fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    fn not_found() -> Self {
        Self::new("CONNECTION_NOT_FOUND", "Connection not found")
    }

    fn not_connected() -> Self {
        Self::new("CONNECTION_NOT_CONNECTED", "Connection is not connected")
    }

    fn active() -> Self {
        Self::new(
            "CONNECTION_ACTIVE",
            "Disconnect the connection before editing it",
        )
    }

    fn active_limit() -> Self {
        Self::new(
            "ACTIVE_CONNECTION_LIMIT",
            "At most 8 connections can be active at once",
        )
    }

    fn credentials_required() -> Self {
        Self::new(
            "CREDENTIALS_REQUIRED",
            "Discovery credentials must be configured",
        )
    }

    fn invalid_profile() -> Self {
        Self::new(
            "INVALID_CONNECTION_PROFILE",
            "Connection profile is invalid",
        )
    }

    fn store_failed() -> Self {
        Self::new(
            "CONNECTION_STORE_FAILED",
            "Connection state operation failed",
        )
    }

    fn initialization_failed() -> Self {
        Self::new(
            "CONNECTION_INITIALIZATION_FAILED",
            "Connection initialization failed",
        )
    }

    fn test_failed() -> Self {
        Self::new("CONNECTION_TEST_FAILED", "Connection test failed")
    }

    fn shutting_down() -> Self {
        Self::new(
            "CONNECTION_SHUTTING_DOWN",
            "Connection manager is shutting down",
        )
    }
}

impl fmt::Display for PublicConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for PublicConnectionError {}

#[async_trait]
pub(crate) trait RuntimeFactory: Send + Sync {
    async fn build(
        &self,
        profile: &ConnectionProfile,
        secrets: &SecretStore,
    ) -> Result<Arc<Scheduler>>;

    async fn test(
        &self,
        profile: &ConnectionProfile,
        admin_token: Option<String>,
        discovery_password: Option<String>,
    ) -> Result<()>;
}

struct ProfileRuntimeFactory;

#[async_trait]
impl RuntimeFactory for ProfileRuntimeFactory {
    async fn build(
        &self,
        profile: &ConnectionProfile,
        secrets: &SecretStore,
    ) -> Result<Arc<Scheduler>> {
        let (admin_token, discovery_password) = read_secrets(secrets, &profile.id)?;
        let upstream = TonicUpstream::from_profile(&profile.admin_tls, admin_token)?;
        let discovery = super::connect_discovery_profile(profile, discovery_password).await?;
        super::build_profile_scheduler(profile, upstream, discovery)
    }

    async fn test(
        &self,
        profile: &ConnectionProfile,
        admin_token: Option<String>,
        discovery_password: Option<String>,
    ) -> Result<()> {
        let upstream = TonicUpstream::from_profile(&profile.admin_tls, admin_token)?;
        if let Some(peer) = profile.admin_seeds.first() {
            upstream
                .collect_node(peer)
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
        }
        if let Some(discovery) =
            super::connect_discovery_profile(profile, discovery_password).await?
        {
            discovery
                .reconcile()
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
        }
        Ok(())
    }
}

pub(crate) struct ConnectionRuntime {
    pub scheduler: Arc<Scheduler>,
    pub shutdown: watch::Sender<bool>,
    pub generation: u64,
    pub summary_projection: Arc<tokio::sync::Mutex<Option<super::http::SummaryProjection>>>,
    task: AsyncMutex<Option<JoinHandle<()>>>,
    stopped: AtomicBool,
}

impl fmt::Debug for ConnectionRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionRuntime")
            .field("generation", &self.generation)
            .field("stopped", &self.stopped.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ConnectionRuntime {
    fn new(scheduler: Arc<Scheduler>, generation: u64) -> Arc<Self> {
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(scheduler.clone().run(receiver));
        Arc::new(Self {
            scheduler,
            shutdown,
            generation,
            summary_projection: Arc::new(tokio::sync::Mutex::new(None)),
            task: AsyncMutex::new(Some(task)),
            stopped: AtomicBool::new(false),
        })
    }

    async fn shutdown(&self) {
        let mut task = self.task.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        let _ = self.shutdown.send(true);
        self.scheduler.shutdown().await;
        if let Some(task) = task.take() {
            let _ = task.await;
        }
        self.stopped.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
struct CleanupWaiter {
    done: watch::Receiver<bool>,
}

impl CleanupWaiter {
    fn completed() -> Self {
        let (done, receiver) = watch::channel(true);
        drop(done);
        Self { done: receiver }
    }

    async fn wait(&self) {
        let mut done = self.done.clone();
        while !*done.borrow() {
            if done.changed().await.is_err() {
                break;
            }
        }
    }
}

struct RetiringCleanup {
    task: JoinHandle<()>,
    _done: watch::Sender<bool>,
}

pub(crate) struct ManagedConnection {
    pub profile: ConnectionProfile,
    pub source: ConnectionSource,
    pub runtime: Option<Arc<ConnectionRuntime>>,
    pub status: ConnectionStatus,
    pub last_error: Option<&'static str>,
    pub operation_generation: u64,
    initializations: usize,
    deleting: bool,
    transitioning: bool,
    cleanup: Option<CleanupWaiter>,
    internal_error: Option<String>,
}

impl ManagedConnection {
    fn new(profile: ConnectionProfile, source: ConnectionSource) -> Self {
        Self {
            profile,
            source,
            runtime: None,
            status: ConnectionStatus::Disconnected,
            last_error: None,
            operation_generation: 0,
            initializations: 0,
            deleting: false,
            transitioning: false,
            cleanup: None,
            internal_error: None,
        }
    }
}

fn project_connection_status(
    status: ConnectionStatus,
    profile: &ConnectionProfile,
    view: &CacheView,
) -> ConnectionStatus {
    if !matches!(
        status,
        ConnectionStatus::Connected | ConnectionStatus::Degraded | ConnectionStatus::Unreachable
    ) {
        return status;
    }
    if view
        .nodes
        .iter()
        .all(|node| node.snapshot.is_none() && node.error.is_none())
        && view.discovery_error.is_none()
    {
        return ConnectionStatus::Connected;
    }
    let all_seeds_failed = if profile.admin_seeds.is_empty() {
        if view.nodes.is_empty() {
            view.discovery_error.is_some()
        } else {
            view.nodes.iter().all(|node| node.error.is_some())
        }
    } else {
        profile.admin_seeds.iter().all(|seed| {
            view.nodes
                .iter()
                .find(|node| {
                    node.peer.node_id == seed.node_id && node.peer.admin_addr == seed.admin_addr
                })
                .is_some_and(|node| node.error.is_some())
        })
    };
    if all_seeds_failed {
        return ConnectionStatus::Unreachable;
    }
    if view.partial {
        return ConnectionStatus::Degraded;
    }
    ConnectionStatus::Connected
}

struct PerConnection {
    operation: AsyncMutex<()>,
    state: Mutex<ManagedConnection>,
    changes: watch::Sender<u64>,
    observed_cluster_id_write: AtomicBool,
}

impl PerConnection {
    fn new(profile: ConnectionProfile, source: ConnectionSource) -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            operation: AsyncMutex::new(()),
            state: Mutex::new(ManagedConnection::new(profile, source)),
            changes,
            observed_cluster_id_write: AtomicBool::new(false),
        }
    }

    fn state(&self) -> MutexGuard<'_, ManagedConnection> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn signal(&self, generation: u64) {
        self.changes.send_replace(generation);
    }
}

struct CreateReservation {
    id: ConnectionId,
    reservations: Arc<Mutex<HashSet<ConnectionId>>>,
}

impl Drop for CreateReservation {
    fn drop(&mut self) {
        self.reservations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.id);
    }
}

pub(crate) struct ConnectionManager {
    store: Arc<ConnectionStore>,
    secrets: Arc<SecretStore>,
    factory: Arc<dyn RuntimeFactory>,
    entries: Arc<Mutex<HashMap<ConnectionId, Arc<PerConnection>>>>,
    create_reservations: Arc<Mutex<HashSet<ConnectionId>>>,
    candidate_tests: Arc<Semaphore>,
    active_transitions: AsyncMutex<()>,
    retiring: Mutex<Vec<RetiringCleanup>>,
    shutdown_execution: AsyncMutex<()>,
    shutting_down: AtomicBool,
}

impl ConnectionManager {
    pub fn new(store: Arc<ConnectionStore>, secrets: Arc<SecretStore>) -> Self {
        Self::with_factory(store, secrets, Arc::new(ProfileRuntimeFactory))
    }

    pub(crate) fn with_factory(
        store: Arc<ConnectionStore>,
        secrets: Arc<SecretStore>,
        factory: Arc<dyn RuntimeFactory>,
    ) -> Self {
        let entries = store
            .list()
            .into_iter()
            .map(|profile| {
                (
                    profile.id.clone(),
                    Arc::new(PerConnection::new(profile, ConnectionSource::Profile)),
                )
            })
            .collect();
        Self {
            store,
            secrets,
            factory,
            entries: Arc::new(Mutex::new(entries)),
            create_reservations: Arc::new(Mutex::new(HashSet::new())),
            candidate_tests: Arc::new(Semaphore::new(MAX_CANDIDATE_TESTS)),
            active_transitions: AsyncMutex::new(()),
            retiring: Mutex::new(Vec::new()),
            shutdown_execution: AsyncMutex::new(()),
            shutting_down: AtomicBool::new(false),
        }
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<ConnectionId, Arc<PerConnection>>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn entry(&self, id: &str) -> Result<Arc<PerConnection>, PublicConnectionError> {
        self.entries()
            .get(id)
            .cloned()
            .ok_or_else(PublicConnectionError::not_found)
    }

    fn is_current(&self, id: &str, entry: &Arc<PerConnection>) -> bool {
        self.entries()
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
    }

    fn active_count(&self) -> usize {
        let entries = self.entries().values().cloned().collect::<Vec<_>>();
        entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.state().status,
                    ConnectionStatus::Connecting
                        | ConnectionStatus::Connected
                        | ConnectionStatus::Degraded
                        | ConnectionStatus::Unreachable
                )
            })
            .count()
    }

    fn summary(&self, entry: &PerConnection) -> Result<ConnectionSummary, PublicConnectionError> {
        let state = entry.state();
        let profile = state.profile.clone();
        let status = state.status;
        let source = state.source;
        let runtime = state.runtime.clone();
        let last_error = state.last_error.map(str::to_string);
        drop(state);
        self.summary_from_state(profile, source, status, runtime, last_error)
    }

    fn disconnected_summary(
        &self,
        entry: &PerConnection,
    ) -> Result<ConnectionSummary, PublicConnectionError> {
        let state = entry.state();
        let profile = state.profile.clone();
        let source = state.source;
        drop(state);
        self.summary_from_state(profile, source, ConnectionStatus::Disconnected, None, None)
    }

    fn summary_from_state(
        &self,
        profile: ConnectionProfile,
        source: ConnectionSource,
        status: ConnectionStatus,
        runtime: Option<Arc<ConnectionRuntime>>,
        last_error: Option<String>,
    ) -> Result<ConnectionSummary, PublicConnectionError> {
        let admin = self
            .secrets
            .get(&profile.id, SecretKind::AdminToken)
            .map_err(|_| PublicConnectionError::store_failed())?;
        let discovery = self
            .secrets
            .get(&profile.id, SecretKind::DiscoveryPassword)
            .map_err(|_| PublicConnectionError::store_failed())?;
        let credential_source = match (
            admin.as_ref().map(|secret| secret.source),
            discovery.as_ref().map(|secret| secret.source),
        ) {
            (Some(admin), Some(discovery)) if admin != discovery => Some("MIXED".to_string()),
            (Some(SecretSource::Memory), _) | (_, Some(SecretSource::Memory)) => {
                Some("MEMORY".to_string())
            }
            (Some(SecretSource::Keychain), _) | (_, Some(SecretSource::Keychain)) => {
                Some("KEYCHAIN".to_string())
            }
            (None, None) => None,
        };
        let credentials_configured = profile.discovery_username.is_none() || discovery.is_some();
        let (status, node_count, group_count) = match runtime {
            Some(runtime) => {
                let view = runtime.scheduler.cache.view();
                (
                    project_connection_status(status, &profile, &view),
                    Some(view.nodes.len() as u64),
                    Some(view.expected_groups().len() as u64),
                )
            }
            None => (status, None, None),
        };
        Ok(ConnectionSummary {
            profile,
            source,
            status,
            credentials_configured,
            credential_source,
            last_error,
            node_count,
            group_count,
        })
    }

    pub fn keychain_available(&self) -> bool {
        self.secrets.keychain_available()
    }

    pub fn list(&self) -> Result<Vec<ConnectionSummary>, PublicConnectionError> {
        let mut entries = self.entries().values().cloned().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.state().profile.id.clone());
        entries.iter().map(|entry| self.summary(entry)).collect()
    }

    pub fn get(&self, id: &str) -> Result<ConnectionSummary, PublicConnectionError> {
        let entry = self.entry(id)?;
        self.summary(&entry)
    }

    pub async fn create(
        &self,
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        remember: bool,
    ) -> Result<ConnectionSummary, PublicConnectionError> {
        self.create_with_source(profile, secrets, remember, ConnectionSource::Profile)
            .await
    }

    pub async fn create_startup(
        &self,
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
    ) -> Result<ConnectionSummary, PublicConnectionError> {
        self.create_with_source(profile, secrets, false, ConnectionSource::Startup)
            .await
    }

    async fn create_with_source(
        &self,
        mut profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        remember: bool,
        source: ConnectionSource,
    ) -> Result<ConnectionSummary, PublicConnectionError> {
        if profile.id.is_empty() {
            profile.id = uuid::Uuid::new_v4().to_string();
        }
        validate_profile(&profile)?;
        let now = rfc3339(SystemTime::now());
        profile.created_at = now.clone();
        profile.updated_at = now;
        profile.observed_cluster_id = None;
        profile.last_connected_at = None;
        let reservation = {
            let mut reservations = self
                .create_reservations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let entries = self.entries();
            if entries.contains_key(&profile.id) || reservations.contains(&profile.id) {
                return Err(PublicConnectionError::invalid_profile());
            }
            reservations.insert(profile.id.clone());
            CreateReservation {
                id: profile.id.clone(),
                reservations: self.create_reservations.clone(),
            }
        };
        let persisted_secrets = self.secrets.clone();
        let store = self.store.clone();
        let entries = self.entries.clone();
        let reservations = self.create_reservations.clone();
        let created = tokio::task::spawn_blocking(move || {
            persisted_secrets
                .update(&profile.id, &secrets, remember)
                .map_err(|_| PublicConnectionError::store_failed())?;
            if source == ConnectionSource::Profile && store.save(&profile).is_err() {
                let _ = persisted_secrets.remove(&profile.id);
                return Err(PublicConnectionError::store_failed());
            }
            let entry = Arc::new(PerConnection::new(profile, source));
            {
                let _reservations = reservations.lock().unwrap_or_else(PoisonError::into_inner);
                let mut entries = entries.lock().unwrap_or_else(PoisonError::into_inner);
                if entries.contains_key(&entry.state().profile.id) {
                    return Err(PublicConnectionError::invalid_profile());
                }
                entries.insert(entry.state().profile.id.clone(), entry.clone());
            }
            drop(reservation);
            Ok(entry)
        })
        .await;
        match created {
            Ok(Ok(entry)) => self.summary(&entry),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(PublicConnectionError::store_failed()),
        }
    }

    pub async fn update(
        &self,
        id: &str,
        mut profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        remember: bool,
    ) -> Result<ConnectionSummary, PublicConnectionError> {
        let entry = self.entry(id)?;
        let _operation = entry.operation.lock().await;
        if !self.is_current(id, &entry) {
            return Err(PublicConnectionError::not_found());
        }
        let transition = {
            let state = entry.state();
            if state.deleting {
                return Err(PublicConnectionError::not_found());
            }
            state.cleanup.clone().filter(|_| state.transitioning)
        };
        if let Some(cleanup) = transition {
            drop(_operation);
            cleanup.wait().await;
            return Box::pin(self.update(id, profile, secrets, remember)).await;
        }
        let (previous, source) = {
            let state = entry.state();
            if state.status != ConnectionStatus::Disconnected {
                return Err(PublicConnectionError::active());
            }
            (state.profile.clone(), state.source)
        };
        profile.id = id.to_string();
        profile.created_at = previous.created_at;
        profile.updated_at = rfc3339(SystemTime::now());
        profile.observed_cluster_id = previous.observed_cluster_id;
        profile.last_connected_at = previous.last_connected_at;
        validate_profile(&profile)?;
        self.secrets
            .update(id, &secrets, remember)
            .map_err(|_| PublicConnectionError::store_failed())?;
        if source == ConnectionSource::Profile {
            self.store
                .save(&profile)
                .map_err(|_| PublicConnectionError::store_failed())?;
        }
        let mut state = entry.state();
        state.profile = profile;
        state.operation_generation = state.operation_generation.saturating_add(1);
        state.last_error = None;
        state.internal_error = None;
        let generation = state.operation_generation;
        drop(state);
        entry.signal(generation);
        drop(_operation);
        self.summary(&entry)
    }

    pub async fn remove(&self, id: &str) -> Result<(), PublicConnectionError> {
        let Ok(entry) = self.entry(id) else {
            return Ok(());
        };
        let mut changes = entry.changes.subscribe();
        let (cleanup, guard) = loop {
            let _operation = entry.operation.lock().await;
            if !self.is_current(id, &entry) {
                return Ok(());
            }
            let deleting = entry.state().deleting;
            if deleting {
                drop(_operation);
                let _ = changes.changed().await;
                continue;
            }
            let mut state = entry.state();
            state.deleting = true;
            state.operation_generation = state.operation_generation.saturating_add(1);
            state.status = ConnectionStatus::Disconnected;
            state.last_error = None;
            state.internal_error = None;
            let previous = state.cleanup.clone().filter(|_| state.transitioning);
            let runtime = state.runtime.take();
            let needs_cleanup =
                previous.is_some() || runtime.is_some() || state.initializations > 0;
            state.transitioning = needs_cleanup;
            let generation = state.operation_generation;
            drop(state);
            entry.signal(generation);
            let cleanup = if needs_cleanup {
                let cleanup = self.start_cleanup(&entry, generation, runtime, previous);
                entry.state().cleanup = Some(cleanup.clone());
                cleanup
            } else {
                CleanupWaiter::completed()
            };
            break (cleanup, DeletingGuard::new(entry.clone()));
        };
        cleanup.wait().await;

        let _operation = entry.operation.lock().await;
        if !self.is_current(id, &entry) {
            return Ok(());
        }
        let source = entry.state().source;
        let removed = if source == ConnectionSource::Profile {
            self.store.remove(id)
        } else {
            Ok(())
        };
        let result = removed
            .map_err(|_| PublicConnectionError::store_failed())
            .and_then(|()| {
                self.secrets
                    .remove(id)
                    .map_err(|_| PublicConnectionError::store_failed())
            });
        if result.is_ok() {
            self.entries().remove(id);
            let generation = entry.state().operation_generation;
            entry.signal(generation);
            guard.disarm();
        }
        result
    }

    pub async fn test_candidate_with_existing(
        &self,
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        existing_id: Option<&str>,
    ) -> Result<(), PublicConnectionError> {
        let (saved_admin, saved_discovery) = match existing_id {
            Some(id) => {
                self.entry(id)?;
                read_secrets(&self.secrets, id)
                    .map_err(|_| PublicConnectionError::store_failed())?
            }
            None => (None, None),
        };
        let admin_token = match secrets.admin_token {
            SecretInput::Keep => saved_admin,
            SecretInput::Clear => None,
            SecretInput::Replace(secret) => Some(secret),
        };
        let discovery_password = match secrets.discovery_password {
            SecretInput::Keep => saved_discovery,
            SecretInput::Clear => None,
            SecretInput::Replace(secret) => Some(secret),
        };
        self.test_candidate(profile, admin_token, discovery_password)
            .await
    }

    pub async fn test_candidate(
        &self,
        profile: ConnectionProfile,
        admin_token: Option<String>,
        discovery_password: Option<String>,
    ) -> Result<(), PublicConnectionError> {
        validate_profile(&profile)?;
        if profile.discovery_username.is_some() && discovery_password.is_none() {
            return Err(PublicConnectionError::credentials_required());
        }
        self.run_candidate_test(profile, admin_token, discovery_password)
            .await
    }

    pub async fn test(&self, id: &str) -> Result<(), PublicConnectionError> {
        let entry = self.entry(id)?;
        let profile = entry.state().profile.clone();
        let (admin_token, discovery_password) =
            read_secrets(&self.secrets, id).map_err(|_| PublicConnectionError::store_failed())?;
        if profile.discovery_username.is_some() && discovery_password.is_none() {
            return Err(PublicConnectionError::credentials_required());
        }
        self.run_candidate_test(profile, admin_token, discovery_password)
            .await
    }

    async fn run_candidate_test(
        &self,
        profile: ConnectionProfile,
        admin_token: Option<String>,
        discovery_password: Option<String>,
    ) -> Result<(), PublicConnectionError> {
        let test = async {
            let _permit = self
                .candidate_tests
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("candidate test semaphore closed"))?;
            self.factory
                .test(&profile, admin_token, discovery_password)
                .await
        };
        tokio::time::timeout(CANDIDATE_TEST_TIMEOUT, test)
            .await
            .map_err(|_| PublicConnectionError::test_failed())?
            .map_err(|_| PublicConnectionError::test_failed())
    }

    pub async fn connect(&self, id: &str) -> Result<ConnectionSummary, PublicConnectionError> {
        let entry = self.entry(id)?;
        let mut changes = entry.changes.subscribe();
        let (generation, profile, wait) = loop {
            let _operation = entry.operation.lock().await;
            if !self.is_current(id, &entry) {
                return Err(PublicConnectionError::not_found());
            }
            if self.shutting_down.load(Ordering::Acquire) {
                return Err(PublicConnectionError::shutting_down());
            }
            let transition = {
                let state = entry.state();
                if state.deleting {
                    return Err(PublicConnectionError::not_found());
                }
                state.cleanup.clone().filter(|_| state.transitioning)
            };
            if let Some(cleanup) = transition {
                drop(_operation);
                cleanup.wait().await;
                continue;
            }
            let (status, generation, profile, has_runtime) = {
                let state = entry.state();
                (
                    state.status,
                    state.operation_generation,
                    state.profile.clone(),
                    state.runtime.is_some(),
                )
            };
            match (status, has_runtime) {
                (
                    ConnectionStatus::Connected
                    | ConnectionStatus::Degraded
                    | ConnectionStatus::Unreachable,
                    true,
                ) => {
                    drop(_operation);
                    return self.summary(&entry);
                }
                (ConnectionStatus::Connecting, _) => {
                    break (generation, profile, true);
                }
                _ => {
                    if profile.discovery_username.is_some()
                        && !discovery_secret_exists(&self.secrets, id)?
                    {
                        return Err(PublicConnectionError::credentials_required());
                    }
                    let _active = self.active_transitions.lock().await;
                    if self.active_count() >= MAX_ACTIVE_CONNECTIONS {
                        return Err(PublicConnectionError::active_limit());
                    }
                    let mut state = entry.state();
                    state.operation_generation = state.operation_generation.saturating_add(1);
                    state.status = ConnectionStatus::Connecting;
                    state.last_error = None;
                    state.internal_error = None;
                    state.initializations += 1;
                    let generation = state.operation_generation;
                    let profile = state.profile.clone();
                    drop(state);
                    entry.signal(generation);
                    break (generation, profile, false);
                }
            }
        };
        if wait {
            loop {
                {
                    let state = entry.state();
                    if state.operation_generation != generation {
                        return Err(PublicConnectionError::not_connected());
                    }
                    if state.status == ConnectionStatus::Connected {
                        drop(state);
                        return self.summary(&entry);
                    }
                    if state.status != ConnectionStatus::Connecting {
                        return Err(PublicConnectionError::initialization_failed());
                    }
                }
                changes
                    .changed()
                    .await
                    .map_err(|_| PublicConnectionError::not_connected())?;
            }
        }

        let mut initialization = InitializationGuard::new(entry.clone(), generation);
        let built = tokio::time::timeout(
            CONNECTION_INIT_TIMEOUT,
            self.factory.build(&profile, &self.secrets),
        )
        .await
        .map_err(|_| anyhow::anyhow!("connection initialization exceeded its total deadline"))
        .and_then(|result| result);

        let result = {
            let _operation = entry.operation.lock().await;
            let is_current =
                self.is_current(id, &entry) && !self.shutting_down.load(Ordering::Acquire);
            let mut state = entry.state();
            let current = state.operation_generation == generation
                && state.status == ConnectionStatus::Connecting
                && is_current;
            let result = match (built, current) {
                (Ok(scheduler), true) => {
                    let mut updated = profile.clone();
                    updated.last_connected_at = Some(rfc3339(SystemTime::now()));
                    let saved = if state.source == ConnectionSource::Profile {
                        self.store.save(&updated)
                    } else {
                        Ok(())
                    };
                    match saved {
                        Ok(()) => {
                            let runtime = ConnectionRuntime::new(scheduler, generation);
                            state.profile = updated;
                            state.runtime = Some(runtime);
                            state.status = ConnectionStatus::Connected;
                            state.last_error = None;
                            state.internal_error = None;
                            Ok(())
                        }
                        Err(error) => {
                            state.status = ConnectionStatus::Disconnected;
                            state.last_error = Some(PublicConnectionError::store_failed().message);
                            state.internal_error = Some(error.to_string());
                            Err(PublicConnectionError::store_failed())
                        }
                    }
                }
                (Ok(_), false) => Err(PublicConnectionError::not_connected()),
                (Err(error), true) => {
                    state.status = ConnectionStatus::Disconnected;
                    state.last_error = Some(PublicConnectionError::initialization_failed().message);
                    state.internal_error = Some(error.to_string());
                    Err(PublicConnectionError::initialization_failed())
                }
                (Err(_), false) => Err(PublicConnectionError::not_connected()),
            };
            initialization.complete(&mut state);
            let generation = state.operation_generation;
            drop(state);
            entry.signal(generation);
            result
        };
        result?;
        let _ = self.resolve(id)?;
        self.summary(&entry)
    }

    pub async fn disconnect(&self, id: &str) -> Result<ConnectionSummary, PublicConnectionError> {
        let entry = self.entry(id)?;
        let cleanup = self.begin_cleanup(&entry, id).await?;
        cleanup.wait().await;
        self.disconnected_summary(&entry)
    }

    async fn begin_cleanup(
        &self,
        entry: &Arc<PerConnection>,
        id: &str,
    ) -> Result<CleanupWaiter, PublicConnectionError> {
        let _operation = entry.operation.lock().await;
        if !self.is_current(id, entry) {
            return Err(PublicConnectionError::not_found());
        }
        let mut state = entry.state();
        if state.transitioning {
            return Ok(state.cleanup.clone().expect("transition cleanup"));
        }
        if state.status == ConnectionStatus::Disconnected
            && state.runtime.is_none()
            && state.initializations == 0
        {
            return Ok(CleanupWaiter::completed());
        }
        let previous = state.cleanup.clone().filter(|_| state.transitioning);
        state.transitioning = true;
        state.operation_generation = state.operation_generation.saturating_add(1);
        state.status = ConnectionStatus::Disconnected;
        state.last_error = None;
        state.internal_error = None;
        let runtime = state.runtime.take();
        let generation = state.operation_generation;
        drop(state);
        entry.signal(generation);
        let cleanup = self.start_cleanup(entry, generation, runtime, previous);
        entry.state().cleanup = Some(cleanup.clone());
        Ok(cleanup)
    }

    fn start_cleanup(
        &self,
        entry: &Arc<PerConnection>,
        generation: u64,
        runtime: Option<Arc<ConnectionRuntime>>,
        previous: Option<CleanupWaiter>,
    ) -> CleanupWaiter {
        let (done, receiver) = watch::channel(false);
        let waiter = CleanupWaiter { done: receiver };
        let entry = entry.clone();
        let completion = done.clone();
        let task = tokio::spawn(async move {
            if let Some(previous) = previous {
                previous.wait().await;
            }
            if let Some(runtime) = runtime {
                runtime.shutdown().await;
            }
            Self::wait_for_initializations(&entry).await;
            let mut state = entry.state();
            if state.operation_generation == generation {
                state.transitioning = false;
            }
            let generation = state.operation_generation;
            drop(state);
            entry.signal(generation);
            completion.send_replace(true);
        });
        let mut retiring = self.retiring.lock().unwrap_or_else(PoisonError::into_inner);
        retiring.retain(|cleanup| !cleanup.task.is_finished());
        retiring.push(RetiringCleanup { task, _done: done });
        waiter
    }

    async fn wait_for_initializations(entry: &Arc<PerConnection>) {
        let mut changes = entry.changes.subscribe();
        loop {
            if entry.state().initializations == 0 {
                return;
            }
            let _ = changes.changed().await;
        }
    }

    pub async fn shutdown_all(&self) {
        self.shutting_down.store(true, Ordering::Release);
        let _execution = self.shutdown_execution.lock().await;
        let entries = self.entries().values().cloned().collect::<Vec<_>>();
        let mut cleanups = Vec::new();
        for entry in &entries {
            let id = entry.state().profile.id.clone();
            if let Ok(cleanup) = self.begin_cleanup(entry, &id).await {
                cleanups.push(cleanup);
            }
        }
        futures::future::join_all(cleanups.iter().map(|cleanup| cleanup.wait())).await;
        self.retiring
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|cleanup| !cleanup.task.is_finished());
    }

    fn maybe_persist_observed_cluster_id(
        &self,
        entry: &Arc<PerConnection>,
        runtime: &Arc<ConnectionRuntime>,
    ) {
        let Some(observed) = runtime.scheduler.cache.view().trusted_cluster_id() else {
            return;
        };
        {
            let state = entry.state();
            if state.source != ConnectionSource::Profile
                || state.deleting
                || state.profile.observed_cluster_id.as_deref() == Some(observed.as_str())
            {
                return;
            }
        }
        if entry
            .observed_cluster_id_write
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let entry = entry.clone();
        let store = self.store.clone();
        tokio::spawn(async move {
            let _operation = entry.operation.lock().await;
            let (generation, mut profile) = {
                let state = entry.state();
                if state.source != ConnectionSource::Profile
                    || state.deleting
                    || state.profile.observed_cluster_id.as_deref() == Some(observed.as_str())
                {
                    entry
                        .observed_cluster_id_write
                        .store(false, Ordering::Release);
                    return;
                }
                (state.operation_generation, state.profile.clone())
            };
            profile.observed_cluster_id = Some(observed.clone());
            let saved = tokio::task::spawn_blocking(move || store.save(&profile)).await;
            match saved {
                Ok(Ok(())) => {
                    let mut state = entry.state();
                    if state.operation_generation == generation && !state.deleting {
                        state.profile.observed_cluster_id = Some(observed);
                    }
                }
                Ok(Err(error)) => {
                    tracing::warn!(%error, "failed to persist observed Console cluster identity");
                }
                Err(error) => {
                    tracing::warn!(%error, "observed Console cluster identity write failed");
                }
            }
            entry
                .observed_cluster_id_write
                .store(false, Ordering::Release);
        });
    }

    pub fn resolve(&self, id: &str) -> Result<Arc<ConnectionRuntime>, PublicConnectionError> {
        let entry = self.entry(id)?;
        let runtime = {
            let state = entry.state();
            if !matches!(
                state.status,
                ConnectionStatus::Connecting
                    | ConnectionStatus::Connected
                    | ConnectionStatus::Degraded
                    | ConnectionStatus::Unreachable
            ) {
                return Err(PublicConnectionError::not_connected());
            }
            state.runtime.clone()
        };
        let Some(runtime) = runtime.filter(|runtime| !runtime.stopped.load(Ordering::Acquire))
        else {
            return Err(PublicConnectionError::not_connected());
        };
        self.maybe_persist_observed_cluster_id(&entry, &runtime);
        Ok(runtime)
    }
}

struct DeletingGuard {
    entry: Arc<PerConnection>,
    active: bool,
}

impl DeletingGuard {
    fn new(entry: Arc<PerConnection>) -> Self {
        Self {
            entry,
            active: true,
        }
    }

    fn disarm(mut self) {
        self.active = false;
    }
}

impl Drop for DeletingGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.entry.state();
        state.deleting = false;
        let generation = state.operation_generation;
        drop(state);
        self.entry.signal(generation);
    }
}

struct InitializationGuard {
    entry: Arc<PerConnection>,
    generation: u64,
    active: bool,
}

impl InitializationGuard {
    fn new(entry: Arc<PerConnection>, generation: u64) -> Self {
        Self {
            entry,
            generation,
            active: true,
        }
    }

    fn complete(&mut self, state: &mut ManagedConnection) {
        state.initializations = state.initializations.saturating_sub(1);
        self.active = false;
    }
}

impl Drop for InitializationGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.entry.state();
        state.initializations = state.initializations.saturating_sub(1);
        if state.operation_generation == self.generation
            && state.status == ConnectionStatus::Connecting
        {
            state.status = ConnectionStatus::Disconnected;
            state.last_error = Some(PublicConnectionError::initialization_failed().message);
        }
        let generation = state.operation_generation;
        drop(state);
        self.entry.signal(generation);
    }
}

fn read_secrets(secrets: &SecretStore, id: &str) -> Result<(Option<String>, Option<String>)> {
    let admin = secrets
        .get(id, SecretKind::AdminToken)?
        .map(|secret| secret.secret);
    let discovery = secrets
        .get(id, SecretKind::DiscoveryPassword)?
        .map(|secret| secret.secret);
    Ok((admin, discovery))
}

fn discovery_secret_exists(secrets: &SecretStore, id: &str) -> Result<bool, PublicConnectionError> {
    secrets
        .get(id, SecretKind::DiscoveryPassword)
        .map(|secret| secret.is_some())
        .map_err(|_| PublicConnectionError::store_failed())
}

fn validate_profile(profile: &ConnectionProfile) -> Result<(), PublicConnectionError> {
    let valid = !profile.name.trim().is_empty()
        && (1..=300).contains(&profile.poll_interval_secs)
        && (!profile.admin_seeds.is_empty() || profile.discovery.is_some())
        && valid_tls(&profile.admin_tls)
        && valid_tls(&profile.discovery_tls)
        && valid_seeds(&profile.admin_seeds);
    if !valid {
        return Err(PublicConnectionError::invalid_profile());
    }
    if let Some(discovery) = &profile.discovery {
        let security = &profile.discovery_tls;
        let valid = profile
            .cluster_name
            .as_deref()
            .is_some_and(|name| !name.trim().is_empty())
            && parse_discovery_endpoints(discovery).is_ok()
            && !(security.ca.is_some() && profile.discovery_allow_insecure)
            && (security.ca.is_some() || profile.discovery_allow_insecure)
            && (profile.discovery_username.is_none() || security.ca.is_some());
        if !valid {
            return Err(PublicConnectionError::invalid_profile());
        }
    }
    super::connection_store::validate_profile(profile)
        .map_err(|_| PublicConnectionError::invalid_profile())
}

fn valid_tls(tls: &TlsProfile) -> bool {
    tls.cert.is_some() == tls.key.is_some()
        && (tls.cert.is_none() || tls.ca.is_some())
        && (tls.domain.is_none() || tls.ca.is_some())
}

fn valid_seeds(peers: &[super::config::SeedPeer]) -> bool {
    let mut ids = HashSet::new();
    let mut addresses = HashSet::new();
    peers.iter().all(|peer| {
        let address = peer.admin_addr.as_str();
        let valid_address = !address.is_empty()
            && !address.contains("://")
            && !address.contains('@')
            && !address.chars().any(char::is_whitespace)
            && address.rsplit_once(':').is_some_and(|(host, port)| {
                !host.trim_matches(['[', ']']).is_empty()
                    && host.trim_matches(['[', ']']) != "0.0.0.0"
                    && host.trim_matches(['[', ']']) != "::"
                    && port.parse::<u16>().is_ok_and(|port| port != 0)
            });
        peer.node_id != 0 && valid_address && ids.insert(peer.node_id) && addresses.insert(address)
    })
}

#[cfg(test)]
mod tests {
    use super::{project_connection_status, ConnectionManager, RuntimeFactory};
    use crate::console::cache::ConsoleCache;
    use crate::console::config::SeedPeer;
    use crate::console::connection_store::{
        ConnectionStore, Keychain, SecretKind, SecretSource, SecretStore,
    };
    use crate::console::model::{
        ConnectionProfile, ConnectionSecrets, ConnectionSource, ConnectionStatus,
        PrometheusProfile, SecretInput, TlsProfile,
    };
    use crate::console::scheduler::Scheduler;
    use crate::console::upstream::{GroupDetail, NodeSnapshot, Upstream, UpstreamError};
    use anyhow::Result;
    use async_trait::async_trait;
    use rr_network::proto::admin::{BootRecordPage, FormationStatus};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, PoisonError};
    use std::time::{Duration, SystemTime};
    use tokio::sync::Notify;

    fn profile(id: &str) -> ConnectionProfile {
        ConnectionProfile {
            id: id.to_string(),
            name: format!("Connection {id}"),
            environment: Some("test".to_string()),
            admin_seeds: vec![SeedPeer {
                node_id: 1,
                admin_addr: "127.0.0.1:52001".to_string(),
            }],
            discovery: None,
            cluster_name: Some(format!("cluster-{id}")),
            admin_tls: TlsProfile::default(),
            discovery_tls: TlsProfile::default(),
            discovery_username: None,
            discovery_allow_insecure: false,
            prometheus: Some(PrometheusProfile {
                url: "http://127.0.0.1:9090".to_string(),
                cluster: "cluster_id=test".to_string(),
            }),
            poll_interval_secs: 5,
            created_at: String::new(),
            updated_at: String::new(),
            observed_cluster_id: None,
            last_connected_at: None,
        }
    }

    fn node_snapshot(node_id: u64) -> NodeSnapshot {
        NodeSnapshot {
            node_id,
            listen_addr: "127.0.0.1:51001".to_string(),
            uptime_secs: 1,
            groups: Some(vec![]),
            routes: Some(vec![]),
            formation: None,
            descriptor: None,
            build: None,
            boot: None,
            topology: None,
            capabilities: vec![],
            collected_at: SystemTime::now(),
        }
    }

    struct FakeUpstream {
        calls: AtomicUsize,
        block: AtomicBool,
        release: Arc<Notify>,
        block_boots: AtomicBool,
        boot_calls: AtomicUsize,
    }

    #[async_trait]
    impl Upstream for FakeUpstream {
        async fn collect_node(&self, peer: &SeedPeer) -> Result<NodeSnapshot, UpstreamError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.block.load(Ordering::SeqCst) {
                self.release.notified().await;
            }
            Ok(node_snapshot(peer.node_id))
        }

        async fn collect_group(
            &self,
            _peer: &SeedPeer,
            _group_id: u64,
        ) -> Result<GroupDetail, UpstreamError> {
            unreachable!("manager lifecycle tests do not request group details")
        }

        async fn list_boot_records(
            &self,
            _peer: &SeedPeer,
            _cursor: Option<String>,
            _page_size: usize,
        ) -> Result<BootRecordPage, UpstreamError> {
            self.boot_calls.fetch_add(1, Ordering::SeqCst);
            if self.block_boots.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            Ok(BootRecordPage::default())
        }
    }

    struct FakeFactory {
        builds: AtomicUsize,
        tests: AtomicUsize,
        block_build: AtomicBool,
        block_test: AtomicBool,
        block_collection: AtomicBool,
        build_started: Notify,
        test_started: Notify,
        release_build: Notify,
        release_test: Notify,
        block_build_id: Mutex<Option<String>>,
        failures: Mutex<HashMap<String, String>>,
        test_failures: Mutex<HashMap<String, String>>,
        upstreams: Mutex<Vec<Arc<FakeUpstream>>>,
    }

    impl FakeFactory {
        fn new() -> Self {
            Self {
                builds: AtomicUsize::new(0),
                tests: AtomicUsize::new(0),
                block_build: AtomicBool::new(false),
                block_test: AtomicBool::new(false),
                block_collection: AtomicBool::new(false),
                build_started: Notify::new(),
                test_started: Notify::new(),
                release_build: Notify::new(),
                release_test: Notify::new(),
                block_build_id: Mutex::new(None),
                failures: Mutex::new(HashMap::new()),
                test_failures: Mutex::new(HashMap::new()),
                upstreams: Mutex::new(Vec::new()),
            }
        }

        async fn wait_for_builds(&self, expected: usize) {
            tokio::time::timeout(Duration::from_secs(1), async {
                while self.builds.load(Ordering::SeqCst) < expected {
                    self.build_started.notified().await;
                }
            })
            .await
            .unwrap();
        }
    }

    #[async_trait]
    impl RuntimeFactory for FakeFactory {
        async fn build(
            &self,
            profile: &ConnectionProfile,
            _secrets: &SecretStore,
        ) -> Result<Arc<Scheduler>> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            self.build_started.notify_waiters();
            let blocked_id = self.block_build_id.lock().unwrap().clone();
            if self.block_build.load(Ordering::SeqCst)
                && blocked_id.as_deref().is_none_or(|id| id == profile.id)
            {
                self.release_build.notified().await;
            }
            if let Some(error) = self.failures.lock().unwrap().get(&profile.id).cloned() {
                anyhow::bail!(error);
            }
            let cache = ConsoleCache::production(
                profile.cluster_name.clone(),
                Duration::from_secs(profile.poll_interval_secs),
            );
            cache.register_peers(&profile.admin_seeds);
            let upstream = Arc::new(FakeUpstream {
                calls: AtomicUsize::new(0),
                block: AtomicBool::new(self.block_collection.load(Ordering::SeqCst)),
                release: Arc::new(Notify::new()),
                block_boots: AtomicBool::new(false),
                boot_calls: AtomicUsize::new(0),
            });
            self.upstreams.lock().unwrap().push(upstream.clone());
            Ok(Arc::new(Scheduler::new(
                upstream,
                cache,
                profile.admin_seeds.clone(),
                Duration::from_secs(profile.poll_interval_secs),
                Duration::from_secs(60),
                4,
            )))
        }

        async fn test(
            &self,
            profile: &ConnectionProfile,
            _admin_token: Option<String>,
            _discovery_password: Option<String>,
        ) -> Result<()> {
            self.tests.fetch_add(1, Ordering::SeqCst);
            self.test_started.notify_waiters();
            if self.block_test.load(Ordering::SeqCst) {
                self.release_test.notified().await;
            }
            if let Some(error) = self.test_failures.lock().unwrap().get(&profile.id).cloned() {
                anyhow::bail!(error);
            }
            Ok(())
        }
    }

    fn manager() -> (
        tempfile::TempDir,
        Arc<ConnectionManager>,
        Arc<FakeFactory>,
        Arc<SecretStore>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(ConnectionStore::load(directory.path().join("connections.json")).unwrap());
        let secrets = Arc::new(SecretStore::without_keychain());
        let factory = Arc::new(FakeFactory::new());
        let manager = Arc::new(ConnectionManager::with_factory(
            store,
            secrets.clone(),
            factory.clone(),
        ));
        (directory, manager, factory, secrets)
    }

    async fn create(manager: &ConnectionManager, id: &str) {
        manager
            .create(profile(id), ConnectionSecrets::default(), false)
            .await
            .unwrap();
    }

    async fn wait_until(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    struct BlockingKeychain {
        started: Arc<Notify>,
        release: Arc<(Mutex<bool>, Condvar)>,
        values: Mutex<HashMap<String, String>>,
    }

    impl Keychain for BlockingKeychain {
        fn get(&self, key: &str) -> Result<Option<String>, ()> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        fn set(&self, key: &str, value: &str) -> Result<(), ()> {
            self.started.notify_one();
            let (lock, condition) = &*self.release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            self.values
                .lock()
                .unwrap()
                .insert(key.to_string(), value.to_string());
            Ok(())
        }

        fn delete(&self, key: &str) -> Result<(), ()> {
            self.values.lock().unwrap().remove(key);
            Ok(())
        }
    }

    #[test]
    fn cache_evidence_projects_unreachable_degraded_and_connected_recovery() {
        let cache = ConsoleCache::new("cluster-a", Duration::from_secs(30), 8, 8);
        let mut profile = profile("a");
        profile.admin_seeds.push(SeedPeer {
            node_id: 2,
            admin_addr: "127.0.0.1:52002".to_string(),
        });
        cache.register_peers(&profile.admin_seeds);
        assert_eq!(
            project_connection_status(ConnectionStatus::Connected, &profile, &cache.view()),
            ConnectionStatus::Connected
        );

        cache
            .record_success(&profile.admin_seeds[0], node_snapshot(1))
            .unwrap();
        assert_eq!(
            project_connection_status(ConnectionStatus::Connected, &profile, &cache.view()),
            ConnectionStatus::Degraded
        );
        cache.record_failure(&profile.admin_seeds[0], "ADMIN_UNAVAILABLE", "offline");
        assert_eq!(
            project_connection_status(ConnectionStatus::Connected, &profile, &cache.view()),
            ConnectionStatus::Degraded
        );
        cache.record_failure(&profile.admin_seeds[1], "ADMIN_UNAVAILABLE", "offline");
        assert_eq!(
            project_connection_status(ConnectionStatus::Connected, &profile, &cache.view()),
            ConnectionStatus::Unreachable
        );

        cache
            .record_success(&profile.admin_seeds[0], node_snapshot(1))
            .unwrap();
        cache
            .record_success(&profile.admin_seeds[1], node_snapshot(2))
            .unwrap();
        assert_eq!(
            project_connection_status(ConnectionStatus::Unreachable, &profile, &cache.view()),
            ConnectionStatus::Connected
        );

        let stale = ConsoleCache::new("stale", Duration::from_millis(1), 8, 8);
        stale.register_peers(&profile.admin_seeds[..1]);
        stale
            .record_success(&profile.admin_seeds[0], node_snapshot(1))
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(
            project_connection_status(ConnectionStatus::Connected, &profile, &stale.view()),
            ConnectionStatus::Degraded
        );
    }

    #[tokio::test]
    async fn unreachable_public_state_keeps_resolve_and_connect_on_the_live_runtime() {
        let (_directory, manager, factory, _secrets) = manager();
        factory.block_collection.store(true, Ordering::SeqCst);
        create(&manager, "a").await;
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        let seed = profile("a").admin_seeds[0].clone();
        runtime
            .scheduler
            .cache
            .record_failure(&seed, "ADMIN_UNAVAILABLE", "offline");

        assert_eq!(
            manager.get("a").unwrap().status,
            ConnectionStatus::Unreachable
        );
        assert!(Arc::ptr_eq(&runtime, &manager.resolve("a").unwrap()));
        assert_eq!(
            manager.connect("a").await.unwrap().status,
            ConnectionStatus::Unreachable
        );
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);

        runtime
            .scheduler
            .cache
            .record_success(&seed, node_snapshot(1))
            .unwrap();
        assert_eq!(
            manager.get("a").unwrap().status,
            ConnectionStatus::Connected
        );
        assert_eq!(
            manager.connect("a").await.unwrap().status,
            ConnectionStatus::Connected
        );
        let upstream = factory.upstreams.lock().unwrap()[0].clone();
        upstream.block.store(false, Ordering::SeqCst);
        upstream.release.notify_waiters();
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn trusted_runtime_identity_is_persisted_once_in_the_background() {
        let (directory, manager, factory, _secrets) = manager();
        factory.block_collection.store(true, Ordering::SeqCst);
        create(&manager, "a").await;
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        let mut tracked = node_snapshot(1);
        tracked.formation = Some(FormationStatus {
            origin: "Genesis".to_string(),
            cluster_uuid: "trusted-cluster-id".to_string(),
            topology_revision: 1,
            store_revision: 1,
            state: "Active".to_string(),
            groups: vec![],
        });
        runtime
            .scheduler
            .cache
            .record_success(&profile("a").admin_seeds[0], tracked)
            .unwrap();
        let _ = manager.resolve("a").unwrap();

        wait_until(|| {
            manager
                .get("a")
                .unwrap()
                .profile
                .observed_cluster_id
                .as_deref()
                == Some("trusted-cluster-id")
        })
        .await;
        assert_eq!(
            manager.store.list()[0].observed_cluster_id.as_deref(),
            Some("trusted-cluster-id")
        );
        let reloaded = ConnectionStore::load(directory.path().join("connections.json")).unwrap();
        assert_eq!(
            reloaded.list()[0].observed_cluster_id.as_deref(),
            Some("trusted-cluster-id")
        );
        for _ in 0..4 {
            let _ = manager.resolve("a").unwrap();
        }
        assert_eq!(manager.list().unwrap().len(), 1);
        let upstream = factory.upstreams.lock().unwrap()[0].clone();
        upstream.block.store(false, Ordering::SeqCst);
        upstream.release.notify_waiters();
        manager.shutdown_all().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocked_create_io_does_not_block_resolving_an_existing_connection() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(ConnectionStore::load(directory.path().join("connections.json")).unwrap());
        let keychain = Arc::new(BlockingKeychain {
            started: Arc::new(Notify::new()),
            release: Arc::new((Mutex::new(false), Condvar::new())),
            values: Mutex::new(HashMap::new()),
        });
        let secrets = Arc::new(SecretStore::with_test_keychain(keychain.clone()));
        let factory = Arc::new(FakeFactory::new());
        let manager = Arc::new(ConnectionManager::with_factory(store, secrets, factory));
        create(&manager, "b").await;
        manager.connect("b").await.unwrap();
        let creating = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .create(
                        profile("a"),
                        ConnectionSecrets {
                            admin_token: SecretInput::Replace("blocked-secret".to_string()),
                            discovery_password: SecretInput::Keep,
                        },
                        true,
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), keychain.started.notified())
            .await
            .unwrap();
        let resolving = tokio::task::spawn_blocking({
            let manager = manager.clone();
            move || manager.resolve("b")
        });
        let resolved = tokio::time::timeout(Duration::from_millis(200), resolving).await;
        let (lock, condition) = &*keychain.release;
        *lock.lock().unwrap() = true;
        condition.notify_all();
        creating.await.unwrap().unwrap();
        resolved
            .expect("B resolve waited for A's blocking create I/O")
            .unwrap()
            .unwrap();
        manager.shutdown_all().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_create_commits_persisted_profile_secret_and_entry() {
        let directory = tempfile::tempdir().unwrap();
        let store_path = directory.path().join("connections.json");
        let store = Arc::new(ConnectionStore::load(&store_path).unwrap());
        let keychain = Arc::new(BlockingKeychain {
            started: Arc::new(Notify::new()),
            release: Arc::new((Mutex::new(false), Condvar::new())),
            values: Mutex::new(HashMap::new()),
        });
        let secrets = Arc::new(SecretStore::with_test_keychain(keychain.clone()));
        let manager = Arc::new(ConnectionManager::with_factory(
            store,
            secrets.clone(),
            Arc::new(FakeFactory::new()),
        ));
        create(&manager, "b").await;
        manager.connect("b").await.unwrap();
        let creating = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .create(
                        profile("a"),
                        ConnectionSecrets {
                            admin_token: SecretInput::Replace("blocked-secret".to_string()),
                            discovery_password: SecretInput::Keep,
                        },
                        true,
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), keychain.started.notified())
            .await
            .unwrap();

        let resolving = tokio::task::spawn_blocking({
            let manager = manager.clone();
            move || manager.resolve("b")
        });
        let resolved = tokio::time::timeout(Duration::from_millis(200), resolving).await;
        creating.abort();
        let cancelled = matches!(creating.await, Err(error) if error.is_cancelled());
        let reserved_after_abort = manager
            .create_reservations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains("a");
        let (lock, condition) = &*keychain.release;
        *lock.lock().unwrap() = true;
        condition.notify_all();

        wait_until(|| manager.store.list().iter().any(|profile| profile.id == "a")).await;
        let committed = tokio::time::timeout(Duration::from_secs(1), async {
            while manager.get("a").is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        let reloaded = ConnectionStore::load(&store_path).unwrap();
        let secret = secrets.get("a", SecretKind::AdminToken).unwrap();
        let reserved_after_io = manager
            .create_reservations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains("a");
        assert!(
            committed,
            "aborted create left store_profile={} reloaded_profile={} secret={} \
             reserved_after_abort={} reserved_after_io={} without a manager entry",
            manager.store.list().iter().any(|profile| profile.id == "a"),
            reloaded.list().iter().any(|profile| profile.id == "a"),
            secret.is_some(),
            reserved_after_abort,
            reserved_after_io,
        );
        assert!(cancelled);
        assert!(reserved_after_abort);
        assert!(!reserved_after_io);
        resolved
            .expect("B resolve waited for aborted A's blocking create I/O")
            .unwrap()
            .unwrap();
        let secret = secret.unwrap();
        assert_eq!(secret.secret, "blocked-secret");
        assert_eq!(secret.source, SecretSource::Keychain);
        assert_eq!(
            manager.get("a").unwrap().credential_source.as_deref(),
            Some("KEYCHAIN")
        );

        let duplicate = manager
            .create(
                profile("a"),
                ConnectionSecrets {
                    admin_token: SecretInput::Replace("duplicate-secret".to_string()),
                    discovery_password: SecretInput::Keep,
                },
                true,
            )
            .await
            .unwrap_err();
        assert_eq!(duplicate.code, "INVALID_CONNECTION_PROFILE");
        assert!(!format!("{duplicate:?}").contains("duplicate-secret"));
        let expected = vec!["a".to_string(), "b".to_string()];
        let manager_ids = manager
            .list()
            .unwrap()
            .iter()
            .map(|summary| summary.profile.id.clone())
            .collect::<Vec<_>>();
        let store_ids = manager
            .store
            .list()
            .iter()
            .map(|profile| profile.id.clone())
            .collect::<Vec<_>>();
        let reloaded_ids = ConnectionStore::load(&store_path)
            .unwrap()
            .list()
            .iter()
            .map(|profile| profile.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(manager_ids, expected);
        assert_eq!(store_ids, expected);
        assert_eq!(reloaded_ids, expected);
        assert_eq!(
            secrets
                .get("a", SecretKind::AdminToken)
                .unwrap()
                .unwrap()
                .secret,
            "blocked-secret"
        );
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn saved_profiles_are_lazy_and_connect_disconnect_are_idempotent() {
        let (directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        create(&manager, "b").await;
        assert_eq!(factory.builds.load(Ordering::SeqCst), 0);
        assert_eq!(manager.list().unwrap().len(), 2);
        assert_eq!(
            manager.resolve("a").unwrap_err().code,
            "CONNECTION_NOT_CONNECTED"
        );

        let connected = manager.connect("a").await.unwrap();
        assert_eq!(connected.status, ConnectionStatus::Connected);
        assert!(connected.profile.last_connected_at.is_some());
        let runtime = manager.resolve("a").unwrap();
        let generation = runtime.generation;
        let again = manager.connect("a").await.unwrap();
        assert_eq!(again.status, ConnectionStatus::Connected);
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&runtime, &manager.resolve("a").unwrap()));

        manager.disconnect("a").await.unwrap();
        manager.disconnect("a").await.unwrap();
        assert_eq!(
            manager.resolve("a").unwrap_err().code,
            "CONNECTION_NOT_CONNECTED"
        );
        assert!(runtime.stopped.load(Ordering::SeqCst));
        manager.connect("a").await.unwrap();
        assert_eq!(factory.builds.load(Ordering::SeqCst), 2);
        assert!(manager.resolve("a").unwrap().generation > generation);
        assert!(manager
            .get("a")
            .unwrap()
            .profile
            .last_connected_at
            .is_some());

        let reloaded = ConnectionManager::with_factory(
            Arc::new(ConnectionStore::load(directory.path().join("connections.json")).unwrap()),
            Arc::new(SecretStore::without_keychain()),
            factory.clone(),
        );
        assert_eq!(
            reloaded.get("a").unwrap().status,
            ConnectionStatus::Disconnected
        );
        assert_eq!(factory.builds.load(Ordering::SeqCst), 2);
        manager.shutdown_all().await;
        reloaded.shutdown_all().await;
    }

    #[tokio::test]
    async fn failed_initialization_is_sanitized_and_reconnect_uses_a_new_generation() {
        let (_directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        factory
            .failures
            .lock()
            .unwrap()
            .insert("a".to_string(), "private-token must not escape".to_string());

        let error = manager.connect("a").await.unwrap_err();
        assert_eq!(error.message, "Connection initialization failed");
        assert!(!format!("{error:?}").contains("private-token"));
        let summary = manager.get("a").unwrap();
        assert_eq!(summary.status, ConnectionStatus::Disconnected);
        assert_eq!(
            summary.last_error.as_deref(),
            Some("Connection initialization failed")
        );
        assert!(manager.resolve("a").is_err());

        factory.failures.lock().unwrap().remove("a");
        manager.connect("a").await.unwrap();
        assert_eq!(factory.builds.load(Ordering::SeqCst), 2);
        assert_eq!(manager.resolve("a").unwrap().generation, 2);
        assert_eq!(manager.get("a").unwrap().last_error, None);
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn connected_profiles_reject_edit_and_disconnected_edit_updates_secrets() {
        let (_directory, manager, _factory, secrets) = manager();
        manager
            .create(
                profile("a"),
                ConnectionSecrets {
                    admin_token: SecretInput::Replace("old-secret".to_string()),
                    discovery_password: SecretInput::Keep,
                },
                false,
            )
            .await
            .unwrap();
        manager.connect("a").await.unwrap();
        let error = manager
            .update("a", profile("a"), ConnectionSecrets::default(), false)
            .await
            .unwrap_err();
        assert_eq!(error.code, "CONNECTION_ACTIVE");

        manager.disconnect("a").await.unwrap();
        let mut replacement = profile("a");
        replacement.name = "Updated".to_string();
        replacement.created_at = "forged".to_string();
        manager
            .update(
                "a",
                replacement,
                ConnectionSecrets {
                    admin_token: SecretInput::Replace("new-secret".to_string()),
                    discovery_password: SecretInput::Keep,
                },
                false,
            )
            .await
            .unwrap();
        assert_eq!(manager.get("a").unwrap().profile.name, "Updated");
        assert_ne!(manager.get("a").unwrap().profile.created_at, "forged");
        assert_eq!(
            secrets
                .get("a", SecretKind::AdminToken)
                .unwrap()
                .unwrap()
                .secret,
            "new-secret"
        );
        assert_eq!(
            secrets
                .get("a", SecretKind::AdminToken)
                .unwrap()
                .unwrap()
                .source,
            SecretSource::Memory
        );
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn active_limit_includes_connecting_and_connected() {
        let (_directory, manager, factory, _secrets) = manager();
        factory.block_build.store(true, Ordering::SeqCst);
        let mut connecting = tokio::task::JoinSet::new();
        for index in 0..8 {
            let id = format!("c{index}");
            create(&manager, &id).await;
            let manager = manager.clone();
            connecting.spawn(async move { manager.connect(&id).await });
        }
        factory.wait_for_builds(8).await;
        create(&manager, "ninth").await;
        let error = manager.connect("ninth").await.unwrap_err();
        assert_eq!(error.code, "ACTIVE_CONNECTION_LIMIT");
        assert_eq!(
            manager.get("ninth").unwrap().status,
            ConnectionStatus::Disconnected
        );
        factory.block_build.store(false, Ordering::SeqCst);
        factory.release_build.notify_waiters();
        while let Some(result) = connecting.join_next().await {
            result.unwrap().unwrap();
        }

        create(&manager, "tenth").await;
        assert_eq!(
            manager.connect("tenth").await.unwrap_err().code,
            "ACTIVE_CONNECTION_LIMIT"
        );
        manager.disconnect("c0").await.unwrap();
        manager.connect("tenth").await.unwrap();
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn delete_stops_runtime_and_removes_profile_and_secrets() {
        let (_directory, manager, _factory, secrets) = manager();
        manager
            .create(
                profile("a"),
                ConnectionSecrets {
                    admin_token: SecretInput::Replace("delete-me".to_string()),
                    discovery_password: SecretInput::Keep,
                },
                false,
            )
            .await
            .unwrap();
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        let mut shutdown = runtime.shutdown.subscribe();
        let store = manager.store.clone();
        let profiles_at_shutdown = tokio::spawn(async move {
            shutdown.changed().await.unwrap();
            store.list().len()
        });

        manager.remove("a").await.unwrap();
        manager.remove("a").await.unwrap();
        assert!(runtime.stopped.load(Ordering::SeqCst));
        assert!(runtime.task.lock().await.is_none());
        assert_eq!(profiles_at_shutdown.await.unwrap(), 1);
        assert_eq!(
            manager.resolve("a").unwrap_err().code,
            "CONNECTION_NOT_FOUND"
        );
        assert_eq!(manager.get("a").unwrap_err().code, "CONNECTION_NOT_FOUND");
        assert!(secrets.get("a", SecretKind::AdminToken).unwrap().is_none());
        assert!(manager.store.list().is_empty());
        manager.shutdown_all().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_delete_persistence_still_stops_the_detached_runtime() {
        use std::os::unix::fs::PermissionsExt;

        let (directory, manager, _factory, _secrets) = manager();
        create(&manager, "a").await;
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let result = manager.remove("a").await;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();

        assert_eq!(result.unwrap_err().code, "CONNECTION_STORE_FAILED");
        assert!(runtime.stopped.load(Ordering::SeqCst));
        assert!(runtime.task.lock().await.is_none());
        assert_eq!(manager.store.list().len(), 1);
        assert_eq!(
            manager.get("a").unwrap().status,
            ConnectionStatus::Disconnected
        );
        manager.remove("a").await.unwrap();
        assert_eq!(
            manager.resolve("a").unwrap_err().code,
            "CONNECTION_NOT_FOUND"
        );
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn shutdown_all_stops_runtimes_and_runtime_subscribers() {
        let (_directory, manager, _factory, _secrets) = manager();
        create(&manager, "a").await;
        create(&manager, "b").await;
        manager.connect("a").await.unwrap();
        manager.connect("b").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        let mut shutdown = runtime.shutdown.subscribe();
        let subscriber = tokio::spawn(async move {
            shutdown.changed().await.unwrap();
            assert!(*shutdown.borrow());
        });

        manager.shutdown_all().await;
        manager.shutdown_all().await;
        tokio::time::timeout(Duration::from_secs(1), subscriber)
            .await
            .unwrap()
            .unwrap();
        assert!(runtime.stopped.load(Ordering::SeqCst));
        assert_eq!(
            manager.resolve("b").unwrap_err().code,
            "CONNECTION_NOT_CONNECTED"
        );
        assert!(manager.connect("a").await.is_err());
    }

    #[tokio::test]
    async fn concurrent_connects_share_one_runtime() {
        let (_directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        factory.block_build.store(true, Ordering::SeqCst);
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let manager = manager.clone();
            tasks.spawn(async move { manager.connect("a").await });
        }
        factory.wait_for_builds(1).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
        factory.block_build.store(false, Ordering::SeqCst);
        factory.release_build.notify_waiters();
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn disconnect_invalidates_late_initialization_without_global_lock() {
        let (_directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        create(&manager, "b").await;
        factory.block_build.store(true, Ordering::SeqCst);
        *factory.block_build_id.lock().unwrap() = Some("a".to_string());
        let connecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.connect("a").await }
        });
        factory.wait_for_builds(1).await;

        manager.connect("b").await.unwrap();
        assert_eq!(
            manager.get("b").unwrap().status,
            ConnectionStatus::Connected
        );
        let disconnecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.disconnect("a").await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!disconnecting.is_finished());
        factory.block_build.store(false, Ordering::SeqCst);
        factory.release_build.notify_waiters();
        assert_eq!(
            connecting.await.unwrap().unwrap_err().code,
            "CONNECTION_NOT_CONNECTED"
        );
        disconnecting.await.unwrap().unwrap();
        assert_eq!(
            manager.resolve("a").unwrap_err().code,
            "CONNECTION_NOT_CONNECTED"
        );
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn delete_racing_connect_leaves_no_runtime_or_profile() {
        let (_directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        factory.block_build.store(true, Ordering::SeqCst);
        let connecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.connect("a").await }
        });
        factory.wait_for_builds(1).await;
        let deleting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.remove("a").await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(manager.entry("a").unwrap().state().deleting);
        let reconnecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.connect("a").await }
        });
        assert_eq!(
            reconnecting.await.unwrap().unwrap_err().code,
            "CONNECTION_NOT_FOUND"
        );
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
        factory.block_build.store(false, Ordering::SeqCst);
        factory.release_build.notify_waiters();
        assert!(connecting.await.unwrap().is_err());
        deleting.await.unwrap().unwrap();
        assert_eq!(
            manager.resolve("a").unwrap_err().code,
            "CONNECTION_NOT_FOUND"
        );
        assert!(manager.store.list().is_empty());
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn shutdown_all_invalidates_every_entry_before_waiting_for_initialization() {
        let (_directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        create(&manager, "b").await;
        let first_id = manager.entries().keys().next().unwrap().clone();
        let other_id = if first_id == "a" { "b" } else { "a" }.to_string();
        manager.connect(&other_id).await.unwrap();
        let other_runtime = manager.resolve(&other_id).unwrap();

        factory.block_build.store(true, Ordering::SeqCst);
        *factory.block_build_id.lock().unwrap() = Some(first_id.clone());
        let connecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.connect(&first_id).await }
        });
        factory.wait_for_builds(2).await;
        let shutting_down = tokio::spawn({
            let manager = manager.clone();
            async move { manager.shutdown_all().await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(other_runtime.stopped.load(Ordering::SeqCst));
        assert!(!shutting_down.is_finished());
        factory.block_build.store(false, Ordering::SeqCst);
        factory.release_build.notify_waiters();
        assert!(connecting.await.unwrap().is_err());
        tokio::time::timeout(Duration::from_secs(1), shutting_down)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn aborted_disconnect_retains_runtime_and_derived_task_cleanup() {
        let (_directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        let upstream = factory.upstreams.lock().unwrap()[0].clone();
        upstream.block_boots.store(true, Ordering::SeqCst);
        let boot = tokio::spawn({
            let scheduler = runtime.scheduler.clone();
            async move { scheduler.list_boot_records(1, None, 5).await }
        });
        wait_until(|| upstream.boot_calls.load(Ordering::SeqCst) == 1).await;

        let cleanup_barrier = runtime.task.lock().await;
        let entry = manager.entry("a").unwrap();
        let disconnecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.disconnect("a").await }
        });
        wait_until(|| entry.state().runtime.is_none()).await;
        disconnecting.abort();
        let _ = disconnecting.await;
        assert!(!runtime.stopped.load(Ordering::SeqCst));
        assert!(manager
            .retiring
            .lock()
            .unwrap()
            .iter()
            .any(|cleanup| !cleanup.task.is_finished()));
        drop(cleanup_barrier);

        wait_until(|| runtime.stopped.load(Ordering::SeqCst)).await;
        assert!(runtime.task.lock().await.is_none());
        let error = tokio::time::timeout(Duration::from_secs(1), boot)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, "CONNECTION_CLOSED");
    }

    #[tokio::test]
    async fn aborted_remove_finishes_cleanup_and_allows_retry() {
        let (_directory, manager, _factory, _secrets) = manager();
        create(&manager, "a").await;
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        let cleanup_barrier = runtime.task.lock().await;
        let entry = manager.entry("a").unwrap();
        let removing = tokio::spawn({
            let manager = manager.clone();
            async move { manager.remove("a").await }
        });
        wait_until(|| entry.state().deleting && entry.state().runtime.is_none()).await;
        removing.abort();
        let _ = removing.await;
        assert!(!entry.state().deleting);
        drop(cleanup_barrier);

        tokio::time::timeout(Duration::from_secs(1), manager.remove("a"))
            .await
            .unwrap()
            .unwrap();
        assert!(runtime.stopped.load(Ordering::SeqCst));
        assert!(runtime.task.lock().await.is_none());
        assert!(manager.store.list().is_empty());
        assert_eq!(
            manager.resolve("a").unwrap_err().code,
            "CONNECTION_NOT_FOUND"
        );
    }

    #[tokio::test]
    async fn concurrent_shutdown_all_waits_for_in_progress_cleanup() {
        let (_directory, manager, _factory, _secrets) = manager();
        create(&manager, "a").await;
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        let cleanup_barrier = runtime.task.lock().await;
        let entry = manager.entry("a").unwrap();
        let first = tokio::spawn({
            let manager = manager.clone();
            async move { manager.shutdown_all().await }
        });
        wait_until(|| entry.state().runtime.is_none()).await;
        let second = tokio::spawn({
            let manager = manager.clone();
            async move { manager.shutdown_all().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!second.is_finished());
        drop(cleanup_barrier);

        tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap();
        assert!(runtime.stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelled_shutdown_all_can_be_retried() {
        let (_directory, manager, _factory, _secrets) = manager();
        create(&manager, "a").await;
        create(&manager, "b").await;
        manager.connect("a").await.unwrap();
        manager.connect("b").await.unwrap();
        let first_id = manager.entries().keys().next().unwrap().clone();
        let other_id = if first_id == "a" { "b" } else { "a" }.to_string();
        let first_entry = manager.entry(&first_id).unwrap();
        let other_entry = manager.entry(&other_id).unwrap();
        let first_runtime = manager.resolve(&first_id).unwrap();
        let other_runtime = manager.resolve(&other_id).unwrap();
        let operation = other_entry.operation.lock().await;
        let first = tokio::spawn({
            let manager = manager.clone();
            async move { manager.shutdown_all().await }
        });
        wait_until(|| first_entry.state().runtime.is_none()).await;
        first.abort();
        let _ = first.await;
        assert!(!other_runtime.stopped.load(Ordering::SeqCst));
        drop(operation);

        tokio::time::timeout(Duration::from_secs(1), manager.shutdown_all())
            .await
            .unwrap();
        assert!(first_runtime.stopped.load(Ordering::SeqCst));
        assert!(other_runtime.stopped.load(Ordering::SeqCst));
        assert!(other_runtime.task.lock().await.is_none());
    }

    #[tokio::test]
    async fn reconnect_waits_for_invalidated_initialization() {
        let (_directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        factory.block_build.store(true, Ordering::SeqCst);
        let original = tokio::spawn({
            let manager = manager.clone();
            async move { manager.connect("a").await }
        });
        factory.wait_for_builds(1).await;
        let entry = manager.entry("a").unwrap();
        let disconnecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.disconnect("a").await }
        });
        wait_until(|| entry.state().transitioning).await;
        let reconnecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.connect("a").await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
        assert!(!reconnecting.is_finished());
        factory.block_build.store(false, Ordering::SeqCst);
        factory.release_build.notify_waiters();

        assert_eq!(
            original.await.unwrap().unwrap_err().code,
            "CONNECTION_NOT_CONNECTED"
        );
        assert_eq!(
            disconnecting.await.unwrap().unwrap().status,
            ConnectionStatus::Disconnected
        );
        let reconnected = reconnecting.await.unwrap().unwrap();
        assert_eq!(reconnected.status, ConnectionStatus::Connected);
        assert!(reconnected.profile.last_connected_at.is_some());
        assert_eq!(factory.builds.load(Ordering::SeqCst), 2);
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn disconnect_delays_reconnect_until_old_runtime_cleanup() {
        let (_directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        let cleanup_barrier = runtime.task.lock().await;
        let entry = manager.entry("a").unwrap();
        let disconnecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.disconnect("a").await }
        });
        wait_until(|| entry.state().runtime.is_none()).await;
        factory.block_build.store(true, Ordering::SeqCst);
        let reconnecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.connect("a").await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
        assert!(!reconnecting.is_finished());
        drop(cleanup_barrier);
        factory.wait_for_builds(2).await;
        factory.block_build.store(false, Ordering::SeqCst);
        factory.release_build.notify_waiters();

        let disconnected = disconnecting.await.unwrap().unwrap();
        let reconnected = reconnecting.await.unwrap().unwrap();
        assert_eq!(disconnected.status, ConnectionStatus::Disconnected);
        assert_eq!(reconnected.status, ConnectionStatus::Connected);
        assert!(runtime.stopped.load(Ordering::SeqCst));
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn disconnect_delays_update_until_old_runtime_cleanup() {
        let (_directory, manager, _factory, _secrets) = manager();
        create(&manager, "a").await;
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();
        let cleanup_barrier = runtime.task.lock().await;
        let entry = manager.entry("a").unwrap();
        let disconnecting = tokio::spawn({
            let manager = manager.clone();
            async move { manager.disconnect("a").await }
        });
        wait_until(|| entry.state().runtime.is_none()).await;
        let mut replacement = profile("a");
        replacement.name = "Updated after cleanup".to_string();
        let updating = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .update("a", replacement, ConnectionSecrets::default(), false)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!updating.is_finished());
        drop(cleanup_barrier);

        assert_eq!(
            disconnecting.await.unwrap().unwrap().status,
            ConnectionStatus::Disconnected
        );
        assert_eq!(
            updating.await.unwrap().unwrap().profile.name,
            "Updated after cleanup"
        );
        assert!(runtime.stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn secret_delete_failure_still_shuts_down_runtime() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(ConnectionStore::load(directory.path().join("connections.json")).unwrap());
        let secrets = Arc::new(SecretStore::with_failing_deletes());
        let factory = Arc::new(FakeFactory::new());
        let manager = ConnectionManager::with_factory(store, secrets, factory);
        create(&manager, "a").await;
        manager.connect("a").await.unwrap();
        let runtime = manager.resolve("a").unwrap();

        let error = manager.remove("a").await.unwrap_err();
        assert_eq!(error.code, "CONNECTION_STORE_FAILED");
        assert_eq!(error.message, "Connection state operation failed");
        assert!(runtime.stopped.load(Ordering::SeqCst));
        assert!(runtime.task.lock().await.is_none());
        assert_eq!(
            manager.get("a").unwrap().status,
            ConnectionStatus::Disconnected
        );
    }

    #[tokio::test]
    async fn candidate_test_has_a_total_deadline_and_creates_no_runtime() {
        let (_directory, manager, factory, _secrets) = manager();
        factory.block_test.store(true, Ordering::SeqCst);
        let started = std::time::Instant::now();
        let error = manager
            .test_candidate(profile("candidate"), None, None)
            .await
            .unwrap_err();
        assert_eq!(error.code, "CONNECTION_TEST_FAILED");
        assert_eq!(error.message, "Connection test failed");
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(factory.tests.load(Ordering::SeqCst), 1);
        assert_eq!(factory.builds.load(Ordering::SeqCst), 0);
        assert!(manager.store.list().is_empty());
    }

    #[tokio::test]
    async fn candidate_and_saved_profile_test_errors_are_fixed_and_sanitized() {
        let (_directory, manager, factory, _secrets) = manager();
        create(&manager, "a").await;
        for id in ["candidate", "a"] {
            factory
                .test_failures
                .lock()
                .unwrap()
                .insert(id.to_string(), "password=do-not-disclose".to_string());
        }
        let candidate = manager
            .test_candidate(
                profile("candidate"),
                Some("do-not-disclose".to_string()),
                None,
            )
            .await
            .unwrap_err();
        let saved = manager.test("a").await.unwrap_err();
        for error in [candidate, saved] {
            assert_eq!(error.code, "CONNECTION_TEST_FAILED");
            assert_eq!(error.message, "Connection test failed");
            assert!(!format!("{error:?}").contains("do-not-disclose"));
        }
        assert_eq!(factory.builds.load(Ordering::SeqCst), 0);
        assert_eq!(
            manager.get("a").unwrap().status,
            ConnectionStatus::Disconnected
        );
    }

    #[tokio::test]
    async fn candidate_test_does_not_require_a_persisted_connection_id() {
        let (_directory, manager, factory, _secrets) = manager();
        let mut candidate = profile("candidate");
        candidate.id.clear();
        manager.test_candidate(candidate, None, None).await.unwrap();
        assert!(manager.store.list().is_empty());
        assert_eq!(factory.builds.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn discovery_candidates_support_ca_only_but_reject_plaintext_auth() {
        let (_directory, manager, factory, _secrets) = manager();
        let ca =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../dev-certs/ca.pem");
        let mut candidate = profile("discovery");
        candidate.admin_seeds.clear();
        candidate.discovery = Some("etcd://127.0.0.1:2379".to_string());
        candidate.cluster_name = Some("test".to_string());
        candidate.discovery_tls.ca = Some(ca);
        manager
            .test_candidate(candidate.clone(), None, None)
            .await
            .unwrap();

        candidate.discovery_username = Some("operator".to_string());
        assert_eq!(
            manager
                .test_candidate(candidate.clone(), None, None)
                .await
                .unwrap_err()
                .code,
            "CREDENTIALS_REQUIRED"
        );
        candidate.discovery_tls.ca = None;
        candidate.discovery_allow_insecure = true;
        let error = manager
            .test_candidate(candidate, None, Some("secret".to_string()))
            .await
            .unwrap_err();
        assert_eq!(error.code, "INVALID_CONNECTION_PROFILE");
        assert_eq!(error.message, "Connection profile is invalid");
        assert_eq!(factory.builds.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn startup_connections_support_lifecycle_without_touching_the_profile_store() {
        let (_directory, manager, _factory, secrets) = manager();
        let summary = manager
            .create_startup(
                profile("startup"),
                ConnectionSecrets {
                    admin_token: SecretInput::Replace("startup-secret".to_string()),
                    discovery_password: SecretInput::Keep,
                },
            )
            .await
            .unwrap();
        assert_eq!(summary.source, ConnectionSource::Startup);
        assert!(manager.store.list().is_empty());

        let connected = manager.connect("startup").await.unwrap();
        assert_eq!(connected.source, ConnectionSource::Startup);
        assert!(connected.profile.last_connected_at.is_some());
        assert!(manager.store.list().is_empty());
        manager.disconnect("startup").await.unwrap();
        assert!(secrets
            .get("startup", SecretKind::AdminToken)
            .unwrap()
            .is_some());

        let mut updated = profile("startup");
        updated.name = "Updated startup".to_string();
        let updated = manager
            .update("startup", updated, ConnectionSecrets::default(), false)
            .await
            .unwrap();
        assert_eq!(updated.source, ConnectionSource::Startup);
        assert_eq!(updated.profile.name, "Updated startup");
        assert!(manager.store.list().is_empty());

        manager.remove("startup").await.unwrap();
        assert_eq!(
            manager.get("startup").unwrap_err().code,
            "CONNECTION_NOT_FOUND"
        );
        assert!(secrets
            .get("startup", SecretKind::AdminToken)
            .unwrap()
            .is_none());
        assert!(manager.store.list().is_empty());
    }
}
