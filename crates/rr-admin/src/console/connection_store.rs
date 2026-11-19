use super::model::{ConnectionId, ConnectionProfile, ConnectionSecrets, SecretInput};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

const PROFILE_STORE_VERSION: u32 = 1;
const KEYCHAIN_SERVICE: &str = "io.github.gardenias.rr.console";

#[derive(Debug, Serialize, Deserialize)]
struct ProfileEnvelope {
    version: u32,
    profiles: Vec<ConnectionProfile>,
}

#[derive(Debug)]
pub(crate) struct ConnectionStore {
    path: PathBuf,
    profiles: Mutex<BTreeMap<ConnectionId, ConnectionProfile>>,
}

impl ConnectionStore {
    pub fn default_path(state_dir: Option<&Path>) -> Result<PathBuf> {
        let directory = match state_dir {
            Some(directory) => directory.to_path_buf(),
            None => dirs::config_dir()
                .context("platform configuration directory is unavailable")?
                .join("rr")
                .join("console"),
        };
        Ok(directory.join("connections.json"))
    }

    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let profiles = match std::fs::read(&path) {
            Ok(bytes) => {
                let envelope: ProfileEnvelope = serde_json::from_slice(&bytes)
                    .with_context(|| format!("parse Console profile store {}", path.display()))?;
                ensure!(
                    envelope.version == PROFILE_STORE_VERSION,
                    "unsupported Console profile store version {}",
                    envelope.version
                );
                let mut profiles = BTreeMap::new();
                for profile in envelope.profiles {
                    validate_profile(&profile)?;
                    ensure!(
                        !profiles.contains_key(&profile.id),
                        "duplicate connection profile ID {}",
                        profile.id
                    );
                    profiles.insert(profile.id.clone(), profile);
                }
                profiles
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read Console profile store {}", path.display()));
            }
        };
        Ok(Self {
            path,
            profiles: Mutex::new(profiles),
        })
    }

    pub fn save(&self, profile: &ConnectionProfile) -> Result<()> {
        validate_profile(profile)?;
        let mut guard = self.lock_profiles();
        let mut profiles = guard.clone();
        profiles.insert(profile.id.clone(), profile.clone());
        self.write(&ProfileEnvelope {
            version: PROFILE_STORE_VERSION,
            profiles: profiles.clone().into_values().collect(),
        })?;
        *guard = profiles;
        Ok(())
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        let mut guard = self.lock_profiles();
        if !guard.contains_key(id) {
            return Ok(());
        }
        let mut profiles = guard.clone();
        profiles.remove(id);
        self.write(&ProfileEnvelope {
            version: PROFILE_STORE_VERSION,
            profiles: profiles.clone().into_values().collect(),
        })?;
        *guard = profiles;
        Ok(())
    }

    pub fn list(&self) -> Vec<ConnectionProfile> {
        self.lock_profiles().values().cloned().collect()
    }

    fn lock_profiles(&self) -> MutexGuard<'_, BTreeMap<ConnectionId, ConnectionProfile>> {
        self.profiles.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self, envelope: &ProfileEnvelope) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(envelope).context("serialize Console profiles")?;
        atomic_write(&self.path, &bytes)
    }
}

pub(crate) fn validate_profile(profile: &ConnectionProfile) -> Result<()> {
    if let Some(prometheus) = &profile.prometheus {
        let url = reqwest::Url::parse(&prometheus.url)
            .context("Prometheus URL must be an absolute HTTP or HTTPS URL")?;
        ensure!(
            matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
            "Prometheus URL must be an absolute HTTP or HTTPS URL"
        );
        let raw_authority = prometheus
            .url
            .split_once("://")
            .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or_default())
            .unwrap_or_default();
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && !raw_authority.contains('@')
                && url.query().is_none()
                && url.fragment().is_none(),
            "Prometheus URL must not contain userinfo, query, or fragment"
        );
    }
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("Console profile store path must have a parent directory")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create Console state directory {}", parent.display()))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).with_context(|| {
        format!(
            "create temporary Console profile store in {}",
            parent.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))
            .context("restrict temporary Console profile store permissions")?;
    }
    temporary
        .write_all(bytes)
        .and_then(|()| temporary.as_file().sync_all())
        .context("write and sync temporary Console profile store")?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| {
            format!(
                "atomically replace Console profile store {}",
                path.display()
            )
        })?;
    #[cfg(unix)]
    if let Ok(directory) = std::fs::File::open(parent) {
        if let Err(error) = directory.sync_all() {
            tracing::warn!(%error, "failed to sync Console profile store directory");
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SecretKind {
    AdminToken,
    DiscoveryPassword,
}

impl SecretKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::AdminToken => "admin_token",
            Self::DiscoveryPassword => "discovery_password",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SecretSource {
    Memory,
    Keychain,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct StoredSecret {
    pub secret: String,
    pub source: SecretSource,
}

impl fmt::Debug for StoredSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredSecret")
            .field("secret", &"REDACTED")
            .field("source", &self.source)
            .finish()
    }
}

pub(crate) trait Keychain: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<String>, ()>;
    fn set(&self, key: &str, value: &str) -> Result<(), ()>;
    fn delete(&self, key: &str) -> Result<(), ()>;
}

struct OsKeychain;

#[cfg(test)]
struct FailingDeleteKeychain;

#[cfg(test)]
impl Keychain for FailingDeleteKeychain {
    fn get(&self, _key: &str) -> Result<Option<String>, ()> {
        Ok(None)
    }

    fn set(&self, _key: &str, _value: &str) -> Result<(), ()> {
        Ok(())
    }

    fn delete(&self, _key: &str) -> Result<(), ()> {
        Err(())
    }
}

impl OsKeychain {
    fn entry(key: &str) -> Result<keyring::Entry, ()> {
        keyring::Entry::new(KEYCHAIN_SERVICE, key).map_err(|_| ())
    }
}

impl Keychain for OsKeychain {
    fn get(&self, key: &str) -> Result<Option<String>, ()> {
        match Self::entry(key)?.get_password() {
            Ok(secret) => Ok(Some(secret)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(_) => Err(()),
        }
    }

    fn set(&self, key: &str, value: &str) -> Result<(), ()> {
        Self::entry(key)?.set_password(value).map_err(|_| ())
    }

    fn delete(&self, key: &str) -> Result<(), ()> {
        match Self::entry(key)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(_) => Err(()),
        }
    }
}

pub(crate) struct SecretStore {
    memory: Mutex<HashMap<(ConnectionId, SecretKind), String>>,
    keychain: Option<Arc<dyn Keychain>>,
    keychain_available: AtomicBool,
}

impl SecretStore {
    pub fn new() -> Self {
        let keychain: Option<Arc<dyn Keychain>> = keyring::Entry::store_status()
            .is_ok()
            .then(|| Arc::new(OsKeychain) as Arc<dyn Keychain>);
        Self::with_keychain(keychain)
    }

    fn with_keychain(keychain: Option<Arc<dyn Keychain>>) -> Self {
        let available = keychain.is_some();
        Self {
            memory: Mutex::new(HashMap::new()),
            keychain,
            keychain_available: AtomicBool::new(available),
        }
    }

    pub(crate) fn without_keychain() -> Self {
        Self::with_keychain(None)
    }

    #[cfg(test)]
    pub(crate) fn with_test_keychain(keychain: Arc<dyn Keychain>) -> Self {
        Self::with_keychain(Some(keychain))
    }

    #[cfg(test)]
    pub(crate) fn with_failing_deletes() -> Self {
        Self::with_keychain(Some(Arc::new(FailingDeleteKeychain)))
    }

    pub fn keychain_available(&self) -> bool {
        self.keychain_available.load(Ordering::Relaxed)
    }

    pub fn get(&self, id: &str, kind: SecretKind) -> Result<Option<StoredSecret>> {
        if let Some(secret) = self.lock_memory().get(&(id.to_string(), kind)).cloned() {
            return Ok(Some(StoredSecret {
                secret,
                source: SecretSource::Memory,
            }));
        }
        let Some(keychain) = self.available_keychain() else {
            return Ok(None);
        };
        let secret = keychain
            .get(&secret_key(id, kind))
            .map_err(|()| self.keychain_error())?;
        Ok(secret.map(|secret| StoredSecret {
            secret,
            source: SecretSource::Keychain,
        }))
    }

    pub fn replace(
        &self,
        id: &str,
        kind: SecretKind,
        value: &str,
        remember: bool,
    ) -> Result<SecretSource> {
        ensure!(!value.is_empty(), "secret must not be empty");
        if remember {
            if let Some(keychain) = self.keychain.as_ref() {
                keychain
                    .set(&secret_key(id, kind), value)
                    .map_err(|()| self.keychain_error())?;
                self.keychain_available.store(true, Ordering::Relaxed);
                self.lock_memory().remove(&(id.to_string(), kind));
                return Ok(SecretSource::Keychain);
            }
        } else if let Some(keychain) = self.keychain.as_ref() {
            keychain
                .delete(&secret_key(id, kind))
                .map_err(|()| self.keychain_error())?;
            self.keychain_available.store(true, Ordering::Relaxed);
        }
        self.lock_memory()
            .insert((id.to_string(), kind), value.to_string());
        Ok(SecretSource::Memory)
    }

    pub fn update(&self, id: &str, secrets: &ConnectionSecrets, remember: bool) -> Result<()> {
        self.apply(id, SecretKind::AdminToken, &secrets.admin_token, remember)?;
        self.apply(
            id,
            SecretKind::DiscoveryPassword,
            &secrets.discovery_password,
            remember,
        )?;
        Ok(())
    }

    pub fn clear(&self, id: &str, kind: SecretKind) -> Result<()> {
        self.lock_memory().remove(&(id.to_string(), kind));
        if let Some(keychain) = self.keychain.as_ref() {
            keychain
                .delete(&secret_key(id, kind))
                .map_err(|()| self.keychain_error())?;
            self.keychain_available.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        self.lock_memory()
            .retain(|(connection, _), _| connection != id);
        let mut failed = false;
        if let Some(keychain) = self.keychain.as_ref() {
            for kind in [SecretKind::AdminToken, SecretKind::DiscoveryPassword] {
                if keychain.delete(&secret_key(id, kind)).is_err() {
                    failed = true;
                }
            }
        }
        if failed {
            return Err(self.keychain_error());
        }
        if self.keychain.is_some() {
            self.keychain_available.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn apply(&self, id: &str, kind: SecretKind, input: &SecretInput, remember: bool) -> Result<()> {
        match input {
            SecretInput::Keep => Ok(()),
            SecretInput::Clear => self.clear(id, kind),
            SecretInput::Replace(value) => self.replace(id, kind, value, remember).map(|_| ()),
        }
    }

    fn available_keychain(&self) -> Option<&Arc<dyn Keychain>> {
        self.keychain_available()
            .then_some(self.keychain.as_ref())
            .flatten()
    }

    fn keychain_error(&self) -> anyhow::Error {
        self.keychain_available.store(false, Ordering::Relaxed);
        anyhow::anyhow!("OS keychain operation failed")
    }

    fn lock_memory(&self) -> MutexGuard<'_, HashMap<(ConnectionId, SecretKind), String>> {
        self.memory.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl fmt::Debug for SecretStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretStore")
            .field("keychain_available", &self.keychain_available())
            .field("memory_secret_count", &self.lock_memory().len())
            .finish_non_exhaustive()
    }
}

fn secret_key(id: &str, kind: SecretKind) -> String {
    format!("{id}/{}", kind.as_str())
}

#[cfg(test)]
mod tests {
    use super::{ConnectionStore, Keychain, SecretKind, SecretSource, SecretStore};
    use crate::console::config::SeedPeer;
    use crate::console::model::{
        ConnectionProfile, ConnectionSecrets, PrometheusProfile, SecretInput, TlsProfile,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

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
            cluster_name: Some("cluster-a".to_string()),
            admin_tls: TlsProfile::default(),
            discovery_tls: TlsProfile::default(),
            discovery_username: None,
            discovery_allow_insecure: false,
            prometheus: Some(PrometheusProfile {
                url: "https://prometheus.internal".to_string(),
                cluster: "cluster_id=cluster-a".to_string(),
            }),
            poll_interval_secs: 5,
            created_at: "2026-11-18T00:00:00.000Z".to_string(),
            updated_at: "2026-11-18T00:00:00.000Z".to_string(),
            observed_cluster_id: None,
            last_connected_at: None,
        }
    }

    #[test]
    fn profiles_round_trip_reload_and_replace_the_same_id() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("connections.json");
        let store = ConnectionStore::load(&path).unwrap();
        assert!(store.list().is_empty());

        store.save(&profile("b")).unwrap();
        store.save(&profile("a")).unwrap();
        let mut replacement = profile("a");
        replacement.name = "Replacement".to_string();
        store.save(&replacement).unwrap();
        assert_eq!(store.list(), vec![replacement.clone(), profile("b")]);

        drop(store);
        let reloaded = ConnectionStore::load(&path).unwrap();
        assert_eq!(reloaded.list(), vec![replacement, profile("b")]);
    }

    #[test]
    fn explicit_state_dir_selects_the_profile_file() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            ConnectionStore::default_path(Some(directory.path())).unwrap(),
            directory.path().join("connections.json")
        );
    }

    #[test]
    fn duplicate_profile_ids_on_disk_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("connections.json");
        let duplicate = profile("duplicate");
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "profiles": [duplicate.clone(), duplicate],
            }))
            .unwrap(),
        )
        .unwrap();

        let error = ConnectionStore::load(&path).unwrap_err().to_string();
        assert!(error.contains("duplicate"), "unexpected error: {error}");
    }

    #[test]
    fn corrupt_json_is_an_error_and_never_an_empty_store() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("connections.json");
        std::fs::write(&path, b"{not-json").unwrap();

        assert!(ConnectionStore::load(&path).is_err());
        assert!(ConnectionStore::load(&path).is_err());
    }

    #[test]
    fn non_current_format_versions_are_rejected() {
        for version in [0, 2] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("connections.json");
            std::fs::write(
                &path,
                serde_json::to_vec(&serde_json::json!({
                    "version": version,
                    "profiles": [],
                }))
                .unwrap(),
            )
            .unwrap();
            let error = ConnectionStore::load(&path).unwrap_err().to_string();
            assert!(error.contains("version"), "unexpected error: {error}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn failed_atomic_write_preserves_the_previous_file_and_memory_state() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("connections.json");
        let store = ConnectionStore::load(&path).unwrap();
        let original = profile("original");
        store.save(&original).unwrap();
        let original_bytes = std::fs::read(&path).unwrap();

        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let result = store.save(&profile("new"));
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();

        assert!(result.is_err());
        assert_eq!(store.list(), vec![original.clone()]);
        assert_eq!(std::fs::read(&path).unwrap(), original_bytes);
        assert_eq!(ConnectionStore::load(&path).unwrap().list(), vec![original]);
    }

    #[cfg(unix)]
    #[test]
    fn profile_files_have_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("connections.json");
        let store = ConnectionStore::load(&path).unwrap();
        store.save(&profile("private")).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn deleting_a_profile_is_persistent_and_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("connections.json");
        let store = ConnectionStore::load(&path).unwrap();
        store.save(&profile("keep")).unwrap();
        store.save(&profile("delete")).unwrap();

        store.remove("delete").unwrap();
        store.remove("delete").unwrap();
        assert_eq!(store.list(), vec![profile("keep")]);
        assert_eq!(
            ConnectionStore::load(&path).unwrap().list(),
            vec![profile("keep")]
        );
    }

    #[test]
    fn prometheus_urls_reject_userinfo_query_and_fragment() {
        let directory = tempfile::tempdir().unwrap();
        let store = ConnectionStore::load(directory.path().join("connections.json")).unwrap();
        for url in [
            "https://prom-secret@prometheus.internal",
            "https://user:prom-secret@prometheus.internal",
            "https://prometheus.internal?token=prom-secret",
            "https://prometheus.internal#prom-secret",
        ] {
            let mut candidate = profile("invalid");
            candidate.prometheus.as_mut().unwrap().url = url.to_string();
            let error = store.save(&candidate).unwrap_err().to_string();
            assert!(!error.contains("prom-secret"), "leaked URL in {error}");
        }
        assert!(store.list().is_empty());
    }

    #[derive(Default)]
    struct FakeKeychain {
        values: Mutex<HashMap<String, String>>,
        fail_get: AtomicBool,
        fail_set: AtomicBool,
        fail_delete: AtomicBool,
    }

    impl Keychain for FakeKeychain {
        fn get(&self, key: &str) -> Result<Option<String>, ()> {
            if self.fail_get.load(Ordering::Relaxed) {
                return Err(());
            }
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        fn set(&self, key: &str, value: &str) -> Result<(), ()> {
            if self.fail_set.load(Ordering::Relaxed) {
                return Err(());
            }
            self.values
                .lock()
                .unwrap()
                .insert(key.to_string(), value.to_string());
            Ok(())
        }

        fn delete(&self, key: &str) -> Result<(), ()> {
            if self.fail_delete.load(Ordering::Relaxed) {
                return Err(());
            }
            self.values.lock().unwrap().remove(key);
            Ok(())
        }
    }

    fn memory_store() -> SecretStore {
        SecretStore::with_keychain(None)
    }

    #[test]
    fn memory_secrets_remain_available_until_explicitly_cleared() {
        let store = memory_store();
        store
            .replace("connection", SecretKind::AdminToken, "admin-secret", false)
            .unwrap();

        let first = store.get("connection", SecretKind::AdminToken).unwrap();
        let second = store.get("connection", SecretKind::AdminToken).unwrap();
        assert_eq!(first.unwrap().secret, "admin-secret");
        assert_eq!(second.unwrap().source, SecretSource::Memory);

        store.clear("connection", SecretKind::AdminToken).unwrap();
        assert!(store
            .get("connection", SecretKind::AdminToken)
            .unwrap()
            .is_none());
    }

    #[test]
    fn unavailable_keychain_falls_back_to_memory_and_reports_its_source() {
        let store = memory_store();
        assert!(!store.keychain_available());
        let source = store
            .replace("connection", SecretKind::AdminToken, "admin-secret", true)
            .unwrap();
        assert_eq!(source, SecretSource::Memory);

        let stored = store
            .get("connection", SecretKind::AdminToken)
            .unwrap()
            .unwrap();
        assert_eq!(stored.secret, "admin-secret");
        assert_eq!(stored.source, SecretSource::Memory);
    }

    #[test]
    fn keychain_secrets_report_keychain_as_their_source() {
        let keychain = Arc::new(FakeKeychain::default());
        let store = SecretStore::with_keychain(Some(keychain));
        assert!(store.keychain_available());
        let source = store
            .replace(
                "connection",
                SecretKind::DiscoveryPassword,
                "discovery-secret",
                true,
            )
            .unwrap();
        assert_eq!(source, SecretSource::Keychain);
        let stored = store
            .get("connection", SecretKind::DiscoveryPassword)
            .unwrap()
            .unwrap();
        assert_eq!(stored.secret, "discovery-secret");
        assert_eq!(stored.source, SecretSource::Keychain);
    }

    #[test]
    fn secret_updates_apply_keep_clear_and_replace_independently() {
        let store = memory_store();
        store
            .replace("connection", SecretKind::AdminToken, "old-admin", false)
            .unwrap();
        store
            .replace(
                "connection",
                SecretKind::DiscoveryPassword,
                "old-discovery",
                false,
            )
            .unwrap();

        store
            .update(
                "connection",
                &ConnectionSecrets {
                    admin_token: SecretInput::Keep,
                    discovery_password: SecretInput::Replace("new-discovery".to_string()),
                },
                false,
            )
            .unwrap();
        assert_eq!(
            store
                .get("connection", SecretKind::AdminToken)
                .unwrap()
                .unwrap()
                .secret,
            "old-admin"
        );
        assert_eq!(
            store
                .get("connection", SecretKind::DiscoveryPassword)
                .unwrap()
                .unwrap()
                .secret,
            "new-discovery"
        );

        store
            .update(
                "connection",
                &ConnectionSecrets {
                    admin_token: SecretInput::Clear,
                    discovery_password: SecretInput::Keep,
                },
                false,
            )
            .unwrap();
        assert!(store
            .get("connection", SecretKind::AdminToken)
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .get("connection", SecretKind::DiscoveryPassword)
                .unwrap()
                .unwrap()
                .secret,
            "new-discovery"
        );
    }

    #[test]
    fn deleting_a_connection_clears_memory_and_keychain_secrets_only_for_it() {
        let keychain = Arc::new(FakeKeychain::default());
        let store = SecretStore::with_keychain(Some(keychain.clone()));
        store
            .replace("delete", SecretKind::AdminToken, "keychain-secret", true)
            .unwrap();
        store
            .replace(
                "delete",
                SecretKind::DiscoveryPassword,
                "memory-secret",
                false,
            )
            .unwrap();
        store
            .replace("keep", SecretKind::AdminToken, "keep-secret", true)
            .unwrap();

        store.remove("delete").unwrap();
        assert!(store
            .get("delete", SecretKind::AdminToken)
            .unwrap()
            .is_none());
        assert!(store
            .get("delete", SecretKind::DiscoveryPassword)
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .get("keep", SecretKind::AdminToken)
                .unwrap()
                .unwrap()
                .secret,
            "keep-secret"
        );
        assert_eq!(keychain.values.lock().unwrap().len(), 1);
    }

    #[test]
    fn secrets_do_not_appear_in_profile_json_debug_or_errors() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("connections.json");
        let profiles = ConnectionStore::load(&path).unwrap();
        profiles.save(&profile("connection")).unwrap();
        let secret = "never-disclose-this-value";
        let keychain = Arc::new(FakeKeychain::default());
        let store = SecretStore::with_keychain(Some(keychain.clone()));
        store
            .replace("connection", SecretKind::AdminToken, secret, true)
            .unwrap();

        assert!(!std::fs::read_to_string(path).unwrap().contains(secret));
        assert!(!format!("{store:?}").contains(secret));
        keychain.fail_delete.store(true, Ordering::Relaxed);
        let error = store
            .clear("connection", SecretKind::AdminToken)
            .unwrap_err()
            .to_string();
        assert!(!error.contains(secret), "secret leaked in {error}");
    }

    #[test]
    fn keychain_read_errors_are_sanitized() {
        let secret = "never-disclose-this-value";
        let keychain = Arc::new(FakeKeychain::default());
        keychain
            .values
            .lock()
            .unwrap()
            .insert("connection/admin_token".to_string(), secret.to_string());
        keychain.fail_get.store(true, Ordering::Relaxed);
        let store = SecretStore::with_keychain(Some(keychain));
        let error = store
            .get("connection", SecretKind::AdminToken)
            .unwrap_err()
            .to_string();
        assert!(!error.contains(secret), "secret leaked in {error}");
    }

    #[test]
    fn clear_and_remove_retry_persistent_cleanup_after_runtime_failures() {
        let keychain = Arc::new(FakeKeychain::default());
        let store = SecretStore::with_keychain(Some(keychain.clone()));
        store
            .replace("connection", SecretKind::AdminToken, "admin-secret", true)
            .unwrap();
        store
            .replace(
                "connection",
                SecretKind::DiscoveryPassword,
                "discovery-secret",
                true,
            )
            .unwrap();

        keychain.fail_get.store(true, Ordering::Relaxed);
        store.get("connection", SecretKind::AdminToken).unwrap_err();
        assert!(!store.keychain_available());
        keychain.fail_get.store(false, Ordering::Relaxed);
        keychain.fail_delete.store(true, Ordering::Relaxed);
        let error = store
            .clear("connection", SecretKind::AdminToken)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("admin-secret"));
        assert!(keychain
            .values
            .lock()
            .unwrap()
            .contains_key("connection/admin_token"));

        keychain.fail_delete.store(false, Ordering::Relaxed);
        store.clear("connection", SecretKind::AdminToken).unwrap();
        assert!(!keychain
            .values
            .lock()
            .unwrap()
            .contains_key("connection/admin_token"));

        keychain.fail_get.store(true, Ordering::Relaxed);
        store
            .get("connection", SecretKind::DiscoveryPassword)
            .unwrap_err();
        assert!(!store.keychain_available());
        store.remove("connection").unwrap();
        assert!(keychain.values.lock().unwrap().is_empty());
    }

    #[test]
    fn failed_persistent_replacement_is_an_error_and_retry_replaces_old_value() {
        let keychain = Arc::new(FakeKeychain::default());
        let store = SecretStore::with_keychain(Some(keychain.clone()));
        store
            .replace("connection", SecretKind::AdminToken, "old-secret", true)
            .unwrap();
        keychain.fail_set.store(true, Ordering::Relaxed);

        let error = store
            .replace("connection", SecretKind::AdminToken, "new-secret", true)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("new-secret"));
        assert_eq!(
            keychain
                .values
                .lock()
                .unwrap()
                .get("connection/admin_token"),
            Some(&"old-secret".to_string())
        );

        keychain.fail_set.store(false, Ordering::Relaxed);
        let source = store
            .replace("connection", SecretKind::AdminToken, "new-secret", true)
            .unwrap();
        assert_eq!(source, SecretSource::Keychain);
        let rebuilt = SecretStore::with_keychain(Some(keychain));
        assert_eq!(
            rebuilt
                .get("connection", SecretKind::AdminToken)
                .unwrap()
                .unwrap()
                .secret,
            "new-secret"
        );
    }

    #[test]
    fn session_only_replacement_removes_old_persistent_value_while_unavailable() {
        let keychain = Arc::new(FakeKeychain::default());
        let store = SecretStore::with_keychain(Some(keychain.clone()));
        store
            .replace("connection", SecretKind::AdminToken, "old-secret", true)
            .unwrap();
        keychain.fail_get.store(true, Ordering::Relaxed);
        store.get("connection", SecretKind::AdminToken).unwrap_err();
        assert!(!store.keychain_available());
        keychain.fail_get.store(false, Ordering::Relaxed);

        let source = store
            .replace("connection", SecretKind::AdminToken, "new-secret", false)
            .unwrap();
        assert_eq!(source, SecretSource::Memory);
        assert_eq!(
            store
                .get("connection", SecretKind::AdminToken)
                .unwrap()
                .unwrap()
                .secret,
            "new-secret"
        );
        let rebuilt = SecretStore::with_keychain(Some(keychain));
        assert!(rebuilt
            .get("connection", SecretKind::AdminToken)
            .unwrap()
            .is_none());
    }
}
