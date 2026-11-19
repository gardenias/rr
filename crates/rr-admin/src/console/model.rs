use super::config::SeedPeer;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

pub(crate) const SCHEMA_VERSION: &str = "console.v1";

pub(crate) type ConnectionId = String;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TlsProfile {
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub domain: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PrometheusProfile {
    pub url: String,
    pub cluster: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectionProfile {
    pub id: ConnectionId,
    pub name: String,
    pub environment: Option<String>,
    pub admin_seeds: Vec<SeedPeer>,
    pub discovery: Option<String>,
    pub cluster_name: Option<String>,
    pub admin_tls: TlsProfile,
    pub discovery_tls: TlsProfile,
    pub discovery_username: Option<String>,
    pub discovery_allow_insecure: bool,
    pub prometheus: Option<PrometheusProfile>,
    pub poll_interval_secs: u64,
    pub created_at: String,
    pub updated_at: String,
    pub observed_cluster_id: Option<String>,
    pub last_connected_at: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ConnectionSource {
    Profile,
    Startup,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ConnectionStatus {
    Connecting,
    Connected,
    Degraded,
    Unreachable,
    Disconnected,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ConnectionSummary {
    #[serde(flatten)]
    pub profile: ConnectionProfile,
    pub source: ConnectionSource,
    pub status: ConnectionStatus,
    pub credentials_configured: bool,
    pub credential_source: Option<String>,
    pub last_error: Option<String>,
    pub node_count: Option<u64>,
    pub group_count: Option<u64>,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) enum SecretInput {
    #[default]
    Keep,
    Clear,
    Replace(String),
}

impl<'de> Deserialize<'de> for SecretInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match Option::<String>::deserialize(deserializer)? {
            Some(secret) if secret.is_empty() => {
                return Err(serde::de::Error::custom("secret must not be empty"));
            }
            Some(secret) => Self::Replace(secret),
            None => Self::Clear,
        })
    }
}

impl std::fmt::Debug for SecretInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Keep => formatter.write_str("Keep"),
            Self::Clear => formatter.write_str("Clear"),
            Self::Replace(_) => formatter.write_str("Replace(REDACTED)"),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectionSecrets {
    #[serde(default)]
    pub admin_token: SecretInput,
    #[serde(default)]
    pub discovery_password: SecretInput,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Capability {
    pub feature: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl Capability {
    pub fn supported(feature: impl Into<String>) -> Self {
        Self {
            feature: feature.into(),
            status: "SUPPORTED".to_string(),
            reason: None,
        }
    }

    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            feature: feature.into(),
            status: "UNSUPPORTED".to_string(),
            reason: Some(reason.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SourceMeta {
    pub node_id: u64,
    pub endpoint_role: String,
    pub observed_at: Option<String>,
    pub age_ms: Option<u64>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ResponseMeta {
    pub schema_version: &'static str,
    pub correlation_id: String,
    pub cluster_id: Option<String>,
    pub generated_at: String,
    pub partial: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_window: Option<String>,
    pub retention_source: String,
    pub sources: Vec<SourceMeta>,
    pub capabilities: Vec<Capability>,
}

impl ResponseMeta {
    pub fn new(
        correlation_id: String,
        cluster_id: String,
        partial: bool,
        sources: Vec<SourceMeta>,
        capabilities: Vec<Capability>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            correlation_id,
            cluster_id: Some(cluster_id),
            generated_at: rfc3339(SystemTime::now()),
            partial,
            time_window: None,
            retention_source: "CONSOLE_MEMORY".to_string(),
            sources,
            capabilities,
        }
    }

    pub fn neutral(correlation_id: String) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            correlation_id,
            cluster_id: None,
            generated_at: rfc3339(SystemTime::now()),
            partial: false,
            time_window: None,
            retention_source: "NONE".to_string(),
            sources: Vec::new(),
            capabilities: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Envelope<T> {
    pub data: T,
    pub meta: ResponseMeta,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ErrorEnvelope {
    pub error: ErrorBody,
    pub meta: ResponseMeta,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GroupObservation {
    pub node_id: u64,
    pub group_id: u64,
    pub is_leader: bool,
    pub current_term: u64,
    pub last_log_index: u64,
    pub leader_node_id: Option<u64>,
    pub observed_at: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct EventView {
    pub id: String,
    pub severity: String,
    pub kind: String,
    pub node_id: Option<u64>,
    pub group_id: Option<u64>,
    pub first_observed_at: String,
    pub last_observed_at: String,
    pub state: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TimeSeriesPoint {
    pub timestamp: String,
    pub node_id: Option<u64>,
    pub metric: String,
    pub labels: BTreeMap<String, String>,
    pub value: Option<f64>,
    pub source: String,
}

#[derive(Clone)]
pub(crate) struct BoundedLru<K, V> {
    capacity: usize,
    ttl: Duration,
    entries: HashMap<K, BoundedLruEntry<V>>,
}

#[derive(Clone)]
struct BoundedLruEntry<V> {
    value: V,
    inserted_at: Instant,
    last_used: Instant,
}

impl<K: Eq + Hash + Clone, V> BoundedLru<K, V> {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            capacity: capacity.max(1),
            ttl,
            entries: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &K, now: Instant) -> Option<&V> {
        if self
            .entries
            .get(key)
            .is_some_and(|entry| now.duration_since(entry.inserted_at) >= self.ttl)
        {
            self.entries.remove(key);
            return None;
        }
        self.entries.get_mut(key).map(|entry| {
            entry.last_used = now;
            &entry.value
        })
    }

    pub fn get_cloned(&mut self, key: &K, now: Instant) -> Option<V>
    where
        V: Clone,
    {
        self.get(key, now).cloned()
    }

    pub fn find_cloned(
        &mut self,
        now: Instant,
        mut matches: impl FnMut(&K, &V) -> bool,
    ) -> Option<(K, V)>
    where
        V: Clone,
    {
        self.entries
            .retain(|_, entry| now.duration_since(entry.inserted_at) < self.ttl);
        self.entries
            .iter_mut()
            .find(|(key, entry)| matches(key, &entry.value))
            .map(|(key, entry)| {
                entry.last_used = now;
                (key.clone(), entry.value.clone())
            })
    }

    pub fn insert(&mut self, key: K, value: V, now: Instant) {
        self.entries
            .retain(|_, entry| now.duration_since(entry.inserted_at) < self.ttl);
        if !self.entries.contains_key(&key) && self.entries.len() >= self.capacity {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(
            key,
            BoundedLruEntry {
                value,
                inserted_at: now,
                last_used: now,
            },
        );
    }

    pub fn values_cloned(&self, now: Instant) -> Vec<V>
    where
        V: Clone,
    {
        self.entries
            .values()
            .filter(|entry| now.duration_since(entry.inserted_at) < self.ttl)
            .map(|entry| entry.value.clone())
            .collect()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

pub(crate) fn rfc3339(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(crate) fn age_ms(now: SystemTime, observed_at: SystemTime) -> u64 {
    now.duration_since(observed_at)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn connection_profile() -> ConnectionProfile {
        ConnectionProfile {
            id: "connection-1".to_string(),
            name: "Production".to_string(),
            environment: Some("prod".to_string()),
            admin_seeds: vec![SeedPeer {
                node_id: 1,
                admin_addr: "10.0.0.1:52001".to_string(),
            }],
            discovery: Some("etcd://10.0.0.1:2379".to_string()),
            cluster_name: Some("prod-east".to_string()),
            admin_tls: TlsProfile {
                ca: Some(PathBuf::from("/etc/rr/admin-ca.pem")),
                cert: None,
                key: None,
                domain: Some("admin.internal".to_string()),
            },
            discovery_tls: TlsProfile::default(),
            discovery_username: Some("console".to_string()),
            discovery_allow_insecure: false,
            prometheus: Some(PrometheusProfile {
                url: "https://prometheus.internal".to_string(),
                cluster: "cluster_id=prod-east".to_string(),
            }),
            poll_interval_secs: 5,
            created_at: "2026-11-18T00:00:00.000Z".to_string(),
            updated_at: "2026-11-18T00:00:00.000Z".to_string(),
            observed_cluster_id: Some("prod-east".to_string()),
            last_connected_at: None,
        }
    }

    #[test]
    fn connection_summary_is_serializable_and_contains_no_secret_fields() {
        let summary = ConnectionSummary {
            profile: connection_profile(),
            source: ConnectionSource::Profile,
            status: ConnectionStatus::Disconnected,
            credentials_configured: false,
            credential_source: None,
            last_error: None,
            node_count: None,
            group_count: None,
        };
        let value = serde_json::to_value(summary).unwrap();
        assert_eq!(value["id"], "connection-1");
        assert_eq!(value["status"], "DISCONNECTED");
        assert_eq!(value["source"], "PROFILE");
        assert_eq!(value["credentials_configured"], false);
        let serialized = value.to_string();
        for forbidden in [
            "admin_token",
            "discovery_password",
            "private_key",
            "viewer-secret",
            "etcd-secret",
        ] {
            assert!(!serialized.contains(forbidden), "serialized {forbidden}");
        }
    }

    #[test]
    fn secret_inputs_support_keep_clear_replace_without_debug_disclosure() {
        let omitted: ConnectionSecrets = serde_json::from_str("{}").unwrap();
        assert_eq!(omitted.admin_token, SecretInput::Keep);
        assert_eq!(omitted.discovery_password, SecretInput::Keep);

        let cleared: ConnectionSecrets =
            serde_json::from_str(r#"{"admin_token":null,"discovery_password":null}"#).unwrap();
        assert_eq!(cleared.admin_token, SecretInput::Clear);
        assert_eq!(cleared.discovery_password, SecretInput::Clear);

        let secret = "not-for-debug-or-api-responses";
        let replaced: ConnectionSecrets = serde_json::from_str(&format!(
            r#"{{"admin_token":"{secret}","discovery_password":"{secret}"}}"#
        ))
        .unwrap();
        assert_eq!(
            replaced.admin_token,
            SecretInput::Replace(secret.to_string())
        );
        assert!(!format!("{replaced:?}").contains(secret));
    }

    #[test]
    fn secret_inputs_reject_empty_replacements() {
        for input in [r#"{"admin_token":""}"#, r#"{"discovery_password":""}"#] {
            let error = serde_json::from_str::<ConnectionSecrets>(input)
                .unwrap_err()
                .to_string();
            assert!(error.contains("must not be empty"), "unexpected {error}");
        }
    }

    #[test]
    fn neutral_meta_has_no_cluster_identity_or_runtime_capabilities() {
        let value =
            serde_json::to_value(ResponseMeta::neutral("correlation-1".to_string())).unwrap();
        assert_eq!(value["schema_version"], "console.v1");
        assert_eq!(value["correlation_id"], "correlation-1");
        assert!(value["cluster_id"].is_null());
        assert_eq!(value["retention_source"], "NONE");
        assert_eq!(value["sources"], serde_json::json!([]));
        assert_eq!(value["capabilities"], serde_json::json!([]));
        assert_eq!(value["partial"], false);
        assert!(value["generated_at"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
    }

    #[test]
    fn openapi_declares_connection_contracts_and_neutral_meta() {
        let specification: serde_json::Value =
            serde_json::from_str(include_str!("../../openapi/console-v1.json")).unwrap();
        assert_eq!(
            specification["components"]["schemas"]["Meta"]["properties"]["cluster_id"]["nullable"],
            true
        );
        for schema in [
            "ConnectionProfile",
            "ConnectionSummary",
            "ConnectionStatus",
            "ConnectionSource",
            "TlsProfile",
            "PrometheusProfile",
            "ConnectionSecrets",
            "ConnectionCandidateInput",
        ] {
            assert!(
                specification["components"]["schemas"][schema].is_object(),
                "missing schema {schema}"
            );
        }
        for secret in ["admin_token", "discovery_password"] {
            assert_eq!(
                specification["components"]["schemas"]["ConnectionSecrets"]["properties"][secret]
                    ["writeOnly"],
                true
            );
            assert_eq!(
                specification["components"]["schemas"]["ConnectionSecrets"]["properties"][secret]
                    ["minLength"],
                1
            );
        }
        for path in [
            "/connections",
            "/connections/test",
            "/connections/{id}",
            "/connections/{id}/test",
            "/connections/{id}/connect",
            "/connections/{id}/disconnect",
            "/connections/{id}/summary",
            "/connections/{id}/nodes",
            "/connections/{id}/nodes/{nodeId}",
            "/connections/{id}/nodes/{nodeId}/boots",
            "/connections/{id}/topology",
            "/connections/{id}/groups",
            "/connections/{id}/groups/{groupId}",
            "/connections/{id}/groups/{groupId}/replicas",
            "/connections/{id}/metrics/performance",
            "/connections/{id}/snapshots",
            "/connections/{id}/lifecycle",
            "/connections/{id}/events",
            "/connections/{id}/sources",
            "/connections/{id}/sources/reconcile",
            "/connections/{id}/stream",
        ] {
            assert!(
                specification["paths"][path].is_object(),
                "missing path {path}"
            );
        }
        let codes = specification["components"]["schemas"]["ConnectionErrorCode"]["enum"]
            .as_array()
            .unwrap();
        for code in [
            "CONNECTION_NOT_FOUND",
            "CONNECTION_NOT_CONNECTED",
            "CONNECTION_ACTIVE",
            "CONNECTION_REQUIRED",
            "CREDENTIALS_REQUIRED",
            "ACTIVE_CONNECTION_LIMIT",
            "INVALID_CONNECTION_PROFILE",
            "CONNECTION_STORE_FAILED",
            "CONNECTION_INITIALIZATION_FAILED",
            "CONNECTION_TEST_FAILED",
            "CONNECTION_SHUTTING_DOWN",
        ] {
            assert!(codes.iter().any(|value| value == code), "missing {code}");
        }
        assert_eq!(
            specification["components"]["schemas"]["ConnectionErrorBody"]["properties"]["code"]
                ["$ref"],
            "#/components/schemas/ConnectionErrorCode"
        );
        for response in [
            "ConnectionNotFound",
            "ConnectionNotConnected",
            "ConnectionActive",
            "ConnectionRequired",
            "CredentialsRequired",
            "ActiveConnectionLimit",
            "ConnectionTestFailed",
            "ConnectionInitializationFailed",
            "ConnectionStoreFailed",
            "ConnectionShuttingDown",
        ] {
            assert_eq!(
                specification["components"]["responses"][response]["content"]["application/json"]
                    ["schema"]["$ref"],
                "#/components/schemas/NeutralConnectionErrorEnvelope",
                "{response} must use the connection-specific error body"
            );
        }
        for response in ["Unauthenticated", "Forbidden", "ControlBadRequest"] {
            assert_eq!(
                specification["components"]["responses"][response]["content"]["application/json"]
                    ["schema"]["$ref"],
                "#/components/schemas/NeutralErrorEnvelope",
                "{response} must retain generic authentication/control errors"
            );
        }

        for (path, method) in [
            ("/summary", "get"),
            ("/nodes", "get"),
            ("/nodes/{nodeId}", "get"),
            ("/nodes/{nodeId}/boots", "get"),
            ("/topology", "get"),
            ("/groups", "get"),
            ("/groups/{groupId}", "get"),
            ("/groups/{groupId}/replicas", "get"),
            ("/metrics/performance", "get"),
            ("/snapshots", "get"),
            ("/lifecycle", "get"),
            ("/events", "get"),
            ("/sources", "get"),
            ("/sources/reconcile", "post"),
            ("/stream", "get"),
        ] {
            assert_eq!(
                specification["paths"][path][method]["responses"]["400"]["$ref"],
                "#/components/responses/LegacyBadRequest",
                "legacy route {path} must allow runtime and neutral 400 envelopes"
            );
            assert_eq!(
                specification["paths"][path][method]["responses"]["409"]["$ref"],
                "#/components/responses/ConnectionNotConnected",
                "legacy route {path} must declare startup disconnect"
            );
            let not_found = if matches!(
                path,
                "/nodes/{nodeId}"
                    | "/nodes/{nodeId}/boots"
                    | "/groups/{groupId}"
                    | "/groups/{groupId}/replicas"
            ) {
                "ResourceOrConnectionNotFound"
            } else {
                "ConnectionNotFound"
            };
            assert_eq!(
                specification["paths"][path][method]["responses"]["404"]["$ref"],
                format!("#/components/responses/{not_found}"),
                "legacy route {path} must declare startup deletion"
            );
        }
        for response in ["LegacyBadRequest", "ResourceOrConnectionNotFound"] {
            let alternatives = specification["components"]["responses"][response]["content"]
                ["application/json"]["schema"]["anyOf"]
                .as_array()
                .unwrap();
            let neutral = if response == "LegacyBadRequest" {
                "NeutralErrorEnvelope"
            } else {
                "NeutralConnectionErrorEnvelope"
            };
            for schema in ["ErrorEnvelope", neutral] {
                assert!(alternatives.iter().any(|alternative| {
                    alternative["$ref"] == format!("#/components/schemas/{schema}")
                }));
            }
        }
        for path in [
            "/connections/{id}/nodes/{nodeId}",
            "/connections/{id}/nodes/{nodeId}/boots",
            "/connections/{id}/groups/{groupId}",
            "/connections/{id}/groups/{groupId}/replicas",
        ] {
            assert_eq!(
                specification["paths"][path]["get"]["responses"]["404"]["$ref"],
                "#/components/responses/ResourceOrConnectionNotFound"
            );
        }
        assert!(specification["components"]["schemas"]["NeutralErrorEnvelope"].is_object());
        assert!(
            specification["components"]["schemas"]["NeutralConnectionErrorEnvelope"].is_object()
        );
        for response in [
            "ControlBadRequest",
            "ControlUnprocessableEntity",
            "ControlPayloadTooLarge",
            "Unauthenticated",
            "Forbidden",
        ] {
            assert_eq!(
                specification["components"]["responses"][response]["content"]["application/json"]
                    ["schema"]["$ref"],
                "#/components/schemas/NeutralErrorEnvelope",
                "{response} must use neutral error metadata"
            );
        }
        assert_eq!(
            specification["paths"]["/connections/test"]["post"]["requestBody"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/ConnectionCandidateInput"
        );
        assert_eq!(
            specification["components"]["schemas"]["ConnectionCandidateInput"]["allOf"][1]
                ["properties"]["existing_connection_id"]["type"],
            "string"
        );
        assert_eq!(
            specification["paths"]["/connections/test"]["post"]["responses"]["404"]["$ref"],
            "#/components/responses/ConnectionNotFound"
        );
        assert_eq!(
            specification["paths"]["/connections/test"]["post"]["responses"]["400"]["$ref"],
            "#/components/responses/ControlBadRequest"
        );
        assert_eq!(
            specification["components"]["schemas"]["LoginResponse"]["properties"]["meta"]["$ref"],
            "#/components/schemas/NeutralMeta"
        );
        assert_eq!(
            specification["components"]["schemas"]["ConnectionInput"]["allOf"][0]["properties"]
                ["remember"]["default"],
            false
        );
        assert!(
            specification["components"]["schemas"]["ConnectionInput"]["allOf"][0]["required"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field == "name")
        );
        for (path, method) in [
            ("/connections", "post"),
            ("/connections/test", "post"),
            ("/connections/{id}", "patch"),
            ("/auth/login", "post"),
        ] {
            assert_eq!(
                specification["paths"][path][method]["responses"]["422"]["$ref"],
                "#/components/responses/ControlUnprocessableEntity"
            );
            assert_eq!(
                specification["paths"][path][method]["responses"]["413"]["$ref"],
                "#/components/responses/ControlPayloadTooLarge"
            );
        }
        assert_eq!(
            specification["components"]["parameters"]["Origin"]["required"],
            true
        );
        for (path, method) in [
            ("/connections", "post"),
            ("/connections/test", "post"),
            ("/connections/{id}", "patch"),
            ("/connections/{id}", "delete"),
            ("/connections/{id}/test", "post"),
            ("/connections/{id}/connect", "post"),
            ("/connections/{id}/disconnect", "post"),
            ("/connections/{id}/sources/reconcile", "post"),
            ("/sources/reconcile", "post"),
            ("/auth/logout", "post"),
        ] {
            let parameters = specification["paths"][path][method]["parameters"]
                .as_array()
                .unwrap();
            assert!(
                parameters
                    .iter()
                    .any(|parameter| { parameter["$ref"] == "#/components/parameters/Origin" }),
                "{method} {path} must require Origin"
            );
        }
        for path in ["/connections/test", "/connections/{id}/test"] {
            assert_eq!(
                specification["paths"][path]["post"]["responses"]["502"]["$ref"],
                "#/components/responses/ConnectionTestFailed"
            );
        }
        assert_eq!(
            specification["paths"]["/connections/{id}/connect"]["post"]["responses"]["502"]["$ref"],
            "#/components/responses/ConnectionInitializationFailed"
        );
        assert_eq!(
            specification["paths"]["/connections/{id}/connect"]["post"]["responses"]["503"]["$ref"],
            "#/components/responses/ConnectionShuttingDown"
        );
        for (path, method) in [
            ("/connections", "get"),
            ("/connections", "post"),
            ("/connections/{id}", "get"),
            ("/connections/{id}", "patch"),
            ("/connections/{id}", "delete"),
        ] {
            assert_eq!(
                specification["paths"][path][method]["responses"]["500"]["$ref"],
                "#/components/responses/ConnectionStoreFailed",
                "{method} {path} must expose store failures"
            );
        }
        for (path, method) in [
            ("/connections/{id}/test", "post"),
            ("/connections/{id}/connect", "post"),
            ("/connections/{id}/disconnect", "post"),
        ] {
            assert_eq!(
                specification["paths"][path][method]["responses"]["500"]["$ref"],
                "#/components/responses/ConnectionStoreFailed"
            );
        }
        assert!(
            specification["components"]["schemas"]["ConnectionSummary"]["allOf"][1]["properties"]
                ["credential_source"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "MIXED")
        );
    }

    #[test]
    fn mixed_error_responses_accept_neutral_and_runtime_payloads() {
        fn resolve<'a>(specification: &'a Value, reference: &str) -> &'a Value {
            reference
                .strip_prefix("#/")
                .unwrap()
                .split('/')
                .fold(specification, |value, segment| &value[segment])
        }

        fn matches_meta(specification: &Value, reference: &str, sample: &Value) -> bool {
            let schema = resolve(specification, reference);
            let base = if reference.ends_with("/NeutralMeta") {
                resolve(specification, schema["allOf"][0]["$ref"].as_str().unwrap())
            } else {
                schema
            };
            if !base["required"].as_array().is_some_and(|required| {
                required.iter().all(|field| {
                    sample
                        .as_object()
                        .is_some_and(|object| object.contains_key(field.as_str().unwrap()))
                })
            }) {
                return false;
            }
            if !sample["schema_version"].is_string()
                || !sample["correlation_id"].is_string()
                || !sample["generated_at"].is_string()
                || !sample["partial"].is_boolean()
                || !(sample["cluster_id"].is_null() || sample["cluster_id"].is_string())
                || !sample["sources"].is_array()
                || !sample["capabilities"].is_array()
            {
                return false;
            }
            if !base["properties"]["retention_source"]["enum"]
                .as_array()
                .is_some_and(|allowed| allowed.contains(&sample["retention_source"]))
            {
                return false;
            }
            if reference.ends_with("/NeutralMeta") {
                let overlay = &schema["allOf"][1]["properties"];
                if !sample["cluster_id"].is_null()
                    || sample["retention_source"] != "NONE"
                    || overlay["sources"]["maxItems"].as_u64() != Some(0)
                    || overlay["capabilities"]["maxItems"].as_u64() != Some(0)
                    || !sample["sources"].as_array().unwrap().is_empty()
                    || !sample["capabilities"].as_array().unwrap().is_empty()
                {
                    return false;
                }
            }
            true
        }

        fn matches_envelope(specification: &Value, reference: &str, sample: &Value) -> bool {
            let schema = resolve(specification, reference);
            if !schema["required"].as_array().is_some_and(|required| {
                required.iter().all(|field| {
                    sample
                        .as_object()
                        .is_some_and(|object| object.contains_key(field.as_str().unwrap()))
                })
            }) || !sample["error"]["code"].is_string()
                || !sample["error"]["message"].is_string()
            {
                return false;
            }
            let meta = schema["properties"]["meta"]["$ref"].as_str().unwrap();
            matches_meta(specification, meta, &sample["meta"])
        }

        let neutral = serde_json::to_value(ErrorEnvelope {
            error: ErrorBody {
                code: "CONNECTION_REQUIRED".to_string(),
                message: "Connection required".to_string(),
            },
            meta: ResponseMeta::neutral("neutral-correlation".to_string()),
        })
        .unwrap();
        let runtime = serde_json::to_value(ErrorEnvelope {
            error: ErrorBody {
                code: "NOT_FOUND".to_string(),
                message: "Node not found".to_string(),
            },
            meta: ResponseMeta::new(
                "runtime-correlation".to_string(),
                "runtime-cluster".to_string(),
                false,
                Vec::new(),
                Vec::new(),
            ),
        })
        .unwrap();
        let specification: Value =
            serde_json::from_str(include_str!("../../openapi/console-v1.json")).unwrap();
        for response in ["LegacyBadRequest", "ResourceOrConnectionNotFound"] {
            let schema = &specification["components"]["responses"][response]["content"]
                ["application/json"]["schema"];
            let alternatives = schema["anyOf"]
                .as_array()
                .or_else(|| schema["oneOf"].as_array())
                .unwrap();
            for sample in [&neutral, &runtime] {
                let matches = alternatives
                    .iter()
                    .filter(|alternative| {
                        matches_envelope(
                            &specification,
                            alternative["$ref"].as_str().unwrap(),
                            sample,
                        )
                    })
                    .count();
                if schema.get("oneOf").is_some() {
                    assert_eq!(
                        matches, 1,
                        "{response} oneOf must accept exactly one branch; sample {sample} matched {matches}"
                    );
                } else {
                    assert!(
                        matches >= 1,
                        "{response} anyOf rejected actual response sample {sample}"
                    );
                }
            }
            assert!(schema.get("oneOf").is_none(), "{response} still uses oneOf");
            assert!(schema["anyOf"].is_array(), "{response} must use anyOf");
        }
    }

    #[test]
    fn bounded_lru_evicts_expired_and_least_recently_used_results() {
        let mut cache = BoundedLru::new(2, Duration::from_secs(5));
        let now = std::time::Instant::now();
        cache.insert("a", Ok::<_, &'static str>(1), now);
        cache.insert("b", Err("missing"), now);
        assert_eq!(cache.get(&"a", now + Duration::from_secs(1)), Some(&Ok(1)));
        cache.insert("c", Ok(3), now + Duration::from_secs(2));
        assert_eq!(cache.get(&"b", now + Duration::from_secs(2)), None);
        assert_eq!(cache.get(&"c", now + Duration::from_secs(2)), Some(&Ok(3)));
        assert_eq!(
            cache.get(&"a", now + Duration::from_secs(7)),
            None,
            "TTL applies to successful and failed entries"
        );
        assert!(cache.len() <= 2);
    }
}
