use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, ConnectOptions, Txn, TxnOp};
use rr_core::{CasOutcome, ClusterLifecycle, LifecycleError, LifecycleStore, VersionedLifecycle};
use serde_json::Value;

#[derive(Clone)]
pub struct EtcdLifecycleStore {
    client: Client,
    cluster_name: String,
}

impl std::fmt::Debug for EtcdLifecycleStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EtcdLifecycleStore")
            .field("cluster_name", &self.cluster_name)
            .finish_non_exhaustive()
    }
}

impl EtcdLifecycleStore {
    pub async fn new(endpoints: Vec<String>, cluster_name: String) -> Result<Self, LifecycleError> {
        Self::connect_with_options(endpoints, cluster_name, None).await
    }

    pub async fn new_with_options(
        endpoints: Vec<String>,
        cluster_name: String,
        options: ConnectOptions,
    ) -> Result<Self, LifecycleError> {
        Self::connect_with_options(endpoints, cluster_name, Some(options)).await
    }

    async fn connect_with_options(
        endpoints: Vec<String>,
        cluster_name: String,
        options: Option<ConnectOptions>,
    ) -> Result<Self, LifecycleError> {
        if endpoints.is_empty() {
            return Err(LifecycleError::CorruptData(
                "etcd endpoints cannot be empty".to_string(),
            ));
        }
        let cluster_name = crate::validate_cluster_id(&cluster_name)
            .map_err(|error| LifecycleError::CorruptData(error.to_string()))?;
        let mut client = Client::connect(endpoints, options)
            .await
            .map_err(|error| map_etcd_error("connect lifecycle store", error))?;
        client
            .status()
            .await
            .map_err(|error| map_etcd_error("check lifecycle store status", error))?;
        Ok(Self {
            client,
            cluster_name,
        })
    }

    fn key(&self) -> String {
        formation_key(&self.cluster_name)
    }
}

fn formation_key(cluster_name: &str) -> String {
    format!("/rr/{cluster_name}/lifecycle/formation")
}

fn map_etcd_error(context: &str, error: etcd_client::Error) -> LifecycleError {
    LifecycleError::Unavailable(format!("{context}: {error}"))
}

fn encode_lifecycle_record(record: &ClusterLifecycle) -> Result<Vec<u8>, LifecycleError> {
    record.validate()?;
    serde_json::to_vec(record)
        .map_err(|error| LifecycleError::CorruptData(format!("encode lifecycle record: {error}")))
}

fn reject_unknown_fields(
    value: &Value,
    allowed: &[&str],
    context: &str,
) -> Result<(), LifecycleError> {
    let object = value
        .as_object()
        .ok_or_else(|| LifecycleError::CorruptData(format!("{context} must be a JSON object")))?;
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(LifecycleError::CorruptData(format!(
            "unknown field {field:?} in {context}"
        )));
    }
    Ok(())
}

fn decode_lifecycle_record(bytes: &[u8]) -> Result<ClusterLifecycle, LifecycleError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| LifecycleError::CorruptData(format!("decode lifecycle JSON: {error}")))?;
    reject_unknown_fields(
        &value,
        &[
            "version",
            "cluster_uuid",
            "topology_revision",
            "seed_hash",
            "origin",
            "groups",
        ],
        "lifecycle record",
    )?;
    let groups = value
        .get("groups")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            LifecycleError::CorruptData("lifecycle groups must be a JSON object".to_string())
        })?;
    for formation in groups.values() {
        reject_unknown_fields(formation, &["bootstrap_epoch", "state"], "group formation")?;
    }
    let record: ClusterLifecycle = serde_json::from_value(value).map_err(|error| {
        LifecycleError::CorruptData(format!("decode lifecycle record: {error}"))
    })?;
    record.validate()?;
    Ok(record)
}

fn revision_from_etcd(revision: i64) -> Result<u64, LifecycleError> {
    u64::try_from(revision).map_err(|_| {
        LifecycleError::CorruptData(format!("negative etcd lifecycle revision {revision}"))
    })
}

fn revision_to_etcd(revision: u64) -> Result<i64, LifecycleError> {
    i64::try_from(revision).map_err(|_| {
        LifecycleError::CorruptData(format!("lifecycle revision {revision} exceeds etcd i64"))
    })
}

#[async_trait]
impl LifecycleStore for EtcdLifecycleStore {
    async fn load_linearizable(&self) -> Result<Option<VersionedLifecycle>, LifecycleError> {
        let mut client = self.client.clone();
        let response = client
            .get(self.key(), None)
            .await
            .map_err(|error| map_etcd_error("load lifecycle record", error))?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        Ok(Some(VersionedLifecycle {
            revision: revision_from_etcd(kv.mod_revision())?,
            record: decode_lifecycle_record(kv.value())?,
        }))
    }

    async fn create_if_absent(
        &self,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError> {
        let key = self.key();
        let encoded = encode_lifecycle_record(&record)?;
        let transaction = Txn::new()
            .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(key, encoded, None)]);
        let response = self
            .client
            .clone()
            .txn(transaction)
            .await
            .map_err(|error| map_etcd_error("create lifecycle record", error))?;
        Ok(if response.succeeded() {
            CasOutcome::Applied
        } else {
            CasOutcome::Conflict
        })
    }

    async fn compare_and_swap(
        &self,
        expected_revision: u64,
        record: ClusterLifecycle,
    ) -> Result<CasOutcome, LifecycleError> {
        let expected = revision_to_etcd(expected_revision)?;
        let Some(current) = self.load_linearizable().await? else {
            return Ok(CasOutcome::Conflict);
        };
        if current.revision != expected_revision {
            return Ok(CasOutcome::Conflict);
        }
        current.record.validate_transition(&record)?;
        let key = self.key();
        let encoded = encode_lifecycle_record(&record)?;
        let transaction = Txn::new()
            .when([Compare::mod_revision(
                key.clone(),
                CompareOp::Equal,
                expected,
            )])
            .and_then([TxnOp::put(key, encoded, None)]);
        let response = self
            .client
            .clone()
            .txn(transaction)
            .await
            .map_err(|error| map_etcd_error("update lifecycle record", error))?;
        Ok(if response.succeeded() {
            CasOutcome::Applied
        } else {
            CasOutcome::Conflict
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rr_core::{
        ClusterLifecycle, ClusterOrigin, FormationState, GroupFormation, LifecycleError,
    };

    use super::{decode_lifecycle_record, formation_key, revision_from_etcd, EtcdLifecycleStore};

    fn forming() -> ClusterLifecycle {
        ClusterLifecycle {
            version: 1,
            cluster_uuid: "12345678-1234-1234-1234-123456789abc".parse().unwrap(),
            topology_revision: 1,
            seed_hash: [7; 32],
            origin: ClusterOrigin::Genesis,
            groups: BTreeMap::from([(
                1,
                GroupFormation {
                    bootstrap_epoch: 1,
                    state: FormationState::Forming,
                },
            )]),
        }
    }

    #[test]
    fn lifecycle_decode_accepts_valid_record_and_uses_formation_key() {
        let record = forming();
        let encoded = serde_json::to_vec(&record).unwrap();
        assert_eq!(decode_lifecycle_record(&encoded).unwrap(), record);
        assert_eq!(
            formation_key("cluster-a"),
            "/rr/cluster-a/lifecycle/formation"
        );
    }

    #[test]
    fn lifecycle_decode_rejects_malformed_and_unknown_fields() {
        assert!(matches!(
            decode_lifecycle_record(b"{not-json"),
            Err(LifecycleError::CorruptData(_))
        ));

        let mut value = serde_json::to_value(forming()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unexpected".into(), serde_json::Value::Null);
        assert!(matches!(
            decode_lifecycle_record(&serde_json::to_vec(&value).unwrap()),
            Err(LifecycleError::CorruptData(_))
        ));

        let mut value = serde_json::to_value(forming()).unwrap();
        value["groups"]["1"]
            .as_object_mut()
            .unwrap()
            .insert("unexpected".into(), serde_json::Value::Null);
        assert!(matches!(
            decode_lifecycle_record(&serde_json::to_vec(&value).unwrap()),
            Err(LifecycleError::CorruptData(_))
        ));
    }

    #[test]
    fn lifecycle_decode_enforces_schema_and_semantic_validation() {
        let mut unsupported = forming();
        unsupported.version = 2;
        assert!(matches!(
            decode_lifecycle_record(&serde_json::to_vec(&unsupported).unwrap()),
            Err(LifecycleError::UnsupportedSchema { .. })
        ));

        let mut empty_groups = forming();
        empty_groups.groups.clear();
        assert!(matches!(
            decode_lifecycle_record(&serde_json::to_vec(&empty_groups).unwrap()),
            Err(LifecycleError::CorruptData(_))
        ));
    }

    #[test]
    fn lifecycle_etcd_revision_conversion_is_checked() {
        assert_eq!(revision_from_etcd(41).unwrap(), 41);
        assert!(matches!(
            revision_from_etcd(-1),
            Err(LifecycleError::CorruptData(_))
        ));
        let error = super::revision_to_etcd(u64::MAX).unwrap_err();
        assert!(matches!(error, LifecycleError::CorruptData(_)));
        assert!(error.to_string().contains("exceeds etcd i64"));
    }

    #[tokio::test]
    async fn lifecycle_unavailable_backend_is_error_not_empty_record() {
        let client = etcd_client::Client::connect(
            ["http://127.0.0.1:1"],
            Some(
                etcd_client::ConnectOptions::new()
                    .with_timeout(std::time::Duration::from_millis(100)),
            ),
        )
        .await
        .unwrap();
        let store = EtcdLifecycleStore {
            client,
            cluster_name: "cluster-a".to_string(),
        };

        let error = rr_core::LifecycleStore::load_linearizable(&store)
            .await
            .unwrap_err();
        assert!(matches!(error, LifecycleError::Unavailable(_)));
    }
}
