use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::time::{Duration, Instant, SystemTime};

pub(crate) const SCHEMA_VERSION: &str = "console.v1";

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
    pub cluster_id: String,
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
            cluster_id,
            generated_at: rfc3339(SystemTime::now()),
            partial,
            time_window: None,
            retention_source: "CONSOLE_MEMORY".to_string(),
            sources,
            capabilities,
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
