//! Framework-level idempotency layer used by `ServiceStateMachineAdapter`.
//!
//! Implements the runtime side of [ADR
//! A-0003](https://github.com/gardenias/rr/blob/main/.sisyphus/adr/A-0003-framework-idempotency-layer.md):
//! the type wires live in `rr_core::idempotency`, the per-Raft-group
//! mutable state lives here.
//!
//! # Algorithm
//!
//! For each `(request_type, request_id)` the layer tracks two pieces of
//! state:
//!
//! - **`watermarks[request_type] = u64`** — the largest `request_id`
//!   ever applied for that namespace. Monotonically nondecreasing.
//! - **`cache: LruCache<(request_type, request_id), (client_id,
//!   response)>`** — bounded LRU of recently-applied responses, keyed
//!   on the dedup identity (NOT on `IdempotencyKey` directly, so two
//!   different `client_id`s reusing the same `(type, id)` pair land
//!   in the same slot for collision detection).
//!
//! On `decide(key)`:
//!
//! 1. **Cache hit + same client_id** → [`IdempotencyDecision::Replay`]
//!    (return cached response, do NOT call user service).
//! 2. **Cache hit + different client_id** →
//!    [`IdempotencyDecision::Collision`] (still return cached response;
//!    audit log records the ambiguity for forensics).
//! 3. **Cache miss + watermark exists + `request_id <= watermark`** →
//!    [`IdempotencyDecision::Expired`] (the entry was applied AND
//!    evicted; replaying is unsafe, client must pick a fresh ID).
//! 4. **Cache miss + (no watermark OR `request_id > watermark`)** →
//!    [`IdempotencyDecision::New`] (proceed; caller MUST follow up
//!    with `record(key, response)` once the service produces output).
//!
//! Watermark presence — the existence of a `watermarks[type]` entry —
//! distinguishes "never applied" from "applied id 0 then forgotten",
//! so Kafka-style streams whose first offset is 0 don't have their
//! initial entry rejected as Expired.
//!
//! Watermark advancement happens in `record`, not `decide`, so out-of-order
//! ID submissions don't accidentally Expire valid IDs.
//!
//! The watermark map is capped at [`MAX_IDEMPO_NAMESPACES`] distinct
//! `request_type` namespaces: it is never evicted and is captured into every
//! snapshot, and `request_type` is client-controlled, so unbounded growth
//! would be a memory/snapshot-size amplifier. A NEW namespace beyond the cap
//! fails loudly with [`RrError::IdempotencyNamespaceLimitExceeded`].
//!
//! # Persistence
//!
//! [`capture`](IdempotencyLayer::capture) / [`restore`](IdempotencyLayer::restore)
//! serialize the layer into / from
//! [`rr_core::framed_snapshot::IdempotencyState`]. The state machine
//! adapter calls these inside its snapshot/restore path so the layer
//! survives leader changes and node restarts.

use std::collections::HashMap;
use std::num::NonZeroUsize;

use lru::LruCache;
use serde::{Deserialize, Serialize};

use rr_core::error::{CodecError, RrError};
use rr_core::framed_snapshot::IdempotencyState;
use rr_core::idempotency::{IdempotencyDecision, IdempotencyKey};

/// Default LRU cache capacity (10 000 entries). Configurable via
/// `NodeConfig::idempotency_cache_size`. Tunable based on:
///
/// - retry frequency (replay window must outlive the longest expected
///   client retry interval),
/// - per-entry response size (cache memory ≈ capacity × avg response
///   bytes).
pub const DEFAULT_IDEMPOTENCY_CACHE_SIZE: usize = 10_000;

/// Hard cap on the number of distinct `request_type` namespaces one group's
/// watermark map will track. `request_type` is client-controlled and
/// watermark entries are NEVER evicted — and are captured into every
/// snapshot — so without a cap a client could grow per-group memory and
/// snapshot size without bound. A NEW namespace beyond the cap fails loudly
/// with [`RrError::IdempotencyNamespaceLimitExceeded`]; already-tracked
/// namespaces are unaffected.
pub const MAX_IDEMPO_NAMESPACES: usize = 1024;

/// Cache key: `(request_type, request_id)`. Deliberately excludes
/// `client_id` so multiple `client_id`s reusing the same identity hit
/// the same cache slot (enabling collision detection).
type CacheKey = (String, u64);

/// Cache value: `(recorded client_id, response bytes)`. The recorded
/// `client_id` is what the *original* applier supplied; comparing it
/// against the incoming key's `client_id` is how we detect collisions.
type CacheValue = (Option<String>, Vec<u8>);

/// Per-Raft-group idempotency state.
///
/// Constructed by [`ServiceStateMachineAdapter`](crate::service_adapter::ServiceStateMachineAdapter)
/// when the framework's idempotency feature is on. Not intended for
/// direct user use; the public surface is the [`IdempotencyDecision`]
/// returned by [`decide`](Self::decide).
pub struct IdempotencyLayer {
    watermarks: HashMap<String, u64>,
    cache: LruCache<CacheKey, CacheValue>,
}

impl IdempotencyLayer {
    /// Create an empty layer with the given cache capacity. A capacity
    /// of zero is treated as one (LruCache requires NonZero); supply
    /// [`DEFAULT_IDEMPOTENCY_CACHE_SIZE`] for typical deployments.
    pub fn new(cache_capacity: usize) -> Self {
        let cap = NonZeroUsize::new(cache_capacity.max(1)).unwrap();
        Self {
            watermarks: HashMap::new(),
            cache: LruCache::new(cap),
        }
    }

    /// Decide what to do with an incoming keyed request.
    ///
    /// `&mut self` because LRU cache lookups bump recency. See module
    /// docs for the four-way dispatch logic.
    ///
    /// Fails with [`RrError::IdempotencyNamespaceLimitExceeded`] when the
    /// key would open a NEW `request_type` namespace beyond
    /// [`MAX_IDEMPO_NAMESPACES`] — the check runs here, before the caller
    /// invokes the user service, so a rejected request never produces
    /// side effects.
    pub fn decide(&mut self, key: &IdempotencyKey) -> Result<IdempotencyDecision, RrError> {
        let cache_key = (key.request_type.clone(), key.request_id);
        if let Some((cached_client_id, response)) = self.cache.get(&cache_key) {
            if cached_client_id != &key.client_id {
                return Ok(IdempotencyDecision::Collision {
                    cached_client_id: cached_client_id.clone(),
                    incoming_client_id: key.client_id.clone(),
                    cached_response: response.clone(),
                });
            }
            return Ok(IdempotencyDecision::Replay(response.clone()));
        }

        if let Some(watermark) = self.watermarks.get(&key.request_type).copied() {
            if key.request_id <= watermark {
                return Ok(IdempotencyDecision::Expired {
                    request_type: key.request_type.clone(),
                    request_id: key.request_id,
                    watermark,
                });
            }
        }

        self.check_namespace_allowed(&key.request_type)?;
        Ok(IdempotencyDecision::New)
    }

    /// Guard shared by [`decide`](Self::decide) and [`record`](Self::record):
    /// a brand-new namespace is accepted only below [`MAX_IDEMPO_NAMESPACES`].
    fn check_namespace_allowed(&self, request_type: &str) -> Result<(), RrError> {
        if !self.watermarks.contains_key(request_type)
            && self.watermarks.len() >= MAX_IDEMPO_NAMESPACES
        {
            return Err(RrError::IdempotencyNamespaceLimitExceeded {
                request_type: request_type.to_string(),
                namespaces: self.watermarks.len(),
                max: MAX_IDEMPO_NAMESPACES,
            });
        }
        Ok(())
    }

    /// Record a response after the user service has produced it. Updates
    /// the watermark to `max(current, key.request_id)` and inserts the
    /// response into the LRU cache.
    ///
    /// Fails with [`RrError::IdempotencyNamespaceLimitExceeded`] when the
    /// key belongs to a NEW namespace and the cap is reached — the same
    /// guard as [`decide`](Self::decide), repeated here so direct callers
    /// cannot bypass the cap.
    pub fn record(&mut self, key: &IdempotencyKey, response: Vec<u8>) -> Result<(), RrError> {
        self.check_namespace_allowed(&key.request_type)?;
        let entry = self.watermarks.entry(key.request_type.clone()).or_insert(0);
        if key.request_id > *entry {
            *entry = key.request_id;
        }
        let cache_key = (key.request_type.clone(), key.request_id);
        self.cache.put(cache_key, (key.client_id.clone(), response));
        Ok(())
    }

    /// Capture the current layer state into the framework-snapshot
    /// envelope's `IdempotencyState` slot. Caller writes the result into
    /// `FrameworkState::idempotency`.
    pub fn capture(&self) -> Result<IdempotencyState, RrError> {
        let internal = IdempotencyStateInternal {
            watermarks: self.watermarks.clone(),
            cache: self
                .cache
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        let bytes = bincode::serde::encode_to_vec(&internal, bincode::config::standard())
            .map_err(|e| RrError::Codec(CodecError::SerializeFailed(e.to_string())))?;
        Ok(IdempotencyState { data: bytes })
    }

    /// Hydrate this layer from the framework-snapshot envelope's
    /// `IdempotencyState` slot. After this call:
    ///
    /// - `watermarks` are wholly replaced with the captured values,
    /// - cache is wholly replaced too: any pre-existing entries in
    ///   `self.cache` are dropped so the post-restore state matches
    ///   the snapshot exactly. (Earlier versions merged, which made
    ///   double-restore non-idempotent and produced cache contents
    ///   that no single snapshot could explain — the replace
    ///   semantic is what operators expect.)
    ///
    /// An empty `state.data` is treated as "no captured state" and
    /// leaves the layer untouched.
    pub fn restore(&mut self, state: IdempotencyState) -> Result<(), RrError> {
        if state.data.is_empty() {
            return Ok(());
        }
        let (decoded, _): (IdempotencyStateInternal, _) =
            bincode::serde::decode_from_slice(&state.data, bincode::config::standard()).map_err(
                |e| {
                    RrError::Codec(CodecError::DeserializeFailed(format!(
                        "idempotency state decode failed: {e}"
                    )))
                },
            )?;
        self.watermarks = decoded.watermarks;
        let cap = self.cache.cap();
        self.cache = LruCache::new(cap);
        for (k, v) in decoded.cache {
            self.cache.put(k, v);
        }
        Ok(())
    }

    /// Number of entries currently in the LRU cache. Exposed for the
    /// `rr_idempotency_cache_size` gauge metric.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Whether the layer has never been touched — both watermarks and
    /// cache are empty. Used by the snapshot path to skip populating
    /// `FrameworkState::idempotency` when no idempotent traffic has
    /// reached the group, keeping snapshots minimal.
    pub fn is_empty(&self) -> bool {
        self.watermarks.is_empty() && self.cache.len() == 0
    }

    /// Test-only inspection helper.
    #[cfg(test)]
    pub fn watermark_for(&self, request_type: &str) -> Option<u64> {
        self.watermarks.get(request_type).copied()
    }
}

#[derive(Serialize, Deserialize)]
struct IdempotencyStateInternal {
    watermarks: HashMap<String, u64>,
    cache: Vec<(CacheKey, CacheValue)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(request_type: &str, request_id: u64) -> IdempotencyKey {
        IdempotencyKey::new(request_type, request_id)
    }

    fn key_with_client(request_type: &str, request_id: u64, client_id: &str) -> IdempotencyKey {
        IdempotencyKey::with_client_id(request_type, request_id, client_id)
    }

    #[test]
    fn fresh_key_decides_new() {
        let mut layer = IdempotencyLayer::new(10);
        assert!(matches!(
            layer.decide(&key("transfer", 1)).unwrap(),
            IdempotencyDecision::New
        ));
    }

    // INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    #[test]
    fn recorded_key_replays() {
        let mut layer = IdempotencyLayer::new(10);
        let k = key("transfer", 1);
        layer.record(&k, b"response-bytes".to_vec()).unwrap();
        match layer.decide(&k).unwrap() {
            IdempotencyDecision::Replay(bytes) => assert_eq!(bytes, b"response-bytes"),
            other => panic!("expected Replay, got {other:?}"),
        }
    }

    #[test]
    fn collision_detected_when_client_id_differs() {
        let mut layer = IdempotencyLayer::new(10);
        let original = key_with_client("transfer", 1, "alice");
        layer.record(&original, b"alice-response".to_vec()).unwrap();
        let retry = key_with_client("transfer", 1, "bob");
        match layer.decide(&retry).unwrap() {
            IdempotencyDecision::Collision {
                cached_client_id,
                incoming_client_id,
                cached_response,
            } => {
                assert_eq!(cached_client_id, Some("alice".into()));
                assert_eq!(incoming_client_id, Some("bob".into()));
                assert_eq!(cached_response, b"alice-response");
            }
            other => panic!("expected Collision, got {other:?}"),
        }
    }

    // INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    #[test]
    fn expired_when_below_watermark_and_evicted() {
        let mut layer = IdempotencyLayer::new(2);
        layer.record(&key("transfer", 1), vec![1]).unwrap();
        layer.record(&key("transfer", 2), vec![2]).unwrap();
        layer.record(&key("transfer", 3), vec![3]).unwrap();
        match layer.decide(&key("transfer", 1)).unwrap() {
            IdempotencyDecision::Expired {
                request_type,
                request_id,
                watermark,
            } => {
                assert_eq!(request_type, "transfer");
                assert_eq!(request_id, 1);
                assert_eq!(watermark, 3);
            }
            other => panic!("expected Expired, got {other:?}"),
        }
    }

    #[test]
    fn out_of_order_record_below_watermark_is_still_cached() {
        let mut layer = IdempotencyLayer::new(10);
        layer
            .record(&key("transfer", 10), b"resp10".to_vec())
            .unwrap();
        layer
            .record(&key("transfer", 5), b"resp5".to_vec())
            .unwrap();
        assert_eq!(layer.watermark_for("transfer"), Some(10));
        match layer.decide(&key("transfer", 5)).unwrap() {
            IdempotencyDecision::Replay(bytes) => assert_eq!(bytes, b"resp5"),
            other => panic!("expected Replay (still cached), got {other:?}"),
        }
    }

    #[test]
    fn distinct_request_types_have_independent_watermarks() {
        let mut layer = IdempotencyLayer::new(10);
        layer.record(&key("transfer", 100), vec![]).unwrap();
        assert_eq!(layer.watermark_for("transfer"), Some(100));
        assert_eq!(layer.watermark_for("create_user"), None);
        assert!(matches!(
            layer.decide(&key("create_user", 1)).unwrap(),
            IdempotencyDecision::New
        ));
    }

    // INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    #[test]
    fn capture_restore_roundtrip_preserves_replay_behaviour() {
        let mut original = IdempotencyLayer::new(10);
        original
            .record(&key("transfer", 1), b"r1".to_vec())
            .unwrap();
        original
            .record(&key("transfer", 2), b"r2".to_vec())
            .unwrap();
        original
            .record(&key("create_user", 100), b"u100".to_vec())
            .unwrap();

        let captured = original.capture().unwrap();
        let mut restored = IdempotencyLayer::new(10);
        restored.restore(captured).unwrap();

        assert_eq!(restored.watermark_for("transfer"), Some(2));
        assert_eq!(restored.watermark_for("create_user"), Some(100));
        match restored.decide(&key("transfer", 1)).unwrap() {
            IdempotencyDecision::Replay(bytes) => assert_eq!(bytes, b"r1"),
            other => panic!("expected Replay after restore, got {other:?}"),
        }
        match restored.decide(&key("create_user", 100)).unwrap() {
            IdempotencyDecision::Replay(bytes) => assert_eq!(bytes, b"u100"),
            other => panic!("expected Replay after restore, got {other:?}"),
        }
    }

    #[test]
    fn restore_of_empty_state_leaves_layer_untouched() {
        let mut layer = IdempotencyLayer::new(10);
        layer.record(&key("transfer", 5), vec![]).unwrap();
        let empty = IdempotencyState::default();
        layer.restore(empty).unwrap();
        assert_eq!(layer.watermark_for("transfer"), Some(5));
    }

    // INV-5: a snapshot is the consistent state at one log index; install fully replaces state, never merges.
    #[test]
    fn restore_replaces_existing_cache_entries_not_merge() {
        let mut original = IdempotencyLayer::new(10);
        original
            .record(&key("a", 1), b"snapshot-resp".to_vec())
            .unwrap();
        let captured = original.capture().unwrap();

        let mut layer = IdempotencyLayer::new(10);
        layer
            .record(&key("a", 99), b"pre-restore-resp".to_vec())
            .unwrap();
        layer
            .record(&key("b", 7), b"another-pre-restore".to_vec())
            .unwrap();
        layer.restore(captured).unwrap();

        match layer.decide(&key("a", 1)).unwrap() {
            IdempotencyDecision::Replay(bytes) => assert_eq!(bytes, b"snapshot-resp"),
            other => panic!("expected captured key in cache, got {other:?}"),
        }
        match layer.decide(&key("a", 99)).unwrap() {
            IdempotencyDecision::New => {}
            other => panic!(
                "post-restore the pre-existing key 99 must be gone (replace-not-merge); \
                 got {other:?}"
            ),
        }
        match layer.decide(&key("b", 7)).unwrap() {
            IdempotencyDecision::New => {}
            other => panic!(
                "post-restore pre-existing key in different request_type must be gone; \
                 got {other:?}"
            ),
        }
    }

    // INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    #[test]
    fn first_request_id_zero_is_new_not_expired() {
        let mut layer = IdempotencyLayer::new(10);
        match layer.decide(&key("kafka_topic", 0)).unwrap() {
            IdempotencyDecision::New => {}
            other => panic!(
                "request_id 0 with no prior record must be New, not {other:?} \
                 (regression: Kafka offsets start at 0)"
            ),
        }
    }

    #[test]
    fn replayed_request_id_zero_after_record_is_replay() {
        let mut layer = IdempotencyLayer::new(10);
        let k = key("kafka_topic", 0);
        layer.record(&k, b"first-response".to_vec()).unwrap();
        match layer.decide(&k).unwrap() {
            IdempotencyDecision::Replay(bytes) => assert_eq!(bytes, b"first-response"),
            other => panic!("expected Replay for re-applied id=0, got {other:?}"),
        }
    }

    #[test]
    fn evicted_request_id_zero_after_record_is_expired() {
        let mut layer = IdempotencyLayer::new(2);
        layer.record(&key("kafka", 0), vec![]).unwrap();
        layer.record(&key("kafka", 1), vec![]).unwrap();
        layer.record(&key("kafka", 2), vec![]).unwrap();
        match layer.decide(&key("kafka", 0)).unwrap() {
            IdempotencyDecision::Expired { watermark, .. } => assert_eq!(watermark, 2),
            other => panic!("expected Expired for evicted id=0, got {other:?}"),
        }
    }

    #[test]
    fn cache_len_reports_size() {
        let mut layer = IdempotencyLayer::new(100);
        assert_eq!(layer.cache_len(), 0);
        layer.record(&key("a", 1), vec![]).unwrap();
        layer.record(&key("a", 2), vec![]).unwrap();
        layer.record(&key("b", 1), vec![]).unwrap();
        assert_eq!(layer.cache_len(), 3);
    }

    // Namespace cap: `request_type` is client-controlled and watermarks are
    // never evicted, so growth beyond MAX_IDEMPO_NAMESPACES must fail loudly.
    // INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    #[test]
    fn new_namespace_beyond_cap_is_rejected_loudly() {
        let mut layer = IdempotencyLayer::new(10);
        for i in 0..MAX_IDEMPO_NAMESPACES {
            let rt = format!("ns-{i}");
            layer.record(&key(&rt, 0), vec![]).unwrap();
        }

        let err = layer
            .decide(&key("one-namespace-too-many", 1))
            .expect_err("a new namespace beyond the cap must be rejected");
        match err {
            RrError::IdempotencyNamespaceLimitExceeded {
                request_type,
                namespaces,
                max,
            } => {
                assert_eq!(request_type, "one-namespace-too-many");
                assert_eq!(namespaces, MAX_IDEMPO_NAMESPACES);
                assert_eq!(max, MAX_IDEMPO_NAMESPACES);
            }
            other => panic!("expected IdempotencyNamespaceLimitExceeded, got {other:?}"),
        }

        let err = layer
            .record(&key("one-namespace-too-many", 1), vec![])
            .expect_err("record must enforce the cap for direct callers");
        assert!(matches!(
            err,
            RrError::IdempotencyNamespaceLimitExceeded { .. }
        ));
    }

    // Namespace cap: already-tracked namespaces keep working at the cap —
    // decide (Replay/Expired/New-above-watermark) and record are unaffected.
    #[test]
    fn cap_does_not_affect_existing_namespaces() {
        let mut layer = IdempotencyLayer::new(4);
        for i in 0..MAX_IDEMPO_NAMESPACES {
            let rt = format!("ns-{i}");
            layer.record(&key(&rt, 0), vec![]).unwrap();
        }
        layer.record(&key("ns-0", 1), b"r1".to_vec()).unwrap();

        // Replay on an existing namespace works at the cap.
        match layer.decide(&key("ns-0", 1)).unwrap() {
            IdempotencyDecision::Replay(bytes) => assert_eq!(bytes, b"r1"),
            other => panic!("expected Replay at the cap, got {other:?}"),
        }
        // New id above the watermark in an existing namespace works.
        assert!(matches!(
            layer.decide(&key("ns-0", 2)).unwrap(),
            IdempotencyDecision::New
        ));
        // record within an existing namespace works.
        layer.record(&key("ns-1", 5), vec![]).unwrap();
        assert_eq!(layer.watermark_for("ns-1"), Some(5));
    }
}
