//! Model-based property tests for [`IdempotencyLayer`] (reliability program
//! R-6; INV-3).
//!
//! The layer is checked against an independent model built from three parts:
//!
//! - an **unbounded** `HashMap` oracle recording every `record()` ever made
//!   (last write wins) — the ground truth for "what the original response
//!   bytes were";
//! - a per-`request_type` watermark oracle (`max` recorded `request_id`);
//! - a capacity-identical twin [`LruCache`] fed the same get/put sequence, so
//!   cache hit/miss — and therefore the `Expired` edge — is predicted exactly.
//!
//! Properties asserted after every op:
//!
//! 1. **Watermark monotonicity**: per-type watermarks never decrease and
//!    always equal the oracle's max.
//! 2. **Replay fidelity**: `Replay`/`Collision` return the originally
//!    recorded response bytes for that `(request_type, request_id)`.
//! 3. **Expired contract** (`rr-core/src/idempotency.rs`, `Expired` variant):
//!    `Expired` occurs ONLY when `request_id <= watermark` AND the key is a
//!    cache miss. (Note the condition is not sufficient for "was once
//!    recorded": an id inside a never-recorded gap below the watermark also
//!    yields `Expired` — the fail-closed choice the contract documents. We
//!    therefore assert only the documented necessary condition, never a
//!    stronger one.)
//! 4. **Dispatch parity**: `decide` outcomes match the model's four-way
//!    dispatch (Replay / Collision / Expired / New) exactly, under random
//!    decide/record sequences with heavy capacity pressure (caps 1..=8).
//! 5. **Snapshot parity**: `capture`+`restore` into a fresh layer under
//!    random load preserves all of the above (the model applies the same
//!    iterate-and-reput transform `restore` performs, so eviction order
//!    stays in lockstep).

use std::collections::HashMap;
use std::num::NonZeroUsize;

use lru::LruCache;
use proptest::prelude::*;
use rr_core::idempotency::{IdempotencyDecision, IdempotencyKey};

use crate::idempotency::IdempotencyLayer;

/// Small type pool so watermarks/LRU slots collide constantly.
const TYPE_POOL: [&str; 4] = ["transfer", "create_user", "kafka_topic", "withdraw"];

fn type_name(t: u8) -> &'static str {
    TYPE_POOL[(t as usize) % TYPE_POOL.len()]
}

fn client_name(c: Option<u8>) -> Option<&'static str> {
    c.map(|i| if i % 2 == 0 { "client-a" } else { "client-b" })
}

fn make_key(t: u8, id: u64, client: Option<u8>) -> IdempotencyKey {
    match client_name(client) {
        None => IdempotencyKey::new(type_name(t), id),
        Some(c) => IdempotencyKey::with_client_id(type_name(t), id, c),
    }
}

#[derive(Debug, Clone)]
enum Op {
    Decide {
        t: u8,
        id: u64,
        client: Option<u8>,
    },
    Record {
        t: u8,
        id: u64,
        client: Option<u8>,
        resp: Vec<u8>,
    },
    /// capture() + restore() into a fresh same-capacity layer.
    SnapshotRestore,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let t = 0u8..4;
    // Small id range: frequent below-watermark and repeat-id hits. 0 is in
    // range, covering the Kafka "first offset is 0" edge.
    let id = 0u64..24;
    let client = prop_oneof![2 => Just(None), 1 => Just(Some(0u8)), 1 => Just(Some(1u8))];
    prop_oneof![
        4 => (t.clone(), id.clone(), client.clone())
            .prop_map(|(t, id, client)| Op::Decide { t, id, client }),
        4 => (t, id, client, prop::collection::vec(any::<u8>(), 0..8))
            .prop_map(|(t, id, client, resp)| Op::Record { t, id, client, resp }),
        1 => Just(Op::SnapshotRestore),
    ]
}

type CacheKey = (String, u64);
type CacheValue = (Option<String>, Vec<u8>);

/// The independent model the layer is checked against.
struct Model {
    /// Unbounded oracle: every recorded response (last write wins).
    recorded: HashMap<CacheKey, CacheValue>,
    /// Max recorded request_id per request_type.
    watermark: HashMap<String, u64>,
    /// Capacity-identical twin of the layer's LRU, fed the same operations.
    cache: LruCache<CacheKey, CacheValue>,
}

impl Model {
    fn new(cap: usize) -> Self {
        Self {
            recorded: HashMap::new(),
            watermark: HashMap::new(),
            cache: LruCache::new(NonZeroUsize::new(cap.max(1)).unwrap()),
        }
    }

    fn watermark_of(&self, request_type: &str) -> Option<u64> {
        self.watermark.get(request_type).copied()
    }

    fn record(&mut self, key: &IdempotencyKey, resp: Vec<u8>) {
        let entry = self.watermark.entry(key.request_type.clone()).or_insert(0);
        if key.request_id > *entry {
            *entry = key.request_id;
        }
        let ck: CacheKey = (key.request_type.clone(), key.request_id);
        self.recorded
            .insert(ck.clone(), (key.client_id.clone(), resp.clone()));
        self.cache.put(ck, (key.client_id.clone(), resp));
    }

    /// Whether the twin cache currently holds the key (`peek`: no recency
    /// bump — this is the hit/miss state `decide` will observe).
    fn cache_miss(&self, ck: &CacheKey) -> bool {
        self.cache.peek(ck).is_none()
    }

    /// Mirror of the layer's documented four-way dispatch. `cache.get`
    /// bumps LRU recency exactly as `IdempotencyLayer::decide` does.
    fn expected_decision(&mut self, key: &IdempotencyKey) -> IdempotencyDecision {
        let ck: CacheKey = (key.request_type.clone(), key.request_id);
        if let Some((cached_client_id, response)) = self.cache.get(&ck) {
            if cached_client_id != &key.client_id {
                return IdempotencyDecision::Collision {
                    cached_client_id: cached_client_id.clone(),
                    incoming_client_id: key.client_id.clone(),
                    cached_response: response.clone(),
                };
            }
            return IdempotencyDecision::Replay(response.clone());
        }
        if let Some(watermark) = self.watermark.get(&key.request_type).copied() {
            if key.request_id <= watermark {
                return IdempotencyDecision::Expired {
                    request_type: key.request_type.clone(),
                    request_id: key.request_id,
                    watermark,
                };
            }
        }
        IdempotencyDecision::New
    }

    /// Mirror the exact transform `IdempotencyLayer::restore` performs on the
    /// cache: iterate the old cache and `put` every entry into a fresh
    /// same-capacity cache. Applying the same transform to the twin keeps
    /// contents AND eviction order identical without relying on `iter()`
    /// direction semantics.
    fn snapshot_restore(&mut self) {
        let cap = self.cache.cap();
        let old = std::mem::replace(&mut self.cache, LruCache::new(cap));
        for (k, v) in old.iter() {
            self.cache.put(k.clone(), v.clone());
        }
    }
}

fn assert_decision_parity(
    actual: &IdempotencyDecision,
    expected: &IdempotencyDecision,
    ctx: &str,
) -> Result<(), TestCaseError> {
    match (actual, expected) {
        (IdempotencyDecision::Replay(a), IdempotencyDecision::Replay(e)) => {
            prop_assert_eq!(a, e, "{}: Replay bytes differ from the model", ctx)
        }
        (
            IdempotencyDecision::Collision {
                cached_client_id: a_cached,
                incoming_client_id: a_incoming,
                cached_response: a_resp,
            },
            IdempotencyDecision::Collision {
                cached_client_id: e_cached,
                incoming_client_id: e_incoming,
                cached_response: e_resp,
            },
        ) => {
            prop_assert_eq!(a_cached, e_cached, "{}: Collision cached_client_id", ctx);
            prop_assert_eq!(
                a_incoming,
                e_incoming,
                "{}: Collision incoming_client_id",
                ctx
            );
            prop_assert_eq!(a_resp, e_resp, "{}: Collision cached_response", ctx);
        }
        (
            IdempotencyDecision::Expired {
                request_type: a_t,
                request_id: a_id,
                watermark: a_wm,
            },
            IdempotencyDecision::Expired {
                request_type: e_t,
                request_id: e_id,
                watermark: e_wm,
            },
        ) => {
            prop_assert_eq!(a_t, e_t, "{}: Expired request_type", ctx);
            prop_assert_eq!(a_id, e_id, "{}: Expired request_id", ctx);
            prop_assert_eq!(a_wm, e_wm, "{}: Expired watermark", ctx);
        }
        (IdempotencyDecision::New, IdempotencyDecision::New) => {}
        _ => {
            return Err(TestCaseError::fail(format!(
                "{ctx}: decision mismatch — layer returned {actual:?}, model expected {expected:?}"
            )));
        }
    }
    Ok(())
}

proptest! {
    // Bounded case count: this runs in the default CI test job (R-6 budget).
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    /// INV-3: a Kafka offset's user-visible effect happens exactly once; replays return the cached response.
    #[test]
    fn proptest_idempotency_lru_watermark_model_parity(
        cap in 1usize..=8,
        ops in prop::collection::vec(op_strategy(), 1..200),
    ) {
        let mut layer = IdempotencyLayer::new(cap);
        let mut model = Model::new(cap);

        for (step, op) in ops.iter().enumerate() {
            let ctx = format!("step {step} op {op:?} (cap {cap})");

            // Pre-op watermark snapshot for the monotonicity check.
            let wm_before: Vec<Option<u64>> =
                TYPE_POOL.iter().map(|t| layer.watermark_for(t)).collect();

            match op {
                Op::Decide { t, id, client } => {
                    let key = make_key(*t, *id, *client);
                    let ck: CacheKey = (key.request_type.clone(), key.request_id);
                    let miss = model.cache_miss(&ck);

                    let expected = model.expected_decision(&key);
                    // TYPE_POOL is far below MAX_IDEMPO_NAMESPACES, so the
                    // namespace cap never trips inside this property.
                    let actual = layer.decide(&key).expect("decide below the namespace cap");

                    // Property 4: exact dispatch parity.
                    assert_decision_parity(&actual, &expected, &ctx)?;

                    // Property 2: Replay returns the originally recorded value.
                    if let IdempotencyDecision::Replay(bytes) = &actual {
                        let (_, original) = model
                            .recorded
                            .get(&ck)
                            .expect("Replay implies a prior record in the oracle");
                        prop_assert_eq!(
                            bytes, original,
                            "{}: Replay must return the originally recorded response",
                            ctx
                        );
                    }

                    // Property 3: Expired only when id <= watermark && cache miss.
                    if let IdempotencyDecision::Expired {
                        request_id,
                        watermark,
                        ..
                    } = &actual
                    {
                        prop_assert!(
                            request_id <= watermark,
                            "{}: Expired requires request_id <= watermark",
                            ctx
                        );
                        prop_assert!(miss, "{}: Expired requires a cache miss", ctx);
                    }

                    // New implies the id is above any known watermark.
                    if matches!(actual, IdempotencyDecision::New) {
                        if let Some(wm) = model.watermark_of(&key.request_type) {
                            prop_assert!(
                                key.request_id > wm,
                                "{}: New for request_id {} at or below watermark {}",
                                ctx,
                                key.request_id,
                                wm
                            );
                        }
                    }
                }
                Op::Record {
                    t,
                    id,
                    client,
                    resp,
                } => {
                    let key = make_key(*t, *id, *client);
                    layer
                        .record(&key, resp.clone())
                        .expect("record below the namespace cap");
                    model.record(&key, resp.clone());
                }
                Op::SnapshotRestore => {
                    let captured = layer.capture().expect("capture must succeed");
                    let mut restored = IdempotencyLayer::new(cap);
                    restored.restore(captured).expect("restore must succeed");
                    layer = restored;
                    model.snapshot_restore();
                }
            }

            // Property 1: watermarks equal the oracle max and never decrease.
            for (i, t) in TYPE_POOL.iter().enumerate() {
                let now = layer.watermark_for(t);
                prop_assert_eq!(
                    now,
                    model.watermark_of(t),
                    "{}: watermark drift for request_type {}",
                    ctx,
                    t
                );
                if let (Some(before), Some(after)) = (wm_before[i], now) {
                    prop_assert!(
                        after >= before,
                        "{}: watermark for {} went backwards ({} -> {})",
                        ctx,
                        t,
                        before,
                        after
                    );
                }
                // None -> Some is fine (first record); Some -> None must not happen.
                if wm_before[i].is_some() {
                    prop_assert!(now.is_some(), "{}: watermark for {} disappeared", ctx, t);
                }
            }

            // Lockstep sanity: cache sizes always agree.
            prop_assert_eq!(
                layer.cache_len(),
                model.cache.len(),
                "{}: cache size diverged from the twin LRU",
                ctx
            );
        }
    }
}
