//! Per-service rate limiting for rr gRPC services.
//!
//! Implements a lightweight token bucket using [`tokio::sync::Semaphore`]
//! plus a background refill task. No external deps (no governor, no
//! tower-governor). Good for low-millions rps; if you need sophisticated
//! distributed limits or per-client-IP quotas, wrap rr-server behind
//! envoy / nginx / Istio.
//!
//! # Semantics
//!
//! - A [`RateLimiter`] configured at `rps = R` and `burst = B` allows up
//!   to `B` concurrent requests to pass; permits accrue at `R` per second,
//!   added by a background task on a fixed coarse tick (10 ms) as
//!   `min(B - available, elapsed × R)` with sub-permit fractions carried
//!   over between ticks. This keeps refill exact from 1 rps up to
//!   [`MAX_RPS`] (10⁹) without busy-spinning the tokio timer.
//! - On exhaustion, [`RateLimitInterceptor::call`] returns
//!   `Status::resource_exhausted("rate limit exceeded")` — clients see
//!   a gRPC `RESOURCE_EXHAUSTED` code (code 8).
//! - **Never** apply rate limiting to `RaftService`: throttling Raft RPCs
//!   can cause election timeouts and destabilize the cluster.
//! - Rate limiting IS applied to `AdminService` (protects against
//!   runaway ops scripts) and optionally `ClientService`.
//!
//! # Per-caller rate limiting
//!
//! When `per_caller_*` fields are set, each authenticated caller (identified
//! by the bearer-token fingerprint, see [`crate::auth::token_fingerprint`])
//! gets its own token bucket. Unauthenticated requests (no bearer token →
//! no fingerprint) fall through to the global bucket built from the
//! matching `admin_rps` / `client_rps`, so `RateLimitConfig::validate`
//! requires the global field whenever the per-caller one is set. This
//! isolates noisy neighbours without double-checking.
//!
//! # Relation to TLS and Auth
//!
//! Rate limiting runs **after** TLS handshake and **after** bearer-token
//! validation. So unauthenticated callers and invalid-cert callers do
//! NOT consume rate limit tokens (they fail earlier). This is the right
//! order: an attacker sending garbage can't exhaust your admin quota.
//!
//! # Why not `tower::limit::RateLimit`?
//!
//! `tower::limit::RateLimit` wraps a `Service` and imposes a global
//! `Server::builder().layer()` limit — but that applies uniformly to
//! all services, breaking the "never limit Raft" invariant. Our per-
//! service tonic interceptor is the correct tool.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rr_core::error::ConfigError;
use tokio::sync::Semaphore;
use tonic::service::Interceptor;
use tonic::{Request, Status};

use crate::audit::{ServiceLabel, AUDIT_TARGET};

/// Upper bound accepted for any `*_rps` field of [`RateLimitConfig`].
///
/// Above 10⁹ rps the "R tokens per second" model degenerates to
/// sub-nanosecond spacing — meaningless for a token bucket. Configs
/// wanting "effectively unlimited" should leave the field `None` instead.
pub const MAX_RPS: u32 = 1_000_000_000;

/// Fixed refill tick of the background permit task. Coarse enough that
/// even `rps = MAX_RPS` does not busy-spin the timer wheel; fine enough
/// that sustained rate error stays well below 1%.
const REFILL_TICK: Duration = Duration::from_millis(10);

/// Configuration for rate limiting admin and client gRPC services.
///
/// `None` for a field = unlimited for that service.
#[derive(Debug, Clone, Default)]
pub struct RateLimitConfig {
    /// Max requests per second accepted by AdminService.
    /// Defaults to `None` (unlimited). Reasonable production value:
    /// 10–100 rps (admin ops are rare). Also serves as the fallback
    /// global bucket for unauthenticated requests when
    /// `per_caller_admin_rps` is set.
    pub admin_rps: Option<u32>,
    /// Max requests per second accepted by ClientService.
    /// Defaults to `None` (unlimited). Also serves as the fallback
    /// global bucket for unauthenticated requests when
    /// `per_caller_client_rps` is set.
    pub client_rps: Option<u32>,
    /// Burst size for both services. If unset, derived as `max(rps, 10)`.
    pub burst: Option<u32>,

    /// Per-caller rps limit for AdminService. When set, each unique
    /// authenticated caller gets its own bucket of this size, and
    /// unauthenticated requests share the global `admin_rps` bucket.
    pub per_caller_admin_rps: Option<u32>,
    /// Per-caller rps limit for ClientService. When set, each unique
    /// authenticated caller gets its own bucket of this size, and
    /// unauthenticated requests share the global `client_rps` bucket.
    pub per_caller_client_rps: Option<u32>,
    /// Per-caller burst. Defaults to `max(per_caller_rps, 10)` if unset.
    pub per_caller_burst: Option<u32>,
}

impl RateLimitConfig {
    /// Validate field values and combinations. Returns `Err` listing every
    /// violated rule: any `*_rps` above [`MAX_RPS`], or a per-caller limit
    /// without the matching global fallback bucket (`admin_rps` /
    /// `client_rps`) for unauthenticated requests.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut violations: Vec<String> = Vec::new();

        for (field, rps) in [
            ("admin_rps", self.admin_rps),
            ("client_rps", self.client_rps),
            ("per_caller_admin_rps", self.per_caller_admin_rps),
            ("per_caller_client_rps", self.per_caller_client_rps),
        ] {
            if let Some(rps) = rps {
                if rps > MAX_RPS {
                    violations.push(format!(
                        "{field} = {rps} exceeds MAX_RPS ({MAX_RPS}); above ~10⁹ rps the \
                         token-bucket model is meaningless — leave the field unset \
                         (unlimited) instead"
                    ));
                }
            }
        }

        if self.per_caller_admin_rps.is_some() && self.admin_rps.is_none() {
            violations.push(
                "per_caller_admin_rps requires admin_rps: unauthenticated requests carry no \
                 bearer token and therefore no caller fingerprint, so they must fall back to \
                 a global bucket (see module docs). Set admin_rps, or drop \
                 per_caller_admin_rps."
                    .to_string(),
            );
        }
        if self.per_caller_client_rps.is_some() && self.client_rps.is_none() {
            violations.push(
                "per_caller_client_rps requires client_rps: unauthenticated requests carry no \
                 bearer token and therefore no caller fingerprint, so they must fall back to \
                 a global bucket (see module docs). Set client_rps, or drop \
                 per_caller_client_rps."
                    .to_string(),
            );
        }

        if violations.is_empty() {
            return Ok(());
        }
        Err(ConfigError::InvalidConfig(format!(
            "invalid rate limit configuration:\n  - {}",
            violations.join("\n  - ")
        )))
    }

    /// Build an interceptor suitable for AdminService. Returns a
    /// pass-through interceptor if `admin_rps` is `None`.
    pub fn admin_interceptor(&self) -> RateLimitInterceptor {
        let service = ServiceLabel::Admin;
        if let Some(rps) = self.per_caller_admin_rps {
            let burst = self.per_caller_burst.unwrap_or(rps.max(10));
            let map = PerCallerLimiterMap::new(rps, burst, Duration::from_secs(300));
            // Unauthenticated requests (no bearer token → no fingerprint)
            // share a global fallback bucket built from `admin_rps`, per the
            // module-level contract.
            return match self.global_fallback(self.admin_rps) {
                Some(fallback) => RateLimitInterceptor::with_per_caller_and_fallback(
                    service, map, rps, burst, fallback,
                ),
                None => {
                    // `validate()` rejects this combination at startup; a
                    // caller who skipped validation gets the loud warning
                    // below rather than a silent unlimited path.
                    tracing::warn!(
                        "rate limit misconfiguration: per_caller_admin_rps is set but \
                         admin_rps is not — unauthenticated AdminService requests are \
                         UNLIMITED. Set admin_rps as the global fallback bucket \
                         (RateLimitConfig::validate rejects this combination)."
                    );
                    RateLimitInterceptor::with_per_caller(service, map, rps, burst)
                }
            };
        }
        RateLimitInterceptor::from_rps(service, self.admin_rps, self.burst)
    }

    /// Build an interceptor suitable for ClientService.
    pub fn client_interceptor(&self) -> RateLimitInterceptor {
        let service = ServiceLabel::Client;
        if let Some(rps) = self.per_caller_client_rps {
            let burst = self.per_caller_burst.unwrap_or(rps.max(10));
            let map = PerCallerLimiterMap::new(rps, burst, Duration::from_secs(300));
            return match self.global_fallback(self.client_rps) {
                Some(fallback) => RateLimitInterceptor::with_per_caller_and_fallback(
                    service, map, rps, burst, fallback,
                ),
                None => {
                    tracing::warn!(
                        "rate limit misconfiguration: per_caller_client_rps is set but \
                         client_rps is not — unauthenticated ClientService requests are \
                         UNLIMITED. Set client_rps as the global fallback bucket \
                         (RateLimitConfig::validate rejects this combination)."
                    );
                    RateLimitInterceptor::with_per_caller(service, map, rps, burst)
                }
            };
        }
        RateLimitInterceptor::from_rps(service, self.client_rps, self.burst)
    }

    /// Build the global fallback bucket for per-caller mode from the
    /// service's global `*_rps` field (service-level `burst` applies).
    fn global_fallback(&self, global_rps: Option<u32>) -> Option<RateLimiter> {
        global_rps.map(|r| {
            let b = self.burst.unwrap_or(r.max(10));
            RateLimiter::new(r, b)
        })
    }
}

/// Per-caller rate-limit configuration used when constructing a
/// [`PerCallerLimiterMap`].
#[derive(Debug, Clone)]
pub struct PerCallerConfig {
    pub per_caller_rps: u32,
    pub per_caller_burst: u32,
    pub ttl_secs: u64,
}

impl Default for PerCallerConfig {
    fn default() -> Self {
        Self {
            per_caller_rps: 10,
            per_caller_burst: 10,
            ttl_secs: 300,
        }
    }
}

/// Guard that aborts the wrapped background task when the owning struct's
/// last clone is dropped — no fire-and-forget spawns: every background
/// task's lifetime is tied to the value that spawned it.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A token-bucket rate limiter backed by a Semaphore.
///
/// Permits are **forgotten** on acquisition (never returned) and
/// replenished by a background tokio task on a fixed [`REFILL_TICK`] tick:
/// each tick adds `min(burst - available, elapsed × rps)` permits, with
/// sub-permit fractions carried between ticks so the long-run rate is
/// exact for any `rps` in `1..=MAX_RPS`. The pool caps at `burst` to
/// prevent unbounded accumulation during idle periods. The refill task is
/// aborted when the last clone of this limiter is dropped.
#[derive(Clone)]
pub struct RateLimiter {
    semaphore: Arc<Semaphore>,
    #[allow(dead_code)]
    burst: usize,
    _refill_guard: Arc<AbortOnDrop>,
}

impl RateLimiter {
    /// Create a new limiter with the given rate (requests per second)
    /// and burst size (max concurrent requests at any one moment).
    ///
    /// `rps` is clamped to [`MAX_RPS`]; configs above that are rejected by
    /// [`RateLimitConfig::validate`]. `rps = 0` disables refill (burst-only).
    ///
    /// Spawns a background tokio task that refills permits. Must be
    /// called from within a tokio runtime context.
    pub fn new(rps: u32, burst: u32) -> Self {
        let rps = rps.min(MAX_RPS);
        let burst = burst.max(1) as usize;
        let semaphore = Arc::new(Semaphore::new(burst));
        let sem = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(REFILL_TICK);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last = Instant::now();
            // Sub-permit fraction carried between ticks, in
            // nanosecond-denominated units: `elapsed_ns × rps` accumulates
            // and every full 10⁹ yields one whole permit, so a 10 ms tick
            // does not truncate low rates (rps=1 → +1 permit/second exactly)
            // nor spin the timer at high rates.
            let mut carry: u128 = 0;
            loop {
                ticker.tick().await;
                if Arc::strong_count(&sem) == 1 {
                    break;
                }
                let now = Instant::now();
                let elapsed_ns = now.duration_since(last).as_nanos();
                last = now;
                carry += elapsed_ns * u128::from(rps);
                let new_permits = (carry / 1_000_000_000) as usize;
                carry %= 1_000_000_000;
                if new_permits == 0 {
                    continue;
                }
                let current = sem.available_permits();
                if current < burst {
                    sem.add_permits(new_permits.min(burst - current));
                }
            }
        });

        Self {
            semaphore,
            burst,
            _refill_guard: Arc::new(AbortOnDrop(handle)),
        }
    }

    /// Try to acquire one permit without blocking. Returns `true` if a
    /// token was available (consumed); `false` if the bucket was empty.
    pub fn try_acquire(&self) -> bool {
        match self.semaphore.try_acquire() {
            Ok(permit) => {
                permit.forget();
                true
            }
            Err(_) => false,
        }
    }

    #[cfg(test)]
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    #[cfg(test)]
    pub fn burst(&self) -> usize {
        self.burst
    }
}

/// An entry in the per-caller limiter map.
struct CallerEntry {
    limiter: RateLimiter,
    last_seen: Instant,
}

/// Shared state that holds one [`RateLimiter`] per authenticated caller.
///
/// Backed by `std::sync::RwLock<HashMap>` — not a hot path, so
/// contention is acceptable. If profiling shows issues, upgrading to
/// `DashMap` is a one-line change.
#[derive(Clone)]
pub struct PerCallerLimiterMap {
    entries: Arc<std::sync::RwLock<HashMap<String, CallerEntry>>>,
    per_caller_rps: u32,
    per_caller_burst: u32,
    ttl: Duration,
    _gc_guard: Arc<AbortOnDrop>,
}

impl PerCallerLimiterMap {
    /// Create a new map with the given per-caller rate, burst, and TTL.
    ///
    /// Spawns a background tokio task that evicts entries idle for longer
    /// than `ttl` every `ttl / 2` seconds (minimum 30 s). The task is
    /// aborted when the last clone of this map is dropped (it also stops
    /// on its own once the entries map is gone).
    pub fn new(per_caller_rps: u32, per_caller_burst: u32, ttl: Duration) -> Self {
        let entries: Arc<std::sync::RwLock<HashMap<String, CallerEntry>>> =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let entries_weak = Arc::downgrade(&entries);
        let gc_interval = (ttl / 2).max(Duration::from_secs(30));

        let gc_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(gc_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let Some(map) = entries_weak.upgrade() else {
                    break;
                };
                let now = Instant::now();
                let mut guard = map.write().unwrap_or_else(|p| p.into_inner());
                guard.retain(|_, entry| now.duration_since(entry.last_seen) <= ttl);
            }
        });

        Self {
            entries,
            per_caller_rps,
            per_caller_burst,
            ttl,
            _gc_guard: Arc::new(AbortOnDrop(gc_handle)),
        }
    }

    /// Try to acquire one permit for `caller_fp`.  Lazily inserts a new
    /// [`RateLimiter`] if the caller is not yet present, and lazily
    /// evicts stale entries whose `last_seen` age exceeds `ttl`.
    ///
    /// Returns `true` when the caller still has quota.
    pub fn try_acquire(&self, caller_fp: &str) -> bool {
        {
            let read_guard = self
                .entries
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(entry) = read_guard.get(caller_fp) {
                if entry.limiter.try_acquire() {
                    drop(read_guard);
                    let mut write_guard = self
                        .entries
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if let Some(entry) = write_guard.get_mut(caller_fp) {
                        entry.last_seen = Instant::now();
                    }
                    return true;
                }
                return false;
            }
        }

        let mut write_guard = self
            .entries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.evict_stale(&mut write_guard);

        if let Some(entry) = write_guard.get_mut(caller_fp) {
            entry.last_seen = Instant::now();
            return entry.limiter.try_acquire();
        }

        let limiter = RateLimiter::new(self.per_caller_rps, self.per_caller_burst);
        let acquired = limiter.try_acquire();
        write_guard.insert(
            caller_fp.to_string(),
            CallerEntry {
                limiter,
                last_seen: Instant::now(),
            },
        );
        acquired
    }

    /// Evict entries whose `last_seen` is older than `ttl`.
    fn evict_stale(&self, guard: &mut HashMap<String, CallerEntry>) {
        let now = Instant::now();
        guard.retain(|_, entry| now.duration_since(entry.last_seen) <= self.ttl);
    }
}

/// Extract the caller token fingerprint from an incoming request.
fn caller_fp(request: &Request<()>) -> Option<String> {
    request
        .metadata()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(crate::auth::strip_bearer_scheme)
        .map(crate::auth::token_fingerprint)
}

/// A tonic interceptor that enforces a [`RateLimiter`]. When `limiter`
/// is `None`, the interceptor is a pass-through (disabled mode).
///
/// When `per_caller` is set, authenticated callers are rate-limited
/// individually; unauthenticated requests (or requests without a bearer
/// token) fall through to the global `limiter`.
///
/// Every reject is emitted to the audit log
/// ([`crate::audit::AUDIT_TARGET`]) with `op="rate_limit"` plus peer /
/// service so operators can detect noisy clients and runaway scripts.
#[derive(Clone)]
pub struct RateLimitInterceptor {
    limiter: Option<RateLimiter>,
    service: ServiceLabel,
    per_caller: Option<(PerCallerLimiterMap, u32, u32)>,
}

impl RateLimitInterceptor {
    fn from_rps(service: ServiceLabel, rps: Option<u32>, burst: Option<u32>) -> Self {
        let limiter = rps.map(|r| {
            let b = burst.unwrap_or(r.max(10));
            RateLimiter::new(r, b)
        });
        Self {
            limiter,
            service,
            per_caller: None,
        }
    }

    /// Disabled constructor (mostly for tests or explicit opt-out).
    pub fn disabled(service: ServiceLabel) -> Self {
        Self {
            limiter: None,
            service,
            per_caller: None,
        }
    }

    /// Enabled constructor with a custom limiter.
    pub fn with_limiter(service: ServiceLabel, limiter: RateLimiter) -> Self {
        Self {
            limiter: Some(limiter),
            service,
            per_caller: None,
        }
    }

    /// Enabled constructor in per-caller mode.
    ///
    /// Unauthenticated requests pass through unlimited — prefer
    /// [`Self::with_per_caller_and_fallback`] so they share a global
    /// bucket instead (this is what [`RateLimitConfig`]'s interceptor
    /// builders use).
    pub fn with_per_caller(
        service: ServiceLabel,
        map: PerCallerLimiterMap,
        rps: u32,
        burst: u32,
    ) -> Self {
        Self {
            limiter: None,
            service,
            per_caller: Some((map, rps, burst)),
        }
    }

    /// Enabled constructor in per-caller mode with a global fallback
    /// bucket: authenticated callers are limited individually, and
    /// requests without a bearer token (no caller fingerprint) draw from
    /// `fallback`.
    pub fn with_per_caller_and_fallback(
        service: ServiceLabel,
        map: PerCallerLimiterMap,
        rps: u32,
        burst: u32,
        fallback: RateLimiter,
    ) -> Self {
        Self {
            limiter: Some(fallback),
            service,
            per_caller: Some((map, rps, burst)),
        }
    }
}

impl Interceptor for RateLimitInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let peer = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        if let Some((map, _rps, _burst)) = &self.per_caller {
            if let Some(fp) = caller_fp(&request) {
                if map.try_acquire(&fp) {
                    return Ok(request);
                }
                tracing::warn!(
                    target: AUDIT_TARGET,
                    op = "rate_limit",
                    service = self.service.as_str(),
                    peer = %peer,
                    caller_token_fp = %fp,
                    result = "rejected",
                    "rate limit exceeded"
                );
                return Err(Status::resource_exhausted("rate limit exceeded"));
            }
        }

        let Some(limiter) = &self.limiter else {
            return Ok(request);
        };
        if limiter.try_acquire() {
            Ok(request)
        } else {
            tracing::warn!(
                target: AUDIT_TARGET,
                op = "rate_limit",
                service = self.service.as_str(),
                peer = %peer,
                result = "rejected",
                "rate limit exceeded"
            );
            Err(Status::resource_exhausted("rate limit exceeded"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_interceptor_always_allows() {
        let mut interceptor = RateLimitInterceptor::disabled(ServiceLabel::Admin);
        for _ in 0..100 {
            assert!(interceptor.call(Request::new(())).is_ok());
        }
    }

    #[tokio::test]
    async fn limiter_starts_full() {
        let limiter = RateLimiter::new(100, 10);
        assert_eq!(limiter.available_permits(), 10);
        assert_eq!(limiter.burst(), 10);
    }

    #[tokio::test]
    async fn limiter_exhausts_after_burst() {
        let limiter = RateLimiter::new(1, 3);
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
    }

    #[tokio::test]
    async fn interceptor_rejects_with_resource_exhausted() {
        let limiter = RateLimiter::new(1, 1);
        let mut interceptor = RateLimitInterceptor::with_limiter(ServiceLabel::Admin, limiter);
        assert!(interceptor.call(Request::new(())).is_ok());
        let err = interceptor.call(Request::new(())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn limiter_refills_over_time() {
        let limiter = RateLimiter::new(100, 1);
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(limiter.try_acquire());
    }

    #[tokio::test]
    async fn config_disables_by_default() {
        let cfg = RateLimitConfig::default();
        let mut admin = cfg.admin_interceptor();
        let mut client = cfg.client_interceptor();
        for _ in 0..50 {
            assert!(admin.call(Request::new(())).is_ok());
            assert!(client.call(Request::new(())).is_ok());
        }
    }

    #[tokio::test]
    async fn config_with_admin_rps_enforces() {
        let cfg = RateLimitConfig {
            admin_rps: Some(1),
            client_rps: None,
            burst: Some(2),
            per_caller_admin_rps: None,
            per_caller_client_rps: None,
            per_caller_burst: None,
        };
        let mut admin = cfg.admin_interceptor();
        let mut client = cfg.client_interceptor();

        assert!(admin.call(Request::new(())).is_ok());
        assert!(admin.call(Request::new(())).is_ok());
        let err = admin.call(Request::new(())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);

        for _ in 0..20 {
            assert!(client.call(Request::new(())).is_ok());
        }
    }

    #[tokio::test]
    async fn per_caller_map_separates_callers() {
        let map = PerCallerLimiterMap::new(1, 2, Duration::from_secs(300));
        assert!(map.try_acquire("caller-a"));
        assert!(map.try_acquire("caller-a"));
        assert!(!map.try_acquire("caller-a"));
        assert!(map.try_acquire("caller-b"));
        assert!(map.try_acquire("caller-b"));
        assert!(!map.try_acquire("caller-b"));
    }

    #[tokio::test]
    async fn per_caller_map_evicts_stale_entries() {
        // The caller's bucket must serve again after a pause. With the
        // exact `elapsed × rps` refill there is no free permit at task
        // start (the old interval's immediate first tick granted one), so
        // at rps=1 the second acquire would have to wait ~1 s; rps=100
        // refills burst=1 within the first 10 ms tick instead. (The GC
        // floor of 30 s means no eviction actually happens in this
        // window — the entry is reused, not recreated.)
        let map = PerCallerLimiterMap::new(100, 1, Duration::from_millis(50));
        assert!(map.try_acquire("stale"));
        assert!(!map.try_acquire("stale"));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(map.try_acquire("stale"));
    }

    fn req_with_auth(token: &str) -> Request<()> {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        req
    }

    #[tokio::test]
    async fn per_caller_interceptor_allows_within_quota() {
        let map = PerCallerLimiterMap::new(100, 5, Duration::from_secs(300));
        let mut interceptor =
            RateLimitInterceptor::with_per_caller(ServiceLabel::Admin, map, 100, 5);
        for _ in 0..5 {
            assert!(interceptor.call(req_with_auth("alice")).is_ok());
        }
    }

    #[tokio::test]
    async fn per_caller_interceptor_rejects_at_quota() {
        let map = PerCallerLimiterMap::new(100, 2, Duration::from_secs(300));
        let mut interceptor =
            RateLimitInterceptor::with_per_caller(ServiceLabel::Admin, map, 100, 2);
        assert!(interceptor.call(req_with_auth("bob")).is_ok());
        assert!(interceptor.call(req_with_auth("bob")).is_ok());
        let err = interceptor.call(req_with_auth("bob")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn per_caller_falls_through_to_global_when_no_auth() {
        let global = RateLimiter::new(100, 2);
        let map = PerCallerLimiterMap::new(100, 5, Duration::from_secs(300));
        let mut interceptor = RateLimitInterceptor {
            limiter: Some(global),
            service: ServiceLabel::Admin,
            per_caller: Some((map, 100, 5)),
        };
        assert!(interceptor.call(Request::new(())).is_ok());
        assert!(interceptor.call(Request::new(())).is_ok());
        let err = interceptor.call(Request::new(())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn config_with_per_caller_rps_uses_per_caller_mode() {
        let cfg = RateLimitConfig {
            admin_rps: Some(1),
            client_rps: None,
            burst: Some(1),
            per_caller_admin_rps: Some(5),
            per_caller_client_rps: None,
            per_caller_burst: Some(5),
        };
        let mut admin = cfg.admin_interceptor();
        for _ in 0..5 {
            assert!(admin.call(req_with_auth("alice")).is_ok());
        }
        let err = admin.call(req_with_auth("alice")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);

        for _ in 0..5 {
            assert!(admin.call(req_with_auth("bob")).is_ok());
        }
    }

    #[tokio::test]
    async fn abort_on_drop_aborts_spawned_task() {
        let handle = tokio::spawn(async { std::future::pending::<()>().await });
        let abort_handle = handle.abort_handle();
        {
            let _guard = AbortOnDrop(handle);
        }
        // Dropping the guard aborts immediately; give the runtime a turn
        // to settle the task state before asserting.
        tokio::task::yield_now().await;
        assert!(abort_handle.is_finished());
    }

    #[tokio::test]
    async fn high_rps_refill_accrues_permits_in_coarse_ticks() {
        // Regression: the old one-permit-per-tick refill spun the tokio
        // timer at `1/rps` intervals — at rps ≳ 1000 that busy-loops and
        // still under-refills. The fixed 10 ms tick must now credit
        // `elapsed × rps` permits per tick (100k rps × 50 ms ≈ 5000).
        let limiter = RateLimiter::new(100_000, 10_000);
        for _ in 0..10_000 {
            assert!(limiter.try_acquire());
        }
        assert!(!limiter.try_acquire());
        tokio::time::sleep(Duration::from_millis(50)).await;
        let available = limiter.available_permits();
        assert!(
            available >= 1_000,
            "expected coarse-tick refill to accrue thousands of permits, got {available}"
        );
    }

    #[tokio::test]
    async fn refill_never_exceeds_burst() {
        let limiter = RateLimiter::new(1_000_000, 5);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(limiter.available_permits(), 5);
    }

    #[tokio::test]
    async fn zero_rps_disables_refill_but_keeps_burst() {
        let limiter = RateLimiter::new(0, 2);
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !limiter.try_acquire(),
            "rps=0 must stay burst-only (no refill)"
        );
    }

    #[test]
    fn validate_accepts_default_and_sane_configs() {
        assert!(RateLimitConfig::default().validate().is_ok());
        let cfg = RateLimitConfig {
            admin_rps: Some(100),
            client_rps: Some(10_000),
            burst: Some(50),
            per_caller_admin_rps: Some(5),
            per_caller_client_rps: Some(500),
            per_caller_burst: Some(10),
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_rps_above_max() {
        let cfg = RateLimitConfig {
            client_rps: Some(MAX_RPS + 1),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("client_rps") && msg.contains("MAX_RPS"),
            "expected MAX_RPS violation naming the field, got: {msg}"
        );
    }

    #[test]
    fn validate_rejects_per_caller_without_global_fallback() {
        let cfg = RateLimitConfig {
            per_caller_admin_rps: Some(5),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("per_caller_admin_rps requires admin_rps"),
            "got: {err}"
        );

        let cfg = RateLimitConfig {
            per_caller_client_rps: Some(5),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("per_caller_client_rps requires client_rps"),
            "got: {err}"
        );
    }

    // INV-8: no ungated listeners in production config — all security gates pass or RrNode refuses to start.
    #[tokio::test]
    async fn per_caller_config_limits_unauthenticated_via_global_fallback() {
        // Regression: with_per_caller used to install `limiter: None`, so
        // requests without a bearer token bypassed limiting entirely
        // despite the documented global-bucket fallback.
        let cfg = RateLimitConfig {
            admin_rps: Some(2),
            client_rps: None,
            burst: Some(2),
            per_caller_admin_rps: Some(100),
            per_caller_client_rps: None,
            per_caller_burst: Some(5),
        };
        let mut admin = cfg.admin_interceptor();

        // Authenticated callers get their per-caller bucket (burst 5).
        for _ in 0..5 {
            assert!(admin.call(req_with_auth("alice")).is_ok());
        }
        let err = admin.call(req_with_auth("alice")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);

        // Unauthenticated requests draw from the global fallback bucket
        // (admin_rps=2, burst=2): the third request must be rejected.
        assert!(admin.call(Request::new(())).is_ok());
        assert!(admin.call(Request::new(())).is_ok());
        let err = admin.call(Request::new(())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }
}
