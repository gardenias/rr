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
//!   to `B` concurrent requests to pass; a permit is refilled roughly
//!   every `1/R` seconds.
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
//! gets its own token bucket. Unauthenticated requests fall through to the
//! global bucket. This isolates noisy neighbours without double-checking.
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

use tokio::sync::Semaphore;
use tonic::service::Interceptor;
use tonic::{Request, Status};

use crate::audit::{ServiceLabel, AUDIT_TARGET};

/// Configuration for rate limiting admin and client gRPC services.
///
/// `None` for a field = unlimited for that service.
#[derive(Debug, Clone, Default)]
pub struct RateLimitConfig {
    /// Max requests per second accepted by AdminService.
    /// Defaults to `None` (unlimited). Reasonable production value:
    /// 10–100 rps (admin ops are rare).
    pub admin_rps: Option<u32>,
    /// Max requests per second accepted by ClientService.
    /// Defaults to `None` (unlimited).
    pub client_rps: Option<u32>,
    /// Burst size for both services. If unset, derived as `max(rps, 10)`.
    pub burst: Option<u32>,

    /// Per-caller rps limit for AdminService (exclusive mode).
    /// When set, each unique caller gets its own bucket of this size.
    /// The global `admin_rps` is ignored when per-caller mode is active.
    pub per_caller_admin_rps: Option<u32>,
    /// Per-caller rps limit for ClientService (exclusive mode).
    /// When set, each unique caller gets its own bucket of this size.
    /// The global `client_rps` is ignored when per-caller mode is active.
    pub per_caller_client_rps: Option<u32>,
    /// Per-caller burst. Defaults to `max(per_caller_rps, 10)` if unset.
    pub per_caller_burst: Option<u32>,
}

impl RateLimitConfig {
    /// Build an interceptor suitable for AdminService. Returns a
    /// pass-through interceptor if `admin_rps` is `None`.
    pub fn admin_interceptor(&self) -> RateLimitInterceptor {
        let service = ServiceLabel::Admin;
        if let Some(rps) = self.per_caller_admin_rps {
            let burst = self.per_caller_burst.unwrap_or(rps.max(10));
            let map = PerCallerLimiterMap::new(rps, burst, Duration::from_secs(300));
            return RateLimitInterceptor::with_per_caller(service, map, rps, burst);
        }
        RateLimitInterceptor::from_rps(service, self.admin_rps, self.burst)
    }

    /// Build an interceptor suitable for ClientService.
    pub fn client_interceptor(&self) -> RateLimitInterceptor {
        let service = ServiceLabel::Client;
        if let Some(rps) = self.per_caller_client_rps {
            let burst = self.per_caller_burst.unwrap_or(rps.max(10));
            let map = PerCallerLimiterMap::new(rps, burst, Duration::from_secs(300));
            return RateLimitInterceptor::with_per_caller(service, map, rps, burst);
        }
        RateLimitInterceptor::from_rps(service, self.client_rps, self.burst)
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

/// A token-bucket rate limiter backed by a Semaphore.
///
/// Permits are **forgotten** on acquisition (never returned) and
/// replenished at a fixed rate by a background tokio task. The pool
/// caps at `burst` to prevent unbounded accumulation during idle
/// periods.
#[derive(Clone)]
pub struct RateLimiter {
    semaphore: Arc<Semaphore>,
    #[allow(dead_code)]
    burst: usize,
}

impl RateLimiter {
    /// Create a new limiter with the given rate (requests per second)
    /// and burst size (max concurrent requests at any one moment).
    ///
    /// Spawns a background tokio task that refills permits. Must be
    /// called from within a tokio runtime context.
    pub fn new(rps: u32, burst: u32) -> Self {
        let burst = burst.max(1) as usize;
        let semaphore = Arc::new(Semaphore::new(burst));
        let sem = Arc::clone(&semaphore);

        let interval_ns = if rps > 0 {
            1_000_000_000u64 / rps as u64
        } else {
            u64::MAX
        };
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_nanos(interval_ns.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if Arc::strong_count(&sem) == 1 {
                    break;
                }
                let current = sem.available_permits();
                if current < burst {
                    sem.add_permits(1);
                }
            }
        });

        Self { semaphore, burst }
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
}

impl PerCallerLimiterMap {
    /// Create a new map with the given per-caller rate, burst, and TTL.
    ///
    /// Spawns a background tokio task that evicts entries idle for longer
    /// than `ttl` every `ttl / 2` seconds (minimum 30 s). The task stops
    /// automatically when all `Arc` clones of this map are dropped.
    pub fn new(per_caller_rps: u32, per_caller_burst: u32, ttl: Duration) -> Self {
        let entries: Arc<std::sync::RwLock<HashMap<String, CallerEntry>>> =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let entries_weak = Arc::downgrade(&entries);
        let gc_interval = (ttl / 2).max(Duration::from_secs(30));

        tokio::spawn(async move {
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
        .and_then(|s| s.strip_prefix("Bearer "))
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
        let map = PerCallerLimiterMap::new(1, 1, Duration::from_millis(50));
        assert!(map.try_acquire("stale"));
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
}
