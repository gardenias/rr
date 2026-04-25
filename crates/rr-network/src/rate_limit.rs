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

use std::sync::Arc;
use std::time::Duration;

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
}

impl RateLimitConfig {
    /// Build an interceptor suitable for AdminService. Returns a
    /// pass-through interceptor if `admin_rps` is `None`.
    pub fn admin_interceptor(&self) -> RateLimitInterceptor {
        RateLimitInterceptor::from_rps(ServiceLabel::Admin, self.admin_rps, self.burst)
    }

    /// Build an interceptor suitable for ClientService.
    pub fn client_interceptor(&self) -> RateLimitInterceptor {
        RateLimitInterceptor::from_rps(ServiceLabel::Client, self.client_rps, self.burst)
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

/// A tonic interceptor that enforces a [`RateLimiter`]. When `limiter`
/// is `None`, the interceptor is a pass-through (disabled mode).
///
/// Every reject is emitted to the audit log
/// ([`crate::audit::AUDIT_TARGET`]) with `op="rate_limit"` plus peer /
/// service so operators can detect noisy clients and runaway scripts.
#[derive(Clone)]
pub struct RateLimitInterceptor {
    limiter: Option<RateLimiter>,
    service: ServiceLabel,
}

impl RateLimitInterceptor {
    fn from_rps(service: ServiceLabel, rps: Option<u32>, burst: Option<u32>) -> Self {
        let limiter = rps.map(|r| {
            let b = burst.unwrap_or(r.max(10));
            RateLimiter::new(r, b)
        });
        Self { limiter, service }
    }

    /// Disabled constructor (mostly for tests or explicit opt-out).
    pub fn disabled(service: ServiceLabel) -> Self {
        Self {
            limiter: None,
            service,
        }
    }

    /// Enabled constructor with a custom limiter.
    pub fn with_limiter(service: ServiceLabel, limiter: RateLimiter) -> Self {
        Self {
            limiter: Some(limiter),
            service,
        }
    }
}

impl Interceptor for RateLimitInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let Some(limiter) = &self.limiter else {
            return Ok(request);
        };
        if limiter.try_acquire() {
            Ok(request)
        } else {
            let peer = request
                .remote_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|| "unknown".to_string());
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
}
