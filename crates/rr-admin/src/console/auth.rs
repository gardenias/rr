use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use subtle::ConstantTimeEq;

pub(crate) const SESSION_COOKIE: &str = "rr_console_session";

#[derive(Clone, Debug)]
pub(crate) struct Session {
    pub token: String,
    pub csrf_token: String,
    pub expires_at: SystemTime,
}

#[derive(Serialize)]
pub(crate) struct LoginResponse {
    pub csrf_token: String,
    pub expires_at: String,
}

impl From<&Session> for LoginResponse {
    fn from(session: &Session) -> Self {
        Self {
            csrf_token: session.csrf_token.clone(),
            expires_at: DateTime::<Utc>::from(session.expires_at).to_rfc3339(),
        }
    }
}

pub(crate) struct AuthState {
    viewer_token: Option<Arc<str>>,
    ttl: Duration,
    secure_cookie: bool,
    sessions: Mutex<HashMap<String, Session>>,
}

impl AuthState {
    pub fn new(viewer_token: Option<Arc<str>>, ttl: Duration, secure_cookie: bool) -> Arc<Self> {
        Arc::new(Self {
            viewer_token,
            ttl,
            secure_cookie,
            sessions: Mutex::new(HashMap::new()),
        })
    }

    pub fn enabled(&self) -> bool {
        self.viewer_token.is_some()
    }

    pub fn secure_cookie(&self) -> bool {
        self.secure_cookie
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn login(&self, candidate: &str, now: SystemTime) -> Option<Session> {
        let expected = self.viewer_token.as_ref()?;
        if expected.as_bytes().ct_eq(candidate.as_bytes()).unwrap_u8() != 1 {
            return None;
        }
        let session = Session {
            token: uuid::Uuid::new_v4().to_string(),
            csrf_token: uuid::Uuid::new_v4().to_string(),
            expires_at: now + self.ttl,
        };
        let mut sessions = self.sessions.lock().expect("session lock");
        sessions.retain(|_, existing| existing.expires_at > now);
        sessions.insert(session.token.clone(), session.clone());
        Some(session)
    }

    pub fn validate(&self, token: &str, now: SystemTime) -> Option<Session> {
        let mut sessions = self.sessions.lock().expect("session lock");
        if sessions
            .get(token)
            .is_some_and(|session| session.expires_at <= now)
        {
            sessions.remove(token);
            return None;
        }
        sessions.get(token).cloned()
    }

    pub fn revoke(&self, token: &str) {
        self.sessions.lock().expect("session lock").remove(token);
    }

    pub fn csrf_matches(&self, session: &Session, candidate: Option<&str>) -> bool {
        candidate.is_some_and(|candidate| {
            session
                .csrf_token
                .as_bytes()
                .ct_eq(candidate.as_bytes())
                .unwrap_u8()
                == 1
        })
    }
}

pub(crate) fn session_cookie(token: &str, ttl: Duration, secure: bool) -> String {
    format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{}",
        ttl.as_secs(),
        if secure { "; Secure" } else { "" }
    )
}

pub(crate) fn clear_session_cookie(secure: bool) -> String {
    session_cookie("", Duration::ZERO, secure)
}

pub(crate) fn session_token(cookie: Option<&str>) -> Option<&str> {
    cookie?.split(';').find_map(|pair| {
        let (name, value) = pair.trim().split_once('=')?;
        (name == SESSION_COOKIE && !value.is_empty()).then_some(value)
    })
}

pub(crate) fn origin_matches(origin: Option<&str>, host: &str, tls: bool) -> bool {
    let scheme = if tls { "https" } else { "http" };
    origin.is_some_and(|origin| origin == format!("{scheme}://{host}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn viewer_login_uses_constant_time_comparison_and_issues_independent_secrets() {
        let auth = AuthState::new(
            Some(Arc::from("viewer-secret")),
            Duration::from_secs(60),
            false,
        );
        assert!(auth.login("wrong", SystemTime::now()).is_none());
        let session = auth.login("viewer-secret", SystemTime::now()).unwrap();
        assert_ne!(session.token, session.csrf_token);
        assert!(session.token.len() >= 32);
        let cookie = session_cookie(&session.token, Duration::from_secs(60), false);
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Strict"));
        assert!(!cookie.contains("Secure"));
        let secure = session_cookie(&session.token, Duration::from_secs(60), true);
        assert!(secure.contains("Secure"));
    }

    #[test]
    fn sessions_expire_and_can_be_revoked() {
        let auth = AuthState::new(
            Some(Arc::from("viewer-secret")),
            Duration::from_millis(10),
            false,
        );
        let now = SystemTime::now();
        let session = auth.login("viewer-secret", now).unwrap();
        assert!(auth.validate(&session.token, now).is_some());
        assert!(auth
            .validate(&session.token, now + Duration::from_millis(11))
            .is_none());
        let session = auth.login("viewer-secret", now).unwrap();
        auth.revoke(&session.token);
        assert!(auth.validate(&session.token, now).is_none());
    }

    #[test]
    fn reconcile_origin_must_match_the_actual_listener_scheme_and_host() {
        assert!(origin_matches(
            Some("http://127.0.0.1:8080"),
            "127.0.0.1:8080",
            false
        ));
        assert!(origin_matches(
            Some("https://console.example"),
            "console.example",
            true
        ));
        assert!(!origin_matches(None, "127.0.0.1:8080", false));
        assert!(!origin_matches(
            Some("https://attacker.invalid"),
            "127.0.0.1:8080",
            false
        ));
    }
}
