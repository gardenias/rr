use super::auth::{
    clear_session_cookie, origin_matches, session_cookie, session_token, AuthState, LoginResponse,
    Session,
};
use super::cache::{CacheView, CachedNode, ConsoleCache};
use super::connection_manager::{ConnectionManager, ConnectionRuntime, PublicConnectionError};
use super::metrics::{HistoryFamily, HistoryQuery};
use super::model::{
    rfc3339, Capability, ConnectionProfile, ConnectionSecrets, Envelope, ErrorBody, ErrorEnvelope,
    GroupObservation, ResponseMeta, SecretInput, SourceMeta, TimeSeriesPoint, TlsProfile,
};
use super::pagination::{paginate, PageQuery};
use super::scheduler::Scheduler;
use axum::body::{Body, Bytes};
use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::header::{
    CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HOST, ORIGIN, SET_COOKIE,
};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use futures::{stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;
use std::path::{Component, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;

const ON_DEMAND_HANDLER_DEADLINE: Duration = Duration::from_secs(2);
const ON_DEMAND_CONCURRENCY: usize = 8;
const NODE_TOPOLOGY_EDGE_WINDOW: Duration = Duration::from_secs(300);
const MAX_NODE_TOPOLOGY_EDGES: usize = 200;
const MAX_NODE_TOPOLOGY_GROUPS: usize = 200;

#[derive(Clone, serde::Serialize)]
struct GroupProjection {
    group_id: u64,
    health: String,
    leader_node_id: Option<u64>,
    current_term: Option<u64>,
    last_log_index: Option<u64>,
    expected_replica_node_ids: Vec<u64>,
    voters_reachable: usize,
    reachable_voters: usize,
    voters_total: Option<usize>,
    source_count: usize,
    observations: Vec<GroupObservation>,
    conflicting_views: Option<Vec<GroupObservation>>,
}

impl GroupProjection {
    fn from_value(value: &Value) -> Self {
        let voters_reachable = value["voters_reachable"]
            .as_u64()
            .expect("reachable voters") as usize;
        Self {
            group_id: value["group_id"].as_u64().expect("group ID"),
            health: value["health"].as_str().expect("group health").to_string(),
            leader_node_id: value["leader_node_id"].as_u64(),
            current_term: value["current_term"].as_u64(),
            last_log_index: value["last_log_index"].as_u64(),
            expected_replica_node_ids: value["expected_replica_node_ids"]
                .as_array()
                .expect("expected replicas")
                .iter()
                .map(|node_id| node_id.as_u64().expect("replica node ID"))
                .collect(),
            voters_reachable,
            reachable_voters: voters_reachable,
            voters_total: value["voters_total"].as_u64().map(|total| total as usize),
            source_count: value["source_count"].as_u64().expect("group source count") as usize,
            observations: group_observations(&value["observations"]),
            conflicting_views: value["conflicting_views"].as_array().map(|views| {
                views
                    .iter()
                    .map(|view| GroupProjection::observation(view))
                    .collect()
            }),
        }
    }

    fn observation(value: &Value) -> GroupObservation {
        GroupObservation {
            node_id: value["node_id"].as_u64().expect("observation node ID"),
            group_id: value["group_id"].as_u64().expect("observation group ID"),
            is_leader: value["is_leader"]
                .as_bool()
                .expect("observation leader role"),
            current_term: value["current_term"].as_u64().expect("observation term"),
            last_log_index: value["last_log_index"]
                .as_u64()
                .expect("observation last log index"),
            leader_node_id: value["leader_node_id"].as_u64(),
            observed_at: value["observed_at"]
                .as_str()
                .expect("observation timestamp")
                .to_string(),
        }
    }
}

fn group_observations(value: &Value) -> Vec<GroupObservation> {
    value
        .as_array()
        .expect("group observations")
        .iter()
        .map(GroupProjection::observation)
        .collect()
}

fn group_views(view: &CacheView) -> Vec<Value> {
    view.group_views()
        .into_iter()
        .map(|mut group| {
            group["voters_reachable"] = group["reachable_voters"].clone();
            group
        })
        .collect()
}

#[derive(Clone)]
pub(crate) struct SummaryProjection {
    generation: u64,
    serialized: Bytes,
    groups: Arc<Vec<GroupProjection>>,
    meta: ResponseMeta,
    stream_serialized: Bytes,
}

impl SummaryProjection {
    fn meta(&self, window: Option<String>) -> ResponseMeta {
        let mut meta = self.meta.clone();
        meta.correlation_id = uuid::Uuid::new_v4().to_string();
        meta.generated_at = rfc3339(SystemTime::now());
        meta.time_window = window;
        meta
    }
}

#[derive(Clone)]
pub(crate) struct AppState {
    manager: Arc<ConnectionManager>,
    legacy_connection: Option<String>,
    auth: Arc<AuthState>,
    static_dir: PathBuf,
    using_tls: bool,
}

impl AppState {
    pub fn new(
        manager: Arc<ConnectionManager>,
        auth: Arc<AuthState>,
        static_dir: PathBuf,
        using_tls: bool,
        legacy_connection: Option<String>,
    ) -> Self {
        Self {
            manager,
            legacy_connection,
            auth,
            static_dir,
            using_tls,
        }
    }

    fn runtime_state(&self, id: &str) -> Result<RuntimeState, PublicConnectionError> {
        let runtime = self.manager.resolve(id)?;
        Ok(RuntimeState {
            scheduler: runtime.scheduler.clone(),
            runtime,
        })
    }
}

#[derive(Clone)]
struct RuntimeState {
    scheduler: Arc<Scheduler>,
    runtime: Arc<ConnectionRuntime>,
}

impl FromRequestParts<AppState> for RuntimeState {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let Path(parameters) = Path::<HashMap<String, String>>::from_request_parts(parts, state)
            .await
            .map_err(|_| {
                neutral_error(
                    StatusCode::BAD_REQUEST,
                    "INVALID_ARGUMENT",
                    "the request path could not be decoded",
                )
            })?;
        let id = match parameters.get("id") {
            Some(id) => id,
            None => state.legacy_connection.as_ref().ok_or_else(|| {
                neutral_error(
                    StatusCode::BAD_REQUEST,
                    "CONNECTION_REQUIRED",
                    "Legacy routes require an ephemeral startup connection",
                )
            })?,
        };
        state.runtime_state(id).map_err(public_connection_error)
    }
}

pub(crate) fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/api/v1/console/connections",
            get(list_connections).post(create_connection),
        )
        .route(
            "/api/v1/console/connections/test",
            post(test_connection_candidate),
        )
        .route(
            "/api/v1/console/connections/{id}",
            get(get_connection)
                .patch(update_connection)
                .delete(delete_connection),
        )
        .route(
            "/api/v1/console/connections/{id}/test",
            post(test_connection),
        )
        .route(
            "/api/v1/console/connections/{id}/connect",
            post(connect_connection),
        )
        .route(
            "/api/v1/console/connections/{id}/disconnect",
            post(disconnect_connection),
        )
        .route("/api/v1/console/connections/{id}/summary", get(summary))
        .route("/api/v1/console/connections/{id}/nodes", get(nodes))
        .route(
            "/api/v1/console/connections/{id}/nodes/{node_id}",
            get(node),
        )
        .route(
            "/api/v1/console/connections/{id}/nodes/{node_id}/boots",
            get(node_boots),
        )
        .route("/api/v1/console/connections/{id}/topology", get(topology))
        .route("/api/v1/console/connections/{id}/groups", get(groups))
        .route(
            "/api/v1/console/connections/{id}/groups/{group_id}",
            get(group),
        )
        .route(
            "/api/v1/console/connections/{id}/groups/{group_id}/replicas",
            get(group_replicas),
        )
        .route(
            "/api/v1/console/connections/{id}/metrics/performance",
            get(performance),
        )
        .route("/api/v1/console/connections/{id}/snapshots", get(snapshots))
        .route("/api/v1/console/connections/{id}/lifecycle", get(lifecycle))
        .route("/api/v1/console/connections/{id}/events", get(events))
        .route("/api/v1/console/connections/{id}/sources", get(sources))
        .route(
            "/api/v1/console/connections/{id}/sources/reconcile",
            post(reconcile),
        )
        .route("/api/v1/console/connections/{id}/stream", get(stream))
        .route("/api/v1/console/summary", get(summary))
        .route("/api/v1/console/nodes", get(nodes))
        .route("/api/v1/console/nodes/{node_id}", get(node))
        .route("/api/v1/console/nodes/{node_id}/boots", get(node_boots))
        .route("/api/v1/console/topology", get(topology))
        .route("/api/v1/console/groups", get(groups))
        .route("/api/v1/console/groups/{group_id}", get(group))
        .route(
            "/api/v1/console/groups/{group_id}/replicas",
            get(group_replicas),
        )
        .route("/api/v1/console/metrics/performance", get(performance))
        .route("/api/v1/console/snapshots", get(snapshots))
        .route("/api/v1/console/lifecycle", get(lifecycle))
        .route("/api/v1/console/events", get(events))
        .route("/api/v1/console/sources", get(sources))
        .route("/api/v1/console/sources/reconcile", post(reconcile))
        .route("/api/v1/console/stream", get(stream))
        .route("/stream", get(stream))
        .route("/api/v1/console/auth/login", post(login))
        .route("/api/v1/console/auth/csrf", get(csrf))
        .route("/api/v1/console/auth/logout", post(logout))
        .route("/api/v1/console", any(api_not_found))
        .route("/api/v1/console/{*path}", any(api_not_found))
        .fallback(static_or_spa)
        .layer(axum::middleware::from_fn(security_headers))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authenticate,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            uniform_api_errors,
        ))
        .with_state(state)
}

async fn security_headers(request: Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'",
        ),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn uniform_api_errors(
    State(_state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let is_api = request.uri().path().starts_with("/api/");
    let response = next.run(request).await;
    if !is_api
        || !(response.status().is_client_error() || response.status().is_server_error())
        || response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        return response;
    }
    let status = response.status();
    let (code, message) = match status {
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            ("INVALID_ARGUMENT", "the request could not be decoded")
        }
        StatusCode::METHOD_NOT_ALLOWED => (
            "METHOD_NOT_ALLOWED",
            "method not allowed for this Console API route",
        ),
        StatusCode::PAYLOAD_TOO_LARGE => (
            "PAYLOAD_TOO_LARGE",
            "request body exceeds the Console limit",
        ),
        status if status.is_server_error() => ("INTERNAL_ERROR", "the Console request failed"),
        _ => ("REQUEST_REJECTED", "the Console request was rejected"),
    };
    neutral_error(status, code, message)
}

async fn authenticate(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let protected_mutation = is_protected_mutation(path, request.method());
    if !state.auth.enabled() || is_public_path(path) {
        if protected_mutation && !valid_origin(&state, request.headers()) {
            return neutral_error(
                StatusCode::FORBIDDEN,
                "ORIGIN_REJECTED",
                "same-origin mutation is required",
            );
        }
        return next.run(request).await;
    }
    let cookie = request
        .headers()
        .get(COOKIE)
        .and_then(|value| value.to_str().ok());
    let session =
        session_token(cookie).and_then(|token| state.auth.validate(token, SystemTime::now()));
    let Some(session) = session else {
        return neutral_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHENTICATED",
            "a valid viewer session is required",
        );
    };
    if protected_mutation {
        if !valid_origin(&state, request.headers()) {
            return neutral_error(
                StatusCode::FORBIDDEN,
                "ORIGIN_REJECTED",
                "same-origin mutation is required",
            );
        }
        let csrf = request
            .headers()
            .get("x-console-csrf")
            .and_then(|value| value.to_str().ok());
        if !state.auth.csrf_matches(&session, csrf) {
            return neutral_error(
                StatusCode::FORBIDDEN,
                "CSRF_REJECTED",
                "a valid X-Console-CSRF header is required",
            );
        }
    }
    request.extensions_mut().insert(session);
    next.run(request).await
}

fn is_protected_mutation(path: &str, method: &Method) -> bool {
    let segments = path.split('/').collect::<Vec<_>>();
    match (method, segments.as_slice()) {
        (&Method::POST, ["", "api", "v1", "console", "connections"])
        | (&Method::POST, ["", "api", "v1", "console", "connections", "test"])
        | (&Method::POST, ["", "api", "v1", "console", "sources", "reconcile"])
        | (&Method::POST, ["", "api", "v1", "console", "auth", "logout"])
        | (&Method::PATCH, ["", "api", "v1", "console", "connections", _])
        | (&Method::DELETE, ["", "api", "v1", "console", "connections", _])
        | (&Method::POST, ["", "api", "v1", "console", "connections", _, "test"])
        | (&Method::POST, ["", "api", "v1", "console", "connections", _, "connect"])
        | (&Method::POST, ["", "api", "v1", "console", "connections", _, "disconnect"])
        | (&Method::POST, ["", "api", "v1", "console", "connections", _, "sources", "reconcile"]) => {
            true
        }
        _ => false,
    }
}

fn valid_origin(state: &AppState, headers: &HeaderMap) -> bool {
    let host = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let origin = headers.get(ORIGIN).and_then(|value| value.to_str().ok());
    origin_matches(origin, host, state.using_tls)
}

fn is_public_path(path: &str) -> bool {
    if path == "/api/v1/console/auth/login" {
        return true;
    }
    !path.starts_with("/api/") && path != "/stream"
}

#[derive(Deserialize)]
struct LoginRequest {
    token: String,
}

async fn login(State(state): State<AppState>, Json(request): Json<LoginRequest>) -> Response {
    let Some(session) = state.auth.login(&request.token, SystemTime::now()) else {
        return neutral_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHENTICATED",
            "viewer authentication failed",
        );
    };
    let cookie = session_cookie(&session.token, state.auth.ttl(), state.auth.secure_cookie());
    let login = LoginResponse::from(&session);
    let mut response = Json(json!({
        "csrf_token": login.csrf_token,
        "expires_at": login.expires_at,
        "meta": neutral_meta(),
    }))
    .into_response();
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_str(&cookie).expect("session cookie header"),
    );
    response
}

async fn csrf(State(state): State<AppState>, request: Request<Body>) -> Response {
    if !state.auth.enabled() {
        return Json(json!({
            "csrf_token": Value::Null,
            "expires_at": Value::Null,
            "meta": neutral_meta(),
        }))
        .into_response();
    }
    let Some(session) = request.extensions().get::<Session>() else {
        return neutral_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHENTICATED",
            "a valid viewer session is required",
        );
    };
    let current = LoginResponse::from(session);
    Json(json!({
        "csrf_token": current.csrf_token,
        "expires_at": current.expires_at,
        "meta": neutral_meta(),
    }))
    .into_response()
}

async fn logout(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(token) = headers
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookie| session_token(Some(cookie)))
    {
        state.auth.revoke(token);
    }
    let mut response = neutral_ok(json!({"revoked": true}));
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_str(&clear_session_cookie(state.auth.secure_cookie()))
            .expect("clear cookie header"),
    );
    response
}

async fn reconcile(state: RuntimeState) -> Response {
    state.scheduler.reconcile().await;
    let view = state.scheduler.cache.view();
    ok(
        &view,
        json!({"reconciled": true, "generation": view.generation}),
    )
}

#[derive(Deserialize)]
struct ConnectionInput {
    name: String,
    #[serde(default)]
    environment: Option<String>,
    #[serde(default)]
    admin_seeds: Vec<super::config::SeedPeer>,
    #[serde(default)]
    discovery: Option<String>,
    #[serde(default)]
    cluster_name: Option<String>,
    #[serde(default)]
    admin_tls: TlsProfile,
    #[serde(default)]
    discovery_tls: TlsProfile,
    #[serde(default)]
    discovery_username: Option<String>,
    #[serde(default)]
    discovery_allow_insecure: bool,
    #[serde(default)]
    prometheus: Option<super::model::PrometheusProfile>,
    #[serde(default = "default_poll_interval_secs")]
    poll_interval_secs: u64,
    #[serde(default)]
    admin_token: SecretInput,
    #[serde(default)]
    discovery_password: SecretInput,
    #[serde(default)]
    remember: bool,
}

fn default_poll_interval_secs() -> u64 {
    5
}

impl ConnectionInput {
    fn profile(&self) -> ConnectionProfile {
        ConnectionProfile {
            id: String::new(),
            name: self.name.clone(),
            environment: self.environment.clone(),
            admin_seeds: self.admin_seeds.clone(),
            discovery: self.discovery.clone(),
            cluster_name: self.cluster_name.clone(),
            admin_tls: self.admin_tls.clone(),
            discovery_tls: self.discovery_tls.clone(),
            discovery_username: self.discovery_username.clone(),
            discovery_allow_insecure: self.discovery_allow_insecure,
            prometheus: self.prometheus.clone(),
            poll_interval_secs: self.poll_interval_secs,
            created_at: String::new(),
            updated_at: String::new(),
            observed_cluster_id: None,
            last_connected_at: None,
        }
    }

    fn secrets(&self) -> ConnectionSecrets {
        ConnectionSecrets {
            admin_token: self.admin_token.clone(),
            discovery_password: self.discovery_password.clone(),
        }
    }
}

async fn list_connections(State(state): State<AppState>) -> Response {
    connection_result(state.manager.list().map(|items| {
        json!({
            "items": items,
            "keychain_available": state.manager.keychain_available(),
        })
    }))
}

async fn create_connection(
    State(state): State<AppState>,
    Json(input): Json<ConnectionInput>,
) -> Response {
    match state
        .manager
        .create(input.profile(), input.secrets(), input.remember)
        .await
    {
        Ok(summary) => (StatusCode::CREATED, neutral_ok(summary)).into_response(),
        Err(error) => public_connection_error(error),
    }
}

async fn get_connection(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    connection_result(state.manager.get(&id))
}

async fn update_connection(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<ConnectionInput>,
) -> Response {
    connection_result(
        state
            .manager
            .update(&id, input.profile(), input.secrets(), input.remember)
            .await,
    )
}

async fn delete_connection(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    if let Err(error) = state.manager.get(&id) {
        return public_connection_error(error);
    }
    match state.manager.remove(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => public_connection_error(error),
    }
}

#[derive(Deserialize)]
struct ConnectionCandidateInput {
    #[serde(flatten)]
    input: ConnectionInput,
    #[serde(default)]
    existing_connection_id: Option<String>,
}

async fn test_connection_candidate(
    State(state): State<AppState>,
    Json(candidate): Json<ConnectionCandidateInput>,
) -> Response {
    connection_result(
        state
            .manager
            .test_candidate_with_existing(
                candidate.input.profile(),
                candidate.input.secrets(),
                candidate.existing_connection_id.as_deref(),
            )
            .await
            .map(|()| json!({"tested": true})),
    )
}

async fn test_connection(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    connection_result(
        state
            .manager
            .test(&id)
            .await
            .map(|()| json!({"tested": true})),
    )
}

async fn connect_connection(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    connection_result(state.manager.connect(&id).await)
}

async fn disconnect_connection(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    connection_result(state.manager.disconnect(&id).await)
}

fn connection_result<T: serde::Serialize>(result: Result<T, PublicConnectionError>) -> Response {
    match result {
        Ok(data) => neutral_ok(data),
        Err(error) => public_connection_error(error),
    }
}

fn public_connection_error(error: PublicConnectionError) -> Response {
    let status = match error.code {
        "CONNECTION_REQUIRED" | "INVALID_CONNECTION_PROFILE" => StatusCode::BAD_REQUEST,
        "CONNECTION_NOT_FOUND" => StatusCode::NOT_FOUND,
        "CONNECTION_NOT_CONNECTED" | "CONNECTION_ACTIVE" | "CREDENTIALS_REQUIRED" => {
            StatusCode::CONFLICT
        }
        "ACTIVE_CONNECTION_LIMIT" => StatusCode::TOO_MANY_REQUESTS,
        "CONNECTION_INITIALIZATION_FAILED" | "CONNECTION_TEST_FAILED" => StatusCode::BAD_GATEWAY,
        "CONNECTION_SHUTTING_DOWN" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    neutral_error(status, error.code, error.message)
}

async fn summary(state: RuntimeState, Query(query): Query<WindowQuery>) -> Response {
    let projection = summary_projection(&state).await;
    summary_response(&projection, query.window)
}

async fn summary_projection(state: &RuntimeState) -> SummaryProjection {
    let generation = state.scheduler.cache.generation();
    let mut cached = state.runtime.summary_projection.lock().await;
    if let Some(projection) = cached
        .as_ref()
        .filter(|projection| projection.generation == generation)
    {
        return projection.clone();
    }
    let view = state.scheduler.cache.view();
    let data = summary_data(&view);
    let groups = data["groups"]
        .as_array()
        .expect("summary groups array")
        .iter()
        .map(GroupProjection::from_value)
        .collect::<Vec<_>>();
    let projection = SummaryProjection {
        generation: view.generation,
        serialized: Bytes::from(serde_json::to_vec(&data).expect("summary JSON")),
        groups: Arc::new(groups),
        meta: view.meta(),
        stream_serialized: Bytes::from(
            serde_json::to_vec(&summary_stream_data(&data)).expect("summary stream JSON"),
        ),
    };
    *cached = Some(projection.clone());
    projection
}

fn summary_envelope(projection: &SummaryProjection, window: Option<String>) -> String {
    let meta = serde_json::to_string(&projection.meta(window)).expect("summary metadata JSON");
    let mut body = String::with_capacity(projection.serialized.len() + meta.len() + 17);
    body.push_str("{\"data\":");
    body.push_str(std::str::from_utf8(&projection.serialized).expect("summary JSON must be UTF-8"));
    body.push_str(",\"meta\":");
    body.push_str(&meta);
    body.push('}');
    body
}

fn summary_stream_data(data: &Value) -> Value {
    let mut data = data.clone();
    if let Some(groups) = data["groups"].as_array_mut() {
        for group in groups {
            if let Some(group) = group.as_object_mut() {
                group.remove("observations");
                group.remove("conflicting_views");
            }
        }
    }
    data
}

fn summary_stream_envelope(projection: &SummaryProjection) -> String {
    let meta = serde_json::to_string(&projection.meta(None)).expect("summary metadata JSON");
    let mut body = String::with_capacity(projection.stream_serialized.len() + meta.len() + 17);
    body.push_str("{\"data\":");
    body.push_str(
        std::str::from_utf8(&projection.stream_serialized)
            .expect("summary stream JSON must be UTF-8"),
    );
    body.push_str(",\"meta\":");
    body.push_str(&meta);
    body.push('}');
    body
}

fn summary_response(projection: &SummaryProjection, window: Option<String>) -> Response {
    if let Some(window) = window
        .as_ref()
        .filter(|window| matches!(window.as_str(), "15m" | "1h" | "6h" | "24h"))
    {
        let body = Bytes::from(summary_envelope(projection, Some(window.clone())));
        let content_length = body.len();
        let mut response = body.into_response();
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        response.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&content_length.to_string()).expect("summary content length"),
        );
        return response;
    }
    let meta =
        Bytes::from(serde_json::to_vec(&projection.meta(window)).expect("summary metadata JSON"));
    let chunks = [
        Bytes::from_static(b"{\"data\":"),
        projection.serialized.clone(),
        Bytes::from_static(b",\"meta\":"),
        meta,
        Bytes::from_static(b"}"),
    ];
    let content_length = chunks.iter().map(Bytes::len).sum::<usize>();
    let body = Body::from_stream(stream::iter(
        chunks.into_iter().map(Ok::<Bytes, Infallible>),
    ));
    let mut response = body.into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string()).expect("summary content length"),
    );
    response
}

fn summary_data(view: &CacheView) -> Value {
    let groups = group_views(view);
    let online = view
        .sources
        .iter()
        .filter(|source| source.endpoint_role == "ADMIN" && source.status == "ONLINE")
        .count();
    let with_leader = groups
        .iter()
        .filter(|group| !group["leader_node_id"].is_null())
        .count();
    let groups_total = groups.len();
    json!({
        "cluster_id": view.cluster_id,
        "quarantined": view.quarantined,
        "nodes": view.nodes.iter().map(node_json).collect::<Vec<_>>(),
        "groups": groups,
        "formation": view.formation().map(|formation| json!({
            "origin": formation.origin,
            "cluster_uuid": formation.cluster_uuid,
            "topology_revision": formation.topology_revision,
            "store_revision": formation.store_revision,
            "state": formation.state,
        })),
        "totals": {
            "nodes_online": online,
            "nodes_total": view.nodes.len(),
            "groups_with_leader": with_leader,
            "groups_total": groups_total,
            "groups_quorum_at_risk": groups.iter().filter(|group| group["health"] == "QUORUM_AT_RISK").count(),
        },
        "recent_events": view.events.iter().rev().take(20).collect::<Vec<_>>(),
    })
}

#[derive(Deserialize)]
struct WindowQuery {
    window: Option<String>,
}

#[derive(Deserialize)]
struct NodesQuery {
    #[serde(flatten)]
    page: PageQuery,
    filter: Option<String>,
}

async fn nodes(state: RuntimeState, Query(query): Query<NodesQuery>) -> Response {
    let view = state.scheduler.cache.view();
    let filter = query.filter.unwrap_or_default().to_ascii_lowercase();
    let items = view
        .nodes
        .iter()
        .filter(|node| {
            filter.is_empty()
                || node.peer.node_id.to_string().contains(&filter)
                || node.peer.admin_addr.to_ascii_lowercase().contains(&filter)
                || node.sources[0]
                    .status
                    .to_ascii_lowercase()
                    .contains(&filter)
        })
        .map(node_json)
        .collect::<Vec<_>>();
    page_response(&view, &items, query.page)
}

#[derive(Deserialize)]
struct NodePath {
    node_id: u64,
    #[serde(rename = "id", default)]
    _connection_id: Option<String>,
}

async fn node(state: RuntimeState, Path(path): Path<NodePath>) -> Response {
    let node_id = path.node_id;
    let view = state.scheduler.cache.view();
    let Some(node) = view.nodes.iter().find(|node| node.peer.node_id == node_id) else {
        return error_response(
            &state.scheduler.cache,
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "node not found",
        );
    };
    let groups = group_views(&view)
        .into_iter()
        .filter(|group| {
            group["observations"]
                .as_array()
                .is_some_and(|observations| {
                    observations
                        .iter()
                        .any(|observation| observation["node_id"] == node_id)
                })
        })
        .collect::<Vec<_>>();
    ok(&view, json!({"node": node_json(node), "groups": groups}))
}

async fn node_boots(
    state: RuntimeState,
    Path(path): Path<NodePath>,
    Query(page): Query<PageQuery>,
) -> Response {
    let node_id = path.node_id;
    let cache = state.scheduler.cache.clone();
    with_handler_deadline(&cache, node_boots_inner(state, node_id, page)).await
}

async fn node_boots_inner(state: RuntimeState, node_id: u64, page: PageQuery) -> Response {
    let view = state.scheduler.cache.view();
    if !view.nodes.iter().any(|node| node.peer.node_id == node_id) {
        return error_response(
            &state.scheduler.cache,
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "node not found",
        );
    }
    let page_size = page.page_size.unwrap_or(50);
    match state
        .scheduler
        .list_boot_records(node_id, page.cursor, page_size)
        .await
    {
        Ok(history) => {
            let items = history
                .records
                .iter()
                .map(|record| boot_record_json(record, Some(&history.persistence)))
                .collect::<Vec<_>>();
            ok(
                &view,
                json!({
                    "items": items,
                    "next_cursor": (!history.next_cursor.is_empty()).then_some(history.next_cursor),
                    "persistence": history.persistence,
                }),
            )
        }
        Err(error) if error.code == "UNIMPLEMENTED" => {
            let mut view = view.clone();
            view.partial = true;
            view.capabilities.push(Capability::unsupported(
                "boot_history",
                "legacy AdminService returned UNIMPLEMENTED for ListBootRecords",
            ));
            ok(
                &view,
                json!({"items": [], "next_cursor": Value::Null, "persistence": Value::Null}),
            )
        }
        Err(error) if matches!(error.code, "INVALID_ARGUMENT" | "INVALID_UPSTREAM_CURSOR") => {
            error_response(
                &state.scheduler.cache,
                StatusCode::BAD_REQUEST,
                error.code,
                &error.message,
            )
        }
        Err(error) => error_response(
            &state.scheduler.cache,
            StatusCode::BAD_GATEWAY,
            error.code,
            &error.message,
        ),
    }
}

fn node_topology_edges(view: &CacheView) -> (Vec<Value>, bool) {
    let now = SystemTime::now();
    let cutoff = now
        .checked_sub(NODE_TOPOLOGY_EDGE_WINDOW)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut edges = BTreeMap::<(u64, u64), (BTreeSet<u64>, SystemTime)>::new();
    for node in view.nodes.iter() {
        for (group_id, detail) in &node.details {
            let Some(replica) = detail
                .replica
                .as_ref()
                .filter(|replica| replica.server_state == "Leader")
            else {
                continue;
            };
            for follower in &replica.leader_replication {
                let Some(observed_at) = follower
                    .last_heartbeat_unix_ms
                    .and_then(|timestamp| {
                        SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(timestamp))
                    })
                    .filter(|observed_at| *observed_at >= cutoff && *observed_at <= now)
                else {
                    continue;
                };
                if follower.node_id == node.peer.node_id {
                    continue;
                }
                let edge = edges
                    .entry((node.peer.node_id, follower.node_id))
                    .or_insert_with(|| (BTreeSet::new(), observed_at));
                edge.0.insert(*group_id);
                edge.1 = edge.1.max(observed_at);
            }
        }
    }
    let truncated = edges.len() > MAX_NODE_TOPOLOGY_EDGES;
    let edges = edges
        .into_iter()
        .take(MAX_NODE_TOPOLOGY_EDGES)
        .map(
            |((source_node_id, target_node_id), (groups, observed_at))| {
                json!({
                    "source_node_id": source_node_id,
                    "target_node_id": target_node_id,
                    "group_count": groups.len(),
                    "observed_at": rfc3339(observed_at),
                })
            },
        )
        .collect();
    (edges, truncated)
}

#[derive(Deserialize)]
struct TopologyQuery {
    view: Option<String>,
    group_id: Option<u64>,
}

async fn node_topology(state: RuntimeState, group_id: Option<u64>) -> Response {
    let initial = state.scheduler.cache.view();
    let mut requests = BTreeMap::<u64, u64>::new();
    for node in initial.nodes.iter().filter(|node| node.error.is_none()) {
        for group in node
            .snapshot
            .as_ref()
            .into_iter()
            .flat_map(|snapshot| snapshot.groups.iter().flatten())
            .filter(|group| group_id.is_none_or(|group_id| group.group_id == group_id))
        {
            if group.leader_node_id == Some(node.peer.node_id) {
                requests
                    .entry(group.group_id)
                    .and_modify(|leader| *leader = (*leader).min(node.peer.node_id))
                    .or_insert(node.peer.node_id);
            }
        }
    }
    let mut requests = requests
        .into_iter()
        .map(|(group_id, leader_node_id)| (leader_node_id, group_id))
        .collect::<Vec<_>>();
    let collection_truncated = requests.len() > MAX_NODE_TOPOLOGY_GROUPS;
    requests.truncate(MAX_NODE_TOPOLOGY_GROUPS);
    let results = state.scheduler.collect_groups(requests).await;

    let mut view = state.scheduler.cache.view();
    for (_, group_id, result) in results {
        if let Err(error) = result {
            view.partial = true;
            view.capabilities.push(Capability {
                feature: format!("node_topology_edges.group.{group_id}"),
                status: "ERROR".to_string(),
                reason: Some(format!("{}: {}", error.code, error.message)),
            });
        }
    }
    if collection_truncated {
        view.partial = true;
        view.capabilities.push(Capability {
            feature: "node_topology_edges".to_string(),
            status: "ON_DEMAND".to_string(),
            reason: Some(format!(
                "leader evidence is collected for at most {MAX_NODE_TOPOLOGY_GROUPS} groups per request"
            )),
        });
    }
    let (edges, edges_truncated) = node_topology_edges(&view);
    ok(
        &view,
        json!({
            "view": "nodes",
            "nodes": view.nodes.iter().map(node_json).collect::<Vec<_>>(),
            "edges": edges,
            "edges_truncated": edges_truncated,
            "collection_truncated": collection_truncated,
        }),
    )
}

async fn topology(state: RuntimeState, Query(query): Query<TopologyQuery>) -> Response {
    let kind = query.view.as_deref().unwrap_or("nodes");
    if kind == "raft" {
        let projection = summary_projection(&state).await;
        let mut groups = projection
            .groups
            .iter()
            .filter(|group| {
                query
                    .group_id
                    .is_none_or(|group_id| group.group_id == group_id)
            })
            .collect::<Vec<_>>();
        let total_groups = groups.len();
        if query.group_id.is_none() {
            groups.truncate(200);
        }
        let returned_groups = groups.len();
        return ok_with_meta(
            projection.meta(None),
            json!({
                "view": kind,
                "groups": groups,
                "truncated": returned_groups < total_groups,
                "total_groups": total_groups,
                "returned_groups": returned_groups,
            }),
        );
    }
    if kind == "nodes" {
        let cache = state.scheduler.cache.clone();
        return with_handler_deadline(&cache, node_topology(state, query.group_id)).await;
    }
    let view = state.scheduler.cache.view();
    let data = match kind {
        "routes" => {
            json!({
                "view": kind,
                "revision": view.topology_revision,
                "routes": view.routes().iter().map(route_json).collect::<Vec<_>>(),
            })
        }
        _ => {
            return error_response(
                &state.scheduler.cache,
                StatusCode::BAD_REQUEST,
                "INVALID_ARGUMENT",
                "view must be nodes, raft, or routes",
            )
        }
    };
    ok(&view, data)
}

async fn groups(state: RuntimeState, Query(query): Query<NodesQuery>) -> Response {
    let projection = summary_projection(&state).await;
    let filter = query.filter.unwrap_or_default();
    let items = projection
        .groups
        .iter()
        .filter(|group| filter.is_empty() || group.group_id.to_string().contains(&filter))
        .collect::<Vec<_>>();
    page_response_with_meta(projection.meta(None), &items, query.page)
}

#[derive(Deserialize)]
struct GroupPath {
    group_id: u64,
    #[serde(rename = "id", default)]
    _connection_id: Option<String>,
}

async fn group(state: RuntimeState, Path(path): Path<GroupPath>) -> Response {
    let group_id = path.group_id;
    let view = state.scheduler.cache.view();
    let Some(group) = group_views(&view)
        .into_iter()
        .find(|group| group["group_id"] == group_id)
    else {
        return error_response(
            &state.scheduler.cache,
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "group not found",
        );
    };
    ok(&view, group)
}

#[derive(Deserialize)]
struct ReplicaQuery {
    node_id: Option<u64>,
}

async fn group_replicas(
    state: RuntimeState,
    Path(path): Path<GroupPath>,
    Query(query): Query<ReplicaQuery>,
) -> Response {
    let group_id = path.group_id;
    let cache = state.scheduler.cache.clone();
    with_handler_deadline(&cache, async move {
        let initial = state.scheduler.cache.view();
        let node_ids = query
            .node_id
            .map(|node_id| vec![node_id])
            .unwrap_or_else(|| {
                initial
                    .nodes
                    .iter()
                    .filter(|node| {
                        node.snapshot.as_ref().is_some_and(|snapshot| {
                            snapshot
                                .groups
                                .iter()
                                .flatten()
                                .any(|group| group.group_id == group_id)
                        })
                    })
                    .map(|node| node.peer.node_id)
                    .collect()
            });
        let mut errors = state
            .scheduler
            .collect_groups(node_ids.into_iter().map(|node_id| (node_id, group_id)))
            .await
            .into_iter()
            .filter_map(|(node_id, _, result)| result.err().map(|error| (node_id, error)))
            .collect::<Vec<_>>();
        errors.sort_by_key(|(node_id, _)| *node_id);

        let mut view = state.scheduler.cache.view();
        for (node_id, error) in &errors {
            view.partial = true;
            view.capabilities.push(Capability {
                feature: format!("group_replica_status.node.{node_id}"),
                status: "ERROR".to_string(),
                reason: Some(format!("{}: {}", error.code, error.message)),
            });
        }
        let replicas = replica_projections(&view, group_id);
        if replicas.is_empty() {
            if let Some((_, error)) = errors.first() {
                return error_response(
                    &state.scheduler.cache,
                    StatusCode::BAD_GATEWAY,
                    error.code,
                    &error.message,
                );
            }
            return error_response(
                &state.scheduler.cache,
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                "no replica observations for group",
            );
        }
        ok(&view, json!({"group_id": group_id, "replicas": replicas}))
    })
    .await
}

#[derive(Deserialize)]
struct PerformanceQuery {
    window: Option<String>,
    group_id: Option<u64>,
    node_id: Option<u64>,
    op: Option<String>,
    source: Option<String>,
}

async fn performance(state: RuntimeState, Query(query): Query<PerformanceQuery>) -> Response {
    let window = query.window.clone().unwrap_or_else(|| "15m".to_string());
    let Some(window_duration) = parse_window(&window) else {
        return error_response(
            &state.scheduler.cache,
            StatusCode::BAD_REQUEST,
            "INVALID_ARGUMENT",
            "window must be one of 15m, 1h, 6h, or 24h",
        );
    };
    let source = query.source.as_deref().unwrap_or("");
    if !matches!(source, "" | "CONSOLE_MEMORY" | "PROMETHEUS") {
        return error_response(
            &state.scheduler.cache,
            StatusCode::BAD_REQUEST,
            "INVALID_ARGUMENT",
            "source must be CONSOLE_MEMORY or PROMETHEUS",
        );
    }
    if source == "PROMETHEUS" || (source.is_empty() && state.scheduler.history_client().is_some()) {
        return performance_history(state, query, window, window_duration).await;
    }
    let view = state.scheduler.cache.view();
    let cutoff = SystemTime::now()
        .checked_sub(window_duration)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let cutoff = rfc3339(cutoff);
    let mut grouped = BTreeMap::<
        (Option<u64>, String, BTreeMap<String, String>),
        Vec<(String, Option<f64>)>,
    >::new();
    for point in view.points.iter().filter(|point| {
        point.source == "PROMETHEUS_SCRAPE"
            && point.timestamp >= cutoff
            && performance_filters_match(point, &query)
    }) {
        grouped
            .entry((point.node_id, point.metric.clone(), point.labels.clone()))
            .or_default()
            .push((point.timestamp.clone(), point.value));
    }
    let series = grouped
        .into_iter()
        .map(|((node_id, metric, labels), points)| {
            metric_series_json(metric, node_id, labels, points)
        })
        .collect::<Vec<_>>();
    if series.is_empty() {
        let mut view = view.clone();
        view.partial = true;
        view.capabilities.push(Capability::unsupported(
            "performance_metrics",
            "no trusted observability metrics are available; legacy AdminService behavior is preserved",
        ));
        return ok_with_window(
            &view,
            json!({
                "status": "UNSUPPORTED",
                "window": window,
                "retention_source": "NONE",
                "series": [],
                "notices": history_notices(),
                "filters": performance_filters(&query),
                "reason": "legacy AdminService has no Prometheus performance projection",
            }),
            Some(window),
        );
    }
    let mut meta = view.meta();
    meta.time_window = Some(window.clone());
    Json(Envelope {
        data: PerformanceData {
            status: "OK".to_string(),
            window: window.clone(),
            retention_source: "CONSOLE_MEMORY".to_string(),
            series,
            notices: vec![unaligned_cohorts_notice()],
            filters: performance_filters(&query),
        },
        meta,
    })
    .into_response()
}

fn parse_window(window: &str) -> Option<Duration> {
    match window {
        "15m" => Some(Duration::from_secs(15 * 60)),
        "1h" => Some(Duration::from_secs(60 * 60)),
        "6h" => Some(Duration::from_secs(6 * 60 * 60)),
        "24h" => Some(Duration::from_secs(24 * 60 * 60)),
        _ => None,
    }
}

fn performance_filters(query: &PerformanceQuery) -> Value {
    json!({
        "window": query.window,
        "group_id": query.group_id,
        "node_id": query.node_id,
        "op": query.op,
        "source": query.source,
    })
}

fn performance_filters_match(point: &TimeSeriesPoint, query: &PerformanceQuery) -> bool {
    if query.node_id.is_some() && point.node_id != query.node_id {
        return false;
    }
    if query.group_id.is_some()
        && point
            .labels
            .get("group_id")
            .and_then(|value| value.parse().ok())
            != query.group_id
    {
        return false;
    }
    if let Some(op) = &query.op {
        let metric_op = point
            .metric
            .strip_prefix(op)
            .is_some_and(|suffix| suffix.starts_with('_'));
        if point.labels.get("op") != Some(op) && !metric_op {
            return false;
        }
    }
    true
}

fn unaligned_cohorts_notice() -> Value {
    json!({
        "feature": "UNALIGNED_COHORTS",
        "status": "UNSUPPORTED",
        "reason": "server total, proposal, and apply observations are independent; no mean stacking, subtraction, or percentile composition is performed",
    })
}

fn history_notices() -> Vec<Value> {
    vec![
        unaligned_cohorts_notice(),
        json!({
            "feature": "replication_lag_history",
            "status": "UNSUPPORTED",
            "reason": "lag is emitted only for same-scrape leaders; fixed history never treats follower absence as zero lag",
        }),
    ]
}

#[derive(serde::Serialize)]
struct MetricPoint {
    timestamp: String,
    value: Option<f64>,
}

#[derive(serde::Serialize)]
struct MetricSeries {
    metric: String,
    labels: BTreeMap<String, String>,
    points: Vec<MetricPoint>,
}

#[derive(serde::Serialize)]
struct PerformanceData {
    status: String,
    window: String,
    retention_source: String,
    series: Vec<MetricSeries>,
    notices: Vec<Value>,
    filters: Value,
}

fn metric_series_json(
    metric: String,
    node_id: Option<u64>,
    mut labels: BTreeMap<String, String>,
    mut points: Vec<(String, Option<f64>)>,
) -> MetricSeries {
    if let Some(node_id) = node_id {
        labels.insert("node_id".to_string(), node_id.to_string());
    }
    points.sort_by(|left, right| left.0.cmp(&right.0));
    MetricSeries {
        metric,
        labels,
        points: points
            .into_iter()
            .map(|(timestamp, value)| MetricPoint { timestamp, value })
            .collect(),
    }
}

async fn performance_history(
    state: RuntimeState,
    query: PerformanceQuery,
    window: String,
    window_duration: Duration,
) -> Response {
    let mut view = state.scheduler.cache.view();
    let Some(client) = state.scheduler.history_client() else {
        view.partial = true;
        view.capabilities.push(Capability::unsupported(
            "performance_history",
            "Prometheus is not configured; live Console memory remains available",
        ));
        return ok_with_window(
            &view,
            json!({
                "status": "UNSUPPORTED",
                "window": window,
                "retention_source": "NONE",
                "series": [],
                "notices": history_notices(),
                "filters": performance_filters(&query),
            }),
            Some(window),
        );
    };
    let end = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    let start = end - window_duration.as_secs_f64();
    let families = [
        (HistoryFamily::ServerRequests, None),
        (HistoryFamily::ServerRequestDuration, Some(0.50)),
        (HistoryFamily::ServerRequestDuration, Some(0.95)),
        (HistoryFamily::ServerRequestDuration, Some(0.99)),
        (HistoryFamily::ProposalDuration, Some(0.50)),
        (HistoryFamily::ProposalDuration, Some(0.95)),
        (HistoryFamily::ProposalDuration, Some(0.99)),
        (HistoryFamily::ApplyDuration, Some(0.50)),
        (HistoryFamily::ApplyDuration, Some(0.95)),
        (HistoryFamily::ApplyDuration, Some(0.99)),
        (HistoryFamily::AppendEntriesRtt, Some(0.95)),
        (HistoryFamily::AppendEntriesRtt, Some(0.99)),
        (HistoryFamily::AppliedEntries, None),
        (HistoryFamily::IngestorCommands, None),
        (HistoryFamily::SnapshotTransferDuration, Some(0.95)),
        (HistoryFamily::SnapshotTransferDuration, Some(0.99)),
        (HistoryFamily::SnapshotTransferBytes, None),
        (HistoryFamily::SnapshotInstalls, None),
    ];
    let mut requests = futures::stream::iter(families.into_iter().map(|(family, quantile)| {
        let client = client.clone();
        let node_ids = query.node_id.into_iter().collect::<Vec<_>>();
        let history_query = HistoryQuery {
            family,
            start,
            end,
            step: Duration::from_secs((window_duration.as_secs() / 60).clamp(15, 300)),
            rate_window: Duration::from_secs((window_duration.as_secs() / 12).clamp(60, 3600)),
            node_ids,
            group_id: query.group_id,
            quantile,
        };
        async move {
            let result = client.query_range(history_query).await;
            (family, quantile, result)
        }
    }))
    .buffer_unordered(4);
    let mut series = Vec::new();
    let mut server_series = Vec::new();
    let mut errors = Vec::new();
    let mut sources = BTreeMap::<u64, String>::new();
    while let Some((family, quantile, result)) = requests.next().await {
        match result {
            Ok(result) => {
                if family == HistoryFamily::ServerRequests {
                    server_series.extend(result.clone());
                }
                for history in result {
                    for point in &history.points {
                        if let Some(timestamp) = unix_timestamp(point.timestamp) {
                            sources.insert(history.node_id, timestamp);
                        }
                    }
                    let points = history
                        .points
                        .into_iter()
                        .filter_map(|point| {
                            unix_timestamp(point.timestamp)
                                .map(|timestamp| (timestamp, Some(point.value)))
                        })
                        .collect::<Vec<_>>();
                    let labels = history
                        .labels
                        .into_iter()
                        .filter(|(name, _)| !matches!(name.as_str(), "cluster_id" | "instance"))
                        .filter(|(name, _)| name != "node_id")
                        .collect::<BTreeMap<_, _>>();
                    if query
                        .op
                        .as_ref()
                        .is_some_and(|op| labels.get("op") != Some(op))
                        && matches!(
                            family,
                            HistoryFamily::ServerRequests | HistoryFamily::ServerRequestDuration
                        )
                    {
                        continue;
                    }
                    series.push(metric_series_json(
                        history_metric_name(family, quantile),
                        Some(history.node_id),
                        labels,
                        points,
                    ));
                }
            }
            Err(error) => errors.push(error),
        }
    }
    series.extend(cluster_history_throughput(
        &server_series,
        query.op.as_deref(),
    ));
    series.sort_by(|left, right| {
        left.metric
            .cmp(&right.metric)
            .then_with(|| left.labels.cmp(&right.labels))
    });
    view.sources.extend(
        sources
            .into_iter()
            .map(|(node_id, observed_at)| SourceMeta {
                node_id,
                endpoint_role: "PROMETHEUS_HISTORY".to_string(),
                observed_at: Some(observed_at),
                age_ms: None,
                status: "ONLINE".to_string(),
                error_code: None,
            }),
    );
    let unmapped = errors.iter().any(|error| error.code == "UNMAPPED");
    let status = if unmapped {
        "UNMAPPED"
    } else if errors.is_empty() {
        "OK"
    } else {
        "PARTIAL"
    };
    if unmapped {
        series.clear();
        view.sources
            .retain(|source| source.endpoint_role != "PROMETHEUS_HISTORY");
    }
    for error in &errors {
        view.partial = true;
        view.capabilities.push(Capability {
            feature: "performance_history".to_string(),
            status: if error.code == "UNMAPPED" {
                "UNMAPPED".to_string()
            } else {
                "ERROR".to_string()
            },
            reason: Some(error.code.to_string()),
        });
    }
    let mut meta = view.meta();
    meta.partial |= unmapped || !errors.is_empty();
    meta.retention_source = "PROMETHEUS".to_string();
    meta.time_window = Some(window.clone());
    Json(Envelope {
        data: PerformanceData {
            status: status.to_string(),
            window: window.clone(),
            retention_source: "PROMETHEUS".to_string(),
            series,
            notices: history_notices(),
            filters: performance_filters(&query),
        },
        meta,
    })
    .into_response()
}

fn history_metric_name(family: HistoryFamily, quantile: Option<f64>) -> String {
    let base = match family {
        HistoryFamily::ServerRequestDuration => "rr_server_request_duration_seconds",
        HistoryFamily::ProposalDuration => "rr_proposal_duration_seconds",
        HistoryFamily::ApplyDuration => "rr_raft_apply_duration_seconds",
        HistoryFamily::AppendEntriesRtt => "rr_raft_append_entries_rtt_seconds",
        HistoryFamily::SnapshotTransferDuration => "rr_snapshot_transfer_duration_seconds",
        HistoryFamily::ServerRequests => "server_requests_tps",
        HistoryFamily::AppliedEntries => "applied_entries_tps",
        HistoryFamily::IngestorCommands => "ingestor_commands_tps",
        HistoryFamily::SnapshotTransferBytes => "snapshot_transfer_bytes_per_second",
        HistoryFamily::SnapshotInstalls => "snapshot_installs_per_second",
        HistoryFamily::ReplicationLag => "rr_raft_replication_lag",
    };
    match quantile {
        Some(quantile) if (quantile - 0.50).abs() < f64::EPSILON => format!("{base}_p50"),
        Some(quantile) if (quantile - 0.95).abs() < f64::EPSILON => format!("{base}_p95"),
        Some(quantile) if (quantile - 0.99).abs() < f64::EPSILON => format!("{base}_p99"),
        Some(_) => format!("{base}_quantile"),
        None => base.to_string(),
    }
}

fn cluster_history_throughput(
    series: &[super::metrics::HistorySeries],
    op_filter: Option<&str>,
) -> Vec<MetricSeries> {
    let mut points = BTreeMap::<(String, String), f64>::new();
    for history in series {
        let Some(op) = history.labels.get("op") else {
            continue;
        };
        if op_filter.is_some_and(|filter| filter != op) {
            continue;
        }
        for point in &history.points {
            if point.value.is_finite() {
                *points
                    .entry((op.clone(), point.timestamp.to_string()))
                    .or_default() += point.value;
            }
        }
    }
    let mut grouped = BTreeMap::<String, Vec<(String, Option<f64>)>>::new();
    for ((op, timestamp), value) in points {
        let Some(timestamp) = timestamp.parse::<f64>().ok().and_then(unix_timestamp) else {
            continue;
        };
        grouped
            .entry(op)
            .or_default()
            .push((timestamp, Some(value)));
    }
    grouped
        .into_iter()
        .map(|(op, points)| metric_series_json(format!("{op}_tps"), None, BTreeMap::new(), points))
        .collect()
}

fn unix_timestamp(timestamp: f64) -> Option<String> {
    if !timestamp.is_finite() || timestamp < 0.0 {
        return None;
    }
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs_f64(timestamp))
        .map(rfc3339)
}

#[derive(Deserialize)]
struct SnapshotQuery {
    #[serde(flatten)]
    page: PageQuery,
    group_id: Option<u64>,
    node_id: Option<u64>,
}

async fn snapshots(state: RuntimeState, Query(query): Query<SnapshotQuery>) -> Response {
    let cache = state.scheduler.cache.clone();
    with_handler_deadline(&cache, snapshots_inner(state, query)).await
}

async fn snapshots_inner(state: RuntimeState, query: SnapshotQuery) -> Response {
    let initial = state.scheduler.cache.view();
    let mut requests = Vec::new();
    for node in initial.nodes.iter() {
        if query
            .node_id
            .is_some_and(|node_id| node_id != node.peer.node_id)
        {
            continue;
        }
        for group in node
            .snapshot
            .as_ref()
            .into_iter()
            .flat_map(|snapshot| snapshot.groups.iter().flatten())
        {
            if query
                .group_id
                .is_some_and(|group_id| group_id != group.group_id)
            {
                continue;
            }
            requests.push((node.peer.node_id, group.group_id));
        }
    }
    requests.sort_unstable();
    requests.dedup();
    let page = match paginate(&requests, query.page) {
        Ok(page) => page,
        Err(message) => {
            return error_response_with_meta(
                initial.meta(),
                StatusCode::BAD_REQUEST,
                "INVALID_ARGUMENT",
                &message,
            )
        }
    };
    let mut errors = state
        .scheduler
        .collect_groups(page.items.iter().copied())
        .await
        .into_iter()
        .filter_map(|(node_id, group_id, result)| {
            result.err().map(|error| (node_id, group_id, error))
        })
        .collect::<Vec<_>>();
    errors.sort_by_key(|(node_id, group_id, _)| (*node_id, *group_id));

    let mut view = state.scheduler.cache.view();
    for (node_id, group_id, error) in errors {
        view.partial = true;
        view.capabilities.push(Capability {
            feature: format!("snapshot_status.node.{node_id}.group.{group_id}"),
            status: "ERROR".to_string(),
            reason: Some(format!("{}: {}", error.code, error.message)),
        });
    }
    let mut items = Vec::new();
    for &(node_id, group_id) in &page.items {
        let Some(node) = view.nodes.iter().find(|node| node.peer.node_id == node_id) else {
            continue;
        };
        let Some(detail) = node.details.get(&group_id) else {
            continue;
        };
        let replica = detail.replica.as_ref();
        let last_log_index = replica
            .and_then(|replica| replica.last_log_index)
            .unwrap_or(detail.status.last_log_index);
        let last_snapshot_index = replica
            .and_then(|replica| replica.last_snapshot_index)
            .or_else(|| {
                detail
                    .snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.last_snapshot_index)
            });
        let last_snapshot_term = replica
            .and_then(|replica| replica.last_snapshot_term)
            .or_else(|| {
                detail
                    .snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.last_snapshot_term)
            });
        let install_count = node
            .metrics
            .as_ref()
            .and_then(|metrics| {
                metrics.samples.iter().find(|sample| {
                    sample.name == "rr_snapshot_installs_total"
                        && sample.labels.get("group_id") == Some(&group_id.to_string())
                })
            })
            .map(|sample| {
                if sample.value >= 0.0
                    && sample.value.fract() == 0.0
                    && sample.value < u64::MAX as f64
                {
                    Value::from(sample.value as u64)
                } else {
                    serde_json::Number::from_f64(sample.value).map_or(Value::Null, Value::Number)
                }
            });
        let install_rate = node
            .metrics
            .as_ref()
            .and_then(|metrics| {
                metrics.rates.iter().find(|rate| {
                    rate.name == "rr_snapshot_installs_total"
                        && rate.labels.get("group_id") == Some(&group_id.to_string())
                })
            })
            .map(|rate| rate.value);
        let transfer_rate = |direction: &str| {
            node.metrics
                .as_ref()
                .and_then(|metrics| {
                    metrics.rates.iter().find(|rate| {
                        rate.name == "rr_snapshot_transfer_bytes"
                            && rate.labels.get("group_id") == Some(&group_id.to_string())
                            && rate.labels.get("direction") == Some(&direction.to_string())
                    })
                })
                .map(|rate| rate.value)
        };
        items.push(json!({
                "group_id": group_id,
                "node_id": node.peer.node_id,
                "last_log_index": last_log_index,
                "last_snapshot_index": last_snapshot_index,
                "last_snapshot_term": last_snapshot_term,
                "snapshot_distance": last_snapshot_index.map(|index| last_log_index.saturating_sub(index)),
                "install_count": install_count,
                "install_rate": install_rate,
                "send_bytes_per_second": transfer_rate("send"),
                "recv_bytes_per_second": transfer_rate("recv"),
                "build_failure": Value::Null,
                "build_failure_status": "UNSUPPORTED",
                "source_count": 1 + usize::from(node.metrics.is_some()),
                "observed_at": rfc3339(detail.collected_at),
            }));
    }
    let build_failures = view
        .nodes
        .iter()
        .filter_map(|node| {
            node.metrics
                .as_ref()
                .and_then(|metrics| {
                    metrics
                        .samples
                        .iter()
                        .find(|sample| sample.name == "rr_snapshot_build_failures_total")
                })
                .map(|failures| (node.peer.node_id, failures.value))
        })
        .collect::<Vec<_>>();
    for (node_id, failures) in build_failures {
        view.capabilities.push(Capability {
            feature: format!("snapshot_build_failures.node.{node_id}"),
            status: "OBSERVED".to_string(),
            reason: Some(format!(
                "node-level build failure counter is {failures}; no group label is available, so Group × Node cells remain UNSUPPORTED"
            )),
        });
    }
    view.capabilities.push(Capability {
        feature: "snapshots".to_string(),
        status: "ON_DEMAND".to_string(),
        reason: Some(
            "snapshot details are collected on demand only for the returned page of replica rows"
                .to_string(),
        ),
    });
    ok(
        &view,
        json!({"items": items, "next_cursor": page.next_cursor}),
    )
}

#[derive(Deserialize)]
struct LifecycleQuery {
    #[serde(flatten)]
    page: PageQuery,
    view: Option<String>,
}

async fn lifecycle(state: RuntimeState, Query(query): Query<LifecycleQuery>) -> Response {
    let cache = state.scheduler.cache.clone();
    with_handler_deadline(&cache, lifecycle_inner(state, query)).await
}

async fn lifecycle_inner(state: RuntimeState, query: LifecycleQuery) -> Response {
    let cache_view = state.scheduler.cache.view();
    match query.view.as_deref().unwrap_or("current") {
        "current" => {
            let incompatible = incompatible_raft_pairs(&cache_view);
            let items = cache_view
                .nodes
                .iter()
                .map(|node| {
                    let snapshot = node.snapshot.as_ref();
                    let build = snapshot.and_then(|snapshot| snapshot.build.as_ref());
                    let boot = snapshot.and_then(|snapshot| snapshot.boot.as_ref());
                    let incompatible_with = incompatible
                        .iter()
                        .filter(|(left, right)| *left == node.peer.node_id || *right == node.peer.node_id)
                        .map(|(left, right)| if *left == node.peer.node_id { *right } else { *left })
                        .collect::<Vec<_>>();
                    json!({
                        "node_id": node.peer.node_id,
                        "status": node.sources[0].status,
                        "ready": node.observability.as_ref().and_then(|observability| observability.ready),
                        "liveness": node.observability.as_ref().and_then(|observability| observability.liveness),
                        "ready_phase": node.observability.as_ref().and_then(|observability| observability.ready_phase.clone()),
                        "uptime_secs": snapshot.map(|snapshot| snapshot.uptime_secs),
                        "process_start_time": build.and_then(|build| unix_ms(build.process_start_unix_ms)),
                        "boot_id": boot.and_then(|boot| boot.current.as_ref()).map(|current| current.boot_id.clone()).or_else(|| build.map(|build| build.boot_id.clone())),
                        "persistence": boot.map(|boot| boot.persistence.clone()),
                        "app_version": build.map(|build| build.app_version.clone()),
                        "rr_version": build.map(|build| build.rr_version.clone()),
                        "git_commit": build.map(|build| build.git_commit.clone()),
                        "git_dirty": build.map(|build| build.git_dirty),
                        "dirty": build.map(|build| build.git_dirty),
                        "profile": build.map(|build| build.build_profile.clone()),
                        "config_fingerprint": build.map(|build| build.config_fingerprint.clone()),
                        "admin_api": build.and_then(|build| build.admin_api.as_ref()).map(protocol_json),
                        "raft_wire": build.and_then(|build| build.raft_wire.as_ref()).map(protocol_json),
                        "admin_protocol": build.and_then(|build| build.admin_api.as_ref()).map(protocol_json),
                        "raft_protocol": build.and_then(|build| build.raft_wire.as_ref()).map(protocol_json),
                        "raft_incompatible_with": incompatible_with,
                        "compatibility": compatibility_json(&cache_view, node.peer.node_id),
                        "observed_at": node.last_success.map(rfc3339),
                    })
                })
                .collect::<Vec<_>>();
            page_response(&cache_view, &items, query.page)
        }
        kind @ ("restarts" | "versions") => {
            let page_size = query.page.page_size.unwrap_or(50);
            if !(1..=200).contains(&page_size) {
                return error_response(
                    &state.scheduler.cache,
                    StatusCode::BAD_REQUEST,
                    "INVALID_ARGUMENT",
                    "page_size must be between 1 and 200",
                );
            }
            let history_limit = if query.page.cursor.is_some() {
                100
            } else {
                page_size.saturating_add(1).min(100)
            };
            let nodes = cache_view
                .nodes
                .iter()
                .map(|node| {
                    let current_boot_id = node.snapshot.as_ref().and_then(|snapshot| {
                        snapshot
                            .boot
                            .as_ref()
                            .and_then(|boot| boot.current.as_ref())
                            .map(|current| current.boot_id.clone())
                            .or_else(|| snapshot.build.as_ref().map(|build| build.boot_id.clone()))
                    });
                    (node.peer.node_id, current_boot_id)
                })
                .collect::<Vec<_>>();
            let mut histories =
                stream::iter(nodes.into_iter().map(|(node_id, current_boot_id)| {
                    let scheduler = state.scheduler.clone();
                    async move {
                        let result = collect_boot_history(&scheduler, node_id, history_limit).await;
                        (node_id, current_boot_id, result)
                    }
                }))
                .buffer_unordered(ON_DEMAND_CONCURRENCY)
                .collect::<Vec<_>>()
                .await;
            histories.sort_by_key(|(node_id, _, _)| *node_id);

            let mut view = cache_view.clone();
            let mut items = Vec::new();
            for (node_id, current_boot_id, result) in histories {
                match result {
                    Ok(records) => {
                        for (record, persistence) in records {
                            let mut item = boot_record_json(&record, Some(&persistence));
                            item["node_id"] = json!(node_id);
                            if kind == "versions" {
                                let current =
                                    current_boot_id.as_deref() == Some(record.boot_id.as_str());
                                item["current"] = json!(current);
                                item["compatibility"] = if current {
                                    compatibility_json(&cache_view, node_id)
                                } else {
                                    json!({
                                        "status": "UNKNOWN",
                                        "reason": "historical boots have no same-time peer compatibility decision",
                                        "incompatible_node_ids": [],
                                    })
                                };
                            }
                            items.push(item);
                        }
                    }
                    Err(error) => {
                        view.partial = true;
                        view.capabilities.push(Capability {
                            feature: format!("lifecycle_history.node.{node_id}"),
                            status: if error.code == "UNIMPLEMENTED" {
                                "UNSUPPORTED".to_string()
                            } else {
                                "ERROR".to_string()
                            },
                            reason: Some(error.message),
                        });
                    }
                }
            }
            items.sort_by(|left, right| {
                let left_started = left["started_at"].as_str().unwrap_or_default();
                let right_started = right["started_at"].as_str().unwrap_or_default();
                right_started
                    .cmp(left_started)
                    .then_with(|| left["node_id"].as_u64().cmp(&right["node_id"].as_u64()))
                    .then_with(|| left["boot_id"].as_str().cmp(&right["boot_id"].as_str()))
            });
            page_response(&view, &items, query.page)
        }
        _ => error_response(
            &state.scheduler.cache,
            StatusCode::BAD_REQUEST,
            "INVALID_ARGUMENT",
            "view must be current, restarts, or versions",
        ),
    }
}

async fn collect_boot_history(
    scheduler: &Arc<Scheduler>,
    node_id: u64,
    limit: usize,
) -> Result<Vec<(rr_network::proto::admin::BootRecord, String)>, super::upstream::UpstreamError> {
    let limit = limit.clamp(1, 100);
    let mut records = Vec::new();
    let mut cursor = None;
    let mut seen = std::collections::HashSet::new();
    for _ in 0..100 {
        let page = scheduler
            .list_boot_records(node_id, cursor.clone(), limit)
            .await?;
        let next_cursor = (!page.next_cursor.is_empty()).then_some(page.next_cursor);
        let persistence = page.persistence;
        records.extend(
            page.records
                .into_iter()
                .map(|record| (record, persistence.clone())),
        );
        records.truncate(limit);
        let Some(next) = next_cursor else {
            return Ok(records);
        };
        if records.len() == limit || !seen.insert(next.clone()) {
            return Ok(records);
        }
        cursor = Some(next);
    }
    Ok(records)
}

fn compatibility_json(view: &CacheView, node_id: u64) -> Value {
    let Some(node) = view.nodes.iter().find(|node| node.peer.node_id == node_id) else {
        return json!({
            "status": "UNKNOWN",
            "reason": "node is absent from the current Console view",
            "incompatible_node_ids": [],
        });
    };
    let groups = node
        .snapshot
        .as_ref()
        .map(|snapshot| {
            snapshot
                .groups
                .iter()
                .flatten()
                .map(|group| group.group_id)
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();
    if node
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.build.as_ref())
        .and_then(|build| build.raft_wire.as_ref())
        .is_none()
    {
        return json!({
            "status": "UNKNOWN",
            "reason": "node did not advertise a Raft protocol version",
            "incompatible_node_ids": [],
        });
    }
    let comparable = view
        .nodes
        .iter()
        .filter(|peer| {
            peer.peer.node_id != node_id
                && peer
                    .snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.build.as_ref())
                    .and_then(|build| build.raft_wire.as_ref())
                    .is_some()
                && peer.snapshot.as_ref().is_some_and(|snapshot| {
                    snapshot
                        .groups
                        .iter()
                        .flatten()
                        .any(|group| groups.contains(&group.group_id))
                })
        })
        .map(|peer| peer.peer.node_id)
        .collect::<Vec<_>>();
    if comparable.is_empty() {
        return json!({
            "status": "UNKNOWN",
            "reason": "no same-group peer advertised a Raft protocol version",
            "incompatible_node_ids": [],
        });
    }
    let incompatible = incompatible_raft_pairs(view);
    let incompatible_node_ids = comparable
        .into_iter()
        .filter(|peer| {
            incompatible
                .iter()
                .any(|pair| *pair == (node_id.min(*peer), node_id.max(*peer)))
        })
        .collect::<Vec<_>>();
    if incompatible_node_ids.is_empty() {
        json!({
            "status": "COMPATIBLE",
            "reason": Value::Null,
            "incompatible_node_ids": [],
        })
    } else {
        json!({
            "status": "INCOMPATIBLE",
            "reason": format!("Raft protocol requirements are not mutually satisfied with nodes {incompatible_node_ids:?}"),
            "incompatible_node_ids": incompatible_node_ids,
        })
    }
}

fn incompatible_raft_pairs(view: &CacheView) -> Vec<(u64, u64)> {
    let builds = view
        .nodes
        .iter()
        .filter_map(|node| {
            node.snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.build.as_ref())
                .and_then(|build| build.raft_wire.as_ref())
                .map(|version| {
                    (
                        node.peer.node_id,
                        node.snapshot
                            .as_ref()
                            .map(|snapshot| {
                                snapshot
                                    .groups
                                    .iter()
                                    .flatten()
                                    .map(|group| group.group_id)
                                    .collect::<std::collections::BTreeSet<_>>()
                            })
                            .unwrap_or_default(),
                        rr_core::ProtocolVersion {
                            major: version.major,
                            minor: version.minor,
                            min_peer_minor: version.min_peer_minor,
                        },
                    )
                })
        })
        .collect::<Vec<_>>();
    let mut pairs = Vec::new();
    for (index, (left_id, left_groups, left)) in builds.iter().enumerate() {
        for (right_id, right_groups, right) in builds.iter().skip(index + 1) {
            if left_groups.is_disjoint(right_groups) {
                continue;
            }
            if !rr_core::raft_pair_compatible(*left, *right) {
                pairs.push((*left_id, *right_id));
            }
        }
    }
    pairs
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(flatten)]
    page: PageQuery,
    severity: Option<String>,
    state: Option<String>,
}

async fn events(state: RuntimeState, Query(query): Query<EventsQuery>) -> Response {
    let view = state.scheduler.cache.view();
    let items = view
        .events
        .iter()
        .filter(|event| {
            query
                .severity
                .as_ref()
                .is_none_or(|severity| event.severity == *severity)
        })
        .filter(|event| {
            query
                .state
                .as_ref()
                .is_none_or(|state| event.state == *state)
        })
        .cloned()
        .collect::<Vec<_>>();
    page_response(&view, &items, query.page)
}

async fn sources(state: RuntimeState) -> Response {
    let view = state.scheduler.cache.view();
    let prometheus = state.scheduler.history_config().map(|config| {
        json!({
            "status": "CONFIGURED",
            "url": config.url,
            "cluster_matcher": format!("{}={}", config.cluster.label, config.cluster.value),
        })
    });
    ok(
        &view,
        json!({
            "mode": view.source_mode,
            "read_only": true,
            "console_version": env!("CARGO_PKG_VERSION"),
            "generation": view.generation,
            "topology_revision": view.topology_revision,
            "quarantined": view.quarantined,
            "discovery_error": view.discovery_error.as_ref().map(|error| json!({"code": error.code, "message": error.message})),
            "retained_observation_points": view.points.len(),
            "sources": view.nodes.iter().map(|node| json!({
                "node_id": node.peer.node_id,
                "admin_addr": node.peer.admin_addr,
                "observability_addr": node.snapshot.as_ref().and_then(|snapshot| snapshot.descriptor.as_ref()).map(|evidence| evidence.descriptor.observability_addr.clone()),
                "directory_revision": node.snapshot.as_ref().and_then(|snapshot| snapshot.descriptor.as_ref()).map(|evidence| evidence.directory_revision),
                "last_success": node.last_success.map(rfc3339),
                "last_attempt": rfc3339(node.last_attempt),
                "error": node.error.as_ref().map(|error| json!({"code": error.code, "message": error.message})),
                "capabilities": node.snapshot.as_ref().map(|snapshot| snapshot.capabilities.clone()),
                "metrics": node.metrics.as_ref().map(|metrics| json!({
                    "status": metrics.source.status,
                    "observed_at": metrics.source.observed_at,
                    "last_attempt": rfc3339(metrics.last_attempt),
                    "error": metrics.error.as_ref().map(|error| json!({"code": error.code, "message": error.message})),
                })),
            })).collect::<Vec<_>>(),
            "prometheus": prometheus.unwrap_or_else(|| json!({"status": "UNSUPPORTED", "url": Value::Null})),
        }),
    )
}

async fn stream(state: RuntimeState) -> Response {
    let mut shutdown = state.runtime.shutdown.subscribe();
    let initial = futures::stream::once({
        let state = state.clone();
        async move { Ok::<Event, Infallible>(summary_event(&state).await) }
    });
    let updates =
        BroadcastStream::new(state.scheduler.cache.subscribe()).filter_map(move |update| {
            let state = state.clone();
            async move {
                match update {
                    Ok(_) => Some(Ok(summary_event(&state).await)),
                    Err(BroadcastStreamRecvError::Lagged(skipped)) => Some(Ok(Event::default()
                        .event("resync")
                        .json_data(json!({"reason": "LAGGED", "skipped": skipped}))
                        .unwrap_or_else(|_| Event::default().event("resync")))),
                }
            }
        });
    let closed = async move {
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
    };
    Sse::new(initial.chain(updates).take_until(closed))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("console-keepalive"),
        )
        .into_response()
}

async fn summary_event(state: &RuntimeState) -> Event {
    Event::default()
        .event("summary")
        .data(summary_stream_envelope(&summary_projection(state).await))
}

async fn api_not_found() -> Response {
    neutral_error(
        StatusCode::NOT_FOUND,
        "NOT_FOUND",
        "Console API route not found",
    )
}

async fn static_or_spa(State(state): State<AppState>, method: Method, uri: Uri) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return (StatusCode::METHOD_NOT_ALLOWED, "method not allowed").into_response();
    }
    let raw_path = uri.path().trim_start_matches('/');
    let Ok(path) = percent_decode(raw_path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    let relative = PathBuf::from(if path.is_empty() { "index.html" } else { &path });
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
        || path.contains(['\\', '\0'])
    {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    }
    let mut candidates = vec![state.static_dir.join(&relative)];
    if relative.extension().is_none() {
        candidates.push(
            state
                .static_dir
                .join(format!("{}.html", relative.display())),
        );
        candidates.push(state.static_dir.join(&relative).join("index.html"));
        candidates.push(state.static_dir.join("index.html"));
    }
    for candidate in candidates {
        match tokio::fs::read(&candidate).await {
            Ok(bytes) => {
                let content_type = content_type(&candidate);
                let mut response = if method == Method::HEAD {
                    Body::empty().into_response()
                } else {
                    Body::from(bytes).into_response()
                };
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
                return response;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::IsADirectory
                ) => {}
            Err(_) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, "static file error").into_response()
            }
        }
    }
    if relative.extension().is_some() {
        return (StatusCode::NOT_FOUND, "static asset not found").into_response();
    }
    let body = "<!doctype html><html><head><meta charset=\"utf-8\"><title>Rr Console</title></head><body><main><h1>Rr Console</h1><p>Console frontend export not found. Build <code>crates/rr-admin/console/out</code>; the JSON and SSE APIs are available under <code>/api/v1/console</code>.</p></main></body></html>";
    let mut response = if method == Method::HEAD {
        Body::empty().into_response()
    } else {
        Html(body).into_response()
    };
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
}

fn percent_decode(path: &str) -> Result<String, ()> {
    let bytes = path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(());
            }
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).map_err(|_| ())?;
            decoded.push(u8::from_str_radix(hex, 16).map_err(|_| ())?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| ())
}

fn content_type(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
    {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript",
        "css" => "text/css",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn resources_json(node: &CachedNode) -> Value {
    let Some(metrics) = &node.metrics else {
        return Value::Null;
    };
    let samples = metrics
        .samples
        .iter()
        .filter(|sample| sample.name.starts_with("rr_node_"))
        .collect::<Vec<_>>();
    let rates = metrics
        .rates
        .iter()
        .filter(|rate| rate.name.starts_with("rr_node_"))
        .collect::<Vec<_>>();
    if samples.is_empty() && rates.is_empty() {
        return Value::Null;
    }

    let mut legacy = serde_json::Map::new();
    let mut evidence = Vec::new();
    for sample in &samples {
        legacy.insert(sample.name.clone(), json!(sample.value));
        evidence.push(json!({
            "name": sample.name,
            "labels": sample.labels,
            "value": sample.value,
            "rate": false,
        }));
    }
    for rate in &rates {
        legacy.insert(
            format!("{}_per_second", rate.name.trim_end_matches("_total")),
            json!(rate.value),
        );
        evidence.push(json!({
            "name": rate.name,
            "labels": rate.labels,
            "value": rate.value,
            "rate": true,
        }));
    }

    let sample = |name: &str, scope: &str, kind: Option<&str>| {
        samples.iter().copied().find(|sample| {
            sample.name == name
                && sample.labels.get("scope").map(String::as_str) == Some(scope)
                && kind
                    .is_none_or(|kind| sample.labels.get("kind").map(String::as_str) == Some(kind))
        })
    };
    let first_sample = |name: &str, scopes: &[&str], kind: Option<&str>| {
        scopes.iter().find_map(|scope| sample(name, scope, kind))
    };
    let rate = |kind: &str| {
        rates.iter().copied().find(|rate| {
            rate.name == "rr_node_network_bytes_total"
                && rate.labels.get("scope").map(String::as_str) == Some("host")
                && rate.labels.get("kind").map(String::as_str) == Some(kind)
        })
    };
    let cpu = first_sample(
        "rr_node_cpu_usage_ratio",
        &["process", "host", "cgroup"],
        None,
    );
    let cpu_cores = first_sample("rr_node_cpu_cores", &["host", "process", "cgroup"], None);
    let cpu_limit = first_sample(
        "rr_node_cpu_limit_cores",
        &["cgroup", "host", "process"],
        None,
    );
    let memory_used = first_sample(
        "rr_node_memory_bytes",
        &["host", "cgroup", "process"],
        Some("used"),
    );
    let memory_total = first_sample(
        "rr_node_memory_bytes",
        &["host", "cgroup", "process"],
        Some("total"),
    );
    let memory_limit = first_sample(
        "rr_node_memory_bytes",
        &["cgroup", "host", "process"],
        Some("limit"),
    );
    let network_rx = rate("rx").or_else(|| {
        rates.iter().copied().find(|rate| {
            rate.name == "rr_node_network_bytes_total"
                && rate.labels.get("kind").map(String::as_str) == Some("rx")
        })
    });
    let network_tx = rate("tx").or_else(|| {
        rates.iter().copied().find(|rate| {
            rate.name == "rr_node_network_bytes_total"
                && rate.labels.get("kind").map(String::as_str) == Some("tx")
        })
    });

    let mut mounts: BTreeMap<(String, String), (Option<f64>, Option<f64>)> = BTreeMap::new();
    for sample in samples
        .iter()
        .copied()
        .filter(|sample| sample.name == "rr_node_filesystem_bytes")
    {
        let scope = sample.labels.get("scope").cloned().unwrap_or_default();
        let mount = sample.labels.get("mount").cloned().unwrap_or_default();
        let values = mounts.entry((scope, mount)).or_default();
        match sample.labels.get("kind").map(String::as_str) {
            Some("used") => values.0 = Some(sample.value),
            Some("total") => values.1 = Some(sample.value),
            _ => {}
        }
    }
    let filesystems = mounts
        .iter()
        .map(|((scope, mount), (used, total))| {
            json!({
                "scope": (!scope.is_empty()).then_some(scope),
                "mount": (!mount.is_empty()).then_some(mount),
                "used_bytes": used,
                "total_bytes": total,
            })
        })
        .collect::<Vec<_>>();
    let filesystem = mounts
        .get(&("host".to_string(), "/".to_string()))
        .map(|values| (("host".to_string(), "/".to_string()), values))
        .or_else(|| {
            mounts
                .first_key_value()
                .map(|(key, values)| (key.clone(), values))
        });
    let scope = |sample: Option<&super::metrics::MetricSample>| {
        sample
            .and_then(|sample| sample.labels.get("scope"))
            .cloned()
    };
    let network_scope = network_rx
        .and_then(|rate| rate.labels.get("scope"))
        .filter(|scope| {
            network_tx
                .and_then(|rate| rate.labels.get("scope"))
                .is_none_or(|tx_scope| tx_scope == *scope)
        });

    json!({
        "observed_at": metrics.source.observed_at,
        "sampled_at": metrics.source.observed_at,
        "metrics": legacy,
        "samples": evidence,
        "cpu_usage_ratio": cpu.map(|sample| sample.value),
        "cpu_scope": scope(cpu),
        "cpu_cores": cpu_cores.map(|sample| sample.value),
        "cpu_cores_scope": scope(cpu_cores),
        "cpu_limit_cores": cpu_limit.map(|sample| sample.value),
        "cpu_limit_scope": scope(cpu_limit),
        "memory_used_bytes": memory_used.map(|sample| sample.value),
        "memory_total_bytes": memory_total.map(|sample| sample.value),
        "memory_limit_bytes": memory_limit.map(|sample| sample.value),
        "memory_scope": scope(memory_used.or(memory_total).or(memory_limit)),
        "memory_used_scope": scope(memory_used),
        "memory_total_scope": scope(memory_total),
        "memory_limit_scope": scope(memory_limit),
        "network_rx_bytes_per_sec": network_rx.map(|rate| rate.value),
        "network_tx_bytes_per_sec": network_tx.map(|rate| rate.value),
        "network_scope": network_scope,
        "network_rx_scope": network_rx.and_then(|rate| rate.labels.get("scope")),
        "network_tx_scope": network_tx.and_then(|rate| rate.labels.get("scope")),
        "filesystem_used_bytes": filesystem.as_ref().and_then(|(_, values)| values.0),
        "filesystem_total_bytes": filesystem.as_ref().and_then(|(_, values)| values.1),
        "filesystem_scope": filesystem.as_ref().and_then(|((scope, _), _)| (!scope.is_empty()).then_some(scope)),
        "filesystem_mount": filesystem.as_ref().and_then(|((_, mount), _)| (!mount.is_empty()).then_some(mount)),
        "filesystems": filesystems,
    })
}

fn node_json(node: &CachedNode) -> Value {
    let snapshot = node.snapshot.as_ref();
    let observability = node.observability.as_ref();
    let descriptor = snapshot.and_then(|snapshot| snapshot.descriptor.as_ref());
    let groups = snapshot
        .map(|snapshot| {
            snapshot
                .groups
                .iter()
                .flatten()
                .map(|group| group.group_id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({
        "node_id": node.peer.node_id,
        "admin_addr": node.peer.admin_addr,
        "listen_addr": snapshot.map(|snapshot| snapshot.listen_addr.clone()),
        "client_addr": descriptor.map(|evidence| evidence.descriptor.client_addr.clone()),
        "observability_addr": descriptor.map(|evidence| evidence.descriptor.observability_addr.clone()),
        "advertise_addr": descriptor.map(|evidence| evidence.descriptor.advertise_addr.clone()),
        "environment": descriptor.map(|evidence| evidence.descriptor.environment.clone()),
        "region": descriptor.and_then(|evidence| evidence.descriptor.region.clone()),
        "zone": descriptor.and_then(|evidence| evidence.descriptor.zone.clone()),
        "rack": descriptor.and_then(|evidence| evidence.descriptor.rack.clone()),
        "directory_revision": descriptor.map(|evidence| evidence.directory_revision),
        "status": node.sources[0].status,
        "ready": observability.and_then(|observability| observability.ready),
        "liveness": observability.and_then(|observability| observability.liveness),
        "ready_phase": observability.and_then(|observability| observability.ready_phase.clone()),
        "group_ids": groups,
        "leader_group_count": snapshot.map(|snapshot| snapshot.groups.iter().flatten().filter(|group| group.is_leader).count()),
        "uptime_secs": snapshot.map(|snapshot| snapshot.uptime_secs),
        "last_success": node.last_success.map(rfc3339),
        "last_attempt": rfc3339(node.last_attempt),
        "error": node.error.as_ref().map(|error| json!({"code": error.code, "message": error.message})),
        "resources": resources_json(node),
        "build": snapshot.and_then(|snapshot| snapshot.build.as_ref()).map(build_json),
        "boot": snapshot.and_then(|snapshot| snapshot.boot.as_ref()).map(|boot| json!({
            "persistence": boot.persistence,
            "current": boot.current.as_ref().map(|record| boot_record_json(record, Some(&boot.persistence))),
        })),
    })
}

fn route_json(route: &rr_network::proto::admin::PartitionRoute) -> Value {
    json!({
        "partition_id": route.partition_id,
        "group_id": route.group_id,
        "replica_node_ids": route.replica_node_ids,
    })
}

fn replica_json(node_id: u64, group_id: u64, detail: &super::upstream::GroupDetail) -> Value {
    let membership = detail.membership.as_ref();
    let replica = detail.replica.as_ref();
    let last_snapshot_index = replica
        .and_then(|replica| replica.last_snapshot_index)
        .or_else(|| {
            detail
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.last_snapshot_index)
        });
    let last_snapshot_term = replica
        .and_then(|replica| replica.last_snapshot_term)
        .or_else(|| {
            detail
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.last_snapshot_term)
        });
    json!({
        "node_id": node_id,
        "group_id": group_id,
        "server_state": replica.map(|replica| replica.server_state.clone()),
        "is_leader": replica.map(|replica| replica.server_state == "Leader").unwrap_or(detail.status.is_leader),
        "current_term": replica.map(|replica| replica.current_term).unwrap_or(detail.status.current_term),
        "last_log_index": replica.and_then(|replica| replica.last_log_index).or(Some(detail.status.last_log_index)),
        "commit_index": replica.and_then(|replica| replica.local_commit_index),
        "local_commit_index": replica.and_then(|replica| replica.local_commit_index),
        "cluster_commit_index": replica.and_then(|replica| replica.cluster_commit_index),
        "applied_index": replica.and_then(|replica| replica.applied_index),
        "matched_index": Value::Null,
        "lag_entries": Value::Null,
        "heartbeat_age_ms": Value::Null,
        "source_node_id": node_id,
        "local_source_node_id": node_id,
        "leader_node_id": replica.and_then(|replica| replica.leader_node_id).or_else(|| (detail.status.leader_node_id != 0).then_some(detail.status.leader_node_id)),
        "leader_view": replica.map(|replica| if replica.server_state == "Leader" { "LEADER" } else { "NOT_LEADER_SOURCE" }),
        "leader_replication": replica.map(|replica| replica.leader_replication.iter().map(|leader| json!({
            "node_id": leader.node_id,
            "matched_index": leader.matched_index,
            "lag_entries": leader.lag_entries,
            "last_heartbeat": leader.last_heartbeat_unix_ms.and_then(unix_ms),
        })).collect::<Vec<_>>()),
        "observed_at": rfc3339(detail.collected_at),
        "local_observed_at": rfc3339(detail.collected_at),
        "replica_observed_at": replica.and_then(|replica| unix_ms(replica.observed_at_unix_ms)),
        "voters": membership.map(|membership| membership.voters.iter().map(|member| json!({
            "node_id": member.node_id,
            "addr": member.addr,
        })).collect::<Vec<_>>()),
        "learners": membership.map(|membership| membership.learners.iter().map(|member| json!({
            "node_id": member.node_id,
            "addr": member.addr,
        })).collect::<Vec<_>>()),
        "last_snapshot_index": last_snapshot_index,
        "last_snapshot_term": last_snapshot_term,
    })
}

fn replica_projections(view: &CacheView, group_id: u64) -> Vec<Value> {
    let mut replicas = view
        .nodes
        .iter()
        .filter_map(|node| {
            node.details
                .get(&group_id)
                .map(|detail| replica_json(node.peer.node_id, group_id, detail))
        })
        .collect::<Vec<_>>();
    let leader_evidence = view
        .nodes
        .iter()
        .filter_map(|node| {
            node.details.get(&group_id).and_then(|detail| {
                detail
                    .replica
                    .as_ref()
                    .map(|replica| (node, detail, replica))
            })
        })
        .flat_map(|(node, detail, replica)| {
            replica
                .leader_replication
                .iter()
                .map(move |leader| (node.peer.node_id, detail, replica, leader))
        })
        .collect::<Vec<_>>();
    for (source_node_id, detail, replica, leader) in leader_evidence {
        let heartbeat_age_ms = leader.last_heartbeat_unix_ms.and_then(|heartbeat| {
            (replica.observed_at_unix_ms != 0)
                .then_some(replica.observed_at_unix_ms)
                .or_else(|| {
                    detail
                        .collected_at
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .ok()
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                })
                .and_then(|observed| observed.checked_sub(heartbeat))
        });
        if let Some(row) = replicas
            .iter_mut()
            .find(|row| row["node_id"] == leader.node_id)
        {
            row["matched_index"] = json!(leader.matched_index);
            row["lag_entries"] = json!(leader.lag_entries);
            row["heartbeat_age_ms"] = json!(heartbeat_age_ms);
            row["source_node_id"] = json!(source_node_id);
            row["observed_at"] = json!(rfc3339(detail.collected_at));
            row["leader_observed_at"] = json!(rfc3339(detail.collected_at));
            continue;
        }
        replicas.push(json!({
            "node_id": leader.node_id,
            "group_id": group_id,
            "role": "UNKNOWN",
            "server_state": Value::Null,
            "is_leader": Value::Null,
            "current_term": Value::Null,
            "last_log_index": Value::Null,
            "commit_index": Value::Null,
            "local_commit_index": Value::Null,
            "cluster_commit_index": Value::Null,
            "applied_index": Value::Null,
            "matched_index": leader.matched_index,
            "lag_entries": leader.lag_entries,
            "heartbeat_age_ms": heartbeat_age_ms,
            "source_node_id": source_node_id,
            "local_source_node_id": Value::Null,
            "leader_node_id": replica.leader_node_id,
            "leader_view": "LEADER",
            "leader_replication": Value::Null,
            "observed_at": rfc3339(detail.collected_at),
            "local_observed_at": Value::Null,
            "replica_observed_at": unix_ms(replica.observed_at_unix_ms),
            "voters": Value::Null,
            "learners": Value::Null,
            "last_snapshot_index": Value::Null,
            "last_snapshot_term": Value::Null,
        }));
    }
    replicas.sort_by_key(|replica| replica["node_id"].as_u64());
    replicas
}

fn unix_ms(value: u64) -> Option<String> {
    (value != 0)
        .then(|| SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(value)))
        .flatten()
        .map(rfc3339)
}

fn build_json(build: &rr_network::proto::admin::BuildInfo) -> Value {
    json!({
        "rr_version": build.rr_version,
        "app_version": build.app_version,
        "git_commit": build.git_commit,
        "git_dirty": build.git_dirty,
        "dirty": build.git_dirty,
        "build_profile": build.build_profile,
        "profile": build.build_profile,
        "admin_api": build.admin_api.as_ref().map(protocol_json),
        "raft_wire": build.raft_wire.as_ref().map(protocol_json),
        "admin_protocol": build.admin_api.as_ref().map(protocol_json),
        "raft_protocol": build.raft_wire.as_ref().map(protocol_json),
        "boot_id": build.boot_id,
        "process_start_time": unix_ms(build.process_start_unix_ms),
        "config_fingerprint": build.config_fingerprint,
    })
}

fn protocol_json(version: &rr_network::proto::admin::ProtocolVersion) -> Value {
    json!({
        "major": version.major,
        "minor": version.minor,
        "min_peer_minor": version.min_peer_minor,
    })
}

fn boot_record_json(
    record: &rr_network::proto::admin::BootRecord,
    persistence: Option<&str>,
) -> Value {
    let build = record.build_info.as_ref();
    json!({
        "boot_id": record.boot_id,
        "started_at": unix_ms(record.started_at_unix_ms),
        "ended_at": record.ended_at_unix_ms.and_then(unix_ms),
        "shutdown_state": record.shutdown_state,
        "persistence": persistence,
        "app_version": build.map(|build| build.app_version.clone()),
        "rr_version": build.map(|build| build.rr_version.clone()),
        "git_commit": build.map(|build| build.git_commit.clone()),
        "git_dirty": build.map(|build| build.git_dirty),
        "dirty": build.map(|build| build.git_dirty),
        "build_profile": build.map(|build| build.build_profile.clone()),
        "profile": build.map(|build| build.build_profile.clone()),
        "config_fingerprint": build.map(|build| build.config_fingerprint.clone()),
        "admin_api": build.and_then(|build| build.admin_api.as_ref()).map(protocol_json),
        "raft_wire": build.and_then(|build| build.raft_wire.as_ref()).map(protocol_json),
        "admin_protocol": build.and_then(|build| build.admin_api.as_ref()).map(protocol_json),
        "raft_protocol": build.and_then(|build| build.raft_wire.as_ref()).map(protocol_json),
    })
}

fn page_response<T: Clone + serde::Serialize>(
    view: &CacheView,
    items: &[T],
    page: PageQuery,
) -> Response {
    page_response_with_meta(view.meta(), items, page)
}

fn page_response_with_meta<T: Clone + serde::Serialize>(
    meta: ResponseMeta,
    items: &[T],
    page: PageQuery,
) -> Response {
    match paginate(items, page) {
        Ok(page) => ok_with_meta(meta, page),
        Err(message) => {
            error_response_with_meta(meta, StatusCode::BAD_REQUEST, "INVALID_ARGUMENT", &message)
        }
    }
}

fn ok<T: serde::Serialize>(view: &CacheView, data: T) -> Response {
    ok_with_window(view, data, None)
}

fn ok_with_window<T: serde::Serialize>(
    view: &CacheView,
    data: T,
    window: Option<String>,
) -> Response {
    let mut meta = view.meta();
    meta.time_window = window;
    ok_with_meta(meta, data)
}

fn ok_with_meta<T: serde::Serialize>(meta: ResponseMeta, data: T) -> Response {
    Json(Envelope { data, meta }).into_response()
}

fn neutral_meta() -> ResponseMeta {
    ResponseMeta::neutral(uuid::Uuid::new_v4().to_string())
}

fn neutral_ok<T: serde::Serialize>(data: T) -> Response {
    ok_with_meta(neutral_meta(), data)
}

fn neutral_error(status: StatusCode, code: &str, message: &str) -> Response {
    error_response_with_meta(neutral_meta(), status, code, message)
}

async fn with_handler_deadline(
    cache: &Arc<ConsoleCache>,
    future: impl std::future::Future<Output = Response>,
) -> Response {
    match tokio::time::timeout(ON_DEMAND_HANDLER_DEADLINE, future).await {
        Ok(response) => response,
        Err(_) => error_response(
            cache,
            StatusCode::GATEWAY_TIMEOUT,
            "HANDLER_DEADLINE_EXCEEDED",
            "on-demand Console collection exceeded the 2s handler deadline",
        ),
    }
}

fn error_response(
    cache: &Arc<ConsoleCache>,
    status: StatusCode,
    code: &str,
    message: &str,
) -> Response {
    error_response_with_meta(cache.view().meta(), status, code, message)
}

fn error_response_with_meta(
    meta: ResponseMeta,
    status: StatusCode,
    code: &str,
    message: &str,
) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorBody {
                code: code.to_string(),
                message: message.to_string(),
            },
            meta,
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::console::config::SeedPeer;
    use crate::console::connection_manager::{ConnectionManager, RuntimeFactory};
    use crate::console::connection_store::{ConnectionStore, SecretStore};
    use crate::console::model::{
        ConnectionProfile, ConnectionSecrets, ConnectionStatus, PrometheusProfile, TlsProfile,
    };
    use crate::console::upstream::{GroupDetail, NodeSnapshot, Upstream, UpstreamError};
    use axum::body::Body;
    use http_body_util::BodyExt;
    use rr_network::proto::admin::{GroupMembership, GroupStatus, SnapshotStatus};
    use std::sync::Mutex;
    use std::time::Instant;
    use tower::ServiceExt;

    #[global_allocator]
    static HTTP_TEST_ALLOCATOR: HttpTestAllocator = HttpTestAllocator;

    struct HttpTestAllocator;

    thread_local! {
        static HTTP_TEST_ALLOCATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    unsafe impl std::alloc::GlobalAlloc for HttpTestAllocator {
        unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
            HTTP_TEST_ALLOCATED.with(|allocated| allocated.set(allocated.get() + layout.size()));
            std::alloc::System.alloc(layout)
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
            std::alloc::System.dealloc(pointer, layout)
        }

        unsafe fn realloc(
            &self,
            pointer: *mut u8,
            layout: std::alloc::Layout,
            new_size: usize,
        ) -> *mut u8 {
            HTTP_TEST_ALLOCATED.with(|allocated| allocated.set(allocated.get() + new_size));
            std::alloc::System.realloc(pointer, layout, new_size)
        }
    }

    fn reset_http_test_allocation() {
        HTTP_TEST_ALLOCATED.with(|allocated| allocated.set(0));
    }

    fn http_test_allocation() -> usize {
        HTTP_TEST_ALLOCATED.with(|allocated| allocated.get())
    }

    struct LegacyUpstream;

    #[async_trait::async_trait]
    impl Upstream for LegacyUpstream {
        async fn collect_node(&self, peer: &SeedPeer) -> Result<NodeSnapshot, UpstreamError> {
            Ok(NodeSnapshot {
                node_id: peer.node_id,
                listen_addr: "127.0.0.1:51001".to_string(),
                uptime_secs: 60,
                groups: Some(vec![rr_network::proto::admin::GroupSummary {
                    group_id: 7,
                    is_leader: false,
                    current_term: 3,
                    last_log_index: 99,
                    leader_node_id: Some(0),
                }]),
                routes: Some(vec![]),
                formation: None,
                descriptor: None,
                build: None,
                boot: None,
                topology: None,
                capabilities: vec![
                    Capability::supported("node_status"),
                    Capability::unsupported("resources", "legacy AdminService"),
                ],
                collected_at: SystemTime::now(),
            })
        }

        async fn collect_group(
            &self,
            _peer: &SeedPeer,
            group_id: u64,
        ) -> Result<GroupDetail, UpstreamError> {
            Ok(GroupDetail {
                status: GroupStatus {
                    group_id,
                    is_leader: false,
                    current_term: 3,
                    last_log_index: 99,
                    leader_node_id: 0,
                },
                membership: Some(GroupMembership {
                    group_id,
                    voters: vec![],
                    learners: vec![],
                }),
                snapshot: Some(SnapshotStatus {
                    group_id,
                    snapshot_in_progress: false,
                    last_snapshot_index: None,
                    last_snapshot_term: None,
                }),
                replica: None,
                capabilities: vec![Capability::supported("group_status")],
                collected_at: SystemTime::now(),
            })
        }
    }

    struct FixedRuntimeFactory {
        scheduler: Arc<Scheduler>,
    }

    #[async_trait::async_trait]
    impl RuntimeFactory for FixedRuntimeFactory {
        async fn build(
            &self,
            _profile: &ConnectionProfile,
            _secrets: &SecretStore,
        ) -> anyhow::Result<Arc<Scheduler>> {
            Ok(self.scheduler.clone())
        }

        async fn test(
            &self,
            _profile: &ConnectionProfile,
            _admin_token: Option<String>,
            _discovery_password: Option<String>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    async fn app_state_for_scheduler(scheduler: Arc<Scheduler>, static_dir: PathBuf) -> AppState {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(ConnectionStore::load(directory.path().join("connections.json")).unwrap());
        let manager = Arc::new(ConnectionManager::with_factory(
            store,
            Arc::new(SecretStore::without_keychain()),
            Arc::new(FixedRuntimeFactory { scheduler }),
        ));
        manager
            .create_startup(http_profile("legacy"), ConnectionSecrets::default())
            .await
            .unwrap();
        manager.connect("legacy").await.unwrap();
        AppState::new(
            manager,
            AuthState::new(None, Duration::from_secs(60), false),
            static_dir,
            false,
            Some("legacy".to_string()),
        )
    }

    async fn test_app(static_dir: PathBuf) -> (Router, Arc<Scheduler>) {
        let peer = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let scheduler = Arc::new(Scheduler::new(
            Arc::new(LegacyUpstream),
            cache,
            vec![peer],
            Duration::from_secs(5),
            Duration::from_secs(1),
            4,
        ));
        scheduler.reconcile().await;
        let state = app_state_for_scheduler(scheduler.clone(), static_dir).await;
        (router(state), scheduler)
    }

    async fn json_response(response: Response) -> (u16, Value) {
        let status = response.status().as_u16();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn summary_http_body_reuses_cached_payload_without_full_copy() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let projection = SummaryProjection {
            generation: 1,
            serialized: Bytes::from(format!(
                r#"{{"payload":"{}"}}"#,
                "x".repeat(4 * 1024 * 1024)
            )),
            groups: Arc::new(Vec::new()),
            meta: cache.view().meta(),
            stream_serialized: Bytes::from(r#"{"payload":"shared"}"#),
        };
        let expected = summary_envelope(&projection, None);
        let expected_len = expected.len();
        let expected = serde_json::from_str::<Value>(&expected).unwrap();
        reset_http_test_allocation();
        let responses = (0..20)
            .map(|_| summary_response(&projection, None))
            .collect::<Vec<_>>();
        let allocated = http_test_allocation();
        println!(
            "20 summary responses allocated {allocated} bytes for a {} byte cached payload",
            projection.serialized.len()
        );
        assert!(
            allocated < 1024 * 1024,
            "20 summary responses allocated {allocated} bytes for a {} byte cached payload",
            projection.serialized.len()
        );
        for response in responses {
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
            assert_eq!(response.headers()[CONTENT_LENGTH], expected_len.to_string());
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body = serde_json::from_slice::<Value>(&bytes).unwrap();
            assert_eq!(body["data"], expected["data"]);
            assert_eq!(
                body["meta"]["schema_version"],
                expected["meta"]["schema_version"]
            );
            assert_eq!(body["meta"]["cluster_id"], expected["meta"]["cluster_id"]);
            assert_eq!(body["meta"]["partial"], expected["meta"]["partial"]);
            assert_eq!(body["meta"]["sources"], expected["meta"]["sources"]);
            assert_eq!(
                body["meta"]["capabilities"],
                expected["meta"]["capabilities"]
            );
            assert!(body["meta"]["correlation_id"].is_string());
            assert!(body["meta"]["generated_at"].is_string());
        }
    }

    #[tokio::test]
    async fn windowed_summary_reuses_data_without_reusing_request_metadata() {
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 8, 8);
        let projection = SummaryProjection {
            generation: 1,
            serialized: Bytes::from(r#"{"payload":"shared"}"#),
            groups: Arc::new(Vec::new()),
            meta: cache.view().meta(),
            stream_serialized: Bytes::from(r#"{"payload":"shared"}"#),
        };

        let (_, first) =
            json_response(summary_response(&projection, Some("15m".to_string()))).await;
        let (_, second) =
            json_response(summary_response(&projection, Some("15m".to_string()))).await;

        assert_eq!(first["data"], second["data"]);
        assert_ne!(
            first["meta"]["correlation_id"],
            second["meta"]["correlation_id"]
        );
        assert!(second["meta"]["generated_at"].as_str() >= first["meta"]["generated_at"].as_str());
    }

    #[tokio::test]
    async fn concurrent_summary_viewers_reuse_projection_for_cache_generation() {
        let upstream = Arc::new(ManyGroupsUpstream {
            group_count: Some(500),
            ..ManyGroupsUpstream::default()
        });
        let peers = (1..=50)
            .map(|node_id| SeedPeer {
                node_id,
                admin_addr: format!("127.0.0.1:5200{node_id}"),
            })
            .collect::<Vec<_>>();
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 32, 32);
        cache.register_peers(&peers);
        let scheduler = Arc::new(Scheduler::new(
            upstream,
            cache,
            peers,
            Duration::from_secs(5),
            Duration::from_secs(30),
            4,
        ));
        scheduler.reconcile().await;
        scheduler.reconcile().await;
        let state = app_state_for_scheduler(
            scheduler.clone(),
            PathBuf::from("definitely-absent-console-export"),
        )
        .await;
        let state = state.runtime_state("legacy").unwrap();
        scheduler.reconcile().await;
        let initial_started = Instant::now();
        let initial = summary_projection(&state).await;
        let initial_elapsed = initial_started.elapsed();
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        let projections = stream::iter((0..20).map(|_| {
            let state = state.clone();
            async move {
                let started = Instant::now();
                let projection = summary_projection(&state).await;
                (projection, started.elapsed())
            }
        }))
        .buffer_unordered(20)
        .collect::<Vec<_>>()
        .await;
        let mut latencies = projections
            .iter()
            .map(|(_, latency)| *latency)
            .collect::<Vec<_>>();
        latencies.sort();
        println!(
            "summary initial_build_ms={:.3} concurrent_20_p95_ms={:.3}",
            initial_elapsed.as_secs_f64() * 1_000.0,
            latencies[18].as_secs_f64() * 1_000.0
        );
        assert!(projections.iter().all(|(projection, _)| {
            projection.serialized.as_ptr() == initial.serialized.as_ptr()
                && projection.generation == initial.generation
        }));

        scheduler.reconcile().await;
        let refreshed = summary_projection(&state).await;
        assert!(refreshed.generation > initial.generation);
        assert_ne!(refreshed.serialized.as_ptr(), initial.serialized.as_ptr());
    }

    #[tokio::test]
    async fn legacy_slice_exposes_absent_leader_and_uniform_partial_metadata() {
        let (app, scheduler) = test_app(PathBuf::from("definitely-absent-console-export")).await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/console/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = json_response(response).await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["schema_version"], "console.v1");
        assert_eq!(body["meta"]["partial"], true);
        assert_eq!(body["data"]["groups"][0]["leader_node_id"], Value::Null);
        assert_eq!(body["data"]["nodes"][0]["node_id"], 1);
        assert_eq!(body["data"]["nodes"][0]["ready"], Value::Null);
        assert_eq!(body["data"]["nodes"][0]["liveness"], Value::Null);
        assert_eq!(body["data"]["nodes"][0]["ready_phase"], Value::Null);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/console/groups/7/replicas")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = json_response(response).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["replicas"][0]["leader_node_id"], Value::Null);
        assert_eq!(body["data"]["replicas"][0]["commit_index"], Value::Null);
        assert_eq!(body["data"]["replicas"][0]["matched_index"], Value::Null);
        assert_eq!(body["data"]["replicas"][0]["lag_entries"], Value::Null);
        assert_eq!(body["data"]["replicas"][0]["heartbeat_age_ms"], Value::Null);
        assert_eq!(body["data"]["replicas"][0]["source_node_id"], 1);
        assert!(body["data"]["replicas"][0]["snapshot_in_progress"].is_null());
        assert!(scheduler.cache.view().nodes[0].details.contains_key(&7));
    }

    #[tokio::test]
    async fn unknown_api_never_falls_through_to_the_spa() {
        let (app, _) = test_app(PathBuf::from("definitely-absent-console-export")).await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/console/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = json_response(response).await;
        assert_eq!(status, 404);
        assert_eq!(body["error"]["code"], "NOT_FOUND");
        assert!(body["meta"]["correlation_id"].as_str().is_some());
        assert_neutral_meta(&body);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/groups/7")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&bytes).contains("Console frontend export not found"));
    }

    #[tokio::test]
    async fn extractor_and_method_rejections_use_uniform_api_errors() {
        let (app, _) = test_app(PathBuf::from("definitely-absent-console-export")).await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/console/nodes?page_size=not-a-number")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = json_response(response).await;
        assert_eq!(status, 400);
        assert_eq!(body["error"]["code"], "INVALID_ARGUMENT");
        assert!(body["meta"]["correlation_id"].as_str().is_some());
        assert_neutral_meta(&body);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/console/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = json_response(response).await;
        assert_eq!(status, 405);
        assert_eq!(body["error"]["code"], "METHOD_NOT_ALLOWED");
        assert!(body["meta"]["correlation_id"].as_str().is_some());
        assert_neutral_meta(&body);
    }

    #[tokio::test]
    async fn sse_has_uniform_metadata_and_requests_resync_after_lag() {
        let (app, scheduler) = test_app(PathBuf::from("definitely-absent-console-export")).await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/console/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        let mut body = response.into_body().into_data_stream();
        let first = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let first = String::from_utf8_lossy(&first);
        assert!(first.contains("event: summary"), "{first}");
        assert!(first.contains("\"meta\""), "{first}");
        assert!(first.contains("\"health\""), "{first}");
        assert!(!first.contains("\"observations\""), "{first}");

        for _ in 0..100 {
            scheduler.cache.notify("summary");
        }
        let second = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&second).contains("event: resync"));
    }

    #[tokio::test]
    async fn static_export_serves_assets_and_index_fallback() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("index.html"), "<html>real export</html>").unwrap();
        std::fs::write(temp.path().join("app.js"), "console.log('ok')").unwrap();
        std::fs::create_dir(temp.path().join("overview")).unwrap();
        std::fs::write(
            temp.path().join("overview").join("index.html"),
            "<html>overview export</html>",
        )
        .unwrap();
        let (app, _) = test_app(temp.path().to_path_buf()).await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/app.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "text/javascript");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lifecycle")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), b"<html>real export</html>");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), b"<html>overview export</html>");
    }

    struct FullUpstream;

    fn full_descriptor(node_id: u64) -> rr_core::NodeDescriptor {
        rr_core::NodeDescriptor {
            node_id,
            client_addr: format!("127.0.0.1:5100{node_id}"),
            admin_addr: format!("127.0.0.1:5200{node_id}"),
            observability_addr: format!("127.0.0.1:5300{node_id}"),
            advertise_addr: format!("127.0.0.1:5400{node_id}"),
            environment: "test".to_string(),
            region: Some("local".to_string()),
            zone: Some("zone-a".to_string()),
            rack: None,
            boot_id: format!("boot-current-{node_id}"),
        }
    }

    fn full_build(node_id: u64) -> rr_network::proto::admin::BuildInfo {
        rr_network::proto::admin::BuildInfo {
            rr_version: "0.2.0".to_string(),
            app_version: "example-app".to_string(),
            git_commit: "abc123".to_string(),
            git_dirty: false,
            build_profile: "test".to_string(),
            admin_api: Some(rr_network::proto::admin::ProtocolVersion {
                major: 1,
                minor: 1,
                min_peer_minor: 0,
            }),
            raft_wire: Some(rr_network::proto::admin::ProtocolVersion {
                major: 1,
                minor: 2,
                min_peer_minor: 1,
            }),
            boot_id: format!("boot-current-{node_id}"),
            process_start_unix_ms: 1_700_000_000_000,
            config_fingerprint: "secret-free-fingerprint".to_string(),
        }
    }

    fn full_boot(node_id: u64, historic: bool) -> rr_network::proto::admin::BootRecord {
        rr_network::proto::admin::BootRecord {
            boot_id: if historic {
                format!("boot-previous-{node_id}")
            } else {
                format!("boot-current-{node_id}")
            },
            started_at_unix_ms: if historic {
                1_600_000_000_000
            } else {
                1_700_000_000_000
            },
            ended_at_unix_ms: historic.then_some(1_600_000_100_000),
            shutdown_state: if historic { "GRACEFUL" } else { "RUNNING" }.to_string(),
            build_info: Some(full_build(node_id)),
        }
    }

    #[async_trait::async_trait]
    impl Upstream for FullUpstream {
        async fn collect_node(&self, peer: &SeedPeer) -> Result<NodeSnapshot, UpstreamError> {
            let nodes = [1, 2]
                .into_iter()
                .map(|node_id| rr_network::proto::admin::NodeDirectoryEntry {
                    node_descriptor: Some(rr_network::proto::admin::NodeDescriptor {
                        node_id,
                        client_addr: format!("127.0.0.1:5100{node_id}"),
                        admin_addr: format!("127.0.0.1:5200{node_id}"),
                        observability_addr: format!("127.0.0.1:5300{node_id}"),
                        advertise_addr: format!("127.0.0.1:5400{node_id}"),
                        environment: "test".to_string(),
                        region: Some("local".to_string()),
                        zone: Some("zone-a".to_string()),
                        rack: None,
                        boot_id: format!("boot-current-{node_id}"),
                    }),
                    revision: 12,
                    lease_id: 100 + node_id as i64,
                })
                .collect();
            Ok(NodeSnapshot {
                node_id: peer.node_id,
                listen_addr: format!("127.0.0.1:5000{}", peer.node_id),
                uptime_secs: 60,
                groups: Some(vec![rr_network::proto::admin::GroupSummary {
                    group_id: 7,
                    is_leader: peer.node_id == 1,
                    current_term: 3,
                    last_log_index: 100,
                    leader_node_id: Some(1),
                }]),
                routes: Some(vec![]),
                formation: Some(rr_network::proto::admin::FormationStatus {
                    origin: "Untracked".to_string(),
                    cluster_uuid: String::new(),
                    topology_revision: 0,
                    store_revision: 0,
                    state: "Untracked".to_string(),
                    groups: vec![],
                }),
                descriptor: Some(crate::console::upstream::DescriptorEvidence {
                    descriptor: full_descriptor(peer.node_id),
                    directory_revision: 12,
                }),
                build: Some(full_build(peer.node_id)),
                boot: Some(rr_network::proto::admin::BootStatus {
                    current: Some(full_boot(peer.node_id, false)),
                    persistence: "PERSISTENT".to_string(),
                }),
                topology: Some(rr_network::proto::admin::ClusterTopology {
                    snapshot_id: "snapshot-12".to_string(),
                    config_revision: 8,
                    directory_revision: 12,
                    nodes,
                    groups: vec![rr_network::proto::admin::GroupTopology {
                        group_id: 7,
                        replica_node_ids: vec![1, 2],
                    }],
                    partitions: vec![rr_network::proto::admin::PartitionRoute {
                        partition_id: 9,
                        group_id: 7,
                        replica_node_ids: vec![1, 2],
                    }],
                    next_cursor: String::new(),
                }),
                capabilities: vec![
                    Capability::supported("node_status"),
                    Capability::supported("node_descriptor"),
                    Capability::supported("cluster_topology"),
                    Capability::supported("build_info"),
                    Capability::supported("boot_status"),
                ],
                collected_at: SystemTime::now(),
            })
        }

        async fn collect_group(
            &self,
            peer: &SeedPeer,
            group_id: u64,
        ) -> Result<GroupDetail, UpstreamError> {
            Ok(GroupDetail {
                status: GroupStatus {
                    group_id,
                    is_leader: peer.node_id == 1,
                    current_term: 3,
                    last_log_index: 100,
                    leader_node_id: 1,
                },
                membership: Some(GroupMembership {
                    group_id,
                    voters: vec![rr_network::proto::admin::NodeMember {
                        node_id: 1,
                        addr: "127.0.0.1:50001".to_string(),
                    }],
                    learners: vec![],
                }),
                snapshot: Some(SnapshotStatus {
                    group_id,
                    snapshot_in_progress: false,
                    last_snapshot_index: Some(95),
                    last_snapshot_term: Some(3),
                }),
                replica: Some(rr_network::proto::admin::GroupReplicaStatus {
                    group_id,
                    node_id: peer.node_id,
                    server_state: if peer.node_id == 1 {
                        "Leader"
                    } else {
                        "Follower"
                    }
                    .to_string(),
                    leader_node_id: Some(1),
                    current_term: 3,
                    last_log_index: Some(100),
                    local_commit_index: Some(98),
                    cluster_commit_index: Some(99),
                    applied_index: Some(97),
                    last_snapshot_index: Some(95),
                    last_snapshot_term: Some(3),
                    leader_replication: if peer.node_id == 1 {
                        vec![rr_network::proto::admin::LeaderReplicaStatus {
                            node_id: 2,
                            matched_index: Some(95),
                            lag_entries: Some(5),
                            last_heartbeat_unix_ms: Some(1_700_000_000_100),
                        }]
                    } else {
                        vec![]
                    },
                    observed_at_unix_ms: 1_700_000_000_200,
                }),
                capabilities: vec![Capability::supported("group_replica_status")],
                collected_at: SystemTime::now(),
            })
        }

        async fn list_boot_records(
            &self,
            peer: &SeedPeer,
            cursor: Option<String>,
            _page_size: usize,
        ) -> Result<rr_network::proto::admin::BootRecordPage, UpstreamError> {
            let historic = cursor.is_some();
            Ok(rr_network::proto::admin::BootRecordPage {
                records: vec![full_boot(peer.node_id, historic)],
                next_cursor: if historic {
                    String::new()
                } else {
                    "second".to_string()
                },
                persistence: "PERSISTENT".to_string(),
            })
        }
    }

    struct FreshHeartbeatUpstream;

    #[async_trait::async_trait]
    impl Upstream for FreshHeartbeatUpstream {
        async fn collect_node(&self, peer: &SeedPeer) -> Result<NodeSnapshot, UpstreamError> {
            FullUpstream.collect_node(peer).await
        }

        async fn collect_group(
            &self,
            peer: &SeedPeer,
            group_id: u64,
        ) -> Result<GroupDetail, UpstreamError> {
            let mut detail = FullUpstream.collect_group(peer, group_id).await?;
            let heartbeat_unix_ms = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            for follower in &mut detail.replica.as_mut().unwrap().leader_replication {
                follower.last_heartbeat_unix_ms = Some(heartbeat_unix_ms);
            }
            Ok(detail)
        }
    }

    #[derive(Default)]
    struct ManyGroupsUpstream {
        collect_group_calls: Arc<std::sync::atomic::AtomicUsize>,
        group_count: Option<u64>,
    }

    #[async_trait::async_trait]
    impl Upstream for ManyGroupsUpstream {
        async fn collect_node(&self, peer: &SeedPeer) -> Result<NodeSnapshot, UpstreamError> {
            let mut snapshot = FullUpstream.collect_node(peer).await?;
            snapshot.groups = Some(
                (1..=self.group_count.unwrap_or(250))
                    .map(|group_id| rr_network::proto::admin::GroupSummary {
                        group_id,
                        is_leader: peer.node_id == 1,
                        current_term: 3,
                        last_log_index: 100,
                        leader_node_id: Some(1),
                    })
                    .collect(),
            );
            Ok(snapshot)
        }

        async fn collect_group(
            &self,
            peer: &SeedPeer,
            group_id: u64,
        ) -> Result<GroupDetail, UpstreamError> {
            self.collect_group_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            FullUpstream.collect_group(peer, group_id).await
        }
    }

    async fn many_groups_app() -> (Router, Arc<std::sync::atomic::AtomicUsize>) {
        let peer = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 32, 32);
        let upstream = Arc::new(ManyGroupsUpstream::default());
        let collect_group_calls = upstream.collect_group_calls.clone();
        let scheduler = Arc::new(Scheduler::new(
            upstream,
            cache,
            vec![peer],
            Duration::from_secs(5),
            Duration::from_secs(1),
            4,
        ));
        scheduler.reconcile().await;
        scheduler.reconcile().await;
        (
            router(
                app_state_for_scheduler(
                    scheduler,
                    PathBuf::from("definitely-absent-console-export"),
                )
                .await,
            ),
            collect_group_calls,
        )
    }

    struct LeaderOnlyUpstream;

    #[async_trait::async_trait]
    impl Upstream for LeaderOnlyUpstream {
        async fn collect_node(&self, peer: &SeedPeer) -> Result<NodeSnapshot, UpstreamError> {
            FullUpstream.collect_node(peer).await
        }

        async fn collect_group(
            &self,
            peer: &SeedPeer,
            group_id: u64,
        ) -> Result<GroupDetail, UpstreamError> {
            if peer.node_id == 2 {
                return Err(UpstreamError {
                    code: "ADMIN_UNAVAILABLE",
                    message: "follower is unavailable".to_string(),
                });
            }
            FullUpstream.collect_group(peer, group_id).await
        }
    }

    struct SlowFullUpstream {
        group_delay: Duration,
        history_delay: Duration,
        active_groups: Arc<std::sync::atomic::AtomicUsize>,
        max_active_groups: Arc<std::sync::atomic::AtomicUsize>,
        active_history: Arc<std::sync::atomic::AtomicUsize>,
        max_active_history: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl SlowFullUpstream {
        fn new(group_delay: Duration, history_delay: Duration) -> Self {
            Self {
                group_delay,
                history_delay,
                active_groups: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                max_active_groups: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                active_history: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                max_active_history: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }
    }

    struct ActiveGroupCall(Arc<std::sync::atomic::AtomicUsize>);

    impl Drop for ActiveGroupCall {
        fn drop(&mut self) {
            self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl Upstream for SlowFullUpstream {
        async fn collect_node(&self, peer: &SeedPeer) -> Result<NodeSnapshot, UpstreamError> {
            FullUpstream.collect_node(peer).await
        }

        async fn collect_group(
            &self,
            peer: &SeedPeer,
            group_id: u64,
        ) -> Result<GroupDetail, UpstreamError> {
            let active = self
                .active_groups
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.max_active_groups
                .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
            let _active = ActiveGroupCall(self.active_groups.clone());
            tokio::time::sleep(self.group_delay).await;
            FullUpstream.collect_group(peer, group_id).await
        }

        async fn list_boot_records(
            &self,
            peer: &SeedPeer,
            cursor: Option<String>,
            page_size: usize,
        ) -> Result<rr_network::proto::admin::BootRecordPage, UpstreamError> {
            let active = self
                .active_history
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.max_active_history
                .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
            let _active = ActiveGroupCall(self.active_history.clone());
            tokio::time::sleep(self.history_delay).await;
            FullUpstream
                .list_boot_records(peer, cursor, page_size)
                .await
        }
    }

    #[derive(Default)]
    struct PagedBootUpstream {
        history_calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Upstream for PagedBootUpstream {
        async fn collect_node(&self, peer: &SeedPeer) -> Result<NodeSnapshot, UpstreamError> {
            FullUpstream.collect_node(peer).await
        }

        async fn collect_group(
            &self,
            peer: &SeedPeer,
            group_id: u64,
        ) -> Result<GroupDetail, UpstreamError> {
            FullUpstream.collect_group(peer, group_id).await
        }

        async fn list_boot_records(
            &self,
            peer: &SeedPeer,
            cursor: Option<String>,
            _page_size: usize,
        ) -> Result<rr_network::proto::admin::BootRecordPage, UpstreamError> {
            self.history_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let offset = cursor
                .as_deref()
                .unwrap_or("0")
                .parse::<u64>()
                .expect("test cursor");
            let mut record = full_boot(peer.node_id, offset > 0);
            record.boot_id = format!("boot-{}-{offset}", peer.node_id);
            record.started_at_unix_ms = 1_700_000_000_000 - offset * 1_000;
            Ok(rr_network::proto::admin::BootRecordPage {
                records: vec![record],
                next_cursor: if offset < 99 {
                    (offset + 1).to_string()
                } else {
                    String::new()
                },
                persistence: "PERSISTENT".to_string(),
            })
        }
    }

    async fn app_with_upstream(
        upstream: Arc<dyn Upstream>,
        peer_count: u64,
        request_timeout: Duration,
    ) -> (Router, Arc<Scheduler>) {
        let peers = (1..=peer_count)
            .map(|node_id| SeedPeer {
                node_id,
                admin_addr: format!("127.0.0.1:5200{node_id}"),
            })
            .collect::<Vec<_>>();
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 32, 32);
        cache.register_peers(&peers);
        let scheduler = Arc::new(Scheduler::new(
            upstream,
            cache,
            peers,
            Duration::from_secs(5),
            request_timeout,
            4,
        ));
        scheduler.reconcile().await;
        scheduler.reconcile().await;
        (
            router(
                app_state_for_scheduler(
                    scheduler.clone(),
                    PathBuf::from("definitely-absent-console-export"),
                )
                .await,
            ),
            scheduler,
        )
    }

    async fn full_app() -> (Router, Arc<Scheduler>) {
        let peer = SeedPeer {
            node_id: 1,
            admin_addr: "127.0.0.1:52001".to_string(),
        };
        let cache = ConsoleCache::new("test", Duration::from_secs(30), 32, 32);
        let scheduler = Arc::new(Scheduler::new(
            Arc::new(FullUpstream),
            cache,
            vec![peer],
            Duration::from_secs(5),
            Duration::from_secs(1),
            4,
        ));
        scheduler.reconcile().await;
        scheduler.reconcile().await;
        let state = app_state_for_scheduler(
            scheduler.clone(),
            PathBuf::from("definitely-absent-console-export"),
        )
        .await;
        (router(state), scheduler)
    }

    async fn get_json(app: Router, uri: &str) -> (u16, Value) {
        let response = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        json_response(response).await
    }

    #[tokio::test]
    async fn group_responses_project_voters_reachable_with_legacy_alias() {
        let (app, _) = full_app().await;
        let (summary_status, summary) = get_json(app.clone(), "/api/v1/console/summary").await;
        let (groups_status, groups) = get_json(app.clone(), "/api/v1/console/groups").await;
        let (detail_status, detail) = get_json(app, "/api/v1/console/groups/7").await;
        assert_eq!(summary_status, 200, "{summary}");
        assert_eq!(groups_status, 200, "{groups}");
        assert_eq!(detail_status, 200, "{detail}");

        for group in [
            &summary["data"]["groups"][0],
            &groups["data"]["items"][0],
            &detail["data"],
        ] {
            assert_eq!(group["expected_replica_node_ids"], json!([1, 2]));
            assert_eq!(group["source_count"], 2);
            assert_eq!(group["voters_total"], 2);
            assert_eq!(group["reachable_voters"], 2);
            assert_eq!(group["voters_reachable"].as_u64(), Some(2));
            assert_eq!(group["voters_reachable"], group["reachable_voters"]);
        }
    }

    #[tokio::test]
    async fn group_page_size_maximum_is_accepted_and_excess_is_rejected() {
        let (app, _) = full_app().await;
        let (status, body) = get_json(app.clone(), "/api/v1/console/groups?page_size=200").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["items"].as_array().unwrap().len(), 1);
        assert!(body["data"]["next_cursor"].is_null());

        let (status, body) = get_json(app, "/api/v1/console/groups?page_size=201").await;
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["error"]["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn cold_node_topology_collects_leader_evidence_on_demand() {
        let (app, scheduler) =
            app_with_upstream(Arc::new(FreshHeartbeatUpstream), 2, Duration::from_secs(30)).await;
        assert!(scheduler
            .cache
            .view()
            .nodes
            .iter()
            .all(|node| node.details.is_empty()));

        let (status, body) = get_json(app, "/api/v1/console/topology?view=nodes").await;

        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["edges"].as_array().unwrap().len(), 1);
        assert_eq!(body["data"]["edges"][0]["source_node_id"], 1);
        assert_eq!(body["data"]["edges"][0]["target_node_id"], 2);
        assert_eq!(body["data"]["edges"][0]["group_count"], 1);
        assert!(scheduler.cache.view().nodes[0].details.contains_key(&7));
    }

    #[tokio::test]
    async fn unselected_raft_topology_is_bounded_and_selected_group_keeps_detail() {
        let (app, _) = many_groups_app().await;
        let (status, body) = get_json(app.clone(), "/api/v1/console/topology?view=raft").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["groups"].as_array().unwrap().len(), 200);
        assert_eq!(body["data"]["truncated"], true);
        assert_eq!(body["data"]["total_groups"], 250);
        assert_eq!(body["data"]["returned_groups"], 200);
        assert_eq!(
            body["data"]["groups"][0]["observations"]
                .as_array()
                .unwrap()
                .len(),
            2
        );

        let (status, body) = get_json(app, "/api/v1/console/topology?view=raft&group_id=250").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["groups"].as_array().unwrap().len(), 1);
        assert_eq!(body["data"]["groups"][0]["group_id"], 250);
        assert_eq!(
            body["data"]["groups"][0]["observations"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(body["data"]["truncated"], false);
        assert_eq!(body["data"]["total_groups"], 1);
        assert_eq!(body["data"]["returned_groups"], 1);
    }

    #[tokio::test]
    async fn unfiltered_snapshots_collect_only_the_returned_page() {
        let (app, collect_group_calls) = many_groups_app().await;
        let (status, first) = get_json(app.clone(), "/api/v1/console/snapshots").await;
        assert_eq!(status, 200, "{first}");
        assert_eq!(first["data"]["items"].as_array().unwrap().len(), 50);
        assert_eq!(first["data"]["items"][0]["node_id"], 1);
        assert_eq!(first["data"]["items"][0]["group_id"], 1);
        assert_eq!(first["data"]["items"][49]["group_id"], 50);
        assert_eq!(first["data"]["items"][0]["last_snapshot_index"], 95);
        assert_eq!(
            collect_group_calls.load(std::sync::atomic::Ordering::Relaxed),
            50
        );
        let cursor = first["data"]["next_cursor"].as_str().unwrap().to_string();

        let (status, second) = get_json(
            app,
            &format!("/api/v1/console/snapshots?cursor={cursor}&page_size=50"),
        )
        .await;
        assert_eq!(status, 200, "{second}");
        assert_eq!(second["data"]["items"].as_array().unwrap().len(), 50);
        assert_eq!(second["data"]["items"][0]["group_id"], 51);
        assert_eq!(
            collect_group_calls.load(std::sync::atomic::Ordering::Relaxed),
            100
        );
    }

    #[tokio::test]
    async fn full_admin_capabilities_project_nodes_replicas_snapshots_and_topology() {
        let (app, scheduler) = full_app().await;
        let (status, body) = get_json(app.clone(), "/api/v1/console/nodes").await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["data"]["items"][0]["observability_addr"],
            "127.0.0.1:53001"
        );
        assert_eq!(
            body["data"]["items"][0]["build"]["app_version"],
            "example-app"
        );
        assert_eq!(
            body["data"]["items"][0]["boot"]["persistence"],
            "PERSISTENT"
        );
        assert!(body["data"]["items"][0]["resources"].is_null());

        let (status, body) = get_json(app.clone(), "/api/v1/console/groups/7/replicas").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["replicas"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["data"]["replicas"][0]["leader_replication"][0]["lag_entries"],
            5
        );
        assert_eq!(body["data"]["replicas"][0]["local_commit_index"], 98);
        assert_eq!(
            body["data"]["replicas"][1]["leader_view"],
            "NOT_LEADER_SOURCE"
        );
        assert_eq!(
            body["data"]["replicas"][1]["leader_replication"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert!(scheduler.cache.view().nodes[0].details.contains_key(&7));

        let (status, body) = get_json(app.clone(), "/api/v1/console/snapshots?group_id=7").await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["items"][0]["snapshot_distance"], 5);
        assert_eq!(
            body["data"]["items"][0]["build_failure_status"],
            "UNSUPPORTED"
        );
        assert!(body["data"]["items"][0]["snapshot_in_progress"].is_null());

        let (status, stale_topology) =
            get_json(app.clone(), "/api/v1/console/topology?view=nodes").await;
        assert_eq!(status, 200, "{stale_topology}");
        assert_eq!(stale_topology["data"]["edges"], json!([]));

        let peer = scheduler.cache.view().nodes[0].peer.clone();
        let mut detail = scheduler.cache.view().nodes[0].details[&7].clone();
        let heartbeat_unix_ms = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        detail.replica.as_mut().unwrap().leader_replication[0].last_heartbeat_unix_ms =
            Some(heartbeat_unix_ms);
        scheduler.cache.record_detail(&peer, 7, detail);

        let (status, node_topology) =
            get_json(app.clone(), "/api/v1/console/topology?view=nodes").await;
        assert_eq!(status, 200, "{node_topology}");
        assert_eq!(node_topology["data"]["edges"].as_array().unwrap().len(), 1);
        assert_eq!(node_topology["data"]["edges"][0]["source_node_id"], 1);
        assert_eq!(node_topology["data"]["edges"][0]["target_node_id"], 2);
        assert_eq!(node_topology["data"]["edges"][0]["group_count"], 1);
        assert!(node_topology["data"]["edges"][0]["observed_at"].is_string());

        let (status, body) = get_json(app, "/api/v1/console/topology?view=routes").await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["revision"], 8);
        assert_eq!(body["data"]["routes"][0]["partition_id"], 9);
        assert_eq!(body["data"]["routes"][0]["replica_node_ids"], json!([1, 2]));
    }

    #[tokio::test]
    async fn node_boots_deadline_is_uniform_and_preserves_shared_single_flight() {
        let upstream = Arc::new(SlowFullUpstream::new(
            Duration::ZERO,
            Duration::from_secs(30),
        ));
        let max_active_history = upstream.max_active_history.clone();
        let (app, _) = app_with_upstream(upstream, 1, Duration::from_secs(4)).await;
        let started = Instant::now();
        let responses = stream::iter(
            (0..20).map(|_| get_json(app.clone(), "/api/v1/console/nodes/1/boots?page_size=1")),
        )
        .buffer_unordered(20)
        .collect::<Vec<_>>()
        .await;
        assert_eq!(responses.len(), 20);
        for (status, body) in responses {
            assert_eq!(status, 504, "{body}");
            assert_eq!(body["error"]["code"], "HANDLER_DEADLINE_EXCEEDED");
        }
        assert!(started.elapsed() < Duration::from_secs(4));
        assert_eq!(
            max_active_history.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "all uncached requests must share the scheduler's single flight"
        );
    }

    #[tokio::test]
    async fn node_responses_project_healthy_and_degraded_observability_probes() {
        let (app, scheduler) = full_app().await;
        let peers = scheduler
            .cache
            .view()
            .nodes
            .iter()
            .map(|node| node.peer.clone())
            .collect::<Vec<_>>();
        let probe = |value, phase: Option<&str>| crate::console::cache::ProbeResult {
            value,
            phase: phase.map(str::to_string),
            error: None,
        };
        scheduler.cache.record_observability(
            &peers[0],
            probe(Some(true), None),
            probe(Some(true), Some("READY")),
        );
        scheduler.cache.record_observability(
            &peers[1],
            probe(Some(true), None),
            probe(Some(false), Some("NOT_READY")),
        );

        let (status, nodes) = get_json(app.clone(), "/api/v1/console/nodes").await;
        assert_eq!(status, 200, "{nodes}");
        let items = nodes["data"]["items"].as_array().unwrap();
        assert_eq!(items[0]["ready"], true);
        assert_eq!(items[0]["liveness"], true);
        assert_eq!(items[0]["ready_phase"], "READY");
        assert_eq!(items[1]["ready"], false);
        assert_eq!(items[1]["liveness"], true);
        assert_eq!(items[1]["ready_phase"], "NOT_READY");
        assert_eq!(items[1]["status"], "DEGRADED");

        let (status, detail) = get_json(app.clone(), "/api/v1/console/nodes/2").await;
        assert_eq!(status, 200, "{detail}");
        assert_eq!(detail["data"]["node"]["ready"], false);
        assert_eq!(detail["data"]["node"]["liveness"], true);
        assert_eq!(detail["data"]["node"]["ready_phase"], "NOT_READY");

        let (status, summary) = get_json(app.clone(), "/api/v1/console/summary").await;
        assert_eq!(status, 200, "{summary}");
        assert_eq!(summary["data"]["nodes"][0]["ready"], true);
        assert_eq!(summary["data"]["nodes"][0]["liveness"], true);
        assert_eq!(summary["data"]["nodes"][0]["ready_phase"], "READY");
        assert_eq!(summary["data"]["nodes"][1]["ready"], false);
        assert_eq!(summary["data"]["nodes"][1]["ready_phase"], "NOT_READY");

        let (status, lifecycle) = get_json(app, "/api/v1/console/lifecycle?view=current").await;
        assert_eq!(status, 200, "{lifecycle}");
        let items = lifecycle["data"]["items"].as_array().unwrap();
        assert_eq!(items[0]["status"], summary["data"]["nodes"][0]["status"]);
        assert_eq!(items[0]["ready_phase"], "READY");
        assert_eq!(items[1]["status"], "DEGRADED");
        assert_eq!(items[1]["ready_phase"], "NOT_READY");
    }

    #[tokio::test]
    async fn boot_records_expose_persistence_and_flat_version_fields() {
        let (app, _) = full_app().await;
        let (status, boots) =
            get_json(app.clone(), "/api/v1/console/nodes/1/boots?page_size=1").await;
        assert_eq!(status, 200, "{boots}");
        let record = &boots["data"]["items"][0];
        assert_eq!(record["persistence"], "PERSISTENT");
        assert_eq!(record["app_version"], "example-app");
        assert_eq!(record["rr_version"], "0.2.0");
        assert_eq!(record["git_commit"], "abc123");
        assert!(record.as_object().unwrap().get("build").is_none());

        let (status, versions) = get_json(app, "/api/v1/console/lifecycle?view=versions").await;
        assert_eq!(status, 200, "{versions}");
        for record in versions["data"]["items"].as_array().unwrap() {
            assert_eq!(record["persistence"], "PERSISTENT");
            assert_eq!(record["app_version"], "example-app");
            assert_eq!(record["rr_version"], "0.2.0");
            assert_eq!(record["git_commit"], "abc123");
        }
    }

    #[tokio::test]
    async fn boot_cursor_history_and_lifecycle_use_real_records_and_build_info() {
        let (app, _) = full_app().await;
        let (status, first) =
            get_json(app.clone(), "/api/v1/console/nodes/1/boots?page_size=1").await;
        assert_eq!(status, 200);
        assert_eq!(first["data"]["next_cursor"], "second");
        assert_eq!(first["data"]["items"][0]["shutdown_state"], "RUNNING");
        let (status, second) = get_json(
            app.clone(),
            "/api/v1/console/nodes/1/boots?cursor=second&page_size=1",
        )
        .await;
        assert_eq!(status, 200);
        assert!(second["data"]["next_cursor"].is_null());
        assert_eq!(second["data"]["items"][0]["shutdown_state"], "GRACEFUL");

        let (status, current) =
            get_json(app.clone(), "/api/v1/console/lifecycle?view=current").await;
        assert_eq!(status, 200);
        assert_eq!(
            current["data"]["items"][0]["config_fingerprint"],
            "secret-free-fingerprint"
        );
        assert_eq!(
            current["data"]["items"][0]["raft_incompatible_with"],
            json!([])
        );
        let (status, versions) = get_json(app, "/api/v1/console/lifecycle?view=versions").await;
        assert_eq!(status, 200, "{versions}");
        assert_eq!(versions["data"]["items"].as_array().unwrap().len(), 4);
        assert!(versions["data"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["current"] == true));
        assert!(versions["data"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["shutdown_state"] == "GRACEFUL"));
    }

    #[tokio::test]
    async fn resources_match_flat_ui_fields_without_collapsing_labeled_samples() {
        let (app, scheduler) = full_app().await;
        let peer = scheduler.cache.view().nodes[0].peer.clone();
        let scrape = |rx, tx, at| {
            let input = format!(
                r#"# TYPE rr_node_cpu_usage_ratio gauge
rr_node_cpu_usage_ratio{{scope="process"}} 0.25
rr_node_cpu_usage_ratio{{scope="host"}} 0.75
# TYPE rr_node_cpu_cores gauge
rr_node_cpu_cores{{scope="host"}} 8
# TYPE rr_node_cpu_limit_cores gauge
rr_node_cpu_limit_cores{{scope="cgroup"}} 2
# TYPE rr_node_memory_bytes gauge
rr_node_memory_bytes{{scope="process",kind="used"}} 100
rr_node_memory_bytes{{scope="host",kind="used"}} 800
rr_node_memory_bytes{{scope="host",kind="total"}} 1000
rr_node_memory_bytes{{scope="cgroup",kind="used"}} 300
rr_node_memory_bytes{{scope="cgroup",kind="limit"}} 400
# TYPE rr_node_network_bytes_total counter
rr_node_network_bytes_total{{scope="host",kind="rx"}} {rx}
rr_node_network_bytes_total{{scope="host",kind="tx"}} {tx}
# TYPE rr_node_filesystem_bytes gauge
rr_node_filesystem_bytes{{scope="host",kind="used",mount="/"}} 500
rr_node_filesystem_bytes{{scope="host",kind="total",mount="/"}} 1000
rr_node_filesystem_bytes{{scope="host",kind="used",mount="/data"}} 200
rr_node_filesystem_bytes{{scope="host",kind="total",mount="/data"}} 400
"#
            );
            crate::console::cache::NodeMetricsScrape {
                peer: peer.clone(),
                snapshot: crate::console::metrics::parse_prometheus(1, &input).unwrap(),
                observed_at: at,
            }
        };
        let now = SystemTime::now();
        scheduler.cache.record_metrics_batch(vec![
            scrape(1_000, 3_000, now - Duration::from_secs(20)),
            scrape(2_000, 5_000, now - Duration::from_secs(10)),
        ]);

        let (status, body) = get_json(app, "/api/v1/console/nodes/1").await;
        assert_eq!(status, 200, "{body}");
        let resources = &body["data"]["node"]["resources"];
        assert_eq!(resources["cpu_usage_ratio"], 0.25);
        assert_eq!(resources["cpu_scope"], "process");
        assert_eq!(resources["cpu_cores"].as_f64(), Some(8.0));
        assert_eq!(resources["cpu_cores_scope"], "host");
        assert_eq!(resources["cpu_limit_cores"].as_f64(), Some(2.0));
        assert_eq!(resources["cpu_limit_scope"], "cgroup");
        assert_eq!(resources["memory_used_bytes"].as_f64(), Some(800.0));
        assert_eq!(resources["memory_total_bytes"].as_f64(), Some(1000.0));
        assert_eq!(resources["memory_limit_bytes"].as_f64(), Some(400.0));
        assert_eq!(resources["memory_scope"], "host");
        assert_eq!(resources["memory_used_scope"], "host");
        assert_eq!(resources["memory_total_scope"], "host");
        assert_eq!(resources["memory_limit_scope"], "cgroup");
        assert_eq!(resources["network_rx_bytes_per_sec"], 100.0);
        assert_eq!(resources["network_tx_bytes_per_sec"], 200.0);
        assert_eq!(resources["network_scope"], "host");
        assert_eq!(resources["filesystem_used_bytes"].as_f64(), Some(500.0));
        assert_eq!(resources["filesystem_total_bytes"].as_f64(), Some(1000.0));
        assert_eq!(resources["filesystem_scope"], "host");
        assert_eq!(resources["filesystem_mount"], "/");
        assert!(resources["sampled_at"].is_string());
        assert!(resources["observed_at"].is_string());
        assert!(resources["metrics"].is_object());

        let samples = resources["samples"].as_array().unwrap();
        assert!(samples.iter().any(|sample| {
            sample["name"] == "rr_node_cpu_usage_ratio"
                && sample["labels"]["scope"] == "host"
                && sample["value"] == 0.75
        }));
        assert!(samples.iter().any(|sample| {
            sample["name"] == "rr_node_memory_bytes"
                && sample["labels"]["scope"] == "cgroup"
                && sample["labels"]["kind"] == "used"
                && sample["value"].as_f64() == Some(300.0)
        }));
        assert!(samples.iter().any(|sample| {
            sample["name"] == "rr_node_network_bytes_total"
                && sample["labels"]["kind"] == "rx"
                && sample["value"] == 100.0
                && sample["rate"] == true
        }));
        assert!(samples.iter().any(|sample| {
            sample["name"] == "rr_node_network_bytes_total"
                && sample["labels"]["kind"] == "tx"
                && sample["value"] == 200.0
                && sample["rate"] == true
        }));
        let filesystems = resources["filesystems"].as_array().unwrap();
        assert_eq!(filesystems.len(), 2);
        assert!(filesystems.iter().any(|filesystem| {
            filesystem["mount"] == "/data"
                && filesystem["used_bytes"].as_f64() == Some(200.0)
                && filesystem["total_bytes"].as_f64() == Some(400.0)
                && filesystem["scope"] == "host"
        }));
    }

    #[tokio::test]
    async fn replicas_expose_flat_ui_fields_with_leader_evidence_provenance() {
        let (app, _) = full_app().await;
        let (status, body) = get_json(app, "/api/v1/console/groups/7/replicas").await;
        assert_eq!(status, 200, "{body}");
        let replicas = body["data"]["replicas"].as_array().unwrap();
        let leader = replicas
            .iter()
            .find(|replica| replica["node_id"] == 1)
            .unwrap();
        assert_eq!(leader["commit_index"], 98);
        assert_eq!(leader["local_commit_index"], 98);
        assert_eq!(leader["cluster_commit_index"], 99);
        assert_eq!(leader["matched_index"], Value::Null);
        assert_eq!(leader["lag_entries"], Value::Null);
        assert_eq!(leader["source_node_id"], 1);
        assert_eq!(leader["leader_replication"][0]["matched_index"], 95);

        let follower = replicas
            .iter()
            .find(|replica| replica["node_id"] == 2)
            .unwrap();
        assert_eq!(follower["commit_index"], 98);
        assert_eq!(follower["matched_index"], 95);
        assert_eq!(follower["lag_entries"], 5);
        assert_eq!(follower["heartbeat_age_ms"], 100);
        assert_eq!(follower["source_node_id"], 1);
        assert_eq!(follower["local_source_node_id"], 2);
        assert_eq!(follower["observed_at"], leader["observed_at"]);
        assert!(follower["local_observed_at"].is_string());
        assert_eq!(follower["leader_replication"], json!([]));
    }

    #[tokio::test]
    async fn leader_only_rows_keep_absent_local_fields_null_and_mark_partial_errors() {
        let (app, _) =
            app_with_upstream(Arc::new(LeaderOnlyUpstream), 2, Duration::from_secs(30)).await;
        let (status, body) = get_json(app, "/api/v1/console/groups/7/replicas").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["meta"]["partial"], true);
        assert!(body["meta"]["capabilities"].as_array().unwrap().iter().any(
            |capability| capability["feature"] == "group_replica_status.node.2"
                && capability["status"] == "ERROR"
        ));
        let follower = body["data"]["replicas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|replica| replica["node_id"] == 2)
            .unwrap()
            .clone();
        assert_eq!(follower["role"], "UNKNOWN");
        assert_eq!(follower["commit_index"], Value::Null);
        assert_eq!(follower["applied_index"], Value::Null);
        assert_eq!(follower["matched_index"], 95);
        assert_eq!(follower["lag_entries"], 5);
        assert_eq!(follower["heartbeat_age_ms"], 100);
        assert_eq!(follower["source_node_id"], 1);
        assert_eq!(follower["local_source_node_id"], Value::Null);
    }

    #[tokio::test]
    async fn lifecycle_and_boot_versions_supply_flat_protocol_and_compatibility_fields() {
        let (app, _) = full_app().await;
        let (status, current) =
            get_json(app.clone(), "/api/v1/console/lifecycle?view=current").await;
        assert_eq!(status, 200, "{current}");
        for item in current["data"]["items"].as_array().unwrap() {
            assert_eq!(item["dirty"], false);
            assert_eq!(item["git_dirty"], false);
            assert_eq!(item["admin_protocol"]["major"], 1);
            assert_eq!(item["admin_protocol"]["minor"], 1);
            assert_eq!(item["raft_protocol"]["minor"], 2);
            assert_eq!(item["compatibility"]["status"], "COMPATIBLE");
            assert_eq!(item["compatibility"]["incompatible_node_ids"], json!([]));
        }

        let (status, versions) = get_json(app, "/api/v1/console/lifecycle?view=versions").await;
        assert_eq!(status, 200, "{versions}");
        let items = versions["data"]["items"].as_array().unwrap();
        let current = items.iter().find(|item| item["current"] == true).unwrap();
        assert_eq!(current["persistence"], "PERSISTENT");
        assert_eq!(current["dirty"], false);
        assert_eq!(current["admin_protocol"]["major"], 1);
        assert_eq!(current["raft_protocol"]["minor"], 2);
        assert_eq!(current["compatibility"]["status"], "COMPATIBLE");
        let historic = items.iter().find(|item| item["current"] == false).unwrap();
        assert_eq!(historic["dirty"], false);
        assert_eq!(historic["admin_protocol"]["major"], 1);
        assert_eq!(historic["raft_protocol"]["minor"], 2);
        assert_eq!(historic["compatibility"]["status"], "UNKNOWN");
    }

    #[tokio::test]
    async fn lifecycle_small_pages_fetch_only_bounded_recent_history_per_node() {
        let upstream = Arc::new(PagedBootUpstream::default());
        let history_calls = upstream.history_calls.clone();
        let (app, _) = app_with_upstream(upstream, 2, Duration::from_secs(30)).await;
        let (status, body) = get_json(
            app.clone(),
            "/api/v1/console/lifecycle?view=restarts&page_size=5",
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["items"].as_array().unwrap().len(), 5);
        assert!(body["data"]["next_cursor"].is_string());
        assert_eq!(
            history_calls.load(std::sync::atomic::Ordering::SeqCst),
            12,
            "two nodes need at most page_size + 1 records each"
        );

        let (status, repeated) = get_json(
            app.clone(),
            "/api/v1/console/lifecycle?view=restarts&page_size=5",
        )
        .await;
        assert_eq!(status, 200, "{repeated}");
        assert_eq!(repeated["data"]["items"], body["data"]["items"]);
        assert_eq!(
            history_calls.load(std::sync::atomic::Ordering::SeqCst),
            12,
            "identical five-second refreshes reuse the scheduler's cached pages"
        );

        let cursor = body["data"]["next_cursor"].as_str().unwrap();
        let (status, older) = get_json(
            app,
            &format!("/api/v1/console/lifecycle?view=restarts&page_size=5&cursor={cursor}"),
        )
        .await;
        assert_eq!(status, 200, "{older}");
        assert_eq!(older["data"]["items"].as_array().unwrap().len(), 5);
        assert_ne!(older["data"]["items"], body["data"]["items"]);
        assert!(older["data"]["next_cursor"].is_string());
    }

    #[tokio::test]
    async fn replica_fanout_is_concurrent_and_bounded_by_scheduler_permits() {
        let upstream = Arc::new(SlowFullUpstream::new(
            Duration::from_millis(50),
            Duration::ZERO,
        ));
        let max_active_groups = upstream.max_active_groups.clone();
        let (app, _) = app_with_upstream(upstream, 8, Duration::from_secs(30)).await;
        let (status, body) = get_json(app, "/api/v1/console/groups/7/replicas").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["replicas"].as_array().unwrap().len(), 8);
        assert_eq!(
            max_active_groups.load(std::sync::atomic::Ordering::SeqCst),
            4
        );
    }

    #[tokio::test]
    async fn on_demand_handlers_have_an_overall_two_second_deadline() {
        let upstream = Arc::new(SlowFullUpstream::new(
            Duration::from_secs(30),
            Duration::from_secs(30),
        ));
        let (app, _) = app_with_upstream(upstream, 8, Duration::from_secs(30)).await;
        for uri in [
            "/api/v1/console/groups/7/replicas",
            "/api/v1/console/snapshots?page_size=5",
            "/api/v1/console/lifecycle?view=restarts&page_size=5",
        ] {
            let (status, body) = get_json(app.clone(), uri).await;
            assert_eq!(status, 504, "{uri}: {body}");
            assert_eq!(body["error"]["code"], "HANDLER_DEADLINE_EXCEEDED");
        }
    }

    #[tokio::test]
    async fn no_observability_or_history_preserves_legacy_unsupported_performance() {
        let (app, _) = test_app(PathBuf::from("definitely-absent-console-export")).await;
        let (status, body) = get_json(app, "/api/v1/console/metrics/performance").await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["status"], "UNSUPPORTED");
        assert_eq!(body["data"]["series"], json!([]));
        assert_eq!(body["data"]["retention_source"], "NONE");
        assert_eq!(
            body["data"]["reason"],
            "legacy AdminService has no Prometheus performance projection"
        );
    }

    #[tokio::test]
    async fn live_metrics_project_performance_and_snapshot_evidence_without_cohort_subtraction() {
        let (app, scheduler) = full_app().await;
        let peer = scheduler.cache.view().nodes[0].peer.clone();
        let scrape = |requests, installs, at| {
            let input = format!(
                r#"# TYPE rr_server_requests_total counter
rr_server_requests_total{{group_id="7",op="write",outcome="success"}} {requests}
# TYPE rr_snapshot_installs_total counter
rr_snapshot_installs_total{{group_id="7"}} {installs}
# TYPE rr_snapshot_build_failures_total counter
rr_snapshot_build_failures_total 2
# TYPE rr_raft_is_leader gauge
rr_raft_is_leader{{group_id="7"}} 1
# TYPE rr_raft_replication_lag gauge
rr_raft_replication_lag{{group_id="7",follower_node_id="2"}} 5
# TYPE rr_server_request_duration_seconds histogram
rr_server_request_duration_seconds_bucket{{group_id="7",op="write",le="0.1"}} 4
rr_server_request_duration_seconds_bucket{{group_id="7",op="write",le="0.5"}} 8
rr_server_request_duration_seconds_bucket{{group_id="7",op="write",le="+Inf"}} 8
rr_server_request_duration_seconds_sum{{group_id="7",op="write"}} 4
rr_server_request_duration_seconds_count{{group_id="7",op="write"}} 8
"#
            );
            crate::console::cache::NodeMetricsScrape {
                peer: peer.clone(),
                snapshot: crate::console::metrics::parse_prometheus(1, &input).unwrap(),
                observed_at: at,
            }
        };
        let now = SystemTime::now();
        scheduler.cache.record_metrics_batch(vec![
            scrape(10, 1, now - Duration::from_secs(20)),
            scrape(30, 3, now - Duration::from_secs(10)),
        ]);

        let (status, body) = get_json(
            app.clone(),
            "/api/v1/console/metrics/performance?window=15m&source=CONSOLE_MEMORY&op=write",
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["retention_source"], "CONSOLE_MEMORY");
        let series = body["data"]["series"].as_array().unwrap();
        let write_tps = series
            .iter()
            .find(|series| series["metric"] == "write_tps")
            .unwrap();
        assert_eq!(write_tps["points"][0]["value"], 2.0);
        let write_p99 = series
            .iter()
            .find(|series| series["metric"] == "write_p99_ms")
            .unwrap();
        assert!((write_p99["points"][0]["value"].as_f64().unwrap() - 492.0).abs() < 1e-6);
        assert!(body["data"]["notices"]
            .as_array()
            .unwrap()
            .iter()
            .any(|notice| notice["feature"] == "UNALIGNED_COHORTS"));
        assert!(!series.iter().any(|series| {
            series["metric"].as_str().is_some_and(|metric| {
                metric.contains("composition") || metric.contains("subtraction")
            })
        }));

        let (status, snapshots) =
            get_json(app, "/api/v1/console/snapshots?group_id=7&node_id=1").await;
        assert_eq!(status, 200);
        assert_eq!(snapshots["data"]["items"][0]["install_count"], 3);
        assert_eq!(snapshots["data"]["items"][0]["install_rate"], 0.2);
        assert_eq!(
            snapshots["data"]["items"][0]["build_failure_status"],
            "UNSUPPORTED"
        );
        assert!(snapshots["meta"]["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| {
                capability["feature"] == "snapshot_build_failures.node.1"
                    && capability["status"] == "OBSERVED"
            }));
    }

    async fn history_server(
        metric: Value,
    ) -> (
        String,
        tokio::sync::mpsc::Receiver<std::collections::HashMap<String, String>>,
    ) {
        let (query_tx, query_rx) = tokio::sync::mpsc::channel(32);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = axum::Router::new().route(
                "/api/v1/query_range",
                axum::routing::get(
                    move |axum::extract::Query(query): axum::extract::Query<
                        std::collections::HashMap<String, String>,
                    >| {
                        let query_tx = query_tx.clone();
                        let metric = metric.clone();
                        async move {
                            query_tx.send(query).await.unwrap();
                            axum::Json(json!({
                                "status":"success",
                                "data":{"resultType":"matrix","result":[{
                                    "metric":metric,
                                    "values":[[1700000000.0,"2.5"]]
                                }]}
                            }))
                        }
                    },
                ),
            );
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{address}"), query_rx)
    }

    #[tokio::test]
    async fn prometheus_history_uses_fixed_queries_and_explicit_retention() {
        let (url, mut queries) = history_server(json!({
            "cluster_id":"prod-east",
            "node_id":"1",
            "group_id":"7",
            "op":"write",
            "instance":"127.0.0.1:9091"
        }))
        .await;
        let (app, scheduler) = full_app().await;
        scheduler
            .configure_history(crate::console::config::PrometheusConfig {
                url,
                cluster: crate::console::metrics::ClusterMatcher {
                    label: "cluster_id".to_string(),
                    value: "prod-east".to_string(),
                },
            })
            .unwrap();
        let (status, body) = get_json(
            app,
            "/api/v1/console/metrics/performance?source=PROMETHEUS&window=15m&group_id=7&node_id=1&op=write",
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["retention_source"], "PROMETHEUS");
        assert!(!body["data"]["series"].as_array().unwrap().is_empty());
        while let Ok(query) = queries.try_recv() {
            assert!(query["query"].contains("cluster_id=\"prod-east\""));
            assert!(query["query"].contains("node_id=\"1\""));
            assert!(query["query"].contains("group_id=\"7\""));
            assert!(!query["query"].contains("instance="));
        }
    }

    #[tokio::test]
    async fn prometheus_history_reports_unmapped_instead_of_guessing_from_instance() {
        let (url, _queries) = history_server(json!({
            "cluster_id":"prod-east",
            "instance":"127.0.0.1:9091"
        }))
        .await;
        let (app, scheduler) = full_app().await;
        scheduler
            .configure_history(crate::console::config::PrometheusConfig {
                url,
                cluster: crate::console::metrics::ClusterMatcher {
                    label: "cluster_id".to_string(),
                    value: "prod-east".to_string(),
                },
            })
            .unwrap();
        let (status, body) = get_json(
            app,
            "/api/v1/console/metrics/performance?source=PROMETHEUS&window=15m",
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["status"], "UNMAPPED");
        assert_eq!(body["data"]["series"], json!([]));
        assert_eq!(body["meta"]["partial"], true);
    }

    #[tokio::test]
    async fn legacy_boot_history_is_explicitly_unsupported_not_empty_success() {
        let (app, _) = test_app(PathBuf::from("definitely-absent-console-export")).await;
        let (status, body) = get_json(app, "/api/v1/console/nodes/1/boots").await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["items"], json!([]));
        assert!(body["meta"]["partial"] == true);
        assert!(body["meta"]["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| {
                capability["feature"] == "boot_history" && capability["status"] == "UNSUPPORTED"
            }));
    }

    struct HttpRuntimeFactory;

    #[async_trait::async_trait]
    impl RuntimeFactory for HttpRuntimeFactory {
        async fn build(
            &self,
            profile: &ConnectionProfile,
            _secrets: &SecretStore,
        ) -> anyhow::Result<Arc<Scheduler>> {
            if profile.name == "connect-failure" {
                anyhow::bail!("private-connect-secret must not escape");
            }
            let cache = ConsoleCache::new(
                profile
                    .cluster_name
                    .clone()
                    .unwrap_or_else(|| profile.id.clone()),
                Duration::from_secs(30),
                32,
                32,
            );
            cache.register_peers(&profile.admin_seeds);
            let scheduler = Arc::new(Scheduler::new(
                Arc::new(LegacyUpstream),
                cache,
                profile.admin_seeds.clone(),
                Duration::from_secs(5),
                Duration::from_secs(1),
                4,
            ));
            scheduler.reconcile().await;
            if let Some(prometheus) = &profile.prometheus {
                scheduler.configure_history(crate::console::config::PrometheusConfig {
                    url: prometheus.url.clone(),
                    cluster: crate::console::config::parse_cluster_matcher(&prometheus.cluster)?,
                })?;
            }
            Ok(scheduler)
        }

        async fn test(
            &self,
            profile: &ConnectionProfile,
            _admin_token: Option<String>,
            _discovery_password: Option<String>,
        ) -> anyhow::Result<()> {
            if profile.name == "test-failure" {
                anyhow::bail!("private-test-secret must not escape");
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingRuntimeFactory {
        tested_secrets: Mutex<Vec<(Option<String>, Option<String>)>>,
    }

    #[async_trait::async_trait]
    impl RuntimeFactory for RecordingRuntimeFactory {
        async fn build(
            &self,
            profile: &ConnectionProfile,
            secrets: &SecretStore,
        ) -> anyhow::Result<Arc<Scheduler>> {
            HttpRuntimeFactory.build(profile, secrets).await
        }

        async fn test(
            &self,
            profile: &ConnectionProfile,
            admin_token: Option<String>,
            discovery_password: Option<String>,
        ) -> anyhow::Result<()> {
            self.tested_secrets
                .lock()
                .unwrap()
                .push((admin_token.clone(), discovery_password.clone()));
            HttpRuntimeFactory
                .test(profile, admin_token, discovery_password)
                .await
        }
    }

    fn http_profile(id: &str) -> ConnectionProfile {
        ConnectionProfile {
            id: id.to_string(),
            name: id.to_string(),
            environment: None,
            admin_seeds: vec![SeedPeer {
                node_id: 1,
                admin_addr: "127.0.0.1:52001".to_string(),
            }],
            discovery: None,
            cluster_name: Some(id.to_string()),
            admin_tls: TlsProfile::default(),
            discovery_tls: TlsProfile::default(),
            discovery_username: None,
            discovery_allow_insecure: false,
            prometheus: None,
            poll_interval_secs: 5,
            created_at: String::new(),
            updated_at: String::new(),
            observed_cluster_id: None,
            last_connected_at: None,
        }
    }

    fn http_connection_body(name: &str) -> String {
        json!({
            "name": name,
            "environment": Value::Null,
            "admin_seeds": [{"node_id": 1, "admin_addr": "127.0.0.1:52001"}],
            "discovery": Value::Null,
            "cluster_name": name,
            "admin_tls": {"ca": Value::Null, "cert": Value::Null, "key": Value::Null, "domain": Value::Null},
            "discovery_tls": {"ca": Value::Null, "cert": Value::Null, "key": Value::Null, "domain": Value::Null},
            "discovery_username": Value::Null,
            "discovery_allow_insecure": false,
            "prometheus": Value::Null,
            "poll_interval_secs": 5,
            "admin_token": "private-http-secret",
            "remember": false
        })
        .to_string()
    }

    fn http_manager() -> (tempfile::TempDir, Arc<ConnectionManager>) {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(ConnectionStore::load(directory.path().join("connections.json")).unwrap());
        let manager = Arc::new(ConnectionManager::with_factory(
            store,
            Arc::new(SecretStore::without_keychain()),
            Arc::new(HttpRuntimeFactory),
        ));
        (directory, manager)
    }

    fn http_manager_app(
        manager: Arc<ConnectionManager>,
        legacy_connection: Option<&str>,
        viewer_token: Option<Arc<str>>,
    ) -> Router {
        router(AppState::new(
            manager,
            AuthState::new(viewer_token, Duration::from_secs(60), false),
            PathBuf::from("definitely-absent-console-export"),
            false,
            legacy_connection.map(str::to_string),
        ))
    }

    async fn request_json(
        app: &Router,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> (u16, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(HOST, "localhost")
            .header(CONTENT_TYPE, "application/json");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status().as_u16();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, body)
    }

    fn assert_neutral_meta(body: &Value) {
        assert!(body["meta"]["cluster_id"].is_null(), "{body}");
        assert_eq!(body["meta"]["retention_source"], "NONE", "{body}");
        assert_eq!(body["meta"]["sources"], json!([]), "{body}");
        assert_eq!(body["meta"]["capabilities"], json!([]), "{body}");
        assert!(body["meta"]["correlation_id"].is_string(), "{body}");
    }

    async fn create_http_connection(app: &Router, name: &str) -> Value {
        let (status, body) = request_json(
            app,
            "POST",
            "/api/v1/console/connections",
            &[("Origin", "http://localhost")],
            &http_connection_body(name),
        )
        .await;
        assert_eq!(status, 201, "{body}");
        assert_neutral_meta(&body);
        body["data"].clone()
    }

    #[tokio::test]
    async fn candidate_keep_resolves_saved_secrets_only_inside_the_bff() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(ConnectionStore::load(directory.path().join("connections.json")).unwrap());
        let factory = Arc::new(RecordingRuntimeFactory::default());
        let manager = Arc::new(ConnectionManager::with_factory(
            store,
            Arc::new(SecretStore::without_keychain()),
            factory.clone(),
        ));
        manager
            .create(
                http_profile("saved"),
                ConnectionSecrets {
                    admin_token: SecretInput::Replace("saved-admin-secret".to_string()),
                    discovery_password: SecretInput::Replace("saved-discovery-secret".to_string()),
                },
                false,
            )
            .await
            .unwrap();
        let app = http_manager_app(manager, None, None);
        let mut candidate: Value =
            serde_json::from_str(&http_connection_body("candidate")).unwrap();
        candidate.as_object_mut().unwrap().remove("admin_token");
        candidate["existing_connection_id"] = json!("saved");

        let (status, response) = request_json(
            &app,
            "POST",
            "/api/v1/console/connections/test",
            &[("Origin", "http://localhost")],
            &candidate.to_string(),
        )
        .await;
        assert_eq!(status, 200, "{response}");
        assert_eq!(
            factory.tested_secrets.lock().unwrap().as_slice(),
            &[(
                Some("saved-admin-secret".to_string()),
                Some("saved-discovery-secret".to_string()),
            )]
        );
        assert!(!response.to_string().contains("saved-admin-secret"));
        assert!(!response.to_string().contains("saved-discovery-secret"));

        candidate["admin_token"] = Value::Null;
        candidate["discovery_password"] = json!("replacement-discovery-secret");
        let (status, response) = request_json(
            &app,
            "POST",
            "/api/v1/console/connections/test",
            &[("Origin", "http://localhost")],
            &candidate.to_string(),
        )
        .await;
        assert_eq!(status, 200, "{response}");
        assert_eq!(
            factory.tested_secrets.lock().unwrap().as_slice()[1],
            (None, Some("replacement-discovery-secret".to_string()))
        );
        assert!(!response
            .to_string()
            .contains("replacement-discovery-secret"));

        candidate["existing_connection_id"] = json!("missing");
        let (status, response) = request_json(
            &app,
            "POST",
            "/api/v1/console/connections/test",
            &[("Origin", "http://localhost")],
            &candidate.to_string(),
        )
        .await;
        assert_eq!(status, 404, "{response}");
        assert!(!response
            .to_string()
            .contains("replacement-discovery-secret"));
    }

    #[tokio::test]
    async fn connection_http_contract_rejects_unsafe_and_inconsistent_profiles() {
        let (_directory, manager) = http_manager();
        let app = http_manager_app(manager, None, None);
        let mut bodies = Vec::new();

        let mut admin_userinfo: Value =
            serde_json::from_str(&http_connection_body("admin-userinfo")).unwrap();
        admin_userinfo["admin_seeds"][0]["admin_addr"] =
            json!("user:endpoint-secret@127.0.0.1:52001");
        bodies.push(admin_userinfo);

        let mut discovery_userinfo: Value =
            serde_json::from_str(&http_connection_body("discovery-userinfo")).unwrap();
        discovery_userinfo["admin_seeds"] = json!([]);
        discovery_userinfo["discovery"] = json!("etcd://user:endpoint-secret@127.0.0.1:2379");
        discovery_userinfo["discovery_allow_insecure"] = json!(true);
        bodies.push(discovery_userinfo);

        for (name, discovery, insecure, username, admin_tls, discovery_tls, port, prometheus) in [
            (
                "discovery-without-transport",
                true,
                false,
                false,
                false,
                false,
                52001,
                None,
            ),
            (
                "discovery-ca-and-insecure",
                true,
                true,
                false,
                false,
                true,
                52001,
                None,
            ),
            (
                "plaintext-username",
                true,
                true,
                true,
                false,
                false,
                52001,
                None,
            ),
            (
                "admin-cert-without-ca",
                false,
                false,
                false,
                true,
                false,
                52001,
                None,
            ),
            (
                "discovery-domain-without-ca",
                true,
                true,
                false,
                false,
                false,
                52001,
                None,
            ),
            ("zero-port", false, false, false, false, false, 0, None),
            (
                "overflow-port",
                false,
                false,
                false,
                false,
                false,
                65536,
                None,
            ),
            (
                "prometheus-scheme",
                false,
                false,
                false,
                false,
                false,
                52001,
                Some("ftp://prometheus.internal"),
            ),
        ] {
            let mut body: Value = serde_json::from_str(&http_connection_body(name)).unwrap();
            if discovery {
                body["admin_seeds"] = json!([]);
                body["discovery"] = json!("etcd://127.0.0.1:2379");
                body["discovery_allow_insecure"] = json!(insecure);
            }
            if username {
                body["discovery_username"] = json!("operator");
            }
            if admin_tls {
                body["admin_tls"]["cert"] = json!("/etc/rr/client.pem");
                body["admin_tls"]["key"] = json!("/etc/rr/client.key");
            }
            if discovery_tls {
                body["discovery_tls"]["ca"] = json!("/etc/rr/ca.pem");
            }
            if name == "discovery-domain-without-ca" {
                body["discovery_tls"]["domain"] = json!("etcd.internal");
            }
            if !discovery {
                body["admin_seeds"][0]["admin_addr"] = json!(format!("127.0.0.1:{port}"));
            }
            if let Some(url) = prometheus {
                body["prometheus"] = json!({"url": url, "cluster": "cluster_id=test"});
            }
            bodies.push(body);
        }

        for body in bodies {
            let (status, response) = request_json(
                &app,
                "POST",
                "/api/v1/console/connections",
                &[("Origin", "http://localhost")],
                &body.to_string(),
            )
            .await;
            assert_eq!(status, 400, "{}: {response}", body["name"]);
            assert_eq!(response["error"]["code"], "INVALID_CONNECTION_PROFILE");
            assert!(!response.to_string().contains("endpoint-secret"));
        }
        let (status, list) =
            request_json(&app, "GET", "/api/v1/console/connections", &[], "").await;
        assert_eq!(status, 200, "{list}");
        assert_eq!(list["data"]["items"], json!([]));
        assert!(!list.to_string().contains("endpoint-secret"));
    }

    #[tokio::test]
    async fn connection_crud_test_and_lifecycle_routes_are_neutral_and_redacted() {
        let (_directory, manager) = http_manager();
        let app = http_manager_app(manager.clone(), None, None);
        let created = create_http_connection(&app, "alpha").await;
        let id = created["id"].as_str().unwrap().to_string();
        assert_eq!(created["source"], "PROFILE");
        assert_eq!(created["status"], "DISCONNECTED");
        assert!(created["node_count"].is_null());
        assert!(!created.to_string().contains("private-http-secret"));
        assert!(created.get("admin_token").is_none());

        let (status, list) =
            request_json(&app, "GET", "/api/v1/console/connections", &[], "").await;
        assert_eq!(status, 200, "{list}");
        assert_eq!(list["data"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(list["data"]["keychain_available"], false);
        assert_neutral_meta(&list);

        let (status, candidate) = request_json(
            &app,
            "POST",
            "/api/v1/console/connections/test",
            &[("Origin", "http://localhost")],
            &http_connection_body("candidate"),
        )
        .await;
        assert_eq!(status, 200, "{candidate}");
        assert_eq!(candidate["data"]["tested"], true);
        assert_neutral_meta(&candidate);
        assert!(!candidate.to_string().contains("private-http-secret"));

        let (status, fetched) = request_json(
            &app,
            "GET",
            &format!("/api/v1/console/connections/{id}"),
            &[],
            "",
        )
        .await;
        assert_eq!(status, 200, "{fetched}");
        assert_eq!(fetched["data"]["id"], id);
        assert_neutral_meta(&fetched);

        for action in ["test", "connect"] {
            let (status, body) = request_json(
                &app,
                "POST",
                &format!("/api/v1/console/connections/{id}/{action}"),
                &[("Origin", "http://localhost")],
                "",
            )
            .await;
            assert_eq!(status, 200, "{action}: {body}");
            assert_neutral_meta(&body);
        }
        let connected = manager.get(&id).unwrap();
        assert_eq!(connected.status, ConnectionStatus::Degraded);
        assert_eq!(connected.node_count, Some(1));

        let (status, active) = request_json(
            &app,
            "PATCH",
            &format!("/api/v1/console/connections/{id}"),
            &[("Origin", "http://localhost")],
            &http_connection_body("blocked"),
        )
        .await;
        assert_eq!(status, 409, "{active}");
        assert_eq!(active["error"]["code"], "CONNECTION_ACTIVE");
        assert_neutral_meta(&active);

        let (status, disconnected) = request_json(
            &app,
            "POST",
            &format!("/api/v1/console/connections/{id}/disconnect"),
            &[("Origin", "http://localhost")],
            "",
        )
        .await;
        assert_eq!(status, 200, "{disconnected}");
        assert_eq!(disconnected["data"]["status"], "DISCONNECTED");
        assert!(disconnected["data"]["node_count"].is_null());

        let (status, updated) = request_json(
            &app,
            "PATCH",
            &format!("/api/v1/console/connections/{id}"),
            &[("Origin", "http://localhost")],
            &http_connection_body("renamed"),
        )
        .await;
        assert_eq!(status, 200, "{updated}");
        assert_eq!(updated["data"]["name"], "renamed");
        assert!(!updated.to_string().contains("private-http-secret"));

        let (status, deleted) = request_json(
            &app,
            "DELETE",
            &format!("/api/v1/console/connections/{id}"),
            &[("Origin", "http://localhost")],
            "",
        )
        .await;
        assert_eq!(status, 204, "{deleted}");
        let (status, missing) = request_json(
            &app,
            "GET",
            &format!("/api/v1/console/connections/{id}"),
            &[],
            "",
        )
        .await;
        assert_eq!(status, 404, "{missing}");
        assert_eq!(missing["error"]["code"], "CONNECTION_NOT_FOUND");
        assert_neutral_meta(&missing);
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn connection_errors_cover_credentials_limits_and_sanitized_failures() {
        let (_directory, manager) = http_manager();
        let app = http_manager_app(manager.clone(), None, None);
        let mut credentials = http_profile("credentials");
        credentials.discovery = Some("etcd://127.0.0.1:2379".to_string());
        credentials.discovery_tls.ca = Some(PathBuf::from("unused-ca.pem"));
        credentials.discovery_username = Some("operator".to_string());
        manager
            .create(credentials, ConnectionSecrets::default(), false)
            .await
            .unwrap();
        for action in ["test", "connect"] {
            let (status, body) = request_json(
                &app,
                "POST",
                &format!("/api/v1/console/connections/credentials/{action}"),
                &[("Origin", "http://localhost")],
                "",
            )
            .await;
            assert_eq!(status, 409, "{body}");
            assert_eq!(body["error"]["code"], "CREDENTIALS_REQUIRED");
            assert_neutral_meta(&body);
        }

        let failed_test = create_http_connection(&app, "test-failure").await;
        let (status, failure) = request_json(
            &app,
            "POST",
            &format!(
                "/api/v1/console/connections/{}/test",
                failed_test["id"].as_str().unwrap()
            ),
            &[("Origin", "http://localhost")],
            "",
        )
        .await;
        assert_eq!(status, 502, "{failure}");
        assert_eq!(failure["error"]["code"], "CONNECTION_TEST_FAILED");
        assert!(!failure.to_string().contains("private-test-secret"));
        assert_neutral_meta(&failure);

        let failed_connect = create_http_connection(&app, "connect-failure").await;
        let (status, failure) = request_json(
            &app,
            "POST",
            &format!(
                "/api/v1/console/connections/{}/connect",
                failed_connect["id"].as_str().unwrap()
            ),
            &[("Origin", "http://localhost")],
            "",
        )
        .await;
        assert_eq!(status, 502, "{failure}");
        assert_eq!(failure["error"]["code"], "CONNECTION_INITIALIZATION_FAILED");
        assert!(!failure.to_string().contains("private-connect-secret"));

        for index in 0..9 {
            manager
                .create(
                    http_profile(&format!("limited-{index}")),
                    ConnectionSecrets::default(),
                    false,
                )
                .await
                .unwrap();
        }
        for index in 0..8 {
            manager.connect(&format!("limited-{index}")).await.unwrap();
        }
        let (status, limited) = request_json(
            &app,
            "POST",
            "/api/v1/console/connections/limited-8/connect",
            &[("Origin", "http://localhost")],
            "",
        )
        .await;
        assert_eq!(status, 429, "{limited}");
        assert_eq!(limited["error"]["code"], "ACTIVE_CONNECTION_LIMIT");
        assert_neutral_meta(&limited);
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn every_connection_mutation_requires_origin_csrf_and_a_session() {
        let (_directory, manager) = http_manager();
        let app = http_manager_app(manager.clone(), None, Some(Arc::from("viewer-secret")));
        let (status, unauthenticated) =
            request_json(&app, "GET", "/api/v1/console/connections", &[], "").await;
        assert_eq!(status, 401);
        assert_eq!(unauthenticated["error"]["code"], "UNAUTHENTICATED");
        assert_neutral_meta(&unauthenticated);

        let (status, rejected_login) = request_json(
            &app,
            "POST",
            "/api/v1/console/auth/login",
            &[],
            r#"{"token":"wrong"}"#,
        )
        .await;
        assert_eq!(status, 401);
        assert_neutral_meta(&rejected_login);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/console/auth/login")
                    .header(HOST, "localhost")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"token":"viewer-secret"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cookie = response
            .headers()
            .get(SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string();
        let (_, login) = json_response(response).await;
        assert_neutral_meta(&login);
        let csrf = login["csrf_token"].as_str().unwrap().to_string();
        let (status, csrf_response) = request_json(
            &app,
            "GET",
            "/api/v1/console/auth/csrf",
            &[("Cookie", &cookie)],
            "",
        )
        .await;
        assert_eq!(status, 200);
        assert_neutral_meta(&csrf_response);

        let (status, created) = request_json(
            &app,
            "POST",
            "/api/v1/console/connections",
            &[
                ("Cookie", &cookie),
                ("Origin", "http://localhost"),
                ("X-Console-CSRF", &csrf),
            ],
            &http_connection_body("secured"),
        )
        .await;
        assert_eq!(status, 201, "{created}");
        let id = created["data"]["id"].as_str().unwrap().to_string();
        let mutations = vec![
            ("POST", "/api/v1/console/connections".to_string()),
            ("POST", "/api/v1/console/connections/test".to_string()),
            ("PATCH", format!("/api/v1/console/connections/{id}")),
            ("DELETE", format!("/api/v1/console/connections/{id}")),
            ("POST", format!("/api/v1/console/connections/{id}/test")),
            ("POST", format!("/api/v1/console/connections/{id}/connect")),
            (
                "POST",
                format!("/api/v1/console/connections/{id}/disconnect"),
            ),
            (
                "POST",
                format!("/api/v1/console/connections/{id}/sources/reconcile"),
            ),
            ("POST", "/api/v1/console/sources/reconcile".to_string()),
            ("POST", "/api/v1/console/auth/logout".to_string()),
        ];
        for (method, uri) in &mutations {
            let body = if matches!(*method, "POST" | "PATCH") && !uri.ends_with("reconcile") {
                http_connection_body("mutation")
            } else {
                String::new()
            };
            let (status, rejected) =
                request_json(&app, *method, uri, &[("Cookie", &cookie)], &body).await;
            assert_eq!(status, 403, "{method} {uri}: {rejected}");
            assert_eq!(rejected["error"]["code"], "ORIGIN_REJECTED");
            assert_neutral_meta(&rejected);

            let (status, rejected) = request_json(
                &app,
                *method,
                uri,
                &[("Cookie", &cookie), ("Origin", "http://localhost")],
                &body,
            )
            .await;
            assert_eq!(status, 403, "{method} {uri}: {rejected}");
            assert_eq!(rejected["error"]["code"], "CSRF_REJECTED");
            assert_neutral_meta(&rejected);

            let (status, rejected) = request_json(
                &app,
                *method,
                uri,
                &[
                    ("Cookie", &cookie),
                    ("Origin", "https://attacker.invalid"),
                    ("X-Console-CSRF", &csrf),
                ],
                &body,
            )
            .await;
            assert_eq!(status, 403, "{method} {uri}: {rejected}");
            assert_eq!(rejected["error"]["code"], "ORIGIN_REJECTED");
            assert_neutral_meta(&rejected);

            let (status, rejected) = request_json(
                &app,
                *method,
                uri,
                &[
                    ("Cookie", &cookie),
                    ("Origin", "http://localhost"),
                    ("X-Console-CSRF", "wrong"),
                ],
                &body,
            )
            .await;
            assert_eq!(status, 403, "{method} {uri}: {rejected}");
            assert_eq!(rejected["error"]["code"], "CSRF_REJECTED");
            assert_neutral_meta(&rejected);

            let (status, rejected) = request_json(
                &app,
                *method,
                uri,
                &[("Origin", "http://localhost"), ("X-Console-CSRF", &csrf)],
                &body,
            )
            .await;
            assert_eq!(status, 401, "{method} {uri}: {rejected}");
            assert_neutral_meta(&rejected);
        }

        let no_auth = http_manager_app(manager.clone(), None, None);
        for (method, uri) in &mutations {
            let body = if matches!(*method, "POST" | "PATCH") && !uri.ends_with("reconcile") {
                http_connection_body("mutation")
            } else {
                String::new()
            };
            for origin in [None, Some("https://attacker.invalid")] {
                let headers = origin
                    .map(|origin| vec![("Origin", origin)])
                    .unwrap_or_default();
                let (status, rejected) =
                    request_json(&no_auth, *method, uri, &headers, &body).await;
                assert_eq!(status, 403, "{method} {uri}: {rejected}");
                assert_eq!(rejected["error"]["code"], "ORIGIN_REJECTED");
                assert_neutral_meta(&rejected);
            }
        }
        manager.shutdown_all().await;
    }

    fn resource_suffixes() -> Vec<&'static str> {
        vec![
            "/summary",
            "/nodes",
            "/nodes/1",
            "/nodes/1/boots",
            "/groups",
            "/groups/7",
            "/groups/7/replicas",
            "/topology?view=nodes",
            "/topology?view=raft",
            "/topology?view=routes",
            "/metrics/performance",
            "/snapshots",
            "/lifecycle",
            "/lifecycle?view=restarts",
            "/lifecycle?view=versions",
            "/events",
            "/sources",
        ]
    }

    async fn first_sse_event(response: Response) -> (String, axum::body::BodyDataStream) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");
        let mut body = response.into_body().into_data_stream();
        let first = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        (String::from_utf8_lossy(&first).to_string(), body)
    }

    #[tokio::test]
    async fn scoped_resources_and_legacy_aliases_stay_on_their_fixed_runtimes() {
        let (_directory, manager) = http_manager();
        manager
            .create_startup(http_profile("a"), ConnectionSecrets::default())
            .await
            .unwrap();
        manager
            .create(http_profile("b"), ConnectionSecrets::default(), false)
            .await
            .unwrap();
        manager.connect("a").await.unwrap();
        manager.connect("b").await.unwrap();
        let app = http_manager_app(manager.clone(), Some("a"), None);

        for (id, cluster) in [("a", "a"), ("b", "b")] {
            for suffix in resource_suffixes() {
                let uri = format!("/api/v1/console/connections/{id}{suffix}");
                let (status, body) = request_json(&app, "GET", &uri, &[], "").await;
                assert_eq!(status, 200, "{uri}: {body}");
                assert_eq!(body["meta"]["cluster_id"], cluster, "{uri}: {body}");
                if suffix == "/summary" {
                    assert_eq!(body["data"]["cluster_id"], cluster, "{uri}: {body}");
                }
            }
            let (status, reconciled) = request_json(
                &app,
                "POST",
                &format!("/api/v1/console/connections/{id}/sources/reconcile"),
                &[("Origin", "http://localhost")],
                "",
            )
            .await;
            assert_eq!(status, 200, "{reconciled}");
            assert_eq!(reconciled["meta"]["cluster_id"], cluster);
        }

        for suffix in resource_suffixes() {
            let uri = format!("/api/v1/console{suffix}");
            let (status, body) = request_json(&app, "GET", &uri, &[], "").await;
            assert_eq!(status, 200, "{uri}: {body}");
            assert_eq!(body["meta"]["cluster_id"], "a", "{uri}: {body}");
        }
        let (status, reconciled) = request_json(
            &app,
            "POST",
            "/api/v1/console/sources/reconcile",
            &[("Origin", "http://localhost")],
            "",
        )
        .await;
        assert_eq!(status, 200, "{reconciled}");
        assert_eq!(reconciled["meta"]["cluster_id"], "a");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/console/connections/a/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (first, mut stream_a) = first_sse_event(response).await;
        assert!(first.contains("\"cluster_id\":\"a\""), "{first}");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/console/connections/b/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (first, mut stream_b) = first_sse_event(response).await;
        assert!(first.contains("\"cluster_id\":\"b\""), "{first}");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/console/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (first, mut legacy_api_stream) = first_sse_event(response).await;
        assert!(first.contains("\"cluster_id\":\"a\""), "{first}");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (first, mut legacy_stream) = first_sse_event(response).await;
        assert!(first.contains("\"cluster_id\":\"a\""), "{first}");

        manager.disconnect("a").await.unwrap();
        for stream in [&mut stream_a, &mut legacy_api_stream, &mut legacy_stream] {
            loop {
                let next = tokio::time::timeout(Duration::from_secs(1), stream.next())
                    .await
                    .expect("disconnect must end SSE");
                if next.is_none() {
                    break;
                }
            }
        }
        manager
            .resolve("b")
            .unwrap()
            .scheduler
            .cache
            .notify("summary");
        let update = tokio::time::timeout(Duration::from_secs(1), stream_b.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&update).contains("\"cluster_id\":\"b\""));

        for uri in [
            "/api/v1/console/connections/a/summary",
            "/api/v1/console/summary",
        ] {
            let (status, body) = request_json(&app, "GET", uri, &[], "").await;
            assert_eq!(status, 409, "{body}");
            assert_eq!(body["error"]["code"], "CONNECTION_NOT_CONNECTED");
            assert_neutral_meta(&body);
        }
        let (status, b) = request_json(
            &app,
            "GET",
            "/api/v1/console/connections/b/summary",
            &[],
            "",
        )
        .await;
        assert_eq!(status, 200, "{b}");
        assert_eq!(b["meta"]["cluster_id"], "b");

        manager.remove("a").await.unwrap();
        let (status, removed) = request_json(&app, "GET", "/api/v1/console/summary", &[], "").await;
        assert_eq!(status, 404, "{removed}");
        assert_eq!(removed["error"]["code"], "CONNECTION_NOT_FOUND");
        assert_neutral_meta(&removed);
        manager.shutdown_all().await;
        loop {
            let next = tokio::time::timeout(Duration::from_secs(1), stream_b.next())
                .await
                .expect("shutdown must end the remaining runtime SSE");
            if next.is_none() {
                break;
            }
        }
    }

    #[tokio::test]
    async fn legacy_routes_require_the_startup_connection_even_when_profiles_are_connected() {
        let (_directory, manager) = http_manager();
        manager
            .create(http_profile("b"), ConnectionSecrets::default(), false)
            .await
            .unwrap();
        manager.connect("b").await.unwrap();
        let app = http_manager_app(manager.clone(), None, None);
        for suffix in resource_suffixes() {
            let uri = format!("/api/v1/console{suffix}");
            let (status, body) = request_json(&app, "GET", &uri, &[], "").await;
            assert_eq!(status, 400, "{uri}: {body}");
            assert_eq!(body["error"]["code"], "CONNECTION_REQUIRED");
            assert_neutral_meta(&body);
        }
        for uri in ["/api/v1/console/stream", "/stream"] {
            let (status, body) = request_json(&app, "GET", uri, &[], "").await;
            assert_eq!(status, 400, "{uri}: {body}");
            assert_eq!(body["error"]["code"], "CONNECTION_REQUIRED");
            assert_neutral_meta(&body);
        }
        let (status, body) = request_json(
            &app,
            "POST",
            "/api/v1/console/sources/reconcile",
            &[("Origin", "http://localhost")],
            "",
        )
        .await;
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["error"]["code"], "CONNECTION_REQUIRED");
        let (status, missing) = request_json(
            &app,
            "GET",
            "/api/v1/console/connections/missing/summary",
            &[],
            "",
        )
        .await;
        assert_eq!(status, 404, "{missing}");
        assert_eq!(missing["error"]["code"], "CONNECTION_NOT_FOUND");
        assert_neutral_meta(&missing);
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn control_rejections_and_auth_disabled_endpoints_use_neutral_meta() {
        let (_directory, manager) = http_manager();
        let app = http_manager_app(manager, None, None);
        for (body, expected_status) in [("{\"name\":", 400), ("{\"name\":42}", 422)] {
            let (status, rejected) = request_json(
                &app,
                "POST",
                "/api/v1/console/connections",
                &[("Origin", "http://localhost")],
                body,
            )
            .await;
            assert_eq!(status, expected_status, "{rejected}");
            assert_eq!(rejected["error"]["code"], "INVALID_ARGUMENT");
            assert_neutral_meta(&rejected);
        }
        let oversized = format!("{{\"name\":\"{}\"}}", "x".repeat(2 * 1024 * 1024));
        let (status, rejected) = request_json(
            &app,
            "POST",
            "/api/v1/console/connections",
            &[("Origin", "http://localhost")],
            &oversized,
        )
        .await;
        assert_eq!(status, 413, "{rejected}");
        assert_eq!(rejected["error"]["code"], "PAYLOAD_TOO_LARGE");
        assert_neutral_meta(&rejected);

        let (status, csrf) = request_json(&app, "GET", "/api/v1/console/auth/csrf", &[], "").await;
        assert_eq!(status, 200);
        assert!(csrf["csrf_token"].is_null());
        assert_neutral_meta(&csrf);
        let (status, logout) = request_json(
            &app,
            "POST",
            "/api/v1/console/auth/logout",
            &[("Origin", "http://localhost")],
            "",
        )
        .await;
        assert_eq!(status, 200, "{logout}");
        assert_eq!(logout["data"]["revoked"], true);
        assert_neutral_meta(&logout);
    }

    #[tokio::test]
    async fn prometheus_history_and_sources_are_isolated_by_connection() {
        let (url_a, mut queries_a) = history_server(json!({
            "cluster_id": "a",
            "node_id": "1",
            "op": "write",
            "instance": "127.0.0.1:9091"
        }))
        .await;
        let (url_b, mut queries_b) = history_server(json!({
            "cluster_id": "b",
            "node_id": "1",
            "op": "write",
            "instance": "127.0.0.1:9092"
        }))
        .await;
        let (_directory, manager) = http_manager();
        for (id, url) in [("a", url_a.clone()), ("b", url_b.clone())] {
            let mut profile = http_profile(id);
            profile.prometheus = Some(PrometheusProfile {
                url,
                cluster: format!("cluster_id={id}"),
            });
            manager
                .create(profile, ConnectionSecrets::default(), false)
                .await
                .unwrap();
            manager.connect(id).await.unwrap();
        }
        let app = http_manager_app(manager.clone(), None, None);
        for (id, url) in [("a", url_a), ("b", url_b)] {
            let (status, performance) = request_json(
                &app,
                "GET",
                &format!("/api/v1/console/connections/{id}/metrics/performance?source=PROMETHEUS"),
                &[],
                "",
            )
            .await;
            assert_eq!(status, 200, "{performance}");
            assert_eq!(performance["meta"]["cluster_id"], id);
            assert_eq!(performance["data"]["retention_source"], "PROMETHEUS");
            let (status, sources) = request_json(
                &app,
                "GET",
                &format!("/api/v1/console/connections/{id}/sources"),
                &[],
                "",
            )
            .await;
            assert_eq!(status, 200, "{sources}");
            assert_eq!(sources["data"]["prometheus"]["url"], url);
        }
        let queries_a = queries_a.try_recv().expect("connection A history query");
        let queries_b = queries_b.try_recv().expect("connection B history query");
        assert!(queries_a["query"].contains("cluster_id=\"a\""));
        assert!(!queries_a["query"].contains("cluster_id=\"b\""));
        assert!(queries_b["query"].contains("cluster_id=\"b\""));
        assert!(!queries_b["query"].contains("cluster_id=\"a\""));
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn captured_reads_finish_after_disconnect_but_new_reads_cannot_resolve() {
        let upstream = Arc::new(SlowFullUpstream::new(
            Duration::from_millis(100),
            Duration::ZERO,
        ));
        let active_groups = upstream.active_groups.clone();
        let peers = [1, 2]
            .into_iter()
            .map(|node_id| SeedPeer {
                node_id,
                admin_addr: format!("127.0.0.1:5200{node_id}"),
            })
            .collect::<Vec<_>>();
        let cache = ConsoleCache::new("captured", Duration::from_secs(30), 32, 32);
        cache.register_peers(&peers);
        let scheduler = Arc::new(Scheduler::new(
            upstream,
            cache,
            peers,
            Duration::from_secs(5),
            Duration::from_secs(1),
            4,
        ));
        scheduler.reconcile().await;
        scheduler.reconcile().await;
        let state =
            app_state_for_scheduler(scheduler, PathBuf::from("definitely-absent-console-export"))
                .await;
        let manager = state.manager.clone();
        let app = router(state);
        let request = tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .uri("/api/v1/console/connections/legacy/groups/7/replicas")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while active_groups.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        manager.disconnect("legacy").await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .expect("captured read was cancelled")
            .unwrap();
        let (status, body) = json_response(response).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["meta"]["cluster_id"], "captured");
        assert!(!body["data"]["replicas"].as_array().unwrap().is_empty());

        let (status, rejected) = request_json(
            &http_manager_app(manager.clone(), None, None),
            "GET",
            "/api/v1/console/connections/legacy/summary",
            &[],
            "",
        )
        .await;
        assert_eq!(status, 409, "{rejected}");
        assert_eq!(rejected["error"]["code"], "CONNECTION_NOT_CONNECTED");
        assert_neutral_meta(&rejected);
        manager.shutdown_all().await;
    }

    #[tokio::test]
    async fn legacy_and_scoped_errors_preserve_runtime_or_neutral_metadata() {
        let (_directory, manager) = http_manager();
        manager
            .create_startup(http_profile("a"), ConnectionSecrets::default())
            .await
            .unwrap();
        manager.connect("a").await.unwrap();
        let app = http_manager_app(manager.clone(), Some("a"), None);

        let (status, invalid) =
            request_json(&app, "GET", "/api/v1/console/topology?view=bad", &[], "").await;
        assert_eq!(status, 400, "{invalid}");
        assert_eq!(invalid["error"]["code"], "INVALID_ARGUMENT");
        assert_eq!(invalid["meta"]["cluster_id"], "a");
        assert_eq!(invalid["meta"]["retention_source"], "CONSOLE_MEMORY");

        for uri in ["/api/v1/console/nodes/999", "/api/v1/console/groups/999"] {
            let (status, missing) = request_json(&app, "GET", uri, &[], "").await;
            assert_eq!(status, 404, "{missing}");
            assert_eq!(missing["error"]["code"], "NOT_FOUND");
            assert_eq!(missing["meta"]["cluster_id"], "a");
            assert_eq!(missing["meta"]["retention_source"], "CONSOLE_MEMORY");
        }

        for suffix in [
            "/nodes/1",
            "/nodes/1/boots",
            "/groups/7",
            "/groups/7/replicas",
        ] {
            let (status, unknown) = request_json(
                &app,
                "GET",
                &format!("/api/v1/console/connections/missing{suffix}"),
                &[],
                "",
            )
            .await;
            assert_eq!(status, 404, "{unknown}");
            assert_eq!(unknown["error"]["code"], "CONNECTION_NOT_FOUND");
            assert_neutral_meta(&unknown);
        }

        manager.remove("a").await.unwrap();
        for suffix in [
            "/nodes/1",
            "/nodes/1/boots",
            "/groups/7",
            "/groups/7/replicas",
        ] {
            let (status, deleted) =
                request_json(&app, "GET", &format!("/api/v1/console{suffix}"), &[], "").await;
            assert_eq!(status, 404, "{deleted}");
            assert_eq!(deleted["error"]["code"], "CONNECTION_NOT_FOUND");
            assert_neutral_meta(&deleted);
        }
        manager.shutdown_all().await;
    }
}
