use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ConsoleProcess {
    child: Child,
    addr: SocketAddr,
}

impl ConsoleProcess {
    fn spawn(extra_env: &[(&str, &str)]) -> Self {
        let addr = unused_addr();
        let mut command = Command::new(env!("CARGO_BIN_EXE_rr-admin"));
        command
            .args([
                "console",
                "--listen",
                &addr.to_string(),
                "--peers",
                "1@127.0.0.1:9",
            ])
            .env_remove("RR_CONSOLE_TOKEN")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        let child = command.spawn().expect("spawn rr-admin console");
        let mut process = Self { child, addr };
        process.wait_until_ready();
        process
    }

    fn wait_until_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().expect("inspect console process") {
                panic!("console exited before accepting requests: {status}");
            }
            if TcpStream::connect(self.addr).is_ok() {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("console did not listen on {} within 10s", self.addr);
    }

    fn request(&self, method: &str, path: &str, headers: &[(&str, &str)], body: &str) -> Response {
        let mut stream = TcpStream::connect(self.addr).expect("connect to console");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set read timeout");
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            self.addr,
            body.len()
        );
        for (name, value) in headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(body);
        stream.write_all(request.as_bytes()).expect("write request");
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).expect("read response");
        Response::parse(&bytes)
    }
}

impl Drop for ConsoleProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug)]
struct Response {
    status: u16,
    headers: HashMap<String, String>,
    body: String,
}

impl Response {
    fn parse(bytes: &[u8]) -> Self {
        let text = String::from_utf8(bytes.to_vec()).expect("UTF-8 HTTP response");
        let (head, body) = text.split_once("\r\n\r\n").expect("HTTP header terminator");
        let mut lines = head.lines();
        let status = lines
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|status| status.parse().ok())
            .expect("HTTP status");
        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
            .collect();
        Self {
            status,
            headers,
            body: body.to_string(),
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or_else(|error| {
            panic!(
                "invalid JSON response (status {}): {error}: {}",
                self.status, self.body
            )
        })
    }
}

fn unused_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read ephemeral address")
}

fn console_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rr-admin"));
    command.args(["console", "--peers", "1@127.0.0.1:9"]);
    command.env_remove("RR_CONSOLE_TOKEN");
    command
}

#[test]
fn non_loopback_requires_tls_and_viewer_authentication() {
    let output = console_command()
        .args(["--listen", "0.0.0.0:18080"])
        .output()
        .expect("run insecure public console");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("non-loopback"),
        "unexpected stderr: {stderr}"
    );
    assert!(stderr.contains("TLS"), "unexpected stderr: {stderr}");
    assert!(stderr.contains("token"), "unexpected stderr: {stderr}");
}

#[test]
fn allow_insecure_is_rejected_for_non_loopback_even_with_other_security_flags() {
    let output = console_command()
        .args([
            "--listen",
            "0.0.0.0:18080",
            "--allow-insecure",
            "--console-token-env",
            "RR_CONSOLE_TOKEN",
            "--tls-cert",
            "unused.pem",
            "--tls-key",
            "unused.key",
        ])
        .env("RR_CONSOLE_TOKEN", "viewer-secret")
        .output()
        .expect("run explicitly insecure public console");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--allow-insecure") && stderr.contains("loopback"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn api_404_is_json_while_spa_route_uses_debug_fallback_without_export() {
    let console = ConsoleProcess::spawn(&[]);
    let missing = console.request("GET", "/api/v1/console/not-a-route", &[], "");
    assert_eq!(missing.status, 404);
    let body = missing.json();
    assert_eq!(body["error"]["code"], "NOT_FOUND");
    assert_eq!(body["meta"]["schema_version"], "console.v1");
    assert!(body["meta"]["correlation_id"].as_str().is_some());

    let spa = console.request("GET", "/nodes/42", &[], "");
    assert_eq!(spa.status, 200);
    assert!(spa
        .headers
        .get("content-type")
        .is_some_and(|value| value.starts_with("text/html")));
    assert!(spa.body.to_ascii_lowercase().contains("<html"));
}

#[test]
fn auth_disabled_csrf_is_nullable_and_reconcile_needs_no_csrf() {
    let console = ConsoleProcess::spawn(&[]);
    let csrf = console.request("GET", "/api/v1/console/auth/csrf", &[], "");
    assert_eq!(csrf.status, 200, "{}", csrf.body);
    assert!(csrf.headers.get("set-cookie").is_none());
    let body = csrf.json();
    assert!(body["csrf_token"].is_null());
    assert!(body["expires_at"].is_null());
    assert_eq!(body["meta"]["schema_version"], "console.v1");

    let reconciled = console.request(
        "POST",
        "/api/v1/console/sources/reconcile",
        &[("Origin", &format!("http://{}", console.addr))],
        "",
    );
    assert_eq!(reconciled.status, 200, "{}", reconciled.body);
}

#[test]
fn viewer_login_issues_http_only_session_and_reconcile_requires_csrf_and_origin() {
    let console = ConsoleProcess::spawn(&[("RR_CONSOLE_TOKEN", "viewer-secret")]);
    let unauthenticated = console.request("GET", "/api/v1/console/summary", &[], "");
    assert_eq!(unauthenticated.status, 401);
    assert_eq!(unauthenticated.json()["error"]["code"], "UNAUTHENTICATED");

    let unauthenticated_csrf = console.request("GET", "/api/v1/console/auth/csrf", &[], "");
    assert_eq!(unauthenticated_csrf.status, 401);

    let rejected = console.request(
        "POST",
        "/api/v1/console/auth/login",
        &[("Content-Type", "application/json")],
        r#"{"token":"wrong"}"#,
    );
    assert_eq!(rejected.status, 401);

    let login = console.request(
        "POST",
        "/api/v1/console/auth/login",
        &[("Content-Type", "application/json")],
        r#"{"token":"viewer-secret"}"#,
    );
    assert_eq!(login.status, 200, "{}", login.body);
    let cookie = login
        .headers
        .get("set-cookie")
        .expect("session cookie")
        .split(';')
        .next()
        .expect("cookie pair")
        .to_string();
    let set_cookie = login
        .headers
        .get("set-cookie")
        .unwrap()
        .to_ascii_lowercase();
    assert!(set_cookie.contains("httponly"));
    assert!(set_cookie.contains("samesite=strict"));
    let login_payload = login.json();
    let login_csrf = login_payload["csrf_token"].as_str().expect("CSRF token");
    let login_expires_at = login_payload["expires_at"]
        .as_str()
        .expect("session expiry");

    let csrf_response = console.request(
        "GET",
        "/api/v1/console/auth/csrf",
        &[("Cookie", &cookie)],
        "",
    );
    assert_eq!(csrf_response.status, 200, "{}", csrf_response.body);
    assert!(csrf_response.headers.get("set-cookie").is_none());
    let csrf_payload = csrf_response.json();
    assert_eq!(csrf_payload["csrf_token"], login_csrf);
    assert_eq!(csrf_payload["expires_at"], login_expires_at);
    assert_eq!(csrf_payload["meta"]["schema_version"], "console.v1");
    let csrf = csrf_payload["csrf_token"]
        .as_str()
        .expect("current-session CSRF token")
        .to_string();

    let session_cookie = [("Cookie", cookie.as_str())];
    let summary = console.request("GET", "/api/v1/console/summary", &session_cookie, "");
    assert_eq!(summary.status, 200, "{}", summary.body);
    assert_eq!(summary.json()["meta"]["schema_version"], "console.v1");

    let no_csrf = console.request(
        "POST",
        "/api/v1/console/sources/reconcile",
        &session_cookie,
        "",
    );
    assert_eq!(no_csrf.status, 403);

    let bad_origin = console.request(
        "POST",
        "/api/v1/console/sources/reconcile",
        &[
            ("Cookie", cookie.as_str()),
            ("X-Console-CSRF", csrf.as_str()),
            ("Origin", "https://attacker.invalid"),
        ],
        "",
    );
    assert_eq!(bad_origin.status, 403);

    let reconciled = console.request(
        "POST",
        "/api/v1/console/sources/reconcile",
        &[
            ("Cookie", cookie.as_str()),
            ("X-Console-CSRF", csrf.as_str()),
            ("Origin", &format!("http://{}", console.addr)),
        ],
        "",
    );
    assert_eq!(reconciled.status, 200, "{}", reconciled.body);
    assert!(reconciled.json()["meta"]["correlation_id"]
        .as_str()
        .is_some());
}
