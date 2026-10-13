//! Connection helpers for tests: gate URLs (ports from `cfg/services.json` configs), clients, request bodies.

use std::sync::LazyLock;
use std::time::Duration;

use serde::Serialize;
use skw_lib_shared::prelude::chrono::Utc;
use skw_lib_shared::prelude::jsonrpc::JsonRpcGateHttpRequest;
use skw_test_service::{find_service, listen_port};

static API_GATE_PORT: LazyLock<u16> = LazyLock::new(|| gate_port("api-gate"));
static WS_GATE_PORT: LazyLock<u16> = LazyLock::new(|| gate_port("ws"));

fn gate_port(bin: &str) -> u16 {
    find_service(bin)
        .and_then(|s| listen_port(&s))
        .expect("invalid cfg/services.json")
        .unwrap_or_else(|| panic!("{bin} is not marked \"listens\" in cfg/services.json"))
}

pub fn api_gate_url(path: &str) -> String {
    format!("https://localhost:{}{path}", *API_GATE_PORT)
}

pub fn ws_gate_url(path: &str) -> String {
    format!("wss://localhost:{}{path}", *WS_GATE_PORT)
}

/// New client per test: a `reqwest::Client` is bound to the runtime it first ran on,
/// and every `#[tokio::test]` has its own runtime.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        // gates use a pre-provisioned self-signed cert
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client")
}

/// Gate request body: `{"Ts": <now>, "Method": ..., "Params": {...}}`.
pub fn gate_body<M: Serialize>(method: &M) -> String {
    let req = JsonRpcGateHttpRequest {
        ts: Utc::now(),
        method: serde_json::to_value(method).expect("serialize rpc method"),
    };
    serde_json::to_string(&req).expect("serialize gate request")
}

/// Same JSON, different bytes: breaks a signature made over `body`.
/// The change is inside the object, so it also survives being embedded as a ws frame's `Data`.
pub fn tamper(body: &str) -> String {
    body.replacen('{', "{ ", 1)
}
