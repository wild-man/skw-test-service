//! `api-gate`-only behavior: HTTP method rules and per-request auth headers on non-signed topics.

use reqwest::StatusCode;
use skw_lib_http_protos::ping::signed;

use crate::common::gate_body;
use crate::gate::{ApiGate, Auth, Gate};
use crate::keys::main_key;

/// valid signature headers don't turn an open-topic request into a signed one
#[tokio::test]
async fn signed_ping_via_open_topic_with_valid_headers_is_bad_request() {
    let gate = ApiGate::client(Auth::Key(main_key().await)).await;
    let body = gate_body(&signed::RpcMethod::PingSigned {});
    let headers = gate.headers(&body);

    gate.post("/ping/open", body, &headers)
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}

/// public topic accepts GET only
#[tokio::test]
async fn post_to_public_topic_is_bad_request() {
    let gate = ApiGate::client(Auth::Key(main_key().await)).await;
    let body = gate_body(&signed::RpcMethod::PingSigned {});
    let headers = gate.headers(&body);

    gate.post("/ping/public", body, &headers)
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}
