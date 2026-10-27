//! `api-gate`-only behavior: HTTP method rules and per-request auth headers on non-signed topics.

use reqwest::StatusCode;
use skw_lib_http_protos::ping::{RpcPayload, signed};

use skw_lib_shared::prelude::chrono::{TimeDelta, Utc};

use crate::common::{gate_body, gate_body_at};
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

// --- provisional replies: an HTTP request gets one reply, either the provisional 201/202
// (request taken, result read later) or the final 200; both are valid ---

/// a 2xx reply to `method`: `provisional` with `expected` payload, or a final 200 with any payload
async fn signed_ping_replies_provisional_or_ok(method: signed::RpcMethod, provisional: StatusCode, expected: fn(&RpcPayload) -> bool) {
    let reply = ApiGate::client(Auth::Key(main_key().await))
        .await
        .signed("ping", &method)
        .await;
    match reply.status {
        StatusCode::OK => {
            reply.payload::<RpcPayload>();
        }
        s if s == provisional => {
            let payload = reply.payload::<RpcPayload>();
            assert!(
                expected(&payload),
                "unexpected {provisional} payload: {payload:?}"
            );
        }
        _ => panic!("expected 200 or {provisional}, got {reply:?}"),
    }
}

#[tokio::test]
async fn ping_created_replies_created_or_ok() {
    signed_ping_replies_provisional_or_ok(
        signed::RpcMethod::PingCreated {},
        StatusCode::CREATED,
        |p| matches!(p, RpcPayload::PingCreated {}),
    )
    .await;
}

#[tokio::test]
async fn ping_accepted_replies_accepted_or_ok() {
    signed_ping_replies_provisional_or_ok(
        signed::RpcMethod::PingAccepted {},
        StatusCode::ACCEPTED,
        |p| matches!(p, RpcPayload::PingAccepted {}),
    )
    .await;
}

/// a validly signed body whose `Ts` is long past (a captured request replayed later) is refused
#[tokio::test]
async fn outdated_signed_request_is_conflict() {
    let gate = ApiGate::client(Auth::Key(main_key().await)).await;
    let body = gate_body_at(
        &signed::RpcMethod::PingSigned {},
        Utc::now() - TimeDelta::minutes(5),
    );
    let headers = gate.headers(&body);

    gate.post("/ping/signed", body, &headers)
        .await
        .assert_status(StatusCode::CONFLICT);
}
