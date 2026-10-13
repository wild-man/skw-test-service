//! `ws`-gate-only behavior: any method can be sent to any topic uri; unsigned requests carry
//! client-chosen request ids.

use reqwest::StatusCode;
use serde_json::json;
use skw_lib_http_protos::ping::{RpcPayload, open, signed};

use crate::gate::{Auth, Gate, WsGate};
use crate::keys::main_key;

/// Unsigned request ids are picked by clients, so two connections may use the same one at the
/// same time; the gate must still deliver each reply to the connection that asked.
#[tokio::test]
async fn same_request_id_on_two_connections_gets_separate_replies() {
    const REQUEST_ID: &str = "same-request-id";
    let mut a = WsGate::client(Auth::None).await;
    let mut b = WsGate::client(Auth::None).await;
    let public = json!({ "Method": "Public", "Params": {} });

    let (reply_a, reply_b) = tokio::join!(
        a.request_with_id("/ping/open", &open::RpcMethod::PingOpen {}, REQUEST_ID),
        b.request_with_id("/ping/public", &public, REQUEST_ID),
    );

    assert!(matches!(reply_a.payload(), RpcPayload::Ping { .. }));
    assert!(matches!(reply_b.payload(), RpcPayload::Public {}));
}

/// the public consumer only knows the `Public` method
#[tokio::test]
async fn signed_ping_via_public_topic_is_bad_request() {
    WsGate::client(Auth::Key(main_key().await))
        .await
        .request("/ping/public", &signed::RpcMethod::PingSigned {})
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}
