//! `ws`-gate-only behavior: any method can be sent to any topic uri; unsigned requests carry
//! client-chosen request ids; many requests can be in flight on one connection.

use std::collections::HashSet;

use reqwest::StatusCode;
use serde_json::json;
use skw_lib_http_protos::ping::{RpcPayload, open, signed};

use crate::gate::{Auth, Gate, WsGate};
use crate::keys::main_key;

const PIPELINED_REQUESTS: usize = 100;

#[derive(Clone, Copy, Debug)]
enum PingRequest {
    Public,
    Open,
    Signed,
}

/// Sends every request of `plan` back-to-back without reading any reply, then collects all
/// replies (they may arrive in any order). Each request must get exactly its own reply.
async fn pipeline(ws: &mut WsGate, plan: &[PingRequest]) {
    let public = json!({ "Method": "Public", "Params": {} });

    let mut sent = Vec::with_capacity(plan.len());
    for req in plan {
        let id = match req {
            PingRequest::Public => ws.send_request("/ping/public", &public).await,
            PingRequest::Open => {
                ws.send_request("/ping/open", &open::RpcMethod::PingOpen {})
                    .await
            }
            PingRequest::Signed => {
                ws.send_request("/ping/signed", &signed::RpcMethod::PingSigned {})
                    .await
            }
        };
        sent.push((*req, id));
    }

    let ids: HashSet<&String> = sent.iter().map(|(_, id)| id).collect();
    assert_eq!(
        ids.len(),
        sent.len(),
        "request ids must be unique to correlate replies"
    );

    let mut ping_counts = HashSet::new();
    let mut pings = 0;
    for (req, id) in &sent {
        match (req, ws.wait_reply(id).await.payload::<RpcPayload>()) {
            (PingRequest::Public, RpcPayload::Public {}) => {}
            (PingRequest::Open | PingRequest::Signed, RpcPayload::Ping { count, .. }) => {
                pings += 1;
                ping_counts.insert(count);
            }
            (req, payload) => panic!("{req:?} request {id} got a reply for another request: {payload:?}"),
        }
    }
    // every Ping reply comes from its own processed request (the service INCRs a redis counter)
    assert_eq!(ping_counts.len(), pings, "duplicate Ping replies");
}

#[tokio::test]
async fn pipelined_requests_on_authorized_connection_all_get_their_replies() {
    let plan: Vec<_> = [PingRequest::Public, PingRequest::Open, PingRequest::Signed]
        .into_iter()
        .cycle()
        .take(PIPELINED_REQUESTS)
        .collect();
    pipeline(
        &mut WsGate::client(Auth::Key(main_key().await)).await,
        &plan,
    )
    .await;
}

/// unsigned requests are correlated by gate-minted ids, the client's ids are only echoed back
#[tokio::test]
async fn pipelined_requests_on_anonymous_connection_all_get_their_replies() {
    let plan: Vec<_> = [PingRequest::Public, PingRequest::Open]
        .into_iter()
        .cycle()
        .take(PIPELINED_REQUESTS)
        .collect();
    pipeline(&mut WsGate::client(Auth::None).await, &plan).await;
}

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
