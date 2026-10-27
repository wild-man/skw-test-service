//! `ws`-gate-only behavior: any method can be sent to any topic uri; unsigned requests carry
//! client-chosen request ids; many requests can be in flight on one connection.

use std::collections::HashSet;
use std::time::Duration;

use reqwest::StatusCode;
use serde_json::json;
use skw_lib_http_protos::ping::{RpcPayload, open, signed};
use skw_lib_shared::prelude::chrono::{TimeDelta, Utc};

use crate::common::gate_body_at;

use crate::gate::{Auth, Gate, WsGate};
use crate::keys::main_key;

const PIPELINED_REQUESTS: usize = 100;
/// the gate's own 30s request watchdog plus a margin: its 504 must arrive before we give up
const FINAL_REPLY_TIMEOUT: Duration = Duration::from_secs(35);

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

// --- provisional replies: a 201/202 frame is not the end of a ws request, the test waits
// (logging every provisional reply) for the final 200 and fails on timeout ---

async fn signed_ping_replies_ok_after_provisional(method: signed::RpcMethod) {
    let mut ws = WsGate::client(Auth::Key(main_key().await)).await;
    let id = ws.send_request("/ping/signed", &method).await;
    ws.wait_final_reply(&id, FINAL_REPLY_TIMEOUT)
        .await
        .assert_status(StatusCode::OK);
}

#[tokio::test]
async fn ping_created_replies_ok_after_created() {
    signed_ping_replies_ok_after_provisional(signed::RpcMethod::PingCreated {}).await;
}

#[tokio::test]
async fn ping_accepted_replies_ok_after_accepted() {
    signed_ping_replies_ok_after_provisional(signed::RpcMethod::PingAccepted {}).await;
}

/// a signed request is keyed by its signature: the identical frame sent again while the first is
/// still pending is refused (409) instead of overwriting it and running twice
/// (`PingSlow` keeps the first one pending: a plain `PingSigned` is often answered before the
/// gate reads the second frame)
#[tokio::test]
async fn duplicate_signed_request_while_pending_is_conflict() {
    let mut ws = WsGate::client(Auth::Key(main_key().await)).await;
    let id = ws
        .send_request_twice("/ping/signed", &signed::RpcMethod::PingSlow {})
        .await;

    // both replies carry the same signature, in no fixed order
    let mut replies = vec![ws.wait_reply(&id).await, ws.wait_reply(&id).await];
    replies.sort_by_key(|r| r.status);

    replies[1].assert_status(StatusCode::CONFLICT);
    assert!(matches!(
        replies.swap_remove(0).payload(),
        RpcPayload::Ping { .. }
    ));
}

/// a validly signed frame whose `Ts` is long past (a captured frame replayed later) is refused,
/// and the connection stays usable
#[tokio::test]
async fn outdated_signed_request_is_conflict() {
    let mut ws = WsGate::client(Auth::Key(main_key().await)).await;
    let data = gate_body_at(
        &signed::RpcMethod::PingSigned {},
        Utc::now() - TimeDelta::minutes(5),
    );

    ws.request_signed_body("/ping/signed", &data)
        .await
        .assert_status(StatusCode::CONFLICT);

    let reply = ws.signed("ping", &signed::RpcMethod::PingSigned {}).await;
    assert!(matches!(reply.payload(), RpcPayload::Ping { .. }));
}
