//! `ws`-gate-only behavior: any method can be sent to any topic uri; unsigned requests carry
//! client-chosen request ids; many requests can be in flight on one connection.

use std::assert_matches;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use serde_json::json;
use skw_lib_http_protos::ping::{RpcPayload, open, signed};
use skw_lib_shared::prelude::chrono::{TimeDelta, Utc};
use skw_lib_shared::prelude::uuid::Uuid;
use tokio::sync::Barrier;
use tokio::time::Instant;

use crate::common::{gate_body, gate_body_at, ws_gate_heartbeat, ws_gate_max_pending_requests};

use crate::gate::ws::{CLOSE_POLICY, ws_frame};
use crate::gate::{Auth, Gate, RawEnd, RawWs, WsGate};
use crate::keys::{ApiKey, main_key, other_key};

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

const CONCURRENT_SIGNED_REQUESTS: usize = 1000;

/// One client of [`concurrent_signed_clients_get_only_their_own_replies`]: waits for `start`,
/// then sends its signed pings as fast as replies allow, keeping up to 5/6 of the gate's
/// `maxPendingRequests` waiting (more would get the connection closed), and reads every reply
/// in arrival order. Returns the signatures sent and received.
async fn signed_client(key: &'static ApiKey, start: Arc<Barrier>) -> (HashSet<String>, Vec<String>) {
    let max_waiting = ws_gate_max_pending_requests() * 5 / 6;
    let mut ws = WsGate::client(Auth::Key(key)).await;
    start.wait().await;

    let mut sent = HashSet::with_capacity(CONCURRENT_SIGNED_REQUESTS);
    let mut received = Vec::with_capacity(CONCURRENT_SIGNED_REQUESTS);
    while received.len() < CONCURRENT_SIGNED_REQUESTS {
        while sent.len() < CONCURRENT_SIGNED_REQUESTS && sent.len() - received.len() < max_waiting {
            let signature = ws
                .send_request("/ping/signed", &signed::RpcMethod::PingSigned {})
                .await;
            assert!(
                sent.insert(signature),
                "duplicate signature: request ids must be unique"
            );
        }

        let (signature, reply) = ws.next_reply().await;
        assert!(
            matches!(reply.payload(), RpcPayload::Ping { .. }),
            "request {signature}: not a Ping reply"
        );
        received.push(signature);
    }
    (sent, received)
}

/// A connection with `maxPendingRequests` requests waiting for their reply is closed (1008) by
/// the next frame. `PingSlow` keeps each request waiting for 1s, longer than sending them all.
#[tokio::test]
async fn too_many_pending_requests_close_the_connection() {
    let max_pending = ws_gate_max_pending_requests();
    let mut ws = WsGate::client(Auth::Key(main_key().await)).await;

    let mut last = String::new();
    for _ in 0..=max_pending {
        last = ws
            .send_request("/ping/signed", &signed::RpcMethod::PingSlow {})
            .await;
    }

    // the Close arrives before any PingSlow reply; it ends every request of the connection
    let reply = ws.wait_reply(&last).await;
    reply.assert_status(StatusCode::FORBIDDEN);
    assert_eq!(
        reply.error_msg.as_deref(),
        Some("Too many pending requests")
    );
}

/// Two connections (different keys) in separate tasks of a multi-threaded runtime send signed
/// requests at the same time: each must get a reply to every signature it sent, and none it
/// didn't send (both gates share one back topic, replies are routed by signature).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_signed_clients_get_only_their_own_replies() {
    let start = Arc::new(Barrier::new(2));
    let a = tokio::spawn(signed_client(main_key().await, start.clone()));
    let b = tokio::spawn(signed_client(other_key().await, start));
    let (a, b) = (a.await.expect("client a"), b.await.expect("client b"));

    for (name, (sent, received)) in [("a", &a), ("b", &b)] {
        let received_set: HashSet<&String> = received.iter().collect();
        assert_eq!(
            received_set.len(),
            received.len(),
            "client {name}: a signature was answered twice"
        );

        let foreign: Vec<_> = received_set
            .iter()
            .filter(|s| !sent.contains(s.as_str()))
            .collect();
        assert!(
            foreign.is_empty(),
            "client {name} got replies it never sent: {foreign:?}"
        );

        let missing: Vec<_> = sent.iter().filter(|s| !received_set.contains(s)).collect();
        assert!(
            missing.is_empty(),
            "client {name} got no reply for: {missing:?}"
        );
    }

    // different keys sign different bodies: the two clients' signatures never collide
    assert!(
        a.0.is_disjoint(&b.0),
        "both clients sent the same signature"
    );
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

// --- connection lifetime: `RawWs` never answers pings nor echoes Close, so only the gate can
// close the socket ---

/// a client that answers no pings is dropped once `clientTimeoutSecs` passed without a pong
/// (checked on each heartbeat tick, hence the extra interval)
#[tokio::test]
async fn silent_client_is_dropped_after_client_timeout() {
    let (interval, client_timeout) = ws_gate_heartbeat();
    let mut raw = RawWs::connect().await;
    let started = Instant::now();
    let deadline = started + client_timeout + interval + Duration::from_secs(2);

    let end = loop {
        match raw.next_frame(deadline).await {
            Ok(_ping) => continue,
            Err(end) => break end,
        }
    };

    assert_eq!(
        end,
        RawEnd::Closed,
        "gate kept a silent client past {deadline:?}"
    );
    assert!(
        started.elapsed() >= client_timeout,
        "gate dropped a silent client after {:?}, before clientTimeoutSecs ({client_timeout:?})",
        started.elapsed()
    );
}

/// after refusing a frame with Close 1008 the gate must close the socket itself, even if the
/// client neither echoes the Close nor goes silent (it keeps sending frames, refreshing the
/// gate's heartbeat)
#[tokio::test]
async fn rejected_client_ignoring_close_is_disconnected() {
    const CLOSE_GRACE: Duration = Duration::from_secs(3);
    let public = json!({ "Method": "Public", "Params": {} });

    let mut raw = RawWs::connect().await;
    raw.send_text("not a request".into())
        .await
        .expect("ws send failed");
    let close = raw
        .next_frame(Instant::now() + Duration::from_secs(10))
        .await
        .expect("gate did not answer the invalid frame");
    assert_eq!(
        close.close_code(),
        Some(CLOSE_POLICY),
        "expected Close 1008, got {close:?}"
    );

    let deadline = Instant::now() + CLOSE_GRACE;
    let mut keep_alive = tokio::time::interval(Duration::from_secs(1));
    let mut answered_after_close = 0;
    let end = loop {
        tokio::select! {
            _ = keep_alive.tick() => {
                let frame = ws_frame(Some("/ping/public"), &gate_body(&public), &Uuid::now_v7().to_string());
                if raw.send_text(frame).await.is_err() {
                    break RawEnd::Closed;
                }
            }
            frame = raw.next_frame(deadline) => match frame {
                Ok(f) if f.is_text() => answered_after_close += 1,
                Ok(_) => {}
                Err(end) => break end,
            },
        }
    };

    assert_matches!(
        end,
        RawEnd::Closed | RawEnd::Timeout,
        "gate kept the connection open {CLOSE_GRACE:?} after its own Close 1008 \
         (and answered {answered_after_close} requests sent after it)"
    );
}
