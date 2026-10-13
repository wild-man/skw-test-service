//! Ping service scenarios, run on every gate provider.

use reqwest::StatusCode;
use skw_lib_http_protos::ping::{RpcPayload, open, signed};

use crate::gate::{Auth, Gate, for_all_gates};
use crate::keys::{main_key, other_key};

for_all_gates! {
    public_returns_public_payload,
    open_ping_returns_ping_payload,
    signed_ping_returns_ping_payload,
    public_without_credentials_works,
    open_ping_without_credentials_works,
    signed_ping_without_credentials_is_forbidden,
    signed_ping_signed_by_another_key_is_forbidden,
    signed_ping_with_tampered_body_is_forbidden,
    signed_ping_with_unknown_key_is_forbidden,
    signed_ping_via_open_topic_is_bad_request,
    unknown_service_is_bad_request,
}

async fn client<G: Gate>() -> G {
    G::client(Auth::Key(main_key().await)).await
}

// --- every topic type works ---

async fn public_returns_public_payload<G: Gate>() {
    let reply = client::<G>().await.public("ping").await;
    assert!(matches!(reply.payload(), RpcPayload::Public {}));
}

async fn open_ping_returns_ping_payload<G: Gate>() {
    let reply = client::<G>()
        .await
        .open("ping", &open::RpcMethod::PingOpen {})
        .await;
    let RpcPayload::Ping { count, .. } = reply.payload() else {
        panic!("expected Ping payload");
    };
    assert!(count > 0);
}

async fn signed_ping_returns_ping_payload<G: Gate>() {
    let reply = client::<G>()
        .await
        .signed("ping", &signed::RpcMethod::PingSigned {})
        .await;
    let RpcPayload::Ping { count, .. } = reply.payload() else {
        panic!("expected Ping payload");
    };
    assert!(count > 0);
}

// --- public/open topics need no credentials ---

async fn public_without_credentials_works<G: Gate>() {
    let reply = G::client(Auth::None).await.public("ping").await;
    assert!(matches!(reply.payload(), RpcPayload::Public {}));
}

async fn open_ping_without_credentials_works<G: Gate>() {
    let reply = G::client(Auth::None)
        .await
        .open("ping", &open::RpcMethod::PingOpen {})
        .await;
    assert!(matches!(reply.payload(), RpcPayload::Ping { .. }));
}

// --- PingSigned is accepted only on the signed topic with valid credentials ---

async fn signed_ping_with<G: Gate>(auth: Auth) -> crate::gate::GateReply {
    G::client(auth)
        .await
        .signed("ping", &signed::RpcMethod::PingSigned {})
        .await
}

async fn signed_ping_without_credentials_is_forbidden<G: Gate>() {
    signed_ping_with::<G>(Auth::None)
        .await
        .assert_status(StatusCode::FORBIDDEN);
}

async fn signed_ping_signed_by_another_key_is_forbidden<G: Gate>() {
    let auth = Auth::SignedByOther {
        key: main_key().await,
        signer: other_key().await,
    };
    signed_ping_with::<G>(auth)
        .await
        .assert_status(StatusCode::FORBIDDEN);
}

async fn signed_ping_with_tampered_body_is_forbidden<G: Gate>() {
    signed_ping_with::<G>(Auth::TamperedBody(main_key().await))
        .await
        .assert_status(StatusCode::FORBIDDEN);
}

async fn signed_ping_with_unknown_key_is_forbidden<G: Gate>() {
    signed_ping_with::<G>(Auth::unknown_key(main_key().await))
        .await
        .assert_status(StatusCode::FORBIDDEN);
}

async fn signed_ping_via_open_topic_is_bad_request<G: Gate>() {
    client::<G>()
        .await
        .open("ping", &signed::RpcMethod::PingSigned {})
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}

// --- routing ---

async fn unknown_service_is_bad_request<G: Gate>() {
    client::<G>()
        .await
        .open("nope", &open::RpcMethod::PingOpen {})
        .await
        .assert_status(StatusCode::BAD_REQUEST);
}
