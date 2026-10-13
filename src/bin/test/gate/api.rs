//! `api-gate` provider: one HTTP request per call, `/{service}/{topic_type}` routing:
//! - public: `GET  /{service}/public` — no body, the gate always sends the `Public` method
//! - open:   `POST /{service}/open`   — JSON-RPC body, no auth
//! - signed: `POST /{service}/signed` — JSON-RPC body + `API-KEY` / `PAYLOAD-SIGNATURE` headers

use reqwest::{RequestBuilder, Response};
use serde::Serialize;
use serde_json::Value;
use skw_lib_shared::prelude::consts::{HTTP_HEADER_API_KEY, HTTP_HEADER_PAYLOAD_SIGNATURE};

use super::{Auth, Gate, GateReply};
use crate::common::{api_gate_url, gate_body, http_client};

pub struct ApiGate {
    auth: Auth,
}

impl Gate for ApiGate {
    async fn client(auth: Auth) -> Self {
        ApiGate { auth }
    }

    async fn public(&mut self, service: &str) -> GateReply {
        send(http_client().get(api_gate_url(&format!("/{service}/public")))).await
    }

    async fn open<M: Serialize>(&mut self, service: &str, method: &M) -> GateReply {
        self.post(&format!("/{service}/open"), gate_body(method), &[])
            .await
    }

    async fn signed<M: Serialize>(&mut self, service: &str, method: &M) -> GateReply {
        let body = gate_body(method);
        let headers = self.headers(&body);
        self.post(
            &format!("/{service}/signed"),
            self.auth.wire_body(&body),
            &headers,
        )
        .await
    }
}

impl ApiGate {
    /// `API-KEY` / `PAYLOAD-SIGNATURE` headers of this client's credentials for `body`
    pub fn headers(&self, body: &str) -> Vec<(&'static str, String)> {
        match self.auth.identity() {
            None => vec![],
            Some((id, signer)) => vec![
                (HTTP_HEADER_API_KEY, id.to_string()),
                (HTTP_HEADER_PAYLOAD_SIGNATURE, signer.sign(body)),
            ],
        }
    }

    /// Low-level POST for api-only cases (headers on non-signed topics, POST to public, ...).
    pub async fn post(&self, path: &str, body: String, headers: &[(&str, String)]) -> GateReply {
        let mut req = http_client().post(api_gate_url(path)).body(body);
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        send(req).await
    }
}

async fn send(req: RequestBuilder) -> GateReply {
    into_reply(req.send().await.expect("api gate request failed")).await
}

async fn into_reply(resp: Response) -> GateReply {
    let status = resp.status();
    let text = resp.text().await.expect("read api gate response");
    let payload = match text.is_empty() {
        true => Value::Null,
        false => serde_json::from_str(&text).unwrap_or(Value::String(text)),
    };
    GateReply {
        status,
        error_msg: None,
        payload,
    }
}
