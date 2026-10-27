//! `ws` gate provider: one connection per client (`wss://.../ws/`).
//!
//! Protocol (see gates/ws/src/ws.rs):
//! - every frame: `{"Signature": ..., "Uri": "/{service}/{topic_type}", "Data": {"Ts", "Method", "Params"}}`;
//!   the topic type is chosen only by `Uri`
//! - public/open topics: no authorization needed, `Signature` is just a client-chosen request id
//! - signed topics: the connection must first send `Authorize { ApiKey }` (no `Uri`) → `{"Authorized": true}`,
//!   then every frame carries the b64 ed25519 signature of `Data`; otherwise the gate closes the
//!   connection with policy code 1008 "Unauthorized"
//! - a signed frame whose `Ts` is outdated (more than `BACK_MESSAGE_TIMEOUT` from the gate clock), or
//!   whose signature is still pending, gets an `ErrCode` 409 reply; an outdated `Authorize` is refused (1008)
//! - replies: `{"Signature", "Status", "ErrorMsg", "ErrCode", "Payload"}`, correlated by the frame's
//!   `Signature`; `Status`/`ErrCode` is the HTTP status the api gate answers in the same case
//! - a provisional reply (`Status` 201/202) means the request was taken; its final reply (200)
//!   comes later with the same `Signature` — [`WsGate::wait_final_reply`] waits for it
//! - the gate drops a client silent for `clientTimeoutSecs` (10s); pings are answered
//!   automatically only while the client is reading (i.e. inside a request)

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use serde::Serialize;
use serde_json::{Value, json};
use skw_lib_shared::prelude::uuid::Uuid;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

use super::{Auth, Gate, GateReply};
use crate::common::{gate_body, ws_gate_url};

const WS_PATH: &str = "/ws/";
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_POLICY: u16 = 1008;

pub struct WsGate {
    conn: WsConn,
    auth: Auth,
    /// set once the gate closed the connection on auth grounds; every later call returns it
    rejected: Option<GateReply>,
}

impl Gate for WsGate {
    /// Connects and, if `auth` carries an identity, authorizes right away.
    async fn client(auth: Auth) -> Self {
        let mut gate = WsGate {
            conn: WsConn::connect().await,
            auth,
            rejected: None,
        };

        if let Some((id, signer)) = gate.auth.identity() {
            // the Authorize frame itself is never tampered: TamperedBody breaks later requests only
            let data = gate_body(&json!({ "Method": "Authorize", "Params": { "ApiKey": id } }));
            gate.conn
                .send_text(ws_frame(None, &data, &signer.sign(&data)))
                .await
                .expect("ws send failed");

            match gate.conn.recv().await {
                Ok(v) if v.get("Authorized") == Some(&Value::Bool(true)) => {}
                Ok(v) => panic!("unexpected authorize reply: {v}"),
                Err(e) => gate.rejected = Some(rejection(e)),
            }
        }
        gate
    }

    /// The client itself sends the `Public` method (the http gate injects it).
    async fn public(&mut self, service: &str) -> GateReply {
        self.request(
            &format!("/{service}/public"),
            &json!({ "Method": "Public", "Params": {} }),
        )
        .await
    }

    async fn open<M: Serialize>(&mut self, service: &str, method: &M) -> GateReply {
        self.request(&format!("/{service}/open"), method).await
    }

    async fn signed<M: Serialize>(&mut self, service: &str, method: &M) -> GateReply {
        self.request(&format!("/{service}/signed"), method).await
    }
}

impl WsGate {
    /// Sends `method` to any `uri` and waits for its reply.
    pub async fn request<M: Serialize>(&mut self, uri: &str, method: &M) -> GateReply {
        let id = self.send_request(uri, method).await;
        self.wait_reply(&id).await
    }

    /// Unsigned request with a caller-chosen request id, for id-collision cases.
    pub async fn request_with_id<M: Serialize>(&mut self, uri: &str, method: &M, request_id: &str) -> GateReply {
        self.send_frame(uri, &gate_body(method), request_id).await;
        self.wait_reply(request_id).await
    }

    /// Signs `method` with this client's credentials and sends it without waiting for the reply;
    /// returns the request id to pass to [`Self::wait_reply`]. Without credentials the frame's
    /// `Signature` is just a fresh request id (accepted on public/open topics).
    pub async fn send_request<M: Serialize>(&mut self, uri: &str, method: &M) -> String {
        let data = gate_body(method);
        let signature = match self.auth.identity() {
            Some((_, signer)) => signer.sign(&data),
            None => Uuid::now_v7().to_string(),
        };
        self.send_frame(uri, &data, &signature).await;
        signature
    }

    /// Sends the identical signed frame (same `Data`, same `Signature`) twice without waiting;
    /// returns the signature both replies carry.
    pub async fn send_request_twice<M: Serialize>(&mut self, uri: &str, method: &M) -> String {
        let data = gate_body(method);
        let (_, signer) = self
            .auth
            .identity()
            .expect("duplicate requests need a signing key");
        let signature = signer.sign(&data);
        self.send_frame(uri, &data, &signature).await;
        self.send_frame(uri, &data, &signature).await;
        signature
    }

    /// Signs and sends a ready-made `Data` body (e.g. one with an outdated `Ts`), waits for its reply.
    pub async fn request_signed_body(&mut self, uri: &str, data: &str) -> GateReply {
        let (_, signer) = self
            .auth
            .identity()
            .expect("signed bodies need a signing key");
        let signature = signer.sign(data);
        self.send_frame(uri, data, &signature).await;
        self.wait_reply(&signature).await
    }

    async fn send_frame(&mut self, uri: &str, data: &str, signature: &str) {
        // a rejected connection is closed; `wait_reply` reports the rejection
        if self.rejected.is_some() {
            return;
        }
        self.conn
            .send_text(ws_frame(Some(uri), &self.auth.wire_body(data), signature))
            .await
            .expect("ws send failed");
    }

    /// Waits for the reply to request `id`; replies to other requests arriving meanwhile are
    /// buffered, so replies can be collected in any order.
    pub async fn wait_reply(&mut self, id: &str) -> GateReply {
        if let Some(rejected) = &self.rejected {
            return rejected.clone();
        }

        match self.conn.reply(id).await {
            Ok(v) => into_reply(v),
            Err(e) => self.reject(e),
        }
    }

    /// Waits up to `timeout` for the final reply to request `id`: provisional replies (201/202)
    /// are logged and skipped. Panics if no final reply arrives in time.
    pub async fn wait_final_reply(&mut self, id: &str, timeout: Duration) -> GateReply {
        if let Some(rejected) = &self.rejected {
            return rejected.clone();
        }

        let started = Instant::now();
        let deadline = started + timeout;
        loop {
            let reply = match self.conn.reply_until(id, deadline).await {
                Ok(v) => into_reply(v),
                Err(WsError::Timeout) => panic!("request {id}: no final reply within {timeout:?}"),
                Err(e) => return self.reject(e),
            };
            if !matches!(reply.status, StatusCode::CREATED | StatusCode::ACCEPTED) {
                return reply;
            }
            eprintln!(
                "[test] request {id}: provisional {} after {:?} {}; waiting for the final reply (up to {timeout:?})",
                reply.status,
                started.elapsed(),
                reply.payload
            );
        }
    }

    /// Remembers a connection-level error: every later call returns it.
    fn reject(&mut self, e: WsError) -> GateReply {
        let rejected = rejection(e);
        self.rejected = Some(rejected.clone());
        rejected
    }
}

/// `Status` is the reply's HTTP-like status (201/202 for a provisional reply); `ErrCode` is kept
/// as the fallback for frames without it (e.g. the gate's own error/timeout frames).
fn into_reply(v: Value) -> GateReply {
    let status = v
        .get("Status")
        .or_else(|| v.get("ErrCode"))
        .and_then(Value::as_u64)
        .map(|c| {
            u16::try_from(c)
                .ok()
                .and_then(|c| StatusCode::from_u16(c).ok())
                .unwrap_or_else(|| panic!("invalid ErrCode in {v}"))
        })
        .unwrap_or(StatusCode::OK);
    GateReply {
        status,
        error_msg: v
            .get("ErrorMsg")
            .and_then(Value::as_str)
            .map(str::to_string),
        payload: v.get("Payload").cloned().unwrap_or(Value::Null),
    }
}

/// Close 1008 is how the ws gate refuses credentials: reported as 403, like the api gate.
fn rejection(e: WsError) -> GateReply {
    match e {
        WsError::Closed {
            code: Some(CLOSE_POLICY),
            reason,
        } => GateReply {
            status: StatusCode::FORBIDDEN,
            error_msg: Some(reason),
            payload: Value::Null,
        },
        WsError::Closed { code, reason } => panic!("ws gate closed the connection: {code:?} {reason}"),
        WsError::Timeout => panic!("ws gate did not reply within {REPLY_TIMEOUT:?}"),
        WsError::Transport(msg) => panic!("ws transport error: {msg}"),
    }
}

/// Raw frame text. `data` is embedded verbatim as `Data`: it must be byte-identical to what was signed.
pub fn ws_frame(uri: Option<&str>, data: &str, signature: &str) -> String {
    let uri = uri.map_or(String::new(), |u| format!(r#","Uri":{}"#, json!(u)));
    format!(r#"{{"Signature":{}{uri},"Data":{data}}}"#, json!(signature))
}

#[derive(Debug)]
enum WsError {
    Closed { code: Option<u16>, reason: String },
    Timeout,
    Transport(String),
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct WsConn {
    stream: WsStream,
    /// text frames received while waiting for another reply
    inbox: VecDeque<Value>,
}

impl WsConn {
    async fn connect() -> Self {
        let (stream, _) = tokio_tungstenite::connect_async_tls_with_config(ws_gate_url(WS_PATH), None, false, Some(tls_connector()))
            .await
            .expect("ws connect failed");
        WsConn {
            stream,
            inbox: VecDeque::new(),
        }
    }

    async fn send_text(&mut self, text: String) -> Result<(), WsError> {
        self.stream
            .send(Message::text(text))
            .await
            .map_err(|e| WsError::Transport(e.to_string()))
    }

    /// Waits for the reply carrying `signature`; other frames are kept for later `recv` calls.
    async fn reply(&mut self, signature: &str) -> Result<Value, WsError> {
        self.reply_until(signature, Instant::now() + REPLY_TIMEOUT)
            .await
    }

    /// [`Self::reply`] with an explicit deadline for the whole wait.
    async fn reply_until(&mut self, signature: &str, deadline: Instant) -> Result<Value, WsError> {
        let matches = |v: &Value| v.get("Signature").and_then(Value::as_str) == Some(signature);

        if let Some(i) = self.inbox.iter().position(matches) {
            return Ok(self.inbox.remove(i).expect("index from position"));
        }
        loop {
            let v = self.read_json(deadline).await?;
            if matches(&v) {
                return Ok(v);
            }
            self.inbox.push_back(v);
        }
    }

    /// Next text frame as JSON (buffered ones first).
    async fn recv(&mut self) -> Result<Value, WsError> {
        match self.inbox.pop_front() {
            Some(v) => Ok(v),
            None => self.read_json(Instant::now() + REPLY_TIMEOUT).await,
        }
    }

    async fn read_json(&mut self, deadline: Instant) -> Result<Value, WsError> {
        loop {
            let msg = tokio::time::timeout_at(deadline, self.stream.next())
                .await
                .map_err(|_| WsError::Timeout)?;
            match msg {
                Some(Ok(Message::Text(text))) => {
                    return serde_json::from_str(&text).map_err(|e| WsError::Transport(format!("non-json frame {text}: {e}")));
                }
                Some(Ok(Message::Close(frame))) => return Err(closed(frame)),
                // pings are answered by tungstenite itself while reading
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(WsError::Transport(e.to_string())),
                None => {
                    return Err(WsError::Closed {
                        code: None,
                        reason: "stream ended".into(),
                    });
                }
            }
        }
    }
}

fn closed(frame: Option<CloseFrame>) -> WsError {
    WsError::Closed {
        code: frame.as_ref().map(|f| u16::from(f.code)),
        reason: frame.map(|f| f.reason.to_string()).unwrap_or_default(),
    }
}

/// Accepts the gate's self-signed cert, like `http_client()`'s `danger_accept_invalid_certs`.
fn tls_connector() -> Connector {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("rustls protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
        .with_no_client_auth();
    Connector::Rustls(Arc::new(config))
}

#[derive(Debug)]
struct AcceptAnyCert(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
