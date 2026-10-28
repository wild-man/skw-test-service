//! Gate providers: every shared test is written once against [`Gate`] and run on each provider
//! via [`for_all_gates!`]. Replies are normalized to an HTTP status, so assertions are identical:
//! the api gate answers with a status, the ws gate with `ErrCode` (close 1008 → 403).

pub mod api;
pub mod ws;

pub use api::ApiGate;
pub use ws::{RawEnd, RawWs, WsGate};

use reqwest::StatusCode;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use skw_lib_shared::prelude::uuid::Uuid;

use crate::common::tamper;
use crate::keys::ApiKey;

/// Credentials a client acts with, including deliberately broken ones.
pub enum Auth {
    /// no credentials at all
    None,
    /// valid key
    Key(&'static ApiKey),
    /// claims `key`, but signs with `signer`
    SignedByOther {
        key: &'static ApiKey,
        signer: &'static ApiKey,
    },
    /// valid key, body changed after signing
    TamperedBody(&'static ApiKey),
    /// key id that doesn't exist, signed by `signer`
    UnknownKey { id: Uuid, signer: &'static ApiKey },
}

impl Auth {
    pub fn unknown_key(signer: &'static ApiKey) -> Self {
        Auth::UnknownKey {
            id: Uuid::now_v7(),
            signer,
        }
    }

    /// claimed key id and the key that actually signs
    pub fn identity(&self) -> Option<(Uuid, &'static ApiKey)> {
        match self {
            Auth::None => None,
            Auth::Key(k) | Auth::TamperedBody(k) => Some((k.id, k)),
            Auth::SignedByOther { key, signer } => Some((key.id, signer)),
            Auth::UnknownKey { id, signer } => Some((*id, signer)),
        }
    }

    /// bytes actually sent for a request whose signature was made over `body`
    pub fn wire_body(&self, body: &str) -> String {
        match self {
            Auth::TamperedBody(_) => tamper(body),
            _ => body.to_string(),
        }
    }
}

/// Gate reply normalized across providers.
#[derive(Debug, Clone)]
pub struct GateReply {
    pub status: StatusCode,
    /// ws `ErrorMsg` / close reason; the api gate sends none
    pub error_msg: Option<String>,
    pub payload: Value,
}

impl GateReply {
    /// Asserts 2xx and deserializes the service payload.
    pub fn payload<T: DeserializeOwned>(self) -> T {
        assert!(
            self.status.is_success(),
            "expected 2xx, got {}",
            self.describe()
        );
        serde_json::from_value(self.payload.clone()).unwrap_or_else(|e| panic!("unexpected payload {}: {e}", self.payload))
    }

    pub fn assert_status(&self, expected: StatusCode) {
        assert_eq!(
            self.status,
            expected,
            "unexpected reply: {}",
            self.describe()
        );
    }

    fn describe(&self) -> String {
        let msg = self.error_msg.as_deref().unwrap_or("-");
        format!("{} (error: {msg}) {}", self.status, self.payload)
    }
}

/// A gate as seen by a client: one value per test (ws: one connection).
pub trait Gate: Sized {
    async fn client(auth: Auth) -> Self;
    async fn public(&mut self, service: &str) -> GateReply;
    async fn open<M: Serialize>(&mut self, service: &str, method: &M) -> GateReply;
    async fn signed<M: Serialize>(&mut self, service: &str, method: &M) -> GateReply;
}

/// Runs each listed `async fn name<G: Gate>()` once per provider:
/// generates `api::name` and `ws::name` tests next to the generic functions.
macro_rules! for_all_gates {
    ($($test:ident),* $(,)?) => {
        mod api {
            $( #[tokio::test] async fn $test() { super::$test::<$crate::gate::ApiGate>().await } )*
        }
        mod ws {
            $( #[tokio::test] async fn $test() { super::$test::<$crate::gate::WsGate>().await } )*
        }
    };
}
pub(crate) use for_all_gates;
