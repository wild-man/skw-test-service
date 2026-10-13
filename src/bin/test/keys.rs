//! Entity keys used by tests. Created through the api gate (`auth` service, `CreateKey` open method)
//! once per test process and shared by all tests; they are fixtures, not the thing under test.

use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};
use skw_lib_http_protos::auth;
use skw_lib_shared::prelude::base64::*;
use skw_lib_shared::prelude::uuid::Uuid;
use tokio::sync::OnceCell;

use crate::common::{api_gate_url, gate_body, http_client};

static MAIN_KEY: OnceCell<ApiKey> = OnceCell::const_new();
static OTHER_KEY: OnceCell<ApiKey> = OnceCell::const_new();

/// The key tests act with.
pub async fn main_key() -> &'static ApiKey {
    MAIN_KEY.get_or_init(ApiKey::create).await
}

/// A second valid key, for "signed by someone else" cases.
pub async fn other_key() -> &'static ApiKey {
    OTHER_KEY.get_or_init(ApiKey::create).await
}

pub struct ApiKey {
    pub id: Uuid,
    signing_key: SigningKey,
}

impl ApiKey {
    async fn create() -> Self {
        let resp = http_client()
            .post(api_gate_url("/auth/open"))
            .body(gate_body(&auth::RpcMethod::CreateKey {
                store_private_key: false,
            }))
            .send()
            .await
            .expect("CreateKey request failed");
        let status = resp.status();
        let text = resp.text().await.expect("read CreateKey response");
        assert!(status.is_success(), "CreateKey failed: {status}: {text}");

        let auth::RpcPayload::CreateKey {
            id, private_key, ..
        } = serde_json::from_str(&text).unwrap_or_else(|e| panic!("unexpected CreateKey payload {text}: {e}"));

        // bare base64 PKCS#8 DER (PEM armor stripped by the auth service)
        let der = BASE64_STANDARD
            .decode(private_key)
            .expect("private key is not base64");
        let signing_key = SigningKey::from_pkcs8_der(&der).expect("private key is not ed25519 PKCS#8");
        ApiKey { id, signing_key }
    }

    /// base64 ed25519 signature of the exact body bytes
    pub fn sign(&self, body: &str) -> String {
        BASE64_STANDARD.encode(self.signing_key.sign(body.as_bytes()).to_bytes())
    }
}
