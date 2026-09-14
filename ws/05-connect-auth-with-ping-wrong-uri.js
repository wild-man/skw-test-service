// Connects, authorizes, then actively sends its own Ping frames on an
// interval (in addition to whatever auto-pong behavior the library does for
// the server's own pings). Once authorized, it also periodically sends a
// real, dispatched RpcMethod::Ping request (uri: "/ping"), which gates/ws
// publishes to the ping service's Iggy topic and routes the async reply
// back to this connection — exercising the full gate -> Iggy -> service ->
// Iggy -> gate -> client round trip, not just the connection-level
// handshake/heartbeat.
// Every message (including the initial Authorize) is built with
// non-canonical JSON field order — see buildMessage() below — to prove
// signature verification depends only on the raw signed bytes, not on any
// particular field ordering.
// Expect: stays authorized and alive indefinitely, exercising the server's
// Frame::Ping -> heartbeat-refresh + Pong-reply path from the client side,
// plus a real RpcPayload::Ping { ts, count } reply arriving asynchronously
// on every RPC ping interval.
const crypto = require('crypto');
const WebSocket = require('ws');

// Dev-only keypair seeded for the root entity (services/auth/migrations/
// 20260513120000_init.up.sql) — same private key gates/api/src/signer.rs
// uses. Its public key is what skw-auth-service's GetKeyById will return
// for this id, so check_authorize (gates/ws/src/main.rs) can find it.
const ROOT_ENTITY_ID = '01a01500-98d5-71fc-9038-71acb78d61c4';
const ROOT_PRIVATE_KEY_PEM = '-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIBciTyz1f9ELrN3rZ+tcxxvQa14krR0sxY6HTJOLcWbK\n-----END PRIVATE KEY-----\n';
const privateKey = crypto.createPrivateKey({ key: ROOT_PRIVATE_KEY_PEM, format: 'pem' });

const url = process.env.WS_URL || 'wss://localhost:9443/ws/';
const PING_INTERVAL_MS = 3000;
const RPC_PING_INTERVAL_MS = 3000;
const MAX_TRACKED_SIGNATURES = 1000;
console.log(`[04] connecting to ${url}`);

const ws = new WebSocket(url, { rejectUnauthorized: false });
let pingTimer;
let rpcPingTimer;
let authorized = false;
let recvPingCount = 0;

// Sliding window of the last MAX_TRACKED_SIGNATURES signatures *we* sent.
// A JS Set iterates in insertion order, so the oldest tracked signature is
// always `sentSignatures.values().next().value` — cheap FIFO eviction with
// no separate array to keep in sync.
const sentSignatures = new Set();

function trackSentSignature(signature) {
  sentSignatures.add(signature);
  if (sentSignatures.size > MAX_TRACKED_SIGNATURES) {
    sentSignatures.delete(sentSignatures.values().next().value);
  }
}

// Field-order test: both the outer envelope (`data`/`uri` declared before
// `signature`, reversed from gates/ws/src/structs.rs's `WsRequest
// { signature, uri, data }`) and `data` itself (Params, Method, Ts —
// reversed from how JsonRpcGateHttpRequest would ever produce them: Ts
// always first, then Method/Params alphabetically) use non-canonical key
// order. The server verifies the signature against the raw `data` bytes
// exactly as sent on the wire (not a re-serialized/reordered copy), and
// JSON object field order never affects by-name deserialization — so this
// should authorize/verify/dispatch identically to canonical key order.
// `uri` selects the target service topic (first path segment, e.g. "ping")
// and is omitted for requests that don't need dispatching, like Authorize.
function buildMessage(method, params, uri) {
  const data = { Params: params, Method: method, Ts: new Date().toISOString() };
  const signature = crypto.sign(null, Buffer.from(JSON.stringify(data)), privateKey).toString('base64');
  const envelope = uri ? { data, uri, signature } : { data, signature };
  return { text: JSON.stringify(envelope), signature };
}

function buildTrackedMessage(method, params, uri) {
  const { text, signature } = buildMessage(method, params, uri);
  trackSentSignature(signature);
  return text;
}

ws.on('open', () => {
  const authMsg = buildTrackedMessage('Authorize', { ApiKey: ROOT_ENTITY_ID });
  console.log('[04] open — sending Authorize', authMsg);
  ws.send(authMsg);

  pingTimer = setInterval(() => {
    console.log('[04] sending ping');
    ws.ping();
  }, PING_INTERVAL_MS);
});
ws.on('ping', (data) => {
  recvPingCount += 1;
  console.log(`[04] recv ping (#${recvPingCount})`, data.toString());
});
ws.on('pong', (data) => console.log('[04] recv pong', data.toString()));
ws.on('message', (data) => {
  const text = data.toString();
  console.log('[04] recv message', text);

  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = null;
  }

  // Sanity check: every reply carrying a Signature must correlate to a
  // request we actually sent (within the last MAX_TRACKED_SIGNATURES sent
  // messages). Anything else means the gate is misrouting replies between
  // connections/requests — crash loudly rather than silently logging it.
  if (parsed && typeof parsed.Signature === 'string' && !sentSignatures.has(parsed.Signature)) {
    throw new Error(`[04] PANIC: received reply for a signature we did not send in the last ${MAX_TRACKED_SIGNATURES} sent messages: ${parsed.Signature}`);
  }

  if (authorized) return;

  if (parsed && parsed.authorized === true) {
    authorized = true;
    console.log('[04] authorized — starting periodic RpcMethod::Ping requests');
    rpcPingTimer = setInterval(() => {
      const msg = buildTrackedMessage('Pping', {}, '/ping');
      console.log('[04] sending real RpcMethod::Ping request', msg);
      ws.send(msg);
    }, RPC_PING_INTERVAL_MS);
  }
});
ws.on('close', (code, reason) => {
  console.log(`[04] closed code=${code} reason=${reason}`);
  clearInterval(pingTimer);
  clearInterval(rpcPingTimer);
});
ws.on('error', (err) => console.error('[04] error', err));
