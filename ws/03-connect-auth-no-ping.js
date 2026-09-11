// Connects, authorizes, then only waits — never calls ws.ping() itself, and
// `autoPong: false` disables the `ws` library's default behavior of
// automatically replying with a Pong to the server's heartbeat Ping frames.
// So this client never answers a Ping/Pong in any form, in any auth state.
// Expect: server's heartbeat (gates/ws/src/main.rs `heartbeat()`) detects
// the stale `hb` after CLIENT_TIMEOUT and force-disconnects via
// `sink.io().terminate()` — 'close' should fire within ~10-15s.
const crypto = require('crypto');
const WebSocket = require('ws');

// Dev-only keypair seeded for the root entity (services/auth/migrations/
// 20260513120000_init.up.sql) — same private key gates/api/src/signer.rs
// uses. Its public key is what skw-auth-service's GetKeyById will return
// for this id, so check_authorize (gates/ws/src/main.rs) can find it.
const ROOT_ENTITY_ID = '01a01500-98d5-71fc-9038-71acb78d61c4';
const ROOT_PRIVATE_KEY_PEM = '-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIBciTyz1f9ELrN3rZ+tcxxvQa14krR0sxY6HTJOLcWbK\n-----END PRIVATE KEY-----\n';
const privateKey = crypto.createPrivateKey({ key: ROOT_PRIVATE_KEY_PEM, format: 'pem' });

function buildMessage(method, params) {
  const data = { Ts: new Date().toISOString(), Method: method, Params: params };
  const signature = crypto.sign(null, Buffer.from(JSON.stringify(data)), privateKey).toString('base64');
  return JSON.stringify({ signature, data });
}

const url = process.env.WS_URL || 'wss://localhost:9443/ws/';
console.log(`[03] connecting to ${url}`);

const ws = new WebSocket(url, { rejectUnauthorized: false, autoPong: false });

ws.on('open', () => {
  const authMsg = buildMessage('Authorize', { ApiKey: ROOT_ENTITY_ID });
  console.log('[03] open — sending Authorize', authMsg);
  ws.send(authMsg);
  console.log('[03] authorized — now waiting, will not answer any pings (autoPong disabled)');
});
ws.on('ping', (data) => console.log('[03] recv ping (NOT answering — autoPong disabled)', data.toString()));
ws.on('pong', (data) => console.log('[03] recv pong', data.toString()));
ws.on('message', (data) => console.log('[03] recv message', data.toString()));
ws.on('close', (code, reason) => {
  console.log(`[03] closed code=${code} reason=${reason}`);
  process.exit(0);
});
ws.on('error', (err) => console.error('[03] error', err));
