// Connects and sends a random non-JSON string.
// Expect: server replies {"error": "..."} (fails to parse as WsRequest),
// connection stays open and unauthorized.
// NOTE: same as script 01 — the server's heartbeat timeout never actually
// closes an unauthorized connection, it just stops pinging it. Without a
// client-side timeout this would hang forever waiting for a 'close'.
const WAIT_MS = 20000;
const crypto = require('crypto');
const WebSocket = require('ws');

const url = process.env.WS_URL || 'wss://localhost:9443/ws/';
console.log(`[02] connecting to ${url}`);

const ws = new WebSocket(url, { rejectUnauthorized: false });

ws.on('open', () => {
  const junk = crypto.randomBytes(16).toString('hex');
  console.log(`[02] open — sending random string: ${junk}`);
  ws.send(junk);
});
ws.on('ping', (data) => console.log('[02] recv ping', data.toString()));
ws.on('pong', (data) => console.log('[02] recv pong', data.toString()));
ws.on('message', (data) => console.log('[02] recv message', data.toString()));
ws.on('close', (code, reason) => {
  console.log(`[02] closed code=${code} reason=${reason}`);
  process.exit(0);
});
ws.on('error', (err) => console.error('[02] error', err));

setTimeout(() => {
  console.log(`[02] no close received after ${WAIT_MS}ms — server does not proactively close idle/unauthorized connections, terminating client-side`);
  ws.terminate();
  process.exit(0);
}, WAIT_MS);
