// Connects and never sends anything.
// NOTE: the server's heartbeat timeout (gates/ws/src/main.rs `heartbeat()`)
// only stops sending pings once the client is considered timed out — it
// never actually sends a Close frame or drops the socket. So an
// unauthorized idle connection is never closed by the server; without a
// client-side timeout this script would hang forever waiting for a 'close'
// that never comes.
const WAIT_MS = 20000;
const WebSocket = require('ws');

const url = process.env.WS_URL || 'wss://localhost:9443/ws/';
console.log(`[01] connecting to ${url}`);

const ws = new WebSocket(url, { rejectUnauthorized: false });

ws.on('open', () => console.log('[01] open — waiting, sending nothing'));
ws.on('ping', (data) => console.log('[01] recv ping', data.toString()));
ws.on('pong', (data) => console.log('[01] recv pong', data.toString()));
ws.on('message', (data) => console.log('[01] recv message', data.toString()));
ws.on('close', (code, reason) => {
  console.log(`[01] closed code=${code} reason=${reason}`);
  process.exit(0);
});
ws.on('error', (err) => console.error('[01] error', err));

setTimeout(() => {
  console.log(`[01] no close received after ${WAIT_MS}ms — server does not proactively close idle/unauthorized connections, terminating client-side`);
  ws.terminate();
  process.exit(0);
}, WAIT_MS);
