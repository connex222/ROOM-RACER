'use strict';
/*
  Room Racers server
  - Serves the TV page (/) and the phone controller (/play)
  - Creates 4-letter room codes
  - Relays phone input to the TV, and TV messages back to phones
  Run locally:  npm install  then  npm start
*/
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const PUBLIC = path.join(__dirname, 'public');
const ROUTES = { '/': 'tv.html', '/tv': 'tv.html', '/play': 'phone.html', '/phone': 'phone.html' };
const MAX_PLAYERS = 6;
const LETTERS = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; // no I or O, they look like 1 and 0
const TV_GRACE_MS = 60000;                  // how long a room waits for a lost TV

function lanAddress() {
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) {
      if (a.family === 'IPv4' && !a.internal) return a.address;
    }
  }
  return null;
}

/* ---------- web pages ---------- */
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }
  const file = ROUTES[url.pathname];
  if (!file) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    return res.end('Not found');
  }
  fs.readFile(path.join(PUBLIC, file), (err, data) => {
    if (err) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      return res.end('Server error');
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

/* ---------- live connections ---------- */
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 8 * 1024 });
const rooms = new Map();

const isOpen = ws => ws && ws.readyState === WebSocket.OPEN;
function send(ws, msg) { if (isOpen(ws)) ws.send(JSON.stringify(msg)); }
function newCode() {
  let c;
  do {
    c = '';
    for (let i = 0; i < 4; i++) c += LETTERS[Math.floor(Math.random() * LETTERS.length)];
  } while (rooms.has(c));
  return c;
}
const newId = () => Math.random().toString(36).slice(2, 10);
const cleanName = n => String(n || '').replace(/[<>&"]/g, '').trim().slice(0, 12) || 'Racer';

function host(ws, m, req) {
  let room = typeof m.code === 'string' ? rooms.get(m.code.toUpperCase()) : null;
  if (room && isOpen(room.tv)) room = null;          // code already has a live TV
  if (!room) {
    const code = newCode();
    room = { code, tv: null, players: new Map(), expire: null };
    rooms.set(code, room);
  }
  if (room.expire) { clearTimeout(room.expire); room.expire = null; }
  room.tv = ws; ws.role = 'tv'; ws.room = room;

  const hostName = (req.headers.host || '').split(':')[0];
  const local = hostName === 'localhost' || hostName === '127.0.0.1';
  const ip = local ? lanAddress() : null;
  send(ws, { t: 'hosted', code: room.code, lan: ip ? `http://${ip}:${PORT}` : null });

  // A TV that reconnects gets told who is still here
  for (const [id, p] of room.players) {
    if (isOpen(p.ws)) {
      send(ws, { t: 'join', id, name: p.name, rejoin: true });
      send(p.ws, { t: 'notice', text: 'TV is back' });
    }
  }
}

function join(ws, m) {
  const code = String(m.code || '').toUpperCase().trim();
  const room = rooms.get(code);
  if (!room) return send(ws, { t: 'rejected', reason: 'nocode' });
  const name = cleanName(m.name);

  let id = typeof m.rid === 'string' && room.players.has(m.rid) ? m.rid : null;
  if (id) {
    const old = room.players.get(id);
    if (old.ws && old.ws !== ws && isOpen(old.ws)) old.ws.close(4000, 'replaced');
  } else {
    let live = 0;
    for (const p of room.players.values()) if (isOpen(p.ws)) live++;
    if (live >= MAX_PLAYERS) return send(ws, { t: 'rejected', reason: 'full' });
    id = newId();
  }
  room.players.set(id, { ws, name });
  ws.role = 'phone'; ws.room = room; ws.pid = id;
  send(ws, { t: 'joined', id, code: room.code });
  if (isOpen(room.tv)) send(room.tv, { t: 'join', id, name });
  else send(ws, { t: 'notice', text: 'Waiting for the TV to reconnect' });
}

function fromPhone(ws, m) {
  const room = ws.room;
  if (m.t === 'in') {
    const s = Math.max(-1, Math.min(1, Number(m.s) || 0));
    send(room.tv, { t: 'in', id: ws.pid, s, g: m.g ? 1 : 0, b: m.b ? 1 : 0 });
  }
}

function fromTv(ws, m) {
  const room = ws.room;
  if (m.t === 'to') {
    const p = room.players.get(m.id);
    if (p) send(p.ws, m.msg);
  } else if (m.t === 'all') {
    for (const p of room.players.values()) send(p.ws, m.msg);
  } else if (m.t === 'kick') {
    const p = room.players.get(m.id);
    if (p) {
      if (m.reason) send(p.ws, { t: 'rejected', reason: m.reason });
      if (isOpen(p.ws)) p.ws.close(4001, 'removed');
      room.players.delete(m.id);
    }
  }
}

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'ping') return send(ws, { t: 'pong', ts: m.ts });
    if (ws.role === 'tv') return fromTv(ws, m);
    if (ws.role === 'phone') return fromPhone(ws, m);
    if (m.t === 'host') return host(ws, m, req);
    if (m.t === 'join') return join(ws, m);
  });

  ws.on('close', () => {
    const room = ws.room;
    if (!room) return;
    if (ws.role === 'tv' && room.tv === ws) {
      room.tv = null;
      for (const p of room.players.values()) send(p.ws, { t: 'notice', text: 'TV lost connection, hang on' });
      room.expire = setTimeout(() => {
        for (const p of room.players.values()) send(p.ws, { t: 'ended' });
        rooms.delete(room.code);
      }, TV_GRACE_MS);
    } else if (ws.role === 'phone') {
      const p = room.players.get(ws.pid);
      if (p && p.ws === ws) {
        p.ws = null;
        send(room.tv, { t: 'leave', id: ws.pid });
      }
    }
  });
});

// Keep connections alive and drop dead ones
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => {
  const ip = lanAddress();
  console.log(`Room Racers running`);
  console.log(`  TV (this laptop):  http://localhost:${PORT}`);
  if (ip) console.log(`  Phones (same Wi-Fi): http://${ip}:${PORT}/play`);
});
