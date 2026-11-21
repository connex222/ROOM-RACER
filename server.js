'use strict';
/*
  Room Racers server
  - Serves the TV page (/) and the phone controller (/play)
  - Creates 4-letter room codes
  - Relays phone input to the TV, and TV messages back to phones
  - Phones use a WebSocket when they can, and fall back to plain web requests when they can't
  Run locally:  npm install  then  npm start
*/
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { WebSocketServer, WebSocket } = require('ws');

const VERSION = '0.4.0';
const PORT = Number(process.env.PORT) || 3000;
const PUBLIC = path.join(__dirname, 'public');
const ART = path.join(PUBLIC, 'art');
const ART_TYPES = { '.png': 'image/png', '.webp': 'image/webp', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg' };
const ROUTES = { '/': 'tv.html', '/tv': 'tv.html', '/play': 'phone.html', '/phone': 'phone.html', '/test': 'diag.html' };
const MAX_PLAYERS = 6;
const LETTERS = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; // no I or O, they look like 1 and 0
const TV_GRACE_MS = 60000;                  // how long a room waits for a lost TV
const POLL_HOLD_MS = 20000;                 // how long a fallback phone's request is held open
const HTTP_TIMEOUT_MS = 8000;               // fallback phone counts as gone after this much silence

function lanAddress() {
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) {
      if (a.family === 'IPv4' && !a.internal) return a.address;
    }
  }
  return null;
}

/* ---------- shared state ---------- */
const rooms = new Map();      // code -> { code, tv, players: Map(id -> player), expire }
const owner = new Map();      // player id -> room

const isOpen = ws => ws && ws.readyState === WebSocket.OPEN;
function send(ws, msg) { if (isOpen(ws)) ws.send(JSON.stringify(msg)); }
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
}
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

/* A player is connected by WebSocket (p.ws) or by the fallback (p.http) */
function isLive(p) { return isOpen(p.ws) || (p.http && (p.http.res || Date.now() - p.http.seen < HTTP_TIMEOUT_MS)); }
function flush(p) {
  const h = p.http;
  if (h && h.res && h.queue.length) {
    const res = h.res; h.res = null; clearTimeout(h.timer);
    json(res, 200, h.queue.splice(0));
  }
}
function sendP(p, msg) {
  if (isOpen(p.ws)) return p.ws.send(JSON.stringify(msg));
  if (p.http) { p.http.queue.push(msg); if (p.http.queue.length > 60) p.http.queue.shift(); flush(p); }
}
function dropConnection(p) {
  if (isOpen(p.ws)) p.ws.close(4000, 'replaced');
  if (p.http && p.http.res) { const r = p.http.res; p.http.res = null; clearTimeout(p.http.timer); json(r, 200, []); }
}

/* ---------- joining (used by both connection types) ---------- */
function joinRoom(m) {
  const code = String(m.code || '').toUpperCase().trim();
  const room = rooms.get(code);
  if (!room) return { reason: 'nocode' };
  let id = typeof m.rid === 'string' && room.players.has(m.rid) ? m.rid : null;
  if (id) {
    dropConnection(room.players.get(id));
  } else {
    let live = 0;
    for (const p of room.players.values()) if (isLive(p)) live++;
    if (live >= MAX_PLAYERS) return { reason: 'full' };
    id = newId();
  }
  const p = { name: cleanName(m.name), ws: null, http: null };
  room.players.set(id, p);
  owner.set(id, room);
  return { room, id, p };
}
function announce(room, id, p) {
  if (isOpen(room.tv)) send(room.tv, { t: 'join', id, name: p.name });
  else sendP(p, { t: 'notice', text: 'Waiting for the TV to reconnect' });
}
function fromPhone(room, id, m) {
  if (m.t === 'in') {
    const s = Math.max(-1, Math.min(1, Number(m.s) || 0));
    send(room.tv, { t: 'in', id, s, g: m.g ? 1 : 0, b: m.b ? 1 : 0 });
  } else if (m.t === 'pick') {
    const car = Math.max(0, Math.min(11, Math.floor(Number(m.car) || 0)));
    const driver = Math.max(0, Math.min(7, Math.floor(Number(m.driver) || 0)));
    send(room.tv, { t: 'pick', id, car, driver, ready: !!m.ready });
  }
}

/* ChatGPT art lives in public/art. A file is found by the ID it starts with, e.g. C01_toaster.png -> C01 */
function artFiles() {
  const found = {};
  let names = [];
  try { names = fs.readdirSync(ART); } catch { return found; }
  for (const n of names) {
    const ext = path.extname(n).toLowerCase();
    const m = /^([CDTPU]\d{2})/i.exec(n);
    if (m && ART_TYPES[ext] && !found[m[1].toUpperCase()]) found[m[1].toUpperCase()] = n;
  }
  return found;
}
function phoneLeft(room, id) {
  if (isOpen(room.tv)) send(room.tv, { t: 'leave', id });
}

/* ---------- web pages and the fallback ---------- */
function readBody(req, cb) {
  let data = '';
  req.on('data', chunk => {
    data += chunk;
    if (data.length > 8192) { req.destroy(); }
  });
  req.on('end', () => { try { cb(JSON.parse(data || '{}')); } catch { cb(null); } });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');

  if (url.pathname === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok ' + VERSION);
  }

  // Fallback: join
  if (url.pathname === '/api/join' && req.method === 'POST') {
    return readBody(req, m => {
      if (!m) return json(res, 400, { t: 'rejected', reason: 'bad' });
      const r = joinRoom(m);
      if (r.reason) return json(res, 200, { t: 'rejected', reason: r.reason });
      r.p.http = { queue: [], res: null, timer: null, seen: Date.now() };
      json(res, 200, { t: 'joined', id: r.id, code: r.room.code });
      announce(r.room, r.id, r.p);
    });
  }

  // Fallback: phone sends a message (input or ping)
  if (url.pathname === '/api/send' && req.method === 'POST') {
    return readBody(req, body => {
      const room = body && owner.get(body.id);
      const p = room && room.players.get(body.id);
      if (!p || !p.http) return json(res, 410, { t: 'gone' });
      p.http.seen = Date.now();
      const m = body.m || {};
      if (m.t === 'ping') return json(res, 200, { t: 'pong', ts: m.ts });
      fromPhone(room, body.id, m);
      json(res, 200, { ok: 1 });
    });
  }

  // Fallback: phone waits for messages from the TV
  if (url.pathname === '/api/poll' && req.method === 'GET') {
    const id = url.searchParams.get('id');
    const room = owner.get(id);
    const p = room && room.players.get(id);
    if (!p || !p.http) return json(res, 410, { t: 'gone' });
    const h = p.http;
    h.seen = Date.now();
    if (h.res) { const old = h.res; clearTimeout(h.timer); json(old, 200, []); }
    h.res = res;
    if (h.queue.length) return flush(p);
    h.timer = setTimeout(() => { if (h.res === res) { h.res = null; h.seen = Date.now(); json(res, 200, []); } }, POLL_HOLD_MS);
    req.on('close', () => { if (h.res === res) { h.res = null; clearTimeout(h.timer); } });
    return;
  }

  if (url.pathname === '/art/list') return json(res, 200, Object.keys(artFiles()));
  const artMatch = /^\/art\/([CDTPU]\d{2})$/i.exec(url.pathname);
  if (artMatch) {
    const name = artFiles()[artMatch[1].toUpperCase()];
    if (!name) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('No art yet'); }
    return fs.readFile(path.join(ART, name), (err, data) => {
      if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('No art yet'); }
      res.writeHead(200, { 'Content-Type': ART_TYPES[path.extname(name).toLowerCase()], 'Cache-Control': 'no-cache' });
      res.end(data);
    });
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
    if (isLive(p)) {
      send(ws, { t: 'join', id, name: p.name, rejoin: true });
      sendP(p, { t: 'notice', text: 'TV is back' });
    }
  }
}

function fromTv(ws, m) {
  const room = ws.room;
  if (m.t === 'to') {
    const p = room.players.get(m.id);
    if (p) sendP(p, m.msg);
  } else if (m.t === 'all') {
    for (const p of room.players.values()) sendP(p, m.msg);
  } else if (m.t === 'kick') {
    const p = room.players.get(m.id);
    if (p) {
      if (m.reason) sendP(p, { t: 'rejected', reason: m.reason });
      if (isOpen(p.ws)) p.ws.close(4001, 'removed');
      room.players.delete(m.id);
      owner.delete(m.id);
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
    if (ws.role === 'phone') return fromPhone(ws.room, ws.pid, m);
    if (m.t === 'host') return host(ws, m, req);
    if (m.t === 'join') {
      const r = joinRoom(m);
      if (r.reason) return send(ws, { t: 'rejected', reason: r.reason });
      r.p.ws = ws; ws.role = 'phone'; ws.room = r.room; ws.pid = r.id;
      send(ws, { t: 'joined', id: r.id, code: r.room.code });
      announce(r.room, r.id, r.p);
    }
  });

  ws.on('close', () => {
    const room = ws.room;
    if (!room) return;
    if (ws.role === 'tv' && room.tv === ws) {
      room.tv = null;
      for (const p of room.players.values()) sendP(p, { t: 'notice', text: 'TV lost connection, hang on' });
      room.expire = setTimeout(() => {
        for (const [id, p] of room.players) { sendP(p, { t: 'ended' }); owner.delete(id); }
        rooms.delete(room.code);
      }, TV_GRACE_MS);
    } else if (ws.role === 'phone') {
      const p = room.players.get(ws.pid);
      if (p && p.ws === ws) { p.ws = null; phoneLeft(room, ws.pid); }
    }
  });
});

// Keep WebSockets alive, drop dead ones, and notice fallback phones that have gone quiet
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    for (const [id, p] of room.players) {
      if (p.http && !p.http.res && now - p.http.seen > HTTP_TIMEOUT_MS) { p.http = null; phoneLeft(room, id); }
    }
  }
}, 3000);

server.listen(PORT, () => {
  const ip = lanAddress();
  console.log('Room Racers ' + VERSION + ' running');
  console.log(`  TV (this laptop):  http://localhost:${PORT}`);
  if (ip) console.log(`  Phones (same Wi-Fi): http://${ip}:${PORT}/play`);
});
