// CASE STORM — realtime server (chat, live drops, online counter, battles between real players).
// No dependencies: Node 18+. Run:  node server.js   (PORT env, default 3000)
// Serves dist/index.html (or index.html) and an SSE stream at /events + JSON API under /api.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = +process.env.PORT || 3000;
const ROOT = __dirname;
const PAGE = fs.existsSync(path.join(ROOT, 'dist', 'index.html')) ? path.join(ROOT, 'dist', 'index.html') : path.join(ROOT, 'index.html');

const clients = new Map();          // res -> { id, name }
const chat = [];                    // last messages
const feed = [];                    // last drops
let top = [];                       // best drops in 24h
const lobbies = new Map();          // id -> lobby
const lastMsg = new Map();          // clientId -> ts (rate limit)
const MODES = { '1v1': 2, '1v1v1': 3, '1v1v1v1': 4, '2v2': 4 };
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const pending = new Map();          // pid -> [gift] (virtual balance gifts waiting for delivery)
let badKeyAt = 0, badKeys = 0;
const PID_RE = /^CS-[0-9A-Z]{8}$/;
function keyOk(k) {
  if (!ADMIN_KEY) return false;
  const a = Buffer.from(String(k || '')), b = Buffer.from(ADMIN_KEY);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
function sendToPid(pid, ev, data) { let n = 0; for (const [res, c] of clients) if (c.pid === pid) { send(res, ev, data); n++; } return n; }

const clean = (s, n) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, n);
function online() { return new Set([...clients.values()].map(c => c.id)).size; }
function send(res, ev, data) { try { res.write(`event: ${ev}\ndata: ${JSON.stringify(data)}\n\n`); } catch (e) { } }
function broadcast(ev, data) { for (const res of clients.keys()) send(res, ev, data); }
function lobbyList() { return [...lobbies.values()].filter(l => l.status !== 'done').map(publicLobby); }
function publicLobby(l) { return { id: l.id, mode: l.mode, cases: l.cases, crazy: l.crazy, players: l.players.map(p => ({ id: p.id, name: p.name })), slots: l.slots, status: l.status, seeds: l.seeds, startAt: l.startAt, created: l.created }; }
function pushLobbies() { broadcast('lobbies', lobbyList()); }

function body(req) {
  return new Promise(res => { let d = ''; req.on('data', c => { d += c; if (d.length > 1e5) req.destroy(); }); req.on('end', () => { try { res(JSON.parse(d || '{}')); } catch (e) { res({}); } }); });
}
function json(res, code, obj) { res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }); res.end(JSON.stringify(obj)); }

const api = {
  async chat(b) {
    const id = clean(b.id, 40), name = clean(b.name, 16) || 'Игрок', text = clean(b.text, 200);
    if (!id || !text) return [400, { error: 'empty' }];
    const now = Date.now(); if (now - (lastMsg.get(id) || 0) < 1500) return [429, { error: 'Не так быстро' }];
    lastMsg.set(id, now);
    const m = { id: crypto.randomUUID(), from: id, pid: PID_RE.test(b.pid) ? b.pid : '', name, text, lvl: Math.max(1, Math.min(99, +b.lvl || 1)), t: now };
    chat.push(m); if (chat.length > 80) chat.shift();
    broadcast('chat', m); return [200, { ok: 1 }];
  },
  async drop(b) {
    const d = { name: clean(b.name, 16) || 'Игрок', skin: clean(b.skin, 120), w: Math.max(0, Math.min(4, +b.w || 0)), st: !!b.st, p: Math.max(0, +b.p || 0), cs: clean(b.cs, 40), t: Date.now() };
    if (!d.skin) return [400, {}];
    feed.unshift(d); if (feed.length > 40) feed.pop();
    top = top.filter(x => x.t > Date.now() - 864e5).concat([d]).sort((a, c) => c.p - a.p).slice(0, 25);
    broadcast('drop', d); return [200, { ok: 1 }];
  },
  async create(b) {
    const id = clean(b.id, 40), name = clean(b.name, 16) || 'Игрок';
    const mode = MODES[b.mode] ? b.mode : '1v1';
    const cases = (Array.isArray(b.cases) ? b.cases : []).slice(0, 10).map(c => clean(c, 40)).filter(Boolean);
    if (!id || !cases.length) return [400, { error: 'bad' }];
    for (const l of lobbies.values()) if (l.status === 'open' && l.players.some(p => p.id === id)) return [409, { error: 'Ты уже в лобби' }];
    const l = { id: crypto.randomBytes(4).toString('hex'), mode, cases, crazy: !!b.crazy, slots: MODES[mode], players: [{ id, name }], status: 'open', created: Date.now() };
    lobbies.set(l.id, l); pushLobbies(); return [200, publicLobby(l)];
  },
  async join(b) {
    const l = lobbies.get(clean(b.lobby, 20)); const id = clean(b.id, 40), name = clean(b.name, 16) || 'Игрок';
    if (!l || l.status !== 'open') return [404, { error: 'Лобби уже недоступно' }];
    if (l.players.some(p => p.id === id)) return [409, { error: 'Ты уже в этом лобби' }];
    if (l.players.length >= l.slots) return [409, { error: 'Мест нет' }];
    l.players.push({ id, name });
    if (l.players.length === l.slots) start(l); else pushLobbies();
    return [200, publicLobby(l)];
  },
  async ack(b) {
    const pid = clean(b.pid, 12), gid = clean(b.gid, 40); const list = pending.get(pid);
    if (list) { const rest = list.filter(g => g.id !== gid); rest.length ? pending.set(pid, rest) : pending.delete(pid); }
    return [200, { ok: 1 }];
  },
  async 'admin/online'(b) {
    if (badKeys > 10 && Date.now() - badKeyAt < 60000) return badKey();
    if (!keyOk(b.key)) return badKey();
    const seen = new Map(); for (const c of clients.values()) if (c.pid) seen.set(c.pid, c.name || 'Игрок');
    return [200, { players: [...seen].map(([pid, name]) => ({ pid, name })), pending: pending.size }];
  },
  async 'admin/gift'(b) {
    if (badKeys > 10 && Date.now() - badKeyAt < 60000) return badKey();
    if (!keyOk(b.key)) return badKey();
    const pid = clean(b.pid, 12).toUpperCase(); const usd = Math.round((+b.usd || 0) * 100) / 100; const note = clean(b.note, 80);
    if (!PID_RE.test(pid)) return [400, { error: 'ID должен быть вида CS-XXXXXXXX' }];
    if (!(usd > 0 && usd <= 100000)) return [400, { error: 'Сумма от 0,01 до 100 000 $' }];
    const g = { id: crypto.randomUUID(), usd, note, t: Date.now() };
    const list = pending.get(pid) || []; list.push(g); pending.set(pid, list.slice(-20));
    const online = sendToPid(pid, 'gift', g);
    return [200, { ok: 1, online: online > 0 }];
  },
  async leave(b) {
    const l = lobbies.get(clean(b.lobby, 20)); const id = clean(b.id, 40);
    if (!l || l.status !== 'open') return [404, { error: 'Лобби уже стартовало' }];
    l.players = l.players.filter(p => p.id !== id);
    if (!l.players.length) lobbies.delete(l.id);
    pushLobbies(); return [200, { ok: 1 }];
  },
};
function badKey() { const now = Date.now(); if (now - badKeyAt > 60000) badKeys = 0; badKeyAt = now; badKeys++; return [ADMIN_KEY ? 403 : 503, { error: ADMIN_KEY ? (badKeys > 10 ? 'Слишком много попыток, подожди минуту' : 'Неверный ключ') : 'На сервере не задан ADMIN_KEY' }]; }
function start(l) {
  l.status = 'running';
  l.seeds = l.cases.map(() => l.players.map(() => crypto.randomBytes(4).readUInt32LE(0)));
  l.startAt = Date.now() + 3500;
  broadcast('battle', publicLobby(l)); pushLobbies();
  const dur = 3500 + l.cases.length * 5200 + 4000;
  setTimeout(() => { l.status = 'done'; pushLobbies(); setTimeout(() => lobbies.delete(l.id), 60000); }, dur);
}
// expire stale lobbies (10 min)
setInterval(() => {
  const now = Date.now(); let ch = false;
  for (const l of lobbies.values()) if (l.status === 'open' && now - l.created > 10 * 60000) { l.status = 'done'; broadcast('cancel', { id: l.id, players: l.players.map(p => p.id) }); lobbies.delete(l.id); ch = true; }
  if (ch) pushLobbies();
}, 15000);
// heartbeat keeps proxies from closing the stream
setInterval(() => { for (const res of clients.keys()) { try { res.write(': ping\n\n'); } catch (e) { } } broadcast('online', online()); }, 20000);

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'GET,POST' }); return res.end(); }
  if (url.pathname === '/events') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'Access-Control-Allow-Origin': '*', 'X-Accel-Buffering': 'no' });
    const pidQ = clean(url.searchParams.get('pid'), 12);
    const c = { id: clean(url.searchParams.get('id'), 40) || crypto.randomUUID(), name: clean(url.searchParams.get('name'), 16), pid: PID_RE.test(pidQ) ? pidQ : '' };
    clients.set(res, c);
    send(res, 'hello', { online: online(), chat: chat.slice(-40), feed: feed.slice(0, 30), top, lobbies: lobbyList() });
    for (const g of (pending.get(c.pid) || [])) send(res, 'gift', g);
    broadcast('online', online());
    req.on('close', () => {
      clients.delete(res);
      // drop the player from open lobbies when the last tab closes
      if (![...clients.values()].some(x => x.id === c.id)) {
        let ch = false;
        for (const l of lobbies.values()) if (l.status === 'open' && l.players.some(p => p.id === c.id)) { l.players = l.players.filter(p => p.id !== c.id); if (!l.players.length) lobbies.delete(l.id); ch = true; broadcast('cancel', { id: l.id, players: [c.id], partial: true }); }
        if (ch) pushLobbies();
      }
      broadcast('online', online());
    });
    return;
  }
  if (url.pathname.startsWith('/api/') && req.method === 'POST') {
    const fn = api[url.pathname.slice(5)]; if (!fn) return json(res, 404, { error: 'no' });
    const [code, out] = await fn(await body(req)); return json(res, code, out);
  }
  const st = url.pathname.match(/^\/cases\/([a-z0-9_-]+)\.webp$/);
  if (st) { const f = path.join(ROOT, 'cases', st[1] + '.webp'); if (fs.existsSync(f)) { res.writeHead(200, { 'Content-Type': 'image/webp', 'Cache-Control': 'public, max-age=86400', 'Access-Control-Allow-Origin': '*' }); return fs.createReadStream(f).pipe(res); } }
  if (url.pathname === '/' || url.pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return fs.createReadStream(PAGE).pipe(res);
  }
  res.writeHead(404); res.end('not found');
});
server.listen(PORT, () => console.log(`CASE STORM server: http://localhost:${PORT}`));
