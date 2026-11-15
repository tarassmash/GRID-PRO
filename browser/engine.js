// ================= Движок сеточного бота в браузере (Bybit API v5) =================
// Тот же алгоритм, что в bot.py на Railway, переписанный на JavaScript. Страница общается с ним
// через те же адреса /api/..., только вместо сервера их обслуживает этот код.
(() => {
"use strict";
const _fetch = window.fetch.bind(window);
const HOSTS = {testnet: "https://api-testnet.bybit.com", demo: "https://api-demo.bybit.com", mainnet: "https://api.bybit.com"};
const DEFAULT_CFG = {api_key: "", api_secret: "", network: "testnet", market: "spot",
  leverage: 1, symbol: "BTCUSDT", capital: 200, range_pct: 5, levels: 4,
  trail_up: true, trail_down: false, trail_up_limit: 0, trail_down_limit: 0,
  trail_pct: 0.5, trail_down_cap: 5, stop_pct: 2,
  radar: "pause", radar_sens: "mid", lot_mode: "geo", lot_mult: 1.4,
  direction: "both", cap_max: false};
const CAP_MAX_SPOT = 0.97, CAP_MAX_FUT = 0.95;
const SPOT_FEE = 0.001, SPOT_FEE_BUF = 0.0015, FUT_FEE = 0.0002;
const CHASE_TRIES = 6, CHASE_WAIT = 2500;
const LIQ_GUARD = 0.35, STOP_BELOW = 0.05, POLL_MS = 3000, TRAIL_MAX_STEPS = 10;
const RADAR_SENS = {low: [75, 45], mid: [60, 35], high: [48, 28]};
const RADAR_CALM_TICKS = 20, RADAR_MAX_PAUSE = 30 * 60, RADAR_EXIT = 90;
const GLOBAL_KEYS = ["api_key", "api_secret", "network"];
const GRID_KEYS = Object.keys(DEFAULT_CFG).filter(k => !GLOBAL_KEYS.includes(k));
const STORE_KEY = "gridpro_store_v1";

const clamp = x => Math.max(0, Math.min(1, x));
class BybitError extends Error {}
const decOf = step => { const s = String(step); if (s.includes("e-")) return parseInt(s.split("e-")[1], 10);
  const i = s.indexOf("."); return i < 0 ? 0 : s.length - i - 1; };
// округлить вниз (к нулю) до шага биржи
function q(x, step) { step = Number(step); if (!(step > 0)) return x;
  const k = Math.trunc(x / step + (x >= 0 ? 1e-9 : -1e-9)); return Number((k * step).toFixed(decOf(step))); }
const fnum = (x, d = 2) => Number(x).toFixed(d);
const hhmmss = () => new Date().toTimeString().slice(0, 8);
const median = a => { if (!a.length) return 0; const s = [...a].sort((x, y) => x - y), m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

// ---------- таймер в фоновом потоке: вкладка в фоне не тормозит бота ----------
const sleep = (() => {
  try {
    const src = "onmessage=e=>setTimeout(()=>postMessage(e.data.id),e.data.ms)";
    const w = new Worker(URL.createObjectURL(new Blob([src], {type: "text/javascript"})));
    const cb = {}; let n = 0, ok = true;
    w.onmessage = e => { const f = cb[e.data]; delete cb[e.data]; f && f(); };
    w.onerror = () => { ok = false; Object.values(cb).forEach(f => f()); };
    return ms => ok ? new Promise(r => { const id = ++n; cb[id] = r; w.postMessage({id, ms}); })
                    : new Promise(r => setTimeout(r, ms));
  } catch (e) { return ms => new Promise(r => setTimeout(r, ms)); }
})();
const uid = () => (crypto.randomUUID ? crypto.randomUUID().replace(/-/g, "") :
  [...crypto.getRandomValues(new Uint8Array(16))].map(b => b.toString(16).padStart(2, "0")).join("")).slice(0, 24);

// ---------- настройки в браузере ----------
function loadStore() {
  let raw = {};
  try { raw = JSON.parse(localStorage.getItem(STORE_KEY) || "{}") || {}; } catch (e) {}
  const st = {};
  GLOBAL_KEYS.forEach(k => st[k] = raw[k] ?? DEFAULT_CFG[k]);
  let grids = raw.grids && typeof raw.grids === "object" ? raw.grids : {"1": {was_running: false}};
  st.grids = {};
  for (const [gid, g] of Object.entries(grids)) {
    st.grids[gid] = Object.assign({}, ...GRID_KEYS.map(k => ({[k]: DEFAULT_CFG[k]})), g);
  }
  if (!Object.keys(st.grids).length) st.grids["1"] = Object.assign({}, ...GRID_KEYS.map(k => ({[k]: DEFAULT_CFG[k]})), {was_running: false});
  return st;
}
function saveStore(st) { try { localStorage.setItem(STORE_KEY, JSON.stringify(st)); } catch (e) { console.warn("не удалось сохранить настройки", e); } }
function gridCfg(gid, st) { st = st || loadStore();
  const g = st.grids[gid] || Object.assign({}, ...GRID_KEYS.map(k => ({[k]: DEFAULT_CFG[k]})));
  return Object.assign({}, g, ...GLOBAL_KEYS.map(k => ({[k]: st[k]}))); }
function setAutostart(flag, gid) { const st = loadStore(); if (st.grids[gid]) { st.grids[gid].was_running = !!flag; saveStore(st); } }
const newGridId = st => String(Math.max(0, ...Object.keys(st.grids).map(Number)) + 1);
const freshGrid = (src = {}) => Object.assign({}, ...GRID_KEYS.map(k => ({[k]: DEFAULT_CFG[k]})),
  Object.fromEntries(Object.entries(src).filter(([k]) => GRID_KEYS.includes(k))), {was_running: false});

// ---------- Bybit API v5 ----------
class Bybit {
  constructor(key, secret, network, cat = "spot") {
    this.cat = cat; this.key = (key || "").trim(); this.secret = (secret || "").trim();
    this.base = HOSTS[network] || HOSTS.testnet; this.offset = 0; this.recv = "10000";
    this.qd = 8; this.pd = 8; this.qqd = 8;   // знаков после запятой для объёма, цены, суммы в USDT
  }
  async sign(msg) {
    if (!crypto.subtle) throw new BybitError("Браузер не даёт подписать запрос: откройте файл в Chrome, Edge, Safari или Firefox");
    if (!this._k) this._k = await crypto.subtle.importKey("raw", new TextEncoder().encode(this.secret),
      {name: "HMAC", hash: "SHA-256"}, false, ["sign"]);
    const s = await crypto.subtle.sign("HMAC", this._k, new TextEncoder().encode(msg));
    return [...new Uint8Array(s)].map(b => b.toString(16).padStart(2, "0")).join("");
  }
  async req(method, path, params = {}, signed = false) {
    let url = this.base + path, body, payload;
    if (method === "GET") {
      payload = new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)])).toString();
      if (payload) url += "?" + payload;
    } else { payload = JSON.stringify(params); body = payload; }
    const headers = {};
    if (method !== "GET") headers["Content-Type"] = "application/json";
    if (signed) {
      if (!this.key || !this.secret) throw new BybitError("Введите API ключ и секрет в настройках");
      const ts = String(Math.round(Date.now() + this.offset));
      const sign = await this.sign(ts + this.key + this.recv + payload);
      Object.assign(headers, {"X-BAPI-API-KEY": this.key, "X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": sign, "X-BAPI-RECV-WINDOW": this.recv});
    }
    const ctl = new AbortController(), to = setTimeout(() => ctl.abort(), 15000);
    let r;
    try { r = await _fetch(url, {method, headers, body, signal: ctl.signal, cache: "no-store"}); }
    catch (e) {
      if (e.name === "AbortError") throw new BybitError("Bybit не ответил за 15 секунд");
      throw new BybitError(`Нет связи с Bybit (${signed ? "запрос с ключом" : "рыночные данные"}). Проверьте интернет. ` +
        `Если интернет есть, значит Bybit не пускает запросы прямо со страницы — тогда нужна версия бота на Railway.`);
    } finally { clearTimeout(to); }
    let res; try { res = await r.json(); } catch (e) { throw new BybitError(`HTTP ${r.status}: Bybit вернул не JSON`); }
    if (res.retCode !== 0) throw new BybitError(`${res.retMsg} (код ${res.retCode})`);
    return res.result;
  }
  async syncTime() { const r = await this.req("GET", "/v5/market/time"); this.offset = Number(r.timeNano) / 1e6 - Date.now(); }
  async price(sym) { return (await this.bbo(sym))[0]; }
  async bbo(sym) {
    const t = (await this.req("GET", "/v5/market/tickers", {category: this.cat, symbol: sym})).list[0];
    if (!t) throw new BybitError(`Нет цены для ${sym}`);
    const last = Number(t.lastPrice), bid = Number(t.bid1Price || 0) || last, ask = Number(t.ask1Price || 0) || last;
    return [last, bid, ask];
  }
  async instrument(sym) {
    const r = await this.req("GET", "/v5/market/instruments-info", {category: this.cat, symbol: sym});
    if (!r.list || !r.list.length) throw new BybitError(`Пара ${sym} не найдена (${this.cat === "spot" ? "спот" : "фьючерсы USDT"})`);
    return r.list[0];
  }
  async balances() {
    const r = await this.req("GET", "/v5/account/wallet-balance", {accountType: "UNIFIED"}, true);
    const out = {};
    for (const acc of r.list || []) {
      for (const c of acc.coin || []) {
        const bal = Number(c.walletBalance || 0), locked = Number(c.locked || 0);
        out[c.coin] = {total: bal, free: bal - locked};
      }
      out._margin = Number(acc.totalAvailableBalance || 0);
    }
    return out;
  }
  fq(x) { return typeof x === "string" ? x : Number(x).toFixed(this.qd); }
  fp(x) { return typeof x === "string" ? x : Number(x).toFixed(this.pd); }
  async place(sym, side, qty, price = null, o = {}) {
    const body = {category: this.cat, symbol: sym, side, orderLinkId: "grid" + uid()};
    if (this.cat === "linear") body.positionIdx = 0;
    if (o.reduce_only) body.reduceOnly = true;
    if (o.market_quote != null) Object.assign(body, {orderType: "Market", qty: Number(o.market_quote).toFixed(this.qqd), marketUnit: "quoteCoin"});
    else if (o.market) { Object.assign(body, {orderType: "Market", qty: this.fq(qty)}); if (this.cat === "spot") body.marketUnit = "baseCoin"; }
    else Object.assign(body, {orderType: "Limit", qty: this.fq(qty), price: this.fp(price), timeInForce: o.post_only === false ? "GTC" : "PostOnly"});
    return (await this.req("POST", "/v5/order/create", body, true)).orderId;
  }
  async openIds(sym) {
    const ids = new Set(); let cursor = "";
    for (;;) {
      const p = {category: this.cat, symbol: sym, limit: 50}; if (cursor) p.cursor = cursor;
      const r = await this.req("GET", "/v5/order/realtime", p, true);
      (r.list || []).forEach(o => ids.add(o.orderId));
      cursor = r.nextPageCursor || "";
      if (!cursor || !(r.list || []).length) return ids;
    }
  }
  async order(sym, oid) { const r = await this.req("GET", "/v5/order/history", {category: this.cat, symbol: sym, orderId: oid}, true);
    return r.list && r.list.length ? r.list[0] : null; }
  async setLeverage(sym, lev) {
    try { await this.req("POST", "/v5/position/switch-mode", {category: "linear", symbol: sym, mode: 0}, true); } catch (e) {}
    try { await this.req("POST", "/v5/position/set-leverage", {category: "linear", symbol: sym, buyLeverage: String(lev), sellLeverage: String(lev)}, true); }
    catch (e) { if (!String(e.message).includes("110043")) throw e; }
  }
  async position(sym) { const r = await this.req("GET", "/v5/position/list", {category: "linear", symbol: sym}, true);
    for (const p of r.list || []) if (Number(p.size || 0) > 0) return p; return null; }
  async cancel(sym, oid) { await this.req("POST", "/v5/order/cancel", {category: this.cat, symbol: sym, orderId: oid}, true); }
  async cancelAll(sym) { await this.req("POST", "/v5/order/cancel-all", {category: this.cat, symbol: sym}, true); }
}

// ---------- радар: лента, стакан, скорость цены, открытый интерес, фандинг ----------
class Radar {
  constructor(network, symbol) {
    this.pub = new Bybit("", "", network === "demo" ? "mainnet" : network, "linear");
    this.sym = symbol; this.cat = "linear";
    this.trades = []; this.seen = new Set(); this.seenQ = [];
    this.prices = []; this.moves = []; this.books = [];
    this.oi_chg = 0; this.funding = 0; this.n = 0; this.dump = 0; this.pump = 0; this.m = {warm: false};
  }
  async init() { try { await this.pub.req("GET", "/v5/market/tickers", {category: "linear", symbol: this.sym}); }
    catch (e) { this.cat = this.pub.cat = "spot"; } }
  async update() {
    const now = Date.now();
    const r = await this.pub.req("GET", "/v5/market/recent-trade", {category: this.cat, symbol: this.sym, limit: this.cat === "linear" ? 1000 : 60});
    for (const t of [...(r.list || [])].reverse()) {
      if (this.seen.has(t.execId)) continue;
      this.seen.add(t.execId); this.seenQ.push(t.execId);
      const pr = Number(t.price); this.trades.push([Number(t.time), t.side, Number(t.size) * pr, pr]);
    }
    while (this.seenQ.length > 30000) this.seen.delete(this.seenQ.shift());
    while (this.trades.length && this.trades[0][0] < now - 12 * 60000) this.trades.shift();
    if (!this.trades.length) return this.m;
    const price = this.trades[this.trades.length - 1][3];
    this.prices.push([now, price]);
    while (this.prices.length && this.prices[0][0] < now - 15 * 60000) this.prices.shift();

    const ob = await this.pub.req("GET", "/v5/market/orderbook", {category: this.cat, symbol: this.sym, limit: 50});
    const bids = (ob.b || []).map(([p, x]) => [Number(p), Number(x)]), asks = (ob.a || []).map(([p, x]) => [Number(p), Number(x)]);
    let bid_d = 0, ask_d = 0;
    if (bids.length && asks.length) {
      const mid = (bids[0][0] + asks[0][0]) / 2;
      bids.forEach(([p, x]) => { if (p >= mid * 0.995) bid_d += p * x; });
      asks.forEach(([p, x]) => { if (p <= mid * 1.005) ask_d += p * x; });
    }
    const prevBooks = [...this.books];
    this.books.push([bid_d, ask_d]); if (this.books.length > 40) this.books.shift();

    if (this.cat === "linear" && this.n % 20 === 0) {
      try {
        const oi = (await this.pub.req("GET", "/v5/market/open-interest", {category: "linear", symbol: this.sym, intervalTime: "5min", limit: 3})).list || [];
        if (oi.length >= 3 && Number(oi[2].openInterest)) this.oi_chg = (Number(oi[0].openInterest) / Number(oi[2].openInterest) - 1) * 100;
        const tk = await this.pub.req("GET", "/v5/market/tickers", {category: "linear", symbol: this.sym});
        this.funding = Number(tk.list[0].fundingRate || 0);
      } catch (e) {}
    }
    this.n++;

    const W = 30000;
    const rec = this.trades.filter(x => x[0] >= now - W);
    const base = this.trades.filter(x => now - 11 * 60000 <= x[0] && x[0] < now - W);
    const span = base.length ? now - W - base[0][0] : 0;
    const warm = span >= 120000 && this.moves.length >= 20;
    const vol = rec.reduce((a, x) => a + x[2], 0), buy = rec.reduce((a, x) => a + (x[1] === "Buy" ? x[2] : 0), 0);
    const delta = vol ? (2 * buy - vol) / vol : 0;
    let burst = 1, speed_x = 1;
    if (span > 0) {
      const bv = base.reduce((a, x) => a + x[2], 0) / span * W, bc = base.length / span * W;
      burst = bv ? vol / bv : 1; speed_x = bc ? rec.length / bc : 1;
    }
    const tps = rec.length / (W / 1000);
    const priceAgo = ms => { for (const [t, p] of this.prices) if (t >= now - ms) return p; return price; };
    const p60 = priceAgo(60000), move60 = p60 ? (price / p60 - 1) * 100 : 0;
    this.moves.push(Math.abs(move60)); if (this.moves.length > 300) this.moves.shift();
    const med = this.moves.length ? Math.max(median(this.moves), 0.02) : 0.05;
    const move_x = move60 / med, move5 = (price / priceAgo(5 * 60000) - 1) * 100;
    const imb = bid_d + ask_d ? (bid_d - ask_d) / (bid_d + ask_d) : 0;
    let bid_pull = 0, ask_pull = 0;
    if (prevBooks.length) {
      const ab = prevBooks.reduce((a, x) => a + x[0], 0) / prevBooks.length, aa = prevBooks.reduce((a, x) => a + x[1], 0) / prevBooks.length;
      bid_pull = ab ? 1 - bid_d / ab : 0; ask_pull = aa ? 1 - ask_d / aa : 0;
    }
    const flow = clamp((Math.max(burst, speed_x) - 1.5) / 3);
    const oi_d = move5 < -0.3 ? clamp(Math.abs(this.oi_chg) / 2) : 0, oi_p = move5 > 0.3 ? clamp(Math.abs(this.oi_chg) / 2) : 0;
    let dump = 100 * (0.30 * clamp((-move_x - 2) / 4) + 0.25 * clamp((-delta - 0.2) / 0.5) + 0.20 * flow * (delta < -0.1 ? 1 : 0.3)
      + 0.15 * Math.max(clamp((-imb - 0.2) / 0.5), clamp((bid_pull - 0.25) / 0.5)) + 0.10 * Math.max(oi_d, clamp((this.funding - 0.0003) / 0.0007)));
    let pump = 100 * (0.30 * clamp((move_x - 2) / 4) + 0.25 * clamp((delta - 0.2) / 0.5) + 0.20 * flow * (delta > 0.1 ? 1 : 0.3)
      + 0.15 * Math.max(clamp((imb - 0.2) / 0.5), clamp((ask_pull - 0.25) / 0.5)) + 0.10 * Math.max(oi_p, clamp((-this.funding - 0.0003) / 0.0007)));
    if (!warm) { dump = Math.min(dump, 40); pump = Math.min(pump, 40); }
    this.dump = Math.max(dump, this.dump * 0.8); this.pump = Math.max(pump, this.pump * 0.8);
    const r2 = (x, d) => Math.round(x * 10 ** d) / 10 ** d;
    this.m = {warm, tps: r2(tps, 1), speed_x: r2(speed_x, 2), burst: r2(burst, 2), delta: r2(delta, 3), move60: r2(move60, 3),
      move_x: r2(move_x, 1), imb: r2(imb, 3), bid_pull: r2(bid_pull, 2), ask_pull: r2(ask_pull, 2), oi: r2(this.oi_chg, 2),
      funding: this.funding, src: this.cat, dump: Math.round(this.dump), pump: Math.round(this.pump)};
    return this.m;
  }
}

// ---------- одна сетка ----------
class Grid {
  constructor(gid) { this.gid = gid; this.running = false; this.busy = false; this.done = null; this.reset(); }
  reset() {
    Object.assign(this, {status: "Остановлен", error: "", symbol: "", base_coin: "", quote_coin: "",
      hist: [], fills: [], log: [], levels: [], price: null, profit: 0, trades: 0, orders: {}, bal: {}, network: "",
      cat: "spot", lev: 1, pos: null, cells: {}, kmin: 0, n: 0, _lv: {}, dn_losses: [], trail_wait: "", cap: 0,
      trail_pct: 0.005, down_cap: 0.05, stop_pct: STOP_BELOW, up_at: null, dn_at: null, shift_up: 0, shift_dn: 0,
      trail_pnl: 0, per_min: 0, geo: false, short: false, neutral: false, trail_up: false, trail_down: false,
      up_limit: 0, dn_limit: 0, radar: null, radar_mode: "off", radar_sens: "mid", guard: null, guard_t: 0,
      paused: {}, calm: 0, exit_spot: false, bid: null, ask: null, maker_n: 0, taker_n: 0, fee: SPOT_FEE,
      fee_buf: SPOT_FEE_BUF, min_amt: 0, min_qty: 0, radar_errs: 0});
  }
  say(kind, text, val = null) {
    this.log.unshift({t: hhmmss(), k: kind, text, v: val}); this.log.length = Math.min(this.log.length, 120);
    console.log(hhmmss(), `[сетка ${this.gid} ${this.symbol}]`, kind, text, val ?? "");
  }
  unrealized() {
    if (!(this.running || this.busy)) return 0;
    if (this.cat === "linear") return this.pos ? Number(this.pos.unrealisedPnl || 0) : 0;
    if (!this.price) return 0;
    let u = 0;
    const held = Object.values(this.orders).filter(o => o.side === "Sell")
      .concat(Object.values(this.paused).filter(c => c.side === "Sell").map(c => ({cell: c.cell, qty: c.sell_qty})));
    for (const o of held) u += o.qty * this.price * (1 - this.fee) - (o.cell.cost || 0);
    return u;
  }
  state() {
    const upnl = this.unrealized(), b = {};
    for (const [k, v] of Object.entries(this.bal)) if (k === this.base_coin || k === this.quote_coin) b[k] = v.free;
    if ("_margin" in this.bal) b._margin = this.bal._margin;
    let pos = null;
    if (this.pos) pos = {side: this.pos.side, size: this.pos.size, entry: Number(this.pos.avgPrice || 0),
      liq: Number(this.pos.liqPrice || 0), upnl: Number(this.pos.unrealisedPnl || 0)};
    return {id: this.gid, running: this.running, busy: this.busy, status: this.status, error: this.error,
      symbol: this.symbol, base: this.base_coin, quote: this.quote_coin, network: this.network, price: this.price,
      levels: this.levels, hist: this.hist.slice(-400), fills: this.fills.slice(-100), log: this.log.slice(0, 60),
      profit: this.profit, upnl, total: this.profit + upnl, trades: this.trades, open: Object.keys(this.orders).length, bal: b,
      market: this.cat, leverage: this.lev, pos, direction: this.short ? "sell" : this.neutral ? "neutral" : "both",
      trail: {up: this.trail_up, down: this.trail_down, nup: this.shift_up, ndn: this.shift_dn, pnl: this.trail_pnl,
        wait: this.trail_wait, pct: this.trail_pct * 100, up_at: this.up_at, dn_at: this.dn_at},
      fees: {maker: this.maker_n, taker: this.taker_n},
      radar: this.radar ? Object.assign({}, this.radar.m, {mode: this.radar_mode, guard: this.guard, paused: Object.keys(this.paused).length}) : {mode: "off"}};
  }
  start(cfg) {
    if (this.running || this.busy) return;
    this.reset(); this.running = this.busy = true; this.status = "Запускаю…";
    setAutostart(true, this.gid); wakeLock();
    this.done = this._run(cfg);
  }
  stop(remember = true) {
    if (remember) setAutostart(false, this.gid);
    if (this.running) { this.running = false; this.busy = true; this.status = "Останавливаю…"; }
  }
  async _run(cfg) {
    let api = null;
    try {
      if (!cfg.api_key || !cfg.api_secret) throw new BybitError("Введите API ключ и секрет в настройках");
      const cat = cfg.market === "futures" ? "linear" : "spot";
      api = new Bybit(cfg.api_key, cfg.api_secret, cfg.network, cat);
      await api.syncTime();
      await this._setup(api, cfg);
      this.busy = false; this.status = "Работает";
      let errors = 0, n = 0;
      while (this.running) {
        try {
          await this._tick(api); errors = 0; n++;
          if (n % 10 === 0) this.bal = await api.balances();
        } catch (e) {
          if (!(e instanceof BybitError)) throw e;
          errors++; this.say("err", e.message);
          if (errors >= 10) throw new BybitError("10 ошибок подряд, бот остановлен: " + e.message);
          await sleep(5000); continue;
        }
        for (let i = 0; i < POLL_MS / 250 && this.running; i++) await sleep(250);
      }
    } catch (e) {
      this.error = e.message || String(e); this.say("err", this.error);
      if (!(e instanceof BybitError)) console.error(e);
    } finally {
      if (api && this.symbol) {
        try {
          let coins = Object.values(this.orders).filter(o => o.side === "Sell").reduce((a, o) => a + o.qty, 0);
          coins += Object.values(this.paused).filter(c => c.side === "Sell").reduce((a, c) => a + c.sell_qty, 0);
          await api.cancelAll(this.symbol);
          if (this.exit_spot && this.cat === "spot" && coins >= this.min_qty) {
            await api.place(this.symbol, "Sell", q(coins, this.bstep), null, {market: true});
            this.say("sell", `аварийная продажа ${q(coins, this.bstep)} ${this.base_coin} по рынку`);
          }
          if (this.cat === "linear") { this.say("info", "все ордера бота отменены"); await this._closePosition(api); }
          else this.say("info", "все ордера бота отменены, монеты остались на счёте");
        } catch (e) { this.say("err", "не удалось отменить ордера или закрыть позицию: " + e.message + ". Проверьте счёт на Bybit вручную!"); }
      }
      this.orders = {}; this.running = false; this.busy = false; this.status = this.error ? "Ошибка" : "Остановлен";
      wakeLock();
    }
  }
  lv(k) { if (!(k in this._lv)) this._lv[k] = q(this.lo0 * Math.pow(this.r, k), this.tick); return this._lv[k]; }
  _refreshLevels() { this.levels = []; for (let k = this.kmin; k < this.kmin + this.n; k++) this.levels.push(this.lv(k)); }
  _trailText() {
    const up = this.trail_up ? "вверх" + (this.up_limit ? ` до ${this.up_limit}` : "") : "";
    const dn = this.trail_down ? "вниз" + (this.dn_limit ? ` до ${this.dn_limit}` : "") : "";
    const both = [up, dn].filter(Boolean).join(" и "); return both ? "трейлинг " + both : "без трейлинга";
  }
  _orderOf(k) { for (const [oid, o] of Object.entries(this.orders)) if (o.cell.i === k) return [oid, o]; return [null, null]; }
  _canTrailDown() { return this.trail_down && (!this.dn_limit || this.lv(this.kmin - 1) >= this.dn_limit); }
  _canTrailUp() { return this.trail_up && (!this.up_limit || this.lv(this.kmin + this.n) <= this.up_limit); }
  _shortCanFollow() { return this.trail_up && (!this.dn_limit || this.lv(this.kmin - 1) >= this.dn_limit); }
  _shortCanAgainst() { return this.trail_down && (!this.up_limit || this.lv(this.kmin + this.n) <= this.up_limit); }

  async _setup(api, cfg) {
    const sym = String(cfg.symbol || "").toUpperCase().trim();
    this.cat = api.cat;
    const inst = await api.instrument(sym);
    this.symbol = sym; this.network = cfg.network;
    this.base_coin = inst.baseCoin; this.quote_coin = inst.quoteCoin;
    const lot = inst.lotSizeFilter, pf = inst.priceFilter;
    this.tick = Number(pf.tickSize); api.pd = decOf(pf.tickSize);
    let min_amt;
    if (this.cat === "linear") {
      this.fee = FUT_FEE; this.fee_buf = 0;
      this.bstep = Number(lot.qtyStep); api.qd = decOf(lot.qtyStep);
      min_amt = Number(lot.minNotionalValue || 5);
      const max_lev = Number((inst.leverageFilter || {}).maxLeverage || 100);
      const lev = parseInt(cfg.leverage || 1, 10);
      if (!(lev >= 1 && lev <= 100)) throw new BybitError("Плечо должно быть от 1 до 100");
      if (lev > max_lev) throw new BybitError(`Для ${sym} Bybit даёт максимум ${max_lev}x`);
      this.lev = lev;
    } else {
      this.fee = SPOT_FEE; this.fee_buf = SPOT_FEE_BUF;
      this.bstep = Number(lot.basePrecision); api.qd = decOf(lot.basePrecision);
      this.qstep = Number(lot.quotePrecision || "0.0001"); api.qqd = Math.min(decOf(lot.quotePrecision || "0.0001"), 8);
      min_amt = Number(lot.minOrderAmt || 0); this.lev = 1;
    }
    const min_qty = Number(lot.minOrderQty || 0);
    let p = await api.price(sym);
    this.price = p; this.hist.push({t: Date.now(), p});
    let n = parseInt(cfg.levels, 10);
    if (!(n >= 3 && n <= 50)) throw new BybitError("Уровней должно быть от 3 до 50");
    const rng = Number(cfg.range_pct) / 100;
    this.short = cfg.direction === "sell"; this.neutral = cfg.direction === "neutral";
    if ((this.short || this.neutral) && this.cat !== "linear") {
      const what = this.short ? "«Только Sell»" : "«Нейтральная»";
      throw new BybitError(`${what} работает на фьючерсах: на споте нельзя продать то, чего нет. Выберите рынок «Фьючерсы USDT».`);
    }
    if (this.neutral && n % 2 === 0) { n += 1; this.say("info", `нейтральная сетка: уровней ${n} (нечётное число — цена на среднем уровне)`); }
    let lo, hi;
    if (this.short) { lo = p * (1 - 0.2 * rng); hi = p * (1 + 1.8 * rng); } else { lo = p * (1 - rng); hi = p * (1 + rng); }
    this.lo0 = lo; this.r = Math.pow(hi / lo, 1 / (n - 1)); this.n = n; this.kmin = 0; this._lv = {};
    const L = []; for (let i = 0; i < n; i++) L.push(this.lv(i));
    this.levels = L;
    this.trail_up = !!cfg.trail_up; this.trail_down = !!cfg.trail_down;
    this.up_limit = Number(cfg.trail_up_limit || 0); this.dn_limit = Number(cfg.trail_down_limit || 0);
    this.radar_mode = cfg.radar || "off"; this.radar_sens = RADAR_SENS[cfg.radar_sens] ? cfg.radar_sens : "mid";
    if (this.radar_mode !== "off") {
      this.radar = new Radar(cfg.network, sym);
      try { await this.radar.init(); } catch (e) { this.say("err", "радар не запустился: " + e.message); this.radar = null; }
    }
    this.bal = await api.balances();
    let cap;
    if (cfg.cap_max) {
      let free;
      if (this.cat === "linear") { free = this.bal._margin || 0; cap = Math.floor(free * CAP_MAX_FUT * 100) / 100; }
      else { free = (this.bal[this.quote_coin] || {}).free || 0; cap = Math.floor(free * CAP_MAX_SPOT * 100) / 100; }
      if (cap <= 0) throw new BybitError(`Свободных ${this.quote_coin} на счёте нет — «весь депозит» не из чего взять.`);
      this.say("info", `весь депозит: свободно ${fnum(free)} ${this.quote_coin}, капитал бота ${cap} ${this.quote_coin}`);
    } else cap = Number(cfg.capital);
    this.cap = cap;
    this.trail_pct = Math.min(Math.max(Number(cfg.trail_pct ?? 2), 0), 50) / 100;
    this.down_cap = Math.max(Number(cfg.trail_down_cap ?? 5), 0) / 100;
    this.stop_pct = Math.min(Math.max(Number(cfg.stop_pct ?? 2), 0.2), 50) / 100;
    const total = cap * this.lev;
    let mult = 1;
    if (cfg.lot_mode === "geo") mult = Math.min(Math.max(Number(cfg.lot_mult || 1.3), 1.01), 3);
    let m;
    if (this.neutral) m = ((n - 1) >> 1) - 1;
    else { m = -1; for (let i = 0; i < n - 1; i++) if (L[i] <= p) m = i; }
    const w = {}, per = {};
    for (let i = 0; i < n - 1; i++) w[i] = Math.pow(mult, i <= m ? m - i : i - m - 1);
    if (this.neutral) {
      let wb = 0, ws = 0; for (const i in w) { if (+i <= m) wb += w[i]; else ws += w[i]; }
      for (const i in w) per[i] = (total / 2) * w[i] / (+i <= m ? wb : ws);
    } else { const sw = Object.values(w).reduce((a, b) => a + b, 0); for (const i in w) per[i] = total * w[i] / sw; }
    this.per_min = Math.min(...Object.values(per)); this.geo = mult > 1;
    if (this.per_min < min_amt * 1.05) {
      throw new BybitError(`Самый маленький лот выходит ${fnum(this.per_min)} ${this.quote_coin}, а минимум ордера на Bybit ${min_amt} ${this.quote_coin}. ` +
        `Увеличьте капитал, уменьшите число уровней${this.geo ? " или множитель лота" : ""}.`);
    }
    if (this.geo) this.say("info", `геометрические лоты ×${mult}: от ${fnum(this.per_min)} до ${fnum(Math.max(...Object.values(per)))} ${this.quote_coin}`);
    this.min_amt = min_amt; this.min_qty = min_qty; this.cells = {};
    for (let i = 0; i < n - 1; i++) {
      const qty = q(per[i] / L[i], this.bstep);
      if (qty < min_qty) {
        throw new BybitError(`Лот ${fnum(per[i])} ${this.quote_coin} меньше минимального объёма Bybit (${min_qty} ${this.base_coin} ≈ ` +
          `${fnum(min_qty * L[i])} ${this.quote_coin}). Увеличьте капитал${this.geo ? ", уменьшите число уровней или множитель лота" : " или уменьшите число уровней"}.`);
      }
      this.cells[i] = {i, qty, cost: qty * L[i] * (1 + this.fee)};
    }
    let sell_cells, buy_cells;
    const cl = Object.values(this.cells);
    if (this.neutral) { sell_cells = cl.filter(c => c.i > m); buy_cells = cl.filter(c => c.i <= m); sell_cells.forEach(c => c.mode = "short"); }
    else { sell_cells = cl.filter(c => L[c.i] > p); buy_cells = cl.filter(c => L[c.i] <= p); }
    const need_base = sell_cells.reduce((a, c) => a + c.qty, 0), need_quote = buy_cells.reduce((a, c) => a + c.qty * L[c.i], 0);
    if (this.cat === "linear") { await this._setupFutures(api, cfg, p, cap, sell_cells, buy_cells, need_base); return; }

    const free_base = (this.bal[this.base_coin] || {}).free || 0, free_quote = (this.bal[this.quote_coin] || {}).free || 0;
    const short = need_base - free_base;
    let buy_qty = 0;
    if (short > 0) {
      buy_qty = q(short * (1 + this.fee_buf * 2), this.bstep);
      if (buy_qty * p < min_amt) buy_qty = q(min_amt * 1.05 / p, this.bstep) + this.bstep;
      buy_qty = q(buy_qty, this.bstep);
    }
    const market_amt = buy_qty * p * 1.003;
    if (free_quote < need_quote + market_amt) {
      throw new BybitError(`Не хватает ${this.quote_coin}: нужно ${fnum(need_quote + market_amt)}, свободно ${fnum(free_quote)}. Пополните счёт или уменьшите капитал.`);
    }
    this.say("info", `сетка ${L[0]} – ${L[L.length - 1]}, ${n} уровней, ${cap} ${this.quote_coin}, ` + this._trailText());
    if (buy_qty > 0) {
      const [got, spent] = await this._chase(api, "Buy", buy_qty);
      this.say("buy", `быстрой лимиткой ${got} ${this.base_coin} на ${fnum(spent)} ${this.quote_coin} для верхних ордеров`);
      if (got > 0) p = spent / got;
    }
    [, this.bid, this.ask] = await api.bbo(sym);
    await this._placeGrid(api, p, sell_cells, buy_cells);
  }
  async _placeGrid(api, p, sell_cells, buy_cells) {
    for (const c of sell_cells) { c.cost = c.qty * p * (1 + this.fee); await this._put(api, c, "Sell", q(c.qty * (1 - this.fee_buf), this.bstep)); }
    for (const c of buy_cells) await this._put(api, c, "Buy", c.qty);
    this.say("info", `выставлено ордеров: ${Object.keys(this.orders).length}`);
  }
  async _setupFutures(api, cfg, p, cap, sell_cells, buy_cells, need_base) {
    this._api = api;
    const sym = this.symbol, L = this.levels, margin = this.bal._margin || 0;
    if (margin < cap) throw new BybitError(`Свободной маржи ${fnum(margin)} USDT, а капитал бота ${cap} USDT. Пополните счёт или уменьшите капитал.`);
    if (await api.position(sym)) throw new BybitError(`По ${sym} уже есть открытая позиция. Закройте её или выберите другую пару.`);
    await api.setLeverage(sym, this.lev);
    const liq_move = 100 / this.lev, rng = Number(cfg.range_pct);
    this.say("info", `фьючерсы, плечо ${this.lev}x, объём сетки ${fnum(cap * this.lev, 0)} USDT, ` + this._trailText());
    if (liq_move < rng * 1.2) this.say("err", `плечо ${this.lev}x: ликвидация примерно при движении на ${liq_move.toFixed(1)}%, а диапазон ±${rng}%. Бот закроет позицию раньше ликвидации.`);
    if (this.neutral) {
      const sb = buy_cells.reduce((a, c) => a + c.qty * L[c.i], 0), ss = sell_cells.reduce((a, c) => a + c.qty * L[c.i + 1], 0);
      this.say("info", `нейтральная сетка без начальной позиции: Buy ${buy_cells.length} шт. на ${fnum(sb, 0)} USDT ниже цены, ` +
        `Sell ${sell_cells.length} шт. на ${fnum(ss, 0)} USDT выше. Зарабатывает на колебаниях в обе стороны`);
      [, this.bid, this.ask] = await api.bbo(sym);
      for (const c of buy_cells) await this._put(api, c, "Buy", c.qty);
      for (const c of sell_cells) await this._put(api, c, "Sell", c.qty);
      this.say("info", `выставлено ордеров: ${Object.keys(this.orders).length}`);
      return;
    }
    if (this.short) {
      this.say("info", `режим «Только Sell»: шорт-лимитки на линиях выше цены, тейки reduce-only, сетка ${this.levels[0]} – ${this.levels[this.levels.length - 1]}`);
      [, this.bid, this.ask] = await api.bbo(sym);
      await this._armShort();
      this.say("info", `выставлено Sell-лимиток: ${Object.keys(this.orders).length}`);
      return;
    }
    if (need_base > 0) {
      const [got, spent] = await this._chase(api, "Buy", q(need_base, this.bstep));
      this.say("buy", `лонг быстрой лимиткой ${got} ${this.base_coin} для верхних ордеров`);
      if (got > 0) p = spent / got;
    }
    [, this.bid, this.ask] = await api.bbo(sym);
    await this._placeGrid(api, p, sell_cells, buy_cells);
  }
  async _closePosition(api) {
    const pos = await api.position(this.symbol);
    if (!pos) { this.say("info", "открытой позиции нет"); return; }
    const side = pos.side === "Buy" ? "Sell" : "Buy";
    await api.place(this.symbol, side, pos.size, null, {market: true, reduce_only: true});
    this.say("info", `позиция ${pos.size} ${this.base_coin} закрыта по рынку, нереализованный PnL был ${fnum(Number(pos.unrealisedPnl || 0))} USDT`);
    this.pos = null;
  }
  _downDayLimit(now) {
    if (!this.down_cap || !this.cap) return false;
    this.dn_losses = this.dn_losses.filter(([t]) => now - t < 86400);
    const lost = -this.dn_losses.reduce((a, [, x]) => a + x, 0);
    if (lost >= this.cap * this.down_cap) { this.trail_wait = `↓ лимит: сдвиги вниз за сутки уже −${fnum(lost)} ${this.quote_coin}`; return true; }
    return false;
  }
  async _trailShort(api, p) {
    let top = this.lv(this.kmin + this.n - 1), bot = this.lv(this.kmin);
    this.dn_at = this.trail_up ? bot * (1 - this.trail_pct) : null; this.up_at = this.trail_down ? top * (1 + this.trail_pct) : null;
    this.trail_wait = ""; let steps = 0;
    if (this._shortCanFollow() && p < bot * (1 - this.trail_pct)) {
      while (steps < TRAIL_MAX_STEPS && this._shortCanFollow() && p < this.lv(this.kmin)) { if (!(await this._shiftShortDown(api))) break; steps++; }
    } else if (this._shortCanAgainst() && p > top * (1 + this.trail_pct)) {
      while (steps < TRAIL_MAX_STEPS && this._shortCanAgainst() && p > this.lv(this.kmin + this.n - 1)) {
        if (this._downDayLimit(Date.now() / 1000) || !(await this._shiftShortUp(api, p))) break; steps++; }
    }
    if (steps) { this._refreshLevels(); top = this.lv(this.kmin + this.n - 1); bot = this.lv(this.kmin);
      this.dn_at = this.trail_up ? bot * (1 - this.trail_pct) : null; this.up_at = this.trail_down ? top * (1 + this.trail_pct) : null; }
  }
  async _shiftShortDown(api) {
    const ktop = this.kmin + this.n - 2, c = this.cells[ktop];
    if (!c || c.pos) return false;
    const [oid, o] = this._orderOf(ktop);
    if (o) { try { await api.cancel(this.symbol, oid); } catch (e) { return false; } }
    if (oid) delete this.orders[oid];
    delete this.cells[ktop]; delete this.paused[ktop]; this.kmin -= 1;
    const k = this.kmin, qty = this.geo ? q(this.per_min / this.lv(k), this.bstep) : c.qty;
    this.cells[k] = {i: k, qty, cost: 0};
    await this._armShort(); this.shift_dn++;
    this.say("info", `трейлинг ↓ шорт-сетка сдвинута за ценой: ${this.lv(this.kmin)} – ${this.lv(this.kmin + this.n - 1)}`);
    return true;
  }
  async _shiftShortUp(api, p) {
    const kb = this.kmin, c = this.cells[kb];
    if (!c) return false;
    const [oid, o] = this._orderOf(kb);
    if (o) { try { await api.cancel(this.symbol, oid); } catch (e) { return false; } }
    if (oid) delete this.orders[oid];
    delete this.cells[kb]; delete this.paused[kb]; this.kmin += 1;
    let loss = 0;
    if (c.pos) {
      const [got, money] = await this._chase(api, "Buy", c.pos, true);
      loss = got ? (c.entry ?? money) - money * (1 + this.fee) : 0;
      this.trail_pnl += loss; this.profit += loss; this.dn_losses.push([Date.now() / 1000, loss]);
    }
    const k = this.kmin + this.n - 2, qty = this.geo ? q(this.per_min / this.lv(k), this.bstep) : c.qty;
    this.cells[k] = {i: k, qty, cost: 0};
    await this._armShort(); this.shift_up++;
    this.say("info", `трейлинг ↑ шорт-сетка сдвинута против шорта: ${this.lv(this.kmin)} – ${this.lv(this.kmin + this.n - 1)}`, Math.round(loss * 1e4) / 1e4);
    return true;
  }
  async _trail(api, p) {
    if (this.short) return this._trailShort(api, p);
    if (this.neutral) { this.up_at = this.dn_at = null; return; }
    let top = this.lv(this.kmin + this.n - 1), bot = this.lv(this.kmin);
    this.up_at = this.trail_up ? top * (1 + this.trail_pct) : null; this.dn_at = this.trail_down ? bot * (1 - this.trail_pct) : null;
    this.trail_wait = ""; let steps = 0;
    if (this._canTrailUp() && p > top * (1 + this.trail_pct)) {
      while (steps < TRAIL_MAX_STEPS && this._canTrailUp() && p > this.lv(this.kmin + this.n - 1)) { if (!(await this._shiftUp(api, p))) break; steps++; }
    } else if (this._canTrailDown() && p < bot * (1 - this.trail_pct)) {
      while (steps < TRAIL_MAX_STEPS && this._canTrailDown() && p < this.lv(this.kmin)) {
        if (this._downDayLimit(Date.now() / 1000) || !(await this._shiftDown(api, p))) break; steps++; }
    }
    if (steps) { this._refreshLevels(); top = this.lv(this.kmin + this.n - 1); bot = this.lv(this.kmin);
      this.up_at = this.trail_up ? top * (1 + this.trail_pct) : null; this.dn_at = this.trail_down ? bot * (1 - this.trail_pct) : null; }
  }
  async _shiftUp(api, p) {
    const [oid, o] = this._orderOf(this.kmin);
    if (!o || o.side !== "Buy") return false;
    if (this.cat === "spot" && q(o.qty * o.price / p * (1 - this.fee_buf), this.bstep) < this.min_qty) {
      this.say("err", "сдвиг вверх: объём ячейки меньше минимума биржи, трейлинг вверх выключен"); this.trail_up = false; return false; }
    try { await api.cancel(this.symbol, oid); } catch (e) { return false; }
    delete this.orders[oid]; delete this.cells[this.kmin]; this.kmin += 1;
    const freed = o.qty * o.price, k = this.kmin + this.n - 2;
    const want = this.geo ? q(this.per_min / p * (1 - this.fee_buf), this.bstep)
      : (this.cat === "linear" ? o.qty : q(freed / p * (1 - this.fee_buf), this.bstep));
    const [got, spent] = await this._chase(api, "Buy", want);
    if (got <= 0) { this.say("err", "сдвиг вверх: не удалось купить, ячейка вернётся при следующем сдвиге"); return false; }
    const qty = this.cat === "linear" ? got : q(got * (1 - this.fee_buf), this.bstep);
    const cell = {i: k, qty, cost: spent * (1 + this.fee)};
    this.cells[k] = cell; await this._put(api, cell, "Sell", qty); this.shift_up++;
    this.fills.push({t: Date.now(), p, k: "buy"});
    this.say("info", `трейлинг ↑ сетка сдвинута: ${this.lv(this.kmin)} – ${this.lv(this.kmin + this.n - 1)}`);
    return true;
  }
  async _shiftDown(api, p) {
    const ktop = this.kmin + this.n - 2, [oid, o] = this._orderOf(ktop);
    if (!o || o.side !== "Sell") return false;
    if (this.cat === "spot") {
      const est = q(o.qty * p * (1 - this.fee) / this.lv(this.kmin - 1), this.bstep);
      if (est < this.min_qty || o.qty * p < this.min_amt) { this.say("err", "сдвиг вниз: объём ячейки меньше минимума биржи, трейлинг вниз выключен"); this.trail_down = false; return false; }
    }
    try { await api.cancel(this.symbol, oid); } catch (e) { return false; }
    delete this.orders[oid]; let cell = this.cells[ktop]; delete this.cells[ktop];
    const [sold, money] = await this._chase(api, "Sell", o.qty, this.cat === "linear");
    const got = money * (1 - this.fee), loss = sold ? got - cell.cost * sold / cell.qty : 0;
    this.trail_pnl += loss; this.dn_losses.push([Date.now() / 1000, loss]); this.profit += loss;
    this.kmin -= 1; const k = this.kmin;
    const qty = this.geo ? q(this.per_min / this.lv(k), this.bstep) : (this.cat === "linear" ? cell.qty : q(got / this.lv(k), this.bstep));
    cell = {i: k, qty, cost: qty * this.lv(k) * (1 + this.fee)};
    this.cells[k] = cell; await this._put(api, cell, "Buy", qty); this.shift_dn++;
    this.fills.push({t: Date.now(), p, k: "sell"});
    this.say("info", `трейлинг ↓ сетка сдвинута: ${this.lv(this.kmin)} – ${this.lv(this.kmin + this.n - 1)}`, Math.round(loss * 1e4) / 1e4);
    return true;
  }
  // быстрая лимитка вместо маркета: Post-Only по лучшей цене, переставляется за ценой; остаток через ~15 с — маркетом
  async _chase(api, side, qty, reduce_only = false) {
    const sym = this.symbol; let done = 0, money = 0;
    for (let t = 0; t < CHASE_TRIES; t++) {
      const left = q(qty - done, this.bstep);
      if (left < this.min_qty) break;
      const [, bid, ask] = await api.bbo(sym); const px = side === "Buy" ? bid : ask;
      if (left * px < this.min_amt) break;
      let oid;
      try { oid = await api.place(sym, side, left, px, {reduce_only}); } catch (e) { this.say("err", "быстрая лимитка: " + e.message); break; }
      await sleep(CHASE_WAIT);
      try { await api.cancel(sym, oid); } catch (e) {}
      let h = null;
      for (let j = 0; j < 3; j++) { h = await api.order(sym, oid); if (h && !["New", "PartiallyFilled", "Untriggered"].includes(h.orderStatus)) break; await sleep(300); }
      const f = h ? Number(h.cumExecQty || 0) : 0;
      if (f > 0) { done += f; money += f * (Number(h.avgPrice || 0) || px); this.maker_n++; }
    }
    const left = q(qty - done, this.bstep);
    if (left >= this.min_qty) {
      const [, bid, ask] = await api.bbo(sym); const px = side === "Buy" ? ask : bid;
      if (left * px >= this.min_amt) {
        await api.place(sym, side, left, null, {market: true, reduce_only});
        done += left; money += left * px; this.taker_n++;
        this.say("info", `лимитка не успела, остаток ${left} ${this.base_coin} добран маркетом`);
      }
    }
    return [done, money];
  }
  async _put(api, cell, side, qty) {
    let price = side === "Sell" ? this.lv(cell.i + 1) : this.lv(cell.i);
    if (side === "Buy" && this.ask && price >= this.ask) price = this.bid;
    else if (side === "Sell" && this.bid && price <= this.bid) price = this.ask;
    const oid = await api.place(this.symbol, side, qty, price, {reduce_only: this.short && side === "Buy"});
    this.orders[oid] = {cell, side, qty, price};
  }
  async _armShort() {
    const busy = new Set(Object.values(this.orders).map(o => o.cell.i).concat(Object.keys(this.paused).map(Number)));
    for (const k of Object.keys(this.cells).map(Number).sort((a, b) => a - b)) {
      const c = this.cells[k];
      if (busy.has(k) || c.pos) continue;
      if (this.ask == null || this.lv(k + 1) > this.ask) await this._put(this._api, c, "Sell", c.qty);
    }
  }
  async _tick(api) {
    this._api = api;
    const [p, bid, ask] = await api.bbo(this.symbol); this.bid = bid; this.ask = ask;
    this.price = p; this.hist.push({t: Date.now(), p}); if (this.hist.length > 1500) this.hist.splice(0, this.hist.length - 1500);
    const pct = x => +(x * 100).toFixed(4);
    if (this.short) {
      if (!this._shortCanAgainst() && p > this.lv(this.kmin + this.n - 1) * (1 + this.stop_pct)) {
        this.error = `Цена ${p} ушла выше шорт-сетки более чем на ${pct(this.stop_pct)}%. Позиция закрыта, бот остановлен для защиты.`;
        this.running = false; return; }
    } else if (this.neutral && (p > this.lv(this.kmin + this.n - 1) * (1 + this.stop_pct) || p < this.lv(this.kmin) * (1 - this.stop_pct))) {
      this.error = `Цена ${p} ушла за нейтральную сетку более чем на ${pct(this.stop_pct)}%. Позиция закрыта, бот остановлен для защиты.`;
      this.running = false; return;
    } else if (!this.neutral && !this._canTrailDown() && p < this.lv(this.kmin) * (1 - this.stop_pct)) {
      const why = this.trail_down ? "достигнут предел трейлинга вниз, " : "";
      this.error = `Цена ${p}: ${why}ушла ниже сетки более чем на ${pct(this.stop_pct)}%. Бот остановлен для защиты.`;
      this.running = false; return;
    }
    if (this.cat === "linear") {
      this.pos = await api.position(this.symbol);
      const liq = this.pos ? Number(this.pos.liqPrice || 0) : 0;
      if (liq > 0) {
        const entry = Number(this.pos.avgPrice || p), long_ = this.pos.side === "Buy";
        const left = long_ && entry !== liq ? (p - liq) / (entry - liq) : (!long_ && liq !== entry ? (liq - p) / (liq - entry) : 1);
        if (left < LIQ_GUARD) { this.error = `Цена ${p} близко к ликвидации (${liq}). Позиция закрыта, бот остановлен.`; this.running = false; return; }
      }
    }
    const open = await api.openIds(this.symbol);
    for (const [oid, o] of Object.entries(this.orders)) {
      if (open.has(oid)) continue;
      const h = await api.order(this.symbol, oid);
      if (!h) continue;
      const st = h.orderStatus;
      if (!["Filled", "Cancelled", "Rejected", "Deactivated", "PartiallyFilledCanceled"].includes(st)) continue;
      delete this.orders[oid];
      const c = o.cell, filled = Number(h.cumExecQty || 0), avg = Number(h.avgPrice || 0) || o.price;
      if (st === "Filled" || (st === "PartiallyFilledCanceled" && filled > 0 && o.side === "Sell")) await this._onFill(api, c, o, filled, avg);
      else {
        if (!String((h.rejectReason || "") + (h.cancelType || "")).includes("PostOnly")) this.say("info", `ордер ${o.side} ${o.price} отменён вне бота, выставляю заново`);
        if (c.i in this.cells) await this._put(api, c, o.side, o.qty);
      }
    }
    if (this.short && this.guard !== "pump") await this._armShort();
    if (this.radar && (await this._guard(api, p)) === "exit") return;
    if (!this.guard) await this._trail(api, p);
  }
  async _guard(api, p) {
    let m;
    try { m = await this.radar.update(); }
    catch (e) { this.radar_errs++; if ([1, 20].includes(this.radar_errs) || this.radar_errs % 200 === 0) this.say("err", `радар без данных (${this.radar_errs} раз): ${e.message}`); return; }
    this.radar_errs = 0;
    if (!m.warm) return;
    const [alert, calm] = RADAR_SENS[this.radar_sens];
    const sg = (x, d) => (x >= 0 ? "+" : "") + x.toFixed(d);
    const why = () => `принты ×${m.speed_x}, объём ×${m.burst}, дельта ${sg(m.delta, 2)}, цена 60с ${sg(m.move60, 2)}%, стакан ${sg(m.imb, 2)}`;
    if (this.guard == null) {
      if (m.dump >= alert && !this.short) await this._pause(api, "dump", "Buy", `ранний сигнал ДАМПА (${m.dump}): ` + why());
      else if (m.pump >= alert) await this._pause(api, "pump", "Sell", `ранний сигнал ПАМПА (${m.pump}): ` + why());
      return;
    }
    if (this.guard === "pump" && this.short && this.radar_mode === "exit" && m.pump >= RADAR_EXIT) {
      this.error = `Аварийный выход: сильный памп против шорта (${m.pump}). ` + why(); this.running = false; return "exit"; }
    if (this.guard === "dump" && this.radar_mode === "exit" && m.dump >= RADAR_EXIT) {
      this.error = `Аварийный выход: сильный дамп (${m.dump}). ` + why(); this.exit_spot = true; this.running = false; return "exit"; }
    const score = this.guard === "dump" ? m.dump : m.pump;
    this.calm = score < calm ? this.calm + 1 : 0;
    if (this.calm >= RADAR_CALM_TICKS) await this._resume(api, "рынок успокоился");
    else if (Date.now() / 1000 - this.guard_t > RADAR_MAX_PAUSE) await this._resume(api, "пауза длится 30 минут");
  }
  async _pause(api, kind, side, text) {
    let n = 0;
    for (const [oid, o] of Object.entries(this.orders)) {
      if (o.side !== side) continue;
      try { await api.cancel(this.symbol, oid); } catch (e) { continue; }
      delete this.orders[oid]; this.paused[o.cell.i] = {cell: o.cell, side, sell_qty: o.qty}; n++;
    }
    this.guard = kind; this.guard_t = Date.now() / 1000; this.calm = 0;
    this.say("err", `${text}. Сняты ${side === "Buy" ? "покупки" : "продажи"}: ${n}`);
  }
  async _resume(api, why) {
    if (this.short) {
      this.paused = {}; const before = Object.keys(this.orders).length; await this._armShort();
      this.say("info", `${why}, защита снята, вернул Sell-лимиток: ${Object.keys(this.orders).length - before}`);
      this.guard = null; this.calm = 0; return;
    }
    let n = 0;
    for (const k of Object.keys(this.paused).map(Number).sort((a, b) => a - b)) {
      const c = this.paused[k]; if (!(k in this.cells)) continue;
      await this._put(api, c.cell, c.side, c.sell_qty); n++;
    }
    this.paused = {}; this.say("info", `${why}, защита снята, вернул ордеров: ${n}`); this.guard = null; this.calm = 0;
  }
  async _onFill(api, c, o, filled, avg) {
    this.trades++; this.maker_n++; const t = Date.now(), r4 = x => Math.round(x * 1e4) / 1e4;
    if (this.short) {
      if (o.side === "Sell") { c.pos = filled; c.entry = filled * avg * (1 - this.fee);
        this.fills.push({t, p: avg, k: "sell"}); this.say("sell", `шорт ${filled} ${this.base_coin} по ${avg}`); await this._put(api, c, "Buy", filled); }
      else { const pr = (c.entry ?? filled * avg) - filled * avg * (1 + this.fee); c.pos = 0; this.profit += pr;
        this.fills.push({t, p: avg, k: "buy"}); this.say("buy", `тейк ${filled} ${this.base_coin} по ${avg}`, r4(pr));
        if (c.i in this.cells && (this.ask == null || this.lv(c.i + 1) > this.ask)) await this._put(api, c, "Sell", c.qty); }
      return;
    }
    if (c.mode === "short") {
      if (o.side === "Sell") { c.entry = filled * avg * (1 - this.fee); this.fills.push({t, p: avg, k: "sell"});
        this.say("sell", `шорт ${filled} ${this.base_coin} по ${avg}`); await this._put(api, c, "Buy", filled); }
      else { const pr = (c.entry ?? filled * avg) - filled * avg * (1 + this.fee); this.profit += pr; this.fills.push({t, p: avg, k: "buy"});
        this.say("buy", `тейк шорта ${filled} ${this.base_coin} по ${avg}`, r4(pr)); await this._put(api, c, "Sell", c.qty); }
      return;
    }
    if (o.side === "Buy") { c.cost = filled * avg * (1 + this.fee); this.fills.push({t, p: avg, k: "buy"});
      this.say("buy", `${filled} ${this.base_coin} по ${avg}`); await this._put(api, c, "Sell", q(filled * (1 - this.fee_buf), this.bstep)); }
    else { const got = filled * avg * (1 - this.fee), pr = got - c.cost; this.profit += pr; this.fills.push({t, p: avg, k: "sell"});
      this.say("sell", `${filled} ${this.base_coin} по ${avg}`, r4(pr)); await this._put(api, c, "Buy", c.qty); }
  }
}

// ---------- поиск самых волатильных монет за 30 минут ----------
const STABLES = new Set(["USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDD", "PYUSD", "BUSD", "EUR", "USDR", "USD1", "RLUSD"]);
const scanCache = {};
async function scanVolatile(market = "spot", minutes = 30) {
  const cat = market === "futures" || market === "linear" ? "linear" : "spot", key = cat + minutes;
  if (scanCache[key] && Date.now() - scanCache[key][0] < 20000) return scanCache[key][1];
  const pub = new Bybit("", "", "mainnet", cat);
  const tick = (await pub.req("GET", "/v5/market/tickers", {category: cat})).list || [];
  const pool = tick.filter(t => (t.symbol || "").endsWith("USDT") && !STABLES.has(t.symbol.slice(0, -4)) && !t.symbol.includes("-")
    && Number(t.turnover24h || 0) >= 3e6).sort((a, b) => Number(b.turnover24h) - Number(a.turnover24h)).slice(0, 80).map(t => t.symbol);
  const turnover = Object.fromEntries(tick.map(t => [t.symbol, Number(t.turnover24h || 0)]));
  const one = async sym => {
    let k; try { k = (await pub.req("GET", "/v5/market/kline", {category: cat, symbol: sym, interval: "1", limit: minutes})).list || []; } catch (e) { return null; }
    if (k.length < minutes * 0.8) return null;
    k = k.map(x => [Number(x[1]), Number(x[2]), Number(x[3]), Number(x[4]), Number(x[6])]).reverse();
    const o0 = k[0][0], close = k[k.length - 1][3], hi = Math.max(...k.map(x => x[1])), lo = Math.min(...k.map(x => x[2]));
    if (lo <= 0 || o0 <= 0) return null;
    const rng = (hi / lo - 1) * 100, candle = k.reduce((a, x) => a + (x[1] / x[2] - 1) * 100, 0) / k.length, chg = (close / o0 - 1) * 100;
    const path = k.reduce((a, x) => a + Math.abs(x[3] / x[0] - 1) * 100, 0), chop = Math.abs(chg) > 0.05 ? path / Math.abs(chg) : path / 0.05;
    const turn30 = k.reduce((a, x) => a + x[4], 0), trend = Math.abs(chg) >= rng * 0.7;
    const kind = trend && rng >= 2 ? (chg > 0 ? "памп" : "дамп") : (trend ? "тренд" : "пила");
    const r = (x, d) => Math.round(x * 10 ** d) / 10 ** d;
    return {symbol: sym, range: r(rng, 2), candle: r(candle, 3), change: r(chg, 2), chop: r(Math.min(chop, 99), 1), turn30: Math.round(turn30),
      turn24: Math.round(turnover[sym] || 0), price: close, kind, grid_ok: kind === "пила" && rng >= 1, sugg_range: rng >= 1.5 ? 5 : 3, sugg_levels: 4};
  };
  const rows = []; let i = 0;
  await Promise.all(Array.from({length: 10}, async () => { while (i < pool.length) { const r = await one(pool[i++]); if (r) rows.push(r); } }));
  rows.sort((a, b) => b.range - a.range);
  const res = {market: cat === "linear" ? "futures" : "spot", minutes, checked: pool.length, t: hhmmss(), rows: rows.slice(0, 20)};
  scanCache[key] = [Date.now(), res]; return res;
}

// ---------- все сетки ----------
const BOTS = {};
const bot = gid => BOTS[gid] || (BOTS[gid] = new Grid(gid));
function gridsSummary() {
  const st = loadStore();
  return Object.keys(st.grids).sort((a, b) => a - b).map(gid => {
    const g = st.grids[gid], b = BOTS[gid], up = b ? b.unrealized() : 0, live = b && (b.running || b.busy || b.symbol);
    return {id: gid, symbol: String(live && b.symbol ? b.symbol : g.symbol || "").toUpperCase(), market: g.market || "spot",
      leverage: g.leverage || 1, direction: g.direction || "both", running: !!(b && b.running), busy: !!(b && b.busy),
      status: b ? b.status : "Остановлен", error: b ? b.error : "", profit: b ? b.profit : 0, trades: b ? b.trades : 0,
      upnl: up, total: (b ? b.profit : 0) + up};
  });
}
async function freeFunds(market) {
  const st = loadStore();
  if (!st.api_key || !st.api_secret) throw new BybitError("Введите API ключ и секрет в настройках");
  const api = new Bybit(st.api_key, st.api_secret, st.network, market === "futures" ? "linear" : "spot");
  await api.syncTime(); const bal = await api.balances();
  if (market === "futures") return [api, bal._margin || 0, CAP_MAX_FUT];
  return [api, (bal.USDT || {}).free || 0, CAP_MAX_SPOT];
}
async function splitStart(body) {
  const market = body.market !== "spot" ? "futures" : "spot", cat = market === "futures" ? "linear" : "spot";
  const lev = market === "futures" ? parseInt(body.leverage || 3, 10) : 1;
  if (!(lev >= 1 && lev <= 100)) throw new BybitError("Плечо должно быть от 1 до 100");
  const sugg = body.sugg || {}, syms = [];
  for (let x of body.symbols || []) { x = String(x).toUpperCase().trim().replace("/", ""); if (x && !x.endsWith("USDT")) x += "USDT"; if (x && !syms.includes(x)) syms.push(x); }
  if (!syms.length) throw new BybitError("Выберите хотя бы одну монету");
  const busy = new Set(Object.values(BOTS).filter(b => (b.running || b.busy) && b.cat === cat).map(b => b.symbol));
  const taken = syms.filter(x => busy.has(x));
  if (taken.length) throw new BybitError("Уже торгуются: " + taken.join(", ") + ". Уберите их из списка или остановите эти сетки.");
  const [api, free, k] = await freeFunds(market);
  const bad = [];
  for (const x of syms) { try { await api.instrument(x); } catch (e) { bad.push(x); } }
  if (bad.length) throw new BybitError("На Bybit нет такой пары (" + (cat === "linear" ? "фьючерсы" : "спот") + "): " + bad.join(", "));
  const per = Math.floor(free * k / syms.length * 100) / 100;
  if (per < 10) throw new BybitError(`Свободно ${fnum(free)} USDT: на ${syms.length} сеток выходит по ${per} USDT, это слишком мало. Уберите часть монет или пополните счёт.`);
  const st = loadStore(), tpl = st.grids[String(body.copy_from || "")] || {}, ids = [];
  for (const x of syms) {
    let gid = Object.keys(st.grids).find(g => String(st.grids[g].symbol || "").toUpperCase() === x && st.grids[g].market === market && !(BOTS[g] && (BOTS[g].running || BOTS[g].busy)));
    if (!gid) { gid = newGridId(st); st.grids[gid] = freshGrid(tpl); }
    const g = st.grids[gid];
    Object.assign(g, {symbol: x, market, leverage: lev, capital: per, cap_max: false, direction: market === "futures" ? "neutral" : "both"});
    const sg = sugg[x] || {}; if (sg.range) g.range_pct = sg.range; if (sg.levels) g.levels = sg.levels;
    ids.push(gid);
  }
  saveStore(st);
  for (const gid of ids) bot(gid).start(gridCfg(gid));
  return {ok: true, ids, per, free, leverage: lev};
}

// ---------- проверка связи с Bybit ----------
async function checkLink() {
  const st = loadStore(), out = {};
  const pub = new Bybit("", "", st.network, "linear");
  try { await pub.syncTime(); out.pub = "ok"; } catch (e) { out.pub = e.message; }
  if (!st.api_key || !st.api_secret) { out.key = "Ключ не введён"; return out; }
  const api = new Bybit(st.api_key, st.api_secret, st.network, "spot");
  try { await api.syncTime(); const b = await api.balances(); out.key = "ok"; out.usdt = (b.USDT || {}).free || 0; out.margin = b._margin || 0; }
  catch (e) { out.key = e.message; }
  return out;
}

// ---------- «сервер» внутри страницы: те же адреса /api/..., что у версии на Railway ----------
const J = (status, body) => new Response(JSON.stringify(body), {status, headers: {"Content-Type": "application/json"}});
async function route(path, method, body) {
  const u = new URL(path, "http://local"), p = u.pathname, qs = u.searchParams;
  try {
    if (method === "GET") {
      if (p === "/api/config") { const st = loadStore(); st.has_secret = !!st.api_secret; delete st.api_secret; st.env_keys = false; st.locked = false; return J(200, st); }
      if (p === "/api/state") { const gid = qs.get("id") || "1"; if (!(gid in loadStore().grids)) return J(404, {error: "нет такой сетки"}); return J(200, bot(gid).state()); }
      if (p === "/api/grids") return J(200, gridsSummary());
      if (p === "/api/scan") return J(200, await scanVolatile(qs.get("market") || "spot"));
      if (p === "/api/funds") { const [, free, k] = await freeFunds(qs.get("market") || "futures"); return J(200, {free, k}); }
      if (p === "/api/check") return J(200, await checkLink());
      return J(404, {error: "not found"});
    }
    if (["/api/start", "/api/save", "/api/new"].includes(p)) {
      const st = loadStore(); let gid = String(body.id || "");
      if (p === "/api/save" && !(gid in st.grids)) return J(404, {error: "нет такой сетки"});
      if (p === "/api/new" || !(gid in st.grids)) { gid = newGridId(st); st.grids[gid] = freshGrid(st.grids[String(body.copy_from || "")] || {}); }
      const g = st.grids[gid];
      for (const k of Object.keys(DEFAULT_CFG)) {
        const v = body[k]; if (v == null || v === "" || (k === "api_secret" && v === "********")) continue;
        if (GLOBAL_KEYS.includes(k)) st[k] = v; else g[k] = v;
      }
      if (p === "/api/start") {
        const sym = String(g.symbol || "").toUpperCase().trim(), cat = g.market === "futures" ? "linear" : "spot";
        for (const [og, ob] of Object.entries(BOTS)) {
          if (og !== gid && (ob.running || ob.busy) && ob.symbol === sym && ob.cat === cat)
            return J(409, {error: `${sym} уже торгует сетка ${og}. Две сетки на одной паре мешают друг другу: выберите другую монету.`, id: gid});
        }
      }
      saveStore(st);
      if (p === "/api/start") bot(gid).start(gridCfg(gid));
      return J(200, {ok: true, id: gid});
    }
    if (p === "/api/split") { try { return J(200, await splitStart(body)); } catch (e) { return J(400, {error: e.message}); } }
    if (p === "/api/stop_all") { let n = 0; for (const b of Object.values(BOTS)) if (b.running) { b.stop(); n++; } return J(200, {ok: true, stopped: n}); }
    if (p === "/api/stop") { const gid = String(body.id || "1"); if (BOTS[gid]) BOTS[gid].stop(); return J(200, {ok: true}); }
    if (p === "/api/delete") {
      const gid = String(body.id || ""), b = BOTS[gid];
      if (b && (b.running || b.busy)) return J(409, {error: "Сначала остановите сетку"});
      const st = loadStore(); delete st.grids[gid]; if (!Object.keys(st.grids).length) st.grids["1"] = freshGrid(); saveStore(st);
      delete BOTS[gid]; return J(200, {ok: true});
    }
    return J(404, {error: "not found"});
  } catch (e) {
    return J(e instanceof BybitError ? 502 : 500, {error: e.message || String(e)});
  }
}
window.fetch = (input, init = {}) => {
  const url = typeof input === "string" ? input : input.url;
  if (url.startsWith("/api/")) {
    let body = {}; try { body = init.body ? JSON.parse(init.body) : {}; } catch (e) {}
    return route(url, (init.method || "GET").toUpperCase(), body);
  }
  return _fetch(input, init);
};

// ---------- экран не засыпает, пока работают сетки ----------
let WL = null;
async function wakeLock() {
  const any = Object.values(BOTS).some(b => b.running || b.busy);
  try {
    if (any && !WL && navigator.wakeLock && document.visibilityState === "visible") { WL = await navigator.wakeLock.request("screen"); WL.addEventListener("release", () => WL = null); }
    else if (!any && WL) { await WL.release(); WL = null; }
  } catch (e) {}
}
document.addEventListener("visibilitychange", wakeLock);
window.addEventListener("beforeunload", e => { if (Object.values(BOTS).some(b => b.running || b.busy)) { e.preventDefault(); e.returnValue = ""; } });

// ---------- сетки, которые работали, когда страницу закрыли ----------
window.GRIDPRO = {
  wasRunning() { const st = loadStore(); return Object.keys(st.grids).filter(g => st.grids[g].was_running && !(BOTS[g] && (BOTS[g].running || BOTS[g].busy)))
    .map(g => ({id: g, symbol: st.grids[g].symbol, market: st.grids[g].market})); },
  async resume(restart) {
    const st = loadStore(), list = this.wasRunning(), res = [];
    for (const g of list) {
      const cfg = gridCfg(g.id, st), api = new Bybit(cfg.api_key, cfg.api_secret, cfg.network, cfg.market === "futures" ? "linear" : "spot");
      try {
        await api.syncTime(); await api.cancelAll(String(cfg.symbol).toUpperCase());
        if (!restart && cfg.market === "futures") {
          const pos = await api.position(String(cfg.symbol).toUpperCase());
          if (pos) await api.place(String(cfg.symbol).toUpperCase(), pos.side === "Buy" ? "Sell" : "Buy", pos.size, null, {market: true, reduce_only: true});
        }
        res.push(`${cfg.symbol}: ордера сняты` + (!restart && cfg.market === "futures" ? ", позиция закрыта" : ""));
      } catch (e) { res.push(`${cfg.symbol}: ${e.message}`); }
      if (restart) { bot(g.id).start(cfg); await sleep(800); } else setAutostart(false, g.id);
    }
    return res;
  },
  checkLink
};
})();
