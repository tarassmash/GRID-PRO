#!/usr/bin/env python3
"""
Сеточный бот для Bybit 24/7 (спот и фьючерсы USDT, API v5) — версия «только лимитки».
По рынку не входит и не выходит: единственный выход по рынку — аварийный стоп (цена ушла от цены запуска
на emerg_pct %, по умолчанию 10%, против позиции) и защита у ликвидации.
Локально:  python bot.py  -> откроется страница в браузере.
На Railway: ключи и пароль задаются в Variables (см. README.md и .env.example).
Нужен только Python 3.8+, без сторонних библиотек.
"""
import json, time, hmac, hashlib, threading, os, uuid, webbrowser, statistics, signal, base64, sys
from collections import deque
import urllib.request, urllib.parse, urllib.error, math
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, ROUND_DOWN
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ON_SERVER = bool(os.environ.get("PORT") or os.environ.get("RAILWAY_ENVIRONMENT"))
PORT = int(os.environ.get("PORT") or 8765)
HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR") or (os.path.isdir("/data") and "/data") or HERE
CFG_PATH = os.path.join(DATA_DIR, "gridbot_config.json")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
ENV_KEY, ENV_SECRET = os.environ.get("BYBIT_API_KEY", ""), os.environ.get("BYBIT_API_SECRET", "")
HOSTS = {
    "testnet": "https://api-testnet.bybit.com",
    "demo": "https://api-demo.bybit.com",
    "mainnet": "https://api.bybit.com",
}
DEFAULT_CFG = {"api_key": "", "api_secret": "", "network": "testnet", "market": "spot",
               "leverage": 2, "symbol": "BTCUSDT", "capital": 200, "range_pct": 5, "levels": 4,
               "trail_up": True, "trail_down": True, "trail_up_limit": 0, "trail_down_limit": 0,
               "trail_pct": 0.5, "trail_down_cap": 5, "stop_pct": 2, "emerg_pct": 10,
               "radar": "pause", "radar_sens": "mid", "lot_mode": "geo", "lot_mult": 1.4,
               "direction": "both", "cap_max": False}
CAP_MAX_SPOT = Decimal("0.97")   # «весь депозит» на споте: запас на комиссию и докупку монеты быстрой лимиткой
CAP_MAX_FUT = Decimal("0.95")    # «весь депозит» на фьючерсах: запас маржи на комиссии и просадку
SPOT_FEE = Decimal("0.001")      # комиссия спота Bybit 0.1%
SPOT_FEE_BUF = Decimal("0.0015") # запас: комиссия покупки на споте списывается монетой
FUT_FEE = Decimal("0.0002")      # мейкерская комиссия фьючерсов (бот торгует лимитками Post-Only)
CHASE_TRIES = 6        # быстрая лимитка: до 6 перестановок по лучшей цене стакана…
CHASE_WAIT = 2.5       # …по 2.5 секунды, потом остаток добирается маркетом
LIQ_GUARD = Decimal("0.35")      # аварийный выход, когда до ликвидации осталось 35% пути
STOP_BELOW = Decimal("0.05")  # (не используется: вместо него аварийный стоп от цены запуска)
EMERG_DEFAULT = Decimal("0.10")  # аварийный стоп: цена ушла на 10% от цены запуска против позиции — единственный выход по рынку
SETUP_TRIES = 20       # вход при запуске только лимитками: до 20 перестановок (~50 с), иначе бот не стартует
CLOSE_TRIES = 12       # закрытие позиции при остановке лимитками: до 12 перестановок (~30 с)
POLL_SEC = 3
# Трейлинг по проценту: сетка сдвигается, когда цена ушла за её границу на trail_pct %. Никаких таймеров.
TRAIL_MAX_STEPS = 10      # сколько шагов можно сдвинуть за одну проверку после подтверждения
RADAR_SENS = {"low": (75, 45), "mid": (60, 35), "high": (48, 28)}  # порог тревоги / порог «успокоилось»
RADAR_CALM_TICKS = 20  # ~1 минута спокойствия, прежде чем вернуть ордера
RADAR_MAX_PAUSE = 30 * 60
RADAR_EXIT = 90        # риск дампа, при котором срабатывает аварийный выход (если включён)


def clamp(x):
    return max(0.0, min(1.0, x))


def q(x, step):
    """Округлить вниз до шага биржи."""
    return (Decimal(x) / step).to_integral_value(rounding=ROUND_DOWN) * step


GLOBAL_KEYS = ("api_key", "api_secret", "network")   # один аккаунт Bybit на все сетки
GRID_KEYS = tuple(k for k in DEFAULT_CFG if k not in GLOBAL_KEYS)
CFG_LOCK = threading.RLock()


def load_store():
    """Файл настроек: ключи и сеть общие, у каждой сетки свои настройки."""
    with CFG_LOCK:
        raw = {}
        try:
            with open(CFG_PATH, encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            pass
        st = {k: raw.get(k, DEFAULT_CFG[k]) for k in GLOBAL_KEYS}
        if os.environ.get("BYBIT_ENV") in HOSTS and "network" not in raw:
            st["network"] = os.environ["BYBIT_ENV"]
        grids = raw.get("grids")
        if not isinstance(grids, dict):          # старый файл с одной сеткой
            g = {k: raw[k] for k in GRID_KEYS if k in raw}
            g["was_running"] = bool(raw.get("was_running"))
            grids = {"1": g}
        st["grids"] = {gid: dict({k: DEFAULT_CFG[k] for k in GRID_KEYS}, **g) for gid, g in grids.items()}
        st["auto"] = raw.get("auto") if isinstance(raw.get("auto"), dict) else {}
        if ENV_KEY and ENV_SECRET:          # ключи из переменных окружения главнее
            st["api_key"], st["api_secret"] = ENV_KEY, ENV_SECRET
        return st


def save_store(st):
    with CFG_LOCK:
        st = dict(st)
        if ENV_KEY and ENV_SECRET:          # не дублируем ключи из Variables на диск
            st["api_key"], st["api_secret"] = "", ""
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            tmp = CFG_PATH + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(st, f, ensure_ascii=False, indent=2)
            os.replace(tmp, CFG_PATH)
        except OSError as e:
            print("не удалось сохранить настройки:", e)


def grid_cfg(gid, st=None):
    """Полные настройки одной сетки: её собственные + общие ключи."""
    st = st or load_store()
    g = st["grids"].get(gid) or {k: DEFAULT_CFG[k] for k in GRID_KEYS}
    return dict(g, **{k: st[k] for k in GLOBAL_KEYS})


def load_cfg():
    """Совместимость: настройки первой сетки."""
    st = load_store()
    return grid_cfg(sorted(st["grids"], key=int)[0] if st["grids"] else "1", st)


def set_autostart(flag, gid="1"):
    with CFG_LOCK:
        st = load_store()
        if gid in st["grids"]:
            st["grids"][gid]["was_running"] = bool(flag)
            save_store(st)


class BybitError(Exception):
    pass


class Bybit:
    def __init__(self, key, secret, network, cat="spot"):
        self.cat = cat
        self.key, self.secret = key.strip(), secret.strip().encode()
        self.base = HOSTS[network]
        self.offset = 0
        self.recv = "10000"

    def _req(self, method, path, params=None, signed=False):
        params = params or {}
        url, data = self.base + path, None
        headers = {"Content-Type": "application/json"}
        if method == "GET":
            payload = urllib.parse.urlencode(params)
            if payload:
                url += "?" + payload
        else:
            payload = json.dumps(params, separators=(",", ":"))
            data = payload.encode()
        if signed:
            ts = str(int(time.time() * 1000 + self.offset))
            sign = hmac.new(self.secret, (ts + self.key + self.recv + payload).encode(),
                            hashlib.sha256).hexdigest()
            headers.update({"X-BAPI-API-KEY": self.key, "X-BAPI-TIMESTAMP": ts,
                            "X-BAPI-SIGN": sign, "X-BAPI-RECV-WINDOW": self.recv})
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                res = json.loads(r.read())
        except urllib.error.HTTPError as e:
            raise BybitError(f"HTTP {e.code}: {e.read()[:200].decode(errors='ignore')}")
        except urllib.error.URLError as e:
            raise BybitError(f"Нет связи с Bybit: {e.reason}")
        if res.get("retCode") != 0:
            raise BybitError(f"{res.get('retMsg')} (код {res.get('retCode')})")
        return res["result"]

    def sync_time(self):
        r = self._req("GET", "/v5/market/time")
        self.offset = int(r["timeNano"]) / 1e6 - time.time() * 1000

    def price(self, sym):
        return self.bbo(sym)[0]

    def bbo(self, sym):
        """Последняя цена, лучшая покупка и лучшая продажа в стакане."""
        t = self._req("GET", "/v5/market/tickers", {"category": self.cat, "symbol": sym})["list"][0]
        last = Decimal(t["lastPrice"])
        bid = Decimal(t.get("bid1Price") or "0") or last
        ask = Decimal(t.get("ask1Price") or "0") or last
        return last, bid, ask

    def instrument(self, sym):
        r = self._req("GET", "/v5/market/instruments-info", {"category": self.cat, "symbol": sym})
        if not r["list"]:
            raise BybitError(f"Пара {sym} не найдена ({'спот' if self.cat == 'spot' else 'фьючерсы USDT'})")
        return r["list"][0]

    def balances(self):
        r = self._req("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"}, True)
        out = {}
        for acc in r.get("list", []):
            for c in acc.get("coin", []):
                bal = Decimal(c.get("walletBalance") or "0")
                locked = Decimal(c.get("locked") or "0")
                out[c["coin"]] = {"total": bal, "free": bal - locked}
            out["_margin"] = Decimal(acc.get("totalAvailableBalance") or "0")
            out["_equity"] = Decimal(acc.get("totalEquity") or acc.get("totalWalletBalance") or "0")
        return out

    def place(self, sym, side, qty, price=None, market_quote=None, market=False, reduce_only=False,
              post_only=True):
        body = {"category": self.cat, "symbol": sym, "side": side,
                "orderLinkId": "grid" + uuid.uuid4().hex[:24]}
        if self.cat == "linear":
            body["positionIdx"] = 0
        if reduce_only:
            body["reduceOnly"] = True
        if market_quote is not None:
            body.update({"orderType": "Market", "qty": str(market_quote), "marketUnit": "quoteCoin"})
        elif market:
            body.update({"orderType": "Market", "qty": str(qty)})
            if self.cat == "spot":
                body["marketUnit"] = "baseCoin"
        else:
            body.update({"orderType": "Limit", "qty": str(qty), "price": str(price),
                         "timeInForce": "PostOnly" if post_only else "GTC"})
        return self._req("POST", "/v5/order/create", body, True)["orderId"]

    def open_ids(self, sym):
        ids, cursor = set(), ""
        while True:
            p = {"category": self.cat, "symbol": sym, "limit": 50}
            if cursor:
                p["cursor"] = cursor
            r = self._req("GET", "/v5/order/realtime", p, True)
            ids.update(o["orderId"] for o in r.get("list", []))
            cursor = r.get("nextPageCursor") or ""
            if not cursor or not r.get("list"):
                return ids

    def order(self, sym, oid):
        r = self._req("GET", "/v5/order/history", {"category": self.cat, "symbol": sym, "orderId": oid}, True)
        return r["list"][0] if r.get("list") else None

    def set_leverage(self, sym, lev):
        try:
            self._req("POST", "/v5/position/switch-mode", {"category": "linear", "symbol": sym, "mode": 0}, True)
        except BybitError:
            pass  # уже односторонний режим или есть позиция
        try:
            self._req("POST", "/v5/position/set-leverage", {"category": "linear", "symbol": sym,
                      "buyLeverage": str(lev), "sellLeverage": str(lev)}, True)
        except BybitError as e:
            if "110043" not in str(e):  # плечо уже такое
                raise

    def position(self, sym):
        r = self._req("GET", "/v5/position/list", {"category": "linear", "symbol": sym}, True)
        for p in r.get("list", []):
            if Decimal(p.get("size") or "0") > 0:
                return p
        return None

    def cancel(self, sym, oid):
        self._req("POST", "/v5/order/cancel", {"category": self.cat, "symbol": sym, "orderId": oid}, True)

    def cancel_all(self, sym):
        self._req("POST", "/v5/order/cancel-all", {"category": self.cat, "symbol": sym}, True)


class Radar:
    """Ранняя защита: лента принтов, стакан, скорость цены, открытый интерес, фандинг.
    Данные берутся с фьючерсного рынка той же пары: он обычно двигается первым."""

    def __init__(self, network, symbol):
        self.pub = Bybit("", "", "mainnet" if network == "demo" else network, "linear")
        self.sym, self.cat = symbol, "linear"
        self.trades, self.seen, self.seen_q = deque(), set(), deque()
        self.prices, self.moves = deque(), deque(maxlen=300)
        self.books = deque(maxlen=40)
        self.oi_chg, self.funding, self.n = 0.0, 0.0, 0
        self.dump = self.pump = 0.0
        self.m = {"warm": False}

    def init(self):
        try:
            self.pub._req("GET", "/v5/market/tickers", {"category": "linear", "symbol": self.sym})
        except BybitError:
            self.cat = self.pub.cat = "spot"

    def update(self):
        now = time.time() * 1000
        r = self.pub._req("GET", "/v5/market/recent-trade",
                          {"category": self.cat, "symbol": self.sym, "limit": 1000 if self.cat == "linear" else 60})
        for t in reversed(r.get("list", [])):
            if t["execId"] in self.seen:
                continue
            self.seen.add(t["execId"]); self.seen_q.append(t["execId"])
            pr = float(t["price"])
            self.trades.append((int(t["time"]), t["side"], float(t["size"]) * pr, pr))
        while len(self.seen_q) > 30000:
            self.seen.discard(self.seen_q.popleft())
        while self.trades and self.trades[0][0] < now - 12 * 60000:
            self.trades.popleft()
        if not self.trades:
            return self.m
        price = self.trades[-1][3]
        self.prices.append((now, price))
        while self.prices and self.prices[0][0] < now - 15 * 60000:
            self.prices.popleft()

        ob = self.pub._req("GET", "/v5/market/orderbook", {"category": self.cat, "symbol": self.sym, "limit": 50})
        bids = [(float(p), float(q_)) for p, q_ in ob.get("b", [])]
        asks = [(float(p), float(q_)) for p, q_ in ob.get("a", [])]
        if bids and asks:
            mid = (bids[0][0] + asks[0][0]) / 2
            bid_d = sum(p * q_ for p, q_ in bids if p >= mid * 0.995)
            ask_d = sum(p * q_ for p, q_ in asks if p <= mid * 1.005)
        else:
            bid_d = ask_d = 0.0
        prev_books = list(self.books)
        self.books.append((bid_d, ask_d))

        if self.cat == "linear" and self.n % 20 == 0:
            try:
                oi = self.pub._req("GET", "/v5/market/open-interest", {"category": "linear", "symbol": self.sym,
                                   "intervalTime": "5min", "limit": 3}).get("list", [])
                if len(oi) >= 3 and float(oi[2]["openInterest"]):
                    self.oi_chg = (float(oi[0]["openInterest"]) / float(oi[2]["openInterest"]) - 1) * 100
                tk = self.pub._req("GET", "/v5/market/tickers", {"category": "linear", "symbol": self.sym})
                self.funding = float(tk["list"][0].get("fundingRate") or 0)
            except (BybitError, KeyError, IndexError, ValueError):
                pass
        self.n += 1

        # ---- лента: объём, скорость принтов, дельта за 30 секунд против фона последних 10 минут
        W = 30000
        rec = [x for x in self.trades if x[0] >= now - W]
        base = [x for x in self.trades if now - 11 * 60000 <= x[0] < now - W]
        span = (now - W - base[0][0]) if base else 0
        warm = span >= 120000 and len(self.moves) >= 20
        vol = sum(x[2] for x in rec)
        buy = sum(x[2] for x in rec if x[1] == "Buy")
        delta = (2 * buy - vol) / vol if vol else 0.0
        burst = speed_x = 1.0
        if span > 0:
            bv = sum(x[2] for x in base) / span * W
            bc = len(base) / span * W
            burst = vol / bv if bv else 1.0
            speed_x = len(rec) / bc if bc else 1.0
        tps = len(rec) / (W / 1000)

        # ---- скорость цены: движение за 60 с относительно обычного для этой пары
        def price_ago(ms):
            for t, p in self.prices:
                if t >= now - ms:
                    return p
            return price
        p60 = price_ago(60000)
        move60 = (price / p60 - 1) * 100 if p60 else 0.0
        self.moves.append(abs(move60))
        med = max(statistics.median(self.moves), 0.02) if self.moves else 0.05
        move_x = move60 / med
        move5 = (price / price_ago(5 * 60000) - 1) * 100

        # ---- стакан: перекос в пределах 0.5% и снятая ликвидность
        imb = (bid_d - ask_d) / (bid_d + ask_d) if bid_d + ask_d else 0.0
        if prev_books:
            ab = sum(b for b, _ in prev_books) / len(prev_books)
            aa = sum(a for _, a in prev_books) / len(prev_books)
            bid_pull = 1 - bid_d / ab if ab else 0.0
            ask_pull = 1 - ask_d / aa if aa else 0.0
        else:
            bid_pull = ask_pull = 0.0

        # ---- оценка риска (0–100)
        flow = clamp((max(burst, speed_x) - 1.5) / 3)
        oi_d = clamp(abs(self.oi_chg) / 2) if move5 < -0.3 else 0.0
        oi_p = clamp(abs(self.oi_chg) / 2) if move5 > 0.3 else 0.0
        dump = 100 * (0.30 * clamp((-move_x - 2) / 4)
                      + 0.25 * clamp((-delta - 0.2) / 0.5)
                      + 0.20 * flow * (1 if delta < -0.1 else 0.3)
                      + 0.15 * max(clamp((-imb - 0.2) / 0.5), clamp((bid_pull - 0.25) / 0.5))
                      + 0.10 * max(oi_d, clamp((self.funding - 0.0003) / 0.0007)))
        pump = 100 * (0.30 * clamp((move_x - 2) / 4)
                      + 0.25 * clamp((delta - 0.2) / 0.5)
                      + 0.20 * flow * (1 if delta > 0.1 else 0.3)
                      + 0.15 * max(clamp((imb - 0.2) / 0.5), clamp((ask_pull - 0.25) / 0.5))
                      + 0.10 * max(oi_p, clamp((-self.funding - 0.0003) / 0.0007)))
        if not warm:
            dump, pump = min(dump, 40), min(pump, 40)
        self.dump = max(dump, self.dump * 0.8)
        self.pump = max(pump, self.pump * 0.8)
        self.m = {"warm": warm, "tps": round(tps, 1), "speed_x": round(speed_x, 2), "burst": round(burst, 2),
                  "delta": round(delta, 3), "move60": round(move60, 3), "move_x": round(move_x, 1),
                  "imb": round(imb, 3), "bid_pull": round(bid_pull, 2), "ask_pull": round(ask_pull, 2),
                  "oi": round(self.oi_chg, 2), "funding": self.funding, "src": self.cat,
                  "dump": round(self.dump), "pump": round(self.pump)}
        return self.m


class Bot:
    def __init__(self, gid="1"):
        self.gid = gid
        self.lock = threading.RLock()
        self.running = False
        self.busy = False
        self.thread = None
        self.reset()

    def reset(self):
        self.status, self.error = "Остановлен", ""
        self.symbol, self.base_coin, self.quote_coin = "", "", ""
        self.hist, self.fills, self.log, self.levels = [], [], [], []
        self.price, self.profit, self.trades = None, 0.0, 0
        self.orders, self.bal, self.network = {}, {}, ""
        self.cat, self.lev, self.pos = "spot", 1, None
        self.cells, self.kmin, self.n, self._lv = {}, 0, 0, {}
        self.out_up = self.out_dn = 0
        self.dn_losses, self.trail_wait, self.cap = [], "", Decimal(0)
        self.trail_pct, self.down_cap = Decimal("0.005"), Decimal("0.05")
        self.stop_pct = STOP_BELOW
        self.up_at = self.dn_at = None
        self.shift_up = self.shift_dn = 0
        self.trail_pnl = 0.0
        self.per_min, self.geo = Decimal(0), False
        self.short = False
        self.neutral = False
        self.trail_up = self.trail_down = False
        self.up_limit = self.dn_limit = Decimal(0)
        self.radar, self.radar_mode, self.radar_sens = None, "off", "mid"
        self.guard, self.guard_t, self.paused, self.calm = None, 0, {}, 0
        self.exit_spot = False
        self.exit_limit = False
        self.emergency = False
        self.ref, self.emerg = None, EMERG_DEFAULT
        self.bid = self.ask = None
        self.maker_n = self.taker_n = 0

    def say(self, kind, text, val=None):
        with self.lock:
            self.log.insert(0, {"t": time.strftime("%H:%M:%S"), "k": kind, "text": text, "v": val})
            del self.log[120:]
        print(time.strftime("%H:%M:%S"), f"[сетка {self.gid} {self.symbol}]", kind, text,
              "" if val is None else val, flush=True)

    def unrealized(self):
        """Плавающий результат открытых позиций сетки, USDT."""
        with self.lock:
            if not (self.running or self.busy):
                return 0.0
            if self.cat == "linear":
                return float(self.pos.get("unrealisedPnl") or 0) if self.pos else 0.0
            if not self.price:
                return 0.0
            u = Decimal(0)
            held = [o for o in self.orders.values() if o["side"] == "Sell"]
            held += [{"cell": c["cell"], "qty": c["sell_qty"]} for c in self.paused.values() if c["side"] == "Sell"]
            for o in held:
                c = o["cell"]
                u += o["qty"] * self.price * (1 - self.fee) - c.get("cost", Decimal(0))
            return float(u)

    def state(self):
        upnl = self.unrealized()
        with self.lock:
            b = {k: float(v["free"]) for k, v in self.bal.items() if k in (self.base_coin, self.quote_coin)}
            if "_margin" in self.bal:
                b["_margin"] = float(self.bal["_margin"])
            pos = None
            if self.pos:
                pos = {"side": self.pos.get("side"), "size": self.pos.get("size"),
                       "entry": float(self.pos.get("avgPrice") or 0),
                       "liq": float(self.pos.get("liqPrice") or 0),
                       "upnl": float(self.pos.get("unrealisedPnl") or 0)}
            return {"id": self.gid, "running": self.running, "busy": self.busy, "status": self.status, "error": self.error,
                    "symbol": self.symbol, "base": self.base_coin, "quote": self.quote_coin,
                    "network": self.network, "price": float(self.price) if self.price else None,
                    "levels": [float(x) for x in self.levels], "hist": self.hist[-400:],
                    "fills": self.fills[-100:], "log": self.log[:60], "profit": self.profit,
                    "upnl": upnl, "total": self.profit + upnl,
                    "trades": self.trades, "open": len(self.orders), "bal": b,
                    "market": self.cat, "leverage": self.lev, "pos": pos, "direction": "sell" if self.short else "neutral" if self.neutral else "both",
                    "trail": {"up": self.trail_up, "down": self.trail_down,
                              "nup": self.shift_up, "ndn": self.shift_dn, "pnl": self.trail_pnl,
                              "wait": self.trail_wait, "pct": float(self.trail_pct * 100),
                              "up_at": float(self.up_at) if self.up_at else None,
                              "dn_at": float(self.dn_at) if self.dn_at else None},
                    "fees": {"maker": self.maker_n, "taker": self.taker_n},
                    "radar": dict(self.radar.m, mode=self.radar_mode, guard=self.guard,
                                  paused=len(self.paused)) if self.radar else {"mode": "off"}}

    def start(self, cfg):
        if self.running or self.busy:
            return
        self.reset()
        self.running = self.busy = True
        self.status = "Запускаю…"
        set_autostart(True, self.gid)
        self.thread = threading.Thread(target=self._run, args=(cfg,), daemon=True)
        self.thread.start()

    def stop(self, remember=True):
        if remember:
            set_autostart(False, self.gid)
        if self.running:
            self.running = False
            self.busy = True
            self.status = "Останавливаю…"

    # ---------- основной цикл ----------
    def _run(self, cfg):
        api = None
        try:
            if not cfg["api_key"] or not cfg["api_secret"]:
                raise BybitError("Введите API ключ и секрет в настройках")
            cat = "linear" if cfg.get("market") == "futures" else "spot"
            api = Bybit(cfg["api_key"], cfg["api_secret"], cfg["network"], cat)
            api.sync_time()
            self._setup(api, cfg)
            self.busy = False
            self.status = "Работает"
            errors, n = 0, 0
            while self.running:
                try:
                    self._tick(api)
                    errors = 0
                    n += 1
                    if n % 10 == 0:
                        self.bal = api.balances()
                except BybitError as e:
                    errors += 1
                    self.say("err", str(e))
                    if errors >= 10:
                        raise BybitError("10 ошибок подряд, бот остановлен: " + str(e))
                    time.sleep(5)
                    continue
                for _ in range(POLL_SEC * 10):
                    if not self.running:
                        break
                    time.sleep(0.1)
        except Exception as e:
            self.error = str(e)
            self.say("err", str(e))
        finally:
            if api and self.symbol:
                try:
                    coins = sum((o["qty"] for o in self.orders.values() if o["side"] == "Sell"), Decimal(0))
                    coins += sum((c["sell_qty"] for c in self.paused.values() if c["side"] == "Sell"), Decimal(0))
                    api.cancel_all(self.symbol)
                    if self.cat == "spot":
                        sell = q(coins, self.bstep)
                        if self.emergency and sell >= self.min_qty:
                            api.place(self.symbol, "Sell", sell, market=True)
                            self.taker_n += 1
                            self.say("sell", f"аварийный стоп: продано {sell} {self.base_coin} по рынку")
                        elif self.exit_limit and sell >= self.min_qty:
                            got, _ = self._chase(api, "Sell", sell, tries=CLOSE_TRIES)
                            self.say("sell", f"выход по радару: продано лимитками {got} из {sell} {self.base_coin}")
                        else:
                            self.say("info", "все ордера бота отменены, монеты остались на счёте")
                    else:
                        self.say("info", "все ордера бота отменены")
                        if self.emergency:
                            self._close_position(api)
                        else:
                            self._close_position_limit(api)
                except Exception as e:
                    self.say("err", "не удалось отменить ордера или закрыть позицию: " + str(e)
                             + ". Проверьте счёт на Bybit вручную!")
            with self.lock:
                self.orders = {}
                self.running = False
                self.busy = False
                self.status = "Ошибка" if self.error else "Остановлен"

    def _setup(self, api, cfg):
        sym = cfg["symbol"].upper().strip()
        self.cat = api.cat
        inst = api.instrument(sym)
        self.symbol, self.network = sym, cfg["network"]
        self.base_coin, self.quote_coin = inst["baseCoin"], inst["quoteCoin"]
        lot, pf = inst["lotSizeFilter"], inst["priceFilter"]
        self.tick = Decimal(pf["tickSize"])
        if self.cat == "linear":
            self.fee, self.fee_buf = FUT_FEE, Decimal(0)
            self.bstep = Decimal(lot["qtyStep"])
            min_amt = Decimal(lot.get("minNotionalValue") or "5")
            max_lev = Decimal(inst.get("leverageFilter", {}).get("maxLeverage") or "100")
            lev = int(cfg.get("leverage") or 1)
            if not 1 <= lev <= 100:
                raise BybitError("Плечо должно быть от 1 до 100")
            if lev > max_lev:
                raise BybitError(f"Для {sym} Bybit даёт максимум {max_lev}x")
            self.lev = lev
        else:
            self.fee, self.fee_buf = SPOT_FEE, SPOT_FEE_BUF
            self.bstep = Decimal(lot["basePrecision"])
            self.qstep = Decimal(lot.get("quotePrecision") or "0.0001")
            min_amt = Decimal(lot.get("minOrderAmt") or "0")
            self.lev = 1
        min_qty = Decimal(lot.get("minOrderQty") or "0")

        p = api.price(sym)
        self.price = p
        self.hist.append({"t": int(time.time() * 1000), "p": float(p)})
        n = int(cfg["levels"])
        if not 3 <= n <= 50:
            raise BybitError("Уровней должно быть от 3 до 50")
        rng = float(cfg["range_pct"]) / 100
        self.short = cfg.get("direction") == "sell"
        self.neutral = cfg.get("direction") == "neutral"
        if (self.short or self.neutral) and self.cat != "linear":
            what = "«Только Sell»" if self.short else "«Нейтральная»"
            raise BybitError(f"{what} работает на фьючерсах: на споте нельзя продать то, чего нет. "
                             "Выберите рынок «Фьючерсы USDT».")
        if self.neutral and n % 2 == 0:   # цена ровно на среднем уровне: поровну линий Buy ниже и Sell выше
            n += 1
            self.say("info", f"нейтральная сетка: уровней {n} (нечётное число — цена на среднем уровне)")
        if self.short:   # шорт-сетка: цена у низа диапазона, почти все линии выше цены
            lo, hi = float(p) * (1 - 0.2 * rng), float(p) * (1 + 1.8 * rng)
        else:
            lo, hi = float(p) * (1 - rng), float(p) * (1 + rng)
        self.lo0, self.r, self.n, self.kmin, self._lv = lo, (hi / lo) ** (1 / (n - 1)), n, 0, {}
        L = [self.lv(i) for i in range(n)]
        self.levels = L
        self.trail_up, self.trail_down = bool(cfg.get("trail_up")), bool(cfg.get("trail_down"))
        self.up_limit = Decimal(str(cfg.get("trail_up_limit") or 0))
        self.dn_limit = Decimal(str(cfg.get("trail_down_limit") or 0))
        self.radar_mode = cfg.get("radar") or "off"
        self.radar_sens = cfg.get("radar_sens") if cfg.get("radar_sens") in RADAR_SENS else "mid"
        if self.radar_mode != "off":
            self.radar = Radar(cfg["network"], sym)
            try:
                self.radar.init()
            except BybitError as e:
                self.say("err", "радар не запустился: " + str(e))
                self.radar = None

        self.bal = api.balances()
        if cfg.get("cap_max"):     # «весь депозит»: капитал = свободные средства на момент запуска
            if self.cat == "linear":
                free = self.bal.get("_margin", Decimal(0))
                cap = (free * CAP_MAX_FUT).quantize(Decimal("0.01"), ROUND_DOWN)
            else:
                free = self.bal.get(self.quote_coin, {}).get("free", Decimal(0))
                cap = (free * CAP_MAX_SPOT).quantize(Decimal("0.01"), ROUND_DOWN)
            if cap <= 0:
                raise BybitError(f"Свободных {self.quote_coin} на счёте нет — «весь депозит» не из чего взять.")
            self.say("info", f"весь депозит: свободно {free:.2f} {self.quote_coin}, капитал бота {cap} {self.quote_coin}")
        else:
            cap = Decimal(str(cfg["capital"]))
        self.cap = cap
        self.trail_pct = min(max(Decimal(str(cfg.get("trail_pct") if cfg.get("trail_pct") is not None else 2)), Decimal(0)), Decimal(50)) / 100
        self.down_cap = max(Decimal(str(cfg.get("trail_down_cap") if cfg.get("trail_down_cap") is not None else 5)), Decimal(0)) / 100
        ep = cfg.get("emerg_pct")
        self.emerg = min(max(Decimal(str(ep if ep is not None else 10)), Decimal(1)), Decimal(50)) / 100
        self.ref = p          # цена запуска: от неё считается аварийный стоп (при сдвиге сетки — от цены сдвига)
        total = cap * self.lev           # объём всей сетки (с учётом плеча)
        # лоты: одинаковые или геометрические — у цены самый маленький, к краям каждый больше в mult раз
        mult = Decimal(1)
        if cfg.get("lot_mode") == "geo":
            mult = min(max(Decimal(str(cfg.get("lot_mult") or "1.3")), Decimal("1.01")), Decimal(3))
        if self.neutral:   # средний уровень = цена: ячейки ниже — Buy, выше — Sell, деньги ровно пополам
            mid = (n - 1) // 2
            m = mid - 1
        else:
            m = max([i for i in range(n - 1) if L[i] <= p], default=-1)      # ячейка, в которой сейчас цена
        dist = {i: (m - i) if i <= m else (i - m - 1) for i in range(n - 1)}
        w = {i: mult ** dist[i] for i in dist}
        if self.neutral:
            wb = sum(w[i] for i in w if i <= m)
            ws = sum(w[i] for i in w if i > m)
            per = {i: (total / 2) * w[i] / (wb if i <= m else ws) for i in w}
        else:
            sw = sum(w.values())
            per = {i: total * w[i] / sw for i in w}
        self.per_min, self.geo = min(per.values()), mult > 1
        if self.per_min < min_amt * Decimal("1.05"):
            hint = " или множитель лота" if self.geo else ""
            raise BybitError(f"Самый маленький лот выходит {self.per_min:.2f} {self.quote_coin}, а минимум ордера на "
                             f"Bybit {min_amt} {self.quote_coin}. Увеличьте капитал, уменьшите число уровней{hint}.")
        if self.geo:
            self.say("info", f"геометрические лоты ×{mult}: от {self.per_min:.2f} до {max(per.values()):.2f} {self.quote_coin}")
        self.min_amt, self.min_qty = min_amt, min_qty
        self.cells = {}
        for i in range(n - 1):
            qty = q(per[i] / L[i], self.bstep)
            if qty < min_qty:
                need = min_qty * L[i]
                hint = ", уменьшите число уровней или множитель лота" if self.geo else " или уменьшите число уровней"
                raise BybitError(f"Лот {per[i]:.2f} {self.quote_coin} меньше минимального объёма Bybit "
                                 f"({min_qty} {self.base_coin} ≈ {need:.2f} {self.quote_coin}). Увеличьте капитал{hint}.")
            self.cells[i] = {"i": i, "qty": qty, "cost": qty * L[i] * (1 + self.fee)}

        if self.neutral:
            sell_cells = [c for c in self.cells.values() if c["i"] > m]
            buy_cells = [c for c in self.cells.values() if c["i"] <= m]
            for c in sell_cells:
                c["mode"] = "short"
        else:
            sell_cells = [c for c in self.cells.values() if L[c["i"]] > p]
            buy_cells = [c for c in self.cells.values() if L[c["i"]] <= p]
        need_base = sum(c["qty"] for c in sell_cells)
        need_quote = sum(c["qty"] * L[c["i"]] for c in buy_cells)

        if self.cat == "linear":
            self._setup_futures(api, cfg, p, cap, sell_cells, buy_cells, need_base)
            return

        free_base = self.bal.get(self.base_coin, {}).get("free", Decimal(0))
        free_quote = self.bal.get(self.quote_coin, {}).get("free", Decimal(0))
        short = need_base - free_base
        buy_qty = Decimal(0)
        if short > 0:
            buy_qty = q(short * (1 + self.fee_buf * 2), self.bstep)
            if buy_qty * p < min_amt:
                buy_qty = q(min_amt * Decimal("1.05") / p, self.bstep) + self.bstep
        market_amt = buy_qty * p * Decimal("1.003")
        if free_quote < need_quote + market_amt:
            raise BybitError(f"Не хватает {self.quote_coin}: нужно {need_quote + market_amt:.2f}, "
                             f"свободно {free_quote:.2f}. Пополните счёт или уменьшите капитал.")
        self.say("info", f"сетка {L[0]} – {L[-1]}, {n} уровней, {cap} {self.quote_coin}, " + self._trail_text())

        if buy_qty > 0:
            got, spent = self._chase(api, "Buy", buy_qty, tries=SETUP_TRIES)
            self.say("buy", f"лимитками {got} {self.base_coin} на {spent:.2f} {self.quote_coin} для верхних ордеров")
            if got < short:
                raise BybitError(f"За ~50 секунд лимитками куплено {got} из {buy_qty} {self.base_coin} — по рынку бот не входит. "
                                 f"Купленное осталось на счёте; запустите ещё раз, бот учтёт эти монеты.")
            if got > 0:
                p = spent / got
        _, self.bid, self.ask = api.bbo(sym)
        self._place_grid(api, p, sell_cells, buy_cells)

    def _place_grid(self, api, p, sell_cells, buy_cells):
        for c in sell_cells:
            c["cost"] = c["qty"] * p * (1 + self.fee)
            self._put(api, c, "Sell", q(c["qty"] * (1 - self.fee_buf), self.bstep))
        for c in buy_cells:
            self._put(api, c, "Buy", c["qty"])
        self.say("info", f"выставлено ордеров: {len(self.orders)}")

    def _setup_futures(self, api, cfg, p, cap, sell_cells, buy_cells, need_base):
        self._api = api
        sym, L = self.symbol, self.levels
        margin = self.bal.get("_margin", Decimal(0))
        if margin < cap:
            raise BybitError(f"Свободной маржи {margin:.2f} USDT, а капитал бота {cap} USDT. "
                             f"Пополните счёт или уменьшите капитал.")
        if api.position(sym):
            raise BybitError(f"По {sym} уже есть открытая позиция. Закройте её или выберите другую пару.")
        api.set_leverage(sym, self.lev)
        liq_move = 100 / self.lev
        rng = float(cfg["range_pct"])
        self.say("info", f"фьючерсы, плечо {self.lev}x, объём сетки {cap * self.lev:.0f} USDT, " + self._trail_text())
        if liq_move < rng * 1.2:
            self.say("err", f"плечо {self.lev}x: ликвидация примерно при движении на {liq_move:.1f}%, "
                            f"а диапазон ±{rng}%. Бот закроет позицию раньше ликвидации.")
        if self.neutral:
            sb = sum(c["qty"] * L[c["i"]] for c in buy_cells)
            ss = sum(c["qty"] * L[c["i"] + 1] for c in sell_cells)
            self.say("info", f"нейтральная сетка без начальной позиции: Buy {len(buy_cells)} шт. на {sb:.0f} USDT ниже цены, "
                             f"Sell {len(sell_cells)} шт. на {ss:.0f} USDT выше. Зарабатывает на колебаниях в обе стороны")
            _, self.bid, self.ask = api.bbo(sym)
            for c in buy_cells:
                self._put(api, c, "Buy", c["qty"])
            for c in sell_cells:
                self._put(api, c, "Sell", c["qty"])
            self.say("info", f"выставлено ордеров: {len(self.orders)}")
            return
        if self.short:
            self.say("info", f"режим «Только Sell»: шорт-лимитки на линиях выше цены, тейки reduce-only, "
                             f"сетка {self.levels[0]} – {self.levels[-1]}")
            _, self.bid, self.ask = api.bbo(sym)
            self._arm_short()
            self.say("info", f"выставлено Sell-лимиток: {len(self.orders)}")
            return
        if need_base > 0:
            want = q(need_base, self.bstep)
            got, spent = self._chase(api, "Buy", want, tries=SETUP_TRIES)
            self.say("buy", f"лонг лимитками {got} {self.base_coin} для верхних ордеров")
            if got < want:
                raise BybitError(f"За ~50 секунд лимитками набрано {got} из {want} {self.base_coin} — по рынку бот не входит. "
                                 f"Набранная позиция будет закрыта лимитками; запустите ещё раз.")
            if got > 0:
                p = spent / got
        _, self.bid, self.ask = api.bbo(sym)
        self._place_grid(api, p, sell_cells, buy_cells)

    def _close_position(self, api):
        """Аварийное закрытие по рынку — только при аварийном стопе и у ликвидации."""
        pos = api.position(self.symbol)
        if not pos:
            self.say("info", "открытой позиции нет")
            return
        side = "Sell" if pos["side"] == "Buy" else "Buy"
        api.place(self.symbol, side, pos["size"], market=True, reduce_only=True)
        self.taker_n += 1
        self.say("info", f"аварийный стоп: позиция {pos['size']} {self.base_coin} закрыта по рынку, "
                         f"нереализованный PnL был {float(pos.get('unrealisedPnl') or 0):.2f} USDT")
        self.pos = None

    def _close_position_limit(self, api):
        """Обычная остановка: позиция закрывается только лимитками. Что не закрылось — остаётся открытым."""
        pos = api.position(self.symbol)
        if not pos:
            self.say("info", "открытой позиции нет")
            return
        side = "Sell" if pos["side"] == "Buy" else "Buy"
        size = Decimal(pos["size"])
        got, _ = self._chase(api, side, size, reduce_only=True, tries=CLOSE_TRIES)
        left = size - got
        if left > 0:
            self.say("err", f"позиция закрыта лимитками на {got} из {size} {self.base_coin}; осталось {left} — "
                            f"по рынку бот не закрывает. Закройте вручную на Bybit или запустите сетку снова.")
        else:
            self.say("info", f"позиция {size} {self.base_coin} закрыта лимитками")
        self.pos = api.position(self.symbol)

    # ---------- уровни с абсолютным номером: сетка может сдвигаться бесконечно ----------
    def lv(self, k):
        if k not in self._lv:
            self._lv[k] = q(Decimal(repr(self.lo0 * self.r ** k)), self.tick)
        return self._lv[k]

    def _refresh_levels(self):
        with self.lock:
            self.levels = [self.lv(k) for k in range(self.kmin, self.kmin + self.n)]

    def _trail_text(self):
        up = "вверх" + (f" до {self.up_limit}" if self.up_limit else "") if self.trail_up else ""
        dn = "вниз" + (f" до {self.dn_limit}" if self.dn_limit else "") if self.trail_down else ""
        both = " и ".join(x for x in (up, dn) if x)
        return "трейлинг " + both if both else "без трейлинга"

    def _order_of(self, k):
        for oid, o in self.orders.items():
            if o["cell"]["i"] == k:
                return oid, o
        return None, None

    def _can_trail_down(self):
        return self.trail_down and (not self.dn_limit or self.lv(self.kmin - 1) >= self.dn_limit)

    def _can_trail_up(self):
        return self.trail_up and (not self.up_limit or self.lv(self.kmin + self.n) <= self.up_limit)

    # шорт-сетка: «трейлинг вверх» в настройках = за ценой вниз (в прибыль), «трейлинг вниз» = вверх против шорта
    def _short_can_follow(self):
        return self.trail_up and (not self.dn_limit or self.lv(self.kmin - 1) >= self.dn_limit)

    def _short_can_against(self):
        return self.trail_down and (not self.up_limit or self.lv(self.kmin + self.n) <= self.up_limit)

    def _down_day_limit(self, now):
        """Лимит убытка: сдвиги вниз за последние сутки уже зафиксировали down_cap капитала. 0 — без лимита."""
        if not self.down_cap or not self.cap:
            return False
        self.dn_losses = [(t, x) for t, x in self.dn_losses if now - t < 86400]
        lost = -sum(x for _, x in self.dn_losses)
        if lost >= float(self.cap * self.down_cap):
            self.trail_wait = f"↓ лимит: сдвиги вниз за сутки уже −{lost:.2f} {self.quote_coin}"
            return True
        return False

    def _trail_short(self, api, p):
        top, bot = self.lv(self.kmin + self.n - 1), self.lv(self.kmin)
        self.dn_at = bot * (1 - self.trail_pct) if self.trail_up else None
        self.up_at = top * (1 + self.trail_pct) if self.trail_down else None
        self.trail_wait = ""
        steps = 0
        if self._short_can_follow() and p < bot * (1 - self.trail_pct):
            while steps < TRAIL_MAX_STEPS and self._short_can_follow() and p < self.lv(self.kmin):
                if not self._shift_short_down(api):
                    break
                steps += 1
        elif self._short_can_against() and p > top * (1 + self.trail_pct):
            while steps < TRAIL_MAX_STEPS and self._short_can_against() and p > self.lv(self.kmin + self.n - 1):
                if self._down_day_limit(time.time()) or not self._shift_short_up(api, p):
                    break
                steps += 1
        if steps:
            self._refresh_levels()
            self.ref = p
            top, bot = self.lv(self.kmin + self.n - 1), self.lv(self.kmin)
            self.dn_at = bot * (1 - self.trail_pct) if self.trail_up else None
            self.up_at = top * (1 + self.trail_pct) if self.trail_down else None

    def _shift_short_down(self, api):
        """Цена ушла под шорт-сетку: все шорты уже закрыты тейками. Переносим верхнюю пустую ячейку вниз —
        без сделок и без убытка."""
        ktop = self.kmin + self.n - 2
        c = self.cells.get(ktop)
        if c is None or c.get("pos"):
            return False
        oid, o = self._order_of(ktop)
        if o:
            try:
                api.cancel(self.symbol, oid)
            except BybitError:
                return False  # успела исполниться — разберёмся на следующей проверке
        with self.lock:
            if oid:
                self.orders.pop(oid, None)
            self.cells.pop(ktop, None)
            self.paused.pop(ktop, None)
            self.kmin -= 1
        k = self.kmin
        qty = q(self.per_min / self.lv(k), self.bstep) if self.geo else c["qty"]
        self.cells[k] = {"i": k, "qty": qty, "cost": Decimal(0)}
        self._arm_short()
        self.shift_dn += 1
        self.say("info", f"трейлинг ↓ шорт-сетка сдвинута за ценой: {self.lv(self.kmin)} – {self.lv(self.kmin + self.n - 1)}")
        return True

    def _shift_short_up(self, api, p):
        """Цена ушла над шорт-сеткой: откупаем самый невыгодный (нижний) шорт и переносим ячейку наверх.
        Фиксирует убыток, поэтому по умолчанию выключено."""
        kb = self.kmin
        c = self.cells.get(kb)
        if c is None:
            return False
        oid, o = self._order_of(kb)
        if o:
            try:
                api.cancel(self.symbol, oid)
            except BybitError:
                return False
            with self.lock:
                self.orders.pop(oid, None)
        loss = 0.0
        if c.get("pos"):
            pos0 = c["pos"]
            got, money = self._chase(api, "Buy", pos0, reduce_only=True)
            entry_part = c.get("entry", money) * got / pos0 if got else 0
            loss = float(entry_part - money * (1 + self.fee)) if got else 0.0
            self.trail_pnl += loss
            self.profit += loss
            pnl_book(self.gid, loss)
            self.dn_losses.append((time.time(), loss))
            left = q(pos0 - got, self.bstep)
            if left >= self.min_qty:   # откуплено не всё — сдвига нет, тейк на остаток возвращается
                c["pos"] = left
                c["entry"] = c.get("entry", Decimal(0)) * left / pos0
                self._put(api, c, "Buy", left)
                self.say("info", f"сдвиг против шорта: лимиткой откуплено {got} из {pos0}, по рынку не откупаю — сетка осталась на месте")
                return False
        with self.lock:
            self.cells.pop(kb, None)
            self.paused.pop(kb, None)
            self.kmin += 1
        k = self.kmin + self.n - 2
        qty = q(self.per_min / self.lv(k), self.bstep) if self.geo else c["qty"]
        self.cells[k] = {"i": k, "qty": qty, "cost": Decimal(0)}
        self._arm_short()
        self.shift_up += 1
        self.say("info", f"трейлинг ↑ шорт-сетка сдвинута против шорта: {self.lv(self.kmin)} – "
                         f"{self.lv(self.kmin + self.n - 1)}", round(loss, 4))
        return True

    def _trail(self, api, p):
        if self.short:
            return self._trail_short(api, p)
        if self.neutral:   # нейтральная сетка стоит на месте, за диапазоном срабатывает стоп
            self.up_at = self.dn_at = None
            return
        """Сдвиг сетки, когда цена ушла за границу на trail_pct %. Сдвигаем столько шагов, сколько нужно,
        чтобы цена снова оказалась внутри сетки."""
        top, bot = self.lv(self.kmin + self.n - 1), self.lv(self.kmin)
        self.up_at = top * (1 + self.trail_pct) if self.trail_up else None
        self.dn_at = bot * (1 - self.trail_pct) if self.trail_down else None
        self.trail_wait = ""
        steps = 0
        if self._can_trail_up() and p > top * (1 + self.trail_pct):
            while steps < TRAIL_MAX_STEPS and self._can_trail_up() and p > self.lv(self.kmin + self.n - 1):
                if not self._shift_up(api, p):
                    break
                steps += 1
        elif self._can_trail_down() and p < bot * (1 - self.trail_pct):
            while steps < TRAIL_MAX_STEPS and self._can_trail_down() and p < self.lv(self.kmin):
                if self._down_day_limit(time.time()) or not self._shift_down(api, p):
                    break
                steps += 1
        if steps:
            self._refresh_levels()
            self.ref = p
            top, bot = self.lv(self.kmin + self.n - 1), self.lv(self.kmin)
            self.up_at = top * (1 + self.trail_pct) if self.trail_up else None
            self.dn_at = bot * (1 - self.trail_pct) if self.trail_down else None

    def _shift_up(self, api, p):
        """Цена выше сетки: снимаем самый нижний ордер на покупку и переносим ячейку наверх."""
        oid, o = self._order_of(self.kmin)
        if not o or o["side"] != "Buy":
            return False
        if self.cat == "spot" and q(o["qty"] * o["price"] / p * (1 - self.fee_buf), self.bstep) < self.min_qty:
            self.say("err", "сдвиг вверх: объём ячейки меньше минимума биржи, трейлинг вверх выключен")
            self.trail_up = False
            return False
        try:
            api.cancel(self.symbol, oid)
        except BybitError:
            return False  # ордер успел исполниться, разберёмся на следующей проверке
        old_cell = self.cells.get(self.kmin)
        with self.lock:
            self.orders.pop(oid, None)
            self.cells.pop(self.kmin, None)
            self.kmin += 1
        freed = o["qty"] * o["price"]
        k = self.kmin + self.n - 2
        if self.geo:   # новая ячейка встаёт рядом с ценой — ей положен самый маленький лот
            want = q(self.per_min / p * (1 - self.fee_buf), self.bstep)
        else:
            want = o["qty"] if self.cat == "linear" else q(freed / p * (1 - self.fee_buf), self.bstep)
        got, spent = self._chase(api, "Buy", want)
        if got <= 0:   # лимитка не исполнилась — сдвига нет, нижняя покупка возвращается на место
            with self.lock:
                self.kmin -= 1
                if old_cell:
                    self.cells[self.kmin] = old_cell
            if old_cell:
                self._put(api, old_cell, "Buy", o["qty"])
            self.say("info", "сдвиг вверх: лимитка не исполнилась, по рынку не покупаю — сетка осталась на месте")
            return False
        qty = got if self.cat == "linear" else q(got * (1 - self.fee_buf), self.bstep)
        cell = {"i": k, "qty": qty, "cost": spent * (1 + self.fee)}
        self.cells[k] = cell
        self._put(api, cell, "Sell", qty)
        self.shift_up += 1
        self.fills.append({"t": int(time.time() * 1000), "p": float(p), "k": "buy"})
        self.say("info", f"трейлинг ↑ сетка сдвинута: {self.lv(self.kmin)} – {self.lv(self.kmin + self.n - 1)}")
        return True

    def _shift_down(self, api, p):
        """Цена ниже сетки: продаём монету самой верхней ячейки и ставим новую покупку ниже."""
        ktop = self.kmin + self.n - 2
        oid, o = self._order_of(ktop)
        if not o or o["side"] != "Sell":
            return False
        if self.cat == "spot":
            est = q(o["qty"] * p * (1 - self.fee) / self.lv(self.kmin - 1), self.bstep)
            if est < self.min_qty or o["qty"] * p < self.min_amt:
                self.say("err", "сдвиг вниз: объём ячейки меньше минимума биржи, трейлинг вниз выключен")
                self.trail_down = False
                return False
        try:
            api.cancel(self.symbol, oid)
        except BybitError:
            return False
        with self.lock:
            self.orders.pop(oid, None)
        cell = self.cells[ktop]
        sold, money = self._chase(api, "Sell", o["qty"], reduce_only=(self.cat == "linear"))
        got = money * (1 - self.fee)
        loss = float(got - cell["cost"] * sold / o["qty"]) if sold else 0.0
        self.trail_pnl += loss
        self.dn_losses.append((time.time(), loss))
        self.profit += loss
        pnl_book(self.gid, loss)
        left = q(o["qty"] - sold, self.bstep)
        if left >= self.min_qty:   # продалось не всё — сдвига нет, остаток ждёт своей продажи на месте
            cell["cost"] = cell["cost"] * left / o["qty"]
            self._put(api, cell, "Sell", left)
            self.say("info", f"сдвиг вниз: лимиткой продано {sold} из {o['qty']}, по рынку не продаю — сетка осталась на месте",
                     round(loss, 4) if sold else None)
            return False
        with self.lock:
            self.cells.pop(ktop, None)
        with self.lock:
            self.kmin -= 1
        k = self.kmin
        if self.geo:   # новая покупка рядом с ценой — самый маленький лот, остаток денег остаётся свободным
            qty = q(self.per_min / self.lv(k), self.bstep)
        else:
            qty = cell["qty"] if self.cat == "linear" else q(got / self.lv(k), self.bstep)
        cell = {"i": k, "qty": qty, "cost": qty * self.lv(k) * (1 + self.fee)}
        self.cells[k] = cell
        self._put(api, cell, "Buy", qty)
        self.shift_dn += 1
        self.fills.append({"t": int(time.time() * 1000), "p": float(p), "k": "sell"})
        self.say("info", f"трейлинг ↓ сетка сдвинута: {self.lv(self.kmin)} – {self.lv(self.kmin + self.n - 1)}",
                 round(loss, 4))
        return True

    def _chase(self, api, side, qty, reduce_only=False, tries=CHASE_TRIES):
        """Быстрая лимитка: Post-Only по лучшей цене стакана, переставляется за ценой. Только мейкер —
        по рынку не добирает: что не исполнилось, то не исполнилось.
        Возвращает (исполнено монет, потрачено/получено в USDT)."""
        sym, done, money = self.symbol, Decimal(0), Decimal(0)
        for _ in range(tries):
            left = q(qty - done, self.bstep)
            if left < self.min_qty:
                break
            _, bid, ask = api.bbo(sym)
            px = bid if side == "Buy" else ask
            if left * px < self.min_amt:
                break
            try:
                oid = api.place(sym, side, left, px, reduce_only=reduce_only)
            except BybitError as e:
                self.say("err", "быстрая лимитка: " + str(e))
                break
            time.sleep(CHASE_WAIT)
            try:
                api.cancel(sym, oid)
            except BybitError:
                pass  # уже исполнилась или отклонена
            h = None
            for _ in range(3):
                h = api.order(sym, oid)
                if h and h["orderStatus"] not in ("New", "PartiallyFilled", "Untriggered"):
                    break
                time.sleep(0.3)
            f = Decimal(h.get("cumExecQty") or "0") if h else Decimal(0)
            if f > 0:
                done += f
                money += f * (Decimal(h.get("avgPrice") or "0") or px)
                self.maker_n += 1
        left = q(qty - done, self.bstep)
        if left >= self.min_qty and done < qty:
            self.say("info", f"лимитка исполнилась не вся: {done} из {qty} {self.base_coin}, по рынку не добираю")
        return done, money

    def _put(self, api, cell, side, qty):
        i = cell["i"]
        price = self.lv(i + 1) if side == "Sell" else self.lv(i)
        # если уровень уже «за ценой», не берём ликвидность: ставим по лучшей цене стакана (мейкер)
        if side == "Buy" and self.ask and price >= self.ask:
            price = self.bid
        elif side == "Sell" and self.bid and price <= self.bid:
            price = self.ask
        oid = api.place(self.symbol, side, qty, price, reduce_only=(self.short and side == "Buy"))
        with self.lock:
            self.orders[oid] = {"cell": cell, "side": side, "qty": qty, "price": price}

    def _arm_short(self):
        """Шорт-сетка: каждая свободная ячейка, у которой линия Sell выше цены, получает Sell-лимитку."""
        busy = {o["cell"]["i"] for o in self.orders.values()} | set(self.paused)
        for k in sorted(self.cells):
            c = self.cells[k]
            if k in busy or c.get("pos"):
                continue
            if self.ask is None or self.lv(k + 1) > self.ask:
                self._put(self._api, c, "Sell", c["qty"])

    def _tick(self, api):
        self._api = api
        p, self.bid, self.ask = api.bbo(self.symbol)
        with self.lock:
            self.price = p
            self.hist.append({"t": int(time.time() * 1000), "p": float(p)})
            del self.hist[:-1500]
        e, ref = self.emerg, self.ref
        up_hit, dn_hit = p >= ref * (1 + e), p <= ref * (1 - e)
        if (self.short and up_hit) or (self.neutral and (up_hit or dn_hit)) or (not self.short and not self.neutral and dn_hit):
            self.emergency = True
            self.error = (f"Аварийный стоп: цена {p} ушла на {float(e * 100):g}% от цены запуска {ref}. "
                          f"Ордера сняты, позиция закрыта по рынку.")
            self.running = False
            return
        if self.cat == "linear":
            self.pos = api.position(self.symbol)
            liq = Decimal(self.pos.get("liqPrice") or "0") if self.pos else Decimal(0)
            if liq > 0:
                entry = Decimal(self.pos.get("avgPrice") or p)
                long_ = self.pos.get("side") == "Buy"
                left = (p - liq) / (entry - liq) if long_ and entry != liq else \
                       (liq - p) / (liq - entry) if (not long_) and liq != entry else Decimal(1)
                if left < LIQ_GUARD:
                    self.emergency = True
                    self.error = (f"Цена {p} близко к ликвидации ({liq}). Позиция закрыта по рынку, бот остановлен.")
                    self.running = False
                    return
        open_ids = api.open_ids(self.symbol)
        for oid, o in list(self.orders.items()):
            if oid in open_ids:
                continue
            h = api.order(self.symbol, oid)
            if not h:
                continue
            st = h["orderStatus"]
            if st not in ("Filled", "Cancelled", "Rejected", "Deactivated", "PartiallyFilledCanceled"):
                continue
            with self.lock:
                self.orders.pop(oid, None)
            c = o["cell"]
            filled = Decimal(h.get("cumExecQty") or "0")
            avg = Decimal(h.get("avgPrice") or "0") or o["price"]
            if st == "Filled" or (st == "PartiallyFilledCanceled" and filled > 0 and o["side"] == "Sell"):
                self._on_fill(api, c, o, filled, avg)
            else:
                if "PostOnly" not in str(h.get("rejectReason", "")) + str(h.get("cancelType", "")):
                    self.say("info", f"ордер {o['side']} {o['price']} отменён вне бота, выставляю заново")
                if c["i"] in self.cells:
                    self._put(api, c, o["side"], o["qty"])
        if self.short and self.guard != "pump":
            self._arm_short()
        if self.radar and self._guard(api, p) == "exit":
            return
        if not self.guard:
            self._trail(api, p)

    # ---------- ранняя защита ----------
    def _guard(self, api, p):
        try:
            m = self.radar.update()
        except BybitError as e:
            self.radar_errs = getattr(self, "radar_errs", 0) + 1
            if self.radar_errs in (1, 20) or self.radar_errs % 200 == 0:
                self.say("err", f"радар без данных ({self.radar_errs} раз): {e}")
            return
        self.radar_errs = 0
        if not m.get("warm"):
            return
        alert, calm = RADAR_SENS[self.radar_sens]
        why = lambda: (f"принты ×{m['speed_x']}, объём ×{m['burst']}, дельта {m['delta']:+.2f}, "
                       f"цена 60с {m['move60']:+.2f}%, стакан {m['imb']:+.2f}")
        if self.guard is None:
            if m["dump"] >= alert and not self.short:
                self._pause(api, "dump", "Buy", f"ранний сигнал ДАМПА ({m['dump']}): " + why())
            elif m["pump"] >= alert:
                self._pause(api, "pump", "Sell", f"ранний сигнал ПАМПА ({m['pump']}): " + why())
            return
        if self.guard == "pump" and self.short and self.radar_mode == "exit" and m["pump"] >= RADAR_EXIT:
            self.error = f"Выход по радару: сильный памп против шорта ({m['pump']}). Позиция закрывается лимитками. " + why()
            self.running = False
            return "exit"
        if self.guard == "dump" and self.radar_mode == "exit" and m["dump"] >= RADAR_EXIT:
            self.error = f"Выход по радару: сильный дамп ({m['dump']}). Позиция закрывается лимитками. " + why()
            self.exit_limit = True
            self.running = False
            return "exit"
        score = m["dump"] if self.guard == "dump" else m["pump"]
        self.calm = self.calm + 1 if score < calm else 0
        if self.calm >= RADAR_CALM_TICKS:
            self._resume(api, "рынок успокоился")
        elif time.time() - self.guard_t > RADAR_MAX_PAUSE:
            self._resume(api, "пауза длится 30 минут")

    def _pause(self, api, kind, side, text):
        n = 0
        for oid, o in list(self.orders.items()):
            if o["side"] != side:
                continue
            try:
                api.cancel(self.symbol, oid)
            except BybitError:
                continue  # успел исполниться — обработаем как обычно
            with self.lock:
                self.orders.pop(oid, None)
                self.paused[o["cell"]["i"]] = {"cell": o["cell"], "side": side, "sell_qty": o["qty"]}
            n += 1
        self.guard, self.guard_t, self.calm = kind, time.time(), 0
        what = "покупки" if side == "Buy" else "продажи"
        self.say("err", f"{text}. Сняты {what}: {n}")

    def _resume(self, api, why):
        if self.short:   # шорт: Sell вернутся только на линии выше цены, без продажи «по рынку» после пампа
            with self.lock:
                self.paused = {}
            before = len(self.orders)
            self._arm_short()
            self.say("info", f"{why}, защита снята, вернул Sell-лимиток: {len(self.orders) - before}")
            self.guard, self.calm = None, 0
            return
        n = 0
        for k, c in sorted(self.paused.items()):
            if k not in self.cells:
                continue
            self._put(api, c["cell"], c["side"], c["sell_qty"])
            n += 1
        with self.lock:
            self.paused = {}
        self.say("info", f"{why}, защита снята, вернул ордеров: {n}")
        self.guard, self.calm = None, 0

    def _on_fill(self, api, c, o, filled, avg):
        self.trades += 1
        self.maker_n += 1
        t = int(time.time() * 1000)
        if self.short:
            if o["side"] == "Sell":      # открылся шорт — ставим тейк линией ниже
                c["pos"] = filled
                c["entry"] = filled * avg * (1 - self.fee)
                self.fills.append({"t": t, "p": float(avg), "k": "sell"})
                self.say("sell", f"шорт {filled} {self.base_coin} по {avg}")
                self._put(api, c, "Buy", filled)
            else:                        # тейк исполнился — фиксируем прибыль и снова Sell на той же линии
                pr = float(c.get("entry", filled * avg) - filled * avg * (1 + self.fee))
                c["pos"] = Decimal(0)
                self.profit += pr
                pnl_book(self.gid, pr)
                self.fills.append({"t": t, "p": float(avg), "k": "buy"})
                self.say("buy", f"тейк {filled} {self.base_coin} по {avg}", round(pr, 4))
                if c["i"] in self.cells and (self.ask is None or self.lv(c["i"] + 1) > self.ask):
                    self._put(api, c, "Sell", c["qty"])
            return
        if c.get("mode") == "short":     # нейтральная сетка, верхняя ячейка: Sell открывает шорт, Buy закрывает
            if o["side"] == "Sell":
                c["entry"] = filled * avg * (1 - self.fee)
                self.fills.append({"t": t, "p": float(avg), "k": "sell"})
                self.say("sell", f"шорт {filled} {self.base_coin} по {avg}")
                self._put(api, c, "Buy", filled)
            else:
                pr = float(c.get("entry", filled * avg) - filled * avg * (1 + self.fee))
                self.profit += pr
                pnl_book(self.gid, pr)
                self.fills.append({"t": t, "p": float(avg), "k": "buy"})
                self.say("buy", f"тейк шорта {filled} {self.base_coin} по {avg}", round(pr, 4))
                self._put(api, c, "Sell", c["qty"])
            return
        if o["side"] == "Buy":
            c["cost"] = filled * avg * (1 + self.fee)
            self.fills.append({"t": t, "p": float(avg), "k": "buy"})
            self.say("buy", f"{filled} {self.base_coin} по {avg}")
            self._put(api, c, "Sell", q(filled * (1 - self.fee_buf), self.bstep))
        else:
            got = filled * avg * (1 - self.fee)
            pr = float(got - c["cost"])
            self.profit += pr
            pnl_book(self.gid, pr)
            self.fills.append({"t": t, "p": float(avg), "k": "sell"})
            self.say("sell", f"{filled} {self.base_coin} по {avg}", round(pr, 4))
            self._put(api, c, "Buy", c["qty"])


# ---------- поиск самых волатильных монет за последние 30 минут ----------
STABLES = {"USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDD", "PYUSD", "BUSD", "EUR", "USDR", "USD1", "RLUSD"}
SCAN_TOP = 80              # сколько самых ликвидных пар проверять
SCAN_MIN_TURNOVER = 3e6    # минимум оборота за 24 часа, USDT: в пустой монете сетка не работает
_scan_cache = {}


def scan_volatile(market="spot", minutes=30, symbols=None):
    cat = "linear" if market in ("futures", "linear") else "spot"
    key = (cat, minutes, tuple(symbols) if symbols else None)
    hit = _scan_cache.get(key)
    if hit and time.time() - hit[0] < 20:
        return hit[1]
    pub = Bybit("", "", "mainnet", cat)  # рыночные данные всегда с основной биржи
    tick = pub._req("GET", "/v5/market/tickers", {"category": cat}).get("list", [])
    pool = []
    for t in tick:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT") or sym[:-4] in STABLES or "-" in sym:
            continue
        try:
            turn = float(t.get("turnover24h") or 0)
        except ValueError:
            continue
        if turn >= SCAN_MIN_TURNOVER:
            pool.append((turn, sym))
    pool = [s_ for _, s_ in sorted(pool, reverse=True)[:SCAN_TOP]]
    if symbols:                        # свой список монет: проверяем только его
        have = {t.get("symbol", "") for t in tick}
        pool = [x for x in symbols if x in have]
    turnover = {t["symbol"]: float(t.get("turnover24h") or 0) for t in tick}

    def one(sym):
        try:
            k = pub._req("GET", "/v5/market/kline", {"category": cat, "symbol": sym,
                                                    "interval": "1", "limit": minutes}).get("list", [])
        except BybitError:
            return None
        if len(k) < minutes * 0.8:
            return None
        k = [(float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[6])) for x in reversed(k)]
        o0, close = k[0][0], k[-1][3]
        hi, lo = max(x[1] for x in k), min(x[2] for x in k)
        if lo <= 0 or o0 <= 0:
            return None
        rng = (hi / lo - 1) * 100                                  # размах за 30 минут
        candle = sum((x[1] / x[2] - 1) * 100 for x in k) / len(k)  # средняя минутная свеча
        chg = (close / o0 - 1) * 100                               # куда ушла цена
        path = sum(abs(x[3] / x[0] - 1) * 100 for x in k)          # пройденный путь
        chop = path / abs(chg) if abs(chg) > 0.05 else path / 0.05 # пила: путь / итоговое движение
        turn30 = sum(x[4] for x in k)
        trend = abs(chg) >= rng * 0.7
        kind = ("памп" if chg > 0 else "дамп") if trend and rng >= 2 else ("тренд" if trend else "пила")
        # подсказка для сетки: диапазон чуть шире размаха, шаг не меньше 0.4% (иначе съест комиссия)
        # по тесту на истории (полгода, 8 монет, окна 24 ч): узкие сетки ±0.5–1% с частыми уровнями в среднем
        # убыточны, лучше всего держится ±5% и 4 уровня (шаг ~3.4%), для спокойной монеты ±3%
        rp = 5.0 if rng >= 1.5 else 3.0
        lv = 4
        return {"symbol": sym, "range": round(rng, 2), "candle": round(candle, 3), "change": round(chg, 2),
                "chop": round(min(chop, 99), 1), "turn30": round(turn30), "turn24": round(turnover.get(sym, 0)),
                "price": close, "kind": kind, "grid_ok": kind == "пила" and rng >= 1,
                "sugg_range": rp, "sugg_levels": lv}

    with ThreadPoolExecutor(max_workers=10) as ex:
        rows = [r for r in ex.map(one, pool) if r]
    rows.sort(key=lambda r: r["range"], reverse=True)
    res = {"market": "futures" if cat == "linear" else "spot", "minutes": minutes,
           "checked": len(pool), "t": time.strftime("%H:%M:%S"), "rows": rows[:20]}
    _scan_cache[key] = (time.time(), res)
    return res


BOTS = {}
BOTS_LOCK = threading.Lock()

# ---- История результата по времени: «сколько заработала сетка за последний час» ----
# Каждая зафиксированная прибыль/убыток пишется с отметкой времени, а раз в минуту — снимок плавающего
# результата открытых позиций. За час = зафиксировано за час + (в позиции сейчас − в позиции час назад).
# Хранится в файле рядом с настройками, поэтому переживает перезапуск сервера.
PNL_PATH = os.path.join(DATA_DIR, "gridbot_pnl_history.json")
PNL_KEEP = 2 * 86400            # храним двое суток
PNL_SNAP_SEC = 60               # снимок «в позиции» раз в минуту
PNL_LOCK = threading.Lock()
PNL_EV = {}                     # gid -> [[ts, сумма], …] зафиксированные результаты
PNL_UP = {}                     # gid -> [[ts, в позиции], …] снимки раз в минуту


def pnl_load():
    global PNL_EV, PNL_UP
    try:
        with open(PNL_PATH, encoding="utf-8") as f:
            d = json.load(f)
        PNL_EV, PNL_UP = d.get("ev", {}) or {}, d.get("up", {}) or {}
    except Exception:
        PNL_EV, PNL_UP = {}, {}


def pnl_save():
    try:
        with PNL_LOCK:
            data = json.dumps({"ev": PNL_EV, "up": PNL_UP})
        tmp = PNL_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(data)
        os.replace(tmp, PNL_PATH)
    except Exception as e:
        print("не удалось сохранить историю результата:", e, flush=True)


def pnl_book(gid, amount):
    """Записать зафиксированный результат сетки (USDT) с текущим временем."""
    if not amount:
        return
    with PNL_LOCK:
        PNL_EV.setdefault(str(gid), []).append([round(time.time(), 1), round(float(amount), 6)])


def pnl_snap_loop():
    while True:
        time.sleep(PNL_SNAP_SEC)
        try:
            now = time.time()
            with BOTS_LOCK:
                items = list(BOTS.items())
            ups = {gid: b.unrealized() for gid, b in items}
            with PNL_LOCK:
                for gid, up in ups.items():
                    PNL_UP.setdefault(str(gid), []).append([round(now, 1), round(up, 6)])
                for d in (PNL_EV, PNL_UP):
                    for gid in list(d):
                        d[gid] = [x for x in d[gid] if x[0] >= now - PNL_KEEP]
                        if not d[gid]:
                            del d[gid]
            pnl_save()
        except Exception as e:
            print("история результата:", e, flush=True)


def pnl_window(gid, up_now, sec=3600):
    """Результат сетки за последние sec секунд: (итог, зафиксировано, изменение в позиции)."""
    since = time.time() - sec
    gid = str(gid)
    with PNL_LOCK:
        real = sum(a for t, a in PNL_EV.get(gid, []) if t >= since)
        snaps = PNL_UP.get(gid, [])
        base = next((u for t, u in snaps if t >= since), None)
        older = [u for t, u in snaps if t < since]
    if older:                 # есть снимок старше окна — берём последний перед началом часа
        base = older[-1]
    if base is None:          # сетка моложе часа или снимков ещё нет — считаем от нуля
        base = 0.0
    dup = up_now - base
    return real + dup, real, dup



def bot(gid):
    with BOTS_LOCK:
        if gid not in BOTS:
            BOTS[gid] = Bot(gid)
        return BOTS[gid]


def grids_summary():
    st = load_store()
    out = []
    for gid in sorted(st["grids"], key=int):
        g, b = st["grids"][gid], BOTS.get(gid)
        up = b.unrealized() if b else 0.0
        live = b is not None and (b.running or b.busy or b.symbol)
        out.append({"id": gid, "symbol": (b.symbol if live and b.symbol else g.get("symbol", "")).upper(),
                    "market": g.get("market", "spot"), "leverage": g.get("leverage", 1),
                    "direction": g.get("direction", "both"),
                    "running": bool(b and b.running), "busy": bool(b and b.busy),
                    "status": b.status if b else "Остановлен", "error": b.error if b else "",
                    "profit": b.profit if b else 0.0, "trades": b.trades if b else 0,
                    "upnl": up, "total": (b.profit if b else 0.0) + up,
                    "capital": float(b.cap) if b and b.cap else float(g.get("capital") or 0)})
        h, hr, hu = pnl_window(gid, up)
        out[-1].update(h1=h, h1_real=hr, h1_up=hu, auto=bool(g.get("auto")),
                       auto_left=max(0, int(float(g.get("auto_t") or 0) + auto_cfg(st)["hold_min"] * 60 - time.time()))
                       if g.get("auto") else 0)
    return out


def free_funds(market):
    """Свободные деньги на счёте для новых сеток: USDT на споте или свободная маржа на фьючерсах."""
    st = load_store()
    if not st["api_key"] or not st["api_secret"]:
        raise BybitError("Введите API ключ и секрет в настройках")
    api = Bybit(st["api_key"], st["api_secret"], st["network"], "linear" if market == "futures" else "spot")
    api.sync_time()
    bal = api.balances()
    if market == "futures":
        return api, bal.get("_margin", Decimal(0)), CAP_MAX_FUT
    return api, bal.get("USDT", {}).get("free", Decimal(0)), CAP_MAX_SPOT


def split_start(body):
    """Поделить свободный депозит поровну между выбранными монетами и запустить по сетке на каждую."""
    market = "futures" if body.get("market") != "spot" else "spot"
    cat = "linear" if market == "futures" else "spot"
    lev = int(body.get("leverage") or 2) if market == "futures" else 1
    if not 1 <= lev <= 100:
        raise BybitError("Плечо должно быть от 1 до 100")
    sugg = body.get("sugg") or {}
    syms = []
    for x in body.get("symbols") or []:
        x = str(x).upper().strip().replace("/", "")
        if x and not x.endswith("USDT"):
            x += "USDT"
        if x and x not in syms:
            syms.append(x)
    if not syms:
        raise BybitError("Выберите хотя бы одну монету")
    busy = {b.symbol for b in BOTS.values() if (b.running or b.busy) and b.cat == cat}
    taken = [x for x in syms if x in busy]
    if taken:
        raise BybitError("Уже торгуются: " + ", ".join(taken) + ". Уберите их из списка или остановите эти сетки.")
    api, free, k = free_funds(market)
    bad = []
    for x in syms:
        try:
            api.instrument(x)
        except BybitError:
            bad.append(x)
    if bad:
        raise BybitError("На Bybit нет такой пары (" + ("фьючерсы" if cat == "linear" else "спот") + "): " + ", ".join(bad))
    per = (free * k / len(syms)).quantize(Decimal("0.01"), ROUND_DOWN)
    if per < 10:
        raise BybitError(f"Свободно {free:.2f} USDT: на {len(syms)} сеток выходит по {per} USDT, это слишком мало. "
                         f"Уберите часть монет или пополните счёт.")
    ids = []
    with CFG_LOCK:
        st = load_store()
        tpl = st["grids"].get(str(body.get("copy_from") or ""), {})
        for x in syms:
            gid = next((g for g, c in st["grids"].items()
                        if str(c.get("symbol", "")).upper() == x and c.get("market") == market
                        and not (BOTS.get(g) and (BOTS[g].running or BOTS[g].busy))), None)
            if gid is None:
                gid = new_grid_id(st)
                st["grids"][gid] = dict({k2: DEFAULT_CFG[k2] for k2 in GRID_KEYS},
                                        **{k2: v for k2, v in tpl.items() if k2 in GRID_KEYS}, was_running=False)
            g = st["grids"][gid]
            g.update(symbol=x, market=market, leverage=lev, capital=float(per), cap_max=False,
                     trail_up=True, trail_down=True)
            g["direction"] = "neutral" if market == "futures" else "both"
            sg = sugg.get(x) or {}
            if sg.get("range"):
                g["range_pct"] = sg["range"]
            if sg.get("levels"):
                g["levels"] = sg["levels"]
            ids.append(gid)
        save_store(st)
    for gid in ids:
        bot(gid).start(grid_cfg(gid))
    return {"ok": True, "ids": ids, "per": float(per), "free": float(free), "leverage": lev}


# ---------- Авторежим 24/7 ----------
# Раз в минуту: сетки, которые отработали hold_min, закрываются; на свободные места берутся самые
# волатильные за последние `minutes` минут монеты из списка (пустой список — самые ликвидные пары биржи).
# Монета, которую только что держали, не берётся снова, пока не пройдёт тот же срок — так монеты меняются.
AUTO_DEFAULT = {"enabled": False, "coins": "", "slots": 3, "hold_min": 120, "minutes": 30,
                "leverage": 2, "max_leverage": 10, "use_pct": 90, "skip_trend": True, "market": "futures"}
AUTO_TICK = 60
AUTO_LOCK = threading.Lock()
AUTO_LOG = []
AUTO_STATE = {"next": 0.0, "last_scan": "", "msg": ""}


def auto_cfg(st=None):
    st = st or load_store()
    a = dict(AUTO_DEFAULT, **{k: v for k, v in (st.get("auto") or {}).items() if k in AUTO_DEFAULT})
    a["slots"] = max(1, min(10, int(a.get("slots") or 3)))
    a["hold_min"] = max(15, min(24 * 60, int(a.get("hold_min") or 120)))
    a["minutes"] = max(5, min(240, int(a.get("minutes") or 30)))
    a["leverage"] = max(1, min(20, int(a.get("leverage") or 2)))
    a["max_leverage"] = max(a["leverage"], min(25, int(a.get("max_leverage") or 10)))
    a["use_pct"] = max(10, min(95, int(a.get("use_pct") or 90)))
    a["market"] = "spot" if a.get("market") == "spot" else "futures"
    return a


def auto_coins(a):
    out = []
    for x in str(a.get("coins") or "").replace(";", ",").replace("\n", ",").replace(" ", ",").split(","):
        x = x.upper().strip().replace("/", "")
        if x and not x.endswith("USDT"):
            x += "USDT"
        if x and x not in out:
            out.append(x)
    return out


def auto_say(text):
    with AUTO_LOCK:
        AUTO_LOG.insert(0, {"t": time.strftime("%H:%M"), "text": text})
        del AUTO_LOG[60:]
    print(time.strftime("%H:%M:%S"), "[авто]", text, flush=True)


def auto_recent(st):
    return (st.get("auto") or {}).setdefault("recent", {})


def lot_share(levels, mult, neutral=True):
    """Доля самого маленького лота от объёма сетки — та же раскладка, что в Bot._setup."""
    n = int(levels)
    if neutral and n % 2 == 0:
        n += 1
    mult = min(max(Decimal(str(mult)), Decimal("1.01")), Decimal(3))
    if neutral:
        m = (n - 1) // 2 - 1
        dist = {i: (m - i) if i <= m else (i - m - 1) for i in range(n - 1)}
        w = {i: mult ** dist[i] for i in dist}
        wb = sum(w[i] for i in w if i <= m)
        ws = sum(w[i] for i in w if i > m)
        return min(Decimal(1) / 2 * w[i] / (wb if i <= m else ws) for i in w)
    w = [mult ** abs(i) for i in range(n - 1)]
    return min(w) / sum(w)


def auto_fit_lev(sym, per, row, a):
    """Плечо для авто-сетки: начинаем с a["leverage"] и поднимаем по 1x, пока самый маленький лот
    не пройдёт минимум ордера Bybit. None — не влезает даже на максимальном плече."""
    if a["market"] != "futures":
        return 1
    inst = Bybit("", "", "mainnet", "linear").instrument(sym)
    min_amt = Decimal(inst["lotSizeFilter"].get("minNotionalValue") or "5")
    max_lev = min(int(Decimal(inst.get("leverageFilter", {}).get("maxLeverage") or "100")), a["max_leverage"])
    share = lot_share(row.get("sugg_levels") or 4, DEFAULT_CFG["lot_mult"])
    need = min_amt * Decimal("1.05") * Decimal("1.03")      # запас как в сетке + округление количества
    for lev in range(a["leverage"], max_lev + 1):
        if per * lev * share >= need:
            return lev
    return None


def auto_launch(row, per, a, now, lev=None):
    sym = row["symbol"]
    with CFG_LOCK:
        st = load_store()
        gid = next((g for g, c in sorted(st["grids"].items(), key=lambda x: int(x[0]))
                    if c.get("auto") and not (BOTS.get(g) and (BOTS[g].running or BOTS[g].busy))), None)
        if gid is None:
            gid = new_grid_id(st)
        fut = a["market"] == "futures"
        g = dict({k: DEFAULT_CFG[k] for k in GRID_KEYS})
        g.update(symbol=sym, market=a["market"], leverage=(lev or a["leverage"]) if fut else 1,
                 capital=float(per), cap_max=False, direction="neutral" if fut else "both",
                 trail_up=True, trail_down=True, range_pct=row.get("sugg_range") or 5,
                 levels=row.get("sugg_levels") or 4, auto=True, auto_t=now, was_running=False)
        st["grids"][gid] = g
        save_store(st)
    auto_say(f"беру {sym[:-4]}: размах за {a['minutes']} мин {row['range']:.1f}%, {row['kind']}, "
             f"{per:.2f} USDT, плечо {g['leverage']}x" + (f" (на {a['leverage']}x лоты меньше минимума Bybit — поднял)"
             if fut and g["leverage"] > a["leverage"] else "") + f" — держу до {a['hold_min'] // 60} ч {a['hold_min'] % 60:02d} мин")
    bot(gid).start(grid_cfg(gid))


def auto_step(force=False):
    st = load_store()
    a = auto_cfg(st)
    if not a["enabled"]:
        AUTO_STATE["msg"] = "ручной режим"
        return
    if not (st.get("api_key") and st.get("api_secret")) or (ON_SERVER and not APP_PASSWORD):
        AUTO_STATE["msg"] = "нет API ключей или пароля APP_PASSWORD"
        return
    now = time.time()
    hold = a["hold_min"] * 60
    recent = auto_recent(st)
    changed = False
    autos = {gid: g for gid, g in st["grids"].items() if g.get("auto")}
    for gid, g in autos.items():
        b, sym = BOTS.get(gid), str(g.get("symbol", "")).upper()
        if b and b.running and now - float(g.get("auto_t") or now) >= hold:
            auto_say(f"{sym[:-4]}: отработала {a['hold_min'] // 60} ч {a['hold_min'] % 60:02d} мин, итог "
                     f"{b.profit + b.unrealized():+.2f} USDT — закрываю и меняю монету")
            recent[sym] = now
            changed = True
            b.stop()
        elif b and not b.running and not b.busy and b.error and sym and recent.get(sym, 0) < float(g.get("auto_t") or 0):
            auto_say(f"{sym[:-4]}: сетка остановилась с ошибкой — пропускаю эту монету")
            recent[sym] = now
            changed = True
    for sym in [x for x, t in recent.items() if now - t > hold]:
        del recent[sym]
        changed = True
    if changed:
        with CFG_LOCK:
            st2 = load_store()
            st2.setdefault("auto", {})["recent"] = recent
            save_store(st2)
    if any(BOTS.get(gid) and BOTS[gid].busy for gid in autos):
        AUTO_STATE["msg"] = "жду, пока сетки закроются или запустятся"
        return
    active = [gid for gid in autos if BOTS.get(gid) and BOTS[gid].running]
    free_slots = a["slots"] - len(active)
    if free_slots <= 0:
        AUTO_STATE["msg"] = f"все {a['slots']} места заняты"
        return
    coins = auto_coins(a)
    res = scan_volatile(a["market"], a["minutes"], symbols=coins or None)
    AUTO_STATE["last_scan"] = res.get("t", "")
    cat = "linear" if a["market"] == "futures" else "spot"
    busy = {b.symbol for b in BOTS.values() if (b.running or b.busy) and b.cat == cat}
    cands = [r for r in res["rows"] if r["symbol"] not in busy and r["symbol"] not in recent
             and not (a["skip_trend"] and r["kind"] in ("памп", "дамп"))]
    if not cands:
        AUTO_STATE["msg"] = "нет подходящих монет — проверю через минуту"
        return
    # Бюджет: в работе должно быть не больше use_pct% всего депозита (остальное — запас маржи).
    # Сейчас занято (депозит − свободно), значит на новые сетки можно ещё use_pct%·депозит − занято.
    api, free, k = free_funds(a["market"])
    if a["market"] == "futures":
        eq = api.balances().get("_equity") or free
        budget = min(free * Decimal("0.98"), eq * Decimal(a["use_pct"]) / 100 - (eq - free))
    else:
        eq, budget = free, free * k
    budget = max(budget, Decimal(0))
    launched = 0
    for slots in range(free_slots, 0, -1):     # не влезает на все места — пробуем меньше сеток, но крупнее
        per = (budget / slots).quantize(Decimal("0.01"), ROUND_DOWN)
        plan = []
        for r in cands:
            if len(plan) == slots:
                break
            try:
                lev = auto_fit_lev(r["symbol"], per, r, a)
            except BybitError:
                continue
            if lev:
                plan.append((r, lev))
        if len(plan) == slots:
            for r, lev in plan:
                auto_launch(r, per, a, now, lev)
            launched = slots
            break
    if not launched:
        msg = (f"депозит {eq:.2f} USDT, свободно {free:.2f}: до {a['use_pct']}% депозита осталось {budget:.2f} USDT — "
               f"не хватает даже на одну сетку при плече до {a['max_leverage']}x, жду")
        if AUTO_STATE["msg"] != msg:
            auto_say(msg)
        AUTO_STATE["msg"] = msg
        return
    AUTO_STATE["msg"] = "работаю" if launched == free_slots else f"работаю: денег хватило на {launched} из {free_slots} мест"


def auto_loop():
    while True:
        try:
            auto_step()
        except Exception as e:
            AUTO_STATE["msg"] = "ошибка: " + str(e)
            auto_say("ошибка авторежима: " + str(e))
        AUTO_STATE["next"] = time.time() + AUTO_TICK
        time.sleep(AUTO_TICK)


def auto_status():
    st = load_store()
    a = auto_cfg(st)
    now = time.time()
    grids = []
    for gid, g in sorted(st["grids"].items(), key=lambda x: int(x[0])):
        b = BOTS.get(gid)
        if g.get("auto") and b and (b.running or b.busy):
            grids.append({"id": gid, "symbol": str(g.get("symbol", "")).upper(),
                          "left": max(0, int(float(g.get("auto_t") or now) + a["hold_min"] * 60 - now)),
                          "total": b.profit + b.unrealized()})
    with AUTO_LOCK:
        log = list(AUTO_LOG[:15])
    return dict(a, recent=list((st.get("auto") or {}).get("recent", {}).keys()), grids=grids, log=log,
                msg=AUTO_STATE["msg"], last_scan=AUTO_STATE["last_scan"],
                next_in=max(0, int(AUTO_STATE["next"] - now)))


def auto_save(body):
    with CFG_LOCK:
        st = load_store()
        a = st.setdefault("auto", {})
        was = bool(a.get("enabled"))
        for k in AUTO_DEFAULT:
            if k in body and body[k] is not None:
                a[k] = body[k]
        a.update({k: v for k, v in auto_cfg(st).items() if k in AUTO_DEFAULT})
        save_store(st)
    if a["enabled"] != was:
        auto_say("включён авторежим 24/7" if a["enabled"] else "включён ручной режим — авто больше не меняет сетки")
    if a["enabled"]:
        threading.Thread(target=lambda: (auto_step(), AUTO_STATE.update(next=time.time() + AUTO_TICK)),
                         daemon=True).start()
    return auto_status()


def new_grid_id(st):
    return str(max([int(x) for x in st["grids"]] + [0]) + 1)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _auth_ok(self):
        if not APP_PASSWORD:
            return True
        h = self.headers.get("Authorization", "")
        if h.startswith("Basic "):
            try:
                _, _, pw = base64.b64decode(h[6:]).decode("utf-8").partition(":")
                return hmac.compare_digest(pw, APP_PASSWORD)
            except Exception:
                return False
        return False

    def _deny(self):
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Grid bot", charset="UTF-8"')
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _locked(self):
        """На сервере без пароля управлять ботом нельзя: страница была бы открыта всему интернету."""
        return ON_SERVER and not APP_PASSWORD

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        data = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/api/ping":
            return self._send(200, {"ok": True, "running": sum(b.running for b in BOTS.values())})
        if not self._auth_ok():
            return self._deny()
        if self.path == "/":
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")
        elif self.path.startswith("/api/state"):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            gid = qs.get("id", ["1"])[0]
            if gid not in load_store()["grids"]:
                return self._send(404, {"error": "нет такой сетки"})
            self._send(200, bot(gid).state())
        elif self.path == "/api/grids":
            self._send(200, grids_summary())
        elif self.path == "/api/auto":
            self._send(200, auto_status())
        elif self.path.startswith("/api/scan"):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:
                self._send(200, scan_volatile(qs.get("market", ["spot"])[0]))
            except BybitError as e:
                self._send(502, {"error": str(e)})
        elif self.path.startswith("/api/funds"):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            mk = qs.get("market", ["futures"])[0]
            try:
                _, free, k = free_funds(mk)
                self._send(200, {"free": float(free), "k": float(k)})
            except BybitError as e:
                self._send(502, {"error": str(e)})
        elif self.path == "/api/config":
            cfg = load_store()
            cfg["has_secret"] = bool(cfg.pop("api_secret"))
            cfg["env_keys"] = bool(ENV_KEY and ENV_SECRET)
            cfg["locked"] = self._locked()
            if cfg["env_keys"]:
                cfg["api_key"] = cfg["api_key"][:4] + "…" + cfg["api_key"][-3:]
            self._send(200, cfg)
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if not self._auth_ok():
            return self._deny()
        if self._locked():
            return self._send(403, {"error": "Задайте переменную APP_PASSWORD на Railway"})
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        if self.path in ("/api/start", "/api/save", "/api/new"):
            with CFG_LOCK:
                st = load_store()
                gid = str(body.get("id") or "")
                if self.path == "/api/save" and gid not in st["grids"]:
                    return self._send(404, {"error": "нет такой сетки"})
                if self.path == "/api/new" or gid not in st["grids"]:
                    gid = new_grid_id(st)
                    src = st["grids"].get(str(body.get("copy_from") or ""), {})
                    st["grids"][gid] = dict({k: DEFAULT_CFG[k] for k in GRID_KEYS},
                                            **{k: v for k, v in src.items() if k in GRID_KEYS}, was_running=False)
                g = st["grids"][gid]
                for k in DEFAULT_CFG:
                    v = body.get(k)
                    if v in (None, "") or (k == "api_secret" and v == "********"):
                        continue
                    if k in GLOBAL_KEYS:
                        if not (k in ("api_key", "api_secret") and ENV_KEY and ENV_SECRET):
                            st[k] = v
                    else:
                        g[k] = v
                if self.path == "/api/start":
                    g.pop("auto", None)
                    g.pop("auto_t", None)
                    sym, mk = str(g.get("symbol", "")).upper().strip(), g.get("market")
                    for og, ob in BOTS.items():
                        if og != gid and (ob.running or ob.busy) and ob.symbol == sym \
                                and ob.cat == ("linear" if mk == "futures" else "spot"):
                            return self._send(409, {"error": f"{sym} уже торгует сетка {og}. Две сетки на одной "
                                                             f"паре мешают друг другу: выберите другую монету.",
                                                    "id": gid})
                save_store(st)
            if self.path == "/api/start":
                bot(gid).start(grid_cfg(gid))
            self._send(200, {"ok": True, "id": gid})
        elif self.path == "/api/auto":
            try:
                self._send(200, auto_save(body))
            except (BybitError, ValueError) as e:
                self._send(400, {"error": str(e)})
        elif self.path == "/api/split":
            try:
                self._send(200, split_start(body))
            except BybitError as e:
                self._send(400, {"error": str(e)})
        elif self.path == "/api/stop_all":
            n = 0
            for b in list(BOTS.values()):
                if b.running:
                    b.stop()
                    n += 1
            self._send(200, {"ok": True, "stopped": n})
        elif self.path == "/api/stop":
            gid = str(body.get("id") or "1")
            if gid in BOTS:
                BOTS[gid].stop()
            self._send(200, {"ok": True})
        elif self.path == "/api/delete":
            gid = str(body.get("id") or "")
            b = BOTS.get(gid)
            if b and (b.running or b.busy):
                return self._send(409, {"error": "Сначала остановите сетку"})
            with CFG_LOCK:
                st = load_store()
                st["grids"].pop(gid, None)
                if not st["grids"]:
                    st["grids"]["1"] = dict({k: DEFAULT_CFG[k] for k in GRID_KEYS}, was_running=False)
                save_store(st)
            BOTS.pop(gid, None)
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": "not found"})


PAGE = r"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Сеточный бот Bybit</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Unbounded:wght@500;700&family=Onest:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap">
<style>
:root{--bg:#eef1ee;--panel:#f8faf8;--fg:#17201b;--muted:#5d6b63;--line:#d3dbd5;--buy:#1d8a59;--sell:#c2412d;--warn:#b7791f;--ink:#17201b;--ink-fg:#f8faf8;
--display:"Unbounded",system-ui,sans-serif;--body:"Onest",system-ui,sans-serif;--mono:"JetBrains Mono",ui-monospace,monospace}
@media (prefers-color-scheme:dark){:root{--bg:#111614;--panel:#171e1b;--fg:#e4ebe6;--muted:#8c9a92;--line:#2a3530;--buy:#3fbf85;--sell:#ec6a54;--warn:#e0a84a;--ink:#e4ebe6;--ink-fg:#111614;color-scheme:dark}}
*{box-sizing:border-box}[hidden]{display:none!important}
body{margin:0;background:var(--bg);color:var(--fg);font-family:var(--body);font-size:15px;line-height:1.5}
.wrap{max-width:860px;margin:0 auto;padding:28px 16px 48px;display:grid;gap:22px}
header{display:flex;justify-content:space-between;align-items:baseline;gap:12px;flex-wrap:wrap}
.pair{font-family:var(--display);font-weight:700;font-size:20px}
.tag{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}
.tag.live{color:var(--sell);font-weight:600}
.status{display:inline-flex;align-items:center;gap:8px;font-size:13px;color:var(--muted)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--muted)}
.on .dot{background:var(--buy);box-shadow:0 0 0 4px color-mix(in srgb,var(--buy) 20%,transparent)}
.hero{display:flex;justify-content:space-between;align-items:flex-end;gap:20px;flex-wrap:wrap}
.lbl{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}
.pnl{font-family:var(--display);font-weight:500;font-size:clamp(40px,9vw,68px);line-height:1;font-variant-numeric:tabular-nums}
.pnl.up{color:var(--buy)}.pnl.down{color:var(--sell)}
.price{font-family:var(--mono);font-size:22px;text-align:right;font-variant-numeric:tabular-nums}
.chart{background:var(--panel);border:1px solid var(--line);border-radius:6px;height:300px}
canvas{width:100%;height:100%;display:block}
#go{font-family:var(--display);font-weight:500;font-size:18px;border:0;border-radius:6px;padding:20px;background:var(--ink);color:var(--ink-fg);cursor:pointer}
#go.stop{background:transparent;color:var(--fg);box-shadow:inset 0 0 0 2px var(--fg)}
#go:disabled{opacity:.5;cursor:wait}
#go:focus-visible{outline:3px solid var(--buy);outline-offset:3px}
.err{border:1px solid var(--sell);color:var(--sell);border-radius:6px;padding:12px 14px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--line);border:1px solid var(--line);border-radius:6px;overflow:hidden}
.stat{background:var(--panel);padding:12px 14px;min-width:0}
.stat b{display:block;font-family:var(--mono);font-weight:500;font-size:17px;font-variant-numeric:tabular-nums;overflow-wrap:anywhere}
.stat span{font-size:12px;color:var(--muted)}
@media (max-width:560px){.stats{grid-template-columns:repeat(2,1fr)}}
details{background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:12px 14px}
summary{cursor:pointer;font-weight:600}
.form{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin-top:14px}
.form label{display:grid;gap:4px;font-size:12px;color:var(--muted);min-width:0}
.form .full{grid-column:1/-1}
input,select{font:inherit;font-family:var(--mono);font-size:14px;padding:9px 10px;border:1px solid var(--line);border-radius:4px;background:var(--bg);color:var(--fg);width:100%}
@media (max-width:560px){.form{grid-template-columns:1fr}}
.log{font-family:var(--mono);font-size:13px;max-height:260px;overflow:auto}
.log div{display:grid;grid-template-columns:70px 64px 1fr auto;gap:10px;padding:5px 0;border-bottom:1px solid var(--line)}
.b{color:var(--buy)}.s{color:var(--sell)}.i{color:var(--muted)}.e{color:var(--sell)}
.note{font-size:13px;color:var(--muted)}
.warn{font-size:13px;color:var(--warn);border-left:3px solid var(--warn);padding:6px 10px}
input[type=range]{padding:0;border:0;background:none;accent-color:var(--fg)}
#levval{font-family:var(--mono);color:var(--fg)}
.chk{display:flex!important;align-items:center;gap:8px;font-size:14px!important;color:var(--fg)!important}
.chk input{width:auto;accent-color:var(--fg)}
.gridline{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;font-family:var(--mono);font-size:13px;color:var(--muted);margin-top:-12px}
.radar{background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:14px;display:grid;gap:14px}
.rhead{display:flex;justify-content:space-between;align-items:center}
.pill{font-size:12px;font-weight:600;padding:3px 10px;border-radius:99px;background:var(--line);color:var(--muted)}
.pill.ok{background:color-mix(in srgb,var(--buy) 16%,transparent);color:var(--buy)}
.pill.warn{background:color-mix(in srgb,var(--warn) 18%,transparent);color:var(--warn)}
.pill.bad{background:var(--sell);color:var(--ink-fg)}
.bars{display:grid;grid-template-columns:1fr 1fr;gap:16px}
@media (max-width:560px){.bars{grid-template-columns:1fr}}
.bt{display:flex;justify-content:space-between;font-size:13px;color:var(--muted)}
.bt b{font-family:var(--mono);color:var(--fg)}
.track{position:relative;height:8px;border-radius:4px;background:var(--line);margin-top:6px}
.track i{position:absolute;left:0;top:0;bottom:0;border-radius:4px;width:0;transition:width .4s}
.track i.dn{background:var(--sell)}.track i.up{background:var(--buy)}
.track em{position:absolute;top:-3px;bottom:-3px;width:2px;background:var(--fg);opacity:.5}
.chips{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:8px 16px}
.chips div{min-width:0}.chips span{display:block;font-size:11px;color:var(--muted)}
.chips b{font-family:var(--mono);font-weight:500;font-size:14px;font-variant-numeric:tabular-nums}
@media (prefers-reduced-motion:reduce){.track i{transition:none}}
.posrow{display:none}.fut .posrow{display:grid}
.presetrow{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
#presetbtn{font:inherit;font-size:13px;font-weight:600;padding:8px 12px;border-radius:4px;border:0;background:var(--ink);color:var(--ink-fg);cursor:pointer}
.pnlsub{font-family:var(--mono);font-size:13px;color:var(--muted);margin-top:6px;font-variant-numeric:tabular-nums}
.seg{display:grid;grid-template-columns:1fr 1fr 1fr;gap:0;border-radius:6px;box-shadow:inset 0 0 0 1px var(--line);background:var(--panel);padding:3px}
.seg button{font:inherit;font-weight:600;font-size:14px;padding:10px;border:0;border-radius:4px;background:transparent;color:var(--muted);cursor:pointer}
.seg button[aria-checked="true"]{background:var(--ink);color:var(--ink-fg)}
.seg button[data-dir="sell"][aria-checked="true"]{background:var(--sell);color:#fff}
.seg button[data-dir="neutral"][aria-checked="true"]{background:var(--buy);color:#fff}
.seg button:disabled{cursor:not-allowed;opacity:.6}
.seg button:focus-visible{outline:3px solid var(--buy);outline-offset:2px}
.split{display:grid;gap:10px;padding:16px;border-radius:8px;background:var(--panel);box-shadow:inset 0 0 0 1px var(--line)}
.split .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.split input,.split select{font:inherit;font-size:14px;padding:8px 10px;border-radius:4px;border:1px solid var(--line);background:var(--bg);color:var(--fg)}
.split #spadd{flex:1;min-width:140px}
.split #splev{width:70px}
.chips{display:flex;gap:6px;flex-wrap:wrap;min-height:10px}
.chip{display:inline-flex;align-items:center;gap:6px;font-weight:600;font-size:14px;padding:6px 6px 6px 12px;border-radius:999px;background:var(--bg);box-shadow:inset 0 0 0 1px var(--line)}
.chip button{font:inherit;border:0;background:transparent;color:var(--muted);cursor:pointer;font-size:16px;line-height:1;padding:0 6px}
.split .btn2{font:inherit;font-weight:600;font-size:14px;padding:8px 14px;border-radius:4px;border:0;background:var(--ink);color:var(--ink-fg);cursor:pointer}
#spgo{font:inherit;font-weight:600;font-size:16px;padding:16px;border:0;border-radius:6px;background:var(--buy);color:#fff;cursor:pointer}
#spgo:disabled{opacity:.5;cursor:not-allowed}
.take.add{background:transparent;color:var(--fg);box-shadow:inset 0 0 0 1px var(--line);margin-left:4px}
#maxbtn{font:inherit;font-weight:600;font-size:15px;padding:14px;border:0;border-radius:6px;background:var(--panel);color:var(--fg);box-shadow:inset 0 0 0 1px var(--line);cursor:pointer}
#maxbtn[aria-pressed="true"]{background:var(--buy);color:#fff;box-shadow:none}
#maxbtn:disabled{cursor:not-allowed;opacity:.6}
#maxbtn:focus-visible{outline:3px solid var(--buy);outline-offset:2px}
#dirnote{margin-top:-14px}
.tabs{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.tab{font:inherit;font-size:13px;display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:6px;border:0;background:var(--panel);color:var(--fg);box-shadow:inset 0 0 0 1px var(--line);cursor:pointer}
.tab b{font-family:var(--mono);font-weight:500}
.tab .d{width:7px;height:7px;border-radius:50%;background:var(--muted)}
.tab.on .d{background:var(--buy)}.tab.err .d{background:var(--sell)}
.tab[aria-current="true"]{box-shadow:inset 0 0 0 2px var(--fg)}
.tab:focus-visible{outline:3px solid var(--buy);outline-offset:2px}
.tab.add{color:var(--muted)}
.tab b.b{color:var(--buy);font-weight:700}.tab b.s{color:var(--sell);font-weight:700}.tab b.z{color:var(--muted)}
.sumbar{display:flex;flex-wrap:wrap;align-items:center;gap:10px 22px;padding:14px 18px;border-radius:8px;background:var(--panel);box-shadow:inset 0 0 0 1px var(--line)}
.sumbar.up{box-shadow:inset 0 0 0 2px var(--buy);background:color-mix(in srgb,var(--buy) 7%,var(--panel))}
.sumbar.down{box-shadow:inset 0 0 0 2px var(--sell);background:color-mix(in srgb,var(--sell) 7%,var(--panel))}
.sumbar .lbl{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.sumbar .big{font-family:var(--mono);font-weight:700;font-size:clamp(30px,6vw,46px);line-height:1;font-variant-numeric:tabular-nums}
.sumbar .big.up{color:var(--buy)}.sumbar .big.down{color:var(--sell)}.sumbar .big.z{color:var(--fg)}
.sumbar .parts{display:flex;gap:18px;font-size:14px}.sumbar .parts b{font-family:var(--mono);font-size:17px}
.sumbar .run{font-size:13px;color:var(--muted)}
.sumbar .hour{padding-left:18px;border-left:1px solid var(--line)}
.sumbar .hour b{font-family:var(--mono);font-size:24px;font-weight:700}
.sumbar .hour .best{font-size:12px;color:var(--muted);margin-top:4px}
.tab .h{font-family:var(--mono);font-size:11px;padding:1px 5px;border-radius:3px}
.tab .h.up{color:var(--buy);background:color-mix(in srgb,var(--buy) 12%,transparent)}
.tab .h.down{color:var(--sell);background:color-mix(in srgb,var(--sell) 12%,transparent)}
.rt td.hr{font-weight:700}
.rt th.sort{cursor:pointer;text-decoration:underline dotted}
.rt th.sort[aria-sort]{color:var(--fg)}
.autobar{display:flex;flex-wrap:wrap;align-items:center;gap:10px 16px;padding:12px 16px;border-radius:8px;background:var(--panel);box-shadow:inset 0 0 0 1px var(--line)}
.autobar.on{box-shadow:inset 0 0 0 2px var(--buy)}
.mode{display:inline-flex;border-radius:6px;box-shadow:inset 0 0 0 1px var(--line);overflow:hidden}
.mode button{font:inherit;font-weight:600;font-size:14px;padding:10px 16px;border:0;background:transparent;color:var(--muted);cursor:pointer}
.mode button[aria-pressed="true"]{background:var(--ink);color:var(--ink-fg)}
.mode button.auto[aria-pressed="true"]{background:var(--buy);color:#fff}
.mode button:focus-visible{outline:3px solid var(--buy);outline-offset:-3px}
.autobar .amsg{font-size:13px;color:var(--muted)}
.autobar .achips{display:flex;flex-wrap:wrap;gap:6px}
.achip{font-family:var(--mono);font-size:12px;padding:4px 8px;border-radius:4px;background:color-mix(in srgb,var(--buy) 10%,transparent)}
#autoset{width:100%}
#autoset summary{cursor:pointer;font-size:13px;color:var(--muted)}
.aform{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-top:12px}
.aform label{display:flex;flex-direction:column;gap:4px;font-size:13px}
.aform .full{grid-column:1/-1}
.aform textarea,.aform input[type=number]{font:inherit;font-family:var(--mono);font-size:13px;padding:8px;border-radius:4px;border:1px solid var(--line);background:var(--bg);color:var(--fg)}
.aform .chk{flex-direction:row;align-items:center}
#asave{font:inherit;font-weight:600;white-space:nowrap;padding:10px 16px;border:0;border-radius:6px;background:var(--ink);color:var(--ink-fg);cursor:pointer;justify-self:start}
.alog{margin-top:10px;font-size:12px;color:var(--muted);max-height:140px;overflow:auto;font-family:var(--mono)}
.tab .h.auto{color:var(--ink-fg);background:var(--buy)}
#resbtn{margin-left:auto;font:inherit;font-weight:600;font-size:14px;padding:10px 16px;border-radius:6px;border:0;background:var(--ink);color:var(--ink-fg);cursor:pointer}
#resbtn:focus-visible{outline:3px solid var(--buy);outline-offset:2px}
#resdlg{border:0;border-radius:10px;padding:0;background:var(--bg);color:var(--fg);width:min(860px,96vw);max-height:90vh;box-shadow:0 20px 60px rgba(0,0,0,.35)}
#resdlg::backdrop{background:rgba(0,0,0,.45)}
#resdlg .hd{display:flex;align-items:center;justify-content:space-between;padding:16px 20px;border-bottom:1px solid var(--line);position:sticky;top:0;background:var(--bg)}
#resdlg .hd h2{margin:0;font-size:18px}
#resdlg .x{font:inherit;font-size:22px;line-height:1;border:0;background:transparent;color:var(--muted);cursor:pointer;padding:4px 8px}
#resdlg .bd{padding:8px 12px 16px;overflow:auto}
.rt{width:100%;border-collapse:collapse;font-size:14px;font-variant-numeric:tabular-nums}
.rt th{text-align:right;font-weight:600;font-size:12px;color:var(--muted);padding:8px;border-bottom:1px solid var(--line);white-space:nowrap}
.rt td{text-align:right;padding:10px 8px;border-bottom:1px solid var(--line);font-family:var(--mono);white-space:nowrap}
.rt th:first-child,.rt td:first-child{text-align:left;font-family:inherit}
.rt td.tot{font-weight:700;font-size:16px}
.rt tr.row{cursor:pointer}.rt tr.row:hover td{background:color-mix(in srgb,var(--fg) 5%,transparent)}
.rt tr.row.up td.tot{background:color-mix(in srgb,var(--buy) 12%,transparent)}
.rt tr.row.down td.tot{background:color-mix(in srgb,var(--sell) 12%,transparent)}
.rt tfoot td{font-weight:700;border-bottom:0;border-top:2px solid var(--fg);font-size:15px}
.rt .st{font-family:inherit;font-size:12px;color:var(--muted)}
.rt .st.on{color:var(--buy)}.rt .st.err{color:var(--sell)}
.rbar{height:6px;border-radius:3px;background:var(--line);overflow:hidden;min-width:60px}
.rbar i{display:block;height:100%}.rbar i.up{background:var(--buy)}.rbar i.down{background:var(--sell)}
.delrow{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-top:14px}
#delbtn{font:inherit;font-size:13px;padding:7px 12px;border-radius:4px;border:0;background:transparent;color:var(--sell);box-shadow:inset 0 0 0 1px var(--sell);cursor:pointer}
.killrow{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:-6px}
#killall{font:inherit;font-weight:600;font-size:14px;padding:10px 16px;border-radius:6px;border:0;background:transparent;color:var(--sell);box-shadow:inset 0 0 0 2px var(--sell);cursor:pointer}
#killall.sure{background:var(--sell);color:#fff}
#killall:disabled{opacity:.5;cursor:wait}
#killall:focus-visible{outline:3px solid var(--sell);outline-offset:2px}
#delbtn.sure{background:var(--sell);color:var(--ink-fg)}
#delbtn:disabled{opacity:.4;cursor:not-allowed}
.scan{display:grid;gap:12px}
.scanhead{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
#scanbtn{font:inherit;font-weight:600;font-size:15px;padding:12px 16px;border-radius:6px;border:0;background:var(--panel);color:var(--fg);box-shadow:inset 0 0 0 1px var(--line);cursor:pointer}
#scanbtn:hover{box-shadow:inset 0 0 0 1px var(--fg)}
#scanbtn:disabled{opacity:.6;cursor:wait}
#scanbtn:focus-visible{outline:3px solid var(--buy);outline-offset:2px}
.tbl{overflow-x:auto;border:1px solid var(--line);border-radius:6px;background:var(--panel)}
.tbl table{border-collapse:collapse;width:100%;font-size:13px;min-width:640px}
.tbl th{text-align:right;font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);padding:9px 10px;border-bottom:1px solid var(--line);white-space:nowrap}
.tbl td{text-align:right;padding:8px 10px;border-bottom:1px solid var(--line);font-family:var(--mono);font-variant-numeric:tabular-nums;white-space:nowrap}
.tbl th:first-child,.tbl td:first-child{text-align:left}
.tbl tr:last-child td{border-bottom:0}
.tbl td.sym{font-family:var(--body);font-weight:600}
.kind{font-family:var(--body);font-size:12px;font-weight:600;padding:2px 8px;border-radius:99px}
.kind.ok{background:color-mix(in srgb,var(--buy) 16%,transparent);color:var(--buy)}
.kind.bad{background:color-mix(in srgb,var(--sell) 14%,transparent);color:var(--sell)}
.kind.mid{background:color-mix(in srgb,var(--warn) 16%,transparent);color:var(--warn)}
.take{font:inherit;font-family:var(--body);font-size:12px;font-weight:600;padding:5px 10px;border-radius:4px;border:0;background:var(--ink);color:var(--ink-fg);cursor:pointer}
.take:disabled{opacity:.4;cursor:not-allowed}
</style></head><body>
<div class="wrap">
<section class="sumbar" id="sumbar" aria-live="polite"></section>
<section class="autobar" id="autobar">
<div class="mode" role="group" aria-label="Режим"><button type="button" id="m_man" aria-pressed="true">✋ Ручной</button><button type="button" class="auto" id="m_auto" aria-pressed="false">🤖 Авто 24/7</button></div>
<span class="amsg" id="amsg">—</span><div class="achips" id="achips"></div>
<details id="autoset"><summary>Настройки авторежима</summary>
<div class="aform">
<label class="full">Монеты для выбора (через запятую). Пусто — 80 самых ликвидных пар Bybit<textarea id="a_coins" rows="2" placeholder="SOL, DOGE, PEPE, WIF, SUI"></textarea></label>
<label>Сеток одновременно<input id="a_slots" type="number" min="1" max="10" step="1"></label>
<label>Держать монету, часов<input id="a_hold" type="number" min="0.25" max="24" step="0.25"></label>
<label>Волатильность за, минут<input id="a_min" type="number" min="5" max="240" step="5"></label>
<label>Плечо (начальное)<input id="a_lev" type="number" min="1" max="20" step="1"></label>
<label>Плечо максимум (если лоты не влезают)<input id="a_maxlev" type="number" min="1" max="25" step="1"></label>
<label>Использовать депозита, %<input id="a_use" type="number" min="10" max="95" step="5"></label>
<label class="chk full"><input type="checkbox" id="a_skip"> Пропускать монеты в сильном пампе/дампе (цена уходит в одну сторону — сетка там теряет)</label>
<button type="button" id="asave">Сохранить настройки</button>
</div>
<div class="note" style="margin-top:8px">Авто: раз в минуту бот закрывает сетки, которые отработали заданное время, и на свободные места ставит самые волатильные монеты из списка. Только что закрытую монету не берёт снова, пока не пройдёт то же время. В работе держит до заданного % всего депозита (по умолчанию 90%), остальное — запас маржи; деньги делятся поровну между свободными местами. Если на начальном плече самый маленький лот меньше минимума Bybit (5 USDT), плечо поднимается по 1x до максимума; не влезает и так — берёт меньше сеток, но крупнее. Ручной: авто ничего не меняет, сетки работают как есть.</div>
<div class="alog" id="alog"></div>
</details>
</section>
<nav class="tabs" id="tabs" aria-label="Сетки"></nav>
<dialog id="resdlg" aria-labelledby="resttl"><div class="hd"><h2 id="resttl">Результаты по сеткам</h2><button type="button" class="x" id="resx" aria-label="Закрыть">×</button></div><div class="bd" id="resbody"></div></dialog>
<div class="killrow" id="killrow" hidden><button type="button" id="killall">Закрыть все сетки</button><span class="note" id="killnote"></span></div>
<header><div><div class="pair" id="pair">—</div><div class="tag" id="net">—</div></div>
<div class="status" id="status"><span class="dot"></span><span id="statusText">Остановлен</span></div></header>
<section class="hero"><div><div class="lbl">Итог сетки</div><div class="pnl" id="pnl">+0.00</div>
<div class="pnlsub" id="pnlsub">зафиксировано +0,00 · в позиции +0,00</div></div>
<div class="price"><div class="lbl">Цена</div><span id="price">—</span></div></section>
<div class="chart"><canvas id="cv"></canvas></div>
<div class="gridline"><span id="grange">Сетка: —</span><span id="gtrail">Трейлинг: —</span><span id="gfee">Лимиток: 0 · маркетом: 0</span></div>
<section class="radar" id="radarbox">
<div class="rhead"><span class="lbl">Радар</span><span class="pill" id="rpill">выключен</span></div>
<div class="bars">
<div class="bar"><div class="bt"><span>Риск дампа</span><b id="rdv">—</b></div><div class="track"><i id="rd" class="dn"></i><em id="rth1"></em></div></div>
<div class="bar"><div class="bt"><span>Риск пампа</span><b id="rpv">—</b></div><div class="track"><i id="rp" class="up"></i><em id="rth2"></em></div></div>
</div>
<div class="chips">
<div><span>Принтов/с</span><b id="m_tps">—</b></div>
<div><span>Объём 30с</span><b id="m_burst">—</b></div>
<div><span>Дельта покуп./прод.</span><b id="m_delta">—</b></div>
<div><span>Цена 60с</span><b id="m_move">—</b></div>
<div><span>Стакан ±0.5%</span><b id="m_imb">—</b></div>
<div><span>Открытый интерес 10м</span><b id="m_oi">—</b></div>
<div><span>Фандинг</span><b id="m_fund">—</b></div>
</div>
</section>
<div class="err" id="err" hidden></div>
<div class="seg" role="radiogroup" aria-label="Какие лимитки ставить">
<button type="button" role="radio" data-dir="neutral">Нейтральная ½ Buy · ½ Sell</button>
<button type="button" role="radio" data-dir="both">Обе стороны (лонг)</button>
<button type="button" role="radio" data-dir="sell">Только Sell</button>
<select id="direction" hidden><option value="neutral">neutral</option><option value="both">both</option><option value="sell">sell</option></select>
</div>
<div class="note" id="dirnote"></div>
<button id="maxbtn" type="button" aria-pressed="false">Брать по максимуму на весь депозит</button>
<div class="note" id="maxnote"></div>
<button id="go" type="button">Запустить бота</button>
<section class="stats">
<div class="stat"><span>Сделок</span><b id="tc">0</b></div>
<div class="stat"><span>Ордеров в стакане</span><b id="oc">0</b></div>
<div class="stat"><span id="qlab">Свободно USDT</span><b id="qb">—</b></div>
<div class="stat"><span id="blab">Свободно монет</span><b id="bb">—</b></div>
</section>
<section class="stats posrow" id="posbox">
<div class="stat"><span>Позиция</span><b id="ps">—</b></div>
<div class="stat"><span>Вход</span><b id="pe">—</b></div>
<div class="stat"><span>Ликвидация</span><b id="pl" class="s">—</b></div>
<div class="stat"><span>Нереализ. PnL</span><b id="pu">—</b></div>
</section>
<details id="set"><summary>Настройки и API ключ</summary>
<div class="form">
<div class="full note" id="envkeys" hidden>API ключ взят из переменных сервера (BYBIT_API_KEY): <b id="envkeyval"></b></div>
<label class="full" id="kbox1">API ключ<input id="api_key" autocomplete="off"></label>
<label class="full" id="kbox2">API секрет<input id="api_secret" type="password" autocomplete="off"></label>
<label>Сеть<select id="network"><option value="testnet">Testnet (тест)</option><option value="demo">Demo trading</option><option value="mainnet">Mainnet (реальные деньги)</option></select></label>
<label>Рынок<select id="market"><option value="spot">Спот (без плеча)</option><option value="futures">Фьючерсы USDT (с плечом)</option></select></label>
<label>Пара<input id="symbol"></label>
<label id="levbox">Плечо: <b id="levval">1x</b><input id="leverage" type="range" min="1" max="100" step="1"></label>
<div class="full warn" id="levwarn" hidden></div>
<label>Ранняя защита от дампа/пампа<select id="radar"><option value="off">Выключена</option><option value="pause">Снимать ордера на время всплеска</option><option value="exit">Снимать ордера + аварийный выход при сильном дампе</option></select></label>
<label>Чувствительность<select id="radar_sens"><option value="low">Низкая (реже, только сильные всплески)</option><option value="mid">Средняя</option><option value="high">Высокая (раньше, больше ложных)</option></select></label>
<label class="chk"><input type="checkbox" id="trail_up"> <span id="tul">Трейлинг вверх</span></label>
<label class="chk"><input type="checkbox" id="trail_down"> <span id="tdl">Трейлинг вниз</span></label>
<label>Аварийный стоп по рынку: цена ушла от цены запуска на, %<input id="emerg_pct" type="number" min="1" max="50" step="0.5"></label>
<div class="full presetrow"><button type="button" id="presetbtn">Лучшие настройки по тесту на истории</button><span class="note">±5%, 4 уровня, лоты ×1.4, трейлинг вверх и вниз 0.5%, аварийный стоп 10%, плечо 2x</span></div>
<label>Перестановка: цена ушла за границу на, %<input id="trail_pct" type="number" min="0" max="50" step="0.5"></label>
<label>Лимит убытка сдвигов вниз, % капитала в сутки (0 = без лимита)<input id="trail_down_cap" type="number" min="0" max="100" step="1"></label>
<label>Предел вверх, цена (0 = без предела)<input id="trail_up_limit" type="number" min="0" step="any"></label>
<label>Предел вниз, цена (0 = без предела)<input id="trail_down_limit" type="number" min="0" step="any"></label>
<p class="full note" style="margin:0">Сетка сдвигается, когда цена ушла за её границу на заданный процент, сразу, без ожидания по времени. Сдвигается столько шагов, сколько нужно, чтобы цена снова оказалась внутри. Вверх: бот снимает нижнюю покупку, докупает монету и ставит продажу выше. Вниз: продаёт монету верхней ячейки и ставит покупку ниже — <b>каждый сдвиг вниз фиксирует убыток</b>, поэтому он выключен по умолчанию.</p>
<label>Капитал (маржа), USDT<input id="capital" type="number" min="10"></label>
<label class="chk"><input type="checkbox" id="cap_max"> Брать по максимуму на весь депозит</label>
<div class="full note" id="capnote" style="margin:0" hidden></div>
<label>Диапазон ± %<input id="range_pct" type="number" min="1" max="50" step="0.5"></label>
<label>Уровней<input id="levels" type="number" min="3" max="50"></label>
<label>Лоты<select id="lot_mode"><option value="equal">Одинаковые</option><option value="geo">Геометрические (растут к краям)</option></select></label>
<label id="multbox">Множитель лота<input id="lot_mult" type="number" min="1.05" max="3" step="0.05"></label>
<div class="full note" id="lotinfo" style="margin:0"></div>
</div>
<div class="delrow"><button type="button" id="delbtn">Удалить эту сетку</button><span class="note" id="delnote"></span></div>
<p class="note">Ключ создайте на Bybit только с правом «Спот торговля», без вывода средств, лучше с привязкой к вашему IP. Сохраняется в gridbot_config.json рядом с программой.</p>
</details>
<section class="split" id="split">
<div class="lbl">Сетки на весь депозит</div>
<div class="note" style="margin:0">Каждая сетка нейтральная: половина капитала на Buy ниже цены, половина на Sell выше — заработок, куда бы рынок ни пошёл (на фьючерсах). 1. Выберите монеты: впишите или добавьте из поиска волатильных ниже кнопкой «+ В список». 2. Нажмите кнопку — свободный депозит поделится поровну, на каждую монету запустится своя сетка.</div>
<div class="row"><input id="spadd" placeholder="Монета, например SOL или BTC, ETH" autocomplete="off"><button type="button" class="btn2" id="spaddbtn">Добавить</button></div>
<div class="chips" id="spchips"></div>
<div class="row"><select id="spmk"><option value="futures">Фьючерсы USDT</option><option value="spot">Спот (без плеча)</option></select>
<label class="row" id="splevbox" style="gap:6px">Плечо <input id="splev" type="number" min="1" max="100" value="2">x</label></div>
<div class="note" id="spinfo" style="margin:0"></div>
<button id="spgo" type="button" disabled>Выберите монеты</button>
</section>
<section class="scan">
<div class="scanhead"><button id="scanbtn" type="button">Найти самые волатильные за 30 минут</button><span class="note" id="scaninfo">Ищет на рынке из настроек (спот или фьючерсы)</span></div>
<div class="tbl" id="scantbl" hidden><table><thead><tr><th>Монета</th><th>Размах 30м</th><th>Свеча 1м</th><th>Изменение</th><th>Пила</th><th>Оборот 30м</th><th>Характер</th><th></th></tr></thead><tbody id="scanbody"></tbody></table></div>
</section>
<section><div class="lbl" style="margin-bottom:6px">Журнал</div><div class="log" id="log"></div></section>
<p class="note">Бот работает на сервере круглосуточно, вкладку можно закрыть. <b>По рынку бот не входит и не выходит</b>: все сделки — лимитки Post-Only. При остановке снимает свои ордера; на споте монеты остаются на счёте, на фьючерсах позиция закрывается лимитками, а что не закрылось за ~30 секунд, остаётся открытым (бот напишет). Единственный выход по рынку — <b>аварийный стоп</b>: цена ушла от цены запуска на заданный % (по умолчанию 10%) против позиции, или до ликвидации осталась треть пути.</p>
</div>
<script>
const $=id=>document.getElementById(id);
const F=["api_key","api_secret","network","market","leverage","symbol","capital","range_pct","levels","trail_up","trail_down","trail_up_limit","trail_down_limit","radar","radar_sens","lot_mode","lot_mult","trail_pct","trail_down_cap","direction","emerg_pct","cap_max"];
const CHK=["trail_up","trail_down","cap_max"];
function levUI(){const fut=$("market").value==="futures",lv=Number($("leverage").value)||1,rng=Number($("range_pct").value)||8;
 $("levbox").hidden=!fut;$("levval").textContent=lv+"x";const w=$("levwarn");
 if(!fut){w.hidden=true;return}
 const liq=100/lv;w.hidden=lv<=3&&liq>rng*1.2;
 w.textContent=`Объём сетки ${(Number($("capital").value)*lv||0).toLocaleString("ru-RU")} USDT. Ликвидация примерно при движении цены на ${liq.toFixed(1)}%`+
  (liq<rng*1.2?`, это внутри диапазона ±${rng}%. Бот закроет позицию раньше, но убыток будет почти весь капитал. Уменьшите плечо или диапазон.`:". Комиссии и просадки умножаются на плечо.")+
  (lv>3?` По тесту на истории худший день внутридневной сетки без плеча −4.5…−5.8%, при ${lv}x это −${(4.5*lv).toFixed(0)}…−${Math.min(5.8*lv,100).toFixed(0)}% капитала.`:"");}
["market","leverage","range_pct","capital"].forEach(k=>$(k).addEventListener("input",levUI));
function capUI(){
 const on=$("cap_max").checked,fut=$("market").value==="futures";$("capital").disabled=on;$("capnote").hidden=!on;
 const mb=$("maxbtn");mb.setAttribute("aria-pressed",String(on));mb.disabled=!!(S&&(S.running||S.busy));
 mb.textContent=on?"✓ Весь депозит: включено":"Брать по максимуму на весь депозит";
 $("maxnote").textContent=on?"":`Сейчас капитал ${fmt(Number($("capital").value)||0)} USDT из настроек.`;
 if(!on)return;
 const free=S&&S.bal?(fut?S.bal._margin:S.bal[S.quote||"USDT"]):null,k=fut?0.95:0.97;
 if(free!=null&&!(S&&S.running)){$("capital").value=Math.floor(free*k*100)/100;levUI();lotUI()}
 $("capnote").textContent=`При запуске капитал = ${fut?"95% свободной маржи":"97% свободных USDT"} на счёте (запас на комиссии`+
  (fut?" и просадку). С плечом объём сетки = капитал × плечо.":" и докупку монеты).")+
  (free!=null?` Сейчас свободно ${fmt(free)} USDT.`:" Сумма определится при запуске.");
 $("maxnote").textContent=$("capnote").textContent;
}
["cap_max","market"].forEach(k=>$(k).addEventListener("change",capUI));
$("maxbtn").onclick=()=>{$("cap_max").checked=!$("cap_max").checked;capUI();saveSoon()};
$("capital").addEventListener("input",capUI);
function lotUI(){
 const geo=$("lot_mode").value==="geo";$("multbox").hidden=!geo;
 const n=Number($("levels").value)||12,fut=$("market").value==="futures",lv=fut?Number($("leverage").value)||1:1;
 const tot=(Number($("capital").value)||0)*lv,mu=geo?Math.max(Number($("lot_mult").value)||1.3,1.01):1;
 const cells=n-1,b=Math.ceil(cells/2),s=cells-b;let w=[];for(let i=0;i<b;i++)w.push(mu**i);for(let i=0;i<s;i++)w.push(mu**i);
 const sw=w.reduce((x,y)=>x+y,0),mn=tot/sw,mx=mn*Math.max(...w);
 $("lotinfo").textContent=geo?`Лоты примерно от ${fmt(mn)} USDT у цены до ${fmt(mx)} USDT на краях диапазона. Шаг уровней и так геометрический: одинаковый в процентах.`
  :`Лот на уровень примерно ${fmt(mn)} USDT. Шаг уровней геометрический: одинаковый в процентах.`;
}
["lot_mode","lot_mult","levels","capital","market","leverage"].forEach(k=>$(k).addEventListener("input",lotUI));
const fmt=(n,d=2)=>n==null?"—":Number(n).toLocaleString("ru-RU",{minimumFractionDigits:d,maximumFractionDigits:d});
let S=null,LOCK="";
const GLOBAL=["api_key","api_secret","network"];
let CFG=null,CUR=null;
try{CUR=localStorage.getItem("grid")}catch(e){}
$("presetbtn").onclick=()=>{const v={range_pct:5,levels:4,lot_mode:"geo",lot_mult:1.4,trail_pct:0.5,emerg_pct:10,leverage:2,trail_down_cap:5};
 for(const k in v)$(k).value=v[k];$("trail_up").checked=true;$("trail_down").checked=true;levUI();lotUI();netTag($("network").value);saveSoon()};
function dirUI(){
 const d=$("direction").value||"both",sell=d==="sell";
 document.querySelectorAll(".seg button").forEach(b=>b.setAttribute("aria-checked",String(b.dataset.dir===d)));
 $("tul").textContent=sell?"Трейлинг за ценой вниз (без убытка)":"Трейлинг вверх";
 $("tdl").textContent=sell?"Трейлинг вверх против шорта (фиксирует убыток)":"Трейлинг вниз";
 $("dirnote").textContent=d==="neutral"?"Нейтральная: старт без позиции, половина капитала на Buy-лимитки ниже цены, половина на Sell-лимитки выше. Купил — продаёт линией выше, продал (шорт) — откупает линией ниже: прибыль на колебаниях в обе стороны. Работает на фьючерсах. Без трейлинга. Аварийный стоп: цена ушла от цены запуска на заданный % в любую сторону — позиция закрывается по рынку.":sell?"Только Sell: шорт-лимитки на линиях выше цены, тейки на линию ниже только закрывают шорт и никогда не откроют лонг. Работает на фьючерсах. Аварийный стоп: цена выросла от цены запуска на заданный % — шорт закрывается по рынку.":"";
}
document.querySelector(".seg").addEventListener("click",e=>{const b=e.target.closest("button[data-dir]");if(!b||b.disabled)return;
 $("direction").value=b.dataset.dir;
 if(b.dataset.dir!=="both"&&$("market").value!=="futures"){$("market").value="futures";levUI();lotUI();netTag($("network").value)}
 dirUI();saveSoon()});
function fillForm(){
 const g=CFG.grids[CUR]||{};
 F.forEach(k=>{
  if(GLOBAL.includes(k)){if(k==="api_secret")$(k).value=CFG.has_secret?"********":"";else $(k).value=CFG[k]??"";return}
  if(CHK.includes(k))$(k).checked=!!g[k];else $(k).value=g[k]??"";
 });
 $("pair").textContent=(g.symbol||"—").replace("USDT"," / USDT");netTag(CFG.network);levUI();lotUI();dirUI();capUI();
}
async function loadCfg(){
 CFG=await (await fetch("/api/config")).json();
 const ids=Object.keys(CFG.grids).sort((a,b)=>a-b);
 if(!CUR||!CFG.grids[CUR])CUR=ids[0];
 fillForm();
 if(CFG.env_keys){$("envkeys").hidden=false;$("envkeyval").textContent=CFG.api_key;$("kbox1").hidden=$("kbox2").hidden=true}
 else if(!CFG.api_key||!CFG.has_secret)$("set").open=true;
 if(CFG.locked){LOCK="На сервере не задан пароль. Добавьте переменную APP_PASSWORD в Railway → Variables, иначе бот был бы открыт всему интернету.";$("go").disabled=true}
}
function formBody(){const b={id:CUR};F.forEach(k=>b[k]=CHK.includes(k)?$(k).checked:$(k).value);
 ["capital","range_pct","levels","leverage","trail_up_limit","trail_down_limit","lot_mult","trail_pct","trail_down_cap","emerg_pct"].forEach(k=>b[k]=Number(b[k])||0);return b}
let saveT=null;
function saveSoon(){clearTimeout(saveT);saveT=setTimeout(saveNow,500)}
async function saveNow(){clearTimeout(saveT);if(!CFG)return;const b=formBody();
 await fetch("/api/save",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)});
 F.forEach(k=>{if(!GLOBAL.includes(k)&&CFG.grids[CUR])CFG.grids[CUR][k]=b[k]})}
F.forEach(k=>$(k).addEventListener("change",saveSoon));
async function selectGrid(id){await saveNow();CUR=id;try{localStorage.setItem("grid",id)}catch(e){}
 S=null;$("delbtn").classList.remove("sure");$("delbtn").textContent="Удалить эту сетку";$("delnote").textContent="";
 await loadCfg();poll()}
async function newGrid(prefill){await saveNow();
 const r=await (await fetch("/api/new",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(Object.assign({copy_from:CUR},prefill||{}))})).json();
 await selectGrid(r.id);$("set").open=true}
let GRIDS=[];
const hm=sec=>{sec=Math.max(0,sec|0);return `${Math.floor(sec/3600)}:${String(Math.floor(sec%3600/60)).padStart(2,"0")}`};
const sgn=v=>(v>0.004?"+":v<-0.004?"−":"")+fmt(Math.abs(v));
const cls=v=>v>0.004?"up":v<-0.004?"down":"z";
function renderTabs(){
 const t=$("tabs"),tot=GRIDS.reduce((a,g)=>a+(g.total||0),0),real=GRIDS.reduce((a,g)=>a+(g.profit||0),0),
  un=GRIDS.reduce((a,g)=>a+(g.upnl||0),0),run=GRIDS.filter(g=>g.running).length;
 t.innerHTML=GRIDS.map(g=>{const sym=(g.symbol||"—").replace("USDT",""),mk=(g.market==="futures"?` ${g.leverage}x`:" спот")+(g.direction==="sell"?" sell":g.direction==="neutral"&&g.market==="futures"?" ½/½":"");
  const v=g.total||0,c=cls(v),p=g.running||g.trades||v?` <b class="${c==="up"?"b":c==="down"?"s":"z"}">${sgn(v)}</b>`:"",
   hv=g.h1||0,hc=cls(hv),hb=(hc==="z"?"":` <span class="h ${hc}" title="за последний час">1ч ${sgn(hv)}</span>`)+
    (g.auto&&g.running?` <span class="h auto" title="авторежим: осталось держать">🤖 ${hm(g.auto_left)}</span>`:"");
  return `<button type="button" class="tab${g.running?" on":""}${g.error?" err":""}" data-id="${g.id}" aria-current="${g.id===CUR}"><span class="d"></span>${esc(sym)}<span class="i">${mk}</span>${p}${hb}</button>`}).join("")+
  `<button type="button" class="tab add" id="addtab">+ Новая сетка</button>`;
 const sb=$("sumbar"),c=cls(tot);sb.className="sumbar"+(c==="z"?"":" "+c);
 sb.innerHTML=`<div><div class="lbl">Всего по всем сеткам</div><div class="big ${c}">${sgn(tot)} <span style="font-size:.5em">USDT</span></div></div>`+
  `<div class="parts"><div><div class="lbl">Зафиксировано</div><b class="${cls(real)==="up"?"b":cls(real)==="down"?"s":""}">${sgn(real)}</b></div>`+
  `<div><div class="lbl">В позициях</div><b class="${cls(un)==="up"?"b":cls(un)==="down"?"s":""}">${sgn(un)}</b></div></div>`+
  (()=>{const h=GRIDS.reduce((a,g)=>a+(g.h1||0),0),hc=cls(h),best=[...GRIDS].sort((a,b)=>(b.h1||0)-(a.h1||0))[0];
   const bs=best&&(best.h1||0)>0.004?`лучшая: ${esc((best.symbol||"").replace("USDT",""))} ${sgn(best.h1)}`:"за час без прибыли";
   return `<div class="hour"><div class="lbl">За последний час</div><b class="${hc==="up"?"b":hc==="down"?"s":""}">${sgn(h)}</b><div class="best">${bs}</div></div>`})()+
  `<div class="run">Работает ${run} из ${GRIDS.length}</div>`+
  `<button type="button" id="resbtn">📊 Результаты по сеткам</button>`;
 if($("resdlg").open)renderRes();
}
let RSORT="total";
function renderRes(){
 const rows=[...GRIDS].sort((a,b)=>(b[RSORT]||0)-(a[RSORT]||0)),mx=Math.max(0.01,...rows.map(g=>Math.abs(g.total||0)));
 const tot=GRIDS.reduce((a,g)=>a+(g.total||0),0),real=GRIDS.reduce((a,g)=>a+(g.profit||0),0),
  un=GRIDS.reduce((a,g)=>a+(g.upnl||0),0),hh=GRIDS.reduce((a,g)=>a+(g.h1||0),0),tr=GRIDS.reduce((a,g)=>a+(g.trades||0),0),cap=GRIDS.filter(g=>g.running).reduce((a,g)=>a+(g.capital||0),0);
 const col=v=>{const c=cls(v);return c==="up"?"b":c==="down"?"s":"i"};
 const pct=(v,c)=>c>0?sgn(v/c*100)+"%":"—";
 $("resbody").innerHTML=rows.length?`<table class="rt"><thead><tr><th>Сетка</th><th>Статус</th><th>Сделок</th><th>Зафикс.</th><th>В позиции</th><th class="sort" data-s="h1"${RSORT==="h1"?' aria-sort="descending"':""}>За 1 час ▾</th><th class="sort" data-s="total"${RSORT==="total"?' aria-sort="descending"':""}>Итог, USDT ▾</th><th>% капит.</th><th></th></tr></thead><tbody>`+
  rows.map((g,i)=>{const v=g.total||0,c=cls(v),sym=(g.symbol||"—").replace("USDT",""),
   mk=g.market==="futures"?`${g.leverage}x`+(g.direction==="neutral"?" ½/½":g.direction==="sell"?" sell":""):"спот";
   const hv=g.h1||0;
   return `<tr class="row ${c}" data-id="${g.id}"><td><b>${i===0&&(g[RSORT]||0)>0.004?(RSORT==="h1"?"🔥 ":"🏆 "):""}${esc(sym)}</b> <span class="i">${mk}</span></td>`+
   `<td><span class="st${g.running?" on":""}${g.error?" err":""}">${g.error?"ошибка":g.running?"работает":"стоп"}</span></td>`+
   `<td>${g.trades||0}</td><td class="${col(g.profit||0)}">${sgn(g.profit||0)}</td><td class="${col(g.upnl||0)}">${sgn(g.upnl||0)}</td>`+
   `<td class="hr ${col(hv)}" title="зафиксировано ${sgn(g.h1_real||0)} · в позиции ${sgn(g.h1_up||0)}">${sgn(hv)}</td>`+
   `<td class="tot ${col(v)}">${sgn(v)}</td><td class="${col(v)}">${pct(v,g.capital)}</td>`+
   `<td><div class="rbar"><i class="${c}" style="width:${Math.round(Math.abs(v)/mx*100)}%"></i></div></td></tr>`}).join("")+
  `</tbody><tfoot><tr><td>Всего</td><td></td><td>${tr}</td><td class="${col(real)}">${sgn(real)}</td><td class="${col(un)}">${sgn(un)}</td>`+
  `<td class="hr ${col(hh)}">${sgn(hh)}</td><td class="tot ${col(tot)}">${sgn(tot)}</td><td class="${col(tot)}">${pct(tot,cap)}</td><td></td></tr></tfoot></table>`+
  `<p class="note" style="margin:12px 8px 0">Нажмите на строку, чтобы открыть сетку; на заголовок «За 1 час» или «Итог» — чтобы отсортировать. За 1 час = зафиксировано за час + изменение результата в позиции за час. % — от капитала сетки.</p>`
  :`<p class="note">Сеток пока нет.</p>`;
}
$("sumbar").addEventListener("click",e=>{if(e.target.closest("#resbtn")){renderRes();$("resdlg").showModal()}});
$("resx").onclick=()=>$("resdlg").close();
$("resdlg").addEventListener("click",e=>{if(e.target===$("resdlg")){$("resdlg").close();return}
 const th=e.target.closest("th.sort");if(th){RSORT=th.dataset.s;renderRes();return}
 const r=e.target.closest("tr.row");if(r){$("resdlg").close();if(r.dataset.id!==CUR)selectGrid(r.dataset.id)}});
let KILLT=null;
function killUI(){const run=GRIDS.filter(g=>g.running).length,b=$("killall");
 $("killrow").hidden=!run&&!b.disabled;if(b.classList.contains("sure")||b.disabled)return;
 b.textContent=`Закрыть все сетки (${run})`}
$("killall").onclick=async()=>{const b=$("killall"),run=GRIDS.filter(g=>g.running).length;
 if(!b.classList.contains("sure")){b.classList.add("sure");b.textContent=`Точно закрыть все ${run}? Нажмите ещё раз`;
  $("killnote").textContent="Снимутся все ордера, позиции на фьючерсах закроются по рынку";
  clearTimeout(KILLT);KILLT=setTimeout(()=>{b.classList.remove("sure");$("killnote").textContent="";killUI()},5000);return}
 clearTimeout(KILLT);b.classList.remove("sure");b.disabled=true;b.textContent="Закрываю все сетки…";
 try{const r=await fetch("/api/stop_all",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}),d=await r.json();
  if(!r.ok)throw new Error(d.error||r.status);
  $("killnote").textContent=`Остановлено сеток: ${d.stopped}. Ордера снимаются, позиции закрываются — смотрите вкладки.`}
 catch(e){$("killnote").textContent="Не получилось: "+e.message}
 setTimeout(()=>{b.disabled=false;killUI()},4000);poll()};
$("tabs").addEventListener("click",e=>{const b=e.target.closest(".tab");if(!b)return;
 if(b.id==="addtab")newGrid();else if(b.dataset.id!==CUR)selectGrid(b.dataset.id)});
$("delbtn").onclick=async()=>{const b=$("delbtn");
 if(S&&(S.running||S.busy)){$("delnote").textContent="Сначала остановите сетку";return}
 if(!b.classList.contains("sure")){b.classList.add("sure");b.textContent="Точно удалить? Нажмите ещё раз";return}
 const r=await fetch("/api/delete",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id:CUR})});
 if(!r.ok){$("delnote").textContent=(await r.json()).error;return}
 clearTimeout(saveT);CFG=null;CUR=null;await selectGrid(null)};
loadCfg().then(()=>{poll();spFunds()});
function netTag(n,m,lv){const t=$("net");m=m||$("market").value;lv=lv||$("leverage").value;
 const mk=m==="futures"||m==="linear"?`фьючерсы ${lv}x`:"спот";
 t.textContent={testnet:"Bybit testnet",demo:"Bybit demo",mainnet:"Bybit · реальные деньги"}[n]+" · "+mk;t.className="tag"+(n==="mainnet"?" live":"")}
$("market").addEventListener("input",()=>{netTag($("network").value);if($("market").value!=="futures"&&$("direction").value!=="both"){$("direction").value="both";dirUI()}});$("leverage").addEventListener("input",()=>netTag($("network").value));
$("network").onchange=e=>netTag(e.target.value);
let STARTERR="";
$("go").onclick=async()=>{
 $("go").disabled=true;STARTERR="";
 if(S&&S.running){await fetch("/api/stop",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id:CUR})})}
 else{clearTimeout(saveT);
  const r=await fetch("/api/start",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(formBody())});
  if(!r.ok)STARTERR=(await r.json()).error||"Не удалось запустить";else $("set").open=false}
 poll();
};
async function poll(){if(!CUR)return;
 try{const [st,gr]=await Promise.all([fetch("/api/state?id="+CUR),fetch("/api/grids")]);
  GRIDS=await gr.json();renderTabs();spUI();killUI();if(st.ok){S=await st.json();render();capUI()}}
 catch(e){$("statusText").textContent="Программа не отвечает"}}
setInterval(poll,2000);
let AUTO=null,AFORM=false;
function autoUI(){if(!AUTO)return;const on=!!AUTO.enabled;
 $("m_man").setAttribute("aria-pressed",String(!on));$("m_auto").setAttribute("aria-pressed",String(on));$("autobar").classList.toggle("on",on);
 $("amsg").textContent=on?`Авто: ${AUTO.msg||"…"}`+(AUTO.next_in?` · проверка через ${AUTO.next_in} с`:"")+` · ${AUTO.slots} сеток по ${hm(AUTO.hold_min*60)} ч`:"Ручной режим: сетки запускаете и останавливаете сами";
 $("achips").innerHTML=(AUTO.grids||[]).map(g=>`<span class="achip">${esc(g.symbol.replace("USDT",""))} · ещё ${hm(g.left)} · <span class="${g.total>0.004?"b":g.total<-0.004?"s":""}">${sgn(g.total)}</span></span>`).join("");
 if(!AFORM){$("a_coins").value=AUTO.coins||"";$("a_slots").value=AUTO.slots;$("a_hold").value=AUTO.hold_min/60;$("a_min").value=AUTO.minutes;$("a_lev").value=AUTO.leverage;$("a_maxlev").value=AUTO.max_leverage;$("a_use").value=AUTO.use_pct;$("a_skip").checked=!!AUTO.skip_trend}
 $("alog").innerHTML=(AUTO.log||[]).map(l=>`<div>${esc(l.t)} ${esc(l.text)}</div>`).join("")||"Пока пусто";}
async function autoPoll(){try{const r=await fetch("/api/auto");if(r.ok){AUTO=await r.json();autoUI()}}catch(e){}}
async function autoSend(b){try{const r=await fetch("/api/auto",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)}),d=await r.json();
 if(!r.ok)throw new Error(d.error||r.status);AUTO=d;AFORM=false;autoUI();setTimeout(autoPoll,3000)}catch(e){$("amsg").textContent="Ошибка: "+e.message}}
function aForm(){return{coins:$("a_coins").value,slots:Number($("a_slots").value)||3,hold_min:Math.round((Number($("a_hold").value)||2)*60),
 minutes:Number($("a_min").value)||30,leverage:Number($("a_lev").value)||2,max_leverage:Number($("a_maxlev").value)||10,use_pct:Number($("a_use").value)||90,skip_trend:$("a_skip").checked}}
["a_coins","a_slots","a_hold","a_min","a_lev","a_maxlev","a_use","a_skip"].forEach(k=>$(k).addEventListener("input",()=>{AFORM=true}));
$("asave").onclick=()=>autoSend(aForm());
$("m_auto").onclick=()=>{if(AUTO&&AUTO.enabled)return;
 if(!confirm("Включить авторежим 24/7? Бот сам будет выбирать самые волатильные монеты, держать до заданного времени и менять их."))return;
 autoSend(Object.assign(AFORM?aForm():{},{enabled:true}))};
$("m_man").onclick=()=>{if(AUTO&&!AUTO.enabled)return;autoSend({enabled:false})};
autoPoll();setInterval(autoPoll,5000);
function render(){
 const g=$("go");g.disabled=S.busy||!!LOCK;g.textContent=S.running?(S.busy?"Запускаю…":"Остановить"):(S.busy?"Останавливаю…":"Запустить бота");
 g.classList.toggle("stop",S.running&&!S.busy);
 $("status").classList.toggle("on",S.running&&!S.busy);$("statusText").textContent=S.status;
 if(S.symbol){$("pair").textContent=S.base+" / "+S.quote;netTag(S.network,S.market,S.leverage)}
 const fut=S.market==="linear";document.body.classList.toggle("fut",fut&&!!S.symbol);
 if(fut){const P=S.pos;$("ps").textContent=P?(P.side==="Buy"?"Лонг ":"Шорт ")+P.size:"нет";
  $("pe").textContent=P?fmt(P.entry):"—";$("pl").textContent=P&&P.liq?fmt(P.liq):"—";
  $("pu").textContent=P?(P.upnl>=0?"+":"")+fmt(P.upnl):"—";$("pu").className=P&&P.upnl<0?"s":"b";
  $("qlab").textContent="Свободная маржа";$("qb").textContent=fmt(S.bal._margin)}
 $("price").textContent=fmt(S.price,S.price&&S.price<10?4:2);
 const T0=S.total??S.profit,sg=v=>(v>=0?"+":"−")+fmt(Math.abs(v));
 const p=$("pnl");p.textContent=sg(T0)+" "+(S.quote||"USDT");
 p.className="pnl"+(T0>0.0001?" up":T0<-0.0001?" down":"");
 $("pnlsub").textContent=`зафиксировано ${sg(S.profit)} · в позиции ${sg(S.upnl||0)}`;
 $("tc").textContent=S.trades;$("oc").textContent=S.open;
 if(!fut){$("qlab").textContent="Свободно "+(S.quote||"USDT");$("qb").textContent=fmt(S.bal[S.quote])}
 $("blab").textContent=fut?"Плечо":"Свободно "+(S.base||"монет");$("bb").textContent=fut?S.leverage+"x":fmt(S.bal[S.base],6);
 const e=$("err");e.hidden=!(S.error||LOCK||STARTERR);e.textContent=LOCK||STARTERR||S.error;
 $("delbtn").disabled=S.running||S.busy;
 document.querySelectorAll(".seg button").forEach(b=>b.disabled=S.running||S.busy);
 if(S.levels.length){const L=S.levels,d=L[0]<10?4:0;$("grange").textContent=`Сетка: ${fmt(L[0],d)} – ${fmt(L[L.length-1],d)}`}
 radarUI(S.radar);
 if(S.fees)$("gfee").textContent=`Лимиток (мейкер): ${S.fees.maker} · маркетом: ${S.fees.taker}`;
 const T=S.trail;$("gtrail").textContent=!S.symbol?"Трейлинг: —":(!T.up&&!T.down?"Трейлинг выключен":
  `Трейлинг ${T.up?"↑":""}${T.down?"↓":""} · сдвигов ↑${T.nup} ↓${T.ndn}`+(T.pnl?` · итог сдвигов ${T.pnl>=0?"+":""}${fmt(T.pnl)}`:"")+(T.up_at?` · ↑ при ${fmt(T.up_at,T.up_at<10?5:1)}`:"")+(T.dn_at?` · ↓ при ${fmt(T.dn_at,T.dn_at<10?5:1)}`:"")+(T.wait?` · ${T.wait}`:""));
 $("log").innerHTML=S.log.map(l=>{const c={buy:"b",sell:"s",info:"i",err:"e"}[l.k],n={buy:"КУПИЛ",sell:"ПРОДАЛ",info:"БОТ",err:"ОШИБКА"}[l.k];
  const v=l.v==null?"":`<span class="${l.v>=0?"b":"s"}">${l.v>=0?"+":""}${fmt(l.v,4)}</span>`;
  return `<div><span class="i">${l.t}</span><span class="${c}">${n}</span><span>${esc(l.text)}</span>${v}</div>`}).join("");
 draw();
}
const SENS={low:75,mid:60,high:48};
function radarUI(R){
 const pill=$("rpill"),sg=(v,d=2)=>(v>0?"+":"")+fmt(v,d);
 const on=R&&R.mode!=="off"&&S.running;
 if(!R||R.mode==="off"){pill.textContent="выключен";pill.className="pill"}
 else if(!S.running){pill.textContent="ждёт запуска";pill.className="pill"}
 else if(R.guard==="dump"){pill.textContent=`дамп: покупки сняты (${R.paused})`;pill.className="pill bad"}
 else if(R.guard==="pump"){pill.textContent=`памп: продажи сняты (${R.paused})`;pill.className="pill bad"}
 else if(!R.warm){pill.textContent="прогрев ~2 мин";pill.className="pill warn"}
 else if(Math.max(R.dump||0,R.pump||0)>=35){pill.textContent="внимание";pill.className="pill warn"}
 else{pill.textContent="спокойно";pill.className="pill ok"}
 const th=SENS[$("radar_sens").value]||60;$("rth1").style.left=$("rth2").style.left=th+"%";
 $("rd").style.width=(on?R.dump||0:0)+"%";$("rp").style.width=(on?R.pump||0:0)+"%";
 $("rdv").textContent=on&&R.dump!=null?R.dump:"—";$("rpv").textContent=on&&R.pump!=null?R.pump:"—";
 const has=on&&R.tps!=null;
 $("m_tps").textContent=has?`${fmt(R.tps,1)} ×${fmt(R.speed_x,1)}`:"—";
 $("m_burst").textContent=has?`×${fmt(R.burst,1)}`:"—";
 $("m_delta").textContent=has?sg(R.delta*100,0)+"%":"—";
 $("m_move").textContent=has?sg(R.move60)+"%":"—";
 $("m_imb").textContent=has?(R.imb>0?"покуп. ":"продав. ")+fmt(Math.abs(R.imb*100),0)+"%":"—";
 $("m_oi").textContent=has&&R.src==="linear"?sg(R.oi)+"%":"—";
 $("m_fund").textContent=has&&R.src==="linear"?sg(R.funding*100,4)+"%":"—";
 ["m_delta","m_move"].forEach(id=>{const v=id==="m_delta"?R&&R.delta:R&&R.move60;$(id).className=!has?"":v>0?"b":v<0?"s":""});
}
const money=v=>v>=1e9?fmt(v/1e9,1)+" млрд":v>=1e6?fmt(v/1e6,1)+" млн":fmt(v/1e3,0)+" тыс";
let SCAN=[];
$("scanbtn").onclick=async()=>{
 const b=$("scanbtn"),info=$("scaninfo");b.disabled=true;b.textContent="Ищу…";
 const mk=$("market").value;
 try{const r=await fetch("/api/scan?market="+mk),d=await r.json();
  if(!r.ok)throw new Error(d.error||r.status);
  SCAN=d.rows;
  info.textContent=`${d.market==="futures"?"Фьючерсы":"Спот"} · проверено ${d.checked} ликвидных пар · ${d.t}`;
  $("scantbl").hidden=!SCAN.length;
  if(!SCAN.length)info.textContent+=" · подходящих не нашлось";
  $("scanbody").innerHTML=SCAN.map((x,i)=>{
   const k=x.kind==="пила"?"ok":x.kind==="тренд"?"mid":"bad";
   const tip=x.grid_ok?"цена ходит туда-сюда, хорошо для сетки":x.kind==="пила"?"мало движения":"цена ушла в одну сторону, сетка рискованна";
   return `<tr><td class="sym">${esc(x.symbol.replace("USDT",""))}<span class="i"> /USDT</span></td>
   <td>${fmt(x.range)}%</td><td>${fmt(x.candle,2)}%</td>
   <td class="${x.change>0?"b":x.change<0?"s":""}">${x.change>0?"+":""}${fmt(x.change)}%</td>
   <td>×${fmt(x.chop,1)}</td><td>${money(x.turn30)}</td>
   <td><span class="kind ${k}" title="${tip}">${x.kind}</span></td>
   <td style="white-space:nowrap"><button class="take" data-i="${i}">${S&&S.running?"В новую сетку":"Взять"}</button><button class="take add" data-add="${i}">+ В список</button></td></tr>`}).join("");
 }catch(e){info.textContent="Не получилось найти: "+e.message}
 b.disabled=false;b.textContent="Найти самые волатильные за 30 минут";
};
$("scanbody").addEventListener("click",async e=>{
 const ad=e.target.closest(".take.add");
 if(ad){const x=SCAN[+ad.dataset.add];spAdd(x.symbol,{range:x.sugg_range,levels:x.sugg_levels});ad.textContent="✓ В списке";return}
 const btn=e.target.closest(".take");if(!btn)return;const x=SCAN[+btn.dataset.i];
 if(S&&(S.running||S.busy))await newGrid();          // текущая сетка работает — монета пойдёт в новую сетку
 $("symbol").value=x.symbol;$("range_pct").value=x.sugg_range;$("levels").value=x.sugg_levels;saveSoon();
 $("pair").textContent=x.symbol.replace("USDT","")+" / USDT";levUI();
 $("scaninfo").textContent=`Взято ${x.symbol}: диапазон ±${x.sugg_range}%, ${x.sugg_levels} уровней. Нажмите «Запустить бота».`+
  (x.grid_ok?"":x.kind==="пила"?" Осторожно: монета сейчас почти не двигается.":" Осторожно: монета сейчас в "+x.kind+"е.");
 $("go").scrollIntoView({behavior:"smooth",block:"center"});
});
// ---------- сетки на весь депозит ----------
let SP={list:[],sugg:{}},SPFREE=null,SPERR="";
try{const v=JSON.parse(localStorage.getItem("split")||"null");if(v&&Array.isArray(v.list))SP=v}catch(e){}
const spSave=()=>{try{localStorage.setItem("split",JSON.stringify(SP))}catch(e){}};
function spAdd(sym,sg){
 String(sym).split(/[\s,;]+/).forEach(t=>{t=t.toUpperCase().replace("/","").trim();if(!t)return;if(!t.endsWith("USDT"))t+="USDT";
  if(!SP.list.includes(t))SP.list.push(t);if(sg)SP.sugg[t]=sg});
 spSave();spUI()}
function spUI(){
 const fut=$("spmk").value==="futures",n=SP.list.length,lv=fut?Math.max(1,Math.min(100,Number($("splev").value)||2)):1;
 $("splevbox").hidden=!fut;
 $("spchips").innerHTML=SP.list.map((x,i)=>`<span class="chip">${esc(x.replace("USDT",""))}<button type="button" data-rm="${i}" aria-label="Убрать ${esc(x)}">×</button></span>`).join("");
 const run=GRIDS.filter(g=>g.running||g.busy).map(g=>g.symbol);
 let t="";
 if(SPERR)t=SPERR;
 else if(SPFREE==null)t="Узнаю свободный депозит…";
 else{const k=fut?0.95:0.97,per=n?Math.floor(SPFREE*k/n*100)/100:0;
  t=`Свободно ${fmt(SPFREE)} USDT`+(fut?" маржи":"")+`. Берём ${fut?95:97}% (запас на комиссии)`+
   (n?` → ${n} ${n===1?"сетка":n<5?"сетки":"сеток"} по ${fmt(per)} USDT`+(fut?`, объём каждой ${fmt(per*lv)} USDT при ${lv}x`:"")+".":".")+
   (fut&&lv>3?` При ${lv}x ликвидация примерно при движении на ${(100/lv).toFixed(0)}%.`:"")}
 const dup=SP.list.filter(x=>run.includes(x));
 if(dup.length)t+=` Уже торгуются: ${dup.map(x=>x.replace("USDT","")).join(", ")} — уберите их из списка.`;
 $("spinfo").textContent=t;
 const b=$("spgo");b.disabled=!n||!!dup.length||!!LOCK;
 b.textContent=n?`Поделить депозит поровну и запустить ${n} ${n===1?"сетку":n<5?"сетки":"сеток"}`:"Выберите монеты";
}
async function spFunds(){SPFREE=null;SPERR="";spUI();
 try{const r=await fetch("/api/funds?market="+$("spmk").value),d=await r.json();if(!r.ok)throw new Error(d.error||r.status);SPFREE=d.free}
 catch(e){SPERR="Не удалось узнать баланс: "+e.message}
 spUI()}
$("spaddbtn").onclick=()=>{spAdd($("spadd").value);$("spadd").value="";$("spadd").focus()};
$("spadd").addEventListener("keydown",e=>{if(e.key==="Enter"){e.preventDefault();$("spaddbtn").click()}});
$("spchips").addEventListener("click",e=>{const b=e.target.closest("button[data-rm]");if(!b)return;SP.list.splice(+b.dataset.rm,1);spSave();spUI()});
$("spmk").onchange=()=>{SP.mk=$("spmk").value;spSave();spFunds()};
$("splev").addEventListener("input",()=>{SP.lev=$("splev").value;spSave();spUI()});
if(SP.mk)$("spmk").value=SP.mk;if(SP.lev)$("splev").value=SP.lev;
$("spgo").onclick=async()=>{
 const b=$("spgo");b.disabled=true;b.textContent="Запускаю…";await saveNow();
 try{const r=await fetch("/api/split",{method:"POST",headers:{"Content-Type":"application/json"},
   body:JSON.stringify({symbols:SP.list,market:$("spmk").value,leverage:Number($("splev").value)||2,sugg:SP.sugg,copy_from:CUR})});
  const d=await r.json();if(!r.ok)throw new Error(d.error||r.status);
  SPERR="";$("spinfo").textContent=`Запущено ${d.ids.length} сеток по ${fmt(d.per)} USDT`+(d.leverage>1?` с плечом ${d.leverage}x`:"")+". Вкладки сверху — по одной на монету.";
  SP.list=[];SP.sugg={};spSave();
  await selectGrid(d.ids[0]);setTimeout(spFunds,8000);
 }catch(e){$("spinfo").textContent="Не запустилось: "+e.message;b.disabled=false;b.textContent="Попробовать ещё раз"}
};
function esc(s){return String(s).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"})[c])}
const cv=$("cv"),ctx=cv.getContext("2d");
function draw(){
 const dpr=devicePixelRatio||1,w=cv.clientWidth,h=cv.clientHeight;
 if(cv.width!==w*dpr||cv.height!==h*dpr){cv.width=w*dpr;cv.height=h*dpr}
 ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);
 const cs=getComputedStyle(document.documentElement),col=n=>cs.getPropertyValue(n).trim();
 if(!S||!S.hist.length){ctx.fillStyle=col("--muted");ctx.font='14px "Onest", sans-serif';ctx.textAlign="center";ctx.textBaseline="middle";
  ctx.fillText("Здесь появится график цены и сетка после запуска",w/2,h/2);ctx.textAlign="left";return}
 const H=S.hist,L=S.levels,padR=72,padY=14,pw=w-padR-8;
 let mn=Math.min(...H.map(x=>x.p),...(L.length?[L[0]]:[])),mx=Math.max(...H.map(x=>x.p),...(L.length?[L[L.length-1]]:[]));
 const m=(mx-mn)*.06||mx*.001;mn-=m;mx+=m;
 const t0=H[0].t,t1=Math.max(H[H.length-1].t,t0+60000);
 const Y=v=>padY+(1-(v-mn)/(mx-mn))*(h-2*padY),X=t=>8+(t-t0)/(t1-t0)*pw;
 const p=S.price;ctx.font='11px "JetBrains Mono", monospace';ctx.textBaseline="middle";
 L.forEach(v=>{const y=Y(v),c=v<p?col("--buy"):col("--sell");ctx.strokeStyle=c;ctx.globalAlpha=.35;ctx.setLineDash([4,4]);
  ctx.beginPath();ctx.moveTo(8,y);ctx.lineTo(8+pw,y);ctx.stroke();ctx.setLineDash([]);ctx.globalAlpha=.85;ctx.fillStyle=c;ctx.fillText(fmt(v,v<10?4:0),w-padR+4,y)});
 ctx.globalAlpha=1;ctx.beginPath();H.forEach((x,i)=>{i?ctx.lineTo(X(x.t),Y(x.p)):ctx.moveTo(X(x.t),Y(x.p))});
 ctx.strokeStyle=col("--fg");ctx.lineWidth=1.6;ctx.stroke();
 S.fills.filter(f=>f.t>=t0).forEach(f=>{const x=X(f.t),y=Y(f.p);ctx.fillStyle=f.k==="buy"?col("--buy"):col("--sell");ctx.beginPath();
  if(f.k==="buy"){ctx.moveTo(x,y-1);ctx.lineTo(x-5,y+8);ctx.lineTo(x+5,y+8)}else{ctx.moveTo(x,y+1);ctx.lineTo(x-5,y-8);ctx.lineTo(x+5,y-8)}ctx.fill()});
 const l=H[H.length-1];ctx.fillStyle=col("--fg");ctx.beginPath();ctx.arc(X(l.t),Y(l.p),3.5,0,7);ctx.fill();
}
addEventListener("resize",draw);
</script></body></html>"""


def shutdown(*_):
    """Railway при перезапуске шлёт SIGTERM: аккуратно снимаем ордера, флаг автозапуска сохраняем."""
    print("получен сигнал остановки, снимаю ордера…", flush=True)
    pnl_save()
    bots = list(BOTS.values())
    for b in bots:
        b.stop(remember=False)
    for b in bots:
        if b.thread:
            b.thread.join(timeout=25)
    sys.exit(0)


def main():
    host = "0.0.0.0" if ON_SERVER else "127.0.0.1"
    srv = ThreadingHTTPServer((host, PORT), Handler)
    signal.signal(signal.SIGTERM, shutdown)
    if ON_SERVER:
        print(f"Сеточный бот слушает порт {PORT}. Настройки: {CFG_PATH}", flush=True)
        if not APP_PASSWORD:
            print("ВНИМАНИЕ: не задан APP_PASSWORD — управление ботом заблокировано", flush=True)
    else:
        url = f"http://127.0.0.1:{PORT}"
        print(f"Сеточный бот запущен: {url}  (Ctrl+C, чтобы выйти)")
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    pnl_load()
    threading.Thread(target=pnl_snap_loop, daemon=True).start()
    threading.Thread(target=lambda: (time.sleep(20), auto_loop()), daemon=True).start()
    st = load_store()
    save_store(st)                       # перевести старый файл настроек в формат с несколькими сетками
    if st.get("api_key") and st.get("api_secret") and not (ON_SERVER and not APP_PASSWORD):
        # На одной паре — только одна сетка: две сетки делят одну позицию на бирже и снимают ордера друг друга.
        # В авторежиме первыми поднимаются авто-сетки, ручная сетка на той же монете не запускается.
        auto_on = bool((st.get("auto") or {}).get("enabled"))
        order = sorted(st["grids"], key=lambda g: (not (auto_on and st["grids"][g].get("auto")), int(g)))
        taken = {}
        for gid in order:
            cfg = grid_cfg(gid, st)
            if not cfg.get("was_running"):
                continue
            key = (str(cfg.get("symbol", "")).upper(), cfg.get("market"))
            if key in taken:
                print(f"сетка {gid} ({cfg['symbol']}) не запускаю: эту монету уже торгует сетка {taken[key]}", flush=True)
                set_autostart(False, gid)
                continue
            taken[key] = gid
            print(f"сетка {gid} ({cfg['symbol']}) работала до перезапуска — запускаю снова", flush=True)
            try:
                Bybit(cfg["api_key"], cfg["api_secret"], cfg["network"],
                      "linear" if cfg.get("market") == "futures" else "spot").cancel_all(cfg["symbol"].upper())
            except Exception as e:
                print("не удалось снять старые ордера:", e, flush=True)
            bot(gid).start(cfg)
            time.sleep(1)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
