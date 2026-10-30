#!/usr/bin/env python3
"""
Сеточный бот для Bybit (спот и фьючерсы USDT, API v5).
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
               "leverage": 1, "symbol": "BTCUSDT", "capital": 200, "range_pct": 8, "levels": 12,
               "trail_up": True, "trail_down": False, "trail_up_limit": 0, "trail_down_limit": 0,
               "trail_pct": 2, "trail_down_cap": 5,
               "radar": "pause", "radar_sens": "mid", "lot_mode": "equal", "lot_mult": 1.3}
SPOT_FEE = Decimal("0.001")      # комиссия спота Bybit 0.1%
SPOT_FEE_BUF = Decimal("0.0015") # запас: комиссия покупки на споте списывается монетой
FUT_FEE = Decimal("0.0002")      # мейкерская комиссия фьючерсов (бот торгует лимитками Post-Only)
CHASE_TRIES = 6        # быстрая лимитка: до 6 перестановок по лучшей цене стакана…
CHASE_WAIT = 2.5       # …по 2.5 секунды, потом остаток добирается маркетом
LIQ_GUARD = Decimal("0.35")      # аварийный выход, когда до ликвидации осталось 35% пути
STOP_BELOW = Decimal("0.05")  # стоп, если цена ушла на 5% ниже нижней границы
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


def load_cfg():
    cfg = dict(DEFAULT_CFG)
    if os.environ.get("BYBIT_ENV") in HOSTS:
        cfg["network"] = os.environ["BYBIT_ENV"]
    try:
        with open(CFG_PATH, encoding="utf-8") as f:
            cfg.update(json.load(f))
    except Exception:
        pass
    if ENV_KEY and ENV_SECRET:          # ключи из переменных окружения главнее
        cfg["api_key"], cfg["api_secret"] = ENV_KEY, ENV_SECRET
    return cfg


def save_cfg(cfg):
    cfg = dict(cfg)
    if ENV_KEY and ENV_SECRET:          # не дублируем ключи из Variables на диск
        cfg["api_key"], cfg["api_secret"] = "", ""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = CFG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CFG_PATH)
    except OSError as e:
        print("не удалось сохранить настройки:", e)


def set_autostart(flag):
    cfg = load_cfg()
    cfg["was_running"] = bool(flag)
    save_cfg(cfg)


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
    def __init__(self):
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
        self.trail_pct, self.down_cap = Decimal("0.02"), Decimal("0.05")
        self.up_at = self.dn_at = None
        self.shift_up = self.shift_dn = 0
        self.trail_pnl = 0.0
        self.per_min, self.geo = Decimal(0), False
        self.trail_up = self.trail_down = False
        self.up_limit = self.dn_limit = Decimal(0)
        self.radar, self.radar_mode, self.radar_sens = None, "off", "mid"
        self.guard, self.guard_t, self.paused, self.calm = None, 0, {}, 0
        self.exit_spot = False
        self.bid = self.ask = None
        self.maker_n = self.taker_n = 0

    def say(self, kind, text, val=None):
        with self.lock:
            self.log.insert(0, {"t": time.strftime("%H:%M:%S"), "k": kind, "text": text, "v": val})
            del self.log[120:]
        print(time.strftime("%H:%M:%S"), kind, text, "" if val is None else val)

    def state(self):
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
            return {"running": self.running, "busy": self.busy, "status": self.status, "error": self.error,
                    "symbol": self.symbol, "base": self.base_coin, "quote": self.quote_coin,
                    "network": self.network, "price": float(self.price) if self.price else None,
                    "levels": [float(x) for x in self.levels], "hist": self.hist[-400:],
                    "fills": self.fills[-100:], "log": self.log[:60], "profit": self.profit,
                    "trades": self.trades, "open": len(self.orders), "bal": b,
                    "market": self.cat, "leverage": self.lev, "pos": pos,
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
        set_autostart(True)
        self.thread = threading.Thread(target=self._run, args=(cfg,), daemon=True)
        self.thread.start()

    def stop(self, remember=True):
        if remember:
            set_autostart(False)
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
                    if self.exit_spot and self.cat == "spot" and coins >= self.min_qty:
                        api.place(self.symbol, "Sell", q(coins, self.bstep), market=True)
                        self.say("sell", f"аварийная продажа {q(coins, self.bstep)} {self.base_coin} по рынку")
                    if self.cat == "linear":
                        self.say("info", "все ордера бота отменены")
                        self._close_position(api)
                    else:
                        self.say("info", "все ордера бота отменены, монеты остались на счёте")
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

        cap = Decimal(str(cfg["capital"]))
        self.cap = cap
        self.trail_pct = min(max(Decimal(str(cfg.get("trail_pct") if cfg.get("trail_pct") is not None else 2)), Decimal(0)), Decimal(50)) / 100
        self.down_cap = max(Decimal(str(cfg.get("trail_down_cap") if cfg.get("trail_down_cap") is not None else 5)), Decimal(0)) / 100
        total = cap * self.lev           # объём всей сетки (с учётом плеча)
        # лоты: одинаковые или геометрические — у цены самый маленький, к краям каждый больше в mult раз
        mult = Decimal(1)
        if cfg.get("lot_mode") == "geo":
            mult = min(max(Decimal(str(cfg.get("lot_mult") or "1.3")), Decimal("1.01")), Decimal(3))
        m = max([i for i in range(n - 1) if L[i] <= p], default=-1)      # ячейка, в которой сейчас цена
        dist = {i: (m - i) if i <= m else (i - m - 1) for i in range(n - 1)}
        w = {i: mult ** dist[i] for i in dist}
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

        sell_cells = [c for c in self.cells.values() if L[c["i"]] > p]
        buy_cells = [c for c in self.cells.values() if L[c["i"]] <= p]
        need_base = sum(c["qty"] for c in sell_cells)
        need_quote = sum(c["qty"] * L[c["i"]] for c in buy_cells)

        self.bal = api.balances()
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
            got, spent = self._chase(api, "Buy", buy_qty)
            self.say("buy", f"быстрой лимиткой {got} {self.base_coin} на {spent:.2f} {self.quote_coin} для верхних ордеров")
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
        if need_base > 0:
            got, spent = self._chase(api, "Buy", q(need_base, self.bstep))
            self.say("buy", f"лонг быстрой лимиткой {got} {self.base_coin} для верхних ордеров")
            if got > 0:
                p = spent / got
        _, self.bid, self.ask = api.bbo(sym)
        self._place_grid(api, p, sell_cells, buy_cells)

    def _close_position(self, api):
        pos = api.position(self.symbol)
        if not pos:
            self.say("info", "открытой позиции нет")
            return
        side = "Sell" if pos["side"] == "Buy" else "Buy"
        api.place(self.symbol, side, pos["size"], market=True, reduce_only=True)
        self.say("info", f"позиция {pos['size']} {self.base_coin} закрыта по рынку, "
                         f"нереализованный PnL был {float(pos.get('unrealisedPnl') or 0):.2f} USDT")
        self.pos = None

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

    def _trail(self, api, p):
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
        if got <= 0:
            self.say("err", "сдвиг вверх: не удалось купить, ячейка вернётся при следующем сдвиге")
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
            cell = self.cells.pop(ktop)
        sold, money = self._chase(api, "Sell", o["qty"], reduce_only=(self.cat == "linear"))
        got = money * (1 - self.fee)
        loss = float(got - cell["cost"] * sold / cell["qty"]) if sold else 0.0
        self.trail_pnl += loss
        self.dn_losses.append((time.time(), loss))
        self.profit += loss
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

    def _chase(self, api, side, qty, reduce_only=False):
        """Быстрая лимитка вместо маркета: Post-Only по лучшей цене стакана, переставляется за ценой.
        Платим мейкерскую комиссию. Что не исполнилось за ~15 секунд, добирается маркетом.
        Возвращает (исполнено монет, потрачено/получено в USDT)."""
        sym, done, money = self.symbol, Decimal(0), Decimal(0)
        for _ in range(CHASE_TRIES):
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
        if left >= self.min_qty:
            _, bid, ask = api.bbo(sym)
            px = ask if side == "Buy" else bid
            if left * px >= self.min_amt:
                api.place(sym, side, left, market=True, reduce_only=reduce_only)
                done += left
                money += left * px
                self.taker_n += 1
                self.say("info", f"лимитка не успела, остаток {left} {self.base_coin} добран маркетом")
        return done, money

    def _put(self, api, cell, side, qty):
        i = cell["i"]
        price = self.lv(i + 1) if side == "Sell" else self.lv(i)
        # если уровень уже «за ценой», не берём ликвидность: ставим по лучшей цене стакана (мейкер)
        if side == "Buy" and self.ask and price >= self.ask:
            price = self.bid
        elif side == "Sell" and self.bid and price <= self.bid:
            price = self.ask
        oid = api.place(self.symbol, side, qty, price)
        with self.lock:
            self.orders[oid] = {"cell": cell, "side": side, "qty": qty, "price": price}

    def _tick(self, api):
        p, self.bid, self.ask = api.bbo(self.symbol)
        with self.lock:
            self.price = p
            self.hist.append({"t": int(time.time() * 1000), "p": float(p)})
            del self.hist[:-1500]
        if not self._can_trail_down() and p < self.lv(self.kmin) * (1 - STOP_BELOW):
            why = "достигнут предел трейлинга вниз, " if self.trail_down else ""
            self.error = (f"Цена {p}: {why}ушла ниже сетки более чем на {int(STOP_BELOW*100)}%. "
                          f"Бот остановлен для защиты.")
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
                    self.error = (f"Цена {p} близко к ликвидации ({liq}). Позиция закрыта, бот остановлен.")
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
            if m["dump"] >= alert:
                self._pause(api, "dump", "Buy", f"ранний сигнал ДАМПА ({m['dump']}): " + why())
            elif m["pump"] >= alert:
                self._pause(api, "pump", "Sell", f"ранний сигнал ПАМПА ({m['pump']}): " + why())
            return
        if self.guard == "dump" and self.radar_mode == "exit" and m["dump"] >= RADAR_EXIT:
            self.error = f"Аварийный выход: сильный дамп ({m['dump']}). " + why()
            self.exit_spot = True
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
        if o["side"] == "Buy":
            c["cost"] = filled * avg * (1 + self.fee)
            self.fills.append({"t": t, "p": float(avg), "k": "buy"})
            self.say("buy", f"{filled} {self.base_coin} по {avg}")
            self._put(api, c, "Sell", q(filled * (1 - self.fee_buf), self.bstep))
        else:
            got = filled * avg * (1 - self.fee)
            pr = float(got - c["cost"])
            self.profit += pr
            self.fills.append({"t": t, "p": float(avg), "k": "sell"})
            self.say("sell", f"{filled} {self.base_coin} по {avg}", round(pr, 4))
            self._put(api, c, "Buy", c["qty"])


# ---------- поиск самых волатильных монет за последние 30 минут ----------
STABLES = {"USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDD", "PYUSD", "BUSD", "EUR", "USDR", "USD1", "RLUSD"}
SCAN_TOP = 80              # сколько самых ликвидных пар проверять
SCAN_MIN_TURNOVER = 3e6    # минимум оборота за 24 часа, USDT: в пустой монете сетка не работает
_scan_cache = {}


def scan_volatile(market="spot", minutes=30):
    cat = "linear" if market in ("futures", "linear") else "spot"
    key = (cat, minutes)
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
        rp = round(min(max(rng * 1.3, 2.0), 20.0), 1)
        step = max(candle * 0.8, 0.4) / 100
        lv = int(math.log((1 + rp / 100) / (1 - rp / 100)) / math.log(1 + step)) + 1
        return {"symbol": sym, "range": round(rng, 2), "candle": round(candle, 3), "change": round(chg, 2),
                "chop": round(min(chop, 99), 1), "turn30": round(turn30), "turn24": round(turnover.get(sym, 0)),
                "price": close, "kind": kind, "grid_ok": kind == "пила" and rng >= 1,
                "sugg_range": rp, "sugg_levels": max(5, min(lv, 40))}

    with ThreadPoolExecutor(max_workers=10) as ex:
        rows = [r for r in ex.map(one, pool) if r]
    rows.sort(key=lambda r: r["range"], reverse=True)
    res = {"market": "futures" if cat == "linear" else "spot", "minutes": minutes,
           "checked": len(pool), "t": time.strftime("%H:%M:%S"), "rows": rows[:20]}
    _scan_cache[key] = (time.time(), res)
    return res


BOT = Bot()


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
            return self._send(200, {"ok": True, "running": BOT.running})
        if not self._auth_ok():
            return self._deny()
        if self.path == "/":
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self._send(200, BOT.state())
        elif self.path.startswith("/api/scan"):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:
                self._send(200, scan_volatile(qs.get("market", ["spot"])[0]))
            except BybitError as e:
                self._send(502, {"error": str(e)})
        elif self.path == "/api/config":
            cfg = load_cfg()
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
        if self.path == "/api/start":
            cfg = load_cfg()
            for k in DEFAULT_CFG:
                v = body.get(k)
                if k in ("api_key", "api_secret") and ENV_KEY and ENV_SECRET:
                    continue
                if v not in (None, "") and not (k == "api_secret" and v == "********"):
                    cfg[k] = v
            save_cfg(cfg)
            BOT.start(cfg)
            self._send(200, {"ok": True})
        elif self.path == "/api/stop":
            BOT.stop()
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
<header><div><div class="pair" id="pair">—</div><div class="tag" id="net">—</div></div>
<div class="status" id="status"><span class="dot"></span><span id="statusText">Остановлен</span></div></header>
<section class="hero"><div><div class="lbl">Прибыль сетки</div><div class="pnl" id="pnl">+0.00</div></div>
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
<label class="chk"><input type="checkbox" id="trail_up"> Трейлинг вверх</label>
<label class="chk"><input type="checkbox" id="trail_down"> Трейлинг вниз</label>
<label>Перестановка: цена ушла за границу на, %<input id="trail_pct" type="number" min="0" max="50" step="0.5"></label>
<label>Лимит убытка сдвигов вниз, % капитала в сутки (0 = без лимита)<input id="trail_down_cap" type="number" min="0" max="100" step="1"></label>
<label>Предел вверх, цена (0 = без предела)<input id="trail_up_limit" type="number" min="0" step="any"></label>
<label>Предел вниз, цена (0 = без предела)<input id="trail_down_limit" type="number" min="0" step="any"></label>
<p class="full note" style="margin:0">Сетка сдвигается, когда цена ушла за её границу на заданный процент, сразу, без ожидания по времени. Сдвигается столько шагов, сколько нужно, чтобы цена снова оказалась внутри. Вверх: бот снимает нижнюю покупку, докупает монету и ставит продажу выше. Вниз: продаёт монету верхней ячейки и ставит покупку ниже — <b>каждый сдвиг вниз фиксирует убыток</b>, поэтому он выключен по умолчанию.</p>
<label>Капитал (маржа), USDT<input id="capital" type="number" min="10"></label>
<label>Диапазон ± %<input id="range_pct" type="number" min="1" max="50" step="0.5"></label>
<label>Уровней<input id="levels" type="number" min="3" max="50"></label>
<label>Лоты<select id="lot_mode"><option value="equal">Одинаковые</option><option value="geo">Геометрические (растут к краям)</option></select></label>
<label id="multbox">Множитель лота<input id="lot_mult" type="number" min="1.05" max="3" step="0.05"></label>
<div class="full note" id="lotinfo" style="margin:0"></div>
</div>
<p class="note">Ключ создайте на Bybit только с правом «Спот торговля», без вывода средств, лучше с привязкой к вашему IP. Сохраняется в gridbot_config.json рядом с программой.</p>
</details>
<section class="scan">
<div class="scanhead"><button id="scanbtn" type="button">Найти самые волатильные за 30 минут</button><span class="note" id="scaninfo">Ищет на рынке из настроек (спот или фьючерсы)</span></div>
<div class="tbl" id="scantbl" hidden><table><thead><tr><th>Монета</th><th>Размах 30м</th><th>Свеча 1м</th><th>Изменение</th><th>Пила</th><th>Оборот 30м</th><th>Характер</th><th></th></tr></thead><tbody id="scanbody"></tbody></table></div>
</section>
<section><div class="lbl" style="margin-bottom:6px">Журнал</div><div class="log" id="log"></div></section>
<p class="note">При остановке бот снимает все свои ордера. На споте купленные монеты остаются на счёте, на фьючерсах позиция закрывается по рынку. Бот останавливается сам, если цена падает на 5% ниже сетки, а на фьючерсах ещё и когда до ликвидации остаётся треть пути.</p>
</div>
<script>
const $=id=>document.getElementById(id);
const F=["api_key","api_secret","network","market","leverage","symbol","capital","range_pct","levels","trail_up","trail_down","trail_up_limit","trail_down_limit","radar","radar_sens","lot_mode","lot_mult","trail_pct","trail_down_cap"];
const CHK=["trail_up","trail_down"];
function levUI(){const fut=$("market").value==="futures",lv=Number($("leverage").value)||1,rng=Number($("range_pct").value)||8;
 $("levbox").hidden=!fut;$("levval").textContent=lv+"x";const w=$("levwarn");
 if(!fut){w.hidden=true;return}
 const liq=100/lv;w.hidden=lv<=3&&liq>rng*1.2;
 w.textContent=`Объём сетки ${(Number($("capital").value)*lv||0).toLocaleString("ru-RU")} USDT. Ликвидация примерно при движении цены на ${liq.toFixed(1)}%`+
  (liq<rng*1.2?`, это внутри диапазона ±${rng}%. Бот закроет позицию раньше, но убыток будет почти весь капитал. Уменьшите плечо или диапазон.`:". Комиссии и просадки умножаются на плечо.");}
["market","leverage","range_pct","capital"].forEach(k=>$(k).addEventListener("input",levUI));
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
fetch("/api/config").then(r=>r.json()).then(c=>{F.forEach(k=>{if(k==="api_secret"){$(k).value=c.has_secret?"********":""}else if(CHK.includes(k)){$(k).checked=!!c[k]}else $(k).value=c[k]});
 if(c.env_keys){$("envkeys").hidden=false;$("envkeyval").textContent=c.api_key;$("kbox1").hidden=$("kbox2").hidden=true}
 else if(!c.api_key||!c.has_secret)$("set").open=true;
 if(c.locked){LOCK="На сервере не задан пароль. Добавьте переменную APP_PASSWORD в Railway → Variables, иначе бот был бы открыт всему интернету.";$("go").disabled=true} $("pair").textContent=c.symbol; netTag(c.network);levUI();lotUI()});
function netTag(n,m,lv){const t=$("net");m=m||$("market").value;lv=lv||$("leverage").value;
 const mk=m==="futures"||m==="linear"?`фьючерсы ${lv}x`:"спот";
 t.textContent={testnet:"Bybit testnet",demo:"Bybit demo",mainnet:"Bybit · реальные деньги"}[n]+" · "+mk;t.className="tag"+(n==="mainnet"?" live":"")}
$("market").addEventListener("input",()=>netTag($("network").value));$("leverage").addEventListener("input",()=>netTag($("network").value));
$("network").onchange=e=>netTag(e.target.value);
$("go").onclick=async()=>{
 $("go").disabled=true;
 if(S&&S.running){await fetch("/api/stop",{method:"POST"})}
 else{const b={};F.forEach(k=>b[k]=CHK.includes(k)?$(k).checked:$(k).value);["capital","range_pct","levels","leverage","trail_up_limit","trail_down_limit","lot_mult","trail_pct","trail_down_cap"].forEach(k=>b[k]=Number(b[k])||0);
  await fetch("/api/start",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)});$("set").open=false}
 poll();
};
async function poll(){try{S=await (await fetch("/api/state")).json();render()}catch(e){$("statusText").textContent="Программа не отвечает"}}
setInterval(poll,2000);poll();
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
 const p=$("pnl");p.textContent=(S.profit>=0?"+":"−")+fmt(Math.abs(S.profit))+" "+(S.quote||"USDT");
 p.className="pnl"+(S.profit>0.0001?" up":S.profit<-0.0001?" down":"");
 $("tc").textContent=S.trades;$("oc").textContent=S.open;
 if(!fut){$("qlab").textContent="Свободно "+(S.quote||"USDT");$("qb").textContent=fmt(S.bal[S.quote])}
 $("blab").textContent=fut?"Плечо":"Свободно "+(S.base||"монет");$("bb").textContent=fut?S.leverage+"x":fmt(S.bal[S.base],6);
 const e=$("err");e.hidden=!(S.error||LOCK);e.textContent=LOCK||S.error;
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
   <td><button class="take" data-i="${i}" ${S&&S.running?"disabled":""}>Взять</button></td></tr>`}).join("");
 }catch(e){info.textContent="Не получилось найти: "+e.message}
 b.disabled=false;b.textContent="Найти самые волатильные за 30 минут";
};
$("scanbody").addEventListener("click",e=>{
 const btn=e.target.closest(".take");if(!btn)return;const x=SCAN[+btn.dataset.i];
 $("symbol").value=x.symbol;$("range_pct").value=x.sugg_range;$("levels").value=x.sugg_levels;
 $("pair").textContent=x.symbol.replace("USDT","")+" / USDT";levUI();
 $("scaninfo").textContent=`Взято ${x.symbol}: диапазон ±${x.sugg_range}%, ${x.sugg_levels} уровней. Нажмите «Запустить бота».`+
  (x.grid_ok?"":x.kind==="пила"?" Осторожно: монета сейчас почти не двигается.":" Осторожно: монета сейчас в "+x.kind+"е.");
 $("go").scrollIntoView({behavior:"smooth",block:"center"});
});
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
    BOT.stop(remember=False)
    if BOT.thread:
        BOT.thread.join(timeout=25)
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
    cfg = load_cfg()
    if cfg.get("was_running") and cfg.get("api_key") and cfg.get("api_secret") and not (ON_SERVER and not APP_PASSWORD):
        print("бот работал до перезапуска — запускаю снова", flush=True)
        try:
            Bybit(cfg["api_key"], cfg["api_secret"], cfg["network"],
                  "linear" if cfg.get("market") == "futures" else "spot").cancel_all(cfg["symbol"].upper())
        except Exception as e:
            print("не удалось снять старые ордера:", e, flush=True)
        BOT.start(cfg)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
