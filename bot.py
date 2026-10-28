#!/usr/bin/env python3
"""Ручний нейтральний грід-бот для Bybit USDT-perp (як грід-бот на Bybit, але свій).

Ви задаєте монету, діапазон, кількість сіток, маржу й плече. Buy стоять нижче ціни, Sell — вище.
Щойно лімітка виконалась, на сусідній лінії ставиться протилежна (мікротейк).
Режими: однакові лоти або геометрична сітка (лінії через однаковий %, лот росте від ціни до краю).

Лише стандартна бібліотека Python 3.9+. Налаштування — змінні середовища або файл .env поруч.
"""

from __future__ import annotations

import sys

if sys.version_info < (3, 9):
    sys.exit("Потрібен Python 3.9 або новіший")

import hashlib
import hmac
import json
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
LOCAL = not os.environ.get("RAILWAY_ENVIRONMENT")


def load_dotenv(path: Path) -> None:
    """KEY=VALUE з .env; справжні змінні середовища важливіші. # — коментар."""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].strip()
        os.environ.setdefault(key, value)


load_dotenv(HERE / ".env")
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass


def env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        value = int(float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        value = default
    return max(lo, min(hi, value))


def env_float(name: str, default: float, lo: float, hi: float) -> float:
    try:
        value = float(os.environ.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(lo, min(hi, value))


HOSTS = {
    "mainnet": ["https://api.bybit.com", "https://api.bytick.com"],
    "testnet": ["https://api-testnet.bybit.com", "https://api-testnet.bytick.com"],
}


class Cfg:
    def __init__(self) -> None:
        self.api_key = os.environ.get("BYBIT_API_KEY", "").strip()
        self.api_secret = os.environ.get("BYBIT_API_SECRET", "").strip()
        env = os.environ.get("BYBIT_ENV", "testnet").strip().lower()
        self.env = env if env in HOSTS else "testnet"
        self.base = HOSTS[self.env][0]
        # критичний стоп-лос, % від маржі позиції (ROI як на Bybit); 0 = вимкнено
        self.sl_roi = env_float("STOP_LOSS_ROI", 150, 0, 1000)
        self.poll = env_int("POLL_SEC", 2, 1, 60)
        self.scalp_min_step = env_float("SCALP_MIN_STEP_PCT", 0.12, 0.05, 2)
        self.tg_token = os.environ.get("TELEGRAM_TOKEN", "").strip()
        self.tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        volume = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
        if volume and os.path.isdir(volume):
            default_state = os.path.join(volume, "state.json")
        elif os.path.isdir("/data"):
            default_state = "/data/state.json"
        else:
            default_state = str(HERE / "data" / "state.json")
        self.state_path = Path(os.environ.get("STATE_PATH", default_state))


CFG = Cfg()
LOCK = threading.Lock()        # стан (STATE)
GRID_LOCK = threading.Lock()   # дії з ордерами сіток
STOP = threading.Event()
OFFSET_MS = 0
HEART = {"tick": 0.0, "started": time.time()}
ACCOUNT = {"equity": "0", "available": "0", "positions": [], "error": "", "at": 0.0}
SPECS: dict[str, dict] = {}
LINK_PREFIX = "mg"  # свої orderLinkId, щоб не чіпати чужі ордери


def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


# ---------------- стан ----------------

def load_state() -> dict:
    if CFG.state_path.exists():
        try:
            data = json.loads(CFG.state_path.read_text())
            if isinstance(data, dict):
                data.setdefault("grids", [])
                data.setdefault("events", [])
                return data
        except (OSError, json.JSONDecodeError):
            log("файл стану не читається, починаю з нуля")
    return {"grids": [], "events": []}


def save_state() -> None:
    cutoff = time.time() - 86400
    STATE["grids"] = [
        g for g in STATE.get("grids") or []
        if g.get("status") == "active" or float(g.get("closedAt") or g.get("created") or 0) > cutoff
    ][:100]
    CFG.state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = CFG.state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(STATE, ensure_ascii=False))
    tmp.replace(CFG.state_path)


STATE = load_state()


def add_event(text: str) -> None:
    with LOCK:
        events = STATE.setdefault("events", [])
        events.append({"t": int(time.time()), "text": text[:300]})
        del events[:-80]


def notify(text: str) -> None:
    log(text)
    add_event(text)
    if not CFG.tg_token or not CFG.tg_chat:
        return
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{CFG.tg_token}/sendMessage",
        data=json.dumps({"chat_id": CFG.tg_chat, "text": text[:3500]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
    except Exception as exc:  # noqa: BLE001 — Telegram ніколи не зупиняє бота
        log(f"telegram: {exc}")


# ---------------- Bybit V5 ----------------

class BybitError(Exception):
    def __init__(self, message: str, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


def bybit(path: str, method: str = "GET", query: dict | None = None, body: dict | None = None,
          auth: bool = False, _retry: bool = True) -> dict:
    global OFFSET_MS
    q = {k: str(v) for k, v in (query or {}).items() if v is not None}
    qs = urllib.parse.urlencode(q)
    body_str = json.dumps(body, separators=(",", ":")) if method == "POST" else ""
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    if auth:
        if not CFG.api_key or not CFG.api_secret:
            raise BybitError("Немає BYBIT_API_KEY / BYBIT_API_SECRET")
        ts = str(int(time.time() * 1000) + OFFSET_MS)
        recv = "20000"
        payload = ts + CFG.api_key + recv + (qs if method == "GET" else body_str)
        headers.update({
            "X-BAPI-API-KEY": CFG.api_key,
            "X-BAPI-SIGN": hmac.new(CFG.api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest(),
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv,
        })
    if method == "POST":
        headers["Content-Type"] = "application/json"
    url = CFG.base + path + (("?" + qs) if qs else "")
    req = urllib.request.Request(url, data=body_str.encode() if method == "POST" else None, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        full = exc.read().decode("utf-8", "ignore")
        detail = full[:180].strip()
        low = full.lower()
        if exc.code == 403 and ("country" in low or "cloudfront" in low or low.lstrip().startswith("<")):
            raise BybitError("Bybit блокує країну сервера (HTTP 403). Railway → сервіс → Settings → Deploy → Regions: оберіть EU West (Amsterdam) або Southeast Asia (Singapore), не US, і перезапустіть.") from exc
        if exc.code == 401:
            raise BybitError(
                f"Bybit не прийняв ключ (HTTP 401) на {CFG.env}. Перевірте: BYBIT_ENV = mainnet чи testnet під ваш ключ; "
                "ключ і секрет без пробілів; ключ без прив'язки до IP або з IP цього сервера."
            ) from exc
        raise BybitError(f"HTTP {exc.code} {detail}".strip()) from exc
    except urllib.error.URLError as exc:
        raise BybitError(f"мережа: {exc.reason}") from exc
    except (TimeoutError, OSError, ValueError) as exc:
        raise BybitError(f"мережа: {exc}") from exc
    if not isinstance(raw, dict):
        raise BybitError("Bybit: дивна відповідь")
    if raw.get("retCode") == 10002 and _retry:
        server = int(raw.get("time") or time.time() * 1000)
        OFFSET_MS = server - int(time.time() * 1000)
        return bybit(path, method, query, body, auth, _retry=False)
    if raw.get("retCode") not in (0, None):
        raise BybitError(raw.get("retMsg") or "Bybit error", raw.get("retCode"))
    return raw.get("result") or {}


def pick_base() -> bool:
    for base in HOSTS[CFG.env]:
        CFG.base = base
        try:
            bybit("/v5/market/time")
            log(f"Bybit {CFG.env}: {base}")
            return True
        except BybitError as exc:
            log(f"Bybit недоступний {base}: {exc}")
    return False


def step_decimals(step: str) -> int:
    return len(step.split(".", 1)[1]) if "." in step else 0


def align(value: float, step_str: str, up: bool) -> str:
    d = step_decimals(step_str)
    factor = 10 ** d
    step_int = max(1, round(float(step_str) * factor))
    if up:
        aligned = math.ceil(math.ceil(value * factor - 1e-6) / step_int) * step_int
    else:
        aligned = math.floor(math.floor(value * factor + 1e-6) / step_int) * step_int
    return f"{aligned / factor:.{d}f}"


def link_id(symbol: str, side: str, price: str) -> str:
    tail = f"{time.time_ns():x}"[-6:]
    return f"{LINK_PREFIX}{symbol[:8]}{'B' if side == 'Buy' else 'S'}{price.replace('.', '')[-6:]}{tail}"[:36]


def instrument(symbol: str) -> dict:
    result = bybit("/v5/market/instruments-info", query={"category": "linear", "symbol": symbol})
    row = (result.get("list") or [None])[0]
    if not row:
        raise BybitError(f"Немає {symbol}")
    if row.get("status") and row["status"] != "Trading":
        raise BybitError(f"{symbol} зараз не торгується")
    return {
        "tick": row["priceFilter"]["tickSize"],
        "qty": row["lotSizeFilter"]["qtyStep"],
        "min_qty": float(row["lotSizeFilter"].get("minOrderQty") or 0),
        "min_notional": float(row["lotSizeFilter"].get("minNotionalValue") or 5),
        "max_lev": float((row.get("leverageFilter") or {}).get("maxLeverage") or 20),
    }


def spec_of(symbol: str) -> dict:
    if symbol not in SPECS:
        SPECS[symbol] = instrument(symbol)
    return SPECS[symbol]


def ticker(symbol: str) -> dict:
    result = bybit("/v5/market/tickers", query={"category": "linear", "symbol": symbol})
    row = (result.get("list") or [None])[0]
    if not row:
        raise BybitError(f"Немає ціни {symbol}")
    return row


def candles(symbol: str, interval: str, limit: int) -> list[dict]:
    result = bybit("/v5/market/kline", query={"category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)})
    return [{"high": float(k[2]), "low": float(k[3]), "close": float(k[4])} for k in reversed(result.get("list") or [])]


def set_leverage(symbol: str, leverage: int) -> None:
    try:
        bybit("/v5/position/set-leverage", "POST", body={
            "category": "linear", "symbol": symbol, "buyLeverage": str(leverage), "sellLeverage": str(leverage),
        }, auth=True)
    except BybitError as exc:
        if exc.code == 110043 or "not modified" in str(exc).lower():
            return
        raise


def create_order(body: dict, side: str) -> None:
    """One-way режим (positionIdx 0), а якщо на рахунку hedge — 1 для Buy, 2 для Sell
    (для reduce-only навпаки: Buy закриває шорт = 2, Sell закриває лонг = 1)."""
    try:
        bybit("/v5/order/create", "POST", body={**body, "positionIdx": 0}, auth=True)
    except BybitError as exc:
        if "position idx" not in str(exc).lower():
            raise
        long_idx = side == "Buy"
        if body.get("reduceOnly"):
            long_idx = not long_idx
        bybit("/v5/order/create", "POST", body={**body, "positionIdx": 1 if long_idx else 2}, auth=True)


def place_limit(symbol: str, order: dict, post_only: bool = True) -> None:
    create_order({
        "category": "linear", "symbol": symbol, "side": order["side"], "orderType": "Limit",
        "qty": order["qty"], "price": order["price"], "timeInForce": "PostOnly" if post_only else "GTC",
        "orderLinkId": order["orderLinkId"], "reduceOnly": bool(order.get("reduce")),
    }, order["side"])


def cancel_link(symbol: str, order_link_id: str) -> None:
    if not order_link_id.startswith(LINK_PREFIX):
        return
    try:
        bybit("/v5/order/cancel", "POST", body={"category": "linear", "symbol": symbol, "orderLinkId": order_link_id}, auth=True)
    except BybitError as exc:
        if any(s in str(exc).lower() for s in ("not exists", "too late", "order not")):
            return
        raise


def positions(symbol: str | None = None) -> list[dict]:
    query = {"category": "linear", "limit": "200"}
    if symbol:
        query["symbol"] = symbol
    else:
        query["settleCoin"] = "USDT"
    result = bybit("/v5/position/list", query=query, auth=True)
    return [p for p in (result.get("list") or []) if float(p.get("size") or 0) > 0]


def close_position(symbol: str) -> str:
    done = []
    for p in positions(symbol):
        side = "Sell" if p.get("side") == "Buy" else "Buy"
        bybit("/v5/order/create", "POST", body={
            "category": "linear", "symbol": symbol, "side": side, "orderType": "Market", "qty": p["size"],
            "reduceOnly": True, "timeInForce": "IOC", "positionIdx": int(p.get("positionIdx") or 0),
        }, auth=True)
        done.append(f"{p.get('side')} {p['size']} PnL {float(p.get('unrealisedPnl') or 0):+.2f}")
    return ", ".join(done) if done else "позиції не було"


def cancel_grid_orders(grid: dict, note: str) -> None:
    for level in grid["levels"]:
        if level["status"] != "open":
            continue
        try:
            cancel_link(grid["symbol"], level["orderLinkId"])
        except BybitError as exc:
            log(f"зняти {grid['symbol']} {level['orderLinkId']}: {exc}")
            continue
        level["status"] = "done"
        level["note"] = note
        time.sleep(0.08)


def parse_num(raw, name: str, lo: float, hi: float, integer: bool = False) -> float:
    """Приймає 50, "50", "50.5", "50,5", " 1 000 ". Відкидає порожнє, текст, NaN, поза межами."""
    if isinstance(raw, bool):
        raise BybitError(f"{name}: потрібне число")
    if isinstance(raw, (int, float)):
        value = float(raw)
    else:
        text = str(raw if raw is not None else "").strip().replace(" ", "").replace(" ", "").replace(",", ".")
        if not text:
            raise BybitError(f"{name}: порожнє поле")
        try:
            value = float(text)
        except ValueError:
            raise BybitError(f"{name}: «{raw}» не число") from None
    if not math.isfinite(value):
        raise BybitError(f"{name}: потрібне число")
    if integer and value != int(value):
        raise BybitError(f"{name}: ціле число, без дробу")
    if value < lo or value > hi:
        raise BybitError(f"{name}: від {lo:g} до {hi:g}")
    return value


def parse_symbol(raw) -> str:
    symbol = str(raw or "").strip().upper()
    if not symbol.isalnum() or not symbol.endswith("USDT") or len(symbol) < 6:
        raise BybitError("Монета на кшталт BTCUSDT")
    return symbol


# ---------------- нейтральний грід ----------------

def grid_params(data: dict) -> dict:
    symbol = parse_symbol(data.get("symbol"))
    lower = parse_num(data.get("lower"), "Нижня ціна", 1e-12, 1e12)
    upper = parse_num(data.get("upper"), "Верхня ціна", 1e-12, 1e12)
    if upper <= lower * 1.002:
        raise BybitError("Верхня ціна має бути вищою за нижню хоча б на 0.2%")
    count = int(parse_num(data.get("count"), "Кількість сіток", 2, 150, integer=True))
    margin = parse_num(data.get("margin"), "Маржа", 1, 1000000)
    lev = int(parse_num(data.get("leverage"), "Плече", 1, 125, integer=True))
    shift_min = int(parse_num(data.get("shiftMin", 0), "Іти за ціною через, хв", 0, 1440, integer=True))
    geo = bool(data.get("geo"))
    geo_mult = parse_num(data.get("geoMult") or 1.3, "Множник лота", 1, 3) if geo else 1.0
    mode = str(data.get("mode") or "both")
    if mode not in MODES:
        raise BybitError("Режим: both, sell або buy")
    return {
        "symbol": symbol, "lower": lower, "upper": upper, "count": count, "margin": margin, "lev": lev,
        "trail": bool(data.get("trail", True)), "shiftMin": shift_min, "geo": geo, "geoMult": geo_mult, "mode": mode,
    }


# both — Buy нижче і Sell вище ціни; sell — лише Sell вище (шорт-сітка); buy — лише Buy нижче (лонг-сітка).
# В однобічному режимі протилежна лімітка — це тейк reduce-only: закриває виконану лімітку і не відкриває зустрічну позицію.
MODES = {"both": "Buy + Sell", "sell": "Тільки Sell", "buy": "Тільки Buy"}


def ladder_of(lower: float, upper: float, count: int, tick: str, geo: bool = False) -> list[str]:
    """count сіток = count + 1 ліній: рівномірно за ціною або через однаковий % (геометрична)."""
    step = (upper - lower) / count
    ratio = (upper / lower) ** (1 / count)
    out: list[str] = []
    for i in range(count + 1):
        price = align(lower * ratio ** i if geo else lower + step * i, tick, False)
        if float(price) > 0 and price not in out:
            out.append(price)
    return out


def equal_qty(margin: float, lev: int, count: int, mid: float, spec: dict) -> str:
    qty = align(margin * lev / count / mid, spec["qty"], False)
    min_n = spec.get("min_notional") or 5
    if float(qty) * mid < min_n or float(qty) < spec["min_qty"]:
        need = max(min_n, spec["min_qty"] * mid) * count / lev
        raise BybitError(
            f"Замало маржі: на {count} сіток по мін. {min_n:g} USDT треба маржу від ~{math.ceil(need * 1.05 * 100) / 100:g} USDT "
            f"при x{lev}, або менше сіток, або більше плече."
        )
    return qty


def grid_orders(symbol: str, ladder: list[str], last: float, bid: float, ask: float, qty: str, mode: str = "both") -> list[dict]:
    """Buy на кожній лінії нижче ціни, Sell — вище; лінія біля ціни лишається порожньою.
    mode=sell — лише Sell, mode=buy — лише Buy."""
    nearest = min(ladder, key=lambda p: abs(float(p) - last))
    orders = []
    for price in ladder:
        n = float(price)
        if price == nearest:
            continue
        side = "Buy" if n < last else "Sell"
        if (side == "Buy" and n >= bid) or (side == "Sell" and n <= ask):
            continue
        if (mode == "sell" and side == "Buy") or (mode == "buy" and side == "Sell"):
            continue
        orders.append({"side": side, "price": price, "qty": qty, "orderLinkId": link_id(symbol, side, price), "status": "open", "note": "grid"})
    return orders


def geo_sizes(orders: list[dict], ladder: list[str], last: float, budget: float, mult: float, spec: dict) -> str:
    """Геометричні лоти: лінія біля ціни — найменший лот, кожна наступна від ціни — у mult разів більший.
    Усі лімітки разом = маржа × плече. Записує order['qty'], повертає 'мін–макс'."""
    if not orders:
        return "0"
    centre = min(range(len(ladder)), key=lambda k: abs(float(ladder[k]) - last))
    weights = []
    for order in orders:
        k = ladder.index(order["price"]) if order["price"] in ladder else centre + 1
        weights.append(mult ** max(0, abs(k - centre) - 1))
    total = sum(weights)
    min_n = spec.get("min_notional") or 5
    for order, w in zip(orders, weights):
        price = float(order["price"])
        qty = align(budget * w / total / price, spec["qty"], False)
        if float(qty) * price < min_n or float(qty) < spec["min_qty"]:
            raise BybitError(
                f"Замало маржі для геометричної сітки: найменша лімітка {float(qty) * price:.2f} USDT, мін. {min_n:g} USDT. "
                f"Зменште множник лота ({mult:g}) чи кількість сіток, або додайте маржу/плече."
            )
        order["qty"] = qty
    qtys = sorted(float(o["qty"]) for o in orders)
    return f"{qtys[0]:g}–{qtys[-1]:g}" if qtys[0] != qtys[-1] else f"{qtys[0]:g}"


def build(symbol: str, lower: float, upper: float, count: int, margin: float, lev: int, geo: bool, mult: float,
          spec: dict, last: float, bid: float, ask: float, mode: str = "both") -> tuple[list[str], list[dict], str]:
    ladder = ladder_of(lower, upper, count, spec["tick"], geo)
    if geo:
        orders = grid_orders(symbol, ladder, last, bid, ask, "0", mode)
        return ladder, orders, geo_sizes(orders, ladder, last, margin * lev, mult, spec)
    if mode == "both":
        qty = equal_qty(margin, lev, count, (lower + upper) / 2, spec)
        return ladder, grid_orders(symbol, ladder, last, bid, ask, qty, mode), qty
    # однобічна сітка: уся маржа × плече ділиться лише на лімітки одного боку
    orders = grid_orders(symbol, ladder, last, bid, ask, "0", mode)
    if not orders:
        return ladder, orders, "0"
    qty = equal_qty(margin, lev, len(orders), sum(float(o["price"]) for o in orders) / len(orders), spec)
    for o in orders:
        o["qty"] = qty
    return ladder, orders, qty


def preview(data: dict) -> dict:
    prm = grid_params(data)
    spec = spec_of(prm["symbol"])
    live = ticker(prm["symbol"])
    last = float(live["lastPrice"])
    lev = min(prm["lev"], int(spec["max_lev"]))
    _, orders, qty = build(prm["symbol"], prm["lower"], prm["upper"], prm["count"], prm["margin"], lev, prm["geo"],
                           prm["geoMult"], spec, last, float(live.get("bid1Price") or last), float(live.get("ask1Price") or last), prm["mode"])
    step = (prm["upper"] - prm["lower"]) / prm["count"]
    fee = 0.0002 * 2  # мейкер з обох боків
    out = {
        "symbol": prm["symbol"], "last": last, "lev": lev, "qty": qty, "step": step, "geo": prm["geo"], "mode": prm["mode"],
        "buys": sum(1 for o in orders if o["side"] == "Buy"), "sells": sum(1 for o in orders if o["side"] == "Sell"),
        "profitLow": round((step / prm["upper"] - fee) * 100, 3), "profitHigh": round((step / prm["lower"] - fee) * 100, 3),
        "inside": prm["lower"] < last < prm["upper"],
        "notional": round(sum(float(o["qty"]) * float(o["price"]) for o in orders), 2),
    }
    if prm["geo"]:
        ratio = (prm["upper"] / prm["lower"]) ** (1 / prm["count"]) - 1
        out.update({
            "geoMult": prm["geoMult"], "stepPct": round(ratio * 100, 3),
            "profitLow": round((ratio - fee) * 100, 3), "profitHigh": round((ratio - fee) * 100, 3),
            "lots": [{"side": o["side"], "price": o["price"], "qty": o["qty"]} for o in sorted(orders, key=lambda o: -float(o["price"]))],
        })
    return out


def place_counter(symbol: str, order: dict, bid: float, ask: float) -> None:
    """PostOnly; якщо ціна вже перетнула лінію — GTC, щоб мікротейк не пропав."""
    cross = (order["side"] == "Sell" and float(order["price"]) <= ask) or (order["side"] == "Buy" and float(order["price"]) >= bid)
    place_limit(symbol, order, post_only=not cross)


def start_grid(data: dict) -> str:
    prm = grid_params(data)
    symbol = prm["symbol"]
    with LOCK:
        if any(g["symbol"] == symbol and g["status"] == "active" for g in STATE["grids"]):
            raise BybitError(f"На {symbol} вже є жива сітка. Спочатку зупиніть її.")
    spec = instrument(symbol)
    SPECS[symbol] = spec
    live = ticker(symbol)
    last = float(live["lastPrice"])
    bid = float(live.get("bid1Price") or last)
    ask = float(live.get("ask1Price") or last)
    lev = min(prm["lev"], int(spec["max_lev"]))
    ladder, orders, qty = build(symbol, prm["lower"], prm["upper"], prm["count"], prm["margin"], lev,
                                prm["geo"], prm["geoMult"], spec, last, bid, ask, prm["mode"])
    if not orders:
        where = {"sell": " вище ціни", "buy": " нижче ціни"}.get(prm["mode"], "")
        raise BybitError(f"Немає ліній для лімітів{where}: ціна поза діапазоном або діапазон замалий")
    set_leverage(symbol, lev)
    opened = []
    with GRID_LOCK:
        for order in orders:
            try:
                place_limit(symbol, order)
                opened.append(order)
            except BybitError as exc:
                log(f"{symbol} {order['side']} {order['price']}: {exc}")
            time.sleep(0.12)
    if not opened:
        raise BybitError("Біржа не прийняла жодну лімітку")
    grid = {
        "id": f"m{time.time_ns():x}", "symbol": symbol,
        "lower": prm["lower"], "upper": prm["upper"], "count": prm["count"], "ladder": ladder,
        "step": (prm["upper"] - prm["lower"]) / prm["count"], "tick": spec["tick"], "qty": qty, "lev": lev,
        "margin": prm["margin"], "trail": prm["trail"], "shiftMin": prm["shiftMin"],
        "geo": prm["geo"], "geoMult": prm["geoMult"], "mode": prm["mode"],
        "levels": opened, "status": "active", "created": int(time.time()), "last": last,
        "fills": 0, "takes": 0, "profit": 0.0, "shifts": 0, "outSince": None,
    }
    with LOCK:
        STATE["grids"].insert(0, grid)
        save_state()
    buys = sum(1 for o in opened if o["side"] == "Buy")
    lots = f", геометрична (лот ×{prm['geoMult']:g}, {qty})" if prm["geo"] else f", лот {qty}"
    side = f", {MODES[prm['mode']]}" if prm["mode"] != "both" else ""
    text = f"Грід {symbol}: {prm['lower']:g}–{prm['upper']:g}, {prm['count']} сіток{side}{lots}, Buy {buys} / Sell {len(opened) - buys}, x{lev}"
    add_event(text)
    return text


def shift_grid(grid: dict, last: float, bid: float, ask: float) -> str:
    symbol = grid["symbol"]
    old_lo, old_hi = grid["lower"], grid["upper"]
    mode = grid.get("mode") or "both"
    # частка ширини нижче ціни: нейтральна — посередині, Sell — ціна біля низу, Buy — біля верху
    below = {"both": 0.5, "sell": 0.1, "buy": 0.9}[mode]
    if grid.get("geo"):
        ratio = old_hi / old_lo  # той самий % ширини
        lower, upper = last / ratio ** below, last * ratio ** (1 - below)
        ladder = ladder_of(lower, upper, grid["count"], grid["tick"], True)
        orders = grid_orders(symbol, ladder, last, bid, ask, "0", mode)
        qty = geo_sizes(orders, ladder, last, grid["margin"] * grid["lev"], float(grid.get("geoMult") or 1.3), spec_of(symbol))
    else:
        width = old_hi - old_lo
        lower, upper = max(last - width * below, last * 0.05), last + width * (1 - below)
        ladder = ladder_of(lower, upper, grid["count"], grid["tick"])
        qty = grid["qty"]
        orders = grid_orders(symbol, ladder, last, bid, ask, qty, mode)
    # лоти пораховані до зняття старих ліміток: помилка розрахунку не лишить сітку порожньою
    cancel_grid_orders(grid, "shift")
    grid["qty"] = qty
    # не нарощувати один бік: бік, що вже тримає маржа × плече, нових ліміток не отримує
    budget = grid["margin"] * grid["lev"]
    held = {p.get("side"): float(p.get("size") or 0) * last for p in positions(symbol)}
    skipped = [s for s in ("Buy", "Sell") if held.get(s, 0) >= budget]
    orders = [o for o in orders if o["side"] not in skipped]
    placed = 0
    for order in orders:
        try:
            place_limit(symbol, order)
            grid["levels"].append(order)
            placed += 1
        except BybitError as exc:
            log(f"зсув {symbol} {order['price']}: {exc}")
        time.sleep(0.12)
    done = [l for l in grid["levels"] if l["status"] != "open"]
    grid["levels"] = done[-80:] + [l for l in grid["levels"] if l["status"] == "open"]
    grid.update({"lower": lower, "upper": upper, "ladder": ladder, "shifts": int(grid.get("shifts") or 0) + 1})
    return (
        f"Грід {symbol} іде за ціною {'вгору' if last > (old_lo + old_hi) / 2 else 'вниз'}: "
        f"{old_lo:g}–{old_hi:g} → {lower:g}–{upper:g}, лімітів {placed}."
        + (f" Без {'/'.join(skipped)}: позиція вже повна." if skipped else "")
    )


def watch_grid(grid: dict) -> list[str]:
    notes: list[str] = []
    symbol = grid["symbol"]
    live = ticker(symbol)
    last = float(live["lastPrice"])
    bid = float(live.get("bid1Price") or last)
    ask = float(live.get("ask1Price") or last)
    grid["last"] = last
    now = time.time()
    if last > grid["upper"] or last < grid["lower"]:
        if not grid.get("outSince"):
            grid["outSince"] = now
            if grid.get("trail"):
                notes.append(f"Грід {symbol}: ціна {last:g} поза діапазоном. Через {grid.get('shiftMin', 0)} хв сітка піде за ціною.")
        elif grid.get("trail") and now - float(grid["outSince"]) >= int(grid.get("shiftMin") or 0) * 60:
            grid["outSince"] = None
            notes.append(shift_grid(grid, last, bid, ask))
            return notes
    else:
        grid["outSince"] = None
    book = bybit("/v5/order/realtime", query={"category": "linear", "symbol": symbol, "limit": "50"}, auth=True)
    resting = {o.get("orderLinkId") for o in book.get("list") or [] if o.get("orderStatus") in {"New", "PartiallyFilled"}}
    ladder = grid.get("ladder") or []
    for level in list(grid["levels"]):
        if level["status"] != "open" or level["orderLinkId"] in resting:
            continue
        hist = bybit("/v5/order/history", query={"category": "linear", "symbol": symbol, "orderLinkId": level["orderLinkId"], "limit": "5"}, auth=True)
        row = next((r for r in hist.get("list") or [] if r.get("orderLinkId") == level["orderLinkId"]), None)
        if not row:
            continue
        filled = row.get("orderStatus") == "Filled" or float(row.get("cumExecQty") or 0) > 0
        level["status"] = "done"
        level["note"] = "filled" if filled else row.get("orderStatus")
        if not filled:
            continue
        fill_px = float(row.get("avgPrice") or level["price"])
        fill_qty = float(row.get("cumExecQty") or level["qty"])
        grid["fills"] = int(grid.get("fills") or 0) + 1
        is_take = level.get("role") == "take" or (level.get("role") is None and bool(level.get("pair")))
        if is_take:
            # тейк попереднього входу: один цикл сітки = один мікротейк
            gain = (fill_px - float(level["pair"])) * fill_qty * (1 if level["side"] == "Sell" else -1)
            grid["takes"] = int(grid.get("takes") or 0) + 1
            grid["profit"] = float(grid.get("profit") or 0) + gain
            notes.append(f"Мікротейк {symbol}: {level['side']} @ {fill_px:g} (від {float(level['pair']):g}), +{gain:.4f} USDT до комісій.")
        # протилежна лімітка на сусідній лінії, тим самим лотом
        if level["price"] in ladder:
            i = ladder.index(level["price"])
        else:
            i = min(range(len(ladder)), key=lambda k: abs(float(ladder[k]) - fill_px)) if ladder else -1
        j = i + 1 if level["side"] == "Buy" else i - 1
        if i < 0 or j < 0 or j >= len(ladder):
            continue  # край діапазону
        side = "Sell" if level["side"] == "Buy" else "Buy"
        price = ladder[j]
        if any(l["status"] == "open" and l["price"] == price for l in grid["levels"]):
            continue
        one_side = (grid.get("mode") or "both") != "both"
        if is_take:
            # тейк виконався — на сусідній лінії знову вхід (у режимі Sell це знову Sell)
            new = {"side": side, "price": price, "qty": level["qty"], "orderLinkId": link_id(symbol, side, price),
                   "status": "open", "note": "grid", "role": "entry"}
        else:
            # вхід виконався — тейк на сусідній лінії; в однобічному режимі reduce-only (не відкриє зустрічну позицію)
            new = {"side": side, "price": price, "qty": level["qty"], "orderLinkId": link_id(symbol, side, price),
                   "status": "open", "note": "counter", "role": "take", "pair": f"{fill_px}", "reduce": one_side}
        try:
            place_counter(symbol, new, bid, ask)
            grid["levels"].append(new)
        except BybitError as exc:
            notes.append(f"Грід {symbol}: не поставив {side} @ {price} ({exc}).")
        time.sleep(0.1)
    return notes


def finish_grid(grid: dict, status: str) -> None:
    grid["status"] = status
    grid["closedAt"] = int(time.time())


def batch_cancel(symbol: str, links: list[str]) -> None:
    """Знімає заявки пачками по 20 (швидко); якщо пачка не пройшла — по одній."""
    links = [l for l in links if l.startswith(LINK_PREFIX)]
    for i in range(0, len(links), 20):
        chunk = links[i:i + 20]
        try:
            bybit("/v5/order/cancel-batch", "POST", body={
                "category": "linear", "request": [{"symbol": symbol, "orderLinkId": l} for l in chunk],
            }, auth=True)
        except BybitError as exc:
            log(f"пачка {symbol}: {exc} — знімаю по одній")
            for link in chunk:
                try:
                    cancel_link(symbol, link)
                except BybitError as e:
                    log(f"зняти {symbol} {link}: {e}")


def cancel_mine_on_exchange(symbol: str) -> int:
    """Усі заявки бота на монеті, які зараз висять на біржі (навіть якщо їх немає в стані)."""
    links = [o["link"] for o in open_orders(symbol) if o["link"].startswith(LINK_PREFIX)]
    if links:
        batch_cancel(symbol, links)
    return len(links)


def emergency_close(grid: dict) -> str:
    """Екстрено: сітка одразу перестає ставити лімітки, усі лімітки бота знімаються, позиція закривається ринком
    (до 3 спроб), потім перевірка, що позиції й заявок бота не лишилось."""
    symbol = grid["symbol"]
    grid["status"] = "closing"  # цикл більше не чіпає цю сітку і не ставить протилежних ліміток
    closed: list[str] = []
    errors: list[str] = []
    left: list[dict] = []
    with GRID_LOCK:
        batch_cancel(symbol, [l["orderLinkId"] for l in grid["levels"] if l["status"] == "open"])
        for level in grid["levels"]:
            if level["status"] == "open":
                level["status"] = "done"
                level["note"] = "emergency"
        try:
            cancel_mine_on_exchange(symbol)
        except BybitError as exc:
            errors.append(f"заявки: {exc}")
        for _ in range(3):
            try:
                if not positions(symbol):
                    break
                closed.append(close_position(symbol))
            except BybitError as exc:
                errors.append(str(exc))
            time.sleep(0.4)
        try:
            left = positions(symbol)
            cancel_mine_on_exchange(symbol)  # раптом щось встигло з'явитись
        except BybitError as exc:
            errors.append(f"перевірка: {exc}")
        finish_grid(grid, "stopped" if left else "closed")
    with LOCK:
        save_state()
    if left:
        size = ", ".join(f"{p.get('side')} {p.get('size')}" for p in left)
        text = (f"⛔ ЕКСТРЕНО {symbol}: лімітки знято, але позицію НЕ закрито ({size}). "
                f"Закрийте її вручну на Bybit! {'; '.join(errors[-2:])}")
    else:
        text = f"⛔ Екстрено закрито {symbol}: лімітки знято, позицію закрито ({'; '.join(closed) or 'позиції не було'})."
    notify(text)
    return text


def emergency(grid_id: str) -> str:
    """Один id — одна сітка; порожній id — усі живі сітки."""
    with LOCK:
        grids = [g for g in STATE["grids"] if g["status"] == "active" and (not grid_id or g["id"] == grid_id)]
    if not grids:
        raise BybitError("Живих сіток немає" if not grid_id else "Сітку не знайдено")
    return "\n".join(emergency_close(g) for g in grids)


def stop_grid(grid_id: str, close: bool) -> str:
    if close:
        return emergency(grid_id)
    with LOCK:
        grid = next((g for g in STATE["grids"] if g["id"] == grid_id and g["status"] == "active"), None)
    if not grid:
        raise BybitError("Сітку не знайдено")
    with GRID_LOCK:
        cancel_grid_orders(grid, "stop")
        if close:
            closed = close_position(grid["symbol"])
            finish_grid(grid, "closed")
            text = f"Грід {grid['symbol']} закрито вручну: ліміти знято, позицію закрито ({closed})."
        else:
            finish_grid(grid, "stopped")
            text = f"Грід {grid['symbol']}: ліміти знято, позиція (якщо є) лишилась під стоп-лосом."
    with LOCK:
        save_state()
    add_event(text)
    return text


# ---------------- стоп-лос ----------------

def guard() -> list[str]:
    """Критичний стоп-лос лише для монет з живими сітками: стоп на біржі + програмна перевірка."""
    notes: list[str] = []
    if CFG.sl_roi <= 0:
        return notes
    with LOCK:
        active = {g["symbol"]: g for g in STATE["grids"] if g["status"] == "active"}
    if not active:
        return notes
    r = CFG.sl_roi / 100
    for p in positions():
        symbol = p["symbol"]
        grid = active.get(symbol)
        if not grid:
            continue
        size = float(p.get("size") or 0)
        avg = float(p.get("avgPrice") or 0)
        lev = float(p.get("leverage") or 0) or float(grid.get("lev") or 1)
        upnl = float(p.get("unrealisedPnl") or 0)
        margin = avg * size / lev if lev > 0 else avg * size
        roi = upnl / margin * 100 if margin > 0 else 0.0
        if roi <= -CFG.sl_roi:
            with GRID_LOCK:
                cancel_grid_orders(grid, "stoploss")
                try:
                    closed = close_position(symbol)
                except BybitError as exc:
                    notes.append(f"СТОП-ЛОС {symbol}: не вдалося закрити ({exc}), повторюю.")
                    continue
                finish_grid(grid, "stoploss")
            notes.append(f"СТОП-ЛОС {roi:.0f}% від маржі {symbol}: ліміти знято, позицію закрито ({closed}).")
            continue
        long = p.get("side") == "Buy"
        raw = avg * (1 - r / lev) if long else avg * (1 + r / lev)
        if raw <= 0 or avg <= 0:
            continue
        tick = spec_of(symbol)["tick"]
        price = align(raw, tick, not long)
        current = float(p.get("stopLoss") or 0)
        if current and abs(current - float(price)) <= float(tick) * 1.5:
            continue
        try:
            bybit("/v5/position/trading-stop", "POST", body={
                "category": "linear", "symbol": symbol, "stopLoss": price, "slTriggerBy": "MarkPrice",
                "tpslMode": "Full", "positionIdx": int(p.get("positionIdx") or 0),
            }, auth=True)
            log(f"стоп {symbol} {p.get('side')} -> {price} (-{CFG.sl_roi:.0f}% від маржі)")
        except BybitError as exc:
            if "not modified" in str(exc).lower() or exc.code == 34040:
                continue
            key = f"{symbol}:{price}"
            if grid.get("slErr") != key:
                grid["slErr"] = key
                notes.append(f"{symbol}: біржа не прийняла стоп {price} ({exc}). Програмний стоп працює, але краще зменшити плече.")
    return notes


# ---------------- авто-підбір параметрів ----------------

def pct_rank(values: list[float], q: float) -> float:
    vals = sorted(values)
    if not vals:
        return 0.0
    return vals[min(len(vals) - 1, max(0, int(round(q * (len(vals) - 1)))))]


def auto_params(data: dict) -> dict:
    """Інтрадей: канал за 24 год без шпильок, крок від 5-хв волатильності, кількість сіток і плече."""
    symbol = parse_symbol(data.get("symbol"))
    margin = parse_num(data.get("margin") or 20, "Маржа", 1, 1000000)
    if str(data.get("profile") or "") == "scalp":
        return scalp_params(symbol, margin, data)
    spec = spec_of(symbol)
    last = float(ticker(symbol)["lastPrice"])
    h15 = candles(symbol, "15", 96)
    m5 = candles(symbol, "5", 144)
    if len(h15) < 40 or len(m5) < 40:
        raise BybitError(f"{symbol}: замало свічок для аналізу")
    lower = pct_rank([c["low"] for c in h15], 0.05)
    upper = pct_rank([c["high"] for c in h15], 0.95)
    if not lower < last < upper:
        half = max(upper - lower, last * 0.02) / 2
        lower, upper = last - half, last + half
    width = upper - lower
    lower = min(lower, last - width * 0.25)
    upper = max(upper, last + width * 0.25)
    width = upper - lower
    width_pct = width / last * 100
    trs = [max(m5[i]["high"] - m5[i]["low"], abs(m5[i]["high"] - m5[i - 1]["close"]), abs(m5[i]["low"] - m5[i - 1]["close"])) for i in range(1, len(m5))]
    atr_pct = sum(trs[-14:]) / 14 / last * 100
    step_pct = min(1.0, max(0.15, atr_pct * 0.5))
    count = int(round(width_pct / step_pct))
    lev = int(max(2, min(25, int(spec["max_lev"]), round(20 / max(width_pct, 0.5)))))
    if str(data.get("leverage") or "").strip():
        lev = int(min(parse_num(data["leverage"], "Плече", 1, 125, integer=True), spec["max_lev"]))
    min_n = spec.get("min_notional") or 5
    fit = int(margin * lev // max(min_n * 1.05, spec["min_qty"] * last * 1.05))
    if fit < 4:
        raise BybitError(f"Замало маржі для {symbol}: {margin:g} USDT × x{lev} вистачає лише на {fit} ліміток по мін. {min_n:g} USDT")
    count = max(4, min(count, 100, fit))
    step = width / count
    takes = sum(int((c["high"] - c["low"]) // step) for c in m5) // 2
    tick = spec["tick"]
    return {
        "symbol": symbol, "last": last,
        "lower": align(lower, tick, False), "upper": align(upper, tick, True),
        "count": count, "leverage": lev, "margin": margin, "trail": True, "shiftMin": 0,
        "widthPct": round(width_pct, 2), "atrPct": round(atr_pct, 3), "stepPct": round(step / last * 100, 3),
        "profitPct": round((step / upper - 0.0004) * 100, 3), "takes12h": takes,
        "why": (
            f"Канал за 24 год без шпильок: {width_pct:.2f}%. Волатильність 5 хв (ATR): {atr_pct:.3f}%. "
            f"Крок {step / last * 100:.3f}% ≈ половина 5-хв руху — лінії перетинаються часто, але крок вищий за комісії. "
            f"Плече x{lev} під ширину каналу. Сітка йде за ціною одразу."
        ),
    }


def scalp_params(symbol: str, margin: float, data: dict) -> dict:
    """Скальпінг: вузький канал за 4 год, малий крок від 1-хв волатильності, багато ліній."""
    spec = spec_of(symbol)
    last = float(ticker(symbol)["lastPrice"])
    m5 = candles(symbol, "5", 48)
    m1 = candles(symbol, "1", 120)
    if len(m5) < 24 or len(m1) < 40:
        raise BybitError(f"{symbol}: замало свічок для скальпінгу")
    lower = pct_rank([c["low"] for c in m5], 0.10)
    upper = pct_rank([c["high"] for c in m5], 0.90)
    width_pct = max(1.0, min(4.0, (upper - lower) / last * 100))
    lower, upper = last * (1 - width_pct / 200), last * (1 + width_pct / 200)
    trs = [max(m1[i]["high"] - m1[i]["low"], abs(m1[i]["high"] - m1[i - 1]["close"]), abs(m1[i]["low"] - m1[i - 1]["close"])) for i in range(1, len(m1))]
    atr1_pct = sum(trs[-30:]) / 30 / last * 100
    step_pct = min(0.4, max(CFG.scalp_min_step, atr1_pct * 0.7))
    count = int(round(width_pct / step_pct))
    lev = int(max(3, min(20, int(spec["max_lev"]), round(15 / width_pct))))
    if str(data.get("leverage") or "").strip():
        lev = int(min(parse_num(data["leverage"], "Плече", 1, 125, integer=True), spec["max_lev"]))
    min_n = spec.get("min_notional") or 5
    fit = int(margin * lev // max(min_n * 1.05, spec["min_qty"] * last * 1.05))
    if fit < 6:
        raise BybitError(
            f"Для скальпінгу на {symbol} замало маржі: {margin:g} USDT × x{lev} вистачає лише на {fit} ліміток по мін. {min_n:g} USDT. "
            f"Потрібно хоча б ~{math.ceil(min_n * 1.05 * 12 / lev):g} USDT маржі."
        )
    count = max(6, min(count, 100, fit))
    step = (upper - lower) / count
    tick = spec["tick"]
    if step < float(tick) * 3:
        raise BybitError(f"{symbol}: крок {step:g} менший за 3 тіки ціни — оберіть іншу монету для скальпінгу")
    takes = sum(int((c["high"] - c["low"]) // step) for c in m1) // 2
    return {
        "symbol": symbol, "last": last, "profile": "scalp",
        "lower": align(lower, tick, False), "upper": align(upper, tick, True),
        "count": count, "leverage": lev, "margin": margin, "trail": True, "shiftMin": 0,
        "widthPct": round(width_pct, 2), "atrPct": round(atr1_pct, 3), "stepPct": round(step / last * 100, 3),
        "profitPct": round((step / upper - 0.0004) * 100, 3), "takes12h": takes, "takesWindow": "2 год",
        "why": (
            f"Скальпінг: вузький канал {width_pct:.2f}% навколо ціни (за 4 год). Рух за 1 хв: {atr1_pct:.3f}%. "
            f"Крок {step / last * 100:.3f}% (≈70% хвилинного руху, не менше {CFG.scalp_min_step:g}%). "
            f"{count} ліній, плече x{lev}, сітка йде за ціною одразу."
        ),
    }


# ---------------- пошук монет для нейтрального гріду на годину ----------------
# Сенс: за останні 5 хв ціна ходить широко, але туди-сюди (малий зсув), а за останню годину — без тренду,
# часто перетинає середину каналу і зараз не на краю. Такі монети найчастіше дають мікротейки в сітці.

SCAN_LOCK = threading.Lock()
SCAN_CACHE: dict = {"at": 0.0, "margin": None, "result": None}
ALL_SPECS = {"at": 0.0}
SCAN_MIN_TURNOVER = env_float("SCAN_MIN_TURNOVER", 5_000_000, 0, 1e12)   # USDT за 24 год
SCAN_POOL = env_int("SCAN_POOL", 80, 10, 300)                            # скільки найліквідніших монет перевіряти


def load_all_specs() -> None:
    """Параметри всіх USDT-перпів одним запитом (раз на 30 хв): тік, крок кількості, мін. сума, макс. плече."""
    if time.time() - ALL_SPECS["at"] < 1800:
        return
    cursor = ""
    for _ in range(5):
        query = {"category": "linear", "limit": "1000"}
        if cursor:
            query["cursor"] = cursor
        res = bybit("/v5/market/instruments-info", query=query)
        for row in res.get("list") or []:
            sym = str(row.get("symbol") or "")
            if not sym.endswith("USDT") or row.get("contractType") not in (None, "LinearPerpetual") or row.get("status") not in (None, "Trading"):
                continue
            try:
                SPECS[sym] = {
                    "tick": row["priceFilter"]["tickSize"], "qty": row["lotSizeFilter"]["qtyStep"],
                    "min_qty": float(row["lotSizeFilter"].get("minOrderQty") or 0),
                    "min_notional": float(row["lotSizeFilter"].get("minNotionalValue") or 5),
                    "max_lev": float((row.get("leverageFilter") or {}).get("maxLeverage") or 20),
                }
            except (KeyError, TypeError, ValueError):
                continue
        cursor = res.get("nextPageCursor") or ""
        if not cursor:
            break
    ALL_SPECS["at"] = time.time()


def judge_coin(t: dict, margin: float) -> dict | None:
    symbol = t["symbol"]
    spec = SPECS.get(symbol)
    if not spec:
        return None
    rows = candles(symbol, "1", 61)  # остання година хвилинками (+ поточна)
    if len(rows) < 30:
        return None
    last = float(t.get("lastPrice") or rows[-1]["close"])
    if last <= 0:
        return None
    # 5 хв: розмах і чистий зсув
    r5 = rows[-5:]
    hi5, lo5 = max(c["high"] for c in r5), min(c["low"] for c in r5)
    range5 = (hi5 - lo5) / last * 100
    first_open = rows[-6]["close"] if len(rows) >= 6 else r5[0]["low"]
    net5 = abs(rows[-1]["close"] - first_open) / last * 100
    er5 = min(1.0, net5 / range5) if range5 > 0 else 1.0
    # година: розмах, зсув, перетини середини, де ціна зараз
    hi60, lo60 = max(c["high"] for c in rows), min(c["low"] for c in rows)
    range60 = (hi60 - lo60) / last * 100
    net60 = abs(rows[-1]["close"] - rows[0]["close"]) / last * 100
    er60 = min(1.0, net60 / range60) if range60 > 0 else 1.0
    mid = (hi60 + lo60) / 2
    signs = [1 if c["close"] >= mid else -1 for c in rows]
    crosses = sum(1 for a, b in zip(signs, signs[1:]) if a != b)
    pos = (last - lo60) / (hi60 - lo60) if hi60 > lo60 else 0.5
    centre = 1 - abs(pos - 0.5) * 2  # 1 = посередині, 0 = на краю
    # пробій: за останні 5 хв ціна вийшла за канал попередніх 55 хв — це тренд, а не хвиля всередині каналу
    prev = rows[:-5]
    phi, plo = max(c["high"] for c in prev), min(c["low"] for c in prev)
    prng = phi - plo
    breakout = prng > 0 and (last > phi + prng * 0.1 or last < plo - prng * 0.1)
    # сітка на годину: канал години (без шпильок) із запасом 20%, але не вужча за 1.5 × 5-хв розмаху; ціна посередині
    channel = (pct_rank([c["high"] for c in rows], 0.9) - pct_rank([c["low"] for c in rows], 0.1)) / last * 100
    width_pct = max(channel * 1.2, range5 * 1.5)
    width_pct = max(0.8, min(6.0, width_pct))
    trs = [max(rows[i]["high"] - rows[i]["low"], abs(rows[i]["high"] - rows[i - 1]["close"]), abs(rows[i]["low"] - rows[i - 1]["close"]))
           for i in range(max(1, len(rows) - 15), len(rows))]
    atr1 = sum(trs) / len(trs) / last * 100 if trs else 0
    step_pct = min(0.5, max(CFG.scalp_min_step, atr1 * 0.6))
    count = int(round(width_pct / step_pct))
    lev = int(max(3, min(20, int(spec["max_lev"]), round(15 / width_pct))))
    min_n = spec.get("min_notional") or 5
    fit = int(margin * lev // max(min_n * 1.05, spec["min_qty"] * last * 1.05))
    count = max(6, min(count, 100, fit)) if fit >= 6 else 0
    lower, upper = last * (1 - width_pct / 200), last * (1 + width_pct / 200)
    step = (upper - lower) / count if count else 0
    takes = sum(int((c["high"] - c["low"]) // step) for c in rows) // 2 if step > 0 else 0
    # за 5 хв одна хвиля коливання схожа на тренд, тому зсув 5 хв має меншу вагу, ніж тренд години й пробій
    score = range5 * (1 - 0.5 * er5) * (1 - 0.7 * er60) * (0.5 + 0.5 * centre) * (1 + min(crosses, 20) / 20)
    if breakout:
        score *= 0.2
    strict = range5 >= 0.2 and not breakout and er5 <= 0.85 and er60 <= 0.55 and centre >= 0.2 and count > 0
    tick = spec["tick"]
    return {
        "symbol": symbol, "last": last, "turnover": float(t.get("turnover24h") or 0),
        "range5": round(range5, 3), "net5": round(net5, 3), "trend5": round(er5 * 100),
        "range60": round(range60, 3), "trend60": round(er60 * 100), "crosses": crosses, "pos": round(pos * 100),
        "breakout": breakout,
        "score": round(score, 4), "strict": strict,
        "lower": align(lower, tick, False), "upper": align(upper, tick, True), "count": count, "leverage": lev,
        "widthPct": round(width_pct, 2), "stepPct": round(width_pct / count, 3) if count else 0,
        "profitPct": round((width_pct / count - 0.04) if count else 0, 3), "takesHour": takes,
        "fit": fit,
    }


def scan_coins(data: dict) -> dict:
    margin = parse_num(data.get("margin") or 20, "Маржа", 1, 1000000)
    with SCAN_LOCK:
        cached = SCAN_CACHE["result"]
        if cached and time.time() - SCAN_CACHE["at"] < 20 and SCAN_CACHE["margin"] == margin:
            return cached
        started = time.time()
        load_all_specs()
        tickers = bybit("/v5/market/tickers", query={"category": "linear"}).get("list") or []
        skip = ("USDCUSDT", "USDEUSDT", "FDUSDUSDT", "DAIUSDT", "TUSDUSDT")
        pool = [
            t for t in tickers
            if str(t.get("symbol", "")).endswith("USDT") and t["symbol"] not in skip and t["symbol"] in SPECS
            and float(t.get("turnover24h") or 0) >= SCAN_MIN_TURNOVER and float(t.get("lastPrice") or 0) > 0
        ]
        pool.sort(key=lambda t: float(t.get("turnover24h") or 0), reverse=True)
        pool = pool[:SCAN_POOL]
        found: list[dict] = []
        errors = 0

        def one(t: dict):
            try:
                return judge_coin(t, margin)
            except BybitError:
                return "err"

        with ThreadPoolExecutor(max_workers=8) as ex:
            for res in ex.map(one, pool):
                if res == "err":
                    errors += 1
                elif res:
                    found.append(res)
        strict = sorted([c for c in found if c["strict"]], key=lambda c: -c["score"])
        relaxed = not strict
        top = (strict or sorted([c for c in found if c["count"] > 0], key=lambda c: -c["score"]))[:12]
        with LOCK:
            busy = {g["symbol"] for g in STATE["grids"] if g["status"] == "active"}
        for c in top:
            c["busy"] = c["symbol"] in busy
        result = {
            "coins": top, "checked": len(found), "pool": len(pool), "errors": errors, "relaxed": relaxed,
            "margin": margin, "sec": round(time.time() - started, 1), "at": int(time.time()),
        }
        SCAN_CACHE.update({"at": time.time(), "margin": margin, "result": result})
        return result


def price_of(symbol) -> dict:
    symbol = parse_symbol(symbol)
    spec = spec_of(symbol)
    return {"symbol": symbol, "last": float(ticker(symbol)["lastPrice"]), "tick": spec["tick"], "maxLev": spec["max_lev"]}


# ---------------- графік і заявки ----------------

INTERVALS = {"1s", "1", "5", "15", "60", "240", "D"}
OI_INTERVAL = {"1s": "5min", "1": "5min", "5": "5min", "15": "15min", "60": "1h", "240": "4h", "D": "1d"}


def open_interest(symbol: str, interval: str) -> list[dict]:
    res = bybit("/v5/market/open-interest", query={
        "category": "linear", "symbol": symbol, "intervalTime": OI_INTERVAL.get(interval, "5min"), "limit": "200",
    })
    return [
        {"time": int(r["timestamp"]) // 1000, "value": float(r["openInterest"])}
        for r in reversed(res.get("list") or []) if r.get("timestamp") and r.get("openInterest")
    ]


def recent_trades(symbol) -> dict:
    """Останні 1000 угод — стартова точка для секундних свічок і CVD (далі браузер бере угоди наживо)."""
    symbol = parse_symbol(symbol)
    res = bybit("/v5/market/recent-trade", query={"category": "linear", "symbol": symbol, "limit": "1000"})
    trades = [
        {"id": r.get("execId"), "t": int(r.get("time") or 0), "p": float(r.get("price") or 0),
         "v": float(r.get("size") or 0), "s": r.get("side")}
        for r in res.get("list") or [] if not r.get("isBlockTrade")
    ]
    trades.sort(key=lambda r: r["t"])
    return {"symbol": symbol, "trades": trades}


def open_orders(symbol: str) -> list[dict]:
    """Усі відкриті заявки на монеті (до 200), з позначкою, чи це заявка цього бота."""
    out: list[dict] = []
    cursor = ""
    for _ in range(4):
        query = {"category": "linear", "symbol": symbol, "limit": "50"}
        if cursor:
            query["cursor"] = cursor
        res = bybit("/v5/order/realtime", query=query, auth=True)
        for o in res.get("list") or []:
            out.append({
                "side": o.get("side"), "price": o.get("price") or o.get("triggerPrice"), "qty": o.get("qty"),
                "filled": o.get("cumExecQty") or "0", "type": o.get("orderType"), "link": o.get("orderLinkId") or "",
                "reduce": bool(o.get("reduceOnly")), "stop": o.get("stopOrderType") or "",
            })
        cursor = res.get("nextPageCursor") or ""
        if not cursor:
            break
    return out


def chart_data(symbol, interval) -> dict:
    symbol = parse_symbol(symbol)
    interval = str(interval or "5")
    if interval not in INTERVALS:
        interval = "5"
    candles_: list[dict] = []
    if interval != "1s":  # секундні свічки браузер будує сам з потоку угод
        res = bybit("/v5/market/kline", query={"category": "linear", "symbol": symbol, "interval": interval, "limit": "300"})
        candles_ = [
            {"time": int(k[0]) // 1000, "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
             "v": float(k[5] or 0)}
            for k in reversed(res.get("list") or [])
        ]
    try:
        oi = open_interest(symbol, interval)
    except BybitError as exc:
        log(f"OI {symbol}: {exc}")
        oi = []
    with LOCK:
        grid = next((g for g in STATE["grids"] if g["symbol"] == symbol and g["status"] == "active"), None)
        notes = {l["orderLinkId"]: l.get("note") for l in (grid or {}).get("levels") or []}
        grid_info = {"lower": grid["lower"], "upper": grid["upper"], "ladder": list(grid.get("ladder") or [])} if grid else None
    orders: list[dict] = []
    position = None
    if CFG.api_key and CFG.api_secret:
        for o in open_orders(symbol):
            o["mine"] = o["link"].startswith(LINK_PREFIX)
            o["kind"] = "тейк" if notes.get(o["link"]) == "counter" else "сітка" if o["mine"] else "чужа"
            orders.append(o)
        pos = positions(symbol)
        if pos:
            p = pos[0]
            position = {"side": p.get("side"), "size": p.get("size"), "avg": float(p.get("avgPrice") or 0),
                        "liq": float(p.get("liqPrice") or 0), "sl": float(p.get("stopLoss") or 0),
                        "pnl": float(p.get("unrealisedPnl") or 0)}
    last = candles_[-1]["close"] if candles_ else float(ticker(symbol)["lastPrice"])
    return {"symbol": symbol, "interval": interval, "last": last, "tick": spec_of(symbol)["tick"], "candles": candles_, "oi": oi,
            "orders": orders, "position": position, "grid": grid_info}


# ---------------- цикл ----------------

def loop() -> None:
    """Кожні POLL_SEC секунд: виконання ліміток → протилежна лімітка; раз на ~10 с — стоп-лос."""
    last_guard = 0.0
    while not STOP.is_set():
        HEART["tick"] = time.time()
        notes: list[str] = []
        try:
            with LOCK:
                grids = [g for g in STATE["grids"] if g["status"] == "active"]
            for grid in grids:
                try:
                    with GRID_LOCK:
                        if grid["status"] == "active":
                            notes.extend(watch_grid(grid))
                except Exception as exc:  # noqa: BLE001 — одна монета не зупиняє інші
                    log(f"{grid['symbol']}: {exc}")
            if grids and time.time() - last_guard >= 10:
                last_guard = time.time()
                try:
                    notes.extend(guard())
                except Exception as exc:  # noqa: BLE001
                    log(f"стоп-лос: {exc}")
            if grids:
                with LOCK:
                    save_state()
        except Exception as exc:  # noqa: BLE001
            log(f"цикл: {exc}")
        for note in notes:
            notify(note)
        STOP.wait(CFG.poll)


def refresh_account() -> None:
    if time.time() - ACCOUNT["at"] < 8 or not CFG.api_key:
        return
    ACCOUNT["at"] = time.time()
    try:
        wallet = bybit("/v5/account/wallet-balance", query={"accountType": "UNIFIED"}, auth=True)
        acct = (wallet.get("list") or [{}])[0]
        ACCOUNT.update({
            "equity": acct.get("totalEquity") or "0",
            "available": acct.get("totalAvailableBalance") or "0",
            "positions": [
                {"symbol": p.get("symbol"), "side": p.get("side"), "size": p.get("size"), "avg": p.get("avgPrice"),
                 "pnl": p.get("unrealisedPnl"), "liq": p.get("liqPrice"), "sl": p.get("stopLoss")}
                for p in positions()
            ],
            "error": "",
        })
    except BybitError as exc:
        ACCOUNT["error"] = str(exc)


def grid_rows() -> list[dict]:
    rows = []
    with LOCK:
        grids = [g for g in STATE.get("grids") or [] if g.get("status") == "active"]
    for g in grids:
        opened = [l for l in g["levels"] if l.get("status") == "open"]
        rows.append({
            "id": g["id"], "symbol": g["symbol"], "lower": g["lower"], "upper": g["upper"], "count": g["count"],
            "step": g.get("step"), "qty": g.get("qty"), "lev": g.get("lev"), "margin": g.get("margin"),
            "trail": bool(g.get("trail")), "shiftMin": g.get("shiftMin"), "last": g.get("last"),
            "buys": sum(1 for l in opened if l["side"] == "Buy"), "sells": sum(1 for l in opened if l["side"] == "Sell"),
            "fills": g.get("fills") or 0, "takes": g.get("takes") or 0, "profit": round(float(g.get("profit") or 0), 6),
            "shifts": g.get("shifts") or 0, "out": bool(g.get("outSince")), "created": g.get("created"),
            "geo": bool(g.get("geo")), "geoMult": g.get("geoMult"), "mode": g.get("mode") or "both",
            "stepPct": round(((g["upper"] / g["lower"]) ** (1 / g["count"]) - 1) * 100, 3) if g.get("geo") else None,
        })
    return rows


def snapshot() -> dict:
    refresh_account()
    with LOCK:
        events = list(reversed((STATE.get("events") or [])[-40:]))
    return {
        "env": CFG.env, "keys": bool(CFG.api_key and CFG.api_secret), "slRoi": CFG.sl_roi,
        "loopAgo": int(time.time() - HEART["tick"]) if HEART["tick"] else -1,
        "upMin": int((time.time() - HEART["started"]) // 60),
        "grids": grid_rows(), "events": events,
        "account": {k: ACCOUNT[k] for k in ("equity", "available", "positions", "error")},
    }


# ---------------- веб-сторінка ----------------

def serve() -> None:
    port = int(os.environ.get("PORT", "8080"))
    page = HERE / "index.html"

    class Handler(BaseHTTPRequestHandler):
        def send_bytes(self, code: int, body: bytes, content_type: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, code: int, payload: dict) -> None:
            self.send_bytes(code, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")

        def do_GET(self):  # noqa: N802
            path = urllib.parse.urlparse(self.path).path
            if path == "/api/ping":
                self.send_json(200, {"ok": True})
            elif path == "/api/status":
                try:
                    self.send_json(200, snapshot())
                except Exception as exc:  # noqa: BLE001
                    self.send_json(500, {"error": str(exc)})
            else:
                self.send_bytes(200, page.read_bytes() if page.exists() else b"index.html missing", "text/html; charset=utf-8")

        def do_POST(self):  # noqa: N802
            path = urllib.parse.urlparse(self.path).path
            length = int(self.headers.get("Content-Length") or 0)
            try:
                data = json.loads(self.rfile.read(length).decode()) if 0 < length <= 20000 else {}
            except (json.JSONDecodeError, UnicodeDecodeError):
                data = {}
            if not isinstance(data, dict):
                data = {}
            routes = {
                "/api/auto": lambda: auto_params(data),
                "/api/price": lambda: price_of(data.get("symbol")),
                "/api/scan": lambda: scan_coins(data),
                "/api/chart": lambda: chart_data(data.get("symbol"), data.get("interval")),
                "/api/trades": lambda: recent_trades(data.get("symbol")),
                "/api/preview": lambda: preview(data),
                "/api/start": lambda: {"ok": True, "text": start_grid(data)},
                "/api/stop": lambda: {"ok": True, "text": stop_grid(str(data.get("id") or ""), bool(data.get("close")))},
                "/api/emergency": lambda: {"ok": True, "text": emergency(str(data.get("id") or ""))},
            }
            if path not in routes:
                self.send_json(404, {"error": "немає такого запиту"})
                return
            try:
                self.send_json(200, routes[path]())
            except BybitError as exc:
                self.send_json(400, {"error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self.send_json(500, {"error": str(exc)})

        def log_message(self, fmt, *args):
            return

    host = os.environ.get("HOST") or ("127.0.0.1" if LOCAL else "0.0.0.0")
    try:
        httpd = ThreadingHTTPServer((host, port), Handler)
    except OSError as exc:
        log(f"Порт {port} зайнятий ({exc}). Бот, схоже, вже запущений — друга копія не стартує.")
        os._exit(1)
    log(f"сторінка: http://{'127.0.0.1' if host == '0.0.0.0' else host}:{port}")
    httpd.serve_forever()


def keep_alive() -> None:
    """Railway: пінгуємо свою адресу, щоб сервіс не засинав."""
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    if not domain:
        return
    while not STOP.is_set():
        try:
            with urllib.request.urlopen(urllib.request.Request(f"https://{domain}/api/ping", headers={"User-Agent": "keepalive"}), timeout=20) as r:
                r.read()
        except Exception as exc:  # noqa: BLE001
            log(f"keepalive: {exc}")
        STOP.wait(240)


def main() -> None:
    threading.Thread(target=serve, name="http", daemon=True).start()
    if LOCAL:
        url = f"http://127.0.0.1:{int(os.environ.get('PORT', '8080'))}"
        log(f"Бот працює на цьому комп'ютері: {url}. Не закривайте вікно. Зупинити: Ctrl+C.")
        if os.environ.get("OPEN_BROWSER", "1") != "0":
            threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    else:
        threading.Thread(target=keep_alive, name="keepalive", daemon=True).start()
    if not pick_base():
        log("Bybit недоступний з цього сервера")
    if not CFG.api_key or not CFG.api_secret:
        log("немає BYBIT_API_KEY / BYBIT_API_SECRET — сторінка відкривається, ордери не ставляться")
    else:
        try:
            bybit("/v5/account/wallet-balance", query={"accountType": "UNIFIED"}, auth=True)
            log(f"ключ Bybit приймається ({CFG.env})")
        except BybitError as exc:
            log(f"УВАГА: ключ не підходить до {CFG.env} ({exc}). Перевірте BYBIT_ENV = mainnet/testnet")
        if CFG.env == "mainnet":
            log("MAINNET: лімітки реальні")
    try:
        loop()
    except KeyboardInterrupt:
        STOP.set()
        log("Зупинено. Лімітки на біржі лишаються, стопи на позиціях теж.")
        with LOCK:
            save_state()


if __name__ == "__main__":
    main()
