#!/usr/bin/env python3
"""
Тест сеточного бота на истории (минутные свечи).

Логика как в bot.py: геометрическая сетка ±range от цены старта, Post-Only лимитки,
трейлинг вверх/вниз, стоп «цена на 5% ниже сетки» (если трейлинг вниз выключен),
на фьючерсах — плечо, фандинг и аварийное закрытие перед ликвидацией.
Радар не моделируется: для него нужна лента сделок и стакан, в свечах их нет.

Запуск:  python backtest.py            (скачает данные с data.binance.vision)
         python backtest.py --quick    (меньше монет и параметров)
"""
import argparse, csv, io, json, math, os, statistics, sys, time, urllib.request, zipfile
from multiprocessing import Pool

COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "SUIUSDT", "LINKUSDT", "PEPEUSDT"]
MONTHS = ["2026-04", "2026-05", "2026-06", "2026-07", "2026-08", "2026-09"]
TEST_FROM = "2026-08"                 # последние 2 месяца не участвуют в подборе
WINDOW_DAYS, STEP_DAYS = 7, 7

RANGES = [0.03, 0.05, 0.08, 0.12, 0.20]
LEVELS = [5, 8, 12, 20, 30, 45]
TRAILS = ["off", "up", "both"]
LEVS = [1, 2, 3, 5, 10, 20]
MULTS = [1.0, 1.3, 1.6]               # 1.0 = одинаковые лоты, иначе геометрические

SPOT_FEE, FUT_FEE = 0.001, 0.0002     # мейкер Bybit для обычного аккаунта
SLIP = 0.0002                         # «быстрая лимитка» при сдвигах трейлинга
EPS = 0.0001                          # лимитка исполняется, только если цена прошла уровень на 0.01%
FUNDING = 0.0001                      # 0.01% каждые 8 часов на лонг
GUARD = 0.35                          # фьючерсы: закрытие, когда от маржи осталось 35%
STOP_BELOW = 0.05
CAP = 1000.0
DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_cache")


# ---------------------------------------------------------------- данные
def load(coin, month):
    os.makedirs(DATA, exist_ok=True)
    path = os.path.join(DATA, f"{coin}-1m-{month}.csv")
    if not os.path.exists(path):
        url = f"https://data.binance.vision/data/spot/monthly/klines/{coin}/1m/{coin}-1m-{month}.zip"
        try:
            raw = urllib.request.urlopen(url, timeout=120).read()
        except Exception as e:
            print(f"  нет данных {coin} {month}: {e}", flush=True)
            return []
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            with open(path, "wb") as f:
                f.write(z.read(z.namelist()[0]))
    rows = []
    with open(path, newline="") as f:
        for r in csv.reader(f):
            if not r or not r[0].isdigit():
                continue
            t = int(r[0])
            if t > 10 ** 14:          # с 2025 года Binance пишет микросекунды
                t //= 1000
            rows.append((t, float(r[1]), float(r[2]), float(r[3]), float(r[4])))
    return rows


# ---------------------------------------------------------------- симуляция
def estimate_tick(prices):
    """Шаг цены биржи: самая маленькая разница между разными ценами."""
    v = sorted(set(round(x, 12) for x in prices[:60000]))
    diffs = [b - a for a, b in zip(v, v[1:]) if b - a > 0]
    if not diffs:
        return 0.0
    d = min(diffs)
    e = math.floor(math.log10(d))
    for m in (1, 2, 5, 10):                     # округляем до «красивого» шага: 1, 2, 5 × 10^k
        if d <= m * 10 ** e * 1.01:
            return m * 10 ** e
    return d


def sim(O, H, L, C, rng, n, trail, fut, tick=0.0, mult=1.0, trail_pct=0.0, stop_below=STOP_BELOW,
        down_cap=None):
    """Один прогон сетки. Возвращает {плечо: результат}. Фьючерсы считаются один раз при 1x:
    все ордера масштабируются линейно, поэтому PnL при плече L = L × PnL(1x),
    а момент аварийного закрытия ищется отдельно для каждого плеча."""
    fee = FUT_FEE if fut else SPOT_FEE
    levs = LEVS if fut else [1]
    p0 = O[0]
    lo = p0 * (1 - rng)
    hi = p0 * (1 + rng)
    lnr = math.log(hi / lo) / (n - 1)
    if tick > 0:
        lv = lambda k: max(round(lo * math.exp(k * lnr) / tick), 1) * tick
    else:
        lv = lambda k: lo * math.exp(k * lnr)
    half = tick * 0.5
    below = lambda x: min(x - half, x * (1 - EPS))   # покупка исполнится, если цена ушла ниже хотя бы на тик
    above = lambda x: max(x + half, x * (1 + EPS))
    m = max([i for i in range(n - 1) if lv(i) <= p0], default=-1)
    w = {i: mult ** ((m - i) if i <= m else (i - m - 1)) for i in range(n - 1)}
    sw = sum(w.values())
    perk = {i: CAP * w[i] / sw for i in w}       # лот каждой ячейки, USDT
    per = min(perk.values())                     # самый маленький лот — для новых ячеек трейлинга
    kmin = 0
    cash, coin, hold = CAP, 0.0, {}
    for i in range(n - 1):                       # верхние ячейки сразу держат монету
        if lv(i) > p0:
            q = perk[i] / lv(i)
            cash -= q * p0 * (1 + fee + SLIP)
            coin += q
            hold[i] = q
    z = math.floor(math.log(p0 / lo) / lnr)
    trades = shifts = 0
    fund = 0.0
    up = trail in ("up", "both")
    down = trail == "both"
    alive = {L_: True for L_ in levs}
    out = {}
    peak = {L_: CAP for L_ in levs}
    mdd = {L_: 0.0 for L_ in levs}
    stopped_all = None
    tp = max(trail_pct, EPS)
    dn_loss = []                                 # (свеча, убыток) сдвигов вниз — для суточного лимита

    def move(b):
        nonlocal z, cash, coin, trades
        while b < below(lv(z)):                  # вниз через уровень покупки ячейки z
            if kmin <= z <= kmin + n - 2 and z not in hold:
                x = lv(z)
                q = perk.get(z, per) / x
                cash -= q * x * (1 + fee)
                coin += q
                hold[z] = q
                trades += 1
            z -= 1
        while b > above(lv(z + 1)):              # вверх через уровень продажи ячейки z
            q = hold.pop(z, None)
            if q is not None:
                cash += q * lv(z + 1) * (1 - fee)
                coin -= q
                trades += 1
            z += 1

    N = len(O)
    for t in range(N):
        o, h, l, c = O[t], H[t], L[t], C[t]
        if c >= o:
            move(l); eq_low = cash + coin * l; move(h)
        else:
            move(h); move(l); eq_low = cash + coin * l
        move(c)

        # стоп ниже сетки (без трейлинга вниз)
        if not down and stop_below and l < lv(kmin) * (1 - stop_below):
            px = lv(kmin) * (1 - stop_below)
            eq = cash + coin * px * (1 - (fee + SLIP if fut else 0))
            stopped_all = (eq, t)
            break
        # аварийное закрытие перед ликвидацией (фьючерсы)
        if fut:
            for L_ in levs:
                if alive[L_] and CAP + L_ * (eq_low - CAP) - L_ * fund <= GUARD * CAP:
                    alive[L_] = False
                    out[L_] = dict(final=GUARD * CAP * (1 - 0.01), stopped=True, t=t, mdd=1 - GUARD)
            if not any(alive.values()):
                break

        # трейлинг на закрытии свечи
        if up and c > lv(kmin + n - 1) * (1 + tp):
            for _ in range(60):
                if c <= lv(kmin + n - 1) * (1 + EPS) or kmin in hold:
                    break
                kmin += 1
                k = kmin + n - 2
                perk[k] = per
                q = per / c
                cash -= per * (1 + fee + SLIP)
                coin += q
                if c > lv(k + 1) * (1 + EPS):    # цена выше и новой продажи: сразу продаётся
                    cash += q * c * (1 - fee)
                    coin -= q
                    trades += 1
                else:
                    hold[k] = q
                shifts += 1
            z = math.floor(math.log(c / lo) / lnr)
        elif down and c < lv(kmin) * (1 - tp):
            for _ in range(60):
                ktop = kmin + n - 2
                if c >= lv(kmin) * (1 - EPS) or ktop not in hold:
                    break
                if down_cap:
                    dn_loss = [(tt, x) for tt, x in dn_loss if t - tt < 1440]
                    if sum(x for _, x in dn_loss) >= down_cap * CAP:
                        break
                q = hold.pop(ktop)
                dn_loss.append((t, max(q * (lv(ktop) - c), 0.0)))
                cash += q * c * (1 - fee - SLIP)
                coin -= q
                kmin -= 1
                perk[kmin] = per
                if c < lv(kmin) * (1 - EPS):     # цена ниже новой покупки: сразу покупается
                    q2 = per / lv(kmin)
                    cash -= q2 * c * (1 + fee + SLIP)
                    coin += q2
                    hold[kmin] = q2
                shifts += 1
            z = math.floor(math.log(c / lo) / lnr)

        if fut and t % 480 == 479:
            fund += coin * c * FUNDING
        if t % 60 == 59:                          # просадка по часам
            eq = cash + coin * c
            for L_ in levs:
                if alive[L_]:
                    e = CAP + L_ * (eq - CAP) - L_ * fund
                    peak[L_] = max(peak[L_], e)
                    mdd[L_] = max(mdd[L_], 1 - e / peak[L_])

    if stopped_all:
        eq, t_stop = stopped_all
    else:
        eq, t_stop = cash + coin * C[-1], None
    for L_ in levs:
        if L_ in out:
            continue
        final = CAP + L_ * (eq - CAP) - L_ * fund
        if final <= GUARD * CAP:
            final = GUARD * CAP * 0.99
        out[L_] = dict(final=final, stopped=stopped_all is not None, t=t_stop, mdd=mdd[L_])
    for L_ in out:
        out[L_].update(trades=trades, shifts=shifts)
    return out


def configs(quick=False):
    rs = [0.05, 0.12] if quick else RANGES
    ls = [8, 20] if quick else LEVELS
    for fut in (False, True):
        fee = FUT_FEE if fut else SPOT_FEE
        for rng in rs:
            for n in ls:
                step = ((1 + rng) / (1 - rng)) ** (1 / (n - 1)) - 1
                if step < 2 * fee + 0.0005:          # шаг меньше двух комиссий — заведомо в минус
                    continue
                for tr in TRAILS:
                    for mu in ([1.0, 1.3] if quick else MULTS):
                        yield fut, rng, n, tr, step, mu


def trail_configs():
    """Набор для проверки трейлинга по проценту и стопов."""
    for fut in (False, True):
        for rng in (0.05, 0.08, 0.12):
            for n in (5, 8, 12, 20):
                step = ((1 + rng) / (1 - rng)) ** (1 / (n - 1)) - 1
                for mu in (1.0, 1.3, 1.6):
                    for sb in (0.03, 0.05, 0.08, 0.12):
                        yield fut, rng, n, "off", step, mu, 0.0, sb
                    for tp in (0.0, 0.01, 0.02, 0.03, 0.05):
                        yield fut, rng, n, "up", step, mu, tp, 0.05
                        yield fut, rng, n, "both", step, mu, tp, 0.05


def run_window_trail(task):
    coin, start_ts, O, H, L, C, quick, tick, prev = task
    hold = C[-1] / O[0] - 1
    pre_rng = max(prev) / min(prev) - 1 if prev else 0.0
    res = []
    for fut, rng, n, tr, step, mu, tp, sb in trail_configs():
        for L_, r in sim(O, H, L, C, rng, n, tr, fut, tick, mu, tp, sb,
                         0.05 if tr == "both" else None).items():
            if L_ not in (1, 2, 3, 5):
                continue
            res.append(dict(coin=coin, start=start_ts, market="futures" if fut else "spot", range=rng, levels=n,
                            trail=tr, mult=mu, tpct=tp, stop=sb, lev=L_, ret=r["final"] / CAP - 1, mdd=r["mdd"],
                            stopped=r["stopped"], trades=r["trades"], shifts=r["shifts"], hold=hold, pre_rng=pre_rng))
    return res


def run_window(task):
    coin, start_ts, O, H, L, C, quick, tick, prev = task
    res = []
    hold = C[-1] / O[0] - 1
    # что было за 3 дня до старта: для проверки фильтра «запускать только во флэте»
    pre_chg = prev[-1] / prev[0] - 1 if prev else 0.0
    pre_rng = max(prev) / min(prev) - 1 if prev else 0.0
    hourly = prev[::60]
    path = sum(abs(b / a - 1) for a, b in zip(hourly, hourly[1:]))
    pre_chop = path / max(abs(pre_chg), 0.002)
    for fut, rng, n, tr, step, mu in configs(quick):
        if tick and ((1 + rng) / (1 - rng)) ** (1 / (n - 1)) * O[0] - O[0] < 3 * tick:
            continue                              # шаг сетки меньше трёх тиков — так бот не поставит
        for L_, r in sim(O, H, L, C, rng, n, tr, fut, tick, mu).items():
            res.append(dict(coin=coin, start=start_ts, market="futures" if fut else "spot", range=rng, levels=n,
                            step=round(step * 100, 3), trail=tr, mult=mu, lev=L_, ret=r["final"] / CAP - 1,
                            mdd=r["mdd"], stopped=r["stopped"], trades=r["trades"], shifts=r["shifts"],
                            hold=hold, pre_chg=pre_chg, pre_rng=pre_rng, pre_chop=pre_chop))
    return res


# ---------------------------------------------------------------- анализ
def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return 0.0
    k = (len(xs) - 1) * p
    f = math.floor(k)
    return xs[f] + (xs[min(f + 1, len(xs) - 1)] - xs[f]) * (k - f)


def summarize(rows, test_from_ts):
    groups = {}
    for r in rows:
        key = (r["market"], r["range"], r["levels"], r["trail"], r["mult"], r["lev"])
        part = "test" if r["start"] >= test_from_ts else "train"
        groups.setdefault(key, {"train": [], "test": []})[part].append(r)

    def stats(rs):
        if not rs:
            return {}
        rets = [x["ret"] for x in rs]
        return dict(n=len(rs), median=statistics.median(rets), mean=statistics.mean(rets),
                    p20=pct(rets, 0.2), worst=min(rets), win=sum(x > 0 for x in rets) / len(rets),
                    stop=sum(x["stopped"] for x in rs) / len(rs),
                    mdd=statistics.mean(x["mdd"] for x in rs),
                    trades=statistics.mean(x["trades"] for x in rs),
                    beat_hold=sum(x["ret"] > x["hold"] for x in rs) / len(rs))

    table = []
    for key, g in groups.items():
        m, rng, n, tr, mu, L_ = key
        tr_s, te_s = stats(g["train"]), stats(g["test"])
        al = stats(g["train"] + g["test"])
        # «стабильность» = доходность плохой недели (20-й перцентиль) с поправкой на долю прибыльных
        score = tr_s.get("p20", -9) + 0.02 * tr_s.get("win", 0)
        table.append(dict(market=m, range=rng, levels=n, step=g["train"][0]["step"] if g["train"] else None,
                          trail=tr, mult=mu, lev=L_, score=score, train=tr_s, test=te_s, all=al))
    table.sort(key=lambda x: x["score"], reverse=True)
    return table


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--out", default="results")
    ap.add_argument("--suite", default="main")
    a = ap.parse_args()
    coins = COINS[:3] if a.quick else COINS
    months = MONTHS[-2:] if a.quick else MONTHS
    t0 = time.time()
    tasks = []
    for coin in coins:
        rows = []
        for m in months:
            rows += load(coin, m)
        if not rows:
            continue
        rows.sort()
        tick = estimate_tick([x[4] for x in rows])
        W, S, P = WINDOW_DAYS * 1440, STEP_DAYS * 1440, 3 * 1440
        for s in range(P, len(rows) - W + 1, S):
            seg = rows[s:s + W]
            prev = [x[4] for x in rows[s - P:s]]
            tasks.append((coin, seg[0][0], [x[1] for x in seg], [x[2] for x in seg],
                          [x[3] for x in seg], [x[4] for x in seg], a.quick, tick, prev))
        print(f"{coin}: {len(rows)} свечей, шаг цены {tick:g} ({tick / rows[-1][4] * 100:.3f}% от цены)", flush=True)
    print(f"окон: {len(tasks)}, процессов: {os.cpu_count()}", flush=True)
    out = []
    fn = run_window_trail if a.suite == "trail" else run_window
    with Pool() as pool:
        for i, res in enumerate(pool.imap_unordered(fn, tasks), 1):
            out += res
            if i % 20 == 0:
                print(f"  {i}/{len(tasks)} окон, {time.time() - t0:.0f} с", flush=True)
    if a.suite == "trail":
        os.makedirs(a.out, exist_ok=True)
        with open(os.path.join(a.out, "runs.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(out[0].keys()))
            w.writeheader()
            w.writerows(out)
        print(f"готово за {time.time() - t0:.0f} с, строк {len(out)}", flush=True)
        return
    import datetime
    tf = int(datetime.datetime.strptime(TEST_FROM + "-01", "%Y-%m-%d").replace(
        tzinfo=datetime.timezone.utc).timestamp() * 1000)
    table = summarize(out, tf)
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "summary.json"), "w") as f:
        json.dump(dict(coins=coins, months=months, test_from=TEST_FROM, windows=len(tasks),
                       seconds=round(time.time() - t0), table=table), f)
    with open(os.path.join(a.out, "runs.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0].keys()))
        w.writeheader()
        w.writerows(out)
    print(f"готово за {time.time() - t0:.0f} с, строк {len(out)}", flush=True)
    for r in table[:15]:
        print(r["market"], r["range"], r["levels"], r["trail"], f"лот×{r['mult']}", f"{r['lev']}x",
              "p20 %.2f%% med %.2f%% win %.0f%% | test med %.2f%% win %.0f%%" % (
                  r["train"]["p20"] * 100, r["train"]["median"] * 100, r["train"]["win"] * 100,
                  r["test"].get("median", 0) * 100, r["test"].get("win", 0) * 100))


if __name__ == "__main__":
    main()
