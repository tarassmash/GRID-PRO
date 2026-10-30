#!/usr/bin/env python3
"""
Тест ВНУТРИДНЕВНОЙ сетки на минутных свечах: окна 6 и 24 часа, узкие диапазоны, фьючерсы.

Строгая модель исполнения: ячейка не может купиться и продаться в одной и той же минутной свече
(новой лимитке нужно время). Если цена уже проскочила уровень новой лимитки — бот ставит её по
лучшей цене стакана, тест исполняет её по открытию следующей свечи.

Выход: results_intraday/runs.csv.gz (по окнам) и configs.json.
"""
import csv, gzip, json, math, os, sys, time
from multiprocessing import Pool

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest import load, estimate_tick, COINS, MONTHS  # noqa: E402

FEE = 0.0002          # мейкер фьючерсов Bybit
SLIP = 0.0002         # сдвиги трейлинга «быстрой лимиткой»
EPS = 0.0001
FUNDING = 0.0001      # каждые 8 часов
GUARD = 0.35
CAP = 1000.0
LEVS = [1, 3, 5, 10]
HORIZONS = {"6h": 360, "24h": 1440}

RANGES = [0.005, 0.01, 0.015, 0.02, 0.03, 0.05]
LEVELS = [4, 6, 10, 16]
TRAILS = [("off", 0.0, 0.02), ("off", 0.0, 0.04), ("up", 0.005, 0.02), ("up", 0.01, 0.02), ("both", 0.01, 0.0)]
MULTS = [1.0, 1.4]


def configs():
    out = []
    for rng in RANGES:
        for n in LEVELS:
            step = ((1 + rng) / (1 - rng)) ** (1 / (n - 1)) - 1
            if step < 2 * FEE + 0.0004:           # шаг меньше ~0.08% — комиссия съест всё
                continue
            for tr, tp, sb in TRAILS:
                for mu in MULTS:
                    out.append(dict(range=rng, levels=n, step=round(step * 100, 3), trail=tr, tpct=tp,
                                    stop=sb, mult=mu))
    return out


CFGS = configs()


def sim(O, H, L, C, cfg, tick):
    rng, n, trail, tp, sb, mult = cfg["range"], cfg["levels"], cfg["trail"], cfg["tpct"], cfg["stop"], cfg["mult"]
    p0 = O[0]
    lo = p0 * (1 - rng)
    lnr = math.log((1 + rng) / (1 - rng)) / (n - 1)
    if tick > 0:
        lv = lambda k: max(round(lo * math.exp(k * lnr) / tick), 1) * tick
    else:
        lv = lambda k: lo * math.exp(k * lnr)
    half = tick * 0.5
    m = max([i for i in range(n - 1) if lv(i) <= p0], default=-1)
    w = {i: mult ** ((m - i) if i <= m else (i - m - 1)) for i in range(n - 1)}
    sw = sum(w.values())
    perk = {i: CAP * w[i] / sw for i in w}
    per = min(perk.values())
    kmin = 0
    cash, coin, hold = CAP, 0.0, {}
    for i in range(n - 1):
        if lv(i) > p0:
            q = perk[i] / lv(i)
            cash -= q * p0 * (1 + FEE + SLIP)
            coin += q
            hold[i] = q
    z = math.floor(math.log(p0 / lo) / lnr)
    trades = shifts = 0
    fund = 0.0
    up = trail in ("up", "both")
    down = trail == "both"
    stamp = {}                                   # ячейка -> номер свечи последнего исполнения
    pending = False
    t = 0

    def move(b):
        nonlocal z, cash, coin, trades, pending
        while b < min(lv(z) - half, lv(z) * (1 - EPS)):
            if kmin <= z <= kmin + n - 2 and z not in hold:
                if stamp.get(z) == t:
                    pending = True               # продана в этой же свече — купится позже
                else:
                    x = lv(z)
                    q = perk.get(z, per) / x
                    cash -= q * x * (1 + FEE)
                    coin += q
                    hold[z] = q
                    stamp[z] = t
                    trades += 1
            z -= 1
        while b > max(lv(z + 1) + half, lv(z + 1) * (1 + EPS)):
            if z in hold:
                if stamp.get(z) == t:
                    pending = True               # куплена в этой же свече — продастся позже
                else:
                    q = hold.pop(z)
                    cash += q * lv(z + 1) * (1 - FEE)
                    coin -= q
                    stamp[z] = t
                    trades += 1
            z += 1

    def settle(px):
        """Лимитки, оказавшиеся «за ценой», исполняются по лучшей цене стакана (мейкер)."""
        nonlocal cash, coin, trades, pending
        pending = False
        for k in range(kmin, kmin + n - 1):
            if k in hold and lv(k + 1) < px:
                q = hold.pop(k)
                cash += q * px * (1 - FEE)
                coin -= q
                stamp[k] = t
                trades += 1
            elif k not in hold and lv(k) > px:
                q = perk.get(k, per) / px
                cash -= q * px * (1 + FEE)
                coin += q
                hold[k] = q
                stamp[k] = t
                trades += 1

    alive = {L_: True for L_ in LEVS}
    out = {}
    peak = {L_: CAP for L_ in LEVS}
    mdd = {L_: 0.0 for L_ in LEVS}
    stopped = None
    N = len(O)
    for t in range(N):
        o, h, l, c = O[t], H[t], L[t], C[t]
        if pending:
            settle(o)
        if c >= o:
            move(l); eq_low = cash + coin * l; move(h)
        else:
            move(h); move(l); eq_low = cash + coin * l
        move(c)
        if not down and sb and l < lv(kmin) * (1 - sb):
            px = lv(kmin) * (1 - sb)
            stopped = cash + coin * px * (1 - FEE - SLIP)
            break
        for L_ in LEVS:
            if alive[L_] and CAP + L_ * (eq_low - CAP) - L_ * fund <= GUARD * CAP:
                alive[L_] = False
                out[L_] = dict(final=GUARD * CAP * 0.99, stopped=True, mdd=1 - GUARD)
        if not any(alive.values()):
            break
        if up and c > lv(kmin + n - 1) * (1 + max(tp, EPS)):
            for _ in range(60):
                if c <= lv(kmin + n - 1) * (1 + EPS) or kmin in hold:
                    break
                kmin += 1
                k = kmin + n - 2
                perk[k] = per
                q = per / c
                cash -= per * (1 + FEE + SLIP)
                coin += q
                hold[k] = q
                stamp[k] = t
                shifts += 1
            pending = True
            z = math.floor(math.log(c / lo) / lnr)
        elif down and c < lv(kmin) * (1 - max(tp, EPS)):
            for _ in range(60):
                ktop = kmin + n - 2
                if c >= lv(kmin) * (1 - EPS) or ktop not in hold:
                    break
                q = hold.pop(ktop)
                cash += q * c * (1 - FEE - SLIP)
                coin -= q
                kmin -= 1
                perk[kmin] = per
                shifts += 1
            pending = True
            z = math.floor(math.log(c / lo) / lnr)
        if t % 480 == 479:
            fund += coin * c * FUNDING
        if t % 15 == 14:
            eq = cash + coin * c
            for L_ in LEVS:
                if alive[L_]:
                    e = CAP + L_ * (eq - CAP) - L_ * fund
                    peak[L_] = max(peak[L_], e)
                    mdd[L_] = max(mdd[L_], 1 - e / peak[L_])
    eq = stopped if stopped is not None else cash + coin * C[-1] * (1 - FEE)
    for L_ in LEVS:
        if L_ in out:
            continue
        final = CAP + L_ * (eq - CAP) - L_ * fund
        out[L_] = dict(final=max(final, GUARD * CAP * 0.99), stopped=stopped is not None, mdd=mdd[L_])
    return out, trades, shifts


def run_window(task):
    ci, hz, start, O, H, L, C, tick, prev = task
    p30 = prev[-30:]
    feats = dict(pre30=max(p30) / min(p30) - 1, pre24=max(prev) / min(prev) - 1, pre24chg=prev[-1] / prev[0] - 1,
                 hold=C[-1] / O[0] - 1)
    rows = []
    for j, cfg in enumerate(CFGS):
        out, tr, sh = sim(O, H, L, C, cfg, tick)
        for L_, r in out.items():
            if hz == "6h" and L_ not in (1, 5):   # чтобы файл влез в GitHub
                continue
            rows.append((ci, hz, start, j, L_, round(r["final"] / CAP - 1, 5), round(r["mdd"], 4),
                         int(r["stopped"]), tr, sh, round(feats["pre30"], 5), round(feats["pre24"], 5),
                         round(feats["pre24chg"], 5), round(feats["hold"], 5)))
    return rows


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "results_intraday"
    t0 = time.time()
    tasks = []
    for ci, coin in enumerate(COINS):
        rows = []
        for m in MONTHS:
            rows += load(coin, m)
        if not rows:
            continue
        rows.sort()
        tick = estimate_tick([x[4] for x in rows])
        closes = [x[4] for x in rows]
        for hz, W in HORIZONS.items():
            for s in range(1440, len(rows) - W + 1, W):
                seg = rows[s:s + W]
                tasks.append((ci, hz, seg[0][0], [x[1] for x in seg], [x[2] for x in seg], [x[3] for x in seg],
                              [x[4] for x in seg], tick, closes[s - 1440:s]))
        print(f"{coin}: {len(rows)} свечей, тик {tick:g}", flush=True)
    print(f"окон: {len(tasks)}, вариантов на окно: {len(CFGS)}", flush=True)
    os.makedirs(out_dir, exist_ok=True)
    n = 0
    files = {hz: gzip.open(os.path.join(out_dir, f"runs_{hz}.csv.gz"), "wt", newline="") for hz in HORIZONS}
    writers = {hz: csv.writer(f) for hz, f in files.items()}
    for w in writers.values():
        w.writerow(["coin", "h", "start", "cfg", "lev", "ret", "mdd", "stopped", "trades", "shifts",
                    "pre30", "pre24", "pre24chg", "hold"])
    with Pool() as pool:
        for i, res in enumerate(pool.imap_unordered(run_window, tasks, chunksize=8), 1):
            if res:
                writers[res[0][1]].writerows(res)
            n += len(res)
            if i % 500 == 0:
                print(f"  {i}/{len(tasks)} окон, {time.time() - t0:.0f} с", flush=True)
    for f in files.values():
        f.close()
    with open(os.path.join(out_dir, "configs.json"), "w") as f:
        json.dump(dict(coins=COINS, configs=CFGS, levs=LEVS, horizons=HORIZONS), f)
    print(f"готово за {time.time() - t0:.0f} с, строк {n}", flush=True)


if __name__ == "__main__":
    main()
