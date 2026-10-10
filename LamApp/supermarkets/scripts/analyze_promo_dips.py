"""
Read-only diagnostic: around a promo, how does a product sell compared with normal?
Measures what decision_maker would otherwise have to assume:

  before   the 3 days before the promo, vs normal (customers waiting for the flyer)
  opening  the first 3 promo days, vs the promo's own average (flyer effect)
  after    days 1-7 and 8-14 after the promo, vs normal (stocked-up customers)
  lift     the promo's average, vs normal

Each day is the product's SHARE of the store's total that day. The flyers start on
the same weekday, so the days before a promo are always the same weekdays, and
holidays hit every product at once: dividing by the store's day cancels both.
"Normal" is the 14 days before the promo less the 3 just before it. With chained
14-day flyers those 14 days are the previous flyer, in which the product was not on
promo (it would have merged into this run), so no older promo can sit in it.

Figures are pooled over products, as ratios of totals: the median of per-product
ratios reads low on short windows, small skewed counts sitting below their mean.
Pooled shares weigh each product by its volume, so this is what the store's turnover
sees. Only products selling about 1/day or more count.

Each measure uses whatever fits the 59 days of history, so a promo still running
gives before and opening, an older one after as well. Run it again in a few weeks
for more promos.

Usage:
    python analyze_promo_dips.py "Todis Gubbio"
"""
import argparse
import random
import statistics
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

# Load the same .env Django loads (see analyze_dispersion.py for why not `source`)
try:
    from dotenv import load_dotenv
    _ENV = Path(__file__).resolve().parents[3] / ".env.production"
    if _ENV.exists():
        load_dotenv(_ENV)
except ImportError:
    pass

GAP_DAYS = 3
BASE_DAYS = 14 - GAP_DAYS
MIN_BASE_UNITS = BASE_DAYS

# (label, numerator, denominator): each figure is total numerator / total denominator
MEASURES = [("3 days before", "before", "base"),
            ("opening / promo avg", "opening", "promo"),
            ("days 1-7 after", "after1", "base"),
            ("days 8-14 after", "after2", "base"),
            ("lift", "promo", "base")]


def measure_runs(history, store_totals, windows, today):
    """
    One dict per promo run of a product: its mean daily share of the store in each
    period that fits the history (base, before, promo, opening, after1, after2).
    [] when the product sells too little to say anything.

    history[i] and store_totals[i] are the day today - 1 - i (completed days only).
    windows: the product's promo runs as (start, end) dates.
    """
    yesterday = today - timedelta(days=1)

    def day_share(d):
        i = (yesterday - d).days
        if i < 0 or i >= len(history) or i >= len(store_totals):
            return None
        v, t = history[i], store_totals[i]
        return v / t if v is not None and t else None

    def normal_share(d):
        # Another promo's day is not a normal one
        return None if any(s <= d <= e for s, e in windows) else day_share(d)

    def mean_of(days, fn, need):
        vals = [x for x in (fn(d) for d in days) if x is not None]
        return statistics.mean(vals) if len(vals) >= need else None

    out = []
    for s, e in windows:
        base_days = [s - timedelta(days=GAP_DAYS + k) for k in range(1, BASE_DAYS + 1)]
        base = mean_of(base_days, normal_share, BASE_DAYS - 3)
        base_units = sum(history[(yesterday - d).days] or 0 for d in base_days
                         if 0 <= (yesterday - d).days < len(history))
        if not base or base_units < MIN_BASE_UNITS:
            continue

        r = {"base": base}
        before = mean_of([s - timedelta(days=k) for k in range(1, GAP_DAYS + 1)], normal_share, 2)
        if before is not None:
            r["before"] = before
        promo_days = [s + timedelta(days=k) for k in range((min(e, yesterday) - s).days + 1)]
        promo = mean_of(promo_days, day_share, 7)
        if promo:
            r["promo"] = promo
            opening = mean_of(promo_days[:3], day_share, 2)
            if opening is not None:
                r["opening"] = opening
        if e < today - timedelta(days=7):
            a1 = mean_of([e + timedelta(days=k) for k in range(1, 8)], normal_share, 5)
            if a1 is not None:
                r["after1"] = a1
        if e < today - timedelta(days=14):
            a2 = mean_of([e + timedelta(days=k) for k in range(8, 15)], normal_share, 5)
            if a2 is not None:
                r["after2"] = a2
        out.append(r)
    return out


def pooled_ratio(runs, num, den, reps=2000, seed=1):
    """(ratio, low, high, n): total num / total den over the runs having both, with a
    bootstrap 95% interval over runs."""
    pairs = [(r[num], r[den]) for r in runs if num in r and den in r]
    if not pairs:
        return None, None, None, 0

    def ratio(ps):
        return sum(a for a, _ in ps) / sum(b for _, b in ps)

    rng = random.Random(seed)
    boot = sorted(ratio(rng.choices(pairs, k=len(pairs))) for _ in range(reps))
    return ratio(pairs), boot[int(0.025 * reps)], boot[int(0.975 * reps) - 1], len(pairs)


def main():
    from DatabaseManager import DatabaseManager
    from helpers import Helper

    parser = argparse.ArgumentParser(description="Measure sales around promos (read-only)")
    parser.add_argument("supermarket", help="Supermarket name, as used for the schema")
    args = parser.parse_args()

    today = date.today()
    db = DatabaseManager(args.supermarket)
    cur = db.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'economics' AND column_name = 'past_windows'
          AND table_schema = current_schema()
    """)
    past_col = "e.past_windows" if cur.fetchone() else "NULL AS past_windows"
    cur.execute(f"""
        SELECT p.settore, ps.sales_sets, e.sale_start, e.sale_end, {past_col}
        FROM product_stats ps
        JOIN products p ON p.cod = ps.cod AND p.v = ps.v
        JOIN economics e ON e.cod = ps.cod AND e.v = ps.v
        WHERE ps.verified = TRUE AND e.sale_start IS NOT NULL AND e.sale_end IS NOT NULL
    """)
    rows = cur.fetchall()
    store_totals = Helper.sales_history(db.get_store_daily_totals())
    closed = Helper.closure_day_mask(store_totals)
    store_totals = [None if c else t for t, c in zip(store_totals, closed)]

    runs_by_settore = defaultdict(list)
    for row in rows:
        windows = Helper.promo_windows(row["sale_start"], row["sale_end"], row["past_windows"])
        runs = measure_runs(Helper.sales_history(row["sales_sets"]), store_totals, windows, today)
        runs_by_settore[row["settore"]] += runs
        runs_by_settore["ALL"] += runs

    print(f"\nSupermarket : {args.supermarket}")
    print(f"Promo runs measured: {len(runs_by_settore['ALL'])} "
          f"(of {len(rows)} products with promo dates; ~1/day or more, inside the 59-day history)\n")
    for settore in sorted(runs_by_settore, key=lambda s: (s == "ALL", s)):
        print(settore)
        for label, num, den in MEASURES:
            ratio, lo, hi, n = pooled_ratio(runs_by_settore[settore], num, den)
            if ratio is None:
                print(f"  {label:<22} -")
            else:
                print(f"  {label:<22} {ratio:5.2f}   95% [{lo:4.2f} - {hi:4.2f}]   n={n}")
        print()
    print("1.00 = as normal. '3 days before' 0.85 = 15% below normal before the promo;")
    print("'opening' 1.20 = the first 3 promo days sell 20% above the promo's own average.")
    print("Trust a figure only when its interval stays clear of 1.00 and n is in the dozens.\n")
    db.close()


if __name__ == "__main__":
    main()
