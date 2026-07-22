"""
Read-only diagnostic: measure a supermarket's real demand dispersion
(variance / mean), which is what sizes the safety stock in processor_N.

1.0 == Poisson, the assumption baked into the legacy sqrt(req_stock) rule.
Simulating the policy end to end: dispersion 1.0 gives 98-99.7% fill rate,
2.0 gives 96-99%, 3.0 falls to 91-98%.

Measured on WEEKDAY-ADJUSTED RESIDUALS — the weekday pattern is predictable and
`coverage` already prices it in via the day weights, so counting it again as
uncertainty double-charges (worth 22-70% on simulated data, growing with
velocity). Also reports promo contamination and serial correlation, the two
things that would otherwise inflate the headline number.

Usage:
    python analyze_dispersion.py "Todis Gubbio"
    python analyze_dispersion.py "Todis Gubbio" --settore DEPERIBILI --coverage 4
"""
import argparse
import statistics
from collections import defaultdict
from datetime import date
from pathlib import Path

# Load the same .env Django loads, so this runs from a bare shell with nothing
# exported. Do NOT `source .env.production` in bash instead: it is a dotenv file,
# not a shell script, and values such as SECRET_KEY contain ( ) & $ * which bash
# tries to interpret as syntax.
try:
    from dotenv import load_dotenv
    _ENV = Path(__file__).resolve().parents[3] / ".env.production"
    if _ENV.exists():
        load_dotenv(_ENV)
except ImportError:
    pass

from DatabaseManager import DatabaseManager

MIN_OBSERVED_DAYS = 28
MIN_TOTAL_UNITS = 20


def weekday_adjusted_residuals(sales_sets, today):
    """
    Strip the deterministic weekday pattern from a sales_sets array.

    sales_sets[i] holds the day (today - 1 - i), so that day's weekday is
    (today.weekday() - 1 - i) % 7. Subtracting each weekday's own mean leaves
    only the genuinely unpredictable part of demand.

    Returns (residuals, mean_daily, observed_days) or None when there isn't
    enough data to say anything.
    """
    base_dow = today.weekday()

    observed = []
    for i, v in enumerate(sales_sets):
        if v is not None:
            observed.append(((base_dow - 1 - i) % 7, float(v)))

    if len(observed) < MIN_OBSERVED_DAYS:
        return None

    values = [v for _, v in observed]
    if sum(values) < MIN_TOTAL_UNITS:
        return None

    by_dow = defaultdict(list)
    for dow, v in observed:
        by_dow[dow].append(v)

    # A weekday seen only once contributes a zero residual by construction and
    # would understate the spread, so require at least two observations.
    dow_means = {d: statistics.mean(vs) for d, vs in by_dow.items() if len(vs) >= 2}
    residuals = [v - dow_means[dow] for dow, v in observed if dow in dow_means]

    if len(residuals) < MIN_OBSERVED_DAYS:
        return None

    # Degrees of freedom: estimating one mean per weekday group costs one df each,
    # so dividing the squared residuals by N would understate the spread by about
    # 11% on a 56-day window. Dividing by (N - groups) is the standard unbiased
    # within-group estimator and recovers the true dispersion on simulated data.
    groups = len(dow_means)
    dof = len(residuals) - groups
    if dof <= 0:
        return None

    variance = sum(x * x for x in residuals) / dof

    return variance, statistics.mean(values), len(observed), residuals


def window_sigma(values, coverage):
    """
    Sigma of demand over a coverage-length window, measured DIRECTLY from rolling
    sums instead of scaled up from daily sigma.

    sigma_L = sigma_daily * sqrt(L) is only valid when days are independent. When
    demand is serially correlated - a heat wave lifts every day of the week, not
    one - the days move together and the window total swings more than sqrt(L)
    predicts. At the extreme of perfect correlation sigma_L = L * sigma_daily,
    which for a 4-day window is twice what sqrt(L) gives.

    Computed from the autocovariance function:

        Var(sum of L days) = L*g0 + 2 * sum_{k=1..L-1} (L-k) * gk

    where gk is the autocovariance at lag k. The sqrt(L) rule keeps only the
    first term, i.e. assumes gk = 0 for every k >= 1.

    NOT computed from the spread of rolling sums, which is the obvious approach
    and is wrong: consecutive rolling windows share (L-1)/L of their terms, so
    the sample spread of those sums is biased low - on genuinely independent data
    it reports 0.78x instead of 1.00x, which would read as "sqrt(L) overstates
    risk" when nothing of the sort is happening.

    Returns (sigma_window, sigma_sqrt_rule) or None.
    """
    n = len(values)
    if coverage < 1 or n < coverage + 14:
        return None

    mean = statistics.mean(values)
    dev = [v - mean for v in values]

    g0 = sum(d * d for d in dev) / n
    if g0 <= 0:
        return None

    total = coverage * g0
    for k in range(1, coverage):
        gk = sum(dev[i] * dev[i + k] for i in range(n - k)) / n
        total += 2 * (coverage - k) * gk

    if total <= 0:
        return None

    return total ** 0.5, (g0 ** 0.5) * (coverage ** 0.5)


def lag1_autocorrelation(residuals):
    """Correlation between consecutive residuals. >0 means runs of high days."""
    n = len(residuals)
    if n < 20:
        return None
    mean = statistics.mean(residuals)
    num = sum((residuals[i] - mean) * (residuals[i + 1] - mean) for i in range(n - 1))
    den = sum((r - mean) ** 2 for r in residuals)
    if den <= 0:
        return None
    return num / den


def main():
    parser = argparse.ArgumentParser(description="Measure demand dispersion (read-only)")
    parser.add_argument("supermarket", help="Supermarket name, as used for the schema")
    parser.add_argument("--settore", default=None, help="Limit to one settore")
    parser.add_argument("--coverage", type=int, default=4,
                        help="Coverage window length in days (default 4)")
    args = parser.parse_args()

    today = date.today()
    db = DatabaseManager(args.supermarket)
    cur = db.cursor()

    # A promo whose window overlaps the sales_sets history inflates that product's
    # variance. Only promos ended within 14 days are excised by the ordering path,
    # so anything older is still sitting in the data being counted as volatility.
    query = """
        SELECT ps.cod, ps.v, p.descrizione, p.settore, ps.sales_sets,
               (e.sale_start IS NOT NULL
                AND e.sale_end >= CURRENT_DATE - 60
                AND e.sale_start <= CURRENT_DATE) AS promo_in_window
        FROM product_stats ps
        JOIN products p ON p.cod = ps.cod AND p.v = ps.v
        LEFT JOIN economics e ON e.cod = ps.cod AND e.v = ps.v
        WHERE ps.verified = TRUE
          AND ps.sales_sets IS NOT NULL
    """
    params = []
    if args.settore:
        query += " AND p.settore = %s"
        params.append(args.settore)

    cur.execute(query, params)
    rows = cur.fetchall()

    results = []
    for row in rows:
        out = weekday_adjusted_residuals(row["sales_sets"] or [], today)
        if out is None:
            continue
        var_resid, mean_daily, observed, residuals = out
        if mean_daily <= 0:
            continue

        raw_values = [float(v) for v in (row["sales_sets"] or []) if v is not None]

        win = window_sigma(raw_values, args.coverage)
        acf = lag1_autocorrelation(residuals)

        results.append({
            "settore": row["settore"],
            "descrizione": row["descrizione"],
            "mean_daily": mean_daily,
            "dispersion": var_resid / mean_daily,
            "sigma_resid": var_resid ** 0.5,
            "sigma_raw": statistics.pstdev(raw_values),
            "volume": sum(raw_values),
            "promo": bool(row["promo_in_window"]),
            "acf1": acf,
            "window_penalty": (win[0] / win[1]) if win and win[1] > 0 else None,
        })

    if not results:
        print("No products had enough history to measure. Nothing to report.")
        return

    results.sort(key=lambda r: r["volume"], reverse=True)
    total_volume = sum(r["volume"] for r in results)

    def weighted_median(key):
        """Volume-weighted median: what the store's turnover actually experiences."""
        ordered = sorted(results, key=lambda r: r[key])
        seen = 0.0
        for r in ordered:
            seen += r["volume"]
            if seen >= total_volume / 2:
                return r[key]
        return ordered[-1][key]

    disps = [r["dispersion"] for r in results]
    infl = [r["sigma_raw"] / r["sigma_resid"] for r in results if r["sigma_resid"] > 0]

    print(f"\nSupermarket : {args.supermarket}")
    print(f"Products    : {len(results)} measurable of {len(rows)} verified")
    print(f"Total units : {total_volume:,.0f} over the sales_sets window\n")

    print("DISPERSION (variance / mean of weekday-adjusted residuals)")
    print(f"  1.0 == Poisson, which is what the current safety stock assumes\n")
    print(f"  plain median        : {statistics.median(disps):.2f}")
    print(f"  volume-weighted     : {weighted_median('dispersion'):.2f}   <- the number that matters")
    print(f"  mean                : {statistics.mean(disps):.2f}")
    for q, label in ((0.25, "p25"), (0.75, "p75"), (0.90, "p90")):
        idx = min(int(q * len(disps)), len(disps) - 1)
        print(f"  {label}                 : {sorted(disps)[idx]:.2f}")

    over = sum(1 for d in disps if d > 1.5) / len(disps)
    print(f"\n  {over:.0%} of products exceed 1.5 (materially over-dispersed)")

    if infl:
        print(f"\nWEEKDAY INFLATION (sigma_raw / sigma_residual)")
        print(f"  median {statistics.median(infl):.2f}x -- how much raw sigma would")
        print(f"  overstate uncertainty if anyone skips the weekday adjustment.")

    # --- Caveat 1: promotions inside the history window inflate variance ---
    promo = [r for r in results if r["promo"]]
    clean = [r for r in results if not r["promo"]]
    print(f"\nPROMO CONTAMINATION")
    print(f"  {len(promo)} of {len(results)} products had a promo overlapping the window")
    if promo and clean:
        def wmed(subset, key):
            vol = sum(s["volume"] for s in subset)
            seen = 0.0
            for s in sorted(subset, key=lambda x: x[key]):
                seen += s["volume"]
                if seen >= vol / 2:
                    return s[key]
            return subset[-1][key]
        pw, cw = wmed(promo, "dispersion"), wmed(clean, "dispersion")
        print(f"  dispersion with promo : {pw:.2f}   (volume-weighted)")
        print(f"  dispersion no promo   : {cw:.2f}")
        if pw > cw * 1.25:
            print(f"  -> promos are inflating the headline number. Use {cw:.2f} to size z.")
        else:
            print(f"  -> promos are NOT driving it. The volatility is real.")

    # --- Caveat 2: serial correlation makes a coverage window riskier than sqrt(L) ---
    pens = [r["window_penalty"] for r in results if r["window_penalty"]]
    acfs = [r["acf1"] for r in results if r["acf1"] is not None]
    if pens:
        print(f"\nWINDOW SIGMA vs sqrt(L) RULE  (coverage = {args.coverage} days)")
        print(f"  measured sigma over real {args.coverage}-day windows, divided by")
        print(f"  sigma_daily * sqrt({args.coverage}). 1.00 = days independent.")
        print(f"    median      : {statistics.median(pens):.2f}x")
        for q, lbl in ((0.75, "p75"), (0.90, "p90")):
            print(f"    {lbl}         : {sorted(pens)[min(int(q*len(pens)), len(pens)-1)]:.2f}x")
        if acfs:
            print(f"  lag-1 autocorrelation of residuals, median: {statistics.median(acfs):+.2f}")
        m = statistics.median(pens)
        if m > 1.15:
            print(f"  -> sqrt(L) understates window risk by ~{100*(m-1):.0f}%. Measure sigma")
            print(f"     on rolling windows directly rather than scaling daily sigma.")
        else:
            print(f"  -> sqrt(L) is close enough. Days behave near-independently.")

    print("\nBY SETTORE (volume-weighted)")
    by_settore = defaultdict(list)
    for r in results:
        by_settore[r["settore"]].append(r)
    for settore, group in sorted(by_settore.items()):
        vol = sum(g["volume"] for g in group)
        ordered = sorted(group, key=lambda g: g["dispersion"])
        seen = 0.0
        med = ordered[-1]["dispersion"]
        for g in ordered:
            seen += g["volume"]
            if seen >= vol / 2:
                med = g["dispersion"]
                break
        print(f"  {settore:<24} n={len(group):>5}  dispersion={med:.2f}")

    print("\nMOST OVER-DISPERSED HIGH-VOLUME PRODUCTS")
    top = sorted(results[:150], key=lambda r: r["dispersion"], reverse=True)[:12]
    for r in top:
        print(f"  {r['dispersion']:5.2f}  {r['mean_daily']:6.2f}/day  {r['descrizione'][:46]}")

    w = weighted_median("dispersion")
    print("\nREADING")
    if w < 1.3:
        print("  Demand is close to Poisson. The current sqrt(req_stock) safety stock is")
        print("  well matched and simulated fill rate is 98-99%. No change warranted.")
    elif w < 2.2:
        print("  Mildly over-dispersed. Simulated fill rate lands around 96-99%, which is")
        print("  acceptable. An explicit z would buy control, not much fill rate.")
    else:
        print("  Materially over-dispersed. Simulated fill rate falls to 91-95% for slow")
        print("  and mid movers, and the sqrt rule cannot see it because it assumes")
        print("  variance equals the mean. This is the case where z*sigma is worth building.")
    print()

    db.conn.close()


if __name__ == "__main__":
    main()
