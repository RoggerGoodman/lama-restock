"""
Read-only diagnostic: measure the real demand dispersion of a supermarket.

Nothing in the restock chain sets an explicit service level. It emerges from
`minimum_stock = presence_target + (sqrt(req_stock - 1) - 1)`, which is a Poisson
safety-stock rule in disguise: for Poisson demand sigma = sqrt(mu), and req_stock
IS mu over the coverage window, so the buff tracks sigma.

That is only sound if demand actually is Poisson. Simulating the shipped policy
end to end gives:

    dispersion 1.0 (Poisson)   -> 98.0 - 99.7% fill rate   (excellent)
    dispersion 2.0             -> 96.1 - 99.4% fill rate   (acceptable)
    dispersion 3.0             -> 91.3 - 98.4% fill rate   (a real problem)

So the entire question is which of those columns this store lives in, and that is
an empirical fact nobody has measured. This script measures it. Run it before
changing anything in the safety-stock formula.

Dispersion is measured on WEEKDAY-ADJUSTED RESIDUALS, not on raw daily sales.
The weekday pattern is predictable and `coverage` already accounts for it through
the day weights, so counting it again as uncertainty double-charges. Measured
against simulated data, raw sigma overstates true uncertainty by 22-70% and the
overstatement grows with velocity - enough to badly over-order fast movers if
anyone implements z*sigma using raw sigma.

Usage:
    python analyze_dispersion.py "Todis Gubbio"
    python analyze_dispersion.py "Todis Gubbio" --settore DEPERIBILI
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

    return variance, statistics.mean(values), len(observed)


def main():
    parser = argparse.ArgumentParser(description="Measure demand dispersion (read-only)")
    parser.add_argument("supermarket", help="Supermarket name, as used for the schema")
    parser.add_argument("--settore", default=None, help="Limit to one settore")
    args = parser.parse_args()

    today = date.today()
    db = DatabaseManager(args.supermarket)
    cur = db.cursor()

    query = """
        SELECT ps.cod, ps.v, p.descrizione, p.settore, ps.sales_sets
        FROM product_stats ps
        JOIN products p ON p.cod = ps.cod AND p.v = ps.v
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
        var_resid, mean_daily, observed = out
        if mean_daily <= 0:
            continue

        raw_values = [float(v) for v in (row["sales_sets"] or []) if v is not None]

        results.append({
            "settore": row["settore"],
            "descrizione": row["descrizione"],
            "mean_daily": mean_daily,
            "dispersion": var_resid / mean_daily,
            "sigma_resid": var_resid ** 0.5,
            "sigma_raw": statistics.pstdev(raw_values),
            "volume": sum(raw_values),
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
