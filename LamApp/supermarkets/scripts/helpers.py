from datetime import datetime
from calendar import monthrange
from collections import defaultdict
import logging
import math
import statistics
import pdfplumber
import re

# Use Django's logging system
logger = logging.getLogger(__name__)

class Helper:

    def __init__(self) -> None:
        # Get the current month and day
        self.current_year = datetime.now().year
        self.current_month = datetime.now().month
        self.current_day = datetime.now().day
        self.days_this_month = monthrange(self.current_year, self.current_month)[1]

        # Calculate previous month and year
        if self.current_month == 1:
            prev_month = 12
            prev_year = self.current_year - 1
        else:
            prev_month = self.current_month - 1
            prev_year = self.current_year

        self.days_previous_month = monthrange(prev_year, prev_month)[1]

    def calculate_weighted_avg_sales_new(self, final_array_sold: list, alpha: float = 3.0, silent: bool = False):
        """
        Returns (avg_daily_sales, avg_sales_last_year)

        - Handles new products gracefully.
        - Uses proportional growth between previous months (this vs last year)
        to stabilize early-month estimates.
        - Blend fades as month progresses.
        """

        if not final_array_sold:
            return 0.0, 0.0

        sold_this_month = final_array_sold[0] if len(final_array_sold) >= 1 and final_array_sold[0] is not None else 0
        sold_prev_month = final_array_sold[1] if len(final_array_sold) >= 2 and final_array_sold[1] is not None else 0
        sold_same_month_last_year = final_array_sold[12] if len(final_array_sold) > 12 and final_array_sold[12] is not None else 0
        sold_prev_month_last_year = final_array_sold[13] if len(final_array_sold) > 13 and final_array_sold[13] is not None else 0

        null_indices = [i for i in [0, 1, 12, 13] if i < len(final_array_sold) and final_array_sold[i] is None]
        if null_indices:
            logger.warning(f"calculate_weighted_avg_sales_new: sold_last_24 has None at indices {null_indices} — data corruption, values defaulted to 0")

        days_this_month = max(1, int(self.days_this_month))
        days_prev_month = max(1, int(self.days_previous_month))
        observed_days = max(1, int(self.current_day) - 1)

        progress = min(1.0, observed_days / days_this_month)

        # Compute base rates
        rate_current_obs = sold_this_month / observed_days
        rate_prev_month = sold_prev_month / days_prev_month if sold_prev_month > 0 else 0.0
        rate_same_month_last_year = sold_same_month_last_year / days_this_month if sold_same_month_last_year > 0 else 0.0
        rate_prev_month_last_year = sold_prev_month_last_year / days_prev_month if sold_prev_month_last_year > 0 else 0.0

        # --- Trend ratio: how much this year has grown vs last year ---
        if rate_prev_month_last_year > 0:
            growth_ratio = rate_prev_month / rate_prev_month_last_year
            growth_ratio = min(growth_ratio, 2)
        else:
            growth_ratio = 1.0

        # --- Adjust prior using growth ratio ---
        if rate_same_month_last_year > 0:
            prior_rate = rate_prev_month
        elif rate_prev_month > 0:
            prior_rate = rate_prev_month
        else:
            prior_rate = rate_current_obs  # fallback for new products

        # --- Dynamic blend based on month progress ---
        w_prior = (1 - progress) ** alpha
        w_current = 1 - w_prior
        avg_daily_sales = (w_current * rate_current_obs) + (w_prior * prior_rate)

        avg_sales_base = rate_same_month_last_year

        if not silent:
            try:
                logger.info(
                    f"Day {self.current_day}/{days_this_month} | progress={progress:.2f} | "
                    f"growth_ratio={growth_ratio:.2f} | w_prior={w_prior:.2f} | "
                    f"rate_obs={rate_current_obs:.2f} | prior_rate={prior_rate:.2f} | "
                    f"avg_daily_sales={avg_daily_sales:.2f}"
                )
            except Exception:
                pass

        return avg_daily_sales, avg_sales_base

    @staticmethod
    def avg_daily_sales_from_sales_sets(daily_sales: list, silent: bool = False):
        """
        Compute a recency-weighted average daily sales rate.

        Args:
            daily_sales: list[int], newest → oldest (index 0 is most recent day)
        Returns:
            float
        """

        if not daily_sales:
            return None

        # Filter out None entries (out-of-stock days where demand was censored)
        daily_sales = [v for v in daily_sales if v is not None]

        min_days = 14
        half_life = 14
        outlier_k = 10
        # No single day below this is worth winsorising, however extreme it looks
        # against a mostly-zero history. Without it the threshold collapses on
        # sparse sellers — a lone 1-unit sale in 60 days was being erased to 0,
        # and a 3-unit day clipped to 1 — because both p95 and the second-highest
        # day are 0 or 1 there. The case this guards against is a 500-unit spike
        # on a dead product, not a customer buying three jars.
        outlier_min_abs = 10

        observed_days = len(daily_sales)
        if observed_days < min_days:
            return None

        # Cap outliers above the threshold to the highest non-outlier value,
        # preserving position. Floored at the second-highest day so that when
        # 95%+ of days are zero (p95 carries no scale) the gate does not tighten
        # onto genuine sales.
        sorted_vals = sorted(daily_sales)
        idx = 0.95 * (len(sorted_vals) - 1)
        lo = int(idx)
        hi = min(lo + 1, len(sorted_vals) - 1)
        p95 = sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])

        threshold = max(outlier_k * p95, sorted_vals[-2], outlier_min_abs)
        outlier_indices = [i for i, v in enumerate(daily_sales) if v > threshold]
        if outlier_indices:
            # Never empty: threshold >= sorted_vals[-2], so every value except
            # the maximum is at or below it, and observed_days >= min_days.
            safe_max = max(v for v in daily_sales if v <= threshold)
            if not silent:
                logger.warning(
                    f"avg_daily_sales_from_sales_sets: capped outliers to {safe_max} "
                    f"(threshold={threshold:.1f}, p95={p95:.1f}): "
                    f"{[(i, daily_sales[i]) for i in outlier_indices]}"
                )
            daily_sales = [safe_max if v > threshold else v for v in daily_sales]

        lam = math.log(2) / half_life

        weighted_sum = 0.0
        weight_total = 0.0

        # index 0 is newest (age=0, highest weight); index n-1 is oldest
        for age, sold in enumerate(daily_sales):
            weight = math.exp(-lam * age)
            weighted_sum += sold * weight
            weight_total += weight

        avg_daily_sales = weighted_sum / weight_total

        if not silent:
            try:
                logger.info(f"avg_daily_sales={avg_daily_sales:.2f}")
            except Exception:
                pass

        return avg_daily_sales

    @staticmethod
    def internal_loss_daily_rate(internal_array: list, months: int = 3, today=None):
        """
        Daily rate of internal consumption (goods taken by store staff) from the
        monthly `extra_losses.internal` array. Returns 0.0 with no usable history.

        Kept OUT of sales_sets on purpose: spreading a monthly total across daily
        slots would fabricate observations, and the recency weighting, outlier
        capping and deviation calc would then run on invented numbers. Two streams
        at different granularities — add the rates, never the series.

        Index 0 (month-to-date) is skipped; a partial month divided by its full
        length would understate the rate.
        """
        if not internal_array:
            return 0.0

        today = today or datetime.now().date()

        total_qty = 0.0
        total_days = 0

        for i in range(1, months + 1):
            if i >= len(internal_array):
                break

            month = today.month - i
            year = today.year
            while month <= 0:
                month += 12
                year -= 1

            entry = internal_array[i]
            # Losses are stored as [qty, cost]; tolerate the legacy plain-int form.
            if isinstance(entry, list) and len(entry) >= 1:
                qty = entry[0] or 0
            elif isinstance(entry, (int, float)):
                qty = entry
            else:
                qty = 0

            total_qty += qty
            total_days += monthrange(year, month)[1]

        if total_days == 0:
            return 0.0

        return total_qty / total_days

    # Promo-lift measurement constants
    PROMO_BASELINE_DAYS = 14   # length of the pre-promo comparison window
    PROMO_BASELINE_GAP = 3     # days skipped immediately before the promo
    PROMO_MIN_OBSERVED = 0.6   # fraction of promo days that must be in stock
    PROMO_MIN_BASELINE_UNITS = 10
    PROMO_MAX_LIFT = 5.0
    PROMO_MAX_DEPTH_RATIO = 2.0   # how far a measured lift may be rescaled

    @staticmethod
    def measure_promo_lift(sales_sets: list, days_since_the_end: int, days_lasted: int):
        """
        Lift a finished promotion actually produced, as a ratio (2.4 == sold 2.4x
        its normal rate). None when it cannot be measured cleanly.

        sales_sets[i] holds the day (today - 1 - i), so the promo's last day sits
        at index (days_since_the_end - 1) and runs `days_lasted` slots from there.
        The baseline is taken after a gap: demand dips just before a promo starts
        (flyers go out in advance) and that dip would inflate the measured lift.
        """
        if not sales_sets or days_since_the_end < 1 or days_lasted < 1:
            return None

        promo_start = days_since_the_end - 1
        promo_end = promo_start + days_lasted
        base_start = promo_end + Helper.PROMO_BASELINE_GAP
        base_end = base_start + Helper.PROMO_BASELINE_DAYS

        if len(sales_sets) < base_end:
            return None

        promo_days = sales_sets[promo_start:promo_end]
        baseline_days = sales_sets[base_start:base_end]

        # Stockout days carry no demand information. A promo that sold out is
        # censored downward, so require most of it to have been in stock.
        promo_obs = [v for v in promo_days if v is not None]
        base_obs = [v for v in baseline_days if v is not None]

        if not promo_obs or not base_obs:
            return None
        if len(promo_obs) / len(promo_days) < Helper.PROMO_MIN_OBSERVED:
            return None
        if sum(base_obs) < Helper.PROMO_MIN_BASELINE_UNITS:
            return None

        promo_rate = sum(promo_obs) / len(promo_obs)
        base_rate = sum(base_obs) / len(base_obs)

        if base_rate <= 0 or promo_rate <= 0:
            return None

        return round(min(promo_rate / base_rate, Helper.PROMO_MAX_LIFT), 3)

    @staticmethod
    def expected_promo_lift(promo_lifts, discount=None):
        """
        Expected lift for an upcoming promotion from this product's own history.
        None when there is nothing usable, so the caller falls back to its default.
        Entries are {"lift": <ratio>, "discount": <pct it was measured at>}.

        Depth rescales WITHIN a product only, never predicts lift for one with no
        history: across products depth is endogenous (the buyer picks 10% for an
        elastic staple and 40% for a niche item precisely because of their
        elasticities), so it correlates with lift the wrong way round.

        Rescaling applies to (lift - 1), not lift: 2.4x measured at 30% scaled
        linearly to 10% would give 0.8x — a promotion that reduces sales. Scaling
        the excess gives 1 + 1.4 * (1/3) = 1.47x.
        """
        if not promo_lifts:
            return None

        lifts, depths = [], []
        for entry in promo_lifts:
            if not isinstance(entry, dict):
                continue
            try:
                lift = float(entry.get("lift"))
            except (TypeError, ValueError):
                continue
            lifts.append(lift)
            try:
                depth = float(entry.get("discount"))
                if depth > 0:
                    depths.append(depth)
            except (TypeError, ValueError):
                pass

        if not lifts:
            return None

        mean_lift = sum(lifts) / len(lifts)

        # Rescale to the upcoming depth only when both depths are known
        if discount and depths:
            mean_depth = sum(depths) / len(depths)
            if mean_depth > 0:
                ratio = min(discount / mean_depth, Helper.PROMO_MAX_DEPTH_RATIO)
                mean_lift = 1.0 + (mean_lift - 1.0) * ratio

        return max(1.0, min(mean_lift, Helper.PROMO_MAX_LIFT))

    # z = standard deviations of cushion. Set against holding cost, not just
    # dispersion: DEPERIBILI is measured at 2.34 but extra buffer there becomes
    # waste, so it gets deliberately less.
    SAFETY_Z_BY_SETTORE = {
        "GENERI VARI": 1.0,
        "SURGELATI": 1.0,
        "DEPERIBILI": 0.8,
        "RIFATTURAZIONE": 1.0,
    }
    SAFETY_Z_DEFAULT = 1.0
    SIGMA_MIN_DAYS = 28

    @staticmethod
    def safety_z_for(settore):
        return Helper.SAFETY_Z_BY_SETTORE.get(settore, Helper.SAFETY_Z_DEFAULT)

    @staticmethod
    def deviation_factor(deviation):
        """Trend multiplier from calculate_deviation. Scales the statistical buffer
        only — a forecast signal has no business shrinking the shelf's facings."""
        if deviation >= 40:
            return 1.3
        if deviation >= 20:
            return 1.2
        if deviation <= -40:
            return 0.6
        if deviation <= -20:
            return 0.8
        return 1.0

    # Below this daily rate a product is a slow mover: it gets a facings penalty
    # instead of a demand buffer, because the pack size already dwarfs its demand
    # (0.16/day against a pack of 8 is 50 days of cover).
    SLOW_MOVER_THRESHOLD = 0.6

    @staticmethod
    def slow_mover_reduction(avg_daily_sales):
        """Units to shave off a slow mover, 0 if it isn't one. A judgement about
        how many facings a near-dead product earns, not a statistical statement."""
        if avg_daily_sales >= Helper.SLOW_MOVER_THRESHOLD:
            return 0
        reduction = 1
        if avg_daily_sales <= 0.1:
            reduction += 1
            if avg_daily_sales <= 0.05:
                reduction += 1
        return reduction

    @staticmethod
    def closure_day_mask(store_daily_totals, threshold=0.05):
        """
        Flag slots where the whole store sold ~nothing — closures, or days the
        VENSETAR sync never delivered. Returns list[bool], True = ignore.

        These land in sales_sets as real zeros (the absent-product branch writes 0
        for any verified product with stock) and wreck sigma, since variance is
        squared: 8 such days on a product selling 191/day account for ~40% of its
        measured dispersion. Detected from the data, not the closure calendar, so
        it also catches sync gaps.
        """
        if not store_daily_totals:
            return []

        positive = [t for t in store_daily_totals if t and t > 0]
        if not positive:
            return [True] * len(store_daily_totals)

        cutoff = statistics.median(positive) * threshold
        return [(t is None or t <= cutoff) for t in store_daily_totals]

    @staticmethod
    def demand_sigma_daily(sales_sets: list, closure_mask=None, today=None):
        """
        Standard deviation of daily demand, on weekday-adjusted residuals.
        None when there is too little history to trust.

        Replaces the Poisson assumption hidden in `buff = sqrt(req_stock - 1) - 1`,
        which is only right when variance equals the mean. Measured dispersion is
        2.67 volume-weighted and 20-60 for pack-bought beverages (batch demand has
        variance scaling with the SQUARE of the batch size), and under sqrt(mu) the
        delivered service level is 1/sqrt(dispersion) — least cushion to the most
        erratic products.
        """
        if not sales_sets:
            return None

        base_dow = (today or datetime.now().date()).weekday()

        observed = []
        for i, v in enumerate(sales_sets):
            if v is None:
                continue
            if closure_mask and i < len(closure_mask) and closure_mask[i]:
                continue
            observed.append(((base_dow - 1 - i) % 7, float(v)))

        if len(observed) < Helper.SIGMA_MIN_DAYS:
            return None

        by_dow = defaultdict(list)
        for dow, v in observed:
            by_dow[dow].append(v)

        # A weekday seen once contributes a zero residual by construction
        dow_means = {d: sum(vs) / len(vs) for d, vs in by_dow.items() if len(vs) >= 2}
        residuals = [v - dow_means[dow] for dow, v in observed if dow in dow_means]

        # Estimating one mean per weekday costs one degree of freedom each;
        # dividing by N instead of (N - groups) understates sigma by ~11%.
        dof = len(residuals) - len(dow_means)
        if dof <= 0 or len(residuals) < Helper.SIGMA_MIN_DAYS:
            return None

        return (sum(r * r for r in residuals) / dof) ** 0.5

    @staticmethod
    def calculate_deviation(sales_sets: list, silent: bool = False):
        """
        Shift in daily demand, recent window vs baseline, as a percentage clamped
        to [-50, 50]. 0 when indistinguishable from sampling noise.

        Three choices, each fixing a measured defect of the old 8-day-median form:

        - Whole-week windows, both of them. An 8-day window is a week plus a day,
          so it double-counts whichever weekday the run lands on — worth +11% on
          stationary demand when the order ran on a Sunday. The baseline is
          truncated to a multiple of 7 for the same reason.
        - Means, not medians. Daily counts are small integers; at ~3 units/day the
          median took only ~20 distinct values, 28% of them exactly 0. Outliers
          are already winsorised in avg_daily_sales_from_sales_sets.
        - A Welch noise gate, not a fixed percentage. A 20% gap means something
          different at 20 units/day than at 2: the constant threshold fired on 55%
          of stationary slow movers and got the sign wrong on 11% of real trends.
          Also removes the need for the arbitrary "median_baseline < 2" cut.
        """
        recent_window = 14
        min_baseline = 14
        z_min = 1.5

        # Filter out None entries (out-of-stock days where demand was censored)
        sales_sets = [v for v in sales_sets if v is not None]

        # Needs both a full recent week-pair and a comparable baseline
        if len(sales_sets) < recent_window + min_baseline:
            return 0

        recent = sales_sets[:recent_window]
        # Whole weeks only: a 46-day baseline counts four weekdays seven times and
        # the other three six times, reintroducing the run-day bias the 14-day
        # recent window was sized to avoid.
        baseline = sales_sets[recent_window:]
        baseline = baseline[:(len(baseline) // 7) * 7]

        mean_recent = statistics.mean(recent)
        mean_baseline = statistics.mean(baseline)

        if mean_baseline <= 0:
            return 0

        # Welch standard error of the difference between the two window means —
        # unequal window sizes and unequal variances are both expected here.
        se_welch = math.sqrt(
            statistics.variance(recent) / len(recent)
            + statistics.variance(baseline) / len(baseline)
        )

        # Floor it at the Poisson standard error under "both rates are equal".
        # Sample variance collapses to 0 on a window of all zeros, which reads as
        # perfect certainty and inflates z: at 0.1 units/day that fired on 22% of
        # stationary products, 91% of them pinned to the -50% clamp. Counts can
        # never really be that certain. Taking the larger of the two keeps the
        # empirical estimate wherever demand is over-dispersed, which is most of
        # the catalogue, and only binds where the sample has gone degenerate.
        pooled_rate = (sum(recent) + sum(baseline)) / (len(recent) + len(baseline))
        se_poisson = math.sqrt(pooled_rate * (1 / len(recent) + 1 / len(baseline)))

        se = max(se_welch, se_poisson)

        if se <= 0:
            return 0

        z = (mean_recent - mean_baseline) / se
        if abs(z) < z_min:
            return 0

        deviation = round((mean_recent - mean_baseline) / mean_baseline * 100, 2)
        deviation = max(-50, min(deviation, 50))

        if not silent:
            logger.info(
                f"Deviation {deviation:+.1f}% (z={z:.2f}, "
                f"recent={mean_recent:.2f}/day over {len(recent)}d, "
                f"baseline={mean_baseline:.2f}/day over {len(baseline)}d)"
            )

        return deviation

    @staticmethod
    def merge_sales_sets(primary: list, secondary: list) -> list:
        """
        Merge two sales_sets arrays (newest-first) by summing per-day slots.
        None + value → value (stockout on one side doesn't censor combined demand).
        None + None  → None (both out of stock — true stockout).
        """
        max_len = max(len(primary), len(secondary))
        merged = []
        for i in range(max_len):
            a = primary[i] if i < len(primary) else None
            b = secondary[i] if i < len(secondary) else None
            if a is None and b is None:
                merged.append(None)
            else:
                merged.append((a or 0) + (b or 0))
        return merged

    @staticmethod
    def compute_expiry_factor(expired_array, sold_array):
        """
        Returns a minimum_stock penalty factor based on historical expiry rate,
        or None if expiry rate is below the 5% threshold.
        Only the most recent 3 months (index 0–2) are considered.
        """
        recent_expired = expired_array[:3]
        total_expired = 0
        for entry in recent_expired:
            if isinstance(entry, list) and len(entry) >= 1:
                total_expired += entry[0]
            elif isinstance(entry, (int, float)):
                total_expired += entry

        total_sold = sum(v for v in sold_array[:3] if v is not None)
        denominator = total_sold + total_expired
        if denominator == 0:
            return None

        expiry_rate = total_expired / denominator
        
        if expiry_rate <= 0.05:
            return None
        elif expiry_rate <= 0.1:
            factor = 0.6
        elif expiry_rate <= 0.2:
            factor = 0.4
        else:
            factor = 0.2

        logger.info(f"Expiry factor: rate={expiry_rate:.1%} ({total_expired} expired / {denominator} total) → factor={factor}")
        return factor

    @staticmethod
    def compute_batch_expiry_factor(bought_sets, sales_sets, stock, shelf_life_days, avg_daily_sales):
        """
        Returns True if a delivery batch is at risk of expiring before being
        fully consumed, or None if no risk. This is a binary signal, not a
        graduated one — callers should cap minimum_stock at 1 outright when
        True, rather than scale it.

        Anchors on current `stock` (ground truth) rather than reconstructing sales
        history from delivery dates: under FIFO, stock is drawn from the most
        recent delivery first, so any stock beyond what that delivery held must
        be leftover from the previous one.

        The clearance rate is derived empirically from `sales_sets` — walking
        back from yesterday until the units already known to be sold from the
        active batch are accounted for — rather than the passed-in average,
        since that can smooth over stockout days or a recent pace shift that
        matters for this specific batch.
        """
        if not bought_sets or avg_daily_sales <= 0:
            return None

        deliveries = [(i, qty) for i, qty in enumerate(bought_sets) if qty and qty > 0]
        if not deliveries:
            return None

        i0, qty0 = deliveries[0]

        # Only meaningful with a second delivery on record — with just one,
        # there's nothing "previous" for stock to have leftover from.
        leftover_prev = stock - qty0 if len(deliveries) >= 2 else 0

        if leftover_prev > 0:
            # Current stock exceeds what the most recent delivery could hold,
            # so the excess must still be leftover from the previous one.
            i_prev, qty_prev = deliveries[1]
            i_batch, qty_batch, remaining = i_prev, qty_prev, min(leftover_prev, qty_prev)
        else:
            # Stock fits entirely within the most recent (or only) delivery, so
            # any earlier one is confirmed fully sold through — no risk from it.
            i_batch, qty_batch, remaining = i0, qty0, stock
            if remaining <= 0:
                return None

        days_left = shelf_life_days - i_batch

        if days_left <= 0:
            logger.info(f"Batch expiry: delivery ({qty_batch} units, {i_batch}d ago) already past {shelf_life_days}d shelf life")
            return True

        sold_from_batch = qty_batch - remaining
        recent_rate = avg_daily_sales
        if sold_from_batch > 0:
            cumulative = 0
            for day_idx, v in enumerate(sales_sets):
                if v is not None:
                    cumulative += v
                if cumulative >= sold_from_batch:
                    recent_rate = sold_from_batch / (day_idx + 1)
                    break

        days_to_clear = remaining / recent_rate
        if days_to_clear < days_left:
            return None

        logger.info(f"Batch expiry risk: {remaining:.1f} units remaining, {days_left}d left of {shelf_life_days}d shelf life, {days_to_clear:.1f}d to clear (rate={recent_rate:.2f})")
        return True

    @staticmethod
    def next_article(product_cod, product_var, package_size, product_name, reason):
        logger.info(f"Will NOT order {product_name}: {product_cod}.{product_var}.{package_size}!")
        logger.info(f"Reason : {reason}")

    @staticmethod
    def order_denied(product_cod:int, product_var:int, package_size:int, product_name:str, category:str, check:int):
        logger.info(f"Will NOT order {product_name}!")
        logger.info(f"Reason : {category}{check}")

    @staticmethod
    def order_this(current_list: list, product_cod: int, product_var: int, qty: int, product_name: str, category: str, check: int, discount: float = None):
        current_list.append((product_cod, product_var, qty, discount))
        
        if discount:
            logger.info(f"ORDER {product_name}: {qty}! 🏷️ ON SALE: {discount}% OFF")
        else:
            logger.info(f"ORDER {product_name}: {qty}!")
        
        logger.info(f"Reason: {category}{check}")
              
    @staticmethod
    def parse_promo_pdf(file_path):
        data = []
        sale_start = None
        sale_end = None

        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""

                # Extract promo dates
                m = re.search(
                    r"Pubblico\s*Dal\s*(\d{2}/\d{2}/\d{4})\s*al\s*(\d{2}/\d{2}/\d{4})",
                    text
                )
                if m:
                    sale_start = datetime.strptime(m.group(1), "%d/%m/%Y").date().isoformat()
                    sale_end = datetime.strptime(m.group(2), "%d/%m/%Y").date().isoformat()

                table = page.extract_table({
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "intersection_tolerance": 5,
                })

                if not table:
                    continue

                for row in table:
                    try:
                        if not row or len(row) < 7:
                            continue

                        codice = str(row[1]) if row[1] else None
                        cost = row[5]
                        price = row[6]

                        if not codice or "." not in codice:
                            continue

                        cod, v = codice.split(".")

                        data.append((
                            int(cod),
                            int(v),
                            float(cost.replace(",", ".")) if cost else None,
                            float(price.replace(",", ".")) if price else None,
                            sale_start,
                            sale_end,
                        ))
                    except Exception:
                        continue

        return data