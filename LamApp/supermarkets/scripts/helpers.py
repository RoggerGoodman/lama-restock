from datetime import datetime
from calendar import monthrange
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
         
        # Calculate how many months until December
        if self.current_month < 12:
            self.months_to_discard = 12 - self.current_month
        else:
            self.months_to_discard = 0

    def clean_convert_reverse(self, values):
        cleaned_values = []
        for value in values:
            # If the value contains a decimal different from ',00', skip the entire row (outer loop iteration)
            if ',' in value and not value.endswith(',00'):
                return None  # This signals that the article must be skipped

            # Clean and convert
            cleaned_value = int(value.replace(',00', '').replace(
                '.', ''))  # Remove commas, convert to int
            cleaned_values.append(cleaned_value)

        cleaned_values.reverse()
        return cleaned_values

    def prepare_array(self, final_array_bought:list, final_array_sold:list):
        # Remove the first elements based on current month
        i = 0
        while len(final_array_bought) > 1 and i < self.months_to_discard:
            final_array_sold.pop(0)
            final_array_bought.pop(0)
            i += 1

        # Remove the last elements from both lists if the bought-list and the sold-list both have a zero as last element
        while len(final_array_bought) > 0 and final_array_bought[-1] == 0 and final_array_sold[-1] == 0:
            final_array_bought.pop()
            final_array_sold.pop()

        # If a list is empty, return [0] instead
        if not final_array_bought:
            final_array_bought = [0]
        if not final_array_sold:
            final_array_sold = [0]

        return final_array_bought, final_array_sold
    
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
    
    def avg_daily_sales_from_sales_sets(self, daily_sales: list, silent: bool = False):
        """
        Compute a recency-weighted average daily sales rate.

        Args:
            daily_sales: list[int], newest → oldest (index 0 is most recent day)
        Returns:
            float
        """

        if not daily_sales:
            return None

        min_days = 14
        half_life = 14
        outlier_k = 10

        observed_days = len(daily_sales)
        if observed_days < min_days:
            return None

        # Outlier capping: a day is an outlier if it exceeds outlier_k × p95.
        # p95 captures the "normal ceiling" of the product's sales.
        # Outliers are capped to the highest non-outlier value in the set,
        # preserving position and temporal structure.
        # If p95 == 0 (product barely sells), skip — no meaningful reference.
        sorted_vals = sorted(daily_sales)
        idx = 0.95 * (len(sorted_vals) - 1)
        lo = int(idx)
        hi = min(lo + 1, len(sorted_vals) - 1)
        p95 = sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])
        if p95 > 0:
            threshold = outlier_k * p95
            outlier_indices = [i for i, v in enumerate(daily_sales) if v > threshold]
            if outlier_indices:
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
    
    def calculate_deviation(self, sales_sets: list):
        """
        Calculate sales deviation from daily sales data.
        Compares median of recent 8 days vs median of baseline (days 8-30).
        Uses median for outlier resistance.

        Returns: deviation percentage clamped to [-50, 50]. 0 if insufficient data.
        """
        min_days = 14
        recent_window = 8

        if not sales_sets or len(sales_sets) < min_days:
            return 0

        recent = sales_sets[:recent_window]
        baseline = sales_sets[recent_window:]

        median_recent = statistics.median(recent)
        median_baseline = statistics.median(baseline)

        if median_baseline == 0:
            return 0

        deviation = ((median_recent - median_baseline) / median_baseline) * 100
        deviation = round(deviation, 2)
        return max(-50, min(deviation, 50))

    def calculate_stock(self, final_array_sold, final_array_bought):
        tot_sold = sum(final_array_sold)
        tot_bought = sum(final_array_bought)
        true_stock = tot_bought - tot_sold
        period = len(final_array_sold)
        if true_stock > 5 :
            true_stock = true_stock - math.floor(period/5)
        logger.info(f"True Stock = {true_stock}")
        return true_stock

    def next_article(self, product_cod, product_var, package_size, product_name, reason):
        logger.info(f"Will NOT order {product_name}: {product_cod}.{product_var}.{package_size}!")
        logger.info(f"Reason : {reason}")

    def order_denied(self, product_cod:int, product_var:int, package_size:int, product_name:str, category:str, check:int):
        logger.info(f"Will NOT order {product_name}!")
        logger.info(f"Reason : {category}{check}")

    def order_this(self, current_list: list, product_cod: int, product_var: int, qty: int, product_name: str, category: str, check: int, discount: float = None):
        current_list.append((product_cod, product_var, qty, discount))
        
        if discount:
            logger.info(f"ORDER {product_name}: {qty}! 🏷️ ON SALE: {discount}% OFF")
        else:
            logger.info(f"ORDER {product_name}: {qty}!")
        
        logger.info(f"Reason: {category}{check}")
              
    def parse_promo_pdf(self, file_path):
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