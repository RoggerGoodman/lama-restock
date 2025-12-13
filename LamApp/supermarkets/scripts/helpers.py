from datetime import datetime
from calendar import monthrange
from .logger import logger
import math


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

        self.stock_list = []

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

    def detect_dead_periods(self, final_array_bought, final_array_sold):
        for i in range(1, len(final_array_bought) - 2):
            if final_array_bought[i] == 0 and final_array_sold[i] == 0 and final_array_bought[i-1] > 0:
                if (final_array_bought[i+1] == 0 and final_array_bought[i+2] == 0 and
                    final_array_sold[i+1] == 0 and final_array_sold[i+2] == 0):
                    # Return lists sliced up to the start of the "3 zero combo"
                    logger.info("Dead period detected")
                    return final_array_bought[:i], final_array_sold[:i]
        
        # If no "3 zero combo" found, return the original lists
        return final_array_bought, final_array_sold

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

        return final_array_bought, final_array_sold

    def calculate_weighted_avg_sales(self, final_array_sold:list):
        
        sales_period = 2
        if len(final_array_sold) > 12:
            array_previous_year_sold = final_array_sold[self.current_month:]
        else : array_previous_year_sold = [0]
        sales_period = min(sales_period, len(final_array_sold))
        sold_this_month = final_array_sold[0]
        if len(final_array_sold) >= 2:
            sold_previous_month = final_array_sold[1]
        else:
            sold_previous_month = 0
        sold_tot = sold_this_month + sold_previous_month
        if sold_tot == 0:
            return sold_tot , 0
        if len(final_array_sold) > 12:
            position = 12 - self.current_month
            last_year_current_month = array_previous_year_sold[position]
            avg_sales_last_year = last_year_current_month/self.days_this_month
            sold_tot += last_year_current_month 
        else:
            sales_period -= 1
            avg_sales_last_year = 0
        
        if self.current_day <= 15:
            avg_daily_sales = sold_tot / max(((sales_period*30)+(self.current_day-1)), 1)
        else:
            avg_sold_previous_month = sold_previous_month * ((self.days_previous_month - self.current_day) / self.days_previous_month)
            if sales_period > 1:
                avg_daily_sales = (sold_this_month + avg_sold_previous_month + last_year_current_month) / (sales_period*30)
            elif sales_period > 0:
                avg_daily_sales = (sold_this_month + avg_sold_previous_month) / (sales_period*30)
            else: 
                avg_daily_sales = sold_this_month / (self.current_day-1)


        if (avg_daily_sales != 0):
            logger.info(f"Avg. Daily Sales = {avg_daily_sales:.2f}")
            logger.info(f"Avg. Sales this month of the previous year= {avg_sales_last_year:.2f}")                       
        return avg_daily_sales, avg_sales_last_year
    
    def calculate_weighted_avg_sales_new(self, final_array_sold: list, alpha: float = 3.0):
        """
        Returns (avg_daily_sales, avg_sales_last_year)

        - Handles new products gracefully.
        - Uses proportional growth between previous months (this vs last year)
        to stabilize early-month estimates.
        - Blend fades as month progresses.
        """

        if not final_array_sold:
            return 0.0, 0.0

        sold_this_month = final_array_sold[0] if len(final_array_sold) >= 1 else 0.0
        sold_prev_month = final_array_sold[1] if len(final_array_sold) >= 2 else 0.0
        sold_same_month_last_year = final_array_sold[12] if len(final_array_sold) > 12 else 0.0
        sold_prev_month_last_year = final_array_sold[13] if len(final_array_sold) > 13 else 0.0

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
    
    def avg_daily_sales_from_sales_sets(self, sales_sets):
        """
        Compute a recency-weighted average daily sales rate from sales_sets.
        If observed_days < min_days: fallback to old method.

        Args:
            sales_sets: list of [sold, days], oldest -> newest.
        Returns:
            float: estimated avg daily sales.
        """

        if not sales_sets:
            return 0
        
        min_days = 20
        half_life = 14

        # Total observed days across all intervals
        observed_days = sum(days for _, days in sales_sets)

        # If not enough data → use old method
        if observed_days < min_days:
            return 0

        # Exponential recency weighting
        lam = math.log(2) / half_life

        weighted_num = 0.0
        weighted_den = 0.0
        cumulative_days_from_now = 0  # used to compute age midpoint

        # Process newest → oldest
        for sold, days in sales_sets:
            rate = sold / days  # daily rate for this interval

            # age of midpoint of this interval
            age_mid = cumulative_days_from_now + (days / 2)
            weight = math.exp(-lam * age_mid)

            # Weighted by days so long intervals matter proportionally
            weighted_num += rate * days * weight
            weighted_den += days * weight

            cumulative_days_from_now += days

        avg_daily_sales = weighted_num / weighted_den
        try:
            logger.info(f"avg_daily_sales={avg_daily_sales:.2f}")
        except Exception:
            pass
        return avg_daily_sales 
    
    def calculate_data_recent_months(self, list: list, period: int):
        weights = [0.7, 0.2, 0.1]  # You can adjust these weights
        weighted_sum = sum(list[i+1] * weights[i] for i in range(period))
        recent_months = weighted_sum / sum(weights)
        return recent_months

    def calculate_avg_monthly_sales(self,final_array_sold):
        sold_yearly = sum(final_array_sold[1:12])
        avg_monthly_sales = sold_yearly / 12
        logger.info(f"Avg. Monthly Sales = {avg_monthly_sales:.2f}")
        return avg_monthly_sales

    def calculate_deviation(self, final_array_sold, recent_months, present_time : bool):
        this_month = final_array_sold[0]
        if present_time:
            last_month = final_array_sold[1]
            days_to_recover = self.days_this_month - (self.current_day - 1)
            if (days_to_recover > 0):
                last_month = (days_to_recover/self.days_this_month)*last_month
                this_month += last_month
        if recent_months != 0:
            deviation = ((this_month - recent_months) /recent_months)*100
            deviation = round(deviation, 2)
        else:
            deviation = 0
        deviation_corrected = max(-50, min(deviation, 50))
        return deviation_corrected
    
    def deviation_blender(self, deviation, ly_deviation):

        alpha = 1.0 - (self.current_day / self.days_this_month)
        blended = round(alpha * ly_deviation + (1.0 - alpha) * deviation, 2)

        return blended
    
    def calculate_stock_oscillation(self, final_array_bought, final_array_sold, avg_daily_sales):
        previous_index = 0
        oscillation = 0
        minimum_stock = 0
        positive_combo = False
        combo_breaker = False
        prevision = math.ceil(avg_daily_sales)
        change = 0
        for index, value in enumerate(final_array_bought):
            if value > 0:
                bought = 0
                bought += value
                sold_since_last_restock = sum(final_array_sold[previous_index:index+1])
                stock = bought - sold_since_last_restock
                if stock > 0 and index == 0:
                    change += stock
                    previous_index = index + 1
                    minimum_stock = stock
                    continue 
                if stock == 0:
                    previous_index = index + 1
                    continue
                elif (oscillation * stock > 0 or oscillation == 0):
                    if combo_breaker == True and stock < 0: # or previous_index < index) to limit the ammount of month he can check
                        break
                    oscillation += stock
                    previous_index = index + 1
                    if stock > 0:
                        positive_combo = True
                else:
                    if oscillation < 0:
                        oscillation += stock
                    if oscillation - prevision == 0 and positive_combo == False:
                        change += oscillation
                        oscillation = 0
                        previous_index = index + 1
                        continue 
                    if combo_breaker == False and positive_combo == False:
                        combo_breaker = True
                        change += oscillation
                        oscillation = 0
                        previous_index = index + 1
                        continue 
                    break
        if minimum_stock > 0 and (oscillation + change) < minimum_stock:
            oscillation = minimum_stock
        else:
            oscillation += change
        oscillation -= prevision
        logger.info(f"Stock Oscillation = {oscillation}")
        return oscillation

    def calculate_biggest_gap(self, final_array_bought, final_array_sold, avg_daily_sales):
            gaps = [b - s for b, s in zip(final_array_bought, final_array_sold)]
            max_abs_gap = max(abs(g) for g in gaps)          
            candidate_indexes = [i for i, g in enumerate(gaps) if abs(g) == max_abs_gap]
            best_stock = 0
            selected_index = 0

            for idx in candidate_indexes:
                    signed_gap = gaps[idx]
                    end = idx + 1 if signed_gap > 0 else idx

                    Stot = sum(final_array_sold[:end])
                    Btot = sum(final_array_bought[:end])
                    stock = Btot - Stot
                    if stock > best_stock:
                            selected_index = idx
                            best_stock = stock

            best_stock -= math.ceil(avg_daily_sales)
            if best_stock > 4 :
                best_stock = best_stock - math.floor(selected_index/5)
            logger.info(f"Biggest gap Stock = {best_stock} and the selected index was {selected_index}")
            return best_stock
    
    def calculate_max_stock(self, final_array_bought:list, final_array_sold:list):    
        
        bought = final_array_bought
        sold = final_array_sold
        
        max_stock = 0
        best_index = -1
        current_index = len(bought) - 1
        
        while bought and sold:
            # Calculate current stock
            tot_bought = sum(bought)
            tot_sold = sum(sold)
            stock = tot_bought - tot_sold
            
            # Update maximum if current stock is better
            if stock > max_stock:
                max_stock = stock
                best_index = current_index
            
            # Remove last elements and continue
            bought.pop()
            sold.pop()
            current_index -= 1

        if max_stock > 4:
            max_stock = max_stock - math.ceil(best_index/3)

        logger.info(f"Max Stock = {max_stock} and the selected index was {best_index}")
        return max_stock
    
    def calculate_expectd_packages(self, final_array_bought:list, package_size:int):
        monthly_packages = (sum(final_array_bought[1:4]) / package_size)/3
        daily_packages = monthly_packages / 30
        expected_packages = daily_packages * (self.current_day - 1)
        if final_array_bought[0] == 0:
            for x in range(1, 4):
                if final_array_bought[x] != 0:
                    break
                expected_packages += daily_packages * 30
        expected_packages -= final_array_bought[0]/package_size
        logger.info(f"Expected packages = {expected_packages:.2f}")
        return expected_packages

    def calculate_stock(self, final_array_sold, final_array_bought):
        tot_sold = sum(final_array_sold)
        tot_bought = sum(final_array_bought)
        true_stock = tot_bought - tot_sold
        period = len(final_array_sold)
        if true_stock > 5 :
            true_stock = true_stock - math.floor(period/5)
        logger.info(f"True Stock = {true_stock}")
        return true_stock

    def find_current_gap(self, final_array_sold, final_array_bought):
        if final_array_sold[0] == 0 and final_array_bought[0] == 0:
            current_gap = final_array_bought[1] - final_array_sold[1]
        else:
            current_gap = final_array_bought[0] - final_array_sold[0]
        return current_gap

    def find_trend(self, final_array_sold, final_array_bought):
        diffs = []
        start = 0
        
        if self.current_day == 1:
            start = 1

        for sold, bought in zip(final_array_sold[start:], final_array_bought[start:]):
            diffs.append(bought - sold)

        if diffs[0] == 0:
            logger.info(f"No trend")
            return 0

        total = diffs[0]
        direction = diffs[0] > 0  # True = positive, False = negative
        combo = 0

        i = 1
        while i < len(diffs):
            d = diffs[i]
            if d == 0:
                i += 1
                continue  # skip zeros

            if (d > 0) == direction:
                # same direction, accumulate
                total += d
                i += 1
                combo += 1
            else:
                # sign changed
                if  i + 1 < len(diffs) and abs(diffs[i+1]) > abs(d) and (diffs[i+1]) == direction:
                    # only continue if stronger than previous
                    total += d + diffs[i+1]
                    i+=2
                    combo += 1
                else:
                    break  # trend broken
        if combo == 0:
            logger.info(f"No trend")
            return 0        
        logger.info(f"Trend value is {total}")
        return total
        
    def calculate_turnover(self, final_array_sold:list, final_array_bought:list, package_size:int, trend):
        bonus = 0.05
        if any(x == 0 for x in final_array_sold[1:4]) or any(x == 0 for x in final_array_bought[1:4]):
            turnover = 0.0
        else :
            diffs = [abs(b - s) / package_size for s, b in zip(final_array_sold[1:4], final_array_bought[1:4])]
            turnover = 1.0 - sum(diffs) / len(diffs)
        if turnover > 0.7:
            if trend < 0:
                turnover += bonus
            elif trend > 0:
                turnover -= bonus
                
        logger.info(f"Turnover coefficient is {round(turnover, 3)}")
        return turnover       

    def custom_round(self, value, threshold):
        # Get the integer part and the decimal part
        integer_part = int(value)
        decimal_part = value - integer_part

        # Apply the rounding logic
        if decimal_part <= threshold:
            return integer_part  # Round down
        else:
            return integer_part + 1  # Round up

    def next_article(self, product_cod, product_var, package_size, product_name, reason):
        logger.info(f"Will NOT order {product_name}: {product_cod}.{product_var}.{package_size}!")
        logger.info(f"Reason : {reason}")

    def order_denied(self, product_cod:int, product_var:int, package_size:int, product_name:str, category:str, check:int):
        logger.info(f"Will NOT order {product_name}!")
        logger.info(f"Reason : {category}{check}")

    def order_this(self, current_list: list, product_cod: int, product_var: int, qty: int, product_name: str, category: str, check: int):
        current_list.append((product_cod, product_var, qty))  # <-- store as tuple now
        logger.info(f"ORDER {product_name}: {qty}!")
        logger.info(f"Reason: {category}{check}")
              
    def line_breaker(self):
        logger.info(f"=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/")
