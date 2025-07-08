from datetime import datetime
from calendar import monthrange
from logger import logger
import math



class Helper:

    def __init__(self) -> None:
        # Get the current month and day
        self.current_month = datetime.now().month
        self.current_day = datetime.now().day
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

        # Remove the last elements from both lists if the bought-list has a zero as last element
        while len(final_array_bought) > 0 and final_array_bought[-1] == 0 and final_array_sold[-1] == 0:
            final_array_bought.pop()
            final_array_sold.pop()

        return final_array_bought, final_array_sold

    def calculate_weighted_avg_sales(self, sales_period, final_array_sold, previous_year_sold):
        sales_period = min(sales_period, len(final_array_sold))
        sold_daily_this_month = final_array_sold[0]
        if len(final_array_sold) >= 2:
            sold_daily_previous_month = final_array_sold[1]
        else:
            sold_daily_previous_month = 0
        sold_daily_tot = sold_daily_this_month + sold_daily_previous_month
        previous_year_sold.reverse()
        last_year_current_month = previous_year_sold[self.current_month-1]
        if (last_year_current_month != 0):
            sold_daily_tot += last_year_current_month 
        else:
            sales_period -= 1
        
        if self.current_day <= 15:
            avg_daily_sales = sold_daily_tot / ((sales_period*30)+(self.current_day-1))
        else:
            sold_daily_previous_month = sold_daily_previous_month * ((30 - self.current_day) / 30)
            if sales_period >= 1:
                avg_daily_sales = (sold_daily_this_month + sold_daily_previous_month + last_year_current_month) / (sales_period*30)
            else: 
                avg_daily_sales = sold_daily_this_month / (self.current_day-1)


        if (avg_daily_sales != 0):
            logger.info(f"Avg. Daily Sales = {avg_daily_sales:.2f}")                       
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

    def calculate_deviation(self, final_array_sold, recent_months):
        today = datetime.now()
        dim = monthrange(today.year, today.month)[1]
        this_month = final_array_sold[0]
        last_month = final_array_sold[1]
        days_to_recover = dim - (today.day - 1)
        if (days_to_recover > 0):
            last_month = (days_to_recover/dim)*last_month
            this_month += last_month
        if recent_months != 0:
            deviation = ((this_month - recent_months) /recent_months)*100
            deviation = round(deviation, 2)
        else:
            deviation = 0
        deviation_corrected = max(-50, min(deviation, 50))
        return deviation_corrected
    
    def deviation_blender(self, deviation, ly_deviation):
        today = datetime.now()
        dim = monthrange(today.year, today.month)[1]
        day = today.day

        alpha = 1.0 - (day / dim)
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
        logger.info(f"True Stock = {true_stock}")
        return true_stock

    def find_current_gap(self, final_array_sold, final_array_bought):
        if final_array_sold[0] == 0 and final_array_bought[0] == 0:
            current_gap = final_array_bought[1] - final_array_sold[1]
        else:
            current_gap = final_array_bought[0] - final_array_sold[0]
        return current_gap

    def find_trend(self, final_array_sold, final_array_bought):
        today = datetime.now()
        diffs = []
        total = 0
        start = 0
        end = 3
        if today.day == 1:
            start = 1
            end = 4

        for sold, bought in zip(final_array_sold[start:end], final_array_bought[start:end]):
            diffs.append(bought - sold)


        if diffs[0] != 0 and diffs[1] != 0 and (diffs[0] > 0) == (diffs[1] > 0):
            total = diffs[0] + diffs[1]
            if diffs[2] != 0 and (diffs[2] > 0) == (diffs[0] > 0):
                total += diffs[2]
            logger.info(f"Trend value is {total}")
            return total
        logger.info(f"No trend")
        return 0

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
        logger.info(f"Will NOT order {product_name}: {product_cod}.{product_var}.{package_size}!")
        logger.info(f"Reason : {category}{check}")

    def order_this(self, current_list: list, product_cod: int, product_var: int, qty: int, product_name: str, category: str, check: int):
        current_list.append((product_cod, product_var, qty))  # <-- store as tuple now
        logger.info(f"ORDER {product_name}: {qty}!")
        logger.info(f"Reason: {category}{check}")
              
    def line_breaker(self):
        logger.info(f"=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/")
