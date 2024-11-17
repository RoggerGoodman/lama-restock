from datetime import datetime
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
        for i in range(len(final_array_bought) - 2):
            if final_array_bought[i] == 0 and final_array_sold[i] == 0:
                if (final_array_bought[i+1] == 0 and final_array_bought[i+2] == 0 and
                    final_array_sold[i+1] == 0 and final_array_sold[i+2] == 0):
                    # Return lists sliced up to the start of the "3 zero combo"
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
        while len(final_array_bought) > 0 and final_array_bought[-1] == 0:
            final_array_bought.pop()
            final_array_sold.pop()

        return final_array_bought, final_array_sold

    def calculate_weighted_avg_sales(self, sales_period, final_array_sold, previous_year_sold):
        sales_period = min(sales_period, len(final_array_sold))
        sold_daily = sum(final_array_sold[:sales_period])
        previous_year_sold.reverse()
        last_year_current_month = previous_year_sold[self.current_month-1]
        if (last_year_current_month != 0):
            sold_daily += last_year_current_month
        else:
            sales_period -= 1
        avg_daily_sales = sold_daily / ((sales_period*30)+(self.current_day-1))
        if (avg_daily_sales != 0):
            logger.info(f"Avg. Daily Sales = {avg_daily_sales:.2f}")                       
        return avg_daily_sales
    
    def calculate_data_recent_months(self, list:list, period:int, mode:str):
        recent_months = sum(list[1:period+1])/period
        logger.info(f"Average {mode} in recent months = {recent_months:.2f}")
        return recent_months
    
    def calculate_avg_monthly_sales(self,final_array_sold):
        sold_yearly = sum(final_array_sold[1:12])
        avg_monthly_sales = sold_yearly / 12
        logger.info(f"Avg. Monthly Sales = {avg_monthly_sales:.2f}")
        return avg_monthly_sales

    def calculate_deviation(self, final_array_sold, recent_months):
        this_month = final_array_sold[0]
        last_month = final_array_sold[1]
        days_to_recover = 30 - (datetime.now().day - 1)
        if (days_to_recover > 0):
            last_month = (days_to_recover/30)*last_month
            this_month += last_month
        if recent_months != 0:
            deviation = ((this_month - recent_months) /recent_months)*100
            deviation = round(deviation, 2)
        else:
            deviation = 0
        logger.info(f"Deviation = {deviation} %")
        deviation_corrected = max(-50, min(deviation, 50))
        return deviation_corrected

    def calculate_avg_stock(self, stock_period, final_array_sold, final_array_bought, package_size): #Currently not used
        if (stock_period <= len(final_array_sold)):
            sold = final_array_sold[stock_period-1]
            bought = final_array_bought[stock_period-1]
            stock = bought - sold
            if stock >= 1:
                self.stock_list.append(stock)
                return self.calculate_avg_stock(stock_period + 1, final_array_sold, final_array_bought, package_size)
            elif sold == 0 and bought == 0:
                return self.calculate_avg_stock(stock_period + 1, final_array_sold, final_array_bought, package_size)
            else:
                self.stock_list.append(package_size)
                return self.calculate_avg_stock(stock_period + 1, final_array_sold, final_array_bought, package_size)
        else:
            if (len(self.stock_list) > 0):
                average_value = sum(self.stock_list) / len(self.stock_list)
                rounded_up_average = math.ceil(average_value)
                self.stock_list.clear()
                logger.info(f"Avg. Stock = {rounded_up_average:.2f}")
                return rounded_up_average
            else:
                return 0

    def calculate_supposed_stock(self, final_array_bought, final_array_sold): #Currently not used
        previous_index = 0
        supposed_stock = 0
        stop = False
        for index, value in enumerate(final_array_bought):
            if value > 0:
                bought = 0
                bought += value
                sold_since_last_restock = sum(final_array_sold[previous_index:index+1])
                stock = bought - sold_since_last_restock
                if stock == 0:
                    previous_index = index + 1
                    continue
                elif (supposed_stock * stock > 0 or supposed_stock == 0) and not stop:
                    supposed_stock += stock
                    previous_index = index + 1
                else:
                    supposed_stock += stock
                    previous_index = index + 1
                    if stop:
                        break
                    stop = True
                    
        logger.info(f"Supposed Stock = {supposed_stock}")
        return supposed_stock
    
    def calculate_stock_oscillation(self, final_array_bought, final_array_sold, avg_daily_sales, package_size):
        previous_index = 0
        oscillation = 0
        combo = 0
        prevision = math.ceil(avg_daily_sales)
        change = 0
        for index, value in enumerate(final_array_bought):
            if value > 0:
                bought = 0
                bought += value
                sold_since_last_restock = sum(final_array_sold[previous_index:index+1])
                stock = bought - sold_since_last_restock
                if stock == 0:
                    combo += 1
                    previous_index = index + 1
                    continue
                elif (oscillation * stock > 0 or oscillation == 0):
                    oscillation += stock
                    combo += 1
                    previous_index = index + 1
                else:
                    if oscillation < 0 or oscillation >= package_size*2:
                        oscillation += stock
                    if oscillation - prevision == 0:
                        change += oscillation
                        oscillation = 0
                        previous_index = index + 1
                        continue 
                    break
        oscillation -= prevision
        oscillation += change
        logger.info(f"Stock Oscillation = {oscillation}")
        return oscillation
    
    def calculate_expectd_packages(self, final_array_bought:list, package_size:int):
        recent_months_bought = self.calculate_data_recent_months(final_array_bought, 3, "bought")
        monthly_packages = math.floor(recent_months_bought / package_size)
        daily_packages = monthly_packages / 30
        expected_packages = daily_packages * (self.current_day - 1)
        expected_packages -= final_array_bought[0]/package_size
        logger.info(f"Expected packages = {expected_packages:.2f}")
        return expected_packages

    def calculate_stock(self, final_array_sold, final_array_bought):
        tot_sold = sum(final_array_sold)
        tot_bought = sum(final_array_bought)
        true_stock = tot_bought - tot_sold
        logger.info(f"True Stock = {true_stock}")
        return true_stock

    def custom_round(self, value, threshold):
        # Get the integer part and the decimal part
        integer_part = int(value)
        decimal_part = value - integer_part

        # Apply the rounding logic
        if decimal_part <= threshold:
            return integer_part  # Round down
        else:
            return integer_part + 1  # Round up
        
    def custom_round2(self, value, deviation, current_stock, package_size): #Currently not used
        # Get the integer part and the decimal part
        integer_part = int(value)
        decimal_part = value - integer_part

        # Apply the rounding logic
        if decimal_part <= 0.3:  # TODO Could be made user editable
            if (deviation > 10):
                return integer_part + 1  # Round up
            elif (current_stock < package_size/4):
                return integer_part + 1  # Round up
            return integer_part  # Round down
        else:
            return integer_part + 1  # Round up



    # def custom_round_misha_edition(value):
    #     if not isinstance(value, int) or not isinstance(value, float):
    #         raise ValueError('Value must be a fucking int or float dumbass')
    #     return int(value) if value - int(value) <= 0.3 else int(value) + 1
    #if combo == 1 and final_array_bought[0]/package == 1:
        #oscillation += final_array_bought[1]-final_array_sold[1]