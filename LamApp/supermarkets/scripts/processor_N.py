import math
from .logger import logger

def process_N_sales(package_size, deviation_corrected, avg_daily_sales, avg_sales_last_year, req_stock, stock):

    order = 1   
    req_stock = round(req_stock)
    minimum_stock = max(avg_sales_last_year, 4)
    leftover_stock = stock - req_stock
    
    if avg_daily_sales >= 1 :
        minimum_stock += round(avg_daily_sales)
        minimum_stock += math.floor(req_stock * 0.1)
    elif avg_daily_sales < 0.6:
        minimum_stock -= 1
        if avg_daily_sales < 0.2:
            minimum_stock -= 1

    logger.info(f"Minimum Stock = {minimum_stock}")

    if deviation_corrected >= 40:
        minimum_stock = math.floor(minimum_stock * 1.2)
    elif deviation_corrected >= 20:
        minimum_stock = math.floor(minimum_stock * 1.1)
    elif deviation_corrected <= -20:
        minimum_stock = math.ceil(minimum_stock * 0.9)
    elif deviation_corrected <= -40:
        minimum_stock = math.ceil(minimum_stock * 0.7)

    minimum_stock = round(minimum_stock)

    order = (req_stock + minimum_stock - stock) / package_size
    if order >= 0:
        tollerance_threshold = avg_daily_sales/package_size
        decimal_part = order % 1
        if decimal_part <= tollerance_threshold:
            order = math.floor(order)
        else:
            order = math.ceil(order)

        if order >= 1:
            return order, 1, "N_success"
        
    if leftover_stock <= minimum_stock:
        order = 1
        return order, 2, "N_success"
       
    return None, 0, "N_fail"

    