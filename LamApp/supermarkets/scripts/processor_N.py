# LamApp/supermarkets/scripts/processor_N.py
import math
import logging

# Use Django's logging system
logger = logging.getLogger(__name__)

def process_N_sales(package_size, deviation_corrected, avg_daily_sales, avg_sales_last_year,
                   req_stock, stock, discount=None, minimum_stock_base=None, minimum_stock_override=None):
    """
    Process N category sales and calculate order quantity.

    Args:
        minimum_stock_base: Base minimum stock from the storage (set per-storage in the UI)
        minimum_stock_override: Product-level override; if set, bypasses the max(avg, base) logic
    """
    order = 1
    req_stock = round(req_stock)

    if minimum_stock_override is not None:
        minimum_stock = minimum_stock_override
    else:
        minimum_stock = max(avg_sales_last_year, minimum_stock_base)
    leftover_stock = stock - req_stock
    
    if avg_daily_sales >= 1:
        minimum_stock += round(avg_daily_sales)
        if discount != None:
            minimum_stock += round(avg_daily_sales)
        minimum_stock += math.floor(req_stock * 0.2)
    elif avg_daily_sales < 0.6:
        minimum_stock -= 1
        if avg_daily_sales < 0.3:
            minimum_stock -= 1
            if avg_daily_sales < 0.2:
                minimum_stock -= 1
                if avg_daily_sales < 0.1:
                    minimum_stock -= 1

    logger.info(f"Minimum Stock = {minimum_stock} (base: {minimum_stock_base}, override: {minimum_stock_override})")

    if deviation_corrected >= 40:
        minimum_stock = math.floor(minimum_stock * 1.2)
    elif deviation_corrected >= 20:
        minimum_stock = math.floor(minimum_stock * 1.1)
    elif deviation_corrected <= -40:
        minimum_stock = math.ceil(minimum_stock * 0.7)
    elif deviation_corrected <= -20:
        minimum_stock = math.ceil(minimum_stock * 0.9)

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
            return order, 1, True, discount
        
    if leftover_stock <= minimum_stock:
        order = 1
        return order, 2, True, discount
       
    return None, 0, False, discount