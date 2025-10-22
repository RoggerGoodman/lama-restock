import math
from helpers import Helper

def process_N_sales(package_size, deviation_corrected, avg_daily_sales, avg_daily_sales_corrected, expected_packages, req_stock, stock, package_consumption, current_gap, trend, turnover, helper: Helper):

    order = 1
    original_stock = stock
    stock = stock - round(avg_daily_sales) #Prevision: sales of the day    
    req_stock = round(req_stock)
    minimum_threshold = 4
    if avg_daily_sales_corrected >= 1 :
        minimum_threshold += round(avg_daily_sales)
    elif avg_daily_sales_corrected < 0.4:
        minimum_threshold -= 1
    minimum_stock = max(math.ceil(req_stock/2), minimum_threshold)
    leftover_stock = stock - req_stock
    leftover_stock = max(leftover_stock, 0)
    if deviation_corrected >= 40:
        minimum_stock *= 1.2
    elif deviation_corrected >= 20:
        minimum_stock *= 1.1

    target = req_stock + minimum_stock - stock

    if target > 0:
        while package_size*order < target:
            order += 1 
        return order, 5, "N_success"

    if leftover_stock <= minimum_stock:
        while minimum_stock > leftover_stock + package_size*order:
            order += 1 
        return order, 1, "N_success"

    if package_consumption >= 1:
        order = math.floor(package_consumption)
        if math.floor(original_stock/package_size) >= order:
            order -= math.floor(original_stock/package_size) - order
        if order >= 0:
            order = max(order, 1)
            return order, 2, "N_success"
        else : order = 1       
        
    return None, 0, "N_fail"

    