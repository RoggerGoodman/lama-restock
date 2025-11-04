import math
from helpers import Helper

def process_N_sales(package_size, deviation_corrected, avg_daily_sales, avg_daily_sales_corrected, req_stock, stock):

    order = 1
    stock = stock - round(avg_daily_sales) #Prevision: sales of the day    
    req_stock = round(req_stock)
    minimum_stock = 4
    if avg_daily_sales_corrected >= 1 :
        minimum_stock += round(avg_daily_sales)
        minimum_stock += math.floor(req_stock * 0.1)
    elif avg_daily_sales_corrected < 0.6:
        minimum_stock -= 1
    leftover_stock = stock - req_stock
    #leftover_stock = max(leftover_stock, 0)
    if deviation_corrected >= 40:
        minimum_stock = math.floor(minimum_stock * 1.2)
    elif deviation_corrected >= 20:
        minimum_stock = math.floor(minimum_stock * 1.1)
    elif deviation_corrected <= -20:
        minimum_stock = math.ceil(minimum_stock * 0.9)
    elif deviation_corrected <= -40:
        minimum_stock = math.ceil(minimum_stock * 0.7)

    
   
    if leftover_stock <= minimum_stock:
        while minimum_stock > leftover_stock + package_size*order:
            order += 1 
        return order, 1, "N_success"
        
    return None, 0, "N_fail"

    target = req_stock + minimum_stock - stock

    if target > 0:
        while package_size*order <= target:
            order += 1 
        return order, 5, "N_success"

    