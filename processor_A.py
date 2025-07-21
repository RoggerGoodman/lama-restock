import math
from helpers import Helper

def process_A_sales(stock_oscillation, package_size, deviation_corrected, real_need, expected_packages, req_stock, current_gap, trend, turnover, helper: Helper):
    """
    Determines the processing outcome for a Category A product.

    Parameters:
        stock_oscillation (int): Oscillation of stock.
        package_size (int): Size of the package.
        deviation_corrected (float): Corrected deviation value.
        real_need (float): The effective needed ammount (to be computed if missing).
        expected_packages (float): Expected number of packages to buy.
        req_stock (float): Required stock level before the next restok.
        use_stock (bool): Whether to use stock.
        stock (int): Current stock level.

    Returns:
        tuple: (result, reason, status)
    """
    order = 1

    if real_need >= package_size or req_stock >= package_size*3:
        order = helper.custom_round(real_need / package_size, 0.6)

        if stock_oscillation <= -package_size:
            order += helper.custom_round(abs(stock_oscillation) / package_size, 0.8)

        if (order*package_size / real_need) < 1.2 and deviation_corrected > 20:
            order += 1
        elif current_gap <= -package_size and stock_oscillation <= 0:
            order += 1

        if trend < 0 and abs(trend) >= package_size:
            order += 1

        if deviation_corrected >= 45 and stock_oscillation <= package_size:
            order +=1

        if req_stock >= package_size*3:
            order +=1

        if order < expected_packages/2:
           order =  math.ceil((order + expected_packages)/2)

        cap = math.ceil(req_stock/package_size)
        if deviation_corrected >= 20: 
            cap += 1
        if order > cap:
            order = cap
           
        return order, 1, "A_success"

    if real_need > math.ceil(package_size / 2) and (deviation_corrected > 20 or stock_oscillation <= math.floor(-package_size / 3)):

        if stock_oscillation <= -package_size:
            order += 1

        if (order*package_size / real_need) < 1.2 and deviation_corrected > 20:
            order += 1
        
        return order, 2, "A_success"
    
    if deviation_corrected >= 25:
        if stock_oscillation < req_stock:
            order += 1
            
        if (stock_oscillation - req_stock) <= math.floor(package_size*0.7) and package_size <= 8:
                return order, 3, "A_success"
            
        if (stock_oscillation - req_stock) <= math.floor(package_size*0.5) and package_size < 18:
                return order, 3, "A_success"
            
        if (stock_oscillation - req_stock) <= math.floor(package_size*0.3) and package_size >= 18:
                return order, 3, "A_success"

    if package_size >= 20 and real_need >= math.ceil(package_size / 4):
        return 1, 4, "A_success"
    
    if turnover >= 0.8 and (stock_oscillation <= package_size*2 or expected_packages >= 1):
        return 1, 5, "A_success"
    
    if stock_oscillation >= 0 and stock_oscillation < current_gap and (current_gap - req_stock) <= package_size:
        return 1, 6, "A_success"

    return None, 0, "A_fail"
