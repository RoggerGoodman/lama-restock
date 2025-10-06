import math

def process_U_sales(stock_oscillation, deviation_corrected, req_stock, current_gap):

    if stock_oscillation <= 1:
        order = math.ceil(req_stock)
        if abs(stock_oscillation) > order:
            dif = stock_oscillation + order
            order += abs(dif)
        
        cap = math.ceil(req_stock)
        if deviation_corrected >= 20: 
            cap += 1
        if order > cap:
            order = cap

        order += math.floor(current_gap/2)

        return order, 1, "U_success"
    
    if current_gap < 0 and deviation_corrected >= 0:
        order = abs(current_gap)
        return order, 2, "U_success"
    
    if deviation_corrected >= 40 and (stock_oscillation - req_stock) <= 1:
        return 1, 3, "U_success"
    
    return None, 0, "U_fail"