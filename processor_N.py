import math
from helpers import Helper

def process_N_sales(package_size, deviation_corrected, real_need, expected_packages, req_stock, stock, package_consumption, current_gap, trend, turnover, helper: Helper):

    order = 1
    if package_consumption >= 0.2:
        if real_need >= package_size:
            order = helper.custom_round(real_need / package_size, 0.6)

            if stock <= -package_size:
                order += helper.custom_round(abs(stock) / package_size, 0.8)

            if (order*package_size / real_need) < 1.2 and deviation_corrected > 20:
                order += 1
            elif current_gap <= -package_size and stock <= 0:
                order += 1

            if deviation_corrected >= 45 and stock <= package_size:
                order +=1

            if stock <= math.ceil(package_size/4):
                order += 1

            if order < expected_packages/2:
                order =  math.ceil((order + expected_packages)/2)

            cap = math.ceil(req_stock/package_size)
            if deviation_corrected >= 20: 
                cap += 1
            if order > cap:
                order = cap
            
            return order, 1, "N_success"

        if req_stock >= 0.75*stock:
            if req_stock >= 1.5*package_size and stock <= package_size:
                order += 1
            elif req_stock > stock and deviation_corrected > 30:
                order += 1

            return order, 2, "N_success"
        
        if (stock - req_stock) <= max(package_size, req_stock):
            if deviation_corrected >= 30 and package_consumption >= 1:
                if stock < req_stock:
                    order += 1
                return order, 3, "N_success"    
            if deviation_corrected >= 35:
                return order, 3, "N_success"
                
        if trend < 0 and stock <= package_size and abs(trend) >= package_size*0.75: 
            return order, 4, "N_success"
        
        if current_gap <= -0.70*package_size and stock <= math.ceil(package_size / 3):
            return 1, 5, "N_success"
                
        if stock <= 4:
            return order, 6, "N_success"
        
    else:
        if stock <= 2:
            return order, 7, "N_success"
        
    if expected_packages >= 1 and deviation_corrected > -10 and stock <= math.ceil(package_size / 3):
        return 1, 8, "N_success"
    
    if turnover >= 0.9 and current_gap <= 0 and (stock <= package_size or expected_packages >= 1):
            return 1, 9, "N_success"
    
    return None, 0, "N_fail"

    