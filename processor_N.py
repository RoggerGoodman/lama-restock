import math
from helpers import Helper

def process_N_sales(package_size, deviation_corrected, real_need, expected_packages, req_stock, stock, package_consumption, current_gap, trend, turnover, helper: Helper):

    order = 1

    if real_need >= package_size or req_stock >= package_size*3:

        order = helper.custom_round(real_need / package_size, 0.6)

        if deviation_corrected >= 45 and stock <= package_size:
            order +=1

        if stock <= -package_size:
            order += helper.custom_round(abs(stock) / package_size, 0.8)

        if (order*package_size / real_need) < 1.2 and deviation_corrected > 20:
            order += 1
        elif current_gap <= -package_size and stock <= 0:
            order += 1

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
    
    if package_consumption >= 1 and deviation_corrected >= 25:
        if stock < req_stock:
            order += 1
            
        if (stock - req_stock) <= math.floor(package_size*0.7) and package_size <= 8:
                return order, 2, "N_success"            
        elif (stock - req_stock) <= math.floor(package_size*0.5) and package_size < 18:
                return order, 2, "N_success"            
        elif (stock - req_stock) <= math.floor(package_size*0.3) and package_size >= 18:
                return order, 2, "N_success"
        
        order = 1
                
    if package_size <= 8:

        if req_stock >= 0.75 * stock:
            if req_stock >= 1.6 * package_size and stock <= package_size:
                order += 1
            elif req_stock > stock and deviation_corrected > 30:
                order += 1
            return order, 3, "N_success"

        if trend < 0 and stock <= package_size and abs(trend) >= math.ceil(package_size * 0.8): 
            return 1, 4, "N_success"            

        if current_gap < 0 and abs(current_gap) >= math.ceil(package_size * 0.7) and stock <= math.ceil(package_size * 0.35):
            return 1, 5, "N_success"

    elif package_size < 18:

        if req_stock >= 0.75 * stock:
            if req_stock >= 1.5 * package_size and stock <= math.ceil(package_size * 0.8):
                order += 1
            elif req_stock > stock and deviation_corrected > 30:
                order += 1
            return order, 3, "N_success"

        if trend < 0 and stock <= math.ceil(package_size * 0.8) and abs(trend) >= math.ceil(package_size * 0.75): 
            return 1, 4, "N_success"

        if current_gap < 0 and abs(current_gap) >= math.ceil(package_size * 0.65) and stock <= math.ceil(package_size * 0.3):
            return 1, 5, "N_success"

    else:  # package_size >= 18

        if req_stock >= 0.75 * stock:
            if req_stock >= 1.4 * package_size and stock <= math.ceil(package_size * 0.6):
                order += 1
            elif req_stock > stock and deviation_corrected > 30:
                order += 1
            return order, 3, "N_success"

        if trend < 0 and stock <= math.ceil(package_size * 0.6) and abs(trend) >= math.ceil(package_size * 0.7): 
            return 1, 4, "N_success"

        if current_gap < 0 and abs(current_gap) >= math.ceil(package_size * 0.6) and stock <= math.ceil(package_size * 0.25):
            return 1, 5, "N_success"
                
    if expected_packages >= 1 and deviation_corrected > -10 and stock <= 4:
        return 1, 6, "N_success"
    
    if turnover >= 0.9 and current_gap <= 0 and (stock <= package_size or expected_packages >= 1):
        return 1, 7, "N_success"
    
    if package_consumption >= 1:        
        if stock <= 4:
            return 1, 8, "N_success"
    if package_consumption >= 0.3:
        if stock <= 3:
            return 1, 8, "N_success"
    else:
        if stock <= 2:
            return 1, 8, "N_success"
    
    return None, 0, "N_fail"

    