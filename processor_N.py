import math
from helpers import Helper

def process_N_sales(package_size, deviation_corrected, real_need, expected_packages, req_stock, stock, helper: Helper):

    order = 1

    if real_need >= package_size:
        order = helper.custom_round(real_need / package_size, 0.6)

        if stock <= -package_size:
            order += helper.custom_round(stock / package_size, 0.8)

        if (order*package_size / real_need) < 1.2 and deviation_corrected > 20:
            order += 1

        if order < expected_packages/2:
           order =  math.ceil((order + expected_packages)/2)
           
        return order, 1, "N_success"

    if req_stock >=  math.floor(package_size / 4) and stock <= math.floor(package_size / 2):
        return order, 2, "N_success"

    if req_stock >= 0.75*stock:
        return order, 3, "N_success"
    
    if stock <= 3:
        return order, 4, "N_success"

    return None, 0, "N_fail"

    