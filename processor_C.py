import math

def process_C_sales(stock_oscillation, package_size, deviation_corrected, expected_packages, trend, current_gap, turnover):

    if deviation_corrected >= 20 and expected_packages > 0.6:
        if stock_oscillation < 0 and package_size <= 8:
            return 1, 1, "C_success"
        
        if stock_oscillation <= math.ceil(-package_size / 4) and package_size < 18:
            return 1, 1, "C_success"
        
        if stock_oscillation <= math.ceil(-package_size / 3) and package_size >= 18:
            return 1, 1, "C_success"

    if  expected_packages > 0.8:     
        if stock_oscillation < 0 and package_size <= 8:
            return 1, 2, "C_success"

        if stock_oscillation <= math.ceil(-package_size / 4) and package_size < 18:
            return 1, 2, "C_success"
        
        if stock_oscillation <= math.ceil(-package_size / 3) and package_size >= 18:
            return 1, 2, "C_success"
    
    if expected_packages >= 1 and deviation_corrected > -10 and stock_oscillation <= math.ceil(package_size / 3):
        return 1, 3, "C_success"
    
    if deviation_corrected >= 50 and stock_oscillation <= 0:
        return 1, 4, "C_success"
    
    if trend < 0 and stock_oscillation <= package_size / 2 and abs(trend) >= package_size*0.75:
        return 1, 5, "C_success"
    
    if current_gap <= -0.70*package_size and stock_oscillation <= 0:
        return 1, 6, "C_success"

    if turnover >= 0.9 and stock_oscillation <= package_size and current_gap <= 0:
        return 1, 7, "C_success"
    
    return None, 0, "C_fail"