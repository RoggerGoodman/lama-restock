import math

def process_C_sales(stock_oscillation, package_size, deviation_corrected, expected_packages, trend, current_gap, turnover):

    if package_size <= 8:
        if deviation_corrected >= 20 and expected_packages > 0.7:
            if stock_oscillation <= math.ceil(package_size * 0.4):
                return 1, 1, "C_success"
        if deviation_corrected >= 0 and expected_packages > 0.85:
            if stock_oscillation < math.ceil(package_size * 0.3):
                return 1, 2, "C_success"
        if deviation_corrected > -10 and expected_packages >= 1:
            if stock_oscillation <= math.ceil(package_size * 0.2):
                return 1, 3, "C_success"
        if deviation_corrected >= 50:
            if stock_oscillation <= math.ceil(package_size * 0.4):
                return 1, 4, "C_success"
        if trend < 0:
            abs_trend = abs(trend)
            if abs_trend >= math.floor(package_size * 0.8) and stock_oscillation <= math.floor(package_size * 0.7):
                return 1, 5, "C_success"
            if abs_trend >= package_size and stock_oscillation <= package_size:
                return 1, 6, "C_success"
        if current_gap < 0:
            abs_gap = abs(current_gap)
            if abs_gap >= math.floor(package_size * 0.6) and stock_oscillation <= math.floor(package_size * 0.6):
                return 1, 7, "C_success"
            if abs_gap >= math.floor(package_size * 0.8) and stock_oscillation <= package_size:
                return 1, 8, "C_success"

    elif package_size < 18:
        if deviation_corrected >= 20 and expected_packages > 0.7:
            if stock_oscillation <= math.ceil(package_size * 0.3):
                return 1, 1, "C_success"
        if deviation_corrected >= 0 and expected_packages > 0.85:
            if stock_oscillation <= math.ceil(package_size * 0.2):
                return 1, 2, "C_success"
        if deviation_corrected > -10 and expected_packages >= 1:
            if stock_oscillation <= math.ceil(package_size * 0.1):
                return 1, 3, "C_success"
        if deviation_corrected >= 50:
            if stock_oscillation <= math.ceil(package_size * 0.3):
                return 1, 4, "C_success"
        if trend < 0:
            abs_trend = abs(trend)
            if abs_trend >= math.floor(package_size * 0.7) and stock_oscillation <= math.floor(package_size * 0.5):
                return 1, 5, "C_success"
            if abs_trend >= math.floor(package_size * 0.9) and stock_oscillation <= math.floor(package_size * 0.7):
                return 1, 6, "C_success"
        if current_gap < 0:
            abs_gap = abs(current_gap)
            if abs_gap >= math.floor(package_size * 0.5) and stock_oscillation <= math.floor(package_size * 0.4):
                return 1, 7, "C_success"
            if abs_gap >= math.floor(package_size * 0.7) and stock_oscillation <= math.floor(package_size * 0.6):
                return 1, 8, "C_success"

    else:  # package_size >= 18
        if deviation_corrected >= 20 and expected_packages > 0.7:
            if stock_oscillation <= math.ceil(package_size * 0.2):
                return 1, 1, "C_success"
        if deviation_corrected >= 0 and expected_packages > 0.85:
            if stock_oscillation <= math.ceil(package_size * 0.1):
                return 1, 2, "C_success"
        if deviation_corrected > -10 and expected_packages >= 1:
            if stock_oscillation <= 0:
                return 1, 3, "C_success"
        if deviation_corrected >= 50:
            if stock_oscillation <= math.ceil(package_size * 0.2):
                return 1, 4, "C_success"
        if trend < 0:
            abs_trend = abs(trend)
            if abs_trend >= math.floor(package_size * 0.6) and stock_oscillation <= math.floor(package_size * 0.3):
                return 1, 5, "C_success"
            if abs_trend >= math.floor(package_size * 0.8) and stock_oscillation <= math.floor(package_size * 0.4):
                return 1, 6, "C_success"
        if current_gap < 0:
            abs_gap = abs(current_gap)
            if abs_gap >= math.floor(package_size * 0.4) and stock_oscillation <= math.floor(package_size * 0.2):
                return 1, 7, "C_success"
            if abs_gap >= math.floor(package_size * 0.6) and stock_oscillation <= math.floor(package_size * 0.3):
                return 1, 8, "C_success"


    if turnover >= 0.85 and current_gap <= 0:
        if stock_oscillation <= package_size and deviation_corrected > 10:
            return 1, 9, "C_success"
        if expected_packages >= 1:
            return 1, 9, "C_success"
    
    return None, 0, "C_fail"