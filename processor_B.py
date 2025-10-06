import math

def process_B_sales(stock_oscillation, package_size, deviation_corrected, req_stock, expected_packages, package_consumption, current_gap, trend, turnover):
    
    order = 1
    req_stock = round(req_stock)

    if stock_oscillation <= req_stock:
        if package_consumption > 0.9:
            if deviation_corrected > 25 and stock_oscillation < math.ceil(package_size * 0.3):
                order += 1
            elif deviation_corrected > 45 and stock_oscillation <= package_size:
                order += 1
            return order, 1, "B_success"

        if package_consumption > 0.8:
            if deviation_corrected > 35 and stock_oscillation < package_size / 2:
                order += 1
            return order, 1, "B_success"

        if req_stock >= math.ceil(package_size*0.3):
            return order, 2, "B_success"
        
        if trend < 0:
            return order, 3, "B_success"
        
    if package_size <= 8:
        if stock_oscillation <= req_stock:
            if abs(current_gap) >= math.ceil(package_size * 0.8):
                return order, 4, "B_success"

        if deviation_corrected >= 25:
            if (stock_oscillation - req_stock) <= math.ceil(package_size * 0.4):
                return order, 5, "B_success"

        if deviation_corrected >= 0:
            if (stock_oscillation - req_stock) <= math.ceil(package_size * 0.3):
                return order, 6, "B_success"

        if current_gap < 0 and abs(current_gap) >= math.ceil(package_size * 0.8):
            if (stock_oscillation - req_stock) <= math.ceil(package_size * 0.3):
                return order, 7, "B_success"

        if trend < 0 and stock_oscillation <= package_size * 0.7 and abs(trend) >= math.ceil(package_size * 0.8):
            return 1, 8, "B_success"

    elif package_size < 18:
        if stock_oscillation <= req_stock:
            if abs(current_gap) >= math.ceil(package_size * 0.6):
                return order, 4, "B_success"

        if deviation_corrected >= 25:
            if (stock_oscillation - req_stock) <= math.ceil(package_size * 0.2):
                return order, 5, "B_success"

        if deviation_corrected >= 0:
            if (stock_oscillation - req_stock) <= math.ceil(package_size * 0.1):
                return order, 6, "B_success"

        if current_gap < 0 and abs(current_gap) >= math.ceil(package_size * 0.6):
            if (stock_oscillation - req_stock) <= math.ceil(package_size * 0.1):
                return order, 7, "B_success"

        if trend < 0 and stock_oscillation <= package_size * 0.6 and abs(trend) >= math.ceil(package_size * 0.7):
            return 1, 8, "B_success"

    else:  # package_size >= 18
        if stock_oscillation <= req_stock:
            if abs(current_gap) >= math.ceil(package_size * 0.4):
                return order, 4, "B_success"

        if deviation_corrected >= 25:
            if (stock_oscillation - req_stock) <= 0:
                return order, 5, "B_success"

        if deviation_corrected >= 0:
            if (stock_oscillation - req_stock) < 0:
                return order, 6, "B_success"

        if current_gap < 0 and abs(current_gap) >= math.ceil(package_size * 0.4):
            if (stock_oscillation - req_stock) < 0:
                return order, 7, "B_success"

        if trend < 0 and stock_oscillation <= package_size * 0.5 and abs(trend) >= math.ceil(package_size * 0.6):
            return 1, 8, "B_success"

    # Shared checks (not dependent on package_size category)
    if expected_packages >= 1 and deviation_corrected >= 0:
        return order, 9, "B_success"

    if turnover >= 0.8 and current_gap <= 0 and (stock_oscillation <= package_size or expected_packages >= 1):
        return 1, 10, "B_success"

    return None, 0, "B_fail"