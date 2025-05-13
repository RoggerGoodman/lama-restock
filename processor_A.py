import math
from helpers import Helper

def process_A_sales(stock_oscillation, package_size, deviation_corrected, real_need, expected_packages, req_stock, use_stock, stock, helper: Helper):
    """
    Determines the processing outcome for a Category A product.

    Parameters:
        stock_oscillation (int): Oscillation of stock.
        package_size (int): Size of the package.
        deviation_corrected (float): Corrected deviation value.
        real_need (float): The effective needed ammount (to be computed if missing).
        expected_packages (float): Expected number of packages to sell.
        req_stock (float): Required stock level.
        use_stock (bool): Whether to use stock.
        stock (int): Current stock level.

    Returns:
        tuple: (result, reason, status)
    """
    order = 1

    if real_need >= package_size:
        order = helper.custom_round(real_need / package_size, 0.6)

        if stock_oscillation <= -package_size:
            order += helper.custom_round(abs(stock_oscillation) / package_size, 0.8)

        if (order*package_size / real_need) < 1.2 and deviation_corrected > 20:
            order += 1

        if order < expected_packages/2:
           order =  math.ceil((order + expected_packages)/2)
           
        return order, 1, "A_success"

    if real_need > math.ceil(package_size / 2) and (deviation_corrected > 20 or stock_oscillation <= math.floor(-package_size / 3)):

        if stock_oscillation <= -package_size:
            order += 1

        if (order*package_size / real_need) < 1.2 and deviation_corrected > 20:
            order += 1
        
        return order, 2, "A_success"

    if stock_oscillation <= math.floor(-package_size / 2):
        return order, 3, "A_success"

    if expected_packages >= 1 and stock_oscillation < package_size / 2:
        return order, 4, "A_success"

    if package_size >= 20 and real_need >= math.ceil(package_size / 4):
        return order, 5, "A_success"

    if stock_oscillation <= math.ceil(req_stock / 2) and expected_packages > 0.3:
        return order, 6, "A_success"

    return None, 0, "A_fail"
