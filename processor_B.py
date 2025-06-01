import math

def process_B_sales(stock_oscillation, package_size, deviation_corrected, req_stock, expected_packages, package_consumption):
    """
    Determines the processing outcome for a Category B product.

    Parameters:
        stock_oscillation (int): Oscillation of stock.
        package_size (int): Size of the package.
        restock_corrected (float): Corrected restock value.
        expected_packages (float): Expected number of packages to sell.
        use_stock (bool): Whether to use stock.
        stock (int): Current stock level.

    Returns:
        tuple: (result, reason, status)
    """
    order = 1

    if package_consumption > 0.8 and stock_oscillation < req_stock:
        if deviation_corrected > 40:
            order += 1
        return order, 1, "B_success"
    
    if package_consumption > 0.5 and deviation_corrected > 25 and stock_oscillation < req_stock:
        return order, 2, "B_success"

    if expected_packages >= 1 and deviation_corrected > -10: #and stock_oscillation <= package_size/2
        return order, 3, "B_success"

    if stock_oscillation <= math.ceil(-package_size / 4):
        return order, 4, "B_success"

    if package_size <= 8 and stock_oscillation <= 0 and deviation_corrected >= -20:
        return order, 5, "B_success"
    
    if deviation_corrected >= 50 and stock_oscillation <= math.floor(package_size/3):
        return 1, 6, "B_success"

    return None, 0, "B_fail"