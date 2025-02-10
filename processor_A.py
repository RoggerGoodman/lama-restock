import math
import pandas as pd
from analyzer import analyzer
from helpers import Helper

order_list = []

def process_category_a(category_a_df, helper: Helper):

    # Iterate through the DataFrame rows
    for _, row in category_a_df.iterrows():
        # Extract necessary values from the row
        product_cod = row['product_cod']
        product_var = row['product_var']
        product_name = row['product_name']
        stock_oscillation = row['stock_oscillation']
        package_size = row['package_size']
        deviation_corrected = row['deviation_corrected']
        expected_packages = row['expected_packages']
        req_stock = row['req_stock']  
        use_stock = row['use_stock']
        stock = row['stock']
        avg_d_sales = row['avg_d_sales']

        restock = req_stock
        

        if stock_oscillation > 0:
            restock -= stock_oscillation

        # Process the product based on conditions
        result, check, status = process_A_sales(
            stock_oscillation,
            package_size,
            deviation_corrected,
            restock,
            expected_packages,
            req_stock,
            use_stock,
            stock,
            helper
        )

        category = "A"

        if result:
            # Log the restock action
            if avg_d_sales <= 0.2 or avg_d_sales*(1 + deviation_corrected / 100) <= 0.2:
                    analyzer.note_recorder(f"Article {product_name}, with code {product_cod}.{product_var}")
            analyzer.stat_recorder(result, status)
            helper.order_this(order_list, product_cod, product_var, result, product_name, category, check)
            helper.line_breaker()
        else:
            # Log that no action was taken
            analyzer.stat_recorder(0, status)
            helper.order_denied(product_cod, product_var, package_size, product_name, category, check)
            helper.line_breaker()
    
    return order_list

def process_A_sales(stock_oscillation, package_size, deviation_corrected, restock, expected_packages, req_stock, use_stock, stock, helper):
    """
    Determines the processing outcome for a Category A product.

    Parameters:
        stock_oscillation (int): Oscillation of stock.
        package_size (int): Size of the package.
        deviation_corrected (float): Corrected deviation value.
        restock (float): Restock value (to be computed if missing).
        expected_packages (float): Expected number of packages to sell.
        req_stock (float): Required stock level.
        use_stock (bool): Whether to use stock.
        stock (int): Current stock level.

    Returns:
        tuple: (result, reason, status)
    """
    if restock >= package_size:
        restock = helper.custom_round(restock / package_size, 0.7)
        if stock_oscillation <= -package_size:
            restock += 1
        return restock, 1, "A_success"

    if use_stock and stock <= math.floor(package_size / 2):
        return 1, 2, "A_success"

    if restock > math.ceil(package_size / 2) and (deviation_corrected > 20 or stock_oscillation <= math.floor(-package_size / 3)):
        order = 2 if stock_oscillation <= -package_size else 1
        return order, 3, "A_success"

    if stock_oscillation <= math.floor(-package_size / 2):
        return 1, 4, "A_success"

    if expected_packages >= 1 and stock_oscillation < package_size / 2:
        return 1, 5, "A_success"

    if package_size >= 20 and restock >= math.ceil(package_size / 4):
        return 1, 6, "A_success"

    if stock_oscillation <= math.ceil(req_stock / 2) and expected_packages > 0.3:
        return 1, 7, "A_success"

    return None, 0, "A_fail"
