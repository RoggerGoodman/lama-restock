import math
import pandas as pd
from analyzer import analyzer
from helpers import Helper

order_list = []

def process_category_b(category_b_df, helper: Helper):
    
    # Iterate through the DataFrame rows
    for _, row in category_b_df.iterrows():
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

        restock_corrected = req_stock
        restock_corrected -= stock_oscillation

        # Process the product based on conditions
        result, check, status = process_B_sales(
            stock_oscillation,
            package_size,
            restock_corrected,
            expected_packages,
            use_stock,
            stock
        )

        category = "B"
        
        if result:
            # Log the restock action
            if avg_d_sales <= 0.2 or avg_d_sales*(1 + deviation_corrected / 100) <= 0.2:
                analyzer.note_recorder(product_name, product_cod, product_var)
            analyzer.stat_recorder(result, status)
            helper.order_this(order_list, product_cod, product_var, result, product_name, category, check)
            helper.line_breaker()
        else:
            # Log that no action was taken
            analyzer.stat_recorder(0, status)
            helper.order_denied(product_cod, product_var, package_size, product_name, category, check)
            helper.line_breaker()

    return order_list

def process_B_sales(stock_oscillation, package_size, restock_corrected, expected_packages, use_stock, stock):
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
    if use_stock and stock <= math.floor(package_size / 2):
        return 1, 1, "B_success"

    if restock_corrected > package_size:
        return 1, 2, "B_success"

    if expected_packages >= 1 and stock_oscillation <= 0: 
        return 1, 3, "B_success"

    if expected_packages >= 0.5 and stock_oscillation <= math.ceil(-package_size / 3):
        return 1, 4, "B_success"

    if stock_oscillation <= math.floor(-package_size / 3):
        return 1, 5, "B_success"

    if package_size <= 8 and stock_oscillation <= 0:
        return 1, 6, "B_success"

    return None, 0, "B_fail"