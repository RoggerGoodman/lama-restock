import math
import pandas as pd
from analyzer import Analyzer
from helpers import Helper

analyzer = Analyzer()
helpers = Helper()

def process_category_b(category_b_df):
    
    # Iterate through the DataFrame rows
    for _, row in category_b_df.iterrows():
        # Extract necessary values from the row
        product_cod = row['product_cod']
        product_var = row['product_var']
        product_name = row['product_name']
        stock_oscillation = row['stock_oscillation']
        package_size = row['package_size']
        expected_packages = row['expected_packages']
        req_stock = row['req_stock']
        use_stock = row['use_stock']
        stock = row['stock']

        restock_corrected = req_stock
        restock_corrected -= stock_oscillation

        # Process the product based on conditions
        result, reason, status = process_B_sales(
            stock_oscillation,
            package_size,
            restock_corrected,
            expected_packages,
            use_stock,
            stock
        )

        if result:
            # Log the restock action
            analyzer.note_recorder(f"Article {product_name}, with code {product_cod}.{product_var}")
            analyzer.stat_recorder(result, status)
            helpers.order_this(product_cod, product_var, result, product_name, reason)
        else:
            # Log that no action was taken
            analyzer.stat_recorder(0, status)
            helpers.next_article(product_cod, product_var, package_size, product_name, reason)

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
        return 1, "B1", "B_success"

    if restock_corrected > package_size:
        return 1, "B2", "B_success"

    if expected_packages >= 1 and stock_oscillation <= 0:
        return 1, "B3", "B_success"

    if expected_packages >= 0.5 and stock_oscillation <= math.ceil(-package_size / 3):
        return 1, "B4", "B_success"

    if stock_oscillation <= math.floor(-package_size / 3):
        return 1, "B5", "B_success"

    if package_size <= 8 and stock_oscillation <= 0:
        return 1, "B6", "B_success"

    return None, "B0", "B_fail"