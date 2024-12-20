import math
import pandas as pd
from analyzer import Analyzer
from helpers import Helper

analyzer = Analyzer()
helpers = Helper()

def process_category_c(category_c_df):

    # Iterate through the DataFrame rows
    for _, row in category_c_df.iterrows():
        # Extract necessary values from the row
        product_cod = row['product_cod']
        product_var = row['product_var']
        product_name = row['product_name']
        stock_oscillation = row['stock_oscillation']
        package_size = row['package_size']
        deviation_corrected = row['deviation_corrected']
        req_stock = row['req_stock']
        use_stock = row['use_stock']
        stock = row['stock']

        restock = req_stock

        if stock_oscillation > 0:
            restock -= stock_oscillation

        # Process the product based on conditions
        result, reason, status = process_C_sales(
            stock_oscillation,
            package_size,
            restock,
            deviation_corrected,
            use_stock,
            stock
        )

        if result:
            # Log the restock action
            analyzer.note_recorder(f"Article {product_name}, with code {product_cod}.{product_var}")
            analyzer.stat_recorder(restock, status)
            helpers.order_this(product_cod, product_var, restock, product_name, reason)
        else:
            # Log that no action was taken
            analyzer.stat_recorder(0, status)
            helpers.next_article(product_cod, product_var, package_size, product_name, reason)

def process_C_sales(stock_oscillation, package_size, restock, deviation_corrected, use_stock, stock):
    """
    Determines the processing outcome for a Category C product.

    Parameters:
        stock_oscillation (int): Oscillation of stock.
        package_size (int): Size of the package.
        restock (float): Number of packages to restock.
        deviation_corrected (float): Corrected deviation value.
        use_stock (bool): Whether to use stock.
        stock (int): Current stock level.

    Returns:
        tuple: (result, reason, status)
    """
    if use_stock and stock <= math.floor(package_size / 2):
        return 1, "C1", "C_success"

    if stock_oscillation <= math.floor(-package_size / 3):
        return 1, "C2", "C_success"

    if package_size <= 8 and stock_oscillation < math.ceil(-package_size / 4):
        return 1, "C3", "C_success"

    if restock >= 1.8 and deviation_corrected >= 10 and stock_oscillation < 0:
        return 1, "C4", "C_success"

    return None, "C0", "C_fail"