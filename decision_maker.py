import sqlite3
import json
from datetime import datetime
from helpers import Helper

from blacklists import blacklists
from logger import logger
from analyzer import analyzer
from Tiers.processor_A import process_A_sales
from Tiers.processor_B import process_B_sales
from Tiers.processor_C import process_C_sales
from Tiers.processor_N import process_N_sales
from Tiers.processor_U import process_U_sales

class DecisionMaker:
    def __init__(self, helper: Helper, db_path=r"C:\Users\Ruggero\Documents\GitHub\lama-restock\Database\supermarket.db"):
        """
        Initialize the decision maker.

        Args:
            db_path (str): Path to your SQLite database.
            helper: Your Helper instance (for calculations, utilities, etc.).
        """
        self.db_path = db_path
        self.helper = helper
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        self.orders_list = []

    def get_products_by_settore(self, settore):
        """
        Retrieve all products (and their stats) for a given settore.
        Returns a list of dict-like rows.
        """
        query = """
            SELECT p.cod, p.v, p.descrizione, ps.stock, ps.sold_last_24, ps.bought_last_24, 
                p.pz_x_collo, p.rapp, ps.verified, p.disponibilita
            FROM products p
            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.settore = ?
        """
        self.cursor.execute(query, (settore,))
        return self.cursor.fetchall()

    def decide_orders_for_settore(self, settore, coverage):
        """
        Main method — iterate over all products in a settore and decide what to order.
        """
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing settore: {settore}")
        products = self.get_products_by_settore(settore)
        print(f"Found {len(products)} products in settore '{settore}'.")

        if settore in blacklists:
                blacklist_granular = blacklists[settore]["blacklist_granular"].copy()

        order_list = []  # store your order decisions here

        for row in products:
            product_cod = row["cod"]
            product_var = row["v"]
            descrizione = row["descrizione"]
            stock = row["stock"]
            sold_array = json.loads(row["sold_last_24"]) if row["sold_last_24"] else []
            bought_array = json.loads(row["bought_last_24"]) if row["bought_last_24"] else []
            package_size = row["pz_x_collo"]
            package_multi = row["rapp"]
            verified = row["verified"]
            disponibilita = row["disponibilita"]

            logger.info(f"Processing {product_cod}.{product_var} - {descrizione} (stock={stock})")

            if (product_cod, product_var) in blacklist_granular:
                    logger.info(f"Skipping blacklisted Cod Article and Var: {product_cod}.{product_var}")
                    blacklist_granular.remove((product_cod, product_var))  # Remove from runtime copy
                    continue  # Skip to the next iteration
                        
            package_size *= package_multi

            if len(bought_array) == 0 and disponibilita == "Si":
                    reason = "The prduct has never been in the system"
                    analyzer.brand_new_recorder(f"Article {descrizione}, with code {product_cod}.{product_var}")
                    self.helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    self.helper.line_breaker()
                    continue
                
            if len(bought_array)  <= 0:
                if disponibilita == "No":
                    reason = "The article is NOT available for restocking and hasn't been bought or sold for the last 3 months" 
                    self.helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    self.helper.line_breaker()
                    continue
                else :
                    reason = "The article is available once more for restocking but hasn't been bought or sold for the last 3 months"
                    analyzer.brand_new_recorder(f"Article {descrizione}, with code {product_cod}.{product_var}")
                    self.helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    self.helper.line_breaker()
                    continue

            avg_daily_sales = self.helper.calculate_weighted_avg_sales(sold_array)
            if avg_daily_sales == 0:
                reason = "No sales in recent months, no reason to continue"
                self.helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                self.helper.line_breaker()
                continue

            if len(sold_array) >= 4:                    
                recent_months_sales = self.helper.calculate_data_recent_months(sold_array, 3)
                expected_packages = self.helper.calculate_expectd_packages(bought_array, package_size)
                deviation_corrected = self.helper.calculate_deviation(sold_array, recent_months_sales, True)
                avg_daily_sales_corrected = avg_daily_sales * (1 + deviation_corrected / 100)                    
                trend = self.helper.find_trend(sold_array, bought_array)
                turnover = self.helper.calculate_turnover(sold_array, bought_array, package_size, trend)
            else:
                recent_months_sales = -1
                expected_packages = 0
                deviation_corrected = 0
                avg_daily_sales_corrected = avg_daily_sales
                trend = 0
                turnover = 0
                logger.info(f"Deviation and recent months sales are not available for this article") 
            
            current_gap = self.helper.find_current_gap(sold_array, bought_array)
            logger.info(f"Current gap is {current_gap}")

            if len(sold_array) >= 16:
                ly_slice = sold_array[12:]
                ly_recent_months_sales = self.helper.calculate_data_recent_months(ly_slice, 3)
                ly_deviation = self.helper.calculate_deviation(ly_slice, ly_recent_months_sales, False)
                deviation_corrected = self.helper.deviation_blender(deviation_corrected, ly_deviation)
                logger.info(f"Deviation Blended = {deviation_corrected} %")
            elif len(sold_array) >= 4:
                logger.info(f"Deviation = {deviation_corrected} %")

            req_stock = avg_daily_sales_corrected*coverage
            logger.info(f"Required stock = {req_stock:.2f}")

            package_consumption = req_stock / package_size 
            logger.info(f"Package consumption = {package_consumption:.2f}")

            real_need = req_stock
            if stock > 0 and verified == 0:
                    real_need -= stock

            if verified == 1:
                category = "N"
                result, check, status = process_N_sales(package_size, deviation_corrected, avg_daily_sales, avg_daily_sales_corrected, expected_packages, req_stock, stock, package_consumption, current_gap, trend, turnover, self.helper)
            elif package_consumption >= 1:
                category = "A"
                result, check, status = process_A_sales(stock, package_size, deviation_corrected, real_need, expected_packages, req_stock, current_gap, trend, turnover, self.helper)
            elif package_consumption >= 0.3:
                category = "B"
                result, check, status = process_B_sales(stock, package_size, deviation_corrected, req_stock, expected_packages, package_consumption, current_gap, trend, turnover)
            else :
                category = "C"
                result, check, status = process_C_sales(stock, package_size, deviation_corrected, expected_packages, trend, current_gap, turnover)

            if result:
            # Log the restock action
                if avg_daily_sales <= 0.2 or avg_daily_sales_corrected <= 0.2:
                    analyzer.low_sale_recorder(descrizione, product_cod, product_var)
                analyzer.stat_recorder(result, status)
                self.helper.order_this(order_list, product_cod, product_var, result, descrizione, category, check)
                self.helper.line_breaker()
            else:
                # Log that no action was taken
                analyzer.stat_recorder(0, status)
                self.helper.order_denied(product_cod, product_var, package_size, descrizione, category, check)
                self.helper.line_breaker()

        analyzer.log_statistics()
        self.orders_list = order_list
        print(f"Finished settore '{settore}'.")

    def close(self):
        """Cleanly close the database connection."""
        self.conn.close()