# LamApp/supermarkets/scripts/decision_maker.py
from .DatabaseManager import DatabaseManager
import json
from datetime import datetime, date
from .helpers import Helper
from .logger import logger
from .analyzer import analyzer
from .processor_N import process_N_sales


class DecisionMaker:
    def __init__(self, db: DatabaseManager, helper: Helper, blacklist_set=None):
        """
        Initialize decision maker with PostgreSQL support.
        """
        self.helper = helper        
        self.conn = db.conn
        self.cursor = db.cursor()

        self.orders_list = []
        
        # NEW: Three separate tracking lists
        self.new_products = []      # Brand new products never in system
        self.skipped_products = []  # Products skipped for various reasons
        self.zombie_products = []   # Products that are finished/not restockable
        
        self.sale_discounts = self.retrive_products_on_sale()
        
        # Store blacklist - if None, create empty set
        self.blacklist = blacklist_set if blacklist_set is not None else set()
        
        logger.info(f"DecisionMaker initialized with {len(self.blacklist)} blacklisted products")

    def get_products_by_settore(self, settore):
        """
        Retrieve all products (and their stats) for a given settore.
        Returns a list of dict-like rows.
        """
        query = """
            SELECT p.cod, p.v, p.descrizione, ps.stock, ps.sold_last_24, ps.bought_last_24, ps.sales_sets,
                p.pz_x_collo, p.rapp, ps.verified, p.disponibilita
            FROM products p
            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.settore = %s
        """
        self.cursor.execute(query, (settore,))
        return self.cursor.fetchall()
    
    def get_internal_use_losess(self):
        """Return list of (cod, v, internal, internal_updated) from extra_losses
        where internal is valid JSON and internal_updated is not null."""
        
        query = """
            SELECT cod, v, internal, internal_updated
            FROM extra_losses
            WHERE internal IS NOT NULL
            AND internal_updated IS NOT NULL;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # Convert JSON strings into Python objects
        extra_losses = []
        for row in rows:
            extra_losses.append({
                "cod": row["cod"],
                "v": row["v"],
                "internal": row["internal"] or [],
                "internal_updated": row["internal_updated"]
            })
        
        return extra_losses
    
    def integrate_internal_losses(self, cod, v, sold_array, extra_losses):
        """If cod,v in results: pad internal with zeros for months since last update
        and return elementwise sum with sold_array."""
        
        # Find the matching entry
        match = next((r for r in extra_losses if r["cod"] == cod and r["v"] == v), None)
    
        # Compute months passed since internal_updated
        updated_date:date = match["internal_updated"]
        today = date.today()
        months_passed = (today.year - updated_date.year) * 12 + (today.month - updated_date.month)

        # Pad internal list with zeros
        internal = match["internal"].copy()
        for _ in range(months_passed):
            internal.insert(0, 0)

        summed = [
            (internal[i] if i < len(internal) else 0) + sold_array[i]
            for i in range(len(sold_array))
        ]

        return summed
    
    def retrive_products_on_sale(self):
        today = date.today() 

        self.cursor.execute("""
            SELECT cod, v, price_std, price_s
            FROM economics
            WHERE sale_start IS NOT NULL
            AND sale_end IS NOT NULL
            AND %s BETWEEN sale_start AND sale_end;
        """, (today,))

        rows = self.cursor.fetchall()
        sale_discounts = {}

        for row in rows:
            cod = row["cod"]
            v = row["v"]

            price_std = row["price_std"]
            price_s = row["price_s"]

            if price_std is None or price_s is None:
                continue
            # avoid division errors
            if price_std > price_s:
                discount_pct = round((price_std - price_s) / price_std * 100, 2)
            else:
                discount_pct = 10

            sale_discounts[(cod, v)] = discount_pct

        return sale_discounts

    def get_discount_for(self, cod, v):
        return self.sale_discounts.get((cod, v))  

    def decide_orders_for_settore(self, settore, coverage):
        """
        Main method — iterate over all products in a settore and decide what to order.
        Now tracks THREE lists: new_products, skipped_products, zombie_products
        """
        logger.info(f"Processing settore: {settore} with coverage: {coverage} days")
        logger.info(f"Active blacklist has {len(self.blacklist)} products")
        
        products = self.get_products_by_settore(settore)
        logger.info(f"Found {len(products)} products in settore '{settore}'")
        
        extra_losses_list = self.get_internal_use_losess()
        extra_losses_lookup = {(item["cod"], item["v"]) for item in extra_losses_list}

        order_list = []
        new_products = []  
        skipped_products = []
        zombie_products = []

        for row in products:
            product_cod = row["cod"]
            product_var = row["v"]
            
            # CHECK BLACKLIST FIRST!
            if (product_cod, product_var) in self.blacklist:
                logger.info(f"Skipping blacklisted product: {product_cod}.{product_var}")
                continue
            
            descrizione = row["descrizione"]
            stock = row["stock"]
            sold_array = row["sold_last_24"] or []
            bought_array = row["bought_last_24"] or []
            sales_sets = row["sales_sets"] or []
            package_size = row["pz_x_collo"]
            package_multi = row["rapp"]
            verified = row["verified"]
            disponibilita = row["disponibilita"]

            logger.info(f"Processing {product_cod}.{product_var} - {descrizione} (stock={stock})")
            
            if verified == False and disponibilita == "No":
                logger.info(f"{product_cod}.{product_var} - {descrizione} skipped because is not verified and not available")
                continue

            if stock == 0 and verified == True and disponibilita == "No":
                logger.info(f"{product_cod}.{product_var} - {descrizione} marked as zombie because is not available and has verified stock of 0")
                zombie_products.append({
                    'cod': product_cod,
                    'var': product_var,
                    'reason': 'Finished and not restockable (disponibilita=No, stock=0)'
                })
                continue
                        
            package_size *= package_multi

            if stock == None:
                logger.info(f"Skipping Article: {product_cod}.{product_var}. Because has no registered stock")
                continue
            
            if stock < 0 and verified == True:
                analyzer.anomalous_stock_recorder(f"Article {descrizione}, with code {product_cod}.{product_var}")
            
            if len(bought_array) == 0 and len(sold_array) == 0:
                if disponibilita == "Si":
                    reason = "Never been in system (brand new product)"
                    analyzer.brand_new_recorder(f"Article {descrizione}, with code {product_cod}.{product_var}")
                    self.helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    self.helper.line_breaker()
                    new_products.append({
                        'cod': product_cod,
                        'var': product_var,
                        'reason': reason
                    })
                    continue
                elif disponibilita == "No":
                    reason = "Not available for restocking and no sales history"
                    self.helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    self.helper.line_breaker()
                    continue
            
            if (product_cod, product_var) in extra_losses_lookup:
                sold_array = self.integrate_internal_losses(product_cod, product_var, sold_array, extra_losses_list)

            avg_daily_sales = self.helper.avg_daily_sales_from_sales_sets(sales_sets)
            avg_sales_base = avg_daily_sales
            if avg_daily_sales == 0: 
                avg_daily_sales, avg_sales_base = self.helper.calculate_weighted_avg_sales_new(sold_array)

            if len(sold_array) >= 4:                    
                recent_months_sales = self.helper.calculate_data_recent_months(sold_array, 3)
                deviation_corrected = self.helper.calculate_deviation(sold_array, recent_months_sales, True)               
                trend = self.helper.find_trend(sold_array, bought_array)
            else:
                recent_months_sales = -1
                deviation_corrected = 0
                trend = 0
                logger.info(f"Deviation and recent months sales are not available for this article") 
            
            if len(sold_array) >= 16:
                ly_slice = sold_array[12:]
                ly_recent_months_sales = self.helper.calculate_data_recent_months(ly_slice, 3)
                ly_deviation = self.helper.calculate_deviation(ly_slice, ly_recent_months_sales, False)
                deviation_corrected = self.helper.deviation_blender(deviation_corrected, ly_deviation)
                logger.info(f"Deviation Blended = {deviation_corrected} %")
            elif len(sold_array) >= 4:
                logger.info(f"Deviation = {deviation_corrected} %")

            req_stock = avg_daily_sales * coverage
            logger.info(f"Required stock = {req_stock:.2f}")

            package_consumption = req_stock / package_size 
            logger.info(f"Package consumption = {package_consumption:.2f}")

            discount = self.get_discount_for(product_cod, product_var)

            if discount is not None:
                if discount == 0:
                    discount = 15
                    logger.info(f"This product is currently on sale: {discount}% with default value")
                else:
                    logger.info(f"This product is currently on sale: {discount}%")

                req_stock += (req_stock * discount/100)

            if verified == True:
                category = "N"
                result, check, status = process_N_sales(
                    package_size, deviation_corrected, avg_daily_sales, 
                    avg_sales_base, req_stock, stock
                )
            else:
                reason = "Not verified in system"
                self.helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                self.helper.line_breaker()
                continue

            if result:
                if avg_daily_sales <= 0.2:
                    analyzer.low_sale_recorder(descrizione, product_cod, product_var)
                analyzer.stat_recorder(result, status)
                self.helper.order_this(order_list, product_cod, product_var, result, descrizione, category, check)
                self.helper.line_breaker()
            else:
                analyzer.stat_recorder(0, status)
                self.helper.order_denied(product_cod, product_var, package_size, descrizione, category, check)
                self.helper.line_breaker()

        analyzer.log_statistics()
        
        # Store all three lists
        self.orders_list = order_list
        self.new_products = new_products 
        self.skipped_products = skipped_products
        self.zombie_products = zombie_products
        
        logger.info(f"Finished settore '{settore}':")
        logger.info(f"  - Orders: {len(order_list)}")
        logger.info(f"  - New products: {len(new_products)}")
        logger.info(f"  - Skipped products: {len(skipped_products)}")
        logger.info(f"  - Zombie products: {len(zombie_products)}")

    def close(self):
        """Cleanly close the database connection."""
        self.conn.close()