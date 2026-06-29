# LamApp/supermarkets/scripts/decision_maker.py
import logging
from .DatabaseManager import DatabaseManager
from datetime import date, timedelta
from math import ceil
from .helpers import Helper
from .analyzer import analyzer
from .processor_N import process_N_sales

# Use Django's logging system - configured in settings.py
# This logger will write to decision_maker.log (separate from other logs due to high volume)
logger = logging.getLogger(__name__)


class DecisionMaker:
    def __init__(self, db: DatabaseManager, helper: Helper, blacklist_set=None, skip_sale: bool = False,
                 secondary_products=None, dominant_to_secondary=None):
        """
        Initialize decision maker with PostgreSQL support.

        secondary_products:      set of (cod, v) that are the phased-out side of a ProductLink.
                                 These are skipped for ordering; their stats are merged into the dominant.
        dominant_to_secondary:   dict {(dom_cod, dom_v): (sec_cod, sec_v)} for stat merging.
        """
        self.helper = helper
        self.conn = db.conn
        self.db = db
        self.cursor = db.cursor()
        self.skip_sale = skip_sale
        self.orders_list = []

        self.zombie_products = []   # Products that are finished/not restockable

        self.sale_discounts = self.retrieve_products_on_sale()
        self.sale_discounts_ended = self.retrieve_products_recently_ended_sale()

        # Store blacklist - if None, create empty set
        self.blacklist = blacklist_set if blacklist_set is not None else set()

        # Product link lookups (global, chain-level)
        self.secondary_products = secondary_products if secondary_products is not None else set()
        self.dominant_to_secondary = dominant_to_secondary if dominant_to_secondary is not None else {}

        logger.info(f"DecisionMaker initialized with {len(self.blacklist)} blacklisted products")

    def get_products_by_settore(self, settore):
        """
        Retrieve all products (and their stats) for a given settore.
        """
        query = """
            SELECT p.cod, p.v, p.descrizione, ps.stock, ps.sold_last_24, ps.bought_last_24, ps.sales_sets,
                ps.bought_sets, p.pz_x_collo, p.rapp, ps.verified, p.disponibilita, p.purge_flag,
                ps.minimum_stock, p.shelf_life_days
            FROM products p
            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.settore = %s
        """
        self.cursor.execute(query, (settore,))
        return self.cursor.fetchall()
    
    def get_extra_losses(self):
        """
        Single-query fetch of extra_losses.
        Returns (internal_list, expired_dict):
          internal_list: list of {cod, v, internal} for products with internal losses
          expired_dict:  {(cod, v): expired_array} for products with expired losses
        """
        self.cursor.execute("""
            SELECT cod, v, internal, expired
            FROM extra_losses
            WHERE internal IS NOT NULL OR expired IS NOT NULL;
        """)
        rows = self.cursor.fetchall()

        internal_list = []
        expired_dict = {}
        for row in rows:
            if row["internal"] is not None:
                internal_list.append({
                    "cod": row["cod"],
                    "v": row["v"],
                    "internal": row["internal"] or [],
                })
            if row["expired"] is not None:
                expired_dict[(row["cod"], row["v"])] = row["expired"]

        return internal_list, expired_dict

    def integrate_internal_losses(self, cod, v, sold_array, extra_losses):
        """Element-wise sum of internal losses with sold_array.
        Arrays are kept aligned by the monthly zero-prepend task."""

        match = next((r for r in extra_losses if r["cod"] == cod and r["v"] == v), None)
        internal = match["internal"]

        summed = []
        for i in range(len(sold_array)):
            internal_value = internal[i] if i < len(internal) else 0

            # Handle both old format (int) and new format ([qty, cost])
            if isinstance(internal_value, list) and len(internal_value) == 2:
                internal_qty = internal_value[0]
            else:
                internal_qty = internal_value

            summed.append(internal_qty + sold_array[i])

        return summed
    
    def retrieve_products_on_sale(self, days_ahead=0):
        today = date.today()
        future = today + timedelta(days=int(days_ahead))

        self.cursor.execute("""
            SELECT cod, v, price_std, price_s, sale_start, sale_end
            FROM economics
            WHERE sale_start IS NOT NULL
            AND sale_end IS NOT NULL
            AND sale_end >= %s
            AND sale_start <= %s;
        """, (today, future))

        rows = self.cursor.fetchall()
        sale_discounts = {}

        for row in rows:
            cod = row["cod"]
            v = row["v"]

            price_std = row["price_std"]
            price_s = row["price_s"]
            sale_start = row["sale_start"]
            sale_end = row["sale_end"]

            if price_std is None or price_s is None:
                continue

            if price_std > price_s:
                discount_pct = round((price_std - price_s) / price_std * 100, 2)
            else:
                discount_pct = 10  # fallback

            sale_discounts[(cod, v)] = {
                "discount": discount_pct,
                "sale_start": sale_start,
                "sale_end": sale_end,
            }

        return sale_discounts
    
    def get_discount_for(self, cod, v):
        return self.sale_discounts.get((cod, v))
    
    def retrieve_products_recently_ended_sale(self):
        today = date.today()

        self.cursor.execute("""
            SELECT cod, v, sale_start, sale_end
            FROM economics
            WHERE sale_start IS NOT NULL
            AND sale_end IS NOT NULL
            AND %s > sale_end
            AND (%s - sale_end) <= 14;
        """, (today, today))

        rows = self.cursor.fetchall()
        sale_discounts_ended = {}

        for row in rows:
            cod = row["cod"]
            v = row["v"]

            sale_start = row["sale_start"]
            sale_end = row["sale_end"]

            # Duration of the sale
            days_lasted = (sale_end - sale_start).days

            # Days passed since sale ended
            days_since_the_end = (today - sale_end).days

            sale_discounts_ended[(cod, v)] = {
                "days_lasted": days_lasted,
                "days_since_the_end": days_since_the_end,
            }

        return sale_discounts_ended

    def get_ended_discount_for(self, cod, v):
        return self.sale_discounts_ended.get((cod, v))

    def is_in_first_60_percent(self, today, sale_start, sale_end):
        total_days = (sale_end - sale_start).days + 1
        threshold_day = ceil(total_days * 0.6)

        threshold_date = sale_start + timedelta(days=threshold_day - 1)
        return today <= threshold_date

    def decide_orders_for_settore(self, settore, coverage, minimum_stock_base=None):
        """
        Main method — iterate over all products in a settore and decide what to order.
        Now tracks zombie_products.
        """
        logger.info(f"Processing settore: {settore} with coverage: {coverage} days")
        logger.info(f"Active blacklist has {len(self.blacklist)} products")
        
        products = self.get_products_by_settore(settore)
        logger.info(f"Found {len(products)} products in settore '{settore}'")
        
        extra_losses_list, expired_lookup = self.get_extra_losses()
        extra_losses_lookup = {(item["cod"], item["v"]) for item in extra_losses_list}

        self.sale_discounts = self.retrieve_products_on_sale(coverage)
        logger.info(f"Products on sale (including upcoming within {coverage} days): {len(self.sale_discounts)}")

        order_list = []
        zombie_products = []

        for row in products:
            product_cod = row["cod"]
            product_var = row["v"]
            
            # CHECK BLACKLIST
            if (product_cod, product_var) in self.blacklist:
                logger.info(f"Skipping blacklisted product: {product_cod}.{product_var}")
                continue

            product_flag = row["purge_flag"]

            # CHECK Purge
            if product_flag:
                logger.info(f"Skipping purging product: {product_cod}.{product_var}")
                continue

            # CHECK PRODUCT LINK — skip secondaries; merge into dominant later
            if (product_cod, product_var) in self.secondary_products:
                logger.info(f"Skipping secondary linked product: {product_cod}.{product_var} (handled by dominant)")
                continue

            descrizione = row["descrizione"]
            stock = row["stock"]

            if stock is None:
                logger.info(f"Skipping Article: {product_cod}.{product_var}. Because has no registered stock")
                continue

            stock = max(0, stock)
            sold_array = row["sold_last_24"] or []
            bought_array = row["bought_last_24"] or []
            sales_sets = row["sales_sets"] or []
            bought_sets = row["bought_sets"] or []

            # PRODUCT LINK — merge secondary's sales_sets and stock into this dominant
            linked_secondary = self.dominant_to_secondary.get((product_cod, product_var))
            if linked_secondary is not None:
                sec_stats = self.db.get_linked_product_stats(linked_secondary[0], linked_secondary[1])
                if sec_stats is not None:
                    sales_sets = Helper.merge_sales_sets(sales_sets, sec_stats["sales_sets"])
                    stock = stock + max(0, sec_stats["stock"])
                    logger.info(
                        f"Merged linked secondary {linked_secondary[0]}.{linked_secondary[1]} "
                        f"into dominant {product_cod}.{product_var}: "
                        f"stock+={sec_stats['stock']}"
                    )
            package_size = row["pz_x_collo"]
            package_multi = row["rapp"]
            verified = row["verified"]
            disponibilita = row["disponibilita"]
            minimum_stock_override = row.get("minimum_stock", None)
            shelf_life_days = row.get("shelf_life_days", None)

            logger.info(f"Processing {product_cod}.{product_var} - {descrizione} (stock={stock})")

            if not verified and disponibilita == "No":
                logger.info(f"{product_cod}.{product_var} - {descrizione} skipped because is not verified and not available")
                continue

            if stock == 0 and verified and disponibilita == "No" and settore != "DEPERIBILI":
                logger.info(f"{product_cod}.{product_var} - {descrizione} marked as zombie because is not available and has verified stock of 0")
                zombie_products.append({
                    'cod': product_cod,
                    'var': product_var,
                    'reason': 'Finished and not restockable (disponibilita=No, stock=0)'
                })
                continue

            package_size *= package_multi
            
            if stock < 0 and verified:
                analyzer.anomalous_stock_recorder(f"Article {descrizione}, with code {product_cod}.{product_var}")
            
            if bought_array[0] == 0 and sold_array[0] == 0:
                if not verified and (disponibilita == "Si" or settore == "DEPERIBILI"):
                    reason = "Never been in system (brand new product)"
                    Helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    continue
                elif disponibilita == "No":
                    reason = "Not available for restocking and no sales history"
                    Helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    continue

            if (product_cod, product_var) in extra_losses_lookup:
                sold_array = self.integrate_internal_losses(product_cod, product_var, sold_array, extra_losses_list)

            sale_end_info = self.get_ended_discount_for(product_cod, product_var)

            if sale_end_info is not None:
                days_lasted = sale_end_info["days_lasted"]
                days_since_the_end = sale_end_info["days_since_the_end"]
                sales_sets = sales_sets[:days_since_the_end] + sales_sets[days_since_the_end + days_lasted:]
                sale_info = None
            else :
                sale_info = self.get_discount_for(product_cod, product_var)

            avg_from_sets = Helper.avg_daily_sales_from_sales_sets(sales_sets)
            if avg_from_sets is not None:
                avg_daily_sales = avg_from_sets
            else:
                avg_daily_sales, _ = self.helper.calculate_weighted_avg_sales_new(sold_array)

            deviation_corrected = Helper.calculate_deviation(sales_sets)
            logger.info(f"Deviation = {deviation_corrected} %")

            req_stock = avg_daily_sales * coverage

            if avg_from_sets is not None:
                oos_window = sales_sets[:7]
                null_count = sum(1 for v in oos_window if v is None)
                if null_count > 0:
                    null_rate = null_count / len(oos_window)
                    correction = 1.5 if null_rate >= 1.0 else min(1.0 / (1.0 - null_rate), 1.5)
                    logger.warning(
                        f"OOS correction {product_cod}.{product_var} '{descrizione}': "
                        f"{null_count}/7 OOS days → ×{correction:.2f} "
                        f"(req_stock {req_stock:.2f} → {req_stock * correction:.2f})"
                    )
                    req_stock *= correction

            logger.info(f"Required stock = {req_stock:.2f}")

            package_consumption = req_stock / package_size 
            logger.info(f"Package consumption = {package_consumption:.2f}")

            if sale_info is not None:
                if self.skip_sale:
                    reason = "Skip products on sale mode is active for this order"
                    Helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    continue
                discount = sale_info["discount"]
                sale_start = sale_info["sale_start"]
                sale_end = sale_info["sale_end"]

                if discount == 0:
                    discount = 10

                today = date.today()
                if sale_start > today:
                    logger.info(f"Upcoming sale in {(sale_start - today).days} days: {discount}%")
                else:
                    logger.info(f"This product is currently on sale: {discount}%")

                if self.is_in_first_60_percent(today, sale_start, sale_end):
                    req_stock += req_stock * 0.10
                    logger.info("Stock buff applied")
                else:
                    logger.info("Sale buff NOT applied (late sale phase)")
            else:
                discount = None

            expiry_factor = None
            if (product_cod, product_var) in expired_lookup:
                expiry_factor = Helper.compute_expiry_factor(
                    expired_lookup[(product_cod, product_var)], sold_array
                )

            batch_expiry_factor = None
            if shelf_life_days is not None:
                batch_expiry_factor = Helper.compute_batch_expiry_factor(
                    bought_sets, sales_sets, shelf_life_days, avg_daily_sales
                )

            if verified:
                category = "N"
                result, check, status, returned_discount = process_N_sales(
                    package_size, deviation_corrected, avg_daily_sales,
                    req_stock, stock, discount, minimum_stock_base, minimum_stock_override,
                    expiry_factor, shelf_life_days, batch_expiry_factor
                )
            else:
                reason = "Not verified in system"
                Helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                continue

            if result:
                if avg_daily_sales <= 0.2:
                    analyzer.low_sale_recorder(descrizione, product_cod, product_var)
                analyzer.stat_recorder(result, status)
                Helper.order_this(order_list, product_cod, product_var, result, descrizione, category, check, returned_discount)
            else:
                analyzer.stat_recorder(0, status)
                self.helper.order_denied(product_cod, product_var, package_size, descrizione, category, check)

        analyzer.log_statistics()
        
        # Store lists
        self.orders_list = order_list
        self.zombie_products = zombie_products

        logger.info(f"Finished settore '{settore}':")
        logger.info(f"  - Orders: {len(order_list)}")
        logger.info(f"  - Zombie products: {len(zombie_products)}")

    def close(self):
        """Cleanly close the database connection."""
        self.conn.close()