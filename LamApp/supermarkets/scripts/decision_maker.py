# LamApp/supermarkets/scripts/decision_maker.py
import logging
from .DatabaseManager import DatabaseManager
from datetime import date
from .helpers import Helper
from .analyzer import analyzer
from .processor_N import process_N_sales

# Writes to decision_maker.log — separate from other logs due to high volume
logger = logging.getLogger(__name__)


class DecisionMaker:
    def __init__(self, db: DatabaseManager, helper: Helper, blacklist_set=None, skip_sale: bool = False,
                 product_links=None):
        """
        Initialize decision maker with PostgreSQL support.

        product_links: list of ProductLink pairs [((pri_cod, pri_v), (sec_cod, sec_v)), ...].
                       Only one side of each pair is ordered; the other side's stats are
                       merged into it. Which side that is gets resolved in
                       _resolve_product_links.
        """
        self.helper = helper
        self.conn = db.conn
        self.db = db
        self.cursor = db.cursor()
        self.skip_sale = skip_sale
        self.orders_list = []

        self.zombie_products = []   # Products that are finished/not restockable

        # Store blacklist - if None, create empty set
        self.blacklist = blacklist_set if blacklist_set is not None else set()

        # Product link lookups — resolved once, used while iterating every settore
        self.link_partner = {}      # (cod, v) of the side to order -> (cod, v) of the side merged into it
        self.link_suppressed = {}   # (cod, v) not to order -> (cod, v) of the side that carries the order
        self._resolve_product_links(product_links or [])

        logger.info(f"DecisionMaker initialized with {len(self.blacklist)} blacklisted products")

    @staticmethod
    def _link_side_is_available(info):
        """A link side can carry the order unless the catalog marks it unavailable."""
        if info is None:
            return False
        return str(info.get("disponibilita") or "").strip().lower() != "no"

    def _resolve_product_links(self, product_links):
        """
        Pick which side of each link is the one to order.

        Normally that is the primary. When the primary is no longer available
        from the supplier but the secondary still is, the roles are flipped so
        the order can still go through on the secondary — the merged sales
        history and stock stay the same either way.
        """
        for primary, secondary in product_links:
            primary_info = self.db.get_linked_product_stats(*primary)
            secondary_info = self.db.get_linked_product_stats(*secondary)

            order_side, merged_side = primary, secondary
            if not self._link_side_is_available(primary_info) and self._link_side_is_available(secondary_info):
                order_side, merged_side = secondary, primary
                logger.info(
                    f"Product link {primary[0]}.{primary[1]} → {secondary[0]}.{secondary[1]}: "
                    f"primary is not available (disponibilita=No), falling back to the secondary "
                    f"{secondary[0]}.{secondary[1]} as the order target"
                )

            self.link_partner[order_side] = merged_side
            self.link_suppressed[merged_side] = order_side

    def get_products_by_settore(self, settore):
        """
        Retrieve all products (and their stats) for a given settore.
        """
        query = """
            SELECT p.cod, p.v, p.descrizione, ps.stock, ps.sold_last_24, ps.bought_last_24, ps.sales_sets,
                ps.bought_sets, p.pz_x_collo, p.rapp, ps.verified, p.disponibilita, p.purge_flag, p.cluster,
                ps.minimum_stock, p.shelf_life_days, ps.promo_lifts, ps.max_stock, ps.bulk_order,
                e.sale_start, e.sale_end, e.past_windows, e.price_std, e.price_s
            FROM products p
            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            LEFT JOIN economics e ON p.cod = e.cod AND p.v = e.v
            WHERE p.settore = %s
        """
        self.cursor.execute(query, (settore,))
        return self.cursor.fetchall()
    
    def get_extra_losses(self):
        """
        Single-query fetch of extra_losses.
        Returns (internal_dict, expired_dict):
          internal_dict: {(cod, v): internal_array} for products with internal losses
          expired_dict:  {(cod, v): expired_array} for products with expired losses
        """
        self.cursor.execute("""
            SELECT cod, v, internal, expired
            FROM extra_losses
            WHERE internal IS NOT NULL OR expired IS NOT NULL;
        """)
        rows = self.cursor.fetchall()

        internal_dict = {}
        expired_dict = {}
        for row in rows:
            if row["internal"] is not None:
                internal_dict[(row["cod"], row["v"])] = row["internal"] or []
            if row["expired"] is not None:
                expired_dict[(row["cod"], row["v"])] = row["expired"]

        return internal_dict, expired_dict


    def settore_lift_prior(self, settore):
        """(lift, sd) assumed for this settore's never-measured products — see Helper.settore_lift_prior."""
        self.cursor.execute("""
            SELECT ps.promo_lifts
            FROM product_stats ps
            JOIN products p ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.settore = %s AND ps.verified = TRUE AND ps.promo_lifts IS NOT NULL
        """, (settore,))
        return Helper.settore_lift_prior(
            [Helper.expected_promo_lift(r["promo_lifts"]) for r in self.cursor.fetchall()]
        )

    @staticmethod
    def promo_discount(price_std, price_s):
        """Promo depth in %, or None when the prices cannot give one."""
        if not price_std or price_s is None or price_s >= price_std:
            return None
        return round((price_std - price_s) / price_std * 100, 2)

    def decide_orders_for_settore(self, settore, coverage, minimum_stock_base=None, lead_days=0.0,
                                  cluster_minimum_stock=None, coverage_window=None, day_weights=None):
        """
        Main method — iterate over all products in a settore and decide what to order.
        Now tracks zombie_products.

        lead_days: weighted days between the order and its delivery
        (RestockSchedule.calculate_lead_days). Only read by the max_stock ceiling;
        0 assumes nothing sells before delivery, the tightest ceiling.

        cluster_minimum_stock: {cluster: base} replacing minimum_stock_base for
        that cluster's products (Storage.cluster_minimum_stocks).

        coverage_window: [(date, share), ...], the days coverage spans and each one's
        weighted share of it (RestockSchedule.coverage_window). Tells which of those
        days are on promo; without it they are approximated from coverage alone.

        day_weights: the store's seven weekday weights, Monday first
        (Supermarket.get_day_weight), so a running promo's first days are judged
        against their own weekdays. Without them every day counts the same.
        """
        cluster_minimum_stock = cluster_minimum_stock or {}
        today = date.today()
        if not coverage_window:
            coverage_window = Helper.coverage_window_fallback(today, coverage)
        mean_weight = sum(day_weights) / 7 if day_weights else 0
        day_shares = [w / mean_weight for w in day_weights] if mean_weight > 0 else None
        lead_days = min(max(0.0, lead_days or 0.0), coverage)
        logger.info(f"Processing settore: {settore} with coverage: {coverage} days, lead time: {lead_days} days")
        logger.info(f"Active blacklist has {len(self.blacklist)} products")
        
        products = self.get_products_by_settore(settore)
        logger.info(f"Found {len(products)} products in settore '{settore}'")
        
        internal_lookup, expired_lookup = self.get_extra_losses()

        # Closure / sync-gap days look like real zeros and would inflate every sigma.
        # Sliced like sales_sets, so mask[i] and sales_sets[i] are the same day.
        closure_mask = Helper.closure_day_mask(Helper.sales_history(self.db.get_store_daily_totals()))
        excluded = sum(1 for c in closure_mask if c)
        if excluded:
            logger.info(f"Excluding {excluded} closure/no-sync day(s) from sigma estimation")

        safety_z = Helper.safety_z_for(settore)
        logger.info(f"Safety-stock z for settore '{settore}' = {safety_z}")

        lift_prior = self.settore_lift_prior(settore)
        logger.info(f"Promo lift assumed for never-measured products: x{lift_prior[0]:.2f} (sd {lift_prior[1]:.2f})")

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

            # CHECK PRODUCT LINK — only one side of a link is ordered; merge into it later
            link_carrier = self.link_suppressed.get((product_cod, product_var))
            if link_carrier is not None:
                logger.info(
                    f"Skipping linked product: {product_cod}.{product_var} "
                    f"(handled by {link_carrier[0]}.{link_carrier[1]})"
                )
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

            # PRODUCT LINK — merge the other side's sales_sets and stock into this one
            linked_partner = self.link_partner.get((product_cod, product_var))
            if linked_partner is not None:
                partner_stats = self.db.get_linked_product_stats(linked_partner[0], linked_partner[1])
                if partner_stats is not None:
                    sales_sets = Helper.merge_sales_sets(sales_sets, partner_stats["sales_sets"])
                    stock = stock + max(0, partner_stats["stock"])
                    logger.info(
                        f"Merged linked product {linked_partner[0]}.{linked_partner[1]} "
                        f"into {product_cod}.{product_var}: "
                        f"stock+={partner_stats['stock']}"
                    )

            # After the merge: merge_sales_sets pairs slots positionally and both sides
            # still carry their running day at slot 0.
            sales_sets = Helper.sales_history(sales_sets)

            package_size = row["pz_x_collo"]
            package_multi = row["rapp"]
            verified = row["verified"]
            disponibilita = row["disponibilita"]
            minimum_stock_override = row.get("minimum_stock", None)
            product_minimum_base = cluster_minimum_stock.get(row.get("cluster"), minimum_stock_base)
            shelf_life_days = row.get("shelf_life_days", None)

            logger.info(f"Processing {product_cod}.{product_var} - {descrizione} (stock={stock})")

            if not verified and disponibilita == "No":
                logger.info(f"{product_cod}.{product_var} - {descrizione} skipped because is not verified and not available")
                continue

            if stock == 0 and verified and disponibilita == "No":
                logger.info(f"{product_cod}.{product_var} - {descrizione} marked as zombie because is not available and has verified stock of 0")
                zombie_products.append({
                    'cod': product_cod,
                    'var': product_var,
                    'reason': 'Finished and not restockable (disponibilita=No, stock=0)'
                })
                continue

            # Divisor from here on. Skip rather than default to 1, which would order
            # loose units against a supplier that ships full cases.
            if not package_size or not package_multi:
                reason = f"Invalid package size (pz_x_collo={package_size}, rapp={package_multi}) — catalog data missing"
                logger.warning(f"{product_cod}.{product_var} - {descrizione}: {reason}")
                Helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                continue

            package_size *= package_multi

            if bought_array[0] == 0 and sold_array[0] == 0:
                if not verified:
                    reason = "Never been in system (brand new product)"
                    Helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    continue
                elif disponibilita == "No":
                    reason = "Not available for restocking and no sales history"
                    Helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    continue

            # Promo days, at any age, stay out of the baseline: the lift below adds the
            # promo back once, on the days of this window that are on promo.
            history = sales_sets
            windows = Helper.promo_windows(row.get("sale_start"), row.get("sale_end"), row.get("past_windows"))
            split = Helper.split_promo_history(history, windows, today)
            baseline = split.baseline
            if not split.lift_allowed:
                logger.info(f"{product_cod}.{product_var}: on promo most of the time lately, promo level taken as normal")

            avg_from_sets = Helper.avg_daily_sales_from_sales_sets(baseline)
            if avg_from_sets is not None:
                avg_daily_sales = avg_from_sets
            else:
                avg_daily_sales, _ = self.helper.calculate_weighted_avg_sales_new(sold_array)

            # Staff consumption is real depletion. register_losses already spreads it into
            # sales_sets, so only the sold_last_24 fallback needs it added.
            internal_array = internal_lookup.get((product_cod, product_var)) if avg_from_sets is None else None
            if internal_array:
                internal_daily = Helper.internal_loss_daily_rate(internal_array)
                if internal_daily > 0:
                    logger.info(
                        f"{product_cod}.{product_var}: internal consumption "
                        f"+{internal_daily:.2f}/day (sales {avg_daily_sales:.2f}/day)"
                    )
                    avg_daily_sales += internal_daily

            deviation_corrected = Helper.calculate_deviation(baseline)

            req_stock = avg_daily_sales * coverage

            if avg_from_sets is not None:
                oos_window = history[:7]
                null_count = sum(1 for v in oos_window if v is None)
                if null_count > 0:
                    null_rate = null_count / len(oos_window)
                    correction = 1.5 if null_rate >= 1.0 else min(1.0 / (1.0 - null_rate), 1.5)
                    logger.warning(
                        f"OOS correction {product_cod}.{product_var} '{descrizione}': "
                        f"{null_count}/7 OOS days → req_stock {req_stock:.2f} (pre-correction) ×{correction:.2f}"
                    )
                    req_stock *= correction

            logger.info(f"Required stock = {req_stock:.2f}")

            package_consumption = req_stock / package_size
            logger.info(f"Package consumption = {package_consumption:.2f} (package_size={package_size})")

            sigma_daily = Helper.demand_sigma_daily(baseline, closure_mask)
            if sigma_daily is None and split.promo_masked:
                # Too few days left without promo for a spread: with them is the cautious side
                sigma_daily = Helper.demand_sigma_daily(history, closure_mask)

            promo_cov, opening_cov = (
                Helper.promo_coverage(windows, coverage_window) if split.lift_allowed else (0.0, 0.0)
            )
            observed = split.observed
            lift, lift_sd = 1.0, 0.0
            discount = None
            if promo_cov > 0:
                if self.skip_sale:
                    reason = "Skip products on sale mode is active for this order"
                    Helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                    continue
                depth = self.promo_discount(row.get("price_std"), row.get("price_s"))
                discount = depth if depth is not None else 10
                measured = Helper.expected_promo_lift(row.get("promo_lifts"), depth)
                if measured is not None:
                    prior, prior_sd, source = measured, Helper.PROMO_LIFT_SD_FRAC * (measured - 1.0), "measured"
                else:
                    (prior, prior_sd), source = lift_prior, "settore median"
                observed_shares = [day_shares[d.weekday()] for d in split.observed_days] if day_shares else None
                lift, lift_sd = Helper.learn_promo_lift(prior, prior_sd, observed, avg_daily_sales, sigma_daily,
                                                        observed_shares=observed_shares)
                # The lift on the window's promo days only, the opening boost on a run's first days
                factor = 1.0 + (
                    (lift - 1.0) * promo_cov + lift * (Helper.PROMO_OPEN_BOOST - 1.0) * opening_cov
                ) / coverage if coverage > 0 else 1.0
                req_stock *= factor
                logger.info(
                    f"Promo {discount}%: {promo_cov:.2f} of {coverage:.2f} window days on promo "
                    f"({opening_cov:.2f} opening), lift x{lift:.2f} from {source} x{prior:.2f}"
                    + (f" and {len(observed)} promo day(s) seen" if observed else "")
                    + f" -> req_stock x{factor:.2f} = {req_stock:.2f}"
                )

            if shelf_life_days is not None and shelf_life_days <= 90:
                expiry_factor = None
                if (product_cod, product_var) in expired_lookup:
                    expiry_factor = Helper.compute_expiry_factor(
                        expired_lookup[(product_cod, product_var)], sold_array
                    )

                batch_expiry_factor = Helper.compute_batch_expiry_factor(
                    bought_sets, split.batch_history, stock, shelf_life_days, avg_daily_sales
                )
            else:
                expiry_factor = None
                batch_expiry_factor = None

            if verified:
                category = "N"
                # Same rate as req_stock, so promo lift and OOS correction carry over
                lead_demand = req_stock * lead_days / coverage if coverage > 0 else 0.0
                if sigma_daily is not None:
                    # Promo days add volume, and the risk that this promo's lift is not the
                    # expected one, which hits all of them at once
                    sigma_L = (sigma_daily ** 2 * (max(coverage, 1) + (lift - 1.0) * promo_cov)
                               + (lift_sd * avg_daily_sales * promo_cov) ** 2) ** 0.5
                else:
                    sigma_L = None

                result, check, status, returned_discount = process_N_sales(
                    package_size, deviation_corrected, avg_daily_sales,
                    req_stock, stock, discount, product_minimum_base, minimum_stock_override,
                    expiry_factor, shelf_life_days, batch_expiry_factor,
                    sigma_L, safety_z,
                    row.get("max_stock"), bool(row.get("bulk_order")), lead_demand,
                )
            else:
                reason = "Not verified in system"
                Helper.next_article(product_cod, product_var, package_size, descrizione, reason)
                continue

            if result:
                if avg_daily_sales <= 0.2:
                    analyzer.low_sale_recorder(descrizione, product_cod, product_var)
                analyzer.stat_recorder(result, status, check)
                Helper.order_this(order_list, product_cod, product_var, result, descrizione, category, check, returned_discount)
            else:
                analyzer.stat_recorder(0, status, check)
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