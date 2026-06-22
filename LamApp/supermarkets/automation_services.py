import math
"""
Automated restock services.
Workflow: calculate order (decision maker) → execute order (orderer).
Both steps run fresh on every execution, including retries.
"""
import logging
from pathlib import Path
from django.conf import settings
from django.utils import timezone
from django.db import transaction
from .models import Storage, RestockLog, ScheduleException
from .services import RestockService
import shutil
from .scripts.decision_maker import DecisionMaker
from .scripts.inventory_scrapper import Inventory_Scrapper
from .scripts.inventory_reader import verify_lost_stock_from_excel_combined
from .scripts.orderer import Orderer

logger = logging.getLogger(__name__)


class AutomatedRestockService(RestockService):
    """Handles automated restock operations."""
    
    def record_losses(self):
        """Record product losses by downloading and processing inventory files."""
        from .models import RestockLog
        from django.utils import timezone

        logger.info(f"Starting loss recording for {self.supermarket.name}")

        log = RestockLog.objects.create(
            storage=self.storage,
            operation_type='loss_recording',
            status='processing',
            current_stage='pending',
            started_at=timezone.now(),
        )

        try:
            inv_scrapper = Inventory_Scrapper(
                supermarket=self.supermarket,
                username=self.supermarket.username,
                password=self.supermarket.password
            )

            try:
                inv_scrapper.login()
                logger.info("Downloading loss inventory files...")
                inv_scrapper.export_all_testate_from_day()

                logger.info("Processing loss files...")
                result = verify_lost_stock_from_excel_combined(self.db)

                log.status = 'completed'
                log.current_stage = 'completed'
                log.completed_at = timezone.now()
                log.total_products = result['total_unique_products']
                log.total_packages = result['total_losses']
                log.set_results({
                    'by_settore': result['by_settore'],
                    'total_losses': result['total_losses'],
                    'files_processed': result['files_processed'],
                    'absent_eans': result['absent_eans'],
                })
                log.save()

                logger.info(f"Loss recording completed for {self.supermarket.name}")
                return True

            finally:
                inv_scrapper.driver.quit()
                shutil.rmtree(inv_scrapper.user_data_dir, ignore_errors=True)

        except Exception as e:
            log.status = 'failed'
            log.current_stage = 'failed'
            log.error_message = str(e)
            log.completed_at = timezone.now()
            log.save()
            logger.exception(f"Error recording losses for {self.supermarket.name}")
            raise
    
    def snapshot_pre_delivery_stock(self) -> dict:
        """
        Lightweight stock-only snapshot called right before apply_ddt_for_storage.
        Returns {'{cod}.{v}': stock} for all verified+available products in this storage.
        Sales data is intentionally excluded — it will be read fresh at report-creation time.
        """
        cur = self.db.cursor()
        cur.execute("""
            SELECT ps.cod, ps.v, ps.stock
            FROM product_stats ps
            JOIN products p ON p.cod = ps.cod AND p.v = ps.v
            WHERE ps.verified = TRUE
              AND p.disponibilita IS NOT NULL AND p.disponibilita != 'No'
              AND p.settore = %s
        """, (self.storage.settore,))
        return {f"{row['cod']}.{row['v']}": (row['stock'] or 0) for row in cur.fetchall()}

    def compute_calibration_for_storage(self, coverage_days: float = 0.0, raw_stock: dict = None) -> dict:
        """
        Classify every verified+available product against calibration thresholds.

        raw_stock: pre-delivery stock dict from snapshot_pre_delivery_stock().
                   When provided, overrides the DB stock value per product so
                   classification reflects pre-delivery state even when called
                   after the DDT has been applied.
                   When None, reads current stock from DB.
        """
        min_floor = self.storage.minimum_stock

        cur = self.db.cursor()
        cur.execute("""
            SELECT ps.cod, ps.v, p.descrizione, ps.stock, ps.minimum_stock AS min_override,
                   ps.sales_sets, p.pz_x_collo, p.rapp,
                   e.sale_start, e.sale_end
            FROM product_stats ps
            JOIN products p ON p.cod = ps.cod AND p.v = ps.v
            LEFT JOIN economics e ON e.cod = ps.cod AND e.v = ps.v
            WHERE ps.verified = TRUE
              AND p.disponibilita IS NOT NULL AND p.disponibilita != 'No'
              AND p.settore = %s
        """, (self.storage.settore,))
        rows = cur.fetchall()

        critical = []
        understocked = []
        overstocked = []
        ok = []

        from datetime import date as _date
        today = _date.today()

        for row in rows:
            key = f"{row['cod']}.{row['v']}"
            stock = raw_stock[key] if raw_stock is not None and key in raw_stock else (row['stock'] or 0)
            min_override = row['min_override']
            sales_sets = row['sales_sets'] or []
            pz_x_collo = row['pz_x_collo'] or 1
            rapp = row['rapp'] or 1
            package_size = pz_x_collo * rapp
            floor = min_override if min_override is not None else min_floor
            sale_start = row.get('sale_start')
            sale_end = row.get('sale_end')
            on_sale = bool(sale_start and sale_end and sale_start <= today <= sale_end)

            avg_daily_sales = self.helper.avg_daily_sales_from_sales_sets(sales_sets, silent=True)
            if avg_daily_sales is None:
                avg_daily_sales = 0.0

            req_stock = round(avg_daily_sales * coverage_days)

            if min_override is not None:
                eff_min = min_override
            else:
                eff_min = min_floor

            if avg_daily_sales >= 0.6:
                eff_min += round(math.sqrt(max(0, req_stock - 1)))
            elif avg_daily_sales < 0.6:
                eff_min -= 1
                if avg_daily_sales < 0.1:
                    eff_min -= 1
                    if avg_daily_sales < 0.05:
                        eff_min -= 1

            deviation = self.helper.calculate_deviation(sales_sets)
            if deviation >= 40:
                eff_min = math.floor(eff_min * 1.2)
            elif deviation >= 20:
                eff_min = math.floor(eff_min * 1.1)
            elif deviation <= -40:
                eff_min = math.ceil(eff_min * 0.7)
            elif deviation <= -20:
                eff_min = math.ceil(eff_min * 0.9)

            eff_min = round(eff_min)
            if min_override is not None:
                eff_min = max(min_override, eff_min)
            else:
                eff_min = max(1, eff_min)

            entry = {
                'cod': row['cod'],
                'v': row['v'],
                'descrizione': row['descrizione'],
                'stock': stock,
                'floor': floor,
                'eff_min': eff_min,
                'floor_delta': eff_min - floor,
                'package_size': package_size,
                'on_sale': on_sale,
                'avg_daily_sales': round(avg_daily_sales, 2),
            }

            if stock <= 0:
                critical.append(entry)
            elif stock < eff_min:
                understocked.append(entry)
            elif stock > eff_min + package_size:
                overstocked.append(entry)
            else:
                ok.append(entry)

        return {
            'products_evaluated': len(rows),
            'products_ok': len(ok),
            'products_understocked': len(understocked),
            'products_critical': len(critical),
            'products_overstocked': len(overstocked),
            'critical': critical,
            'understocked': understocked,
            'overstocked': overstocked,
            'ok': ok,
        }

    def apply_ddt_for_storage(self, log: RestockLog, cod_v_qty: dict, invoice_numbers: list):
        """
        DB-only: apply already-fetched DDT data for this specific storage.
        Called by import_ddt_for_supermarket after the API work is done once.
        cod_v_qty: {(cod, v): qty}
        """
        logger.info(f"Applying DDT deliveries for {self.storage.name}")

        report = {'updated': 0, 'not_found': [], 'errors': [], 'unverified_products': []}

        # Guard: require at least 1 verified product among delivered (cod, v) in THIS settore
        cur = self.db.cursor()
        pairs = list(cod_v_qty.keys())
        placeholders = ','.join(['(%s,%s)'] * len(pairs))
        flat = [x for pair in pairs for x in pair]
        cur.execute(f"""
            SELECT 1 FROM product_stats ps
            JOIN products p ON p.cod = ps.cod AND p.v = ps.v
            WHERE (ps.cod, ps.v) IN ({placeholders})
              AND p.settore = %s
              AND ps.verified = TRUE
            LIMIT 1
        """, flat + [self.storage.settore])
        has_verified = cur.fetchone() is not None

        if not has_verified:
            logger.warning(
                f"No verified products in settore '{self.storage.settore}' "
                f"among delivered cod.v for '{self.storage.name}'. Skipping."
            )
        else:
            report = self.db.apply_invoice_deliveries(cod_v_qty)
            logger.info(f"Deliveries applied for '{self.storage.name}': {report}")

        purged_products = self.db.check_and_purge_flagged()
        if purged_products:
            logger.info(f"[AUTO-PURGE] Purged {len(purged_products)} products for {self.storage.name}")

        # Filter not_found against the 'Non gestiti' blacklist for this storage
        from .models import BlacklistEntry
        blacklisted = set(
            BlacklistEntry.objects.filter(
                blacklist__storage=self.storage,
                blacklist__name='Non gestiti',
            ).values_list('product_code', 'product_var')
        )
        not_found_filtered = [
            p for p in report.get('not_found', [])
            if (p['cod'], p['v']) not in blacklisted
        ]
        if len(not_found_filtered) < len(report.get('not_found', [])):
            logger.info(
                f"[DDT] Filtered {len(report.get('not_found', [])) - len(not_found_filtered)} "
                f"blacklisted products from not_found for '{self.storage.name}'"
            )

        with transaction.atomic():
            log = RestockLog.objects.select_for_update().get(id=log.id)
            log.current_stage = 'completed'
            log.stats_updated_at = timezone.now()
            log.total_products = report.get('updated', 0)
            log.set_results({
                'invoices': invoice_numbers,
                'updated': report.get('updated', 0),
                'not_found': not_found_filtered,
                'errors': report.get('errors', []),
                'unverified_products': report.get('unverified_products', []),
            })
            log.save()

        logger.info(f"DDT import complete for {self.storage.name}: invoices={invoice_numbers}")
        return True
    
    def run_full_restock_workflow(self, coverage=None, log=None, progress_callback=None, skip_stats_update=False):
        """
        Run complete restock workflow: calculate order then execute it.
        Both steps always run fresh — no checkpoint skipping.
        """
        logger.info(f"Starting restock workflow for {self.storage.name}")

        if log is None:
            log = RestockLog.objects.create(
                storage=self.storage,
                status='processing',
                current_stage='processing',
                started_at=timezone.now()
            )

        try:
            if not skip_stats_update:
                if progress_callback:
                    progress_callback(10, 'Updating product statistics...')
                self.import_ddt_deliveries(log)

            # Step 1: Calculate order
            if progress_callback:
                progress_callback(20, 'Analyzing product needs...')

            today_date = timezone.now().date()
            if coverage is None:
                schedule = self.storage.schedule
                coverage = schedule.calculate_coverage_for_day(today_date.weekday(), reference_date=today_date)

            skip_sale = ScheduleException.objects.filter(
                schedule=self.storage.schedule,
                date=today_date,
                skip_sale=True
            ).exists()

            log.coverage_used = coverage
            log.save()

            if progress_callback:
                progress_callback(30, 'Running decision algorithm...')

            try:
                from .models import ProductLink
                secondary_products, dominant_to_secondary = ProductLink.build_lookup(self.supermarket)
                decision_maker = DecisionMaker(
                    self.db,
                    self.helper,
                    blacklist_set=self.get_blacklist_set(),
                    skip_sale=skip_sale,
                    secondary_products=secondary_products,
                    dominant_to_secondary=dominant_to_secondary,
                )
                decision_maker.decide_orders_for_settore(self.settore, coverage, self.storage.minimum_stock)
                orders_list = decision_maker.orders_list
                zombie_products = decision_maker.zombie_products

                log.total_products = len(self.db.get_all_stats_by_settore(self.settore))
                log.products_ordered = len(orders_list)
                log.total_packages = sum(order[2] for order in orders_list if len(order) >= 3)
                log.set_results({
                    'orders': [
                        {
                            'cod': order[0],
                            'var': order[1],
                            'qty': order[2],
                            'discount': order[3] if len(order) > 3 else None
                        }
                        for order in orders_list
                    ],
                    'zombie_products': zombie_products,
                    'settore': self.settore,
                    'coverage': float(coverage)
                })
                log.save()
            finally:
                decision_maker.close()
                self.db.close()

            if progress_callback:
                progress_callback(50, f'Order calculated: {len(orders_list)} products')
            logger.info(f"Order calculated: {len(orders_list)} products, {len(zombie_products)} zombie")

            # Step 2: Execute order
            if not orders_list:
                logger.info(f"No items to order for {self.storage.name}")
                log.status = 'completed'
                log.current_stage = 'completed'
                log.completed_at = timezone.now()
                log.save()
                return log

            if progress_callback:
                progress_callback(70, f'Placing order for {len(orders_list)} products...')

            orderer = Orderer(
                username=self.supermarket.username,
                password=self.supermarket.password
            )
            try:
                orderer.login()
                successful_orders, order_skipped = orderer.make_orders(self.storage.name, orders_list)

                results = log.get_results()
                results.setdefault('order_skipped_products', []).extend(order_skipped)
                log.set_results(results)

                log.products_ordered = len(successful_orders)
                log.total_packages = sum(order[2] for order in successful_orders)
                log.status = 'completed'
                log.current_stage = 'completed'
                log.completed_at = timezone.now()
                log.save()
            finally:
                orderer.driver.quit()

            if progress_callback:
                progress_callback(100, 'Order placed successfully!')
            logger.info(f"Restock workflow completed successfully for {self.storage.name}")
            return log

        except Exception as e:
            logger.exception(f"Restock workflow failed for {self.storage.name}")
            log.status = 'failed'
            log.current_stage = 'failed'
            log.error_message = str(e)
            log.save()
            raise