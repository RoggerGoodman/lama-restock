# automation_services.py - FIXED: No stats in retry, only 2 checkpoints
"""
Automated services with checkpoint-based recovery.
UPDATED: Stats are done nightly, so retry only has 2 steps:
  1. Calculate order (decision maker)
  2. Execute order (orderer)
"""
import logging
from pathlib import Path
from django.conf import settings
from django.utils import timezone
from django.db import transaction
from .models import Storage, RestockLog, ScheduleException
from .services import RestockService
from .scripts.decision_maker import DecisionMaker
from .scripts.inventory_scrapper import Inventory_Scrapper
from .scripts.inventory_reader import verify_lost_stock_from_excel_combined
from .scripts.scrapper import Scrapper
from .scripts.orderer import Orderer

logger = logging.getLogger(__name__)


class AutomatedRestockService(RestockService):
    """
    Handles automated restock operations with checkpoint recovery.
    FIXED: Retry logic no longer updates stats (done nightly).
    """
    
    def record_losses(self):
        """Record product losses by downloading and processing inventory files."""
        logger.info(f"Starting loss recording for {self.supermarket.name}")
        
        try:
            inv_scrapper = Inventory_Scrapper(
                username=self.supermarket.username,
                password=self.supermarket.password
            )
            
            try:
                inv_scrapper.login()
                inv_scrapper.inventory()
                
                logger.info("Downloading loss inventory files...")
                inv_scrapper.export_all_testate_from_day()
                
                logger.info("Processing loss files...")
                verify_lost_stock_from_excel_combined(self.db)
                
                logger.info(f"✅ Loss recording completed for {self.supermarket.name}")
                return True
                
            finally:
                inv_scrapper.driver.quit()
                
        except Exception as e:
            logger.exception(f"Error recording losses for {self.supermarket.name}")
            raise
    
    def update_product_stats_checkpoint(self, log: RestockLog, full: bool = True, progress_callback=None):
        """
        CHECKPOINT 1 (NIGHTLY ONLY): Update product statistics from PAC2000A.
        This should ONLY be called during nightly automated runs.
        Manual orders and retries should SKIP this step.
        """
        logger.info(f"[CHECKPOINT 1 - NIGHTLY] Updating product stats for {self.storage.name}")
        
        if progress_callback:
            progress_callback(10, 'Connecting to PAC2000A...')
        
        with transaction.atomic():
            log = RestockLog.objects.select_for_update().get(id=log.id)
            
            if log.stats_updated_at:
                logger.warning(f"Stats already updated, skipping (race condition prevented)")
                return
            
            log.current_stage = 'updating_stats'
            log.save()
        
        try:
            if progress_callback:
                progress_callback(15, 'Downloading product statistics (5-10 min)...')
            
            scrapper = Scrapper(
                username=self.supermarket.username,
                password=self.supermarket.password,
                helper=self.helper,
                db=self.db
            )
            
            try:
                scrapper.navigate()
                
                if progress_callback:
                    progress_callback(30, 'Processing product data...')
                
                scrapper.init_product_stats_for_settore(self.settore, full)
                
                if progress_callback:
                    progress_callback(50, 'Finalizing statistics update...')
                
                purged_products = self.db.check_and_purge_flagged()
                if purged_products:
                    logger.info(f"[AUTO-PURGE] ✅ Purged {len(purged_products)} products")
                
                with transaction.atomic():
                    log = RestockLog.objects.select_for_update().get(id=log.id)
                    log.current_stage = 'stats_updated'
                    log.stats_updated_at = timezone.now()
                    log.save()
                
                logger.info(f"✅ [CHECKPOINT 1 COMPLETE] Stats updated")
                return True
                    
            finally:
                scrapper.driver.quit()
                    
        except Exception as e:
            with transaction.atomic():
                log = RestockLog.objects.select_for_update().get(id=log.id)
                log.current_stage = 'failed'
                log.status = 'failed'
                log.error_message = f"Stats update failed: {str(e)}"
                log.save()
            
            logger.exception(f"[CHECKPOINT 1 FAILED]")
            raise
    
    def calculate_order_checkpoint(self, log: RestockLog, coverage=None, progress_callback=None):
        """
        CHECKPOINT 1 (RETRY): Calculate what needs to be ordered.
        This is now the FIRST checkpoint in retry logic.
        """
        logger.info(f"[CHECKPOINT 1 - RETRY] Calculating order for {self.storage.name}")
        
        if progress_callback:
            progress_callback(20, 'Analyzing product needs...')
        
        log.current_stage = 'calculating_order'
        log.save()
        
        try:
            # Calculate coverage if not provided
            if coverage is None:
                schedule = self.storage.schedule
                today = timezone.now().weekday()
                coverage = schedule.calculate_coverage_for_day(today)

            # Check if today has a skip_sale exception
            today_date = timezone.now().date()
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
                decision_maker = DecisionMaker(
                    self.db,
                    self.helper,
                    blacklist_set=self.get_blacklist_set(),
                    skip_sale=skip_sale
                )
                
                decision_maker.decide_orders_for_settore(self.settore, coverage)
                
                orders_list = decision_maker.orders_list
                new_products = decision_maker.new_products
                skipped_products = decision_maker.skipped_products
                zombie_products = decision_maker.zombie_products
                
                log.total_products = len(self.db.get_all_stats_by_settore(self.settore))
                log.products_ordered = len(orders_list)
                
                # Handle 4-element tuples (with discount)
                total_packages = 0
                for order in orders_list:
                    if len(order) >= 3:
                        total_packages += order[2]  # qty is always 3rd element
                
                log.total_packages = total_packages
                
                # Store discount in results
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
                    'new_products': new_products,
                    'skipped_products': skipped_products,
                    'zombie_products': zombie_products,
                    'settore': self.settore,
                    'coverage': float(coverage)
                })
                
                # Mark checkpoint as complete
                log.current_stage = 'order_calculated'
                log.order_calculated_at = timezone.now()
                log.save()
                
                if progress_callback:
                    progress_callback(50, f'Order calculated: {len(orders_list)} products')
                
                logger.info(
                    f"✅ [CHECKPOINT 1 COMPLETE] Order calculated: "
                    f"{len(orders_list)} products ordered, "
                    f"{len(new_products)} new, {len(skipped_products)} skipped, "
                    f"{len(zombie_products)} zombie"
                )
                return orders_list
                
            finally:
                decision_maker.close()
                self.db.close()
            
        except Exception as e:
            log.current_stage = 'failed'
            log.status = 'failed'
            log.error_message = f"Order calculation failed: {str(e)}"
            log.save()
            
            logger.exception(f"❌ [CHECKPOINT 1 FAILED]")
            raise
   
    def execute_order_checkpoint(self, log: RestockLog, orders_list, progress_callback=None):
        """
        CHECKPOINT 2 (RETRY): Execute the order in PAC2000A.
        This is now the SECOND and FINAL checkpoint in retry logic.
        """
        logger.info(f"[CHECKPOINT 2 - RETRY] Executing order for {self.storage.name}")
        
        if progress_callback:
            progress_callback(70, 'Connecting to ordering system...')
        
        log.current_stage = 'executing_order'
        log.save()
        
        try:
            if not orders_list:
                logger.info(f"No items to order for {self.storage.name}")
                log.current_stage = 'completed'
                log.status = 'completed'
                log.completed_at = timezone.now()
                log.save()
                return True
            
            logger.info(f"Executing order with {len(orders_list)} items")
            
            if progress_callback:
                progress_callback(80, f'Placing order for {len(orders_list)} products...')
            
            orderer = Orderer(
                username=self.supermarket.username,
                password=self.supermarket.password
            )
            
            try:
                orderer.login()
                successful_orders, order_skipped = orderer.make_orders(
                    self.storage.name, 
                    orders_list
                )
                
                # Add order-skipped products to results
                results = log.get_results()
                if 'order_skipped_products' not in results:
                    results['order_skipped_products'] = []
                results['order_skipped_products'].extend(order_skipped)
                log.set_results(results)
                
                log.products_ordered = len(successful_orders)
                log.total_packages = sum(order[2] for order in successful_orders)
                
                log.current_stage = 'completed'
                log.status = 'completed'
                log.order_executed_at = timezone.now()
                log.completed_at = timezone.now()
                log.save()
                
                if progress_callback:
                    progress_callback(100, 'Order placed successfully!')
                
                logger.info(f"✅ [CHECKPOINT 2 COMPLETE] Order executed successfully")
                return True
                
            finally:
                orderer.driver.quit()
                
        except Exception as e:
            log.current_stage = 'failed'
            log.status = 'failed'
            log.error_message = f"Order execution failed: {str(e)}"
            log.save()
            
            logger.exception(f"❌ [CHECKPOINT 2 FAILED]")
            raise
    
    def run_full_restock_workflow(self, coverage=None, log=None, progress_callback=None, skip_stats_update=False):
        """
        Run complete restock workflow with optional progress reporting.
        
        Args:
            coverage: Days to cover (None = auto-calculate)
            log: Existing RestockLog or None to create new
            progress_callback: Function to call with (progress_pct, status_msg)
            skip_stats_update: If True, skip CHECKPOINT 1 (stats update)
        
        UPDATED: skip_stats_update is now the default for manual orders and retries.
        """
        logger.info(f"Starting restock workflow for {self.storage.name} (skip_stats={skip_stats_update})")
        
        if log is None:
            log = RestockLog.objects.create(
                storage=self.storage,
                status='processing',
                current_stage='pending',
                started_at=timezone.now()
            )
        
        try:
            # CHECKPOINT 1 (NIGHTLY ONLY): Update stats
            if skip_stats_update:
                logger.info(f"[CHECKPOINT 1 SKIP] Stats update skipped (manual order/retry)")
                if progress_callback:
                    progress_callback(10, 'Skipping stats update (already done)...')
            else:
                if progress_callback:
                    progress_callback(10, 'Updating product statistics...')
                
                with transaction.atomic():
                    log.refresh_from_db()
                    
                    if log.stats_updated_at:
                        logger.info(f"[CHECKPOINT 1 SKIP] Stats already updated")
                    else:
                        logger.info(f"[CHECKPOINT 1 START] Updating stats...")
                        self.update_product_stats_checkpoint(log)
            
            # CHECKPOINT 1 (RETRY): Calculate order
            if progress_callback:
                progress_callback(20, 'Calculating order quantities...')
            
            with transaction.atomic():
                log.refresh_from_db()
                
                if log.order_calculated_at:
                    results = log.get_results()
                    orders_list = [
                        (o['cod'], o['var'], o['qty']) 
                        for o in results.get('orders', [])
                    ]
                    logger.info(f"[CHECKPOINT 1 SKIP] Order already calculated")
                else:
                    logger.info(f"[CHECKPOINT 1 START] Calculating order...")
                    orders_list = self.calculate_order_checkpoint(log, coverage, progress_callback)
            
            # CHECKPOINT 2 (RETRY): Execute order
            if progress_callback:
                progress_callback(70, 'Placing order in PAC2000A...')
            
            with transaction.atomic():
                log.refresh_from_db()
                
                if log.order_executed_at:
                    logger.info(f"[CHECKPOINT 2 SKIP] Order already executed")
                else:
                    logger.info(f"[CHECKPOINT 2 START] Executing order...")
                    self.execute_order_checkpoint(log, orders_list, progress_callback)
            
            if progress_callback:
                progress_callback(100, 'Restock completed successfully!')
            
            logger.info(f"✅ Restock workflow completed successfully")
            return log
            
        except Exception as e:
            logger.exception(f"❌ Restock workflow failed")
            
            with transaction.atomic():
                log = RestockLog.objects.select_for_update().get(id=log.id)
                
                if log.status != 'completed':
                    log.status = 'failed'
                    log.save()
            
            raise
    
    def retry_from_checkpoint(self, log, coverage=None):
        """
        Retry workflow from last successful checkpoint.
        FIXED: No longer tries to update stats on retry.
        """
        logger.info(f"🔄 Retrying from checkpoint for {self.storage.name}")
        
        # CHECKPOINT 1: Calculate order
        if log.order_calculated_at:
            logger.info(f"[CHECKPOINT 1 SKIP] Order already calculated")
            results = log.get_results()
            orders_list = [
                (o['cod'], o['var'], o['qty'], o.get('discount'))
                for o in results.get('orders', [])
            ]
        else:
            logger.info(f"[CHECKPOINT 1 START] Calculating order...")
            orders_list = self.calculate_order_checkpoint(log, coverage)
        
        # CHECKPOINT 2: Execute order
        if log.order_executed_at:
            logger.info(f"[CHECKPOINT 2 SKIP] Order already executed")
        else:
            logger.info(f"[CHECKPOINT 2 START] Executing order...")
            self.execute_order_checkpoint(log, orders_list)
        
        logger.info(f"✅ Restock retry completed")
        return log