# automation_services.py (UPDATED - NOW INHERITS FROM RestockService)
"""
Automated services with checkpoint-based recovery.
Each stage saves progress, allowing retry from last successful checkpoint.
"""
import logging
from pathlib import Path
from django.conf import settings
from django.utils import timezone
from django.db import transaction
from .models import Storage, RestockLog
from .services import RestockService  # ← NEW: Import base class
from .scripts.decision_maker import DecisionMaker
from .scripts.inventory_scrapper import Inventory_Scrapper
from .scripts.inventory_reader import verify_lost_stock_from_excel_combined
from .scripts.scrapper import Scrapper
from .scripts.orderer import Orderer

logger = logging.getLogger(__name__)


class AutomatedRestockService(RestockService):  # ← NEW: Inherit from RestockService
    """
    Handles automated restock operations with checkpoint recovery.
    
    Inherits database access and basic helpers from RestockService.
    Adds workflow capabilities for automated operations.
    
    Each operation is divided into stages:
    1. Update product stats (scrapper)
    2. Calculate order (decision maker)
    3. Execute order (orderer)
    
    If a stage fails, it can be retried from that checkpoint.
    """    
    def record_losses(self):
        """
        Record product losses by downloading and processing inventory files.
        """
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
    
    def update_product_stats_checkpoint(self, log: RestockLog, full: bool = True):
        """
        CHECKPOINT 1: Update product statistics from PAC2000A
        """
        logger.info(f"[CHECKPOINT 1] Updating product stats for {self.storage.name}")
        
        # CRITICAL: Mark as started BEFORE doing anything
        with transaction.atomic():
            log = RestockLog.objects.select_for_update().get(id=log.id)
            
            if log.stats_updated_at:
                logger.warning(f"Stats already updated, skipping (race condition prevented)")
                return
            
            log.current_stage = 'updating_stats'
            log.save()
        
        try:
            scrapper = Scrapper(
                username=self.supermarket.username,
                password=self.supermarket.password,
                helper=self.helper,
                db=self.db
            )
            
            try:
                scrapper.navigate()
                scrapper.init_product_stats_for_settore(self.settore, full)
                
                # Auto-purge check
                purged_products = self.db.check_and_purge_flagged()
                if purged_products:
                    logger.info(f"[AUTO-PURGE] ✅ Purged {len(purged_products)} products")
                
                # Update timestamp atomically
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
                log.error_message = f"Stats update failed: {str(e)}"
                log.save()
            
            logger.exception(f"[CHECKPOINT 1 FAILED]")
            raise
    
    def calculate_order_checkpoint(self, log: RestockLog, coverage=None):
        """
        CHECKPOINT 2: Calculate what needs to be ordered.
        Returns orders_list on success, raises exception on failure.
        """
        logger.info(f"[CHECKPOINT 2] Calculating order for {self.storage.name}")
        
        log.current_stage = 'calculating_order'
        log.save()
        
        try:
            # Calculate coverage if not provided
            if coverage is None:
                schedule = self.storage.schedule
                today = timezone.now().weekday()
                coverage = schedule.calculate_coverage_for_day(today)
            
            log.coverage_used = coverage
            log.save()
            
            try:
                decision_maker = DecisionMaker(
                    self.db, 
                    self.helper, 
                    blacklist_set=self.get_blacklist_set()
                )
                
                # Run decision logic
                logger.info(f"Running decision maker with coverage={coverage}")
                decision_maker.decide_orders_for_settore(self.settore, coverage)
                
                # Get results
                orders_list = decision_maker.orders_list
                new_products = decision_maker.new_products
                skipped_products = decision_maker.skipped_products
                zombie_products = decision_maker.zombie_products
                
                # Update log statistics
                log.total_products = len(self.db.get_all_stats_by_settore(self.settore))
                log.products_ordered = len(orders_list)
                log.total_packages = sum(qty for _, _, qty in orders_list)
                
                # Store detailed results
                log.set_results({
                    'orders': [
                        {'cod': cod, 'var': var, 'qty': qty}
                        for cod, var, qty in orders_list
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
                
                logger.info(
                    f"✅ [CHECKPOINT 2 COMPLETE] Order calculated: "
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
            log.error_message = f"Order calculation failed: {str(e)}"
            log.save()
            
            logger.exception(f"❌ [CHECKPOINT 2 FAILED]")
            raise
   
    def execute_order_checkpoint(self, log: RestockLog, orders_list):
        """
        CHECKPOINT 3: Execute the order in PAC2000A.
        """
        logger.info(f"[CHECKPOINT 3] Executing order for {self.storage.name}")
        
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
                
                # Update statistics
                log.products_ordered = len(successful_orders)
                log.total_packages = sum(qty for _, _, qty in successful_orders)
                
                log.current_stage = 'completed'
                log.status = 'completed'
                log.order_executed_at = timezone.now()
                log.completed_at = timezone.now()
                log.save()
                
                logger.info(f"✅ [CHECKPOINT 3 COMPLETE] Order executed successfully")
                return True
                
            finally:
                orderer.driver.quit()
                
        except Exception as e:
            log.current_stage = 'failed'
            log.status = 'failed'
            log.error_message = f"Order execution failed: {str(e)}"
            log.save()
            
            logger.exception(f"❌ [CHECKPOINT 3 FAILED]")
            raise
    
    def run_full_restock_workflow(self, coverage=None, log=None):
        """
        Run complete restock workflow with checkpoints.
        Uses database locks to prevent duplicate execution.
        """
        logger.info(f"Starting restock workflow for {self.storage.name}")
        
        if log is None:
            log = RestockLog.objects.create(
                storage=self.storage,
                status='processing',
                current_stage='pending',
                started_at=timezone.now()
            )
        else:
            with transaction.atomic():
                log = RestockLog.objects.select_for_update().get(id=log.id)
                log.retry_count += 1
                log.status = 'processing'
                log.save()
        
        try:
            # CHECKPOINT 1: Update stats
            with transaction.atomic():
                log.refresh_from_db()
                
                if log.stats_updated_at:
                    logger.info(f"[CHECKPOINT 1 SKIP] Stats already updated")
                else:
                    logger.info(f"[CHECKPOINT 1 START] Updating stats...")
                    self.update_product_stats_checkpoint(log)
            
            # CHECKPOINT 2: Calculate order
            with transaction.atomic():
                log.refresh_from_db()
                
                if log.order_calculated_at:
                    results = log.get_results()
                    orders_list = [
                        (o['cod'], o['var'], o['qty']) 
                        for o in results.get('orders', [])
                    ]
                    logger.info(f"[CHECKPOINT 2 SKIP] Order already calculated")
                else:
                    logger.info(f"[CHECKPOINT 2 START] Calculating order...")
                    orders_list = self.calculate_order_checkpoint(log, coverage)
            
            # CHECKPOINT 3: Execute order
            with transaction.atomic():
                log.refresh_from_db()
                
                if log.order_executed_at:
                    logger.info(f"[CHECKPOINT 3 SKIP] Order already executed")
                else:
                    logger.info(f"[CHECKPOINT 3 START] Executing order...")
                    self.execute_order_checkpoint(log, orders_list)
            
            logger.info(f"✅ Restock workflow completed successfully")
            return log
            
        except Exception as e:
            logger.exception(f"❌ Restock workflow failed")
            
            with transaction.atomic():
                log = RestockLog.objects.select_for_update().get(id=log.id)
                
                if log.status != 'completed':
                    log.status = 'failed'
                    log.save()
            
            if log.can_retry():
                logger.info(f"Will retry from checkpoint (attempt {log.retry_count + 1}/{log.max_retries})")
            
            raise
    
    def retry_from_checkpoint(self, log, coverage=None):
        """Retry workflow from last successful checkpoint"""
        logger.info(f"Retrying workflow from checkpoint for {self.storage.name}")
        
        # CHECKPOINT 1: Stats update
        if log.stats_updated_at:
            logger.info(f"[CHECKPOINT 1 SKIP] Stats already updated")
        else:
            logger.info(f"[CHECKPOINT 1 START] Updating stats...")
            self.update_product_stats_checkpoint(log)
        
        # CHECKPOINT 2: Order calculation
        if log.order_calculated_at:
            logger.info(f"[CHECKPOINT 2 SKIP] Order already calculated")
            results = log.get_results()
            orders_list = [
                (o['cod'], o['var'], o['qty'])
                for o in results.get('orders', [])
            ]
        else:
            logger.info(f"[CHECKPOINT 2 START] Calculating order...")
            orders_list = self.calculate_order_checkpoint(log, coverage)
        
        # CHECKPOINT 3: Order execution
        if log.order_executed_at:
            logger.info(f"[CHECKPOINT 3 SKIP] Order already executed")
        else:
            logger.info(f"[CHECKPOINT 3 START] Executing order...")
            self.execute_order_checkpoint(log, orders_list)
        
        logger.info(f"✅ Restock workflow completed")
        return log