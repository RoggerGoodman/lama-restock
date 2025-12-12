# LamApp/supermarkets/automation_services.py
"""
Automated services with checkpoint-based recovery.
Each stage saves progress, allowing retry from last successful checkpoint.
"""
import logging
from pathlib import Path
from django.conf import settings
from django.utils import timezone

from .models import Storage, RestockLog
from .scripts.DatabaseManager import DatabaseManager
from .scripts.decision_maker import DecisionMaker
from .scripts.helpers import Helper
from .scripts.inventory_scrapper import Inventory_Scrapper
from .scripts.inventory_reader import verify_lost_stock_from_excel_combined
from .scripts.scrapper import Scrapper
from .scripts.orderer import Orderer

logger = logging.getLogger(__name__)


class AutomatedRestockService:
    """
    Handles automated restock operations with checkpoint recovery.
    
    Each operation is divided into stages:
    1. Update product stats (scrapper)
    2. Calculate order (decision maker)
    3. Execute order (orderer)
    
    If a stage fails, it can be retried from that checkpoint.
    """
    
    def __init__(self, storage: Storage):
        self.storage = storage
        self.settore = storage.settore
        self.supermarket = storage.supermarket
        self.helper = Helper()
        
        # Get database path for this supermarket
        self.db_path = self.get_db_path()
        self.db = DatabaseManager(self.helper, db_path=self.db_path)

    def close(self):
        """Clean up database connection - CRITICAL for thread safety"""
        try:
            if hasattr(self, 'db') and self.db:
                self.db.close()
                logger.debug(f"Closed database connection for {self.storage.name}")
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")
    
    def get_blacklist_set(self):
        """Get all blacklisted products for this storage as a set of (cod, var) tuples."""
        from .models import BlacklistEntry
        
        blacklist_set = set()
        
        for blacklist in self.storage.blacklists.all():
            for entry in blacklist.entries.all():
                blacklist_set.add((entry.product_code, entry.product_var))
        
        logger.info(f"Loaded {len(blacklist_set)} blacklisted products for {self.storage.name}")
        return blacklist_set
    
    def get_db_path(self):
        """Get database path for this storage's supermarket"""
        db_dir = Path(settings.BASE_DIR) / 'databases'
        db_dir.mkdir(exist_ok=True)
        
        safe_name = "".join(c for c in self.supermarket.name if c.isalnum() or c in (' ', '_')).strip()
        safe_name = safe_name.replace(' ', '_')
        
        return str(db_dir / f"{safe_name}.db")
    
    def record_losses(self):
        """
        Record product losses by downloading and processing inventory files.
        Should run the day before orders at 22:30.
        """
        logger.info(f"Starting loss recording for {self.supermarket.name}")
        
        try:
            inv_scrapper = Inventory_Scrapper()
            
            try:
                inv_scrapper.login()
                inv_scrapper.inventory()
                
                target1 = "ROTTURE"
                target2 = "SCADUTO"
                target3 = "UTILIZZO INTERNO"
                
                logger.info("Downloading loss inventory files...")
                inv_scrapper.downloader(target1)
                inv_scrapper.downloader(target2)
                inv_scrapper.downloader(target3)
                
                logger.info("Processing loss files...")
                verify_lost_stock_from_excel_combined(self.db)
                
                logger.info(f"Loss recording completed for {self.supermarket.name}")
                return True
                
            finally:
                inv_scrapper.driver.quit()
                
        except Exception as e:
            logger.exception(f"Error recording losses for {self.supermarket.name}")
            raise
    
    def update_product_stats_checkpoint(self, log: RestockLog):
        """
        CHECKPOINT 1: Update product statistics from PAC2000A.
        THREAD-SAFE: Creates fresh DB connection in this thread.
        Also runs auto-purge check after update!
        Returns True on success, raises exception on failure.
        """
        logger.info(f"[CHECKPOINT 1] Updating product stats for {self.storage.name}")
        
        # Update log stage
        log.current_stage = 'updating_stats'
        log.save()
        
        try:
            # CRITICAL: Create fresh DB connection in THIS thread
            from .scripts.DatabaseManager import DatabaseManager
            thread_db = DatabaseManager(self.helper, db_path=self.db_path)
            
            scrapper = Scrapper(self.helper, thread_db)
            
            try:
                scrapper.navigate()
                scrapper.init_product_stats_for_settore(self.settore)
                
                # Auto-purge check after successful update
                logger.info(f"[AUTO-PURGE] Checking for products ready to purge...")
                purged_products = thread_db.check_and_purge_flagged()
                
                if purged_products:
                    logger.info(f"[AUTO-PURGE] Purged {len(purged_products)} products with zero stock")
                    for result in purged_products:
                        logger.info(f"  - {result['cod']}.{result['v']}: {result['message']}")
                else:
                    logger.info(f"[AUTO-PURGE] No products ready for purging")
                
                # Mark checkpoint as complete
                log.current_stage = 'stats_updated'
                log.stats_updated_at = timezone.now()
                log.save()
                
                logger.info(f" [CHECKPOINT 1 COMPLETE] Stats updated for {self.storage.name}")
                return True
                
            finally:
                scrapper.driver.quit()
                thread_db.close()  # CRITICAL: Close thread-specific connection
                
        except Exception as e:
            log.current_stage = 'failed'
            log.error_message = f"Stats update failed: {str(e)}"
            log.save()
            
            logger.exception(f" [CHECKPOINT 1 FAILED] Error updating stats for {self.storage.name}")
            raise
    
    def calculate_order_checkpoint(self, log: RestockLog, coverage=None):
        """
        CHECKPOINT 2: Calculate what needs to be ordered.
        THREAD-SAFE: Uses fresh DB connection.
        Includes skipped products in results!
        Returns orders_list on success, raises exception on failure.
        """
        logger.info(f"[CHECKPOINT 2] Calculating order for {self.storage.name}")
        
        # Update log stage
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
            
            # Get blacklist
            blacklist_set = self.get_blacklist_set()
            
            # CRITICAL: Create fresh DB connection for THIS thread
            from .scripts.DatabaseManager import DatabaseManager
            thread_db = DatabaseManager(self.helper, db_path=self.db_path)
            
            try:
                # Initialize decision maker with thread-specific DB
                decision_maker = DecisionMaker(
                    self.helper, 
                    db_path=self.db_path,
                    blacklist_set=blacklist_set
                )
                
                # Run decision logic
                logger.info(f"Running decision maker with coverage={coverage} for settore={self.settore}")
                decision_maker.decide_orders_for_settore(self.settore, coverage)
                
                # Get orders list AND skipped products
                orders_list = decision_maker.orders_list
                skipped_products = decision_maker.skipped_products
                
                # Update log statistics
                log.total_products = len(thread_db.get_all_stats_by_settore(self.settore))
                log.products_ordered = len(orders_list)
                log.total_packages = sum(qty for _, _, qty in orders_list)
                
                # Store detailed results INCLUDING skipped products
                log.set_results({
                    'orders': [
                        {'cod': cod, 'var': var, 'qty': qty}
                        for cod, var, qty in orders_list
                    ],
                    'skipped_products': skipped_products,
                    'settore': self.settore,
                    'coverage': float(coverage)
                })
                
                # Mark checkpoint as complete
                log.current_stage = 'order_calculated'
                log.order_calculated_at = timezone.now()
                log.save()
                
                logger.info(
                    f" [CHECKPOINT 2 COMPLETE] Order calculated: "
                    f"{len(orders_list)} products ordered, "
                    f"{len(skipped_products)} products skipped, "
                    f"{log.total_packages} packages"
                )
                return orders_list
                
            finally:
                decision_maker.close()
                thread_db.close()  # CRITICAL: Close thread-specific connection
            
        except Exception as e:
            log.current_stage = 'failed'
            log.error_message = f"Order calculation failed: {str(e)}"
            log.save()
            
            logger.exception(f" [CHECKPOINT 2 FAILED] Error calculating order for {self.storage.name}")
            raise
    
    def execute_order_checkpoint(self, log: RestockLog, orders_list):
        """
        CHECKPOINT 3: Execute the order in PAC2000A.
        NOW: Merges skipped products from stage 3 with stage 2 skipped list!
        Returns True on success, raises exception on failure.
        """
        logger.info(f"[CHECKPOINT 3] Executing order for {self.storage.name}")
        
        # Update log stage
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
            
            logger.info(f"Executing order with {len(orders_list)} items for settore={self.settore}")
            orderer = Orderer()
            
            try:
                orderer.login()
                successful_orders, stage3_skipped = orderer.make_orders(self.storage.name, orders_list)
                
                # NEW: Merge stage 3 skipped products with existing results
                results = log.get_results()
                stage2_skipped = results.get('skipped_products', [])
                
                # Combine both skip lists
                all_skipped = stage2_skipped + stage3_skipped
                
                logger.info(
                    f"Total skipped: {len(all_skipped)} "
                    f"(Stage 2: {len(stage2_skipped)}, Stage 3: {len(stage3_skipped)})"
                )
                
                # Update results with merged skip list
                results['skipped_products'] = all_skipped
                
                # Update successful orders count
                results['orders'] = [
                    {'cod': cod, 'var': var, 'qty': qty}
                    for cod, var, qty in successful_orders
                ]
                
                log.products_ordered = len(successful_orders)
                log.total_packages = sum(qty for _, _, qty in successful_orders)
                log.set_results(results)
                
                # Mark checkpoint as complete
                log.current_stage = 'completed'
                log.status = 'completed'
                log.order_executed_at = timezone.now()
                log.completed_at = timezone.now()
                log.save()
                
                logger.info(
                    f" [CHECKPOINT 3 COMPLETE] Order executed for {self.storage.name}: "
                    f"{len(successful_orders)} successful, {len(all_skipped)} total skipped"
                )
                return True
                
            finally:
                orderer.driver.quit()
                
        except Exception as e:
            log.current_stage = 'failed'
            log.status = 'failed'
            log.error_message = f"Order execution failed: {str(e)}"
            log.save()
            
            logger.exception(f" [CHECKPOINT 3 FAILED] Error executing order for {self.storage.name}")
            raise
    
    def run_full_restock_workflow(self, coverage=None, log=None):
        """
        Run complete restock workflow with checkpoint recovery.
        
        If a log is provided, it will attempt to resume from the last checkpoint.
        Otherwise, creates a new log and runs from the beginning.
        
        Returns:
            RestockLog instance with results
        """
        logger.info(f"Starting restock workflow for {self.storage.name}")
        
        # Create new log if not provided
        if log is None:
            log = RestockLog.objects.create(
                storage=self.storage,
                status='processing',
                current_stage='pending',
                started_at=timezone.now()
            )
            logger.info(f"Created new RestockLog #{log.id}")
        else:
            log.retry_count += 1
            log.status = 'processing'
            log.save()
            logger.info(f"Resuming RestockLog #{log.id} from stage: {log.current_stage} (retry {log.retry_count})")
        
        try:
            # CHECKPOINT 1: Update stats
            if log.current_stage in ['pending', 'updating_stats']:
                self.update_product_stats_checkpoint(log)
            else:
                logger.info(f"[CHECKPOINT 1 SKIP] Stats already updated at {log.stats_updated_at}")
            
            # CHECKPOINT 2: Calculate order
            if log.current_stage in ['stats_updated', 'calculating_order']:
                orders_list = self.calculate_order_checkpoint(log, coverage)
            else:
                # Retrieve orders from log
                results = log.get_results()
                orders_list = [
                    (o['cod'], o['var'], o['qty'])
                    for o in results.get('orders', [])
                ]
                logger.info(f"[CHECKPOINT 2 SKIP] Order already calculated at {log.order_calculated_at}")
            
            # CHECKPOINT 3: Execute order
            if log.current_stage in ['order_calculated', 'executing_order']:
                self.execute_order_checkpoint(log, orders_list)
            else:
                logger.info(f"[CHECKPOINT 3 SKIP] Order already executed at {log.order_executed_at}")
            
            logger.info(f" Restock workflow completed successfully for {self.storage.name}")
            return log
            
        except Exception as e:
            logger.exception(f" Restock workflow failed for {self.storage.name}")
            
            # Check if we can retry
            if log.can_retry():
                logger.info(f"Will retry from checkpoint {log.current_stage} (attempt {log.retry_count + 1}/{log.max_retries})")
            else:
                logger.error(f"Max retries ({log.max_retries}) reached for RestockLog #{log.id}")
                log.status = 'failed'
                log.save()
            
            raise
    
    def retry_from_checkpoint(self, log, coverage=None):
        """Retry workflow from last successful checkpoint"""
        from django.utils import timezone
        
        logger.info(f"Retrying workflow from checkpoint for {self.storage.name} with coverage={coverage}")
        
        # Checkpoint 1: Stats update
        if log.stats_updated_at:
            logger.info(f"[CHECKPOINT 1 SKIP] Stats already updated at {log.stats_updated_at}")
        else:
            logger.info(f"[CHECKPOINT 1 START] Updating stats...")
            self.update_product_stats_checkpoint(log)
        
        # Checkpoint 2: Order calculation
        if log.order_calculated_at:
            logger.info(f"[CHECKPOINT 2 SKIP] Order already calculated at {log.order_calculated_at}")
            # Retrieve existing orders from log
            results = log.get_results()
            orders_list = [
                (o['cod'], o['var'], o['qty'])
                for o in results.get('orders', [])
            ]
        else:
            logger.info(f"[CHECKPOINT 2 START] Calculating order with coverage={coverage}...")
            orders_list = self.calculate_order_checkpoint(log, coverage)
        
        # Checkpoint 3: Order execution
        if log.order_executed_at:
            logger.info(f"[CHECKPOINT 3 SKIP] Order already executed at {log.order_executed_at}")
        else:
            logger.info(f"[CHECKPOINT 3 START] Executing order...")
            self.execute_order_checkpoint(log, orders_list)
        
        logger.info(f"[SUCCESS] Restock workflow completed for {self.storage.name}")
        return log