# LamApp/supermarkets/automation_services.py
"""
Automated services for scheduled restock operations.
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
    Handles automated restock operations triggered by schedule.
    """
    
    def __init__(self, storage: Storage):
        self.storage = storage
        self.settore = storage.settore  # FIX: Use storage.settore, not storage.name
        self.supermarket = storage.supermarket
        self.helper = Helper()
        
        # Get database path for this supermarket
        self.db_path = self.get_db_path()
        self.db = DatabaseManager(self.helper, db_path=self.db_path)
    
    def get_blacklist_set(self):
        """
        Get all blacklisted products for this storage as a set of (cod, var) tuples.
        """
        from .models import BlacklistEntry
        
        blacklist_set = set()
        
        # Get all blacklists for this storage
        for blacklist in self.storage.blacklists.all():
            # Get all entries in this blacklist
            for entry in blacklist.entries.all():
                blacklist_set.add((entry.product_code, entry.product_var))
        
        logger.info(f"Loaded {len(blacklist_set)} blacklisted products for {self.storage.name}")
        return blacklist_set
    
    def get_db_path(self):
        """Get database path for this storage's supermarket"""
        db_dir = Path(settings.BASE_DIR) / 'databases'
        db_dir.mkdir(exist_ok=True)
        
        # Sanitize supermarket name for filename
        safe_name = "".join(c for c in self.supermarket.name if c.isalnum() or c in (' ', '_')).strip()
        safe_name = safe_name.replace(' ', '_')
        
        return str(db_dir / f"{safe_name}.db")
    
    def record_losses(self):
        """
        Record product losses by downloading and processing inventory files.
        Should run the day before orders at 22:30.
        """
        logger.info(f"Starting loss recording for {self.storage.name} (settore: {self.settore})")
        
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
                
                logger.info(f"Loss recording completed for {self.storage.name}")
                return True
                
            finally:
                inv_scrapper.driver.quit()
                
        except Exception as e:
            logger.exception(f"Error recording losses for {self.storage.name}")
            raise
    
    def update_product_stats(self):
        """
        Update product statistics from PAC2000A.
        Should run on order day before making the order.
        """
        logger.info(f"Starting product stats update for {self.storage.name} (settore: {self.settore})")
        
        try:
            scrapper = Scrapper(self.helper, self.db)
            
            try:
                scrapper.navigate()
                scrapper.init_product_stats_for_settore(self.settore)
                
                logger.info(f"Product stats updated for {self.storage.name}")
                return True
                
            finally:
                scrapper.driver.quit()
                
        except Exception as e:
            logger.exception(f"Error updating product stats for {self.storage.name}")
            raise
    
    def generate_and_execute_order(self, coverage=None):
        """
        Generate order decisions and execute the order.
        
        Args:
            coverage: Number of days to cover. If None, uses schedule calculation.
        
        Returns:
            RestockLog instance with results
        """
        logger.info(f"Starting order generation for {self.storage.name} (settore: {self.settore})")
        
        log = RestockLog.objects.create(
            storage=self.storage,
            status='processing',
            started_at=timezone.now()
        )
        
        try:
            # Calculate coverage if not provided
            if coverage is None:
                schedule = self.storage.schedule
                today = timezone.now().weekday()
                coverage = schedule.calculate_coverage_for_day(today)
            
            log.coverage_used = coverage
            log.save()
            
            # Get blacklist for this storage
            blacklist_set = self.get_blacklist_set()
            
            # Initialize decision maker with blacklist
            decision_maker = DecisionMaker(
                self.helper, 
                db_path=self.db_path,
                blacklist_set=blacklist_set
            )
            
            # Run decision logic
            logger.info(f"Running decision maker with coverage={coverage} for settore={self.settore}")
            decision_maker.decide_orders_for_settore(self.settore, coverage)
            
            # Get orders list
            orders_list = decision_maker.orders_list
            
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
                'settore': self.settore,
                'coverage': float(coverage)
            })
            log.save()
            
            # Now execute the order
            if orders_list:
                logger.info(f"Executing order with {len(orders_list)} items for settore={self.settore}")
                orderer = Orderer()
                
                try:
                    orderer.login()
                    # Use storage.name for the orderer (this is what it expects for the dropdown)
                    orderer.make_orders(self.storage.name, orders_list)
                    
                    log.status = 'completed'
                    log.completed_at = timezone.now()
                    log.save()
                    
                    logger.info(f"Order completed successfully for {self.storage.name}")
                    
                finally:
                    orderer.driver.quit()
            else:
                log.status = 'completed'
                log.completed_at = timezone.now()
                log.save()
                logger.info(f"No items to order for {self.storage.name}")
            
            return log
            
        except Exception as e:
            logger.exception(f"Error generating/executing order for {self.storage.name}")
            log.status = 'failed'
            log.error_message = str(e)
            log.completed_at = timezone.now()
            log.save()
            raise
    
    def run_full_restock_workflow(self, coverage=None):
        """
        Run the complete restock workflow:
        1. Update product stats
        2. Generate and execute order
        
        This is what gets called by the scheduler on order day.
        """
        logger.info(f"Starting full restock workflow for {self.storage.name} (settore: {self.settore})")
        
        try:
            # Step 1: Update stats
            self.update_product_stats()
            
            # Step 2: Generate and execute order
            log = self.generate_and_execute_order(coverage)
            
            logger.info(f"Full restock workflow completed for {self.storage.name}")
            return log
            
        except Exception as e:
            logger.exception(f"Full restock workflow failed for {self.storage.name}")
            raise
    
    def close(self):
        """Clean up resources"""
        self.db.close()