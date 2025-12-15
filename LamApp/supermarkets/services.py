# LamApp/supermarkets/services.py
"""
Service layer to integrate existing DatabaseManager with Django models
"""
import re
from .scripts.DatabaseManager import DatabaseManager
from .scripts.decision_maker import DecisionMaker
from .scripts.helpers import Helper
from .scripts.scrapper import Scrapper
from .scripts.orderer import Orderer
from .models import Storage, RestockLog
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)


class RestockService:
    """Service to handle restock operations"""
    
    def get_db_path(self):
        """Get database path - now returns supermarket name for PostgreSQL."""
        return self.supermarket.name

    def __init__(self, storage: Storage):
        self.storage = storage
        self.settore = storage.settore
        self.supermarket = storage.supermarket
        self.helper = Helper()
        
        # NEW: Pass supermarket name instead of db_path
        self.db = DatabaseManager(
            self.helper, 
            supermarket_name=self.supermarket.name
        )
        
        self.decision_maker = DecisionMaker(self.db, self.helper, blacklist_set=self.get_blacklist_set())
    
    def get_blacklist_set(self):
        """
        Get all blacklisted products for this storage as a set of (cod, var) tuples.
        """
        blacklist_set = set()
        
        # Get all blacklists for this storage
        for blacklist in self.storage.blacklists.all():
            # Get all entries in this blacklist
            for entry in blacklist.entries.all():
                blacklist_set.add((entry.product_code, entry.product_var))
        
        logger.info(f"Loaded {len(blacklist_set)} blacklisted products for {self.storage.name}")
        return blacklist_set
    
    def run_restock_check(self, coverage=None):
        """
        Run the restock check for this storage
        Returns: RestockLog instance
        """
        log = RestockLog.objects.create(
            storage=self.storage,
            status='processing'
        )
        
        try:
            # Calculate coverage if not provided
            if coverage is None:
                schedule = self.storage.schedule
                today = timezone.now().weekday()  # 0=Monday, 6=Sunday
                coverage = schedule.calculate_coverage_for_day(today)
            
            log.coverage_used = coverage
            log.save()
            
            # Run decision maker
            self.decision_maker.decide_orders_for_settore(self.settore, coverage)
            
            # Get orders list
            orders_list = self.decision_maker.orders_list
            
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
            
            log.status = 'completed'
            log.completed_at = timezone.now()
            log.save()
            
            return log, orders_list
            
        except Exception as e:
            logger.exception(f"Error during restock check for {self.storage}")
            log.status = 'failed'
            log.error_message = str(e)
            log.completed_at = timezone.now()
            log.save()
            raise
    
    def execute_order(self, orders_list):
        """
        Execute the actual order placement using Orderer
        """
        orderer = Orderer(
            username=self.supermarket.username,
            password=self.supermarket.password
        )
        
        try:
            orderer.login()
            orderer.make_orders(self.storage.name, orders_list)
            return True
        except Exception as e:
            logger.exception(f"Error executing order for {self.storage}")
            raise
        finally:
            orderer.driver.quit()
    
    def import_products_from_excel(self, file_path):
        """Import products from Excel file"""
        self.db.import_from_excel(file_path, self.settore)
    
    def update_product_stats(self):
        """
        Update product statistics from PAC2000A
        """
        
        scrapper = Scrapper(
            username=self.supermarket.username,
            password=self.supermarket.password,
            helper=self.helper,
            db=self.db
        )
        
        try:
            scrapper.navigate()
            scrapper.init_product_stats_for_settore(self.settore)
        finally:
            scrapper.driver.quit()
    
    def verify_inventory(self, csv_file_path):
        """Verify inventory from CSV file"""
        from .scripts.inventory_reader import verify_stocks_from_excel
        verify_stocks_from_excel(self.db)
    
    def register_losses(self, loss_type, csv_file_path):
        """Register product losses (broken, expired, internal use)"""
        from .scripts.inventory_reader import verify_lost_stock_from_excel_combined
        verify_lost_stock_from_excel_combined(self.db)
    
    def close(self):
        """Clean up resources"""
        self.db.close()


class StorageService:
    """Service to manage storage discovery and setup"""
    
    @staticmethod
    def discover_storages(supermarket):
        """
        Use Finder to discover available storages for a supermarket
        Returns: list of storage names
        """
        from .scripts.finder import Finder 
        
        
        finder = Finder(
            username=supermarket.username,
            password=supermarket.password
        )
        
        try:
            finder.login()
            return finder.find_storages()
        finally:
            finder.driver.quit()
    
    @staticmethod
    def sync_storages(supermarket):
        """
        Sync storages from PAC2000A to Django database
        """
        storage_names = StorageService.discover_storages(supermarket)
        
        for name in storage_names:
            # Remove numeric prefix if present
            settore = re.sub(r'^[^ ]+\s*-?\s*', '', name)
            
            Storage.objects.get_or_create(
                supermarket=supermarket,
                name=name,
                settore = settore
            )