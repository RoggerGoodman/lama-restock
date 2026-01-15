from .scripts.DatabaseManager import DatabaseManager
from .scripts.helpers import Helper
from .models import Storage
import logging

logger = logging.getLogger(__name__)


class RestockService:
    """
    Base service for storage operations.
    Provides database access and basic helpers.
    
    Use this for:
    - Quick database queries
    - Manual adjustments
    - Simple operations
    """
    
    def __init__(self, storage: Storage):
        self.storage = storage
        self.settore = storage.settore
        self.supermarket = storage.supermarket
        self.helper = Helper()
        
        # Create database connection
        self.db = DatabaseManager(
            self.helper, 
            supermarket_name=self.supermarket.name
        )

    def __enter__(self):
        """Enable 'with' statement usage"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Auto-close database connection when exiting 'with' block"""
        self.close()
        return False

    def get_blacklist_set(self):
        """
        Get all blacklisted products for this storage as a set of (cod, var) tuples.
        Used by DecisionMaker and other components.
        """
        blacklist_set = set()
        
        for blacklist in self.storage.blacklists.all():
            for entry in blacklist.entries.all():
                blacklist_set.add((entry.product_code, entry.product_var))
        
        logger.info(f"Loaded {len(blacklist_set)} blacklisted products for {self.storage.name}")
        return blacklist_set
    
    def import_products_from_CSV(self, file_path):
        """Import products from CSV file"""
        self.db.import_from_CSV(file_path, self.settore)
    
    def close(self):
        """Clean up resources - CRITICAL for thread safety"""
        try:
            if hasattr(self, 'db') and self.db:
                self.db.close()
                logger.debug(f"Closed database connection for {self.storage.name}")
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")


# Optional: Keep StorageService for discovery operations
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
        import re
        from .models import Storage
        
        storage_names = StorageService.discover_storages(supermarket)
        
        for name in storage_names:
            # Remove numeric prefix if present
            settore = re.sub(r'^[^ ]+\s*-?\s*', '', name)
            
            Storage.objects.get_or_create(
                supermarket=supermarket,
                name=name,
                settore=settore
            )