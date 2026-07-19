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
        self.db = DatabaseManager(supermarket_name=self.supermarket.name)

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


def delete_blacklist_entries_for_purged(purged_products, *, storage=None, supermarket=None):
    """
    Remove BlacklistEntry rows left behind after purge_product() clears a
    product's stats. purge_product() intentionally keeps the `products` row
    (losses reference it) and has no notion of Storage/Blacklist, so callers
    must reconcile the blacklist tables themselves once a purge completes.

    purged_products: list of dicts as returned by DatabaseManager.purge_product()
    (or a list of such dicts from purge_obsolete_products/check_and_purge_flagged).
    Pass `storage` when the purge ran against a single Storage, or `supermarket`
    when it ran across all storages of a Supermarket (e.g. purge_obsolete_products).
    """
    from django.db.models import Q
    from .models import BlacklistEntry

    codes = [(p['cod'], p['v']) for p in purged_products if p.get('action') == 'purged']
    if not codes:
        return 0

    if storage is not None:
        qs = BlacklistEntry.objects.filter(blacklist__storage=storage)
    elif supermarket is not None:
        qs = BlacklistEntry.objects.filter(blacklist__storage__supermarket=supermarket)
    else:
        raise ValueError("delete_blacklist_entries_for_purged requires storage or supermarket")

    code_filter = Q()
    for cod, v in codes:
        code_filter |= Q(product_code=cod, product_var=v)

    deleted, _ = qs.filter(code_filter).delete()
    if deleted:
        logger.info(f"Removed {deleted} stale blacklist entr{'y' if deleted == 1 else 'ies'} for purged products")
    return deleted


# Optional: Keep StorageService for discovery operations
class StorageService:
    """Service to manage storage discovery and setup"""
    
    @staticmethod
    def discover_storages(supermarket):
        """
        Use Finder to discover available storages for a supermarket.
        Returns: list of (name, id_cod_mag) tuples
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
        Sync storages from Dropzone to Django database.
        Creates new storages and updates id_cod_mag on existing ones.
        """
        import re
        from .models import Storage

        storage_tuples = StorageService.discover_storages(supermarket)

        for name, id_cod_mag in storage_tuples:
            # Remove numeric prefix if present
            settore = re.sub(r'^[^ ]+\s*-?\s*', '', name)

            storage, created = Storage.objects.get_or_create(
                supermarket=supermarket,
                name=name,
                defaults={'settore': settore, 'id_cod_mag': id_cod_mag}
            )
            if not created:
                # Update id_cod_mag on existing storages
                storage.id_cod_mag = id_cod_mag
                storage.save(update_fields=['id_cod_mag'])