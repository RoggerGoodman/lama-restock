# LamApp/supermarkets/list_update_service.py - FIXED VERSION
"""
Service to handle automatic product list updates.
Downloads latest product list from PAC2000A and imports to database.
"""
import logging
from pathlib import Path
from django.conf import settings
from django.utils import timezone
from .models import Storage
from .scripts.web_lister import download_product_list
from .scripts.DatabaseManager import DatabaseManager
from .scripts.helpers import Helper

logger = logging.getLogger(__name__)


class ListUpdateService:
    """Handles automated product list updates"""
    
    def __init__(self, storage: Storage):
        self.storage = storage
        self.settore = storage.settore
        self.supermarket = storage.supermarket
        self.helper = Helper()
        
        # Pass supermarket name instead of db_path
        self.db = DatabaseManager(
            self.helper, 
            supermarket_name=self.supermarket.name
        )
        
        # Create temp directory for downloads
        self.download_dir = Path(settings.BASE_DIR) / 'temp_lists'
        self.download_dir.mkdir(exist_ok=True)

    def __enter__(self):
        """Enable 'with' statement usage"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Auto-close database connection when exiting 'with' block"""
        self.close()
        return False
    
    def download_list(self) -> str:
        """
        Download product list from PAC2000A.
        
        Returns:
            str: Path to downloaded Excel file
        """
        logger.info(f"Downloading product list for {self.storage.name}")
        
        file_path = download_product_list(
            username=self.supermarket.username,
            password=self.supermarket.password,
            storage_name=self.storage.name,
            download_dir=str(self.download_dir),
            headless=True
        )
        
        logger.info(f"Downloaded: {file_path}")
        return file_path
    
    def import_list(self, file_path: str):
        """
        Import product list to database.
        
        Args:
            file_path: Path to Excel file
        """
        logger.info(f"Importing products from {file_path}")
        
        self.db.import_from_excel(file_path, self.settore)
        
        logger.info(f"Import completed for {self.storage.name}")
    
    def update_and_import(self) -> dict:
        """
        Download and import product list.
        
        Returns:
            dict: Result with status and details
        """        
        try:
            # Download list
            file_path = self.download_list()
            
            # Import to database
            self.import_list(file_path)
            
            # Clean up file
            Path(file_path).unlink()
            
            # Update storage timestamp
            self.storage.last_list_update = timezone.now()
            self.storage.save()
            
            logger.info(f"List update completed for {self.storage.name}")
            
            # ✅ FIXED: Return proper result dict without undefined 'log'
            return {
                'success': True,
                'message': f'Product list updated successfully for {self.storage.name}',
                'storage_id': self.storage.id,
                'storage_name': self.storage.name,
                'file_path': file_path
            }
            
        except Exception as e:
            logger.exception(f"Error updating list for {self.storage.name}")
            
            # ✅ FIXED: Return error dict instead of trying to update non-existent log
            return {
                'success': False,
                'message': f'Error: {str(e)}',
                'storage_id': self.storage.id,
                'storage_name': self.storage.name
            }
    
    def close(self):
        """Clean up resources"""
        self.db.close()