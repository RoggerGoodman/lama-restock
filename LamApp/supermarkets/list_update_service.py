# LamApp/supermarkets/list_update_service.py
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
        
        # NEW: Pass supermarket name instead of db_path
        self.db = DatabaseManager(
            self.helper, 
            supermarket_name=self.supermarket.name
        )
        
        # Create temp directory for downloads
        self.download_dir = Path(settings.BASE_DIR) / 'temp_lists'
        self.download_dir.mkdir(exist_ok=True)
    
    def get_db_path(self):
        """Get database path for this storage's supermarket"""
        db_dir = Path(settings.BASE_DIR) / 'databases'
        db_dir.mkdir(exist_ok=True)
        
        safe_name = "".join(
            c for c in self.supermarket.name 
            if c.isalnum() or c in (' ', '_')
        ).strip().replace(' ', '_')
        
        return str(db_dir / f"{safe_name}.db")
    
    def should_update(self) -> bool:
        """
        Check if product list should be updated.
        
        Returns:
            bool: True if update is needed
        """
        try:
            schedule = self.storage.list_update_schedule
            
            # Check if enough time has passed since last update
            if self.storage.last_list_update:
                days_since = (timezone.now() - self.storage.last_list_update).days
                
                if schedule.frequency == 'weekly':
                    return days_since >= 7
                elif schedule.frequency == 'biweekly':
                    return days_since >= 14
                elif schedule.frequency == 'monthly':
                    return days_since >= 30
            else:
                # Never updated, do it now
                return True
                
        except:
            # No schedule configured
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
        
        self.db.import_from_CSV(file_path, self.settore)
        
        logger.info(f"Import completed for {self.storage.name}")
    
    def update_and_import(self) -> dict:
        """
        Download and import product list.
        
        Returns:
            dict: Result with status and details
        """
        from .models import ListUpdateLog
        
        log = ListUpdateLog.objects.create(
            storage=self.storage,
            status='processing',
            started_at=timezone.now()
        )
        
        try:
            # Download list
            file_path = self.download_list()
            
            # Import to database
            self.import_list(file_path)
            
            # Clean up file
            Path(file_path).unlink()
            
            # Update storage
            self.storage.last_list_update = timezone.now()
            self.storage.save()
            
            # Update log
            log.status = 'completed'
            log.completed_at = timezone.now()
            log.file_path = file_path
            log.save()
            
            logger.info(f"List update completed for {self.storage.name}")
            
            return {
                'success': True,
                'message': 'Product list updated successfully',
                'log': log
            }
            
        except Exception as e:
            logger.exception(f"Error updating list for {self.storage.name}")
            
            log.status = 'failed'
            log.error_message = str(e)
            log.completed_at = timezone.now()
            log.save()
            
            return {
                'success': False,
                'message': f'Error: {str(e)}',
                'log': log
            }
    
    def close(self):
        """Clean up resources"""
        self.db.close()