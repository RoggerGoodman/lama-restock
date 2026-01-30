# LamApp/supermarkets/list_update_service.py
"""
Service to handle automatic product list updates.
Downloads latest product list from PAC2000A and imports to database.
Also detects cost changes that affect recipe margins.
"""
import logging
from decimal import Decimal
from pathlib import Path
from django.conf import settings
from django.utils import timezone
from .models import Storage, RecipeProductItem, RecipeCostAlert
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
            str: Path to downloaded CSV file
        """
        logger.info(f"Downloading product list for {self.storage.name}")
        
        file_path = download_product_list(
            username=self.supermarket.username,
            password=self.supermarket.password,
            storage_name=self.storage.name,
            download_dir=str(self.download_dir),
            id_cod_mag=self.storage.id_cod_mag,
            id_cliente=self.supermarket.id_cliente,
            id_azienda=self.supermarket.id_azienda,
            id_marchio=self.supermarket.id_marchio,
            id_clienti_canale=self.supermarket.id_clienti_canale,
            id_clienti_area=self.supermarket.id_clienti_area,
            headless=True
        )
        
        logger.info(f"Downloaded: {file_path}")
        return file_path
    
    def import_list(self, file_path: str):
        """
        Import product list to database.
        
        Args:
            file_path: Path to CSV file
        """
        logger.info(f"Importing products from {file_path}")
        
        self.db.import_from_CSV(file_path, self.settore)
        
        logger.info(f"Import completed for {self.storage.name}")
    
    def _get_recipe_product_costs(self) -> dict:
        """
        Get current costs for all products used in recipes for this supermarket.

        Returns:
            dict: {(cod, var): (cost_std, description)} mapping
        """
        # Get all product items used in recipes for this supermarket
        recipe_items = RecipeProductItem.objects.filter(
            recipe__supermarket=self.supermarket
        ).values_list('product_code', 'product_var', 'cached_description').distinct()

        if not recipe_items:
            return {}

        # Query current costs from external DB
        costs = {}
        cur = self.db.cursor()

        for cod, var, description in recipe_items:
            try:
                cur.execute(
                    "SELECT cost_std FROM economics WHERE cod = %s AND v = %s",
                    (cod, var)
                )
                row = cur.fetchone()
                if row and row['cost_std'] is not None:
                    costs[(cod, var)] = (Decimal(str(row['cost_std'])), description or f"Product {cod}.{var}")
            except Exception as e:
                logger.warning(f"Could not get cost for {cod}.{var}: {e}")
                continue

        return costs

    def _create_cost_alerts(self, old_costs: dict, new_costs: dict):
        """
        Compare old and new costs, create alerts for changed products.

        Args:
            old_costs: dict of {(cod, var): (old_cost_std, description)}
            new_costs: dict of {(cod, var): (new_cost_std, description)}

        Returns:
            int: Number of alerts created
        """
        alerts_created = 0

        for (cod, var), (old_cost, description) in old_costs.items():
            new_data = new_costs.get((cod, var))
            if new_data is None:
                continue

            new_cost = new_data[0]

            # Skip if cost hasn't changed
            if abs(float(new_cost) - float(old_cost)) < 0.01:
                continue

            # Find all recipes affected by this cost change
            affected_items = RecipeProductItem.objects.filter(
                recipe__supermarket=self.supermarket,
                product_code=cod,
                product_var=var
            ).select_related('recipe')

            for item in affected_items:
                recipe = item.recipe

                try:
                    # Calculate cost difference for this item
                    old_item_cost = float(old_cost) * (item.use_percentage / 100)
                    new_item_cost = float(new_cost) * (item.use_percentage / 100)
                    cost_difference = new_item_cost - old_item_cost

                    # Current recipe cost (with new costs)
                    new_recipe_cost = recipe.get_total_cost()
                    old_recipe_cost = new_recipe_cost - cost_difference

                    # Calculate margins
                    selling_price = float(recipe.selling_price) if recipe.selling_price else 0
                    if selling_price > 0:
                        old_margin_pct = ((selling_price - old_recipe_cost) / selling_price) * 100
                        new_margin_pct = ((selling_price - new_recipe_cost) / selling_price) * 100
                    else:
                        old_margin_pct = 0
                        new_margin_pct = 0

                    # Create alert
                    RecipeCostAlert.objects.create(
                        recipe=recipe,
                        product_code=cod,
                        product_var=var,
                        product_description=description,
                        old_cost=old_cost,
                        new_cost=new_cost,
                        old_recipe_cost=Decimal(str(round(old_recipe_cost, 2))),
                        new_recipe_cost=Decimal(str(round(new_recipe_cost, 2))),
                        old_margin_pct=Decimal(str(round(old_margin_pct, 2))),
                        new_margin_pct=Decimal(str(round(new_margin_pct, 2)))
                    )

                    alerts_created += 1
                    logger.info(
                        f"Created cost alert: recipe '{recipe.name}', "
                        f"{cod}.{var} changed {old_cost} -> {new_cost}"
                    )

                    # Update cached cost in RecipeProductItem
                    item.cached_cost_std = new_cost
                    item.save(update_fields=['cached_cost_std'])

                except Exception as e:
                    logger.exception(f"Error creating alert for {recipe.name}: {e}")

        if alerts_created > 0:
            logger.info(f"Created {alerts_created} recipe cost alerts")

        return alerts_created

    def update_and_import(self) -> dict:
        """
        Download and import product list.
        Detects cost changes that affect recipe margins and creates alerts.

        Returns:
            dict: Result with status and details
        """
        try:
            # Step 1: Get costs BEFORE import
            old_costs = self._get_recipe_product_costs()

            # Step 2: Download and import
            file_path = self.download_list()
            self.import_list(file_path)

            # Step 3: Get costs AFTER import
            new_costs = self._get_recipe_product_costs()

            # Step 4: Compare and create alerts
            alerts_created = 0
            if old_costs:
                alerts_created = self._create_cost_alerts(old_costs, new_costs)

            # Clean up
            Path(file_path).unlink()
            self.storage.last_list_update = timezone.now()
            self.storage.save()

            logger.info(f"List update completed for {self.storage.name}")

            return {
                'success': True,
                'message': f'Product list updated successfully for {self.storage.name}',
                'storage_id': self.storage.id,
                'storage_name': self.storage.name,
                'file_path': file_path,
                'recipe_alerts_created': alerts_created
            }

        except Exception as e:
            logger.exception(f"Error updating list for {self.storage.name}")

            return {
                'success': False,
                'message': f'Error: {str(e)}',
                'storage_id': self.storage.id,
                'storage_name': self.storage.name
            }

    def close(self):
        """Clean up resources"""
        self.db.close()