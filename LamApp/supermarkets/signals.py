import logging
from django.db.models.signals import post_save
from django.dispatch import receiver

logger = logging.getLogger(__name__)


@receiver(post_save, sender='supermarkets.Storage')
def create_storage_schema(sender, instance, created, **kwargs):
    if not created:
        return
    from .scripts.helpers import Helper
    from .scripts.DatabaseManager import DatabaseManager
    db = None
    try:
        db = DatabaseManager(Helper(), supermarket_name=instance.supermarket.name)
        db.create_tables()
        logger.info(f"Schema created for storage '{instance.name}' (supermarket: {instance.supermarket.name})")
    except Exception:
        logger.exception(f"Failed to create schema for storage '{instance.name}'")
    finally:
        if db:
            db.close()
