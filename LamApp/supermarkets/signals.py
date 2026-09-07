import logging
from django.contrib.auth.signals import user_logged_in
from django.db.models.signals import post_save
from django.dispatch import receiver

logger = logging.getLogger(__name__)


@receiver(user_logged_in)
def refresh_demo_on_login(sender, request, user, **kwargs):
    """Keep the demo looking freshly synced without any beat task: rebuild its
    data on the demo user's login, throttled to once per day inside the helper."""
    from .demo import is_demo_user
    if not is_demo_user(user):
        return
    from .demo_seed import refresh_if_stale
    refresh_if_stale()


@receiver(post_save, sender='supermarkets.Storage')
def create_storage_schema(sender, instance, created, **kwargs):
    if not created:
        return
    from .scripts.DatabaseManager import DatabaseManager
    db = None
    try:
        db = DatabaseManager(supermarket_name=instance.supermarket.name)
        db.create_tables()
        logger.info(f"Schema created for storage '{instance.name}' (supermarket: {instance.supermarket.name})")
    except Exception:
        logger.exception(f"Failed to create schema for storage '{instance.name}'")
    finally:
        if db:
            db.close()
