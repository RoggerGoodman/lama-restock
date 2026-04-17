# LamApp/LamApp/celery.py
"""
Celery configuration for LamaRestock project.
This file initializes Celery and loads all configuration from Django settings.

All Celery settings (broker, time limits, workers, etc.) are configured in settings.py
with the CELERY_ prefix. This ensures a single source of truth for configuration.
"""
import os
from celery import Celery
from celery.schedules import crontab

# Set default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'LamApp.settings')

# Create Celery app
app = Celery('LamApp')

# Load ALL config from Django settings with 'CELERY' prefix
# This reads all CELERY_* settings from settings.py
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks in all installed apps
app.autodiscover_tasks()

# Configure Celery Beat schedule for automated tasks
#
# Daily timeline:
#   00:30  monthly-loss-zero-prepend  (1st of month only)
#   03:00  check-list-updates
#   03:30  backfill-ean
#   05:00  update-stats-morning       (DDT import)
#   05:30  VENSETAR sync              (store PC pushes sold data)
#   06:00  run-scheduled-orders
#   12:00  monthly-stock-snapshots    (1st of month only)
#   22:30  record-losses-nightly
#
app.conf.beat_schedule = {
    # 1st of month — 00:30
    'monthly-loss-zero-prepend': {
        'task': 'supermarkets.tasks.prepend_monthly_loss_zeros',
        'schedule': crontab(hour=0, minute=30, day_of_month='1'),
    },

    # 03:00 — refresh product lists for all scheduled storages
    'check-list-updates': {
        'task': 'supermarkets.tasks.run_scheduled_list_updates',
        'schedule': crontab(hour=3, minute=0),
    },

    # 03:30 — backfill missing EANs (runs after list update)
    'backfill-ean': {
        'task': 'supermarkets.tasks.backfill_ean_and_id_for_verified_products',
        'schedule': crontab(hour=3, minute=30),
    },

    # 05:00 — import DDT invoices and update product stats
    'update-stats-morning': {
        'task': 'supermarkets.tasks.update_stats_all_scheduled_storages',
        'schedule': crontab(hour=5, minute=0),
    },

    # 06:00 — place orders for storages scheduled today (VENSETAR sync arrives ~05:30)
    'run-scheduled-orders': {
        'task': 'supermarkets.tasks.run_scheduled_orders',
        'schedule': crontab(hour=6, minute=0),
    },

    # 1st of month — 12:00
    'monthly-stock-snapshots': {
        'task': 'supermarkets.tasks.create_monthly_stock_snapshots',
        'schedule': crontab(hour=12, minute=0, day_of_month='1'),
    },

    # 22:30 — record losses for all supermarkets
    # 'record-losses-nightly': {
    #     'task': 'supermarkets.tasks.record_losses_all_supermarkets',
    #     'schedule': crontab(hour=22, minute=30),
    # },
}

@app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery is working"""
    print(f'Request: {self.request!r}')