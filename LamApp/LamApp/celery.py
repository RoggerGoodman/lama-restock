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
#   00:05  daily-sales-day-roll       (open today's slot in sales_sets)
#   00:30  monthly-loss-zero-prepend  (1st of month only)
#   00:35  monthly-bought-zero-prepend (1st of month only)
#   00:40  monthly-sold-zero-prepend  (1st of month only — see note below)
#   03:00  check-list-updates
#   03:30  backfill-ean
#   05:00  update-stats-morning       (DDT import — also saves pending calibration snapshot)
#   08:00  daily-calibration          (grades yesterday, which closed at the 21:30 sync)
#   08:30-21:30 real-time sales sync  (store PC pushes today's running totals, every 30 min)
#   */15   run-scheduled-orders       (fires each storage at its own configured time)
#   12:00  monthly-stock-snapshots    (1st of month only)
#   22:30  record-losses-nightly
#
app.conf.beat_schedule = {
    # 1st of month — 00:30
    'monthly-loss-zero-prepend': {
        'task': 'supermarkets.tasks.prepend_monthly_loss_zeros',
        'schedule': crontab(hour=0, minute=30, day_of_month='1'),
    },

    # 1st of month — 00:35
    'monthly-bought-zero-prepend': {
        'task': 'supermarkets.tasks.prepend_monthly_bought_zeros',
        'schedule': crontab(hour=0, minute=35, day_of_month='1'),
    },

    # 1st of month — 00:40. Must land before the day's first sync (08:30), so the 1st's
    # own sales go into the new month's slot.
    'monthly-sold-zero-prepend': {
        'task': 'supermarkets.tasks.prepend_monthly_sold_zeros',
        'schedule': crontab(hour=0, minute=40, day_of_month='1'),
    },

    # 00:05 daily — open today's slot in sales_sets before anything reads it.
    'daily-sales-day-roll': {
        'task': 'supermarkets.tasks.roll_sales_day_all_supermarkets',
        'schedule': crontab(hour=0, minute=5),
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

    # Every 15 min — order times are per storage and per weekday, so one daily trigger
    # cannot express them. OrderDispatch keeps each storage to once per day.
    'run-scheduled-orders': {
        'task': 'supermarkets.tasks.run_scheduled_orders',
        'schedule': crontab(minute='*/15'),
    },

    # 08:00 — complete calibration reports against yesterday's now-final sales
    'daily-calibration': {
        'task': 'supermarkets.tasks.run_daily_calibration',
        'schedule': crontab(hour=8, minute=0),
    },

    # 1st of month — 12:00
    'monthly-stock-snapshots': {
        'task': 'supermarkets.tasks.create_monthly_stock_snapshots',
        'schedule': crontab(hour=12, minute=0, day_of_month='1'),
    },

    # 22:30 — record losses for all supermarkets
    'record-losses-nightly': {
        'task': 'supermarkets.tasks.record_losses_all_supermarkets',
        'schedule': crontab(hour=22, minute=30),
    },

    # Sunday 01:00 — delete old restock logs (keep last 10 per storage, max 6 months)
    'cleanup-old-restock-logs': {
        'task': 'supermarkets.tasks.cleanup_old_restock_logs',
        'schedule': crontab(hour=1, minute=0, day_of_week='sunday'),
    },

    # Sunday 01:05 — delete old sales sync logs (keep last 30 per supermarket, max 90 days)
    'cleanup-old-sales-sync-logs': {
        'task': 'supermarkets.tasks.cleanup_old_sales_sync_logs',
        'schedule': crontab(hour=1, minute=5, day_of_week='sunday'),
    },

    # Sunday 01:10 — delete stale recipe cost alerts (read >30d, unread >90d)
    'cleanup-old-recipe-cost-alerts': {
        'task': 'supermarkets.tasks.cleanup_old_recipe_cost_alerts',
        'schedule': crontab(hour=1, minute=10, day_of_week='sunday'),
    },

    # Sunday 01:15 — delete per-order decision_maker log files older than 7 days
    'cleanup-old-decision-maker-logs': {
        'task': 'supermarkets.tasks.cleanup_old_decision_maker_logs',
        'schedule': crontab(hour=1, minute=15, day_of_week='sunday'),
    },
}

@app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery is working"""
    print(f'Request: {self.request!r}')