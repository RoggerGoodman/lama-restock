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
app.conf.beat_schedule = {
    # Loss recording at 22:30 every day
    'record-losses-nightly': {
        'task': 'supermarkets.tasks.record_losses_all_supermarkets',
        'schedule': crontab(hour=22, minute=30),
    },

    # Stats update at 6:00 AM every day
    'update-stats-morning': {
        'task': 'supermarkets.tasks.update_stats_all_scheduled_storages',
        'schedule': crontab(hour=6, minute=0),
    },

    # List updates check at 3:00 AM every day
    'check-list-updates': {
        'task': 'supermarkets.tasks.run_scheduled_list_updates',
        'schedule': crontab(hour=3, minute=0),
    },

    # Monthly stock value snapshots on the 1st at 00:30
    'monthly-stock-snapshots': {
        'task': 'supermarkets.tasks.create_monthly_stock_snapshots',
        'schedule': crontab(hour=0, minute=30, day_of_month='1'),
    },
}

@app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery is working"""
    print(f'Request: {self.request!r}')